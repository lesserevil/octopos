package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/octopos/octopos/pkg/lockcheck"
)

var osExecutable = os.Executable

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, lockcheck.ErrLocked) {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "octopos-lockcheck: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("octopos-lockcheck", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	role := flags.String("role", "self-test", "Role: self-test, hold, or try")
	path := flags.String("path", filepath.Join(os.TempDir(), "octopos-lockcheck.lock"), "Lock file path")
	kind := flags.String("lock", string(lockcheck.KindFlock), "Lock kind: flock or fcntl")
	mode := flags.String("mode", string(lockcheck.ModeExclusive), "Lock mode: exclusive or shared")
	hold := flags.Duration("hold", 3*time.Second, "How long a holder keeps the lock")
	readyFile := flags.String("ready-file", "", "File touched after a holder acquires the lock")
	if err := flags.Parse(args); err != nil {
		return err
	}

	lockKind := lockcheck.Kind(*kind)
	lockMode := lockcheck.Mode(*mode)
	switch *role {
	case "hold":
		return holdLock(*path, lockKind, lockMode, *hold, *readyFile)
	case "try":
		return tryLock(*path, lockKind, lockMode)
	case "self-test":
		return selfTest(*path)
	default:
		return fmt.Errorf("unknown role %q", *role)
	}
}

func holdLock(path string, kind lockcheck.Kind, mode lockcheck.Mode, hold time.Duration, readyFile string) error {
	file, err := lockcheck.OpenLockFile(path)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := lockcheck.LockWithMode(file, kind, mode, false); err != nil {
		return err
	}
	defer lockcheck.Unlock(file, kind)
	if readyFile != "" {
		if err := os.WriteFile(readyFile, []byte("ready\n"), 0600); err != nil {
			return err
		}
	}
	time.Sleep(hold)
	return nil
}

func tryLock(path string, kind lockcheck.Kind, mode lockcheck.Mode) error {
	file, err := lockcheck.OpenLockFile(path)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := lockcheck.LockWithMode(file, kind, mode, true); err != nil {
		return err
	}
	defer lockcheck.Unlock(file, kind)
	fmt.Printf("%s %s lock acquired on %s\n", kind, mode, path)
	return nil
}

func selfTest(basePath string) error {
	for _, kind := range []lockcheck.Kind{lockcheck.KindFlock, lockcheck.KindFcntl} {
		if err := selfTestKind(basePath+"."+string(kind)+".exclusive", kind, lockcheck.ModeExclusive, lockcheck.ModeExclusive, true); err != nil {
			return err
		}
		if err := selfTestKind(basePath+"."+string(kind)+".shared", kind, lockcheck.ModeShared, lockcheck.ModeShared, false); err != nil {
			return err
		}
		if err := selfTestKind(basePath+"."+string(kind)+".shared-exclusive", kind, lockcheck.ModeShared, lockcheck.ModeExclusive, true); err != nil {
			return err
		}
	}
	fmt.Println("lock self-test passed")
	return nil
}

func selfTestKind(path string, kind lockcheck.Kind, holderMode lockcheck.Mode, contenderMode lockcheck.Mode, wantLocked bool) error {
	exe, err := executablePath()
	if err != nil {
		return err
	}
	tmpDir := os.TempDir()
	readyFile := filepath.Join(tmpDir, fmt.Sprintf("octopos-lockcheck-%d-%s-%s.ready", os.Getpid(), kind, holderMode))
	defer os.Remove(readyFile)
	holder := exec.Command(exe, "--role", "hold", "--lock", string(kind), "--mode", string(holderMode), "--path", path, "--hold", "2s", "--ready-file", readyFile)
	holder.Stdout = os.Stdout
	holder.Stderr = os.Stderr
	if err := holder.Start(); err != nil {
		return err
	}
	defer holder.Wait()
	if err := waitReady(readyFile, time.Second); err != nil {
		_ = holder.Process.Kill()
		return err
	}
	contender := exec.Command(exe, "--role", "try", "--lock", string(kind), "--mode", string(contenderMode), "--path", path)
	err = contender.Run()
	if wantLocked {
		if err == nil {
			_ = holder.Process.Kill()
			return fmt.Errorf("%s %s contender acquired lock while %s holder was active", kind, contenderMode, holderMode)
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 2 {
			return nil
		}
		_ = holder.Process.Kill()
		return fmt.Errorf("%s %s contender failed unexpectedly: %w", kind, contenderMode, err)
	}
	if err != nil {
		_ = holder.Process.Kill()
		return fmt.Errorf("%s %s contender should share %s holder: %w", kind, contenderMode, holderMode, err)
	}
	return nil
}

func executablePath() (string, error) {
	exe, err := osExecutable()
	if err == nil && exe != "" {
		return exe, nil
	}
	if os.Args[0] != "" {
		return os.Args[0], nil
	}
	return "", err
}

func waitReady(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s", path)
}
