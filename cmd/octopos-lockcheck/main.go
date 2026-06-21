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
	hold := flags.Duration("hold", 3*time.Second, "How long a holder keeps the lock")
	readyFile := flags.String("ready-file", "", "File touched after a holder acquires the lock")
	if err := flags.Parse(args); err != nil {
		return err
	}

	lockKind := lockcheck.Kind(*kind)
	switch *role {
	case "hold":
		return holdLock(*path, lockKind, *hold, *readyFile)
	case "try":
		return tryLock(*path, lockKind)
	case "self-test":
		return selfTest(*path)
	default:
		return fmt.Errorf("unknown role %q", *role)
	}
}

func holdLock(path string, kind lockcheck.Kind, hold time.Duration, readyFile string) error {
	file, err := lockcheck.OpenLockFile(path)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := lockcheck.Lock(file, kind, false); err != nil {
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

func tryLock(path string, kind lockcheck.Kind) error {
	file, err := lockcheck.OpenLockFile(path)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := lockcheck.Lock(file, kind, true); err != nil {
		return err
	}
	defer lockcheck.Unlock(file, kind)
	fmt.Printf("%s lock acquired on %s\n", kind, path)
	return nil
}

func selfTest(basePath string) error {
	for _, kind := range []lockcheck.Kind{lockcheck.KindFlock, lockcheck.KindFcntl} {
		if err := selfTestKind(basePath+"."+string(kind), kind); err != nil {
			return err
		}
	}
	fmt.Println("lock self-test passed")
	return nil
}

func selfTestKind(path string, kind lockcheck.Kind) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	tmpDir := os.TempDir()
	readyFile := filepath.Join(tmpDir, fmt.Sprintf("octopos-lockcheck-%d-%s.ready", os.Getpid(), kind))
	defer os.Remove(readyFile)
	holder := exec.Command(exe, "--role", "hold", "--lock", string(kind), "--path", path, "--hold", "2s", "--ready-file", readyFile)
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
	contender := exec.Command(exe, "--role", "try", "--lock", string(kind), "--path", path)
	if err := contender.Run(); err == nil {
		_ = holder.Process.Kill()
		return fmt.Errorf("%s contender acquired lock while holder was active", kind)
	} else {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 2 {
			return nil
		}
		_ = holder.Process.Kill()
		return fmt.Errorf("%s contender failed unexpectedly: %w", kind, err)
	}
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
