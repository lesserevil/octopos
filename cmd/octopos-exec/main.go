package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/octopos/octopos/pkg/ssi"
	"golang.org/x/sys/unix"
)

var (
	rootFS     = flag.String("rootfs", ssi.DefaultRootFS, "SSI root filesystem")
	mountBase  = flag.String("mount-base", ssi.DefaultMountBase, "SSI virtual filesystem mount base")
	cwd        = flag.String("cwd", "/", "Working directory inside the SSI root")
	requireVFS = flag.Bool("require-vfs", true, "Require virtual /proc and /sys mounts")
)

func main() {
	flag.Parse()
	argv := flag.Args()
	if len(argv) == 0 {
		fatalf("missing command")
	}

	if err := enterSSI(*rootFS, *mountBase, *cwd, *requireVFS, argv); err != nil {
		fatalf("%v", err)
	}
}

func enterSSI(root, base, workdir string, strictVFS bool, argv []string) error {
	root = filepath.Clean(root)
	base = filepath.Clean(base)
	if err := validateRoot(root); err != nil {
		return err
	}

	if err := unix.Unshare(unix.CLONE_NEWNS | unix.CLONE_NEWUTS | unix.CLONE_NEWIPC); err != nil {
		return fmt.Errorf("unshare SSI namespaces: %w", err)
	}
	if err := setSSIHostname(); err != nil {
		return err
	}
	if err := unix.Mount("", "/", "", unix.MS_PRIVATE|unix.MS_REC, ""); err != nil {
		return fmt.Errorf("make mounts private: %w", err)
	}

	for _, name := range []string{"proc", "dev", "sys", "tmp"} {
		if err := os.MkdirAll(filepath.Join(root, name), 0755); err != nil {
			return fmt.Errorf("create /%s in SSI root: %w", name, err)
		}
	}
	if err := mountVirtualFS(root, base, "proc", strictVFS); err != nil {
		return err
	}
	if err := mountVirtualFS(root, base, "dev", strictVFS); err != nil {
		return err
	}
	if err := mountVirtualFS(root, base, "sys", strictVFS); err != nil {
		return err
	}

	if err := syscall.Chroot(root); err != nil {
		return fmt.Errorf("chroot %s: %w", root, err)
	}
	if workdir == "" {
		workdir = "/"
	}
	if err := os.Chdir(workdir); err != nil {
		return fmt.Errorf("chdir %s: %w", workdir, err)
	}

	env := ensureDefaultEnv(os.Environ())
	path, err := lookPathInRoot(argv[0], env)
	if err != nil {
		return err
	}
	return syscall.Exec(path, argv, env)
}

func validateRoot(root string) error {
	for _, path := range []string{"bin", "usr/bin"} {
		if info, err := os.Stat(filepath.Join(root, path)); err == nil && info.IsDir() {
			return nil
		}
	}
	return fmt.Errorf("SSI rootfs %s is not bootstrapped: missing bin or usr/bin", root)
}

func mountVirtualFS(root, base, name string, strict bool) error {
	target := filepath.Join(root, name)
	if name == "dev" {
		return mountPrivateDev(target)
	}
	source := filepath.Join(base, name)
	if mounted, _ := ssi.IsMountPoint(source); mounted {
		return bindMount(source, target, true)
	}
	if info, err := os.Stat(source); err == nil && info.IsDir() {
		return bindMount(source, target, true)
	}
	if strict {
		return fmt.Errorf("SSI virtual /%s is not mounted at %s", name, source)
	}
	switch name {
	case "proc":
		return unix.Mount("proc", target, "proc", 0, "")
	case "sys":
		return bindMount("/"+name, target, true)
	default:
		return nil
	}
}

func mountPrivateDev(target string) error {
	if err := unix.Mount("tmpfs", target, "tmpfs", unix.MS_NOSUID|unix.MS_STRICTATIME, "mode=755,size=65536k"); err != nil {
		return fmt.Errorf("mount private /dev: %w", err)
	}
	for _, dir := range []string{"pts", "shm"} {
		if err := os.MkdirAll(filepath.Join(target, dir), 0755); err != nil {
			return fmt.Errorf("create /dev/%s: %w", dir, err)
		}
	}
	devices := []struct {
		name         string
		mode         uint32
		major, minor uint32
	}{
		{"null", 0666, 1, 3},
		{"zero", 0666, 1, 5},
		{"full", 0666, 1, 7},
		{"random", 0666, 1, 8},
		{"urandom", 0666, 1, 9},
		{"tty", 0666, 5, 0},
		{"kvm", 0666, 10, 232},
	}
	for _, dev := range devices {
		path := filepath.Join(target, dev.name)
		mode := uint32(unix.S_IFCHR) | dev.mode
		if err := unix.Mknod(path, mode, int(unix.Mkdev(dev.major, dev.minor))); err != nil && err != unix.EEXIST {
			return fmt.Errorf("create /dev/%s: %w", dev.name, err)
		}
	}
	if err := unix.Mount("devpts", filepath.Join(target, "pts"), "devpts", unix.MS_NOSUID|unix.MS_NOEXEC, "newinstance,ptmxmode=0666,mode=0620"); err != nil {
		return fmt.Errorf("mount /dev/pts: %w", err)
	}
	links := map[string]string{
		"ptmx":   "pts/ptmx",
		"fd":     "/proc/self/fd",
		"stdin":  "/proc/self/fd/0",
		"stdout": "/proc/self/fd/1",
		"stderr": "/proc/self/fd/2",
	}
	for name, targetPath := range links {
		if err := os.Symlink(targetPath, filepath.Join(target, name)); err != nil && !os.IsExist(err) {
			return fmt.Errorf("create /dev/%s symlink: %w", name, err)
		}
	}
	shm := filepath.Join(target, "shm")
	if err := os.Chmod(shm, 01777); err != nil {
		return fmt.Errorf("chmod /dev/shm: %w", err)
	}
	if err := unix.Mount("tmpfs", shm, "tmpfs", unix.MS_NOSUID|unix.MS_NODEV, "mode=1777,size=64m"); err != nil {
		return fmt.Errorf("mount /dev/shm: %w", err)
	}
	return nil
}

func bindMount(source, target string, readonly bool) error {
	if err := unix.Mount(source, target, "", unix.MS_BIND|unix.MS_REC, ""); err != nil {
		return fmt.Errorf("bind mount %s to %s: %w", source, target, err)
	}
	if readonly {
		flags := uintptr(unix.MS_BIND | unix.MS_REMOUNT | unix.MS_RDONLY | unix.MS_REC)
		if err := unix.Mount(source, target, "", flags, ""); err != nil {
			return fmt.Errorf("remount %s readonly: %w", target, err)
		}
	}
	return nil
}

func lookPathInRoot(file string, env []string) (string, error) {
	if strings.ContainsRune(file, '/') {
		if err := unix.Access(file, unix.X_OK); err != nil {
			return "", fmt.Errorf("command %s is not executable in SSI root: %w", file, err)
		}
		return file, nil
	}
	pathEnv := envValue(env, "PATH")
	if pathEnv == "" {
		pathEnv = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
	}
	for _, dir := range filepath.SplitList(pathEnv) {
		if dir == "" {
			dir = "."
		}
		candidate := filepath.Join(dir, file)
		if err := unix.Access(candidate, unix.X_OK); err == nil {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("command %s not found in SSI root PATH", file)
}

func envValue(env []string, key string) string {
	prefix := key + "="
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			return strings.TrimPrefix(entry, prefix)
		}
	}
	return ""
}

func ensureDefaultEnv(env []string) []string {
	if envValue(env, "PATH") == "" {
		env = append(env, "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
	}
	return env
}

func setSSIHostname() error {
	name := os.Getenv("OCTOPOS_CLUSTER_HOSTNAME")
	if name == "" {
		name = ssi.DefaultHostname
	}
	name = normalizeHostname(name)
	if name == "" {
		name = ssi.DefaultHostname
	}
	if err := unix.Sethostname([]byte(name)); err != nil {
		return fmt.Errorf("set SSI hostname %s: %w", name, err)
	}
	return nil
}

func normalizeHostname(name string) string {
	name = strings.TrimSpace(name)
	var b strings.Builder
	for _, r := range name {
		if b.Len() >= 63 {
			break
		}
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return strings.Trim(b.String(), "-.")
}

func fatalf(format string, args ...any) {
	fmt.Fprintln(os.Stderr, "octopos-exec:", fmt.Sprintf(format, args...))
	os.Exit(127)
}
