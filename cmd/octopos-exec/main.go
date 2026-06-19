package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/octopos/octopos/pkg/cluster"
	"github.com/octopos/octopos/pkg/nvidia"
	"github.com/octopos/octopos/pkg/ssi"
	"golang.org/x/sys/unix"
)

var (
	rootFS     = flag.String("rootfs", ssi.DefaultRootFS, "SSI root filesystem")
	mountBase  = flag.String("mount-base", ssi.DefaultMountBase, "SSI virtual filesystem mount base")
	cwd        = flag.String("cwd", "/", "Working directory inside the SSI root")
	requireVFS = flag.Bool("require-vfs", true, "Require virtual /proc and /sys mounts")

	nvidiaGPUs         = flag.String("nvidia-gpus", "", "Allocated NVIDIA GPU spec (internal)")
	nvidiaProjection   = flag.String("nvidia-projection", nvidia.DefaultProjectionRoot, "Host NVIDIA projection root")
	nvidiaCapabilities = flag.String("nvidia-capabilities", nvidia.DefaultDriverCapabilities, "NVIDIA driver capabilities")
	nvidiaDevRoot      = flag.String("nvidia-dev-root", "/dev", "Host NVIDIA device root")

	mknod = unix.Mknod
	chmod = os.Chmod
)

func main() {
	flag.Parse()
	argv := flag.Args()
	if len(argv) == 0 {
		fatalf("missing command")
	}

	gpuDevices, err := nvidia.ParseDeviceSpec(*nvidiaGPUs)
	if err != nil {
		fatalf("%v", err)
	}
	nvidiaRuntime := nvidiaRuntimeConfig{
		devices:      gpuDevices,
		projection:   *nvidiaProjection,
		capabilities: *nvidiaCapabilities,
		devRoot:      *nvidiaDevRoot,
	}

	if err := enterSSI(*rootFS, *mountBase, *cwd, *requireVFS, nvidiaRuntime, argv); err != nil {
		fatalf("%v", err)
	}
}

type nvidiaRuntimeConfig struct {
	devices      []cluster.GPUDevice
	projection   string
	capabilities string
	devRoot      string
}

func (c nvidiaRuntimeConfig) enabled() bool {
	return len(c.devices) > 0
}

func enterSSI(root, base, workdir string, strictVFS bool, gpu nvidiaRuntimeConfig, argv []string) error {
	root = filepath.Clean(root)
	base = filepath.Clean(base)
	gpu.projection = filepath.Clean(gpu.projection)
	gpu.devRoot = filepath.Clean(gpu.devRoot)
	if err := validateRoot(root); err != nil {
		return err
	}
	if gpu.enabled() {
		if !filepath.IsAbs(gpu.projection) {
			return fmt.Errorf("NVIDIA projection path %s must be absolute", gpu.projection)
		}
		if !filepath.IsAbs(gpu.devRoot) {
			return fmt.Errorf("NVIDIA device root %s must be absolute", gpu.devRoot)
		}
		if err := nvidia.EnsureProjection(gpu.projection); err != nil {
			return fmt.Errorf("prepare NVIDIA projection: %w", err)
		}
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
	if err := mountVirtualFS(root, base, "proc", strictVFS, gpu); err != nil {
		return err
	}
	if err := mountVirtualFS(root, base, "dev", strictVFS, gpu); err != nil {
		return err
	}
	if err := mountVirtualFS(root, base, "sys", strictVFS, gpu); err != nil {
		return err
	}
	if gpu.enabled() {
		if err := mountNVIDIAProjection(root, gpu.projection); err != nil {
			return err
		}
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
	if gpu.enabled() {
		env = applyNVIDIAEnv(env, gpu)
	}
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

func mountVirtualFS(root, base, name string, strict bool, gpu nvidiaRuntimeConfig) error {
	target := filepath.Join(root, name)
	if name == "dev" {
		return mountPrivateDev(target, gpu)
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

func mountPrivateDev(target string, gpu nvidiaRuntimeConfig) error {
	if err := unix.Mount("tmpfs", target, "tmpfs", unix.MS_NOSUID|unix.MS_STRICTATIME, "mode=755,size=65536k"); err != nil {
		return fmt.Errorf("mount private /dev: %w", err)
	}
	for _, dir := range []string{"pts", "shm"} {
		if err := os.MkdirAll(filepath.Join(target, dir), 0755); err != nil {
			return fmt.Errorf("create /dev/%s: %w", dir, err)
		}
	}
	devices := []deviceNode{
		{"null", 0666, 1, 3},
		{"zero", 0666, 1, 5},
		{"full", 0666, 1, 7},
		{"random", 0666, 1, 8},
		{"urandom", 0666, 1, 9},
		{"tty", 0666, 5, 0},
		{"kvm", 0666, 10, 232},
	}
	for _, dev := range devices {
		if err := createDeviceNode(filepath.Join(target, dev.name), dev); err != nil {
			return err
		}
	}
	if gpu.enabled() {
		if err := createNVIDIADeviceNodes(target, gpu); err != nil {
			return err
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

func createNVIDIADeviceNodes(devTarget string, gpu nvidiaRuntimeConfig) error {
	for _, dev := range gpu.devices {
		name := "nvidia" + strconv.Itoa(dev.Index)
		hostPath := hostNVIDIADevicePath(gpu.devRoot, dev, name)
		if err := createHostDeviceNode(devTarget, hostPath, name, true); err != nil {
			return err
		}
	}
	for _, name := range []string{"nvidiactl", "nvidia-uvm", "nvidia-uvm-tools", "nvidia-modeset"} {
		required := name == "nvidiactl"
		if err := createHostDeviceNode(devTarget, filepath.Join(gpu.devRoot, name), name, required); err != nil {
			return err
		}
	}
	return createNVIDIACapDeviceNodes(devTarget, gpu.devRoot)
}

func hostNVIDIADevicePath(devRoot string, dev cluster.GPUDevice, name string) string {
	if dev.Path != "" && devRoot == "/dev" {
		return dev.Path
	}
	return filepath.Join(devRoot, name)
}

func createNVIDIACapDeviceNodes(devTarget, devRoot string) error {
	sourceDir := filepath.Join(devRoot, "nvidia-caps")
	entries, err := os.ReadDir(sourceDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read NVIDIA caps devices: %w", err)
	}
	targetDir := filepath.Join(devTarget, "nvidia-caps")
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("create /dev/nvidia-caps: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := filepath.Join("nvidia-caps", entry.Name())
		if err := createHostDeviceNode(devTarget, filepath.Join(sourceDir, entry.Name()), name, false); err != nil {
			return err
		}
	}
	return nil
}

func createHostDeviceNode(devTarget, hostPath, name string, required bool) error {
	dev, ok, err := hostDeviceNode(hostPath, name)
	if err != nil {
		if required {
			return err
		}
		return nil
	}
	if !ok {
		if required {
			return fmt.Errorf("required NVIDIA device %s is not a character device", hostPath)
		}
		return nil
	}
	target := filepath.Join(devTarget, name)
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return fmt.Errorf("create /dev/%s parent: %w", name, err)
	}
	return createDeviceNode(target, dev)
}

func hostDeviceNode(path, name string) (deviceNode, bool, error) {
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		if os.IsNotExist(err) {
			return deviceNode{}, false, fmt.Errorf("NVIDIA device %s does not exist", path)
		}
		return deviceNode{}, false, fmt.Errorf("stat NVIDIA device %s: %w", path, err)
	}
	if st.Mode&unix.S_IFMT != unix.S_IFCHR {
		return deviceNode{}, false, nil
	}
	mode := uint32(st.Mode & 0777)
	if mode == 0 {
		mode = 0666
	}
	return deviceNode{
		name:  name,
		mode:  mode,
		major: unix.Major(uint64(st.Rdev)),
		minor: unix.Minor(uint64(st.Rdev)),
	}, true, nil
}

func mountNVIDIAProjection(root, projection string) error {
	for _, name := range []string{"bin", "lib64", "etc"} {
		source := filepath.Join(projection, name)
		info, err := os.Stat(source)
		if err != nil {
			return fmt.Errorf("NVIDIA projection %s is unavailable: %w", source, err)
		}
		if !info.IsDir() {
			return fmt.Errorf("NVIDIA projection %s is not a directory", source)
		}
		target := filepath.Join(root, "usr", "local", "nvidia", name)
		if err := os.MkdirAll(target, 0755); err != nil {
			return fmt.Errorf("create NVIDIA projection mount target %s: %w", target, err)
		}
		if err := bindMount(source, target, true); err != nil {
			return err
		}
	}
	return nil
}

type deviceNode struct {
	name         string
	mode         uint32
	major, minor uint32
}

func createDeviceNode(path string, dev deviceNode) error {
	mode := uint32(unix.S_IFCHR) | dev.mode
	if err := mknod(path, mode, int(unix.Mkdev(dev.major, dev.minor))); err != nil && err != unix.EEXIST {
		return fmt.Errorf("create /dev/%s: %w", dev.name, err)
	}
	if err := chmod(path, os.FileMode(dev.mode)); err != nil {
		return fmt.Errorf("chmod /dev/%s: %w", dev.name, err)
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

func applyNVIDIAEnv(env []string, gpu nvidiaRuntimeConfig) []string {
	visible := nvidia.VisibleDevicesValue(gpu.devices)
	if visible != "" {
		env = setEnvValue(env, "NVIDIA_VISIBLE_DEVICES", visible)
		env = setEnvValue(env, "CUDA_VISIBLE_DEVICES", visible)
	}
	capabilities := gpu.capabilities
	if capabilities == "" {
		capabilities = nvidia.DefaultDriverCapabilities
	}
	env = setEnvValue(env, "NVIDIA_DRIVER_CAPABILITIES", capabilities)
	env = prependEnvPaths(env, "PATH", []string{"/usr/local/nvidia/bin", "/usr/local/cuda/bin"})
	env = prependEnvPaths(env, "LD_LIBRARY_PATH", []string{"/usr/local/nvidia/lib64", "/usr/local/cuda/lib64"})
	return env
}

func setEnvValue(env []string, key, value string) []string {
	prefix := key + "="
	entry := prefix + value
	out := make([]string, 0, len(env)+1)
	replaced := false
	for _, existing := range env {
		if strings.HasPrefix(existing, prefix) {
			if !replaced {
				out = append(out, entry)
				replaced = true
			}
			continue
		}
		out = append(out, existing)
	}
	if !replaced {
		out = append(out, entry)
	}
	return out
}

func prependEnvPaths(env []string, key string, paths []string) []string {
	current := envValue(env, key)
	seen := make(map[string]bool)
	values := make([]string, 0, len(paths)+8)
	for _, path := range paths {
		if path == "" || seen[path] {
			continue
		}
		seen[path] = true
		values = append(values, path)
	}
	for _, path := range filepath.SplitList(current) {
		if path == "" || seen[path] {
			continue
		}
		seen[path] = true
		values = append(values, path)
	}
	return setEnvValue(env, key, strings.Join(values, string(os.PathListSeparator)))
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
