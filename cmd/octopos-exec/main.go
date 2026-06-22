package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/octopos/octopos/pkg/cluster"
	"github.com/octopos/octopos/pkg/nvidia"
	"github.com/octopos/octopos/pkg/remotechild"
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
	vfioGroups         = flag.String("vfio-groups", "", "Allocated VFIO group IDs (internal)")
	vfioDevRoot        = flag.String("vfio-dev-root", "/dev/vfio", "Host VFIO device root")
	ttySlave           = flag.String("tty-slave", "", "Host PTY slave path (internal)")

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
	vfioGroupIDs, err := parseVFIOGroups(*vfioGroups)
	if err != nil {
		fatalf("%v", err)
	}
	vfioRuntime := vfioRuntimeConfig{
		groups:  vfioGroupIDs,
		devRoot: *vfioDevRoot,
	}
	ttyRuntime := ttyRuntimeConfig{slave: *ttySlave}

	if err := enterSSI(*rootFS, *mountBase, *cwd, *requireVFS, nvidiaRuntime, vfioRuntime, ttyRuntime, argv); err != nil {
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

type vfioRuntimeConfig struct {
	groups  []int
	devRoot string
}

func (c vfioRuntimeConfig) enabled() bool {
	return len(c.groups) > 0
}

type ttyRuntimeConfig struct {
	slave string
}

func (c ttyRuntimeConfig) enabled() bool {
	return c.slave != ""
}

func parseVFIOGroups(spec string) ([]int, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, nil
	}
	parts := strings.Split(spec, ",")
	groups := make([]int, 0, len(parts))
	seen := make(map[int]bool)
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		groupID, err := strconv.Atoi(part)
		if err != nil || groupID <= 0 {
			return nil, fmt.Errorf("invalid VFIO group %q", part)
		}
		if seen[groupID] {
			continue
		}
		seen[groupID] = true
		groups = append(groups, groupID)
	}
	return groups, nil
}

func enterSSI(root, base, workdir string, strictVFS bool, gpu nvidiaRuntimeConfig, vfio vfioRuntimeConfig, tty ttyRuntimeConfig, argv []string) error {
	root = filepath.Clean(root)
	base = filepath.Clean(base)
	gpu.projection = filepath.Clean(gpu.projection)
	gpu.devRoot = filepath.Clean(gpu.devRoot)
	vfio.devRoot = filepath.Clean(vfio.devRoot)
	if tty.enabled() {
		tty.slave = filepath.Clean(tty.slave)
		if err := validateTTYSlave(tty.slave); err != nil {
			return err
		}
	}
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
		if err := nvidia.EnsureProfile(root); err != nil {
			return fmt.Errorf("install NVIDIA profile hook: %w", err)
		}
		if err := nvidia.EnsureProjection(gpu.projection); err != nil {
			return fmt.Errorf("prepare NVIDIA projection: %w", err)
		}
	}
	if vfio.enabled() && !filepath.IsAbs(vfio.devRoot) {
		return fmt.Errorf("VFIO device root %s must be absolute", vfio.devRoot)
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
		if err := ensureSSIRootDir(root, name); err != nil {
			return err
		}
	}
	if err := mountVirtualFS(root, base, "proc", strictVFS, gpu, vfio, tty); err != nil {
		return err
	}
	if err := mountVirtualFS(root, base, "dev", strictVFS, gpu, vfio, tty); err != nil {
		return err
	}
	if err := mountVirtualFS(root, base, "sys", strictVFS, gpu, vfio, tty); err != nil {
		return err
	}
	if gpu.enabled() {
		if err := mountNVIDIAProjection(root, gpu.projection); err != nil {
			return err
		}
	}
	if err := mountRuntimeSocket(root, remotechild.DefaultSocketPath); err != nil {
		return err
	}
	hostFDDir, err := mountHostFDDir(root)
	if err != nil {
		return err
	}
	parentStdioPipeEnv := parentStdioPipeEnvFromCurrentProcess()
	if err := closeInheritedNonStdioFDs(); err != nil {
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
	if hostFDDir != "" {
		env = setEnvValue(env, remotechild.EnvHostFDDir, hostFDDir)
	}
	env = applyParentStdioPipeEnv(env, parentStdioPipeEnv)
	if gpu.enabled() {
		env = applyNVIDIAEnv(env, gpu)
	}
	if err := applyFDReopenPlan(env); err != nil {
		return err
	}
	path, err := lookPathInRoot(argv[0], env)
	if err != nil {
		return err
	}
	return syscall.Exec(path, argv, env)
}

func applyFDReopenPlan(env []string) error {
	raw := envValue(env, remotechild.EnvFDPlan)
	if raw == "" {
		return nil
	}
	fds, err := remotechild.DecodeReopenFDs(raw)
	if err != nil {
		return fmt.Errorf("decode remote-child fd plan: %w", err)
	}
	for _, plan := range fds {
		if plan.FD < 3 {
			return fmt.Errorf("refusing to reopen reserved fd %d", plan.FD)
		}
		if plan.Path == "" || !filepath.IsAbs(plan.Path) {
			return fmt.Errorf("fd %d reopen path %q must be absolute", plan.FD, plan.Path)
		}
		flags := plan.Flags
		if flags == 0 {
			flags = os.O_RDONLY
		}
		file, err := os.OpenFile(plan.Path, flags, 0)
		if err != nil {
			return fmt.Errorf("reopen fd %d from %s: %w", plan.FD, plan.Path, err)
		}
		if plan.Offset > 0 && flags&os.O_APPEND == 0 {
			if _, err := file.Seek(plan.Offset, 0); err != nil {
				_ = file.Close()
				return fmt.Errorf("seek fd %d to %d: %w", plan.FD, plan.Offset, err)
			}
		}
		if int(file.Fd()) != plan.FD {
			if err := unix.Dup2(int(file.Fd()), plan.FD); err != nil {
				_ = file.Close()
				return fmt.Errorf("dup fd %d to %d: %w", file.Fd(), plan.FD, err)
			}
			if err := file.Close(); err != nil {
				return fmt.Errorf("close temporary fd for %d: %w", plan.FD, err)
			}
		}
		if _, err := unix.FcntlInt(uintptr(plan.FD), unix.F_SETFD, 0); err != nil {
			return fmt.Errorf("clear close-on-exec for fd %d: %w", plan.FD, err)
		}
	}
	return nil
}

func closeInheritedNonStdioFDs() error {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read inherited fds: %w", err)
	}
	for _, fd := range fdNamesToClose(entries) {
		_ = unix.Close(fd)
	}
	return nil
}

func fdNamesToClose(entries []os.DirEntry) []int {
	fds := make([]int, 0, len(entries))
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil || fd <= 2 {
			continue
		}
		fds = append(fds, fd)
	}
	return fds
}

func validateRoot(root string) error {
	for _, path := range []string{"bin", "usr/bin"} {
		if info, err := os.Stat(filepath.Join(root, path)); err == nil && info.IsDir() {
			return nil
		}
	}
	return fmt.Errorf("SSI rootfs %s is not bootstrapped: missing bin or usr/bin", root)
}

func ensureSSIRootDir(root, name string) error {
	path := filepath.Join(root, name)
	if info, err := os.Stat(path); err == nil {
		if info.IsDir() {
			return nil
		}
		return fmt.Errorf("create /%s in SSI root: %s exists and is not a directory", name, path)
	} else if !os.IsNotExist(err) {
		if errors.Is(err, syscall.ENOTCONN) {
			_ = unix.Unmount(path, unix.MNT_DETACH)
			if err := os.MkdirAll(path, 0755); err == nil {
				return nil
			}
		}
		return fmt.Errorf("stat /%s in SSI root: %w", name, err)
	}
	if err := os.MkdirAll(path, 0755); err != nil {
		info, statErr := os.Stat(path)
		if statErr == nil && info.IsDir() {
			return nil
		}
		if os.IsExist(err) && statErr == nil {
			return fmt.Errorf("create /%s in SSI root: %s exists and is not a directory", name, path)
		}
		return fmt.Errorf("create /%s in SSI root: %w", name, err)
	}
	return nil
}

func mountVirtualFS(root, base, name string, strict bool, gpu nvidiaRuntimeConfig, vfio vfioRuntimeConfig, tty ttyRuntimeConfig) error {
	target := filepath.Join(root, name)
	if name == "dev" {
		return mountPrivateDev(target, gpu, vfio, tty)
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

func mountPrivateDev(target string, gpu nvidiaRuntimeConfig, vfio vfioRuntimeConfig, tty ttyRuntimeConfig) error {
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
	if vfio.enabled() {
		if err := createVFIODeviceNodes(target, vfio); err != nil {
			return err
		}
	}
	if tty.enabled() {
		if err := projectTTYSlave(target, tty.slave); err != nil {
			return err
		}
	} else {
		if err := unix.Mount("devpts", filepath.Join(target, "pts"), "devpts", unix.MS_NOSUID|unix.MS_NOEXEC, "newinstance,ptmxmode=0666,mode=0620"); err != nil {
			return fmt.Errorf("mount /dev/pts: %w", err)
		}
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

func validateTTYSlave(path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("PTY slave path %s must be absolute", path)
	}
	if !strings.HasPrefix(path, "/dev/pts/") {
		return fmt.Errorf("PTY slave path %s must be under /dev/pts", path)
	}
	base := filepath.Base(path)
	if base == "." || base == ".." || base == "ptmx" {
		return fmt.Errorf("PTY slave path %s is not a terminal slave", path)
	}
	if _, err := strconv.ParseUint(base, 10, 32); err != nil {
		return fmt.Errorf("PTY slave path %s is not a numeric /dev/pts entry", path)
	}

	var slaveStat unix.Stat_t
	if err := unix.Stat(path, &slaveStat); err != nil {
		return fmt.Errorf("stat PTY slave %s: %w", path, err)
	}
	if slaveStat.Mode&unix.S_IFMT != unix.S_IFCHR {
		return fmt.Errorf("PTY slave %s is not a character device", path)
	}

	var stdinStat unix.Stat_t
	if err := unix.Fstat(int(os.Stdin.Fd()), &stdinStat); err != nil {
		return fmt.Errorf("stat stdin PTY: %w", err)
	}
	if stdinStat.Mode&unix.S_IFMT != unix.S_IFCHR {
		return fmt.Errorf("stdin is not a character device for PTY exec")
	}
	if stdinStat.Rdev != slaveStat.Rdev {
		return fmt.Errorf("PTY slave %s does not match stdin", path)
	}
	return nil
}

func projectTTYSlave(devTarget, slavePath string) error {
	slaveName := filepath.Base(slavePath)
	target := filepath.Join(devTarget, "pts", slaveName)
	file, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_RDONLY, 0600)
	if err != nil {
		return fmt.Errorf("create /dev/pts/%s target: %w", slaveName, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close /dev/pts/%s target: %w", slaveName, err)
	}
	if err := unix.Mount(slavePath, target, "", unix.MS_BIND, ""); err != nil {
		return fmt.Errorf("bind PTY slave %s to /dev/pts/%s: %w", slavePath, slaveName, err)
	}
	return createDeviceNode(filepath.Join(devTarget, "pts", "ptmx"), deviceNode{
		name:  "pts/ptmx",
		mode:  0666,
		major: 5,
		minor: 2,
	})
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

func createVFIODeviceNodes(devTarget string, vfio vfioRuntimeConfig) error {
	targetDir := filepath.Join(devTarget, "vfio")
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("create /dev/vfio: %w", err)
	}
	if err := createHostDeviceNodeWithLabel(devTarget, filepath.Join(vfio.devRoot, "vfio"), filepath.Join("vfio", "vfio"), "VFIO", true); err != nil {
		return err
	}
	for _, groupID := range vfio.groups {
		name := strconv.Itoa(groupID)
		if err := createHostDeviceNodeWithLabel(devTarget, filepath.Join(vfio.devRoot, name), filepath.Join("vfio", name), "VFIO", true); err != nil {
			return err
		}
	}
	return nil
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
	return createHostDeviceNodeWithLabel(devTarget, hostPath, name, "NVIDIA", required)
}

func createHostDeviceNodeWithLabel(devTarget, hostPath, name, label string, required bool) error {
	dev, ok, err := hostDeviceNode(hostPath, name, label)
	if err != nil {
		if required {
			return err
		}
		return nil
	}
	if !ok {
		if required {
			return fmt.Errorf("required %s device %s is not a character device", label, hostPath)
		}
		return nil
	}
	target := filepath.Join(devTarget, name)
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return fmt.Errorf("create /dev/%s parent: %w", name, err)
	}
	return createDeviceNode(target, dev)
}

func hostDeviceNode(path, name, label string) (deviceNode, bool, error) {
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		if os.IsNotExist(err) {
			return deviceNode{}, false, fmt.Errorf("%s device %s does not exist", label, path)
		}
		return deviceNode{}, false, fmt.Errorf("stat %s device %s: %w", label, path, err)
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

func mountRuntimeSocket(root, source string) error {
	var st unix.Stat_t
	if err := unix.Lstat(source, &st); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat runtime socket %s: %w", source, err)
	}
	if st.Mode&unix.S_IFMT != unix.S_IFSOCK {
		return fmt.Errorf("runtime socket %s is not a socket", source)
	}
	target := filepath.Join(root, strings.TrimPrefix(source, string(filepath.Separator)))
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return fmt.Errorf("create runtime socket parent %s: %w", filepath.Dir(target), err)
	}
	file, err := createBindFileTarget(target)
	if err != nil {
		return fmt.Errorf("create runtime socket target %s: %w", target, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close runtime socket target %s: %w", target, err)
	}
	if err := unix.Mount(source, target, "", unix.MS_BIND, ""); err != nil {
		return fmt.Errorf("bind runtime socket %s to %s: %w", source, target, err)
	}
	return nil
}

func createBindFileTarget(target string) (*os.File, error) {
	file, err := os.OpenFile(target, os.O_RDONLY|os.O_CREATE, 0600)
	if err == nil {
		return file, nil
	}
	if !errors.Is(err, syscall.ENXIO) && !errors.Is(err, syscall.ELOOP) {
		return nil, err
	}
	if removeErr := os.Remove(target); removeErr != nil && !os.IsNotExist(removeErr) {
		if errors.Is(removeErr, syscall.EBUSY) {
			_ = unix.Unmount(target, unix.MNT_DETACH)
			removeErr = os.Remove(target)
		}
		if removeErr != nil && !os.IsNotExist(removeErr) {
			return nil, removeErr
		}
	}
	return os.OpenFile(target, os.O_RDONLY|os.O_CREATE, 0600)
}

func mountHostFDDir(root string) (string, error) {
	source := "/proc/self/fd"
	if info, err := os.Stat(source); err != nil || !info.IsDir() {
		return "", nil
	}
	inner := "/run/octopos/host-fd"
	target := filepath.Join(root, strings.TrimPrefix(inner, string(filepath.Separator)))
	if err := createBindDirTarget(target); err != nil {
		return "", fmt.Errorf("create host fd target %s: %w", target, err)
	}
	if err := bindMount(source, target, false); err != nil {
		return "", fmt.Errorf("mount host fd projection: %w", err)
	}
	return inner, nil
}

func createBindDirTarget(target string) error {
	if info, err := os.Lstat(target); err == nil {
		if info.IsDir() {
			return nil
		}
		if err := os.Remove(target); err != nil {
			if errors.Is(err, syscall.EBUSY) {
				_ = unix.Unmount(target, unix.MNT_DETACH)
				err = os.Remove(target)
			}
			if err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(target, 0755); err != nil {
		if os.IsExist(err) {
			_ = unix.Unmount(target, unix.MNT_DETACH)
			_ = os.RemoveAll(target)
			return os.MkdirAll(target, 0755)
		}
		return err
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

func parentStdioPipeEnvFromCurrentProcess() []string {
	plans, err := remotechild.ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		return nil
	}
	return parentStdioPipeEnvFromPlans(plans)
}

func parentStdioPipeEnvFromPlans(plans []remotechild.FDPlan) []string {
	env := make([]string, 0, 3)
	for _, plan := range plans {
		if plan.FD < 0 || plan.FD > 2 || plan.PipeID == "" {
			continue
		}
		env = append(env, remotechild.EnvParentStdioPipeFD(plan.FD)+"="+plan.PipeID)
	}
	return env
}

func applyParentStdioPipeEnv(env []string, markers []string) []string {
	for _, marker := range markers {
		key, value, ok := strings.Cut(marker, "=")
		if !ok || key == "" || value == "" {
			continue
		}
		env = setEnvValue(env, key, value)
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
