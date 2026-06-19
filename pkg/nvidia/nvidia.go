package nvidia

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/octopos/octopos/pkg/cluster"
	"golang.org/x/sys/unix"
)

const (
	DefaultProjectionRoot     = "/run/octopos/nvidia"
	DefaultDriverCapabilities = "compute,utility"
	defaultDevRoot            = "/dev"
)

var driverLibraryPatterns = []string{
	"libcuda.so*",
	"libnvidia-allocator.so*",
	"libnvidia-cfg.so*",
	"libnvidia-compiler.so*",
	"libnvidia-encode.so*",
	"libnvidia-fatbinaryloader.so*",
	"libnvidia-ml.so*",
	"libnvidia-opencl.so*",
	"libnvidia-ptxjitcompiler.so*",
	"libnvcuvid.so*",
}

var driverLibraryDirs = []string{
	"/usr/lib/x86_64-linux-gnu",
	"/usr/lib64",
	"/usr/lib",
	"/lib/x86_64-linux-gnu",
	"/run/opengl-driver/lib",
	"/usr/local/cuda/compat",
	"/usr/local/nvidia/lib64",
}

// DiscoverDevices returns NVIDIA character devices available on the local host.
func DiscoverDevices(devRoot string) ([]cluster.GPUDevice, error) {
	if devRoot == "" {
		devRoot = defaultDevRoot
	}
	entries, err := os.ReadDir(devRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	uuids := queryGPUUUIDsByIndex()
	devices := make([]cluster.GPUDevice, 0)
	for _, entry := range entries {
		index, ok := nvidiaDeviceIndex(entry.Name())
		if !ok {
			continue
		}
		path := filepath.Join(devRoot, entry.Name())
		major, minor, ok, err := DeviceMajorMinor(path)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		devices = append(devices, cluster.GPUDevice{
			Index: index,
			UUID:  uuids[index],
			Path:  filepath.Join(defaultDevRoot, entry.Name()),
			Major: major,
			Minor: minor,
		})
	}
	sort.Slice(devices, func(i, j int) bool {
		return devices[i].Index < devices[j].Index
	})
	return devices, nil
}

// DeviceMajorMinor returns the major/minor numbers for a character device.
func DeviceMajorMinor(path string) (uint32, uint32, bool, error) {
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		if os.IsNotExist(err) {
			return 0, 0, false, nil
		}
		return 0, 0, false, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFCHR {
		return 0, 0, false, nil
	}
	return unix.Major(uint64(st.Rdev)), unix.Minor(uint64(st.Rdev)), true, nil
}

// EncodeDeviceSpec serializes allocated devices for the privileged launcher.
func EncodeDeviceSpec(devices []cluster.GPUDevice) string {
	parts := make([]string, 0, len(devices))
	for _, dev := range devices {
		fields := []string{"index=" + strconv.Itoa(dev.Index)}
		if dev.UUID != "" {
			fields = append(fields, "uuid="+dev.UUID)
		}
		if dev.Path != "" {
			fields = append(fields, "path="+dev.Path)
		}
		if dev.Major != 0 || dev.Minor != 0 {
			fields = append(fields,
				"major="+strconv.FormatUint(uint64(dev.Major), 10),
				"minor="+strconv.FormatUint(uint64(dev.Minor), 10),
			)
		}
		parts = append(parts, strings.Join(fields, ","))
	}
	return strings.Join(parts, ";")
}

// ParseDeviceSpec parses an EncodeDeviceSpec string.
func ParseDeviceSpec(spec string) ([]cluster.GPUDevice, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, nil
	}
	parts := strings.Split(spec, ";")
	devices := make([]cluster.GPUDevice, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		dev := cluster.GPUDevice{Index: -1}
		if index, err := strconv.Atoi(part); err == nil {
			dev.Index = index
		} else {
			for _, field := range strings.Split(part, ",") {
				key, value, ok := strings.Cut(strings.TrimSpace(field), "=")
				if !ok {
					return nil, fmt.Errorf("invalid NVIDIA GPU field %q", field)
				}
				switch key {
				case "index":
					index, err := strconv.Atoi(value)
					if err != nil {
						return nil, fmt.Errorf("invalid NVIDIA GPU index %q: %w", value, err)
					}
					dev.Index = index
				case "uuid":
					dev.UUID = value
				case "path":
					dev.Path = value
				case "major":
					major, err := strconv.ParseUint(value, 10, 32)
					if err != nil {
						return nil, fmt.Errorf("invalid NVIDIA GPU major %q: %w", value, err)
					}
					dev.Major = uint32(major)
				case "minor":
					minor, err := strconv.ParseUint(value, 10, 32)
					if err != nil {
						return nil, fmt.Errorf("invalid NVIDIA GPU minor %q: %w", value, err)
					}
					dev.Minor = uint32(minor)
				default:
					return nil, fmt.Errorf("unknown NVIDIA GPU field %q", key)
				}
			}
		}
		if dev.Index < 0 {
			return nil, fmt.Errorf("NVIDIA GPU spec %q is missing index", part)
		}
		if dev.Path == "" {
			dev.Path = filepath.Join(defaultDevRoot, "nvidia"+strconv.Itoa(dev.Index))
		}
		devices = append(devices, dev)
	}
	sort.Slice(devices, func(i, j int) bool {
		return devices[i].Index < devices[j].Index
	})
	return devices, nil
}

// VisibleDevicesValue returns the value to expose through NVIDIA/CUDA env vars.
func VisibleDevicesValue(devices []cluster.GPUDevice) string {
	if len(devices) == 0 {
		return ""
	}
	allUUIDs := true
	values := make([]string, 0, len(devices))
	for _, dev := range devices {
		if dev.UUID == "" {
			allUUIDs = false
			break
		}
		values = append(values, dev.UUID)
	}
	if allUUIDs {
		return strings.Join(values, ",")
	}
	values = values[:0]
	for _, dev := range devices {
		values = append(values, strconv.Itoa(dev.Index))
	}
	return strings.Join(values, ",")
}

// EnsureProjection prepares host NVIDIA user-space files for bind mounting.
func EnsureProjection(root string) error {
	if root == "" {
		root = DefaultProjectionRoot
	}
	binDir := filepath.Join(root, "bin")
	libDir := filepath.Join(root, "lib64")
	etcDir := filepath.Join(root, "etc")
	for _, dir := range []string{binDir, libDir, etcDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("create NVIDIA projection dir %s: %w", dir, err)
		}
	}

	if smi := findNvidiaSMI(); smi != "" {
		if err := projectRegularFile(smi, filepath.Join(binDir, "nvidia-smi")); err != nil {
			return err
		}
	}

	for _, lib := range discoverDriverLibraries() {
		if err := projectLibrary(lib, libDir); err != nil {
			return err
		}
	}
	return nil
}

// DriverVersion returns the installed NVIDIA driver version when available.
func DriverVersion() string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "nvidia-smi", "--query-gpu=driver_version", "--format=csv,noheader,nounits")
	out, err := cmd.Output()
	if err == nil {
		for _, line := range strings.Split(string(out), "\n") {
			line = strings.TrimSpace(line)
			if line != "" {
				return line
			}
		}
	}
	data, err := os.ReadFile("/proc/driver/nvidia/version")
	if err != nil {
		return ""
	}
	fields := strings.Fields(string(data))
	for i, field := range fields {
		if field == "Module" && i+1 < len(fields) {
			return fields[i+1]
		}
	}
	return ""
}

func nvidiaDeviceIndex(name string) (int, bool) {
	if !strings.HasPrefix(name, "nvidia") {
		return 0, false
	}
	suffix := strings.TrimPrefix(name, "nvidia")
	if suffix == "" {
		return 0, false
	}
	for _, r := range suffix {
		if r < '0' || r > '9' {
			return 0, false
		}
	}
	index, err := strconv.Atoi(suffix)
	return index, err == nil
}

func queryGPUUUIDsByIndex() map[int]string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "nvidia-smi", "--query-gpu=index,uuid", "--format=csv,noheader,nounits")
	out, err := cmd.Output()
	if err != nil {
		return nil
	}
	uuids := make(map[int]string)
	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), ",")
		if len(parts) < 2 {
			continue
		}
		index, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			continue
		}
		uuid := strings.TrimSpace(parts[1])
		if uuid != "" {
			uuids[index] = uuid
		}
	}
	return uuids
}

func discoverDriverLibraries() []string {
	paths := make(map[string]struct{})
	for _, dir := range driverLibraryDirs {
		for _, pattern := range driverLibraryPatterns {
			matches, _ := filepath.Glob(filepath.Join(dir, pattern))
			for _, match := range matches {
				if info, err := os.Stat(match); err == nil && !info.IsDir() {
					paths[match] = struct{}{}
				}
			}
		}
	}
	for _, path := range ldconfigLibraryPaths() {
		paths[path] = struct{}{}
	}

	out := make([]string, 0, len(paths))
	for path := range paths {
		out = append(out, path)
	}
	sort.Strings(out)
	return out
}

func ldconfigLibraryPaths() []string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ldconfig", "-p").Output()
	if err != nil {
		return nil
	}
	var paths []string
	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		line := scanner.Text()
		_, path, ok := strings.Cut(line, "=>")
		if !ok {
			continue
		}
		path = strings.TrimSpace(path)
		base := filepath.Base(path)
		for _, pattern := range driverLibraryPatterns {
			if matched, _ := filepath.Match(pattern, base); matched {
				paths = append(paths, path)
				break
			}
		}
	}
	return paths
}

func findNvidiaSMI() string {
	if path, err := exec.LookPath("nvidia-smi"); err == nil {
		return path
	}
	for _, path := range []string{"/usr/bin/nvidia-smi", "/usr/local/nvidia/bin/nvidia-smi"} {
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			return path
		}
	}
	return ""
}

func projectLibrary(src, destDir string) error {
	resolved, err := filepath.EvalSymlinks(src)
	if err != nil {
		return nil
	}
	targetName := filepath.Base(resolved)
	target := filepath.Join(destDir, targetName)
	if err := projectRegularFile(resolved, target); err != nil {
		return err
	}
	linkName := filepath.Base(src)
	if linkName != targetName {
		link := filepath.Join(destDir, linkName)
		if err := os.Symlink(targetName, link); err != nil && !os.IsExist(err) {
			return fmt.Errorf("project NVIDIA library symlink %s: %w", link, err)
		}
	}
	return nil
}

func projectRegularFile(src, dest string) error {
	info, err := os.Stat(src)
	if err != nil {
		return nil
	}
	if info.IsDir() {
		return nil
	}
	if _, err := os.Stat(dest); err == nil {
		return nil
	}
	if err := os.Link(src, dest); err == nil || os.IsExist(err) {
		return nil
	}
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open NVIDIA projection source %s: %w", src, err)
	}
	defer in.Close()
	out, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_EXCL, info.Mode().Perm())
	if err != nil {
		if os.IsExist(err) {
			return nil
		}
		return fmt.Errorf("create NVIDIA projection file %s: %w", dest, err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return fmt.Errorf("copy NVIDIA projection file %s: %w", dest, err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("close NVIDIA projection file %s: %w", dest, err)
	}
	return nil
}
