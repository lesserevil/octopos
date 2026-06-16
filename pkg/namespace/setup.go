package namespace

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
)

type Manager struct {
	cgroupRoot string
}

func NewManager() (*Manager, error) {
	root := "/sys/fs/cgroup"
	if _, err := os.Stat(root); os.IsNotExist(err) {
		return nil, fmt.Errorf("cgroup v2 not available: %w", err)
	}
	return &Manager{cgroupRoot: root}, nil
}

// CreateCgroup creates a cgroup for a session with CPU/memory limits.
func (m *Manager) CreateCgroup(sessionID string, cpuMillicores int64, memoryBytes int64) error {
	path := filepath.Join(m.cgroupRoot, "octopos", sessionID)
	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("mkdir cgroup %s: %w", path, err)
	}

	if cpuMillicores > 0 {
		cpuMax := fmt.Sprintf("%d 100000", cpuMillicores*100)
		if err := os.WriteFile(filepath.Join(path, "cpu.max"), []byte(cpuMax), 0644); err != nil {
			return fmt.Errorf("set cpu.max: %w", err)
		}
	}

	if memoryBytes > 0 {
		if err := os.WriteFile(filepath.Join(path, "memory.max"), []byte(strconv.FormatInt(memoryBytes, 10)), 0644); err != nil {
			return fmt.Errorf("set memory.max: %w", err)
		}
	}

	return nil
}

// AddPID adds a PID to a session cgroup.
func (m *Manager) AddPID(sessionID string, pid int) error {
	path := filepath.Join(m.cgroupRoot, "octopos", sessionID, "cgroup.procs")
	return os.WriteFile(path, []byte(strconv.Itoa(pid)), 0644)
}

// DestroyCgroup removes a session cgroup.
func (m *Manager) DestroyCgroup(sessionID string) error {
	path := filepath.Join(m.cgroupRoot, "octopos", sessionID)

	// Move all PIDs to parent
	procsPath := filepath.Join(path, "cgroup.procs")
	if data, err := os.ReadFile(procsPath); err == nil && len(data) > 0 {
		parentProcs := filepath.Join(m.cgroupRoot, "octopos", "cgroup.procs")
		os.WriteFile(parentProcs, data, 0644)
	}

	return os.RemoveAll(path)
}

// SetupNamespaces configures namespaces for a process.
// Returns a function that should be called in the child process before exec.
func SetupNamespaces(pidNS bool, mountNS bool, netNS bool) func() error {
	return func() error {
		var flags uintptr

		if pidNS {
			flags |= syscall.CLONE_NEWPID
		}
		if mountNS {
			flags |= syscall.CLONE_NEWNS
		}
		if netNS {
			flags |= syscall.CLONE_NEWNET
		}

		if flags == 0 {
			return nil
		}

		if _, _, err := syscall.Syscall(syscall.SYS_UNSHARE, flags, 0, 0); err != 0 {
			return fmt.Errorf("unshare namespaces: %w", err)
		}

		return nil
	}
}
