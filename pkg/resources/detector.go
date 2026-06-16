package resources

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/octopos/octopos/pkg/cluster"
)

// Detector gathers hardware resource information
type Detector struct {
	procPath string
	sysPath  string
}

// NewDetector creates a new resource detector
func NewDetector(procPath, sysPath string) *Detector {
	if procPath == "" {
		procPath = "/proc"
	}
	if sysPath == "" {
		sysPath = "/sys"
	}
	return &Detector{
		procPath: procPath,
		sysPath:  sysPath,
	}
}

// DetectCPU returns CPU topology information
func (d *Detector) DetectCPU() (*cluster.ResourceSpec, error) {
	cpuinfo, err := os.ReadFile(filepath.Join(d.procPath, "cpuinfo"))
	if err != nil {
		return nil, err
	}

	spec := &cluster.ResourceSpec{}

	// Parse /proc/cpuinfo
	scanner := bufio.NewScanner(strings.NewReader(string(cpuinfo)))
	var physicalIDs, coreIDs map[string]bool
	physicalIDs = make(map[string]bool)
	coreIDs = make(map[string]bool)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "processor\t:") {
			spec.CPU += 1000 // 1 core = 1000 millicores
		} else if strings.HasPrefix(line, "physical id\t:") {
			parts := strings.Split(line, ":")
			if len(parts) == 2 {
				physicalIDs[strings.TrimSpace(parts[1])] = true
			}
		} else if strings.HasPrefix(line, "core id\t:") {
			parts := strings.Split(line, ":")
			if len(parts) == 2 {
				coreIDs[strings.TrimSpace(parts[1])] = true
			}
		}
	}

	// Detect NUMA nodes
	_, _ = d.detectNUMANodes()

	return spec, nil
}

// DetectMemory returns memory information
func (d *Detector) DetectMemory() (int64, error) {
	meminfo, err := os.ReadFile(filepath.Join(d.procPath, "meminfo"))
	if err != nil {
		return 0, err
	}

	scanner := bufio.NewScanner(strings.NewReader(string(meminfo)))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, _ := strconv.ParseInt(fields[1], 10, 64)
				return kb * 1024, nil // Convert KB to bytes
			}
		}
	}
	return 0, nil
}

// DetectAll returns complete resource specification
func (d *Detector) DetectAll() (*cluster.ResourceSpec, error) {
	cpuSpec, err := d.DetectCPU()
	if err != nil {
		return nil, err
	}

	mem, err := d.DetectMemory()
	if err != nil {
		return nil, err
	}

	cpuSpec.Memory = mem

	// Detect NUMA nodes
	numaNodes, _ := d.detectNUMANodes()
	cpuSpec.NUMANodes = numaNodes

	// Detect GPUs
	gpuCount, _ := d.detectGPUs()
	cpuSpec.GPUCount = gpuCount

	return cpuSpec, nil
}

func (d *Detector) detectNUMANodes() (int, error) {
	numaPath := filepath.Join(d.sysPath, "devices", "system", "node")
	entries, err := os.ReadDir(numaPath)
	if err != nil {
		return 0, nil // NUMA not available
	}
	count := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "node") {
			count++
		}
	}
	return count, nil
}

func (d *Detector) detectGPUs() (int, error) {
	// Check for NVIDIA GPUs via nvidia-smi or /proc/driver/nvidia
	if _, err := os.Stat("/usr/bin/nvidia-smi"); err == nil {
		// Could run nvidia-smi --query-gpu=count --format=csv,noheader
		// For now return 0, will be enhanced with PCI detection
	}

	// Check PCI devices for GPU class
	pciPath := filepath.Join(d.sysPath, "bus", "pci", "devices")
	entries, err := os.ReadDir(pciPath)
	if err != nil {
		return 0, nil
	}

	count := 0
	for _, e := range entries {
		classPath := filepath.Join(pciPath, e.Name(), "class")
		if data, err := os.ReadFile(classPath); err == nil {
			class := strings.TrimSpace(string(data))
			// 0x030000 = VGA compatible controller, 0x030200 = 3D controller
			if class == "0x030000" || class == "0x030200" {
				count++
			}
		}
	}
	return count, nil
}
