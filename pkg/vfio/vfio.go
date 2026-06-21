package vfio

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/octopos/octopos/pkg/cluster"
	"golang.org/x/sys/unix"
)

const (
	defaultSysRoot = "/sys"
	defaultDevRoot = "/dev"
)

var defaultSafeDrivers = []string{
	"vfio-pci",
	"vfio-platform",
	"vfio-ap",
	"vfio_ccw",
}

// Policy controls which locally discovered VFIO groups are advertised.
type Policy struct {
	Enabled        bool
	AllowedGroups  []int
	DeniedGroups   []int
	AllowedClasses []string
	AllowedVendors []string
	SafeDrivers    []string

	allowNonCharacterDevices bool
}

// Discover returns PCI devices that belong to viable VFIO groups and the
// corresponding allocatable groups.
func Discover(sysRoot, devRoot string, policy Policy) ([]cluster.PCIDevice, []cluster.VFIOGroup, error) {
	if !policy.Enabled {
		return nil, nil, nil
	}
	if sysRoot == "" {
		sysRoot = defaultSysRoot
	}
	if devRoot == "" {
		devRoot = defaultDevRoot
	}
	if len(policy.SafeDrivers) == 0 {
		policy.SafeDrivers = defaultSafeDrivers
	}

	groupsDir := filepath.Join(sysRoot, "kernel", "iommu_groups")
	entries, err := os.ReadDir(groupsDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("read VFIO iommu groups: %w", err)
	}
	if !vfioControlDeviceAvailable(filepath.Join(devRoot, "vfio", "vfio"), policy.allowNonCharacterDevices) {
		return nil, nil, nil
	}

	var devices []cluster.PCIDevice
	var groups []cluster.VFIOGroup
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		groupID, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		if !policy.groupAllowed(groupID) {
			continue
		}
		if !vfioControlDeviceAvailable(filepath.Join(devRoot, "vfio", entry.Name()), policy.allowNonCharacterDevices) {
			continue
		}

		groupDevices, err := discoverGroupDevices(filepath.Join(groupsDir, entry.Name(), "devices"), groupID)
		if err != nil {
			return nil, nil, err
		}
		if len(groupDevices) == 0 {
			continue
		}
		if !policy.groupMatches(groupDevices) {
			continue
		}
		if !policy.groupDriversSafe(groupDevices) {
			continue
		}

		devices = append(devices, groupDevices...)
		groups = append(groups, cluster.VFIOGroup{
			GroupID: groupID,
			Devices: append([]cluster.PCIDevice(nil),
				groupDevices...),
		})
	}

	sort.Slice(devices, func(i, j int) bool {
		if devices[i].VFIOGroup == devices[j].VFIOGroup {
			return devices[i].Address < devices[j].Address
		}
		return devices[i].VFIOGroup < devices[j].VFIOGroup
	})
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].GroupID < groups[j].GroupID
	})
	return devices, groups, nil
}

func discoverGroupDevices(devicesDir string, groupID int) ([]cluster.PCIDevice, error) {
	entries, err := os.ReadDir(devicesDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read VFIO group devices %s: %w", devicesDir, err)
	}

	devices := make([]cluster.PCIDevice, 0, len(entries))
	for _, entry := range entries {
		path := filepath.Join(devicesDir, entry.Name())
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil {
			resolved = path
		}
		dev, err := readPCIDevice(resolved, entry.Name(), groupID)
		if err != nil {
			return nil, err
		}
		devices = append(devices, dev)
	}
	sort.Slice(devices, func(i, j int) bool {
		return devices[i].Address < devices[j].Address
	})
	return devices, nil
}

func readPCIDevice(path, address string, groupID int) (cluster.PCIDevice, error) {
	dev := cluster.PCIDevice{
		Address:    address,
		IOMMUGroup: groupID,
		VFIOGroup:  groupID,
	}
	var err error
	if dev.VendorID, err = readHexID(path, "vendor"); err != nil {
		return dev, err
	}
	if dev.DeviceID, err = readHexID(path, "device"); err != nil {
		return dev, err
	}
	if dev.Class, err = readHexID(path, "class"); err != nil {
		return dev, err
	}
	dev.Driver = readDriverName(path)
	return dev, nil
}

func readHexID(path, name string) (string, error) {
	data, err := os.ReadFile(filepath.Join(path, name))
	if err != nil {
		return "", fmt.Errorf("read PCI %s for %s: %w", name, path, err)
	}
	value := strings.ToLower(strings.TrimSpace(string(data)))
	value = strings.TrimPrefix(value, "0x")
	return value, nil
}

func readDriverName(path string) string {
	link := filepath.Join(path, "driver")
	target, err := os.Readlink(link)
	if err != nil {
		return ""
	}
	return filepath.Base(target)
}

func vfioControlDeviceAvailable(path string, allowNonCharacter bool) bool {
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		return false
	}
	if st.Mode&unix.S_IFMT == unix.S_IFCHR {
		return true
	}
	return allowNonCharacter
}

func (p Policy) groupAllowed(groupID int) bool {
	for _, denied := range p.DeniedGroups {
		if denied == groupID {
			return false
		}
	}
	if len(p.AllowedGroups) == 0 {
		return true
	}
	for _, allowed := range p.AllowedGroups {
		if allowed == groupID {
			return true
		}
	}
	return false
}

func (p Policy) groupMatches(devices []cluster.PCIDevice) bool {
	if len(p.AllowedClasses) == 0 && len(p.AllowedVendors) == 0 {
		return true
	}
	for _, dev := range devices {
		if len(p.AllowedClasses) > 0 && !matchesPrefix(p.AllowedClasses, dev.Class) {
			continue
		}
		if len(p.AllowedVendors) > 0 && !matchesExact(p.AllowedVendors, dev.VendorID) {
			continue
		}
		return true
	}
	return false
}

func (p Policy) groupDriversSafe(devices []cluster.PCIDevice) bool {
	for _, dev := range devices {
		if !matchesExact(p.SafeDrivers, dev.Driver) {
			return false
		}
	}
	return true
}

func matchesPrefix(allowed []string, value string) bool {
	value = normalizeSelector(value)
	for _, item := range allowed {
		if strings.HasPrefix(value, normalizeSelector(item)) {
			return true
		}
	}
	return false
}

func matchesExact(allowed []string, value string) bool {
	value = normalizeSelector(value)
	for _, item := range allowed {
		if normalizeSelector(item) == value {
			return true
		}
	}
	return false
}

func normalizeSelector(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	return strings.TrimPrefix(value, "0x")
}
