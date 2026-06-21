package vfio

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestDiscoverReportsViableVFIOGroup(t *testing.T) {
	root := t.TempDir()
	makeVFIOGroup(t, root, 7, []fakePCIDevice{{
		address: "0000:01:00.0",
		vendor:  "0x8086",
		device:  "0x10fb",
		class:   "0x020000",
		driver:  "vfio-pci",
	}})

	devices, groups, err := Discover(filepath.Join(root, "sys"), filepath.Join(root, "dev"), Policy{
		Enabled:                  true,
		allowNonCharacterDevices: true,
	})
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(groups) != 1 || groups[0].GroupID != 7 {
		t.Fatalf("groups = %+v, want group 7", groups)
	}
	if len(devices) != 1 {
		t.Fatalf("devices = %+v, want one device", devices)
	}
	dev := devices[0]
	if dev.Address != "0000:01:00.0" || dev.VendorID != "8086" || dev.DeviceID != "10fb" || dev.Class != "020000" || dev.Driver != "vfio-pci" || dev.VFIOGroup != 7 {
		t.Fatalf("device = %+v", dev)
	}
}

func TestDiscoverSkipsGroupWithUnsafeDriver(t *testing.T) {
	root := t.TempDir()
	makeVFIOGroup(t, root, 7, []fakePCIDevice{{
		address: "0000:01:00.0",
		vendor:  "0x8086",
		device:  "0x10fb",
		class:   "0x020000",
		driver:  "ixgbe",
	}})

	devices, groups, err := Discover(filepath.Join(root, "sys"), filepath.Join(root, "dev"), Policy{
		Enabled:                  true,
		allowNonCharacterDevices: true,
	})
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(devices) != 0 || len(groups) != 0 {
		t.Fatalf("devices/groups = %+v/%+v, want none", devices, groups)
	}
}

func TestDiscoverHonorsAllowAndDenyPolicy(t *testing.T) {
	root := t.TempDir()
	makeVFIOGroup(t, root, 7, []fakePCIDevice{{
		address: "0000:01:00.0",
		vendor:  "0x8086",
		device:  "0x10fb",
		class:   "0x020000",
		driver:  "vfio-pci",
	}})
	makeVFIOGroup(t, root, 8, []fakePCIDevice{{
		address: "0000:02:00.0",
		vendor:  "0x15b3",
		device:  "0x1017",
		class:   "0x020000",
		driver:  "vfio-pci",
	}})

	_, groups, err := Discover(filepath.Join(root, "sys"), filepath.Join(root, "dev"), Policy{
		Enabled:                  true,
		AllowedGroups:            []int{7, 8},
		DeniedGroups:             []int{7},
		AllowedVendors:           []string{"15b3"},
		AllowedClasses:           []string{"0200"},
		allowNonCharacterDevices: true,
	})
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(groups) != 1 || groups[0].GroupID != 8 {
		t.Fatalf("groups = %+v, want only group 8", groups)
	}
}

func TestDiscoverMissingVFIOControlReturnsEmpty(t *testing.T) {
	root := t.TempDir()
	makeVFIOGroup(t, root, 7, []fakePCIDevice{{
		address: "0000:01:00.0",
		vendor:  "0x8086",
		device:  "0x10fb",
		class:   "0x020000",
		driver:  "vfio-pci",
	}})
	if err := os.Remove(filepath.Join(root, "dev", "vfio", "vfio")); err != nil {
		t.Fatal(err)
	}

	devices, groups, err := Discover(filepath.Join(root, "sys"), filepath.Join(root, "dev"), Policy{
		Enabled:                  true,
		allowNonCharacterDevices: true,
	})
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(devices) != 0 || len(groups) != 0 {
		t.Fatalf("devices/groups = %+v/%+v, want none", devices, groups)
	}
}

type fakePCIDevice struct {
	address string
	vendor  string
	device  string
	class   string
	driver  string
}

func makeVFIOGroup(t *testing.T, root string, group int, devices []fakePCIDevice) {
	t.Helper()

	sysRoot := filepath.Join(root, "sys")
	devRoot := filepath.Join(root, "dev", "vfio")
	groupDir := filepath.Join(sysRoot, "kernel", "iommu_groups", itoa(group))
	devicesDir := filepath.Join(groupDir, "devices")
	if err := os.MkdirAll(devicesDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(devRoot, 0755); err != nil {
		t.Fatal(err)
	}
	touch(t, filepath.Join(devRoot, "vfio"))
	touch(t, filepath.Join(devRoot, itoa(group)))

	for _, dev := range devices {
		deviceDir := filepath.Join(sysRoot, "bus", "pci", "devices", dev.address)
		if err := os.MkdirAll(deviceDir, 0755); err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(deviceDir, "vendor"), dev.vendor+"\n")
		writeFile(t, filepath.Join(deviceDir, "device"), dev.device+"\n")
		writeFile(t, filepath.Join(deviceDir, "class"), dev.class+"\n")

		driverDir := filepath.Join(sysRoot, "bus", "pci", "drivers", dev.driver)
		if err := os.MkdirAll(driverDir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join("..", "..", "drivers", dev.driver), filepath.Join(deviceDir, "driver")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join("..", "..", "..", "..", "bus", "pci", "devices", dev.address), filepath.Join(devicesDir, dev.address)); err != nil {
			t.Fatal(err)
		}
	}
}

func writeFile(t *testing.T, path, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}
}

func touch(t *testing.T, path string) {
	t.Helper()
	writeFile(t, path, "")
}

func itoa(value int) string {
	return strconv.Itoa(value)
}
