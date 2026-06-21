package nvidia

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/octopos/octopos/pkg/cluster"
)

func TestEncodeParseDeviceSpec(t *testing.T) {
	devices := []cluster.GPUDevice{
		{Index: 1, UUID: "GPU-one", Path: "/dev/nvidia1", Major: 195, Minor: 1},
		{Index: 0, UUID: "GPU-zero", Path: "/dev/nvidia0", Major: 195, Minor: 0},
	}

	spec := EncodeDeviceSpec(devices)
	got, err := ParseDeviceSpec(spec)
	if err != nil {
		t.Fatalf("ParseDeviceSpec: %v", err)
	}
	want := []cluster.GPUDevice{
		{Index: 0, UUID: "GPU-zero", Path: "/dev/nvidia0", Major: 195, Minor: 0},
		{Index: 1, UUID: "GPU-one", Path: "/dev/nvidia1", Major: 195, Minor: 1},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parsed devices = %+v, want %+v", got, want)
	}
}

func TestParseDeviceSpecDefaultsPath(t *testing.T) {
	got, err := ParseDeviceSpec("2")
	if err != nil {
		t.Fatalf("ParseDeviceSpec: %v", err)
	}
	want := []cluster.GPUDevice{{Index: 2, Path: "/dev/nvidia2"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parsed devices = %+v, want %+v", got, want)
	}
}

func TestVisibleDevicesValue(t *testing.T) {
	withUUID := []cluster.GPUDevice{
		{Index: 3, UUID: "GPU-three"},
		{Index: 5, UUID: "GPU-five"},
	}
	if got := VisibleDevicesValue(withUUID); got != "GPU-three,GPU-five" {
		t.Fatalf("VisibleDevicesValue with UUIDs = %q", got)
	}

	withoutUUID := []cluster.GPUDevice{
		{Index: 3},
		{Index: 5, UUID: "GPU-five"},
	}
	if got := VisibleDevicesValue(withoutUUID); got != "3,5" {
		t.Fatalf("VisibleDevicesValue without full UUIDs = %q", got)
	}
}

func TestEnsureProfile(t *testing.T) {
	root := t.TempDir()
	if err := EnsureProfile(root); err != nil {
		t.Fatalf("EnsureProfile: %v", err)
	}

	path := filepath.Join(root, ProfileScriptRelPath)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read profile script: %v", err)
	}
	if string(data) != ProfileScript {
		t.Fatal("profile script content mismatch")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat profile script: %v", err)
	}
	if got := info.Mode().Perm(); got != 0644 {
		t.Fatalf("profile script mode = %#o, want 0644", got)
	}
	if sh, err := exec.LookPath("sh"); err == nil {
		if out, err := exec.Command(sh, "-n", path).CombinedOutput(); err != nil {
			t.Fatalf("profile script syntax: %v\n%s", err, out)
		}
	}
}
