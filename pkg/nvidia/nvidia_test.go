package nvidia

import (
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
