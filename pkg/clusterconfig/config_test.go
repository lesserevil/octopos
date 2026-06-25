package clusterconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadExecDefaultsUsesConfigValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "octoposd.yaml")
	if err := os.WriteFile(path, []byte("exec_defaults:\n  cpu_cores: 3\n  memory_gb: 8\n"), 0600); err != nil {
		t.Fatal(err)
	}

	got, err := LoadExecDefaults(path)
	if err != nil {
		t.Fatalf("LoadExecDefaults: %v", err)
	}
	if got.CPUCores != 3 || got.MemoryGB != 8 {
		t.Fatalf("defaults = %+v, want 3 CPU / 8 GB", got)
	}
}

func TestLoadExecDefaultsMissingFileUsesBuiltInDefaults(t *testing.T) {
	got, err := LoadExecDefaults(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("LoadExecDefaults: %v", err)
	}
	if got != (ExecDefaults{CPUCores: 1, MemoryGB: 1}) {
		t.Fatalf("defaults = %+v, want built-in defaults", got)
	}
}

func TestLoadExecDefaultsZeroValuesUseBuiltInDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "octoposd.yaml")
	if err := os.WriteFile(path, []byte("exec_defaults:\n  cpu_cores: 0\n  memory_gb: -1\n"), 0600); err != nil {
		t.Fatal(err)
	}

	got, err := LoadExecDefaults(path)
	if err != nil {
		t.Fatalf("LoadExecDefaults: %v", err)
	}
	if got != (ExecDefaults{CPUCores: 1, MemoryGB: 1}) {
		t.Fatalf("defaults = %+v, want built-in defaults", got)
	}
}

func TestMarshalFileIncludesExecDefaults(t *testing.T) {
	data, err := MarshalFile(File{
		NodeID:                        "node-1",
		GRPCAddr:                      "0.0.0.0:50051",
		RequireSSI:                    true,
		RemoteChildSessionCPUQuota:    12000,
		RemoteChildSessionMemoryQuota: 64 << 30,
		RemoteChildSessionGPUQuota:    2,
		VFIOEnabled:                   true,
		VFIOAllowedGroups:             []int{7},
		VFIODeniedGroups:              []int{9},
		VFIOAllowedClasses:            []string{"0200"},
		VFIOAllowedVendors:            []string{"8086"},
		ExecDefaults:                  ExecDefaults{CPUCores: 2, MemoryGB: 4},
	})
	if err != nil {
		t.Fatalf("MarshalFile: %v", err)
	}
	text := string(data)
	for _, want := range []string{
		"node_id: node-1",
		"remote_child_session_cpu_quota: 12000",
		"remote_child_session_memory_quota: 68719476736",
		"remote_child_session_gpu_quota: 2",
		"cpu_cores: 2",
		"memory_gb: 4",
		"vfio_allowed_groups:",
		"vfio_denied_groups:",
		"vfio_allowed_classes:",
		"vfio_allowed_vendors:",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("config missing %q:\n%s", want, text)
		}
	}
}
