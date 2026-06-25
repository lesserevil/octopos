package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/octopos/octopos/pkg/clusterconfig"
)

func TestLoadConfigFileParsesGeneratedClusterConfig(t *testing.T) {
	data, err := clusterconfig.MarshalFile(clusterconfig.File{
		NodeID:                        "node-1",
		GRPCAddr:                      "0.0.0.0:50051",
		WGInterface:                   "wg-octopos",
		JuiceFSMount:                  "/cluster",
		SSIRootFS:                     "/cluster",
		RequireSSI:                    true,
		ChildLease:                    "2m",
		ChildTokenTTL:                 "24h",
		RemoteChildSessionCPUQuota:    12000,
		RemoteChildSessionMemoryQuota: 64 << 30,
		RemoteChildSessionGPUQuota:    2,
		VFIOEnabled:                   true,
		VFIOAllowedGroups:             []int{7},
		VFIODeniedGroups:              []int{9},
		VFIOAllowedClasses:            []string{"0200"},
		VFIOAllowedVendors:            []string{"8086"},
		ExecDefaults:                  clusterconfig.ExecDefaults{CPUCores: 3, MemoryGB: 8},
	})
	if err != nil {
		t.Fatalf("MarshalFile: %v", err)
	}
	path := filepath.Join(t.TempDir(), "octoposd.yaml")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}

	cfg := &Config{
		ChildLease:    time.Minute,
		ChildTokenTTL: time.Hour,
		ExecDefaults:  clusterconfig.DefaultExecDefaults(),
	}
	if err := loadConfigFile(path, cfg); err != nil {
		t.Fatalf("loadConfigFile: %v\n%s", err, data)
	}
	if cfg.NodeID != "node-1" || cfg.GRPCAddr != "0.0.0.0:50051" {
		t.Fatalf("config identity = %q/%q", cfg.NodeID, cfg.GRPCAddr)
	}
	if cfg.ChildLease != 2*time.Minute || cfg.ChildTokenTTL != 24*time.Hour {
		t.Fatalf("durations = %s/%s", cfg.ChildLease, cfg.ChildTokenTTL)
	}
	if cfg.RemoteChildSessionCPUQuota != 12000 || cfg.RemoteChildSessionMemoryQuota != 64<<30 || cfg.RemoteChildSessionGPUQuota != 2 {
		t.Fatalf("remote child quotas = cpu %d memory %d gpu %d", cfg.RemoteChildSessionCPUQuota, cfg.RemoteChildSessionMemoryQuota, cfg.RemoteChildSessionGPUQuota)
	}
	if cfg.ExecDefaults.CPUCores != 3 || cfg.ExecDefaults.MemoryGB != 8 {
		t.Fatalf("exec defaults = %+v", cfg.ExecDefaults)
	}
	if !cfg.VFIOEnabled || len(cfg.VFIOAllowedGroups) != 1 || cfg.VFIOAllowedGroups[0] != 7 {
		t.Fatalf("VFIO allow config = enabled %v groups %+v", cfg.VFIOEnabled, cfg.VFIOAllowedGroups)
	}
	if len(cfg.VFIODeniedGroups) != 1 || cfg.VFIODeniedGroups[0] != 9 {
		t.Fatalf("VFIO deny config = %+v", cfg.VFIODeniedGroups)
	}
	if len(cfg.VFIOAllowedClasses) != 1 || cfg.VFIOAllowedClasses[0] != "0200" {
		t.Fatalf("VFIO classes = %+v", cfg.VFIOAllowedClasses)
	}
	if len(cfg.VFIOAllowedVendors) != 1 || cfg.VFIOAllowedVendors[0] != "8086" {
		t.Fatalf("VFIO vendors = %+v", cfg.VFIOAllowedVendors)
	}
}

func TestRPCServerOptionsIncludesRemoteChildSessionQuotas(t *testing.T) {
	server := &Server{config: &Config{
		JuiceFSMount:                  "/cluster",
		SSIRootFS:                     "/cluster",
		ChildState:                    "/var/lib/octopos/remote-children.json",
		ChildLease:                    3 * time.Minute,
		ChildTokenTTL:                 6 * time.Hour,
		RemoteChildSessionCPUQuota:    16000,
		RemoteChildSessionMemoryQuota: 128 << 30,
		RemoteChildSessionGPUQuota:    4,
		VFIOState:                     "/var/lib/octopos/vfio-allocations.json",
	}}

	opts := server.rpcServerOptions()
	if opts.MaxRemoteChildSessionCPU != 16000 {
		t.Fatalf("CPU quota = %d, want 16000", opts.MaxRemoteChildSessionCPU)
	}
	if opts.MaxRemoteChildSessionMemory != 128<<30 {
		t.Fatalf("memory quota = %d, want %d", opts.MaxRemoteChildSessionMemory, int64(128<<30))
	}
	if opts.MaxRemoteChildSessionGPUs != 4 {
		t.Fatalf("GPU quota = %d, want 4", opts.MaxRemoteChildSessionGPUs)
	}
	if opts.RemoteChildStorePath != "/var/lib/octopos/remote-children.json" || opts.RemoteChildLeaseTimeout != 3*time.Minute || opts.RemoteChildTokenTTL != 6*time.Hour {
		t.Fatalf("remote child lifecycle options = path %q lease %s ttl %s", opts.RemoteChildStorePath, opts.RemoteChildLeaseTimeout, opts.RemoteChildTokenTTL)
	}
	if opts.VFIOAllocationStorePath != "/var/lib/octopos/vfio-allocations.json" {
		t.Fatalf("VFIO state = %q", opts.VFIOAllocationStorePath)
	}
}
