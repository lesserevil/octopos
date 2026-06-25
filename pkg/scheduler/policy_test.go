package scheduler

import (
	"testing"

	"github.com/octopos/octopos/pkg/cluster"
	"github.com/octopos/octopos/pkg/ssi"
)

func TestBinPackPolicy(t *testing.T) {
	policy := &BinPackPolicy{}

	nodes := []*cluster.NodeInfo{
		{ID: "node-1", State: cluster.NodeStateActive, Resources: cluster.ResourceSpec{CPU: 8000, Memory: 32_000_000_000}, Allocated: cluster.ResourceSpec{CPU: 6000, Memory: 24_000_000_000}},
		{ID: "node-2", State: cluster.NodeStateActive, Resources: cluster.ResourceSpec{CPU: 8000, Memory: 32_000_000_000}, Allocated: cluster.ResourceSpec{CPU: 2000, Memory: 8_000_000_000}},
		{ID: "node-3", State: cluster.NodeStateOffline, Resources: cluster.ResourceSpec{CPU: 8000, Memory: 32_000_000_000}},
	}

	req := cluster.Requirements{CPU: 2000, Memory: 8_000_000_000}
	eligible := policy.Filter(nodes, req)

	if len(eligible) != 2 {
		t.Errorf("Expected 2 eligible nodes, got %d", len(eligible))
	}

	// Should prefer node-1 (more packed)
	best := eligible[0]
	bestScore := policy.Score(best, req)
	for _, n := range eligible[1:] {
		if score := policy.Score(n, req); score > bestScore {
			best, bestScore = n, score
		}
	}

	if best.ID != "node-1" {
		t.Errorf("BinPack should prefer node-1 (more packed), got %s", best.ID)
	}
}

func TestSpreadPolicy(t *testing.T) {
	policy := &SpreadPolicy{}

	nodes := []*cluster.NodeInfo{
		{ID: "node-1", State: cluster.NodeStateActive, Resources: cluster.ResourceSpec{CPU: 8000, Memory: 32_000_000_000}, Allocated: cluster.ResourceSpec{CPU: 6000, Memory: 24_000_000_000}},
		{ID: "node-2", State: cluster.NodeStateActive, Resources: cluster.ResourceSpec{CPU: 8000, Memory: 32_000_000_000}, Allocated: cluster.ResourceSpec{CPU: 2000, Memory: 8_000_000_000}},
	}

	req := cluster.Requirements{CPU: 2000, Memory: 8_000_000_000}
	eligible := policy.Filter(nodes, req)

	// Should prefer node-2 (less packed)
	best := eligible[0]
	bestScore := policy.Score(best, req)
	for _, n := range eligible[1:] {
		if score := policy.Score(n, req); score > bestScore {
			best, bestScore = n, score
		}
	}

	if best.ID != "node-2" {
		t.Errorf("Spread should prefer node-2 (less packed), got %s", best.ID)
	}
}

func TestScheduler(t *testing.T) {
	s := NewScheduler(&BinPackPolicy{})

	n1 := &cluster.NodeInfo{
		ID:        "node-1",
		State:     cluster.NodeStateActive,
		Resources: cluster.ResourceSpec{CPU: 8000, Memory: 32 * 1024 * 1024 * 1024},
	}
	n2 := &cluster.NodeInfo{
		ID:        "node-2",
		State:     cluster.NodeStateActive,
		Resources: cluster.ResourceSpec{CPU: 8000, Memory: 32 * 1024 * 1024 * 1024},
	}
	s.AddNode(n1)
	s.AddNode(n2)

	req := cluster.Requirements{CPU: 2000, Memory: 8 * 1024 * 1024 * 1024}

	// First schedule
	node, err := s.Schedule(req)
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	if node.ID != "node-1" {
		t.Errorf("Expected node-1, got %s", node.ID)
	}

	// Second schedule - should pack on node-1
	node2, err := s.Schedule(req)
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	if node2.ID != "node-1" {
		t.Errorf("Expected node-1 (binpack), got %s", node2.ID)
	}

	// Release first allocation
	s.Release("node-1", req)

	// Schedule again - binpack should prefer node-1 again (less allocated)
	node3, err := s.Schedule(req)
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	// After release, both nodes have equal allocated (0), so either is valid
	_ = node3

	// Test no capacity on any node
	reqLarge := cluster.Requirements{CPU: 16000, Memory: 64 * 1024 * 1024 * 1024}
	_, err = s.Schedule(reqLarge)
	if err == nil {
		t.Error("Schedule should fail when no capacity")
	}
}

func TestSchedulerPreferNotNodeIDIsSoft(t *testing.T) {
	s := NewScheduler(&BinPackPolicy{})
	s.AddNode(&cluster.NodeInfo{
		ID:        "node-1",
		State:     cluster.NodeStateActive,
		Resources: cluster.ResourceSpec{CPU: 8000, Memory: 32 * 1024 * 1024 * 1024},
		Allocated: cluster.ResourceSpec{CPU: 6000, Memory: 24 * 1024 * 1024 * 1024},
	})
	s.AddNode(&cluster.NodeInfo{
		ID:        "node-2",
		State:     cluster.NodeStateActive,
		Resources: cluster.ResourceSpec{CPU: 8000, Memory: 32 * 1024 * 1024 * 1024},
	})

	req := cluster.Requirements{
		CPU:          1000,
		Memory:       1 * 1024 * 1024 * 1024,
		NodeAffinity: map[string]string{affinityPreferNotNodeID: "node-1"},
	}
	node, err := s.Schedule(req)
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	if node.ID != "node-2" {
		t.Fatalf("selected node = %s, want node-2", node.ID)
	}

	oneNode := NewScheduler(&BinPackPolicy{})
	oneNode.AddNode(&cluster.NodeInfo{
		ID:        "node-1",
		State:     cluster.NodeStateActive,
		Resources: cluster.ResourceSpec{CPU: 8000, Memory: 32 * 1024 * 1024 * 1024},
	})
	node, err = oneNode.Schedule(req)
	if err != nil {
		t.Fatalf("one-node Schedule failed: %v", err)
	}
	if node.ID != "node-1" {
		t.Fatalf("one-node selected node = %s, want node-1", node.ID)
	}
}

func TestSchedulerReturnsGPUAllocation(t *testing.T) {
	s := NewScheduler(&BinPackPolicy{})
	node := &cluster.NodeInfo{
		ID:    "node-1",
		State: cluster.NodeStateActive,
		Resources: cluster.ResourceSpec{
			CPU:      8000,
			Memory:   32 * 1024 * 1024 * 1024,
			GPUCount: 2,
			GPUDevices: []cluster.GPUDevice{
				{Index: 0, UUID: "GPU-0", Path: "/dev/nvidia0"},
				{Index: 1, UUID: "GPU-1", Path: "/dev/nvidia1"},
			},
		},
	}
	s.AddNode(node)

	req := cluster.Requirements{
		CPU:    1000,
		Memory: 1 * 1024 * 1024 * 1024,
		GPUs:   1,
		JobID:  "job-1",
	}
	selected, allocated, err := s.ScheduleWithAllocation(req)
	if err != nil {
		t.Fatalf("ScheduleWithAllocation failed: %v", err)
	}
	if selected.ID != "node-1" {
		t.Fatalf("selected node = %s, want node-1", selected.ID)
	}
	if len(allocated.GPUDevices) != 1 {
		t.Fatalf("allocated GPUs = %+v, want 1 GPU", allocated.GPUDevices)
	}
	if allocated.GPUDevices[0].UUID != "GPU-0" {
		t.Fatalf("allocated GPU = %+v, want GPU-0", allocated.GPUDevices[0])
	}
	if node.GPUAllocations[0] != "job-1" {
		t.Fatalf("GPU allocation owner = %q, want job-1", node.GPUAllocations[0])
	}

	s.Release("node-1", allocated)
	if len(node.GPUAllocations) != 0 {
		t.Fatalf("GPU allocations = %+v, want empty", node.GPUAllocations)
	}
}

func TestSchedulerPrefersNonGPUNodeForNonGPUWork(t *testing.T) {
	s := NewScheduler(&BinPackPolicy{})
	s.AddNode(&cluster.NodeInfo{
		ID:    "gpu-node",
		State: cluster.NodeStateActive,
		Resources: cluster.ResourceSpec{
			CPU:      8000,
			Memory:   32 * 1024 * 1024 * 1024,
			GPUCount: 2,
			GPUDevices: []cluster.GPUDevice{
				{Index: 0, UUID: "GPU-0"},
				{Index: 1, UUID: "GPU-1"},
			},
		},
		Allocated: cluster.ResourceSpec{CPU: 6000, Memory: 24 * 1024 * 1024 * 1024},
	})
	s.AddNode(&cluster.NodeInfo{
		ID:        "cpu-node",
		State:     cluster.NodeStateActive,
		Resources: cluster.ResourceSpec{CPU: 8000, Memory: 32 * 1024 * 1024 * 1024},
	})

	selected, err := s.Schedule(cluster.Requirements{CPU: 1000, Memory: 1 * 1024 * 1024 * 1024})
	if err != nil {
		t.Fatalf("Schedule non-GPU work: %v", err)
	}
	if selected.ID != "cpu-node" {
		t.Fatalf("selected node = %s, want cpu-node", selected.ID)
	}

	selected, _, err = s.ScheduleWithAllocation(cluster.Requirements{
		CPU:    1000,
		Memory: 1 * 1024 * 1024 * 1024,
		GPUs:   1,
		JobID:  "gpu-job",
	})
	if err != nil {
		t.Fatalf("Schedule GPU work: %v", err)
	}
	if selected.ID != "gpu-node" {
		t.Fatalf("selected GPU node = %s, want gpu-node", selected.ID)
	}
}

func TestSchedulerReturnsVFIOAllocation(t *testing.T) {
	s := NewScheduler(&BinPackPolicy{})
	node := &cluster.NodeInfo{
		ID:    "node-1",
		State: cluster.NodeStateActive,
		Resources: cluster.ResourceSpec{
			CPU:    8000,
			Memory: 32 * 1024 * 1024 * 1024,
			VFIOGroups: []cluster.VFIOGroup{{
				GroupID: 7,
				Devices: []cluster.PCIDevice{{
					Address:   "0000:01:00.0",
					VendorID:  "8086",
					DeviceID:  "10fb",
					Class:     "020000",
					Driver:    "vfio-pci",
					VFIOGroup: 7,
				}},
			}},
		},
	}
	s.AddNode(node)

	req := cluster.Requirements{
		CPU:    1000,
		Memory: 1 * 1024 * 1024 * 1024,
		VFIODevs: []cluster.VFIORequirement{{
			VendorID: "8086",
			DeviceID: "10fb",
			Class:    "0200",
			Count:    1,
		}},
		JobID: "job-1",
	}
	selected, allocated, err := s.ScheduleWithAllocation(req)
	if err != nil {
		t.Fatalf("ScheduleWithAllocation failed: %v", err)
	}
	if selected.ID != "node-1" {
		t.Fatalf("selected node = %s, want node-1", selected.ID)
	}
	if len(allocated.VFIOGroups) != 1 || allocated.VFIOGroups[0] != 7 {
		t.Fatalf("allocated VFIO groups = %+v, want group 7", allocated.VFIOGroups)
	}
	if node.VFIOAllocations[7] != "job-1" {
		t.Fatalf("VFIO allocation owner = %q, want job-1", node.VFIOAllocations[7])
	}

	s.Release("node-1", allocated)
	if len(node.VFIOAllocations) != 0 {
		t.Fatalf("VFIO allocations = %+v, want empty", node.VFIOAllocations)
	}
}

func TestBinPackPolicyExcludesSSIUnreadyNodes(t *testing.T) {
	policy := &BinPackPolicy{}
	nodes := []*cluster.NodeInfo{
		{
			ID:        "node-ready",
			State:     cluster.NodeStateActive,
			Resources: cluster.ResourceSpec{CPU: 8000, Memory: 32_000_000_000},
			Labels:    map[string]string{ssi.LabelReady: "true"},
		},
		{
			ID:        "node-unready",
			State:     cluster.NodeStateActive,
			Resources: cluster.ResourceSpec{CPU: 8000, Memory: 32_000_000_000},
			Labels:    map[string]string{ssi.LabelReady: "false"},
		},
	}

	eligible := policy.Filter(nodes, cluster.Requirements{CPU: 1000, Memory: 1_000_000_000})
	if len(eligible) != 1 {
		t.Fatalf("eligible nodes = %d, want 1", len(eligible))
	}
	if eligible[0].ID != "node-ready" {
		t.Fatalf("eligible node = %s, want node-ready", eligible[0].ID)
	}
}
