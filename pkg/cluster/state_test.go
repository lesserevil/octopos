package cluster

import (
	"testing"
	"time"
)

func TestNodeStateMachine(t *testing.T) {
	sm := NewNodeStateMachine("test-node")

	// Initial state should be Offline
	if sm.State() != NodeStateOffline {
		t.Errorf("Initial state should be Offline, got %s", sm.State())
	}

	// Valid transitions from Offline
	if err := sm.Transition(NodeStateActive); err != nil {
		t.Errorf("Offline -> Active should succeed: %v", err)
	}
	if sm.State() != NodeStateActive {
		t.Errorf("State should be Active, got %s", sm.State())
	}

	// Active -> Draining
	if err := sm.Transition(NodeStateDraining); err != nil {
		t.Errorf("Active -> Draining should succeed: %v", err)
	}
	if sm.State() != NodeStateDraining {
		t.Errorf("State should be Draining, got %s", sm.State())
	}

	// Draining -> Drained
	if err := sm.Transition(NodeStateDrained); err != nil {
		t.Errorf("Draining -> Drained should succeed: %v", err)
	}
	if sm.State() != NodeStateDrained {
		t.Errorf("State should be Drained, got %s", sm.State())
	}

	// Drained -> Active
	if err := sm.Transition(NodeStateActive); err != nil {
		t.Errorf("Drained -> Active should succeed: %v", err)
	}

	// Invalid transition: Draining -> Maintenance (not allowed)
	sm2 := NewNodeStateMachine("test-node-2")
	sm2.Transition(NodeStateActive)
	sm2.Transition(NodeStateDraining)
	if err := sm2.Transition(NodeStateMaintenance); err == nil {
		t.Errorf("Draining -> Maintenance should fail")
	}

	// Valid: Draining -> Offline (allowed for emergency)
	if err := sm2.Transition(NodeStateOffline); err != nil {
		t.Errorf("Draining -> Offline should succeed: %v", err)
	}
}

func TestHeartbeatTracker(t *testing.T) {
	tracker := NewHeartbeatTracker(100 * time.Millisecond)
	nodeID := "test-node"

	// Initially not alive
	if tracker.IsAlive(nodeID) {
		t.Error("Node should not be alive initially")
	}

	// Record heartbeat
	tracker.RecordHeartbeat(nodeID)
	if !tracker.IsAlive(nodeID) {
		t.Error("Node should be alive after heartbeat")
	}

	// Wait for expiry
	time.Sleep(150 * time.Millisecond)
	if tracker.IsAlive(nodeID) {
		t.Error("Node should not be alive after threshold")
	}

	// Remove node
	tracker.RecordHeartbeat(nodeID)
	tracker.RemoveNode(nodeID)
	if tracker.IsAlive(nodeID) {
		t.Error("Node should not be alive after removal")
	}
}

func TestNodeInfoReserveRelease(t *testing.T) {
	node := &NodeInfo{
		ID:        "node-1",
		State:     NodeStateActive,
		Resources: ResourceSpec{CPU: 8000, Memory: 32_000_000_000, GPUCount: 4},
	}

	req := Requirements{CPU: 2000, Memory: 8_000_000_000, GPUs: 1}

	// Should succeed
	if !node.Reserve(req) {
		t.Error("Reserve should succeed")
	}
	if node.Allocated.CPU != 2000 {
		t.Errorf("Allocated CPU should be 2000, got %d", node.Allocated.CPU)
	}
	if node.Allocated.Memory != 8_000_000_000 {
		t.Errorf("Allocated Memory should be 8GB, got %d", node.Allocated.Memory)
	}
	if node.Allocated.GPUCount != 1 {
		t.Errorf("Allocated GPU should be 1, got %d", node.Allocated.GPUCount)
	}

	// Second reserve
	if !node.Reserve(req) {
		t.Error("Second reserve should succeed")
	}
	if node.Allocated.CPU != 4000 {
		t.Errorf("Allocated CPU should be 4000, got %d", node.Allocated.CPU)
	}

	// Exceed CPU
	reqLarge := Requirements{CPU: 5000, Memory: 1_000_000_000}
	if node.Reserve(reqLarge) {
		t.Error("Reserve should fail when exceeding capacity")
	}

	// Release
	node.Release(req)
	if node.Allocated.CPU != 2000 {
		t.Errorf("After release CPU should be 2000, got %d", node.Allocated.CPU)
	}

	// Release more than allocated (should not go negative)
	node.Release(req)
	node.Release(req)
	if node.Allocated.CPU != 0 {
		t.Errorf("CPU should not go negative, got %d", node.Allocated.CPU)
	}

	// Inactive node should not allow reserve
	nodeInactive := &NodeInfo{
		ID:        "node-2",
		State:     NodeStateDraining,
		Resources: ResourceSpec{CPU: 8000, Memory: 32_000_000_000},
	}
	if nodeInactive.Reserve(req) {
		t.Error("Inactive node should not allow reserve")
	}
}

func TestNodeInfoReserveWithGPUAllocation(t *testing.T) {
	node := &NodeInfo{
		ID:    "node-1",
		State: NodeStateActive,
		Resources: ResourceSpec{
			CPU:      8000,
			Memory:   32_000_000_000,
			GPUCount: 2,
			GPUDevices: []GPUDevice{
				{Index: 0, UUID: "GPU-0", Path: "/dev/nvidia0", Major: 195, Minor: 0},
				{Index: 1, UUID: "GPU-1", Path: "/dev/nvidia1", Major: 195, Minor: 1},
			},
		},
	}

	req1 := Requirements{CPU: 1000, Memory: 1_000_000_000, GPUs: 1, JobID: "job-1"}
	alloc1, ok := node.ReserveWithAllocation(req1)
	if !ok {
		t.Fatal("first GPU reserve failed")
	}
	if len(alloc1.GPUDevices) != 1 || alloc1.GPUDevices[0].Index != 0 {
		t.Fatalf("first allocation = %+v, want GPU index 0", alloc1.GPUDevices)
	}

	req2 := Requirements{CPU: 1000, Memory: 1_000_000_000, GPUs: 1, JobID: "job-2"}
	alloc2, ok := node.ReserveWithAllocation(req2)
	if !ok {
		t.Fatal("second GPU reserve failed")
	}
	if len(alloc2.GPUDevices) != 1 || alloc2.GPUDevices[0].Index != 1 {
		t.Fatalf("second allocation = %+v, want GPU index 1", alloc2.GPUDevices)
	}

	req3 := Requirements{CPU: 1000, Memory: 1_000_000_000, GPUs: 1, JobID: "job-3"}
	if _, ok := node.ReserveWithAllocation(req3); ok {
		t.Fatal("third GPU reserve should fail while both GPUs are allocated")
	}

	node.Release(alloc1)
	alloc3, ok := node.ReserveWithAllocation(req3)
	if !ok {
		t.Fatal("reserve after releasing GPU 0 failed")
	}
	if len(alloc3.GPUDevices) != 1 || alloc3.GPUDevices[0].Index != 0 {
		t.Fatalf("allocation after release = %+v, want GPU index 0", alloc3.GPUDevices)
	}

	node.Release(alloc2)
	node.Release(alloc3)
	if node.Allocated.GPUCount != 0 {
		t.Fatalf("allocated GPU count = %d, want 0", node.Allocated.GPUCount)
	}
	if len(node.GPUAllocations) != 0 {
		t.Fatalf("GPU allocations = %+v, want empty", node.GPUAllocations)
	}
}

func TestNodeInfoReserveWithVFIOAllocation(t *testing.T) {
	node := &NodeInfo{
		ID:    "node-1",
		State: NodeStateActive,
		Resources: ResourceSpec{
			CPU:    8000,
			Memory: 32_000_000_000,
			VFIOGroups: []VFIOGroup{
				{
					GroupID: 7,
					Devices: []PCIDevice{{
						Address:   "0000:01:00.0",
						VendorID:  "8086",
						DeviceID:  "10fb",
						Class:     "020000",
						Driver:    "vfio-pci",
						VFIOGroup: 7,
					}},
				},
				{
					GroupID: 8,
					Devices: []PCIDevice{{
						Address:   "0000:02:00.0",
						VendorID:  "15b3",
						DeviceID:  "1017",
						Class:     "020000",
						Driver:    "vfio-pci",
						VFIOGroup: 8,
					}},
				},
			},
		},
	}

	req1 := Requirements{
		CPU:    1000,
		Memory: 1_000_000_000,
		VFIODevs: []VFIORequirement{{
			VendorID: "8086",
			Class:    "0200",
			Count:    1,
		}},
		JobID: "job-1",
	}
	alloc1, ok := node.ReserveWithAllocation(req1)
	if !ok {
		t.Fatal("first VFIO reserve failed")
	}
	if len(alloc1.VFIOGroups) != 1 || alloc1.VFIOGroups[0] != 7 {
		t.Fatalf("VFIO allocation = %+v, want group 7", alloc1.VFIOGroups)
	}
	if node.VFIOAllocations[7] != "job-1" {
		t.Fatalf("VFIO owner = %q, want job-1", node.VFIOAllocations[7])
	}
	if node.Resources.VFIOGroups[0].ClaimedBy != "job-1" {
		t.Fatalf("group claim = %q, want job-1", node.Resources.VFIOGroups[0].ClaimedBy)
	}

	req2 := Requirements{
		CPU:    1000,
		Memory: 1_000_000_000,
		VFIOGroups: []int{
			7,
		},
		JobID: "job-2",
	}
	if _, ok := node.ReserveWithAllocation(req2); ok {
		t.Fatal("explicit reserve of claimed group should fail")
	}

	req3 := Requirements{
		CPU:    1000,
		Memory: 1_000_000_000,
		VFIOGroups: []int{
			8,
		},
		JobID: "job-3",
	}
	alloc3, ok := node.ReserveWithAllocation(req3)
	if !ok {
		t.Fatal("explicit reserve of free group failed")
	}
	if len(alloc3.VFIOGroups) != 1 || alloc3.VFIOGroups[0] != 8 {
		t.Fatalf("explicit VFIO allocation = %+v, want group 8", alloc3.VFIOGroups)
	}

	node.Release(alloc1)
	if _, claimed := node.VFIOAllocations[7]; claimed {
		t.Fatalf("group 7 still claimed: %+v", node.VFIOAllocations)
	}
	if node.Resources.VFIOGroups[0].ClaimedBy != "" {
		t.Fatalf("group 7 claim = %q, want empty", node.Resources.VFIOGroups[0].ClaimedBy)
	}

	node.Release(alloc3)
	if len(node.VFIOAllocations) != 0 {
		t.Fatalf("VFIO allocations = %+v, want empty", node.VFIOAllocations)
	}
}
