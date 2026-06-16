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
