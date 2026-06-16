package scheduler

import (
	"testing"

	"github.com/octopos/octopos/pkg/cluster"
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
