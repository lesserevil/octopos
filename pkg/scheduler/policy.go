package scheduler

import (
	"sort"

	"github.com/octopos/octopos/pkg/cluster"
)

// Policy defines scheduling policy interface
type Policy interface {
	Filter(nodes []*cluster.NodeInfo, req cluster.Requirements) []*cluster.NodeInfo
	Score(node *cluster.NodeInfo, req cluster.Requirements) int
	Name() string
}

// BinPackPolicy packs jobs onto fewest nodes
type BinPackPolicy struct{}

func (b *BinPackPolicy) Name() string { return "binpack" }

func (b *BinPackPolicy) Filter(nodes []*cluster.NodeInfo, req cluster.Requirements) []*cluster.NodeInfo {
	var eligible []*cluster.NodeInfo
	for _, n := range nodes {
		if n.State != cluster.NodeStateActive {
			continue
		}
		// Check CPU
		if n.Allocated.CPU+req.CPU > n.Resources.CPU {
			continue
		}
		// Check Memory
		if n.Allocated.Memory+req.Memory > n.Resources.Memory {
			continue
		}
		// Check GPUs
		if n.Allocated.GPUCount+req.GPUs > n.Resources.GPUCount {
			continue
		}
		eligible = append(eligible, n)
	}
	return eligible
}

func (b *BinPackPolicy) Score(node *cluster.NodeInfo, req cluster.Requirements) int {
	// Prefer nodes with more allocated resources (pack tightly)
	// Score = allocated / capacity ratio (higher = more packed)
	cpuRatio := float64(node.Allocated.CPU) / float64(node.Resources.CPU)
	memRatio := float64(node.Allocated.Memory) / float64(node.Resources.Memory)
	return int((cpuRatio + memRatio) * 5000) // 0-10000
}

// SpreadPolicy spreads jobs across nodes
type SpreadPolicy struct{}

func (s *SpreadPolicy) Name() string { return "spread" }

func (s *SpreadPolicy) Filter(nodes []*cluster.NodeInfo, req cluster.Requirements) []*cluster.NodeInfo {
	return (&BinPackPolicy{}).Filter(nodes, req) // Same eligibility
}

func (s *SpreadPolicy) Score(node *cluster.NodeInfo, req cluster.Requirements) int {
	// Prefer nodes with fewer allocated resources (spread out)
	cpuRatio := float64(node.Allocated.CPU) / float64(node.Resources.CPU)
	memRatio := float64(node.Allocated.Memory) / float64(node.Resources.Memory)
	return int((2.0 - cpuRatio - memRatio) * 5000) // 0-10000, inverted
}

// Scheduler manages job scheduling
type Scheduler struct {
	policy Policy
	nodes  map[cluster.NodeID]*cluster.NodeInfo
}

func NewScheduler(policy Policy) *Scheduler {
	if policy == nil {
		policy = &BinPackPolicy{}
	}
	return &Scheduler{
		policy: policy,
		nodes:  make(map[cluster.NodeID]*cluster.NodeInfo),
	}
}

func (s *Scheduler) SetPolicy(p Policy) {
	s.policy = p
}

func (s *Scheduler) AddNode(n *cluster.NodeInfo) {
	s.nodes[n.ID] = n
}

func (s *Scheduler) RemoveNode(id cluster.NodeID) {
	delete(s.nodes, id)
}

func (s *Scheduler) GetNode(id cluster.NodeID) (*cluster.NodeInfo, bool) {
	n, ok := s.nodes[id]
	return n, ok
}

func (s *Scheduler) ListNodes() []*cluster.NodeInfo {
	nodes := make([]*cluster.NodeInfo, 0, len(s.nodes))
	for _, n := range s.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

func (s *Scheduler) Schedule(req cluster.Requirements) (*cluster.NodeInfo, error) {
	eligible := s.policy.Filter(s.listNodesSlice(), req)
	if len(eligible) == 0 {
		return nil, ErrNoEligibleNode
	}

	// Score and pick best
	best := eligible[0]
	bestScore := s.policy.Score(best, req)
	for _, n := range eligible[1:] {
		if score := s.policy.Score(n, req); score > bestScore {
			best, bestScore = n, score
		}
	}

	// Reserve resources
	if !best.Reserve(req) {
		return s.Schedule(req) // Retry
	}
	return best, nil
}

func (s *Scheduler) listNodesSlice() []*cluster.NodeInfo {
	nodes := make([]*cluster.NodeInfo, 0, len(s.nodes))
	for _, n := range s.nodes {
		nodes = append(nodes, n)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
	return nodes
}

func (s *Scheduler) Release(nodeID cluster.NodeID, req cluster.Requirements) {
	if n, ok := s.nodes[nodeID]; ok {
		n.Release(req)
	}
}

// ErrNoEligibleNode returned when no node can satisfy requirements
var ErrNoEligibleNode = &ErrNoEligibleNodeType{}

type ErrNoEligibleNodeType struct{}

func (e *ErrNoEligibleNodeType) Error() string {
	return "no eligible node for requirements"
}
