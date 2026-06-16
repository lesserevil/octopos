package cluster

import (
	"errors"
	"sync"
	"time"
)

// ErrInvalidTransition is returned for invalid state transitions
var ErrInvalidTransition = errors.New("invalid state transition")

// NodeStateMachine manages node lifecycle state
type NodeStateMachine struct {
	mu     sync.RWMutex
	state  NodeState
	nodeID string
}

// NewNodeStateMachine creates a new state machine
func NewNodeStateMachine(nodeID string) *NodeStateMachine {
	return &NodeStateMachine{
		state:  NodeStateOffline,
		nodeID: nodeID,
	}
}

// State returns current state
func (n *NodeStateMachine) State() NodeState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

// Transition attempts to change state
func (n *NodeStateMachine) Transition(newState NodeState) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	valid := validTransitions[n.state]
	for _, v := range valid {
		if v == newState {
			n.state = newState
			return nil
		}
	}
	return ErrInvalidTransition
}

// validTransitions defines allowed state transitions
var validTransitions = map[NodeState][]NodeState{
	NodeStateOffline:     {NodeStateActive, NodeStateMaintenance},
	NodeStateActive:      {NodeStateDraining, NodeStateMaintenance, NodeStateOffline},
	NodeStateDraining:    {NodeStateDrained, NodeStateActive, NodeStateOffline},
	NodeStateDrained:     {NodeStateActive, NodeStateMaintenance, NodeStateOffline},
	NodeStateMaintenance: {NodeStateActive, NodeStateOffline},
}

// HeartbeatTracker tracks node heartbeats
type HeartbeatTracker struct {
	mu        sync.RWMutex
	lastSeen  map[string]time.Time
	threshold time.Duration
}

// NewHeartbeatTracker creates a new tracker
func NewHeartbeatTracker(threshold time.Duration) *HeartbeatTracker {
	return &HeartbeatTracker{
		lastSeen:  make(map[string]time.Time),
		threshold: threshold,
	}
}

// RecordHeartbeat updates the last seen time
func (h *HeartbeatTracker) RecordHeartbeat(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastSeen[nodeID] = time.Now()
}

// IsAlive checks if node is considered alive
func (h *HeartbeatTracker) IsAlive(nodeID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	last, ok := h.lastSeen[nodeID]
	if !ok {
		return false
	}
	return time.Since(last) < h.threshold
}

// GetLastSeen returns last heartbeat time
func (h *HeartbeatTracker) GetLastSeen(nodeID string) (time.Time, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	t, ok := h.lastSeen[nodeID]
	return t, ok
}

// RemoveNode removes a node from tracking
func (h *HeartbeatTracker) RemoveNode(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.lastSeen, nodeID)
}
