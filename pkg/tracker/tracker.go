package tracker

import (
	"sync"
	"time"

	"github.com/octopos/octopos/pkg/cluster"
)

// Tracker tracks processes across the cluster
type Tracker struct {
	mu        sync.RWMutex
	procs     map[cluster.GlobalPID]*cluster.ProcessInfo
	byNode    map[cluster.NodeID]map[int]*cluster.ProcessInfo
	bySession map[cluster.SessionID]map[cluster.GlobalPID]bool
	byJob     map[cluster.JobID]map[cluster.GlobalPID]bool
}

// NewTracker creates a new process tracker
func NewTracker() *Tracker {
	return &Tracker{
		procs:     make(map[cluster.GlobalPID]*cluster.ProcessInfo),
		byNode:    make(map[cluster.NodeID]map[int]*cluster.ProcessInfo),
		bySession: make(map[cluster.SessionID]map[cluster.GlobalPID]bool),
		byJob:     make(map[cluster.JobID]map[cluster.GlobalPID]bool),
	}
}

// Register adds a process to tracking
func (t *Tracker) Register(proc *cluster.ProcessInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.procs[proc.GlobalPID] = proc

	if t.byNode[proc.NodeID] == nil {
		t.byNode[proc.NodeID] = make(map[int]*cluster.ProcessInfo)
	}
	t.byNode[proc.NodeID][proc.LocalPID] = proc

	if t.bySession[proc.SessionID] == nil {
		t.bySession[proc.SessionID] = make(map[cluster.GlobalPID]bool)
	}
	t.bySession[proc.SessionID][proc.GlobalPID] = true

	if t.byJob[proc.JobID] == nil {
		t.byJob[proc.JobID] = make(map[cluster.GlobalPID]bool)
	}
	t.byJob[proc.JobID][proc.GlobalPID] = true
}

// Unregister removes a process from tracking
func (t *Tracker) Unregister(globalPID cluster.GlobalPID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	proc, ok := t.procs[globalPID]
	if !ok {
		return
	}

	delete(t.procs, globalPID)
	delete(t.byNode[proc.NodeID], proc.LocalPID)
	delete(t.bySession[proc.SessionID], globalPID)
	delete(t.byJob[proc.JobID], globalPID)
}

// Get returns process info by global PID
func (t *Tracker) Get(globalPID cluster.GlobalPID) (*cluster.ProcessInfo, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	p, ok := t.procs[globalPID]
	return p, ok
}

// GetByNode returns all processes on a node
func (t *Tracker) GetByNode(nodeID cluster.NodeID) []*cluster.ProcessInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()

	nodeProcs := t.byNode[nodeID]
	result := make([]*cluster.ProcessInfo, 0, len(nodeProcs))
	for _, p := range nodeProcs {
		result = append(result, p)
	}
	return result
}

// GetBySession returns all processes in a session
func (t *Tracker) GetBySession(sessionID cluster.SessionID) []*cluster.ProcessInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()

	pids := t.bySession[sessionID]
	result := make([]*cluster.ProcessInfo, 0, len(pids))
	for pid := range pids {
		if p, ok := t.procs[pid]; ok {
			result = append(result, p)
		}
	}
	return result
}

// GetByJob returns all processes in a job
func (t *Tracker) GetByJob(jobID cluster.JobID) []*cluster.ProcessInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()

	pids := t.byJob[jobID]
	result := make([]*cluster.ProcessInfo, 0, len(pids))
	for pid := range pids {
		if p, ok := t.procs[pid]; ok {
			result = append(result, p)
		}
	}
	return result
}

// UpdateHeartbeat updates process last-seen time
func (t *Tracker) UpdateHeartbeat(globalPID cluster.GlobalPID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if p, ok := t.procs[globalPID]; ok {
		p.StartTime = time.Now() // Reuse field for heartbeat
	}
}

// ListAll returns all tracked processes
func (t *Tracker) ListAll() []*cluster.ProcessInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]*cluster.ProcessInfo, 0, len(t.procs))
	for _, p := range t.procs {
		result = append(result, p)
	}
	return result
}

// Count returns total process count
func (t *Tracker) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.procs)
}
