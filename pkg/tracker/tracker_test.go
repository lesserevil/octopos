package tracker

import (
	"testing"
	"time"

	"github.com/octopos/octopos/pkg/cluster"
)

func TestTracker(t *testing.T) {
	tracker := NewTracker()

	proc1 := &cluster.ProcessInfo{
		GlobalPID: 0x000100000001, // node-1, pid 1
		NodeID:    "node-1",
		LocalPID:  100,
		SessionID: "sess-1",
		JobID:     "job-1",
		Comm:      "sleep",
		Cmdline:   "sleep 100",
		StartTime: time.Now(),
	}

	proc2 := &cluster.ProcessInfo{
		GlobalPID: 0x000100000002,
		NodeID:    "node-1",
		LocalPID:  101,
		SessionID: "sess-1",
		JobID:     "job-2",
		Comm:      "cat",
		Cmdline:   "cat file.txt",
		StartTime: time.Now(),
	}

	proc3 := &cluster.ProcessInfo{
		GlobalPID: 0x000200000001,
		NodeID:    "node-2",
		LocalPID:  200,
		SessionID: "sess-2",
		JobID:     "job-3",
		Comm:      "python",
		Cmdline:   "python train.py",
		StartTime: time.Now(),
	}

	// Register processes
	tracker.Register(proc1)
	tracker.Register(proc2)
	tracker.Register(proc3)

	if tracker.Count() != 3 {
		t.Errorf("Expected 3 processes, got %d", tracker.Count())
	}

	// Get by global PID
	found, ok := tracker.Get(proc1.GlobalPID)
	if !ok {
		t.Error("Should find proc1")
	}
	if found.LocalPID != 100 {
		t.Errorf("Expected PID 100, got %d", found.LocalPID)
	}

	// Get by node
	node1Procs := tracker.GetByNode("node-1")
	if len(node1Procs) != 2 {
		t.Errorf("Expected 2 processes on node-1, got %d", len(node1Procs))
	}

	node2Procs := tracker.GetByNode("node-2")
	if len(node2Procs) != 1 {
		t.Errorf("Expected 1 process on node-2, got %d", len(node2Procs))
	}

	// Get by session
	sess1Procs := tracker.GetBySession("sess-1")
	if len(sess1Procs) != 2 {
		t.Errorf("Expected 2 processes in sess-1, got %d", len(sess1Procs))
	}

	// Get by job
	job1Procs := tracker.GetByJob("job-1")
	if len(job1Procs) != 1 {
		t.Errorf("Expected 1 process in job-1, got %d", len(job1Procs))
	}

	// Unregister
	tracker.Unregister(proc1.GlobalPID)
	if tracker.Count() != 2 {
		t.Errorf("Expected 2 processes after unregister, got %d", tracker.Count())
	}

	_, ok = tracker.Get(proc1.GlobalPID)
	if ok {
		t.Error("Should not find unregistered process")
	}

	// List all
	all := tracker.ListAll()
	if len(all) != 2 {
		t.Errorf("ListAll should return 2, got %d", len(all))
	}
}
