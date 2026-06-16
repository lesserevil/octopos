package session

import (
	"os"
	"testing"

	"github.com/octopos/octopos/pkg/cluster"
)

func skipNoRoot(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("skipping test; requires root")
	}
}

func TestNewManager(t *testing.T) {
	skipNoRoot(t)
	mgr, err := NewManager()
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if mgr == nil {
		t.Fatal("manager is nil")
	}
}

func TestCreateDestroySession(t *testing.T) {
	skipNoRoot(t)

	mgr, err := NewManager()
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	sess := &cluster.Session{
		ID:   "test-session",
		User: "test",
	}

	ms, err := mgr.Create(sess)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if ms.State != SessionActive {
		t.Fatalf("expected active, got %v", ms.State)
	}

	got, ok := mgr.Get("test-session")
	if !ok {
		t.Fatal("session not found")
	}
	if got.User != "test" {
		t.Fatalf("expected user test, got %s", got.User)
	}

	if err := mgr.Destroy("test-session"); err != nil {
		t.Fatalf("Destroy: %v", err)
	}

	if _, ok := mgr.Get("test-session"); ok {
		t.Fatal("session still exists after destroy")
	}
}

func TestAddJob(t *testing.T) {
	skipNoRoot(t)

	mgr, err := NewManager()
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	sess := &cluster.Session{ID: "test-job-session"}
	mgr.Create(sess)
	defer mgr.Destroy("test-job-session")

	job := &cluster.JobInfo{
		ID:     "job-1",
		Status: cluster.JobStatusRunning,
	}

	if err := mgr.AddJob("test-job-session", job); err != nil {
		t.Fatalf("AddJob: %v", err)
	}

	ms, _ := mgr.Get("test-job-session")
	if len(ms.Jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(ms.Jobs))
	}
}

func TestList(t *testing.T) {
	skipNoRoot(t)

	mgr, err := NewManager()
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	before := mgr.Count()
	mgr.Create(&cluster.Session{ID: "list-session-1"})
	mgr.Create(&cluster.Session{ID: "list-session-2"})
	defer mgr.Destroy("list-session-1")
	defer mgr.Destroy("list-session-2")

	sessions := mgr.List()
	if len(sessions) != before+2 {
		t.Fatalf("expected %d sessions, got %d", before+2, len(sessions))
	}
}
