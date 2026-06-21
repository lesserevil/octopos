package remotechild

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestStoreUpsertListAndCounts(t *testing.T) {
	store := NewStore()
	store.Upsert(ShadowRecord{
		SessionID:    "session-1",
		ParentJobID:  "job-parent",
		RemoteJobID:  "job-child",
		RemoteNodeID: "node-1",
		Command:      []string{"echo", "hi"},
		State:        StateRunning,
	})
	store.Upsert(ShadowRecord{
		SessionID:    "session-1",
		ParentJobID:  "job-parent",
		RemoteJobID:  "job-done",
		RemoteNodeID: "node-1",
		State:        StateCompleted,
	})

	if got := store.CountActive(ListFilter{ParentJobID: "job-parent"}); got != 1 {
		t.Fatalf("active count = %d, want 1", got)
	}
	records := store.List(ListFilter{SessionID: "session-1", ActiveOnly: true})
	if len(records) != 1 || records[0].RemoteJobID != "job-child" {
		t.Fatalf("active records = %#v", records)
	}
	records[0].Command[0] = "mutated"
	record, ok := store.Get("job-child")
	if !ok {
		t.Fatal("record missing")
	}
	if record.Command[0] != "echo" {
		t.Fatalf("store returned mutable command slice: %#v", record.Command)
	}
}

func TestStoreLifecycleTransitions(t *testing.T) {
	store := NewStore()
	store.Upsert(ShadowRecord{
		RemoteJobID: "job-child",
		State:       StateScheduled,
	})

	started := time.Unix(20, 0)
	store.MarkRunning("job-child", 42, started)
	record, ok := store.Get("job-child")
	if !ok {
		t.Fatal("record missing")
	}
	if record.State != StateRunning || record.RemoteGlobalPID != 42 || !record.StartedAt.Equal(started) {
		t.Fatalf("running record = %#v", record)
	}

	finished := time.Unix(21, 0)
	store.MarkFinished("job-child", StateFailed, -1, 15, "test failure", finished)
	record, _ = store.Get("job-child")
	if record.State != StateFailed || record.ExitCode != -1 || record.Signal != 15 || record.FailureReason != "test failure" {
		t.Fatalf("finished record = %#v", record)
	}
	if !record.FinishedAt.Equal(finished) {
		t.Fatalf("finished_at = %s, want %s", record.FinishedAt, finished)
	}
}

func TestStorePruneTerminal(t *testing.T) {
	store := NewStore()
	oldFinished := time.Unix(10, 0)
	recentFinished := time.Unix(20, 0)
	store.Upsert(ShadowRecord{RemoteJobID: "active", State: StateRunning, UpdatedAt: oldFinished})
	store.Upsert(ShadowRecord{RemoteJobID: "old", State: StateCompleted, FinishedAt: oldFinished})
	store.Upsert(ShadowRecord{RemoteJobID: "recent", State: StateCompleted, FinishedAt: recentFinished})

	if got := store.PruneTerminal(time.Unix(15, 0)); got != 1 {
		t.Fatalf("pruned = %d, want 1", got)
	}
	if _, ok := store.Get("old"); ok {
		t.Fatal("old terminal record was not pruned")
	}
	if _, ok := store.Get("active"); !ok {
		t.Fatal("active record was pruned")
	}
	if _, ok := store.Get("recent"); !ok {
		t.Fatal("recent terminal record was pruned")
	}
}

func TestPersistentStoreReloadsActiveRecordsAsRecovering(t *testing.T) {
	path := t.TempDir() + "/remote-children.json"
	store, err := NewPersistentStore(path)
	if err != nil {
		t.Fatalf("NewPersistentStore returned error: %v", err)
	}
	store.Upsert(ShadowRecord{
		SessionID:   "session-1",
		ParentJobID: "job-parent",
		RemoteJobID: "job-active",
		State:       StateRunning,
		Command:     []string{"sleep", "100"},
	})
	store.Upsert(ShadowRecord{
		SessionID:   "session-1",
		ParentJobID: "job-parent",
		RemoteJobID: "job-complete",
		State:       StateCompleted,
		Command:     []string{"true"},
		FinishedAt:  time.Unix(10, 0),
	})
	if err := store.LastPersistError(); err != nil {
		t.Fatalf("persist error: %v", err)
	}

	reloaded, err := NewPersistentStore(path)
	if err != nil {
		t.Fatalf("reload returned error: %v", err)
	}
	if got := reloaded.RecoveringOnLoad(); got != 1 {
		t.Fatalf("recovering on load = %d, want 1", got)
	}
	active, ok := reloaded.Get("job-active")
	if !ok {
		t.Fatal("active record missing after reload")
	}
	if active.State != StateRecovering {
		t.Fatalf("active state after reload = %s, want recovering", active.State)
	}
	if active.FailureReason == "" {
		t.Fatalf("recovery reason was not set: %#v", active)
	}
	if !active.FinishedAt.IsZero() {
		t.Fatalf("recovering record should not have finished time: %#v", active)
	}
	if got := reloaded.CountActive(ListFilter{ParentJobID: "job-parent"}); got != 1 {
		t.Fatalf("active count after reload = %d, want 1", got)
	}
	completed, ok := reloaded.Get("job-complete")
	if !ok {
		t.Fatal("completed record missing after reload")
	}
	if completed.State != StateCompleted {
		t.Fatalf("completed state after reload = %s, want completed", completed.State)
	}
}

func TestStoreAuditEventsAreClonedAndPersisted(t *testing.T) {
	path := t.TempDir() + "/remote-children.json"
	store, err := NewPersistentStore(path)
	if err != nil {
		t.Fatalf("NewPersistentStore returned error: %v", err)
	}
	event := AuditEvent{
		Event:             "authorize",
		Decision:          "rejected",
		SessionID:         "session-1",
		ParentJobID:       "job-parent",
		RemoteJobID:       "job-child",
		Command:           []string{"/bin/true"},
		AuthFailureReason: "remote child token expired",
	}
	store.RecordAudit(event)
	if err := store.LastAuditError(); err != nil {
		t.Fatalf("audit persist error: %v", err)
	}
	events := store.AuditEvents()
	if len(events) != 1 {
		t.Fatalf("audit events = %d, want 1", len(events))
	}
	events[0].Command[0] = "mutated"
	events = store.AuditEvents()
	if events[0].Command[0] != "/bin/true" {
		t.Fatalf("audit command slice was mutable: %#v", events[0])
	}
	data, err := os.ReadFile(path + ".audit.jsonl")
	if err != nil {
		t.Fatalf("read audit file: %v", err)
	}
	if !strings.Contains(string(data), "remote child token expired") {
		t.Fatalf("audit file missing failure reason: %s", string(data))
	}
}
