package namespace

import (
	"os"
	"testing"
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

func TestCreateDestroyCgroup(t *testing.T) {
	skipNoRoot(t)

	mgr, err := NewManager()
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	sessionID := "test-session-123"
	if err := mgr.CreateCgroup(sessionID, 1000, 1<<30); err != nil {
		t.Fatalf("CreateCgroup: %v", err)
	}

	if err := mgr.DestroyCgroup(sessionID); err != nil {
		t.Fatalf("DestroyCgroup: %v", err)
	}
}

func TestSetupNamespaces(t *testing.T) {
	fn := SetupNamespaces(true, false, false)
	if fn == nil {
		t.Fatal("SetupNamespaces returned nil")
	}
}
