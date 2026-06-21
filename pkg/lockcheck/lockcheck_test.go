package lockcheck

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestValidateKind(t *testing.T) {
	if err := ValidateKind(KindFlock); err != nil {
		t.Fatalf("flock rejected: %v", err)
	}
	if err := ValidateKind("bad"); err == nil {
		t.Fatal("bad kind accepted")
	}
}

func TestValidateMode(t *testing.T) {
	if err := ValidateMode(ModeShared); err != nil {
		t.Fatalf("shared rejected: %v", err)
	}
	if err := ValidateMode("bad"); err == nil {
		t.Fatal("bad mode accepted")
	}
}

func TestFlockConflict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lock")
	first, err := OpenLockFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := OpenLockFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	if err := Lock(first, KindFlock, true); err != nil {
		t.Fatalf("first lock: %v", err)
	}
	defer Unlock(first, KindFlock)
	if err := Lock(second, KindFlock, true); !errors.Is(err, ErrLocked) {
		t.Fatalf("second lock = %v, want ErrLocked", err)
	}
}

func TestSharedFlockCanCoexist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lock")
	first, err := OpenLockFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := OpenLockFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	if err := LockWithMode(first, KindFlock, ModeShared, true); err != nil {
		t.Fatalf("first shared lock: %v", err)
	}
	defer Unlock(first, KindFlock)
	if err := LockWithMode(second, KindFlock, ModeShared, true); err != nil {
		t.Fatalf("second shared lock: %v", err)
	}
	defer Unlock(second, KindFlock)
}
