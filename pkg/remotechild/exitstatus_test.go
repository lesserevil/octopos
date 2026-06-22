package remotechild

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWorkerExitStatusPathIsStableAndSafe(t *testing.T) {
	got := WorkerExitStatusPath("../job child")
	if !strings.HasPrefix(got, WorkerExitStatusDir+"/") {
		t.Fatalf("path = %q, want under %s", got, WorkerExitStatusDir)
	}
	if strings.Contains(got, "..") || strings.Contains(got, " ") {
		t.Fatalf("path = %q, want sanitized path", got)
	}
	if got != WorkerExitStatusPath("../job child") {
		t.Fatalf("path is not stable")
	}
	if got == WorkerExitStatusPath("job child") {
		t.Fatalf("distinct job IDs produced same path: %q", got)
	}
}

func TestWorkerExitStatusRoundTrip(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "worker.json")
	want := WorkerExitStatus{
		JobID:    "job-child",
		PID:      123,
		ExitCode: 7,
		ExitedAt: time.Unix(200, 0).UTC(),
	}
	if err := WriteWorkerExitStatus(filePath, want); err != nil {
		t.Fatalf("WriteWorkerExitStatus: %v", err)
	}
	got, err := ReadWorkerExitStatus(filePath)
	if err != nil {
		t.Fatalf("ReadWorkerExitStatus: %v", err)
	}
	if got.JobID != want.JobID || got.PID != want.PID || got.ExitCode != want.ExitCode || !got.ExitedAt.Equal(want.ExitedAt) {
		t.Fatalf("status = %#v, want %#v", got, want)
	}
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("stat status file: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("status file mode = %#o, want 0600", info.Mode().Perm())
	}
}

func TestWriteWorkerExitStatusRejectsRelativePath(t *testing.T) {
	if err := WriteWorkerExitStatus("relative.json", WorkerExitStatus{}); err == nil {
		t.Fatal("WriteWorkerExitStatus accepted relative path")
	}
}
