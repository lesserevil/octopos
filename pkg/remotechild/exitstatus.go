package remotechild

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

const WorkerExitStatusDir = "/var/lib/octopos/worker-exits"

type WorkerExitStatus struct {
	JobID    string    `json:"job_id"`
	PID      int       `json:"pid,omitempty"`
	ExitCode int       `json:"exit_code"`
	Signal   int       `json:"signal,omitempty"`
	Error    string    `json:"error,omitempty"`
	ExitedAt time.Time `json:"exited_at"`
}

func WorkerExitStatusPath(jobID string) string {
	safe := safeWorkerExitJobID(jobID)
	sum := sha256.Sum256([]byte(jobID))
	return path.Join(WorkerExitStatusDir, safe+"-"+hex.EncodeToString(sum[:8])+".json")
}

func WriteWorkerExitStatus(filePath string, status WorkerExitStatus) error {
	if filePath == "" {
		return nil
	}
	if !path.IsAbs(filePath) {
		return fmt.Errorf("worker exit status path %q must be absolute", filePath)
	}
	if status.ExitedAt.IsZero() {
		status.ExitedAt = time.Now()
	}
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create worker exit status dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(filePath)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create worker exit status temp file: %w", err)
	}
	tmpName := tmp.Name()
	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(status); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("encode worker exit status: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("close worker exit status temp file: %w", err)
	}
	if err := os.Chmod(tmpName, 0600); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("chmod worker exit status temp file: %w", err)
	}
	if err := os.Rename(tmpName, filePath); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("commit worker exit status: %w", err)
	}
	return nil
}

func ReadWorkerExitStatus(filePath string) (WorkerExitStatus, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return WorkerExitStatus{}, err
	}
	var status WorkerExitStatus
	if err := json.Unmarshal(data, &status); err != nil {
		return WorkerExitStatus{}, err
	}
	return status, nil
}

func safeWorkerExitJobID(jobID string) string {
	var b strings.Builder
	for _, r := range jobID {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '.', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
		if b.Len() >= 48 {
			break
		}
	}
	safe := strings.Trim(b.String(), "._-")
	if safe == "" {
		return "job"
	}
	return safe
}
