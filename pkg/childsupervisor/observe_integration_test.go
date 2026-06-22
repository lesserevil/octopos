//go:build linux

package childsupervisor

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestRunObserveReportsDirectExec(t *testing.T) {
	runObserveIntegration(t, []string{"/bin/true"})
}

func TestRunObserveReportsShellExec(t *testing.T) {
	runObserveIntegration(t, []string{"/bin/sh", "-c", "exec /bin/true"})
}

func runObserveIntegration(t *testing.T, argv []string) {
	t.Helper()
	if !CheckSupport().ProductionSupervisorUsable {
		t.Skip("seccomp user notification is unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var log bytes.Buffer
	if err := RunObserve(ctx, argv, ObserveOptions{Log: &log}); err != nil {
		if strings.Contains(err.Error(), "seccomp user notification is unavailable") ||
			strings.Contains(err.Error(), "operation not permitted") ||
			strings.Contains(err.Error(), "permission denied") {
			t.Skipf("seccomp observe unavailable in this environment: %v", err)
		}
		t.Fatalf("RunObserve(%v): %v\n%s", argv, err, log.String())
	}
	if !strings.Contains(log.String(), "observed syscall=") {
		t.Fatalf("observe log missing syscall observation:\n%s", log.String())
	}
	if !strings.Contains(log.String(), "decision=continue") {
		t.Fatalf("observe log missing continue decision:\n%s", log.String())
	}
}

func TestRunObserveInvokesPolicy(t *testing.T) {
	if !CheckSupport().ProductionSupervisorUsable {
		t.Skip("seccomp user notification is unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	events := make(chan SyscallEvent, 1)
	var log bytes.Buffer
	err := RunObserve(ctx, []string{"/bin/true"}, ObserveOptions{
		Log: &log,
		Policy: func(ctx context.Context, event SyscallEvent) SyscallDecision {
			select {
			case events <- event:
			default:
			}
			return ContinueSyscall("test policy")
		},
	})
	if err != nil {
		if strings.Contains(err.Error(), "seccomp user notification is unavailable") ||
			strings.Contains(err.Error(), "operation not permitted") ||
			strings.Contains(err.Error(), "permission denied") {
			t.Skipf("seccomp observe unavailable in this environment: %v", err)
		}
		t.Fatalf("RunObserve policy: %v\n%s", err, log.String())
	}
	select {
	case event := <-events:
		if event.SyscallName == "" {
			t.Fatalf("policy event missing syscall name: %#v", event)
		}
	default:
		t.Fatalf("policy was not invoked:\n%s", log.String())
	}
}
