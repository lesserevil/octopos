//go:build linux

package childsupervisor

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestExecNotificationFilterTrapsExecAndAllowsOtherSyscalls(t *testing.T) {
	filters := execNotificationFilter()
	if len(filters) != 5 {
		t.Fatalf("filter length = %d, want 5", len(filters))
	}
	if filters[1].K == filters[2].K {
		t.Fatalf("exec syscall filters are identical: %#v", filters)
	}
	if filters[3].K == filters[4].K {
		t.Fatalf("trap and allow returns are identical: %#v", filters)
	}
}

func TestSyscallName(t *testing.T) {
	if got := syscallName(int32(unix.SYS_EXECVE)); got != "execve" {
		t.Fatalf("syscallName(execve) = %q", got)
	}
	if got := syscallName(int32(unix.SYS_EXECVEAT)); got != "execveat" {
		t.Fatalf("syscallName(execveat) = %q", got)
	}
	if got := syscallName(-1); got != "syscall_-1" {
		t.Fatalf("syscallName(-1) = %q", got)
	}
}

func TestDecideSyscallDefaultsToContinue(t *testing.T) {
	event := SyscallEvent{ID: 42, Syscall: int32(unix.SYS_EXECVE)}
	decision := decideSyscall(context.Background(), event, nil)
	if decision.Action != SyscallDecisionContinue {
		t.Fatalf("default decision = %q, want continue", decision.Action)
	}
}

func TestDecideSyscallPolicyCanDeny(t *testing.T) {
	event := SyscallEvent{ID: 42, Syscall: int32(unix.SYS_EXECVE)}
	decision := decideSyscall(context.Background(), event, func(ctx context.Context, got SyscallEvent) SyscallDecision {
		if got.ID != event.ID {
			t.Fatalf("policy event ID = %d, want %d", got.ID, event.ID)
		}
		return DenySyscall(syscall.EACCES, "test policy")
	})
	if decision.Action != SyscallDecisionDeny {
		t.Fatalf("decision action = %q, want deny", decision.Action)
	}
	if decision.Errno != syscall.EACCES {
		t.Fatalf("decision errno = %d, want %d", decision.Errno, syscall.EACCES)
	}
}

func TestResponseForDecision(t *testing.T) {
	continued, err := responseForDecision(7, ContinueSyscall("test"))
	if err != nil {
		t.Fatalf("continue response: %v", err)
	}
	if continued.ID != 7 || continued.Error != 0 || continued.Flags == 0 {
		t.Fatalf("continue response = %#v", continued)
	}

	denied, err := responseForDecision(8, DenySyscall(syscall.EPERM, "test"))
	if err != nil {
		t.Fatalf("deny response: %v", err)
	}
	if denied.ID != 8 || denied.Error != -int32(syscall.EPERM) || denied.Flags != 0 {
		t.Fatalf("deny response = %#v", denied)
	}
}

func TestLogSyscallDecisionText(t *testing.T) {
	var buf bytes.Buffer
	event := SyscallEvent{
		ID:          1,
		PID:         2,
		Syscall:     int32(unix.SYS_EXECVE),
		SyscallName: "execve",
		At:          time.Unix(1, 2).UTC(),
	}
	if err := logSyscallDecision(ObserveOptions{Log: &buf}, event, ContinueSyscall("observe")); err != nil {
		t.Fatalf("log text decision: %v", err)
	}
	log := buf.String()
	if !strings.Contains(log, "observed syscall=") || !strings.Contains(log, "decision=continue") || !strings.Contains(log, "syscall_name=execve") {
		t.Fatalf("text log missing fields:\n%s", log)
	}
}

func TestLogSyscallDecisionJSON(t *testing.T) {
	var buf bytes.Buffer
	event := SyscallEvent{
		ID:          1,
		PID:         2,
		Syscall:     int32(unix.SYS_EXECVE),
		SyscallName: "execve",
		At:          time.Unix(1, 2).UTC(),
	}
	if err := logSyscallDecision(ObserveOptions{Log: &buf, JSONLog: true}, event, ContinueSyscall("observe")); err != nil {
		t.Fatalf("log json decision: %v", err)
	}
	var entry syscallDecisionLog
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("parse json log %q: %v", buf.String(), err)
	}
	if entry.Event.SyscallName != "execve" || entry.Decision.Action != SyscallDecisionContinue {
		t.Fatalf("json log = %#v", entry)
	}
}
