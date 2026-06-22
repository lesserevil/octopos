package childsupervisor

import (
	"context"
	"io"
	"syscall"
	"time"
)

type SyscallDecisionAction string

const (
	SyscallDecisionContinue SyscallDecisionAction = "continue"
	SyscallDecisionDeny     SyscallDecisionAction = "deny"
)

type SyscallEvent struct {
	ID                 uint64
	PID                uint32
	Syscall            int32
	SyscallName        string
	Arch               uint32
	InstructionPointer uint64
	Args               [6]uint64
	At                 time.Time
}

type SyscallDecision struct {
	Action SyscallDecisionAction
	Reason string
	Errno  syscall.Errno
}

type SyscallPolicy func(context.Context, SyscallEvent) SyscallDecision

type ObserveOptions struct {
	Log     io.Writer
	Policy  SyscallPolicy
	JSONLog bool
}

func ContinueSyscall(reason string) SyscallDecision {
	return SyscallDecision{Action: SyscallDecisionContinue, Reason: reason}
}

func DenySyscall(errno syscall.Errno, reason string) SyscallDecision {
	if errno == 0 {
		errno = syscall.EPERM
	}
	return SyscallDecision{Action: SyscallDecisionDeny, Errno: errno, Reason: reason}
}
