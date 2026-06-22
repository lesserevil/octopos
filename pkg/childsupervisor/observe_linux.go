//go:build linux

package childsupervisor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

type seccompData struct {
	Nr                 int32
	Arch               uint32
	InstructionPointer uint64
	Args               [6]uint64
}

type seccompNotif struct {
	ID    uint64
	PID   uint32
	Flags uint32
	Data  seccompData
}

type seccompNotifResp struct {
	ID    uint64
	Val   int64
	Error int32
	Flags uint32
}

type syscallDecisionLog struct {
	Event    SyscallEvent    `json:"event"`
	Decision SyscallDecision `json:"decision"`
}

func RunObserve(ctx context.Context, argv []string, opts ObserveOptions) error {
	if len(argv) == 0 {
		return nil
	}
	report := CheckSupport()
	if !report.ProductionSupervisorUsable {
		return fmt.Errorf("seccomp user notification is unavailable")
	}
	listenerFD, err := installExecNotificationFilter()
	if err != nil {
		return err
	}
	defer unix.Close(listenerFD)
	if err := unix.SetNonblock(listenerFD, true); err != nil {
		return fmt.Errorf("set seccomp listener nonblocking: %w", err)
	}

	observeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	observeErr := make(chan error, 1)
	go func() {
		observeErr <- serveNotifications(observeCtx, listenerFD, opts)
	}()

	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	startErr := make(chan error, 1)
	go func() {
		startErr <- cmd.Start()
	}()
	select {
	case err := <-startErr:
		if err != nil {
			cancel()
			_ = unix.Close(listenerFD)
			<-observeErr
			return err
		}
	case err := <-observeErr:
		cancel()
		_ = unix.Close(listenerFD)
		return err
	case <-ctx.Done():
		cancel()
		_ = unix.Close(listenerFD)
		<-observeErr
		return ctx.Err()
	}
	waitErr := cmd.Wait()
	cancel()
	_ = unix.Close(listenerFD)
	if err := <-observeErr; err != nil {
		return err
	}
	return waitErr
}

func installExecNotificationFilter() (int, error) {
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return -1, fmt.Errorf("set no_new_privs: %w", err)
	}
	filters := execNotificationFilter()
	prog := unix.SockFprog{
		Len:    uint16(len(filters)),
		Filter: &filters[0],
	}
	fd, _, errno := unix.Syscall(unix.SYS_SECCOMP, uintptr(unix.SECCOMP_SET_MODE_FILTER), uintptr(unix.SECCOMP_FILTER_FLAG_NEW_LISTENER), uintptr(unsafe.Pointer(&prog)))
	if errno != 0 {
		return -1, fmt.Errorf("install seccomp notification filter: %w", errno)
	}
	return int(fd), nil
}

func execNotificationFilter() []unix.SockFilter {
	const (
		bpfLD  = 0x00
		bpfW   = 0x00
		bpfABS = 0x20
		bpfJMP = 0x05
		bpfJEQ = 0x10
		bpfK   = 0x00
		bpfRET = 0x06
	)
	return []unix.SockFilter{
		{Code: bpfLD | bpfW | bpfABS, K: 0},
		{Code: bpfJMP | bpfJEQ | bpfK, Jt: 1, Jf: 0, K: uint32(unix.SYS_EXECVE)},
		{Code: bpfJMP | bpfJEQ | bpfK, Jt: 0, Jf: 1, K: uint32(unix.SYS_EXECVEAT)},
		{Code: bpfRET | bpfK, K: uint32(unix.SECCOMP_RET_USER_NOTIF)},
		{Code: bpfRET | bpfK, K: uint32(unix.SECCOMP_RET_ALLOW)},
	}
}

func serveNotifications(ctx context.Context, listenerFD int, opts ObserveOptions) error {
	for {
		events := []unix.PollFd{{Fd: int32(listenerFD), Events: unix.POLLIN}}
		n, err := unix.Poll(events, 10)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if errors.Is(err, unix.EBADF) && ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("poll seccomp notification listener: %w", err)
		}
		if n == 0 {
			select {
			case <-ctx.Done():
				return nil
			default:
				continue
			}
		}
		var req seccompNotif
		if err := notificationIOCTL(listenerFD, unix.SECCOMP_IOCTL_NOTIF_RECV, unsafe.Pointer(&req)); err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if errors.Is(err, unix.EAGAIN) {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(10 * time.Millisecond):
					continue
				}
			}
			if errors.Is(err, unix.EBADF) || errors.Is(err, unix.ENOENT) || errors.Is(err, unix.EINVAL) {
				if ctx.Err() != nil {
					return nil
				}
			}
			return fmt.Errorf("receive seccomp notification: %w", err)
		}
		event := syscallEventFromNotification(req, time.Now())
		decision := decideSyscall(ctx, event, opts.Policy)
		if err := logSyscallDecision(opts, event, decision); err != nil {
			return err
		}
		resp, err := responseForDecision(req.ID, decision)
		if err != nil {
			return err
		}
		if err := notificationIOCTL(listenerFD, unix.SECCOMP_IOCTL_NOTIF_SEND, unsafe.Pointer(&resp)); err != nil {
			if errors.Is(err, unix.ENOENT) {
				continue
			}
			if errors.Is(err, unix.EBADF) && ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("send seccomp notification decision: %w", err)
		}
		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

func syscallEventFromNotification(req seccompNotif, at time.Time) SyscallEvent {
	return SyscallEvent{
		ID:                 req.ID,
		PID:                req.PID,
		Syscall:            req.Data.Nr,
		SyscallName:        syscallName(req.Data.Nr),
		Arch:               req.Data.Arch,
		InstructionPointer: req.Data.InstructionPointer,
		Args:               req.Data.Args,
		At:                 at,
	}
}

func decideSyscall(ctx context.Context, event SyscallEvent, policy SyscallPolicy) SyscallDecision {
	if policy == nil {
		return ContinueSyscall("observe")
	}
	decision := policy(ctx, event)
	if decision.Action == "" {
		decision.Action = SyscallDecisionContinue
	}
	if decision.Action == SyscallDecisionDeny && decision.Errno == 0 {
		decision.Errno = syscall.EPERM
	}
	return decision
}

func responseForDecision(id uint64, decision SyscallDecision) (seccompNotifResp, error) {
	switch decision.Action {
	case SyscallDecisionContinue:
		return seccompNotifResp{
			ID:    id,
			Flags: uint32(unix.SECCOMP_USER_NOTIF_FLAG_CONTINUE),
		}, nil
	case SyscallDecisionDeny:
		errno := decision.Errno
		if errno == 0 {
			errno = syscall.EPERM
		}
		return seccompNotifResp{
			ID:    id,
			Error: -int32(errno),
		}, nil
	default:
		return seccompNotifResp{}, fmt.Errorf("unsupported syscall decision action %q", decision.Action)
	}
}

func logSyscallDecision(opts ObserveOptions, event SyscallEvent, decision SyscallDecision) error {
	if opts.Log == nil {
		return nil
	}
	if opts.JSONLog {
		return json.NewEncoder(opts.Log).Encode(syscallDecisionLog{
			Event:    event,
			Decision: decision,
		})
	}
	fmt.Fprintf(opts.Log, "octopos-child-supervisor: observed syscall=%d syscall_name=%s pid=%d id=%d at=%s decision=%s",
		event.Syscall, event.SyscallName, event.PID, event.ID, event.At.Format(time.RFC3339Nano), decision.Action)
	if decision.Reason != "" {
		fmt.Fprintf(opts.Log, " reason=%q", decision.Reason)
	}
	if decision.Action == SyscallDecisionDeny {
		fmt.Fprintf(opts.Log, " errno=%d", decision.Errno)
	}
	fmt.Fprintln(opts.Log)
	return nil
}

func syscallName(nr int32) string {
	switch nr {
	case unix.SYS_EXECVE:
		return "execve"
	case unix.SYS_EXECVEAT:
		return "execveat"
	default:
		return fmt.Sprintf("syscall_%d", nr)
	}
}

func notificationIOCTL(fd int, req uint, arg unsafe.Pointer) error {
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), uintptr(req), uintptr(arg))
	if errno != 0 {
		return errno
	}
	return nil
}

func ObserveExitCode(err error) (int, bool) {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			return status.ExitStatus(), true
		}
	}
	return 0, false
}
