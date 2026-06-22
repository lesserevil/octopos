package execclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/octopos/octopos/pkg/rpc"
	"github.com/octopos/octopos/pkg/termio"
)

const StreamResizeSignal = -1

type ExitError struct {
	Code int
}

func (e *ExitError) Error() string {
	return fmt.Sprintf("exit status %d", e.Code)
}

type Options struct {
	Stdin          io.Reader
	Stdout         io.Writer
	Stderr         io.Writer
	TTY            bool
	RawTerminal    bool
	TerminalFD     uintptr
	ForwardSignals bool
	SignalSet      []os.Signal
	OpenStream     StreamOpener
	// TerminateOnContextCancel sends SIGTERM to the remote process before
	// closing the stream when ctx is canceled. It is intended for local shadow
	// processes that must clean up their worker when their parent disappears.
	TerminateOnContextCancel bool
}

type StreamOpener func(context.Context) (Stream, error)

type Stream interface {
	Send(*rpc.ExecStreamRequest) error
	Recv() (*rpc.ExecStreamResponse, error)
	CloseSend() error
}

func RunForeground(ctx context.Context, client rpc.ClusterClient, req *rpc.ExecuteRequest, opts Options) error {
	if req == nil {
		return errors.New("nil execute request")
	}
	stdin := opts.Stdin
	if stdin == nil {
		stdin = os.Stdin
	}
	stdout := opts.Stdout
	if stdout == nil {
		stdout = os.Stdout
	}
	stderr := opts.Stderr
	if stderr == nil {
		stderr = os.Stderr
	}

	tty := opts.TTY || req.Tty
	fd := opts.TerminalFD
	if fd == 0 {
		fd = os.Stdin.Fd()
	}
	var rawMu sync.Mutex
	var rawState *termio.State
	restoreTerminal := func() {
		rawMu.Lock()
		defer rawMu.Unlock()
		if rawState == nil {
			return
		}
		termio.Restore(fd, rawState)
		rawState = nil
	}
	makeRaw := func() error {
		rawMu.Lock()
		defer rawMu.Unlock()
		if rawState != nil {
			return nil
		}
		state, err := termio.MakeRaw(fd)
		if err != nil {
			return err
		}
		rawState = state
		return nil
	}
	if tty {
		if !termio.IsTerminal(fd) {
			return errors.New("--tty requires stdin to be a terminal")
		}
		if opts.RawTerminal {
			if err := makeRaw(); err != nil {
				return fmt.Errorf("enable raw terminal mode: %w", err)
			}
			defer restoreTerminal()
		}
		req.Env = appendTTYEnv(req.Env, fd)
	}

	openStream := opts.OpenStream
	if openStream == nil {
		openStream = func(ctx context.Context) (Stream, error) {
			return client.ExecStream(ctx)
		}
	}
	streamCtx := ctx
	if opts.TerminateOnContextCancel {
		if err := ctx.Err(); err != nil {
			return err
		}
		streamCtx = context.Background()
	}
	stream, err := openStream(streamCtx)
	if err != nil {
		return fmt.Errorf("open exec stream: %w", err)
	}

	var sendMu sync.Mutex
	send := func(req *rpc.ExecStreamRequest) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(req)
	}

	if err := send(&rpc.ExecStreamRequest{
		Payload: &rpc.ExecStreamRequest_Exec{Exec: req},
	}); err != nil {
		return fmt.Errorf("send exec request: %w", err)
	}
	if opts.TerminateOnContextCancel {
		stopContextWatcher := terminateStreamOnContextDone(ctx, send, stream)
		defer stopContextWatcher()
	}

	if tty {
		stopResize := forwardTerminalResize(fd, send)
		defer stopResize()
		_ = sendTerminalResize(fd, send)
	}
	if opts.ForwardSignals {
		hooks := signalForwardHooks{}
		if tty && opts.RawTerminal {
			hooks.BeforeSuspend = restoreTerminal
			hooks.AfterResume = func() {
				_ = makeRaw()
				_ = sendTerminalResize(fd, send)
			}
		}
		stopSignals := forwardSignals(send, opts.SignalSet, hooks)
		defer stopSignals()
	}

	go copyStdin(stdin, stream, send, &sendMu)

	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("receive exec stream: %w", err)
		}

		switch payload := resp.Payload.(type) {
		case *rpc.ExecStreamResponse_Exec:
			if payload.Exec.Error != "" {
				return fmt.Errorf("Execute failed: %s", payload.Exec.Error)
			}
		case *rpc.ExecStreamResponse_StdoutData:
			if _, err := stdout.Write(payload.StdoutData); err != nil {
				terminateStream(send, stream)
				return err
			}
		case *rpc.ExecStreamResponse_StderrData:
			if _, err := stderr.Write(payload.StderrData); err != nil {
				terminateStream(send, stream)
				return err
			}
		case *rpc.ExecStreamResponse_Error:
			return fmt.Errorf("Execute failed: %s", payload.Error)
		case *rpc.ExecStreamResponse_ExitCode:
			if payload.ExitCode != 0 {
				return &ExitError{Code: int(payload.ExitCode)}
			}
			return nil
		}
	}
}

func terminateStream(send func(*rpc.ExecStreamRequest) error, stream Stream) {
	_ = sendSignal(send, syscall.SIGTERM)
	_ = stream.CloseSend()
}

func terminateStreamOnContextDone(ctx context.Context, send func(*rpc.ExecStreamRequest) error, stream Stream) func() {
	done := make(chan struct{})
	var stopOnce sync.Once
	go func() {
		select {
		case <-ctx.Done():
			terminateStream(send, stream)
		case <-done:
		}
	}()
	return func() {
		stopOnce.Do(func() {
			close(done)
		})
	}
}

func copyStdin(stdin io.Reader, stream Stream, send func(*rpc.ExecStreamRequest) error, sendMu *sync.Mutex) {
	buf := make([]byte, 32*1024)
	for {
		n, readErr := stdin.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			err := send(&rpc.ExecStreamRequest{
				Payload: &rpc.ExecStreamRequest_StdinData{StdinData: data},
			})
			if err != nil {
				return
			}
		}
		if readErr != nil {
			_ = send(&rpc.ExecStreamRequest{
				Payload: &rpc.ExecStreamRequest_CloseStdin{CloseStdin: true},
			})
			sendMu.Lock()
			_ = stream.CloseSend()
			sendMu.Unlock()
			return
		}
	}
}

func appendTTYEnv(env []string, fd uintptr) []string {
	if term := os.Getenv("TERM"); term != "" {
		env = append(env, "TERM="+term)
	}
	if size, err := termio.GetSize(fd); err == nil {
		env = append(env,
			fmt.Sprintf("OCTOPOS_TTY_ROWS=%d", size.Rows),
			fmt.Sprintf("OCTOPOS_TTY_COLS=%d", size.Cols),
		)
	}
	return env
}

func forwardTerminalResize(fd uintptr, send func(*rpc.ExecStreamRequest) error) func() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGWINCH)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ch:
				_ = sendTerminalResize(fd, send)
			}
		}
	}()
	return func() {
		signal.Stop(ch)
		close(done)
	}
}

func sendTerminalResize(fd uintptr, send func(*rpc.ExecStreamRequest) error) error {
	size, err := termio.GetSize(fd)
	if err != nil || size.Rows == 0 || size.Cols == 0 {
		return err
	}
	return send(&rpc.ExecStreamRequest{
		Payload: &rpc.ExecStreamRequest_Signal{
			Signal: &rpc.SignalRequest{
				GlobalPid: encodeTerminalSize(size),
				Signal:    StreamResizeSignal,
			},
		},
	})
}

func encodeTerminalSize(size termio.Size) uint64 {
	return uint64(size.Rows)<<32 | uint64(size.Cols)
}

type signalForwardHooks struct {
	BeforeSuspend func()
	AfterResume   func()
	StopSelf      func() error
}

func forwardSignals(send func(*rpc.ExecStreamRequest) error, signalSet []os.Signal, hooks signalForwardHooks) func() {
	if len(signalSet) == 0 {
		signalSet = []os.Signal{
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGHUP,
			syscall.SIGQUIT,
			syscall.SIGTSTP,
			syscall.SIGCONT,
		}
	}
	if hooks.StopSelf == nil {
		hooks.StopSelf = func() error {
			return syscall.Kill(os.Getpid(), syscall.SIGSTOP)
		}
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, signalSet...)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			case sig := <-ch:
				if sysSig, ok := sig.(syscall.Signal); ok {
					handleForwardedSignal(send, sysSig, hooks)
				}
			}
		}
	}()
	return func() {
		signal.Stop(ch)
		close(done)
	}
}

func handleForwardedSignal(send func(*rpc.ExecStreamRequest) error, sig syscall.Signal, hooks signalForwardHooks) {
	_ = sendSignal(send, sig)
	if sig != syscall.SIGTSTP {
		return
	}
	if hooks.BeforeSuspend != nil {
		hooks.BeforeSuspend()
	}
	if hooks.StopSelf != nil {
		_ = hooks.StopSelf()
	}
	if hooks.AfterResume != nil {
		hooks.AfterResume()
	}
	_ = sendSignal(send, syscall.SIGCONT)
}

func sendSignal(send func(*rpc.ExecStreamRequest) error, sig syscall.Signal) error {
	return send(&rpc.ExecStreamRequest{
		Payload: &rpc.ExecStreamRequest_Signal{
			Signal: &rpc.SignalRequest{Signal: int32(sig)},
		},
	})
}
