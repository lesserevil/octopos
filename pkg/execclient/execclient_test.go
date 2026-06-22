package execclient

import (
	"context"
	"io"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/octopos/octopos/pkg/rpc"
)

func TestHandleForwardedSignalForwardsSimpleSignal(t *testing.T) {
	var signals []syscall.Signal
	send := func(req *rpc.ExecStreamRequest) error {
		signals = append(signals, syscall.Signal(req.GetSignal().Signal))
		return nil
	}

	handleForwardedSignal(send, syscall.SIGINT, signalForwardHooks{})

	if !reflect.DeepEqual(signals, []syscall.Signal{syscall.SIGINT}) {
		t.Fatalf("signals = %v, want [SIGINT]", signals)
	}
}

func TestHandleForwardedSignalSuspendsAndContinues(t *testing.T) {
	var signals []syscall.Signal
	var events []string
	send := func(req *rpc.ExecStreamRequest) error {
		signals = append(signals, syscall.Signal(req.GetSignal().Signal))
		return nil
	}
	hooks := signalForwardHooks{
		BeforeSuspend: func() { events = append(events, "before") },
		StopSelf: func() error {
			events = append(events, "stop")
			return nil
		},
		AfterResume: func() { events = append(events, "after") },
	}

	handleForwardedSignal(send, syscall.SIGTSTP, hooks)

	if !reflect.DeepEqual(signals, []syscall.Signal{syscall.SIGTSTP, syscall.SIGCONT}) {
		t.Fatalf("signals = %v, want [SIGTSTP SIGCONT]", signals)
	}
	if !reflect.DeepEqual(events, []string{"before", "stop", "after"}) {
		t.Fatalf("events = %v, want before/stop/after", events)
	}
}

func TestTerminateStreamSendsSIGTERMAndCloses(t *testing.T) {
	stream := &recordingStream{}
	var signals []syscall.Signal
	send := func(req *rpc.ExecStreamRequest) error {
		signals = append(signals, syscall.Signal(req.GetSignal().Signal))
		return stream.Send(req)
	}

	terminateStream(send, stream)

	if !reflect.DeepEqual(signals, []syscall.Signal{syscall.SIGTERM}) {
		t.Fatalf("signals = %v, want [SIGTERM]", signals)
	}
	if !stream.closed {
		t.Fatal("stream was not closed")
	}
}

func TestTerminateStreamOnContextDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := &recordingStream{closedCh: make(chan struct{})}
	var signals []syscall.Signal
	send := func(req *rpc.ExecStreamRequest) error {
		signals = append(signals, syscall.Signal(req.GetSignal().Signal))
		return stream.Send(req)
	}

	stop := terminateStreamOnContextDone(ctx, send, stream)
	defer stop()
	cancel()

	select {
	case <-stream.closedCh:
	case <-time.After(time.Second):
		t.Fatal("stream was not closed after context cancellation")
	}
	if !reflect.DeepEqual(signals, []syscall.Signal{syscall.SIGTERM}) {
		t.Fatalf("signals = %v, want [SIGTERM]", signals)
	}
}

type recordingStream struct {
	closed   bool
	closedCh chan struct{}
	sent     []*rpc.ExecStreamRequest
}

func (s *recordingStream) Send(req *rpc.ExecStreamRequest) error {
	s.sent = append(s.sent, req)
	return nil
}

func (s *recordingStream) Recv() (*rpc.ExecStreamResponse, error) {
	return nil, io.EOF
}

func (s *recordingStream) CloseSend() error {
	if !s.closed {
		s.closed = true
		if s.closedCh != nil {
			close(s.closedCh)
		}
	}
	return nil
}

var _ Stream = (*recordingStream)(nil)
