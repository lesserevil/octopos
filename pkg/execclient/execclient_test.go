package execclient

import (
	"io"
	"reflect"
	"syscall"
	"testing"

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

type recordingStream struct {
	closed bool
	sent   []*rpc.ExecStreamRequest
}

func (s *recordingStream) Send(req *rpc.ExecStreamRequest) error {
	s.sent = append(s.sent, req)
	return nil
}

func (s *recordingStream) Recv() (*rpc.ExecStreamResponse, error) {
	return nil, io.EOF
}

func (s *recordingStream) CloseSend() error {
	s.closed = true
	return nil
}

var _ Stream = (*recordingStream)(nil)
