package rpc

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/octopos/octopos/pkg/cluster"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type activeStream struct {
	cmd      *exec.Cmd
	stdin    io.Writer
	done     chan struct{}
	exitCode int32
	cancel   context.CancelFunc
}

var (
	streamsMu sync.Mutex
	streams   = make(map[string]*activeStream)
	streamSeq uint64
)

// ExecStream handles bidirectional streaming for executing commands with stdin/stdout/stderr.
func (s *ClusterServerImpl) ExecStream(stream Cluster_ExecStreamServer) error {
	// First message must be an exec request
	msg, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "expected exec request: %v", err)
	}

	execReq := msg.GetExec()
	if execReq == nil {
		return status.Error(codes.InvalidArgument, "first message must contain exec")
	}

	// Create context with cancel for the stream
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	// Build the command
	cmd := exec.CommandContext(ctx, execReq.Command[0], execReq.Command[1:]...)
	cmd.Dir = execReq.Cwd
	cmd.Env = execReq.Env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// Create pipes
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return status.Errorf(codes.Internal, "stdin pipe: %v", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return status.Errorf(codes.Internal, "stdout pipe: %v", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return status.Errorf(codes.Internal, "stderr pipe: %v", err)
	}

	streamID := fmt.Sprintf("stream-%d", atomic.AddUint64(&streamSeq, 1))
	as := &activeStream{
		cmd:    cmd,
		stdin:  stdinPipe,
		done:   make(chan struct{}),
		cancel: cancel,
	}

	streamsMu.Lock()
	streams[streamID] = as
	streamsMu.Unlock()

	defer func() {
		streamsMu.Lock()
		delete(streams, streamID)
		streamsMu.Unlock()
	}()

	// Register in tracker
	jobID := cluster.JobID(execReq.JobId)
	globalPID := cluster.GlobalPID(atomic.AddUint64(&streamSeq, 1))
	proc := &cluster.ProcessInfo{
		GlobalPID: globalPID,
		NodeID:    s.nodeID,
		LocalPID:  0,
		SessionID: cluster.SessionID(execReq.SessionId),
		JobID:     jobID,
		Comm:      execReq.Command[0],
		Cmdline:   fmt.Sprintf("%v", execReq.Command),
		CWD:       execReq.Cwd,
		StartTime: time.Now(),
	}
	s.tracker.Register(proc)
	defer s.tracker.Unregister(globalPID)

	// Send exec response
	if err := stream.Send(&ExecStreamResponse{Payload: &ExecStreamResponse_Exec{
		Exec: &ExecuteResponse{
			JobId:     execReq.JobId,
			GlobalPid: uint64(globalPID),
		},
	}}); err != nil {
		return err
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		return stream.Send(&ExecStreamResponse{Payload: &ExecStreamResponse_Error{
			Error: fmt.Sprintf("start failed: %v", err),
		}})
	}

	proc.LocalPID = cmd.Process.Pid

	// Goroutine to forward stdout to stream
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		buf := make([]byte, 32*1024)
		for {
			n, err := stdoutPipe.Read(buf)
			if n > 0 {
				data := make([]byte, n)
				copy(data, buf[:n])
				if sendErr := stream.Send(&ExecStreamResponse{Payload: &ExecStreamResponse_StdoutData{
					StdoutData: data,
				}}); sendErr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		buf := make([]byte, 32*1024)
		for {
			n, err := stderrPipe.Read(buf)
			if n > 0 {
				data := make([]byte, n)
				copy(data, buf[:n])
				if sendErr := stream.Send(&ExecStreamResponse{Payload: &ExecStreamResponse_StderrData{
					StderrData: data,
				}}); sendErr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	// Goroutine to receive stream messages (stdin data, signals, close)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}

			switch payload := msg.Payload.(type) {
			case *ExecStreamRequest_StdinData:
				if _, writeErr := stdinPipe.Write(payload.StdinData); writeErr != nil {
					return
				}
			case *ExecStreamRequest_CloseStdin:
				stdinPipe.Close()
			case *ExecStreamRequest_Signal:
				if cmd.Process != nil {
					sig := syscall.Signal(payload.Signal.Signal)
					proc := cmd.Process
					if proc != nil {
						syscall.Kill(-proc.Pid, sig)
					}
				}
			case *ExecStreamRequest_Exec:
				return
			}
		}
	}()

	// Wait for process to finish
	err = cmd.Wait()
	stdinPipe.Close()
	wg.Wait()

	exitCode := int32(0)
	if exitErr, ok := err.(*exec.ExitError); ok {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			exitCode = int32(status.ExitStatus())
		}
	} else if err != nil {
		exitCode = -1
	}

	// Send exit code
	stream.Send(&ExecStreamResponse{Payload: &ExecStreamResponse_ExitCode{
		ExitCode: exitCode,
	}})

	close(as.done)
	return nil
}
