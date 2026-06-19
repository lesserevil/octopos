package rpc

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/octopos/octopos/pkg/cluster"
	"github.com/octopos/octopos/pkg/ssi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const streamResizeSignal = -1

// ExecStream handles foreground execution with stdin/stdout/stderr attached.
func (s *ClusterServerImpl) ExecStream(stream Cluster_ExecStreamServer) error {
	msg, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "expected exec request: %v", err)
	}

	req := msg.GetExec()
	if req == nil {
		return status.Error(codes.InvalidArgument, "first message must contain exec")
	}
	if len(req.Command) == 0 {
		return sendStreamError(stream, "empty command")
	}

	node, reqs, jobID, err := s.scheduleJob(req)
	if err != nil {
		return sendStreamError(stream, err.Error())
	}

	if node.ID != s.nodeID {
		return s.proxyExecStream(stream, req, jobID, node, reqs)
	}
	if req.Tty {
		return s.executeLocalPTYStream(stream, req, jobID, node, reqs)
	}
	return s.executeLocalStream(stream, req, jobID, node, reqs)
}

func (s *ClusterServerImpl) executeBackground(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	if len(req.Command) == 0 {
		return &ExecuteResponse{JobId: req.GetJobId(), ExitCode: -1, Error: "empty command"}, nil
	}

	node, reqs, jobID, err := s.scheduleJob(req)
	if err != nil {
		return &ExecuteResponse{JobId: req.GetJobId(), ExitCode: -1, Error: err.Error()}, nil
	}

	if node.ID != s.nodeID {
		return s.executeRemoteBackground(ctx, req, jobID, node, reqs)
	}
	return s.executeLocalBackground(req, jobID, node, reqs)
}

func (s *ClusterServerImpl) executeRemoteBackground(ctx context.Context, req *ExecuteRequest, jobID cluster.JobID, node *cluster.NodeInfo, reqs cluster.Requirements) (*ExecuteResponse, error) {
	if s.clientPool == nil {
		s.failJob(jobID, node.ID, reqs, "no peer connections available")
		return &ExecuteResponse{JobId: string(jobID), ExitCode: -1, Error: "no peer connections available"}, nil
	}

	client, ok := s.clientPool.GetPeer(node.ID)
	if !ok {
		errMsg := fmt.Sprintf("no connection to node %s", node.ID)
		s.failJob(jobID, node.ID, reqs, errMsg)
		return &ExecuteResponse{JobId: string(jobID), ExitCode: -1, Error: errMsg}, nil
	}

	forwarded := cloneExecuteRequestForNode(req, node.ID)
	resp, err := client.Execute(ctx, forwarded)
	if err != nil {
		s.failJob(jobID, node.ID, reqs, err.Error())
		return nil, err
	}
	if resp.Error != "" {
		s.failJob(jobID, node.ID, reqs, resp.Error)
		return resp, nil
	}

	go s.followRemoteJob(context.Background(), client, jobID, node.ID, reqs)
	return resp, nil
}

func (s *ClusterServerImpl) executeLocalBackground(req *ExecuteRequest, jobID cluster.JobID, node *cluster.NodeInfo, reqs cluster.Requirements) (*ExecuteResponse, error) {
	cmd, err := s.buildCommand(context.Background(), req)
	if err != nil {
		s.failJob(jobID, node.ID, reqs, err.Error())
		return &ExecuteResponse{JobId: string(jobID), ExitCode: -1, Error: err.Error()}, nil
	}
	if err := cmd.Start(); err != nil {
		s.failJob(jobID, node.ID, reqs, fmt.Sprintf("start: %v", err))
		return &ExecuteResponse{JobId: string(jobID), ExitCode: -1, Error: fmt.Sprintf("start: %v", err)}, nil
	}

	globalPID := s.registerProcess(req, jobID, cmd.Process.Pid)
	s.markJobStarted(jobID)

	go func() {
		exitCode := waitExitCode(cmd.Wait())
		s.tracker.Unregister(globalPID)
		s.finishJob(jobID, node.ID, reqs, exitCode)
	}()

	return &ExecuteResponse{
		JobId:     string(jobID),
		GlobalPid: uint64(globalPID),
	}, nil
}

func (s *ClusterServerImpl) executeLocalStream(stream Cluster_ExecStreamServer, req *ExecuteRequest, jobID cluster.JobID, node *cluster.NodeInfo, reqs cluster.Requirements) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	cmd, err := s.buildCommand(ctx, req)
	if err != nil {
		s.failJob(jobID, node.ID, reqs, err.Error())
		return sendStreamError(stream, err.Error())
	}
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		s.failJob(jobID, node.ID, reqs, fmt.Sprintf("stdin pipe: %v", err))
		return sendStreamError(stream, fmt.Sprintf("stdin pipe: %v", err))
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		s.failJob(jobID, node.ID, reqs, fmt.Sprintf("stdout pipe: %v", err))
		return sendStreamError(stream, fmt.Sprintf("stdout pipe: %v", err))
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		s.failJob(jobID, node.ID, reqs, fmt.Sprintf("stderr pipe: %v", err))
		return sendStreamError(stream, fmt.Sprintf("stderr pipe: %v", err))
	}

	if err := cmd.Start(); err != nil {
		s.failJob(jobID, node.ID, reqs, fmt.Sprintf("start: %v", err))
		return sendStreamError(stream, fmt.Sprintf("start: %v", err))
	}

	globalPID := s.registerProcess(req, jobID, cmd.Process.Pid)
	s.markJobStarted(jobID)
	defer s.tracker.Unregister(globalPID)

	var sendMu sync.Mutex
	send := func(resp *ExecStreamResponse) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(resp)
	}

	if err := send(&ExecStreamResponse{Payload: &ExecStreamResponse_Exec{
		Exec: &ExecuteResponse{
			JobId:     string(jobID),
			GlobalPid: uint64(globalPID),
		},
	}}); err != nil {
		cancel()
		s.failJob(jobID, node.ID, reqs, err.Error())
		return err
	}

	var stdinCloseOnce sync.Once
	closeStdin := func() {
		stdinCloseOnce.Do(func() {
			_ = stdinPipe.Close()
		})
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go copyStreamOutput(&wg, stdoutPipe, func(data []byte) error {
		return send(&ExecStreamResponse{Payload: &ExecStreamResponse_StdoutData{StdoutData: data}})
	})
	go copyStreamOutput(&wg, stderrPipe, func(data []byte) error {
		return send(&ExecStreamResponse{Payload: &ExecStreamResponse_StderrData{StderrData: data}})
	})
	go func() {
		defer closeStdin()
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			switch payload := msg.Payload.(type) {
			case *ExecStreamRequest_StdinData:
				if len(payload.StdinData) == 0 {
					continue
				}
				if _, err := stdinPipe.Write(payload.StdinData); err != nil {
					return
				}
			case *ExecStreamRequest_CloseStdin:
				closeStdin()
			case *ExecStreamRequest_Signal:
				if cmd.Process != nil {
					_ = syscall.Kill(-cmd.Process.Pid, syscall.Signal(payload.Signal.Signal))
				}
			}
		}
	}()

	exitCode := waitExitCode(cmd.Wait())
	closeStdin()
	cancel()
	wg.Wait()

	s.finishJob(jobID, node.ID, reqs, exitCode)
	return send(&ExecStreamResponse{Payload: &ExecStreamResponse_ExitCode{ExitCode: exitCode}})
}

func (s *ClusterServerImpl) executeLocalPTYStream(stream Cluster_ExecStreamServer, req *ExecuteRequest, jobID cluster.JobID, node *cluster.NodeInfo, reqs cluster.Requirements) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	cmd, err := s.buildPTYCommand(ctx, req)
	if err != nil {
		s.failJob(jobID, node.ID, reqs, err.Error())
		return sendStreamError(stream, err.Error())
	}
	ptmx, err := pty.StartWithSize(cmd, initialPTYSize(req.Env))
	if err != nil {
		s.failJob(jobID, node.ID, reqs, fmt.Sprintf("start pty: %v", err))
		return sendStreamError(stream, fmt.Sprintf("start pty: %v", err))
	}
	defer ptmx.Close()

	globalPID := s.registerProcess(req, jobID, cmd.Process.Pid)
	s.markJobStarted(jobID)
	defer s.tracker.Unregister(globalPID)

	var sendMu sync.Mutex
	send := func(resp *ExecStreamResponse) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(resp)
	}

	if err := send(&ExecStreamResponse{Payload: &ExecStreamResponse_Exec{
		Exec: &ExecuteResponse{
			JobId:     string(jobID),
			GlobalPid: uint64(globalPID),
		},
	}}); err != nil {
		cancel()
		s.failJob(jobID, node.ID, reqs, err.Error())
		return err
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go copyStreamOutput(&wg, ptmx, func(data []byte) error {
		return send(&ExecStreamResponse{Payload: &ExecStreamResponse_StdoutData{StdoutData: data}})
	})

	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			switch payload := msg.Payload.(type) {
			case *ExecStreamRequest_StdinData:
				if len(payload.StdinData) == 0 {
					continue
				}
				if _, err := ptmx.Write(payload.StdinData); err != nil {
					return
				}
			case *ExecStreamRequest_CloseStdin:
				return
			case *ExecStreamRequest_Signal:
				if handlePTYSignal(ptmx, cmd, payload.Signal) {
					continue
				}
			}
		}
	}()

	exitCode := waitExitCode(cmd.Wait())
	_ = ptmx.Close()
	cancel()
	wg.Wait()

	s.finishJob(jobID, node.ID, reqs, exitCode)
	return send(&ExecStreamResponse{Payload: &ExecStreamResponse_ExitCode{ExitCode: exitCode}})
}

func (s *ClusterServerImpl) proxyExecStream(stream Cluster_ExecStreamServer, req *ExecuteRequest, jobID cluster.JobID, node *cluster.NodeInfo, reqs cluster.Requirements) error {
	if s.clientPool == nil {
		errMsg := "no peer connections available"
		s.failJob(jobID, node.ID, reqs, errMsg)
		return sendStreamError(stream, errMsg)
	}

	client, ok := s.clientPool.GetPeer(node.ID)
	if !ok {
		errMsg := fmt.Sprintf("no connection to node %s", node.ID)
		s.failJob(jobID, node.ID, reqs, errMsg)
		return sendStreamError(stream, errMsg)
	}

	peerStream, err := client.ExecStream(stream.Context())
	if err != nil {
		s.failJob(jobID, node.ID, reqs, err.Error())
		return sendStreamError(stream, err.Error())
	}
	if err := peerStream.Send(&ExecStreamRequest{Payload: &ExecStreamRequest_Exec{Exec: cloneExecuteRequestForNode(req, node.ID)}}); err != nil {
		s.failJob(jobID, node.ID, reqs, err.Error())
		return sendStreamError(stream, err.Error())
	}

	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		defer peerStream.CloseSend()
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			if err := peerStream.Send(msg); err != nil {
				return
			}
		}
	}()

	for {
		resp, err := peerStream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			s.failJob(jobID, node.ID, reqs, err.Error())
			_ = sendStreamError(stream, err.Error())
			return nil
		}

		switch payload := resp.Payload.(type) {
		case *ExecStreamResponse_ExitCode:
			s.finishJob(jobID, node.ID, reqs, payload.ExitCode)
		case *ExecStreamResponse_Error:
			s.failJob(jobID, node.ID, reqs, payload.Error)
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// Wait waits for a background job to reach a terminal state.
func (s *ClusterServerImpl) Wait(ctx context.Context, req *WaitRequest) (*WaitResponse, error) {
	interval := 200 * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var timeout <-chan time.Time
	if req.TimeoutMs > 0 {
		timer := time.NewTimer(time.Duration(req.TimeoutMs) * time.Millisecond)
		defer timer.Stop()
		timeout = timer.C
	}

	for {
		resp, err := s.GetJobStatus(ctx, &GetJobStatusRequest{JobId: req.JobId})
		if err != nil {
			return nil, err
		}
		if !resp.Found || resp.Job == nil {
			return &WaitResponse{ExitCode: -1, Error: "job not found"}, nil
		}
		switch resp.Job.Status {
		case JobStatus_JOB_STATUS_COMPLETED:
			exitCode := int32(0)
			if len(resp.Job.ExitCodes) > 0 {
				exitCode = resp.Job.ExitCodes[0]
			}
			return &WaitResponse{ExitCode: exitCode}, nil
		case JobStatus_JOB_STATUS_FAILED, JobStatus_JOB_STATUS_STOPPED:
			exitCode := int32(-1)
			if len(resp.Job.ExitCodes) > 0 {
				exitCode = resp.Job.ExitCodes[0]
			}
			return &WaitResponse{ExitCode: exitCode, Error: resp.Job.Status.String()}, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout:
			return &WaitResponse{TimedOut: true}, nil
		case <-ticker.C:
		}
	}
}

func (s *ClusterServerImpl) scheduleJob(req *ExecuteRequest) (*cluster.NodeInfo, cluster.Requirements, cluster.JobID, error) {
	if req.JobId == "" {
		req.JobId = fmt.Sprintf("job-%d", time.Now().UnixNano())
	}
	if err := s.normalizeScheduledRequest(req); err != nil {
		return nil, cluster.Requirements{}, cluster.JobID(req.JobId), err
	}
	if req.Resources == nil {
		req.Resources = &Requirements{}
	}
	req.Resources.SessionId = req.SessionId
	req.Resources.JobId = req.JobId

	reqs := s.protoToRequirements(req.Resources)
	jobID := cluster.JobID(req.JobId)

	s.mu.Lock()
	defer s.mu.Unlock()

	node, err := s.scheduler.Schedule(reqs)
	if err != nil {
		return nil, reqs, jobID, err
	}

	s.cluster.jobs[jobID] = &cluster.JobInfo{
		ID:          jobID,
		SessionID:   cluster.SessionID(req.SessionId),
		Commands:    s.protoToCommands(req.Command, req.Env, req.Cwd, reqs),
		PipeMap:     req.PipeMap,
		Status:      cluster.JobStatusRunning,
		CreatedAt:   time.Now(),
		PrimaryNode: node.ID,
	}

	return node, reqs, jobID, nil
}

func (s *ClusterServerImpl) registerProcess(req *ExecuteRequest, jobID cluster.JobID, localPID int) cluster.GlobalPID {
	localSeq := atomic.AddUint64(&s.localPIDCounter, 1)
	globalPID := cluster.GlobalPID(localSeq)
	s.tracker.Register(&cluster.ProcessInfo{
		GlobalPID: globalPID,
		NodeID:    s.nodeID,
		LocalPID:  localPID,
		SessionID: cluster.SessionID(req.SessionId),
		JobID:     jobID,
		Comm:      req.Command[0],
		Cmdline:   fmt.Sprintf("%v", req.Command),
		CWD:       req.Cwd,
		StartTime: time.Now(),
	})
	return globalPID
}

func (s *ClusterServerImpl) markJobStarted(jobID cluster.JobID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job, exists := s.cluster.jobs[jobID]; exists {
		job.Status = cluster.JobStatusRunning
		job.StartedAt = time.Now()
	}
}

func (s *ClusterServerImpl) finishJob(jobID cluster.JobID, nodeID cluster.NodeID, reqs cluster.Requirements, exitCode int32) {
	s.mu.Lock()
	if job, exists := s.cluster.jobs[jobID]; exists {
		job.Status = cluster.JobStatusCompleted
		job.ExitCodes = []int{int(exitCode)}
		job.FinishedAt = time.Now()
	}
	s.mu.Unlock()
	s.scheduler.Release(nodeID, reqs)
}

func (s *ClusterServerImpl) failJob(jobID cluster.JobID, nodeID cluster.NodeID, reqs cluster.Requirements, _ string) {
	s.mu.Lock()
	if job, exists := s.cluster.jobs[jobID]; exists {
		job.Status = cluster.JobStatusFailed
		job.ExitCodes = []int{-1}
		job.FinishedAt = time.Now()
	}
	s.mu.Unlock()
	if nodeID != "" {
		s.scheduler.Release(nodeID, reqs)
	}
}

func (s *ClusterServerImpl) followRemoteJob(ctx context.Context, client ClusterClient, jobID cluster.JobID, nodeID cluster.NodeID, reqs cluster.Requirements) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			resp, err := client.GetJobStatus(ctx, &GetJobStatusRequest{JobId: string(jobID)})
			if err != nil || !resp.Found || resp.Job == nil {
				continue
			}
			switch resp.Job.Status {
			case JobStatus_JOB_STATUS_COMPLETED, JobStatus_JOB_STATUS_FAILED, JobStatus_JOB_STATUS_STOPPED:
				exitCode := int32(-1)
				if len(resp.Job.ExitCodes) > 0 {
					exitCode = resp.Job.ExitCodes[0]
				}
				if resp.Job.Status == JobStatus_JOB_STATUS_COMPLETED {
					s.finishJob(jobID, nodeID, reqs, exitCode)
				} else {
					s.failJob(jobID, nodeID, reqs, "")
				}
				return
			}
		}
	}
}

func (s *ClusterServerImpl) buildCommand(ctx context.Context, req *ExecuteRequest) (*exec.Cmd, error) {
	if s.ssiConfig.Required {
		return s.buildSSICommand(ctx, req, true)
	}
	dir, err := s.localExecDir(req)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, req.Command[0], req.Command[1:]...)
	cmd.Dir = dir
	if len(req.Env) > 0 {
		cmd.Env = append(os.Environ(), req.Env...)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return cmd, nil
}

func (s *ClusterServerImpl) buildPTYCommand(ctx context.Context, req *ExecuteRequest) (*exec.Cmd, error) {
	if s.ssiConfig.Required {
		return s.buildSSICommand(ctx, req, false)
	}
	dir, err := s.localExecDir(req)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, req.Command[0], req.Command[1:]...)
	cmd.Dir = dir
	if len(req.Env) > 0 {
		cmd.Env = append(os.Environ(), req.Env...)
	}
	return cmd, nil
}

func (s *ClusterServerImpl) buildSSICommand(ctx context.Context, req *ExecuteRequest, setProcessGroup bool) (*exec.Cmd, error) {
	cfg := s.ssiConfig.WithDefaults()
	if err := ssi.Validate(cfg); err != nil {
		return nil, fmt.Errorf("SSI is not ready on node %s: %w", s.nodeID, err)
	}
	hostDir, logicalDir, err := ssi.ResolveClusterCWD(cfg.ClusterRoot, cfg.RootFS, req.Cwd)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(hostDir)
	if err != nil {
		return nil, fmt.Errorf("cwd %s is unavailable in SSI root: %w", logicalDir, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("cwd %s is not a directory in SSI root", logicalDir)
	}

	args := []string{
		"--rootfs", cfg.RootFS,
		"--mount-base", cfg.MountBase,
		"--cwd", logicalDir,
		"--",
	}
	args = append(args, req.Command...)
	cmd := exec.CommandContext(ctx, cfg.Executor, args...)
	cmd.Env = append(os.Environ(), req.Env...)
	if setProcessGroup {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	}
	return cmd, nil
}

func (s *ClusterServerImpl) normalizeScheduledRequest(req *ExecuteRequest) error {
	if !s.ssiConfig.Required {
		return nil
	}
	cfg := s.ssiConfig.WithDefaults()
	_, logicalDir, err := ssi.ResolveClusterCWD(cfg.ClusterRoot, cfg.RootFS, req.Cwd)
	if err != nil {
		return err
	}
	req.Cwd = logicalDir
	req.Env = appendMissingEnv(req.Env,
		"OCTOPOS_SSI=1",
		"OCTOPOS_CLUSTER_ROOT=/",
		"OCTOPOS_CLUSTER_HOSTNAME="+ssi.DefaultHostname,
		"OCTOPOS_HOST_CLUSTER_ROOT="+cfg.ClusterRoot,
		"OCTOPOS_NODE_ID="+string(s.nodeID),
	)
	return nil
}

func (s *ClusterServerImpl) localExecDir(req *ExecuteRequest) (string, error) {
	if !s.ssiConfig.Required {
		return req.Cwd, nil
	}
	cfg := s.ssiConfig.WithDefaults()
	if err := ssi.Validate(cfg); err != nil {
		return "", fmt.Errorf("SSI is not ready on node %s: %w", s.nodeID, err)
	}
	hostDir, _, err := ssi.ResolveClusterCWD(cfg.ClusterRoot, cfg.RootFS, req.Cwd)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(hostDir)
	if err != nil {
		return "", fmt.Errorf("cwd %s is unavailable in SSI root: %w", req.Cwd, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("cwd %s is not a directory in SSI root", req.Cwd)
	}
	return hostDir, nil
}

func appendMissingEnv(env []string, values ...string) []string {
	seen := make(map[string]bool, len(env))
	for _, entry := range env {
		key, _, ok := strings.Cut(entry, "=")
		if ok {
			seen[key] = true
		}
	}
	for _, entry := range values {
		key, _, ok := strings.Cut(entry, "=")
		if ok && seen[key] {
			continue
		}
		env = append(env, entry)
	}
	return env
}

func cloneExecuteRequestForNode(req *ExecuteRequest, nodeID cluster.NodeID) *ExecuteRequest {
	forwarded := *req
	if req.Resources != nil {
		resources := *req.Resources
		resources.NodeAffinity = map[string]string{"node_id": string(nodeID)}
		forwarded.Resources = &resources
	}
	if req.PipeMap != nil {
		forwarded.PipeMap = make(map[int32]int32, len(req.PipeMap))
		for k, v := range req.PipeMap {
			forwarded.PipeMap[k] = v
		}
	}
	return &forwarded
}

func copyStreamOutput(wg *sync.WaitGroup, reader io.Reader, send func([]byte) error) {
	defer wg.Done()
	buf := make([]byte, 32*1024)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			if sendErr := send(data); sendErr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func waitExitCode(err error) int32 {
	if err == nil {
		return 0
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		if waitStatus, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			return int32(waitStatus.ExitStatus())
		}
	}
	return -1
}

func sendStreamError(stream Cluster_ExecStreamServer, msg string) error {
	return stream.Send(&ExecStreamResponse{Payload: &ExecStreamResponse_Error{Error: msg}})
}

func initialPTYSize(env []string) *pty.Winsize {
	size := &pty.Winsize{Rows: 24, Cols: 80}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		switch key {
		case "OCTOPOS_TTY_ROWS":
			if rows, err := strconv.ParseUint(value, 10, 16); err == nil && rows > 0 {
				size.Rows = uint16(rows)
			}
		case "OCTOPOS_TTY_COLS":
			if cols, err := strconv.ParseUint(value, 10, 16); err == nil && cols > 0 {
				size.Cols = uint16(cols)
			}
		}
	}
	return size
}

func handlePTYSignal(ptmx *os.File, cmd *exec.Cmd, signal *SignalRequest) bool {
	if signal == nil {
		return true
	}
	if signal.Signal == streamResizeSignal {
		rows, cols := decodePTYResize(signal.GlobalPid)
		if rows > 0 && cols > 0 {
			_ = pty.Setsize(ptmx, &pty.Winsize{Rows: rows, Cols: cols})
		}
		return true
	}
	if cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.Signal(signal.Signal))
	}
	return true
}

func decodePTYResize(encoded uint64) (uint16, uint16) {
	rows := uint16(encoded >> 32)
	cols := uint16(encoded)
	return rows, cols
}
