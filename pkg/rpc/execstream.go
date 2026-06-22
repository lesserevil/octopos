package rpc

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
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
	"github.com/octopos/octopos/pkg/nvidia"
	"github.com/octopos/octopos/pkg/remotechild"
	"github.com/octopos/octopos/pkg/ssi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const streamResizeSignal = -1
const internalForwardMetadata = "octopos-internal-forward"

type execStreamServer interface {
	Send(*ExecStreamResponse) error
	Recv() (*ExecStreamRequest, error)
	Context() context.Context
}

type execStreamClient interface {
	Send(*ExecStreamRequest) error
	Recv() (*ExecStreamResponse, error)
	CloseSend() error
}

// ExecStream handles foreground execution with stdin/stdout/stderr attached.
func (s *ClusterServerImpl) ExecStream(stream Cluster_ExecStreamServer) error {
	return s.execStream(stream, false)
}

// RemoteChildStream handles foreground distributed-child execution. It requires
// authenticated remote-child metadata on the initial ExecuteRequest.
func (s *ClusterServerImpl) RemoteChildStream(stream Cluster_RemoteChildStreamServer) error {
	return s.execStream(stream, true)
}

func (s *ClusterServerImpl) execStream(stream execStreamServer, requireRemoteChild bool) error {
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
	if requireRemoteChild && !isRemoteChildRequest(req) {
		return sendStreamError(stream, fmt.Sprintf("remote child request missing %s=1", remotechild.EnvRemoteChild))
	}
	if err := s.authorizeRemoteChildRequest(stream.Context(), req); err != nil {
		return sendStreamError(stream, err.Error())
	}

	node, reqs, jobID, err := s.scheduleJob(req)
	if err != nil {
		return sendStreamError(stream, err.Error())
	}

	if node.ID != s.nodeID {
		return s.proxyExecStream(stream, req, jobID, node, reqs)
	}
	if req.Tty {
		return s.executeLocalPTYStream(stream, req, jobID, node, reqs, requireRemoteChild)
	}
	return s.executeLocalStream(stream, req, jobID, node, reqs, requireRemoteChild)
}

func (s *ClusterServerImpl) executeBackground(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	if len(req.Command) == 0 {
		return &ExecuteResponse{JobId: req.GetJobId(), ExitCode: -1, Error: "empty command"}, nil
	}
	if err := s.authorizeRemoteChildRequest(ctx, req); err != nil {
		return &ExecuteResponse{JobId: req.GetJobId(), ExitCode: -1, Error: err.Error()}, nil
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
	forwardCtx := internalForwardContext(ctx)
	resp, err := client.Execute(forwardCtx, forwarded)
	if err != nil {
		s.failJob(jobID, node.ID, reqs, err.Error())
		return nil, err
	}
	if resp.Error != "" {
		s.failJob(jobID, node.ID, reqs, resp.Error)
		return resp, nil
	}
	if resp.GlobalPid != 0 {
		s.recordRemoteChildWorker(jobID, cluster.GlobalPID(resp.GlobalPid))
	}

	go s.followRemoteJob(context.Background(), client, jobID, node.ID, reqs)
	return resp, nil
}

func (s *ClusterServerImpl) executeLocalBackground(req *ExecuteRequest, jobID cluster.JobID, node *cluster.NodeInfo, reqs cluster.Requirements) (*ExecuteResponse, error) {
	cmd, err := s.buildCommand(context.Background(), req, reqs)
	if err != nil {
		s.failJob(jobID, node.ID, reqs, err.Error())
		return &ExecuteResponse{JobId: string(jobID), ExitCode: -1, Error: err.Error()}, nil
	}
	if err := cmd.Start(); err != nil {
		s.failJob(jobID, node.ID, reqs, fmt.Sprintf("start: %v", err))
		return &ExecuteResponse{JobId: string(jobID), ExitCode: -1, Error: fmt.Sprintf("start: %v", err)}, nil
	}

	globalPID := s.registerProcess(req, jobID, cmd.Process.Pid)
	s.recordRemoteChildWorker(jobID, globalPID)
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

func (s *ClusterServerImpl) executeLocalStream(stream execStreamServer, req *ExecuteRequest, jobID cluster.JobID, node *cluster.NodeInfo, reqs cluster.Requirements, keepAliveOnDisconnect bool) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	cmd, err := s.buildCommand(commandContextForStream(ctx, keepAliveOnDisconnect), req, reqs)
	if err != nil {
		s.failJob(jobID, node.ID, reqs, err.Error())
		return sendStreamError(stream, err.Error())
	}
	pipeKeys := remoteChildPipeKeys(req)
	var brokeredFiles []*os.File
	var stdinPipe io.WriteCloser
	var stdoutPipe io.ReadCloser
	var stderrPipe io.ReadCloser
	if key := pipeKeys[0]; key != "" {
		file, err := s.attachPipeEndpoint(ctx, req, key, 0)
		if err != nil {
			s.failJob(jobID, node.ID, reqs, fmt.Sprintf("stdin pipe graph: %v", err))
			return sendStreamError(stream, fmt.Sprintf("stdin pipe graph: %v", err))
		}
		cmd.Stdin = file
		brokeredFiles = append(brokeredFiles, file)
	} else {
		stdinPipe, err = cmd.StdinPipe()
		if err != nil {
			s.failJob(jobID, node.ID, reqs, fmt.Sprintf("stdin pipe: %v", err))
			return sendStreamError(stream, fmt.Sprintf("stdin pipe: %v", err))
		}
	}
	if key := pipeKeys[1]; key != "" {
		file, err := s.attachPipeEndpoint(ctx, req, key, 1)
		if err != nil {
			s.failJob(jobID, node.ID, reqs, fmt.Sprintf("stdout pipe graph: %v", err))
			return sendStreamError(stream, fmt.Sprintf("stdout pipe graph: %v", err))
		}
		cmd.Stdout = file
		brokeredFiles = append(brokeredFiles, file)
	} else {
		stdoutPipe, err = cmd.StdoutPipe()
		if err != nil {
			s.failJob(jobID, node.ID, reqs, fmt.Sprintf("stdout pipe: %v", err))
			return sendStreamError(stream, fmt.Sprintf("stdout pipe: %v", err))
		}
	}
	if key := pipeKeys[2]; key != "" {
		file, err := s.attachPipeEndpoint(ctx, req, key, 2)
		if err != nil {
			s.failJob(jobID, node.ID, reqs, fmt.Sprintf("stderr pipe graph: %v", err))
			return sendStreamError(stream, fmt.Sprintf("stderr pipe graph: %v", err))
		}
		cmd.Stderr = file
		brokeredFiles = append(brokeredFiles, file)
	} else {
		stderrPipe, err = cmd.StderrPipe()
		if err != nil {
			s.failJob(jobID, node.ID, reqs, fmt.Sprintf("stderr pipe: %v", err))
			return sendStreamError(stream, fmt.Sprintf("stderr pipe: %v", err))
		}
	}

	if err := cmd.Start(); err != nil {
		for _, file := range brokeredFiles {
			_ = file.Close()
		}
		s.failJob(jobID, node.ID, reqs, fmt.Sprintf("start: %v", err))
		return sendStreamError(stream, fmt.Sprintf("start: %v", err))
	}
	for _, file := range brokeredFiles {
		_ = file.Close()
	}

	globalPID := s.registerProcess(req, jobID, cmd.Process.Pid)
	s.recordRemoteChildWorker(jobID, globalPID)
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
			if stdinPipe != nil {
				_ = stdinPipe.Close()
			}
		})
	}

	var wg sync.WaitGroup
	if stdoutPipe != nil {
		wg.Add(1)
		go copyStreamOutput(&wg, stdoutPipe, func(data []byte) error {
			return send(&ExecStreamResponse{Payload: &ExecStreamResponse_StdoutData{StdoutData: data}})
		})
	}
	if stderrPipe != nil {
		wg.Add(1)
		go copyStreamOutput(&wg, stderrPipe, func(data []byte) error {
			return send(&ExecStreamResponse{Payload: &ExecStreamResponse_StderrData{StderrData: data}})
		})
	}
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
				if stdinPipe == nil {
					continue
				}
				if _, err := stdinPipe.Write(payload.StdinData); err != nil {
					return
				}
			case *ExecStreamRequest_CloseStdin:
				closeStdin()
			case *ExecStreamRequest_Signal:
				if cmd.Process != nil {
					s.markJobSignalState(jobID, syscall.Signal(payload.Signal.Signal))
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

func (s *ClusterServerImpl) executeLocalPTYStream(stream execStreamServer, req *ExecuteRequest, jobID cluster.JobID, node *cluster.NodeInfo, reqs cluster.Requirements, keepAliveOnDisconnect bool) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	ptmx, tty, err := pty.Open()
	if err != nil {
		s.failJob(jobID, node.ID, reqs, fmt.Sprintf("open pty: %v", err))
		return sendStreamError(stream, fmt.Sprintf("open pty: %v", err))
	}
	defer ptmx.Close()
	defer tty.Close()
	if size := initialPTYSize(req.Env); size != nil {
		if err := pty.Setsize(ptmx, size); err != nil {
			s.failJob(jobID, node.ID, reqs, fmt.Sprintf("resize pty: %v", err))
			return sendStreamError(stream, fmt.Sprintf("resize pty: %v", err))
		}
	}

	cmd, err := s.buildPTYCommand(commandContextForStream(ctx, keepAliveOnDisconnect), req, reqs, tty.Name())
	if err != nil {
		s.failJob(jobID, node.ID, reqs, err.Error())
		return sendStreamError(stream, err.Error())
	}
	cmd.Stdin = tty
	cmd.Stdout = tty
	cmd.Stderr = tty
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setsid = true
	cmd.SysProcAttr.Setctty = true
	if err := cmd.Start(); err != nil {
		s.failJob(jobID, node.ID, reqs, fmt.Sprintf("start pty: %v", err))
		return sendStreamError(stream, fmt.Sprintf("start pty: %v", err))
	}
	_ = tty.Close()

	globalPID := s.registerProcess(req, jobID, cmd.Process.Pid)
	s.recordRemoteChildWorker(jobID, globalPID)
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
				if payload.Signal != nil {
					s.markJobSignalState(jobID, syscall.Signal(payload.Signal.Signal))
				}
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

func commandContextForStream(ctx context.Context, keepAliveOnDisconnect bool) context.Context {
	if keepAliveOnDisconnect {
		return context.Background()
	}
	return ctx
}

func (s *ClusterServerImpl) proxyExecStream(stream execStreamServer, req *ExecuteRequest, jobID cluster.JobID, node *cluster.NodeInfo, reqs cluster.Requirements) error {
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

	forwardCtx := internalForwardContext(stream.Context())
	var peerStream execStreamClient
	var err error
	if isRemoteChildRequest(req) {
		peerStream, err = client.RemoteChildStream(forwardCtx)
	} else {
		peerStream, err = client.ExecStream(forwardCtx)
	}
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
			if payload := msg.GetSignal(); payload != nil {
				s.markJobSignalState(jobID, syscall.Signal(payload.Signal))
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
		case *ExecStreamResponse_Exec:
			if payload.Exec != nil && payload.Exec.GlobalPid != 0 {
				s.recordRemoteChildWorker(jobID, cluster.GlobalPID(payload.Exec.GlobalPid))
			}
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
		case JobStatus_JOB_STATUS_FAILED:
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
	if err := s.normalizeScheduledCWD(req); err != nil {
		return nil, cluster.Requirements{}, cluster.JobID(req.JobId), err
	}
	if req.Resources == nil {
		req.Resources = &Requirements{}
	}
	pipeKeys := remoteChildPipeKeys(req)
	if isRemoteChildRequest(req) && len(pipeKeys) > 0 {
		s.applyPipePlacementAffinity(req, pipeKeys)
	}
	req.Resources.SessionId = req.SessionId
	req.Resources.JobId = req.JobId

	reqs := s.protoToRequirements(req.Resources)
	jobID := cluster.JobID(req.JobId)

	s.mu.Lock()
	defer s.mu.Unlock()

	node, allocatedReqs, err := s.scheduler.ScheduleWithAllocation(reqs)
	if err != nil {
		return nil, reqs, jobID, err
	}
	reqs = allocatedReqs
	remoteInfo := remoteChildInfoFromEnv(req, jobID, node.ID)
	if remoteInfo != nil && s.maxRemoteChildrenPerNode > 0 {
		activeOnNode := s.remoteChildren.CountActive(remotechild.ListFilter{RemoteNodeID: string(node.ID)})
		if activeOnNode >= s.maxRemoteChildrenPerNode {
			s.scheduler.Release(node.ID, reqs)
			return nil, reqs, jobID, fmt.Errorf("remote child limit exceeded for node %s: %d active, limit %d", node.ID, activeOnNode, s.maxRemoteChildrenPerNode)
		}
	}
	childToken, err := generateChildToken()
	if err != nil {
		s.scheduler.Release(node.ID, reqs)
		return nil, reqs, jobID, err
	}
	s.applyScheduledEnv(req, node.ID, childToken)
	now := time.Now()
	childTokenExpiresAt := time.Time{}
	if s.remoteChildTokenTTL > 0 {
		childTokenExpiresAt = now.Add(s.remoteChildTokenTTL)
	}

	s.cluster.jobs[jobID] = &cluster.JobInfo{
		ID:                  jobID,
		SessionID:           cluster.SessionID(req.SessionId),
		Commands:            s.protoToCommands(req.Command, req.Env, req.Cwd, reqs),
		PipeMap:             req.PipeMap,
		Status:              cluster.JobStatusRunning,
		CreatedAt:           now,
		PrimaryNode:         node.ID,
		RemoteChild:         remoteInfo,
		ChildToken:          childToken,
		ChildTokenExpiresAt: childTokenExpiresAt,
	}
	if remoteInfo != nil {
		s.remoteChildren.Upsert(remoteChildRecordFromInfo(req, remoteInfo))
		s.recordRemoteChildAudit(req, "spawn", "scheduled", "", remoteInfo)
	}
	s.pipes.recordPlacement(pipeKeys, node.ID)

	return node, reqs, jobID, nil
}

func (s *ClusterServerImpl) applyPipePlacementAffinity(req *ExecuteRequest, pipeKeys map[int]string) {
	if req.Resources == nil || req.Resources.NodeAffinity == nil {
		if req.Resources == nil {
			req.Resources = &Requirements{}
		}
		req.Resources.NodeAffinity = make(map[string]string)
	}
	if req.Resources.NodeAffinity["node_id"] != "" {
		return
	}
	if nodeID := s.pipes.preferredNode(pipeKeys); nodeID != "" {
		req.Resources.NodeAffinity["node_id"] = string(nodeID)
		delete(req.Resources.NodeAffinity, "prefer_not_node_id")
	}
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
		now := time.Now()
		job.Status = cluster.JobStatusRunning
		job.StartedAt = now
		if job.RemoteChild != nil {
			job.RemoteChild.State = remotechild.StateRunning
			if job.RemoteChild.StartedAt.IsZero() {
				job.RemoteChild.StartedAt = now
			}
			s.remoteChildren.MarkRunning(string(jobID), job.RemoteChild.RemoteGlobalPID, now)
		}
	}
}

func (s *ClusterServerImpl) recordRemoteChildWorker(jobID cluster.JobID, globalPID cluster.GlobalPID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job, exists := s.cluster.jobs[jobID]; exists && job.RemoteChild != nil {
		now := time.Now()
		localPID := 0
		if proc, ok := s.tracker.Get(globalPID); ok && proc.NodeID == s.nodeID {
			localPID = proc.LocalPID
		}
		if job.StartedAt.IsZero() {
			job.StartedAt = now
		}
		job.RemoteChild.RemoteGlobalPID = uint64(globalPID)
		if localPID > 0 {
			job.RemoteChild.RemoteLocalPID = localPID
		}
		job.RemoteChild.State = remotechild.StateRunning
		if job.RemoteChild.StartedAt.IsZero() {
			job.RemoteChild.StartedAt = now
		}
		s.remoteChildren.MarkRunningWithLocalPID(string(jobID), uint64(globalPID), localPID, now)
	}
}

func (s *ClusterServerImpl) markJobStopped(jobID cluster.JobID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job, exists := s.cluster.jobs[jobID]; exists && !isTerminalJobStatus(job.Status) {
		job.Status = cluster.JobStatusStopped
	}
}

func (s *ClusterServerImpl) markJobSignalState(jobID cluster.JobID, sig syscall.Signal) {
	var status cluster.JobStatus
	var childState remotechild.ShadowState
	var reason string
	switch sig {
	case syscall.SIGTSTP, syscall.SIGSTOP, syscall.SIGTTIN, syscall.SIGTTOU:
		status = cluster.JobStatusStopped
		childState = remotechild.StateStopped
		reason = fmt.Sprintf("stopped by signal %d", sig)
	case syscall.SIGCONT:
		status = cluster.JobStatusRunning
		childState = remotechild.StateRunning
	default:
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	job, exists := s.cluster.jobs[jobID]
	if !exists || isTerminalJobStatus(job.Status) && job.Status != cluster.JobStatusStopped {
		return
	}
	job.Status = status
	if job.RemoteChild != nil {
		job.RemoteChild.State = string(childState)
		if childState == remotechild.StateStopped {
			job.RemoteChild.FailureReason = reason
		} else {
			job.RemoteChild.FailureReason = ""
		}
		s.remoteChildren.MarkState(string(jobID), childState, reason, time.Now())
	}
}

func (s *ClusterServerImpl) authorizeRemoteChildRequest(ctx context.Context, req *ExecuteRequest) error {
	if !isRemoteChildRequest(req) {
		return nil
	}
	if isInternalForward(ctx) {
		return nil
	}

	parentJobID := cluster.JobID(requestEnvValue(req.Env, remotechild.EnvParentJobID))
	token := requestEnvValue(req.Env, remotechild.EnvChildToken)
	if parentJobID == "" {
		err := fmt.Errorf("remote child request missing %s", remotechild.EnvParentJobID)
		s.recordRemoteChildAudit(req, "authorize", "rejected", err.Error(), nil)
		return err
	}
	if token == "" {
		err := fmt.Errorf("remote child request missing %s", remotechild.EnvChildToken)
		s.recordRemoteChildAudit(req, "authorize", "rejected", err.Error(), nil)
		return err
	}

	s.mu.RLock()
	parent, exists := s.cluster.jobs[parentJobID]
	if !exists {
		s.mu.RUnlock()
		err := fmt.Errorf("remote child parent job not found: %s", parentJobID)
		s.recordRemoteChildAudit(req, "authorize", "rejected", err.Error(), nil)
		return err
	}
	if parent.SessionID != cluster.SessionID(req.SessionId) {
		s.mu.RUnlock()
		err := fmt.Errorf("remote child session %s does not match parent job %s", req.SessionId, parentJobID)
		s.recordRemoteChildAudit(req, "authorize", "rejected", err.Error(), nil)
		return err
	}
	if parent.ChildToken == "" || subtle.ConstantTimeCompare([]byte(token), []byte(parent.ChildToken)) != 1 {
		s.mu.RUnlock()
		err := fmt.Errorf("remote child token rejected for parent job %s", parentJobID)
		s.recordRemoteChildAudit(req, "authorize", "rejected", err.Error(), nil)
		return err
	}
	if !parent.ChildTokenExpiresAt.IsZero() && time.Now().After(parent.ChildTokenExpiresAt) {
		s.mu.RUnlock()
		err := fmt.Errorf("remote child token expired for parent job %s", parentJobID)
		s.recordRemoteChildAudit(req, "authorize", "rejected", err.Error(), nil)
		return err
	}
	if parent.Status != cluster.JobStatusRunning {
		s.mu.RUnlock()
		err := fmt.Errorf("remote child parent job %s is not running", parentJobID)
		s.recordRemoteChildAudit(req, "authorize", "rejected", err.Error(), nil)
		return err
	}
	activeChildren := 0
	activeChildren = s.remoteChildren.CountActive(remotechild.ListFilter{ParentJobID: string(parentJobID)})
	activeSessionChildren := s.remoteChildren.CountActive(remotechild.ListFilter{SessionID: req.SessionId})
	limit := s.maxRemoteChildrenPerParent
	sessionLimit := s.maxRemoteChildrenPerSession
	s.mu.RUnlock()
	if limit > 0 && activeChildren >= limit {
		err := fmt.Errorf("remote child limit exceeded for parent job %s: %d active, limit %d", parentJobID, activeChildren, limit)
		s.recordRemoteChildAudit(req, "authorize", "rejected", err.Error(), nil)
		return err
	}
	if sessionLimit > 0 && activeSessionChildren >= sessionLimit {
		err := fmt.Errorf("remote child limit exceeded for session %s: %d active, limit %d", req.SessionId, activeSessionChildren, sessionLimit)
		s.recordRemoteChildAudit(req, "authorize", "rejected", err.Error(), nil)
		return err
	}
	s.recordRemoteChildAudit(req, "authorize", "accepted", "", nil)
	return nil
}

func isRemoteChildRequest(req *ExecuteRequest) bool {
	return req != nil && requestEnvValue(req.Env, remotechild.EnvRemoteChild) == "1"
}

func isTerminalJobStatus(status cluster.JobStatus) bool {
	switch status {
	case cluster.JobStatusCompleted, cluster.JobStatusFailed:
		return true
	default:
		return false
	}
}

func isInternalForward(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	for _, value := range md.Get(internalForwardMetadata) {
		if value == "1" {
			return true
		}
	}
	return false
}

func internalForwardContext(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, internalForwardMetadata, "1")
}

func (s *ClusterServerImpl) finishJob(jobID cluster.JobID, nodeID cluster.NodeID, reqs cluster.Requirements, exitCode int32) {
	var stopChildren bool
	s.mu.Lock()
	if job, exists := s.cluster.jobs[jobID]; exists {
		now := time.Now()
		job.Status = cluster.JobStatusCompleted
		job.ExitCodes = []int{int(exitCode)}
		job.FinishedAt = now
		if job.RemoteChild != nil {
			job.RemoteChild.State = remotechild.StateCompleted
			job.RemoteChild.FinishedAt = now
			s.remoteChildren.MarkFinished(string(jobID), remotechild.StateCompleted, int(exitCode), 0, "", now)
		} else {
			stopChildren = true
		}
	}
	s.releaseVFIOAllocationsForJobLocked(jobID)
	s.mu.Unlock()
	s.scheduler.Release(nodeID, reqs)
	if stopChildren {
		s.stopRemoteChildrenForParent(jobID, "parent job completed")
	}
}

func (s *ClusterServerImpl) failJob(jobID cluster.JobID, nodeID cluster.NodeID, reqs cluster.Requirements, reason string) {
	var stopChildren bool
	s.mu.Lock()
	if job, exists := s.cluster.jobs[jobID]; exists {
		now := time.Now()
		job.Status = cluster.JobStatusFailed
		job.ExitCodes = []int{-1}
		job.FinishedAt = now
		if job.RemoteChild != nil {
			job.RemoteChild.State = remotechild.StateFailed
			job.RemoteChild.FailureReason = reason
			job.RemoteChild.FinishedAt = now
			s.remoteChildren.MarkFinished(string(jobID), remotechild.StateFailed, -1, 0, reason, now)
		} else {
			stopChildren = true
		}
	}
	s.releaseVFIOAllocationsForJobLocked(jobID)
	s.mu.Unlock()
	if nodeID != "" {
		s.scheduler.Release(nodeID, reqs)
	}
	if stopChildren {
		s.stopRemoteChildrenForParent(jobID, reason)
	}
}

func (s *ClusterServerImpl) followRemoteJob(ctx context.Context, client remoteJobStatusClient, jobID cluster.JobID, nodeID cluster.NodeID, reqs cluster.Requirements) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var missingSince time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			resp, err := client.GetJobStatus(ctx, &GetJobStatusRequest{JobId: string(jobID)})
			if err != nil || !resp.Found || resp.Job == nil {
				now := time.Now()
				if missingSince.IsZero() {
					missingSince = now
					continue
				}
				if s.remoteChildLeaseTimeout > 0 && now.Sub(missingSince) >= s.remoteChildLeaseTimeout {
					s.failJob(jobID, nodeID, reqs, fmt.Sprintf("remote child lease expired after %s without status from node %s", s.remoteChildLeaseTimeout, nodeID))
					return
				}
				continue
			}
			missingSince = time.Time{}
			switch resp.Job.Status {
			case JobStatus_JOB_STATUS_RUNNING:
				if resp.Job.RemoteChild != nil {
					s.applyRecoveredRemoteChildJob(remotechild.ShadowRecord{
						RemoteJobID:  string(jobID),
						RemoteNodeID: string(nodeID),
					}, resp.Job, time.Now())
				}
			case JobStatus_JOB_STATUS_STOPPED:
				if resp.Job.RemoteChild != nil {
					s.applyRecoveredRemoteChildJob(remotechild.ShadowRecord{
						RemoteJobID:  string(jobID),
						RemoteNodeID: string(nodeID),
					}, resp.Job, time.Now())
				} else {
					s.markJobStopped(jobID)
				}
			case JobStatus_JOB_STATUS_COMPLETED, JobStatus_JOB_STATUS_FAILED:
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

func (s *ClusterServerImpl) buildCommand(ctx context.Context, req *ExecuteRequest, reqs cluster.Requirements) (*exec.Cmd, error) {
	if s.ssiConfig.Required {
		return s.buildSSICommand(ctx, req, reqs, true, "")
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

func (s *ClusterServerImpl) buildPTYCommand(ctx context.Context, req *ExecuteRequest, reqs cluster.Requirements, ttySlavePath string) (*exec.Cmd, error) {
	if s.ssiConfig.Required {
		return s.buildSSICommand(ctx, req, reqs, false, ttySlavePath)
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

func (s *ClusterServerImpl) buildSSICommand(ctx context.Context, req *ExecuteRequest, reqs cluster.Requirements, setProcessGroup bool, ttySlavePath string) (*exec.Cmd, error) {
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
	}
	if ttySlavePath != "" {
		args = append(args, "--tty-slave", ttySlavePath)
	}
	if len(reqs.GPUDevices) > 0 {
		args = append(args,
			"--nvidia-gpus", nvidia.EncodeDeviceSpec(reqs.GPUDevices),
			"--nvidia-projection", nvidia.DefaultProjectionRoot,
			"--nvidia-capabilities", nvidia.DefaultDriverCapabilities,
		)
	}
	if len(reqs.VFIOGroups) > 0 {
		args = append(args, "--vfio-groups", vfioGroupFlag(reqs.VFIOGroups))
	}
	if isRemoteChildRequest(req) {
		args = append(args, "--exit-status-file", remotechild.WorkerExitStatusPath(req.JobId))
	}
	args = append(args, "--")
	args = append(args, req.Command...)
	cmd := exec.CommandContext(ctx, cfg.Executor, args...)
	cmd.Env = append(os.Environ(), req.Env...)
	if setProcessGroup {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	}
	return cmd, nil
}

func vfioGroupFlag(groups []int) string {
	return strings.Join(vfioGroupStrings(groups), ",")
}

func (s *ClusterServerImpl) normalizeScheduledRequest(req *ExecuteRequest) error {
	if err := s.normalizeScheduledCWD(req); err != nil {
		return err
	}
	token, err := generateChildToken()
	if err != nil {
		return err
	}
	s.applyScheduledEnv(req, s.nodeID, token)
	return nil
}

func (s *ClusterServerImpl) normalizeScheduledCWD(req *ExecuteRequest) error {
	if !s.ssiConfig.Required {
		return nil
	}
	cfg := s.ssiConfig.WithDefaults()
	_, logicalDir, err := ssi.ResolveClusterCWD(cfg.ClusterRoot, cfg.RootFS, req.Cwd)
	if err != nil {
		return err
	}
	req.Cwd = logicalDir
	return nil
}

func (s *ClusterServerImpl) applyScheduledEnv(req *ExecuteRequest, nodeID cluster.NodeID, childToken string) {
	if !s.ssiConfig.Required {
		return
	}
	cfg := s.ssiConfig.WithDefaults()
	brokerPort := s.grpcPort
	if brokerPort <= 0 {
		brokerPort = 50051
	}
	values := []string{
		"OCTOPOS_SSI=1",
		"OCTOPOS_CLUSTER_ROOT=/",
		"OCTOPOS_CLUSTER_HOSTNAME=" + ssi.DefaultHostname,
		"OCTOPOS_HOST_CLUSTER_ROOT=" + cfg.ClusterRoot,
		remotechild.EnvBrokerAddr + "=" + fmt.Sprintf("127.0.0.1:%d", brokerPort),
		"OCTOPOS_NODE_ID=" + string(nodeID),
		"OCTOPOS_SESSION_ID=" + req.SessionId,
		"OCTOPOS_JOB_ID=" + req.JobId,
		remotechild.EnvChildToken + "=" + childToken,
	}
	if requestEnvValue(req.Env, remotechild.EnvPipeCoordinator) == "" {
		values = append(values, remotechild.EnvPipeCoordinator+"="+string(s.nodeID))
	}
	req.Env = upsertEnv(req.Env, values...)
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

func upsertEnv(env []string, values ...string) []string {
	overrides := make(map[string]string, len(values))
	for _, entry := range values {
		key, _, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		overrides[key] = entry
	}

	out := make([]string, 0, len(env)+len(values))
	for _, entry := range env {
		key, _, ok := strings.Cut(entry, "=")
		if ok && overrides[key] != "" {
			continue
		}
		out = append(out, entry)
	}
	for _, entry := range values {
		key, _, ok := strings.Cut(entry, "=")
		if !ok || overrides[key] == "" {
			continue
		}
		out = append(out, entry)
	}
	return out
}

func generateChildToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate child token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func remoteChildInfoFromEnv(req *ExecuteRequest, jobID cluster.JobID, nodeID cluster.NodeID) *cluster.RemoteChildInfo {
	if !isRemoteChildRequest(req) {
		return nil
	}
	info := &cluster.RemoteChildInfo{
		ParentJobID:        cluster.JobID(requestEnvValue(req.Env, remotechild.EnvParentJobID)),
		RemoteJobID:        jobID,
		RemoteNodeID:       nodeID,
		Command:            append([]string{}, req.Command...),
		PlacementReason:    requestEnvValue(req.Env, remotechild.EnvPlacementReason),
		FallbackReason:     requestEnvValue(req.Env, remotechild.EnvFallbackReason),
		FallbackReasonCode: requestEnvValue(req.Env, remotechild.EnvFallbackCode),
		State:              remotechild.StateScheduled,
	}
	if info.PlacementReason == "" {
		info.PlacementReason = "explicit"
	}
	if pid, err := strconv.Atoi(requestEnvValue(req.Env, remotechild.EnvParentPID)); err == nil {
		info.ParentPID = pid
	}
	if pid, err := strconv.Atoi(requestEnvValue(req.Env, remotechild.EnvShadowPID)); err == nil {
		info.ShadowPID = pid
	}
	return info
}

func remoteChildRecordFromInfo(req *ExecuteRequest, info *cluster.RemoteChildInfo) remotechild.ShadowRecord {
	if info == nil {
		return remotechild.ShadowRecord{}
	}
	return remotechild.ShadowRecord{
		SessionID:          req.SessionId,
		ParentJobID:        string(info.ParentJobID),
		ParentPID:          info.ParentPID,
		ShadowPID:          info.ShadowPID,
		RemoteJobID:        string(info.RemoteJobID),
		RemoteNodeID:       string(info.RemoteNodeID),
		RemoteGlobalPID:    info.RemoteGlobalPID,
		RemoteLocalPID:     info.RemoteLocalPID,
		Command:            append([]string{}, info.Command...),
		State:              remotechild.ShadowState(info.State),
		StartedAt:          info.StartedAt,
		FinishedAt:         info.FinishedAt,
		PlacementReason:    info.PlacementReason,
		FallbackReason:     info.FallbackReason,
		FallbackReasonCode: info.FallbackReasonCode,
		FailureReason:      info.FailureReason,
	}
}

func (s *ClusterServerImpl) recordRemoteChildAudit(req *ExecuteRequest, event string, decision string, failureReason string, info *cluster.RemoteChildInfo) {
	if s == nil || s.remoteChildren == nil || req == nil {
		return
	}
	if info == nil {
		info = remoteChildInfoFromEnv(req, cluster.JobID(req.JobId), "")
	}
	audit := remotechild.AuditEvent{
		Event:             event,
		Decision:          decision,
		SessionID:         req.SessionId,
		RemoteJobID:       req.JobId,
		Command:           append([]string{}, req.Command...),
		AuthFailureReason: failureReason,
	}
	if info != nil {
		audit.ParentJobID = string(info.ParentJobID)
		audit.ParentPID = info.ParentPID
		audit.ShadowPID = info.ShadowPID
		if audit.RemoteJobID == "" {
			audit.RemoteJobID = string(info.RemoteJobID)
		}
		audit.RemoteNodeID = string(info.RemoteNodeID)
		audit.PlacementReason = info.PlacementReason
		audit.FallbackReason = info.FallbackReason
		audit.FallbackReasonCode = info.FallbackReasonCode
	}
	s.remoteChildren.RecordAudit(audit)
}

func (s *ClusterServerImpl) stopRemoteChildrenForParent(parentJobID cluster.JobID, reason string) {
	if reason == "" {
		reason = "parent job exited"
	}
	records := s.remoteChildren.List(remotechild.ListFilter{
		ParentJobID: string(parentJobID),
		ActiveOnly:  true,
	})
	for _, record := range records {
		s.remoteChildren.MarkState(record.RemoteJobID, remotechild.StateStopping, reason, time.Now())
		if record.RemoteGlobalPID == 0 {
			s.remoteChildren.MarkFinished(record.RemoteJobID, remotechild.StateFailed, -1, int(syscall.SIGTERM), reason, time.Now())
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = s.Signal(ctx, &SignalRequest{
			GlobalPid: record.RemoteGlobalPID,
			Signal:    int32(syscall.SIGTERM),
		})
		cancel()
		s.remoteChildren.MarkFinished(record.RemoteJobID, remotechild.StateFailed, -1, int(syscall.SIGTERM), reason, time.Now())
	}
}

func requestEnvValue(env []string, key string) string {
	prefix := key + "="
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			return strings.TrimPrefix(entry, prefix)
		}
	}
	return ""
}

func cloneExecuteRequestForNode(req *ExecuteRequest, nodeID cluster.NodeID) *ExecuteRequest {
	forwarded := proto.Clone(req).(*ExecuteRequest)
	if forwarded.Resources == nil {
		forwarded.Resources = &Requirements{}
	}
	affinity := make(map[string]string, len(forwarded.Resources.NodeAffinity)+1)
	for key, value := range forwarded.Resources.NodeAffinity {
		affinity[key] = value
	}
	affinity["node_id"] = string(nodeID)
	forwarded.Resources.NodeAffinity = affinity
	return forwarded
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

func sendStreamError(stream execStreamServer, msg string) error {
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
