package rpc

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/octopos/octopos/pkg/cluster"
	"github.com/octopos/octopos/pkg/remotechild"
	"google.golang.org/grpc"
)

type remoteJobStatusClient interface {
	GetJobStatus(ctx context.Context, in *GetJobStatusRequest, opts ...grpc.CallOption) (*GetJobStatusResponse, error)
}

type RemoteChildRecoveryStats struct {
	Recovering   int
	Running      int
	Stopped      int
	Completed    int
	Failed       int
	Orphaned     int
	Unobservable int
}

func (s RemoteChildRecoveryStats) HasPending() bool {
	return s.Recovering > s.Running+s.Stopped+s.Completed+s.Failed+s.Orphaned
}

func (s RemoteChildRecoveryStats) HasChanges() bool {
	return s.Running+s.Stopped+s.Completed+s.Failed+s.Orphaned > 0
}

type remoteChildRecoveryOutcome int

const (
	remoteChildRecoveryUnobservable remoteChildRecoveryOutcome = iota
	remoteChildRecoveryRunning
	remoteChildRecoveryStopped
	remoteChildRecoveryCompleted
	remoteChildRecoveryFailed
	remoteChildRecoveryOrphaned
)

// RecoverRemoteChildren reconciles persistent records left in recovering state
// after an octoposd restart. It queries the recorded worker node directly and
// updates the parent-side lifecycle store with the worker's current job state.
func (s *ClusterServerImpl) RecoverRemoteChildren(ctx context.Context) RemoteChildRecoveryStats {
	var stats RemoteChildRecoveryStats
	if s == nil || s.remoteChildren == nil {
		return stats
	}

	now := time.Now()
	for _, record := range s.remoteChildren.List(remotechild.ListFilter{ActiveOnly: true}) {
		if record.State != remotechild.StateRecovering {
			continue
		}
		stats.Recovering++

		if cluster.NodeID(record.RemoteNodeID) == s.nodeID {
			outcome := s.recoverLocalRemoteChildRecord(record, now)
			switch outcome {
			case remoteChildRecoveryRunning:
				stats.Running++
				go s.followRecoveredLocalProcess(context.Background(), record)
			case remoteChildRecoveryOrphaned:
				stats.Orphaned++
			default:
				stats.Unobservable++
			}
			continue
		}

		client := s.recoveryClientFor(record)
		outcome := s.recoverRemoteChildRecord(ctx, record, client, now)
		switch outcome {
		case remoteChildRecoveryRunning:
			stats.Running++
			go s.followRemoteJob(context.Background(), client, cluster.JobID(record.RemoteJobID), cluster.NodeID(record.RemoteNodeID), cluster.Requirements{})
		case remoteChildRecoveryStopped:
			stats.Stopped++
			go s.followRemoteJob(context.Background(), client, cluster.JobID(record.RemoteJobID), cluster.NodeID(record.RemoteNodeID), cluster.Requirements{})
		case remoteChildRecoveryCompleted:
			stats.Completed++
		case remoteChildRecoveryFailed:
			stats.Failed++
		case remoteChildRecoveryOrphaned:
			stats.Orphaned++
		default:
			stats.Unobservable++
		}
	}
	return stats
}

func (s *ClusterServerImpl) recoverLocalRemoteChildRecord(record remotechild.ShadowRecord, now time.Time) remoteChildRecoveryOutcome {
	if record.RemoteLocalPID <= 0 {
		if outcome, ok := s.recoverLocalRemoteChildExitStatus(record, now); ok {
			return outcome
		}
		return s.expireOrDeferRecoveringChild(record, now)
	}
	if !processAlive(record.RemoteLocalPID) {
		if outcome, ok := s.recoverLocalRemoteChildExitStatus(record, now); ok {
			return outcome
		}
		reason := fmt.Sprintf("local worker pid %d is not running after daemon restart", record.RemoteLocalPID)
		record = terminalRecoveredRecord(record, remotechild.StateOrphaned, -1, 0, reason, now)
		s.remoteChildren.MarkFinished(record.RemoteJobID, record.State, record.ExitCode, record.Signal, record.FailureReason, record.FinishedAt)
		s.upsertRecoveredJob(record, cluster.JobStatusFailed, recoveredJobInfoFromRecord(record, JobStatus_JOB_STATUS_FAILED, now))
		return remoteChildRecoveryOrphaned
	}
	if record.RemoteGlobalPID == 0 {
		record.RemoteGlobalPID = atomic.AddUint64(&s.localPIDCounter, 1)
	} else {
		s.ensureLocalPIDCounterAtLeast(record.RemoteGlobalPID)
	}
	if record.StartedAt.IsZero() {
		record.StartedAt = now
	}
	record.State = remotechild.StateRunning
	record.FailureReason = ""
	s.remoteChildren.Upsert(record)
	s.remoteChildren.MarkRunningWithLocalPID(record.RemoteJobID, record.RemoteGlobalPID, record.RemoteLocalPID, now)
	if s.tracker != nil {
		s.tracker.Register(processInfoFromRecoveredRecord(s.nodeID, record))
	}
	s.upsertRecoveredJob(record, cluster.JobStatusRunning, recoveredJobInfoFromRecord(record, JobStatus_JOB_STATUS_RUNNING, now))
	return remoteChildRecoveryRunning
}

func (s *ClusterServerImpl) recoveryClientFor(record remotechild.ShadowRecord) remoteJobStatusClient {
	if record.RemoteNodeID == "" {
		return nil
	}
	if s.clientPool == nil {
		return nil
	}
	client, ok := s.clientPool.GetPeer(cluster.NodeID(record.RemoteNodeID))
	if !ok {
		return nil
	}
	return client
}

func (s *ClusterServerImpl) recoverRemoteChildRecord(ctx context.Context, record remotechild.ShadowRecord, client remoteJobStatusClient, now time.Time) remoteChildRecoveryOutcome {
	if client == nil {
		return s.expireOrDeferRecoveringChild(record, now)
	}
	resp, err := client.GetJobStatus(internalForwardContext(ctx), &GetJobStatusRequest{JobId: record.RemoteJobID})
	if err != nil || resp == nil || !resp.Found || resp.Job == nil {
		return s.expireOrDeferRecoveringChild(record, now)
	}
	return s.applyRecoveredRemoteChildJob(record, resp.Job, now)
}

func (s *ClusterServerImpl) applyRecoveredRemoteChildJob(record remotechild.ShadowRecord, job *JobInfo, now time.Time) remoteChildRecoveryOutcome {
	if job == nil {
		return s.expireOrDeferRecoveringChild(record, now)
	}
	record = mergeRecoveredRemoteChildRecord(record, job)
	s.remoteChildren.Upsert(record)

	switch job.Status {
	case JobStatus_JOB_STATUS_RUNNING:
		record.State = remotechild.StateRunning
		record.FailureReason = ""
		s.remoteChildren.MarkRunning(record.RemoteJobID, record.RemoteGlobalPID, nonZeroTime(record.StartedAt, now))
		s.upsertRecoveredJob(record, cluster.JobStatusRunning, job)
		return remoteChildRecoveryRunning
	case JobStatus_JOB_STATUS_STOPPED:
		reason := record.FailureReason
		if reason == "" {
			reason = "remote job is stopped"
		}
		record.State = remotechild.StateStopped
		record.FailureReason = reason
		s.remoteChildren.MarkState(record.RemoteJobID, remotechild.StateStopped, reason, now)
		s.upsertRecoveredJob(record, cluster.JobStatusStopped, job)
		return remoteChildRecoveryStopped
	case JobStatus_JOB_STATUS_COMPLETED:
		exitCode := recoveredExitCode(job)
		record = terminalRecoveredRecord(record, remotechild.StateCompleted, exitCode, 0, "", nonZeroTime(record.FinishedAt, now))
		s.remoteChildren.MarkFinished(record.RemoteJobID, record.State, record.ExitCode, record.Signal, record.FailureReason, record.FinishedAt)
		s.upsertRecoveredJob(record, cluster.JobStatusCompleted, job)
		return remoteChildRecoveryCompleted
	case JobStatus_JOB_STATUS_FAILED:
		exitCode := recoveredExitCode(job)
		if exitCode == 0 {
			exitCode = -1
		}
		reason := record.FailureReason
		if reason == "" {
			reason = "remote job failed"
		}
		record = terminalRecoveredRecord(record, remotechild.StateFailed, exitCode, int(recoveredSignal(record)), reason, nonZeroTime(record.FinishedAt, now))
		s.remoteChildren.MarkFinished(record.RemoteJobID, record.State, record.ExitCode, record.Signal, record.FailureReason, record.FinishedAt)
		s.upsertRecoveredJob(record, cluster.JobStatusFailed, job)
		return remoteChildRecoveryFailed
	default:
		s.remoteChildren.MarkState(record.RemoteJobID, remotechild.StateScheduled, "remote job has not started yet", now)
		s.upsertRecoveredJob(record, cluster.JobStatusPending, job)
		return remoteChildRecoveryRunning
	}
}

func (s *ClusterServerImpl) expireOrDeferRecoveringChild(record remotechild.ShadowRecord, now time.Time) remoteChildRecoveryOutcome {
	if s.remoteChildLeaseTimeout <= 0 {
		return remoteChildRecoveryUnobservable
	}
	if now.Sub(recoveryReferenceTime(record, now)) < s.remoteChildLeaseTimeout {
		return remoteChildRecoveryUnobservable
	}
	reason := fmt.Sprintf("remote child recovery expired after %s without status from node %s", s.remoteChildLeaseTimeout, record.RemoteNodeID)
	record = terminalRecoveredRecord(record, remotechild.StateOrphaned, -1, 0, reason, now)
	s.remoteChildren.MarkFinished(record.RemoteJobID, record.State, record.ExitCode, record.Signal, record.FailureReason, record.FinishedAt)
	s.upsertRecoveredJob(record, cluster.JobStatusFailed, &JobInfo{JobId: record.RemoteJobID, Status: JobStatus_JOB_STATUS_FAILED})
	return remoteChildRecoveryOrphaned
}

func mergeRecoveredRemoteChildRecord(record remotechild.ShadowRecord, job *JobInfo) remotechild.ShadowRecord {
	if record.RemoteJobID == "" {
		record.RemoteJobID = job.JobId
	}
	if record.SessionID == "" {
		record.SessionID = job.SessionId
	}
	if record.StartedAt.IsZero() {
		record.StartedAt = unixTimeFromProto(job.StartedAt)
	}
	if record.FinishedAt.IsZero() {
		record.FinishedAt = unixTimeFromProto(job.FinishedAt)
	}
	if len(record.Command) == 0 && len(job.Commands) > 0 {
		record.Command = append([]string{}, job.Commands[0].Argv...)
	}
	if job.RemoteChild != nil {
		child := job.RemoteChild
		if record.ParentJobID == "" {
			record.ParentJobID = child.ParentJobId
		}
		if record.ParentPID == 0 {
			record.ParentPID = int(child.ParentPid)
		}
		if record.ShadowPID == 0 {
			record.ShadowPID = int(child.ShadowPid)
		}
		if record.RemoteNodeID == "" {
			record.RemoteNodeID = child.RemoteNodeId
		}
		if child.RemoteGlobalPid != 0 {
			record.RemoteGlobalPID = child.RemoteGlobalPid
		}
		if len(record.Command) == 0 {
			record.Command = append([]string{}, child.Command...)
		}
		if record.PlacementReason == "" {
			record.PlacementReason = child.PlacementReason
		}
		if record.FallbackReason == "" {
			record.FallbackReason = child.FallbackReason
		}
		if record.FallbackReasonCode == "" {
			record.FallbackReasonCode = child.FallbackReasonCode
		}
		if child.FailureReason != "" {
			record.FailureReason = child.FailureReason
		}
		if record.StartedAt.IsZero() {
			record.StartedAt = unixTimeFromProto(child.StartedAt)
		}
		if record.FinishedAt.IsZero() {
			record.FinishedAt = unixTimeFromProto(child.FinishedAt)
		}
	}
	return record
}

func (s *ClusterServerImpl) upsertRecoveredJob(record remotechild.ShadowRecord, status cluster.JobStatus, remoteJob *JobInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()

	jobID := cluster.JobID(record.RemoteJobID)
	job, exists := s.cluster.jobs[jobID]
	if !exists {
		job = &cluster.JobInfo{
			ID:          jobID,
			SessionID:   cluster.SessionID(record.SessionID),
			CreatedAt:   nonZeroTime(record.CreatedAt, time.Now()),
			PrimaryNode: cluster.NodeID(record.RemoteNodeID),
			RemoteChild: remoteChildInfoFromRecord(record),
		}
		if len(record.Command) > 0 {
			job.Commands = []cluster.CommandSpec{{Argv: append([]string{}, record.Command...)}}
		}
		s.cluster.jobs[jobID] = job
	}
	job.Status = status
	job.StartedAt = nonZeroTime(record.StartedAt, job.StartedAt)
	if status == cluster.JobStatusCompleted || status == cluster.JobStatusFailed {
		exitCode := recoveredExitCode(remoteJob)
		if exitCode == 0 && record.ExitCode != 0 {
			exitCode = record.ExitCode
		}
		if status == cluster.JobStatusFailed && exitCode == 0 {
			exitCode = -1
		}
		job.FinishedAt = nonZeroTime(record.FinishedAt, time.Now())
		job.ExitCodes = []int{exitCode}
	}
	if job.RemoteChild == nil {
		job.RemoteChild = remoteChildInfoFromRecord(record)
	} else {
		job.RemoteChild = remoteChildInfoFromRecord(record)
	}
}

func (s *ClusterServerImpl) followRecoveredLocalProcess(ctx context.Context, record remotechild.ShadowRecord) {
	if record.RemoteLocalPID <= 0 {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if processAlive(record.RemoteLocalPID) {
				continue
			}
			now := time.Now()
			if _, ok := s.recoverLocalRemoteChildExitStatus(record, now); ok {
				if s.tracker != nil {
					s.tracker.Unregister(cluster.GlobalPID(record.RemoteGlobalPID))
				}
				return
			}
			reason := fmt.Sprintf("recovered local worker pid %d exited; exit status unavailable after daemon restart", record.RemoteLocalPID)
			record = terminalRecoveredRecord(record, remotechild.StateOrphaned, -1, 0, reason, now)
			if s.tracker != nil {
				s.tracker.Unregister(cluster.GlobalPID(record.RemoteGlobalPID))
			}
			s.remoteChildren.MarkFinished(record.RemoteJobID, record.State, record.ExitCode, record.Signal, record.FailureReason, record.FinishedAt)
			s.upsertRecoveredJob(record, cluster.JobStatusFailed, recoveredJobInfoFromRecord(record, JobStatus_JOB_STATUS_FAILED, now))
			return
		}
	}
}

func (s *ClusterServerImpl) recoverLocalRemoteChildExitStatus(record remotechild.ShadowRecord, now time.Time) (remoteChildRecoveryOutcome, bool) {
	status, ok := s.readLocalRemoteChildExitStatus(record)
	if !ok {
		return remoteChildRecoveryUnobservable, false
	}
	finishedAt := nonZeroTime(status.ExitedAt, now)
	state := remotechild.ShadowState(remotechild.StateCompleted)
	jobStatus := cluster.JobStatusCompleted
	outcome := remoteChildRecoveryCompleted
	reason := ""
	if status.Error != "" {
		state = remotechild.StateFailed
		jobStatus = cluster.JobStatusFailed
		outcome = remoteChildRecoveryFailed
		reason = status.Error
	}
	exitCode := status.ExitCode
	if status.Signal > 0 && exitCode < 0 {
		exitCode = 128 + status.Signal
	}
	record = terminalRecoveredRecord(record, state, exitCode, status.Signal, reason, finishedAt)
	s.remoteChildren.MarkFinished(record.RemoteJobID, record.State, record.ExitCode, record.Signal, record.FailureReason, record.FinishedAt)
	s.upsertRecoveredJob(record, jobStatus, recoveredJobInfoFromRecord(record, jobStatusToProto(jobStatus), now))
	return outcome, true
}

func (s *ClusterServerImpl) readLocalRemoteChildExitStatus(record remotechild.ShadowRecord) (remotechild.WorkerExitStatus, bool) {
	if record.RemoteJobID == "" || !s.ssiConfig.Required {
		return remotechild.WorkerExitStatus{}, false
	}
	statusPath := s.remoteChildExitStatusPath(record.RemoteJobID)
	status, err := remotechild.ReadWorkerExitStatus(statusPath)
	if err != nil {
		return remotechild.WorkerExitStatus{}, false
	}
	if status.JobID != "" && status.JobID != record.RemoteJobID {
		return remotechild.WorkerExitStatus{}, false
	}
	return status, true
}

func terminalRecoveredRecord(record remotechild.ShadowRecord, state remotechild.ShadowState, exitCode int, signal int, reason string, at time.Time) remotechild.ShadowRecord {
	record.State = state
	record.ExitCode = exitCode
	record.Signal = signal
	record.FailureReason = reason
	record.FinishedAt = at
	record.UpdatedAt = at
	return record
}

func (s *ClusterServerImpl) ensureLocalPIDCounterAtLeast(value uint64) {
	for {
		current := atomic.LoadUint64(&s.localPIDCounter)
		if current >= value {
			return
		}
		if atomic.CompareAndSwapUint64(&s.localPIDCounter, current, value) {
			return
		}
	}
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	if err := syscall.Kill(pid, 0); err == nil {
		return true
	}
	if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); err == nil {
		return true
	}
	return false
}

func processInfoFromRecoveredRecord(nodeID cluster.NodeID, record remotechild.ShadowRecord) *cluster.ProcessInfo {
	return &cluster.ProcessInfo{
		GlobalPID:   cluster.GlobalPID(record.RemoteGlobalPID),
		NodeID:      nodeID,
		LocalPID:    record.RemoteLocalPID,
		PPID:        record.ParentPID,
		SessionID:   cluster.SessionID(record.SessionID),
		JobID:       cluster.JobID(record.RemoteJobID),
		Comm:        firstArg(record.Command),
		Cmdline:     strings.Join(record.Command, " "),
		StartTime:   record.StartedAt,
		State:       string(remotechild.StateRunning),
		ProcessKind: "remote-child",
		RemoteChild: remoteChildInfoFromRecord(record),
	}
}

func recoveredJobInfoFromRecord(record remotechild.ShadowRecord, status JobStatus, now time.Time) *JobInfo {
	job := &JobInfo{
		JobId:       record.RemoteJobID,
		SessionId:   record.SessionID,
		Status:      status,
		CreatedAt:   unixTimeOrZero(nonZeroTime(record.CreatedAt, now)),
		StartedAt:   unixTimeOrZero(nonZeroTime(record.StartedAt, now)),
		PrimaryNode: record.RemoteNodeID,
		RemoteChild: remoteChildInfoToProto(remoteChildInfoFromRecord(record)),
	}
	if len(record.Command) > 0 {
		job.Commands = []*CommandSpec{{Argv: append([]string{}, record.Command...)}}
	}
	if status == JobStatus_JOB_STATUS_FAILED || status == JobStatus_JOB_STATUS_COMPLETED {
		job.FinishedAt = unixTimeOrZero(nonZeroTime(record.FinishedAt, now))
		job.ExitCodes = []int32{int32(record.ExitCode)}
	}
	return job
}

func recoveryReferenceTime(record remotechild.ShadowRecord, fallback time.Time) time.Time {
	switch {
	case !record.UpdatedAt.IsZero():
		return record.UpdatedAt
	case !record.CreatedAt.IsZero():
		return record.CreatedAt
	default:
		return fallback
	}
}

func recoveredExitCode(job *JobInfo) int {
	if job != nil && len(job.ExitCodes) > 0 {
		return int(job.ExitCodes[0])
	}
	return 0
}

func recoveredSignal(record remotechild.ShadowRecord) int {
	return record.Signal
}

func nonZeroTime(primary, fallback time.Time) time.Time {
	if !primary.IsZero() {
		return primary
	}
	return fallback
}

type localJobStatusClient struct {
	server *ClusterServerImpl
}

func (c localJobStatusClient) GetJobStatus(ctx context.Context, req *GetJobStatusRequest, _ ...grpc.CallOption) (*GetJobStatusResponse, error) {
	if c.server == nil {
		return &GetJobStatusResponse{Found: false}, nil
	}
	c.server.mu.RLock()
	defer c.server.mu.RUnlock()
	job, exists := c.server.cluster.jobs[cluster.JobID(req.JobId)]
	if !exists {
		return &GetJobStatusResponse{Found: false}, nil
	}
	return &GetJobStatusResponse{Found: true, Job: c.server.jobInfoToProto(job)}, nil
}
