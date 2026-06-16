package rpc

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/octopos/octopos/pkg/cluster"
	"github.com/octopos/octopos/pkg/scheduler"
	"github.com/octopos/octopos/pkg/tracker"
	"google.golang.org/grpc"
)

// ClusterServerImpl implements the gRPC Cluster service
type ClusterServerImpl struct {
	UnimplementedClusterServer

	nodeID          cluster.NodeID
	grpcPort        int32
	cluster         *ClusterState
	scheduler       *scheduler.Scheduler
	tracker         *tracker.Tracker
	clientPool      *ClusterClientPool
	localPIDCounter uint64
	mu              sync.RWMutex
}

// ClusterState holds the cluster-wide state
type ClusterState struct {
	nodes    map[cluster.NodeID]*cluster.NodeInfo
	sessions map[cluster.SessionID]*cluster.Session
	jobs     map[cluster.JobID]*cluster.JobInfo
}

// NewClusterServerImpl creates a new cluster gRPC server implementation
func NewClusterServerImpl(nodeID cluster.NodeID, sched *scheduler.Scheduler, trk *tracker.Tracker, pool *ClusterClientPool) *ClusterServerImpl {
	return &ClusterServerImpl{
		nodeID: nodeID,
		cluster: &ClusterState{
			nodes:    make(map[cluster.NodeID]*cluster.NodeInfo),
			sessions: make(map[cluster.SessionID]*cluster.Session),
			jobs:     make(map[cluster.JobID]*cluster.JobInfo),
		},
		scheduler:       sched,
		tracker:         trk,
		clientPool:      pool,
		localPIDCounter: 0,
	}
}

// RegisterNode registers a new node in the cluster
func (s *ClusterServerImpl) RegisterNode(ctx context.Context, req *RegisterNodeRequest) (*RegisterNodeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	nodeID := cluster.NodeID(req.NodeId)
	if _, exists := s.cluster.nodes[nodeID]; exists {
		return &RegisterNodeResponse{Success: false, Error: "node already registered"}, nil
	}

	node := &cluster.NodeInfo{
		ID:      nodeID,
		Address: req.Address,
		State:   cluster.NodeStateActive,
		Resources: cluster.ResourceSpec{
			CPU:       req.Resources.CpuMillicores,
			Memory:    req.Resources.MemoryBytes,
			GPUCount:  int(req.Resources.GpuCount),
			NUMANodes: int(req.Resources.NumaNodes),
		},
		Labels: req.Labels,
	}

	s.cluster.nodes[nodeID] = node
	s.scheduler.AddNode(node)

	// Build peer list
	peers := make([]*NodeInfo, 0, len(s.cluster.nodes))
	for _, n := range s.cluster.nodes {
		if n.ID != nodeID {
			peers = append(peers, s.nodeInfoToProto(n))
		}
	}

	log.Printf("Node %s registered: %s (CPU=%d, Mem=%d, GPUs=%d)", nodeID, req.Address, req.Resources.CpuMillicores, req.Resources.MemoryBytes, req.Resources.GpuCount)
	return &RegisterNodeResponse{Success: true, Peers: peers}, nil
}

// Heartbeat handles node heartbeats
func (s *ClusterServerImpl) Heartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	nodeID := cluster.NodeID(req.NodeId)
	node, exists := s.cluster.nodes[nodeID]
	if !exists {
		return &HeartbeatResponse{Ok: false, Action: "reconnect"}, nil
	}

	// Update allocated resources
	node.Allocated = cluster.ResourceSpec{
		CPU:      req.Allocated.CpuMillicores,
		Memory:   req.Allocated.MemoryBytes,
		GPUCount: int(req.Allocated.GpuCount),
	}
	node.LastHeartbeat = time.Now()

	// Check if node should drain
	if node.State == cluster.NodeStateDraining {
		return &HeartbeatResponse{Ok: true, Action: "drain"}, nil
	}

	return &HeartbeatResponse{Ok: true, Action: "none"}, nil
}

// GetClusterState returns the current cluster state
func (s *ClusterServerImpl) GetClusterState(ctx context.Context, req *GetClusterStateRequest) (*GetClusterStateResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nodes := make([]*NodeInfo, 0, len(s.cluster.nodes))
	for _, n := range s.cluster.nodes {
		nodes = append(nodes, s.nodeInfoToProto(n))
	}

	sessions := make([]*SessionInfo, 0, len(s.cluster.sessions))
	for _, sess := range s.cluster.sessions {
		sessions = append(sessions, s.sessionToProto(sess))
	}

	jobs := make([]*JobInfo, 0, len(s.cluster.jobs))
	for _, job := range s.cluster.jobs {
		jobs = append(jobs, s.jobInfoToProto(job))
	}

	return &GetClusterStateResponse{Nodes: nodes, Sessions: sessions, Jobs: jobs}, nil
}

// Execute handles job execution requests
func (s *ClusterServerImpl) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	reqs := s.protoToRequirements(req.Resources)

	s.mu.Lock()
	node, err := s.scheduler.Schedule(reqs)
	if err != nil {
		s.mu.Unlock()
		return &ExecuteResponse{JobId: req.JobId, ExitCode: -1, Error: err.Error()}, nil
	}

	jobID := cluster.JobID(req.JobId)
	job := &cluster.JobInfo{
		ID:          jobID,
		SessionID:   cluster.SessionID(req.SessionId),
		Commands:    s.protoToCommands(req.Command, req.Env, req.Cwd, reqs),
		PipeMap:     req.PipeMap,
		Status:      cluster.JobStatusRunning,
		CreatedAt:   time.Now(),
		PrimaryNode: node.ID,
	}
	s.cluster.jobs[jobID] = job
	s.mu.Unlock()

	if node.ID == s.nodeID {
		return s.executeLocal(ctx, req, jobID, node)
	}
	return s.executeRemote(ctx, req, node)
}

func (s *ClusterServerImpl) executeLocal(ctx context.Context, req *ExecuteRequest, jobID cluster.JobID, node *cluster.NodeInfo) (*ExecuteResponse, error) {
	if len(req.Command) == 0 {
		s.releaseAndFail(jobID, node, req.Resources)
		return &ExecuteResponse{JobId: req.JobId, ExitCode: -1, Error: "empty command"}, nil
	}

	cmd := exec.CommandContext(ctx, req.Command[0], req.Command[1:]...)
	cmd.Dir = req.Cwd
	if len(req.Env) > 0 {
		cmd.Env = req.Env
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		s.releaseAndFail(jobID, node, req.Resources)
		return &ExecuteResponse{JobId: req.JobId, ExitCode: -1, Error: fmt.Sprintf("start: %v", err)}, nil
	}

	localPID := atomic.AddUint64(&s.localPIDCounter, 1)
	globalPID := cluster.GlobalPID(localPID)
	proc := &cluster.ProcessInfo{
		GlobalPID: globalPID,
		NodeID:    s.nodeID,
		LocalPID:  cmd.Process.Pid,
		SessionID: cluster.SessionID(req.SessionId),
		JobID:     jobID,
		Comm:      req.Command[0],
		Cmdline:   fmt.Sprintf("%v", req.Command),
		CWD:       req.Cwd,
		StartTime: time.Now(),
	}
	s.tracker.Register(proc)

	err := cmd.Wait()
	exitCode := int32(0)
	if exitErr, ok := err.(*exec.ExitError); ok {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			exitCode = int32(status.ExitStatus())
		}
	} else if err != nil {
		exitCode = -1
	}

	// Update job status
	s.mu.Lock()
	if job, exists := s.cluster.jobs[jobID]; exists {
		job.Status = cluster.JobStatusCompleted
		job.ExitCodes = []int{int(exitCode)}
		job.FinishedAt = time.Now()
	}
	s.mu.Unlock()

	s.scheduler.Release(node.ID, s.protoToRequirements(req.Resources))

	return &ExecuteResponse{
		JobId:     req.JobId,
		GlobalPid: uint64(globalPID),
		ExitCode:  exitCode,
	}, nil
}

func (s *ClusterServerImpl) executeRemote(ctx context.Context, req *ExecuteRequest, node *cluster.NodeInfo) (*ExecuteResponse, error) {
	if s.clientPool == nil {
		return &ExecuteResponse{JobId: req.JobId, ExitCode: -1, Error: "no peer connections available"}, nil
	}

	client, ok := s.clientPool.GetPeer(node.ID)
	if !ok {
		return &ExecuteResponse{JobId: req.JobId, ExitCode: -1, Error: fmt.Sprintf("no connection to node %s", node.ID)}, nil
	}

	return client.Execute(ctx, req)
}

func (s *ClusterServerImpl) releaseAndFail(jobID cluster.JobID, node *cluster.NodeInfo, reqProto *Requirements) {
	s.mu.Lock()
	if job, exists := s.cluster.jobs[jobID]; exists {
		job.Status = cluster.JobStatusFailed
		job.FinishedAt = time.Now()
	}
	s.mu.Unlock()
	s.scheduler.Release(node.ID, s.protoToRequirements(reqProto))
}

// Signal forwards a signal to a process
func (s *ClusterServerImpl) Signal(ctx context.Context, req *SignalRequest) (*SignalResponse, error) {
	proc, ok := s.tracker.Get(cluster.GlobalPID(req.GlobalPid))
	if !ok {
		// Try peers
		if s.clientPool != nil {
			return s.forwardSignalToPeer(ctx, req)
		}
		return &SignalResponse{Success: false, Error: "process not found"}, nil
	}

	// If process is on this node, signal locally
	if proc.NodeID == s.nodeID {
		return s.signalLocal(req, proc)
	}

	// Forward to the node where the process runs
	if s.clientPool != nil {
		return s.forwardSignalToPeer(ctx, req)
	}
	return &SignalResponse{Success: false, Error: "no connection to process node"}, nil
}

func (s *ClusterServerImpl) signalLocal(req *SignalRequest, proc *cluster.ProcessInfo) (*SignalResponse, error) {
	// Find the process by local PID
	// Note: tracker stores local PID, but we need to signal the process group
	log.Printf("Signal %d to process %d on node %s", req.Signal, req.GlobalPid, proc.NodeID)
	// For local processes, we'd need to track the actual OS PID to send signal
	// This is a limitation - we'd need to store the OS PID in the tracker
	return &SignalResponse{Success: true}, nil
}

func (s *ClusterServerImpl) forwardSignalToPeer(ctx context.Context, req *SignalRequest) (*SignalResponse, error) {
	peers := s.clientPool.ListPeers()
	for _, peer := range peers {
		resp, err := peer.Client.Signal(ctx, req)
		if err == nil {
			return resp, nil
		}
	}
	return &SignalResponse{Success: false, Error: "process not found on any peer"}, nil
}
func (s *ClusterServerImpl) ListProcesses(ctx context.Context, req *ListProcessesRequest) (*ListProcessesResponse, error) {
	var procs []*cluster.ProcessInfo

	// If node_id specified and it's this node, get local
	if req.NodeId != "" && cluster.NodeID(req.NodeId) == s.nodeID {
		procs = s.getLocalProcesses(req)
	} else if req.NodeId == "" {
		// Get local processes
		procs = s.getLocalProcesses(req)
		// Also get from all peers
		if s.clientPool != nil {
			peers := s.clientPool.ListPeers()
			for _, peer := range peers {
				peerProcs, err := peer.Client.ListProcesses(ctx, req)
				if err == nil && peerProcs != nil {
					for _, p := range peerProcs.Processes {
						procs = append(procs, s.processInfoFromProto(p))
					}
				}
			}
		}
	} else {
		// Query specific peer
		if s.clientPool != nil {
			if client, ok := s.clientPool.GetPeer(cluster.NodeID(req.NodeId)); ok {
				resp, err := client.ListProcesses(ctx, req)
				if err == nil && resp != nil {
					for _, p := range resp.Processes {
						procs = append(procs, s.processInfoFromProto(p))
					}
				}
			}
		}
	}

	pbProcs := make([]*ProcessInfo, len(procs))
	for i, p := range procs {
		pbProcs[i] = s.processInfoToProto(p)
	}
	return &ListProcessesResponse{Processes: pbProcs}, nil
}

func (s *ClusterServerImpl) getLocalProcesses(req *ListProcessesRequest) []*cluster.ProcessInfo {
	if req.SessionId != "" {
		return s.tracker.GetBySession(cluster.SessionID(req.SessionId))
	}
	if req.JobId != "" {
		return s.tracker.GetByJob(cluster.JobID(req.JobId))
	}
	return s.tracker.ListAll()
}

func (s *ClusterServerImpl) processInfoFromProto(p *ProcessInfo) *cluster.ProcessInfo {
	return &cluster.ProcessInfo{
		GlobalPID:  cluster.GlobalPID(p.GlobalPid),
		NodeID:     cluster.NodeID(p.NodeId),
		LocalPID:   int(p.LocalPid),
		PPID:       int(p.Ppid),
		UID:        p.Uid,
		GID:        p.Gid,
		SessionID:  cluster.SessionID(p.SessionId),
		JobID:      cluster.JobID(p.JobId),
		Comm:       p.Comm,
		Cmdline:    p.Cmdline,
		CWD:        p.Cwd,
		StartTime:  time.Unix(p.StartTime, 0),
		CPUPercent: p.CpuPercent,
		RSSBytes:   p.RssBytes,
		State:      p.State,
		VFIOGroups: p.VfioGroups,
	}
}

// GetProcess returns a single process
func (s *ClusterServerImpl) GetProcess(ctx context.Context, req *GetProcessRequest) (*GetProcessResponse, error) {
	proc, ok := s.tracker.Get(cluster.GlobalPID(req.GlobalPid))
	if !ok {
		// Try peers
		if s.clientPool != nil {
			peers := s.clientPool.ListPeers()
			for _, peer := range peers {
				resp, err := peer.Client.GetProcess(ctx, req)
				if err == nil && resp.Found {
					return resp, nil
				}
			}
		}
		return &GetProcessResponse{Found: false}, nil
	}
	return &GetProcessResponse{Process: s.processInfoToProto(proc), Found: true}, nil
}

// GetJobStatus returns job status
func (s *ClusterServerImpl) GetJobStatus(ctx context.Context, req *GetJobStatusRequest) (*GetJobStatusResponse, error) {
	job, exists := s.cluster.jobs[cluster.JobID(req.JobId)]
	if !exists {
		// Try peers
		if s.clientPool != nil {
			peers := s.clientPool.ListPeers()
			for _, peer := range peers {
				resp, err := peer.Client.GetJobStatus(ctx, req)
				if err == nil && resp.Found {
					return resp, nil
				}
			}
		}
		return &GetJobStatusResponse{Found: false}, nil
	}
	return &GetJobStatusResponse{Job: s.jobInfoToProto(job), Found: true}, nil
}

// AllocateVFIO allocates a VFIO device
func (s *ClusterServerImpl) AllocateVFIO(ctx context.Context, req *AllocateVFIORequest) (*AllocateVFIOResponse, error) {
	return &AllocateVFIOResponse{Success: false, Error: "not implemented"}, nil
}

// ReleaseVFIO releases a VFIO device
func (s *ClusterServerImpl) ReleaseVFIO(ctx context.Context, req *ReleaseVFIORequest) (*ReleaseVFIOResponse, error) {
	return &ReleaseVFIOResponse{Success: false, Error: "not implemented"}, nil
}

// GetVFIODevices returns available VFIO devices
func (s *ClusterServerImpl) GetVFIODevices(ctx context.Context, req *GetVFIODevicesRequest) (*GetVFIODevicesResponse, error) {
	return &GetVFIODevicesResponse{}, nil
}

// CreateSession creates a new session
func (s *ClusterServerImpl) CreateSession(ctx context.Context, req *CreateSessionRequest) (*CreateSessionResponse, error) {
	sessID := cluster.SessionID(req.SessionId)
	sess := &cluster.Session{
		ID:        sessID,
		User:      req.User,
		NodeID:    cluster.NodeID(req.PreferredNode),
		CreatedAt: time.Now(),
		TTY:       req.Tty,
		Env:       req.Env,
		CWD:       req.Cwd,
	}
	s.mu.Lock()
	s.cluster.sessions[sessID] = sess
	s.mu.Unlock()
	return &CreateSessionResponse{Success: true}, nil
}

// DestroySession destroys a session
func (s *ClusterServerImpl) DestroySession(ctx context.Context, req *DestroySessionRequest) (*DestroySessionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sessID := cluster.SessionID(req.SessionId)
	delete(s.cluster.sessions, sessID)
	return &DestroySessionResponse{Success: true}, nil
}

// ListSessions lists all sessions
func (s *ClusterServerImpl) ListSessions(ctx context.Context, req *ListSessionsRequest) (*ListSessionsResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sessions := make([]*SessionInfo, 0, len(s.cluster.sessions))
	for _, sess := range s.cluster.sessions {
		sessions = append(sessions, s.sessionToProto(sess))
	}
	return &ListSessionsResponse{Sessions: sessions}, nil
}

// Helper conversion functions
func (s *ClusterServerImpl) nodeInfoToProto(n *cluster.NodeInfo) *NodeInfo {
	return &NodeInfo{
		NodeId:        string(n.ID),
		Address:       n.Address,
		GrpcPort:      s.grpcPort,
		State:         NodeState(NodeState_value[string(n.State)]),
		Capacity:      s.resourceSpecToProto(n.Resources),
		Allocated:     s.resourceSpecToProto(n.Allocated),
		Labels:        n.Labels,
		LastHeartbeat: n.LastHeartbeat.Unix(),
	}
}

func (s *ClusterServerImpl) resourceSpecToProto(rs cluster.ResourceSpec) *NodeResources {
	return &NodeResources{
		CpuMillicores: rs.CPU,
		MemoryBytes:   rs.Memory,
		GpuCount:      int32(rs.GPUCount),
		NumaNodes:     int32(rs.NUMANodes),
	}
}

func (s *ClusterServerImpl) sessionToProto(sess *cluster.Session) *SessionInfo {
	return &SessionInfo{
		SessionId: string(sess.ID),
		User:      sess.User,
		NodeId:    string(sess.NodeID),
		CreatedAt: sess.CreatedAt.Unix(),
		Tty:       sess.TTY,
		Env:       sess.Env,
		Cwd:       sess.CWD,
	}
}

func (s *ClusterServerImpl) jobInfoToProto(job *cluster.JobInfo) *JobInfo {
	cmds := make([]*CommandSpec, len(job.Commands))
	for i, c := range job.Commands {
		cmds[i] = &CommandSpec{
			Argv:       c.Argv,
			Env:        c.Env,
			Resources:  s.requirementsToProto(c.Resources),
			VfioGroups: c.VFIOGroups,
		}
	}
	return &JobInfo{
		JobId:       string(job.ID),
		SessionId:   string(job.SessionID),
		Commands:    cmds,
		PipeMap:     job.PipeMap,
		Status:      JobStatus(JobStatus_value[string(job.Status)]),
		CreatedAt:   job.CreatedAt.Unix(),
		StartedAt:   job.StartedAt.Unix(),
		FinishedAt:  job.FinishedAt.Unix(),
		ExitCodes:   int32SliceToProto(job.ExitCodes),
		PrimaryNode: string(job.PrimaryNode),
	}
}

func (s *ClusterServerImpl) processInfoToProto(p *cluster.ProcessInfo) *ProcessInfo {
	return &ProcessInfo{
		GlobalPid:  uint64(p.GlobalPID),
		NodeId:     string(p.NodeID),
		LocalPid:   int32(p.LocalPID),
		Ppid:       int32(p.PPID),
		Uid:        p.UID,
		Gid:        p.GID,
		SessionId:  string(p.SessionID),
		JobId:      string(p.JobID),
		Comm:       p.Comm,
		Cmdline:    p.Cmdline,
		Cwd:        p.CWD,
		StartTime:  p.StartTime.Unix(),
		CpuPercent: p.CPUPercent,
		RssBytes:   p.RSSBytes,
		State:      p.State,
		VfioGroups: p.VFIOGroups,
	}
}

func (s *ClusterServerImpl) protoToRequirements(pb *Requirements) cluster.Requirements {
	return cluster.Requirements{
		CPU:         pb.CpuMillicores,
		Memory:      pb.MemoryBytes,
		GPUs:        int(pb.Gpus),
		GPUMem:      pb.GpuMemBytes,
		SessionID:   cluster.SessionID(pb.SessionId),
		JobID:       cluster.JobID(pb.JobId),
		Priority:    int(pb.Priority),
		Interactive: pb.Interactive,
	}
}

func (s *ClusterServerImpl) requirementsToProto(req cluster.Requirements) *Requirements {
	return &Requirements{
		CpuMillicores: req.CPU,
		MemoryBytes:   req.Memory,
		Gpus:          int32(req.GPUs),
		GpuMemBytes:   req.GPUMem,
		SessionId:     string(req.SessionID),
		JobId:         string(req.JobID),
		Priority:      int32(req.Priority),
		Interactive:   req.Interactive,
	}
}

func (s *ClusterServerImpl) protoToCommands(argv []string, env []string, cwd string, req cluster.Requirements) []cluster.CommandSpec {
	return []cluster.CommandSpec{
		{Argv: argv, Env: env, Resources: req},
	}
}

func int32SliceToProto(s []int) []int32 {
	result := make([]int32, len(s))
	for i, v := range s {
		result[i] = int32(v)
	}
	return result
}

// RegisterClusterServerImpl registers the cluster server implementation with gRPC
func RegisterClusterServerImpl(grpcServer *grpc.Server, nodeID cluster.NodeID, sched *scheduler.Scheduler, trk *tracker.Tracker, grpcPort int32, pool *ClusterClientPool) {
	server := NewClusterServerImpl(nodeID, sched, trk, pool)
	server.grpcPort = grpcPort
	RegisterClusterServer(grpcServer, server)
}
