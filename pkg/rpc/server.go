package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/octopos/octopos/pkg/cluster"
	"github.com/octopos/octopos/pkg/remotechild"
	"github.com/octopos/octopos/pkg/scheduler"
	"github.com/octopos/octopos/pkg/ssi"
	"github.com/octopos/octopos/pkg/tracker"
	"google.golang.org/grpc"
)

// ClusterServerImpl implements the gRPC Cluster service
type ClusterServerImpl struct {
	UnimplementedClusterServer

	nodeID                      cluster.NodeID
	grpcPort                    int32
	cluster                     *ClusterState
	scheduler                   *scheduler.Scheduler
	tracker                     *tracker.Tracker
	clientPool                  *ClusterClientPool
	ssiConfig                   ssi.Config
	maxRemoteChildrenPerParent  int
	maxRemoteChildrenPerSession int
	maxRemoteChildrenPerNode    int
	remoteChildLeaseTimeout     time.Duration
	remoteChildTokenTTL         time.Duration
	remoteChildExitStatusDir    string
	remoteChildren              *remotechild.Store
	vfioAllocationStorePath     string
	pipes                       *pipeCoordinator
	unixSockets                 *unixSocketCoordinator
	localPIDCounter             uint64
	mu                          sync.RWMutex
}

// ClusterState holds the cluster-wide state
type ClusterState struct {
	nodes           map[cluster.NodeID]*cluster.NodeInfo
	sessions        map[cluster.SessionID]*cluster.Session
	jobs            map[cluster.JobID]*cluster.JobInfo
	vfioAllocations map[string]vfioAllocation
}

type vfioAllocation struct {
	NodeID    cluster.NodeID
	SessionID cluster.SessionID
	JobID     cluster.JobID
	GroupID   int
	CreatedAt time.Time
}

type ServerOptions struct {
	SSI                         ssi.Config
	MaxRemoteChildrenPerParent  int
	MaxRemoteChildrenPerSession int
	MaxRemoteChildrenPerNode    int
	RemoteChildStorePath        string
	RemoteChildLeaseTimeout     time.Duration
	RemoteChildTokenTTL         time.Duration
	RemoteChildExitStatusDir    string
	VFIOAllocationStorePath     string
}

// NewClusterServerImpl creates a new cluster gRPC server implementation
func NewClusterServerImpl(nodeID cluster.NodeID, sched *scheduler.Scheduler, trk *tracker.Tracker, pool *ClusterClientPool) *ClusterServerImpl {
	return NewClusterServerImplWithOptions(nodeID, sched, trk, pool, ServerOptions{})
}

func NewClusterServerImplWithOptions(nodeID cluster.NodeID, sched *scheduler.Scheduler, trk *tracker.Tracker, pool *ClusterClientPool, opts ServerOptions) *ClusterServerImpl {
	maxRemoteChildren := opts.MaxRemoteChildrenPerParent
	if maxRemoteChildren <= 0 {
		maxRemoteChildren = 128
	}
	maxSessionChildren := opts.MaxRemoteChildrenPerSession
	if maxSessionChildren <= 0 {
		maxSessionChildren = 256
	}
	maxNodeChildren := opts.MaxRemoteChildrenPerNode
	if maxNodeChildren <= 0 {
		maxNodeChildren = 128
	}
	remoteChildLeaseTimeout := opts.RemoteChildLeaseTimeout
	if remoteChildLeaseTimeout <= 0 {
		remoteChildLeaseTimeout = 2 * time.Minute
	}
	remoteChildTokenTTL := opts.RemoteChildTokenTTL
	if remoteChildTokenTTL <= 0 {
		remoteChildTokenTTL = 24 * time.Hour
	}
	remoteChildExitStatusDir := opts.RemoteChildExitStatusDir
	if remoteChildExitStatusDir == "" {
		remoteChildExitStatusDir = remotechild.WorkerExitStatusDir
	}
	remoteChildren := remotechild.NewStore()
	if opts.RemoteChildStorePath != "" {
		persistentStore, err := remotechild.NewPersistentStore(opts.RemoteChildStorePath)
		if err != nil {
			log.Printf("remote-child lifecycle persistence disabled: load %s: %v", opts.RemoteChildStorePath, err)
		} else {
			if recovering := persistentStore.RecoveringOnLoad(); recovering > 0 {
				log.Printf("Loaded %d remote-child lifecycle records for daemon restart recovery", recovering)
			}
			remoteChildren = persistentStore
		}
	}
	server := &ClusterServerImpl{
		nodeID: nodeID,
		cluster: &ClusterState{
			nodes:           make(map[cluster.NodeID]*cluster.NodeInfo),
			sessions:        make(map[cluster.SessionID]*cluster.Session),
			jobs:            make(map[cluster.JobID]*cluster.JobInfo),
			vfioAllocations: make(map[string]vfioAllocation),
		},
		scheduler:                   sched,
		tracker:                     trk,
		clientPool:                  pool,
		ssiConfig:                   opts.SSI.WithDefaults(),
		maxRemoteChildrenPerParent:  maxRemoteChildren,
		maxRemoteChildrenPerSession: maxSessionChildren,
		maxRemoteChildrenPerNode:    maxNodeChildren,
		remoteChildLeaseTimeout:     remoteChildLeaseTimeout,
		remoteChildTokenTTL:         remoteChildTokenTTL,
		remoteChildExitStatusDir:    remoteChildExitStatusDir,
		remoteChildren:              remoteChildren,
		vfioAllocationStorePath:     opts.VFIOAllocationStorePath,
		pipes:                       newPipeCoordinator(),
		unixSockets:                 newUnixSocketCoordinator(),
		localPIDCounter:             0,
	}
	if sched != nil {
		for _, node := range sched.ListNodes() {
			server.cluster.nodes[node.ID] = node
		}
	}
	server.recoverVFIOAllocations()
	return server
}

func (s *ClusterServerImpl) PruneRemoteChildren(retention time.Duration) int {
	if s == nil || s.remoteChildren == nil || retention <= 0 {
		return 0
	}
	return s.remoteChildren.PruneTerminal(time.Now().Add(-retention))
}

// RegisterNode registers a new node in the cluster
func (s *ClusterServerImpl) RegisterNode(ctx context.Context, req *RegisterNodeRequest) (*RegisterNodeResponse, error) {
	nodeID := cluster.NodeID(req.NodeId)
	resources := s.resourceSpecFromProto(req.Resources)
	node := &cluster.NodeInfo{
		ID:         nodeID,
		Address:    req.Address,
		State:      cluster.NodeStateActive,
		Resources:  resources,
		VFIOGroups: resources.VFIOGroups,
		Labels:     req.Labels,
	}

	s.mu.Lock()
	if existing, exists := s.cluster.nodes[nodeID]; exists {
		existing.Address = node.Address
		existing.State = node.State
		existing.Resources = node.Resources
		existing.VFIOGroups = node.VFIOGroups
		existing.Labels = node.Labels
		node = existing
	} else {
		s.cluster.nodes[nodeID] = node
	}
	s.scheduler.AddNode(node)
	s.reconcileVFIOAllocationsForNodeLocked(nodeID)

	// Build peer list
	peers := make([]*NodeInfo, 0, len(s.cluster.nodes))
	for _, n := range s.cluster.nodes {
		if n.ID != nodeID {
			peers = append(peers, s.nodeInfoToProto(n))
		}
	}
	s.mu.Unlock()

	if nodeID != s.nodeID && s.clientPool != nil {
		peerAddr := normalizeGRPCAddress(req.Address, req.GrpcPort)
		if err := s.clientPool.AddPeer(nodeID, peerAddr); err != nil {
			log.Printf("Failed to connect to registered node %s at %s: %v", nodeID, peerAddr, err)
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

// Execute submits a detached job execution request.
func (s *ClusterServerImpl) Execute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	return s.executeBackground(ctx, req)
}

// RemoteChildExecute submits a detached distributed-child execution request.
func (s *ClusterServerImpl) RemoteChildExecute(ctx context.Context, req *ExecuteRequest) (*ExecuteResponse, error) {
	if !isRemoteChildRequest(req) {
		return &ExecuteResponse{
			JobId:    req.GetJobId(),
			ExitCode: -1,
			Error:    "remote child request missing " + remotechild.EnvRemoteChild + "=1",
		}, nil
	}
	return s.executeBackground(ctx, req)
}

func (s *ClusterServerImpl) releaseAndFail(jobID cluster.JobID, node *cluster.NodeInfo, reqProto *Requirements) {
	s.mu.Lock()
	if job, exists := s.cluster.jobs[jobID]; exists {
		job.Status = cluster.JobStatusFailed
		job.FinishedAt = time.Now()
	}
	s.releaseVFIOAllocationsForJobLocked(jobID)
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
	log.Printf("Signal %d to process %d on node %s", req.Signal, req.GlobalPid, proc.NodeID)
	if proc.LocalPID <= 0 {
		return &SignalResponse{Success: false, Error: "process has no local PID"}, nil
	}
	s.markJobSignalState(proc.JobID, syscall.Signal(req.Signal))
	if err := syscall.Kill(-proc.LocalPID, syscall.Signal(req.Signal)); err != nil {
		return &SignalResponse{Success: false, Error: err.Error()}, nil
	}
	return &SignalResponse{Success: true}, nil
}

func (s *ClusterServerImpl) forwardSignalToPeer(ctx context.Context, req *SignalRequest) (*SignalResponse, error) {
	if isInternalForward(ctx) {
		return &SignalResponse{Success: false, Error: "process not found on this peer"}, nil
	}
	ctx = internalForwardContext(ctx)
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
		if s.clientPool != nil && !isInternalForward(ctx) {
			peerCtx := internalForwardContext(ctx)
			peers := s.clientPool.ListPeers()
			for _, peer := range peers {
				peerProcs, err := peer.Client.ListProcesses(peerCtx, req)
				if err == nil && peerProcs != nil {
					for _, p := range peerProcs.Processes {
						procs = append(procs, s.processInfoFromProto(p))
					}
				}
			}
		}
	} else {
		// Query specific peer
		if s.clientPool != nil && !isInternalForward(ctx) {
			if client, ok := s.clientPool.GetPeer(cluster.NodeID(req.NodeId)); ok {
				resp, err := client.ListProcesses(internalForwardContext(ctx), req)
				if err == nil && resp != nil {
					for _, p := range resp.Processes {
						procs = append(procs, s.processInfoFromProto(p))
					}
				}
			}
		}
	}

	procs = s.appendRemoteChildProcessRecords(procs, req)
	pbProcs := make([]*ProcessInfo, len(procs))
	for i, p := range procs {
		pbProcs[i] = s.processInfoToProto(p)
	}
	return &ListProcessesResponse{Processes: pbProcs}, nil
}

func (s *ClusterServerImpl) ListRemoteChildren(ctx context.Context, req *ListRemoteChildrenRequest) (*ListRemoteChildrenResponse, error) {
	filter := remotechild.ListFilter{
		SessionID:    req.SessionId,
		ParentJobID:  req.ParentJobId,
		RemoteJobID:  req.RemoteJobId,
		RemoteNodeID: req.RemoteNodeId,
		ActiveOnly:   req.ActiveOnly,
	}
	records := s.remoteChildren.List(filter)
	if s.clientPool != nil && !isInternalForward(ctx) {
		peerCtx := internalForwardContext(ctx)
		for _, peer := range s.clientPool.ListPeers() {
			resp, err := peer.Client.ListRemoteChildren(peerCtx, req)
			if err != nil || resp == nil {
				continue
			}
			for _, child := range resp.Children {
				records = append(records, remoteChildRecordFromProto(child))
			}
		}
	}
	records = dedupeRemoteChildRecords(records)
	out := make([]*RemoteChildRecord, 0, len(records))
	for _, record := range records {
		out = append(out, remoteChildRecordToProto(record))
	}
	return &ListRemoteChildrenResponse{Children: out}, nil
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
	out := &cluster.ProcessInfo{
		GlobalPID:   cluster.GlobalPID(p.GlobalPid),
		NodeID:      cluster.NodeID(p.NodeId),
		LocalPID:    int(p.LocalPid),
		PPID:        int(p.Ppid),
		UID:         p.Uid,
		GID:         p.Gid,
		SessionID:   cluster.SessionID(p.SessionId),
		JobID:       cluster.JobID(p.JobId),
		Comm:        p.Comm,
		Cmdline:     p.Cmdline,
		CWD:         p.Cwd,
		StartTime:   time.Unix(p.StartTime, 0),
		CPUPercent:  p.CpuPercent,
		RSSBytes:    p.RssBytes,
		State:       p.State,
		VFIOGroups:  p.VfioGroups,
		ProcessKind: p.ProcessKind,
	}
	if p.RemoteChild != nil {
		out.RemoteChild = remoteChildInfoFromProto(p.RemoteChild)
	}
	return out
}

// GetProcess returns a single process
func (s *ClusterServerImpl) GetProcess(ctx context.Context, req *GetProcessRequest) (*GetProcessResponse, error) {
	proc, ok := s.tracker.Get(cluster.GlobalPID(req.GlobalPid))
	if !ok {
		// Try peers
		if s.clientPool != nil && !isInternalForward(ctx) {
			peerCtx := internalForwardContext(ctx)
			peers := s.clientPool.ListPeers()
			for _, peer := range peers {
				resp, err := peer.Client.GetProcess(peerCtx, req)
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
		if s.clientPool != nil && !isInternalForward(ctx) {
			peerCtx := internalForwardContext(ctx)
			peers := s.clientPool.ListPeers()
			for _, peer := range peers {
				resp, err := peer.Client.GetJobStatus(peerCtx, req)
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
	if req.SessionId == "" {
		return &AllocateVFIOResponse{Success: false, Error: "session_id is required"}, nil
	}
	if req.JobId == "" {
		return &AllocateVFIOResponse{Success: false, Error: "job_id is required"}, nil
	}
	if req.Device == nil {
		return &AllocateVFIOResponse{Success: false, Error: "device requirement is required"}, nil
	}
	count := req.Device.Count
	if count <= 0 {
		count = 1
	}
	if s.scheduler == nil {
		return &AllocateVFIOResponse{Success: false, Error: "scheduler is not configured"}, nil
	}

	requirements := cluster.Requirements{
		VFIODevs:  []cluster.VFIORequirement{s.vfioRequirementFromProto(req.Device)},
		SessionID: cluster.SessionID(req.SessionId),
		JobID:     cluster.JobID(req.JobId),
	}
	requirements.VFIODevs[0].Count = int(count)

	s.mu.Lock()
	defer s.mu.Unlock()
	if job, exists := s.cluster.jobs[cluster.JobID(req.JobId)]; exists && job.SessionID != cluster.SessionID(req.SessionId) {
		return &AllocateVFIOResponse{Success: false, Error: "job belongs to a different session"}, nil
	}

	node, allocated, err := s.scheduler.ScheduleWithAllocation(requirements)
	if err != nil {
		return &AllocateVFIOResponse{Success: false, Error: err.Error()}, nil
	}
	if len(allocated.VFIOGroups) != int(count) {
		s.scheduler.Release(node.ID, allocated)
		return &AllocateVFIOResponse{Success: false, Error: "scheduler did not return the requested VFIO groups"}, nil
	}
	for _, groupID := range allocated.VFIOGroups {
		key := vfioAllocationKey(node.ID, groupID)
		if _, exists := s.cluster.vfioAllocations[key]; exists {
			s.scheduler.Release(node.ID, allocated)
			return &AllocateVFIOResponse{Success: false, Error: fmt.Sprintf("VFIO group %d was already allocated", groupID)}, nil
		}
	}
	now := time.Now()
	for _, groupID := range allocated.VFIOGroups {
		key := vfioAllocationKey(node.ID, groupID)
		s.cluster.vfioAllocations[key] = vfioAllocation{
			NodeID:    node.ID,
			SessionID: cluster.SessionID(req.SessionId),
			JobID:     cluster.JobID(req.JobId),
			GroupID:   groupID,
			CreatedAt: now,
		}
	}
	if err := s.saveVFIOAllocationsLocked(); err != nil {
		for _, groupID := range allocated.VFIOGroups {
			delete(s.cluster.vfioAllocations, vfioAllocationKey(node.ID, groupID))
		}
		s.scheduler.Release(node.ID, allocated)
		return &AllocateVFIOResponse{Success: false, Error: "persist VFIO allocation: " + err.Error()}, nil
	}
	groupIDs := make([]int32, 0, len(allocated.VFIOGroups))
	devicePaths := make([]string, 0, len(allocated.VFIOGroups))
	for _, groupID := range allocated.VFIOGroups {
		groupIDs = append(groupIDs, int32(groupID))
		devicePaths = append(devicePaths, vfioDevicePath(groupID))
	}
	return &AllocateVFIOResponse{
		Success:     true,
		GroupId:     groupIDs[0],
		ContainerFd: -1,
		DeviceFd:    -1,
		DevicePath:  devicePaths[0],
		GroupIds:    groupIDs,
		DevicePaths: devicePaths,
	}, nil
}

// ReleaseVFIO releases a VFIO device
func (s *ClusterServerImpl) ReleaseVFIO(ctx context.Context, req *ReleaseVFIORequest) (*ReleaseVFIOResponse, error) {
	if req.SessionId == "" {
		return &ReleaseVFIOResponse{Success: false, Error: "session_id is required"}, nil
	}
	groupIDs := releaseVFIOGroupIDs(req)
	if len(groupIDs) == 0 {
		return &ReleaseVFIOResponse{Success: false, Error: "group_id is required"}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	owned := make(map[string]vfioAllocation)
	for _, groupID := range groupIDs {
		alloc, key, foreign := s.vfioAllocationForReleaseLocked(cluster.SessionID(req.SessionId), groupID)
		if foreign {
			return &ReleaseVFIOResponse{Success: false, Error: fmt.Sprintf("VFIO group %d is owned by a different session", groupID)}, nil
		}
		if key == "" {
			if s.vfioGroupExistsLocked(groupID) {
				continue
			}
			return &ReleaseVFIOResponse{Success: false, Error: fmt.Sprintf("VFIO group %d not found", groupID)}, nil
		}
		owned[key] = alloc
	}
	for key := range owned {
		delete(s.cluster.vfioAllocations, key)
	}
	if err := s.saveVFIOAllocationsLocked(); err != nil {
		for key, alloc := range owned {
			s.cluster.vfioAllocations[key] = alloc
		}
		return &ReleaseVFIOResponse{Success: false, Error: "persist VFIO release: " + err.Error()}, nil
	}
	if s.scheduler != nil {
		for _, alloc := range owned {
			s.scheduler.Release(alloc.NodeID, cluster.Requirements{
				JobID:      alloc.JobID,
				VFIOGroups: []int{alloc.GroupID},
			})
		}
	}
	return &ReleaseVFIOResponse{Success: true}, nil
}

// GetVFIODevices returns available VFIO devices
func (s *ClusterServerImpl) GetVFIODevices(ctx context.Context, req *GetVFIODevicesRequest) (*GetVFIODevicesResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if req.NodeId != "" {
		node, ok := s.cluster.nodes[cluster.NodeID(req.NodeId)]
		if !ok {
			return &GetVFIODevicesResponse{}, nil
		}
		return &GetVFIODevicesResponse{Groups: s.vfioGroupsToProto(vfioGroupsWithClaims(node.VFIOGroups, node.Resources.VFIOGroups))}, nil
	}

	var groups []*VFIOGroup
	for _, node := range s.cluster.nodes {
		groups = append(groups, s.vfioGroupsToProto(vfioGroupsWithClaims(node.VFIOGroups, node.Resources.VFIOGroups))...)
	}
	return &GetVFIODevicesResponse{Groups: groups}, nil
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
	s.releaseVFIOAllocationsForSessionLocked(sessID)
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
	vfioGroups := n.VFIOGroups
	if len(vfioGroups) == 0 {
		vfioGroups = n.Resources.VFIOGroups
	}
	return &NodeInfo{
		NodeId:        string(n.ID),
		Address:       n.Address,
		GrpcPort:      s.grpcPort,
		State:         nodeStateToProto(n.State),
		Capacity:      s.resourceSpecToProto(n.Resources),
		Allocated:     s.resourceSpecToProto(n.Allocated),
		Labels:        n.Labels,
		LastHeartbeat: n.LastHeartbeat.Unix(),
		VfioGroups:    s.vfioGroupsToProto(vfioGroups),
	}
}

func (s *ClusterServerImpl) resourceSpecToProto(rs cluster.ResourceSpec) *NodeResources {
	return &NodeResources{
		CpuMillicores: rs.CPU,
		MemoryBytes:   rs.Memory,
		GpuCount:      int32(rs.GPUCount),
		NumaNodes:     int32(rs.NUMANodes),
		PciDevices:    s.pciDevicesToProto(rs.PCIDevices),
	}
}

func (s *ClusterServerImpl) resourceSpecFromProto(pb *NodeResources) cluster.ResourceSpec {
	if pb == nil {
		return cluster.ResourceSpec{}
	}
	pciDevices := s.pciDevicesFromProto(pb.PciDevices)
	return cluster.ResourceSpec{
		CPU:        pb.CpuMillicores,
		Memory:     pb.MemoryBytes,
		GPUCount:   int(pb.GpuCount),
		NUMANodes:  int(pb.NumaNodes),
		PCIDevices: pciDevices,
		VFIOGroups: vfioGroupsFromPCIDevices(pciDevices),
	}
}

func (s *ClusterServerImpl) pciDevicesToProto(devices []cluster.PCIDevice) []*PCIDevice {
	out := make([]*PCIDevice, 0, len(devices))
	for _, dev := range devices {
		out = append(out, &PCIDevice{
			Address:    dev.Address,
			VendorId:   dev.VendorID,
			DeviceId:   dev.DeviceID,
			Class:      dev.Class,
			Driver:     dev.Driver,
			IommuGroup: int32(dev.IOMMUGroup),
			VfioGroup:  int32(dev.VFIOGroup),
		})
	}
	return out
}

func (s *ClusterServerImpl) pciDevicesFromProto(devices []*PCIDevice) []cluster.PCIDevice {
	out := make([]cluster.PCIDevice, 0, len(devices))
	for _, dev := range devices {
		if dev == nil {
			continue
		}
		out = append(out, cluster.PCIDevice{
			Address:    dev.Address,
			VendorID:   dev.VendorId,
			DeviceID:   dev.DeviceId,
			Class:      dev.Class,
			Driver:     dev.Driver,
			IOMMUGroup: int(dev.IommuGroup),
			VFIOGroup:  int(dev.VfioGroup),
		})
	}
	return out
}

func (s *ClusterServerImpl) vfioRequirementFromProto(req *VFIORequirement) cluster.VFIORequirement {
	if req == nil {
		return cluster.VFIORequirement{}
	}
	return cluster.VFIORequirement{
		VendorID: req.VendorId,
		DeviceID: req.DeviceId,
		Class:    req.Class,
		Count:    int(req.Count),
	}
}

func (s *ClusterServerImpl) vfioRequirementsFromProto(reqs []*VFIORequirement) []cluster.VFIORequirement {
	out := make([]cluster.VFIORequirement, 0, len(reqs))
	for _, req := range reqs {
		if req == nil {
			continue
		}
		out = append(out, s.vfioRequirementFromProto(req))
	}
	return out
}

func (s *ClusterServerImpl) vfioRequirementsToProto(reqs []cluster.VFIORequirement) []*VFIORequirement {
	out := make([]*VFIORequirement, 0, len(reqs))
	for _, req := range reqs {
		out = append(out, &VFIORequirement{
			VendorId: req.VendorID,
			DeviceId: req.DeviceID,
			Class:    req.Class,
			Count:    int32(req.Count),
		})
	}
	return out
}

func (s *ClusterServerImpl) vfioGroupsToProto(groups []cluster.VFIOGroup) []*VFIOGroup {
	out := make([]*VFIOGroup, 0, len(groups))
	for _, group := range groups {
		out = append(out, &VFIOGroup{
			GroupId:   int32(group.GroupID),
			Devices:   s.pciDevicesToProto(group.Devices),
			ClaimedBy: string(group.ClaimedBy),
		})
	}
	return out
}

func vfioGroupsFromPCIDevices(devices []cluster.PCIDevice) []cluster.VFIOGroup {
	byGroup := make(map[int][]cluster.PCIDevice)
	for _, dev := range devices {
		if dev.VFIOGroup <= 0 {
			continue
		}
		byGroup[dev.VFIOGroup] = append(byGroup[dev.VFIOGroup], dev)
	}
	groups := make([]cluster.VFIOGroup, 0, len(byGroup))
	for groupID, groupDevices := range byGroup {
		groups = append(groups, cluster.VFIOGroup{
			GroupID: groupID,
			Devices: groupDevices,
		})
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].GroupID < groups[j].GroupID
	})
	return groups
}

func vfioGroupsWithClaims(primary, fallback []cluster.VFIOGroup) []cluster.VFIOGroup {
	if len(primary) > 0 {
		return primary
	}
	return fallback
}

type vfioAllocationState struct {
	Allocations []vfioAllocation `json:"allocations"`
}

func loadVFIOAllocationState(path string) ([]vfioAllocation, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil, nil
	}

	var state vfioAllocationState
	if err := json.Unmarshal(data, &state); err == nil {
		return state.Allocations, nil
	}

	var legacy []vfioAllocation
	if err := json.Unmarshal(data, &legacy); err != nil {
		return nil, err
	}
	return legacy, nil
}

func (s *ClusterServerImpl) recoverVFIOAllocations() {
	if s.vfioAllocationStorePath == "" {
		return
	}
	allocations, err := loadVFIOAllocationState(s.vfioAllocationStorePath)
	if err != nil {
		log.Printf("VFIO allocation persistence disabled: load %s: %v", s.vfioAllocationStorePath, err)
		s.vfioAllocationStorePath = ""
		return
	}
	var dropped int
	for _, alloc := range allocations {
		if alloc.NodeID == "" || alloc.SessionID == "" || alloc.JobID == "" || alloc.GroupID <= 0 {
			dropped++
			continue
		}
		key := vfioAllocationKey(alloc.NodeID, alloc.GroupID)
		if _, exists := s.cluster.vfioAllocations[key]; exists {
			dropped++
			continue
		}
		if alloc.CreatedAt.IsZero() {
			alloc.CreatedAt = time.Now()
		}
		s.cluster.vfioAllocations[key] = alloc
		if _, ok := s.cluster.nodes[alloc.NodeID]; ok {
			if !s.reserveRecoveredVFIOAllocationLocked(alloc) {
				delete(s.cluster.vfioAllocations, key)
				dropped++
			}
		}
	}
	if len(s.cluster.vfioAllocations) > 0 {
		log.Printf("Loaded %d VFIO allocation records for daemon restart recovery", len(s.cluster.vfioAllocations))
	}
	if dropped > 0 {
		if err := s.saveVFIOAllocationsLocked(); err != nil {
			log.Printf("compact VFIO allocation state %s after dropping %d stale records: %v", s.vfioAllocationStorePath, dropped, err)
		}
	}
}

func (s *ClusterServerImpl) reconcileVFIOAllocationsForNodeLocked(nodeID cluster.NodeID) {
	var changed bool
	for key, alloc := range s.cluster.vfioAllocations {
		if alloc.NodeID != nodeID {
			continue
		}
		if !s.reserveRecoveredVFIOAllocationLocked(alloc) {
			delete(s.cluster.vfioAllocations, key)
			changed = true
		}
	}
	if changed {
		if err := s.saveVFIOAllocationsLocked(); err != nil {
			log.Printf("compact VFIO allocation state %s after node %s reconciliation: %v", s.vfioAllocationStorePath, nodeID, err)
		}
	}
}

func (s *ClusterServerImpl) reserveRecoveredVFIOAllocationLocked(alloc vfioAllocation) bool {
	if s.scheduler == nil {
		return false
	}
	node, ok := s.scheduler.GetNode(alloc.NodeID)
	if !ok {
		return false
	}
	req := cluster.Requirements{
		JobID:      alloc.JobID,
		VFIOGroups: []int{alloc.GroupID},
	}
	if node.VFIOAllocations != nil {
		if owner, ok := node.VFIOAllocations[alloc.GroupID]; ok {
			if owner != alloc.JobID {
				return false
			}
			node.Release(req)
		}
	}
	allocated, ok := node.ReserveWithAllocation(req)
	if !ok {
		return false
	}
	if len(allocated.VFIOGroups) != 1 || allocated.VFIOGroups[0] != alloc.GroupID {
		node.Release(allocated)
		return false
	}
	return true
}

func (s *ClusterServerImpl) saveVFIOAllocationsLocked() error {
	if s.vfioAllocationStorePath == "" {
		return nil
	}
	records := make([]vfioAllocation, 0, len(s.cluster.vfioAllocations))
	for _, alloc := range s.cluster.vfioAllocations {
		records = append(records, alloc)
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].NodeID == records[j].NodeID {
			return records[i].GroupID < records[j].GroupID
		}
		return records[i].NodeID < records[j].NodeID
	})
	data, err := json.MarshalIndent(vfioAllocationState{Allocations: records}, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(s.vfioAllocationStorePath), 0o755); err != nil {
		return err
	}
	tmp := s.vfioAllocationStorePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.vfioAllocationStorePath); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func (s *ClusterServerImpl) saveVFIOAllocationsForCleanupLocked(reason string) {
	if err := s.saveVFIOAllocationsLocked(); err != nil {
		log.Printf("persist VFIO allocation cleanup after %s: %v", reason, err)
	}
}

func vfioAllocationKey(nodeID cluster.NodeID, groupID int) string {
	return string(nodeID) + ":" + strconv.Itoa(groupID)
}

func vfioDevicePath(groupID int) string {
	return "/dev/vfio/" + strconv.Itoa(groupID)
}

func (s *ClusterServerImpl) vfioGroupExistsLocked(groupID int) bool {
	for _, node := range s.cluster.nodes {
		for _, group := range vfioGroupsWithClaims(node.VFIOGroups, node.Resources.VFIOGroups) {
			if group.GroupID == groupID {
				return true
			}
		}
	}
	return false
}

func releaseVFIOGroupIDs(req *ReleaseVFIORequest) []int {
	seen := make(map[int]bool)
	var out []int
	add := func(groupID int) {
		if groupID <= 0 || seen[groupID] {
			return
		}
		seen[groupID] = true
		out = append(out, groupID)
	}
	add(int(req.GroupId))
	for _, groupID := range req.GroupIds {
		add(int(groupID))
	}
	return out
}

func (s *ClusterServerImpl) vfioAllocationForReleaseLocked(sessionID cluster.SessionID, groupID int) (vfioAllocation, string, bool) {
	var foundForeign bool
	for key, alloc := range s.cluster.vfioAllocations {
		if alloc.GroupID != groupID {
			continue
		}
		if alloc.SessionID != sessionID {
			foundForeign = true
			continue
		}
		return alloc, key, false
	}
	return vfioAllocation{}, "", foundForeign
}

func (s *ClusterServerImpl) releaseVFIOAllocationsForJobLocked(jobID cluster.JobID) {
	var changed bool
	for key, alloc := range s.cluster.vfioAllocations {
		if alloc.JobID != jobID {
			continue
		}
		if s.scheduler != nil {
			s.scheduler.Release(alloc.NodeID, cluster.Requirements{
				JobID:      alloc.JobID,
				VFIOGroups: []int{alloc.GroupID},
			})
		}
		delete(s.cluster.vfioAllocations, key)
		changed = true
	}
	if changed {
		s.saveVFIOAllocationsForCleanupLocked("job " + string(jobID))
	}
}

func (s *ClusterServerImpl) releaseVFIOAllocationsForSessionLocked(sessionID cluster.SessionID) {
	var changed bool
	for key, alloc := range s.cluster.vfioAllocations {
		if alloc.SessionID != sessionID {
			continue
		}
		if s.scheduler != nil {
			s.scheduler.Release(alloc.NodeID, cluster.Requirements{
				JobID:      alloc.JobID,
				VFIOGroups: []int{alloc.GroupID},
			})
		}
		delete(s.cluster.vfioAllocations, key)
		changed = true
	}
	if changed {
		s.saveVFIOAllocationsForCleanupLocked("session " + string(sessionID))
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
			Env:        redactCommandEnv(c.Env),
			Resources:  s.requirementsToProto(c.Resources),
			VfioGroups: c.VFIOGroups,
		}
	}
	out := &JobInfo{
		JobId:       string(job.ID),
		SessionId:   string(job.SessionID),
		Commands:    cmds,
		PipeMap:     job.PipeMap,
		Status:      jobStatusToProto(job.Status),
		CreatedAt:   unixTimeOrZero(job.CreatedAt),
		StartedAt:   unixTimeOrZero(job.StartedAt),
		FinishedAt:  unixTimeOrZero(job.FinishedAt),
		ExitCodes:   int32SliceToProto(job.ExitCodes),
		PrimaryNode: string(job.PrimaryNode),
	}
	if job.RemoteChild != nil {
		out.RemoteChild = remoteChildInfoToProto(job.RemoteChild)
	}
	return out
}

func remoteChildInfoToProto(info *cluster.RemoteChildInfo) *RemoteChildInfo {
	if info == nil {
		return nil
	}
	return &RemoteChildInfo{
		ParentJobId:        string(info.ParentJobID),
		ParentPid:          int32(info.ParentPID),
		ShadowPid:          int32(info.ShadowPID),
		RemoteJobId:        string(info.RemoteJobID),
		RemoteNodeId:       string(info.RemoteNodeID),
		RemoteGlobalPid:    info.RemoteGlobalPID,
		Command:            append([]string{}, info.Command...),
		PlacementReason:    info.PlacementReason,
		FallbackReason:     info.FallbackReason,
		FallbackReasonCode: info.FallbackReasonCode,
		State:              info.State,
		FailureReason:      info.FailureReason,
		StartedAt:          unixTimeOrZero(info.StartedAt),
		FinishedAt:         unixTimeOrZero(info.FinishedAt),
	}
}

func remoteChildInfoFromProto(info *RemoteChildInfo) *cluster.RemoteChildInfo {
	if info == nil {
		return nil
	}
	return &cluster.RemoteChildInfo{
		ParentJobID:        cluster.JobID(info.ParentJobId),
		ParentPID:          int(info.ParentPid),
		ShadowPID:          int(info.ShadowPid),
		RemoteJobID:        cluster.JobID(info.RemoteJobId),
		RemoteNodeID:       cluster.NodeID(info.RemoteNodeId),
		RemoteGlobalPID:    info.RemoteGlobalPid,
		Command:            append([]string{}, info.Command...),
		PlacementReason:    info.PlacementReason,
		FallbackReason:     info.FallbackReason,
		FallbackReasonCode: info.FallbackReasonCode,
		State:              info.State,
		FailureReason:      info.FailureReason,
		StartedAt:          unixTimeFromProto(info.StartedAt),
		FinishedAt:         unixTimeFromProto(info.FinishedAt),
	}
}

func remoteChildInfoFromRecord(record remotechild.ShadowRecord) *cluster.RemoteChildInfo {
	return &cluster.RemoteChildInfo{
		ParentJobID:        cluster.JobID(record.ParentJobID),
		ParentPID:          record.ParentPID,
		ShadowPID:          record.ShadowPID,
		RemoteJobID:        cluster.JobID(record.RemoteJobID),
		RemoteNodeID:       cluster.NodeID(record.RemoteNodeID),
		RemoteGlobalPID:    record.RemoteGlobalPID,
		RemoteLocalPID:     record.RemoteLocalPID,
		Command:            append([]string{}, record.Command...),
		PlacementReason:    record.PlacementReason,
		FallbackReason:     record.FallbackReason,
		FallbackReasonCode: record.FallbackReasonCode,
		State:              string(record.State),
		FailureReason:      record.FailureReason,
		StartedAt:          record.StartedAt,
		FinishedAt:         record.FinishedAt,
	}
}

func remoteChildRecordToProto(record remotechild.ShadowRecord) *RemoteChildRecord {
	return &RemoteChildRecord{
		SessionId:          record.SessionID,
		ParentJobId:        record.ParentJobID,
		ParentPid:          int32(record.ParentPID),
		ShadowPid:          int32(record.ShadowPID),
		RemoteJobId:        record.RemoteJobID,
		RemoteNodeId:       record.RemoteNodeID,
		RemoteGlobalPid:    record.RemoteGlobalPID,
		Command:            append([]string{}, record.Command...),
		State:              string(record.State),
		StartedAt:          unixTimeOrZero(record.StartedAt),
		FinishedAt:         unixTimeOrZero(record.FinishedAt),
		ExitCode:           int32(record.ExitCode),
		Signal:             int32(record.Signal),
		PlacementReason:    record.PlacementReason,
		FallbackReason:     record.FallbackReason,
		FallbackReasonCode: record.FallbackReasonCode,
		FailureReason:      record.FailureReason,
		CreatedAt:          unixTimeOrZero(record.CreatedAt),
		UpdatedAt:          unixTimeOrZero(record.UpdatedAt),
	}
}

func remoteChildRecordFromProto(record *RemoteChildRecord) remotechild.ShadowRecord {
	if record == nil {
		return remotechild.ShadowRecord{}
	}
	return remotechild.ShadowRecord{
		SessionID:          record.SessionId,
		ParentJobID:        record.ParentJobId,
		ParentPID:          int(record.ParentPid),
		ShadowPID:          int(record.ShadowPid),
		RemoteJobID:        record.RemoteJobId,
		RemoteNodeID:       record.RemoteNodeId,
		RemoteGlobalPID:    record.RemoteGlobalPid,
		Command:            append([]string{}, record.Command...),
		State:              remotechild.ShadowState(record.State),
		StartedAt:          unixTimeFromProto(record.StartedAt),
		FinishedAt:         unixTimeFromProto(record.FinishedAt),
		ExitCode:           int(record.ExitCode),
		Signal:             int(record.Signal),
		PlacementReason:    record.PlacementReason,
		FallbackReason:     record.FallbackReason,
		FallbackReasonCode: record.FallbackReasonCode,
		FailureReason:      record.FailureReason,
		CreatedAt:          unixTimeFromProto(record.CreatedAt),
		UpdatedAt:          unixTimeFromProto(record.UpdatedAt),
	}
}

func unixTimeOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

func unixTimeFromProto(seconds int64) time.Time {
	if seconds == 0 {
		return time.Time{}
	}
	return time.Unix(seconds, 0)
}

func dedupeRemoteChildRecords(records []remotechild.ShadowRecord) []remotechild.ShadowRecord {
	byJob := make(map[string]remotechild.ShadowRecord, len(records))
	for _, record := range records {
		if record.RemoteJobID == "" {
			continue
		}
		existing, exists := byJob[record.RemoteJobID]
		if !exists || preferRemoteChildRecord(record, existing) {
			byJob[record.RemoteJobID] = record
		}
	}
	out := make([]remotechild.ShadowRecord, 0, len(byJob))
	for _, record := range byJob {
		out = append(out, record)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out
}

func preferRemoteChildRecord(candidate, existing remotechild.ShadowRecord) bool {
	if candidate.RemoteGlobalPID != 0 && existing.RemoteGlobalPID == 0 {
		return true
	}
	if candidate.UpdatedAt.After(existing.UpdatedAt) {
		return true
	}
	return false
}

func redactCommandEnv(env []string) []string {
	out := make([]string, 0, len(env))
	tokenPrefix := remotechild.EnvChildToken + "="
	for _, entry := range env {
		if strings.HasPrefix(entry, tokenPrefix) {
			continue
		}
		out = append(out, entry)
	}
	return out
}

func nodeStateToProto(state cluster.NodeState) NodeState {
	switch state {
	case cluster.NodeStateActive:
		return NodeState_NODE_STATE_ACTIVE
	case cluster.NodeStateDraining:
		return NodeState_NODE_STATE_DRAINING
	case cluster.NodeStateDrained:
		return NodeState_NODE_STATE_DRAINED
	case cluster.NodeStateMaintenance:
		return NodeState_NODE_STATE_MAINTENANCE
	case cluster.NodeStateOffline:
		return NodeState_NODE_STATE_OFFLINE
	default:
		return NodeState_NODE_STATE_OFFLINE
	}
}

func jobStatusToProto(status cluster.JobStatus) JobStatus {
	switch status {
	case cluster.JobStatusPending:
		return JobStatus_JOB_STATUS_PENDING
	case cluster.JobStatusRunning:
		return JobStatus_JOB_STATUS_RUNNING
	case cluster.JobStatusStopped:
		return JobStatus_JOB_STATUS_STOPPED
	case cluster.JobStatusCompleted:
		return JobStatus_JOB_STATUS_COMPLETED
	case cluster.JobStatusFailed:
		return JobStatus_JOB_STATUS_FAILED
	default:
		return JobStatus_JOB_STATUS_FAILED
	}
}

func (s *ClusterServerImpl) processInfoToProto(p *cluster.ProcessInfo) *ProcessInfo {
	out := &ProcessInfo{
		GlobalPid:   uint64(p.GlobalPID),
		NodeId:      string(p.NodeID),
		LocalPid:    int32(p.LocalPID),
		Ppid:        int32(p.PPID),
		Uid:         p.UID,
		Gid:         p.GID,
		SessionId:   string(p.SessionID),
		JobId:       string(p.JobID),
		Comm:        p.Comm,
		Cmdline:     p.Cmdline,
		Cwd:         p.CWD,
		StartTime:   unixTimeOrZero(p.StartTime),
		CpuPercent:  p.CPUPercent,
		RssBytes:    p.RSSBytes,
		State:       p.State,
		VfioGroups:  p.VFIOGroups,
		ProcessKind: p.ProcessKind,
	}
	if p.RemoteChild != nil {
		out.RemoteChild = remoteChildInfoToProto(p.RemoteChild)
	}
	return out
}

func (s *ClusterServerImpl) appendRemoteChildProcessRecords(procs []*cluster.ProcessInfo, req *ListProcessesRequest) []*cluster.ProcessInfo {
	seenJobs := make(map[cluster.JobID]bool, len(procs))
	for _, proc := range procs {
		seenJobs[proc.JobID] = true
	}
	for _, record := range s.remoteChildren.List(remotechild.ListFilter{ActiveOnly: true}) {
		if req.SessionId != "" && record.SessionID != req.SessionId {
			continue
		}
		if req.JobId != "" && record.RemoteJobID != req.JobId && record.ParentJobID != req.JobId {
			continue
		}
		if req.NodeId != "" && record.RemoteNodeID != req.NodeId {
			continue
		}
		jobID := cluster.JobID(record.RemoteJobID)
		if seenJobs[jobID] {
			continue
		}
		procs = append(procs, &cluster.ProcessInfo{
			GlobalPID:   cluster.GlobalPID(record.RemoteGlobalPID),
			NodeID:      cluster.NodeID(record.RemoteNodeID),
			LocalPID:    0,
			PPID:        record.ParentPID,
			SessionID:   cluster.SessionID(record.SessionID),
			JobID:       jobID,
			Comm:        firstArg(record.Command),
			Cmdline:     strings.Join(record.Command, " "),
			StartTime:   record.StartedAt,
			State:       string(record.State),
			ProcessKind: "remote-child",
			RemoteChild: remoteChildInfoFromRecord(record),
		})
		seenJobs[jobID] = true
	}
	return procs
}

func firstArg(argv []string) string {
	if len(argv) == 0 {
		return ""
	}
	return argv[0]
}

func (s *ClusterServerImpl) protoToRequirements(pb *Requirements) cluster.Requirements {
	if pb == nil {
		return cluster.Requirements{}
	}
	return cluster.Requirements{
		CPU:          pb.CpuMillicores,
		Memory:       pb.MemoryBytes,
		GPUs:         int(pb.Gpus),
		GPUMem:       pb.GpuMemBytes,
		VFIODevs:     s.vfioRequirementsFromProto(pb.VfioDevs),
		NodeAffinity: pb.NodeAffinity,
		SessionID:    cluster.SessionID(pb.SessionId),
		JobID:        cluster.JobID(pb.JobId),
		Priority:     int(pb.Priority),
		Interactive:  pb.Interactive,
	}
}

func (s *ClusterServerImpl) requirementsToProto(req cluster.Requirements) *Requirements {
	return &Requirements{
		CpuMillicores: req.CPU,
		MemoryBytes:   req.Memory,
		Gpus:          int32(req.GPUs),
		GpuMemBytes:   req.GPUMem,
		VfioDevs:      s.vfioRequirementsToProto(req.VFIODevs),
		NodeAffinity:  req.NodeAffinity,
		SessionId:     string(req.SessionID),
		JobId:         string(req.JobID),
		Priority:      int32(req.Priority),
		Interactive:   req.Interactive,
	}
}

func (s *ClusterServerImpl) protoToCommands(argv []string, env []string, cwd string, req cluster.Requirements) []cluster.CommandSpec {
	return []cluster.CommandSpec{
		{Argv: argv, Env: env, Resources: req, VFIOGroups: vfioGroupStrings(req.VFIOGroups)},
	}
}

func vfioGroupStrings(groups []int) []string {
	out := make([]string, 0, len(groups))
	for _, groupID := range groups {
		out = append(out, strconv.Itoa(groupID))
	}
	return out
}

func int32SliceToProto(s []int) []int32 {
	result := make([]int32, len(s))
	for i, v := range s {
		result[i] = int32(v)
	}
	return result
}

// RegisterClusterServerImpl registers the cluster server implementation with gRPC
func RegisterClusterServerImpl(grpcServer *grpc.Server, nodeID cluster.NodeID, sched *scheduler.Scheduler, trk *tracker.Tracker, grpcPort int32, pool *ClusterClientPool) *ClusterServerImpl {
	return RegisterClusterServerImplWithOptions(grpcServer, nodeID, sched, trk, grpcPort, pool, ServerOptions{})
}

func RegisterClusterServerImplWithOptions(grpcServer *grpc.Server, nodeID cluster.NodeID, sched *scheduler.Scheduler, trk *tracker.Tracker, grpcPort int32, pool *ClusterClientPool, opts ServerOptions) *ClusterServerImpl {
	server := NewClusterServerImplWithOptions(nodeID, sched, trk, pool, opts)
	server.grpcPort = grpcPort
	RegisterClusterServer(grpcServer, server)
	return server
}
