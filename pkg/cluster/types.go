package cluster

import (
	"strconv"
	"time"
)

// NodeID uniquely identifies a cluster node
type NodeID string

// SessionID uniquely identifies a user session
type SessionID string

// JobID uniquely identifies a job within a session
type JobID string

// GlobalPID = (NodeID << 32) | LocalPID
type GlobalPID uint64

// ResourceSpec describes available resources on a node
type ResourceSpec struct {
	CPU        int64 // millicores (1000 = 1 core)
	Memory     int64 // bytes
	GPUCount   int
	GPUDevices []GPUDevice
	NUMANodes  int
	PCIDevices []PCIDevice
}

// GPUDevice represents one NVIDIA GPU device visible on a node.
type GPUDevice struct {
	Index int    `json:"index"`
	UUID  string `json:"uuid,omitempty"`
	Path  string `json:"path,omitempty"`
	Major uint32 `json:"major,omitempty"`
	Minor uint32 `json:"minor,omitempty"`
}

// PCIDevice represents a PCI device
type PCIDevice struct {
	Address    string // e.g., "0000:01:00.0"
	VendorID   string
	DeviceID   string
	Class      string
	Driver     string
	IOMMUGroup int
	VFIOGroup  int
}

// NodeState represents the state of a cluster node
type NodeState string

const (
	NodeStateActive      NodeState = "active"
	NodeStateDraining    NodeState = "draining"
	NodeStateDrained     NodeState = "drained"
	NodeStateMaintenance NodeState = "maintenance"
	NodeStateOffline     NodeState = "offline"
)

// Requirements describes job resource requirements
type Requirements struct {
	CPU          int64 // millicores
	Memory       int64 // bytes
	GPUs         int
	GPUMem       int64 // bytes per GPU
	GPUDevices   []GPUDevice
	VFIODevs     []VFIORequirement
	NodeAffinity map[string]string // key=value labels
	SessionID    SessionID
	JobID        JobID
	Priority     int
	Interactive  bool
}

// VFIORequirement describes a VFIO device requirement
type VFIORequirement struct {
	VendorID string
	DeviceID string
	Class    string
	Count    int
}

// ProcessInfo describes a running process
type ProcessInfo struct {
	GlobalPID   GlobalPID        `json:"global_pid"`
	NodeID      NodeID           `json:"node_id"`
	LocalPID    int              `json:"local_pid"`
	PPID        int              `json:"ppid"`
	UID         uint32           `json:"uid"`
	GID         uint32           `json:"gid"`
	SessionID   SessionID        `json:"session_id"`
	JobID       JobID            `json:"job_id"`
	Comm        string           `json:"comm"`
	Cmdline     string           `json:"cmdline"`
	CWD         string           `json:"cwd"`
	StartTime   time.Time        `json:"start_time"`
	CPUPercent  float64          `json:"cpu_percent"`
	RSSBytes    uint64           `json:"rss_bytes"`
	State       string           `json:"state"` // R, S, D, Z, etc.
	VFIOGroups  []string         `json:"vfio_groups"`
	ProcessKind string           `json:"process_kind,omitempty"`
	RemoteChild *RemoteChildInfo `json:"remote_child,omitempty"`
}

// ExecRequest represents a command execution request
type ExecRequest struct {
	SessionID SessionID       `json:"session_id"`
	JobID     JobID           `json:"job_id"`
	Command   []string        `json:"command"`
	Env       []string        `json:"env"`
	CWD       string          `json:"cwd"`
	Stdin     bool            `json:"stdin"`
	Stdout    bool            `json:"stdout"`
	Stderr    bool            `json:"stderr"`
	TTY       bool            `json:"tty"`
	Resources Requirements    `json:"resources"`
	PipeMap   map[int32]int32 `json:"pipe_map"` // stdout[i] -> stdin[j]
}

// ExecResponse represents command execution response
type ExecResponse struct {
	GlobalPID GlobalPID `json:"global_pid"`
	JobID     JobID     `json:"job_id"`
	ExitCode  int       `json:"exit_code"`
	Stdout    []byte    `json:"stdout"`
	Stderr    []byte    `json:"stderr"`
	Error     string    `json:"error,omitempty"`
}

// Session represents a user session
type Session struct {
	ID        SessionID         `json:"id"`
	User      string            `json:"user"`
	NodeID    NodeID            `json:"node_id"` // preferred node
	CreatedAt time.Time         `json:"created_at"`
	TTY       bool              `json:"tty"`
	Env       map[string]string `json:"env"`
	CWD       string            `json:"cwd"`
}

// VFIOAlloc represents a VFIO device allocation
type VFIOAlloc struct {
	DeviceFD    int // file descriptor
	GroupID     int
	DevicePath  string
	ContainerFD int // VFIO container fd
}

// JobStatus represents the status of a job
type JobStatus string

const (
	JobStatusPending   JobStatus = "pending"
	JobStatusRunning   JobStatus = "running"
	JobStatusStopped   JobStatus = "stopped"
	JobStatusCompleted JobStatus = "completed"
	JobStatusFailed    JobStatus = "failed"
)

// CommandSpec describes a single command in a pipeline
type CommandSpec struct {
	Argv       []string     `json:"argv"`
	Env        []string     `json:"env"`
	Resources  Requirements `json:"resources"`
	VFIOGroups []string     `json:"vfio_groups"`
}

// JobInfo tracks a job across nodes
type JobInfo struct {
	ID                  JobID            `json:"id"`
	SessionID           SessionID        `json:"session_id"`
	Commands            []CommandSpec    `json:"commands"`
	PipeMap             map[int32]int32  `json:"pipe_map"`
	Status              JobStatus        `json:"status"`
	CreatedAt           time.Time        `json:"created_at"`
	StartedAt           time.Time        `json:"started_at,omitempty"`
	FinishedAt          time.Time        `json:"finished_at,omitempty"`
	ExitCodes           []int            `json:"exit_codes,omitempty"`
	PrimaryNode         NodeID           `json:"primary_node"`
	RemoteChild         *RemoteChildInfo `json:"remote_child,omitempty"`
	ChildToken          string           `json:"-"`
	ChildTokenExpiresAt time.Time        `json:"-"`
}

// RemoteChildInfo records the explicit local-shadow to remote-worker mapping
// used by octopos-remote-child.
type RemoteChildInfo struct {
	ParentJobID     JobID     `json:"parent_job_id,omitempty"`
	ParentPID       int       `json:"parent_pid,omitempty"`
	ShadowPID       int       `json:"shadow_pid,omitempty"`
	RemoteJobID     JobID     `json:"remote_job_id,omitempty"`
	RemoteNodeID    NodeID    `json:"remote_node_id,omitempty"`
	RemoteGlobalPID uint64    `json:"remote_global_pid,omitempty"`
	RemoteLocalPID  int       `json:"remote_local_pid,omitempty"`
	Command         []string  `json:"command,omitempty"`
	PlacementReason string    `json:"placement_reason,omitempty"`
	FallbackReason  string    `json:"fallback_reason,omitempty"`
	State           string    `json:"state,omitempty"`
	FailureReason   string    `json:"failure_reason,omitempty"`
	StartedAt       time.Time `json:"started_at,omitempty"`
	FinishedAt      time.Time `json:"finished_at,omitempty"`
}

// NodeInfo describes a cluster node
type NodeInfo struct {
	ID             NodeID            `json:"id"`
	Address        string            `json:"address"` // WireGuard IP
	State          NodeState         `json:"state"`
	Resources      ResourceSpec      `json:"resources"`
	Allocated      ResourceSpec      `json:"allocated"`
	Labels         map[string]string `json:"labels"`
	LastHeartbeat  time.Time         `json:"last_heartbeat"`
	GPUAllocations map[int]JobID     `json:"-"`
}

// CanReserve reports whether this node has enough available resources.
func (n *NodeInfo) CanReserve(req Requirements) bool {
	if n.State != NodeStateActive {
		return false
	}

	// Check CPU
	if n.Allocated.CPU+req.CPU > n.Resources.CPU {
		return false
	}
	// Check Memory
	if n.Allocated.Memory+req.Memory > n.Resources.Memory {
		return false
	}
	// Check GPUs
	if n.Allocated.GPUCount+req.GPUs > n.gpuCapacity() {
		return false
	}
	if req.GPUs > 0 && len(n.freeGPUDevices()) < req.GPUs {
		return false
	}

	return true
}

// Reserve attempts to reserve resources atomically.
func (n *NodeInfo) Reserve(req Requirements) bool {
	_, ok := n.ReserveWithAllocation(req)
	return ok
}

// ReserveWithAllocation reserves resources and returns any concrete devices selected.
func (n *NodeInfo) ReserveWithAllocation(req Requirements) (Requirements, bool) {
	if !n.CanReserve(req) {
		return Requirements{}, false
	}

	allocated := req
	if req.GPUs > 0 {
		selected := n.freeGPUDevices()[:req.GPUs]
		allocated.GPUDevices = append([]GPUDevice(nil), selected...)
		if n.GPUAllocations == nil {
			n.GPUAllocations = make(map[int]JobID)
		}
		for _, dev := range selected {
			n.GPUAllocations[dev.Index] = req.JobID
		}
	}

	// Reserve
	n.Allocated.CPU += req.CPU
	n.Allocated.Memory += req.Memory
	n.Allocated.GPUCount += req.GPUs
	return allocated, true
}

// Release releases reserved resources
func (n *NodeInfo) Release(req Requirements) {
	n.Allocated.CPU -= req.CPU
	if n.Allocated.CPU < 0 {
		n.Allocated.CPU = 0
	}
	n.Allocated.Memory -= req.Memory
	if n.Allocated.Memory < 0 {
		n.Allocated.Memory = 0
	}
	n.Allocated.GPUCount -= req.GPUs
	if n.Allocated.GPUCount < 0 {
		n.Allocated.GPUCount = 0
	}
	n.releaseGPUAllocations(req)
}

func (n *NodeInfo) gpuCapacity() int {
	if len(n.Resources.GPUDevices) > 0 {
		return len(n.Resources.GPUDevices)
	}
	return n.Resources.GPUCount
}

func (n *NodeInfo) freeGPUDevices() []GPUDevice {
	devices := n.Resources.GPUDevices
	if len(devices) == 0 && n.Resources.GPUCount > 0 {
		devices = make([]GPUDevice, 0, n.Resources.GPUCount)
		for i := 0; i < n.Resources.GPUCount; i++ {
			devices = append(devices, GPUDevice{
				Index: i,
				Path:  "/dev/nvidia" + strconv.Itoa(i),
			})
		}
	}

	free := make([]GPUDevice, 0, len(devices))
	for _, dev := range devices {
		if _, allocated := n.GPUAllocations[dev.Index]; !allocated {
			free = append(free, dev)
		}
	}
	return free
}

func (n *NodeInfo) releaseGPUAllocations(req Requirements) {
	if len(n.GPUAllocations) == 0 || req.GPUs <= 0 {
		return
	}
	if len(req.GPUDevices) > 0 {
		for _, dev := range req.GPUDevices {
			delete(n.GPUAllocations, dev.Index)
		}
		return
	}

	released := 0
	for index, owner := range n.GPUAllocations {
		if req.JobID != "" && owner != req.JobID {
			continue
		}
		delete(n.GPUAllocations, index)
		released++
		if released >= req.GPUs {
			return
		}
	}
	if req.JobID != "" {
		return
	}
	for index := range n.GPUAllocations {
		delete(n.GPUAllocations, index)
		released++
		if released >= req.GPUs {
			return
		}
	}
}
