package remotechild

import "strconv"

const DefaultSocketPath = "/run/octopos/childd.sock"

const (
	EnvChildToken      = "OCTOPOS_CHILD_TOKEN"
	EnvChildCPU        = "OCTOPOS_CHILD_CPU"
	EnvChildGPU        = "OCTOPOS_CHILD_GPU"
	EnvChildLocal      = "OCTOPOS_CHILD_LOCAL"
	EnvChildMem        = "OCTOPOS_CHILD_MEM"
	EnvChildNode       = "OCTOPOS_CHILD_NODE"
	EnvDebug           = "OCTOPOS_REMOTE_CHILD_DEBUG"
	EnvFDPlan          = "OCTOPOS_REMOTE_CHILD_FD_PLAN"
	EnvForceLocal      = "OCTOPOS_REMOTE_CHILD_FORCE_LOCAL"
	EnvHostFDDir       = "OCTOPOS_HOST_FD_DIR"
	EnvIPCCompat       = "OCTOPOS_REMOTE_IPC_COMPAT"
	EnvAllowFileLocks  = "OCTOPOS_REMOTE_CHILD_ALLOW_FILE_LOCKS"
	EnvBrokerAddr      = "OCTOPOS_BROKER_ADDR"
	EnvMode            = "OCTOPOS_REMOTE_CHILDREN"
	EnvFIFOFDPrefix    = "OCTOPOS_REMOTE_CHILD_FIFO_FD_"
	EnvPipeCoordinator = "OCTOPOS_REMOTE_CHILD_PIPE_COORD_NODE"
	EnvPipeFDPrefix    = "OCTOPOS_REMOTE_CHILD_PIPE_FD_"
	EnvPolicyAllow     = "OCTOPOS_REMOTE_CHILD_ALLOW"
	EnvPolicyDeny      = "OCTOPOS_REMOTE_CHILD_DENY"
	EnvPreloadActive   = "OCTOPOS_REMOTE_CHILD_PRELOAD_ACTIVE"
	EnvRemoteChild     = "OCTOPOS_REMOTE_CHILD"
	EnvPlacementReason = "OCTOPOS_REMOTE_CHILD_REASON"
	EnvFallbackReason  = "OCTOPOS_REMOTE_CHILD_FALLBACK_REASON"
	EnvFallbackCode    = "OCTOPOS_REMOTE_CHILD_FALLBACK_CODE"
	EnvParentJobID     = "OCTOPOS_PARENT_JOB_ID"
	EnvParentPID       = "OCTOPOS_PARENT_PID"
	EnvShadowPID       = "OCTOPOS_SHADOW_PID"
)

func EnvPipeFD(fd int) string {
	return EnvPipeFDPrefix + strconv.Itoa(fd)
}

func EnvFIFOFD(fd int) string {
	return EnvFIFOFDPrefix + strconv.Itoa(fd)
}
