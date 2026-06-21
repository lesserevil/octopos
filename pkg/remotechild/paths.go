package remotechild

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
	EnvMode            = "OCTOPOS_REMOTE_CHILDREN"
	EnvPolicyAllow     = "OCTOPOS_REMOTE_CHILD_ALLOW"
	EnvPolicyDeny      = "OCTOPOS_REMOTE_CHILD_DENY"
	EnvPreloadActive   = "OCTOPOS_REMOTE_CHILD_PRELOAD_ACTIVE"
	EnvRemoteChild     = "OCTOPOS_REMOTE_CHILD"
	EnvPlacementReason = "OCTOPOS_REMOTE_CHILD_REASON"
	EnvParentJobID     = "OCTOPOS_PARENT_JOB_ID"
	EnvParentPID       = "OCTOPOS_PARENT_PID"
	EnvShadowPID       = "OCTOPOS_SHADOW_PID"
)
