package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/octopos/octopos/pkg/execclient"
	"github.com/octopos/octopos/pkg/remotechild"
	octopospb "github.com/octopos/octopos/pkg/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type config struct {
	Addr               string
	SessionID          string
	JobID              string
	NodeID             string
	CWD                string
	CPU                int
	MemoryGB           int
	GPUs               int
	TTY                bool
	LocalIfUnsupported bool
	AddrExplicit       bool
}

var localExec = execLocal

func main() {
	cfg, command, err := parseArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "octopos-remote-child: %v\n", err)
		os.Exit(2)
	}
	if err := applyLocalPolicy(cfg, command); err != nil {
		fmt.Fprintf(os.Stderr, "octopos-remote-child: %v\n", err)
		os.Exit(1)
	}
	fdPlan, err := remoteFDPlanFromCurrentProcess()
	if err != nil {
		fmt.Fprintf(os.Stderr, "octopos-remote-child: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, endpoint, err := dialCluster(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "octopos-remote-child: connect to %s: %v\n", endpoint, err)
		os.Exit(1)
	}
	defer conn.Close()

	req := buildRequestWithFDPlan(cfg, command, os.Environ(), os.Getpid(), os.Getppid(), fdPlan)
	client := octopospb.NewClusterClient(conn)
	err = execclient.RunForeground(context.Background(), client, req, execclient.Options{
		TTY:            cfg.TTY,
		RawTerminal:    cfg.TTY,
		TerminalFD:     os.Stdin.Fd(),
		ForwardSignals: true,
		OpenStream: func(ctx context.Context) (execclient.Stream, error) {
			return client.RemoteChildStream(ctx)
		},
	})
	if err != nil {
		var exitErr *execclient.ExitError
		if errors.As(err, &exitErr) {
			os.Exit(exitErr.Code)
		}
		fmt.Fprintf(os.Stderr, "octopos-remote-child: %v\n", err)
		os.Exit(1)
	}
}

func applyLocalPolicy(cfg config, command []string) error {
	if os.Getenv(remotechild.EnvForceLocal) == "1" {
		if debugEnabled() {
			fmt.Fprintln(os.Stderr, "octopos-remote-child: forced local execution")
		}
		return localExec(command)
	}
	if allowed, reason := commandPolicyAllowed(command); !allowed {
		if cfg.LocalIfUnsupported || os.Getenv(remotechild.EnvPreloadActive) == "1" {
			if debugEnabled() {
				fmt.Fprintf(os.Stderr, "octopos-remote-child: local fallback: %s\n", reason)
			}
			return localExec(command)
		}
		return errors.New(reason)
	}
	plans, err := remotechild.ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		return err
	}
	plans = remotechild.PrepareFDPlans(plans, remotechild.FDPlanOptions{AllowReopen: ssiEnvActive(os.Environ())})
	unsupported := remotechild.UnsupportedFDs(plans)
	if len(unsupported) == 0 {
		return nil
	}
	reason := remotechild.FormatUnsupportedFDs(unsupported)
	reasonCode := remotechild.FormatUnsupportedReasonCodes(unsupported)
	if cfg.LocalIfUnsupported || os.Getenv(remotechild.EnvPreloadActive) == "1" {
		if debugEnabled() {
			fmt.Fprintf(os.Stderr, "octopos-remote-child: local fallback [%s]: %s\n", reasonCode, reason)
		}
		return localExec(command)
	}
	return fmt.Errorf("unsupported inherited file descriptors [%s]: %s", reasonCode, reason)
}

func remoteFDPlanFromCurrentProcess() (string, error) {
	if !ssiEnvActive(os.Environ()) {
		return "", nil
	}
	plans, err := remotechild.ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		return "", err
	}
	plans = remotechild.PrepareFDPlans(plans, remotechild.FDPlanOptions{AllowReopen: true})
	return remotechild.EncodeReopenFDs(remotechild.ReopenFDs(plans))
}

var defaultDenyCommands = []string{
	"halt",
	"insmod",
	"ip",
	"iptables",
	"modprobe",
	"mount",
	"nft",
	"poweroff",
	"reboot",
	"rmmod",
	"service",
	"shutdown",
	"su",
	"sudo",
	"swapoff",
	"swapon",
	"sysctl",
	"systemctl",
	"umount",
}

func commandPolicyAllowed(command []string) (bool, string) {
	if len(command) == 0 || command[0] == "" {
		return false, "empty command"
	}
	allowPatterns := splitPolicyPatterns(os.Getenv(remotechild.EnvPolicyAllow))
	if len(allowPatterns) > 0 && !matchesAnyCommandPattern(command[0], allowPatterns) {
		return false, fmt.Sprintf("command %q is not allowed by %s", command[0], remotechild.EnvPolicyAllow)
	}
	denyPatterns := append([]string{}, defaultDenyCommands...)
	denyPatterns = append(denyPatterns, splitPolicyPatterns(os.Getenv(remotechild.EnvPolicyDeny))...)
	if matchesAnyCommandPattern(command[0], denyPatterns) {
		return false, fmt.Sprintf("command %q is denied by remote-child policy", command[0])
	}
	return true, ""
}

func splitPolicyPatterns(raw string) []string {
	if raw == "" {
		return nil
	}
	fields := strings.FieldsFunc(raw, func(r rune) bool {
		switch r {
		case ',', ':', ';', '\n', '\t', ' ':
			return true
		default:
			return false
		}
	})
	patterns := make([]string, 0, len(fields))
	for _, field := range fields {
		if field = strings.TrimSpace(field); field != "" {
			patterns = append(patterns, field)
		}
	}
	return patterns
}

func matchesAnyCommandPattern(command string, patterns []string) bool {
	base := filepath.Base(command)
	for _, pattern := range patterns {
		if pattern == "" {
			continue
		}
		if matched, _ := filepath.Match(pattern, command); matched {
			return true
		}
		if matched, _ := filepath.Match(pattern, base); matched {
			return true
		}
		if command == pattern || base == pattern {
			return true
		}
	}
	return false
}

func execLocal(command []string) error {
	path, err := osexec.LookPath(command[0])
	if err != nil {
		return err
	}
	return syscall.Exec(path, command, os.Environ())
}

func debugEnabled() bool {
	return os.Getenv(remotechild.EnvDebug) == "1"
}

func parseArgs(args []string) (config, []string, error) {
	now := time.Now().UnixNano()
	cfg := config{
		Addr:      "127.0.0.1:50051",
		SessionID: firstNonEmpty(os.Getenv("OCTOPOS_SESSION_ID"), fmt.Sprintf("remote-child-%d", now)),
		JobID:     fmt.Sprintf("remote-child-%d-%d", os.Getpid(), now),
		NodeID:    os.Getenv(remotechild.EnvChildNode),
		CWD:       currentWorkingDirectory(),
		CPU:       positiveIntEnv(remotechild.EnvChildCPU, 1),
		MemoryGB:  positiveIntEnv(remotechild.EnvChildMem, 1),
		GPUs:      positiveIntEnv(remotechild.EnvChildGPU, 0),
	}

	flags := flag.NewFlagSet("octopos-remote-child", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	gpus := newTrackedInt(0)
	gpu := newTrackedInt(0)
	tty := false
	shortTTY := false
	flags.StringVar(&cfg.Addr, "addr", cfg.Addr, "gRPC server address")
	flags.StringVar(&cfg.SessionID, "session", cfg.SessionID, "Session ID for the remote child")
	flags.StringVar(&cfg.JobID, "job", cfg.JobID, "Job ID for the remote child")
	flags.StringVar(&cfg.NodeID, "node", cfg.NodeID, "Node affinity for the remote child")
	flags.StringVar(&cfg.CWD, "cwd", cfg.CWD, "Working directory inside the SSI root")
	flags.IntVar(&cfg.CPU, "cpu", cfg.CPU, "CPU cores required")
	flags.IntVar(&cfg.MemoryGB, "mem", cfg.MemoryGB, "Memory required (GB)")
	flags.Var(gpus, "gpus", "GPUs required")
	flags.Var(gpu, "gpu", "GPUs required (alias for --gpus)")
	flags.BoolVar(&tty, "tty", false, "Allocate a pseudo-TTY")
	flags.BoolVar(&shortTTY, "t", false, "Allocate a pseudo-TTY")
	flags.BoolVar(&cfg.LocalIfUnsupported, "local-if-unsupported", false, "Reserved for future fd/IPC fallback policy")

	if err := flags.Parse(args); err != nil {
		return config{}, nil, err
	}
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "addr" {
			cfg.AddrExplicit = true
		}
		if f.Name == "node" {
			cfg.NodeID = strings.TrimSpace(cfg.NodeID)
		}
	})
	if gpu.changed && gpus.changed && gpu.value != gpus.value {
		return config{}, nil, errors.New("--gpu and --gpus specify different values")
	}
	if gpu.changed {
		cfg.GPUs = gpu.value
	} else if gpus.changed {
		cfg.GPUs = gpus.value
	}
	if cfg.NodeID == "" && truthyEnv(remotechild.EnvChildLocal) {
		cfg.NodeID = os.Getenv("OCTOPOS_NODE_ID")
	}
	cfg.TTY = tty || shortTTY

	command := flags.Args()
	if len(command) == 0 {
		return config{}, nil, errors.New("missing command")
	}
	return cfg, command, nil
}

func dialCluster(ctx context.Context, cfg config) (*grpc.ClientConn, string, error) {
	endpoint := cfg.Addr
	if !cfg.AddrExplicit && unixSocketAvailable(remotechild.DefaultSocketPath) {
		endpoint = "unix://" + remotechild.DefaultSocketPath
	}
	conn, err := grpc.DialContext(ctx, endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	return conn, endpoint, err
}

func unixSocketAvailable(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode()&os.ModeSocket != 0
}

func buildRequest(cfg config, command []string, env []string, pid int, ppid int) *octopospb.ExecuteRequest {
	return buildRequestWithFDPlan(cfg, command, env, pid, ppid, "")
}

func buildRequestWithFDPlan(cfg config, command []string, env []string, pid int, ppid int, fdPlan string) *octopospb.ExecuteRequest {
	reason := "explicit"
	if lookupEnv(env, remotechild.EnvPreloadActive) == "1" {
		reason = "transparent"
	}
	reqEnv := filterEnv(env, remotechild.EnvPreloadActive)
	reqEnv = append(reqEnv,
		remotechild.EnvRemoteChild+"=1",
		remotechild.EnvPlacementReason+"="+reason,
		remotechild.EnvShadowPID+"="+strconv.Itoa(pid),
		remotechild.EnvParentPID+"="+strconv.Itoa(ppid),
	)
	if parentJobID := lookupEnv(env, "OCTOPOS_JOB_ID"); parentJobID != "" {
		reqEnv = append(reqEnv, remotechild.EnvParentJobID+"="+parentJobID)
	}
	if fdPlan != "" {
		reqEnv = append(reqEnv, remotechild.EnvFDPlan+"="+fdPlan)
	}

	req := &octopospb.ExecuteRequest{
		SessionId: cfg.SessionID,
		JobId:     cfg.JobID,
		Command:   command,
		Env:       reqEnv,
		Cwd:       cfg.CWD,
		Stdin:     true,
		Stdout:    true,
		Stderr:    true,
		Tty:       cfg.TTY,
		Resources: &octopospb.Requirements{
			CpuMillicores: int64(cfg.CPU * 1000),
			MemoryBytes:   int64(cfg.MemoryGB * 1024 * 1024 * 1024),
			Gpus:          int32(cfg.GPUs),
		},
	}
	if cfg.NodeID != "" {
		req.Resources.NodeAffinity = map[string]string{"node_id": cfg.NodeID}
	} else if parentNodeID := lookupEnv(env, "OCTOPOS_NODE_ID"); parentNodeID != "" {
		req.Resources.NodeAffinity = map[string]string{"prefer_not_node_id": parentNodeID}
	}
	return req
}

func ssiEnvActive(env []string) bool {
	return lookupEnv(env, "OCTOPOS_SSI") == "1"
}

type trackedInt struct {
	value   int
	changed bool
}

func newTrackedInt(value int) *trackedInt {
	return &trackedInt{value: value}
}

func (v *trackedInt) Set(raw string) error {
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return err
	}
	v.value = parsed
	v.changed = true
	return nil
}

func (v *trackedInt) String() string {
	return strconv.Itoa(v.value)
}

func currentWorkingDirectory() string {
	cwd, err := os.Getwd()
	if err != nil || cwd == "" {
		return "/"
	}
	return cwd
}

func positiveIntEnv(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return fallback
	}
	return value
}

func truthyEnv(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func lookupEnv(env []string, key string) string {
	prefix := key + "="
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			return strings.TrimPrefix(entry, prefix)
		}
	}
	return ""
}

func filterEnv(env []string, dropKeys ...string) []string {
	if len(dropKeys) == 0 {
		return append([]string{}, env...)
	}
	drop := make(map[string]struct{}, len(dropKeys))
	for _, key := range dropKeys {
		drop[key] = struct{}{}
	}
	filtered := make([]string, 0, len(env))
	for _, entry := range env {
		key, _, ok := strings.Cut(entry, "=")
		if !ok {
			filtered = append(filtered, entry)
			continue
		}
		if _, shouldDrop := drop[key]; shouldDrop {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}
