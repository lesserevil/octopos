package main

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/octopos/octopos/pkg/remotechild"
	"golang.org/x/sys/unix"
)

func TestParseArgsBuildsConfig(t *testing.T) {
	cfg, command, err := parseArgs([]string{
		"--addr", "shedwards-octo1:50051",
		"--session", "session-a",
		"--job", "job-b",
		"--node", "shedwards-octo2",
		"--cwd", "/work",
		"--cpu", "2",
		"--mem", "3",
		"--gpu", "1",
		"--local-if-unsupported",
		"-t",
		"--",
		"/bin/bash",
		"-lc",
		"echo ok",
	})
	if err != nil {
		t.Fatalf("parseArgs returned error: %v", err)
	}
	if cfg.Addr != "shedwards-octo1:50051" {
		t.Fatalf("Addr = %q", cfg.Addr)
	}
	if !cfg.AddrExplicit {
		t.Fatal("AddrExplicit = false")
	}
	if cfg.SessionID != "session-a" || cfg.JobID != "job-b" {
		t.Fatalf("session/job = %q/%q", cfg.SessionID, cfg.JobID)
	}
	if cfg.NodeID != "shedwards-octo2" || cfg.CWD != "/work" {
		t.Fatalf("node/cwd = %q/%q", cfg.NodeID, cfg.CWD)
	}
	if cfg.CPU != 2 || cfg.MemoryGB != 3 || cfg.GPUs != 1 {
		t.Fatalf("resources = cpu %d mem %d gpus %d", cfg.CPU, cfg.MemoryGB, cfg.GPUs)
	}
	if !cfg.TTY {
		t.Fatal("TTY = false")
	}
	if !cfg.LocalIfUnsupported {
		t.Fatal("LocalIfUnsupported = false")
	}
	if got := len(command); got != 3 {
		t.Fatalf("command length = %d", got)
	}
	if command[0] != "/bin/bash" || command[1] != "-lc" || command[2] != "echo ok" {
		t.Fatalf("command = %#v", command)
	}
}

func TestParseArgsUsesChildHintEnvironment(t *testing.T) {
	t.Setenv(remotechild.EnvChildNode, "shedwards-octo2")
	t.Setenv(remotechild.EnvChildCPU, "3")
	t.Setenv(remotechild.EnvChildMem, "5")
	t.Setenv(remotechild.EnvChildGPU, "1")

	cfg, command, err := parseArgs([]string{"--", "true"})
	if err != nil {
		t.Fatalf("parseArgs returned error: %v", err)
	}
	if len(command) != 1 || command[0] != "true" {
		t.Fatalf("command = %#v", command)
	}
	if cfg.NodeID != "shedwards-octo2" || cfg.CPU != 3 || cfg.MemoryGB != 5 || cfg.GPUs != 1 {
		t.Fatalf("config from env = %#v", cfg)
	}
}

func TestParseArgsUsesLocalHint(t *testing.T) {
	t.Setenv(remotechild.EnvChildLocal, "1")
	t.Setenv("OCTOPOS_NODE_ID", "shedwards-octo1")

	cfg, _, err := parseArgs([]string{"--", "true"})
	if err != nil {
		t.Fatalf("parseArgs returned error: %v", err)
	}
	if cfg.NodeID != "shedwards-octo1" {
		t.Fatalf("NodeID = %q, want parent node", cfg.NodeID)
	}
}

func TestParseArgsFlagsOverrideChildHintEnvironment(t *testing.T) {
	t.Setenv(remotechild.EnvChildNode, "shedwards-octo2")
	t.Setenv(remotechild.EnvChildGPU, "1")

	cfg, _, err := parseArgs([]string{"--node", "shedwards-octo3", "--gpu", "2", "--", "true"})
	if err != nil {
		t.Fatalf("parseArgs returned error: %v", err)
	}
	if cfg.NodeID != "shedwards-octo3" || cfg.GPUs != 2 {
		t.Fatalf("config = %#v, want flag values", cfg)
	}
}

func TestParseArgsRejectsConflictingGPUAliases(t *testing.T) {
	_, _, err := parseArgs([]string{"--gpu", "1", "--gpus", "2", "--", "true"})
	if err == nil {
		t.Fatal("parseArgs succeeded with conflicting gpu aliases")
	}
}

func TestFDPlanOptionsAllowsFileLocksOnlyWhenExplicit(t *testing.T) {
	env := []string{"OCTOPOS_SSI=1"}
	opts := fdPlanOptions(env)
	if !opts.AllowReopen {
		t.Fatal("AllowReopen = false")
	}
	if !opts.AllowPipeProxy {
		t.Fatal("AllowPipeProxy = false")
	}
	if !opts.AllowFIFOProxy {
		t.Fatal("AllowFIFOProxy = false")
	}
	if opts.AllowFileLocks {
		t.Fatal("AllowFileLocks = true without opt-in")
	}

	env = append(env, remotechild.EnvAllowFileLocks+"=1")
	opts = fdPlanOptions(env)
	if !opts.AllowFileLocks {
		t.Fatal("AllowFileLocks = false with opt-in")
	}
}

func TestUnixSocketAvailable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "childd.sock")
	if unixSocketAvailable(path) {
		t.Fatal("unixSocketAvailable returned true for missing path")
	}
	lis, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	defer lis.Close()
	if !unixSocketAvailable(path) {
		t.Fatal("unixSocketAvailable returned false for unix socket")
	}
}

func TestBuildRequestPreservesParentMetadata(t *testing.T) {
	cfg := config{
		SessionID: "session-a",
		JobID:     "job-child",
		NodeID:    "shedwards-octo3",
		CWD:       "/work",
		CPU:       2,
		MemoryGB:  4,
		GPUs:      1,
		TTY:       true,
	}
	req := buildRequest(cfg, []string{"hostname"}, []string{"PATH=/usr/bin", "OCTOPOS_JOB_ID=job-parent", remotechild.EnvChildToken + "=parent-token"}, 123, 45)

	if req.SessionId != "session-a" || req.JobId != "job-child" {
		t.Fatalf("session/job = %q/%q", req.SessionId, req.JobId)
	}
	if req.Command[0] != "hostname" || req.Cwd != "/work" {
		t.Fatalf("command/cwd = %#v/%q", req.Command, req.Cwd)
	}
	if !req.Stdin || !req.Stdout || !req.Stderr || !req.Tty {
		t.Fatalf("stdio/tty flags = stdin %t stdout %t stderr %t tty %t", req.Stdin, req.Stdout, req.Stderr, req.Tty)
	}
	if req.Resources.CpuMillicores != 2000 || req.Resources.MemoryBytes != 4*1024*1024*1024 || req.Resources.Gpus != 1 {
		t.Fatalf("resources = %#v", req.Resources)
	}
	if req.Resources.NodeAffinity["node_id"] != "shedwards-octo3" {
		t.Fatalf("node affinity = %#v", req.Resources.NodeAffinity)
	}
	assertEnv(t, req.Env, "OCTOPOS_REMOTE_CHILD=1")
	assertEnv(t, req.Env, "OCTOPOS_REMOTE_CHILD_REASON=explicit")
	assertEnv(t, req.Env, "OCTOPOS_SHADOW_PID=123")
	assertEnv(t, req.Env, "OCTOPOS_PARENT_PID=45")
	assertEnv(t, req.Env, "OCTOPOS_PARENT_JOB_ID=job-parent")
	assertEnv(t, req.Env, remotechild.EnvChildToken+"=parent-token")
}

func TestBuildRequestStripsPreloadGuard(t *testing.T) {
	cfg := config{SessionID: "session-a", JobID: "job-child", CWD: "/", CPU: 1, MemoryGB: 1}
	req := buildRequest(cfg, []string{"hostname"}, []string{
		"PATH=/usr/bin",
		remotechild.EnvMode + "=safe",
		remotechild.EnvPreloadActive + "=1",
	}, 123, 45)

	assertEnv(t, req.Env, remotechild.EnvPlacementReason+"=transparent")
	assertEnv(t, req.Env, remotechild.EnvMode+"=safe")
	assertNoEnv(t, req.Env, remotechild.EnvPreloadActive)
}

func TestBuildRequestAddsParentNodeSoftAntiAffinity(t *testing.T) {
	cfg := config{SessionID: "session-a", JobID: "job-child", CWD: "/", CPU: 1, MemoryGB: 1}
	req := buildRequest(cfg, []string{"hostname"}, []string{"OCTOPOS_NODE_ID=node-1"}, 123, 45)

	if req.Resources.NodeAffinity["prefer_not_node_id"] != "node-1" {
		t.Fatalf("node affinity = %#v, want prefer_not_node_id=node-1", req.Resources.NodeAffinity)
	}
}

func TestBuildRequestIncludesFDPlan(t *testing.T) {
	cfg := config{SessionID: "session-a", JobID: "job-child", CWD: "/", CPU: 1, MemoryGB: 1}
	req := buildRequestWithFDPlan(cfg, []string{"hostname"}, []string{"OCTOPOS_JOB_ID=job-parent"}, 123, 45, `[{"fd":9,"path":"/tmp/file","flags":2}]`, remotechild.EnvPipeFD(1)+"=12345")

	assertEnv(t, req.Env, remotechild.EnvFDPlan+`=[{"fd":9,"path":"/tmp/file","flags":2}]`)
	assertEnv(t, req.Env, remotechild.EnvPipeFD(1)+"=12345")
}

func TestRemotePipeEnvFromCurrentProcess(t *testing.T) {
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readEnd.Close()
	defer writeEnd.Close()
	if _, err := unix.FcntlInt(writeEnd.Fd(), unix.F_SETFD, 0); err != nil {
		t.Fatalf("clear close-on-exec: %v", err)
	}

	savedStdout, err := unix.Dup(1)
	if err != nil {
		t.Fatalf("dup stdout: %v", err)
	}
	defer unix.Close(savedStdout)
	if err := unix.Dup2(int(writeEnd.Fd()), 1); err != nil {
		t.Fatalf("replace stdout: %v", err)
	}
	defer unix.Dup2(savedStdout, 1)

	env, err := remotePipeEnvFromCurrentProcess()
	if err != nil {
		t.Fatalf("remotePipeEnvFromCurrentProcess: %v", err)
	}
	found := false
	for _, entry := range env {
		if strings.HasPrefix(entry, remotechild.EnvPipeFD(1)+"=") {
			found = true
		}
	}
	if !found {
		t.Fatalf("pipe env missing stdout pipe id: %#v", env)
	}
}

func TestRemotePipeEnvFromCurrentProcessSkipsParentStdioPipe(t *testing.T) {
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readEnd.Close()
	defer writeEnd.Close()

	savedStdout, err := unix.Dup(1)
	if err != nil {
		t.Fatalf("dup stdout: %v", err)
	}
	defer unix.Close(savedStdout)
	if err := unix.Dup2(int(writeEnd.Fd()), 1); err != nil {
		t.Fatalf("replace stdout: %v", err)
	}
	defer unix.Dup2(savedStdout, 1)

	t.Setenv(remotechild.EnvParentStdioPipeFD(1), currentPipeIDForFD(t, 1))
	env, err := remotePipeEnvFromCurrentProcess()
	if err != nil {
		t.Fatalf("remotePipeEnvFromCurrentProcess: %v", err)
	}
	for _, entry := range env {
		if strings.HasPrefix(entry, remotechild.EnvPipeFD(1)+"=") {
			t.Fatalf("parent stdio pipe was not skipped: %#v", env)
		}
	}
}

func TestRemotePipeEnvFromCurrentProcessReportsFIFO(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.fifo")
	if err := unix.Mkfifo(path, 0600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_NONBLOCK, 0600)
	if err != nil {
		t.Fatalf("open fifo: %v", err)
	}
	defer unix.Close(fd)

	savedStdout, err := unix.Dup(1)
	if err != nil {
		t.Fatalf("dup stdout: %v", err)
	}
	defer unix.Close(savedStdout)
	if err := unix.Dup2(fd, 1); err != nil {
		t.Fatalf("replace stdout: %v", err)
	}
	defer unix.Dup2(savedStdout, 1)

	env, err := remotePipeEnvFromCurrentProcess()
	if err != nil {
		t.Fatalf("remotePipeEnvFromCurrentProcess: %v", err)
	}
	want := remotechild.EnvFIFOFD(1) + "=" + path
	for _, entry := range env {
		if entry == want {
			return
		}
	}
	t.Fatalf("fifo env missing %q in %#v", want, env)
}

func TestApplyLocalPolicyRejectsUnsupportedFD(t *testing.T) {
	file := openNonCloseOnExecFile(t)
	defer file.Close()

	err := applyLocalPolicy(config{}, []string{"true"})
	if err == nil {
		t.Fatal("applyLocalPolicy accepted unsupported fd")
	}
}

func TestApplyLocalPolicyAllowsReopenableFDInSSI(t *testing.T) {
	t.Setenv("OCTOPOS_SSI", "1")
	file := openNonCloseOnExecFile(t)
	defer file.Close()

	err := applyLocalPolicy(config{}, []string{"true"})
	if err != nil {
		t.Fatalf("applyLocalPolicy rejected reopenable fd in SSI: %v", err)
	}
	plan, err := remoteFDPlanFromCurrentProcess()
	if err != nil {
		t.Fatalf("remoteFDPlanFromCurrentProcess: %v", err)
	}
	if !strings.Contains(plan, `"fd":`+strconv.Itoa(int(file.Fd()))) {
		t.Fatalf("fd plan %s does not include fd %d", plan, file.Fd())
	}
}

func TestApplyLocalPolicyFallbackExecsLocal(t *testing.T) {
	file := openNonCloseOnExecFile(t)
	defer file.Close()

	called := false
	oldLocalExec := localExec
	localExec = func(command []string) error {
		called = true
		if len(command) != 1 || command[0] != "true" {
			t.Fatalf("command = %#v", command)
		}
		return nil
	}
	defer func() { localExec = oldLocalExec }()

	err := applyLocalPolicy(config{LocalIfUnsupported: true}, []string{"true"})
	if err != nil {
		t.Fatalf("applyLocalPolicy returned error: %v", err)
	}
	if !called {
		t.Fatal("local fallback was not called")
	}
}

func TestTransparentUnsupportedFDFallsBackLocal(t *testing.T) {
	t.Setenv(remotechild.EnvPreloadActive, "1")
	file := openNonCloseOnExecFile(t)
	defer file.Close()

	called := false
	oldLocalExec := localExec
	localExec = func(command []string) error {
		called = true
		if len(command) != 1 || command[0] != "true" {
			t.Fatalf("command = %#v", command)
		}
		return nil
	}
	defer func() { localExec = oldLocalExec }()

	err := applyLocalPolicy(config{}, []string{"true"})
	if err != nil {
		t.Fatalf("applyLocalPolicy returned error: %v", err)
	}
	if !called {
		t.Fatal("transparent unsupported fd did not fall back local")
	}
}

func TestCommandPolicyDeniesHostSensitiveCommands(t *testing.T) {
	allowed, reason := commandPolicyAllowed([]string{"/usr/bin/systemctl", "restart", "octoposd"})
	if allowed {
		t.Fatal("systemctl was allowed")
	}
	if !strings.Contains(reason, "denied") {
		t.Fatalf("reason = %q, want denied", reason)
	}
}

func TestCommandPolicyAllowList(t *testing.T) {
	t.Setenv(remotechild.EnvPolicyAllow, "hostname,/bin/echo")

	if allowed, reason := commandPolicyAllowed([]string{"hostname"}); !allowed {
		t.Fatalf("hostname rejected: %s", reason)
	}
	if allowed, reason := commandPolicyAllowed([]string{"/usr/bin/id"}); allowed {
		t.Fatalf("id was allowed despite allow-list, reason %q", reason)
	}
}

func TestTransparentDeniedCommandFallsBackLocal(t *testing.T) {
	t.Setenv(remotechild.EnvPreloadActive, "1")
	called := false
	oldLocalExec := localExec
	localExec = func(command []string) error {
		called = true
		if command[0] != "systemctl" {
			t.Fatalf("command = %#v", command)
		}
		return nil
	}
	defer func() { localExec = oldLocalExec }()

	if err := applyLocalPolicy(config{}, []string{"systemctl", "status"}); err != nil {
		t.Fatalf("applyLocalPolicy returned error: %v", err)
	}
	if !called {
		t.Fatal("transparent denied command did not fall back local")
	}
}

func assertEnv(t *testing.T, env []string, want string) {
	t.Helper()
	for _, entry := range env {
		if entry == want {
			return
		}
	}
	t.Fatalf("env missing %q in %#v", want, env)
}

func assertNoEnv(t *testing.T, env []string, key string) {
	t.Helper()
	prefix := key + "="
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			t.Fatalf("env should not contain %q in %#v", prefix, env)
		}
	}
}

func openNonCloseOnExecFile(t *testing.T) *os.File {
	t.Helper()
	file, err := os.OpenFile(filepath.Join(t.TempDir(), "open-file"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unix.FcntlInt(file.Fd(), unix.F_SETFD, 0); err != nil {
		t.Fatalf("clear close-on-exec: %v", err)
	}
	return file
}

func currentPipeIDForFD(t *testing.T, fd int) string {
	t.Helper()
	plans, err := remotechild.ClassifyInheritedFDs(os.Getpid())
	if err != nil {
		t.Fatalf("ClassifyInheritedFDs: %v", err)
	}
	for _, plan := range plans {
		if plan.FD != fd {
			continue
		}
		if plan.PipeID == "" {
			t.Fatalf("fd %d plan missing pipe id: %#v", fd, plan)
		}
		return plan.PipeID
	}
	t.Fatalf("fd %d plan not found in %#v", fd, plans)
	return ""
}
