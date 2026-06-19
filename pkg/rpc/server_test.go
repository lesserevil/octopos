package rpc

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/octopos/octopos/pkg/cluster"
	"github.com/octopos/octopos/pkg/scheduler"
	"github.com/octopos/octopos/pkg/ssi"
	"github.com/octopos/octopos/pkg/tracker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func TestClusterServer(t *testing.T) {
	// Create test components
	sched := scheduler.NewScheduler(&scheduler.BinPackPolicy{})
	trk := tracker.NewTracker()
	nodeID := cluster.NodeID("test-node")

	// Create server
	server := NewClusterServerImpl(nodeID, sched, trk, nil)

	// Create in-process gRPC server
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	RegisterClusterServer(grpcServer, server)
	go grpcServer.Serve(lis)

	// Create client connection
	conn, err := grpc.DialContext(
		context.Background(),
		"bufnet:",
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithInsecure(),
	)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	client := NewClusterClient(conn)
	ctx := context.Background()

	// Test RegisterNode - use same ID as server so execution is local
	resp, err := client.RegisterNode(ctx, &RegisterNodeRequest{
		NodeId:  "test-node",
		Address: "10.0.0.1",
		Resources: &NodeResources{
			CpuMillicores: 8000,
			MemoryBytes:   32000000000,
		},
	})
	if err != nil {
		t.Fatalf("RegisterNode failed: %v", err)
	}
	if !resp.Success {
		t.Errorf("RegisterNode should succeed")
	}

	// Test GetClusterState
	state, err := client.GetClusterState(ctx, &GetClusterStateRequest{})
	if err != nil {
		t.Fatalf("GetClusterState failed: %v", err)
	}
	if len(state.Nodes) != 1 {
		t.Errorf("Expected 1 node, got %d", len(state.Nodes))
	}

	// Test CreateSession
	sessResp, err := client.CreateSession(ctx, &CreateSessionRequest{
		SessionId: "sess-1",
		User:      "test",
	})
	if err != nil {
		t.Fatalf("CreateSession failed: %v", err)
	}
	if !sessResp.Success {
		t.Errorf("CreateSession should succeed")
	}

	// Test Execute
	execResp, err := client.Execute(ctx, &ExecuteRequest{
		SessionId: "sess-1",
		JobId:     "job-1",
		Command:   []string{"echo", "hello"},
		Resources: &Requirements{
			CpuMillicores: 1000,
			MemoryBytes:   1000000000,
		},
	})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if execResp.JobId != "job-1" {
		t.Errorf("Expected job-1, got %s", execResp.JobId)
	}

	// Test ListProcesses
	procs, err := client.ListProcesses(ctx, &ListProcessesRequest{})
	if err != nil {
		t.Fatalf("ListProcesses failed: %v", err)
	}
	if len(procs.Processes) != 1 {
		t.Errorf("Expected 1 process, got %d", len(procs.Processes))
	}

	// Test ListSessions
	sessions, err := client.ListSessions(ctx, &ListSessionsRequest{})
	if err != nil {
		t.Fatalf("ListSessions failed: %v", err)
	}
	if len(sessions.Sessions) != 1 {
		t.Errorf("Expected 1 session, got %d", len(sessions.Sessions))
	}
}

func TestExecStreamStreamsStdio(t *testing.T) {
	client, cleanup := newTestClusterClient(t)
	defer cleanup()

	registerTestNode(t, client)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.ExecStream(ctx)
	if err != nil {
		t.Fatalf("ExecStream: %v", err)
	}

	if err := stream.Send(&ExecStreamRequest{Payload: &ExecStreamRequest_Exec{Exec: &ExecuteRequest{
		SessionId: "sess-stream",
		JobId:     "job-stream",
		Command:   []string{"/bin/sh", "-c", "cat; echo err >&2"},
		Stdin:     true,
		Stdout:    true,
		Stderr:    true,
		Resources: &Requirements{
			CpuMillicores: 1000,
			MemoryBytes:   1000000000,
		},
	}}}); err != nil {
		t.Fatalf("send exec: %v", err)
	}
	if err := stream.Send(&ExecStreamRequest{Payload: &ExecStreamRequest_StdinData{StdinData: []byte("hello\n")}}); err != nil {
		t.Fatalf("send stdin: %v", err)
	}
	if err := stream.Send(&ExecStreamRequest{Payload: &ExecStreamRequest_CloseStdin{CloseStdin: true}}); err != nil {
		t.Fatalf("close stdin: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}

	var stdout, stderr strings.Builder
	var exitCode int32
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		switch payload := resp.Payload.(type) {
		case *ExecStreamResponse_StdoutData:
			stdout.Write(payload.StdoutData)
		case *ExecStreamResponse_StderrData:
			stderr.Write(payload.StderrData)
		case *ExecStreamResponse_Error:
			t.Fatalf("stream error: %s", payload.Error)
		case *ExecStreamResponse_ExitCode:
			exitCode = payload.ExitCode
		}
	}

	if got, want := stdout.String(), "hello\n"; got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
	if got, want := stderr.String(), "err\n"; got != want {
		t.Fatalf("stderr = %q, want %q", got, want)
	}
	if exitCode != 0 {
		t.Fatalf("exit code = %d, want 0", exitCode)
	}
}

func TestExecStreamTTYAllocatesPTY(t *testing.T) {
	client, cleanup := newTestClusterClient(t)
	defer cleanup()

	registerTestNode(t, client)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.ExecStream(ctx)
	if err != nil {
		t.Fatalf("ExecStream: %v", err)
	}

	if err := stream.Send(&ExecStreamRequest{Payload: &ExecStreamRequest_Exec{Exec: &ExecuteRequest{
		SessionId: "sess-tty",
		JobId:     "job-tty",
		Command:   []string{"/bin/sh", "-c", "test -t 0 && echo tty-ok"},
		Stdin:     true,
		Stdout:    true,
		Stderr:    true,
		Tty:       true,
		Env:       []string{"TERM=xterm", "OCTOPOS_TTY_ROWS=33", "OCTOPOS_TTY_COLS=101"},
		Resources: &Requirements{
			CpuMillicores: 1000,
			MemoryBytes:   1000000000,
		},
	}}}); err != nil {
		t.Fatalf("send exec: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}

	var output strings.Builder
	var exitCode int32
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		switch payload := resp.Payload.(type) {
		case *ExecStreamResponse_StdoutData:
			output.Write(payload.StdoutData)
		case *ExecStreamResponse_StderrData:
			output.Write(payload.StderrData)
		case *ExecStreamResponse_Error:
			t.Fatalf("stream error: %s", payload.Error)
		case *ExecStreamResponse_ExitCode:
			exitCode = payload.ExitCode
		}
	}

	if !strings.Contains(output.String(), "tty-ok") {
		t.Fatalf("PTY output missing tty-ok: %q", output.String())
	}
	if exitCode != 0 {
		t.Fatalf("exit code = %d, want 0", exitCode)
	}
}

func TestExecuteBackgroundAndWait(t *testing.T) {
	client, cleanup := newTestClusterClient(t)
	defer cleanup()

	registerTestNode(t, client)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Execute(ctx, &ExecuteRequest{
		SessionId: "sess-bg",
		JobId:     "job-bg",
		Command:   []string{"/bin/sh", "-c", "exit 7"},
		Resources: &Requirements{
			CpuMillicores: 1000,
			MemoryBytes:   1000000000,
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("Execute error = %q", resp.Error)
	}
	if resp.JobId != "job-bg" {
		t.Fatalf("job id = %q, want job-bg", resp.JobId)
	}

	waitResp, err := client.Wait(ctx, &WaitRequest{JobId: "job-bg", TimeoutMs: 5000})
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if waitResp.TimedOut {
		t.Fatal("Wait timed out")
	}
	if waitResp.ExitCode != 7 {
		t.Fatalf("exit code = %d, want 7", waitResp.ExitCode)
	}
}

func TestStrictSSIRequestUsesConfiguredRoot(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"usr/bin", "usr/lib"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0755); err != nil {
			t.Fatal(err)
		}
	}
	executor := filepath.Join(t.TempDir(), "octopos-exec")
	if err := os.WriteFile(executor, []byte("#!/bin/sh\n"), 0755); err != nil {
		t.Fatal(err)
	}

	server := NewClusterServerImplWithOptions(
		cluster.NodeID("test-node"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
		ServerOptions{SSI: ssi.Config{
			ClusterRoot:  root,
			RootFS:       root,
			Executor:     executor,
			RequireMount: false,
			Required:     true,
		}},
	)

	req := &ExecuteRequest{Command: []string{"pwd"}}
	if err := server.normalizeScheduledRequest(req); err != nil {
		t.Fatalf("normalizeScheduledRequest: %v", err)
	}
	if req.Cwd != "/" {
		t.Fatalf("cwd = %q, want /", req.Cwd)
	}
	dir, err := server.localExecDir(req)
	if err != nil {
		t.Fatalf("localExecDir: %v", err)
	}
	if dir != root {
		t.Fatalf("local dir = %q, want %q", dir, root)
	}
}

func TestStrictSSIEnvIsAuthoritativePerExecutingNode(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"usr/bin", "usr/lib"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0755); err != nil {
			t.Fatal(err)
		}
	}
	executor := filepath.Join(t.TempDir(), "octopos-exec")
	if err := os.WriteFile(executor, []byte("#!/bin/sh\n"), 0755); err != nil {
		t.Fatal(err)
	}

	server := NewClusterServerImplWithOptions(
		cluster.NodeID("worker-node"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
		ServerOptions{SSI: ssi.Config{
			ClusterRoot:  root,
			RootFS:       root,
			Executor:     executor,
			RequireMount: false,
			Required:     true,
		}},
	)

	req := &ExecuteRequest{
		Command: []string{"env"},
		Env: []string{
			"OCTOPOS_NODE_ID=ingress-node",
			"OCTOPOS_SSI=0",
			"USER_VALUE=preserved",
		},
	}
	if err := server.normalizeScheduledRequest(req); err != nil {
		t.Fatalf("normalizeScheduledRequest: %v", err)
	}

	if got, want := envValue(req.Env, "OCTOPOS_NODE_ID"), "worker-node"; got != want {
		t.Fatalf("OCTOPOS_NODE_ID = %q, want %q; env=%v", got, want, req.Env)
	}
	if got, want := envValue(req.Env, "OCTOPOS_SSI"), "1"; got != want {
		t.Fatalf("OCTOPOS_SSI = %q, want %q; env=%v", got, want, req.Env)
	}
	if got, want := envValue(req.Env, "USER_VALUE"), "preserved"; got != want {
		t.Fatalf("USER_VALUE = %q, want %q; env=%v", got, want, req.Env)
	}
}

func TestCloneExecuteRequestForNodePinsNodeAndPreservesAffinity(t *testing.T) {
	req := &ExecuteRequest{
		Resources: &Requirements{
			NodeAffinity: map[string]string{
				"rack":    "r1",
				"node_id": "old-node",
			},
		},
		PipeMap: map[int32]int32{1: 2},
	}

	forwarded := cloneExecuteRequestForNode(req, cluster.NodeID("new-node"))

	if got, want := forwarded.Resources.NodeAffinity["node_id"], "new-node"; got != want {
		t.Fatalf("forwarded node_id = %q, want %q", got, want)
	}
	if got, want := forwarded.Resources.NodeAffinity["rack"], "r1"; got != want {
		t.Fatalf("forwarded rack = %q, want %q", got, want)
	}
	if got, want := req.Resources.NodeAffinity["node_id"], "old-node"; got != want {
		t.Fatalf("original node_id mutated to %q, want %q", got, want)
	}
	forwarded.PipeMap[1] = 3
	if got, want := req.PipeMap[1], int32(2); got != want {
		t.Fatalf("original pipe map mutated to %d, want %d", got, want)
	}
}

func envValue(env []string, key string) string {
	prefix := key + "="
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			return strings.TrimPrefix(entry, prefix)
		}
	}
	return ""
}

func newTestClusterClient(t *testing.T) (ClusterClient, func()) {
	t.Helper()

	sched := scheduler.NewScheduler(&scheduler.BinPackPolicy{})
	trk := tracker.NewTracker()
	server := NewClusterServerImpl(cluster.NodeID("test-node"), sched, trk, nil)

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	RegisterClusterServer(grpcServer, server)
	go grpcServer.Serve(lis)

	conn, err := grpc.DialContext(
		context.Background(),
		"bufnet:",
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithInsecure(),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	cleanup := func() {
		conn.Close()
		grpcServer.Stop()
	}
	return NewClusterClient(conn), cleanup
}

func registerTestNode(t *testing.T, client ClusterClient) {
	t.Helper()

	resp, err := client.RegisterNode(context.Background(), &RegisterNodeRequest{
		NodeId:  "test-node",
		Address: "10.0.0.1",
		Resources: &NodeResources{
			CpuMillicores: 8000,
			MemoryBytes:   32000000000,
			GpuCount:      1,
		},
	})
	if err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if !resp.Success {
		t.Fatalf("RegisterNode failed: %s", resp.Error)
	}
}
