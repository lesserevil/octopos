package rpc

import (
	"context"
	"net"
	"testing"

	"github.com/octopos/octopos/pkg/cluster"
	"github.com/octopos/octopos/pkg/scheduler"
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
