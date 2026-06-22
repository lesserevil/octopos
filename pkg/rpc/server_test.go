package rpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/octopos/octopos/pkg/cluster"
	"github.com/octopos/octopos/pkg/remotechild"
	"github.com/octopos/octopos/pkg/scheduler"
	"github.com/octopos/octopos/pkg/ssi"
	"github.com/octopos/octopos/pkg/tracker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
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

func TestRegisterNodeCarriesVFIOGroupsFromPCIDevices(t *testing.T) {
	server := NewClusterServerImpl("test-node", scheduler.NewScheduler(&scheduler.BinPackPolicy{}), tracker.NewTracker(), nil)

	resp, err := server.RegisterNode(context.Background(), &RegisterNodeRequest{
		NodeId:  "vfio-node",
		Address: "10.0.0.2",
		Resources: &NodeResources{
			CpuMillicores: 4000,
			MemoryBytes:   8_000_000_000,
			PciDevices: []*PCIDevice{{
				Address:    "0000:01:00.0",
				VendorId:   "8086",
				DeviceId:   "10fb",
				Class:      "020000",
				Driver:     "vfio-pci",
				IommuGroup: 7,
				VfioGroup:  7,
			}},
		},
	})
	if err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if !resp.Success {
		t.Fatalf("RegisterNode failed: %s", resp.Error)
	}

	state, err := server.GetClusterState(context.Background(), &GetClusterStateRequest{})
	if err != nil {
		t.Fatalf("GetClusterState: %v", err)
	}
	if len(state.Nodes) != 1 {
		t.Fatalf("nodes = %d, want 1", len(state.Nodes))
	}
	node := state.Nodes[0]
	if len(node.Capacity.PciDevices) != 1 {
		t.Fatalf("PCI devices = %+v, want one", node.Capacity.PciDevices)
	}
	if len(node.VfioGroups) != 1 || node.VfioGroups[0].GroupId != 7 {
		t.Fatalf("VFIO groups = %+v, want group 7", node.VfioGroups)
	}
}

func TestVFIOAllocateListAndRelease(t *testing.T) {
	sched := scheduler.NewScheduler(&scheduler.BinPackPolicy{})
	node := testVFIONode()
	sched.AddNode(node)
	server := NewClusterServerImpl("test-node", sched, tracker.NewTracker(), nil)

	listBefore, err := server.GetVFIODevices(context.Background(), &GetVFIODevicesRequest{NodeId: "node-1"})
	if err != nil {
		t.Fatalf("GetVFIODevices before: %v", err)
	}
	if len(listBefore.Groups) != 1 || listBefore.Groups[0].ClaimedBy != "" {
		t.Fatalf("groups before = %+v, want one unclaimed group", listBefore.Groups)
	}

	alloc, err := server.AllocateVFIO(context.Background(), &AllocateVFIORequest{
		SessionId: "sess-1",
		JobId:     "job-1",
		Device: &VFIORequirement{
			VendorId: "8086",
			DeviceId: "10fb",
			Class:    "0200",
			Count:    1,
		},
	})
	if err != nil {
		t.Fatalf("AllocateVFIO: %v", err)
	}
	if !alloc.Success {
		t.Fatalf("AllocateVFIO failed: %s", alloc.Error)
	}
	if alloc.GroupId != 7 || alloc.DevicePath != "/dev/vfio/7" || alloc.ContainerFd != -1 || alloc.DeviceFd != -1 {
		t.Fatalf("allocation = %+v, want group 7 and fd placeholders", alloc)
	}
	if node.VFIOAllocations[7] != "job-1" {
		t.Fatalf("node VFIO allocations = %+v, want job-1 owner", node.VFIOAllocations)
	}

	listAfter, err := server.GetVFIODevices(context.Background(), &GetVFIODevicesRequest{NodeId: "node-1"})
	if err != nil {
		t.Fatalf("GetVFIODevices after: %v", err)
	}
	if len(listAfter.Groups) != 1 || listAfter.Groups[0].ClaimedBy != "job-1" {
		t.Fatalf("groups after = %+v, want claimed by job-1", listAfter.Groups)
	}

	foreignRelease, err := server.ReleaseVFIO(context.Background(), &ReleaseVFIORequest{
		SessionId: "sess-2",
		GroupId:   7,
	})
	if err != nil {
		t.Fatalf("foreign ReleaseVFIO: %v", err)
	}
	if foreignRelease.Success {
		t.Fatal("foreign release succeeded, want denial")
	}

	release, err := server.ReleaseVFIO(context.Background(), &ReleaseVFIORequest{
		SessionId: "sess-1",
		GroupId:   7,
	})
	if err != nil {
		t.Fatalf("ReleaseVFIO: %v", err)
	}
	if !release.Success {
		t.Fatalf("ReleaseVFIO failed: %s", release.Error)
	}
	if len(node.VFIOAllocations) != 0 {
		t.Fatalf("node VFIO allocations = %+v, want empty", node.VFIOAllocations)
	}
}

func TestAllocateVFIORejectsDoubleAllocation(t *testing.T) {
	sched := scheduler.NewScheduler(&scheduler.BinPackPolicy{})
	sched.AddNode(testVFIONode())
	server := NewClusterServerImpl("test-node", sched, tracker.NewTracker(), nil)
	req := &AllocateVFIORequest{
		SessionId: "sess-1",
		JobId:     "job-1",
		Device:    &VFIORequirement{VendorId: "8086", Count: 1},
	}
	first, err := server.AllocateVFIO(context.Background(), req)
	if err != nil {
		t.Fatalf("first AllocateVFIO: %v", err)
	}
	if !first.Success {
		t.Fatalf("first AllocateVFIO failed: %s", first.Error)
	}

	second, err := server.AllocateVFIO(context.Background(), &AllocateVFIORequest{
		SessionId: "sess-2",
		JobId:     "job-2",
		Device:    &VFIORequirement{VendorId: "8086", Count: 1},
	})
	if err != nil {
		t.Fatalf("second AllocateVFIO: %v", err)
	}
	if second.Success {
		t.Fatal("second AllocateVFIO succeeded, want no eligible node")
	}
}

func TestAllocateVFIOMultipleGroups(t *testing.T) {
	sched := scheduler.NewScheduler(&scheduler.BinPackPolicy{})
	node := testVFIONodeWithGroups(7, 8)
	sched.AddNode(node)
	server := NewClusterServerImpl("test-node", sched, tracker.NewTracker(), nil)

	alloc, err := server.AllocateVFIO(context.Background(), &AllocateVFIORequest{
		SessionId: "sess-1",
		JobId:     "job-1",
		Device:    &VFIORequirement{VendorId: "8086", Count: 2},
	})
	if err != nil {
		t.Fatalf("AllocateVFIO: %v", err)
	}
	if !alloc.Success {
		t.Fatalf("AllocateVFIO failed: %s", alloc.Error)
	}
	if !proto.Equal(alloc, &AllocateVFIOResponse{
		Success:     true,
		GroupId:     7,
		ContainerFd: -1,
		DeviceFd:    -1,
		DevicePath:  "/dev/vfio/7",
		GroupIds:    []int32{7, 8},
		DevicePaths: []string{"/dev/vfio/7", "/dev/vfio/8"},
	}) {
		t.Fatalf("allocation = %+v, want groups 7 and 8", alloc)
	}
	if node.VFIOAllocations[7] != "job-1" || node.VFIOAllocations[8] != "job-1" {
		t.Fatalf("node VFIO allocations = %+v, want job-1 owner for groups 7 and 8", node.VFIOAllocations)
	}

	release, err := server.ReleaseVFIO(context.Background(), &ReleaseVFIORequest{
		SessionId: "sess-1",
		GroupIds:  []int32{7, 8},
	})
	if err != nil {
		t.Fatalf("ReleaseVFIO: %v", err)
	}
	if !release.Success {
		t.Fatalf("ReleaseVFIO failed: %s", release.Error)
	}
	if len(node.VFIOAllocations) != 0 {
		t.Fatalf("node VFIO allocations = %+v, want empty", node.VFIOAllocations)
	}
}

func TestDestroySessionReleasesVFIOAllocations(t *testing.T) {
	sched := scheduler.NewScheduler(&scheduler.BinPackPolicy{})
	node := testVFIONode()
	sched.AddNode(node)
	server := NewClusterServerImpl("test-node", sched, tracker.NewTracker(), nil)
	if _, err := server.CreateSession(context.Background(), &CreateSessionRequest{
		SessionId: "sess-1",
		User:      "test",
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	alloc, err := server.AllocateVFIO(context.Background(), &AllocateVFIORequest{
		SessionId: "sess-1",
		JobId:     "job-1",
		Device:    &VFIORequirement{VendorId: "8086", Count: 1},
	})
	if err != nil {
		t.Fatalf("AllocateVFIO: %v", err)
	}
	if !alloc.Success {
		t.Fatalf("AllocateVFIO failed: %s", alloc.Error)
	}

	resp, err := server.DestroySession(context.Background(), &DestroySessionRequest{SessionId: "sess-1"})
	if err != nil {
		t.Fatalf("DestroySession: %v", err)
	}
	if !resp.Success {
		t.Fatalf("DestroySession failed: %s", resp.Error)
	}
	if len(node.VFIOAllocations) != 0 {
		t.Fatalf("VFIO allocations = %+v, want empty", node.VFIOAllocations)
	}
	list, err := server.GetVFIODevices(context.Background(), &GetVFIODevicesRequest{NodeId: "node-1"})
	if err != nil {
		t.Fatalf("GetVFIODevices: %v", err)
	}
	if len(list.Groups) != 1 || list.Groups[0].ClaimedBy != "" {
		t.Fatalf("VFIO groups after destroy = %+v, want unclaimed", list.Groups)
	}
}

func TestVFIOAllocationsPersistAndRecover(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "vfio-allocations.json")

	sched := scheduler.NewScheduler(&scheduler.BinPackPolicy{})
	node := testVFIONode()
	sched.AddNode(node)
	server := NewClusterServerImplWithOptions("test-node", sched, tracker.NewTracker(), nil, ServerOptions{
		VFIOAllocationStorePath: statePath,
	})
	alloc, err := server.AllocateVFIO(context.Background(), &AllocateVFIORequest{
		SessionId: "sess-1",
		JobId:     "job-1",
		Device:    &VFIORequirement{VendorId: "8086", Count: 1},
	})
	if err != nil {
		t.Fatalf("AllocateVFIO: %v", err)
	}
	if !alloc.Success {
		t.Fatalf("AllocateVFIO failed: %s", alloc.Error)
	}
	if _, err := os.Stat(statePath); err != nil {
		t.Fatalf("stat VFIO state: %v", err)
	}

	recoveredSched := scheduler.NewScheduler(&scheduler.BinPackPolicy{})
	recoveredNode := testVFIONode()
	recoveredSched.AddNode(recoveredNode)
	recovered := NewClusterServerImplWithOptions("test-node", recoveredSched, tracker.NewTracker(), nil, ServerOptions{
		VFIOAllocationStorePath: statePath,
	})
	if recoveredNode.VFIOAllocations[7] != "job-1" {
		t.Fatalf("recovered VFIO allocations = %+v, want job-1", recoveredNode.VFIOAllocations)
	}
	list, err := recovered.GetVFIODevices(context.Background(), &GetVFIODevicesRequest{NodeId: "node-1"})
	if err != nil {
		t.Fatalf("GetVFIODevices: %v", err)
	}
	if len(list.Groups) != 1 || list.Groups[0].ClaimedBy != "job-1" {
		t.Fatalf("recovered VFIO groups = %+v, want claimed by job-1", list.Groups)
	}
}

func TestVFIOReleaseUpdatesPersistentState(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "vfio-allocations.json")

	sched := scheduler.NewScheduler(&scheduler.BinPackPolicy{})
	node := testVFIONode()
	sched.AddNode(node)
	server := NewClusterServerImplWithOptions("test-node", sched, tracker.NewTracker(), nil, ServerOptions{
		VFIOAllocationStorePath: statePath,
	})
	alloc, err := server.AllocateVFIO(context.Background(), &AllocateVFIORequest{
		SessionId: "sess-1",
		JobId:     "job-1",
		Device:    &VFIORequirement{VendorId: "8086", Count: 1},
	})
	if err != nil {
		t.Fatalf("AllocateVFIO: %v", err)
	}
	if !alloc.Success {
		t.Fatalf("AllocateVFIO failed: %s", alloc.Error)
	}
	release, err := server.ReleaseVFIO(context.Background(), &ReleaseVFIORequest{
		SessionId: "sess-1",
		GroupId:   7,
	})
	if err != nil {
		t.Fatalf("ReleaseVFIO: %v", err)
	}
	if !release.Success {
		t.Fatalf("ReleaseVFIO failed: %s", release.Error)
	}

	recoveredSched := scheduler.NewScheduler(&scheduler.BinPackPolicy{})
	recoveredNode := testVFIONode()
	recoveredSched.AddNode(recoveredNode)
	NewClusterServerImplWithOptions("test-node", recoveredSched, tracker.NewTracker(), nil, ServerOptions{
		VFIOAllocationStorePath: statePath,
	})
	if len(recoveredNode.VFIOAllocations) != 0 {
		t.Fatalf("recovered VFIO allocations = %+v, want empty", recoveredNode.VFIOAllocations)
	}
}

func testVFIONode() *cluster.NodeInfo {
	return testVFIONodeWithGroups(7)
}

func testVFIONodeWithGroups(groupIDs ...int) *cluster.NodeInfo {
	groups := make([]cluster.VFIOGroup, 0, len(groupIDs))
	for _, groupID := range groupIDs {
		groups = append(groups, cluster.VFIOGroup{
			GroupID: groupID,
			Devices: []cluster.PCIDevice{{
				Address:   fmt.Sprintf("0000:%02x:00.0", groupID),
				VendorID:  "8086",
				DeviceID:  "10fb",
				Class:     "020000",
				Driver:    "vfio-pci",
				VFIOGroup: groupID,
			}},
		})
	}
	return &cluster.NodeInfo{
		ID:    "node-1",
		State: cluster.NodeStateActive,
		Resources: cluster.ResourceSpec{
			CPU:        8000,
			Memory:     16_000_000_000,
			VFIOGroups: groups,
		},
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
		Command:   []string{"/bin/sh", "-c", "test -t 0 && tty"},
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
	var execResp *ExecuteResponse
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		switch payload := resp.Payload.(type) {
		case *ExecStreamResponse_Exec:
			execResp = payload.Exec
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

	if strings.Contains(output.String(), "not a tty") || !strings.Contains(output.String(), "/dev/") {
		t.Fatalf("PTY output does not name a tty: %q", output.String())
	}
	if exitCode != 0 {
		t.Fatalf("exit code = %d, want 0", exitCode)
	}
	if execResp == nil {
		t.Fatal("missing exec response")
	}
	if execResp.ProcessGroupId <= 0 || execResp.KernelSessionId <= 0 || execResp.ForegroundProcessGroupId <= 0 {
		t.Fatalf("exec process-control metadata = pgid:%d sid:%d fg:%d", execResp.ProcessGroupId, execResp.KernelSessionId, execResp.ForegroundProcessGroupId)
	}
	jobResp, err := client.GetJobStatus(ctx, &GetJobStatusRequest{JobId: "job-tty"})
	if err != nil {
		t.Fatalf("GetJobStatus: %v", err)
	}
	if !jobResp.Found || jobResp.Job == nil {
		t.Fatal("job status missing for PTY exec")
	}
	if jobResp.Job.ProcessGroupId != execResp.ProcessGroupId || jobResp.Job.KernelSessionId != execResp.KernelSessionId || jobResp.Job.ForegroundProcessGroupId != execResp.ForegroundProcessGroupId {
		t.Fatalf("job process-control metadata = pgid:%d sid:%d fg:%d, exec = pgid:%d sid:%d fg:%d",
			jobResp.Job.ProcessGroupId, jobResp.Job.KernelSessionId, jobResp.Job.ForegroundProcessGroupId,
			execResp.ProcessGroupId, execResp.KernelSessionId, execResp.ForegroundProcessGroupId)
	}
}

func TestRemoteChildStreamRequiresRemoteChildMarker(t *testing.T) {
	client, cleanup := newTestClusterClient(t)
	defer cleanup()

	registerTestNode(t, client)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.RemoteChildStream(ctx)
	if err != nil {
		t.Fatalf("RemoteChildStream: %v", err)
	}
	if err := stream.Send(&ExecStreamRequest{Payload: &ExecStreamRequest_Exec{Exec: &ExecuteRequest{
		SessionId: "sess-child",
		JobId:     "job-child",
		Command:   []string{"/bin/true"},
		Resources: &Requirements{
			CpuMillicores: 1000,
			MemoryBytes:   1000000000,
		},
	}}}); err != nil {
		t.Fatalf("send exec: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	payload, ok := resp.Payload.(*ExecStreamResponse_Error)
	if !ok {
		t.Fatalf("response payload = %T, want error", resp.Payload)
	}
	if !strings.Contains(payload.Error, remotechild.EnvRemoteChild) {
		t.Fatalf("error = %q, want missing marker", payload.Error)
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
			"OCTOPOS_BROKER_ADDR=user-supplied:1",
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
	if got, want := envValue(req.Env, remotechild.EnvBrokerAddr), "127.0.0.1:50051"; got != want {
		t.Fatalf("%s = %q, want %q; env=%v", remotechild.EnvBrokerAddr, got, want, req.Env)
	}
	if got, want := envValue(req.Env, "USER_VALUE"), "preserved"; got != want {
		t.Fatalf("USER_VALUE = %q, want %q; env=%v", got, want, req.Env)
	}
}

func TestRemoteChildInfoFromEnv(t *testing.T) {
	req := &ExecuteRequest{
		Command: []string{"/bin/sh", "-c", "echo child"},
		Env: []string{
			"OCTOPOS_REMOTE_CHILD=1",
			"OCTOPOS_REMOTE_CHILD_REASON=explicit",
			"OCTOPOS_PARENT_JOB_ID=job-parent",
			"OCTOPOS_PARENT_PID=100",
			"OCTOPOS_SHADOW_PID=101",
		},
	}

	info := remoteChildInfoFromEnv(req, cluster.JobID("job-child"), cluster.NodeID("node-2"))
	if info == nil {
		t.Fatal("remoteChildInfoFromEnv returned nil")
	}
	if info.ParentJobID != "job-parent" || info.RemoteJobID != "job-child" || info.RemoteNodeID != "node-2" {
		t.Fatalf("job/node metadata = %#v", info)
	}
	if info.ParentPID != 100 || info.ShadowPID != 101 {
		t.Fatalf("pid metadata = parent %d shadow %d", info.ParentPID, info.ShadowPID)
	}
	if got := strings.Join(info.Command, " "); got != "/bin/sh -c echo child" {
		t.Fatalf("command = %q", got)
	}
	if info.PlacementReason != "explicit" {
		t.Fatalf("placement reason = %q", info.PlacementReason)
	}
}

func TestRemoteChildInfoFromTypedLaunch(t *testing.T) {
	req := &ExecuteRequest{
		Command: []string{"/bin/sh", "-c", "echo child"},
		RemoteChild: &RemoteChildLaunch{
			ParentJobId:        "job-parent",
			ParentPid:          100,
			ShadowPid:          101,
			PlacementReason:    "transparent",
			FallbackReason:     "local fallback reason",
			FallbackReasonCode: "ipc.memfd",
			ChildToken:         "parent-token",
			FdPlan:             `[{"fd":9}]`,
		},
		Env: []string{
			remotechild.EnvChildToken + "=legacy-token",
			"USER_VALUE=preserved",
		},
	}

	if !isRemoteChildRequest(req) {
		t.Fatal("typed remote child request was not recognized")
	}
	info := remoteChildInfoFromEnv(req, cluster.JobID("job-child"), cluster.NodeID("node-2"))
	if info == nil {
		t.Fatal("remoteChildInfoFromEnv returned nil")
	}
	if info.ParentJobID != "job-parent" || info.ParentPID != 100 || info.ShadowPID != 101 {
		t.Fatalf("typed parent metadata = %#v", info)
	}
	if info.PlacementReason != "transparent" || info.FallbackReason != "local fallback reason" || info.FallbackReasonCode != "ipc.memfd" {
		t.Fatalf("typed reason metadata = %#v", info)
	}

	normalizeRemoteChildRequest(req)
	if got := requestEnvValue(req.Env, remotechild.EnvChildToken); got != "" {
		t.Fatalf("parent token remained in env as %q", got)
	}
	for key, want := range map[string]string{
		remotechild.EnvRemoteChild:     "1",
		remotechild.EnvParentJobID:     "job-parent",
		remotechild.EnvParentPID:       "100",
		remotechild.EnvShadowPID:       "101",
		remotechild.EnvPlacementReason: "transparent",
		remotechild.EnvFallbackReason:  "local fallback reason",
		remotechild.EnvFallbackCode:    "ipc.memfd",
		remotechild.EnvFDPlan:          `[{"fd":9}]`,
		"USER_VALUE":                   "preserved",
	} {
		if got := requestEnvValue(req.Env, key); got != want {
			t.Fatalf("%s = %q, want %q; env=%v", key, got, want, req.Env)
		}
	}
}

func TestRemoteChildExecuteRequestToExecuteRequest(t *testing.T) {
	req := &RemoteChildExecuteRequest{
		Exec: &ExecuteRequest{
			SessionId: "session-1",
			JobId:     "job-child",
			Command:   []string{"hostname"},
			Env:       []string{"USER_VALUE=preserved"},
		},
		Launch: &RemoteChildLaunch{
			ParentJobId: "job-parent",
			ParentPid:   100,
			ShadowPid:   101,
			ChildToken:  "parent-token",
		},
	}

	execReq, err := remoteChildExecuteRequestToExecuteRequest(req)
	if err != nil {
		t.Fatalf("remoteChildExecuteRequestToExecuteRequest: %v", err)
	}
	if execReq.RemoteChild == nil || execReq.RemoteChild.ParentJobId != "job-parent" || execReq.RemoteChild.ChildToken != "parent-token" {
		t.Fatalf("remote child launch = %#v", execReq.RemoteChild)
	}
	if req.Exec.RemoteChild != nil {
		t.Fatalf("source exec request mutated: %#v", req.Exec.RemoteChild)
	}

	_, err = remoteChildExecuteRequestToExecuteRequest(&RemoteChildExecuteRequest{
		Exec: &ExecuteRequest{
			Command:     []string{"hostname"},
			RemoteChild: &RemoteChildLaunch{ParentJobId: "nested"},
		},
		Launch: &RemoteChildLaunch{ParentJobId: "job-parent"},
	})
	if err == nil || !strings.Contains(err.Error(), "dedicated request envelope") {
		t.Fatalf("nested metadata error = %v, want dedicated envelope rejection", err)
	}
}

func TestRemoteChildStreamRequestToExecStreamRequest(t *testing.T) {
	msg, err := remoteChildStreamRequestToExecStreamRequest(&RemoteChildStreamRequest{
		Payload: &RemoteChildStreamRequest_Exec{Exec: &RemoteChildExecuteRequest{
			Exec:   &ExecuteRequest{Command: []string{"hostname"}},
			Launch: &RemoteChildLaunch{ParentJobId: "job-parent", ChildToken: "parent-token"},
		}},
	})
	if err != nil {
		t.Fatalf("remoteChildStreamRequestToExecStreamRequest: %v", err)
	}
	execReq := msg.GetExec()
	if execReq == nil || execReq.RemoteChild == nil || execReq.RemoteChild.ParentJobId != "job-parent" {
		t.Fatalf("converted exec request = %#v", execReq)
	}

	signal := &SignalRequest{Signal: int32(syscall.SIGTERM)}
	msg, err = remoteChildStreamRequestToExecStreamRequest(&RemoteChildStreamRequest{
		Payload: &RemoteChildStreamRequest_Signal{Signal: signal},
	})
	if err != nil {
		t.Fatalf("signal conversion: %v", err)
	}
	if msg.GetSignal() == nil || msg.GetSignal().Signal != int32(syscall.SIGTERM) {
		t.Fatalf("converted signal = %#v", msg.GetSignal())
	}
}

func TestScheduleJobAppliesSelectedNodeEnv(t *testing.T) {
	root := t.TempDir()
	server := NewClusterServerImplWithOptions(
		cluster.NodeID("ingress-node"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
		ServerOptions{SSI: ssi.Config{
			ClusterRoot:  root,
			RootFS:       root,
			RequireMount: false,
			Required:     true,
		}},
	)
	for _, nodeID := range []string{"ingress-node", "worker-node"} {
		resp, err := server.RegisterNode(context.Background(), &RegisterNodeRequest{
			NodeId:  nodeID,
			Address: "10.0.0.1",
			Resources: &NodeResources{
				CpuMillicores: 2000,
				MemoryBytes:   2 * 1024 * 1024 * 1024,
			},
		})
		if err != nil {
			t.Fatalf("RegisterNode(%s): %v", nodeID, err)
		}
		if !resp.Success {
			t.Fatalf("RegisterNode(%s) failed: %s", nodeID, resp.Error)
		}
	}

	req := &ExecuteRequest{
		SessionId: "session-1",
		JobId:     "job-1",
		Command:   []string{"env"},
		Resources: &Requirements{
			CpuMillicores: 1000,
			MemoryBytes:   1024 * 1024 * 1024,
			NodeAffinity:  map[string]string{"node_id": "worker-node"},
		},
	}
	node, _, jobID, err := server.scheduleJob(req)
	if err != nil {
		t.Fatalf("scheduleJob: %v", err)
	}
	if node.ID != "worker-node" {
		t.Fatalf("scheduled node = %q, want worker-node", node.ID)
	}
	job := server.cluster.jobs[jobID]
	if got, want := envValue(job.Commands[0].Env, "OCTOPOS_NODE_ID"), "worker-node"; got != want {
		t.Fatalf("job command OCTOPOS_NODE_ID = %q, want %q; env=%v", got, want, job.Commands[0].Env)
	}
	if got := envValue(job.Commands[0].Env, remotechild.EnvChildToken); got == "" {
		t.Fatalf("%s missing from scheduled env: %v", remotechild.EnvChildToken, job.Commands[0].Env)
	} else if got != job.ChildToken {
		t.Fatalf("scheduled env token does not match job token")
	}
}

func TestAuthorizeRemoteChildRequestRequiresParentToken(t *testing.T) {
	server := NewClusterServerImpl(
		cluster.NodeID("node-1"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
	)
	parentJob := &cluster.JobInfo{
		ID:         "job-parent",
		SessionID:  "session-1",
		Status:     cluster.JobStatusRunning,
		ChildToken: "parent-token",
	}
	server.cluster.jobs[parentJob.ID] = parentJob

	validReq := &ExecuteRequest{
		SessionId: "session-1",
		Command:   []string{"true"},
		Env: []string{
			remotechild.EnvRemoteChild + "=1",
			remotechild.EnvParentJobID + "=job-parent",
			remotechild.EnvChildToken + "=parent-token",
		},
	}
	if err := server.authorizeRemoteChildRequest(context.Background(), validReq); err != nil {
		t.Fatalf("valid remote child rejected: %v", err)
	}

	typedReq := &ExecuteRequest{
		SessionId: "session-1",
		Command:   []string{"true"},
		RemoteChild: &RemoteChildLaunch{
			ParentJobId: "job-parent",
			ParentPid:   100,
			ShadowPid:   101,
			ChildToken:  "parent-token",
		},
		Env: []string{"USER_VALUE=preserved"},
	}
	if err := server.authorizeRemoteChildRequest(context.Background(), typedReq); err != nil {
		t.Fatalf("typed remote child rejected: %v", err)
	}

	missingToken := cloneExecuteRequestForTest(validReq)
	missingToken.Env = []string{
		remotechild.EnvRemoteChild + "=1",
		remotechild.EnvParentJobID + "=job-parent",
	}
	if err := server.authorizeRemoteChildRequest(context.Background(), missingToken); err == nil {
		t.Fatal("missing token accepted")
	}

	wrongToken := cloneExecuteRequestForTest(validReq)
	wrongToken.Env = []string{
		remotechild.EnvRemoteChild + "=1",
		remotechild.EnvParentJobID + "=job-parent",
		remotechild.EnvChildToken + "=wrong",
	}
	if err := server.authorizeRemoteChildRequest(context.Background(), wrongToken); err == nil {
		t.Fatal("wrong token accepted")
	}

	wrongSession := cloneExecuteRequestForTest(validReq)
	wrongSession.SessionId = "other-session"
	if err := server.authorizeRemoteChildRequest(context.Background(), wrongSession); err == nil {
		t.Fatal("wrong session accepted")
	}

	internalCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(internalForwardMetadata, "1"))
	if err := server.authorizeRemoteChildRequest(internalCtx, wrongToken); err != nil {
		t.Fatalf("internal forwarded request rejected: %v", err)
	}

	events := server.remoteChildren.AuditEvents()
	if len(events) < 5 {
		t.Fatalf("audit events = %d, want at least 5", len(events))
	}
	if events[0].Decision != "accepted" || events[0].ParentJobID != "job-parent" {
		t.Fatalf("first audit event = %#v", events[0])
	}
	last := events[len(events)-1]
	if last.Decision != "rejected" || !strings.Contains(last.AuthFailureReason, "session") {
		t.Fatalf("last audit event = %#v, want session rejection", last)
	}
}

func TestAuthorizeRemoteChildRequestRequiresRunningParentAndLimit(t *testing.T) {
	server := NewClusterServerImplWithOptions(
		cluster.NodeID("node-1"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
		ServerOptions{MaxRemoteChildrenPerParent: 1},
	)
	parentJob := &cluster.JobInfo{
		ID:         "job-parent",
		SessionID:  "session-1",
		Status:     cluster.JobStatusRunning,
		ChildToken: "parent-token",
	}
	server.cluster.jobs[parentJob.ID] = parentJob

	req := &ExecuteRequest{
		SessionId: "session-1",
		JobId:     "job-new-child",
		Command:   []string{"true"},
		Env: []string{
			remotechild.EnvRemoteChild + "=1",
			remotechild.EnvParentJobID + "=job-parent",
			remotechild.EnvChildToken + "=parent-token",
		},
	}
	server.remoteChildren.Upsert(remotechild.ShadowRecord{
		SessionID:   "session-1",
		ParentJobID: "job-parent",
		RemoteJobID: "job-existing-child",
		State:       remotechild.StateRunning,
	})
	if err := server.authorizeRemoteChildRequest(context.Background(), req); err == nil {
		t.Fatal("request accepted despite remote child limit")
	}

	server.remoteChildren.MarkFinished("job-existing-child", remotechild.StateCompleted, 0, 0, "", time.Now())
	if err := server.authorizeRemoteChildRequest(context.Background(), req); err != nil {
		t.Fatalf("request rejected after completed child no longer counts: %v", err)
	}

	parentJob.Status = cluster.JobStatusCompleted
	if err := server.authorizeRemoteChildRequest(context.Background(), req); err == nil {
		t.Fatal("request accepted after parent completed")
	}
}

func TestAuthorizeRemoteChildRequestRejectsExpiredToken(t *testing.T) {
	server := NewClusterServerImpl(
		cluster.NodeID("node-1"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
	)
	parentJob := &cluster.JobInfo{
		ID:                  "job-parent",
		SessionID:           "session-1",
		Status:              cluster.JobStatusRunning,
		ChildToken:          "parent-token",
		ChildTokenExpiresAt: time.Now().Add(-time.Second),
	}
	server.cluster.jobs[parentJob.ID] = parentJob

	req := &ExecuteRequest{
		SessionId: "session-1",
		Command:   []string{"true"},
		Env: []string{
			remotechild.EnvRemoteChild + "=1",
			remotechild.EnvParentJobID + "=job-parent",
			remotechild.EnvChildToken + "=parent-token",
		},
	}
	if err := server.authorizeRemoteChildRequest(context.Background(), req); err == nil {
		t.Fatal("expired token accepted")
	}
	events := server.remoteChildren.AuditEvents()
	if len(events) != 1 || !strings.Contains(events[0].AuthFailureReason, "expired") {
		t.Fatalf("audit events = %#v, want expired rejection", events)
	}
}

func TestRemoteChildLifecycleStateTransitions(t *testing.T) {
	server := NewClusterServerImpl(
		cluster.NodeID("node-1"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
	)
	resp, err := server.RegisterNode(context.Background(), &RegisterNodeRequest{
		NodeId:  "node-1",
		Address: "10.0.0.1",
		Resources: &NodeResources{
			CpuMillicores: 2000,
			MemoryBytes:   2 * 1024 * 1024 * 1024,
		},
	})
	if err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if !resp.Success {
		t.Fatalf("RegisterNode failed: %s", resp.Error)
	}

	req := &ExecuteRequest{
		SessionId: "session-1",
		JobId:     "job-child",
		Command:   []string{"true"},
		Env: []string{
			remotechild.EnvRemoteChild + "=1",
			remotechild.EnvParentJobID + "=job-parent",
			remotechild.EnvParentPID + "=100",
			remotechild.EnvShadowPID + "=101",
		},
		Resources: &Requirements{
			CpuMillicores: 1000,
			MemoryBytes:   1024 * 1024 * 1024,
		},
	}
	node, reqs, jobID, err := server.scheduleJob(req)
	if err != nil {
		t.Fatalf("scheduleJob: %v", err)
	}
	job := server.cluster.jobs[jobID]
	if job.RemoteChild == nil {
		t.Fatal("remote child metadata missing")
	}
	if job.RemoteChild.State != remotechild.StateScheduled {
		t.Fatalf("state = %q, want %q", job.RemoteChild.State, remotechild.StateScheduled)
	}
	record, ok := server.remoteChildren.Get(string(jobID))
	if !ok {
		t.Fatal("remote child lifecycle record missing")
	}
	if record.State != remotechild.StateScheduled {
		t.Fatalf("record state = %q, want %q", record.State, remotechild.StateScheduled)
	}

	control := processControlInfo{ProcessGroupID: 4200, KernelSessionID: 4300, ForegroundProcessGroupID: 4200}
	server.recordRemoteChildWorker(jobID, cluster.GlobalPID(42), control)
	if got := job.RemoteChild.State; got != remotechild.StateRunning {
		t.Fatalf("state after worker = %q, want %q", got, remotechild.StateRunning)
	}
	if job.RemoteChild.RemoteGlobalPID != 42 {
		t.Fatalf("remote global pid = %d", job.RemoteChild.RemoteGlobalPID)
	}
	if job.RemoteChild.StartedAt.IsZero() {
		t.Fatal("started_at not set after worker registration")
	}
	if job.ProcessGroupID != control.ProcessGroupID || job.KernelSessionID != control.KernelSessionID || job.ForegroundProcessGroupID != control.ForegroundProcessGroupID {
		t.Fatalf("job control metadata = pgid:%d sid:%d fg:%d", job.ProcessGroupID, job.KernelSessionID, job.ForegroundProcessGroupID)
	}
	record, _ = server.remoteChildren.Get(string(jobID))
	if record.State != remotechild.StateRunning || record.RemoteGlobalPID != 42 {
		t.Fatalf("record after worker = %#v", record)
	}
	if record.ProcessGroupID != control.ProcessGroupID || record.KernelSessionID != control.KernelSessionID || record.ForegroundProcessGroupID != control.ForegroundProcessGroupID {
		t.Fatalf("record control metadata = %#v", record)
	}

	server.finishJob(jobID, node.ID, reqs, 0)
	if got := job.RemoteChild.State; got != remotechild.StateCompleted {
		t.Fatalf("state after finish = %q, want %q", got, remotechild.StateCompleted)
	}
	if job.RemoteChild.FinishedAt.IsZero() {
		t.Fatal("finished_at not set after completion")
	}
	record, _ = server.remoteChildren.Get(string(jobID))
	if record.State != remotechild.StateCompleted || record.ExitCode != 0 {
		t.Fatalf("record after finish = %#v", record)
	}
}

func TestRemoteChildPipePlacementCoLocatesEndpoints(t *testing.T) {
	server := NewClusterServerImpl(
		cluster.NodeID("node-1"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
	)
	for _, nodeID := range []string{"node-1", "node-2"} {
		if _, err := server.RegisterNode(context.Background(), &RegisterNodeRequest{
			NodeId:  nodeID,
			Address: "10.0.0." + nodeID[len(nodeID)-1:],
			Resources: &NodeResources{
				CpuMillicores: 4000,
				MemoryBytes:   4 * 1024 * 1024 * 1024,
			},
		}); err != nil {
			t.Fatalf("RegisterNode %s: %v", nodeID, err)
		}
	}
	parentJob := &cluster.JobInfo{
		ID:         "job-parent",
		SessionID:  "session-1",
		Status:     cluster.JobStatusRunning,
		ChildToken: "parent-token",
	}
	server.cluster.jobs[parentJob.ID] = parentJob

	first := &ExecuteRequest{
		SessionId: "session-1",
		JobId:     "job-producer",
		Command:   []string{"producer"},
		Env: []string{
			remotechild.EnvRemoteChild + "=1",
			remotechild.EnvParentJobID + "=job-parent",
			remotechild.EnvChildToken + "=parent-token",
			remotechild.EnvPipeFD(1) + "=pipe-123",
		},
		Resources: &Requirements{
			CpuMillicores: 1000,
			MemoryBytes:   1024 * 1024 * 1024,
		},
	}
	firstNode, firstReqs, firstJobID, err := server.scheduleJob(first)
	if err != nil {
		t.Fatalf("schedule first endpoint: %v", err)
	}
	defer server.scheduler.Release(firstNode.ID, firstReqs)
	defer delete(server.cluster.jobs, firstJobID)

	second := &ExecuteRequest{
		SessionId: "session-1",
		JobId:     "job-consumer",
		Command:   []string{"consumer"},
		Env: []string{
			remotechild.EnvRemoteChild + "=1",
			remotechild.EnvParentJobID + "=job-parent",
			remotechild.EnvChildToken + "=parent-token",
			remotechild.EnvPipeFD(0) + "=pipe-123",
		},
		Resources: &Requirements{
			CpuMillicores: 1000,
			MemoryBytes:   1024 * 1024 * 1024,
			NodeAffinity:  map[string]string{"prefer_not_node_id": string(firstNode.ID)},
		},
	}
	secondNode, secondReqs, secondJobID, err := server.scheduleJob(second)
	if err != nil {
		t.Fatalf("schedule second endpoint: %v", err)
	}
	defer server.scheduler.Release(secondNode.ID, secondReqs)
	defer delete(server.cluster.jobs, secondJobID)

	if secondNode.ID != firstNode.ID {
		t.Fatalf("second endpoint scheduled on %s, want %s", secondNode.ID, firstNode.ID)
	}
	if got := second.Resources.NodeAffinity["node_id"]; got != string(firstNode.ID) {
		t.Fatalf("node affinity after pipe placement = %#v", second.Resources.NodeAffinity)
	}
	if _, ok := second.Resources.NodeAffinity["prefer_not_node_id"]; ok {
		t.Fatalf("prefer_not_node_id was not removed: %#v", second.Resources.NodeAffinity)
	}
}

func TestRemoteChildSignalStateTransitions(t *testing.T) {
	server := NewClusterServerImpl(
		cluster.NodeID("node-1"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
	)
	jobID := cluster.JobID("job-child")
	server.cluster.jobs[jobID] = &cluster.JobInfo{
		ID:        jobID,
		SessionID: "session-1",
		Status:    cluster.JobStatusRunning,
		RemoteChild: &cluster.RemoteChildInfo{
			RemoteJobID: jobID,
			State:       remotechild.StateRunning,
		},
	}
	server.remoteChildren.Upsert(remotechild.ShadowRecord{
		SessionID:   "session-1",
		RemoteJobID: string(jobID),
		State:       remotechild.StateRunning,
	})

	server.markJobSignalState(jobID, syscall.SIGTSTP)
	job := server.cluster.jobs[jobID]
	if job.Status != cluster.JobStatusStopped {
		t.Fatalf("job status = %s, want stopped", job.Status)
	}
	if job.RemoteChild.State != remotechild.StateStopped {
		t.Fatalf("remote child state = %s, want stopped", job.RemoteChild.State)
	}
	record, _ := server.remoteChildren.Get(string(jobID))
	if record.State != remotechild.StateStopped || record.StateReason == "" || record.FailureReason != "" {
		t.Fatalf("record after stop = %#v", record)
	}
	if job.RemoteChild.StateReason == "" || job.RemoteChild.FailureReason != "" {
		t.Fatalf("remote child stop reason = %#v", job.RemoteChild)
	}

	server.markJobSignalState(jobID, syscall.SIGCONT)
	if job.Status != cluster.JobStatusRunning {
		t.Fatalf("job status after cont = %s, want running", job.Status)
	}
	if job.RemoteChild.State != remotechild.StateRunning || job.RemoteChild.StateReason == "" || job.RemoteChild.FailureReason != "" {
		t.Fatalf("remote child after cont = %#v", job.RemoteChild)
	}
	record, _ = server.remoteChildren.Get(string(jobID))
	if record.State != remotechild.StateRunning || record.StateReason == "" || record.FailureReason != "" {
		t.Fatalf("record after cont = %#v", record)
	}
}

func TestParentExitMarksLocalRemoteChildJobTerminal(t *testing.T) {
	server := NewClusterServerImpl(
		cluster.NodeID("node-1"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
	)
	parentID := cluster.JobID("job-parent")
	childID := cluster.JobID("job-child")
	server.cluster.jobs[parentID] = &cluster.JobInfo{
		ID:        parentID,
		SessionID: "session-1",
		Status:    cluster.JobStatusRunning,
	}
	server.cluster.jobs[childID] = &cluster.JobInfo{
		ID:          childID,
		SessionID:   "session-1",
		Status:      cluster.JobStatusRunning,
		PrimaryNode: "node-1",
		RemoteChild: &cluster.RemoteChildInfo{
			ParentJobID:  parentID,
			RemoteJobID:  childID,
			RemoteNodeID: "node-1",
			State:        remotechild.StateRunning,
		},
		ChildToken:          "child-token",
		ChildTokenExpiresAt: time.Now().Add(time.Minute),
	}
	server.remoteChildren.Upsert(remotechild.ShadowRecord{
		SessionID:    "session-1",
		ParentJobID:  string(parentID),
		RemoteJobID:  string(childID),
		RemoteNodeID: "node-1",
		State:        remotechild.StateRunning,
	})

	server.failJob(parentID, "", cluster.Requirements{}, "parent failed")

	child := server.cluster.jobs[childID]
	if child.Status != cluster.JobStatusFailed {
		t.Fatalf("child status = %s, want failed", child.Status)
	}
	if child.ChildToken != "" || !child.ChildTokenExpiresAt.IsZero() {
		t.Fatalf("child token not cleared: token=%q expiry=%s", child.ChildToken, child.ChildTokenExpiresAt)
	}
	if child.RemoteChild == nil || child.RemoteChild.State != remotechild.StateFailed || child.RemoteChild.FailureReason != "parent failed" {
		t.Fatalf("child remote state = %#v", child.RemoteChild)
	}
	if child.FinishedAt.IsZero() || child.RemoteChild.FinishedAt.IsZero() {
		t.Fatalf("child finish time missing: job=%s remote=%s", child.FinishedAt, child.RemoteChild.FinishedAt)
	}
	record, _ := server.remoteChildren.Get(string(childID))
	if record.State != remotechild.StateFailed || record.FailureReason != "parent failed" {
		t.Fatalf("record after parent failure = %#v", record)
	}
}

func TestCommandContextForRemoteChildSurvivesStreamCancel(t *testing.T) {
	parentCtx, cancel := context.WithCancel(context.Background())
	workerCtx := commandContextForStream(parentCtx, true)
	cancel()

	select {
	case <-workerCtx.Done():
		t.Fatal("remote-child worker context was canceled with stream context")
	default:
	}
}

func TestCommandContextForNormalExecFollowsStreamCancel(t *testing.T) {
	parentCtx, cancel := context.WithCancel(context.Background())
	workerCtx := commandContextForStream(parentCtx, false)
	cancel()

	select {
	case <-workerCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("normal exec worker context did not follow stream cancellation")
	}
}

func TestWaitExitCodeReturnsShellStatusForSignal(t *testing.T) {
	cmd := exec.Command("/bin/sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	if err := cmd.Process.Signal(syscall.SIGINT); err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("signal sleep: %v", err)
	}
	if got, want := waitExitCode(cmd.Wait()), int32(128+syscall.SIGINT); got != want {
		t.Fatalf("waitExitCode = %d, want %d", got, want)
	}
}

func TestRecoverRemoteChildRecordFromRunningJob(t *testing.T) {
	server := NewClusterServerImpl(
		cluster.NodeID("node-1"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
	)
	now := time.Unix(200, 0)
	record := remotechild.ShadowRecord{
		SessionID:    "sess-1",
		ParentJobID:  "job-parent",
		RemoteJobID:  "job-child",
		RemoteNodeID: "node-2",
		State:        remotechild.StateRecovering,
		UpdatedAt:    now,
	}
	server.remoteChildren.Upsert(record)

	outcome := server.applyRecoveredRemoteChildJob(record, &JobInfo{
		JobId:     "job-child",
		SessionId: "sess-1",
		Status:    JobStatus_JOB_STATUS_RUNNING,
		StartedAt: now.Unix(),
		RemoteChild: &RemoteChildInfo{
			ParentJobId:     "job-parent",
			RemoteJobId:     "job-child",
			RemoteNodeId:    "node-2",
			RemoteGlobalPid: 42,
			Command:         []string{"/bin/sleep", "100"},
			State:           remotechild.StateRunning,
		},
	}, now)
	if outcome != remoteChildRecoveryRunning {
		t.Fatalf("outcome = %v, want running", outcome)
	}
	recovered, ok := server.remoteChildren.Get("job-child")
	if !ok {
		t.Fatal("record missing after recovery")
	}
	if recovered.State != remotechild.StateRunning || recovered.RemoteGlobalPID != 42 || recovered.FailureReason != "" {
		t.Fatalf("recovered record = %#v", recovered)
	}
	if len(recovered.Command) != 2 || recovered.Command[0] != "/bin/sleep" {
		t.Fatalf("recovered command = %#v", recovered.Command)
	}
}

func TestRecoverRemoteChildRecordFromStoppedJob(t *testing.T) {
	server := NewClusterServerImpl(
		cluster.NodeID("node-1"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
	)
	now := time.Unix(200, 0)
	record := remotechild.ShadowRecord{
		SessionID:    "sess-1",
		ParentJobID:  "job-parent",
		RemoteJobID:  "job-child",
		RemoteNodeID: "node-2",
		State:        remotechild.StateRecovering,
		UpdatedAt:    now,
	}
	server.remoteChildren.Upsert(record)

	outcome := server.applyRecoveredRemoteChildJob(record, &JobInfo{
		JobId:     "job-child",
		SessionId: "sess-1",
		Status:    JobStatus_JOB_STATUS_STOPPED,
		RemoteChild: &RemoteChildInfo{
			ParentJobId:     "job-parent",
			RemoteJobId:     "job-child",
			RemoteNodeId:    "node-2",
			RemoteGlobalPid: 42,
			State:           remotechild.StateStopped,
			StateReason:     "stopped by signal 20",
		},
	}, now)
	if outcome != remoteChildRecoveryStopped {
		t.Fatalf("outcome = %v, want stopped", outcome)
	}
	recovered, _ := server.remoteChildren.Get("job-child")
	if recovered.State != remotechild.StateStopped || recovered.StateReason != "stopped by signal 20" || recovered.FailureReason != "" {
		t.Fatalf("recovered stopped record = %#v", recovered)
	}
	job, ok := server.cluster.jobs["job-child"]
	if !ok || job.Status != cluster.JobStatusStopped {
		t.Fatalf("recovered job = %#v, ok=%v", job, ok)
	}
}

func TestRecoverRemoteChildRecordFromCompletedJob(t *testing.T) {
	server := NewClusterServerImpl(
		cluster.NodeID("node-1"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
	)
	now := time.Unix(200, 0)
	record := remotechild.ShadowRecord{
		SessionID:    "sess-1",
		ParentJobID:  "job-parent",
		RemoteJobID:  "job-child",
		RemoteNodeID: "node-2",
		State:        remotechild.StateRecovering,
		UpdatedAt:    now,
	}
	server.remoteChildren.Upsert(record)

	outcome := server.applyRecoveredRemoteChildJob(record, &JobInfo{
		JobId:      "job-child",
		SessionId:  "sess-1",
		Status:     JobStatus_JOB_STATUS_COMPLETED,
		FinishedAt: now.Unix(),
		ExitCodes:  []int32{7},
		RemoteChild: &RemoteChildInfo{
			ParentJobId:  "job-parent",
			RemoteJobId:  "job-child",
			RemoteNodeId: "node-2",
			State:        remotechild.StateCompleted,
		},
	}, now)
	if outcome != remoteChildRecoveryCompleted {
		t.Fatalf("outcome = %v, want completed", outcome)
	}
	recovered, _ := server.remoteChildren.Get("job-child")
	if recovered.State != remotechild.StateCompleted || recovered.ExitCode != 7 || recovered.FinishedAt.IsZero() {
		t.Fatalf("recovered completed record = %#v", recovered)
	}
	job := server.cluster.jobs["job-child"]
	if job.RemoteChild == nil || job.RemoteChild.State != remotechild.StateCompleted || job.RemoteChild.FinishedAt.IsZero() {
		t.Fatalf("recovered job remote child = %#v", job.RemoteChild)
	}
}

func TestRecoverRemoteChildRecordExpiresUnobservableChild(t *testing.T) {
	server := NewClusterServerImplWithOptions(
		cluster.NodeID("node-1"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
		ServerOptions{RemoteChildLeaseTimeout: time.Second},
	)
	now := time.Unix(200, 0)
	record := remotechild.ShadowRecord{
		SessionID:    "sess-1",
		ParentJobID:  "job-parent",
		RemoteJobID:  "job-child",
		RemoteNodeID: "node-2",
		State:        remotechild.StateRecovering,
		UpdatedAt:    now.Add(-2 * time.Second),
	}
	server.remoteChildren.Upsert(record)

	outcome := server.recoverRemoteChildRecord(context.Background(), record, nil, now)
	if outcome != remoteChildRecoveryOrphaned {
		t.Fatalf("outcome = %v, want orphaned", outcome)
	}
	recovered, _ := server.remoteChildren.Get("job-child")
	if recovered.State != remotechild.StateOrphaned || recovered.FailureReason == "" {
		t.Fatalf("orphaned record = %#v", recovered)
	}
}

func TestRecoverLocalRemoteChildRecordReattachesLivePID(t *testing.T) {
	server := NewClusterServerImpl(
		cluster.NodeID("node-1"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
	)
	now := time.Unix(200, 0)
	record := remotechild.ShadowRecord{
		SessionID:       "sess-1",
		ParentJobID:     "job-parent",
		ParentPID:       os.Getppid(),
		RemoteJobID:     "job-child",
		RemoteNodeID:    "node-1",
		RemoteGlobalPID: 42,
		RemoteLocalPID:  os.Getpid(),
		Command:         []string{"/bin/sleep", "75"},
		State:           remotechild.StateRecovering,
		UpdatedAt:       now,
	}
	server.remoteChildren.Upsert(record)

	outcome := server.recoverLocalRemoteChildRecord(record, now)
	if outcome != remoteChildRecoveryRunning {
		t.Fatalf("outcome = %v, want running", outcome)
	}
	recovered, _ := server.remoteChildren.Get("job-child")
	if recovered.State != remotechild.StateRunning || recovered.RemoteLocalPID != os.Getpid() || recovered.RemoteGlobalPID != 42 {
		t.Fatalf("recovered local record = %#v", recovered)
	}
	proc, ok := server.tracker.Get(42)
	if !ok || proc.LocalPID != os.Getpid() || proc.JobID != "job-child" {
		t.Fatalf("tracker proc = %#v, ok=%v", proc, ok)
	}
}

func TestRecoverLocalRemoteChildRecordOrphansMissingPID(t *testing.T) {
	server := NewClusterServerImplWithOptions(
		cluster.NodeID("node-1"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
		ServerOptions{RemoteChildLeaseTimeout: time.Second},
	)
	now := time.Unix(200, 0)
	record := remotechild.ShadowRecord{
		SessionID:       "sess-1",
		ParentJobID:     "job-parent",
		RemoteJobID:     "job-child",
		RemoteNodeID:    "node-1",
		RemoteGlobalPID: 42,
		RemoteLocalPID:  99999999,
		Command:         []string{"/bin/sleep", "75"},
		State:           remotechild.StateRecovering,
		UpdatedAt:       now,
	}
	server.remoteChildren.Upsert(record)

	outcome := server.recoverLocalRemoteChildRecord(record, now)
	if outcome != remoteChildRecoveryOrphaned {
		t.Fatalf("outcome = %v, want orphaned", outcome)
	}
	recovered, _ := server.remoteChildren.Get("job-child")
	if recovered.State != remotechild.StateOrphaned || recovered.FailureReason == "" {
		t.Fatalf("orphaned local record = %#v", recovered)
	}
	job := server.cluster.jobs["job-child"]
	if job.RemoteChild == nil || job.RemoteChild.State != remotechild.StateOrphaned || job.RemoteChild.FailureReason == "" {
		t.Fatalf("orphaned local job remote child = %#v", job.RemoteChild)
	}
}

func TestRecoverLocalRemoteChildRecordUsesExitStatusForMissingPID(t *testing.T) {
	root := t.TempDir()
	exitStatusDir := t.TempDir()
	server := NewClusterServerImplWithOptions(
		cluster.NodeID("node-1"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
		ServerOptions{SSI: ssi.Config{
			ClusterRoot:  root,
			RootFS:       root,
			RequireMount: false,
			Required:     true,
		}, RemoteChildExitStatusDir: exitStatusDir},
	)
	now := time.Unix(200, 0)
	record := remotechild.ShadowRecord{
		SessionID:       "sess-1",
		ParentJobID:     "job-parent",
		RemoteJobID:     "job-child",
		RemoteNodeID:    "node-1",
		RemoteGlobalPID: 42,
		Command:         []string{"/bin/sh", "-c", "exit 7"},
		State:           remotechild.StateRecovering,
		UpdatedAt:       now,
	}
	server.remoteChildren.Upsert(record)
	statusPath := remotechild.WorkerExitStatusPathInDir(exitStatusDir, record.RemoteJobID)
	if err := remotechild.WriteWorkerExitStatus(statusPath, remotechild.WorkerExitStatus{
		JobID:    record.RemoteJobID,
		PID:      99999999,
		ExitCode: 7,
		ExitedAt: now.Add(time.Second),
	}); err != nil {
		t.Fatalf("WriteWorkerExitStatus: %v", err)
	}

	outcome := server.recoverLocalRemoteChildRecord(record, now)
	if outcome != remoteChildRecoveryCompleted {
		t.Fatalf("outcome = %v, want completed", outcome)
	}
	recovered, _ := server.remoteChildren.Get("job-child")
	if recovered.State != remotechild.StateCompleted || recovered.ExitCode != 7 || recovered.FailureReason != "" {
		t.Fatalf("recovered local record = %#v", recovered)
	}
	job := server.cluster.jobs["job-child"]
	if job == nil || job.Status != cluster.JobStatusCompleted || len(job.ExitCodes) != 1 || job.ExitCodes[0] != 7 {
		t.Fatalf("recovered job = %#v", job)
	}
}

func TestRecoverLocalRemoteChildRecordUsesSignalExitStatus(t *testing.T) {
	root := t.TempDir()
	exitStatusDir := t.TempDir()
	server := NewClusterServerImplWithOptions(
		cluster.NodeID("node-1"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
		ServerOptions{SSI: ssi.Config{
			ClusterRoot:  root,
			RootFS:       root,
			RequireMount: false,
			Required:     true,
		}, RemoteChildExitStatusDir: exitStatusDir},
	)
	now := time.Unix(200, 0)
	record := remotechild.ShadowRecord{
		SessionID:       "sess-1",
		ParentJobID:     "job-parent",
		RemoteJobID:     "job-child",
		RemoteNodeID:    "node-1",
		RemoteGlobalPID: 42,
		Command:         []string{"/bin/sleep", "75"},
		State:           remotechild.StateRecovering,
		UpdatedAt:       now,
	}
	server.remoteChildren.Upsert(record)
	statusPath := remotechild.WorkerExitStatusPathInDir(exitStatusDir, record.RemoteJobID)
	if err := remotechild.WriteWorkerExitStatus(statusPath, remotechild.WorkerExitStatus{
		JobID:    record.RemoteJobID,
		PID:      99999999,
		ExitCode: -1,
		Signal:   int(syscall.SIGTERM),
		ExitedAt: now.Add(time.Second),
	}); err != nil {
		t.Fatalf("WriteWorkerExitStatus: %v", err)
	}

	outcome := server.recoverLocalRemoteChildRecord(record, now)
	if outcome != remoteChildRecoveryCompleted {
		t.Fatalf("outcome = %v, want completed", outcome)
	}
	recovered, _ := server.remoteChildren.Get("job-child")
	if recovered.State != remotechild.StateCompleted || recovered.ExitCode != 128+int(syscall.SIGTERM) || recovered.Signal != int(syscall.SIGTERM) {
		t.Fatalf("recovered local record = %#v", recovered)
	}
	job := server.cluster.jobs["job-child"]
	if job == nil || job.Status != cluster.JobStatusCompleted || len(job.ExitCodes) != 1 || job.ExitCodes[0] != int(128+syscall.SIGTERM) {
		t.Fatalf("recovered job = %#v", job)
	}
}

func TestScheduleJobSetsChildTokenExpiry(t *testing.T) {
	server := NewClusterServerImplWithOptions(
		cluster.NodeID("node-1"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
		ServerOptions{RemoteChildTokenTTL: time.Minute},
	)
	if _, err := server.RegisterNode(context.Background(), &RegisterNodeRequest{
		NodeId:  "node-1",
		Address: "10.0.0.1",
		Resources: &NodeResources{
			CpuMillicores: 2000,
			MemoryBytes:   2 * 1024 * 1024 * 1024,
		},
	}); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	req := &ExecuteRequest{
		SessionId: "session-1",
		JobId:     "job-parent",
		Command:   []string{"true"},
		Resources: &Requirements{
			CpuMillicores: 1000,
			MemoryBytes:   1024 * 1024 * 1024,
		},
	}
	_, _, jobID, err := server.scheduleJob(req)
	if err != nil {
		t.Fatalf("scheduleJob: %v", err)
	}
	job := server.cluster.jobs[jobID]
	if job.ChildToken == "" {
		t.Fatal("ChildToken missing")
	}
	if job.ChildTokenExpiresAt.IsZero() {
		t.Fatal("ChildTokenExpiresAt missing")
	}
	if time.Until(job.ChildTokenExpiresAt) <= 0 || time.Until(job.ChildTokenExpiresAt) > time.Minute {
		t.Fatalf("ChildTokenExpiresAt = %s, want within configured TTL", job.ChildTokenExpiresAt)
	}
}

func TestScheduleJobRotatesChildTokenOnJobIDReuse(t *testing.T) {
	root := t.TempDir()
	server := NewClusterServerImplWithOptions(
		cluster.NodeID("node-1"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
		ServerOptions{
			RemoteChildTokenTTL: time.Minute,
			SSI: ssi.Config{
				ClusterRoot:  root,
				RootFS:       root,
				RequireMount: false,
				Required:     true,
			},
		},
	)
	if _, err := server.RegisterNode(context.Background(), &RegisterNodeRequest{
		NodeId:  "node-1",
		Address: "10.0.0.1",
		Resources: &NodeResources{
			CpuMillicores: 4000,
			MemoryBytes:   4 * 1024 * 1024 * 1024,
		},
	}); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	req1 := &ExecuteRequest{
		SessionId: "session-1",
		JobId:     "job-parent",
		Command:   []string{"true"},
		Resources: &Requirements{
			CpuMillicores: 1000,
			MemoryBytes:   1024 * 1024 * 1024,
		},
	}
	if _, _, _, err := server.scheduleJob(req1); err != nil {
		t.Fatalf("schedule first job: %v", err)
	}
	firstToken := server.cluster.jobs["job-parent"].ChildToken
	if firstToken == "" {
		t.Fatal("first token missing")
	}

	req2 := &ExecuteRequest{
		SessionId: "session-1",
		JobId:     "job-parent",
		Command:   []string{"true"},
		Resources: &Requirements{
			CpuMillicores: 1000,
			MemoryBytes:   1024 * 1024 * 1024,
		},
	}
	if _, _, _, err := server.scheduleJob(req2); err != nil {
		t.Fatalf("schedule restarted job: %v", err)
	}
	restarted := server.cluster.jobs["job-parent"]
	if restarted.ChildToken == "" || restarted.ChildToken == firstToken {
		t.Fatalf("token was not rotated: first=%q restarted=%q", firstToken, restarted.ChildToken)
	}
	if got := requestEnvValue(req2.Env, remotechild.EnvChildToken); got != restarted.ChildToken {
		t.Fatalf("scheduled env token = %q, want current token %q", got, restarted.ChildToken)
	}

	oldTokenReq := &ExecuteRequest{
		SessionId: "session-1",
		Command:   []string{"true"},
		RemoteChild: &RemoteChildLaunch{
			ParentJobId: "job-parent",
			ChildToken:  firstToken,
		},
	}
	if err := server.authorizeRemoteChildRequest(context.Background(), oldTokenReq); err == nil {
		t.Fatal("old child token accepted after parent job reschedule")
	}

	newTokenReq := &ExecuteRequest{
		SessionId: "session-1",
		Command:   []string{"true"},
		RemoteChild: &RemoteChildLaunch{
			ParentJobId: "job-parent",
			ChildToken:  restarted.ChildToken,
		},
	}
	if err := server.authorizeRemoteChildRequest(context.Background(), newTokenReq); err != nil {
		t.Fatalf("current child token rejected: %v", err)
	}
}

func TestFinishJobClearsChildToken(t *testing.T) {
	server := NewClusterServerImplWithOptions(
		cluster.NodeID("node-1"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
		ServerOptions{RemoteChildTokenTTL: time.Minute},
	)
	if _, err := server.RegisterNode(context.Background(), &RegisterNodeRequest{
		NodeId:  "node-1",
		Address: "10.0.0.1",
		Resources: &NodeResources{
			CpuMillicores: 2000,
			MemoryBytes:   2 * 1024 * 1024 * 1024,
		},
	}); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	req := &ExecuteRequest{
		SessionId: "session-1",
		JobId:     "job-parent",
		Command:   []string{"true"},
		Resources: &Requirements{
			CpuMillicores: 1000,
			MemoryBytes:   1024 * 1024 * 1024,
		},
	}
	node, reqs, jobID, err := server.scheduleJob(req)
	if err != nil {
		t.Fatalf("scheduleJob: %v", err)
	}
	if server.cluster.jobs[jobID].ChildToken == "" {
		t.Fatal("token missing before finish")
	}

	server.finishJob(jobID, node.ID, reqs, 0)
	job := server.cluster.jobs[jobID]
	if job.ChildToken != "" || !job.ChildTokenExpiresAt.IsZero() {
		t.Fatalf("terminal job kept child token: token=%q expires=%s", job.ChildToken, job.ChildTokenExpiresAt)
	}
}

func TestListRemoteChildrenUsesLifecycleStore(t *testing.T) {
	server := NewClusterServerImpl(
		cluster.NodeID("node-1"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
	)
	server.remoteChildren.Upsert(remotechild.ShadowRecord{
		SessionID:       "session-1",
		ParentJobID:     "job-parent",
		ParentPID:       100,
		ShadowPID:       101,
		RemoteJobID:     "job-child",
		RemoteNodeID:    "node-2",
		RemoteGlobalPID: 42,
		RemoteLocalPID:  4242,
		Command:         []string{"hostname"},
		State:           remotechild.StateRunning,
		StartedAt:       time.Unix(20, 0),
	})
	server.remoteChildren.Upsert(remotechild.ShadowRecord{
		SessionID:    "session-1",
		ParentJobID:  "job-parent",
		RemoteJobID:  "job-complete",
		RemoteNodeID: "node-2",
		Command:      []string{"true"},
		State:        remotechild.StateCompleted,
		FinishedAt:   time.Unix(21, 0),
	})

	resp, err := server.ListRemoteChildren(context.Background(), &ListRemoteChildrenRequest{
		ParentJobId: "job-parent",
		ActiveOnly:  true,
	})
	if err != nil {
		t.Fatalf("ListRemoteChildren: %v", err)
	}
	if len(resp.Children) != 1 {
		t.Fatalf("children = %d, want 1: %#v", len(resp.Children), resp.Children)
	}
	child := resp.Children[0]
	if child.RemoteJobId != "job-child" || child.State != remotechild.StateRunning {
		t.Fatalf("child = %#v", child)
	}
	if child.RemoteLocalPid != 4242 {
		t.Fatalf("remote local pid = %d, want 4242", child.RemoteLocalPid)
	}
}

func TestJobInfoToProtoIncludesRemoteChild(t *testing.T) {
	server := NewClusterServerImpl(
		cluster.NodeID("node-1"),
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		nil,
	)
	job := &cluster.JobInfo{
		ID:          "job-child",
		SessionID:   "session-1",
		Status:      cluster.JobStatusRunning,
		CreatedAt:   time.Unix(10, 0),
		PrimaryNode: "node-2",
		Commands: []cluster.CommandSpec{{
			Argv: []string{"hostname"},
			Env:  []string{"PATH=/usr/bin", remotechild.EnvChildToken + "=secret"},
		}},
		RemoteChild: &cluster.RemoteChildInfo{
			ParentJobID:     "job-parent",
			ParentPID:       100,
			ShadowPID:       101,
			RemoteJobID:     "job-child",
			RemoteNodeID:    "node-2",
			RemoteGlobalPID: 42,
			RemoteLocalPID:  4242,
			Command:         []string{"hostname"},
			PlacementReason: "explicit",
			State:           remotechild.StateRunning,
			StartedAt:       time.Unix(20, 0),
		},
	}

	protoJob := server.jobInfoToProto(job)
	if protoJob.RemoteChild == nil {
		t.Fatal("RemoteChild = nil")
	}
	if protoJob.RemoteChild.ParentJobId != "job-parent" || protoJob.RemoteChild.RemoteNodeId != "node-2" {
		t.Fatalf("remote child = %#v", protoJob.RemoteChild)
	}
	if protoJob.RemoteChild.RemoteGlobalPid != 42 {
		t.Fatalf("remote global pid = %d", protoJob.RemoteChild.RemoteGlobalPid)
	}
	if protoJob.RemoteChild.RemoteLocalPid != 4242 {
		t.Fatalf("remote local pid = %d", protoJob.RemoteChild.RemoteLocalPid)
	}
	if protoJob.RemoteChild.State != remotechild.StateRunning || protoJob.RemoteChild.StartedAt != 20 {
		t.Fatalf("remote lifecycle fields = %#v", protoJob.RemoteChild)
	}
	for _, entry := range protoJob.Commands[0].Env {
		if strings.HasPrefix(entry, remotechild.EnvChildToken+"=") {
			t.Fatalf("child token leaked in proto env: %v", protoJob.Commands[0].Env)
		}
	}
}

func TestStrictSSIPTYCommandIncludesSlaveProjection(t *testing.T) {
	root := t.TempDir()
	exitStatusDir := t.TempDir()
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
		}, RemoteChildExitStatusDir: exitStatusDir},
	)

	req := &ExecuteRequest{
		Command: []string{"/bin/sh"},
		Cwd:     "/",
	}
	if err := server.normalizeScheduledRequest(req); err != nil {
		t.Fatalf("normalizeScheduledRequest: %v", err)
	}

	cmd, err := server.buildPTYCommand(context.Background(), req, cluster.Requirements{}, "/dev/pts/7")
	if err != nil {
		t.Fatalf("buildPTYCommand: %v", err)
	}

	if !argPairBeforeCommand(cmd.Args, "--tty-slave", "/dev/pts/7") {
		t.Fatalf("command args missing --tty-slave before command separator: %v", cmd.Args)
	}
}

func TestStrictSSICommandIncludesVFIOGroups(t *testing.T) {
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
		Command: []string{"/bin/sh"},
		Cwd:     "/",
	}
	if err := server.normalizeScheduledRequest(req); err != nil {
		t.Fatalf("normalizeScheduledRequest: %v", err)
	}

	cmd, err := server.buildSSICommand(context.Background(), req, cluster.Requirements{VFIOGroups: []int{7, 8}}, false, "")
	if err != nil {
		t.Fatalf("buildSSICommand: %v", err)
	}
	if !argPairBeforeCommand(cmd.Args, "--vfio-groups", "7,8") {
		t.Fatalf("command args missing --vfio-groups before command separator: %v", cmd.Args)
	}
}

func TestStrictSSIRemoteChildCommandIncludesExitStatusFile(t *testing.T) {
	root := t.TempDir()
	exitStatusDir := t.TempDir()
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
		}, RemoteChildExitStatusDir: exitStatusDir},
	)

	req := &ExecuteRequest{
		JobId:   "job-child",
		Command: []string{"/bin/sh"},
		Cwd:     "/",
		Env:     []string{remotechild.EnvRemoteChild + "=1"},
	}
	if err := server.normalizeScheduledRequest(req); err != nil {
		t.Fatalf("normalizeScheduledRequest: %v", err)
	}

	cmd, err := server.buildSSICommand(context.Background(), req, cluster.Requirements{}, false, "")
	if err != nil {
		t.Fatalf("buildSSICommand: %v", err)
	}
	if !argPairBeforeCommand(cmd.Args, "--exit-status-file", remotechild.WorkerExitStatusPathInDir(exitStatusDir, "job-child")) {
		t.Fatalf("command args missing --exit-status-file before command separator: %v", cmd.Args)
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

func cloneExecuteRequestForTest(req *ExecuteRequest) *ExecuteRequest {
	return proto.Clone(req).(*ExecuteRequest)
}

func argPairBeforeCommand(args []string, key, value string) bool {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "--" {
			return false
		}
		if args[i] == key && args[i+1] == value {
			return true
		}
	}
	return false
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
