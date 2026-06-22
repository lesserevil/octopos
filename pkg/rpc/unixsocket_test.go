package rpc

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/octopos/octopos/pkg/cluster"
	"github.com/octopos/octopos/pkg/scheduler"
	"github.com/octopos/octopos/pkg/ssi"
	"github.com/octopos/octopos/pkg/tracker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

func TestNormalizeUnixSocketBrokerPath(t *testing.T) {
	root := t.TempDir()
	cfg := ssi.Config{RootFS: root}
	path, err := normalizeUnixSocketBrokerPath(cfg, filepath.Join(root, "run", "..", "sock"))
	if err != nil {
		t.Fatalf("normalize valid path: %v", err)
	}
	if path != filepath.Join(root, "sock") {
		t.Fatalf("normalized path = %q", path)
	}

	rejects := []string{
		"",
		"relative.sock",
		root,
		filepath.Join(filepath.Dir(root), "outside.sock"),
		"@abstract",
	}
	for _, reject := range rejects {
		if _, err := normalizeUnixSocketBrokerPath(cfg, reject); err == nil {
			t.Fatalf("normalizeUnixSocketBrokerPath(%q) succeeded, want error", reject)
		}
	}
}

func TestUnixSocketRegisterListUnregister(t *testing.T) {
	root := t.TempDir()
	server := newUnixSocketTestServer("node-1", root, nil)
	client, cleanup := newClientForRPCServer(t, server)
	defer cleanup()

	path := filepath.Join(root, "svc.sock")
	register, err := client.RegisterUnixSocket(context.Background(), &RegisterUnixSocketRequest{Path: path})
	if err != nil {
		t.Fatalf("RegisterUnixSocket: %v", err)
	}
	if !register.Success {
		t.Fatalf("RegisterUnixSocket failed: %s", register.Error)
	}

	list, err := client.ListUnixSockets(context.Background(), &ListUnixSocketsRequest{})
	if err != nil {
		t.Fatalf("ListUnixSockets: %v", err)
	}
	if len(list.Sockets) != 1 || list.Sockets[0].Path != path || list.Sockets[0].NodeId != "node-1" {
		t.Fatalf("registered sockets = %+v", list.Sockets)
	}

	unregister, err := client.UnregisterUnixSocket(context.Background(), &UnregisterUnixSocketRequest{Path: path})
	if err != nil {
		t.Fatalf("UnregisterUnixSocket: %v", err)
	}
	if !unregister.Success {
		t.Fatalf("UnregisterUnixSocket failed: %s", unregister.Error)
	}
	list, err = client.ListUnixSockets(context.Background(), &ListUnixSocketsRequest{})
	if err != nil {
		t.Fatalf("ListUnixSockets after unregister: %v", err)
	}
	if len(list.Sockets) != 0 {
		t.Fatalf("registered sockets after unregister = %+v", list.Sockets)
	}
}

func TestUnixSocketStreamLocalEcho(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "echo.sock")
	stopEcho := startUnixEcho(t, path)
	defer stopEcho()

	server := newUnixSocketTestServer("node-1", root, nil)
	client, cleanup := newClientForRPCServer(t, server)
	defer cleanup()
	registerUnixSocketForTest(t, client, path)

	got := unixSocketRoundTrip(t, client, path, []byte("ping"))
	if string(got) != "echo:ping" {
		t.Fatalf("round trip = %q", got)
	}
}

func TestUnixSocketStreamForwardsToRegisteredNode(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "echo.sock")
	stopEcho := startUnixEcho(t, path)
	defer stopEcho()

	owner := newUnixSocketTestServer("owner", root, nil)
	ownerClient, ownerCleanup := newClientForRPCServer(t, owner)
	defer ownerCleanup()
	registerUnixSocketForTest(t, ownerClient, path)

	pool := &ClusterClientPool{
		peers: map[cluster.NodeID]*PeerClient{
			"owner": {
				NodeID: "owner",
				Client: ownerClient,
			},
		},
		nodeID: "proxy",
	}
	proxy := newUnixSocketTestServer("proxy", root, pool)
	proxyClient, proxyCleanup := newClientForRPCServer(t, proxy)
	defer proxyCleanup()

	internalCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(internalForwardMetadata, "1"))
	resp, err := proxy.RegisterUnixSocket(internalCtx, &RegisterUnixSocketRequest{
		Path:       path,
		NodeId:     "owner",
		TargetPath: path,
	})
	if err != nil {
		t.Fatalf("proxy RegisterUnixSocket: %v", err)
	}
	if !resp.Success {
		t.Fatalf("proxy RegisterUnixSocket failed: %s", resp.Error)
	}

	got := unixSocketRoundTrip(t, proxyClient, path, []byte("remote"))
	if string(got) != "echo:remote" {
		t.Fatalf("forwarded round trip = %q", got)
	}
}

func newUnixSocketTestServer(nodeID cluster.NodeID, root string, pool *ClusterClientPool) *ClusterServerImpl {
	return NewClusterServerImplWithOptions(
		nodeID,
		scheduler.NewScheduler(&scheduler.BinPackPolicy{}),
		tracker.NewTracker(),
		pool,
		ServerOptions{SSI: ssi.Config{ClusterRoot: root, RootFS: root}},
	)
}

func newClientForRPCServer(t *testing.T, server *ClusterServerImpl) (ClusterClient, func()) {
	t.Helper()
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
	return NewClusterClient(conn), func() {
		conn.Close()
		grpcServer.Stop()
	}
}

func startUnixEcho(t *testing.T, path string) func() {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir unix echo dir: %v", err)
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen unix echo: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		data, err := io.ReadAll(conn)
		if err != nil {
			return
		}
		_, _ = conn.Write(append([]byte("echo:"), data...))
	}()
	return func() {
		_ = listener.Close()
		<-done
		_ = os.Remove(path)
	}
}

func registerUnixSocketForTest(t *testing.T, client ClusterClient, path string) {
	t.Helper()
	resp, err := client.RegisterUnixSocket(context.Background(), &RegisterUnixSocketRequest{Path: path})
	if err != nil {
		t.Fatalf("RegisterUnixSocket: %v", err)
	}
	if !resp.Success {
		t.Fatalf("RegisterUnixSocket failed: %s", resp.Error)
	}
}

func unixSocketRoundTrip(t *testing.T, client ClusterClient, path string, data []byte) []byte {
	t.Helper()
	stream, err := client.UnixSocketStream(context.Background())
	if err != nil {
		t.Fatalf("UnixSocketStream: %v", err)
	}
	if err := stream.Send(&UnixSocketFrame{Path: path, Data: data, Close: true}); err != nil {
		t.Fatalf("send UnixSocketFrame: %v", err)
	}
	var out bytes.Buffer
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("recv UnixSocketFrame: %v", err)
		}
		if frame.Error != "" {
			t.Fatalf("UnixSocketStream error: %s", frame.Error)
		}
		if len(frame.Data) > 0 {
			out.Write(frame.Data)
		}
		if frame.Close {
			break
		}
	}
	_ = stream.CloseSend()
	return out.Bytes()
}
