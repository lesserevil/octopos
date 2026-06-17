package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	octopospb "github.com/octopos/octopos/pkg/rpc"
)

func TestGatewayInit(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("test gRPC server stopped: %v", err)
		}
	}()
	defer grpcServer.Stop()

	oldHostKeyFile := *hostKeyFile
	oldVIPAddr := *vipAddr
	t.Cleanup(func() {
		*hostKeyFile = oldHostKeyFile
		*vipAddr = oldVIPAddr
	})

	*hostKeyFile = writeTestHostKey(t)
	*vipAddr = "127.0.0.1:0"

	gw := &Gateway{
		grpcAddr:  lis.Addr().String(),
		nodeID:    "test-node",
		mountBase: t.TempDir(),
		sessions:  make(map[string]*ClusterSession),
	}

	if err := gw.init(); err != nil {
		t.Fatalf("init: %v", err)
	}
	defer gw.close()

	if gw.clusterClient == nil {
		t.Fatal("expected cluster client to be initialized")
	}
	if gw.sshServer == nil {
		t.Fatal("expected SSH server to be initialized")
	}
	if _, err := os.Stat(gw.mountBase); err != nil {
		t.Fatalf("mount base was not created: %v", err)
	}
}

func TestGatewayGRPCClientHandlesUnavailableServer(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	defer lis.Close()
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

	client := octopospb.NewClusterClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = client.CreateSession(ctx, &octopospb.CreateSessionRequest{
		SessionId: "test-session",
		User:      "test",
	})
	if err == nil {
		t.Fatal("expected CreateSession to fail with no registered server")
	}
}

func TestGatewayStartFUSESkipsMissingDaemons(t *testing.T) {
	t.Setenv("PATH", t.TempDir())

	mountBase := t.TempDir()
	gw := &Gateway{mountBase: mountBase}

	mountPoints, cleanup, err := gw.startFUSE("session-1")
	if err != nil {
		t.Fatalf("startFUSE: %v", err)
	}
	t.Cleanup(cleanup)

	if len(mountPoints) != 0 {
		t.Fatalf("expected no mount points when daemons are missing, got %#v", mountPoints)
	}

	for _, name := range []string{"proc-session-1", "dev-session-1", "sys-session-1"} {
		if _, err := os.Stat(filepath.Join(mountBase, name)); err != nil {
			t.Fatalf("expected mount directory %s to be created: %v", name, err)
		}
	}
}

func TestGatewayCloseRunsSessionCleanups(t *testing.T) {
	var calls int32
	gw := &Gateway{
		sessions: map[string]*ClusterSession{
			"a": {Cleanup: func() { atomic.AddInt32(&calls, 1) }},
			"b": {Cleanup: func() { atomic.AddInt32(&calls, 1) }},
		},
	}

	gw.close()

	if calls != 2 {
		t.Fatalf("expected 2 cleanup calls, got %d", calls)
	}
}

func writeTestHostKey(t *testing.T) string {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate host key: %v", err)
	}

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	path := filepath.Join(t.TempDir(), "ssh_host_rsa_key")
	if err := os.WriteFile(path, keyPEM, 0600); err != nil {
		t.Fatalf("write host key: %v", err)
	}
	return path
}
