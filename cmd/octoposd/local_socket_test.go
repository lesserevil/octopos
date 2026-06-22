package main

import (
	"context"
	"net"
	"testing"

	orpc "github.com/octopos/octopos/pkg/rpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type testAddr string

func (a testAddr) Network() string { return "unix" }
func (a testAddr) String() string  { return string(a) }

func TestAuthorizeLocalSocketRPCRestrictsMethodsAndUID(t *testing.T) {
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: unixPeerAddr{
			Addr: testAddr("childd.sock"),
			cred: unixPeerCred{pid: 1234, uid: 0, gid: 0},
		},
	})

	if err := authorizeLocalSocketRPC(ctx, orpc.Cluster_SpawnRemoteChild_FullMethodName); err != nil {
		t.Fatalf("SpawnRemoteChild rejected: %v", err)
	}
	if err := authorizeLocalSocketRPC(ctx, orpc.Cluster_RemoteChildLaunchStream_FullMethodName); err != nil {
		t.Fatalf("RemoteChildLaunchStream rejected: %v", err)
	}
	if err := authorizeLocalSocketRPC(ctx, orpc.Cluster_RemoteChildStream_FullMethodName); err != nil {
		t.Fatalf("RemoteChildStream rejected: %v", err)
	}
	if err := authorizeLocalSocketRPC(ctx, orpc.Cluster_Execute_FullMethodName); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("Execute error = %v, want permission denied", err)
	}

	userCtx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: unixPeerAddr{
			Addr: testAddr("childd.sock"),
			cred: unixPeerCred{pid: 1235, uid: 1000, gid: 1000},
		},
	})
	if err := authorizeLocalSocketRPC(userCtx, orpc.Cluster_RemoteChildLaunchStream_FullMethodName); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("non-root RemoteChildLaunchStream error = %v, want permission denied", err)
	}
}

func TestAuthorizeLocalSocketRPCIgnoresNonUnixPeer(t *testing.T) {
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 50051},
	})
	if err := authorizeLocalSocketRPC(ctx, orpc.Cluster_Execute_FullMethodName); err != nil {
		t.Fatalf("non-local socket peer should not be filtered here: %v", err)
	}
}
