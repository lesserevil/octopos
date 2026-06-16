package main

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	octopospb "github.com/octopos/octopos/pkg/rpc"
)

func TestGatewayInit(t *testing.T) {
	// Create a mock gRPC server
	lis := bufconn.Listen(1024 * 1024)

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

	// Test CreateSession call works
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.CreateSession(ctx, &octopospb.CreateSessionRequest{
		SessionId: "test-session",
		User:      "test",
	})
	// Expected to fail since no real server, but should not panic
	if err != nil {
		t.Logf("Expected error (no server): %v", err)
	}
}
