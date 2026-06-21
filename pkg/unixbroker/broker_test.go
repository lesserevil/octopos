package unixbroker

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBrokerProxiesUnixStream(t *testing.T) {
	dir := t.TempDir()
	targetPath := filepath.Join(dir, "target.sock")
	brokerPath := filepath.Join(dir, "broker.sock")

	target, err := net.Listen("unix", targetPath)
	if err != nil {
		t.Fatalf("listen target: %v", err)
	}
	defer target.Close()
	go func() {
		for {
			conn, err := target.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				line, err := bufio.NewReader(conn).ReadString('\n')
				if err == nil {
					_, _ = fmt.Fprintf(conn, "echo:%s", line)
				}
			}()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- Broker{ListenPath: brokerPath, TargetPath: targetPath}.Serve(ctx)
	}()
	waitForSocket(t, brokerPath)

	conn, err := net.Dial("unix", brokerPath)
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	if _, err := fmt.Fprintln(conn, "hello"); err != nil {
		t.Fatalf("write broker: %v", err)
	}
	got, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("read broker: %v", err)
	}
	_ = conn.Close()
	if got != "echo:hello\n" {
		t.Fatalf("response = %q", got)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("broker serve: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("broker did not stop")
	}
}

func TestPrepareListenPathRefusesNonSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-socket")
	if err := os.WriteFile(path, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := prepareListenPath(path); err == nil {
		t.Fatal("prepareListenPath replaced non-socket")
	}
}

func waitForSocket(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if info, err := os.Stat(path); err == nil && info.Mode()&os.ModeSocket != 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for socket %s", path)
}
