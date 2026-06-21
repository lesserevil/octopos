package rpc

import (
	"io"
	"testing"

	"github.com/octopos/octopos/pkg/cluster"
	"github.com/octopos/octopos/pkg/remotechild"
)

func TestRemoteChildPipeIDsFromEnv(t *testing.T) {
	got := remoteChildPipeIDsFromEnv([]string{
		remotechild.EnvPipeFD(0) + "=pipe-a",
		remotechild.EnvPipeFD(1) + "=pipe-b",
		remotechild.EnvPipeFDPrefix + "bad=ignored",
		remotechild.EnvPipeFDPrefix + "3=ignored",
	})
	if got[0] != "pipe-a" || got[1] != "pipe-b" {
		t.Fatalf("pipe ids = %#v", got)
	}
	if _, ok := got[3]; ok {
		t.Fatalf("unexpected fd 3 pipe id: %#v", got)
	}
}

func TestPipeCoordinatorPlacement(t *testing.T) {
	coordinator := newPipeCoordinator()
	keys := map[int]string{1: "session\x00parent\x00pipe-a"}
	if got := coordinator.preferredNode(keys); got != "" {
		t.Fatalf("preferred node before placement = %q", got)
	}
	coordinator.recordPlacement(keys, cluster.NodeID("node-2"))
	if got := coordinator.preferredNode(map[int]string{0: "session\x00parent\x00pipe-a"}); got != "node-2" {
		t.Fatalf("preferred node = %q, want node-2", got)
	}
}

func TestPipeCoordinatorLocalPipe(t *testing.T) {
	coordinator := newPipeCoordinator()
	writer, err := coordinator.attachLocal("pipe-a", 1)
	if err != nil {
		t.Fatalf("attach writer: %v", err)
	}
	reader, err := coordinator.attachLocal("pipe-a", 0)
	if err != nil {
		t.Fatalf("attach reader: %v", err)
	}
	defer reader.Close()

	if _, err := writer.Write([]byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("pipe data = %q", data)
	}
	if len(coordinator.local) != 0 {
		t.Fatalf("local pipe registry not cleaned: %#v", coordinator.local)
	}
}
