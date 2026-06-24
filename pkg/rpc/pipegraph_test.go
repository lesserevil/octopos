package rpc

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/octopos/octopos/pkg/cluster"
	"github.com/octopos/octopos/pkg/remotechild"
	"google.golang.org/grpc/metadata"
)

func TestRemoteChildPipeIDsFromEnv(t *testing.T) {
	got := remoteChildPipeIDsFromEnv([]string{
		remotechild.EnvPipeFD(0) + "=pipe-a",
		remotechild.EnvFIFOFD(1) + "=/cluster/tmp/fifo-b",
		remotechild.EnvPipeFDPrefix + "bad=ignored",
		remotechild.EnvPipeFDPrefix + "3=ignored",
	})
	if got[0] != "pipe:pipe-a" || got[1] != "fifo:/cluster/tmp/fifo-b" {
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

func TestPipeCoordinatorSelectsCoordinatorNodeForSchedule(t *testing.T) {
	coordinator := newPipeCoordinator()
	keys := map[int]string{1: "session\x00parent\x00pipe-a"}
	if got := coordinator.coordinatorNodeForSchedule(keys, "node-2"); got != "node-2" {
		t.Fatalf("coordinator node before placement = %q, want node-2", got)
	}
	coordinator.recordPlacement(keys, "node-3")
	if got := coordinator.coordinatorNodeForSchedule(map[int]string{0: "session\x00parent\x00pipe-a"}, "node-2"); got != "node-3" {
		t.Fatalf("coordinator node after placement = %q, want node-3", got)
	}
}

func TestPipeCoordinatorTracksPipelineGroups(t *testing.T) {
	coordinator := newPipeCoordinator()
	producerKey := remoteChildPipeKey("session-1", "job-parent", "pipe:pipe-a")
	consumerKey := remoteChildPipeKey("session-1", "job-parent", "pipe:pipe-a")

	coordinator.recordPipelineChild(map[int]string{1: producerKey}, "node-1", "job-producer")
	coordinator.recordPipelineChild(map[int]string{0: consumerKey}, "node-2", "job-consumer")
	coordinator.recordPipelineChild(map[int]string{1: producerKey}, "node-1", "job-producer")

	groupKey, ok := remoteChildPipelineGroupKey(producerKey)
	if !ok {
		t.Fatalf("pipeline group key missing for %q", producerKey)
	}
	group, ok := coordinator.pipelineGroupSnapshot(groupKey)
	if !ok {
		t.Fatalf("pipeline group %q missing", groupKey)
	}
	if len(group.Children) != 2 {
		t.Fatalf("children = %#v, want two", group.Children)
	}
	producer := group.Children["job-producer"]
	if producer.NodeID != "node-1" || len(producer.Endpoints) != 1 || producer.Endpoints[0].Direction != remotechild.PipeEndpointWrite {
		t.Fatalf("producer child = %#v", producer)
	}
	consumer := group.Children["job-consumer"]
	if consumer.NodeID != "node-2" || len(consumer.Endpoints) != 1 || consumer.Endpoints[0].Direction != remotechild.PipeEndpointRead {
		t.Fatalf("consumer child = %#v", consumer)
	}
	if len(group.PipeKeys) != 1 {
		t.Fatalf("pipe keys = %#v, want one", group.PipeKeys)
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

func TestPipeCoordinatorFIFOBlocksUntilPeer(t *testing.T) {
	coordinator := newPipeCoordinator()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	readerCh := make(chan struct {
		file *os.File
		err  error
	}, 1)
	go func() {
		file, err := coordinator.attachFIFO(ctx, "session\x00job\x00fifo-path:/cluster/tmp/test", 0)
		readerCh <- struct {
			file *os.File
			err  error
		}{file: file, err: err}
	}()

	select {
	case got := <-readerCh:
		if got.err == nil {
			_ = got.file.Close()
		}
		t.Fatalf("reader attached before writer: %#v", got)
	case <-time.After(100 * time.Millisecond):
	}

	writer, err := coordinator.attachFIFO(ctx, "session\x00job\x00fifo-path:/cluster/tmp/test", 1)
	if err != nil {
		t.Fatalf("attach writer: %v", err)
	}
	defer writer.Close()

	got := <-readerCh
	if got.err != nil {
		t.Fatalf("reader attach failed: %v", got.err)
	}
	defer got.file.Close()

	if _, err := writer.Write([]byte("fifo data")); err != nil {
		t.Fatalf("write fifo: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	data, err := io.ReadAll(got.file)
	if err != nil {
		t.Fatalf("read fifo: %v", err)
	}
	if string(data) != "fifo data" {
		t.Fatalf("fifo data = %q", data)
	}
	if len(coordinator.fifos) != 0 {
		t.Fatalf("fifo registry not cleaned: %#v", coordinator.fifos)
	}
}

func TestPipeCoordinatorKeyPrefix(t *testing.T) {
	key := "session\x00job\x00fifo-path:/cluster/tmp/test"
	wrapped := pipeKeyWithCoordinator("node-1", key)
	nodeID, localKey := splitPipeCoordinatorKey(wrapped)
	if nodeID != "node-1" || localKey != key {
		t.Fatalf("split coordinator key = (%q, %q), want node-1 and %q", nodeID, localKey, key)
	}
	if !pipeKeyIsFIFO(localKey) {
		t.Fatalf("pipeKeyIsFIFO(%q) = false", localKey)
	}
}

func TestPipeStreamBridgesRemoteEndpoints(t *testing.T) {
	client, cleanup := newTestClusterClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reader, err := client.PipeStream(ctx)
	if err != nil {
		t.Fatalf("reader PipeStream: %v", err)
	}
	if err := reader.Send(&PipeFrame{Key: "pipe-stream-test", Fd: 0}); err != nil {
		t.Fatalf("attach reader: %v", err)
	}

	readDone := make(chan struct {
		data []byte
		err  error
	}, 1)
	go func() {
		var buf bytes.Buffer
		for {
			frame, err := reader.Recv()
			if err != nil {
				readDone <- struct {
					data []byte
					err  error
				}{buf.Bytes(), err}
				return
			}
			if frame.Error != "" {
				readDone <- struct {
					data []byte
					err  error
				}{buf.Bytes(), io.ErrUnexpectedEOF}
				return
			}
			buf.Write(frame.Data)
			if frame.Close {
				readDone <- struct {
					data []byte
					err  error
				}{buf.Bytes(), nil}
				return
			}
		}
	}()

	writer, err := client.PipeStream(ctx)
	if err != nil {
		t.Fatalf("writer PipeStream: %v", err)
	}
	if err := writer.Send(&PipeFrame{Key: "pipe-stream-test", Fd: 1}); err != nil {
		t.Fatalf("attach writer: %v", err)
	}
	if err := writer.Send(&PipeFrame{Data: []byte("hello over pipe")}); err != nil {
		t.Fatalf("send pipe data: %v", err)
	}
	if err := writer.Send(&PipeFrame{Close: true}); err != nil {
		t.Fatalf("send pipe close: %v", err)
	}
	if err := writer.CloseSend(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	select {
	case got := <-readDone:
		if got.err != nil {
			t.Fatalf("reader failed: %v", got.err)
		}
		if string(got.data) != "hello over pipe" {
			t.Fatalf("pipe data = %q", got.data)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	stats, err := client.GetPipeStats(ctx, &GetPipeStatsRequest{})
	if err != nil {
		t.Fatalf("GetPipeStats: %v", err)
	}
	if stats.TotalStreams != 2 {
		t.Fatalf("total streams = %d, want 2", stats.TotalStreams)
	}
	if stats.BytesFromWriters != uint64(len("hello over pipe")) || stats.BytesToReaders != uint64(len("hello over pipe")) {
		t.Fatalf("pipe byte stats = from writers %d to readers %d", stats.BytesFromWriters, stats.BytesToReaders)
	}
}

func TestWriteFullRetriesShortWrites(t *testing.T) {
	writer := &shortChunkWriter{max: 3}
	if err := writeFull(writer, []byte("hello over chunks")); err != nil {
		t.Fatalf("writeFull: %v", err)
	}
	if got := writer.String(); got != "hello over chunks" {
		t.Fatalf("written data = %q", got)
	}
	if writer.writes <= 1 {
		t.Fatalf("writes = %d, want chunked writes", writer.writes)
	}
}

func TestPipeStreamToFileReportsBrokenPipe(t *testing.T) {
	server := NewClusterServerImpl("node-1", nil, nil, nil)
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	_ = readEnd.Close()
	defer writeEnd.Close()

	stream := &fakePipeStreamServer{
		ctx:    context.Background(),
		frames: []*PipeFrame{{Data: []byte("downstream closed")}},
	}
	if err := server.pipeStreamToFile(stream, writeEnd); err != nil {
		t.Fatalf("pipeStreamToFile: %v", err)
	}
	stats := server.pipes.stats()
	if stats.BrokenPipes != 1 {
		t.Fatalf("broken pipes = %d, want 1", stats.BrokenPipes)
	}
	if len(stream.sent) != 1 || stream.sent[0].Error == "" || !stream.sent[0].Close {
		t.Fatalf("sent frames = %#v, want error close frame", stream.sent)
	}
}

func TestAttachRemotePipeEndpoint(t *testing.T) {
	client, cleanup := newTestClusterClient(t)
	defer cleanup()

	pool := &ClusterClientPool{
		peers: map[cluster.NodeID]*PeerClient{
			"coordinator": {NodeID: "coordinator", Client: client},
		},
	}
	worker := NewClusterServerImpl("worker", nil, nil, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reader, err := worker.attachRemotePipeEndpoint(ctx, "coordinator", "remote-endpoint-test", 0)
	if err != nil {
		t.Fatalf("attach remote reader: %v", err)
	}
	defer reader.Close()
	writer, err := worker.attachRemotePipeEndpoint(ctx, "coordinator", "remote-endpoint-test", 1)
	if err != nil {
		t.Fatalf("attach remote writer: %v", err)
	}
	if _, err := writer.Write([]byte("remote endpoint data")); err != nil {
		t.Fatalf("write remote endpoint: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read remote endpoint: %v", err)
	}
	if string(data) != "remote endpoint data" {
		t.Fatalf("remote endpoint data = %q", data)
	}
}

type shortChunkWriter struct {
	bytes.Buffer
	max    int
	writes int
}

func (w *shortChunkWriter) Write(data []byte) (int, error) {
	w.writes++
	if len(data) > w.max {
		data = data[:w.max]
	}
	return w.Buffer.Write(data)
}

type fakePipeStreamServer struct {
	ctx    context.Context
	frames []*PipeFrame
	sent   []*PipeFrame
}

func (s *fakePipeStreamServer) Send(frame *PipeFrame) error {
	s.sent = append(s.sent, frame)
	return nil
}

func (s *fakePipeStreamServer) Recv() (*PipeFrame, error) {
	if len(s.frames) == 0 {
		return nil, io.EOF
	}
	frame := s.frames[0]
	s.frames = s.frames[1:]
	return frame, nil
}

func (s *fakePipeStreamServer) SetHeader(metadata.MD) error {
	return nil
}

func (s *fakePipeStreamServer) SendHeader(metadata.MD) error {
	return nil
}

func (s *fakePipeStreamServer) SetTrailer(metadata.MD) {
}

func (s *fakePipeStreamServer) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

func (s *fakePipeStreamServer) SendMsg(any) error {
	return nil
}

func (s *fakePipeStreamServer) RecvMsg(any) error {
	return io.EOF
}
