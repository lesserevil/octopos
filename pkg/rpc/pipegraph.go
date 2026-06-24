package rpc

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/octopos/octopos/pkg/cluster"
	"github.com/octopos/octopos/pkg/remotechild"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type pipeCoordinator struct {
	mu               sync.Mutex
	placement        map[string]cluster.NodeID
	groups           map[string]*pipelineGroup
	local            map[string]*localPipe
	fifos            map[string]*pendingFIFO
	activeStreams    uint64
	totalStreams     uint64
	bytesFromWriters uint64
	bytesToReaders   uint64
	brokenPipes      uint64
}

type localPipe struct {
	read  *os.File
	write *os.File
}

type pipelineGroup struct {
	Key      string
	Children map[cluster.JobID]pipelineChild
	PipeKeys map[string]struct{}
}

type pipelineChild struct {
	JobID     cluster.JobID
	NodeID    cluster.NodeID
	Endpoints []pipelineEndpoint
}

type pipelineEndpoint struct {
	PipeKey   string
	FD        int
	Direction remotechild.PipeEndpointDirection
}

type pendingFIFO struct {
	read   *os.File
	write  *os.File
	reader *fifoWaiter
	writer *fifoWaiter
}

type fifoWaiter struct {
	fd    int
	file  *os.File
	err   error
	ready chan struct{}
}

type pipeStats struct {
	ActiveStreams    uint64
	TotalStreams     uint64
	BytesFromWriters uint64
	BytesToReaders   uint64
	BrokenPipes      uint64
}

func newPipeCoordinator() *pipeCoordinator {
	return &pipeCoordinator{
		placement: make(map[string]cluster.NodeID),
		groups:    make(map[string]*pipelineGroup),
		local:     make(map[string]*localPipe),
		fifos:     make(map[string]*pendingFIFO),
	}
}

func remoteChildPipeIDsFromEnv(env []string) map[int]string {
	out := make(map[int]string)
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || value == "" {
			continue
		}
		var rawFD string
		var namespace string
		switch {
		case strings.HasPrefix(key, remotechild.EnvPipeFDPrefix):
			rawFD = strings.TrimPrefix(key, remotechild.EnvPipeFDPrefix)
			namespace = "pipe"
		case strings.HasPrefix(key, remotechild.EnvFIFOFDPrefix):
			rawFD = strings.TrimPrefix(key, remotechild.EnvFIFOFDPrefix)
			namespace = "fifo"
		default:
			continue
		}
		fd, err := strconv.Atoi(rawFD)
		if err != nil || fd < 0 || fd > 2 {
			continue
		}
		out[fd] = namespace + ":" + value
	}
	return out
}

func remoteChildPipeKeys(req *ExecuteRequest) map[int]string {
	ids := remoteChildPipeIDsFromEnv(req.Env)
	if len(ids) == 0 {
		return nil
	}
	out := make(map[int]string, len(ids))
	parentJobID := requestEnvValue(req.Env, remotechild.EnvParentJobID)
	for fd, id := range ids {
		out[fd] = remoteChildPipeKey(req.SessionId, parentJobID, id)
	}
	return out
}

func remoteChildPipeKey(sessionID string, parentJobID string, pipeID string) string {
	return sessionID + "\x00" + parentJobID + "\x00" + pipeID
}

func remoteChildPipelineGroupKey(pipeKey string) (string, bool) {
	sessionID, rest, ok := strings.Cut(pipeKey, "\x00")
	if !ok || sessionID == "" {
		return "", false
	}
	parentJobID, pipeID, ok := strings.Cut(rest, "\x00")
	if !ok || parentJobID == "" || pipeID == "" {
		return "", false
	}
	return sessionID + "\x00" + parentJobID, true
}

const (
	pipeCoordinatorKeyPrefix = "coord:"
	fifoPipeKeyPrefix        = "fifo-path:"
)

func pipeKeyWithCoordinator(nodeID cluster.NodeID, key string) string {
	if nodeID == "" {
		return key
	}
	return pipeCoordinatorKeyPrefix + string(nodeID) + "\x00" + key
}

func splitPipeCoordinatorKey(key string) (cluster.NodeID, string) {
	if !strings.HasPrefix(key, pipeCoordinatorKeyPrefix) {
		return "", key
	}
	rest := strings.TrimPrefix(key, pipeCoordinatorKeyPrefix)
	node, localKey, ok := strings.Cut(rest, "\x00")
	if !ok || node == "" || localKey == "" {
		return "", key
	}
	return cluster.NodeID(node), localKey
}

func pipeKeyIsFIFO(key string) bool {
	parts := strings.Split(key, "\x00")
	if len(parts) == 0 {
		return false
	}
	return strings.HasPrefix(parts[len(parts)-1], fifoPipeKeyPrefix)
}

func remoteChildPipeCoordinatorNode(env []string) cluster.NodeID {
	if nodeID := requestEnvValue(env, remotechild.EnvPipeCoordinator); nodeID != "" {
		return cluster.NodeID(nodeID)
	}
	return ""
}

func (p *pipeCoordinator) preferredNode(keys map[int]string) cluster.NodeID {
	if p == nil || len(keys) == 0 {
		return ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, key := range keys {
		if nodeID := p.placement[key]; nodeID != "" {
			return nodeID
		}
	}
	return ""
}

func (p *pipeCoordinator) recordPlacement(keys map[int]string, nodeID cluster.NodeID) {
	if p == nil || len(keys) == 0 || nodeID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, key := range keys {
		if p.placement[key] == "" {
			p.placement[key] = nodeID
		}
	}
}

func (p *pipeCoordinator) recordPipelineChild(keys map[int]string, nodeID cluster.NodeID, jobID cluster.JobID) {
	if p == nil || len(keys) == 0 || nodeID == "" || jobID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for fd, key := range keys {
		groupKey, ok := remoteChildPipelineGroupKey(key)
		if !ok {
			continue
		}
		group := p.groups[groupKey]
		if group == nil {
			group = &pipelineGroup{
				Key:      groupKey,
				Children: make(map[cluster.JobID]pipelineChild),
				PipeKeys: make(map[string]struct{}),
			}
			p.groups[groupKey] = group
		}
		group.PipeKeys[key] = struct{}{}
		child := group.Children[jobID]
		if child.JobID == "" {
			child.JobID = jobID
			child.NodeID = nodeID
		}
		if !pipelineChildHasEndpoint(child, key, fd) {
			child.Endpoints = append(child.Endpoints, pipelineEndpoint{
				PipeKey:   key,
				FD:        fd,
				Direction: pipeEndpointDirectionForFD(fd),
			})
		}
		group.Children[jobID] = child
	}
}

func pipelineChildHasEndpoint(child pipelineChild, pipeKey string, fd int) bool {
	for _, endpoint := range child.Endpoints {
		if endpoint.PipeKey == pipeKey && endpoint.FD == fd {
			return true
		}
	}
	return false
}

func pipeEndpointDirectionForFD(fd int) remotechild.PipeEndpointDirection {
	if fd == 0 {
		return remotechild.PipeEndpointRead
	}
	return remotechild.PipeEndpointWrite
}

func (p *pipeCoordinator) pipelineGroupSnapshot(groupKey string) (pipelineGroup, bool) {
	if p == nil || groupKey == "" {
		return pipelineGroup{}, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	group, ok := p.groups[groupKey]
	if !ok {
		return pipelineGroup{}, false
	}
	out := pipelineGroup{
		Key:      group.Key,
		Children: make(map[cluster.JobID]pipelineChild, len(group.Children)),
		PipeKeys: make(map[string]struct{}, len(group.PipeKeys)),
	}
	for key := range group.PipeKeys {
		out.PipeKeys[key] = struct{}{}
	}
	for jobID, child := range group.Children {
		child.Endpoints = append([]pipelineEndpoint{}, child.Endpoints...)
		out.Children[jobID] = child
	}
	return out, true
}

func (p *pipeCoordinator) attachLocal(key string, fd int) (*os.File, error) {
	if p == nil || key == "" {
		return nil, fmt.Errorf("missing pipe coordinator")
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	pipe := p.local[key]
	if pipe == nil {
		read, write, err := os.Pipe()
		if err != nil {
			return nil, err
		}
		pipe = &localPipe{read: read, write: write}
		p.local[key] = pipe
	}

	var file *os.File
	var err error
	switch fd {
	case 0:
		if pipe.read == nil {
			return nil, fmt.Errorf("pipe read endpoint already attached")
		}
		file, err = dupFile(pipe.read, "octopos-pipe-read")
		if err == nil {
			_ = pipe.read.Close()
			pipe.read = nil
		}
	case 1, 2:
		if pipe.write == nil {
			return nil, fmt.Errorf("pipe write endpoint already attached")
		}
		file, err = dupFile(pipe.write, "octopos-pipe-write")
		if err == nil {
			_ = pipe.write.Close()
			pipe.write = nil
		}
	default:
		return nil, fmt.Errorf("unsupported pipe fd %d", fd)
	}
	if err != nil {
		return nil, err
	}
	if pipe.read == nil && pipe.write == nil {
		delete(p.local, key)
	}
	return file, nil
}

func (p *pipeCoordinator) attachFIFO(ctx context.Context, key string, fd int) (*os.File, error) {
	if p == nil || key == "" {
		return nil, fmt.Errorf("missing pipe coordinator")
	}
	if fd != 0 && fd != 1 {
		return nil, fmt.Errorf("unsupported FIFO fd %d", fd)
	}

	waiter := &fifoWaiter{fd: fd, ready: make(chan struct{})}
	p.mu.Lock()
	pending := p.fifos[key]
	if pending == nil {
		read, write, err := os.Pipe()
		if err != nil {
			p.mu.Unlock()
			return nil, err
		}
		pending = &pendingFIFO{read: read, write: write}
		p.fifos[key] = pending
	}

	var err error
	switch fd {
	case 0:
		if pending.reader != nil {
			err = fmt.Errorf("FIFO read endpoint already pending")
		} else {
			pending.reader = waiter
		}
	case 1:
		if pending.writer != nil {
			err = fmt.Errorf("FIFO write endpoint already pending")
		} else {
			pending.writer = waiter
		}
	}
	if err != nil {
		if pending.reader == nil && pending.writer == nil {
			p.closeAndDeleteFIFO(key, pending)
		}
		p.mu.Unlock()
		return nil, err
	}
	p.completeFIFOIfReadyLocked(key, pending)
	p.mu.Unlock()

	select {
	case <-waiter.ready:
		if waiter.err != nil {
			return nil, waiter.err
		}
		return waiter.file, nil
	case <-ctx.Done():
		p.cancelFIFOWaiter(key, waiter)
		return nil, ctx.Err()
	}
}

func (p *pipeCoordinator) completeFIFOIfReadyLocked(key string, pending *pendingFIFO) {
	if pending.reader == nil || pending.writer == nil {
		return
	}
	reader, readErr := dupFile(pending.read, "octopos-fifo-read")
	writer, writeErr := dupFile(pending.write, "octopos-fifo-write")
	_ = pending.read.Close()
	_ = pending.write.Close()
	delete(p.fifos, key)

	pending.reader.file = reader
	pending.reader.err = readErr
	close(pending.reader.ready)
	pending.writer.file = writer
	pending.writer.err = writeErr
	close(pending.writer.ready)
}

func (p *pipeCoordinator) cancelFIFOWaiter(key string, waiter *fifoWaiter) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pending := p.fifos[key]
	if pending == nil {
		return
	}
	if pending.reader == waiter {
		pending.reader = nil
	}
	if pending.writer == waiter {
		pending.writer = nil
	}
	if pending.reader == nil && pending.writer == nil {
		p.closeAndDeleteFIFO(key, pending)
	}
}

func (p *pipeCoordinator) closeAndDeleteFIFO(key string, pending *pendingFIFO) {
	if pending.read != nil {
		_ = pending.read.Close()
	}
	if pending.write != nil {
		_ = pending.write.Close()
	}
	delete(p.fifos, key)
}

func (p *pipeCoordinator) beginProxyStream() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.activeStreams++
	p.totalStreams++
}

func (p *pipeCoordinator) endProxyStream() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.activeStreams > 0 {
		p.activeStreams--
	}
}

func (p *pipeCoordinator) addBytesFromWriter(n int) {
	if p == nil || n <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.bytesFromWriters += uint64(n)
}

func (p *pipeCoordinator) addBytesToReader(n int) {
	if p == nil || n <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.bytesToReaders += uint64(n)
}

func (p *pipeCoordinator) recordBrokenPipe() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.brokenPipes++
}

func (p *pipeCoordinator) stats() pipeStats {
	if p == nil {
		return pipeStats{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return pipeStats{
		ActiveStreams:    p.activeStreams,
		TotalStreams:     p.totalStreams,
		BytesFromWriters: p.bytesFromWriters,
		BytesToReaders:   p.bytesToReaders,
		BrokenPipes:      p.brokenPipes,
	}
}

func dupFile(file *os.File, name string) (*os.File, error) {
	fd, err := syscall.Dup(int(file.Fd()))
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

func (s *ClusterServerImpl) attachPipeEndpoint(ctx context.Context, req *ExecuteRequest, key string, fd int) (*os.File, error) {
	coordinatorNode := remoteChildPipeCoordinatorNode(req.Env)
	if coordinatorNode == "" || coordinatorNode == s.nodeID {
		return s.pipes.attachLocal(key, fd)
	}
	return s.attachRemotePipeEndpoint(ctx, coordinatorNode, key, fd)
}

func (s *ClusterServerImpl) attachRemotePipeEndpoint(ctx context.Context, coordinatorNode cluster.NodeID, key string, fd int) (*os.File, error) {
	if s.clientPool == nil {
		return nil, fmt.Errorf("no peer connections available for pipe coordinator %s", coordinatorNode)
	}
	client, ok := s.clientPool.GetPeer(coordinatorNode)
	if !ok {
		return nil, fmt.Errorf("no connection to pipe coordinator node %s", coordinatorNode)
	}
	stream, err := client.PipeStream(internalForwardContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("connect pipe coordinator %s: %w", coordinatorNode, err)
	}
	if err := stream.Send(&PipeFrame{Key: key, Fd: int32(fd)}); err != nil {
		_ = stream.CloseSend()
		return nil, fmt.Errorf("attach remote pipe endpoint: %w", err)
	}

	switch fd {
	case 0:
		readEnd, writeEnd, err := os.Pipe()
		if err != nil {
			_ = stream.CloseSend()
			return nil, err
		}
		go pipeStreamToWriter(stream, writeEnd)
		return readEnd, nil
	case 1, 2:
		readEnd, writeEnd, err := os.Pipe()
		if err != nil {
			_ = stream.CloseSend()
			return nil, err
		}
		go readerToPipeStream(readEnd, stream)
		return writeEnd, nil
	default:
		_ = stream.CloseSend()
		return nil, fmt.Errorf("unsupported pipe fd %d", fd)
	}
}

func pipeStreamToWriter(stream Cluster_PipeStreamClient, writer *os.File) {
	defer writer.Close()
	defer stream.CloseSend()
	for {
		frame, err := stream.Recv()
		if err != nil {
			return
		}
		if frame.Error != "" {
			return
		}
		if len(frame.Data) > 0 {
			if _, err := writer.Write(frame.Data); err != nil {
				_ = stream.Send(&PipeFrame{Error: err.Error(), Close: true})
				return
			}
		}
		if frame.Close {
			return
		}
	}
}

func readerToPipeStream(reader *os.File, stream Cluster_PipeStreamClient) {
	defer reader.Close()
	defer stream.CloseSend()
	buf := make([]byte, 32*1024)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			if sendErr := stream.Send(&PipeFrame{Data: data}); sendErr != nil {
				return
			}
		}
		if err != nil {
			_ = stream.Send(&PipeFrame{Close: true})
			return
		}
	}
}

func (s *ClusterServerImpl) PipeStream(stream Cluster_PipeStreamServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.Key == "" {
		return status.Error(codes.InvalidArgument, "pipe key is required")
	}
	if coordinatorNode, localKey := splitPipeCoordinatorKey(first.Key); coordinatorNode != "" {
		first.Key = localKey
		if coordinatorNode != s.nodeID {
			if isInternalForward(stream.Context()) {
				_ = stream.Send(&PipeFrame{Error: fmt.Sprintf("pipe coordinator %s is not local node %s", coordinatorNode, s.nodeID), Close: true})
				return nil
			}
			return s.proxyPipeStream(stream, coordinatorNode, first)
		}
	}
	fd := int(first.Fd)
	if fd < 0 || fd > 2 {
		return status.Errorf(codes.InvalidArgument, "unsupported pipe fd %d", fd)
	}
	s.pipes.beginProxyStream()
	defer s.pipes.endProxyStream()
	isFIFO := pipeKeyIsFIFO(first.Key)
	var file *os.File
	if isFIFO {
		file, err = s.pipes.attachFIFO(stream.Context(), first.Key, fd)
	} else {
		file, err = s.pipes.attachLocal(first.Key, fd)
	}
	if err != nil {
		_ = stream.Send(&PipeFrame{Error: err.Error(), Close: true})
		return nil
	}
	defer file.Close()
	if isFIFO {
		if err := stream.Send(&PipeFrame{}); err != nil {
			return err
		}
	}

	switch fd {
	case 0:
		return s.pipeFileToStream(file, stream)
	case 1, 2:
		return s.pipeStreamToFile(stream, file)
	default:
		return status.Errorf(codes.InvalidArgument, "unsupported pipe fd %d", fd)
	}
}

func (s *ClusterServerImpl) proxyPipeStream(stream Cluster_PipeStreamServer, nodeID cluster.NodeID, first *PipeFrame) error {
	if s.clientPool == nil {
		_ = stream.Send(&PipeFrame{Error: "no peer connections available for pipe coordinator", Close: true})
		return nil
	}
	client, ok := s.clientPool.GetPeer(nodeID)
	if !ok {
		_ = stream.Send(&PipeFrame{Error: fmt.Sprintf("no connection to pipe coordinator node %s", nodeID), Close: true})
		return nil
	}
	peerStream, err := client.PipeStream(internalForwardContext(stream.Context()))
	if err != nil {
		_ = stream.Send(&PipeFrame{Error: err.Error(), Close: true})
		return nil
	}
	if err := peerStream.Send(first); err != nil {
		_ = peerStream.CloseSend()
		_ = stream.Send(&PipeFrame{Error: err.Error(), Close: true})
		return nil
	}

	go func() {
		defer peerStream.CloseSend()
		for {
			frame, err := stream.Recv()
			if err != nil {
				return
			}
			if err := peerStream.Send(frame); err != nil {
				return
			}
			if frame.GetClose() || frame.GetError() != "" {
				return
			}
		}
	}()

	for {
		frame, err := peerStream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			_ = stream.Send(&PipeFrame{Error: err.Error(), Close: true})
			return nil
		}
		if err := stream.Send(frame); err != nil {
			return err
		}
		if frame.GetClose() || frame.GetError() != "" {
			return nil
		}
	}
}

func (s *ClusterServerImpl) GetPipeStats(ctx context.Context, req *GetPipeStatsRequest) (*GetPipeStatsResponse, error) {
	stats := s.pipes.stats()
	return &GetPipeStatsResponse{
		ActiveStreams:    stats.ActiveStreams,
		TotalStreams:     stats.TotalStreams,
		BytesFromWriters: stats.BytesFromWriters,
		BytesToReaders:   stats.BytesToReaders,
		BrokenPipes:      stats.BrokenPipes,
	}, nil
}

func (s *ClusterServerImpl) pipeFileToStream(reader *os.File, stream Cluster_PipeStreamServer) error {
	buf := make([]byte, 32*1024)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			if sendErr := stream.Send(&PipeFrame{Data: data}); sendErr != nil {
				s.pipes.recordBrokenPipe()
				return sendErr
			}
			s.pipes.addBytesToReader(n)
		}
		if err != nil {
			if err == io.EOF {
				_ = stream.Send(&PipeFrame{Close: true})
				return nil
			}
			_ = stream.Send(&PipeFrame{Error: err.Error(), Close: true})
			s.pipes.recordBrokenPipe()
			return nil
		}
	}
}

func (s *ClusterServerImpl) pipeStreamToFile(stream Cluster_PipeStreamServer, writer *os.File) error {
	for {
		frame, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if frame.Error != "" {
			return nil
		}
		if len(frame.Data) > 0 {
			if _, err := writer.Write(frame.Data); err != nil {
				_ = stream.Send(&PipeFrame{Error: err.Error(), Close: true})
				s.pipes.recordBrokenPipe()
				return nil
			}
			s.pipes.addBytesFromWriter(len(frame.Data))
		}
		if frame.Close {
			return nil
		}
	}
}
