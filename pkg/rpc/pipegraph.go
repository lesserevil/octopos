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
	mu        sync.Mutex
	placement map[string]cluster.NodeID
	local     map[string]*localPipe
}

type localPipe struct {
	read  *os.File
	write *os.File
}

func newPipeCoordinator() *pipeCoordinator {
	return &pipeCoordinator{
		placement: make(map[string]cluster.NodeID),
		local:     make(map[string]*localPipe),
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
	fd := int(first.Fd)
	if fd < 0 || fd > 2 {
		return status.Errorf(codes.InvalidArgument, "unsupported pipe fd %d", fd)
	}
	file, err := s.pipes.attachLocal(first.Key, fd)
	if err != nil {
		_ = stream.Send(&PipeFrame{Error: err.Error(), Close: true})
		return nil
	}
	defer file.Close()

	switch fd {
	case 0:
		return pipeFileToStream(file, stream)
	case 1, 2:
		return pipeStreamToFile(stream, file)
	default:
		return status.Errorf(codes.InvalidArgument, "unsupported pipe fd %d", fd)
	}
}

func pipeFileToStream(reader *os.File, stream Cluster_PipeStreamServer) error {
	buf := make([]byte, 32*1024)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			if sendErr := stream.Send(&PipeFrame{Data: data}); sendErr != nil {
				return sendErr
			}
		}
		if err != nil {
			if err == io.EOF {
				_ = stream.Send(&PipeFrame{Close: true})
				return nil
			}
			_ = stream.Send(&PipeFrame{Error: err.Error(), Close: true})
			return nil
		}
	}
}

func pipeStreamToFile(stream Cluster_PipeStreamServer, writer *os.File) error {
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
				return nil
			}
		}
		if frame.Close {
			return nil
		}
	}
}
