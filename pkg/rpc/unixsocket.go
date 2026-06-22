package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/octopos/octopos/pkg/cluster"
	"github.com/octopos/octopos/pkg/ssi"
)

type unixSocketCoordinator struct {
	mu      sync.RWMutex
	entries map[string]unixSocketEntry
}

type unixSocketEntry struct {
	Path       string
	NodeID     cluster.NodeID
	TargetPath string
	CreatedAt  time.Time
}

type unixSocketStreamResult struct {
	side string
	err  error
}

func newUnixSocketCoordinator() *unixSocketCoordinator {
	return &unixSocketCoordinator{
		entries: make(map[string]unixSocketEntry),
	}
}

func (c *unixSocketCoordinator) register(entry unixSocketEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}
	c.entries[entry.Path] = entry
}

func (c *unixSocketCoordinator) unregister(path string, nodeID cluster.NodeID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[path]
	if !ok {
		return
	}
	if nodeID != "" && entry.NodeID != nodeID {
		return
	}
	delete(c.entries, path)
}

func (c *unixSocketCoordinator) lookup(path string) (unixSocketEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[path]
	return entry, ok
}

func (c *unixSocketCoordinator) list() []unixSocketEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]unixSocketEntry, 0, len(c.entries))
	for _, entry := range c.entries {
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Path == out[j].Path {
			return out[i].NodeID < out[j].NodeID
		}
		return out[i].Path < out[j].Path
	})
	return out
}

func (s *ClusterServerImpl) RegisterUnixSocket(ctx context.Context, req *RegisterUnixSocketRequest) (*RegisterUnixSocketResponse, error) {
	path, err := s.normalizeUnixSocketBrokerPath(req.GetPath())
	if err != nil {
		return &RegisterUnixSocketResponse{Error: err.Error()}, nil
	}
	targetPath := req.GetTargetPath()
	if targetPath == "" {
		targetPath = path
	}
	targetPath, err = s.normalizeUnixSocketBrokerPath(targetPath)
	if err != nil {
		return &RegisterUnixSocketResponse{Error: err.Error()}, nil
	}

	nodeID := cluster.NodeID(req.GetNodeId())
	if nodeID == "" {
		nodeID = s.nodeID
	}
	if !isInternalForward(ctx) && nodeID != s.nodeID {
		return &RegisterUnixSocketResponse{Error: "cannot externally register a Unix socket for another node"}, nil
	}

	entry := unixSocketEntry{
		Path:       path,
		NodeID:     nodeID,
		TargetPath: targetPath,
		CreatedAt:  time.Now(),
	}
	s.unixSockets.register(entry)

	if !isInternalForward(ctx) {
		s.broadcastUnixSocketRegistration(ctx, entry)
	}
	return &RegisterUnixSocketResponse{Success: true}, nil
}

func (s *ClusterServerImpl) UnregisterUnixSocket(ctx context.Context, req *UnregisterUnixSocketRequest) (*UnregisterUnixSocketResponse, error) {
	path, err := s.normalizeUnixSocketBrokerPath(req.GetPath())
	if err != nil {
		return &UnregisterUnixSocketResponse{Error: err.Error()}, nil
	}
	nodeID := cluster.NodeID(req.GetNodeId())
	if nodeID == "" {
		nodeID = s.nodeID
	}
	if !isInternalForward(ctx) && nodeID != s.nodeID {
		return &UnregisterUnixSocketResponse{Error: "cannot externally unregister a Unix socket for another node"}, nil
	}

	s.unixSockets.unregister(path, nodeID)
	if !isInternalForward(ctx) {
		s.broadcastUnixSocketUnregistration(ctx, path, nodeID)
	}
	return &UnregisterUnixSocketResponse{Success: true}, nil
}

func (s *ClusterServerImpl) ListUnixSockets(ctx context.Context, req *ListUnixSocketsRequest) (*ListUnixSocketsResponse, error) {
	entries := s.unixSockets.list()
	out := make([]*UnixSocketInfo, 0, len(entries))
	for _, entry := range entries {
		out = append(out, &UnixSocketInfo{
			Path:       entry.Path,
			NodeId:     string(entry.NodeID),
			TargetPath: entry.TargetPath,
			CreatedAt:  entry.CreatedAt.Unix(),
		})
	}
	return &ListUnixSocketsResponse{Sockets: out}, nil
}

func (s *ClusterServerImpl) UnixSocketStream(stream Cluster_UnixSocketStreamServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	path, err := s.normalizeUnixSocketBrokerPath(first.GetPath())
	if err != nil {
		_ = stream.Send(&UnixSocketFrame{Error: err.Error(), Close: true})
		return nil
	}
	first.Path = path

	entry, ok := s.unixSockets.lookup(path)
	if !ok {
		_ = stream.Send(&UnixSocketFrame{Error: fmt.Sprintf("Unix socket %s is not registered", path), Close: true})
		return nil
	}
	if entry.NodeID != s.nodeID {
		if isInternalForward(stream.Context()) {
			_ = stream.Send(&UnixSocketFrame{Error: fmt.Sprintf("Unix socket %s is registered on node %s, not %s", path, entry.NodeID, s.nodeID), Close: true})
			return nil
		}
		return s.proxyUnixSocketStream(stream, entry.NodeID, first)
	}
	return s.serveLocalUnixSocketStream(stream, entry.TargetPath, first)
}

func (s *ClusterServerImpl) broadcastUnixSocketRegistration(ctx context.Context, entry unixSocketEntry) {
	if s.clientPool == nil {
		return
	}
	req := &RegisterUnixSocketRequest{
		Path:       entry.Path,
		NodeId:     string(entry.NodeID),
		TargetPath: entry.TargetPath,
	}
	s.clientPool.Broadcast(internalForwardContext(ctx), func(ctx context.Context, client ClusterClient, nodeID cluster.NodeID) error {
		resp, err := client.RegisterUnixSocket(ctx, req)
		if err != nil {
			return err
		}
		if resp != nil && resp.Error != "" {
			return errors.New(resp.Error)
		}
		return nil
	})
}

func (s *ClusterServerImpl) broadcastUnixSocketUnregistration(ctx context.Context, path string, nodeID cluster.NodeID) {
	if s.clientPool == nil {
		return
	}
	req := &UnregisterUnixSocketRequest{
		Path:   path,
		NodeId: string(nodeID),
	}
	s.clientPool.Broadcast(internalForwardContext(ctx), func(ctx context.Context, client ClusterClient, nodeID cluster.NodeID) error {
		resp, err := client.UnregisterUnixSocket(ctx, req)
		if err != nil {
			return err
		}
		if resp != nil && resp.Error != "" {
			return errors.New(resp.Error)
		}
		return nil
	})
}

func (s *ClusterServerImpl) normalizeUnixSocketBrokerPath(path string) (string, error) {
	clean, err := normalizeUnixSocketBrokerPath(s.ssiConfig, path)
	if err != nil {
		return "", err
	}
	return clean, nil
}

func normalizeUnixSocketBrokerPath(cfg ssi.Config, path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("Unix socket path is required")
	}
	if strings.HasPrefix(path, "\x00") || strings.HasPrefix(path, "@") {
		return "", fmt.Errorf("abstract Unix sockets are not supported")
	}
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("Unix socket path %q must be absolute", path)
	}
	clean := filepath.Clean(path)
	root := filepath.Clean(cfg.WithDefaults().RootFS)
	if clean == root {
		return "", fmt.Errorf("Unix socket path must name a socket below SSI root %s", root)
	}
	rel, err := filepath.Rel(root, clean)
	if err != nil {
		return "", fmt.Errorf("compare Unix socket path to SSI root: %w", err)
	}
	if rel == "." || rel == ".." || strings.HasPrefix(rel, "../") {
		return "", fmt.Errorf("Unix socket path %s is outside SSI root %s", clean, root)
	}
	return clean, nil
}

func (s *ClusterServerImpl) serveLocalUnixSocketStream(stream Cluster_UnixSocketStreamServer, targetPath string, first *UnixSocketFrame) error {
	conn, err := net.Dial("unix", targetPath)
	if err != nil {
		_ = stream.Send(&UnixSocketFrame{Error: fmt.Sprintf("dial Unix socket %s: %v", targetPath, err), Close: true})
		return nil
	}
	defer conn.Close()

	errCh := make(chan unixSocketStreamResult, 2)
	go s.grpcUnixSocketToConn(stream, conn, first, errCh)
	go s.connToGRPCUnixSocket(conn, stream, errCh)

	for {
		result := <-errCh
		if result.err != nil {
			_ = conn.Close()
			return result.err
		}
		if result.side == "grpc" {
			continue
		}
		_ = conn.Close()
		return nil
	}
}

func (s *ClusterServerImpl) grpcUnixSocketToConn(stream Cluster_UnixSocketStreamServer, conn net.Conn, first *UnixSocketFrame, errCh chan<- unixSocketStreamResult) {
	defer closeUnixWrite(conn)
	frame := first
	for {
		if frame.GetError() != "" {
			errCh <- unixSocketStreamResult{side: "grpc"}
			return
		}
		if len(frame.GetData()) > 0 {
			if _, err := conn.Write(frame.GetData()); err != nil {
				errCh <- unixSocketStreamResult{side: "grpc"}
				return
			}
		}
		if frame.GetClose() {
			errCh <- unixSocketStreamResult{side: "grpc"}
			return
		}

		next, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				errCh <- unixSocketStreamResult{side: "grpc"}
				return
			}
			errCh <- unixSocketStreamResult{side: "grpc", err: err}
			return
		}
		frame = next
	}
}

func (s *ClusterServerImpl) connToGRPCUnixSocket(conn net.Conn, stream Cluster_UnixSocketStreamServer, errCh chan<- unixSocketStreamResult) {
	buf := make([]byte, 32*1024)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			if sendErr := stream.Send(&UnixSocketFrame{Data: data}); sendErr != nil {
				errCh <- unixSocketStreamResult{side: "unix", err: sendErr}
				return
			}
		}
		if err != nil {
			if err == io.EOF {
				_ = stream.Send(&UnixSocketFrame{Close: true})
				errCh <- unixSocketStreamResult{side: "unix"}
				return
			}
			_ = stream.Send(&UnixSocketFrame{Error: err.Error(), Close: true})
			errCh <- unixSocketStreamResult{side: "unix"}
			return
		}
	}
}

func (s *ClusterServerImpl) proxyUnixSocketStream(stream Cluster_UnixSocketStreamServer, nodeID cluster.NodeID, first *UnixSocketFrame) error {
	if s.clientPool == nil {
		_ = stream.Send(&UnixSocketFrame{Error: "no peer connections available for Unix socket broker", Close: true})
		return nil
	}
	client, ok := s.clientPool.GetPeer(nodeID)
	if !ok {
		_ = stream.Send(&UnixSocketFrame{Error: fmt.Sprintf("no connection to Unix socket owner node %s", nodeID), Close: true})
		return nil
	}
	peerStream, err := client.UnixSocketStream(internalForwardContext(stream.Context()))
	if err != nil {
		_ = stream.Send(&UnixSocketFrame{Error: err.Error(), Close: true})
		return nil
	}
	if err := peerStream.Send(first); err != nil {
		_ = peerStream.CloseSend()
		_ = stream.Send(&UnixSocketFrame{Error: err.Error(), Close: true})
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
			_ = stream.Send(&UnixSocketFrame{Error: err.Error(), Close: true})
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

func closeUnixWrite(conn net.Conn) {
	if c, ok := conn.(*net.UnixConn); ok {
		_ = c.CloseWrite()
	}
}
