package rpc

import (
	"context"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/octopos/octopos/pkg/cluster"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type PeerClient struct {
	NodeID  cluster.NodeID
	Address string
	conn    *grpc.ClientConn
	Client  ClusterClient
}

type ClusterClientPool struct {
	mu       sync.RWMutex
	peers    map[cluster.NodeID]*PeerClient
	nodeID   cluster.NodeID
	selfAddr string
	selfPort int32
}

func NewClusterClientPool(nodeID cluster.NodeID, selfAddr string, selfPort int32) *ClusterClientPool {
	return &ClusterClientPool{
		peers:    make(map[cluster.NodeID]*PeerClient),
		nodeID:   nodeID,
		selfAddr: selfAddr,
		selfPort: selfPort,
	}
}

func (p *ClusterClientPool) dial(addr string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
}

func normalizeGRPCAddress(address string, port int32) string {
	address = strings.TrimSpace(address)
	if address == "" {
		return address
	}
	if _, _, err := net.SplitHostPort(address); err == nil {
		return address
	}
	if port == 0 {
		port = 50051
	}
	return net.JoinHostPort(address, strconv.Itoa(int(port)))
}

func (p *ClusterClientPool) AddPeer(nodeID cluster.NodeID, address string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if nodeID == p.nodeID {
		return nil
	}

	if _, exists := p.peers[nodeID]; exists {
		return nil
	}

	conn, err := p.dial(address)
	if err != nil {
		return err
	}

	p.peers[nodeID] = &PeerClient{
		NodeID:  nodeID,
		Address: address,
		conn:    conn,
		Client:  NewClusterClient(conn),
	}

	log.Printf("Connected to peer %s at %s", nodeID, address)
	return nil
}

func (p *ClusterClientPool) RemovePeer(nodeID cluster.NodeID) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if peer, exists := p.peers[nodeID]; exists {
		peer.conn.Close()
		delete(p.peers, nodeID)
		log.Printf("Disconnected from peer %s", nodeID)
	}
}

func (p *ClusterClientPool) GetPeer(nodeID cluster.NodeID) (ClusterClient, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	peer, exists := p.peers[nodeID]
	if !exists {
		return nil, false
	}
	return peer.Client, true
}

func (p *ClusterClientPool) ListPeers() []*PeerClient {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make([]*PeerClient, 0, len(p.peers))
	for _, peer := range p.peers {
		result = append(result, peer)
	}
	return result
}

func (p *ClusterClientPool) Broadcast(ctx context.Context, fn func(ctx context.Context, client ClusterClient, nodeID cluster.NodeID) error) {
	p.mu.RLock()
	peers := make([]*PeerClient, 0, len(p.peers))
	for _, peer := range p.peers {
		peers = append(peers, peer)
	}
	p.mu.RUnlock()

	for _, peer := range peers {
		if err := fn(ctx, peer.Client, peer.NodeID); err != nil {
			log.Printf("Broadcast to peer %s failed: %v", peer.NodeID, err)
		}
	}
}

func (p *ClusterClientPool) RegisterWithPeers(ctx context.Context, resources *NodeResources, labels map[string]string, addresses []string) {
	if len(addresses) == 0 {
		return
	}

	req := &RegisterNodeRequest{
		NodeId:    string(p.nodeID),
		Address:   p.selfAddr,
		Resources: resources,
		Labels:    labels,
		GrpcPort:  p.selfPort,
	}

	for _, addr := range addresses {
		func(addr string) {
			conn, err := p.dial(addr)
			if err != nil {
				log.Printf("Failed to connect to seed peer %s: %v", addr, err)
				return
			}
			defer conn.Close()

			client := NewClusterClient(conn)

			// Register with the seed peer
			if _, err := client.RegisterNode(ctx, req); err != nil {
				log.Printf("Failed to register with seed peer %s: %v", addr, err)
				return
			}
			log.Printf("Registered with seed peer %s successfully", addr)

			// Get full cluster state to discover all peers including the seed
			state, err := client.GetClusterState(ctx, &GetClusterStateRequest{})
			if err != nil {
				log.Printf("Failed to get cluster state from %s: %v", addr, err)
				return
			}

			for _, nodeInfo := range state.Nodes {
				peerNodeID := cluster.NodeID(nodeInfo.NodeId)
				peerAddr := nodeInfo.Address
				port := nodeInfo.GrpcPort
				if port == 0 {
					port = 50051
				}
				fullAddr := normalizeGRPCAddress(peerAddr, port)
				if err := p.AddPeer(peerNodeID, fullAddr); err != nil {
					log.Printf("Failed to connect to peer %s at %s: %v", peerNodeID, fullAddr, err)
				}
			}
		}(addr)
	}
}

func (p *ClusterClientPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for id, peer := range p.peers {
		peer.conn.Close()
		delete(p.peers, id)
	}
}
