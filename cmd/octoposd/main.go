package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/octopos/octopos/pkg/cluster"
	octoebpf "github.com/octopos/octopos/pkg/ebpf"
	"github.com/octopos/octopos/pkg/resources"
	"github.com/octopos/octopos/pkg/rpc"
	"github.com/octopos/octopos/pkg/scheduler"
	"github.com/octopos/octopos/pkg/tracker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

var (
	configFile  = flag.String("config", "/etc/octopos/octoposd.yaml", "Config file path")
	nodeID      = flag.String("node-id", "", "Node ID (default: hostname)")
	grpcAddr    = flag.String("grpc-addr", "0.0.0.0:50051", "gRPC listen address")
	wgInterface = flag.String("wg-interface", "wg-octopos", "WireGuard interface")
	peers       = flag.String("peers", "", "Comma-separated seed peer addresses (e.g., 10.0.0.2:50051,10.0.0.3:50051)")
)

type Config struct {
	NodeID       string `yaml:"node_id"`
	GRPCAddr     string `yaml:"grpc_addr"`
	WGInterface  string `yaml:"wg_interface"`
	RedisAddrs   string `yaml:"redis_addrs"`
	JuiceFSMount string `yaml:"juicefs_mount"`
	VFIOEnabled  bool   `yaml:"vfio_enabled"`
}

type Server struct {
	config       *Config
	nodeInfo     *cluster.NodeInfo
	grpcPort     int32
	scheduler    *scheduler.Scheduler
	tracker      *tracker.Tracker
	detector     *resources.Detector
	clientPool   *rpc.ClusterClientPool
	ebpfLoader   *octoebpf.Loader
	grpcServer   *grpc.Server
	healthServer *health.Server
	ctx          context.Context
	cancel       context.CancelFunc
}

func main() {
	flag.Parse()

	// Load config
	cfg := &Config{
		NodeID:       *nodeID,
		GRPCAddr:     *grpcAddr,
		WGInterface:  *wgInterface,
		RedisAddrs:   "10.0.0.1:6379,10.0.0.2:6379,10.0.0.3:6379",
		JuiceFSMount: "/cluster",
		VFIOEnabled:  true,
	}

	if *configFile != "" {
		// TODO: Load from YAML
	}

	if cfg.NodeID == "" {
		hostname, _ := os.Hostname()
		cfg.NodeID = hostname
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &Server{
		config: cfg,
		ctx:    ctx,
		cancel: cancel,
	}

	// Initialize components
	if err := s.init(); err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}

	// Start gRPC server
	if err := s.startGRPC(); err != nil {
		log.Fatalf("Failed to start gRPC: %v", err)
	}

	// Start background tasks
	go s.heartbeatLoop()
	go s.resourceUpdateLoop()

	// Wait for shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	s.shutdown()
}

func (s *Server) init() error {
	// Detect resources
	s.detector = resources.NewDetector("", "")
	resSpec, err := s.detector.DetectAll()
	if err != nil {
		return fmt.Errorf("resource detection failed: %w", err)
	}

	// Parse gRPC port from listen address
	_, portStr, err := net.SplitHostPort(s.config.GRPCAddr)
	if err != nil {
		return fmt.Errorf("invalid grpc-addr %s: %w", s.config.GRPCAddr, err)
	}
	port, err := net.LookupPort("tcp", portStr)
	if err != nil {
		return fmt.Errorf("invalid port %s: %w", portStr, err)
	}
	s.grpcPort = int32(port)

	// Create node info
	wgIP := s.getWGIP()
	s.nodeInfo = &cluster.NodeInfo{
		ID:        cluster.NodeID(s.config.NodeID),
		Address:   wgIP,
		State:     cluster.NodeStateActive,
		Resources: *resSpec,
		Labels:    map[string]string{"role": "compute"},
	}

	// Initialize scheduler and tracker
	s.scheduler = scheduler.NewScheduler(&scheduler.BinPackPolicy{})
	s.scheduler.AddNode(s.nodeInfo)
	s.tracker = tracker.NewTracker()

	// Initialize client pool
	s.clientPool = rpc.NewClusterClientPool(s.nodeInfo.ID, wgIP, s.grpcPort)

	// Initialize eBPF loader (optional, non-fatal on failure)
	if loader, err := octoebpf.SetupDefault(s.ctx); err != nil {
		log.Printf("eBPF loader skipped (run as root to enable): %v", err)
	} else {
		s.ebpfLoader = loader
		log.Println("eBPF programs loaded and attached")
	}

	log.Printf("Node %s initialized: CPU=%d, Mem=%d, GPUs=%d",
		s.nodeInfo.ID, s.nodeInfo.Resources.CPU, s.nodeInfo.Resources.Memory, s.nodeInfo.Resources.GPUCount)

	// Connect to seed peers and register
	if *peers != "" {
		go s.connectToPeers(*peers)
	}

	return nil
}

func (s *Server) connectToPeers(peerList string) {
	// Give gRPC server a moment to start
	time.Sleep(100 * time.Millisecond)

	addresses := strings.Split(peerList, ",")
	for i := range addresses {
		addresses[i] = strings.TrimSpace(addresses[i])
	}

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		resources := &rpc.NodeResources{
			CpuMillicores: s.nodeInfo.Resources.CPU,
			MemoryBytes:   s.nodeInfo.Resources.Memory,
			GpuCount:      int32(s.nodeInfo.Resources.GPUCount),
			NumaNodes:     int32(s.nodeInfo.Resources.NUMANodes),
		}
		s.clientPool.RegisterWithPeers(s.ctx, resources, s.nodeInfo.Labels, addresses)

		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) getWGIP() string {
	iface, err := net.InterfaceByName(s.config.WGInterface)
	if err != nil {
		log.Printf("WireGuard interface %s not found, using default address: %v", s.config.WGInterface, err)
		return "10.0.0.1"
	}
	addrs, err := iface.Addrs()
	if err != nil || len(addrs) == 0 {
		log.Printf("No addresses on %s, using default: %v", s.config.WGInterface, err)
		return "10.0.0.1"
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	log.Printf("No IPv4 address on %s, using default", s.config.WGInterface)
	return "10.0.0.1"
}

func (s *Server) startGRPC() error {
	lis, err := net.Listen("tcp", s.config.GRPCAddr)
	if err != nil {
		return err
	}

	s.grpcServer = grpc.NewServer()
	s.healthServer = health.NewServer()
	grpc_health_v1.RegisterHealthServer(s.grpcServer, s.healthServer)
	s.healthServer.SetServingStatus("octopos.Cluster", grpc_health_v1.HealthCheckResponse_SERVING)

	// Enable reflection for grpcurl
	reflection.Register(s.grpcServer)

	// Register Cluster service
	rpc.RegisterClusterServerImpl(s.grpcServer, s.nodeInfo.ID, s.scheduler, s.tracker, s.grpcPort, s.clientPool)

	go func() {
		log.Printf("gRPC server listening on %s", s.config.GRPCAddr)
		if err := s.grpcServer.Serve(lis); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	return nil
}

func (s *Server) heartbeatLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.nodeInfo.LastHeartbeat = time.Now()

			// Send heartbeat to all peers
			s.clientPool.Broadcast(s.ctx, func(ctx context.Context, client rpc.ClusterClient, nodeID cluster.NodeID) error {
				_, err := client.Heartbeat(ctx, &rpc.HeartbeatRequest{
					NodeId: string(s.nodeInfo.ID),
					Allocated: &rpc.NodeResources{
						CpuMillicores: s.nodeInfo.Allocated.CPU,
						MemoryBytes:   s.nodeInfo.Allocated.Memory,
						GpuCount:      int32(s.nodeInfo.Allocated.GPUCount),
					},
				})
				return err
			})
		}
	}
}

func (s *Server) resourceUpdateLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			resSpec, err := s.detector.DetectAll()
			if err != nil {
				log.Printf("Resource detection failed: %v", err)
				continue
			}
			s.nodeInfo.Resources = *resSpec
		}
	}
}

func (s *Server) shutdown() {
	if s.clientPool != nil {
		s.clientPool.Close()
	}
	if s.ebpfLoader != nil {
		s.ebpfLoader.Close()
	}
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	if s.healthServer != nil {
		s.healthServer.Shutdown()
	}
	s.cancel()
}
