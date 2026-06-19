package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/octopos/octopos/pkg/cluster"
	octoebpf "github.com/octopos/octopos/pkg/ebpf"
	"github.com/octopos/octopos/pkg/resources"
	"github.com/octopos/octopos/pkg/rpc"
	"github.com/octopos/octopos/pkg/scheduler"
	"github.com/octopos/octopos/pkg/ssi"
	"github.com/octopos/octopos/pkg/tracker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

var (
	configFile   = flag.String("config", "/etc/octopos/octoposd.yaml", "Config file path")
	nodeID       = flag.String("node-id", "", "Node ID (default: hostname)")
	grpcAddr     = flag.String("grpc-addr", "0.0.0.0:50051", "gRPC listen address")
	wgInterface  = flag.String("wg-interface", "wg-octopos", "WireGuard interface")
	peers        = flag.String("peers", "", "Comma-separated seed peer addresses (e.g., 10.0.0.2:50051,10.0.0.3:50051)")
	clusterRoot  = flag.String("cluster-root", ssi.DefaultClusterRoot, "JuiceFS cluster filesystem mount point")
	ssiRootFS    = flag.String("ssi-rootfs", "", "SSI root filesystem path (default: <cluster-root>)")
	ssiMountBase = flag.String("ssi-mount-base", ssi.DefaultMountBase, "Base directory for SSI virtual filesystem mounts")
	ssiExecutor  = flag.String("ssi-executor", ssi.DefaultExecutor, "Privileged SSI command launcher")
	requireSSI   = flag.Bool("require-ssi", true, "Require a mounted cluster filesystem and bootstrapped SSI rootfs before serving jobs")
)

type Config struct {
	NodeID       string `yaml:"node_id"`
	GRPCAddr     string `yaml:"grpc_addr"`
	WGInterface  string `yaml:"wg_interface"`
	RedisAddrs   string `yaml:"redis_addrs"`
	JuiceFSMount string `yaml:"juicefs_mount"`
	SSIRootFS    string `yaml:"ssi_rootfs"`
	SSIMountBase string `yaml:"ssi_mount_base"`
	SSIExecutor  string `yaml:"ssi_executor"`
	RequireSSI   bool   `yaml:"require_ssi"`
	Peers        string `yaml:"peers"`
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
	ssiProcs     []*exec.Cmd
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
		JuiceFSMount: *clusterRoot,
		SSIRootFS:    *ssiRootFS,
		SSIMountBase: *ssiMountBase,
		SSIExecutor:  *ssiExecutor,
		RequireSSI:   *requireSSI,
		Peers:        *peers,
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
	if err := s.startSSIVirtualFS(); err != nil {
		log.Fatalf("Failed to start SSI virtual filesystems: %v", err)
	}
	if s.config.Peers != "" {
		go s.connectToPeers(s.config.Peers)
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
	labels := map[string]string{"role": "compute"}
	ssiCfg := s.ssiConfig()
	if s.config.RequireSSI {
		if err := ssi.Validate(ssiCfg); err != nil {
			return fmt.Errorf("SSI validation failed: %w", err)
		}
		labels = ssi.MergeLabels(labels, ssiCfg.Labels())
	}
	s.nodeInfo = &cluster.NodeInfo{
		ID:        cluster.NodeID(s.config.NodeID),
		Address:   wgIP,
		State:     cluster.NodeStateActive,
		Resources: *resSpec,
		Labels:    labels,
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

	return nil
}

func (s *Server) ssiConfig() ssi.Config {
	return ssi.Config{
		ClusterRoot:  s.config.JuiceFSMount,
		RootFS:       s.config.SSIRootFS,
		MountBase:    s.config.SSIMountBase,
		Executor:     s.config.SSIExecutor,
		RequireMount: true,
		Required:     s.config.RequireSSI,
	}.WithDefaults()
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
	rpc.RegisterClusterServerImplWithOptions(s.grpcServer, s.nodeInfo.ID, s.scheduler, s.tracker, s.grpcPort, s.clientPool, rpc.ServerOptions{
		SSI: s.ssiConfig(),
	})

	go func() {
		log.Printf("gRPC server listening on %s", s.config.GRPCAddr)
		if err := s.grpcServer.Serve(lis); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	return nil
}

func (s *Server) startSSIVirtualFS() error {
	if !s.config.RequireSSI {
		return nil
	}
	cfg := s.ssiConfig()
	if err := os.MkdirAll(cfg.MountBase, 0755); err != nil {
		return fmt.Errorf("create SSI mount base: %w", err)
	}

	mounts := []struct {
		name string
		bin  string
		args []string
	}{
		{"proc", "octopos-procfs", []string{"--grpc-addr", net.JoinHostPort("127.0.0.1", fmt.Sprint(s.grpcPort))}},
		{"sys", "octopos-sysfs", []string{"--grpc-addr", net.JoinHostPort("127.0.0.1", fmt.Sprint(s.grpcPort))}},
	}
	for _, item := range mounts {
		mountPoint := filepath.Join(cfg.MountBase, item.name)
		if mounted, _ := ssi.IsMountPoint(mountPoint); mounted {
			if isUsableMount(mountPoint) {
				continue
			}
			log.Printf("Unmounting stale SSI virtual /%s at %s", item.name, mountPoint)
			if err := lazyUnmount(mountPoint); err != nil {
				return fmt.Errorf("unmount stale %s mount at %s: %w", item.name, mountPoint, err)
			}
		}
		if err := os.MkdirAll(mountPoint, 0755); err != nil {
			return fmt.Errorf("create %s mount point: %w", item.name, err)
		}
		binPath, err := exec.LookPath(item.bin)
		if err != nil {
			return fmt.Errorf("%s is required for SSI mode: %w", item.bin, err)
		}
		args := []string{"--mount", mountPoint}
		args = append(args, item.args...)
		cmd := exec.CommandContext(s.ctx, binPath, args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("start %s: %w", item.bin, err)
		}
		s.ssiProcs = append(s.ssiProcs, cmd)
		if err := waitForUsableMount(mountPoint, 3*time.Second); err != nil {
			return fmt.Errorf("%s did not mount at %s: %w", item.bin, mountPoint, err)
		}
		log.Printf("SSI virtual /%s mounted at %s", item.name, mountPoint)
	}
	return nil
}

func waitForUsableMount(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if mounted, _ := ssi.IsMountPoint(path); mounted && isUsableMount(path) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout")
}

func isUsableMount(path string) bool {
	_, err := os.ReadDir(path)
	return err == nil
}

func lazyUnmount(path string) error {
	return exec.Command("umount", "-l", path).Run()
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
	s.stopSSIVirtualFS()
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

func (s *Server) stopSSIVirtualFS() {
	for _, cmd := range s.ssiProcs {
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			done := make(chan struct{})
			go func(cmd *exec.Cmd) {
				_ = cmd.Wait()
				close(done)
			}(cmd)
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				_ = cmd.Process.Kill()
				<-done
			}
		}
	}
	if !s.config.RequireSSI {
		return
	}
	base := s.ssiConfig().MountBase
	for _, name := range []string{"proc", "dev", "sys"} {
		mountPoint := filepath.Join(base, name)
		if mounted, _ := ssi.IsMountPoint(mountPoint); mounted {
			_ = lazyUnmount(mountPoint)
		}
	}
}
