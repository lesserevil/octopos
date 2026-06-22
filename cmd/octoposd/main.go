package main

import (
	"context"
	"errors"
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
	"github.com/octopos/octopos/pkg/clusterconfig"
	octoebpf "github.com/octopos/octopos/pkg/ebpf"
	"github.com/octopos/octopos/pkg/nvidia"
	"github.com/octopos/octopos/pkg/remotechild"
	"github.com/octopos/octopos/pkg/resources"
	"github.com/octopos/octopos/pkg/rpc"
	"github.com/octopos/octopos/pkg/scheduler"
	"github.com/octopos/octopos/pkg/ssi"
	"github.com/octopos/octopos/pkg/tracker"
	"github.com/octopos/octopos/pkg/vfio"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

var (
	configFile    = flag.String("config", "/etc/octopos/octoposd.yaml", "Config file path")
	nodeID        = flag.String("node-id", "", "Node ID (default: hostname)")
	grpcAddr      = flag.String("grpc-addr", "0.0.0.0:50051", "gRPC listen address")
	wgInterface   = flag.String("wg-interface", "wg-octopos", "WireGuard interface")
	peers         = flag.String("peers", "", "Comma-separated seed peer addresses (e.g., 10.0.0.2:50051,10.0.0.3:50051)")
	clusterRoot   = flag.String("cluster-root", ssi.DefaultClusterRoot, "JuiceFS cluster filesystem mount point")
	ssiRootFS     = flag.String("ssi-rootfs", "", "SSI root filesystem path (default: <cluster-root>)")
	ssiMountBase  = flag.String("ssi-mount-base", ssi.DefaultMountBase, "Base directory for SSI virtual filesystem mounts")
	ssiExecutor   = flag.String("ssi-executor", ssi.DefaultExecutor, "Privileged SSI command launcher")
	requireSSI    = flag.Bool("require-ssi", true, "Require a mounted cluster filesystem and bootstrapped SSI rootfs before serving jobs")
	childSocket   = flag.String("child-socket", remotechild.DefaultSocketPath, "Unix socket exposed to exec namespaces for local child process control")
	childState    = flag.String("remote-child-state", "/var/lib/octopos/remote-children.json", "Persistent remote-child lifecycle state file")
	childLease    = flag.Duration("remote-child-lease-timeout", 2*time.Minute, "How long to keep polling an unobservable remote child before failing its parent-side lease")
	childTokenTTL = flag.Duration("remote-child-token-ttl", 24*time.Hour, "How long a parent exec's remote-child token remains valid")
	vfioState     = flag.String("vfio-allocation-state", "/var/lib/octopos/vfio-allocations.json", "Persistent VFIO allocation state file")
)

type Config struct {
	NodeID             string                     `yaml:"node_id"`
	GRPCAddr           string                     `yaml:"grpc_addr"`
	WGInterface        string                     `yaml:"wg_interface"`
	RedisAddrs         string                     `yaml:"redis_addrs"`
	JuiceFSMount       string                     `yaml:"juicefs_mount"`
	SSIRootFS          string                     `yaml:"ssi_rootfs"`
	SSIMountBase       string                     `yaml:"ssi_mount_base"`
	SSIExecutor        string                     `yaml:"ssi_executor"`
	RequireSSI         bool                       `yaml:"require_ssi"`
	ChildSocket        string                     `yaml:"child_socket"`
	ChildState         string                     `yaml:"remote_child_state"`
	ChildLease         time.Duration              `yaml:"remote_child_lease_timeout"`
	ChildTokenTTL      time.Duration              `yaml:"remote_child_token_ttl"`
	VFIOState          string                     `yaml:"vfio_allocation_state"`
	Peers              string                     `yaml:"peers"`
	VFIOEnabled        bool                       `yaml:"vfio_enabled"`
	VFIOAllowedGroups  []int                      `yaml:"vfio_allowed_groups"`
	VFIODeniedGroups   []int                      `yaml:"vfio_denied_groups"`
	VFIOAllowedClasses []string                   `yaml:"vfio_allowed_classes"`
	VFIOAllowedVendors []string                   `yaml:"vfio_allowed_vendors"`
	VFIODriverRebind   bool                       `yaml:"vfio_driver_rebind"`
	ExecDefaults       clusterconfig.ExecDefaults `yaml:"exec_defaults"`
}

type Server struct {
	config        *Config
	nodeInfo      *cluster.NodeInfo
	grpcPort      int32
	scheduler     *scheduler.Scheduler
	tracker       *tracker.Tracker
	detector      *resources.Detector
	clientPool    *rpc.ClusterClientPool
	clusterServer *rpc.ClusterServerImpl
	ebpfLoader    *octoebpf.Loader
	grpcServer    *grpc.Server
	healthServer  *health.Server
	localSocket   string
	ssiProcs      []*exec.Cmd
	ctx           context.Context
	cancel        context.CancelFunc
}

func main() {
	flag.Parse()

	// Load config
	cfg := &Config{
		NodeID:        *nodeID,
		GRPCAddr:      *grpcAddr,
		WGInterface:   *wgInterface,
		RedisAddrs:    "10.0.0.1:6379,10.0.0.2:6379,10.0.0.3:6379",
		JuiceFSMount:  *clusterRoot,
		SSIRootFS:     *ssiRootFS,
		SSIMountBase:  *ssiMountBase,
		SSIExecutor:   *ssiExecutor,
		RequireSSI:    *requireSSI,
		ChildSocket:   *childSocket,
		ChildState:    *childState,
		ChildLease:    *childLease,
		ChildTokenTTL: *childTokenTTL,
		VFIOState:     *vfioState,
		Peers:         *peers,
		VFIOEnabled:   true,
		ExecDefaults:  clusterconfig.DefaultExecDefaults(),
	}

	if *configFile != "" {
		if err := loadConfigFile(*configFile, cfg); err != nil {
			log.Fatalf("load config: %v", err)
		}
		applyExplicitConfigFlags(cfg)
	}
	cfg.ExecDefaults = cfg.ExecDefaults.WithDefaults()

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

func loadConfigFile(path string, cfg *Config) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	return nil
}

func applyExplicitConfigFlags(cfg *Config) {
	flag.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "node-id":
			cfg.NodeID = *nodeID
		case "grpc-addr":
			cfg.GRPCAddr = *grpcAddr
		case "wg-interface":
			cfg.WGInterface = *wgInterface
		case "peers":
			cfg.Peers = *peers
		case "cluster-root":
			cfg.JuiceFSMount = *clusterRoot
		case "ssi-rootfs":
			cfg.SSIRootFS = *ssiRootFS
		case "ssi-mount-base":
			cfg.SSIMountBase = *ssiMountBase
		case "ssi-executor":
			cfg.SSIExecutor = *ssiExecutor
		case "require-ssi":
			cfg.RequireSSI = *requireSSI
		case "child-socket":
			cfg.ChildSocket = *childSocket
		case "remote-child-state":
			cfg.ChildState = *childState
		case "remote-child-lease-timeout":
			cfg.ChildLease = *childLease
		case "remote-child-token-ttl":
			cfg.ChildTokenTTL = *childTokenTTL
		case "vfio-allocation-state":
			cfg.VFIOState = *vfioState
		}
	})
}

func (s *Server) init() error {
	// Detect resources
	s.detector = resources.NewDetector("", "")
	s.detector.SetVFIOPolicy(vfio.Policy{
		Enabled:        s.config.VFIOEnabled,
		AllowedGroups:  s.config.VFIOAllowedGroups,
		DeniedGroups:   s.config.VFIODeniedGroups,
		AllowedClasses: s.config.VFIOAllowedClasses,
		AllowedVendors: s.config.VFIOAllowedVendors,
	})
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
	if resSpec.GPUCount > 0 {
		if err := s.ensureNVIDIARuntime(ssiCfg); err != nil {
			return err
		}
		labels["octopos.io/nvidia"] = "true"
		labels["octopos.io/nvidia-gpus"] = fmt.Sprint(resSpec.GPUCount)
		if version := nvidia.DriverVersion(); version != "" {
			labels["octopos.io/nvidia-driver-version"] = version
		}
	}
	if len(resSpec.VFIOGroups) > 0 {
		labels["octopos.io/vfio"] = "true"
		labels["octopos.io/vfio-groups"] = fmt.Sprint(len(resSpec.VFIOGroups))
	}
	s.nodeInfo = &cluster.NodeInfo{
		ID:         cluster.NodeID(s.config.NodeID),
		Address:    wgIP,
		State:      cluster.NodeStateActive,
		Resources:  *resSpec,
		VFIOGroups: resSpec.VFIOGroups,
		Labels:     labels,
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

func (s *Server) ensureNVIDIARuntime(ssiCfg ssi.Config) error {
	if err := nvidia.EnsureProjection(nvidia.DefaultProjectionRoot); err != nil {
		return fmt.Errorf("prepare NVIDIA projection: %w", err)
	}
	if s.config.RequireSSI {
		if err := ssi.Validate(ssiCfg); err != nil {
			return fmt.Errorf("SSI validation failed: %w", err)
		}
		if err := nvidia.EnsureProfile(ssiCfg.RootFS); err != nil {
			return fmt.Errorf("install NVIDIA profile hook: %w", err)
		}
	}
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
			PciDevices:    pciDevicesToProto(s.nodeInfo.Resources.PCIDevices),
		}
		s.clientPool.RegisterWithPeers(s.ctx, resources, s.nodeInfo.Labels, addresses)

		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func pciDevicesToProto(devices []cluster.PCIDevice) []*rpc.PCIDevice {
	out := make([]*rpc.PCIDevice, 0, len(devices))
	for _, dev := range devices {
		out = append(out, &rpc.PCIDevice{
			Address:    dev.Address,
			VendorId:   dev.VendorID,
			DeviceId:   dev.DeviceID,
			Class:      dev.Class,
			Driver:     dev.Driver,
			IommuGroup: int32(dev.IOMMUGroup),
			VfioGroup:  int32(dev.VFIOGroup),
		})
	}
	return out
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
	tcpLis, err := net.Listen("tcp", s.config.GRPCAddr)
	if err != nil {
		return err
	}

	s.grpcServer = grpc.NewServer(
		grpc.UnaryInterceptor(s.authorizeLocalSocketUnary),
		grpc.StreamInterceptor(s.authorizeLocalSocketStream),
	)
	s.healthServer = health.NewServer()
	grpc_health_v1.RegisterHealthServer(s.grpcServer, s.healthServer)
	s.healthServer.SetServingStatus("octopos.Cluster", grpc_health_v1.HealthCheckResponse_SERVING)

	// Enable reflection for grpcurl
	reflection.Register(s.grpcServer)

	// Register Cluster service
	s.clusterServer = rpc.RegisterClusterServerImplWithOptions(s.grpcServer, s.nodeInfo.ID, s.scheduler, s.tracker, s.grpcPort, s.clientPool, rpc.ServerOptions{
		SSI:                     s.ssiConfig(),
		RemoteChildStorePath:    s.config.ChildState,
		RemoteChildLeaseTimeout: s.config.ChildLease,
		RemoteChildTokenTTL:     s.config.ChildTokenTTL,
		VFIOAllocationStorePath: s.config.VFIOState,
	})
	go s.pruneRemoteChildRecords()
	go s.recoverRemoteChildRecords()

	s.serveGRPC(tcpLis, "tcp "+s.config.GRPCAddr)
	if s.config.ChildSocket != "" {
		unixLis, err := listenUnixSocket(s.config.ChildSocket)
		if err != nil {
			_ = tcpLis.Close()
			s.grpcServer.Stop()
			return err
		}
		s.localSocket = s.config.ChildSocket
		s.serveGRPC(unixLis, "unix "+s.config.ChildSocket)
	}

	return nil
}

func (s *Server) pruneRemoteChildRecords() {
	if s.clusterServer == nil {
		return
	}
	const retention = 24 * time.Hour
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if pruned := s.clusterServer.PruneRemoteChildren(retention); pruned > 0 {
				log.Printf("Pruned %d terminal remote-child lifecycle records older than %s", pruned, retention)
			}
		}
	}
}

func (s *Server) recoverRemoteChildRecords() {
	if s.clusterServer == nil {
		return
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		stats := s.clusterServer.RecoverRemoteChildren(s.ctx)
		if stats.HasChanges() {
			log.Printf(
				"Remote-child recovery: recovering=%d running=%d stopped=%d completed=%d failed=%d orphaned=%d unobservable=%d",
				stats.Recovering,
				stats.Running,
				stats.Stopped,
				stats.Completed,
				stats.Failed,
				stats.Orphaned,
				stats.Unobservable,
			)
		}
		if stats.Recovering == 0 {
			return
		}
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) authorizeLocalSocketUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := authorizeLocalSocketRPC(ctx, info.FullMethod); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func (s *Server) authorizeLocalSocketStream(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := authorizeLocalSocketRPC(stream.Context(), info.FullMethod); err != nil {
		return err
	}
	return handler(srv, stream)
}

func authorizeLocalSocketRPC(ctx context.Context, method string) error {
	cred, ok := localUnixPeerCred(ctx)
	if !ok {
		return nil
	}
	switch method {
	case rpc.Cluster_SpawnRemoteChild_FullMethodName,
		rpc.Cluster_RemoteChildLaunchStream_FullMethodName,
		rpc.Cluster_RemoteChildExecute_FullMethodName,
		rpc.Cluster_RemoteChildStream_FullMethodName:
	default:
		return status.Errorf(codes.PermissionDenied, "method %s is not available on the local child socket", method)
	}
	if cred.uid != 0 {
		return status.Errorf(codes.PermissionDenied, "local child socket requires uid 0, got uid %d", cred.uid)
	}
	return nil
}

func localUnixPeerCred(ctx context.Context) (unixPeerCred, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return unixPeerCred{}, false
	}
	addr, ok := p.Addr.(unixPeerAddr)
	if !ok {
		return unixPeerCred{}, false
	}
	return addr.cred, true
}

func (s *Server) serveGRPC(lis net.Listener, label string) {
	go func() {
		log.Printf("gRPC server listening on %s", label)
		if err := s.grpcServer.Serve(lis); err != nil {
			log.Printf("gRPC server error on %s: %v", label, err)
		}
	}()
}

func listenUnixSocket(path string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("create %s: %w", filepath.Dir(path), err)
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("refusing to replace non-socket %s", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("remove stale socket %s: %w", path, err)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("stat socket %s: %w", path, err)
	}
	lis, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("listen unix %s: %w", path, err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		_ = lis.Close()
		return nil, fmt.Errorf("chmod unix socket %s: %w", path, err)
	}
	return &peerCredUnixListener{Listener: lis}, nil
}

type unixPeerCred struct {
	pid int32
	uid uint32
	gid uint32
}

type unixPeerAddr struct {
	net.Addr
	cred unixPeerCred
}

func (a unixPeerAddr) Network() string {
	return "unix"
}

func (a unixPeerAddr) String() string {
	if a.Addr == nil {
		return fmt.Sprintf("unix pid=%d uid=%d gid=%d", a.cred.pid, a.cred.uid, a.cred.gid)
	}
	return fmt.Sprintf("%s pid=%d uid=%d gid=%d", a.Addr.String(), a.cred.pid, a.cred.uid, a.cred.gid)
}

type peerCredConn struct {
	net.Conn
	addr unixPeerAddr
}

func (c *peerCredConn) RemoteAddr() net.Addr {
	return c.addr
}

type peerCredUnixListener struct {
	net.Listener
}

func (l *peerCredUnixListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	cred, err := peerCred(conn)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &peerCredConn{
		Conn: conn,
		addr: unixPeerAddr{Addr: conn.RemoteAddr(), cred: cred},
	}, nil
}

func peerCred(conn net.Conn) (unixPeerCred, error) {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return unixPeerCred{}, fmt.Errorf("local child socket accepted non-Unix connection %T", conn)
	}
	rawConn, err := unixConn.SyscallConn()
	if err != nil {
		return unixPeerCred{}, fmt.Errorf("get raw Unix connection: %w", err)
	}
	var cred *unix.Ucred
	var controlErr error
	if err := rawConn.Control(func(fd uintptr) {
		cred, controlErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return unixPeerCred{}, fmt.Errorf("inspect Unix peer credentials: %w", err)
	}
	if controlErr != nil {
		return unixPeerCred{}, fmt.Errorf("inspect Unix peer credentials: %w", controlErr)
	}
	if cred == nil {
		return unixPeerCred{}, fmt.Errorf("inspect Unix peer credentials: no credentials returned")
	}
	return unixPeerCred{pid: cred.Pid, uid: cred.Uid, gid: cred.Gid}, nil
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
			if resSpec.GPUCount > 0 {
				if err := s.ensureNVIDIARuntime(s.ssiConfig()); err != nil {
					log.Printf("NVIDIA runtime refresh failed: %v", err)
					continue
				}
				s.nodeInfo.Labels["octopos.io/nvidia"] = "true"
				s.nodeInfo.Labels["octopos.io/nvidia-gpus"] = fmt.Sprint(resSpec.GPUCount)
				if version := nvidia.DriverVersion(); version != "" {
					s.nodeInfo.Labels["octopos.io/nvidia-driver-version"] = version
				}
			} else {
				delete(s.nodeInfo.Labels, "octopos.io/nvidia")
				delete(s.nodeInfo.Labels, "octopos.io/nvidia-gpus")
				delete(s.nodeInfo.Labels, "octopos.io/nvidia-driver-version")
			}
			if len(resSpec.VFIOGroups) > 0 {
				s.nodeInfo.Labels["octopos.io/vfio"] = "true"
				s.nodeInfo.Labels["octopos.io/vfio-groups"] = fmt.Sprint(len(resSpec.VFIOGroups))
			} else {
				delete(s.nodeInfo.Labels, "octopos.io/vfio")
				delete(s.nodeInfo.Labels, "octopos.io/vfio-groups")
			}
			s.nodeInfo.Resources = *resSpec
			s.nodeInfo.VFIOGroups = resSpec.VFIOGroups
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
		s.stopGRPC(5 * time.Second)
	}
	if s.localSocket != "" {
		_ = os.Remove(s.localSocket)
	}
	if s.healthServer != nil {
		s.healthServer.Shutdown()
	}
	s.cancel()
}

func (s *Server) stopGRPC(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		log.Printf("gRPC graceful stop timed out after %s; forcing stop", timeout)
		s.grpcServer.Stop()
		<-done
	}
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
