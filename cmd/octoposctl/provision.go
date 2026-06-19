package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	octopospb "github.com/octopos/octopos/pkg/rpc"
	"github.com/octopos/octopos/pkg/ssi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type provisionConfig struct {
	NodeID        string
	Address       string // SSH address of the remote node
	WgIP          string // WireGuard IP for the new node
	SSHUser       string
	SSHPassword   string
	WgEndpoint    string // WireGuard endpoint (IP:port) for existing nodes
	LocalEndpoint string // WireGuard endpoint (IP:port) for this node
	SeedPeer      string // gRPC seed peer for the new node
	WgListenPort  int
	GrpcPort      int
	BinDir        string
	EBPFEnabled   bool
	FUSEEnabled   bool
	ClusterRoot   string
	SSIRootFS     string
	RequireSSI    bool
	ClusterFSMeta string
	ClusterFSOpts string
}

func (p *provisionConfig) sshCmd(cmd string) *exec.Cmd {
	sshArgs := []string{
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		"-o", "LogLevel=ERROR",
	}
	if p.SSHPassword != "" {
		sshArgs = append(sshArgs, "-o", "PasswordAuthentication=yes")
	}
	sshArgs = append(sshArgs, fmt.Sprintf("%s@%s", p.SSHUser, p.Address), cmd)

	if p.SSHPassword != "" {
		sshArgs = append([]string{"-p", p.SSHPassword}, sshArgs...)
		return exec.Command("sshpass", sshArgs...)
	}
	return exec.Command("ssh", sshArgs...)
}

func (p *provisionConfig) scpCmd(src, dst string) *exec.Cmd {
	scpArgs := []string{
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		"-o", "LogLevel=ERROR",
	}
	scpArgs = append(scpArgs, src, fmt.Sprintf("%s@%s:%s", p.SSHUser, p.Address, dst))

	if p.SSHPassword != "" {
		scpArgs = append([]string{"-p", p.SSHPassword}, scpArgs...)
		return exec.Command("sshpass", scpArgs...)
	}
	return exec.Command("scp", scpArgs...)
}

func (p *provisionConfig) run(title string, cmd *exec.Cmd) error {
	fmt.Printf("  %s... ", title)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		fmt.Println("FAILED")
		fmt.Println(out.String())
		return fmt.Errorf("%s: %w\n%s", title, err, out.String())
	}
	fmt.Println("OK")
	return nil
}

func (p *provisionConfig) runOutput(cmd *exec.Cmd) (string, error) {
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%s", out.String())
	}
	return strings.TrimSpace(out.String()), nil
}

func (p *provisionConfig) installRemoteClusterFSService() error {
	tmpSvc := "/tmp/octopos-clusterfs.service"
	if err := os.WriteFile(tmpSvc, []byte(clusterFSUnitContent(p.ClusterRoot, p.ClusterFSMeta, p.ClusterFSOpts)), 0644); err != nil {
		return fmt.Errorf("write clusterfs service file: %w", err)
	}
	defer os.Remove(tmpSvc)
	if err := p.run("scp clusterfs systemd unit", p.scpCmd(tmpSvc, "/tmp/octopos-clusterfs.service")); err != nil {
		return err
	}
	if err := p.run("install clusterfs systemd unit",
		p.sshCmd("sudo cp /tmp/octopos-clusterfs.service /etc/systemd/system/ && sudo systemctl daemon-reload")); err != nil {
		return err
	}
	return nil
}

func runLocalOutput(cmd *exec.Cmd) (string, error) {
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%s", out.String())
	}
	return strings.TrimSpace(out.String()), nil
}

func localInterfaceIPv4(name string) (string, error) {
	iface, err := net.InterfaceByName(name)
	if err != nil {
		return "", err
	}
	addrs, err := iface.Addrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if ok && ipNet.IP.To4() != nil {
			return ipNet.IP.String(), nil
		}
	}
	return "", fmt.Errorf("no IPv4 address on %s", name)
}

func provisionNode(cfg *provisionConfig) error {
	ssiCfg := ssi.Config{ClusterRoot: cfg.ClusterRoot, RootFS: cfg.SSIRootFS, Required: cfg.RequireSSI}.WithDefaults()
	cfg.ClusterRoot = ssiCfg.ClusterRoot
	cfg.SSIRootFS = ssiCfg.RootFS
	if cfg.RequireSSI {
		cfg.FUSEEnabled = true
	}

	fmt.Printf("Provisioning node %s (WG: %s, SSH: %s@%s)...\n",
		cfg.NodeID, cfg.WgIP, cfg.SSHUser, cfg.Address)

	// 1. Install system dependencies
	fmt.Println("[1/7] Installing system dependencies...")
	packages := []string{"wireguard"}
	if cfg.FUSEEnabled {
		packages = append(packages, "fuse3")
	}
	if cfg.EBPFEnabled {
		packages = append(packages, "clang", "llvm", "bpftool", "linux-headers-$(uname -r)")
	}
	installCMDs := []string{
		"sudo apt update -qq",
		"sudo apt install -y -qq " + strings.Join(packages, " "),
	}
	for _, cmd := range installCMDs {
		if err := cfg.run(cmd, cfg.sshCmd(cmd)); err != nil {
			return err
		}
	}
	if err := cfg.run("configure headless multi-user target", cfg.sshCmd(remoteHeadlessSystemdCommand())); err != nil {
		return err
	}
	if cfg.RequireSSI {
		if cfg.ClusterFSMeta != "" {
			if _, err := cfg.runOutput(cfg.sshCmd("command -v juicefs")); err != nil {
				return fmt.Errorf("juicefs is required on %s for strict SSI clusterfs mounts", cfg.Address)
			}
			if err := cfg.installRemoteClusterFSService(); err != nil {
				return err
			}
		} else if err := cfg.run("verify octopos-clusterfs.service", cfg.sshCmd("systemctl cat octopos-clusterfs.service >/dev/null")); err != nil {
			return fmt.Errorf("strict SSI requires octopos-clusterfs.service on %s; pass --clusterfs-meta or install the service first: %w", cfg.Address, err)
		}
	}

	// 2. Generate WireGuard keys
	fmt.Println("[2/7] Configuring WireGuard...")
	wgKeyCmd := "wg genkey | sudo tee /etc/wireguard/private.key | wg pubkey | sudo tee /etc/wireguard/public.key"
	if err := cfg.run("generate WireGuard keys", cfg.sshCmd(wgKeyCmd)); err != nil {
		return err
	}

	pubKey, err := cfg.runOutput(cfg.sshCmd("sudo cat /etc/wireguard/public.key"))
	if err != nil {
		return fmt.Errorf("read public key: %w", err)
	}
	privKey, err := cfg.runOutput(cfg.sshCmd("sudo cat /etc/wireguard/private.key"))
	if err != nil {
		return fmt.Errorf("read private key: %w", err)
	}

	localPubKey, err := runLocalOutput(exec.Command("sudo", "wg", "show", "wg-octopos", "public-key"))
	if err != nil {
		localPubKey, err = runLocalOutput(exec.Command("sudo", "cat", "/etc/wireguard/public.key"))
		if err != nil {
			return fmt.Errorf("read local WireGuard public key: %w", err)
		}
	}
	localWGIP, err := localInterfaceIPv4("wg-octopos")
	if err != nil {
		return fmt.Errorf("detect local WireGuard IP: %w", err)
	}
	localEndpoint := cfg.LocalEndpoint
	if localEndpoint == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("detect local hostname: %w", err)
		}
		localEndpoint = fmt.Sprintf("%s:%d", hostname, cfg.WgListenPort)
	}
	seedPeer := cfg.SeedPeer
	if seedPeer == "" {
		seedPeer = fmt.Sprintf("%s:%d", localWGIP, cfg.GrpcPort)
	}

	wgConfig := fmt.Sprintf(`[Interface]
Address = %s/24
PrivateKey = %s
ListenPort = %d
SaveConfig = true

[Peer]
PublicKey = %s
Endpoint = %s
AllowedIPs = %s/32
PersistentKeepalive = 25
`, cfg.WgIP, privKey, cfg.WgListenPort, localPubKey, localEndpoint, localWGIP)

	tmpWgCfg := "/tmp/wg-octopos.conf"
	if err := os.WriteFile(tmpWgCfg, []byte(wgConfig), 0600); err != nil {
		return fmt.Errorf("write temp wg config: %w", err)
	}
	defer os.Remove(tmpWgCfg)

	if err := cfg.run("copy WireGuard config", cfg.scpCmd(tmpWgCfg, "/tmp/wg-octopos.conf")); err != nil {
		return err
	}
	if err := cfg.run("install WireGuard config", cfg.sshCmd(
		"sudo cp /tmp/wg-octopos.conf /etc/wireguard/wg-octopos.conf && sudo chmod 600 /etc/wireguard/wg-octopos.conf",
	)); err != nil {
		return err
	}

	// 3. Build binaries locally
	fmt.Println("[3/7] Building binaries...")
	buildTargets := []struct{ bin, pkg string }{
		{"octoposd", "./cmd/octoposd"},
		{"octoposctl", "./cmd/octoposctl"},
		{"octopos-exec", "./cmd/octopos-exec"},
	}
	for _, t := range buildTargets {
		if err := cfg.run(fmt.Sprintf("go build %s", t.bin),
			exec.Command("go", "build", "-o", filepath.Join(cfg.BinDir, t.bin), t.pkg)); err != nil {
			return err
		}
	}

	// 4. Build eBPF programs (optional)
	if cfg.EBPFEnabled {
		fmt.Println("[4/7] Building eBPF programs...")
		for _, dir := range []string{"proc_aggregator", "sys_aggregator", "dev_proxy", "pipe_splice"} {
			target := filepath.Join("ebpf", dir)
			if err := cfg.run(fmt.Sprintf("make -C %s", target),
				exec.Command("make", "-C", target)); err != nil {
				return err
			}
		}
	}

	// 5. Build FUSE daemons (optional)
	if cfg.FUSEEnabled {
		fmt.Println("[5/7] Building FUSE daemons...")
		for _, dir := range []string{"fuse/procfs", "fuse/devfs", "fuse/sysfs"} {
			binName := "octopos-" + filepath.Base(dir)
			if err := cfg.run(fmt.Sprintf("go build %s", binName),
				exec.Command("go", "build", "-o", filepath.Join("bin", binName), fmt.Sprintf("./%s", dir))); err != nil {
				return err
			}
		}
	}

	// 6. Deploy binaries to node
	fmt.Println("[5/7] Deploying binaries...")
	binaries := []string{"octoposd", "octoposctl", "octopos-exec"}
	if cfg.FUSEEnabled {
		binaries = append(binaries, "octopos-procfs", "octopos-devfs", "octopos-sysfs")
	}
	for _, bin := range binaries {
		localPath := filepath.Join(cfg.BinDir, bin)
		if err := cfg.run(fmt.Sprintf("scp %s", bin), cfg.scpCmd(localPath, "/tmp/"+bin)); err != nil {
			return err
		}
	}

	deployCmd := "sudo install -m 0755"
	for _, bin := range binaries {
		deployCmd += fmt.Sprintf(" /tmp/%s", bin)
	}
	deployCmd += " /usr/local/bin/"
	if err := cfg.run("install to /usr/local/bin", cfg.sshCmd(deployCmd)); err != nil {
		return err
	}

	// 7. Configure systemd service
	fmt.Println("[6/7] Configuring systemd service...")
	peerArgs := ""
	if seedPeer != "" {
		peerArgs = fmt.Sprintf(" --peers %s", seedPeer)
	}

	svcContent := fmt.Sprintf(`[Unit]
Description=OctopOS Cluster Daemon
After=network.target wg-quick@wg-octopos.service octopos-clusterfs.service
Wants=wg-quick@wg-octopos.service octopos-clusterfs.service
Requires=octopos-clusterfs.service

[Service]
ExecStart=/usr/local/bin/octoposd --node-id %s --grpc-addr 0.0.0.0:%d --wg-interface wg-octopos --cluster-root %s --ssi-rootfs %s --require-ssi=%t%s
Restart=always
RestartSec=5
User=root

[Install]
WantedBy=multi-user.target
`, cfg.NodeID, cfg.GrpcPort, cfg.ClusterRoot, cfg.SSIRootFS, cfg.RequireSSI, peerArgs)

	tmpSvc := "/tmp/octoposd.service"
	if err := os.WriteFile(tmpSvc, []byte(svcContent), 0644); err != nil {
		return fmt.Errorf("write service file: %w", err)
	}
	defer os.Remove(tmpSvc)

	if err := cfg.run("scp systemd unit", cfg.scpCmd(tmpSvc, "/tmp/octoposd.service")); err != nil {
		return err
	}
	if err := cfg.run("install systemd unit",
		cfg.sshCmd("sudo cp /tmp/octoposd.service /etc/systemd/system/ && sudo systemctl daemon-reload")); err != nil {
		return err
	}

	// 8. Add WireGuard peer on the local node before the new daemon starts.
	fmt.Print("Adding WireGuard peer on local node... ")
	wgAddCmd := exec.Command("sudo", "wg", "set", "wg-octopos",
		"peer", pubKey,
		"endpoint", cfg.WgEndpoint,
		"allowed-ips", cfg.WgIP+"/32",
		"persistent-keepalive", "25",
	)
	if out, err := wgAddCmd.CombinedOutput(); err != nil {
		fmt.Println("FAILED")
		fmt.Printf("  %s\n", strings.TrimSpace(string(out)))
		fmt.Println("  Add the WireGuard peer manually:")
		fmt.Printf("  sudo wg set wg-octopos peer %s endpoint %s allowed-ips %s/32 persistent-keepalive 25\n",
			pubKey, cfg.WgEndpoint, cfg.WgIP)
	} else {
		fmt.Println("OK")
		// Save config to persist after reboot
		exec.Command("sudo", "wg-quick", "save", "wg-octopos").Run()
	}

	// 9. Start services
	if cfg.WgListenPort == 0 {
		cfg.WgListenPort = 51820
	}
	fmt.Println("[7/7] Starting services...")
	startCommands := []string{
		"sudo systemctl enable wg-quick@wg-octopos 2>/dev/null; sudo wg-quick up wg-octopos 2>/dev/null || true",
	}
	if cfg.RequireSSI {
		startCommands = append(startCommands,
			"sudo systemctl enable octopos-clusterfs",
			"sudo systemctl start octopos-clusterfs",
		)
	}
	startCommands = append(startCommands,
		"sudo systemctl enable octoposd",
		"sudo systemctl start octoposd",
	)
	for _, cmd := range startCommands {
		if err := cfg.run(cmd, cfg.sshCmd(cmd)); err != nil {
			return err
		}
	}

	// Wait for octoposd to start
	time.Sleep(2 * time.Second)

	// 9. Register the new node with the cluster via gRPC
	fmt.Print("\nRegistering node with cluster... ")
	if err := registerNodeWithCluster(cfg); err != nil {
		fmt.Println("FAILED")
		fmt.Printf("  %v\n", err)
		fmt.Println("  The node was provisioned but registration failed. Run the following on an existing node:")
		fmt.Printf("  sudo wg set wg-octopos peer %s endpoint %s allowed-ips %s/32 persistent-keepalive 25\n",
			pubKey, cfg.WgEndpoint, cfg.WgIP)
	} else {
		fmt.Println("OK")
	}

	fmt.Println("\n=== Provisioning Complete ===")
	fmt.Printf("Node %s (%s) is ready and registered with the cluster.\n", cfg.NodeID, cfg.WgIP)
	return nil
}

func registerNodeWithCluster(cfg *provisionConfig) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("connect to cluster gRPC (%s): %w", grpcAddr, err)
	}
	defer conn.Close()

	client := octopospb.NewClusterClient(conn)

	// Detect resources on the new node via SSH
	cpu, err := cfg.runOutput(cfg.sshCmd("nproc"))
	if err != nil {
		cpu = "0"
	}
	mem, err := cfg.runOutput(cfg.sshCmd("awk '/MemTotal/ {printf \"%d\", $2 * 1024}' /proc/meminfo"))
	if err != nil {
		mem = "0"
	}
	gpus, err := cfg.runOutput(cfg.sshCmd("if command -v nvidia-smi >/dev/null 2>&1; then nvidia-smi -L 2>/dev/null | wc -l; else echo 0; fi"))
	if err != nil {
		gpus = "0"
	}

	var cpuMillicores int64 = 0
	var memoryBytes int64 = 0
	var gpuCount int64 = 0
	fmt.Sscanf(cpu, "%d", &cpuMillicores)
	fmt.Sscanf(mem, "%d", &memoryBytes)
	fmt.Sscanf(gpus, "%d", &gpuCount)
	cpuMillicores *= 1000 // nproc gives cores, convert to millicores

	labels := map[string]string{"role": "compute"}
	if cfg.RequireSSI {
		labels[ssi.LabelReady] = "true"
		labels[ssi.LabelClusterRoot] = cfg.ClusterRoot
		labels[ssi.LabelRootFS] = cfg.SSIRootFS
	}

	resp, err := client.RegisterNode(ctx, &octopospb.RegisterNodeRequest{
		NodeId:  cfg.NodeID,
		Address: cfg.WgIP,
		Resources: &octopospb.NodeResources{
			CpuMillicores: cpuMillicores,
			MemoryBytes:   memoryBytes,
			GpuCount:      int32(gpuCount),
		},
		Labels: labels,
	})
	if err != nil {
		return fmt.Errorf("RegisterNode RPC: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("RegisterNode failed: %s", resp.Error)
	}
	return nil
}

func autoAssignWGIP() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return "", fmt.Errorf("connect to cluster (%s): %w", grpcAddr, err)
	}
	defer conn.Close()

	client := octopospb.NewClusterClient(conn)
	state, err := client.GetClusterState(ctx, &octopospb.GetClusterStateRequest{})
	if err != nil {
		return "", fmt.Errorf("get cluster state: %w", err)
	}

	used := make(map[string]bool)
	for _, n := range state.Nodes {
		if n.Address != "" {
			used[n.Address] = true
		}
	}

	for i := 2; i <= 254; i++ {
		ip := fmt.Sprintf("10.0.0.%d", i)
		if !used[ip] {
			return ip, nil
		}
	}
	return "", fmt.Errorf("no available IPs in 10.0.0.0/24")
}
