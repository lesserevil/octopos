package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/gliderlabs/ssh"
	gocryptossh "golang.org/x/crypto/ssh"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	octopospb "github.com/octopos/octopos/pkg/rpc"
)

var (
	vipAddr        = flag.String("vip", "10.0.0.100:22", "VIP address to listen on (IP:port)")
	grpcAddr       = flag.String("grpc-addr", "127.0.0.1:50051", "octoposd gRPC address")
	nodeID         = flag.String("node-id", "", "This node's ID (for gateway identity)")
	mountBase      = flag.String("mount-base", "/tmp/octopos", "Base directory for FUSE mounts")
	hostKeyFile    = flag.String("host-key", "/etc/ssh/ssh_host_ed25519_key", "SSH host key file")
	authorizedKeys = flag.String("authorized-keys", "", "Authorized keys file (default: ~/.ssh/authorized_keys)")
)

type Gateway struct {
	grpcAddr      string
	nodeID        string
	mountBase     string
	sshServer     *ssh.Server
	grpcConn      *grpc.ClientConn
	clusterClient octopospb.ClusterClient
	mu            sync.Mutex
	sessions      map[string]*ClusterSession
}

type ClusterSession struct {
	SessionID   string
	User        string
	MountPoints map[string]string
	CgroupPath  string
	NetNSPath   string
	Pty         *os.File
	Cmd         *exec.Cmd
	Cleanup     func()
}

func main() {
	flag.Parse()

	if *nodeID == "" {
		hostname, _ := os.Hostname()
		*nodeID = hostname
	}

	log.Printf("Starting OctopOS Gateway on %s (node: %s)", *vipAddr, *nodeID)

	gw := &Gateway{
		grpcAddr:  *grpcAddr,
		nodeID:    *nodeID,
		mountBase: *mountBase,
		sessions:  make(map[string]*ClusterSession),
	}

	if err := gw.init(); err != nil {
		log.Fatalf("Init failed: %v", err)
	}
	defer gw.close()

	// Handle shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Shutting down...")
		gw.close()
		os.Exit(0)
	}()

	// Start SSH server
	log.Printf("SSH server listening on %s", *vipAddr)
	if err := gw.sshServer.ListenAndServe(); err != nil {
		log.Fatalf("SSH server failed: %v", err)
	}
}

func (g *Gateway) init() error {
	// Connect to octoposd
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, g.grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("connect to octoposd: %w", err)
	}
	g.grpcConn = conn
	g.clusterClient = octopospb.NewClusterClient(conn)

	// Load SSH host key
	hostKey, err := os.ReadFile(*hostKeyFile)
	if err != nil {
		return fmt.Errorf("read host key: %w", err)
	}
	signer, err := gocryptossh.ParsePrivateKey(hostKey)
	if err != nil {
		return fmt.Errorf("parse host key: %w", err)
	}

	// Setup SSH server
	g.sshServer = &ssh.Server{
		Addr:             *vipAddr,
		Handler:          g.handleSSH,
		PublicKeyHandler: g.authPublicKey,
		PasswordHandler:  g.authPassword,
	}
	g.sshServer.AddHostKey(signer)

	// Ensure mount base exists
	os.MkdirAll(g.mountBase, 0755)

	return nil
}

func (g *Gateway) close() {
	g.mu.Lock()
	for _, sess := range g.sessions {
		if sess.Cleanup != nil {
			sess.Cleanup()
		}
	}
	g.mu.Unlock()
	if g.grpcConn != nil {
		g.grpcConn.Close()
	}
}

func (g *Gateway) authPublicKey(ctx ssh.Context, key ssh.PublicKey) bool {
	// Check against authorized_keys file
	if *authorizedKeys == "" {
		home, _ := os.UserHomeDir()
		*authorizedKeys = filepath.Join(home, ".ssh", "authorized_keys")
	}

	authKeys, err := os.ReadFile(*authorizedKeys)
	if err != nil {
		log.Printf("No authorized_keys file: %v", err)
		return false
	}

	for _, line := range strings.Split(string(authKeys), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// Simple match - in production use ssh.ParseAuthorizedKey
		if strings.Contains(line, strings.TrimSpace(string(key.Marshal()))) {
			return true
		}
	}
	return false
}

func (g *Gateway) authPassword(ctx ssh.Context, password string) bool {
	// For now, allow password auth (integrate with PAM in production)
	log.Printf("Password auth for user: %s", ctx.User())
	return true
}

func (g *Gateway) handleSSH(s ssh.Session) {
	user := s.User()
	log.Printf("SSH connection from %s (user: %s)", s.RemoteAddr(), user)

	// Create cluster session
	sessionID := fmt.Sprintf("ssh-%s-%d", user, time.Now().UnixNano())

	ctx := context.Background()
	createResp, err := g.clusterClient.CreateSession(ctx, &octopospb.CreateSessionRequest{
		SessionId: sessionID,
		User:      user,
	})
	if err != nil || !createResp.Success {
		s.Write([]byte(fmt.Sprintf("Failed to create session: %v\n", err)))
		s.Exit(1)
		return
	}
	log.Printf("Created cluster session: %s", sessionID)

	// Start FUSE daemons for this session
	mountPoints, cleanupFUSE, err := g.startFUSE(sessionID)
	if err != nil {
		s.Write([]byte(fmt.Sprintf("Failed to start FUSE: %v\n", err)))
		s.Exit(1)
		return
	}

	// Create cgroup for session
	cgroupPath := filepath.Join("/sys/fs/cgroup/octopos", sessionID)
	if err := g.createCgroup(sessionID); err != nil {
		log.Printf("Warning: cgroup creation failed: %v", err)
	}

	// Setup cleanup
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			cleanupFUSE()
			g.destroyCgroup(sessionID)
			g.destroySession(sessionID)
		})
	}

	// Create session tracking
	clusterSess := &ClusterSession{
		SessionID:   sessionID,
		User:        user,
		MountPoints: mountPoints,
		CgroupPath:  cgroupPath,
		Cleanup:     cleanup,
	}
	g.mu.Lock()
	g.sessions[sessionID] = clusterSess
	g.mu.Unlock()

	// Request PTY if needed
	ptyReq, winCh, isPty := s.Pty()
	if isPty {
		log.Printf("PTY requested: %s %dx%d", ptyReq.Term, ptyReq.Window.Width, ptyReq.Window.Height)
	}

	// Enter namespace and spawn shell
	if err := g.enterNamespaceAndRun(s, clusterSess, ptyReq, winCh, isPty); err != nil {
		log.Printf("Session error: %v", err)
		s.Write([]byte(fmt.Sprintf("Session error: %v\n", err)))
	}

	// Cleanup on exit
	cleanup()
	g.mu.Lock()
	delete(g.sessions, sessionID)
	g.mu.Unlock()
	log.Printf("Session %s ended", sessionID)
}

func (g *Gateway) startFUSE(sessionID string) (map[string]string, func(), error) {
	mountPoints := make(map[string]string)
	var procs []*exec.Cmd

	fuseDaemons := []struct {
		name   string
		binary string
		mount  string
		args   []string
	}{
		{"procfs", "octopos-procfs", filepath.Join(g.mountBase, "proc-"+sessionID), []string{"--mount", filepath.Join(g.mountBase, "proc-"+sessionID)}},
		{"devfs", "octopos-devfs", filepath.Join(g.mountBase, "dev-"+sessionID), []string{"--mount", filepath.Join(g.mountBase, "dev-"+sessionID)}},
		{"sysfs", "octopos-sysfs", filepath.Join(g.mountBase, "sys-"+sessionID), []string{"--mount", filepath.Join(g.mountBase, "sys-"+sessionID)}},
	}

	for _, d := range fuseDaemons {
		mountDir := d.mount
		os.MkdirAll(mountDir, 0755)

		// Check if binary exists
		binPath, err := exec.LookPath(d.binary)
		if err != nil {
			log.Printf("FUSE daemon %s not found, skipping", d.name)
			continue
		}

		cmd := exec.Command(binPath, d.args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			log.Printf("Failed to start %s: %v", d.name, err)
			continue
		}
		procs = append(procs, cmd)
		mountPoints[d.name] = mountDir
		log.Printf("Started %s at %s", d.name, mountDir)
	}

	// Wait a bit for FUSE to be ready
	time.Sleep(500 * time.Millisecond)

	cleanup := func() {
		for _, cmd := range procs {
			if cmd.Process != nil {
				cmd.Process.Signal(syscall.SIGTERM)
				cmd.Wait()
			}
		}
		// Unmount
		for _, mp := range mountPoints {
			syscall.Unmount(mp, syscall.MNT_DETACH)
			os.RemoveAll(mp)
		}
	}

	return mountPoints, cleanup, nil
}

func (g *Gateway) createCgroup(sessionID string) error {
	path := filepath.Join("/sys/fs/cgroup/octopos", sessionID)
	if err := os.MkdirAll(path, 0755); err != nil {
		return err
	}
	return nil
}

func (g *Gateway) destroyCgroup(sessionID string) {
	path := filepath.Join("/sys/fs/cgroup/octopos", sessionID)
	procsPath := filepath.Join(path, "cgroup.procs")
	if data, err := os.ReadFile(procsPath); err == nil && len(data) > 0 {
		parentProcs := filepath.Join("/sys/fs/cgroup/octopos", "cgroup.procs")
		os.WriteFile(parentProcs, data, 0644)
	}
	os.RemoveAll(path)
}

func (g *Gateway) destroySession(sessionID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	g.clusterClient.DestroySession(ctx, &octopospb.DestroySessionRequest{
		SessionId: sessionID,
	})
}

func (g *Gateway) enterNamespaceAndRun(s ssh.Session, sess *ClusterSession, ptyReq ssh.Pty, winCh <-chan ssh.Window, isPty bool) error {
	// Get the shell
	shell := os.Getenv("SHELL")
	if shell == "" {
		shell = "/bin/bash"
	}

	// Create command
	cmd := exec.Command(shell, "-l")
	cmd.Env = append(os.Environ(),
		"OCTOPOS_SESSION="+sess.SessionID,
		"OCTOPOS_MOUNT_BASE="+g.mountBase,
		"PS1=\\u@octopos-cluster:\\w\\$ ",
	)
	cmd.Dir = "/home/" + s.User()

	// Setup namespace isolation
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags:   syscall.CLONE_NEWPID | syscall.CLONE_NEWNS | syscall.CLONE_NEWNET | syscall.CLONE_NEWUTS | syscall.CLONE_NEWIPC,
		Unshareflags: syscall.CLONE_NEWNS,
	}

	var ptyFile *os.File
	var err error

	if isPty {
		// Use PTY
		ptyFile, err = pty.Start(cmd)
		if err != nil {
			return fmt.Errorf("pty start: %w", err)
		}
		sess.Pty = ptyFile

		// Handle window size changes
		go func() {
			for win := range winCh {
				pty.Setsize(ptyFile, &pty.Winsize{
					Rows: uint16(win.Height),
					Cols: uint16(win.Width),
				})
			}
		}()

		// Copy SSH session <-> PTY
		go func() { io.Copy(ptyFile, s) }()
		go func() { io.Copy(s, ptyFile) }()

		// Wait for command to finish
		return cmd.Wait()
	} else {
		// No PTY - use pipes
		stdin, _ := cmd.StdinPipe()
		stdout, _ := cmd.StdoutPipe()
		stderr, _ := cmd.StderrPipe()

		if err := cmd.Start(); err != nil {
			return fmt.Errorf("start: %w", err)
		}

		go io.Copy(stdin, s)
		go io.Copy(s, stdout)
		go io.Copy(s.Stderr(), stderr)

		return cmd.Wait()
	}
}
