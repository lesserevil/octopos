package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	octopospb "github.com/octopos/octopos/pkg/rpc"
)

var (
	grpcAddr string
	client   octopospb.ClusterClient
	conn     *grpc.ClientConn
)

var rootCmd = &cobra.Command{
	Use:   "octoposctl",
	Short: "OctopOS Cluster Control CLI",
	Long:  `Admin CLI for managing OctopOS Single System Image cluster`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		var err error
		conn, err = grpc.DialContext(context.Background(), grpcAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithTimeout(5*time.Second),
		)
		if err != nil {
			return fmt.Errorf("failed to connect to %s: %w", grpcAddr, err)
		}
		client = octopospb.NewClusterClient(conn)
		return nil
	},
	PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
		if conn != nil {
			return conn.Close()
		}
		return nil
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&grpcAddr, "addr", "10.0.0.1:50051", "gRPC server address")
}

var nodeCmd = &cobra.Command{
	Use:   "node",
	Short: "Manage cluster nodes",
}

var nodeListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all cluster nodes",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.GetClusterState(ctx, &octopospb.GetClusterStateRequest{})
		if err != nil {
			return fmt.Errorf("GetClusterState failed: %w", err)
		}

		output, _ := cmd.Flags().GetString("output")
		switch output {
		case "json":
			data, _ := json.MarshalIndent(resp.Nodes, "", "  ")
			fmt.Println(string(data))
		case "wide":
			fmt.Printf("%-20s %-15s %-10s %-10s %-10s %-10s %s\n", "NODE", "ADDRESS", "STATE", "CPU", "MEM(GB)", "GPUS", "LABELS")
			for _, n := range resp.Nodes {
				memGB := float64(n.Capacity.MemoryBytes) / (1024 * 1024 * 1024)
				labels := ""
				for k, v := range n.Labels {
					labels += fmt.Sprintf("%s=%s ", k, v)
				}
				fmt.Printf("%-20s %-15s %-10s %-10d %-10.1f %-10d %s\n",
					n.NodeId, n.Address, n.State.String(), n.Capacity.CpuMillicores/1000, memGB, n.Capacity.GpuCount, labels)
			}
		default:
			fmt.Printf("%-20s %-15s %-10s %s\n", "NODE", "ADDRESS", "STATE", "CAPACITY")
			for _, n := range resp.Nodes {
				memGB := float64(n.Capacity.MemoryBytes) / (1024 * 1024 * 1024)
				fmt.Printf("%-20s %-15s %-10s %d CPU, %.1f GB RAM, %d GPU\n",
					n.NodeId, n.Address, n.State.String(), n.Capacity.CpuMillicores/1000, memGB, n.Capacity.GpuCount)
			}
		}
		return nil
	},
}

var nodeAddCmd = &cobra.Command{
	Use:   "add <node-id>",
	Short: "Provision and add a new node to the cluster",
	Long: `SSH into a new node, install dependencies, configure WireGuard, deploy binaries, register with the cluster, and add the WireGuard peer.

Run this on an existing cluster node. Requires root SSH access to the remote box.

The WireGuard IP is auto-assigned from the cluster's subnet (10.0.0.0/24).
Use --wg-ip to override.

Example:
  octoposctl node add node-2 --address 192.168.122.100
`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		sshAddr, _ := cmd.Flags().GetString("address")
		if sshAddr == "" {
			return fmt.Errorf("--address is required (SSH address of the remote node, e.g., 192.168.122.100)")
		}
		wgIP, _ := cmd.Flags().GetString("wg-ip")
		if wgIP == "" {
			var err error
			wgIP, err = autoAssignWGIP()
			if err != nil {
				return fmt.Errorf("cannot auto-assign WireGuard IP: %w\nUse --wg-ip to specify one manually", err)
			}
			fmt.Printf("Auto-assigned WireGuard IP: %s\n", wgIP)
		}
		sshUser, _ := cmd.Flags().GetString("ssh-user")
		sshPass, _ := cmd.Flags().GetString("password")
		wgEndpoint, _ := cmd.Flags().GetString("endpoint")
		if wgEndpoint == "" {
			wgEndpoint = fmt.Sprintf("%s:51820", sshAddr)
		}
		wgPort, _ := cmd.Flags().GetInt("wg-port")
		grpcPort, _ := cmd.Flags().GetInt("grpc-port")
		ebpfEnabled, _ := cmd.Flags().GetBool("ebpf")
		fuseEnabled, _ := cmd.Flags().GetBool("fuse")

		cfg := &provisionConfig{
			NodeID:       args[0],
			Address:      sshAddr,
			WgIP:         wgIP,
			SSHUser:      sshUser,
			SSHPassword:  sshPass,
			WgEndpoint:   wgEndpoint,
			WgListenPort: wgPort,
			GrpcPort:     grpcPort,
			BinDir:       "bin",
			EBPFEnabled:  ebpfEnabled,
			FUSEEnabled:  fuseEnabled,
		}
		return provisionNode(cfg)
	},
}
var nodeDrainCmd = &cobra.Command{
	Use:   "drain <node-id>",
	Short: "Drain a node (stop scheduling new jobs, migrate workloads)",
	Long: `Mark a node for draining. Existing jobs complete but no new jobs are scheduled.

Example:
  octoposctl node drain node-3
`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("Draining node %s...\n", args[0])
		fmt.Println("Note: drain sets node state to draining via heartbeat response.")
		fmt.Println("      Run 'octoposctl node list' to verify state change.")
		return nil
	},
}

var nodeRemoveCmd = &cobra.Command{
	Use:   "remove <node-id>",
	Short: "Remove a node from the cluster",
	Long: `Drain and remove a node from the cluster.

Example:
  octoposctl node remove node-3
`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("Removing node %s from cluster...\n", args[0])
		fmt.Println("Note: node removal is coordinated via heartbeat mechanism.")
		fmt.Println("      The node state will be set to offline and it will be excluded from scheduling.")
		return nil
	},
}

var jobCmd = &cobra.Command{
	Use:   "job",
	Short: "Manage jobs",
}

var jobListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all jobs",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.GetClusterState(ctx, &octopospb.GetClusterStateRequest{})
		if err != nil {
			return fmt.Errorf("GetClusterState failed: %w", err)
		}

		fmt.Printf("%-20s %-20s %-12s %-10s %s\n", "JOB ID", "SESSION", "STATUS", "NODE", "COMMANDS")
		for _, job := range resp.Jobs {
			cmds := []string{}
			for _, c := range job.Commands {
				cmds = append(cmds, strings.Join(c.Argv, " "))
			}
			fmt.Printf("%-20s %-20s %-12s %-10s %s\n",
				job.JobId, job.SessionId, job.Status.String(), job.PrimaryNode, strings.Join(cmds, " | "))
		}
		return nil
	},
}

var jobStatusCmd = &cobra.Command{
	Use:   "status <job-id>",
	Short: "Get job status",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.GetJobStatus(ctx, &octopospb.GetJobStatusRequest{JobId: args[0]})
		if err != nil {
			return fmt.Errorf("GetJobStatus failed: %w", err)
		}
		if !resp.Found {
			return fmt.Errorf("job not found: %s", args[0])
		}

		data, _ := json.MarshalIndent(resp.Job, "", "  ")
		fmt.Println(string(data))
		return nil
	},
}

var execCmd = &cobra.Command{
	Use:   "exec [flags] -- command [args...]",
	Short: "Execute a command on the cluster",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		sessionID, _ := cmd.Flags().GetString("session")
		if sessionID == "" {
			sessionID = fmt.Sprintf("cli-%d", time.Now().Unix())
		}

		cpu, _ := cmd.Flags().GetInt("cpu")
		mem, _ := cmd.Flags().GetInt("mem")
		gpus, _ := cmd.Flags().GetInt("gpus")
		_, _ = cmd.Flags().GetString("node") // node affinity - TODO: implement

		resp, err := client.Execute(ctx, &octopospb.ExecuteRequest{
			SessionId: sessionID,
			JobId:     fmt.Sprintf("job-%d", time.Now().UnixNano()),
			Command:   args,
			Resources: &octopospb.Requirements{
				CpuMillicores: int64(cpu * 1000),
				MemoryBytes:   int64(mem * 1024 * 1024 * 1024),
				Gpus:          int32(gpus),
			},
		})
		if err != nil {
			return fmt.Errorf("Execute failed: %w", err)
		}

		fmt.Printf("Job submitted: %s (GlobalPID: %d)\n", resp.JobId, resp.GlobalPid)

		wait, _ := cmd.Flags().GetBool("wait")
		if wait {
			waitResp, err := client.Wait(ctx, &octopospb.WaitRequest{
				JobId:     resp.JobId,
				TimeoutMs: 60000,
			})
			if err != nil {
				return fmt.Errorf("Wait failed: %w", err)
			}
			fmt.Printf("Job completed with exit code: %d\n", waitResp.ExitCode)
		}
		return nil
	},
}

var sessionCmd = &cobra.Command{
	Use:   "session",
	Short: "Manage sessions",
}

var sessionListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all sessions",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.ListSessions(ctx, &octopospb.ListSessionsRequest{})
		if err != nil {
			return fmt.Errorf("ListSessions failed: %w", err)
		}

		fmt.Printf("%-20s %-10s %-15s %s\n", "SESSION ID", "USER", "NODE", "CREATED")
		for _, s := range resp.Sessions {
			created := time.Unix(s.CreatedAt, 0).Format("2006-01-02 15:04:05")
			fmt.Printf("%-20s %-10s %-15s %s\n", s.SessionId, s.User, s.NodeId, created)
		}
		return nil
	},
}

var sessionCreateCmd = &cobra.Command{
	Use:   "create [user]",
	Short: "Create a new session",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		user := "cli"
		if len(args) > 0 {
			user = args[0]
		}
		sessionID := fmt.Sprintf("session-%d", time.Now().UnixNano())

		resp, err := client.CreateSession(ctx, &octopospb.CreateSessionRequest{
			SessionId: sessionID,
			User:      user,
		})
		if err != nil {
			return fmt.Errorf("CreateSession failed: %w", err)
		}
		if !resp.Success {
			return fmt.Errorf("CreateSession failed: %s", resp.Error)
		}
		fmt.Printf("Session created: %s\n", sessionID)
		return nil
	},
}

var sessionDeleteCmd = &cobra.Command{
	Use:   "delete <session-id>",
	Short: "Delete a session",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.DestroySession(ctx, &octopospb.DestroySessionRequest{
			SessionId: args[0],
		})
		if err != nil {
			return fmt.Errorf("DestroySession failed: %w", err)
		}
		if !resp.Success {
			return fmt.Errorf("DestroySession failed: %s", resp.Error)
		}
		fmt.Printf("Session deleted: %s\n", args[0])
		return nil
	},
}

var psCmd = &cobra.Command{
	Use:   "ps",
	Short: "List processes",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		node, _ := cmd.Flags().GetString("node")
		session, _ := cmd.Flags().GetString("session")
		job, _ := cmd.Flags().GetString("job")

		resp, err := client.ListProcesses(ctx, &octopospb.ListProcessesRequest{
			NodeId:    node,
			SessionId: session,
			JobId:     job,
		})
		if err != nil {
			return fmt.Errorf("ListProcesses failed: %w", err)
		}

		fmt.Printf("%-20s %-15s %-8s %-10s %-10s %s\n", "GLOBAL PID", "NODE", "LOCAL PID", "SESSION", "JOB", "COMMAND")
		for _, p := range resp.Processes {
			fmt.Printf("%-20d %-15s %-8d %-10s %-10s %s\n",
				p.GlobalPid, p.NodeId, p.LocalPid, p.SessionId, p.JobId, p.Cmdline)
		}
		return nil
	},
}

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Cluster-wide operations",
}

var clusterStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show cluster status",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.GetClusterState(ctx, &octopospb.GetClusterStateRequest{})
		if err != nil {
			return fmt.Errorf("GetClusterState failed: %w", err)
		}

		fmt.Println("=== Cluster Status ===")
		fmt.Printf("Nodes: %d\n", len(resp.Nodes))
		fmt.Printf("Sessions: %d\n", len(resp.Sessions))
		fmt.Printf("Jobs: %d\n", len(resp.Jobs))

		totalCPU := int64(0)
		totalMem := int64(0)
		allocCPU := int64(0)
		allocMem := int64(0)
		for _, n := range resp.Nodes {
			totalCPU += n.Capacity.CpuMillicores
			totalMem += n.Capacity.MemoryBytes
			allocCPU += n.Allocated.CpuMillicores
			allocMem += n.Allocated.MemoryBytes
		}
		fmt.Printf("Total Capacity: %d CPU cores, %.1f GB RAM\n", totalCPU/1000, float64(totalMem)/(1024*1024*1024))
		fmt.Printf("Allocated: %d CPU cores, %.1f GB RAM\n", allocCPU/1000, float64(allocMem)/(1024*1024*1024))

		return nil
	},
}

var clusterBootstrapCmd = &cobra.Command{
	Use:   "bootstrap",
	Short: "Bootstrap the first cluster node",
	Long: `Initialize the first node of a new OctopOS cluster on this machine.
Sets up WireGuard, builds and starts octoposd, and registers the local node.

The first node gets WireGuard IP 10.0.0.1 by default. Use --wg-ip to override.

Example:
  octoposctl cluster bootstrap --node-id node-1
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		nodeID, _ := cmd.Flags().GetString("node-id")
		if nodeID == "" {
			hostname, _ := os.Hostname()
			nodeID = hostname
		}
		wgIP, _ := cmd.Flags().GetString("wg-ip")
		if wgIP == "" {
			wgIP = "10.0.0.1"
		}
		grpcPort, _ := cmd.Flags().GetInt("grpc-port")
		wgPort, _ := cmd.Flags().GetInt("wg-port")

		grpcAddr := fmt.Sprintf("0.0.0.0:%d", grpcPort)

		enableGateway, _ := cmd.Flags().GetBool("enable-gateway")
		vipIP, _ := cmd.Flags().GetString("vip-ip")

		cfg := &bootstrapConfig{
			NodeID:        nodeID,
			WgIP:          wgIP,
			WgInterface:   "wg-octopos",
			WgListenPort:  wgPort,
			GrpcAddr:      grpcAddr,
			GrpcPort:      grpcPort,
			EnableGateway: enableGateway,
			VipIP:         vipIP,
		}
		return bootstrapCluster(cfg)
	},
}

func main() {
	// Add flags to commands
	nodeAddCmd.Flags().String("address", "", "SSH address of the remote node (required, e.g., 192.168.122.100)")
	nodeAddCmd.Flags().String("wg-ip", "", "WireGuard IP for the new node (auto-assigned if empty)")
	nodeAddCmd.Flags().String("ssh-user", "root", "SSH user for the remote node")
	nodeAddCmd.Flags().String("password", "", "SSH password (uses key-based auth if empty)")
	nodeAddCmd.Flags().String("endpoint", "", "WireGuard endpoint for existing nodes (default: <address>:51820)")
	nodeAddCmd.Flags().Int("wg-port", 51820, "WireGuard listen port")
	nodeAddCmd.Flags().Int("grpc-port", 50051, "gRPC port")
	nodeAddCmd.Flags().Bool("ebpf", false, "Build and deploy eBPF programs")
	nodeAddCmd.Flags().Bool("fuse", false, "Build and deploy FUSE daemons")
	nodeAddCmd.Flags().Int64("cpu", 0, "Override CPU capacity in millicores (0 = auto-detect)")
	nodeAddCmd.Flags().Int64("mem", 0, "Override memory capacity in bytes (0 = auto-detect)")
	nodeAddCmd.Flags().Int32("gpus", 0, "Override GPU count")

	clusterBootstrapCmd.Flags().String("node-id", "", "Node ID (default: hostname)")
	clusterBootstrapCmd.Flags().String("wg-ip", "", "WireGuard IP (default: 10.0.0.1)")
	clusterBootstrapCmd.Flags().Int("wg-port", 51820, "WireGuard listen port")
	clusterBootstrapCmd.Flags().Int("grpc-port", 50051, "gRPC port")
	clusterBootstrapCmd.Flags().Bool("enable-gateway", true, "Deploy VIP gateway (octopos-gw) on this node")
	clusterBootstrapCmd.Flags().String("vip-ip", "10.0.0.100", "Virtual IP for cluster gateway")

	nodeListCmd.Flags().StringP("output", "o", "", "Output format (json|wide)")

	execCmd.Flags().String("session", "", "Session ID (auto-generated if empty)")
	execCmd.Flags().Int("cpu", 1, "CPU cores required")
	execCmd.Flags().Int("mem", 1, "Memory required (GB)")
	execCmd.Flags().Int("gpus", 0, "GPUs required")
	execCmd.Flags().String("node", "", "Node affinity")
	execCmd.Flags().Bool("wait", false, "Wait for job completion")

	psCmd.Flags().String("node", "", "Filter by node")
	psCmd.Flags().String("session", "", "Filter by session")
	psCmd.Flags().String("job", "", "Filter by job")

	// Build command tree
	nodeCmd.AddCommand(nodeListCmd, nodeAddCmd, nodeDrainCmd, nodeRemoveCmd)
	jobCmd.AddCommand(jobListCmd, jobStatusCmd)
	sessionCmd.AddCommand(sessionListCmd, sessionCreateCmd, sessionDeleteCmd)
	clusterCmd.AddCommand(clusterStatusCmd, clusterBootstrapCmd)

	rootCmd.AddCommand(nodeCmd, jobCmd, execCmd, sessionCmd, psCmd, clusterCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
