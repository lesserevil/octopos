package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/octopos/octopos/pkg/clusterconfig"
	"github.com/octopos/octopos/pkg/execclient"
	"github.com/octopos/octopos/pkg/remotechild"
	octopospb "github.com/octopos/octopos/pkg/rpc"
	"github.com/octopos/octopos/pkg/ssi"
)

var (
	grpcAddr          string
	clusterConfigPath string
	client            octopospb.ClusterClient
	conn              *grpc.ClientConn
)

var rootCmd = &cobra.Command{
	Use:           "octoposctl",
	Short:         "OctopOS Cluster Control CLI",
	Long:          `Admin CLI for managing OctopOS Single System Image cluster`,
	SilenceUsage:  true,
	SilenceErrors: true,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if !requiresClusterConnection(cmd) {
			return nil
		}
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

func requiresClusterConnection(cmd *cobra.Command) bool {
	for c := cmd; c != nil; c = c.Parent() {
		if c == clusterBootstrapCmd {
			return false
		}
	}
	return true
}

func remoteChildrenEnvironment(remoteChildren string, ipcCompat string, allowFileLocks bool) ([]string, error) {
	switch remoteChildren {
	case "off", "safe", "aggressive":
	default:
		return nil, fmt.Errorf("--remote-children must be off, safe, or aggressive")
	}
	if ipcCompat == "" {
		ipcCompat = "strict"
	}
	switch ipcCompat {
	case "strict", "relaxed":
	default:
		return nil, fmt.Errorf("--remote-ipc-compat must be strict or relaxed")
	}
	if remoteChildren == "off" {
		return nil, nil
	}
	env := []string{
		remotechild.EnvMode + "=" + remoteChildren,
		remotechild.EnvIPCCompat + "=" + ipcCompat,
		"LD_PRELOAD=" + remoteChildPreloadPath,
	}
	if allowFileLocks {
		env = append(env, remotechild.EnvAllowFileLocks+"=1")
	}
	return env, nil
}

func execResourceDefaults(cmd *cobra.Command, configPath string) (int, int, error) {
	defaults, err := clusterconfig.LoadExecDefaults(configPath)
	if err != nil {
		return 0, 0, err
	}
	cpu := defaults.CPUCores
	mem := defaults.MemoryGB
	if cmd.Flags().Changed("cpu") {
		cpu, err = cmd.Flags().GetInt("cpu")
		if err != nil {
			return 0, 0, err
		}
	}
	if cmd.Flags().Changed("mem") {
		mem, err = cmd.Flags().GetInt("mem")
		if err != nil {
			return 0, 0, err
		}
	}
	return cpu, mem, nil
}

func clusterExecDefaultsForCommand(cmd *cobra.Command, configPath string) (clusterconfig.ExecDefaults, error) {
	defaults, err := clusterconfig.LoadExecDefaults(configPath)
	if err != nil {
		return clusterconfig.ExecDefaults{}, err
	}
	if cmd.Flags().Changed("default-exec-cpu") {
		defaults.CPUCores, err = cmd.Flags().GetInt("default-exec-cpu")
		if err != nil {
			return clusterconfig.ExecDefaults{}, err
		}
	}
	if cmd.Flags().Changed("default-exec-mem") {
		defaults.MemoryGB, err = cmd.Flags().GetInt("default-exec-mem")
		if err != nil {
			return clusterconfig.ExecDefaults{}, err
		}
	}
	return defaults.WithDefaults(), nil
}

func parseVFIORequirements(specs []string) ([]*octopospb.VFIORequirement, error) {
	reqs := make([]*octopospb.VFIORequirement, 0, len(specs))
	for _, spec := range specs {
		spec = strings.TrimSpace(spec)
		if spec == "" {
			continue
		}
		req := &octopospb.VFIORequirement{Count: 1}
		for _, field := range strings.Split(spec, ",") {
			key, value, ok := strings.Cut(strings.TrimSpace(field), "=")
			if !ok {
				return nil, fmt.Errorf("invalid --vfio field %q in %q", field, spec)
			}
			key = strings.TrimSpace(key)
			value = strings.TrimSpace(value)
			switch key {
			case "vendor":
				req.VendorId = value
			case "device":
				req.DeviceId = value
			case "class":
				req.Class = value
			case "count":
				count, err := strconv.Atoi(value)
				if err != nil || count <= 0 {
					return nil, fmt.Errorf("invalid --vfio count %q", value)
				}
				req.Count = int32(count)
			default:
				return nil, fmt.Errorf("unknown --vfio key %q", key)
			}
		}
		if req.VendorId == "" && req.DeviceId == "" && req.Class == "" {
			return nil, fmt.Errorf("--vfio %q must include vendor, device, or class", spec)
		}
		reqs = append(reqs, req)
	}
	return reqs, nil
}

func init() {
	rootCmd.PersistentFlags().StringVar(&grpcAddr, "addr", "10.0.0.1:50051", "gRPC server address")
	rootCmd.PersistentFlags().StringVar(&clusterConfigPath, "config", clusterconfig.DefaultPath, "Cluster config file path")
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
		localEndpoint, _ := cmd.Flags().GetString("local-endpoint")
		seedPeer, _ := cmd.Flags().GetString("seed-peer")
		wgPort, _ := cmd.Flags().GetInt("wg-port")
		grpcPort, _ := cmd.Flags().GetInt("grpc-port")
		ebpfEnabled, _ := cmd.Flags().GetBool("ebpf")
		fuseEnabled, _ := cmd.Flags().GetBool("fuse")
		clusterRoot, _ := cmd.Flags().GetString("cluster-root")
		ssiRootFS, _ := cmd.Flags().GetString("ssi-rootfs")
		requireSSI, _ := cmd.Flags().GetBool("require-ssi")
		clusterFSMeta, _ := cmd.Flags().GetString("clusterfs-meta")
		clusterFSOpts, _ := cmd.Flags().GetString("clusterfs-options")
		objectProxy, _ := cmd.Flags().GetBool("objectstore-proxy")
		objectListen, _ := cmd.Flags().GetString("objectstore-proxy-listen")
		objectTargets, _ := cmd.Flags().GetString("objectstore-proxy-targets")
		execDefaults, err := clusterExecDefaultsForCommand(cmd, clusterConfigPath)
		if err != nil {
			return err
		}

		cfg := &provisionConfig{
			NodeID:        args[0],
			Address:       sshAddr,
			WgIP:          wgIP,
			SSHUser:       sshUser,
			SSHPassword:   sshPass,
			WgEndpoint:    wgEndpoint,
			LocalEndpoint: localEndpoint,
			SeedPeer:      seedPeer,
			WgListenPort:  wgPort,
			GrpcPort:      grpcPort,
			BinDir:        "bin",
			EBPFEnabled:   ebpfEnabled,
			FUSEEnabled:   fuseEnabled,
			ClusterRoot:   clusterRoot,
			SSIRootFS:     ssiRootFS,
			RequireSSI:    requireSSI,
			ClusterFSMeta: clusterFSMeta,
			ClusterFSOpts: clusterFSOpts,
			ObjectProxy:   objectProxy,
			ObjectListen:  objectListen,
			ObjectTargets: objectTargets,
			ExecDefaults:  execDefaults,
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

var jobChildrenCmd = &cobra.Command{
	Use:   "children [parent-job-id]",
	Short: "List remote child processes",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		session, _ := cmd.Flags().GetString("session")
		node, _ := cmd.Flags().GetString("node")
		activeOnly, _ := cmd.Flags().GetBool("active")
		req := &octopospb.ListRemoteChildrenRequest{
			SessionId:    session,
			RemoteNodeId: node,
			ActiveOnly:   activeOnly,
		}
		if len(args) > 0 {
			req.ParentJobId = args[0]
		}
		resp, err := client.ListRemoteChildren(ctx, req)
		if err != nil {
			return fmt.Errorf("ListRemoteChildren failed: %w", err)
		}

		fmt.Printf("%-20s %-20s %-15s %-10s %-10s %-12s %-24s %s\n", "REMOTE JOB", "PARENT JOB", "REMOTE NODE", "SHADOW", "PID", "STATE", "REASON CODE", "COMMAND")
		for _, child := range resp.Children {
			fmt.Printf("%-20s %-20s %-15s %-10d %-10d %-12s %-24s %s\n",
				child.RemoteJobId,
				child.ParentJobId,
				child.RemoteNodeId,
				child.ShadowPid,
				child.RemoteGlobalPid,
				child.State,
				child.FallbackReasonCode,
				strings.Join(child.Command, " "),
			)
		}
		return nil
	},
}

var vfioCmd = &cobra.Command{
	Use:   "vfio",
	Short: "Manage VFIO device groups",
}

var vfioListCmd = &cobra.Command{
	Use:   "list",
	Short: "List VFIO device groups",
	RunE: func(cmd *cobra.Command, args []string) error {
		node, _ := cmd.Flags().GetString("node")
		output, _ := cmd.Flags().GetString("output")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.GetVFIODevices(ctx, &octopospb.GetVFIODevicesRequest{NodeId: node})
		if err != nil {
			return fmt.Errorf("GetVFIODevices failed: %w", err)
		}
		if output == "json" {
			data, _ := json.MarshalIndent(resp.Groups, "", "  ")
			fmt.Println(string(data))
			return nil
		}
		fmt.Printf("%-8s %-16s %-12s %s\n", "GROUP", "CLAIMED_BY", "DEVICES", "MATCH")
		for _, group := range resp.Groups {
			claimedBy := group.ClaimedBy
			if claimedBy == "" {
				claimedBy = "-"
			}
			fmt.Printf("%-8d %-16s %-12d %s\n", group.GroupId, claimedBy, len(group.Devices), vfioDeviceSummary(group.Devices))
		}
		return nil
	},
}

var vfioAllocateCmd = &cobra.Command{
	Use:   "allocate",
	Short: "Manually allocate one VFIO group",
	RunE: func(cmd *cobra.Command, args []string) error {
		sessionID, _ := cmd.Flags().GetString("session")
		jobID, _ := cmd.Flags().GetString("job")
		vendorID, _ := cmd.Flags().GetString("vendor")
		deviceID, _ := cmd.Flags().GetString("device")
		classID, _ := cmd.Flags().GetString("class")
		count, _ := cmd.Flags().GetInt("count")
		reqs, err := parseVFIORequirements([]string{formatVFIORequirement(vendorID, deviceID, classID, count)})
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		resp, err := client.AllocateVFIO(ctx, &octopospb.AllocateVFIORequest{
			SessionId: sessionID,
			JobId:     jobID,
			Device:    reqs[0],
		})
		if err != nil {
			return fmt.Errorf("AllocateVFIO failed: %w", err)
		}
		if !resp.Success {
			return fmt.Errorf("AllocateVFIO failed: %s", resp.Error)
		}
		fmt.Printf("VFIO group allocated: %d (%s)\n", resp.GroupId, resp.DevicePath)
		return nil
	},
}

var vfioReleaseCmd = &cobra.Command{
	Use:   "release",
	Short: "Release a manually allocated VFIO group",
	RunE: func(cmd *cobra.Command, args []string) error {
		sessionID, _ := cmd.Flags().GetString("session")
		groupID, _ := cmd.Flags().GetInt("group")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		resp, err := client.ReleaseVFIO(ctx, &octopospb.ReleaseVFIORequest{
			SessionId: sessionID,
			GroupId:   int32(groupID),
		})
		if err != nil {
			return fmt.Errorf("ReleaseVFIO failed: %w", err)
		}
		if !resp.Success {
			return fmt.Errorf("ReleaseVFIO failed: %s", resp.Error)
		}
		fmt.Printf("VFIO group released: %d\n", groupID)
		return nil
	},
}

func formatVFIORequirement(vendorID, deviceID, classID string, count int) string {
	fields := []string{}
	if vendorID != "" {
		fields = append(fields, "vendor="+vendorID)
	}
	if deviceID != "" {
		fields = append(fields, "device="+deviceID)
	}
	if classID != "" {
		fields = append(fields, "class="+classID)
	}
	if count > 0 {
		fields = append(fields, "count="+strconv.Itoa(count))
	}
	return strings.Join(fields, ",")
}

func vfioDeviceSummary(devices []*octopospb.PCIDevice) string {
	parts := make([]string, 0, len(devices))
	for _, dev := range devices {
		if dev == nil {
			continue
		}
		match := ""
		if dev.VendorId != "" || dev.DeviceId != "" {
			match = dev.VendorId + ":" + dev.DeviceId
		}
		if dev.Class != "" {
			if match != "" {
				match += "/"
			}
			match += dev.Class
		}
		if match == "" {
			match = dev.Address
		}
		parts = append(parts, match)
	}
	return strings.Join(parts, ",")
}

var execCmd = &cobra.Command{
	Use:   "exec [flags] -- command [args...]",
	Short: "Execute a command on the cluster",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		sessionID, _ := cmd.Flags().GetString("session")
		if sessionID == "" {
			sessionID = fmt.Sprintf("cli-%d", time.Now().Unix())
		}

		cpu, mem, err := execResourceDefaults(cmd, clusterConfigPath)
		if err != nil {
			return err
		}
		gpus, _ := cmd.Flags().GetInt("gpus")
		gpuAlias, _ := cmd.Flags().GetInt("gpu")
		if cmd.Flags().Changed("gpu") {
			if cmd.Flags().Changed("gpus") && gpuAlias != gpus {
				return fmt.Errorf("--gpu and --gpus specify different values")
			}
			gpus = gpuAlias
		}
		node, _ := cmd.Flags().GetString("node")
		cwd, _ := cmd.Flags().GetString("cwd")
		background, _ := cmd.Flags().GetBool("background")
		tty, _ := cmd.Flags().GetBool("tty")
		vfioSpecs, _ := cmd.Flags().GetStringArray("vfio")
		vfioReqs, err := parseVFIORequirements(vfioSpecs)
		if err != nil {
			return err
		}
		remoteChildren, _ := cmd.Flags().GetString("remote-children")
		remoteIPCCompat, _ := cmd.Flags().GetString("remote-ipc-compat")
		remoteFileLocks, _ := cmd.Flags().GetBool("remote-file-locks")
		remoteEnv, err := remoteChildrenEnvironment(remoteChildren, remoteIPCCompat, remoteFileLocks)
		if err != nil {
			return err
		}

		req := &octopospb.ExecuteRequest{
			SessionId: sessionID,
			JobId:     fmt.Sprintf("job-%d", time.Now().UnixNano()),
			Command:   args,
			Cwd:       cwd,
			Stdin:     !background,
			Stdout:    !background,
			Stderr:    !background,
			Tty:       !background && tty,
			Resources: &octopospb.Requirements{
				CpuMillicores: int64(cpu * 1000),
				MemoryBytes:   int64(mem * 1024 * 1024 * 1024),
				Gpus:          int32(gpus),
				VfioDevs:      vfioReqs,
			},
		}
		req.Env = append(req.Env, remoteEnv...)
		if node != "" {
			req.Resources.NodeAffinity = map[string]string{"node_id": node}
		}

		if !background {
			return execclient.RunForeground(context.Background(), client, req, execclient.Options{
				TTY:            req.Tty,
				RawTerminal:    req.Tty,
				TerminalFD:     os.Stdin.Fd(),
				ForwardSignals: true,
			})
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.Execute(ctx, req)
		if err != nil {
			return fmt.Errorf("Execute failed: %w", err)
		}
		if resp.Error != "" {
			return fmt.Errorf("Execute failed: %s", resp.Error)
		}

		fmt.Printf("Job submitted: %s (GlobalPID: %d)\n", resp.JobId, resp.GlobalPid)
		wait, _ := cmd.Flags().GetBool("wait")
		if wait {
			waitCtx, waitCancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer waitCancel()
			waitResp, err := client.Wait(waitCtx, &octopospb.WaitRequest{
				JobId:     resp.JobId,
				TimeoutMs: 60000,
			})
			if err != nil {
				return fmt.Errorf("Wait failed: %w", err)
			}
			fmt.Printf("Job completed with exit code: %d\n", waitResp.ExitCode)
			if waitResp.ExitCode != 0 {
				return &execclient.ExitError{Code: int(waitResp.ExitCode)}
			}
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

		fmt.Printf("%-20s %-15s %-8s %-14s %-10s %-10s %-24s %s\n", "GLOBAL PID", "NODE", "LOCAL PID", "KIND", "SESSION", "JOB", "REMOTE", "COMMAND")
		for _, p := range resp.Processes {
			kind := p.ProcessKind
			if kind == "" {
				kind = "process"
			}
			remote := "-"
			if child := p.RemoteChild; child != nil {
				remote = fmt.Sprintf("%d->%s/%d", child.ShadowPid, child.RemoteNodeId, child.RemoteGlobalPid)
			}
			fmt.Printf("%-20d %-15s %-8d %-14s %-10s %-10s %-24s %s\n",
				p.GlobalPid, p.NodeId, p.LocalPid, kind, p.SessionId, p.JobId, remote, p.Cmdline)
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

var clusterDoctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Validate SSI cluster readiness",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.GetClusterState(ctx, &octopospb.GetClusterStateRequest{})
		if err != nil {
			return fmt.Errorf("GetClusterState failed: %w", err)
		}
		if len(resp.Nodes) == 0 {
			return fmt.Errorf("cluster has no registered nodes")
		}

		fmt.Printf("%-20s %-8s %-16s %s\n", "NODE", "SSI", "ROOT", "ROOTFS")
		var failed []string
		for _, node := range resp.Nodes {
			ready := node.Labels[ssi.LabelReady]
			if ready == "" {
				ready = "unknown"
			}
			fmt.Printf("%-20s %-8s %-16s %s\n",
				node.NodeId,
				ready,
				node.Labels[ssi.LabelClusterRoot],
				node.Labels[ssi.LabelRootFS],
			)
			if ready != "true" {
				failed = append(failed, node.NodeId)
			}
		}
		if len(failed) > 0 {
			return fmt.Errorf("SSI is not ready on: %s", strings.Join(failed, ", "))
		}
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
		clusterRoot, _ := cmd.Flags().GetString("cluster-root")
		ssiRootFS, _ := cmd.Flags().GetString("ssi-rootfs")
		requireSSI, _ := cmd.Flags().GetBool("require-ssi")
		clusterFSMeta, _ := cmd.Flags().GetString("clusterfs-meta")
		clusterFSOpts, _ := cmd.Flags().GetString("clusterfs-options")
		objectProxy, _ := cmd.Flags().GetBool("objectstore-proxy")
		objectListen, _ := cmd.Flags().GetString("objectstore-proxy-listen")
		objectTargets, _ := cmd.Flags().GetString("objectstore-proxy-targets")
		execDefaults, err := clusterExecDefaultsForCommand(cmd, clusterConfigPath)
		if err != nil {
			return err
		}

		cfg := &bootstrapConfig{
			NodeID:        nodeID,
			WgIP:          wgIP,
			WgInterface:   "wg-octopos",
			WgListenPort:  wgPort,
			GrpcAddr:      grpcAddr,
			GrpcPort:      grpcPort,
			EnableGateway: enableGateway,
			VipIP:         vipIP,
			ClusterRoot:   clusterRoot,
			SSIRootFS:     ssiRootFS,
			RequireSSI:    requireSSI,
			ClusterFSMeta: clusterFSMeta,
			ClusterFSOpts: clusterFSOpts,
			ObjectProxy:   objectProxy,
			ObjectListen:  objectListen,
			ObjectTargets: objectTargets,
			ExecDefaults:  execDefaults,
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
	nodeAddCmd.Flags().String("local-endpoint", "", "WireGuard endpoint for this existing node (default: <hostname>:wg-port)")
	nodeAddCmd.Flags().String("seed-peer", "", "gRPC seed peer for the new node (default: local WireGuard IP:grpc-port)")
	nodeAddCmd.Flags().Int("wg-port", 51820, "WireGuard listen port")
	nodeAddCmd.Flags().Int("grpc-port", 50051, "gRPC port")
	nodeAddCmd.Flags().Bool("ebpf", false, "Build and deploy eBPF programs")
	nodeAddCmd.Flags().Bool("fuse", false, "Build and deploy FUSE daemons")
	nodeAddCmd.Flags().String("cluster-root", "/cluster", "JuiceFS cluster filesystem mount point")
	nodeAddCmd.Flags().String("ssi-rootfs", "", "SSI root filesystem path (default: <cluster-root>)")
	nodeAddCmd.Flags().Bool("require-ssi", true, "Require cluster filesystem and SSI rootfs before serving jobs")
	nodeAddCmd.Flags().String("clusterfs-meta", "", "JuiceFS metadata URL for octopos-clusterfs.service (for example tikv://10.0.0.1:2379/octopos)")
	nodeAddCmd.Flags().String("clusterfs-options", defaultClusterFSOptions, "JuiceFS mount options for octopos-clusterfs.service")
	nodeAddCmd.Flags().Bool("objectstore-proxy", true, "Install node-local HA proxy for MinIO/JuiceFS object storage")
	nodeAddCmd.Flags().String("objectstore-proxy-listen", defaultObjectStoreProxyListen, "Node-local object-store proxy listen address")
	nodeAddCmd.Flags().String("objectstore-proxy-targets", defaultObjectStoreProxyTargets, "Comma-separated MinIO backend addresses for the object-store proxy")
	nodeAddCmd.Flags().Int("default-exec-cpu", clusterconfig.DefaultExecDefaults().CPUCores, "Default CPU cores for exec when --cpu is omitted")
	nodeAddCmd.Flags().Int("default-exec-mem", clusterconfig.DefaultExecDefaults().MemoryGB, "Default memory in GB for exec when --mem is omitted")
	nodeAddCmd.Flags().Int64("cpu", 0, "Override CPU capacity in millicores (0 = auto-detect)")
	nodeAddCmd.Flags().Int64("mem", 0, "Override memory capacity in bytes (0 = auto-detect)")
	nodeAddCmd.Flags().Int32("gpus", 0, "Override GPU count")

	clusterBootstrapCmd.Flags().String("node-id", "", "Node ID (default: hostname)")
	clusterBootstrapCmd.Flags().String("wg-ip", "", "WireGuard IP (default: 10.0.0.1)")
	clusterBootstrapCmd.Flags().Int("wg-port", 51820, "WireGuard listen port")
	clusterBootstrapCmd.Flags().Int("grpc-port", 50051, "gRPC port")
	clusterBootstrapCmd.Flags().Bool("enable-gateway", true, "Deploy VIP gateway (octopos-gw) on this node")
	clusterBootstrapCmd.Flags().String("vip-ip", "10.0.0.100", "Virtual IP for cluster gateway")
	clusterBootstrapCmd.Flags().String("cluster-root", "/cluster", "JuiceFS cluster filesystem mount point")
	clusterBootstrapCmd.Flags().String("ssi-rootfs", "", "SSI root filesystem path (default: <cluster-root>)")
	clusterBootstrapCmd.Flags().Bool("require-ssi", true, "Require cluster filesystem and SSI rootfs before serving jobs")
	clusterBootstrapCmd.Flags().String("clusterfs-meta", "", "JuiceFS metadata URL for octopos-clusterfs.service (for example tikv://10.0.0.1:2379/octopos)")
	clusterBootstrapCmd.Flags().String("clusterfs-options", defaultClusterFSOptions, "JuiceFS mount options for octopos-clusterfs.service")
	clusterBootstrapCmd.Flags().Bool("objectstore-proxy", true, "Install node-local HA proxy for MinIO/JuiceFS object storage")
	clusterBootstrapCmd.Flags().String("objectstore-proxy-listen", defaultObjectStoreProxyListen, "Node-local object-store proxy listen address")
	clusterBootstrapCmd.Flags().String("objectstore-proxy-targets", defaultObjectStoreProxyTargets, "Comma-separated MinIO backend addresses for the object-store proxy")
	clusterBootstrapCmd.Flags().Int("default-exec-cpu", clusterconfig.DefaultExecDefaults().CPUCores, "Default CPU cores for exec when --cpu is omitted")
	clusterBootstrapCmd.Flags().Int("default-exec-mem", clusterconfig.DefaultExecDefaults().MemoryGB, "Default memory in GB for exec when --mem is omitted")

	nodeListCmd.Flags().StringP("output", "o", "", "Output format (json|wide)")

	execCmd.Flags().String("session", "", "Session ID (auto-generated if empty)")
	execCmd.Flags().Int("cpu", 1, "CPU cores required")
	execCmd.Flags().Int("mem", 1, "Memory required (GB)")
	execCmd.Flags().Int("gpus", 0, "GPUs required")
	execCmd.Flags().Int("gpu", 0, "GPUs required (alias for --gpus)")
	execCmd.Flags().StringArray("vfio", nil, "VFIO requirement as vendor=...,device=...,class=...,count=... (repeatable)")
	execCmd.Flags().String("node", "", "Node affinity")
	execCmd.Flags().String("cwd", "", "Working directory inside the SSI root (default: /)")
	execCmd.Flags().BoolP("interactive", "i", false, "Keep stdin open for interactive commands")
	execCmd.Flags().BoolP("tty", "t", false, "Allocate a pseudo-TTY")
	execCmd.Flags().Bool("background", false, "Submit the command as a background job")
	execCmd.Flags().Bool("wait", false, "With --background, wait for the detached job to finish")
	execCmd.Flags().String("remote-children", "off", "Remote eligible child execs with the LD_PRELOAD prototype: off, safe, or aggressive")
	execCmd.Flags().String("remote-ipc-compat", "strict", "Remote child IPC compatibility policy: strict or relaxed")
	execCmd.Flags().Bool("remote-file-locks", false, "Allow remoting inherited locked SSI files after validating cluster filesystem lock semantics")

	psCmd.Flags().String("node", "", "Filter by node")
	psCmd.Flags().String("session", "", "Filter by session")
	psCmd.Flags().String("job", "", "Filter by job")
	jobChildrenCmd.Flags().String("session", "", "Filter by session")
	jobChildrenCmd.Flags().String("node", "", "Filter by remote node")
	jobChildrenCmd.Flags().Bool("active", false, "Show only active remote children")

	vfioListCmd.Flags().String("node", "", "Filter by node")
	vfioListCmd.Flags().StringP("output", "o", "", "Output format (json)")
	vfioAllocateCmd.Flags().String("session", "", "Owning session ID")
	vfioAllocateCmd.Flags().String("job", "", "Owning job ID")
	vfioAllocateCmd.Flags().String("vendor", "", "PCI vendor ID")
	vfioAllocateCmd.Flags().String("device", "", "PCI device ID")
	vfioAllocateCmd.Flags().String("class", "", "PCI class prefix")
	vfioAllocateCmd.Flags().Int("count", 1, "Number of groups to allocate; current RPC supports 1")
	vfioReleaseCmd.Flags().String("session", "", "Owning session ID")
	vfioReleaseCmd.Flags().Int("group", 0, "VFIO group ID")

	// Build command tree
	nodeCmd.AddCommand(nodeListCmd, nodeAddCmd, nodeDrainCmd, nodeRemoveCmd)
	jobCmd.AddCommand(jobListCmd, jobStatusCmd, jobChildrenCmd)
	vfioCmd.AddCommand(vfioListCmd, vfioAllocateCmd, vfioReleaseCmd)
	sessionCmd.AddCommand(sessionListCmd, sessionCreateCmd, sessionDeleteCmd)
	clusterCmd.AddCommand(clusterStatusCmd, clusterDoctorCmd, clusterBootstrapCmd)

	rootCmd.AddCommand(nodeCmd, jobCmd, execCmd, sessionCmd, psCmd, vfioCmd, clusterCmd)

	if err := rootCmd.Execute(); err != nil {
		var exitErr *execclient.ExitError
		if errors.As(err, &exitErr) {
			os.Exit(exitErr.Code)
		}
		log.Fatal(err)
	}
}
