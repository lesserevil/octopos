package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	octopospb "github.com/octopos/octopos/pkg/rpc"
	"github.com/spf13/cobra"
)

const (
	defaultVerifyClusterRoot     = "/cluster"
	defaultVerifyHostHelper      = "/usr/local/bin/octopos-remote-child"
	defaultVerifyMinIOHealthURL  = "http://127.0.0.1:19000/minio/health/cluster"
	defaultVerifyRemoteTmpPrefix = "/tmp/octopos-remote-child-"
)

type clusterVerifyOptions struct {
	SSHUser             string
	SSHHostOverrides    map[string]string
	UseNodeAddress      bool
	Timeout             time.Duration
	ClusterRoot         string
	RWDir               string
	PDEndpoints         []string
	MinIOHealthURLs     []string
	HostHelper          string
	SharedHelper        string
	LocalHelper         string
	ExpectedHelperSHA   string
	InstallSharedHelper string
	InstallVia          string
}

type verifyRow struct {
	Node   string
	Check  string
	Status string
	Detail string
}

var clusterVerifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Run bounded operational checks for the SSI cluster",
	Long: `Run bounded SSH-backed checks against each registered node.

The default mode is read-only: it verifies PD health, JuiceFS mount state,
MinIO health, shared filesystem read/write, and host/shared remote-child helper
hashes. Pass --install-shared-helper to stage a helper binary on one node,
atomically replace the shared SSI-root helper, and then verify the new hash from
all nodes.`,
	RunE: runClusterVerify,
}

func init() {
	clusterVerifyCmd.Flags().String("ssh-user", "", "SSH user for node checks (empty uses the default SSH user/config)")
	clusterVerifyCmd.Flags().StringArray("ssh-host", nil, "Override SSH host as node=host (repeatable)")
	clusterVerifyCmd.Flags().Bool("ssh-use-node-address", false, "SSH to registered node addresses instead of node IDs")
	clusterVerifyCmd.Flags().Duration("timeout", 10*time.Second, "Timeout for each bounded SSH operation")
	clusterVerifyCmd.Flags().String("cluster-root", defaultVerifyClusterRoot, "SSI cluster filesystem mount point")
	clusterVerifyCmd.Flags().String("rw-dir", "", "Directory for shared filesystem write probe (default: <cluster-root>/tmp)")
	clusterVerifyCmd.Flags().String("pd-endpoints", "", "Comma-separated PD endpoints host:port (default: registered node addresses on port 2379)")
	clusterVerifyCmd.Flags().StringArray("minio-health-url", []string{defaultVerifyMinIOHealthURL}, "Node-local MinIO health URL to check (repeatable)")
	clusterVerifyCmd.Flags().String("host-helper", defaultVerifyHostHelper, "Host helper path to hash on each node")
	clusterVerifyCmd.Flags().String("shared-helper", "", "Shared SSI-root helper path (default: <cluster-root>/usr/local/bin/octopos-remote-child)")
	clusterVerifyCmd.Flags().String("local-helper", "", "Local helper binary whose SHA256 is the expected helper hash")
	clusterVerifyCmd.Flags().String("expect-helper-sha", "", "Expected SHA256 for host/shared helper binaries")
	clusterVerifyCmd.Flags().String("install-shared-helper", "", "Local helper binary to atomically install at --shared-helper before verification")
	clusterVerifyCmd.Flags().String("install-via", "", "Node ID or SSH host to use for --install-shared-helper (default: first registered node)")
}

func runClusterVerify(cmd *cobra.Command, args []string) error {
	opts, err := clusterVerifyOptionsFromFlags(cmd)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	state, err := client.GetClusterState(ctx, &octopospb.GetClusterStateRequest{})
	if err != nil {
		return fmt.Errorf("GetClusterState failed: %w", err)
	}
	nodes := sortedVerifyNodes(state.Nodes)
	if len(nodes) == 0 {
		return fmt.Errorf("cluster has no registered nodes")
	}
	if len(opts.PDEndpoints) == 0 {
		opts.PDEndpoints = derivePDEndpoints(nodes)
	}
	if len(opts.PDEndpoints) == 0 {
		return fmt.Errorf("no PD endpoints configured or derivable from registered node addresses")
	}
	if opts.ExpectedHelperSHA == "" && opts.LocalHelper != "" {
		opts.ExpectedHelperSHA, err = fileSHA256(opts.LocalHelper)
		if err != nil {
			return fmt.Errorf("hash local helper %s: %w", opts.LocalHelper, err)
		}
	}
	if opts.InstallSharedHelper != "" {
		sha, err := fileSHA256(opts.InstallSharedHelper)
		if err != nil {
			return fmt.Errorf("hash shared helper installer %s: %w", opts.InstallSharedHelper, err)
		}
		opts.ExpectedHelperSHA = sha
		if err := installSharedHelper(context.Background(), opts, nodes, sha); err != nil {
			return err
		}
	}

	var rows []verifyRow
	for _, node := range nodes {
		nodeRows := verifyNode(context.Background(), opts, node)
		rows = append(rows, nodeRows...)
	}
	rows = append(rows, helperConsistencyRows(rows)...)
	printVerifyRows(rows)
	failures := verifyFailures(rows)
	if len(failures) > 0 {
		return fmt.Errorf("cluster verify failed: %s", strings.Join(failures, "; "))
	}
	return nil
}

func clusterVerifyOptionsFromFlags(cmd *cobra.Command) (clusterVerifyOptions, error) {
	var opts clusterVerifyOptions
	var err error
	opts.SSHUser, _ = cmd.Flags().GetString("ssh-user")
	hostOverrides, _ := cmd.Flags().GetStringArray("ssh-host")
	opts.SSHHostOverrides, err = parseSSHHostOverrides(hostOverrides)
	if err != nil {
		return opts, err
	}
	opts.UseNodeAddress, _ = cmd.Flags().GetBool("ssh-use-node-address")
	opts.Timeout, _ = cmd.Flags().GetDuration("timeout")
	if opts.Timeout <= 0 {
		return opts, fmt.Errorf("--timeout must be positive")
	}
	opts.ClusterRoot, _ = cmd.Flags().GetString("cluster-root")
	if opts.ClusterRoot == "" {
		opts.ClusterRoot = defaultVerifyClusterRoot
	}
	opts.RWDir, _ = cmd.Flags().GetString("rw-dir")
	if opts.RWDir == "" {
		opts.RWDir = filepath.Join(opts.ClusterRoot, "tmp")
	}
	pdEndpointString, _ := cmd.Flags().GetString("pd-endpoints")
	opts.PDEndpoints = splitCommaList(pdEndpointString)
	opts.MinIOHealthURLs, _ = cmd.Flags().GetStringArray("minio-health-url")
	if len(opts.MinIOHealthURLs) == 0 {
		opts.MinIOHealthURLs = []string{defaultVerifyMinIOHealthURL}
	}
	opts.HostHelper, _ = cmd.Flags().GetString("host-helper")
	opts.SharedHelper, _ = cmd.Flags().GetString("shared-helper")
	if opts.SharedHelper == "" {
		opts.SharedHelper = filepath.Join(opts.ClusterRoot, "usr/local/bin/octopos-remote-child")
	}
	opts.LocalHelper, _ = cmd.Flags().GetString("local-helper")
	opts.ExpectedHelperSHA, _ = cmd.Flags().GetString("expect-helper-sha")
	opts.InstallSharedHelper, _ = cmd.Flags().GetString("install-shared-helper")
	opts.InstallVia, _ = cmd.Flags().GetString("install-via")
	if opts.ExpectedHelperSHA != "" && !isSHA256Hex(opts.ExpectedHelperSHA) {
		return opts, fmt.Errorf("--expect-helper-sha must be a SHA256 hex digest")
	}
	return opts, nil
}

func verifyNode(ctx context.Context, opts clusterVerifyOptions, node *octopospb.NodeInfo) []verifyRow {
	nodeID := node.GetNodeId()
	var rows []verifyRow
	add := func(check, status, detail string) {
		rows = append(rows, verifyRow{Node: nodeID, Check: check, Status: status, Detail: detail})
	}
	if node.GetState() == octopospb.NodeState_NODE_STATE_ACTIVE {
		add("rpc-node", "OK", node.GetAddress())
	} else {
		add("rpc-node", "FAIL", node.GetState().String())
	}

	sshHost := verifySSHHost(opts, node)
	if _, err := runVerifySSH(ctx, opts, sshHost, "true"); err != nil {
		add("ssh", "FAIL", err.Error())
		return rows
	}
	add("ssh", "OK", sshHost)

	if out, err := runVerifySSH(ctx, opts, sshHost, "systemctl is-active octopos-clusterfs.service"); err != nil {
		add("clusterfs-service", "FAIL", strings.TrimSpace(outErrDetail(out, err)))
	} else {
		add("clusterfs-service", "OK", strings.TrimSpace(out))
	}

	mountScript := fmt.Sprintf("findmnt -T %s -n -o FSTYPE,SOURCE,OPTIONS", shellQuote(opts.ClusterRoot))
	if out, err := runVerifySSH(ctx, opts, sshHost, mountScript); err != nil {
		add("juicefs-mount", "FAIL", strings.TrimSpace(outErrDetail(out, err)))
	} else if !looksLikeJuiceFSMount(out) {
		add("juicefs-mount", "FAIL", strings.TrimSpace(out))
	} else {
		add("juicefs-mount", "OK", compactWhitespace(out))
	}

	for _, endpoint := range opts.PDEndpoints {
		status, detail := verifyPDHealth(ctx, opts, sshHost, endpoint)
		add("pd "+endpoint, status, detail)
	}
	for _, url := range opts.MinIOHealthURLs {
		script := fmt.Sprintf("curl -fsS %s", shellQuote(url))
		if out, err := runVerifySSH(ctx, opts, sshHost, script); err != nil {
			add("minio", "FAIL", url+": "+strings.TrimSpace(outErrDetail(out, err)))
		} else {
			detail := strings.TrimSpace(out)
			if detail == "" {
				detail = "healthy"
			}
			add("minio", "OK", url+": "+detail)
		}
	}

	rwScript := fmt.Sprintf(`dir=%s; sudo -n install -d -m 1777 "$dir"; tmp="$dir/.octopos-verify-$(hostname)-$$"; trap 'sudo -n rm -f "$tmp"' EXIT; printf octopos | sudo -n tee "$tmp" >/dev/null; test "$(cat "$tmp")" = octopos`,
		shellQuote(opts.RWDir),
	)
	if out, err := runVerifySSH(ctx, opts, sshHost, rwScript); err != nil {
		add("cluster-rw", "FAIL", strings.TrimSpace(outErrDetail(out, err)))
	} else {
		detail := strings.TrimSpace(out)
		if detail == "" {
			detail = opts.RWDir
		}
		add("cluster-rw", "OK", detail)
	}

	if opts.HostHelper != "" {
		status, detail := verifyRemoteSHA(ctx, opts, sshHost, opts.HostHelper, opts.ExpectedHelperSHA)
		add("host-helper", status, detail)
	}
	if opts.SharedHelper != "" {
		status, detail := verifyRemoteSHA(ctx, opts, sshHost, opts.SharedHelper, opts.ExpectedHelperSHA)
		add("shared-helper", status, detail)
	}
	return rows
}

func verifyPDHealth(ctx context.Context, opts clusterVerifyOptions, sshHost, endpoint string) (string, string) {
	url := "http://" + endpoint + "/pd/api/v1/health"
	out, err := runVerifySSH(ctx, opts, sshHost, "curl -fsS "+shellQuote(url))
	if err != nil {
		return "FAIL", strings.TrimSpace(outErrDetail(out, err))
	}
	var health []struct {
		Name   string `json:"name"`
		Health bool   `json:"health"`
	}
	if err := json.Unmarshal([]byte(out), &health); err != nil {
		return "FAIL", "parse PD health: " + err.Error()
	}
	if len(health) == 0 {
		return "FAIL", "empty PD health response"
	}
	var unhealthy []string
	for _, member := range health {
		if !member.Health {
			unhealthy = append(unhealthy, member.Name)
		}
	}
	if len(unhealthy) > 0 {
		return "FAIL", "unhealthy: " + strings.Join(unhealthy, ",")
	}
	return "OK", fmt.Sprintf("%d members healthy", len(health))
}

func verifyRemoteSHA(ctx context.Context, opts clusterVerifyOptions, sshHost, path, expected string) (string, string) {
	out, err := runVerifySSH(ctx, opts, sshHost, "sha256sum "+shellQuote(path))
	if err != nil {
		return "FAIL", strings.TrimSpace(outErrDetail(out, err))
	}
	fields := strings.Fields(out)
	if len(fields) == 0 || !isSHA256Hex(fields[0]) {
		return "FAIL", "invalid sha256sum output: " + strings.TrimSpace(out)
	}
	if expected != "" && fields[0] != expected {
		return "FAIL", fields[0] + " != " + expected
	}
	return "OK", fields[0]
}

func installSharedHelper(ctx context.Context, opts clusterVerifyOptions, nodes []*octopospb.NodeInfo, sha string) error {
	node, sshHost := installNode(opts, nodes)
	if node == "" && sshHost == "" {
		return fmt.Errorf("no node available for shared helper install")
	}
	if sshHost == "" {
		return fmt.Errorf("--install-via %q does not match a registered node or SSH override", opts.InstallVia)
	}
	remotePath := defaultVerifyRemoteTmpPrefix + sha
	fmt.Printf("Installing shared helper through %s (%s)...\n", node, sshHost)
	if err := runVerifySCP(ctx, opts, opts.InstallSharedHelper, sshHost, remotePath); err != nil {
		return fmt.Errorf("copy helper to %s: %w", sshHost, err)
	}
	dir := filepath.Dir(opts.SharedHelper)
	script := fmt.Sprintf(`set -e; src=%s; dst=%s; dir=%s; tmp="$dir/.octopos-remote-child.%s.tmp"; trap 'sudo -n rm -f "$tmp" "$src"' EXIT; sudo -n install -d "$dir"; sudo -n install -m 0755 "$src" "$tmp"; sudo -n mv -f "$tmp" "$dst"; sha256sum "$dst"`,
		shellQuote(remotePath),
		shellQuote(opts.SharedHelper),
		shellQuote(dir),
		sha,
	)
	out, err := runVerifySSH(ctx, opts, sshHost, script)
	if err != nil {
		return fmt.Errorf("install shared helper on %s: %w\n%s", sshHost, err, out)
	}
	fmt.Printf("Shared helper installed: %s", out)
	if !strings.HasSuffix(out, "\n") {
		fmt.Println()
	}
	return nil
}

func installNode(opts clusterVerifyOptions, nodes []*octopospb.NodeInfo) (string, string) {
	if opts.InstallVia == "" {
		node := nodes[0]
		return node.GetNodeId(), verifySSHHost(opts, node)
	}
	for _, node := range nodes {
		if node.GetNodeId() == opts.InstallVia || verifySSHHost(opts, node) == opts.InstallVia || node.GetAddress() == opts.InstallVia {
			return node.GetNodeId(), verifySSHHost(opts, node)
		}
	}
	if strings.Contains(opts.InstallVia, ".") || strings.Contains(opts.InstallVia, ":") {
		return opts.InstallVia, opts.InstallVia
	}
	return "", ""
}

func runVerifySSH(ctx context.Context, opts clusterVerifyOptions, host, script string) (string, error) {
	timeout := verifyTimeoutSeconds(opts.Timeout)
	remote := fmt.Sprintf("timeout %ds sh -lc %s", timeout, shellQuote(script))
	args := verifySSHArgs(opts, host, remote)
	cmdCtx, cancel := context.WithTimeout(ctx, opts.Timeout+2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cmdCtx, "ssh", args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	if cmdCtx.Err() == context.DeadlineExceeded {
		return out.String(), fmt.Errorf("timeout after %s", opts.Timeout)
	}
	return out.String(), err
}

func runVerifySCP(ctx context.Context, opts clusterVerifyOptions, src, host, dst string) error {
	args := []string{
		"-o", "BatchMode=yes",
		"-o", "ConnectTimeout=" + strconv.Itoa(verifyTimeoutSeconds(opts.Timeout)),
		src,
		verifySSHTarget(opts.SSHUser, host) + ":" + dst,
	}
	cmdCtx, cancel := context.WithTimeout(ctx, opts.Timeout+5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cmdCtx, "scp", args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		if cmdCtx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("timeout after %s", opts.Timeout)
		}
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(out.String()))
	}
	return nil
}

func verifySSHArgs(opts clusterVerifyOptions, host, remote string) []string {
	return []string{
		"-o", "BatchMode=yes",
		"-o", "ConnectTimeout=" + strconv.Itoa(verifyTimeoutSeconds(opts.Timeout)),
		verifySSHTarget(opts.SSHUser, host),
		remote,
	}
}

func verifySSHTarget(user, host string) string {
	if user == "" || strings.Contains(host, "@") {
		return host
	}
	return user + "@" + host
}

func verifySSHHost(opts clusterVerifyOptions, node *octopospb.NodeInfo) string {
	if override := opts.SSHHostOverrides[node.GetNodeId()]; override != "" {
		return override
	}
	if opts.UseNodeAddress && node.GetAddress() != "" {
		return node.GetAddress()
	}
	return node.GetNodeId()
}

func verifyTimeoutSeconds(timeout time.Duration) int {
	seconds := int(timeout.Round(time.Second) / time.Second)
	if seconds < 1 {
		return 1
	}
	return seconds
}

func sortedVerifyNodes(nodes []*octopospb.NodeInfo) []*octopospb.NodeInfo {
	out := append([]*octopospb.NodeInfo(nil), nodes...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].GetNodeId() < out[j].GetNodeId()
	})
	return out
}

func derivePDEndpoints(nodes []*octopospb.NodeInfo) []string {
	var endpoints []string
	for _, node := range nodes {
		addr := node.GetAddress()
		if addr == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(addr); err == nil {
			endpoints = append(endpoints, addr)
			continue
		}
		endpoints = append(endpoints, net.JoinHostPort(addr, "2379"))
	}
	return endpoints
}

func parseSSHHostOverrides(values []string) (map[string]string, error) {
	overrides := make(map[string]string, len(values))
	for _, value := range values {
		node, host, ok := strings.Cut(value, "=")
		if !ok || strings.TrimSpace(node) == "" || strings.TrimSpace(host) == "" {
			return nil, fmt.Errorf("invalid --ssh-host %q, want node=host", value)
		}
		overrides[strings.TrimSpace(node)] = strings.TrimSpace(host)
	}
	return overrides, nil
}

func splitCommaList(value string) []string {
	var out []string
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func isSHA256Hex(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func looksLikeJuiceFSMount(output string) bool {
	fields := strings.Fields(output)
	if len(fields) < 2 {
		return false
	}
	fstype := fields[0]
	source := fields[1]
	return strings.Contains(fstype, "fuse.juicefs") || strings.Contains(source, "JuiceFS")
}

func compactWhitespace(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func outErrDetail(out string, err error) string {
	out = strings.TrimSpace(out)
	if out == "" {
		return err.Error()
	}
	return err.Error() + ": " + out
}

func printVerifyRows(rows []verifyRow) {
	fmt.Printf("%-20s %-28s %-6s %s\n", "NODE", "CHECK", "STATUS", "DETAIL")
	for _, row := range rows {
		fmt.Printf("%-20s %-28s %-6s %s\n", row.Node, row.Check, row.Status, row.Detail)
	}
}

func helperConsistencyRows(rows []verifyRow) []verifyRow {
	hostHashes := hashesForCheck(rows, "host-helper")
	sharedHashes := hashesForCheck(rows, "shared-helper")
	out := make([]verifyRow, 0, 3)
	if len(hostHashes) > 0 {
		out = append(out, consistencyRow("cluster", "host-helper-consistency", hostHashes))
	}
	if len(sharedHashes) > 0 {
		out = append(out, consistencyRow("cluster", "shared-helper-consistency", sharedHashes))
	}
	if len(hostHashes) == 1 && len(sharedHashes) == 1 {
		hostHash := onlyHash(hostHashes)
		sharedHash := onlyHash(sharedHashes)
		if hostHash == sharedHash {
			out = append(out, verifyRow{Node: "cluster", Check: "helper-consistency", Status: "OK", Detail: hostHash})
		} else {
			out = append(out, verifyRow{Node: "cluster", Check: "helper-consistency", Status: "FAIL", Detail: "host " + hostHash + " != shared " + sharedHash})
		}
	}
	return out
}

func hashesForCheck(rows []verifyRow, check string) map[string]int {
	hashes := map[string]int{}
	for _, row := range rows {
		if row.Check == check && row.Status == "OK" && isSHA256Hex(row.Detail) {
			hashes[row.Detail]++
		}
	}
	return hashes
}

func consistencyRow(node, check string, hashes map[string]int) verifyRow {
	if len(hashes) == 1 {
		return verifyRow{Node: node, Check: check, Status: "OK", Detail: onlyHash(hashes)}
	}
	parts := make([]string, 0, len(hashes))
	for hash, count := range hashes {
		parts = append(parts, fmt.Sprintf("%s(%d)", hash, count))
	}
	sort.Strings(parts)
	return verifyRow{Node: node, Check: check, Status: "FAIL", Detail: strings.Join(parts, ", ")}
}

func onlyHash(hashes map[string]int) string {
	for hash := range hashes {
		return hash
	}
	return ""
}

func verifyFailures(rows []verifyRow) []string {
	var failures []string
	for _, row := range rows {
		if row.Status != "OK" {
			failures = append(failures, fmt.Sprintf("%s/%s: %s", row.Node, row.Check, row.Detail))
		}
	}
	return failures
}
