package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/octopos/octopos/pkg/remotechild"
	octopospb "github.com/octopos/octopos/pkg/rpc"
)

func TestOctoposctlHelpStarts(t *testing.T) {
	output := runOctoposctl(t, "--help")

	for _, want := range []string{"node", "job", "exec", "session", "ps", "vfio", "pipe", "cluster"} {
		if !strings.Contains(output, want) {
			t.Fatalf("help output missing %q:\n%s", want, output)
		}
	}
}

func TestOctoposctlBootstrapHelpRegistersGatewayFlagsOnce(t *testing.T) {
	output := runOctoposctl(t, "cluster", "bootstrap", "--help")

	for _, flag := range []string{"--enable-gateway", "--vip-ip"} {
		if count := strings.Count(output, flag); count != 1 {
			t.Fatalf("expected %s to appear once, got %d:\n%s", flag, count, output)
		}
	}
}

func TestRemoteChildrenEnvironmentAddsIPCCompatPolicy(t *testing.T) {
	env, err := remoteChildrenEnvironment("safe", "relaxed", true)
	if err != nil {
		t.Fatalf("remoteChildrenEnvironment: %v", err)
	}

	for _, want := range []string{
		remotechild.EnvMode + "=safe",
		remotechild.EnvIPCCompat + "=relaxed",
		remotechild.EnvAllowFileLocks + "=1",
		"LD_PRELOAD=" + remoteChildPreloadPath,
	} {
		if !stringSliceContains(env, want) {
			t.Fatalf("env missing %q in %#v", want, env)
		}
	}
}

func TestRemoteChildrenEnvironmentOffOmitsPreload(t *testing.T) {
	env, err := remoteChildrenEnvironment("off", "relaxed", true)
	if err != nil {
		t.Fatalf("remoteChildrenEnvironment: %v", err)
	}
	if len(env) != 0 {
		t.Fatalf("env = %#v, want empty", env)
	}
}

func TestRemoteChildrenEnvironmentRejectsUnknownPolicy(t *testing.T) {
	if _, err := remoteChildrenEnvironment("maybe", "strict", false); err == nil {
		t.Fatal("accepted invalid remote child mode")
	}
	if _, err := remoteChildrenEnvironment("safe", "loose", false); err == nil {
		t.Fatal("accepted invalid IPC compatibility mode")
	}
}

func TestExecResourceDefaultsUsesClusterConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "octoposd.yaml")
	if err := os.WriteFile(path, []byte("exec_defaults:\n  cpu_cores: 4\n  memory_gb: 12\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := resourceDefaultsTestCommand()
	if err := cmd.Flags().Parse(nil); err != nil {
		t.Fatal(err)
	}

	cpu, mem, err := execResourceDefaults(cmd, path)
	if err != nil {
		t.Fatalf("execResourceDefaults: %v", err)
	}
	if cpu != 4 || mem != 12 {
		t.Fatalf("defaults = cpu %d mem %d, want 4/12", cpu, mem)
	}
}

func TestExecResourceDefaultsFlagsOverrideClusterConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "octoposd.yaml")
	if err := os.WriteFile(path, []byte("exec_defaults:\n  cpu_cores: 4\n  memory_gb: 12\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := resourceDefaultsTestCommand()
	if err := cmd.Flags().Parse([]string{"--cpu", "2"}); err != nil {
		t.Fatal(err)
	}

	cpu, mem, err := execResourceDefaults(cmd, path)
	if err != nil {
		t.Fatalf("execResourceDefaults: %v", err)
	}
	if cpu != 2 || mem != 12 {
		t.Fatalf("defaults = cpu %d mem %d, want 2/12", cpu, mem)
	}
}

func TestClusterExecDefaultsForCommandUsesConfigAndFlagOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), "octoposd.yaml")
	if err := os.WriteFile(path, []byte("exec_defaults:\n  cpu_cores: 4\n  memory_gb: 12\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := clusterDefaultsTestCommand()
	if err := cmd.Flags().Parse([]string{"--default-exec-mem", "6"}); err != nil {
		t.Fatal(err)
	}

	got, err := clusterExecDefaultsForCommand(cmd, path)
	if err != nil {
		t.Fatalf("clusterExecDefaultsForCommand: %v", err)
	}
	if got.CPUCores != 4 || got.MemoryGB != 6 {
		t.Fatalf("defaults = %+v, want 4 CPU / 6 GB", got)
	}
}

func TestParseVFIORequirements(t *testing.T) {
	got, err := parseVFIORequirements([]string{
		"vendor=8086,device=10fb,class=0200,count=2",
		"class=0300",
	})
	if err != nil {
		t.Fatalf("parseVFIORequirements: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("requirements = %+v, want two", got)
	}
	if got[0].VendorId != "8086" || got[0].DeviceId != "10fb" || got[0].Class != "0200" || got[0].Count != 2 {
		t.Fatalf("first requirement = %+v", got[0])
	}
	if got[1].Class != "0300" || got[1].Count != 1 {
		t.Fatalf("second requirement = %+v", got[1])
	}
}

func TestParseVFIORequirementsRejectsInvalid(t *testing.T) {
	for _, spec := range []string{"count=1", "class=0200,count=0", "foo=bar", "class"} {
		if _, err := parseVFIORequirements([]string{spec}); err == nil {
			t.Fatalf("parseVFIORequirements(%q) succeeded, want error", spec)
		}
	}
}

func TestFormatVFIORequirement(t *testing.T) {
	got := formatVFIORequirement("8086", "10fb", "0200", 1)
	if got != "vendor=8086,device=10fb,class=0200,count=1" {
		t.Fatalf("formatVFIORequirement = %q", got)
	}
}

func TestClusterVerifyHelpRegistersFlags(t *testing.T) {
	output := runOctoposctl(t, "cluster", "verify", "--help")

	for _, want := range []string{"--install-shared-helper", "--pd-endpoints", "--minio-health-url", "--local-helper"} {
		if !strings.Contains(output, want) {
			t.Fatalf("verify help missing %q:\n%s", want, output)
		}
	}
}

func TestParseSSHHostOverrides(t *testing.T) {
	got, err := parseSSHHostOverrides([]string{"node-1=10.0.0.1", "node-2=octo2"})
	if err != nil {
		t.Fatalf("parseSSHHostOverrides: %v", err)
	}
	if got["node-1"] != "10.0.0.1" || got["node-2"] != "octo2" {
		t.Fatalf("overrides = %#v", got)
	}
	if _, err := parseSSHHostOverrides([]string{"node-1"}); err == nil {
		t.Fatal("accepted invalid override")
	}
}

func TestDerivePDEndpoints(t *testing.T) {
	got := derivePDEndpoints([]*octopospb.NodeInfo{
		{NodeId: "node-1", Address: "10.0.0.1"},
		{NodeId: "node-2", Address: "10.0.0.2:1234"},
	})
	want := []string{"10.0.0.1:2379", "10.0.0.2:1234"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("endpoints = %#v, want %#v", got, want)
	}
}

func TestClusterVerifyOptionsDefaults(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().String("ssh-user", "", "")
	cmd.Flags().StringArray("ssh-host", nil, "")
	cmd.Flags().Bool("ssh-use-node-address", false, "")
	cmd.Flags().Duration("timeout", 10, "")
	cmd.Flags().String("cluster-root", "/cluster", "")
	cmd.Flags().String("rw-dir", "", "")
	cmd.Flags().String("pd-endpoints", "10.0.0.1:2379, 10.0.0.2:2379", "")
	cmd.Flags().StringArray("minio-health-url", []string{defaultVerifyMinIOHealthURL}, "")
	cmd.Flags().String("host-helper", defaultVerifyHostHelper, "")
	cmd.Flags().String("shared-helper", "", "")
	cmd.Flags().String("local-helper", "", "")
	cmd.Flags().String("expect-helper-sha", "", "")
	cmd.Flags().String("install-shared-helper", "", "")
	cmd.Flags().String("install-via", "", "")

	opts, err := clusterVerifyOptionsFromFlags(cmd)
	if err != nil {
		t.Fatalf("clusterVerifyOptionsFromFlags: %v", err)
	}
	if opts.SharedHelper != "/cluster/usr/local/bin/octopos-remote-child" {
		t.Fatalf("shared helper = %q", opts.SharedHelper)
	}
	if opts.RWDir != "/cluster/tmp" {
		t.Fatalf("rw dir = %q", opts.RWDir)
	}
	if len(opts.PDEndpoints) != 2 || opts.PDEndpoints[1] != "10.0.0.2:2379" {
		t.Fatalf("pd endpoints = %#v", opts.PDEndpoints)
	}
}

func TestHelperConsistencyRowsDetectMismatch(t *testing.T) {
	hashA := strings.Repeat("a", 64)
	hashB := strings.Repeat("b", 64)
	rows := helperConsistencyRows([]verifyRow{
		{Node: "node-1", Check: "host-helper", Status: "OK", Detail: hashA},
		{Node: "node-2", Check: "host-helper", Status: "OK", Detail: hashA},
		{Node: "node-1", Check: "shared-helper", Status: "OK", Detail: hashB},
	})
	if len(rows) != 3 {
		t.Fatalf("rows = %#v", rows)
	}
	if rows[0].Status != "OK" || rows[1].Status != "OK" || rows[2].Status != "FAIL" {
		t.Fatalf("consistency rows = %#v", rows)
	}
}

func TestVFIODeviceSummary(t *testing.T) {
	got := vfioDeviceSummary([]*octopospb.PCIDevice{{
		Address:  "0000:01:00.0",
		VendorId: "8086",
		DeviceId: "10fb",
		Class:    "020000",
	}})
	if got != "8086:10fb/020000" {
		t.Fatalf("vfioDeviceSummary = %q", got)
	}
}

func resourceDefaultsTestCommand() *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Flags().Int("cpu", 1, "")
	cmd.Flags().Int("mem", 1, "")
	return cmd
}

func clusterDefaultsTestCommand() *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Flags().Int("default-exec-cpu", 1, "")
	cmd.Flags().Int("default-exec-mem", 1, "")
	return cmd
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func runOctoposctl(t *testing.T, args ...string) string {
	t.Helper()

	cmdArgs := append([]string{"-test.run=^TestOctoposctlHelperProcess$", "--"}, args...)
	cmd := exec.Command(os.Args[0], cmdArgs...)
	cmd.Env = append(os.Environ(), "OCTOPOSCTL_HELPER_PROCESS=1")

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("octoposctl %s failed: %v\n%s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

func TestOctoposctlHelperProcess(t *testing.T) {
	if os.Getenv("OCTOPOSCTL_HELPER_PROCESS") != "1" {
		return
	}

	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = append([]string{"octoposctl"}, os.Args[i+1:]...)
			main()
			os.Exit(0)
		}
	}

	t.Fatal("missing helper process argument separator")
}
