package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/octopos/octopos/pkg/remotechild"
)

func TestOctoposctlHelpStarts(t *testing.T) {
	output := runOctoposctl(t, "--help")

	for _, want := range []string{"node", "job", "exec", "session", "ps", "cluster"} {
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
	env, err := remoteChildrenEnvironment("safe", "relaxed")
	if err != nil {
		t.Fatalf("remoteChildrenEnvironment: %v", err)
	}

	for _, want := range []string{
		remotechild.EnvMode + "=safe",
		remotechild.EnvIPCCompat + "=relaxed",
		"LD_PRELOAD=" + remoteChildPreloadPath,
	} {
		if !stringSliceContains(env, want) {
			t.Fatalf("env missing %q in %#v", want, env)
		}
	}
}

func TestRemoteChildrenEnvironmentOffOmitsPreload(t *testing.T) {
	env, err := remoteChildrenEnvironment("off", "relaxed")
	if err != nil {
		t.Fatalf("remoteChildrenEnvironment: %v", err)
	}
	if len(env) != 0 {
		t.Fatalf("env = %#v, want empty", env)
	}
}

func TestRemoteChildrenEnvironmentRejectsUnknownPolicy(t *testing.T) {
	if _, err := remoteChildrenEnvironment("maybe", "strict"); err == nil {
		t.Fatal("accepted invalid remote child mode")
	}
	if _, err := remoteChildrenEnvironment("safe", "loose"); err == nil {
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
