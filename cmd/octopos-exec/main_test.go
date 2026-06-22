package main

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/octopos/octopos/pkg/cluster"
	"github.com/octopos/octopos/pkg/remotechild"
	"golang.org/x/sys/unix"
)

func TestNormalizeHostname(t *testing.T) {
	tests := map[string]string{
		"OctoPOS":                "octopos",
		" shedwards-octo1 ":      "shedwards-octo1",
		"bad host/name":          "bad-host-name",
		"..cluster..":            "cluster",
		"DOCTYPE HTML PUBLIC...": "doctype-html-public",
		"0123456789012345678901234567890123456789012345678901234567890123456789": "012345678901234567890123456789012345678901234567890123456789012",
	}

	for input, want := range tests {
		if got := normalizeHostname(input); got != want {
			t.Fatalf("normalizeHostname(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestCreateDeviceNodeRestoresModeAfterUmask(t *testing.T) {
	oldMknod := mknod
	oldChmod := chmod
	t.Cleanup(func() {
		mknod = oldMknod
		chmod = oldChmod
	})

	var gotMode int
	var gotDev int
	mknod = func(path string, mode uint32, dev int) error {
		gotMode = int(mode)
		gotDev = dev
		return os.WriteFile(path, []byte{}, 0644)
	}
	chmod = os.Chmod

	path := filepath.Join(t.TempDir(), "null")
	if err := createDeviceNode(path, deviceNode{name: "null", mode: 0666, major: 1, minor: 3}); err != nil {
		t.Fatalf("createDeviceNode: %v", err)
	}

	if gotMode&unix.S_IFMT != unix.S_IFCHR {
		t.Fatalf("mknod mode type = %#o, want char device", gotMode)
	}
	if gotMode&0777 != 0666 {
		t.Fatalf("mknod perms = %#o, want 0666", gotMode&0777)
	}
	if wantDev := int(unix.Mkdev(1, 3)); gotDev != wantDev {
		t.Fatalf("device number = %d, want %d", gotDev, wantDev)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat created node: %v", err)
	}
	if got := info.Mode().Perm(); got != 0666 {
		t.Fatalf("final mode = %#o, want 0666", got)
	}
}

func TestEnsureSSIRootDirAcceptsExistingDirectory(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "proc"), 0755); err != nil {
		t.Fatalf("mkdir proc: %v", err)
	}

	if err := ensureSSIRootDir(root, "proc"); err != nil {
		t.Fatalf("ensureSSIRootDir: %v", err)
	}
}

func TestEnsureSSIRootDirRejectsFile(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "proc"), []byte("not a directory"), 0644); err != nil {
		t.Fatalf("write proc file: %v", err)
	}

	err := ensureSSIRootDir(root, "proc")
	if err == nil {
		t.Fatal("ensureSSIRootDir succeeded for file")
	}
	if !errors.Is(err, syscall.ENOTDIR) && !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("ensureSSIRootDir error = %v, want not a directory", err)
	}
}

func TestApplyNVIDIAEnv(t *testing.T) {
	env := []string{
		"PATH=/usr/bin:/bin",
		"LD_LIBRARY_PATH=/lib",
		"NVIDIA_VISIBLE_DEVICES=all",
	}
	got := applyNVIDIAEnv(env, nvidiaRuntimeConfig{
		devices: []cluster.GPUDevice{
			{Index: 2, UUID: "GPU-two"},
		},
		capabilities: "compute,utility",
	})

	if value := envValue(got, "NVIDIA_VISIBLE_DEVICES"); value != "GPU-two" {
		t.Fatalf("NVIDIA_VISIBLE_DEVICES = %q, want GPU-two", value)
	}
	if value := envValue(got, "CUDA_VISIBLE_DEVICES"); value != "GPU-two" {
		t.Fatalf("CUDA_VISIBLE_DEVICES = %q, want GPU-two", value)
	}
	if value := envValue(got, "NVIDIA_DRIVER_CAPABILITIES"); value != "compute,utility" {
		t.Fatalf("NVIDIA_DRIVER_CAPABILITIES = %q, want compute,utility", value)
	}
	if value := envValue(got, "PATH"); !strings.HasPrefix(value, "/usr/local/nvidia/bin:/usr/local/cuda/bin:") {
		t.Fatalf("PATH = %q, missing NVIDIA/CUDA prefixes", value)
	}
	if value := envValue(got, "LD_LIBRARY_PATH"); !strings.HasPrefix(value, "/usr/local/nvidia/lib64:/usr/local/cuda/lib64:") {
		t.Fatalf("LD_LIBRARY_PATH = %q, missing NVIDIA/CUDA prefixes", value)
	}
}

func TestApplyParentStdioPipeEnvMarksPipeFDs(t *testing.T) {
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readEnd.Close()
	defer writeEnd.Close()

	savedStdout, err := unix.Dup(1)
	if err != nil {
		t.Fatalf("dup stdout: %v", err)
	}
	defer unix.Close(savedStdout)
	if err := unix.Dup2(int(writeEnd.Fd()), 1); err != nil {
		t.Fatalf("replace stdout: %v", err)
	}
	defer unix.Dup2(savedStdout, 1)

	markers := parentStdioPipeEnvFromCurrentProcess()
	got := applyParentStdioPipeEnv([]string{remotechild.EnvParentStdioPipeFD(1) + "=stale"}, markers)
	if value := envValue(got, remotechild.EnvParentStdioPipeFD(1)); value == "" || value == "stale" {
		t.Fatalf("%s = %q, want detected pipe id", remotechild.EnvParentStdioPipeFD(1), value)
	}
}

func TestParseVFIOGroups(t *testing.T) {
	groups, err := parseVFIOGroups("7, 8,7")
	if err != nil {
		t.Fatalf("parseVFIOGroups: %v", err)
	}
	if len(groups) != 2 || groups[0] != 7 || groups[1] != 8 {
		t.Fatalf("groups = %+v, want [7 8]", groups)
	}
}

func TestParseVFIOGroupsRejectsInvalid(t *testing.T) {
	for _, spec := range []string{"abc", "0", "-1"} {
		if _, err := parseVFIOGroups(spec); err == nil {
			t.Fatalf("parseVFIOGroups(%q) succeeded, want error", spec)
		}
	}
}

func TestApplyFDReopenPlan(t *testing.T) {
	target := filepath.Join(t.TempDir(), "fd-target")
	if err := os.WriteFile(target, []byte("start"), 0600); err != nil {
		t.Fatal(err)
	}
	fd := 9
	_ = unix.Close(fd)
	encoded, err := remotechild.EncodeReopenFDs([]remotechild.ReopenFD{{
		FD:     fd,
		Path:   target,
		Flags:  os.O_WRONLY | os.O_APPEND,
		Offset: 0,
		Kind:   remotechild.FDKindRegular,
	}})
	if err != nil {
		t.Fatalf("EncodeReopenFDs: %v", err)
	}
	defer unix.Close(fd)
	if err := applyFDReopenPlan([]string{remotechild.EnvFDPlan + "=" + encoded}); err != nil {
		t.Fatalf("applyFDReopenPlan: %v", err)
	}
	file := os.NewFile(uintptr(fd), "fd-target")
	if file == nil {
		t.Fatal("fd 9 was not opened")
	}
	if _, err := file.Write([]byte("-remote")); err != nil {
		t.Fatalf("write reopened fd: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close reopened fd: %v", err)
	}
	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if string(data) != "start-remote" {
		t.Fatalf("target data = %q, want start-remote", string(data))
	}
}

func TestCreateBindFileTargetReplacesStaleSocket(t *testing.T) {
	target := filepath.Join(t.TempDir(), "childd.sock")
	listener, err := net.Listen("unix", target)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	file, err := createBindFileTarget(target)
	if err != nil {
		t.Fatalf("createBindFileTarget: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close target: %v", err)
	}
	info, err := os.Lstat(target)
	if err != nil {
		t.Fatalf("stat target: %v", err)
	}
	if info.Mode()&os.ModeSocket != 0 {
		t.Fatalf("target is still a socket: %s", info.Mode())
	}
}

func TestCreateBindDirTargetReplacesStaleFile(t *testing.T) {
	target := filepath.Join(t.TempDir(), "host-fd")
	if err := os.WriteFile(target, []byte("stale"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := createBindDirTarget(target); err != nil {
		t.Fatalf("createBindDirTarget: %v", err)
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatalf("stat target: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("target is not a directory: %s", info.Mode())
	}
}
