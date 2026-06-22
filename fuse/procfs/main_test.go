package main

import (
	"context"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	octopospb "github.com/octopos/octopos/pkg/rpc"
)

func TestProcRootOnAddPopulatesSyntheticProcesses(t *testing.T) {
	root := &procRoot{pidMap: make(map[uint32]procInfo)}

	root.OnAdd(context.Background())

	pid := uint32(os.Getpid())
	if got := root.pidMap[pid]; got.comm != "octopos-procfs" || got.ppid != uint32(os.Getppid()) {
		t.Fatalf("current process entry = %#v", got)
	}
	if got := root.pidMap[1]; got.comm != "octopos-init" || got.ppid != 0 {
		t.Fatalf("init process entry = %#v", got)
	}
}

func TestProcRootReaddirIncludesStaticFilesAndProcesses(t *testing.T) {
	root := &procRoot{
		pidMap: map[uint32]procInfo{
			42: {pid: 42, ppid: 1, comm: "job", cmdline: "job"},
		},
	}

	stream, errno := root.Readdir(context.Background())
	names := dirEntryNames(t, stream, errno)

	for _, want := range []string{"self", "cpuinfo", "meminfo", "uptime", "stat", "loadavg", "version", "mounts", "filesystems", "42"} {
		if !names[want] {
			t.Fatalf("directory listing missing %q: %#v", want, names)
		}
	}
}

func TestProcDirReaddirIncludesExpectedProcessFiles(t *testing.T) {
	dir := &procDir{info: procInfo{pid: 42, ppid: 1, comm: "job", cmdline: "sleep 1"}}

	stream, errno := dir.Readdir(context.Background())
	names := dirEntryNames(t, stream, errno)

	for _, want := range []string{"status", "comm", "cmdline", "cwd", "exe", "octopos", "mounts", "mountinfo", "mountstats", "fd", "ns"} {
		if !names[want] {
			t.Fatalf("process directory listing missing %q: %#v", want, names)
		}
	}
}

func TestProcFileReadAndAttributes(t *testing.T) {
	file := &procFile{
		name: "status",
		info: procInfo{pid: 42, ppid: 1, uid: 1000, comm: "worker"},
	}

	var attrs fuse.AttrOut
	if errno := file.Getattr(context.Background(), nil, &attrs); errno != 0 {
		t.Fatalf("Getattr errno = %v", errno)
	}
	if attrs.Mode != syscall.S_IFREG|0444 {
		t.Fatalf("mode = %#o", attrs.Mode)
	}

	result, errno := file.Read(context.Background(), nil, make([]byte, 128), 0)
	data := readResultBytes(t, result, errno)
	for _, want := range []string{"Name:\tworker", "Pid:\t42", "PPid:\t1", "Uid:\t1000"} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("status content missing %q:\n%s", want, data)
		}
	}

	comm := &procFile{name: "comm", info: procInfo{comm: "worker"}}
	result, errno = comm.Read(context.Background(), nil, make([]byte, 3), 1)
	offsetData := readResultBytes(t, result, errno)
	if got, want := string(offsetData), "ork"; got != want {
		t.Fatalf("offset read = %q, want %q", got, want)
	}

	mounts := &procFile{name: "mounts"}
	result, errno = mounts.Read(context.Background(), nil, make([]byte, 512), 0)
	mountData := readResultBytes(t, result, errno)
	for _, want := range []string{"fuse.juicefs", "/proc proc", "/sys sysfs", "/dev tmpfs"} {
		if !strings.Contains(string(mountData), want) {
			t.Fatalf("mounts content missing %q:\n%s", want, mountData)
		}
	}

	filesystems := &procFile{name: "filesystems"}
	result, errno = filesystems.Read(context.Background(), nil, make([]byte, 128), 0)
	fsData := readResultBytes(t, result, errno)
	for _, want := range []string{"nodev\tproc", "fuse.juicefs"} {
		if !strings.Contains(string(fsData), want) {
			t.Fatalf("filesystems content missing %q:\n%s", want, fsData)
		}
	}
}

func TestProcFileSymlinkAttributesAndReadlink(t *testing.T) {
	cwd := &procFile{name: "cwd", info: procInfo{cwd: "/work"}}
	var attrs fuse.AttrOut
	if errno := cwd.Getattr(context.Background(), nil, &attrs); errno != 0 {
		t.Fatalf("cwd Getattr errno = %v", errno)
	}
	if attrs.Mode != syscall.S_IFLNK|0777 {
		t.Fatalf("cwd mode = %#o, want symlink", attrs.Mode)
	}
	target, errno := cwd.Readlink(context.Background())
	if errno != 0 {
		t.Fatalf("cwd Readlink errno = %v", errno)
	}
	if string(target) != "/work" {
		t.Fatalf("cwd target = %q", target)
	}

	exe := &procFile{name: "exe", info: procInfo{cmdline: "/usr/bin/python app.py"}}
	target, errno = exe.Readlink(context.Background())
	if errno != 0 {
		t.Fatalf("exe Readlink errno = %v", errno)
	}
	if string(target) != "/usr/bin/python" {
		t.Fatalf("exe target = %q", target)
	}
}

func TestProcessToProcInfoUsesShadowPIDForRemoteChild(t *testing.T) {
	info := processToProcInfo(&octopospb.ProcessInfo{
		GlobalPid:   9001,
		NodeId:      "remote-node",
		Ppid:        111,
		Uid:         1000,
		Comm:        "worker",
		Cmdline:     "worker --arg",
		Cwd:         "/work",
		State:       "running",
		ProcessKind: "remote-child",
		RemoteChild: &octopospb.RemoteChildInfo{
			ParentJobId:     "job-parent",
			ParentPid:       123,
			ShadowPid:       456,
			RemoteJobId:     "job-child",
			RemoteNodeId:    "remote-node",
			RemoteGlobalPid: 9001,
			Command:         []string{"/bin/sh", "-lc", "hostname"},
			PlacementReason: "transparent",
			State:           "running",
		},
	})

	if info.pid != 456 {
		t.Fatalf("pid = %d, want shadow pid 456", info.pid)
	}
	if info.ppid != 123 {
		t.Fatalf("ppid = %d, want parent pid 123", info.ppid)
	}
	if info.remoteNode != "remote-node" || info.remoteGlobalPID != 9001 || info.remoteJob != "job-child" {
		t.Fatalf("remote mapping = %#v", info)
	}
	if info.cmdline != "/bin/sh -lc hostname" {
		t.Fatalf("cmdline = %q", info.cmdline)
	}
}

func TestRemoteChildProcFilesExposeOctopOSMapping(t *testing.T) {
	info := procInfo{
		pid:             456,
		ppid:            123,
		uid:             1000,
		comm:            "sh",
		cmdline:         "/bin/sh -lc hostname",
		cwd:             "/work",
		state:           "running",
		processKind:     "remote-child",
		remoteParentJob: "job-parent",
		remoteJob:       "job-child",
		remoteNode:      "shedwards-octo2",
		remoteGlobalPID: 9001,
		remoteWorkerPID: 9001,
		placement:       "transparent",
	}

	statusFile := &procFile{name: "status", info: info}
	result, errno := statusFile.Read(context.Background(), nil, make([]byte, 512), 0)
	status := string(readResultBytes(t, result, errno))
	for _, want := range []string{"Pid:\t456", "PPid:\t123", "OctopOSProcessKind:\tremote-child", "OctopOSRemoteNode:\tshedwards-octo2", "OctopOSRemotePID:\t9001"} {
		if !strings.Contains(status, want) {
			t.Fatalf("status missing %q:\n%s", want, status)
		}
	}

	octoposFile := &procFile{name: "octopos", info: info}
	result, errno = octoposFile.Read(context.Background(), nil, make([]byte, 1024), 0)
	octopos := string(readResultBytes(t, result, errno))
	for _, want := range []string{"shadow_pid: 456", "remote_job_id: job-child", "remote_node: shedwards-octo2", "remote_global_pid: 9001", "placement: transparent"} {
		if !strings.Contains(octopos, want) {
			t.Fatalf("octopos metadata missing %q:\n%s", want, octopos)
		}
	}

	cwdFile := &procFile{name: "cwd", info: info}
	result, errno = cwdFile.Read(context.Background(), nil, make([]byte, 128), 0)
	if got := string(readResultBytes(t, result, errno)); got != "/work\n" {
		t.Fatalf("cwd content = %q, want /work newline", got)
	}
}

func dirEntryNames(t *testing.T, stream fs.DirStream, errno syscall.Errno) map[string]bool {
	t.Helper()
	if errno != 0 {
		t.Fatalf("Readdir errno = %v", errno)
	}
	defer stream.Close()

	names := make(map[string]bool)
	for stream.HasNext() {
		entry, errno := stream.Next()
		if errno != 0 {
			t.Fatalf("Next errno = %v", errno)
		}
		names[entry.Name] = true
	}
	return names
}

func readResultBytes(t *testing.T, result fuse.ReadResult, errno syscall.Errno) []byte {
	t.Helper()
	if errno != 0 {
		t.Fatalf("Read errno = %v", errno)
	}
	defer result.Done()

	data, status := result.Bytes(make([]byte, result.Size()))
	if status != fuse.OK {
		t.Fatalf("Read status = %v", status)
	}
	return data
}

func TestProcRootReaddirIncludesCurrentPidAfterOnAdd(t *testing.T) {
	root := &procRoot{pidMap: make(map[uint32]procInfo)}
	root.OnAdd(context.Background())

	stream, errno := root.Readdir(context.Background())
	names := dirEntryNames(t, stream, errno)
	if !names[strconv.Itoa(os.Getpid())] {
		t.Fatalf("directory listing missing current pid: %#v", names)
	}
}
