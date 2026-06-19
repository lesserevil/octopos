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

	for _, want := range []string{"status", "comm", "cmdline", "exe", "mounts", "mountinfo", "mountstats", "fd", "ns"} {
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
