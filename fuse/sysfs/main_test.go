package main

import (
	"context"
	"strings"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func TestNewSysRootStoresConfiguredResources(t *testing.T) {
	root := newSysRoot(16, 64*1024*1024*1024, 2)

	if root.cpus != 16 {
		t.Fatalf("cpus = %d", root.cpus)
	}
	if root.memory != 64*1024*1024*1024 {
		t.Fatalf("memory = %d", root.memory)
	}
	if root.gpus != 2 {
		t.Fatalf("gpus = %d", root.gpus)
	}
}

func TestSysRootReaddirIncludesTopLevelDirectories(t *testing.T) {
	root := newSysRoot(8, 32*1024*1024*1024, 0)

	stream, errno := root.Readdir(context.Background())
	names := sysDirEntryNames(t, stream, errno)

	for _, want := range []string{"class", "devices", "kernel", "fs", "module", "cpu"} {
		if !names[want] {
			t.Fatalf("directory listing missing %q: %#v", want, names)
		}
	}
}

func TestSysFileReadAndAttributes(t *testing.T) {
	file := &sysFile{name: "osrelease", parent: "kernel"}

	var attrs fuse.AttrOut
	if errno := file.Getattr(context.Background(), nil, &attrs); errno != 0 {
		t.Fatalf("Getattr errno = %v", errno)
	}
	if attrs.Mode != syscall.S_IFREG|0444 {
		t.Fatalf("mode = %#o", attrs.Mode)
	}

	result, errno := file.Read(context.Background(), nil, make([]byte, 256), 0)
	data := sysReadResultBytes(t, result, errno)
	if !strings.Contains(string(data), "6.8.0-octopos") {
		t.Fatalf("osrelease content = %q", data)
	}

	ostype := &sysFile{name: "ostype", parent: "kernel"}
	result, errno = ostype.Read(context.Background(), nil, make([]byte, 4), 1)
	offsetData := sysReadResultBytes(t, result, errno)
	if got, want := string(offsetData), "inux"; got != want {
		t.Fatalf("offset read = %q, want %q", got, want)
	}
}

func sysDirEntryNames(t *testing.T, stream fs.DirStream, errno syscall.Errno) map[string]bool {
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

func sysReadResultBytes(t *testing.T, result fuse.ReadResult, errno syscall.Errno) []byte {
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
