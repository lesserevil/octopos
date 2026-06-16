package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

var (
	mountPoint = flag.String("mount", "/tmp/octopos-proc", "Mount point")
	ebpfPin    = flag.String("ebpf-pin", "/sys/fs/bpf", "eBPF pin path")
	nodeID     = flag.Int("node-id", 1, "Node ID")
)

type procRoot struct {
	fs.Inode
	nodeID int
	pidMap map[uint32]procInfo
}

type procInfo struct {
	pid     uint32
	ppid    uint32
	uid     uint32
	comm    string
	cmdline string
}

type procDir struct {
	fs.Inode
	info procInfo
}

type procFile struct {
	fs.Inode
	name string
	info procInfo
}

func (r *procRoot) OnAdd(ctx context.Context) {
	pid := uint32(os.Getpid())
	ppid := uint32(os.Getppid())
	r.pidMap[pid] = procInfo{pid: pid, ppid: ppid, comm: "octopos-procfs", cmdline: "octopos-procfs"}
	r.pidMap[1] = procInfo{pid: 1, ppid: 0, comm: "octopos-init", cmdline: "/sbin/init"}
}

func (r *procRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if pid, err := strconv.ParseUint(name, 10, 32); err == nil {
		info, ok := r.pidMap[uint32(pid)]
		if !ok {
			info = procInfo{pid: uint32(pid), ppid: 1, comm: "proc-" + name, cmdline: name}
		}
		child := &procDir{info: info}
		return r.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
	}

	switch name {
	case "cpuinfo", "meminfo", "uptime", "stat", "loadavg", "version", "self":
		child := &procFile{name: name}
		return r.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG}), 0
	}

	return nil, syscall.ENOENT
}

func (r *procRoot) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: "self", Mode: syscall.S_IFLNK},
		{Name: "cpuinfo", Mode: syscall.S_IFREG},
		{Name: "meminfo", Mode: syscall.S_IFREG},
		{Name: "uptime", Mode: syscall.S_IFREG},
		{Name: "stat", Mode: syscall.S_IFREG},
		{Name: "loadavg", Mode: syscall.S_IFREG},
		{Name: "version", Mode: syscall.S_IFREG},
	}
	for pid := range r.pidMap {
		entries = append(entries, fuse.DirEntry{
			Name: strconv.FormatUint(uint64(pid), 10),
			Mode: syscall.S_IFDIR,
		})
	}
	return fs.NewListDirStream(entries), 0
}

func (d *procDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	switch name {
	case "status", "comm", "cmdline", "exe":
		child := &procFile{name: name, info: d.info}
		return d.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG}), 0
	case "fd", "fdinfo", "ns":
		child := &procDir{info: d.info}
		return d.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
	}
	return nil, syscall.ENOENT
}

func (d *procDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: "status", Mode: syscall.S_IFREG},
		{Name: "comm", Mode: syscall.S_IFREG},
		{Name: "cmdline", Mode: syscall.S_IFREG},
		{Name: "exe", Mode: syscall.S_IFLNK},
		{Name: "fd", Mode: syscall.S_IFDIR},
		{Name: "ns", Mode: syscall.S_IFDIR},
	}
	return fs.NewListDirStream(entries), 0
}

func (f *procFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFREG | 0444
	out.Size = uint64(len(f.content()))
	return 0
}

func (f *procFile) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (f *procFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	data := f.content()
	end := int(off) + len(dest)
	if end > len(data) {
		end = len(data)
	}
	if int(off) >= len(data) {
		return fuse.ReadResultData(nil), 0
	}
	return fuse.ReadResultData(data[off:end]), 0
}

func (f *procFile) content() []byte {
	switch f.name {
	case "cpuinfo":
		return []byte("processor\t: 0\nmodel name\t: OctopOS Virtual CPU\ncpu MHz\t\t: 2400.000\n")
	case "meminfo":
		total := 32 * 1024 * 1024 * 1024
		return []byte(fmt.Sprintf("MemTotal:       %12d kB\nMemFree:        %12d kB\n", total/1024, total/1024/2))
	case "uptime":
		return []byte("3600.00 1800.00\n")
	case "stat":
		return []byte("cpu  1000 0 1000 0 0 0 0 0 0 0\nintr 0\nctxt 0\nbtime 0\n")
	case "loadavg":
		return []byte("0.00 0.00 0.00 1/1 1\n")
	case "version":
		return []byte("OctopOS virtual kernel version 6.8.0-octopos\n")
	case "status":
		return []byte(fmt.Sprintf("Name:\t%s\nPid:\t%d\nPPid:\t%d\nUid:\t%d\n",
			f.info.comm, f.info.pid, f.info.ppid, f.info.uid))
	case "comm":
		return []byte(f.info.comm + "\n")
	case "cmdline":
		return []byte(f.info.cmdline + "\x00")
	case "exe":
		return []byte("/proc/self/exe")
	}
	return nil
}

func main() {
	flag.Parse()

	if err := os.MkdirAll(*mountPoint, 0755); err != nil {
		log.Fatalf("mkdir mount point: %v", err)
	}

	root := &procRoot{
		nodeID: *nodeID,
		pidMap: make(map[uint32]procInfo),
	}

	server, err := fs.Mount(*mountPoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug:   false,
			FsName:  "octopos-procfs",
			Name:    "octopos-procfs",
			Options: []string{"allow_other", "ro"},
		},
	})
	if err != nil {
		log.Fatalf("mount: %v", err)
	}

	log.Printf("OctopOS procfs mounted at %s", *mountPoint)
	server.Wait()
}
