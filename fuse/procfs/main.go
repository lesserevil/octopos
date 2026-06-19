package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	octopospb "github.com/octopos/octopos/pkg/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	mountPoint = flag.String("mount", "/tmp/octopos-proc", "Mount point")
	ebpfPin    = flag.String("ebpf-pin", "/sys/fs/bpf", "eBPF pin path")
	nodeID     = flag.Int("node-id", 1, "Node ID")
	grpcAddr   = flag.String("grpc-addr", "", "octoposd gRPC address for cluster process data")
)

type procRoot struct {
	fs.Inode
	nodeID int
	pidMap map[uint32]procInfo
	client octopospb.ClusterClient
}

type procInfo struct {
	pid     uint32
	ppid    uint32
	uid     uint32
	comm    string
	cmdline string
	rss     uint64
	state   string
}

type procDir struct {
	fs.Inode
	info procInfo
}

type procFile struct {
	fs.Inode
	name string
	info procInfo
	root *procRoot
}

func (r *procRoot) OnAdd(ctx context.Context) {
	pid := uint32(os.Getpid())
	ppid := uint32(os.Getppid())
	r.pidMap[pid] = procInfo{pid: pid, ppid: ppid, comm: "octopos-procfs", cmdline: "octopos-procfs"}
	r.pidMap[1] = procInfo{pid: 1, ppid: 0, comm: "octopos-init", cmdline: "/sbin/init"}
}

func (r *procRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if pid, err := strconv.ParseUint(name, 10, 32); err == nil {
		info, ok := r.procInfo(ctx, uint32(pid))
		if !ok {
			info = procInfo{pid: uint32(pid), ppid: 1, comm: "proc-" + name, cmdline: name}
		}
		child := &procDir{info: info}
		return r.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
	}

	switch name {
	case "self":
		child := &procDir{info: procInfo{pid: uint32(os.Getpid()), ppid: uint32(os.Getppid()), comm: "self", cmdline: "self"}}
		return r.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
	case "cpuinfo", "meminfo", "uptime", "stat", "loadavg", "version", "mounts", "filesystems":
		child := &procFile{name: name, root: r}
		return r.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG}), 0
	}

	return nil, syscall.ENOENT
}

func (r *procRoot) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: "self", Mode: syscall.S_IFDIR},
		{Name: "cpuinfo", Mode: syscall.S_IFREG},
		{Name: "meminfo", Mode: syscall.S_IFREG},
		{Name: "uptime", Mode: syscall.S_IFREG},
		{Name: "stat", Mode: syscall.S_IFREG},
		{Name: "loadavg", Mode: syscall.S_IFREG},
		{Name: "version", Mode: syscall.S_IFREG},
		{Name: "mounts", Mode: syscall.S_IFREG},
		{Name: "filesystems", Mode: syscall.S_IFREG},
	}
	for pid := range r.pidMap {
		entries = append(entries, fuse.DirEntry{
			Name: strconv.FormatUint(uint64(pid), 10),
			Mode: syscall.S_IFDIR,
		})
	}
	for _, info := range r.clusterProcesses(ctx) {
		entries = append(entries, fuse.DirEntry{
			Name: strconv.FormatUint(uint64(info.pid), 10),
			Mode: syscall.S_IFDIR,
		})
	}
	return fs.NewListDirStream(entries), 0
}

func (r *procRoot) procInfo(ctx context.Context, pid uint32) (procInfo, bool) {
	if info, ok := r.pidMap[pid]; ok {
		return info, true
	}
	for _, info := range r.clusterProcesses(ctx) {
		if info.pid == pid {
			return info, true
		}
	}
	return procInfo{}, false
}

func (r *procRoot) clusterProcesses(ctx context.Context) []procInfo {
	if r.client == nil {
		return nil
	}
	callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := r.client.ListProcesses(callCtx, &octopospb.ListProcessesRequest{})
	if err != nil {
		return nil
	}
	infos := make([]procInfo, 0, len(resp.Processes))
	for _, p := range resp.Processes {
		infos = append(infos, procInfo{
			pid:     uint32(p.GlobalPid),
			ppid:    uint32(p.Ppid),
			uid:     p.Uid,
			comm:    p.Comm,
			cmdline: p.Cmdline,
			rss:     p.RssBytes,
			state:   p.State,
		})
	}
	return infos
}

func (r *procRoot) clusterMemory(ctx context.Context) uint64 {
	if r.client == nil {
		return 32 * 1024 * 1024 * 1024
	}
	callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := r.client.GetClusterState(callCtx, &octopospb.GetClusterStateRequest{})
	if err != nil {
		return 32 * 1024 * 1024 * 1024
	}
	var total uint64
	for _, node := range resp.Nodes {
		if node.Capacity != nil && node.Capacity.MemoryBytes > 0 {
			total += uint64(node.Capacity.MemoryBytes)
		}
	}
	if total == 0 {
		return 32 * 1024 * 1024 * 1024
	}
	return total
}

func (d *procDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	switch name {
	case "status", "comm", "cmdline", "exe", "mounts", "mountinfo", "mountstats":
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
		{Name: "mounts", Mode: syscall.S_IFREG},
		{Name: "mountinfo", Mode: syscall.S_IFREG},
		{Name: "mountstats", Mode: syscall.S_IFREG},
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
		total := uint64(32 * 1024 * 1024 * 1024)
		if f.root != nil {
			total = f.root.clusterMemory(context.Background())
		}
		return []byte(fmt.Sprintf("MemTotal:       %12d kB\nMemFree:        %12d kB\n", total/1024, total/1024/2))
	case "uptime":
		return []byte("3600.00 1800.00\n")
	case "stat":
		return []byte("cpu  1000 0 1000 0 0 0 0 0 0 0\nintr 0\nctxt 0\nbtime 0\n")
	case "loadavg":
		return []byte("0.00 0.00 0.00 1/1 1\n")
	case "version":
		return []byte("OctopOS virtual kernel version 6.8.0-octopos\n")
	case "mounts":
		return []byte("JuiceFS:octopos / fuse.juicefs rw,relatime 0 0\n" +
			"octopos-procfs /proc proc rw,nosuid,nodev,noexec,relatime 0 0\n" +
			"octopos-sysfs /sys sysfs rw,nosuid,nodev,noexec,relatime 0 0\n" +
			"tmpfs /dev tmpfs rw,nosuid,mode=755 0 0\n" +
			"devpts /dev/pts devpts rw,nosuid,noexec,relatime,mode=620,ptmxmode=666 0 0\n" +
			"shm /dev/shm tmpfs rw,nosuid,nodev,noexec,relatime 0 0\n")
	case "mountinfo":
		return []byte("1 0 0:1 / / rw,relatime - fuse.juicefs JuiceFS:octopos rw\n" +
			"2 1 0:2 / /proc rw,nosuid,nodev,noexec,relatime - proc octopos-procfs rw\n" +
			"3 1 0:3 / /sys rw,nosuid,nodev,noexec,relatime - sysfs octopos-sysfs rw\n" +
			"4 1 0:4 / /dev rw,nosuid - tmpfs tmpfs rw,mode=755\n" +
			"5 4 0:5 / /dev/pts rw,nosuid,noexec,relatime - devpts devpts rw,mode=620,ptmxmode=666\n" +
			"6 4 0:6 / /dev/shm rw,nosuid,nodev,noexec,relatime - tmpfs shm rw\n")
	case "mountstats":
		return []byte("")
	case "filesystems":
		return []byte("nodev\tsysfs\nnodev\tproc\nnodev\tdevpts\nnodev\ttmpfs\nfuse.juicefs\n")
	case "status":
		return []byte(fmt.Sprintf("Name:\t%s\nState:\t%s\nPid:\t%d\nPPid:\t%d\nUid:\t%d\nVmRSS:\t%d kB\n",
			f.info.comm, f.info.state, f.info.pid, f.info.ppid, f.info.uid, f.info.rss/1024))
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

	var client octopospb.ClusterClient
	var conn *grpc.ClientConn
	if *grpcAddr != "" {
		var err error
		conn, err = grpc.Dial(*grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("connect to octoposd: %v", err)
		}
		defer conn.Close()
		client = octopospb.NewClusterClient(conn)
	}

	root := &procRoot{
		nodeID: *nodeID,
		pidMap: make(map[uint32]procInfo),
		client: client,
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
