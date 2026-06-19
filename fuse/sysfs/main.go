package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	octopospb "github.com/octopos/octopos/pkg/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	mountPoint  = flag.String("mount", "/tmp/octopos-sys", "Mount point")
	cpuCount    = flag.Int("cpus", 8, "Number of CPUs to expose")
	memoryBytes = flag.Int64("memory", 34359738368, "Memory in bytes (default 32GB)")
	gpuCount    = flag.Int("gpus", 0, "Number of GPUs")
	grpcAddr    = flag.String("grpc-addr", "", "octoposd gRPC address for cluster resource data")
)

type sysRoot struct {
	fs.Inode
	cpus   int
	memory int64
	gpus   int
	client octopospb.ClusterClient
}

type sysDir struct {
	fs.Inode
	name string
}

type sysFile struct {
	fs.Inode
	name   string
	parent string
}

func newSysRoot(cpus int, memory int64, gpus int) *sysRoot {
	return &sysRoot{cpus: cpus, memory: memory, gpus: gpus}
}

func (r *sysRoot) resources(ctx context.Context) (int, int64, int) {
	if r.client == nil {
		return r.cpus, r.memory, r.gpus
	}
	callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := r.client.GetClusterState(callCtx, &octopospb.GetClusterStateRequest{})
	if err != nil {
		return r.cpus, r.memory, r.gpus
	}
	var cpus int64
	var memory int64
	var gpus int32
	for _, node := range resp.Nodes {
		if node.Capacity == nil {
			continue
		}
		cpus += node.Capacity.CpuMillicores / 1000
		memory += node.Capacity.MemoryBytes
		gpus += node.Capacity.GpuCount
	}
	if cpus <= 0 {
		cpus = int64(r.cpus)
	}
	if memory <= 0 {
		memory = r.memory
	}
	return int(cpus), memory, int(gpus)
}

func (r *sysRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	switch name {
	case "class", "devices", "kernel", "fs", "module", "cpu", "block":
		child := &sysDir{name: name}
		return r.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
	}
	return nil, syscall.ENOENT
}

func (r *sysRoot) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: "class", Mode: syscall.S_IFDIR},
		{Name: "devices", Mode: syscall.S_IFDIR},
		{Name: "kernel", Mode: syscall.S_IFDIR},
		{Name: "fs", Mode: syscall.S_IFDIR},
		{Name: "module", Mode: syscall.S_IFDIR},
		{Name: "cpu", Mode: syscall.S_IFDIR},
	}
	return fs.NewListDirStream(entries), 0
}

func (d *sysDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	switch d.name {
	case "class":
		switch name {
		case "vfio", "drm", "kvm", "misc", "mem":
			f := &sysFile{name: name, parent: "class"}
			return d.NewInode(ctx, f, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
		}
	case "devices":
		switch name {
		case "system", "virtual", "pci0000:00":
			f := &sysFile{name: name, parent: "devices"}
			return d.NewInode(ctx, f, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
		}
	case "kernel":
		switch name {
		case "hostname", "ostype", "osrelease", "version":
			f := &sysFile{name: name, parent: "kernel"}
			return d.NewInode(ctx, f, fs.StableAttr{Mode: syscall.S_IFREG}), 0
		}
	case "cpu":
		if name == "online" {
			f := &sysFile{name: "online", parent: "cpu"}
			return d.NewInode(ctx, f, fs.StableAttr{Mode: syscall.S_IFREG}), 0
		}
	}
	return nil, syscall.ENOENT
}

func (d *sysDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	root := d.Root().Operations().(*sysRoot)

	switch d.name {
	case "class":
		entries := []fuse.DirEntry{
			{Name: "vfio", Mode: syscall.S_IFDIR},
			{Name: "drm", Mode: syscall.S_IFDIR},
			{Name: "kvm", Mode: syscall.S_IFDIR},
			{Name: "misc", Mode: syscall.S_IFDIR},
			{Name: "mem", Mode: syscall.S_IFDIR},
		}
		return fs.NewListDirStream(entries), 0
	case "devices":
		entries := []fuse.DirEntry{
			{Name: "system", Mode: syscall.S_IFDIR},
			{Name: "virtual", Mode: syscall.S_IFDIR},
			{Name: "pci0000:00", Mode: syscall.S_IFDIR},
		}
		return fs.NewListDirStream(entries), 0
	case "kernel":
		entries := []fuse.DirEntry{
			{Name: "hostname", Mode: syscall.S_IFREG},
			{Name: "ostype", Mode: syscall.S_IFREG},
			{Name: "osrelease", Mode: syscall.S_IFREG},
			{Name: "version", Mode: syscall.S_IFREG},
		}
		return fs.NewListDirStream(entries), 0
	case "cpu":
		cpus, _, _ := root.resources(ctx)
		entries := make([]fuse.DirEntry, cpus+1)
		for i := 0; i < cpus; i++ {
			entries[i] = fuse.DirEntry{
				Name: fmt.Sprintf("cpu%d", i),
				Mode: syscall.S_IFDIR,
			}
		}
		entries[cpus] = fuse.DirEntry{Name: "online", Mode: syscall.S_IFREG}
		return fs.NewListDirStream(entries), 0
	}

	return fs.NewListDirStream(nil), 0
}

func (f *sysFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFREG | 0444
	out.Size = uint64(len(f.content()))
	return 0
}

func (f *sysFile) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (f *sysFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
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

func (f *sysFile) content() []byte {
	switch f.name {
	case "hostname":
		host, _ := os.Hostname()
		return []byte(host + "\n")
	case "ostype":
		return []byte("Linux\n")
	case "osrelease":
		return []byte("6.8.0-octopos #1 SMP PREEMPT_DYNAMIC OctopOS\n")
	case "version":
		return []byte("#1 OctopOS SMP PREEMPT_DYNAMIC\n")
	case "online":
		root := f.Root().Operations().(*sysRoot)
		cpus, _, _ := root.resources(context.Background())
		if cpus <= 1 {
			return []byte("0\n")
		}
		return []byte(fmt.Sprintf("0-%d\n", cpus-1))
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

	root := newSysRoot(*cpuCount, *memoryBytes, *gpuCount)
	root.client = client

	server, err := fs.Mount(*mountPoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug:   false,
			FsName:  "octopos-sysfs",
			Name:    "octopos-sysfs",
			Options: []string{"allow_other", "ro"},
		},
	})
	if err != nil {
		log.Fatalf("mount: %v", err)
	}

	log.Printf("OctopOS sysfs mounted at %s", *mountPoint)
	server.Wait()
}
