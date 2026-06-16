package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

var (
	mountPoint = flag.String("mount", "/tmp/octopos-dev", "Mount point")
	vfioGroups = flag.String("vfio-groups", "", "Comma-separated VFIO group IDs to expose")
)

type devRoot struct {
	fs.Inode
	devices []deviceEntry
}

type deviceEntry struct {
	name    string
	major   uint32
	minor   uint32
	devType string
}

type devDir struct {
	fs.Inode
	name string
}

type devNode struct {
	fs.Inode
	dev deviceEntry
}

func newDevRoot(vfioGroupsStr string) *devRoot {
	r := &devRoot{}
	if vfioGroupsStr != "" {
		for _, g := range strings.Split(vfioGroupsStr, ",") {
			g = strings.TrimSpace(g)
			if gid, err := strconv.Atoi(g); err == nil {
				r.devices = append(r.devices, deviceEntry{
					name:    fmt.Sprintf("vfio/%d", gid),
					major:   10,
					minor:   uint32(gid) + 196,
					devType: "char",
				})
				r.devices = append(r.devices, deviceEntry{
					name:    fmt.Sprintf("dri/renderD%d", gid+128),
					major:   226,
					minor:   uint32(gid) + 128,
					devType: "char",
				})
			}
		}
	}
	r.devices = append(r.devices,
		deviceEntry{name: "null", major: 1, minor: 3, devType: "char"},
		deviceEntry{name: "zero", major: 1, minor: 5, devType: "char"},
		deviceEntry{name: "random", major: 1, minor: 8, devType: "char"},
		deviceEntry{name: "urandom", major: 1, minor: 9, devType: "char"},
		deviceEntry{name: "tty", major: 5, minor: 0, devType: "char"},
		deviceEntry{name: "ptmx", major: 5, minor: 2, devType: "char"},
		deviceEntry{name: "kvm", major: 10, minor: 232, devType: "char"},
	)
	return r
}

func (r *devRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	switch name {
	case "null", "zero", "random", "urandom", "tty", "ptmx", "kvm":
		for _, d := range r.devices {
			if d.name == name {
				child := &devNode{dev: d}
				return r.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFCHR}), 0
			}
		}
	case "vfio", "dri", "pts", "shm":
		child := &devDir{name: name}
		return r.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
	}
	return nil, syscall.ENOENT
}

func (r *devRoot) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: "null", Mode: syscall.S_IFCHR},
		{Name: "zero", Mode: syscall.S_IFCHR},
		{Name: "random", Mode: syscall.S_IFCHR},
		{Name: "urandom", Mode: syscall.S_IFCHR},
		{Name: "tty", Mode: syscall.S_IFCHR},
		{Name: "ptmx", Mode: syscall.S_IFCHR},
		{Name: "kvm", Mode: syscall.S_IFCHR},
		{Name: "pts", Mode: syscall.S_IFDIR},
		{Name: "shm", Mode: syscall.S_IFDIR},
	}
	if r.hasVFIO() {
		entries = append(entries, fuse.DirEntry{Name: "vfio", Mode: syscall.S_IFDIR})
		entries = append(entries, fuse.DirEntry{Name: "dri", Mode: syscall.S_IFDIR})
	}
	return fs.NewListDirStream(entries), 0
}

func (r *devRoot) hasVFIO() bool {
	for _, d := range r.devices {
		if strings.HasPrefix(d.name, "vfio/") || strings.HasPrefix(d.name, "dri/") {
			return true
		}
	}
	return false
}

func (d *devDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.ENOENT
}

func (d *devDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return fs.NewListDirStream(nil), 0
}

func (n *devNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFCHR | 0666
	out.Rdev = (n.dev.major << 20) | n.dev.minor
	return 0
}

func (n *devNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	return nil, 0, 0
}

func main() {
	flag.Parse()

	if err := os.MkdirAll(*mountPoint, 0755); err != nil {
		log.Fatalf("mkdir mount point: %v", err)
	}

	root := newDevRoot(*vfioGroups)

	server, err := fs.Mount(*mountPoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug:   false,
			FsName:  "octopos-devfs",
			Name:    "octopos-devfs",
			Options: []string{"allow_other", "ro"},
		},
	})
	if err != nil {
		log.Fatalf("mount: %v", err)
	}

	log.Printf("OctopOS devfs mounted at %s", *mountPoint)
	server.Wait()
}
