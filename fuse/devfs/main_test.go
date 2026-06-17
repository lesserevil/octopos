package main

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func TestNewDevRootAddsBaseAndVFIODevices(t *testing.T) {
	root := newDevRoot("3, 7, invalid")

	for _, want := range []string{"null", "zero", "random", "urandom", "tty", "ptmx", "kvm", "vfio/3", "dri/renderD131", "vfio/7", "dri/renderD135"} {
		if !hasDevice(root.devices, want) {
			t.Fatalf("expected device %q in %#v", want, root.devices)
		}
	}
	if hasDevice(root.devices, "vfio/invalid") {
		t.Fatal("invalid VFIO group was added as a device")
	}
}

func TestDevRootReaddirIncludesVFIOOnlyWhenConfigured(t *testing.T) {
	stream, errno := newDevRoot("").Readdir(context.Background())
	withoutVFIO := devDirEntryNames(t, stream, errno)
	if withoutVFIO["vfio"] || withoutVFIO["dri"] {
		t.Fatalf("unexpected VFIO directories without groups: %#v", withoutVFIO)
	}

	stream, errno = newDevRoot("2").Readdir(context.Background())
	withVFIO := devDirEntryNames(t, stream, errno)
	for _, want := range []string{"null", "zero", "random", "urandom", "tty", "ptmx", "kvm", "pts", "shm", "vfio", "dri"} {
		if !withVFIO[want] {
			t.Fatalf("directory listing missing %q: %#v", want, withVFIO)
		}
	}
}

func TestDevNodeGetattrUsesCharacterDeviceMetadata(t *testing.T) {
	node := &devNode{dev: deviceEntry{name: "kvm", major: 10, minor: 232, devType: "char"}}

	var attrs fuse.AttrOut
	if errno := node.Getattr(context.Background(), nil, &attrs); errno != 0 {
		t.Fatalf("Getattr errno = %v", errno)
	}
	if attrs.Mode != syscall.S_IFCHR|0666 {
		t.Fatalf("mode = %#o", attrs.Mode)
	}
	if got, want := attrs.Rdev, uint32(10<<20|232); got != want {
		t.Fatalf("rdev = %d, want %d", got, want)
	}
}

func hasDevice(devices []deviceEntry, name string) bool {
	for _, dev := range devices {
		if dev.name == name {
			return true
		}
	}
	return false
}

func devDirEntryNames(t *testing.T, stream fs.DirStream, errno syscall.Errno) map[string]bool {
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
