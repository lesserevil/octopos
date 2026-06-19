package main

import (
	"os"
	"path/filepath"
	"testing"

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
