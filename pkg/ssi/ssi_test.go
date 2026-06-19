package ssi

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveClusterCWD(t *testing.T) {
	tests := []struct {
		name        string
		cwd         string
		wantHost    string
		wantLogical string
	}{
		{name: "default root", cwd: "", wantHost: "/cluster", wantLogical: "/"},
		{name: "absolute root", cwd: "/", wantHost: "/cluster", wantLogical: "/"},
		{name: "cluster alias", cwd: "/cluster/home", wantHost: "/cluster/home", wantLogical: "/home"},
		{name: "relative", cwd: "var/tmp", wantHost: "/cluster/var/tmp", wantLogical: "/var/tmp"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, logical, err := ResolveClusterCWD("/cluster", "/cluster", tt.cwd)
			if err != nil {
				t.Fatalf("ResolveClusterCWD: %v", err)
			}
			if host != tt.wantHost {
				t.Fatalf("host = %q, want %q", host, tt.wantHost)
			}
			if logical != tt.wantLogical {
				t.Fatalf("logical = %q, want %q", logical, tt.wantLogical)
			}
		})
	}
}

func TestValidateRequiresBootstrappedRootAndExecutor(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "usr/bin"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "usr/lib"), 0755); err != nil {
		t.Fatal(err)
	}
	executor := filepath.Join(t.TempDir(), "octopos-exec")
	if err := os.WriteFile(executor, []byte("#!/bin/sh\n"), 0755); err != nil {
		t.Fatal(err)
	}

	err := Validate(Config{
		ClusterRoot:  root,
		RootFS:       root,
		Executor:     executor,
		RequireMount: false,
		Required:     true,
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
}
