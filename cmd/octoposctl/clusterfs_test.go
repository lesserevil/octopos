package main

import (
	"strings"
	"testing"
)

func TestClusterFSUnitContent(t *testing.T) {
	unit := clusterFSUnitContent("/cluster", "tikv://10.0.0.1:2379/octopos", "", false)
	for _, want := range []string{
		"Description=OctopOS JuiceFS cluster filesystem",
		"ExecStartPre=/bin/mkdir -p /cluster",
		"ExecStart=/usr/local/bin/juicefs mount --cache-large-write -o allow_other,dev,suid tikv://10.0.0.1:2379/octopos /cluster",
		"mountpoint -q /cluster",
		"Restart=on-failure",
	} {
		if !strings.Contains(unit, want) {
			t.Fatalf("unit missing %q:\n%s", want, unit)
		}
	}
}

func TestClusterFSUnitContentWithObjectStoreProxy(t *testing.T) {
	unit := clusterFSUnitContent("/cluster", "tikv://10.0.0.1:2379/octopos", "", true)
	for _, want := range []string{
		"After=network-online.target docker.service octopos-objectstore-proxy.service",
		"Wants=network-online.target docker.service octopos-objectstore-proxy.service",
	} {
		if !strings.Contains(unit, want) {
			t.Fatalf("unit missing %q:\n%s", want, unit)
		}
	}
}
