package main

import (
	"strings"
	"testing"
)

func TestObjectStoreProxyUnitContent(t *testing.T) {
	unit, err := objectStoreProxyUnitContent("127.0.0.1:19000", "10.0.0.1:9000,10.0.0.2:9000,10.0.0.3:9000")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Description=OctopOS object store proxy",
		"ExecStart=/usr/local/bin/octopos-objectstore-proxy --listen 127.0.0.1:19000 --targets 10.0.0.1:9000,10.0.0.2:9000,10.0.0.3:9000 --health-path /minio/health/cluster",
		"Restart=always",
	} {
		if !strings.Contains(unit, want) {
			t.Fatalf("unit missing %q:\n%s", want, unit)
		}
	}
}

func TestParseObjectStoreTargetsRejectsInvalidTarget(t *testing.T) {
	if _, err := parseObjectStoreTargets("10.0.0.1"); err == nil {
		t.Fatal("expected invalid target to fail")
	}
}

func TestObjectStoreProxyUnitContentRejectsInvalidListenAddress(t *testing.T) {
	if _, err := objectStoreProxyUnitContent("127.0.0.1", defaultObjectStoreProxyTargets); err == nil {
		t.Fatal("expected invalid listen address to fail")
	}
}
