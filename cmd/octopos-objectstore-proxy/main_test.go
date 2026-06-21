package main

import "testing"

func TestParseBackends(t *testing.T) {
	backends, err := parseBackends("10.0.0.1:9000, 10.0.0.2:9000")
	if err != nil {
		t.Fatal(err)
	}
	if len(backends) != 2 {
		t.Fatalf("expected 2 backends, got %d", len(backends))
	}
	if backends[0].url.String() != "http://10.0.0.1:9000" {
		t.Fatalf("unexpected backend URL: %s", backends[0].url)
	}
}

func TestParseBackendsRejectsInvalidTarget(t *testing.T) {
	if _, err := parseBackends("10.0.0.1"); err == nil {
		t.Fatal("expected invalid target to fail")
	}
}

func TestPickReturnsHealthyBackend(t *testing.T) {
	backends, err := parseBackends("10.0.0.1:9000,10.0.0.2:9000")
	if err != nil {
		t.Fatal(err)
	}
	backends[1].healthy.Store(true)
	state := &proxyState{backends: backends}
	if got := state.pick(); got == nil || got.raw != "10.0.0.2:9000" {
		t.Fatalf("expected healthy backend 10.0.0.2:9000, got %#v", got)
	}
}

func TestSingleJoiningSlash(t *testing.T) {
	if got := singleJoiningSlash("/base/", "/object"); got != "/base/object" {
		t.Fatalf("unexpected joined path: %q", got)
	}
	if got := singleJoiningSlash("/base", "object"); got != "/base/object" {
		t.Fatalf("unexpected joined path: %q", got)
	}
}
