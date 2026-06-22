package main

import "testing"

func TestFIFOModeFD(t *testing.T) {
	tests := []struct {
		mode string
		want int
	}{
		{mode: "read", want: 0},
		{mode: "write", want: 1},
		{mode: " READ ", want: 0},
	}
	for _, tt := range tests {
		got, err := fifoModeFD(tt.mode)
		if err != nil {
			t.Fatalf("fifoModeFD(%q): %v", tt.mode, err)
		}
		if got != tt.want {
			t.Fatalf("fifoModeFD(%q) = %d, want %d", tt.mode, got, tt.want)
		}
	}
	if _, err := fifoModeFD("rdwr"); err == nil {
		t.Fatal("fifoModeFD accepted invalid mode")
	}
}

func TestFIFOPipeKey(t *testing.T) {
	got, err := fifoPipeKey("/cluster/tmp/test.fifo", "sess", "job", "node-1")
	if err != nil {
		t.Fatal(err)
	}
	want := "coord:node-1\x00sess\x00job\x00fifo-path:/cluster/tmp/test.fifo"
	if got != want {
		t.Fatalf("fifoPipeKey = %q, want %q", got, want)
	}
}

func TestFIFOPipeKeyRequiresAbsolutePath(t *testing.T) {
	if _, err := fifoPipeKey("relative.fifo", "sess", "job", "node-1"); err == nil {
		t.Fatal("fifoPipeKey accepted relative path")
	}
}
