package ebpf

import (
	"context"
	"os"
	"testing"
)

func skipNoRoot(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("skipping test; requires root (CAP_BPF + CAP_SYS_ADMIN)")
	}
}

func TestNewLoader(t *testing.T) {
	skipNoRoot(t)
	ctx := context.Background()
	loader, err := NewLoader(ctx)
	if err != nil {
		t.Fatalf("NewLoader failed: %v", err)
	}
	defer loader.Close()
}

func TestLoadAuto(t *testing.T) {
	skipNoRoot(t)

	ctx := context.Background()
	loader, err := NewLoader(ctx)
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}
	defer loader.Close()

	if err := loader.LoadAuto(ProgramProcAggregator); err != nil {
		t.Fatalf("LoadAuto(proc_aggregator): %v", err)
	}
}

func TestLoadAndAttachProcAggregator(t *testing.T) {
	skipNoRoot(t)

	ctx := context.Background()
	loader, err := NewLoader(ctx)
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}
	defer loader.Close()

	if err := loader.LoadAuto(ProgramProcAggregator); err != nil {
		t.Fatalf("LoadAuto: %v", err)
	}

	tracepoints := map[string]string{
		"trace_exec": "sched/sched_process_exec",
		"trace_exit": "sched/sched_process_exit",
		"trace_fork": "sched/sched_process_fork",
	}
	for progName, tp := range tracepoints {
		if err := loader.AttachTracepoint(ProgramProcAggregator, tp, progName); err != nil {
			t.Fatalf("AttachTracepoint(%s, %s): %v", tp, progName, err)
		}
	}

	if err := loader.StartRingbuf(ProgramProcAggregator, "events"); err != nil {
		t.Fatalf("StartRingbuf: %v", err)
	}

	m := loader.Map(ProgramProcAggregator, "proc_map")
	if m == nil {
		t.Fatal("Map(proc_map) is nil")
	}

	if err := loader.Stop(ProgramProcAggregator); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestSplitTracepoint(t *testing.T) {
	tests := []struct {
		input     string
		wantGroup string
		wantEvent string
	}{
		{"sched/sched_process_exec", "sched", "sched_process_exec"},
		{"syscalls/sys_enter_write", "syscalls", "sys_enter_write"},
		{"no-slash", "no-slash", ""},
		{"a/b/c", "a", "b/c"},
	}

	for _, tt := range tests {
		group, event := splitTracepoint(tt.input)
		if group != tt.wantGroup || event != tt.wantEvent {
			t.Errorf("splitTracepoint(%q) = (%q, %q), want (%q, %q)",
				tt.input, group, event, tt.wantGroup, tt.wantEvent)
		}
	}
}
