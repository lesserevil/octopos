package ebpf

import (
	"context"
	"fmt"
)

// SetupDefault loads and attaches all standard eBPF programs.
// Caller must have root (CAP_BPF + CAP_SYS_ADMIN).
func SetupDefault(ctx context.Context) (*Loader, error) {
	loader, err := NewLoader(ctx)
	if err != nil {
		return nil, fmt.Errorf("new loader: %w", err)
	}

	programs := []struct {
		typ         ProgramType
		tracepoints map[string]string // progName -> tracepoint
		ringbuf     string            // map name for ringbuf
	}{
		{
			typ: ProgramProcAggregator,
			tracepoints: map[string]string{
				"trace_exec": "sched/sched_process_exec",
				"trace_exit": "sched/sched_process_exit",
				"trace_fork": "sched/sched_process_fork",
			},
			ringbuf: "events",
		},
		{
			typ: ProgramSysAggregator,
			tracepoints: map[string]string{
				"trace_sys_write": "syscalls/sys_enter_write",
			},
			ringbuf: "sys_events",
		},
		{
			typ: ProgramDevProxy,
			tracepoints: map[string]string{
				"trace_openat": "syscalls/sys_enter_openat",
				"trace_close":  "syscalls/sys_exit_close",
			},
			ringbuf: "dev_events",
		},
		{
			typ: ProgramPipeSplice,
			tracepoints: map[string]string{
				"trace_pipe_read":  "syscalls/sys_enter_read",
				"trace_pipe_write": "syscalls/sys_enter_write",
				"trace_splice":     "syscalls/sys_enter_splice",
			},
			ringbuf: "pipe_events",
		},
	}

	for _, p := range programs {
		if err := loader.LoadAuto(p.typ); err != nil {
			loader.Close()
			return nil, fmt.Errorf("load %s: %w", p.typ, err)
		}

		for progName, tracepoint := range p.tracepoints {
			if err := loader.AttachTracepoint(p.typ, tracepoint, progName); err != nil {
				loader.Close()
				return nil, fmt.Errorf("attach %s/%s: %w", p.typ, progName, err)
			}
		}

		if p.ringbuf != "" {
			if err := loader.StartRingbuf(p.typ, p.ringbuf); err != nil {
				loader.Close()
				return nil, fmt.Errorf("ringbuf %s/%s: %w", p.typ, p.ringbuf, err)
			}
		}
	}

	return loader, nil
}
