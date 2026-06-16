#include "../../common/include/vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>
#include "../../common/include/octopos_common.h"

// Ringbuf event type - compact, proc-only
struct octopos_event {
	__u64 global_pid;
	__u32 event_type;
	__u32 node_id;
	__u64 timestamp;
	struct proc_entry proc;
};

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, MAX_PIDS);
	__type(key, __u64);
	__type(value, struct proc_entry);
} proc_map SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, MAX_SESSIONS);
	__type(key, __u64);
	__type(value, struct session_filter);
} session_filter_map SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_RINGBUF);
	__uint(max_entries, 1 << 20);
} events SEC(".maps");

char LICENSE[] SEC("license") = "GPL";

// Tracepoint: sched/sched_process_exec
SEC("tp/sched/sched_process_exec")
int trace_exec(struct trace_event_raw_sched_process_exec *ctx) {
	__u32 pid = bpf_get_current_pid_tgid() >> 32;
	__u32 node_id = 1;
	__u64 global_pid = MAKE_GLOBAL_PID(node_id, pid);

	struct session_filter *filter = bpf_map_lookup_elem(&session_filter_map, &global_pid);
	if (filter && filter->node_filter && filter->node_filter != node_id) {
		return 0;
	}

	struct proc_entry entry = {};
	entry.global_pid = global_pid;
	entry.node_id = node_id;
	entry.pid = pid;
	entry.ppid = ctx->pid;
	entry.uid = bpf_get_current_uid_gid() >> 32;
	entry.gid = bpf_get_current_uid_gid() & 0xFFFFFFFF;
	entry.start_time = bpf_ktime_get_ns();
	bpf_get_current_comm(&entry.comm, sizeof(entry.comm));

	bpf_map_update_elem(&proc_map, &global_pid, &entry, BPF_ANY);

	struct octopos_event *event = bpf_ringbuf_reserve(&events, sizeof(*event), 0);
	if (event) {
		event->global_pid = global_pid;
		event->event_type = EVENT_PROC_EXEC;
		event->node_id = node_id;
		event->timestamp = bpf_ktime_get_ns();
		__builtin_memcpy(&event->proc, &entry, sizeof(entry));
		bpf_ringbuf_submit(event, 0);
	}

	return 0;
}

// Tracepoint: sched/sched_process_exit
SEC("tp/sched/sched_process_exit")
int trace_exit(struct trace_event_raw_sched_process_exit *ctx) {
	__u32 pid = bpf_get_current_pid_tgid() >> 32;
	__u32 node_id = 1;
	__u64 global_pid = MAKE_GLOBAL_PID(node_id, pid);

	bpf_map_delete_elem(&proc_map, &global_pid);

	struct octopos_event *event = bpf_ringbuf_reserve(&events, sizeof(*event), 0);
	if (event) {
		event->global_pid = global_pid;
		event->event_type = EVENT_PROC_EXIT;
		event->node_id = node_id;
		event->timestamp = bpf_ktime_get_ns();
		bpf_ringbuf_submit(event, 0);
	}

	return 0;
}

// Tracepoint: sched/sched_process_fork
SEC("tp/sched/sched_process_fork")
int trace_fork(struct trace_event_raw_sched_process_fork *ctx) {
	__u32 pid = ctx->child_pid;
	__u32 ppid = ctx->parent_pid;
	__u32 node_id = 1;
	__u64 global_pid = MAKE_GLOBAL_PID(node_id, pid);
	__u64 parent_gpid = MAKE_GLOBAL_PID(node_id, ppid);

	struct proc_entry *parent = bpf_map_lookup_elem(&proc_map, &parent_gpid);

	struct proc_entry entry = {};
	entry.global_pid = global_pid;
	entry.node_id = node_id;
	entry.pid = pid;
	entry.ppid = ppid;
	entry.uid = bpf_get_current_uid_gid() >> 32;
	entry.gid = bpf_get_current_uid_gid() & 0xFFFFFFFF;
	entry.start_time = bpf_ktime_get_ns();

	if (parent) {
		__builtin_memcpy(entry.comm, parent->comm, sizeof(entry.comm));
		__builtin_memcpy(entry.cmdline, parent->cmdline, sizeof(entry.cmdline));
	} else {
		bpf_get_current_comm(&entry.comm, sizeof(entry.comm));
	}

	bpf_map_update_elem(&proc_map, &global_pid, &entry, BPF_ANY);

	struct octopos_event *event = bpf_ringbuf_reserve(&events, sizeof(*event), 0);
	if (event) {
		event->global_pid = global_pid;
		event->event_type = EVENT_PROC_FORK;
		event->node_id = node_id;
		event->timestamp = bpf_ktime_get_ns();
		__builtin_memcpy(&event->proc, &entry, sizeof(entry));
		bpf_ringbuf_submit(event, 0);
	}

	return 0;
}
