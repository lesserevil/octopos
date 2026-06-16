#include "../../common/include/vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>
#include "../../common/include/octopos_common.h"

struct pipe_event {
	__u64 global_pid;
	__u32 event_type;
	__u32 node_id;
	__u64 timestamp;
	__u32 fd;
	__u32 len;
	__u8 is_splice;
	__u8 padding[3];
};

struct {
	__uint(type, BPF_MAP_TYPE_RINGBUF);
	__uint(max_entries, 1 << 20);
} pipe_events SEC(".maps");

char LICENSE[] SEC("license") = "GPL";

SEC("tp/syscalls/sys_enter_read")
int trace_pipe_read(struct trace_event_raw_sys_enter *ctx) {
	unsigned long fd = ctx->args[0];
	size_t count = (size_t)ctx->args[2];

	if (fd > 2 && count > 0) {
		__u32 pid = bpf_get_current_pid_tgid() >> 32;
		__u32 node_id = 1;
		__u64 global_pid = MAKE_GLOBAL_PID(node_id, pid);

		struct pipe_event *event = bpf_ringbuf_reserve(&pipe_events, sizeof(*event), 0);
		if (event) {
			event->global_pid = global_pid;
			event->event_type = EVENT_PIPE_READ;
			event->node_id = node_id;
			event->timestamp = bpf_ktime_get_ns();
			event->fd = (__u32)fd;
			event->len = (__u32)count;
			event->is_splice = 0;
			bpf_ringbuf_submit(event, 0);
		}
	}

	return 0;
}

SEC("tp/syscalls/sys_enter_write")
int trace_pipe_write(struct trace_event_raw_sys_enter *ctx) {
	unsigned long fd = ctx->args[0];
	size_t count = (size_t)ctx->args[2];

	if (fd > 2 && count > 0) {
		__u32 pid = bpf_get_current_pid_tgid() >> 32;
		__u32 node_id = 1;
		__u64 global_pid = MAKE_GLOBAL_PID(node_id, pid);

		struct pipe_event *event = bpf_ringbuf_reserve(&pipe_events, sizeof(*event), 0);
		if (event) {
			event->global_pid = global_pid;
			event->event_type = EVENT_PIPE_WRITE;
			event->node_id = node_id;
			event->timestamp = bpf_ktime_get_ns();
			event->fd = (__u32)fd;
			event->len = (__u32)count;
			event->is_splice = 0;
			bpf_ringbuf_submit(event, 0);
		}
	}

	return 0;
}

SEC("tp/syscalls/sys_enter_splice")
int trace_splice(struct trace_event_raw_sys_enter *ctx) {
	__u32 pid = bpf_get_current_pid_tgid() >> 32;
	__u32 node_id = 1;
	__u64 global_pid = MAKE_GLOBAL_PID(node_id, pid);

	int fd_out = (int)ctx->args[2];
	size_t len = (size_t)ctx->args[4];

	struct pipe_event *event = bpf_ringbuf_reserve(&pipe_events, sizeof(*event), 0);
	if (event) {
		event->global_pid = global_pid;
		event->event_type = EVENT_PIPE_WRITE;
		event->node_id = node_id;
		event->timestamp = bpf_ktime_get_ns();
		event->fd = (__u32)fd_out;
		event->len = (__u32)len;
		event->is_splice = 1;
		bpf_ringbuf_submit(event, 0);
	}

	return 0;
}
