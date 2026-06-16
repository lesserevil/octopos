#include "../../common/include/vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>
#include "../../common/include/octopos_common.h"

struct sys_change_event {
	__u64 global_pid;
	__u32 event_type;
	__u32 node_id;
	__u64 timestamp;
	__u32 uid;
	__u32 fd;
	__u32 len;
};

struct {
	__uint(type, BPF_MAP_TYPE_RINGBUF);
	__uint(max_entries, 1 << 18);
} sys_events SEC(".maps");

char LICENSE[] SEC("license") = "GPL";

SEC("tp/syscalls/sys_enter_write")
int trace_sys_write(struct trace_event_raw_sys_enter *ctx) {
	__u32 pid = bpf_get_current_pid_tgid() >> 32;
	__u32 node_id = 1;
	__u64 global_pid = MAKE_GLOBAL_PID(node_id, pid);

	unsigned long fd = ctx->args[0];
	if (fd <= 2) return 0;

	struct sys_change_event *event = bpf_ringbuf_reserve(&sys_events, sizeof(*event), 0);
	if (!event) return 0;

	event->global_pid = global_pid;
	event->event_type = EVENT_SYS_CHANGE;
	event->node_id = node_id;
	event->timestamp = bpf_ktime_get_ns();
	event->uid = bpf_get_current_uid_gid() >> 32;
	event->fd = (__u32)fd;
	event->len = (__u32)ctx->args[2];

	bpf_ringbuf_submit(event, 0);
	return 0;
}
