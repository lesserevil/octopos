#include "../../common/include/vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>
#include "../../common/include/octopos_common.h"

#define MAX_DEV_PATH 128

struct dev_event {
	__u64 global_pid;
	__u32 event_type;
	__u32 node_id;
	__u64 timestamp;
	__u32 major;
	__u32 minor;
	char path[MAX_DEV_PATH];
	__u32 flags;
};

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, MAX_PIDS);
	__type(key, __u64);
	__type(value, struct dev_event);
} open_devices SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_RINGBUF);
	__uint(max_entries, 1 << 18);
} dev_events SEC(".maps");

char LICENSE[] SEC("license") = "GPL";

static __always_inline int is_dev_path(const char *path, int len) {
	if (len < 5) return 0;
	char dev_prefix[5];
	bpf_probe_read_str(dev_prefix, sizeof(dev_prefix), path);
	return dev_prefix[0] == '/' && dev_prefix[1] == 'd' &&
	       dev_prefix[2] == 'e' && dev_prefix[3] == 'v' &&
	       dev_prefix[4] == '/';
}

SEC("tp/syscalls/sys_enter_openat")
int trace_openat(struct trace_event_raw_sys_enter *ctx) {
	__u32 pid = bpf_get_current_pid_tgid() >> 32;
	__u32 node_id = 1;
	__u64 global_pid = MAKE_GLOBAL_PID(node_id, pid);

	const char __attribute__((unused)) *pathname = (const char *)ctx->args[1];
	int flags = (int)ctx->args[2];

	char path_buf[MAX_DEV_PATH] = {};
	int ret = bpf_probe_read_str(path_buf, sizeof(path_buf), pathname);
	if (ret <= 0) return 0;

	if (!is_dev_path(path_buf, ret)) return 0;

	struct dev_event entry = {};
	entry.global_pid = global_pid;
	entry.event_type = EVENT_DEV_OPEN;
	entry.node_id = node_id;
	entry.timestamp = bpf_ktime_get_ns();
	entry.flags = flags;
	__builtin_memcpy(entry.path, path_buf, sizeof(entry.path));

	bpf_map_update_elem(&open_devices, &global_pid, &entry, BPF_ANY);

	struct dev_event *event = bpf_ringbuf_reserve(&dev_events, sizeof(*event), 0);
	if (event) {
		__builtin_memcpy(event, &entry, sizeof(*event));
		bpf_ringbuf_submit(event, 0);
	}

	return 0;
}

SEC("tp/syscalls/sys_exit_close")
int trace_close(struct trace_event_raw_sys_exit *ctx) {
	__u32 pid = bpf_get_current_pid_tgid() >> 32;
	__u32 node_id = 1;
	__u64 global_pid = MAKE_GLOBAL_PID(node_id, pid);

	struct dev_event *dev = bpf_map_lookup_elem(&open_devices, &global_pid);
	if (!dev) return 0;

	struct dev_event *event = bpf_ringbuf_reserve(&dev_events, sizeof(*event), 0);
	if (event) {
		__builtin_memcpy(event, dev, sizeof(*event));
		event->event_type = EVENT_DEV_CLOSE;
		event->timestamp = bpf_ktime_get_ns();
		bpf_ringbuf_submit(event, 0);
	}

	bpf_map_delete_elem(&open_devices, &global_pid);
	return 0;
}
