#ifndef __OCTOPOS_COMMON_H
#define __OCTOPOS_COMMON_H

#ifndef __BPF__
#include <linux/types.h>
#endif
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>

#define MAX_NODES 32
#define MAX_PIDS 100000
#define MAX_CMDS 128
#define MAX_PATH 256
#define MAX_SESSIONS 1000
#define MAX_JOBS 10000

// Node ID is u32 (1-32)
typedef __u32 node_id_t;

// Global PID = (node_id << 32) | local_pid
typedef __u64 global_pid_t;

// Session ID = hash(session_string)
typedef __u64 session_id_t;

// Job ID = hash(job_string)
typedef __u64 job_id_t;

// Event types for ring buffer
enum event_type {
	EVENT_PROC_EXEC = 1,
	EVENT_PROC_EXIT = 2,
	EVENT_PROC_FORK = 3,
	EVENT_SYS_CHANGE = 4,
	EVENT_DEV_OPEN = 5,
	EVENT_DEV_CLOSE = 6,
	EVENT_PIPE_READ = 7,
	EVENT_PIPE_WRITE = 8,
};

// Process entry in proc_map
// Compact version suitable for eBPF stack.
// Full cmdline/cwd available from userspace via /proc/<pid>.
struct proc_entry {
	__u64 global_pid;
	__u32 node_id;
	__u32 pid;
	__u32 ppid;
	__u32 uid;
	__u32 gid;
	__u64 start_time;
	char comm[16];
	char cmdline[MAX_CMDS];
	__u32 cpu_percent;
	__u64 rss_bytes;
	char state; // R, S, D, Z, T, etc.
	__u8 padding[7];
};

// Sysfs entry
struct sys_entry {
	__u32 node_id;
	char path[MAX_PATH];
	char value[512];
	__u64 timestamp;
};

// Device entry
struct dev_entry {
	__u32 node_id;
	__u32 major;
	__u32 minor;
	char name[64];
	char type[16]; // block, char, net, etc.
	__u32 vfio_group;
	__u8 is_remote;
	__u8 padding[3];
};

// Pipe buffer entry for cross-node splicing
struct octo_pipe_buffer {
	__u64 global_pid;
	__u32 pipe_id;
	__u32 seq;
	__u32 len;
	__u32 flags;
	__u8 data[4096]; // inline small buffers
};

// Session filter map
struct session_filter {
	__u64 session_id;
	__u32 node_filter; // 0 = all nodes
};

// Event for userspace ringbuf notification
struct octo_event {
	__u64 global_pid;
	__u32 event_type;
	__u32 node_id;
	__u64 timestamp;
	union {
		struct proc_entry proc;
		struct sys_entry sys;
		struct dev_entry dev;
		struct octo_pipe_buffer pipe;
	};
};

// Helper macros
#define GET_NODE_ID(gpid) ((gpid) >> 32)
#define GET_LOCAL_PID(gpid) ((gpid) & 0xFFFFFFFF)
#define MAKE_GLOBAL_PID(node, pid) (((__u64)(node) << 32) | (__u64)(pid))

#endif /* __OCTOPOS_COMMON_H */