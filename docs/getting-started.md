# Getting Started with OctopOS

OctopOS is a distributed operating system that spans multiple physical machines,
providing unified process, device, and resource management across a cluster.

## Architecture Overview

```
┌──────────────────────────────────────────────────┐
│                  octoposd                         │
│  ┌─────────┐  ┌──────────┐  ┌────────────────┐   │
│  │ gRPC    │  │ eBPF     │  │ Scheduler      │   │
│  │ Server  │  │ Loader   │  │ BinPack/Spread │   │
│  └────┬────┘  └────┬─────┘  └───────┬────────┘   │
│       │            │                │             │
│  ┌────┴────┐  ┌────┴─────┐  ┌───────┴────────┐   │
│  │ Tracker │  │ Resource │  │ Session        │   │
│  │         │  │ Detector │  │ Manager        │   │
│  └─────────┘  └──────────┘  └────────────────┘   │
└──────────────────────────────────────────────────┘
         │              │              │
    ┌────┴────┐   ┌────┴────┐   ┌────┴────┐
    │ Node-1  │   │ Node-2  │   │ Node-3  │
    │10.0.0.1 │   │10.0.0.2 │   │10.0.0.3 │
    └─────────┘   └─────────┘   └─────────┘
         │              │              │
    ┌────┴──────────────┴──────────────┴────┐
    │           WireGuard Mesh              │
    │           10.0.0.0/24                │
    └───────────────────────────────────────┘
```

WireGuard IPs are auto-assigned from `10.0.0.0/24`. The first node gets `.1`,
subsequent nodes auto-assigned `.2`, `.3`, etc.

## Prerequisites

### Hardware
- 3+ machines (x86_64 or aarch64), physical or virtual, each with:
  - 8+ CPU cores
  - 32GB+ RAM
  - 20GB+ disk
  - Network connectivity between nodes

### Software
| Component | Minimum | Recommended | Notes |
|-----------|---------|-------------|-------|
| **OS** | Ubuntu 24.04 LTS, Debian 12, RHEL 9, Fedora 34+ | Ubuntu 24.04 LTS | Any distro with kernel ≥ 5.8 |
| **Kernel** | **5.8+** (ringbuf, BTF, CO-RE) | 6.8+ (Ubuntu 24.04) | `uname -r` to verify |
| **Go** | 1.21+ | 1.22+ | For building daemons |
| **Clang** | 15+ | 18+ | For eBPF compilation |
| **libbpf/bpftool** | 1.0+ | 1.4+ | `apt install libbpf-dev bpftool` |
| **WireGuard** | Built-in (kernel 5.6+) | Latest | `apt install wireguard` |
| **FUSE3** | 3.0+ | 3.16+ | `apt install fuse3` |
| **Kernel headers** | Matching kernel | Matching | `apt install linux-headers-$(uname -r)` |

**Root access required** for eBPF loading, namespace operations, and FUSE mounts.

### Verify Your System

```bash
# Check kernel version (≥ 5.8 required)
uname -r

# Check BTF support (required for CO-RE)
ls -l /sys/kernel/btf/vmlinux

# Check ringbuf map support
bpftool map create /tmp/test type ringbuf size 4096 2>&1 | head -1

# Ubuntu 24.04 should pass all checks automatically
```

## Installation

### 1. Build the CLI

```bash
git clone https://github.com/octopos/octopos && cd octopos
go build -o bin/octoposctl ./cmd/octoposctl
sudo cp bin/octoposctl /usr/local/bin/
```

### 2. Bootstrap the First Node

On the first machine, run:

```bash
octoposctl cluster bootstrap --node-id node-1
```

This will:
- Install WireGuard and generate keys
- Create `/etc/wireguard/wg-octopos.conf` with IP `10.0.0.1`
- **Assign cluster VIP (10.0.0.100) to WireGuard interface**
- Build `octoposd` and install it to `/usr/local/bin`
- Install and start a systemd service for `octoposd`
- **Build and start the VIP gateway (octopos-gw) as a systemd service**
- Bring up the WireGuard interface

Verify it worked:

```bash
octoposctl --addr 127.0.0.1:50051 cluster status
```

**After bootstrap, you can SSH directly to the cluster:**
```bash
ssh user@10.0.0.100
# → Lands in cluster namespace with unified /proc, /dev, /sys across all nodes
```

### 3. Add More Nodes

On the first node, add each additional machine:

```bash
octoposctl node add node-2 --address 192.168.122.18
octoposctl node add node-3 --address 192.168.122.12
```

Where `--address` is the SSH-accessible IP of the remote machine.

This will:
- SSH into the remote machine (uses root key-based auth by default)
- Install dependencies (Go, clang, WireGuard, FUSE)
- Generate WireGuard keys and config with an auto-assigned IP
- Build `octoposd` locally and SCP it to the remote machine
- Install and start the daemon via systemd
- Register the new node with the cluster via gRPC
- Add the WireGuard peer on the local node

The WireGuard IP is auto-assigned from `10.0.0.0/24` by querying the cluster
for used IPs. Override with `--wg-ip` if needed.

## Usage

### CLI Commands

```bash
# Check cluster status
octoposctl --addr 10.0.0.1:50051 cluster status

# List nodes
octoposctl node list

# Create a session
octoposctl session create my-session

# Execute a command
octoposctl exec --session my-session --command "echo hello" --wait

# List processes
octoposctl ps

# List sessions
octoposctl session list

# Get job status
octoposctl job status <job-id>
```

### Interactive Shell

```bash
# Start an interactive session with streaming I/O
octoposctl exec --session my-session --command /bin/bash --interactive
```

### Using Namespace Isolation

```bash
# Mount virtual filesystems for a sandboxed session
octopos-procfs --mount /tmp/octopos-proc &
octopos-devfs --mount /tmp/octopos-dev --vfio-groups 1,2 &
octopos-sysfs --mount /tmp/octopos-sys --cpus 8 --memory 34359738368 &

# Use them as rootfs for a namespace-isolated process
sudo unshare --pid --mount --net \
  --mount-proc=/tmp/octopos-proc \
  /bin/bash
```

## CLI Reference

### `octoposctl cluster bootstrap`

Initialize the first cluster node on the local machine.

```bash
octoposctl cluster bootstrap [flags]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--node-id` | hostname | Node identifier |
| `--wg-ip` | `10.0.0.1` | WireGuard IP for this node |
| `--wg-port` | `51820` | WireGuard listen port |
| `--grpc-port` | `50051` | gRPC server port |

### `octoposctl node add <node-id>`

Provision and register a new cluster node via SSH.

```bash
octoposctl node add <node-id> --address <ssh-addr> [flags]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--address` | (required) | SSH address of the remote node |
| `--wg-ip` | auto-assigned | WireGuard IP for the new node |
| `--ssh-user` | `root` | SSH user |
| `--password` | "" | SSH password (key-based auth if empty) |
| `--endpoint` | `<address>:51820` | WireGuard endpoint for peers |
| `--wg-port` | `51820` | WireGuard listen port |
| `--grpc-port` | `50051` | gRPC port on the new node |
| `--ebpf` | `false` | Build and deploy eBPF programs |
| `--fuse` | `false` | Build and deploy FUSE daemons |

### `octoposctl node list`

List all cluster nodes.

```bash
octoposctl node list [--output json|wide]
```

### `octoposctl exec`

Execute a command on the cluster.

```bash
octoposctl exec [flags] -- <command> [args...]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--session` | auto | Session ID |
| `--cpu` | `1` | CPU cores required |
| `--mem` | `1` | Memory required (GB) |
| `--gpus`, `--gpu` | `0` | NVIDIA GPUs required |
| `--wait` | `false` | Wait for completion |
| `--node` | "" | Node affinity |

When a command requests GPUs, OctopOS schedules it onto a node with local NVIDIA
devices, projects the host driver files into `/usr/local/nvidia`, and prepends
`/usr/local/nvidia/bin` and `/usr/local/nvidia/lib64` to the command environment.

## Testing

```bash
# Run all unit tests (root-requiring tests are skipped automatically)
go test ./pkg/...

# Run specific package tests
go test -v ./pkg/cluster/
go test -v ./pkg/ebpf/

# Run eBPF verification (requires root)
sudo bpftool prog load ebpf/proc_aggregator/proc_aggregator.bpf.o /sys/fs/bpf/test type tracepoint
```

## Project Structure

```
octopos/
├── cmd/
│   ├── octoposd/        # Core daemon
│   └── octoposctl/      # CLI tool
├── ebpf/
│   ├── common/          # Shared headers, vmlinux.h
│   ├── proc_aggregator/ # Process lifecycle tracing
│   ├── sys_aggregator/  # System call monitoring
│   ├── dev_proxy/       # Device access tracking
│   └── pipe_splice/     # Cross-node pipe splicing
├── fuse/
│   ├── procfs/          # Virtual /proc filesystem
│   ├── devfs/           # Virtual /dev filesystem
│   └── sysfs/           # Virtual /sys filesystem
├── pkg/
│   ├── cluster/         # Node state, types
│   ├── ebpf/            # Go eBPF loader
│   ├── namespace/       # Cgroups, namespace setup
│   ├── resources/       # Resource detection
│   ├── rpc/             # gRPC service, protobuf
│   ├── scheduler/       # BinPack/Spread scheduling
│   ├── session/         # Session lifecycle
│   ├── tracker/         # Process tracking
│   └── vfio/            # VFIO device management
├── deploy/
│   └── systemd/         # Systemd unit files
└── docs/
    ├── getting-started.md
    └── quickstart.md
```

## Troubleshooting

### eBPF Issues
- **"BPF stack limit exceeded"**: Reduce struct sizes in eBPF programs (512-byte limit).
- **"Operation not permitted"**: Run as root or grant CAP_BPF + CAP_SYS_ADMIN.
- **"vmlinux.h not found"**: Run `bpftool btf dump file /sys/kernel/btf/vmlinux format c > ebpf/common/include/vmlinux.h`.

### Cluster Issues
- **Node not registering**: Check WireGuard connectivity between nodes.
- **Heartbeat timeout**: Ensure port 50051 is reachable between nodes.
- **Provisioning fails**: SSH as root into the remote node and check connectivity.

### FUSE Issues
- **"fusermount not found"**: Install fuse3 package: `apt install fuse3`.
- **"permission denied"**: Ensure `allow_other` is set in `/etc/fuse.conf`.

### Provisioning
- **"cannot auto-assign WireGuard IP"**: The cluster is unreachable. Use `--wg-ip` to specify manually.
- **SSH connection refused**: Verify `--address` is correct and SSH is running on the remote node.
- **Missing Go**: The provisioning script installs dependencies automatically on Ubuntu.

## Next Steps

- Explore the [Quickstart Guide](quickstart.md) for a condensed setup.
- Review eBPF programs in `ebpf/` to understand process/device monitoring.
- Extend the scheduler policy in `pkg/scheduler/policy.go`.
- Add new gRPC endpoints in `pkg/rpc/octopos.proto` (re-generate with `make proto`).
