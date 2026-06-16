# OctopOS Quickstart

Get a 3-node OctopOS cluster running in under 10 minutes.

## Requirements

| Component | Minimum | Notes |
|-----------|---------|-------|
| **OS** | Ubuntu 24.04 LTS, Debian 12, Fedora 34+ | Any distro with kernel ≥ 5.8 |
| **Kernel** | **5.8+** | `uname -r` - need ringbuf, BTF, CO-RE |
| **Arch** | x86_64 or aarch64 | Auto-detected at build time |
| **Go** | 1.21+ | For building |
| **Clang** | 15+ | For eBPF |
| **Root** | Yes | Required for eBPF, namespaces, FUSE |

**Quick check:**
```bash
uname -r                    # ≥ 5.8
ls /sys/kernel/btf/vmlinux  # BTF must exist
```

## 1. Provision 3 VMs

## 2. Build the CLI and Bootstrap the First Node

On the first machine:

```bash
# Clone and build octoposctl
git clone https://github.com/octopos/octopos && cd octopos
go build -o bin/octoposctl ./cmd/octoposctl
sudo cp bin/octoposctl /usr/local/bin/

# Bootstrap the first cluster node (with VIP gateway)
sudo octoposctl cluster bootstrap --node-id node-1
```

This automatically:
- Installs Go, WireGuard, clang, and other dependencies
- Generates WireGuard keys and config (IP: 10.0.0.1)
- **Assigns cluster VIP (10.0.0.100) to WireGuard interface**
- Builds and starts the daemon (octoposd) as a systemd service
- **Builds and starts the VIP gateway (octopos-gw) as a systemd service**
- Registers the first node in the cluster

**After bootstrap, you can SSH directly to the cluster:**
```bash
ssh user@10.0.0.100
# → Lands in cluster namespace with unified /proc, /dev, /sys across all nodes
```

## 3. Add Remaining Nodes

On the first node, add each additional machine:

```bash
# Ensure SSH key access to the remote machine first
ssh-copy-id root@192.168.122.18
ssh-copy-id root@192.168.122.12

# Provision and register each node (WireGuard IPs auto-assigned)
octoposctl --addr 127.0.0.1:50051 node add node-2 --address 192.168.122.18
octoposctl --addr 127.0.0.1:50051 node add node-3 --address 192.168.122.12
```

Each `node add` will:
- SSH into the remote machine
- Install dependencies (Go, WireGuard, etc.)
- Build the daemon locally and deploy it
- Auto-assign the next WireGuard IP (10.0.0.2, 10.0.0.3, ...)
- Start the daemon and register with the cluster
- Add the WireGuard peer on the local node

## 4. Verify the Cluster

```bash
octoposctl --addr 10.0.0.1:50051 cluster status
octoposctl --addr 10.0.0.1:50051 node list
```

## 5. Connect via Cluster VIP (Shared App-Space)

```bash
# SSH into the cluster VIP - lands in shared namespace
ssh user@10.0.0.100

# Inside the cluster namespace:
ps aux              # Shows processes from ALL nodes
ls /dev/vfio/       # Shows GPU/VFIO devices from ALL nodes
cat /proc/meminfo   # Shows CLUSTER-TOTAL memory
octoposctl exec --command "htop"  # Runs on best node
exit                # Back to your machine
```

## 6. Run a Command (Traditional CLI)

```bash
octoposctl --addr 10.0.0.1:50051 exec --command "uname -a" --wait
octoposctl --addr 10.0.0.1:50051 ps
```

## 6. (Optional) Load eBPF Programs

```bash
# eBPF loads automatically when octoposd runs as root.
# To verify:
sudo bpftool prog show | grep octopos
```

## 7. (Optional) Mount Virtual Filesystems

```bash
# Build and run FUSE daemons for namespace isolation:
go build -o bin/octopos-procfs ./fuse/procfs
go build -o bin/octopos-devfs ./fuse/devfs
go build -o bin/octopos-sysfs ./fuse/sysfs

sudo octopos-procfs --mount /mnt/octopos/proc &
sudo octopos-devfs --mount /mnt/octopos/dev &
sudo octopos-sysfs --mount /mnt/octopos/sys &
```

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `octoposctl: command not found` | `sudo cp bin/octoposctl /usr/local/bin/` |
| `Connection refused` | Check daemon is running: `sudo systemctl status octoposd` |
| SSH key prompt during `node add` | Run `ssh-copy-id root@<address>` first |
| `cannot auto-assign WireGuard IP` | Cluster unreachable; pass `--wg-ip` explicitly |
| WireGuard not starting | `sudo wg-quick up wg-octopos` and check logs |
| eBPF permission denied | Daemon must run as root for eBPF |

## What's Next?

See the [Getting Started Guide](getting-started.md) for full details, including
CLI reference, namespace isolation, and project structure.
