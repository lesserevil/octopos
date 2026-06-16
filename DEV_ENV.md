# OctopOS Test Environment

> **Generated:** $(date -u +"%Y-%m-%d %H:%M:%S UTC")  
> **Host:** AMD Threadripper 3970X (64 CPUs, 60 GB RAM)  
> **Hypervisor:** libvirt/KVM (nested virt enabled)  
> **Status:** **PROVISIONED** - All 3 nodes running with full stack

---

## VM Specifications

| VM | Hostname | mgmt IP | WireGuard IP | vCPU | RAM | Disk |
|----|----------|---------|--------------|------|-----|------|
| 1 | octopos-node-1 | 192.168.122.205 | 10.0.0.1 | 8 | 32 GB | 20 GB |
| 2 | octopos-node-2 | 192.168.122.18 | 10.0.0.2 | 8 | 32 GB | 20 GB |
| 3 | octopos-node-3 | 192.168.122.12 | 10.0.0.3 | 8 | 32 GB | 20 GB |

**Total allocation:** 24 vCPU, 96 GB RAM, 60 GB disk

---

## Network

- **Management:** libvirt default network (192.168.122.0/24) via virbr0
- **Cluster:** WireGuard full mesh (10.0.0.0/24)
  - PersistentKeepalive=25
  - Port: 51820
- **Host access:** SSH to mgmt IPs (password: ubuntu)

---

## Credentials

| User | Access | Key |
|------|--------|-----|
| ubuntu | SSH + sudo (NOPASSWD) | Password: ubuntu |
| octopos | SSH + sudo (NOPASSWD) | ~/.ssh/id_ed25519.pub |
| root | Console only | N/A |

---

## Software Stack (Ubuntu 24.04, kernel 6.8.0-117-generic)

| Component | Version | Purpose |
|-----------|---------|---------|
| wireguard-tools | latest | Mesh networking |
| redis-server | 7.2+ | Cluster metadata (3 masters) |
| minio | RELEASE.2025-09-07 | Object storage (single-node) |
| juicefs | 1.2.1 | Distributed FS (root FS at /cluster) |
| golang-go | 1.22+ | octoposd, FUSE daemons |
| clang/llvm | 18+ | eBPF compilation |
| libbpf-dev | 1.4+ | eBPF development |
| bpftool | 7.4+ | eBPF management |
| libfuse3-dev | 3.16+ | FUSE daemons |
| linux-tools-generic | 6.8.0-31 | perf, bpftool |
| yq | v4.53.3 | YAML processing |
| octoposd | dev | Cluster agent (gRPC on :50051) |
| octoposctl | dev | Admin CLI (node/job/session/ps/cluster) |

---

## Quick Start

```bash
# Check status
./clusterctl.sh status

# Get IPs
./clusterctl.sh ips

# SSH to node (password: ubuntu)
./clusterctl.sh ssh 1

# Guest agent command (no SSH needed)
./clusterctl.sh guest-exec 1 "command"

# Cluster CLI (from host)
./bin/octoposctl --addr 192.168.122.205:50051 cluster status
./bin/octoposctl --addr 192.168.122.205:50051 node list
./bin/octoposctl --addr 192.168.122.205:50051 job list
./bin/octoposctl --addr 192.168.122.205:50051 ps
./bin/octoposctl --addr 192.168.122.205:50051 exec -- echo hello
```

---

## Cluster CLI (octoposctl)

```bash
# Cluster status
octoposctl --addr <mgmt-ip>:50051 cluster status

# Node management
octoposctl --addr <mgmt-ip>:50051 node list [-o wide|json]

# Job management
octoposctl --addr <mgmt-ip>:50051 job list
octoposctl --addr <mgmt-ip>:50051 job status <job-id>

# Session management
octoposctl --addr <mgmt-ip>:50051 session create [user]
octoposctl --addr <mgmt-ip>:50051 session list
octoposctl --addr <mgmt-ip>:50051 session delete <session-id>

# Process listing
octoposctl --addr <mgmt-ip>:50051 ps [--node <node>] [--session <id>] [--job <id>]

# Execute commands
octoposctl --addr <mgmt-ip>:50051 exec [--cpu N] [--mem N] [--gpus N] [--wait] -- <command> [args...]
```

---

## Cluster Management

```bash
# Start cluster
./clusterctl.sh start

# Stop gracefully
./clusterctl.sh stop

# Force stop
./clusterctl.sh force-stop

# Rebuild from scratch (run as root)
sudo ./provision_simple.sh

# Destroy completely
./clusterctl.sh destroy
```

---

## WireGuard Mesh

After provisioning, each node runs:
- Interface: wg-octopos
- Port: 51820
- Peers: 2 per node (full mesh)

```bash
# Check mesh
sudo wg show wg-octopos

# Test connectivity
ping 10.0.0.2  # from node-1
ping 10.0.0.3  # from node-1
```

---

## Storage Layout (per node)

```
/dev/vda1  →  / (local OS, 20 GB)
/mnt/minio/data  →  MinIO data (local)
/cluster  →  JuiceFS mount (distributed, 1.0P)
```

---

## Infrastructure Status

| Service | Node-1 | Node-2 | Node-3 | Notes |
|---------|--------|--------|--------|-------|
| SSH (password) | ✅ | ✅ | ✅ | User: ubuntu, Pass: ubuntu |
| WireGuard | ✅ | ✅ | ✅ | Full mesh, handshakes working |
| Redis | ✅ | ✅ | ✅ | 3-master cluster, slots covered |
| MinIO | ✅ | - | - | Single-node on node-1 |
| JuiceFS | ✅ | ✅ | ✅ | Mounted at /cluster, cross-node tested |
| **octoposd** | ✅ | ✅ | ✅ | gRPC :50051, health check passing |

---

## eBPF Development

```bash
# Kernel headers available
ls /usr/src/linux-headers-$(uname -r)

# Compile eBPF
clang -target bpf -D__TARGET_ARCH_x86 -O2 -c ebpf/proc_aggregator/proc_aggregator.bpf.c -o proc_aggregator.o

# Load and verify
bpftool prog load proc_aggregator.o /sys/fs/bpf/proc_aggregator type tracepoint
bpftool prog show pinned /sys/fs/bpf/proc_aggregator
```

---

## GPU Passthrough (Future)

When GPU hardware available:

```xml
<!-- libvirt XML snippet -->
<hostdev mode='subsystem' type='pci' managed='yes'>
  <source>
    <address domain='0x0000' bus='0x01' slot='0x00' function='0x0'/>
  </source>
  <driver name='vfio'/>
</hostdev>
<cpu mode='host-passthrough' check='none'/>
```

Requires host IOMMU/VT-d enabled.

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| VM won't boot | `virsh console node-1` to see GRUB/kernel |
| No mgmt IP | `virsh domifaddr node-1` / check libvirt dhcp |
| WireGuard not up | `journalctl -u wg-quick@wg-octopos` |
| Can't SSH | Check `~/.ssh/known_hosts` for old keys; use `sshpass -p ubuntu` |
| Nested KVM fail | `cat /sys/module/kvm_amd/parameters/nested` → must be Y |
| JuiceFS not mounted | `systemctl status juicefs-mount` |
| Redis cluster down | `redis-cli -c -h 10.0.0.1 cluster nodes` |
| MinIO not accessible | `curl http://10.0.0.1:9000/minio/health/live` |

---

## Useful Commands

```bash
# View all VMs
virsh list --all

# VM details
virsh dominfo octopos-node-1

# Serial console
virsh console octopos-node-1

# VM IP
virsh domifaddr octopos-node-1

# Guest agent execute
virsh qemu-agent-command octopos-node-1 '{"execute":"guest-exec", "arguments":{"path":"/bin/bash", "arg":["-c","cmd"], "capture-output":true}}'

# Snapshot
virsh snapshot-create-as octopos-node-1 pre-test --disk-only

# Revert
virsh snapshot-revert octopos-node-1 pre-test

# Host resources
htop
watch -n 1 'virsh list --all && echo "---" && free -h'
```