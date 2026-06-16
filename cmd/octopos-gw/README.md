# OctopOS Gateway (octopos-gw)

The VIP Gateway provides a single SSH entry point to the entire OctopOS cluster.

## Overview

```
User SSH → VIP (10.0.0.100:22) → octopos-gw
                                           │
                                           ├── Creates cluster session via octoposd
                                           ├── Mounts FUSE virtual filesystems (procfs, devfs, sysfs)
                                           ├── Enters PID/mount/net namespace
                                           └── Spawns user shell in cluster namespace
```

## Features

- **Single SSH endpoint** for the entire cluster
- **Cluster-wide process view** via virtual `/proc`
- **Cluster-wide device access** via virtual `/dev` (including VFIO/GPU)
- **Cluster-wide sysfs** via virtual `/sys`
- **Automatic namespace isolation** per session
- **cgroup resource limits** per session

## Building

```bash
go build -o bin/octopos-gw ./cmd/octopos-gw
sudo cp bin/octopos-gw /usr/local/bin/
```

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--vip` | `10.0.0.100:22` | VIP address to listen on |
| `--grpc-addr` | `127.0.0.1:50051` | octoposd gRPC address |
| `--node-id` | hostname | Gateway node identifier |
| `--mount-base` | `/tmp/octopos` | FUSE mount base directory |
| `--host-key` | `/etc/ssh/ssh_host_ed25519_key` | SSH host key |
| `--authorized-keys` | `~/.ssh/authorized_keys` | Authorized keys file |

## Deployment

### 1. Assign VIP on Leader Node

```bash
# On the leader node (where gateway runs)
sudo ip addr add 10.0.0.100/32 dev wg-octopos
```

### 2. Install Systemd Service

```bash
sudo cp deploy/systemd/octopos-gw.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable octopos-gw
sudo systemctl start octopos-gw
```

### 3. High Availability (keepalived)

For VIP failover, use keepalived on all nodes:

```bash
# /etc/keepalived/keepalived.conf
vrrp_instance octopos_vip {
    state MASTER        # BACKUP on other nodes
    interface wg-octopos
    virtual_router_id 51
    priority 100        # lower on backup nodes
    advert_int 1
    authentication {
        auth_type PASS
        auth_pass octopos
    }
    virtual_ipaddress {
        10.0.0.100/32
    }
}
```

## Usage

```bash
# SSH into the cluster
ssh developer@10.0.0.100

# Inside the cluster namespace:
ps aux              # Shows processes from ALL nodes
ls /dev/vfio/       # Shows GPU/VFIO devices from ALL nodes  
cat /proc/meminfo   # Shows CLUSTER TOTAL memory
octoposctl exec --command "htop"  # Runs on best node
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    EXTERNAL CLIENT                          │
│                      ssh user@10.0.0.100                    │
└────────────────────────────┬────────────────────────────────┘
                             │
                    ┌────────▼────────┐
                    │  octopos-gw     │  (Leader node)
                    │  10.0.0.100:22  │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
        ┌──────────┐  ┌──────────┐  ┌──────────┐
        │ Node-1   │  │ Node-2   │  │ Node-3   │
        │octoposd  │  │octoposd  │  │octoposd  │
        └──────────┘  └──────────┘  └──────────┘
```

## Session Lifecycle

1. **SSH Connection** → octopos-gw accepts
2. **CreateSession** → gRPC call to octoposd
3. **Start FUSE** → procfs, devfs, sysfs for this session
4. **Create cgroup** → Resource limits
5. **unshare()** → PID, mount, net, UTS, IPC namespaces
6. **Bind mount** → FUSE over /proc, /dev, /sys
7. **Exec shell** → User lands in cluster namespace
8. **Cleanup** → On exit: destroy session, stop FUSE, remove cgroup

## Requirements

- Root privileges (for namespace operations, FUSE mounts, cgroups)
- FUSE daemons built and in PATH: `octopos-procfs`, `octopos-devfs`, `octopos-sysfs`
- octoposd running and accessible via gRPC
- WireGuard mesh between nodes
- Linux kernel with namespace support

## Security

- SSH authentication via authorized_keys or password
- Each session isolated in its own namespaces
- cgroup limits prevent resource exhaustion
- No direct host access from session namespace