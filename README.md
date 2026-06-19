# OctopOS

OctopOS is an experimental single-system-image cluster runtime for Linux hosts.
It combines a WireGuard node mesh, a shared cluster filesystem, virtual `/proc`
and `/sys` views, resource-aware scheduling, foreground and interactive exec, and
NVIDIA GPU projection for jobs that request GPUs.

The current implementation is Go-based and targets Linux systems with systemd,
FUSE, namespaces, and root access for privileged runtime setup.

## Features

- Cluster bootstrap and node provisioning with `octoposctl`
- WireGuard-based private cluster network
- Strict SSI mode backed by a shared cluster filesystem
- Foreground, background, and PTY exec across the cluster
- Per-exec private `/dev`, `/proc`, and `/sys` setup
- NVIDIA GPU scheduling and projection with `--gpu` / `--gpus`
- FUSE-backed virtual procfs, sysfs, and devfs components
- Optional eBPF support for process and system aggregation work

## Requirements

- Linux with systemd
- Go 1.21 or newer
- Root or sudo access on cluster hosts
- WireGuard
- FUSE3 for strict SSI mode
- A shared cluster filesystem such as JuiceFS mounted at `/cluster`
- NVIDIA drivers on hosts that should run GPU jobs

See [docs/getting-started.md](docs/getting-started.md) for the full setup notes.

## Build

```bash
go build -o bin/octoposctl ./cmd/octoposctl
go build -o bin/octoposd ./cmd/octoposd
go build -o bin/octopos-exec ./cmd/octopos-exec
```

Or build the common binaries with:

```bash
make build
```

## Bootstrap A Cluster

Build and install the CLI on the first node:

```bash
go build -o bin/octoposctl ./cmd/octoposctl
sudo install -m 0755 bin/octoposctl /usr/local/bin/octoposctl
```

Bootstrap the first node:

```bash
sudo octoposctl cluster bootstrap \
  --node-id node-1 \
  --cluster-root /cluster \
  --require-ssi=true
```

Add more nodes from the first node:

```bash
octoposctl --addr 127.0.0.1:50051 node add node-2 --address <ssh-host>
octoposctl --addr 127.0.0.1:50051 node add node-3 --address <ssh-host>
```

`cluster bootstrap` and `node add` configure cluster hosts for
`multi-user.target` and stop common display managers. OctopOS hosts should not
run a local desktop or X/Wayland display server.

## Usage

List nodes:

```bash
octoposctl --addr 10.0.0.1:50051 node list -o wide
```

Run a command in the cluster filesystem:

```bash
octoposctl --addr 10.0.0.1:50051 exec -- ls /
```

Open an interactive shell:

```bash
octoposctl --addr 10.0.0.1:50051 exec -it -- bash
```

Run on a specific node:

```bash
octoposctl --addr 10.0.0.1:50051 exec --node node-2 -- hostname
```

Request an NVIDIA GPU:

```bash
octoposctl --addr 10.0.0.1:50051 exec --gpu 1 -- nvidia-smi
```

Submit a background job:

```bash
octoposctl --addr 10.0.0.1:50051 exec --background -- sleep 60
octoposctl --addr 10.0.0.1:50051 job list
```

## Development

Run tests and static checks:

```bash
go test ./...
go vet ./...
```

Format Go changes:

```bash
gofmt -w ./cmd ./pkg ./fuse
```

Generated protobuf files are checked in. Regenerate them only when
`pkg/rpc/octopos.proto` changes:

```bash
make generate
```

## Repository Layout

- `cmd/octoposctl`: cluster CLI, bootstrap, provisioning, and exec client
- `cmd/octoposd`: node daemon and gRPC server
- `cmd/octopos-exec`: privileged SSI command launcher
- `cmd/octopos-gw`: optional SSH gateway for cluster access
- `pkg/cluster`: cluster data types and resource accounting
- `pkg/rpc`: gRPC service implementation and generated protobuf bindings
- `pkg/scheduler`: resource scheduling policy
- `pkg/ssi`: SSI root and mount validation helpers
- `pkg/nvidia`: NVIDIA device discovery and projection support
- `fuse/`: virtual procfs, sysfs, and devfs daemons
- `ebpf/`: optional eBPF programs
- `docs/`: setup and usage guides

## License

OctopOS is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
