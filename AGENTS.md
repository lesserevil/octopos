# Agent Instructions

This repository contains OctopOS, a Go-based Linux cluster runtime. Treat changes
as infrastructure code: keep edits scoped, verify behavior, and avoid destructive
host or git operations unless explicitly requested.

## Project Basics

- Module: `github.com/octopos/octopos`
- Go version: see `go.mod`
- Main binaries:
  - `./cmd/octoposctl`
  - `./cmd/octoposd`
  - `./cmd/octopos-exec`
  - `./cmd/octopos-remote-child`
  - `./cmd/octopos-lockcheck`
  - `./cmd/octopos-unixsock-proxy`
  - `./cmd/octopos-gw`
  - `./cmd/octopos-objectstore-proxy`
- Generated protobuf files live under `pkg/rpc/` and should only be regenerated
  when `pkg/rpc/octopos.proto` changes.

## Build And Test

Use these checks for normal code changes:

```bash
go test ./...
go vet ./...
```

For targeted work, run the affected package tests first, then run the full suite
before finalizing. Format Go code with:

```bash
gofmt -w <files>
```

The Makefile also provides:

```bash
make build
make test
make vet
make generate
```

`make test` writes `coverage.out`; remove that file before committing unless
the user explicitly asks to keep coverage artifacts.

## Runtime And Cluster Notes

- OctopOS hosts are expected to boot to `multi-user.target`.
- Do not enable or start graphical desktop/display-manager services on cluster
  hosts.
- Strict SSI mode expects a shared cluster filesystem and an SSI rootfs,
  commonly `/cluster`.
- `octopos-exec` is privileged and handles mount namespace, chroot, private
  `/dev`, and NVIDIA projection setup. Treat changes there as security-sensitive.
- GPU support is NVIDIA-focused. Schedulable GPU capacity comes from local
  `/dev/nvidia[0-9]+` devices, not generic PCI VGA devices.

The development cluster hosts currently use names like `shedwards-octo1`,
`shedwards-octo2`, and `shedwards-octo3`. If you use them for validation, keep
commands bounded with timeouts and avoid actions that would disrupt SSH access
unless the user has asked for that operational change.

## Coding Conventions

- Prefer existing package boundaries and patterns over introducing new
  abstractions.
- Keep user-facing CLI behavior backwards compatible where reasonable.
- Keep privilege-sensitive inputs internal. Do not trust user-provided
  environment variables for device allocation or namespace setup.
- Use structured parsing and typed helpers instead of ad hoc shell/string logic
  when Go code is the right layer.
- Do not revert unrelated work in a dirty tree.

## Git

- Do not commit or push unless the user asks for it.
- Never use `git reset --hard` or destructive checkout commands unless the user
  explicitly asks.
- Before finalizing, report the tests or live validations you ran and any
  residual risks.
