# VFIO Allocation and Release Plan

## Purpose

This plan defines how to complete the sketched VFIO allocation work in OctopOS.
The current repository has protobuf messages, cluster structs, virtual `/dev`
hooks, and RPC stubs for VFIO, but it does not yet discover VFIO groups,
reserve them in the scheduler, expose them safely inside exec namespaces, or
release them reliably.

The goal is to make VFIO device groups an explicitly allocated cluster resource
that can be requested by jobs and projected into an exec namespace without
accidentally sharing an IOMMU group across jobs.

This is not a replacement for the NVIDIA `--gpu` path. NVIDIA compute should
continue to use the NVIDIA device projection model. VFIO is for workloads that
need direct access to a VFIO group, such as userspace drivers, DPDK, or device
passthrough-style applications.

## Current State

Implemented today:

- RPC schema:
  - `AllocateVFIO`
  - `ReleaseVFIO`
  - `GetVFIODevices`
- Cluster data structures:
  - `PCIDevice`
  - `VFIORequirement`
  - `VFIOAlloc`
  - `NodeInfo.VFIOGroups`
  - `Requirements.VFIODevs`
  - `ProcessInfo.VFIOGroups`
- `octopos-devfs` has a prototype `--vfio-groups` option.
- `octoposd` has a `vfio_enabled` config field.

Not implemented today:

- VFIO group discovery from sysfs.
- Group viability validation.
- Node heartbeat propagation of available and claimed VFIO groups.
- Scheduler filtering and reservation for `Requirements.VFIODevs`.
- Allocation ownership records tied to session/job lifecycle.
- RPC implementations in `pkg/rpc/server.go`.
- CLI commands for listing, allocating, or releasing VFIO groups.
- Exec namespace projection for `/dev/vfio/vfio` and `/dev/vfio/<group>`.
- Release-on-job-exit, release-on-session-destroy, or daemon-restart recovery.

## Design Principles

- Allocate whole IOMMU groups, never individual PCI functions. VFIO isolation is
  at group granularity.
- Only advertise groups that are viable and safe to expose.
- Do not automatically detach active host drivers during normal scheduling.
  Driver rebinding is disruptive and should be an explicit admin operation.
- Treat VFIO allocations as leases owned by a session and job.
- Keep allocation state authoritative in `octoposd` and mirrored into scheduler
  resource state.
- Do not attempt to return live file descriptors over ordinary gRPC. The
  current `container_fd` and `device_fd` protobuf fields cannot work over the
  existing TCP gRPC transport. For the first implementation, return group IDs,
  allocation IDs, and device paths; mark fd fields as unused or deprecated.
- Project VFIO device nodes into exec namespaces only after scheduler allocation
  has assigned those groups to the job.
- Prefer fail-closed behavior. If group state cannot be verified, do not expose
  the group.

## User-Facing Model

Initial CLI shape:

```bash
octoposctl vfio list [--node NODE] [-o wide|json]
octoposctl vfio allocate --session SESSION --job JOB --class CLASS [--vendor VENDOR] [--device DEVICE] [--count N]
octoposctl vfio release --session SESSION --group GROUP [--node NODE]
```

Job-integrated shape:

```bash
octoposctl exec --vfio class=0200,count=1 -- ./dpdk-app
octoposctl exec --vfio vendor=8086,device=10fb,count=1 -- ./userspace-driver
```

The `exec --vfio ...` path should be the preferred user path. Manual allocate
and release commands are useful for debugging and operational control, but jobs
should normally own and release their allocations automatically.

## Allocation Semantics

An allocation reserves one or more VFIO groups on one node.

An allocation record should include:

```go
type VFIOAllocation struct {
    AllocationID string
    NodeID       cluster.NodeID
    SessionID    cluster.SessionID
    JobID        cluster.JobID
    Groups       []int
    Devices      []cluster.PCIDevice
    State        VFIOAllocationState
    CreatedAt    time.Time
    UpdatedAt    time.Time
    ReleasedAt   time.Time
    FailureReason string
}
```

States:

- `reserved`
- `projected`
- `released`
- `failed`
- `orphaned`

Release triggers:

- job reaches terminal state
- session is destroyed
- explicit `ReleaseVFIO`
- scheduler rollback after exec launch failure
- daemon startup recovery determines the owning job no longer exists

Allocation must be idempotent on release. Releasing an already released group
should return success if the caller owns it, and a clear error otherwise.

## Phase 1: VFIO Discovery

Goal: populate accurate local VFIO group inventory.

Tasks:

1. Add `pkg/vfio`.
2. Implement sysfs discovery:
   - list `/sys/kernel/iommu_groups/*`
   - resolve each group's `devices/*` symlinks
   - read PCI `vendor`, `device`, `class`, `driver`, and `iommu_group`
   - map group to `/dev/vfio/<group>`
3. Mark a group available only when:
   - `/dev/vfio/vfio` exists
   - `/dev/vfio/<group>` exists and is a character device
   - all devices in the group are bound to `vfio-pci`, `vfio-platform`, or
     another configured safe VFIO driver
   - the group is not on an admin denylist
4. Add cluster config fields:
   - `vfio_enabled`
   - `vfio_allowed_groups`
   - `vfio_denied_groups`
   - `vfio_allowed_classes`
   - `vfio_allowed_vendors`
   - `vfio_driver_rebind` default `false`
5. Extend `resources.Detector.DetectAll` to fill `ResourceSpec.PCIDevices`.
6. Add helper to group `PCIDevice` entries into `VFIOGroup` records.
7. Unit tests using temporary fake `/sys` and `/dev` trees.

Acceptance criteria:

- A host with no VFIO support reports zero VFIO groups without failing node
  startup.
- A viable fake group is detected with the expected group ID and PCI metadata.
- A group with any non-VFIO-bound device is not marked allocatable.
- Denylisted groups are hidden from scheduling.

## Phase 2: Cluster State and Scheduler Reservation

Goal: make VFIO groups first-class schedulable resources.

Tasks:

1. Extend `cluster.NodeInfo` with an internal `VFIOAllocations` map:

```go
map[int]cluster.JobID
```

2. Extend `ResourceSpec` or `NodeInfo` helpers to expose:
   - all VFIO groups
   - free VFIO groups
   - claimed VFIO groups
3. Update `CanReserve` and `ReserveWithAllocation`:
   - filter by vendor/device/class/count
   - allocate whole groups
   - write selected groups back into `Requirements`
   - reject requests when any selected group is already claimed
4. Update `Release` to release selected VFIO groups by group ID and job owner.
5. Extend scheduler tests with:
   - class match
   - vendor/device match
   - count > 1
   - allocation exhaustion
   - release and reuse
   - node affinity plus VFIO requirement
6. Ensure GPU and VFIO allocation state cannot conflict. If a physical GPU is
   advertised through NVIDIA device projection, it must not also be advertised
   as allocatable VFIO unless an admin explicitly binds it to VFIO and removes
   it from NVIDIA discovery.

Acceptance criteria:

- A VFIO requirement can schedule only on nodes with matching free groups.
- Scheduler returns the concrete allocated groups in the allocated
  requirements.
- Releasing a job frees exactly the groups owned by that job.
- A group cannot be double-booked.

## Phase 3: RPC Implementation

Goal: make the existing VFIO RPCs useful and safe.

Tasks:

1. Implement `GetVFIODevices`.
   - With `node_id`, return groups for that node.
   - Without `node_id`, aggregate known groups across nodes.
   - Include `claimed_by` for allocated groups.
2. Implement `AllocateVFIO`.
   - Validate non-empty session ID and job ID.
   - Validate session/job ownership where the job already exists.
   - Convert protobuf `VFIORequirement` to cluster requirements.
   - Reuse scheduler reservation logic.
   - Persist allocation record in `octoposd`.
   - Return group ID, allocation ID if the protobuf is extended, and device path.
   - Leave `container_fd` and `device_fd` unset or `-1` because fd transfer is
     not supported by the existing gRPC transport.
3. Implement `ReleaseVFIO`.
   - Validate session ownership.
   - Release only groups owned by that session/job.
   - Return clear diagnostics on nonexistent or foreign groups.
4. Consider a protobuf revision:
   - add `allocation_id`
   - add `node_id`
   - change release to accept `allocation_id`
   - mark fd fields as deprecated in comments
5. Add RPC unit tests with an in-memory scheduler and fake node inventory.
6. Add concurrency tests:
   - two simultaneous allocation requests for one group
   - release racing with job completion

Acceptance criteria:

- RPCs no longer return `"not implemented"`.
- `GetVFIODevices` reflects claimed/free groups.
- Concurrent allocation cannot double-claim a group.
- Releases are idempotent for the owner and denied for non-owners.

## Phase 4: Exec Integration

Goal: make `octoposctl exec --vfio ...` reserve groups and expose them only to
the target job.

Tasks:

1. Add CLI parsing for `--vfio`.
   - Accept repeated flags.
   - Parse `vendor=`, `device=`, `class=`, `count=`.
   - Convert to `Requirements.VFIODevs`.
2. Update protobuf conversion helpers for `Requirements.VfioDevs`.
3. Ensure `Execute` and `ExecStream` carry selected concrete VFIO groups after
   scheduling.
4. Extend `buildSSICommand` to pass allocated VFIO group IDs to
   `octopos-exec`.
5. Add `octopos-exec` flags:
   - `--vfio-groups`
   - optional `--vfio-dev-root` default `/dev/vfio`
6. In `octopos-exec`, create a private `/dev/vfio` with:
   - `/dev/vfio/vfio`
   - `/dev/vfio/<allocated group>`
7. Do not expose unallocated VFIO groups in the exec namespace.
8. Add cgroup/device-controller handling if the current runtime uses device
   cgroups. If OctopOS is running privileged without device cgroups, document
   that as the current behavior and keep the device-node projection narrow.
9. Ensure release happens on:
   - normal foreground completion
   - background job completion
   - exec launch failure after scheduler reservation
   - stream cancellation

Acceptance criteria:

- A job with no VFIO request has no `/dev/vfio/<group>` nodes projected.
- A job with a VFIO allocation sees only its allocated groups.
- Launch failure releases the groups.
- Job completion releases the groups.

## Phase 5: Lifecycle, Persistence, and Recovery

Goal: avoid leaked groups after daemon restart or partial failure.

Tasks:

1. Persist VFIO allocation records under `/var/lib/octopos/vfio-allocations.json`
   or fold them into the existing daemon lifecycle state if that becomes a
   cleaner shared store.
2. On daemon startup:
   - load records
   - mark active records `recovering`
   - reconcile against job tracker state
   - keep reservations for running jobs
   - release records for terminal or missing jobs after a short lease
3. On heartbeat/resource refresh:
   - keep claimed groups claimed even if discovery order changes
   - mark missing groups failed and terminate or fail owning jobs if policy
     requires exclusive hardware access
4. Add metrics/log lines:
   - allocations
   - releases
   - denied allocations
   - orphan recovery
5. Add admin-visible status through:
   - `octoposctl vfio list -o wide`
   - `job status`
   - `octoposctl ps` `vfio_groups`

Acceptance criteria:

- Restarting `octoposd` does not immediately leak or double-free active VFIO
  groups.
- Terminal jobs do not hold groups indefinitely.
- Operators can see which job owns a group.

## Phase 6: CLI and Documentation

Goal: make the feature operable.

Tasks:

1. Add `octoposctl vfio list`.
2. Add `octoposctl vfio allocate` and `octoposctl vfio release` for diagnostics.
3. Add `octoposctl exec --vfio`.
4. Add README documentation:
   - what VFIO is for
   - how to prepare a host
   - how to request a group
   - why NVIDIA compute should use `--gpu`
   - release and failure behavior
5. Add admin notes for host preparation:
   - enable IOMMU in firmware/kernel
   - load `vfio`, `vfio_iommu_type1`, and `vfio_pci`
   - bind intended devices to VFIO explicitly
   - verify `/dev/vfio/vfio` and `/dev/vfio/<group>`

Acceptance criteria:

- A user can list VFIO groups and see claimed/free state.
- A user can run an exec with a VFIO requirement.
- Documentation explains why groups may be unavailable.

## Phase 7: Validation

Goal: prove the implementation is correct before using real devices.

Unit tests:

- fake sysfs discovery
- group viability
- allow/deny policy
- scheduler reserve/release
- RPC allocation/release
- exec flag parsing
- namespace projection logic
- recovery from persisted allocation records

Integration tests:

- single node with fake VFIO groups
- two nodes with competing allocation requests
- allocation followed by launch failure
- daemon restart with active allocation
- session destroy releases allocation

Live validation on `shedwards-octo*`, only when a VFIO-capable test device is
available and the user approves host-level driver binding:

1. Verify IOMMU and VFIO kernel support.
2. Bind one non-critical test device to `vfio-pci`.
3. Confirm `octoposctl vfio list` shows the group.
4. Run a bounded exec that opens `/dev/vfio/vfio` and the allocated group.
5. Confirm a second allocation for the same group is rejected.
6. Confirm completion releases the group.

## Security and Safety Notes

- VFIO gives a process direct DMA-capable device access. Only expose groups that
  are isolated by IOMMU and explicitly allowed by admin policy.
- Never expose a partial IOMMU group.
- Never expose a group that contains host-critical devices.
- Do not auto-rebind drivers during ordinary `exec`; require an explicit admin
  preparation step.
- `AllocateVFIO` must verify ownership. A session must not release or inspect
  another session's private allocation beyond high-level claimed/free status.
- Device-node projection must be narrow. A VFIO job should not inherit all of
  `/dev/vfio`.

## Suggested Implementation Order

1. Add `pkg/vfio` discovery and tests.
2. Add cluster/scheduler VFIO reservation and tests.
3. Implement `GetVFIODevices`.
4. Implement allocation/release RPCs with in-memory lifecycle records.
5. Wire `exec --vfio` through scheduling and namespace projection.
6. Add persistent recovery.
7. Add CLI/docs.
8. Run bounded live validation on prepared hardware.

Each phase should be committed separately after `go test ./...` and `go vet
./...` pass.
