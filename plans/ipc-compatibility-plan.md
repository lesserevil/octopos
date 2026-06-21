# IPC Compatibility Plan

## Purpose

This plan records the OctopOS IPC strategy for distributed child processes. The
goal is to support IPC patterns that can be made correct and useful across
nodes, while explicitly keeping kernel-local objects local instead of building a
fragile illusion of full Linux kernel transparency.

This plan extends `plans/option3-distributed-child-processes.md`.

## Prior-Art Summary

Prior single-system-image cluster systems commonly used a pragmatic model:

- A migrated process kept a home node.
- A home-node deputy handled site-dependent kernel work.
- Remote execution was valuable mainly for CPU-heavy work with low to moderate
  I/O.
- Regular file access was optimized through consistent cluster file systems and
  direct file access paths.
- Later systems added direct communication and migratable sockets to avoid
  routing all communicating-process traffic through home nodes.
- Shared memory, writable `MAP_SHARED`, SysV shared memory, futexes, ptrace,
  and many newer kernel-local syscalls were not supported for migratable
  processes.
- Some systems exposed compatibility flags that let programs continue by
  failing unsupported syscalls or by treating harmless shared mappings more
  privately, but they did not attempt transparent distributed shared memory.

## Design Position

OctopOS should follow the same boundary:

- Support common byte-stream IPC by explicit OctopOS proxy/broker channels.
- Support regular files through the SSI cluster filesystem.
- Keep shared-memory and kernel-object IPC local unless a future feature
  explicitly implements that object type end to end.
- Prefer force-local fallback over partial remote behavior that can silently
  corrupt application semantics.
- Make every fallback explainable through diagnostics and job metadata.

The production target is not "all Linux IPC works across nodes". The target is:

- Ordinary shell and subprocess workflows can distribute useful work.
- Programs that rely on local kernel object identity stay on one node.
- Advanced users can see why a child was remoted, kept local, or rejected.

## Compatibility Modes

### Safe Mode

Safe mode is the default for transparent child placement.

Behavior:

- Remote only when inherited file descriptors and known syscall behavior are
  representable.
- Force local on shared memory, memfd, futex, ptrace, pidfd, abstract Unix
  sockets, unsupported anonymous inodes, and unknown descriptors.
- Log the precise force-local reason.

Use this for normal `octoposctl exec --remote-children=safe`.

### Explicit Remote Mode

Explicit remote mode applies when a user directly asks for remote placement,
for example `octopos-remote-child --node ...`.

Behavior:

- Reject unsupported IPC by default.
- Allow `--local-if-unsupported` to fall back local.
- Return clear stderr diagnostics and structured lifecycle metadata.

This avoids surprising a user who explicitly requested a remote worker.

### Relaxed Compatibility Mode

This should be opt-in and implemented only after safe mode is stable.

Possible flag:

```bash
octoposctl exec --remote-children=safe --remote-ipc-compat=relaxed
```

Initial relaxed behaviors:

- Permit read-only file-backed `MAP_SHARED` as private remote mappings when the
  mapping is only used as a read path.
- Convert a small allowlist of known harmless unsupported syscalls to `ENOSYS`
  or force-local, based on process policy.
- Warn once per process family when relaxed behavior is used.

Do not relax:

- Writable `MAP_SHARED`.
- SysV or POSIX shared memory.
- Futexes on shared memory.
- `SCM_RIGHTS`.
- `ptrace`.
- pidfds.
- Abstract Unix sockets.

## IPC Support Matrix

| IPC or kernel object | OctopOS decision | Difficulty | Implementation direction |
| --- | --- | --- | --- |
| stdin/stdout/stderr | Support | Done/basic | Keep using stream proxy and PTY path. |
| Exit status and `waitpid` | Support | Done/basic | Keep shadow PID as the waitable local child. |
| Common signals | Support | Medium | Continue forwarding foreground and lifecycle signals. |
| Process groups and job control | Support | Medium-hard | Finish stopped job table semantics and process-group mapping. |
| Regular files on SSI rootfs | Support | Medium | Use cluster FS, reopen eligible inherited files remotely. |
| File locks | Support after validation | Medium-hard | Validate `flock` and `fcntl` lock semantics on JuiceFS. |
| Anonymous pipes | Support common streams | Medium-hard | Build full pipe graph coordination with bounded buffers. |
| Named FIFOs | Support later | Medium-hard | Broker FIFO endpoints or force-local until proven. |
| TCP/UDP routable addresses | Support | Low-medium | Let normal networking handle it. |
| Localhost TCP | Force local by default | Medium | Optional future loopback proxy with explicit opt-in. |
| Unix pathname sockets | Support later for streams | Hard | Broker bind/connect/send/recv and peer identity. |
| Unix datagram sockets | Defer | Hard | Requires message-boundary and credential semantics. |
| Unix abstract sockets | Force local | Very hard | Kernel-local namespace, no SSI path to broker safely. |
| `SCM_RIGHTS` fd passing | Force local | Very hard | Requires arbitrary cross-node FD recreation and policy. |
| POSIX/System V shared memory | Force local | Very hard | Requires distributed shared memory coherence. |
| `memfd` and `/dev/shm` | Force local | Very hard | Same shared-memory problem plus lifecycle complexity. |
| Writable `mmap(MAP_SHARED)` | Force local | Very hard | Needs coherent shared page state and invalidation. |
| Read-only file-backed `MAP_SHARED` | Possible relaxed support | Medium | Treat as private/read-only if policy allows. |
| Shared-memory futexes | Force local | Extremely hard | Needs coherent memory plus distributed futex wait/wake. |
| POSIX/System V semaphores | Force local | Hard | Kernel-local semaphore state and wakeup ordering. |
| POSIX/System V message queues | Force local initially | Hard | Could be brokered, but not worth first wave. |
| `eventfd` | Force local initially | Medium-hard | Could be proxied later for simple counter semantics. |
| `signalfd` | Force local | Medium-hard | Signal mask and delivery semantics are process-local. |
| `timerfd` | Force local initially | Medium | Could be recreated remotely only when not shared. |
| `pidfd` | Force local | Hard | Shadow PID is not a real local kernel task. |
| `ptrace` | Force local | Extremely hard | Requires remote memory/register/syscall mediation. |
| `inotify` and `fanotify` | Force local initially | Hard | Shared FS event ordering is subtle. |
| Netlink | Force local for system state | Hard | Often refers to local kernel, device, and namespace state. |

## Implementation Phases

### Phase 1: Diagnostics and Policy Hardening

Goal: make every unsupported IPC fallback or rejection explicit.

Tasks:

1. Extend the FD classifier test matrix.
   - Unix pathname socket.
   - Unix abstract socket.
   - `socketpair(AF_UNIX)`.
   - `eventfd`.
   - `timerfd`.
   - `signalfd`.
   - `memfd_create`.
   - POSIX shm under `/dev/shm`.
   - SysV shm attach where test environment permits it.
   - pidfd where kernel support permits it.
2. Add structured force-local reason codes, not only prose strings.
   - Example: `ipc.unix_socket`, `ipc.memfd`, `ipc.eventfd`,
     `ipc.shared_memory`, `ipc.unknown_fd`.
3. Surface reason codes in:
   - remote-child lifecycle records
   - `octoposctl job children`
   - debug logs from transparent interception
4. Add docs describing the compatibility contract.
5. Ensure explicit remote mode rejects unsupported IPC unless
   `--local-if-unsupported` is present.

Acceptance criteria:

- Each unsupported IPC class has a unit test.
- A transparent child with unsupported IPC runs locally and prints one clear
  debug diagnostic when debug logging is enabled.
- An explicit remote child with unsupported IPC fails with a clear error unless
  `--local-if-unsupported` is set.

### Phase 2: Full Pipe Graph Coordination

Goal: make common shell pipelines reliable across nodes.

Tasks:

1. Track pipe endpoint identities across children in the same parent job.
2. Add a parent-side pipe graph registry.
3. Decide placement for a whole pipeline group, not each child independently.
4. Use native pipes when both endpoints land on the same worker node.
5. Use OctopOS stream proxies when endpoints are local/remote or remote/remote
   across different nodes.
6. Preserve:
   - EOF
   - backpressure
   - cancellation
   - `EPIPE`
   - `SIGPIPE` where practical
7. Bound proxy memory with fixed-size buffers.
8. Add metrics for pipe proxy bytes, blocked writes, and broken pipes.

Acceptance criteria:

- `yes | head -n 10 | wc -l` terminates without runaway buffering.
- `cat largefile | wc -c` does not buffer the full file in memory.
- `producer | false` terminates without leaked proxy goroutines.
- A three-stage pipeline can place stages on different nodes when policy allows.

Suggested junior tasks:

- Implement pipe registry data types.
- Add proxy read/write loop with bounded buffers.
- Add pipeline placement tests.
- Add broken-pipe tests.
- Add lifecycle cleanup tests.

### Phase 3: File Locks on the SSI Rootfs

Goal: define which file lock semantics are safe on the cluster filesystem.

Tasks:

1. Build a lock validation test binary.
   - `flock` exclusive/shared.
   - `fcntl` byte-range locks.
   - blocking and non-blocking lock attempts.
   - lock release on process exit.
2. Run validation across `shedwards-octo1`, `shedwards-octo2`, and
   `shedwards-octo3`.
3. Document the observed JuiceFS behavior.
4. Teach the eligibility policy whether inherited regular-file locks permit
   remoting.
5. Add a runtime guard if the mounted FS does not meet the required semantics.

Acceptance criteria:

- Cross-node lock behavior is documented.
- OctopOS either supports locks on the active cluster FS or force-locals locked
  processes with a clear reason.

### Phase 4: Named FIFO Broker

Goal: support named FIFO byte streams when semantics are simple.

Tasks:

1. Detect FIFO open paths under the SSI rootfs.
2. Add a FIFO endpoint broker in `octoposd`.
3. Preserve blocking open semantics for reader/writer rendezvous.
4. Preserve EOF and broken-pipe behavior.
5. Force local for unsupported mode combinations.
6. Add cleanup for abandoned FIFO endpoints.

Acceptance criteria:

- `mkfifo f; producer >f & consumer <f` works when producer and consumer are
  placed on different nodes.
- Blocking open does not spin or leak goroutines.
- Unsupported FIFO use falls back local.

### Phase 5: Unix Pathname Socket Broker

Goal: support selected pathname Unix sockets without supporting all AF_UNIX
behavior.

Scope for first version:

- `SOCK_STREAM`.
- Filesystem pathname sockets under the SSI rootfs.
- No abstract sockets.
- No `SCM_RIGHTS`.
- No credential-sensitive server mode unless peer credentials are explicitly
  modeled.

Tasks:

1. Intercept `socket`, `bind`, `connect`, `listen`, `accept`, `read`, `write`,
   `sendmsg`, and `recvmsg` for eligible pathname sockets.
2. Represent the socket listener in an OctopOS broker registry.
3. Route accepted streams between client and server workers.
4. Preserve byte-stream ordering and close semantics.
5. Return clear errors or force-local when ancillary data, credentials, or
   abstract addresses appear.
6. Add lifecycle cleanup for bound socket paths.

Acceptance criteria:

- A simple AF_UNIX echo server and client can run on different nodes.
- Path permissions are respected.
- Attempts to pass file descriptors force local or fail explicitly.

### Phase 6: Optional Read-Only Mapping Compatibility Mode

Goal: improve compatibility for programs that use `MAP_SHARED` for read-only
file mappings, without claiming shared-memory support.

Tasks:

1. Add an opt-in compatibility policy flag.
2. Intercept `mmap` in the transparent runtime or supervisor path.
3. Permit conversion only when:
   - mapping is file-backed
   - protections do not include write
   - mapping is not `/dev/shm`, `memfd`, procfs, sysfs, or a device
4. Emit one warning per process family.
5. Add tests showing writable shared mappings still force local.

Acceptance criteria:

- Read-only `MAP_SHARED` file mapping can run remotely under explicit compat
  mode.
- Writable `MAP_SHARED` still forces local.
- Shared-memory objects still force local.

## Permanent Non-Goals

These are not planned for transparent distributed support:

- General distributed shared memory.
- Shared-memory futexes.
- Transparent migration of multithreaded processes that rely on `CLONE_VM`.
- Arbitrary `SCM_RIGHTS` fd passing.
- Transparent `ptrace` across nodes.
- Transparent pidfd semantics across nodes.
- Abstract Unix socket transparency.
- Host administration workloads that inspect or mutate local kernel state.

If a future requirement needs one of these, treat it as a new architecture
project, not an incremental IPC feature.

## Operational Guidance

- Default cluster behavior should prefer correctness over distribution.
- Force-local decisions are successful scheduling outcomes, not failures.
- Users should be able to inspect force-local decisions after the fact.
- Live validation should use bounded commands and avoid disrupting active
  cluster workloads.
- Shared-memory-heavy frameworks should be configured to use TCP or explicit
  distributed transports when run under OctopOS.

## Recommended Priority

1. Diagnostics and reason codes.
2. Full pipe graph coordination.
3. File lock validation.
4. Named FIFO broker.
5. Pathname Unix socket broker for stream sockets.
6. Optional read-only mapping compatibility mode.

Defer everything involving distributed shared memory, futexes, `SCM_RIGHTS`,
pidfds, ptrace, and abstract sockets.
