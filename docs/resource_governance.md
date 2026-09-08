# Process resource governance

The kernel owns resource limits. Libc `getrlimit` and `setrlimit` call the
version-1 kernel ABI; they do not maintain fallback process-local state.
`PROCESS` operations 48, 49 and 50 query limits, set limits and query usage.
Their arguments are resource ID, user address and ABI version, in the existing
PROCESS syscall register order. `abi/resource.hpp` and libc's
`sys/resource_policy.h` define the fixed-width layouts and infinity value.

Threads use the process owner's policy. Fork/clone/spawn snapshot policy under
the owner's resource lock; exec preserves it. Usage and denial observations are
not copied into a new process. Lowering a soft limit does not revoke existing
resources or already admitted operations. Raising a hard limit requires an
effective UID of zero; soft must not exceed hard. NOFILE cannot exceed WOS's
physical 256-entry descriptor table even for root.

## Implemented enforcement

- **NOFILE:** Numeric descriptor bounds apply before open/create/truncate,
  socket/accept allocation and pipe publication. Reservations are hidden from
  lookup and fork, survive subsequent soft-limit lowering, and are canceled on
  rollback and fatal syscall exit. Pipe endpoints publish as one pair. Existing
  descriptors remain usable after lowering a limit. Internal exec ELF reads do
  not consume user descriptors. A dynamic loader can still require descriptors
  to open shared libraries; the zero-limit exec test uses a static fixture.
- **FSIZE:** TMPFS, XFS and FAT32 writes/truncate and remote VFS consumer writes/
  truncate check the caller's soft endpoint bound before mutation. TMPFS/XFS
  append selects EOF and admits the prefix under the inode I/O mutex. A request
  crossing the boundary returns a short write; a fully denied nonempty request
  returns `EFBIG` and queues signal 25 (`SIGXFSZ`). A denied truncate has the same
  error/signal behavior. Empty writes and truncation to zero need no growth.
  Accepted remote write-behind bytes retain their admission if policy is later
  lowered. Pre-scheduler initramfs writes have no process policy.

- **CORE:** Complete binary size (header, tables, captured pages and embedded
  ELF) must fit both CORE and the crashing process's FSIZE. A denied capture
  produces no file and returns every temporary page/task reference. Zero skips
  capture before claiming a ring slot. The daemon retains the admitted policy
  owner through completion, even if the process exits or lowers its limit.
  Current/peak describe successful complete output sizes. The existing binary
  version, entropy redaction and fixed four-slot ring remain unchanged.

- **STACK:** Automatic main-stack construction, argv,
  signal frames and fault growth honor the soft depth bound. Whole-page backing
  rounds down to the limit; zero prevents a new image. The existing 64-MiB
  virtual reservation, TLS/SafeStack/guards and GS bootstrap page are preserved.
  The separate bootstrap page does not count as downward usable stack growth.
  Lowering leaves existing pages usable; raising permits later faults within
  the reservation. Growth rejection becomes SIGSEGV, new-image allocation
  failure ENOMEM, and arguments that exceed the stack budget E2BIG. Explicit
  pthread stacks are ordinary VM mappings, governed by AS. Current records the
  main stack's downward extent, including holes between backed pages; peak
  survives exec. Failed staged exec does not replace the old current observation.

- **CPU:** User and syscall CPU time accumulate across
  the process's threads. Internal accounting uses microseconds; the limit and
  procfs observations use whole seconds. At the soft threshold the consuming
  thread receives SIGXCPU, repeated at most once per further CPU second. At the
  hard threshold it receives uncatchable SIGKILL, using the existing group-exit
  path. Enforcement occurs at accounting/return boundaries, so concurrent CPUs
  can consume up to their scheduling latency beyond a threshold. Fork resets
  consumption while inheriting policy; exec retains both. Sleeping time and
  non-process kernel daemons are excluded. A retained owner prevents thread
  accounting from depending on process-registry lookup in timer context.
  Soft handlers can be prepared on a validated incoming user frame; hard
  expiry waits until the handoff has committed on that task's own kernel stack.
  Kernel resumes of blocked syscalls do not consume CPU expiry events.

- **AS:** The soft bound counts occupied user virtual
  pages, including nonresident reservations, guards, automatic stack/TLS,
  lazy ELF/file/anonymous pages and shared-memory attachments. VM transactions
  admit growth before reservation or destructive replacement. MAP_FIXED charges
  net growth, so an equal-size replacement remains possible after lowering.
  WOS mremap allocates its destination before copying/removing the source; both
  ranges count during that relocation. Denial returns ENOMEM and preserves the
  source. Fork inherits existing mappings and policy; failed staged exec leaves
  the old image and observations intact. Exec preserves the task's AS peak.
  AS current/peak describe the queried task's address space; policy and denial
  telemetry belong to its process owner. This also covers WOS thread exec,
  which can retain siblings on the old root. Shared-root mutations publish
  observations to every live publisher. Procfs reads completed-transaction
  observations; RESOURCE_USAGE refreshes under VM publication serialization.
  Accounting takes the union of lazy range metadata and occupied lower-half
  page-table leaves, including resident PROT_NONE pages, without double counting.

- **NPROC:** The soft bound applies to the node's live tasks with the creator's
  real UID, including threads and pending creation reservations. Root also
  obeys finite limits. Fork, clone, pthread and spawn reserve before publishing
  a task or applying spawn file actions; denial returns EAGAIN. Reservations
  transfer to the created task and unwind on failure or fatal syscall exit.
  Logical exit releases the charge before zombie metadata is reclaimed.
  Lowering below the current population leaves existing tasks alive; exec adds
  no charge. A real-UID change must fit the destination population before any
  credentials change, otherwise it returns EAGAIN. Changing to the same UID
  needs no new admission. Current reports the queried task's UID population;
  peak is its observed high-water population, while policy and denial counters
  belong to the process owner. Failed pthread creation frees the unpublished
  TCB/TLS and automatic stack, preserving caller-owned explicit stacks.

Init service budgets and persistent accounting are implemented; see
[the init manifest](init_supervisor.md) for syntax and restart semantics.
Remote compute requires the negotiated resource capability on both peers. Its
versioned submission
record carries limits, lifetime observations, CPU notification history, and
inherited standard-descriptor presence. Receiver policy validation precedes
executable fetch and process admission. Inherited IPC and fallback standard
descriptors install atomically and remain valid after NOFILE was lowered below
their descriptor numbers. Closed and CLOEXEC standard descriptors stay absent
when this contract is negotiated.

Resource-bearing completions retain the original submission baseline across
early delivery and proxy publication. Validation checks the peer session and
monotonic observations; the winning proxy finalizer merges only remote deltas
before publishing the child exit. The version-2 resource record is 832 bytes
and carries separate process user/system CPU counters. The scheduler classifies
the same slice for Task statistics and process policy after subtracting IRQ and
precharged syscall time. Fork resets these counters; exec migration retains
them. Completion adds only their remote deltas to the proxy's wait accounting.
Children reaped remotely contribute separate completion-only user/system deltas,
including work returned by nested remote execution. They do not inflate the
process CPU limit. Submission records require those child deltas to be zero.
Receiver completion waits for thread CPU contributions to retire before copying
the stable child totals. Retained dead-thread metadata does not delay completion.
It also waits for admitted deferred core output to publish its CORE/FSIZE
observations or finish unsuccessfully, using a separate retained counter.
Proxy exit readiness is published after descriptor and process-charge cleanup.
Version-1 resource records and legacy submissions without the resource
identity are rejected before executable fetch. Failed remote placement preserves rejection errors through
spawn and exec: unsupported policy is `ENOTSUP`, process admission is `EAGAIN`,
exhausted CPU is `EDQUOT`, invalid policy is `EINVAL`, and receiver allocation
failure is `ENOMEM`. Transport timeout is `ETIMEDOUT`. A policy rejection or
timeout does not retry with another binary delivery method; ordinary local
fallback follows the existing placement flags.

After validating a resource submission, the receiver can append its admission
observations to `TASK_REJECT`. The 16-byte response header remains unchanged;
reserved bytes 0–1 contain the 832-byte suffix length in little endian, and byte
2 is zero. Unpublished task teardown preserves the snapshot before freeing the
task. The origin validates unchanged policy and CPU counters against the exact
submission baseline, then merges only peak usage and denial deltas after winning
the response waiter. A rejected allocation does not import live charges or reset
local policy. Duplicate, stale-session and malformed responses cannot merge the
observations again. Rejections before contract validation keep the legacy header.

FD export validates the complete retained descriptor set before creating any
exports. Export objects are prepared privately with checked allocations;
registry publication uses separate links in those objects and cannot allocate.
Partial preparation is released through the snapshot's normal and fatal-exit
cleanup paths. All entries finish preparation before any export or handoff pin
is published. Optional RDMA rings and backlogs remain private across region
registration; cancellation and retirement revoke the registration before freeing
the backing memory. Export and backlog membership use nonallocating intrusive
registries. RDMA polling starts on acceptance alongside input pumps.
A deferred pipe writer pins its exporter and RDMA ring while using ring bytes
outside the IPC lock. Peer or submission cleanup may detach the endpoint then;
the returning worker completes retirement before releasing its File reference.
An executor-linked submission guard owns published export IDs and handoff File
references through route construction and submission preparation. Publication
moves them to durable task tracking under its lock. Tracking slots use checked
allocation and retain stable addresses and reuse; allocation failure reports
`ENOMEM`. Receiver running-task publication is also checked before scheduling;
failure destroys the prepared child, descriptors, and process charge. Compaction
preserves running-task order and bounded accept-retry fairness. Imported proxy
registry insertion cannot allocate after FD installation.
Completed receivers retain their monitor reference, output, and resource state
if allocating a completion record fails. A later timer tick retries collection.
Once queued, a failed transport send reuses the same detached completion node
without allocation; output and accounting remain attached until delivery or
session retirement.
Oversized wire frames have separate executor-linked ownership through send/wait,
so fatal exit releases them even after IPC ownership has moved to task tracking.
Send failure, timeout, and
rejection return ownership to the guard before consuming that tracking entry;
fatal exit drains remaining entries even during a re-entrant File release.
Accepted handoff pins remain on that cleanup list until exact-FD commit has
consumed them all. Preparing or rejecting a submission leaves native pipe
capacity unchanged; optional growth occurs on actual remote output, preserving
buffered bytes. Zero-length and invalid writes do not grow the pipe.
More than 16 IPC descriptors, local epoll state, and unsupported
ordinary inherited files reject with `ENOTSUP`. This includes regular files on
standard descriptors: capturing output at home cannot enforce a limit lowered
later by the remote process. Pipes, sockets, PTYs, and non-IPC DEVFS standard
descriptor fallback remain supported; CLOEXEC descriptors are excluded. A
strict remote request receives the error, while placement that permits local
fallback executes locally. Receiver-opened files use ordinary kernel resource
enforcement. Export admission preserves supported inherited descriptors even
after NOFILE is lowered below their numbers.

Source read pumps remain inactive while the receiver admits the task. Only
accepted submissions activate them, after the origin commits its descriptor
handoff. Rejection, send failure, and timeout do not start pumps. Activation
matches the consumer and resource identity under the export lock; repeated
activation is harmless. PTY aliases retain a single pump on the lowest FD.

The kernel advertises `WKI_CAP_RESOURCE_LIMITS` by default. Both direct submit
APIs require a policy owner and negotiated support; placement checks capability
before descriptor export. Strict placement returns `ENOTSUP` for incompatible
peers, and permitted local fallback preserves enforcement. The diagnostic boot
token `wki.resource_limits_off` removes this capability without changing local
limits or allowing legacy receiver execution. Multi-node validation evidence is
recorded in the goal ledger.

The kernel also provides a retained `RemoteServiceAccounting` object for a
receiver and its descendants. Process inheritance retains membership; threads
charge through their process owner. Snapshots include CPU charged during the
execution, non-CPU peaks/current usage, and denial deltas after attachment.
Imported denial history is excluded, and completed members contribute their
observations once. A monitor reference can outlive the root process while its
descendants continue running. Service-bearing WKI submissions attach this
accounting object before scheduler publication and retain a receiver monitor
until a final subtree report is queued reliably or the session is retired.

`ServiceReportTarget` retains a destination independently of a submitted Task:
either a local service slot or an enclosing remote accounting object. Its report
model validates monotonically increasing cumulative observations and adds only
new CPU and denial counts. Each pending stream owns its latest current usage and
live-process count. Moving a target preserves that ownership; a final report must
show no remaining processes or nested work. Lost streams preserve known totals
and set the `ACCOUNTING_INCOMPLETE` service snapshot flag, which `servicectl`
explains as incomplete cumulative accounting. The version-1 `TASK_SERVICE_REPORT`
message (type `0x57`, 384 bytes) carries cumulative observations and a sequence
on the submission's reliable RESOURCE channel. Receivers copy unaligned packet
bytes, validate the exact node/channel generation, and merge each delta once.
A submitted row remains reserved for reports after the ordinary root result is
consumed; it retains the service destination without retaining the root Task.
Receiver rows likewise release the root and captured output after queuing its
completion, then monitor descendants and nested report streams independently.
Finality waits for exit accounting readiness, zero active CPU contributors,
and completion of admitted deferred core observations,
independently of later Task metadata reclamation. Retained exited members still
contribute historical counters until their destructors fold them into totals.
Cancellation never looks up the exited root's potentially reused PID.

Finite aggregate service CPU policy is refused with `ENOTSUP` before descriptor
export. Strict placement returns the error; placement allowing local fallback
keeps local enforcement. This observation protocol cannot enforce a shared finite
budget by copying it into another process. Unlimited services can use the report
contract when resource capability negotiation is enabled. Known admission rejection
ends the report hold without marking accounting incomplete; uncertain transmission,
executor abandonment, or peer retirement preserves known totals as lower bounds.
Final report send success transfers a retransmittable copy to the channel before
the receiver releases its monitor; transient send failure retains it for retry.
The capability covers this report protocol together with resourcev2 admission
and completion; all three use the same negotiated execution contract.

For the service-report contract, WKI completion merges process counters
and excludes report-counted denial increments under one service/resource lock
scope. This prevents a snapshot from publishing a duplicate count between the
two updates. Service CPU then comes from the report stream; process wait usage
still comes from the completion record. Accepted proxies are excluded from
executing service membership without releasing their physical charges. Receiver
attachment can use the original imported denial history so construction-time
denials remain new observations. Affiliated submissions set the service-report
flag after capability and aggregate-policy admission.

## Observations and access

`/proc/PID/limits`, `/proc/self/limits`, and `/proc/PID/task/TID/limits` provide a
snapshot with columns:

```
Resource Soft Hard Current Peak Denied Reason Units Enforcement
```

Infinity is written as `infinity`. Reasons are `none`, `soft_limit`,
`overflow` or `hard_limit`. All seven resource rows report `enforced`.
Opening a fresh file obtains a
new snapshot; repeated reads of one open file retain its original snapshot.
Every read checks the reader again, including reads of an inherited cached
file. The process itself and root can read its observations; unrelated
unprivileged processes receive `EACCES`.

Process exit closes new thread admission and waits for existing threads to
finish their cleanup and final CPU accounting while the leader remains
schedulable. This count is separate from thread metadata references and does
not wait for garbage collection. Thread and leader finalization flush the last
runtime slice with its user/system split, then freeze further Task CPU charges.
The immutable parent exit event includes the complete process lifetime counters
plus reaped-child CPU, so wait4 retains both joined and group-terminated workers.

Process CPU clocks, `times()`/`getrusage()`, and process procfs CPU fields read
the persistent user/system counters used by resource accounting. They retain
reclaimed-thread and imported remote-exec history. Thread clocks and procfs
thread views retain their own per-thread CPU scope. Process procfs child CPU
columns expose reaped-child totals, serialized with local reaping and remote
completion. CPU time converts directly from microseconds to `timespec`, without
a wrapping intermediate nanosecond multiplication.

NOFILE current includes committed descriptors and pending reservations; peak
is its lifetime high-water count. FSIZE is a per-file endpoint bound, not the
sum of all file sizes: current is the latest successful file extent observed
by that process, and peak is the largest such extent. For a remote buffered
write, the observation is the accepted endpoint, before eventual durability.
Denied includes requests shortened at the boundary, but only a wholly refused
write queues SIGXFSZ. A denied operation never creates a resource charge.

## Validation workflow

Use Normal `Build WOS` and isolated KTEST; preserve their separate artifacts.
With existing host network setup, `bin/wos-ktest --no-setup` and
`bin/wos-cluster --no-setup` can launch rootlessly. The latter also supports
`--debug-node` and a live-debug descriptor for WOSDBG or GDB.

`testprog resource-limits` runs isolated userspace boundary, inheritance,
privilege, concurrent descriptor and TMPFS/XFS append tests. It also checks
procfs counters and cached-file privacy. The current matrix uses a root-owned
XFS fixture under `/root` and a TMPFS fixture under `/tmp` and removes them.

The core fixture deliberately faults with `ud2`. Zero, one-byte CORE and
one-byte FSIZE cases must leave no file. An unlimited case retains its exact
reported `/tmp/wos_resource_core_probe_*_coredump.bin` artifact for WOSDBG
inspection; remove that owned fixture before repeating the core matrix.

`testprog resource-limits cpu-observation` checks process clock, `getrusage`,
and procfs agreement after three CPU-consuming threads have joined, then checks
reaped-child totals. It does not need a custom service manifest.

`testprog resource-limits cpu-wait-observation` checks wait4, parent rusage,
and procfs after a child joins CPU workers and after it exits with workers still
running. Both cases must include the workers' CPU contributions.

For service validation, append `tests/fixtures/resource_governance_services.fragment`
to the default manifest only in a disposable VM's rootfs, before boot. Inspect
`servicectl status resource-policy`, `resource-nproc-zero`, `resource-as-denied`,
and `resource-cpu-restart`. The first two must exit successfully, zero AS must
report exec error 12, and the CPU service must stop near two accumulated CPU
seconds despite individual runs lasting only 250 ms. A manual restart after
exhaustion must retain the CPU total and report error 122 (`EDQUOT`). Finite
policy checks are logged under `journalctl -u resource-policy`; current
observations return to zero after exit while peaks/denials remain. The default
manifest stays unchanged. `testprog resource-limits service-public-status`
checks private resource-read denial and public status after dropping UID.

For two-node service admission checks, install
`tests/fixtures/resource_governance_remote_services.fragment` in a disposable
`wos-0` rootfs. Once `wos-1` is connected, restart `resource-remote-finite` and
`resource-remote-fallback`. Both have a finite aggregate CPU budget. The strict
`on wos-1` command must report `ENOTSUP`; the remote-preferred command must finish
locally. This exercises unsupported aggregate placement without changing the
normal service manifest.

## Completion evidence (2026-09-08)

Goal 22 is complete. Normal `Build WOS` (`RelWithDebInfo`) and the isolated
KTEST diagnostic build passed. The final diagnostic run passed 8,618 checks;
71 host ABI, resource/report model, wire-contract and init tests passed under
ASan/UBSan. All 14 changed source-invariant test scripts passed.

The final capable two-node run passed the 160-check resource matrix, nine CPU
signal checks, ten CPU wait checks and fourteen CPU observation checks. A
33,296-byte remote core dump was reflected in the originating service's peak.
Background and nested descendants continued reporting after their roots exited,
then both peers reclaimed their report rows. Deliberate receiver loss retained
known totals, marked them incomplete and reclaimed compute/IPC state. With
resource capability disabled on one peer, both directions rejected strict
placement before execution; local fallback retained finite NOFILE policy, and
the disabled peer passed the full local resource matrix.

The goal ledger records earlier local service restart/exhaustion, privilege,
concurrent reservation and failure-injection evidence, plus the final kernel
hashes and log paths. Test VMs were stopped, and normal and KTEST artifacts
remained isolated. Two-node boots used delayed WKI link-up after local root
mounting because testing exposed a separate root-mount/discovery `EAGAIN` race;
that boot race is not fixed by resource governance.
