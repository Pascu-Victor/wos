# Goal 37: Crash-Consistent Replicated VM Volumes over WKI

## Copy-pastable goal command

```text
/goal Deliver a WKI-backed replicated block volume for VM disks that preserves acknowledged durable writes through one storage-node failure, rejects stale writers and supports consistent immutable snapshots. Satisfy every acceptance criterion in goals/37-wki-replicated-vm-volumes.md using executable storage models, crash/reorder injection, Normal Build WOS and user-run three-node disk and guest evidence. Preserve existing block leases, flush semantics and bounded authenticated WKI admission. Scope work to the stated three-replica volume service and adapters, iterate with bounded subagents and recorded evidence, and follow the file's blocker rule if durable-storage or runtime prerequisites are missing.
```

## Outcome and scale

Make a VM disk independent of one storage owner: a three-replica WKI volume
retains every write acknowledged as durable after any one storage-node failure,
with explicit quorum loss and fenced single-writer behavior.
**Target: one to two continuous weeks with storage, protocol/model, and
failure-test subagents alongside the integration agent.**

The storage service can be developed independently of virtualization.
**Final guest acceptance requires goals 33–35.** Goals 20, 25, 26 and 30
are integration overlaps, not assumed prerequisites; use authenticated messages
and the existing block API. Avoid making secure RDMA or a new asynchronous
block stack a hidden prerequisite.

## Baseline and impact map

Read `modules/kern/src/dev/block_device.hpp` and `block_device.cpp`,
`modules/kern/src/net/wki/{wki.hpp,wire.hpp,wki.cpp,peer.cpp,remotable.hpp,dev_proxy.hpp,dev_server.hpp,blk_ring.hpp}`,
their relevant implementations, `modules/kern/src/vfs/vfs.hpp`,
`modules/kern/src/vfs/fs/xfs/xfs_trans.hpp`, and
`modules/kern/src/test/{fault_block_device.hpp,fault_block_device_impl.hpp,block_device_ktest.cpp}`.

On 2026-09-08, the block abstraction exposes read/write/flush and overlapping
writer leases; WKI supplies owner/proxy attachments. That is not a replicated
commit log or quorum authority. Reuse local durability/fault infrastructure
only after verifying its actual implementation.

Flow: VM block request → volume writer epoch → ordered replicated log →
durable quorum → guest completion. Snapshot creation binds a committed index to
immutable block mappings. Rejoin transfers a verified prefix and data before a
replica becomes eligible for reads, writes, or elections.

## Checkpoints and parallel ownership

1. Freeze the consistency model, three-member fixed membership, commit/flush
   rules, storage format and failure assumptions. Primary owns the shared wire
   contract; subagents own the executable model and disk parser/fault harness.
2. Implement durable log/recovery and authenticated replication with monotonically
   fenced terms, request deduplication and bounded queues.
3. Integrate block adapter, snapshots/clones, bounded catch-up and checksummed
   repair; a subagent owns administration and diagnostics.
4. Inject crashes around each write/flush/publication point and run user-driven
   storage/VM failover workloads against the actual implementation.

## Acceptance criteria

1. Specify linearization points for write, flush, snapshot and writer transfer.
   A successful flush covers the advertised prior-write order on a durable
   majority; document whether ordinary write ACKs are durable or volatile.
   No response may promise durability before the required physical flush.
2. Use a reviewed consensus algorithm with a persistent term/vote/log contract
   for the fixed three-member group. Prove safety under message loss, duplication,
   reordering, partitions and restart with exhaustive bounded model traces.
   Heartbeat absence alone never authorizes a new writer.
3. A stale leader/writer cannot commit or mutate the active volume after a newer
   epoch takes authority. A minority returns a bounded unavailable result.
4. For every crash point, recovered reads match the reference model's committed
   history. Test torn sectors, lost/reordered writes before flush, checksum
   failure, duplicate operation IDs and response loss.
5. A completed snapshot names one committed disk generation. Restore/clone
   reads immutable data; crashes during manifest publication or deletion cannot
   expose partial snapshots or prematurely recycle shared blocks.
6. Rejoining replicas verify data, discard conflicting uncommitted suffixes and
   catch up without serving stale reads or exceeding configured memory/I/O
   budgets. Corruption is reported or repaired from verified replicas.
7. The operator can create, attach, inspect, snapshot, detach and remove a
   test volume; destructive removal is explicit and limited to its named data.
   Fixed membership is exposed honestly; online reconfiguration is excluded.
8. User-run three-node tests kill each storage node in turn during guest writes,
   flushes, snapshot publication and rejoin. After recovery, all guest-acknowledged
   durable sequence markers and checksums remain present.
9. A 2-hour mixed guest I/O run and 100 snapshot/rejoin cycles show bounded
   journal growth after compaction and balanced block/channel/buffer references.
   Report normal/degraded throughput and p95/p99 latency with fixture metadata.

10. Integrate the volume through `wos-vmm`'s scoped block backing adapter from
    the [architecture contract](VIRTUALIZATION-ARCHITECTURE.md). Guest durable-I/O
    and snapshot evidence must include both Intel and AMD executors. Storage
    replica protocol/format must not depend on a VM backend or controller vendor.

## Boundaries, invariants, and rollback

No general distributed filesystem, erasure coding, multi-writer shared guest
filesystem, dynamic quorum membership, or claim of zero downtime during quorum
loss. Model crash-stop failures and detectable storage corruption; Byzantine
replicas and simultaneous loss of a durable majority are outside the contract.

Registry, replication-log, writer-lease, buffer-cache and proxy locks must have
a documented order. No storage flush, WKI ACK wait, or reclamation callback runs
under RX or spinlock admission. Guest write uncertainty is an explicit status.
Use a new opt-in volume format; rollback preserves its disks and rejects new
attachments rather than opening replica backing files as independent writable
disks. Any format migration requires a tested read/rollback path.

## Execution, evidence, and stop rules

Read `AGENTS.md`, the subsystem index, applicable kernel/WKI/critical-path/build
instructions, and the source entrypoints below before changing code. Source
wins over this dated plan. Make an impact map and the full repository patch
contract, including locks, unsafe contexts, ownership, ABI, validation, and
rollback, before each materially different critical patch.

Use a primary integration agent and up to three active subagents with bounded,
non-overlapping assignments; rotate assignments between checkpoints. The primary
owns shared headers, integration, and acceptance. Keep
`goals/progress/37/ledger.md` and `goals/progress/37/evidence.md`:
checkpoint, hypothesis, change, command, result,
artifact/build ID, remaining failure, next action. Preserve existing user edits.

Run Normal `Build WOS`; if the task is unavailable, mirror the current
`.vscode/tasks.json` command/environment. Register executable host tests with
the existing CMake test infrastructure and run their generated CTest entries.
Use `scripts/dev/format_repo.sh --check <changed-code-paths>`.
Prepare isolated diagnostics with `bin/wos-ktest --build-only`, retaining
`build-ktest/` and `ktest-data/` isolation. Read
`docs/kernel_debug_flags.md` before changing diagnostic flags. Never combine
report-mode KUBSan with the UBSan trap profile.

Kernel/cluster runtime execution and debugging are user-run under the current
instructions. Supply exact fixture-specific commands, expected observations,
and log locations early; analyze returned evidence while continuing independent
implementation and host validation. Runtime requirements in this file are mandatory,
not replaced by model tests, source-string tests, a successful build, or a demo
script that has not run.

Finish only when **all** acceptance criteria in this file have supporting evidence.
The one-to-two-week horizon is a scope estimate, not an enforced delay, token
budget, autonomy guarantee, or alternative finish line. Do not pad work or
silently shrink the contract. If a premise changes, record the discrepancy and
reconcile the contract with the actual tree before dependent edits. If an
indispensable blocker persists for three consecutive goal turns and no useful
authorized work remains, report the attempts, evidence, exact missing input,
and restart step, and mark the active goal blocked. Missing prerequisites or
hardware are never completion. Budget and pause controls remain user/system
controlled.
