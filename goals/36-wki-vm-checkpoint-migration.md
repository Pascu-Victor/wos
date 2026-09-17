# Goal 36: Recoverable VM Checkpoints and WKI Live Migration

## Copy-pastable goal command

```text
/goal Implement versioned recoverable VM checkpoints and bounded pre-copy live migration between compatible Intel VMX/EPT executors and between compatible AMD SVM/NPT executors over authenticated WKI, with one authoritative execution owner and consistent virtual-device state. Verify all acceptance criteria in goals/36-wki-vm-checkpoint-migration.md through crash-point state tests, malformed-checkpoint tests, Normal Build WOS and user-run migration traces. Preserve guest memory, disk ordering, CPU compatibility and host isolation. Limit work to VM state transfer, checkpoint persistence and ownership handoff; iterate with bounded subagents and the file's evidence ledger, and report indispensable blockers according to its stop rule.
```

## Outcome and scale

A running Linux VM can be checkpointed, restored, and moved from WOS executor A
to compatible executor B while retaining architectural state and virtual-device
identity. **Target: one to two continuous weeks with a primary and up to three
active subagents.** Work includes persistence, dirty tracking, quiescence,
ownership transfer, failure recovery, and measurements.

**Hard prerequisites:** accepted goals 33–35, including explicit VM ownership and
remote device binding identity. Goal 37 is optional; use a stable owner-hosted
disk whose backing survives the executor move. This goal must specify what a
checkpoint captures about disk contents rather than assuming shared disk
visibility creates a consistent snapshot.

## Baseline and impact map

Investigated 2026-09-08. No native VM snapshot/migration engine was found.
The current node launcher uses `host,migratable=no,+invtsc`; those outer-QEMU
settings do not establish a portable guest CPU contract.

Read the accepted VM/VMM/control implementations and
`modules/kern/src/platform/{asm/cpu.hpp,sched/task.hpp,mm/virt.hpp,mm/virt.opt.cpp}`,
`modules/kern/src/net/wki/{wire.hpp,wki.hpp,wki.cpp,peer.cpp,channel.hpp,dev_proxy.hpp,dev_server.hpp}`,
`modules/kern/src/dev/block_device.hpp`, `modules/kern/src/vfs/vfs.hpp`,
`scripts/cluster/node_setup.py`, and goal 35's failure model.
Inspect guest virtual timers/interrupt state, device queue/reset state, and the
backing store's actual durability calls before defining the checkpoint format.

Flow: prepare destination → validate CPU/device contract → pre-copy dirty RAM →
quiesce all vCPUs and device workers → capture final dirty pages and device state →
commit execution epoch → start destination → retire source. A checkpoint also
needs an atomic manifest and a matching immutable disk generation.

## Kernel and userspace state ownership

Use the [architecture contract](VIRTUALIZATION-ARCHITECTURE.md). `vmmd` coordinates
transfer/ownership; `wos-vmm` quiesces its event loop and I/O workers and serializes
UART, PCI/virtqueue, I/O APIC, timer and backend state. The kernel exports validated
architectural vCPU, virtual LAPIC and EPT/NPT dirty state. A CPU register dump
alone cannot restore a VM. Freeze one capture boundary across those components,
including pending interrupt levels/EOIs and completed-but-unpublished guest I/O.

Require Intel→compatible Intel and AMD→compatible AMD migration and restore.
The destination rebuilds its backend structures from the architectural schema.
Sharing a userspace machine model does not imply safe Intel↔AMD live-state
transfer: cross-vendor migration is rejected before source quiescence/cutover.
Loading the same Linux disk afresh on either vendor remains supported.

## Checkpoints and parallel ownership

1. Primary fixes cutover and crash semantics; subagents model ownership epochs,
   build checked checkpoint codecs, and implement dirty bitmap tests.
2. Add local stopped checkpoint/restore with full vCPU/timer/device state and
   disk generation matching before implementing live transfer.
3. Integrate bounded pre-copy, explicit stop-and-copy fallback, device drain,
   destination admission, console identity and time conversion.
4. Inject every handoff failure; collect user-run migration and compatibility
   measurements; independently review the ambiguity recovery paths.

## Acceptance criteria

1. A versioned bounded checkpoint records architecture/features, vCPU registers,
   XSAVE, pending interrupts, timer/clock state, RAM, device state, VM identity,
   resource generations and disk-generation reference. It contains no host
   pointer or serialized VMCS/VMCB/host-save image requiring the old host.
2. Restore verifies all lengths, ranges, required features, hashes and disk
   generations before execution. Truncated, corrupted, newer-unsupported and
   incompatible CPU/device artifacts reject without leaking resources.
3. Local restore of a 4-vCPU Linux VM preserves deterministic memory/register
   sentinels and fsynced disk hashes. Define crash-consistent versus application-
   consistent semantics; do not claim application consistency without guest
   cooperation.
4. Dirty tracking captures writes by both guest CPUs and emulated devices.
   Tests cover dirty-bit clearing races, slot invalidation, write protection,
   generation rollover and pages dirtied during final collection.
5. A durable or conservatively recoverable ownership state machine guarantees
   that at most one executor can run a VM after cutover. Lost commit/ACK,
   controller reboot and partition cannot authorize two executors.
   Ambiguous ownership freezes progress until safely reconciled.
6. Migration preserves disk ordering, pending I/O completion identity, console
   continuity and guest monotonic time. Externally visible writes are not
   duplicated through replay of a captured in-flight request.
7. Pre-copy has finite round/byte/time bounds. A guest dirtying faster than the
   link either follows explicitly selected bounded stop-and-copy or cancels
   before cutover; it never loops forever or resumes two owners.
8. On a pinned fixture with at least 1 Gbit/s measured link capacity, a 1 GiB,
   2-vCPU guest dirtying at most 10 MiB/s completes migration in 60 seconds with
   final pause at most 2 seconds over 20 runs. If the fixture is unavailable,
   this criterion remains unverified; record results separately for other sizes.
9. Exercise source/destination/controller failures before and after every
   durable handoff transition, including during device drain. Evidence proves
   exact owner state and balanced resources, not merely successful ping afterward.

10. Run the restore, migration, failure and performance criteria independently
    on Intel and AMD executor pairs. Test second-level dirty tracking and timer
    state with the backend's real capability/fallback paths. Verify cross-vendor
    migration rejection leaves the source VM running and its resources intact.
11. Userspace device queues, kernel virtual interrupts and in-flight I/O agree
    after restore. Exercise capture between backend completion, used-ring
    publication and interrupt injection on both vendors.

## Boundaries, invariants, and rollback

No cross-vendor migration, post-copy, transparent restart after total volatile
state loss, arbitrary firmware/device support, or uninterrupted availability
during partition. Compatible CPU baselines are negotiated independently of the
outer QEMU CPU model. Never use wall-clock agreement alone as a fencing proof.

Keep the source resumable until the transfer contract relinquishes ownership.
After that point rollback is a new fenced reverse transfer, not an unconditional
source resume. Checkpoint writes happen outside VM/page-table/runqueue/WKI
spinlocks. Limit snapshot memory and network credits. Feature rollback disables
new transfers while retaining readable committed checkpoint formats.

## Execution, evidence, and stop rules

Read `AGENTS.md`, the subsystem index, applicable kernel/WKI/critical-path/build
instructions, and the source entrypoints below before changing code. Source
wins over this dated plan. Make an impact map and the full repository patch
contract, including locks, unsafe contexts, ownership, ABI, validation, and
rollback, before each materially different critical patch.

Use a primary integration agent and up to three active subagents with bounded,
non-overlapping assignments; rotate assignments between checkpoints. The primary
owns shared headers, integration, and acceptance. Keep
`goals/progress/36/ledger.md` and `goals/progress/36/evidence.md`:
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
