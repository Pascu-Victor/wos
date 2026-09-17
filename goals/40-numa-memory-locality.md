# Goal 40: NUMA-Aware Placement and Safe Large Guest/Process Pages

## Copy-pastable goal command

```text
/goal Implement NUMA-aware CPU/memory placement and opt-in safe 2 MiB anonymous mappings in WOS, with validated firmware topology, explicit placement policy, complete page lifetime and accountable fallback. Satisfy all criteria in goals/40-numa-memory-locality.md with executable topology/allocator tests, VM lifecycle regressions, Normal Build WOS and user-run NUMA locality and performance evidence. Preserve physical accounting, COW/usercopy/reclaim correctness, scheduler fairness and UMA boot. Work within the listed topology/MM/scheduler interfaces using bounded subagents and measured checkpoints; follow the file's blocked rule for missing hardware or evidence.
```

## Outcome and scale

Make WOS use a multi-socket machine's memory deliberately: tasks allocate near
their CPU domain or according to explicit policy, and opt-in large anonymous
mappings have correct COW, pinning, teardown and pressure behavior.
**Target: one to two continuous weeks with topology, MM-lifecycle and
benchmark/test subagents.** Topology labels alone do not meet this goal.

This goal can start independently of virtualization. If native guest-memory
slots exist by execution time, integrate their supported placement policy and
pin lifecycle; do not invent a dependency on an unfinished VMM.

## Baseline and impact map

Investigated 2026-09-08. `platform/smt/smt.cpp::init_cpu_domains()` constructs a
root and single flat group and explicitly leaves NUMA parsing for later.
`platform/mm/phys.hpp` already has physical zones and an optional huge allocator.
`virt.opt.cpp::snapshot_user_page_mapping_locked()` rejects huge-leaf subpage
pins; user-space teardown also has huge-page skip paths. These are concrete
lifetime limitations to resolve before exposing general large-page mappings.

Read `modules/kern/src/platform/acpi/{acpi.cpp,acpi.hpp,tables/sdt.hpp,madt/madt.cpp}`,
`modules/kern/src/platform/smt/{smt.hpp,smt.cpp}`,
`modules/kern/src/platform/sched/{scheduler.hpp,scheduler.cpp,migration_policy.hpp,task.hpp}`,
`modules/kern/src/platform/mm/{phys.hpp,phys.opt.cpp,page_alloc.hpp,page_alloc.cpp,paging.hpp,virt.hpp,virt.opt.cpp,reclaim.hpp,physical_balance.hpp}`,
`modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp`, and
`modules/kern/src/test/{mm_ktest.cpp,address_space_ktest.cpp,anonymous_swap_ktest.cpp}`.
Read current `memacc`/`perf` CMake, sources, aliases and procfs ABI before
extending their diagnostics.

Flow: validated firmware CPU/memory affinity and distances → immutable boot
topology → allocation policy/domain → zone/physical owner → 4 KiB or compound
2 MiB mapping → access/pin/COW/reclaim → safe split and reclamation. Scheduler
placement consumes locality hints while retaining affinity and fairness.

## Checkpoints and parallel ownership

1. Primary freezes domain/policy/accounting interfaces. A subagent implements
   bounded SRAT/SLIT parsing/tests; others audit compound-page lifetime and
   prepare UMA/NUMA workload fixtures.
2. Integrate domain-aware allocation, first touch, preferred/bind/interleave
   policy, per-domain reserves and pressure fallback.
3. Implement opt-in 2 MiB anonymous mappings, split/COW/pin/unmap lifecycle and
   integration with supported guest-memory slots where present.
4. Collect user-run locality and pressure results, check scheduler fairness,
   and independently audit frame ownership from allocation through final free.

## Acceptance criteria

1. Reject malformed/truncated/checksum-invalid or contradictory affinity tables
   safely. Memory holes, sparse proximity IDs, memory-only domains and CPUs
   without usable local RAM have defined handling; missing tables yield UMA.
   Explicitly reject unsupported CPU/domain counts without truncating masks.
2. Preferred policy falls back visibly, strict bind returns a defined allocation
   failure, and interleave distributes pages predictably. Policy is inherited
   across supported task creation without bypassing quota/affinity limits.
3. Under sufficient local free memory, at least 95% of eligible first-touch
   pages in a CPU-pinned workload reside in its local memory domain. Counters
   exclude reserved/pinned exceptions explicitly instead of hiding them.
4. Domain pressure uses local reclaim/fallback policy without starving another
   domain's progress reserves. Global and per-domain physical ownership totals
   reconcile exactly before/after stress.
5. Eligible opt-in aligned anonymous ranges use 2 MiB leaves; partial unmap,
   mprotect, COW/fork, usercopy pins, ptrace access and teardown split or retain
   compound backing correctly. No huge-page teardown skip leaks newly supported
   mappings, and no subpage free releases another live mapping.
6. Swapping/reclaim of a large mapping either safely splits to supported 4 KiB
   ownership or applies a documented charged nonreclaimable policy with admission
   bounds. Memory pressure never evades process limits through huge allocation.
7. TLB invalidation precedes frame reuse at every split/protection/unmap path.
   Property tests cover pin/unpin/COW/free races and forced allocation failure
   during splits. Supported guest slots do not conflict with these transitions.
8. User-run UMA and at least two-domain NUMA fixtures exercise first touch,
   interleave, strict exhaustion and 1,000 map/fork/pin/unmap cycles. No physical
   ownership mismatch or monotonic retained-memory growth remains.
9. On a real NUMA fixture, a fixed CPU-pinned pointer-chase benchmark improves
   median access latency by at least 10% over deliberate remote placement across
   10 paired runs. Large-page evidence verifies mappings and TLB-sensitive
   workload results separately; emulated topology is not real NUMA performance
   evidence. If the machine cannot supply that contrast, record the blocker.
10. Existing UMA workloads show no greater than 5% median throughput regression
    over 10 paired runs on the fixed reference suite, with variance/raw samples
    reported. Scheduler fairness and task-affinity tests still pass.

## Boundaries, invariants, and rollback

No CPU/memory hotplug, automatic cross-node WKI memory placement, 1 GiB userspace
pages, global compaction rewrite, or automatic NUMA migration daemon.
Prove 2 MiB lifecycle before enabling promotion; keep promotion opt-in.

Boot topology parsing cannot depend on unavailable allocator services.
Allocator-zone, page metadata, address-space, pin, reclaim and runqueue locks
need a documented order; topology snapshots are immutable after publication.
Rollback disables new large mappings and NUMA policy, safely splits/drains
supported live mappings, and retains a tested UMA path.

## Execution, evidence, and stop rules

Read `AGENTS.md`, the subsystem index, applicable kernel/WKI/critical-path/build
instructions, and the source entrypoints below before changing code. Source
wins over this dated plan. Make an impact map and the full repository patch
contract, including locks, unsafe contexts, ownership, ABI, validation, and
rollback, before each materially different critical patch.

Use a primary integration agent and up to three active subagents with bounded,
non-overlapping assignments; rotate assignments between checkpoints. The primary
owns shared headers, integration, and acceptance. Keep
`goals/progress/40/ledger.md` and `goals/progress/40/evidence.md`:
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
