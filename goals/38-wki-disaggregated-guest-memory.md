# Goal 38: WKI Disaggregated Guest Memory

## Copy-pastable goal command

```text
/goal Allow a hardware-virtualized WOS guest to use RAM backed by other WOS nodes through a bounded authenticated WKI page service and local EPT/NPT cache, preserving guest bytes and memory ownership during eviction, faults and provider loss. Complete all criteria in goals/38-wki-disaggregated-guest-memory.md with executable ownership models, actual page-service tests, Normal Build WOS and user-run Linux guest capacity/fault evidence. Preserve host reclaim progress, isolation and fail-closed data-loss behavior. Restrict scope to one execution node per VM with remote RAM providers; use bounded subagents and checkpointed evidence, and apply the file's blocked stop rule for unavailable prerequisites.
```

## Outcome and scale

Pool RAM across WOS systems for a single VM: vCPUs execute on one node while a
bounded local cache fetches and evicts guest pages to authenticated WKI memory
providers. **Target: one to two continuous weeks with page-state, transport/test,
and VMM integration subagents.** This is a distinct distributed-memory feature,
beyond remote launch or migration.

**Hard prerequisites:** accepted goals 33–35. Migration and disk replication are
not required. Secure RDMA goal 25 is optional and cannot be bypassed to make the
benchmark pass. Simultaneously running one guest's vCPUs on several physical
nodes is excluded: that additionally requires a cross-node guest memory
consistency and interrupt architecture. This goal does not claim to supply it.

## Baseline and impact map

Read `modules/kern/src/net/wki/{wki.hpp,wire.hpp,wki.cpp,zone.hpp,zone.cpp,peer.cpp,auth_policy.cpp}`,
`modules/kern/src/platform/mm/{virt.hpp,virt.opt.cpp,phys.hpp,reclaim.hpp,reclaim.cpp,anonymous_swap_state.hpp}`,
`modules/kern/src/platform/sched/resource.hpp`, and the accepted EPT/NPT/VM/device
implementations. On 2026-09-08, WKI zones have explicit local backing, peer
identity, locks, and message-based reads/writes. They do not make remote physical
addresses CPU-addressable guest memory.

Propose a VM-specific page service and cache policy. Flow: guest EPT/NPT miss →
retain VM/page-generation request → park vCPU → task worker authenticates/fetches
page → verify identity and checksum → install EPT/NPT → invalidate translations as
needed → wake vCPU. Eviction write-protects/unmaps all access paths, drains
device pins, writes a stable page version, receives acknowledgement, and only
then frees local backing.

## Userspace and backend integration

Follow the [architecture contract](VIRTUALIZATION-ARCHITECTURE.md). Replace direct
all-resident RAM assumptions in `libwosvirt`/`wos-vmm` device access helpers with
checked residency-aware reads/writes and retained page generations. Guest CPU
faults enter a kernel deferred page worker; userspace emulated DMA follows the
same ownership protocol, never an unrelated stale mapping of the page.

The common second-level mapping interface supplies Intel EPT and AMD NPT
implementations with equivalent fault, invalidation and dirty-page semantics.
`vmmd` advertises provider capacity/policy and binds authorized reservations;
`vmctl inspect` shows local residency, provider bytes and stalled/failed pages.
Hardware-specific dirty-bit facilities are optional only with a tested correct
fallback. Both executor vendors are required acceptance targets.

## Checkpoints and parallel ownership

1. Primary fixes page/version/ownership and failure state machines. A subagent
   implements an independent reference model; others prepare provider protocol
   and actual guest capacity workloads.
2. Build bounded reservations, page transfer, local cache and cancellation with
   deterministic fault injection before connecting hardware EPT/NPT faults.
3. Integrate vCPU parking, EPT/NPT invalidation, emulated-device memory access,
   reclaim pressure, quotas and provider withdrawal.
4. Collect user-run capacity and fault traces; replay races found in execution
   against the model and audit every dirty-page ownership transition.

## Acceptance criteria

1. Per-page state distinguishes absent, fetching, clean resident, dirty resident,
   evicting and unavailable, including VM incarnation and page version.
   Concurrent faults coalesce without sharing waiters across VM generations.
2. No EPT/NPT entry points to a remote physical address. Only owned, locally resident
   backing is installed. VM close cancels or joins every fetch/evict/device pin
   before freeing page metadata.
3. Dirty data is never discarded until a validated remote copy exists.
   Eviction covers vCPU writes, emulated DMA, copy helpers, and checkpoint-facing
   APIs; stale replies cannot overwrite newer page data.
4. Host metadata, RX pools, page-transfer buffers and the minimum progress
   reserve remain local and bounded. The fault service does not require pages
   whose retrieval depends on its own exhausted allocation path.
5. Provider allocation is quota-controlled and bound to authorized VM/owner
   identities; stale sessions and recycled page IDs are rejected. Explicit
   withdrawal drains or returns busy; it never drops live backing silently.
6. Provider failure pauses or fails the affected VM within a documented bounded
   deadline and preserves unaffected VMs. Never substitute zeros for lost
   committed page contents. Volatile remote RAM is not a durability guarantee.
7. A user-run 4 GiB, 2-vCPU Linux VM operates with at most 1 GiB local resident
   guest RAM and at least two remote providers. Write and verify unique patterns
   over all guest test memory, repeat 10 working-set sweeps, and show provider
   bytes plus local-cache accounting prove actual remote capacity use.
8. Exercise duplicate/stale responses, partition, provider reboot, simultaneous
   eviction/fault, VMM kill and host memory pressure; no leaked pins or workers.
   Run a 2-hour mixed memory/device workload with a bounded host-memory plateau.
9. Report cache-hit/miss latency, p95/p99 guest stall, remote bandwidth,
   amplification, local reserve and performance at several working-set sizes.
   No claim of local-RAM latency or speedup is required.
10. Either support migration/checkpoint interaction explicitly with page-generation
    capture or reject those operations for remote-backed VMs before mutation.
    Do not silently produce incomplete snapshots.

11. Complete the capacity, pressure, fault and device-I/O criteria on an Intel
    VMX/EPT executor and an AMD SVM/NPT executor, including mixed-vendor RAM
    providers. Verify both hardware invalidation implementations and all device
    memory helpers participate in the same page-generation protocol.

## Constraints and rollback

No global WOS process-memory rewrite, cross-node coherent CPU cache, transparent
remote RAM failover, RDMA security downgrade, or memory overcommit without
provider reservation. MM/runqueue/EPT/NPT/WKI locks never surround a network wait;
faults park only after restoring a safe host context.

Rollback first drains remote backing to verified local capacity when available;
if capacity is insufficient, retain/pause the VM or terminate it explicitly.
Disabling the feature cannot free remotely authoritative pages while a VM still
depends on them. Keep the ordinary all-local VM path covered by regression tests.

## Execution, evidence, and stop rules

Read `AGENTS.md`, the subsystem index, applicable kernel/WKI/critical-path/build
instructions, and the source entrypoints below before changing code. Source
wins over this dated plan. Make an impact map and the full repository patch
contract, including locks, unsafe contexts, ownership, ABI, validation, and
rollback, before each materially different critical patch.

Use a primary integration agent and up to three active subagents with bounded,
non-overlapping assignments; rotate assignments between checkpoints. The primary
owns shared headers, integration, and acceptance. Keep
`goals/progress/38/ledger.md` and `goals/progress/38/evidence.md`:
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
