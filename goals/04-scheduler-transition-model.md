# Goal: Establish an Executable Scheduler Transition Model and Invariant Suite

## Planning metadata

- **Expected scale:** 1–2 continuous weeks of agent time with subagents active; the clock does not complete the work.
- **Primary integration agent:** owns the transition taxonomy, production refactor, lock/context contract, context-switch boundary, and final behavior-equivalence review.
- **Three bounded subagent workstreams:** (1) extract queue/state transition helpers and validators; (2) build deterministic fake-clock/fake-IPI model and interleaving tests; (3) migrate diagnostics/source tests and build scheduler stress/trace analysis fixtures.
- **Critical surfaces:** scheduler/preemption, timer IRQ and wake IPI, per-CPU runqueue locks, cross-CPU migration, handoff/context switch, task registry/lifetime, epoch GC, syscall return, WKI placement, and scheduler procfs/perf surfaces.
- **Completion rule:** executable evidence must cover the stated transitions and invariants. Splitting the large file, adding assertions, or consuming the planned time is insufficient.

## Copy-pastable goal command

```text
/goal Outcome: WOS scheduler queue and task-state mutations are expressed as explicit locked transitions with machine-checkable invariants and a deterministic interleaving harness covering wake, park, handoff, migration, preemption, exit, and GC, while preserving current EEVDF and placement behavior. | Verification surface: Normal Build WOS, focused host scheduler/context/TLB tests, isolated KTEST transition and SMP-model cases, plus user-run SMP self-host and scheduler-trace evidence. | Constraints: Preserve EEVDF weights/deadlines, wake policy, affinity/domains, WKI placement, timer/accounting behavior, assembly offsets, TSS/GS/CR3/FPU context, lock order, and allocation-free IRQ/locked paths. | Boundaries: Refactor and verify scheduler transitions only; do not tune policy, redesign waitpid/process lifecycle, alter WKI protocol, rewrite context-switch assembly, or perform unrelated cleanup. | Iteration policy: Specify invariants first, extract one behavior-neutral transition family at a time, run old and new tests after every stage, and add a reproducing model sequence before fixing each discovered invariant failure. | Blocked stop condition: Stop and mark blocked only after the same indispensable runtime-only scheduler blocker repeats for three consecutive goal turns, deterministic/model and source alternatives are exhausted, and the precise user-run trace/backtrace request is documented. | Live execution and debugging: I am able to run and debug WOS live without root access by appending --no-setup to the wos-ktest and wos-cluster scripts.
```

## Local source evidence

`modules/kern/src/platform/sched/scheduler.cpp` is currently 8,914 lines and combines EEVDF policy, runqueue mutation, timer decisions, wait scanning, handoff, migration, process hooks, accounting, GC, diagnostics, and in-file selftests. Critical mutation entrypoints include `process_tasks`, `deferred_task_switch`, `commit_handoff_task_at_return_boundary`, `reschedule_task_for_cpu_once`, `move_task_owner_to_cpu`, `place_task_in_wait_queue`, and `insert_into_dead_list`.

`RunQueue` is defined in `scheduler.hpp`; runnable, wait, dead, current, and handoff ownership share invariants across many helpers. `modules/kern/src/platform/smt/smt.hpp::PerCpuCrossAccess` protects runqueues with IRQ-disabling per-CPU locks and takes migration locks in ascending CPU order. Scheduler state also crosses `g_placement_lock` and `global_task_registry_lock`, task reference/epoch state, APIC timer/IPI paths, and context-switch assembly.

Executable coverage is comparatively narrow: `modules/kern/src/test/scheduler_ktest.cpp` is 219 lines, while `tests/host/unit/scheduler_source_test.py` is 2,357 lines and largely asserts source tokens and ordering. Current selftests cover valuable individual predicates, but they do not execute long sequences or systematically enumerate wake-before-park, two-CPU migration, handoff, and exit/GC interleavings.

## Measurable completion contract

1. Document and encode invariants for queue uniqueness, owner CPU, current/handoff exclusivity, heap index, publication, wake token, task state, preempt depth, and lifetime/epoch eligibility.
2. All production mutations of runnable/wait/dead/current/handoff membership route through a small reviewed transition layer; exceptional direct mutations are named and checked.
3. Validators can run under selftest/debug configurations at safe lock-held boundaries and emit bounded, allocation-free snapshots on failure.
4. A deterministic harness supplies explicit clocks, CPU identities, timer decisions, IPI delivery, and chosen interleavings without requiring a live VM.
5. Tests cover event-before-park, event-after-park, duplicate/concurrent reschedule, handoff wake, local and two-CPU migration, pin/domain rejection, preempt-disable pending work, task exit, dead-list insertion, and GC eligibility.
6. Each modeled sequence checks invariants after every step and has a bounded exploration strategy rather than an unbounded state search.
7. Source-shape tests remain only for true assembly/hardware ordering that cannot be executed in KTEST; transition semantics move to executable tests.
8. Production scheduling results, timer/accounting counters, and diagnostic output remain behaviorally equivalent unless a test exposes a concrete bug with its own regression case.

## Invariants, locks, unsafe contexts, and ABI

Runqueue locks disable interrupts; no transition may allocate, block, yield, or invoke VFS/network/WKI work while locked. Two-runqueue locks remain ordered by CPU number. Any relationship with `global_task_registry_lock`, `g_placement_lock`, futex/signal state, and epoch GC must be stated and tested. Cross-CPU readers either hold the corresponding lock or use an explicitly atomic snapshot. A published task cannot disappear between registries and queue ownership, and no task can be reserved/current on two CPUs.

Preserve `WOS_DEFERRED_TASK_SWITCH_OFFSET`, saved register/frame layouts, syscall-return handoff, TSS `rsp0`, GS scratch state, CR3, FPU state, APIC timer behavior, preemption nesting, and procfs/perf consumers. No syscall or wire ABI changes are expected.

## Boundaries

In scope are transition helpers, validators, model/KTEST infrastructure, minimal file extraction, and diagnostic tests. Out of scope are EEVDF tuning, load-balancing retuning, process-child event redesign, new CPU topology policy, WKI changes, assembly replacement, and broad Task modernization.

## Parallel workstreams

Subagent 1 extracts and specifies transition helpers under primary-owned interfaces. Subagent 2 builds the model runner and sequence/exploration corpus using production transition logic where possible. Subagent 3 converts brittle tests, adds invariant snapshots, and prepares stress/trace comparisons. The primary agent integrates one transition family at a time and arbitrates all shared scheduler headers.

## Verification plan

- Run the Normal `Build WOS` task.
- Run focused host scheduler, wait/poll, context-switch, TLB-shootdown, signal, futex, and WKI placement tests.
- Run isolated `bin/wos-ktest` scheduler, task, process, futex, signal, MM, and new transition-model suites.
- Per repository rules, ask the user to run SMP self-host builds, wake/migration/fork storms, affinity/domain scenarios, and collect `/proc/kcpustate`, scheduler traces, serial logs, and WOSDBG analysis if an invariant fires.

## Iteration policy

Freeze baseline behavior and counters first. Extract queue membership, then wake/park, then migration/handoff, then exit/GC. Require a model sequence for each behavior change. Do not weaken an invariant merely to pass a live workload.

## Blocked stop condition

Apply the blocked condition only after the same runtime-only obstacle has repeated three goal turns.

## Rollback and staging

Keep invariant declarations/harness, each transition-family extraction, validator integration, and source-test removal in separate stages. If an extraction changes behavior without a justified regression test, revert that stage while retaining the harness and prior verified helpers. Validators must be independently disableable if diagnostic overhead obscures runtime comparison.

## Completion record (2026-08-12)

- **Status:** Completed.
- **Goal activation:** The `/goal` string above was loaded verbatim and then extended with the requested statement that WOS can be run and debugged live without root access by appending `--no-setup` to `wos-ktest` and `wos-cluster`.
- **Transition layer:** runnable, wait, dead, current, and handoff membership mutations now pass through explicit lock-held primitives. Boot current publication, idle restoration/initial publication, heap corruption repair, the assembly-delimited handoff commit, corrupted-task leak handling, and selftest corruption injection are named exceptions.
- **Invariants and diagnostics:** the diagnostic-only validator checks queue/tag uniqueness, heap indices and EEVDF sums, owner CPU, publication, current/handoff/idle state, wait/dead list counts, dead epoch/refcount/GC eligibility, and list conflicts. It performs bounded scans without allocation, blocking, yielding, logging, VFS, network, or WKI work while locked and records a fixed trivially copyable first-failure snapshot.
- **Executable model:** the shared freestanding fixed-storage model supplies two fake CPUs, explicit time and epoch advancement, timers, deferred preemption, wake IPIs, task ownership/membership/publication, pin/domain policy, handoff reservation, exit, and GC. It validates every step, retains bounded replay data, runs deterministic bounded exploration, and gives every declared invariant a negative witness.
- **Compatibility:** EEVDF weights, vruntime/deadline calculations, placement and wake policy, affinity/domains, WKI behavior, timer/accounting counters, lock order, task and syscall/wire ABI, assembly offsets, TSS/GS/CR3/FPU state, and context-switch assembly were not changed. Normal builds keep transition validation disabled.

### Verification evidence

- Normal `Build WOS`: passed from the final source.
- Formatting and static checks: `scripts/dev/format_repo.sh --check` passed for every touched C++ file, including clang-tidy for `scheduler.cpp`; Python compilation and `git diff --check` passed.
- Full host suite: 155/155 passed. This includes the compiled scheduler-transition model, scheduler/context/preemption/migration tests, poll, futex, waitpid, signal, TLB-shootdown, WKI placement, manifest audit, and six fuzz smoke tests.
- Isolated diagnostic build: `bin/wos-ktest --build-only` passed with KCFI, report-mode KUBSan, KASan, KCOV, selftests, tracing/provenance diagnostics, and `WOS_SCHED_TRANSITION_VALIDATION=ON`.
- Live rootless KTEST: the default `wos-lan-KT`/`wos-wki-KT` TAPs were absent, so a temporary node spec reused the already provisioned node-0 TAPs while retaining the isolated `build-ktest/` and `ktest-data/` paths. `bin/wos-ktest --no-setup` then booted a 32-vCPU VM and passed 6,084 assertions with 0 failures. All 13 new `SchedulerTransitionModel/*` and `SchedulerTransitionValidator/*` cases passed, and the VM powered off normally.
- Live rootless debugging: `bin/wos-ktest --no-build --no-package --no-setup --debug-node` launched the VM paused with its GDB stub on `127.0.0.1:1234`. GDB connected, read live RIP at reset, loaded the diagnostic kernel symbols, and installed a hardware breakpoint resolving to `scheduler.cpp` before the VM completed its diagnostic boot.
- Live rootless SMP self-host: `bin/wos-cluster --config configs/cluster_selfhost.json --launch --no-setup` booted the normal image with 32 vCPUs. A 64-thread futex/exit workload completed, and the native scheduler benchmark used 31–32/32 CPUs while completing spawn, context-switch, parallel-efficiency, memory-bandwidth, and timer phases.
- Scheduler trace/counters: a scheduler-only `perf record` captured 761,863 event bytes during the native benchmark. `perf sched` decoded `WKE`, `SLP`, and `CTX` transitions with CPU, task, wait-site, runtime, and lag fields; `perf cpustat` reported nonzero context-switch, preemption, sleep/wake, timer, wake-IPI, load-balance, and GC counters. No panic, fatal, scheduler-corruption, transition-violation, or failed-assertion signature appeared in the VM serial log, and the cluster stopped cleanly.
- Final mutation audit: production direct writes are confined to the transition primitives and the named exceptions above; remaining raw membership writes are selftest setup. Source-shape transition assertions were replaced by executable host/KTEST coverage, leaving only architectural ordering checks plus explicitly labelled pre-existing out-of-goal contracts.

The baseline evidence above was stale in four material details: `scheduler.cpp` was 8,439 lines rather than 8,914, `scheduler_ktest.cpp` was 429 lines rather than 219, `scheduler_source_test.py` was 3,018 lines rather than 2,357, and work stealing uses a local lock plus nonblocking victim `try_with_lock` rather than blocking two-runqueue acquisition in ascending order. Current/handoff tasks may also legitimately overlay the same CPU's runnable heap; the forbidden state is incompatible stable-list membership or reservation on different CPUs. KTEST parks secondary CPUs, so the deterministic two-CPU model supplies chosen cross-CPU interleavings while the separate live self-host run supplies SMP evidence. No physical-hardware run was performed, and no remaining completion blocker is known.
