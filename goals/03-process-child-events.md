# Goal: Replace Scheduler-Owned Waitpid Repair with Process Child Events

## Planning metadata

- **Expected scale:** 1–2 continuous weeks of agent time with subagents active. Evidence, not duration, determines completion.
- **Primary integration agent:** owns the lifecycle state machine, lock ordering, publication/reaping transaction, scheduler seam, and final POSIX/WOS semantics.
- **Three bounded subagent workstreams:** (1) parent/child registry and fork/spawn publication; (2) wait selectors, exit/ptrace/job-control event production and consumption; (3) GC, signal/WKI integration, diagnostics, deterministic tests, and userspace stress tooling.
- **Critical surfaces:** scheduler task state, process exit/group exit, waitpid ABI, signals/job control, ptrace, WKI remote process completion, epoch GC, task lifetime references, and usercopy effect ordering.
- **Completion rule:** no periodic repair or global task scan may be needed for correctness; time spent or passing only predicate tests does not complete the goal.

## Copy-pastable goal command

```text
/goal Outcome: The process subsystem owns explicit parent/child membership and a lossless child-event queue for exit, ptrace stop, and job-control stop; waitpid atomically consumes matching events, while the scheduler only performs generic park/wake and contains no waitpid completion, global scan, lease timer, or orphan-repair logic. | Verification surface: Normal Build WOS, focused host waitpid/process/signal/ptrace/WKI tests, isolated KTEST lifecycle interleavings, and user-run SMP plus remote-process fork/exit/wait evidence. | Constraints: Preserve waitpid selectors/options, status and rusage encoding, SIGCHLD, ptrace/job-control behavior, child CPU accounting, group exit, epoch safety, usercopy-before-consume ordering, and no blocking under runqueue or lifecycle spinlocks. | Boundaries: Change child lifecycle and the scheduler wait seam only; do not retune scheduling policy, redesign signals/ptrace/WKI protocols, renumber ABI operations, or clean up unrelated process code. | Iteration policy: Add the authoritative registry/events alongside current behavior, prove equivalence with deterministic tests, migrate producers and consumers one class at a time, then remove repair paths only after lost-wake coverage passes. | Blocked stop condition: Stop and mark blocked only after the same indispensable SMP or cluster-only blocker repeats for three consecutive goal turns, all safe model/KTEST alternatives are exhausted, and the exact user-run trace or backtrace request is recorded. | Live execution note: I am able to run and debug WOS live without root access by appending --no-setup to the wos-ktest and wos-cluster scripts.
```

## Local source evidence

`modules/kern/src/platform/sched/scheduler.cpp` contains roughly 294 `waitpid`/`WAITPID` references and includes process `waitpid.hpp` and `exit.hpp` directly. Correctness currently depends on `ORPHANED_WAITPID_SCAN_BATCH`, `ORPHANED_WAITPID_SCAN_INTERVAL_US`, `WAITPID_REPAIR_FALLBACK_MIN_US`, `recover_stalled_waitpid_completion_claim`, `repair_one_orphaned_waitpid_from_registry`, and timer-driven scans inside `process_tasks`.

`modules/kern/src/syscalls_impl/process/waitpid.cpp` and scheduler completion helpers search global active/dead task registries through `find_active_task_lifetime_ref_if`, `find_dead_task_lifetime_ref_if`, and related scans. `modules/kern/src/platform/sched/task.hpp::Task` distributes lifecycle state across `waiting_for_pid`, wait output/resume addresses, `waitpid_last_repair_us`, `waitpid_completion_claimed`, `waitpid_publish_pending`, `awaitee_on_exit`, `exit_notify_ready`, `waited_on`, and zombie-reclaim fields. `modules/kern/src/syscalls_impl/process/exit.cpp` separately drains exit waiters and publishes group-exit state. Existing KTESTs prove isolated claims and repair predicates, but the fallback scans show that no single object owns event publication, park, wake, and consumption.

## Measurable completion contract

1. Every process has authoritative child membership with explicit ownership/lifetime rules established transactionally during fork/spawn publication.
2. Exit, ptrace stop, continued/job-control stop, and relevant remote-proxy completion publish immutable child events exactly once and wake eligible parents generically.
3. `waitpid` handles positive PID, `-1`, zero, and negative process-group selectors plus supported options without scanning the global active or dead task registries.
4. Event claiming is single-winner. Output pointers are validated/copied before irreversible event consumption, and a copy failure preserves a safely retryable event.
5. Parent death/reparenting, process-group changes, thread-group exit, ptrace ownership, and WKI proxy tasks have explicit event transfer/discard rules.
6. Scheduler code no longer contains child matching, wait-status/rusage writes, completion-claim leases, orphan scans, or waitpid repair deadlines. A typed generic wait-channel label may remain for diagnostics only.
7. Zombie heavy resources and final task objects are reclaimed from explicit lifecycle states without premature pagemap/task release.
8. Deterministic tests enumerate publish-before-park, park-before-publish, simultaneous waiters, signal interruption, copy failure, child stop/exit races, parent death, and GC interleavings.

## Invariants, locks, unsafe contexts, and ABI

Define one lock order covering the child registry/event lock, global task registry, `exit_waiters_lock` during migration, ptrace state, and per-CPU runqueue locks. No usercopy, address-space destruction, VFS cleanup, logging allocation, or scheduler blocking occurs while a lifecycle spinlock is held. Events retain the child/task data needed for status and rusage until consumption; epoch reclamation cannot free referenced objects. A child status is consumed once, and a task is never simultaneously current, handoff-reserved, runnable, waiting, or dead on incompatible queues.

Preserve `abi/callnums/process.h`, libc `waitpid` expectations, negative errno values, status encoding, rusage layout, `WNOHANG`/stop semantics, SIGCHLD, ptrace stops, child time accumulation, and WKI-visible process identity. Scheduler interrupt paths stay allocation-free.

## Boundaries

In scope are process child registries/events, fork/spawn/exit/wait integration, the generic scheduler wake seam, GC/lifetime integration, and focused tests. Out of scope are EEVDF or CPU-domain changes, a general signal rewrite, new process APIs, WKI wire changes, exec-loader redesign, or unrelated Task cleanup.

## Parallel workstreams

Subagent 1 builds child membership and transactional publication with fork/spawn tests. Subagent 2 builds event matching/consumption for waitpid, exit, ptrace, and job-control semantics. Subagent 3 integrates parent death, group exit, GC, signals, WKI proxies, and stress diagnostics. The primary agent owns shared `Task`/process types, lock-order review, scheduler removal, and staged equivalence decisions.

## Verification plan

- Run the Normal `Build WOS` task.
- Run focused host tests including `waitpid_source_test.py`, process/exec/signal/ptrace/usercopy/shutdown tests, and relevant WKI remote-compute tests.
- Run isolated `bin/wos-ktest` process, task, scheduler, signal, ptrace, and new child-event interleaving tests.
- Per repository rules, ask the user to run SMP fork/exec/exit storms, selector/job-control/ptrace cases, parent-death cases, and WKI remote-process waits, then provide logs/backtraces. Do not infer runtime success from a build.

## Iteration policy

Introduce events in observation/dual-check mode, migrate exit events, then stop events, selectors, parent death, and remote proxies. Remove one repair mechanism only after its replacement has deterministic lost-wake coverage. Continue diagnosing from source and model tests when runtime data is absent.

## Blocked stop condition

Apply the three-turn blocked condition from the `/goal` command exactly.

## Rollback and staging

Stage data structures, producers, consumers, scheduler simplification, and cleanup separately. During migration, retain a diagnostic-only mismatch counter rather than silently choosing conflicting results. If a producer class regresses, revert that migration while retaining the verified event core; restore repair logic only as a temporary rollback, never as the completed state.

## Completion record (2026-08-12)

- **Status:** Completed.
- **Live execution:** WOS was built, launched, and debugged rootlessly with `bin/wos-ktest --no-build --no-setup` and `bin/wos-cluster --launch --no-setup`, as recorded in the appended goal string.
- **Implementation:** `Task` now embeds transactional parent/child and tracer topology, immutable exit/ptrace/job-control/continued event nodes, waiter registrations, and claim state guarded by one IRQ-save lifecycle spinlock. Fork, exec-created processes, and WKI proxy children publish membership before scheduler visibility. Exit, signal/job-control, ptrace, reparenting, and WKI completion use the same event authority.
- **Consumption and lifetime:** `waitpid` translates every supported selector without a global active/dead-task scan, claims one matching event, performs status and full 144-byte rusage usercopy before consumption, and releases a failed claim for retry. Parent and tracer exit audiences retire independently; child accounting and waitable-zombie references survive until the final observer consumes or discards its event.
- **Scheduler boundary:** scheduler waitpid completion, output writes, repair leases/deadlines, orphan scans, and completion claims were removed. The scheduler retains only generic park/wake diagnostics and explicit lifecycle-state-based zombie resource reclamation.
- **Compatibility:** waitpid selector/option values, status encoding, SIGCHLD and stop behavior, child CPU accounting, syscall numbers, ptrace ABI, and WKI wire formats remain unchanged. Wakes, signals, logging, usercopy, allocation, blocking, and address-space destruction occur outside the lifecycle spinlock.

### Verification evidence

- Normal `Build WOS`: passed from the final formatted source.
- Repository formatting and clang-tidy gate on all 28 touched C++ files: passed; the gate reported non-fatal warnings but no check failure.
- Full host suite: 154/154 passed, including waitpid, scheduler, signal, usercopy, exec, shutdown, ptrace-adjacent, and WKI remote-compute source checks.
- Isolated diagnostic build: passed with KCFI, report-mode KUBSan, KASan, KCOV, selftests, network tracing, and allocation provenance enabled.
- Live 32-vCPU isolated KTEST VM: 6,034 passed and 0 failed. All 10 `ChildEvents/*` tests passed, covering publication ordering, copy retry/single winner, waiter handoff, selector invalidation, publish races, dual parent/tracer observers, stop/continued backpressure, reparenting, and parent death.
- Live four-node, 8-vCPU-per-node WKI cluster: `testd` passed 110/110, including fork/exit, specific/any-child waitpid lost-wake races, multi-child drains, and remote-process/proxy IPC cases. No kernel panic, page-fault, protection-fault, sanitizer, assertion, or QEMU guest-error signature appeared in any node log; the cluster shut down cleanly.
- Final source audit: `git diff --check` passed; waitpid contains no active/dead task-registry scan, scheduler contains none of the removed waitpid completion/repair mechanisms, and syscall/ptrace/WKI ABI files were not changed.

The earlier **Local source evidence**, **Verification plan**, and staged migration text above are the pre-implementation baseline retained for provenance; this completion record and the current local source supersede their descriptions of then-current behavior. No physical-hardware run was performed, and no remaining completion blocker is known.
