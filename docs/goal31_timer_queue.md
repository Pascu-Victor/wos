# Timer queue patch contract

Problem: the original ktime callback registration was unused and had no deadline or cancellation semantics. The foundation below now also backs the scheduler's per-CPU timed-wait index through synchronous extraction.

Expected files to change: `platform/ktime/timer_queue.hpp`, host queue tests, and `test/timer_queue_ktest.cpp`.

Files already read: ktime, scheduler wait transitions and timer IRQ, task lifetime methods, run_heap, PerCpuCrossAccess, time syscalls, kernel CMake and kernel/critical-path/logging instructions.

Invariants to preserve: no allocation, callbacks, blocking, or logging during queue operations; no timer loss from fixed storage capacity; no generation wrap; one claimant per expiry; cancelled extracted generations cannot execute; running callbacks retain their lifetime and suppress periodic rearm when cancelled.

Locks involved: caller supplies one IRQ-safe lock covering all queue/node operations, including claim and finish. The core takes no locks. Callback execution occurs in task context outside that lock. Scheduler integration must specify its lock ordering separately.

Unsafe contexts: arm, cancel and bounded extraction may run in IRQ context under the caller lock. Claim/finish only manipulate queue state. Arbitrary callbacks must never run in IRQ context.

Allocation/blocking assumptions: intrusive AVL nodes are provided by owners; operations cost O(log n), peek is O(1), extraction is bounded by the caller's output span. Tree recursion is bounded by AVL height. The deferred transport is caller-owned and preallocated. Node and callback context lifetime must remain retained until every extracted token is consumed, including stale tokens; cancellation alone does not establish reclamation safety.

ABI/wire/syscall compatibility impact: none; new internal service core only.

Build/test plan: fake-clock ordering/storm/randomized model tests, cancellation and rearm interleavings, generation exhaustion, periodic overflow, and KTEST bounded storm/cancellation checks; normal kernel build during integration.

Rollback plan: remove new core and its tests before integration; existing scheduler timer paths are untouched.

## State and dispatch contract

Each node transitions `idle -> armed -> ready -> running -> idle/armed`. Extracted tokens contain the node's generation. Arm invalidates a previous ready token; cancellation of armed/ready state returns `cancelled`. Cancellation of running state returns `running` and prevents periodic rearm, but cannot stop the callback. A worker claims under the queue lock, invokes the captured callback outside it, then finishes under the same lock. A failed claim still requires releasing the token's owner lifetime reference. Consumers must not copy a token into multiple independently reclaiming work items.

Periodic deadlines preserve their original phase and skip missed periods arithmetically. Saturated deadlines are allowed once; a periodic timer disarms when no strictly future deadline is representable. Generations never wrap: arm fails permanently for a node whose generation reached UINT64_MAX. A deferred node remains associated with its original queue so stale tokens cannot race cross-queue locks; queue lifetime must enclose every node association and outstanding token. Synchronous queues cannot extract deferred tokens, and pop/cancel releases association under the owning lock so scheduler migration can reuse the node safely.

The scheduler now replaces timeout scanning with at most 16 synchronous due-root extractions per IRQ. The generic deferred interface still requires a task-context worker supplied by its consumer; it does not establish end-to-end expiry latency by itself.

## Verification performed

The eight host tests pass using the production header with AddressSanitizer and UndefinedBehaviorSanitizer: 10,000 simultaneous deadlines with extraction batches of 16, 100,000 randomized mutations against an ordered reference, stale-token cancellation/rearm, callback ownership under two host threads and a caller mutex, periodic phase, terminal deadline saturation, and synchronous cross-queue transfer. Leak detection was disabled because this execution environment runs under ptrace, which LeakSanitizer does not support. The scheduler source regression passes unchanged. Queue and scheduler-index KTEST cases passed in the baseline live diagnostic run; final integration evidence is recorded in `goal31_timekeeping.md`. Generation exhaustion is guarded in source but has not been exercised by advancing a node through UINT64_MAX generations.

## Scheduler integration requirements

The scheduler's original wait-list detach was O(n); it now provides constant-time unlink with an exact list-owner check. Shared dead-list and GC cursor behavior remain intact. After process timer service integration, the wait-index key is solely `wake_at_us`; REAL interval timers belong to the process service. The index is synchronized at attach, detach and migration, and when `publish_reserved_task_wakeup_locked` clears a deadline while the outgoing reserved task still has wait-list membership. Shutdown also clears deadlines before detach. Job-control and signal continue paths clear deadline fields outside these transition helpers and are checked at their subsequent scheduler ownership boundary.

Deferred cancellation does not drain extracted tokens. Scheduler integration therefore uses a separate synchronous mode with no outstanding tokens, retaining existing migration lock ordering. This leaves the deferred association rule intact.

`get_run_queue_stats()` now exposes timer queue depth, extraction count, extraction budget exhaustion, maximum indexed deadline lateness in microseconds, and the IRQ extraction budget of 16. Existing `wait_list_scan_*` diagnostics retain their names for readers but count inspected due roots rather than unrelated wait-list entries.

## Scheduler integration patch contract

Problem: timer IRQs scan arbitrarily many waiters and repeat linear removal/recomputation for each wake.

Expected files to change: timer_queue.hpp, task.hpp, scheduler.hpp/cpp, run_heap.hpp/cpp, timer host/KTEST tests and scheduler KTEST checks.

Files already read: all direct wake_at_us and itimer_real_expire_us mutation sites, scheduler transition helpers, reserved handoff wake, migration and shutdown paths, futex publication, job-control continue, signal continue, intrusive lists and GC list iteration.

Invariants to preserve: wait-list scheduler ownership, dead-list epoch reclamation, current/handoff wake tokens, futex cleanup outside runqueue locks, signal pending publication, EEVDF accounting, no arbitrary callbacks in IRQ, and no fixed-capacity dropped timers. Cancellation detaches the timed index before migration or reclamation.

Locks involved: same per-CPU IRQ-safe RunQueue lock protects the wait list and its deadline index; migration retains existing CPU-ordered lock acquisition. The exact intrusive list-owner pointer is atomic because ownership probes may address a different queue during migration. Links are accessed only under the proven owner lock.

Unsafe contexts: process_tasks IRQ; wait transitions during task/syscall/preemption paths. Index operations do not allocate, block, log or invoke callbacks. At most 16 due index entries are popped per IRQ. Existing bounded wake publication and post-lock futex/signal work remain unchanged.

Allocation/blocking assumptions: each Task embeds a timer node and list links. Each RunQueue embeds an AVL index. A synchronous queue mode forbids deferred-token extraction and releases node association on removal, so no token can escape the locked operation; this supplies the mechanical migration proof without weakening deferred queue association.

ABI/wire/syscall compatibility impact: internal Task/RunQueue layout changes only. Public deadline fields and time units remain microseconds.

Build/test plan: synchronous transfer/expiry host tests, existing scheduler source/model tests, queue host tests under sanitizers, kernel build and isolated KTEST. Verify reserved-handoff deadline cancellation and intrusive list middle/tail/foreign-owner removal.

Rollback plan: revert scheduler/list integration and retain the independently tested deferred queue foundation.

## Legacy interval expiry retirement contract

Problem: after the process timer service is installed, retaining per-task REAL expiry in the scheduler would duplicate signal generation and preserve obsolete per-thread ownership.

Expected files to change: scheduler.cpp and Task thread-construction resets in task.cpp; the primary agent removes legacy Task fields after service integration is ready.

Files read: process timer model, scheduler blocked/current expiry, APIC timer decisions, process exit and retired-main loop, resource CPU accounting and owner lifetime.

Invariants: preserve the bounded wait index for ordinary sleeps/futex timeouts, reserved-handoff wake tokens, fixed scheduler accounting ticks, and post-lock futex cleanup. The new service must own all REAL expiry before removing the old checks. Keep retired diagnostic enum/counter fields for reader compatibility.

Locks and unsafe contexts: removing interval generation from IRQ reduces work and introduces no locks. The service owns process timer locking and deferred signal dispatch; no callbacks or blocking are added to scheduler context.

ABI impact: no syscall selector changes in this removal. Process-owned interval semantics are supplied by the coordinated service.

Build/test plan: scheduler source regression, queue host tests, Normal Build WOS, and a fresh isolated KTEST including repeated fixture initialization and process timer integration.

Rollback: restore legacy scheduler and Task fields only together with reverting the new interval service, never enable both expiry owners.
