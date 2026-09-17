# Goal 31 process interval timer integration contract

Problem: legacy REAL timers belong to individual tasks and CPU interval timers
are unsupported. A blocked leader can strand a process-directed timer signal.

Expected files: Task signal/state fields, time interval service, CPU accounting,
time syscall/libc adapters, scheduler legacy expiry removal, process exit/remote
exec admission, SMP worker startup, and model/kernel/runtime tests.

Files already read: Task and SignalPendingClaim, resource owner/accounting and
ExecAdmission, process exit and thread retirement, remote exec preparation,
SMP daemon startup, scheduler timer index, interval model and syscall adapters.

Invariants: exactly one timer set per canonical retained process owner; fork
starts disarmed; local exec and main-thread retirement preserve timers; process
exit closes timers. Pending timer signals use a shared atomic pool with existing
claim/commit semantics and per-recipient masks. Expiry never builds signal frames
or invokes scheduler wake operations while resource/service locks are held.

Locks: runqueue -> service -> resource where nesting is necessary. CPU charging
publishes pending bits under resource_lock, then schedules deferred wake work
after releasing it. No reverse resource -> service nesting. Timer nodes/work
items retain owner lifetime until removed and all references release outside locks.

Unsafe contexts: accounting may run in IRQ/scheduler context; fixed three-domain
math only, no allocation or blocking. Daemon dispatch is task context. Signal
claims remain at existing safe return boundaries. Worker startup may allocate.

ABI impact: preserve existing selectors/layouts, add versioned atomic set/old
operation. Remote exec has no interval-state wire format: reject an armed or
pending timer transfer explicitly while exec admission prevents new arming.
Local exec remains available; never silently drop or duplicate timer state.

Build/test plan: model cancellation/phase tests, real owner/shared claim KTEST,
service CPU/REAL lifecycle KTEST, ABI parity and source guards, Normal Build WOS,
isolated live KTEST, optimized process/thread timer and sleep fixtures.

Rollback: revert service, lifecycle, shared signal pool and ABI integration as a
unit, restoring legacy REAL expiry. Keep the independently verified clock and
scheduler deadline index foundation.

## Ignored timer signal correction contract

Problem: a masked timer signal pending before SIG_IGN, or expiring while ignored,
could survive until a handler was restored and then be delivered incorrectly.
Expected changes: process.cpp sigaction publication/discard, process_timer.cpp
expiry publication (service owner), and service_interval_timers.c regression cases.
Read: Task handler/pending/claim helpers, SignalPendingClaim, sigaction,
process timer publication, resource admission, signal delivery and exit hooks.
Invariants: timer arming/periodic phase is unchanged; discard only shared timer
pending bits, never forcibly clear an in-flight claim reservation; no new locks
under usercopy, no new timer callbacks, no changes to local ordinary signal pools.
Lock order: timer service takes service_lock -> resource_lock -> handler store
lock for disposition sampling. Handler store lock methods contain only copies
and unlock before returning, never acquire resource_lock. sigaction keeps its
existing action mutex, publishes under handler store lock, releases that lock,
then acquires resource_lock to discard. The mutex excludes later handler restore
until discard finishes; resource_lock orders discard after any old-disposition
expiry publication. This introduces no handler-lock -> resource-lock nesting.
Unsafe contexts: expiry may publish during IRQ CPU accounting; sigaction is
syscall context and its resource-lock section is atomic-only, bounded work.
Allocation/blocking: no allocation or sleeping under either spinlock.
ABI/wire impact: none. Rollback: revert both publication/discard sides together.
Validation: strict fixture syntax/target link checks, live masked ignore/restore
regressions in the final VM, plus primary Normal Build WOS and diagnostic suite.
