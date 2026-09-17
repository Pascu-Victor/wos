# Goal 31 interval timer model contract

Problem: provide one bounded, overflow-safe state machine for process REAL,
VIRTUAL and PROF timers before wiring process ownership and signal delivery.

Expected files: platform/ktime/interval_timer_model.hpp, host model tests,
and isolated KTEST model tests. No scheduler, Task, resource, or syscall edits.

Files read: resource accounting/owner/admission implementation, Task shared signal
claim helpers, scheduler park/accounting paths, process/thread exit and worker exec,
TimerQueue token ownership, clock arithmetic helpers, host/KTEST conventions.

Invariants: a noncopyable model belongs to exactly one retained process owner;
REAL uses monotonic elapsed microseconds, VIRTUAL cumulative process user microseconds,
PROF cumulative process total microseconds. Configure/disarm invalidates older tokens;
expiry produces at most one pending standard signal per timer. Phase-aligned periodic
rearm takes constant work even after many missed periods. UINT64_MAX deadlines expire
once and disarm. Generation or expiry-sequence exhaustion fails replacement without mutation; cancel/close
still work and never reuse a version. Fork constructs a new model; exec preserves it;
thread retirement preserves it; process retirement closes it permanently.

Locks: caller serializes every method using the owning process's IRQ-safe timer/resource
lock. The model itself takes no locks and performs no atomics, allocation, callbacks,
usercopy, registry lookup, or blocking. Returned tokens retain no object lifetime: caller
must retain the owner until all tokens have been consumed/discarded. Token identity uses
the exact model address, configuration generation and expiry sequence; copying/moving the model is disabled.

Unsafe contexts: safe for fixed CPU accounting state under runqueue/resource locks;
signal selection/wake and callback dispatch are external and must occur outside those locks.

ABI impact: none. get/set adapters convert validated timeval fields and preserve operation
selectors; the model can atomically capture old state within the caller's lock.

Tests: host executable checks all three domains, simultaneous expiry, coalescing,
replacement/cancel/close token races, wrong-owner claims, generation exhaustion,
periodic phase, saturation, and exhaustive small-domain periodic arithmetic. KTEST
runs representative same production-model transitions. Primary owns full build/runtime.

Rollback: remove unused model/tests. No live timer state or ABI is changed in this patch.

## Process service implementation contract

Problem: wire the tested process model into REAL events, existing CPU accounting,
and get/setitimer while eliminating task-private timer semantics.

Expected changes owned by this workstream: new process_timer.hpp/.cpp; resource.cpp
CPU charge hook; time syscall adapters and appended atomic setitimer ABI; mirrored
mlibc wrapper; model disarm/adapter tests. Primary owns Task fields/shared signal
helpers, service startup, exit close, remote placement gates. Timer workstream owns
removal of legacy scheduler timer branches after integration is ready.

Lock order: runqueue -> process timer service lock -> resource_lock. CPU accounting
updates model and publishes timer_sig_pending under resource_lock, releases it, then
queues deferred recipient wake under the service lock. No path holds resource_lock
while acquiring service lock. No signal wake, registry lookup, allocation, usercopy,
or blocking occurs under service/resource locks.

Lifetime: synchronous TimerQueue stores one retained process ref per armed REAL node;
the intrusive wake queue stores one retained ref per entry. Worker work transfers or
acquires an independent ref before releasing service_lock. interval_work_refs counts
these refs, including in-flight work, for remote placement admission. Cancellation and
close remove both queue entries in O(log timers)/O(1), close model state and clear shared
pending bits, then drop refs outside locks. Already extracted worker refs remain valid
until its bounded work finishes. Primary's shared signal claims give one owner to delivery.

REAL work runs in a dedicated daemon. Each pass processes a bounded batch, then requests
a 1 ms sleep before checking deadlines; IRQ accounting only updates fixed CPU state and
queues deferred wakes. The timer service exposes generation/closed-state checks, rejects
setters during committed process exit or exec admission, and preserves pending standard
signals when timers are disarmed/replaced. Fork starts empty; local exec preserves state;
main pthread_exit and ordinary worker exit preserve process timers. Remote exec refuses
armed/pending/in-flight timer state until a separate transfer protocol exists.

ABI: retain operations 4/5 for existing binaries. Append SETITIMER_V1 with a fixed-width
request and explicit version, atomically snapshot previous value under the same resource
lock as replacement. All usercopy occurs in the caller's syscall context outside locks.
Internal cancel/close remain available at generation exhaustion. Syscall replacement, including set-to-zero, reports EOVERFLOW once the model version is exhausted; failed replacements preserve state.

Validation: model host/KTEST suites; target syntax checks of every changed translation
unit; existing time/strace source checks; primary Normal Build WOS and live REAL/VIRTUAL/
PROF, shared-thread, fork/exec/exit and masked-recipient cases. Rollback reverts service and
its callers/lifecycle hooks together; do not leave mixed legacy/new timer ownership active.

## Integration validation and runtime fixture

The new service, resource hook, time syscall adapter, KTEST service translation unit,
strace decoder, and mlibc time/sysdep translation units passed targeted compiler syntax
checks. time_abi_math_test, interval_timer_model_test, and wos_abi_parity_test passed
through the configured host CTest build. Time, strace, usercopy and syscall-manifest
guards pass. The actual-service KTEST calls production dispatch synchronously with an
explicit observation time: KTEST runs before daemon/SMP startup and must not park waiting
for the worker. It covers canonical-thread setters, resource CPU charging, real expiry,
retained masked-signal retry, repeated cancel, exec setter admission, and extracted-ref close.

`service_interval_timers.c` / `.py` provide 85 live assertions including all timer kinds,
atomic old snapshots, malformed values, shared-thread setters, masked-owner recipient
selection, SIG_IGN transitions, fork disarm, exec preservation, main pthread_exit survival,
and 32 child lifetimes that arm/cancel repeatedly then exit with armed periodic timers.
The fixture owns a separate 120-second watchdog child; the SSH command is bounded at
180 seconds. The runner preserves before/after clock diagnostics and removes the uploaded
helper. It operates only on an already running VM.

Host smoke completed 83 of 85 checks; the two strict ignored-while-blocked expiry checks
differ from observed Linux behavior. This is harness validation, not WOS runtime evidence.
The target fixture compiled/linked with the WOS sysroot and requires live WOS execution.
CPU timer observations use the existing scheduler/syscall accounting boundaries; they do
not retrospectively reclassify an executing slice or change CPU-accounting resolution.

Wake selection is retried with at most the worker budget each millisecond until shared
pending state is consumed/cleared/closed. Changing a receiver's mask or retiring it after
selection therefore cannot permanently strand a process signal. SIG_IGN is checked under
resource ownership at expiry; changing disposition to ignore clears already pending bits.
Close clears pending bits but preserves borrowed claim reservations until their holders
release them. All-masked pending state can retain one queued owner reference until exit
or consumption, and remote exec admission explicitly observes this work.
