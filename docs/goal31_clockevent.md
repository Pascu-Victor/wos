# Local APIC clockevent contract

Problem: per-CPU startup overwrites global scheduler calibration variables, while idle CPUs lazily update another shared calibration under different runqueue locks. Idle delay conversion also rounds down and calibrates for 10 ms inside scheduler locking.

Expected files: context_switch.cpp/.hpp, scheduler.cpp, a pure clockevent conversion header, and host/KTEST arithmetic checks.

Files read: APIC calibration/programming, context-switch timer IRQ/start/reschedule paths, scheduler startup/idle arming and CPU-local storage conventions.

Invariants: keep the 4 ms default/configured scheduler quantum and idle 10 ms probe cap; no task/signal/accounting policy change; no shared mutable calibration across CPUs; no early expiry from conversion truncation; APIC counts remain in [1, UINT32_MAX].

Locks/context: local calibration occurs once during scheduler startup with interrupts masked, before the initial no-task idle loop. Calibration explicitly leaves the APIC disarmed until idle/task context is published. Immutable calibration is release-published per CPU for diagnostic reads. IRQ/idle consumers already mask interrupts; the reschedule fallback masks interrupts across local calibration lookup and APIC programming. No calibration or blocking remains in idle timer conversion.

Allocation/blocking: fixed CPU slots use the existing MAX_CPUS bound; no allocation. Startup calibration waits on HPET for a fixed 4 ms sample, independent of requested quantum. Other operations use integer arithmetic only. Calls before initialization use a one-tick programming fallback and expose uninitialized metadata.

ABI: no syscall/wire changes. Query metadata reports local calibration, configured quantum, resolution, minimum/maximum delay and initialization state for procfs integration.

Validation: pure conversion tests cover round-up, overflow, zero/missing calibration and count clamping; scheduler source regression; Normal Build WOS and fresh isolated KTEST/runtime latency comparison owned by the integrating agent.

Rollback: revert clockevent changes together; retain previously validated timer queue and arithmetic fixes.

Four host tests pass with ASan/UBSan, including 729 edge-value conversion comparisons against a full-width integer reference. The context-switch, scheduler (with WOS_SELFTEST and transition validation), and clockevent KTEST translation units pass target-compiler syntax checks; scheduler source regression also passes. Full build and refreshed live measurements are performed by the integrating agent.
