# Goal 31: High-Resolution Timekeeping and Timer Services

## Planning metadata

- **Target horizon:** one to two continuous weeks with clocksource, timer queue, ABI/libc, and qualification workstreams.
- **Critical surfaces:** TSC/RTC/NTP, SMP synchronization, scheduler timers, nanosleep/itimer/signals, timeout users, suspend/resume policy, libc ABI, and telemetry.
- **Completion rule:** clocks must meet explicit monotonicity/accuracy/expiry bounds under SMP, wall-clock adjustment, overflow, and load.

## Copy-pastable goal command

```text
/goal Outcome: Give WOS a coherent high-resolution clocksource/clockevent and timer service with monotonic, realtime, process/thread CPU clocks, absolute and relative sleeps, interval timers, bounded cancellation, disciplined wall-clock adjustment, and scalable expiry under SMP load. | Verification surface: arithmetic and timer-queue models, fake-clock deterministic tests, SMP monotonicity and cancellation races, ABI/libc conformance, Normal Build WOS, isolated KTEST timer storms, and user-run drift/NTP/load evidence with explicit thresholds. | Constraints: Preserve scheduler accounting, signal delivery boundaries, timeout semantics, no backward monotonic time, bounded IRQ work, no floating-point kernel dependency, overflow safety, and compatibility for existing time syscalls while extending them version-safely. | Boundaries: Refactor TSC/RTC/NTP/time syscall/timer-queue seams and direct timeout consumers; do not redesign the scheduler, networking protocols, or distributed global-time ordering. | Iteration policy: Establish integer clock arithmetic and fake-clock models, publish clocksource status, replace the timer queue, migrate sleeps/itimers/timeouts, add ABI/libc coverage, then tune only from measured drift and expiry latency. | Blocked stop condition: Stop only after the same indispensable runtime/platform blocker repeats for three goal turns, all safe fake-clock/model/KTEST alternatives are exhausted, and the missing timing trace or user action is explicit.
```

## Local source evidence

The time ABI currently exposes a small set of operations: gettimeofday, clock_gettime, nanosleep, times, setitimer, and getitimer. `sys_time_get()` rounds relative sleeps to microseconds, supports only `ITIMER_REAL`, and treats unrecognized clock IDs as monotonic rather than rejecting them. Realtime is RTC epoch plus an NTP offset; monotonic time uses TSC when available and otherwise tick microseconds. CPU-time aggregation scans active tasks.

`platform/ktime` provides a compact tick-scheduled callback interface. Many scheduler, network, WKI, I/O, and userspace wait paths independently compute microsecond deadlines, making overflow, cancellation, precision, and expiry-work bounds a cross-cutting seam.

## Measurable completion contract

1. A clocksource abstraction reports resolution, stability, frequency, conversion parameters, and fallback state with overflow-safe integer arithmetic.
2. Monotonic time never regresses across CPUs; TSC synchronization/drift and feature failures select an explicit fallback before use.
3. Realtime adjustment uses bounded step/slew policy and cannot perturb monotonic deadlines; RTC/NTP failures are observable and nonfatal.
4. A scalable timer queue supports absolute/relative deadlines, periodic rearm, cancellation, generation-safe callbacks, and bounded IRQ extraction with task-context deferred work.
5. Nanosleep reports remaining time on interruption and supports required clock/absolute variants through a versioned ABI/libc path.
6. Real, virtual, and profiling interval timers have documented process/thread accounting and signal semantics without timer duplication after fork/exec/exit.
7. Unknown clock IDs and malformed timespec/timeval values return correct errors; all conversions handle saturation and long uptimes.
8. Direct timeout consumers migrate to common deadline/cancellation helpers or document why their context requires a specialized timer.
9. Procfs/perf diagnostics report clocksource, drift/adjustment, queue depth, late expiry, and callback budget without distorting timing.
10. Deterministic and runtime evidence meets stated monotonicity, drift, expiry-latency, and timer-storm bounds.

## Invariants, locks, and unsafe contexts

Timer IRQ work is bounded, allocation-free, and cannot invoke blocking callbacks. Cancellation versus expiry has one owner and terminal outcome. Clock reads are lock-free or bounded and valid before full userspace. Signal delivery remains at valid user-return boundaries. No deadline arithmetic wraps into immediate or infinite waits accidentally.

## Boundaries and overlap

In scope are local-node clocks and timers plus direct ABI/timeout consumers. Cross-node synchronized ordering, PTP, scheduler policy, and a general async I/O API are out of scope. Coordinate scheduler timer changes with Goal 16 regression work and TCP timers with Goal 29.

## Parallel workstreams

The primary agent owns clock/timer contracts, ABI strategy, and integration. Subagent A builds arithmetic/clocksource/fake-clock tests. Subagent B implements the timer queue and cancellation. Subagent C handles syscall/libc/itimer semantics. Subagent D may migrate timeout consumers and diagnostics on disjoint files.

## Verification, iteration, and rollback

Run arithmetic/model tests, time/signal/scheduler host suites, Normal `Build WOS`, isolated KTEST timer storms, and user-run SMP drift/NTP/load scenarios. Retain the old tick scheduler behind an internal bridge until migrated callbacks pass equivalence. Never roll back by allowing monotonic regression or silently accepting unknown clocks. Use the `/goal` blocked condition exactly.

## Execution ledger

- Status: Done (2026-09-17).
- Live execution authorization: You are able to run and debug WOS live by appending --no-setup to the wos-ktest and wos-cluster scripts to not need root access.
- Reconnaissance and patch contracts: `docs/goal31_timekeeping.md`.
- Source correction: process CPU clocks already use shared resource-owner accounting; ktime callbacks are an unused stub and actual timer expiry lives in scheduler wait-list scans.

- Completion evidence: `docs/goal31_timekeeping.md` and `docs/goal31_live_time_qualification.md`. Final validation: 17 focused host suites; normal-profile modules and fresh private images; isolated diagnostic KTEST 60,549 passed / 0 failed; live sleep ABI and 85 interval-timer checks; one million SMP clock reads without regression; unloaded/storm p99 lateness 4.272/43.615 ms; 60-second drift [-288.142, +280.453] ppm.
- Preserved existing normal disks for final packaging; new private images were used after automatic review rejected replacement of the populated normal rootfs. Three preexisting broad host-guard failures remain documented separately.
