# Goal 31 wall-clock patch contract

Problem: RTC reads accept inconsistent/invalid dates and NTP corrections wrap
or step without a bounded discipline. The NTP daemon busy-waits for five seconds.

Expected files to change: platform/rtc/rtc.{cpp,hpp}, new rtc/wall_clock.hpp,
platform/ntp/ntp.cpp, host/KTEST wall-clock tests and host test registration.
Files read: these files, platform/ktime/arithmetic.hpp, tsc.hpp, scheduler sleep
helpers, sys/spinlock.{cpp,hpp}, all get_epoch_ns/adjust_offset_ns callsites,
time syscall, and the goal31 reconnaissance/instruction set.

Invariants: monotonic time and all monotonic deadlines are untouched; first
accepted NTP correction may step by at most 24 hours, later corrections slew
at at most 500 ppm. Invalid dates and unstable RTC snapshots produce observable
nonfatal fallback. No floating point, wraparound, allocation or callbacks under
the clock-state lock. NTP errors leave the last valid state intact.

Locks: new RTC state ticket lock with IRQ-save, held for a fixed-size snapshot
or arithmetic update. No other locks, logging or network work under it. All
current realtime callers are boot/task/VFS/syscall contexts, not NMI/panic.
Monotonic reads retain their separate lock-free contract. Writers cannot be
preempted while holding the lock; readers have bounded lock-held work.

Unsafe contexts: do not introduce RTC reads in NMI or recursive logging. Boot
initialization remains before AP startup; state uses constant initialization.
Allocation/blocking: no allocation in RTC; NTP delays use daemon park helpers.
ABI/wire impact: no wire or syscall layout change; internal adjustment reports
accept/reject status. Procfs diagnostics expose validity, pending slew,
steps, adjustments and rejected corrections.
Tests: calendar boundaries, signed extremes, initial-step bound, positive and
negative slew at fake times, adjustment replacement/continuity, host sanitizers,
KTEST, Normal Build WOS and live ABI clock probe. NTP reachability is measured
separately and not inferred from model tests.
Rollback: revert wall-clock stage as a unit, retaining checked arithmetic.

Review correction: negative slew must be applied to elapsed monotonic time
before adding the epoch anchor. Saturating the epoch sum first and then
subtracting an increasing slew made realtime regress near UINT64_MAX. The
corrected ordering preserves saturation and has host/KTEST boundary coverage.
