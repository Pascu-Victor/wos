# Goal 31 clocksource contract

Problem: the monotonic clock unconditionally used a single BSP TSC calibration,
accepted missing invariant-TSC support, and invented a frequency on failure.
HPET conversion overflowed at long uptimes and HPET capabilities were unchecked.

Expected files to change: platform/tsc/tsc.{cpp,hpp},
platform/acpi/hpet/hpet.{cpp,hpp}, platform/ktime/clocksource_policy.hpp,
tests/host/unit/clocksource_policy_test.cpp, and this document.
Files already read: the above existing source files, ktime.{cpp,hpp}, arithmetic.hpp,
asm/msr.hpp, smt.cpp/hpp early CPU count, APIC calibration, kernel CMake and
kernel/logging/critical-path/testing/subsystem instructions.
Invariants to preserve: boot-relative epoch, nondecreasing monotonic reads,
integer conversion, existing TSC raw-cycle conversion API, unchanged syscall ABI.
Locks involved: none added; shared monotonic floor uses lock-free 64-bit CAS.
Unsafe contexts: reads occur during boot, IRQ, scheduler, panic, and syscalls.
Allocation/blocking assumptions: reads allocate nothing and take no locks;
calibration busy-waits only during BSP initialization before secondary CPUs run.
ABI/wire/syscall compatibility impact: no layout or operation changes; clock
resolution now describes the selected source, and kernel status is additive.
Build/test plan: host policy/fake-source tests with concurrent readers;
parent runs Normal Build WOS and authorized live/KTEST checks.
Rollback plan: revert these source and policy changes together, preserving the
independently introduced overflow-safe arithmetic conversions.

## Source policy and bounds

HPET is the shared reference and keeps its original boot-reset epoch. Require a
64-bit counter and nonzero period no greater than 100,000,000 femtoseconds. A
32-bit HPET is rejected during boot because extending it safely requires a
periodic wrap observer that this kernel does not yet provide.

A TSC candidate requires CPUID TSC and invariant-TSC feature bits, three 10 ms
HPET-referenced samples in 1 MHz..20 GHz, and at most 1,000 ppm sample spread.
These are qualification thresholds, not a guarantee of physical clock accuracy.
Multi-CPU systems explicitly select HPET: invariant frequency does not prove
cross-CPU offset agreement. No TSC synchronization success is fabricated.
Single-CPU TSC reads are ordered and bracketed by HPET reads. Drift outside the
reference bracket by more than 100 us permanently selects HPET. A backwards TSC
relative to its origin also selects HPET. The common epoch and shared monotonic
floor prevent a source change or concurrent read from moving time backwards;
ahead-of-reference readings can cause at most a 100 us plateau at fallback.

Status exposes source/reason, frequency, integer conversion ratio, nominal
resolution, invariant feature, sample spread, and observed watchdog deviation.
Before initialization, reads return zero and status reports unavailable.
Constant-initialized atomics must not reset when global constructors run later.

This deliberately favors correctness over speed: SMP reads incur HPET MMIO and
a shared atomic update; UP TSC watchdog reads also incur HPET MMIO. Physical HPET
accuracy, suspend/resume recovery, and TSC cross-CPU qualification remain platform
limitations. Live suspend/resume is unsupported; no runtime HPET reset is allowed.

## Checks performed in this workstream

Three host GoogleTests passed under ASan/UBSan: feature/capability and frequency
qualification boundaries, preemption-aware drift brackets and fallback plateau,
and 400,000 concurrent candidate publications from four threads. These exercise
the same policy and atomic publication helpers used by the kernel; they do not
claim hardware calibration or live SMP performance results. Formatting checks
passed; the complete formatter/tidy wrapper completed successfully. Normal Build
WOS and live integration checks are owned by the primary integration workstream.
