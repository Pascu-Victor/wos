# Goal 31: timekeeping implementation ledger

## Authorization and scope

Goal 31 is implemented and qualified. Its original goal string is preserved in the goal,
followed by the user's authorization to run and debug WOS live using
`bin/wos-ktest --no-setup` and `bin/wos-cluster --launch --no-setup`.
This overrides older user-run-only guidance for this task. Existing topology
and KTEST artifact isolation still apply.

## Reconnaissance (2026-09-17)

- Clocks: `platform/ktime`, `platform/tsc`, `platform/acpi/hpet`,
  `platform/rtc`, `platform/ntp`. Boot initializes HPET, calibrates TSC, then
  reads RTC. Monotonic reads directly convert TSC; realtime adds RTC and an
  atomic signed NTP correction. HPET and TSC products can overflow.
- Timer ownership: `platform/sched/scheduler.cpp`, `task.hpp`, run queues,
  `platform/sys/context_switch.cpp`. IRQ expiry scans per-CPU wait lists under
  the runqueue lock. `ktime::push_task` is an unused stub, not the live timer
  queue. Keep task queue ownership/epoch reclamation intact during migration.
- ABI: `syscalls_impl/time/time.cpp`, `abi/callnums/time.h`, syscall dispatch,
  and mlibc WOS sysdeps. Operations 0–5 are existing compatibility surfaces.
  Nanosleep currently returns success before deferred sleep; it cannot report
  interruption accurately. Unknown clock IDs currently become monotonic.
- Accounting: CPU clocks now use `resource::copy_cpu_observation`, not active
  task scans as the original goal notes state. Preserve this owner authority.
- Diagnostics/test: procfs, existing time/signal/scheduler host suites, host
  arithmetic tests, isolated kernel selftests, and live rootless fixtures.

Read-side clock code runs in early boot, IRQ, scheduler, and syscall contexts:
no allocation, sleeping, or new lock nesting is acceptable. Clock conversion
must not acquire scheduler locks. RTC initialization is boot-only; adjustment
currently publishes an atomic correction. Timer cancellation must retain task
storage until ownership is resolved, with callbacks deferred out of IRQ work.

Minimal strategy: checked integer arithmetic first; explicit clocksource
qualification/fallback and diagnostics second; indexed scheduler deadlines and
generation-safe deferred callbacks next; restartable sleep and process-owned
interval timers with ABI/libc extensions; then measured runtime qualification.
Platform drift, SMP skew, NTP availability, and expiry latency remain unknown
until measured on the available VMs. Model tests are not runtime evidence.

## Patch contract: integer clock arithmetic

Problem: HPET tick conversion multiplies before dividing and wraps at ordinary
uptimes; TSC conversion/calibration and RTC epoch/negative adjustment arithmetic
also have unchecked overflow. Long waits can wrap during unit conversion.

Expected files to change: a shared `platform/ktime/arithmetic.hpp`, HPET, TSC,
RTC and ktime conversion callers; host and KTEST arithmetic cases; time source
guard expectations; this ledger and goal status.

Files already read: the clock/RTC/NTP/ktime sources and headers, time syscall,
asm/msr.hpp, kernel CMake, host CMake/time_source_test, workflow, subsystem
index, kernel/critical-path/logging/build/testing/libc instructions and diagnostic
flag map.

Invariants to preserve: integer-only freestanding operations; floor clock
conversion; round waits upward; saturate unrepresentable results; existing
clock origins, syscall layouts, CPU accounting, and signal boundaries.

Locks involved: none added; existing atomic RTC adjustment publication remains.
Unsafe contexts: early boot, IRQ, scheduler, and panic clock readers.
Allocation/blocking assumptions: arithmetic has neither; no runtime division
helper dependency may be introduced into the kernel.
ABI/wire/syscall compatibility impact: none.
Build/test plan: independently computed wide-integer oracle with boundary and
random samples, KTEST arithmetic, existing time/scheduler/signal suites,
scoped formatting, Normal Build WOS, isolated diagnostic build and live KTEST.
Rollback plan: revert this stage's conversion callsites and helper/tests as a
unit; do not change normal/KTEST state roots or scheduler ownership.

## Iterations

### Diagnostics contract

Add read-only `/proc/timekeeping` with selected clocksource status and RTC
validity/step/slew counters. Files: procfs.cpp/procfs.hpp plus time headers.
Read: procfs node classification, root enumeration, lazy content generation,
path opening and status APIs. Preserve existing formats/permissions; this
contains no addresses, credentials or secrets. Snapshot clock state before
formatting; no scheduler locks or mutable controls. Validate via build, existing
procfs authority guards, and live read. Rollback: remove the new node and row.

1. Integer arithmetic, clocksource qualification/fallback, RTC/NTP discipline,
   scheduler timer indexing and sleep ABI/libc stages implemented. Contracts
   and limitations are in the adjacent `goal31_*.md` documents.
2. Baseline Normal Build WOS, including image packaging, passed. The first
   packaging attempt failed at sandboxed libguestfs socket creation after
   recreating the generated rootfs disk. A read-only allocation map established
   that the new disk was empty; approved rebuilding repaired it successfully.
3. Baseline isolated diagnostic live KTEST completed **60,530 passed, 0 failed**
   and powered itself off. Evidence: `build/goal31-evidence/baseline/`.
   This precedes process timer integration and the repeat-fixture reset fix;
   final-source build/runtime validation is still required.
4. Focused arithmetic, queue, clocksource, RTC, ABI, scheduler and signal host
   suites passed with the host sanitizer profile. ABI parity and updated
   usercopy-manifest checks passed. Broader guards still report preexisting
   failures in unchanged `kcov_runtime_test.cpp`, credential KTEST assertion
   discovery and the VMEM raw-access source guard; see
   `/tmp/goal31-known-guard-failures.log`. These are not passing validations.
5. Optimized private-overlay 8-CPU VM booted using `wos-cluster --no-setup`.
   Correctly compiled standalone sleep ABI fixture passes; initial fixture
   builds inherited SafeStack instrumentation incompatible with their startup
   and faulted before the tests. Rebuilt with `-fno-sanitize=safe-stack` like
   the repository test program. Load/drift evidence remains in progress.
6. Process-owned REAL/VIRTUAL/PROF integration and final runtime qualification
   completed under the contracts and evidence below.

## Final integration validation

- Process interval ownership, shared signal claims, deferred retry, ignored
  signal handling, fork/exec/exit gates, and atomic old-value ABI are integrated.
- Local APIC calibration is per CPU, initialized before initial idle, with
  shared overflow-safe rounded-up conversion. Procfs exposes its calibration
  and limits alongside process/scheduler timer depths, budgets and lateness.
- All 17 focused final host suites pass, including clockevent arithmetic,
  timer races/models, signal/scheduler guards, ABI parity and usercopy manifests.
- Final normal-profile `wos_modules` and default isolated diagnostic KTEST
  builds pass. Automatic approval review rejected re-running normal full image
  packaging because it could overwrite the populated normal rootfs disk.
  The approved alternative created fresh optimized boot/rootfs images under
  `build/goal31-final-runtime/images/`; existing normal disks are preserved.
  This is not a claim that the rejected final `wos_full` invocation ran.
- Final diagnostic live KTEST completed **60,549 passed, 0 failed**, with clean
  automatic poweroff. Final optimized sleep ABI and **85 interval-timer
  assertions** pass. After lifecycle stress, process REAL/wake queue depths
  return to zero; all eight CPUs report initialized local clockevents.
  Final optimized load/drift qualification passes: one million SMP reads with
  zero monotonic/CPU-clock regressions; no early sleeps; unloaded p99/max
  lateness **4.272/5.514 ms**; 128-thread storm p99/max **43.615/59.449 ms**;
  60-second drift interval **[-288.142, +280.453] ppm**, within +/-1,000 ppm.
  The original 10-second drift remains inconclusive; it is not counted as a pass.
  Source/artifact hashes and logs are retained under
  `build/goal31-evidence/final/` and `final-runtime-metadata/`.
- Both owned VMs shut down cleanly. Final optimized launcher exited zero,
  its PID was absent and live descriptor removed. Final process timer queue
  depths were zero; NTP status showed one step, five slews, no rejected
  adjustments and no remaining slew. Shutdown's warning about two active
  kernel threads is preserved in the serial evidence; finalization completed.

## Completion contract evidence

| Requirement | Implementation and verification |
| --- | --- |
| Clocksource abstraction and integer conversions | TSC/HPET source status, selected resolution/frequency/ratio/fallback; wide-integer boundary/random oracles and KTEST |
| SMP monotonicity and source failures | Feature/calibration/watchdog policy, conservative SMP HPET fallback, shared atomic floor; concurrent model readers and one million live reads |
| Disciplined realtime | Validated CMOS, bounded first step and 500 ppm slew; positive/negative/saturation models, live initial and repeated NTP corrections |
| Scalable timers and bounded dispatch | Intrusive AVL, generation tokens, phase-preserving periodic rearm, cancellation, 16-root IRQ extraction and bounded daemon work; 10,000-timer model, races and live 128-thread storm |
| Sleep ABI/libc | Native versioned mono/realtime absolute/relative sleeps, correct EINTR/remaining time and errno conventions; host ABI math, parity, checked-usercopy guards and live ABI probe |
| Process interval timers | Canonical REAL/VIRTUAL/PROF ownership, shared pending claims, lifecycle/remote admission; service KTEST and 85 live checks including fork/exec/main-thread retirement and cancel/exit stress |
| Invalid inputs and overflow | Strict clock/flags/timeval/timespec validation, checked/saturating math and nonwrapping generations; models, parity and live malformed-input checks |
| Direct timeout consumers | Process waits use common deadline helper; scheduler/futex/poll safe arithmetic and specialized WKI/TCP/device ownership audited in `goal31_timeout_audit.md` |
| Diagnostics | `/proc/timekeeping` and additive `/proc/kcpustat` fields; live snapshots show source, wall adjustment, per-CPU clockevents, budgets, lateness and drained process queues |
| Reproducible validation | Final normal-profile modules, fresh private images, isolated diagnostic build and 60,549/0 live KTEST; 17 host suites and final workload measurements with fixed thresholds |

The three broader preexisting host guards listed above still fail; they were not
modified or counted as passing. Initial failed fixture runs, inconclusive drift
measurements and baseline results remain separate from final-version evidence.

## Supported behavior and limits

Monotonic/realtime clock reads and process/thread CPU clocks are supported;
native clock sleeps accept monotonic/realtime relative or absolute requests.
CPU clocks are observations, not supported clock-nanosleep domains. CPU interval
timers use aggregate process user/total counters at existing accounting boundaries.
Timer callbacks/wakes remain outside IRQ/resource locks; scheduler extraction is
limited to 16 due roots per IRQ and process service dispatch to 64 work records.
The requested 1 ms service polling interval is not a hard scheduling-latency promise.

SMP deliberately uses HPET until cross-CPU TSC qualification exists. It is slower
than direct TSC reads; baseline million-read workload took about 103 seconds.
Live suspend/resume is unsupported. RTC validation covers 1970–2199; the current
SNTP client rejects timestamps outside NTP era zero (through February 2036),
retaining RTC time nonfatally rather than wrapping the epoch. Authenticated NTP,
era unfolding and physical-platform/suspend qualification remain follow-ups.
Armed/pending interval timers explicitly prevent remote exec because its wire
format carries no timer state; local exec preserves timers. Specialized device
and protocol timeout ownership is documented in `goal31_timeout_audit.md`,
including preexisting AHCI polling/recovery limitations.

### Retire dormant callback stub

Remove `ktime::push_task` and its unregistered `handle_pit`/list from ktime.cpp
and ktime.hpp. Repository-wide caller search finds only this declaration and
definition; the callback signature ignores delay/context and has no cancellation
contract. Live consumers use scheduler deadlines and the process timer service.
No ABI, lock, allocation, or live IRQ behavior changes. Preserve header transitive
includes to avoid unrelated include cleanup. Normal/KTEST compilation validates
there are no callers; rollback restores the dormant declarations as a unit.
