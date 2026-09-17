# Goal 31 time ABI patch contract

Problem: time syscalls accept unknown clocks; nanosleep reports success after interruption;
libc emulates absolute sleep once and lacks resolution and native clock sleep sysdeps.

Expected files to change: kernel time syscall implementation and ABI selector header;
matching mlibc ABI header, time wrappers, sysdep tags/signatures/registry/implementation,
ANSI time implementation; focused time arithmetic and ABI tests.

Files already read: kernel syscall dispatcher, time syscall files, ktime/tsc/rtc interfaces,
scheduler park helper and signal wake/deferred switch paths, task accounting and resource
owner implementation, libc time wrappers and generic time entrypoints, time source tests.

Invariants to preserve: selectors 0–5 and legacy register ordering; common checked usercopy;
retained process accounting owner; 100 Hz times accounting; syscall accounting excludes
parked time; only relative sleeps write EINTR remaining time; absolute sleep follows its
chosen clock, and wall adjustments never affect relative or monotonic deadlines.

Locks involved: no added locks. Sleep enters the existing current-context scheduler park
safe point without locks; usercopy occurs in syscall context before/after park. Existing
resource lock protects CPU observations. Interval timer behavior is outside this patch.

Unsafe contexts: do not perform usercopy from timer IRQ or runqueue critical sections;
do not alter signal delivery boundaries or scheduler task fields.

Allocation/blocking assumptions: requests use stack storage and checked usercopy; sleeps
can block through preemptible_syscall_park. Absolute realtime sleeps periodically reevaluate
the wall clock with a maximum 1 ms monotonic parking interval so steps/slews are observed.

ABI/wire/syscall compatibility impact: append CLOCK_GETRES=6 and CLOCK_NANOSLEEP=7;
new clock-sleep request has fixed-width signed sec/nsec, clock/flags, remaining pointer,
and version checked in arg2. Existing operation signatures and selectors remain unchanged.
Libc clock_nanosleep returns positive errno and preserves errno; unknown flags/clocks
return errors instead of aborting. Kernel rejects malformed or overflowing nanoseconds.

Build/test plan: compile and execute focused integer conversion/deadline tests; run time
source regression guards and ABI mirror checks; primary agent runs Normal Build WOS and
live isolated KTEST/ABI qualification using the user-authorized --no-setup workflow.

Rollback plan: revert appended selectors and matching libc callers together; leave existing
selectors stable. Do not restore acceptance of invalid clocks or silent interrupted success.

## Validation completed for this patch

- The initial standalone arithmetic harness compiled and passed; it was then converted to registered GTest cases, and `time_abi_math_test` passes through CTest. Use the registered target for the current source.
- `python3 tests/host/unit/time_source_test.py`: passed.
- Kernel time.cpp and mlibc WOS sysdeps.cpp/ANSI time.cpp: target compiler syntax
  checks passed using their current compile_commands.json entries with output/dependency
  generation removed and `-fsyntax-only`, `CCACHE_DISABLE=1`.
- `scripts/dev/format_repo.sh --check --format-only` on changed kernel/model/probe C/C++: passed.
- `git diff --check` in both repository and mlibc submodule: passed.

`tests/fixtures/service_time_abi.c` is the live WOS ABI probe. It compiles on the host,
but its stricter invalid-flags contract intentionally differs from Linux, which accepted
unknown clock_nanosleep flag bit 2 during the host smoke run. That run stopped at this
assertion and is not WOS runtime evidence. The probe passed against the baseline rebuilt WOS libc/kernel; final integration
validation is tracked in `goal31_timekeeping.md`. It checks clock errors, nullable resolution output, absolute and
relative sleeps, errno preservation, SA_RESTART interruption, and remaining time.

Current resolution/latency distinction: clock reads retain nanosecond values; scheduler
parks round deadlines upward to microseconds. Realtime absolute sleeps reevaluate their
wall-clock deadline with at most 1 ms between requested wakes, subject to scheduling load.

Build integration: the strace time-operation name switch now handles appended operations
6/7. Its source test checks every numeric time ABI selector has a decoder case.
Read strace CMakeLists, main, aliases and decoder before this small compatibility edit.
