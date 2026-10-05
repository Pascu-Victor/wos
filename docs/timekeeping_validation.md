# Clock and timer validation

The dated results below describe the exact historical images used on
2026-09-17. Their raw archive paths may no longer be available. Run the retained
fixtures again to qualify a current build.

These acceptance thresholds are fixed before any measurements from this fixture.
Use a normal optimized WOS VM for performance evidence. Diagnostic KTEST runs can
exercise semantics but do not qualify latency or drift performance. The runner
requires an explicit `--profile normal|diagnostic` recorded in its results.

| Surface | Workload | Acceptance |
| --- | --- | --- |
| Monotonicity | 8 threads, 125,000 reads each (1,000,000 total) | Zero per-thread regressions and zero clock errors |
| CPU clocks | Each monotonic worker samples process/thread CPU clocks every 1,024 reads under work | Zero regressions and positive aggregate work progress |
| Unloaded expiry | 1,000 absolute CLOCK_MONOTONIC sleeps, each requested 1 ms into the future | Zero early returns; p99 lateness <=5 ms; maximum <=50 ms |
| Timer storm | 128 concurrent sleepers, 128 absolute 1 ms sleeps each | Zero early returns; p99 lateness <=100 ms; maximum <=1 s |
| Drift | >=10 seconds host perf_counter elapsed between bounded guest clock sample exchanges | Full measured drift interval within +/-1,000 ppm |

The storm starts all 128 workers through a shared condition-variable gate after
thread creation; stacks are explicitly 64 KiB. Every latency sample sets a fresh
future deadline, so late samples cannot turn subsequent iterations into zero-delay
catch-up calls. Percentiles use nearest rank. The fixture has a 180-second process
alarm and runner commands have configurable deadlines (default 240 seconds).

Drift uses a persistent helper over one SSH channel. For each sample, the guest
reads CLOCK_MONOTONIC after receiving a request and replies with its nanoseconds;
host perf_counter_ns brackets the request/reply. Sixteen samples are collected at
each endpoint and the minimum-RTT interval is selected. The endpoint interval math
uses no assumption of symmetric network delay. The true host delta is bounded by
`[last.send - first.receive, last.receive - first.send]`; divide the guest delta by
both bounds to derive a conservative drift interval. If RTT-induced half-width
exceeds 1,000 ppm, report INCONCLUSIVE. An interval wholly outside the threshold
fails; an interval straddling a threshold is INCONCLUSIVE, never a pass.

Runner: `python3 tests/fixtures/service_time_load.py HOST --binary PRECOMPILED_BINARY
--profile normal --output NEW_OUTPUT_DIRECTORY`. Authentication reuses the repository's
SSH-agent connector convention. The runner uploads a unique /tmp helper, saves bounded
stdout/stderr, captures /proc/timekeeping and /proc/kcpustat before and after, records
binary SHA-256, command timing and return status, then removes the helper in finally.
It does not launch or stop VMs. Output paths must be new to preserve prior evidence.

The CPU workload verifies observation semantics, not exact attribution. Dedicated
wall-step/slew, NTP response, cancellation and process-lifecycle tests remain separate.
An inconclusive drift result needs lower-RTT transport or a longer separately declared
measurement; do not loosen the threshold after seeing results.

## Fixture validation

The C fixture passes strict host and WOS target syntax checks. A target executable
was linked with the current WOS sysroot, `-O2`, and `-pthread`. Host smoke runs completed
all requested counts with zero regressions or early sleeps; these runs validate the
harness only and are not WOS qualification evidence. Python bytecode compilation and
CLI parsing passed. Deterministic drift-bound checks cover a precise pass, a clear
frequency failure, excessive RTT uncertainty, and nonpositive elapsed time.

The host model tests were converted to registered GTest cases; both
`time_abi_math_test` and `interval_timer_model_test` pass through CTest in
`/tmp/wos-goal31-host`. No VM was started or modified by fixture preparation.

## Standalone fixture build and capture requirements

The WOS compiler enables SafeStack by default, while these standalone fixture
executables do not initialize its unsafe-stack TLS. Match the userspace module
build's explicit `-fno-sanitize=safe-stack`; otherwise fixtures fault in the
function prologue before checking any clock behavior. For example:

```sh
mkdir -p build/time-validation
toolchain/host/bin/clang --target=x86_64-pc-wos --sysroot=toolchain/sysroot \
  -O2 -fno-sanitize=safe-stack -pthread tests/fixtures/service_time_load.c \
  -o build/time-validation/service_time_load
```

Use the same flags for `service_time_abi.c`. Proc snapshots are collected through
bounded remote `cat`: the current SFTP server returns empty content for synthetic
proc files with zero reported size. Empty snapshots are rejected, and the runner
saves command exit status even when a crash produces no JSON measurement.

## Optimized live result, 2026-09-17

Single disposable 8-vCPU VM, HPET fallback (`hpet:smp-unqualified`, 100 MHz,
10 ns resolution), current normal kernel, standalone fixtures compiled as above:

| Check | Observed | Result |
| --- | --- | --- |
| ABI | Clock IDs/resolution, relative and absolute sleeps, SA_RESTART interruption and remaining time | PASS |
| Monotonic / CPU clocks | 1,000,000 reads, 8 threads; zero errors or regressions; positive CPU progress | PASS |
| Unloaded expiry | 1,000 sleeps; zero early; p99 4.26350 ms; max 6.47314 ms | PASS |
| Storm | 128 threads, 16,384 sleeps; zero early; p99 47.19329 ms; max 63.98769 ms | PASS |
| Initial 10 s drift | Conservative interval [-1513.77, +1633.28] ppm; RTT uncertainty 1573.52 ppm | INCONCLUSIVE |
| Separately declared 60 s drift | Conservative interval [-264.740, +301.841] ppm; RTT uncertainty 283.290 ppm | PASS |

The accuracy threshold stayed at +/-1,000 ppm. The 60-second follow-up was
specified before that measurement and retained the original inconclusive result.
Monotonic workload elapsed time was 102.807 s, exposing the cost of the conservative
HPET path; no throughput target was claimed. During heavy load one periodic NTP
attempt failed nonfatally and later attempts succeeded; snapshots show one wall
step, repeated slews, and no rejected clock adjustments. This does not establish
absolute UTC accuracy or qualify other machines, VM configurations, or suspend.

Evidence directories under `build/goal31-evidence/`: `normal-abi-fixed`,
`normal-load-fixed`, `normal-drift60`, and `normal-runtime-metadata` contain command
status, hashes, raw samples, proc snapshots, and serial/config capture. Earlier
`normal-abi`/`normal-load` failures preserve the invalid SafeStack fixture attempts;
those are fixture startup failures, not clock-test results. All temporary guest
helpers were removed. That baseline VM was subsequently shut down gracefully
before packaging the final integration image below.

## Final integrated optimized result, 2026-09-17

Measurement version `normal-final-20260917` uses newly packaged private images,
an 8-vCPU/8-GiB VM, and the normal optimized kernel built at
`2026-09-17T08:15:22.852738Z`. Its SHA-256 is
`101d31792453b1835af4d9bd8a819f326c8cd42e9bdc1e858d0a4347aba36af8`.
The source is `hpet:smp-unqualified` at 100 MHz with 10 ns resolution;
clockevents use `x2apic-oneshot`. TSC calibration spread was 9 ppm, but that
does not qualify cross-CPU synchronization, so the conservative HPET policy
remained selected. The separate final diagnostic KTEST run reported 60,549
passed and zero failed; its performance is not included in this table.

| Check | Observed | Result |
| --- | --- | --- |
| ABI | Clock IDs/resolution, relative and absolute sleeps, SA_RESTART interruption and remaining time | PASS |
| Interval timers | 85 checks, zero failures; real/virtual/prof, shared owner, ignored signals, fork/exec behavior | PASS |
| Monotonic / CPU clocks | 1,000,000 reads, 8 threads; zero errors or regressions; positive CPU progress | PASS |
| Unloaded expiry | 1,000 sleeps; zero early; p99 4.271980 ms; max 5.513950 ms | PASS |
| Storm | 128 threads, 16,384 sleeps; zero early; p99 43.615250 ms; max 59.448690 ms | PASS |
| Initial 10 s drift | Conservative interval [-1675.049, +1490.239] ppm; RTT uncertainty 1582.644 ppm | INCONCLUSIVE |
| Separately declared 60 s drift | Conservative interval [-288.142, +280.453] ppm; RTT uncertainty 284.297 ppm | PASS |

The original 10-second result and its runner exit status 2 remain in the evidence.
The separately declared 60-second observation used the same +/-1,000 ppm bound;
no acceptance threshold changed. The monotonic workload took 103.435 seconds,
which exposes HPET read cost without claiming a throughput qualification.

Final counters before shutdown reported one wall step, five slews, zero rejected
adjustments, zero pending correction, and the 500 ppm slew limit. Both process
timer queue depths were zero; 19 expirations were recorded with 4,035 us maximum
lateness and a 64-item work budget. Under the heavy read workload, NTP accepted
a -53.244492 ms correction with 108.068453 ms RTT; one later periodic attempt
failed nonfatally, followed by successful samples. These measurements establish
neither absolute UTC accuracy nor NTP accuracy under asymmetric network delay.
They qualify this VM configuration, not other machines or suspend behavior.

Serial output retained adopted-child warnings with status zero and a shutdown
warning that two kernel threads remained active. Shutdown continued through
filesystem unmounts; the poweroff command and cluster launcher both exited zero,
QEMU PID 139446 disappeared, and its live-debug descriptor was removed. No kernel
panic or user fault appeared in this final run. All temporary probe helpers were
removed, and private images and overlays were preserved.

Evidence under `build/goal31-evidence/` is separated into `final-abi`,
`final-interval`, `final-load`, `final-drift60`, `final-probe-orchestration`, and
`final-runtime-metadata`. Metadata includes kernel, image and fixture hashes and
timestamps, final proc snapshots, serial/QEMU/launch logs, the pre-shutdown live
descriptor, poweroff status, and verified shutdown with post-shutdown overlay
hashes. Earlier baseline and invalid SafeStack fixture attempts remain separate.
