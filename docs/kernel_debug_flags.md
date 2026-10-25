# WOS Kernel And Module Debug Flags

This file maps the build-time diagnostic flags that are intentionally exposed
through CMake for the kernel and WOS modules. Prefer these switches over ad hoc
source edits when enabling expensive diagnostics.

Use them with a fresh configure, for example:

```sh
cmake -B build -DWOS_KCOV=ON -DWOS_KCOV_PANIC_TRACE=ON .
```

For allocator/MM performance-control runs, use a separate build directory with
the provenance control switch:

```sh
cmake -B build-mm-perf-control -GNinja -DWOS_MM_PROVENANCE_PERF_CONTROL=ON .
cmake --build build-mm-perf-control --target wos_full
```

## Kernel Build Profiles

- Normal build: `Build WOS` / `build/`, default
  `CMAKE_BUILD_TYPE=RelWithDebInfo`. This is the default fast/safe profile:
  optimized code, debug info, kernel freestanding/no-builtin/no-red-zone/no-SIMD
  flags, deterministic overflow semantics, libc++ `fast` hardening, and
  expensive sanitizer/coverage/provenance diagnostics off.
- Diagnostic KTEST: `bin/wos-ktest`, isolated in `build-ktest/` and
  `ktest-data/`. It enables KCFI, report-mode KUBSan, KASan, KCOV, selftests,
  network tracing, and allocation provenance.
- Fast diagnostic KTEST: `bin/wos-ktest --fast`, using `RelWithDebInfo`,
  libc++ `fast` hardening, and no KCOV source-friendly `O0` override.
- Trap-mode UBSan KTEST: `bin/wos-ktest --ubtrap`, replacing report-mode
  KUBSan with `WOS_KERNEL_UBSAN_TRAP=ON`.

Do not combine `WOS_KUBSAN=ON` with `WOS_KERNEL_UBSAN_TRAP=ON`; the former
uses WOS UBSan handlers for reports/panic, while the latter emits trap
instructions and does not require UBSan runtime callbacks. Local Clang rejects
`-fsanitize=function` with the kernel code model and rejects
`-fsanitize-kcfi-arity` for `x86_64-pc-wos`, so those suggested checks are not
enabled here until the target supports them.

## Kernel CMake Options

| Flag | Default | CMake owner | Compile definition | What it enables | Downsides |
| --- | --- | --- | --- | --- | --- |
| `WOS_KERNEL_LIBCXX_HARDENING_MODE` | `fast` | `modules/kern/CMakeLists.txt` | `_LIBCPP_HARDENING_MODE=...` | Kernel libc++ header hardening mode: `none`, `fast`, `extensive`, or `debug`. The default selects security-critical constant-time checks with low overhead. | `debug` can be very expensive and should stay in diagnostic builds. |
| `WOS_KCFI` | `OFF` | `modules/kern/CMakeLists.txt` | `WOS_KCFI=1` | Clang kernel control-flow integrity. | Adds compiler instrumentation and can expose toolchain/runtime integration issues. |
| `WOS_BLOCK_WAIT_DIAGNOSTICS` | `OFF` | `modules/kern/CMakeLists.txt` | `WOS_BLOCK_WAIT_DIAGNOSTICS=1` | Bounded per-queue block-request polling exit counters in `/proc/blockq`, merged once per completed wait. | Counter work and a count-cap elapsed sample perturb diagnostic timing; use observed exits to investigate, and exclude instrumented timings from performance qualification. All fields and work compile out when disabled. |
| `WOS_KUBSAN` | `OFF` | `modules/kern/CMakeLists.txt` | `WOS_KUBSAN=1` | Kernel undefined-behavior sanitizer using WOS `__ubsan_handle_*` callbacks for report/panic behavior. | Adds checks to instrumented C/C++ and can change hot-path timing. Do not combine with `WOS_KERNEL_UBSAN_TRAP`. |
| `WOS_KERNEL_UBSAN_TRAP` | `OFF` | `modules/kern/CMakeLists.txt` | `WOS_KERNEL_UBSAN_TRAP=1` | Runtime-free trap-mode UBSan profile covering undefined, integer, implicit-conversion, bounds, local-bounds, nullability, and float checks supported by the kernel code model. | Omits deterministic `-fwrapv`/`-fwrapv-pointer` semantics to catch signed/pointer overflow; use only in diagnostic boot/test images. Do not combine with `WOS_KUBSAN`. |
| `WOS_KASAN` | `OFF` | `modules/kern/CMakeLists.txt` | `WOS_KASAN=1` | Kernel address sanitizer with the WOS shadow map and KASAN reports. | Adds memory-access instrumentation, shadow memory pressure, and page-fault interaction. Some early/MM/fault files are explicitly excluded. |
| `WOS_KCOV` | `OFF` | `modules/kern/CMakeLists.txt` | `WOS_KCOV=1` | `-fsanitize-coverage=trace-pc` for kernel coverage collection used by Syzkaller and optional selftest coverage output. | Inserts a compiler callback at basic-block entries. Leave off for scheduler/MM/network performance work. |
| `WOS_KCOV_NO_PRUNE` | `ON` | `modules/kern/CMakeLists.txt` | none | Adds `-fsanitize-coverage-no-prune` so KCOV instruments all eligible trace-pc blocks instead of letting LLVM coalesce redundant points. This yields denser line/block reports from the same KCOV runtime. | Increases instrumentation density and callback overhead for KCOV-enabled builds. |
| `WOS_KCOV_SOURCE_FRIENDLY` | `OFF` | `modules/kern/CMakeLists.txt` | none | Overrides the kernel target's usual optimized codegen with `-O0`, disables most inlining, and disables tail-call folding for KCOV builds so DWARF/addr2line maps stay closer to source statements and call sites. This is a good fit for isolated `build-ktest/` coverage runs. | Much slower and larger kernel code; coverage callbacks fire on less-optimized control flow, so do not use for performance-sensitive comparisons. Requires `WOS_KCOV=ON`. |
| `WOS_KCOV_PANIC_TRACE` | `OFF` | `modules/kern/CMakeLists.txt` | `WOS_KCOV_PANIC_TRACE=1` | A per-CPU recent-PC ring dumped during panic. Requires `WOS_KCOV=ON`. | Adds work to every KCOV callback even when no userspace coverage buffer is active. |
| `WOS_KERNEL_STRICT_WARNINGS` | `OFF` | `modules/kern/CMakeLists.txt` | none | Strict kernel warning audit profile with `-Werror`, conversion, shadowing, format, switch, unsafe-buffer, and C++ override/move/thread-safety warnings. | Expected to be noisy; use for focused warning cleanup, not routine builds. |
| `WOS_KERNEL_EVERYTHING_WARNINGS` | `OFF` | `modules/kern/CMakeLists.txt` | none | Adds a Clang `-Weverything` C++ warning audit profile with only compatibility/padding suppressions. | Very noisy and intentionally not a default. |
| `WOS_SELFTEST` | `OFF` | `modules/kern/CMakeLists.txt` | `WOS_SELFTEST=1` | Builds kernel selftest sources and enables `--selftest` boot handling. | Includes test code in the kernel image. Selftests run only when requested by the kernel command line. |
| `WOS_ASYNC_BLOCK_IO` | `ON` | `modules/kern/CMakeLists.txt` | `WOS_ASYNC_BLOCK_IO=1` | Bounded physical-root request queues, safe sync bridge and negotiated AHCI NCQ; additive `/proc/blockq` counters. | Normal storage policy, enabled after Goal 30 host, KTEST, local/remote recovery and fixed paired performance qualification. Set OFF for the synchronous compatibility profile; see `docs/asynchronous_block_io.md`. |
| `WOS_FILE_FAULT_FATAL_DIAGNOSTICS` | `OFF` | `modules/kern/CMakeLists.txt` | `WOS_FILE_FAULT_FATAL_DIAGNOSTICS=1` | Carries the actual file-fault decision to unrecovered CPL3 fatal logging. | Diagnostic build only; adds fault-path scalar capture and a bounded pool scan, and uses the existing journal locks/UART polling after all MM locks are released. No performance qualification. |
| `WOS_SCHED_TRANSITION_VALIDATION` | `OFF` | `modules/kern/CMakeLists.txt` | `WOS_SCHED_TRANSITION_VALIDATION=1` | Checks scheduler queue, ownership, publication, handoff, accounting, and GC invariants at stable runqueue-lock boundaries, recording a bounded allocation-free failure snapshot. Enabled by the isolated KTEST profile. | Adds bounded scans to scheduler transition paths; keep it off for normal builds and scheduler performance comparisons. |
| `WOS_PREEMPT_GUARD_TIMING` | `OFF` | `modules/kern/CMakeLists.txt` | `WOS_PREEMPT_GUARD_TIMING=1` | Samples the outermost preemption guard's start and maximum hold duration. Enabled by KTEST; when off, timing fields remain zero/unavailable while guard depth, owner tracking and pending reschedules remain active. | Two clock reads per outermost guard, including spinlocks; HPET fallback reads cause device accesses and can dominate normal lock costs. Keep off for normal performance measurements. |
| `WOS_NET_TRACE` | `OFF` | `modules/kern/CMakeLists.txt` | `NET_TRACE=1` | Network hot-path span timing through `NET_TRACE_SPAN()` and periodic trace dumps. | Adds cycle counting and periodic logging in RX/TX/TCP paths. |
| `WOS_NET_PACKET_DEBUG` | `OFF` | `modules/kern/CMakeLists.txt` | `WOS_NET_PACKET_DEBUG=1` | Packet buffer allocation/free provenance and TX-refusal holder snapshots. | Adds per-packet CPU/site/sequence tracking and scans the packet pool on selected TX refusal events. |
| `WOS_TCP_CONGESTION_CONTROL` | `ON` | `modules/kern/CMakeLists.txt` | `WOS_TCP_CONGESTION_CONTROL=1` | Bounded NewReno congestion control, sender SACK hints, fast recovery and zero-window persist. | Normal sender policy, enabled after goal 29 correctness, IPv4/IPv6 impairment, fairness and clean-link regression qualification. Set OFF for the peer-window diagnostic control; see `docs/tcp_sender_qualification.md`. |
| `WOS_MEMACC_FULL_DEFAULT` | `OFF` | `modules/kern/CMakeLists.txt` | `WOS_MEMACC_FULL_DEFAULT=1` | Compiles and default-enables full `/proc/memacc` provenance by implying `WOS_PHYS_ALLOC_CALLER_STATS=ON` and `WOS_KMALLOC_DEBUG_INFO=ON`. | Enables allocation provenance costs for `memacc` diagnostics. |
| `WOS_PHYS_ALLOC_CALLER_STATS` | `OFF` | `modules/kern/CMakeLists.txt` | `WOS_PHYS_ALLOC_CALLER_STATS=1` | Per-call-site physical page allocation histogram for OOM reports. | Captures return addresses and updates a global spin-protected table on page allocation success. |
| `WOS_PHYS_LOCK_DEBUG` | `OFF` | `modules/kern/CMakeLists.txt` | `WOS_PHYS_LOCK_DEBUG=1` | Physical allocator memlock holder CR3/CPU/RIP tracking. | Adds metadata writes and return-address capture while acquiring the physical allocator lock. |
| `WOS_KMALLOC_DEBUG_INFO` | `OFF` | `modules/kern/CMakeLists.txt` | `WOS_KMALLOC_DEBUG_INFO=1` | kmalloc caller/tag side table and slab debug-index dumps for OOM/corruption reports. | Adds allocation-time return-address/tag capture and a global debug table. |
| `WOS_MM_RECLAIM_MAGIC_PROBES` | `OFF` | `modules/kern/CMakeLists.txt` | `WOS_MM_RECLAIM_MAGIC_PROBES=1` | Opts destroy-user-space reclaim back into content-backed `UNKNOWN` frame magic probes for diagnostic builds. | Reads page contents for otherwise-ambiguous frames and can perturb MM teardown hot paths. Forced `OFF` by `WOS_MM_PROVENANCE_PERF_CONTROL`. |
| `WOS_MM_PROVENANCE_PERF_CONTROL` | `ON` | `modules/kern/CMakeLists.txt` | none | Forces `WOS_MEMACC_FULL_DEFAULT=OFF`, `WOS_PHYS_ALLOC_CALLER_STATS=OFF`, `WOS_PHYS_LOCK_DEBUG=OFF`, `WOS_KMALLOC_DEBUG_INFO=OFF`, and `WOS_MM_RECLAIM_MAGIC_PROBES=OFF` for the default fast/safe build. | Removes allocation provenance diagnostics unless a diagnostic profile turns them back on. |

## Module CMake Options

| Flag | Default | CMake owner | Compile definition | What it enables | Downsides |
| --- | --- | --- | --- | --- | --- |
| `WOS_MANDELBENCH_DEBUG` | `OFF` | `modules/testprog/CMakeLists.txt` | `MANDELBENCH_DEBUG=1` or `0` | Verbose mandelbench CPU and WKI render instrumentation in `testprog`. | Adds extra benchmark logging and can distort renderbench/testprog timings. |
| `WOS_UASAN` | `OFF` | `modules/CMakeLists.txt` | `WOS_UASAN=1` | AddressSanitizer instrumentation for WOS userspace module targets, excluding the kernel. The WOS compiler-rt build installs ASAN runtime archives, and procfs exposes `/proc/<pid>/maps` for runtime map discovery. | Requires a rebuilt WOS toolchain with ASAN compiler-rt support, or a compatible runtime directory passed through `WOS_UASAN_RUNTIME_DIR`. Adds shadow-memory/runtime overhead and can expose target-runtime integration gaps; it is less mature than kernel KASAN. |

## KTEST Coverage

KTEST coverage records a set of PCs in a bitmap with one bit per byte of
kernel text. Repeated visits do not consume additional space, so long selftest
runs do not need a larger VM to avoid filling a raw-PC trace buffer. The
`wos-ktest-cov` fingerprint hashes the exact loaded range between the kernel
text symbols, including linker alignment padding. Existing logs with
`KCOV_TRUNCATED` still need a fresh `wos-ktest` run for complete coverage;
the report tool continues to reject dropped PCs and mismatched ELFs by default.

## KCOV-Off Behavior

`WOS_KCOV=OFF` is a normal build mode:

- CMake does not pass `-fsanitize-coverage=trace-pc`, so no compiler-emitted
  `__sanitizer_cov_trace_pc()` calls are required.
- `WOS_KCOV_NO_PRUNE` has no effect unless `WOS_KCOV=ON`.
- Kernel selftests still build and run when `WOS_SELFTEST=ON`; they simply skip
  `[KCOV_BEGIN]`, `[KCOV]`, and `[KCOV_END]` output.
- Panic dumps still run; they simply omit the optional recent-PC trace unless
  both `WOS_KCOV=ON` and `WOS_KCOV_PANIC_TRACE=ON`.

## MM Provenance Control Builds

The default kernel configure is the fast/safe control build:
`WOS_MM_PROVENANCE_PERF_CONTROL=ON` forces `WOS_MEMACC_FULL_DEFAULT`,
`WOS_PHYS_ALLOC_CALLER_STATS`, `WOS_PHYS_LOCK_DEBUG`,
`WOS_KMALLOC_DEBUG_INFO`, and `WOS_MM_RECLAIM_MAGIC_PROBES` off so scheduler,
allocator, network, and renderbench attribution is not mixed with extra
provenance overhead. Keep diagnostic and control configurations in separate
directories so cache values do not overwrite each other.

Use `WOS_MM_PROVENANCE_PERF_CONTROL=OFF` plus the specific provenance flags
when corruption, OOM, or `/proc/memacc` investigation needs full allocation
context. The isolated `bin/wos-ktest` diagnostic profile does this by default.

If disabling flags manually, set `WOS_MEMACC_FULL_DEFAULT=OFF` first; while it
is `ON`, CMake intentionally forces `WOS_PHYS_ALLOC_CALLER_STATS=ON` and
`WOS_KMALLOC_DEBUG_INFO=ON` to keep full `/proc/memacc` provenance available.

## Runtime Kernel Command-Line Diagnostics

These diagnostics are compiled into normal kernels but stay disabled unless
the boot command line contains the exact token.

| Token | Location | What it does | Downsides |
| --- | --- | --- | --- |
| `wki.routing_rx_timing` | `modules/kern/src/net/wki/routing.cpp` | Saves five intermediate monotonic checkpoints and a bounded snapshot for the same slowest LSA/lease-request handler as `max_rx_us`; `/proc/wki/netdiag` adds `wki_routing_rx version=1` and `wki_routing_wake version=1`. | Extra enabled clock reads and their overhead lie inside the unchanged wall-time total. RX stages cover validation, origin lookup, preparation, database lock wait, apply/unlock, and remaining tail including scheduler wake. The wake row links to the same RX `started_us` and reports target selection, token/placement preparation, leader admission, reschedule drain, aggregate runqueue lock wait/body/return and timer/IPI poke spans with pass/lock/scan/poke counts. Lock and poke spans overlap reschedule time; all are elapsed time, not exclusive CPU. Fixed start-CPU IRQ/timer counters may include unrelated execution; committed IRQ time excludes scheduler timer duration. Enabled boots also retain a bounded fixed-start-CPU timer trace: 64 slots per runqueue and at most 32 copied events, with caller identity, arm intent/ticks, interrupt RIP/CS and completed handoffs. Bulk trace storage is separate from the per-handler wake sample; copying occurs only for an enabled winning maximum, after the original RX end timestamp under its existing diagnostic lock. `wki_routing_timer_trace` declares sequence range, retained count, omitted/missing events and completeness; event rows share `rx_started_us`. Sequence is reservation order, not timestamp order. Arm observations precede hardware writes; C++ IRQ boundaries omit assembly and may have no exit for a non-returning path. No retained Task/stack pointers, observer locks or waits; busy slots drop. Enabled event clocks/atomics perturb timing, and OFF branch/stack plus fixed per-runqueue storage costs are not zero. `recorded=0` means no worker wake was sampled. A mask identifies RX stages reached; ignored own LSAs and empty/disabled legacy ACKs remain excluded. Endpoint CPU observations do not prove no intervening migration. Disabled boots add no diagnostic clocks/counter reads, allocation, locks or periodic work; branch/stack costs are not assumed zero. |
| `net.napi_diag` | `modules/kern/src/net/netpoll.cpp`, `modules/kern/src/dev/virtio/virtio_net.cpp` | Enables fixed NAPI scheduling-source, poll-origin, RX-budget-work, TX-reclamation and worker-wait counters. Virtio queue rows in `/proc/wki/napi` and `/proc/wki/netdiag` append `napi_diag=1` and the counters; ordinary boots append only `napi_diag=0`. | Adds relaxed atomic counter updates when enabled. Snapshots are independent cumulative observations, not a transactional state or a packet trace. |
| `--napi-irq-test` | `modules/kern/src/dev/virtio/virtio_net.cpp` (`WOS_SELFTEST` only) | Opt-in live IRQ-publication recovery fixture; requires `net.napi_diag`, four CPUs, one virtio device and one configured queue pair. | Use the private socket backend runner below; omit BSP `--selftest` and `--virtio-test-live`. Suppresses software scheduling publication from an admitted real IRQ, not physical IRQ delivery. |
| `sched.cpu_dump` | `modules/kern/src/platform/sys/context_switch.cpp`, `modules/kern/src/platform/sched/scheduler.cpp` | Periodically logs per-CPU scheduler state from CPU 0 as bounded `schedcpu`, `schedcpu_preempt`, `schedcpu_migrate`, and `schedcpu_wait` records correlated by CPU/current PID. Together they include saved/live frame classes, preemption and migration depth/owner, pending/transition state, last source/target CPU and rejection reason, queues, timer counters, wake IPIs, and local reschedules. | Scheduler timer-path logging and runqueue lock sampling; use only for short diagnostic boots. |
| `sched.cpu_dump_local` | `modules/kern/src/platform/sys/context_switch.cpp`, `modules/kern/src/platform/sched/scheduler.cpp` | Periodically logs only the current CPU's scheduler state from that CPU's timer path, including live interrupt CS/RIP/RSP plus current task wait channel/callsite; avoids CPU 0 cross-CPU runqueue inspection. | Scheduler timer-path logging on every active CPU; use instead of `sched.cpu_dump` when CPU 0 or cross-CPU dump progress is suspect. |
| `sched.preempt_stall` | `modules/kern/src/platform/sched/scheduler.cpp` | Rate-limited log when a scheduler timer tick cannot preempt while runnable work is queued; includes the live frame class, exact rejection reason, task/source/target CPU, live CS/RIP/RSP, preemption and migration depth/owner, transition state, queues, wait metadata, and syscall start time. | Timer-path logging on the affected CPU; use only to diagnose non-preemptible syscall/kernel stalls. The same last-decision scalars are always available without logging in `/proc/kcpustate`. |
| `sched.kpreempt=on` / `sched.kpreempt=off` | `modules/kern/src/platform/sched/scheduler.cpp` | Ordinary PROCESS kernel-frame timer preemption is enabled by default. Valid timer-suspended kernel frames may load-balance across CPUs only after the centralized frame, resource, guard, transition, affinity, domain, and WKI checks pass. `on` is an explicit enable; exact-token `off` restores the legacy voluntary-only policy and wins if both tokens are supplied. | Use `off` as the emergency correctness baseline; it reduces kernel preemption coverage and can hide latency or concurrency bugs specific to ordinary syscall execution. |
| `sched.single_process_cpu[=N]` | `modules/kern/src/platform/sched/scheduler.cpp` | Forces ordinary, unpinned user processes onto CPU `N` (default 0) and disables idle work stealing so parallel process workloads can be tested without cross-CPU user-process execution. | Diagnostic only: collapses user-process parallelism onto one CPU and can hide load-balancing bugs or timing-sensitive races. |
| `sched.no_process_migration` | `modules/kern/src/platform/sched/scheduler.cpp` | Keeps ordinary user processes on their current owner CPU when they wake and disables process idle stealing, while still allowing initial process placement to spread new processes across CPUs. | Diagnostic only: reduces load balancing and can hide migration-specific bugs; use to distinguish migration bugs from general SMP user-process corruption. |
| `sched.dual_run_panic` | `modules/kern/src/platform/sched/scheduler.cpp` | Panics if a process task is observed as `current_task` or `handoff_task` on another CPU while the local CPU is running or reserving that same task. | Diagnostic only: bounded cross-CPU scheduler snapshot from hot paths; use to catch duplicate task execution or stale handoff ownership. |
| `cpu.no_avx` | `modules/kern/src/platform/asm/cpu.cpp` | Keeps XSAVE/OSXSAVE enabled but masks XCR0 down to x87+SSE, so userspace feature probing can still use `xgetbv` while AVX xstate is unavailable. | Diagnostic only: changes guest-visible CPU feature availability and can reduce userspace performance; use to isolate AVX save/restore issues. |
| `virtio_net.no_mq` | `modules/kern/src/dev/virtio/virtio_net.cpp` | Disables virtio-net multiqueue feature negotiation and forces each virtio-net device to use a single RX/TX queue pair. | Diagnostic only: reduces network parallelism; use to isolate RX ordering or queue-steering bugs from TCP/userspace corruption. |
| `virtio_net.no_mrg_rxbuf` | `modules/kern/src/dev/virtio/virtio_net.cpp` | Disables virtio-net mergeable RX buffer feature negotiation and uses the fixed-size virtio-net header. | Diagnostic only: may reduce RX efficiency; use to isolate mergeable-buffer/header handling bugs from TCP/userspace corruption. |
| `vmem.no_lazy_anon` | `modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp` | Restores eager zero-page-PTE population for writable anonymous mappings that do not request `MAP_POPULATE`. | Diagnostic only: increases mmap, page-table, refcount, TLB, and later COW work; use to compare demand paging against the legacy anonymous mapping path. |
| `vmem.fork_eager_copy` | `modules/kern/src/platform/mm/virt.opt.cpp` | Makes fork eagerly copy writable private user pages into the child instead of converting them to shared COW mappings. | Diagnostic only: makes fork much more expensive and increases memory pressure; use to isolate fork-COW, refcount, and stale-TLB corruption from unrelated allocator bugs. |
| `mutex.stall` | `modules/kern/src/platform/sys/mutex.cpp` | Rate-limited log when a `sys::Mutex` waiter has waited for more than 500 ms, including mutex address, waiter PID/site, owner PID/site, owner scheduler wait/saved-RIP state, waiter count, elapsed time, park attempts, and fallback reasons; parked waiters use a diagnostic timeout so owner information is emitted even when no unlock arrives. | Extra logging, PID lookups, and timed wakeups in contended mutex slow paths; use only when scheduler dumps show mutex wait channels/callsites. |
| `wki.compute_credentials_off` | `modules/kern/src/net/wki/wki.cpp` | Suppresses complete compute credential record negotiation; local credential enforcement remains active and legacy submission requires an exact snapshot round trip. | Cheap mixed-version diagnostic; restricted strict placement returns ENOTSUP and optional placement can remain local. |
| `wki.vfs_open_identity_off` | `modules/kern/src/net/wki/wki.cpp` | Suppresses ordinary OPEN credential negotiation; clients return ENOTSUP without a legacy retry. Explicit regular-file reads and local enforcement remain enabled. | Cheap mixed-version diagnostic. |
| `wki.vfs_fd_metadata_off` | `modules/kern/src/net/wki/wki.cpp` | Suppresses retained-FD metadata negotiation; FSTAT returns ENOTSUP without pathname, shared-cache or synthetic fallback. Ordinary OPEN and local metadata remain enabled. | Cheap mixed-version diagnostic. |
| `wki.vfs_stat_identity_off` | `modules/kern/src/net/wki/wki.cpp` | Suppresses credential-aware pathname STAT/LSTAT/access and batch STAT negotiation; requests return ENOTSUP. | Cheap mixed-version diagnostic. |
| `wki.vfs_path_identity_off` | `modules/kern/src/net/wki/wki.cpp` | Suppresses credential-bearing path envelope negotiation (READLINK); requests return ENOTSUP. | Cheap mixed-version diagnostic. |
| `wki.vfs_component_paths_off` | `modules/kern/src/net/wki/wki.cpp` | Suppresses preserved dot-component negotiation. Mapped clients return ENOTSUP for paths containing `.` or `..`; ordinary names keep their existing operation capabilities. | Cheap mixed-version diagnostic. |
| `wki.vfs_directory_paths_off` | `modules/kern/src/net/wki/wki.cpp` | Rejects descriptor-relative PATH_AS READLINK_AT, STAT_AT and OPEN_AT selectors with ENOTSUP, emulating a receiver without those selectors. Other path operations are unchanged. | Cheap mixed-version diagnostic. |
| `wki.vfs_realpath_off` | `modules/kern/src/net/wki/wki.cpp` | Rejects the PATH_AS REALPATH selector with ENOTSUP, emulating a receiver without mapped canonicalization. Existing selectors retain their behavior. | Cheap mixed-version diagnostic. |
| `wki.spawn_diag` | `modules/kern/src/net/wki/remote_compute.cpp` | Logs WKI exec-placement decisions, including result, reason, pid/name, target flags, submitter identity, proxy/local fd hints, and selected node. | Adds journal output on WKI-aware exec placement paths; use only for short remote-compute placement investigations. |
| `wki.wait_diag` | `modules/kern/src/net/wki/wki.cpp`, `modules/kern/src/net/wki/remote_ipc.cpp` | Rate-limited logs for WKI waits that remain pending, PTY ioctl send/handle/response breadcrumbs, and remote IPC pipe/PTY send retry loops with peer, resource, retry count, and task identity. | Adds journal output from WKI wait and remote IPC control/data paths; use only for short deadlock investigations. |
| `pipe.diag` | `modules/kern/src/vfs/core.cpp` | Logs local pipe reader/writer block points and read/write-end close events, including pipe buffer state, waiters, open-end count, PID/task, and callsite. | Adds journal output from local pipe hot paths; use only around focused EOF or local pipe ownership investigations. |

`/proc/wki/napi` is a separate 64 KiB snapshot of admitted virtio queue pairs,
readable only with kernel-debug authority. It shares the `virtio_net` row format
with the aggregate view and includes full worker PIDs and CPUs. Its version-1
header declares `scope=virtio_admitted`; the final `wki_napi_end` row reports
`complete`, emitted `rows` and `snapshot_rows`. Require `complete=1` and matching
counts before using a snapshot. The driver statically bounds admitted queues to
32, and rows are staged before copying so buffer exhaustion preserves whole rows
and reports `complete=0`. Closed admission excludes retiring devices. Reopen the
file for a fresh snapshot; counters remain independent cumulative observations.
This view does not include other drivers or establish completeness of the larger
`/proc/wki/netdiag` aggregate, which uses a separate 4 MiB text buffer. Require
its own final `wki_netdiag_end complete=1` row before treating the aggregate as
complete; allocation failure while staging origin snapshots suppresses that row.

NAPI scheduling counters distinguish `irq`, completion `rearm`, `startup` and
`unspecified` attempts; `accepted_*` counts successful IDLE-to-SCHEDULED
transitions. Coalesced, polling and disabled refusals are separate. Poll counters
distinguish `worker`, caller-driven `inline` and watchdog `rescue` origins;
the origin travels as a callback argument, including across a completion that
admits another poll owner. `rxwork_*` is the driver's returned RX budget work,
which can include dropped or invalid completions, rather than delivered packets.
If multiqueue activation fails, pair 0 can also drain configured inactive RX
queues; that work is attributed to pair 0's poll.
`rxzero_*` does not establish an empty or spurious poll. `txreclaimed_*` counts
owned buffers detached by virtio NAPI polling, excluding opportunistic send-side
reclamation. Other drivers do not provide this virtio TX accounting.
`park_calls` counts attempted block calls, including calls that consume an
already-published event. `park_empty_returns` records a return with no work flag
and no SCHEDULED state observed; a racing producer can change either observation.
The unsigned 64-bit counters may wrap and are read independently. They do not
prove which interrupt was lost, match each wake to a packet, or establish a
hardware recovery bound. Diagnostics do not change the watchdog cadence,
notification handshake, polling budgets or trust policy.

`inline_refused` counts inline POLLING admission failures, including an already
owned or DISABLED queue. It does not imply useful work or an IRQ failure.

## Legacy Source-Only Debug Macros

These macros exist in source but are not CMake options at the time of writing.
Prefer adding a CMake option and documenting it here before using one in a
shared workflow.

| Macro | Location | What it does | Downsides |
| --- | --- | --- | --- |
| `SCHED_DEBUG` | `modules/kern/src/platform/sched`, `modules/kern/src/platform/sys/context_switch.cpp` | Enables scheduler/context-switch debug logging and checks. | Scheduler hot-path logging and timing perturbation. |
| `DEBUG_KMALLOC` | `modules/kern/src/platform/mm/dyn/kmalloc.opt.cpp` | Emits kmalloc allocation/free serial diagnostics. | Allocator hot-path logging and serial output. |
| `ELF_DEBUG` | `modules/kern/src/platform/loader`, `modules/kern/src/platform/mm/virt.opt.cpp` | Enables ELF-loader and mapping debug logs. | Very noisy process-load/MM output. |
| `ELF_DEBUG_EXTRA` | `modules/kern/src/platform/loader/elf_loader.cpp` | Enables additional ELF-loader detail beyond `ELF_DEBUG`. | Extremely verbose process-load output. |
| `VERBOSE_PAGEMAP_SWITCH` | `modules/kern/src/platform/mm/virt.opt.cpp` | Enables detailed pagemap-switch logging. | MM hot-path logging. |

## Practical Guidance

- Default builds are fast/safe control builds with expensive sanitizer,
  coverage, tracing, and MM provenance options off unless explicitly enabled.
- Keep `WOS_KCOV`, `WOS_KCOV_PANIC_TRACE`, `WOS_NET_TRACE`, and
  `WOS_NET_PACKET_DEBUG` off for performance investigations unless the
  specific diagnostic is needed.
- Keep corruption, OOM, panic, sanitizer violation, and user-visible failure
  logs enabled. The flags above only gate extra provenance/tracing.
- Reconfigure after changing an option; many of these flags affect compile-time
  instrumentation and object layout.

### Live NAPI IRQ-publication recovery fixture

Build the isolated diagnostic kernel, stop other WOS VMs, then run
`python3 scripts/test/napi_irq_recovery.py`. `--prepare-only` packages and verifies
without launching; `--no-package --run-dir <prepared directory>` launches the
verified image. All private boot images, overlays, endpoints, configs and raw
receipts stay under `ktest-data/napi-irq/`. The runner uses the shared KTEST
single-node socket-multicast backend on loopback, one queue pair and four CPUs.
It does not change the normal images, TAPs, bridges or trust configuration.
In this named selftest boot only, initialization reserves the fixture NIC but
skips WKI Ethernet transport registration and broadcasts. This prevents
background control completions; the reservation also excludes IPv6 setup.

Four ordinary cookies precede eight suppressed cases; four ordinary cookies
follow them. The backend holds each fault reply until the kernel arms its
one-shot hook after 200 ms of queue quiet. The hook runs after the admitted IRQ
masks notifications and records one pending RX used index, descriptor and
length without reading DMA payload. Normal managed RX completion must deliver
the same cookie and tuple through a `RESCUE` poll, within 150000 microseconds
(the 100 ms idle rescue interval plus a declared 50 ms scheduling allowance).
Every fault window must have zero scheduling-producer attempts, bad cookies,
duplicates and foreign packets. Both ordinary phases need accepted IRQ and
worker RX progress. The kernel releases its driver/shutdown hold before the
ordinary poweroff finalizer. Raw failed attempts remain evidence.

This fixture qualifies software IRQ-schedule publication loss on that real
single-pair vNIC. It does not establish physical IRQ-loss, multiqueue recovery,
four-node throughput, or idle CPU bounds. Host hook tests use clock and DMA
seams; they separately exercise eligibility, ownership and rejected verdicts.

## Fatal file-fault decision records

`WOS_FILE_FAULT_FATAL_DIAGNOSTICS=ON` is a separate diagnostic kernel profile.
With the flag absent, the original probe, handler and exception-path tokens are
unchanged. No Task, core, syscall or wire layout changes. Recovered page faults,
ptrace stops and synchronous-signal recovery emit no decision record.

At the existing unrecovered user page-fault boundary, `file_fault_decision`
identifies the actual probe return site/disposition and the handler's exact
completion outcome. All three records carry the same stack-POD-address `attempt`
key and captured Task/PID, so concurrent fatal records can be joined. This key
is local to a live attempt; stack reuse does not make it a global event ID. `file_fault_pool` is an earlier probe-return snapshot,
collected under the existing pool lock; it is not the final completion epoch.
`file_fault_range` is the copied probe range, not a later borrowed File read.
Validity bits in `virt.hpp` distinguish unavailable data. With RANGE valid,
`context_invalid` records the actual composite guard bits; the pool snapshot
also includes its locked next-sequence value. Pool status is valid
only for a nonzero matching published/completed generation; completion status
comes from the handler's exact-generation acquire. Translation is recorded only
when the original status-zero short-circuit actually performs that walk.
Preemption depth is captured before probe locks. Raw and retained root values
are labelled separately; neither printing a pointer nor a missing core page
proves a page-table state. If `same_task=0`, the record must not be interpreted
as the current fault's coherent context. The logger is allocation-free here but
is not wait-free: existing journal/serial locking and UART polling still apply.
