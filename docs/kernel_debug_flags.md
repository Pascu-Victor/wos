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
| `WOS_NET_TRACE` | `OFF` | `modules/kern/CMakeLists.txt` | `NET_TRACE=1` | Network hot-path span timing through `NET_TRACE_SPAN()` and periodic trace dumps. | Adds cycle counting and periodic logging in RX/TX/TCP paths. |
| `WOS_NET_PACKET_DEBUG` | `OFF` | `modules/kern/CMakeLists.txt` | `WOS_NET_PACKET_DEBUG=1` | Packet buffer allocation/free provenance and TX-refusal holder snapshots. | Adds per-packet CPU/site/sequence tracking and scans the packet pool on selected TX refusal events. |
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
| `sched.cpu_dump` | `modules/kern/src/platform/sys/context_switch.cpp`, `modules/kern/src/platform/sched/scheduler.cpp` | Periodically logs per-CPU scheduler state from CPU 0, including runqueue/waitqueue size, current task, preemption state, wait channel/callsite, reschedule token state, timer counters, wake IPI counters, and local reschedule counters. | Scheduler timer-path logging and runqueue lock sampling; use only for short diagnostic boots. |
| `sched.cpu_dump_local` | `modules/kern/src/platform/sys/context_switch.cpp`, `modules/kern/src/platform/sched/scheduler.cpp` | Periodically logs only the current CPU's scheduler state from that CPU's timer path, including live interrupt CS/RIP/RSP plus current task wait channel/callsite; avoids CPU 0 cross-CPU runqueue inspection. | Scheduler timer-path logging on every active CPU; use instead of `sched.cpu_dump` when CPU 0 or cross-CPU dump progress is suspect. |
| `sched.preempt_stall` | `modules/kern/src/platform/sched/scheduler.cpp` | Rate-limited log when a scheduler timer tick cannot preempt while runnable work is queued because the current context is not preemptible, such as a PROCESS interrupted in kernel mode or a DAEMON with preemption disabled; includes CPU, task, live CS/RIP/RSP, queue sizes, wait metadata, preempt state, and syscall start time. | Timer-path logging on the affected CPU; use only to diagnose non-preemptible syscall/kernel stalls. |
| `sched.single_process_cpu[=N]` | `modules/kern/src/platform/sched/scheduler.cpp` | Forces ordinary, unpinned user processes onto CPU `N` (default 0) and disables idle work stealing so parallel process workloads can be tested without cross-CPU user-process execution. | Diagnostic only: collapses user-process parallelism onto one CPU and can hide load-balancing bugs or timing-sensitive races. |
| `sched.no_process_migration` | `modules/kern/src/platform/sched/scheduler.cpp` | Keeps ordinary user processes on their current owner CPU when they wake and disables process idle stealing, while still allowing initial process placement to spread new processes across CPUs. | Diagnostic only: reduces load balancing and can hide migration-specific bugs; use to distinguish migration bugs from general SMP user-process corruption. |
| `sched.dual_run_panic` | `modules/kern/src/platform/sched/scheduler.cpp` | Panics if a process task is observed as `current_task` or `handoff_task` on another CPU while the local CPU is running or reserving that same task. | Diagnostic only: bounded cross-CPU scheduler snapshot from hot paths; use to catch duplicate task execution or stale handoff ownership. |
| `cpu.no_avx` | `modules/kern/src/platform/asm/cpu.cpp` | Keeps XSAVE/OSXSAVE enabled but masks XCR0 down to x87+SSE, so userspace feature probing can still use `xgetbv` while AVX xstate is unavailable. | Diagnostic only: changes guest-visible CPU feature availability and can reduce userspace performance; use to isolate AVX save/restore issues. |
| `virtio_net.no_mq` | `modules/kern/src/dev/virtio/virtio_net.cpp` | Disables virtio-net multiqueue feature negotiation and forces each virtio-net device to use a single RX/TX queue pair. | Diagnostic only: reduces network parallelism; use to isolate RX ordering or queue-steering bugs from TCP/userspace corruption. |
| `virtio_net.no_mrg_rxbuf` | `modules/kern/src/dev/virtio/virtio_net.cpp` | Disables virtio-net mergeable RX buffer feature negotiation and uses the fixed-size virtio-net header. | Diagnostic only: may reduce RX efficiency; use to isolate mergeable-buffer/header handling bugs from TCP/userspace corruption. |
| `vmem.fork_eager_copy` | `modules/kern/src/platform/mm/virt.opt.cpp` | Makes fork eagerly copy writable private user pages into the child instead of converting them to shared COW mappings. | Diagnostic only: makes fork much more expensive and increases memory pressure; use to isolate fork-COW, refcount, and stale-TLB corruption from unrelated allocator bugs. |
| `mutex.stall` | `modules/kern/src/platform/sys/mutex.cpp` | Rate-limited log when a `sys::Mutex` waiter has waited for more than 500 ms, including mutex address, waiter PID/site, owner PID/site, owner scheduler wait/saved-RIP state, waiter count, elapsed time, park attempts, and fallback reasons; parked waiters use a diagnostic timeout so owner information is emitted even when no unlock arrives. | Extra logging, PID lookups, and timed wakeups in contended mutex slow paths; use only when scheduler dumps show mutex wait channels/callsites. |
| `wki.spawn_diag` | `modules/kern/src/net/wki/remote_compute.cpp` | Logs WKI exec-placement decisions, including result, reason, pid/name, target flags, submitter identity, proxy/local fd hints, and selected node. | Adds journal output on WKI-aware exec placement paths; use only for short remote-compute placement investigations. |
| `wki.wait_diag` | `modules/kern/src/net/wki/wki.cpp`, `modules/kern/src/net/wki/remote_ipc.cpp` | Rate-limited logs for WKI waits that remain pending, PTY ioctl send/handle/response breadcrumbs, and remote IPC pipe/PTY send retry loops with peer, resource, retry count, and task identity. | Adds journal output from WKI wait and remote IPC control/data paths; use only for short deadlock investigations. |
| `pipe.diag` | `modules/kern/src/vfs/core.cpp` | Logs local pipe reader/writer block points and read/write-end close events, including pipe buffer state, waiters, open-end count, PID/task, and callsite. | Adds journal output from local pipe hot paths; use only around focused EOF or local pipe ownership investigations. |

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
