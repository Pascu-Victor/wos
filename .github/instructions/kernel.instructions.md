---
applyTo: "modules/kern/src/**"
---

# Kernel Conventions and Navigation

Kernel source lives under `modules/kern/src/**`.

Local workspace reviewed on 2026-06-22: CMake builds the kernel target `wos` from recursively globbed C, C++, assembly, and NASM sources. Kernel C++ is built with no exceptions, no RTTI, no thread-safe statics, no C++ static destructor machinery, freestanding/no-builtin/no-red-zone/no-SIMD flags, deterministic overflow semantics in the normal build, and libc++ `fast` hardening by default. Avoid language/runtime features that depend on exceptions, RTTI, hosted C++ runtime behavior, compiler-emitted SIMD/FPU state, or red-zone assumptions.

## Read before changing kernel code

- `AGENTS.md`
- `.github/instructions/logging.instructions.md`
- `.github/instructions/critical_paths.instructions.md`
- `modules/kern/CMakeLists.txt`
- The subsystem-specific instruction file, if any.

## Kernel safety checklist

For every kernel change, check:

- Is this code reachable before initialization is complete?
- Is this code reachable in interrupt, NAPI, scheduler, syscall, panic, or early boot context?
- Can this code allocate? Which allocator?
- Can this code block or spin-wait?
- Which locks are held on entry?
- Which locks are taken inside the function?
- Does this code call into VFS, scheduler, networking, WKI, or logging?
- Are user pointers involved?
- Are object lifetimes and ownership obvious?
- Are atomics using appropriate memory order?
- Is the error convention consistent with callers?

## Common kernel areas

- `platform/sys/`: syscall entry and dispatch.
- `abi/`: syscall ABI enums, call number headers, and shared kernel/userspace ABI structs.
- `syscalls_impl/`: syscall subsystem handlers.
- `platform/sched/`: scheduler, task state, queues, process/thread control.
- `platform/mm/`: physical/virtual/dynamic memory management.
- `platform/ktime/`: time source.
- `platform/interrupt/`: interrupt/GDT/IDT-related setup.
- `platform/dbg/`: kernel debug, coredump, and journal-backed logging support.
- `platform/debug/`: ptrace/debugger support used by WOS `strace` and debug tooling.
- `platform/perf/`: kernel perf event recording used by the WOS `perf` utility and procfs views.
- `vfs/`: VFS core, mounts, files, file operations, filesystem implementations.
- `vfs/fs/procfs.cpp`: WOS `/proc` implementation, including `top`, `perf`, `memacc`, and WKI diagnostic surfaces.
- `net/`: net devices, protocols, packet buffers, netpoll/NAPI.
- `net/wki/`: distributed WKI subsystem.
- `dev/`: hardware and virtual device drivers.

## Syscall notes

The local syscall handler in `modules/kern/src/platform/sys/syscall.cpp` copies `RAX` to `ker::abi::callnums`, then copies `RDI`, `RSI`, `RDX`, `R8`, `R9`, and `R10` into `A1..A6`. `A1` is usually the subsystem operation enum. When changing syscalls, inspect both sides:

- Kernel dispatch and implementation.
- ABI headers.
- Userspace/libc wrapper/sysdep code.
- Ptrace syscall-stop behavior when observable by `strace`.

Preserve kernel/user error conventions and argument ordering.

## WOS diagnostic surfaces

Several userland utilities depend on kernel-private WOS interfaces, not Linux-compatible ones:

- `journalctl` / `journald` use the journal device and `platform/dbg/journal.*`; `JournalRecord` is an ABI with fixed size assertions.
- `strace` uses `platform/debug/ptrace.*` and `abi/ptrace.hpp`; request numbers and ABI structs are covered by kernel tests.
- `perf` uses `platform/perf/perf_events.*` plus `/proc/kperf`, `/proc/kperfctl`, `/proc/kcpustat`, `/proc/kcontstat`, `/proc/kwkistat`, and `/proc/kipcstat`.
- `top` reads WOS `/proc` process, stat, load, and memory views.
- `memacc` reads and writes `/proc/memacc/**`; provenance availability depends on kernel memory-accounting build flags.
- `wkictl` consumes process/WKI target and VFS-routing syscall/procfs surfaces.

Before changing these surfaces, read the matching userspace utility under `modules/`.

## Scheduler notes

Scheduler/task-state changes require special care:

- Identify subject task versus currently running task.
- Avoid blocking while scheduler locks are held.
- Check state transitions and dead-list/epoch reclamation.
- Check remote compute hooks if WKI is enabled.

## VFS notes

VFS changes often affect syscall behavior, task FD state, remote VFS, and userspace compatibility. For remote paths, read WKI instructions first.

## Networking notes

Network RX and NAPI-like paths must not call operations that need the current context to process the response. For WKI ACK-wait operations, defer work out of RX/NAPI context when needed.
