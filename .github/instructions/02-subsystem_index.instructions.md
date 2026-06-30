---
applyTo: "**"
---

# WOS Subsystem Index

Use this index to decide what to read before changing code.

## Build and toolchain

Read first:

- `AGENTS.md`
- `.github/instructions/build_instructions.instructions.md`
- Root `CMakeLists.txt`
- `modules/CMakeLists.txt`
- The target module's `CMakeLists.txt`
- `.gitmodules`

Current local workspace:

- Tools are configured through an `ExternalProject` under `tools/`.
- Modules build against `toolchain/sysroot` as the sysroot.
- Module subdirectories include `kern`, `init`, `testprog`, `testd`, `netd`, `httpd`, `debugserver`, `perf`, `top`, `memacc`, `journal`, `wkictl`, `renderbench`, `strace`, and `sftpserver`.
- Rootfs command aliases and install paths are declared in `configs/rootfs/aliases.tsv`.

Risk areas:

- Generated sysroot/toolchain state.
- Cluster/VM state during rootfs/disk sync.
- Submodule state.
- Recursive source globbing can make new `.cpp` files participate automatically.

## Kernel core

Applies to:

- `modules/kern/src/**`

Read first:

- `.github/instructions/kernel.instructions.md`
- `.github/instructions/logging.instructions.md`
- `modules/kern/CMakeLists.txt`

Common entry areas:

- `platform/sys/syscall.cpp`: syscall dispatch.
- `platform/sched/`: scheduler, tasks, run queues, remote-placement hook.
- `platform/mm/`: memory management and allocation.
- `platform/interrupt/`: interrupt and descriptor setup.
- `vfs/`: VFS core, mounts, files, filesystem implementations.
- `net/`: networking, packet buffers, NAPI/netpoll, protocols, drivers.

## Syscalls and ABI

Read first:

- `.github/instructions/critical_paths.instructions.md`
- `modules/kern/src/platform/sys/syscall.cpp`
- `modules/kern/src/platform/sys/abi/callnums.hpp`
- Relevant `modules/kern/src/platform/sys/abi/callnums/*.h`
- Relevant `modules/kern/src/platform/sys/syscalls_impl/**`
- libc/userspace wrapper code for the operation being changed.

Risks:

- Register argument ordering.
- Negative errno convention at kernel boundary.
- User pointer validation and lifetime.
- Compatibility with libc wrappers.

## VFS

Read first:

- `modules/kern/src/vfs/vfs.hpp`
- `modules/kern/src/vfs/core.cpp`
- `modules/kern/src/vfs/mount.cpp`
- `modules/kern/src/vfs/file.hpp`
- Relevant filesystem implementation under `modules/kern/src/vfs/fs/`
- For remote paths, `.github/instructions/wki.instructions.md` and `modules/kern/src/net/wki/remote_vfs.*`

Risks:

- Path resolution across mount types.
- Remote mount recursion.
- Per-task file descriptor context.
- Open file lifetimes.
- Blocking/locking behavior inside syscall paths.

## Networking

Read first:

- `modules/kern/src/net/netdevice.hpp`
- `modules/kern/src/net/netpoll.hpp`
- Relevant driver under `modules/kern/src/dev/**`
- `modules/kern/src/net/proto/ethernet.cpp`
- WKI instructions when touching WKI ethertype/transport paths.

Risks:

- NAPI/re-entrance context.
- PacketBuffer ownership.
- RX/TX path allocation.
- Driver interrupt assumptions.

## WKI distributed subsystem

Applies to:

- `modules/kern/src/net/wki/**`

Read first:

- `.github/instructions/wki.instructions.md`
- `.github/instructions/wki_research_cache.instructions.md`
- `modules/kern/src/net/wki/wki.hpp`
- `modules/kern/src/net/wki/wire.hpp`
- `modules/kern/src/net/wki/wki.cpp`
- The specific subsystem file being modified.

Risk areas:

- Wire struct layout.
- Channel reliability and retransmit state.
- Peer state and fencing.
- NAPI context and spin-wait deadlocks.
- Remote VFS path recursion.
- Remote IPC exported-fd/proxy lifetime and poll wakeups.
- Scheduler remote-placement assumptions.
- Device proxy/server lifecycle.

## libc / userspace compatibility

Read first:

- `.github/instructions/mlibc.instructions.md` if `toolchain/src/mlibc/**` exists locally.
- `.gitmodules` and local libc/toolchain submodule directories.
- Relevant syscall ABI headers and userspace wrappers.

Risks:

- Mixing WOS-specific hacks with broadly compatible libc behavior.
- Wrong errno sign convention.
- Async-signal-unsafe fallbacks.
- Header ABI drift.

## WOS userspace utilities

Applies to:

- `modules/strace/**`
- `modules/wkictl/**`
- `modules/journal/**`
- `modules/memacc/**`
- `modules/top/**`
- `modules/perf/**`

Read first:

- The target utility's `CMakeLists.txt`.
- The target utility's `src/main.cpp`.
- `configs/rootfs/aliases.tsv` for installed command names and symlinks.
- Kernel syscall, procfs, journal, WKI, or perf sources when the utility crosses into those surfaces.

Utility map:

- `strace`: WOS ptrace syscall tracer for commands and `-p` attaches, with WKI-aware remote/proxy helpers.
- `wkictl`: WKI target-placement and VFS-routing control/query tool; also backs `locally`, `remotely`, `homeward`, `on`, `forward`, and `wosid`.
- `journalctl` / `journald`: installed names for the `modules/journal` binary; query/follow/filter journal records or run the journal persistence daemon. The module also builds `libjournal.so`.
- `memacc`: `/proc/memacc` memory accounting viewer/control utility for summaries, process rows, allocator rows, tracking, watch, and reclaim.
- `top`: interactive WOS process/CPU/memory monitor over WOS `/proc`.
- `perf`: WOS kernel event/performance utility for stat/record/report, scheduler CPU stats, WKI/local/VMEM/IPC reports, tail views, traces, and traced command runs.

Risks:

- Familiar names are not compatibility promises. These are WOS utilities, not GNU coreutils, systemd, procps, Linux `perf`, or upstream `strace`; option coverage may be smaller, larger, or simply different.
- Many commands depend on WOS-specific `/proc`, `/dev/journal`, ptrace, WKI, or perf ABI surfaces.
- Runtime behavior often requires the user to run the kernel or cluster and provide logs/output.

## Init/test/httpd/netd modules

Read first:

- Relevant module `CMakeLists.txt`.
- User-facing syscall/libc wrappers used by the program.
- Kernel syscall implementation if behavior crosses into the kernel.

Risks:

- Assuming Linux ABI behavior where WOS differs.
- Using libc functions whose sysdeps are incomplete.
- Masking kernel bugs with app-local workarounds.
