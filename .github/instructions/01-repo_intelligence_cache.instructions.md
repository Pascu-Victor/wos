---
applyTo: "**"
---

# WOS Repo Intelligence Cache

Generated: 2026-06-01.

Baseline sources:

- Public repository baseline: `Pascu-Victor/wos` on `main`.
- User-supplied local instruction files: `AGENTS.md`, `build_instructions.instructions.md`, `logging.instructions.md`, `wki.instructions.md`, `mlibc.instructions.md`.
- Local workspace files verified for utility/module inventory: `modules/CMakeLists.txt`, utility module `CMakeLists.txt` files, utility `src/main.cpp` entrypoints, and `configs/rootfs/aliases.tsv`.

This cache is a map, not authority. The user's local branch may contain work that is not on GitHub. Always verify claims against local source before editing.

## Project identity

WOS is a multi-system, multi-tasking operating system intended to scale across multiple networked computers. The public README describes a CMake/Ninja build flow, a custom toolchain build step, and QEMU/GDB debugging through `wos-cluster --launch --debug-node 0` and port `1234`.

## Build facts

Public `main` previously showed only the smaller baseline module set. Local workspace verified on 2026-06-01 shows:

- Root `CMakeLists.txt` includes `ExternalProject`, builds tools from `tools/`, and then adds `modules/`.
- `modules/CMakeLists.txt` uses a sysroot at `toolchain/sysroot`, sets `clang`/`clang++` from the host toolchain area, enables C++23, uses static linking, and adds subdirectories:
  - `kern`
  - `init`
  - `testprog`
  - `testd`
  - `netd`
  - `httpd`
  - `debugserver`
  - `perf`
  - `top`
  - `memacc`
  - `journal`
  - `wkictl`
  - `renderbench`
  - `strace`
  - `sftpserver`
- `modules/kern/CMakeLists.txt` builds the kernel executable `wos` from recursive `src/*.c`, `src/*.cpp`, `src/*.S`, and `src/*.asm`; kernel compile options include freestanding/no-builtin/no-red-zone/no-SIMD constraints, no exceptions/RTTI/thread-safe statics for C++, deterministic overflow semantics in the normal build, libc++ `fast` hardening by default, and `-mcmodel=kernel`.
- `configs/rootfs/aliases.tsv` installs several WOS utility targets into `/usr/bin` or `/usr/sbin`, including `perf`, `top`, `memacc`, `strace`, `journalctl`, `journald`, and `wkictl` plus the `wkictl` convenience symlinks.

Prefer the workspace `Build WOS` task over manual public README commands. Manual commands are fallback only when the task is unavailable.
Do not use retired root-level build helpers such as `scripts/build_kern.sh`;
inspect `.vscode/tasks.json` for the current task command shape instead.
Manual formatting uses `scripts/dev/format_repo.sh --check <paths>`; do not
use the retired root-level `scripts/format_repo.sh` path.

## Local submodules

Local `.gitmodules` reviewed on 2026-06-01 lists:

- `modules/extern/limine` from `https://github.com/limine-bootloader/limine.git`
- `toolchain/src/llvm-project` from `https://github.com/Pascu-Victor/llvm-project.git`
- `toolchain/src/mlibc` from `https://github.com/Pascu-Victor/mlibc.git`
- `toolchain/src/dropbear` from `https://github.com/Pascu-Victor/dropbear.git`
- `toolchain/src/busybox` from `https://github.com/Pascu-Victor/busybox.git`

The old public-baseline `modules/stdlib/musl` note is stale for this workspace. Verify local submodule checkout state before editing toolchain/libc code.

## Major source areas

- `modules/kern/src/`: kernel source.
- `modules/kern/src/platform/`: low-level platform, scheduling, syscall, time, memory, interrupts.
- `modules/kern/src/platform/sys/syscall.cpp`: syscall dispatch entrypoint.
- `modules/kern/src/platform/sched/`: scheduler and task integration.
- `modules/kern/src/vfs/`: VFS core, files, mounts, filesystem implementations.
- `modules/kern/src/net/`: networking core, protocol handling, net devices.
- `modules/kern/src/net/wki/`: WKI distributed networking/resource subsystem.
- `modules/init/`: init userspace target.
- `modules/testprog/`: test userspace target.
- `modules/testd/`: test daemon/userspace target.
- `modules/netd/`: network daemon/userspace target.
- `modules/httpd/`: HTTP server/userspace target.
- `modules/debugserver/`: debug server/userspace target.
- `modules/perf/`: WOS kernel event/performance utility.
- `modules/top/`: WOS interactive process/CPU/memory monitor.
- `modules/memacc/`: WOS memory-accounting utility over `/proc/memacc`.
- `modules/journal/`: WOS journal query/daemon binary and `libjournal.so`.
- `modules/wkictl/`: WKI control utility and placement/routing convenience command implementation.
- `modules/renderbench/`: rendering benchmark/userspace target.
- `modules/strace/`: WOS ptrace syscall tracer.
- `modules/sftpserver/`: SFTP server/userspace target.
- `tools/`: host/toolchain build support.
- `toolchain/`: generated or local toolchain/sysroot area, verify locally.

## WOS userspace utility notes

Familiar command names are not compatibility promises. `strace`, `journalctl`, `top`, `perf`, `memacc`, and `wkictl` are WOS utilities, not GNU coreutils or guaranteed Linux/systemd/procps/upstream-tool equivalents. Their features may be fewer, more, or different than expected from another OS; read the local module source or usage text before assuming CLI behavior.

- `strace` launches or attaches to WOS processes through ptrace and has WKI-aware remote/proxy routing paths.
- `wkictl` manages/query WKI target placement and VFS routing, reports WOS identity, and backs `locally`, `remotely`, `homeward`, `on`, `forward`, and `wosid` aliases.
- `journalctl` / `journald` are installed names for the `modules/journal` binary. Query mode reads persisted `/var/log/journal/wos.journal` and live `/dev/journal`; daemon mode persists journal records. The module also builds `libjournal.so`.
- `memacc` reads and writes `/proc/memacc` views for memory summaries, process memory, kernel allocator/accounting rows, tracking toggles, watch mode, and reclaim controls.
- `top` renders WOS process, CPU, memory, load, and task-state information from WOS `/proc`.
- `perf` records and reports WOS kernel events, including CPU/process stats, scheduler data, WKI/local/VMEM/IPC reports, tail/trace views, and traced command runs.

## WKI local facts

Key files under `modules/kern/src/net/wki/` reviewed locally on 2026-06-01 include:

- `wki.hpp`, `wki.cpp`
- `wire.hpp`
- `peer.hpp`, `peer.cpp`
- `channel.hpp`, `channel.cpp`
- `routing.hpp`, `routing.cpp`
- `zone.hpp`, `zone.cpp`
- `event.hpp`, `event.cpp`
- `dev_proxy.hpp`, `dev_proxy.cpp`
- `dev_server.hpp`, `dev_server.cpp`
- `remote_vfs.hpp`, `remote_vfs.cpp`
- `remote_compute.hpp`, `remote_compute.cpp`
- `remote_ipc.hpp`, `remote_ipc.cpp`, `remote_ipc_socket.cpp`
- `remote_net.hpp`, `remote_net.cpp`
- `remotable.hpp`, `remotable.cpp`
- `irq_fwd.hpp`, `irq_fwd.cpp`
- `transport_eth.hpp`, `transport_eth.cpp`
- `transport_roce.hpp`, `transport_roce.cpp`
- `transport_ivshmem.hpp`, `transport_ivshmem.cpp`
- `blk_ring.hpp`

`wki.hpp` defines the main WKI public API and global state shape. `wire.hpp` defines protocol constants, channels, message types, packed wire structs, and size assertions. `wki.cpp` defines `g_wki`, initialization, transport registry, peer/channel management, send paths, timer/RX integration, and subsystem initialization order.

## Local instruction facts already supplied by the user

Existing local instructions specify:

- Use the `Build WOS` task.
- Use `scripts/dev/format_repo.sh --check <paths>` for formatting checks; the
  old root-level `scripts/format_repo.sh` path is retired.
- Do not use the old root-level `scripts/build_kern.sh` path; use `Build WOS`
  or the current task command in `.vscode/tasks.json`.
- Treat mountfs/libguestfs/qemu appliance build failures as cluster/debug-VM state problems.
- Runtime debugging must be performed by the user.
- Kernel logging should use the journal-backed logging API with typed loggers for new code.
- WKI code lives in `ker::net::wki` and has explicit naming, locking, wire-format, and remote VFS gotchas.
- mlibc work should use the tag-based sysdep system when `toolchain/src/mlibc/**` exists locally.

## Cache use rules

- Read this file for navigation only.
- Verify all facts in local code before editing.
- If a listed file no longer exists, search for renamed equivalents.
- If local source contradicts this cache, follow local source and report the stale cache section.
- Do not silently update this cache unless the task includes instruction/cache maintenance.
