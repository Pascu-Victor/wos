# WOS Agent Instructions

This file is the root entrypoint for agents working in WOS. The detailed project context lives in `.github/instructions/*.instructions.md`.

WOS is a large, fast-moving operating-system repository with kernel, userspace, networking, filesystem, toolchain/libc, and distributed-compute paths. Treat narrow search results as incomplete until the relevant subsystem entrypoints have been read.

## Authority and staleness

When determining current behavior, the local source tree is authoritative. These markdown files are navigation aids and conventions, not a substitute for reading code.

Use this precedence when context conflicts:

1. The user's current task and explicit constraints.
2. The local source code currently visible in the workspace.
3. Path-specific `.github/instructions/*.instructions.md` files.
4. This `AGENTS.md` file.
5. Repo intelligence/cache notes.
6. Public GitHub `main` observations.

The public GitHub branch may lag behind the user's local branch. Verify every cache claim against local files before relying on it. If a cache entry is wrong or stale, mention it in the final response.

## Required workflow before editing

For any non-trivial change:

1. Identify the subsystem(s) touched.
2. Read the relevant instruction files in `.github/instructions/`.
3. Read the source entrypoints listed in the subsystem index.
4. Build a small impact map: files, types, functions, locks, ABI/wire surfaces, tests/build tasks.
5. Decide whether the task needs reconnaissance, implementation, review, or cache maintenance.
6. Avoid unrelated cleanup.
7. Prefer minimal patches whose safety can be explained mechanically.

For critical paths, produce a patch contract before editing. Critical paths include scheduler/preemption, syscall ABI, VFS path resolution, network/NAPI, WKI wire protocol, WKI peer/channel state, remote VFS, remote compute, block/device proxying, interrupt/panic paths, allocator paths, and libc ABI/sysdep behavior.

## Reconnaissance mode

When the task is broad, ambiguous, or touches many files, do reconnaissance first and do not edit files until the relevant call/data flow is mapped.

Required reconnaissance output:

- Relevant files grouped by subsystem.
- Main entrypoints and public APIs.
- Call/data-flow sketch.
- Locks, lifetimes, context restrictions, and allocation/blocking constraints.
- ABI, syscall, wire-format, or userspace compatibility surfaces.
- Build/test/debug plan.
- Unknowns that require the user to run the kernel or cluster.
- Minimal implementation strategy.

## Patch contract for risky changes

Before editing critical code, write a short patch contract:

```text
Problem:
Expected files to change:
Files already read:
Invariants to preserve:
Locks involved:
Unsafe contexts:
Allocation/blocking assumptions:
ABI/wire/syscall compatibility impact:
Build/test plan:
Rollback plan:
```

## Build and debugging

Use the `Build WOS` task when building WOS.

Manual formatting uses `scripts/dev/format_repo.sh --check <paths>`. Do not
use the retired root-level `scripts/format_repo.sh` path.

Do not use the retired root-level `scripts/build_kern.sh` path. If the build
task is unavailable, read `.vscode/tasks.json` and use the current local CMake
command shape instead.

Kernel and module diagnostic flags are documented in `docs/kernel_debug_flags.md`. Read that map before enabling expensive sanitizer, coverage, allocator, scheduler, MM, or network tracing flags.

## Build types and default profile

Use these names precisely in agent plans and final responses:

- Normal `Build WOS`: the default fast/safe build. A fresh `build/` configure
  defaults to `CMAKE_BUILD_TYPE=RelWithDebInfo`, with optimized code, debug
  info, kernel freestanding/no-builtin/no-red-zone/no-SIMD compile semantics,
  deterministic signed/pointer overflow behavior, libc++ `fast` hardening, and
  expensive sanitizers/coverage/provenance diagnostics off unless explicitly
  enabled.
- `Release`: optimized build without the normal debug-info expectation. Do not
  call this the default unless the cache or command line explicitly uses it.
- `Debug`: diagnostic build type. It is slower and is normally used through the
  isolated KTEST workflow, not for routine agent validation.
- `bin/wos-ktest`: isolated diagnostic selftest build. Its default CMake
  profile enables KCFI, report-mode KUBSan, KASan, KCOV, selftests, network
  tracing, and allocation provenance in `build-ktest/` and `ktest-data/`.
- `bin/wos-ktest --fast`: faster diagnostic selftest profile using
  `RelWithDebInfo`, libc++ `fast` hardening, and no KCOV source-friendly `O0`
  override.
- `bin/wos-ktest --ubtrap`: runtime-free UBSan trap profile. It disables
  report-mode KUBSan and enables `WOS_KERNEL_UBSAN_TRAP=ON`.

Do not combine `WOS_KUBSAN=ON` with `WOS_KERNEL_UBSAN_TRAP=ON`; they are
separate UBSan profiles. Use `WOS_KERNEL_STRICT_WARNINGS=ON` or
`WOS_KERNEL_EVERYTHING_WARNINGS=ON` only for explicit warning-audit builds.

## Isolated KTEST selftest VM

Use `bin/wos-ktest` for the isolated kernel selftest VM workflow. It builds,
packages, and launches a single-node WOS VM with expensive kernel diagnostics
enabled and the kernel command line set from `configs/node.json` (normally
`--selftest`).

The KTEST workflow must stay separate from the normal WOS build and disk
artifacts. Its default state roots are:

- CMake build: `build-ktest/`
- Generated data, disks, overlays, logs, and target sysroot: `ktest-data/`
- Isolated libc/userspace build roots: `ktest-data/mlibc-build`,
  `ktest-data/busybox-build`, `ktest-data/busybox-install`, and
  `ktest-data/dropbear-build`

Do not route KTEST through the normal `build/`, `toolchain/sysroot`,
`disk.qcow2`, or `mountfs.qcow2` paths unless the user explicitly asks to
destroy that isolation.

Useful KTEST commands:

- `bin/wos-ktest --build-only --reset-sysroot`: re-seed the isolated sysroot
  and build diagnostic artifacts without touching VM topology.
- `bin/wos-ktest --no-build --no-package`: launch from already built and
  packaged KTEST artifacts.
- `bin/wos-ktest --teardown`: tear down the single-node topology.

The shared per-node VM layout lives in `scripts/cluster/node_setup.py`.
`scripts/cluster/cluster_setup.py` and `scripts/test/ktest_setup.py` both use
that layout; avoid duplicating QEMU argument, overlay, hostname, or NIC logic.
`configs/node.json` is the single-node KTEST spec and should keep the same VM
spec shape that cluster setup uses.

When KTEST appears not to boot, inspect `ktest-data/serial-vm0.log` and
`ktest-data/qemu-vm0.log`. If serial output stops immediately after OVMF starts
the hard disk, verify the boot partition contains `/EFI/BOOT/BOOTX64.EFI`,
`/limine/limine.conf`, `/wos`, and `/initramfs.cpio`; that is a packaging or
bootloader issue, not yet kernel runtime evidence.

If the build fails during mountfs disk sync, `sync_rootfs.sh`, `mountfs_disk`, or a libguestfs/qemu appliance step, treat it as cluster/debug-VM state rather than a code failure. Ask the user to stop the cluster/VMs and rerun the build.

Debugging must be done by the user. When runtime/debug information is needed, ask the user to run the kernel or cluster and provide logs/backtraces.

## WOS userspace utilities

Several WOS utilities intentionally use familiar Unix/Linux names. Do not assume GNU coreutils, systemd, upstream `strace`, Linux `perf`, or procps behavior from the command name alone. These are WOS utilities, may have fewer, more, or different options than similarly named tools, and their current local source is the authority.

The rootfs alias map in `configs/rootfs/aliases.tsv` installs the current utility targets under `/usr/bin` or `/usr/sbin`. Check the module source before relying on CLI behavior:

- `strace` (`modules/strace`): WOS ptrace-based syscall tracer for launching a command or attaching with `-p`; includes WKI-aware remote/proxy routing helpers.
- `wkictl` (`modules/wkictl`): WKI control/query utility for target placement, VFS forwarding rules, WOS identity, and perf visibility hints. It also backs the `locally`, `remotely`, `homeward`, `on`, `forward`, and `wosid` convenience commands.
- `journalctl` / `journald` (`modules/journal`, build target `journal`): journal reader/follower/filter for `/dev/journal` and persisted `/var/log/journal/wos.journal`; the same binary runs daemon mode as `journald` or with `--daemon`, and the module also builds `libjournal.so`.
- `memacc` (`modules/memacc`): memory accounting utility over `/proc/memacc` for summaries, per-process memory, kernel allocator/accounting rows, watch mode, tracking toggles, and reclaim controls.
- `top` (`modules/top`): WOS interactive process/CPU/memory monitor using WOS `/proc` files.
- `perf` (`modules/perf`): WOS kernel performance/event tool for CPU sampling, event recording/reporting, scheduler stats, WKI/local/VMEM/IPC reports, tail views, traces, and traced command runs.

Known artifact paths:

- Kernel: `build/modules/kern/wos`
- Init: `build/modules/init/init`
- Test program: `build/modules/testprog/testprog`
- HTTP server: `build/modules/httpd/httpd`
- Perf utility: `build/modules/perf/perf`
- Top utility: `build/modules/top/top`
- Memory accounting utility: `build/modules/memacc/memacc`
- Journal utility/daemon binary: `build/modules/journal/journal`
- WKI control utility: `build/modules/wkictl/wkictl`
- Strace utility: `build/modules/strace/strace`

## Final response requirements

For code changes, summarize:

- Files changed.
- What was fixed or implemented.
- Invariants preserved.
- Builds/tests run, or why they were not run.
- Remaining risks, stale docs, or required user-run debugging.

Do not claim a build, test, runtime check, or debug session succeeded unless it was actually performed in the current task.
