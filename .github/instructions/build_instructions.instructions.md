---
applyTo: "**"
---

# WOS Build and Debugging Instructions

## Primary build rule

When building WOS, use the `Build WOS` task.

Do not substitute manual commands unless the task is unavailable in the current environment. If manual commands are needed, treat public README commands as fallback context only:

```sh
cd tools
./build_llvm.sh
cd ..
cmake -GNinja -B build .
cmake --build build
```

When in doubt, inspect `.vscode/tasks.json` and mirror the current `Build WOS`
task command.

## Build type names

Use these names exactly when selecting or reporting build profiles:

- Normal `Build WOS`: default fast/safe agent build. A fresh `build/` configure
  uses `CMAKE_BUILD_TYPE=RelWithDebInfo`, builds optimized artifacts with debug
  info, keeps expensive sanitizers/coverage off, and uses the kernel's
  freestanding/no-builtin/no-red-zone/no-SIMD compile contract plus libc++
  `fast` hardening.
- `Release`: optimized build without the default debug-info expectation. It is
  not the normal agent default unless the CMake cache or command line explicitly
  selects it.
- `Debug`: slower diagnostic build type. Prefer the isolated KTEST workflow for
  this rather than mutating the normal `build/` cache.
- `bin/wos-ktest`: isolated diagnostic selftest build in `build-ktest/` and
  `ktest-data/`. Its default profile enables KCFI, report-mode KUBSan, KASan,
  KCOV, selftests, network tracing, and allocation provenance.
- `bin/wos-ktest --fast`: faster diagnostic selftest profile using
  `RelWithDebInfo`, libc++ `fast` hardening, and no KCOV source-friendly `O0`
  override.
- `bin/wos-ktest --ubtrap`: runtime-free UBSan trap profile. It disables
  report-mode KUBSan and enables `WOS_KERNEL_UBSAN_TRAP=ON`.

Do not combine `WOS_KUBSAN=ON` and `WOS_KERNEL_UBSAN_TRAP=ON`. Use
`WOS_KERNEL_STRICT_WARNINGS=ON` and `WOS_KERNEL_EVERYTHING_WARNINGS=ON` only
for explicit warning-audit builds, not as routine validation defaults.

## Formatting checks

When running the repo formatter manually, use the current workspace path:

```sh
scripts/dev/format_repo.sh --check <paths>
```

Use this formatter path for manual checks unless the workspace instructions are
updated again.

## Cluster / VM state failures

If a WOS build fails during mountfs disk sync, for example in:

- `sync_rootfs.sh`
- `mountfs_disk`
- libguestfs appliance setup
- qemu appliance setup
- rootfs/disk sync while cluster/debug VMs are running

then treat it as a cluster/debug-VM state problem, not automatically as a code failure. Ask the user to stop the cluster/VMs and rerun the build.

## Debugging rule

Runtime debugging must be done by the user. When runtime information is required, ask the user to run the kernel, cluster, or debugger and provide logs/backtraces.

Known public README debug flow:

```sh
wos-cluster --launch --debug-node 0
```

Then in another terminal:

```sh
gdb build/modules/kern/wos
target remote localhost:1234
continue
```

Do not claim to have run this debug flow unless it was actually available and run in the current task.

## Artifact paths

- Kernel binary: `build/modules/kern/wos`
- Init binary: `build/modules/init/init`
- Test program binary: `build/modules/testprog/testprog`
- Test daemon binary: `build/modules/testd/testd`
- Network daemon binary: `build/modules/netd/netd`
- Debug server binary: `build/modules/debugserver/debugserver`
- HTTP server binary: `build/modules/httpd/httpd`
- Perf utility: `build/modules/perf/perf`
- Top utility: `build/modules/top/top`
- Memory accounting utility: `build/modules/memacc/memacc`
- Journal utility/daemon binary: `build/modules/journal/journal`
- WKI control utility: `build/modules/wkictl/wkictl`
- Power control utility: `build/modules/powerctl/powerctl`
- Render benchmark binary: `build/modules/renderbench/renderbench`
- Strace utility: `build/modules/strace/strace`
- SFTP server binary: `build/modules/sftpserver/sftp-server`

## CMake facts

Local workspace verified on 2026-07-01:

- Root `CMakeLists.txt` builds `tools/` through `ExternalProject` and then adds `modules/`.
- A fresh single-config root build defaults to `CMAKE_BUILD_TYPE=RelWithDebInfo`;
  host-side WOS tools use the separate `WOS_TOOLS_BUILD_TYPE` cache value,
  defaulting to `Release`.
- `modules/CMakeLists.txt` uses `toolchain/sysroot` as sysroot and adds `kern`, `init`, `testprog`, `testd`, `netd`, `httpd`, `debugserver`, `perf`, `top`, `memacc`, `journal`, `wkictl`, `powerctl`, `renderbench`, `strace`, and `sftpserver`.
- `modules/journal/CMakeLists.txt` also builds the shared `journal_lib` target as `libjournal.so`.
- `modules/kern/CMakeLists.txt` builds kernel target `wos`, recursively globs sources under `modules/kern/src`, disables hosted C++ runtime features for kernel C++, applies freestanding/no-builtin/no-red-zone/no-SIMD kernel flags, and defaults kernel libc++ hardening to `fast`.
- `configs/rootfs/aliases.tsv` installs utility targets such as `testprog`, `testd`, `perf`, `top`, `memacc`, `renderbench`, `debugserver`, `strace`, `journalctl`, `journald`, `wkictl`, `powerctl`, and `sftp-server` into the target rootfs, plus convenience symlinks for `wkictl`, `powerctl`, and toolchain utilities.

Do not infer GNU coreutils, systemd, Linux `perf`, procps `top`, or upstream `strace` behavior from these utility names. They are WOS userland programs and their option set may differ.

## Build-result reporting

In final responses, report exactly:

- Build command/task run.
- Whether it passed or failed.
- First relevant failure category.
- Whether failure appears code-related, toolchain-related, or cluster/VM-state-related.
- Any user action needed.
