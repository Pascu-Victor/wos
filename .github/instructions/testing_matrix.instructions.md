---
applyTo: "**"
---

# WOS Build, Test, and Debug Matrix

Use this to select checks after changes.

## Default check

Use the `Build WOS` task.

For a fresh normal build cache, this means the default fast/safe profile:
`CMAKE_BUILD_TYPE=RelWithDebInfo`, optimized artifacts with debug info, kernel
freestanding/no-builtin/no-red-zone/no-SIMD compile semantics, libc++ `fast`
hardening, and expensive sanitizer/coverage/provenance diagnostics off.

Report whether it passed or failed. If it failed, classify the failure as code, toolchain/sysroot, submodule, or cluster/VM state when possible.

## Fallback public build commands

Only use these if the workspace build task is unavailable:

```sh
cd tools
./build_llvm.sh
cd ..
cmake -GNinja -B build .
cmake --build build
```

## Artifact paths

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

These utility names are WOS programs, not GNU coreutils or guaranteed Linux/systemd/procps/upstream-tool equivalents. Check the local module source or usage text before assuming options.

## Runtime debugging

Runtime debugging must be performed by the user. Ask the user to run the kernel/cluster and provide output when runtime behavior is needed.

Public README fallback:

```sh
wos-cluster --launch --debug-node 0
```

Then:

```sh
gdb build/modules/kern/wos
target remote localhost:1234
continue
```

## Suggested checks by change type

### Build/toolchain/CMake

- `Build WOS`
- Check generated artifact paths.
- Check sysroot/toolchain assumptions.
- Check whether submodules are initialized.
- Use `bin/wos-ktest`, `bin/wos-ktest --fast`, or `bin/wos-ktest --ubtrap`
  only when the task explicitly needs isolated diagnostic/selftest coverage.

### Formatting-only or style-sensitive changes

- `scripts/dev/format_repo.sh --check <paths>`
- Do not use retired root-level formatter references such as
  `scripts/format_repo.sh`; this workspace uses `scripts/dev/format_repo.sh`.

### Kernel syscall changes

- `Build WOS`
- Inspect libc/userspace wrappers.
- Ask user to run focused userspace program if behavior is runtime-only.

### VFS changes

- `Build WOS`
- Ask user to run the kernel and exercise open/read/write/readdir/path cases when needed.
- For remote VFS, include cluster/peer scenario details.

### Scheduler/task changes

- `Build WOS`
- Ask user for runtime scheduler logs/backtrace if needed.
- Check task lifecycle and dead-list/epoch implications manually.

### WKI protocol/RX/channel changes

- `Build WOS`
- Ask user to run WKI cluster/VM scenario.
- Request packet/log traces around peer connect, heartbeat, ACK/retransmit, or target operation.
- Treat build failures in rootfs/disk sync as possible cluster/VM state.

### Logging-only changes

- `Build WOS`
- Verify no direct serial use was introduced outside allowed paths.
- Verify hot paths were not made unsafe by logging allocations/locks.

### WOS userspace utility changes

- `Build WOS`
- Check `configs/rootfs/aliases.tsv` when installed command names or symlinks are affected.
- For `strace`, `perf`, `top`, `memacc`, `journalctl`/`journald`, or `wkictl`, inspect the matching kernel ABI/procfs/device/syscall surface.
- Ask the user to run the utility inside WOS when behavior is runtime-only.

### libc/mlibc changes

- `Build WOS`
- Check syscall ABI wrappers.
- Check errno conventions.
- Ask user to run affected userspace program if needed.

## Final response format for checks

```text
Checks:
- Build WOS: pass/fail/not run
- Other checks: ...
- Runtime debug: user-run required/not required
```
