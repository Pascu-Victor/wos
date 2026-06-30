---
applyTo: "**"
---

# Agent Workflow for WOS

WOS is high-complexity and actively changing. Do not edit from a single grep result when a task crosses subsystem boundaries.

## Task classification

Use the smallest safe workflow that fits the task.

### Small task

Examples: spelling fix, obvious one-file constant rename, isolated comment correction.

Required behavior:

- Read the directly touched file.
- Check nearby code for conventions.
- Make a minimal patch.
- Report files changed and checks run.

### Normal task

Examples: small feature, bug fix with 2-6 files, local API adjustment.

Required behavior:

- Read relevant path-specific instruction files.
- Read caller and callee code around the change.
- Check build/test implications.
- Avoid unrelated cleanup.

### Critical-path task

Examples: scheduler, syscall ABI, VFS path resolution, WKI transport/RX, WKI wire structs, remote VFS, remote compute, block/device proxying, locks/lifetimes, interrupt/panic paths, allocator changes, libc ABI/sysdeps.

Required behavior:

- Produce a patch contract before editing.
- Verify source code over cache notes.
- Check lock ordering, blocking, allocation, lifetime, error convention, and ABI/wire compatibility.
- Run the relevant build if available, or explain why not.

### WOS userspace utility task

Examples: `strace`, `wkictl`, `journalctl`/`journald`, `memacc`, `top`, `perf`, rootfs aliases, or userland tools that depend on WOS `/proc`, ptrace, WKI, journal, or perf surfaces.

Required behavior:

- Read the utility's `CMakeLists.txt` and `src/main.cpp`.
- Read `configs/rootfs/aliases.tsv` for installed command names and symlinks.
- Do not assume GNU coreutils, systemd, procps, Linux `perf`, or upstream `strace` option behavior from a familiar command name.
- Inspect the kernel ABI/procfs/device/syscall surface the utility consumes before changing either side.
- If runtime output is needed, ask the user to run the utility inside WOS and provide logs/output.

### Reconnaissance task

Examples: “understand this subsystem,” “why does this happen,” “find the right place to implement X,” or tasks that appear to require researching many files.

Required behavior:

- Do not edit files.
- Produce a bounded map of files, entrypoints, flows, risks, and a minimal strategy.
- Mention stale/conflicting docs.

## Before editing checklist

- Which subsystem owns this behavior?
- Which instruction files apply?
- Which files define the public API or ABI?
- Which code calls into this path?
- Which code is called from this path?
- Can this code run in interrupt, NAPI, scheduler, syscall, or panic context?
- Can it allocate?
- Can it block or spin-wait?
- Which locks are held when it is called?
- Are there wire structs, syscall numbers, errno conventions, or userspace compatibility constraints?
- Is this a WOS utility with a familiar external name whose local behavior must be verified?
- Does the installed command name come from `configs/rootfs/aliases.tsv` rather than the CMake target name?

## During editing

- Prefer source-local, minimal changes.
- Do not broaden locks without checking call contexts.
- Do not insert sleeps, blocking waits, heap allocation, or logging into unsafe contexts without proving safety.
- Do not change packed struct layout, syscall ABI, or wire constants accidentally.
- Do not “modernize” unrelated code.
- Do not replace carefully staged compatibility code unless the task requires it.

## After editing checklist

- Summarize changed files and rationale.
- List invariants preserved.
- List build/test commands run.
- If not run, say exactly why.
- Note stale cache/docs discovered.
- Identify user-run runtime/debug steps if needed.
