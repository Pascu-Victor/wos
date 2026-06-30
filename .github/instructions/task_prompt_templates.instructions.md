---
applyTo: "**"
---

# Prompt Templates for Codex Tasks

These templates are for users and agents to structure large WOS tasks. Use them as workflow patterns, not as source-of-truth facts.

## Reconnaissance-only task

```text
Do not edit files.

Goal:
Analyze <subsystem/problem/feature>.

Required output:
1. Relevant files grouped by subsystem.
2. Main entrypoints and public APIs.
3. Call graph or data-flow sketch.
4. Locks, lifetimes, allocation/blocking constraints, and unsafe contexts.
5. ABI, syscall, wire-format, or userspace compatibility surfaces.
6. Existing tests/build targets.
7. Docs/cache entries that appear stale or contradictory.
8. Minimal safe implementation strategy.

Rules:
- Source code is authoritative over docs.
- If docs conflict with code, report the conflict.
- Do not propose broad rewrites unless required for correctness.
```

## Critical-path implementation task

```text
Implement <change> with minimum surface area.

Before editing:
- Read AGENTS.md.
- Read relevant .github/instructions files.
- Inspect current source paths.
- Produce a patch contract.

Patch contract must include:
- Problem statement.
- Files expected to change.
- Files read.
- Invariants to preserve.
- Locks and unsafe contexts.
- Allocation/blocking assumptions.
- ABI/wire/syscall compatibility impact.
- Build/test plan.

During editing:
- Avoid unrelated cleanup.
- Preserve ABI/wire format unless explicitly required.
- Preserve logging conventions.
- Update comments/docs only when they would become misleading.

After editing:
- Run the relevant build/test command when available.
- Summarize changed files, invariants checked, checks run, and remaining risks.
```

## Review task

```text
Review this branch for correctness, not style.

Focus on:
- scheduler/preemption assumptions
- lock ordering
- blocking in unsafe contexts
- ABI/wire/syscall compatibility
- errno/error conventions
- lifetime and ownership
- use-after-free risk
- logging safety
- tests/build coverage

Output concrete findings with file/function references.
Do not suggest broad rewrites unless required for correctness.
```

## Cache maintenance task

```text
Update instruction/cache markdown for <subsystem>.

Rules:
- Read current source first.
- Keep facts checkable and path-specific.
- Include last-reviewed date.
- Mark stale or uncertain claims explicitly.
- Do not encode hidden reasoning; encode durable facts, invariants, entrypoints, and gotchas.
- Source code remains authoritative.
```

## WOS utility task

```text
Update or debug the WOS utility <name>.

Before editing:
- Read the utility CMakeLists.txt and src/main.cpp.
- Read configs/rootfs/aliases.tsv for installed command names.
- Read any kernel ABI/procfs/device/syscall surface the utility uses.
- Do not assume GNU/Linux/systemd/procps/upstream behavior from the command name.

After editing:
- Run Build WOS when available, or explain why not.
- If runtime behavior is involved, ask the user to run the utility in WOS and provide output.
- Report whether utility help/usage, aliases, and implementation remain consistent.
```
