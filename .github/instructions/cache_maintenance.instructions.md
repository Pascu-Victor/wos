---
applyTo: ".github/instructions/**"
---

# Instruction and Cache Maintenance Rules

Reviewed: 2026-06-01.

Instruction files are meant to reduce repeated research, not replace source inspection.

## What belongs in instruction/cache files

Good entries are factual, durable, and checkable:

- Subsystem purpose.
- Source entrypoints.
- Public APIs.
- Wire/syscall/ABI constants.
- Locking rules.
- Unsafe contexts.
- Error conventions.
- Known gotchas.
- Build/test/debug workflows.
- Rootfs-installed command names and aliases.
- Local module/submodule inventory.
- How to verify the claim.

Bad entries are vague or unverifiable:

- “Think deeply about X.”
- “Remember the architecture.”
- “This should probably work.”
- Unverified guesses about intent.
- Assuming familiar utility names imply GNU/Linux/systemd/procps/upstream behavior.

## Required stale-safety header

Every cache-like instruction file should include:

```md
Generated or reviewed: YYYY-MM-DD.
Source code is authoritative. Local branch may differ from public main. Verify before editing.
```

## Updating cache files

When updating cache files:

- Read local source first.
- Prefer exact paths, function names, constants, and invariants.
- Verify module inventory against `modules/CMakeLists.txt`.
- Verify installed WOS utility names against `configs/rootfs/aliases.tsv`.
- Verify local submodules against `.gitmodules` and local directories.
- Mark public-main-only observations as public baseline.
- Mark local/unpushed observations as local if known.
- Do not silently erase old warnings unless source proves they are obsolete.
- Keep path-specific instructions scoped with `applyTo` front matter.

## Conflict handling

If source contradicts cache:

1. Follow source for the code task.
2. Mention the conflict in the final response.
3. Update cache only if the task includes cache/documentation maintenance.

## File layout rule

Keep `AGENTS.md` at repository root. Keep all other markdown instruction/cache files under `.github/instructions/`.
