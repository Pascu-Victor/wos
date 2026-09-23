# Goal 21: Modern Kernel Self-Protection Baseline

## Planning metadata

- **Target horizon:** one to two continuous weeks with section-mapping, relocation, and privilege-boundary workstreams.
- **Critical surfaces:** early boot, linker layout, page tables, CR0/CR4/EFER, syscall/interrupt assembly, usercopy, kernel stacks, panic/coredump, and WOSDBG.
- **Completion rule:** protection status must match measured CPU/PTE state; enabling flags without negative probes and symbolization evidence is insufficient.

## Copy-pastable goal command

```text
/goal Outcome: Boot WOS with section-correct kernel W^X, randomized kernel placement, SMEP/SMAP-mediated user access, guarded kernel stacks, and stack-canary protection while keeping panic, coredump, sanitizer, KCFI, and WOSDBG behavior exact. | Verification surface: ELF/linker and runtime page-table audits, repeated-boot slide sampling, controlled KTEST protection violations, Normal Build WOS under normal and diagnostic profiles, feature-present/absent QEMU boots, and coredump/WOSDBG symbolization at multiple slides. | Constraints: Preserve early boot, IRQ/NAPI, scheduler, page-fault, syscall, panic/emergency, KASan/KUBSan/KCFI, no-red-zone/no-SIMD, and usercopy semantics; never expose canaries, entropy, or relocation secrets. | Boundaries: Change kernel linker/boot mappings, runtime kernel-range APIs, CPU feature setup, usercopy gates, stack allocation, canary runtime, coredump/debug metadata, and tests; do not redesign userspace ASLR or unrelated MM policy. | Iteration policy: Land section metadata and audits first, then W^X, guards/canaries, relocatable image/range consumers, SMEP, SMAP around usercopy, and default policy, verifying each protection with a negative probe before proceeding. | Blocked stop condition: Stop only after the same indispensable platform/runtime blocker repeats for three goal turns, all safe QEMU/model/KTEST alternatives are exhausted, and the exact missing CPU trace or user action is stated.
```

## Local source evidence

`modules/kern/CMakeLists.txt` explicitly selects `-fno-stack-protector`. `modules/kern/linker.ld` fixes the image at `0xffffffff80000000`. `platform/mm/virt.opt.cpp` maps the kernel with a broad writable/executable kernel page type rather than final permissions derived from ELF/linker sections. CPU feature setup enables selected architectural features but has no complete SMEP/SMAP policy.

Kernel and WOSDBG code contains fixed higher-half range assumptions. Goal 15 hardened userspace layout only; Goal 01 supplies the bounded usercopy seam required to make SMAP tractable.

## Measurable completion contract

1. Linker/boot metadata describes text, rodata, data, bss, init, trampoline, sanitizer, and relocation ranges without overlaps or writable text aliases.
2. After initialization, text is executable/read-only, rodata is non-writable/non-executable, data is writable/non-executable, and obsolete writable aliases are removed.
3. Kernel placement varies across repeated boots with bounded unbiased entropy and a deterministic test-only mode; no fixed-base consumer remains unexplained.
4. Every CPU enables a compatible SMEP/SMAP policy before ordinary task execution, or reports a precise unsupported/fallback state.
5. User memory is accessible only inside the established usercopy window, with nesting and interrupt behavior proven for all syscall/signal/ptrace paths.
6. Every kernel stack has an unmapped guard and correct lifetime through scheduler migration, handoff, exit, and coredump collection.
7. Stack canaries are initialized from kernel entropy per task/CPU policy and corruption reaches a safe panic path without leaking the value.
8. Ptrace, perf, coredumps, runtime image catalogs, and WOSDBG carry the kernel slide/build ID and symbolize exactly.
9. Negative probes for text write, data execution, direct user dereference, stack guard, and canary corruption fail in the intended controlled way.

## Invariants, locks, and unsafe contexts

Early boot cannot depend on final allocators or journal logging. SMAP gates must be balanced across faults, interrupts, and partial usercopy. Stack guards and relocation cannot invalidate assembly frame layouts or `rsp0`. All CPUs must agree on policy. Kernel text may be writable only in an explicitly bounded pre-finalization or patch window that is closed and audited.

## Boundaries and overlap

In scope are kernel image/stack protection and the direct tooling consumers of relocation state. Out of scope are userspace ASLR, module loading, speculative-execution mitigations beyond needed SMEP/SMAP setup, and unrelated scheduler policy. Serialize shared entry/return assembly changes with Goal 16 regression work.

## Parallel workstreams

The primary agent owns the threat model, boot sequence, assembly contract, and integration. Subagent A handles linker sections, W^X, and guards. Subagent B handles relocation and fixed-range consumers. Subagent C handles CPU policy, SMAP/usercopy, canaries, and negative KTESTs. WOSDBG changes may be a disjoint fourth lane.

## Verification, iteration, and rollback

Run formatting, Normal `Build WOS`, diagnostic KTEST builds/runs, ELF/PTE audits, multiple QEMU CPU profiles, repeated slides, coredumps, and WOSDBG semantic tests. Introduce a documented diagnostic rollback policy before default enablement. Never roll back by claiming a protection that is inactive or by disabling validation. Use the `/goal` blocked condition exactly.

## Completion record

**Status: Completed (2026-09-23).** The original goal command above remains
verbatim. The active goal appended: `I am able to run and debug WOS live without
root access by appending --no-setup to the wos-ktest and wos-cluster scripts.`

The kernel now boots as a randomized ET_DYN image with section-derived W^X,
read-only/NX direct aliases, CR0.WP and NX auditing, per-CPU SMEP/SMAP policy,
AC-gated pinned usercopy, guarded managed and emergency stacks, and an
entropy-backed GS stack canary with a lock-free emergency failure path. Runtime
image catalogs, coredumps, ptrace/procfs consumers, and WOSDBG use exact image
ranges and authenticated build IDs; raw QEMU debugging discovers a read-only
metadata descriptor at a stable diagnostic address without exposing entropy or
the canary.

Static ELF auditing found distinct R+X, R, and RW PT_LOAD segments, no W+X
segment, ordered linker ranges, and 8,017 GS-relative canary loads. Repeated
rootless KTEST boots exercised randomized and deterministic placement plus CPU
feature-present and feature-absent policy. Controlled probes covered text
writes, data execution, direct user dereferences, stack guards, and canary
failure. The final randomized rootless run at `0xffffffffba547000` completed
66,679 tests with zero failures. Normal `Build WOS`, the diagnostic build,
cluster host tests, WOSDBG live semantic/incident tests, and two-slide coredump
symbolization passed. These live runs used `bin/wos-ktest --no-setup`; the same
rootless network backend is shared by `bin/wos-cluster --no-setup`.

One pre-existing WOSDBG source-interface test still reports that the root agent
guidance lacks the documentation token `Analysis Tools`; it is unrelated to the
kernel protection implementation. The standalone upstream LLVM lit test could
not be run because this tree has no configured lit site file, so compiler
coverage is supplied by the linked-kernel audit and the live canary-corruption
probe.
