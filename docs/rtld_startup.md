# Runtime-linker startup milestones

WOS can trace native image startup through optional numeric milestones in the
initial auxiliary vector and the `PERF_TRACE` logging syscall. Images without
an active capture token take the normal path without issuing marker syscalls.
The kernel ABI and mlibc mirror share the version, auxiliary-vector tags and
stage IDs in `callnums/perf_trace.h`.

## Boundaries and ownership

The stages identify image entry, dependency loading, relocation and RELRO
completion, TCB installation, runtime-linker completion, CRT entry, and entry
and return from `main`. TLS-image initialization and constructors follow the
TCB boundary and precede runtime-linker completion. These are inclusive elapsed
intervals: they include faults, kernel work, recording and possible scheduling
or host delay. They do not measure exclusive userspace CPU time.

Each native exec attempt owns fixed trace state through request preparation,
worker-to-owner transfer, image commit and completion. Recursive shebang
attempts retain their original subjects and correlations. Final return assembly
emits `exec_image_owner` and `exec_user_return` only for the published current
task returning to that image's initial RIP. This timestamp precedes the first
userspace instruction and any instruction-fetch fault.

The kernel accepts numeric user markers only for the current process image,
its matching capture session and correlation, the supported version and stage,
and an allowed return location. Repeated stages are suppressed. Fork does not
inherit another image's pending marker. Ring and summary admission recheck the
capture session under their existing locks, preventing an old image from
contaminating a reset capture.

## Startup work

The runtime linker leaves anonymous BSS pages demand-zero, clearing only the
BSS portion of the last file-backed page. GNU hash lookup uses a missing Bloom
bit to reject an object before reading its bucket or chain; malformed Bloom
metadata retains the existing fallback lookup. Symbol eligibility, versions,
scope order, COPY exclusions and RTLD_NEXT retain their existing behavior.
libc++ links with `--as-needed` so dummy compatibility DSOs are retained only
when referenced.

The matching host tests execute actual loader and resolver code, including
ELF32/ELF64, dirty backing, protected-page Bloom misses, capture reset, image
ownership and failure paths. The tests establish those behaviors independently
of guest performance measurements.

## Qualification

The completed launch-overhead goal used private normal and diagnostic builds,
an isolated KTEST run and a four-node ordinary workload. The final KTEST
reported 66,705 assertions, zero failures and three existing skipped cases.
The ordinary comparison retained 3,200 profiling-off samples per cohort.

The final comparison changed serial FIFO output while retaining the same
userspace and loader payloads. Rejection-log median elapsed time fell from
1.250 to 1.060 ms, and the tail from 1.293 to 1.125 ms. The balanced placement
launch median of eight group medians fell from 3.900 to 3.763 ms. Direct,
`locally` and `homeward` medians increased slightly in the sequential comparison.
These observations do not establish a general startup speedup, a causal
runtime-linker gain, or a matched Linux ratio. Ordinary placement launches
still exceed roughly 1 ms.

The final proof is retained locally at
`build/goal48-launch-performance-result-v87/goal-result-proof.json`.
Superseded handoffs and raw experiment archives were pruned during worktree
cleanup on 2026-10-05; the receipt summarizes historical validation. See
[perf capture](perf_capture.md) for event-stream completeness and observer
semantics; a complete drain does not by itself establish complete semantic spans.
