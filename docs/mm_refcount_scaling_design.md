# MM Refcount Scaling Design

Status: instrumentation-first (`?`). No refcount storage change is safe in this
tranche.

Date: 2026-06-07

## Scope

This note covers tranche 17, "Sharded or Per-CPU Refcounts". It was written
while the git index had staged changes in the MM implementation files. The
clean-index follow-up added relaxed global refcount operation, retry,
zero-candidate, validation-failure, batch, and freed-page counters, but still
does not change refcount storage.

The goal is to define the exact-zero protocol and the smallest instrumentation
needed before any storage change. The current single atomic refcount remains
the only production-safe owner of final-free decisions.

## Patch Contract

Problem:
COW-heavy workloads may contend on hot frame refcounts, but final free needs
exact zero and unique ownership of the physical free.

Expected files to change when the index is clean:
`docs/mm_refcount_scaling_design.md` first. Future instrumentation may touch
`modules/kern/src/platform/mm/page_alloc.hpp`,
`modules/kern/src/platform/mm/page_alloc.cpp`,
`modules/kern/src/platform/mm/phys.hpp`,
`modules/kern/src/platform/mm/phys.opt.cpp`,
`modules/kern/src/platform/mm/virt.opt.cpp`,
`modules/kern/src/vfs/fs/procfs.cpp`, `modules/perf/src/main.cpp`, and
`modules/kern/src/test/mm_ktest.cpp`.

Files already read:
`AGENTS.md`, `.github/instructions/critical_paths.instructions.md`,
`.github/instructions/kernel.instructions.md`,
`modules/kern/src/platform/mm/page_alloc.hpp`,
`modules/kern/src/platform/mm/page_alloc.cpp`,
`modules/kern/src/platform/mm/phys.hpp`,
`modules/kern/src/platform/mm/phys.opt.cpp`,
`modules/kern/src/platform/mm/virt.opt.cpp`, and
`modules/kern/src/test/mm_ktest.cpp`.

Invariants to preserve:
COW correctness, final-free uniqueness, no ABA on zero, batch-free safety,
metadata reset before reuse, no page-table or kmalloc/slab frame reclamation via
data-page refcount paths, and no ABI or wire-format change.

Locks involved:
The current hot refcount operations use atomic side-table entries without the
allocator lock. Only zero-ref candidates enter the owning `PageAllocator` lock,
where `free_order0_at()` or `free_order0_range_at()` validates order-0 metadata
before returning pages to the buddy lists. Any future sharded protocol must keep
one lock order: refcount state is sealed first, then the owning allocator lock is
taken for physical free.

Unsafe contexts:
Refcount helpers are reachable from page faults, fork/COW pagemap cloning,
`destroy_user_space()` and its budgeted reaper, PTE unmap/drop helpers, and
kernel tests. They must not allocate, block, log routinely, call VFS, or wait on
scheduler-visible work. Short atomic spin loops are already used, but any new
drain path must be bounded or explicitly user-run benchmarked.

Allocation/blocking assumptions:
No heap allocation is allowed in the refcount hot path or final-free validation.
Final free may take the existing allocator IRQ-disabling lock. A drain protocol
may spin on per-page refcount state only if it cannot depend on another blocked
thread making progress while the allocator lock is held.

ABI/wire/syscall compatibility impact:
None for a doc-only tranche. Future counter exposure through `/proc/kcpustat` or
`perf cpustat` is a WOS diagnostic surface and must be appended/versioned with
the local utilities updated together.

Build/test plan:
For this tranche, run markdown whitespace checks only. If instrumentation lands
later, run format checks for touched C++ files, build the kernel target through
the WOS build path, and ask the user to boot KTEST plus fork/COW stress.

Rollback plan:
Remove this document or the future counters. No behavior changes are made here.

## Current Refcount Shape

The current implementation has a simple and strong safety boundary:

- `PageAllocator::alloc()` and the staged `alloc_order0()` initialize live pages
  with `PageKind::NORMAL` and refcount `1`.
- `phys::page_ref_inc()` and `phys::page_ref_add()` use CAS loops and reject
  resurrection from zero.
- `phys::page_ref_dec()` first atomically decrements the page refcount. Only the
  thread that observes `NEXT_REF == 0` creates a zero-ref candidate.
- `phys::page_ref_dec_batch()` performs the same atomic decrement per page,
  sorts zero-ref candidates by allocator and index, then frees contiguous
  order-0 runs under the owning allocator lock.
- `PageAllocator::free_order0_at()` and `free_order0_range_at()` are the last
  line of defense. They reject non-order-0 leaves and still run live medium and
  large-kmalloc guards before metadata is reset to `FREE` and refcount `0`.
- COW fault handling reads the refcount for promotion, pins the old page before
  copying, rereads the PTE for racing COW resolution, then drops the pin and the
  old PTE reference in order.
- COW fork cloning marks writable source and destination PTEs read-only+COW and
  increments the shared backing frame through a `PageLookupHint`.

This shape must not be weakened. In particular, a page with distributed
refcount state must not become buddy-reusable until all distributed state for
the page's current allocation generation has been sealed, folded, and observed
as exact zero.

## Why Full Replacement Is Not Yet Safe

Replacing `page_refcounts[idx]` with per-CPU shards is not an obvious drop-in
change for WOS today.

- `page_ref_get()` is a public kernel helper and currently returns an exact
  refcount. The COW fault path uses `REFCOUNT <= 1` to promote instead of copy.
  An overestimate is safe but changes behavior; an underestimate is a COW
  correctness bug.
- `page_ref_dec()` returns the new refcount. Tests assert exact values, and
  callers may rely on zero meaning "this call performed the last decrement".
- References are not CPU-affine. A ref incremented on one CPU can be decremented
  on another CPU, so naive positive per-CPU counters cannot know which shard to
  subtract from.
- Signed per-CPU deltas can represent cross-CPU decrement traffic, but final
  zero then requires a sealed drain. Without an in-flight operation barrier, a
  drainer can miss a shard update that raced with the seal and free a live page.
- ABA on zero matters because freed physical pages can be reallocated quickly.
  Stale shard state from the old allocation generation must not be foldable into
  the new allocation.
- Batch final-free currently depends on sorting zero-ref candidates and freeing
  contiguous order-0 runs under the allocator lock. Any sharded design must
  preserve that batching after exact zero, not before.

Recommendation: do not replace the canonical atomic refcount in tranche 17.
Start with instrumentation and, if storage changes later, use a conservative
augmentation that keeps a canonical exact-free owner.

## Exact-Zero Protocol

The only acceptable storage-change direction is a sealed-drain protocol. It may
augment the current atomic count with per-CPU state, but final free remains a
single-owner transition.

Required future metadata per page:

- `generation`: incremented on every allocation before publication and on final
  free before reuse. Shard entries must be associated with this generation or be
  cleared while the allocator lock still owns the page.
- `state`: one of `FREE`, `OPEN`, `DRAINING`, or `FREEING`.
- `canonical`: the central count used for exact operations. In a biased design,
  this may include reserved but unused per-CPU increment credits.
- `active_fast_ops`: an in-flight counter or equivalent per-page/per-generation
  barrier for shard operations.
- `cpu_credit[]` or `cpu_delta[]`: optional per-CPU storage. It must be signed
  if decrements are distributed, or credit-based if only increments are
  amortized.

Allocation protocol:

1. Under the owning allocator lock, initialize flags, kind, caller provenance,
   generation, shards, `canonical = 1`, and `state = OPEN`.
2. Publish the page only after metadata stores are release-visible.
3. `page_ref_inc()` must reject `FREE`, `DRAINING`, and `FREEING` unless it
   falls back to the exact slow path and proves a nonzero current generation.

Fast operation protocol for any shard update:

1. Load `state` and `generation` with acquire ordering.
2. If state is not `OPEN`, go to the exact slow path.
3. Enter `active_fast_ops`.
4. Re-read `state` and `generation`. If either changed, leave
   `active_fast_ops` and go slow.
5. Apply the per-CPU shard or credit update with release ordering.
6. Leave `active_fast_ops`.

The recheck is mandatory. It prevents a shard update from landing after a
drainer has already exchanged that shard and decided the page is zero.

Exact read or scalar decrement protocol:

1. CAS `state` from `OPEN` to `DRAINING`. If another CPU is draining, spin or
   use the exact slow path for that state; do not touch shards.
2. Wait until `active_fast_ops == 0`.
3. Fold every shard for the current generation into `canonical`, clearing the
   shard slots. A generation mismatch is corruption and must trap in debug
   builds.
4. For `page_ref_get()`, return the folded exact value and reopen the page if
   the value is nonzero.
5. For `page_ref_dec()`, subtract the caller's reference from the folded exact
   value. If the result is positive, store it in `canonical`, reopen the page,
   and return the exact result.
6. If the result is zero, transition to `FREEING`. Do not reopen.
7. Validate kind, order-0 metadata, generation, and zero canonical count under
   the owning allocator lock. Then use the existing `free_order0_at()` or
   `free_order0_range_at()` final-free path.
8. Final free resets kind/refcount metadata, clears shards, advances generation,
   and leaves the page in allocator-owned `FREE` state before it can be reused.

Batch decrement protocol:

1. Batch decrement may collect zero candidates only after each page has passed
   the exact scalar zero protocol or an equivalent grouped drain.
2. After exact zero, candidates may be sorted by allocator and index exactly as
   today.
3. Contiguous run freeing remains an allocator-lock operation.
4. A candidate invalidated by metadata validation must not be freed or counted
   as `pages_freed`; it should increment a diagnostic counter.

Important constraint:

Until the exact read and scalar decrement APIs are audited, only a credit-based
increment augmentation is a plausible first storage change. Sharding decrements
or returning conservative nonzero values from `page_ref_dec()` would alter the
current API and test contract.

## Minimal Instrumentation Before Storage Change

Counters are now present before any storage change. The goal is to measure
contention and zero-candidate behavior without making the refcount hot path
allocate or log.

Minimum global or per-CPU counters:

- `pa_ref_inc_ops`
- `pa_ref_inc_cas_retries`
- `pa_ref_add_ops`
- `pa_ref_add_refs`
- `pa_ref_add_cas_retries`
- `pa_ref_dec_ops`
- `pa_ref_dec_cas_retries`
- `pa_ref_dec_zero_candidates`
- `pa_ref_dec_zero_pages_freed`
- `pa_ref_dec_zero_validation_failed`
- `pa_ref_batch_calls`
- `pa_ref_batch_pages`
- `pa_ref_batch_zero_candidates`
- `pa_ref_batch_free_runs`
- `pa_ref_batch_pages_freed`

Optional call-site counters if overhead remains low:

- `cow_fork_ref_inc_pages` in `deep_copy_user_pagemap_cow()`.
- `cow_fault_pin_ref_inc_pages` in the COW copy path.
- `cow_fault_exact_ref_gets` for promotion decisions.
- `dus_refdec_batch_pages` and `dus_page_table_refdec_batch_pages` if the
  existing destroy-user-space counters do not already give enough separation.

Counter rules:

- Use relaxed atomics or per-CPU counters only.
- Count CAS retries in `page_ref_inc()`, `page_ref_add()`, and
  `page_ref_dec_atomic()` before adding any new storage.
- Count zero candidates at the point `NEXT_REF == 0` is observed.
- Count pages actually freed only after `note_zero_ref_pages_freed()` succeeds.
- Count validation failures when a zero candidate cannot be freed under the
  allocator lock.
- Expose counters only after reading the local `/proc/kcpustat` and `perf`
  rendering code. Treat field names as WOS utility compatibility surface.

## Validation Plan

Static and build checks for instrumentation:

- `git diff --check -- docs/mm_refcount_scaling_design.md`
- Format checks for touched C++ files.
- Kernel build through the WOS build path.

Required KTEST coverage before any storage change:

- Current refcount basics and lookup-hint tests.
- Batch final-free contiguous-run and duplicate/null-entry tests.
- Split versus unsplit large-allocation final-free tests.
- Page-kind protection tests for slab, medium, large-kmalloc, page-table, and
  normal pages.
- New tests for zero-candidate validation failure counters, CAS retry counters
  where practical, and exact `page_ref_get()` after mixed inc/dec traffic.

Required user-run runtime evidence:

- KTEST boot with instrumentation enabled.
- Fork/COW stress comparing pre/post counters.
- Cross-OS render or process-per-core workload with `/proc/kcpustat` deltas.
- Evidence that `pa_ref_dec_zero_candidates == pa_ref_dec_zero_pages_freed`
  except for intentionally injected protected/corrupt frame tests.
- Evidence that COW promotion behavior is unchanged when exact refcount reads
  are required.

## Current Recommendation

Keep tranche 17 implementation closed to source changes until the staged MM work
lands and the instrumentation counters above are added. If this document is
accepted, the first checklist item can be marked complete as a design artifact,
but the preservation, augment-versus-replace, and benchmark-cycle items should
remain open.

The next safe code tranche is instrumentation only. A storage change should
start with an increment-credit augmentation that keeps the canonical count as
the final-free authority; full signed sharding should wait until exact
`page_ref_get()` and `page_ref_dec()` semantics are either preserved by drain or
explicitly narrowed with tests and call-site audits.
