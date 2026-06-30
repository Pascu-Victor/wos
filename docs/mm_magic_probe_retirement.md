# MM Magic Probe Retirement Audit

Status: destroy-user-space fallback probes quarantined behind a diagnostic build
flag (`*` for pagemap teardown; allocator-owned corruption guards remain).

Date: 2026-06-08

## Coordination Note

Worker 4 could not edit live MM source in this pass because
`git diff --cached --name-only` was non-empty and included the assigned source
paths:

- `modules/kern/src/platform/mm/page_alloc.cpp`
- `modules/kern/src/platform/mm/page_alloc.hpp`
- `modules/kern/src/platform/mm/phys.opt.cpp`
- `modules/kern/src/platform/mm/virt.opt.cpp`

The clean-index follow-up implemented the source plan in two steps:
destroy-user-space first switched to one metadata-first classifier and exposed
`dus_magic_unknown_*` counters, then the remaining `UNKNOWN` content reads were
placed behind `WOS_MM_RECLAIM_MAGIC_PROBES`. Normal/control builds now skip the
fallback probes entirely; diagnostic builds can opt in to the old classifier.

## Scope

This audit covers the remaining MM "magic" probes that classify allocator-owned
frames by reading in-page headers. `PageKind` metadata is now the primary frame
classifier; magic probes are retained only as safety fallbacks, kmalloc tracker
integrity checks, or emergency diagnostics.

Do not remove the live slab, medium, or large kmalloc detection in final-free or
`page_free()` until the criteria below are satisfied with a replacement
diagnostic path. Pagemap teardown has already moved its fallback content reads
behind an explicit diagnostic option.

## Current Probe Inventory

| Area | File / helper | Probe | Current role | Retirement class |
| --- | --- | --- | --- | --- |
| Buddy final-free guard | `modules/kern/src/platform/mm/page_alloc.cpp`, `page_has_live_medium_alloc_magic()` | `MEDIUM_ALLOC_MAGIC` at page offset `+16` | Panics if a refcount final-free path tries to release a live medium kmalloc page. | Hot safety fallback. |
| Buddy final-free guard | `modules/kern/src/platform/mm/page_alloc.cpp`, `page_has_live_large_alloc_magic()` | `LARGE_ALLOC_MAGIC` at page offset `+16` | Panics if a refcount final-free path tries to release a live large kmalloc page. | Hot safety fallback. |
| Direct `page_free()` guard | `modules/kern/src/platform/mm/page_alloc.cpp`, `PageAllocator::free()` | Medium/large header magic | Catches accidental `phys::page_free()` on the header page of a still-live tracked kmalloc allocation. | Hot safety fallback. |
| Order-0 reclaim guard | `modules/kern/src/platform/mm/page_alloc.cpp`, `free_order0_at()` / `free_order0_range_at()` | Medium/large header magic | Catches leaf reclaim of live tracked kmalloc pages after refcount teardown. | Hot safety fallback. |
| Pagemap destroy classifier | `modules/kern/src/platform/mm/virt.opt.cpp`, `classify_live_allocator_frame()` | Medium/slab/large magic only when `PageKind` is `UNKNOWN` and `WOS_MM_RECLAIM_MAGIC_PROBES=ON` | Diagnostic opt-in for checking whether a corrupt user PTE points at a live allocator frame after metadata coverage should be complete. Normal builds skip the content read and count `UNKNOWN` as non-reclaimable. | Quarantined diagnostic fallback. |
| Kmalloc tier free | `modules/kern/src/platform/mm/dyn/kmalloc.opt.cpp`, `try_free_medium_alloc()` / `try_free_large_alloc()` | Medium/large header magic plus tracker-list membership | Determines whether a pointer belongs to the tracked medium/large tier and identifies double-free/list corruption. | Must remain until tracker keys change. |
| Kmalloc realloc/list diagnostics | `modules/kern/src/platform/mm/dyn/kmalloc.opt.cpp` | Medium/large header magic | Validates list nodes while moving allocations and while dumping corruption context. | Tracker integrity / debug dump. |
| Slab allocator integrity | `modules/kern/src/minimalist_malloc/slab_allocator.hpp`, `Slab::alloc_unlocked()` / `free_unlocked()` | Slab header magic | Validates slab headers and catches page-reuse UAF in the slab chain. | Must remain as slab-owned integrity. |
| Slab pointer classifier | `modules/kern/src/minimalist_malloc/mini_malloc.cpp`, `mini_get_slab_size()` / `mini_free()` | Slab header magic through `slab_ptr` | Validates that a small allocation pointer still belongs to a live slab. | Must remain as small-allocation integrity. |

## Current Classification Contract

`PageKind` owns frame classification for normal MM teardown:

- `PageAllocator::alloc()` initializes allocated pages as `PageKind::NORMAL`
  with refcount `1`.
- Page-table allocation marks pages as `PageKind::PAGE_TABLE`.
- Slab page acquisition marks backing pages as `PageKind::SLAB`.
- Medium and large kmalloc page acquisition marks backing pages as
  `PageKind::MEDIUM` or `PageKind::KMALLOC_LARGE`.
- Split multi-page allocations preserve kind/refcount/caller metadata across the
  exposed order-0 leaves.
- Buddy free paths reset released pages to `PageKind::FREE` and refcount `0`.

The remaining pagemap-destroy magic probes are now opt-in diagnostics. In
normal builds, `UNKNOWN` frames are counted and skipped without reading frame
contents. With `WOS_MM_RECLAIM_MAGIC_PROBES=ON`, pagemap teardown consults the
fallback only when the side-table kind is `UNKNOWN`; known nonmatching kinds
still skip the header read. In allocator final-free paths, magic checks remain
guards because the cost of a false free is much higher than the cost of a couple
of header reads.

## What Can Be Retired First

The pagemap destroy probes in `virt.opt.cpp` are the safest retirement target.
They are already quarantine-style fallbacks behind `PageKind::UNKNOWN`. A safe
transition is:

1. Add explicit fallback counters for `UNKNOWN` magic hits by tier. Done.
2. Run with counters enabled until all expected workloads report zero hits.
   Done for the captured render/mandel suite; `magic_*` counters stayed cold.
3. Convert `UNKNOWN` magic hits to a diagnostic-only mode in non-debug builds.
   Done via `WOS_MM_RECLAIM_MAGIC_PROBES`.
4. Keep a debug/test gate that panics or fails when a magic hit occurs after
   `PageKind` coverage is declared complete. The current guard test proves the
   default-off behavior and keeps the diagnostic branch compiling; panic-on-hit
   remains optional future work.

## Runtime Validation

- User-run KTEST passed on 2026-06-08 after the
  `DestroyUserSpaceUnknownMagicProbeIsDiagnosticOnly` synthetic-magic cleanup
  fix.
- User-run `cross-os-suite-20260608-024020/manifest.json` passed all WOS
  benchmark steps: `wos-mandelbench`,
  `wos-render-default-scene-node-threads`,
  `wos-render-default-scene-process-per-core`,
  `wos-render-duck-node-threads`, and
  `wos-render-duck-process-per-core`.
- `perf cpustat` after-samples for every step and WOS host reported
  `magic_reads=0`, `magic_slab=0`, `magic_medium=0`, and `magic_large=0`.
  That proves the normal default-off build no longer reads UNKNOWN-frame
  allocator magic during the covered reclaim workloads.
- Protected skip buckets stayed zero in the same after-samples:
  `unknown_skip=0`, `slab_skip=0`, `large_skip=0`, and `alias_skip=0`.
- The per-CPU order-0 cache remained active with `stale=0`, and deferred
  cleanup queues drained to `depth=0` in every after-sample.

The allocator guards in `page_alloc.cpp` should be retired later, if at all.
They protect final-free and direct `page_free()` paths from turning allocator
metadata bugs into silent physical memory reuse. Keep them until a cheaper
metadata-only guard can prove the same property under the owning allocator lock.

The kmalloc and slab internal magic checks are not retirement candidates in this
tranche. They validate allocator-private tracker state rather than classifying
arbitrary physical frames.

## Proposed Counters and Gates

Add counters before changing behavior. Candidate raw `/proc/kcpustat` keys:

- `dus_magic_unknown_medium_hit`
- `dus_magic_unknown_slab_hit`
- `dus_magic_unknown_large_hit`
- `pa_magic_medium_guard_hit`
- `pa_magic_large_guard_hit`
- `pa_magic_probe_reads`

The first three belong with destroy-user-space stats because they measure
fallback classification during pagemap teardown. The page-allocator counters
should be global allocator diagnostics or per-CPU counters; they should not log
on every read in hot paths.

Implemented build gate:

- `WOS_MM_RECLAIM_MAGIC_PROBES`: enables the pagemap-destroy `UNKNOWN` content
  reads and `dus_magic_unknown_*` tier hits for diagnostic builds. It defaults
  `OFF` and is forced `OFF` by `WOS_MM_PROVENANCE_PERF_CONTROL`.

Candidate future gates:

- `WOS_MM_MAGIC_PROBE_DIAGNOSTICS`: keep probes enabled and count reads/hits.
- `WOS_MM_MAGIC_PROBE_PANIC_ON_HIT`: panic on any fallback hit after the system
  is expected to have full `PageKind` coverage.
- `WOS_MM_STRICT_PAGE_KIND`: skip magic classification in pagemap teardown and
  treat `UNKNOWN` as non-reclaimable plus counted corruption/quarantine.

Do not add a gate that simply disables allocator guards without counters. A
silent disable would make regressions look like allocator performance wins until
they reappear as delayed memory corruption.

## Exact Next Patch Contract

Problem:
Hot reclaim and destroy-user-space paths should avoid repeated content-backed
classification now that `PageKind` is authoritative. Remaining content probes
should be counted, debug-gated, and treated as quarantine diagnostics.

Expected files to change:

- `modules/kern/src/platform/mm/virt.opt.cpp`
- `modules/kern/src/platform/mm/virt.hpp`
- `modules/kern/src/vfs/fs/procfs.cpp`
- optionally `modules/kern/src/platform/mm/phys.opt.cpp` and
  `modules/kern/src/platform/mm/page_alloc.cpp` for allocator-guard counters
  only after the destroy-user-space counter path is landed.

Files already read:

- `modules/kern/src/platform/mm/page_alloc.cpp`
- `modules/kern/src/platform/mm/page_alloc.hpp`
- `modules/kern/src/platform/mm/phys.opt.cpp`
- `modules/kern/src/platform/mm/phys.hpp`
- `modules/kern/src/platform/mm/virt.opt.cpp`
- `modules/kern/src/platform/mm/virt.hpp`
- `modules/kern/src/platform/mm/dyn/kmalloc.opt.cpp`
- `modules/kern/src/vfs/fs/procfs.cpp`
- `docs/mm_per_cpu_page_cache_audit.md`

Entrypoints:

- `virt::destroy_user_space()`
- `virt::destroy_user_space_budgeted()`
- `collect_page_table_frames()`
- `free_user_data_pages()`
- `free_page_table_pages()`
- `phys::page_ref_dec()` / `phys::page_ref_dec_batch()`
- `PageAllocator::free()`
- `PageAllocator::free_order0_at()`
- `PageAllocator::free_order0_range_at()`

Callers:
Scheduler/process teardown, GC pagemap cleanup, COW/refcount teardown, and
direct physical page free paths.

Callees:
`phys::page_kind_get()`, `phys::page_ref_get()`,
`phys::page_ref_dec_batch()`, `PageAllocator` order-0 free helpers, and
diagnostic log/panic paths on detected corruption.

Locks held:
Destroy-user-space classification does not hold the page allocator lock while
reading frame content. Refcount final-free groups zero-ref pages by allocator
and then holds the owning `PageAllocator::lock_irq()` while calling
`free_order0_at()` or `free_order0_range_at()`. Kmalloc tracker magic is guarded
by `medium_alloc_lock` or `large_alloc_lock`; do not reuse tracker internals
from VM teardown.

Can allocate/block/spin/log:
The classifier and counters must not allocate or block. The normal path should
not log. Existing corruption traps may log or panic only on impossible states,
matching current behavior.

ABI/wire/syscall impact:
No syscall or wire-format impact. Adding fields to `DestroyUserSpaceStats` and
new `/proc/kcpustat` key/value columns is a WOS diagnostic surface change; keep
existing keys and append new keys only.

Lifetime/ownership impact:
Do not reclaim `SLAB`, `MEDIUM`, `KMALLOC_LARGE`, or `PAGE_TABLE` frames as
data. `UNKNOWN` frames that hit fallback magic must be skipped/quarantined and
counted, not refcount-freed, until strict metadata-only mode has runtime proof.

Build/test plan:
Run the relevant format check for touched C++ files, then build the kernel.
Runtime validation must be user-run KTEST/cluster evidence with before/after
`/proc/kcpustat` snapshots.

Runtime/debug data needed from user:
KTEST output, boot logs for corruption traps, and `/proc/kcpustat` rows after
fork/COW-heavy and render/user teardown workloads.

Rollback plan:
Restore the current three-helper magic checks and remove the appended
`/proc/kcpustat` keys.

## Exact Counter Implementation Plan

The clean-index source pass landed destroy-user-space fallback counters,
de-duplicated repeated `UNKNOWN` content reads, and quarantined those reads:

1. In `virt.hpp`, appended fields to `DestroyUserSpaceStats`:
   `magic_unknown_probe_reads`, `magic_unknown_slab_hits`,
   `magic_unknown_medium_hits`, and `magic_unknown_kmalloc_large_hits`.
2. In `virt.opt.cpp`, added matching atomics to `DestroyUserSpaceStatsAtomic` and
   fields to `DestroyUserSpaceCallStats`; aggregate them in
   `note_destroy_user_space_stats()` and return them from
   `get_destroy_user_space_stats()`.
3. Replaced the three independent `frame_is_live_medium_alloc()`,
   `frame_is_live_slab_alloc()`, and `frame_is_live_large_kmalloc_alloc()` calls
   at destroy-user-space sites with one metadata-first classifier:

   ```text
   classify_live_allocator_frame(phys_addr, kind, cache, stats)
   ```

   The classifier:

   - return immediately from `PageKind::SLAB`, `PageKind::MEDIUM`, and
     `PageKind::KMALLOC_LARGE` without reading page contents;
   - return "none" immediately for every known nonmatching kind;
   - for `PageKind::UNKNOWN` with `WOS_MM_RECLAIM_MAGIC_PROBES=OFF`, returns
     "none" without content reads so the caller counts a normal `UNKNOWN` skip;
   - for `PageKind::UNKNOWN` with `WOS_MM_RECLAIM_MAGIC_PROBES=ON`, calls
     `phys_to_hhdm_for_live_probe()` once, counts one
     `magic_unknown_probe_reads`, reads the slab word and allocation magic at
     `+16`, counts exactly one tier hit if matched, and returns the tier;
   - never refcount-free a fallback magic hit.

4. In `procfs.cpp`, appended `/proc/kcpustat` keys after the existing `dus_*`
   fields:

   - `dus_magic_unknown_reads=`
   - `dus_magic_unknown_slab=`
   - `dus_magic_unknown_medium=`
   - `dus_magic_unknown_kmalloc_large=`

5. Keep existing `dus_unknown_skip`, `dus_slab_skip`, `dus_medium_skip`,
   `dus_kmalloc_large_skip`, and `dus_corrupt_skip` keys unchanged for tool
   compatibility.

After runtime evidence shows zero fallback hits,
add a debug-only strict gate:

```text
WOS_MM_STRICT_PAGE_KIND
```

When enabled, `PageKind::UNKNOWN` destroy-user-space frames should skip
content/magic classification entirely, increment `dus_unknown_skip` plus a
strict-mode skip counter if added, clear the PTE, and avoid refcount release.
Diagnostic builds may instead panic on a fallback magic hit with
`WOS_MM_MAGIC_PROBE_PANIC_ON_HIT`.

Allocator/refcount traps are a later pass. If counters are added there, keep the
metadata guard first:

- if `PageKind` is `SLAB`, `MEDIUM`, or `KMALLOC_LARGE`, trap without reading
  page contents;
- if `PageKind` is known nonmatching, do not probe;
- if `PageKind` is `UNKNOWN`, count the fallback content read and preserve the
  existing emergency panic on a magic hit.

## Safe Removal Criteria

Retire a probe only after all applicable criteria are true:

- Every allocation path that can expose a physical page to VM teardown marks an
  accurate `PageKind` before publication.
- Every release path resets kind/refcount metadata before the frame can be
  reused or cached.
- Refcount final-free releases only proven independently freeable order-0
  leaves; unsplit multi-page allocation heads and continuations are rejected.
- Kmalloc medium/large tracker list insertion sets magic and `PageKind` under a
  race-free contract; list removal clears magic before physical release.
- Slab page acquisition and slab page return preserve `PageKind::SLAB` while
  live and reset to `FREE` only through the physical allocator.
- Fallback counters report zero hits across KTEST and cross-OS stress suites.
- Existing skip buckets remain zero unless the workload intentionally injects
  corrupt mappings: `dus_unknown_skip`, `dus_slab_skip`, `dus_medium_skip`,
  `dus_kmalloc_large_skip`, `dus_corrupt`, and `dus_corrupt_skip`.
- A diagnostic replacement remains available in debug builds: either panic on
  fallback hit, or quarantine the frame and emit enough context to identify the
  owner and frame kind.

## Validation Plan

Required local/static checks for source changes:

- `git diff --check -- docs/mm_magic_probe_retirement.md`
- Format checks for any touched C++ source.
- Kernel build for any source counter or gate change.

Required KTEST coverage:

- Existing page-kind/refcount tests:
  - `MM.PageKindTracksSplitBatchFree`
  - `MM.UnsplitLargeAllocationContinuationIsNotLeafFreeable`
  - `MM.UnsplitLargeAllocationHeadIsNotLeafFreeable`
  - `MM.SplitLargeAllocationMakesAllLeavesIndependentlyFreeable`
  - `MM.PerCpuPageCacheRevivalOrder0FreeResetsMetadata`
- Add targeted tests before removal:
  - medium and large kmalloc pages have the expected `PageKind` while live;
  - slab backing pages have `PageKind::SLAB` while live;
  - `destroy_user_space()` does not need magic fallback for valid page tables;
  - injected `UNKNOWN` tracked frames are quarantined/counted, not reclaimed.

Required runtime/stress evidence:

- Boot and run all KTESTs with probes and counters enabled.
- Run a cross-OS stress suite comparable to
  `benchmarks/results/cross-os/cross-os-suite-20260607-195533/manifest.json`.
- Confirm protected/corrupt skip buckets remain zero for normal workloads.
- Capture `/proc/kcpustat` before/after for `dus_*` and proposed `*_magic_*`
  counters.
- Include render and fork/COW-heavy workloads, since pagemap teardown and
  refcount release are the paths most likely to expose stale classification.

## Current Recommendation

Keep all live probes for now. The next implementation tranche should validate
the new low-overhead fallback counters, add a strict debug gate if the counters
stay clean, then rerun KTEST and the cross-OS stress suite. If those counters
stay at zero, retire only the `virt.opt.cpp` `UNKNOWN` fallback probes first.
Leave kmalloc/slab internal magic checks in place as allocator-private integrity
checks.
