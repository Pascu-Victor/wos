# Per-CPU Page Cache Revival Audit

Status: safe revival implemented and runtime validated; continued soak and
before/after performance comparison remain useful.

Date: 2026-06-08

## Scope

This audit covers the per-CPU order-0 page cache in
`modules/kern/src/platform/mm/phys.opt.cpp` and the buddy metadata contracts in
`modules/kern/src/platform/mm/page_alloc.cpp`.

## Current State

`USE_PER_CPU_PAGE_CACHE` is now enabled, but the old bare-pointer cache path has
been replaced.

The live cache stores allocator/index pairs, not unproven pointers. A page can
enter the cache only through `PageAllocator` helpers that run under the owning
allocator lock and prove one of these states:

- A buddy order-0 free-list head is claimed into a cached order-0 state.
- A direct `page_free()` pointer is a live allocated order-0 head with refcount
  `1`.

Cached pages use a distinct allocator flag, `FLAG_CACHED_ORDER0`. They are free
capacity, have `PageKind::FREE` and refcount `0`, but are not linked into buddy
free lists and cannot be merged by buddy coalescing until deliberately drained
back.

The slow buddy path currently provides the safety properties callers rely on:

- `PageAllocator::alloc()` marks every page in the allocated block
  `PageKind::NORMAL` and sets every page refcount to `1`.
- `PageAllocator::free()` recovers the buddy order from the allocation head,
  then resets kind to `FREE` and refcount to `0` for every page it releases.
- Refcount final-release paths only call `free_order0_at()` or
  `free_order0_range_at()` after the page is known to be a zero-ref,
  independently freeable order-0 leaf.
- `page_split_to_order0()` is the explicit conversion step for multi-page
  allocations whose individual 4 KiB leaves will later be reclaimed through PTE
  teardown.

## Audit Conclusion

The previous unsafe revival strategy remains forbidden: do not cache bare
pointers or cache pages before allocator metadata proves order-0 eligibility.

The implemented strategy satisfies the metadata contract mechanically:

- Cache insertion resets kind/refcount/caller metadata before accounting the
  caller-visible free.
- Cache allocation restores allocated-head, `PageKind::NORMAL`, refcount `1`,
  caller/provenance metadata, KASan unpoisoning, and normal zeroing/full-overwrite
  semantics before exposing the page.
- Cache drain converts cached pages back through the buddy coalescing path
  without double-counting caller-visible frees.
- `/proc/kcpustat`, `perf cpustat`, and `/proc/memacc/zones` expose cache hit,
  miss, refill, drain, stale-entry, capacity, and cached-page counters.

## Revival Contract

Any future changes to this cache must preserve these invariants:

- Eligibility: only a live allocated head whose buddy order is exactly 0 may
  enter the cache. Multi-page allocations must never be cached through
  `page_free()` unless the free path can prove they were already split into
  independent order-0 leaves.
- Metadata on cache insertion: cached pages must no longer look live to public
  metadata readers. At minimum, `page_ref_get(page)` must return `0` and
  `page_kind_get(page)` must return `FREE` after `page_free(page)`, even if the
  page is retained by a per-CPU cache instead of the buddy free list.
- Metadata on cache allocation: a page popped from a cache must be restored to
  the normal allocation contract before it is returned: order-0 allocated-head
  state, `PageKind::NORMAL`, refcount `1`, caller/provenance state updated when
  enabled, KASan unpoisoned, and contents zeroed.
- Buddy isolation: a cache-owned page must not appear on any buddy free list and
  must not be mergeable by buddy coalescing until it is deliberately flushed back
  through one well-defined path.
- Accounting: allocation/free counters must count each logical allocation and
  release exactly once. If a cached page is later flushed to the buddy allocator,
  that flush must not double-count a free that was already accounted when the
  caller released the page.
- Locking: metadata transitions must use the owning `PageAllocator`
  synchronization. Future code needs a single documented lock order between the
  per-CPU cache lock and the allocator lock, including cache flush paths.
- Context safety: the fast path must preserve the existing allocator
  assumptions: no blocking waits, no heap allocation, no routine logging, and no
  new unsafe work in interrupt-disabled sections.
- Diagnostics: corruption traps for live slab, medium, and large kmalloc pages
  must remain effective for both cache insertions and cache pops.

## Test Coverage

Existing MM ktests already exercise much of the contract through public APIs:

- `RefCountBatchFinalFreeContiguousRun`
- `RefCountBatchFinalFreeWithLookupHint`
- `PageKindTracksSplitBatchFree`
- `UnsplitLargeAllocationContinuationIsNotLeafFreeable`
- `UnsplitLargeAllocationHeadIsNotLeafFreeable`
- `SplitLargeAllocationMakesAllLeavesIndependentlyFreeable`

`MM.PerCpuPageCacheRevivalOrder0FreeResetsMetadata` is the low-risk guard for
the direct `page_alloc()`/`page_free()` order-0 path. The test intentionally
uses only existing public APIs. It should keep passing with the cache disabled
and with the safe cache enabled, and it should fail if the old cache branch is
revived without resetting kind/refcount metadata on free and allocation.

## Runtime Validation

- User-run KTEST passed on 2026-06-08 with the cache enabled.
- User-run `cross-os-suite-20260608-020147/manifest.json` passed all WOS
  benchmark steps: `wos-mandelbench`,
  `wos-render-default-scene-node-threads`,
  `wos-render-default-scene-process-per-core`,
  `wos-render-duck-node-threads`, and
  `wos-render-duck-process-per-core`.
- `perf cpustat` after-samples for that suite show the cache enabled, nonzero
  hit/miss/refill/free/drain activity, and `stale=0` in every captured sample.
  The heaviest case, `wos-render-duck-process-per-core`, aggregated
  `hit=228098`, `miss=128882`, `refill_pg=224385`, `free=4333`,
  `drain_pg=592`, and `stale=0` across the four WOS host after-samples.
- User-captured `/proc/memacc/zones` on the enabled-cache kernel listed 10
  zones with `free_count_mismatch=0`, `invalid_allocator=0`, `bad_order=0`, and
  `free_pages=scanned_free_pages`. The snapshot had `cached_order0_pages=0`,
  which is acceptable for an idle/drained capture because active cache use was
  already proven by the `perf cpustat` counters.
- User-run serial fork/COW samples completed on the enabled-cache source line,
  including `32MiB`, `children=32`, `iterations=100`,
  `write_pages_per_child=256` at `1609.21 forks/s` and
  `411956.86 COW writes/s`.

Remaining useful validation:

- Capture a deliberate allocation/free microburst followed immediately by
  `/proc/memacc/zones` if a nonzero `cached_order0_pages` snapshot is desired.
- Run a strict before/after comparison for the full-overwrite COW helper and
  per-CPU cache separately if performance attribution matters.
