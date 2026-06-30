# MM Full-Overwrite Allocation Plan

## Scope

This note started as a patch plan for tranche 12 while the index had staged MM
changes. The clean-index follow-up implemented the narrow source slice:
`phys::page_alloc_full_overwrite_page()` now skips allocator zero-fill for
`PAGE_SIZE` callers that fully overwrite before exposure, and non-zero COW copy
faults use it.

## Current State

- `phys::page_alloc()` is still the default zeroing API. It allocates from the
  staged order-0 fast path when possible, falls back to the general buddy path,
  records accounting/caller stats, checks the slab double-alloc sentinel, zeroes
  the returned range, and unpoisons KASAN shadow.
- `PageAllocator::alloc()` and the staged `PageAllocator::alloc_order0()` set
  the live metadata before the public zeroing wrapper runs: buddy flags,
  `PageKind::NORMAL`, refcount `1`, and caller provenance when enabled.
- Page-table allocation now uses `alloc_zeroed_page_table()`. These allocations
  are not full-overwrite candidates because most page-table callers initialize
  only selected entries and rely on the rest being zero.
- The COW copy-fault path has the intended proof shape for non-zero pages:
  `old_virt` is pinned, the new page is not placed into the PTE until after the
  whole-page copy, and the racing-COW recheck discards the private page if
  another CPU already resolved the mapping.
- Zero-page COW still depends on receiving a zeroed private page and intentionally
  skips the copy.

## Patch Contract

Problem:
Zero-on-allocation is wasted when a non-zero COW copy fault overwrites the entire
destination page before making it reachable through a PTE.

Expected files to change when the index is clean:
`modules/kern/src/platform/mm/phys.hpp`,
`modules/kern/src/platform/mm/phys.opt.cpp`, and
`modules/kern/src/platform/mm/virt.opt.cpp`.
`page_alloc.cpp`/`page_alloc.hpp` should not need changes unless the staged
order-0 fast path is revised before this tranche resumes.

Files read:
`AGENTS.md`, `.github/instructions/critical_paths.instructions.md`,
`.github/instructions/kernel.instructions.md`,
`modules/kern/src/platform/mm/phys.hpp`,
`modules/kern/src/platform/mm/phys.opt.cpp`,
`modules/kern/src/platform/mm/virt.opt.cpp`,
`modules/kern/src/platform/mm/page_alloc.cpp`, and
`modules/kern/src/platform/mm/page_alloc.hpp`.

Invariants to preserve:
`phys::page_alloc()` remains zeroing. Unzeroed pages are returned only to callers
that fully overwrite a `PAGE_SIZE` page before PTE exposure. Allocator metadata,
refcount, page kind, caller provenance, accounting, OOM diagnostics, and
discard/free behavior remain identical to normal allocation. Zero-page COW keeps
using the zeroing path.

Locks involved:
Only existing allocator locks are used: zone allocator `lock_irq()` in
`find_free_order0_block()` / `find_free_block()`, plus current refcount atomics.
The COW path should not acquire new locks around the copy or PTE update.

Unsafe contexts:
The helper is for the existing page-fault COW path. It must not be used for page
tables, lazy zero-fill mappings, kmalloc/slab, interrupt handlers, panic paths,
or callers that may expose or read the destination before a full write.

Allocation/blocking assumptions:
No new blocking beyond the current physical allocation. The helper should not
add logging except for the existing OOM path inherited from `page_alloc()`.

ABI/wire/syscall compatibility impact:
None. This is kernel-internal and does not alter PTE layout, syscall ABI, wire
formats, or userspace-visible utility output.

Build/test plan:
Run the repo formatter/check on touched files, then build the kernel target via
the WOS build path. Runtime validation needs user-run KTEST and fork/COW stress,
because kernel/cluster debugging is user-run by policy.

Rollback plan:
Switch the COW allocation expression back to
`phys::page_alloc(paging::PAGE_SIZE, "...")`.

## Minimal Implementation Strategy

1. Add a deliberately narrow public-in-kernel wrapper in `phys.hpp`, for example:
   `auto page_alloc_full_overwrite_page(std::string_view name = "full_overwrite") -> void*;`
   Avoid a size parameter unless a later caller proves a multi-page full
   overwrite; COW only needs one page.
2. In `phys.opt.cpp`, factor the slow/common body of `page_alloc()` into an
   anonymous-namespace helper with a zeroing policy. The default `page_alloc()`
   should call it with `ZeroReturnedRange::Yes`.
3. Implement `page_alloc_full_overwrite_page()` by calling the same helper with
   `ZeroReturnedRange::No` and `size == paging::PAGE_SIZE`.
4. Even in the no-zero policy, keep all non-zeroing side effects:
   accounting, caller stats, HHDM sanity check, slab double-alloc sentinel, OOM
   diagnostics, and KASAN unpoisoning. KASAN unpoisoning must happen before the
   COW `memcpy()` writes into the returned page.
5. In `virt.opt.cpp`, change only the non-zero COW copy allocation:
   use `page_alloc_full_overwrite_page("cow_copy")` when
   `DESTINATION_FULL_OVERWRITE_BEFORE_EXPOSURE` is true; keep
   `page_alloc(paging::PAGE_SIZE, "cow_zero")` for zero-page COW.
6. Keep the existing COW ordering:
   pin old page, allocate destination, copy the full page, re-read PTE, discard
   on racing resolution, then install the new frame and release refs.
7. Do not convert page-table allocation in this tranche. The existing
   `alloc_zeroed_page_table()` helper is the correct page-table API until a
   separate page-table page cache or exact full-initialization proof exists.

## Implemented Source Slice

- `phys::page_alloc()` remains the default zeroing API.
- `phys::page_alloc_full_overwrite_page()` shares allocator metadata,
  accounting, provenance, OOM, stale-slab sentinel, and KASan unpoison behavior
  with the zeroing path, but skips zero-fill and the zeroing CR3 switch.
- Non-zero COW copy faults use the full-overwrite helper after pinning the old
  frame and before publishing the new PTE.
- Zero-page COW still uses the normal zeroing allocation path.
- Page-table allocation still uses `alloc_zeroed_page_table()`.

## Checklist Recommendation

Tranche 12's implementation checkbox can be checked after source review. Keep
runtime validation open until KTEST and fork/COW stress run on the new helper.
