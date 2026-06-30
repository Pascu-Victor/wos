# MM Zone Lookup Hint Expansion

This reconnaissance was written while the index was non-empty. The clean-index
follow-up implemented the safest source slice: `virt::collect_user_memory_stats()`
now carries a local `phys::PageLookupHint` through read-only refcount probes,
and the COW fault path reuses a local hint for the old frame's get/inc/dec
sequence.

## Current Hint Surfaces

- `phys::PageLookupHint` stores the last `PageAllocator*` plus the owner domain
  that proved it (`REGULAR_ZONE` or `HUGE_ZONE`). It is a short-lived lookup
  cache, not an ownership token.
- The hinted APIs are `page_kind_get()`, `page_ref_inc()`, `page_ref_add()`,
  `page_ref_dec()`, `page_ref_dec_batch()`, and `page_ref_get()`
  (`modules/kern/src/platform/mm/phys.hpp:78`).
- `find_allocator_for_page_cached()` first checks the hinted allocator and falls
  back to the full zone scan before refreshing the hint
  (`modules/kern/src/platform/mm/phys.opt.cpp:1271`).
- The fallback remains authoritative. A stale or wrong hint, including a hint
  whose saved owner domain no longer matches the allocator, only costs one
  failed containment check before the normal zone walk.

## Implemented Clean-Index Patch

Candidate: carry a `phys::PageLookupHint` through
`virt::collect_user_memory_stats()`.

Call chain:

- `/proc` process summary views call `virt::collect_user_memory_stats()` from
  `modules/kern/src/vfs/fs/procfs.cpp:574`, `:714`, `:833`, and `:1132`.
- `virt::collect_user_memory_stats()` walks user PML4/PML3/PML2/PML1 entries in
  `modules/kern/src/platform/mm/virt.opt.cpp:625`.
- Its local `count_present_leaf()` helper calls unhinted
  `phys::page_ref_get()` for each non-`PAGE_SHARED` present user leaf at
  `modules/kern/src/platform/mm/virt.opt.cpp:648`.
- This is structurally the older compact-stats twin of the staged memacc
  breakdown walk. The staged memacc change already creates one
  `PageLookupHint ref_lookup` per page-table walk and passes it into
  `add_present_leaf()` at `modules/kern/src/platform/mm/memacc.cpp:95`,
  `:112`, `:125`, and `:135`; the refcount probe uses it at `:76`.

Implemented:

- Add `phys::PageLookupHint ref_lookup{};` near
  `virt.opt.cpp:630`, before the `count_present_leaf` lambda.
- Capture it by reference in the lambda as today and change
  `phys::page_ref_get(virt_page) > 1` to
  `phys::page_ref_get(virt_page, &ref_lookup) > 1`.
- Added a block-local COW fault hint for the old frame's exact refcount read,
  pin increment, and old-frame decrements.
- No output changes are expected. The shared-page decision still uses the same
  refcount load and fallback lookup.

Safety notes:

- This path is diagnostic/procfs accounting, not final-free semantics.
- The hint lifetime is exactly one `collect_user_memory_stats()` call and one
  synchronous page-table walk.
- The hinted allocator is revalidated by `allocator_owns_page()` before use; if
  adjacent leaves cross zones, fallback lookup refreshes the hint.
- This does not affect huge-zone or regular-zone ownership rules. It only
  avoids repeated zone scans before read-only refcount loads.
- It preserves `/proc` output: `virtual_pages`, `resident_pages`,
  `shared_pages`, and `page_table_pages` are computed the same way.

## Implemented Owner-Domain Patch

The follow-up source slice makes the huge-zone/regular-zone distinction explicit
inside `PageLookupHint`:

- Added `phys::PageLookupOwner::{NONE, REGULAR_ZONE, HUGE_ZONE}` and stored it
  beside the cached allocator pointer.
- Split the allocator lookup fallback into an owner-producing helper.
- Changed hinted lookup to reuse a cached allocator only when the cached owner
  domain matches the allocator domain.
- Threaded the full hint through `page_ref_dec()` and `page_ref_dec_batch()`,
  rather than preserving only the allocator pointer in those paths.
- Added `MM/LookupHintOwnerMustMatchAllocatorDomain`, which corrupts the owner
  tag deliberately and, when the huge zone is available, reuses one hint across
  regular and huge pages.

Validation status: user-run KTEST and serial fork/COW stress passed on the
owner-domain hint kernel. The checklist item is complete for the explicit
regular-zone/huge-zone ownership rule.

## Added Compact Regular-Zone Index

The next source slice adds a fixed-size, sorted index over regular physical
zones:

- The index is built once after `phys::init()` finalizes the regular zone list.
- It contains regular zones only; the huge-page zone remains a separate
  explicitly checked owner domain.
- `page_free()`, `page_split_to_order0()`, and the refcount/kind fallback lookup
  use the indexed regular-zone dispatch instead of walking the linked list.
- Allocation policy still iterates zones, so first-fit/free-space behavior does
  not change.
- If the fixed index ever overflows during init, lookup falls back to the old
  linked-list scan.

Validation status: source is added but booted-kernel validation is pending.

Suggested patch contract if applied:

```text
Problem:
  collect_user_memory_stats() rescans zones for each non-shared user leaf even
  though adjacent leaves often belong to the same allocator zone.
Expected files to change:
  modules/kern/src/platform/mm/virt.opt.cpp
Files already read:
  AGENTS.md
  .github/instructions/critical_paths.instructions.md
  .github/instructions/kernel.instructions.md
  modules/kern/src/platform/mm/phys.hpp
  modules/kern/src/platform/mm/phys.opt.cpp
  modules/kern/src/platform/mm/memacc.cpp
  modules/kern/src/platform/mm/virt.opt.cpp
  modules/kern/src/platform/mm/page_alloc.hpp
  modules/kern/src/platform/mm/page_alloc.cpp
Invariants to preserve:
  Hints never outlive the stats walk; fallback lookup remains correct; huge-zone
  and regular-zone ownership checks stay inside phys lookup; refcount values and
  procfs output are unchanged.
Locks involved:
  No new locks. Existing page metadata refcount loads use acquire ordering.
Unsafe contexts:
  Procfs diagnostic path; can run while task/page-table state changes, but the
  proposed change only caches zone lookup, not page-table contents.
Allocation/blocking assumptions:
  No allocation and no blocking added.
ABI/proc impact:
  None. Same fields and formatting.
Build/test plan:
  Run format check for modules/kern/src/platform/mm/virt.opt.cpp and Build WOS
  or focused kernel build. Runtime memacc/procfs capture remains user-run.
Rollback:
  Remove the local PageLookupHint and call the unhinted page_ref_get().
```

## Other Candidate Chains

### COW Page-Fault Local Hint

Call chain:

- `pagefault_handler()` handles writable COW faults in
  `modules/kern/src/platform/mm/virt.opt.cpp:450`.
- The handler loads `REFCOUNT` with unhinted `page_ref_get(old_virt)` at
  `virt.opt.cpp:510`.
- On the multi-owner path it then calls unhinted `page_ref_inc(old_virt)` at
  `virt.opt.cpp:538`.
- Cleanup calls unhinted `page_ref_dec(old_virt)` at `virt.opt.cpp:549`,
  `:565`, `:588`, and `:589`.

Possible patch:

- Introduce `phys::PageLookupHint cow_lookup{};` after `old_virt` is computed.
- Use it for the same old page's `page_ref_get()`, `page_ref_inc()`, and
  old-page `page_ref_dec()` calls in that fault handling block.
- Keep `new_page` discard with a separate hint or unhinted call. It belongs to a
  freshly allocated page and is not part of the old-page lookup sequence.

Safety notes:

- This is a hotter path than procfs accounting and has concurrency-sensitive COW
  pin/recheck semantics. It is mechanically safe if scoped to lookup caching
  only, but it is not the lowest-risk tranche-14 follow-up.
- The hint must stay block-local and must not be stored in task or PTE state.
- Exact zero-ref behavior and the two old-page decrements must remain unchanged.

### Existing Destroy-Path Hinting

Already present in the staged tree:

- `DestroyUserSpaceBudgetState::RefdecBatch` owns a
  `phys::PageLookupHint lookup` at `virt.opt.cpp:1406`.
- `destroy_refdec_batch_flush()` passes the hint into
  `page_ref_dec_batch()` at `virt.opt.cpp:1451`.
- `FrameProbeCache` carries a `PageLookupHint` through frame ownership probes at
  `virt.opt.cpp:1180`, and the same cache reaches `page_ref_get()` and
  `page_kind_get()` at `virt.opt.cpp:1224`, `:1250`, and `:1267`.

Do not expand this further in the same tranche unless there is a clear measured
hot spot. It touches final-free semantics, page-table alias detection, and
zero-ref freeing. The staged code already does the important low-level grouping:
`page_ref_dec_batch()` sorts zero-ref pages by allocator and frees contiguous
runs at `phys.opt.cpp:1614` and `phys.opt.cpp:1525`.

### Cross-Subsystem Candidates To Defer

- `platform/dbg/coredump.cpp` has a repeated `phys_to_hhdm_checked()` walk that
  calls `page_ref_get()` at line `299`. This path is panic/debug adjacent and
  should be handled under coredump/debug ownership, not as a low-risk MM hint
  patch.
- `syscalls_impl/shm/shm.cpp` releases and attaches SysV shared-memory backing
  pages in loops at lines `146` and `269`. This is final-free and syscall
  behavior, so it is not a good tranche-14 "easy lifetime" candidate.
- `syscalls_impl/vmem/sys_vmem.cpp` has file-mmap cache refcount operations and
  zero-page bulk increments. Some could use hints, but they sit under syscall
  and cache-lock behavior and are outside the assigned write set.

## Checklist Recommendation

For Tranche 14:

- Leave "Use `PageLookupHint` in additional repeated operations where lifetimes
  are easy to prove" checked after the staged memacc and destroy-path hint work.
- Mark the low-risk repeated lookup item complete for
  `collect_user_memory_stats()` and COW old-frame refcount operations.
- Keep "Consider compact physical-address-to-zone indexing" open until zone
  count or profiling justifies a broader index.
- Keep "Keep huge-zone and regular-zone ownership rules explicit" complete
  after the owner-domain hint patch and booted-kernel validation.
