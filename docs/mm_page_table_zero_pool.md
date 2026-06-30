# Page-Table Zeroed Page Pool Contract

Status: root-pagemap zero-pool source slice added (`?`). Booted KTEST and
benchmark validation are still required before marking the tranche complete.

Date: 2026-06-07

## Scope

This note covers the page-table allocation path in
`modules/kern/src/platform/mm/virt.opt.cpp` and the physical allocator behavior
in `modules/kern/src/platform/mm/phys.opt.cpp` and
`modules/kern/src/platform/mm/page_alloc.cpp`.

The current MM batch centralizes page-table allocation through
`alloc_zeroed_page_table()`, removes the old extra `memset()` calls, and adds a
small root-pagemap-only zeroed page pool. This document records the context
restrictions the pool must preserve and the work that remains before
intermediate page-table pages can use the same idea.

## Current Contract

`alloc_zeroed_page_table()` is the only helper that should allocate new page
tables in `virt.opt.cpp`. It calls `phys::page_alloc()` for one page, then marks
the allocation `PageKind::PAGE_TABLE` before the page is installed into any
page-table tree.

`phys::page_alloc()` remains a zeroed allocation API. It obtains exclusive
ownership of a block from the buddy allocator under the owning
`PageAllocator::lock_irq()`, releases that lock, validates the stale-allocation
sentinel, temporarily switches to `kernel_cr3` when needed, zeroes the block,
unpoisons KASan state when enabled, then restores the previous CR3.

Before `phys::set_kernel_cr3()` runs during MM init, allocator zeroing happens
under the current boot/kernel mapping. After `set_kernel_cr3()` runs, allocator
zeroing may switch to the kernel pagemap so HHDM writes are valid even when the
caller runs under a user pagemap.

`PageAllocator::alloc()` and `PageAllocator::alloc_order0()` return pages as
`PageKind::NORMAL` with refcount `1`. Page-table ownership is a separate
retagging step through `page_mark_kind()`. Final refcount release frees only
proven order-0 pages and resets released pages to `PageKind::FREE` with
refcount `0`.

`destroy_user_space()` and its budgeted variant reclaim user data first, then
page-table pages, then perform the final CR3 reload/TLB flush phase.
Intermediate page-table freeing currently returns pages to the generic physical
allocator. The root PML4 page is released afterward through
`virt::release_pagemap()`, which may cache a zeroed root page for later
`create_pagemap()` reuse.

## CR3 and Context Restrictions

Any future page-table-specific allocator must keep these restrictions explicit:

- Zeroing must use the same CR3 switch/restore rules as `phys::page_alloc()`.
  Do not add a separate zeroing path that writes HHDM memory while running under
  an arbitrary user CR3.
- Do not hold `PageAllocator::lock_irq()` across zeroing, KASan unpoisoning, or
  CR3 switching. The current allocator mutates metadata under the lock and
  zeroes only after the page is exclusively owned.
- Do not add heap allocation, blocking waits, routine logging, or new lock
  recursion to page-table allocation or final-free paths.
- A page-table page must be zeroed and tagged `PageKind::PAGE_TABLE` before it
  is published through a present page-table entry.
- A reused page-table page must not be reachable from any live CR3 while it is
  being zeroed or while it sits in a private pool.
- Any pool lock needs a documented order with the owning `PageAllocator` lock.
  Cache insert, cache pop, drain, and fallback-to-buddy paths must all use the
  same order.

## Implemented Root-Pagemap Pool

The implemented source slice caches only root pagemap pages after user-space
teardown has completed. Cached root pages stay allocated, retain
`PageKind::PAGE_TABLE`, keep refcount `1`, and are zeroed before entering the
private pool. `alloc_zeroed_page_table()` first tries this pool and falls back
to the generic zeroing allocator on a miss.

The pool rejects the kernel pagemap and the currently loaded CR3 root. It also
falls back to the old `phys::page_free()` behavior when a released candidate is
not a live page-table page with refcount `1` or when the fixed pool is full.

The implementation exposes additive `/proc/kcpustat` counters:
`pt_pool_capacity`, `pt_pool_cached`, `pt_pool_hit`, `pt_pool_miss`,
`pt_pool_release`, and `pt_pool_reject`.

## Why Intermediate Page-Table Pooling Is Not Yet Mechanical

There is no final-free metadata state for "cached zeroed page table" in the
generic refcount/buddy path. Today a released intermediate page-table page is
either a live typed page such as `PAGE_TABLE` or a buddy-owned `PageKind::FREE`
page. A private pool for intermediate pages would need a state that keeps cached
pages out of the buddy free lists and out of normal `page_alloc()` results
without making them look like reachable live page tables.

The current intermediate final-free API is generic. Page-table reclaim queues
zero-ref pages through `page_ref_dec_batch()`, and `free_order0_at()` /
`free_order0_range_at()` return them to the buddy allocator. A page-table pool
would need a page-table-specific release hook or a proven interception point
that preserves the existing final-free guards for slab, medium, large-kmalloc,
and corrupt page kinds.

The destroy path flushes TLBs after page-table refdec/free work. Before a freed
intermediate page-table page can be reused from a zeroed pool, the
implementation must prove that no live address space can still walk the old
table contents, including the budgeted destroy path and any remote CPU that may
still hold the old CR3.

## Minimal Future Implementation Contract

1. Add an explicit private-pool metadata state, or an equivalent side-table
   proof, for intermediate pages that are zeroed and reserved for page-table
   reuse.
2. Keep routing only `alloc_zeroed_page_table()` through page-table pools. Do not let generic
   `phys::page_alloc()` consume pool-owned pages as `PageKind::NORMAL`.
3. Add an intermediate page-table-specific release path only after teardown can
   prove the old table is unreachable or after reuse is deferred until the
   required TLB flush has completed.
4. Zero pages for the pool outside allocator locks using the current
   kernel-CR3 switch/restore behavior.
5. Keep the fallback path to `phys::page_alloc()` intact and preserve default
   zero-on-allocation behavior for all other callers.
6. Extend the low-overhead counters if intermediate-page pooling adds refill or
   drain behavior beyond the current root-pagemap pool.

## Validation Plan

Required static checks for source changes:

- `git diff --check -- modules/kern/src/platform/mm/virt.opt.cpp`
- `git diff --check -- modules/kern/src/platform/mm/phys.opt.cpp`
- `git diff --check -- modules/kern/src/platform/mm/page_alloc.cpp`
- Format checks for any touched C++ files.

Required kernel tests before marking the root pool complete:

- Page-table allocations are zero before first installation.
- Pool hits return pages tagged `PageKind::PAGE_TABLE`, refcount `1`, and not
  visible as `PageKind::NORMAL`.
- Pool pages are not on buddy free lists while cached.
- Root pagemap teardown does not cache the kernel pagemap or the currently
  loaded CR3 root.
- Existing page-kind, split-allocation, refcount-batch, COW, fork/exec, and
  destroy-user-space ktests still pass.

Runtime validation remains user-run: boot with MM ktests, then run fork/COW and
process-exit stress workloads while watching page-kind skip counters and any new
page-table-pool counters.

Current source checks passed:

- `git diff --check`
- `scripts/dev/format_repo.sh --check` on the touched kernel files
- `cmake --build build --target wos`

## Checklist Recommendation

Keep tranche 11 marked `?`.

The root-pagemap pool source item can be treated as started, but intermediate
page-table pooling should remain open until the release hook and TLB-lifetime
proof exist.
