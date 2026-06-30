# MM Owned-Frame And Reclaim Backlog Design Notes

Status: backlog instrumentation implemented (`?`). Conservative owned-frame
tracking source slice implemented (`?`). No owned-frame reclaim consumption or
background-worker behavior change.

Date: 2026-06-08

## Coordination

`git diff --cached --name-only` was non-empty before this note was written, so
the first pass avoided staged paths and documented the current contracts for
Tranches 18 and 19. The clean-index follow-up added scheduler-GC deferred
cleanup backlog counters and idle/foreground pass counters only.

## Patch Contract

Problem: exit reclaim discovers frames by walking PTEs, and deferred scheduler
GC can accumulate transient pagemap reclaim backlog. Owned-frame tracking or a
background reclaim path would alter pagemap lifetime, frame ownership, and
scheduler cleanup ordering.

Expected files to change in this slice: `docs/mm_owned_frame_reclaim_design.md`
only.

Files read:

- `AGENTS.md`
- `.github/instructions/critical_paths.instructions.md`
- `.github/instructions/kernel.instructions.md`
- `modules/kern/src/platform/mm/paging.hpp`
- `modules/kern/src/platform/mm/phys.hpp`
- `modules/kern/src/platform/mm/page_alloc.hpp`
- `modules/kern/src/platform/mm/virt.hpp`
- `modules/kern/src/platform/mm/virt.opt.cpp`
- `modules/kern/src/platform/sched/scheduler.cpp`
- `modules/kern/src/platform/sched/scheduler.hpp`
- `modules/kern/src/platform/sched/task.hpp`
- `modules/kern/src/vfs/fs/procfs.cpp`
- `modules/perf/src/main.cpp`
- `benchmarks/cpustat_diff.py`
- `benchmarks/render_variance_report.py`
- `benchmarks/README.md`

Invariants to preserve:

- `PageTable` remains exactly one hardware page table page. `PageTable*` is the
  public pagemap handle, so future owned-frame state should live in side metadata
  keyed by the root pagemap, not inside `PageTable`.
- `PageKind::NORMAL` is the only data-page kind eligible for process teardown
  refdec/free. `PAGE_TABLE`, `SLAB`, `MEDIUM`, `KMALLOC_LARGE`, `UNKNOWN`,
  huge-page, corrupt, and page-table-alias cases must keep explicit skip paths.
- Threads and DAEMON tasks must not free a shared or kernel pagemap. Process
  pagemap cleanup remains gated by sibling checks.
- COW, read-only shared, file-backed, and aliased mappings are not exclusive
  pagemap-owned frames until the implementation can prove ownership transfer.
- Scheduler runqueue locks remain short-held. Pagemap walks, page refdec/free,
  `delete`, debug unregister, and VFS/log-heavy work stay outside runqueue locks.
- User-visible process semantics, wait/zombie behavior, and syscall ABI stay
  unchanged.

Locks involved:

- `RunQueue` locks protect runnable/wait/dead-list manipulation and detach
  decisions. Heavy cleanup intentionally happens after detach outside that lock.
- `global_task_registry_lock` protects active/PID registry access.
- Page allocator locks protect allocator metadata and refcount final free.
- Future owned-frame metadata needs one documented lock order with page-table
  mutation and allocator metadata. Do not introduce a lock taken under runqueue
  locks unless it is nonblocking and mechanically bounded.

Unsafe contexts:

- Scheduler task-state transitions and dead-list insertion.
- Page fault COW handling.
- Syscall and VFS/user-copy paths that may fault lazy stack or COW pages.
- Pagemap destruction and task cleanup after process exit.

Allocation/blocking assumptions:

- Future background reclaim may allocate queue nodes or metadata only before
  transferring ownership, or it must have a no-allocation fallback.
- GC worker cleanup may block/yield, but not while holding runqueue locks.
- Page fault ownership updates must not perform unbounded allocation or logging.

ABI/wire/syscall compatibility impact:

- None for this slice. Future observability exposed through `/proc/kcpustat`,
  `/proc/memacc`, or `perf cpustat` is a WOS utility compatibility surface and
  needs schema-preserving additions.

Build/test plan:

- Markdown-only change: no build required.
- Future scripts, if added, should pass `python3 -m py_compile`.
- Runtime validation remains user-run: boot/selftest, fork-COW stress, render
  stress, and before/after `perf cpustat` capture.

Rollback plan:

- Remove this document. No kernel state or behavior changes are introduced.

## Current Pagemap-Owned Frame Lifecycle

### Pagemap Creation And Mapping

- `create_pagemap()` returns a raw `PageTable*` allocated by
  `alloc_zeroed_page_table()`.
- `alloc_zeroed_page_table()` uses the opt-in reclaiming physical allocator and
  immediately tags the frame as `PageKind::PAGE_TABLE`.
- `PageTable` is a packed array of 512 `PageTableEntry` values with
  `static_assert(sizeof(PageTable) == PAGE_SIZE)`. There is no spare in-object
  room for owned-frame lists.
- `map_page()` and `map_page_batched()` create intermediate page tables as
  needed, then write a leaf PTE. They do not currently record reverse mappings
  or per-pagemap ownership.
- `reserve_page_range()` replaces a present leaf with `PAGE_RESERVED` and drops
  the old present-leaf ref.
- `unmap_page()` purges the leaf PTE, invalidates the TLB entry, and calls
  `drop_present_leaf_ref()`.

### COW And Fork

- `deep_copy_user_pagemap_cow()` allocates fresh page-table pages for the child
  and increments data-frame refcounts.
- Writable, non-shared mappings are converted to read-only `PAGE_COW` in both
  parent and child. These frames are not exclusive to either pagemap after fork.
- Read-only or `PAGE_SHARED` mappings remain shared and still get refcounted.
- Huge COW copies are split into independent 4 KiB allocations so teardown can
  later refdec leaves safely.
- COW write faults either promote a sole-owner page in place or allocate a new
  normal page, copy data when required, replace the PTE, and drop the old frame
  references.

### Destroy-User-Space

The current teardown path deliberately discovers ownership by walking page
tables:

1. Collect page-table frames for the pagemap tree.
2. Walk user leaf PTEs, count `dus_leaf`, and queue refdec only for
   non-page-table `PageKind::NORMAL` frames.
3. Skip and count huge pages, `UNKNOWN`, `SLAB`, `MEDIUM`, `KMALLOC_LARGE`,
   corrupt entries, and user PTEs aliasing page-table frames.
4. Purge visited PTEs.
5. Walk and refdec page-table pages separately.
6. Flush TLB.

The budgeted path has the same phases, but advances by a bounded number of PTE
steps and keeps state in `DestroyUserSpaceBudgetState`.

### Process Exit And Zombie Lifetime

- `wos_proc_exit()` now releases non-thread user address spaces before setting
  `exit_notify_ready` and waking waiters. It switches the current CPU to the
  kernel pagemap, clears `Task::pagemap`, destroys user mappings, then releases
  the root pagemap page.
- Waitable zombies keep only the `Task` fields needed by `waitpid()`:
  PID/parent metadata, exit status, rusage/accounting fields, and small
  scheduler/debug state. User heap, stack, mmap, code, and page tables should
  no longer remain attached to an unreaped zombie.
- Thread tasks still skip exit-time pagemap cleanup because they share the
  process pagemap. Kernel stack, thread object, and syscall scratch storage are
  still scheduler-GC work because the exiting task is running on that kernel
  stack until `jump_to_next_task_no_save()`.
- Scheduler waitable-zombie stripping remains a fallback and kernel-remnant
  cleanup path. If a zombie somehow reaches `DEAD` with `pagemap != nullptr`,
  GC may still destroy it after epoch/current-task guards prove the task is no
  longer executing.

## Current Scheduler/Deferred Reclaim Flow

- Exiting tasks are inserted into a dead list by paths such as
  `remove_current_task()`, `jump_to_next_task()`, and deferred task switching.
- Normal process exit releases the dying process's user pagemap before the task
  is published as waitable. The dead-list path should usually see
  `pagemap == nullptr` for an unreaped zombie.
- `insert_into_dead_list()` detaches the task from runnable/wait structures,
  marks it `DEAD_GC`, and queues it for GC.
- `detach_next_reclaimable_task_locked()` runs under a runqueue lock and only
  detaches tasks that are `DEAD`, epoch-safe, not current on any CPU, have
  refcount 1, and are not unreaped zombies whose parent is still active.
- The same detach path decides whether a process pagemap can be freed by
  checking active and dead siblings that share the pagemap.
- Heavy cleanup happens outside the runqueue lock.
- If a detached process owns a reclaimable pagemap, `queue_detached_gc_task_cleanup()`
  creates a `DestroyUserSpaceBudgetState` and pushes a deferred cleanup item.
- `process_deferred_gc_cleanup_slice()` keeps the oldest deferred item at the
  queue head until its budgeted `destroy_user_space_budgeted()` call completes.
  It then frees the root pagemap page and completes thread/task cleanup.
- Scheduler GC originally had two budget profiles:
  foreground `32` tasks, `500 us`, `64` pagemap steps; idle `256` tasks,
  `10000 us`, `4096` pagemap steps.
- A follow-up source slice adds a middle foreground-deferred profile used when a
  GC pass starts with queued pagemap cleanup: `128` tasks, `2000 us`, and `1024`
  pagemap steps. This is intentionally smaller than idle fast-reap but much
  larger than ordinary foreground cleanup, targeting COW variance where old
  address spaces were being split across many tiny destroy-user-space slices.
- Allocation pressure now has a stronger opt-in path: callers using
  `phys::page_alloc_with_reclaim()` run one exclusive scheduler-GC pressure pass
  before yielding, instead of only waking `sched_gc` and hoping it wins the race.
  The exclusive guard is required because deferred pagemap cleanup is still a
  single global linked queue.
- Fork producers also apply pressure backoff before child creation when the
  dead-task backlog is high or free pages fall below the low watermark. This is
  the first line of defense for COW storms, because COW write-fault handlers can
  arrive with interrupts disabled and must not block/yield inside the page-fault
  frame.

The queue is currently scheduler-GC-local serial work. It is observable through
aggregate phase timing, but not by explicit queue length, queued bytes/pages, or
oldest-item age.

## Existing Observability

`/proc/kcpustat` and `perf cpustat` expose:

- Scheduler GC pass/task/time counters: `gc_pass`, `gc_reclaim`, `gc_us`,
  `gc_max_us`.
- GC phase timing: `gc_task_us`, `gc_detach_us`, `gc_pm_us`, `gc_thr_us`,
  `gc_misc_us`, `gc_dbg_us`, plus max fields.
- Destroy-user-space timing: `dus_collect_us`, `dus_data_us`, `dus_pt_us`,
  `dus_tlb_us`, plus max fields.
- Destroy-user-space volume: `dus_calls`, `dus_leaf`, `dus_refdec`,
  `dus_freed`, `dus_ptrefdec`, `dus_ptfree`.
- Safety skips: huge, unknown, slab, medium, kmalloc-large, alias, corrupt.

`benchmarks/cpustat_diff.py` already summarizes before/after deltas from
benchmark manifests, and `benchmarks/render_variance_report.py` reuses that
parser for render timing and MM counter correlation.

## Observability Gap For Tranche 19

Existing counters can prove that GC and pagemap reclaim time happened, but they
cannot directly answer backlog questions:

- How many deferred pagemap cleanup items are waiting?
- How many normal data leaves, page-table pages, or refdec operations remain in
  the queue?
- How old is the oldest deferred pagemap cleanup?
- Did idle fast-reap drain the queue before peak memory pressure increased?
- Is high memory pressure caused by the queue, active COW sharing, or allocator
  fragmentation?

Low-risk instrumentation now adds counters rather than behavior:

- deferred cleanup items queued, completed, and current depth;
- max deferred cleanup depth;
- max observed wait time for a completed deferred cleanup item;
- number of budgeted pagemap slices run and slices completed;
- optional estimated remaining walk work by phase, if it can be computed without
  another page-table walk;
- `gc_idle_boost_pass` and `gc_foreground_pass`, so idle fast-reap can be
  measured separately from regular GC.

Those fields should be appended to existing `/proc/kcpustat` lines and rendered
by `perf cpustat` without removing old keys.

## Owned-Frame List Design Direction

The safest next structural design is side metadata:

- Keep `PageTable*` as the pagemap root and hardware page-table page.
- Add an internal `PageMapMetadata` side table keyed by root `PageTable*` or root
  physical frame.
- Store an intrusive list or counted set of frames whose current ownership is
  exclusive to that pagemap and whose `PageKind` is `NORMAL`.
- Keep COW/shared/file-backed/aliased frames out of the exclusive list until a
  later design can model shared ownership explicitly.
- On teardown, refdec the exclusive owned list first, then keep a conservative
  page-table walk fallback for skipped, shared, unknown, and debug validation
  cases.

Required lifecycle hooks:

- `map_page()` and `map_page_batched()`: add exclusive ownership only when the
  mapping is user, present, non-shared, non-COW, and the caller transfers a
  normal frame into the pagemap.
- `reserve_page_range()` and `unmap_page()`: remove owned membership before
  dropping the present-leaf ref.
- COW fork: remove or mark old exclusive membership when a page becomes COW in
  parent and child.
- COW write fault: add the newly allocated private normal page to the current
  pagemap, and remove this pagemap's membership from the old shared frame if it
  was present.
- Shared/file-backed mmap and aliases: do not add to the exclusive list without
  a separate shared reverse-map design.
- Destroy budget state: freeze or transfer the pagemap's owned list at detach so
  no other task can mutate it after sibling checks prove ownership.

Important failure mode: a missed removal is worse than a missed addition. A
missed addition only falls back to the current PTE walk. A missed removal can
double-refdec or free a frame still mapped elsewhere. The first implementation
should therefore prefer under-tracking and validation counters.

## Implemented Conservative Owned-Frame Source Slice

The 2026-06-08 source slice adds a bounded, fixed-probe, hash-sharded side
table in `virt.opt.cpp` keyed by physical frame. It records only frames that can
prove:

- the mapping is below the user/kernel split;
- the PTE is present and user-accessible;
- the PTE is neither `PAGE_COW` nor `PAGE_SHARED`;
- the backing frame is `PageKind::NORMAL`;
- the backing frame refcount is exactly `1`.

The table is deliberately not a reclaim authority yet. It is diagnostic
metadata and future input only; current destroy-user-space reclaim still walks
PTEs and uses the existing protected-kind checks.

Ownership is refreshed or removed in these transitions:

- `map_page()` and `map_page_batched()` replace old ownership for overwritten
  leaves and attempt to track the new private normal leaf.
- `map_same_page_range()` clears any same-frame alias result after the batched
  mapping, so a repeated physical frame is not retained as exclusive.
- `unify_page_flags()` refreshes the leaf after present/user/write/NX changes.
- `reserve_page_range()` and `unmap_page()` untrack before dropping the old
  present-leaf reference.
- COW promote tracks the sole-owner page after clearing `PAGE_COW`.
- COW copy untracks the old frame for this vaddr and tracks the new private
  frame after the PTE replacement.
- Fork/COW clone untracks every shared present user leaf in the parent before
  incrementing the child-shared frame refcount, including read-only leaves.
- Pagemap destroy, budget-state creation, and root release purge any remaining
  side-table rows for that root.

The fixed table currently exposes `/proc/kcpustat` keys with the `of_` prefix
and renders them in `perf cpustat`: capacity, entries, track attempts, added,
replaced, skipped, conflicts, probe failures, untrack attempts, removed, missed,
purge calls, and purge removals.

The first booted validation of the unsharded table passed all KTESTs and the
`cross-os-suite-20260608-042614` render/mandel suite. Render wall times were not
obviously regressed versus `cross-os-suite-20260608-034815`: default-scene
node-threads `39.324s` vs `40.275s`, default-scene process-per-core `46.769s`
vs `46.583s`, duck node-threads `180.590s` vs `198.295s`, and duck
process-per-core `189.549s` vs `189.415s`.

The same validation found the important problem: standalone fork/COW throughput
regressed to `143k cow_writes/s` for the 32-child/100-iteration sample, down
from the earlier validated `~412k-464k cow_writes/s` range. The likely cause was
the single global owned-frame lock on concurrent COW faults. The table has since
been changed to 64 hash shards, capacity `131072`, with hot track/untrack taking
only one shard lock.

Sharded-only reruns improved some shapes but remained variable under
32 children: the 256-write sample ranged `~141k-239k cow_writes/s`, while the
800-write sample ranged `~269k-452k cow_writes/s`. A follow-up source change now
uses a trusted fresh-page track helper for pages allocated inside the COW handler.
That path skips redundant kind/refcount lookup for newly allocated normal pages
and skips the old shared-frame lock probe on every copy fault. Warmed
32-child/100-iteration/800-write samples after that helper held a much tighter
`~494k-544k cow_writes/s` range.

The same live run pointed the next optimization away from owned-frame lookup and
toward page-GC slicing. `perf cpustat` showed the deferred queue drained
(`depth=0`, `depth_max=1`) but needed `1877663` slices for `28814` completed
cleanups, roughly `65` slices per cleanup, with `gc_defer_wait_max_us=115592`.

The 2026-06-08 `cross-os-suite-20260608-050050` validation changed the
owned-frame picture: KTEST and render/mandel stress passed, `of_conflict`
remained `0`, and the duck process-per-core after-sample on `wos-0` reported
only `of_probe_fail=593` with `of_entries=8289`, `of_track=1892171`,
`of_added=1788702`, `of_removed=1521658`, and `of_purge_removed=258755`.
That is good enough to keep designing on top of the instrumentation.

Known limitations before reclaim consumption:

- The table is bounded and may under-track on probe pressure.
- Alias detection is conservative and evicts ownership when a physical frame is
  observed at another `(pagemap, vaddr)`.
- There is no per-pagemap owned-list drain yet, so exit reclaim cannot consume
  this metadata without another design pass.
- Runtime validation should keep proving `of_conflict` and `of_probe_fail`
  remain acceptably low on COW and render workloads before the table is used for
  reclaim decisions.
- Latest validation meets the probe-pressure requirement for instrumentation
  (`of_conflict=0`, `of_probe_fail=593`), but reclaim consumption remains
  blocked because the current structure is still a fixed-probe physical-frame
  table, not a per-pagemap owned-list drain.

## Deferred GC Budget Validation

The foreground-deferred GC budget tier is functionally safe but not yet a win.
User-run KTEST and `cross-os-suite-20260608-050050` passed, and an immediate
post-COW `memacc` sample after the second 32-child/100-iteration/800-write run
showed stable memory: `free=32507124 KiB`, `used=696264 KiB`, allocator
`188568 KiB`, physical allocator `live=174162` pages, and no zone mismatch.

The performance signal remains mixed:

- Standalone warmed COW after the budget tier reported `370918` and
  `443028 cow_writes/s`, below the previous trusted-COW-helper band of
  `~494k-544k`.
- Duck process-per-core render in `cross-os-suite-20260608-050050` was healthy
  at `186.399s` / `137.340M rays/s`, but duck node-threads was slow at
  `202.320s` / `126.532M rays/s`.
- The duck process-per-core step increased deferred cleanup from `212` to `713`
  completions and slices from `15965` to `232221`, or `216256` slices for `501`
  completed cleanups. That is roughly `432` slices per cleanup, with cumulative
  `wait_max_us=513623`.

Conclusion: this tier is safe enough to keep experimenting, but it is not the
final page-GC speedup. The next policy should reduce deferred slice count
without letting foreground COW work lose throughput.

## COW Allocation Pressure Backoff

Continuous no-pause COW benchmark spam exposed a more severe producer/reclaimer
failure mode: `cow_copy` could hit `pageAlloc failed for size 0x1000` while many
dead process pagemaps were still waiting for scheduler GC cleanup. A pressure
snapshot during the repro showed `sched_gc` running, about `24 GiB` of live
physical allocator pages, and depleted lower regular zones, so eventual reclaim
was not a sufficient contract for the COW fault producer.

The current source response is intentionally conservative:

- keep the deferred cleanup queue single-owner under the scheduler GC daemon;
- add may-fail physical allocation wrappers that return `nullptr` without
  dumping OOM diagnostics;
- add opt-in physical allocation wrappers that try a bounded number of silent
  allocations, request scheduler-GC memory-pressure mode, and `kern_yield()` the
  producer between attempts before taking the existing diagnostic OOM path;
- use those opt-in wrappers for COW copy/zero destination allocation and
  page-table allocation;
- let the GC daemon use a larger bounded pressure profile (`512` tasks,
  `50000 us`, `65536` pagemap steps) and keep that pressure mode active while
  deferred pagemap work remains;
- preserve the old OOM logging/panic path after bounded retries fail;
- keep the old frame pinned across the wait and rely on the existing post-copy
  PTE recheck to handle a racing COW handler.

Do not use `top`'s 1-second memory sample as proof that allocator pressure was
not real: the fork-COW benchmark can burn through the reclaimable headroom in
under 100 ms. The allocator-side OOM site and OOM dump are the authoritative
snapshot for these spikes. The opt-in retry budget is intentionally much larger
than one top interval sample would imply, while the default allocator path stays
nonblocking.

This is backpressure, not a replacement for faster reclaim. The validation gate
is the no-pause spam-COW repro: it should stop panicking, memory should plateau
or oscillate below exhaustion, and COW throughput should remain close to the
trusted-COW-helper baseline once reclaim catches up.

The first retry patch moved the observed OOM from `cow_copy` to
`alloc_zeroed_page_table()` inside `deep_copy_user_pagemap_cow`. That confirms
the pressure problem is not only COW data pages; fork-time page-table copying is
also a producer. Page-table allocation now uses the same pressure backoff and
final fallback failures are named `page_table`.

The next no-pause spam-COW failure moved to the child kernel-stack allocation in
`wos_proc_fork()` (`0x80000` bytes). That path now uses the opt-in reclaiming
allocator before the final diagnostic allocation. The parent syscall register
snapshot is saved only after this pressure wait completes, preserving the fork
return-frame invariant while allowing reclaim to catch up before a child task is
materialized.

The full allocator dump from the later no-pause spam-COW repro clarified why
allocator-side retries alone are not sufficient. At OOM, all regular zones were
empty, CPU0 had `15134` tasks on its dead list, the scanner found `1800` tasks,
and exited/zombie `testprog` instances held `67951872 KB` of user and pagemap
memory. The live userspace RSS view was therefore misleading; most pressure was
dead address-space cleanup that had not caught up yet.

The fork producer now applies a stricter admission contract before creating a
child task. In normal syscall context it runs foreground pressure reclaim while
free pages are below the low watermark. If reclaim reports no completed cleanup
for `64` yield cycles and the free-page watermark is still not restored,
`fork()` returns `-ENOMEM` before any child state is materialized. That
intentionally moves the pathological sustained pressure failure mode from kernel
allocator OOM to a userspace-visible fork failure.

The next dump showed why raw dead-list length is not enough: CPU0 held `16661`
dead tasks and exited `testprog` rows accounted for `77082620 KB`, but many of
those tasks were still wait-status zombies. Scheduler GC now splits zombie
lifetime from heavy resource lifetime. Once a DEAD zombie is epoch-safe, not
current on any CPU, and has refcount `1`, GC may reclaim its pagemap, thread,
kernel stack, and syscall scratch storage even when its active parent has not
called `waitpid()` yet. The `Task` object, PID, parent relationship, exit
status, and rusage timing remain in the active/dead registries so `waitpid()`
continues to work. A later GC pass deletes the small task record only after
`waited_on` becomes true or the parent is gone.

With that split, fork admission now uses free-page headroom rather than raw
dead-list count as the blocking signal. Otherwise thousands of already-stripped
zombie status records would keep throttling forks after their memory was safe.

The following failure moved to kmalloc medium backing (`0x2000` bytes in
`kmalloc.opt.cpp`). Medium and large kmalloc backing now use the opt-in
reclaiming physical allocator before inserting into kmalloc tracking lists, so
the existing medium/large lock and magic invariants are preserved. Mini-slab
expansion is deliberately not moved yet because it currently requests pages
while holding the per-size-class slab spinlock; it needs a separate design if it
becomes the next pressure producer.

Allocator-OOM diagnostics must not depend on the journal or serial lock. The
`page_alloc` OOM banner and early dump sections now use unlocked emergency UART
writes, matching the OOM dump's no-allocation contract. The runtime success
criterion is that spam-COW failures produce the full page allocation dump rather
than stopping after the first OOM line.

## Background Reclaim Design Direction

Do not jump straight to parallel reclaim. A safe sequence is:

1. Keep scheduler GC as the only owner of task detach.
2. Add backlog observability for the existing serial deferred cleanup queue.
3. Validate idle fast-reap against fork-COW peak memory and renderbench latency.
4. If backlog remains the dominant signal, introduce an MM-owned reclaim queue
   that receives only detached, sibling-free pagemap cleanup work.
5. Make queue transfer explicit: after transfer, scheduler owns no pagemap
   cleanup state except a completion reference to the detached task.
6. Use bounded workers with backpressure. If queue allocation fails or backlog is
   above a hard limit, fall back to existing serial cleanup.
7. Preserve the current destroy budget phases and safety skip counters.

Parallel workers must not scan active task registries to decide pagemap
ownership. That decision belongs at scheduler detach time while the task and
dead-list state are still authoritative.

## Benchmark And Validation Plan

Minimum user-run cycle before marking either tranche complete:

- Boot/selftest with MM ktests.
- Fork-COW stress with `perf cpustat` before/after and peak memory observation.
- Render default and duck scenes in both node-thread and process-per-core modes.
- `benchmarks/cpustat_diff.py` over the new suite manifest.
- `benchmarks/render_variance_report.py` over the same suite.

Signals to compare:

- backlog depth/age if instrumentation is added;
- `gc_pm_us`, `gc_pm_max`, `gc_task_us`, `gc_us`;
- `dus_leaf`, `dus_refdec`, `dus_freed`, `dus_ptrefdec`, `dus_ptfree`;
- all protected/corrupt skip buckets;
- elapsed seconds, rays/sec, and worker variance for renderbench;
- peak/transient memory during fork-COW stress.

## Checklist Recommendation

- Tranche 18 should remain unchecked. This note maps the lifecycle and proposes
  the metadata contract, but no owned-frame list or reverse-map implementation
  exists yet.
- Tranche 19 should remain `?`. Idle fast-reap exists in staged/local scheduler
  code, but backlog/peak-memory validation is still pending and explicit queue
  observability is not implemented in this slice.
