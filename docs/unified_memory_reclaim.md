# Unified Memory Reclaim

WOS memory pressure is coordinated by MM through
`platform/mm/reclaim.{hpp,cpp}`. The physical allocator supplies order-aware
zone state and requests reclaim, but it does not call filesystem, networking,
VM, or scheduler reclaim functions directly.

## Policy

Each managed regular zone exports its total and free pages plus largest
available buddy order. MM derives three watermarks from the zone's share of
managed memory:

- `critical`: immediate pressure, including failure to provide the requested
  buddy order;
- `low`: start reclaim;
- `high`: background-reclaim recovery target.

The historical 256 MiB reserve remains the normal low-watermark ceiling. Small
systems scale that reserve down, and every level has an order-derived floor.
Pressure classification therefore accounts for both page quantity and
fragmentation.

Order-0 allocations sample pressure after every 256 allocated pages. Requests
of order 4 or higher sample immediately. A low or critical sample coalesces a
wakeup for the `mm_reclaim` worker, which reclaims toward the high watermark in
bounded passes. Allocation failure performs at most eight direct coordinator
passes and then preserves the existing may-fail or diagnostic-OOM behavior.

## Coordinator contract

The registry is a fixed 16-entry table. Registration and telemetry snapshots
hold its IRQ-safe spinlock only for bounded copies. One scan lease serializes
callbacks; a task that re-enters reclaim is reported as recursion, while a
different task is reported as contention. Neither case waits for the lease.

Every shrinker declares:

- its native reclaim and scan units (`pages`, `bytes`, or `objects`);
- stable rank and minimum pressure priority;
- whether it may block, perform I/O, or allocate;
- hard callback and scan budgets;
- allocation-free count and bounded scan callbacks.

Callbacks that over-report either budget are clamped and counted as failures.
No-progress callbacks receive exponential round-based backoff; critical and
explicit runs may bypass cooldown. Fairness is stable within a rank, and ranks
define the global callback order.

Direct and explicit reclaim only run in a normal task context with interrupts
enabled and preemption available. IRQ, page-fault, panic, scheduler-transition,
and preemption-disabled paths skip reclaim. If an allocation inside a shrinker
re-enters the allocator, recursion detection prevents retry or yield while the
outer callback may still hold subsystem locks.

## Registered shrinkers

| Rank | Name | Native unit | Minimum priority | Capabilities | Per-callback bound |
| ---: | --- | --- | --- | --- | --- |
| 0 | `kernel_vmap` | pages | low | block | 512 pages |
| 1 | `scheduler_gc` | objects | normal | block, I/O, allocate | scheduler pressure budget; 4096 scanned objects |
| 2 | `buffer_cache` | bytes | low | block | 64 MiB; 8192 scanned buffers |
| 3 | `file_mmap_cache` | pages | low | block | 128 pages; 4096 scanned pages |
| 4 | `xfs_inode` | objects | low | block | inode batch and bucket bounds |
| 5 | `packet_pool` | chunks | normal | none | one released chunk; eight inspected chunks |
| 6 | `tmpfs` | pages | critical | block, I/O, allocate | 32 pages; 4096 scanned pages |

Important subsystem invariants are unchanged:

- `kernel_vmap` only drains already queued mapping frees and performs TLB work
  outside unsafe contexts.
- Scheduler GC keeps task detach decisions under runqueue locks and performs
  pagemap/task destruction after detach under its existing exclusive pass.
- Buffer reclaim frees only clean, unlocked, unreferenced entries. Dirty data
  remains on the writeback path.
- File-mmap and XFS scans use persistent cursors and release victims outside
  their short-held cache locks.
- The packet pool never retires the permanent RX/TX reserve. Growth is released
  in complete chunks, and exact physical progress accounts for the current
  order-2 (four-page) backing of every packet buffer. An explicit target is
  compared with total capacity so a later request can finish a growth chunk
  after all of its in-flight buffers have returned from draining.
- Tmpfs releases a resident page only after its swap write succeeds. The node
  registry uses nonblocking node-lock acquisition so reclaim cannot invert its
  lifetime lock order.
- Kernel stacks consume their fixed reserve first. Fork admission protects
  order-0 global headroom; only the fallback allocator asks for the actual
  stack order.

## Progress and telemetry

Shrinker callbacks report their native unit. The coordinator separately
samples allocator-visible free pages before and after each callback; only that
positive delta contributes to the cross-subsystem `reclaimed_pages` total.
This avoids adding bytes, objects, and pages into a fictitious aggregate.

`/proc/memacc/reclaim/coordinator` exposes schema 1 with:

- global direct/background/explicit attempts, callback totals, typed scan and
  reported-unit totals, physical pages recovered, failures, guard skips,
  cooldowns, latency, transitions, explicit request/completion/rejection
  sequences, and worker activity;
- one `zone_watermark` row per managed zone;
- one `reclaim_shrinker` row per registered shrinker with descriptor, last
  count, native work totals, and physical-page progress.

Read it directly or through the WOS utility:

```sh
memacc raw reclaim/coordinator
memacc kernel
memacc dump --full
```

The existing writable endpoints remain compatible and now dispatch through
the coordinator. Procfs only fills one fixed-capacity request slot and wakes
the existing MM worker, so a syscall never executes a blocking shrinker while
preemption is disabled. `memacc` waits for the matching completion sequence
with a ten-second bound before printing its before/after result. A worker job
may cross up to 64 consecutive no-progress cursor slices while a shrinker still
reports more candidates; this is enough for one complete sparse XFS inode-cache
sweep, remains bounded, and does not alter direct or background reclaim passes:

```sh
memacc reclaim buffer_cache
memacc reclaim packet_pool
memacc reclaim xfs_inode
memacc reclaim file_mmap_cache
```

`memacc raw all` reads procfs endpoints sequentially. Its summary section is an
internally coherent physical-accounting snapshot; the other sections are
adjacent observations and must not be described as one atomic snapshot.

## Validation

Run the host policy and structural tests after configuring the host suite:

```sh
scripts/test/run_tests.sh build
ctest --test-dir /tmp/wos-tests --output-on-failure \
  -R '^(reclaim_policy_test|reclaim_source_test|phys_source_test|memacc_source_test|kernel_vmap_source_test|buffer_cache_test|tmpfs_swap_source_test|xfs_inode_source_test)$'
```

Use normal `Build WOS` for the integrated build. The isolated diagnostic VM can
be built and run without repeating privileged host setup:

```sh
bin/wos-ktest --no-setup
```

Likewise, an already prepared cluster can be launched without root setup and
inspected live:

```sh
bin/wos-cluster --launch --no-setup
scripts/remote/wos_ssh.sh wos-0 /usr/bin/memacc raw reclaim/coordinator
```

The exact physical-memory regression gate remains documented in
`docs/physical_memory_regression_gate.md`.
