# NUMA topology and qualification

WOS discovers x86 SRAT CPU/memory affinity and SLIT distances through the
bounded ACPI reader. The owned boot snapshot is published once, before AP
startup. Sparse firmware proximity IDs map to dense internal node IDs; these
are distinct from administrative scheduler CPU-domain IDs. `get_cpu_node()`
continues to index CPU startup storage.

Malformed tables are rejected with a diagnostic and explicit UMA fallback.
Missing SRAT yields UMA. Missing SLIT uses labelled default distances (10 local,
20 remote). CPU-only and memory-only nodes remain visible. RAM affinity ranges
cover initialized physical zones, excluding holes; their byte totals include
allocator metadata and regular zone descriptor pages. They describe managed
coverage, not free capacity. Firmware reserves and per-CPU cache storage remain
separate from the managed identity. Existing
ACPI initialization still requires a usable RSDP.

The parser supports 256 CPU identities and 64 proximity domains. The current
scheduler/affinity ABI supports 1–64 online CPUs and requires the BSP at logical
index zero; boot explicitly rejects unsupported enumeration before publishing
partial masks. The public policy setter accepts LOCAL, PREFERRED, BIND and
INTERLEAVE after validating the mode and node mask against the boot topology.
Topology discovery alone does not qualify placement or large-page behavior.

Task defaults are stored per executor under a short IRQ-safe snapshot lock.
Creation copies the initiating executor's value independently of its credential
owner. Owned exec preparation carries the value to successful canonical image
commit; failed or canceled preparation preserves the existing default. These
internal paths support saved policy values. The public setter updates the actual
executor independently of its credential/quota owner.

The internal placed allocator supports preferred, strict-bind and deterministic
virtual-page interleave selection. A checked interval claim returns either one
frame or 512 absolutely aligned, independently owned order-0 frames. Move-only
ownership frees an uncommitted span. Private lazy anonymous and COW allocation
consume the supplied executor's copied policy and a captured CPU-node value.
Hardware origin uses the saved exception CPL; software ensures remain separately
classified. Installed private-copy frames, zero-COW and new anonymous pages have
separate counters; remote fallback remains a miss in an eligible locality count.
Opt-in large anonymous mappings use the captured policy as described below.

Domain-constrained claims bypass unfiltered CPU caches, so cached capacity can
cause conservative allocation failure. Allocation-pressure bookkeeping can wake
reclaim, but these attempts do not reclaim or wait. Only the exact UMA LOCAL
one-page/4 KiB compatibility path can delegate to the existing reclaim helpers;
private lazy anonymous allocation retains its nonwaiting behavior.

WKI keeps non-LOCAL memory policies on the originating machine: automatic
placement stays local and explicit remote placement returns ENOTSUP. Queued
worker exec uses the initiating request's saved policy. Direct submission also
refuses non-LOCAL policy and releases transferred preparation ownership. These
rules do not put local node IDs into the wire protocol.

## Large-page admission and reclaim policy

Private anonymous allocations explicitly requesting `MAP_HUGETLB` use 2 MiB
leaves. Their length must be a multiple of 2 MiB, and the chosen virtual address
and physical backing are aligned to 2 MiB. The public allocation path requires
`MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB`; `MAP_POPULATE` is its only additional
supported mapping flag. Ordinary anonymous mappings retain their existing path.

An unsplit large leaf is nonreclaimable by the ordinary 4 KiB anonymous swap
selector, which rejects an intermediate page-table entry with the PS bit set.
Allocation uses `ReclaimMode::NEVER`; it can fail under memory pressure without
waiting for reclaim or automatically demoting other large leaves. Supported
mapping mutations can split a leaf into independently owned 4 KiB frames.

The registry bounds this nonreclaimable backing. Each node admits at most one
sixteenth of its usable pages, rounded down to complete 512-page leaves. The
global limit is the smaller of the same rounded fraction across all nodes and
128 leaves (256 MiB). Each address-space root admits at most 64 leaves (128 MiB),
subject to the node and global limits. Preparing, mapped, splitting and retired
rows all retain their capacity charge. Failed preparation returns its owned
frames before releasing that charge. Split and teardown completion release
registry capacity only after the required TLB completion and frame-ownership
handoff.

Large allocations also pass the initiating task's address-space resource
admission before preparation and again before publication, then commit normal
mapping accounting. Large-page admission does not bypass `RLIMIT_AS`. The public
`testprog numa-huge --phase correctness` workload includes refusal below the
required address-space limit, alongside mapping mutations and fork/COW checks.
These are implementation and test descriptions; current pressure, external
mapping/ownership observations and paired performance qualification require
their actual run results. The dated results below retain their original scope.

The explicit per-node balance diagnostic scans bounded physical metadata and
collects global and node owner totals during one allocator-lock interval. Its
fixed buffers belong in BSS; `max_frames` refuses excess work before acquiring
allocator locks. It is unsuitable for an allocation hot path. Refcounts remain
sampled telemetry because they can change outside allocator locks; a transient
zero reference does not turn an allocated frame into free capacity.

The collector translation unit excludes KASAN checks: a first shadow-page fault
can allocate and recursively wait for the allocator locks being observed.
KCFI, report-mode UBSan and KCOV remain enabled in the diagnostic profile, and
the actual metadata scanner's Host tests retain ASan/UBSan. This boundary does
not alter shadow poison, page ownership or the rest of the kernel's diagnostics.
Generated-code checks at configured optimization and O0 also cover shared weak
atomic/array helpers from instrumented callers. Their existing libc++ sanitizer
ignorelist exclusions are part of the current safety boundary; the translation
unit flag alone does not establish that every selected helper is uninstrumented.

## Address-space admission and retirement

Ordinary public mapping inspection and preparation acquire the actual Task/root
lease and publication guard before entering a short preemption guard, then
revalidate that authority inside the guard. Reader-admission waits may yield
with interrupts enabled while retaining the Task lease and publication guard.
No striped reader or debt epoch survives that wait. Raw-root inspection keeps
its nonwaiting contract.

Faulting usercopy uses a separate typed pin attempt. On root-gate contention,
it releases its unsuccessful frame/root pin and Task pagemap lease before an
eligible ordinary syscall executor yields. The preparation scope temporarily
enables interrupts and restores the incoming IF afterward. An otherwise eligible
ordinary caller may enter with IF clear. Mapped-only access, ineligible contexts,
preemption-disabled callers, retained pins and actual retained ptrace effects
refuse this handoff. A pending ptrace operation without a local effect remains
eligible. Each retry repeats preparation and acquires a fresh target root.
Callers retain the target Task's lifetime and enter without other
MM/publication/access owners.

Prepared fork receives a one-use capability issued only by the factory for a
fresh, unpublished child root. Creator/child identity, page kind, references,
root incarnation, empty file index and empty user half remain checked. Source
exclusive admission precedes lazy adoption. A busy attempt releases its owners,
waits in ordinary context and starts preparation again. Success transfers the
real source gate and Task lease to the copier, which releases the gate before
the lease. Copy and adoption failures remain terminal for that attempt.

The BSP selects page-table-pool policy after topology publication and before
AP startup. UMA retains its cache; multi-node systems return ordinary tables
to physical ownership, preserving the fixed origin of emergency reserve roots.
An exiting non-thread `PROCESS` switches its CPU to the kernel root while
retaining `Task::pagemap` for scheduler-GC retirement under the existing epoch,
current-task, shared-root and lifetime guards. Waitable zombies may retain
resources until that retirement. These paths provide no bounded completion or
starvation-free fork guarantee.

After four optimistic `mkdir` attempts return `-EAGAIN`, a local host-namespace
request makes one fresh serialized native lookup. Earlier lookup/publication
owners have ended; captured credentials and the already-masked mode remain.
Remote operations are not replayed. Mapping, COW, TLB-before-reuse, quota and
syscall contracts remain in force throughout these progress changes.

## Virtual NUMA

The optional `vm.numa` section uses this shape:

```json
{
  "memory": "8G",
  "cpus": 8,
  "numa": {
    "nodes": [
      {"id": 0, "cpus": [0, 1, 2, 3], "memoryMiB": 4096},
      {"id": 1, "cpus": [4, 5, 6, 7], "memoryMiB": 4096}
    ],
    "distances": [[10, 20], [20, 10]]
  }
}
```

Node IDs must match array positions. Every CPU belongs to exactly one sorted
CPU list, and node memory sums exactly to the canonical `M`/`G` VM size. A
memory-only node may have an empty CPU list. A CPU-only node has zero memory;
empty nodes are rejected. Distances are optional; when supplied the complete
matrix has diagonal 10 and off-diagonal integers 11–255. Asymmetric distances
are allowed. Validation precedes overlay or log mutation.

The shared cluster/KTEST node builder emits anonymous memory backends and QEMU
NUMA arguments. It imposes no host NUMA binding. The zero-memory-node argument
shape was admitted by installed QEMU 11.1.2 using a paused diskless VM; that
check alone does not prove guest firmware discovery.

`configs/node_numa_ktest.json` provides an isolated eight-CPU, two-domain fixture
with socket multicast NICs. Build and launch through the existing KTEST path:

```sh
bin/wos-ktest --config configs/node_numa_ktest.json --fast --build-only
bin/wos-ktest --config configs/node_numa_ktest.json --fast --no-build --no-setup
```

The second command packages the isolated KTEST artifacts before launch. Its
`--numa-test-two-nodes` boot token makes the native snapshot test require two
accepted domains, both with managed RAM, the expected CPU membership and SLIT
distances. Inspect `ktest-data/serial-numa-vm0.log` and
`ktest-data/qemu-numa-vm0.log`. Preserve logs before repeating a run, because the
shared launcher replaces these log paths.

Run one disposable topology at a time. Measure allocated image/cache usage and
free space before each batch; use a 20 GiB disposable budget and 40 GiB free
floor. After either outcome, verify exact ownership and absence of all process
references before pruning only the run's overlays. Keep current KTEST disks,
sysroots and required shared caches/seeds; retain small receipts instead of
completed disks.

The user selected virtual NUMA for this goal. Record actual emulated locality
and benchmark samples separately from any physical NUMA result. Emulated SLIT
labels do not establish a real remote-memory latency penalty or the original
real-hardware performance threshold.

## Foundation validation, 2026-10-15

Normal Build WOS, isolated `bin/wos-ktest --build-only --fast`, six registered Host
entries and the changed C++ format/tidy check passed. The actual eight-CPU,
two-domain rootless KTEST run passed 1,473 cases and 128,273 assertions, with zero
failures, using kernel SHA-256
`cd7ebfb222ae66abc7a982bf2bf3e7fac34d5c0d930ac997e5246794e313984d`.
The three topology and seven allocator cases cover physical confinement,
zeroing, aligned 512-frame ownership, move/reset, rollback and 1,000 ordinary
allocation/release cycles. Both emulated nodes had RAM, so the conditional
memoryless-node fallback branch was not exercised by that run.

The run used the stable host launch guard even with a private TMPDIR. Disposable
allocation peaked at 268,058,624 bytes; the exact-owned boot copy, two overlays
and generated authentication file were removed after inactivity/reference and
retained-backing checks. Current artifacts and the unrelated VM were preserved.
Small local receipts are retained under `build/goal40-evidence/native-numa03/`.

The corresponding UMA foundation run passed 1,473 cases and 128,239 assertions
with the same kernel, zero failures and natural poweroff. Its private images
were also removed, with the shared guard and unrelated VM preserved. These
foundation runs precede Task-policy inheritance and the per-node collector.

The first native collector run exposed a KASAN shadow fault while holding the
allocator locks. A bounded stack capture identified recursive physical allocation
waiting for those locks. That run remains failed evidence; its disks were removed.

After the narrow fix, fresh virtual NUMA qualification passed 1,480 cases and
128,439 assertions with zero failures and natural poweroff, using kernel SHA-256
`56b64ccaa6ee87a77982f4ae97c29938df8c4c7aad23ecc0b27a5133d53b3f19`.
The new tests cover actual per-node/global managed ownership, placed-frame
owner closure, Task-policy state and exec publication. Full scans of 1,877,360
frames took 245–250 ms. This IRQ-disabled diagnostic cost is unsuitable for hot
allocation paths. All watched inputs stayed fixed; the original guard and
unrelated VM stayed unchanged. Disposable images peaked at 268,320,768 bytes and
were removed after reference/backing checks. The receipt is
`build/goal40-evidence/native-numa06/result.json`.

UMA qualification of the same fixed kernel passed 1,480 cases and 128,335
assertions with zero failures and natural poweroff. Its receipt is
`build/goal40-evidence/native-uma07/result.json`; watched inputs, original guard
and unrelated VM stayed unchanged, and all disposable images were removed.
Ordinary KTEST parks secondary CPUs, so these runs do not qualify SMP races or
CPU-pinned userspace locality benchmarks.

These checks establish the internal foundation. Later demand/WKI validation is
recorded separately below; the foundation results do not qualify those changes.
User first touch, per-node
reserves and pressure policy, 2 MiB mapping lifetime, map/fork/pin/unmap stress and paired
locality/UMA throughput measurements remain required.

## Private demand and WKI admission, 2026-10-15

Normal Build WOS, isolated fast KTEST build, all 18 registered relevant Host
entries and changed C++ formatting/clang-tidy passed. The factory Host target
executes the actual placed allocation implementation with 13 dispatch,
validation, zeroing and ownership cases; its physical seams do not qualify the
native buddy or reclaim behavior.

The first native demand run failed because its observer used a current-process
root helper for a deliberately unpublished target. The failed run and assertion
summary are retained; its images were removed. The correction uses the exact
supplied root's registered incarnation while preserving the current-process
API's admission checks. A subsequent driver attempt stopped on the prior run's
log before guest launch; its unused image was removed and serial freshness was
added to the driver. Neither attempt is passing runtime evidence.

Fresh emulated NUMA qualification passed 1,493 cases and 129,241 assertions,
with zero failures and natural poweroff, using kernel SHA-256
`17fc27ec2f9264663228aa074dcd62d1c1478110bd572aba5ed0da42cf8c6f0c`.
The nine demand cases exercise actual private anonymous, genuine zero-COW and
private COW installs, physical bind/interleave confinement, copied-policy
stability, sole-reference promotion, allocation/protection rollback and owned
frame closure. Four WKI cases exercise sender admission, direct-submit
cancellation and captured worker policy.

The prescheduler suite has no queryable current executor. It therefore does not
qualify scheduled foreign-executor access, real CPL3 first touch, IF-on reclaim
progress, network transmission or SMP races. Native tests report these limits
explicitly. Peak disposable disk allocation was 268,910,592 bytes; all owned
boot/overlay/auth files were removed after inactivity/reference/backing checks.
The original host guard and unrelated VM remained unchanged. The receipt is
`build/goal40-evidence/native-numa12/result.json`.

UMA control of the same kernel passed 1,493 cases and 129,078 assertions with
zero failures and natural poweroff. All watched sources stayed fixed; the
original guard and unrelated VM were preserved. Its peak disposable allocation
was also 268,910,592 bytes and its owned images/auth files were removed. The
receipt is `build/goal40-evidence/native-uma13/result.json`. This qualifies the
same internal paths on UMA, with the prescheduler limitations above.

## Swap preservation and teardown ordering, 2026-10-15

Generic anonymous software fault-in now refuses a tracked swap marker instead
of installing fresh zero backing over saved bytes. Two native tests exercise
both generic ensure and usercopy refusal, preserve the real swap slot and
marker, then recover and compare every byte through the existing page-in path.
This does not add blocking swap preparation to arbitrary usercopy callers.

Both teardown walkers detach a valid data leaf before enqueueing its owner.
Every nonempty data or page-table batch retains the actual root and completes
its full-root TLB flush before decrementing references. This includes the
immediate capacity flush and budget slices/disposal. Existing page-table parent
detachment and final binding retirement remain in place. SMP initialization or
CPU online publication now fails if its required shootdown vector is absent.
Callers still own the no-future-root-publisher precondition; disposing a partial
budget state does not finish the remaining tree.

Normal Build WOS, isolated fast KTEST build, all 24 registered relevant Host
entries and the changed C++ formatting/clang-tidy check passed. New executable
Host adapters exercise actual teardown/readiness bodies through disclosed
physical/TLB primitives. Their ordering assertions reject the original code;
they do not model an actual remote CPU acknowledging an IPI.

Fresh virtual NUMA passed 1,495 cases and 129,429 assertions, with zero failures
and natural poweroff, using kernel SHA-256
`f14ebe9beae60cd76c5548978a5e7dc89c11400db629b26944dacda32b5a95f8`.
Both new swap tests and the earlier demand/WKI cases passed. Watched sources,
the host launch guard and the unrelated VM stayed unchanged. Disposable image
allocation peaked at 268,976,128 bytes and returned to zero after exact-owned
inactivity/reference/backing checks. The receipt is
`build/goal40-evidence/native-numa14/result.json`.

UMA control of the same kernel passed 1,495 cases and 129,266 assertions, with
zero failures and natural poweroff. Watched sources, the original guard and
unrelated VM stayed unchanged. Its disposable allocation also peaked at
268,976,128 bytes and returned to zero. The receipt is
`build/goal40-evidence/native-uma15/result.json`. These prescheduler tests do
not qualify ordinary SMP teardown/reuse, IF-off cross-CPU shootdown progress,
scheduled CPL3 first touch or general huge-page lifetime.

## Eager placement, root reserves and free repair, 2026-10-15

Private eager anonymous and read-only reserved-page backing now use the placed
frame factory. The initiating Task's copied policy and actual CPU node are
captured before VM publication; each page uses its destination virtual index.
Metadata-only, lazy and genuine shared-zero routes retain their existing
semantics. Native tests cover zeroing, bind/interleave, prefix-allocation
rollback, reserved-leaf refusal and genuine zero-COW adoption. These helper
tests do not establish full current-process syscall OOM atomicity.

Boot seeds up to 64 independently owned emergency root frames per memory node.
Only failed ordinary root allocation can borrow a frame from its exact local
reserve. Registered/raw/budget root disposal returns it to its original slot
after the existing ownership and TLB retirement protocol. Intermediate page
tables cannot borrow it. Capacity shortfalls remain explicit; no cross-node
reserve borrowing or local reclaim progress is claimed.

Free-side buddy repair now completes the merged metadata and credits only the
original freed span before recounting. This fixes double credit during final
insertion repair and lost credit during a later coalescing repair. Actual
allocator Host tests reproduce the original failures, check both repair
windows and close a deterministic allocation/cache/free sequence.

Normal Build WOS, isolated fast KTEST, all 26 relevant registered Host entries
and changed C++ formatting/clang-tidy passed. Earlier Host adapter failures
remain recorded: two extraction boundaries needed the newly inserted context
builder excluded, and a source oracle still required the old repair order.
The corrected adapters retain their previous behavioral vectors. Final linked
KTEST inspection also checked the healthy reserve lock/helper closure; this
does not establish arbitrary panic/corruption or remote-CPU progress.

Fresh virtual NUMA passed 1,507 cases and 150,787 assertions with zero failures
and natural poweroff, using kernel SHA-256
`1b81f2ab1101d9c8da8994d363c310c0dedc9f252776fa2ec5625c5f9fb257c8`.
All five eager-placement and seven root-reserve cases passed. Both memory
nodes supplied their 64-frame reserves. The real control-provisioning rollback
branch ran, and 1,000 actual root factory/registration cycles returned their
reserve debt and ordinary free count. Those root cycles are separate from the
still-required userspace map/fork/pin/unmap stress.

All 81 watched inputs stayed fixed; the original launch guard and unrelated
VM were preserved. Disposable allocation peaked at 269,434,880 bytes and
returned to zero after exact-owned inactivity/reference/backing checks. The
receipt is `build/goal40-evidence/native-numa16/result.json`. These
prescheduler checks do not qualify scheduled CPL3 first touch, SMP races,
domain pressure/reclaim, general 2 MiB lifetime or paired performance results.

UMA control of the same kernel passed 1,507 cases and 147,604 assertions with
zero failures and natural poweroff. The real reserve rollback and 1,000 root
cycles also ran. All watched inputs, the original guard and unrelated VM stayed
unchanged. Its disposable allocation peaked at 269,434,880 bytes and returned
to zero. The receipt is `build/goal40-evidence/native-uma17/result.json`;
the same prescheduler limitations apply.


## Cheap node counters and userspace observation

Each physical allocator binds its node counters after topology publication and
before AP startup. Binding prepares every allocator before committing any;
malformed or incomplete coverage refuses publication. Fixed simultaneous
snapshots expose usable, inclusive free, cached free and buddy free pages.
Cached free is a subset of inclusive free; buddy free is their difference.
Usable excludes allocator metadata and zone descriptors, unlike firmware managed
coverage. These snapshots do not scan frame metadata or count per-node owners;
the bounded full balance diagnostic remains the ownership authority.

The installed WOS `sys/numa.h` records and `sys/vmem.h` wrappers provide immutable
topology/CPU masks, the actor's policy and at most 64 resident-page node rows per
request. Target-page observation does not fault absent payload into memory;
request/output transport follows ordinary checked usercopy rules and may fault.
It exports no physical address or kernel root identity. The policy setter
updates the actual executor, independently of its credential/quota owner.
Version, size, geometry, node masks and reserved fields are checked. Connected
placement/pressure qualification remains separate from API availability.
The existing strace decoder names both appended operations.

`testprog numa-first-touch` uses real pinned pthread workers, verifies all pages
are absent before volatile CPL3 writes, checks zero tails and records every
resident page in its local 95% denominator. `testprog numa-lifecycle` defaults to
1,000 fresh mmap/fork/COW/cross-page pipe-usercopy/wait/unmap cycles, with checked
FD/map cleanup and bounded waits. Scheduled results retain their individual
workload and source scopes, as described below. Their own-resource ledger
explicitly does not claim a global Task/File/pin/physical census, persistent
pins, huge pages or pressure.

Current counter/query integration passes Normal Build WOS, isolated fast KTEST,
33 selected Host tests and C++ formatting/tidy. The repaired counter fixture
prepares its diagnostic buffers before sampling, records the real cold BIND
allocation and proves its return, then claims 512 individual physical pages in
that warmed extent. It checks exact per-node/global free, cached and owner
totals before, during and after these claims. A counted scoped owner releases
every successful page claim, including on an early assertion return.

The original NUMA18, NUMA22 and NUMA24 failures are retained. They exposed errno
expectation width mismatches, cold diagnostic shadow allocation, an unsupported
factory address-reuse assumption and an unsupported single-buddy-block claim.
Contiguous free pages can span multiple buddy blocks. These fixture repairs
change no production allocation policy or counter logic and introduce no numeric
tolerance, guessed shadow offset or sanitizer suppression.

Fresh virtual NUMA26 passes all 1,512 native cases and 160,138 assertions with
zero failures and natural poweroff. All watched sources stayed unchanged; its
disposable images peaked at 270,090,240 bytes and were removed after reference
and backing-chain checks. UMA27 passes the same 1,512 cases and 152,600
assertions with zero failures and natural poweroff; its peak was also
270,090,240 bytes and its disposable images returned to zero. Both runs
preserved the original launch guard and unrelated VM. These are native kernel
tests; scheduled userspace, pressure, huge-page lifetime and performance
qualification remain separate requirements.

## Final C78 comparison, 2026-10-29

The earlier results retain their original scope. Later scheduled placement,
lifecycle, pressure and huge-page evidence is bound to its saved runs and source
applicability records. A separate Fast diagnostic KTEST run passed 1,560 tests
and 197,815 assertions, with zero failures, three recorded skips and natural
poweroff. Normal scheduling checks also passed the three-trial weighted-fairness
workload, with four workers per trial (12 total), and the separate bounded
affinity stress workload. These results do not establish universal scheduler
progress or validate subsequent kernel edits.

C78 completed all twenty original UMA arms in ten pairs, alternating reference
and current order. Each arm passed all 117 warmup and 117 measured cases, all
eleven raw metrics, natural poweroff and its independent physical-zero check.
The same eighteen checker inputs were held across all arms. Owned process
forests closed and disposable images were pruned after every arm; the ten
required immutable images were preserved within the inclusive 20 GiB budget.

The final result is **FAIL_C10_EXACT_MEDIAN_THROUGHPUT**. Only `fork_cow` fails:
its current/reference median-rate ratio is **0.9304342821**, below 0.95. The
other ten metrics pass. All 220 measured durations are retained; no trimming,
replacement samples or threshold adjustment is permitted. Earlier stopped
C65/C67/C76/C77 campaigns remain excluded and are not resumed or incorporated
into this dataset.

The gate divides the current median invocation rate by the reference median
invocation rate and requires at least 19/20 for every metric. Each invocation
rate is the exact rational 1,000,000,000/elapsed_ns before either median is taken.
Paired ratios and their median are diagnostic only. Duration and rate sample
variances use divisor nine. The [110-row sample CSV](evidence/numa-c78/samples.csv)
contains all 220 measured durations. The [exact report](evidence/numa-c78/summary.json)
records rational results, variances, distinct Normal profiles, kernel identities
and content commitments for the input receipts.

The current arm used the retained Normal current845 kernel (`c8d74b05…`), built
before the later weighted-fairness fixture, Host-test and documentation changes.
The follow-up Normal build preserved the six canonical artifacts. The fixed
reference (`32dbb500…`) is the pre-NUMA base plus the explicitly selected common
correctness corrections through the shared pending-file-fault repair; it is not
unmodified 940. Exact kernel hashes, build IDs and build-proof hashes are in the
report. These measurements apply to those frozen artifacts.

Virtual NUMA remains the accepted qualification environment. The original
requirement for a ten-percent gain on physical NUMA hardware remains
**UNVERIFIED**. The earlier renderer fault remains causally unresolved; later
passing runs alone do not prove its repair. C78 fails criterion ten, and this
appendix makes no Goal40 completion claim.

## Correctness validation before host tool refresh, 2026-11-01

Normal Build WOS and the subsequent complete-input validation build passed with
the current Normal kernel `66a869bd…` (build ID `2fd8cb74ce15…`). The isolated
Fast KTEST kernel is `728c8830…` (build ID `8687d32e9db6…`). The retained fixed
reference for the next throughput comparison is `83a3efdc…` (build ID
`48f085979f0b…`), with 17 explicitly selected common-correctness source files.
That reference retains its recorded baseline and selection; the earlier C78
artifacts and results above are unchanged.

Fresh Fast KTEST runs on UMA and two-domain virtual NUMA each completed 1,560
tests, with 197,827 and 214,877 passing assertions respectively, zero failures
and three skips each. Separate strict lifecycle runs completed 1,000 warmup
and 1,000 measured cycles per topology, with 1,000 consumed successful child
exits in each phase. The census checked all 41 physical-owner classes, 23 MM
fields and combined coverage 511. Global/domain free and ownership totals
reconciled; the oracle permits changes within the cached-free partition.
Owned maps and FDs returned to zero. Each phase had 2,000 eligible COW-resident
pages, all local; NUMA phases each included 500 actual cross-node COW operations.
Init consumed the workload exits and ordinary shutdown completed.

The final scoped criterion-eight review passed by joining these fresh lifecycle
results with the separately recorded healthy public policy, eligible first-touch
and scheduled selected-node exhaustion evidence. Those historical executions,
public-pressure limits and original diagnostic/completion flags remain unchanged.

Fresh Normal scheduling checks passed six full-interval weighted-share
comparisons and the affinity workload: 3,072 controller calls, 2,597 successes,
475 Busy results, 64 destination observations and 256 pipe round trips. These
two VMs received the expected host QMP quit after init consumed their workload
exits. A fresh one-CPU Normal run passed 20 serial forks over a 32 MiB mapping,
5,120 COW writes and 10 CPU-wait checks, then completed ordinary shutdown. Its
guest and host checks require the workload's quiescent consumed service status
(`pid=-1 pgid=-1`, generation 1, no restart, successful wait and exit history).
A captured-status regression exercises only these predicates; the complete
fresh guest reports and shutdown supply the one-CPU runtime result. The
finalizer's own consumed exit and fully drained GC are outside that result.
All seven roles passed separate independent saved-oracle and physical-zero
checks. Owned process forests closed and disposable images were pruned.

Focused Host tests exercise the actual pin/gate/retry functions, owner release
before yielding, fresh-root acquisition, interrupt restoration and ptrace-effect
refusal, with compiled negative controls. Their finite ports retain their stated
scope alongside the native results. Core placement/large-page policy and measured
dependent-load loops retain their documented source continuity; the changed pin
front end has its own source review and correctness tests.

The earlier ten-pair locality and 4 KiB/2 MiB TLB-sensitive measurements retain
their original artifacts, raw samples and source scopes. They are historical
timings. The physical NUMA ten-percent threshold remains **UNVERIFIED** under
the accepted virtual substitute; no hardware TLB-miss count is claimed.

The final combined review accepted criteria one through nine at these recorded
and source-applicable scopes. Earlier failed batches and individual diagnostic
flags remain unchanged.

Criterion ten remains open. Its fresh fixed-reference campaign must complete
all twenty arms in ten pairs, all 117 warmup and 117 measured cases per arm,
and all eleven metrics at the unchanged 19/20 exact median-rate threshold,
with raw samples, variance and independent closure evidence. The seven
correctness roles supply no paired-throughput credit. Goal40 remains incomplete.

## Host tool refresh and throughput status, 2026-11-01

After the host QEMU and Python packages changed, complete-input Normal and Fast
validation builds passed again. The kernel contents remained `66a869bd…` and
`728c8830…`, respectively. All seven correctness roles were then repeated and
passed their independent saved-oracle and cleanup checks. The refreshed UMA
selftest completed 1,560 tests with 197,826 passing assertions, zero failures
and three skips; NUMA completed 1,560 tests with 214,877 passing assertions,
zero failures and three skips. The earlier assertion count above belongs to
its original run. Both strict lifecycle runs again completed 1,000 warmup and
1,000 measured cycles with all 41 physical-owner classes, and the Normal
single-CPU, fairness and affinity checks passed.

The benchmark supervisor checks now accept a positive historical probe error
only for a currently running, ready daemon with a successful readiness
transition at its current state timestamp. Workload errors, restarts, identity
changes and timeouts remain failures. The C++ fixture regressions passed 229
checks; Python regressions passed 122, including 50 wire-format checks. Both
benchmark arms use the same corrected helpers.

The C100 throughput attempt stopped during its seventh run and supplies no
completed-campaign acceptance. C101 published its complete twenty-run plan,
then stopped during host input validation before any VM or packaging step.
The updated QEMU executable exceeds the initial reader's size limit and has
ownership that reader does not accept. The next harness revision exercised
that same initial path before nonce publication. No sample, threshold or
runtime deadline is relaxed. Published
nonces remain spent, and failed campaigns cannot resume or supply replacement
or prefix credit. Criterion ten remains open.

C102 subsequently passed that shared initial callback and the real launcher
parent check before publishing its twenty-run plan. The exact host executable
is verified separately while ordinary receipt ownership, size limits, byte
accounting and closing checks remain enforced. This establishes the host
initialization path; it supplies no guest-runtime or throughput acceptance.
Its first read-only admission then stopped before packaging or VM launch:
the admission caller still selected the older decoder validator. The active
selection required correction and a regression against the untouched published
plan; historical plan validators retain their original decoder identities.
C103's pre-publication replay then exercised the same corrected loader used
by admission: the untouched C102 twenty-run plan passed, and the older
validator rejected it at the expected decoder check. This is a plan-schema
regression, with no resumed run or throughput credit.

C103 reached its first reference VM but failed qualification. During measured
`command_mix_064`, the parallel file-operation workflow returned status 1;
its timing interval contains a file-mapping `ENOMEM` and an unexpected libc
panic. The later cases, including fork/COW, completed, but controller completion
was absent and the original 900-second VM deadline expired. The whole campaign
failed, with zero accepted pairs. Its reports and serial log were preserved and
all five disposable images were removed.

A deterministic Host test exposed a separate, plausible cause: address-space
counting returned failure on a transient striped MM gate refusal, which mapping
admission reported as `ENOMEM`. Counting the same unchanged root succeeded
before and after the colliding gate was held. The fix preserves a typed Busy
outcome and lets public file and anonymous mappings release publication and
reader scopes, cooperate in an ordinary syscall context, then acquire the
current root and recount. Invalid mappings, reader saturation and genuine
address-space limit denial remain terminal; legacy boolean interfaces remain
nonblocking and fail closed. File mappings retain their original open file
description across retries. Retry work is bounded separately from geometry
preparation.

The focused gate/accounting Host profile passed 4,933 checks and its negative
controls. File-mapping integration passed 263 cases and six compiled negative
controls, including refusal with an incoming preemption guard or Task lease.
Both Normal (`bef86b5c…`) and isolated Fast (`da006b03…`) kernel builds passed.
Fresh validation builds held their complete current input sets, respectively
2,542 and 2,645 entries, before and after each build. Fresh runtime qualification
is still required. A separate, explicitly nonqualifying run of the unchanged
reference kernel completed with ordinary shutdown and cleaned its disposable
images; it did not reproduce the libc panic and no debugger attached. The
native cause of C103's panic remains unproved. Earlier correctness evidence
retains its original kernel scope; it is not a runtime result for the newly
changed kernel. Criterion ten remains open.

The fixed reference now includes the same mapping-accounting correctness
repair, without the NUMA or large-page implementation. Its Normal kernel
(`f9b3873e…`, build ID `07fbb7c0…`) built successfully, and a separate validation
build held 3,464 complete inputs before and after execution. The first build's
lack of a complete prebuild dependency hold remains recorded separately. Both
sysroots and the earlier reference were preserved.

Fresh current-kernel selftests passed in UMA and two-domain NUMA: 1,560 tests
each, with respectively 197,811 and 214,877 passing assertions, zero failures
and three skips. Both lifecycle runs passed 1,000 warmup and 1,000 measured
cycles with complete ownership checks; each NUMA phase included 500 actual
cross-node COW cases. All four runs shut down normally and passed independent
cleanup checks. The Normal single-CPU fork/COW check also passed.

Fresh Normal fairness qualification failed with a deadline error under the
weighted helper's original 60-second limit, and no accepted report was collected.
The failure logs and closed process receipts were retained, and the disposable
images were removed. Its cause is under investigation. Affinity and the next
throughput campaign have not advanced; no new throughput pair is accepted.

Three explicitly nonqualifying diagnostic boots added sparse phase logging and
a bounded atomic snapshot on deadline failure. All three completed the six
weighted comparisons and passed physical cleanup checks. They did not reproduce
the original failure. One healthy run spent about 5.5 seconds setting up its
next trial, with zero affinity `EBUSY` retries; that observation does not locate
the delay. The original fairness failure remains unresolved, and these
instrumented runs supply no qualifying fairness or throughput result.

A subsequent nonqualifying replay used the unchanged original fairness helper
and also completed all six comparisons. It finished before the conditional
live-capture threshold, so no debugger attached. Reports and independent
physical cleanup checks passed; the original deadline failure remains
unresolved, and this replay supplies no qualifying result.

A later, prospectively bounded diagnostic captured a slow run with the unchanged
helper. Its final trial had a 27.287-second setup gap, followed by passing warmup
and measurement intervals. The kernel snapshot showed correctly placed runnable
workers and no active publication-mutex, TLB, GC or teardown stall. Real host
counters showed the vCPUs receiving CPU time during the slow interval. These
observations narrow the investigation without establishing the delay's cause.
The debugger restored the running VM, and independent physical cleanup passed.

One new strict fairness confirmation was declared before execution, followed
conditionally by one strict affinity confirmation. Both passed with the original
helpers, limits, raw report oracles and current kernel. Neither used a debugger
or passive sampler. Fairness passed all six interval comparisons; affinity
completed 3,072 controller calls, 64 destination observations and 256 pipe round
trips. Separate independent checks replayed both reports, their complete source
and kernel dependency checks, process closure and disposable-image cleanup.

The earlier fairness deadline failure remains an unresolved intermittent
reliability risk. These fresh passes neither erase it nor establish a causal
repair. The ten-pair fixed-reference throughput campaign is still outstanding;
criterion ten has zero accepted performance pairs and Goal40 remains incomplete.

The next campaign preparation passed its complete saved-evidence review,
source bootstrap, initial host-state checks, and both current/reference writer
preflights. Its twenty declarations were published and permanently recorded,
but the resulting plan was 1,062,368 bytes, exceeding the unchanged 1 MiB input
limit by 13,792 bytes. Independent publication and registry audits confirmed
the declarations and refused operator release. No benchmark VM or disk image
was created for that campaign, and its plan remains immutable and ineligible.

The corrected writer encoded that same saved plan in 760,001 bytes. An actual
writer/reader regression confirmed that the original bounded reader rejects
the old representation and accepts the compact representation, preserving typed
contents, array order and all twenty declarations. The next campaign preparation
retains the existing input limits, workload, kernel artifacts and acceptance
criteria. There are still no accepted performance pairs.

The compact successor plan passed the original reader limit and independent
declaration audits. Its first reference VM completed all 117 warmup and 117
measured cases with zero recorded body or wait failures, then shut down normally.
The required independent post-run validator exceeded its original 30-second
limit while decoding saved reports. The whole campaign stopped with zero
accepted arms; the remaining nineteen arms were not launched. The reports and
failed validation log are retained. A separate fresh physical cleanup check
confirmed zero disposable image bytes, no readable process or descriptor
references to the removed images, and preservation of the existing user VM.
That cleanup check does not qualify the failed benchmark arm.

The timeout stack identifies the validation step active at the deadline, rather
than its dominant cost. A bounded saved-data comparison preserved the complete
reference proof, documents and all 5,706 input identities while using scoped
dependency-path resolution. The original API took 6.97 seconds and the scoped
version 3.53 seconds in that single ordered comparison. Both completed their
fresh closing checks under the same limits. A separate full saved-data
validation then completed in 28.44 seconds under its fresh 30-second diagnostic
limit, including raw-report replay and closing checks. It reproduced the
saved audit exactly. The failed campaign remains invalid and no performance
pair is qualified by these diagnostics.

The prospective successor declares a 60-second host validation budget while
retaining the 900-second VM limit and 1,200-second arm limit. Its stages borrow
the earlier parent deadline and reserve the additional check time before VM
launch. Guest deadlines, workload windows and performance thresholds stay fixed.

The current-kernel acceptance review now carries forward criteria one through
nine using the current accounting tests, kernel identities, seven correctness
runs and the applicable earlier topology, pressure, locality and large-page
evidence. Their recorded scopes and failures remain explicit. Criterion ten's
ten-pair throughput campaign is the remaining runtime acceptance requirement.

The successor stopped before its first VM launch. Packaging succeeded, then
startup admission rejected four shared guestfs cache entries whose modification
and change timestamps had been updated during packaging. Their paths, inodes,
sizes, ownership and allocated storage were unchanged; the cache still occupied
1,331,216,384 bytes. The validation rule had incorrectly treated this reusable
cache's historical timestamps as immutable. The two prepared disk images were
removed by startup cleanup. A separate physical cleanup observation is pending.
The campaign remains failed, with zero accepted arms or pairs; its remaining
nineteen arms were never launched. The prospective correction must measure the
cache afresh while retaining ownership, path and inclusive storage limits.

The separate read-only physical cleanup check then passed under its fresh
30-second limit. It found zero disposable image bytes, no readable same-user
process or descriptor references to the removed paths or two known parent
inodes, and unchanged required images and protected user VM. Six inaccessible
desktop-process exceptions are explicitly disclosed. Required images, current
cache and retained work totalled 11,458,875,392 allocated bytes, within 20 GiB.
This cleanup result leaves the failed campaign and its benchmark credit unchanged.

Fresh cache-accounting host tests passed 37 checks, including 22 rejection
cases. They exercised legitimate timestamp changes and allocation growth,
ownership and link checks, unique allocation accounting, storage and deadline
limits, and changes during observation. Temporary fixtures were removed; the
shared cache was neither modified nor read for content. These tests cover the
cache helper. Integration into the next campaign and actual admission replay
remain pending, with no new performance pairs accepted.

Admission endpoint tests then passed 43 checks, including 18 rejection cases.
The complete saved-data validator passed all nine stages in 40.83 seconds after
adding three omitted historical records to a new source manifest; the first
failed validation is preserved. The independent source/data review and the
69-member bootstrap and shared initial checks also passed.

The subsequent complete writer preflight stopped on an inherited function that
still required the previous campaign's freeze and review identifiers. Earlier
endpoint tests explicitly modeled that boundary and did not prove this entry
point. No nonce, plan or VM image was created. A new source version adds an
explicit current-campaign function and a direct positive/negative regression;
the ten-pair performance requirement remains open.

The new source generation passed that direct guard regression and a fresh
admission-endpoint regression. Its complete saved-data validation passed all
nine stages and five gates in 41.19 seconds, with closed process ownership.
Independent source/data review, bootstrap, shared initial checks and the complete
current/reference writer preflight also passed. The preflight recorded 9,722
closing holds and checked both 17-receipt contexts.

Once-only publication produced a 768,781-byte plan, within the unchanged 1 MiB
reader limit and its predeclared 768,895-byte upper bound. Independent audits
checked the actual bounded reader, all twenty declarations and source/spec
joins, then independently reconstructed the permanent registry as 575 prior
declarations plus twenty new ones, totalling 595. Earlier failed preparations
and campaigns remain unchanged; no declaration may be refunded or replaced.

After those audits, Root confirmed one original operator start under its
30,000-second owner deadline. Admission was in progress at the recorded launch;
no VM completion or accepted throughput sample is established by that report.
Criterion ten still has zero accepted arms or pairs, and Goal40 remains
incomplete. Criteria one through nine retain their scoped acceptance. The eleven
metrics, all 117 warmup and 117 measured cases per arm, ten paired comparisons,
95-percent median-throughput floor, raw variance and reader limits stay fixed.
Physical NUMA's ten-percent improvement remains **UNVERIFIED** under the user's
accepted virtual-NUMA substitute.


The first original reference/current pair has since passed both native runs,
independent physical cleanup and the unchanged saved-output postchecks. Their
cleanup observations took 28.60 and 28.30 seconds within the 60-second host
window; each confirmed zero disposable image bytes and all four images absent.
Both samples remain in the original twenty-run campaign. The operator advanced
to the third run; no final throughput acceptance is claimed from this prefix.

A separate numerical auditor has passed independent source review. After the
complete campaign and closed process ownership, it must replay all twenty
strict saved-arm checks, decode all 880 measured timing files and independently
recompute all eleven metrics from 220 measured samples. It preserves the ten
samples per kernel, exact median invocation rates, 95-percent floor and sample
variance; warmup values stay recorded outside the performance gate. This source
review supplies no numerical result. Criterion ten and final goal completion
remain pending.


The C107 campaign completed all twenty original runs and passed their native,
saved-output and independent cleanup checks, but **failed criterion ten's
performance gate**. Fork/COW median invocation throughput was 94.6961 percent
of the corrected Reference132 value, a 5.3039-percent regression against the
unchanged five-percent limit. The other ten metrics passed. This is a genuine
performance failure; the campaign cannot be resumed, replaced or reported as
qualified. All declarations remain spent and the failed raw data is retained.
The separate qualifying numerical auditor and public-results exporter have not
run. Investigation now targets the fork/COW path while preserving ownership,
NUMA policy and teardown correctness. Goal40 remains incomplete.

The subsequent current194 Normal kernel has build ID
`85d89de4b1498b1de6b4b4f29c9a958d7753c51f`. Three extra preemption scopes used
for selftest observations are now confined to selftest builds; the production
mapping locks and lifetime guards remain. This change has no accepted throughput
result yet. Fresh fairness and affinity confirmations passed with their original
helpers and limits. The combined current194 review accepts criteria one through
nine using the current runs and the separately scoped historical evidence; it
does not establish physical NUMA's ten-percent latency advantage.

The next campaign, C108, stopped before a VM launch because its watchdog source
binding referenced the wrong preparation directory. Its declarations remain
spent. C109 then completed five original runs before its sixth run, on the
current kernel, exceeded the unchanged 900-second VM deadline. The whole C109
campaign is failed and cannot resume or contribute partial performance samples.

Saved C109 reports show an earlier timing-helper launch reporting `ENOMEM`
after its command-mix body passed. They also show that the final fork/COW body,
END stamp and child wait completed; the later suite/controller wait has not yet
been located. Kernel source contains admission paths that can translate page
table gate contention into `ENOMEM`, but this has not been established as the
runtime cause. Separate diagnostic runs must remain outside qualification.
Cleanup and an independent host check confirmed that C109's disposable images
were removed and the required seeds and existing user VM were preserved.
Criterion ten remains unaccepted, and the earlier C107 performance failure
remains part of the record.

A separate nonqualifying replay on the unchanged current194 kernel also
reached the 900-second VM deadline. Its bounded snapshot authenticated the
kernel build ID and restored the running VM. It found 115 tasks in the bounded
run-queue census but no controller or descendants there; that does not prove
their global absence or identify their wait. Seven CPUs were idle and the
remaining CPU was servicing the file-retirement worker's sleep timer. The
observed publication, COW and pin locks were not held at that instant.

This replay recorded a fork refusal at the legacy file-mapping commit stage:
private `EAGAIN` became public `ENOMEM`. That commit does not allocate, but the
record alone cannot distinguish its context, registry-lock or snapshot-match
refusals, or connect it to C109's earlier helper failure. A shell command-path
error also does not by itself distinguish fork-before-exec from `execve`.
Stopped report and journal reads then failed because Linux refused the XFS
read-only mount with “Structure needs cleaning.” No successful filesystem or
report readback is claimed. The failed readback logs and kernel snapshot were
retained, and all disposable images and private appliances were removed.

A second separate diagnostic on the same unchanged kernel completed naturally.
All 117 warmup and 117 measured cases passed, with matching successful timing
and child-wait records. It shut down before the conditional debugger snapshot
was due. Its disposable images and appliances were removed, and an independent
host check confirmed cleanup. This is diagnostic completion, with no accepted
performance pairs and no proof that the intermittent earlier failure is fixed.

The current worktree adds a narrow fork optimization: after a locked check
proves that the source has no legacy file-mapping rows, a single-use certificate
allows the later empty commit to avoid reacquiring the registry mutex. The
certificate binds the preparation, tasks and continuously held publication
guard, including a generation that rejects guard-address reuse. Ordinary and
nonempty commits retain their original checks and rollback. Eight focused host
tests passed, including compiled negative controls. Normal Build WOS and the
isolated Fast diagnostic build also passed. Fresh UMA and two-domain NUMA
selftests each completed 1,560 tests with zero failed assertions. Both strict
lifecycle runs completed 1,000 warmup and 1,000 measured cycles with full
physical-owner accounting; the NUMA run included 500 actual cross-node COW
operations per phase. Single-CPU boot, weighted fairness and task affinity also
passed. Each of these seven runs passed its separate saved-evidence and physical
cleanup check; disposable images and appliances were removed. The fresh
seven-role index has SHA-256 `973d5d46cf434834fd9417fee731c04ba9c434dc20003979063d57102f9d3052`.

These correctness results do not establish a throughput improvement or repair
the intermittent C109 failure. The observed diagnostic refusal has not been
proved to involve an empty source. The revised kernel still requires the full
ten-pair performance campaign. Criterion ten remains unaccepted; the last
complete fork/COW result remains 94.6961 percent against the 95-percent floor.

The combined review now accepts criteria one through nine for this revised
kernel, using the seven fresh runs, their full saved-data validation and the
separately scoped evidence for unchanged topology and memory-lifetime paths.
Historical locality and large-page timings retain their original build scope;
this decision claims no new timing result or physical NUMA latency advantage.
Criterion ten still requires a complete passing ten-pair campaign.

C110 passed its source, saved-data and publication checks, then stopped before
creating a VM because the host launch command supplied the campaign plan as the
VM seed manifest. Its execution claim and all twenty identifiers remain spent;
it produced no performance samples. A separate host check confirmed that all
eighty possible disposable images were absent and the ten required seeds and
existing user VM were preserved. A checked launcher now derives the seed
manifest from the validated context and keeps it separate from the campaign
plan. Its three regression checks passed, including rejection of a wrong input
and refusal to resume the stopped campaign. A fresh complete campaign remains
required.

## Resident usercopy validation, 2026-11-04

Current-task usercopy now first attempts the existing checked, nonfaulting pin
for a resident mapping. The common pin path also permits this early check for
read access. Successful pins retain the existing task/root lease, permission,
file-binding and physical-frame checks. A failed attempt releases its owners
before entering the existing fallible preparation path. Exiting tasks retain
their preparation refusal. NUMA allocation policy and admission still apply
when a page must actually be allocated.

Normal Build WOS passed with kernel build ID
`8fadc14227f905297ebe6c40d387f0e4e49232af`. The isolated Fast diagnostic kernel
has build ID `02e137d3e54828102155538b75184ac6120e0575`. Its UMA and two-domain
NUMA selftests each completed 1,560 tests with zero failed assertions. Separate
strict UMA and NUMA lifecycle runs each completed 1,000 warmup and 1,000 measured
cycles, including physical-owner reconciliation and successful child waits.
The NUMA fixture exercised 500 cross-node COW operations per phase. Fresh
single-CPU boot, weighted fairness and task-affinity runs also passed. All seven
runs passed their saved-output checks and separate cleanup observations;
disposable images and appliances were removed.

The current review accepts criteria one through nine using these results,
source continuity and the separately scoped topology, policy, pressure,
placement and large-page evidence above. Historical locality and TLB-sensitive
timings retain their measured kernel identities. Physical NUMA's ten-percent
latency advantage remains unverified under the accepted virtual-NUMA execution.

Criterion ten still requires a complete ten-pair comparison of this Normal
kernel against the fixed Reference132, with all eleven median invocation-rate
ratios at least 0.95 and every raw sample and variance retained. The last
complete fork/COW comparison remains C107's failing 94.6961 percent. The current
kernel has no accepted ten-pair throughput result, and the earlier intermittent
C109 timeout has not been established as repaired.

## Owner performance acceptance, 2026-11-05

The owner explicitly directed: "mark the current campain as sufficient in
performance." Further effort to recover the remaining roughly 0.3
percentage-point margin was judged excessive for the expected benefit.
Criterion ten's performance requirement is therefore accepted by owner decision.
Further optimization and another ten-pair qualification campaign are not required
for this goal. The existing fairness and affinity results remain applicable.

The measured record is preserved: C107 completed ten pairs, passed ten of eleven
metrics, and recorded fork/COW throughput at 94.6961 percent of Reference132
against the original 95-percent floor. The current resident-usercopy kernel has
no complete ten-pair throughput result. This acceptance changes neither those
measurements nor the failed campaign records.

Separate diagnostics on the current kernel completed three full-suite replays
and one observed replay, each with 117 warmup and 117 measured cases passing.
The observed run also verified bounded live progress collection and temporary
SSH-forward removal. These runs provide diagnostic evidence only. An
intermittent full-suite timeout remains unexplained; no timeout fix or physical
NUMA latency benefit is claimed. Further performance qualification work stops
under the owner's decision.

The separate Git/Clang diagnostic stress fixture subsequently completed all
100 pairs (200 successful case exits), followed by stress SSH exit zero and
ordinary WOS shutdown. Its overall diagnostic receipt remains failed because
the supplemental journal was 1,147,296 bytes, exceeding the unchanged 512 KiB
capture bound. No journal truncation or rerun was used. Independent host checks
confirmed process, temporary-forward and disposable-image cleanup; required
seeds and the user's VM were preserved. This diagnostic does not establish a
fix for the earlier timeout.
