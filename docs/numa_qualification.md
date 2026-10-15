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
partial masks. Public preferred/bind/interleave policy and general 2 MiB user mappings remain
gated. Topology discovery alone does not qualify either feature.

Task defaults are stored per executor under a short IRQ-safe snapshot lock.
Creation copies the initiating executor's value independently of its credential
owner. Owned exec preparation carries the value to successful canonical image
commit; failed or canceled preparation preserves the existing default. These
internal paths support saved policy values. The public setter currently admits
only LOCAL with an empty node mask; other well-formed modes return ENOTSUP.

The internal placed allocator supports preferred, strict-bind and deterministic
virtual-page interleave selection. A checked interval claim returns either one
frame or 512 absolutely aligned, independently owned order-0 frames. Move-only
ownership frees an uncommitted span. Private lazy anonymous and COW allocation
consume the supplied executor's copied policy and a captured CPU-node value.
Hardware origin uses the saved exception CPL; software ensures remain separately
classified. Installed private-copy frames, zero-COW and new anonymous pages have
separate counters; remote fallback remains a miss in an eligible locality count.
Public non-LOCAL selection and large user mappings remain gated.

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
It exports no physical address or kernel root identity. The LOCAL-only setter
updates the actual executor, independently of its credential/quota owner.
Version, size, geometry and reserved fields are checked. Public non-LOCAL modes
remain unavailable while connected placement/pressure qualification continues.
The existing strace decoder names both appended operations.

`testprog numa-first-touch` uses real pinned pthread workers, verifies all pages
are absent before volatile CPL3 writes, checks zero tails and records every
resident page in its local 95% denominator. `testprog numa-lifecycle` defaults to
1,000 fresh mmap/fork/COW/cross-page pipe-usercopy/wait/unmap cycles, with checked
FD/map cleanup and bounded waits. These commands are built; their scheduled
qualification is pending. Their own-resource ledger explicitly does not claim
a global Task/File/pin/physical census, persistent pins, huge pages or pressure.

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
