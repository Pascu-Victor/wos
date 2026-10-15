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
partial masks. NUMA allocation policy and general 2 MiB user mappings are still
under implementation. Topology discovery alone does not qualify either feature.

Task defaults are stored per executor under a short IRQ-safe snapshot lock.
Creation copies the initiating executor's value independently of its credential
owner. Owned exec preparation carries the value to successful canonical image
commit; failed or canceled preparation preserves the existing default. These
internal paths have no public policy selector yet.

The internal placed allocator supports preferred, strict-bind and deterministic
virtual-page interleave selection. A checked interval claim returns either one
frame or 512 absolutely aligned, independently owned order-0 frames. Move-only
ownership frees an uncommitted span. This helper has no public selector and is
not yet connected to anonymous faults or large user mappings.
It bypasses unfiltered CPU caches for domain-constrained claims; cached capacity
can therefore cause conservative allocation failure. Allocation-pressure
bookkeeping can wake reclaim, but the attempt itself does not reclaim or wait.

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

These checks establish the internal foundation. User first touch, per-node
reserves and pressure policy, 2 MiB mapping lifetime, map/fork/pin/unmap stress and paired
locality/UMA throughput measurements remain required.
