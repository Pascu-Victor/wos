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
allocator metadata and describe managed coverage, not free capacity. The regular
zone descriptor page is currently outside those ranges, although global physical
balance counts it; these topology totals are not the global PMM balance. Existing
ACPI initialization still requires a usable RSDP.

The parser supports 256 CPU identities and 64 proximity domains. The current
scheduler/affinity ABI supports 1–64 online CPUs and requires the BSP at logical
index zero; boot explicitly rejects unsupported enumeration before publishing
partial masks. NUMA allocation policy and general 2 MiB user mappings are still
under implementation. Topology discovery alone does not qualify either feature.

The internal placed allocator supports preferred, strict-bind and deterministic
virtual-page interleave selection. A checked interval claim returns either one
frame or 512 absolutely aligned, independently owned order-0 frames. Move-only
ownership frees an uncommitted span. This helper has no public selector and is
not yet connected to task inheritance, anonymous faults or large user mappings.
It bypasses unfiltered CPU caches for domain-constrained claims; cached capacity
can therefore cause conservative allocation failure. Allocation-pressure
bookkeeping can wake reclaim, but the attempt itself does not reclaim or wait.

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

These checks establish the internal foundation. User first touch, per-node
reserves/accounting, 2 MiB mapping lifetime, map/fork/pin/unmap stress and paired
locality/UMA throughput measurements remain required.
