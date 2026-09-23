# Goal 20: IOMMU-Backed Device-Scoped DMA Ownership

## Planning metadata

- **Target horizon:** one to two continuous weeks with VT-d, DMA API, and driver-conversion subagents.
- **Critical surfaces:** ACPI/PCI, page tables and invalidation, bus mastering, IRQ/NAPI, DMA buffer lifetime, xHCI, AHCI, virtio, e1000e, and ivshmem.
- **Completion rule:** every in-scope PCI DMA address must originate from the common mapping API and teardown must leave zero live mappings.

## Copy-pastable goal command

```text
/goal Outcome: Add a managed kernel DMA API and IOMMU-backed device domains so each PCI device can access only explicitly mapped buffers, bus mastering starts only after domain setup, and mapping, synchronization, fault, quiesce, and teardown ownership are auditable. | Verification surface: VT-d/page-table model tests, map/unmap and failure-unwind accounting, deliberate stale/unmapped DMA faults, Normal Build WOS, isolated KTEST, QEMU Intel-IOMMU boots with AHCI/virtio-net/e1000e/xHCI/ivshmem workloads, plus user-run hardware evidence where needed. | Constraints: Never unmap or free hardware-visible memory before device quiescence and IRQ drain; preserve DMA masks, cache/coherency rules, xHCI retirement, block durability, packet ownership, WKI fencing, and nonblocking IRQ/NAPI behavior. | Boundaries: Implement ACPI DMAR discovery, device grouping/domains, coherent and streaming DMA APIs, fault diagnostics, fallback policy, and convert in-scope PCI drivers; do not redesign raw WKI RDMA security, add every IOMMU vendor, or rewrite driver protocols. | Iteration policy: Land pure table/invalidation models, then boot-time discovery and identity/fallback domains, common allocation/mapping, one driver family at a time, teardown/fault injection, and finally strict enforcement. | Blocked stop condition: Stop only after the same indispensable hardware/runtime blocker repeats for three goal turns, QEMU/model/KTEST alternatives are exhausted, and the exact missing trace or platform capability is documented.
```

## Starting source evidence (historical)

`modules/kern/src/dev/pci.cpp` enables PCI bus mastering directly. There is no kernel DMAR/IVRS/IOMMU implementation. xHCI and e1000e each implement private virtual-to-physical translation and program raw physical addresses; AHCI and virtio allocate ordinary physical pages directly for rings and buffers. PCI BAR mappings and device discovery likewise lack one general retained resource/unmap object.

These patterns work only while every device is trusted with unrestricted physical memory and every driver perfectly orders teardown. Goal 05 hardened xHCI request lifetime, but did not constrain the addresses hardware can access.

## Measurable completion contract

1. ACPI DMAR parsing validates lengths, scopes, reserved regions, units, and device identities with malformed-table tests.
2. Device groups and domains have explicit ownership, supported address width, reserved mappings, and invalidation rules.
3. Coherent allocation and streaming map/unmap APIs define alignment, masks, direction, synchronization, pinning, and failure behavior.
4. Bus mastering stays disabled until a domain and all required boot mappings are live; failure unwinds mappings before freeing frames.
5. AHCI, virtio-net, e1000e, xHCI, and applicable ivshmem paths obtain every programmed DMA address through the common API.
6. IRQ, hot-unplug, timeout, reset, and fence paths quiesce hardware and drain completion admission before unmapping.
7. Fault records identify device, domain, address, access type, and generation without allocating or recursively logging in fault context.
8. Feature-unavailable policy is explicit and observable: strict failure or documented passthrough, never a false claim of isolation.
9. Mapping/page accounting proves no leaked or premature-freed DMA region after success, probe failure, reset, or detach.

## Invariants, locks, and unsafe contexts

Invalidation completion precedes frame reuse. Device and domain references remain pinned through IRQ completion. Fault and interrupt handlers perform bounded nonblocking work. Lock ordering between PCI registry, IOMMU units/domains, driver locks, allocator, and IRQ retirement is documented and model-tested. No DMA mapping silently exposes unrelated adjacent pages.

## Boundaries and overlap

In scope are Intel VT-d first, a vendor-neutral DMA API, PCI admission/resource lifecycle, listed drivers, diagnostics, and tests. AMD IOMMU, userspace device passthrough, SR-IOV, and WKI's cryptographic RDMA record are out of scope. Coordinate with Goal 25 for secure RDMA and avoid concurrent xHCI lifecycle edits with Goal 05 regression work.

## Parallel workstreams

The primary agent owns architecture, grouping, boot/fallback policy, and integration. Subagent A implements and fuzzes DMAR/table/invalidation logic. Subagent B builds the generic DMA/resource API and accounting. Subagent C converts network/virtio; Subagent D may convert AHCI/xHCI only if a concurrency slot and disjoint ownership are available.

## Verification plan

- Run formatting and the Normal `Build WOS` task.
- Run pure host table/domain/invalidation and driver source/semantic tests.
- Run isolated KTEST with deliberate mapping, stale-generation, teardown, and fault cases.
- Ask the user to run QEMU with IOMMU on/off and relevant hardware, providing serial, fault, device, and sanitizer logs.

## Iteration, rollback, and blocked behavior

Keep common DMA APIs usable in an explicit passthrough mode before strict translation is enabled. Convert and qualify one driver at a time. Roll back strict enforcement per device, never by reporting isolation while silently programming unrestricted physical addresses. Use the `/goal` blocked condition exactly.

## Active execution (2026-09-17)

Status: Completed (2026-09-23), for the documented Intel VT-d/PCI subset with QEMU qualification. The original goal string above is unchanged. Physical-platform qualification limits are recorded below.

User-authorized addition to the active goal: You are able and authorized to run and debug WOS live by appending --no-setup to the wos-ktest and wos-cluster scripts to not need root access.

Reconnaissance, patch contracts, evidence, and pending qualification are recorded in `docs/goal20_dma_ownership.md`.

### Stage 1: DMAR and VT-d foundations

Implemented bounded DMAR decoding, legacy VT-d entry construction and invalidation sequencing, host/fuzz/KTEST cases, and rootless KTEST backend preservation. Normal and isolated builds, 18 host cases, 30 cluster cases, and final 5,016,084-input fuzzing pass. Final rootless isolated KTEST completed with 60,701 assertions passed and 0 failed, both new suites passed, the coverage fingerprint matched the saved ELF, and the VM powered off normally. This does not complete the goal or enable hardware isolation. See `docs/goal20_dma_ownership.md` and `build/goal20-stage1/` for evidence and remaining work.


### Stage 2: bounded boot discovery (2026-09-22)

Added bounded ACPI lookup, retained DMAR inventory, legacy VT-d capability
qualification before AHCI, and opt-in rootless QEMU IOMMU fixtures. Normal
Build WOS and isolated diagnostic builds pass, as do 27 host cases, DMAR fuzz
smoke, and 31 launcher cases. Live Intel-IOMMU-on KTEST completed with 60,715
assertions passed and 0 failed; its KCOV fingerprint matches the retained ELF.
An IOMMU-off boot using the same packaged image confirmed explicit absence,
AHCI initialization and selftest entry, then was intentionally stopped (not a
full off-mode selftest pass). See `docs/goal20_dma_ownership.md` and
`build/goal20-stage2/` for receipts and limitations. The original source-evidence
paragraph describes the starting tree; discovery now exists, but managed DMA,
device domains, driver conversion and enforcement are still pending. Goal active.

### Stage 3: PCI topology and scope ownership (2026-09-22)

PCI enumeration now retains ancestry and reports incomplete topology. The
bounded DMAR scope qualifier resolves multi-hop paths, unit ownership and
RMRR membership, rejecting ambiguous/unsupported results without partial
bindings. Both builds, 35 host cases, 4,920,735 fuzz inputs, and live rootless
IOMMU-on KTEST passed (60,751 assertions, 0 failed; normal poweroff and matched
KCOV/ELF fingerprint). QEMU coverage is flat PCI; nested bridges are model-tested.
Receipts and outstanding domain/DMA/driver/enforcement work remain documented in
`docs/goal20_dma_ownership.md`. Goal active; hardware isolation is not enabled.

### Stage 4: explicit passthrough domains and PCI admission (2026-09-22)

Added owned VT-d root/context tables, bounded activation and invalidation,
cache publication, fallible preparation/unwind, physical table accounting,
serialized PCI config cycles and bus-master admission, and explicit AHCI
admission. Unsupported bridge aliases use software fallback; strict mode
halts until managed translation is implemented. Both builds and 45 host cases
pass. Final rootless IOMMU-on KTEST: 60,816 assertions passed, 0 failed, matched
KCOV/ELF fingerprint and normal poweroff. Focused absent/off/strict/bridge boots
and a live partial-allocation unwind check passed; these shorter runs are not
full-suite results. See `docs/goal20_dma_ownership.md` and
`build/goal20-stage4/` for evidence. Goal remains active: common DMA APIs, driver
conversion, translated isolation, retirement, faults and full qualification
remain pending. Passthrough does not provide DMA isolation.

### Stage 5: common DMA ownership API (2026-09-22)

Added coherent allocations, exclusive streaming bounce mappings, checked
masks/alignment/extents, generation-qualified submissions, explicit retirement,
release/quarantine, and physical backing accounting. The same manager is used
by kernel and host tests. Normal and isolated builds pass, as do 56 host cases.
Final rootless IOMMU-on KTEST passed 65,912 assertions with 0 failures and normal
poweroff; its coverage fingerprint matches the saved ELF. New tests exercise
real allocations and CPU-emulated bounce writes, not actual device commands.
See `docs/goal20_dma_ownership.md` and `build/goal20-stage5/` for evidence and
limits. Goal remains active: reusable streaming bindings, driver conversion,
translated isolation, retirement/fault handling and full qualification remain.

### Stage 6: AHCI managed DMA conversion (2026-09-22)

AHCI now uses managed coherent rings and reusable streaming pools for every
programmed DMA address, with bounded command/engine waits, ordered engine stop,
explicit retirement, probe unwind and uncertain-failure quarantine. IDENTIFY,
read/write and durable flush share the serialized command path. Normal and
isolated builds passed; 62 host unit cases plus source/fuzz checks passed.
Rootless Intel-IOMMU KTEST passed 65,943 assertions, zero failures, with normal
poweroff and an ELF-matched coverage fingerprint. Deliberate failure after
seven probe allocations returned DMA accounting to zero without publishing a
disk. Evidence and limitations are in `docs/goal20_dma_ownership.md` and
`build/goal20-stage6/`. Goal remains active: AHCI full teardown, other driver
conversions, translated isolation, fault handling and full qualification remain.

### Stage 7: AHCI full mapping retirement (2026-09-22)

Added terminal AHCI command-admission closure, bounded reader drain, final
flush, engine/BME quiescence, mapping release and failure retention/quarantine.
Normal power finalization calls it after VFS unmount and before CPU halt.
Per-device DMA accounting and final-phase KTEST verify physical ownership,
late callbacks, idempotence and refusal to reopen retired storage. Normal and
isolated builds passed. Full Intel-IOMMU KTEST passed 65,960 assertions with
zero failures, including release of all 210 AHCI regions / 13,068 pages;
three focused retirement/failure boots passed and powered off normally.
The runtime manifest audit covers all 1,164 enabled tests. Two broader source
inventory checks have independently reproduced baseline failures, documented
in `docs/goal20_dma_ownership.md` and `build/goal20-stage7/`. Goal remains active:
remaining drivers, translated isolation, real DMA faults and broader runtime/
hardware qualification are still required.


### Stage 8a: virtio ring/control DMA ownership (2026-09-22)

Converted split rings and the MQ control page to owned common DMA regions.
Probe prepares persistent mappings before BME, validates reset acknowledgement
before freeing exposed storage, retains uncertain backing, and prevents unsafe
modern-to-legacy retry. Packet DMA and published NIC teardown are still pending.
Both builds, format checks and 77 host cases passed. Four focused rootless boots
confirmed successful modern/legacy/IOMMU-absent release and quarantine on withheld
reset proof. A real `MQ pairs=1` control command was acknowledged; this does not
qualify multiqueue traffic. Full IOMMU-on KTEST passed 65,970 assertions, zero
failures, with 1,165 enabled cases audited and normal poweroff. All five logs
match the saved ELF; task-owned VMs exited. Two existing audit failures remain
unchanged. See `docs/goal20_dma_ownership.md` and `build/goal20-stage8a/` for
receipts and remaining scope. Goal active; passthrough provides no isolation.

### Stage 8b: virtio packet streaming DMA (2026-09-22)

Converted RX/TX descriptors to preallocated directional streaming DMA buffers,
with bounded completion checks before descriptor reuse and packet delivery/free.
Uncertain reset retains device-visible backing while detaching packet borrows.
Corrected modern non-mergeable header size and qualified both legacy header
modes. Both builds, formatting and 85 host cases passed. Six focused rootless
boots and the full KTEST boot each verified intact 64-byte and 9,014-byte packet
echoes. Full KTEST passed 65,973 assertions, zero failures; all 1,165 enabled
cases were audited. Virtio retired 516 regions / 2,059 pages and restored the
packet pool; withheld reset proof instead quarantined all exposed backing.
All seven logs match the retained ELF and all task-owned VMs exited. Existing
broader audit failures are unchanged. Receipts and limits are in
`docs/goal20_dma_ownership.md` and `build/goal20-stage8b/`.

Goal remains active: published NIC teardown, concurrent IRQ/NAPI/SMP and
multiqueue workloads, other PCI drivers, translated isolation, actual DMA
faults and hardware qualification are still outstanding.

### Stage 8c: terminal virtio callback and DMA retirement (2026-09-22)

Added monotonic callback admission/drain, explicit IRQ ownership and checked
interrupt masking, terminal reset/BME/IRQ retirement, stable closed device/NAPI
objects, refused reopen/reprobe, and normal power-finalizer integration. Missing
proof retains backing. Both builds, formatting, 91 host cases and four focused
rootless boots passed. Full KTEST passed 65,977 assertions, zero failures, with
all 1,165 enabled cases audited. Actual netdevice/IRQ publication, two packet
echoes, mapping release/quarantine, late callback rejection and repeated shutdown
were checked on serialized BSP. All five logs match the retained ELF; task-owned
VMs exited. Existing broader audit failures remain documented.

Live SMP/NAPI retirement is still pending. Source inspection found that
cooperative daemon exit at a yield can strand a callback reference; the bounded
drain retains DMA safely, but successful shutdown under that race needs further
work. See `docs/goal20_dma_ownership.md` and `build/goal20-stage8c/`. Goal active;
remaining drivers, translated isolation and real DMA-fault qualification remain.

### Stage 8d: live SMP/NAPI DMA retirement (2026-09-22)

Added scoped cooperative-shutdown deferral for virtio callbacks/retirement and
retained NAPI worker ownership. Live debugging also fixed skipped shutdown
targets during concurrent task removal and stale KASan shadow left by abandoned
scheduler frames; cleanup preserves live-frame shadow and is absent in Normal
Build WOS. Both builds, formatting, 98 host cases and four source checks passed.
Four rootless four-CPU live boots passed with real IRQ/NAPI traffic, verified
64-byte/9,014-byte echoes, held-callback shutdown deferral, IRQ drain and late-call
rejection. Successful retirement released all 514 regions; withheld reset proof
quarantined all 514. All cases reached ordinary power finalization and AHCI
release. Full isolated Intel-IOMMU KTEST passed 65,977 assertions, zero failures;
all 1,165 enabled cases were audited. All five packaged kernels match the retained
ELF, the full coverage fingerprint matches, and every task-owned VM exited.
Existing broader inventory-check failures remain documented baseline issues.

Evidence and the next e1000e conversion reconnaissance are recorded in
`docs/goal20_dma_ownership.md` and `build/goal20-stage8d/`. Live qualification is
one queue pair on four CPUs; arbitrary scheduler starvation, multiqueue and
physical hardware remain unqualified. Goal active: passthrough still provides
no DMA isolation. e1000e/xHCI/ivshmem conversion, translated/grouped domains,
fault diagnostics, deliberate stale/unmapped DMA and strict enforcement remain.

### Stage 9a: e1000 managed DMA and terminal retirement (2026-09-22)

Converted descriptor rings and packet buffers to managed DMA, with map-before-BME
probe, bounded RX completion handling, TX ownership, callback shutdown scopes,
IRQ drain and failure quarantine. Only 82574 currently has a qualified master
drain release path; other models retain exposed backing. Both builds, formatting
and 106 host cases passed. Five rootless focused boots cover normal 82574,
withheld stop proof, failed allocation, legacy e1000 and IOMMU absent. Traffic
cases each verify 300 64/1514-byte echoes across ring wrap and restore CPU packet
ownership. Full isolated KTEST passed 65,975 assertions, zero failures; all
1,166 enabled cases audited. Boot manifests and coverage fingerprints match the
retained ELF. Existing broader audit failures remain baseline issues; the stale
virtio RX-reservation source check was corrected to inspect production probes.

Evidence: `build/goal20-stage9a/` and `docs/goal20_dma_ownership.md`. The focused
e1000 tests use serialized BSP polling, not live concurrent IRQ/NAPI. Goal active:
SMP qualification, per-model drain proof, other drivers, translated isolation,
actual DMA faults and strict enforcement remain. A documented Intel reset-access
delay correction is being validated separately as stage 9b.

### Stage 9b: e1000 reset access timing (2026-09-22)

Added the Intel-required pause after CTRL.RST before reading device registers,
using the existing HPET delay. Both builds, formatting and focused host checks
passed. Modern and legacy rootless boots each verified 300 echoes, matched the
new ELF and powered off normally. Stage 9a full-suite evidence remains preserved
under its prior ELF. See `build/goal20-stage9b/`. Live SMP/NAPI qualification is
the next e1000 step; the goal remains active and translated isolation is absent.

### Stage 9c: live e1000 IRQ/NAPI retirement (2026-09-22)

Added a diagnostic-only live harness. Both builds, formatting and 23 selected
host targets (106 unit cases) passed. Four rootless four-CPU boots cover 82574
with/without IOMMU, withheld stop proof and legacy e1000. Each verified at least
160 replies of each 64/1514 size through actual IRQ/NAPI activity, cross-CPU
traffic, a held callback during retirement and cooperative shutdown deferral.
Normal 82574 releases 514 regions/1,282 pages; withheld and legacy retain all
backing. CPU owners detach, IRQs drain, BME disables, late callbacks reject and
repeated retirement is stable. All reached ordinary finalization/AHCI release
and exited. Boot manifests match the retained ELF; WOSDBG analysis passed.

Evidence: `build/goal20-stage9c/` and `docs/goal20_dma_ownership.md`. Full KTEST
was not rerun for diagnostic-only hooks; earlier stage 9a evidence is retained.
Physical hardware, non-82574 drain proof and additional fault injection remain
unqualified. xHCI reconnaissance is recorded for the next driver conversion.
Goal active: translated DMA isolation, remaining drivers and real DMA faults
are still unfinished.


### Stage 10a: xHCI controller DMA (2026-09-22)

Controller DCBAA, command/event rings, ERST and scratchpads now have managed
coherent ownership, checked masks and halt/IRQ-drained probe retirement.
Live testing found and fixed ERST publication while BME was off; startup now
rejects HCE/HSE. Both builds, formatting and 26 host targets/120 unit cases
passed. Final full KTEST: 65,982 assertions, zero failures; four focused
failure/fallback boots and 32 live USB attach/detach cycles passed. Receipts,
matched ELF/source and checker limitations: build/goal20-stage10a-final and
docs/goal20_dma_ownership.md. Goal active: slot/transfer conversion, controller
terminal shutdown, ivshmem, translated isolation and fault/strict enforcement
remain incomplete. Passthrough still provides no DMA isolation.


### Stage 10b: xHCI slot DMA and retirement (2026-09-22)

Device/input contexts and endpoint rings now use managed coherent mappings.
Slot release requires successful endpoint/slot retirement and drained CDC
readers; unproven mappings remain pinned. Ring-lock admission closure prevents
late doorbells, and the request waiter no longer treats unavailable snapshots
as completed DMA. Both builds, formatting and 26 host targets/126 unit cases
passed. Final live evidence: 32 USB detach/reuse cycles, partial-allocation
unwind, five-region withheld-stop quarantine, a four-CPU held-publication
race, and two focused KTEST boots (39 assertions each, zero failures).
Matched ELF/source and receipts: build/goal20-stage10b and
`docs/goal20_dma_ownership.md`. Previous full KTEST is retained under Stage10a;
it is not a full-suite claim for this newer ELF. Transfer streaming/64KiB
segmentation, terminal xHCI shutdown, ivshmem, translated isolation, faults,
strict enforcement and final qualification remain. Goal active.

### Stage 10c: xHCI streaming DMA and terminal completion (2026-09-22)

Control/bulk buffers now use directional managed DMA bound to owned CPU
storage. Transfer plans split at 64KiB boundaries; short control events wait
for Status and bulk completion uses a unique terminal Event Data cookie.
Errors retain backing until hardware and readers are proven retired.
Both builds, formatting and 26 host targets/135 unit cases passed. Seven
rootless boots verified 32 detach/reuse cycles, held-publication cancellation,
allocation unwind, withheld-stop retention, 64 exact USB echoes and focused
IOMMU-on/off KTEST (39 assertions each, zero failures). All receipts match the
retained ELF/source; WOSDBG checks passed and task VMs exited. Evidence:
build/goal20-stage10c and docs/goal20_dma_ownership.md.

Actual USB payloads used single-segment plans; larger multisegment payload
qualification remains. Prior full KTEST remains tied to Stage10a's ELF.
Published xHCI terminal shutdown, ivshmem, translated isolation, real faults,
strict enforcement and final qualification remain. Goal active.

### Stage 10d: terminal xHCI DMA retirement (2026-09-22)

Added monotonic lifecycle/I/O admission, cooperative worker ownership,
bounded class I/O drain, halt/BME/IRQ retirement and ordinary power-finalizer
integration. Closed metadata remains valid for late callers; uncertain
hardware or software retirement retains backing. Both builds, formatting and
26 host targets/137 unit cases passed. Five four-CPU live shutdown cases
verified held publication, 128 exact USB echoes, IOMMU absence, normal release
to zero, withheld-halt quarantine, repeated shutdown and late-call rejection.
A separate 32-cycle hotplug regression passed.

Full isolated KTEST on the final ELF passed 65,982 assertions with zero
failures; all 1,167 enabled cases were audited and the coverage fingerprint
matched. All seven boot manifests and 103 source hashes match the retained
artifacts; 14 WOSDBG checks passed. Task VMs exited; unrelated QEMU was untouched.
Evidence: build/goal20-stage10d/validation.json and docs/goal20_dma_ownership.md.

Goal remains active. Larger multi-TRB USB payload qualification, applicable
ivshmem ownership/admission, translated grouped domains/RMRR policy, actual
DMA faults, strict enforcement and final qualification remain. Current
passthrough domains still provide no DMA isolation.

### Stage 11a: ivshmem PCI resource admission (2026-09-22)

Added a shared retained PCI resource owner for ivshmem-net and WKI: validated
revision-1 BAR/MSI-X extents, BME-off preparation, IRQ ownership before MSI-X
activation, correct doorbell encoding, plain mode with no IRQ/BME, checked
failed-activation mask/drain, and no reuse of failed probe metadata. Shared
BAR memory remains CPU-addressed and pinned; no guest-RAM DMA mapping or peer
revocation is claimed. Extended the shared node launcher for explicit doorbell
fixtures and repaired KTEST conversion that dropped explicit ivshmem devices.

Normal Build WOS and isolated diagnostic builds, format checks, 29 host targets
(143 GTest cases), and 32 launcher regression cases pass. Eight focused rootless
boots cover unpublished plain/MSI-X/undersized-BAR/IOMMU-off ownership plus
publication of both drivers in both modes. A nonzero-ID self-doorbell delivers
one real MSI-X interrupt, followed by successful mask/drain and BME-off.
The four publication fixtures end through private QMP quit and do not prove
published teardown or inter-peer traffic. The final 32-CPU isolated full KTEST
boot passes 65,983 assertions with zero failures and powers off normally;
manifest audit verifies 1,168 enabled tests and three disabled tests. All nine
boot manifests, 113 source hashes, coverage fingerprints and 18 WOSDBG calls
validate against the retained final ELF. Evidence:
`build/goal20-stage11a/validation.json` and `docs/goal20_dma_ownership.md`.
All task-owned VMs exited; the unrelated host QEMU was left untouched.

Goal remains active. Next ivshmem work is published callback closure/IRQ drain,
shared-ring and offset bounds, and traffic qualification. Larger multi-TRB USB
payloads, translated grouped domains/RMRR, actual DMA faults, strict enforcement
and final qualification remain. Current passthrough domains do not isolate DMA.
The original goal string and --no-setup live-debug authorization remain intact.

### Stage 11b: published ivshmem shutdown ownership (2026-09-22)

Added common terminal callback admission and bounded retirement for net/WKI
ivshmem. Callback ownership survives cooperative task shutdown; late calls
cannot touch BARs or rearm vectors. WKI IRQ dispatch is deferred to its task
poll, and registry/chaos transport leases close and drain before unregister.
Core shutdown retains peer/auth state when drain or IRQ-stop proof is missing.
The ordinary power finalizer retires ivshmem-net. Failed proofs retain ownership
and BAR mappings; raw peer access and escaped pointers remain unrevoked.

Normal Build WOS, isolated diagnostic build, formatting and all 34 selected
host targets pass (200 GTest cases plus source, launcher and fuzz checks).
Twelve rootless published-driver fixtures pass: plain/MSI-X, held admission,
failed drain/stop proof, independent WKI deferred leases and ordinary poweroff.
Four unpublished PCI regressions also pass. All fixtures power off normally.
These are callback/IRQ ownership proofs, not authenticated two-peer workloads.

Final 32-CPU isolated KTEST: 65,983 assertions passed, zero failed; runtime
audit confirms 1,168 enabled tests passed and three disabled tests skipped.
The e1000 workload completed 300 exact echoes with packet-pool restoration.
All seventeen manifests, 121 archived source hashes, coverage fingerprints
and 34 WOSDBG calls validate against the retained final ELF. Evidence:
`build/goal20-stage11b/validation.json` and `docs/goal20_dma_ownership.md`.
All task-owned VMs exited; the unrelated host VM was untouched.

Goal remains active: shared-BAR bounds/traffic, live multi-TRB USB payloads,
translated grouped domains/RMRR, real DMA faults, strict enforcement and final
qualification remain. Passthrough still provides no DMA isolation. The exact
original goal string and appended --no-setup live-debug authorization remain.

### Stage 12a: translated page-table mutation and ownership engine (2026-09-22)

Added a VT-d 4-KiB page-table engine and kernel table-storage backend. Whole
ranges validate before mutation; zeroed child tables publish before parent
links; invalidation precedes map/unmap success. Uncertain invalidation retains
data/table ownership. Root exposure checks owner/address/generation, rejecting
stale snapshots even when physical root addresses are reused. Busy mutations
do not wait under allocator/IRQ locks. Exposed table storage is never implicitly
freed. These components are not yet connected to live translated domains.

Normal Build WOS, isolated diagnostic build, formatting and 35 host targets
pass (211 GTest cases, including eleven new engine cases). Final rootless
32-CPU KTEST passes 66,074 assertions with zero failures; all 1,171 enabled
tests pass and three disabled tests skip. Three new kernel tests verify real
table-page allocation/leaf contents, failure unwind/accounting, and qualified
CLFLUSH publication. Their roots remain unpublished, so these tests do not
prove hardware DMA translation. The existing 300-packet e1000 echo and terminal
regressions also pass, followed by normal poweroff.

The final boot manifest, ELF coverage fingerprint, 126 archived source files,
runtime inventory and WOSDBG checks validate. Evidence is in
`build/goal20-stage12a/validation.json` and `docs/goal20_dma_ownership.md`.
An earlier run was intentionally interrupted while adding the root-generation
guard; its separate archive is not claimed as a full-suite pass. All task-owned
VMs exited and the unrelated host VM was untouched.

Goal remains active: connect the engine to DMA/per-device translated contexts,
qualify groups/RMRR and actual DMA faults, implement strict enforcement, and
complete the remaining shared-BAR and multi-TRB USB workloads. Current PCI
domains remain passthrough and provide no DMA isolation. The original goal
string and appended --no-setup authorization remain unchanged.

### Stage 12b: opt-in live translated DMA (2026-09-22)

Connected per-requester VT-d contexts and the real page-table engine to managed
DMA with `--iommu-translated`. Each requester has a distinct initially empty
tree. Exact directional PTEs are published and invalidated before use; release
verifies original physical extent/permissions and invalidates before freeing.
Unit-wide uncertainty closes admission and inhibits BME, with PCI writes
serialized against quarantine. Exposed tables remain owned for kernel lifetime.
Default passthrough is preserved. Unsupported translation requests fail closed;
strict mode remains unavailable pending full qualification.

Normal Build WOS, isolated diagnostic build, formatting and 35 host targets
pass (216 GTest cases). The final translated 32-CPU KTEST passes 66,147 assertions
with zero failures, all 1,172 enabled tests, and normal power-off. Active context
and PTE inspection verifies distinct roots/DIDs, exact permissions, unmapped
neighbors/foreign domains and cleared mappings after release. Native AHCI,
e1000e and xHCI workloads pass; the full run completes 300 exact packet echoes.

Additional final-ELF fixtures pass: default passthrough retirement; translated
four-CPU IRQ/NAPI traffic and held-callback retirement (326 exact host frames,
all 514 NIC regions released); withheld stop proof (556 exact frames, 514 regions
retained); and five fail-closed boot-policy checks. A live register read during
filesystem activity shows translation enabled and fault status clear. This is
positive translated-DMA evidence, not deliberate hardware-fault evidence.

All nine final boot manifests, 126 archived source hashes, full runtime
inventory, coverage fingerprint and 18 WOSDBG calls validate. Evidence and
remaining work: `build/goal20-stage12b/validation.json` and
`docs/goal20_dma_ownership.md`. All task-owned VMs exited; the unrelated host VM
was preserved. The initial smoke ELF is archived separately from final evidence.

Goal remains active: actual stale/unmapped DMA faults, fault IRQ diagnostics,
live failed-invalidation quarantine, virtio ACCESS_PLATFORM, groups/RMRR,
strict enforcement, shared-BAR peer workloads, multi-TRB USB and necessary
hardware qualification remain. The original goal string and appended
--no-setup authorization are unchanged.

### Stage 12c: VT-d fault capture and deliberate forbidden DMA

Implemented bounded allocation-free fault IRQ capture, immutable raw requester/
domain/address/access/reason evidence, task-context allocation diagnostics and
unit-wide admission closure/BME inhibition. Invalidation cannot return success
following observed fault closure; uncertain DMA backing stays pinned. The
worker starts after BSP GS/per-CPU initialization. An initial earlier-start
attempt reset the guest and is archived separately as failed evidence.

Final rootless live foreign-page and revoked-but-retained-page reads both
produced the expected real VT-d IRQ, exact SID/DID/address and read reason 6.
Backing guards remained unchanged; all unit requesters refused BME/new DMA,
and NIC retirement drained IRQs while retaining uncertain pages. A distinct
withheld-invalidation-proof case passed quarantine checks with no synthetic
hardware fault. These three VMs were intentionally QMP-stopped. The foreign
case also had a userspace fault after the shared IOMMU unit, including AHCI,
was inhibited; continued filesystem operation is not claimed.

Normal Build WOS, isolated diagnostic build and formatting passed. Freshly
rebuilt host targets: 36 passing, 223 GTest cases. Correction to stage 12b:
its broad host report included a stale virtqueue test binary with an outdated
unmap mock; this checkpoint fixes the mock and verifies rebuilt executable
hashes. Full translated KTEST: 66,179 assertions, zero failures, all 1,172
enabled tests passing and three disabled skips; normal power-off. Positive
four-CPU live regression: 568 exact frames, all 514 NIC regions/1,282 pages
released, normal power finalizer and AHCI release.

Final ELF: `d0647323f8428e7535facf43f3d123cd0a2b04a309d8b24005f996871a9cab8d`.
Five final boot manifests/serial hashes, 128 source files, 24 host executable
hashes, runtime inventory and ten WOSDBG calls validate. Evidence:
`build/goal20-stage12c/validation.json` and `docs/goal20_dma_ownership.md`.
All task-owned VMs exited; the unrelated host VM remains untouched.

Goal remains active. Next: virtio ACCESS_PLATFORM and translated workloads;
then requester groups/aliases/RMRR and strict admission. Shared-BAR peer/ring
workloads, live multi-TRB USB and necessary physical-hardware qualification
also remain. The exact original goal string and appended --no-setup live
run/debug authorization remain unchanged.

### Stage 12d: virtio ACCESS_PLATFORM and translated DMA

Implemented modern VERSION_1/ACCESS_PLATFORM negotiation and a central atomic
platform-admission gate. Translated virtio cannot enable bus mastering until
FEATURES_OK succeeds; reset revokes that qualification. Queue DMA preparation
remains possible before BME admission. Legacy and missing-feature probes are
refused in translated mode; passthrough stays explicit. Unit quarantine still
overrides transport qualification, and quiesce/IRQ drain plus invalidation
remain prerequisites for releasing backing.

Targeted rootless results: modern probe/control/packet/retirement checks pass
44 assertions; legacy and missing-platform refusal each pass 35 assertions
with BME off and no mappings. Translated four-CPU traffic verifies 235 exact
64/9,014-byte frames and releases 514 regions/2,056 pages. Passthrough traffic
verifies 129 frames with the same release proof. Real foreign and stale-page
reads both fault through VT-d with exact SID/DID/address/read reason 6;
unit admission closes, BME is inhibited and uncertain backing stays pinned.
The separate withheld-invalidation test also passes without fabricating a
hardware fault. Negative fault VMs were intentionally QMP-stopped; continued
filesystem availability on their shared failed AHCI unit is not claimed.

Normal/isolated builds and formatting pass; all 37 selected host targets pass
226 GTest cases with rebuilt executable hashes. A failed initial probe caused
by an echo-port mismatch is retained, then corrected and rerun with the same
ELF. Review also removed an invalid raw PCI write from the new domain KTEST
before the full suite: raw unauthorized writes deliberately panic; the typed
refusal API and real command-bit readback remain.

Targeted qualification ELF:
`6d25d8fc6f55db241cf0c857f7354849294e3899f8c2c3d759266f14ab1c76bf`.
Final full-suite ELF:
`fd54e3cc4f64c7b911b2fd048a9111e39d08e4de632437d02bf3005a505179b2`.
The sole source difference is `iommu_domains_ktest.cpp`; production sources
are identical and both builds have separate archives/manifests. Full-suite
results and final evidence verification follow after completion.

12d full-suite result: **66,185 assertions, zero failures**, all 1,172 enabled
tests pass and three disabled tests skip; normal power-off. The central
pre-negotiation admission/PTE tests and final virtio packet/control/retirement
tests pass. Nine boot manifests/serial hashes, both 130-file source archives,
25 host executable hashes, full runtime inventory and 18 WOSDBG calls validate
in `build/goal20-stage12d/validation.json`. All task-owned VMs exited; the
unrelated VM was preserved. No sanitizer or userspace fault report appeared
in the final qualifying logs.

Goal remains active: requester groups/aliases/RMRR and strict admission are
next, followed by the remaining shared-BAR peer/ring, multi-TRB USB and required
hardware qualification. The original goal string and appended --no-setup
run/debug authorization are unchanged.

### Stage 13a: bounded PCI configuration and isolation inventory

Added a bounded MCFG/ECAM parser and read-only capability walker, plus immutable
per-device boot snapshots. Function identity must match legacy PCI enumeration;
invalid windows, RAM overlap, malformed/cyclic capability chains and ambiguous
records fail closed. New PCI integration, host tests/fuzzer and KTEST capture
PCIe, ACS, ATS/PRI/PASID/SR-IOV and device-control evidence without modifying
device controls. Bridge/RMRR and strict-policy refusals remain unchanged.

Normal Build WOS, isolated diagnostic build and formatting pass. All 39
selected rebuilt host targets pass (235 GTest cases); a separate 50,000-input
fuzz run passes. Rootless flat, root-port, PCIe-to-PCI bridge and no-MCFG guests
verify bounded inventory and independent QMP identity/topology. The root port
reports ACS capability `0x5f` with control `0x0`, so its mere presence does not
establish isolation. Those four inventory guests were intentionally QMP-stopped.

Full translated KTEST passes **66,214 assertions, zero failures**, all 1,173
enabled tests and normal power-off. It includes the new retained-snapshot test,
300 verified Ethernet echoes and terminal DMA retirement. No sanitizer or
userspace fault report appears in the five qualifying logs. Exact ELF:
`db21b3fa6dac7bd8a97fbb658409209678b7da1b56fe3243a20b16e168209994`.
Five boot/serial manifests, 136 archived/current source files, 27 host binaries,
the runtime manifest and ten WOSDBG calls validate in
`build/goal20-stage13a/validation.json`. All private VMs exited; the unrelated
VM was preserved.

Goal remains active. Next: actual requester groups/aliases with shared roots
and unambiguous fault/allocation attribution, then RMRR ownership and strict
admission. Shared-BAR peer/ring traffic, live multi-TRB USB payloads and required
hardware qualification remain. The exact original goal string and appended
--no-setup live run/debug authorization remain unchanged.

### Stage 13b: requester groups and alias contexts

Implemented canonical group roots/DIDs and identical original/bridge-alias
contexts. Nonisolating branches share explicit hardware visibility; allocation
handles remain per-device. Cross-unit ambiguity and unsupported enabled
requester modes refuse admission. Fault capture resolves the canonical root;
deferred diagnostics distinguish allocation ownership from alias ambiguity.
Quiesce, IRQ drain, invalidation and quarantine rules remain unchanged. ACS
controls are unchanged, and strict/RMRR admission still refuses unqualified use.

Both builds, formatting and 40 selected host targets pass (244 GTest cases).
Root-port live traffic/retirement passes 392 exact frames. A real bridged stale
DMA read faults at SID `0x200`, DID 3 and generation 1; diagnostics identify
allocation owner 4 with `source_alias=1`. Admission closes, BME is inhibited
and uncertain backing stays pinned. The negative test also stops shared AHCI
DMA and is followed by a journald page fault; no availability claim is made.

The full bridged guest passed its expanded group/PTE tests. QMP independently
confirms identical contexts for original and absent alias SIDs. Remaining
bridged tests and the queued flat full suite are running; final counts and
evidence audit remain pending. Qualification ELF:
`47989be3d5cc6870234cc145caa3cb248f138c1b99d32eff21c857c1a15bfbdb`.
Evidence and the prepared final validator are in `build/goal20-stage13b/`.
Goal remains active with its original string and run/debug authorization intact.

13b final result: bridged and flat full KTEST suites pass **67,957** and
**67,339 assertions**, zero failures; each passes all 1,173 enabled tests and
powers off normally. The flat/root-port e1000e fixtures release NIC mappings.
The bridged legacy e1000 fixture instead preserves 514 quarantined regions
because its stop proof is unsupported; its passing terminal check does not
claim release. Both full guests verify 300 exact Ethernet echoes.

Four boot/log manifests, independent live alias-context reads, 138 source-file
hashes, 28 host binary hashes, both runtime inventories and eight WOSDBG calls
validate in `build/goal20-stage13b/validation.json`. Every private VM exited;
the unrelated VM was preserved. Goal remains active: strict/ACS qualification,
RMRR identity mapping, remaining ivshmem/USB workloads and physical evidence
are unfinished. A private allocation-free reserved-memory ownership prototype
passes eight ASan/UBSan host cases, but production RMRR integration is pending.

### Stage 13c: reserved firmware DMA mappings and preparation rollback (2026-09-23)

Added bounded reserved-memory coverage/MMIO exclusions, normalized group-scoped
identity RW mappings before root exposure, retained firmware accounting and
collision rejection in driver mapping APIs. Firmware backing is never DMA
allocator-owned or freed. NVS and allocator/kernel/ACPI ownership remain
protected. A live context-allocation failure exposed a cleanup defect in
nonempty unpublished roots; an explicit preparation-discard operation now frees
only never-exposed table storage while preserving ordinary destruction and
hardware-visible lifetime restrictions.

Normal Build WOS, isolated diagnostic build, formatting and 41 host targets
pass (259 GTest cases in 29 rebuilt binaries). Seven corrected private rootless
VM fixtures verify valid/overlapping RMRRs, allocator/MMIO/NVS refusal, complete
allocation unwind and the full translated suite. Independent hardware walks
confirm scoped RW leaves and unmapped neighbors/other groups; negative cases
keep TE and all PCI BME bits clear. Debugger accounting proves 12 -> 0 prepared
table pages, with the reserved payload unchanged.

The complete suite passes **67,924 assertions, zero failures**, covering all
**1,175 enabled tests** plus three disabled skips, 300 exact NIC echoes and clean
AHCI/e1000e/xHCI driver-DMA retirement. It powers off normally. Kernel fingerprint,
seven manifests/logs, 142 source hashes, 29 host binary hashes, runtime inventory
and 14 WOSDBG calls validate in `build/goal20-stage13c/validation.json`.
Corrected ELF:
`1d8dd4dbf7ad7dde4bde7a5bbee1dc3f12cc658261df85ca0ef36480dda37d08`.
The earlier failed cleanup fixture is retained separately; it is not counted as
successful unwind evidence. Every private VM exited; the unrelated VM remains.

Goal remains active. Strict/ACS admission, remaining ivshmem/USB workloads and
physical qualification are unfinished. A private shared-ring model passes nine
optimized and ASan/UBSan host cases (leak detection disabled under sandbox
ptrace), but is not yet production integration or live peer-traffic evidence.
Details, limitations and the next-workload model are recorded in the ledger.
The original goal string and appended live run/debug authorization are intact.

### Stage 14: bounded shared rings and live peer qualification (2026-09-23)

Net and WKI ivshmem now use retained, validated BAR geometry and shared
acquire/release indices. Complete records are copied without truncation;
malformed indices/lengths fail closed without advancing tail. WKI producers
have nonblocking admission, raw-RDMA arithmetic is bounded, and an atomic
header claim publishes magic last without resetting an existing valid peer.
Packet, callback, IRQ and retained-BAR lifetime rules remain unchanged.

Normal Build WOS, isolated diagnostic build, formatting, 42 host targets
(272 GTest cases), four plain/MSI-X terminal fixtures and the complete suite
pass on ELF `5e78762af016034724d738408285a381f66679b18539e235142adc21cde960c4`.
The full suite reports 68,212 assertions, zero failures, all 1,176 enabled tests
and three disabled skips, 300 exact NIC echoes and clean driver-DMA retirement.
Its source/artifact/runtime audit is `build/goal20-stage14/validation.json`.

Actual paired authenticated WKI testing found a packed remote-OPEN Stat
alignment panic after VFS request timeouts. That run is preserved as a failure,
not a payload pass. A bounded aligned-copy fix and regression are now in
remote_vfs.cpp; both builds and formatting pass. The diagnostic reply-admission
regression passes on new ELF
`b23981aaeda70be0a4017e748b48ea05463993862046fc6e918ebb2662de4031`; the full
suite is still running at this checkpoint. A separate paired attempt exposed
role 1's bounded header wait expiring before role 0 initialized. That refusal
is also retained; traffic qualification and bootstrap follow-up remain open.

Goal remains active. Peer payload/IRQ qualification, strict/ACS enforcement,
remaining USB multi-TRB and physical evidence are unfinished. No indispensable
external blocker has been established. The original goal string and rootless
live run/debug authorization remain unchanged. See the implementation ledger
for patch contracts, failed fixture receipts and the next steps.

Stage 14b checkpoint update: the corrected alignment regression and complete
suite now pass (68,212 assertions, zero failures; 1,176 enabled tests and three
skips), with normal poweroff. The 149-source/ELF/runtime audit passes in
`build/goal20-stage14b/validation.json`. Actual peer payload qualification and
the separately observed bootstrap timing issue remain open; the valid-header
peer fixture is running. This does not complete Goal 20.

Stage 14b peer checkpoint: node 0 completed all three 512-KiB remote write/read/
compare rounds without the prior alignment panic. Node 1 timed out on remote
STAT_AS, so the pair failed and is not qualified. The live snapshots taken after
traffic resumed did not identify the earlier stall cause. Both guest logs,
partial results and debugger evidence are preserved; all task-owned VMs exited.
The remaining peer variants are prepared, not executed. Bootstrap and live
receive/timer/waiter investigation continue next; Goal 20 remains active.

### Stages 14c–14d: bootstrap ordering and diagnostic resume (2026-09-23)

Either configured ivshmem role can now initialize an empty header exactly once;
automatic attach selects the opposite published initializer role. Existing ring
contents, header offsets, PCI admission and callback lifetime rules are preserved.
Host races and live delayed explicit/automatic peer boots pass. Mixed-version
legacy automatic attachment is not qualified.

Live stall analysis also identified expensive aligned KASan shadow clearing in
the kernel-resume tail. The aligned path now uses the existing excluded memset,
with adjacent-shadow and partial-granule behavior covered by KTEST. Both builds
and formatting pass. Final diagnostic ELF
`fc0fcec6ac0a505935df87f58c64f286a35d32ff8b6a325e09ebbdc0aa105f62` passes
68,223 assertions, zero failures, all 1,177 enabled tests, three skips and 300
exact NIC echoes, then powers off normally. The 154-source/boot/ELF/KCOV/runtime
receipt is `build/goal20-stage14d/validation.json`.

Paired payload results remain mixed: both generic-net modes passed on 14c, while
14d repeats still exposed intermittent VFS waits; direct WKI remains unqualified.
The aligned shadow optimization does not resolve those protocol timeouts.
Ordinary paired shutdown also remains unqualified. Live GDB at ahci_shutdown
confirmed RFLAGS=0x86 (IF clear), entered through the userspace power syscall;
converted driver retirement correctly refuses that unsafe waiting context.
The power-finalizer context needs repair without weakening driver safety gates.

Goal active. Next work: power-finalizer wait context, normal optimized paired
workload comparison and bounded WKI receive progress, then strict/ACS enforcement,
remaining USB multi-TRB and physical evidence. No indispensable external blocker
has been established. Original goal text and rootless live-debug authorization
remain unchanged; detailed receipts and limitations are in the implementation
ledger.

### Stage 14e: real poweroff retirement and optimized peer comparison

Fixed the ordinary power-finalizer wait context: unsafe scheduler entry refuses
before claiming shutdown, and the authorized non-returning finalizer enables
completion IRQs. Driver quiesce, IRQ drain, uncertain-backing retention, authority
and VFS-sync-before-storage-retirement rules are unchanged.

Normal RelWithDebInfo and isolated Debug builds, formatting and the shutdown
source guard pass. Six actual userspace forced/init-managed poweroff fixtures
pass; live GDB verifies IF set at AHCI retirement. AHCI, active virtio and e1000e
release all tracked DMA mappings/pages, and applicable ivshmem callbacks/IRQs
drain. The complete diagnostic suite passes 68,223 assertions, zero failures,
1,177 enabled cases and three skips, 300 exact NIC echoes and normal poweroff.
Frozen Debug ELF: 6e424faaddcbb15dda6845b9841fda5f724038a53e55fbef5cca5395a84429be.
Normal ELF: 87660591e6bf671ece7afa8224439bf2ac0893cef428eb67ccac9d64b1a83ad7.
The 154-source/boot/ELF/KCOV/runtime audit is build/goal20-stage14e/validation.json.

Optimized delayed-start peer workloads now pass three complete 512-KiB write/
read/compare rounds per node and clean device retirement/poweroff for direct
plain, generic-net plain and generic-net MSI-X. Direct MSI-X still fails after
one round per node with a remote write timeout. Immediate-start workloads also
retain failures; one generic-net MSI-X payload pass had incomplete asynchronous
poweroff, explicitly recorded as an external stop rather than clean retirement.
These results qualify the passing steady workloads, not general WKI reliability.
Forty-six WOSDBG calls and the final artifact/source audit pass. All task-owned
VMs exited; unrelated QEMU PID 36201 was preserved.

Goal remains active. Continue exact-channel reply/admission and direct receive
progress diagnosis, then strict/ACS enforcement, remaining actual USB multi-TRB
qualification and indispensable physical evidence where needed. No external
blocker has been established. The original goal string and appended rootless
live run/debug authorization remain unchanged.

### Stages 14f–14h: completed replies and actual chained USB payloads

Live capture proved an acknowledged VFS STAT request lost its reply before
reliable enqueue (NO_ROUTE, with exact matching binding/channel generation).
A separate diagnostic observed transient peer-lifecycle admission refusal.
Common completed metadata/write replies now use the existing bounded retry
mechanism: immutable bytes, no filesystem re-execution, no retry after WKI_OK,
and retirement/deadline/generation checks. Deferred device workers retain
shutdown ownership through binding and operation-pool release.

The final guarded normal MSI-X pair passes three 512-KiB write/read/compare rounds
per node and clean device retirement/poweroff. Both builds, formatting, source
guards and five shutdown-ownership host cases pass. Its complete Debug suite
passes 68,223 assertions, zero failures, 1,177 enabled cases, three skips and
300 exact NIC echoes, then powers off normally. Exact frozen source/ELF/boot/
KCOV receipts: build/goal20-stage14g-owned/validation.json.

A later test-only USB split selection and optional bounded planner cap qualify
actual chained payloads within QEMU's small USB-network buffer limit. Both builds,
formatting, xHCI source checks and 19 host cases pass. All 64 live byte-exact
65/1514/1801/2015-byte echoes cross the 64-byte first TRB in both directions.
xHCI releases 10 regions/13 pages to zero; AHCI/e1000e also retire to zero and
poweroff is normal. Current source/ELF/boot/live receipts:
build/goal20-stage14h/validation.json. The complete suite above belongs to the
preceding guarded ELF; only three USB files changed afterward.

Goal active. Next: strict/ACS admission and qualification. Strict still refuses;
intentional small-payload splitting does not claim natural 64-KiB physical
crossings, full large USB transfers, or hardware evidence. Optional WKI feature
query timeouts persist; separate OPEN/xattr/batch reply paths were not broadened.
All task-owned VMs exited; unrelated QEMU PID 36201 remains. No indispensable
external blocker has been established. Original goal text and the appended
rootless live run/debug authorization are unchanged.

### Stage 15: strict independent-domain admission

Strict mode now requires translated, independent requester domains and qualified
source boundaries, preserving explicit refusal before exposure and all existing
unsupported-mode checks. Both builds, formatting and 28 host cases pass. The
strict full suite passes 68,250 assertions, zero failures and 300 exact NIC echoes,
then powers off normally. Strict virtio probe/retirement passes 44 assertions;
64 chained USB echoes and the optimized two-node MSI-X workload also pass with
normal poweroff and healthy DMA retirement to zero. Each WKI node completes
three 512-KiB write/read/compare rounds.

Live stale and foreign DMA produce real VT-d faults; withheld invalidation
produces quarantine without inventing a hardware fault. All three close admission,
inhibit bus mastering and retain uncertain backing; each was intentionally stopped.
Absent-IOMMU, conflicting-options and disabled-ACS boots refuse before translation;
live PCI-command/GSTS reads confirm BME/TE remain off. Eighteen WOSDBG calls and
the frozen-source/artifact audit pass: build/goal20-stage15/validation.json.

Stage 16 adds guarded boot-time ACS activation and fresh readback; its builds and
13 host cases pass, with live boundary qualification still in progress. The
normal compatibility policy remains explicitly logged passthrough; strict is
opt-in. See docs/dma_ownership.md for the current ownership and operating contract.
Goal remains active pending this final qualification and evidence review.


### Stage 16: guarded ACS activation and completion

Implemented one-shot BSP ACS activation with all bus masters disabled, fresh
identity/capability validation, bounded 16-bit writes, preserved unrelated bits,
exact readback, and immutable original discovery snapshots. Domain publication
and DMA lifetime/IRQ rules are unchanged. Unsupported boundaries still refuse.

Normal Build WOS (RelWithDebInfo), isolated Debug build, formatting and 13 ACS/
ECAM host cases pass. The final external-root-port strict full suite passes
68,578 assertions with zero failures, 1,177 enabled cases and three skips,
300 exact NIC echoes, matched boot/ELF/KCOV identity, zero healthy driver mappings
at retirement and normal poweroff. Independent QMP reads verify ACS capability
0x5f/control0x1f, bridge BME enabled and VT-d translation active. An ACS-less
root port and a conventional alias bridge refuse with all9/all10 BME bits and
VT-d TE clear; the alias refusal follows successful upstream ACS activation.
Those negative fixtures were intentionally stopped. Six WOSDBG calls and the
157-file frozen-source/artifact audit pass: build/goal20-stage16/validation.json.
Final normal ELF: 7a5b121d9967704a55fe2dce6e6478a3be386631286cc1851714b1969e551cb5.
Final Debug ELF: 057a4c1c7c9b421aa070c59c804a28256bf6f57be94bda8cca64e184b3e08f0e.

The implementation/completion contract is satisfied for the documented supported
Intel VT-d subset. Stage15 supplies strict virtio/USB/WKI and deliberate real DMA
fault evidence; stage16 changes only boot-time ACS activation and its host tests.
Healthy retirement releases mappings; uncertain quiescence/invalidation remains
quarantined deliberately. Default compatibility mode remains explicitly logged
passthrough; per-device enforcement requires --iommu-strict. Unsupported topology
fails rather than silently claiming isolation. No syscall or WKI wire ABI changed.

Physical chipset/firmware validation, full large USB transfers and natural
physical64-KiB USB crossings remain unqualified; boundary planning and actual
small chained transfers are covered. These are deployment qualification limits,
not evidence of an indispensable blocker in the implemented supported subset.
No interrupt-remapping/MSI-injection or raw WKI-peer security claim is made.
Optional WKI feature-query timeouts and the known cached WKI payload-limit
mismatch are recorded in the evidence ledger, without a general WKI reliability
claim. All goal-owned VMs exited; unrelated QEMU PID36201 was preserved.
Original goal text and appended --no-setup live run/debug authorization are intact.

User confirmation after completion: only QEMU is available for now. Physical
Intel VT-d/ACS and device qualification remain explicitly unverified.
