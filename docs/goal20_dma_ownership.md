# Goal 20: DMA ownership implementation ledger

## Scope and source reconnaissance (2026-09-17)

Goal 20 is active. The original goal command in
`goals/20-iommu-dma-ownership.md` was loaded without changing its text. The user
additionally authorizes live WOS execution/debugging with `--no-setup` on
`bin/wos-ktest` and `bin/wos-cluster`; this overrides older user-run-only
guidance. That option skips privileged topology setup, and still requires the
configured TAP/bridge topology to exist.

Source map and flow:

- ACPI: `platform/acpi/acpi.{hpp,cpp}` locates checksummed tables via the
  Limine RSDP; `tables/sdt.hpp` describes the 36-byte header. Its existing
  lookup trusts firmware lengths before checksumming. A bounded DMAR decoder
  alone does not make that earlier lookup safe against arbitrary firmware.
- Boot: `platform/init/init_registry.hpp` enumerates PCI in phase 3 before
  AHCI and filesystems. Network and USB probes run in phase 5. Domain setup
  must precede AHCI, not merely precede network registration.
- PCI: `dev/pci.{hpp,cpp}` retains discovered devices in a static array,
  uses legacy segment-zero configuration I/O, and enables bus mastering
  directly. Configuration address/data access currently lacks serialization;
  16-bit writes read/modify/write a dword, including status bits. Both require
  care before concurrent domain admission or bridge topology resolution.
- AHCI: `dev/ahci.cpp` allocates CLB/FIS/command tables and translates caller
  buffers to PRDT addresses under a per-port mutex. Failure/timeout handling
  must establish engine quiescence before any mapping/frame retirement.
- Virtio: `dev/virtio/virtio.cpp` allocates contiguous queues;
  `virtio_net.cpp` has separate legacy/modern probes, queues, and DMA buffers.
- e1000e: `dev/e1000e/e1000e.cpp` translates packet data on TX under an
  IRQ-saving lock; close is currently empty. Mapping cannot add blocking or
  allocator dependencies to this lock-held path.
- xHCI: `dev/usb/xhci.cpp`, `xhci_lifecycle.hpp`, and `xhci_request.hpp`
  separate controller, slot, request, IRQ, and worker lifetimes. Existing
  retirement/quarantine must precede DMA invalidation and frame release.
- ivshmem: `dev/ivshmem/ivshmem_net.cpp` copies through shared BAR memory;
  bus mastering is enabled before BAR mapping. Shared-memory exposure and WKI
  fencing are distinct from device-originated DMA translation.
- Network public contracts: `net/netdevice.hpp` retains generation-qualified
  registrations; `net/netpoll.hpp` separates bounded IRQ scheduling from
  polling and task-context disable. Domain storage must outlive all admitted
  completions.

No syscall, wire-format, or userspace ABI change is planned. New mapping
handles must carry device/domain ownership and generation; identity fallback
must explicitly report no isolation. Partial-page streaming buffers require
exclusive backing or bounce buffers, not silent adjacent-page exposure.

## Stage 1 patch contract

Problem: No bounded DMAR decoder or independently testable DMA ownership
foundation exists in the current tree.

Expected files to change: new `dev/iommu/dmar.hpp`, host unit/fuzz tests,
kernel selftests, host CMake registration, this ledger, and local goal status.

Files already read: source entrypoints listed above, kernel CMake, host test
CMake/helpers and representative tests, KTEST registration, build task,
formatter, runtime launcher option handling, and kernel diagnostic flag map.

Invariants to preserve: no hardware behavior changes; fail closed on malformed
input; bounded work; no unaligned typed firmware loads; no exposed partial
parse after failure; borrowed table bytes remain immutable and live.

Locks involved: none in the decoder; caller owns immutable input storage.

Unsafe contexts: no new interrupt, NAPI, allocator, or boot call site.

Allocation/blocking assumptions: decoder uses caller-owned spans, no heap,
logging, waits, or callbacks; fixed table/record/scope limits bound work.

ABI/wire/syscall compatibility impact: none.

Build/test plan: malformed/truncated/checksum/range/scope tests using the real
decoder under host ASan/UBSan and libFuzzer; KTEST tests of the same decoder;
repository formatting; Normal Build WOS; isolated diagnostic build and live
KTEST with `--no-setup` when prepared artifacts are available.

Rollback plan: remove decoder and registrations; no hardware state is changed
in this stage.

### Stage 1 extension: VT-d encoding and invalidation ordering

Before hardware integration, add `dev/iommu/vtd_model.hpp` and matching host
and KTEST cases. Intel VT-d revision 4.0 sections 9.1, 9.3, 9.8, 11.4.6.1,
and 11.4.6.3 were read from the vendor specification. The model constructs
only legacy root/context entries and 4 KiB second-stage entries, and tracks
global context-then-IOTLB invalidation with exact software operation tickets.
Reserved bits stay clear; absent, narrow, misaligned and unsupported encodings
fail. Hardware busy or error responses never authorize reuse. Timeouts
quarantine the state rather than making frames reusable. This stage still
has no hardware caller or lock acquisition. A future unit lock must serialize
the model with MMIO writes and observations; it does not establish device
quiescence, IRQ drain, cache synchronization, or hardware capability by itself.

### Rootless qualification fixture repair

Live preparation found that `cluster_config_from_node_spec()` discarded the
already validated `qemu_netdev` field. Consequently a rootless KTEST socket NIC
became a TAP NIC. A minimal propagation change in `scripts/cluster/node_setup.py`
preserves that field by deep copy. The regression checks the complete
node-to-zone-to-node round trip and ensures topology validation does not probe
host links. Default TAP configuration and isolated image paths are unchanged.
Read the scripts/cluster AGENTS notes and both conversion callers before editing.

## Qualification still required

Implement and test scope resolution/grouping, domain/page-table/invalidation
ownership, ACPI boot integration and fallback policy, DMA allocation/mapping,
PCI admission, every in-scope driver, quiescence and fault accounting. Then
qualify QEMU IOMMU-on/off workloads and real hardware where emulation cannot
establish the contract. Model tests alone do not establish DMA isolation.

Reference: Intel's DMAR layout definitions in
[EDK II](https://github.com/tianocore/edk2/blob/master/MdePkg/Include/IndustryStandard/DmaRemappingReportingTable.h)
were consulted for record offsets, scopes, address width, and reserved ranges.
The complete [Intel VT-d revision 4.0 specification](https://cdrdv2-public.intel.com/671081/vt-directed-io-spec.pdf)
was subsequently downloaded and checked for the encoding and ordering rules.

## Stage 1 evidence and iteration notes

- `source .vscode/wos-env.sh && scripts/dev/build_wos.sh`: normal
  `RelWithDebInfo` build passed outside the sandbox. The first attempt failed
  at libguestfs `bind: Operation not permitted`, not compilation; both logs
  are retained.
- `bin/wos-ktest --build-only`: isolated diagnostic `Debug` build passed,
  including both new test translation units and the existing diagnostic flags.
- Host ASan/UBSan: 12 DMAR cases and six VT-d encoding/invalidation cases
  passed; `dmar_fuzz_smoke` passed. The final seeded fuzz run completed
  5,016,084 iterations in 31 seconds without a sanitizer failure. An earlier
  9,598,773-iteration run preceded the register-bank alignment and scope-order
  refinements and is not counted as final-source qualification.
- Formatting and clang-tidy completed. The kernel-test compilation database
  was used with `WOS_TIDY_CACHE=0` because the normal build excludes tests
  and the default external tidy cache was read-only. Advisory optional-access
  warnings occur behind `KREQUIRE_TRUE(ENTRY.has_value())`.
- All 30 cluster-setup cases passed, including the rootless KTEST round-trip
  regression; Python byte-compilation and KTEST help succeeded.
- Existing host/ktest manifest gates fail on unchanged files: the former
  expects GTest in the standalone `kcov_runtime_test.cpp`; the latter does
  not recognize assertions inside eight credential helper-based tests. The
  same source constructs were checked in `HEAD`. These are not decoder
  failures and were left outside this patch's ownership.
- Initial live run: rootless `wos-cluster --launch --no-setup`, using isolated
  KTEST images, passed `Dmar/BoundedFirmwareDecode` at guest time 247.284 s.
  That run was explicitly stopped through its dedicated QMP socket to load
  the final image containing the VT-d tests. It is not a full-suite pass.
- Final live run: `bin/wos-ktest --config ktest-data/goal20/node.json --no-build
  --no-setup --live-debug-descriptor ktest-data/goal20/live-debug.json` uses
  one socket-backed NIC, default 32 GiB/32 CPUs, xHCI, and the isolated disks.
  **60,701 assertions passed, 0 failed**. Both new suites passed. The VM
  reported selftest poweroff and the launcher exited zero. The emitted kernel
  text fingerprint matched the saved diagnostic ELF with strict matching.
- WOSDBG loaded the serial evidence and found both new model-test passes.
  Build isolation invariants passed, and final source/ELF hashes matched the
  recorded manifest. `validation.json` and `serial-final.log` retain the totals
  and serial SHA-256. This completes stage 1 qualification, not Goal 20.

Artifacts are retained in `build/goal20-stage1/`, including build/fuzz logs,
source hashes, and the final diagnostic ELF. Runtime configuration, overlays,
QMP socket, and serial output are under `ktest-data/goal20/`. No production
DMA enforcement is claimed by this stage.

## Next integration constraints established from source

- `platform/interrupt/gates.{hpp,cpp}` already provides task-context
  `free_irq()` admission closure and reader draining outside its registry
  lock. The device must mask its interrupt source first. Reuse this contract.
- `platform/init/init_wrappers.cpp` runs KTEST in `sched_init()` before phase-5
  NIC/xHCI initialization, and currently requires exact `--selftest` command
  line equality. Therefore this selftest run does not qualify those drivers
  or a future combined IOMMU/selftest command line.
- `dev/e1000e/e1000e.cpp` uses packet storage directly for RX as well as TX.
  `net/packet.cpp` page-rounds each packet allocation, but a DMA page may also
  contain packet metadata. Page rounding alone does not establish exclusive
  buffer ownership; inspect offsets before permitting direct streaming maps.
- `platform/mm/phys.opt.cpp` reserves firmware map ranges during allocator
  initialization. Bounded ACPI access needs retained firmware-range evidence;
  simply passing an untrusted SDT length into a span is insufficient.

Next finite stage: bounded PCI scope resolution and explicit grouping policy,
followed by safely bounded ACPI discovery and boot-time unit capability
qualification. Prepare a new patch contract before changing boot, config-I/O,
or MMIO behavior. DMA API, driver conversion, strict enforcement, fault
injection and hardware qualification all remain pending.

## Stage 2 patch contract: bounded boot discovery

Problem: The tested decoder has no production caller; existing ACPI lookup
checksums an unbounded firmware length. Hardware capabilities and absence of
DMA isolation are not reported before drivers start.

Expected files to change: new bounded ACPI reader/model, boot memory-map
accessor in `platform/mm/mm.{hpp,opt.cpp}`, ACPI wrapper, IOMMU discovery and
capability code, runtime/metadata init registries, QEMU IOMMU fixture option in
`scripts/cluster/node_setup.py`, host/kernel tests and this ledger.

Files already read: ACPI/RSDP/SDT sources, `mm.opt.cpp`, `phys.opt.cpp`,
`virt.opt.cpp` direct-map setup, `addr.opt.cpp`, PCI enumeration, init wrappers,
both registries/executor/validation, DMAR/VT-d models, launcher and cluster
tests. Intel VT-d rev. 4.0 capability-register and DMAR definitions were checked;
QEMU's public Intel-IOMMU register definitions confirm the emulated offsets.

Invariants to preserve: no PCI bus-master or IOMMU command-register writes in
this stage; no claimed isolation; bounded reads within mapped firmware-owned
RAM; reject conflicting/duplicate tables, unsupported bounds and RAM-backed
MMIO claims; no partially published inventory after structural failure.

Locks involved: none held on discovery entry. Existing kernel page-table
mapping locks may be taken to map the first MMIO page. Discovery is BSP boot
work before AHCI initialization and scheduler startup, never IRQ/NAPI work.

Allocation/blocking assumptions: ACPI parser allocates nothing; inventory and
retained DMAR bytes use fixed storage. MMIO page-table installation can use
the existing boot allocator. No hardware completion waits or device callbacks.

ABI/wire/syscall compatibility impact: none. The new `vm.iommu` launch option
is opt-in; normal machine/network configuration remains unchanged.

Build/test plan: ASan/UBSan bounded-reader and capability negative tests, kernel
tests, launcher argument/validation tests, normal and isolated builds, rootless
QEMU Intel-IOMMU on/off boots with explicit discovery/fallback serial evidence.

Rollback plan: remove the new boot module and opt-in fixture option. No device
or IOMMU command state has been changed by discovery. Scope resolution and
domain installation follow after actual firmware/capability evidence exists.

### Stage 2 implementation (2026-09-22)

Production discovery now runs after PCI enumeration and before AHCI. The new
bounded ACPI reader independently checks firmware memory-map ownership and each
CPU mapping before it reads an RSDP/root/child extent. It validates both RSDP
checksums, caps root entries and table lengths, and rejects duplicate requested
tables. The legacy ACPI lookup used by other subsystems is unchanged.

Discovery copies a validated DMAR into fixed retained storage, bounds the
inventory to 16 units, rejects MMIO claims overlapping non-reserved memory, and
reads only VER/CAP/ECAP/GSTS from the first register page. Legacy capability
qualification validates widths, domain counts, supported page levels, variable
register-bank extents and drain support. Active firmware translation/queued
invalidation/interrupt remapping is reported as a distinct state. There are no
IOMMU or PCI command writes. Logs explicitly report that DMA isolation is
unavailable while existing drivers still perform unmanaged DMA.

`vm.iommu: true` in the shared node spec adds
`intel-iommu,intremap=off,caching-mode=on,aw-bits=48` to a q35 VM. The default is
false. Invalid boolean/machine combinations fail during normalization, before
overlay preparation. Rootless socket-multicast backend propagation remains
covered by the launcher tests.

The environment refresh removed the earlier tool handles, `/tmp` host build,
and `build/goal20-stage1/` evidence directory. Historical stage-1 observations
above are retained as history, not substituted for current receipts. Fresh
stage-2 evidence is being collected under `build/goal20-stage2/` and isolated
runtime fixtures under `ktest-data/goal20-stage2/{on,off}/`.

The KTEST inventory test deliberately supports absence and unsupported firmware;
fixture serial evidence separately proves successful discovery on the enabled
VM. Final validation results follow below.

Next production dependency: resolve DMAR scope paths through a bounded PCI
snapshot; distinguish explicit unit matches, include-all defaults, RMRRs,
unsupported segments, aliases and groups. The current PCI registry has a
64-device limit with no completeness flag, bridge recursion has no visited-bus
set, and CF8/CFC read/modify/write transactions are not serialized. These must
be addressed before domain admission relies on enumeration or concurrent
bus-master transitions. Legacy 16-bit config writes currently rewrite the
adjacent status word, which is unsuitable for an admission gate. No driver
conversion or translation-enable claim is made by this stage.


### Stage 2 verified checkpoint

- Normal `Build WOS` (`RelWithDebInfo`): passed on retry. The first invocation
  failed in the native NASM build while its sysroot `stdint.h` was being
  refreshed. No NASM, libc, or toolchain source changes were made for this goal.
- `bin/wos-ktest --build-only`: passed, isolated `Debug` diagnostics. Fixed the
  new test's unsupported `KEXPECT_LE` macro to use the actual KTEST API.
- 27 host cases (DMAR, VT-d table/invalidation, bounded ACPI, capabilities),
  ASan/UBSan, and DMAR fuzz smoke: passed. Fresh host build is in
  `/tmp/wos-goal20-tests`, using the repository-pinned GoogleTest source.
- 31 launcher tests, Python compilation, build-isolation check, formatting, and
  focused clang-tidy: passed. Clang-tidy still reports advisory include-cleaner
  warnings; the command completed successfully.
- Rootless Intel-IOMMU-on KTEST: **60,715 assertions passed, 0 failed**, normal
  poweroff at 484.518 seconds. Both discovery tests, DMAR and VT-d model passed.
  Discovery reported one unit at `0xfed90000`, VER `0x10`, CAP
  `0x80d2008c222f0686`, ECAP `0xf42`, GSTS zero, legacy backend available,
  four-level tables, 48-bit IOVA and 65,536 domains. ECAP.C is clear, so future
  table publication must explicitly maintain caches; discovery does not imply
  that such publication has been implemented.
- Rootless Intel-IOMMU-off boot: reported DMAR absent, zero units, and unavailable
  isolation; AHCI initialized and the selftest suite started. This VM was
  intentionally stopped through its private QMP socket after the boot check.
  **This is not a second full selftest pass.** `--no-build --no-package` reused
  the same boot image, whose SHA-256 matched before and after the off run.
- Full on-run KCOV fingerprint matched the retained ELF with mismatch rejection:
  text start `0xffffffff80000158`, end `0xffffffff817b0296`, FNV64
  `0x4092f827648126e1`. The shortened off run has no KCOV fingerprint.
- WOSDBG shared CLI backend loaded both retained logs and confirmed discovery,
  relevant test passes and the full on-run total. Both task-owned VM launcher
  sessions exited successfully; no task-owned VM is left running.

Receipts: `build/goal20-stage2/` contains build/check logs, saved diagnostic ELF,
source hashes, boot-image hash, both serial logs, validation JSON and WOSDBG
batches/results. On-run serial SHA-256:
`f52518caa78c33e2a40357dac04bafa183e84d2bdaef86816ae71b8468e7ddff`.

The full goal remains active. This stage implements bounded discovery and
capability inventory only. No bus-master or IOMMU enable/disable commands were
added. Device scopes/groups, domains, managed DMA allocation/mapping/sync,
conversion of every in-scope driver, quiesce/IRQ-drain retirement, deliberate
faults, strict policy and final workload/hardware qualification remain required.
KTEST runs before phase-5 network/xHCI initialization, so its passing result
cannot qualify those driver workloads.

## Stage 3 patch contract: PCI topology and DMAR scope ownership

Problem: Domain admission cannot safely infer remapping-unit ownership from
DRHD include-all flags alone. PCI enumeration silently truncates and does not
retain bridge ancestry or reject repeated buses.

Expected files to change: `dev/pci.{hpp,cpp}`, pure `dev/iommu/scopes.hpp`,
IOMMU discovery inventory/caller, host and kernel scope tests, host CMake.
Files already read: PCI enumeration/config access and driver BME callers, DMAR
parser, boot discovery, init order, spinlock implementation and kernel/critical
path instructions. Scope semantics checked against Intel VT-d specification
(https://cdrdv2-public.intel.com/671081/vt-directed-io-spec.pdf).
Invariants to preserve: bounded enumeration; stable existing device pointers;
no PCI/IOMMU command writes added; no allocation or configuration access from
the pure resolver; no partial ownership result after ambiguity or unsupported
firmware; missing scope endpoints are distinguished from invalid paths.
Locks involved: none added. Enumeration and snapshot construction remain BSP
boot work before drivers start. Config transaction serialization remains a
prerequisite for the later concurrent bus-master gate, not part of this patch.
Unsafe contexts: enumeration/discovery must not run from IRQ, NAPI or panic.
Allocation/blocking assumptions: fixed device/visited-bus storage; pure model
uses bounded spans and fixed arrays; no waits or callbacks.
ABI/wire/syscall compatibility impact: kernel-private PCI metadata only.
Build/test plan: malformed/ambiguous scope and topology tests, host sanitizers,
normal and isolated builds, rootless QEMU inventory validation. Preserve the
stage-2 ELF and receipts; new receipts identify the new source/kernel state.
Rollback plan: remove scope qualification and extra enumeration metadata;
existing DMA hardware state is not changed by this patch.

### Stage 3 implementation and next admission dependency

PCI enumeration now retains each device's parent registry index and configured
secondary/subordinate buses. A 256-entry visited set prevents repeated-bus
recursion; device-capacity overflow and inconsistent bridge configuration mark
the snapshot incomplete. Existing pointers still reference the same fixed
registry, and no PCI command writes were added.

The pure scope qualifier requires a complete, bounded preorder snapshot,
validates ancestry/BDF uniqueness/bridge windows, resolves multi-hop firmware
paths, applies explicit scopes before the segment default, and retains a
64-bit RMRR membership mask per device. A sub-hierarchy includes its named
bridge, as specified by Intel VT-d section 8.3.1. Ambiguous ownership, invalid
paths, uncovered requesters, unsupported segments/records and capacity limits
return no partial bindings. Empty slots are counted separately. IOAPIC/HPET
scope entries are interrupt-source metadata, not PCI DMA endpoint assignments.

The result establishes DRHD ownership only. It does not establish requester
alias/ACS groups, authorize RMRR identity mappings, or permit bus mastering.
Those remain mandatory domain-admission checks. In particular, preserving an
RMRR address is not permission to expose allocator-owned memory at that address.

The next patch must account for an existing AHCI dependency: its controller
initialization does not call `pci_enable_bus_master`; it relies on firmware's
PCI command state. A blanket boot BME clear would break AHCI before its explicit
admission path is added. Review controller stop/reset and outstanding-command
retirement together with that change. CF8/CFC transactions still need an IRQ-safe
shared lock, and 8/16-bit config writes must avoid rewriting adjacent W1C status
bits before a concurrent bus-master gate uses them.

### Stage 3 verified checkpoint (2026-09-22)

- Normal `Build WOS` (`RelWithDebInfo`) and isolated diagnostic KTEST build:
  passed.
- Eight new scope cases plus the preceding 27 host cases: **35 passed** with
  ASan/UBSan; DMAR/scope fuzz smoke passed. Cases include 64-level ancestry,
  function-specific paths, multiple RMRRs, incomplete snapshots, duplicates,
  conflicting windows, malformed firmware and no-partial-result failures.
- Extended DMAR/scope fuzzer: **4,920,735 inputs in 31 seconds**, no reported
  sanitizer/invariant failure. Retained seeded/mutated corpus is in the receipt
  directory.
- Formatting and focused clang-tidy completed successfully. Advisory style and
  include-cleaner warnings remain; no claim of a warning-free audit is made.
- Rootless Intel-IOMMU-on VM: complete eight-device PCI snapshot; scope status
  zero, eight assigned devices, no RMRRs or absent scopes in this QEMU fixture.
  Full KTEST: **60,751 assertions passed, 0 failed**, followed by normal poweroff
  at 486.826 seconds. The new live ownership test passed.
- Retained serial/ELF KCOV fingerprints match with mismatch rejection. Source
  hashes still match the files used for this checkpoint. WOSDBG read-only CLI
  analysis confirms discovery, relevant test passes, and the total.

Receipts: `build/goal20-stage3/validation.json`, build/host/fuzz/format logs,
`source-sha256.json`, saved `wos-ktest.elf`, `serial.log`, WOSDBG batch/results
and `fuzz-corpus/`. Fixture: `ktest-data/goal20-stage3/node.json`. The task-owned
VM and its launcher exited. This fixture has flat PCI topology; bridge behavior
has host-model coverage, not live bridge-hardware qualification yet.

Additional next-stage source evidence: bundled Limine's
`common/protos/limine.c` calls `iommu_disable_all()` unless KEEP_IOMMU was
requested; WOS does not issue that request. The backend must still inspect
GSTS and refuse unknown active state rather than assume the bootloader version
or handoff policy. WOS starts on Limine's default 64-KiB stack; the bounded
64-device recursive enumeration has diagnostic frames of 32/32/768 bytes for
bus/slot/function in this build. Do not increase the registry/recursion limit
without revisiting boot-stack bounds (or replacing recursion).

Remaining full-goal work is unchanged: requester alias/ACS groups, domains and
fallback policy, cache-aware table publication/invalidation, managed DMA API,
bus-master admission with AHCI conversion, other driver conversions, retirement
and fault injection, accounting, strict enforcement and full workloads/hardware
qualification. No DMA isolation is claimed by the scope result.

## Stage 4 patch contract: explicit passthrough domains and VT-d activation

Problem: Ownership is known but no hardware domains exist. PCI config cycles
race across CPUs and subword writes rewrite adjacent W1C status bits. AHCI
relies on firmware BME instead of explicit admission.
Expected files to change: VT-d activation model/backend and domain registry,
entry models, PCI config access/BME helpers, AHCI init, init wrapper, physical
owner enum/descriptor, host/kernel tests and this ledger.
Files already read: stage-1/2/3 models and callers, PCI config/driver callers,
AHCI init/probe, IRQ-safe spinlock, physical allocator interfaces/descriptors,
time source, kernel command-line/selftest entry, Intel VT-d sections 6.8, 9.3,
11.4.4–6 and enhanced-command response definitions.
Invariants to preserve: default policy explicitly remains unrestricted
passthrough, never claimed isolation. No translated driver is admitted yet.
Prepare all pages before hardware exposure; CPU cache maintenance precedes
root installation, context/IOTLB invalidation and TE. Clear known-device BME
during activation, restore only after domain publication. Any failure after
hardware exposure retains frames, keeps BME clear and halts boot. Failure
before exposure frees prepared pages and reports software fallback. Existing
frame-owner numeric values stay stable; append IOMMU-table accounting owner.
Locks involved: one IRQ-save PCI config lock covers each address/data cycle and
command update; never hold it across IOMMU waits, allocations, logging or panic.
Domain activation is BSP boot-only; no concurrent mapping/invalidation API yet.
Unsafe contexts: activation is forbidden in IRQ/NAPI/panic. PCI config access
remains bounded and IRQ-safe, but is not a panic interface.
Allocation/blocking assumptions: bounded root/context allocation, fallible
physical page allocation, bounded MMIO polling with elapsed-time and iteration
limits. No reclaim or device callback while holding a spinlock.
ABI/wire/syscall compatibility impact: none; additive allocator diagnostic row.
Policy flags `--iommu-off` and `--iommu-strict` are explicit; strict must fail
while driver translation is unimplemented. Permit selftest plus policy tokens.
Build/test plan: activation ordering/timeout/granularity models, domain/page
accounting and live register checks, normal/isolated builds, rootless IOMMU-on
and absent/off boots. Preserve current VM isolation and saved stage-3 evidence.
Rollback plan: remove activation hook and PCI admission dependency together;
never roll back a live exposed table by freeing it without draining hardware.

### Stage 4 implementation and qualification limits

The backend prepares legacy root/context tables with distinct nonzero domain
IDs and passthrough entries, publishes them with CLFLUSH/mfence when required,
clears enumerated-device bus mastering, installs roots, completes global
context and IOTLB invalidation, enables translation and then publishes domain
admission. It preserves persistent GCMD state and bounds every command wait.
Firmware-active translation/interrupt/queued-invalidation state is refused;
active protected-memory or enhanced-command state is not taken over.

All table allocation precedes hardware exposure. A preparation failure frees
every unpublished table and reports software passthrough. Any failure after
root exposure disables admission and halts with all exposed pages retained.
The appended `IOMMU_TABLE` physical owner makes this storage visible without
renumbering existing owners. A selftest-only allocation budget permits a real
root-allocation/context-allocation failure and accounting check.

PCI address/data cycles now share an IRQ-saving lock. Subword writes use their
actual I/O lanes, avoiding adjacent W1C status writes. Typed and raw command
writes require a published domain before setting BME; AHCI now requests that
admission explicitly. Domains are currently immutable after BSP initialization.
Future mutable admission needs synchronization with command updates; the
present check before the config lock must not be mistaken for runtime teardown
serialization. Multi-operation BAR sizing and MSI programming still require
caller serialization.

Hardware passthrough is deferred for any bridge or downstream endpoint because
requester aliases and ACS grouping are not qualified yet. Flat PCI coverage
does not prove arbitrary hardware requester identity. Neither hardware nor
software passthrough provides isolation. `--iommu-off` selects the latter;
`--iommu-strict` halts before admission until managed driver translation exists.
Selftest detection now accepts `--selftest` alongside these separate tokens.

Next DMA API constraints confirmed against current source: the physical
allocator has no mask-constrained allocation interface; it guarantees page
alignment, not arbitrary DMA alignment relative to physical zero. Fallback
allocations must check complete address extents and fail safely when they cannot
meet a mask. AHCI serializes ordinary commands with a task mutex and limits
transfers to 512 KiB, but its engine-stop loop is unbounded. Its conversion must
add bounded stop/failure quarantine before introducing mapping retirement.
e1000e submission holds an IRQ-saving TX lock, so it needs preallocated DMA
storage or a bounded nonblocking mapping pool. xHCI already owns private bounce
buffers and explicit request retirement; retain that authority when replacing
its allocator. A mapping API must never infer completion merely from a handle
being destroyed.

Virtio's modern negotiation currently writes only `VIRTIO_F_VERSION_1` in the
upper feature word (`virtio_net.cpp`); it has no ACCESS_PLATFORM definition or
negotiation. The eventual translated virtio conversion must audit transport
feature negotiation and QEMU's DMA address-space selection together with queue
addresses. Merely observing VT-d TE while a virtio NIC is present cannot qualify
that NIC's DMA isolation. The stage-4 selftest still runs before NIC probe.

### Stage 4 verified checkpoint (2026-09-22)

- Normal `Build WOS` (`RelWithDebInfo`) and isolated diagnostic KTEST (`Debug`):
  passed with the final bridge guard. Formatting and focused clang-tidy passed
  with advisory warnings; the CPUID aggregate also produces a missing-field
  initializer warning in the diagnostic compile.
- Host ASan/UBSan: **45 cases passed**, covering the preceding models, five
  activation cases and five physical-balance cases; all eight selected CTest
  targets passed, including the DMAR/scope fuzz smoke target. Activation tests
  cover command order, every wait failure, busy/active rejection, invalid actual
  granularity, nonzero domain IDs and passthrough address-width selection.
- Final rootless Intel-IOMMU-on run: hardware passthrough for eight devices,
  two accounted table pages, TE enabled, AHCI disks/partitions available and
  live domain/accounting/fault-status assertions passed. Full KTEST:
  **60,816 assertions passed, 0 failed**, normal poweroff at 478.421 seconds.
  The KCOV fingerprint matches the retained diagnostic ELF; recorded source
  hashes match. The preliminary run before the bridge guard was intentionally
  stopped and is not counted as full-suite evidence.
- Focused rootless boots with DMAR absent and with `--iommu-off`: explicit
  software passthrough, zero table pages, AHCI and selftest entry. Both were
  intentionally stopped via their private QMP sockets; neither is a full-suite
  result.
- Focused `--iommu-strict` boot: expected halt with the precise unsupported
  managed-translation reason, before any TE enable or domain admission.
- Real PCIe root-port fixture with virtio beneath it: complete nine-device
  topology and valid scope qualification, then the explicit bridge-alias guard
  selected software passthrough with zero table pages. AHCI and selftest entry
  remained functional. The fixture uses the shared node QEMU layout and cluster
  exclusion lock, adding only the root port and NIC bus assignment. An initial
  fixture attempt lacked its explicit MAC and stopped before QEMU launch; the
  corrected fixture passed.
- `--iommu-test-fail-alloc`: the one-allocation budget allows the root table
  and fails context preparation. Software fallback reports zero tables; the
  live domain test passed at 244.652 seconds, checking zero physical-owner pages
  and objects. This run was intentionally stopped after that assertion, not
  reported as a full-suite pass.
- All policy boot manifests identify the same retained kernel. WOSDBG shared
  CLI log analysis succeeded for the full run and focused policy boots. Every
  task-owned launcher/VM exited; unrelated host VMs were left running.

Receipts: `build/goal20-stage4/validation-on.json`, per-policy
`validation-*.json`, `source-sha256.json`, saved ELF, serial/build/host/format
logs, WOSDBG batch/results, boot manifests and task-local qualification scripts.
Fixtures and their separate boot images/overlays live under
`ktest-data/goal20-stage4/`. No normal disk or sysroot was used for KTEST.

Goal 20 remains active. Next implement the common coherent/streaming DMA API
in explicit passthrough mode, with generation-qualified ownership, checked full
address extents/alignment/masks, exclusive DMA backing or bounce storage, and
separate never-published versus hardware-retired release paths. Then convert
and qualify each driver, add translated mappings/invalidation and fault
retirement, qualify requester groups/RMRRs, and enable strict enforcement.
Current table storage has kernel lifetime; runtime map/unmap, driver teardown,
translated isolation and deliberate stale-DMA fault evidence are still absent.

## Stage 5 patch contract: common DMA ownership API

Problem: Drivers still allocate untracked raw DMA backing and translate CPU
addresses themselves. Establish the common allocation/streaming and retirement
contract before converting a driver or admitting translated domains.
Expected files to change: new `dev/dma/` API, shared manager and kernel backend,
appended physical owner, host and KTEST coverage, host CMake, this ledger.
Files already read: domain registry/backend, physical allocator and accounting,
IRQ-saving spinlock, AHCI command/recovery/allocation paths, virtio queues and
feature negotiation, e1000e locked submission, xHCI bounce/retirement helpers,
kernel init/testing/build instructions and existing host/kernel tests.
Invariants to preserve: no driver behavior change in this step; passthrough is
explicit, not isolation. Every allocation has exclusive page backing; masks,
alignment, extents and arithmetic are checked. Handle and submission generations
reject stale/cross-device completion. Never-published storage can be released;
published storage requires caller-proven hardware completion and reader drain.
Coherent rings require whole-device quiescence. Unmap failure quarantines backing
rather than freeing it. No automatic destructor retirement. Streaming maps use
bounce storage and borrow only kernel CPU buffers whose lifetime callers retain.
Locks involved: one IRQ-saving registry lock, held only for bounded metadata
transitions. Allocate/free, copying, synchronization, map/unmap and diagnostics
run outside it. Busy transitions exclude racing retirement/reuse.
Unsafe contexts: allocation/map/free are boot or task work; preallocated
submission/completion must not allocate, block on hardware or log. Copy work is
bounded by the region size and remains the driver's context responsibility.
Allocation/blocking assumptions: fixed-capacity metadata; fallible, zeroed,
power-of-two physical backing; no reclaim waits. Mask-constrained fallback may
fail rather than silently truncate an address. Backend map/unmap contracts must
distinguish safely rejected work from possibly hardware-exposed quarantine.
ABI/wire/syscall compatibility impact: kernel-private API and appended physical
owner only; preserve all existing enum values and driver protocols.
Build/test plan: exercise the actual shared manager with a hostile/failing host
backend, stale/wrong-owner tickets, masked/unaligned extents, capacity failure,
copy directions/partial completions, busy races and unmap quarantine. KTEST
checks real allocator ownership and publication/retirement without issuing DMA.
Run Normal Build WOS and isolated KTEST; retain stage-4 artifacts independently.
Rollback plan: remove unused API/test/owner additions together. Once converted,
drivers may never bypass the manager to free hardware-visible storage.

### Stage 5 implementation notes

`dev/dma/dma.hpp` exposes coherent allocation, streaming bounce mapping,
generation-qualified preparation/completion, explicit release, quarantine and
accounting. The fixed-capacity manager is shared verbatim with the host tests.
Its backend boundary separates physical storage from mapping and requires
successful unmap/invalidation before free. Safely rejected maps unwind;
uncertain map or unmap results retain backing and metadata. Handle identity
includes registry, PCI device, slot and allocation generation; submissions add
a nonwrapping sequence. The registry never frees in its destructor.

The kernel backend accepts only the two explicit passthrough domain modes.
It allocates zeroed, exclusively owned power-of-two backing with checked
alignment padding, validates the complete page-rounded DMA extent against the
mask and uses x86 PCI coherent-memory fences. It adds `DMA_BUFFER` accounting
without changing existing owner values. This is not a translated mapping yet.
The current capacity is 4,096 regions and backing is bounded to 64 MiB per
region including padding. An unsuitable physical address fails cleanly rather
than truncating a mask or exposing a neighboring allocation.

Allocator, copy, synchronization and mapping callbacks execute outside the
IRQ-saving registry lock. Busy record states reject concurrent release or
reuse during those callbacks. Preparation and completion reuse storage without
allocating. Once a current scheduler task exists, allocation/release reject
atomic/transition contexts. Before that point, BSP boot callers must establish
ordinary init context themselves: queue setup alone does not establish a
current task or a usable preemption-depth test. KTEST runs in that interval.

Retirement values are explicit assertions by the driver, not hardware proof
manufactured by the DMA subsystem. A never-submitted address can be cancelled;
published streaming storage needs command completion and reader drain;
coherent rings need device quiescence and reader drain. Drivers still own
descriptor protocol barriers and serialization of CPU views. No live driver
uses the new API yet, so driver conversion must validate each assertion.

Next AHCI conversion evidence: `identify_disk()` currently deletes its DMA
buffer before recovery on TFES; change that ordering with the conversion.
Engine stop and IDENTIFY/flush completion waits are unbounded. The driver
enables global AHCI interrupts but has no registered AHCI handler in this
translation unit; its operations are polled. All these paths need bounded
retirement before managed releases. Keep flush failure distinct from durable
success. Also account for block I/O from page-fault/atomic contexts: prefer
preallocated per-port streaming storage plus a bounded rebind operation, not
new allocation for each command. The same rebind facility will let network
descriptor pools reuse DMA backing for changing PacketBuffer CPU spans.

### Stage 5 verified checkpoint (2026-09-22)

- Added common API/backend/manager under `modules/kern/src/dev/dma/`, appended
  allocator ownership, and added shared-manager host and real-allocator KTEST
  coverage. Existing driver DMA paths are unchanged in this stage.
- Normal `Build WOS` (`RelWithDebInfo`) and isolated diagnostic KTEST (`Debug`)
  passed. An initial diagnostic compilation exposed a test-only pointer-cast
  error; it was corrected before the successful builds and live run.
- Host ASan/UBSan: **56 cases passed** across nine selected CTest targets,
  including 11 DMA manager cases, prior IOMMU/accounting cases and fuzz smoke.
  Cases exercise exact command generations, cross-device/registry rejection,
  capacity and counter exhaustion, all streaming directions/partial copies,
  allocation/map failures, invalid backend addresses, busy interleavings,
  context rejection and retained storage on uncertain map/unmap/timeout.
- Repository formatting and focused clang-tidy completed successfully with
  advisory warnings. `git diff --check` passed.
- Rootless IOMMU-on KTEST: **65,912 assertions passed, 0 failed**, normal
  poweroff at 485.485 seconds. All three new DMA tests passed, as did domain
  accounting. Tests verify real exclusive backing, alignment, mask-failure
  unwind, CPU/bounce copy bounds, stale submissions, and allocation-free
  preparation/completion with IRQs disabled. Test addresses were never handed
  to hardware; CPU-emulated bounce writes are not claimed as device-DMA evidence.
- The running kernel's KCOV fingerprint matches the retained ELF. Final
  coherency-contract comments were rebuilt in both profiles; the documented
  build has identical executable text to the tested ELF. Both source snapshots
  and manifests are retained, together with the original and documented ELFs.
  WOSDBG shared CLI log analysis succeeded. The task-owned VM/launcher exited.

Receipts: `build/goal20-stage5/validation-on.json`, `verify-on.log`, build/host/
format logs, source manifests and archives, saved ELFs, serial log and WOSDBG
batch/results. Fixture: `ktest-data/goal20-stage5/on/node.json`.

Remaining qualification is explicit: the backend assumes driver-configured,
ordinary snooped x86 PCI DMA, and does not detect device-specific noncoherent
operation. Retain protocol fences for updates to already published coherent
rings. No converted driver, translated mapping, per-device retirement callback,
fault interrupt handler, deliberate stale hardware DMA, strict isolation or
real-hardware qualification is claimed. Next add bounded streaming rebind for
preallocated pools and convert AHCI with bounded stop/recovery, exact mapping
ownership, S64A masks, durable flush semantics and failure quarantine.
Goal 20 remains active.

### Stage 8a patch contract: virtio queue/control DMA ownership

Problem: split rings and the MQ command page use raw physical allocations;
probe failures discard the software owner without resetting exposed queues.
This increment converts these persistent objects before packet streaming and
terminal NIC retirement. It does not claim complete virtio conversion/isolation.

Expected files: `dev/virtio/{virtio.hpp,virtio.cpp,virtio_net.hpp,virtio_net.cpp}`,
new shared `virtio_dma.hpp`, host ownership tests and existing source guards,
focused diagnostic tests if needed, and this ledger.
Files read: these driver entrypoints, DMA public API/manager/backend, PCI config
and BME admission, AHCI coherency setup, netdevice/netpoll/packet interfaces,
netpoll worker/disable/inline paths, Ethernet dispatch, init ordering, kernel
CMake/tasks, subsystem/kernel/critical-path/WKI/logging instructions.

Flow: serialized boot probe resets the transport, disables BME, establishes
ordered snooped PCIe access, allocates/maps/prepares rings and control storage,
publishes queue addresses, then admits BME before DRIVER_OK. Failed unpublished
probe resets and reads status zero, disables BME, then completes/releases all
owned mappings; a missing reset/BME proof pins exposed storage. Modern transport
failure after ownership begins must not fall through into a legacy reprobe.

Invariants: split ring ABI/layout, packet ownership, notification handshake and
MQ fallback remain intact. Legacy queue PFNs fit the 32-bit register (44-bit
byte-address mask); unsupported legacy queue sizes are rejected rather than
silently programming a different layout. Used control storage remains owned
through late ACK/timeout until whole-device reset; timeout is not retirement.

Locks/unsafe contexts: allocation, reset polling and unwind are boot/task-only,
before IRQ handlers/NAPI/netdev publication. Existing TX and notification
spinlocks and RX/NAPI paths gain no allocation or blocking work. No software
reader drain is inferred from `napi_disable`: published NIC teardown remains
pending and retains stable control objects because workers/snapshots hold raw
pointers. Probe unwind is explicitly forbidden after callback publication.

ABI/wire/syscall impact: none. ACCESS_PLATFORM/translated virtio domains remain
pending; current managed addresses are explicit passthrough physical addresses.
Tests: shared ownership/reset/layout host cases, driver source ordering guards,
Normal Build WOS, isolated build/selftests and rootless QEMU NIC probe evidence.
Rollback: revert this increment together; never free retained uncertain DMA.
Unknowns: physical-device reset completion, live SMP NIC terminal retirement,
packet streaming conversion and translated endpoint capability qualification.

### Stage 8a implementation and qualification (2026-09-22)

Virtio split rings now own common coherent DMA allocations and submissions.
Modern queue registers use offsets within that submission; legacy PFNs use an
explicit 44-bit byte-address mask. The shared layout accepts only supported
power-of-two queue sizes; legacy hardware sizes are no longer silently clamped
to a different ring layout. The MQ control page is prepared during probe and
retained through success or timeout, replacing the untracked raw-page leak.
Control completion checks the used index, head ID, length and ACK.

Probe resets and reads status zero, disables BME, verifies ordered/snooped PCIe
access, prepares persistent mappings, then enables BME. On failure before
callback publication, it resets, checks BME disable and releases all owned
queues/control storage. Missing quiescence proof quarantines exposed mappings
and retains the failed owner; it cannot be reprobed. Modern owned-probe failure
does not fall through to legacy. Already published devices are not reprobed.
Normal packet, IRQ/NAPI and administrative open/close paths remain unchanged.
Published NIC retirement and packet DMA conversion are still outstanding.

The reset acknowledgement rule follows Virtio 1.3 section 4.1.4.3.2 in the
[OASIS specification](https://docs.oasis-open.org/virtio/virtio/v1.3/virtio-v1.3.pdf).
The shared `Region` helper deliberately has no freeing destructor; its exposure
and retirement state machines, layout and bounded reset wait have eight host
cases. Source checks cover production address provenance and ordering. A new
final-phase KTEST drives the production unpublished probe, enables the device,
issues a real control command when offered, and resets/unwinds before any
IRQ/NAPI/netdev callback publication.

Validation:

- Normal `Build WOS` (`RelWithDebInfo`) and isolated diagnostic build (`Debug`)
  passed, with format/tidy checks successful and advisory style warnings.
  The first normal link exposed a packed atomic load's libatomic dependency;
  the final code uses the page-aligned x86 16-bit load and DMA acquire fence.
- 77 host cases across 11 executables, DMAR fuzz smoke and two source checks
  passed (14 selected CTest targets). The existing credential-wrapper assertion
  and unreachable scheduler source-test audit failures reproduce unchanged;
  the final-phase checks pass. See `audit-baseline.json`.
- Four focused rootless boots passed and powered off normally. Modern IOMMU-on
  and IOMMU-absent boots each acknowledged `MQ pairs=1` and released four
  mappings / 11 pages. Withheld reset proof retained all four / 11 as quarantined
  despite BME being disabled. Legacy-only mode released two mappings / eight
  pages. Counts were 27, 27, 27 and 26 assertions respectively, zero failures.
  Withholding proof exercises the failure branch; it does not emulate a hung
  reset. Socket backends expose one pair: this is not multiqueue traffic evidence.
- Full Intel-IOMMU KTEST passed **65,970 assertions, zero failures**, then
  powered off normally at 519.608 seconds. The new virtio case ran after the
  filesystem workloads, acknowledged its control command and released all
  four mappings / 11 pages. The runtime audit confirmed all 1,165 enabled
  cases and the three declared disabled skips. Its coverage fingerprint matches
  the same saved ELF. All ten WOSDBG load/search calls across the five logs
  succeeded, and every task-owned launcher/VM exited.

All focused fingerprints match the saved ELF:
`text_start=0xffffffff80000158`, `text_end=0xffffffff817dd776`,
`text_fnv64=0xe4aec61dd70818b2`; SHA-256
`39c2c8c90d84f28c237517764a5fb2f529b24b777e60dcc33c776ff6eff11785`.
The source archive covers 66 files. WOSDBG loaded and searched all four logs
successfully. Receipts/scripts are under `build/goal20-stage8a/`; fixtures are
isolated under `ktest-data/goal20-stage8a/`.

Next conversion constraints, verified against current local source:

- RX refill and TX still program raw packet addresses. Preallocated streaming
  slots must complete before descriptor reuse or packet delivery/free, validate
  used IDs/lengths, and introduce no DMA allocation in IRQ/NAPI or under TX locks.
  Four NICs at four pairs of 256 RX/TX slots exceed the current 4096-record DMA
  registry; capacity/footprint or an explicit subregion design must be resolved.
- `gates::free_irq` already fences context admission and drains IRQ readers.
  `napi_disable` does not drain raw worker/snapshot references; device/NAPI
  objects need stable lifetime plus a driver admission/drain contract before
  queue storage can be freed. The worker's initial registry scan is unlocked.
- `netdev_unregister_begin` unpublishes and retires route/interface/ARP/NDP
  state; owned packets must be released before waiting for retained references.
  WKI transport references/fencing and administrative reopen semantics still
  require a full terminal-retirement flow review.
- ACCESS_PLATFORM, translated/grouped domains, physical hardware, live packet
  traffic, allocation-failure sweeps, published NIC teardown, other PCI drivers
  and strict enforcement remain unqualified. Passthrough provides no isolation.
  Goal 20 remains active.

### Stage 8b patch contract: virtio packet streaming DMA

Problem: RX/TX descriptor addresses still directly expose PacketBuffer backing;
descriptor reuse precedes managed completion and malformed used entries can
continue consuming/freeing queue state. The 4096-record registry cannot cover
four NICs with four pairs of 256 RX/TX descriptors each.
Expected files: virtio queue/network entrypoints and tests, DMA registry capacity,
host production-queue harness, KTEST/probe qualification and goal evidence.
Files read: current virtio queue/driver/ownership headers, common DMA manager and
backend, packet storage/pool, netdevice/netpoll lifetimes, Ethernet dispatch,
PCI/IRQ entrypoints, applicable kernel/critical/network/WKI/test instructions.

Flow: probe preallocates one directional streaming slot per RX/TX descriptor
before exposing rings/admitting BME. Submission binds only a bounded packet
storage span and prepares/copies it before publishing the descriptor. Completion
acquires the used index, validates bounded pending count, ID/ownership and RX
length, completes the DMA copy, then recycles the descriptor. Invalid completion
stops that queue and retains all outstanding owners for reset/retirement.

Invariants: packet storage/metadata is never directly device-visible; RX copies
only reported validated bytes, TX never copies back; no new DMA allocation or
blocking work in IRQ/NAPI/TX locks. Jumbo and mergeable headers, MQ fallback,
notification rearm, successful/failed TX packet consumption and WKI liveness
remain intact. Ring/control wire ABI and syscall ABI do not change.
Locks: existing caller TX lock / NAPI queue ownership, short DMA metadata lock;
copy/sync outside DMA registry lock. Probe/retirement is serialized boot/task
context. No published queue teardown is inferred from NAPI disable.
Allocation: raise the bounded registry to 16384 entries with explicit footprint
and NIC-capacity checks; allocate exclusive streaming backing only at probe.
Unwind: never free exposed backing on unproven reset; quarantine retains mappings,
and borrowed packet references may be consumed only after completion/quarantine
has detached the CPU borrow. Unknown state retains the packet too.
Build/tests: actual production virtqueue linked to the shared DMA manager in
host tests (copy bounds, wrap/duplicate/bad-ID/length, allocation failure, reset
unwind); Normal Build WOS; isolated KTEST; rootless QEMU packet/control traffic
and IOMMU on/off/legacy variants where supported.
Rollback: revert this increment together; never reclaim quarantined DMA without
an independently proved reset and reader drain. Published SMP teardown, physical
hardware and translated-domain isolation remain separately required work.

Stage 8b contract addendum before header correction: Virtio 1.3 sections 5.1.6
and 5.1.6.1 require the modern base header's `num_buffers` field even without
MRG_RXBUF; only legacy may use the shorter header. Current source selected 10
bytes for both when mergeable RX was disabled, so that feature mode shifts the
actual Ethernet payload. Correct modern size to 12 and apply the existing
no-mergeable boot option to legacy too, then qualify both wire layouts with
small/jumbo packet echoes. This fixes conformance rather than defining a new ABI.

### Stage 8b implementation (2026-09-22)

RX/TX virtqueues now preallocate one directional streaming DMA allocation per
descriptor. Submission binds only packet storage, prepares the bounce copy,
and publishes the managed device address. Completion validates the used count,
descriptor ownership and RX length, completes the DMA operation, then permits
descriptor reuse and packet delivery/free. TX ignores the legacy device's
unreliable used length because it has no writable payload. Invalid completions
stop the queue and retain outstanding owners for terminal reset. Runtime queue
operations allocate no DMA backing and keep their existing nonblocking context.

Probe retirement completes or quarantines every slot before consuming its CPU
packet reference. A withheld reset proof retains all exposed queue backing,
including idle slots; quarantine detaches CPU borrows while keeping device-visible
pages pinned. Published NIC teardown remains separate work. The registry now has
16,384 entries, enough for the configured maximum of four NICs with four pairs
of 256 RX/TX descriptors plus other drivers. Its linked object occupies 1,703,952
bytes (under the enforced 2 MiB limit). Preallocated packet backing costs 32 MiB
per four-pair NIC, or 128 MiB at that maximum, excluding rings/control pages.

The modern network header is always 12 bytes, including when mergeable receive
buffers are disabled; legacy retains its negotiated 10/12-byte format. Both
transports now honor the existing no-mergeable boot option. This follows
[Virtio 1.3, sections 5.1.6 and 5.1.6.1](https://docs.oasis-open.org/virtio/virtio/v1.3/virtio-v1.3.pdf).

Eight new host cases link the actual production virtqueue implementation to
the shared DMA manager and real PacketBuffer definition. They cover bounded
RX copying and metadata exclusion, TX copy-before-publication, invalid/duplicate
completion retention, 16-bit index wrap, allocation-failure unwind at each slot,
and uncertain-reset quarantine. The final KTEST probe optionally transmits
64-byte and 9,014-byte Ethernet frames through the normal queue submission path;
a rootless host multicast peer independently checks every outgoing byte and
returns marked replies that WOS verifies after managed RX completion. This is
serialized unpublished-probe traffic, not concurrent IRQ/NAPI or multiqueue
workload qualification.

Stage 8b validation:

- Normal `Build WOS` (`RelWithDebInfo`) and isolated diagnostic build (`Debug`)
  passed. Formatting/clang-tidy passed with advisory style warnings. All 85
  host cases across twelve executables passed, plus DMAR fuzz smoke and two
  source checks (15 selected CTest targets).
- Six focused rootless boots passed: modern, withheld reset proof, legacy,
  IOMMU absent, modern without mergeable RX, and legacy without mergeable RX.
  Each checked both frame sizes and restored the packet pool `1024->1024`.
  Modern variants released `516` regions / `2059` pages; legacy released
  `514` / `2056`. Withheld proof quarantined all `516` / `2059` instead.
  Modern cases passed 30 assertions each; legacy cases passed 29 each.
- Full rootless Intel-IOMMU KTEST passed **65,973 assertions, zero failures**.
  The runtime audit confirmed all 1,165 enabled tests and three declared skips.
  Final AHCI retirement released `210->0` regions / `13068->0` pages. Virtio
  checked both packet echoes and released `516->0` regions / `2059->0` pages,
  with no quarantine and the packet pool restored. Normal poweroff followed.
- All seven coverage fingerprints match the retained diagnostic ELF:
  `text_start=0xffffffff80000158`, `text_end=0xffffffff817e2de6`,
  `text_fnv64=0x2042fcb5c3c13240`. SHA-256:
  `55ecde6b39f216a6499f117bbeb58dd0ac382dc309ba478dab731988d7789c83`.
  The 69-file source manifest matches current code. All fourteen WOSDBG shared
  CLI load/search calls succeeded. Task-owned launchers and VMs exited.
- The two broader audit failures documented above remain reproducible baseline
  issues: eight credential wrappers lack direct assertion macros, and one
  scheduler source-test function is unreachable. Neither audit is reported as
  passing or bypassed. Final-phase checks pass independently.

Receipts: `build/goal20-stage8b/validation.json`, individual validation/echo JSON,
build/host/format/runtime-audit logs, source archive/manifest, saved ELF and
WOSDBG results. Rootless fixtures remain in `ktest-data/goal20-stage8b/`.

Next-step lifetime reconnaissance: `free_irq()` closes admission and drains
context handlers. `napi_disable()` removes registry membership and waits for
POLLING to end, but snapshots and worker epilogues can retain raw NAPI pointers;
the current worker lookup also scans the registry without its lock. Device/NAPI
objects therefore need stable lifetime and a separate callback admission/drain
gate before queue retirement. `netdev_unregister_begin()` unpublishes and closes
new references; owned packets must retire before waiting for their references.
Ethernet WKI transports retain raw NetDevice pointers, so unregister alone does
not make freeing the driver object safe. IRQ registration failures and MSI-X
fallback ownership must also be accounted for. These are implementation inputs,
not evidence of completed published-NIC teardown.

Remaining scope includes published IRQ/NAPI/SMP and multiqueue traffic/teardown,
physical hardware, remaining PCI drivers, translated/grouped domains, actual
stale/unmapped DMA faults and strict enforcement. Passthrough still provides
no DMA isolation. Goal 20 remains active.

### Stage 8c patch contract: terminal virtio callback and DMA retirement

Problem: published IRQ/NAPI/TX/control/diagnostic callbacks can access queue
memory and no terminal NIC retirement path drains them before freeing DMA.
Expected files: virtio driver/header plus shared lifecycle rules, power finalizer,
host lifecycle tests, kernel probe qualification and affected source checks.
Files read: virtio queue/network implementation, DMA manager/backend, PCI MSI/
MSI-X/config interfaces, IRQ registry drain, netdevice registration/control,
NAPI worker/inline snapshot flow, Ethernet/WKI transport references, AHCI
retirement, power ordering and relevant kernel/critical/WKI instructions.
Flow: close a monotonic atomic callback admission gate; drain existing guards;
unpublish netdevice control/registration; disable retained NAPI contexts; prove
PCI interrupt masking, transport reset and BME disable; drain owned IRQ handlers;
retire queues/control allocations. Keep the closed device/NAPI objects alive
for raw WKI references, snapshots and worker epilogues. Never reopen/reprobe.
Invariants: all queue/transport callback access is guarded; failed TX still
consumes its packet once; no DMA allocation or waiting in IRQ/NAPI admission.
Reader-drain failure retains all queue and packet state. Unproven hardware/IRQ
retirement pins DMA backing. No free before device quiescence and IRQ drain.
Locks: atomic callback references, existing TX/notification locks inside guards;
single retirement claimant per device; no spinlock across teardown waits.
Unsafe contexts: retirement only sleepable task or serialized pre-task BSP;
never IRQ/NAPI, panic, or inside an admitted callback. Power calls it after WKI
shutdown and VFS/AHCI retirement, while other CPUs can still drain readers.
Allocation/blocking: no new retirement allocation; bounded reader/reset waits;
IRQ drain uses the existing registry contract. CPU tombstones are kernel-lifetime.
ABI/wire impact: none; only internal lifecycle/API and diagnostic selftest flags.
Tests: shared production ordering and concurrent admission tests; late callback,
successful and withheld-proof real-device retirement qualification; both builds,
source checks, isolated KTEST and rootless QEMU. Serialized BSP tests do not
replace later SMP/NAPI traffic qualification.
Rollback: keep admission closed and backing retained on any missing proof;
revert lifecycle and all guarded call sites together, never force-free quarantine.

### Stage 8c implementation: terminal virtio retirement (2026-09-22)

Added a monotonic callback admission gate shared by production and host tests.
IRQ, NAPI, TX, control, open/close and diagnostic entrypoints acquire a reference
before accessing queues or transport registers. Terminal retirement closes the
gate and waits with a deadline for admitted callbacks. A failed drain disables
BME but retains queue ownership and CPU borrows without modifying their state.
After a successful drain it unpublishes the netdevice, fences controls, disables
retained NAPI contexts, masks and reads back MSI/MSI-X/INTx sources, resets the
transport, checks BME disable, drains owned IRQ registrations, and finally
retires managed allocations. Missing hardware proof quarantines backing.

The closed device/NAPI objects and existing NAPI registry entries stay alive
for raw WKI transport pointers, inline snapshots and worker epilogues. No
netdevice reference wait is needed to free this storage because it is never
freed or registered again. Queue pages can retire independently after the
driver admission/drain proof. Repeated shutdown returns the stored terminal
outcome, and reprobe/reopen is refused. The normal power finalizer calls NIC
retirement after WKI and VFS/AHCI shutdown and before halting other CPUs.

IRQ ownership is explicit: allocated vectors and successfully registered INTx
handlers belong to the device; a conflicting handler is never freed. Registration
failures retire the partially published driver. MSI fallback disables the prior
MSI-X mode first, and discarded modern queue vectors are unassigned before
release. The existing nonblocking queue/notification operations remain inside
their original locks; admission uses only atomic references.

The `--virtio-test-terminal` final-phase diagnostic publishes actual netdevice
and IRQ registry entries after the packet echo workload, invokes production
shutdown, then exercises late TX/poll/IRQ/control/open/reprobe and repeated
shutdown. It deliberately does not start NAPI workers on parked BSP KTEST CPUs.
This proves real-device reset/masking/release and callback rejection, not live
SMP scheduling or asynchronous IRQ/NAPI drain under load.

Next qualification issue found in source: scheduler
`maybe_exit_current_kernel_thread_for_shutdown()` can terminate a DAEMON at a
yield/sleep boundary without unwinding C++ stack guards. It currently defers for
preemption-disabled regions and an owned unpublished process, but not a driver
callback reference. If a callback yields through a protocol path during power
shutdown, its reference may therefore remain pinned. The bounded drain safely
retains all DMA in that case; successful normal SMP shutdown is not yet proved.
Resolve cooperative-exit ownership and exercise task-context traffic/retirement
before claiming complete virtio lifecycle qualification. No scheduler edit is
included in Stage 8c.

Stage 8c validation:

- Normal `Build WOS` (`RelWithDebInfo`) and isolated diagnostic build (`Debug`)
  passed. Formatting/clang-tidy completed with advisory warnings. All 91 host
  cases across thirteen executables passed, plus DMAR fuzz smoke and three
  source checks (17 selected CTest targets). Six new lifecycle cases exercise
  actual shared ordering, every missing hardware proof, failed reader drain,
  failed release, concurrent admitted/late callers and nested references.
- Four focused rootless boots passed: modern (34 assertions), withheld reset
  proof (34), legacy (33), and IOMMU absent (34). Each verified both packet
  sizes, real IRQ/netdevice publication, IRQ removal, late callback rejection,
  refused reprobe, stable repeated shutdown and restored packet pool. Modern
  released 516 regions / 2,059 pages; legacy released 514 / 2,056. Withheld
  proof quarantined all 516 regions / 2,059 pages while detaching CPU borrows.
- Full rootless Intel-IOMMU KTEST passed **65,977 assertions, zero failures**.
  Runtime audit confirmed all 1,165 enabled cases and three declared skips.
  AHCI released `210->0` regions / `13068->0` pages, followed by virtio
  `516->0` / `2059->0`; both had zero quarantined mappings. Both packet echoes
  passed and the packet pool returned `1024->1024`. The guest powered off
  normally and the launcher exited zero.
- All five coverage fingerprints match the retained ELF:
  `text_start=0xffffffff80000158`, `text_end=0xffffffff817eb876`,
  `text_fnv64=0xf78cbd7509127356`. SHA-256:
  `ef2e9d6b000b8e39b08f393f583cf8bd37608cd08fe385360f315955db377179`.
  All 71 source hashes match current implementation/test files. Ten WOSDBG
  shared CLI load/search calls succeeded. All task-owned VMs exited.
- The two known broader audit failures remain independently reproducible:
  unchanged credential wrapper assertions and unreachable scheduler source-test
  function. They are not passing gates; final-phase checks pass independently.

Receipts and scripts are in `build/goal20-stage8c/`, especially `validation.json`,
per-boot validation/echo/serial logs, `runtime-audit.log`, build/host/format logs,
`audit-baseline.json`, the source archive/manifest and saved ELF. Fixtures remain
isolated in `ktest-data/goal20-stage8c/`. These tests do not prove asynchronous
SMP/NAPI retirement, cooperative daemon-exit safety, physical hardware behavior,
translated domains or real stale/unmapped DMA faults. Goal 20 remains active;
other PCI drivers and strict isolation are still required.

<!-- Stage 8d contract recorded before scheduler/NAPI ownership edits. -->

### Stage 8d patch contract: callback ownership across cooperative exit

Problem: scheduler shutdown exits DAEMON tasks at yield/sleep boundaries without
C++ stack unwinding, which can strand a virtio callback admission reference.
NAPI contexts retain worker pointers without task references, so worker exit/GC
can also invalidate a late snapshot/wakeup even when device storage stays alive.
Expected files: task/scheduler ownership helper and exit predicate, virtio
callback guards/live diagnostic, NAPI worker retention/lookup, host/kernel tests,
runtime fixture and evidence. No scheduling placement or wire protocol changes.
Files read: task construction/reference/exit fields, scheduler cooperative exit,
yield/block helpers and task lookup, NAPI initialization/worker/wakeup/snapshots,
virtio callbacks/retirement, init phases, power finalizer, critical/kernel/WKI
instructions and current build/test entrypoints.
Invariants: only the currently executing task's scope defers cooperative exit;
yielding, interrupts and preemption remain enabled. Nested ownership releases
the callback gate before releasing its shutdown hold. Pending shutdown stays
pending and can exit at the next safe boundary. Existing unpublished-process,
preemption and interrupt exit guards remain intact. No force-free after failed
callback/reset/IRQ proof. Worker references are obtained before scheduling.
Locks/context: guard operations are allocation-free atomics, valid in IRQ/NAPI/
TX contexts and a no-op before task context exists. No scheduler lock is added
to callback admission. Registry lookup uses its existing lock; no blocking under
it. Live retirement runs from a separate sleepable diagnostic daemon.
ABI: no syscall/wire changes. Task layout is internal; assembly-visible offsets
must remain generated from offsetof and be rebuilt in both profiles.
Tests: nested/current-owner deferral and exit-eligibility model tests, source
integration checks, Normal Build WOS, isolated KTEST, then non-BSP rootless
traffic/retirement with actual NAPI workers on multiple CPUs. Missing runtime
evidence remains incomplete rather than being inferred from BSP tests.
Rollback: retain backing and closed admission if live drain cannot be proved;
revert exit predicate and ownership guards together, never bypass pending exit
or DMA retirement proofs to make the diagnostic pass.

## Stage 6 patch contract: AHCI managed DMA conversion

Problem: AHCI still programs raw allocation/translated caller addresses, frees
IDENTIFY storage before stopping on errors, and has unbounded engine/flush waits.
Expected files to change: common DMA reusable streaming bindings, AHCI source
and a shared engine/completion model, host/kernel tests and affected source
regression assertions; optional diagnostic probe-failure hook and this ledger.
Files already read: complete AHCI source/header, common DMA manager/backend,
PCI config/admission/capability API, block provider contracts and source tests,
mutex/scheduler context, init order and kernel/critical-path instructions.
Intel AHCI 1.3.1 sections 6.2.2, 10.1 and 10.3 establish ordered ST/CR then
FRE/FR stop, at least 500-ms stop allowance, and restart readiness rules:
https://www.intel.com/content/dam/www/public/us/en/documents/technical-specifications/serial-ata-ahci-spec-rev1-3-1.pdf
Invariants to preserve: command/FIS/PRDT ABI, non-NCQ per-port serialization,
512-KiB command limit, errno behavior and flush durability. Every programmed
DMA address comes from a managed submission. Stop inherited engines first;
prepare all mappings with BME disabled; enable BME only after ring installation.
Polling has time and iteration bounds. Unproven retirement quarantines buffers
and disables controller admission; never free/rebind them on a timeout alone.
Locks involved: existing per-port sleeping mutex; short DMA/PCI IRQ-save guards
inside calls, never held across allocator work or hardware waits. AHCI remains
polled, masks both global/port interrupts and installs no IRQ readers to drain.
Unsafe contexts: no new IRQ/NAPI polling. Block I/O may run with allocation
forbidden, so allocate streaming pools during probe and bind caller buffers
without allocating for each command. Kernel CPU spans only enter the DMA API.
Allocation/blocking assumptions: fallible probe allocations; no command-time
allocation/free. Safe probe failure unwinds all never-exposed mappings. Runtime
failed-port/controller storage stays retained until a full safe teardown is
implemented; do not imply that this is zero-mapping detach qualification.
ABI/wire/syscall compatibility impact: none. PCIe No Snoop/relaxed ordering
permissions are cleared for this coherent, ordered backend when present.
Build/test plan: shared stop/completion model tests, streaming rebind/lifetime
tests, source audit forbidding raw AHCI DMA addresses, normal/isolated builds,
rootless IOMMU-on filesystem workloads and focused failure-unwind evidence.
Rollback plan: keep the working passthrough backend while converting; never
revert live storage ownership by freeing or reusing unretired regions.

## Stage 6: AHCI managed DMA and reusable streaming pools (2026-09-22)

AHCI command lists, received-FIS areas, command tables, IDENTIFY payloads and
read/write payloads now obtain every programmed DMA address from the common
DMA API. Probe stops inherited engines, masks global/port interrupts, disables
BME, prepares all managed mappings, installs rings, and then enables BME.
The existing polled, non-NCQ port mutex covers command preparation through
completion. S64A controls the DMA mask. PCIe No Snoop and Relaxed Ordering
permissions are cleared and checked when the capability is present.

Each port has a preallocated 512-KiB streaming pool. New `allocate_streaming`
and `bind_streaming` operations separate fallible probe allocation from bounded,
allocation-free command submission. Bindings enforce capacity and mapping
direction, reject partially overlapping CPU views, and are consumed by
completion or quarantine. User buffers pass through existing pinned, no-fault
usercopy operations; devices never receive borrowed user/kernel addresses.

Data, IDENTIFY and durable flush use one bounded command path. Engine stop
clears ST, waits for CR, then clears FRE and waits for FR. A failed stop pins
managed backing and requests BME disable; timeout alone never permits reuse.
Command errors retire streaming data only after proven stop. Unsafe restart
conditions leave the port offline with backing retained. Successful flush is
still required for durability. Command-slot count now decodes CAP.NCS as N+1.
The optional AHCI benchmark is retained for data commands.

The shared stop/completion model has four host cases. DMA tests add pool reuse,
borrow consumption, stale submissions, atomic-context operation and overlap/
direction/capacity checks. The block-device source regression now checks managed
addresses, common command serialization and mapping-before-BME order. Its old
SmallVec/remove_block_dev_node assertions were stale against the current
BlockRegistryEntry implementation; they now check stable wrapper allocation,
publication and removal-before-free using current source. No block registry
implementation was changed.

Validation in this stage:

- Normal `Build WOS` (`RelWithDebInfo`) and isolated diagnostic build (`Debug`)
  passed. Formatting and clang-tidy completed; clang-tidy reports advisory
  include/bracing/style warnings, so this is not a warning-clean audit.
- 62 host unit cases passed across nine executables, plus the DMAR fuzz smoke
  and block-device source check: 11 selected CTest targets, all passing.
- Full rootless Intel-IOMMU KTEST passed **65,943 assertions, zero failures**,
  with normal poweroff at 515.764 seconds. Both SATA disks and partitions were
  registered through managed DMA; the filesystem suite exercised real QEMU
  AHCI transfers. AHCI retained 210 managed regions / 13,068 backing pages.
  All four DMA kernel tests and IOMMU physical-accounting tests passed.
- A separate `--ahci-test-fail-prepare` boot deliberately stopped preparation
  after seven coherent allocations. It reported complete unwind with regions
  `0->0` and pages `0->0`, and no SATA publication. The task-owned VM was then
  deliberately stopped via its private QMP socket; this was a focused check,
  not a full-suite run.
- A focused rootless boot without an IOMMU selected software passthrough,
  allocated the same 210 managed AHCI regions, registered both SATA disks and
  reached KTEST. It was deliberately stopped through private QMP after that
  evidence; this is fallback boot/read evidence, not a second full-suite run.
- The full-run KCOV fingerprint matches the retained diagnostic ELF, and all
  45 retained source files match their SHA-256 manifest. WOSDBG shared CLI
  log analysis succeeded. Receipts, source archive, saved ELF and logs are in
  `build/goal20-stage6/`; isolated fixtures are in `ktest-data/goal20-stage6/`.

Limits remain explicit. Domains still use passthrough, with no DMA isolation.
Successful command retirement and partial-probe unwind are qualified here;
runtime controller reset/detach, deliberately stalled hardware and zero-live-
mapping teardown are not. Active coherent rings/pools remain allocated for
controller lifetime, and uncertain failure backing remains pinned. No claim
is made about translated mappings, real stale/unmapped DMA faults or physical
hardware. Next close AHCI command admission and qualify full quiesce/teardown,
then convert the remaining drivers and implement strict translation/faults.
Goal 20 remains active.

## Stage 7 patch contract: AHCI admission closure and full retirement

Problem: converted AHCI mappings remain allocated for controller lifetime;
there is no production teardown that proves command readers and engines have
retired before unmap/free, or per-device accounting to audit that boundary.
Expected files to change: AHCI driver/header and shared lifecycle rules, DMA
accounting API/manager, power finalizer, KTEST final-phase registry/runner/linker
section, focused diagnostic flags, host/kernel/source tests and this ledger.
Files already read: current AHCI source/header/rules, DMA API/manager/backend,
PCI bus-master gate, IOMMU domain lifetime, block-provider lifetime, mutex,
allocator context predicate, power finalizer, KTEST registry/runner/linker/init
entrypoints, kernel/critical-path/logging instructions. Intel AHCI 1.3.1
sections 10.1/10.3 were rechecked for both engine idle conditions.
Invariants to preserve: accepted commands finish under their existing port
mutex; admission closure precedes draining all port locks. Final ATA flush
occurs after draining and before stopping engines. Failed flush never reports
durability. No release before every engine has stopped and BME disable is
confirmed. Failure to prove quiescence retains/quarantines backing. No IRQ
handler exists; global and port IRQ admission stay masked and are checked.
Block-provider objects remain static tombstones whose callbacks reject I/O.
Boot initialization cannot resurrect a closed controller. Per-device IOMMU
domain/table lifetime is unchanged; no translated isolation is claimed.
Locks involved: lifecycle sleeping mutex -> all port mutexes in ascending
order -> short PCI/DMA IRQ-save guards. Commands hold one port mutex and never
take the lifecycle mutex. Reader drain uses bounded try-lock polling; hardware
waits remain bounded. No allocator/registry spinlock spans device waits.
Unsafe contexts: production shutdown runs before CPU halt, after VFS sync and
unmount. Task callers must be sleepable; pre-task BSP selftests are serialized.
No teardown from IRQ/NAPI/panic or while already holding a port mutex.
Allocation/blocking assumptions: no new teardown allocations. Completion,
unmap and free occur only in valid task/BSP context after retirement. An
unproven reader drain retains all backing without touching unowned port state.
ABI/wire/syscall impact: none; kernel-only shutdown/accounting interfaces and
selftest final-phase registry change. Existing command/descriptor ABI stays.
Build/test plan: shared lifecycle order/failure and admission-race host tests;
per-device accounting; final KTEST real read/flush/teardown/late-callback checks;
Normal Build WOS, isolated IOMMU-on full suite, focused unproven-stop and
flush-failure diagnostic boots. Injection is not a real hardware hang claim.
Rollback: leave admission closed and retain backing on uncertain teardown;
never undo retirement failure by force-freeing or reopening old ring addresses.

### Next-driver reconnaissance note

The current virtio-net modern initializer reads upper features but accepts only
`VIRTIO_F_VERSION_1`; both modern and legacy paths still program raw translated
addresses and enable BME before queue allocation. These paths need a complete
NAPI/IRQ/packet-ownership mapping before conversion. The local QEMU device-help
output reports `iommu_platform` defaulting off. Its output is retained in
`build/goal20-stage7/virtio-device-help.txt`.

Strict virtio qualification must include the platform-address contract, not
merely an enabled Intel-IOMMU device. Virtio's ACCESS_PLATFORM feature (bit 33)
is the contract for restricted/translated device access; without it, the
specification requires physical addresses. See [Virtio 1.3, sections 6 and
6.1](https://docs.oasis-open.org/virtio/virtio/v1.3/virtio-v1.3.pdf).
Negotiate the offered feature and configure the QEMU endpoint appropriately
when testing translated mappings; define explicit legacy/unavailable-feature
policy. This is reconnaissance, not a completed driver conversion or proof of
virtio DMA isolation.

## Stage 7: terminal AHCI shutdown and final-phase qualification (2026-09-22)

Added terminal `ahci_shutdown()` and connected it to normal power finalization
between VFS unmount and CPU halt. A separate command-admission gate closes
before bounded acquisition of every port mutex. Already admitted commands can
finish; queued commands reject I/O after acquiring their port lock. The final
ATA flush executes while all readers are drained, followed by ordered engine
stop, checked BME disable, clearing ring registers and managed completion/
unmap/free. ST/FRE readback is checked along with CR/FR. Static block providers
remain safe offline objects, and repeated shutdown or initialization cannot
resurrect freed ring addresses.

The shared lifecycle algorithm tests release ordering, reader-drain failure,
missing engine/BME proof, flush failure, failed release, and an admitted/queued
command race. A failed drain retains backing without touching port state whose
mutex was not acquired. Unproven engine/BME retirement quarantines every owned
region. Flush failure still returns `-EIO` when safe DMA release succeeds.
The stop-failure diagnostic now records whether BME disable actually succeeded.
Common DMA accounting accepts a PCI-device filter and includes quarantined
ownership; it does not conflate another device's mappings with AHCI's.

KTEST has an explicit final linker section for destructive device retirement
tests, run after ordinary cases and before coverage/summary. The final AHCI case
performs a real direct disk read, syncs VFS, calls production shutdown, checks
physical-owner/page and per-device region accounting, verifies BME is clear,
then checks late read/flush callbacks, repeated shutdown and refused reprobe.
The manifest, runtime-log audit and semantic-oracle scanners recognize final
registrations as enabled tests. `--ahci-test-retire-only` selects this phase for
focused diagnostics; it is not a substitute for a full-suite run.

Validation:

- Normal `Build WOS` (`RelWithDebInfo`) and isolated diagnostic build (`Debug`)
  passed. Formatting/clang-tidy completed with advisory style warnings;
  this was not a warning-audit build.
- 69 host unit cases across ten executables passed, plus DMAR fuzz smoke and
  block-device source checks (12 selected CTest targets). Runtime-audit and
  shutdown source tests also passed.
- Three focused rootless Intel-IOMMU boots each passed 17 assertions and powered
  off normally. Success released regions `210->0` and pages `13068->0`.
  Withheld stop proof retained all 210 regions / 13,068 pages as quarantined.
  Injected flush failure reported failure while safely releasing all regions.
  All three confirmed BME disabled and rejected late I/O/reprobe. These flags
  exercise production failure branches; they do not emulate a real device
  hang, actual failed ATA flush, IOMMU fault or outstanding malicious DMA.
- Full rootless Intel-IOMMU KTEST passed **65,960 assertions, zero failures**.
  AHCI retirement followed the filesystem workloads at 443.969 seconds, with
  regions `210->0`, pages `13068->0`, zero quarantined regions and BME disabled.
  The VM powered off normally at 513.730 seconds. The runtime manifest audit
  confirmed all 1,164 enabled cases and the three declared disabled skips.
- Full and focused coverage fingerprints match the retained diagnostic ELF:
  `text_start=0xffffffff80000158`, `text_end=0xffffffff817d5106`,
  `text_fnv64=0x9bf200edf501bcd5`. SHA-256:
  `204470e79641445575f55459297662610e0e2d2dfe88ff48df295bb8d46cbf93`.
  The 58-file source manifest/archive matches current implementation and audit
  files. WOSDBG shared CLI analysis of all four logs succeeded. Every task-owned
  launcher exited; no VM was left running.

Two broader audit failures are explicitly unresolved baseline issues:
`ktest_manifest_test` rejects eight unchanged credential wrappers that delegate
their assertions to helpers, and `semantic_regression_test` finds an unreachable
scheduler source-test function. `audit-baseline.json` reproduces both with the
original HEAD checkers and verifies the affected source files are unchanged.
The new final-phase parser, registration, assertion, runner-order and semantic
oracle checks pass independently. These broader audit targets are **not**
reported as passing or bypassed. The old shutdown source check also expected
an exact `strcmp("--selftest")` path; it now checks the current token-aware
entrypoint and the VFS/AHCI/CPU shutdown order.

Receipts: `build/goal20-stage7/validation*.json`, `focused.log`, normal/isolated
build and host/audit logs, `audit-baseline.json`, `runtime-audit.log`, source
archive/manifest, saved ELF, serial logs and WOSDBG results. Fixtures remain
isolated under `ktest-data/goal20-stage7/`.

Remaining scope: passthrough provides no isolation; IOMMU domains/tables remain
kernel-lifetime objects. Normal power-finalizer ordering has build/source
coverage, while the real shutdown function was exercised from serialized BSP
KTEST. Simultaneous live SMP I/O teardown, physical hardware and real stalled
engines remain unqualified. AHCI recovery/reset error cases need further
qualification; other PCI drivers, translated/grouped domains, fault handling,
stale/unmapped hardware DMA and strict enforcement are still outstanding.
Goal 20 remains active.

### Stage 8d supplementary patch contract: abandoned scheduler stack shadow

Problem: live four-CPU Debug boot fails in DMA completion at a stack-local
Submission. The retained Stage 8c ELF reproduces the same site. A GDB hardware
watchpoint proves scheduler::process_tasks writes f1 at shadow 0xffffec0002160047;
the blocked-task enter_idle_loop path abandons its instrumented frame without
an epilogue, leaving that poison on the suspended worker's reusable stack.
Expected files: context_switch.cpp/.asm, a pure sanitizer stack-range helper,
host range/boundary tests and source integration checks, this evidence ledger.
Files read: scheduler process_tasks/enter_idle_loop and return-task ownership,
context-switch assembly return macros and C++ frame validation, task stack bounds,
KASan unpoison/runtime and CMake exclusions, diagnostic flag map and kernel /
critical-path instructions. Trace: build/goal20-stage8d/watch-shadow.log.
Invariants: preserve shadow for every live byte at/above the saved resume RSP;
only complete shadow granules below that RSP inside the selected task's stack
may be cleared. No change to saved registers, frames, task publication or FPU
ownership. Keep sanitizers enabled; do not change DMA copies or lifetime checks.
Locks/context: final kernel-return assembly, interrupts masked, on incoming
stack before handoff commit. No allocation, logging, waiting or added locks.
The cleanup helper itself must be uninstrumented and return through assembly.
Only the KASan profile pays the bounded stack-shadow clearing cost (up to 64KiB
per kernel resume with a 512KiB stack); normal builds omit the call entirely.
ABI: internal C/assembly helper only; no syscall/wire surface changes.
Build/test: pure invalid-range/live-boundary tests; verify both assembly return
macros invoke cleanup before commit; both build profiles; rootless Debug SMP
traffic and retirement, then full isolated KTEST. Preserve failure ELF/logs.
Rollback: revert this cleanup independently; sanitizer-enabled live evidence
then remains failing, without weakening the DMA retirement proof requirements.

Stage 8d shutdown enumeration contract addendum: the live test still shows the
newest holder/retirer never receive their per-task request. The current shutdown
loop walks numeric active-registry indices while earlier targets exit on other
CPUs; active_list_remove swaps the last task into an already visited index.
Change request_kernel_threads_shutdown to collect a bounded batch of retained,
unrequested targets and mark them under global_task_registry_lock, then wake
and release outside that lock. Restart each batch scan from index zero; cap
new requests at MAX_ACTIVE_TASKS. Already requested tasks were woken by the
request that marked them. Preserve zero-timeout flag-only behavior, caller
exclusion and cooperative-exit eligibility. No allocation or nested runqueue
locking under the registry lock. Test by the live retained holder/retirer flags
and protected yields, source checks, both builds and full KTEST.

### Stage 8d implementation and live qualification (2026-09-22)

Virtio callback admission and terminal retirement now hold an explicit scope on
the executing task that defers cooperative daemon exit. The scope captures its
owner, supports nesting, and releases callback admission before allowing exit.
It does not disable interrupts, scheduling or preemption. NAPI takes a retained
worker reference before posting the task and locks its registry lookup; worker
and NAPI storage remain kernel-lifetime objects, including after worker exit.
Replacing a retained worker is refused because snapshot reclamation is absent.

The four-CPU live diagnostic runs through normal initialization, real IRQs,
NAPI workers and concurrent TX, without the BSP `--selftest` mode. A separate
task holds a callback while retirement closes admission. The diagnostic verifies
unchanged DMA accounting during this hold, requests cooperative shutdown, proves
the owner yields with its actual per-task shutdown request set, then releases
the callback. It checks owner exit, completed retirement, detached packet owners,
IRQ drain, late-call rejection and stable repeated shutdown. It finishes through
the ordinary power finalizer, including VFS shutdown and AHCI retirement.

Live debugging exposed two prerequisite defects:

- The retained Stage 8c ELF reproduced the same KASan failure in DMA completion
  as the initial Stage 8d build. A GDB hardware watchpoint identified poison
  written by `scheduler::process_tasks`; the idle path abandoned that frame.
  Kernel resume now clears only whole shadow granules below the saved RSP in
  the incoming task's stack. Live-frame poison remains intact. Normal builds
  omit this KASan-only operation; diagnostic builds incur its bounded cost.
- Shutdown enumerated registry indices while waking tasks on other CPUs. Exit
  moves the last registry entry into the vacated index, so that walk could skip
  owners. Bounded batches now retain and mark targets under the registry lock,
  then wake/release outside it. No allocation or runqueue lock is added under
  the registry lock; caller exclusion and zero-timeout behavior are preserved.

All four final live boots passed with the same retained Debug ELF:

| Case | Host-verified echoes | Worker RX / IRQs | Regions / pages after retirement |
| --- | ---: | ---: | --- |
| Modern, Intel IOMMU | 128 | 123 / 224 | 514->0 / 2056->0 |
| Modern, reset proof withheld | 242 | 199 / 382 | 514 / 2056, all quarantined |
| Legacy, Intel IOMMU | 238 | 195 / 369 | 514->0 / 2056->0 |
| Modern, IOMMU absent | 128 | 56 / 114 | 514->0 / 2056->0 |

Each case verifies every byte of 64-byte and 9,014-byte echoed packets, actual
worker reception and reception on CPUs other than the TX CPU. Host echo counts
can exceed delivered replies when retirement begins. All cases reported the
held owner and retiring task marked for shutdown, successful protected yields,
owner exit and every retirement check passing. AHCI subsequently released all
210 regions / 13,068 pages, and every launcher exited zero after normal poweroff.
Withheld proof observes a successful reset but refuses to use it as release
proof; it exercises quarantine, not a real hardware hang or IOMMU fault.

Normal Build WOS (`RelWithDebInfo`) and isolated diagnostic (`Debug`, KASan,
KUBSan, KCFI and KCOV) builds passed. Formatting/clang-tidy exited zero with
warnings, not a warning-audit pass. Twenty selected CTest targets passed:
98 executed unit cases, four source integration checks and DMAR fuzz smoke.
The two broader inventory failures remain independently reproduced baseline
issues in `audit-baseline.json`; they are not counted as passing targets.

ELF SHA-256: `e821e2c2bda5b23c209b028e8c4d91628c8e5c42625c11dd9a9883295eb70f88`.
All four boot manifests match this retained ELF. WOSDBG shared CLI analysis of
the four logs succeeded. The source snapshot currently contains 84 files.
Receipts, complete logs, debugger traces and failure artifacts are retained in
`build/goal20-stage8d/`; rootless fixtures use `ktest-data/goal20-stage8d/`.

Qualification limits: these live cases use one queue pair and four CPUs, not
multiqueue or physical hardware. An earlier fixture placed the coordinator and
retirement loop on one CPU and exceeded the production five-second drain bound
while waiting for the deliberately held owner; backing was retained safely.
The passing fixture gives retirement a separate CPU. This does not qualify
scheduler fairness or successful bounded retirement under arbitrary starvation.
Initial fixture failures (zero-timeout request and counting multicast request
loopback as a reply) are archived separately from passing receipts.

Final full-suite checkpoint: the rootless Intel-IOMMU KTEST passed **65,977
assertions, zero failures**, with all 1,165 enabled cases and three declared
disabled skips confirmed by `runtime_test_audit.py`. Final retirement released
AHCI's 210 regions / 13,068 pages and virtio's 516 regions / 2,059 pages, with
zero quarantine. The full suite also verified two packet echoes, control-command
acknowledgement, IRQ drain, late-call rejection and repeated retirement. This is
the serialized BSP suite; the four separate live cases above supply SMP/NAPI
evidence. Normal poweroff occurred at 515.842 seconds and the launcher exited
zero. Its coverage fingerprint matches the retained ELF:
`text_start=0xffffffff80000158`, `text_end=0xffffffff817f3966`,
`text_fnv64=0xc2124f501f349a96`; its boot-manifest kernel hash matches too.
WOSDBG full-log analysis succeeded. Binary inspection confirms Normal Build WOS
omits the shadow helper and the Debug helper invokes unpoison without ASan
callbacks. All 84 source hashes still match. The process audit found only the
pre-existing unrelated QEMU PID 36201; every task-owned VM/debugger exited.
Final receipts: `build/goal20-stage8d/validation.json`, `validation-full.json`,
`runtime-audit.log`, `stack-resume-binary-check.json`, source archive/manifest
and WOSDBG outputs. This closes Stage 8d qualification, not Goal 20.

### Stage 9 reconnaissance: e1000/e1000e ownership conversion

Read entrypoints: `dev/e1000e/e1000e.cpp/.hpp`, `net/netdevice.hpp` and registry/
RX entrypoints, `net/netpoll.hpp` and the Stage 8d worker lifetime, `net/packet.hpp`,
`net/proto/ethernet.cpp`, common DMA and domain APIs, PCI BME/IRQ APIs,
`platform/interrupt/gates.cpp`, init metadata, power finalization and shared
QEMU NIC construction. Relevant instructions are the kernel, critical-path,
logging and subsystem-index files. No e1000 implementation changes yet.

Current call/data flow: phase-five init scans supported Intel PCI IDs, allocates
the embedded NetDevice/NAPI object, enables BME, maps BAR0, attempts reset,
allocates two physical descriptor rings, posts raw packet addresses, enables
RX/TX, publishes IRQ/netdevice/NAPI, then enables interrupt causes. IRQ acknowledges
and schedules NAPI; NAPI consumes RX and rearms interrupts. `start_xmit` owns TX
reclamation under its IRQ-save lock. RX hands each packet to `netdev_rx`, whose
protocol/WKI consumers may yield, then allocates a replacement. Every packet
and ring address still bypasses the common DMA API.

Concrete hazards for the conversion to address:

- BME precedes all ring/buffer setup; reset exhaustion is silently accepted.
- Partial RX initialization still posts the whole ring. RX replacement failure
  returns a descriptor with address zero to hardware. RX length is unchecked,
  and only successfully delivered packets consume the poll budget.
- TX reads a raw PacketBuffer address, narrows length and frees completed packets
  without managed synchronization. Its descriptor completion loads need explicit
  device-memory ordering; TX lock already serializes producers/reclamation.
- Vector, IRQ registration and netdevice publication failures lack complete
  unwind; the current vector failure deletes the device with engines active.
- No terminal shutdown path drains callbacks/IRQs or retires mappings. The
  published object must remain stable for raw NAPI/WKI snapshots, as in virtio.

Minimal strategy: prepare coherent rings and per-slot directional streaming
pools in task/probe context with engines/BME off; complete every persistent
mapping before admission. Publish only prepared DMA submissions. At completion,
validate DD/EOP/error/length, synchronize before packet delivery or free, and
rebind an existing pool before returning the descriptor. On refill exhaustion,
retain/reuse a valid CPU-owned packet instead of publishing a null address.
Bound work by descriptors examined, including drops. Keep existing MTU/wire
behavior and WKI hooks; no packet DMA allocation in IRQ/NAPI/TX paths.

Terminal retirement needs monotonic callback admission and shutdown holds,
unpublication, device-specific interrupt masking/reset and BME readback, IRQ
drain, then release or quarantine. Retain CPU owners on failed software drain;
after readers drain, uncertain hardware backing can be quarantined while CPU
packet borrows are detached. Exact reset/posted-write/No-Snoop rules must be
verified against supported device documentation before implementing release
proof; a cleared enable bit alone is insufficient. Existing IRQ APIs support
one owner per vector, so a conflicting legacy IRQ must fail probe safely.

Expected impact: e1000 driver/header plus a pure ownership/completion helper,
host and isolated KTEST cases, power shutdown integration and rootless fixtures.
No syscall or wire ABI changes. Lock order remains callback ownership before
TX lock before bounded DMA registry operations; no task waits with those locks
held. MMIO and device objects stay retained through IRQ drain and late calls.
Update the memory-accounting source check deliberately if probe helper names
change; it currently asserts RX reserve precedes initialization/replacement.

Build/test plan: Normal and isolated builds, pure boundary/completion/unwind
tests, source checks, rootless QEMU `e1000e` plus legacy `e1000` using the shared
NIC model option, IOMMU on/off, ordinary packet echoes, malformed completion/
allocation/refill failure injection and retirement/quarantine accounting.
The existing 2,048-byte RX buffer/1,500-byte MTU should be preserved; jumbo RX
support would be a separate protocol change. Physical I211/I217/I219 behavior
and actual stalled DMA require later hardware evidence. A patch contract is
still required before implementation. Goal 20 remains active: translated
domains/grouping, real faults, strict enforcement, e1000e/xHCI/ivshmem conversion
and remaining qualification have not been completed.

Device-documentation follow-up: Intel's 82574 datasheet, revision 2.5,
sections 3.1.3.10 and 10.2.1, specifies setting CTRL.GIO Master Disable (bit 2)
and polling STATUS.GIO Master Enable Status (bit 19) clear before disabling PCI
mastering; this drains pending requests. Reset clears the disable control, so
retirement ordering must preserve the stopped state. Sections 3.1.3.8.3 and
10.2.3.1 locate the six No-Snoop controls at GCR (0x5b00), bits 5:0; require them
clear for the current coherent backend. Section 4.6.1 requires interrupt masking
again after reset, and 4.6.2 requires setting GCR bit 22 during initialization.
These are 82574-specific facts, not qualification of the other supported IDs.
Source: [Intel 82574 datasheet, distributor-hosted copy](https://docs.rs-online.com/96e8/0900766b81384733.pdf).
The old Intel PDF URL redirects to a missing document; this mirror is an older
Intel-authored revision. Current errata and per-model sequences remain to be
checked before implementing or claiming hardware release proof.

### Stage 9 patch contract: managed e1000 DMA and terminal ownership

Problem: e1000 publishes raw ring/packet addresses, enables BME before mapping,
and lacks safe probe failure and terminal DMA retirement.
Expected files: e1000 driver/header and pure DMA helper; shared callback gate
extracted from virtio without behavior change; power finalizer; host/KTEST
tests and source integration checks; isolated fixtures and this ledger.
Files already read: Stage 9 reconnaissance entrypoints, PCI capability and BME
implementation, IRQ request/free ownership, virtio retirement and DMA adapters,
DMA manager, packet/netdevice/NAPI APIs, KTEST registration and build tasks.
Invariants: every programmed ring/packet address comes from a prepared common
DMA submission. Prepare all persistent mappings before BME. No mapping allocation
or release in IRQ/NAPI/TX. Completion DD plus ordering precedes sync/delivery/free.
Never repost an empty RX descriptor. Bound dropped-fragment work by the poll
budget. Preserve MTU, descriptor ABI, WKI hooks and consume-on-TX-return ownership.
Closed callbacks cannot reach MMIO or freed queues. Failed reader drain retains
all owners; uncertain hardware quiescence quarantines exposed backing. Device/
NAPI objects remain stable after publication. Shared gate extraction preserves
the existing virtio API through aliases.
Locks involved: callback gate/shutdown scope, TX IRQ-save lock, common DMA
registry lock; PCI/IRQ/netdevice registry locks only through their existing APIs.
Unsafe contexts: IRQ acknowledges/schedules only; NAPI/RX can yield into protocol
handlers, so a captured shutdown scope must survive. No retirement from callbacks.
Allocation/blocking: pool/ring allocations in probe; terminal waits bounded in
sleepable context; no waits with TX/registry locks held. Never infer quiescence
from a timeout. Model-specific proof is required; unsupported retirement proof
pins backing and reports failure rather than freeing on a guessed register.
ABI/wire/syscall compatibility: no external changes; hardware packed descriptor
layout and packet ownership conventions remain unchanged.
Build/test plan: pure buffer retirement/completion/bounds tests, failure-injected
probe KTEST, actual QEMU e1000e/e1000 packet echo and terminal accounting with
IOMMU on/off, both builds, source checks and full isolated KTEST.
Rollback: revert driver/helper/power integration together; never restore raw
addresses selectively or bypass proof checks. Retain diagnostic artifacts.

### Stage 9a: e1000/e1000e managed rings and packet DMA (2026-09-22)

The driver now allocates two coherent rings and 512 directional streaming pools
before enabling BME. Every ring-base and packet-descriptor address comes from a
prepared common DMA submission; the private physical-address conversion and
ordinary ring page allocations are gone. PCIe probe disables No Snoop and
relaxed ordering in Device Control, and 82574 probe additionally clears the six
GCR No-Snoop controls and sets its required initialization bit. The conventional
82540 path retains the ordinary snooped PCI contract. This still uses explicit
passthrough domains and does not establish hardware isolation.

RX reads DD with device-memory ordering, bounds completion length to 2,048 bytes,
and rejects errors and fragmented frames before copying. Every descriptor
examined consumes budget, including drops. DMA completion detaches the old CPU
borrow before packet delivery. A replacement is obtained and rebound before the
old packet is transferred to the network stack; if the packet pool is exhausted,
the frame is dropped and its valid packet/bounce slot is reused. No zero-address
RX descriptor is returned to hardware. TX validates its span inside PacketBuffer
storage, prepares a directional submission and publishes only after a fence.
DD completion precedes packet free. A descriptor is left unused so a full queue
cannot wrap TDT onto TDH and appear empty.

Callback admission is shared with virtio through `dev/dma/lifecycle.hpp`; the old
virtio header aliases the same implementation without changing its behavior.
e1000 RX/TX/poll/IRQ callbacks hold captured cooperative-shutdown scopes. Failed
DMA transitions close admission and stop new DMA without freeing backing in a
hot path. Terminal shutdown drains callbacks, unpublishes the netdevice, masks
interrupt sources, proves device quiescence where supported, disables BME, drains
owned IRQs, then releases or quarantines buffers. Never-published allocations can
unwind without hardware proof. CPU packets detach only after successful completion
or quarantine; busy quarantine retains the CPU owner. Published device/NAPI
objects and worker references remain stable for late raw snapshots. IRQ vector,
registration, NAPI worker and netdevice publication failures take this same
terminal path. Reopen/reprobe and repeated shutdown cannot resurrect mappings.
Normal power finalization invokes e1000 shutdown before halting other CPUs.

Release proof is currently implemented for **82574 only**: set GIO Master Disable,
poll GIO Master Enable Status clear, and keep the disable control asserted while
turning BME off. Do not reset afterward and reopen the master gate. Other listed
Intel models are converted to managed DMA but deliberately retain exposed backing
on retirement until their exact transaction-drain proof is qualified. Their
reset/BME observations alone are not accepted as proof. This is an explicit
remaining part of the goal, not a completed zero-mapping teardown guarantee for
the entire family. Primary references: [Intel 82574 datasheet, sections 3.1.3.10
and 10.2.3.1](https://docs.rs-online.com/96e8/0900766b81384733.pdf),
[Intel PCI/PCI-X software manual, section 14 reset behavior](https://www.intel.com/content/dam/doc/manual/pci-pci-x-family-gbe-controllers-software-dev-manual.pdf).
PCIe capability/Device Control masks were also checked against the installed
Linux UAPI `pci_regs.h` (EXP=0x10, DEVCTL=+8, RO=0x10, NoSnoop=0x800).

Eight new host cases cover RX/TX bounds, failed bind/prepare, never-submitted
unwind, completion ownership, monotonic exposure, quiesced release and failed
quarantine. Integration checks cover mapping-before-BME, synchronization and
callback guards, budgeted drops, and power-finalizer order. The existing memory
accounting check incorrectly counted all virtio RX-reserve calls in the file;
the earlier packet selftest adds a third call. It now checks the two production
probe bodies separately, preserving its original requirement. This is a stale
source-check assumption, not a new runtime allocator failure.

Normal Build WOS and isolated Debug builds passed. Formatting/clang-tidy exited
zero with warnings; no warning-audit claim is made. The selected host run passed
23 CTest targets: **106 unit cases**, six source checks and DMAR fuzz smoke.
The two broader inventory failures remain independently reproduced baseline
issues (`audit-baseline.json`); final-phase registration/assertion checks pass.

Five rootless focused boots passed using the same diagnostic ELF:

| Case | KTEST assertions | Verified echoes | Final regions / pages |
| --- | ---: | ---: | --- |
| 82574, Intel IOMMU | 32 | 300 | 514->0 / 1282->0 |
| 82574, withheld stop proof | 31 | 300 | 514 / 1282, all quarantined |
| 82574, fail after 17 allocations | 26 | 0 | 0 / 0, no published NIC |
| 82540 legacy e1000 | 31 | 300 | 514 / 1282, all quarantined |
| 82574, IOMMU absent | 32 | 300 | 514->0 / 1282->0 |

Each traffic case alternates 64-byte and 1,514-byte frames for 300 independently
host-verified echoes, exercising ring-index wrap. The kernel checks every reply
byte, late TX/open rejection, owned IRQ drain, disabled BME, stable repeated
shutdown and packet-pool recovery (`1024->1024`). All focused launchers exited
zero after normal selftest poweroff. Withheld proof observes the real 82574
handshake but refuses to use it for release; this is not a physical device hang.
The initial two-packet matrix is archived separately. Its first host verifier
incorrectly expected 1,026 backing pages; actual allocator size-class rounding
uses four pages per TX pool, for 1,282 total. The kernel assertions passed in
that run; the corrected final verifier matches actual accounting.

ELF SHA-256: `e68c57372636ee8f7f6ae36983f70c8bb67b3eb7040c02bf43481a66ef0113a2`.
All five boot manifests and KCOV fingerprints match that retained ELF. WOSDBG
shared CLI analysis of the five logs succeeded. The source archive/manifest
contains 92 files. Artifacts: `build/goal20-stage9a/`; private rootless fixtures:
`ktest-data/goal20-stage9a/`. Full isolated Intel-IOMMU KTEST subsequently
passed **65,975 assertions, zero failures**. The runtime audit accounts for all
**1,166 enabled cases** and three disabled skips. Its packaged kernel and KCOV
fingerprint match the retained ELF, WOSDBG full-log analysis succeeded, and the
launcher exited zero after selftest poweroff.

Remaining qualification: focused packet tests invoke the production poll path
on serialized BSP with IRQ registration published but device causes masked;
they do not qualify concurrent e1000 IRQ/NAPI/SMP teardown. Actual refill pressure,
malformed device completions, publication failure injection, non-82574 drain proof
and hardware/errata coverage remain. No new jumbo-frame support is claimed.
Goal 20 remains active: e1000 qualification, xHCI/ivshmem conversion, translated
groups/domains, fault diagnostics, stale/unmapped DMA tests and strict enforcement
are still required.

### Stage 9b reset timing patch contract

Problem: Intel 8254x CTRL.RST requires a delay before the first subsequent
register access; stage 9a immediately polls CTRL.
Expected files to change: e1000e.cpp, e1000_dma_source_test.py, this ledger.
Files already read: kernel/critical-path/logging/build/subsystem instructions;
e1000e reset, probe and retirement; ktime and HPET implementation; netdevice,
netpoll and Ethernet entrypoints; kernel CMake; Intel 8254x section 13.4.1.
Invariants to preserve: mappings before BME; completion before CPU ownership;
no release without quiescence and IRQ drain; non-82574 quarantine unchanged.
Locks involved: none around reset; callback admission is closed/drained during
terminal retirement. Unsafe contexts: reset is probe/retirement only, never RX,
TX, IRQ or NAPI. Allocation/blocking assumptions: no allocation or scheduler
blocking; use the existing HPET spin-delay primitive (HPET must be running, as
required by boot timing); device-ready polling retains its existing bounds.
ABI/wire/syscall compatibility impact: none.
Build/test plan: retain stage 9a evidence; both builds; formatting; integration
check; focused rootless legacy/modern boots with a separately retained ELF.
Rollback plan: revert only the added delay and its source integration assertion.

Use two microseconds through the existing rounded-up HPET delay to cover the
required approximate one-microsecond minimum despite counter phase. This is a
reset ordering correction, not physical hardware or additional model drain
qualification. Reference: [Intel 8254x manual, CTRL.RST description, section
13.4.1](https://www.intel.com/content/dam/doc/manual/pci-pci-x-family-gbe-controllers-software-dev-manual.pdf).

Stage 9b verification passed: Normal Build WOS, isolated Debug KTEST build,
formatting, eight e1000 host cases and its source integration check. An initial
build-only invocation incorrectly included launch-only `--no-setup`; the CLI
rejected it before building. Retrying `bin/wos-ktest --build-only` succeeded.
The modern and legacy focused rootless boots each verified 300 echoes and normal
selftest poweroff (32 and 31 assertions respectively). Release versus quarantine
accounting remains as in stage 9a. Both boot manifests and KCOV fingerprints
match the new retained ELF; both WOSDBG logs passed analysis.

ELF SHA-256: `e0dbbcbb6005c56c89cf95691bd435e3936f11fa58f9f5e1f9bda77eb7f06cab`. Artifacts: `build/goal20-stage9b/`;
92-file source manifest differs from stage 9a only in e1000e.cpp and its source
integration check. Full-suite stage 9a evidence is preserved under its original
ELF; the timing-only correction was verified with the two focused model boots.

Next qualification step: adapt the existing virtio live four-CPU harness to
e1000 using ordinary boot, real IRQ/NAPI workers, concurrent TX, a held callback
during terminal retirement and cooperative daemon shutdown. Read both harnesses
and the production poll/IRQ entrypoints before editing. Keep separate private
QMP/serial/boot fixtures and exercise 82574 normal/withheld, legacy quarantine
and IOMMU-absent modes. No new SMP or hardware claim is made by stages 9a/9b.

All stage 9a/9b task-owned VMs exited. The final process audit shows only the
pre-existing unrelated QEMU PID 36201, which was not touched.

### Stage 9c live e1000 qualification patch contract

Previous goal turn: verified progress (stage 9a full suite and stage 9b reset
timing correction). Goal remains active; passthrough has no DMA isolation.
Problem: serialized BSP e1000 tests do not exercise actual IRQ/NAPI callbacks
or concurrent ownership during terminal DMA retirement.
Expected files to change: e1000e.cpp diagnostic hooks/harness, ledger, goal
checkpoint; private ignored build/ktest fixtures.
Files already read: kernel/critical-path/logging/subsystem/build instructions;
e1000 probe, RX/TX, poll/IRQ, retirement and tests; common lifecycle; virtio live
harness; netdevice/netpoll and Ethernet entrypoints; power finalizer; stage 8d
rootless runner and node spec.
Invariants to preserve: all live hooks compile only under WOS_SELFTEST and are
activated by an explicit token; callbacks never sleep or allocate for the test;
DMA release still requires closed/drained callbacks, device proof and IRQ drain;
CPU packet borrows detach before freeing; missing proof quarantines backing.
Locks involved: existing TX IRQ-save lock and NAPI CAS; test counters atomic;
retirement waits outside locks. Unsafe contexts: test RX/IRQ hooks may run in
NAPI/IRQ, and only validate/account/free completed packets as the existing
selftest does. Allocation/blocking assumptions: daemon coordinator/sender/held
owner/retirer use ordinary task APIs and bounded waits; hold intentionally
survives cooperative shutdown yields. Published test task references retained.
ABI/wire/syscall compatibility impact: none; diagnostic Ethernet marker only.
Build/test plan: both builds, formatting and focused host integration; four
rootless four-CPU boots (82574, withheld proof, legacy quarantine, IOMMU absent),
at least 160 echoes of each 64/1514 size through real worker/IRQ paths, concurrent
retirement and ordinary finalizer/AHCI teardown; inspect saved logs via WOSDBG.
Rollback plan: remove only new WOS_SELFTEST live hooks/harness; preserve production
DMA conversion and previous retained evidence. Hardware qualification and other
model drain proofs remain outstanding; this fixture tests one NIC/queue.

Stage 9c implementation adds WOS_SELFTEST-only hooks and a token-activated live
harness in `dev/e1000e/e1000e.cpp`. Production teardown behavior is unchanged.
Normal ELF symbol inspection confirms the e1000 live harness is absent. RX
validates every byte of diagnostic replies; IRQ counting occurs only after a
nonzero device cause. At least 160 replies of each 64/1514 size are required,
exercising ring wrap. Worker-task identity and CPU counters distinguish real
NAPI worker processing from rescue/inline polling. TX runs on CPU 1; observed
RX CPU masks are 0xa, which includes another CPU. A retained task on CPU 2
holds callback admission while CPU 3 retires and CPU 0 coordinates. The test
requests cooperative daemon shutdown, verifies the held owner survives yields,
releases it, and verifies exit after its scope. No storage reuse occurs while
the held reader prevents retirement.

Normal Build WOS and isolated Debug KTEST build passed. Formatting/clang-tidy
passed with warnings; 23 selected CTest targets passed (106 unit cases, six
source checks and DMAR fuzz smoke). The four rootless four-CPU live boots pass:

| Case | Host-verified outgoing frames | Verified RX 64 / 1514 | Worker RX / IRQ | DMA regions after |
| --- | ---: | ---: | ---: | --- |
| 82574 + Intel IOMMU | 328 | 164 / 163 | 281 / 422 | 0 |
| 82574 withheld stop proof | 333 | 166 / 166 | 295 / 448 | 514 quarantined |
| 82540 legacy | 325 | 162 / 162 | 226 / 431 | 514 quarantined |
| 82574 IOMMU absent | 332 | 166 / 166 | 301 / 443 | 0 |

All four verify held-reader waiting, shutdown deferral and subsequent owner
exit, detached CPU buffers, drained IRQ ownership, disabled BME, late TX/open
rejection, stable repeated retirement and exact 1,282-page release/quarantine
accounting. Some replies are intentionally still in flight when retirement
closes admission, so RX totals need not equal outgoing totals. All reach the
ordinary power finalizer, VFS shutdown and AHCI 210->0 mapping release. Each
launcher exited zero. Withheld proof rejects an observed real model handshake;
it does not simulate a physical hang. Legacy quarantine remains deliberate.

All boot manifest kernels match retained ELF SHA-256
`df86c8e614abdca7018fb48e4d72691ee1a0d03469ea6c92aa9d5d298328b36f`. The 92-file source
manifest matches the workspace, and WOSDBG shared CLI analysis of all four logs
passed. Ordinary live boots do not emit the full KTEST coverage fingerprint;
boot-manifest matching is the artifact check here. Full KTEST was not repeated
for this diagnostic-only harness; stage 9a retains its verified full-suite ELF.
Artifacts: `build/goal20-stage9c/`, `ktest-data/goal20-stage9c/`. All task VMs
exited; only unrelated QEMU PID 36201 remains and was not touched.

Limitations: one NIC and one queue on four CPUs, no physical hardware or
non-82574 drain proof. Refill exhaustion, malformed hardware completions and
publication-failure injection remain e1000 qualification gaps. Stale broader
inventory checks remain documented baseline issues; none were changed here.

### Next driver reconnaissance: xHCI (no xHCI edits yet)

Local files inspected: `dev/usb/xhci.cpp`, `xhci.hpp`, event/request and lifecycle
headers located, CDC quiesce/detach entrypoints located, existing USB hotplug
stress script and xHCI/CDC KTEST/source tests located. This is an initial map;
read the remaining full entrypoint bodies and tests before implementation.

- `init_controller` currently enables BME before MMIO halt/reset, then allocates
  raw DCBAA, scratchpad array/pages, command/event rings and ERST. Convert these
  coherent allocations to persistent managed handles; enable BME only after
  preparation/programming. Honor HCCPARAMS1 AC64 and existing alignment rules.
- `enumerate_device` allocates device/input contexts and EP0 ring;
  `xhci_configure_bulk_endpoints` allocates bulk rings. Their addresses also
  serve as request TRB identities. Preserve correlation using device addresses.
- `xhci_control_transfer` / `xhci_bulk_transfer` allocate directional bounce
  storage, reserve requests under endpoint ring lock, publish TRBs and ring
  doorbells. IRQ `process_events` completes fixed request records; task waiters
  copy completed input and release storage. Timeout keeps endpoint bounce state
  for recovery. Managed ownership must never retain a dead caller buffer after
  timeout; investigate the API's bounce-detach/quarantine semantics first.
- `teardown_device` closes endpoint admission, quiesces CDC, cancels requests,
  stops/drops endpoints, and requires successful Disable Slot before detach
  and freeing rings/contexts. Preserve this slot ownership barrier independently
  from whole-controller halt. CDC/WKI readers must finish before CPU storage
  reuse; no new wait or allocator may enter IRQ.
- `cleanup_controller` frees raw controller resources on probe unwind; after
  an attempted RUN it clears RUN/INTE and drains IRQ without a separate bounded
  HCH proof there. Managed conversion must require that proof or quarantine.
  Published controllers currently have no terminal shutdown API; add one with
  worker/callback ownership and power-finalizer integration after call-flow
  review. Do not free controller objects still reachable by registry snapshots.

ABI/wire surfaces: preserve packed USB descriptors, TRB cycles/links, DCI/context
layout, W1C/ERDP rules and CDC public transfer return conventions; no WKI change.
Build/test plan: existing request/lifecycle host and KTEST cases, managed failure
unwind tests, both builds, rootless QMP USB-network hotplug traffic fixtures and
ordinary terminal shutdown. Real hardware still needs separate qualification.
Goal 20 is active: remaining driver conversion, translated grouped domains,
actual stale/unmapped DMA faults, diagnostics and strict enforcement are absent.

### Stage 10a controller DMA patch contract

Problem: xHCI controller rings/DCBAA/scratchpads use raw physical addresses and
BME is enabled before ownership setup. Probe unwind can free after attempted
RUN without checking HCHalted.
Expected files: xhci.cpp/hpp, xhci_dma.hpp, host model/source tests and CMake,
focused KTEST controller-probe entrypoint, ledger/goal checkpoint.
Files read: preceding xHCI reconnaissance entrypoints, DMA API/manager,
xhci_request/lifecycle tests and source contracts, CDC quiesce/detach, USB
hotplug script, kernel/subsystem/critical-path/build/logging instructions.
Invariants: preserve TRB identities/cycles, event IRQ boundedness, request and
slot retirement, USB descriptors and CDC/WKI ownership. Unproven hardware/IRQ
retirement pins controller DMA and controller metadata. Raw slot/transfer
allocations remain for the next phase, with mask checks preserved/added.
Locks: no DMA allocate/release under ring/IRQ locks; probe owns unpublished
controller; free_irq drains completion readers before buffer release.
Unsafe contexts: existing event IRQ never allocates, waits or frees DMA.
Allocation/blocking: boot/task fallible allocations, bounded halt polling; no
scheduler/VFS/USB callbacks in probe cleanup.
ABI/wire impact: none. AC64 selects address mask/access width; 4KiB page support
checked; PCIe No Snoop/Relaxed Ordering disabled before coherent use.
Build/test: pure production-helper failure tests, existing USB source contracts,
both builds, focused actual controller start/stop/failure/withheld-halt boots,
then rootless live USB attach/detach regression.
Rollback: revert controller-only conversion; never restore free-on-unproven-halt.
Published-controller terminal shutdown and slot/transfer managed conversion
remain later phases, not claimed by this checkpoint.

Primary reference: [Intel xHCI 1.2b, sections 5.3.6, 5.4.1 and 4.18](https://cdrdv2-public.intel.com/625472/625472_xHCI_Rev1_2b.pdf):
AC64 selects pointer width; HCHalted indicates pending pipelined transactions
finished; No Snoop is controlled through PCIe Device Control.

Stage 10a followup contract: local HCSPARAMS2 scratchpad decoding reverses the
Hi (25:21) and Lo (31:27) fields. Intel xHCI 1.2b table 5-11 confirms this.
Change only a pure decoder in xhci_dma.hpp, its production call and an exhaustive
1,024-value host case. No lock/context/ABI change; allocation bounds remain
1,024 slots. Preserve current full-suite ELF/manifest; validate the correction
with both builds, host enumeration of every count and final live fixtures.
Rollback is limited to this decoder/call/test; do not silently retain the known
reversed-field behavior. QEMU zero-scratchpad boots do not qualify nonzero
scratchpad allocation on hardware. Followup artifacts: build/goal20-stage10a-fix.

Stage 10a implementation now uses explicit coherent allocation/submission
records for DCBAA, command/event rings, ERST and scratchpad array/pages. Probe
keeps BME off until mappings and IRQ setup exist, marks
exposure conservatively before attempted BME enable, then sets RUN. Cleanup
checks HCHalted, masks interrupts, disables BME and drains owned IRQs before
release. Missing proof pins exposed regions and controller/request metadata.
Unpublished prepared regions can unwind without a hardware retirement claim.

The existing slot/transfer allocation helper now checks the whole DMA extent
against AC64's mask; MMIO address writes select AC64 width. PCIe No Snoop and
Relaxed Ordering are disabled/read back, and 4KiB controller page support is
required. Failed shared-vector registration no longer frees an unowned vector.

Seven production-helper host cases cover empty/unpublished/exposed retirement,
repeated release and preparation/completion/invalidation failure. Integration
checks verify mapping-before-BME/RUN and halt/IRQ-before-release. Both builds and
formatting passed. The initial host run passed 26 targets/119 unit cases. Four
rootless focused boots passed: normal and IOMMU absent each 39 assertions,
withheld halt 40, allocation failure 37; zero failures. Normal controller DMA
released 4->0 regions/pages, withheld proof retained/quarantined all four, and
allocation failure after two regions unwound to zero. Each also verified 300
e1000 echoes. Boot manifests/KCOV fingerprints match the initial retained ELF;
WOSDBG focused analysis passed. Full suite still running at this checkpoint.

The scratchpad decoder followup adds an eighth helper case covering all 1,024
counts and unrelated HCSPARAMS2 bits. Its two builds and three focused host
targets passed. The old reversed decoder cannot be qualified by QEMU's zero
scratchpads; the fixed ELF and source archive are kept separately.

A parallel live-USB launch was rejected by WOS's global launcher guard while
the full KTEST launcher was active; no second VM started. The host runner also
attempted to save a nonexistent serial file. Its failure cleanup now tolerates
that case, and the USB launch will run sequentially after full-suite completion.
Neither launch failure was kernel runtime evidence.

Live followup found a real staging bug in stage 10a: after current-kernel boot,
QMP observes USBCMD=5 (RUN/INTE) and USBSTS=0x1000 (HCE). ERSTBA was programmed
while PCI BME was off; QEMU reads the event-segment table on that write. Thus
the old focused/full probe checks were insufficient: HCH clear did not imply
a working event engine. Retain those runs as historical test evidence, not
proof of successful hotplug. Stale serial reuse additionally made retry snapshots
observe firmware state; runner now clears its own previous log before launch.
A GDB attachment succeeded but its type expression failed before inspection;
no successful GDB state analysis is claimed. QMP supplied the decisive evidence.

Patch contract adjustment: keep allocation/preparation and IRQ registration
before BME, then enable BME before publishing DCBAAP/CRCR/ERST addresses, since
register writes themselves may initiate DMA even with RUN clear. Every address
is already mapped. Preserve the exposure/cleanup proof. Reject HCE/HSE in the
start check and strengthen source ordering checks. This changes probe only;
no lock, ABI, or IRQ-context allocation change. Rebuild both profiles; rerun
focused fault matrix, live hotplug and full KTEST under a new retained ELF.
Artifacts: build/goal20-stage10a-final.

Stage 10a final evidence (supersedes the earlier startup checks): both builds,
formatting and 26 host targets/120 unit cases passed. Final rootless matrix:
normal 39, withheld halt 40, allocation failure 37, IOMMU-off 39 assertions;
full KTEST 65,982 assertions, zero failures, 1,167 enabled/3 disabled cases.
Each boot verifies the retained ELF against boot manifest and KCOV fingerprint.
WOSDBG analysis of all six logs passed. Final ELF SHA-256:
cfa353cd1631922e7823e778958054c4853a192d3b6a7d19148269f6a569be98.
The 102-file source archive and all receipts are in build/goal20-stage10a-final.

Live ordinary boot completed 32 USB-net attach/configure/detach cycles without
controller error, panic or sanitizer report. The host post-run predicate falsely
matched two info-level KASan startup lines after all cycles; the saved log and
manifest passed a corrected validator excluding only those exact startup lines.
This fixture ends with private QMP quit; it is not terminal-controller shutdown
or USB packet-throughput evidence. All task VMs exited. Source-inventory checks
still reproduce the documented, unchanged credential/scheduler baseline issues.
The QEMU primary source confirms ERST table fetch on programming and HCE on
failed DMA/invalid segment: https://raw.githubusercontent.com/qemu/qemu/master/hw/usb/hcd-xhci.c
(xhci_er_reset). Translated isolation, slot/transfer DMA and published-controller
terminal shutdown remain incomplete.

### Stage 10b slot DMA and retirement patch contract

Problem: device/input contexts and endpoint rings still use raw DMA addresses;
slot teardown ignores Stop Endpoint / Configure Endpoint failures. An admitted
caller can also publish after teardown closes admission unless both operations
serialize on its endpoint ring lock.
Expected files: xhci.cpp/hpp, xhci_dma.hpp, host helper/source tests, this ledger
and goal checkpoint. No CDC or WKI protocol rewrite.
Files read: subsystem/kernel/critical/logging instructions, xhci.cpp/hpp,
xhci_dma/request/lifecycle helpers and tests, common DMA manager/API, CDC
quiesce/detach/callers and fixed request completion path. Module build entrypoint
and shared IRQ reader drain were read in preceding stages.
Invariants: every context/ring address comes from a prepared coherent mapping;
exposure before DCBAA/command publication; no mapping release before successful
slot stop/drop/Disable Slot and class-reader drain. Failed proofs retain state
and quarantine exposed coherent mappings. Transfer bounce conversion follows
separately; no false full-driver-conversion claim.
Locks: ring lock serializes final transfer admission/publication and admission
closure. Fixed request atomic state transitions publish completion; commands serialize
on command_busy. No DMA allocation/release or wait while ring locks are held.
Unsafe contexts: hard IRQ continues bounded event publication only.
Allocation/blocking: managed fallible coherent allocations in task context;
existing bounded command wait and CDC reader drain. Idle endpoint Stop may be
skipped only when output state is Disabled/Stopped; Running requires successful
Stop completion, whose prior transfer events share the single event ring.
ABI/wire/syscall impact: none; retain context stride, DCI, TRB layout/cycles and
USB/CDC return conventions. No external pointer outlives existing CDC pins.
Build/test: production helper tests, source ordering, both builds, 32-cycle live
USB regression with post-detach DMA accounting, allocation-failure/withheld-stop
fixtures, focused probe regression. Full suite only if new failure justifies it.
Rollback: revert slot conversion as a unit while retaining fail-closed retirement
and admission synchronization. Never restore free after an unproven stop.
Primary reference: Intel xHCI 1.2b 4.6.4 (Disable Slot preconditions) and 4.6.9
(Stop Endpoint and ordered transfer/completion events).

Stage 10b review adjustment: FixedRequestTable::snapshot can return false while
IRQ completion or cancellation publishes fields. wait_for_request previously
returned -EIO with a nonterminal/default snapshot, and callers could release
still-visible storage. Retry a transient unavailable snapshot; classify an
actually stale handle as TIMED_OUT/unproven so callers retain DMA. After timeout,
require a published snapshot instead of ignoring its success flag. Non-cancelled
unproven transfer returns request slot recovery. This changes task waiting only;
no IRQ/API/layout change. Add source guards alongside existing completion/timeout
semantic tests, rebuild both profiles and rerun live/focused fixtures under a
new retained ELF; earlier stage10b live receipts are historical.
The wait loop is shared as xhci_request::await_result with injected clock/yield
callbacks, so host tests can exercise delayed completion/timeout snapshots and
stale handles using the real fixed request table. Additional changed files are
xhci_request.hpp and its existing host test; no request record/USB ABI changes.

Stage 10b SMP validation addition: a WOS_SELFTEST-only flag pauses a bulk-IN
caller after bounce preparation but before its ring-lock publication. Live QMP
unplug closes endpoint admission while the callback is held; it resumes only
when closure is observed. The real submit_transfer check must reject it, CDC
detach must drain it, and coherent slot accounting must return to four controller
regions. No production branch/layout changes or new wait in IRQ. Preserve the
pre-injection ELF/receipts, then rerun the final fixtures with this evidence.

### Next xHCI phase: transfer DMA reconnaissance

Remaining raw-DMA entrypoints are alloc_pages/virt_to_phys, control/bulk bounce
allocation and their normal/never-submitted/post-Disable-Slot page_free paths.
Controller metadata is CPU-only and may continue using page_alloc/page_free.
Use allocate_streaming with Direction::to_device/from_device and bind its own
stable CPU view. Never bind the caller's buffer: timeout returns while DMA may
continue, so a caller stack or PacketBuffer cannot be borrowed until retirement.
Prepare before ring publication, complete before reading received bytes, and
release outside endpoint/IRQ locks. On timeout preserve the submission and its
owned view in endpoint metadata until slot retirement. Allocation, completion,
and invalidation failures must retain any nonempty ownership handle.

Current task flow: request_busy pins one endpoint request; ring_lock protects
final acceptance/reservation/TRB/doorbell; IRQ only publishes request results;
caller copies after completed DMA; CDC cancellation returns while retaining
hardware storage; teardown stops/drop/disables then cdc_detach drains I/O before
retiring request storage. Failed teardown cannot blindly mutate a streaming
record while its CPU caller is finishing allocation/completion. Keep the record
owned or add an explicit caller-drain handoff; no stale borrowed caller memory.

The existing MAX_TRANSFER_LENGTH is 0x1ffff, but one Data/Normal TRB currently
covers that entire allocation. Intel xHCI 1.2b section 6.4.1 explicitly requires
transfer buffers not to cross 64KiB boundaries. Current CDC/descriptor tests use
small page-contained payloads; they do not qualify the generic larger-transfer
API. The streaming conversion must split at DMA-address boundaries, preserve
TD chain/short-packet/completion semantics and expand fixed request identities
if needed (control setup + up to three data segments + status can exceed the
current four identities). Test boundary/length/residual behavior against the
production planner, then actual byte-checked USB bulk traffic. Do not narrow
the public transfer limit simply to avoid segmentation.

Published-controller terminal halt remains a later required step: worker and
class/callback admission must close and drain before controller rings, IRQ and
registry ownership retire. Passthrough domains still do not enforce isolation;
ivshmem conversion, translated groups/domains, actual faults and strict mode
remain required by the original goal.

### Stage 10b verified checkpoint

Changed xhci.cpp/hpp, xhci_dma.hpp, xhci_request.hpp, both xHCI host unit
files and lifecycle source guards. Device/input contexts and endpoint rings
now use owned coherent mappings; exposure precedes hardware publication.
Retirement checks endpoint state, Stop/Configure/Disable results and drains
CDC callers before release; failed proof quarantines slot coherent storage.
Invalidation failure retains metadata and prevents later Enable Slot from
overwriting it. Ring-lock closure rejects already-admitted late publishers.
The shared task waiter now retries incomplete snapshots and never converts an
invalid/stale record into a completed-DMA claim. Four host cases exercise that
waiter with the real request table plus controlled snapshot deferral.

Normal Build WOS, isolated diagnostic build, formatting, 26 host targets/126
unit cases and final integration guards passed. Final rootless live fixtures:
- 32 USB-net attach/configure/detach cycles; all five slot coherent regions
  released after every detach, leaving only four controller regions/pages.
- Failure after two slot allocations: rollback returns to four controller
  regions/pages without class publication.
- Withheld endpoint-stop proof: all five slot regions quarantined; nine total
  regions/pages remain, and neither CDC detach nor slot release is reported.
- Four-CPU paused bulk-IN callback: unplug closes admission, resumed submission
  is rejected, CDC readers drain, and slot mappings return to baseline.
- Focused IOMMU-on/off KTEST: 39 assertions each, zero failures, normal poweroff,
  300 verified e1000 echo frames each and controller-probe DMA release to zero.

All six boot manifests match final ELF
ed40dca0425c539c66f86e6ae90a24fe361342ac1c907541d64de33e820c40af;
focused KTEST KCOV fingerprints match too. WOSDBG log loading/search passed all
12 calls. The retained 102-file source archive matches current code. Evidence:
build/goal20-stage10b/validation.json, per-case JSON/logs, runner/config files,
ELF and archive. Earlier stage10b receipts are kept in before-waiter and
before-held-submit with their own ELFs. Ordinary live fixtures end with private
QMP quit, not terminal xHCI shutdown. All task VMs/launchers have exited.

No full KTEST rerun was needed for this slot checkpoint: live USB fixtures
exercise paths that the full selftest probe does not, and affected host/helper
and focused probe checks pass. Stage10a's 65,982-assertion full run remains tied
to its older ELF and is not claimed for Stage10b. The current source-inventory
credential/scheduler baseline issues remain documented, not suppressed.
Transfer bounce DMA remains raw/unconverted, payload-throughput qualification
is still pending, and IOMMU domains remain passthrough. Goal 20 remains active.

### Stage 10c streaming DMA patch contract

Problem: control/bulk bounce allocations still bypass the common DMA API and
one TRB may cross a 64KiB boundary. Intermediate short-packet events currently
complete a whole request, which cannot prove retirement after segmentation.
Expected files: xhci.cpp/hpp, xhci_dma.hpp, xhci_request.hpp, existing DMA/request
host tests and source guards, ledger/goal. Files read: preceding full xHCI/CDC
call-flow, DMA API/manager, fixed request reserve/complete/cancel/snapshot,
slot cleanup, physical allocator and IRQ registry; subsystem/kernel/critical/
logging/build instructions remain applicable. Primary source: Intel xHCI 1.2b
4.10.1.1.2, 4.11.2.1, 6.4.1; QEMU hcd-xhci.c xhci_xfer_report/control parsing.
Invariants: streaming allocation binds only its OWN CPU view, never a caller
stack/PacketBuffer retained across timeout; prepare precedes publication,
completion precedes input copy, unmap precedes physical reuse. Hardware errors,
malformed completion or timeout retain ownership until a slot/controller proof.
Segment only at DMA-address 64KiB boundaries, preserve the existing maximum
length, and bound request identities/storage. Intermediate control data-short
events save length but do not complete before Status. Bulk uses a final Event
Data TRB with a monotonically unique controller cookie and total transferred
length. Errors still match every submitted TRB so they cannot strand waiters.
Locks: request_busy serializes endpoint CPU ownership; ring_lock final admission
and publication; atomic request transitions serialize IRQ result publication.
Cancellation must retry an intermediate publication. Never allocate/release or
wait inside IRQ/ring locks. Registry state remains retained through class drain.
Unsafe contexts: IRQ adds only bounded scalar result publication. No DMA APIs
or caller copy in IRQ. Task cleanup runs after CDC readers drain; failed teardown
must not race a still-running caller's streaming allocation/completion fields.
Allocation/blocking: fallible managed task allocation, same request deadlines,
fixed-size segment/identity arrays. ABI/wire/syscall: no public layout change;
internal TRB chaining and completion accounting follow the existing USB protocol.
Build/test: production streaming/planner/completion tests including errors,
boundaries, short packets and cancellation; both builds, existing live hotplug/
held-publication/failure/quarantine fixtures, then byte-checked bulk traffic.
Rollback: revert streaming/planner/event integration together, never restore
intermediate-event DMA release or borrowed caller memory across timeout.

Stage 10c review detail: widen only the internal UsbDevice::max_packet0 to
uint16_t; its previous uint8_t truncated SuperSpeed's 512-byte EP0 packet size
before context setup/segmentation. USB descriptor wire fields are unchanged.
Ring Link TRBs now preserve Chain across a wrapping segmented TD.

Stage 10c live byte-check extension: WOS_SELFTEST-only --xhci-test-packets
selects 9000-byte MTU for the QEMU CDC fixture and echoes strictly patterned
received frames through the same public bulk-OUT API after the IN mapping has
completed/released. No production CDC behavior changes. Add cdc_ether.cpp to
expected files (quiesce/detach/RX call flow read earlier). Host socket-multicast
checks exact round-trip bytes and the kernel logs RX/TX segment counts. Use
65/1514/8193/9014-byte frames: QEMU ECM flushes on a short USB packet; exact
packet-size-multiple/ZLP behavior needs separate qualification and is not
silently claimed by this fixture. No test hook runs in IRQ.

QEMU source qualification corrects the live payload plan: USBNetState has only
2048-byte input/output arrays, despite the kernel's larger packet pool. The
fixture therefore uses 65/1514/1801/2015-byte frames and a 9018-byte RX request,
which can exercise a short receive across a multi-TRB descriptor. This does not
qualify full 64KiB/128KiB payload transfers; the exhaustive planner covers those
layouts, and a larger USB device/hardware fixture is still required. No oversized
QEMU fixture is counted as a pass. Source: QEMU hw/usb/dev-network.c USBNetState.

The first USB bulk run verified all 64 host echoes byte-for-byte and returned
slot DMA to baseline. Its post-run check failed because the diagnostic hook
classified multicast reflections (reply marker 2) as invalid new requests
(marker 1). This is a diagnostic classification issue; retain those receipts
under reflected-replies and ignore reflected replies in the hook. Repeat under
a final ELF and vary the VM CPU allocation layout to seek a real multi-segment
RX descriptor; never count a one-segment run as segmented-device evidence.

### Stage 10c validated streaming checkpoint

All xHCI hardware buffer addresses now originate from managed DMA. Streaming
buffers bind their own CPU storage; timeout/error cannot retain caller memory.
TRBs split at 64KiB boundaries, Link TRBs preserve chaining, control requests
wait for Status after intermediate short events, and bulk requests wait for a
unique terminal Event Data cookie. Unproven completion retains ownership.

Normal Build WOS, isolated diagnostic build, formatting, 26 host targets/135
unit cases and source guards passed. Seven final rootless live boots passed:
32 attach/detach cycles, held-publication cancellation on four CPUs, partial
allocation unwind, withheld stop proof, byte-checked bulk traffic, and focused
IOMMU-on/off KTEST (39 assertions each, zero failed, 300 e1000 echoes each).
All 64 USB echoes match exactly at 65/1514/1801/2015 bytes. Actual RX/TX plans
were one segment; larger physical multi-TRB payloads remain unqualified.
Withheld stop retains ten regions/pages: four controller, five quarantined
slot coherent regions, and one device-owned streaming region. Successful
detach returns to the four-region controller baseline.

All seven manifests match ELF
708a68e4dcad9d5840d8de2af04c3a714457be5e0ff5315977374d1110fcdc2b;
the two focused coverage fingerprints match. The 102-file source archive was
verified against the working tree, and all 14 WOSDBG load/search calls passed.
Receipts: build/goal20-stage10c/validation.json and per-case artifacts. All
task-owned VMs exited. USB fixtures use QMP quit, not controller shutdown.
No new full-suite result is claimed; Stage10a's full run retains its older ELF.
Terminal controller retirement, ivshmem, translated isolation, actual faults,
strict enforcement and broader qualification remain. Goal 20 stays active.

### Stage 10d terminal controller reconnaissance and patch contract

Problem: published xHCI controllers have no terminal software drain or power
finalizer integration. USB/CDC worker pointers also outlive cooperative task
exit without retained task references, and yielded callback ownership can be
abandoned unless held through shutdown.
Expected files: xhci.cpp/hpp, new xhci_terminal.hpp, cdc_ether.cpp,
power.cpp, xhci DMA host tests and lifecycle source guards, evidence/goal.
Files read: xHCI probe/IRQ/event/command/transfer/enumeration/slot teardown,
CDC attach/publish/quiesce/detach/TX/RX, shared DMA admission/retirement and
manager, scheduler shutdown ownership/task references, NAPI retained-worker
pattern, netdevice unregister begin/wait and power finalizer.
Entrypoints: ordinary finalizer calls xhci_shutdown after WKI shutdown/VFS
unmount and before CPU halt. Controller lifecycle worker and public transfer/
configuration/command entrypoints acquire separate monotonic gates. Shutdown
closes both, drains lifecycle mutation, closes endpoint doorbells, quiesces
classes/cancels requests, drains task I/O, then halts/masks/BME-off/IRQ-drains.
CDC terminal drain keeps closed class/netdevice/USB metadata stable forever;
it does not use ordinary detach, which waits unboundedly for external retained
netdevice references and resets recyclable class storage.
Invariants: no DMA retirement without lifecycle, I/O, class, halt, BME and IRQ
proof. Permanent quarantine remains permanent. No late callbacks or reprobe
can reopen the controller. Retained metadata makes failed admission safe.
Locks: existing endpoint ring locks serialize closure with final publication;
atomic monotonic admission counts and retirement state serialize shutdown.
No waiting/allocating under ring locks; IRQ stays bounded and allocation-free.
Unsafe contexts: shutdown rejects non-waitable task contexts; callback/worker
ownership holds defer cooperative exit but permit preemption and migration.
Allocation/blocking: no new terminal allocation, bounded reader/halt waits,
unchanged task-context DMA release. Drain failure retains backing without
racing ownership fields still being mutated by readers.
ABI/wire/syscall: only internal UsbClassDriver terminal callback/layout changes.
Build/test: production ordering/failure model tests, source guards, both builds,
rootless four-CPU attached/held/withheld-halt shutdown fixtures, idempotence,
late admission rejection, byte echoes and accounting, existing focused probes.
Rollback: revert terminal integration/gates/class hook together; never leave
finalizer freeing buffers without the corresponding software/hardware proofs.

Stage10d implementation detail: close lifecycle admission first and let an
already-admitted enumerator finish before closing I/O admission. Even a failed
lifecycle drain closes I/O admission and disables BME, but never traverses
mutable slot ownership. Successful lifecycle drain precedes endpoint closure,
class quiesce and cancellation of indefinite bulk-IN requests. Software drain
failure retains backing without racing its owner; only fully drained ownership
can be quarantined/released by the terminal path. The shared production model
tests all 4096 combinations of backend outcomes, plus exact success ordering.

Stage10d live checkpoint (full suite still pending at this entry): Normal
Build WOS and isolated diagnostic builds passed, format/tidy checks passed
(style warnings remain non-fatal), and 26 host targets/137 unit cases passed.
Five rootless four-CPU boots powered off normally. Held publication survived
15 shutdown-requested yields, rejected its late doorbell and released ten
regions/pages. IOMMU-on/off traffic cases each echoed 64 exact USB packets
(65/1514/1801/2015 bytes) before terminal release to zero. Withheld halt proof
quarantined all ten regions/pages; actual halt, IRQ drain and BME-off were
observed, but deliberately not accepted as retirement proof. A separate
ordinary power-finalizer fixture retired its still-attached USB controller to
zero after VFS unmount, without a prior explicit xhci_shutdown call.
Repeated terminal calls and late control/bulk/command calls were checked.
All five manifests match ELF
68f6803ee624130236c0ae85734331c6d1812268d3c68d3719b73993a828e658;
the 103-file source archive matches the current code and all ten WOSDBG live
log calls passed. Receipts: build/goal20-stage10d/validation-usb-*.json.

Terminal shutdown retains closed controller, request, class/netdevice and
worker metadata; it releases DMA backing only. It does not reopen quarantined
mappings, guarantee progress under arbitrary scheduler starvation, or qualify
physical controllers. Existing ordinary detach can still wait for external
netdevice references; if a lifecycle worker is stuck there, terminal shutdown
times out and retains backing. Larger multi-TRB USB payloads remain unqualified.

### Next scope reconnaissance (preliminary, no ivshmem edits)

The remaining ivshmem paths are dev/ivshmem/ivshmem_net.cpp and
net/wki/transport_ivshmem.cpp. Both copy CPU data through BAR2 and write a peer
doorbell in BAR0. The WKI RDMA registration ignores its phys_addr parameter and
allocates offsets within the shared BAR; read/write use memcpy. These are not
guest-RAM DMA descriptors to convert into streaming allocations. Both probes
currently call pci_enable_bus_master before mapping BARs; that wrapper panics
if the common domain admission fails. Applicable admission, IRQ/worker terminal
ownership and QEMU shared-memory qualification still need explicit review.
Do not claim that a translated IOMMU isolates shared BAR memory from the peer,
or broaden this goal into redesigning WKI RDMA security/shared-ring protocols.
Before editing, finish WKI core/transport/wire and launcher call-flow reads,
confirm MSI/doorbell behavior against the actual QEMU device model, and write
the bounded patch contract. The generic DMA backend and domains.cpp still
implement passthrough only; translated page tables, grouping/RMRR handling,
actual fault records/injection and strict admission remain mandatory.

### Stage 10d final validation checkpoint

The final ELF above passed full isolated Intel-IOMMU KTEST: 65,982 assertions,
zero failures, all 1,167 enabled cases accounted for and three disabled cases
skipped. It reached normal selftest poweroff. Its coverage fingerprint is
text_start=0xffffffff80000158, text_end=0xffffffff81822f56,
text_fnv64=0xed2acb82c464cee0. Full-suite e1000 traffic verified 300 frames.
The final 32-cycle USB attach/detach regression also passed with the new gates,
returning to the four-controller-region baseline after every cycle; that
fixture ends with private QMP quit and is separate from terminal poweroff tests.

All seven boot manifests match the retained ELF. Final source hashes match all
103 archived code files, all 14 WOSDBG load/search calls passed, and
build/goal20-stage10d/validate-final.py generated validation.json successfully.
Normal Build WOS used `source .vscode/wos-env.sh && scripts/dev/build_wos.sh`
(RelWithDebInfo); isolated build used `bin/wos-ktest --build-only` (Debug with
KCFI/report KUBSan/KASan/KCOV). Both passed. Formatting and the 26 selected host
targets/137 unit cases passed. Runtime launchers used --no-setup with private
isolated configs, disks, overlays, multicast and QMP endpoints.

Process audit must use the host namespace: the sandbox-local process list does
not show escalated QEMU processes. Final host audit confirmed only the unrelated
QEMU PID 36201 remains; it was not touched. All task-owned fixtures exited.
The previously documented broader credential/scheduler source-inventory issues
remain baseline observations, not newly repaired or suppressed by these tests.

Published xHCI terminal DMA retirement is now implemented and qualified under
these QEMU cases. Goal 20 remains active: no translated isolation is enabled.
Outstanding scope includes multi-TRB payload qualification, applicable ivshmem
ownership/admission, translated grouped domains/RMRR policy, fault diagnostics
and deliberate stale/unmapped DMA, strict enforcement and final qualification.

### Stage 11a ivshmem reconnaissance and patch contract

Problem: both ivshmem probes enable BME before BAR ownership is checked, try
MSI instead of MSI-X, and fabricate an INTx fallback for revision-1 plain
devices. Doorbell writes reverse the PCI device's peer/vector fields.
Subsystems/files: dev/ivshmem/{ivshmem_net.cpp,hpp,new PCI ownership/model
helpers}, net/wki/transport_ivshmem.cpp, existing/new host tests and evidence.
Read: full ivshmem-net entrypoints; WKI transport init/TX/RX/RDMA/poll/mailbox;
WkiTransport API, WKI init/shutdown/register/unregister and wire constants;
PCI config/BAR sizing/mapping/MSI/MSI-X; IRQ reservation/request/free; NAPI
ownership; shared DMA admission and existing NIC retirement patterns;
init selection and shared node launcher ivshmem command construction.
Flow: init chooses netdevice or WKI ownership, maps BAR0/BAR2, builds CPU ring
views, configures interrupts if present, and publishes class callbacks. WKI
also polls from its peer timer. Raw RDMA returns offsets/pointers within BAR2;
there are no guest-RAM DMA descriptors or mappings to replace.
Primary sources: QEMU ivshmem specification and current hw/misc/ivshmem-pci.c,
hw/pci/msix.c and msi.c. Revision 1 plain has no interrupt capability; doorbell
has MSI-X BAR1. Doorbell bits 31:16 select peer, 15:0 select vector. BAR0
Interrupt Mask/Status are reserved in revision 1 and do not mask MSI-X.
URLs: https://www.qemu.org/docs/master/specs/ivshmem-spec.html and
https://raw.githubusercontent.com/qemu/qemu/master/hw/misc/ivshmem-pci.c .

Minimal strategy for this checkpoint: shared explicit PCI resource owner,
validated revision/BAR extents/MSI-X table bounds, BME-off prepare, owned IRQ
registration before interrupt enable, MSI-X admission only after domain setup,
no IRQ/BME for plain mode, checked mask/drain/BME-off on failed activation,
and the specified doorbell encoding. Keep CPU BAR mappings and failed owner
metadata pinned: the current PCI mapper has no retained-user unmap contract.
Follow-up terminal callback closure must precede published ownership release;
this probe checkpoint does not claim published teardown or whole-driver
workload qualification. Legacy revision 0 is explicitly unsupported until its
late BAR2 availability/INTx lifecycle has a separate qualified contract.
Invariants: no guest-RAM DMA mapping is invented for CPU shared BAR memory;
never claim peer isolation/revocation. Do not recycle a failed owner containing
a potentially registered IRQ. Preserve WKI frame formats, authentication,
offset/rkey semantics, fencing and packet ownership. No new blocking IRQ work.
Locks: PCI config guard per transaction; IRQ registry drain only in task/boot
context. Probe is boot-serialized; no callbacks until owner/private fields are
initialized. Fixed owner metadata; no new mapping or allocation in IRQ/NAPI.
ABI/wire: private driver/resource types and correct hardware doorbell encoding;
shared BAR layouts and WKI wire structures unchanged.
Tests/build: host production selection/bounds/ordering/failure tests, source
guards, Normal Build WOS and isolated diagnostic build, rootless QEMU plain,
doorbell and malformed BAR/probe-failure fixtures with zero DMA regions and
checked BME/IRQ state. Broader live transport/terminal work follows explicitly.
Unknowns: physical devices and legacy revision 0 remain unqualified. Existing
ivshmem-net stores ring head/tail in private fields despite its shared-ring
comment; shared traffic qualification must address that separately without
changing the wire layout. WKI currently drains protocol work inline in IRQ;
terminal integration should move that work to its existing task poll path.
Rollback: revert shared resource/probe integration together; never leave an
enabled IRQ pointing to recycled metadata or count an unmapped BAR as DMA.

Stage 11a qualification extension: add a SELFTEST-only explicit PCI-test
reservation in both probe entrypoints, with a final-phase test owning a stable,
unpublished resource record. Exercise plain/MSI-X activate, actual self-doorbell
IRQ, mask/drain/repeat-stop, BAR undersize rejection and unchanged per-device
DMA counters. Extend the shared node launcher with explicit plain/doorbell mode
and a UNIX server socket for rootless fixtures; defaults remain plain. This
keeps QEMU argument construction in node_setup.py. It does not retire a
published WKI/netdevice or revoke escaped BAR pointers.

### Stage 11a implementation and focused qualification

Added `dev/ivshmem/pci_model.hpp` and `pci_resources.{hpp,cpp}` and routed both
`ivshmem_net.cpp` and `net/wki/transport_ivshmem.cpp` through that owner. Prepare
holds BME off, requires PCI domain admission, validates revision-1 BAR and
MSI-X table/PBA extents, and constructs stable CPU BAR views. Plain mode has
no IRQ or BME. Doorbell mode owns the IRQ before enabling MSI-X and BME.
Failure unwind masks the hardware, disables BME, and drains owned IRQ readers;
failed owner records and BAR mappings remain pinned. Repeated net probes and
WKI probe retries cannot recycle those records. NAPI masks the MSI-X table
entry rather than reserved revision-1 BAR0 mask/status fields. Doorbells use
peer ID in bits 31:16 and the single configured vector zero in bits 15:0;
forwarded IRQ data remains in the existing shared mailbox. Shared memory and
WKI wire layouts, authentication, raw offset/rkey semantics and packet
ownership are unchanged. No guest-RAM DMA allocation is invented for BAR2.

Added six production-model host tests covering revision selection, BAR range
arithmetic, table/PBA overlap, ordering and every modeled activation failure,
and doorbell bit placement. Existing WKI source guards now check shared-owner
publication order and retained failed claims. A diagnostic-only PCI reservation
and final-phase KTEST exercise a stable unpublished resource owner.

The shared launcher accepts explicit plain/doorbell device modes, with a UNIX
server path for doorbell mode. KTEST node-to-zone-to-node conversion now retains
explicit ivshmem devices separately from zone link topology, and retains its
firmware string overrides. Launcher regression tests cover both QEMU command
shapes and the conversion. The first fixture had no NIC and produced no node;
the next exposed dropped explicit ivshmem configuration and deliberately failed
the device-required assertion. These were fixture/conversion defects, with
logs preserved under `build/goal20-stage11a/pre-repeat-guard/`. After the
conversion correction, every focused fixture passed; a final repeated-probe
ownership guard was then added and all eight fixtures were rerun on the final
kernel, rather than attributing the earlier ELF evidence to that edit.

Final focused evidence: `build/goal20-stage11a/`, private mutable fixtures under
`ktest-data/goal20-stage11a/`. Final diagnostic ELF SHA-256:
`ba2038d8f2ad24d5a7dd13ed4cb83185e76f179c70854c98a6aa19a7aaf75485`.
Normal Build WOS (`RelWithDebInfo`) and isolated `bin/wos-ktest --build-only`
(`Debug`, KCFI/KUBSan/KASan/KCOV/selftests) pass. Format/tidy checks pass with
nonfatal style/global-constructor warnings. The 29 selected host targets pass,
including 143 GTest cases and 32 launcher regression cases.

Four rootless 4-CPU KTEST fixtures powered off normally with zero failed
assertions: plain with IOMMU (47), MSI-X with IOMMU (48), undersized 1-MiB BAR2
rejection (39), and plain without IOMMU (47). The MSI-X fixture's private UNIX
server assigned ID 7; the guest received one actual self-doorbell IRQ, then
masked/drained it, cleared BME, rejected reactivation and repeated stop safely.
Plain mode allocated no IRQ and kept BME off. Per-device DMA counters remained
zero. All four coverage fingerprints match the retained ELF:
`text_start=0xffffffff80000158 text_end=0xffffffff81827b16
text_fnv64=0x1338cbd71b734ba0`.

Four additional rootless 4-CPU boots published ivshmem-net and WKI transports
in both plain and MSI-X modes. Host inspection confirmed the guest-written
BAR2 header magic/version/layout, and activation logs report the expected
IRQ/BME state. These fixtures terminate through their private QMP sockets
after publication. They are NOT published-driver retirement or inter-peer
traffic evidence. The solo WKI instances correctly continued after their
bounded peer-ready timeout; no peer authentication/session success is claimed.
All eight boot manifests match the retained final ELF.

Remaining ivshmem work: close/drain published CPU callbacks before stopping
interrupts, defer WKI RX protocol work out of IRQ context, qualify the existing
two-role doorbell peer-ID assumption with real peer traffic, and repair/qualify
shared ring ownership and BAR offset bounds. Existing net head/tail fields are
private copies; its 2048-byte RX buffer does not substantiate the advertised
9000-byte MTU. Raw RDMA pointers and peer BAR access have no revocation contract,
so BAR mappings stay kernel-lifetime. Legacy revision 0 and physical hardware
remain unqualified. No translated IOMMU isolation is enabled by this stage.

Stage 11a final full-suite evidence: the retained final ELF completed a rootless
32-CPU, 32-GiB isolated KTEST boot with 65,983 assertions passed and zero failed,
then powered off normally. Runtime manifest audit confirms all 1,168 enabled
tests passed and three disabled tests skipped. The E1000 fixture completed
300 exact packet echoes and restored its packet pool. The full-suite coverage
fingerprint matches the same retained ELF. All nine boot manifests match;
113 archived source-file hashes match the workspace; all 18 WOSDBG log-load
and search calls succeeded. Consolidated result is
`build/goal20-stage11a/validation.json`, with reproducible runners and validator.
Final host-namespace process audit found only unrelated QEMU PID 36201; it was
not touched. All task-owned fixtures exited. Previously documented broad
credential/scheduler source-inventory failures remain baseline observations,
not repaired or suppressed by this checkpoint.

Goal 20 remains active. Published ivshmem teardown/traffic, larger USB multi-TRB
payload qualification, translated grouped domains/RMRR, real stale/unmapped
DMA fault diagnostics, strict enforcement and final qualification remain.

### Stage 11b reconnaissance and patch contract

Previous turn classification: progress, verified by the 11a source/evidence
archive and current production integration. No indispensable runtime blocker.
Problem: published ivshmem callbacks may touch BARs or re-enable vectors during
shutdown; WKI dispatches protocol work from IRQ, and core shutdown can clear
auth state before a transport callback has drained. Escaped RDMA pointers have
no revocation contract and cannot justify unmapping shared BARs.
Expected files: PCI resource/model helpers, new callback guard and diagnostic
runner, ivshmem-net and WKI transport headers/implementations, WKI shutdown,
power finalizer, host/source tests and execution ledger.
Read: current net driver and all private operations; WKI transport RX/TX/RDMA,
mailbox, init/poll; WkiTransport API and wire constants; core register/unregister
and shutdown; peer task poll; zone RDMA borrow/free callsites; NAPI admission,
worker retention and disable; e1000 retirement and live ownership fixtures;
scheduler KernelShutdownHold entry/exit; current PCI resource mask/drain.
Flow: net ops/IRQ/NAPI and WKI ops/public RDMA/task poll acquire a monotonic
resource callback gate plus cooperative-shutdown hold. IRQ only schedules NAPI
or masks the WKI vector; the existing WKI task poll performs bounded dispatch.
Retirement closes admission, drains without registry/driver locks, unpublishes
class routing, then masks/BME-off/drains IRQ. Repeats never reopen. WKI core
retains its state if this transport cannot prove its callback drain.
Invariants: metadata and BAR mappings remain kernel-lifetime; no new DMA
mapping or peer revocation claim. Failed software drain does not mutate class
owners or free IRQ metadata. Failure may mask the function and clear BME while
existing callbacks unwind; vector unmask alone cannot undo the function mask.
Successful drain permits class unpublication without waiting for external raw
metadata snapshots to vanish. Wire layouts, authentication, rkeys, fencing and
packet ownership stay unchanged.
Locks: no blocking drain with transport registry, netdevice registry, bitmap,
TX or IRQ locks held. Admission and IRQ masking are allocation-free; task/boot
retirement bounded to five seconds. Class callback ownership spans yields,
including cooperative kernel-thread shutdown. IRQ never waits for WKI ACKs.
ABI impact: private resource/driver shutdown functions only; no wire/syscall
change. WKI receive-handler publication becomes atomic.
Build/test plan: production retirement ordering/failure model tests; actual
callback coverage/source checks; normal and isolated builds; rootless 4-CPU
published plain/MSI-X net/WKI fixtures with real interrupts, held callback
shutdown, late-call/repeat rejection and deliberately withheld stop proof;
ordinary power-finalizer fixtures. Retain exact ELF/source/manifest evidence.
Rollback: revert closure and its callers together; do not unmap BARs or recycle
IRQ/private storage on missing drain proof. Physical device qualification and
raw peer-pointer revocation remain outside this software proof.

Stage 11b contract refinement after tracing chaos delivery: deferred RX can
outlive the ivshmem driver callback and owns `chaos_in_flight`. Split the
existing chaos unregister admission-close from its final wait/removal, close
that queue before ivshmem retirement, and include its lease count in the same
bounded five-second drain. Only a zero count after closure permits the existing
unregister finalization. Protect immediate/deferred lease scopes against
cooperative task exit with KernelShutdownHold. Add chaos.{hpp,cpp} and its
source guards to the impact map. WKI closes/drains after goodbye transmission
but before peer/core destruction; an unproven drain retains that state. No
protocol or chaos scheduling-policy change, and no new wait under chaos lock.

### Stage 11b implementation and published-owner qualification

Both ivshmem drivers now own a monotonic callback admission gate. The common
`CallbackGuard` holds cooperative shutdown ownership until its callback lease
has been released. Retirement closes admission, waits for admitted owners,
unpublishes routing, and then masks/BME-off/drains the IRQ. Failure of the
bounded software drain only inhibits hardware; it leaves class metadata and
IRQ ownership intact. Failed stop proof retains the IRQ owner. Repeated
retirement never reopens admission. Shared BAR mappings and private metadata
remain pinned on every path because escaped pointers and peer BAR accesses
still have no revocation contract.

Netdevice TX, open/close, IRQ and NAPI callbacks use this guard; rejected TX
still consumes its packet. The power finalizer retires published and failed
net probes. WKI TX, RX dispatch, task polling, IRQ, RDMA and mailbox operations
also use it. WKI IRQ now only masks its vector, with protocol dispatch and
rearming in the existing peer task poll. Receive-handler publication is atomic.
No packet, header, mailbox, wire, rkey or syscall ABI changes were made.

WKI retirement closes both registry and chaos lease admission under their
existing lock order, then waits outside those locks for callback admission and
`chaos_in_flight` to drain. Only that proof permits transport unregister.
Cooperative shutdown holds cover immediate/deferred chaos deliveries and
resolved-route/broadcast registry leases. Core shutdown sends its existing
goodbyes first, then requires ivshmem retirement before destroying peer/auth
state. An unproven drain or IRQ stop preserves that state. This does not extend
revocation to the raw pointers returned to zone users.

Normal Build WOS (`RelWithDebInfo`), isolated diagnostic build and format/tidy
checks pass. There are nonfatal existing style/global-constructor warnings.
All 34 selected host targets pass, including 200 GTest cases, the DMAR fuzz
smoke, 32 launcher cases and WKI chaos/routing source/model regressions.
The production retirement model covers all four drain/stop outcomes and
checks unpublication and hardware-inhibition ordering. Source checks cover
actual callback gates, deferred IRQ dispatch and lease shutdown ordering.

All twelve rootless 4-CPU published-driver fixtures passed and exited through
normal poweroff: net/WKI plain and MSI-X held-admission cases; failed drain
and withheld stop proof for each MSI-X driver; independent WKI deferred-lease
success and timeout; and ordinary power finalizers with each published MSI-X
driver. The private doorbell server assigned ID 7 and each MSI-X fixture
received a real self-doorbell interrupt. Normal retirement cleared BME and
released the IRQ. Missing proofs retained IRQ ownership, returned `-EIO`,
and repeated retirement returned the same error. Late calls rejected new
work without changing packet-pool counts, RDMA bitmap, mailbox, NAPI state,
or handler/IRQ state. All cases retained BARs and reported zero guest-DMA
regions for ivshmem, which is CPU shared-memory hardware.

The held-admission cases use the production callback guard with a controlled
owner on another CPU; the deferred-lease cases hold the real transport counter
independently of driver admission. They prove bounded drain and cooperative
exit ownership, not authenticated peer traffic or an in-flight remote RDMA
workload. The diagnostic `--ivshmem-test-withhold-stop` flag withholds the
common stop proof globally, including an empty WKI resource owner in the net
fixture; that fixture therefore also logs WKI core retention. Ordinary power
fixtures have no injected proof failures. Shared ring/offset validation and
actual two-peer traffic remain separate work.

Retained diagnostic ELF SHA-256:
`4a9f4d82236f4eb55558d13129fdd2db62a15791c495d3141a3ff1ebc68f6efa`.
The 121-file source archive matches the workspace. Reproducible fixtures,
per-case manifests/results and build logs are in `build/goal20-stage11b/`;
mutable VM state is isolated under `ktest-data/goal20-stage11b/`.

Final regression evidence on that same ELF: all four unpublished PCI fixtures
powered off normally with zero failed assertions (plain 47, MSI-X 48,
undersized BAR 39, IOMMU-off plain 47). Their coverage fingerprint is
`text_start=0xffffffff80000158 text_end=0xffffffff81832786
text_fnv64=0x59ef780e222c6f54`. Full isolated 32-CPU, 32-GiB KTEST completed
65,983 assertions with zero failures, all 1,168 enabled tests passed, and three
disabled tests skipped. It completed 300 exact e1000 packet echoes with pool
restoration, AHCI/e1000/xHCI terminal tests, and normal selftest poweroff.
The full coverage fingerprint also matches the retained ELF.

`build/goal20-stage11b/validate-final.py` verifies all seventeen boot manifests,
serial hashes, the 121 archived source contents and workspace hashes, the
34-target host result, runtime manifest and all 34 WOSDBG load/search calls.
No UBSan/KASan failure report was found in these final logs. Consolidated result:
`build/goal20-stage11b/validation.json`. The final host-namespace process audit
found only unrelated QEMU PID 36201; it was not touched. Every task-owned VM
exited. Earlier broad credential/scheduler source-inventory failures remain
documented baseline observations; this stage did not fix or suppress them.

Goal 20 remains active. Current domains still use passthrough and do not
provide DMA isolation. Shared-BAR workload bounds/traffic, larger USB
multi-TRB payloads, translated grouped domains/RMRR policy, actual stale or
unmapped DMA fault diagnostics, strict enforcement and final qualification
remain. This stage made verified implementation progress; there is no repeated
indispensable runtime blocker.

### Follow-up reconnaissance: shared BAR workload limits

Current source remains authoritative. `ivshmem_net.{hpp,cpp}` still copies
ring indices into private fields, whereas WKI's `WkiRing` already points to
shared indices. Both ring readers truncate oversized frames and consume the
claimed length without checking the available committed bytes; both probe
paths consume shared header offsets without a complete immutable bounds
snapshot. Net RX uses a 2048-byte temporary despite a 9000-byte MTU and a
10240-byte PacketBuffer capacity. WKI TX has no producer serialization.
These defects predate the terminal work and its fixtures do not exercise them.

Relevant paths: netdevice TX -> ring write -> peer doorbell; IRQ -> NAPI or
WKI vector mask; NAPI rescue / WKI peer task -> ring read -> packet or protocol
handler. NAPI already serializes the consumer, and WKI has its own atomic
single-drainer flag. Net has a TX lock; WKI needs equivalent bounded producer
serialization. Shared [head:u32, tail:u32, len:u16, data, pad-to-four] layouts
can be preserved while validating indices/lengths and using acquire/release
publication. Never reset a ring under an active peer to recover malformed data.

`transport_ivshmem.cpp` also needs subtract-before-add extent checks for
rkey+remote_offset+length, pointer offsets and rounded bitmap page counts.
`zone.cpp` allocates/borrows those pointers and frees backing through the
existing private bitmap API. Bounds repair must not claim remote revocation,
change raw rkey semantics, or redesign cross-peer RDMA allocation/security.
The mailbox is a fixed header overlay; keep its wire layout and terminal guard.

The shared node launcher already supplies logical role and WKI peer identity;
these are distinct from the ivshmem server's hardware peer IDs. The actual
TX/doorbell helpers still assume hardware IDs 0/1. ID-7 self-doorbell tests
prove the PCI bit encoding only. Two-peer workload qualification must control
and verify that role/ID relation rather than infer it from a self-interrupt.
Use private rootless paired fixtures, complete/jumbo frames, wrap/full/malformed
ring models and unchanged packet-pool accounting. Keep authenticated WKI
traffic evidence distinct from a diagnostic raw shared-ring echo.

### Stage 12a: translated page-table engine contract

Previous goal turn: verified implementation progress (11b builds, seventeen
live boots and archived evidence). No indispensable blocker. The next main
isolation dependency is a mutable second-stage page-table engine; existing
VT-d helpers only encode entries and activation/invalidation commands.

Problem: DMA backend still returns physical addresses in unrestricted
passthrough. Introduce the real table mutation/ownership algorithm required
by its forthcoming translated backend, before changing endpoint admission.
Expected files: new dev/iommu page-table engine, host tests, kernel selftests,
host test registration and ledger. Domain policy stays explicitly passthrough
until platform invalidation, grouping/RMRR and device qualification are wired.
Files read: domains/discovery/scopes/vtd_model/vtd_activation/vtd_capability;
DMA manager/backend/API; PCI admission; physical allocation API; existing
domain/table KTEST and host tests; kernel/build/testing instructions and tasks.
Primary reference: Intel VT-d rev.4.0, sections 3.6, 6.2, 6.5, 6.8 and 9.8,
local /tmp/wos-goal20-spec/vtd.txt, from the Intel specification linked above.

Flow: serialized task/boot mutation validates complete page-aligned IOVA and
physical extents and all existing leaves; prepares missing empty branches;
publishes leaves; invalidates before success. Unmap validates the whole range
before clearing leaves, then requires invalidation before caller may free data.
Invariants: only requested 4-KiB pages exposed, access bits preserved, no huge
pages/ATS, no partial unmap on bad arguments, no overlapping replacement.
Allocation failure before leaf publication leaves data unexposed. Empty table
branches already reachable by hardware remain owned and counted. Failed
invalidation poisons the table owner and retains mapped-data accounting even
after leaf clearing. Hardware-visible table pages are never implicitly freed.
Locks: nonblocking per-table mutation admission; no IRQ-disabled/spin lock
across allocator calls. Backend invalidation must serialize its unit register
commands. Kernel callers must hold cooperative shutdown ownership across a
mutation that may allocate/yield. Busy mutation fails rather than spinning.
Unsafe contexts: map/unmap are task/boot only; IRQ/NAPI use preallocated DMA
submissions and never mutate tables. Table walking uses retained backend-owned
metadata to reject foreign/wrong-level/wrong-subtree page references.
Allocation/blocking: fallible table-page allocation, bounded mapping extent
and table capacity; explicit cleanup only for roots never exposed to hardware.
ABI/wire/syscall impact: none. Driver DMA API and fallback policy unchanged.
Tests: use the exact engine with independent software translation walks,
direction and domain separation, branch boundaries, allocation failures at
every preparation point, overlap/malformed ranges, stale/foreign table
references, failed invalidation, reentrant busy calls and exact page accounting.
Kernel qualification uses real IOMMU_TABLE allocator pages with an unpublished
root; this is CPU table/alignment/accounting proof, not hardware isolation.
Normal Build WOS, isolated diagnostic build and rootless KTEST are required.
Rollback: remove engine/tests together; never reclaim exposed tables or data
on an uncertain invalidation result. Keep translated hardware activation,
actual fault delivery and strict policy explicitly outstanding.

12a refinement: add `page_storage.{hpp,cpp}` as the kernel allocator/cache
backend, with caller-owned bounded metadata and strict level/subtree matching.
An independent root-lifetime generation and owner token bind `expose` to the
snapshot whose address the caller will publish; stale, foreign and exhausted
lifetimes must fail even if the allocator reuses the same physical root page.
The first full-suite fixture was intentionally stopped through its private
QMP socket while this review improvement was added. Its old ELF/source/logs
are retained in `build/goal20-stage12a/pre-root-generation/`; no full-suite
success is claimed for that interrupted run.

12a implementation: `page_table.hpp` now owns exact 4-KiB map/unmap mutations
for legacy three-, four- and five-level trees. It validates complete extents
and existing leaves before mutation, creates and publishes zero children
before parent links, and requires invalidation after live leaf updates.
Allocation failure before leaf publication returns an unexposed-data result;
empty published branches remain counted and owned. Uncertain invalidation
quarantines the owner and retains data-page accounting even after PTE clearing.
Root publication requires an owner/address/generation token. Generation
exhaustion refuses reinitialization rather than reusing a stale token.

`page_storage.{hpp,cpp}` supplies real IOMMU_TABLE allocations, a bounded
caller-provided metadata span, strict physical/level/subtree lookup, cache-line
publication and an explicit unit-invalidation callback. Missing invalidation
callback fails closed. Table destruction is explicit and allowed only while
the root has never been exposed and no data mappings remain. Kernel callers
must retain the metadata/backend and hold cooperative shutdown ownership.
The nonblocking mutation gate does not hold an IRQ-disabled allocator lock.

Eleven host cases exercise the exact engine, with independent software walks
and 2,000 randomized mutation rounds, full 64-MiB mappings, last-page bounds,
permissions/domain separation, all preparation allocation-failure positions,
overlap/wrong-unmap rejection, retained failed invalidation, malformed and
foreign table pointers, concurrent/reentrant mutation admission, stale/foreign
root snapshots and generation exhaustion. The kernel selftests use real table
pages to inspect encoded leaves and physical owner accounting, exercise
bounded metadata exhaustion/unwind, and execute qualified CLFLUSH publication
across a leaf cache-line boundary. Their roots are deliberately unpublished;
they are not hardware DMA translation or fault evidence.

Next integration impact map (still pending): domains.cpp must own stable
per-domain storage/engine metadata, expose root tokens immediately before
context publication, and serialize invalidation through each unit's registers.
DMA backend map/unmap must use these trees, with original physical address and
allocation direction retained through release. `Manager::Record::direction`
changes on streaming rebind; `allowed_direction` is the original mapping
permission and must be used for unmap verification. Extend both failure-unwind
and ordinary-release backend calls together. Existing DMA KTEST emulates
device writes by treating an address as identity; keep identity IOVAs initially
or replace that test-only assumption when introducing an IOVA allocator.
Unit invalidation uncertainty must inhibit new admissions and preserve backing
across all domains sharing the unit. Group/RMRR ownership, virtio
ACCESS_PLATFORM, actual faults and strict enforcement remain mandatory gates.
The engine's `busy` result is known non-admission, not failed invalidation:
the platform adapter should serialize or boundedly retry it in safe task/boot
context before translating an unmap result to the DMA manager's boolean proof.
Do not turn ordinary concurrent map/release activity into unnecessary permanent
quarantine. Every retry must retain cooperative shutdown ownership and run
outside unit/PCI/allocator spin locks.

12a final evidence: Normal Build WOS (`RelWithDebInfo`, task-equivalent
`source .vscode/wos-env.sh && scripts/dev/build_wos.sh`), isolated diagnostic
`bin/wos-ktest --build-only`, and format/tidy checks pass. Tidy reports nonfatal
style warnings. All 35 selected host targets pass, including 211 GTest cases,
32 launcher cases, source guards and DMAR fuzz smoke. Eleven of the GTest
cases exercise the new page-table engine.

The final rootless 32-CPU, 32-GiB Intel-IOMMU KTEST boot completed 66,074
assertions with zero failures, passed all 1,171 enabled tests and skipped three
disabled tests, then powered off normally. All three new VtdPageTable tests
passed, including real table-allocation accounting restoration and the
CLFLUSH path with 64-byte cache lines. E1000 completed 300 exact echoes with
packet-pool restoration; existing terminal device tests passed. No UBSan/KASan
failure report was found in the final log.

Final ELF SHA-256:
`ef3cc3e1601eacc9772f7c2b882c29dcba5183b4f7da2684d27e7dc31a59254f`.
Coverage fingerprint:
`text_start=0xffffffff80000158 text_end=0xffffffff8183c216
text_fnv64=0x0f53a5e5c336e62f`. The boot manifest, coverage fingerprint,
serial hash, 126 archived source contents/workspace hashes, runtime inventory
and both WOSDBG load/search calls pass final validation. Results and runner:
`build/goal20-stage12a/validation.json` and `validate-final.py`. Private final
VM artifacts are under `ktest-data/goal20-stage12a/full/`; interrupted earlier
fixtures are retained separately. Final host process audit found only unrelated
QEMU PID 36201, which was untouched. All task-owned VMs exited.

Goal 20 remains active. This implements and qualifies the table engine and
kernel storage backend, not active second-stage DMA translation. Next wire
them into the DMA backend/per-device contexts, qualify grouping/RMRR and real
faults, then enforce strict admission. The existing shared-BAR traffic and
multi-TRB USB qualification gaps remain recorded. No hardware isolation or
noncoherent device-cache qualification is inferred from unpublished CPU walks.

## Stage 12b — opt-in live translated domains (in progress)

Problem: the qualified page-table engine is not yet connected to VT-d context
entries or managed DMA map/unmap. Passthrough offers no isolation.
Expected files to change: iommu/domains.*, vtd_activation.hpp, dma/dma.*,
dma/manager.hpp, pci.cpp, init_wrappers.cpp, IOMMU kernel tests, DMA/VT-d host
tests and this ledger/goal. Read entrypoints: all those files, discovery.*,
scopes.hpp, page_table.hpp, page_storage.*, vtd_model.hpp, capability decoder,
physical allocator context predicate, scheduler shutdown ownership, PCI config
writers, host fixtures and isolated launcher runner.
Invariants to preserve: exact mapping ownership and original permissions;
BME admission only after context installation; no data free without device
retirement plus completed invalidation; failure pins backing and all exposed
tables; no fallback after hardware exposure; unchanged default passthrough.
Locks involved: DMA registry lock only for records; unit task-context admission
gate serializes table mutation and hardware invalidation, without disabling
interrupts/preemption; PCI config lock serializes final BME admission with
quarantine. No allocator call under a spinlock. Unit failure closes every
associated domain, not just the triggering tree.
Unsafe contexts: map/release remain boot/task-only. IRQ/NAPI prepare/complete
continue using preallocated mappings and do not walk/allocate tables.
Allocation/blocking assumptions: bounded table metadata per requester, real
IOMMU_TABLE allocations; task gate waits yield only in an eligible task, with
cooperative shutdown hold retained across allocation/map/free. Boot is BSP-only.
ABI/wire/syscall compatibility impact: none. New explicit development boot
option --iommu-translated; unsupported topology/RMRR/virtio refuses the request,
never silently downgrades it. --iommu-strict remains unavailable pending full
qualification, fault handling and production policy. Initial IOVA equals physical
address but only explicitly mapped pages have PTEs. Exposed empty tables remain
owned for kernel lifetime; dynamic table accounting is reported separately.
Build/test plan: exact-unmap and revoked-admission host regressions, repeated
register invalidation failure model, Normal Build WOS, isolated KTEST; fresh
rootless Intel-IOMMU e1000e/AHCI/xHCI fixtures with exact ELF/manifest/source
capture. Inspect live table encodings and unit status. Deliberate hardware faults
and general grouping/RMRR/virtio remain separate required follow-ups if not
qualified here. Preserve all prior evidence and unrelated host VM.
Rollback plan: omit --iommu-translated for existing explicit passthrough behavior;
revert only this stage's changes if needed. Never disable live translation or
free a published tree as rollback.

The VT-d rev. 4.0 register invalidation specification (11.4.6/11.4.7 and 6.8)
requires global context invalidation followed by equally broad IOTLB invalidation;
actual granularity must confirm both, with read/write draining requested. These
operations also implicitly flush write buffers on RWBF hardware. The backend
uses this full sequence for every live change, including not-present additions.

12b implementation now connects the real PageStorage/PageTable engine to
per-requester contexts under `--iommu-translated`. Each requester has bounded
512-record metadata and a distinct zeroed tree/domain ID. Initial IOVA equals
physical address, but empty neighbors and other domains remain inaccessible.
Prepared root tokens are exposed before the first RTADDR publication. Firmware
BME is not restored in this mode; drivers enable it through the admission API.
Default boot still publishes explicit passthrough. Requested translation fails
closed before activation for bridges, RMRRs, unavailable capabilities/storage,
and unqualified 1af4 devices (ivshmem 1af4:1110 is recognized separately).

The runtime unit gate covers table allocation, publication and the complete
context/IOTLB register sequence. It is a task-context atomic admission gate,
not a spinlock: contention can yield with a five-second bound and never holds
IRQ-disabled state across allocation. Boot contention refuses immediately.
All managed allocating/releasing entrypoints hold cooperative shutdown ownership
through the entire transaction. A refused map before mutation frees its new
backing; an unmap which cannot obtain proof pins the existing backing. Empty
exposed table pages remain owned for kernel lifetime and are counted through
atomic snapshots. Exact comparison with allocator accounting requires a
quiescent checkpoint.

Live invalidation checks the active legacy root, translation state and absence
of outstanding commands, then verifies actual global context and global IOTLB
granularities with read/write draining. Uncertain invalidation or corrupt tables
permanently closes the whole unit, inhibits BME on every associated requester,
and retains all uncertain data. PCI typed and raw command writes recheck
admission under the configuration lock, closing the enable-versus-quarantine
race. A raw write that raced closure has BME stripped; the initial diagnostic
check still rejects already-unadmitted writes outside the lock.

DMA unmap now receives the original physical extent and original allocation
permission, including alignment padding and the invalid-address unwind path.
Streaming rebind can narrow a submission direction without changing the PTE
permission used to verify eventual release. Prepare also rejects submissions
from previously allocated pools when their unit closes. IRQ/NAPI prepare,
bind and complete remain allocation-free and do not mutate page tables.

Host regressions exercise 100 consecutive real backend command sequences,
mode/root/busy preflight rejection, each timeout and bad granularity, permanent
refusal after uncertainty, original-permission release after rebind, aligned
physical versus returned-address unwind, and admission closure of existing
pools. The new ordinary KTEST inspects the active RTADDR/context/PTE hierarchy
independently of the software owner: distinct roots/DIDs, exact directional
leaves, unmapped neighbors and foreign-domain addresses, and cleared leaves
plus restored data ownership after release. In passthrough it explicitly reports
that active-translation coverage is unavailable.

Initial translated smoke boot passed 40 assertions, 300 exact e1000e echoes,
AHCI/e1000e/xHCI terminal retirement and normal power-off. Its ELF was
`6a1c0450bc2bcac96d7d30bd2f8b28110e2d9f2ad00cbe601e7fc230b39db639`;
evidence is retained under `build/goal20-stage12b/initial-smoke/` with the
original private `ktest-data/goal20-stage12b/smoke/` boot manifest. Two review
fixes then distinguished ivshmem from virtio admission and ensured the
passthrough branch of the new KTEST still executes an assertion. The final
ELF and full/SMP fixtures are separate; final evidence follows after completion.

Follow-up fault integration reconnaissance (not implemented in 12b):
`gates::allocate_vector/request_irq` can install a kernel-lifetime unit handler
before translation (IOMMU init is after interrupt/APIC setup). Map the validated
fault-record bank as well as command registers; FECTL/FEDATA/FEADDR/FEUADDR are
at 0x38/0x3c/0x40/0x44. CAP.NFR bounds the fixed record count. Intel VT-d rev.
4.0 sections 7.2/7.3 and 11.4.7 require FIFO processing from FSTS.FRI, stable
F-marked 128-bit records, RW1C clearing of F, and clearing overflow only after
pending records are serviced. Capture raw address, SID, read/write type,
reason and domain/root generation into bounded retained storage without logging
or allocation in IRQ context; report from task context. Mask fault delivery and
close admission immediately on uncertainty. Split admission closure from BME
inhibition so an IRQ-set closed flag does not accidentally suppress later
idempotent hardware inhibition. A unit gate owner must observe that closure
before returning a successful map/unmap proof. Fault-overflow and invalidation
error diagnostics must not infer missing records or recovery.

Controlled negative-DMA fixture strategy: use retained managed pages mapped to
a different requester (with guard bytes) as the deliberately forbidden address
in a selftest-only descriptor. Keep descriptor rings valid and retain all test
backing through reset/IRQ drain. A stale/unmapped case must retain its physical
backing after PTE revocation rather than release it to the allocator before
submitting the intentionally invalid request. Fresh private VMs isolate each
terminal unit fault. Positive traffic, a real fault record and unchanged guard
bytes are separate evidence. Withholding a completed invalidation proof tests
quarantine policy but is not a simulated hardware fault record or hardware hang.

12b final qualification: Normal Build WOS (`RelWithDebInfo`, current task
command shape), isolated `bin/wos-ktest --build-only` diagnostic build, and
format/tidy checks pass. Tidy has nonfatal style warnings. All 35 selected host
targets pass: 216 GTest cases (five new cases), 32 launcher cases, source guards
and DMAR fuzz smoke. The existing documented broader source-inventory baseline
failures were not hidden or changed.

The final rootless 32-CPU/32-GiB Intel-IOMMU translated boot passes 66,147
assertions with zero failures: all 1,172 enabled tests pass and three disabled
tests skip. Both active-domain tests pass, including independent active PTE
inspection and physical owner accounting. AHCI filesystem operations, 300 exact
e1000e packet echoes, xHCI terminal retirement and normal power-off complete.
A read-only QMP inspection during filesystem activity records FSTS=0 and
GSTS=0xc0000000 (translation enabled) in `live-unit-status.json`.

Additional final-ELF rootless fixtures:

| Fixture | Result | Scope |
| --- | --- | --- |
| Default hardware passthrough | 40 assertions, zero failures, 300 exact echoes, normal power-off | Existing explicit fallback behavior and native driver retirement |
| Translated four-CPU live e1000e | 326 exact host frames; RX 163/162, worker RX 103, IRQ 399, RX CPUs 0xa | Held callback survives shutdown yields, later exits; 514 regions/1,282 pages released after drain |
| Translated live withheld device-stop proof | 556 exact host frames; RX 278/278, worker RX 281, IRQ 688, RX CPUs 0xa | Same callback/IRQ/late-admission checks; all 514 regions/1,282 pages quarantined |
| Requested translation, no IOMMU | Explicit boot refusal before TE | No silent fallback |
| Requested translation, unqualified virtio | Explicit boot refusal before TE | ACCESS_PLATFORM gate |
| Requested translation, preparation allocation failure | Explicit boot refusal before TE | No published partial context set |
| Strict mode | Explicit boot refusal before TE | Full strict policy is still unavailable |
| Conflicting off + translated options | Explicit boot refusal before TE | Ambiguous policy cannot enable hardware |

The five negative boot cases were intentionally stopped through their own QMP
sockets after the expected refusal, not reported as normal shutdown/full-suite
passes. Both SMP fixtures reach the ordinary power finalizer and release AHCI
210 regions/13,068 pages. The withheld case is a driver retirement-proof test,
not live unit-invalidation uncertainty or a deliberately generated IOMMU fault.

Final ELF SHA-256:
`39e072e914a47cf5bf458cf66d52db3f7e9f774d089ea80c83a1a6d87a3016d5`.
Full coverage fingerprint:
`text_start=0xffffffff80000158 text_end=0xffffffff81843d36
text_fnv64=0x29d2932c93c37a19`. All nine boot manifests and serial hashes,
126 archived source files/workspace hashes, full runtime test inventory and
18 WOSDBG shared-backend load/search calls pass `validate-final.py`.
Source archive SHA-256:
`d010449e0a1ba7651af85f6b8a517fbf536dfe245b77b84824cc2e5acef0df3d`.
Evidence: `build/goal20-stage12b/validation.json`, runners/logs/archive in that
directory, and private fixtures under `ktest-data/goal20-stage12b/`. No sanitizer
failure report was found in the positive final logs. Final host audit finds
only unrelated QEMU PID 36201; all task-owned VMs have exited.

Goal 20 remains active. This checkpoint qualifies opt-in native translated DMA
on a flat QEMU topology, not general hardware isolation policy or strict mode.
Next implement/qualify fault IRQ diagnostics and actual stale/unmapped DMA,
including live unit-wide failed-invalidation quarantine. Then qualify virtio
ACCESS_PLATFORM, groups/RMRR and strict admission. Shared-BAR peer/ring work,
live multi-TRB USB payloads and necessary physical-hardware evidence remain.
Kernel-lifetime exposed table pages are deliberately retained; zero leaked
DMA data mappings is distinct from reclaiming live translation infrastructure.

## Stage 12c — fault capture and forbidden DMA

Previous goal turn classification: progress, verified by the 12b code, two
builds, 35 host targets and nine final-ELF rootless fixtures. No blocker exists.

Problem: translated domains lack fault IRQ capture/reporting and deliberate
forbidden-DMA evidence. IRQ-triggered closure must also race safely with unit
mutation and PCI admission without reclaiming any exposed resource.
Expected files to change: iommu/fault.hpp, domains.*, DMA diagnostic lookup,
init wrapper fault-worker startup, e1000 selftest hooks, host/kernel tests and
ledger/goal. Files already read: 12b runtime/manager/engine, PCI writers,
init phase registries and wrappers, gates IRQ ownership/allocation, scheduler
shutdown/task APIs, e1000 lifecycle/live harness and retained VT-d rev.4.0
fault-register specification sections 7.2/7.3/11.4.7.
Invariants to preserve: root/table/data lifetimes, immutable requester/domain
identity; close admission before reporting; no allocation, wait, recursive log
or PCI spinlock in fault IRQ; raw records never fabricate missing generations;
all failure paths pin data until independently proved retirement/invalidation.
Locks involved: existing unit task gate, DMA registry metadata lock and PCI
configuration lock; IRQ uses only atomic admission/publication of retained
unit-owned fault storage. Task reporting must not run under the unit gate.
Unsafe contexts: IRQ capture is bounded by CAP.NFR <=256, no reclaim/logging.
Allocation/blocking assumptions: fixed kernel-lifetime fault metadata/IRQ;
worker allocation only after scheduler queues exist, bounded work then sleep.
Test backing stays managed and pinned while foreign/stale DMA is attempted.
ABI/wire/syscall compatibility impact: none. Diagnostic-only descriptor misuse
is WOS_SELFTEST and token-gated; no userspace or WKI protocol changes.
Build/test plan: shared fault-register capture host models (ordering, overflow,
unstable/invalid records), both builds, full isolated KTEST, positive translated
SMP regression and private negative-DMA/invalidation-proof fixtures. IRQ count,
raw SID/address/type/reason, generations, BME inhibition and retained backing
must be independently checked. Faulted VMs may require intentional private-QMP
termination because AHCI shares the quarantined unit; do not claim normal
shutdown or preserved live I/O on that failed unit.
Rollback plan: retain default passthrough and remove only this stage's changes
if qualification fails. Never free published IRQ data/tables as rollback.

12c implementation: `iommu/fault.hpp` supplies the shared, bounded register-bank
capture algorithm. It starts from valid FRI, scans at most CAP.NFR records,
retains raw low/high fields and stability, writes only the F RW1C bit for a
stable record, and clears overflow only after pending records are serviced.
Unstable records, invalid register bounds/indexes and invalidation errors do
not fabricate successful recovery. The host model covers holes/overflow,
unstable records, all access encodings, invalid ranges and the full 256-record
bank (bounded 768 reads/257 writes in that fixture).

Each translated unit owns retained fault storage and an IRQ vector. MSI is
installed before TE and unmasked after activation. The IRQ masks further
fault delivery, closes admission and publishes immutable raw records with
bounded requester/DID/root-generation lookup; it never logs, allocates,
waits for a unit gate, takes PCI configuration locks or frees memory. The
phase-5 worker inhibits all unit requesters' bus mastering, then reports the
raw records and a task-context DMA-registry observation of allocation identity.
Foreign addresses are reported as unknown allocation generation rather than
attributed to another device. This registry lookup is not proof of a live PTE
or an extension of a buffer lifetime. Fault storage, exposed tables and the
installed fault handler have kernel lifetime; no rearm/recovery is claimed.

Admission closure and hardware inhibition use distinct atomic states so an
IRQ-set failed flag cannot suppress later BME inhibition. Invalidation checks
closure after register completion as well as before issuing commands. Existing
PCI config-lock admission rechecks prevent re-enabling a quarantined requester.
The worker holds shutdown ownership only while processing and yields outside
that hold. Early phase-3 AHCI faults can be captured before phase-5 reporting;
if boot stops earlier, retained fault metadata is the available evidence.

An initial integration placed worker creation in `sched_init` after queue
setup but before `start_smt` established BSP GS/per-CPU and init-task ownership.
The guest repeatedly reset at that boundary. That attempt is preserved under
`build/goal20-stage12c/pre-worker-phase/` (ELF
`dbc400c0efb3d7413a658786b7e3a720d773e7fef067e8b066ee8256ec472caa`)
and its private fixture is `ktest-data/goal20-stage12c/pre-worker-foreign/`.
It is failed evidence, not a boot pass. The final implementation creates the
worker as the first phase-5 module, after `start_smt`; the init metadata also
expresses the fault-worker dependency of network/USB/shared-memory drivers.

Coverage correction to 12b: its broad host-test report included a stale
`virtqueue_dma_test` executable. The full fresh rebuild in 12c exposed its
three-argument mock `unmap`, which no longer matched the five-argument exact
physical/direction API introduced in 12b. The mock is now corrected, and all
36 selected targets were rebuilt before the final run. The earlier statement
that all 35 results established current-source host coverage was too broad;
12b's actual kernel builds and guest evidence remain separately recorded.
The current 36-target run passes 223 GTest cases, 32 launcher cases, source
guards and DMAR fuzz smoke, with hashes for all 24 selected C++ executables.

The e1000 negative harness exists only under WOS_SELFTEST and consumes a
specific packet marker plus explicit command-line flags. Foreign-page and
stale-page cases retain managed guard backing, submit one deliberately invalid
TX read and require exact IRQ/SID/DID/address/access/reason evidence. The
stale case revokes an unsubmitted mapping before the intentional device read;
it never returns that backing to the allocator. A separate case withholds a
real completed invalidation proof and requires quarantine without inventing
a hardware fault. Every case checks unit-wide BME inhibition, refused new
admission/allocation, NIC IRQ drain and retained backing after retirement.

Final negative fixtures (all use the final ELF below):

| Fixture | Actual evidence | Retained NIC backing |
| --- | --- | --- |
| Foreign requester page | IRQ 1, SID 0x20, DID 5/root generation 1, address 0x1256e000, read reason 6, stable raw high 0xc000000600000020; fault reporter correctly says allocation unknown | 514 regions / 1,282 pages, all quarantined; guard bytes unchanged |
| Revoked, retained same-requester page | IRQ 1, same SID/DID/root generation and read reason, address 0x127d1000; allocation generation 729 known/quarantined | 515 regions / 1,283 pages, all quarantined; guard bytes unchanged |
| Withheld invalidation completion proof | Unit closed/inhibited, IRQ 0, no hardware fault report; allocation generation 729 retained | 515 regions / 1,283 pages, all quarantined |

Both real faults reported FSTS 0x2, one stable record and remaining status 0.
Their host echo counts before terminal fault were 327 and 323; the proof-withheld
case had 320. All three were intentionally stopped through their own QMP
sockets after evidence capture. They are not normal-shutdown results.
The foreign fixture also recorded a userspace page fault after the unit-wide
inhibition/PASS boundary. AHCI shares that failed unit and cannot continue DMA;
this observation is retained as a limitation, not dismissed as proven unrelated.
No claim of filesystem continuity or general operation after the fatal fault
is made. No kernel KASan/UBSan failure report appeared in those final logs.

The positive four-CPU regression completed 568 exact echoed frames (284 each
of 64 and 1,514 bytes), 268 worker RX events, 671 IRQs and RX CPU mask 0xa.
Its held callback deferred retirement, then exited; late entry was rejected,
IRQs drained and all 514 NIC regions/1,282 pages were released with zero
quarantine. The normal power finalizer also released all 210 AHCI regions/
13,068 pages. No userspace fault or kernel sanitizer failure appeared in that
positive log. The full-suite result is recorded separately after completion.

12c final qualification: Normal Build WOS (`RelWithDebInfo`), isolated
`bin/wos-ktest --build-only` diagnostic build, formatting/tidy and `git diff
--check` pass. Tidy retains nonfatal style warnings. The rootless 32-CPU/
32-GiB translated full suite completes 66,179 assertions, zero failures;
all 1,172 enabled tests pass and three disabled tests skip. This includes
healthy fault status assertions for each active domain, AHCI filesystem work,
300 exact e1000 packet echoes, xHCI retirement and normal selftest power-off.
No kernel sanitizer failure was found in the positive final logs.

Final ELF SHA-256:
`d0647323f8428e7535facf43f3d123cd0a2b04a309d8b24005f996871a9cab8d`.
Coverage fingerprint:
`text_start=0xffffffff80000158 text_end=0xffffffff8184f616
text_fnv64=0x322eaf8fdaae7a45`.
Source archive SHA-256:
`d0ea5ca320fd6b4517209e650d41d7bef40ac91ae378e5bb29013dc4ac6170e0`.
The five boot manifests/serial hashes, 128 source/archive/current hashes,
24 host executable hashes, full runtime inventory and ten WOSDBG shared-backend
load/search calls pass `build/goal20-stage12c/validate-final.py`.
The complete receipt is `build/goal20-stage12c/validation.json`; private
fixtures are under `ktest-data/goal20-stage12c/`. The final host process audit
finds only unrelated QEMU PID 36201, preserved unchanged. All task-owned VMs
exited. The initial failed worker-placement fixture is separate evidence.

Goal remains active. Real fault qualification here covers one QEMU VT-d unit
and forbidden reads; overflow, unstable records and maximum bank size are
model evidence. No physical-hardware, forbidden-write, fault recovery or
translation-infrastructure reclamation claim is made. Next implement and
qualify virtio ACCESS_PLATFORM; groups/aliases/RMRR, strict admission,
shared-BAR peer/ring workloads, live multi-TRB USB payloads and necessary
hardware evidence remain.

Next-stage reconnaissance: modern virtio currently accepts only VERSION_1 in
the upper feature word (`virtio_net.cpp: init_device_modern`); legacy fallback
occurs only on -ENOTSUP. VT-d preparation currently rejects all 1af4 devices
except shared-BAR ivshmem 1110. The shared node launcher accepts a model string
and currently does not add iommu_platform. [Virtio 1.2 section 6](https://docs.oasis-open.org/virtio/virtio/v1.2/virtio-v1.2.html)
defines ACCESS_PLATFORM (bit 33) for limited/translated device access; a driver
accepting it must configure translation or disable the IOMMU. A next patch must
coordinate central BME admission with successful modern negotiation before
allowing queue DMA, refuse unsafe legacy/unqualified paths in translated mode,
and prove actual translated device access with negative DMA as well as positive
traffic. Matching identity addresses alone cannot establish that an emulator
used its IOMMU path. No next-stage source changes have been made in this checkpoint.

## Stage 12d — virtio platform DMA admission

Previous goal turn classification: progress: implemented fault handling and
verified five final-ELF guests, both builds, fresh host binaries and full KTEST.
No blocker is pending.

Problem: virtio bypasses translated admission until modern ACCESS_PLATFORM is
negotiated; current all-virtio boot refusal prevents driver qualification.
Expected files to change: iommu/domains.*, virtio feature policy/net driver,
selftest hooks and host/kernel tests; private rootless fixture scripts.
Files already read: domain initialization/map/admission/fault paths, PCI command
writers, DMA backend admission, virtio probe/reset/retirement/queue submission,
init registry, NAPI ownership, current KTEST and host test entrypoints; kernel,
critical-path, subsystem-index and logging instructions. The shared node
launcher already accepts explicit virtio model options for private fixtures.
Invariants to preserve: central BME gate rejects unnegotiated translated virtio;
all data addresses still originate from DMA registry; reset revokes transport
qualification; feature rejection cannot silently fall back to legacy translated
DMA; quiesce/IRQ drain and invalidation precede backing release. Fixed IRQ state
and unit quarantine still override transport qualification.
Locks involved: PCI configuration admission lock, existing queue/callback locks,
unit mutation gate and DMA registry lock unchanged; transport qualification is
atomic and read-only under PCI lock. Probe/reset callers serialize one device.
Unsafe contexts: production feature negotiation/reset in boot/task context;
no new allocation/blocking in packet/IRQ/NAPI paths. Negative selftest only
uses a retained page and a token-gated descriptor substitution.
Allocation/blocking assumptions: prepare maps before BME is admitted; existing
reset waits remain bounded; failed hardware proof pins backing.
ABI/wire/syscall compatibility impact: no userspace/WKI ABI change. Explicit
modern virtio ACCESS_PLATFORM bit negotiation changes device contract only.
Build/test plan: feature-policy matrix and central pre-negotiation admission
KTEST; normal/isolated builds; translated modern packet/control/probe retirement,
SMP live traffic, negative forbidden DMA and missing-feature/legacy refusal;
existing passthrough regression and full KTEST. Exact ELF manifests retained.
Rollback plan: restore all-virtio translated refusal; keep prior native/fault
implementation and lifetime invariants. Never release exposed resources to roll
back a failed live fixture.

12d implementation: modern virtio negotiates VERSION_1 plus ACCESS_PLATFORM
when offered. A translated domain requires both; unknown upper bits are never
accepted. Successful FEATURES_OK precedes a driver assertion to the central
atomic per-requester platform-admission gate. DMA preparation remains separate
from BME admission, so rings/pools can be mapped before hardware is permitted
to run. Every PCI command writer still checks central admission; unit quarantine
independently overrides the transport flag. Reset revokes that flag before
writing status zero; it is not itself proof that outstanding DMA has stopped.
Existing protocol reset, BME readback, callback/IRQ drain and invalidation
continue to precede backing release.

Translated preparation now permits supported virtio-net PCI IDs to reach the
modern probe with BME initially denied. Unsupported virtio device types remain
boot-refused. Modern feature refusal cannot fall through to legacy DMA; legacy
probe itself is refused in translated mode. Explicit passthrough keeps its
observable behavior and also accepts ACCESS_PLATFORM when offered. No launcher
production changes are needed: the existing model string supports the private
fixture's `virtio-net-pci,mq=on,disable-legacy=on,iommu_platform=on` options.

The selftest descriptor substitution runs only under WOS_SELFTEST, requires a
specific 64-byte marker, and happens under the TX queue lock before publishing
the avail index. Its override is cleared even when submission fails. The same
real IRQ/NAPI sender first verifies small and jumbo echoes, then the task-only
harness attempts a foreign-page or revoked-but-retained-page read. Uncertain
backing is kept through reset/IRQ drain. The separate withheld-invalidation
case never synthesizes a hardware fault. No normal build includes this hook.

Host qualification passes 37 selected targets and 226 GTest cases, including
three new feature-policy cases (missing modern/platform bits and unknown-bit
rejection); all 25 C++ executable hashes were recorded after rebuilding the
selected targets. Source checks cover negotiation -> FEATURES_OK -> admission
-> mapped queue setup -> BME ordering, reset revocation and rejected legacy
fallback. Kernel probe tests also verify admission remains closed after reset
or feature refusal and that the hardware BME command bit stays clear.

Two test-integration corrections are retained explicitly:

- The first focused probe used a multicast echo helper on port 47921 while its
  VM used 47922. It completed platform/control negotiation, reset and zero-map
  retirement but failed the two-packet assertion, then panicked by test policy.
  It was stopped through its own QMP and archived under
  `build/goal20-stage12d/failed-probe-echo-port/` and the matching private fixture.
  The corrected runner uses 47922 and passes with the same kernel ELF.
- Review of the new pre-negotiation KTEST found an invalid raw command-write
  attempt: unauthorized raw writes deliberately panic. This was removed before
  the full suite ran; the fallible typed API rejection and actual BME readback
  remain. It was never treated as a completed full-suite result. The targeted
  qualification ELF and final full-suite ELF differ only in
  `modules/kern/src/test/iommu_domains_ktest.cpp`; production sources are identical.
  Separate source archives and ELF/manifests preserve that distinction.

The earlier initial live/fault smoke tests use ELF
`fc0b615104d263a32af2d47d5619c48ae18cea71dcdbc198f8222277c53e7833`,
retained under `build/goal20-stage12d/initial/` and private `initial-*` fixtures.
They passed but are not substituted for the later targeted qualification below.

Targeted qualification ELF:
`6d25d8fc6f55db241cf0c857f7354849294e3899f8c2c3d759266f14ab1c76bf`.
Its source and ELF copies use the `qualification-` prefix in
`build/goal20-stage12d/`.

| Fixture | Evidence |
| --- | --- |
| Translated modern probe/control/packets/terminal reset | 44 assertions, zero failures; FEATURES_OK/ACCESS_PLATFORM, control ACK, 64/9,014-byte echoes; 516 regions/2,059 pages released, post-reset admission closed, normal power-off |
| Missing ACCESS_PLATFORM | 35 assertions, zero failures; modern probe refused, BME denied, zero mappings, normal power-off |
| Legacy virtio | 35 assertions, zero failures; legacy translated probe refused, BME denied, zero mappings, normal power-off |
| Translated four-CPU live traffic | 235 exact host frames; RX 118/117, worker RX 189, IRQ 382, RX CPU mask 0xa; held callback/late admission/IRQ drain checks; all 514 regions/2,056 pages released; normal finalizer and AHCI release |
| Hardware-passthrough four-CPU live traffic with ACCESS_PLATFORM | 129 exact host frames; RX 64/64, worker RX 57, IRQ 110, RX CPU mask 0xa; same ownership/retirement checks and zero remaining data regions; normal finalizer and AHCI release |
| Foreign-page read | 128 exact positive frames, then real VT-d IRQ 1; SID 0x20, DID 5/root generation 1, address 0x14450000, read reason 6; allocation correctly unknown to NIC; all 514 regions/2,056 pages pinned/quarantined |
| Revoked-but-retained-page read | 129 exact positive frames, then real VT-d IRQ 1; same SID/DID/root generation/read reason, address 0x12476000; allocation generation 729 identified; all 515 regions/2,057 pages pinned/quarantined |
| Withheld invalidation completion proof | 128 exact positive frames; unit failed/inhibited, no hardware IRQ or fault report, allocation generation 729 retained; all 515 regions/2,057 pages pinned/quarantined |

Both real faults captured one stable record with FSTS 0x2 -> 0 and raw high
0xc000000600000020. All three negative cases verified new admission/allocation
refusal, unchanged guard backing, NIC IRQ drain and unit-wide BME inhibition.
They were intentionally stopped through their own QMP sockets; they do not
claim normal power-off or continued filesystem operation on the shared failed
AHCI unit. Final targeted logs contain no kernel sanitizer report or userspace
page fault. Positive finalizers release AHCI 210 regions/13,068 pages.

Full-suite final ELF:
`fd54e3cc4f64c7b911b2fd048a9111e39d08e4de632437d02bf3005a505179b2`.
Normal Build WOS (`RelWithDebInfo`), isolated diagnostic build and format/tidy
checks pass after the test correction. Tidy retains nonfatal style warnings.

Next-stage reconnaissance: PCI config access currently uses CF8/CFC and
8-bit offsets only; no MCFG/ECAM or extended-capability reader exists.
`scopes::qualify` validates topology, DRHD ownership and RMRR scope membership,
but explicitly makes no ACS, requester-alias or reserved-memory ownership
claim. Intel VT-d rev.4 sections 3.12.1/3.12.2 require identical context entries
for bridge aliases sharing a domain; 3.12.5 also covers enabled phantom
functions. Root-complex integrated requesters are distinguished by SID
(section 3.12.4). Section 3.16 requires scoped identity RW mappings for reported
reserved-memory uses and protection against conflicting software IOVAs.
The [Linux VFIO group description](https://docs.kernel.org/driver-api/vfio.html)
also explains why a bridge or multifunction enclosure can reduce isolation
granularity. These are requirements for the next implementation, not proof
that current per-requester roots isolate such topologies. The current bridge/
RMRR and strict-policy refusals remain. ACPI is initialized before PCI; a
bounded MCFG reader can reuse the existing checked firmware-table lookup.
RMRR validation must establish reserved physical ownership before any mapping:
MM initializes in phase 0, long before DMAR discovery, so later blindly reserving
an already-allocatable physical range would be unsafe.

12d final full-suite result: rootless 32-CPU/32-GiB translated KTEST passes
66,185 assertions with zero failures; all 1,172 enabled tests pass, three
disabled tests skip. The central pre-negotiation admission and active PTE tests
pass. Final virtio control/packet/terminal tests verify two exact frame sizes,
control ACK, 516 regions/2,059 pages released and admission closed after reset.
AHCI/xHCI final retirement and normal selftest power-off complete. No sanitizer,
userspace page-fault or panic report appears in the nine final fixture logs.
Coverage fingerprint:
`text_start=0xffffffff80000158 text_end=0xffffffff818576f6
text_fnv64=0x3dc6a25500708f26`.

`build/goal20-stage12d/validate-final.py` passes: nine boot manifests/serial
hashes, both 130-file source archives and their exact test-only difference,
current workspace hashes, 25 host executable hashes, full runtime inventory
and 18 WOSDBG shared-backend load/search calls. Final source archive SHA-256:
`50bedc421a387e61cf84d51d13fbe88e1847a633d6c4f9c036902663c28b26f5`.
Targeted qualification archive SHA-256:
`d90d9d929a2fcb4d473048b4b272a8e2a0bc21e9275c677c6baedf76410b2adc`.
Receipt: `build/goal20-stage12d/validation.json`. All private task VMs have
exited; final host audit finds only unrelated QEMU PID 36201. Final
`git diff --check` passes. The initial smoke and failed echo-port probe are
preserved separately, not counted among the nine final qualifying fixtures.

Goal 20 remains active. Virtio platform negotiation and translated native
traffic/fault paths are now qualified in these QEMU fixtures. Requester
groups/aliases/RMRR, strict admission, shared-BAR peer/ring workloads, live
multi-TRB USB payloads and required physical-hardware evidence remain. There
is no forbidden-write, fault recovery or translation-infrastructure reclamation
claim. The next implementation should establish bounded extended PCI
configuration and grouping/alias evidence before relaxing bridge admission.

## Stage 13a — bounded extended PCI configuration and isolation inventory

Previous goal turn: progress, verified by 12d implementation, both builds,
37 rebuilt host targets, nine qualifying guests and full KTEST. No blocker.

Reconnaissance/impact map: ACPI `find_table_bounded` supplies checked immutable
MCFG bytes after phase-2 ACPI init. PCI currently enumerates segment 0 through
CF8/CFC, with stable parent-before-child registry entries; all config offsets
are uint8_t. VT-d `discover` consumes that registry and `scopes::qualify`
resolves DRHD/RMRR ownership. No extended capability reader exists. New pure
MCFG/address/capability models feed boot-only per-device MMIO snapshots; later
grouping will consume those snapshots. The production legacy config API and
all driver DMA/IRQ paths retain their existing behavior in this prerequisite.

Problem: ACS, ATS/PRI/PASID/SR-IOV and requester-alias evidence cannot currently
be inspected safely, so removing blanket bridge refusal would be unsupported.
Expected files to change: new pci_ecam model/runtime, PCI public boot inventory
entrypoint/integration, host and kernel tests, CMake registrations, ledger/goal.
Files already read: PCI enumeration/config writers and header, bounded ACPI
reader/model, boot memory-map API and physical allocator reservations, init
phase registry, IOMMU discovery/scopes/domain initialization, kernel build and
host-test registration, workflow/index/kernel/critical/logging/build/test
instructions. External references: Linux ACPI PCI host bridge documentation
(MCFG base denotes bus 0 even for a nonzero start bus), Linux PCI ACS handling,
and retained Intel VT-d rev.4 sections 3.12/3.16.
Invariants to preserve: no config access beyond validated function/window;
malformed/truncated/ambiguous tables and cyclic capability lists fail closed;
no RAM cache-attribute changes; identity matches the stable legacy-enumerated
BDF; no BME/ACS/ATS control changes merely from inventory collection; no false
claim that readable ACS metadata proves active isolation or alias ownership.
Locks involved: serialized BSP boot collection, existing PCI config lock for
legacy identity reads; MM mapping outside config lock. Published metadata is
immutable and kernel-lifetime. No MMIO access from consumers after publication.
Unsafe contexts: collection only after MM/ACPI/PCI init, before driver probes;
no IRQ/NAPI/scheduler path additions. Fixed bounded storage and walks.
Allocation/blocking assumptions: mapping may allocate page tables during boot;
map only each known function's 4-KiB page, not an entire 256-MiB aperture.
ABI/wire/syscall compatibility impact: none. Segment 0 enumeration unchanged;
other MCFG segments can be validated but are not claimed as enumerated devices.
Build/test plan: malformed MCFG/address overflow/overlap and capability-chain
models; current-source host binaries, both builds, full isolated KTEST; rootless
flat and bridge fixtures compare real ECAM IDs and capability evidence against
legacy config and QMP topology. No physical-hardware evidence is assumed.
Rollback plan: remove only inventory integration, retain bridge/strict refusal
and previous qualified translated paths; inventory changes no device controls.
Minimal strategy: first validate MCFG and bounded function snapshots, then
implement group/alias selection and shared domain ownership from evidence in
13b. RMRR ownership/admission remains a separate required follow-up; do not
infer allocator reservation from a firmware mapping request.

13a implementation: `dev/pci_ecam.hpp` validates bounded MCFG records, bus-zero
base arithmetic, checksum/reserved fields, physical and bus overlaps, CPU/HHDM
bounds and boot-memory-map exclusion. Its fixed-size capability walker checks
legacy vendor/device/header identity before following conventional and extended
capability chains; cycles, duplicates, invalid offsets, unsupported versions
and inaccessible reads return no partial evidence. It records PCIe type,
ACS capability/control, ATS, PRI, PASID, SR-IOV and device-control fields.
Conventional devices never receive speculative extended-capability reads.

`dev/pci_extended.{hpp,cpp}` maps only known function pages after complete PCI
enumeration in `pci.cpp`, before probes. Immutable, kernel-lifetime snapshots
publish with release/acquire ordering; repeated collection performs no MMIO.
Collection is serialized BSP boot work, with no IRQ/NAPI additions or device
control writes. A valid window inventory does not imply every function is
readable: consumers must check the individual snapshot state. Nonzero MCFG
segments are validated but not claimed as enumerated by segment-zero PCI.
No bridge, RMRR or strict refusal is relaxed by this observation-only stage.

Host registration adds `pci_ecam_test.cpp` (nine cases) and
`pci_ecam_fuzz.cpp`; `pci_ecam_ktest.cpp` verifies retained identity/capability
headers against legacy config, pointer ownership and idempotent publication.
Normal Build WOS (`RelWithDebInfo`), isolated KTEST (`Debug`), formatting and
39 selected rebuilt host targets pass (235 GTest cases, 27 executable hashes).
A separate 50,000-input capability/MCFG fuzz run passes; the short CTest fuzz
smoke is not substituted for that run.

Rootless live inventory fixtures with the same ELF cover flat Q35 (eight
devices), a PCIe root port (nine), a PCIe-to-PCI bridge (ten), and a no-MCFG
legacy machine. Each readable ECAM identity matches independent QMP physical
reads and topology. The root port reports ACS capability `0x5f` but control
`0x0`; capability presence alone therefore cannot justify isolation. The
PCIe-to-PCI bridge is type 7, with a conventional requester on secondary bus
2; the no-MCFG case records unavailable windows/functions without guessed
addresses. These four early inventory fixtures were intentionally QMP-stopped,
not normal selftest power-offs. Bridge fixtures use a private QEMU wrapper;
production launcher behavior is unchanged. QMP independently checks identity
and topology, not ACS control values.

13b impact-map refinement from current runtime: `domains.cpp` indexes
translation roots by device, assigns one DID per device, and resolves fault
generation through that same index. Shared groups must introduce a canonical
group owner for root mutation and generation, populate every alias context
with the same DID/root, and refuse groups spanning incompatible remapping
units. The fault worker currently scans all matching DIDs and overwrites its
mapping result; that must become unambiguous source/group-aware attribution
before sharing DIDs. Per-device DMA allocation ownership must remain distinct
from hardware group visibility. Existing unit gate, shutdown ownership,
quarantine/BME inhibition and proof-before-release rules remain mandatory.

13a final runtime: the 32-CPU/32-GiB translated full KTEST passes **66,214
assertions, zero failures**; all **1,173 enabled tests** pass and three disabled
tests skip. The retained PCI snapshot test passes, as do directional PTE,
AHCI, e1000 and xHCI terminal tests. The host verifies 300 exact 64/1,514-byte
Ethernet frames. The NIC releases 514 regions/1,282 pages; xHCI releases four
regions/four pages; both report BME off. Normal selftest power-off completes.
No sanitizer, userspace page-fault or panic report appears in the five logs.

One exact ELF is used throughout:
`db21b3fa6dac7bd8a97fbb658409209678b7da1b56fe3243a20b16e168209994`.
Coverage fingerprint: `text_start=0xffffffff80000158
text_end=0xffffffff81861f96 text_fnv64=0xc3fcdc6aa7fee191`.
`build/goal20-stage13a/validate-final.py` passes current/archive hashes for
136 source files, five boot manifests and serial hashes, 27 host executable
hashes, the full runtime inventory, and ten WOSDBG shared-backend load/search
calls. Source archive SHA-256:
`4a24fa2d4048d0fb7cdb7ff6aac835cf33f9deed7a8752d0a84a299b2d65ec3e`.
Receipt: `build/goal20-stage13a/validation.json`. Final host audit finds only
unrelated QEMU PID 36201; every private fixture has exited.

Goal 20 remains active. This completes the bounded configuration prerequisite,
not requester grouping or strict isolation. Stage 13b must implement and
validate group/alias domain ownership and fault attribution; RMRR ownership,
strict admission, shared-BAR peer/ring traffic, live multi-TRB USB payloads
and required hardware qualification still remain. No blocker was encountered.

## Stage 13b — requester groups and alias contexts

Previous turn: progress, with 13a source/runtime inventory and complete passing
validation. Current-source reconnaissance confirms one root/DID per device in
`iommu/domains.cpp`, immutable bounded snapshots in `pci_extended.*`, and
per-device generation handles in `dma/manager.hpp`. Intel VT-d rev.4 section
3.12 requires secondary-bus:00 aliases for PCIe-to-PCI bridges, bridge-SID aliases
for conventional bridges and identical context entries for their domains.
Root-complex integrated requesters remain distinguishable by SID. Local scope
validation already proves bounded parent-before-child topology and DRHD owner.

Problem: blanket bridge refusal prevents managed translated DMA behind bridges;
simply removing it would omit aliases and misattribute shared-DID faults.
Expected files to change: new pure iommu/groups.hpp and host tests, domains
runtime/header, domain KTEST, host registration, this ledger/goal.
Files already read: workflow/index/kernel/critical/logging/build/testing
instructions, PCI enumeration/admission, extended config model/runtime,
DMAR discovery/scopes, domains and fault paths, DMA backend/manager interfaces,
active-root KTEST, host CMake and current build task. Intel rev.4 3.12 and
upstream Linux pci_acs_enabled corroborate the topology/ACS distinctions.
Invariants to preserve: all aliases in a group get the identical DID/root;
cross-unit ambiguity refuses admission; malformed or unavailable required
metadata and enabled unsupported requester modes refuse translation; group
visibility is explicit and never called per-function isolation. Allocation
handles remain per-device. Invalidation and quiesce/IRQ drain still precede
release; faults close the unit before deferred diagnostics. No ACS/control
writes in this stage. Strict and RMRR refusal remain until separately qualified.
Locks involved: immutable boot group publication; existing per-unit mutation
gate and shutdown ownership; existing PCI config lock; DMA registry lock only
in deferred fault lookup. No new lock nesting or allocation in fault IRQ.
Unsafe contexts: group construction and context allocation are BSP boot only;
fault capture performs bounded retained lookup, with no logging/allocation.
Allocation/blocking assumptions: one translation root per canonical group;
bounded fixed group/requester metadata, context pages only for actual aliases.
ABI/wire/syscall compatibility impact: none; kernel domain metadata extends.
Build/test plan: pure topology/group/alias/refusal cases; real active-context
identity and directional mappings across every group; normal/isolated builds;
rootless flat and bridged translated traffic/retirement, plus runtime audit.
Rollback plan: restore blanket bridge refusal and per-device canonical owners;
never reclaim exposed tables. Existing explicit fallback remains observable.
Minimal implementation: union nonisolating bridge branches/multifunction peers,
install original and bridge alias SIDs once, share roots by canonical member,
resolve faults through retained requester metadata, and update KTEST to verify
visibility exactly at group boundaries. Physical hardware evidence remains
unknown; QEMU checks do not substitute for final strict qualification.

13b implementation now present: `iommu/groups.hpp` constructs bounded canonical
groups from qualified topology/configuration evidence, unions nonisolating
bridge branches and external multifunction peers, and retains original plus
bridge-alias SIDs. Alias collisions merge ownership and preserve ambiguous
source attribution; cross-unit groups and enabled Phantom/ARI/ATS/PRI/PASID/
SR-IOV requester modes refuse admission. Missing ACS controls conservatively
merge branches; this does not claim that requester spoofing is prevented.
Root-complex integrated requesters retain distinct SIDs per VT-d 3.12.4;
physical strict admission must still qualify platform/topology assumptions.

`domains.cpp/.hpp` allocate one root/DID per canonical group, install identical
contexts for every original/alias SID, route mutations to that root and expose
member counts. Bridge forwarding BME is enabled only after all contexts are
active; endpoint admission remains driver-owned. The fault IRQ resolves
retained requester metadata without allocation/locks. Deferred fault lookup
finds allocation ownership across group members, rejects ambiguous matches
and logs allocation owner separately from alias-source ambiguity. The PCI
inventory diagnostic now explicitly delegates admission to IOMMU policy.

The expanded domain KTEST checks every original/alias context, every member's
directional leaves, visibility outside the group, separate per-device DMA
registry ownership, release and physical-page accounting. Review corrected
an overly broad adjacent-page assumption before runtime qualification: AHCI
can already own neighboring pages, so the test verifies their exact existing
mapping/permission and preservation across release instead of requiring zero.
It still requires zero for neighbors with no known owner. The current test
fixtures have one remapping unit; its DID comparison should be scoped to a
unit when adding multi-unit runtime qualification, since DIDs are unit-local.

Normal Build WOS (`RelWithDebInfo`), isolated diagnostic build (`Debug`) and
format checks pass. All 40 selected rebuilt host targets pass (244 GTest cases,
28 executable hashes), including nine new grouping cases. Frozen qualification
ELF: `47989be3d5cc6870234cc145caa3cb248f138c1b99d32eff21c857c1a15bfbdb`;
138 source files are archived under `build/goal20-stage13b/`.

Rootless root-port live traffic passes 392 exact frames, NIC IRQ/NAPI/SMP
retirement and normal shutdown, releasing NIC 514 regions/1,282 pages and AHCI
210 regions/13,068 pages. A bridged stale-read fixture passes with real VT-d
fault SID `0x200`, DID 3, domain generation 1, read reason 6 and IRQ count 1.
Deferred diagnostics report allocation generation 729, allocation owner 4,
`source_alias=1 matches=1`; admission closes and BME is inhibited. All 515 NIC
regions/1,283 pages remain quarantined and guard bytes are unchanged. The
negative fixture is intentionally QMP-stopped. Its shared failed unit also
inhibits AHCI; a journald userspace page fault follows, so continued filesystem
availability is not claimed.

The full bridged guest has already passed both domain KTESTs. Independent QMP
reads in `qmp-alias-contexts.json` prove identical actual contexts for SIDs
`0x10`, `0x100`, `0x200` and `0x218`, a different unrelated SID-0 root, and no
enumerated function at alias `0x200`. Remaining bridged tests and the queued
flat full suite are still running in launcher session 13906; final runtime
counts/audit are pending, not claimed as passed. Prepared final verifier:
`build/goal20-stage13b/validate-final.py`.

Next-stage reconnaissance: `phys.opt.cpp::init` excludes every non-USABLE boot
map range before building allocator zones, and retains firmware reserve
accounting. DMAR parsing already validates inclusive page-aligned RMRRs, but
that alone does not establish RAM ownership. A later mapper must prove complete
safe firmware-reserved coverage, reject allocatable/kernel/table/MMIO conflicts
and install scoped identity RW leaves before activation, with explicit retained
reserved accounting. It must not create a late reservation in already-live
allocator RAM. ACS/strict, RMRR, remaining device workloads and physical
qualification remain required; the goal is active and no blocker exists.

13b full bridged regression has completed: 67,957 assertions, zero failures,
all 1,173 enabled tests pass and three disabled tests skip, with normal power-off.
The flat full regression is now running in the same session 13906. Final
combined validation remains pending.

13c preparatory patch contract (independent ownership model only, runtime not
changed while 13b qualification runs):
Problem: RMRR syntax/scope validity does not establish physical ownership.
Expected files to change: initially private reserved-range model/test artifacts
under `build/goal20-stage13c`, later production model and runtime integration.
Files already read: DMAR/scopes/groups, domain preparation and page storage,
bounded ACPI reader, boot memory-map provider, physical allocator init/reserve
classification, Intel VT-d 3.16, current QEMU IOMMU properties/ACPI table builder.
Invariants to preserve: complete safe reserved coverage; no gaps, address
overflow, allocatable/kernel/table overlap or known MMIO conflicts; no late
reservation of allocator RAM and no inferred DMA release proof.
Locks involved: none in pure model; immutable bounded input spans.
Unsafe contexts: production consumption will be serialized boot only; not IRQ.
Allocation/blocking assumptions: no heap allocation or per-page walking in model;
all record counts/walks bounded. Runtime table allocation remains separate.
ABI/wire/syscall compatibility impact: none.
Build/test plan: reserved/NVS coverage, unsorted/overlapping descriptors, holes,
protected conflicts, overflow, width, alignment and explicit input limits.
Rollback plan: retain current RMRR refusal until production qualification and
identity mapping tests pass. This model alone cannot authorize a live mapping.

Fixture reconnaissance: installed QEMU exposes no RMRR-specific IOMMU option.
Its upstream `hw/i386/acpi-build.c` adds user ACPI tables after generated DMAR,
so a plain extra `-acpitable` does not replace DMAR and would trigger WOS's
duplicate-table refusal. A private early-boot debugger fixture can instead
reserve dedicated pages in the Limine memory map before `phys::init`, supply
one replacement DMAR pointer in the root table with corrected checksums, and
exercise normal bounded discovery/ownership paths. No production bypass or
late allocator mutation is needed. This fixture is not yet implemented or run.

Private 13c ownership prototype: `build/goal20-stage13c/reserved.hpp` and
`reserved_test.cpp` now pass eight tests with host AddressSanitizer/UBSan.
The model accepts complete reserved/NVS coverage, rejects conflicting protected
descriptors even when reserved coverage also exists, detects one-byte gaps,
validates widths/alignment/overflow and excludes intersecting MMIO ranges.
Its descriptor-union walk is bounded and allocation-free, independent of the
number of mapped pages; the worst-order 4,096-descriptor test completes in
26 ms in this host run. This prototype is not registered in production or
included in the 13b kernel/138-file archive; it does not yet authorize any RMRR
mapping. Production classification, complete MMIO exclusions, group mapping
plan, retained firmware accounting and live firmware-fixture tests remain.

13b final combined validation passes. Bridged and flat full suites report
**67,957** and **67,339** assertions respectively, both with zero failures;
each passes all 1,173 enabled tests, skips three disabled tests and powers off
normally. Both verify 300 exact Ethernet frames. The flat `e1000e` fixture
releases 514 NIC regions/1,282 pages; the bridged legacy `e1000` fixture lacks
the supported stop proof and correctly retains those 514 regions/1,282 pages
in quarantine. Its green terminal test verifies refusal/pinning, not successful
release. AHCI and xHCI retirement succeed in both full suites. The separate
root-port `e1000e` live fixture proves clean release with shared group roots.

`build/goal20-stage13b/validate-final.py` passes four boot manifests/log hashes,
the independent QMP alias-context receipt, all 138 archived/current source
hashes, 28 rebuilt host binary hashes, both complete runtime inventories and
eight WOSDBG shared-backend load/search calls. Final receipt:
`build/goal20-stage13b/validation.json`. Source archive SHA-256:
`eef457206eeb5f2d7a04ffedb6f4667937a8563ff3470852b680615108dd4f54`.
Both full coverage fingerprints match the same ELF:
`text_start=0xffffffff80000158 text_end=0xffffffff8186a8a6
text_fnv64=0xa134363b8ae5a7f4`. All task-owned guests have exited; final host
audit finds only unrelated QEMU PID 36201. No sanitizer or panic report appears
in the four qualifying logs. The negative stale-fault fixture's previously
recorded userspace fault remains an explicit limitation.

Goal 20 remains active. Group/alias context ownership is qualified for these
fixtures; strict isolation, RMRR integration, remaining ivshmem/USB workloads
and physical qualification remain. The private 13c prototype is preparatory
work, not a claim that reserved mappings are implemented. No blocker exists.

## Stage 13c — reserved-memory ownership and identity mappings (in progress)

Previous turn: progress; full 13b verification completed and the private
reserved-coverage prototype passed. No live task VM remains.

Production patch contract:
Problem: reported RMRRs currently cause blanket translated-mode refusal;
syntax/scope validation alone cannot authorize late access to allocator RAM.
Expected files to change: reserved model/runtime headers and implementation,
domains preparation/admission/accounting, domain KTEST, host reserved tests and
registration, ledger/goal. Existing PCI BAR sizing is reused only at BSP boot.
Files already read: prototype/tests, DMAR/scopes/groups, domains/PTE/storage,
DMA ownership backend, PCI config/sizing, extended inventory, boot MM/physical
allocator and direct-map policy, bounded ACPI reader, platform APIC/HPET/MMIO
interfaces, prior 13b receipts and the relevant project instructions.
Invariants to preserve: firmware-backed identity RW mappings only for scoped
groups; complete already-reserved coverage and no known MMIO/protected overlap;
no late allocator reservation, no freeing firmware pages, no map/unmap collision
with retained RMRRs; aliases share identical roots. Data-buffer ownership,
quiesce, IRQ drain, synchronous invalidation and failure quarantine are unchanged.
Locks involved: serialized BSP preparation, existing PCI config lock per access;
BAR sizing only after all requesters' BME readback is clear. Live mapping keeps
the existing unit gate/shutdown ownership. No new IRQ lock or allocation.
Unsafe contexts: preparation and classification are boot only. Static retained
plan lookups are bounded; no firmware parsing or BAR probing in runtime paths.
Allocation/blocking assumptions: bounded static boot-map/exclusion/plan arrays;
normalized ranges map in bounded chunks before root exposure. Prepared failure
unwinds only unpublished tables; firmware physical storage never belongs to the
DMA allocator. Exposed translation infrastructure keeps kernel lifetime.
ABI/wire/syscall compatibility impact: none; kernel domain statistics gain
separate firmware-reserved mapping accounting.
Build/test plan: production coverage/normalization/group tests; both builds;
rootless controlled early-boot firmware fixtures for valid, overlapping and
unsafe RMRRs, independent active-PTE checks and ordinary translated regression.
Rollback plan: restore blanket RMRR refusal; do not free firmware/exposed storage.
Policy limitation: ACPI NVS remains protected in production until exact table
ownership exclusions are available, because NVS can contain live FACS/tables.
The pure model's NVS category is not itself production admission. Unsupported
ownership must fail explicitly; strict admission remains separately unfinished.

13c implementation and initial qualification (2026-09-23):

- `reserved.hpp` now supplies the production bounded coverage and group-plan
  model. `reserved_runtime.cpp` classifies the immutable Limine map, excludes
  platform/PCI MMIO, and sizes/restores BARs only on serialized BSP boot with
  every PCI BME bit already read back clear. Only already-RESERVED descriptors
  qualify in production; NVS and every allocator/kernel/bootloader/ACPI class
  remain protected. Missing or unsupported ownership evidence refuses admission.
- Group plans coalesce overlap/adjacency, enforce address widths and a 256 MiB
  per-group bound, and install identity RW leaves before root exposure. Firmware
  pages have separate retained accounting; driver map/unmap cannot claim them
  or expose their physical backing through another group. Firmware storage is
  never allocated or freed by the DMA allocator. Prepared failures destroy only
  unpublished translation infrastructure; hardware-visible lifetime rules stay
  unchanged. No userspace, syscall or wire ABI changes.
- Normal `Build WOS` (RelWithDebInfo), isolated Debug KTEST build and formatting
  checks pass. The selected host regression passes 41 targets, including 257
  GTest cases in 29 rebuilt binaries and the source/fuzz checks. Thirteen new
  host cases cover reserved-range ownership, gaps, overflow, bounded union
  coverage, group normalization, per-root accounting and output cleanup.
- The private guest fixture stops at `mm::init()` before allocator consumption,
  carves 64 KiB from original USABLE firmware handoff memory, and replaces the
  handoff with a copied descriptor list. Fourteen pages hold bootloader-class
  table metadata and two payload pages are reserved (NVS in that negative case).
  Checksum-valid copied RSDP/root/DMAR tables supply scoped RMRRs through normal
  discovery. This edits private guest firmware inputs, not production kernel
  code or the frozen ELF. Original/modified maps and tables are retained in
  `build/goal20-stage13c/firmware-*.json`.
- Early live `valid` and `overlap` fixtures admit respectively one and two
  firmware pages. Independent stopped-guest QMP walks confirm identity RW only
  under SID 00:04.0/group 4, zero adjacent leaves and zero corresponding leaves
  in every other group. Overlapping RMRRs normalize to one mapping. Deliberate
  USABLE RAM and NVS ranges reject with protected-memory error 5; ECAM rejects
  with MMIO error 6. QMP confirms TE=0 and BME clear on all eight functions in
  each refusal. These are intentional QMP stops, not full selftest passes.

The current 13c ELF is
`e2e1e94c702cd694b87373e6f8748b95cd8698a994b39ab6c088f14cac05ab31`;
142 source files are frozen beside it. Full-suite and allocation-unwind
qualification are still running/pending at this entry. The first private wrapper
also intercepted guestfish capability probes, causing nonfatal SSH-key injection
warnings; it was narrowed to this fixture's actual QMP command before the later
fixtures. The initial valid boot is retained separately; no failed probe is
counted as kernel evidence. Strict admission and physical qualification remain
unfinished; no broader firmware or hardware support is inferred.

13c failure-unwind correction contract (before editing):
The complete initial full suite passed 67,397 assertions (1,174 enabled tests),
but the subsequent context-allocation failure fixture found a real preparation
cleanup defect. `destroy_unpublished()` intentionally refuses nonempty roots,
so the newly prepared firmware root halted cleanup after four empty roots were
freed: physical IOMMU ownership fell from 12 to 8 pages, then the kernel refused
further release. No root was exposed and no firmware page was freed. This is a
failed fixture, not successful unwind qualification.

Expected files: page-table engine, domain cleanup, host/page-table KTEST tests.
Files read: complete engine cleanup/mutation contract, storage backend,
preparation/release call sites, host and kernel page-table tests, actual debugger
and panic evidence. Preserve ordinary empty-root destruction semantics and every
exposed/quarantined-hardware lifetime rule. Add an explicitly named unpublished
preparation-discard operation that releases only backend table storage, even if
unpublished leaf entries exist. It must atomically refuse busy or ever-exposed
roots, never free referenced data, and invalidate old generation tokens. Use it
only for boot preparation rollback. Existing nonblocking table guard remains the
only lock; no new waits, allocations, IRQ operations or ABI/wire changes.
Tests: mapped/unmapped/partial-preparation and exposed/busy refusal models,
isolated KTEST, both builds, and repeat all 13c live fixtures on the corrected
frozen ELF. Rollback: restore RMRR refusal rather than permit incomplete unwind.

The corrected 13c kernel passes both builds, formatting, and the 41-target host
regression (now 259 GTest cases in 29 binaries). `discard_unpublished()` is a
separate explicit operation; `destroy_unpublished()` still requires no live
leaves. Busy or ever-exposed roots refuse both cleanup paths. Discard releases
only backend table pages, leaves referenced data ownership untouched, and keeps
the generation counter monotonic across reuse. New host cases cover partial
preparation, stale tokens and exposed/quarantined refusal; the existing
concurrent test now checks discard refusal too. A new ordinary KTEST checks
actual table and referenced-data physical accounting independently.

The corrected live failure fixture now records **12 -> 0 IOMMU-owned pages and
objects**, with all 8 KiB of firmware payload unchanged byte-for-byte. QMP shows
TE=0 and BME clear on all eight PCI functions. The intended requested-translation
refusal follows complete cleanup; this is a deliberate boot failure/QMP stop,
not a successful normal boot. `unwind-accounting.json` records allocator counters
read with every guest CPU debugger-stopped, and `unwind-payload.bin` records the
physical payload (which need not remain CPU-mapped). The earlier failed fixture,
initial full pass and old source/ELF are preserved in `pre-unwind-fix/`.

Corrected ELF SHA-256:
`1d8dd4dbf7ad7dde4bde7a5bbee1dc3f12cc658261df85ca0ef36480dda37d08`.
The valid, overlap, USABLE, ECAM and NVS fixtures were also repeated successfully
on this ELF; the corrected complete suite is still running at this entry.

While that suite runs, a private next-workload prototype in
`build/goal20-stage14/` validates shared-ring geometry and complete frame
boundaries without changing production source or the frozen 142-file receipt.
Its eight cases cover disjoint extents, wrap/full/empty, malformed indices,
incomplete/oversized frames, length truncation, randomized FIFO traffic and
32-bit index arithmetic near the address limit. Optimized and ASan/UBSan runs
pass. LeakSanitizer could not finish under sandbox ptrace; the first log is
retained and the passing sanitizer repeat explicitly disables leak detection.
This is not production integration or cross-VM ordering/traffic qualification.

13c final qualification passes on the corrected ELF. The full translated guest
with two overlapping firmware RMRRs completes **67,924 assertions, zero failures**;
the source-derived runtime audit accounts for all **1,175 enabled tests** and
three disabled skips. It verifies the new scoped/retained firmware mapping test
and the actual-page preparation-discard test. AHCI retires 210 regions/13,068
pages to zero; e1000e retires 514 regions/1,282 pages to zero after 300 exact
64/1514-byte echoes with unchanged packet-pool accounting; xHCI retires four
regions/four pages to zero. The guest powers off normally. Firmware mappings
remain platform-owned; these driver retirement counts do not include or free
firmware backing or the kernel-lifetime exposed IOMMU infrastructure.

`build/goal20-stage13c/validate-final.py` passes all seven corrected boot/log
manifests, six independent early QMP receipts, the debugger-based 12 -> 0 table
unwind and unchanged firmware payload, 142 archived/current source hashes,
29 rebuilt host binary hashes, the complete runtime inventory and 14 WOSDBG
shared-backend load/search calls. Final receipt:
`build/goal20-stage13c/validation.json`. Source archive SHA-256:
`8e8b2c82eeaf958747d34fd6c7c7dc7fd64c7e0f883640d6edb68f0ec25099e6`.
Full-suite coverage fingerprint matches the corrected ELF:
`text_start=0xffffffff80000158 text_end=0xffffffff81879216
text_fnv64=0x94d74ed64ec55c5c`. No sanitizer report appears in qualifying logs;
the four deliberate boot-refusal cases have expected panic/stop outcomes and
are not normal-boot passes. Every task-owned guest has exited. Final host audit
finds only unrelated QEMU PID 36201, which was left untouched.

The private shared-ring prototype now has a ninth passing case: 10,000 complete
frames exchanged by concurrent host producer/consumer threads using acquire and
release indices. Optimized and ASan/UBSan runs both pass all nine cases. This
still does not qualify production BAR cache attributes, inter-VM publication,
bootstrap identity, authenticated WKI traffic, or shared-ring integration.

Goal 20 remains active. This stage qualifies reserved identity mappings for
explicitly supported firmware ownership and the tested QEMU scopes; NVS remains
protected, and physical firmware/platform coverage remains unknown. Strict/ACS
admission, production ivshmem bounds and peer workloads, live multi-TRB USB
payloads and required physical-hardware evidence remain. No indispensable
blocker was encountered. The original goal string and explicit live run/debug
`--no-setup` authorization remain unchanged.

## Stage 14 — shared BAR bounds and complete peer workloads (in progress)

Previous turn: verified progress (13c production RMRR mappings, corrected live
unwind, both builds, full KTEST and archived evidence). No indispensable blocker.
Current source reinspection confirms the Stage 11b shared-ring findings.

Patch contract before production edits:
Problem: net indices are private copies; both readers truncate/consume unchecked
records; peer-writable geometry is reread during pointer construction; WKI TX
has no producer serialization and RDMA offset/page arithmetic can wrap.
Expected files: shared-ring model/backend/bootstrap helpers; net and WKI ivshmem
sources; host/KTEST registration and semantic tests; later bounded live fixture
hooks if actual traffic cannot otherwise be observed. Existing resource lifetime
and shutdown code remain authoritative.
Files read: kernel/critical/testing/WKI instructions and subsystem index; current
netdevice/netpoll/packet/Ethernet entrypoints; PCI BAR mapping and retained PCI
resources; both complete ivshmem drivers, WKI transport callbacks/registry/send
entrypoints, wire/header constants, raw RDMA/zone API, existing source tests and
private nine-case ring prototype. The cached WKI instruction's 8954-byte payload
constant is stale: current authenticated source subtracts the 40-byte trailer.
Keep the current source envelope, not that navigation note.
Flow: admitted TX -> serialized producer -> complete ring record -> release
head -> peer doorbell; IRQ only schedules/masks; serialized task/NAPI RX ->
acquire head -> validate committed extent -> complete copied packet -> release
tail -> network/WKI dispatch. Bootstrap copies and validates geometry once.
Invariants: same version-1 shared header/ring/mailbox wire offsets; same WKI v3
authenticated envelope; exact packet ownership; no reset of an active ring;
no BAR/data reclamation, no new raw-RDMA security or revocation claim. A partial
or malformed frame cannot advance tail or leak a truncated packet. Failures
remain bounded and preserve terminal callback/IRQ ownership.
Locks: net's existing TX lock; WKI gets nonblocking producer admission, preserving
single-drainer RX and callback/shutdown guards. Bitmap keeps its IRQ-safe lock.
Unsafe contexts: no allocation, wait, logging or protocol dispatch inside ring
primitive/producer admission. Boot header waiting remains bounded; RX uses only
existing packet-pool or fixed frame storage. No ACK waits in IRQ/NAPI.
Allocation/blocking: fixed private geometry; no dynamic ring metadata. Raw RDMA
bounds use subtract-before-add and full rounded-extent validation. Shared backing
and escaped pointers keep platform lifetime.
ABI/wire impact: no layout/version change. Header magic is published last; an
unpublished initialization claim prevents simultaneous auto-initializers. Existing
valid headers are attached without resetting indices. Invalid/unknown layouts
refuse publication and BME admission.
Build/test: registered model/concurrency/malformed input tests, production helper
KTEST, both builds, formatting/source guards, then private rootless paired net
and authenticated WKI fixtures with jumbo/wrap/full/retirement evidence. Hardware
peer IDs versus logical roles must be controlled/verified in the fixtures.
Rollback: keep translated-domain and retained resource protections; refuse
unsupported shared layouts rather than reverting to unchecked peer offsets.

### Stage 14 integration validation and actual peer finding

The shared-ring integration builds and qualifies on frozen diagnostic ELF
`5e78762af016034724d738408285a381f66679b18539e235142adc21cde960c4`.
Normal RelWithDebInfo Build WOS, isolated Debug KTEST build and formatting pass.
42 host targets pass, including 272 GTest cases in 30 rebuilt binaries. All four
net/WKI plain/MSI-X held-callback terminal fixtures pass with ordinary poweroff,
BME off, IRQ ownership released and shared BAR backing retained. The complete
translated diagnostic suite passes 68,212 assertions, zero failures, all 1,176
enabled tests and three disabled skips. It verifies the new real shared-view
jumbo/partial-record KTEST, 300 exact NIC echoes and AHCI/e1000e/xHCI retirement,
then powers off normally. Five manifests/logs, 145 archived source hashes,
30 host binaries, complete runtime inventory and ten WOSDBG calls validate in
`build/goal20-stage14/validation.json`. Archive SHA-256:
`4cf6ff765f8755160dc55ba884996e50885bc40c73ddfbc4c3d75202b4943f1b`.
This receipt records the source state before the following live-discovered fix.

A private two-node `wos-cluster --launch --no-setup` workload separates the
Ethernet multicast endpoints and provisions authenticated WKI over the same
16-MiB ivshmem backing. Both nodes connect with `transport=wki-ivshmem` and
publish/mount remote VFS resources. Two initial fixture-only manifest rejections
(restart budget zero, then missing required journald dependency) are preserved
in `peer-manifest-refusal/` and `peer-journal-refusal/`; neither is a payload
pass. The corrected private manifest passes the production parser/validator.
No production service manifest was changed.

The first actual file workload fails: remote stat requests time out, and node 0
then panics under UBSan at remote_vfs.cpp:13971 when an OPEN response's packed
Stat member is bound to an 8-byte-aligned reference. The wire Stat begins at
byte six. Node 1 reports a failed copy and enters shutdown after its peer stops.
Both task-owned guests were intentionally QMP-stopped; no payload or successful
shutdown claim applies. Logs/receipt: `build/goal20-stage14/serial-peer-plain-*.log`
and `validation-peer-plain.json`; original remote_vfs.cpp is retained beside them.
The stat timeout remains a separate observation requiring revalidation. The
paired plain/MSI-X net/WKI variants are prepared but have not yet passed.

### Stage 14b patch contract — packed OPEN stat alignment

Problem: authenticated remote OPEN success passes a packed Stat member by
reference, triggering a reproducible UBSan alignment panic during the required
real peer workload. Fixing this path is necessary to qualify that workload.
Expected files: remote_vfs.cpp (bounded byte copy helper, caller, matching encoder
and existing reply-admission selftest); evidence/goal ledger only otherwise.
Files read: WKI and critical-path instructions; WKI core/wire headers and entry
points, remote_vfs.hpp context/lane contracts, remote_vfs.cpp reply encoding,
admission, open/cache paths and selftest; VFS core REMOTE-open dispatch and Stat
ABI; existing WkiRemoteVfsOpen reply-admission KTEST registration.
Invariants: exact packed wire offsets/lengths and Stat ABI; same admission checks,
credentials, remote FD lifetime, cancellation and cache publication. No VFS
routing, protocol or DMA ownership redesign.
Locks involved: existing per-file stat_cache_lock only; no lock order change.
Unsafe contexts: existing task-context remote-open completion; no IRQ changes.
Allocation/blocking assumptions: copy fixed 144-byte Stat into aligned local
storage, with no new allocation, wait or dispatch. Selftest uses local storage.
ABI/wire/syscall impact: none; byte encoding remains identical.
Build/test plan: exercise the production copy/cache path with a guaranteed
misaligned packed member under existing diagnostic reply-admission KTEST;
format, both builds, then repeat paired workload and complete KTEST on a newly
archived ELF. Preserve the failed runtime separately rather than rewriting it.
Rollback: reject/cache-skip such replies rather than binding a misaligned
reference; retain original crash and source for comparison.

14b implementation/validation: OPEN stat encoding and cache publication now
copy the packed member by byte offset; only the aligned local Stat reaches the
existing cache lock/reference API. The wire offset remains six, with an explicit
static assertion. The existing reply-admission selftest exercises this exact
helper using an aligned reply whose nested Stat is deliberately misaligned,
then verifies cached 64-bit IDs, mode, size and timestamps.

Both builds and formatting pass. Corrected diagnostic ELF:
`b23981aaeda70be0a4017e748b48ea05463993862046fc6e918ebb2662de4031`.
The complete suite passes 68,212 assertions, zero failures, all 1,176 enabled
tests and three skips, including the new alignment path. It verifies 300 exact
NIC echoes and ordinary DMA retirement, then powers off normally. The new
149-file source archive, ELF/boot/coverage fingerprint, runtime inventory and
two WOSDBG calls validate in `build/goal20-stage14b/validation.json`. The 145
Stage 14 host-tested sources remain byte-identical; those 272 host cases were
not needlessly rerun for a remote-VFS-only change tested by diagnostic KTEST.
Seven additional WOSDBG calls analyze both original failed peer logs, including
WKI reconstruction and a distributed timeline; it explicitly reports partial
timestamps and no global order.

The first 14b peer attempt did not reach traffic: role 1 reached its driver more
than five wall seconds before role 0 and exhausted its bounded zero-header wait.
The pair was intentionally QMP-stopped; `launcher-results.json` records peer
failure separately from the passing full suite. A preinitialized valid-header
traffic fixture is now running, with separate output paths. This is attach/data
qualification, not proof that the bootstrap timing issue is resolved. Source
review identifies a follow-up: permit either explicitly configured role to win
the one-time initializer claim, while preserving automatic-role selection using
the existing header role field. No such bootstrap edit has yet been made.

14b paired attach/data outcome: the valid-header plain fixture authenticates both
peers. Node 0 completes all three 512-KiB write/remote-compare/readback/local-compare
rounds with the corrected packed-stat path and no UBSan panic. Node 1 times out
on OP_VFS_STAT_AS (0x041b, destination-file stat) and reports workload failure.
The pair is **not** qualified. Node 1 enters shutdown; node 0 observes fencing.
The failed pair was intentionally stopped after preserving its logs. No clean
peer retirement claim is made; its shutdown logs retain unproven DMA backing.

Read-only GDB inspection after receive progress resumed shows ring_failed=false,
rx_drain_active=false, tx_active=false and callback count zero. A later bounded
saved-task walk shows wki_timer sleeping normally. These snapshots do not prove
the cause of the earlier nearly-full RX ring or the repeated 60-second attach/
stat waits. The only current source caller of wki_ivshmem_transport_poll is the
peer timer tick; the next investigation must capture that timer/dispatch and
server/waiter stacks while the stall is actually present, before changing RX
ownership. Do not infer a permanent RX-gate deadlock from the ring snapshot.

Receipt: `build/goal20-stage14b/peer-partial-evidence.json`; debugger logs
`inspect-peer-0.log` and `tasks-peer-0.log`; five further WOSDBG calls load/search
both logs and construct a timeline. MSI-X WKI and both generic-net peer workloads
remain prepared but unexecuted because the first complete pair failed. The
private MSI-X server itself passed a two-client protocol/eventfd routing check;
that is not a guest IRQ qualification. All task-owned guests have exited; the
final host audit finds only unrelated QEMU PID 36201, preserved untouched.

Next: repair configured-role bootstrap without breaking mixed automatic role
selection; capture the live WKI timer/channel/STAT waiter path at the start of
an actual stall; complete both-role payload/plain/MSI-X qualifications, followed
by strict/ACS and remaining USB/hardware requirements. The goal remains active
with verified progress, not blocked: local implementation/debug alternatives
remain available.

### Stage 14c patch contract: initializer independent of configured role

Problem: configured role 1 refuses an empty header after five seconds if role 0
boots later, preventing an otherwise valid paired transport from starting.
Expected files to change: shared_ring.hpp, both ivshmem driver initializers,
header role comments, host ring tests and the existing kernel ring selftest.
Files already read: both driver initializers/headers, shared_ring.hpp, host/kernel
ring tests and WKI source ownership tests; kernel/WKI/critical-path instructions.
Invariants to preserve: one CAS initializer, magic published last, no reset of
published rings, immutable validated geometry, automatic peers choose opposite
roles; PCI admission and callback retirement remain unchanged.
Locks involved: atomic shared header claim and acquire/release publication only.
Unsafe contexts: bootstrap runs before publication; IRQ paths are unchanged.
Allocation/blocking assumptions: no new allocations or waits; retain the bounded
wait for an initializer already in progress.
ABI/wire/syscall compatibility impact: retain version 1 header size and offsets;
existing vm_id records the winning initializer role before magic publication.
Old zero-initialized vm_id remains compatible with automatic peer role 1.
Build/test plan: configured-role-1-first, mixed automatic roles and racing claims
in host tests; kernel shared-view test; both builds and format; zero-header live
paired workloads and early-stall debugger capture with private artifacts.
Rollback plan: restore Stage 14b source from its archive; retain failed receipts.

14c implementation: either configured role may win the empty-header CAS. The
winner publishes its role in the existing vm_id field before releasing magic;
automatic attach chooses the opposite published role, refusing invalid values.
Both drivers retain their existing geometry validation and PCI admission order.
Fifteen sanitizer-enabled host ring cases pass, including 100 configured-role
races, role-1-first/late-auto attachment and invalid initializer metadata. The
three focused CTest targets (ring, PCI ownership, source guards), formatting,
Normal Build WOS and isolated Debug build pass. An initial host invocation used
the wrong binary path; a sandbox retry could not finish LeakSanitizer. The final
unsandboxed host runs above complete with exit zero.

Frozen diagnostic ELF: 62d113e05cb20ed21875386757ea052bb9f5c1d948c211aefd2a4956971f7658.
Sources and fixtures are retained in build/goal20-stage14c. The zero-header plain
pair authenticates both peers and both complete one 512-KiB write/compare/readback
round. Node 1 subsequently times out on OP_VFS_WRITE (0x0402) in round two; early
OP_VFS_STAT_AS waits also time out. The pair is not qualified and was deliberately
QMP-stopped. Its launcher exit zero does not mean normal poweroff; the receipt
explicitly records the separate external stops. Shutdown retains uncertain DMA
backing, and no clean paired retirement claim is made.

A three-second nonadvancing-ring trigger captures both guests with free RX/TX
gates, timer tasks sleeping and autoattach workers in reply waits. A later
snapshot has empty rings and an ordinary roughly 500-ms timer sleep. This proves
neither a permanently stuck receive gate nor the cause of the write timeout.
The next paired fixtures capture server shards, pending proxies and channel state
during later transfer waits. Five WOSDBG calls successfully load/search both logs
and build the distributed timeline without claiming global timestamp order.

Controlled bootstrap: role 1 initializes while node 0 is paused before boot;
node 0 remains paused eight additional seconds, then connects as role 0 without
changing published geometry or initializer role. This is an intentionally stopped
bootstrap-only qualification. The launcher defaults an omitted role to explicit
zero, so this first receipt does not claim automatic-role runtime coverage; a
separate fw_cfg auto override fixture tests that path. Full KTEST and remaining
paired MSI-X/generic-net fixtures are still running/queued at this checkpoint.

Compatibility limit: header size/version and WKI frame layouts stay unchanged;
new automatic attach understands either initializer role and existing headers
with vm_id=0. Older automatic-role implementations always choose role 1 on
attach (and the older configured role-0 initializer resets a live header), so
mixed-version rolling attachment is not qualified. Runtime receipts use the
same archived build on both peers; do not claim otherwise.

### Stage 14d patch contract: aligned shadow clearing during diagnostic resume

Problem: live stalled MSI-X captures repeatedly sample CPU 3 in
wos_kasan_prepare_kernel_resume -> unpoison_range while clearing about 512 KiB
of retired stack extent. WKI timer/deferred tasks are RUNNABLE with old wake
records, RX gate clear, and ring backlog 65,504/65,528 bytes. This is evidence
of heavy resume-path work, not yet proof it causes every protocol timeout.
Expected files to change: sanitizer/kasan.cpp and test/kasan_ktest.cpp.
Files already read: kasan.cpp/.hpp, stack_resume.hpp, context_switch.cpp/.asm,
util/mem.opt.cpp, kernel CMake exclusion rules, existing KASan tests, kernel and
critical-path/testing instructions.
Invariants to preserve: identical shadow bytes for aligned whole granules;
partial-granule handling untouched; preserve live granule at/above resume RSP;
no change to scheduling state, IRQ admission, mapping ownership or WKI protocol.
Locks involved: none added; existing low-level caller lock context unchanged.
Unsafe contexts: early boot, allocator and IRQ-disabled final resume tail.
Allocation/blocking assumptions: no new allocation/wait/logging; same shadow
pages may demand-fault as before. Existing memset uses excluded scalar/rep stosb
code, no SIMD or sanitizer recursion.
ABI/wire/syscall compatibility impact: none.
Build/test plan: boundary and multi-page shadow KTEST; both builds/format; new
archived ELF and full diagnostic run; repeat failed live pairs with same fixture.
Stage 14c live fixtures continue using their frozen ELF and private images.
Rollback plan: remove the aligned fast path; preserve the original loop and
Stage 14c source/artifacts for causal comparison.

14c completed validation: full KTEST passes 68,214 assertions, zero failures,
1,176 enabled cases and three skips, 300 exact NIC echoes, and normal poweroff.
The 149-source archive, full boot/ELF/KCOV fingerprint, runtime inventory and
WOSDBG calls validate in build/goal20-stage14c/validation.json. The explicit
fw_cfg auto override delayed-peer fixture also connects as role 0 after role 1
initializes and waits eight seconds. Both bootstrap receipts are deliberately
stopped focused runs, not full payload/retirement passes.

Both generic-net paired fixtures complete all three 512-KiB bidirectional
write/remote-compare/readback/local-compare rounds, plain and MSI-X, with the
same kernel. The direct WKI MSI-X pair instead stalls during attach/STAT and
fails its workload. A stronger mid-stall capture shows node 1's receive ring at
65,504/65,528 occupied bytes, RX/TX gates clear, and WKI timer/deferred/autoattach
RUNNABLE on CPU 3 with old wake records. Node 0 retains an unacknowledged STAT
request. Live CPU 3 is inside KASan's approximately 512-KiB stack-resume clear;
CPU 2 is also in this clear in that snapshot. The first server diagnostic had a
libc++ anonymous-field accessor error; its partial outputs are preserved as
early-stall-peer-msix-*. The corrected capture records pending proxies/channels
and empty server queues, and detaches normally. The optimization in 14d is a
controlled test of resume overhead, not a claim of proven timeout causality.

Generic-net payload PASS is separate from retirement: paired shutdown logs
report unproven DMA/callback ownership and retained backing. The standalone
terminal retirement/full-KTEST receipts remain the clean retirement evidence;
ordinary paired-traffic shutdown is not yet qualified as zero live mappings.

Ordinary power-path follow-up found during 14d validation (no edit yet):
platform/sys/syscall.cpp configures IA32_FMASK to clear IF on syscall entry;
syscalls_impl/power/sys_power.cpp calls begin_reboot_command directly, and
run_finalizer preserves that interrupt-disabled context across its yielding
helpers. Each converted driver retirement gate uses phys::can_wait_for_reclaim,
which requires IF enabled. This explains a concrete source-level route to the
uniform -EWOULDBLOCK (-11) retirement refusals on userspace-triggered poweroff;
selftest_shutdown runs from a different task context. Validate the finalizer's
actual IF/preempt/transition state before changing its entry contract. Do not
weaken driver retirement gates or infer quiescence from the platform poweroff.

14d implementation/initial validation: an aligned, nonwrapping unpoison range
now clears exactly size/8 contiguous shadow bytes with the existing excluded
memset. Zero-size, wrapping and partial-granule paths retain their previous
behavior. The new four-shadow-page KTEST checks the accessible interior and
both adjacent poisoned granules before restoring its local shadow.
Normal Build WOS, isolated Debug build and formatting pass. Full KTEST passes
68,223 assertions, zero failures, all 1,177 enabled tests and three skips, with
300 exact NIC echoes and normal poweroff. Archived diagnostic ELF:
fc0fcec6ac0a505935df87f58c64f286a35d32ff8b6a325e09ebbdc0aa105f62.
The 154-source archive, boot/coverage fingerprints, runtime inventory and two
WOSDBG calls validate in build/goal20-stage14d/validation.json. Focused host
receipts carry forward from 14c because those tested sources are unchanged;
no claim of a new host-suite run is made. Live comparisons are now running.

14d live comparison: direct plain and MSI-X fixtures still fail remote VFS waits.
The generic-net plain repeat is also intermittent: node 1 completes three rounds
while node 0 times out on OP_VFS_OPEN_AS (1054). Generic-net MSI-X completes all
three rounds on both nodes. Thus neither the earlier generic-net passes nor the
shadow optimization proves overall paired reliability. Corrected GDB snapshots
are preserved alongside the earlier partial captures; this is still a live
investigation, not a reason to weaken reliability/admission rules.

The final generic-net MSI-X run provides direct power-context proof:
build/goal20-stage14d/power-net-peer-msix.log stops at ahci_shutdown with RFLAGS
0x86 (IF clear), and the caller stack is run_finalizer -> begin_reboot_command ->
sys_power -> syscall_handler. The debugger deletes its breakpoint and detaches,
exit zero, with no timeout. Retirement then returns -11 as its explicit safe-wait
check requires. Repair the finalizer context, not the driver's refusal. Node 1
also remains after peer shutdown; payload PASS is not clean-retirement evidence.

Before speculative protocol changes, compare the same source using the Normal
Build WOS optimized ELF: all paired runs so far used the expensive Debug KTEST
profile. Preserve failure evidence under that profile, and avoid claiming the
KASan fast path fixed the observed WKI timeout. Direct ivshmem currently polls
only from the peer timer; inline wait progress polls Ethernet but not ivshmem,
and its masked MSI-X handler does not notify the timer. Those are concrete
receive-progress follow-ups, but the generic-net repeat means they are not yet
an exclusive explanation. Strict/ACS, multi-TRB USB and hardware remain pending.

14d final receipts: paired-summary.json records complete rounds [node0,node1]
as direct plain [0,0], direct MSI-X [0,1], generic plain [0,3], generic MSI-X
[3,3]. Only the last is a complete payload pass on this build. All four pairs
were intentionally stopped after failure or incomplete shutdown; no pair is a
clean-retirement pass. Sixteen WOSDBG calls load/search all eight logs. The final
154-source/ELF/evidence audit passes in final-audit.json. All task-owned guests
and launchers exited; process-audit.json finds only unrelated QEMU PID 36201,
which was preserved. The goal remains active with verified implementation and
runtime progress, not blocked; the local follow-ups above are still available.

### Stage 14e patch contract: userspace power finalizer wait context

Problem: live GDB proves ordinary poweroff reaches AHCI retirement with IF clear
(RFLAGS=0x86), so all converted driver safe-wait gates refuse with -EWOULDBLOCK.
Expected files to change: platform/power/power.cpp/.hpp and shutdown source guards.
Files already read: complete finalizer and callers, power syscall/ABI, syscall
entry/FM​​ASK, scheduler wait helpers and shutdown admission, physical safe-wait
gate, AHCI/ivshmem retirement, powerctl and init shutdown paths, kernel/workflow/
critical-path/testing instructions. Current tree and frozen 14d GDB receipt agree.
Invariants to preserve: POWER authority and single finalizer ownership precede
interrupt enable; invalid, unauthorized and unsafe-context calls leave flags and
shutdown phase unchanged. Driver quiesce/IRQ-drain gates, VFS sync-before-storage
retirement, uncertain-backing retention and syscall numbers remain unchanged.
Locks involved: no new locks; entry must be a task without preemption-disabled
ownership or an in-progress scheduler handoff. Existing driver locks unchanged.
Unsafe contexts: reject locked/transition/block-pending callers before claiming
FINALIZING. Accepted finalizer never returns; it may enable interrupts for normal
bounded task-context waits and disables them again only at platform halt.
Allocation/blocking assumptions: no new allocation or wait; enable the already
required completion IRQs and scheduler waits. Selftest callers retain their
existing ordinary task-context precondition.
ABI/wire/syscall compatibility impact: no layout/opcode/authority changes;
unsafe internal invocation explicitly returns -EWOULDBLOCK before phase change.
Build/test plan: source ordering guards, both builds and format; private rootless
single-node forced and init-managed poweroff on Normal RelWithDebInfo and Debug;
GDB verifies IF at retirement; audit actual driver mapping/IRQ retirement; full
KTEST plus optimized paired payload comparison using separately frozen images.
Rollback: restore 14d power sources and retain its safe refusal behavior; never
remove driver safe-wait checks to make retirement appear successful.

Stage 14e focused power qualification: both normal RelWithDebInfo and isolated
Debug builds plus formatting and the shutdown source guard pass. Frozen ELFs:
normal 87660591e6bf671ece7afa8224439bf2ac0893cef428eb67ccac9d64b1a83ad7;
Debug 6e424faaddcbb15dda6845b9841fda5f724038a53e55fbef5cca5395a84429be.
Six rootless single-node userspace poweroff cases pass in
build/goal20-stage14e/power-validation.json: forced and init-managed on each
profile with active modern ACCESS_PLATFORM virtio, plus two normal cases with
virtio's safe feature-refusal/unwind. The initial fixture had duplicate NIC
zone IDs and was rejected before boot; corrected configurations are preserved.
The first normal fixtures omitted iommu_platform=on, so those are explicitly
probe-refusal cases, not active virtio qualification.

Every GDB receipt reaches AHCI from the real power syscall with IF set, deletes
its breakpoint and detaches successfully. Every guest exits without external
stop. AHCI regions/pages 210/13068, active virtio 514/2056, and e1000e 514/1282
all reach zero with no quarantine after their device-stop/IRQ gates. Direct
ivshmem reports drained=1, BME=0, irq_owned=0 and retained BAR backing; the xHCI
controller reports halted/masked, BME off, no IRQ or DMA regions. No USB
peripheral was attached, so this does not extend USB payload qualification.
Forced shutdown reports the busy root mount and performs its final sync;
init-managed shutdown unmounts root before storage retirement. The preparatory
512-KiB write used tmpfs, not a claimed block-durability workload. Twelve WOSDBG
load/search calls succeeded. Full KTEST and optimized paired checks are pending.

The first optimized direct-WKI plain pair fails remote read after VFS timeouts;
this excludes expensive Debug instrumentation as the sole explanation. Early
live snapshots are preserved separately as early-stall-peer-plain-* before any
later automatic capture. Node 0 RX has 65460/65528 bytes pending with free
single-drainer/producer gates. The active CPU stack is an autoattach waiter
inside wki_spin_yield -> wki_timer_tick; the receive timer is runnable. Current
inline progress drives Ethernet/NAPI and retransmissions but never direct
ivshmem RX; its IRQ only masks the vector. This is concrete follow-up evidence,
not proof of a lock deadlock or a claim that one progress change fixes all
previous generic-net failures. Both nodes were intentionally stopped after the
payload failure; this pair is not a clean-retirement pass.

14e optimized immediate-start comparison: complete rounds are direct plain
[0,0], direct MSI-X [0,0], generic plain [3,0], generic MSI-X [3,3]. The first
three pairs were intentionally stopped on workload failure. A requested direct
MSI-X snapshot arrived after both guests had already exited; its missing-socket
receipt is preserved, not reported as successful debugger evidence. Generic
plain's node1 pending STAT_AS is transport-acknowledged (tx_seq=tx_ack=1), but
rx_seq=0 and no reply has arrived; server queues are empty at the snapshot.
This differs from the direct receive backlog. Reply-send results in
handle_vfs_op are discarded, which is a candidate to instrument, not a proven
cause. Boot-time mount/export churn remains a fixture variable.

Generic MSI-X has two payload PASS markers and node0's real power-finalizer
retirement succeeds, with GDB IF set. Node1 never logs finalizer entry and has
no workload shell in the bounded active-task snapshot; it is not evidence of a
second driver safe-wait refusal. The runner's launcher exit zero followed our
explicit QMP quit of this remaining node. validation-net-peer-msix.json is
annotated with net-msix-external-stop.json and clean_retirement_proven=false;
paired-summary.json does not count this as a clean paired poweroff. The original
bare asynchronous poweroff request did not establish local finalizer entry on
node1. Delayed-start fixtures retain the same source/ELF/rings but wait 60 seconds
before payload and explicitly run locally /sbin/poweroff -f afterward; this
isolates steady transfer and terminal DMA from boot churn/async power requests.
Full suite and these two delayed-start cases are still queued/running here.

14e full diagnostic qualification now passes: 68,223 assertions, zero failures,
all 1,177 enabled cases and three disabled skips, 300 exact NIC echoes, device
retirement phase and normal selftest poweroff. build/goal20-stage14e/validation.json
verifies both builds, formatting, the 154-source archive/current tree, frozen
Debug ELF versus boot manifest and KCOV fingerprint, runtime inventory, and two
WOSDBG full-log calls. source-delta.json proves only power.cpp, power.hpp and the
shutdown source guard differ from the 14d source archive. The ring/PCI focused
host results are explicitly carried forward from 14c; only the shutdown source
guard was rerun for this stage. Six actual userspace poweroff fixtures supplement
this suite because selftest_poweroff does not use the production finalizer.

The first two delayed-start comparisons now pass fully on the frozen normal
ELF: direct plain and generic-net MSI-X both complete three 512-KiB write/read/
compare rounds per node, with expected source/readback SHA-256, explicit local
forced-poweroff markers, successful device retirement and no external stop.
Direct plain ring sampling observes repeated wraps (advisory); payload cmp/hash
is the correctness evidence. Each node's AHCI/e1000 mappings reach zero and
ivshmem reports drained callbacks, BME off and no IRQ ownership. GDB verifies IF
at AHCI entry on node0. Direct plain still records short optional VFS feature
query timeouts, so this is steady-workload qualification, not a claim that all
WKI timeout/performance behavior is fixed. The matching direct MSI-X and generic
plain delayed-start checks are running/queued using the same immutable boot and
workload roots with fresh per-node overlays.

The delayed direct MSI-X repeat is not qualified: each node completed one
512-KiB round, then both timed out on OP_VFS_WRITE (1026), and the runner stopped
them on failure. A manual early GDB capture overlapped the runner's automatic
first-round capture; steady-msix-capture-overlap.json flags duplicate gdbstub
creation and shared snapshot-file overwrites. Do not use those files as one
coherent stalled-state observation. The guest payload failure is independent.
Source follow-up confirms wki_send_impl retains a retransmit entry and accepts
a logical send even when the first transport attempt fails. Therefore the new
nonblocking ivshmem producer refusal alone does not prove a lost reply; do not
replace it with an IRQ-blocking lock based on that hypothesis. A reply-send
failure before reliable enqueue, binding/admission state, and direct RX progress
remain candidates to instrument with exact channel/generation evidence.

14e final checkpoint: generic plain delayed-start also passes both nodes' three
payload rounds and clean retirement/poweroff. Thus three delayed variants pass
(direct plain, generic plain, generic MSI-X), while delayed direct MSI-X fails
[1,1] rounds. paired-summary.json holds all eight comparison outcomes and never
confuses the original MSI-X external stop with normal poweroff. Final audits
verify 154 current sources, six power cases, full 68,223/0 suite and 46 successful
WOSDBG calls (12 power, 2 full, 32 paired). process-audit.json finds no task-owned
VM/launcher/helper and preserves only unrelated QEMU PID 36201. The first audit
incorrectly matched its own shell command text; corrected exact executable and
script matching passed. All tool sessions are terminal at this checkpoint.

Next bounded work: investigate the still-reproducible delayed direct MSI-X
write failure using separate capture filenames and one debugger per VM; capture
exact reply enqueue return, channel generation/sequence and retained server
binding alongside client wait state. Do not infer lost reliable data from a
first transport refusal: wki_send_impl already retains that frame. Evaluate the
missing direct RX poll in wki_spin_yield and minimal IRQ task notification with
explicit reentrancy/lifetime/context proof before changing them. Preserve the
three qualified steady workloads as controls. Strict still unconditionally
refuses; ACS, final strict admission, actual multi-TRB USB and physical evidence
remain open, so the goal is active, not complete or externally blocked.

### Stage 14f patch contract: bounded VFS reply-enqueue evidence

Previous goal turn: progress (power-finalizer repair, six real poweroff passes,
full suite pass, three clean paired controls, and reproducible delayed direct
MSI-X failure). No external blocker or running task-owned VM remains.
Problem: fixed task-context VFS workers discard reply-send errors before reliable
enqueue; acknowledged requests with no reply cannot distinguish that failure
from receive progress/binding state using current logs.
Expected files to change: remote_vfs.cpp diagnostic only, then investigation
artifacts and ledger. No protocol behavior fix is inferred yet.
Files already read: workflow/testing/critical/WKI instructions; current debug
flag map; VFS reply lambdas and callers, retained dev-server fixed-worker FIFO,
WkiChannelIdentity/send APIs and wire constants, wki_send_impl admission/auth/
retransmit path, direct ring callback/producer/drainer and peer timer/waits.
Invariants: unchanged send result, no retry/re-execution of VFS operations, no
wire/ABI/generation changes, no driver or ring ownership changes. First transport
failure is still retained by reliable enqueue. Snapshot tests stay separate
from payload/poweroff receipts; one debugger per VM and distinct capture names.
Locks: diagnostic runs only after send returns with channel/route locks released;
retained worker binding remains owned by its caller. One relaxed counter limits
logs to first 16 negative reply-enqueue results. No new lock or buffer allocation.
Unsafe contexts: handle_vfs_op is the existing task-context worker dispatch;
no IRQ/NAPI change or newly invoked protocol work in interrupt context.
Allocation/blocking: no allocation/wait added; use existing task-context logger
only on negative return, at most 16 records for kernel lifetime.
ABI/wire impact: none.
Build/test: Normal Build WOS and formatting for this observational patch; frozen
optimized ELF on private delayed direct MSI-X workload, retaining earlier clean
controls. Diagnostic build/full suite follows any retained functional change.
Rollback: remove counter/wrapper if diagnostic is superseded; preserve all
captured failures and the frozen source/ELF corresponding to them.

14f first diagnostic result: Normal Build WOS and formatting pass after fixing
an initial missing logger alias. Frozen ELF
54012522aa701e39cfd2f7d42d66deb29cc8004c6464aa59779f2498a6639e17
is read back from the private boot image. Both existing dev-server/VFS source
guards pass; the VFS guard first exposed a stale stage14b expectation of packed
Stat assignment, now updated to the existing aligned memcpy implementation.
The old failing assertion and its fix do not change kernel behavior.

At 93.082 seconds node0 reports exactly: peer 2/channel114/generation1,
STAT_AS1051/request-cookie1, 152-byte reply, result -2 (WKI_ERR_NO_ROUTE).
The single automatic capture succeeds on both nodes without overlap. Node0's
retained binding is active, not retiring, generation1, exported /rootfs/tmp;
its channel has rx_seq=2 and tx_seq=tx_ack=1 with no retransmit entry. Node1's
matching channel has tx_seq=tx_ack=2 and rx_seq=1; proxy waiter expects opcode
1051/cookie1/generation1. The already acknowledged request's reply never entered
the reliable stream. Node0 completes three rounds; node1 times out after one.
Both guests are stopped after the failure; observe.json and exact binding/channel
snapshots preserve this evidence. No claim that this explains every prior
failure is made.

14f route-diagnostic extension contract: change only private ResolvedRoute error
classification and bounded DEV_OP_RESP admission-failure logging in wki.cpp.
Read current resolve, registry/deadline/peer lifecycle gates and core send paths.
Record which existing refusal branch fired (origin lifecycle, route snapshot,
hop lifecycle/identity, direct contact, transport registry), without changing
its boolean result, errno, locking, leases, or fencing. No allocation/wait; one
literal pointer in the stack route object, and at most 16 task-context reply
failure records after resolve releases its explicit locks. Existing lease
ownership/destructor remains intact. Normal build/source guards/format and a
fresh delayed MSI-X fixture will verify; keep the first diagnostic ELF/source/
logs immutable in goal20-stage14f and use goal20-stage14f-route for the next run.
Rollback: remove the private reason tracking/logging, never relax admission.
Core route-failure logging is additionally gated on IF enabled and preempt_count
zero, so interrupt/locked callers retain only the pre-existing return behavior.
The typed logger was traced through emit_v/emit: fixed stack formatting, fixed
journal/module storage and IRQ-safe journal publication; no heap allocation.
The VFS worker diagnostic remains the authoritative complete failed-reply count.

14f route diagnostic result: frozen normal ELF
8e2d64bafa55ab1b6a49e99e4638c14784475075ac625b50e61965a036e077b3
passes both nodes' three payload rounds and normal poweroff, with no external
stop. At guest123.270 node1 records a DEV_OP_RESP refusal for peer1/channel75/
generation2, reason origin-lifecycle. No common-lambda failure follows, consistent
with an already-retrying reply path. This establishes real transient lifecycle
admission refusal, but is not a capture of the first run's STAT failure itself.
The first capture remains the direct evidence for silently discarded STAT reply.

### Stage 14g patch contract: retain completed common VFS replies

Problem: metadata/write/error replies through the common response lambdas drop
pre-enqueue NO_ROUTE/NO_MEM/NO_CREDITS failures, unlike completed read replies.
Expected files: remote_vfs.cpp, existing VFS source guard, this ledger; restore
wki.cpp to the archived14e source to remove only temporary route diagnostics.
Files read: WKI instructions/core send and ResolvedRoute/lifecycle APIs; wire and
channel identity definitions; dev-server retained binding/FIFO dispatch; common
VFS reply callers, read/write/stat/readdir/path mutation/fsync/truncate/RDMA-write
paths; existing completed-read retry helper/tests and proxy wait pacing.
Invariants: no filesystem operation replay; exact response bytes/cookie/channel
generation survive retries. WKI_OK is final even if initial transport TX fails.
Retirement/deadline/generation checks before and after pacing remain unchanged.
Locks: common callers release s_vfs_lock before replies; VFS operations return
before response construction. Worker owns retained binding until handler returns;
no new held lock. A retiring binding breaks the retry to avoid lifecycle writer
waiting on its retained reference. No changes to channel/peer/transport gates.
Unsafe contexts: fixed task-context worker only. Null binding (async CLOSE)
still cannot retry. Add an explicit IF/preempt/task gate in the production
wrapper so an unexpected unsafe caller performs only its existing first send.
Allocation/blocking: no new allocation; existing bounded paced retry only. Do not
extend separate xattr/batch/direct-OPEN reply paths without their own audit.
ABI/wire/syscall impact: none. DMA/IRQ/ring ownership unchanged.
Build/test: existing deterministic immutable-response/terminal-error/retirement/
deadline/generation retry selftest; source guard for common reply integration;
Normal Build WOS, isolated diagnostic build/full suite, fresh optimized paired
MSI-X payload and clean shutdown, retaining previous controls.
Rollback: restore these narrowly changed helper/lambda/test lines from14f archive;
retain all captured evidence. Goal remains active; strict/ACS/USB work is open.

Next USB reconnaissance (no USB code changes in14g): xhci_bulk_transfer obtains
one owned streaming Submission, calls xhci_dma::plan_transfer, then publishes at
most three chained Normal TRBs plus one terminal Event Data cookie under the
endpoint ring lock. finish_transfer waits for terminal completion, copies only
the reported IN prefix, then releases streaming storage and request identity.
The WOS_SELFTEST packet hook echoes 65/1514/1801/2015-byte frames; QEMU usb-net's
2048-byte buffers preclude a large full-length payload. Page-aligned managed
storage also keeps those small payloads before a natural 64KiB split.

A bounded next strategy is an opt-in selftest first-TRB length cap (e.g.512)
through the production planner, leaving normal default segmentation unchanged.
The 1514/1801/2015-byte frames would cross that split in both directions and the
9018-byte IN request would exercise a short receive followed by the existing
terminal Event Data completion. Preserve the three-segment/four-identity bound;
reject over-capacity plans rather than expand rings or request-table ownership.
Exhaust every low-address offset for fixture-sized requests and verify exact
coverage, per-TRB64KiB bounds, remaining-packet counts, zero/overflow rejection
and unchanged default plans. Then use a fresh private translated-IOMMU USB
terminal fixture, byte-check64 echoes and zero-mapping retirement. This would
qualify actual chained payloads, not full64KiB/128KiB device transfers or physical
hardware; keep those limits explicit. Existing reusable fixture is
build/goal20-stage10d/run-terminal.py (normal case), but give new paths/logs and
add --iommu-translated plus the new opt-in split flag. No USB patch applied yet.

Strict-policy reconnaissance: initialize_domains still unconditionally rejects
strict before translated preparation. Existing groups::qualify merges absent/
disabled ACS and alias branches, with collective group DMA visibility; that is
not per-device isolation or requester-spoof prevention. Final strict admission
must explicitly qualify topology/source-validation and supported requester modes
before removing the refusal. Merely setting translated=true, silently trusting
missing ACS, or rebranding merged groups as isolated devices is insufficient.

14g review follow-up contract (14g-owned): scheduler maybe_exit_current_kernel_
thread_for_shutdown exits DAEMON tasks at kern_sleep/kern_yield without stack
unwind unless KernelShutdownHold is active. run_deferred_vfs_op currently retains
a pre-ACK binding and operation-pool entry without such a hold. New common-reply
pacing broadens the existing read wait exposure. Add the established shutdown
ownership guard after the null-op check and before touching retained work, lasting
through release_binding and deferred_vfs_op_release on every return path. Files:
dev_server.cpp and existing source guard, plus ledger. Read worker dequeue/run/
release paths, scheduler shutdown target/exit/wait checks and shutdown_ownership.
No new lock, allocation, wait or ABI; guard increments/decrements the current
task's ownership counter and does not disable IRQs/preemption. Existing VFS
operation/reply deadlines and binding-retirement checks remain unchanged. This
protects owned work from non-unwinding cooperative exit; it does not assert that
all pending worker queues are drained. Validate both builds, source guard, the
shutdown ownership host model, paired normal poweroff and final diagnostic suite.
The already-running14g full suite uses its immutable earlier ELF and is only
intermediate evidence; keep those receipts, use fresh14g-owned artifacts for the
final guard. Rollback is one guard scope and its source assertion.

### Stage14h patch contract: actual small USB payloads across chained TRBs

Problem: live USB echoes were one TRB despite model coverage of natural64KiB
boundaries; QEMU usb-net cannot carry the previously proposed huge frames.
Expected files: xhci_dma.hpp pure planner, xhci.cpp WOS_SELFTEST-only selection/
existing packet diagnostics, xhci_dma_test.cpp, private fixture artifacts/ledger.
Files read: controller/endpoint streaming prepare/submit/finish and ring enqueue,
request identity and terminal Event Data rules, xhci_dma Streaming/planner,
existing host planner/retirement tests, packet echo hook and terminal fixture,
CDC packet hook and debug flag map; applicable kernel/critical/testing guidance.
Invariants: normal first-segment cap defaults unlimited; no change to default
segmentation, DMA allocation, mapping permissions, retirement or wire payload.
Opt-in --xhci-test-split-first caps just the first bulk TRB to one endpoint USB
packet. Every segment remains within one64KiB boundary; at most three Normal
TRBs plus one Event Data identity. Overflow/capacity refusal remains unpublished.
Locks: prepare plan before endpoint ring lock; existing request_busy ownership,
ring publication and terminal completion unchanged. No added locks or callbacks.
Unsafe contexts: selection/logging in existing task-context bulk/test paths only;
no IRQ change. Allocation/blocking: none added, fixed arrays remain bounded.
ABI/wire/syscall impact: none; standard chained TRBs still describe exactly the
original payload. No large-QEMU-payload or physical-boundary evidence claimed.
Build/test: exhaustive host low-address offsets for fixture lengths/caps and
unchanged default tests; both builds and source guards/format; new translated
private USB fixture,64 byte-exact echoes, verify actual payload crosses first
TRB in both directions and terminal release to zero. Preserve14g-owned frozen
ELFs and in-flight pair/full receipts separately; source-current-before-usb.json
verifies all final14g-owned source hashes immediately before this USB edit.
Rollback: remove optional planner parameter/selection/log fields and tests; no
state migration, API ownership or device protocol changes.

14g completed-reply checkpoint: the first retry build passed the MSI-X pair
and full 68,223/0 suite. Its immutable receipts are in build/goal20-stage14g.
Subsequent review found the deferred worker's cooperative-exit lifetime gap;
this earlier build is retained only as intermediate evidence.

Final 14g-owned source adds KernelShutdownHold around the complete deferred
operation, including binding and pool-entry release. Both builds, formatting,
VFS/dev-server source guards and five shutdown-ownership host cases pass. The
first host attempt passed assertions but LeakSanitizer failed under the ptrace
sandbox; the preserved log is followed by a successful unsandboxed sanitizer run.
Normal ELF: f769a414d9d5c4b16c90596b381cb1355cd6518969cb4783f2846a27c1820d5f.
Debug ELF: 252babd8f7406a0df97b1c6e9d51394e0b886cbbe005045005a5b5825bcd90af.
Both are frozen with 157 source hashes. The full boot package uses its own frozen
kernel/init tree, so subsequent USB rebuilds cannot replace the tested image.

Both final optimized MSI-X nodes complete three 512-KiB copy/compare rounds and
normal poweroff. AHCI 210 regions/13,068 pages and e1000e 514 regions/1,282 pages
retire to zero; ivshmem callbacks drain with BME/IRQ ownership cleared. No
external stop or common-reply enqueue failure occurs. Optional 100-ms feature
query timeouts still appear; OPEN/xattr/batch separate reply paths remain outside
this bounded fix. The complete final diagnostic suite passes 68,223 assertions,
zero failures, all 1,177 enabled cases, three skips, the completed-reply regression,
300 exact NIC echoes and normal poweroff. Six final WOSDBG calls pass.
build/goal20-stage14g-owned/validation.json records source/ELF/boot/KCOV/runtime
checks; source-current-before-usb.json verifies the working tree before the later
three-file USB change. The archive is independently hash-checked. No general WKI
reliability or physical-hardware qualification is claimed.

### Stage 14h: actual chained USB payload qualification

Added an optional first-segment limit to the bounded xHCI planner. Normal callers
retain the unlimited default. Only WOS_SELFTEST with --xhci-test-split-first
selects one endpoint packet for the first bulk TRB. Mapping ownership, buffer
length, terminal Event Data completion, IRQ work, and retirement are unchanged.
Existing packet-test logs now report the actual first-TRB byte count.

Normal Build WOS and isolated Debug builds, formatting, xHCI source guard and
19 sanitizer-enabled host cases pass. The new host case exhausts all 65,536
low-address offsets for five fixture lengths and three packet/cap sizes; it
checks complete coverage, per-TRB boundary limits, packet accounting and bounded
capacity refusal, alongside the unchanged default planner and ownership tests.
Normal ELF: b155fa82928a9d221473028985ce2ae9983db1a13b6f0256ed4740752e4cd44f.
Debug ELF: 6c50be77708aa06616ffc4bd43bb68d7a0a0acae06b22763d88fa982eab958a3.
The current 157-file archive hash is
37f125b8e26ea12bc4209849329c6ce4126684607b43a73a5cecf4b2ece1fc3b.

The fresh private rootless translated-IOMMU USB fixture passes all 64 byte-exact
host echoes at 65/1514/1801/2015 bytes. Every TX and RX payload uses two TRBs and
crosses the actual 64-byte first TRB; all-chained.json verifies all 64 in each
direction. The stricter minimum-size receipt separately records 48 payloads over
1,024 bytes in each direction. The 9,018-byte IN request completes with a short
payload and the existing terminal Event Data identity. Terminal retirement is
idempotent, rejects late work, and releases 10 xHCI regions/13 pages to zero with
halt, IRQ drain and BME-off evidence. AHCI and e1000e also retire to zero, and the
VM powers off normally. Both WOSDBG calls pass. Receipts and final source/boot/
ELF/runtime audit are in build/goal20-stage14h/validation.json.

This qualifies real small payloads across an intentional split, not natural
64-KiB physical-boundary crossings, full 64-KiB/128-KiB device transfers, or
hardware. No full-suite rerun is attributed to the later USB ELF; the full suite
above belongs to the frozen 14g-owned ELF, with exactly these three USB files
changed afterward. All task-owned VMs/launchers have exited; unrelated QEMU PID
36201 remains. The first process audit's executable-name filter missed it; the
corrected command-line audit plus ps confirmed it and absent private QMP sockets.

Next bounded work is strict/ACS admission and its model/live qualification.
Small actual multi-TRB USB payloads are now qualified. Preserve the explicit
large-transfer/hardware limits and the optional WKI feature-query limitation.
Goal remains active, with no indispensable external blocker established. The
original goal string and user authorization remain unchanged. The earlier noted
WKI instruction constant (8954 versus actual 8954 minus authentication trailer)
remains stale; local wire.hpp is authoritative.

## Stage 15 reconnaissance and strict-admission patch contract

Previous goal turn: progress (completed-reply ownership, full guarded suite,
actual chained USB payloads, clean retirement). No live task-owned process or
indispensable external blocker remains. Current source still rejects every
--iommu-strict boot before domain preparation.

Files/subsystems: ACPI discovery/scopes establish the complete immutable PCI
registry and remapping-unit ownership; pci_extended/pcie_ecam retain configuration
identity, requester-mode and ACS evidence. groups::qualify creates aliases and
collective groups. domains::prepare builds roots and firmware mappings, then
initialize_domains disables all BME, exposes/activates tables, publishes ready
and admits bridge forwarding. Driver probes prepare mappings before BME. The
raw PCI command-write gate and typed bus-master API both consult domain state.

Primary specification: Intel VT-d sections 3.12.3 (root-port source protection),
3.12.4 (unique source IDs for PCI/PCIe root-complex integrated devices), 3.16
(explicit reserved mappings), 3.17 (root-complex peer requests are translated):
https://cdrdv2-public.intel.com/831418/vt-directed-io-spec.pdf
The older downloaded revision in /tmp/wos-goal20-spec/vtd.txt contains the same
relevant rules. Linux's upstream drivers/iommu/iommu.c also distinguishes ACS
boundaries and DMA aliases; it is corroborating reference, not WOS authority.
This policy trusts a VT-d-conforming root complex and accurate firmware topology;
it does not certify hardware from enumeration alone. Interrupt-address requests
have separate architectural semantics (VT-d3.15); this DMA policy does not add
interrupt remapping or claim interrupt-injection protection.

Problem: development translated mode permits collective groups, so simply
removing the strict refusal would mislabel shared domains as device isolation.
Expected files: groups.hpp, domains.cpp/.hpp, iommu_groups_test.cpp and existing
IommuDomains KTEST. Ledger and private strict/failure fixtures accompany them.
Files already read: applicable workflow/kernel/critical/build/testing/index
instructions, current CMake/task entrypoints, PCI enumeration/config admission,
ECAM capability snapshots, scope/group/reserved qualifiers, domain activation/
map/unmap/fault paths, DMA APIs, existing group and active-page-table tests.

Invariants: strict implies translated and never falls back. Conflicting off/
strict options refuse. Each strict device must retain its own group/root/DID;
requester aliases and shared groups refuse before any table exposure. Root-bus
legacy PCI and declared root-complex integrated PCIe functions use VT-d's unique
integrated-source rule; ordinary PCIe endpoints cannot masquerade as integrated
root functions. Every external bridge must have implemented/enabled source
validation, translation blocking, request/completion redirection and upstream
forwarding, with direct translated P2P disabled. Existing unsupported requester
mode refusals remain. No ACS controls are silently enabled or assumed. Missing
controls give a concrete strict refusal; development grouping remains available
only through its existing explicit mode. Reserved mappings remain explicit,
scoped, retained and distinct from driver-owned DMA buffers.

Locks/unsafe contexts: pure qualification allocates nothing and executes in BSP
boot before driver/IRQ publication. Existing PCI config lock, unit admission,
fault quarantine and driver callback locks are unchanged. No new IRQ/NAPI work.
Allocation/blocking: no new allocation in qualification; existing fallible table
preparation/invalidation and exposed-page quarantine rules stay unchanged.
ABI/wire/syscall impact: none; internal stats report strict policy only after
ready publication. No driver or protocol conversion changes.
Build/test: positive integrated/ACS topology and negative missing/disabled ACS,
shared/alias/impossible placement model cases; active strict roots must be unique.
Normal Build WOS and isolated Debug build, formatting, strict full QEMU KTEST,
explicit absent-IOMMU/off-conflict/ACS-refusal boots and managed workload checks.
Runtime fixtures remain private/rootless and preserve existing VM36201. Hardware
conformance remains a distinct qualification surface, not inferred from QEMU.
Rollback: revert policy gate/stats/model tests together; restore unconditional
strict refusal. Never roll strict back into silent passthrough or shared isolation.

## Stage 16 ACS activation patch contract

Problem: strict admission correctly refuses a supported external root port whose
firmware left ACS disabled. Enable the required supported controls before DMA
admission, then qualify fresh readback, without changing the observation snapshot.
Expected files: pci_ecam.hpp, pci_extended.cpp/.hpp, domains.cpp,
pci_ecam_test.cpp; evidence ledger and private fixtures.
Files read: stage15 source/instruction entrypoints, complete ECAM parser and
snapshot collector, PCI command gate, domain preparation/activation, host tests.
Invariants: one serialized BSP activation attempt; all enumerated bus masters off;
no published domains; no table exposure; validate identity and bounded capability
lists again; write only the 16-bit ACS control, preserve unrelated bits, set
supported required 0x1f and clear direct translated P2P 0x40; verify exact readback.
Absent/incomplete ACS remains unqualified where strict topology requires it.
Immutable discovery snapshots remain immutable. Failure never relaxes strict mode.
Locks: existing PCI config lock for command reads; no driver/unit locks held.
Unsafe contexts: exclusively pre-driver BSP boot, never IRQ/NAPI or runtime.
Allocation/blocking: fixed arrays, bounded config reads, no allocation or waits.
ABI/wire/syscall: none. Non-strict mode unchanged; DMA lifetime rules unchanged.
Build/test: fake config identity/BME/capability/write/readback/idempotence failures;
normal and isolated builds; live strict external-root-port positive, missing ACS
negative, and alias bridge negative; artifact identities retained per stage.
Rollback: remove activation call/helper, restoring strict refusal of disabled ACS.

## Stage 15 frozen qualification result

Frozen normal ELF 6c183376bf0d6c3cf6ca2c42c3a653f52ef8fe507f355b58da8f693a2f487da4;
Debug ELF 2b9110c150522f77db0dfef17621babf36c2f73465f3a9b788eea1d8eda8c831.
Normal Build WOS, isolated Debug build, format checks, 15 group plus 13 reserved
host cases pass. Full strict KTEST: 68,250 assertions, zero failures, 300 exact
e1000e echoes, matched ELF/manifest/KCOV and normal poweroff. Strict virtio terminal
probe: 44 assertions and two exact 64/9014-byte echoes, 516 regions/2059 pages
retired to zero. Strict USB: 64 exact small payload echoes with actual chained
TRBs; terminal 10 regions/13 pages to zero, normal poweroff.

Strict optimized direct MSI-X pair: three complete 512-KiB rounds per node,
matching f75324209a867c78dc4493767de07bf0d2a2cfeb11ade7a7d8aa7da386bd9242;
normal VM exits with no forced stops. Each node releases AHCI210/13068 and
e1000e514/1282 to zero; ivshmem BME0/IRQ0/drained. Root unmount reports open files
and uses its existing final-sync fallback; this is device-retirement evidence,
not proof that every filesystem unmounted cleanly. A 100-ms optional VFS feature
query timeout occurred; no completed-reply enqueue failure was observed.

Stale/foreign DMA: actual reason0x6 read faults with requester/domain and known
allocation generation; admission closes, BME inhibited, guards preserved, IRQ
ownership drained, uncertain backing quarantined. Host exact echoes before
fault: 128/322 respectively. Withheld invalidation: 128 echoes, quarantine without
hardware fault/IRQ. These three runs were deliberately QMP-stopped, not normal
poweroff or zero-quarantine claims. Refusal boots for absent IOMMU/off-conflict/
disabled ACS show every readable PCI BME off (8/8/9 devices) and all present VT-d
TE bits clear by live QMP register reads. They were intentionally stopped.

Nine single-node receipts, the pair, 18 WOSDBG calls and all157 archived source
hashes pass build/goal20-stage15/audit.py. Its validation.json records the five
subsequent ACS source changes separately. An initial offline audit used an
incorrect ivshmem log prefix and was corrected to the actual ivshmem-pci receipt;
no kernel change or rerun was needed for that assertion correction.

## Stage 16 fixture correction

The first missing-ACS fixture requested nonexistent QEMU property x-acs and
exited before WOS boot. Failed launch/argv receipts are retained under
build/goal20-stage16/initial-invalid-property-*. No kernel success is inferred.
The corrected fixture uses ioh3420, whose root-port model has no ACS offset;
the generic pcie-root-port supplies ACS. QEMU primary model sources inspected:
https://raw.githubusercontent.com/qemu/qemu/master/hw/pci-bridge/ioh3420.c and
https://raw.githubusercontent.com/qemu/qemu/master/hw/pci-bridge/gen_pcie_root_port.c.
The private wrapper now injects devices only into commands carrying WOS fw_cfg,
so libguestfs appliance QEMU calls pass through unchanged. Actual local config
readback remains the authority for the live result.

A second private negative fixture initially placed the PCIe e1000e endpoint
behind a conventional PCI bridge. Its inaccessible/malformed extended config
correctly refused admission before ACS programming, so it did not isolate the
intended alias condition. That serial/register evidence is retained under
initial-pcie-on-pci-*; the fixture now uses the conventional e1000 endpoint.
No production relaxation was made to accommodate either fixture error.

The corrected alias fixture exposed a private runner race: it read the preceding
serial file before the new QMP socket existed. The failed runner/guest receipts
are retained as initial-stale-serial-*. The runner now removes its own stale
serial file before launch. The orphan PID was already gone when its owned-QMP
cleanup was attempted (connection refused, confirmed by ps); no unrelated VM
was addressed. The subsequent fresh run is the authoritative alias receipt.

## Stage 16 final qualification and completion review

Final normal ELF: 7a5b121d9967704a55fe2dce6e6478a3be386631286cc1851714b1969e551cb5.
Final Debug ELF: 057a4c1c7c9b421aa070c59c804a28256bf6f57be94bda8cca64e184b3e08f0e.
Frozen157-file archive: 21f098687a2256797977e3d9955087ee89b7dec41822e1a4a71198d2b3aef64e.
Normal Build WOS is RelWithDebInfo; isolated KTEST is Debug. Both builds and
format checks exit0. Existing CPUID partial-initializer and static-init ELF
warnings remain; this is not a warning-clean build claim. Thirteen ACS/ECAM
host cases pass, including last-page bounded access, no writes on malformed/
active-BME input, control preservation, idempotence and failed-write/readback.

The final full diagnostic run through the external PCIe root port passes68,578
assertions, zero failures,1,177 enabled cases and three existing skips. Its
manifest, frozen ELF and KCOV fingerprint match. e1000e exchanges300 exact
frames; AHCI210regions/13068pages, e1000e514/1282 and xHCI4/4 retire to zero.
Normal poweroff, launcher0. Hardware-context KTEST verifies all10 independent
DIDs/roots, per-device directional leaves, unchanged neighbors and complete
unmap. Read-only QMP independently verifies root-port ACS capability0x5f,
control0x1f at offset0x148, BME1 and VT-d GSTS0xc0000000 after admission.

Negative authoritative receipts: ACS-less ioh3420 refuses, all9 PCI BME bits0;
PCIe-to-PCI alias refuses at device3 after root-port ACS0x1f readback, all10 BME
bits0. Both have VT-d TE0 and no translation/fallback publication, with exact
boot manifests and intentional QMP stops. Six final WOSDBG calls pass. The
stage16 audit verifies every current source hash against its frozen archive;
its only changes since stage15 are domains.cpp, pci_ecam.hpp,
pci_extended.cpp/.hpp and pci_ecam_test.cpp. Stage15's strict multi-driver,
real-fault, USB and optimized WKI receipts therefore remain separately scoped
to their exact preceding artifacts, not relabeled as final-kernel runs.

Authoritative final receipts: build/goal20-stage16/validation.json,
live-boundary.json and process-audit.json; preceding driver/fault/pair receipts:
build/goal20-stage15/validation.json. No task-owned QEMU processes remain;
unrelated PID36201 is still present. No commit or deployment was performed.

Completion-contract review:

| Contract | Implemented and verified surface |
| --- | --- |
| DMAR validation | Bounded firmware parsing, scopes/RMRRs/unit identity; host/fuzz/KTEST receipts in stages1–3 and13–14. |
| Domain ownership | Retained contexts, roots, groups, reserved mappings, capabilities and invalidation; final strict hardware-context KTEST and boundary fixtures. |
| Managed API | Coherent allocation, exclusive streaming bounce pages, masks/alignment, generation-qualified submissions, synchronization and audited release/quarantine. |
| BME admission | PCI gate, all-BME-off preparation, strict ACS readback and fail-closed activation; positive/negative live register checks. |
| Driver conversion | AHCI/virtio/e1000e/xHCI submission addresses use the common API; ivshmem shared BAR/IRQ lifetime handled explicitly. |
| Retirement | Command/callback admission closure, hardware stop/reset, IRQ drain, late-work refusal, idempotence and retained uncertain ownership. |
| Fault reporting | Bounded capture plus deferred device/domain/access/address/generation diagnostics; real stale/foreign VT-d faults and withheld-invalidation quarantine. |
| Fallback | Documented/logged compatibility passthrough; opt-in strict refuses unsupported state and never silently falls back. |
| Accounting | Healthy success/probe/terminal retirement returns driver counts to zero; deliberately uncertain fault/stop paths retain quarantined backing. |

Goal20 is complete for this documented Intel VT-d/PCI subset. Operational API,
policy and deployment limits are in docs/dma_ownership.md. Physical hardware,
full large USB payloads and natural physical64-KiB USB boundary crossings remain
unqualified; model boundary tests and actual small chained transfers are the
available evidence. They do not establish physical ACS/cache/firmware behavior.
No indispensable hardware-only blocker was demonstrated in this supported QEMU
contract. Interrupt remapping/MSI security, AMD support and raw WKI peer security
remain outside scope. Existing optional WKI query timeouts and its stale cached
payload constant remain documented; no broad WKI reliability claim is made.
