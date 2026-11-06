# Physical GPU platform

Goal 41 is in progress. It provides the common platform needed by physical AMD
and NVIDIA drivers; it does not yet provide a GPU driver or a hardware support
claim. The selected deployment is a whole physical PCI function on bare metal
or passed to WOS by an external VFIO host. Emulated GPUs do not qualify it.

## AMD firmware observations and backend policy

`dev/iommu/ivrs.hpp` is a bounded, allocation-free IVRS syntax observer. It
retains raw revision/IVinfo and counts IVHD hardware blocks, IVMD memory blocks
and device entries. Alternative IVHD10/11/40 blocks can describe the same IOMMU;
the block count is not a unique-unit inventory. Syntax success grants no PCI
ownership, register mapping, DMA translation or interrupt-remapping authority.
AMD-Vi activation, requester normalization and IVMD mapping policy remain open.

The observer bounds the whole accessible table to 64 KiB, 256 blocks and 4096
entries. It checks the ACPI signature/length/checksum and header reserved bytes,
IVHD10/11/40 fixed lengths, IVMD20/21/22 framing and complete entry extents.
Ordinary and alias/extended ranges require a matched, nondecreasing inclusive
end; an alias range has one destination requester ID. Padding can intervene,
but other range interleaving is outside this subset. Entries below 0x80 have
4/8-byte framing. HID entry 0xf0 has 22 fixed bytes plus its bounded UID length;
other upper encodings are rejected rather than assigned an invented size.
HID/CID/UID values are retained without decoding or AML identity resolution.

Revisions 1/2, legacy capability offsets 0x40..0xfc aligned to four bytes,
nonzero page-aligned register bases and positive, nonwrapping full-page IVMD
extents are explicit WOS observer policies. They are not universal claims about
newer firmware. IVHD flags, feature/attribute words, EFR/EFR2 and IVMD segment
bytes are preserved. Extended alternatives/entries, HID, unknown safely framed
forms, unfamiliar padding and reserved-setting bits report unsupported semantics.
They never create an Intel `Unit` or resolved requester ownership. Failed parses
return no partial table or counts.

Layout provenance is [AMD publication 43872 Rev. 3.05, sections 8.2/8.3](https://www.amd.com/content/dam/amd/en/documents/archived-tech-docs/programmer-references/43872.pdf)
and [ACPICA's pinned IVRS declarations](https://github.com/open-acpica/acpica/blob/e85aa3bebaae81ed1e6d163511ac7e1eafe732cf/source/include/actbl2.h).
The older AMD document describes a particular chipset. The complete modern
[48882-PUB Rev. 3.11, April 2026](https://docs.amd.com/v/u/en-US/48882_3.11_IOMMU_PUB)
is now pinned locally (SHA256
`f7c375a15db5ed63de760356867211063d164a2ed59f2d38613daec95894ce22`).
Its reserved-field, revision, aperture and activation rules still need to be
implemented and qualified before a hardware backend is available.
The WOS byte parser is original code; no Linux driver implementation is imported.

Boot discovery looks for both DMAR and IVRS before any vendor register access,
copies the selected bounded bytes privately, then parses that retained copy.
AMD discovery never maps or probes its register banks. A single valid DMAR keeps
the existing Intel probe path. Two non-absent lookup results, including ambiguous
root/table errors, refuse every boot policy before `--iommu-off`: WOS cannot
safely choose a vendor or rule out inherited active Intel state. A single AMD
source supports only the existing explicitly nonisolating compatibility/off
fallback; requested translated/strict operation inhibits known PCI requesters
and refuses boot because the AMD-Vi backend is unavailable. Synthetic table
tests establish parsing/refusal behavior, not physical AMD hardware support.

## Bounded PCI inventory

`dev/pci_inventory_limits.hpp` supplies one 256-function capacity for the
nonmoving PCI identities/resources, ECAM snapshots and IOMMU ownership/domain
ledgers. Requester storage derives 512 slots, allowing each function's original
RID plus a bridge alias before deduplication. Registry indices/parents remain
16-bit; a source RID of 0xffff is still a valid BDF encoding. The supported
enumerator remains segment zero with its existing host-bridge root heuristic;
MCFG parsing does not imply discovery of every segment/root on a machine.

Bridge descent uses a fixed 256-bus cursor array in private boot storage instead
of recursion on Limine's 64 KiB bootstrap stack. It preserves preorder, parent
identity, absent-function0 suppression and the order of multifunction siblings
after function0's complete subtree. Visited-bus, invalid bridge and capacity
failures remain explicit. Repeated enumeration keeps the same exact pointers
and retained BAR/MSI-X cells without additional configuration access.

`pci_inventory_status()` returns stored count, capacity, enumeration/completeness,
first error and accumulated reason bits. Before serialized boot publication it
returns an unenumerated value. A stored prefix is not a required-total estimate:
an omitted bridge can hide an entire subtree. Incomplete inventory cannot supply
qualified ECAM/requester ownership or hardware DMA preparation; compatibility
software fallback remains explicitly nonisolating.

Growing domain preparation arrays use private constant-initialized BSP scratch,
not a large stack temporary. No scratch span becomes runtime ownership. The
always-resident function metadata grows by about 3.11 MiB; boot scratch and
exclusion storage add separate bounded costs. Full native frame/section audits
and controlled fixtures qualify a build, rather than assuming the 512 KiB task
stack is available during early boot.

Independent limits remain explicit: 64 firmware-region membership bits, 16
IOMMU units, 32 MCFG windows and 512 output reserved mappings. The 4096-row MMIO
exclusion scratch is checked against all platform rows plus seven per function;
overflow refuses the whole plan. A larger registry does not guarantee sufficient
hardware domain IDs, translation-table records or reserved-map output capacity,
and does not add hotplug, releasable resource claims or companion/reset authority.

## PCI resource validation

`dev/pci_resource.hpp` interprets complete Type0/Type1 BAR arrays. A 64-bit BAR
consumes both slots; callers cannot use its high word as another resource.
The helpers retain prefetchability and full addresses, reject unsupported
encodings, validate width-correct sizing masks and construct checked page-rounded
MMIO extents. The mapper uses actual CPU physical-address width and refuses
overlap with boot RAM, kernel/modules, ACPI data or framebuffer backing before
changing direct-map cache attributes. Unlisted or firmware-reserved device space
can pass these checks. Prefetchability alone does not authorize WC mappings.

`pci_get_bar_size_checked()` distinguishes an absent resource (zero) from an
invalid/stale BAR or failed probe (nullopt). The reserved-memory planner must
reject errors rather than omit their MMIO exclusions. The probe engine verifies
BAR restoration before restoring memory decoding; uncertain restoration attempts
to inhibit memory decoding and bus mastering. Host tests exercise the same
production transaction template, including failed readback.

Sizing also verifies all four read-only BAR attribute bits after restoring the
saved configuration. Only an entirely zero ordinary 32-bit BAR and zero raw
probe response prove absence. A 64-bit or prefetchable BAR with no address-mask
bits fails validation and poisons the retained function; it cannot enable bus
mastering or produce a mapping. A valid positive mask at address zero still
reports its size, independently of the mapper's address-zero refusal.

The legacy address/size/map wrappers preserve their zero/null error conventions.
Mapping failure no longer invents a 4 KiB resource. Bridges expose only their two
BARs; window registers are separate resources.

For controlled serial-only platform fixtures, the shared cluster/KTEST node
spec accepts `"vm": {"vga": "none"}`. This explicitly removes QEMU's default
VGA function; `-display none` alone only hides its output. Omit the option for
the usual layout. This fixture option provides no physical-GPU qualification
or guest authority to trust QMP-reported BAR sizes.

## Retained boot resources and MSI-X

The PCI registry now retains checked sizes and MMIO mappings for kernel lifetime.
Only exact registry pointers can prepare them. First preparation requires caller
quiescence and BME off; the complete BAR save/probe/restore uses one config lock.
Single-producer slots publish complete immutable values, return busy to competing
producers, and retain terminal failure. Mapping runs outside the config lock.
Public config writes cannot relocate retained BARs, including their high words
and partial byte/word writes. Invalid BDFs are rejected before port access.

`pci_prepare_msix()` runs before DMA activation and returns ready, absent or
error. It validates the full capability chain, retained table/PBA BARs, complete
extents and physical overlap. MSI-X is enabled with FunctionMask set before
table access; all inherited entries are masked before initial activation. The
PBA is retained without changing pending bits. A present malformed resource
fails probe; absent MSI-X preserves the MSI/INTx fallback.

Runtime entry programming uses the retained table only. Serialized masked writes
are flushed and verified before unmasking, without BAR probing or VM mapping.
CPU indices and the current 8-bit APIC destination limit are checked. Entry
masking shares this serialization, including ivshmem's IRQ/NAPI path. Unmasking
requires activation and a healthy domain. Failed programming attempts function
masking/BME inhibition and permanently denies later BME admission; backing stays
retained. CPU steering preserves the owned vector and callback lifetime; an
already-issued IRQ may reach its old CPU. Batch placement can have partial
changes when an IRQ update fails, and reports the error without claiming rollback.

## Exclusive boot driver claims

`dev/pci_driver_claim.hpp` gives each completely enumerated function one
move-only driver claim with a nonzero, nonreused boot epoch. AHCI, e1000e,
virtio-net, xHCI and both ivshmem transports acquire it before their first probe
control write. Competing probes cannot adopt the same function. A discarded or
failed claim stays retained until reboot; it cannot be rebound.

Closing a claim blocks new DMA allocation/preparation, cold resource preparation,
bus-master enable and IRQ activation/unmasking. Counted operations cover actual
DMA mapping, preparation, publication and rollback; terminal drains join them
with existing driver callbacks. Raw config writes preserve terminal INTx masks,
refuse new MSI/MSI-X enable/unmask and MSI steering, and cannot restore BME after
it has been cleared. Emergency masking and BME disable remain available.
Trusted kernel DMA diagnostics may reserve a permanently pinned anonymous claim
on an otherwise unclaimed function, outside the DMA registry lock.

An allocation closed during mapping unmaps and invalidates before physical free;
uncertain invalidation quarantines its backing. Old submissions can still complete
and release after closure under existing retirement proofs. A drained operation
gate proves software preparation ended. Hardware quiescence, IRQ retirement,
releasable MMIO and reset authority require their own proofs; these boot claims
do not grant any of them. Existing legacy BAR and MSI-X aliases remain pinned;
the opt-in typed register path below supplies separate mapping and retirement
authority.

## Opt-in cold UC register mappings

`--typed-pci-mmio` selects the kernel-only cold register path for the actual
Intel 82574/8086:10d3 consumer. The default legacy path remains available when
this option is absent. A selected typed probe has no raw mapping fallback.
`pci::map_cold_uc_82574(claim, controller, requests, empty_mapping)` consumes
facts from the actual exclusive driver claim and immutable hardware inventory.
Each `mmio::ViewRequest` names only a BAR index, byte offset and length. The
issuer checks positive page-aligned windows against exact probed BAR geometry
and attributes; callers cannot adopt a supplied physical address or copied
report. Requested windows must have disjoint physical pages, and resumption
must preserve the original plan. The three-argument overload keeps the original
full-BAR0 plan for existing callers.

The initial platform profile is a trusted, conforming, controlled q35-compatible
deployment. It observes bounded MCFG, MADT including the full type5 address,
HPET, FADT/GAS and discovered IOMMU/reserved ranges. HPET's fixed bank and
FADT's byte-accessed GPE blocks have dedicated format checks; scalar GAS
validation remains separate. Full-width EPBAR, MCHBAR, DMIBAR, PCIEXBAR, RCBA
and the supported DRAM/remap/SMRAM configuration join these exclusions. Active
opaque TSEG, integrated-graphics stolen allocations,
unknown decoder geometry, incomplete topology and unsupported capabilities
refuse typed admission. The QEMU profile uses the existing `vm.machine` value
`q35,smm=off`; a successful firmware boot still requires qualification. The
shared `vm.vga="none"` option removes the default VGA function for a controlled
headless fixture. Default VGA resource uncertainty can legitimately refuse the
NIC's typed mapping.

The chipset geometry comes from the [Intel 316966-002 datasheet](https://www.intel.com/Assets/PDF/datasheet/316966.pdf)
and the pinned [QEMU 11.1.3 q35 model](https://github.com/qemu/qemu/blob/v11.1.3/hw/pci-host/q35.c)
and [ICH9 model](https://github.com/qemu/qemu/blob/v11.1.3/hw/isa/lpc_ich9.c).
This restricted profile supplies no universal chipset or physical-GPU claim.
Foreign BARs are never sized live. Their complete possible extents, ROMs and
bridge forwarding windows are retained conservatively: an unknown ordinary
zero32 BAR remains a possible [0,2GiB) decoder. The common conventional writer
fence covers effective byte/word/dword writes, geometry and reset strobes;
ECAM ACS preparation finishes before inventory sealing. Disabled decoders stay
fenced, and a foreign MEM enable cannot change its immutable footprint.

Before any driver-side BME clear, the factory requires observed BME0, disabled
inherited MSI/MSI-X and no prior WOS register-map, IRQ or DMA producer. It closes
new DMA admission while retaining its own counted operation, inhibits IO/MEM,
masks INTx and holds ROM disabled with verified readback. It then requires all
other operations drained and checks the actual DMA registry outside the config
lock. Only this serialized transaction sizes every own memory BAR and verifies
restoration.
BAR0 must cover the consumer's highest register offset; assigned-positive zero
addresses, overlap and malformed sizing fail. Complete resource extents are
bounded by the actual CPU physical width and validated HHDM geometry, with at
most six memory spans. Their size does not allocate page tables. The aggregate
materialized child windows remain limited to 16 MiB and eight views. Large sparse
resource ownership does not supply GPU-scale aperture mapping or WC policy.
Cold describes WOS CPU exposure; firmware usage and engine quiescence remain
separate obligations.

One `mmio::Mapping` owns the function and its complete physical exclusions.
Its immutable set of at most eight child views has separate guarded VA ranges,
qualified UC policies, preparation progress and original CPU tickets. All own
memory BARs remain fenced, including gaps, unmapped tails and other BARs with
no child view. Preparing a child does not grant ownership of a second function
or permit another cache alias.

`platform/mm/mmio_mapping` begins bounded physical-exposure history during boot
before generic mapping publication. After the complete PCI inventory, before
AHCI, AP startup and Task roots, it creates the private shared PML4[505]
bank only when platform/possible-HHDM extents leave that namespace available.
Unsupported profiles leave ordinary boot available without creating that bank.
All own memory spans become boot-persistent physical tombstones. Nonreserved
boot memory, previous WOS exposure and present aliases refuse the cold path;
the implementation preserves existing RESERVED direct mappings.

Physical admission is resumable. An early private handle retains the exact
claim and view plan without reserving VA, publishing leaves, enabling MEM or
admitting DMA. Each step performs at most 128 HHDM walk visits under the actual
MM pin lock. A mutation epoch, real kernel-root frame and HHDM identity bind all
resource spans; an intervening publication restarts the entire scan. Present
aliases, including HHDM translations to different RAM, refuse admission. The
final pin transaction rechecks boot, exposure-history, private-frame and retained
resource exclusions, then commits all resource fences and child VA reservations
together. Epoch saturation refuses new admission without wrapping.

The actual boot consumer resumes the same claim, plan and Controller only for
`pending_retained`. A qualified monotonic five-second budget and a finite call
limit bound starting new calls. A successful call remains successful even if its
bounded work crosses that deadline. Abandoned, uncommitted handles retain only
private metadata; committed ownership retains its existing fences and tickets.

The actual MM pin serialization protects the dedicated VA namespace, encoded
incoming and displaced physical spans, and private page-table frames across
legacy mutation and root-copy/free paths. Device pages receive no PMM ownership
or generic frame decrement. Private table frames remain retained. Geometry,
capacity or history uncertainty refuses new typed work before publication.

Each child's initial preparation retains a genuine qualified UC range and its
own CPU maintenance ticket. Every configured ready CPU completes current-root
TLB invalidation before the WBINVD round; both rounds revalidate actual state.
Inherited PAT, MTRR/protected-range policy and NX/WP prerequisites remain
mandatory. Supervisor RW/NX UC leaves publish only after exact completion and
handler drain. Every requested child must finish preparation before PCI enables
MEM, exposes the public Mapping and reopens DMA admission. Partial preparation
retains the same private function handle, issued tickets and completed child
progress. No config/MM lock spans allocation or a CPU wait.

The move-only `mmio::Mapping` supplies scoped, IRQ-safe `Borrow` objects rather
than raw register pointers. One Borrow leases the whole immutable child set;
`read32(view_index, byte_offset)` and `write32(view_index, byte_offset, value)`
check the function generation, selected child, alignment and bounds. Existing
single-offset overloads select view zero. Copied reports expose each view's
geometry and tickets; the original scalar geometry and ticket fields describe
view zero. Existing borrows survive function admission closure. Mapping
destruction closes and retains the whole function; it never stops hardware or
unmaps a live register window. Child views cannot be independently unmapped or
added after publication.

The actual e1000 consumer routes its existing register offsets through BAR0
windows `[0,0x1000)` and `[0x2000,0x6000)`. These materialize five 4 KiB pages,
compared with 32 pages for a full 128 KiB BAR0. The intervening gap and remaining
BAR0 tail retain the parent's exclusions. A single normal or terminal Borrow
covers both views, preserving the driver's callback and hardware operations.
The unique terminal Borrow requires function closure and zero normal readers.

Retirement preserves the established 82574 master drain, interrupt masking,
BME disable, callback/IRQ drain and DMA release order. A closed typed claim may
mask INTx while preserving an already-live BME under the final config lock;
this exception admits neither fresh BME nor new DMA. After the terminal borrow's
last register access and destruction, the actual driver's private
`QuiescedMapping` witness binds the function. Decode-off readback happens once;
each child then withdraws its own PTEs and completes its original all-CPU TLB
ticket before its VA allocation becomes reusable. The function reports complete
only after every child completes, with normal and terminal readers drained.
Physical tombstones and private tables stay pinned until reboot. Timeout
preserves the pending child's original ticket; an accepted finalizer skips
completed children on retry without accessing BARs again or repeating hardware,
IRQ or DMA release.

The public scheduled DAEMON maintenance API keeps its existing contract. Private
nonmovable Controllers borrow authority synchronously from the actual phase5
InitExecutor or the accepted power finalizer on its own kernel stack. Tickets,
mappings and driver storage retain no Task or issuer back-reference. Boot calls
preserve incoming IRQ state; finalizer retries stay in that accepted
continuation, before other CPUs halt. Reset/rebind, physical storage return,
hotplug, WC mappings and userspace BAR mappings remain outside this path.

The maintained `--selftest-e1000-mmio` diagnostic exercises the actual typed
consumer, scoped borrowers and network traffic, then asks PID1 to perform its
normal shutdown. It checks both sparse views and their guarded translations.
The selftest-only `--selftest-e1000-mmio-admission-steps` option requires the
existing typed diagnostic flags and reduces the actual resource walk to one
visit per call, exercising boot-caller continuation across the NIC's own BARs.
`--selftest-e1000-mmio-withdraw-retry` additionally withholds one real remote
action for the first child's withdrawal and retries that same ticket. Host fixtures
link actual engines with finite hardware ports. These interfaces do not by
themselves establish native or physical-GPU qualification; receipts must identify
the frozen source, artifacts, actual CPU completion and guest shutdown.

## Immutable firmware service

`dev/firmware` provides a kernel-only service for named raw blobs. Set
`WOS_FIRMWARE_ARCHIVE=/absolute/path/firmware.cpio` when packaging to add a
separate newc module tagged `wos-firmware-v1`. Entries use
`lib/firmware/<canonical-relative-name>`. Both image backends and stock/generated
Limine menus support it; the boot manifest includes its hash. Ordinary boots
need no firmware archive. Compressed archives/blobs are not decompressed.

Serialized boot capture validates exactly one tagged source before reading its
payload: checked HHDM conversion, complete page-rounded kernel/module reservation
without conflicting memory descriptors, actual supervisor read-only/NX physical
translations, CR0.WP and no kernel-image physical alias. It retains only immutable
payload/index views. Reserved firmware modules never reach the writable TMPFS
unpacker or fallback executable handover, even when capture fails. This does not
harden the general initramfs unpacker or change its trusted-bootloader contract.

The bounded allocation-free scanner validates the entire archive, including
unused records and the trailer, before publishing an index. It rejects malformed
metadata, canonical aliases, firmware links and nonzero padding. Limits are
512MiB/source, 256MiB/blob, 4096 records including the trailer, 512 indexed files,
512-byte archive paths and 256-byte relative names including NUL, and 511 trailing
zero bytes. Duplicate detection uses bounded quadratic header/name work at boot;
it neither rereads nor hashes every prior payload.

A trusted kernel `Policy` pins canonical name, exact/max length, SHA256 and a
revision matched by the caller. Revision identifies the catalog's hash-bound
bytes; it is not a vendor-format parser. A manifest accompanying untrusted bytes
cannot supply policy authority. The service reuses the existing allocation-free
SHA256 implementation without changing WKI protocols.

Use a stack-owned `firmware::Scope` in ordinary task context, outside IRQ/fault,
spin, VM, IOMMU and driver locks. `request(policy, revision, view)` returns checked
immutable bytes or an explicit error while leaving failed output unchanged.
Concurrent identical policies share one generation/object and verification;
conflicting live policies fail. `release(view)` rejects foreign, stale and
duplicate leases. Destruction drains all remaining leases. Views are borrowed;
copying one does not acquire ownership, and use ends when its lease is released.

Scopes cannot move or cross tasks. Each holds cooperative task shutdown ownership
through its whole lifetime, including mutex waits and scheduling. Explicit task
exit with a live Scope is forbidden; unsafe/cross-task destruction is fatal.
Unsafe requests/releases/snapshots refuse before the sleeping mutex. Hashing
occurs under that task mutex and performs no I/O, callbacks or allocation. The
cache has 64 metadata slots, 1024 references/object and 64 leases/scope. Last
release evicts metadata; generation exhaustion fails without wrap. Source bytes
remain reserved until shutdown. Firmware possession grants no DMA/device access.

`scripts/test/make_firmware_fixture.py OUTPUT` creates only redistributable WOS
test bytes for the isolated `--selftest-firmware` scheduled test. The fixture's
policy is independently compiled into the test kernel. This is service evidence,
not qualification of any AMD/NVIDIA firmware catalog, processor or GPU driver.

The scheduled diagnostic requests normal PID 1 poweroff after all firmware
owners return. Shutdown preparation must preserve demand paging, I/O, GC and
File retirement providers while it returns to userspace. Provider retirement
belongs after consumer quiescence in the nonreturning finalizer; firmware hashes
or a successful lifetime test cannot substitute for that ordering.

## Optional CPU register reads

`platform/asm/cpu_safe_msr.hpp` provides `try_read_msr()` for optional cache-policy
registers. It requires CPUID.MSR and verifies the current CPU's installed WOS
IDTR before probing. Only #GP(0) at the single dedicated kernel `RDMSR` site
recovers; failure returns false without changing the output. Interrupt entry
edits the actual saved return RIP. Unrelated kernel faults and userspace faults
retain their normal handling. The probe/fixup and local-IDTR check avoid
sanitizer, coverage and GS stack-canary dependencies in early boot.

This primitive does not qualify MTRRs, AMD cache overrides, MMIO cache types or
physical resources. Those checks and the ordered all-CPU TLB/cache operations
remain prerequisites for releasable UC/WC mappings. Existing mandatory MSR
helpers and their callers are unchanged.
The API cannot detect a VMM silently accepting an unknown register with a zero
result; future cache qualification must retain its platform and semantic checks.

## Inherited cache policy and device mappings

`platform/mm/mmio_cache` separately captures later CPU state after the local
IDT, GS and x2APIC are installed. Each configured CPU publishes one immutable
terminal observation. Actual GS/APIC identity is checked under a bounded IRQ
guard; unsupported state leaves ordinary boot available. The inherited early
PAT observations keep their existing role.

The qualifier resolves fixed/variable/default MTRRs over finite physical
interval boundaries and compares accessible cache policy, full PAT values and
protected exclusions across the CPU cohort. Current documented register layouts
cover eight AMD and ten Intel variable pairs; larger enabled counts require a
proven index map and are refused before probing. Inactive fields stay dormant.
Intel PAT-UC composition and AMD's distinct CD result are handled separately.
AMD SYS_CFG, IORR and TOP_MEM routing are checked independently of MTRR enable;
TOM2 force-WB changes the default after explicit variable-range selection.

Intel SMRR/SEAMRR and AMD SMM intervals are excluded before ordinary type
composition. AMD SMM controls use the generic APM layout or the explicitly
selected Family17h 00h–2fh profile. Unobservable/model-specific PRMRR/EPC state,
unprofiled extensions, active encrypted-address geometry and unsupported cache
modes return named refusals. The source/layout provenance is
[Intel SDM 093](https://cdrdv2-public.intel.com/929363/335592-093-sdm-vol-4.pdf),
[AMD APM 3.46](https://docs.amd.com/api/khub/documents/LNBl5evreogi0r25VIFF2g/content),
and the scoped [Family17h OSRR 3.03](https://www.amd.com/content/dam/amd/en/documents/processor-tech-docs/programmer-references/56255_OSRR.pdf).

Only the actual captured cohort can mint a private `QualifiedUcRange` for exact
page-rounded bounds. Partial/mismatched cohorts cannot seal; duplicate capture
and detected hardware drift invalidate admission permanently. Local validation
streams the observed registers and restores incoming IRQ state. It does not
refresh the snapshots. Snapshot consistency assumes conforming firmware/VMM
behavior, including meaningful optional-register observations.

The opt-in UC register mapper combines these CPU tokens with independently
issued PCI ownership, absent aliases and the actual completed all-CPU barrier.
CPU tokens alone grant none of those other authorities. WC, active encrypted
mappings and physical GPU qualification remain Goal41 work.

`platform/mm/cpu_maintenance` provides a separate scheduled CPU-maintenance
rendezvous. A private move-only ticket retains a genuine qualified UC range in
one of 16 permanent slots. All configured CPUs, including the controller's CPU,
must have independently reached the scheduler-ready boundary. Parked early
diagnostic APs supply no readiness. Each CPU validates
its current hardware state and reloads its actual current CR3. Only an explicit
controller wait can start the cache round after every translation completion;
each CPU then validates again and executes fenced WBINVD. The current profile
requires PGE, PCIDE and LA57 disabled.

Controllers are ordinary scheduled DAEMONs with real shutdown ownership,
interrupts enabled and preemption depth zero, outside MM, PCI and driver locks.
Waiting uses a qualified monotonic clock, a finite work budget and bounded IPI
retries. A timeout retains the request, token and late-handler storage; a later
explicit wait can retry the same ID. Observation and late acknowledgments never
start another round. Abandoned published requests consume bounded capacity
until reboot. Completion requires exact per-CPU generations, closed reader
admission and drained handler tails before metadata reuse.

Copied completion reports grant no PTE, alias, BAR or storage-release authority.
The typed UC mapper retains its actual physical ownership, withdrawn translations
and mutation exclusion across maintenance. Scheduled CPU actions with existing
mappings alone do not prove a safe cache-type transition. The private bootstrap
and accepted-finalizer Controllers provide the scoped integration described
above; legacy synchronous shootdowns keep their existing behavior. Physical
GPU qualification remains separate.

Bootstrap preserves each framebuffer page's incoming PAT selector when replacing
Limine's page tables. It verifies the inherited supervisor translation and
physical address, converts large-leaf PAT bit 12 to 4 KiB leaf bit 7, and audits
the final root before switching CR3. The audit retains only page counts, selector
bits and matching physical-address/selector hashes; it never reads framebuffer
contents or retains the bootloader's page-table root.

The explicit 4 KiB leaf constructor preserves PAT bit 7. Ancestors receive only
PRESENT, WRITE and the requested USER bit; leaf cache, NX and software flags
cannot select the page-table memory type or become ancestor policy. The legacy
entry constructor still clears bit 7 for its nonleaf role.

Read-only per-CPU PAT observations are captured before CPUs enter scheduling.
CPUID gates the privileged read. All configured CPU reports must arrive before
WB-at-0, UC-at-3 or WC-at-5 encoding readiness is published. Unsupported PAT
leaves WC unavailable; mismatches are observable, and duplicate or invalid
captures permanently invalidate readiness without replacing observations. This
does not qualify MTRRs, cache aliases or physical resources and grants no new WC
mapping authority. WOS does not write PAT or MTRRs in this path.

Files without an admitted retained device provider still reject nonregular mmap
with ENODEV after open/mount authorization and before allocation, reads, lazy
publication or MAP_FIXED replacement. Device files cannot silently become copied
or zero-filled anonymous pages. Regular-file mappings retain their existing paths.

## Retained endpoint and mapping ownership

`dev/device_mapping_client` owns a character endpoint's Device wrapper, copied
operation table, name and context. Its checked move-only references retain that
storage across namespace replacement and unlink. Callback code is built into the
kernel; this API does not support unloading it. Typed endpoints use generic
client creation and reject legacy open callbacks that could redirect the wrapper.
Legacy endpoints keep their existing callbacks and devfs prefix layout.

Each typed open attaches one client to the shared `File`, independently of
backend private data. It copies the admitted credentials and explicit mount/open
ceilings; it owns no Task or File back-reference. Dup and inherited descriptors
share that File/client. Backend close runs while the client is live; client
release precedes mount-owner release. Typed publication replaces an existing
inode with a fresh node, preserving the old File's identity and metadata.
Unlinked metadata references continue to retain their endpoint. Endpoint
admission closure refuses new clients and preserves existing ones; it does not
claim mapping revocation. Dynamic endpoints stay out of the borrowed Device
registry until typed lookup ownership exists there.

`FileBindingSnapshot` owns an exact binding SNAPSHOT role captured by the real
mapping index. Its root address is identity only. `BindingRootMutationLease`
requires that sealed owner and the current executor's actual publication guard,
tries the permanent root gate, and validates the registered serial and exact
indexed binding before root access. It acquires no Task or root-frame reference.
Attempts are one-shot; busy callers must drop guards before retrying. A borrowed
snapshot cannot move or reset until its lease finishes.

Owned retirement observations distinguish INDEXED, TLB_PENDING and TLB_COMPLETE.
Only the existing post-invalidation batch return publishes COMPLETE. This does
not prove pins/installers ended, destructive File callbacks returned, or a device
stopped DMA. Existing shootdown waits are not made bounded by try-only root
admission.

## Coherent system-page mapping providers

`dev/device_mapping_object` owns zeroed ordinary PMM pages and a separately
retained quota account. Admission requires complete CPU PAT observations with
selector zero encoding WB. This does not establish MTRR or aperture authority.
Resident charges cover objects even when unmapped; address-space aliases remain
subject to each process's AS limit. The diagnostic PMM tag USER_SHARED_MEMORY
does not replace either quota. Configurable object limits default to 1 GiB;
checked allocation and actual permanent retirement-ticket capacity can refuse.

A provider configures a per-open Client grant table (default/maximum 256 entries).
Grants identify complete globally nonreused page-aligned offset intervals, each
followed by a guard page. Lookup uses only that retained File's Client, supports
base-plus-page-delta, and intersects the object, grant and captured open/mount
rights. Removal closes future lookup; a prepared operation also rechecks object
state and generation at publication. No physical address is accepted as a user
handle. Grant creation is a kernel-provider API; generic non-PTY ioctl requests
still do not pass unchecked user pointers to a driver.

The initial mapping class is eager shared ordinary WB system pages, NX, with
READ, WRITE or NONE protections. PROT_NONE retains its owned supervisor PTE and
binding. Private, executable, aperture, doorbell and UC/WC mappings are not
admitted. Occupied MAP_FIXED replacement, mixed device/other-backing changes,
device mremap and device msync refuse before effects. Partial device-only unmap
and protection changes prepare replacement bindings before withdrawal. Holes are
permitted for unmap and return ENOMEM for protection. Ordinary mutation routes
recheck device coverage under their final publication guard.

Preparation allocates loans, bindings and table spares before publication.
Commit retains actual root authority across complete validation, bounded IRQ
slices and the real TLB barrier. A typed fork attaches child bindings after its
genuine empty-child proof and before copying shared leaves. Raw copiers refuse
device sources. The existing counted remap suspension also excludes new device
publication while generic relocation copies its source.

Each family owns the exact File, Object generation and immutable object-relative
window/protection ceiling. Objects own no File, Client, Task or root. Reverse
binding membership is nonowning and persists from preparation through callback
start, including detached translations; zero-owner callback rows cannot be
resurrected. Independent user-frame exports, including queued futex frames,
retain an IRQ-safe counted Object export without retaining a binding or Task.
Their PMM reference returns before the export token. Final Object/account puts
submit pre-reserved actual callbacks; backing and charges return before the
executor publishes callback completion.

`device_vm::revoke_cpu_mappings` permanently closes admission, captures actual
bindings and withdraws each through its exact typed root lease. EAGAIN preserves
closed storage for retry while roots, prepared operations, TLB retirement,
callbacks, pins or exports remain. A recycled/missing root is not an ACK. Final
Object counts decide drain after snapshots are dropped. This is CPU mapping
revocation; it is not DMA, interrupt, bounded hardware reset or storage-reuse
qualification.

## Remaining platform work

These records permit one initial MSI-X activation per function and retain boot
resources. They do not establish general function claim/release ownership,
reset/rebind/relocation, hotplug or revocation ownership, expansion ROM,
Resizable BAR or safe arbitrary runtime probing. BME off alone does not prove
firmware engines quiescent. Modern virtio now explicitly inhibits inherited BME
before cold BAR preparation, then requires transport reset before admitting new
queue DMA. Global firmware handoff and AHCI/virtio bootstrap ownership still need
device-specific treatment. Existing xHCI/virtio activation relies on halted or
not-DRIVER_OK device state before handler publication. Large legacy apertures
still map in proportion to their 4 KiB page count.
GPU-scale and on-demand aperture mapping remain open; the opt-in cold register
service retains large resource extents but materializes at most 16 MiB of UC
child windows as described above.

The shared 256-function capacity now covers PCI, ECAM and IOMMU ownership
metadata with bounded traversal; it does not add resource/reset authority.
Vendor firmware catalogs, GPU-scale aperture/doorbell and userspace BAR
mappings, WC architecture policy, charged revocable user-page pins, GPU-scale SG,
AMD-Vi and interrupt remapping also remain open. Existing Intel DMA strict mode
does not provide MSI isolation. Reuse the current MM binding/root/TLB lifetime
contracts when adding device mappings.

Physical qualification needs named AMD and NVIDIA fixtures. The local AMD GPU
is the active host display and is preserved. Inventory is not WOS runtime proof.
Agents are authorized to run/debug available fixtures directly; use
`wos-cluster --no-setup` and `wos-ktest --no-setup` for rootless launches. Keep
isolated tests separate from normal artifacts and prune exact-owned inactive
disposable images after passing and failed runs, preserving required seeds and
small evidence.

## Software validation

The host tests include the actual `pci.cpp` translation unit with a simulated
CF8/CFC bank, a nonrecursive lock and mapping backend. They check complete probe
serialization, mapping outside the lock, copied/invalid identities, terminal
failure, retained BAR write refusal and zero runtime steering config/probe/map
operations. Separate production-helper tests inject posted-write and readback
failures. These are software tests, not physical-device isolation evidence.

The maintained `scripts/test/napi_irq_recovery.py` fixture exercises prepared
modern MSI-X, live IRQ/NAPI recovery and ordinary DMA/IRQ retirement. During
qualification it exposed inherited BME preventing modern cold BAR mapping;
the corrected fresh boot records BME inhibition and passes all 16 IRQ cases.
The failed original run and exact cleanup evidence remain preserved. CPU
steering on real hardware, firmware handoff and physical GPU qualification
remain separate requirements.

Firmware host tests exercise the production scanner/cache, actual Scope service
with explicit host task/mutex ports, boot-source qualification helpers and both
image packaging backends. The scheduled firmware VM uses actual tasks, mutex,
verified source and shutdown ownership: two requests share one object/hash,
negative policies retain no objects, and 1,000 release cycles return to baseline.
Its early normal PID 1 poweroff exposed premature provider retirement; the
corrected production path shuts down journald, XFS and device DMA cleanly.
The ordinary KTEST boot also rejects a corrupt final archive padding byte without
publishing its otherwise valid first blob. These checks pass on synthetic WOS
test bytes; vendor firmware/device qualification is still required.
