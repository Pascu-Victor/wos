# Physical GPU platform

Goal 41 is in progress. It provides the common platform needed by physical AMD
and NVIDIA drivers; it does not yet provide a GPU driver or a hardware support
claim. The selected deployment is a whole physical PCI function on bare metal
or passed to WOS by an external VFIO host. Emulated GPUs do not qualify it.

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

The legacy address/size/map wrappers preserve their zero/null error conventions.
Mapping failure no longer invents a 4 KiB resource. Bridges expose only their two
BARs; window registers are separate resources.

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

## Remaining platform work

These records permit one initial MSI-X activation per function and retain boot
resources. They do not establish general function claim/release ownership,
reset/rebind/relocation, hotplug or revocation ownership, expansion ROM,
Resizable BAR or safe arbitrary runtime probing. BME off alone does not prove
firmware engines quiescent. Modern virtio now explicitly inhibits inherited BME
before cold BAR preparation, then requires transport reset before admitting new
queue DMA. Global firmware handoff and AHCI/virtio bootstrap ownership still need
device-specific treatment. Existing xHCI/virtio activation relies on halted or
not-DRIVER_OK device state before handler publication. Large apertures still map
in proportion to their 4 KiB page count; bounded/on-demand mapping remains open.

The fixed 64-device capacity is shared by PCI, ECAM and IOMMU ownership metadata;
changing only enumeration is unsafe. Firmware verification/caching, device-backed
mmap, WC architecture policy, charged revocable user-page pins, GPU-scale SG,
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
