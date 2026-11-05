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

## Remaining platform work

This checkpoint does not establish retained resource/reset ownership, whole
probe-transaction serialization, quiescence, safe runtime reprobes, MSI-X
table/PBA extent validation, expansion ROM or Resizable BAR ownership. In
particular, existing MSI-X runtime steering still invokes BAR sizing. Drivers
must serialize probe/quiescence; per-access config locking alone is insufficient.

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
