# Managed PCI DMA and Intel VT-d

WOS drivers use `dev/dma/dma.hpp` for coherent storage and streaming transfers.
AHCI, e1000e, virtio-net and xHCI obtain device addresses from managed
submissions. ivshmem is a shared PCI BAR transport, not a host-memory DMA engine;
its BAR and interrupt lifetime are retained through WKI/IRQ retirement.

## Boot policy

| Kernel option | Policy |
| --- | --- |
| `--iommu-strict` | Require Intel VT-d translation and independent, qualified requester domains. Any unsupported topology, capability, preparation or activation fails boot; no fallback. |
| `--iommu-translated` | Explicit development translation. Alias and nonisolated devices can share a group and therefore see that group's mappings. This is not per-device isolation. |
| No IOMMU option | Compatibility policy: hardware passthrough where supported, otherwise software passthrough. Boot logs explicitly report that DMA isolation is unavailable. |
| `--iommu-off` | Explicit software passthrough. Conflicts with either translation option. |

Strict mode is opt-in. Successful normal compatibility boots do not demonstrate
DMA isolation. Existing firmware-active Intel translation is refused rather
than replaced without ownership proof.

AMD IVRS firmware is structurally observed and privately retained, without AMD
register probing or an AMD-Vi backend. AMD-only compatibility/off boots remain
explicit software passthrough; AMD requested translation/strict mode refuses
with known bus masters inhibited. Ambiguous DMAR/IVRS selection (two non-absent
lookup results, including untrustworthy root errors) refuses every policy before
the off branch, so it cannot hide inherited active Intel state. The observer's
bounded grammar and unsupported features are documented in
[the GPU platform contract](gpu_platform.md#amd-firmware-observations-and-backend-policy).

Strict admission uses validated ACPI DMAR scopes, complete PCI topology and
bounded MCFG/configuration evidence. Integrated root-complex functions rely on
the Intel VT-d requirement for distinct requester IDs. Supported external PCIe
boundaries require ACS source validation, translation blocking, request and
completion redirection, and upstream forwarding; direct translated P2P is off.
WOS enables these supported ACS controls once during BSP boot, while every bus
master is disabled, and verifies readback before publishing any translation.
Discovery snapshots remain observations of the original firmware state.

PCI, ECAM and IOMMU ledgers share an explicit 256-function/512-requester boot
capacity. Traversal is iterative and growing preparation arrays use private BSP
scratch. Incomplete inventory refuses hardware preparation; compatibility
fallback does not establish isolation. Firmware-region membership remains a
separate 64-bit mask and reserved-map output stays bounded to 512 rows. See
[PCI inventory limits and ownership](gpu_platform.md#bounded-pci-inventory).

Conventional/PCIe-to-PCI aliases, shared groups, unqualified boundaries, enabled
Phantom/ARI/ATS/PRI/PASID/SR-IOV requester modes and unsupported virtio DMA devices
refuse strict admission. This conservative subset does not support every PCIe
switch or platform. Firmware RMRR mappings remain explicit, scoped, reserved,
identity-mapped and separate from driver allocation accounting.

## Ownership contract

Managed drivers first acquire an exclusive boot `pci::DriverClaim`. Its epoch
cannot transfer or repeat, even after probe failure. Closing it denies new DMA
allocation/preparation and hardware activation. Function operation guards stay
held through real mapping/publication or rollback, and driver shutdown drains
them alongside callback admission. A map closed before publication is invalidated
before free, or quarantined if invalidation is uncertain. Completion and release
of old owned records remain available after closure. Trusted kernel diagnostic
allocations may pin an anonymous claim on an otherwise unclaimed function.
The claim gate does not prove hardware quiescence or replace IRQ/DMA retirement.

1. Allocate coherent memory or a streaming pool in ordinary boot/task context.
   Allocation, mapping and release may wait; never call them while holding a
   driver spinlock or from IRQ/NAPI context.
2. Bind streaming storage to a kernel span and call `prepare`. Only its returned
   submission address may be made visible to the device. Keep borrowed CPU
   storage alive through completion or quarantine. Streaming transfers use
   exclusive bounce pages, preventing exposure of neighboring caller memory.
3. Prove command retirement, or stop/reset the device, and drain completion
   readers before `complete`. Its generation/sequence must match the submission.
   Receive completion copies only the reported completed bytes. Coherent rings
   require device quiescence.
4. Release only CPU-owned retired storage. Unmap and completed invalidation
   precede physical reuse. Failure to prove retirement or invalidation retains
   storage in quarantine; a timeout is not permission to free it.

Binding, preparation and completion use preallocated bounded metadata and can
run in the driver's serialized IRQ/NAPI path. Registry locks cover metadata;
allocation and IOMMU mutation happen outside that lock. The driver remains
responsible for serializing CPU accesses and device ownership. DMA masks and
alignment are checked; narrow masks can fail even when higher RAM is available.
The current backend assumes ordinary snooped x86 PCI DMA, not device No-Snoop
or arbitrary noncoherent operation.

Fault capture performs bounded interrupt work. Deferred diagnostics identify
unit, requester/domain, address, direction and known allocation generation.
The affected unit closes admission and inhibits bus mastering; uncertain mappings
and page tables remain pinned. Healthy terminal retirement returns driver
mapping/page counts to zero. Quarantine counts intentionally remain nonzero.

## Validation and limits

Historical qualification covered bounded DMAR parsing, strict requester-domain
admission, managed driver transfers, terminal retirement and fault quarantine.
The final diagnostic run recorded 68,578 assertions with zero failures; QEMU
fixtures also covered ACS admission and explicit refusal of unsupported
boundaries. These are prior results, not a validation of the current source.
The completed implementation ledger was removed during worktree cleanup on
2026-10-05, together with its private experiment artifacts.

Fresh fixtures use the isolated KTEST or cluster workflow with private
configuration and disks. Follow the current repository instructions for runtime
authorization and preserve the normal build and unrelated VMs.

QEMU qualification does not establish physical chipset, firmware, cache or
ACS behavior. The policy assumes a conforming trusted root complex and firmware.
It does not implement interrupt remapping, protect against MSI injection,
redesign ivshmem/WKI peer access, support AMD IOMMUs, or provide userspace device
passthrough. Small USB payloads have exercised actual chained TRBs; full large
USB transfers and natural physical 64-KiB crossings remain hardware qualification
limits, with boundary planning covered by host tests.
