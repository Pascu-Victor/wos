#pragma once

#include <cstddef>
#include <cstdint>
#include <dev/iommu/fault.hpp>
#include <dev/iommu/groups.hpp>
#include <dev/iommu/page_table.hpp>
#include <dev/iommu/reserved.hpp>

namespace ker::dev::pci {
struct PCIDevice;
}
namespace ker::dev::iommu {

enum class DomainMode : uint8_t { unavailable, software_passthrough, hardware_passthrough, quarantined, translated };
struct DomainInfo {
    DomainMode mode = DomainMode::unavailable;
    uint16_t id = 0;
    uint16_t unit = UINT16_MAX;
    uint16_t group = groups::NONE;
    uint16_t members = 0;
    uint64_t firmware_pages = 0;
};
struct DomainStats {
    size_t devices = 0;
    size_t table_pages = 0;
    size_t active_units = 0;
    uint64_t firmware_pages = 0;
    bool ready = false;
    bool strict = false;
};
// BSP boot only, after discovery and before AHCI. Kernel-lifetime domains.
// Strict implies translated and requires independent requester groups and
// qualified source boundaries; any preparation failure refuses boot.
void initialize_domains(bool disabled, bool strict, bool translated = false);
[[nodiscard]] auto domain_info(const pci::PCIDevice* device) -> DomainInfo;
[[nodiscard]] auto domain_stats() -> DomainStats;
// Immutable hardware group/requester publication; null in software fallback.
[[nodiscard]] auto group_inventory() -> const groups::Result*;
// Firmware-owned identity mappings, retained for platform lifetime; separate
// from software DMA allocations. Never authorize CPU access or reclamation.
[[nodiscard]] auto reserved_mappings() -> std::span<const reserved::Mapping>;
// BSP boot after start_smt established GS/per-CPU state and init task ownership.
// Fault IRQ/metadata have kernel lifetime.
void start_fault_worker();
struct UnitStatus {
    bool failed = false;
    bool inhibited = false;
    uint64_t irq_count = 0;
};
[[nodiscard]] auto unit_status(size_t unit) -> UnitStatus;
// Immutable after acquire publication; null until a real hardware fault capture.
[[nodiscard]] auto fault_report(size_t unit) -> const vtd::FaultReport*;
[[nodiscard]] auto bus_master_allowed(const pci::PCIDevice* device) -> bool;
// Serialized virtio-net probe/reset only: driver asserts modern ACCESS_PLATFORM
// plus FEATURES_OK was accepted. Revoke BEFORE reset; this does not stop DMA,
// authorize release, or override a failed unit. Mapping preparation is separate.
[[nodiscard]] auto set_platform_dma_ready(const pci::PCIDevice* device, bool negotiated) -> bool;
// Boot/task context with cooperative shutdown ownership held; no driver/IRQ
// locks. Identity IOVA, exact PTE permissions, synchronous invalidation proof.
[[nodiscard]] auto map(const pci::PCIDevice* device, uint64_t physical, size_t bytes, vtd::Access access) -> vtd::TableResult;
[[nodiscard]] auto unmap(const pci::PCIDevice* device, uint64_t address, uint64_t physical, size_t bytes, vtd::Access access) -> bool;
#ifdef WOS_SELFTEST
// Withhold one completed invalidation proof, never synthesize a hardware fault.
[[nodiscard]] auto selftest_withhold_invalidation(const pci::PCIDevice* device) -> bool;
// Configure before boot allocation only; used to prove partial-prepare unwind.
[[nodiscard]] auto selftest_limit_table_allocations(size_t successful_allocations) -> bool;
#endif
}  // namespace ker::dev::iommu
