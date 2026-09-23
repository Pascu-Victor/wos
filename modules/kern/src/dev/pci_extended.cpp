#include "pci_extended.hpp"

#include <array>
#include <atomic>
#include <dev/iommu/domains.hpp>
#include <dev/pci.hpp>
#include <platform/acpi/acpi.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/virt.hpp>

namespace ker::dev::pci {
namespace {
using log = mod::dbg::logger<"pci-ecam">;
constexpr size_t MAX_DEVICES = 64;
std::array<ExtendedConfig, MAX_DEVICES> snapshots{};
ExtendedInventory inventory{};
std::atomic<bool> published{false};
std::atomic<bool> acs_attempted{false};

auto safe_window(const ecam::Window& window, uint8_t physical_bits) -> bool {
    auto const* memory = mod::mm::boot_memory_map();
    if (memory == nullptr || memory->entries == nullptr) return false;
    return ecam::safe_aperture(window, physical_bits, mod::mm::addr::get_hhdm_offset(), memory->entry_count,
                               [memory](size_t i) -> std::optional<ecam::MemoryRegion> {
                                   auto const* entry = memory->entries[i];
                                   if (entry == nullptr) return std::nullopt;
                                   return ecam::MemoryRegion{entry->base, entry->length, entry->type == LIMINE_MEMMAP_RESERVED};
                               });
}
struct Reader {
    volatile uint8_t* base;
    auto write16(uint16_t offset, uint16_t value) const -> bool {
        if (offset % 2 != 0 || offset > 4094) return false;
        *reinterpret_cast<volatile uint16_t*>(base + offset) = value;
        return true;
    }
    auto read16(uint16_t offset) const -> std::optional<uint16_t> {
        if (offset % 2 != 0 || offset > 4094) return std::nullopt;
        return *reinterpret_cast<volatile uint16_t*>(base + offset);
    }
    auto read32(uint16_t offset) const -> std::optional<uint32_t> {
        if (offset % 4 != 0 || offset > 4092) return std::nullopt;
        return *reinterpret_cast<volatile uint32_t*>(base + offset);
    }
};
void collect() {
    inventory.devices = pci_device_count();
    if (!pci_enumeration_complete() || inventory.devices > MAX_DEVICES) return;
    auto const FOUND = mod::acpi::find_table_bounded("MCFG");
    if (!FOUND.valid()) {
        inventory.state = FOUND.error == mod::acpi::bounded::Error::not_found ? ExtendedState::unavailable : ExtendedState::firmware_error;
        return;
    }
    auto const TABLE = ecam::parse(FOUND.bytes);
    if (!TABLE.valid()) {
        inventory.state = ExtendedState::table_error;
        inventory.table_error = TABLE.error;
        return;
    }
    mod::cpu::CpuidContext cpu{.function = static_cast<int>(0x80000000U)};
    mod::cpu::cpuid(&cpu);
    if (static_cast<uint32_t>(cpu.eax) < 0x80000008U) {
        inventory.state = ExtendedState::unsafe_window;
        return;
    }
    cpu.function = static_cast<int>(0x80000008U);
    mod::cpu::cpuid(&cpu);
    uint8_t const PHYSICAL_BITS = cpu.eax & 255U;
    for (size_t i = 0; i < TABLE.count; ++i)
        if (!safe_window(TABLE.windows[i], PHYSICAL_BITS)) {
            inventory.state = ExtendedState::unsafe_window;
            return;
        }
    inventory.windows = TABLE.count;
    for (size_t i = 0; i < inventory.devices; ++i) {
        auto const* device = pci_get_device(i);
        auto& result = snapshots[i];
        if (device == nullptr) continue;
        auto const ADDRESS = ecam::address(TABLE, 0, device->bus, device->slot, device->function, 0, 4);
        if (!ADDRESS) {
            result.state = ExtendedState::missing_window;
            continue;
        }
        result.physical = *ADDRESS;
        mod::mm::virt::map_range_to_kernel_page_table({*ADDRESS, *ADDRESS + 4096}, mod::mm::virt::kernel_mmio_page_flags());
        Reader reader{reinterpret_cast<volatile uint8_t*>(mod::mm::addr::get_virt_pointer(*ADDRESS))};
        result.capabilities = ecam::inspect(reader, device->vendor_id, device->device_id, device->header_type);
        result.state = result.capabilities.error == ecam::CapabilityError::identity ? ExtendedState::identity_error
                       : result.capabilities.valid()                                ? ExtendedState::ready
                                                                                    : ExtendedState::capability_error;
        if (result.state == ExtendedState::ready) ++inventory.readable;
    }
    inventory.state = ExtendedState::ready;  // Individual snapshots must also be checked.
}
}  // namespace

void collect_extended_config() {
    if (published.load(std::memory_order_acquire)) return;
    collect();
    log::info("inventory=%u error=%u windows=%zu devices=%zu readable=%zu; observation only, admission evaluated by IOMMU",
              static_cast<unsigned>(inventory.state), static_cast<unsigned>(inventory.table_error), inventory.windows, inventory.devices,
              inventory.readable);
    for (size_t i = 0; i < inventory.devices && i < snapshots.size(); ++i) {
        auto const* device = pci_get_device(i);
        if (device == nullptr) continue;
        auto const& result = snapshots[i];
        auto const& cap = result.capabilities;
        log::info(
            "bdf=%02x:%02x.%x state=%u error=%u physical=0x%llx pcie=%u type=%u ext=%u acs=0x%x/0x%x ats=0x%x pri=0x%x pasid=0x%x "
            "sriov=0x%x devctl=0x%x devctl2=0x%x",
            device->bus, device->slot, device->function, static_cast<unsigned>(result.state), static_cast<unsigned>(cap.error),
            static_cast<unsigned long long>(result.physical), cap.pcie_version, cap.pcie_type, cap.extended_count, cap.acs_capability,
            cap.acs_control, cap.ats_control, cap.pri_control, cap.pasid_control, cap.sriov_control, cap.device_control,
            cap.device_control2);
    }
    published.store(true, std::memory_order_release);
}
auto extended_inventory() -> ExtendedInventory { return published.load(std::memory_order_acquire) ? inventory : ExtendedInventory{}; }
auto extended_config(const PCIDevice* device) -> const ExtendedConfig* {
    if (!published.load(std::memory_order_acquire) || device == nullptr) return nullptr;
    for (size_t i = 0; i < inventory.devices && i < snapshots.size(); ++i)
        if (pci_get_device(i) == device) return &snapshots[i];
    return nullptr;
}
auto enable_strict_acs(std::span<ecam::Capabilities> capabilities) -> bool {
    if (!published.load(std::memory_order_acquire) || inventory.state != ExtendedState::ready || inventory.devices != capabilities.size() ||
        inventory.devices > snapshots.size() || iommu::domain_stats().ready || acs_attempted.exchange(true, std::memory_order_acq_rel))
        return false;
    // Complete this pass before the first write: no downstream requester may
    // remain active while an upstream boundary changes routing controls.
    for (size_t i = 0; i < inventory.devices; ++i) {
        auto const* device = pci_get_device(i);
        if (device == nullptr || snapshots[i].state != ExtendedState::ready ||
            (pci_config_read16(device->bus, device->slot, device->function, PCI_COMMAND) & PCI_COMMAND_BUS_MASTER) != 0)
            return false;
    }
    for (size_t i = 0; i < inventory.devices; ++i) {
        auto const* device = pci_get_device(i);
        Reader config{reinterpret_cast<volatile uint8_t*>(mod::mm::addr::get_virt_pointer(snapshots[i].physical))};
        auto const RESULT = ecam::enable_acs(config, device->vendor_id, device->device_id, device->header_type);
        if (!RESULT) return false;
        capabilities[i] = *RESULT;
        if (RESULT->acs != 0)
            log::info("ACS readback bdf=%02x:%02x.%x capability=0x%x control=0x%x; BME off", device->bus, device->slot, device->function,
                      RESULT->acs_capability, RESULT->acs_control);
    }
    return true;
}
}  // namespace ker::dev::pci
