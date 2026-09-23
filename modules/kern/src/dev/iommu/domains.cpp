#include "domains.hpp"

#include <array>
#include <atomic>
#include <dev/dma/dma.hpp>
#include <dev/iommu/discovery.hpp>
#include <dev/iommu/page_storage.hpp>
#include <dev/iommu/reserved_runtime.hpp>
#include <dev/iommu/vtd_activation.hpp>
#include <dev/pci.hpp>
#include <dev/pci_extended.hpp>
#include <new>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/smt/smt.hpp>
#include <util/hcf.hpp>

namespace ker::dev::iommu {
namespace {
using log = mod::dbg::logger<"iommu">;
using Table = std::array<vtd::Entry, 256>;
static_assert(sizeof(Table) == vtd::PAGE_SIZE);
struct RuntimeUnit {
    Table* root = nullptr;
    std::array<Table*, 256> contexts{};
    vtd::Activation state = vtd::Activation::unprepared;
    std::atomic_flag busy = ATOMIC_FLAG_INIT;
    std::atomic<bool> failed{false};
    std::atomic<uint8_t> inhibition{0}, fault_state{0};
    std::atomic<bool> inhibited{false};
    std::atomic<uint64_t> fault_irqs{0};
    vtd::FaultReport faults{};
    uint8_t fault_vector = 0;
    bool reported = false;
#ifdef WOS_SELFTEST
    bool withhold_invalidation = false;
#endif
    vtd::Invalidation invalidation{};
    const Unit* hardware = nullptr;
    size_t index = 0;
};
std::array<RuntimeUnit, MAX_UNITS> units{};
std::array<DomainInfo, scopes::MAX_DEVICES> domains{};
groups::Result grouping{};
std::array<reserved::Mapping, reserved::MAX_MAPPINGS> firmware_mappings{};
reserved::Plan firmware_plan{};
std::array<std::atomic<bool>, scopes::MAX_DEVICES> platform_dma_ready{};
auto invalidate_unit(void* context) -> bool;
struct Translation {
    std::array<vtd::TableRecord, 512> records{};
    vtd::PageStorage storage;
    vtd::PageTable<vtd::PageStorage> table;
    vtd::TableSnapshot prepared{};
    std::atomic<size_t> pages{0};
    Translation(RuntimeUnit& unit, size_t cache_line)
        : storage(records, unit.hardware->capability.coherent, cache_line, invalidate_unit, &unit), table(storage) {}
    void count() {
        auto const SNAPSHOT = table.snapshot();
        if (SNAPSHOT) pages.store(SNAPSHOT->table_pages, std::memory_order_release);
    }
};
std::array<std::optional<Translation>, scopes::MAX_DEVICES> translations{};
std::atomic<bool> ready{false};
DomainStats stats{};
#ifdef WOS_SELFTEST
size_t allocation_budget = SIZE_MAX;
#endif

[[noreturn]] void fail_boot(const char* reason) {
    // Do not free any hardware-visible page or re-enable a requester here.
    for (size_t i = 0; i < pci::pci_device_count(); ++i) (void)pci::pci_set_bus_master(pci::pci_get_device(i), false);
    for (auto& domain : domains) domain.mode = DomainMode::quarantined;
    ready.store(false, std::memory_order_release);
    mod::dbg::panic_handler(reason);
    hcf();
}

auto physical(const void* pointer) -> uint64_t { return reinterpret_cast<uint64_t>(pointer) - mod::mm::addr::get_hhdm_offset(); }

auto allocate_table(uint8_t bits) -> Table* {
#ifdef WOS_SELFTEST
    if (allocation_budget == 0) return nullptr;
    --allocation_budget;
#endif
    void* storage = mod::mm::phys::page_alloc_may_fail(mod::mm::PhysicalPageOwner::IOMMU_TABLE, vtd::PAGE_SIZE, "iommu_table");
    if (storage == nullptr) return nullptr;
    if (!vtd::page_address_valid(physical(storage), bits)) {
        mod::mm::phys::page_free(storage);
        return nullptr;
    }
    ++stats.table_pages;
    return new (storage) Table{};
}

void release_unpublished_tables() {
    for (auto& unit : units) {
        if (unit.fault_vector != 0) {
            // Delivery was never enabled on this pre-activation unwind.
            mod::gates::free_irq(unit.fault_vector);
            unit.fault_vector = 0;
        }
    }
    for (auto& translation : translations) {
        if (!translation) continue;
        if (!translation->table.discard_unpublished()) fail_boot("Attempt to discard a busy or exposed DMA root");
        translation.reset();
    }
    for (auto& unit : units) {
        for (auto*& context : unit.contexts) {
            if (context != nullptr) {
                mod::mm::phys::page_free(context);
                context = nullptr;
                --stats.table_pages;
            }
        }
        if (unit.root != nullptr) {
            mod::mm::phys::page_free(unit.root);
            unit.root = nullptr;
            --stats.table_pages;
        }
    }
}

auto cache_line_bytes() -> size_t {
    mod::cpu::CpuidContext cpu{.function = 1};
    mod::cpu::cpuid(&cpu);
    if ((static_cast<uint32_t>(cpu.edx) & (1U << 19U)) == 0) return 0;
    size_t const BYTES = ((static_cast<uint32_t>(cpu.ebx) >> 8U) & 255U) * 8U;
    return BYTES >= 8 && BYTES <= vtd::PAGE_SIZE && (BYTES & (BYTES - 1)) == 0 ? BYTES : 0;
}

void publish_table(Table* table, bool coherent, size_t line_bytes) {
    if (!coherent) {
        auto const* bytes = reinterpret_cast<const uint8_t*>(table);
        for (size_t i = 0; i < sizeof(Table); i += line_bytes) asm volatile("clflush (%0)" ::"r"(bytes + i) : "memory");
    }
    asm volatile("mfence" ::: "memory");
}

struct Registers {
    volatile uint8_t* base;
    auto read32(uint32_t offset) const -> uint32_t { return *reinterpret_cast<volatile uint32_t*>(base + offset); }
    auto read64(uint32_t offset) const -> uint64_t { return *reinterpret_cast<volatile uint64_t*>(base + offset); }
    void write32(uint32_t offset, uint32_t value) { *reinterpret_cast<volatile uint32_t*>(base + offset) = value; }
    void write64(uint32_t offset, uint64_t value) { *reinterpret_cast<volatile uint64_t*>(base + offset) = value; }
    template <typename Read>
    auto wait(Read read, uint64_t mask, uint64_t expected) const -> bool {
        uint64_t const START = mod::time::get_monotonic_ns();
        for (size_t i = 0; i < 1000000; ++i) {
            if ((read() & mask) == expected) return true;
            if (mod::time::get_monotonic_ns() - START >= 250000000) return false;
            asm volatile("pause" ::: "memory");
        }
        return false;
    }
    auto wait32(uint32_t offset, uint32_t mask, uint32_t expected) const -> bool {
        return wait([&] { return read32(offset); }, mask, expected);
    }
    auto wait64(uint32_t offset, uint64_t mask, uint64_t expected) const -> bool {
        return wait([&] { return read64(offset); }, mask, expected);
    }
};

auto registers(const Unit& unit) -> Registers {
    return {reinterpret_cast<volatile uint8_t*>(mod::mm::addr::get_virt_pointer(unit.physical))};
}

// The gate is held across table allocation and invalidation but never disables
// interrupts or scheduling. Caller owns a shutdown hold and normal task context.
struct UnitGuard {
    RuntimeUnit& unit;
    bool admitted = false;
    explicit UnitGuard(RuntimeUnit& value) : unit(value) {
        uint64_t const START = mod::time::get_monotonic_ns();
        for (;;) {
            if (unit.failed.load(std::memory_order_acquire)) return;
            if (!unit.busy.test_and_set(std::memory_order_acquire)) {
                admitted = true;
                return;
            }
            if (!mod::mm::phys::can_wait_for_reclaim() || mod::time::get_monotonic_ns() - START >= 5000000000ULL) return;
            mod::sched::kern_yield();
        }
    }
    ~UnitGuard() {
        if (admitted) unit.busy.clear(std::memory_order_release);
    }
    UnitGuard(const UnitGuard&) = delete;
    auto operator=(const UnitGuard&) -> UnitGuard& = delete;
};

void quarantine_unit(RuntimeUnit& unit) {
    unit.failed.store(true, std::memory_order_release);
    uint8_t expected = 0;
    if (!unit.inhibition.compare_exchange_strong(expected, 1, std::memory_order_acq_rel)) return;
    // Final enable checks run under PCI config_lock, so every late requester
    // either precedes this disable or observes the closed unit admission.
    bool stopped = true;
    for (size_t i = 0; i < stats.devices; ++i)
        if (domains[i].unit == unit.index) stopped &= pci::pci_set_bus_master(pci::pci_get_device(i), false);
    unit.inhibited.store(stopped, std::memory_order_release);
    unit.inhibition.store(2, std::memory_order_release);
    log::error("unit=%zu quarantined; BME inhibited=%u; all tables and uncertain DMA backing retained", unit.index,
               static_cast<unsigned>(stopped));
}

void capture_unit_fault(RuntimeUnit& unit, bool from_irq) {
    if (from_irq) unit.fault_irqs.fetch_add(1, std::memory_order_relaxed);
    auto io = registers(*unit.hardware);
    uint32_t const STATUS = io.read32(vtd::FSTS);
    if ((STATUS & vtd::FAULT_STATUS) == 0) return;
    uint8_t expected = 0;
    if (!unit.fault_state.compare_exchange_strong(expected, 1, std::memory_order_acq_rel)) return;
    io.write32(vtd::FECTL, io.read32(vtd::FECTL) | vtd::FAULT_MASK);
    unit.failed.store(true, std::memory_order_release);
    vtd::capture_faults(io, unit.hardware->capability.fault_offset, unit.hardware->capability.fault_records, STATUS, unit.faults);
    for (size_t n = 0; n < unit.faults.count; ++n) {
        auto& record = unit.faults.records[n];
        if (!record.stable) continue;
        for (size_t i = 0; i < grouping.requester_count; ++i) {
            auto const& requester = grouping.requesters[i];
            size_t const OWNER = requester.owner;
            if (domains[OWNER].unit != unit.index || record.source() != requester.sid) continue;
            record.domain = domains[OWNER].id;
            record.domain_generation = translations[OWNER] ? translations[OWNER]->prepared.generation : 0;
            record.identified = true;
            break;
        }
    }
    unit.fault_state.store(2, std::memory_order_release);
}
void fault_irq(uint8_t, void* context) { capture_unit_fault(*static_cast<RuntimeUnit*>(context), true); }

auto prepare_fault_irq(RuntimeUnit& unit) -> bool {
    auto io = registers(*unit.hardware);
    io.write32(vtd::FECTL, io.read32(vtd::FECTL) | vtd::FAULT_MASK);
    if ((io.read32(vtd::FSTS) & vtd::FAULT_STATUS) != 0) return false;  // Do not discard firmware fault evidence.
    auto const APIC = mod::smt::get_apic_id_for_cpu(0);
    if (APIC > 255) return false;  // Legacy physical MSI destination, no truncation.
    unit.fault_vector = mod::gates::allocate_vector();
    if (unit.fault_vector == 0) return false;
    if (mod::gates::request_irq(unit.fault_vector, fault_irq, &unit, "iommu-fault") != 0) return false;
    io.write32(vtd::FEDATA, unit.fault_vector);
    io.write32(vtd::FEADDR, 0xfee00000U | (APIC << 12U));
    io.write32(vtd::FEUADDR, 0);
    return io.read32(vtd::FEDATA) == unit.fault_vector && io.read32(vtd::FEADDR) == (0xfee00000U | (APIC << 12U)) &&
           io.read32(vtd::FEUADDR) == 0 && (io.read32(vtd::FECTL) & vtd::FAULT_MASK) != 0;
}

void fault_worker() {
    for (;;) {
        {
            auto hold = mod::sched::hold_kernel_shutdown();
            for (auto& unit : units) {
                if (!unit.failed.load(std::memory_order_acquire)) continue;
                quarantine_unit(unit);
                if (unit.reported || unit.fault_state.load(std::memory_order_acquire) != 2) continue;
                unit.reported = true;
                log::error("fault unit=%zu status=0x%x remaining=0x%x records=%zu invalid=%u irq=%llu; admission closed", unit.index,
                           unit.faults.status, unit.faults.remaining, unit.faults.count, unit.faults.invalid,
                           static_cast<unsigned long long>(unit.fault_irqs.load(std::memory_order_acquire)));
                for (size_t n = 0; n < unit.faults.count; ++n) {
                    auto const& fault = unit.faults.records[n];
                    dma::MappingInfo mapping{};
                    uint16_t allocation_owner = groups::NONE;
                    bool source_alias = false;
                    size_t matches = 0;
                    if (fault.stable && fault.identified) {
                        for (size_t r = 0; r < grouping.requester_count; ++r)
                            if (grouping.requesters[r].sid == fault.source()) source_alias = grouping.requesters[r].alias;
                        for (size_t i = 0; i < stats.devices; ++i) {
                            if (domains[i].unit != unit.index || domains[i].id != fault.domain) continue;
                            auto const candidate = dma::mapping_info(pci::pci_get_device(i), fault.address());
                            if (candidate.handle.registry == nullptr) continue;
                            mapping = candidate;
                            allocation_owner = static_cast<uint16_t>(i);
                            ++matches;
                        }
                    }
                    // Never attribute an ambiguous address to the last member.
                    if (matches != 1) {
                        mapping = {};
                        allocation_owner = groups::NONE;
                    }
                    log::error(
                        "fault unit=%zu record=%u allocation_generation=%llu allocation_known=%u quarantined=%u release_pending=%u "
                        "allocation_owner=%u source_alias=%u matches=%zu",
                        unit.index, fault.index, static_cast<unsigned long long>(mapping.handle.generation),
                        mapping.handle.registry != nullptr, mapping.quarantined, mapping.release_pending, allocation_owner, source_alias,
                        matches);
                    log::error(
                        "fault unit=%zu record=%u sid=0x%x domain=%u domain_generation=%llu address=0x%llx access=%s reason=0x%x "
                        "identified=%u stable=%u raw_high=0x%llx",
                        unit.index, fault.index, fault.source(), fault.domain, static_cast<unsigned long long>(fault.domain_generation),
                        static_cast<unsigned long long>(fault.address()), fault.type(), fault.reason(), fault.identified, fault.stable,
                        static_cast<unsigned long long>(fault.high));
                }
            }
        }
        mod::sched::kern_sleep_us(2000);
    }
}

auto invalidate_unit(void* context) -> bool {
    auto& unit = *static_cast<RuntimeUnit*>(context);
    auto io = registers(*unit.hardware);
    bool completed = !unit.failed.load(std::memory_order_acquire) &&
                     vtd::invalidate_live(io, unit.invalidation, physical(unit.root), unit.hardware->capability.iotlb_offset);
#ifdef WOS_SELFTEST
    if (unit.withhold_invalidation) {
        unit.withhold_invalidation = false;
        completed = false;
    }
#endif
    // A fault can arrive while the task owns the unit gate. Check again after
    // register completion; fault capture itself never waits for this gate.
    if (completed && !unit.failed.load(std::memory_order_acquire)) return true;
    quarantine_unit(unit);
    return false;
}

auto prepare(const Discovery& inventory, size_t cache_line, bool translated, bool strict) -> bool {
    if (inventory.state != DiscoveryState::discovered || !inventory.ownership.valid() || !pci::pci_enumeration_complete()) return false;
    std::array<scopes::Device, scopes::MAX_DEVICES> topology{};
    std::array<groups::Evidence, scopes::MAX_DEVICES> evidence{};
    std::array<pci::ecam::Capabilities, scopes::MAX_DEVICES> strict_capabilities{};
    if (strict && !pci::enable_strict_acs(std::span{strict_capabilities}.first(stats.devices))) return false;
    for (size_t i = 0; i < stats.devices; ++i) {
        auto const* device = pci::pci_get_device(i);
        if (device == nullptr) return false;
        // 1af4:1110 is the shared-BAR ivshmem device, not a virtio DMA device.
        if (translated && device->vendor_id == 0x1af4 && device->device_id != 0x1110 &&
            !(device->class_code == pci::PCI_CLASS_NETWORK && (device->device_id == 0x1000 || device->device_id == 0x1041))) {
            log::warn("translated admission refused: unsupported virtio device");
            return false;
        }
        topology[i] = {.bus = device->bus,
                       .slot = device->slot,
                       .function = device->function,
                       .header = static_cast<uint8_t>(device->header_type & 0x7fU),
                       .parent = device->parent_index,
                       .secondary = device->secondary_bus,
                       .subordinate = device->subordinate_bus};
        auto const* snapshot = pci::extended_config(device);
        if (snapshot != nullptr)
            evidence[i] = {.readable = snapshot->state == pci::ExtendedState::ready, .capability = snapshot->capabilities};
        if (strict) evidence[i] = {.readable = true, .capability = strict_capabilities[i]};
    }
    grouping = groups::qualify(std::span<const scopes::Device>{topology}.first(stats.devices),
                               std::span<const groups::Evidence>{evidence}.first(stats.devices), inventory.ownership, strict);
    if (!grouping.valid()) {
        log::warn("hardware groups refused: error=%u device=%u strict=%u", static_cast<unsigned>(grouping.error), grouping.error_device,
                  strict);
        return false;
    }
    for (size_t i = 0; i < inventory.unit_count; ++i) {
        auto const& unit = inventory.units[i];
        auto const& cap = unit.capability;
        if (unit.state != UnitState::available || (!translated && !cap.passthrough) || (!cap.coherent && cache_line == 0)) return false;
        auto const IO = registers(unit);
        if ((unit.cap & (uint64_t{1} << 61U)) != 0 && (IO.read64(0x410) & 1U) != 0) return false;
        if ((unit.cap & (uint64_t{3} << 5U)) != 0 && (IO.read32(0x64) & 0x80000001U) != 0) return false;
        uint64_t const COMMAND_END = uint64_t{cap.iotlb_offset} + 8U;
        uint64_t const FAULT_END = uint64_t{cap.fault_offset} + uint64_t{cap.fault_records} * 16U;
        uint64_t const END = ((COMMAND_END > FAULT_END ? COMMAND_END : FAULT_END) + vtd::PAGE_SIZE - 1) & ~(vtd::PAGE_SIZE - 1);
        mod::mm::virt::map_range_to_kernel_page_table({unit.physical, unit.physical + END}, mod::mm::virt::kernel_mmio_page_flags());
        units[i].hardware = &unit;
        units[i].index = i;
        units[i].root = allocate_table(cap.physical_bits);
        if (units[i].root == nullptr) return false;
    }
    if (translated) {
        firmware_plan = reserved::prepare(inventory, grouping, firmware_mappings);
        if (!firmware_plan.valid()) {
            log::warn("reserved DMA admission refused: error=%u", static_cast<unsigned>(firmware_plan.error));
            return false;
        }
        stats.firmware_pages = firmware_plan.pages;
    }
    std::array<uint32_t, MAX_UNITS> next_id{};
    next_id.fill(1);  // DID zero is reserved on caching-mode hardware.
    std::array<vtd::Entry, scopes::MAX_DEVICES> entries{};
    for (size_t i = 0; i < stats.devices; ++i) {
        size_t const GROUP = grouping.owners[i];
        if (GROUP != i) {
            domains[i] = domains[GROUP];
            continue;
        }
        size_t const OWNER = inventory.ownership.devices[i].unit;
        if (OWNER >= inventory.unit_count) return false;
        auto const& cap = inventory.units[OWNER].capability;
        uint32_t const ID = next_id[OWNER]++;
        if (ID >= cap.domains) return false;
        auto& unit = units[OWNER];
        std::optional<vtd::Entry> entry;
        if (translated) {
            auto& translation = translations[i].emplace(unit, cache_line);
            if (translation.table.initialize(cap.physical_bits, cap.iova_bits, cap.levels) != vtd::TableError::none) return false;
            for (size_t r = 0; r < firmware_plan.count; ++r) {
                auto const& mapping = firmware_mappings[r];
                if (mapping.group != GROUP) continue;
                for (uint64_t offset = 0; offset < mapping.bytes;) {
                    constexpr uint64_t CHUNK = 64 * 1024 * 1024;
                    uint64_t const BYTES = mapping.bytes - offset < CHUNK ? mapping.bytes - offset : CHUNK;
                    if (translation.table.map(mapping.base + offset, mapping.base + offset, BYTES, vtd::Access::read_write).error !=
                        vtd::TableError::none)
                        return false;
                    offset += BYTES;
                }
            }
            auto const SNAPSHOT = translation.table.snapshot();
            if (!SNAPSHOT) return false;
            translation.prepared = *SNAPSHOT;
            translation.count();
            entry = vtd::context_entry(SNAPSHOT->root, cap.physical_bits, static_cast<uint16_t>(ID), cap.levels, cap.supported_agaw,
                                       cap.domains);
        } else {
            entry = vtd::passthrough_entry(static_cast<uint16_t>(ID), cap.supported_agaw, cap.domains, cap.passthrough);
        }
        if (!entry) return false;
        entries[i] = *entry;
        uint16_t members = 0;
        for (size_t j = 0; j < stats.devices; ++j) members += grouping.owners[j] == i;
        uint64_t firmware_pages = 0;
        for (size_t r = 0; r < firmware_plan.count; ++r)
            if (firmware_mappings[r].group == GROUP) firmware_pages += firmware_mappings[r].bytes / vtd::PAGE_SIZE;
        domains[i] = {.mode = translated ? DomainMode::translated : DomainMode::hardware_passthrough,
                      .id = static_cast<uint16_t>(ID),
                      .unit = static_cast<uint16_t>(OWNER),
                      .group = static_cast<uint16_t>(GROUP),
                      .members = members,
                      .firmware_pages = firmware_pages};
    }
    for (size_t i = 0; i < grouping.requester_count; ++i) {
        auto const& requester = grouping.requesters[i];
        auto const& domain = domains[requester.owner];
        auto& unit = units[domain.unit];
        auto const& cap = unit.hardware->capability;
        uint8_t const BUS = requester.sid >> 8U;
        auto*& context = unit.contexts[BUS];
        if (context == nullptr) {
            context = allocate_table(cap.physical_bits);
            if (context == nullptr) return false;
            auto const ROOT = vtd::root_entry(physical(context), cap.physical_bits);
            if (!ROOT) return false;
            (*unit.root)[BUS] = *ROOT;
        }
        (*context)[requester.sid & 255U] = entries[requester.owner];
    }
    for (size_t i = 0; i < inventory.unit_count; ++i) {
        for (auto* context : units[i].contexts)
            if (context != nullptr) publish_table(context, inventory.units[i].capability.coherent, cache_line);
        publish_table(units[i].root, inventory.units[i].capability.coherent, cache_line);
        if (translated && !prepare_fault_irq(units[i])) return false;
    }
    return true;
}

void publish_fallback(const char* reason) {
    release_unpublished_tables();
    grouping = {};
    firmware_plan = {};
    stats.firmware_pages = 0;
    for (size_t i = 0; i < stats.devices; ++i) domains[i] = {.mode = DomainMode::software_passthrough};
    stats.ready = true;
    ready.store(true, std::memory_order_release);
    log::warn("domains=software-passthrough devices=%zu tables=%zu reason=%s; DMA isolation unavailable", stats.devices, stats.table_pages,
              reason);
}
}  // namespace

void initialize_domains(bool disabled, bool strict, bool translated) {
    if (ready.load(std::memory_order_acquire)) return;
    if (disabled && (translated || strict)) fail_boot("Conflicting IOMMU off and translated/strict options");
    translated = translated || strict;
    stats.strict = strict;
    stats.devices = pci::pci_device_count();
    if (stats.devices > domains.size()) fail_boot("PCI inventory exceeds DMA domain capacity");
    auto const* inventory = discovery();
    if (inventory == nullptr) fail_boot("DMA domain initialization preceded firmware discovery");
    for (size_t i = 0; i < inventory->unit_count; ++i)
        if (inventory->units[i].state == UnitState::firmware_active) fail_boot("Cannot take over firmware-active IOMMU state");
    if (disabled) {
        publish_fallback("explicit --iommu-off");
        return;
    }
    std::array<bool, scopes::MAX_DEVICES> was_master{};
    for (size_t i = 0; i < stats.devices; ++i) {
        auto* device = pci::pci_get_device(i);
        was_master[i] =
            (pci::pci_config_read16(device->bus, device->slot, device->function, pci::PCI_COMMAND) & pci::PCI_COMMAND_BUS_MASTER) != 0;
        if (!pci::pci_set_bus_master(device, false)) fail_boot("Cannot disable PCI bus mastering for IOMMU activation");
    }
    if (!prepare(*inventory, cache_line_bytes(), translated, strict)) {
        if (translated) {
            release_unpublished_tables();
            fail_boot(strict ? "Requested strict DMA isolation cannot be safely prepared"
                             : "Requested translated DMA domains cannot be safely prepared");
        }
        publish_fallback("unavailable capabilities, topology or table storage");
        for (size_t i = 0; i < stats.devices; ++i)
            if (was_master[i] && !pci::pci_set_bus_master(pci::pci_get_device(i), true))
                fail_boot("Cannot restore explicit fallback bus mastering");
        return;
    }
    // These tokens were used to construct the unpublished context entries.
    // Mark every tree exposed BEFORE writing the first hardware root pointer.
    for (auto& translation : translations)
        if (translation && !translation->table.expose(translation->prepared)) fail_boot("Stale DMA translation root token");
    for (size_t i = 0; i < inventory->unit_count; ++i) {
        auto const& unit = inventory->units[i];
        auto io = registers(unit);
        units[i].state = vtd::activate(io, physical(units[i].root), unit.capability.physical_bits, unit.capability.iotlb_offset);
        if (units[i].state != vtd::Activation::active) {
            log::error("activation failed unit=%zu stage=%u; table pages pinned=%zu, bus mastering disabled", i,
                       static_cast<unsigned>(units[i].state), stats.table_pages);
            fail_boot("IOMMU activation failed; exposed table storage is quarantined");
        }
        ++stats.active_units;
        if (translated) {
            io.write32(vtd::FECTL, io.read32(vtd::FECTL) & ~vtd::FAULT_MASK);
            if ((io.read32(vtd::FECTL) & vtd::FAULT_MASK) != 0) fail_boot("Cannot enable IOMMU fault delivery");
        }
        log::info("unit=%zu TE=1 root=0x%llx mode=%s", i, static_cast<unsigned long long>(physical(units[i].root)),
                  translated ? "translated" : "passthrough; DMA isolation unavailable");
    }
    stats.ready = true;
    ready.store(true, std::memory_order_release);
    for (size_t i = 0; i < stats.devices; ++i)
        if (!translated && was_master[i] && !pci::pci_set_bus_master(pci::pci_get_device(i), true))
            fail_boot("Cannot restore admitted PCI bus mastering");
    // Bridge BME admits forwarding only after every original/alias context is
    // live. Endpoint drivers retain control of their own BME admission.
    for (size_t i = 0; i < stats.devices; ++i) {
        auto* device = pci::pci_get_device(i);
        if (translated && (device->header_type & 0x7fU) == pci::PCI_HEADER_TYPE_BRIDGE && !pci::pci_set_bus_master(device, true))
            fail_boot("Cannot admit bridge forwarding after DMA group activation");
        log::info("group device=%zu sid=0x%x owner=%u members=%u unit=%u domain=%u reason=0x%x", i,
                  (device->bus << 8U) | (device->slot << 3U) | device->function, domains[i].group, domains[i].members, domains[i].unit,
                  domains[i].id, grouping.reasons[i]);
    }
    log::info("firmware mappings=%zu pages=%llu; retained reserved ownership, excluded from driver DMA accounting", firmware_plan.count,
              static_cast<unsigned long long>(stats.firmware_pages));
    for (size_t r = 0; r < firmware_plan.count; ++r) {
        auto const& mapping = firmware_mappings[r];
        log::info("firmware group=%u base=0x%llx bytes=0x%llx identity=RW", mapping.group, static_cast<unsigned long long>(mapping.base),
                  static_cast<unsigned long long>(mapping.bytes));
    }
    log::info("groups=%zu requesters=%zu; shared members have collective DMA visibility", grouping.count, grouping.requester_count);
    log::info("domains=%s devices=%zu tables=%zu active_units=%zu; %s", translated ? "translated" : "hardware-passthrough", stats.devices,
              domain_stats().table_pages, stats.active_units,
              strict       ? "strict independent DMA domains; source boundaries qualified"
              : translated ? "explicit mappings only; development qualification in progress"
                           : "DMA isolation unavailable");
}

void start_fault_worker() {
    static bool started = false;
    if (started || !ready.load(std::memory_order_acquire)) return;
    bool needed = false;
    for (auto const& unit : units) needed |= unit.fault_vector != 0;
    if (!needed) return;
    auto* worker = mod::sched::task::Task::create_kernel_thread("iommu_fault", fault_worker);
    if (worker == nullptr || !mod::sched::post_task(worker)) fail_boot("Cannot start IOMMU fault reporter");
    started = true;
}
auto unit_status(size_t index) -> UnitStatus {
    if (!ready.load(std::memory_order_acquire) || index >= units.size()) return {};
    auto& unit = units[index];
    return {unit.failed.load(std::memory_order_acquire), unit.inhibited.load(std::memory_order_acquire),
            unit.fault_irqs.load(std::memory_order_acquire)};
}
auto fault_report(size_t index) -> const vtd::FaultReport* {
    if (!ready.load(std::memory_order_acquire) || index >= units.size() || units[index].fault_state.load(std::memory_order_acquire) != 2)
        return nullptr;
    return &units[index].faults;
}

auto domain_info(const pci::PCIDevice* device) -> DomainInfo {
    if (!ready.load(std::memory_order_acquire) || device == nullptr) return {};
    for (size_t i = 0; i < stats.devices; ++i)
        if (pci::pci_get_device(i) == device) {
            auto result = domains[i];
            if (result.unit < units.size() && units[result.unit].failed.load(std::memory_order_acquire))
                result.mode = DomainMode::quarantined;
            return result;
        }
    return {};
}
auto group_inventory() -> const groups::Result* { return ready.load(std::memory_order_acquire) && grouping.valid() ? &grouping : nullptr; }
auto reserved_mappings() -> std::span<const reserved::Mapping> {
    if (!ready.load(std::memory_order_acquire) || !firmware_plan.valid()) return {};
    return std::span<const reserved::Mapping>{firmware_mappings}.first(firmware_plan.count);
}
auto domain_stats() -> DomainStats {
    if (!ready.load(std::memory_order_acquire)) return {};
    auto result = stats;
    // A concurrent mutation may allocate between snapshots. Exact allocator
    // equality requires a quiescent checkpoint; these counters never data-race.
    for (auto& translation : translations)
        if (translation) result.table_pages += translation->pages.load(std::memory_order_acquire);
    return result;
}
auto bus_master_allowed(const pci::PCIDevice* device) -> bool {
    auto const MODE = domain_info(device).mode;
    if (MODE == DomainMode::software_passthrough || MODE == DomainMode::hardware_passthrough) return true;
    if (MODE != DomainMode::translated) return false;
    if (device->vendor_id != 0x1af4 || device->device_id == 0x1110) return true;
    for (size_t i = 0; i < stats.devices; ++i)
        if (pci::pci_get_device(i) == device) return platform_dma_ready[i].load(std::memory_order_acquire);
    return false;
}
auto set_platform_dma_ready(const pci::PCIDevice* device, bool negotiated) -> bool {
    if (device == nullptr || device->vendor_id != 0x1af4 || device->class_code != pci::PCI_CLASS_NETWORK ||
        (device->device_id != 0x1000 && device->device_id != 0x1041))
        return false;
    for (size_t i = 0; i < stats.devices; ++i) {
        if (pci::pci_get_device(i) != device) continue;
        auto const MODE = domain_info(device).mode;
        bool const ALLOWED =
            MODE == DomainMode::translated || MODE == DomainMode::software_passthrough || MODE == DomainMode::hardware_passthrough;
        platform_dma_ready[i].store(negotiated && ALLOWED, std::memory_order_release);
        // Unit failure independently wins every BME admission check.
        return !negotiated || ALLOWED;
    }
    return false;
}
auto mutate(const pci::PCIDevice* device, uint64_t address, uint64_t physical, size_t bytes, vtd::Access access, bool removing)
    -> vtd::TableResult {
    auto const DOMAIN = domain_info(device);
    if (DOMAIN.mode != DomainMode::translated) return {.error = vtd::TableError::unavailable, .retain_data = removing};
    auto const RESERVED = reserved_mappings();
    if (reserved::overlaps(RESERVED, DOMAIN.group, address, bytes) || reserved::overlaps_any(RESERVED, physical, bytes))
        return {.error = vtd::TableError::overlap, .retain_data = removing};
    auto& unit = units[DOMAIN.unit];
    UnitGuard guard(unit);
    if (!guard.admitted) return {.error = vtd::TableError::busy, .retain_data = removing};
    if (unit.failed.load(std::memory_order_acquire)) return {.error = vtd::TableError::quarantined, .retain_data = removing};
    for (size_t i = 0; i < stats.devices; ++i) {
        if (pci::pci_get_device(i) != device || DOMAIN.group >= translations.size() || !translations[DOMAIN.group]) continue;
        auto& translation = *translations[DOMAIN.group];
        auto result =
            removing ? translation.table.unmap(address, physical, bytes, access) : translation.table.map(address, physical, bytes, access);
        translation.count();
        if (result.error == vtd::TableError::corrupt || result.error == vtd::TableError::quarantined) quarantine_unit(unit);
        return result;
    }
    return {.error = vtd::TableError::unavailable, .retain_data = removing};
}
auto map(const pci::PCIDevice* device, uint64_t physical, size_t bytes, vtd::Access access) -> vtd::TableResult {
    return mutate(device, physical, physical, bytes, access, false);
}
auto unmap(const pci::PCIDevice* device, uint64_t address, uint64_t physical, size_t bytes, vtd::Access access) -> bool {
    return mutate(device, address, physical, bytes, access, true).error == vtd::TableError::none;
}
#ifdef WOS_SELFTEST
auto selftest_withhold_invalidation(const pci::PCIDevice* device) -> bool {
    auto const DOMAIN = domain_info(device);
    if (DOMAIN.mode != DomainMode::translated) return false;
    UnitGuard guard(units[DOMAIN.unit]);
    if (!guard.admitted || units[DOMAIN.unit].failed.load(std::memory_order_acquire)) return false;
    units[DOMAIN.unit].withhold_invalidation = true;
    return true;
}
auto selftest_limit_table_allocations(size_t successful_allocations) -> bool {
    if (ready.load(std::memory_order_acquire) || stats.table_pages != 0) return false;
    allocation_budget = successful_allocations;
    return true;
}
#endif
}  // namespace ker::dev::iommu
