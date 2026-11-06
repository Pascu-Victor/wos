// Included only by pci.cpp, inside its private namespace. All mutable geometry
// and write intent use the existing config_lock; no MM/DMA lock nests inside it.
using ColdStatus = mod::mm::mmio::Status;
enum class ColdStage : uint8_t { legacy, preparing, activated, published, retired, failed };
struct ColdRow {
    aperture::Image image{};
    ecam::Capabilities capabilities{};
    std::array<aperture::Span, 9> possible{};
    size_t count{};
    size_t forwarding_start = SIZE_MAX;
};
struct ColdRecord {
    std::atomic<bool> dma_open{true};
    std::atomic<bool> selected{false};
    ColdStage stage = ColdStage::legacy;
    bool invoking = false;
    bool probed = false;
    uint64_t id{}, epoch{};
    uint64_t bar0_physical{}, bar0_bytes{};
    size_t span_count{};
    std::array<detail::Span, 6> spans{};
    std::array<detail::MemoryBar, 6> bars{};
    size_t view_count{};
    std::array<detail::PreparedView, mod::mm::mmio::VIEW_CAPACITY> views{};
    mod::mm::mmio::detail::PreparedMapping prepared{};
};
constinit std::array<ColdRecord, inventory_limits::FUNCTION_CAPACITY> cold_records{};
constinit std::array<ColdRow, inventory_limits::FUNCTION_CAPACITY> cold_rows{};
constinit aperture::Platform cold_platform{};
constinit aperture::Image cold_platform_host{}, cold_platform_lpc{};
std::atomic<bool> cold_sealed{false};
bool cold_attempted = false;
ColdStatus cold_inventory_status = ColdStatus::not_ready;
uint64_t cold_inventory_generation = 0, cold_owner_id = 0;
enum class ColdIntent : uint8_t { none, probe, activation, retirement };
ColdIntent cold_intent = ColdIntent::none;
size_t cold_intent_index = SIZE_MAX;
struct ColdWriteScope {
    ColdWriteScope(size_t index, ColdIntent intent) {
        if (cold_intent != ColdIntent::none) __builtin_trap();
        cold_intent_index = index;
        cold_intent = intent;
    }
    ~ColdWriteScope() {
        cold_intent = ColdIntent::none;
        cold_intent_index = SIZE_MAX;
    }
    ColdWriteScope(const ColdWriteScope&) = delete;
    auto operator=(const ColdWriteScope&) -> ColdWriteScope& = delete;
};
auto cold_dma_allowed(size_t index) -> bool { return cold_records[index].dma_open.load(std::memory_order_acquire); }
auto cold_selected(size_t index) -> bool { return cold_records[index].selected.load(std::memory_order_acquire); }
void cold_close(size_t index) {
    if (cold_selected(index)) cold_records[index].dma_open.store(false, std::memory_order_release);
}
void cold_note_command_unlocked(const PCIDevice& device, uint8_t offset, size_t width, uint32_t value) {
    if (!mod::mm::mmio::enabled()) return;
    size_t const AT = aperture::effective_offset(offset, width);
    if (!aperture::touches(AT, width, PCI_COMMAND, 2)) return;
    auto const INDEX = device_index(&device);
    if (!INDEX) return;
    uint16_t const PRIOR = config_read16_unlocked(device, PCI_COMMAND);
    if ((aperture::project16(PRIOR, PCI_COMMAND, AT, width, value) & PCI_COMMAND_BUS_MASTER) != 0)
        resources[*INDEX].dma_produced.store(true, std::memory_order_release);
}

auto cold_write_allowed_unlocked(const PCIDevice& device, uint8_t offset, size_t width, uint32_t value) -> bool {
    if (!cold_sealed.load(std::memory_order_acquire)) return true;
    auto const INDEX = device_index(&device);
    if (!INDEX) return false;
    size_t const AT = aperture::effective_offset(offset, width);
    auto const& row = cold_rows[*INDEX];
    auto const& record = cold_records[*INDEX];
    bool const PRIVATE = cold_intent_index == *INDEX;
    if (aperture::geometry_write(device.header_type, AT, width)) {
        // Only the actual synchronous sizing transaction may use these BAR
        // strobes. Decoding stays off until exact restoration/readback.
        if (!PRIVATE || cold_intent != ColdIntent::probe) return false;
        uint16_t const COMMAND = config_read16_unlocked(device, PCI_COMMAND);
        if ((COMMAND & (PCI_COMMAND_MEM_SPACE | PCI_COMMAND_BUS_MASTER)) != 0 ||
            (record.selected.load(std::memory_order_relaxed) && (COMMAND & 1U) != 0))
            return false;
        size_t const BAR_END = (device.header_type & 0x7fU) == 0 ? 0x28 : 0x18;
        if (width == 4 && AT >= 0x10 && AT < BAR_END) return value == UINT32_MAX || value == row.image.read(AT, 4);
        return width == 4 && AT == 0x30 && (device.header_type & 0x7fU) == 0 && value == (row.image.read(0x30, 4) & ~1U);
    }
    if (aperture::touches(AT, width, PCI_COMMAND, 2)) {
        if (!record.selected.load(std::memory_order_relaxed)) return true;
        uint16_t const PRIOR = config_read16_unlocked(device, PCI_COMMAND);
        uint16_t const NEXT = aperture::project16(PRIOR, PCI_COMMAND, AT, width, value);
        if ((NEXT & 1U) != 0) return false;  // Selected function has no I/O consumer.
        bool const ACTIVATING = PRIVATE && cold_intent == ColdIntent::activation && record.stage == ColdStage::preparing;
        if ((NEXT & PCI_COMMAND_MEM_SPACE) != 0 && record.stage != ColdStage::published && !ACTIVATING) return false;
        if ((NEXT & PCI_COMMAND_BUS_MASTER) != 0 && !record.dma_open.load(std::memory_order_acquire) &&
            !cold_closed_cleanup_unlocked(*INDEX, NEXT))
            return false;
        return true;
    }
    if (AT < 0x40) {
        // Status W1C, cache-line/latency and interrupt line are safe. Header,
        // capability pointer, geometry and all reset strobes remain immutable.
        return (AT >= 4 && AT + width <= 8) || (AT >= 0x0c && AT + width <= 0x0e) || (AT >= 0x3c && AT + width <= 0x3e);
    }
    // q35 programmable chipset geometry is frozen as a whole, including
    // disabled windows, PAM/SMRAM/remap and LPC ROM/firmware decode controls.
    if ((device.vendor_id == 0x8086 && device.device_id == 0x29c0) || (device.vendor_id == 0x8086 && device.device_id == 0x2918))
        return false;
    if (record.selected.load(std::memory_order_relaxed) && record.stage != ColdStage::published) return false;
    auto const& cap = row.capabilities;
    if (cap.msi != 0) {
        uint16_t const CONTROL = static_cast<uint16_t>(row.image.read(cap.msi + 2, 2));
        size_t const DATA = cap.msi + ((CONTROL & 0x80U) != 0 ? 12 : 8);
        size_t const END = DATA + ((CONTROL & 0x100U) != 0 ? 12 : 2);
        if (AT >= cap.msi + 4 && AT + width <= END) return true;
        if (AT >= cap.msi + 2 && AT + width <= cap.msi + 4) {
            uint16_t const OLD = config_read16_unlocked(device, static_cast<uint8_t>(cap.msi + 2));
            return ((OLD ^ aperture::project16(OLD, cap.msi + 2, AT, width, value)) & ~uint16_t{0x71}) == 0;
        }
    }
    if (cap.msix != 0 && AT >= cap.msix + 2 && AT + width <= cap.msix + 4) {
        uint16_t const OLD = config_read16_unlocked(device, static_cast<uint8_t>(cap.msix + 2));
        return ((OLD ^ aperture::project16(OLD, cap.msix + 2, AT, width, value)) & ~uint16_t{0xc000}) == 0;
    }
    if (cap.pcie != 0 && AT >= cap.pcie + 8 && AT + width <= cap.pcie + 10) {
        uint16_t const OLD = config_read16_unlocked(device, static_cast<uint8_t>(cap.pcie + 8));
        uint16_t const NEXT = aperture::project16(OLD, cap.pcie + 8, AT, width, value);
        return aperture::safe_device_control(OLD, NEXT);
    }
    return false;  // PM, FLR, slot/link/reset and unmodeled controls never reopen.
}

auto cold_geometry_matches_unlocked(size_t index) -> bool {
    auto const& device = discovered_devices[index];
    auto const& image = cold_rows[index].image;
    if (config_read32_unlocked(device, 0) != image.read(0, 4) ||
        (config_read32_unlocked(device, 0x0c) & 0x00ff0000U) != (image.read(0x0c, 4) & 0x00ff0000U))
        return false;
    size_t const COUNT = (device.header_type & 0x7fU) == 0 ? 6 : 2;
    for (size_t bar = 0; bar < COUNT; ++bar)
        if (config_read32_unlocked(device, static_cast<uint8_t>(0x10 + bar * 4)) != image.words[4 + bar]) return false;
    uint8_t const ROM = (device.header_type & 0x7fU) == 0 ? 0x30 : 0x38;
    uint32_t const EXPECTED_ROM = cold_selected(index) ? image.read(ROM, 4) & ~1U : image.read(ROM, 4);
    return config_read32_unlocked(device, ROM) == EXPECTED_ROM;
}

// A closed typed owner may still mask INTx before the actual hardware master
// drain. This preserves only an already-live BME and changes no other command
// bits; no DMA allocation/submission admission is reopened. Both the diagnostic
// precheck and the final writer call this under the actual config lock.
auto cold_closed_cleanup_unlocked(size_t index, uint16_t next) -> bool {
    auto const& record = cold_records[index];
    auto const& slot = driver_slots[index];
    auto const PHASE = slot.phase.load(std::memory_order_acquire);
    if (!record.selected.load(std::memory_order_acquire) || record.stage != ColdStage::published ||
        record.dma_open.load(std::memory_order_acquire) || record.epoch != slot.epoch || slot.driver != DriverKind::e1000e ||
        (PHASE != DriverPhase::closed && PHASE != DriverPhase::retained) ||
        (slot.operations.load(std::memory_order_acquire) & DRIVER_CLOSED) == 0 || !resource_dma_health_allowed(index) ||
        !iommu::bus_master_allowed(&discovered_devices[index]) || !cold_geometry_matches_unlocked(index))
        return false;
    uint16_t const PRIOR = config_read16_unlocked(discovered_devices[index], PCI_COMMAND);
    return (PRIOR & PCI_COMMAND_BUS_MASTER) != 0 && next == static_cast<uint16_t>(PRIOR | PCI_COMMAND_INT_DISABLE);
}

auto cold_irq_disabled_unlocked(size_t index) -> bool {
    auto const& device = discovered_devices[index];
    auto const& caps = cold_rows[index].capabilities;
    return (caps.msi == 0 || (config_read16_unlocked(device, static_cast<uint8_t>(caps.msi + 2)) & 1U) == 0) &&
           (caps.msix == 0 || (config_read16_unlocked(device, static_cast<uint8_t>(caps.msix + 2)) & 0x8000U) == 0);
}

auto cold_is_ancestor(size_t ancestor, size_t child) -> bool {
    for (size_t count = 0; count < discovered_devices.size(); ++count) {
        uint16_t const PARENT = discovered_devices[child].parent_index;
        if (PARENT == NO_PARENT) return false;
        if (PARENT >= child) return false;
        if (PARENT == ancestor) return true;
        child = PARENT;
    }
    return false;
}
auto cold_excluded(size_t index, aperture::Span span) -> bool {
    if (cold_platform.excludes(span)) return true;
    size_t child = index;
    for (size_t depth = 0; depth < discovered_devices.size(); ++depth) {
        uint16_t const PARENT = discovered_devices[child].parent_index;
        if (PARENT == NO_PARENT) break;
        if (PARENT >= child) return true;
        auto const& parent = cold_rows[PARENT];
        bool routed = false;
        for (size_t i = parent.forwarding_start; i < parent.count; ++i)
            if (aperture::contains(parent.possible[i], span)) routed = true;
        if (!routed || (parent.image.read(PCI_COMMAND, 2) & PCI_COMMAND_MEM_SPACE) == 0) return true;
        child = PARENT;
    }
    for (size_t other = 0; other < static_cast<size_t>(device_count); ++other) {
        if (other == index) continue;
        auto const& row = cold_rows[other];
        for (size_t range = 0; range < row.count; ++range) {
            // Only complete containing bridge forwarding windows of the exact
            // ancestry are routing, not competing endpoint decoders. BAR/ROM
            // entries precede the final two windows and are never exempted.
            bool const FORWARDING = range >= row.forwarding_start && cold_is_ancestor(other, index);
            if (FORWARDING && aperture::contains(row.possible[range], span)) continue;
            if (aperture::overlaps(row.possible[range], span)) return true;
        }
    }
    return false;
}

auto cold_add_possible(ColdRow& row, aperture::Span span) -> bool {
    if (row.count == row.possible.size()) return false;
    row.possible[row.count++] = span;
    return true;
}
auto cold_collect_row_unlocked(size_t index) -> bool {
    auto const& device = discovered_devices[index];
    auto& row = cold_rows[index];
    for (size_t word = 0; word < row.image.words.size(); ++word)
        row.image.words[word] = config_read32_unlocked(device, static_cast<uint8_t>(word * 4));
    if (row.image.read(0, 4) != (uint32_t{device.device_id} << 16 | device.vendor_id) || row.image.read(0x0e, 1) != device.header_type ||
        (device.header_type & 0x7fU) > 1)
        return false;
    if (!aperture::conventional_layout(row.image) || (row.image.read(0x0f, 1) & 0x40U) != 0 ||
        ((device.header_type & 0x7fU) == 1 && (row.image.read(0x3e, 2) & 0x40U) != 0))
        return false;
    auto const* extended = extended_config(&device);
    if (extended == nullptr || extended->state != ExtendedState::ready || !extended->capabilities.valid() ||
        !extended->capabilities.cold_profile)
        return false;
    row.capabilities = extended->capabilities;
    auto const& cap = row.capabilities;
    if ((cap.pm != 0 && cap.pm > 0xf8) || (cap.msix != 0 && cap.msix > 0xf4)) return false;
    if (cap.msi != 0) {
        uint16_t const CONTROL = static_cast<uint16_t>(row.image.read(cap.msi + 2, 2));
        size_t const END = cap.msi + ((CONTROL & 0x80U) != 0 ? 12 : 8) + ((CONTROL & 0x100U) != 0 ? 12 : 2);
        if (END > 256) return false;
    }
    size_t const BARS = (device.header_type & 0x7fU) == 0 ? 6 : 2;
    for (size_t bar = 0; bar < BARS; ++bar) {
        if (row.image.read(0x10 + bar * 4, 4) != device.bar[bar]) return false;
        auto const BAR = resource::decode_bar(device.bar, device.header_type, bar);
        if (!BAR.valid()) return false;
        if (BAR.memory()) {
            auto const RANGE = aperture::possible_bar(BAR, cold_platform.physical_bits);
            if (!RANGE || !cold_add_possible(row, *RANGE)) return false;
            if (BAR.kind == resource::BarKind::memory64) ++bar;
        }
    }
    auto const ROM = aperture::possible_rom(row.image.read((device.header_type & 0x7fU) == 0 ? 0x30 : 0x38, 4));
    if (!ROM || !cold_add_possible(row, *ROM)) return false;
    if ((device.header_type & 0x7fU) == 1) {
        auto const WINDOWS = aperture::bridge_windows(row.image, cold_platform.physical_bits);
        if (!WINDOWS.valid) return false;
        row.forwarding_start = row.count;
        if (WINDOWS.memory.bytes != 0 && !cold_add_possible(row, WINDOWS.memory)) return false;
        if (WINDOWS.prefetch.bytes != 0 && !cold_add_possible(row, WINDOWS.prefetch)) return false;
    }
    return true;
}
