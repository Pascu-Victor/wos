#include "pci.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <dev/iommu/domains.hpp>
#include <dev/pci_bar_probe.hpp>
#include <dev/pci_boot_resource.hpp>
#include <dev/pci_driver_claim.hpp>
#include <dev/pci_extended.hpp>
#include <dev/pci_msix.hpp>
#include <dev/pci_port_io.hpp>
#include <dev/pci_resource.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/virt.hpp>
#include <platform/smt/smt.hpp>
#include <platform/sys/spinlock.hpp>
#include <utility>

namespace ker::dev::pci {

using log = ker::mod::dbg::logger<"pci">;

namespace {
std::array<PCIDevice, inventory_limits::FUNCTION_CAPACITY> discovered_devices = {};
struct PreparedMsix {
    bool present = false;
    msix::Layout layout{};
    volatile uint32_t* table = nullptr;
};
struct DeviceResources {
    std::array<resource::BootResource<uint64_t>, BAR_COUNT> sizes{};
    std::array<resource::BootResource<void*>, BAR_COUNT> mappings{};
    resource::BootResource<PreparedMsix> msix{};
    std::atomic<bool> poisoned{false};
    bool activated = false;  // Protected by config_lock; one boot owner only.
};
constinit std::array<DeviceResources, inventory_limits::FUNCTION_CAPACITY> resources{};
int device_count = 0;
std::atomic<size_t> published_device_count{0};
constexpr uint32_t DRIVER_CLOSED = uint32_t{1} << 31;
constexpr uint32_t DRIVER_OPERATIONS = DRIVER_CLOSED - 1;
struct DriverSlot {
    std::atomic<DriverPhase> phase{DriverPhase::unclaimed};
    std::atomic<uint32_t> operations{0};
    DriverKind driver = DriverKind::diagnostic;
    uint64_t epoch = 0;
};
static_assert(std::atomic<DriverPhase>::is_always_lock_free && std::atomic<uint32_t>::is_always_lock_free &&
              std::atomic<size_t>::is_always_lock_free);
constinit std::array<DriverSlot, inventory_limits::FUNCTION_CAPACITY> driver_slots{};
uint64_t driver_epoch = 0;  // config_lock; never wraps or reuses a claim.
bool enumerated = false;
bool enumeration_error = false;
InventoryError first_enumeration_error = InventoryError::none;
uint32_t enumeration_reasons = 0;
std::array<bool, inventory_limits::BUS_CAPACITY> visited_buses{};
struct BusCursor {
    uint16_t parent;
    uint8_t bus;
    uint8_t slot;
    uint8_t next_function;
};
static_assert(sizeof(BusCursor) <= 8);
// BSP-only traversal storage, never exposed to runtime consumers. Deep bridge
// chains retain cursors here instead of growing the Limine boot call stack.
constinit std::array<BusCursor, inventory_limits::BUS_CAPACITY> scan_stack{};
mod::sys::Spinlock config_lock{};
struct ConfigGuard {
    uint64_t flags = config_lock.lock_irqsave();
    bool held = true;
    void release() {
        if (held) {
            held = false;
            config_lock.unlock_irqrestore(flags);
        }
    }
    ~ConfigGuard() { release(); }
    ConfigGuard() = default;
    ConfigGuard(const ConfigGuard&) = delete;
    auto operator=(const ConfigGuard&) -> ConfigGuard& = delete;
};
constexpr int PCI_FUNCTION_COUNT = 8;
constexpr int PCI_SLOT_COUNT = 32;

using port_io::inl;
using port_io::outb;
using port_io::outl;
using port_io::outw;

auto config_address(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset) -> uint32_t {
    return (static_cast<uint32_t>(bus) << 16) | (static_cast<uint32_t>(slot) << 11) | (static_cast<uint32_t>(func) << 8) |
           (offset & 0xfcU) | 0x80000000U;
}

// All callers own config_lock. Never call the public locking wrappers from a
// complete probe/MSI-X transaction.
auto config_read32_unlocked(const PCIDevice& device, uint8_t offset) -> uint32_t {
    outl(PCI_CONFIG_ADDRESS, config_address(device.bus, device.slot, device.function, offset));
    return inl(PCI_CONFIG_DATA);
}
auto config_read16_unlocked(const PCIDevice& device, uint8_t offset) -> uint16_t {
    return static_cast<uint16_t>(config_read32_unlocked(device, offset) >> ((offset & 2U) * 8U));
}
void config_write32_unlocked(const PCIDevice& device, uint8_t offset, uint32_t value) {
    outl(PCI_CONFIG_ADDRESS, config_address(device.bus, device.slot, device.function, offset));
    outl(PCI_CONFIG_DATA, value);
}
void config_write16_unlocked(const PCIDevice& device, uint8_t offset, uint16_t value) {
    outl(PCI_CONFIG_ADDRESS, config_address(device.bus, device.slot, device.function, offset));
    outw(static_cast<uint16_t>(PCI_CONFIG_DATA + (offset & 2U)), value);
}
auto device_index(const PCIDevice* device) -> std::optional<size_t> {
    // Integer membership avoids both dereferencing a foreign pointer and
    // subtracting/comparing unrelated C++ pointers. Interior pointers refuse.
    auto const ADDRESS = reinterpret_cast<uintptr_t>(device);
    auto const BASE = reinterpret_cast<uintptr_t>(discovered_devices.data());
    if (ADDRESS < BASE) return std::nullopt;
    uintptr_t const OFFSET = ADDRESS - BASE;
    if (OFFSET % sizeof(PCIDevice) != 0) return std::nullopt;
    size_t const INDEX = OFFSET / sizeof(PCIDevice);
    return INDEX < static_cast<size_t>(device_count) ? std::optional<size_t>{INDEX} : std::nullopt;
}
auto published_device_index(const PCIDevice* device) -> std::optional<size_t> {
    auto const ADDRESS = reinterpret_cast<uintptr_t>(device);
    auto const BASE = reinterpret_cast<uintptr_t>(discovered_devices.data());
    if (ADDRESS < BASE) return std::nullopt;
    uintptr_t const OFFSET = ADDRESS - BASE;
    if (OFFSET % sizeof(PCIDevice) != 0) return std::nullopt;
    size_t const INDEX = OFFSET / sizeof(PCIDevice);
    return INDEX < published_device_count.load(std::memory_order_acquire) ? std::optional<size_t>{INDEX} : std::nullopt;
}
auto driver_activation_allowed(size_t index) -> bool {
    auto const& slot = driver_slots[index];
    return slot.phase.load(std::memory_order_acquire) == DriverPhase::unclaimed ||
           (slot.operations.load(std::memory_order_acquire) & DRIVER_CLOSED) == 0;
}
auto reserve_driver_unlocked(size_t index, DriverKind kind) -> bool {
    auto& slot = driver_slots[index];
    if (!enumerated || enumeration_error || slot.phase.load(std::memory_order_acquire) != DriverPhase::unclaimed ||
        driver_epoch == UINT64_MAX)
        return false;
    // A raw boot producer must finish before a new owner can adopt the slot.
    // Claimed producers instead hold an operation until publication completes.
    auto const& record = resources[index];
    if (record.msix.state() == resource::BootState::preparing) return false;
    for (size_t i = 0; i < BAR_COUNT; ++i)
        if (record.sizes[i].state() == resource::BootState::preparing || record.mappings[i].state() == resource::BootState::preparing)
            return false;
    slot.driver = kind;
    slot.epoch = ++driver_epoch;
    slot.phase.store(DriverPhase::open, std::memory_order_release);
    return true;
}
void close_driver_unlocked(size_t index, uint64_t epoch, bool retained) {
    auto& slot = driver_slots[index];
    auto const PHASE = slot.phase.load(std::memory_order_acquire);
    if (PHASE == DriverPhase::unclaimed || slot.epoch != epoch) return;
    slot.operations.fetch_or(DRIVER_CLOSED, std::memory_order_acq_rel);
    if (PHASE != DriverPhase::retained) slot.phase.store(retained ? DriverPhase::retained : DriverPhase::closed, std::memory_order_release);
}
auto resource_dma_allowed(size_t index) -> bool {
    auto const& record = resources[index];
    auto allowed = [](resource::BootState state) {
        return state != resource::BootState::preparing && state != resource::BootState::failed;
    };
    if (record.poisoned.load(std::memory_order_acquire) || !allowed(record.msix.state())) return false;
    for (size_t i = 0; i < BAR_COUNT; ++i)
        if (!allowed(record.sizes[i].state()) || !allowed(record.mappings[i].state())) return false;
    return true;
}
auto resources_sealed(size_t index) -> bool {
    auto const& record = resources[index];
    if (record.msix.state() != resource::BootState::empty) return true;
    for (size_t i = 0; i < BAR_COUNT; ++i)
        if (record.sizes[i].state() != resource::BootState::empty || record.mappings[i].state() != resource::BootState::empty) return true;
    return false;
}
auto bar_write_allowed(uint8_t bus, uint8_t slot, uint8_t function, uint8_t offset, size_t width) -> bool {
    for (int i = 0; i < device_count; ++i) {
        auto const& device = discovered_devices[static_cast<size_t>(i)];
        if (device.bus == bus && device.slot == slot && device.function == function &&
            resource::bar_register_overlap(device.header_type, offset, width) && resources_sealed(static_cast<size_t>(i)))
            return false;
    }
    return true;
}
void authorize_bar_write(uint8_t bus, uint8_t slot, uint8_t function, uint8_t offset, size_t width) {
    if (bar_write_allowed(bus, slot, function, offset, width)) return;
    mod::dbg::panic_handler("Raw PCI write attempted relocation of retained BAR resources");
    hcf();
}

// Pure admission lookup is safe under config_lock. Quarantine publishes its
// closed flag before disabling each requester under this same lock.
auto bus_master_write_allowed(uint8_t bus, uint8_t slot, uint8_t func, uint32_t value) -> bool {
    if ((value & PCI_COMMAND_BUS_MASTER) == 0) return true;
    for (int i = 0; i < device_count; ++i) {
        auto const& device = discovered_devices[static_cast<size_t>(i)];
        if (device.bus == bus && device.slot == slot && device.function == func && resource_dma_allowed(static_cast<size_t>(i)) &&
            iommu::bus_master_allowed(&device))
            return true;
    }
    return false;
}
// config_lock is held. Cleanup command RMW may preserve an existing BME bit,
// but a closed claim must never make a fresh 0->1 transition. Old poison and
// domain admission remain mandatory even for a preserving write.
auto bus_master_transition_allowed_unlocked(uint8_t bus, uint8_t slot, uint8_t func, uint32_t value) -> bool {
    if (!bus_master_write_allowed(bus, slot, func, value)) return false;
    if ((value & PCI_COMMAND_BUS_MASTER) == 0) return true;
    for (int i = 0; i < device_count; ++i) {
        auto const& device = discovered_devices[static_cast<size_t>(i)];
        if (device.bus == bus && device.slot == slot && device.function == func)
            return driver_activation_allowed(static_cast<size_t>(i)) ||
                   (config_read16_unlocked(device, PCI_COMMAND) & PCI_COMMAND_BUS_MASTER) != 0;
    }
    return false;
}
// Do not undo a terminal INTx mask through a stale command RMW. Byte5 is
// independently writable, so this check uses the effective port width too.
auto preserve_closed_intx_mask_unlocked(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset, size_t width, uint32_t value) -> uint32_t {
    if (offset > PCI_COMMAND + 1U || offset + width <= PCI_COMMAND + 1U) return value;
    for (int i = 0; i < device_count; ++i) {
        auto const& device = discovered_devices[static_cast<size_t>(i)];
        if (device.bus != bus || device.slot != slot || device.function != func) continue;
        if (!driver_activation_allowed(static_cast<size_t>(i)) &&
            (config_read16_unlocked(device, PCI_COMMAND) & PCI_COMMAND_INT_DISABLE) != 0)
            value |= uint32_t{1} << ((PCI_COMMAND + 1U - offset) * 8U + 2U);
        break;
    }
    return value;
}
// Only closed claims need this bounded capability walk. Effective offsets
// match the byte/word/dword port access, including unaligned public arguments.
// Ambiguous malformed/looping chains cannot authorize a capability write.
auto closed_irq_control_write_allowed_unlocked(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset, size_t width, uint32_t value)
    -> bool {
    if (offset < 0x40) return true;
    const PCIDevice* device = nullptr;
    for (int i = 0; i < device_count; ++i) {
        auto const& candidate = discovered_devices[static_cast<size_t>(i)];
        if (candidate.bus == bus && candidate.slot == slot && candidate.function == func) {
            if (driver_activation_allowed(static_cast<size_t>(i))) return true;
            device = &candidate;
            break;
        }
    }
    if (device == nullptr || (config_read16_unlocked(*device, PCI_STATUS) & (1U << 4)) == 0) return true;
    auto next = static_cast<uint8_t>(config_read32_unlocked(*device, PCI_CAP_PTR));
    uint64_t visited = 0;
    for (int limit = 48; next != 0 && limit-- > 0;) {
        if (next < 0x40 || (next & 3U) != 0 || (visited & (uint64_t{1} << (next / 4U))) != 0) return false;
        visited |= uint64_t{1} << (next / 4U);
        auto const HEADER = config_read32_unlocked(*device, next);
        auto const KIND = static_cast<uint8_t>(HEADER);
        auto const CONTROL_OFFSET = static_cast<uint16_t>(next) + 2U;
        auto const OLD = static_cast<uint16_t>(HEADER >> 16U);
        if ((KIND == PCI_CAP_ID_MSI || KIND == PCI_CAP_ID_MSIX) && offset < CONTROL_OFFSET + 2U && offset + width > CONTROL_OFFSET) {
            uint16_t updated = OLD;
            for (size_t byte = 0; byte < width; ++byte) {
                size_t const AT = offset + byte;
                if (AT < CONTROL_OFFSET || AT >= CONTROL_OFFSET + 2U) continue;
                auto const SHIFT = static_cast<unsigned>((AT - CONTROL_OFFSET) * 8U);
                updated = static_cast<uint16_t>((updated & ~(uint16_t{255} << SHIFT)) | (((value >> (byte * 8U)) & 255U) << SHIFT));
            }
            if (KIND == PCI_CAP_ID_MSI && (OLD & 1U) == 0 && (updated & 1U) != 0) return false;
            if (KIND == PCI_CAP_ID_MSI && ((OLD ^ updated) & 0x70U) != 0) return false;
            if (KIND == PCI_CAP_ID_MSIX &&
                (((OLD & 0x8000U) == 0 && (updated & 0x8000U) != 0) || ((OLD & 0x4000U) != 0 && (updated & 0x4000U) == 0)))
                return false;
        }
        if (KIND == PCI_CAP_ID_MSI) {
            bool const WIDE = (OLD & 0x80U) != 0;
            bool const MASKING = (OLD & 0x100U) != 0;
            uint16_t const DATA = static_cast<uint16_t>(next + (WIDE ? 12U : 8U));
            uint16_t const MASK = static_cast<uint16_t>(next + (WIDE ? 16U : 12U));
            if (DATA + 2U > 256U || (MASKING && MASK + 8U > 256U)) return false;
            for (size_t byte = 0; byte < width; ++byte) {
                size_t const AT = offset + byte;
                bool const DESTINATION = (AT >= next + 4U && AT < DATA) || (AT >= DATA && AT < DATA + 2U);
                bool const MASK_BYTE = MASKING && AT >= MASK && AT < MASK + 4U;
                if (!DESTINATION && !MASK_BYTE) continue;
                auto const PRIOR = static_cast<uint8_t>(config_read32_unlocked(*device, static_cast<uint8_t>(AT)) >> ((AT & 3U) * 8U));
                auto const UPDATED = static_cast<uint8_t>(value >> (byte * 8U));
                if ((DESTINATION && UPDATED != PRIOR) || (MASK_BYTE && (PRIOR & ~UPDATED) != 0)) return false;
            }
        }
        next = static_cast<uint8_t>(HEADER >> 8U);
    }
    return next == 0;
}
void authorize_bus_master_write(uint8_t bus, uint8_t slot, uint8_t func, uint32_t value) {
    if (bus_master_write_allowed(bus, slot, func, value)) return;
    mod::dbg::panic_handler("Raw PCI command write attempted bus mastering without a ready DMA domain");
    hcf();
}

struct ConfigReader {
    PCIDevice* device;
    auto read32(uint8_t offset) -> uint32_t { return config_read32_unlocked(*device, offset); }
    auto read16(uint8_t offset) -> uint16_t { return config_read16_unlocked(*device, offset); }
    void write32(uint8_t offset, uint32_t value) { config_write32_unlocked(*device, offset, value); }
    void write16(uint8_t offset, uint16_t value) {
        if (offset == PCI_COMMAND && !bus_master_transition_allowed_unlocked(device->bus, device->slot, device->function, value))
            value = static_cast<uint16_t>(value & ~PCI_COMMAND_BUS_MASTER);
        config_write16_unlocked(*device, offset, value);
    }
};
void poison_unlocked(size_t index, uint8_t capability = 0) {
    resources[index].poisoned.store(true, std::memory_order_release);
    auto const& device = discovered_devices[index];
    if (capability == 0) {
        auto const* prepared = resources[index].msix.get();
        if (prepared && prepared->present) capability = prepared->layout.capability;
    }
    if (capability != 0) {
        auto const OFFSET = static_cast<uint8_t>(capability + 2);
        config_write16_unlocked(device, OFFSET, static_cast<uint16_t>(config_read16_unlocked(device, OFFSET) | 0x4000U));
        (void)config_read16_unlocked(device, OFFSET);
    }
    config_write16_unlocked(device, PCI_COMMAND,
                            static_cast<uint16_t>(config_read16_unlocked(device, PCI_COMMAND) & ~PCI_COMMAND_BUS_MASTER));
    (void)config_read16_unlocked(device, PCI_COMMAND);
}
auto destination(uint64_t cpu, uint8_t vector) -> std::optional<uint32_t> {
    if (cpu >= mod::smt::get_core_count() || vector < 32 || vector == 255) return std::nullopt;
    uint32_t const APIC = mod::smt::get_apic_id_for_cpu(cpu);
    return APIC <= 255 ? std::optional<uint32_t>{APIC} : std::nullopt;
}
struct TableReader {
    size_t index;
    const PreparedMsix& prepared;
    auto read32(uint32_t word) -> uint32_t { return prepared.table[word]; }
    void write32(uint32_t word, uint32_t value) { prepared.table[word] = value; }
    void inhibit() { poison_unlocked(index, prepared.layout.capability); }
};

void record_enumeration_error(InventoryError error) {
    enumeration_error = true;
    enumeration_reasons |= static_cast<uint32_t>(error);
    if (first_enumeration_error == InventoryError::none) first_enumeration_error = error;
}

struct DownstreamBus {
    uint8_t bus;
    uint16_t parent;
};

auto scan_function(uint8_t bus, uint8_t slot, uint8_t func, uint16_t parent) -> std::optional<DownstreamBus> {
    uint16_t const VENDOR = pci_config_read16(bus, slot, func, PCI_VENDOR_ID);
    if (VENDOR == 0xFFFF) {
        return std::nullopt;
    }
    if (std::cmp_greater_equal(device_count, discovered_devices.size())) {
        record_enumeration_error(InventoryError::function_capacity);
        return std::nullopt;
    }

    auto* dev = &discovered_devices.at(static_cast<size_t>(device_count));
    dev->bus = bus;
    dev->slot = slot;
    dev->function = func;
    dev->parent_index = parent;
    dev->vendor_id = VENDOR;
    dev->device_id = pci_config_read16(bus, slot, func, PCI_DEVICE_ID);

    // Read class/subclass/prog_if from the 32-bit register at offset 0x08
    uint32_t const CLASS_DWORD = pci_config_read32(bus, slot, func, PCI_CLASS_CODE);
    dev->class_code = (CLASS_DWORD >> 24) & 0xFF;
    dev->subclass_code = (CLASS_DWORD >> 16) & 0xFF;
    dev->prog_if = (CLASS_DWORD >> 8) & 0xFF;

    dev->header_type = pci_config_read8(bus, slot, func, PCI_HEADER_TYPE);
    dev->interrupt_line = pci_config_read8(bus, slot, func, PCI_INTERRUPT_LINE);
    dev->interrupt_pin = pci_config_read8(bus, slot, func, PCI_INTERRUPT_PIN);

    // Bridges have two BARs; their remaining registers describe bus/windows.
    auto const HEADER = dev->header_type & 0x7fU;
    size_t const BARS = HEADER == PCI_HEADER_TYPE_NORMAL ? BAR_COUNT : HEADER == PCI_HEADER_TYPE_BRIDGE ? 2 : 0;
    dev->bar.fill(0);
    if (BARS != 0) {
        for (size_t i = 0; i < BARS; i++) {
            dev->bar.at(i) = pci_config_read32(bus, slot, func, static_cast<uint8_t>(PCI_BAR0 + (i * sizeof(uint32_t))));
        }
    }

    device_count++;

    // Return the secondary bus only after this bridge's stable registry entry
    // is complete. The iterative caller descends before scanning its siblings.
    if ((dev->header_type & 0x7F) == PCI_HEADER_TYPE_BRIDGE) {
        uint32_t const BUSES = pci_config_read32(bus, slot, func, 0x18);
        auto const PRIMARY = static_cast<uint8_t>(BUSES);
        dev->secondary_bus = static_cast<uint8_t>(BUSES >> 8);
        dev->subordinate_bus = static_cast<uint8_t>(BUSES >> 16);
        if (dev->secondary_bus == 0 && dev->subordinate_bus == 0) {
            return std::nullopt;  // Unconfigured bridge: no downstream bus.
        }
        if (PRIMARY != bus || dev->secondary_bus == 0 || dev->secondary_bus == bus || dev->subordinate_bus < dev->secondary_bus ||
            (parent != NO_PARENT && (dev->secondary_bus < discovered_devices[parent].secondary_bus ||
                                     dev->subordinate_bus > discovered_devices[parent].subordinate_bus))) {
            record_enumeration_error(InventoryError::bridge_topology);
            return std::nullopt;
        }
        return DownstreamBus{dev->secondary_bus, static_cast<uint16_t>(device_count - 1)};
    }
    return std::nullopt;
}

void enter_bus(uint8_t bus, uint16_t parent, size_t& depth) {
    if (visited_buses[bus]) {
        record_enumeration_error(InventoryError::bus_revisit);
        return;
    }
    if (depth >= scan_stack.size()) {
        record_enumeration_error(InventoryError::traversal_capacity);
        return;
    }
    visited_buses[bus] = true;
    scan_stack[depth++] = BusCursor{parent, bus, 0, 0};
}

void scan_bus(uint8_t bus, uint16_t parent = NO_PARENT) {
    size_t depth = 0;
    enter_bus(bus, parent, depth);
    while (depth != 0) {
        auto& cursor = scan_stack[depth - 1];
        if (cursor.slot >= PCI_SLOT_COUNT) {
            --depth;
            continue;
        }
        std::optional<DownstreamBus> downstream;
        if (cursor.next_function == 0) {
            if (pci_config_read16(cursor.bus, cursor.slot, 0, PCI_VENDOR_ID) == 0xffff) {
                ++cursor.slot;  // An absent function0 suppresses all siblings.
                continue;
            }
            cursor.next_function = 1;
            downstream = scan_function(cursor.bus, cursor.slot, 0, cursor.parent);
        } else {
            if (cursor.next_function == 1) {
                // Match the old recursive scan_slot: inspect multifunction
                // only after function0's entire bridge subtree has returned.
                uint8_t const HEADER = pci_config_read8(cursor.bus, cursor.slot, 0, PCI_HEADER_TYPE);
                if ((HEADER & PCI_HEADER_TYPE_MULTI_FUNC) == 0) {
                    ++cursor.slot;
                    cursor.next_function = 0;
                    continue;
                }
            }
            if (cursor.next_function >= PCI_FUNCTION_COUNT) {
                ++cursor.slot;
                cursor.next_function = 0;
                continue;
            }
            uint8_t const FUNCTION = cursor.next_function++;
            if (pci_config_read16(cursor.bus, cursor.slot, FUNCTION, PCI_VENDOR_ID) != 0xffff)
                downstream = scan_function(cursor.bus, cursor.slot, FUNCTION, cursor.parent);
        }
        // The parent cursor already points past this function before descent.
        if (downstream) enter_bus(downstream->bus, downstream->parent, depth);
    }
}
}  // namespace

DriverClaim::~DriverClaim() { retain(); }
DriverClaim::DriverClaim(DriverClaim&& other) noexcept
    : device_(std::exchange(other.device_, nullptr)), epoch_(std::exchange(other.epoch_, 0)) {}
auto DriverClaim::operator=(DriverClaim&& other) noexcept -> DriverClaim& {
    if (this != &other) {
        retain();
        device_ = std::exchange(other.device_, nullptr);
        epoch_ = std::exchange(other.epoch_, 0);
    }
    return *this;
}
void DriverClaim::close_data_admission() {
    auto const INDEX = published_device_index(device_);
    if (!INDEX) return;
    ConfigGuard const GUARD;
    close_driver_unlocked(*INDEX, epoch_, false);
}
auto DriverClaim::operations_drained() const -> bool {
    auto const INDEX = published_device_index(device_);
    if (!INDEX) return true;
    auto const& slot = driver_slots[*INDEX];
    return slot.phase.load(std::memory_order_acquire) != DriverPhase::unclaimed && slot.epoch == epoch_ &&
           slot.operations.load(std::memory_order_acquire) == DRIVER_CLOSED;
}
void DriverClaim::retain() {
    auto const INDEX = published_device_index(device_);
    if (INDEX) {
        ConfigGuard const GUARD;
        close_driver_unlocked(*INDEX, epoch_, true);
    }
    device_ = nullptr;
    epoch_ = 0;
}

DriverOperation::~DriverOperation() { release(); }
DriverOperation::DriverOperation(DriverOperation&& other) noexcept : device_(std::exchange(other.device_, nullptr)) {}
auto DriverOperation::operator=(DriverOperation&& other) noexcept -> DriverOperation& {
    if (this != &other) {
        release();
        device_ = std::exchange(other.device_, nullptr);
    }
    return *this;
}
void DriverOperation::release() {
    auto const INDEX = published_device_index(device_);
    if (INDEX) driver_slots[*INDEX].operations.fetch_sub(1, std::memory_order_release);
    device_ = nullptr;
}

auto try_claim_driver(PCIDevice* device, DriverKind kind) -> std::optional<DriverClaim> {
    auto const INDEX = published_device_index(device);
    if (!INDEX || static_cast<uint8_t>(kind) > static_cast<uint8_t>(DriverKind::ivshmem)) return std::nullopt;
    ConfigGuard const GUARD;
    if (!reserve_driver_unlocked(*INDEX, kind)) return std::nullopt;
    return DriverClaim{device, driver_slots[*INDEX].epoch};
}
auto try_dma_operation(const PCIDevice* device) -> DriverOperation {
    auto const INDEX = published_device_index(device);
    if (!INDEX) return {};
    auto& slot = driver_slots[*INDEX];
    if (slot.phase.load(std::memory_order_acquire) == DriverPhase::unclaimed) return {};
    auto operations = slot.operations.load(std::memory_order_acquire);
    for (;;) {
        if ((operations & DRIVER_CLOSED) != 0 || operations == DRIVER_OPERATIONS) return {};
        if (slot.operations.compare_exchange_weak(operations, operations + 1, std::memory_order_acq_rel, std::memory_order_acquire))
            return DriverOperation{device};
    }
}
auto begin_dma_allocation(const PCIDevice* device) -> DriverOperation {
    auto const INDEX = published_device_index(device);
    if (!INDEX) return {};
    if (driver_slots[*INDEX].phase.load(std::memory_order_acquire) == DriverPhase::unclaimed) {
        ConfigGuard const GUARD;
        if (driver_slots[*INDEX].phase.load(std::memory_order_acquire) == DriverPhase::unclaimed &&
            !reserve_driver_unlocked(*INDEX, DriverKind::diagnostic))
            return {};
    }
    return try_dma_operation(device);
}
auto driver_dma_admission_allowed(const PCIDevice* device) -> bool {
    auto const INDEX = published_device_index(device);
    if (!INDEX) return false;
    auto const& slot = driver_slots[*INDEX];
    return slot.phase.load(std::memory_order_acquire) != DriverPhase::unclaimed &&
           (slot.operations.load(std::memory_order_acquire) & DRIVER_CLOSED) == 0;
}
auto driver_claim_status(const PCIDevice* device) -> std::optional<DriverStatus> {
    auto const INDEX = published_device_index(device);
    if (!INDEX) return std::nullopt;
    auto const& slot = driver_slots[*INDEX];
    auto const PHASE = slot.phase.load(std::memory_order_acquire);
    if (PHASE == DriverPhase::unclaimed) return DriverStatus{};
    auto const OPERATIONS = slot.operations.load(std::memory_order_acquire);
    return DriverStatus{PHASE == DriverPhase::open && (OPERATIONS & DRIVER_CLOSED) != 0 ? DriverPhase::closed : PHASE, slot.driver,
                        slot.epoch, OPERATIONS & DRIVER_OPERATIONS};
}

// Read 32-bit from PCI configuration space
auto pci_config_read32(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset) -> uint32_t {
    if (slot >= PCI_SLOT_COUNT || func >= PCI_FUNCTION_COUNT) return UINT32_MAX;
    ConfigGuard const GUARD;
    outl(PCI_CONFIG_ADDRESS, config_address(bus, slot, func, offset));
    return inl(PCI_CONFIG_DATA);
}

// Read 16-bit from PCI configuration space
auto pci_config_read16(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset) -> uint16_t {
    uint32_t const DWORD = pci_config_read32(bus, slot, func, offset);
    return (DWORD >> ((offset & 2) * 8)) & 0xFFFF;
}

// Read 8-bit from PCI configuration space
auto pci_config_read8(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset) -> uint8_t {
    uint32_t const DWORD = pci_config_read32(bus, slot, func, offset);
    return (DWORD >> ((offset & 3) * 8)) & 0xFF;
}

// Write 32-bit to PCI configuration space
auto pci_config_write32(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset, uint32_t value) -> void {
    if (slot >= PCI_SLOT_COUNT || func >= PCI_FUNCTION_COUNT) return;
    authorize_bar_write(bus, slot, func, offset, 4);
    if ((offset & 0xfcU) == PCI_COMMAND) authorize_bus_master_write(bus, slot, func, value);
    ConfigGuard const GUARD;
    if (!bar_write_allowed(bus, slot, func, offset, 4)) return;
    // A unit may close after the pre-lock diagnostic check. Do not restore BME.
    if ((offset & 0xfcU) == PCI_COMMAND && !bus_master_transition_allowed_unlocked(bus, slot, func, value))
        value = static_cast<uint32_t>(value & ~PCI_COMMAND_BUS_MASTER);
    value = preserve_closed_intx_mask_unlocked(bus, slot, func, offset & 0xfcU, 4, value);
    if (!closed_irq_control_write_allowed_unlocked(bus, slot, func, offset & 0xfcU, 4, value)) return;
    outl(PCI_CONFIG_ADDRESS, config_address(bus, slot, func, offset));
    outl(PCI_CONFIG_DATA, value);
}

// Write 16-bit to PCI configuration space
auto pci_config_write16(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset, uint16_t value) -> void {
    if (slot >= PCI_SLOT_COUNT || func >= PCI_FUNCTION_COUNT) return;
    authorize_bar_write(bus, slot, func, offset, 2);
    if ((offset & 0xfeU) == PCI_COMMAND) authorize_bus_master_write(bus, slot, func, value);
    ConfigGuard const GUARD;
    if (!bar_write_allowed(bus, slot, func, offset, 2)) return;
    // A unit may close after the pre-lock diagnostic check. Do not restore BME.
    if ((offset & 0xfeU) == PCI_COMMAND && !bus_master_transition_allowed_unlocked(bus, slot, func, value))
        value = static_cast<uint16_t>(value & ~PCI_COMMAND_BUS_MASTER);
    value = static_cast<uint16_t>(preserve_closed_intx_mask_unlocked(bus, slot, func, offset & 0xfeU, 2, value));
    if (!closed_irq_control_write_allowed_unlocked(bus, slot, func, offset & 0xfeU, 2, value)) return;
    outl(PCI_CONFIG_ADDRESS, config_address(bus, slot, func, offset));
    outw(static_cast<uint16_t>(PCI_CONFIG_DATA + (offset & 2U)), value);
}

// Write 8-bit to PCI configuration space
auto pci_config_write8(uint8_t bus, uint8_t slot, uint8_t func, uint8_t offset, uint8_t value) -> void {
    if (slot >= PCI_SLOT_COUNT || func >= PCI_FUNCTION_COUNT) return;
    authorize_bar_write(bus, slot, func, offset, 1);
    if (offset == PCI_COMMAND) authorize_bus_master_write(bus, slot, func, value);
    ConfigGuard const GUARD;
    if (!bar_write_allowed(bus, slot, func, offset, 1)) return;
    // A unit may close after the pre-lock diagnostic check. Do not restore BME.
    if (offset == PCI_COMMAND && !bus_master_transition_allowed_unlocked(bus, slot, func, value))
        value = static_cast<uint8_t>(value & ~PCI_COMMAND_BUS_MASTER);
    value = static_cast<uint8_t>(preserve_closed_intx_mask_unlocked(bus, slot, func, offset, 1, value));
    if (!closed_irq_control_write_allowed_unlocked(bus, slot, func, offset, 1, value)) return;
    outl(PCI_CONFIG_ADDRESS, config_address(bus, slot, func, offset));
    outb(static_cast<uint16_t>(PCI_CONFIG_DATA + (offset & 3U)), value);
}

// Enumerate all PCI devices across all buses
auto pci_enumerate_all() -> int {
    if (enumerated) {
        return device_count;
    }

    device_count = 0;
    enumeration_error = false;
    first_enumeration_error = InventoryError::none;
    enumeration_reasons = 0;
    visited_buses.fill(false);

    // Check if host bridge is multi-function (multiple root buses)
    uint8_t const HEADER_TYPE = pci_config_read8(0, 0, 0, PCI_HEADER_TYPE);
    if ((HEADER_TYPE & PCI_HEADER_TYPE_MULTI_FUNC) != 0) {
        for (uint8_t func = 0; func < PCI_FUNCTION_COUNT; func++) {
            if (pci_config_read16(0, 0, func, PCI_VENDOR_ID) != 0xFFFF) {
                scan_bus(func);
            }
        }
    } else {
        scan_bus(0);
    }

    enumerated = true;

    log::info("found %d devices (topology complete=%u) capacity=%zu first_error=%u reasons=0x%x", device_count,
              static_cast<unsigned>(!enumeration_error), inventory_limits::FUNCTION_CAPACITY,
              static_cast<unsigned>(first_enumeration_error), enumeration_reasons);
    for (int i = 0; i < device_count; i++) {
        auto* d = &discovered_devices.at(static_cast<size_t>(i));
        log::debug("%d:%d.%d vendor=%x device=%x class=%x:%x prog_if=%x", static_cast<int>(d->bus), static_cast<int>(d->slot),
                   static_cast<int>(d->function), static_cast<int>(d->vendor_id), static_cast<int>(d->device_id),
                   static_cast<int>(d->class_code), static_cast<int>(d->subclass_code), static_cast<int>(d->prog_if));
    }

    collect_extended_config();
    published_device_count.store(static_cast<size_t>(device_count), std::memory_order_release);
    return device_count;
}

auto pci_get_device(size_t idx) -> PCIDevice* {
    if (std::cmp_greater_equal(idx, device_count)) {
        return nullptr;
    }
    return &discovered_devices.at(idx);
}

auto pci_device_count() -> size_t { return static_cast<size_t>(device_count); }

auto pci_enumeration_complete() -> bool { return enumerated && !enumeration_error; }

auto pci_inventory_status() -> InventoryStatus {
    if (!enumerated) return {};
    return {.stored_count = static_cast<size_t>(device_count),
            .capacity = inventory_limits::FUNCTION_CAPACITY,
            .enumerated = true,
            .complete = !enumeration_error,
            .first_error = first_enumeration_error,
            .reason_mask = enumeration_reasons};
}

auto pci_find_by_class(uint8_t cls, uint8_t sub) -> PCIDevice* {
    if (!enumerated) {
        pci_enumerate_all();
    }
    for (int i = 0; i < device_count; i++) {
        auto& dev = discovered_devices.at(static_cast<size_t>(i));
        if (dev.class_code == cls && dev.subclass_code == sub) {
            return &dev;
        }
    }
    return nullptr;
}

auto pci_find_by_vendor_device(uint16_t vendor, uint16_t device) -> PCIDevice* {
    if (!enumerated) {
        pci_enumerate_all();
    }
    for (int i = 0; i < device_count; i++) {
        auto& dev = discovered_devices.at(static_cast<size_t>(i));
        if (dev.vendor_id == vendor && dev.device_id == device) {
            return &dev;
        }
    }
    return nullptr;
}

auto pci_enable_bus_master(PCIDevice* dev) -> void {
    if (!pci_set_bus_master(dev, true)) {
        mod::dbg::panic_handler("PCI bus mastering requested without a ready DMA domain");
        hcf();
    }
}

auto pci_set_bus_master(PCIDevice* dev, bool enable) -> bool {
    auto const INDEX = device_index(dev);
    if (!INDEX || (enable && (!driver_activation_allowed(*INDEX) || !resource_dma_allowed(*INDEX) || !iommu::bus_master_allowed(dev))))
        return false;
    ConfigGuard const GUARD;
    if (enable && (!driver_activation_allowed(*INDEX) || !resource_dma_allowed(*INDEX) || !iommu::bus_master_allowed(dev))) return false;
    outl(PCI_CONFIG_ADDRESS, config_address(dev->bus, dev->slot, dev->function, PCI_COMMAND));
    auto command = static_cast<uint16_t>(inl(PCI_CONFIG_DATA));
    command = enable ? static_cast<uint16_t>(command | PCI_COMMAND_BUS_MASTER) : static_cast<uint16_t>(command & ~PCI_COMMAND_BUS_MASTER);
    outw(PCI_CONFIG_DATA, command);
    return (static_cast<uint16_t>(inl(PCI_CONFIG_DATA)) & PCI_COMMAND_BUS_MASTER) == (enable ? PCI_COMMAND_BUS_MASTER : 0);
}

auto pci_enable_memory_space(PCIDevice* dev) -> void {
    auto const INDEX = device_index(dev);
    if (!INDEX) return;
    ConfigGuard const GUARD;
    if (resources[*INDEX].poisoned.load(std::memory_order_acquire) || !driver_activation_allowed(*INDEX)) return;
    ConfigReader reader{dev};
    reader.write16(PCI_COMMAND, static_cast<uint16_t>(reader.read16(PCI_COMMAND) | PCI_COMMAND_MEM_SPACE));
}

auto pci_find_capability(PCIDevice* dev, uint8_t cap_id) -> uint8_t {
    // Check if capabilities list is supported (bit 4 of Status register)
    uint16_t const STATUS = pci_config_read16(dev->bus, dev->slot, dev->function, PCI_STATUS);
    if ((STATUS & (1 << 4)) == 0) {
        return 0;
    }

    uint8_t offset = pci_config_read8(dev->bus, dev->slot, dev->function, PCI_CAP_PTR);
    offset &= 0xFC;  // Align to dword boundary

    int limit = 48;  // prevent infinite loops on malformed capability chains
    while (offset != 0 && limit-- > 0) {
        uint8_t const ID = pci_config_read8(dev->bus, dev->slot, dev->function, offset);
        if (ID == cap_id) {
            return offset;
        }
        offset = pci_config_read8(dev->bus, dev->slot, dev->function, offset + 1);
        offset &= 0xFC;
    }
    return 0;
}

auto pci_enable_msi(PCIDevice* dev, uint8_t vector, uint64_t target_cpu) -> int {
    auto const APIC = destination(target_cpu, vector);
    auto const INDEX = device_index(dev);
    if (!APIC || !INDEX) return -1;
    // Keep final admission and all capability writes in one transaction, so a
    // concurrent close cannot complete before a late MSI enable write.
    ConfigGuard const GUARD;
    if (!driver_activation_allowed(*INDEX) || !resource_dma_allowed(*INDEX)) return -1;
    if ((config_read16_unlocked(*dev, PCI_STATUS) & (1U << 4)) == 0) return -1;
    uint8_t offset = static_cast<uint8_t>(config_read32_unlocked(*dev, PCI_CAP_PTR) & 0xfcU);
    uint8_t MSI_OFF = 0;
    for (int limit = 48; offset >= 0x40 && limit-- > 0;) {
        auto const HEADER = config_read32_unlocked(*dev, offset);
        if ((HEADER & 255U) == PCI_CAP_ID_MSI) {
            MSI_OFF = offset;
            break;
        }
        offset = static_cast<uint8_t>((HEADER >> 8U) & 0xfcU);
    }
    if (MSI_OFF == 0) {
        return -1;
    }

    // MSI capability layout:
    // +0: Cap ID (8) | Next (8) | Message Control (16)
    // +4: Message Address (lower 32)
    // +8: Message Address (upper 32) [if 64-bit capable]
    // then: Message Data (16)

    uint16_t msg_ctrl = config_read16_unlocked(*dev, MSI_OFF + 2);
    bool const IS_64BIT = ((msg_ctrl >> 7) & 1) != 0;
    if (MSI_OFF > (IS_64BIT ? 0xf0 : 0xf4)) return -1;

    // Message Address: 0xFEE00000 | (dest APIC ID << 12), physical destination mode.
    // target_cpu is a logical CPU index; translate to APIC ID for the hardware.
    uint32_t const APIC_ID = *APIC;
    uint32_t const MSG_ADDR = 0xFEE00000U | (APIC_ID << 12);
    config_write32_unlocked(*dev, MSI_OFF + 4, MSG_ADDR);

    if (IS_64BIT) {
        config_write32_unlocked(*dev, MSI_OFF + 8, 0);
        config_write16_unlocked(*dev, MSI_OFF + 12, vector);
    } else {
        config_write16_unlocked(*dev, MSI_OFF + 8, vector);
    }

    // Enable MSI (bit 0), request 1 vector (bits 4-6 = 000)
    msg_ctrl = (msg_ctrl & ~(0x7 << 4)) | 1;
    config_write16_unlocked(*dev, MSI_OFF + 2, msg_ctrl);

    // Disable legacy INTx
    uint16_t cmd = config_read16_unlocked(*dev, PCI_COMMAND);
    cmd |= PCI_COMMAND_INT_DISABLE;
    ConfigReader reader{dev};
    reader.write16(PCI_COMMAND, cmd);

    return 0;
}

auto pci_prepare_msix(PCIDevice* dev) -> int {
    auto const INDEX = device_index(dev);
    if (!INDEX) return -1;
    auto& record = resources[*INDEX];
    ConfigGuard guard;
    if (record.poisoned.load(std::memory_order_acquire)) return -1;
    if (auto const* prepared = record.msix.get()) return prepared->present ? 0 : 1;
    if (!driver_activation_allowed(*INDEX)) return -1;
    auto operation = try_dma_operation(dev);
    if (driver_slots[*INDEX].phase.load(std::memory_order_acquire) != DriverPhase::unclaimed && !operation) return -1;
    if ((config_read16_unlocked(*dev, PCI_COMMAND) & PCI_COMMAND_BUS_MASTER) != 0) return -1;
    auto const* prepared = record.msix.prepare([&]() -> std::optional<PreparedMsix> {
        ConfigReader reader{dev};
        auto const CAP = msix::inspect(reader);
        if (CAP.state == msix::CapabilityState::absent) return PreparedMsix{};
        if (CAP.state != msix::CapabilityState::ready) {
            poison_unlocked(*INDEX);
            return std::nullopt;
        }
        auto const CONTROL = static_cast<uint8_t>(CAP.offset + 2);
        // Some devices expose the table only with MSI-X enabled. FunctionMask
        // remains set throughout preparation, so this admits no IRQ delivery.
        reader.write16(CONTROL, static_cast<uint16_t>(CAP.control | 0xc000U));
        if (reader.read16(CONTROL) != static_cast<uint16_t>(CAP.control | 0xc000U)) {
            poison_unlocked(*INDEX, CAP.offset);
            return std::nullopt;
        }
        // The preparing record blocks BME/relocation while page-table work is
        // performed with the original IRQ state restored and no config lock.
        guard.release();
        auto fail = [&]() -> std::optional<PreparedMsix> {
            ConfigGuard const GUARD;
            poison_unlocked(*INDEX, CAP.offset);
            return std::nullopt;
        };
        std::array<uint64_t, BAR_COUNT> sizes{};
        for (auto bar : {CAP.table & 7U, CAP.pba & 7U}) {
            if (bar >= BAR_COUNT) return fail();
            auto const SIZE = pci_get_bar_size_checked(dev, static_cast<int>(bar));
            if (!SIZE || *SIZE == 0) return fail();
            sizes[bar] = *SIZE;
        }
        auto const LAYOUT = msix::layout(CAP, dev->bar, dev->header_type, sizes);
        if (!LAYOUT) return fail();
        auto* base = static_cast<uint8_t*>(pci_map_bar(dev, LAYOUT->table_bar));
        if (base == nullptr || pci_map_bar(dev, LAYOUT->pba_bar) == nullptr) return fail();
        auto* table = reinterpret_cast<volatile uint32_t*>(base + LAYOUT->table_offset);
        {
            ConfigGuard const GUARD;
            if (!driver_activation_allowed(*INDEX)) {
                poison_unlocked(*INDEX, CAP.offset);
                return std::nullopt;
            }
            uint16_t const COMMAND = config_read16_unlocked(*dev, PCI_COMMAND);
            reader.write16(PCI_COMMAND, static_cast<uint16_t>(COMMAND | PCI_COMMAND_MEM_SPACE));
            if (reader.read16(PCI_COMMAND) != static_cast<uint16_t>(COMMAND | PCI_COMMAND_MEM_SPACE)) {
                poison_unlocked(*INDEX, CAP.offset);
                return std::nullopt;
            }
        }
        // Do not expose inherited firmware vector entries when activation
        // clears FunctionMask. Short sections keep the 2048-entry case bounded.
        for (uint32_t entry = 0; entry < LAYOUT->entries; ++entry) {
            ConfigGuard const GUARD;
            uint32_t const WORD = entry * 4U + 3U;
            uint32_t const MASKED = table[WORD] | 1U;
            table[WORD] = MASKED;
            if (table[WORD] != MASKED) {
                poison_unlocked(*INDEX, CAP.offset);
                return std::nullopt;
            }
        }
        return PreparedMsix{true, *LAYOUT, table};
    });
    return prepared == nullptr ? -1 : prepared->present ? 0 : 1;
}

auto pci_enable_msix(PCIDevice* dev, uint8_t vector, uint64_t target_cpu) -> int {
    auto const INDEX = device_index(dev);
    auto const APIC = destination(target_cpu, vector);
    if (!INDEX || !APIC) return -1;
    ConfigGuard const GUARD;
    auto& record = resources[*INDEX];
    auto const* prepared = record.msix.get();
    if (!prepared || !prepared->present || record.activated || record.poisoned.load(std::memory_order_acquire) ||
        !driver_activation_allowed(*INDEX) || !iommu::bus_master_allowed(dev))
        return -1;
    ConfigReader reader{dev};
    auto const OFFSET = static_cast<uint8_t>(prepared->layout.capability + 2);
    uint16_t const CONTROL = reader.read16(OFFSET);
    reader.write16(OFFSET, static_cast<uint16_t>(CONTROL | 0xc000U));
    if (reader.read16(OFFSET) != static_cast<uint16_t>(CONTROL | 0xc000U)) {
        poison_unlocked(*INDEX, prepared->layout.capability);
        return -1;
    }
    TableReader table{*INDEX, *prepared};
    if (!msix::program_entry(table, prepared->layout.entries, 0, vector, *APIC)) return -1;
    uint16_t const COMMAND = reader.read16(PCI_COMMAND);
    reader.write16(PCI_COMMAND, static_cast<uint16_t>(COMMAND | PCI_COMMAND_INT_DISABLE));
    if (reader.read16(PCI_COMMAND) != static_cast<uint16_t>(COMMAND | PCI_COMMAND_INT_DISABLE)) {
        poison_unlocked(*INDEX, prepared->layout.capability);
        return -1;
    }
    uint16_t const ENABLED = static_cast<uint16_t>((CONTROL | 0x8000U) & ~0x4000U);
    reader.write16(OFFSET, ENABLED);
    if (reader.read16(OFFSET) != ENABLED) {
        poison_unlocked(*INDEX, prepared->layout.capability);
        return -1;
    }
    record.activated = true;
    return 0;
}

auto pci_get_bar_addr(PCIDevice* dev, int bar_idx) -> uint64_t {
    if (!device_index(dev) || bar_idx < 0) return 0;
    return resource::decode_bar(dev->bar, dev->header_type, static_cast<size_t>(bar_idx)).address;
}

auto pci_get_bar_size_checked(PCIDevice* dev, int bar_idx) -> std::optional<uint64_t> {
    auto const INDEX = device_index(dev);
    if (!INDEX || bar_idx < 0 || std::cmp_greater_equal(bar_idx, BAR_COUNT) ||
        !resource::decode_bar(dev->bar, dev->header_type, static_cast<size_t>(bar_idx)).memory())
        return std::nullopt;
    auto& record = resources[*INDEX];
    auto& cache = record.sizes[static_cast<size_t>(bar_idx)];
    if (record.poisoned.load(std::memory_order_acquire)) return std::nullopt;
    if (auto const* value = cache.get()) return *value;
    if (!driver_activation_allowed(*INDEX)) return std::nullopt;
    ConfigGuard const GUARD;
    if (record.poisoned.load(std::memory_order_acquire)) return std::nullopt;
    if (auto const* value = cache.get()) return *value;
    if (!driver_activation_allowed(*INDEX)) return std::nullopt;
    if ((config_read16_unlocked(*dev, PCI_COMMAND) & PCI_COMMAND_BUS_MASTER) != 0) return std::nullopt;
    auto const* value = cache.prepare([&]() -> std::optional<uint64_t> {
        ConfigReader reader{dev};
        auto const SIZE = resource::probe_memory_bar(dev->bar, dev->header_type, static_cast<size_t>(bar_idx), reader);
        if (!SIZE) poison_unlocked(*INDEX);
        return SIZE;
    });
    return value ? std::optional<uint64_t>{*value} : std::nullopt;
}

auto pci_get_bar_size(PCIDevice* dev, int bar_idx) -> uint64_t { return pci_get_bar_size_checked(dev, bar_idx).value_or(0); }

auto pci_map_bar(PCIDevice* dev, int bar_idx) -> void* {
    auto const INDEX = device_index(dev);
    if (!INDEX || bar_idx < 0 || std::cmp_greater_equal(bar_idx, BAR_COUNT)) return nullptr;
    auto const BAR = resource::decode_bar(dev->bar, dev->header_type, static_cast<size_t>(bar_idx));
    if (!BAR.memory() || BAR.address == 0) return nullptr;
    auto& record = resources[*INDEX];
    auto& cache = record.mappings[static_cast<size_t>(bar_idx)];
    ConfigGuard guard;
    if (record.poisoned.load(std::memory_order_acquire)) return nullptr;
    if (auto const* value = cache.get()) return *value;
    if (!driver_activation_allowed(*INDEX)) return nullptr;
    auto operation = try_dma_operation(dev);
    if (driver_slots[*INDEX].phase.load(std::memory_order_acquire) != DriverPhase::unclaimed && !operation) return nullptr;
    if ((config_read16_unlocked(*dev, PCI_COMMAND) & PCI_COMMAND_BUS_MASTER) != 0) return nullptr;
    auto const* value = cache.prepare([&]() -> std::optional<void*> {
        guard.release();
        auto fail = [&]() -> std::optional<void*> {
            ConfigGuard const GUARD;
            poison_unlocked(*INDEX);
            return std::nullopt;
        };
        auto const SIZE = pci_get_bar_size_checked(dev, bar_idx);
        if (!SIZE || *SIZE == 0) return fail();
        mod::cpu::CpuidContext cpu{};
        cpu.function = static_cast<int>(0x80000000U);
        mod::cpu::cpuid(&cpu);
        if (static_cast<uint32_t>(cpu.eax) < 0x80000008U) return fail();
        cpu.function = static_cast<int>(0x80000008U);
        mod::cpu::cpuid(&cpu);
        auto const RANGE = resource::mmio_range(BAR, *SIZE, mod::mm::addr::get_hhdm_offset(), cpu.eax & 255U);
        if (!RANGE) return fail();
        auto const* memory = mod::mm::boot_memory_map();
        if (memory == nullptr || memory->entries == nullptr ||
            !resource::mmio_memory_safe(*RANGE, memory->entry_count, [memory](size_t i) -> std::optional<resource::MemoryRegion> {
                auto const* entry = memory->entries[i];
                if (entry == nullptr) return std::nullopt;
                return resource::MemoryRegion{entry->base, entry->length, entry->type == LIMINE_MEMMAP_RESERVED};
            }))
            return fail();
        mod::mm::virt::map_range_to_kernel_page_table({RANGE->start, RANGE->end}, mod::mm::virt::kernel_mmio_page_flags());
        // Mapping is retained even when closure wins during page-table work;
        // never hand a newly prepared pointer to the closed probe. Generic
        // unmap cannot release these borrowed device frames.
        ConfigGuard const GUARD;
        if (!driver_activation_allowed(*INDEX)) {
            poison_unlocked(*INDEX);
            return std::nullopt;
        }
        return reinterpret_cast<void*>(mod::mm::addr::get_virt_pointer(BAR.address));
    });
    return value ? *value : nullptr;
}

// Legacy wrapper: find AHCI controller using full enumeration
auto pci_configure_msix_entry(PCIDevice* dev, uint32_t entry_idx, uint8_t vector, uint64_t target_cpu) -> int {
    auto const INDEX = device_index(dev);
    auto const APIC = destination(target_cpu, vector);
    if (!INDEX || !APIC) return -1;
    ConfigGuard const GUARD;
    auto const& record = resources[*INDEX];
    auto const* prepared = record.msix.get();
    if (!prepared || !prepared->present || !record.activated || record.poisoned.load(std::memory_order_acquire) ||
        !driver_activation_allowed(*INDEX) || !iommu::bus_master_allowed(dev))
        return -1;
    TableReader reader{*INDEX, *prepared};
    return msix::program_entry(reader, prepared->layout.entries, entry_idx, vector, *APIC) ? 0 : -1;
}

auto pci_mask_msix_entry(PCIDevice* dev, uint32_t entry_idx, bool masked) -> int {
    auto const INDEX = device_index(dev);
    if (!INDEX) return -1;
    ConfigGuard const GUARD;
    auto const& record = resources[*INDEX];
    auto const* prepared = record.msix.get();
    if (!prepared || !prepared->present || entry_idx >= prepared->layout.entries || record.poisoned.load(std::memory_order_acquire))
        return -1;
    if (!masked && (!record.activated || !driver_activation_allowed(*INDEX) || !iommu::bus_master_allowed(dev))) return -1;
    uint32_t const WORD = entry_idx * 4U + 3U;
    uint32_t const CONTROL = prepared->table[WORD];
    uint32_t const VALUE = masked ? CONTROL | 1U : CONTROL & ~uint32_t{1};
    prepared->table[WORD] = VALUE;
    if (prepared->table[WORD] != VALUE) {
        poison_unlocked(*INDEX, prepared->layout.capability);
        return -1;
    }
    return 0;
}

auto pci_find_ahci_controller() -> PCIDevice* {
    if (!enumerated) {
        pci_enumerate_all();
    }

    for (int i = 0; i < device_count; i++) {
        auto* dev = &discovered_devices.at(static_cast<size_t>(i));
        bool const IS_AHCI = (dev->class_code == PCI_CLASS_STORAGE && dev->subclass_code == PCI_SUBCLASS_SATA) ||
                             (dev->vendor_id == 0x8086 && dev->device_id == 0x2922);
        if (IS_AHCI) {
            pci_log("pci: FOUND AHCI SATA CONTROLLER\n");
            return dev;
        }
    }
    pci_log("pci: No AHCI controller found\n");
    return nullptr;
}

}  // namespace ker::dev::pci
