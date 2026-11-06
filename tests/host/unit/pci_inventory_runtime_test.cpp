#include <gtest/gtest.h>
#include <pthread.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <dev/iommu/discovery.hpp>
#include <dev/iommu/domains.hpp>
#include <dev/iommu/groups.hpp>
#include <dev/pci.hpp>
#include <dev/pci_driver_claim.hpp>
#include <dev/pci_extended.hpp>
#include <dev/pci_port_io.hpp>
#include <limits>
#include <memory>
#include <platform/acpi/acpi.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/mm/mm.hpp>
#include <span>
#include <string_view>
#include <test_backend.hpp>
#include <vector>

// Link the complete production PCI, ECAM collector and IOMMU discovery TUs.
// Only firmware/config/CPU/MMIO and not-yet-published domain state are ports.
// No test reset or replacement collector/ownership algorithm is provided.
#define PCI_LEGACY_ACTUAL_EXTENDED
#define PCI_LEGACY_NO_DMA_ENGINE
#include "pci_legacy_mmio_ports.inc"

namespace pci_inventory_runtime {
namespace pci = ker::dev::pci;
namespace iommu = ker::dev::iommu;
namespace acpi = ker::mod::acpi::bounded;
thread_local unsigned lock_depth = 0;
std::atomic<uint64_t> lock_acquisitions{0};
constexpr uint64_t ECAM_BASE = 0xe0000000;
constexpr uint64_t BAR_BASE = 0x80000000;
constexpr uint64_t IOMMU_BASE = 0xfed90000;
constexpr uint64_t RSDP = 0x100, XSDT = 0x1000, DMAR = 0x4000, MCFG = 0x8000;
constexpr uint64_t CAP = (uint64_t{3} << 54) | (uint64_t{0x22} << 24) | (uint64_t{47} << 16) | (6U << 8) | 6U;
constexpr uint64_t ECAP = (0xfU << 8) | 0x41U;

auto rid(uint8_t bus, uint8_t slot, uint8_t function) -> uint16_t {
    return static_cast<uint16_t>((uint16_t{bus} << 8U) | (slot << 3U) | function);
}
struct Function {
    uint8_t bus{}, slot{}, function{};
    uint32_t bar_address{};
    uint32_t bar_bytes = 4096;
    alignas(4096) std::array<uint8_t, 4096> config{};
    alignas(4096) std::array<uint8_t, 8192> bar{};
    std::array<bool, 6> sizing{};
    auto source() const -> uint16_t { return rid(bus, slot, function); }
};
struct Blob {
    uint64_t physical;
    std::vector<uint8_t> bytes;
};
struct Counters {
    uint64_t config_io{}, sizing_probes{}, bar_maps{}, ecam_maps{}, iommu_maps{}, firmware_reads{}, firmware_lookups{};
    auto operator==(const Counters&) const -> bool = default;
};
struct Fixture {
    std::vector<std::unique_ptr<Function>> functions;
    std::vector<Function*> expected;
    std::vector<uint16_t> parents;
    std::vector<Blob> firmware;
    Counters calls;
    uint32_t address{};
    bool scan_active = false;
    uintptr_t lowest_frame = UINTPTR_MAX, highest_frame = 0;
    void* registers{};

    Fixture() {
        registers = mmap(nullptr, 4096, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        assert(registers != MAP_FAILED);
        uint32_t const VERSION = 0x10;
        auto* bytes = static_cast<uint8_t*>(registers);
        std::memcpy(bytes, &VERSION, 4);
        std::memcpy(bytes + 8, &CAP, 8);
        std::memcpy(bytes + 16, &ECAP, 8);
        assert(mprotect(registers, 4096, PROT_READ) == 0);
        boot_firmware();
    }
    ~Fixture() { assert(munmap(registers, 4096) == 0); }
    static void put(std::span<uint8_t> bytes, size_t offset, uint64_t value, size_t count) {
        assert(offset <= bytes.size() && count <= bytes.size() - offset);
        for (size_t i = 0; i < count; ++i) bytes[offset + i] = static_cast<uint8_t>(value >> (8U * i));
    }
    static auto load(std::span<const uint8_t> bytes, size_t offset, size_t count) -> uint32_t {
        assert(offset <= bytes.size() && count <= bytes.size() - offset);
        uint32_t value = 0;
        for (size_t i = 0; i < count; ++i) value |= uint32_t{bytes[offset + i]} << (8U * i);
        return value;
    }
    static void checksum(std::vector<uint8_t>& bytes, size_t at) {
        bytes.at(at) = 0;
        uint8_t sum = 0;
        for (auto value : bytes) sum = static_cast<uint8_t>(sum + value);
        bytes.at(at) = static_cast<uint8_t>(0U - sum);
    }
    static auto table(std::string_view signature, size_t length) -> std::vector<uint8_t> {
        std::vector<uint8_t> bytes(length);
        std::copy(signature.begin(), signature.end(), bytes.begin());
        put(bytes, 4, length, 4);
        bytes.at(8) = 1;
        return bytes;
    }
    void boot_firmware() {
        auto dmar = table("DMAR", 64);
        dmar[36] = 47;
        dmar[50] = 16;
        dmar[52] = 1;
        put(dmar, 56, IOMMU_BASE, 8);
        checksum(dmar, 9);
        auto mcfg = table("MCFG", 60);
        put(mcfg, 44, ECAM_BASE, 8);
        mcfg[55] = 255;
        checksum(mcfg, 9);
        auto xsdt = table("XSDT", 52);
        put(xsdt, 36, DMAR, 8);
        put(xsdt, 44, MCFG, 8);
        checksum(xsdt, 9);
        std::vector<uint8_t> rsdp(36);
        constexpr std::string_view SIGNATURE = "RSD PTR ";
        std::copy(SIGNATURE.begin(), SIGNATURE.end(), rsdp.begin());
        rsdp[15] = 2;
        put(rsdp, 20, 36, 4);
        put(rsdp, 24, XSDT, 8);
        uint8_t sum = 0;
        for (size_t i = 0; i < 20; ++i) sum = static_cast<uint8_t>(sum + rsdp[i]);
        rsdp[8] = static_cast<uint8_t>(0U - sum);
        checksum(rsdp, 32);
        firmware = {{RSDP, std::move(rsdp)}, {XSDT, std::move(xsdt)}, {DMAR, std::move(dmar)}, {MCFG, std::move(mcfg)}};
    }
    auto read(uint64_t physical, size_t length) -> std::span<const uint8_t> {
        ++calls.firmware_reads;
        for (auto const& blob : firmware)
            if (acpi::contains(blob.physical, blob.bytes.size(), physical, length))
                return std::span<const uint8_t>{blob.bytes}.subspan(physical - blob.physical, length);
        return {};
    }
    auto find(uint16_t source) -> Function* {
        for (auto& value : functions)
            if (value->source() == source) return value.get();
        return nullptr;
    }
    auto add(uint8_t bus, uint8_t slot, uint8_t function = 0, uint8_t header = 0) -> Function& {
        assert(find(rid(bus, slot, function)) == nullptr);
        auto object = std::make_unique<Function>();
        auto& value = *object;
        value.bus = bus;
        value.slot = slot;
        value.function = function;
        value.bar_address = static_cast<uint32_t>(BAR_BASE + functions.size() * 0x10000);
        value.bar_bytes = functions.size() % 2 == 0 ? 4096 : 8192;
        put(value.config, 0, (uint32_t{static_cast<uint16_t>(functions.size() + 1)} << 16U) | 0x1234U, 4);
        put(value.config, 4, 2, 2);  // MEM enabled, BME always off.
        put(value.config, 0x0e, header, 1);
        put(value.config, 0x10, value.bar_address, 4);
        if ((header & 0x7fU) == 1) put(value.config, 8, 0x06040000, 4);
        functions.push_back(std::move(object));
        return value;
    }
    void bridge(Function& value, uint8_t secondary, uint8_t subordinate, bool alias = false) {
        put(value.config, 0x18, value.bus | (uint32_t{secondary} << 8U) | (uint32_t{subordinate} << 16U), 4);
        if (alias) {
            put(value.config, 6, 0x10, 2);
            value.config[0x34] = 0x40;
            value.config[0x40] = 0x10;
            value.config[0x42] = 0x71;  // Version1 PCIe-to-PCI bridge, no ACS.
        }
    }
    void expect(Function& value, uint16_t parent = pci::NO_PARENT) {
        expected.push_back(&value);
        parents.push_back(parent);
    }
    void mixed(size_t count) {
        assert(count >= 3 && count <= 259);
        auto& host = add(0, 0);
        auto& downstream = add(0, 1, 0, 0x81);
        bridge(downstream, 1, 1);
        auto& companion = add(0, 1, 1);
        companion.bar_bytes = 8192;
        expect(host);
        expect(downstream);
        for (size_t i = 0; i < count - 3; ++i) {
            auto& child = add(1, static_cast<uint8_t>(i / 8), static_cast<uint8_t>(i % 8), i % 8 == 0 ? 0x80 : 0);
            expect(child, 1);
        }
        expect(companion);
    }
    void deep() {
        for (size_t i = 0; i < 256; ++i) {
            bool const BRIDGE = i != 255;
            auto& value = add(static_cast<uint8_t>(i), i == 0 ? 0 : 1, 0, BRIDGE ? 1 : 0);
            if (BRIDGE) bridge(value, static_cast<uint8_t>(i + 1), 255, true);
            expect(value, i == 0 ? pci::NO_PARENT : static_cast<uint16_t>(i - 1));
        }
    }
};
Fixture* active{};

template <typename Fn>
void isolated(Fn&& fn) {
    EXPECT_EXIT(
        {
            alarm(20);
            {
                Fixture fixture;
                active = &fixture;
                EXPECT_EQ(pci::pci_device_count(), 0U);
                auto const BEFORE = pci::pci_inventory_status();
                EXPECT_FALSE(BEFORE.enumerated);
                EXPECT_FALSE(BEFORE.complete);
                EXPECT_EQ(BEFORE.stored_count, 0U);
                EXPECT_EQ(BEFORE.reason_mask, 0U);
                fn(fixture);
                active = nullptr;
            }
            std::_Exit(::testing::Test::HasFailure() ? 17 : 0);
        },
        ::testing::ExitedWithCode(0), ".*");
}

void enumerate(Fixture& fixture, bool guarded_stack = false) {
    auto run = [](void* context) -> void* {
        auto& f = *static_cast<Fixture*>(context);
        f.scan_active = true;
        (void)pci::pci_enumerate_all();
        f.scan_active = false;
        iommu::discover();
        return nullptr;
    };
    if (!guarded_stack) {
        (void)run(&fixture);
        return;
    }
    constexpr size_t PAGE = 4096, STACK = 64 * 1024;
    void* memory = mmap(nullptr, STACK + 2 * PAGE, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ASSERT_NE(memory, MAP_FAILED);
    auto* usable = static_cast<uint8_t*>(memory) + PAGE;
    ASSERT_EQ(mprotect(usable, STACK, PROT_READ | PROT_WRITE), 0);
    pthread_attr_t attributes;
    ASSERT_EQ(pthread_attr_init(&attributes), 0);
    ASSERT_EQ(pthread_attr_setstack(&attributes, usable, STACK), 0);
    pthread_t thread;
    ASSERT_EQ(pthread_create(&thread, &attributes, run, &fixture), 0);
    ASSERT_EQ(pthread_attr_destroy(&attributes), 0);
    ASSERT_EQ(pthread_join(thread, nullptr), 0);
    ASSERT_EQ(munmap(memory, STACK + 2 * PAGE), 0);
    // Frame addresses measure the real host stack, not ASan's optional fake
    // stack. Native kernel frames are audited separately by qualification.
    ASSERT_GT(fixture.highest_frame, 0U);
    EXPECT_LT(fixture.highest_frame - fixture.lowest_frame, 16 * 1024U);
}

void retained_registry(Fixture& fixture, size_t count) {
    ASSERT_EQ(pci::pci_device_count(), count);
    ASSERT_GE(fixture.expected.size(), count);
    for (size_t i = 0; i < count; ++i) {
        auto const* value = pci::pci_get_device(i);
        ASSERT_NE(value, nullptr);
        auto const& expected = *fixture.expected[i];
        EXPECT_EQ(value->bus, expected.bus);
        EXPECT_EQ(value->slot, expected.slot);
        EXPECT_EQ(value->function, expected.function);
        EXPECT_EQ(value->parent_index, fixture.parents[i]);
        EXPECT_EQ(value->bar[0], expected.bar_address);
        EXPECT_EQ(pci::pci_get_device(i), value);
    }
    EXPECT_EQ(pci::pci_get_device(count), nullptr);
    EXPECT_EQ(pci::pci_get_device(SIZE_MAX), nullptr);
}

auto grouping(size_t count) -> iommu::groups::Result {
    std::vector<iommu::scopes::Device> nodes;
    std::vector<iommu::groups::Evidence> evidence;
    for (size_t i = 0; i < count; ++i) {
        auto const* value = pci::pci_get_device(i);
        assert(value != nullptr);
        nodes.push_back({.bus = value->bus,
                         .slot = value->slot,
                         .function = value->function,
                         .header = static_cast<uint8_t>(value->header_type & 0x7fU),
                         .parent = value->parent_index,
                         .secondary = value->secondary_bus,
                         .subordinate = value->subordinate_bus});
        auto const* snapshot = pci::extended_config(value);
        assert(snapshot != nullptr);
        evidence.push_back({.readable = snapshot->state == pci::ExtendedState::ready, .capability = snapshot->capabilities});
    }
    auto const* found = iommu::discovery();
    assert(found != nullptr);
    return iommu::groups::qualify(nodes, evidence, found->ownership);
}

void complete_ledgers(Fixture& fixture, size_t count) {
    retained_registry(fixture, count);
    EXPECT_TRUE(pci::pci_enumeration_complete());
    auto const STATUS = pci::pci_inventory_status();
    EXPECT_EQ(STATUS.stored_count, count);
    EXPECT_EQ(STATUS.capacity, pci::inventory_limits::FUNCTION_CAPACITY);
    EXPECT_TRUE(STATUS.enumerated);
    EXPECT_TRUE(STATUS.complete);
    EXPECT_EQ(STATUS.first_error, pci::InventoryError::none);
    EXPECT_EQ(STATUS.reason_mask, 0U);
    auto const ECAM = pci::extended_inventory();
    EXPECT_EQ(ECAM.state, pci::ExtendedState::ready);
    EXPECT_EQ(ECAM.devices, count);
    EXPECT_EQ(ECAM.readable, count);
    EXPECT_EQ(fixture.calls.ecam_maps, count);
    for (size_t i = 0; i < count; ++i) {
        auto const* device = pci::pci_get_device(i);
        auto const* snapshot = pci::extended_config(device);
        ASSERT_NE(snapshot, nullptr);
        EXPECT_EQ(snapshot->state, pci::ExtendedState::ready);
        EXPECT_EQ(snapshot->physical, ECAM_BASE + uint64_t{rid(device->bus, device->slot, device->function)} * 4096);
        EXPECT_TRUE(snapshot->capabilities.valid());
        EXPECT_EQ(pci::extended_config(device), snapshot);
    }
    auto const* found = iommu::discovery();
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found->vendor, iommu::FirmwareVendor::intel);
    EXPECT_EQ(found->state, iommu::DiscoveryState::discovered);
    EXPECT_EQ(found->unit_count, 1U);
    ASSERT_TRUE(found->ownership.valid());
    EXPECT_EQ(found->ownership.device_count, count);
    for (size_t i = 0; i < count; ++i) EXPECT_EQ(found->ownership.devices[i].unit, 0);
    auto const GROUPS = grouping(count);
    EXPECT_TRUE(GROUPS.valid());
    EXPECT_EQ(GROUPS.devices, count);
}

void rejected_ledgers(Fixture& fixture, pci::InventoryError reason) {
    EXPECT_FALSE(pci::pci_enumeration_complete());
    auto const STATUS = pci::pci_inventory_status();
    EXPECT_TRUE(STATUS.enumerated);
    EXPECT_FALSE(STATUS.complete);
    EXPECT_EQ(STATUS.first_error, reason);
    EXPECT_NE(STATUS.reason_mask & static_cast<uint32_t>(reason), 0U);
    EXPECT_NE(pci::extended_inventory().state, pci::ExtendedState::ready);
    EXPECT_EQ(fixture.calls.ecam_maps, 0U);
    for (size_t i = 0; i < pci::pci_device_count(); ++i) {
        auto const BEFORE = fixture.calls;
        auto* device = pci::pci_get_device(i);
        EXPECT_FALSE(pci::try_claim_driver(device, pci::DriverKind::diagnostic));
        EXPECT_FALSE(pci::begin_dma_allocation(device));
        EXPECT_FALSE(pci::try_dma_operation(device));
        EXPECT_FALSE(pci::driver_dma_admission_allowed(device));
        EXPECT_EQ(pci::driver_claim_status(device)->phase, pci::DriverPhase::unclaimed);
        EXPECT_EQ(fixture.calls, BEFORE);
        auto const* snapshot = pci::extended_config(pci::pci_get_device(i));
        ASSERT_NE(snapshot, nullptr);
        EXPECT_NE(snapshot->state, pci::ExtendedState::ready);
    }
    auto const* found = iommu::discovery();
    ASSERT_NE(found, nullptr);
    EXPECT_FALSE(found->ownership.valid());
    EXPECT_EQ(found->ownership.error, iommu::scopes::Error::incomplete);
    EXPECT_EQ(found->ownership.device_count, 0U);
    EXPECT_EQ(grouping(pci::pci_device_count()).error, iommu::groups::Error::incomplete);
}

void idempotent(Fixture& fixture) {
    auto const BEFORE = fixture.calls;
    auto const COUNT = pci::pci_device_count();
    auto const* FIRST = pci::pci_get_device(0);
    auto const* LAST = COUNT == 0 ? nullptr : pci::pci_get_device(COUNT - 1);
    auto const* FOUND = iommu::discovery();
    EXPECT_EQ(pci::pci_enumerate_all(), COUNT);
    pci::collect_extended_config();
    iommu::discover();
    EXPECT_EQ(fixture.calls, BEFORE);
    EXPECT_EQ(pci::pci_get_device(0), FIRST);
    EXPECT_EQ(COUNT == 0 ? nullptr : pci::pci_get_device(COUNT - 1), LAST);
    EXPECT_EQ(iommu::discovery(), FOUND);
}

void first_and_last_resources(Fixture& fixture) {
    for (size_t index : {size_t{0}, pci::pci_device_count() - 1}) {
        auto* device = pci::pci_get_device(index);
        auto& expected = *fixture.expected[index];
        auto const SIZE = pci::pci_get_bar_size_checked(device, 0);
        ASSERT_TRUE(SIZE);
        EXPECT_EQ(*SIZE, expected.bar_bytes);
        void* const MAPPING = pci::pci_map_bar(device, 0);
        EXPECT_EQ(MAPPING, expected.bar.data());
        auto const BEFORE = fixture.calls;
        EXPECT_EQ(pci::pci_get_bar_size_checked(device, 0), SIZE);
        EXPECT_EQ(pci::pci_map_bar(device, 0), MAPPING);
        EXPECT_EQ(fixture.calls, BEFORE);
        auto copied = *device;
        EXPECT_FALSE(pci::pci_get_bar_size_checked(&copied, 0));
        EXPECT_EQ(pci::pci_map_bar(&copied, 0), nullptr);
        EXPECT_EQ(fixture.calls, BEFORE);
        EXPECT_FALSE(pci::pci_set_bus_master(device, true));  // No domain authority in this fixture.
        EXPECT_EQ(fixture.calls, BEFORE);
    }
}
}  // namespace pci_inventory_runtime

namespace ker::dev::pci::port_io {
auto inl(uint16_t port) -> uint32_t {
    auto& fixture = *pci_inventory_runtime::active;
    assert(pci_inventory_runtime::lock_depth == 1 && port == PCI_CONFIG_DATA && (fixture.address & 0x80000003U) == 0x80000000U);
    ++fixture.calls.config_io;
    if (fixture.scan_active) {
        auto const FRAME = reinterpret_cast<uintptr_t>(__builtin_frame_address(0));
        fixture.lowest_frame = std::min(fixture.lowest_frame, FRAME);
        fixture.highest_frame = std::max(fixture.highest_frame, FRAME);
    }
    auto* function = fixture.find(static_cast<uint16_t>(fixture.address >> 8U));
    if (function == nullptr) return UINT32_MAX;
    size_t const OFFSET = fixture.address & 0xfcU;
    if (OFFSET >= 0x10 && OFFSET < 0x28 && function->sizing[(OFFSET - 0x10) / 4]) return OFFSET == 0x10 ? ~(function->bar_bytes - 1U) : 0;
    return pci_inventory_runtime::Fixture::load(function->config, OFFSET, 4);
}
void write(uint16_t port, uint32_t value, size_t count) {
    auto& fixture = *pci_inventory_runtime::active;
    assert(pci_inventory_runtime::lock_depth == 1 && port >= PCI_CONFIG_DATA && port + count <= PCI_CONFIG_DATA + 4);
    ++fixture.calls.config_io;
    auto* function = fixture.find(static_cast<uint16_t>(fixture.address >> 8U));
    if (function == nullptr) return;
    size_t const OFFSET = (fixture.address & 0xfcU) + port - PCI_CONFIG_DATA;
    if (count == 4 && OFFSET >= 0x10 && OFFSET < 0x28) {
        auto const BAR = (OFFSET - 0x10) / 4;
        if (value == UINT32_MAX) {
            function->sizing[BAR] = true;
            ++fixture.calls.sizing_probes;
            return;
        }
        function->sizing[BAR] = false;
    }
    assert(!(OFFSET == 4 && count == 4));  // Preserve status W1C: command writes are 16-bit.
    pci_inventory_runtime::Fixture::put(function->config, OFFSET, value, count);
}
void outl(uint16_t port, uint32_t value) {
    if (port == PCI_CONFIG_ADDRESS) {
        assert(pci_inventory_runtime::lock_depth == 1);
        ++pci_inventory_runtime::active->calls.config_io;
        pci_inventory_runtime::active->address = value;
    } else {
        write(port, value, 4);
    }
}
void outw(uint16_t port, uint16_t value) { write(port, value, 2); }
void outb(uint16_t port, uint8_t value) { write(port, value, 1); }
}  // namespace ker::dev::pci::port_io

namespace ker::mod::cpu {
void cpuid(CpuidContext* context) {
    context->eax = context->function == static_cast<int>(0x80000000U) ? static_cast<int>(0x80000008U) : 48;
}
}  // namespace ker::mod::cpu
namespace ker::mod::smt {
auto get_core_count() -> uint64_t { return 2; }
auto get_apic_id_for_cpu(uint64_t cpu) -> uint32_t { return static_cast<uint32_t>(cpu); }
}  // namespace ker::mod::smt
namespace ker::mod::acpi {
auto find_table_bounded(std::string_view signature) -> bounded::Result {
    auto& fixture = *pci_inventory_runtime::active;
    ++fixture.calls.firmware_lookups;
    auto reader = [&fixture](uint64_t physical, size_t count) { return fixture.read(physical, count); };
    return bounded::find(reader, pci_inventory_runtime::RSDP, signature);
}
}  // namespace ker::mod::acpi
namespace ker::dev::iommu {
auto domain_stats() -> DomainStats { return {}; }  // BSP domain publication has not occurred.
auto bus_master_allowed(const pci::PCIDevice*) -> bool { return false; }
}  // namespace ker::dev::iommu
namespace ker::mod::mm {
auto boot_memory_map() -> const limine_memmap_response* {
    static limine_memmap_entry entry{pci_inventory_runtime::BAR_BASE, 0x80000000, LIMINE_MEMMAP_RESERVED};
    static limine_memmap_entry* entries[]{&entry};
    static limine_memmap_response response{0, 1, entries};
    return &response;
}
namespace addr {
auto get_hhdm_offset() -> uint64_t { return 0; }
auto get_virt_pointer(vaddr_t physical) -> vaddr_t* {
    auto& fixture = *pci_inventory_runtime::active;
    if (physical == pci_inventory_runtime::IOMMU_BASE) return static_cast<vaddr_t*>(fixture.registers);
    if (physical >= pci_inventory_runtime::ECAM_BASE && physical < pci_inventory_runtime::ECAM_BASE + 256 * 0x100000ULL) {
        auto* function = fixture.find(static_cast<uint16_t>((physical - pci_inventory_runtime::ECAM_BASE) >> 12U));
        assert(function != nullptr);
        return reinterpret_cast<vaddr_t*>(function->config.data() + (physical & 4095U));
    }
    for (auto& function : fixture.functions)
        if (physical >= function->bar_address && physical < uint64_t{function->bar_address} + function->bar_bytes)
            return reinterpret_cast<vaddr_t*>(function->bar.data() + physical - function->bar_address);
    std::abort();
}
}  // namespace addr
namespace virt {
auto kernel_mmio_page_flags() -> uint64_t { return 0x123; }
void map_range_to_kernel_page_table(Range range, uint64_t flags) {
    auto& fixture = *pci_inventory_runtime::active;
    assert(pci_inventory_runtime::lock_depth == 0 && flags == 0x123 && range.start < range.end);
    if (range.start == pci_inventory_runtime::IOMMU_BASE) {
        assert(range.end - range.start == 4096);
        ++fixture.calls.iommu_maps;
    } else if (range.start >= pci_inventory_runtime::ECAM_BASE && range.start < pci_inventory_runtime::ECAM_BASE + 256 * 0x100000ULL) {
        assert(range.end - range.start == 4096);
        ++fixture.calls.ecam_maps;
    } else {
        assert(range.start >= pci_inventory_runtime::BAR_BASE && range.start < pci_inventory_runtime::BAR_BASE + 0x10000000);
        ++fixture.calls.bar_maps;
    }
}
}  // namespace virt
}  // namespace ker::mod::mm

namespace pci_inventory_runtime {
TEST(PciInventoryRuntime, CompleteBoundariesShareRegistryResourceEcamAndOwnershipIdentity) {
    for (size_t count : {size_t{64}, size_t{65}, size_t{256}}) {
        isolated([count](Fixture& fixture) {
            fixture.mixed(count);
            enumerate(fixture);
            complete_ledgers(fixture, count);
            EXPECT_EQ(pci::pci_get_device(count - 1)->function, 1);
            EXPECT_EQ(pci::pci_get_device(count - 1)->parent_index, pci::NO_PARENT);
            auto const GROUPS = grouping(count);
            ASSERT_TRUE(GROUPS.valid());
            EXPECT_EQ(GROUPS.owners[count - 1], count - 1);
            first_and_last_resources(fixture);
            idempotent(fixture);
        });
    }
}

TEST(PciInventoryRuntime, OnePastCapacityKeepsOnlyThePrefixAndPublishesNoUsableOwnership) {
    isolated([](Fixture& fixture) {
        fixture.mixed(257);
        enumerate(fixture);
        retained_registry(fixture, 256);
        rejected_ledgers(fixture, pci::InventoryError::function_capacity);
        EXPECT_EQ(pci::pci_inventory_status().stored_count, 256U);
        idempotent(fixture);
    });
}

TEST(PciInventoryRuntime, DeepestBusChainFitsGuardedBootSizedStackAndRetainsAllAliasRequesters) {
    isolated([](Fixture& fixture) {
        fixture.deep();
        enumerate(fixture, true);
        complete_ledgers(fixture, 256);
        EXPECT_EQ(pci::pci_get_device(255)->bus, 255);
        EXPECT_EQ(pci::pci_get_device(255)->parent_index, 254);
        auto const GROUPS = grouping(256);
        ASSERT_TRUE(GROUPS.valid());
        EXPECT_EQ(GROUPS.count, 1U);
        EXPECT_EQ(GROUPS.requester_count, 511U);
        for (size_t i = 0; i < 256; ++i) EXPECT_EQ(GROUPS.owners[i], 0);
        EXPECT_EQ(std::count_if(GROUPS.requesters.begin(), GROUPS.requesters.begin() + GROUPS.requester_count,
                                [](auto const& requester) { return requester.alias; }),
                  255);
        first_and_last_resources(fixture);
        idempotent(fixture);
    });
}

TEST(PciInventoryRuntime, MissingFunctionZeroAndNonMultifunctionHeadersDoNotExposeSiblings) {
    isolated([](Fixture& fixture) {
        auto& root = fixture.add(0, 0);
        (void)fixture.add(0, 1, 1);  // No function0.
        auto& single = fixture.add(0, 2);
        (void)fixture.add(0, 2, 1);  // Function0 does not advertise multifunction.
        fixture.expect(root);
        fixture.expect(single);
        enumerate(fixture);
        complete_ledgers(fixture, 2);
        idempotent(fixture);
    });
}

TEST(PciInventoryRuntime, ExistingMultipleRootHeuristicAndUnconfiguredBridgeRemainBounded) {
    isolated([](Fixture& fixture) {
        auto& root = fixture.add(0, 0, 0, 0x80);
        auto& second_root = fixture.add(0, 0, 2);
        auto& endpoint = fixture.add(2, 1);
        auto& unconfigured = fixture.add(2, 2, 0, 1);
        fixture.bridge(unconfigured, 0, 0);
        fixture.expect(root);
        fixture.expect(second_root);
        fixture.expect(endpoint);
        fixture.expect(unconfigured);
        enumerate(fixture);
        complete_ledgers(fixture, 4);
        idempotent(fixture);
    });
}

TEST(PciInventoryRuntime, MalformedBridgeWindowsAndRevisitedBusesNeverBecomeQualifiedOwners) {
    for (unsigned scenario = 0; scenario < 5; ++scenario) {
        isolated([scenario](Fixture& fixture) {
            (void)fixture.add(0, 0);
            auto& bridge = fixture.add(0, 1, 0, 1);
            fixture.bridge(bridge, 1, 2);
            if (scenario == 0) Fixture::put(bridge.config, 0x18, 0x020101, 4);  // Wrong primary.
            if (scenario == 1) Fixture::put(bridge.config, 0x18, 0x020000, 4);  // Zero secondary but nonzero subordinate.
            if (scenario == 2) Fixture::put(bridge.config, 0x18, 0x010200, 4);  // Decreasing secondary/subordinate.
            if (scenario == 3) {
                auto& child = fixture.add(1, 1, 0, 1);
                fixture.bridge(child, 2, 3);  // Escapes retained parent's window.
            }
            if (scenario == 4) {
                auto& duplicate = fixture.add(0, 2, 0, 1);
                fixture.bridge(duplicate, 1, 2);
            }
            enumerate(fixture);
            rejected_ledgers(fixture, scenario == 4 ? pci::InventoryError::bus_revisit : pci::InventoryError::bridge_topology);
            idempotent(fixture);
        });
    }
}

TEST(PciInventoryRuntime, MaximumRequesterIdRemainsARealFunctionRatherThanAnIndexSentinel) {
    isolated([](Fixture& fixture) {
        auto& root = fixture.add(0, 0);
        auto& bridge = fixture.add(0, 1, 0, 1);
        fixture.bridge(bridge, 255, 255, true);
        fixture.expect(root);
        fixture.expect(bridge);
        for (uint8_t function = 0; function < 8; ++function) {
            auto& endpoint = fixture.add(255, 31, function, function == 0 ? 0x80 : 0);
            fixture.expect(endpoint, 1);
        }
        enumerate(fixture);
        complete_ledgers(fixture, 10);
        auto const* last = pci::pci_get_device(9);
        ASSERT_NE(last, nullptr);
        EXPECT_EQ(rid(last->bus, last->slot, last->function), UINT16_MAX);
        auto const* snapshot = pci::extended_config(last);
        ASSERT_NE(snapshot, nullptr);
        EXPECT_EQ(snapshot->physical, ECAM_BASE + 256 * 0x100000ULL - 4096);
        auto const GROUPS = grouping(10);
        ASSERT_TRUE(GROUPS.valid());
        auto const REQUESTER = std::find_if(GROUPS.requesters.begin(), GROUPS.requesters.begin() + GROUPS.requester_count,
                                            [](auto const& entry) { return entry.sid == UINT16_MAX; });
        ASSERT_NE(REQUESTER, GROUPS.requesters.begin() + GROUPS.requester_count);
        EXPECT_EQ(REQUESTER->device, 9);
        EXPECT_NE(REQUESTER->device, iommu::groups::NONE);
        EXPECT_EQ(REQUESTER->owner, 1);
        EXPECT_FALSE(REQUESTER->alias);
        first_and_last_resources(fixture);
        idempotent(fixture);
    });
}

TEST(PciInventoryRuntime, FirstFailureIsRetainedWhileLaterTraversalReasonsAccumulate) {
    isolated([](Fixture& fixture) {
        auto& root = fixture.add(0, 0);
        auto& malformed = fixture.add(0, 1, 0, 1);
        Fixture::put(malformed.config, 0x18, 0x010101, 4);  // First failure: primary bus1 on bus0.
        auto& route = fixture.add(0, 2, 0, 1);
        fixture.bridge(route, 2, 2);
        auto& duplicate = fixture.add(0, 3, 0, 1);
        fixture.bridge(duplicate, 2, 2);  // Later failure: already visited bus2.
        for (auto* value : {&root, &malformed, &route, &duplicate}) fixture.expect(*value);
        enumerate(fixture);
        retained_registry(fixture, 4);
        rejected_ledgers(fixture, pci::InventoryError::bridge_topology);
        uint32_t const REASONS =
            static_cast<uint32_t>(pci::InventoryError::bridge_topology) | static_cast<uint32_t>(pci::InventoryError::bus_revisit);
        EXPECT_EQ(pci::pci_inventory_status().reason_mask, REASONS);
        idempotent(fixture);
        EXPECT_EQ(pci::pci_inventory_status().first_error, pci::InventoryError::bridge_topology);
        EXPECT_EQ(pci::pci_inventory_status().reason_mask, REASONS);
    });
}
}  // namespace pci_inventory_runtime
