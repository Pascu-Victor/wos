#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <dev/dma/dma.hpp>
#include <dev/iommu/domains.hpp>
#include <dev/pci.hpp>
#include <dev/pci_driver_claim.hpp>
#include <dev/pci_port_io.hpp>
#include <latch>
#include <platform/asm/cpu.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <test_backend.hpp>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "pci_legacy_mmio_ports.inc"

namespace pci_runtime {
namespace pci = ker::dev::pci;
thread_local unsigned lock_depth = 0;
std::atomic<uint64_t> lock_acquisitions{0};
constexpr size_t DEVICES = 29;
constexpr uint64_t BASE = 0x80000000;
struct Function {
    std::array<uint8_t, 256> config{};
    alignas(4096) std::array<uint32_t, 2048> mmio{};
    std::array<bool, 6> sizing{};
    std::array<uint32_t, 6> sizing_masks{};
    bool fail_restore = false;
    bool domain_healthy = true;
    uint64_t probe_epoch = 0;
    bool transaction = false;
};
std::array<Function, DEVICES> functions{};
uint32_t address = 0;
uint64_t io = 0, probes = 0, maps = 0, status_dword_writes = 0;
std::latch* map_entered = nullptr;
std::latch* map_release = nullptr;
namespace dma = ker::dev::dma;
namespace iommu = ker::dev::iommu;
struct Backing {
    void* cpu;
    size_t bytes;
};
std::vector<Backing> backing;
uint64_t dma_allocations = 0, dma_frees = 0, dma_maps = 0, dma_unmaps = 0;
bool invalidation_succeeds = true;
pci::DriverClaim* close_on_dma_map = nullptr;
ker::mod::sched::ShutdownOwnership shutdown_ownership;

auto physical(size_t slot) -> uint32_t { return static_cast<uint32_t>(BASE + slot * 0x10000); }
auto load(const Function& f, size_t at, size_t bytes) -> uint32_t {
    uint32_t value = 0;
    for (size_t i = 0; i < bytes; ++i) value |= uint32_t{f.config[at + i]} << (8U * i);
    return value;
}
void store(Function& f, size_t at, uint32_t value, size_t bytes) {
    for (size_t i = 0; i < bytes; ++i) f.config[at + i] = static_cast<uint8_t>(value >> (8U * i));
}
auto selected() -> Function* {
    assert(lock_depth == 1 && (address & 0x80000000U) != 0);
    uint32_t const SLOT = (address >> 11U) & 31U;
    return (address & 0x00ff0700U) == 0 && SLOT < DEVICES ? &functions[SLOT] : nullptr;
}
void check_epoch(Function& f) {
    if (f.transaction) assert(f.probe_epoch == lock_acquisitions.load());
}
void write(uint16_t port, uint32_t value, size_t bytes) {
    assert(lock_depth == 1 && port >= pci::PCI_CONFIG_DATA && port + bytes <= pci::PCI_CONFIG_DATA + 4);
    ++io;
    auto* f = selected();
    if (f == nullptr) return;
    check_epoch(*f);
    size_t const OFFSET = (address & 0xfcU) + port - pci::PCI_CONFIG_DATA;
    if (bytes == 4 && OFFSET >= 0x10 && OFFSET < 0x28) {
        size_t const BAR = (OFFSET - 0x10) / 4;
        if (value == UINT32_MAX) {
            ++probes;
            f->transaction = true;
            f->probe_epoch = lock_acquisitions.load();
            f->sizing[BAR] = true;
            return;
        }
        if (f->sizing[BAR] && f->fail_restore) return;
        f->sizing[BAR] = false;
    }
    if (OFFSET == 4 && bytes == 4) {
        ++status_dword_writes;
        store(*f, 6, load(*f, 6, 2) & ~(value >> 16U), 2);
        store(*f, 4, value, 2);
    } else {
        store(*f, OFFSET, value, bytes);
    }
    if (OFFSET == 4 && (value & 6U) == 0 && f->fail_restore) f->transaction = false;
}
void setup() {
    for (size_t slot = 0; slot < DEVICES; ++slot) {
        auto& f = functions[slot];
        f.sizing_masks.fill(0xfffff000);
        store(f, 0, 0x10001234U + static_cast<uint32_t>(slot) * 0x10000, 4);
        store(f, 4, 2, 2);
        store(f, 6, 0x8010, 2);
        store(f, 0x10, physical(slot), 4);
        store(f, 0x14, physical(slot) + 4096, 4);
        store(f, 0x34, 0x40, 1);
        store(f, 0x40, 0x11, 2);
        store(f, 0x41, 0x60, 1);
        store(f, 0x42, 3, 2);  // Four entries in BAR0; separate PBA within it.
        store(f, 0x44, 0, 4);
        store(f, 0x48, 0x800, 4);
        store(f, 0x60, 0x05, 2);
        store(f, 0x62, 0x180, 2);  // Conventional 64-bit MSI with per-vector masks.
    }
    // Independent real-adapter inputs: true absence, flagged zero payloads,
    // and a positive assigned mask whose immutable prefetch bit changes.
    store(functions[25], 0x10, 0, 4);
    functions[25].sizing_masks[0] = 0;
    store(functions[26], 0x10, 8, 4);
    functions[26].sizing_masks[0] = 8;
    store(functions[27], 0x10, 4, 4);
    store(functions[27], 0x14, 0, 4);
    functions[27].sizing_masks[0] = 4;
    functions[27].sizing_masks[1] = 0;
    store(functions[28], 0x10, physical(28) | 8U, 4);
}
class PciRuntime : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        setup();
        ASSERT_EQ(pci::pci_enumerate_all(), DEVICES);
        ASSERT_TRUE(pci::pci_enumeration_complete());
    }
    static auto device(size_t slot) -> pci::PCIDevice* { return pci::pci_get_device(slot); }
};
}  // namespace pci_runtime

namespace ker::dev::pci::port_io {
auto inl(uint16_t port) -> uint32_t {
    using namespace pci_runtime;
    assert(port == PCI_CONFIG_DATA && lock_depth == 1);
    ++io;
    auto* f = selected();
    if (f == nullptr) return UINT32_MAX;
    check_epoch(*f);
    size_t const OFFSET = address & 0xfcU;
    if (OFFSET >= 0x10 && OFFSET < 0x28 && f->sizing[(OFFSET - 0x10) / 4]) return f->sizing_masks[(OFFSET - 0x10) / 4];
    uint32_t const VALUE = load(*f, OFFSET, 4);
    if (OFFSET == 4 && (VALUE & 6U) == 2 && !f->sizing[0] && !f->sizing[1]) f->transaction = false;
    return VALUE;
}
void outl(uint16_t port, uint32_t value) {
    assert(pci_runtime::lock_depth == 1);
    if (port == PCI_CONFIG_ADDRESS) {
        ++pci_runtime::io;
        pci_runtime::address = value;
    } else
        pci_runtime::write(port, value, 4);
}
void outw(uint16_t port, uint16_t value) { pci_runtime::write(port, value, 2); }
void outb(uint16_t port, uint8_t value) { pci_runtime::write(port, value, 1); }
}  // namespace ker::dev::pci::port_io

namespace ker::dev::pci {
void collect_extended_config() {}  // This test exercises conventional public PCI adapters.
}  // namespace ker::dev::pci
namespace ker::dev::iommu {
auto bus_master_allowed(const pci::PCIDevice* device) -> bool {
    return device != nullptr && device->slot < pci_runtime::DEVICES && pci_runtime::functions[device->slot].domain_healthy;
}
auto domain_info(const pci::PCIDevice* device) -> DomainInfo {
    return {.mode = bus_master_allowed(device) ? DomainMode::translated : DomainMode::unavailable};
}
auto map(const pci::PCIDevice* device, uint64_t physical, size_t bytes, vtd::Access) -> vtd::TableResult {
    using namespace pci_runtime;
    assert(lock_depth == 0 && physical % 4096 == 0 && bytes % 4096 == 0);
    assert(pci::driver_claim_status(device)->active_operations > 0);
    assert(shutdown_ownership.held());
    ++dma_maps;
    if (close_on_dma_map != nullptr) {
        close_on_dma_map->close_data_admission();
        EXPECT_FALSE(close_on_dma_map->operations_drained());
    }
    return {};
}
auto unmap(const pci::PCIDevice*, uint64_t address, uint64_t physical, size_t bytes, vtd::Access) -> bool {
    using namespace pci_runtime;
    assert(lock_depth == 0 && address == physical && bytes % 4096 == 0);
    assert(shutdown_ownership.held());
    ++dma_unmaps;
    return invalidation_succeeds;
}
}  // namespace ker::dev::iommu
namespace ker::mod::sched {
auto can_query_current_task() -> bool { return true; }
auto hold_kernel_shutdown() -> KernelShutdownHold { return KernelShutdownHold{&pci_runtime::shutdown_ownership}; }
}  // namespace ker::mod::sched
namespace ker::mod::cpu {
void cpuid(CpuidContext* c) { c->eax = c->function == static_cast<int>(0x80000000U) ? static_cast<int>(0x80000008U) : 48; }
}  // namespace ker::mod::cpu
namespace ker::mod::smt {
auto get_core_count() -> uint64_t { return 2; }
auto get_apic_id_for_cpu(uint64_t cpu) -> uint32_t { return static_cast<uint32_t>(cpu + 2); }
}  // namespace ker::mod::smt
namespace ker::mod::mm {
auto boot_memory_map() -> const limine_memmap_response* {
    static limine_memmap_entry entry{pci_runtime::BASE, 0x200000, LIMINE_MEMMAP_RESERVED};
    static limine_memmap_entry* entries[]{&entry};
    static limine_memmap_response response{0, 1, entries};
    return &response;
}
namespace addr {
auto get_hhdm_offset() -> uint64_t { return 0; }
auto get_virt_pointer(vaddr_t physical) -> vaddr_t* {
    size_t const SLOT = (physical - pci_runtime::BASE) / 0x10000;
    assert(SLOT < pci_runtime::DEVICES);
    return reinterpret_cast<vaddr_t*>(reinterpret_cast<uint8_t*>(pci_runtime::functions[SLOT].mmio.data()) +
                                      (physical - pci_runtime::physical(SLOT)));
}
}  // namespace addr
namespace virt {
auto kernel_mmio_page_flags() -> uint64_t { return 0x123; }
void map_range_to_kernel_page_table(Range range, uint64_t flags) {
    assert(pci_runtime::lock_depth == 0 && range.start < range.end && flags == 0x123);
    ++pci_runtime::maps;
    if (pci_runtime::map_entered) {
        pci_runtime::map_entered->count_down();
        pci_runtime::map_release->wait();
    }
}
}  // namespace virt
namespace phys {
auto can_wait_for_reclaim() -> bool { return pci_runtime::lock_depth == 0; }
auto page_alloc_may_fail(PhysicalPageOwner owner, uint64_t bytes, std::string_view name) -> void* {
    using namespace pci_runtime;
    assert(lock_depth == 0 && owner == PhysicalPageOwner::DMA_BUFFER && name == "dma_buffer");
    assert(shutdown_ownership.held());
    auto* result = std::aligned_alloc(4096, bytes);
    if (result != nullptr) {
        std::memset(result, 0, bytes);
        backing.push_back({result, bytes});
        ++dma_allocations;
    }
    return result;
}
void page_free(void* pointer) {
    using namespace pci_runtime;
    assert(lock_depth == 0 && shutdown_ownership.held());
    auto const IT = std::find_if(backing.begin(), backing.end(), [&](auto const& item) { return item.cpu == pointer; });
    assert(IT != backing.end());
    std::free(pointer);
    backing.erase(IT);
    ++dma_frees;
}
}  // namespace phys
}  // namespace ker::mod::mm

namespace pci_runtime {
TEST_F(PciRuntime, InvalidBdfsCannotAliasRealFunctionsOrReachPortIo) {
    uint64_t const BEFORE = io;
    auto const LOCKS = lock_acquisitions.load();
    for (auto bdf : {std::array<uint8_t, 2>{0, 8}, {32, 0}, {255, 255}}) {
        EXPECT_EQ(pci::pci_config_read32(0, bdf[0], bdf[1], 0), UINT32_MAX);
        EXPECT_EQ(pci::pci_config_read16(0, bdf[0], bdf[1], 0), UINT16_MAX);
        EXPECT_EQ(pci::pci_config_read8(0, bdf[0], bdf[1], 0), UINT8_MAX);
        pci::pci_config_write32(0, bdf[0], bdf[1], 0x10, 0);
        pci::pci_config_write16(0, bdf[0], bdf[1], 0x11, 0);
        pci::pci_config_write8(0, bdf[0], bdf[1], 0x13, 0);
    }
    EXPECT_EQ(io, BEFORE);
    EXPECT_EQ(lock_acquisitions.load(), LOCKS);
    EXPECT_EQ(load(functions[1], 0x10, 4), physical(1));
}

TEST_F(PciRuntime, CopiedDeviceIdentitiesRejectWithoutIoOrMapping) {
    auto copied = *device(0);
    uint64_t const BEFORE = io, MAPS = maps;
    EXPECT_FALSE(pci::pci_set_bus_master(&copied, false));
    EXPECT_EQ(pci::pci_prepare_msix(&copied), -1);
    EXPECT_EQ(pci::pci_enable_msix(&copied, 48), -1);
    EXPECT_EQ(pci::pci_configure_msix_entry(&copied, 0, 48), -1);
    EXPECT_EQ(pci::pci_get_bar_addr(&copied, 0), 0U);
    EXPECT_FALSE(pci::pci_get_bar_size_checked(&copied, 0));
    EXPECT_EQ(pci::pci_map_bar(&copied, 0), nullptr);
    EXPECT_EQ(io, BEFORE);
    EXPECT_EQ(maps, MAPS);
}

TEST_F(PciRuntime, EntireSizingTransactionUsesOneLockAndPreservesStatusW1c) {
    auto const LOCKS = lock_acquisitions.load();
    uint64_t const PROBES = probes, STATUS_WRITES = status_dword_writes;
    EXPECT_EQ(pci::pci_get_bar_size_checked(device(1), 0), 4096U);
    EXPECT_EQ(lock_acquisitions.load(), LOCKS + 1);
    EXPECT_EQ(probes, PROBES + 1);
    EXPECT_EQ(status_dword_writes, STATUS_WRITES);
    EXPECT_EQ(load(functions[1], 4, 2), 2U);
    EXPECT_EQ(load(functions[1], 6, 2), 0x8010U);
    EXPECT_FALSE(functions[1].transaction);
}

TEST_F(PciRuntime, FirstProbeWithBusMasteringOnRefusesWithoutSizingAndMayLaterPrepare) {
    ASSERT_TRUE(pci::pci_set_bus_master(device(2), true));
    uint64_t const PROBES = probes, MAPS = maps;
    EXPECT_FALSE(pci::pci_get_bar_size_checked(device(2), 0));
    EXPECT_EQ(pci::pci_map_bar(device(2), 0), nullptr);
    EXPECT_EQ(probes, PROBES);
    EXPECT_EQ(maps, MAPS);
    ASSERT_TRUE(pci::pci_set_bus_master(device(2), false));
    EXPECT_EQ(pci::pci_get_bar_size_checked(device(2), 0), 4096U);
}

TEST_F(PciRuntime, EntirelyZeroAbsentBarIsCachedWithoutPoisoningTheFunction) {
    auto* const DEV = device(25);
    auto const BEFORE = functions[25].config;
    auto const PROBES = probes, MAPS = maps, STATUS_WRITES = status_dword_writes;
    ASSERT_EQ(pci::pci_get_bar_size_checked(DEV, 0), std::optional<uint64_t>{0});
    EXPECT_EQ(functions[25].config, BEFORE);
    EXPECT_EQ(probes, PROBES + 1);
    EXPECT_EQ(status_dword_writes, STATUS_WRITES);
    auto const IO = io;
    EXPECT_EQ(pci::pci_get_bar_size_checked(DEV, 0), std::optional<uint64_t>{0});
    EXPECT_EQ(pci::pci_map_bar(DEV, 0), nullptr);
    EXPECT_EQ(io, IO);
    EXPECT_EQ(probes, PROBES + 1);
    EXPECT_EQ(maps, MAPS);
    ASSERT_TRUE(pci::pci_set_bus_master(DEV, true));
    EXPECT_EQ(load(functions[25], 4, 2) & pci::PCI_COMMAND_BUS_MASTER, pci::PCI_COMMAND_BUS_MASTER);
    EXPECT_TRUE(pci::pci_set_bus_master(DEV, false));
}

TEST_F(PciRuntime, FlaggedZeroPayloadOrChangedAttributesPoisonWithoutMappingOrRetry) {
    for (size_t slot : {26U, 27U, 28U}) {
        auto* const DEV = device(slot);
        auto const BEFORE = functions[slot].config;
        auto const PROBES = probes, MAPS = maps, STATUS_WRITES = status_dword_writes;
        EXPECT_FALSE(pci::pci_get_bar_size_checked(DEV, 0)) << slot;
        EXPECT_EQ(functions[slot].config, BEFORE);
        EXPECT_EQ(probes, PROBES + (slot == 27 ? 2U : 1U));
        EXPECT_EQ(status_dword_writes, STATUS_WRITES);
        EXPECT_FALSE(functions[slot].transaction);
        auto const IO = io, RETAINED_PROBES = probes;
        EXPECT_FALSE(pci::pci_get_bar_size_checked(DEV, 0));
        EXPECT_EQ(pci::pci_map_bar(DEV, 0), nullptr);
        EXPECT_FALSE(pci::pci_set_bus_master(DEV, true));
        EXPECT_EQ(io, IO);
        EXPECT_EQ(probes, RETAINED_PROBES);
        EXPECT_EQ(maps, MAPS);
        EXPECT_EQ(load(functions[slot], 4, 2) & pci::PCI_COMMAND_BUS_MASTER, 0U);
    }
}

TEST_F(PciRuntime, MappingClaimsBlockDmaAndCompetitorsWhileMappingOutsideConfigLock) {
    std::latch entered(1), release(1);
    map_entered = &entered;
    map_release = &release;
    void* result = nullptr;
    std::thread producer([&] { result = pci::pci_map_bar(device(3), 0); });
    entered.wait();
    uint64_t const PROBES = probes, MAPS = maps;
    EXPECT_EQ(pci::pci_map_bar(device(3), 0), nullptr);
    EXPECT_FALSE(pci::pci_set_bus_master(device(3), true));
    EXPECT_EQ(probes, PROBES);
    EXPECT_EQ(maps, MAPS);
    release.count_down();
    producer.join();
    map_entered = map_release = nullptr;
    ASSERT_EQ(result, functions[3].mmio.data());
    ASSERT_TRUE(pci::pci_set_bus_master(device(3), true));
    uint64_t const BEFORE = io;
    for (size_t i = 0; i < 32; ++i) {
        EXPECT_EQ(pci::pci_map_bar(device(3), 0), result);
        EXPECT_EQ(pci::pci_get_bar_size_checked(device(3), 0), 4096U);
    }
    EXPECT_EQ(io, BEFORE);
    EXPECT_EQ(probes, PROBES);
    EXPECT_EQ(maps, MAPS);
}

TEST_F(PciRuntime, PreparedMsixSteeringUsesRetainedTableWithoutConfigProbeOrMap) {
    uint64_t const BEFORE = io, PROBES = probes, MAPS = maps;
    EXPECT_EQ(pci::pci_enable_msix(device(4), 48), -1);
    EXPECT_EQ(pci::pci_configure_msix_entry(device(4), 0, 48), -1);
    EXPECT_EQ(io, BEFORE);
    EXPECT_EQ(probes, PROBES);
    EXPECT_EQ(maps, MAPS);
    ASSERT_EQ(pci::pci_prepare_msix(device(4)), 0);
    for (size_t entry = 0; entry < 4; ++entry) EXPECT_EQ(functions[4].mmio[entry * 4 + 3] & 1U, 1U);
    ASSERT_TRUE(pci::pci_set_bus_master(device(4), true));
    ASSERT_EQ(pci::pci_enable_msix(device(4), 48), 0);
    uint64_t const RETAINED_IO = io, RETAINED_PROBES = probes, RETAINED_MAPS = maps;
    EXPECT_EQ(pci::pci_configure_msix_entry(device(4), 2, 52, 1), 0);
    EXPECT_EQ(functions[4].mmio[8], 0xfee03000U);
    EXPECT_EQ(functions[4].mmio[9], 0U);
    EXPECT_EQ(functions[4].mmio[10], 52U);
    EXPECT_EQ(functions[4].mmio[11] & 1U, 0U);
    EXPECT_EQ(functions[4].mmio[15] & 1U, 1U);
    EXPECT_EQ(io, RETAINED_IO);
    EXPECT_EQ(probes, RETAINED_PROBES);
    EXPECT_EQ(maps, RETAINED_MAPS);
}

TEST_F(PciRuntime, FailedRestorePermanentlyBlocksDmaAndFurtherProbing) {
    functions[5].fail_restore = true;
    EXPECT_FALSE(pci::pci_get_bar_size_checked(device(5), 0));
    EXPECT_EQ(load(functions[5], 4, 2) & 6U, 0U);
    uint64_t const BEFORE = io, PROBES = probes;
    EXPECT_FALSE(pci::pci_set_bus_master(device(5), true));
    EXPECT_FALSE(pci::pci_get_bar_size_checked(device(5), 0));
    EXPECT_EQ(pci::pci_map_bar(device(5), 0), nullptr);
    EXPECT_EQ(io, BEFORE);
    EXPECT_EQ(probes, PROBES);
}

TEST_F(PciRuntime, LaterResourcePoisonMasksPreviouslyPreparedMsix) {
    ASSERT_EQ(pci::pci_prepare_msix(device(6)), 0);
    ASSERT_EQ(pci::pci_enable_msix(device(6), 48), 0);
    EXPECT_EQ(load(functions[6], 0x42, 2) & 0x4000U, 0U);
    functions[6].fail_restore = true;
    EXPECT_FALSE(pci::pci_get_bar_size_checked(device(6), 1));
    EXPECT_NE(load(functions[6], 0x42, 2) & 0x4000U, 0U);
    EXPECT_FALSE(pci::pci_set_bus_master(device(6), true));
}

TEST_F(PciRuntime, RetainedBarsRejectByteWordAndDwordRelocation) {
    ASSERT_EQ(pci::pci_get_bar_size_checked(device(7), 0), 4096U);
    EXPECT_DEATH(pci::pci_config_write8(0, 7, 0, 0x13, 0), "retained BAR");
    EXPECT_DEATH(pci::pci_config_write16(0, 7, 0, 0x13, 0), "retained BAR");
    EXPECT_DEATH(pci::pci_config_write32(0, 7, 0, 0x13, 0), "retained BAR");
    EXPECT_EQ(load(functions[7], 0x10, 4), physical(7));
}

TEST_F(PciRuntime, MaskingUsesPreparedOwnershipAndUnmaskingRequiresActivationAndHealthyDomain) {
    ASSERT_EQ(pci::pci_prepare_msix(device(8)), 0);
    EXPECT_EQ(load(functions[8], 0x42, 2) & 0xc000U, 0xc000U);
    uint64_t before = io;
    EXPECT_EQ(pci::pci_mask_msix_entry(device(8), 0, false), -1);
    EXPECT_EQ(io, before);
    ASSERT_EQ(pci::pci_enable_msix(device(8), 48), 0);
    before = io;
    uint64_t const PROBES = probes, MAPS = maps;
    EXPECT_EQ(pci::pci_mask_msix_entry(device(8), 0, true), 0);
    EXPECT_EQ(functions[8].mmio[3] & 1U, 1U);
    EXPECT_EQ(pci::pci_mask_msix_entry(device(8), 0, false), 0);
    EXPECT_EQ(functions[8].mmio[3] & 1U, 0U);
    auto const TABLE = functions[8].mmio;
    for (uint32_t entry : {4U, UINT32_MAX}) {
        EXPECT_EQ(pci::pci_mask_msix_entry(device(8), entry, true), -1);
        EXPECT_EQ(pci::pci_configure_msix_entry(device(8), entry, 48), -1);
    }
    for (uint8_t vector : {0, 31, 255}) EXPECT_EQ(pci::pci_configure_msix_entry(device(8), 0, vector), -1);
    for (uint64_t cpu : std::array<uint64_t, 2>{2, UINT64_MAX}) EXPECT_EQ(pci::pci_configure_msix_entry(device(8), 0, 48, cpu), -1);
    EXPECT_EQ(functions[8].mmio, TABLE);
    functions[8].domain_healthy = false;
    EXPECT_EQ(pci::pci_mask_msix_entry(device(8), 0, false), -1);
    EXPECT_EQ(pci::pci_configure_msix_entry(device(8), 0, 48), -1);
    EXPECT_EQ(pci::pci_mask_msix_entry(device(8), 0, true), 0);
    EXPECT_EQ(functions[8].mmio[3] & 1U, 1U);
    EXPECT_EQ(io, before);
    EXPECT_EQ(probes, PROBES);
    EXPECT_EQ(maps, MAPS);
}
}  // namespace pci_runtime

namespace pci_runtime {
static_assert(!std::is_copy_constructible_v<pci::DriverClaim> && !std::is_copy_assignable_v<pci::DriverClaim>);
static_assert(!std::is_copy_constructible_v<pci::DriverOperation> && !std::is_copy_assignable_v<pci::DriverOperation>);

TEST_F(PciRuntime, ExclusiveClaimsHaveNonreusedEpochsAndMoveOnlyRetainedOwnership) {
    auto first = pci::try_claim_driver(device(9), pci::DriverKind::ahci);
    auto second = pci::try_claim_driver(device(10), pci::DriverKind::xhci);
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    auto const FIRST_EPOCH = first->epoch(), SECOND_EPOCH = second->epoch();
    EXPECT_NE(FIRST_EPOCH, 0U);
    EXPECT_GT(SECOND_EPOCH, FIRST_EPOCH);
    EXPECT_EQ(first->device(), device(9));
    EXPECT_FALSE(pci::try_claim_driver(device(9), pci::DriverKind::e1000e));
    EXPECT_FALSE(first->operations_drained());  // An open gate is not retirement.
    auto op = pci::try_dma_operation(device(9));
    ASSERT_TRUE(op);
    auto moved_op = std::move(op);
    EXPECT_FALSE(op);
    EXPECT_EQ(pci::driver_claim_status(device(9))->active_operations, 1U);
    first->close_data_admission();
    EXPECT_FALSE(pci::driver_dma_admission_allowed(device(9)));
    EXPECT_FALSE(first->operations_drained());
    EXPECT_FALSE(pci::try_dma_operation(device(9)));
    EXPECT_FALSE(pci::begin_dma_allocation(device(9)));
    moved_op = {};
    EXPECT_TRUE(first->operations_drained());
    *first = std::move(*second);  // Destination's prior owner is retained.
    EXPECT_FALSE(*second);
    EXPECT_EQ(first->epoch(), SECOND_EPOCH);
    EXPECT_EQ(pci::driver_claim_status(device(9))->phase, pci::DriverPhase::retained);
    EXPECT_FALSE(pci::try_claim_driver(device(9), pci::DriverKind::ahci));
    first.reset();
    auto const STATUS = pci::driver_claim_status(device(10));
    ASSERT_TRUE(STATUS);
    EXPECT_EQ(STATUS->phase, pci::DriverPhase::retained);
    EXPECT_EQ(STATUS->driver, pci::DriverKind::xhci);
    EXPECT_EQ(STATUS->epoch, SECOND_EPOCH);
    EXPECT_FALSE(pci::try_claim_driver(device(10), pci::DriverKind::xhci));
}

TEST_F(PciRuntime, AnonymousDiagnosticAllocationClaimIsPinnedAndNeverTransfersToADriver) {
    ASSERT_EQ(pci::driver_claim_status(device(11))->phase, pci::DriverPhase::unclaimed);
    EXPECT_FALSE(pci::try_dma_operation(device(11)));
    auto const LOCKS = lock_acquisitions.load();
    auto op = pci::begin_dma_allocation(device(11));
    ASSERT_TRUE(op);
    EXPECT_GT(lock_acquisitions.load(), LOCKS);
    auto const STATUS = pci::driver_claim_status(device(11));
    ASSERT_TRUE(STATUS);
    EXPECT_EQ(STATUS->phase, pci::DriverPhase::open);
    EXPECT_EQ(STATUS->driver, pci::DriverKind::diagnostic);
    EXPECT_EQ(STATUS->active_operations, 1U);
    EXPECT_NE(STATUS->epoch, 0U);
    auto const ATOMIC_LOCKS = lock_acquisitions.load();
    auto second = pci::try_dma_operation(device(11));
    ASSERT_TRUE(second);
    EXPECT_EQ(pci::driver_claim_status(device(11))->active_operations, 2U);
    second = {};
    op = {};
    EXPECT_EQ(pci::driver_claim_status(device(11))->active_operations, 0U);
    EXPECT_EQ(lock_acquisitions.load(), ATOMIC_LOCKS);
    EXPECT_FALSE(pci::try_claim_driver(device(11), pci::DriverKind::virtio_net));
    EXPECT_TRUE(pci::driver_dma_admission_allowed(device(11)));
}

TEST_F(PciRuntime, ClosureBlocksActivationButAllowsExistingBmeCleanupAndPreparedIrqMasking) {
    auto claim = pci::try_claim_driver(device(12), pci::DriverKind::e1000e);
    ASSERT_TRUE(claim);
    ASSERT_EQ(pci::pci_prepare_msix(device(12)), 0);
    ASSERT_TRUE(pci::pci_set_bus_master(device(12), true));
    ASSERT_EQ(pci::pci_enable_msix(device(12), 48), 0);
    claim->close_data_admission();
    ASSERT_TRUE(claim->operations_drained());
    auto const OLD_COMMAND = static_cast<uint16_t>(load(functions[12], 4, 2));
    pci::pci_config_write8(0, 12, 0, 4, static_cast<uint8_t>(OLD_COMMAND));
    pci::pci_config_write16(0, 12, 0, 4, OLD_COMMAND | pci::PCI_COMMAND_INT_DISABLE);
    pci::pci_config_write32(0, 12, 0, 4, OLD_COMMAND | pci::PCI_COMMAND_INT_DISABLE);
    EXPECT_NE(load(functions[12], 4, 2) & pci::PCI_COMMAND_BUS_MASTER, 0U);
    EXPECT_FALSE(pci::pci_set_bus_master(device(12), true));
    auto const BEFORE = io;
    auto const TABLE = functions[12].mmio;
    EXPECT_EQ(pci::pci_enable_msi(device(12), 48), -1);
    EXPECT_EQ(pci::pci_enable_msix(device(12), 48), -1);
    EXPECT_EQ(pci::pci_configure_msix_entry(device(12), 0, 52), -1);
    EXPECT_EQ(pci::pci_mask_msix_entry(device(12), 0, false), -1);
    EXPECT_EQ(functions[12].mmio, TABLE);
    EXPECT_EQ(io, BEFORE);
    EXPECT_EQ(pci::pci_mask_msix_entry(device(12), 0, true), 0);
    EXPECT_NE(functions[12].mmio[3] & 1U, 0U);
    EXPECT_TRUE(pci::pci_set_bus_master(device(12), false));
    EXPECT_EQ(load(functions[12], 4, 2) & pci::PCI_COMMAND_BUS_MASTER, 0U);
    pci::pci_config_write8(0, 12, 0, 5, 0);
    EXPECT_NE(load(functions[12], 4, 2) & pci::PCI_COMMAND_INT_DISABLE, 0U);
    pci::pci_config_write16(0, 12, 0, 5, 2);  // Effective word offset4.
    EXPECT_NE(load(functions[12], 4, 2) & pci::PCI_COMMAND_INT_DISABLE, 0U);
    pci::pci_config_write32(0, 12, 0, 7, 2);  // Effective dword offset4.
    EXPECT_NE(load(functions[12], 4, 2) & pci::PCI_COMMAND_INT_DISABLE, 0U);
    pci::pci_config_write8(0, 12, 0, 4, 6);
    EXPECT_EQ(load(functions[12], 4, 2) & pci::PCI_COMMAND_BUS_MASTER, 0U);
    pci::pci_config_write16(0, 12, 0, 4, 6);
    EXPECT_EQ(load(functions[12], 4, 2) & pci::PCI_COMMAND_BUS_MASTER, 0U);
    pci::pci_config_write32(0, 12, 0, 4, 6);
    EXPECT_EQ(load(functions[12], 4, 2) & pci::PCI_COMMAND_BUS_MASTER, 0U);
    EXPECT_EQ(load(functions[12], 4, 2) & pci::PCI_COMMAND_BUS_MASTER, 0U);
}

TEST_F(PciRuntime, ClosedUnpreparedFunctionRefusesColdMappingSizingAndIrqPreparationWithoutIo) {
    auto claim = pci::try_claim_driver(device(13), pci::DriverKind::ivshmem);
    ASSERT_TRUE(claim);
    claim->close_data_admission();
    auto const BEFORE = io, PROBES = probes, MAPS = maps;
    EXPECT_EQ(pci::pci_map_bar(device(13), 0), nullptr);
    EXPECT_FALSE(pci::pci_get_bar_size_checked(device(13), 0));
    EXPECT_EQ(pci::pci_prepare_msix(device(13)), -1);
    EXPECT_EQ(pci::pci_enable_msi(device(13), 48), -1);
    EXPECT_EQ(pci::pci_enable_msix(device(13), 48), -1);
    EXPECT_EQ(io, BEFORE);
    EXPECT_EQ(probes, PROBES);
    EXPECT_EQ(maps, MAPS);
}

TEST_F(PciRuntime, ConcurrentDriversCannotBothWinOneFunction) {
    std::latch start(1);
    std::array<std::optional<pci::DriverClaim>, 2> claims;
    std::thread first([&] {
        start.wait();
        claims[0] = pci::try_claim_driver(device(14), pci::DriverKind::ahci);
    });
    std::thread second([&] {
        start.wait();
        claims[1] = pci::try_claim_driver(device(14), pci::DriverKind::e1000e);
    });
    start.count_down();
    first.join();
    second.join();
    EXPECT_NE(claims[0].has_value(), claims[1].has_value());
    EXPECT_NE(pci::driver_claim_status(device(14))->epoch, 0U);
}

TEST_F(PciRuntime, ClosureDuringActualMmioPreparationRetainsAliasAndDrainsBeforeRefusal) {
    auto claim = pci::try_claim_driver(device(15), pci::DriverKind::xhci);
    ASSERT_TRUE(claim);
    std::latch entered(1), resume(1);
    map_entered = &entered;
    map_release = &resume;
    void* result = reinterpret_cast<void*>(1);
    std::thread mapper([&] { result = pci::pci_map_bar(device(15), 0); });
    entered.wait();
    EXPECT_GT(pci::driver_claim_status(device(15))->active_operations, 0U);
    claim->close_data_admission();
    EXPECT_FALSE(claim->operations_drained());
    EXPECT_FALSE(pci::try_dma_operation(device(15)));
    resume.count_down();
    mapper.join();
    map_entered = map_release = nullptr;
    EXPECT_EQ(result, nullptr);
    EXPECT_TRUE(claim->operations_drained());
    EXPECT_EQ(pci::pci_map_bar(device(15), 0), nullptr);
    EXPECT_FALSE(pci::pci_set_bus_master(device(15), true));
}

TEST_F(PciRuntime, ActualDmaBackendHonorsClosedClaimsButCompletesAndReleasesOldStorage) {
    auto claim = pci::try_claim_driver(device(16), pci::DriverKind::e1000e);
    ASSERT_TRUE(claim);
    auto const ALLOCS = dma_allocations, FREES = dma_frees, MAPS = dma_maps, UNMAPS = dma_unmaps;
    auto const REGION = dma::allocate_coherent(device(16), 4096);
    ASSERT_EQ(REGION.error, dma::Error::none);
    ASSERT_NE(REGION.allocation.cpu, nullptr);
    EXPECT_EQ(pci::driver_claim_status(device(16))->active_operations, 0U);
    auto const SUBMISSION = dma::prepare(REGION.allocation.handle);
    ASSERT_EQ(SUBMISSION.error, dma::Error::none);
    claim->close_data_admission();
    EXPECT_TRUE(claim->operations_drained());
    EXPECT_EQ(dma::allocate_coherent(device(16), 4096).error, dma::Error::unavailable_device);
    EXPECT_EQ(dma::complete(SUBMISSION.submission, 0, dma::Retirement::device_quiesced), dma::Error::none);
    EXPECT_EQ(dma::prepare(REGION.allocation.handle).error, dma::Error::unavailable_device);
    EXPECT_EQ(dma::release(REGION.allocation.handle), dma::Error::none);
    EXPECT_EQ(dma::stats(device(16)).regions, 0U);
    EXPECT_EQ(dma_allocations, ALLOCS + 1);
    EXPECT_EQ(dma_frees, FREES + 1);
    EXPECT_EQ(dma_maps, MAPS + 1);
    EXPECT_EQ(dma_unmaps, UNMAPS + 1);
}

TEST_F(PciRuntime, ActualMapClosureInvalidatesBeforeFreeAndFailedInvalidationQuarantines) {
    for (size_t slot : {17U, 18U}) {
        auto claim = pci::try_claim_driver(device(slot), pci::DriverKind::ahci);
        ASSERT_TRUE(claim);
        auto const FREES = dma_frees, UNMAPS = dma_unmaps;
        invalidation_succeeds = slot == 17;
        close_on_dma_map = &*claim;
        auto const RESULT = dma::allocate_coherent(device(slot), 4096);
        close_on_dma_map = nullptr;
        EXPECT_TRUE(claim->operations_drained());
        EXPECT_EQ(dma_unmaps, UNMAPS + 1);
        if (invalidation_succeeds) {
            EXPECT_EQ(RESULT.error, dma::Error::unavailable_device);
            EXPECT_EQ(dma_frees, FREES + 1);
            EXPECT_EQ(dma::stats(device(slot)).regions, 0U);
        } else {
            EXPECT_EQ(RESULT.error, dma::Error::quarantined);
            EXPECT_EQ(RESULT.allocation.cpu, nullptr);
            EXPECT_EQ(dma_frees, FREES);
            EXPECT_EQ(dma::stats(device(slot)).quarantined, 1U);
            EXPECT_EQ(dma::release(RESULT.allocation.handle), dma::Error::quarantined);
        }
    }
    invalidation_succeeds = true;
}

TEST_F(PciRuntime, ClosedStreamingSubmissionHasNoCopyOrPublicationAndRemainsReleasable) {
    auto claim = pci::try_claim_driver(device(19), pci::DriverKind::virtio_net);
    ASSERT_TRUE(claim);
    auto const REGION = dma::allocate_streaming(device(19), 4096, dma::Direction::to_device);
    ASSERT_EQ(REGION.error, dma::Error::none);
    std::array<std::byte, 64> source{};
    source.fill(std::byte{0xa7});
    ASSERT_EQ(dma::bind_streaming(REGION.allocation.handle, source, dma::Direction::to_device), dma::Error::none);
    claim->close_data_admission();
    EXPECT_EQ(dma::prepare(REGION.allocation.handle).error, dma::Error::unavailable_device);
    EXPECT_EQ(REGION.allocation.cpu[0], std::byte{});
    EXPECT_EQ(dma::stats(device(19)).device_owned, 0U);
    EXPECT_TRUE(claim->operations_drained());
    EXPECT_EQ(dma::release(REGION.allocation.handle), dma::Error::none);
}

TEST_F(PciRuntime, ForeignCopiedInteriorAndNullPointersCannotMintClaimsOrDmaOperations) {
    auto copied = *device(20);
    for (auto* value :
         {static_cast<pci::PCIDevice*>(nullptr), &copied, reinterpret_cast<pci::PCIDevice*>(reinterpret_cast<uintptr_t>(device(20)) + 1),
          reinterpret_cast<pci::PCIDevice*>(reinterpret_cast<uintptr_t>(device(0)) - sizeof(pci::PCIDevice)),
          reinterpret_cast<pci::PCIDevice*>(reinterpret_cast<uintptr_t>(device(0)) + DEVICES * sizeof(pci::PCIDevice))}) {
        auto const LOCKS = lock_acquisitions.load();
        auto const ALLOCS = dma_allocations, MAPS = dma_maps, BEFORE = io;
        EXPECT_FALSE(pci::try_claim_driver(value, pci::DriverKind::diagnostic));
        EXPECT_FALSE(pci::driver_claim_status(value));
        EXPECT_FALSE(pci::driver_dma_admission_allowed(value));
        EXPECT_FALSE(pci::begin_dma_allocation(value));
        EXPECT_FALSE(pci::try_dma_operation(value));
        EXPECT_EQ(dma::allocate_coherent(value, 4096).error, dma::Error::unavailable_device);
        EXPECT_EQ(lock_acquisitions.load(), LOCKS);
        EXPECT_EQ(dma_allocations, ALLOCS);
        EXPECT_EQ(dma_maps, MAPS);
        EXPECT_EQ(io, BEFORE);
    }
    EXPECT_FALSE(pci::try_claim_driver(device(20), static_cast<pci::DriverKind>(255)));
    EXPECT_EQ(pci::driver_claim_status(device(20))->phase, pci::DriverPhase::unclaimed);
}

TEST_F(PciRuntime, ClosedRawMsiAndMsixControlsCannotReactivateButCanDisableAndMask) {
    auto claim = pci::try_claim_driver(device(21), pci::DriverKind::e1000e);
    ASSERT_TRUE(claim);
    ASSERT_EQ(pci::pci_prepare_msix(device(21)), 0);
    ASSERT_EQ(pci::pci_enable_msix(device(21), 48), 0);
    ASSERT_EQ(pci::pci_enable_msi(device(21), 49), 0);
    claim->close_data_admission();
    // Preserving enabled MSI and MSI-X while masking is permitted cleanup.
    pci::pci_config_write8(0, 21, 0, 0x43, 0xc0);
    pci::pci_config_write16(0, 21, 0, 0x62, 0x181);
    pci::pci_config_write8(0, 21, 0, 0x43, 0x80);
    EXPECT_EQ(load(functions[21], 0x42, 2), 0xc003U);
    pci::pci_config_write16(0, 21, 0, 0x42, 0x8003);
    EXPECT_EQ(load(functions[21], 0x42, 2), 0xc003U);
    pci::pci_config_write32(0, 21, 0, 0x40, 0x80036011);
    EXPECT_EQ(load(functions[21], 0x42, 2), 0xc003U);
    pci::pci_config_write16(0, 21, 0, 0x42, 0x4003);
    pci::pci_config_write8(0, 21, 0, 0x62, 0x80);
    pci::pci_config_write8(0, 21, 0, 0x62, 0x81);
    EXPECT_EQ(load(functions[21], 0x62, 2), 0x180U);
    pci::pci_config_write16(0, 21, 0, 0x62, 0x181);
    EXPECT_EQ(load(functions[21], 0x62, 2), 0x180U);
    pci::pci_config_write32(0, 21, 0, 0x60, 0x01810005);
    EXPECT_EQ(load(functions[21], 0x62, 2), 0x180U);
    EXPECT_EQ(load(functions[21], 0x42, 2), 0x4003U);
    EXPECT_EQ(load(functions[21], 0x62, 2), 0x180U);
}

TEST_F(PciRuntime, ClosedMsiPerVectorMasksAndDestinationsRemainFencedForBothAddressWidths) {
    for (size_t slot : {23U, 24U}) {
        bool const WIDE = slot == 24;
        auto& function = functions[slot];
        store(function, 0x62, WIDE ? 0x180 : 0x100, 2);
        auto claim = pci::try_claim_driver(device(slot), pci::DriverKind::e1000e);
        ASSERT_TRUE(claim);
        ASSERT_EQ(pci::pci_enable_msi(device(slot), 48), 0);
        auto const DATA = static_cast<uint8_t>(WIDE ? 0x6c : 0x68);
        auto const MASK = static_cast<uint8_t>(WIDE ? 0x70 : 0x6c);
        claim->close_data_admission();
        pci::pci_config_write32(0, slot, 0, MASK, UINT32_MAX);  // Cleanup masks all vectors.
        ASSERT_EQ(load(function, MASK, 4), UINT32_MAX);
        pci::pci_config_write8(0, slot, 0, MASK, 0xfe);
        EXPECT_EQ(load(function, MASK, 4), UINT32_MAX);
        pci::pci_config_write16(0, slot, 0, MASK + 1, 0xfffe);  // Effective aligned word.
        EXPECT_EQ(load(function, MASK, 4), UINT32_MAX);
        pci::pci_config_write32(0, slot, 0, MASK + 3, 0xfffffffe);  // Effective aligned dword.
        EXPECT_EQ(load(function, MASK, 4), UINT32_MAX);
        auto const ADDRESS = load(function, 0x64, 4);
        pci::pci_config_write32(0, slot, 0, 0x64, ADDRESS);  // Identical preservation is safe.
        pci::pci_config_write8(0, slot, 0, 0x65, 0xff);
        pci::pci_config_write16(0, slot, 0, 0x65, 0);
        pci::pci_config_write32(0, slot, 0, 0x67, ADDRESS ^ 0x1000);
        EXPECT_EQ(load(function, 0x64, 4), ADDRESS);
        pci::pci_config_write16(0, slot, 0, DATA, 49);
        EXPECT_EQ(load(function, DATA, 2), 48U);
        if (WIDE) {
            pci::pci_config_write32(0, slot, 0, 0x68, 1);
            EXPECT_EQ(load(function, 0x68, 4), 0U);
        }
        auto const CONTROL = load(function, 0x62, 2);
        pci::pci_config_write16(0, slot, 0, 0x62, CONTROL | 0x10);
        EXPECT_EQ(load(function, 0x62, 2), CONTROL);
    }
}

TEST_F(PciRuntime, ActualLegacyDiagnosticDmaRetainsAnonymousFunctionIdentity) {
    auto const RESULT = dma::allocate_coherent(device(22), 4096);
    ASSERT_EQ(RESULT.error, dma::Error::none);
    EXPECT_EQ(pci::driver_claim_status(device(22))->driver, pci::DriverKind::diagnostic);
    EXPECT_EQ(pci::driver_claim_status(device(22))->active_operations, 0U);
    EXPECT_FALSE(pci::try_claim_driver(device(22), pci::DriverKind::ahci));
    auto const PREPARED = dma::prepare(RESULT.allocation.handle);
    ASSERT_EQ(PREPARED.error, dma::Error::none);
    EXPECT_EQ(dma::complete(PREPARED.submission, 0, dma::Retirement::never_submitted), dma::Error::none);
    EXPECT_EQ(dma::release(RESULT.allocation.handle), dma::Error::none);
}
}  // namespace pci_runtime
