#include <gtest/gtest.h>

#include <array>
#include <cassert>
#include <cstring>
#include <dev/pci.hpp>
#include <dev/pci_port_io.hpp>
#include <latch>
#include <platform/asm/cpu.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/virt.hpp>
#include <test_backend.hpp>
#include <thread>

namespace pci_runtime {
namespace pci = ker::dev::pci;
thread_local unsigned lock_depth = 0;
std::atomic<uint64_t> lock_acquisitions{0};
constexpr size_t DEVICES = 9;
constexpr uint64_t BASE = 0x80000000;
struct Function {
    std::array<uint8_t, 256> config{};
    alignas(4096) std::array<uint32_t, 2048> mmio{};
    std::array<bool, 6> sizing{};
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
        store(f, 0, 0x10001234U + static_cast<uint32_t>(slot) * 0x10000, 4);
        store(f, 4, 2, 2);
        store(f, 6, 0x8010, 2);
        store(f, 0x10, physical(slot), 4);
        store(f, 0x14, physical(slot) + 4096, 4);
        store(f, 0x34, 0x40, 1);
        store(f, 0x40, 0x11, 2);
        store(f, 0x42, 3, 2);  // Four entries in BAR0; separate PBA within it.
        store(f, 0x44, 0, 4);
        store(f, 0x48, 0x800, 4);
    }
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
    if (OFFSET >= 0x10 && OFFSET < 0x28 && f->sizing[(OFFSET - 0x10) / 4]) return 0xfffff000;
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
}  // namespace ker::dev::iommu
namespace ker::mod::cpu {
void cpuid(CpuidContext* c) { c->eax = c->function == static_cast<int>(0x80000000U) ? static_cast<int>(0x80000008U) : 48; }
}  // namespace ker::mod::cpu
namespace ker::mod::smt {
auto get_core_count() -> uint64_t { return 2; }
auto get_apic_id_for_cpu(uint64_t cpu) -> uint32_t { return static_cast<uint32_t>(cpu + 2); }
}  // namespace ker::mod::smt
namespace ker::mod::mm {
auto boot_memory_map() -> const limine_memmap_response* {
    static limine_memmap_entry entry{pci_runtime::BASE, 0x100000, LIMINE_MEMMAP_RESERVED};
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
