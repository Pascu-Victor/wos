#include <gtest/gtest.h>
#include <sys/mman.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <dev/iommu/discovery.hpp>
#include <dev/pci.hpp>
#include <platform/acpi/acpi.hpp>
#include <platform/mm/mm.hpp>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

// The entire production discovery.cpp is linked separately. These ports supply
// checked firmware storage, PCI observations and an actual read-only host page;
// no test reset or second implementation of source selection is provided.
namespace discovery_runtime {
namespace acpi = ker::mod::acpi::bounded;
namespace iommu = ker::dev::iommu;
namespace pci = ker::dev::pci;
constexpr uint64_t RSDP = 0x100;
constexpr uint64_t XSDT = 0x1000;
constexpr uint64_t DMAR = 0x4000;
constexpr uint64_t IVRS = 0x20000;
constexpr uint64_t MMIO = 0xfed90000;
constexpr uint64_t CAP = (uint64_t{3} << 54) | (uint64_t{0x22} << 24) | (uint64_t{47} << 16) | (6U << 8) | 6U;
constexpr uint64_t ECAP = (0xfU << 8) | 0x41U;

struct Counters {
    size_t lookups{}, memory{}, flags{}, maps{}, registers{}, pci_count{}, pci_device{}, pci_complete{};
    auto operator==(const Counters&) const -> bool = default;
};
struct Blob {
    uint64_t physical{};
    std::vector<uint8_t> bytes;
    size_t readable = SIZE_MAX;
};
struct Fixture {
    std::vector<Blob> blobs;
    std::vector<std::string_view> signatures;
    std::vector<std::pair<uint64_t, size_t>> reads;
    Counters calls;
    bool allow_intel = false;
    bool enumeration_complete = true;
    bool mutate_intel_after_second_lookup = false;
    size_t pci_count = 1;
    uint64_t memory_type = LIMINE_MEMMAP_RESERVED;
    void* register_page{};
    pci::PCIDevice device{.bus = 0, .slot = 2, .function = 0};

    ~Fixture() {
        if (register_page != nullptr) assert(munmap(register_page, 4096) == 0);
    }
    static void put(std::vector<uint8_t>& bytes, size_t at, uint64_t value, size_t count) {
        assert(at <= bytes.size() && count <= bytes.size() - at);
        for (size_t i = 0; i < count; ++i) bytes.at(at + i) = static_cast<uint8_t>(value >> (8U * i));
    }
    static void checksum(std::vector<uint8_t>& bytes, size_t offset) {
        bytes.at(offset) = 0;
        uint8_t sum = 0;
        for (auto byte : bytes) sum = static_cast<uint8_t>(sum + byte);
        bytes.at(offset) = static_cast<uint8_t>(0U - sum);
    }
    static auto table(std::string_view signature, size_t length, uint8_t revision = 1) -> std::vector<uint8_t> {
        std::vector<uint8_t> bytes(length);
        std::copy(signature.begin(), signature.end(), bytes.begin());
        put(bytes, 4, length, 4);
        bytes.at(8) = revision;
        checksum(bytes, 9);
        return bytes;
    }
    static auto intel(size_t units = 1) -> std::vector<uint8_t> {
        auto bytes = table("DMAR", 48 + 16 * units);
        bytes.at(36) = 47;
        for (size_t i = 0; i < units; ++i) {
            size_t const at = 48 + i * 16;
            put(bytes, at + 2, 16, 2);
            bytes.at(at + 4) = 1;
            put(bytes, at + 6, i, 2);  // One include-all default per segment.
            put(bytes, at + 8, MMIO + i * 4096, 8);
        }
        checksum(bytes, 9);
        return bytes;
    }
    static auto amd() -> std::vector<uint8_t> {
        auto bytes = table("IVRS", 72, 2);
        bytes.at(48) = 0x10;
        put(bytes, 50, 24, 2);
        put(bytes, 52, 16, 2);  // IOMMU PCI function 00:02.0.
        put(bytes, 54, 0x40, 2);
        put(bytes, 56, MMIO, 8);
        checksum(bytes, 9);
        return bytes;
    }
    auto add(uint64_t physical, std::vector<uint8_t> bytes) -> Blob& {
        blobs.push_back({physical, std::move(bytes)});
        return blobs.back();
    }
    void root(std::span<const uint64_t> entries) {
        auto xsdt = table("XSDT", 36 + entries.size() * 8);
        for (size_t i = 0; i < entries.size(); ++i) put(xsdt, 36 + i * 8, entries[i], 8);
        checksum(xsdt, 9);
        add(XSDT, std::move(xsdt));
        std::vector<uint8_t> rsdp(36);
        constexpr std::string_view signature = "RSD PTR ";
        std::copy(signature.begin(), signature.end(), rsdp.begin());
        rsdp.at(15) = 2;
        put(rsdp, 20, 36, 4);
        put(rsdp, 24, XSDT, 8);
        uint8_t sum = 0;
        for (size_t i = 0; i < 20; ++i) sum = static_cast<uint8_t>(sum + rsdp[i]);
        rsdp.at(8) = static_cast<uint8_t>(0U - sum);
        checksum(rsdp, 32);
        add(RSDP, std::move(rsdp));
    }
    auto blob(uint64_t physical) -> Blob& {
        auto it = std::find_if(blobs.begin(), blobs.end(), [physical](auto const& b) { return b.physical == physical; });
        assert(it != blobs.end());
        return *it;
    }
    auto read(uint64_t physical, size_t count) -> std::span<const uint8_t> {
        reads.emplace_back(physical, count);
        for (auto const& b : blobs) {
            size_t const accessible = std::min(b.readable, b.bytes.size());
            if (acpi::contains(b.physical, accessible, physical, count))
                return std::span<const uint8_t>{b.bytes}.subspan(physical - b.physical, count);
        }
        return {};
    }
    void intel_registers(uint32_t version = 0x10, uint32_t status = 0) {
        allow_intel = true;
        register_page = mmap(nullptr, 4096, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        assert(register_page != MAP_FAILED);
        auto* bytes = static_cast<uint8_t*>(register_page);
        std::memcpy(bytes, &version, sizeof(version));
        std::memcpy(bytes + 8, &CAP, sizeof(CAP));
        std::memcpy(bytes + 16, &ECAP, sizeof(ECAP));
        std::memcpy(bytes + 28, &status, sizeof(status));
        assert(mprotect(register_page, 4096, PROT_READ) == 0);  // Any hardware write is a test failure.
    }
};
Fixture* active{};

template <typename Fn>
void isolated(Fn&& fn) {
    // The parent never discovers anything. Every child inherits pristine
    // production globals and exercises the real irreversible boot publication.
    EXPECT_EXIT(
        {
            {
                Fixture fixture;
                active = &fixture;
                EXPECT_EQ(iommu::discovery(), nullptr);
                fn(fixture);
                active = nullptr;
            }
            std::_Exit(::testing::Test::HasFailure() ? 17 : 0);
        },
        ::testing::ExitedWithCode(0), ".*");
}

void both_lookups(const Fixture& fixture) {
    EXPECT_EQ(fixture.calls.lookups, 2U);
    EXPECT_EQ(fixture.signatures, (std::vector<std::string_view>{"DMAR", "IVRS"}));
}
void no_hardware(const Fixture& fixture) {
    EXPECT_EQ(fixture.calls.memory + fixture.calls.flags + fixture.calls.maps + fixture.calls.registers, 0U);
    EXPECT_EQ(fixture.calls.pci_count + fixture.calls.pci_device + fixture.calls.pci_complete, 0U);
}
void no_vendor_tables(const iommu::Discovery& result) {
    EXPECT_TRUE(result.table.bytes.empty());
    EXPECT_TRUE(result.amd_table.bytes.empty());
    EXPECT_EQ(result.unit_count, 0U);
}
void idempotent(Fixture& fixture, const iommu::Discovery* result) {
    auto const before = fixture.calls;
    auto const reads = fixture.reads;
    iommu::discover();
    EXPECT_EQ(iommu::discovery(), result);
    EXPECT_EQ(fixture.calls, before);
    EXPECT_EQ(fixture.reads, reads);
}
}  // namespace discovery_runtime

namespace ker::mod::acpi {
auto find_table_bounded(std::string_view signature) -> bounded::Result {
    auto& fixture = *discovery_runtime::active;
    ++fixture.calls.lookups;
    fixture.signatures.push_back(signature);
    auto reader = [&fixture](uint64_t physical, size_t count) { return fixture.read(physical, count); };
    auto result = bounded::find(reader, discovery_runtime::RSDP, signature);
    if (signature == "IVRS" && fixture.mutate_intel_after_second_lookup) {
        auto& bytes = fixture.blob(discovery_runtime::DMAR).bytes;
        bytes.at(8) = 0;
        discovery_runtime::Fixture::checksum(bytes, 9);
    }
    return result;
}
}  // namespace ker::mod::acpi
namespace ker::mod::mm {
auto boot_memory_map() -> const limine_memmap_response* {
    auto& fixture = *discovery_runtime::active;
    assert(fixture.allow_intel && fixture.calls.lookups == 2);
    ++fixture.calls.memory;
    static limine_memmap_entry entry{};
    static limine_memmap_entry* entries[]{&entry};
    static limine_memmap_response response{0, 1, entries};
    entry = {discovery_runtime::MMIO, 4096, fixture.memory_type};
    return &response;
}
namespace addr {
auto get_hhdm_offset() -> uint64_t { return 0; }
auto get_virt_pointer(vaddr_t physical) -> vaddr_t* {
    auto& fixture = *discovery_runtime::active;
    assert(fixture.allow_intel && fixture.calls.lookups == 2 && physical == discovery_runtime::MMIO && fixture.calls.maps == 1);
    ++fixture.calls.registers;
    return static_cast<vaddr_t*>(fixture.register_page);
}
}  // namespace addr
namespace virt {
auto kernel_mmio_page_flags() -> uint64_t {
    auto& fixture = *discovery_runtime::active;
    assert(fixture.allow_intel && fixture.calls.lookups == 2);
    ++fixture.calls.flags;
    return 0x123;
}
void map_range_to_kernel_page_table(Range range, uint64_t flags) {
    auto& fixture = *discovery_runtime::active;
    assert(fixture.allow_intel && fixture.calls.lookups == 2);
    assert(range.start == discovery_runtime::MMIO && range.end == discovery_runtime::MMIO + 4096 && flags == 0x123);
    ++fixture.calls.maps;
}
}  // namespace virt
}  // namespace ker::mod::mm
namespace ker::dev::pci {
auto pci_device_count() -> size_t {
    auto& fixture = *discovery_runtime::active;
    assert(fixture.allow_intel);
    ++fixture.calls.pci_count;
    return fixture.pci_count;
}
auto pci_get_device(size_t index) -> PCIDevice* {
    auto& fixture = *discovery_runtime::active;
    assert(fixture.allow_intel && index == 0);
    ++fixture.calls.pci_device;
    return &fixture.device;
}
auto pci_enumeration_complete() -> bool {
    auto& fixture = *discovery_runtime::active;
    assert(fixture.allow_intel);
    ++fixture.calls.pci_complete;
    return fixture.enumeration_complete;
}
}  // namespace ker::dev::pci

namespace discovery_runtime {
TEST(IommuDiscoveryRuntimeDeathTest, AbsentPublishesOnceWithoutHardware) {
    isolated([](Fixture& f) {
        f.root({});
        iommu::discover();
        auto const* result = iommu::discovery();
        ASSERT_NE(result, nullptr);
        EXPECT_EQ(result->state, iommu::DiscoveryState::absent);
        EXPECT_EQ(result->vendor, iommu::FirmwareVendor::none);
        both_lookups(f);
        no_hardware(f);
        no_vendor_tables(*result);
        idempotent(f, result);
    });
}
TEST(IommuDiscoveryRuntimeDeathTest, AmdCopiesPrivateBytesAndNeverProbesIntel) {
    isolated([](Fixture& f) {
        std::array entries{IVRS};
        f.root(entries);
        f.add(IVRS, Fixture::amd());
        auto const original = f.blob(IVRS).bytes;
        iommu::discover();
        auto const* result = iommu::discovery();
        ASSERT_NE(result, nullptr);
        EXPECT_EQ(result->state, iommu::DiscoveryState::ivrs_discovered_unsupported);
        EXPECT_EQ(result->vendor, iommu::FirmwareVendor::amd);
        EXPECT_EQ(result->ivrs_error, iommu::ivrs::Error::none);
        EXPECT_EQ(result->amd_table.hardware_blocks, 1U);
        EXPECT_EQ(result->amd_table.memory_blocks, 0U);
        EXPECT_EQ(result->amd_table.device_entries, 0U);
        EXPECT_EQ(result->amd_table.revision, 2U);
        EXPECT_EQ(result->amd_table.ivinfo, 0U);
        EXPECT_EQ(result->unit_count, 0U);
        EXPECT_TRUE(result->table.bytes.empty());
        EXPECT_EQ(result->ownership.error, iommu::scopes::Error::incomplete);
        ASSERT_EQ(result->amd_table.bytes.size(), original.size());
        EXPECT_NE(result->amd_table.bytes.data(), f.blob(IVRS).bytes.data());
        f.blob(IVRS).bytes.assign(original.size(), 0xa5);
        EXPECT_TRUE(std::equal(original.begin(), original.end(), result->amd_table.bytes.begin()));
        both_lookups(f);
        no_hardware(f);
        idempotent(f, result);
    });
}
TEST(IommuDiscoveryRuntimeDeathTest, OneVendorLookupFailuresAreExplicitAndNeverProbe) {
    for (bool amd : {false, true}) {
        for (unsigned failure = 0; failure < 4; ++failure) {
            isolated([amd, failure](Fixture& f) {
                uint64_t const physical = amd ? IVRS : DMAR;
                std::array entries{physical};
                f.root(entries);
                f.add(physical, amd ? Fixture::amd() : Fixture::intel());
                auto& source = f.blob(physical);
                acpi::Error expected = acpi::Error::checksum;
                if (failure == 0) source.bytes.at(10) ^= 1;
                if (failure == 1) {
                    Fixture::put(source.bytes, 4, acpi::MAX_TABLE_BYTES + 1, 4);
                    expected = acpi::Error::length;
                }
                if (failure == 2) {
                    source.readable = 36;
                    expected = acpi::Error::inaccessible;
                }
                if (failure == 3) {
                    auto& root = f.blob(XSDT).bytes;
                    root.resize(52);
                    Fixture::put(root, 4, root.size(), 4);
                    Fixture::put(root, 44, physical, 8);
                    Fixture::checksum(root, 9);
                    expected = acpi::Error::duplicate;
                }
                iommu::discover();
                auto const* result = iommu::discovery();
                ASSERT_NE(result, nullptr);
                EXPECT_EQ(result->state, iommu::DiscoveryState::firmware_error);
                EXPECT_EQ(result->vendor, amd ? iommu::FirmwareVendor::amd : iommu::FirmwareVendor::intel);
                EXPECT_EQ(result->firmware_error, expected);
                both_lookups(f);
                no_hardware(f);
                no_vendor_tables(*result);
                idempotent(f, result);
            });
        }
    }
}
TEST(IommuDiscoveryRuntimeDeathTest, AnyTwoPresentSourcesConflictBeforeParsingOrProbe) {
    for (unsigned intel_error = 0; intel_error < 3; ++intel_error) {
        for (unsigned amd_error = 0; amd_error < 3; ++amd_error) {
            isolated([intel_error, amd_error](Fixture& f) {
                std::array entries{DMAR, IVRS};
                f.root(entries);
                f.add(DMAR, Fixture::intel());
                f.add(IVRS, Fixture::amd());
                for (auto [physical, error] : {std::pair{DMAR, intel_error}, std::pair{IVRS, amd_error}}) {
                    auto& bytes = f.blob(physical).bytes;
                    if (error == 1) bytes.at(10) ^= 1;  // Bounded lookup failure.
                    if (error == 2) {                   // ACPI-valid, invalid vendor parser revision.
                        bytes.at(8) = 0;
                        Fixture::checksum(bytes, 9);
                    }
                }
                iommu::discover();
                auto const* result = iommu::discovery();
                ASSERT_NE(result, nullptr);
                EXPECT_EQ(result->state, iommu::DiscoveryState::conflicting_firmware);
                EXPECT_EQ(result->vendor, iommu::FirmwareVendor::ambiguous);
                EXPECT_EQ(result->firmware_error, intel_error == 1 || amd_error == 1 ? acpi::Error::checksum : acpi::Error::none);
                both_lookups(f);
                no_hardware(f);
                no_vendor_tables(*result);
            });
        }
    }
}
TEST(IommuDiscoveryRuntimeDeathTest, BrokenGlobalAcpiIsAmbiguousAndDmarErrorWinsDeterministically) {
    isolated([](Fixture& f) {
        f.root({});
        f.blob(RSDP).bytes.at(10) ^= 1;
        iommu::discover();
        auto const* result = iommu::discovery();
        ASSERT_NE(result, nullptr);
        EXPECT_EQ(result->state, iommu::DiscoveryState::conflicting_firmware);
        EXPECT_EQ(result->vendor, iommu::FirmwareVendor::ambiguous);
        EXPECT_EQ(result->firmware_error, acpi::Error::checksum);
        both_lookups(f);
        no_hardware(f);
        no_vendor_tables(*result);
    });
    isolated([](Fixture& f) {
        std::array entries{DMAR, IVRS};
        f.root(entries);
        f.add(DMAR, Fixture::intel());
        f.add(IVRS, Fixture::amd());
        Fixture::put(f.blob(DMAR).bytes, 4, acpi::MAX_TABLE_BYTES + 1, 4);
        f.blob(IVRS).bytes.at(10) ^= 1;
        iommu::discover();
        auto const* result = iommu::discovery();
        ASSERT_NE(result, nullptr);
        EXPECT_EQ(result->state, iommu::DiscoveryState::conflicting_firmware);
        EXPECT_EQ(result->firmware_error, acpi::Error::length);
        no_hardware(f);
        no_vendor_tables(*result);
    });
}
TEST(IommuDiscoveryRuntimeDeathTest, AmdParserFailurePublishesNoBorrowedOrIntelMetadata) {
    isolated([](Fixture& f) {
        std::array entries{IVRS};
        f.root(entries);
        auto bytes = Fixture::amd();
        bytes.at(8) = 0;
        Fixture::checksum(bytes, 9);
        f.add(IVRS, std::move(bytes));
        iommu::discover();
        auto const* result = iommu::discovery();
        ASSERT_NE(result, nullptr);
        EXPECT_EQ(result->state, iommu::DiscoveryState::ivrs_error);
        EXPECT_EQ(result->vendor, iommu::FirmwareVendor::amd);
        EXPECT_NE(result->ivrs_error, iommu::ivrs::Error::none);
        both_lookups(f);
        no_hardware(f);
        no_vendor_tables(*result);
        idempotent(f, result);
    });
}
TEST(IommuDiscoveryRuntimeDeathTest, SelectedSourceCannotExceedPrivateCopyCapacity) {
    for (bool amd : {false, true}) {
        isolated([amd](Fixture& f) {
            uint64_t const physical = amd ? IVRS : DMAR;
            std::array entries{physical};
            f.root(entries);
            f.add(physical, Fixture::table(amd ? "IVRS" : "DMAR", iommu::dmar::MAX_TABLE_SIZE + 1));
            iommu::discover();
            auto const* result = iommu::discovery();
            ASSERT_NE(result, nullptr);
            EXPECT_EQ(result->vendor, amd ? iommu::FirmwareVendor::amd : iommu::FirmwareVendor::intel);
            EXPECT_EQ(result->state, amd ? iommu::DiscoveryState::ivrs_error : iommu::DiscoveryState::dmar_error);
            EXPECT_EQ(result->firmware_error, acpi::Error::none);
            if (amd)
                EXPECT_EQ(result->ivrs_error, iommu::ivrs::Error::length);
            else
                EXPECT_EQ(result->table_error, iommu::dmar::Error::length);
            both_lookups(f);
            no_hardware(f);
            no_vendor_tables(*result);
        });
    }
}
TEST(IommuDiscoveryRuntimeDeathTest, SelectedBytesAreParsedAfterBoundedLookupAndPrivateCapture) {
    isolated([](Fixture& f) {
        std::array entries{DMAR};
        f.root(entries);
        f.add(DMAR, Fixture::intel());
        f.mutate_intel_after_second_lookup = true;
        iommu::discover();
        auto const* result = iommu::discovery();
        ASSERT_NE(result, nullptr);
        EXPECT_EQ(result->state, iommu::DiscoveryState::dmar_error);
        EXPECT_EQ(result->table_error, iommu::dmar::Error::revision);
        EXPECT_EQ(result->vendor, iommu::FirmwareVendor::intel);
        both_lookups(f);
        no_hardware(f);
        no_vendor_tables(*result);
    });
}
TEST(IommuDiscoveryRuntimeDeathTest, IntelRetainsActualProbeAndPrivateFirmwareOwnership) {
    isolated([](Fixture& f) {
        std::array entries{DMAR};
        f.root(entries);
        f.add(DMAR, Fixture::intel());
        auto const original = f.blob(DMAR).bytes;
        f.intel_registers();
        iommu::discover();
        auto const* result = iommu::discovery();
        ASSERT_NE(result, nullptr);
        EXPECT_EQ(result->state, iommu::DiscoveryState::discovered);
        EXPECT_EQ(result->vendor, iommu::FirmwareVendor::intel);
        ASSERT_EQ(result->unit_count, 1U);
        auto const& unit = result->units[0];
        EXPECT_EQ(unit.state, iommu::UnitState::available);
        EXPECT_EQ(unit.physical, MMIO);
        EXPECT_EQ(unit.register_bytes, 4096U);
        EXPECT_EQ(unit.segment, 0U);
        EXPECT_TRUE(unit.include_all);
        EXPECT_EQ(unit.table_offset, 48U);
        EXPECT_EQ(unit.version, 0x10U);
        EXPECT_EQ(unit.cap, CAP);
        EXPECT_EQ(unit.ecap, ECAP);
        EXPECT_EQ(unit.global_status, 0U);
        EXPECT_TRUE(unit.capability.valid());
        EXPECT_EQ(result->ownership.error, iommu::scopes::Error::none);
        EXPECT_EQ(result->ownership.device_count, 1U);
        EXPECT_TRUE(result->amd_table.bytes.empty());
        EXPECT_EQ(f.calls.memory, 1U);
        EXPECT_EQ(f.calls.maps, 1U);
        EXPECT_EQ(f.calls.registers, 1U);
        EXPECT_EQ(f.calls.pci_count, 1U);
        EXPECT_EQ(f.calls.pci_device, 1U);
        EXPECT_EQ(f.calls.pci_complete, 1U);
        EXPECT_NE(result->table.bytes.data(), f.blob(DMAR).bytes.data());
        f.blob(DMAR).bytes.assign(original.size(), 0x5a);
        EXPECT_TRUE(std::equal(original.begin(), original.end(), result->table.bytes.begin()));
        both_lookups(f);
        idempotent(f, result);
    });
}
TEST(IommuDiscoveryRuntimeDeathTest, IntelUnsafeCapabilityAndFirmwareActiveStatesArePreserved) {
    for (unsigned scenario = 0; scenario < 3; ++scenario) {
        isolated([scenario](Fixture& f) {
            std::array entries{DMAR};
            f.root(entries);
            f.add(DMAR, Fixture::intel());
            f.intel_registers(scenario == 1 ? 0 : 0x10, scenario == 2 ? uint32_t{1} << 31 : 0);
            if (scenario == 0) f.memory_type = LIMINE_MEMMAP_USABLE;
            iommu::discover();
            auto const* result = iommu::discovery();
            ASSERT_NE(result, nullptr);
            EXPECT_EQ(result->state, iommu::DiscoveryState::discovered);
            EXPECT_EQ(result->vendor, iommu::FirmwareVendor::intel);
            ASSERT_EQ(result->unit_count, 1U);
            auto const expected = scenario == 0   ? iommu::UnitState::unsafe_register_range
                                  : scenario == 1 ? iommu::UnitState::capability_error
                                                  : iommu::UnitState::firmware_active;
            EXPECT_EQ(result->units[0].state, expected);
            EXPECT_EQ(f.calls.maps, scenario == 0 ? 0U : 1U);
            EXPECT_EQ(f.calls.registers, scenario == 0 ? 0U : 1U);
            EXPECT_TRUE(result->amd_table.bytes.empty());
            both_lookups(f);
            idempotent(f, result);
        });
    }
}
TEST(IommuDiscoveryRuntimeDeathTest, IntelParserAndInventoryLimitsFailBeforeProbe) {
    for (bool limit : {false, true}) {
        isolated([limit](Fixture& f) {
            std::array entries{DMAR};
            f.root(entries);
            auto bytes = Fixture::intel(limit ? iommu::MAX_UNITS + 1 : 1);
            if (!limit) bytes.at(8) = 0;
            Fixture::checksum(bytes, 9);
            f.add(DMAR, std::move(bytes));
            iommu::discover();
            auto const* result = iommu::discovery();
            ASSERT_NE(result, nullptr);
            EXPECT_EQ(result->vendor, iommu::FirmwareVendor::intel);
            EXPECT_EQ(result->state, limit ? iommu::DiscoveryState::inventory_limit : iommu::DiscoveryState::dmar_error);
            both_lookups(f);
            no_hardware(f);
            no_vendor_tables(*result);
        });
    }
}
}  // namespace discovery_runtime
