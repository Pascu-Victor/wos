#include <gtest/gtest.h>

#include <dev/pci_aperture.hpp>
#include <dev/pci_platform_apertures.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/asm/cpu_safe_msr.hpp>
#include <platform/asm/msr.hpp>
#include <utility>

#define PCI_COLD_DEFINE_PORTS
#include "pci_cold_fixture.hpp"

namespace aperture = ker::dev::pci::aperture;
namespace resource = ker::dev::pci::resource;
namespace hardware = pci_cold_host;
namespace {
uint8_t physical_bits = 36;
bool apic_readable = true;
uint64_t apic_base = 0xfee00c00;
auto image(const hardware::Function& function) -> aperture::Image {
    aperture::Image result;
    for (size_t i = 0; i < result.words.size(); ++i) result.words[i] = static_cast<uint32_t>(hardware::get(function.config, i * 4, 4));
    return result;
}
auto platform() -> aperture::Platform {
    aperture::Platform result;
    result.physical_bits = 36;
    result.ecam.count = 1;
    result.ecam.windows[0] = {.base = hardware::ECAM, .last_bus = 255};
    return result;
}
class PciAperture : public ::testing::Test {
    void SetUp() override {
        hardware::setup();
        physical_bits = 36;
        apic_readable = true;
        apic_base = 0xfee00c00;
    }
};
auto collect() -> aperture::Platform {
    aperture::Platform result;
    aperture::collect_platform(result, image(hardware::functions[0]), image(hardware::functions[31]));
    return result;
}
}  // namespace

namespace ker::mod::cpu {
void cpuid(CpuidContext* cpu) { cpu->eax = cpu->function == static_cast<int>(0x80000000U) ? static_cast<int>(0x80000008U) : physical_bits; }
auto try_read_msr(uint32_t index, uint64_t& output) -> bool {
    EXPECT_EQ(index, IA32_APIC_BASE);
    if (!apic_readable) return false;
    output = apic_base;
    return true;
}
}  // namespace ker::mod::cpu

TEST_F(PciAperture, UnknownBarBoundsNeverTurnZeroIntoAbsence) {
    auto range = aperture::possible_bar({.kind = resource::BarKind::memory32}, 36);
    ASSERT_TRUE(range);
    EXPECT_EQ(range->start, 0U);
    EXPECT_EQ(range->bytes, 0x80000000U);
    range = aperture::possible_bar({.kind = resource::BarKind::memory32, .address = 0x81060000}, 36);
    ASSERT_TRUE(range);
    EXPECT_EQ(range->start, 0x81060000U);
    EXPECT_EQ(range->bytes, 0x20000U);
    range = aperture::possible_bar({.kind = resource::BarKind::memory64}, 36);
    ASSERT_TRUE(range);
    EXPECT_EQ(range->bytes, uint64_t{1} << 36);
    range = aperture::possible_bar({.kind = resource::BarKind::memory64, .address = 0x100000000ULL}, 36);
    ASSERT_TRUE(range);
    EXPECT_EQ(range->bytes, 0x100000000ULL);
    EXPECT_FALSE(aperture::possible_bar({.kind = resource::BarKind::memory32, .address = 0x100000000ULL}, 36));
    EXPECT_FALSE(aperture::possible_bar({.kind = resource::BarKind::io, .address = 0x1000}, 36));
    EXPECT_FALSE(aperture::possible_bar({.kind = resource::BarKind::memory64}, 53));
}
TEST_F(PciAperture, RoundedExtentAndRomBoundsAreChecked) {
    auto range = aperture::rounded(0x81234567, 16);
    ASSERT_TRUE(range);
    EXPECT_EQ(range->start, 0x81234000U);
    EXPECT_EQ(range->bytes, 4096U);
    EXPECT_FALSE(aperture::rounded(UINT64_MAX - 7, 16));
    EXPECT_FALSE(aperture::rounded(4096, 0));
    range = aperture::possible_rom(0);
    ASSERT_TRUE(range);
    EXPECT_EQ(range->bytes, 0x1000000U);
    range = aperture::possible_rom(0xfffff801);
    ASSERT_TRUE(range);
    EXPECT_EQ(range->start, 0xfffff000U);
    EXPECT_EQ(range->end(), 0x100000000ULL);
}
TEST_F(PciAperture, BridgeWindowsHandleInclusiveLimitsDisabledAnd64BitUpperWords) {
    aperture::Image config;
    config.words[0x20 / 4] = 0x8ff08000;
    config.words[0x24 / 4] = 0x9ff19001;
    config.words[0x28 / 4] = config.words[0x2c / 4] = 2;
    auto windows = aperture::bridge_windows(config, 36);
    ASSERT_TRUE(windows.valid);
    EXPECT_EQ(windows.memory.start, 0x80000000U);
    EXPECT_EQ(windows.memory.end(), 0x90000000U);
    EXPECT_EQ(windows.prefetch.start, 0x290000000ULL);
    EXPECT_EQ(windows.prefetch.end(), 0x2a0000000ULL);
    config.words[0x20 / 4] = 0x80009000;
    EXPECT_EQ(aperture::bridge_windows(config, 36).memory.bytes, 0U);
    config.words[0x24 / 4] |= 2;
    EXPECT_FALSE(aperture::bridge_windows(config, 36).valid);
    config.words[0x24 / 4] = 0x9ff09000;
    EXPECT_FALSE(aperture::bridge_windows(config, 36).valid);  // Nonzero upper words in32-bit form.
}
TEST_F(PciAperture, EffectiveWritesCoverBothBarHalvesRomBridgeResetAndCommands) {
    EXPECT_TRUE(aperture::geometry_write(0, aperture::effective_offset(0x17, 4), 4));
    EXPECT_TRUE(aperture::geometry_write(0, aperture::effective_offset(0x33, 2), 2));
    EXPECT_TRUE(aperture::geometry_write(1, 0x1a, 1));
    EXPECT_TRUE(aperture::geometry_write(1, 0x3e, 2));
    EXPECT_FALSE(aperture::geometry_write(0, 4, 4));
    EXPECT_EQ(aperture::project16(0x0402, 4, 5, 1, 0), 2);
    EXPECT_EQ(aperture::project16(0x0402, 4, 4, 4, 0xabcd0004), 4);
    aperture::Image bounded;
    EXPECT_EQ(bounded.read(255, 4), UINT32_MAX);
    EXPECT_EQ(bounded.read64(252), UINT64_MAX);
}
TEST_F(PciAperture, ActualCollectorAcceptsExplicitHeadlessQ35InputsAndExcludesPlatformWindows) {
    auto result = collect();
    ASSERT_TRUE(result.valid);
    EXPECT_FALSE(result.excludes({hardware::BAR0, hardware::BAR_BYTES}));
    EXPECT_TRUE(result.excludes({hardware::ECAM, 4096}));
    EXPECT_TRUE(result.excludes({0xfee00000, 4096}));
    EXPECT_TRUE(result.excludes({0xfed00000, 4096}));
    EXPECT_TRUE(result.excludes({0xa0000, 4096}));
}
TEST_F(PciAperture, NativeCollectorRefusesMissingMalformedMultiSegmentAndUnknownController) {
    hardware::mcfg.clear();
    EXPECT_FALSE(collect().valid);
    hardware::setup();
    hardware::put(hardware::mcfg, 52, 1, 2);
    hardware::checksum(hardware::mcfg);
    EXPECT_FALSE(collect().valid);
    hardware::setup();
    hardware::madt[44] = 255;
    hardware::checksum(hardware::madt);
    EXPECT_FALSE(collect().valid);
    hardware::setup();
    hardware::fadt[112 + 2] |= 16;
    hardware::checksum(hardware::fadt);
    EXPECT_FALSE(collect().valid);
}
TEST_F(PciAperture, MadtTypeFiveReadsAllEightAddressBytesAndRefusesDuplicates) {
    size_t const at = hardware::madt.size();
    hardware::madt.resize(at + 12);
    hardware::madt[at] = 5;
    hardware::madt[at + 1] = 12;
    hardware::put(hardware::madt, at + 4, 0x100000000ULL, 8);
    hardware::put(hardware::madt, 4, hardware::madt.size(), 4);
    hardware::checksum(hardware::madt);
    auto result = collect();
    ASSERT_TRUE(result.valid);
    EXPECT_TRUE(result.excludes({0x100000000ULL, 4096}));
    hardware::madt.resize(at + 24);
    hardware::madt[at + 12] = 5;
    hardware::madt[at + 13] = 12;
    hardware::put(hardware::madt, at + 16, 0x100001000ULL, 8);
    hardware::put(hardware::madt, 4, hardware::madt.size(), 4);
    hardware::checksum(hardware::madt);
    EXPECT_FALSE(collect().valid);
}
TEST_F(PciAperture, FullWidthProgrammableWindowsAreExcludedAndReservedBitsRefuse) {
    hardware::put(hardware::functions[0].config, 0x40, 0x180000001ULL, 8);
    hardware::put(hardware::functions[0].config, 0x48, 0x180004001ULL, 8);
    hardware::put(hardware::functions[0].config, 0x68, 0x180008001ULL, 8);
    hardware::put(hardware::functions[31].config, 0xf0, 0x82000001, 4);
    auto result = collect();
    ASSERT_TRUE(result.valid);
    for (auto base : {0x180000000ULL, 0x180004000ULL, 0x180008000ULL, 0x82000000ULL}) EXPECT_TRUE(result.excludes({base, 4096}));
    hardware::put(hardware::functions[0].config, 0x48, 0x180004003ULL, 8);
    EXPECT_FALSE(collect().valid);
}
TEST_F(PciAperture, PciexbarLengthFullAddressAndMcfgMustAgree) {
    for (unsigned code = 0; code < 3; ++code) {
        hardware::setup();
        uint64_t const bytes = uint64_t{1} << (28 - code);
        hardware::put(hardware::functions[0].config, 0x60, hardware::ECAM | 1 | (code << 1), 8);
        hardware::mcfg[55] = static_cast<uint8_t>((bytes >> 20) - 1);
        hardware::checksum(hardware::mcfg);
        EXPECT_TRUE(collect().valid) << code;
    }
    hardware::setup();
    hardware::put(hardware::functions[0].config, 0x60, hardware::ECAM | 7, 8);
    EXPECT_FALSE(collect().valid);
    hardware::setup();
    hardware::put(hardware::functions[0].config, 0x60, 0x1e0000001ULL, 8);
    EXPECT_FALSE(collect().valid);
    hardware::setup();
    hardware::put(hardware::functions[0].config, 0x60, hardware::ECAM, 8);
    EXPECT_FALSE(collect().valid);
}
TEST_F(PciAperture, ActiveOpaqueTsegIsRefusedAndObservableExtentProtected) {
    hardware::put(hardware::functions[0].config, 0x9d, 0x0a, 1);
    hardware::put(hardware::functions[0].config, 0x9e, 1, 1);
    EXPECT_FALSE(collect().valid);  // QEMU hidden below4G state is not inferred.
    hardware::put(hardware::functions[0].config, 0xac, 0x7ff00000, 4);
    auto result = collect();
    ASSERT_TRUE(result.valid);
    EXPECT_TRUE(result.excludes({0x7ff00000, 0x100000}));
    hardware::put(hardware::functions[0].config, 0xac, 0x7ff00001, 4);
    EXPECT_FALSE(collect().valid);
}
TEST_F(PciAperture, OpaqueGraphicsReservedPamAndInvalidSmbaseCannotClaimCompleteness) {
    for (auto offset : {0x52U, 0xa4U, 0xa8U, 0x90U, 0x9cU}) {
        hardware::setup();
        hardware::put(hardware::functions[0].config, offset, offset == 0x90 ? 0x80 : 0xff, 1);
        EXPECT_FALSE(collect().valid) << offset;
    }
}
TEST_F(PciAperture, DramRemapAndUpperGeometryAreProtectedWithoutUsingTomAsAHoleFreeRange) {
    hardware::put(hardware::functions[0].config, 0xa0, 0x80, 2);  // 8GiB total need not be contiguous below8GiB.
    hardware::put(hardware::functions[0].config, 0xa2, 0x2400, 2);
    hardware::put(hardware::functions[0].config, 0xb0, 0x8000, 2);
    hardware::put(hardware::functions[0].config, 0x98, 0x80, 2);
    hardware::put(hardware::functions[0].config, 0x9a, 0x8f, 2);
    auto result = collect();
    ASSERT_TRUE(result.valid);
    EXPECT_TRUE(result.excludes({0x7ff00000, 4096}));
    EXPECT_FALSE(result.excludes({hardware::BAR0, 4096}));
    EXPECT_TRUE(result.excludes({0x200000000ULL, 4096}));
    EXPECT_FALSE(result.excludes({0x240000000ULL, 4096}));
}
TEST_F(PciAperture, MissingApicOrPhysicalGeometryFailsClosed) {
    physical_bits = 53;
    EXPECT_FALSE(collect().valid);
    physical_bits = 36;
    apic_readable = false;
    EXPECT_FALSE(collect().valid);
    apic_readable = true;
    apic_base &= ~(uint64_t{1} << 11);
    EXPECT_FALSE(collect().valid);
}
TEST_F(PciAperture, CapacityAndMalformedPublicScratchFailClosed) {
    auto output = platform();
    for (size_t i = 0; i < output.spans.size(); ++i) ASSERT_TRUE(output.add(0x10000000 + i * 8192, 4096));
    EXPECT_FALSE(output.add(0x50000000, 4096));
    EXPECT_TRUE(output.add(0x10000000, 16));
    output.count = output.spans.size() + 1;
    output.valid = true;
    EXPECT_FALSE(output.add(1, 1));
    EXPECT_TRUE(output.excludes({hardware::BAR0, 4096}));
}

TEST_F(PciAperture, IntelObservedRegisterAndRmrrRangesAreRetainedIndependentlyOfDomainPolicy) {
    namespace iommu = ker::dev::iommu;
    auto bytes = hardware::table("DMAR", 96, 1);
    bytes[36] = 35;
    hardware::put(bytes, 50, 16, 2);
    bytes[52] = 1;
    hardware::put(bytes, 56, 0x180000000ULL, 8);
    hardware::put(bytes, 64, 1, 2);
    hardware::put(bytes, 66, 32, 2);
    hardware::put(bytes, 72, 0x90000000, 8);
    hardware::put(bytes, 80, 0x90000fff, 8);
    bytes[88] = 1;
    bytes[89] = 8;
    bytes[94] = 4;
    hardware::checksum(bytes);
    auto parsed = iommu::dmar::parse(bytes);
    ASSERT_TRUE(parsed.valid());
    auto& inventory = hardware::iommu_inventory;
    inventory.state = iommu::DiscoveryState::discovered;
    inventory.vendor = iommu::FirmwareVendor::intel;
    inventory.table = parsed.table;
    inventory.unit_count = 1;
    inventory.units[0].physical = 0x180000000ULL;
    inventory.units[0].register_bytes = 16384;
    auto result = collect();
    ASSERT_TRUE(result.valid);
    EXPECT_TRUE(result.excludes({0x180003000ULL, 4096}));
    EXPECT_TRUE(result.excludes({0x90000000, 4096}));
    EXPECT_FALSE(result.excludes({hardware::BAR0, 4096}));
    inventory.units[0].segment = 1;
    EXPECT_FALSE(collect().valid);
    inventory.units[0].segment = 0;
    inventory.table.unsupported = true;
    EXPECT_FALSE(collect().valid);
}

TEST_F(PciAperture, AmdObservedRegisterProfileAndIvmdRangesNeverActivateATranslator) {
    namespace iommu = ker::dev::iommu;
    auto bytes = hardware::table("IVRS", 104, 2);
    bytes[48] = 0x10;
    hardware::put(bytes, 50, 24, 2);
    hardware::put(bytes, 54, 0x40, 2);
    hardware::put(bytes, 56, 0x180000000ULL, 8);
    bytes[72] = 0x20;
    hardware::put(bytes, 74, 32, 2);
    hardware::put(bytes, 88, 0x90000000, 8);
    hardware::put(bytes, 96, 4096, 8);
    hardware::checksum(bytes);
    auto parsed = iommu::ivrs::parse(bytes);
    ASSERT_TRUE(parsed.valid());
    auto& inventory = hardware::iommu_inventory;
    inventory.state = iommu::DiscoveryState::ivrs_discovered_unsupported;
    inventory.vendor = iommu::FirmwareVendor::amd;
    inventory.amd_table = parsed.table;
    auto result = collect();
    ASSERT_TRUE(result.valid);
    EXPECT_TRUE(result.excludes({0x18007f000ULL, 4096}));
    EXPECT_FALSE(result.excludes({0x180080000ULL, 4096}));
    EXPECT_TRUE(result.excludes({0x90000000, 4096}));
    EXPECT_FALSE(result.excludes({hardware::BAR0, 4096}));
    hardware::put(bytes, 64, 1, 2);
    hardware::checksum(bytes);
    EXPECT_FALSE(collect().valid);
}

TEST_F(PciAperture, OptionalMemoryGasIsCollectedEvenWithoutItsFeatureFlag) {
    hardware::put(hardware::fadt, 112, 0, 4);
    hardware::fadt[116] = 0;
    hardware::fadt[117] = 32;
    hardware::fadt[119] = 3;
    hardware::put(hardware::fadt, 120, 0x90000004, 8);
    hardware::checksum(hardware::fadt);
    auto result = collect();
    ASSERT_TRUE(result.valid);
    EXPECT_TRUE(result.excludes({0x90000000, 4096}));
    EXPECT_FALSE(aperture::possible_rom(0x80000002));
    EXPECT_TRUE(result.excludes({UINT64_MAX, 2}));
}

TEST_F(PciAperture, ColdCapabilityExtentsCannotAliasPowerOrResetControls) {
    auto config = image(hardware::nic());
    ASSERT_TRUE(aperture::conventional_layout(config));
    hardware::put(hardware::nic().config, 0x40, 0x4805, 2);
    hardware::put(hardware::nic().config, 0x48, 1, 2);
    EXPECT_FALSE(aperture::conventional_layout(image(hardware::nic())));
    hardware::setup();
    hardware::put(hardware::nic().config, 0x42, 0x380, 2);
    EXPECT_FALSE(aperture::conventional_layout(image(hardware::nic())));
    hardware::setup();
    hardware::put(hardware::nic().config, 0x78, 0x8000, 2);
    EXPECT_FALSE(aperture::conventional_layout(image(hardware::nic())));
    hardware::setup();
    hardware::put(hardware::nic().config, 0x34, 0xfc, 1);
    hardware::put(hardware::nic().config, 0xfc, 1, 2);
    EXPECT_FALSE(aperture::conventional_layout(image(hardware::nic())));
}

TEST_F(PciAperture, UnchangedObservedFlrStrobeIsNeverAnAllowedDeviceControlWrite) {
    EXPECT_TRUE(aperture::safe_device_control(0x0810, 0));
    EXPECT_TRUE(aperture::safe_device_control(0x0810, 0x0800));
    EXPECT_FALSE(aperture::safe_device_control(0, 0x0810));
    EXPECT_FALSE(aperture::safe_device_control(0x8000, 0x8000));
    EXPECT_FALSE(aperture::safe_device_control(0x8000, 0));
    EXPECT_FALSE(aperture::safe_device_control(0, 1));
}

TEST_F(PciAperture, EveryPossibleHhdmExtentMustAvoidTheWholePrivateBank) {
    constexpr uint64_t HHDM = 0xffff800000000000ULL;
    constexpr aperture::Span ARENA{0xfffffc8000000000ULL, 0x8000000000ULL};
    constexpr uint64_t DISPLACEMENT = ARENA.start - HHDM;
    static_assert(aperture::hhdm_disjoint({0, 0x100000}, HHDM, ARENA));
    EXPECT_TRUE(aperture::hhdm_disjoint({DISPLACEMENT - 4096, 4096}, HHDM, ARENA));
    EXPECT_TRUE(aperture::hhdm_disjoint({DISPLACEMENT + ARENA.bytes, 4096}, HHDM, ARENA));
    EXPECT_FALSE(aperture::hhdm_disjoint({DISPLACEMENT, 4096}, HHDM, ARENA));
    EXPECT_FALSE(aperture::hhdm_disjoint({DISPLACEMENT - 4096, 8192}, HHDM, ARENA));
    EXPECT_FALSE(aperture::hhdm_disjoint({UINT64_MAX - 4096, 4096}, HHDM, ARENA));
    EXPECT_FALSE(aperture::hhdm_disjoint({0, 4096}, 0xfffffffffffff000ULL, ARENA));
    EXPECT_FALSE(aperture::hhdm_disjoint({0x80000000, 4096}, 0, ARENA));
    EXPECT_FALSE(aperture::hhdm_disjoint({0, 4096}, HHDM, {0, 4096}));
}

TEST_F(PciAperture, CapturedNativeFormatsPassActualCollectorWithoutChangingAbsentGas) {
    hardware::use_captured_profile();
    ASSERT_EQ(hardware::hpet[41], 0U);
    ASSERT_EQ(hardware::fadt.size(), 244U);
    ASSERT_EQ(hardware::fadt[8], 3U);
    ASSERT_EQ(hardware::fadt[221], 128U);
    ASSERT_EQ(hardware::get(hardware::fadt, 224, 8), 0x620U);
    ASSERT_EQ(hardware::get(hardware::functions[0].config, 0x9d, 2), 0U);
    auto result = collect();
    ASSERT_TRUE(result.valid);
    EXPECT_TRUE(result.excludes({0xfed00000, 4096}));
    EXPECT_TRUE(result.excludes({0xfed1c000, 16384}));
    EXPECT_FALSE(result.excludes({hardware::BAR0, hardware::BAR_BYTES}));
    EXPECT_TRUE(aperture::conventional_layout(image(hardware::nic())));
}

TEST_F(PciAperture, HpetFixedBankAcceptsKnownWidthFormsButRefusesScalarOrReservedEncodings) {
    for (uint8_t width : {uint8_t{0}, uint8_t{64}}) {
        hardware::use_captured_profile();
        hardware::hpet[41] = width;
        hardware::checksum(hardware::hpet);
        EXPECT_TRUE(collect().valid) << unsigned(width);
    }
    for (auto [offset, value] : {std::pair{40U, 1U}, {41U, 32U}, {42U, 1U}, {43U, 1U}}) {
        hardware::use_captured_profile();
        hardware::hpet[offset] = static_cast<uint8_t>(value);
        hardware::checksum(hardware::hpet);
        EXPECT_FALSE(collect().valid) << offset;
    }
    for (uint64_t address : {uint64_t{0}, uint64_t{0xfed00001}, UINT64_MAX - 4095}) {
        hardware::use_captured_profile();
        hardware::put(hardware::hpet, 44, address, 8);
        hardware::checksum(hardware::hpet);
        EXPECT_FALSE(collect().valid) << address;
    }
}

TEST_F(PciAperture, GpeByteBlockProtectsItsWholeExtentAcrossPagesAndKeepsScalarGasStrict) {
    hardware::use_captured_profile();
    hardware::fadt[220] = 0;  // Same128-bit block in system memory.
    hardware::put(hardware::fadt, 224, 0x90000ff8, 8);
    hardware::checksum(hardware::fadt);
    auto result = collect();
    ASSERT_TRUE(result.valid);
    EXPECT_TRUE(result.excludes({0x90000000, 4096}));
    EXPECT_TRUE(result.excludes({0x90001000, 4096}));
    EXPECT_FALSE(result.excludes({0x90002000, 4096}));
    hardware::fadt[117] = 128;  // Reset remains a scalar GAS.
    hardware::checksum(hardware::fadt);
    EXPECT_FALSE(collect().valid);
}

TEST_F(PciAperture, GpeBlockRejectsMismatchedLengthOffsetsAccessUnknownSpaceAndAbsentPayload) {
    for (auto [offset, value] : {std::pair{92U, 15U}, {92U, 0U}, {220U, 2U}, {221U, 64U}, {222U, 1U}, {223U, 2U}}) {
        hardware::use_captured_profile();
        hardware::fadt[offset] = static_cast<uint8_t>(value);
        hardware::checksum(hardware::fadt);
        EXPECT_FALSE(collect().valid) << offset;
    }
    for (uint64_t address : {uint64_t{0}, uint64_t{0x621}, uint64_t{0xfff8}}) {
        hardware::use_captured_profile();
        hardware::put(hardware::fadt, 224, address, 8);
        hardware::checksum(hardware::fadt);
        EXPECT_FALSE(collect().valid) << address;
    }
    hardware::use_captured_profile();
    hardware::fadt[220] = 0;
    hardware::put(hardware::fadt, 224, UINT64_MAX - 7, 8);
    hardware::checksum(hardware::fadt);
    EXPECT_FALSE(collect().valid);
    hardware::use_captured_profile();
    hardware::fadt[161] = 8;  // Absent PM1b GAS still requires an all-zero payload.
    hardware::checksum(hardware::fadt);
    EXPECT_FALSE(collect().valid);
}

TEST_F(PciAperture, SmmAbsentVariantRequiresTheExactKnownZeroPair) {
    hardware::use_captured_profile();
    ASSERT_TRUE(collect().valid);
    for (auto [smram, extended] : {std::pair{0U, 1U}, {0U, 0x38U}, {1U, 0U}, {0x80U, 0U}, {0x0aU, 1U}}) {
        hardware::use_captured_profile();
        hardware::put(hardware::functions[0].config, 0x9d, smram, 1);
        hardware::put(hardware::functions[0].config, 0x9e, extended, 1);
        EXPECT_FALSE(collect().valid) << smram << ':' << extended;
    }
}

TEST_F(PciAperture, CapturedRciepVersionOneAndTwoHaveNoLinkGroup) {
    auto from_bytes = [](std::span<const uint8_t> bytes) {
        aperture::Image result;
        for (size_t i = 0; i < result.words.size(); ++i) result.words[i] = static_cast<uint32_t>(hardware::get(bytes, i * 4, 4));
        return result;
    };
    ASSERT_TRUE(aperture::conventional_layout(from_bytes(hardware::captured_profile::NIC)));
    ASSERT_TRUE(aperture::conventional_layout(from_bytes(hardware::captured_profile::XHCI)));
    auto config = hardware::captured_profile::NIC;
    hardware::put(config, 0xe1, 0xe8, 1);
    hardware::put(config, 0xe8, 1, 2);
    EXPECT_FALSE(aperture::conventional_layout(from_bytes(config)));  // DeviceCtl/Status overlap.
    config = hardware::captured_profile::XHCI;
    hardware::put(config, 0xa1, 0xb8, 1);
    hardware::put(config, 0xb8, 1, 2);
    EXPECT_FALSE(aperture::conventional_layout(from_bytes(config)));  // The v2 hole remains owned by the extent.
    config = hardware::captured_profile::NIC;
    hardware::put(config, 0xe8, 0x8000, 2);
    EXPECT_FALSE(aperture::conventional_layout(from_bytes(config)));  // FLR remains a forbidden strobe.
    config = hardware::captured_profile::NIC;
    hardware::put(config, 0xd2, 0x280, 2);
    EXPECT_FALSE(aperture::conventional_layout(from_bytes(config)));  // MSI reserved control remains refused.
}

TEST_F(PciAperture, RciepExtentEndpointsRejectTruncationAndUnprovenVersions) {
    for (auto [version, start] : {std::pair{1U, 0xf4U}, {2U, 0xd4U}}) {
        hardware::nic().config.fill(0);
        hardware::put(hardware::nic().config, 6, 0x10, 2);
        hardware::put(hardware::nic().config, 0x34, start, 1);
        hardware::put(hardware::nic().config, start, 0x10, 2);
        hardware::put(hardware::nic().config, start + 2, 0x90 | version, 2);
        ASSERT_TRUE(aperture::conventional_layout(image(hardware::nic()))) << version;
        hardware::put(hardware::nic().config, start + 2, 0x93, 2);
        EXPECT_FALSE(aperture::conventional_layout(image(hardware::nic())));
        hardware::nic().config.fill(0);
        hardware::put(hardware::nic().config, 6, 0x10, 2);
        hardware::put(hardware::nic().config, 0x34, start + 4, 1);
        hardware::put(hardware::nic().config, start + 4, 0x10, 2);
        hardware::put(hardware::nic().config, start + 6, 0x90 | version, 2);
        EXPECT_FALSE(aperture::conventional_layout(image(hardware::nic()))) << version;
    }
}
