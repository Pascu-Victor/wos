#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <map>
#include <platform/mm/mmio_cache_observation.hpp>
#include <set>
#include <utility>
#include <vector>

namespace {
namespace arch = ker::mod::mm::cache_arch;
namespace model = ker::mod::mm::mmio_cache::detail;
using ker::mod::mm::mmio_cache::Status;
using model::Cpuid;
using model::Observation;
using model::RawCpu;
using Key = std::pair<uint32_t, uint32_t>;
constexpr uint64_t GIB = uint64_t{1} << 30;
constexpr uint64_t VALID = 0x800;
constexpr uint64_t PAT = 0x0000010500070406ULL;
constexpr std::array<uint32_t, 11> FIXED{0x250, 0x258, 0x259, 0x268, 0x269, 0x26a, 0x26b, 0x26c, 0x26d, 0x26e, 0x26f};

struct CpuIo {
    std::map<Key, Cpuid> leaves;
    std::map<Key, Cpuid> second_leaves;
    std::map<Key, size_t> cpuid_counts;
    std::vector<Key> cpuid_calls;
    std::vector<Key> unexpected_leaves;
    std::map<uint32_t, std::vector<uint64_t>> registers;
    std::map<uint32_t, size_t> read_counts;
    std::map<uint32_t, size_t> fault_on;
    std::vector<uint32_t> reads;
    std::vector<uint32_t> unexpected_registers;
    std::vector<uint64_t> cr0_values{0x80010011};
    std::vector<uint64_t> cr4_values{0x20};
    std::vector<bool> idt_values{true};
    size_t cr0_reads{};
    size_t cr4_reads{};
    size_t idt_reads{};

    auto cpuid(uint32_t leaf, uint32_t subleaf) -> Cpuid {
        Key const KEY{leaf, subleaf};
        cpuid_calls.push_back(KEY);
        size_t const COUNT = ++cpuid_counts[KEY];
        auto const FOUND = leaves.find(KEY);
        if (FOUND == leaves.end()) {
            unexpected_leaves.push_back(KEY);
            return {};
        }
        auto const SECOND = second_leaves.find(KEY);
        return COUNT > 1 && SECOND != second_leaves.end() ? SECOND->second : FOUND->second;
    }

    auto read_msr(uint32_t index, uint64_t& output) -> bool {
        reads.push_back(index);
        size_t const COUNT = ++read_counts[index];
        auto const FOUND = registers.find(index);
        auto const FAULT = fault_on.find(index);
        if (FOUND == registers.end() || FOUND->second.empty() || (FAULT != fault_on.end() && FAULT->second == COUNT)) {
            if (FOUND == registers.end()) unexpected_registers.push_back(index);
            output = 0xdeaddeaddeaddeadULL;  // Failed output is not a value observation.
            return false;
        }
        output = FOUND->second[std::min(COUNT - 1, FOUND->second.size() - 1)];
        return true;
    }

    auto read_cr0() -> uint64_t { return cr0_values[std::min(cr0_reads++, cr0_values.size() - 1)]; }
    auto read_cr4() -> uint64_t { return cr4_values[std::min(cr4_reads++, cr4_values.size() - 1)]; }
    auto local_idt_ready() -> bool { return idt_values[std::min(idt_reads++, idt_values.size() - 1)]; }
};

auto io_for(arch::Vendor vendor = arch::Vendor::intel) -> CpuIo {
    CpuIo io;
    bool const AMD = vendor == arch::Vendor::amd;
    io.leaves[{0, 0}] = AMD ? Cpuid{7, 0x68747541, 0x444d4163, 0x69746e65} : Cpuid{7, 0x756e6547, 0x6c65746e, 0x49656e69};
    io.leaves[{1, 0}] = {AMD ? 0x00a00f10U : 0x000906e9U, 0x02000000, 0, (1U << 5) | (1U << 12) | (1U << 16)};
    io.leaves[{7, 0}] = {};
    io.leaves[{0x80000000U, 0}] = {0x80000008U, 0, 0, 0};
    io.leaves[{0x80000001U, 0}] = {0, 0, 0, 1U << 29};
    io.leaves[{0x80000008U, 0}] = {36, 0, 0, 0};
    io.registers[0x277] = {PAT};
    io.registers[0xfe] = {0x402};
    io.registers[0x2ff] = {AMD ? 0x800U : 0x806U};
    io.registers[0x201] = {0};
    io.registers[0x203] = {0};
    if (AMD) {
        io.registers[0xc0010010U] = {0};
        io.registers[0xc0010113U] = {0};
    }
    return io;
}

void add_variable(CpuIo& io, size_t index, uint64_t start, uint64_t size, uint8_t type = 0) {
    uint32_t const BASE = 0x200 + static_cast<uint32_t>(2 * index);
    io.registers[BASE] = {start | type};
    io.registers[BASE + 1] = {((uint64_t{1} << 36) - size) | VALID};
}

auto rich_intel() -> CpuIo {
    auto io = io_for();
    io.leaves[{7, 0}].ecx = 1U << 13;
    io.registers[0x982] = {1};  // Locked but disabled is not active TME.
    io.registers[0xfe] = {0x8d0a};
    io.registers[0x2ff] = {0xc06};
    for (auto index : FIXED) io.registers[index] = {0x0606060606060606ULL};
    for (size_t i = 0; i < 10; ++i) io.registers[0x201 + static_cast<uint32_t>(2 * i)] = {0};
    add_variable(io, 0, 0, 0x200000);
    io.registers[0x1f3] = {0xfff00800};
    io.registers[0x1f2] = {0x30000006};
    io.registers[0x1401] = {((uint64_t{1} << 36) - 0x2000000) | VALID};
    io.registers[0x1400] = {4 * GIB | 8};
    return io;
}

auto rich_amd() -> CpuIo {
    auto io = io_for(arch::Vendor::amd);
    io.leaves[{0x80000000U, 0}].eax = 0x8000001fU;
    io.leaves[{0x8000001fU, 0}] = {2, 47, 0, 0};
    io.registers[0xc0010131U] = {0};
    io.registers[0xfe] = {0x508};
    io.registers[0x2ff] = {0xc00};
    for (auto index : FIXED) io.registers[index] = {0x0606060606060606ULL};
    for (size_t i = 0; i < 8; ++i) io.registers[0x201 + static_cast<uint32_t>(2 * i)] = {0};
    add_variable(io, 0, 0, 2 * GIB, 6);
    io.registers[0xc0010010U] = {(uint64_t{1} << 18) | (uint64_t{1} << 20) | (uint64_t{1} << 21) | (uint64_t{1} << 22)};
    io.registers[0xc001001aU] = {3 * GIB};
    io.registers[0xc001001dU] = {16 * GIB};
    io.registers[0xc0010017U] = {((uint64_t{1} << 36) - 0x200000) | VALID};
    io.registers[0xc0010016U] = {32 * GIB};
    io.registers[0xc0010019U] = {0};
    io.registers[0xc0010113U] = {((uint64_t{1} << 36) - 0x20000) | 3};
    io.registers[0xc0010112U] = {0x30000000};
    return io;
}

void expect_known_reads(const CpuIo& io) {
    EXPECT_TRUE(io.unexpected_leaves.empty());
    EXPECT_TRUE(io.unexpected_registers.empty());
}

auto collect(CpuIo& io) -> RawCpu {
    RawCpu raw{};
    EXPECT_EQ(model::collect_registers(io, raw), Status::ready);
    expect_known_reads(io);
    return raw;
}

auto cpu_policy(const RawCpu& raw) -> model::CpuPolicy {
    model::CpuPolicy policy{};
    EXPECT_EQ(arch::decode_mtrrs(raw.mtrr, policy.mtrr), arch::Status::ok);
    policy.pat = raw.pat;
    policy.cache_cr0 = raw.cr0;
    policy.cache_cr4 = raw.cr4;
    if (raw.mtrr.vendor == arch::Vendor::amd) {
        EXPECT_EQ(model::decode_amd(raw.amd, policy.mtrr, policy.amd), Status::ready);
        EXPECT_EQ(model::decode_amd_protected(raw.amd_protected, raw.mtrr.physical_bits, policy.protected_ranges), Status::ready);
    } else {
        EXPECT_EQ(model::decode_intel_protected(raw.intel_protected, raw.mtrr.physical_bits, policy.protected_ranges), Status::ready);
    }
    return policy;
}

TEST(MmioCacheObservation, IntelMinimalCaptureAndRevalidationUseOnlyTheExactFiniteMap) {
    auto io = io_for();
    auto const RAW = collect(io);
    EXPECT_EQ(RAW.mtrr.vendor, arch::Vendor::intel);
    EXPECT_EQ(RAW.mtrr.physical_bits, 36U);
    EXPECT_EQ(RAW.mtrr.observed_masks, 3U);
    EXPECT_EQ(RAW.mtrr.observed_bases, 0U);
    EXPECT_FALSE(RAW.mtrr.observed_fixed);
    EXPECT_EQ(RAW.failed_msr, UINT32_MAX);
    constexpr std::array<uint32_t, 10> EXPECTED{0x277, 0xfe, 0x2ff, 0x201, 0x203, 0x277, 0xfe, 0x2ff, 0x201, 0x203};
    EXPECT_EQ(io.reads, (std::vector<uint32_t>(EXPECTED.begin(), EXPECTED.end())));
    EXPECT_EQ(RAW.tme_activate.observation, Observation::dormant);
    EXPECT_EQ(RAW.amd.syscfg.observation, Observation::dormant);
    EXPECT_EQ(model::qualify_cpu_uc(cpu_policy(RAW), 0xfee00000, 4096), Status::ready);
}

TEST(MmioCacheObservation, EveryFeatureAndTheLocalIdtGatePrecedeAllOptionalReads) {
    for (unsigned bit : {5U, 12U, 16U}) {
        auto io = io_for();
        io.leaves[{1, 0}].edx &= ~(uint32_t{1} << bit);
        RawCpu raw{};
        EXPECT_EQ(model::collect_registers(io, raw), Status::missing_features);
        EXPECT_TRUE(io.reads.empty());
    }
    auto io = io_for();
    io.idt_values = {false};
    RawCpu raw{};
    EXPECT_EQ(model::collect_registers(io, raw), Status::local_idt_unavailable);
    EXPECT_TRUE(io.reads.empty());
    io = io_for();
    io.leaves[{0x80000001U, 0}].edx = 0;
    EXPECT_EQ(model::collect_registers(io, raw), Status::missing_features);
    EXPECT_TRUE(io.reads.empty());
    io = io_for();
    io.leaves[{0, 0}].ebx = 0;
    EXPECT_EQ(model::collect_registers(io, raw), Status::unsupported_vendor);
    EXPECT_TRUE(io.reads.empty());
    io = io_for();
    io.leaves[{0, 0}].eax = 0;
    EXPECT_EQ(model::collect_registers(io, raw), Status::missing_features);
    EXPECT_TRUE(io.reads.empty());
    EXPECT_EQ(io.cpuid_counts[(Key{1, 0})], 0U);
    io = io_for();
    io.leaves[{0x80000000U, 0}].eax = 0x80000000U;
    EXPECT_EQ(model::collect_registers(io, raw), Status::missing_features);
    EXPECT_TRUE(io.reads.empty());
    EXPECT_EQ(io.cpuid_counts[(Key{0x80000001U, 0})], 0U);
}

TEST(MmioCacheObservation, ExplicitLeafSevenSubleafZeroGatesTmeWithoutGuessingOtherLeaves) {
    auto io = io_for();
    io.leaves[{7, 0}].ecx = 1U << 13;
    io.registers[0x982] = {0};
    auto const RAW = collect(io);
    EXPECT_TRUE(RAW.tme_activate.observed());
    EXPECT_EQ(io.cpuid_counts[(Key{7, 0})], 2U);
    for (auto [leaf, subleaf] : io.cpuid_calls) EXPECT_EQ(subleaf, 0U) << leaf;
    io = io_for();
    io.leaves[{0, 0}].eax = 1;
    io.leaves.erase(Key{7, 0});
    auto const LEGACY = collect(io);
    EXPECT_EQ(LEGACY.tme_activate.observation, Observation::dormant);
    EXPECT_EQ(io.read_counts[0x982], 0U);
}

TEST(MmioCacheObservation, IntelFallbackWidthAndAmdRequiredWidthAreDistinct) {
    auto io = io_for();
    io.leaves[{0x80000000U, 0}].eax = 0x80000001U;
    io.leaves.erase(Key{0x80000008U, 0});
    EXPECT_EQ(collect(io).mtrr.physical_bits, 36U);
    io = io_for(arch::Vendor::amd);
    io.leaves[{0x80000000U, 0}].eax = 0x80000001U;
    RawCpu raw{};
    EXPECT_EQ(model::collect_registers(io, raw), Status::needs_address_geometry);
    EXPECT_TRUE(io.reads.empty());
    for (uint32_t bits : {0U, 31U, 53U, 255U}) {
        io = io_for();
        io.leaves[{0x80000008U, 0}].eax = bits;
        EXPECT_EQ(model::collect_registers(io, raw), Status::invalid_geometry);
        EXPECT_TRUE(io.reads.empty());
    }
}

TEST(MmioCacheObservation, InvalidControlsAndPatRefuseBeforeLaterRegisters) {
    RawCpu raw{};
    for (uint64_t cr0 : {uint64_t{1}, uint64_t{0x80000000}, uint64_t{0xc0000001}, uint64_t{0xa0000001}}) {
        auto io = io_for();
        io.cr0_values = {cr0};
        EXPECT_EQ(model::collect_registers(io, raw), Status::unsupported_mode);
        EXPECT_TRUE(io.reads.empty());
    }
    for (uint64_t cr4 : {uint64_t{0}, uint64_t{0xa0}, uint64_t{0x1020}, uint64_t{0x20020}}) {
        auto io = io_for();
        io.cr4_values = {cr4};
        EXPECT_EQ(model::collect_registers(io, raw), Status::unsupported_mode);
        EXPECT_TRUE(io.reads.empty());
    }
    auto io = io_for();
    io.registers[0x277] = {2};
    EXPECT_EQ(model::collect_registers(io, raw), Status::invalid_type);
    EXPECT_EQ(io.reads, (std::vector<uint32_t>{0x277}));
    io = io_for();
    io.registers[0x277] = {0x80};
    EXPECT_EQ(model::collect_registers(io, raw), Status::invalid_reserved);
    EXPECT_EQ(io.read_counts[0xfe], 0U);
}

TEST(MmioCacheObservation, UnsupportedCountsAndCapabilityBitsCannotCauseGuessedReads) {
    for (auto vendor : {arch::Vendor::intel, arch::Vendor::amd}) {
        uint64_t const FIRST_UNSUPPORTED = vendor == arch::Vendor::intel ? 11 : 9;
        for (uint64_t count : {FIRST_UNSUPPORTED, uint64_t{40}, uint64_t{59}, uint64_t{127}, uint64_t{255}}) {
            auto io = io_for(vendor);
            io.registers[0xfe] = {0x500 | count};
            io.registers[0x2ff] = {0xc00};
            RawCpu raw{};
            EXPECT_EQ(model::collect_registers(io, raw), Status::unsupported_layout);
            EXPECT_EQ(io.reads, (std::vector<uint32_t>{0x277, 0xfe, 0x2ff}));
            expect_known_reads(io);
        }
        auto io = io_for(vendor);
        io.registers[0xfe] = {0x602};
        RawCpu raw{};
        EXPECT_EQ(model::collect_registers(io, raw), Status::unsupported_capability);
        EXPECT_EQ(io.read_counts[0x201], 0U);
    }
}

TEST(MmioCacheObservation, DisabledTypingSkipsAllFixedAndVariableRegistersEvenWithUnprovenCount) {
    auto io = io_for();
    io.registers[0xfe] = {0x5ff};
    io.registers[0x2ff] = {0x407};
    io.registers.erase(0x201);
    io.registers.erase(0x203);
    auto const RAW = collect(io);
    EXPECT_EQ(RAW.mtrr.observed_masks, 0U);
    EXPECT_EQ(RAW.mtrr.observed_bases, 0U);
    EXPECT_FALSE(RAW.mtrr.observed_fixed);
    EXPECT_EQ(io.reads, (std::vector<uint32_t>{0x277, 0xfe, 0x2ff, 0x277, 0xfe, 0x2ff}));
}

TEST(MmioCacheObservation, EnabledMasksPrecedeOnlyTheirValidBasesAndFixedNeedsBothFlags) {
    auto io = io_for();
    io.registers[0x2ff] = {0xc06};  // FE without FIX stays dormant.
    add_variable(io, 1, 0x4000, 4096, 4);
    auto const RAW = collect(io);
    EXPECT_FALSE(RAW.mtrr.observed_fixed);
    EXPECT_EQ(RAW.mtrr.observed_bases, 2U);
    EXPECT_EQ(io.reads, (std::vector<uint32_t>{0x277, 0xfe, 0x2ff, 0x201, 0x203, 0x202, 0x277, 0xfe, 0x2ff, 0x201, 0x203, 0x202}));
    EXPECT_EQ(io.read_counts[0x200], 0U);
}

TEST(MmioCacheObservation, RichIntelCapturesTheCompleteKnownMapAndRealProtectedModel) {
    auto io = rich_intel();
    auto const RAW = collect(io);
    EXPECT_EQ(RAW.mtrr.observed_masks, 0x3ffU);
    EXPECT_EQ(RAW.mtrr.observed_bases, 1U);
    EXPECT_TRUE(RAW.mtrr.observed_fixed);
    EXPECT_TRUE(RAW.intel_protected.smrr_base.observed());
    EXPECT_TRUE(RAW.intel_protected.seamrr_base.observed());
    auto const POLICY = cpu_policy(RAW);
    EXPECT_EQ(POLICY.protected_ranges.count, 2U);
    EXPECT_EQ(model::qualify_cpu_uc(POLICY, 0xd0000000, 4096), Status::ready);
    EXPECT_EQ(model::qualify_cpu_uc(POLICY, 0x30000000, 4096), Status::protected_range);
    EXPECT_EQ(model::qualify_cpu_uc(POLICY, 4 * GIB, 4096), Status::protected_range);
    std::set<uint32_t> const UNIQUE(io.reads.begin(), io.reads.end());
    for (auto index : UNIQUE) EXPECT_EQ(io.read_counts[index], 2U) << index;
    EXPECT_EQ(io.read_counts[0x214], 0U);
    EXPECT_EQ(io.read_counts[0x1f4], 0U);
}

TEST(MmioCacheObservation, IntelTmeAndUnselectedPrmrrCannotBecomeImplicitAbsence) {
    auto io = io_for();
    io.leaves[{7, 0}].ecx = 1U << 13;
    io.registers[0x982] = {2};
    RawCpu raw{};
    EXPECT_EQ(model::collect_registers(io, raw), Status::needs_address_geometry);
    EXPECT_TRUE(raw.tme_activate.observed());
    EXPECT_EQ(io.read_counts[0x201], 0U);
    io = io_for();
    io.registers[0xfe] = {0x1402};
    EXPECT_EQ(model::collect_registers(io, raw), Status::protected_ranges_unobservable);
    EXPECT_EQ(io.read_counts[0x1f4], 0U);
    EXPECT_EQ(io.read_counts[0x2f4], 0U);
    EXPECT_EQ(io.read_counts[0x201], 0U);
}

TEST(MmioCacheObservation, AdvertisedSgxRefusesWithoutPrmrrOrAnyGuessedEpcEnumeration) {
    auto io = io_for();
    io.leaves[{7, 0}].ebx |= uint32_t{1} << 2;
    RawCpu raw{};
    EXPECT_EQ(model::collect_registers(io, raw), Status::protected_ranges_unobservable);
    EXPECT_EQ(raw.mtrr.capability & 0x1000U, 0U);
    EXPECT_EQ(io.reads, (std::vector<uint32_t>{0x277, 0xfe, 0x2ff}));
    uint32_t failed = 0;
    EXPECT_EQ(model::revalidate_registers(io, raw, failed), Status::protected_ranges_unobservable);
    EXPECT_EQ(io.read_counts[0x201], 0U);
    EXPECT_EQ(io.read_counts[0x1f4], 0U);
    EXPECT_EQ(io.read_counts[0x2f4], 0U);
    EXPECT_EQ(io.read_counts[0x2a0], 0U);
    EXPECT_EQ(io.cpuid_counts[(Key{0x12, 0})], 0U);
    EXPECT_EQ(io.cpuid_counts[(Key{0x12, 2})], 0U);
    expect_known_reads(io);
}

TEST(MmioCacheObservation, InvalidProtectionMasksAreObservedButTheirDormantBasesAreNeverRead) {
    auto io = rich_intel();
    io.registers[0x1f3] = {0xdead0000};
    io.registers[0x1401] = {0};
    io.registers.erase(0x1f2);
    io.registers.erase(0x1400);
    auto const RAW = collect(io);
    EXPECT_TRUE(RAW.intel_protected.smrr_mask.observed());
    EXPECT_TRUE(RAW.intel_protected.seamrr_mask.observed());
    EXPECT_EQ(RAW.intel_protected.smrr_base.observation, Observation::dormant);
    EXPECT_EQ(RAW.intel_protected.seamrr_base.observation, Observation::dormant);
}

TEST(MmioCacheObservation, AmdMinimalCaptureKeepsZeroObservedAndDormantStatesDistinct) {
    auto io = io_for(arch::Vendor::amd);
    auto const RAW = collect(io);
    EXPECT_TRUE(RAW.amd.syscfg.observed());
    EXPECT_EQ(RAW.amd.syscfg.value, 0U);
    EXPECT_TRUE(RAW.amd_protected.smm_mask.observed());
    EXPECT_EQ(RAW.amd.top_mem.observation, Observation::dormant);
    EXPECT_EQ(RAW.amd.top_mem2.observation, Observation::dormant);
    EXPECT_EQ(RAW.amd.iorr_mask[0].observation, Observation::dormant);
    EXPECT_EQ(RAW.amd.sev_status.observation, Observation::dormant);
    EXPECT_EQ(RAW.amd_protected.smm_base.observation, Observation::dormant);
    EXPECT_EQ(model::qualify_cpu_uc(cpu_policy(RAW), 0xd0000000, 4096), Status::ready);
}

TEST(MmioCacheObservation, RichAmdCapturesRoutingEncryptionAndProtectionWithARealPositiveIoHole) {
    auto io = rich_amd();
    auto const RAW = collect(io);
    EXPECT_EQ(RAW.mtrr.observed_masks, 0xffU);
    EXPECT_EQ(RAW.mtrr.observed_bases, 1U);
    EXPECT_EQ(RAW.amd.encryption_eax, 2U);
    EXPECT_EQ(RAW.amd.encryption_ebx, 47U);
    EXPECT_TRUE(RAW.amd.sev_status.observed());
    EXPECT_TRUE(RAW.amd.top_mem.observed());
    EXPECT_TRUE(RAW.amd.top_mem2.observed());
    EXPECT_TRUE(RAW.amd.iorr_base[0].observed());
    EXPECT_EQ(RAW.amd.iorr_base[1].observation, Observation::dormant);
    EXPECT_TRUE(RAW.amd_protected.smm_base.observed());
    auto const POLICY = cpu_policy(RAW);
    EXPECT_EQ(model::qualify_cpu_uc(POLICY, 3 * GIB + GIB / 4, 4096), Status::ready);
    EXPECT_EQ(model::qualify_cpu_uc(POLICY, 0x30000000, 4096), Status::protected_range);
    EXPECT_EQ(model::qualify_cpu_uc(POLICY, 4 * GIB, 4096), Status::routed_to_memory);
    EXPECT_EQ(io.read_counts[0xc0010018U], 0U);
    EXPECT_EQ(io.read_counts[0x211], 0U);
}

TEST(MmioCacheObservation, AmdRoutingAndSmmPredicatesRemainActiveWhenMtrrTypingIsDisabled) {
    auto io = rich_amd();
    io.registers[0x2ff] = {0};
    auto const RAW = collect(io);
    EXPECT_EQ(RAW.mtrr.observed_masks, 0U);
    EXPECT_FALSE(RAW.mtrr.observed_fixed);
    EXPECT_TRUE(RAW.amd.top_mem.observed());
    EXPECT_TRUE(RAW.amd.top_mem2.observed());
    EXPECT_TRUE(RAW.amd.iorr_mask[0].observed());
    EXPECT_TRUE(RAW.amd_protected.smm_base.observed());
    EXPECT_EQ(io.read_counts[0x250], 0U);
    EXPECT_EQ(io.read_counts[0x201], 0U);
    EXPECT_EQ(model::qualify_cpu_uc(cpu_policy(RAW), 3 * GIB + GIB / 4, 4096), Status::ready);
}

TEST(MmioCacheObservation, AmdTopAndSevPredicatesAreIndependent) {
    auto io = io_for(arch::Vendor::amd);
    io.registers[0xc0010010U] = {uint64_t{1} << 21};
    io.registers[0xc001001dU] = {16 * GIB};
    io.leaves[{0x80000000U, 0}].eax = 0x8000001fU;
    io.leaves[{0x8000001fU, 0}] = {1, 0, 0, 0};  // SME advertisement is not SEV.
    auto const RAW = collect(io);
    EXPECT_TRUE(RAW.amd.top_mem2.observed());
    EXPECT_EQ(RAW.amd.top_mem.observation, Observation::dormant);
    EXPECT_EQ(RAW.amd.iorr_mask[0].observation, Observation::dormant);
    EXPECT_EQ(RAW.amd.sev_status.observation, Observation::dormant);
    EXPECT_EQ(io.read_counts[0xc001001aU], 0U);
    EXPECT_EQ(io.read_counts[0xc0010131U], 0U);
    EXPECT_EQ(model::qualify_cpu_uc(cpu_policy(RAW), 32 * GIB, 4096), Status::ready);
}

TEST(MmioCacheObservation, AmdOpaqueAndEncryptedStateRemainsRawUntilTheActualModelRefusesIt) {
    for (uint64_t syscfg : {uint64_t{1}, uint64_t{1} << 23}) {
        auto io = io_for(arch::Vendor::amd);
        io.registers[0xc0010010U] = {syscfg};
        auto const RAW = collect(io);
        arch::MtrrPolicy mtrr{};
        ASSERT_EQ(arch::decode_mtrrs(RAW.mtrr, mtrr), arch::Status::ok);
        model::AmdPolicy output{};
        EXPECT_EQ(model::decode_amd(RAW.amd, mtrr, output), syscfg == 1 ? Status::unknown_extension : Status::needs_address_geometry);
    }
    auto io = rich_amd();
    io.registers[0xc0010131U] = {1};
    auto const RAW = collect(io);
    arch::MtrrPolicy mtrr{};
    ASSERT_EQ(arch::decode_mtrrs(RAW.mtrr, mtrr), arch::Status::ok);
    model::AmdPolicy output{};
    EXPECT_EQ(model::decode_amd(RAW.amd, mtrr, output), Status::needs_address_geometry);
}

TEST(MmioCacheObservation, EveryObservedMsrFaultHasAnExactIndexAndNeverBecomesZeroAbsence) {
    for (auto prototype : {rich_intel(), rich_amd()}) {
        auto reference = prototype;
        static_cast<void>(collect(reference));
        std::set<uint32_t> const INDICES(reference.reads.begin(), reference.reads.end());
        for (auto index : INDICES) {
            for (size_t attempt : {size_t{1}, size_t{2}}) {
                auto io = prototype;
                io.fault_on[index] = attempt;
                RawCpu raw{};
                EXPECT_EQ(model::collect_registers(io, raw), Status::read_fault) << index << ':' << attempt;
                EXPECT_EQ(raw.failed_msr, index);
                expect_known_reads(io);
            }
        }
    }
    auto io = io_for(arch::Vendor::amd);
    io.fault_on[0xc0010010U] = 1;
    RawCpu raw{};
    EXPECT_EQ(model::collect_registers(io, raw), Status::read_fault);
    EXPECT_EQ(raw.amd.syscfg.observation, Observation::fault);
    EXPECT_NE(raw.amd.syscfg.value, 0xdeaddeaddeaddeadULL);
}

TEST(MmioCacheObservation, EveryObservedMsrIsRecheckedAndAnyValueDriftHasItsExactIndex) {
    for (auto prototype : {rich_intel(), rich_amd()}) {
        auto reference = prototype;
        static_cast<void>(collect(reference));
        std::set<uint32_t> const INDICES(reference.reads.begin(), reference.reads.end());
        for (auto index : INDICES) {
            auto io = prototype;
            uint64_t const FIRST = io.registers.at(index)[0];
            io.registers[index] = {FIRST, FIRST ^ 1U};
            RawCpu raw{};
            EXPECT_EQ(model::collect_registers(io, raw), Status::state_drift) << index;
            EXPECT_EQ(raw.failed_msr, index);
            expect_known_reads(io);
        }
    }
}

TEST(MmioCacheObservation, MaskDriftStopsBeforeAnyNewlyActivatedDormantBase) {
    for (uint32_t mask : {0x201U, 0x1f3U, 0x1401U, 0xc0010017U, 0xc0010113U}) {
        auto io = mask >= 0xc0000000U ? rich_amd() : rich_intel();
        uint32_t const BASE = mask - 1;
        uint64_t const ACTIVE = mask == 0xc0010113U ? 2 : VALID;
        io.registers[mask] = {0, ACTIVE};
        io.registers.erase(BASE);
        RawCpu raw{};
        EXPECT_EQ(model::collect_registers(io, raw), Status::state_drift) << mask;
        EXPECT_EQ(raw.failed_msr, mask);
        EXPECT_EQ(io.read_counts[BASE], 0U);
        expect_known_reads(io);
    }
}

TEST(MmioCacheObservation, ActivationControlDriftNeverStartsANewRegisterPlan) {
    auto io = io_for(arch::Vendor::amd);
    io.registers[0xc0010010U] = {0, uint64_t{1} << 20};
    RawCpu raw{};
    EXPECT_EQ(model::collect_registers(io, raw), Status::state_drift);
    EXPECT_EQ(raw.failed_msr, 0xc0010010U);
    EXPECT_EQ(io.read_counts[0xc001001aU], 0U);
    EXPECT_EQ(io.read_counts[0xc0010017U], 0U);
    io = io_for();
    io.registers[0xfe] = {0x402, 0x8c02};
    EXPECT_EQ(model::collect_registers(io, raw), Status::state_drift);
    EXPECT_EQ(raw.failed_msr, 0xfeU);
    EXPECT_EQ(io.read_counts[0x1f3], 0U);
    EXPECT_EQ(io.read_counts[0x1401], 0U);
    io = io_for();
    io.registers[0x2ff] = {0, 0x806};
    EXPECT_EQ(model::collect_registers(io, raw), Status::state_drift);
    EXPECT_EQ(raw.failed_msr, 0x2ffU);
    EXPECT_EQ(io.read_counts[0x201], 0U);
}

TEST(MmioCacheObservation, CpuFeatureControlAndFinalIdtDriftCannotPublishAStableCapture) {
    for (Key key : {Key{0, 0}, Key{1, 0}, Key{7, 0}, Key{0x80000000U, 0}, Key{0x80000001U, 0}, Key{0x80000008U, 0}}) {
        auto io = io_for();
        auto changed = io.leaves.at(key);
        changed.eax ^= 1;
        io.second_leaves[key] = changed;
        RawCpu raw{};
        EXPECT_EQ(model::collect_registers(io, raw), Status::state_drift);
        EXPECT_EQ(raw.failed_msr, UINT32_MAX);
        EXPECT_EQ(io.read_counts[0x277], 1U);
    }
    for (size_t stage : {size_t{1}, size_t{2}}) {
        auto io = io_for();
        io.cr0_values = {0x80010011, 0x80010011, 0x80010011};
        io.cr0_values[stage] ^= uint64_t{1} << 30;
        RawCpu raw{};
        EXPECT_EQ(model::collect_registers(io, raw), Status::state_drift);
        EXPECT_EQ(raw.failed_msr, UINT32_MAX);
    }
    auto io = io_for();
    io.cr4_values = {0x20, 0xa0};
    RawCpu raw{};
    EXPECT_EQ(model::collect_registers(io, raw), Status::state_drift);
    EXPECT_EQ(raw.failed_msr, UINT32_MAX);
    io = io_for();
    io.idt_values = {true, true, false};
    EXPECT_EQ(model::collect_registers(io, raw), Status::local_idt_unavailable);
    EXPECT_EQ(raw.failed_msr, UINT32_MAX);
}

TEST(MmioCacheObservation, LocalTaskSwitchBitsDoNotChangeCachePolicyOrOverwriteRawDiagnostics) {
    auto io = io_for();
    io.cr0_values = {0x80010011, 0x80010019};  // CR0.TS changes independently.
    io.cr4_values = {0x20, 0x40030};           // PSE and OSXSAVE are incidental.
    auto const RAW = collect(io);
    EXPECT_EQ(RAW.cr0, 0x80010011U);
    EXPECT_EQ(RAW.cr4, 0x20U);
    uint32_t failed = 0;
    EXPECT_EQ(model::revalidate_registers(io, RAW, failed), Status::ready);
    EXPECT_EQ(failed, UINT32_MAX);
    EXPECT_EQ(RAW.cr0, 0x80010011U);
    EXPECT_EQ(RAW.cr4, 0x20U);
}

TEST(MmioCacheObservation, CpuidOsxsaveAndOspkeFollowIncidentalOsControlsWithoutHidingStaticDrift) {
    auto io = io_for();
    io.cr4_values = {0x20, 0x440020};
    auto features = io.leaves.at(Key{1, 0});
    features.ecx |= uint32_t{1} << 27;
    io.second_leaves[Key{1, 0}] = features;
    auto structured = io.leaves.at(Key{7, 0});
    structured.ecx |= uint32_t{1} << 4;
    io.second_leaves[Key{7, 0}] = structured;
    auto const RAW = collect(io);
    EXPECT_EQ(RAW.features.ecx & (uint32_t{1} << 27), 0U);
    EXPECT_EQ(RAW.structured.ecx & (uint32_t{1} << 4), 0U);
    uint32_t failed = 0;
    EXPECT_EQ(model::revalidate_registers(io, RAW, failed), Status::ready);
    for (auto key : {Key{1, 0}, Key{7, 0}}) {
        io = io_for();
        auto changed = io.leaves.at(key);
        changed.ecx |= uint32_t{1} << (key.first == 1 ? 26 : 13);
        io.second_leaves[key] = changed;
        RawCpu refused{};
        EXPECT_EQ(model::collect_registers(io, refused), Status::state_drift);
        EXPECT_EQ(refused.failed_msr, UINT32_MAX);
        EXPECT_EQ(io.read_counts[0x982], 0U);
    }
}

TEST(MmioCacheObservation, ExplicitRevalidationRefusesUnprovenCountsAndMissingSavedFields) {
    auto io = io_for();
    auto expected = collect(io);
    io.reads.clear();
    expected.mtrr.capability = 255;
    uint32_t failed = 0;
    EXPECT_EQ(model::revalidate_registers(io, expected, failed), Status::unsupported_layout);
    EXPECT_TRUE(io.reads.empty());
    io = io_for();
    expected = collect(io);
    expected.mtrr.observed_masks = 0;
    io.reads.clear();
    EXPECT_EQ(model::revalidate_registers(io, expected, failed), Status::missing_observation);
    EXPECT_EQ(failed, UINT32_MAX);
    EXPECT_EQ(io.read_counts[0x200], 0U);
}
}  // namespace
