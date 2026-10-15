#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <platform/acpi/numa_topology.hpp>
#include <vector>

namespace numa = ker::mod::acpi::numa;
namespace {
using Bytes = std::vector<uint8_t>;
constexpr uint64_t PAGE = numa::PAGE_BYTES;

void put(Bytes& bytes, size_t at, uint64_t value, size_t count) {
    for (size_t i = 0; i < count; ++i) bytes.at(at + i) = static_cast<uint8_t>(value >> (8 * i));
}
void seal(Bytes& bytes) {
    put(bytes, 4, bytes.size(), 4);
    bytes.at(9) = 0;
    uint8_t sum = 0;
    for (auto value : bytes) sum = static_cast<uint8_t>(sum + value);
    bytes.at(9) = static_cast<uint8_t>(0U - sum);
}
auto table(std::string_view signature, size_t bytes) -> Bytes {
    Bytes out(bytes);
    std::copy(signature.begin(), signature.end(), out.begin());
    out.at(8) = 3;
    seal(out);
    return out;
}
auto srat() -> Bytes {
    auto out = table("SRAT", 48);
    put(out, 36, 1, 4);
    seal(out);
    return out;
}
void cpu(Bytes& bytes, uint32_t apic, uint32_t domain, bool legacy = false, uint32_t flags = 1) {
    size_t const AT = bytes.size();
    bytes.resize(AT + (legacy ? 16 : 24));
    bytes[AT] = legacy ? 0 : 2;
    bytes[AT + 1] = legacy ? 16 : 24;
    if (legacy) {
        bytes[AT + 2] = static_cast<uint8_t>(domain);
        bytes[AT + 3] = static_cast<uint8_t>(apic);
        put(bytes, AT + 4, flags, 4);
        put(bytes, AT + 9, domain >> 8, 3);
    } else {
        put(bytes, AT + 4, domain, 4);
        put(bytes, AT + 8, apic, 4);
        put(bytes, AT + 12, flags, 4);
    }
    seal(bytes);
}
void memory(Bytes& bytes, uint64_t base, uint64_t length, uint32_t domain, uint32_t flags = 1) {
    size_t const AT = bytes.size();
    bytes.resize(AT + 40);
    bytes[AT] = 1;
    bytes[AT + 1] = 40;
    put(bytes, AT + 2, domain, 4);
    put(bytes, AT + 8, base, 8);
    put(bytes, AT + 16, length, 8);
    put(bytes, AT + 28, flags, 4);
    seal(bytes);
}
auto slit(size_t nodes) -> Bytes {
    auto out = table("SLIT", 44 + nodes * nodes);
    put(out, 36, nodes, 8);
    for (size_t i = 0; i < nodes; ++i)
        for (size_t j = 0; j < nodes; ++j) out[44 + i * nodes + j] = i == j ? 10 : 20;
    seal(out);
    return out;
}

class NumaTopology : public testing::Test {
   protected:
    numa::Topology output{};
    numa::Workspace workspace{};
    std::vector<numa::CpuInput> cpus{{0, 17}, {1, 0x124}};
    std::vector<numa::RamRange> ram{{0x100000, 2 * PAGE}, {0x200000, 2 * PAGE}};
    Bytes affinity = srat();
    Bytes distances{};

    NumaTopology() {
        // Deliberately reversed raw domains and two CPU encodings.
        cpu(affinity, 17, 30, true);
        cpu(affinity, 0x124, 10);
        memory(affinity, ram[0].base, ram[0].length, 30);
        memory(affinity, ram[1].base, ram[1].length, 10);
        output.node_count = 7;
        output.nodes[0].proximity_id = 0xfeed;
    }
    auto parse() -> numa::Result { return numa::parse(affinity, distances, cpus, ram, output, workspace); }
    void refuses(numa::Error error) {
        auto const BEFORE = output;
        EXPECT_EQ(parse().error, error);
        EXPECT_EQ(output, BEFORE);
    }
};

TEST_F(NumaTopology, SortsSparseDomainsAndUsesExplicitDefaultDistances) {
    ASSERT_TRUE(parse().valid());
    EXPECT_EQ(output.provenance, numa::Provenance::SRAT_DEFAULT_DISTANCES);
    ASSERT_EQ(output.node_count, 2);
    EXPECT_EQ(output.nodes[0].proximity_id, 10U);
    EXPECT_EQ(output.nodes[1].proximity_id, 30U);
    EXPECT_EQ(output.cpu_nodes[0], 1);
    EXPECT_EQ(output.cpu_nodes[1], 0);
    EXPECT_TRUE(output.nodes[1].cpus.contains(0));
    EXPECT_FALSE(output.nodes[1].cpus.contains(1));
    EXPECT_EQ(output.cpu_apic_ids[1], 0x124U);
    EXPECT_EQ(output.nodes[0].usable_bytes, 2 * PAGE);
    EXPECT_EQ(output.memory[0].base, 0x100000U);
    EXPECT_EQ(output.memory[0].node, 1);
    EXPECT_EQ(output.distances[0][0], 10);
    EXPECT_EQ(output.distances[0][1], 20);
    EXPECT_EQ(output.cpu_nodes[255], numa::INVALID_NODE);
}

TEST_F(NumaTopology, ReadsSlitByRawProximityIdsNotDenseIdsAndPreservesAsymmetry) {
    distances = slit(31);
    distances[44 + 10 * 31 + 30] = 27;
    distances[44 + 30 * 31 + 10] = 255;
    seal(distances);
    ASSERT_TRUE(parse().valid());
    EXPECT_EQ(output.provenance, numa::Provenance::SRAT_SLIT);
    EXPECT_EQ(output.distances[0][1], 27);
    EXPECT_EQ(output.distances[1][0], 255);
}

TEST_F(NumaTopology, MissingSratIsExplicitUmaEvenWithValidSlit) {
    affinity.clear();
    distances = slit(2);
    ASSERT_TRUE(parse().valid());
    EXPECT_EQ(output.provenance, numa::Provenance::UMA_MISSING_SRAT);
    ASSERT_EQ(output.node_count, 1);
    EXPECT_EQ(output.nodes[0].usable_bytes, 4 * PAGE);
    EXPECT_EQ(output.memory_range_count, 2);
    EXPECT_EQ(output.cpu_nodes[1], 0);
}

TEST_F(NumaTopology, MissingTablesUmaStillValidatesPresentSlit) {
    affinity.clear();
    distances = slit(2);
    distances[44] = 11;
    seal(distances);
    refuses(numa::Error::SLIT_DISTANCE);
}

TEST_F(NumaTopology, SupportsEveryOneOf256CpusWithoutTruncatingMasks) {
    cpus.clear();
    affinity = srat();
    for (uint32_t i = 0; i < 256; ++i) {
        cpus.push_back({i, 0x1000 + i});
        cpu(affinity, 0x1000 + i, 42);
    }
    for (auto const& range : ram) memory(affinity, range.base, range.length, 42);
    ASSERT_TRUE(parse().valid());
    EXPECT_EQ(output.cpu_count, 256);
    for (size_t i = 0; i < 256; ++i) {
        EXPECT_TRUE(output.nodes[0].cpus.contains(i));
        EXPECT_EQ(output.cpu_nodes[i], 0);
    }
    for (auto word : output.nodes[0].cpus.words) EXPECT_EQ(word, UINT64_MAX);
    cpus.push_back({256, 0x2000});
    refuses(numa::Error::CAPACITY);
}

TEST_F(NumaTopology, SupportsFullWidthSparseIdsWithoutSlit) {
    affinity = srat();
    cpu(affinity, 17, UINT32_MAX, true);
    cpu(affinity, 0x124, 5);
    for (auto const& range : ram) memory(affinity, range.base, range.length, 5);
    ASSERT_TRUE(parse().valid());
    EXPECT_EQ(output.nodes[1].proximity_id, UINT32_MAX);
    EXPECT_EQ(output.cpu_nodes[0], 1);
    distances = slit(2);
    refuses(numa::Error::SLIT_COVERAGE);
}

TEST_F(NumaTopology, RetainsMemoryOnlyNodesAndCpusWithoutLocalUsableRam) {
    affinity = srat();
    cpu(affinity, 17, 10);
    cpu(affinity, 0x124, 30);
    for (auto const& range : ram) memory(affinity, range.base, range.length, 30);
    memory(affinity, 0x900000, PAGE, 50);
    ASSERT_TRUE(parse().valid());
    ASSERT_EQ(output.node_count, 3);
    EXPECT_EQ(output.cpu_nodes[0], 0);
    EXPECT_EQ(output.nodes[0].usable_bytes, 0U);
    EXPECT_EQ(output.nodes[1].usable_bytes, 4 * PAGE);
    EXPECT_EQ(output.nodes[2].proximity_id, 50U);
    EXPECT_EQ(output.nodes[2].cpus.words, (std::array<uint64_t, 4>{}));
}

TEST_F(NumaTopology, IntersectsFirmwareRangesWithActualUsableRamAndNeverInventsHoles) {
    ram = {{0x100000, PAGE}, {0x103000, PAGE}};
    affinity = srat();
    cpu(affinity, 17, 10);
    cpu(affinity, 0x124, 10);
    memory(affinity, 0x100000, 4 * PAGE, 10, 7);
    ASSERT_TRUE(parse().valid());
    ASSERT_EQ(output.memory_range_count, 2);
    EXPECT_EQ(output.memory[0].length, PAGE);
    EXPECT_EQ(output.memory[1].base, 0x103000U);
    EXPECT_EQ(output.memory[1].affinity_flags, 7);
    EXPECT_EQ(output.nodes[0].usable_bytes, 2 * PAGE);
}

TEST_F(NumaTopology, CoalescesOnlyAdjacentSameNodeAndFlags) {
    ram = {{0x100000, PAGE}, {0x101000, PAGE}, {0x102000, PAGE}};
    affinity = srat();
    cpu(affinity, 17, 1);
    cpu(affinity, 0x124, 1);
    memory(affinity, 0x100000, 2 * PAGE, 1);
    memory(affinity, 0x102000, PAGE, 1, 3);
    ASSERT_TRUE(parse().valid());
    ASSERT_EQ(output.memory_range_count, 2);
    EXPECT_EQ(output.memory[0].length, 2 * PAGE);
    EXPECT_EQ(output.memory[1].affinity_flags, 3);
}

TEST_F(NumaTopology, OutputOwnsAllValuesAndDoesNotBorrowInputBytes) {
    ASSERT_TRUE(parse().valid());
    auto const SAVED = output;
    std::fill(affinity.begin(), affinity.end(), 0);
    cpus.clear();
    ram.clear();
    EXPECT_EQ(output, SAVED);
}

TEST_F(NumaTopology, RejectsCorruptChecksumSignatureLengthAndEveryTruncatedPrefix) {
    auto const ORIGINAL = affinity;
    affinity[9] ^= 1;
    refuses(numa::Error::CHECKSUM);
    affinity = ORIGINAL;
    affinity[0] = 'X';
    seal(affinity);
    refuses(numa::Error::SIGNATURE);
    for (size_t length = 1; length < ORIGINAL.size(); ++length) {
        affinity.assign(ORIGINAL.begin(), ORIGINAL.begin() + length);
        refuses(numa::Error::LENGTH);
    }
    affinity = ORIGINAL;
    affinity.push_back(0);
    refuses(numa::Error::LENGTH);
}

TEST_F(NumaTopology, RejectsZeroShortAndOverlongRecordLengths) {
    for (uint8_t length : {0, 1, 15, 17, 255}) {
        auto bad = affinity;
        bad[49] = length;
        seal(bad);
        affinity.swap(bad);
        refuses(numa::Error::RECORD_LENGTH);
        affinity.swap(bad);
    }
    affinity.push_back(0xaa);
    seal(affinity);
    refuses(numa::Error::RECORD_LENGTH);
}

TEST_F(NumaTopology, RejectsDuplicateAndMissingActiveCpuAffinity) {
    cpu(affinity, 17, 99);
    refuses(numa::Error::DUPLICATE_CPU);
    affinity = srat();
    cpu(affinity, 17, 30);
    refuses(numa::Error::MISSING_CPU);
}

TEST_F(NumaTopology, IgnoresDisabledCpuAndMemoryPayloadsAndBoundedExtensions) {
    cpu(affinity, 17, 99, false, 0);
    memory(affinity, UINT64_MAX, UINT64_MAX, 99, 0);
    affinity.push_back(200);
    affinity.push_back(3);
    affinity.push_back(0xff);
    seal(affinity);
    ASSERT_TRUE(parse().valid());
    EXPECT_EQ(output.node_count, 2);
}

TEST_F(NumaTopology, RejectsEnabledUnsupportedArchitecturalCpuRatherThanIgnoringIt) {
    for (uint8_t type : {3, 7}) {
        size_t const AT = affinity.size();
        auto const LENGTH = static_cast<uint8_t>(type == 3 ? 18 : 20);
        affinity.resize(AT + LENGTH);
        affinity[AT] = type;
        affinity[AT + 1] = LENGTH;
        put(affinity, AT + (type == 3 ? 10 : 12), 1, 4);
        seal(affinity);
        refuses(numa::Error::UNSUPPORTED_CPU);
        put(affinity, AT + (type == 3 ? 10 : 12), 0, 4);
        seal(affinity);
        ASSERT_TRUE(parse().valid());
    }
    affinity[56] = 1;  // Enabled legacy SAPIC EID cannot be treated as x86 APIC.
    seal(affinity);
    refuses(numa::Error::UNSUPPORTED_CPU);
}

TEST_F(NumaTopology, RejectsReservedHeaderAndEnabledFlagBits) {
    auto const ORIGINAL = affinity;
    for (size_t offset : {36U, 40U, 52U}) {
        affinity = ORIGINAL;
        affinity[offset] ^= offset == 52 ? 2 : 1;
        seal(affinity);
        refuses(numa::Error::RESERVED);
    }
}

TEST_F(NumaTopology, RejectsOverlappingZeroAndOverflowingEnabledMemory) {
    memory(affinity, ram[0].base, PAGE, 99);
    refuses(numa::Error::MEMORY_OVERLAP);
    affinity = srat();
    memory(affinity, 0, 0, 1);
    refuses(numa::Error::MEMORY_RANGE);
    affinity = srat();
    memory(affinity, UINT64_MAX - PAGE, 2 * PAGE, 1);
    refuses(numa::Error::MEMORY_RANGE);
}

TEST_F(NumaTopology, RejectsUncoveredOrFractionalUsableMemoryInsteadOfGuessingNodeZero) {
    ram.push_back({0x300000, PAGE});
    refuses(numa::Error::MEMORY_COVERAGE);
    ram.pop_back();
    affinity = srat();
    cpu(affinity, 17, 0);
    cpu(affinity, 0x124, 0);
    memory(affinity, ram[0].base, PAGE + 1, 0);
    refuses(numa::Error::MEMORY_RANGE);
}

TEST_F(NumaTopology, RejectsInvalidCpuInventoryAndRamInputWithoutModifyingOutput) {
    auto original = cpus;
    cpus[1].logical_index = 0;
    refuses(numa::Error::CPU_INPUT);
    cpus = original;
    cpus[1].apic_id = cpus[0].apic_id;
    refuses(numa::Error::CPU_INPUT);
    cpus = original;
    cpus[1].logical_index = 256;
    refuses(numa::Error::CPU_INPUT);
    cpus = original;
    for (auto value :
         {numa::RamRange{0, 0}, numa::RamRange{1, PAGE}, numa::RamRange{0, PAGE + 1}, numa::RamRange{UINT64_MAX - PAGE + 1, PAGE}}) {
        ram = {value};
        refuses(numa::Error::RAM_INPUT);
    }
    ram = {{0x200000, PAGE}, {0x100000, PAGE}};
    refuses(numa::Error::RAM_INPUT);
    ram = {{0x100000, 2 * PAGE}, {0x101000, PAGE}};
    refuses(numa::Error::RAM_INPUT);
}

TEST_F(NumaTopology, RejectsInvalidSlitDimensionsAndDistances) {
    for (uint64_t count : {uint64_t{0}, uint64_t{3}, UINT64_MAX}) {
        distances = slit(2);
        put(distances, 36, count, 8);
        seal(distances);
        refuses(numa::Error::SLIT_SHAPE);
    }
    for (auto [at, value] : {std::pair{44U, 9U}, {44U, 11U}, {45U, 0U}, {45U, 10U}}) {
        distances = slit(31);
        distances[at] = static_cast<uint8_t>(value);
        seal(distances);
        refuses(numa::Error::SLIT_DISTANCE);
    }
    distances = slit(2);
    refuses(numa::Error::SLIT_COVERAGE);
}

TEST_F(NumaTopology, RejectsNodeAndMemoryRecordCapacityInsteadOfTruncation) {
    affinity = srat();
    cpu(affinity, 17, 0);
    cpu(affinity, 0x124, 0);
    for (auto const& range : ram) memory(affinity, range.base, range.length, 0);
    for (uint32_t i = 1; i < numa::MAX_NODES; ++i) memory(affinity, 0x10000000 + i * PAGE, PAGE, i);
    ASSERT_TRUE(parse().valid());
    EXPECT_EQ(output.node_count, numa::MAX_NODES);
    EXPECT_EQ(output.nodes[numa::MAX_NODES - 1].proximity_id, numa::MAX_NODES - 1);
    memory(affinity, 0x10000000 + numa::MAX_NODES * PAGE, PAGE, numa::MAX_NODES);
    refuses(numa::Error::CAPACITY);
    affinity = srat();
    cpu(affinity, 17, 0);
    cpu(affinity, 0x124, 0);
    for (auto const& range : ram) memory(affinity, range.base, range.length, 0);
    for (size_t i = ram.size(); i < numa::MAX_MEMORY_RANGES; ++i) memory(affinity, 0x10000000 + i * PAGE, PAGE, 0);
    ASSERT_TRUE(parse().valid());
    EXPECT_EQ(workspace.memory_count, numa::MAX_MEMORY_RANGES);
    EXPECT_EQ(output.nodes[0].usable_bytes, 4 * PAGE);
    memory(affinity, 0x10000000 + numa::MAX_MEMORY_RANGES * PAGE, PAGE, 0);
    refuses(numa::Error::CAPACITY);
    affinity = srat();
    for (uint32_t i = 0; i <= numa::MAX_CPUS; ++i) cpu(affinity, 0x1000 + i, 0);
    refuses(numa::Error::CAPACITY);
}

TEST_F(NumaTopology, RejectsNormalizedOutputCapacityAndWorkspaceAlias) {
    affinity.clear();
    ram.clear();
    for (size_t i = 0; i < numa::MAX_MEMORY_RANGES; ++i) ram.push_back({0x100000 + i * 2 * PAGE, PAGE});
    ASSERT_TRUE(parse().valid());
    EXPECT_EQ(output.memory_range_count, numa::MAX_MEMORY_RANGES);
    EXPECT_EQ(output.memory[numa::MAX_MEMORY_RANGES - 1].base, ram.back().base);
    ram.push_back({0x100000 + numa::MAX_MEMORY_RANGES * 2 * PAGE, PAGE});
    refuses(numa::Error::CAPACITY);
    auto const BEFORE = workspace.candidate;
    EXPECT_EQ(numa::parse({}, {}, cpus, {}, workspace.candidate, workspace).error, numa::Error::OUTPUT_ALIAS);
    EXPECT_EQ(workspace.candidate, BEFORE);
}
}  // namespace
