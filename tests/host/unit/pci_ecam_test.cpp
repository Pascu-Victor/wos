#include <gtest/gtest.h>

#include <algorithm>
#include <dev/pci_ecam.hpp>
#include <vector>

namespace ecam = ker::dev::pci::ecam;
namespace {
void put(std::span<uint8_t> bytes, size_t at, uint64_t value, size_t width) {
    for (size_t i = 0; i < width; ++i) bytes[at + i] = static_cast<uint8_t>(value >> (8 * i));
}
void checksum(std::span<uint8_t> bytes) {
    bytes[9] = 0;
    uint8_t sum = 0;
    for (auto b : bytes) sum = static_cast<uint8_t>(sum + b);
    bytes[9] = static_cast<uint8_t>(0U - sum);
}
auto table(std::initializer_list<ecam::Window> windows) -> std::vector<uint8_t> {
    std::vector<uint8_t> bytes(44 + windows.size() * 16);
    std::copy_n("MCFG", 4, bytes.begin());
    put(bytes, 4, bytes.size(), 4);
    bytes[8] = 1;
    size_t at = 44;
    for (auto const& w : windows) {
        put(bytes, at, w.base, 8);
        put(bytes, at + 8, w.segment, 2);
        bytes[at + 10] = w.first_bus;
        bytes[at + 11] = w.last_bus;
        at += 16;
    }
    checksum(bytes);
    return bytes;
}
struct Config {
    std::array<uint8_t, 4096> bytes{};
    std::vector<uint16_t> reads;
    uint16_t unavailable = 0xffff;
    bool bounded = true;
    bool writable = true, retain_write = true;
    size_t writes = 0;
    Config() {
        put(bytes, 0, 0x10411af4, 4);
        put(bytes, 6, 0x10, 2);
        put(bytes, 0x34, 0x40, 4);
        put(bytes, 0x40, 0x10, 2);
        put(bytes, 0x42, 2, 2);
    }
    auto read32(uint16_t at) -> std::optional<uint32_t> {
        reads.push_back(at);
        bounded &= at <= 4092 && at % 4 == 0;
        if (at > 4092 || at % 4 != 0 || at == unavailable) return std::nullopt;
        return static_cast<uint32_t>(ker::mod::acpi::bounded::detail::read(bytes, at, 4));
    }
    auto read16(uint16_t at) -> std::optional<uint16_t> {
        reads.push_back(at);
        bounded &= at <= 4094 && at % 2 == 0;
        if (at > 4094 || at % 2 != 0 || at == unavailable) return std::nullopt;
        return static_cast<uint16_t>(ker::mod::acpi::bounded::detail::read(bytes, at, 2));
    }
    auto write16(uint16_t at, uint16_t value) -> bool {
        ++writes;
        bounded &= at <= 4094 && at % 2 == 0;
        if (!writable || at > 4094 || at % 2 != 0) return false;
        if (retain_write) put(bytes, at, value, 2);
        return true;
    }
    void cap(uint16_t at, uint16_t kind, uint16_t next = 0) { put(bytes, at, kind | 0x10000U | (uint32_t{next} << 20), 4); }
    auto inspect() -> ecam::Capabilities { return ecam::inspect(*this, 0x1af4, 0x1041, 0); }
};
}  // namespace
TEST(PciAcs, EnablesSupportedControlsPreservesOtherBitsAndIsIdempotent) {
    for (auto offset : {0x100U, 0xff8U}) {
        Config c;
        if (offset != 0x100) c.cap(0x100, 1, offset);
        c.cap(offset, 0xd);
        put(c.bytes, offset + 4, 0x5f, 2);
        put(c.bytes, offset + 6, 0xa060, 2);
        auto before = c.bytes;
        auto result = ecam::enable_acs(c, 0x1af4, 0x1041, 0);
        ASSERT_TRUE(result);
        EXPECT_EQ(result->acs_control, 0xa03f);
        put(before, offset + 6, 0xa03f, 2);
        EXPECT_EQ(c.bytes, before);
        EXPECT_EQ(c.writes, 1U);
        EXPECT_TRUE(ecam::enable_acs(c, 0x1af4, 0x1041, 0));
        EXPECT_EQ(c.writes, 1U);
        EXPECT_TRUE(c.bounded);
    }
}
TEST(PciAcs, RefusesActiveCommandInvalidIdentityAndMalformedCapabilitiesWithoutWrites) {
    for (int mode = 0; mode < 5; ++mode) {
        Config c;
        c.cap(0x100, 0xd);
        put(c.bytes, 0x104, 0x5f, 2);
        if (mode == 0) put(c.bytes, 4, 4, 2);
        if (mode == 1) c.unavailable = 4;
        if (mode == 2) put(c.bytes, 0, 0xffffffffU, 4);
        if (mode == 3) c.cap(0x100, 0xd, 0x100);
        if (mode == 4) c.unavailable = 0x106;
        auto const BEFORE = c.bytes;
        EXPECT_FALSE(ecam::enable_acs(c, 0x1af4, 0x1041, 0));
        EXPECT_EQ(c.bytes, BEFORE);
        EXPECT_EQ(c.writes, 0U);
    }
}
TEST(PciAcs, MissingUnsupportedControlsStayUnqualifiedWithoutWrites) {
    for (int bit = -1; bit < 5; ++bit) {
        Config c;
        if (bit >= 0) {
            c.cap(0x100, 0xd);
            put(c.bytes, 0x104, 0x1fU & ~(1U << bit), 2);
        }
        auto const BEFORE = c.bytes;
        auto result = ecam::enable_acs(c, 0x1af4, 0x1041, 0);
        ASSERT_TRUE(result);
        EXPECT_EQ(result->acs_control, 0);
        EXPECT_EQ(c.bytes, BEFORE);
        EXPECT_EQ(c.writes, 0U);
    }
}
TEST(PciAcs, FailedWriteOrHardwareReadbackNeverReturnsAdmissionEvidence) {
    for (bool writable : {false, true}) {
        Config c;
        c.cap(0x100, 0xd);
        put(c.bytes, 0x104, 0x5f, 2);
        c.writable = writable;
        c.retain_write = false;
        EXPECT_FALSE(ecam::enable_acs(c, 0x1af4, 0x1041, 0));
        EXPECT_EQ(c.writes, 1U);
    }
}
TEST(PciEcam, AddressesUseBusZeroBaseAndBoundEveryFunctionAccess) {
    auto const T = ecam::parse(table({{.base = 0xe0000000, .first_bus = 32, .last_bus = 63}}));
    ASSERT_TRUE(T.valid());
    EXPECT_EQ(T.windows[0].start(), 0xe2000000U);
    EXPECT_EQ(T.windows[0].end(), 0xe4000000U);
    EXPECT_EQ(ecam::address(T, 0, 32, 0, 0, 0, 4), 0xe2000000U);
    EXPECT_EQ(ecam::address(T, 0, 63, 31, 7, 4092, 4), 0xe3fffffcU);
    for (auto width : {1U, 2U, 4U}) EXPECT_FALSE(ecam::address(T, 0, 32, 0, 0, 4096, width));
    EXPECT_FALSE(ecam::address(T, 0, 31, 0, 0, 0, 4));
    EXPECT_FALSE(ecam::address(T, 0, 64, 0, 0, 0, 4));
    EXPECT_FALSE(ecam::address(T, 1, 32, 0, 0, 0, 4));
    EXPECT_FALSE(ecam::address(T, 0, 32, 32, 0, 0, 4));
    EXPECT_FALSE(ecam::address(T, 0, 32, 0, 8, 0, 4));
    EXPECT_FALSE(ecam::address(T, 0, 32, 0, 0, 2, 4));
    EXPECT_FALSE(ecam::address(T, 0, 32, 0, 0, 4095, 2));
    EXPECT_FALSE(ecam::address(T, 0, 32, 0, 0, 0, 8));
    EXPECT_FALSE(ecam::address(T, 0, 32, 0, 0, 0, 0));
}
TEST(PciEcam, RejectsEveryTruncationChecksumReservedAndOversizedTable) {
    auto bytes = table({{.base = 0xe0000000, .last_bus = 255}});
    for (size_t size = 0; size < bytes.size(); ++size) EXPECT_FALSE(ecam::parse(std::span{bytes}.first(size)).valid()) << size;
    auto changed = bytes;
    changed[20] ^= 1;
    EXPECT_EQ(ecam::parse(changed).error, ecam::Error::checksum);
    for (size_t at : {size_t{8}, size_t{36}, size_t{56}}) {
        changed = bytes;
        changed[at] = 2;
        checksum(changed);
        EXPECT_EQ(ecam::parse(changed).error, ecam::Error::reserved);
    }
    changed = bytes;
    put(changed, 4, 76, 4);
    checksum(changed);
    EXPECT_EQ(ecam::parse(changed).error, ecam::Error::length);
    changed.resize(44 + 33 * 16);
    EXPECT_EQ(ecam::parse(changed).error, ecam::Error::limit);
}
TEST(PciEcam, RejectsPhysicalAliasesBusOverlapsOverflowAndInvalidRange) {
    for (auto w : {ecam::Window{}, ecam::Window{.base = 0xe0000001}, ecam::Window{.base = 0xfffffffffff00000, .last_bus = 255},
                   ecam::Window{.base = 0xe0000000, .first_bus = 4, .last_bus = 3}})
        EXPECT_EQ(ecam::parse(table({w})).error, ecam::Error::range);
    EXPECT_EQ(ecam::parse(table({{.base = 0xe0000000, .last_bus = 63}, {.base = 0xf0000000, .first_bus = 63, .last_bus = 64}})).error,
              ecam::Error::overlap);
    EXPECT_EQ(ecam::parse(table({{.base = 0xe0000000, .last_bus = 63}, {.base = 0xe0000000, .segment = 1, .last_bus = 63}})).error,
              ecam::Error::overlap);
    auto const T = ecam::parse(table({{.base = 0xe0000000, .last_bus = 63}, {.base = 0xf0000000, .segment = 1, .last_bus = 63}}));
    ASSERT_TRUE(T.valid());
    EXPECT_EQ(T.count, 2U);
    EXPECT_EQ(ecam::address(T, 1, 0, 0, 0, 0, 4), 0xf0000000U);
}
TEST(PciEcam, ReadsRelevantControlsWithoutChangingConfiguration) {
    Config c;
    c.cap(0x100, 0xd, 0x120);
    c.cap(0x120, 0xf, 0x140);
    c.cap(0x140, 0x13, 0x160);
    c.cap(0x160, 0x1b, 0x180);
    c.cap(0x180, 0x10);
    put(c.bytes, 0x104, 0x1f, 2);
    put(c.bytes, 0x106, 0x15, 2);
    put(c.bytes, 0x126, 0x8000, 2);
    put(c.bytes, 0x144, 1, 2);
    put(c.bytes, 0x166, 1, 2);
    put(c.bytes, 0x188, 1, 2);
    put(c.bytes, 0x48, 0x200, 2);
    put(c.bytes, 0x68, 0x20, 2);
    auto const BEFORE = c.bytes;
    auto r = c.inspect();
    ASSERT_TRUE(r.valid());
    EXPECT_EQ(r.extended_count, 5);
    EXPECT_EQ(r.acs_capability, 0x1f);
    EXPECT_EQ(r.acs_control, 0x15);
    EXPECT_EQ(r.ats_control, 0x8000);
    EXPECT_EQ(r.pri_control, 1);
    EXPECT_EQ(r.pasid_control, 1);
    EXPECT_EQ(r.sriov_control, 1);
    EXPECT_EQ(r.device_control, 0x200);
    EXPECT_EQ(r.device_control2, 0x20);
    EXPECT_EQ(c.bytes, BEFORE);
    EXPECT_TRUE(c.bounded);
}
TEST(PciEcam, RejectsIdentityMismatchBeforeFollowingDevicePointers) {
    for (size_t at : {size_t{0}, size_t{2}, size_t{14}}) {
        Config c;
        c.bytes[at] ^= 1;
        auto r = c.inspect();
        EXPECT_EQ(r.error, ecam::CapabilityError::identity);
        EXPECT_EQ(c.reads.size(), 2U);
        EXPECT_EQ(r.pcie, 0);
    }
}
TEST(PciEcam, RejectsConventionalCyclesAlignmentAndTruncatedPcieCapability) {
    for (uint16_t ptr : {uint16_t{0x3c}, uint16_t{0x41}, uint16_t{0xfc}}) {
        Config c;
        put(c.bytes, 0x34, ptr, 4);
        put(c.bytes, ptr, 0x10, 2);
        EXPECT_FALSE(c.inspect().valid());
        EXPECT_TRUE(c.bounded);
    }
    Config cycle;
    put(cycle.bytes, 0x40, 0x4010, 2);
    EXPECT_EQ(cycle.inspect().error, ecam::CapabilityError::malformed);
    Config duplicate;
    put(duplicate.bytes, 0x40, 0x8010, 2);
    put(duplicate.bytes, 0x80, 0x10, 2);
    EXPECT_EQ(duplicate.inspect().error, ecam::CapabilityError::malformed);
    Config conventional;
    put(conventional.bytes, 0x40, 5, 2);
    ASSERT_TRUE(conventional.inspect().valid());
    EXPECT_TRUE(std::ranges::all_of(conventional.reads, [](auto at) { return at < 256; }));
}
TEST(PciEcam, RejectsExtendedCyclesDuplicateControlsBadVersionsAndEdges) {
    for (uint16_t next : {uint16_t{0x100}, uint16_t{0x101}, uint16_t{0xfc}, uint16_t{0xffc}}) {
        Config c;
        c.cap(0x100, 0xd, next);
        c.cap(0xffc, 0xd);
        EXPECT_FALSE(c.inspect().valid());
        EXPECT_TRUE(c.bounded);
    }
    Config duplicate;
    duplicate.cap(0x100, 0xd, 0x120);
    duplicate.cap(0x120, 0xd);
    EXPECT_EQ(duplicate.inspect().error, ecam::CapabilityError::malformed);
    Config version;
    version.cap(0x100, 0xd);
    version.bytes[0x102] = 2;
    EXPECT_EQ(version.inspect().error, ecam::CapabilityError::malformed);
    Config zero;
    zero.cap(0x100, 0xd, 0x120);
    EXPECT_EQ(zero.inspect().error, ecam::CapabilityError::malformed);
}
TEST(PciEcam, BoundedMaximumChainAndUnavailableRegistersYieldNoPartialEvidence) {
    Config full;
    for (uint16_t at = 0x100; at <= 0xffc; at += 4) full.cap(at, 0x99, at == 0xffc ? 0 : at + 4);
    auto r = full.inspect();
    ASSERT_TRUE(r.valid());
    EXPECT_EQ(r.extended_count, 960);
    EXPECT_TRUE(full.bounded);
    Config baseline;
    baseline.cap(0x100, 0xd);
    ASSERT_TRUE(baseline.inspect().valid());
    for (auto at : baseline.reads) {
        Config c;
        c.cap(0x100, 0xd);
        c.unavailable = at;
        r = c.inspect();
        EXPECT_EQ(r.error, ecam::CapabilityError::inaccessible) << at;
        EXPECT_EQ(r.pcie, 0);
        EXPECT_EQ(r.acs, 0);
    }
}

TEST(PciEcam, AperturesNeverRemapRamAndRejectInvalidMemoryMapOrCpuRange) {
    ecam::Window w{.base = 0xe0000000, .last_bus = 255};
    std::vector<ecam::MemoryRegion> memory{{0, 0xe0000000, false}, {0xe0000000, 0x10000000, true}, {0xf0000000, 0x1000, false}};
    auto read = [&](size_t i) -> std::optional<ecam::MemoryRegion> { return memory[i]; };
    constexpr uint64_t HHDM = 0xffff800000000000;
    EXPECT_TRUE(ecam::safe_aperture(w, 40, HHDM, memory.size(), read));
    memory[1].reserved = false;
    EXPECT_FALSE(ecam::safe_aperture(w, 40, HHDM, memory.size(), read));
    memory.erase(memory.begin() + 1);
    EXPECT_TRUE(ecam::safe_aperture(w, 40, HHDM, memory.size(), read));
    memory[0].length++;
    EXPECT_FALSE(ecam::safe_aperture(w, 40, HHDM, memory.size(), read));
    memory[0].length--;
    memory[1].base--;
    EXPECT_FALSE(ecam::safe_aperture(w, 40, HHDM, memory.size(), read));
    memory[1].base++;
    memory.push_back({UINT64_MAX, 1, true});
    EXPECT_FALSE(ecam::safe_aperture(w, 40, HHDM, memory.size(), read));
    memory.pop_back();
    for (uint8_t bits : {uint8_t{0}, uint8_t{31}, uint8_t{53}, uint8_t{64}})
        EXPECT_FALSE(ecam::safe_aperture(w, bits, HHDM, memory.size(), read));
    EXPECT_FALSE(ecam::safe_aperture(w, 40, UINT64_MAX - 0xefffffff, memory.size(), read));
    EXPECT_FALSE(ecam::safe_aperture(w, 40, HHDM, 0, read));
    EXPECT_FALSE(ecam::safe_aperture(w, 40, HHDM, 4097, read));
    EXPECT_FALSE(ecam::safe_aperture(w, 40, HHDM, 1, [](size_t) -> std::optional<ecam::MemoryRegion> { return std::nullopt; }));
    w.base = uint64_t{1} << 40;
    EXPECT_FALSE(ecam::safe_aperture(w, 40, HHDM, memory.size(), read));
}
