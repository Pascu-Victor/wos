#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <dev/iommu/ivrs.hpp>
#include <span>
#include <utility>
#include <vector>

namespace ivrs = ker::dev::iommu::ivrs;
namespace {
using Bytes = std::vector<uint8_t>;

void put(Bytes& bytes, size_t offset, uint64_t value, size_t count) {
    for (size_t i = 0; i < count; ++i) bytes.at(offset + i) = static_cast<uint8_t>(value >> (8U * i));
}

void seal(Bytes& bytes) {
    put(bytes, 4, bytes.size(), 4);
    bytes.at(9) = 0;
    uint8_t sum = 0;
    for (auto byte : bytes) sum = static_cast<uint8_t>(sum + byte);
    bytes.at(9) = static_cast<uint8_t>(0U - sum);
}

auto entry4(uint8_t type, uint16_t id = 0, uint8_t setting = 0) -> Bytes {
    return {type, static_cast<uint8_t>(id), static_cast<uint8_t>(id >> 8U), setting};
}

auto entry8(uint8_t type, uint16_t id = 0) -> Bytes {
    auto bytes = entry4(type, id);
    bytes.resize(8);
    return bytes;
}

void append(Bytes& bytes, const Bytes& suffix) { bytes.insert(bytes.end(), suffix.begin(), suffix.end()); }

auto hid(uint8_t length, uint8_t format = 2) -> Bytes {
    Bytes bytes(22 + size_t{length}, 0);
    bytes[0] = 0xf0;
    bytes[1] = 0x34;
    bytes[2] = 0x12;
    put(bytes, 4, 0x4847464544434241, 8);  // Eight fixed HID bytes, no C-string dependency.
    put(bytes, 12, 0x0807060504030201, 8);
    bytes[20] = format;
    bytes[21] = length;
    for (size_t i = 22; i < bytes.size(); ++i) bytes[i] = 0x81;
    return bytes;
}

auto hardware(uint8_t type = 0x10, const Bytes& entries = {}) -> Bytes {
    Bytes bytes(type == 0x10 ? 24 : 40);
    bytes[0] = type;
    put(bytes, 4, 2, 2);
    put(bytes, 6, 0x40, 2);
    put(bytes, 8, 0xfed90000, 8);
    append(bytes, entries);
    put(bytes, 2, bytes.size(), 2);
    return bytes;
}

auto memory(uint8_t type = 0x20, uint64_t start = 0x200000, uint64_t length = 4096) -> Bytes {
    Bytes bytes(32);
    bytes[0] = type;
    bytes[1] = 7;
    put(bytes, 2, 32, 2);
    put(bytes, 4, 0x1234, 2);
    put(bytes, 6, 0x1235, 2);
    put(bytes, 16, start, 8);
    put(bytes, 24, length, 8);
    return bytes;
}

auto table(const Bytes& blocks) -> Bytes {
    Bytes bytes(48);
    bytes[0] = 'I';
    bytes[1] = 'V';
    bytes[2] = 'R';
    bytes[3] = 'S';
    bytes[8] = 1;
    put(bytes, 36, 0x00403040, 4);
    append(bytes, blocks);
    seal(bytes);
    return bytes;
}

void expect_empty(const ivrs::Result& result) {
    EXPECT_TRUE(result.table.bytes.empty());
    EXPECT_EQ(result.table.bytes.data(), nullptr);
    EXPECT_EQ(result.table.hardware_blocks, 0U);
    EXPECT_EQ(result.table.memory_blocks, 0U);
    EXPECT_EQ(result.table.device_entries, 0U);
    EXPECT_EQ(result.table.ivinfo, 0U);
    EXPECT_EQ(result.table.revision, 0U);
    EXPECT_FALSE(result.table.unsupported);
}

void expect_error(std::span<const uint8_t> bytes, ivrs::Error error, size_t offset) {
    auto const result = ivrs::parse(bytes);
    EXPECT_FALSE(result.valid());
    EXPECT_EQ(result.error, error);
    EXPECT_EQ(result.offset, offset);
    expect_empty(result);
}
}  // namespace

TEST(Ivrs, BorrowsExactlyTheDeclaredImmutableExtent) {
    auto bytes = table(hardware());
    auto const original = bytes;
    auto result = ivrs::parse(bytes);
    ASSERT_TRUE(result.valid());
    EXPECT_EQ(bytes, original);
    EXPECT_EQ(result.table.bytes.data(), bytes.data());
    EXPECT_EQ(result.table.bytes.size(), 72U);
    EXPECT_EQ(result.table.hardware_blocks, 1U);
    EXPECT_EQ(result.table.memory_blocks, 0U);
    EXPECT_EQ(result.table.device_entries, 0U);
    EXPECT_EQ(result.table.ivinfo, 0x00403040U);
    EXPECT_EQ(result.table.revision, 1);
    EXPECT_FALSE(result.table.unsupported);
    bytes.resize(128, 0xff);  // Accessible trailing bytes are outside firmware's declaration.
    result = ivrs::parse(bytes);
    ASSERT_TRUE(result.valid());
    EXPECT_EQ(result.table.bytes.size(), 72U);
    EXPECT_EQ(result.table.hardware_blocks, 1U);
}

TEST(Ivrs, ExplicitRevisionOneAndTwoObserverPolicyRejectsOtherVersions) {
    for (unsigned revision = 0; revision < 256; ++revision) {
        SCOPED_TRACE(revision);
        auto bytes = table(hardware());
        bytes[8] = static_cast<uint8_t>(revision);
        seal(bytes);
        if (revision == 1 || revision == 2) {
            auto const result = ivrs::parse(bytes);
            ASSERT_TRUE(result.valid());
            EXPECT_EQ(result.table.revision, revision);
        } else {
            expect_error(bytes, ivrs::Error::revision, 8);
        }
    }
}

TEST(Ivrs, RejectsAllAccessibleTruncationsWithoutPublishingPartialInventory) {
    auto blocks = hardware(0x40, hid(255));
    append(blocks, memory(0x22));
    auto bytes = table(blocks);
    for (size_t prefix = 0; prefix < bytes.size(); ++prefix) {
        SCOPED_TRACE(prefix);
        expect_error(std::span(bytes).first(prefix), prefix < 48 ? ivrs::Error::truncated : ivrs::Error::length, prefix < 48 ? prefix : 4);
    }
    ASSERT_TRUE(ivrs::parse(bytes).valid());
}

TEST(Ivrs, ChecksSignatureChecksumHeaderReservedAndMandatoryHardware) {
    auto bytes = table(hardware());
    bytes[0] = 'D';
    expect_error(bytes, ivrs::Error::signature, 0);
    bytes = table(hardware());
    bytes[10] ^= 0x20;
    expect_error(bytes, ivrs::Error::checksum, 9);
    for (size_t offset = 40; offset < 48; ++offset) {
        bytes = table(hardware());
        bytes[offset] = 1;
        seal(bytes);
        expect_error(bytes, ivrs::Error::reserved, 40);
    }
    expect_error(table({}), ivrs::Error::missing_hardware, 48);
    expect_error(table(memory()), ivrs::Error::missing_hardware, 48);
    expect_error(table({0x77, 0, 4, 0}), ivrs::Error::missing_hardware, 48);
}

TEST(Ivrs, ChecksDeclaredTableLengthAndExactByteCapacity) {
    for (uint64_t declared : {uint64_t{0}, uint64_t{47}, uint64_t{73}, uint64_t{65537}, uint64_t{UINT32_MAX}}) {
        auto bytes = table(hardware());
        put(bytes, 4, declared, 4);
        expect_error(bytes, ivrs::Error::length, 4);
    }
    auto blocks = hardware();
    Bytes opaque(65536 - 72);
    opaque[0] = 0x77;
    put(opaque, 2, opaque.size(), 2);
    append(blocks, opaque);
    auto bytes = table(blocks);
    auto const result = ivrs::parse(bytes);
    ASSERT_TRUE(result.valid());
    EXPECT_EQ(result.table.bytes.size(), ivrs::MAX_TABLE_SIZE);
    EXPECT_TRUE(result.table.unsupported);
    bytes.push_back(0);
    seal(bytes);
    expect_error(bytes, ivrs::Error::length, 4);
}

TEST(Ivrs, BoundsEveryRecordHeaderAndFixedHardwareHeaderBeforeFieldReads) {
    for (uint8_t type : {0x10, 0x11, 0x40}) {
        size_t const fixed = type == 0x10 ? 24 : 40;
        for (size_t length = 1; length < fixed; ++length) {
            auto block = hardware(type);
            block.resize(length);
            if (length >= 4) put(block, 2, length, 2);
            expect_error(table(block), ivrs::Error::record_length, 48);
        }
    }
    for (uint16_t declared : {0, 1, 3, 23, 25, 65535}) {
        auto block = hardware();
        put(block, 2, declared, 2);
        expect_error(table(block), ivrs::Error::record_length, 48);
    }
    for (size_t tail = 1; tail <= 3; ++tail) {
        auto blocks = hardware();
        blocks.resize(blocks.size() + tail);
        expect_error(table(blocks), ivrs::Error::record_length, 72);
    }
}

TEST(Ivrs, RecordsAlternativeFormatsWithoutInventingUniqueUnitOwnership) {
    Bytes blocks;
    for (uint8_t type : {0x10, 0x11, 0x40}) {
        auto block = hardware(type);
        block[1] = 0xff;  // Modern flag bits are not the old low-five-bit mask.
        put(block, 16, 0x1234, 2);
        put(block, 18, 0xffff, 2);
        put(block, 20, UINT32_MAX, 4);
        if (type != 0x10) {
            put(block, 24, UINT64_MAX, 8);
            put(block, 32, 0xaabbccdd11223344, 8);  // EFR2 is not universal zero padding.
        }
        append(blocks, block);
        auto const result = ivrs::parse(table(block));
        ASSERT_TRUE(result.valid());
        EXPECT_EQ(result.table.unsupported, type != 0x10);
    }
    auto bytes = table(blocks);
    auto const result = ivrs::parse(bytes);
    ASSERT_TRUE(result.valid());
    EXPECT_EQ(result.table.hardware_blocks, 3U);  // All three share base and DeviceID.
    EXPECT_TRUE(result.table.unsupported);
    EXPECT_EQ(result.table.bytes[49], 0xff);
    EXPECT_EQ(result.table.bytes[48 + 24 + 32], 0x44);
}

TEST(Ivrs, EnforcesOnlyTheDeclaredObserverRegisterAndCapabilityGeometry) {
    for (uint64_t base : {uint64_t{0}, uint64_t{1}, uint64_t{0xfed90001}, uint64_t{UINT64_MAX}}) {
        auto block = hardware();
        put(block, 8, base, 8);
        expect_error(table(block), ivrs::Error::register_range, 56);
    }
    for (uint64_t base : {uint64_t{4096}, uint64_t{0xfed91000}, uint64_t{UINT64_MAX - 4095}}) {
        auto block = hardware();
        put(block, 8, base, 8);
        EXPECT_TRUE(ivrs::parse(table(block)).valid());  // Observation never maps these addresses.
    }
    for (uint16_t capability : {0, 0x3c, 0x41, 0xfd, 0x100, 0xfffc}) {
        auto block = hardware();
        put(block, 6, capability, 2);
        expect_error(table(block), ivrs::Error::capability_offset, 54);
    }
    for (uint16_t capability : {0x40, 0xfc}) {
        auto block = hardware();
        put(block, 6, capability, 2);
        EXPECT_TRUE(ivrs::parse(table(block)).valid());
    }
}

TEST(Ivrs, ExercisesAllEntryTypeBytesWithoutInventingVariableLengths) {
    constexpr std::array<uint8_t, 9> basic{0, 1, 2, 3, 4, 0x40, 0x42, 0x43, 0x48};
    for (unsigned type = 0; type < 256; ++type) {
        SCOPED_TRACE(type);
        Bytes entries;
        if (type == 4) append(entries, entry4(3, 0));
        Bytes item(type < 0x40 ? 4 : type < 0x80 ? 8 : 32);
        item[0] = static_cast<uint8_t>(type);
        if (type == 0x48) item[7] = 1;
        if (type == 0xf0) item = hid(0);
        append(entries, item);
        if (type == 3 || type == 0x43 || type == 0x47) append(entries, entry4(4, 0xffff));
        auto bytes = table(hardware(0x10, entries));
        if (type >= 0x80 && type != 0xf0) {
            expect_error(bytes, ivrs::Error::device_entry_type, 72);
            continue;
        }
        auto const result = ivrs::parse(bytes);
        ASSERT_TRUE(result.valid());
        EXPECT_EQ(result.table.device_entries, type == 3 || type == 4 || type == 0x43 || type == 0x47 ? 2U : 1U);
        bool supported = false;
        for (auto known : basic) supported |= type == known;
        EXPECT_EQ(result.table.unsupported, !supported);
    }
}

TEST(Ivrs, TruncatedFixedEntriesCannotBorrowBytesFromTheNextBlock) {
    for (uint8_t type : {0x02, 0x42, 0x46, 0x48}) {
        auto item = type == 2 ? entry4(type) : entry8(type);
        for (size_t length = 1; length < item.size(); ++length) {
            auto prefix = Bytes(item.begin(), item.begin() + static_cast<ptrdiff_t>(length));
            auto blocks = hardware(0x10, prefix);
            append(blocks, hardware());
            expect_error(table(blocks), ivrs::Error::device_entry_length, 72);
        }
    }
}

TEST(Ivrs, InclusiveRangesPermitPaddingButNeverSilentlyReplacePendingState) {
    for (uint8_t kind : {0x03, 0x43, 0x47}) {
        for (auto [first, last] : {std::pair<uint16_t, uint16_t>{0, 0xffff}, {0xffff, 0xffff}, {0x1234, 0x1234}}) {
            auto entries = kind == 3 ? entry4(kind, first) : entry8(kind, first);
            if (kind == 0x43) put(entries, 5, 0xffff, 2);  // One target, even for a full source range.
            append(entries, entry4(0));
            append(entries, entry8(0x40));
            append(entries, entry4(4, last));
            auto const result = ivrs::parse(table(hardware(0x10, entries)));
            ASSERT_TRUE(result.valid());
            EXPECT_EQ(result.table.device_entries, 4U);
            EXPECT_EQ(result.table.unsupported, kind == 0x47);
        }
        auto start = kind == 3 ? entry4(kind, 2) : entry8(kind, 2);
        expect_error(table(hardware(0x10, start)), ivrs::Error::device_range, 72 + start.size());
        auto bad = start;
        append(bad, entry4(4, 1));
        expect_error(table(hardware(0x10, bad)), ivrs::Error::device_range, 72 + start.size());
        for (uint8_t interrupting : {0x02, 0x03, 0x42, 0x43, 0x46, 0x47, 0x48}) {
            bad = start;
            append(bad, interrupting < 0x40 ? entry4(interrupting) : entry8(interrupting));
            expect_error(table(hardware(0x10, bad)), ivrs::Error::device_range, 72 + start.size());
        }
    }
    expect_error(table(hardware(0x10, entry4(4))), ivrs::Error::device_range, 72);
    auto blocks = hardware(0x10, entry4(3));
    append(blocks, hardware(0x10, entry4(4)));
    expect_error(table(blocks), ivrs::Error::device_range, 76);
}

TEST(Ivrs, RejectsOnlySourcedAliasReservedBytesAndPreservesTargetIdentity) {
    for (uint8_t kind : {0x42, 0x43}) {
        auto item = entry8(kind, 0x1234);
        put(item, 5, 0xffff, 2);
        Bytes entries = item;
        if (kind == 0x43) append(entries, entry4(4, 0xffff));
        auto bytes = table(hardware(0x10, entries));
        auto result = ivrs::parse(bytes);
        ASSERT_TRUE(result.valid());
        EXPECT_FALSE(result.table.unsupported);
        EXPECT_EQ(result.table.bytes[77], 0xff);
        EXPECT_EQ(result.table.bytes[78], 0xff);
        for (size_t reserved : {size_t{4}, size_t{7}}) {
            auto bad = bytes;
            bad[72 + reserved] = 1;
            seal(bad);
            expect_error(bad, ivrs::Error::reserved, 72);
        }
    }
}

TEST(Ivrs, UnknownSettingsPaddingExtendedAndSpecialFormsRemainObservableUnsupported) {
    for (uint8_t type : {0x00, 0x02, 0x40, 0x46}) {
        auto item = type < 0x40 ? entry4(type) : entry8(type);
        item[3] = 8;
        auto const result = ivrs::parse(table(hardware(0x10, item)));
        ASSERT_TRUE(result.valid());
        EXPECT_TRUE(result.table.unsupported);
    }
    auto padding = entry8(0x40);
    padding[7] = 0xff;
    EXPECT_TRUE(ivrs::parse(table(hardware(0x10, padding))).table.unsupported);
    for (uint32_t raw : {0U, 1U, 0x80000000U, UINT32_MAX}) {
        auto item = entry8(0x46);
        put(item, 4, raw, 4);
        auto const result = ivrs::parse(table(hardware(0x10, item)));
        ASSERT_TRUE(result.valid());
        EXPECT_TRUE(result.table.unsupported);
    }
    for (unsigned variety = 0; variety < 256; ++variety) {
        auto item = entry8(0x48);
        item[4] = 0xff;
        put(item, 5, 0x5678, 2);
        item[7] = static_cast<uint8_t>(variety);
        auto const result = ivrs::parse(table(hardware(0x10, item)));
        ASSERT_TRUE(result.valid());
        EXPECT_EQ(result.table.unsupported, variety != 1 && variety != 2);
    }
}

TEST(Ivrs, HidUsesExactUidLengthAndAllowsUnalignedFollowingEntries) {
    for (uint8_t length : {0, 1, 8, 255}) {
        for (uint8_t format : {0, 1, 2, 0xff}) {
            auto entries = hid(length, format);
            append(entries, entry4(2, 0xbeef));
            auto const result = ivrs::parse(table(hardware(0x40, entries)));
            ASSERT_TRUE(result.valid());
            EXPECT_TRUE(result.table.unsupported);
            EXPECT_EQ(result.table.device_entries, 2U);
        }
        auto item = hid(length);
        for (size_t prefix = 1; prefix < item.size(); ++prefix) {
            auto block = hardware(0x40, Bytes(item.begin(), item.begin() + static_cast<ptrdiff_t>(prefix)));
            append(block, hardware());
            expect_error(table(block), ivrs::Error::device_entry_length, 88);
        }
    }
    // A legacy IVHD containing HID remains structurally observed, unsupported.
    auto const result = ivrs::parse(table(hardware(0x10, hid(0))));
    ASSERT_TRUE(result.valid());
    EXPECT_TRUE(result.table.unsupported);
}

TEST(Ivrs, ChecksEveryIvmdLengthAndRangeWhilePreservingSegmentLikeRawBytes) {
    for (uint8_t kind : {0x20, 0x21, 0x22}) {
        for (size_t length : {size_t{4}, size_t{31}, size_t{33}, size_t{40}}) {
            auto block = memory(kind);
            block.resize(length);
            put(block, 2, length, 2);
            auto blocks = hardware();
            append(blocks, block);
            expect_error(table(blocks), ivrs::Error::record_length, 72);
        }
        auto block = memory(kind);
        put(block, 8, 0xaabbccddeeff0001, 8);
        auto blocks = hardware();
        append(blocks, block);
        auto bytes = table(blocks);
        auto const result = ivrs::parse(bytes);
        ASSERT_TRUE(result.valid());
        EXPECT_EQ(result.table.memory_blocks, 1U);
        EXPECT_TRUE(result.table.unsupported);
        EXPECT_EQ(result.table.bytes[80], 1);
        for (auto [start, length] :
             {std::pair<uint64_t, uint64_t>{0, 0}, {1, 4096}, {0, 1}, {UINT64_MAX - 4095, 8192}, {0x1000, UINT64_MAX}}) {
            blocks = hardware();
            append(blocks, memory(kind, start, length));
            expect_error(table(blocks), ivrs::Error::memory_range, 88);
        }
        for (uint64_t start : {uint64_t{0}, uint64_t{UINT64_MAX - 4095}}) {
            blocks = hardware();
            append(blocks, memory(kind, start, 4096));
            auto const last_page = ivrs::parse(table(blocks));
            ASSERT_TRUE(last_page.valid());
            EXPECT_EQ(last_page.table.memory_blocks, 1U);
        }
    }
    auto reversed = memory(0x22);
    put(reversed, 6, 0x1233, 2);
    auto blocks = hardware();
    append(blocks, reversed);
    expect_error(table(blocks), ivrs::Error::device_range, 76);
    auto unknown_flags = memory();
    unknown_flags[1] = 0x80;
    blocks = hardware();
    append(blocks, unknown_flags);
    auto const result = ivrs::parse(table(blocks));
    ASSERT_TRUE(result.valid());
    EXPECT_TRUE(result.table.unsupported);
}

TEST(Ivrs, EnforcesGlobalBlockAndEntryCapacityExactlyWithoutPartialResults) {
    auto blocks = hardware();
    for (size_t i = 1; i < ivrs::MAX_BLOCKS; ++i) append(blocks, {0x77, 0, 4, 0});
    auto bytes = table(blocks);
    ASSERT_TRUE(ivrs::parse(bytes).valid());
    size_t const overflowing_block = bytes.size();
    append(blocks, {0x77, 0, 4, 0});
    expect_error(table(blocks), ivrs::Error::limit, overflowing_block);

    Bytes entries;
    for (size_t i = 0; i < ivrs::MAX_DEVICE_ENTRIES; ++i) append(entries, entry4(2, static_cast<uint16_t>(i)));
    auto const exact = ivrs::parse(table(hardware(0x10, entries)));
    ASSERT_TRUE(exact.valid());
    EXPECT_EQ(exact.table.device_entries, ivrs::MAX_DEVICE_ENTRIES);
    append(entries, entry4(2));
    expect_error(table(hardware(0x10, entries)), ivrs::Error::limit, 72 + ivrs::MAX_DEVICE_ENTRIES * 4);

    // The entry budget is global across alternative hardware blocks as well.
    Bytes half(entries.begin(), entries.begin() + 2048 * 4);
    blocks = hardware(0x10, half);
    append(blocks, hardware(0x11, half));
    ASSERT_TRUE(ivrs::parse(table(blocks)).valid());
    append(half, entry4(2));
    blocks = hardware(0x10, Bytes(entries.begin(), entries.begin() + 2048 * 4));
    append(blocks, hardware(0x11, half));
    expect_error(table(blocks), ivrs::Error::limit, 48 + 24 + 2048 * 4 + 40 + 2048 * 4);
}
