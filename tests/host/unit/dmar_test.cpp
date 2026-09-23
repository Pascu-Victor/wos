#include <gtest/gtest.h>

#include <dev/iommu/dmar.hpp>
#include <vector>

namespace dmar = ker::dev::iommu::dmar;
namespace {

void put(std::vector<uint8_t>& bytes, size_t offset, uint64_t value, size_t count) {
    for (size_t i = 0; i < count; ++i) {
        bytes.at(offset + i) = static_cast<uint8_t>(value >> (i * 8U));
    }
}

void checksum(std::vector<uint8_t>& bytes) {
    put(bytes, 4, bytes.size(), 4);
    bytes.at(9) = 0;
    uint8_t sum = 0;
    for (auto byte : bytes) sum = static_cast<uint8_t>(sum + byte);
    bytes.at(9) = static_cast<uint8_t>(0U - sum);
}

auto base_table() -> std::vector<uint8_t> {
    std::vector<uint8_t> bytes(64);
    bytes[0] = 'D';
    bytes[1] = 'M';
    bytes[2] = 'A';
    bytes[3] = 'R';
    bytes[8] = 1;
    bytes[36] = 47;
    put(bytes, 50, 16, 2);
    bytes[52] = 1;
    put(bytes, 56, 0xfed90000, 8);
    checksum(bytes);
    return bytes;
}

void append_scope(std::vector<uint8_t>& bytes, uint8_t type = 1) { bytes.insert(bytes.end(), {type, 8, 0, 0, 0, 0, 3, 1}); }

void append_rmrr(std::vector<uint8_t>& bytes) {
    size_t const START = bytes.size();
    bytes.resize(START + 24);
    put(bytes, START, 1, 2);
    put(bytes, START + 2, 32, 2);
    put(bytes, START + 8, 0x100000, 8);
    put(bytes, START + 16, 0x10ffff, 8);
    append_scope(bytes);
    checksum(bytes);
}

void expect_error(const std::vector<uint8_t>& bytes, dmar::Error error) {
    auto const RESULT = dmar::parse(bytes);
    EXPECT_FALSE(RESULT.valid());
    EXPECT_EQ(RESULT.error, error);
    EXPECT_TRUE(RESULT.table.bytes.empty());
    EXPECT_EQ(RESULT.table.units, 0U);
    EXPECT_EQ(RESULT.table.scopes, 0U);
}

}  // namespace

TEST(Dmar, DecodesUnitsAndReservedDeviceScopesWithoutOwningInput) {
    auto bytes = base_table();
    append_rmrr(bytes);
    auto const RESULT = dmar::parse(bytes);
    ASSERT_TRUE(RESULT.valid());
    EXPECT_EQ(RESULT.table.bytes.data(), bytes.data());
    EXPECT_EQ(RESULT.table.bytes.size(), bytes.size());
    EXPECT_EQ(RESULT.table.address_bits, 48);
    EXPECT_EQ(RESULT.table.units, 1U);
    EXPECT_EQ(RESULT.table.reserved_regions, 1U);
    EXPECT_EQ(RESULT.table.scopes, 1U);
    EXPECT_FALSE(RESULT.table.unsupported);
}

TEST(Dmar, RejectsEveryTruncationAndDoesNotReadTrailingAccessibleBytes) {
    auto bytes = base_table();
    append_rmrr(bytes);
    for (size_t size = 0; size < bytes.size(); ++size) {
        auto const RESULT = dmar::parse(std::span(bytes).first(size));
        EXPECT_FALSE(RESULT.valid()) << size;
        EXPECT_TRUE(RESULT.table.bytes.empty());
    }
    size_t const SIZE = bytes.size();
    bytes.resize(SIZE + 16, 0xff);
    auto const RESULT = dmar::parse(bytes);
    ASSERT_TRUE(RESULT.valid());
    EXPECT_EQ(RESULT.table.bytes.size(), SIZE);
}

TEST(Dmar, ChecksSignatureChecksumRevisionAddressWidthAndReservedFields) {
    auto bytes = base_table();
    bytes[0] = 'X';
    expect_error(bytes, dmar::Error::signature);
    bytes = base_table();
    bytes[10] ^= 1;
    expect_error(bytes, dmar::Error::checksum);
    bytes = base_table();
    bytes[8] = 2;
    checksum(bytes);
    expect_error(bytes, dmar::Error::revision);
    for (uint8_t bits : {uint8_t{0}, uint8_t{10}, uint8_t{64}, uint8_t{255}}) {
        bytes = base_table();
        bytes[36] = bits;
        checksum(bytes);
        expect_error(bytes, dmar::Error::address_width);
    }
    for (size_t offset = 37; offset < 48; ++offset) {
        bytes = base_table();
        bytes[offset] = 0x80;
        checksum(bytes);
        expect_error(bytes, dmar::Error::reserved);
    }
}

TEST(Dmar, RejectsRecordLengthWrapAndMisorderedRecords) {
    for (uint16_t size : {0, 1, 3, 4, 15, 17, 65535}) {
        auto bytes = base_table();
        put(bytes, 50, size, 2);
        checksum(bytes);
        expect_error(bytes, dmar::Error::record_length);
    }
    auto bytes = base_table();
    auto const UNIT = std::vector<uint8_t>(bytes.begin() + 48, bytes.end());
    append_rmrr(bytes);
    bytes.insert(bytes.end(), UNIT.begin(), UNIT.end());
    checksum(bytes);
    expect_error(bytes, dmar::Error::record_order);
    bytes = base_table();
    bytes.push_back(0);
    checksum(bytes);
    expect_error(bytes, dmar::Error::record_length);
}

TEST(Dmar, RejectsBadAndOverlappingRegisterBanksAndDuplicateSegmentDefaults) {
    for (uint64_t address : {uint64_t{0}, uint64_t{0xfed90001}, uint64_t{1} << 48}) {
        auto bytes = base_table();
        put(bytes, 56, address, 8);
        checksum(bytes);
        expect_error(bytes, dmar::Error::register_range);
    }
    auto bytes = base_table();
    put(bytes, 56, (uint64_t{1} << 48) - 4096, 8);
    bytes[53] = 1;
    checksum(bytes);
    expect_error(bytes, dmar::Error::register_range);
    bytes = base_table();
    auto const UNIT = std::vector<uint8_t>(bytes.begin() + 48, bytes.end());
    bytes.insert(bytes.end(), UNIT.begin(), UNIT.end());
    checksum(bytes);
    expect_error(bytes, dmar::Error::duplicate_unit);
    put(bytes, 72, 0xfed91000, 8);
    checksum(bytes);
    expect_error(bytes, dmar::Error::duplicate_unit);
    put(bytes, 70, 1, 2);  // A different segment may have its own default.
    checksum(bytes);
    ASSERT_TRUE(dmar::parse(bytes).valid());
    put(bytes, 72, 0xfed90000, 8);  // Register overlap is invalid even across segments.
    checksum(bytes);
    expect_error(bytes, dmar::Error::duplicate_unit);
}

TEST(Dmar, RequiresScopesAndChecksEveryPathHop) {
    auto bytes = base_table();
    bytes[52] = 0;
    checksum(bytes);
    expect_error(bytes, dmar::Error::missing_scope);
    append_scope(bytes, 2);
    bytes.insert(bytes.end(), {31, 7});
    bytes[65] = 10;
    put(bytes, 50, 26, 2);
    checksum(bytes);
    ASSERT_TRUE(dmar::parse(bytes).valid());
    for (size_t offset : {size_t{70}, size_t{72}}) {
        auto bad = bytes;
        bad[offset] = 32;
        checksum(bad);
        expect_error(bad, dmar::Error::scope_path);
        bad = bytes;
        bad[offset + 1] = 8;
        checksum(bad);
        expect_error(bad, dmar::Error::scope_path);
    }
    for (uint8_t length : {0, 6, 7, 9, 11, 255}) {
        auto bad = bytes;
        bad[65] = length;
        checksum(bad);
        expect_error(bad, dmar::Error::scope_length);
    }
    for (uint8_t type : {0, 6, 255}) {
        auto bad = bytes;
        bad[64] = type;
        checksum(bad);
        expect_error(bad, dmar::Error::scope_type);
    }
}

TEST(Dmar, ValidatesReservedRangeAlignmentWidthOrderAndScopeClass) {
    auto bytes = base_table();
    append_rmrr(bytes);
    for (auto [offset, value] :
         {std::pair<size_t, uint64_t>{72, 0x100001}, {80, 0x10fffe}, {72, 0x110000}, {80, (uint64_t{1} << 48) | 4095}}) {
        auto bad = bytes;
        put(bad, offset, value, 8);
        checksum(bad);
        expect_error(bad, dmar::Error::reserved_range);
    }
    bytes[88] = 3;
    checksum(bytes);
    expect_error(bytes, dmar::Error::scope_type);
    bytes.resize(88);
    put(bytes, 66, 24, 2);
    checksum(bytes);
    expect_error(bytes, dmar::Error::missing_scope);
}

TEST(Dmar, Accepts64BitAddressWidthWithoutUndefinedShift) {
    auto bytes = base_table();
    bytes[36] = 63;
    put(bytes, 56, UINT64_MAX - 4095, 8);
    checksum(bytes);
    ASSERT_TRUE(dmar::parse(bytes).valid());
    bytes[53] = 1;
    checksum(bytes);
    expect_error(bytes, dmar::Error::register_range);
}

TEST(Dmar, UnsupportedSemanticsRemainVisibleAndCannotLookFullySupported) {
    auto bytes = base_table();
    bytes.insert(bytes.end(), {99, 0, 4, 0});
    checksum(bytes);
    auto result = dmar::parse(bytes);
    ASSERT_TRUE(result.valid());
    EXPECT_TRUE(result.table.unsupported);
    bytes = base_table();
    append_scope(bytes, 5);
    put(bytes, 50, 24, 2);
    checksum(bytes);
    result = dmar::parse(bytes);
    ASSERT_TRUE(result.valid());
    EXPECT_TRUE(result.table.unsupported);
}

TEST(Dmar, BoundsTableRecordsAndScopes) {
    auto bytes = base_table();
    bytes.resize(dmar::MAX_TABLE_SIZE + 1);
    checksum(bytes);
    expect_error(bytes, dmar::Error::length);
    bytes = base_table();
    for (size_t i = 0; i < dmar::MAX_RECORDS; ++i) bytes.insert(bytes.end(), {99, 0, 4, 0});
    checksum(bytes);
    expect_error(bytes, dmar::Error::limit);
    bytes = base_table();
    bytes[52] = 0;
    for (size_t i = 0; i <= dmar::MAX_SCOPES; ++i) append_scope(bytes);
    put(bytes, 50, bytes.size() - 48, 2);
    checksum(bytes);
    expect_error(bytes, dmar::Error::limit);
    bytes = base_table();
    bytes.resize(48);
    checksum(bytes);
    expect_error(bytes, dmar::Error::missing_unit);
}

TEST(Dmar, RegisterBankMustBeAlignedToItsFullSize) {
    auto bytes = base_table();
    bytes[53] = 2;  // Four pages.
    put(bytes, 56, 0xfed91000, 8);
    checksum(bytes);
    expect_error(bytes, dmar::Error::register_range);
    put(bytes, 56, 0xfed90000, 8);
    checksum(bytes);
    EXPECT_TRUE(dmar::parse(bytes).valid());
}

TEST(Dmar, SegmentDefaultMustFollowExplicitUnitsAndOnlyListNonPciScopes) {
    auto bytes = base_table();
    append_scope(bytes);
    put(bytes, 50, 24, 2);
    checksum(bytes);
    expect_error(bytes, dmar::Error::scope_type);
    bytes[64] = 3;
    checksum(bytes);
    EXPECT_TRUE(dmar::parse(bytes).valid());
    bytes[64] = 4;
    bytes[65] = 10;
    bytes.insert(bytes.end(), {0, 0});
    put(bytes, 50, 26, 2);
    checksum(bytes);
    expect_error(bytes, dmar::Error::scope_path);
    bytes = base_table();
    auto unit = std::vector<uint8_t>(bytes.begin() + 48, bytes.end());
    unit[4] = 0;
    unit[2] = 24;
    unit[10] = 0xda;
    bytes.insert(bytes.end(), unit.begin(), unit.end());
    append_scope(bytes);
    checksum(bytes);
    expect_error(bytes, dmar::Error::record_order);
}
