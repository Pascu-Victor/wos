#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <platform/acpi/bounded_tables.hpp>
#include <vector>

namespace acpi = ker::mod::acpi::bounded;
namespace {
struct Firmware {
    std::array<uint8_t, 8192> data{};
    size_t readable = data.size();
    std::vector<std::pair<uint64_t, size_t>> reads;

    void put(size_t at, uint64_t value, size_t count) {
        for (size_t i = 0; i < count; ++i) data[at + i] = static_cast<uint8_t>(value >> (8 * i));
    }
    void sum(size_t at, size_t size, size_t field) {
        data[at + field] = 0;
        uint8_t value = 0;
        for (size_t i = 0; i < size; ++i) value = static_cast<uint8_t>(value + data[at + i]);
        data[at + field] = static_cast<uint8_t>(0U - value);
    }
    void table(size_t at, std::string_view signature, size_t length) {
        std::copy(signature.begin(), signature.end(), data.begin() + at);
        put(at + 4, length, 4);
        sum(at, length, 9);
    }
    explicit Firmware(bool extended = true) {
        std::string_view const SIGNATURE = "RSD PTR ";
        std::copy(SIGNATURE.begin(), SIGNATURE.end(), data.begin());
        data[15] = extended ? 2 : 0;
        put(16, 256, 4);
        put(20, 36, 4);
        put(24, extended ? 256 : 0, 8);
        sum(0, 20, 8);
        sum(0, 36, 32);
        put(256 + 36, 4096, extended ? 8 : 4);
        table(256, extended ? "XSDT" : "RSDT", extended ? 44 : 40);
        table(4096, "DMAR", 48);
    }
    auto operator()(uint64_t physical, size_t size) -> std::span<const uint8_t> {
        reads.emplace_back(physical, size);
        if (!acpi::contains(0, readable, physical, size)) return {};
        return std::span<const uint8_t>{data}.subspan(physical, size);
    }
};
}  // namespace

TEST(AcpiBounded, FindsLegacyAndExtendedTablesAndBorrowsValidatedBytes) {
    for (bool extended : {false, true}) {
        Firmware firmware(extended);
        auto const RESULT = acpi::find(firmware, 0, "DMAR");
        ASSERT_TRUE(RESULT.valid());
        EXPECT_EQ(RESULT.physical, 4096U);
        EXPECT_EQ(RESULT.bytes.size(), 48U);
        EXPECT_EQ(RESULT.bytes.data(), firmware.data.data() + 4096);
        EXPECT_EQ(acpi::find(firmware, 0, "NOPE").error, acpi::Error::not_found);
    }
}

TEST(AcpiBounded, RejectsEachInaccessibleReadWithoutReturningPartialTable) {
    for (size_t available : {0, 19, 20, 35, 256 + 35, 256 + 43, 4096 + 35, 4096 + 47}) {
        Firmware firmware;
        firmware.readable = available;
        auto const RESULT = acpi::find(firmware, 0, "DMAR");
        EXPECT_EQ(RESULT.error, acpi::Error::inaccessible) << available;
        EXPECT_TRUE(RESULT.bytes.empty());
    }
}

TEST(AcpiBounded, RejectsCorruptChecksumsAtEveryLevel) {
    for (size_t byte : {10, 33, 256 + 10, 4096 + 10}) {
        Firmware firmware;
        firmware.data[byte] ^= 1;
        EXPECT_EQ(acpi::find(firmware, 0, "DMAR").error, acpi::Error::checksum) << byte;
    }
}

TEST(AcpiBounded, CapsUntrustedLengthsBeforeReadingThem) {
    for (uint64_t length : {0U, 35U, 1024U * 1024U + 1U, UINT32_MAX}) {
        Firmware firmware;
        firmware.put(4100, length, 4);
        EXPECT_EQ(acpi::find(firmware, 0, "DMAR").error, acpi::Error::length);
        EXPECT_EQ(firmware.reads.back(), (std::pair<uint64_t, size_t>{4096, 36}));
    }
    for (uint64_t length : {0U, 35U, 4097U, UINT32_MAX}) {
        Firmware firmware;
        firmware.put(20, length, 4);
        EXPECT_EQ(acpi::find(firmware, 0, "DMAR").error, acpi::Error::length);
        EXPECT_EQ(firmware.reads.back(), (std::pair<uint64_t, size_t>{0, 36}));
    }
}

TEST(AcpiBounded, RejectsDuplicateRootEntriesBadSignaturesAndEntryCounts) {
    Firmware duplicate;
    duplicate.put(300, 4096, 8);
    duplicate.table(256, "XSDT", 52);
    EXPECT_EQ(acpi::find(duplicate, 0, "DMAR").error, acpi::Error::duplicate);
    EXPECT_TRUE(acpi::find(duplicate, 0, "DMAR").bytes.empty());
    for (size_t length : {37U, 36U + 257U * 8U}) {
        Firmware firmware;
        firmware.table(256, "XSDT", length);
        EXPECT_EQ(acpi::find(firmware, 0, "DMAR").error, acpi::Error::root_entries);
    }
    Firmware signature;
    signature.table(256, "RSDT", 44);
    EXPECT_EQ(acpi::find(signature, 0, "DMAR").error, acpi::Error::signature);
    EXPECT_EQ(acpi::find(signature, 0, "BAD").error, acpi::Error::signature);
}

TEST(AcpiBounded, CheckedRangesRejectOverflowZeroAndBoundaryCrossing) {
    EXPECT_TRUE(acpi::contains(100, 100, 100, 100));
    EXPECT_TRUE(acpi::contains(100, 100, 199, 1));
    EXPECT_FALSE(acpi::contains(100, 100, 99, 1));
    EXPECT_FALSE(acpi::contains(100, 100, 200, 1));
    EXPECT_FALSE(acpi::contains(100, 100, 100, 101));
    EXPECT_FALSE(acpi::contains(0, UINT64_MAX, 1, UINT64_MAX));
    EXPECT_FALSE(acpi::contains(UINT64_MAX, 2, UINT64_MAX, 1));
    EXPECT_FALSE(acpi::contains(0, 100, 0, 0));
    Firmware firmware;
    firmware.put(292, UINT64_MAX - 15, 8);
    firmware.sum(256, 44, 9);
    EXPECT_EQ(acpi::find(firmware, 0, "DMAR").error, acpi::Error::inaccessible);
}
