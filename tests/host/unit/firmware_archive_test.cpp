#include <gtest/gtest.h>
#include <sys/mman.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <dev/firmware/archive.hpp>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace {
namespace firmware = ker::dev::firmware;
using Error = firmware::Error;
constexpr uint32_t REGULAR = 0100644;
constexpr uint32_t DIRECTORY = 0040755;

void field(std::span<uint8_t> bytes, size_t at, uint32_t value) {
    constexpr std::string_view HEX = "0123456789abcdef";
    for (size_t i = 0; i < 8; ++i) bytes[at + i] = HEX[(value >> (28U - i * 4U)) & 15U];
}
struct Record {
    size_t offset, name, name_end, data, data_end, next;
};
struct Cpio {
    std::vector<uint8_t> bytes;
    auto append(std::string_view name, std::string_view contents = "abc", uint32_t mode = REGULAR, uint32_t links = 1) -> Record {
        size_t const OFFSET = bytes.size();
        bytes.resize(OFFSET + 110, '0');
        std::copy_n("070701", 6, bytes.begin() + static_cast<ptrdiff_t>(OFFSET));
        field(bytes, OFFSET + 14, mode);
        field(bytes, OFFSET + 38, links);
        field(bytes, OFFSET + 54, static_cast<uint32_t>(contents.size()));
        field(bytes, OFFSET + 94, static_cast<uint32_t>(name.size() + 1));
        size_t const NAME = bytes.size();
        bytes.insert(bytes.end(), name.begin(), name.end());
        bytes.push_back(0);
        size_t const NAME_END = bytes.size();
        while (bytes.size() % 4 != 0) bytes.push_back(0);
        size_t const DATA = bytes.size();
        bytes.insert(bytes.end(), contents.begin(), contents.end());
        size_t const DATA_END = bytes.size();
        while (bytes.size() % 4 != 0) bytes.push_back(0);
        return {OFFSET, NAME, NAME_END, DATA, DATA_END, bytes.size()};
    }
    auto finish(size_t padding = 0) -> Record {
        auto const TRAILER = append("TRAILER!!!", {}, 0);
        bytes.resize(bytes.size() + padding);
        return TRAILER;
    }
};
auto scan(std::span<const uint8_t> bytes) -> firmware::ArchiveResult {
    std::array<firmware::ArchiveEntry, firmware::MAX_INDEX_ENTRIES> output{};
    return firmware::scan_archive(bytes, output);
}
void expect_failure(std::span<const uint8_t> bytes) {
    auto const RESULT = scan(bytes);
    EXPECT_FALSE(RESULT.valid());
    EXPECT_EQ(RESULT.count, 0U);
    EXPECT_LE(RESULT.offset, bytes.size());
}

TEST(FirmwareArchive, CanonicalNamesAreLengthBoundedAndHaveNoTraversalOrEmptyComponents) {
    for (std::string_view name : {"a", "amdgpu/gc_11_0_0.bin", "nvidia/ga102/gsp.bin", "a.b/_-+"})
        EXPECT_TRUE(firmware::canonical_name(name));
    for (std::string_view name : {"", ".", "..", "/a", "./a", "../a", "a/", "a//b", "a/./b", "a/../b", "a/.", "a/.."})
        EXPECT_FALSE(firmware::canonical_name(name));
    EXPECT_FALSE(firmware::canonical_name(std::string_view{"a\0b", 3}));
    EXPECT_TRUE(firmware::canonical_name(std::string(firmware::MAX_NAME_BYTES - 1, 'a')));
    EXPECT_FALSE(firmware::canonical_name(std::string(firmware::MAX_NAME_BYTES, 'a')));
}

TEST(FirmwareArchive, IndexesOnlyFirmwareFilesAsZeroCopyCanonicalViewsIncludingEmptyBlobs) {
    Cpio archive;
    archive.append(".", {}, DIRECTORY, 2);
    archive.append("./lib", {}, DIRECTORY, 3);
    archive.append("lib/firmware", {}, DIRECTORY, 2);
    archive.append("./lib/firmware/amdgpu", {}, DIRECTORY, 2);
    archive.append("etc/config", "unrelated");
    archive.append("lib/firmware-extra/not-firmware.bin");
    auto const FIRST = archive.append("./lib/firmware/amdgpu/test.bin", std::string_view{"a\0b", 3});
    auto const EMPTY = archive.append("lib/firmware/empty.bin", {});
    archive.finish(76);
    auto const BEFORE = archive.bytes;
    std::array<firmware::ArchiveEntry, 2> output{};
    auto const RESULT = firmware::scan_archive(archive.bytes, output);
    ASSERT_TRUE(RESULT.valid());
    ASSERT_EQ(RESULT.count, 2U);
    EXPECT_EQ(output[0].name, "amdgpu/test.bin");
    EXPECT_EQ(output[0].name.data(), reinterpret_cast<const char*>(archive.bytes.data() + FIRST.name + 2 + 13));
    EXPECT_EQ(output[0].bytes.data(), archive.bytes.data() + FIRST.data);
    EXPECT_EQ(output[0].bytes.size(), 3U);
    EXPECT_EQ(output[0].bytes[1], 0);
    EXPECT_EQ(output[1].name, "empty.bin");
    EXPECT_TRUE(output[1].bytes.empty());
    EXPECT_EQ(output[1].bytes.data(), archive.bytes.data() + EMPTY.data);
    EXPECT_EQ(archive.bytes, BEFORE);
}

TEST(FirmwareArchive, ValidArchiveWithoutFirmwareNeedsNoOutputStorage) {
    Cpio archive;
    archive.append(".", {}, DIRECTORY, 2);
    archive.append("sbin/init");
    archive.append("alias", "/sbin/init", 0120777);
    archive.append("hardlink", {}, REGULAR, 2);
    archive.append("dev/null", {}, 0020666);
    archive.finish();
    auto const RESULT = firmware::scan_archive(archive.bytes, {});
    EXPECT_TRUE(RESULT.valid());
    EXPECT_EQ(RESULT.count, 0U);
    Cpio empty;
    empty.finish();
    EXPECT_TRUE(firmware::scan_archive(empty.bytes, {}).valid());
}

TEST(FirmwareArchive, RejectsEveryTruncatedPrefixWithoutPublishingEarlierMatches) {
    Cpio archive;
    archive.append("lib/firmware/a.bin");
    archive.append("lib/firmware/b.bin", "second");
    archive.finish();
    for (size_t size = 0; size < archive.bytes.size(); ++size) {
        SCOPED_TRACE(size);
        expect_failure(std::span{archive.bytes}.first(size));
    }
    EXPECT_TRUE(scan(archive.bytes).valid());
}

TEST(FirmwareArchive, EveryHeaderHexFieldAndMagicByteIsValidatedEvenOnUnrelatedRecords) {
    Cpio archive;
    archive.append("lib/firmware/a.bin");
    auto const LATE = archive.append("etc/late");
    archive.finish();
    for (size_t field_index = 0; field_index < 13; ++field_index) {
        for (size_t digit = 0; digit < 8; ++digit) {
            auto malformed = archive.bytes;
            malformed[LATE.offset + 6 + field_index * 8 + digit] = 'g';
            auto const RESULT = scan(malformed);
            EXPECT_EQ(RESULT.error, Error::header);
            EXPECT_EQ(RESULT.count, 0U);
        }
    }
    for (size_t digit = 0; digit < 6; ++digit) {
        auto malformed = archive.bytes;
        malformed[LATE.offset + digit] = 'x';
        EXPECT_EQ(scan(malformed).error, Error::magic);
    }
    auto uppercase = archive.bytes;
    field(uppercase, LATE.offset + 22, 0xabcdef12);
    for (size_t at = LATE.offset + 22; at < LATE.offset + 30; ++at)
        if (uppercase[at] >= 'a' && uppercase[at] <= 'f') uppercase[at] -= 'a' - 'A';
    EXPECT_TRUE(scan(uppercase).valid());
}

TEST(FirmwareArchive, RejectsChecksumZeroLinksInvalidModesAndNonFilePayloadMetadata) {
    for (int mode = 0; mode < 7; ++mode) {
        Cpio archive;
        auto const ENTRY = archive.append("other", mode >= 4 ? "data" : "", mode == 4 ? DIRECTORY : mode == 5 ? 0010644 : REGULAR);
        archive.finish();
        if (mode == 0) field(archive.bytes, ENTRY.offset + 102, 1);
        if (mode == 1) field(archive.bytes, ENTRY.offset + 38, 0);
        if (mode == 2) field(archive.bytes, ENTRY.offset + 14, 0);
        if (mode == 3) field(archive.bytes, ENTRY.offset + 14, 0x800081a4);
        if (mode == 6) field(archive.bytes, ENTRY.offset + 14, 0020666);
        expect_failure(archive.bytes);
    }
    Cpio root_file;
    root_file.append(".");
    root_file.finish();
    EXPECT_EQ(scan(root_file.bytes).error, Error::entry_type);
}

TEST(FirmwareArchive, RejectsMissingEmbeddedNulsAndImpossibleNameOrDataExtents) {
    for (int mode = 0; mode < 7; ++mode) {
        Cpio archive;
        auto const ENTRY = archive.append("lib/firmware/name.bin");
        archive.finish();
        if (mode == 0) archive.bytes[ENTRY.name_end - 1] = 'x';
        if (mode == 1) archive.bytes[ENTRY.name + 2] = 0;
        if (mode == 2) field(archive.bytes, ENTRY.offset + 94, 0);
        if (mode == 3) field(archive.bytes, ENTRY.offset + 94, UINT32_MAX);
        if (mode == 4) field(archive.bytes, ENTRY.offset + 54, UINT32_MAX);
        if (mode == 5) field(archive.bytes, ENTRY.offset + 94, static_cast<uint32_t>(archive.bytes.size()));
        if (mode == 6) field(archive.bytes, ENTRY.offset + 54, static_cast<uint32_t>(archive.bytes.size()));
        expect_failure(archive.bytes);
    }
}

TEST(FirmwareArchive, RejectsAllTraversalAndNoncanonicalArchivePathSpellings) {
    for (std::string_view name : {"", "/lib/firmware/a", "././lib/firmware/a", "../lib/firmware/a", "lib//firmware/a", "lib/./firmware/a",
                                  "lib/firmware/../a", "lib/firmware/a/", "lib/firmware/", "other/../a", "other//a"}) {
        Cpio archive;
        archive.append(name);
        archive.finish();
        EXPECT_EQ(scan(archive.bytes).error, Error::path) << name;
    }
}

TEST(FirmwareArchive, RejectsDuplicateCanonicalAliasesAndFileDirectoryCollisionsAnywhere) {
    for (bool directory : {false, true}) {
        for (std::string_view first : {"lib/firmware/a.bin", "./lib/firmware/a.bin", "other", "./other", "."}) {
            Cpio archive;
            archive.append(first, {}, directory || first == "." ? DIRECTORY : REGULAR, directory || first == "." ? 2 : 1);
            auto const CANONICAL = first.starts_with("./") ? first.substr(2) : first;
            archive.append(std::string("./") + std::string(CANONICAL), {}, CANONICAL == "." ? DIRECTORY : REGULAR,
                           CANONICAL == "." ? 2 : 1);
            archive.finish();
            EXPECT_EQ(scan(archive.bytes).error, Error::duplicate) << first;
        }
    }
}

TEST(FirmwareArchive, FirmwareSymlinksHardlinksAndSpecialFilesNeverEnterTheIndex) {
    for (uint32_t type : {0120777U, 0020666U, 0060666U, 0010644U, 0140644U}) {
        Cpio archive;
        archive.append("lib/firmware/a.bin", type == 0120777 ? "target" : "", type);
        archive.finish();
        EXPECT_EQ(scan(archive.bytes).error, Error::entry_type);
    }
    Cpio linked;
    linked.append("lib/firmware/a.bin", "abc", REGULAR, 2);
    linked.finish();
    EXPECT_EQ(scan(linked.bytes).error, Error::entry_type);
}

TEST(FirmwareArchive, ArchiveAndFirmwareNameLimitsIncludeTheirTerminatingNul) {
    for (size_t length : {firmware::MAX_NAME_BYTES - 1, firmware::MAX_NAME_BYTES}) {
        Cpio archive;
        archive.append(std::string("lib/firmware/") + std::string(length, 'a'));
        archive.finish();
        auto const RESULT = scan(archive.bytes);
        EXPECT_EQ(RESULT.valid(), length < firmware::MAX_NAME_BYTES);
    }
    for (size_t length : {firmware::MAX_ARCHIVE_NAME_BYTES - 1, firmware::MAX_ARCHIVE_NAME_BYTES}) {
        Cpio archive;
        archive.append(std::string(length, 'a'));
        archive.finish();
        EXPECT_EQ(scan(archive.bytes).valid(), length < firmware::MAX_ARCHIVE_NAME_BYTES);
    }
}

TEST(FirmwareArchive, BothNameAndDataAlignmentPaddingAreValidated) {
    Cpio archive;
    auto const ENTRY = archive.append("lib/firmware/a", "xy");
    archive.finish();
    ASSERT_GT(ENTRY.data, ENTRY.name_end);
    ASSERT_GT(ENTRY.next, ENTRY.data_end);
    for (auto offset : {ENTRY.name_end, ENTRY.data_end}) {
        auto malformed = archive.bytes;
        malformed[offset] = 0xff;
        EXPECT_EQ(scan(malformed).error, Error::header);
    }
}

TEST(FirmwareArchive, RequiresExactTrailerMetadataAndValidatesTheEntireTail) {
    for (int mode = 0; mode < 8; ++mode) {
        Cpio archive;
        archive.append("lib/firmware/a.bin");
        auto const TRAILER = archive.finish();
        if (mode == 0) field(archive.bytes, TRAILER.offset + 14, REGULAR);
        if (mode == 1) field(archive.bytes, TRAILER.offset + 38, 2);
        if (mode == 2) field(archive.bytes, TRAILER.offset + 102, 1);
        if (mode == 3) field(archive.bytes, TRAILER.offset + 54, 1);
        if (mode == 4) archive.bytes.push_back(1);
        if (mode == 5) archive.bytes.resize(archive.bytes.size() + firmware::MAX_TRAILER_PADDING + 1);
        if (mode == 6) {
            Cpio second;
            second.finish();
            archive.bytes.insert(archive.bytes.end(), second.bytes.begin(), second.bytes.end());
        }
        if (mode == 7) archive.bytes.resize(TRAILER.offset);
        expect_failure(archive.bytes);
    }
    Cpio alias;
    alias.append("./TRAILER!!!", {}, 0);
    EXPECT_EQ(scan(alias.bytes).error, Error::trailer);
    for (size_t padding : {size_t{0}, size_t{1}, firmware::MAX_TRAILER_PADDING}) {
        Cpio archive;
        archive.finish(padding);
        EXPECT_TRUE(scan(archive.bytes).valid());
    }
}

TEST(FirmwareArchive, IndexCapacityAndPrivateScratchFailuresNeverPublishAPartialCount) {
    Cpio archive;
    for (size_t i = 0; i < firmware::MAX_INDEX_ENTRIES; ++i) archive.append("lib/firmware/f" + std::to_string(i), {});
    Cpio const EXACT = archive;
    archive.finish();
    auto const RESULT = scan(archive.bytes);
    EXPECT_TRUE(RESULT.valid());
    EXPECT_EQ(RESULT.count, firmware::MAX_INDEX_ENTRIES);
    std::array<firmware::ArchiveEntry, 1> small{};
    auto const SHORT = firmware::scan_archive(archive.bytes, small);
    EXPECT_EQ(SHORT.error, Error::limit);
    EXPECT_EQ(SHORT.count, 0U);
    EXPECT_EQ(firmware::scan_archive(archive.bytes, {}).error, Error::limit);
    archive = EXACT;
    archive.append("lib/firmware/one-too-many", {});
    archive.finish();
    EXPECT_EQ(scan(archive.bytes).error, Error::limit);
}

TEST(FirmwareArchive, NearRecordLimitAllows4096TotalHeadersIncludingTheTrailer) {
    Cpio archive;
    for (size_t i = 1; i < firmware::MAX_RECORDS; ++i) archive.append("d" + std::to_string(i), {}, DIRECTORY, 2);
    Cpio const EXACT = archive;
    archive.finish();
    EXPECT_TRUE(scan(archive.bytes).valid());
    archive = EXACT;
    archive.append("extra", {}, DIRECTORY, 2);
    archive.finish();
    auto const RESULT = scan(archive.bytes);
    EXPECT_EQ(RESULT.error, Error::limit);
    EXPECT_EQ(RESULT.count, 0U);
}

struct Mapping {
    void* address;
    size_t size;
    explicit Mapping(size_t bytes)
        : address(mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0)), size(bytes) {}
    ~Mapping() {
        if (address != MAP_FAILED) munmap(address, size);
    }
    Mapping(const Mapping&) = delete;
    auto operator=(const Mapping&) -> Mapping& = delete;
    auto bytes() const -> std::span<uint8_t> { return {static_cast<uint8_t*>(address), size}; }
};

TEST(FirmwareArchive, LargeBlobLimitUsesAccessibleSparseHostMemoryWithoutPayloadCopies) {
    // Anonymous mappings reserve address space; only the small headers/trailer
    // are touched. No 256-MiB payload is copied, hashed or stored on disk.
    for (size_t length : {firmware::MAX_BLOB_BYTES, firmware::MAX_BLOB_BYTES + 1}) {
        Cpio prefix;
        auto const ENTRY = prefix.append("lib/firmware/large.bin", {});
        Cpio trailer;
        trailer.finish();
        size_t const TRAILER_OFFSET = (ENTRY.data + length + 3U) & ~size_t{3};
        Mapping mapping(TRAILER_OFFSET + trailer.bytes.size());
        ASSERT_NE(mapping.address, MAP_FAILED);
        auto const BYTES = mapping.bytes();
        std::copy(prefix.bytes.begin(), prefix.bytes.end(), BYTES.begin());
        field(BYTES, ENTRY.offset + 54, static_cast<uint32_t>(length));
        std::copy(trailer.bytes.begin(), trailer.bytes.end(), BYTES.begin() + static_cast<ptrdiff_t>(TRAILER_OFFSET));
        std::array<firmware::ArchiveEntry, 1> output{};
        auto const RESULT = firmware::scan_archive(BYTES, output);
        EXPECT_EQ(RESULT.valid(), length <= firmware::MAX_BLOB_BYTES);
        if (RESULT.valid()) {
            EXPECT_EQ(RESULT.count, 1U);
            EXPECT_EQ(output[0].bytes.size(), length);
            EXPECT_EQ(output[0].bytes.data(), BYTES.data() + ENTRY.data);
        } else {
            EXPECT_EQ(RESULT.error, Error::length);
            EXPECT_EQ(RESULT.count, 0U);
        }
    }
}

TEST(FirmwareArchive, SourceLimitRefusesOversizedAccessibleExtentBeforeParsing) {
    Mapping mapping(firmware::MAX_SOURCE_BYTES + 1);
    ASSERT_NE(mapping.address, MAP_FAILED);
    auto const RESULT = firmware::scan_archive(mapping.bytes(), {});
    EXPECT_EQ(RESULT.error, Error::source_limit);
    EXPECT_EQ(RESULT.count, 0U);
}

}  // namespace
