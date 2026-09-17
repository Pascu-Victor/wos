#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <span>
#include <vfs/content_match.hpp>

namespace {
constexpr std::array<uint8_t, 9> IMAGE{0x7f, 'E', 'L', 'F', 1, 2, 3, 4, 5};

auto compare(std::span<const uint8_t> actual, size_t max_read = 100) -> bool {
    std::array<uint8_t, 4> scratch{};
    return ker::vfs::content_matches(
        IMAGE, scratch,
        [&](uint8_t* bytes, size_t size, size_t offset) -> ptrdiff_t {
            if (offset >= actual.size()) return 0;
            size = std::min({size, actual.size() - offset, max_read});
            std::memcpy(bytes, actual.data() + offset, size);
            return static_cast<ptrdiff_t>(size);
        },
        [] { return true; });
}
}  // namespace

TEST(FileContentMatch, AcceptsExactBytesAcrossBoundedAndPartialReads) {
    EXPECT_TRUE(compare(IMAGE));
    EXPECT_TRUE(compare(IMAGE, 1));
    EXPECT_TRUE(compare(IMAGE, 3));
}

TEST(FileContentMatch, RejectsEveryChangedByteAndTruncatedPrefix) {
    for (size_t offset = 0; offset < IMAGE.size(); ++offset) {
        auto changed = IMAGE;
        changed.at(offset) ^= 1;
        EXPECT_FALSE(compare(changed));
        EXPECT_FALSE(compare(std::span(IMAGE).first(offset)));
    }
    std::array<uint8_t, IMAGE.size() + 1> extended{};
    std::copy(IMAGE.begin(), IMAGE.end(), extended.begin());
    EXPECT_FALSE(compare(extended));
}

TEST(FileContentMatch, RejectsErrorsAndOversizedReaderResultsBeforeComparison) {
    std::array<uint8_t, 4> scratch{};
    for (ptrdiff_t error : {-1, 0, 5}) {
        EXPECT_FALSE(ker::vfs::content_matches(IMAGE, scratch, [=](uint8_t*, size_t, size_t) { return error; }, [] { return true; }));
    }
    EXPECT_FALSE(ker::vfs::content_matches(
        IMAGE, scratch,
        [](uint8_t* bytes, size_t size, size_t offset) -> ptrdiff_t {
            if (offset == IMAGE.size()) return -1;
            std::memcpy(bytes, IMAGE.data() + offset, size);
            return static_cast<ptrdiff_t>(size);
        },
        [] { return true; }));
}

TEST(FileContentMatch, CancellationIsCheckedBeforeReadsAndAfterEof) {
    std::array<uint8_t, 4> scratch{};
    for (unsigned permitted = 0; permitted < 5; ++permitted) {
        unsigned checks = 0;
        EXPECT_FALSE(ker::vfs::content_matches(
            IMAGE, scratch,
            [](uint8_t* bytes, size_t size, size_t offset) -> ptrdiff_t {
                if (offset == IMAGE.size()) return 0;
                std::memcpy(bytes, IMAGE.data() + offset, size);
                return static_cast<ptrdiff_t>(size);
            },
            [&] { return checks++ < permitted; }));
        EXPECT_EQ(checks, permitted + 1);
    }
}

TEST(FileContentMatch, EmptyInputsCannotValidateACandidate) {
    std::array<uint8_t, 4> scratch{};
    unsigned reads = 0;
    auto read = [&](uint8_t*, size_t, size_t) {
        ++reads;
        return 0;
    };
    EXPECT_FALSE(ker::vfs::content_matches({}, scratch, read, [] { return true; }));
    EXPECT_FALSE(ker::vfs::content_matches(IMAGE, {}, read, [] { return true; }));
    EXPECT_EQ(reads, 0U);
}
