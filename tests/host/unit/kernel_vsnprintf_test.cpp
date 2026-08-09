#include <gtest/gtest.h>

#include <array>
#include <cstdarg>
#include <cstdint>
#include <cstring>
#include <limits>
#include <util/string.hpp>

namespace {

auto format_into(char* output, size_t capacity, const char* format, ...) -> int {
    va_list args;
    va_start(args, format);
    int const result = ker::util::string::vsnprintf(output, capacity, format, args);
    va_end(args);
    return result;
}

TEST(KernelVsnprintf, ExactFitAndTruncationReturnRequiredLength) {
    std::array<char, 5> exact{};
    EXPECT_EQ(format_into(exact.data(), exact.size(), "%s", "abcd"), 4);
    EXPECT_STREQ(exact.data(), "abcd");

    std::array<char, 4> truncated{};
    EXPECT_EQ(format_into(truncated.data(), truncated.size(), "%s", "abcdef"), 6);
    EXPECT_STREQ(truncated.data(), "abc");
}

TEST(KernelVsnprintf, LongJournalSizedInputCannotCrossCanaries) {
    struct GuardedBuffer {
        std::array<uint8_t, 16> before{};
        std::array<char, 512> output{};
        std::array<uint8_t, 16> after{};
    } guarded;
    guarded.before.fill(0xA5);
    guarded.after.fill(0x5A);

    std::array<char, 701> source{};
    source.fill('q');
    source.back() = '\0';

    EXPECT_EQ(format_into(guarded.output.data(), guarded.output.size(), "%s", source.data()), 700);
    EXPECT_EQ(std::strlen(guarded.output.data()), 511U);
    EXPECT_EQ(guarded.output.front(), 'q');
    EXPECT_EQ(guarded.output.at(510), 'q');
    EXPECT_EQ(guarded.output.back(), '\0');
    for (uint8_t value : guarded.before) {
        EXPECT_EQ(value, 0xA5);
    }
    for (uint8_t value : guarded.after) {
        EXPECT_EQ(value, 0x5A);
    }
}

TEST(KernelVsnprintf, ZeroAndOneByteBuffersKeepCSemantics) {
    std::array<char, 1> one{'x'};
    EXPECT_EQ(format_into(one.data(), one.size(), "abc"), 3);
    EXPECT_EQ(one.front(), '\0');

    EXPECT_EQ(format_into(nullptr, 0, "abc%s", "def"), 6);

    std::array<char, 2> untouched{'x', 'y'};
    EXPECT_EQ(format_into(untouched.data(), 0, "abc"), 3);
    EXPECT_EQ(untouched.at(0), 'x');
    EXPECT_EQ(untouched.at(1), 'y');
}

TEST(KernelVsnprintf, SupportedConversionsRetainFormatting) {
    std::array<char, 160> output{};
    auto* const pointer = reinterpret_cast<void*>(uintptr_t{0x1234});
    int const result = format_into(output.data(), output.size(), "%s:%04x:%lld:%zu:%c:%%:%.3s:%p:%h:%b", "tag", 0x2aU,
                                   -9223372036854775807LL, size_t{17}, 'Z', "abcdef", pointer, 0x0a, 5U);
    EXPECT_STREQ(output.data(), "tag:002a:-9223372036854775807:17:Z:%:abc:0x1234:0a:101");
    EXPECT_EQ(result, static_cast<int>(std::strlen(output.data())));
}

TEST(KernelVsnprintf, InvalidArgumentsAndUnrepresentableLengthReturnError) {
    std::array<char, 4> output{'x', 'x', 'x', 'x'};
    EXPECT_EQ(format_into(output.data(), output.size(), nullptr), -1);
    EXPECT_EQ(output.front(), '\0');
    EXPECT_EQ(format_into(nullptr, 1, "x"), -1);

    int const max_int = std::numeric_limits<int>::max();
    EXPECT_EQ(format_into(output.data(), 1, "%*s", max_int, ""), max_int);
    EXPECT_EQ(output.front(), '\0');
    EXPECT_EQ(format_into(output.data(), 1, "%*sx", max_int, ""), -1);
    EXPECT_EQ(output.front(), '\0');
}

}  // namespace
