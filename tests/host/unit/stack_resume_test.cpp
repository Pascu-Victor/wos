#include <gtest/gtest.h>

#include <array>
#include <sanitizer/stack_resume.hpp>

using ker::mod::kasan::stack_resume_range;

TEST(StackResume, RejectsInvalidBoundsAndForeignStack) {
    for (auto const R : {stack_resume_range(0, 512, 1), stack_resume_range(4096, 0, 4096), stack_resume_range(4096, 512, 3584),
                         stack_resume_range(4096, 512, 4097), stack_resume_range(4097, 512, 4096), stack_resume_range(4096, 511, 4096)}) {
        EXPECT_EQ(R.size, 0U);
    }
}
TEST(StackResume, ClearsOnlyDeadGranulesBelowEveryResumeBoundary) {
    constexpr uintptr_t BASE = 4096;
    constexpr size_t SIZE = 512;
    for (uintptr_t rsp = BASE + 1; rsp <= BASE + SIZE; ++rsp) {
        std::array<unsigned char, SIZE / 8> shadow{};
        shadow.fill(0xf1);
        auto const RANGE = stack_resume_range(BASE + SIZE, SIZE, rsp);
        ASSERT_EQ(RANGE.begin, BASE);
        ASSERT_LE(RANGE.size, SIZE);
        for (size_t i = 0; i < RANGE.size / 8; ++i) shadow[i] = 0;
        for (size_t i = 0; i < shadow.size(); ++i) {
            EXPECT_EQ(shadow[i], BASE + (i + 1) * 8 <= rsp ? 0 : 0xf1);
        }
    }
}
