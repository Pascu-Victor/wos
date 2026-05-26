#include <gtest/gtest.h>

#include <minimalist_malloc/bitmap.hpp>

// Bitmap<N>: N valid bit positions (0 to N-1), storage is N uint32_t words.

TEST(Bitmap, InitAllClear) {
    Bitmap<64> bm;
    bm.init();
    for (unsigned i = 0; i < 64; ++i) {
        EXPECT_FALSE(bm.check_used(i)) << "bit " << i << " should be clear after init";
    }
}

TEST(Bitmap, SetAndCheck) {
    Bitmap<64> bm;
    bm.init();
    bm.set_used(0);
    bm.set_used(15);
    bm.set_used(31);
    bm.set_used(63);
    EXPECT_TRUE(bm.check_used(0));
    EXPECT_TRUE(bm.check_used(15));
    EXPECT_TRUE(bm.check_used(31));
    EXPECT_TRUE(bm.check_used(63));
    EXPECT_FALSE(bm.check_used(1));
    EXPECT_FALSE(bm.check_used(32));
}

TEST(Bitmap, SetUnset) {
    Bitmap<64> bm;
    bm.init();
    bm.set_used(10);
    EXPECT_TRUE(bm.check_used(10));
    EXPECT_TRUE(bm.check_unused(10) == false);
    bm.set_unused(10);
    EXPECT_FALSE(bm.check_used(10));
    EXPECT_TRUE(bm.check_unused(10));
}

TEST(Bitmap, FindUnusedEmpty) {
    Bitmap<64> bm;
    bm.init();
    EXPECT_EQ(bm.find_unused(0), 0u);
}

TEST(Bitmap, FindUnusedAfterSets) {
    Bitmap<64> bm;
    bm.init();
    bm.set_used(0);
    bm.set_used(1);
    bm.set_used(2);
    EXPECT_EQ(bm.find_unused(0), 3u);
    EXPECT_EQ(bm.find_unused(3), 3u);
}

TEST(Bitmap, FindUnusedFromMiddle) {
    Bitmap<64> bm;
    bm.init();
    bm.set_used(0);
    bm.set_used(1);
    bm.set_used(3);
    // start at 2 — should find 2 (not used)
    EXPECT_EQ(bm.find_unused(2), 2u);
    // start at 3 — 3 is used, so 4 is first unused
    EXPECT_EQ(bm.find_unused(3), 4u);
}

TEST(Bitmap, FindUnusedAllFull) {
    Bitmap<32> bm;
    bm.init();
    for (unsigned i = 0; i < 32; ++i) {
        bm.set_used(i);
    }
    EXPECT_EQ(bm.find_unused(0), static_cast<unsigned>(BITMAP_NO_BITS_LEFT));
}

TEST(Bitmap, FindUsedNoneSet) {
    Bitmap<64> bm;
    bm.init();
    EXPECT_EQ(bm.find_used(0), static_cast<unsigned>(BITMAP_NO_BITS_LEFT));
}

TEST(Bitmap, FindUsedBasic) {
    Bitmap<64> bm;
    bm.init();
    bm.set_used(7);
    bm.set_used(20);
    EXPECT_EQ(bm.find_used(0), 7u);
    EXPECT_EQ(bm.find_used(7), 7u);
    EXPECT_EQ(bm.find_used(8), 20u);
    EXPECT_EQ(bm.find_used(21), static_cast<unsigned>(BITMAP_NO_BITS_LEFT));
}

TEST(Bitmap, ToggleMultipleTimes) {
    Bitmap<64> bm;
    bm.init();
    for (int round = 0; round < 4; ++round) {
        bm.set_used(5);
        EXPECT_TRUE(bm.check_used(5));
        bm.set_unused(5);
        EXPECT_FALSE(bm.check_used(5));
    }
}

TEST(Bitmap, AllBitsSetSequentially) {
    Bitmap<32> bm;
    bm.init();
    for (unsigned i = 0; i < 32; ++i) {
        EXPECT_EQ(bm.find_unused(0), i);
        bm.set_used(i);
    }
    EXPECT_EQ(bm.find_unused(0), static_cast<unsigned>(BITMAP_NO_BITS_LEFT));
    // Free all and verify they come back
    for (unsigned i = 0; i < 32; ++i) {
        bm.set_unused(i);
    }
    EXPECT_EQ(bm.find_unused(0), 0u);
}
