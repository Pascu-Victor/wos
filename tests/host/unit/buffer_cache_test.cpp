// Host unit tests for the buffer cache (bread / bwrite / brelse / invalidate).
//
// Compiles buffer_cache.cpp with kernel shims so tests run natively without QEMU.
// A null block device fills reads with zeros and silently accepts writes.

#include <gtest/gtest.h>

#include <cstring>
#include <dev/block_device.hpp>
#include <vfs/buffer_cache.hpp>

using namespace ker::vfs;
using namespace ker::dev;

// ---------------------------------------------------------------------------
// Test fixture: null block device + per-test cache invalidation
// ---------------------------------------------------------------------------

namespace {

auto null_read(BlockDevice* dev, uint64_t /*blk*/, size_t count, void* buf) -> int {
    memset(buf, 0, count * dev->block_size);
    return 0;
}

auto null_write(BlockDevice* /*dev*/, uint64_t /*blk*/, size_t /*count*/,
                const void* /*buf*/) -> int {
    return 0;
}

BlockDevice make_null_bdev(size_t block_size = 512, uint64_t total_blocks = 1024) {
    BlockDevice d{};
    d.block_size   = block_size;
    d.total_blocks = total_blocks;
    d.read_blocks  = null_read;
    d.write_blocks = null_write;
    return d;
}

}  // namespace

class BufferCacheTest : public ::testing::Test {
protected:
    BlockDevice dev;

    void SetUp() override {
        dev = make_null_bdev();
        buffer_cache_init();
        invalidate_bdev(&dev);
    }

    void TearDown() override { invalidate_bdev(&dev); }
};

// ---------------------------------------------------------------------------
// Cache hit: two bread() calls for the same block return the same pointer
// ---------------------------------------------------------------------------

TEST_F(BufferCacheTest, CacheHit) {
    BufHead* bh1 = bread(&dev, 1);
    ASSERT_NE(bh1, nullptr);
    BufHead* bh2 = bread(&dev, 1);
    ASSERT_NE(bh2, nullptr);
    EXPECT_EQ(bh1, bh2);
    brelse(bh2);
    brelse(bh1);
}

// ---------------------------------------------------------------------------
// bread / brelse: no crash, block_no and size fields correctly set
// ---------------------------------------------------------------------------

TEST_F(BufferCacheTest, BreadBrelse) {
    BufHead* bh = bread(&dev, 5);
    ASSERT_NE(bh, nullptr);
    EXPECT_EQ(bh->block_no, 5u);
    EXPECT_EQ(bh->size,     dev.block_size);
    EXPECT_NE(bh->data,     nullptr);
    brelse(bh);
}

// ---------------------------------------------------------------------------
// Write-readback: pattern written via bwrite is visible on next bread
// (cache hit path, same BufHead pointer)
// ---------------------------------------------------------------------------

TEST_F(BufferCacheTest, WriteReadback) {
    BufHead* bh = bread(&dev, 3);
    ASSERT_NE(bh, nullptr);

    for (size_t i = 0; i < bh->size; ++i) {
        bh->data[i] = static_cast<uint8_t>(i & 0xFF);
    }
    EXPECT_EQ(bwrite(bh), 0);
    brelse(bh);

    BufHead* bh2 = bread(&dev, 3);
    ASSERT_NE(bh2, nullptr);
    bool ok = true;
    for (size_t i = 0; i < bh2->size; ++i) {
        if (bh2->data[i] != static_cast<uint8_t>(i & 0xFF)) {
            ok = false;
            break;
        }
    }
    EXPECT_TRUE(ok);
    brelse(bh2);
}

// ---------------------------------------------------------------------------
// Invalidate: cache miss after invalidate → null_read fills zeros
// ---------------------------------------------------------------------------

TEST_F(BufferCacheTest, InvalidateCausesReread) {
    BufHead* bh = bread(&dev, 10);
    ASSERT_NE(bh, nullptr);
    memset(bh->data, 0xAB, bh->size);
    brelse(bh);

    invalidate_bdev(&dev);

    BufHead* bh2 = bread(&dev, 10);
    ASSERT_NE(bh2, nullptr);
    bool is_zero = true;
    for (size_t i = 0; i < bh2->size; ++i) {
        if (bh2->data[i] != 0) { is_zero = false; break; }
    }
    EXPECT_TRUE(is_zero);
    brelse(bh2);
}

// ---------------------------------------------------------------------------
// Different blocks: bread for distinct block numbers returns distinct pointers
// ---------------------------------------------------------------------------

TEST_F(BufferCacheTest, DistinctBlocks) {
    BufHead* bh_a = bread(&dev, 7);
    BufHead* bh_b = bread(&dev, 8);
    ASSERT_NE(bh_a, nullptr);
    ASSERT_NE(bh_b, nullptr);
    EXPECT_NE(bh_a, bh_b);
    EXPECT_EQ(bh_a->block_no, 7u);
    EXPECT_EQ(bh_b->block_no, 8u);
    brelse(bh_a);
    brelse(bh_b);
}

// ---------------------------------------------------------------------------
// LRU pressure: allocate many blocks without keeping references;
// subsequent re-reads must still succeed (eviction must not break anything).
// ---------------------------------------------------------------------------

TEST_F(BufferCacheTest, LruPressure) {
    for (uint64_t blk = 0; blk < 200; ++blk) {
        BufHead* bh = bread(&dev, blk);
        ASSERT_NE(bh, nullptr) << "bread failed at block " << blk;
        brelse(bh);
    }
    // Re-read a few early blocks — must not crash regardless of eviction state
    for (uint64_t blk = 0; blk < 5; ++blk) {
        BufHead* bh = bread(&dev, blk);
        EXPECT_NE(bh, nullptr);
        if (bh) brelse(bh);
    }
}

// ---------------------------------------------------------------------------
// bdirty: marking dirty then syncing should not crash
// ---------------------------------------------------------------------------

TEST_F(BufferCacheTest, BdirtyAndSync) {
    BufHead* bh = bread(&dev, 20);
    ASSERT_NE(bh, nullptr);
    bh->data[0] = 0xBB;
    bdirty(bh);
    brelse(bh);
    EXPECT_EQ(sync_blockdev(&dev), 0);
}

// ---------------------------------------------------------------------------
// Buffer cache stats: hits/misses update after operations
// ---------------------------------------------------------------------------

TEST_F(BufferCacheTest, Stats) {
    BufferCacheStats before = buffer_cache_stats();

    BufHead* bh = bread(&dev, 99);
    ASSERT_NE(bh, nullptr);
    brelse(bh);
    // Second access is a cache hit
    bh = bread(&dev, 99);
    ASSERT_NE(bh, nullptr);
    brelse(bh);

    BufferCacheStats after = buffer_cache_stats();
    // At least one miss (cold start) and one hit
    EXPECT_GT(after.misses, before.misses);
    EXPECT_GT(after.hits,   before.hits);
}
