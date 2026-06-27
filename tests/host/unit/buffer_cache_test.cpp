// Host unit tests for the buffer cache (bread / bwrite / brelse / invalidate).
//
// Compiles buffer_cache.cpp with kernel shims so tests run natively without QEMU.
// A null block device fills reads with zeros and silently accepts writes.

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <dev/block_device.hpp>
#include <limits>
#include <mutex>
#include <thread>
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

auto null_write(BlockDevice* /*dev*/, uint64_t /*blk*/, size_t /*count*/, const void* /*buf*/) -> int { return 0; }

BlockDevice make_null_bdev(size_t block_size = 512, uint64_t total_blocks = 1024) {
    BlockDevice d{};
    d.block_size = block_size;
    d.total_blocks = total_blocks;
    d.read_blocks = null_read;
    d.write_blocks = null_write;
    return d;
}

struct BlockingReadState {
    std::mutex mutex;
    std::condition_variable cv;
    size_t active_reads = 0;
    size_t read_calls = 0;
    bool release = false;
};

auto blocking_read(BlockDevice* dev, uint64_t /*blk*/, size_t count, void* buf) -> int {
    auto* state = static_cast<BlockingReadState*>(dev->private_data);
    if (state == nullptr) {
        return null_read(dev, 0, count, buf);
    }

    {
        std::unique_lock lock(state->mutex);
        state->active_reads++;
        state->read_calls++;
        state->cv.notify_all();
        state->cv.wait(lock, [&state] { return state->release; });
    }

    memset(buf, 0x5A, count * dev->block_size);
    return 0;
}

struct RecordingIoState {
    size_t write_calls = 0;
    size_t flush_calls = 0;
    uint64_t last_write_block = 0;
    size_t last_write_count = 0;
    bool fail_writes = false;
    size_t fail_write_calls_remaining = 0;
    std::array<uint8_t, 4096> last_write{};
    std::array<uint64_t, 128> write_blocks{};
    std::array<size_t, 128> write_counts{};
    std::array<uint8_t, 128> write_first_bytes{};
};

struct RedirtyingWriteState {
    BufHead* bh = nullptr;
    size_t write_calls = 0;
    uint8_t redirty_value = 0;
    std::array<uint8_t, 4096> last_write{};
};

struct BlockingWriteState {
    std::mutex mutex;
    std::condition_variable cv;
    size_t active_writes = 0;
    size_t write_calls = 0;
    size_t flush_calls = 0;
    bool release = false;
};

auto recording_write(BlockDevice* dev, uint64_t blk, size_t count, const void* buf) -> int {
    auto* state = static_cast<RecordingIoState*>(dev->private_data);
    if (state == nullptr) {
        return 0;
    }

    size_t const BYTE_COUNT = count * dev->block_size;
    if (BYTE_COUNT > state->last_write.size()) {
        return -1;
    }

    size_t const CALL_INDEX = state->write_calls++;
    state->last_write_block = blk;
    state->last_write_count = count;
    if (CALL_INDEX < state->write_blocks.size()) {
        state->write_blocks[CALL_INDEX] = blk;
        state->write_counts[CALL_INDEX] = count;
        state->write_first_bytes[CALL_INDEX] = static_cast<const uint8_t*>(buf)[0];
    }
    bool const FAIL_THIS_WRITE = state->fail_writes || state->fail_write_calls_remaining != 0;
    if (state->fail_write_calls_remaining != 0) {
        state->fail_write_calls_remaining--;
    }
    if (FAIL_THIS_WRITE) {
        return -1;
    }
    memcpy(state->last_write.data(), buf, BYTE_COUNT);
    return 0;
}

auto redirtying_write(BlockDevice* dev, uint64_t /*blk*/, size_t count, const void* buf) -> int {
    auto* state = static_cast<RedirtyingWriteState*>(dev->private_data);
    if (state == nullptr || state->bh == nullptr) {
        return 0;
    }

    size_t const BYTE_COUNT = count * dev->block_size;
    if (BYTE_COUNT > state->last_write.size()) {
        return -1;
    }

    state->write_calls++;
    state->bh->data[0] = state->redirty_value;
    bdirty(state->bh);
    memcpy(state->last_write.data(), buf, BYTE_COUNT);
    return 0;
}

auto recording_flush(BlockDevice* dev) -> int {
    auto* state = static_cast<RecordingIoState*>(dev->private_data);
    if (state == nullptr) {
        return 0;
    }
    state->flush_calls++;
    return 0;
}

auto blocking_write(BlockDevice* dev, uint64_t /*blk*/, size_t /*count*/, const void* /*buf*/) -> int {
    auto* state = static_cast<BlockingWriteState*>(dev->private_data);
    if (state == nullptr) {
        return 0;
    }

    std::unique_lock lock(state->mutex);
    state->active_writes++;
    state->write_calls++;
    state->cv.notify_all();
    state->cv.wait(lock, [&state] { return state->release; });
    return 0;
}

auto blocking_flush(BlockDevice* dev) -> int {
    auto* state = static_cast<BlockingWriteState*>(dev->private_data);
    if (state == nullptr) {
        return 0;
    }

    std::lock_guard lock(state->mutex);
    state->flush_calls++;
    state->cv.notify_all();
    return 0;
}

auto test_hash_key(const BlockDevice* bdev, uint64_t block_no, size_t size) -> size_t {
    auto h = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(bdev));
    h ^= block_no;
    h *= 0x517cc1b727220a95ULL;
    h ^= static_cast<uint64_t>(size);
    h *= 0x517cc1b727220a95ULL;
    h ^= (h >> 32);
    return static_cast<size_t>(h % BUFFER_CACHE_HASH_BUCKETS);
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
    EXPECT_EQ(bh->size, dev.block_size);
    EXPECT_NE(bh->data, nullptr);
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

TEST_F(BufferCacheTest, FailedBwriteMarksCleanBufferDirtyForRetry) {
    RecordingIoState io{};
    io.fail_writes = true;
    dev.write_blocks = recording_write;
    dev.private_data = &io;

    constexpr uint64_t BLOCK_NO = 30;
    BufHead* bh = bget(&dev, BLOCK_NO);
    ASSERT_NE(bh, nullptr);
    bh->data[0] = 0x7E;

    EXPECT_FALSE(has_dirty_bdev_range(&dev, BLOCK_NO, 1));
    EXPECT_EQ(bwrite(bh), -EIO);
    EXPECT_EQ(io.write_calls, 1u);
    EXPECT_TRUE(has_dirty_bdev_range(&dev, BLOCK_NO, 1));
    brelse(bh);

    io.fail_writes = false;
    io.write_calls = 0;
    EXPECT_EQ(sync_blockdev(&dev), 0);
    EXPECT_EQ(io.write_calls, 1u);
    EXPECT_EQ(io.last_write_block, BLOCK_NO);
    EXPECT_EQ(io.last_write_count, 1u);
    EXPECT_EQ(io.last_write[0], 0x7E);
    EXPECT_FALSE(has_dirty_bdev_range(&dev, BLOCK_NO, 1));
    dev.private_data = nullptr;
}

TEST_F(BufferCacheTest, BwriteDoesNotClearForeignWritebackMarker) {
    RecordingIoState io{};
    dev.write_blocks = recording_write;
    dev.private_data = &io;

    constexpr uint64_t BLOCK_NO = 31;
    constexpr uint64_t FOREIGN_EPOCH = 0xBEEFULL;
    BufHead* bh = bget(&dev, BLOCK_NO);
    ASSERT_NE(bh, nullptr);
    bh->data[0] = 0x44;
    bdirty(bh);

    bh->flags |= BH_WRITEBACK;
    bh->writeback_epoch = FOREIGN_EPOCH;
    bh->writeback_dirty_epoch = bh->dirty_epoch;

    EXPECT_EQ(bwrite(bh), 0);
    EXPECT_EQ(io.write_calls, 1u);
    EXPECT_FALSE(has_dirty_bdev_range(&dev, BLOCK_NO, 1));
    EXPECT_NE(bh->flags & BH_WRITEBACK, 0u);
    EXPECT_EQ(bh->writeback_epoch, FOREIGN_EPOCH);

    bh->flags &= ~BH_WRITEBACK;
    bh->writeback_epoch = 0;
    bh->writeback_dirty_epoch = 0;
    brelse(bh);
    dev.private_data = nullptr;
}

TEST_F(BufferCacheTest, BwriteUsesStableSnapshotWhenRedirtiedDuringDeviceWrite) {
    RedirtyingWriteState redirty{};
    redirty.redirty_value = 0x20;
    dev.write_blocks = redirtying_write;
    dev.private_data = &redirty;

    constexpr uint64_t BLOCK_NO = 32;
    BufHead* bh = bget(&dev, BLOCK_NO);
    ASSERT_NE(bh, nullptr);
    bh->data[0] = 0x10;
    bdirty(bh);
    redirty.bh = bh;

    EXPECT_EQ(bwrite(bh), 0);
    EXPECT_EQ(redirty.write_calls, 1u);
    EXPECT_EQ(redirty.last_write[0], 0x10);
    EXPECT_EQ(bh->data[0], redirty.redirty_value);
    EXPECT_TRUE(has_dirty_bdev_range(&dev, BLOCK_NO, 1));
    brelse(bh);

    RecordingIoState io{};
    dev.write_blocks = recording_write;
    dev.private_data = &io;
    EXPECT_EQ(sync_blockdev(&dev), 0);
    EXPECT_EQ(io.write_calls, 1u);
    EXPECT_EQ(io.last_write_block, BLOCK_NO);
    EXPECT_EQ(io.last_write_count, 1u);
    EXPECT_EQ(io.last_write[0], redirty.redirty_value);
    EXPECT_FALSE(has_dirty_bdev_range(&dev, BLOCK_NO, 1));
    dev.private_data = nullptr;
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
        if (bh2->data[i] != 0) {
            is_zero = false;
            break;
        }
    }
    EXPECT_TRUE(is_zero);
    brelse(bh2);
}

TEST_F(BufferCacheTest, InvalidateKeepsReferencedBuffersUntilRelease) {
    constexpr uint64_t BLOCK_NO = 1000;
    BufHead* pinned = bget(&dev, BLOCK_NO);
    ASSERT_NE(pinned, nullptr);

    pinned->data[0] = 0xCC;
    bdirty(pinned);
    BufferCacheStats const BEFORE = buffer_cache_stats();

    invalidate_bdev(&dev);

    BufferCacheStats const PINNED = buffer_cache_stats();
    EXPECT_EQ(pinned->refcount.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(pinned->data[0], 0xCC);
    EXPECT_TRUE(has_dirty_bdev_range(&dev, BLOCK_NO, 1));
    EXPECT_EQ(PINNED.total_buffers, BEFORE.total_buffers);
    EXPECT_EQ(PINNED.dirty_buffers, BEFORE.dirty_buffers);

    brelse(pinned);
    invalidate_bdev(&dev);

    BufferCacheStats const AFTER_RELEASE = buffer_cache_stats();
    EXPECT_FALSE(has_dirty_bdev_range(&dev, BLOCK_NO, 1));
    EXPECT_LT(AFTER_RELEASE.total_buffers, BEFORE.total_buffers);
    EXPECT_LT(AFTER_RELEASE.dirty_buffers, BEFORE.dirty_buffers);
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
// Concurrent cold miss: racing bread() calls for the same block may both issue
// disk reads while the cache lock is dropped, but only one BufHead may survive.
// ---------------------------------------------------------------------------

TEST_F(BufferCacheTest, ConcurrentBreadDeduplicatesColdBlock) {
    BlockingReadState state{};
    dev.read_blocks = blocking_read;
    dev.private_data = &state;

    constexpr size_t THREAD_COUNT = 2;
    constexpr uint64_t BLOCK_NO = 123;
    std::array<BufHead*, THREAD_COUNT> results{};
    std::array<std::thread, THREAD_COUNT> workers{};

    for (size_t i = 0; i < THREAD_COUNT; ++i) {
        workers[i] = std::thread([&, i] { results[i] = bread(&dev, BLOCK_NO); });
    }

    bool observed_race = false;
    {
        std::unique_lock lock(state.mutex);
        observed_race = state.cv.wait_for(lock, std::chrono::seconds(5), [&state] { return state.active_reads == THREAD_COUNT; });
        state.release = true;
    }
    state.cv.notify_all();

    for (auto& worker : workers) {
        worker.join();
    }

    bool const BOTH_RETURNED = results[0] != nullptr && results[1] != nullptr;
    bool const SAME_BUFFER = BOTH_RETURNED && results[0] == results[1];
    int32_t const REFCOUNT = BOTH_RETURNED ? results[0]->refcount.load(std::memory_order_relaxed) : -1;
    bool data_ok = BOTH_RETURNED;
    if (BOTH_RETURNED) {
        for (size_t i = 0; i < results[0]->size; ++i) {
            if (results[0]->data[i] != 0x5A) {
                data_ok = false;
                break;
            }
        }
    }

    if (results[1] != nullptr) {
        brelse(results[1]);
    }
    if (results[0] != nullptr) {
        brelse(results[0]);
    }
    dev.private_data = nullptr;

    EXPECT_TRUE(observed_race);
    EXPECT_EQ(state.read_calls, THREAD_COUNT);
    EXPECT_TRUE(BOTH_RETURNED);
    EXPECT_TRUE(SAME_BUFFER);
    EXPECT_EQ(REFCOUNT, static_cast<int32_t>(THREAD_COUNT));
    EXPECT_TRUE(data_ok);
}

// ---------------------------------------------------------------------------
// Concurrent multi-block cold miss: bread_multi() drops the lock for disk I/O
// too, and must deduplicate by exact byte span when racing on the same range.
// ---------------------------------------------------------------------------

TEST_F(BufferCacheTest, ConcurrentBreadMultiDeduplicatesExactSpan) {
    BlockingReadState state{};
    dev.read_blocks = blocking_read;
    dev.private_data = &state;

    constexpr size_t THREAD_COUNT = 2;
    constexpr uint64_t BLOCK_NO = 321;
    constexpr size_t BLOCK_COUNT = 8;
    std::array<BufHead*, THREAD_COUNT> results{};
    std::array<std::thread, THREAD_COUNT> workers{};

    for (size_t i = 0; i < THREAD_COUNT; ++i) {
        workers[i] = std::thread([&, i] { results[i] = bread_multi(&dev, BLOCK_NO, BLOCK_COUNT); });
    }

    bool observed_race = false;
    {
        std::unique_lock lock(state.mutex);
        observed_race = state.cv.wait_for(lock, std::chrono::seconds(5), [&state] { return state.active_reads == THREAD_COUNT; });
        state.release = true;
    }
    state.cv.notify_all();

    for (auto& worker : workers) {
        worker.join();
    }

    bool const BOTH_RETURNED = results[0] != nullptr && results[1] != nullptr;
    bool const SAME_BUFFER = BOTH_RETURNED && results[0] == results[1];
    size_t const SIZE = BOTH_RETURNED ? results[0]->size : 0;
    int32_t const REFCOUNT = BOTH_RETURNED ? results[0]->refcount.load(std::memory_order_relaxed) : -1;
    bool data_ok = BOTH_RETURNED;
    if (BOTH_RETURNED) {
        for (size_t i = 0; i < results[0]->size; ++i) {
            if (results[0]->data[i] != 0x5A) {
                data_ok = false;
                break;
            }
        }
    }

    if (results[1] != nullptr) {
        brelse(results[1]);
    }
    if (results[0] != nullptr) {
        brelse(results[0]);
    }
    dev.private_data = nullptr;

    EXPECT_TRUE(observed_race);
    EXPECT_EQ(state.read_calls, THREAD_COUNT);
    EXPECT_TRUE(BOTH_RETURNED);
    EXPECT_TRUE(SAME_BUFFER);
    EXPECT_EQ(SIZE, BLOCK_COUNT * dev.block_size);
    EXPECT_EQ(REFCOUNT, static_cast<int32_t>(THREAD_COUNT));
    EXPECT_TRUE(data_ok);
}

TEST_F(BufferCacheTest, SizeMismatchCoexistKeepsNewerMultiBlockDirtyData) {
    constexpr uint64_t BLOCK_NO = 600;
    constexpr size_t BLOCK_COUNT = 8;

    BufHead* single = bget(&dev, BLOCK_NO);
    ASSERT_NE(single, nullptr);
    memset(single->data, 0xBB, single->size);
    bdirty(single);
    brelse(single);

    BufHead* multi = bget_multi(&dev, BLOCK_NO, BLOCK_COUNT);
    ASSERT_NE(multi, nullptr);
    ASSERT_EQ(multi->size, BLOCK_COUNT * dev.block_size);
    for (size_t i = 0; i < multi->size; ++i) {
        multi->data[i] = static_cast<uint8_t>(i & 0xFFU);
    }
    bdirty(multi);
    brelse(multi);

    BufHead* reread = bread_multi(&dev, BLOCK_NO, BLOCK_COUNT);
    ASSERT_NE(reread, nullptr);
    ASSERT_EQ(reread->size, BLOCK_COUNT * dev.block_size);
    for (size_t i = 0; i < reread->size; ++i) {
        ASSERT_EQ(reread->data[i], static_cast<uint8_t>(i & 0xFFU));
    }
    brelse(reread);
}

// ---------------------------------------------------------------------------
// Dirty range sync: an overlapping range must find a dirty multi-block buffer,
// write its full span, clear the dirty state, and flush exactly when needed.
// ---------------------------------------------------------------------------

TEST_F(BufferCacheTest, SyncRangeWritesOverlappingMultiBlockDirtyBuffer) {
    RecordingIoState io{};
    dev.write_blocks = recording_write;
    dev.flush = recording_flush;
    dev.private_data = &io;

    constexpr uint64_t BLOCK_NO = 40;
    constexpr size_t BLOCK_COUNT = 8;
    BufHead* bh = bget_multi(&dev, BLOCK_NO, BLOCK_COUNT);
    ASSERT_NE(bh, nullptr);
    ASSERT_EQ(bh->size, BLOCK_COUNT * dev.block_size);
    for (size_t i = 0; i < bh->size; ++i) {
        bh->data[i] = static_cast<uint8_t>(0xA0U + (i & 0x0FU));
    }
    bdirty(bh);
    brelse(bh);

    EXPECT_TRUE(has_dirty_bdev_range(&dev, BLOCK_NO + 3, 1));
    EXPECT_FALSE(has_dirty_bdev_range(&dev, BLOCK_NO + BLOCK_COUNT, 1));
    EXPECT_EQ(sync_bdev_range(&dev, BLOCK_NO + 3, 1), 0);
    EXPECT_EQ(io.write_calls, 1u);
    EXPECT_EQ(io.flush_calls, 1u);
    EXPECT_EQ(io.last_write_block, BLOCK_NO);
    EXPECT_EQ(io.last_write_count, BLOCK_COUNT);
    EXPECT_FALSE(has_dirty_bdev_range(&dev, BLOCK_NO, BLOCK_COUNT));

    for (size_t i = 0; i < BLOCK_COUNT * dev.block_size; ++i) {
        ASSERT_EQ(io.last_write[i], static_cast<uint8_t>(0xA0U + (i & 0x0FU)));
    }

    EXPECT_EQ(sync_bdev_range(&dev, BLOCK_NO + BLOCK_COUNT, 1), 0);
    EXPECT_EQ(io.write_calls, 1u);
    EXPECT_EQ(io.flush_calls, 1u);
    dev.private_data = nullptr;
}

TEST_F(BufferCacheTest, SyncBlockdevWritesOverlappingDirtyBuffersOldestFirst) {
    RecordingIoState io{};
    dev.write_blocks = recording_write;
    dev.private_data = &io;

    uint64_t block_no = 0;
    bool found_hash_order = false;
    for (uint64_t candidate = 1; candidate < 900; ++candidate) {
        size_t const SINGLE_HASH = test_hash_key(&dev, candidate, dev.block_size);
        size_t const MULTI_HASH = test_hash_key(&dev, candidate, dev.block_size * 2);
        if (SINGLE_HASH < MULTI_HASH) {
            block_no = candidate;
            found_hash_order = true;
            break;
        }
    }
    ASSERT_TRUE(found_hash_order);

    BufHead* older_multi = bget_multi(&dev, block_no, 2);
    ASSERT_NE(older_multi, nullptr);
    memset(older_multi->data, 0xA1, older_multi->size);
    bdirty(older_multi);
    brelse(older_multi);

    BufHead* newer_single = bget(&dev, block_no);
    ASSERT_NE(newer_single, nullptr);
    memset(newer_single->data, 0xB2, newer_single->size);
    bdirty(newer_single);
    brelse(newer_single);

    EXPECT_EQ(sync_blockdev(&dev), 0);
    ASSERT_EQ(io.write_calls, 2u);
    EXPECT_EQ(io.write_blocks[0], block_no);
    EXPECT_EQ(io.write_counts[0], 2u);
    EXPECT_EQ(io.write_first_bytes[0], 0xA1);
    EXPECT_EQ(io.write_blocks[1], block_no);
    EXPECT_EQ(io.write_counts[1], 1u);
    EXPECT_EQ(io.write_first_bytes[1], 0xB2);
    EXPECT_FALSE(has_dirty_bdev_range(&dev, block_no, 2));
    dev.private_data = nullptr;
}

TEST_F(BufferCacheTest, FailedOlderOverlappingWriteBlocksNewerAliasUntilRetry) {
    RecordingIoState io{};
    io.fail_write_calls_remaining = 1;
    dev.write_blocks = recording_write;
    dev.private_data = &io;

    uint64_t block_no = 0;
    bool found_hash_order = false;
    for (uint64_t candidate = 1; candidate < 900; ++candidate) {
        size_t const SINGLE_HASH = test_hash_key(&dev, candidate, dev.block_size);
        size_t const MULTI_HASH = test_hash_key(&dev, candidate, dev.block_size * 2);
        if (SINGLE_HASH < MULTI_HASH) {
            block_no = candidate;
            found_hash_order = true;
            break;
        }
    }
    ASSERT_TRUE(found_hash_order);

    BufHead* older_multi = bget_multi(&dev, block_no, 2);
    ASSERT_NE(older_multi, nullptr);
    memset(older_multi->data, 0xA1, older_multi->size);
    bdirty(older_multi);
    brelse(older_multi);

    BufHead* newer_single = bget(&dev, block_no);
    ASSERT_NE(newer_single, nullptr);
    memset(newer_single->data, 0xB2, newer_single->size);
    bdirty(newer_single);
    brelse(newer_single);

    EXPECT_EQ(sync_blockdev(&dev), -EIO);
    ASSERT_EQ(io.write_calls, 1u);
    EXPECT_EQ(io.write_blocks[0], block_no);
    EXPECT_EQ(io.write_counts[0], 2u);
    EXPECT_EQ(io.write_first_bytes[0], 0xA1);
    EXPECT_TRUE(has_dirty_bdev_range(&dev, block_no, 2));

    io.write_calls = 0;
    EXPECT_EQ(sync_blockdev(&dev), 0);
    ASSERT_EQ(io.write_calls, 2u);
    EXPECT_EQ(io.write_blocks[0], block_no);
    EXPECT_EQ(io.write_counts[0], 2u);
    EXPECT_EQ(io.write_first_bytes[0], 0xA1);
    EXPECT_EQ(io.write_blocks[1], block_no);
    EXPECT_EQ(io.write_counts[1], 1u);
    EXPECT_EQ(io.write_first_bytes[1], 0xB2);
    EXPECT_FALSE(has_dirty_bdev_range(&dev, block_no, 2));
    dev.private_data = nullptr;
}

// ---------------------------------------------------------------------------
// Dirty range discard: overlapping unreferenced buffers are dropped without
// writeback, but referenced buffers must survive and remain dirty.
// ---------------------------------------------------------------------------

TEST_F(BufferCacheTest, DiscardRangeDropsOnlyUnreferencedOverlappingBuffers) {
    RecordingIoState io{};
    dev.write_blocks = recording_write;
    dev.flush = recording_flush;
    dev.private_data = &io;

    constexpr uint64_t PINNED_BLOCK = 90;
    BufHead* pinned = bget_multi(&dev, PINNED_BLOCK, 8);
    ASSERT_NE(pinned, nullptr);
    memset(pinned->data, 0x11, pinned->size);
    bdirty(pinned);

    discard_bdev_range(&dev, PINNED_BLOCK + 2, 1);
    EXPECT_TRUE(has_dirty_bdev_range(&dev, PINNED_BLOCK, 8));
    EXPECT_EQ(io.write_calls, 0u);
    EXPECT_EQ(io.flush_calls, 0u);
    EXPECT_EQ(pinned->refcount.load(std::memory_order_relaxed), 1);
    brelse(pinned);

    constexpr uint64_t DROPPED_BLOCK = 120;
    BufHead* dropped = bget_multi(&dev, DROPPED_BLOCK, 8);
    ASSERT_NE(dropped, nullptr);
    memset(dropped->data, 0x22, dropped->size);
    bdirty(dropped);
    brelse(dropped);

    EXPECT_TRUE(has_dirty_bdev_range(&dev, DROPPED_BLOCK + 7, 1));
    BufferCacheStats const BEFORE = buffer_cache_stats();
    discard_bdev_range(&dev, DROPPED_BLOCK + 3, 1);
    BufferCacheStats const AFTER = buffer_cache_stats();
    EXPECT_FALSE(has_dirty_bdev_range(&dev, DROPPED_BLOCK, 8));
    EXPECT_EQ(io.write_calls, 0u);
    EXPECT_EQ(io.flush_calls, 0u);
    EXPECT_LT(AFTER.total_buffers, BEFORE.total_buffers);
    EXPECT_LT(AFTER.dirty_buffers, BEFORE.dirty_buffers);

    BufHead* reread = bread_multi(&dev, DROPPED_BLOCK, 8);
    ASSERT_NE(reread, nullptr);
    bool zeroed = true;
    for (size_t i = 0; i < reread->size; ++i) {
        if (reread->data[i] != 0) {
            zeroed = false;
            break;
        }
    }
    EXPECT_TRUE(zeroed);
    brelse(reread);
    dev.private_data = nullptr;
}

TEST_F(BufferCacheTest, RangeOperationsHandleLastBlockWithoutWrapping) {
    RecordingIoState io{};
    dev.write_blocks = recording_write;
    dev.private_data = &io;

    constexpr uint64_t LAST_BLOCK = std::numeric_limits<uint64_t>::max();
    BufHead* bh = bget(&dev, LAST_BLOCK);
    ASSERT_NE(bh, nullptr);
    bh->data[0] = 0xE7;
    bdirty(bh);
    brelse(bh);

    EXPECT_TRUE(has_dirty_bdev_range(&dev, LAST_BLOCK, 1));
    EXPECT_FALSE(has_dirty_bdev_range(&dev, 0, 1));
    EXPECT_EQ(sync_bdev_range(&dev, LAST_BLOCK, 1), 0);
    ASSERT_EQ(io.write_calls, 1u);
    EXPECT_EQ(io.last_write_block, LAST_BLOCK);
    EXPECT_EQ(io.last_write_count, 1u);
    EXPECT_EQ(io.last_write[0], 0xE7);
    EXPECT_FALSE(has_dirty_bdev_range(&dev, LAST_BLOCK, 1));
    dev.private_data = nullptr;
}

// ---------------------------------------------------------------------------
// Failed writes must not cause sync_blockdev() to recollect the same full batch
// forever. Dirty buffers stay dirty, but the call must make bounded progress.
// ---------------------------------------------------------------------------

TEST_F(BufferCacheTest, SyncBlockdevFailedWritesProgressAcrossBatches) {
    RecordingIoState io{};
    io.fail_writes = true;
    dev.write_blocks = recording_write;
    dev.private_data = &io;

    constexpr uint64_t FIRST_BLOCK = 500;
    constexpr size_t DIRTY_COUNT = 70;
    constexpr size_t DIRTY_STRIDE = 2;
    constexpr size_t DIRTY_SPAN = (DIRTY_COUNT - 1) * DIRTY_STRIDE + 1;
    for (size_t i = 0; i < DIRTY_COUNT; ++i) {
        BufHead* bh = bget(&dev, FIRST_BLOCK + static_cast<uint64_t>(i * DIRTY_STRIDE));
        ASSERT_NE(bh, nullptr);
        bh->data[0] = static_cast<uint8_t>(i);
        bdirty(bh);
        brelse(bh);
    }

    EXPECT_EQ(sync_blockdev(&dev), -EIO);
    EXPECT_EQ(io.write_calls, DIRTY_COUNT);
    EXPECT_TRUE(has_dirty_bdev_range(&dev, FIRST_BLOCK, DIRTY_SPAN));

    io.fail_writes = false;
    io.write_calls = 0;
    EXPECT_EQ(sync_blockdev(&dev), 0);
    EXPECT_EQ(io.write_calls, DIRTY_COUNT);
    EXPECT_FALSE(has_dirty_bdev_range(&dev, FIRST_BLOCK, DIRTY_SPAN));
    dev.private_data = nullptr;
}

TEST_F(BufferCacheTest, ConcurrentSyncBlockdevWaitsForInFlightWriteback) {
    BlockingWriteState state{};
    dev.write_blocks = blocking_write;
    dev.private_data = &state;

    constexpr uint64_t BLOCK_NO = 650;
    BufHead* bh = bget(&dev, BLOCK_NO);
    ASSERT_NE(bh, nullptr);
    bh->data[0] = 0x5C;
    bdirty(bh);
    brelse(bh);

    std::atomic<int> first_result{-999};
    std::atomic<int> second_result{-999};
    std::atomic<bool> second_done{false};

    std::thread first_sync([&] { first_result.store(sync_blockdev(&dev), std::memory_order_relaxed); });

    bool observed_first_write = false;
    {
        std::unique_lock lock(state.mutex);
        observed_first_write = state.cv.wait_for(lock, std::chrono::seconds(5), [&state] { return state.active_writes == 1; });
    }
    EXPECT_TRUE(observed_first_write);
    if (!observed_first_write) {
        {
            std::lock_guard lock(state.mutex);
            state.release = true;
        }
        state.cv.notify_all();
        first_sync.join();
        dev.private_data = nullptr;
        return;
    }

    std::thread second_sync([&] {
        second_result.store(sync_blockdev(&dev), std::memory_order_relaxed);
        second_done.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(25));
    EXPECT_FALSE(second_done.load(std::memory_order_acquire));

    {
        std::lock_guard lock(state.mutex);
        state.release = true;
    }
    state.cv.notify_all();

    first_sync.join();
    second_sync.join();

    EXPECT_EQ(first_result.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(second_result.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(state.write_calls, 1u);
    EXPECT_FALSE(has_dirty_bdev_range(&dev, BLOCK_NO, 1));
    dev.private_data = nullptr;
}

TEST_F(BufferCacheTest, SyncRangeFlushesAfterWaitingForForeignWriteback) {
    BlockingWriteState state{};
    dev.write_blocks = blocking_write;
    dev.flush = blocking_flush;
    dev.private_data = &state;

    constexpr uint64_t BLOCK_NO = 655;
    BufHead* bh = bget(&dev, BLOCK_NO);
    ASSERT_NE(bh, nullptr);
    bh->data[0] = 0x6D;
    bdirty(bh);
    brelse(bh);

    std::atomic<int> blockdev_result{-999};
    std::atomic<int> range_result{-999};
    std::atomic<bool> range_done{false};

    std::thread blockdev_sync([&] { blockdev_result.store(sync_blockdev(&dev), std::memory_order_relaxed); });

    bool observed_write = false;
    {
        std::unique_lock lock(state.mutex);
        observed_write = state.cv.wait_for(lock, std::chrono::seconds(5), [&state] { return state.active_writes == 1; });
    }
    EXPECT_TRUE(observed_write);
    if (!observed_write) {
        {
            std::lock_guard lock(state.mutex);
            state.release = true;
        }
        state.cv.notify_all();
        blockdev_sync.join();
        dev.private_data = nullptr;
        return;
    }

    std::thread range_sync([&] {
        range_result.store(sync_bdev_range(&dev, BLOCK_NO, 1), std::memory_order_relaxed);
        range_done.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(25));
    EXPECT_FALSE(range_done.load(std::memory_order_acquire));

    {
        std::lock_guard lock(state.mutex);
        state.release = true;
    }
    state.cv.notify_all();

    blockdev_sync.join();
    range_sync.join();

    EXPECT_EQ(blockdev_result.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(range_result.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(state.write_calls, 1u);
    EXPECT_GE(state.flush_calls, 2u);
    EXPECT_FALSE(has_dirty_bdev_range(&dev, BLOCK_NO, 1));
    dev.private_data = nullptr;
}

TEST_F(BufferCacheTest, RedirtyDuringInFlightWritebackRemainsDirty) {
    BlockingWriteState state{};
    dev.write_blocks = blocking_write;
    dev.private_data = &state;

    constexpr uint64_t BLOCK_NO = 660;
    BufHead* bh = bget(&dev, BLOCK_NO);
    ASSERT_NE(bh, nullptr);
    bh->data[0] = 0x10;
    bdirty(bh);

    std::atomic<int> sync_result{-999};
    std::thread sync_thread([&] { sync_result.store(sync_blockdev(&dev), std::memory_order_relaxed); });

    bool observed_write = false;
    {
        std::unique_lock lock(state.mutex);
        observed_write = state.cv.wait_for(lock, std::chrono::seconds(5), [&state] { return state.active_writes == 1; });
    }
    EXPECT_TRUE(observed_write);
    if (!observed_write) {
        {
            std::lock_guard lock(state.mutex);
            state.release = true;
        }
        state.cv.notify_all();
        sync_thread.join();
        brelse(bh);
        dev.private_data = nullptr;
        return;
    }

    bh->data[0] = 0x20;
    bdirty(bh);
    EXPECT_TRUE(has_dirty_bdev_range(&dev, BLOCK_NO, 1));

    {
        std::lock_guard lock(state.mutex);
        state.release = true;
    }
    state.cv.notify_all();
    sync_thread.join();

    EXPECT_EQ(sync_result.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(state.write_calls, 1u);
    EXPECT_TRUE(has_dirty_bdev_range(&dev, BLOCK_NO, 1));
    EXPECT_EQ(bh->data[0], 0x20);
    brelse(bh);

    RecordingIoState io{};
    dev.write_blocks = recording_write;
    dev.private_data = &io;
    EXPECT_EQ(sync_blockdev(&dev), 0);
    EXPECT_EQ(io.write_calls, 1u);
    EXPECT_EQ(io.last_write_block, BLOCK_NO);
    EXPECT_EQ(io.last_write_count, 1u);
    EXPECT_EQ(io.last_write[0], 0x20);
    EXPECT_FALSE(has_dirty_bdev_range(&dev, BLOCK_NO, 1));
    dev.private_data = nullptr;
}

// ---------------------------------------------------------------------------
// The range-limited sync path uses the same fixed-size batching. Persistent
// write failures must return an error without livelocking on the first batch.
// ---------------------------------------------------------------------------

TEST_F(BufferCacheTest, SyncRangeFailedWritesProgressAcrossBatches) {
    RecordingIoState io{};
    io.fail_writes = true;
    dev.write_blocks = recording_write;
    dev.private_data = &io;

    constexpr uint64_t FIRST_BLOCK = 700;
    constexpr size_t DIRTY_COUNT = 70;
    constexpr size_t DIRTY_STRIDE = 2;
    constexpr size_t DIRTY_SPAN = (DIRTY_COUNT - 1) * DIRTY_STRIDE + 1;
    for (size_t i = 0; i < DIRTY_COUNT; ++i) {
        BufHead* bh = bget(&dev, FIRST_BLOCK + static_cast<uint64_t>(i * DIRTY_STRIDE));
        ASSERT_NE(bh, nullptr);
        bh->data[0] = static_cast<uint8_t>(0x80U + i);
        bdirty(bh);
        brelse(bh);
    }

    EXPECT_EQ(sync_bdev_range(&dev, FIRST_BLOCK, DIRTY_SPAN), -EIO);
    EXPECT_EQ(io.write_calls, DIRTY_COUNT);
    EXPECT_TRUE(has_dirty_bdev_range(&dev, FIRST_BLOCK, DIRTY_SPAN));

    io.fail_writes = false;
    io.write_calls = 0;
    EXPECT_EQ(sync_bdev_range(&dev, FIRST_BLOCK, DIRTY_SPAN), 0);
    EXPECT_EQ(io.write_calls, DIRTY_COUNT);
    EXPECT_FALSE(has_dirty_bdev_range(&dev, FIRST_BLOCK, DIRTY_SPAN));
    dev.private_data = nullptr;
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
    EXPECT_GT(after.hits, before.hits);
}
