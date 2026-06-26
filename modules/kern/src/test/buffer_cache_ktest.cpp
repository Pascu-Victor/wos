#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dev/block_device.hpp>
#include <test/ktest.hpp>
#include <vfs/buffer_cache.hpp>

// Fake block device: reads fill zeros, writes are no-ops.
// total_blocks must cover the highest block number used in any test.

namespace {

auto null_read(ker::dev::BlockDevice* dev, uint64_t /*block*/, size_t count, void* buffer) -> int {
    memset(buffer, 0, count * dev->block_size);
    return 0;
}

auto null_write(ker::dev::BlockDevice* /*dev*/, uint64_t /*block*/, size_t /*count*/, const void* /*buffer*/) -> int { return 0; }

struct RecordingWriteState {
    size_t write_calls = 0;
    uint64_t last_write_block = 0;
    size_t last_write_count = 0;
    bool fail_writes = false;
    size_t fail_write_calls_remaining = 0;
    uint8_t last_write[4096]{};
    uint64_t write_blocks[128]{};
    size_t write_counts[128]{};
    uint8_t write_first_bytes[128]{};
};

struct RecordingReadState {
    size_t read_calls = 0;
};

struct LargeCountingWriteState {
    size_t write_calls = 0;
    uint64_t write_blocks[8]{};
    size_t write_counts[8]{};
};

auto recording_read(ker::dev::BlockDevice* dev, uint64_t /*block*/, size_t count, void* buffer) -> int {
    auto* state = static_cast<RecordingReadState*>(dev->private_data);
    if (state != nullptr) {
        state->read_calls++;
    }
    memset(buffer, 0, count * dev->block_size);
    return 0;
}

struct RedirtyingWriteState {
    ker::vfs::BufHead* bh = nullptr;
    size_t write_calls = 0;
    uint8_t redirty_value = 0;
    uint8_t last_write[4096]{};
};

auto recording_write(ker::dev::BlockDevice* dev, uint64_t block, size_t count, const void* buffer) -> int {
    auto* state = static_cast<RecordingWriteState*>(dev->private_data);
    if (state == nullptr) {
        return 0;
    }
    size_t const BYTE_COUNT = count * dev->block_size;
    if (BYTE_COUNT > sizeof(state->last_write)) {
        return -1;
    }
    size_t const CALL_INDEX = state->write_calls++;
    state->last_write_block = block;
    state->last_write_count = count;
    if (CALL_INDEX < 128) {
        state->write_blocks[CALL_INDEX] = block;
        state->write_counts[CALL_INDEX] = count;
        state->write_first_bytes[CALL_INDEX] = static_cast<const uint8_t*>(buffer)[0];
    }
    bool const FAIL_THIS_WRITE = state->fail_writes || state->fail_write_calls_remaining != 0;
    if (state->fail_write_calls_remaining != 0) {
        state->fail_write_calls_remaining--;
    }
    if (FAIL_THIS_WRITE) {
        return -1;
    }
    memcpy(state->last_write, buffer, std::min(BYTE_COUNT, sizeof(state->last_write)));
    return 0;
}

auto large_counting_write(ker::dev::BlockDevice* dev, uint64_t block, size_t count, const void* /*buffer*/) -> int {
    auto* state = static_cast<LargeCountingWriteState*>(dev->private_data);
    if (state == nullptr) {
        return 0;
    }
    size_t const CALL_INDEX = state->write_calls++;
    if (CALL_INDEX < 8) {
        state->write_blocks[CALL_INDEX] = block;
        state->write_counts[CALL_INDEX] = count;
    }
    return 0;
}

auto redirtying_write(ker::dev::BlockDevice* dev, uint64_t /*block*/, size_t count, const void* buffer) -> int {
    auto* state = static_cast<RedirtyingWriteState*>(dev->private_data);
    if (state == nullptr || state->bh == nullptr) {
        return 0;
    }
    size_t const BYTE_COUNT = count * dev->block_size;
    if (BYTE_COUNT > sizeof(state->last_write)) {
        return -1;
    }
    state->write_calls++;
    state->bh->data[0] = state->redirty_value;
    ker::vfs::bdirty(state->bh);
    memcpy(state->last_write, buffer, BYTE_COUNT);
    return 0;
}

auto make_null_bdev() -> ker::dev::BlockDevice {
    ker::dev::BlockDevice d{};
    d.block_size = 512;
    d.total_blocks = 2048;
    d.read_blocks = null_read;
    d.write_blocks = null_write;
    return d;
}

auto test_hash_key(const ker::dev::BlockDevice* bdev, uint64_t block_no, size_t size) -> size_t {
    auto h = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(bdev));
    h ^= block_no;
    h *= 0x517cc1b727220a95ULL;
    h ^= static_cast<uint64_t>(size);
    h *= 0x517cc1b727220a95ULL;
    h ^= (h >> 32);
    return static_cast<size_t>(h % ker::vfs::BUFFER_CACHE_HASH_BUCKETS);
}

}  // namespace

KTEST(BufferCache, SizingKeepsDirtyLimitsBelowCleanCache) {
    constexpr uint64_t ONE_GIB = uint64_t{1024} * 1024 * 1024;

    size_t const CACHE_MAX = ker::vfs::buffer_cache_selftest_choose_cache_max_bytes(ONE_GIB);
    size_t const DIRTY_TARGET = ker::vfs::buffer_cache_selftest_choose_dirty_target_bytes(ONE_GIB, CACHE_MAX);
    size_t const DIRTY_HARD = ker::vfs::buffer_cache_selftest_choose_dirty_hard_bytes(DIRTY_TARGET, CACHE_MAX);

    KEXPECT_EQ(CACHE_MAX, static_cast<size_t>(ONE_GIB / 2));
    KEXPECT_EQ(DIRTY_TARGET, static_cast<size_t>(ONE_GIB / 10));
    KEXPECT_EQ(DIRTY_HARD, CACHE_MAX);

    size_t const FALLBACK_MAX = ker::vfs::buffer_cache_selftest_choose_cache_max_bytes(0);
    size_t const FALLBACK_TARGET = ker::vfs::buffer_cache_selftest_choose_dirty_target_bytes(0, FALLBACK_MAX);
    size_t const FALLBACK_HARD = ker::vfs::buffer_cache_selftest_choose_dirty_hard_bytes(FALLBACK_TARGET, FALLBACK_MAX);
    KEXPECT_EQ(FALLBACK_MAX, ker::vfs::BUFFER_CACHE_DEFAULT_SIZE);
    KEXPECT_EQ(FALLBACK_TARGET, ker::vfs::BUFFER_CACHE_DEFAULT_SIZE / 2);
    KEXPECT_EQ(FALLBACK_HARD, ker::vfs::BUFFER_CACHE_DEFAULT_SIZE);
}

KTEST(BufferCache, BreadHit) {
    ker::dev::BlockDevice dev = make_null_bdev();
    ker::vfs::invalidate_bdev(&dev);

    ker::vfs::BufHead* bh1 = ker::vfs::bread(&dev, 1);
    KREQUIRE_NE(bh1, nullptr);
    ker::vfs::BufHead* bh2 = ker::vfs::bread(&dev, 1);
    KREQUIRE_NE(bh2, nullptr);
    KEXPECT_EQ(bh1, bh2);  // cache hit: same pointer

    ker::vfs::brelse(bh2);
    ker::vfs::brelse(bh1);
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, BreadBrelse) {
    ker::dev::BlockDevice dev = make_null_bdev();
    ker::vfs::invalidate_bdev(&dev);

    ker::vfs::BufHead* bh = ker::vfs::bread(&dev, 2);
    KREQUIRE_NE(bh, nullptr);
    ker::vfs::brelse(bh);
    // No crash = pass
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, WriteReadback) {
    ker::dev::BlockDevice dev = make_null_bdev();
    ker::vfs::invalidate_bdev(&dev);

    ker::vfs::BufHead* bh = ker::vfs::bread(&dev, 3);
    KREQUIRE_NE(bh, nullptr);

    // Write a recognizable pattern into the cached buffer
    for (size_t i = 0; i < bh->size; ++i) {
        bh->data[i] = static_cast<uint8_t>(i & 0xFF);
    }
    KEXPECT_EQ(ker::vfs::bwrite(bh), 0);
    ker::vfs::brelse(bh);

    // Second bread: should be a cache hit — same buffer, same data
    ker::vfs::BufHead* bh2 = ker::vfs::bread(&dev, 3);
    KREQUIRE_NE(bh2, nullptr);
    bool pattern_ok = true;
    for (size_t i = 0; i < bh2->size; ++i) {
        if (bh2->data[i] != static_cast<uint8_t>(i & 0xFF)) {
            pattern_ok = false;
            break;
        }
    }
    KEXPECT_TRUE(pattern_ok);
    ker::vfs::brelse(bh2);
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, FailedBwriteMarksCleanBufferDirtyForRetry) {
    ker::dev::BlockDevice dev = make_null_bdev();
    RecordingWriteState io{};
    io.fail_writes = true;
    dev.write_blocks = recording_write;
    dev.private_data = &io;
    ker::vfs::invalidate_bdev(&dev);

    const uint64_t BLOCK_NO = 30;
    ker::vfs::BufHead* bh = ker::vfs::bget(&dev, BLOCK_NO);
    KREQUIRE_NE(bh, nullptr);
    bh->data[0] = 0x7E;

    KEXPECT_FALSE(ker::vfs::has_dirty_bdev_range(&dev, BLOCK_NO, 1));
    KEXPECT_EQ(ker::vfs::bwrite(bh), -1);
    KEXPECT_EQ(io.write_calls, static_cast<size_t>(1));
    KEXPECT_TRUE(ker::vfs::has_dirty_bdev_range(&dev, BLOCK_NO, 1));
    ker::vfs::brelse(bh);

    io.fail_writes = false;
    io.write_calls = 0;
    KEXPECT_EQ(ker::vfs::sync_blockdev(&dev), 0);
    KEXPECT_EQ(io.write_calls, static_cast<size_t>(1));
    KEXPECT_FALSE(ker::vfs::has_dirty_bdev_range(&dev, BLOCK_NO, 1));
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, BwriteDoesNotClearForeignWritebackMarker) {
    ker::dev::BlockDevice dev = make_null_bdev();
    RecordingWriteState io{};
    dev.write_blocks = recording_write;
    dev.private_data = &io;
    ker::vfs::invalidate_bdev(&dev);

    const uint64_t BLOCK_NO = 31;
    const uint64_t FOREIGN_EPOCH = 0xBEEFULL;
    ker::vfs::BufHead* bh = ker::vfs::bget(&dev, BLOCK_NO);
    KREQUIRE_NE(bh, nullptr);
    bh->data[0] = 0x44;
    ker::vfs::bdirty(bh);

    bh->flags |= ker::vfs::BH_WRITEBACK;
    bh->writeback_epoch = FOREIGN_EPOCH;
    bh->writeback_dirty_epoch = bh->dirty_epoch;

    KEXPECT_EQ(ker::vfs::bwrite(bh), 0);
    KEXPECT_EQ(io.write_calls, static_cast<size_t>(1));
    KEXPECT_FALSE(ker::vfs::has_dirty_bdev_range(&dev, BLOCK_NO, 1));
    KEXPECT_NE(bh->flags & ker::vfs::BH_WRITEBACK, static_cast<uint32_t>(0));
    KEXPECT_EQ(bh->writeback_epoch, FOREIGN_EPOCH);

    bh->flags &= ~ker::vfs::BH_WRITEBACK;
    bh->writeback_epoch = 0;
    bh->writeback_dirty_epoch = 0;
    ker::vfs::brelse(bh);
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, BwriteUsesStableSnapshotWhenRedirtiedDuringDeviceWrite) {
    ker::dev::BlockDevice dev = make_null_bdev();
    RedirtyingWriteState redirty{};
    redirty.redirty_value = 0x20;
    dev.write_blocks = redirtying_write;
    dev.private_data = &redirty;
    ker::vfs::invalidate_bdev(&dev);

    const uint64_t BLOCK_NO = 32;
    ker::vfs::BufHead* bh = ker::vfs::bget(&dev, BLOCK_NO);
    KREQUIRE_NE(bh, nullptr);
    bh->data[0] = 0x10;
    ker::vfs::bdirty(bh);
    redirty.bh = bh;

    KEXPECT_EQ(ker::vfs::bwrite(bh), 0);
    KEXPECT_EQ(redirty.write_calls, static_cast<size_t>(1));
    KEXPECT_EQ(redirty.last_write[0], static_cast<uint8_t>(0x10));
    KEXPECT_EQ(bh->data[0], redirty.redirty_value);
    KEXPECT_TRUE(ker::vfs::has_dirty_bdev_range(&dev, BLOCK_NO, 1));
    ker::vfs::brelse(bh);

    RecordingWriteState io{};
    dev.write_blocks = recording_write;
    dev.private_data = &io;
    KEXPECT_EQ(ker::vfs::sync_blockdev(&dev), 0);
    KEXPECT_EQ(io.write_calls, static_cast<size_t>(1));
    KEXPECT_EQ(io.last_write_block, BLOCK_NO);
    KEXPECT_EQ(io.last_write_count, static_cast<size_t>(1));
    KEXPECT_EQ(io.last_write[0], redirty.redirty_value);
    KEXPECT_FALSE(ker::vfs::has_dirty_bdev_range(&dev, BLOCK_NO, 1));
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, Invalidate) {
    ker::dev::BlockDevice dev = make_null_bdev();
    ker::vfs::invalidate_bdev(&dev);

    ker::vfs::BufHead* bh = ker::vfs::bread(&dev, 10);
    KREQUIRE_NE(bh, nullptr);
    // Write pattern
    for (size_t i = 0; i < bh->size; ++i) {
        bh->data[i] = 0xAB;
    }
    ker::vfs::brelse(bh);

    // Invalidate evicts all cached buffers for this device
    ker::vfs::invalidate_bdev(&dev);

    // Re-read: cache miss, null_read fills zeros
    ker::vfs::BufHead* bh2 = ker::vfs::bread(&dev, 10);
    KREQUIRE_NE(bh2, nullptr);
    bool is_zero = true;
    for (size_t i = 0; i < bh2->size; ++i) {
        if (bh2->data[i] != 0) {
            is_zero = false;
            break;
        }
    }
    KEXPECT_TRUE(is_zero);
    ker::vfs::brelse(bh2);
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, InvalidateKeepsReferencedBuffersUntilRelease) {
    ker::dev::BlockDevice dev = make_null_bdev();
    ker::vfs::invalidate_bdev(&dev);

    const uint64_t BLOCK_NO = 1000;
    ker::vfs::BufHead* pinned = ker::vfs::bget(&dev, BLOCK_NO);
    KREQUIRE_NE(pinned, nullptr);

    pinned->data[0] = 0xCC;
    ker::vfs::bdirty(pinned);
    ker::vfs::BufferCacheStats const BEFORE = ker::vfs::buffer_cache_stats();

    ker::vfs::invalidate_bdev(&dev);

    ker::vfs::BufferCacheStats const PINNED = ker::vfs::buffer_cache_stats();
    KEXPECT_EQ(pinned->refcount.load(std::memory_order_relaxed), 1);
    KEXPECT_EQ(pinned->data[0], 0xCC);
    KEXPECT_TRUE(ker::vfs::has_dirty_bdev_range(&dev, BLOCK_NO, 1));
    KEXPECT_EQ(PINNED.total_buffers, BEFORE.total_buffers);
    KEXPECT_EQ(PINNED.dirty_buffers, BEFORE.dirty_buffers);

    ker::vfs::brelse(pinned);
    ker::vfs::invalidate_bdev(&dev);

    ker::vfs::BufferCacheStats const AFTER_RELEASE = ker::vfs::buffer_cache_stats();
    KEXPECT_FALSE(ker::vfs::has_dirty_bdev_range(&dev, BLOCK_NO, 1));
    KEXPECT_TRUE(AFTER_RELEASE.total_buffers < BEFORE.total_buffers);
    KEXPECT_TRUE(AFTER_RELEASE.dirty_buffers < BEFORE.dirty_buffers);
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, LRUEviction) {
    ker::dev::BlockDevice dev = make_null_bdev();
    ker::vfs::invalidate_bdev(&dev);

    // Allocate 200 distinct blocks sequentially; brelse each immediately.
    // Exercises the LRU path without requiring the full 64 MB cache to be filled.
    for (uint64_t blk = 0; blk < 200; ++blk) {
        ker::vfs::BufHead* bh = ker::vfs::bread(&dev, blk);
        KEXPECT_NE(bh, nullptr);
        if (bh != nullptr) {
            ker::vfs::brelse(bh);
        }
    }

    // Re-read the first few blocks — must succeed regardless of eviction
    for (uint64_t blk = 0; blk < 5; ++blk) {
        ker::vfs::BufHead* bh = ker::vfs::bread(&dev, blk);
        KEXPECT_NE(bh, nullptr);
        if (bh != nullptr) {
            ker::vfs::brelse(bh);
        }
    }

    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, HitMarksReferencedBufferForSecondChance) {
    ker::dev::BlockDevice dev = make_null_bdev();
    ker::vfs::invalidate_bdev(&dev);

    ker::vfs::BufHead* referenced = ker::vfs::bget(&dev, 1200);
    KREQUIRE_NE(referenced, nullptr);
    ker::vfs::brelse(referenced);

    ker::vfs::BufHead* newer = ker::vfs::bget(&dev, 1201);
    KREQUIRE_NE(newer, nullptr);
    ker::vfs::brelse(newer);

    KEXPECT_EQ(referenced->flags & ker::vfs::BH_LRU_REFERENCED, static_cast<uint32_t>(0));
    ker::vfs::BufHead* hit = ker::vfs::bread(&dev, 1200);
    KREQUIRE_NE(hit, nullptr);
    KEXPECT_EQ(hit, referenced);
    KEXPECT_NE(hit->flags & ker::vfs::BH_LRU_REFERENCED, static_cast<uint32_t>(0));
    ker::vfs::brelse(hit);

    ker::vfs::invalidate_bdev(&dev);
}

// ---------------------------------------------------------------------------
// Multi-block (bget_multi / bread_multi) tests
// These mirror the XFS write → mmap-read path:
//   write: xfs_buf_get → bget_multi → memcpy → bdirty → brelse
//   read:  bread_multi (aligned path in xfs_vfs_read)
// ---------------------------------------------------------------------------

KTEST(BufferCache, BgetMultiReadback) {
    // Key regression: dirty multi-block buffer must be visible to bread_multi
    // without going to disk.  A null_read device returns zeros, so if
    // bread_multi reads from disk the data-pattern check below will fail.
    ker::dev::BlockDevice dev = make_null_bdev();
    ker::vfs::invalidate_bdev(&dev);

    const uint64_t BLK = 400;
    const size_t RATIO = 8;  // 4096-byte XFS block / 512-byte sector

    ker::vfs::BufHead* wp = ker::vfs::bget_multi(&dev, BLK, RATIO);
    KREQUIRE_NE(wp, nullptr);
    KEXPECT_EQ(wp->size, static_cast<size_t>(RATIO * 512));
    for (size_t i = 0; i < wp->size; i++) {
        wp->data[i] = static_cast<uint8_t>(i & 0xFF);
    }
    ker::vfs::bdirty(wp);
    ker::vfs::brelse(wp);

    ker::vfs::BufHead* rp = ker::vfs::bread_multi(&dev, BLK, RATIO);
    KREQUIRE_NE(rp, nullptr);
    KEXPECT_EQ(rp->size, static_cast<size_t>(RATIO * 512));
    bool ok = true;
    for (size_t i = 0; i < rp->size; i++) {
        if (rp->data[i] != static_cast<uint8_t>(i & 0xFF)) {
            ok = false;
            break;
        }
    }
    KEXPECT_TRUE(ok);
    ker::vfs::brelse(rp);
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, BdirtyVisibleToBread) {
    // Dirty single-sector buffer must be returned by bread (data, not zeros).
    ker::dev::BlockDevice dev = make_null_bdev();
    ker::vfs::invalidate_bdev(&dev);

    ker::vfs::BufHead* wp = ker::vfs::bread(&dev, 500);
    KREQUIRE_NE(wp, nullptr);
    memset(wp->data, 0xAA, wp->size);
    ker::vfs::bdirty(wp);
    ker::vfs::brelse(wp);

    ker::vfs::BufHead* rp = ker::vfs::bread(&dev, 500);
    KREQUIRE_NE(rp, nullptr);
    bool ok = true;
    for (size_t i = 0; i < rp->size; i++) {
        if (rp->data[i] != 0xAA) {
            ok = false;
            break;
        }
    }
    KEXPECT_TRUE(ok);
    ker::vfs::brelse(rp);
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, SizeMismatchCoexist) {
    // If a single-sector entry already exists at BLK, a subsequent
    // bget_multi(BLK, 8) must create a separate multi-sector entry.
    // bread_multi(BLK, 8) must then find and return the multi-sector buffer
    // with the data written to it — NOT the old single-sector buffer or zeros.
    ker::dev::BlockDevice dev = make_null_bdev();
    ker::vfs::invalidate_bdev(&dev);

    const uint64_t BLK = 600;

    // Plant a single-sector dirty buffer (simulates prior metadata read)
    ker::vfs::BufHead* sb = ker::vfs::bget(&dev, BLK);
    KREQUIRE_NE(sb, nullptr);
    memset(sb->data, 0xBB, sb->size);
    ker::vfs::bdirty(sb);
    ker::vfs::brelse(sb);

    // Multi-sector write at the same block (simulates XFS data block write)
    ker::vfs::BufHead* mb = ker::vfs::bget_multi(&dev, BLK, 8);
    KREQUIRE_NE(mb, nullptr);
    KEXPECT_EQ(mb->size, static_cast<size_t>(8 * 512));
    for (size_t i = 0; i < mb->size; i++) {
        mb->data[i] = static_cast<uint8_t>(i & 0xFF);
    }
    ker::vfs::bdirty(mb);
    ker::vfs::brelse(mb);

    // bread_multi(8) must return the 8-sector buffer with the written pattern
    ker::vfs::BufHead* rp = ker::vfs::bread_multi(&dev, BLK, 8);
    KREQUIRE_NE(rp, nullptr);
    KEXPECT_EQ(rp->size, static_cast<size_t>(8 * 512));
    bool ok = true;
    for (size_t i = 0; i < rp->size; i++) {
        if (rp->data[i] != static_cast<uint8_t>(i & 0xFF)) {
            ok = false;
            break;
        }
    }
    KEXPECT_TRUE(ok);
    ker::vfs::brelse(rp);
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, DirtyRangeIndexFindsAndOverlaysDirtySpan) {
    ker::dev::BlockDevice dev = make_null_bdev();
    ker::vfs::invalidate_bdev(&dev);

    const uint64_t BLK = 1000;
    ker::vfs::BufHead* mb = ker::vfs::bget_multi(&dev, BLK, 8);
    KREQUIRE_NE(mb, nullptr);
    memset(mb->data, 0xCC, mb->size);
    ker::vfs::bdirty(mb);
    ker::vfs::brelse(mb);

    KEXPECT_TRUE(ker::vfs::has_dirty_bdev_range(&dev, BLK + 4, 1));
    KEXPECT_TRUE(ker::vfs::has_dirty_bdev_range(&dev, BLK, 16));
    KEXPECT_FALSE(ker::vfs::has_dirty_bdev_range(&dev, BLK + 8, 8));
    KEXPECT_TRUE(ker::vfs::has_cached_bdev_range(&dev, BLK + 4, 1));
    KEXPECT_TRUE(ker::vfs::has_cached_bdev_range(&dev, BLK, 8));
    KEXPECT_FALSE(ker::vfs::has_cached_bdev_range(&dev, BLK + 8, 8));

    std::array<uint8_t, 16 * 512> read_buf{};
    KEXPECT_TRUE(ker::vfs::copy_dirty_bdev_range(&dev, BLK, 16, read_buf.data()));
    KEXPECT_EQ(read_buf[0], static_cast<uint8_t>(0xCC));
    KEXPECT_EQ(read_buf[(8 * 512) - 1], static_cast<uint8_t>(0xCC));
    KEXPECT_EQ(read_buf[8 * 512], static_cast<uint8_t>(0));

    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, BreadMultiMissCopiesCompleteDirtyRangeWithoutDeviceRead) {
    ker::dev::BlockDevice dev = make_null_bdev();
    RecordingReadState reads{};
    dev.read_blocks = recording_read;
    dev.private_data = &reads;
    ker::vfs::invalidate_bdev(&dev);

    constexpr uint64_t BLK = 1100;
    constexpr size_t COUNT = 4;
    for (size_t i = 0; i < COUNT; ++i) {
        ker::vfs::BufHead* dirty = ker::vfs::bget(&dev, BLK + i);
        KREQUIRE_NE(dirty, nullptr);
        memset(dirty->data, static_cast<int>(0x30 + i), dirty->size);
        ker::vfs::bdirty(dirty);
        ker::vfs::brelse(dirty);
    }

    std::array<uint8_t, COUNT * 512> copied{};
    KEXPECT_TRUE(ker::vfs::copy_dirty_bdev_range_if_complete(&dev, BLK, COUNT, copied.data()));
    for (size_t i = 0; i < COUNT; ++i) {
        KEXPECT_EQ(copied.at(i * 512), static_cast<uint8_t>(0x30 + i));
    }

    ker::vfs::BufHead* reread = ker::vfs::bread_multi(&dev, BLK, COUNT);
    KREQUIRE_NE(reread, nullptr);
    KEXPECT_EQ(reads.read_calls, static_cast<size_t>(0));
    for (size_t i = 0; i < COUNT; ++i) {
        KEXPECT_EQ(reread->data[i * 512], static_cast<uint8_t>(0x30 + i));
    }

    ker::vfs::brelse(reread);
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, BreadHitOverlaysDirtyMultiAlias) {
    ker::dev::BlockDevice dev = make_null_bdev();
    ker::vfs::invalidate_bdev(&dev);

    const uint64_t BLK = 1200;

    ker::vfs::BufHead* clean_single = ker::vfs::bread(&dev, BLK + 3);
    KREQUIRE_NE(clean_single, nullptr);
    KEXPECT_EQ(clean_single->data[0], static_cast<uint8_t>(0));
    ker::vfs::brelse(clean_single);

    ker::vfs::BufHead* dirty_multi = ker::vfs::bget_multi(&dev, BLK, 8);
    KREQUIRE_NE(dirty_multi, nullptr);
    memset(dirty_multi->data, 0x5A, dirty_multi->size);
    ker::vfs::bdirty(dirty_multi);
    ker::vfs::brelse(dirty_multi);

    ker::vfs::BufHead* reread_single = ker::vfs::bread(&dev, BLK + 3);
    KREQUIRE_NE(reread_single, nullptr);
    KEXPECT_EQ(reread_single->data[0], static_cast<uint8_t>(0x5A));
    KEXPECT_EQ(reread_single->data[reread_single->size - 1], static_cast<uint8_t>(0x5A));
    ker::vfs::brelse(reread_single);
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, BreadMissCopiesCleanMultiAliasAfterWriteback) {
    ker::dev::BlockDevice dev = make_null_bdev();
    RecordingReadState reads{};
    dev.read_blocks = recording_read;
    dev.private_data = &reads;
    ker::vfs::invalidate_bdev(&dev);

    const uint64_t BLK = 1500;

    ker::vfs::BufHead* clean_single = ker::vfs::bread(&dev, BLK + 3);
    KREQUIRE_NE(clean_single, nullptr);
    KEXPECT_EQ(clean_single->data[0], static_cast<uint8_t>(0));
    ker::vfs::brelse(clean_single);
    KEXPECT_EQ(reads.read_calls, static_cast<size_t>(1));

    ker::vfs::BufHead* dirty_multi = ker::vfs::bget_multi(&dev, BLK, 8);
    KREQUIRE_NE(dirty_multi, nullptr);
    memset(dirty_multi->data, 0x61, dirty_multi->size);
    ker::vfs::bdirty(dirty_multi);
    ker::vfs::brelse(dirty_multi);

    KEXPECT_EQ(ker::vfs::sync_blockdev(&dev), 0);
    KEXPECT_FALSE(ker::vfs::has_dirty_bdev_range(&dev, BLK, 8));

    std::array<uint8_t, 2 * 512> copied{};
    KEXPECT_TRUE(ker::vfs::copy_cached_bdev_range_if_complete(&dev, BLK + 2, 2, copied.data()));
    KEXPECT_EQ(copied.at(0), static_cast<uint8_t>(0x61));
    KEXPECT_EQ(copied.at(copied.size() - 1), static_cast<uint8_t>(0x61));
    KEXPECT_EQ(reads.read_calls, static_cast<size_t>(1));

    ker::vfs::BufHead* reread_single = ker::vfs::bread(&dev, BLK + 3);
    KREQUIRE_NE(reread_single, nullptr);
    KEXPECT_EQ(reads.read_calls, static_cast<size_t>(1));
    KEXPECT_EQ(reread_single->data[0], static_cast<uint8_t>(0x61));
    KEXPECT_EQ(reread_single->data[reread_single->size - 1], static_cast<uint8_t>(0x61));
    ker::vfs::brelse(reread_single);
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, BreadMultiMissOverlaysDirtySingleAlias) {
    ker::dev::BlockDevice dev = make_null_bdev();
    ker::vfs::invalidate_bdev(&dev);

    const uint64_t BLK = 1300;
    const size_t RATIO = 8;

    ker::vfs::BufHead* dirty_single = ker::vfs::bread(&dev, BLK + 4);
    KREQUIRE_NE(dirty_single, nullptr);
    memset(dirty_single->data, 0xC3, dirty_single->size);
    ker::vfs::bdirty(dirty_single);
    ker::vfs::brelse(dirty_single);

    ker::vfs::BufHead* reread_multi = ker::vfs::bread_multi(&dev, BLK, RATIO);
    KREQUIRE_NE(reread_multi, nullptr);
    KEXPECT_EQ(reread_multi->data[0], static_cast<uint8_t>(0));
    KEXPECT_EQ(reread_multi->data[(4 * 512)], static_cast<uint8_t>(0xC3));
    KEXPECT_EQ(reread_multi->data[(5 * 512) - 1], static_cast<uint8_t>(0xC3));
    KEXPECT_EQ(reread_multi->data[(5 * 512)], static_cast<uint8_t>(0));
    ker::vfs::brelse(reread_multi);
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, BreadMultiHitOverlaysDirtySingleAlias) {
    ker::dev::BlockDevice dev = make_null_bdev();
    ker::vfs::invalidate_bdev(&dev);

    const uint64_t BLK = 1400;
    const size_t RATIO = 8;

    ker::vfs::BufHead* clean_multi = ker::vfs::bread_multi(&dev, BLK, RATIO);
    KREQUIRE_NE(clean_multi, nullptr);
    KEXPECT_EQ(clean_multi->data[0], static_cast<uint8_t>(0));
    ker::vfs::brelse(clean_multi);

    ker::vfs::BufHead* dirty_single = ker::vfs::bread(&dev, BLK + 2);
    KREQUIRE_NE(dirty_single, nullptr);
    memset(dirty_single->data, 0x7E, dirty_single->size);
    ker::vfs::bdirty(dirty_single);
    ker::vfs::brelse(dirty_single);

    ker::vfs::BufHead* reread_multi = ker::vfs::bread_multi(&dev, BLK, RATIO);
    KREQUIRE_NE(reread_multi, nullptr);
    KEXPECT_EQ(reread_multi->data[0], static_cast<uint8_t>(0));
    KEXPECT_EQ(reread_multi->data[(2 * 512)], static_cast<uint8_t>(0x7E));
    KEXPECT_EQ(reread_multi->data[(3 * 512) - 1], static_cast<uint8_t>(0x7E));
    KEXPECT_EQ(reread_multi->data[(3 * 512)], static_cast<uint8_t>(0));
    ker::vfs::brelse(reread_multi);
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, SyncBlockdevWritesOverlappingDirtyBuffersOldestFirst) {
    ker::dev::BlockDevice dev = make_null_bdev();
    RecordingWriteState io{};
    dev.write_blocks = recording_write;
    dev.private_data = &io;
    ker::vfs::invalidate_bdev(&dev);

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
    KREQUIRE_TRUE(found_hash_order);

    ker::vfs::BufHead* older_multi = ker::vfs::bget_multi(&dev, block_no, 2);
    KREQUIRE_NE(older_multi, nullptr);
    memset(older_multi->data, 0xA1, older_multi->size);
    ker::vfs::bdirty(older_multi);
    ker::vfs::brelse(older_multi);

    ker::vfs::BufHead* newer_single = ker::vfs::bget(&dev, block_no);
    KREQUIRE_NE(newer_single, nullptr);
    memset(newer_single->data, 0xB2, newer_single->size);
    ker::vfs::bdirty(newer_single);
    ker::vfs::brelse(newer_single);

    KEXPECT_EQ(ker::vfs::sync_blockdev(&dev), 0);
    KEXPECT_EQ(io.write_calls, static_cast<size_t>(2));
    KEXPECT_EQ(io.write_blocks[0], block_no);
    KEXPECT_EQ(io.write_counts[0], static_cast<size_t>(2));
    KEXPECT_EQ(io.write_first_bytes[0], static_cast<uint8_t>(0xA1));
    KEXPECT_EQ(io.write_blocks[1], block_no);
    KEXPECT_EQ(io.write_counts[1], static_cast<size_t>(1));
    KEXPECT_EQ(io.write_first_bytes[1], static_cast<uint8_t>(0xB2));
    KEXPECT_FALSE(ker::vfs::has_dirty_bdev_range(&dev, block_no, 2));
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, FailedOlderOverlappingWriteBlocksNewerAliasUntilRetry) {
    ker::dev::BlockDevice dev = make_null_bdev();
    RecordingWriteState io{};
    io.fail_write_calls_remaining = 1;
    dev.write_blocks = recording_write;
    dev.private_data = &io;
    ker::vfs::invalidate_bdev(&dev);

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
    KREQUIRE_TRUE(found_hash_order);

    ker::vfs::BufHead* older_multi = ker::vfs::bget_multi(&dev, block_no, 2);
    KREQUIRE_NE(older_multi, nullptr);
    memset(older_multi->data, 0xA1, older_multi->size);
    ker::vfs::bdirty(older_multi);
    ker::vfs::brelse(older_multi);

    ker::vfs::BufHead* newer_single = ker::vfs::bget(&dev, block_no);
    KREQUIRE_NE(newer_single, nullptr);
    memset(newer_single->data, 0xB2, newer_single->size);
    ker::vfs::bdirty(newer_single);
    ker::vfs::brelse(newer_single);

    KEXPECT_EQ(ker::vfs::sync_blockdev(&dev), -1);
    KREQUIRE_EQ(io.write_calls, static_cast<size_t>(1));
    KEXPECT_EQ(io.write_blocks[0], block_no);
    KEXPECT_EQ(io.write_counts[0], static_cast<size_t>(2));
    KEXPECT_EQ(io.write_first_bytes[0], static_cast<uint8_t>(0xA1));
    KEXPECT_TRUE(ker::vfs::has_dirty_bdev_range(&dev, block_no, 2));

    io.write_calls = 0;
    KEXPECT_EQ(ker::vfs::sync_blockdev(&dev), 0);
    KREQUIRE_EQ(io.write_calls, static_cast<size_t>(2));
    KEXPECT_EQ(io.write_blocks[0], block_no);
    KEXPECT_EQ(io.write_counts[0], static_cast<size_t>(2));
    KEXPECT_EQ(io.write_first_bytes[0], static_cast<uint8_t>(0xA1));
    KEXPECT_EQ(io.write_blocks[1], block_no);
    KEXPECT_EQ(io.write_counts[1], static_cast<size_t>(1));
    KEXPECT_EQ(io.write_first_bytes[1], static_cast<uint8_t>(0xB2));
    KEXPECT_FALSE(ker::vfs::has_dirty_bdev_range(&dev, block_no, 2));
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, BgetMultiSamePointer) {
    // Two bget_multi calls for the same (dev, block, count) must return
    // the same buffer (cache hit), so dirty data is always visible.
    ker::dev::BlockDevice dev = make_null_bdev();
    ker::vfs::invalidate_bdev(&dev);

    ker::vfs::BufHead* a = ker::vfs::bget_multi(&dev, 700, 8);
    KREQUIRE_NE(a, nullptr);
    ker::vfs::BufHead* b = ker::vfs::bget_multi(&dev, 700, 8);
    KREQUIRE_NE(b, nullptr);
    KEXPECT_EQ(a, b);
    ker::vfs::brelse(b);
    ker::vfs::brelse(a);
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, SyncBlockdevFailedWritesProgressAcrossBatches) {
    ker::dev::BlockDevice dev = make_null_bdev();
    RecordingWriteState io{};
    io.fail_writes = true;
    dev.write_blocks = recording_write;
    dev.private_data = &io;
    ker::vfs::invalidate_bdev(&dev);

    const uint64_t FIRST_BLOCK = 800;
    const size_t DIRTY_COUNT = 70;
    for (size_t i = 0; i < DIRTY_COUNT; ++i) {
        ker::vfs::BufHead* bh = ker::vfs::bget(&dev, FIRST_BLOCK + i);
        KREQUIRE_NE(bh, nullptr);
        bh->data[0] = static_cast<uint8_t>(i);
        ker::vfs::bdirty(bh);
        ker::vfs::brelse(bh);
    }

    KEXPECT_EQ(ker::vfs::sync_blockdev(&dev), -1);
    KREQUIRE_EQ(io.write_calls, static_cast<size_t>(1));
    KEXPECT_EQ(io.write_blocks[0], FIRST_BLOCK);
    KEXPECT_EQ(io.write_counts[0], DIRTY_COUNT);
    KEXPECT_TRUE(ker::vfs::has_dirty_bdev_range(&dev, FIRST_BLOCK, DIRTY_COUNT));

    io.fail_writes = false;
    io.write_calls = 0;
    KEXPECT_EQ(ker::vfs::sync_blockdev(&dev), 0);
    KREQUIRE_EQ(io.write_calls, static_cast<size_t>(1));
    KEXPECT_EQ(io.write_blocks[0], FIRST_BLOCK);
    KEXPECT_EQ(io.write_counts[0], DIRTY_COUNT);
    KEXPECT_FALSE(ker::vfs::has_dirty_bdev_range(&dev, FIRST_BLOCK, DIRTY_COUNT));
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, SyncBlockdevCoalescesContiguousDirtyRunsUpToFourMiB) {
    ker::dev::BlockDevice dev = make_null_bdev();
    dev.total_blocks = 16384;
    LargeCountingWriteState io{};
    dev.write_blocks = large_counting_write;
    dev.private_data = &io;
    ker::vfs::invalidate_bdev(&dev);

    constexpr uint64_t FIRST_BLOCK = 0;
    constexpr size_t BLOCKS_PER_DIRTY_BUFFER = 256;
    constexpr size_t DIRTY_BUFFER_COUNT = 32;
    constexpr size_t TOTAL_BLOCKS = BLOCKS_PER_DIRTY_BUFFER * DIRTY_BUFFER_COUNT;

    for (size_t i = 0; i < DIRTY_BUFFER_COUNT; ++i) {
        uint64_t const BLOCK = FIRST_BLOCK + (i * BLOCKS_PER_DIRTY_BUFFER);
        ker::vfs::BufHead* bh = ker::vfs::bget_multi(&dev, BLOCK, BLOCKS_PER_DIRTY_BUFFER);
        KREQUIRE_NE(bh, nullptr);
        memset(bh->data, static_cast<int>(0x40U + i), bh->size);
        ker::vfs::bdirty(bh);
        ker::vfs::brelse(bh);
    }

    KEXPECT_EQ(ker::vfs::sync_blockdev(&dev), 0);
    KREQUIRE_EQ(io.write_calls, static_cast<size_t>(1));
    KEXPECT_EQ(io.write_blocks[0], FIRST_BLOCK);
    KEXPECT_EQ(io.write_counts[0], TOTAL_BLOCKS);
    KEXPECT_FALSE(ker::vfs::has_dirty_bdev_range(&dev, FIRST_BLOCK, TOTAL_BLOCKS));
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, SyncBlockdevCoalescesManySmallDirtyBuffers) {
    ker::dev::BlockDevice dev = make_null_bdev();
    dev.total_blocks = 8192;
    LargeCountingWriteState io{};
    dev.write_blocks = large_counting_write;
    dev.private_data = &io;
    ker::vfs::invalidate_bdev(&dev);

    constexpr uint64_t FIRST_BLOCK = 0;
    constexpr size_t BLOCKS_PER_DIRTY_BUFFER = 8;
    constexpr size_t DIRTY_BUFFER_COUNT = 1024;
    constexpr size_t TOTAL_BLOCKS = BLOCKS_PER_DIRTY_BUFFER * DIRTY_BUFFER_COUNT;

    for (size_t i = 0; i < DIRTY_BUFFER_COUNT; ++i) {
        uint64_t const BLOCK = FIRST_BLOCK + (i * BLOCKS_PER_DIRTY_BUFFER);
        ker::vfs::BufHead* bh = ker::vfs::bget_multi(&dev, BLOCK, BLOCKS_PER_DIRTY_BUFFER);
        KREQUIRE_NE(bh, nullptr);
        memset(bh->data, static_cast<int>(0x30U + (i & 0x3FU)), bh->size);
        ker::vfs::bdirty(bh);
        ker::vfs::brelse(bh);
    }

    KEXPECT_EQ(ker::vfs::sync_blockdev(&dev), 0);
    KREQUIRE_EQ(io.write_calls, static_cast<size_t>(1));
    KEXPECT_EQ(io.write_blocks[0], FIRST_BLOCK);
    KEXPECT_EQ(io.write_counts[0], TOTAL_BLOCKS);
    KEXPECT_FALSE(ker::vfs::has_dirty_bdev_range(&dev, FIRST_BLOCK, TOTAL_BLOCKS));
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, SyncRangeCoalescesContiguousDirtyRunsByBlockOrder) {
    ker::dev::BlockDevice dev = make_null_bdev();
    RecordingWriteState io{};
    dev.write_blocks = recording_write;
    dev.private_data = &io;
    ker::vfs::invalidate_bdev(&dev);

    constexpr uint64_t FIRST_BLOCK = 1000;
    constexpr std::array<uint64_t, 4> DIRTY_ORDER{FIRST_BLOCK, FIRST_BLOCK + 2, FIRST_BLOCK + 1, FIRST_BLOCK + 3};

    for (size_t i = 0; i < DIRTY_ORDER.size(); ++i) {
        ker::vfs::BufHead* bh = ker::vfs::bget(&dev, DIRTY_ORDER.at(i));
        KREQUIRE_NE(bh, nullptr);
        bh->data[0] = static_cast<uint8_t>(0x90U + i);
        ker::vfs::bdirty(bh);
        ker::vfs::brelse(bh);
    }

    KEXPECT_EQ(ker::vfs::sync_bdev_range(&dev, FIRST_BLOCK, DIRTY_ORDER.size()), 0);
    KREQUIRE_EQ(io.write_calls, static_cast<size_t>(1));
    KEXPECT_EQ(io.write_blocks[0], FIRST_BLOCK);
    KEXPECT_EQ(io.write_counts[0], DIRTY_ORDER.size());
    KEXPECT_FALSE(ker::vfs::has_dirty_bdev_range(&dev, FIRST_BLOCK, DIRTY_ORDER.size()));
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, RangeOperationsHandleLastBlockWithoutWrapping) {
    ker::dev::BlockDevice dev = make_null_bdev();
    ker::vfs::invalidate_bdev(&dev);

    const uint64_t LAST_BLOCK = UINT64_MAX;
    ker::vfs::BufHead* bh = ker::vfs::bget(&dev, LAST_BLOCK);
    KREQUIRE_NE(bh, nullptr);
    bh->data[0] = 0xE7;
    ker::vfs::bdirty(bh);
    ker::vfs::brelse(bh);

    KEXPECT_TRUE(ker::vfs::has_dirty_bdev_range(&dev, LAST_BLOCK, 1));
    KEXPECT_FALSE(ker::vfs::has_dirty_bdev_range(&dev, 0, 1));
    ker::vfs::discard_bdev_range(&dev, LAST_BLOCK, 1);
    KEXPECT_FALSE(ker::vfs::has_dirty_bdev_range(&dev, LAST_BLOCK, 1));
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(BufferCache, SyncRangeFailedWritesProgressAcrossBatches) {
    ker::dev::BlockDevice dev = make_null_bdev();
    RecordingWriteState io{};
    io.fail_writes = true;
    dev.write_blocks = recording_write;
    dev.private_data = &io;
    ker::vfs::invalidate_bdev(&dev);

    const uint64_t FIRST_BLOCK = 900;
    const size_t DIRTY_COUNT = 70;
    for (size_t i = 0; i < DIRTY_COUNT; ++i) {
        ker::vfs::BufHead* bh = ker::vfs::bget(&dev, FIRST_BLOCK + i);
        KREQUIRE_NE(bh, nullptr);
        bh->data[0] = static_cast<uint8_t>(0x80U + i);
        ker::vfs::bdirty(bh);
        ker::vfs::brelse(bh);
    }

    KEXPECT_EQ(ker::vfs::sync_bdev_range(&dev, FIRST_BLOCK, DIRTY_COUNT), -1);
    KREQUIRE_EQ(io.write_calls, static_cast<size_t>(1));
    KEXPECT_EQ(io.write_blocks[0], FIRST_BLOCK);
    KEXPECT_EQ(io.write_counts[0], DIRTY_COUNT);
    KEXPECT_TRUE(ker::vfs::has_dirty_bdev_range(&dev, FIRST_BLOCK, DIRTY_COUNT));

    io.fail_writes = false;
    io.write_calls = 0;
    KEXPECT_EQ(ker::vfs::sync_bdev_range(&dev, FIRST_BLOCK, DIRTY_COUNT), 0);
    KREQUIRE_EQ(io.write_calls, static_cast<size_t>(1));
    KEXPECT_EQ(io.write_blocks[0], FIRST_BLOCK);
    KEXPECT_EQ(io.write_counts[0], DIRTY_COUNT);
    KEXPECT_FALSE(ker::vfs::has_dirty_bdev_range(&dev, FIRST_BLOCK, DIRTY_COUNT));
    ker::vfs::invalidate_bdev(&dev);
}

KTEST_OFF(BufferCache, ConcurrentDedup) {
    // Requires scheduler running (two tasks racing bread for same block).
    // Disabled: selftests run single-threaded before smt::start_smt().
    // Host coverage: BufferCacheTest.ConcurrentBreadDeduplicatesColdBlock
}

KTEST_OFF(BufferCache, RedirtyDuringInFlightWriteback) {
    // Requires scheduler running with one task blocked in device writeback
    // while another task dirties the same referenced buffer.
    // Disabled: selftests run single-threaded before smt::start_smt().
    // Host coverage: BufferCacheTest.RedirtyDuringInFlightWritebackRemainsDirty
}

KTEST_OFF(BufferCache, SyncRangeFlushesAfterWaitingForForeignWriteback) {
    // Requires scheduler running with one task blocked in device writeback
    // while another task syncs the same block range.
    // Disabled: selftests run single-threaded before smt::start_smt().
    // Host coverage: BufferCacheTest.SyncRangeFlushesAfterWaitingForForeignWriteback
}
