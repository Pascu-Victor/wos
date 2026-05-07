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

auto null_write(ker::dev::BlockDevice* /*dev*/, uint64_t /*block*/, size_t /*count*/, const void* /*buffer*/) -> int {
    return 0;
}

auto make_null_bdev() -> ker::dev::BlockDevice {
    ker::dev::BlockDevice d{};
    d.block_size = 512;
    d.total_blocks = 512;
    d.read_blocks = null_read;
    d.write_blocks = null_write;
    return d;
}

}  // namespace

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

    const uint64_t BLK   = 400;
    const size_t   RATIO = 8;  // 4096-byte XFS block / 512-byte sector

    ker::vfs::BufHead* wp = ker::vfs::bget_multi(&dev, BLK, RATIO);
    KREQUIRE_NE(wp, nullptr);
    KEXPECT_EQ(wp->size, static_cast<size_t>(RATIO * 512));
    for (size_t i = 0; i < wp->size; i++) { wp->data[i] = static_cast<uint8_t>(i & 0xFF); }
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
        if (rp->data[i] != 0xAA) { ok = false; break; }
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
    for (size_t i = 0; i < mb->size; i++) { mb->data[i] = static_cast<uint8_t>(i & 0xFF); }
    ker::vfs::bdirty(mb);
    ker::vfs::brelse(mb);

    // bread_multi(8) must return the 8-sector buffer with the written pattern
    ker::vfs::BufHead* rp = ker::vfs::bread_multi(&dev, BLK, 8);
    KREQUIRE_NE(rp, nullptr);
    KEXPECT_EQ(rp->size, static_cast<size_t>(8 * 512));
    bool ok = true;
    for (size_t i = 0; i < rp->size; i++) {
        if (rp->data[i] != static_cast<uint8_t>(i & 0xFF)) { ok = false; break; }
    }
    KEXPECT_TRUE(ok);
    ker::vfs::brelse(rp);
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

KTEST_OFF(BufferCache, ConcurrentDedup) {
    // Requires scheduler running (two tasks racing bread for same block).
    // Disabled: selftests run single-threaded before smt::start_smt().
}
