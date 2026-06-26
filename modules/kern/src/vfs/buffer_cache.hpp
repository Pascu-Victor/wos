#pragma once

// Buffer Cache for block-level metadata I/O.
// Provides bread/brelse/bwrite API used by filesystem drivers (especially XFS).
// Reference: Linux fs/buffer.c, xfs/xfs_buf.c

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <dev/block_device.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::vfs {

// Buffer flags
constexpr uint32_t BH_DIRTY = (1U << 0);            // Buffer has been modified
constexpr uint32_t BH_VALID = (1U << 1);            // Buffer contains valid data from disk
constexpr uint32_t BH_LOCKED = (1U << 2);           // Buffer is locked for I/O
constexpr uint32_t BH_WRITEBACK = (1U << 3);        // Buffer is being written back
constexpr uint32_t BH_DATA_PAGE_ALLOC = (1U << 4);  // Buffer data was allocated through the page allocator
constexpr uint32_t BH_DIRTY_INDEXED = (1U << 5);    // Buffer is present in the dirty range index
constexpr uint32_t BH_RANGE_INDEXED = (1U << 6);    // Buffer is present in the cached range index

// Buffer head - represents a single cached block from a block device.
// Analogous to Linux struct buffer_head / simplified xfs_buf.
struct BufHead {
    uint8_t* data{};                   // Pointer to cached block data
    uint64_t block_no{};               // Block number on the device
    dev::BlockDevice* bdev{};          // Owning block device
    std::atomic<int32_t> refcount;     // Reference count (0 = reclaimable)
    uint32_t flags{};                  // BH_DIRTY, BH_VALID, etc.
    uint64_t writeback_epoch{};        // Owner token for an in-progress writeback
    uint64_t dirty_epoch{};            // Incremented whenever data is marked dirty
    uint64_t writeback_dirty_epoch{};  // Dirty epoch captured by in-progress writeback
    size_t size{};                     // Size of buffer in bytes (== bdev->block_size)

    // LRU doubly-linked list pointers (protected by cache lock)
    BufHead* lru_prev{};
    BufHead* lru_next{};

    // Hash chain pointers (protected by cache lock)
    BufHead* hash_next{};

    // Dirty index links (protected by cache lock). Dirty buffers are indexed
    // per block device by their actual interval, independent of the exact
    // hash key used for overlapping single-block vs multi-block cache entries.
    BufHead* dirty_prev{};
    BufHead* dirty_next{};
    BufHead* dirty_left{};
    BufHead* dirty_right{};
    BufHead* dirty_parent{};
    uint64_t dirty_subtree_last_block{};

    // Cached range index links (protected by cache lock). This indexes clean
    // and dirty buffers by device block span so exact-size cache misses can
    // still reuse overlapping cached aliases safely.
    BufHead* range_left{};
    BufHead* range_right{};
    BufHead* range_parent{};
    uint64_t range_subtree_last_block{};
};

// Buffer cache configuration. This is the minimum/default cap; the live kernel
// scales the cache up at init when enough physical memory is available.
constexpr size_t BUFFER_CACHE_DEFAULT_SIZE = static_cast<size_t>(64) * 1024 * 1024;  // 64 MB
constexpr size_t BUFFER_CACHE_HASH_BUCKETS = 4096;

// Initialize the buffer cache subsystem. Call once at kernel init.
void buffer_cache_init();

// Read a block from the cache (or disk if not cached).
// Returns a referenced BufHead* with valid data. Caller must call brelse() when done.
// Returns nullptr on I/O error.
auto bread(dev::BlockDevice* bdev, uint64_t block_no) -> BufHead*;

// Read multiple contiguous blocks. Returns pointer to the first BufHead in an
// internally-managed array. Caller must call brelse() on the returned BufHead.
// `count` blocks starting at `block_no` are read. The returned buffer's `size`
// field reflects the total byte count (count * block_size).
// Returns nullptr on I/O error.
auto bread_multi(dev::BlockDevice* bdev, uint64_t block_no, size_t count) -> BufHead*;

// Release a buffer - decrements refcount. If refcount reaches 0 the buffer
// becomes eligible for LRU eviction but is NOT freed immediately.
void brelse(BufHead* bh);

// Mark a buffer dirty and write it to disk immediately (synchronous).
// Returns 0 on success, negative errno on failure.
auto bwrite(BufHead* bh) -> int;

// Mark a buffer dirty for deferred writeback (async).
void bdirty(BufHead* bh);

// Get (or create) a buffer for a block WITHOUT reading from disk.
// Use for blocks that are about to be fully overwritten (newly allocated).
// The buffer data is uninitialized; the caller must write valid content before
// calling bdirty() / bwrite(). Returns a referenced BufHead* on success.
auto bget(dev::BlockDevice* bdev, uint64_t block_no) -> BufHead*;

// Like bget but for count contiguous device blocks (analogous to bread_multi).
// Returns a single BufHead whose data covers count * block_size bytes.
auto bget_multi(dev::BlockDevice* bdev, uint64_t block_no, size_t count) -> BufHead*;

// Write all dirty buffers for a given block device to disk.
// Returns 0 on success, negative errno on failure.
auto sync_blockdev(dev::BlockDevice* bdev) -> int;

// Return true if any dirty cached buffer overlaps a device block range.
auto has_dirty_bdev_range(dev::BlockDevice* bdev, uint64_t block_no, size_t count) -> bool;

// Return true if any cached buffer overlaps a device block range.
auto has_cached_bdev_range(dev::BlockDevice* bdev, uint64_t block_no, size_t count) -> bool;

// Overlay dirty cached buffers that overlap a device block range onto an
// already-read destination buffer. Dirty buffers are applied in dirty_epoch
// order so newer overlapping cache entries remain authoritative.
auto copy_dirty_bdev_range(dev::BlockDevice* bdev, uint64_t block_no, size_t count, uint8_t* dst) -> bool;

// Copy dirty cached buffers only when they fully cover a device block range.
// Returns false without guaranteeing dst contents when coverage is partial.
auto copy_dirty_bdev_range_if_complete(dev::BlockDevice* bdev, uint64_t block_no, size_t count, uint8_t* dst) -> bool;

// Copy clean and dirty cached buffers only when they fully cover a device block
// range. Dirty buffers are overlaid last so they stay authoritative.
// Returns false without guaranteeing dst contents when coverage is partial.
auto copy_cached_bdev_range_if_complete(dev::BlockDevice* bdev, uint64_t block_no, size_t count, uint8_t* dst) -> bool;

// Write dirty buffers overlapping a device block range to disk.
// Returns 0 on success, negative errno on failure.
auto sync_bdev_range(dev::BlockDevice* bdev, uint64_t block_no, size_t count) -> int;

// Request background dirty-cache writeback if dirty bytes exceed the target.
// This never waits for writeback to complete.
void kick_dirty_buffer_cache_writeback(dev::BlockDevice* bdev);

// Apply dirty-cache pressure for a block device. Writers only wait after the
// hard dirty limit is exceeded; below that this only kicks background writeback.
void throttle_dirty_buffer_cache(dev::BlockDevice* bdev);

// Invalidate unreferenced cached buffers for a device (e.g. on unmount).
// Referenced buffers are left cached so callers holding BufHead pointers do
// not observe use-after-free; releasing them and calling invalidate again will
// remove them.
void invalidate_bdev(dev::BlockDevice* bdev);

// Discard cached buffers overlapping a device block range without writeback.
// Use only when the caller has made the storage range authoritative by other
// means, such as overwriting freshly allocated blocks directly.
void discard_bdev_range(dev::BlockDevice* bdev, uint64_t block_no, size_t count);

// Get cache statistics for diagnostics.
struct BufferCacheStats {
    size_t total_buffers;
    size_t dirty_buffers;
    size_t total_bytes;
    size_t clean_bytes;
    size_t dirty_bytes;
    size_t max_bytes;
    size_t dirty_bdevs;
    size_t dirty_target_bytes;
    size_t dirty_hard_bytes;
    size_t dirty_waiters;
    uint64_t hits;
    uint64_t misses;
};
auto buffer_cache_stats() -> BufferCacheStats;

struct BufferCacheBdevStats {
    dev::BlockDevice* bdev;
    const char* name;
    size_t dirty_buffers;
    size_t dirty_bytes;
    uint64_t oldest_dirty_epoch;
};

auto buffer_cache_bdev_stats(BufferCacheBdevStats* out, size_t capacity) -> size_t;

struct BufferCacheReclaimStats {
    size_t before_bytes;
    size_t after_bytes;
    size_t freed_buffers;
    size_t freed_bytes;
};

// Drop clean, unreferenced buffers until the logical cache is at or below
// target_bytes. Dirty or referenced buffers are never written or discarded.
auto reclaim_clean_buffer_cache(size_t target_bytes) -> BufferCacheReclaimStats;

}  // namespace ker::vfs
