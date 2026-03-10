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
constexpr uint32_t BH_DIRTY = (1u << 0);      // Buffer has been modified
constexpr uint32_t BH_VALID = (1u << 1);      // Buffer contains valid data from disk
constexpr uint32_t BH_LOCKED = (1u << 2);     // Buffer is locked for I/O
constexpr uint32_t BH_WRITEBACK = (1u << 3);  // Buffer is being written back

// Buffer head — represents a single cached block from a block device.
// Analogous to Linux struct buffer_head / simplified xfs_buf.
struct BufHead {
    uint8_t* data;                  // Pointer to cached block data
    uint64_t block_no;              // Block number on the device
    dev::BlockDevice* bdev;         // Owning block device
    std::atomic<int32_t> refcount;  // Reference count (0 = reclaimable)
    uint32_t flags;                 // BH_DIRTY, BH_VALID, etc.
    size_t size;                    // Size of buffer in bytes (== bdev->block_size)

    // LRU doubly-linked list pointers (protected by cache lock)
    BufHead* lru_prev;
    BufHead* lru_next;

    // Hash chain pointers (protected by cache lock)
    BufHead* hash_next;
};

// Buffer cache configuration
constexpr size_t BUFFER_CACHE_DEFAULT_SIZE = 64 * 1024 * 1024;  // 64 MB
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

// Release a buffer — decrements refcount. If refcount reaches 0 the buffer
// becomes eligible for LRU eviction but is NOT freed immediately.
void brelse(BufHead* bh);

// Mark a buffer dirty and write it to disk immediately (synchronous).
// Returns 0 on success, negative errno on failure.
auto bwrite(BufHead* bh) -> int;

// Mark a buffer dirty for deferred writeback (async).
void bdirty(BufHead* bh);

// Write all dirty buffers for a given block device to disk.
// Returns 0 on success, negative errno on failure.
auto sync_blockdev(dev::BlockDevice* bdev) -> int;

// Invalidate all cached buffers for a device (e.g. on unmount).
void invalidate_bdev(dev::BlockDevice* bdev);

// Get cache statistics for diagnostics.
struct BufferCacheStats {
    size_t total_buffers;
    size_t dirty_buffers;
    size_t total_bytes;
    size_t max_bytes;
    uint64_t hits;
    uint64_t misses;
};
auto buffer_cache_stats() -> BufferCacheStats;

}  // namespace ker::vfs
