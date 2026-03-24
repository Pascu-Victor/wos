// Buffer Cache implementation.
// Provides bread/brelse/bwrite block caching for filesystem metadata I/O.
// Reference: Linux fs/buffer.c, xfs/xfs_buf.c
//
// Design:
//   - Hash table indexed by (bdev, block_no) for O(1) lookup.
//   - LRU doubly-linked list for eviction of unreferenced buffers.
//   - Configurable maximum cache size (default 64 MB).
//   - All operations protected by a single spinlock (sufficient for the
//     current single-threaded I/O path; can be sharded later).

#include "buffer_cache.hpp"

#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>

namespace ker::vfs {

namespace {

// --- Hash table -----------------------------------------------------------

BufHead* hash_buckets[BUFFER_CACHE_HASH_BUCKETS] = {};  // NOLINT

auto hash_key(const dev::BlockDevice* bdev, uint64_t block_no) -> size_t {
    // FNV-1a-style mix of device pointer and block number
    auto h = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(bdev));
    h ^= block_no;
    h *= 0x517cc1b727220a95ULL;
    h ^= (h >> 32);
    return static_cast<size_t>(h % BUFFER_CACHE_HASH_BUCKETS);
}

auto hash_lookup(dev::BlockDevice* bdev, uint64_t block_no) -> BufHead* {
    size_t idx = hash_key(bdev, block_no);
    for (BufHead* bh = hash_buckets[idx]; bh != nullptr; bh = bh->hash_next) {
        if (bh->bdev == bdev && bh->block_no == block_no) {
            return bh;
        }
    }
    return nullptr;
}

void hash_insert(BufHead* bh) {
    size_t idx = hash_key(bh->bdev, bh->block_no);
    bh->hash_next = hash_buckets[idx];
    hash_buckets[idx] = bh;
}

void hash_remove(BufHead* bh) {
    size_t idx = hash_key(bh->bdev, bh->block_no);
    BufHead** pp = &hash_buckets[idx];
    while (*pp != nullptr) {
        if (*pp == bh) {
            *pp = bh->hash_next;
            bh->hash_next = nullptr;
            return;
        }
        pp = &(*pp)->hash_next;
    }
}

// --- LRU list (most-recently-used at head, least at tail) -----------------

BufHead lru_sentinel;  // Dummy sentinel node for circular doubly-linked list
size_t lru_count = 0;

void lru_init() {
    lru_sentinel.lru_prev = &lru_sentinel;
    lru_sentinel.lru_next = &lru_sentinel;
    lru_count = 0;
}

void lru_remove(BufHead* bh) {
    if (bh->lru_prev == nullptr && bh->lru_next == nullptr) {
        return;  // Not on LRU
    }
    bh->lru_prev->lru_next = bh->lru_next;
    bh->lru_next->lru_prev = bh->lru_prev;
    bh->lru_prev = nullptr;
    bh->lru_next = nullptr;
    lru_count--;
}

// Insert at head (most recently used)
void lru_touch(BufHead* bh) {
    // Remove if already on list
    if (bh->lru_prev != nullptr || bh->lru_next != nullptr) {
        lru_remove(bh);
    }
    // Insert after sentinel (= head)
    bh->lru_next = lru_sentinel.lru_next;
    bh->lru_prev = &lru_sentinel;
    lru_sentinel.lru_next->lru_prev = bh;
    lru_sentinel.lru_next = bh;
    lru_count++;
}

// Get the least-recently-used buffer (tail of the list, just before sentinel)
auto lru_tail() -> BufHead* {
    BufHead* bh = lru_sentinel.lru_prev;
    if (bh == &lru_sentinel) {
        return nullptr;
    }
    return bh;
}

// --- Cache bookkeeping ----------------------------------------------------

mod::sys::Spinlock cache_lock;
size_t cache_total_bytes = 0;
size_t cache_max_bytes = BUFFER_CACHE_DEFAULT_SIZE;
size_t cache_total_buffers = 0;
size_t cache_dirty_buffers = 0;

uint64_t stat_hits = 0;
uint64_t stat_misses = 0;

bool cache_initialized = false;

// Free a buffer's resources completely (removes from hash + LRU).
void free_buffer(BufHead* bh) {
    hash_remove(bh);
    lru_remove(bh);

    if ((bh->flags & BH_DIRTY) != 0) {
        cache_dirty_buffers--;
    }
    cache_total_bytes -= bh->size;
    cache_total_buffers--;

    mod::mm::dyn::kmalloc::free(bh->data);
    mod::mm::dyn::kmalloc::free(bh);
}

// Try to evict unreferenced clean buffers until we are below the max cache size.
void evict_lru() {
    while (cache_total_bytes > cache_max_bytes) {
        BufHead* victim = lru_tail();
        if (victim == nullptr) {
            break;  // Nothing to evict
        }
        // Skip buffers that are still referenced or dirty
        if (victim->refcount.load(std::memory_order_relaxed) > 0 || (victim->flags & BH_DIRTY) != 0) {
            // Walk backwards looking for an evictable buffer
            BufHead* cur = victim;
            bool found = false;
            while (cur != &lru_sentinel) {
                if (cur->refcount.load(std::memory_order_relaxed) == 0 && (cur->flags & BH_DIRTY) == 0) {
                    victim = cur;
                    found = true;
                    break;
                }
                cur = cur->lru_prev;
            }
            if (!found) {
                break;  // All buffers are referenced or dirty — cannot evict
            }
        }
        free_buffer(victim);
    }
}

// Allocate a new BufHead with backing data buffer.
auto alloc_buffer(dev::BlockDevice* bdev, uint64_t block_no) -> BufHead* {
    auto* bh = static_cast<BufHead*>(mod::mm::dyn::kmalloc::malloc(sizeof(BufHead)));
    if (bh == nullptr) {
        return nullptr;
    }

    size_t blk_size = bdev->block_size;
    auto* data = static_cast<uint8_t*>(mod::mm::dyn::kmalloc::malloc(blk_size));
    if (data == nullptr) {
        mod::mm::dyn::kmalloc::free(bh);
        return nullptr;
    }

    bh->data = data;
    bh->block_no = block_no;
    bh->bdev = bdev;
    bh->refcount.store(1, std::memory_order_relaxed);
    bh->flags = 0;
    bh->size = blk_size;
    bh->lru_prev = nullptr;
    bh->lru_next = nullptr;
    bh->hash_next = nullptr;

    cache_total_bytes += blk_size;
    cache_total_buffers++;

    return bh;
}

// Read the block data from disk into an already-allocated buffer.
auto read_block_from_disk(BufHead* bh) -> int {
    int rc = dev::block_read(bh->bdev, bh->block_no, 1, bh->data);
    if (rc == 0) {
        bh->flags |= BH_VALID;
    }
    return rc;
}

// Write the buffer data to disk.
auto write_block_to_disk(BufHead* bh) -> int {
    size_t block_count = bh->size / bh->bdev->block_size;
    if (block_count == 0) block_count = 1;
    return dev::block_write(bh->bdev, bh->block_no, block_count, bh->data);
}

}  // anonymous namespace

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

void buffer_cache_init() {
    if (cache_initialized) {
        return;
    }
    memset(hash_buckets, 0, sizeof(hash_buckets));
    lru_init();
    cache_total_bytes = 0;
    cache_total_buffers = 0;
    cache_dirty_buffers = 0;
    stat_hits = 0;
    stat_misses = 0;
    cache_initialized = true;
    mod::dbg::log("[buffer_cache] initialized (max %lu bytes)\n", static_cast<uint64_t>(cache_max_bytes));
}

auto bread(dev::BlockDevice* bdev, uint64_t block_no) -> BufHead* {
    if (bdev == nullptr) {
        return nullptr;
    }

    uint64_t irqflags = cache_lock.lock_irqsave();

    // 1. Lookup in cache
    BufHead* bh = hash_lookup(bdev, block_no);
    if (bh != nullptr) {
        bh->refcount.fetch_add(1, std::memory_order_relaxed);
        lru_touch(bh);
        stat_hits++;
        cache_lock.unlock_irqrestore(irqflags);
        return bh;
    }

    // 2. Cache miss — allocate and read from disk
    stat_misses++;

    // Evict if necessary before allocating
    evict_lru();

    bh = alloc_buffer(bdev, block_no);
    if (bh == nullptr) {
        cache_lock.unlock_irqrestore(irqflags);
        mod::dbg::log("[buffer_cache] OOM allocating buffer for block %lu\n", block_no);
        return nullptr;
    }

    hash_insert(bh);
    lru_touch(bh);

    // Drop lock during disk I/O (disk read may be slow)
    cache_lock.unlock_irqrestore(irqflags);

    int rc = read_block_from_disk(bh);
    if (rc != 0) {
        // Read failed — remove from cache
        irqflags = cache_lock.lock_irqsave();
        free_buffer(bh);
        cache_lock.unlock_irqrestore(irqflags);
        return nullptr;
    }

    return bh;
}

auto bread_multi(dev::BlockDevice* bdev, uint64_t block_no, size_t count) -> BufHead* {
    if (bdev == nullptr || count == 0) {
        return nullptr;
    }
    if (count == 1) {
        return bread(bdev, block_no);
    }

    size_t blk_size = bdev->block_size;
    size_t total_size = blk_size * count;

    // Multi-block buffers are cached by (bdev, block_no) like single-block
    // buffers.  This is critical for correctness: within a transaction,
    // multiple reads of the same XFS block (e.g. AG block 0) must return
    // the same buffer so that in-transaction writes are visible to subsequent
    // reads and the transaction system can merge dirty regions correctly.
    uint64_t irqflags = cache_lock.lock_irqsave();

    BufHead* bh = hash_lookup(bdev, block_no);
    if (bh != nullptr) {
        // Verify size matches (should always match for fixed XFS block size)
        if (bh->size == total_size) {
            bh->refcount.fetch_add(1, std::memory_order_relaxed);
            lru_touch(bh);
            stat_hits++;
            cache_lock.unlock_irqrestore(irqflags);
            return bh;
        }
        // Size mismatch — fall through to allocate fresh (shouldn't happen)
    }

    stat_misses++;
    evict_lru();

    bh = static_cast<BufHead*>(mod::mm::dyn::kmalloc::malloc(sizeof(BufHead)));
    if (bh == nullptr) {
        cache_lock.unlock_irqrestore(irqflags);
        return nullptr;
    }
    auto* data = static_cast<uint8_t*>(mod::mm::dyn::kmalloc::malloc(total_size));
    if (data == nullptr) {
        mod::mm::dyn::kmalloc::free(bh);
        cache_lock.unlock_irqrestore(irqflags);
        return nullptr;
    }

    bh->data = data;
    bh->block_no = block_no;
    bh->bdev = bdev;
    bh->refcount.store(1, std::memory_order_relaxed);
    bh->flags = 0;
    bh->size = total_size;
    bh->lru_prev = nullptr;
    bh->lru_next = nullptr;
    bh->hash_next = nullptr;

    cache_total_bytes += total_size;
    cache_total_buffers++;

    hash_insert(bh);
    lru_touch(bh);

    cache_lock.unlock_irqrestore(irqflags);

    // Read from disk outside the lock
    int rc = dev::block_read(bdev, block_no, count, data);
    if (rc != 0) {
        irqflags = cache_lock.lock_irqsave();
        free_buffer(bh);
        cache_lock.unlock_irqrestore(irqflags);
        return nullptr;
    }
    bh->flags |= BH_VALID;

    return bh;
}

void brelse(BufHead* bh) {
    if (bh == nullptr) {
        return;
    }

    int32_t prev = bh->refcount.fetch_sub(1, std::memory_order_acq_rel);
    if (prev <= 1) {
        // Refcount hit zero. If the buffer is not in the hash (multi-block),
        // free it immediately.
        if (bh->lru_prev == nullptr && bh->lru_next == nullptr && bh->hash_next == nullptr) {
            // Not in cache (multi-block temporary buffer) — check if it's also
            // not the sole entry in any hash bucket. For multi-block buffers
            // (which are never inserted into the hash), just free.
            // Simple heuristic: if size > block_size, it's a multi-block buffer.
            if (bh->bdev != nullptr && bh->size > bh->bdev->block_size) {
                mod::mm::dyn::kmalloc::free(bh->data);
                mod::mm::dyn::kmalloc::free(bh);
                return;
            }
        }
        // Otherwise the buffer stays in the cache for potential reuse.
        // LRU eviction will reclaim it when space is needed.
    }
}

auto bwrite(BufHead* bh) -> int {
    if (bh == nullptr) {
        return -1;
    }

    int rc = write_block_to_disk(bh);
    if (rc == 0) {
        uint64_t irqflags = cache_lock.lock_irqsave();
        if ((bh->flags & BH_DIRTY) != 0) {
            bh->flags &= ~BH_DIRTY;
            cache_dirty_buffers--;
        }
        cache_lock.unlock_irqrestore(irqflags);
    }
    return rc;
}

void bdirty(BufHead* bh) {
    if (bh == nullptr) {
        return;
    }

    uint64_t irqflags = cache_lock.lock_irqsave();
    if ((bh->flags & BH_DIRTY) == 0) {
        bh->flags |= BH_DIRTY;
        cache_dirty_buffers++;
    }
    cache_lock.unlock_irqrestore(irqflags);
}

auto bget(dev::BlockDevice* bdev, uint64_t block_no) -> BufHead* {
    if (bdev == nullptr) {
        return nullptr;
    }

    uint64_t irqflags = cache_lock.lock_irqsave();

    // If the block is already cached, return it (caller will overwrite).
    BufHead* bh = hash_lookup(bdev, block_no);
    if (bh != nullptr) {
        bh->refcount.fetch_add(1, std::memory_order_relaxed);
        lru_touch(bh);
        cache_lock.unlock_irqrestore(irqflags);
        return bh;
    }

    // Not cached — allocate a new buffer and insert without reading.
    evict_lru();
    bh = alloc_buffer(bdev, block_no);
    if (bh == nullptr) {
        cache_lock.unlock_irqrestore(irqflags);
        return nullptr;
    }
    bh->flags = BH_VALID;  // Mark valid without disk read
    hash_insert(bh);
    lru_touch(bh);
    cache_lock.unlock_irqrestore(irqflags);
    return bh;
}

auto bget_multi(dev::BlockDevice* bdev, uint64_t block_no, size_t count) -> BufHead* {
    if (bdev == nullptr || count == 0) {
        return nullptr;
    }
    if (count == 1) {
        return bget(bdev, block_no);
    }

    size_t blk_size = bdev->block_size;
    size_t total_size = blk_size * count;

    uint64_t irqflags = cache_lock.lock_irqsave();

    // If already cached, return it (caller will overwrite contents).
    BufHead* bh = hash_lookup(bdev, block_no);
    if (bh != nullptr) {
        if (bh->size == total_size) {
            bh->refcount.fetch_add(1, std::memory_order_relaxed);
            lru_touch(bh);
            cache_lock.unlock_irqrestore(irqflags);
            return bh;
        }
        // Size mismatch — fall through to allocate fresh (shouldn't happen)
    }

    evict_lru();

    bh = static_cast<BufHead*>(mod::mm::dyn::kmalloc::malloc(sizeof(BufHead)));
    if (bh == nullptr) {
        cache_lock.unlock_irqrestore(irqflags);
        return nullptr;
    }
    auto* data = static_cast<uint8_t*>(mod::mm::dyn::kmalloc::malloc(total_size));
    if (data == nullptr) {
        mod::mm::dyn::kmalloc::free(bh);
        cache_lock.unlock_irqrestore(irqflags);
        return nullptr;
    }

    bh->data = data;
    bh->block_no = block_no;
    bh->bdev = bdev;
    bh->refcount.store(1, std::memory_order_relaxed);
    bh->flags = BH_VALID;
    bh->size = total_size;
    bh->lru_prev = nullptr;
    bh->lru_next = nullptr;
    bh->hash_next = nullptr;

    cache_total_bytes += total_size;
    cache_total_buffers++;

    hash_insert(bh);
    lru_touch(bh);
    cache_lock.unlock_irqrestore(irqflags);

    return bh;
}

auto sync_blockdev(dev::BlockDevice* bdev) -> int {
    if (bdev == nullptr) {
        return -1;
    }

    int result = 0;

    uint64_t irqflags = cache_lock.lock_irqsave();

    // Walk all hash buckets collecting dirty buffers for this device.
    // We bump refcount so they stay alive while we write with the lock dropped.
    // Use a fixed-size batch to avoid dynamic allocation.
    constexpr size_t BATCH_SIZE = 64;
    BufHead* batch[BATCH_SIZE];  // NOLINT
    size_t batch_count = 0;
    bool more = true;

    while (more) {
        batch_count = 0;
        more = false;

        for (size_t i = 0; i < BUFFER_CACHE_HASH_BUCKETS && batch_count < BATCH_SIZE; i++) {
            for (BufHead* bh = hash_buckets[i]; bh != nullptr && batch_count < BATCH_SIZE; bh = bh->hash_next) {
                if (bh->bdev == bdev && (bh->flags & BH_DIRTY) != 0) {
                    bh->refcount.fetch_add(1, std::memory_order_relaxed);
                    batch[batch_count++] = bh;
                }
            }
            // If the batch is full, there may be more dirty buffers
            if (batch_count >= BATCH_SIZE) {
                more = true;
                break;
            }
        }

        cache_lock.unlock_irqrestore(irqflags);

        // Write each dirty buffer with lock released
        for (size_t i = 0; i < batch_count; i++) {
            int rc = bwrite(batch[i]);
            if (rc != 0) {
                result = rc;
            }
            brelse(batch[i]);
        }

        if (more) {
            irqflags = cache_lock.lock_irqsave();
        }
    }

    // Flush the device if it supports it
    if (bdev->flush != nullptr) {
        int rc = dev::block_flush(bdev);
        if (rc != 0) {
            result = rc;
        }
    }

    return result;
}

void invalidate_bdev(dev::BlockDevice* bdev) {
    if (bdev == nullptr) {
        return;
    }

    uint64_t irqflags = cache_lock.lock_irqsave();

    // Walk all hash buckets; remove buffers belonging to this device.
    for (size_t i = 0; i < BUFFER_CACHE_HASH_BUCKETS; i++) {
        BufHead** pp = &hash_buckets[i];
        while (*pp != nullptr) {
            BufHead* bh = *pp;
            if (bh->bdev == bdev) {
                *pp = bh->hash_next;
                bh->hash_next = nullptr;
                lru_remove(bh);

                if ((bh->flags & BH_DIRTY) != 0) {
                    cache_dirty_buffers--;
                }
                cache_total_bytes -= bh->size;
                cache_total_buffers--;

                mod::mm::dyn::kmalloc::free(bh->data);
                mod::mm::dyn::kmalloc::free(bh);
            } else {
                pp = &bh->hash_next;
            }
        }
    }

    cache_lock.unlock_irqrestore(irqflags);
}

auto buffer_cache_stats() -> BufferCacheStats {
    uint64_t irqflags = cache_lock.lock_irqsave();
    BufferCacheStats s{};
    s.total_buffers = cache_total_buffers;
    s.dirty_buffers = cache_dirty_buffers;
    s.total_bytes = cache_total_bytes;
    s.max_bytes = cache_max_bytes;
    s.hits = stat_hits;
    s.misses = stat_misses;
    cache_lock.unlock_irqrestore(irqflags);
    return s;
}

}  // namespace ker::vfs
