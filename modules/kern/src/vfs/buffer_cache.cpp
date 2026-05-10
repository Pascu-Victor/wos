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

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <platform/dbg/dbg.hpp>

#include "dev/block_device.hpp"
#include "platform/sys/spinlock.hpp"

namespace ker::vfs {

namespace {

// --- Hash table -----------------------------------------------------------

std::array<BufHead*, BUFFER_CACHE_HASH_BUCKETS> hash_buckets{};
using log = ker::mod::dbg::logger<"buffer_cache">;

auto hash_bucket_at(size_t idx) -> BufHead*& {
    // hash_key() constrains dynamic indexes to the fixed bucket table.
    return hash_buckets[idx];  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
}

auto hash_key(const dev::BlockDevice* bdev, uint64_t block_no) -> size_t {
    // FNV-1a-style mix of device pointer and block number
    auto h = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(bdev));
    h ^= block_no;
    h *= 0x517cc1b727220a95ULL;
    h ^= (h >> 32);
    return static_cast<size_t>(h % BUFFER_CACHE_HASH_BUCKETS);
}

// Returns true if ptr looks like a valid kernel heap or static pointer.
// Used to detect hash chain corruption before it spreads to live entries.
bool is_valid_bufhead_ptr(const BufHead* ptr) {
    auto addr = reinterpret_cast<uintptr_t>(ptr);
    if ((addr & 7U) != 0) {
        return false;
    }  // must be at least 8-byte aligned
    const bool IN_HHDM = (addr >= 0xffff800000000000ULL && addr < 0xffff900000000000ULL);
    const bool IN_STATIC = (addr >= 0xffffffff80000000ULL && addr < 0xffffffffc0000000ULL);
    return IN_HHDM || IN_STATIC;
}

// Dump a single hash bucket chain for diagnostics (called while cache_lock held).
void dump_hash_bucket(size_t idx) {
    log::critical("bucket[%zu] chain:", idx);
    BufHead* bh = hash_bucket_at(idx);
    int depth = 0;
    while (bh != nullptr && depth < 32) {
        if (!is_valid_bufhead_ptr(bh)) {
            log::critical("  [%d] CORRUPT ptr=%p (not a valid kernel address)", depth, reinterpret_cast<void*>(bh));
            break;
        }
        log::critical("  [%d] bh=%p bdev=%p block=%llu refcount=%d hash_next=%p", depth, reinterpret_cast<void*>(bh),
                      reinterpret_cast<void*>(bh->bdev), static_cast<unsigned long long>(bh->block_no),
                      static_cast<int>(bh->refcount.load(std::memory_order_relaxed)), reinterpret_cast<void*>(bh->hash_next));
        bh = bh->hash_next;
        depth++;
    }
}

auto hash_lookup(dev::BlockDevice* bdev, uint64_t block_no) -> BufHead* {
    size_t const IDX = hash_key(bdev, block_no);
    BufHead const* prev = nullptr;
    for (BufHead* bh = hash_bucket_at(IDX); bh != nullptr; bh = bh->hash_next) {
        if (!is_valid_bufhead_ptr(bh)) {
            log::critical("corrupt hash chain in lookup(bdev=%p block=%llu bucket=%zu)", reinterpret_cast<void*>(bdev),
                          static_cast<unsigned long long>(block_no), IDX);
            log::critical("  bad ptr=%p loaded from %s=%p", reinterpret_cast<void*>(bh),
                          (prev != nullptr) ? "prev->hash_next" : "hash_buckets[idx]",
                          (prev != nullptr) ? reinterpret_cast<const void*>(prev) : static_cast<void*>(&hash_bucket_at(IDX)));
            dump_hash_bucket(IDX);
            mod::dbg::panic_handler("buffer_cache: corrupt hash chain detected in lookup");
        }
        if (bh->bdev == bdev && bh->block_no == block_no) {
            return bh;
        }
        prev = bh;
    }
    return nullptr;
}

void hash_insert(BufHead* bh) {
    size_t const IDX = hash_key(bh->bdev, bh->block_no);
    BufHead* old_head = hash_bucket_at(IDX);
    if (old_head != nullptr && !is_valid_bufhead_ptr(old_head)) {
        log::critical("corrupt bucket head in insert(bh=%p bdev=%p block=%llu bucket=%zu) old_head=%p", reinterpret_cast<void*>(bh),
                      reinterpret_cast<void*>(bh->bdev), static_cast<unsigned long long>(bh->block_no), IDX,
                      reinterpret_cast<void*>(old_head));
        mod::dbg::panic_handler("buffer_cache: corrupt bucket head detected in insert");
    }
    bh->hash_next = old_head;
    hash_bucket_at(IDX) = bh;
}

void hash_remove(BufHead* bh) {
    size_t const IDX = hash_key(bh->bdev, bh->block_no);
    // Validate bh->hash_next before propagating — if it is garbage it would
    // corrupt the live predecessor's hash_next and cause the crash we've seen.
    if (bh->hash_next != nullptr && !is_valid_bufhead_ptr(bh->hash_next)) {
        log::critical("corrupt hash_next in remove(bh=%p bdev=%p block=%llu bucket=%zu) hash_next=%p", reinterpret_cast<void*>(bh),
                      reinterpret_cast<void*>(bh->bdev), static_cast<unsigned long long>(bh->block_no), IDX,
                      reinterpret_cast<void*>(bh->hash_next));
        dump_hash_bucket(IDX);
        mod::dbg::panic_handler("buffer_cache: corrupt hash_next detected in remove — not propagating");
    }
    BufHead** pp = &hash_bucket_at(IDX);
    while (*pp != nullptr) {
        if (!is_valid_bufhead_ptr(*pp)) {
            log::critical("corrupt chain entry in remove walk(bh=%p bucket=%zu bad_ptr=%p)", reinterpret_cast<void*>(bh), IDX,
                          reinterpret_cast<void*>(*pp));
            mod::dbg::panic_handler("buffer_cache: corrupt chain entry during remove walk");
        }
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
    if (bh->lru_prev == nullptr || bh->lru_next == nullptr) {
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
    return bh;  // NOLINT(clang-analyzer-cplusplus.NewDelete): lru_remove unlinks victims before free_buffer deletes them.
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

    delete[] bh->data;
    delete bh;
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
                break;  // All buffers are referenced or dirty - cannot evict
            }
        }
        free_buffer(victim);
    }
}

// Allocate a new BufHead with backing data buffer.
auto alloc_buffer(dev::BlockDevice* bdev, uint64_t block_no) -> BufHead* {
    auto* bh = new BufHead{};
    if (bh == nullptr) {
        return nullptr;
    }

    size_t const BLK_SIZE = bdev->block_size;
    auto* data = new uint8_t[BLK_SIZE];
    if (data == nullptr) {
        delete bh;
        return nullptr;
    }

    bh->data = data;
    bh->block_no = block_no;
    bh->bdev = bdev;
    bh->refcount.store(1, std::memory_order_relaxed);
    bh->flags = 0;
    bh->size = BLK_SIZE;
    bh->lru_prev = nullptr;
    bh->lru_next = nullptr;
    bh->hash_next = nullptr;

    cache_total_bytes += BLK_SIZE;
    cache_total_buffers++;

    return bh;
}

// Read the block data from disk into an already-allocated buffer.
auto read_block_from_disk(BufHead* bh) -> int {
    int const RC = dev::block_read(bh->bdev, bh->block_no, 1, bh->data);
    if (RC == 0) {
        bh->flags |= BH_VALID;
    }
    return RC;
}

// Write the buffer data to disk.
auto write_block_to_disk(BufHead* bh) -> int {
    size_t block_count = bh->size / bh->bdev->block_size;
    if (block_count == 0) {
        block_count = 1;
    }
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
    hash_buckets.fill(nullptr);
    lru_init();
    cache_total_bytes = 0;
    cache_total_buffers = 0;
    cache_dirty_buffers = 0;
    stat_hits = 0;
    stat_misses = 0;
    cache_initialized = true;
    log::info("initialized (max %lu bytes)", static_cast<uint64_t>(cache_max_bytes));
}

auto bread(dev::BlockDevice* bdev, uint64_t block_no) -> BufHead* {
    if (bdev == nullptr) {
        return nullptr;
    }

    // Lazily initialize the cache if not already done
    if (!cache_initialized) {
        buffer_cache_init();
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

    // 2. Cache miss - allocate and read from disk
    stat_misses++;

    // Evict if necessary before allocating
    evict_lru();

    bh = alloc_buffer(bdev, block_no);
    if (bh == nullptr) {
        cache_lock.unlock_irqrestore(irqflags);
        log::error("OOM allocating buffer for block %lu", block_no);
        return nullptr;
    }

    // Drop lock during disk I/O (disk read may be slow)
    cache_lock.unlock_irqrestore(irqflags);

    int const RC = read_block_from_disk(bh);
    if (RC != 0) {
        irqflags = cache_lock.lock_irqsave();
        free_buffer(bh);
        cache_lock.unlock_irqrestore(irqflags);
        return nullptr;
    }

    irqflags = cache_lock.lock_irqsave();
    BufHead* existing = hash_lookup(bdev, block_no);
    if (existing != nullptr) {
        existing->refcount.fetch_add(1, std::memory_order_relaxed);
        lru_touch(existing);
        free_buffer(bh);
        cache_lock.unlock_irqrestore(irqflags);
        return existing;
    }

    hash_insert(bh);
    lru_touch(bh);
    cache_lock.unlock_irqrestore(irqflags);

    return bh;
}

auto bread_multi(dev::BlockDevice* bdev, uint64_t block_no, size_t count) -> BufHead* {
    if (bdev == nullptr || count == 0) {
        return nullptr;
    }
    if (count == 1) {
        return bread(bdev, block_no);
    }

    // Lazily initialize the cache if not already done
    if (!cache_initialized) {
        buffer_cache_init();
    }

    size_t const BLK_SIZE = bdev->block_size;
    size_t const TOTAL_SIZE = BLK_SIZE * count;

    // Multi-block buffers are cached by (bdev, block_no) like single-block
    // buffers.  This is critical for correctness: within a transaction,
    // multiple reads of the same XFS block (e.g. AG block 0) must return
    // the same buffer so that in-transaction writes are visible to subsequent
    // reads and the transaction system can merge dirty regions correctly.
    uint64_t irqflags = cache_lock.lock_irqsave();

    BufHead* bh = hash_lookup(bdev, block_no);
    if (bh != nullptr) {
        // Verify size matches (should always match for fixed XFS block size)
        if (bh->size == TOTAL_SIZE) {
            bh->refcount.fetch_add(1, std::memory_order_relaxed);
            lru_touch(bh);
            stat_hits++;
            cache_lock.unlock_irqrestore(irqflags);
            return bh;
        }
        // Size mismatch - fall through to allocate fresh (shouldn't happen)
    }

    stat_misses++;
    evict_lru();

    bh = new BufHead{};
    if (bh == nullptr) {
        cache_lock.unlock_irqrestore(irqflags);
        return nullptr;
    }
    auto* data = new uint8_t[TOTAL_SIZE];
    if (data == nullptr) {
        delete bh;
        cache_lock.unlock_irqrestore(irqflags);
        return nullptr;
    }

    bh->data = data;
    bh->block_no = block_no;
    bh->bdev = bdev;
    bh->refcount.store(1, std::memory_order_relaxed);
    bh->flags = 0;
    bh->size = TOTAL_SIZE;
    bh->lru_prev = nullptr;
    bh->lru_next = nullptr;
    bh->hash_next = nullptr;

    cache_total_bytes += TOTAL_SIZE;
    cache_total_buffers++;

    cache_lock.unlock_irqrestore(irqflags);

    // Read from disk outside the lock
    int const RC = dev::block_read(bdev, block_no, count, data);
    if (RC != 0) {
        irqflags = cache_lock.lock_irqsave();
        free_buffer(bh);
        cache_lock.unlock_irqrestore(irqflags);
        return nullptr;
    }
    bh->flags |= BH_VALID;

    irqflags = cache_lock.lock_irqsave();
    BufHead* existing = hash_lookup(bdev, block_no);
    if (existing != nullptr && existing->size == TOTAL_SIZE) {
        existing->refcount.fetch_add(1, std::memory_order_relaxed);
        lru_touch(existing);
        free_buffer(bh);
        cache_lock.unlock_irqrestore(irqflags);
        return existing;
    }

    hash_insert(bh);
    lru_touch(bh);
    cache_lock.unlock_irqrestore(irqflags);

    return bh;
}

void brelse(BufHead* bh) {
    if (bh == nullptr) {
        return;
    }

    int32_t const PREV = bh->refcount.fetch_sub(1, std::memory_order_acq_rel);
    if (PREV <= 0) {
        bh->refcount.store(0, std::memory_order_relaxed);
    }
}

auto bwrite(BufHead* bh) -> int {
    if (bh == nullptr) {
        return -1;
    }

    int const RC = write_block_to_disk(bh);
    if (RC == 0) {
        uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
        if ((bh->flags & BH_DIRTY) != 0) {
            bh->flags &= ~BH_DIRTY;
            cache_dirty_buffers--;
        }
        cache_lock.unlock_irqrestore(IRQFLAGS);
    }
    return RC;
}

void bdirty(BufHead* bh) {
    if (bh == nullptr) {
        return;
    }

    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    if ((bh->flags & BH_DIRTY) == 0) {
        bh->flags |= BH_DIRTY;
        cache_dirty_buffers++;
    }
    cache_lock.unlock_irqrestore(IRQFLAGS);
}

auto bget(dev::BlockDevice* bdev, uint64_t block_no) -> BufHead* {
    if (bdev == nullptr) {
        return nullptr;
    }

    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();

    // If the block is already cached, return it (caller will overwrite).
    BufHead* bh = hash_lookup(bdev, block_no);
    if (bh != nullptr) {
        bh->refcount.fetch_add(1, std::memory_order_relaxed);
        lru_touch(bh);
        cache_lock.unlock_irqrestore(IRQFLAGS);
        return bh;
    }

    // Not cached - allocate a new buffer and insert without reading.
    evict_lru();
    bh = alloc_buffer(bdev, block_no);
    if (bh == nullptr) {
        cache_lock.unlock_irqrestore(IRQFLAGS);
        return nullptr;
    }
    bh->flags = BH_VALID;  // Mark valid without disk read
    hash_insert(bh);
    lru_touch(bh);
    cache_lock.unlock_irqrestore(IRQFLAGS);
    return bh;
}

auto bget_multi(dev::BlockDevice* bdev, uint64_t block_no, size_t count) -> BufHead* {
    if (bdev == nullptr || count == 0) {
        return nullptr;
    }
    if (count == 1) {
        return bget(bdev, block_no);
    }

    size_t const BLK_SIZE = bdev->block_size;
    size_t const TOTAL_SIZE = BLK_SIZE * count;

    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();

    // If already cached, return it (caller will overwrite contents).
    BufHead* bh = hash_lookup(bdev, block_no);
    if (bh != nullptr) {
        if (bh->size == TOTAL_SIZE) {
            bh->refcount.fetch_add(1, std::memory_order_relaxed);
            lru_touch(bh);
            cache_lock.unlock_irqrestore(IRQFLAGS);
            return bh;
        }
        // Size mismatch - fall through to allocate fresh (shouldn't happen)
    }

    evict_lru();

    bh = new BufHead{};
    if (bh == nullptr) {
        cache_lock.unlock_irqrestore(IRQFLAGS);
        return nullptr;
    }
    auto* data = new uint8_t[TOTAL_SIZE];
    if (data == nullptr) {
        delete bh;
        cache_lock.unlock_irqrestore(IRQFLAGS);
        return nullptr;
    }

    bh->data = data;
    bh->block_no = block_no;
    bh->bdev = bdev;
    bh->refcount.store(1, std::memory_order_relaxed);
    bh->flags = BH_VALID;
    bh->size = TOTAL_SIZE;
    bh->lru_prev = nullptr;
    bh->lru_next = nullptr;
    bh->hash_next = nullptr;

    cache_total_bytes += TOTAL_SIZE;
    cache_total_buffers++;

    hash_insert(bh);
    lru_touch(bh);
    cache_lock.unlock_irqrestore(IRQFLAGS);

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
    std::array<BufHead*, BATCH_SIZE> batch{};
    size_t batch_count = 0;
    bool more = true;

    while (more) {
        batch_count = 0;
        more = false;

        for (auto* bucket_head : hash_buckets) {
            for (BufHead* bh = bucket_head; bh != nullptr && batch_count < BATCH_SIZE; bh = bh->hash_next) {
                if (bh->bdev == bdev && (bh->flags & BH_DIRTY) != 0) {
                    bh->refcount.fetch_add(1, std::memory_order_relaxed);
                    batch[batch_count++] = bh;  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
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
            auto* bh = batch[i];  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
            int const RC = bwrite(bh);
            if (RC != 0) {
                result = RC;
            }
            brelse(bh);
        }

        if (more) {
            irqflags = cache_lock.lock_irqsave();
        }
    }

    // Flush the device if it supports it
    if (bdev->flush != nullptr) {
        int const RC = dev::block_flush(bdev);
        if (RC != 0) {
            result = RC;
        }
    }

    return result;
}

void invalidate_bdev(dev::BlockDevice* bdev) {
    if (bdev == nullptr) {
        return;
    }

    // Lazily initialize the cache if not already done
    if (!cache_initialized) {
        buffer_cache_init();
    }

    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();

    // Walk all hash buckets; remove buffers belonging to this device.
    for (auto& hash_bucket : hash_buckets) {
        BufHead** pp = &hash_bucket;
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

                delete[] bh->data;
                delete bh;
            } else {
                pp = &bh->hash_next;
            }
        }
    }

    cache_lock.unlock_irqrestore(IRQFLAGS);
}

auto buffer_cache_stats() -> BufferCacheStats {
    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    BufferCacheStats s{};
    s.total_buffers = cache_total_buffers;
    s.dirty_buffers = cache_dirty_buffers;
    s.total_bytes = cache_total_bytes;
    s.max_bytes = cache_max_bytes;
    s.hits = stat_hits;
    s.misses = stat_misses;
    cache_lock.unlock_irqrestore(IRQFLAGS);
    return s;
}

}  // namespace ker::vfs
