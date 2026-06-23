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
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>

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

auto hash_key(const dev::BlockDevice* bdev, uint64_t block_no, size_t size) -> size_t {
    // FNV-1a-style mix of device pointer, start block, and span.
    auto h = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(bdev));
    h ^= block_no;
    h *= 0x517cc1b727220a95ULL;
    h ^= static_cast<uint64_t>(size);
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
#ifdef WOS_HOST_TEST
    return true;
#else
    const bool IN_HHDM = (addr >= 0xffff800000000000ULL && addr < 0xffff900000000000ULL);
    const bool IN_STATIC = (addr >= 0xffffffff80000000ULL && addr < 0xffffffffc0000000ULL);
    return IN_HHDM || IN_STATIC;
#endif
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

auto hash_lookup(dev::BlockDevice* bdev, uint64_t block_no, size_t size) -> BufHead* {
    size_t const IDX = hash_key(bdev, block_no, size);
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
        if (bh->bdev == bdev && bh->block_no == block_no && bh->size == size) {
            return bh;
        }
        prev = bh;
    }
    return nullptr;
}

void hash_insert(BufHead* bh) {
    size_t const IDX = hash_key(bh->bdev, bh->block_no, bh->size);
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
    size_t const IDX = hash_key(bh->bdev, bh->block_no, bh->size);
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
size_t cache_dirty_bytes = 0;

uint64_t stat_hits = 0;
uint64_t stat_misses = 0;
std::atomic<uint64_t> next_writeback_epoch{1};
std::atomic<uint64_t> next_dirty_epoch{1};
std::atomic<bool> dirty_throttle_active{false};

bool cache_initialized = false;

constexpr size_t DIRTY_THROTTLE_HIGH_MULTIPLIER = 4;
constexpr size_t HOT_EVICT_SCAN_BUDGET = 512;
constexpr size_t HOT_EVICT_MAX_VICTIMS = 64;

struct WritebackSnapshot {
    uint8_t* data = nullptr;
    uint32_t flags = 0;
};

auto perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp op) -> uint64_t {
    return ker::mod::perf::is_wki_scope_recording_enabled(ker::mod::perf::WkiPerfScope::LOCAL_XFS, static_cast<uint8_t>(op))
               ? ker::mod::time::get_us()
               : 0;
}

auto perf_xfs_started_us() -> uint64_t { return ker::mod::perf::is_local_xfs_recording_enabled() ? ker::mod::time::get_us() : 0; }

auto dirty_throttle_high_bytes_locked() -> size_t {
    if (cache_max_bytes > SIZE_MAX / DIRTY_THROTTLE_HIGH_MULTIPLIER) {
        return SIZE_MAX;
    }
    return cache_max_bytes * DIRTY_THROTTLE_HIGH_MULTIPLIER;
}

auto perf_elapsed_since_us(uint64_t started_us) -> uint32_t {
    uint64_t const NOW_US = ker::mod::time::get_us();
    uint64_t const ELAPSED_US = NOW_US >= started_us ? NOW_US - started_us : 0;
    return ELAPSED_US > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(ELAPSED_US);
}

void perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp op, uint64_t started_us, int32_t status, uint64_t bytes) {
    if (started_us == 0) {
        return;
    }
    ker::mod::perf::record_local_xfs_summary(op, status, perf_elapsed_since_us(started_us), true, bytes);
}

void perf_record_xfs_count(ker::mod::perf::WkiPerfLocalXfsOp op, uint64_t bytes = 0, int32_t status = 0) {
    if (!ker::mod::perf::is_wki_scope_recording_enabled(ker::mod::perf::WkiPerfScope::LOCAL_XFS, static_cast<uint8_t>(op))) {
        return;
    }
    ker::mod::perf::record_local_xfs_summary(op, status, 0, false, bytes);
}

auto allocate_buffer_data(size_t size, uint32_t& flags) -> uint8_t* {
    flags &= ~BH_DATA_PAGE_ALLOC;
    if (size >= ker::mod::mm::paging::PAGE_SIZE && (size % ker::mod::mm::paging::PAGE_SIZE) == 0) {
        auto* page_data = ker::mod::mm::phys::page_alloc(size, "buffer_cache");
        if (page_data == nullptr) {
            return nullptr;
        }
        flags |= BH_DATA_PAGE_ALLOC;
        return static_cast<uint8_t*>(page_data);
    }
    return new uint8_t[size];
}

void free_data_buffer(uint8_t* data, uint32_t flags) {
    if (data == nullptr) {
        return;
    }
    if ((flags & BH_DATA_PAGE_ALLOC) != 0) {
        ker::mod::mm::phys::page_free(data);
    } else {
        delete[] data;
    }
}

void free_buffer_data(BufHead* bh) {
    if (bh == nullptr || bh->data == nullptr) {
        return;
    }
    free_data_buffer(bh->data, bh->flags);
    bh->data = nullptr;
    bh->flags &= ~BH_DATA_PAGE_ALLOC;
}

auto make_writeback_snapshot(const BufHead* bh) -> WritebackSnapshot {
    WritebackSnapshot snapshot{};
    snapshot.data = allocate_buffer_data(bh->size, snapshot.flags);
    if (snapshot.data == nullptr) {
        return snapshot;
    }
    memcpy(snapshot.data, bh->data, bh->size);
    return snapshot;
}

void free_writeback_snapshot(WritebackSnapshot& snapshot) {
    free_data_buffer(snapshot.data, snapshot.flags);
    snapshot.data = nullptr;
    snapshot.flags = 0;
}

void free_unlinked_buffer(BufHead* bh) {
    if ((bh->flags & BH_DIRTY) != 0) {
        cache_dirty_buffers--;
        cache_dirty_bytes -= bh->size;
    }
    cache_total_bytes -= bh->size;
    cache_total_buffers--;

    free_buffer_data(bh);
    delete bh;
}

// Free a buffer's resources completely (removes from hash + LRU).
void free_buffer(BufHead* bh) {
    hash_remove(bh);
    lru_remove(bh);

    free_unlinked_buffer(bh);
}

auto is_reclaimable_clean_buffer(const BufHead* bh) -> bool {
    if (bh == nullptr || bh == &lru_sentinel) {
        return false;
    }
    uint32_t constexpr BLOCKING_FLAGS = BH_DIRTY | BH_LOCKED | BH_WRITEBACK;
    return bh->refcount.load(std::memory_order_relaxed) == 0 && (bh->flags & BLOCKING_FLAGS) == 0;
}

auto find_reclaimable_lru_buffer(size_t scan_budget = SIZE_MAX) -> BufHead* {
    size_t scanned = 0;
    for (BufHead* cur = lru_tail(); cur != nullptr && cur != &lru_sentinel; cur = cur->lru_prev) {
        if (scanned++ >= scan_budget) {
            break;
        }
        if (is_reclaimable_clean_buffer(cur)) {  // NOLINT(clang-analyzer-cplusplus.NewDelete): victims are unlinked before delete.
            return cur;
        }
    }
    return nullptr;
}

// Try to evict unreferenced clean buffers until we are below the max cache size.
void evict_lru() {
    size_t victims = 0;
    while (cache_total_bytes > cache_max_bytes && victims < HOT_EVICT_MAX_VICTIMS) {
        // This runs under cache_lock in the buffer allocation hot path. When
        // the cache is dirty-heavy, an exhaustive clean-victim search can turn
        // every miss into a full LRU walk.
        BufHead* victim = find_reclaimable_lru_buffer(HOT_EVICT_SCAN_BUDGET);
        if (victim == nullptr) {
            break;  // No clean victim found within the hot-path scan budget.
        }
        free_buffer(victim);
        victims++;
    }
}

auto alloc_buffer_with_size(dev::BlockDevice* bdev, uint64_t block_no, size_t size, uint32_t initial_flags) -> BufHead* {
    auto* bh = new BufHead{};
    if (bh == nullptr) {
        return nullptr;
    }

    uint32_t data_flags = 0;
    auto* data = allocate_buffer_data(size, data_flags);
    if (data == nullptr) {
        delete bh;
        return nullptr;
    }

    bh->data = data;
    bh->block_no = block_no;
    bh->bdev = bdev;
    bh->refcount.store(1, std::memory_order_relaxed);
    bh->flags = initial_flags | data_flags;
    bh->size = size;
    bh->writeback_epoch = 0;
    bh->dirty_epoch = 0;
    bh->writeback_dirty_epoch = 0;
    bh->lru_prev = nullptr;
    bh->lru_next = nullptr;
    bh->hash_next = nullptr;

    cache_total_bytes += size;
    cache_total_buffers++;

    return bh;
}

// Allocate a new BufHead with backing data buffer.
auto alloc_buffer(dev::BlockDevice* bdev, uint64_t block_no) -> BufHead* {
    return alloc_buffer_with_size(bdev, block_no, bdev->block_size, 0);
}

auto read_blocks_with_retry(dev::BlockDevice* bdev, uint64_t block_no, size_t count, uint8_t* data) -> int {
    constexpr int MAX_ATTEMPTS = 3;
    uint64_t const STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::BUF_DISK_READ);
    int rc = -EIO;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; ++attempt) {
        int const RC = dev::block_read(bdev, block_no, count, data);
        if (RC == 0) {
            rc = 0;
            break;
        }
        if (attempt < MAX_ATTEMPTS) {
            log::warn("transient read failure on %s block=%llu count=%zu rc=%d attempt=%d/%d", bdev->name.data(),
                      static_cast<unsigned long long>(block_no), count, RC, attempt, MAX_ATTEMPTS);
            ker::mod::sched::kern_yield();
            continue;
        }
        log::warn("read failed on %s block=%llu count=%zu rc=%d after %d attempts", bdev->name.data(),
                  static_cast<unsigned long long>(block_no), count, RC, MAX_ATTEMPTS);
        rc = RC;
    }
    perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_DISK_READ, STARTED_US, rc,
                          count * static_cast<uint64_t>(bdev->block_size));
    return rc;
}

// Read the block data from disk into an already-allocated buffer.
auto read_block_from_disk(BufHead* bh) -> int {
    int const RC = read_blocks_with_retry(bh->bdev, bh->block_no, 1, bh->data);
    if (RC == 0) {
        bh->flags |= BH_VALID;
    }
    return RC;
}

// Write a stable buffer-data snapshot to disk.
auto write_block_to_disk(BufHead* bh, const uint8_t* data) -> int {
    size_t block_count = bh->size / bh->bdev->block_size;
    if (block_count == 0) {
        block_count = 1;
    }
    uint64_t const STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::BUF_DISK_WRITE);
    int const RC = dev::block_write(bh->bdev, bh->block_no, block_count, data);
    perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_DISK_WRITE, STARTED_US, RC, bh->size);
    return RC;
}

auto buffer_block_count(const BufHead* bh) -> size_t {
    size_t block_count = bh->size / bh->bdev->block_size;
    if (block_count == 0) {
        block_count = 1;
    }
    return block_count;
}

auto block_range_last_block(uint64_t block_no, size_t count) -> uint64_t {
    if (count == 0) {
        return block_no;
    }
    size_t const LAST_OFFSET = count - 1;
    if (LAST_OFFSET > static_cast<size_t>(UINT64_MAX - block_no)) {
        return UINT64_MAX;
    }
    return block_no + static_cast<uint64_t>(LAST_OFFSET);
}

auto block_ranges_overlap(uint64_t a_block_no, size_t a_count, uint64_t b_block_no, size_t b_count) -> bool {
    if (a_count == 0 || b_count == 0) {
        return false;
    }
    uint64_t const A_LAST = block_range_last_block(a_block_no, a_count);
    uint64_t const B_LAST = block_range_last_block(b_block_no, b_count);
    return a_block_no <= B_LAST && b_block_no <= A_LAST;
}

auto buffers_overlap(const BufHead* a, const BufHead* b) -> bool {
    if (a == nullptr || b == nullptr || a == b || a->bdev != b->bdev) {
        return false;
    }
    return block_ranges_overlap(a->block_no, buffer_block_count(a), b->block_no, buffer_block_count(b));
}

auto buffer_overlaps_range(const BufHead* bh, dev::BlockDevice* bdev, uint64_t block_no, size_t count) -> bool {
    if (bh == nullptr || bh->bdev != bdev) {
        return false;
    }
    return block_ranges_overlap(bh->block_no, buffer_block_count(bh), block_no, count);
}

auto allocate_dirty_epoch() -> uint64_t {
    uint64_t epoch = next_dirty_epoch.fetch_add(1, std::memory_order_relaxed);
    if (epoch == 0) {
        epoch = next_dirty_epoch.fetch_add(1, std::memory_order_relaxed);
    }
    return epoch;
}

auto allocate_writeback_epoch() -> uint64_t {
    uint64_t epoch = next_writeback_epoch.fetch_add(1, std::memory_order_relaxed);
    if (epoch == 0) {
        epoch = next_writeback_epoch.fetch_add(1, std::memory_order_relaxed);
    }
    return epoch;
}

void mark_buffer_writeback(BufHead* bh, uint64_t epoch) {
    bh->flags |= BH_WRITEBACK;
    bh->writeback_epoch = epoch;
    bh->writeback_dirty_epoch = bh->dirty_epoch;
}

void clear_buffer_writeback(BufHead* bh) {
    bh->flags &= ~BH_WRITEBACK;
    bh->writeback_epoch = 0;
    bh->writeback_dirty_epoch = 0;
}

auto owns_writeback_epoch(const BufHead* bh, uint64_t epoch) -> bool {
    return epoch != 0 && (bh->flags & BH_WRITEBACK) != 0 && bh->writeback_epoch == epoch;
}

auto mark_buffer_dirty_locked(BufHead* bh) -> bool {
    bh->dirty_epoch = allocate_dirty_epoch();
    if ((bh->flags & BH_DIRTY) != 0) {
        return false;
    }

    bh->flags |= BH_DIRTY;
    cache_dirty_buffers++;
    cache_dirty_bytes += bh->size;
    return true;
}

auto has_older_overlapping_dirty_buffer_locked(const BufHead* target) -> bool {
    for (auto* bucket_head : hash_buckets) {
        for (BufHead* bh = bucket_head; bh != nullptr; bh = bh->hash_next) {
            if ((bh->flags & BH_DIRTY) == 0 || !buffers_overlap(target, bh)) {
                continue;
            }
            if (bh->dirty_epoch < target->dirty_epoch) {
                return true;
            }
        }
    }
    return false;
}

void clear_writeback_for_bdev(dev::BlockDevice* bdev, uint64_t epoch) {
    for (auto* bucket_head : hash_buckets) {
        for (BufHead* bh = bucket_head; bh != nullptr; bh = bh->hash_next) {
            if (bh->bdev == bdev && bh->writeback_epoch == epoch) {
                clear_buffer_writeback(bh);
            }
        }
    }
}

void clear_writeback_for_bdev_range(dev::BlockDevice* bdev, uint64_t block_no, size_t count, uint64_t epoch) {
    for (auto* bucket_head : hash_buckets) {
        for (BufHead* bh = bucket_head; bh != nullptr; bh = bh->hash_next) {
            if (bh->writeback_epoch == epoch && buffer_overlaps_range(bh, bdev, block_no, count)) {
                clear_buffer_writeback(bh);
            }
        }
    }
}

auto write_buffer_snapshot_for_epoch(BufHead* bh, uint64_t writeback_dirty_epoch, uint64_t owned_writeback_epoch) -> int {
    WritebackSnapshot snapshot = make_writeback_snapshot(bh);
    if (snapshot.data == nullptr) {
        uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
        if (owns_writeback_epoch(bh, owned_writeback_epoch)) {
            clear_buffer_writeback(bh);
        }
        cache_lock.unlock_irqrestore(IRQFLAGS);
        return -ENOMEM;
    }

    int const RC = write_block_to_disk(bh, snapshot.data);
    free_writeback_snapshot(snapshot);
    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    if (RC == 0 && (bh->flags & BH_DIRTY) != 0 && bh->dirty_epoch == writeback_dirty_epoch) {
        bh->flags &= ~BH_DIRTY;
        cache_dirty_buffers--;
        cache_dirty_bytes -= bh->size;
    }
    if (owns_writeback_epoch(bh, owned_writeback_epoch)) {
        clear_buffer_writeback(bh);
    }
    cache_lock.unlock_irqrestore(IRQFLAGS);
    return RC;
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
    cache_dirty_bytes = 0;
    stat_hits = 0;
    stat_misses = 0;
    cache_initialized = true;
    log::info("initialized (max %lu bytes)", static_cast<uint64_t>(cache_max_bytes));
}

auto bread(dev::BlockDevice* bdev, uint64_t block_no) -> BufHead* {
    if (bdev == nullptr) {
        return nullptr;
    }
    uint64_t const PERF_STARTED_US = perf_xfs_started_us();

    // Lazily initialize the cache if not already done
    if (!cache_initialized) {
        buffer_cache_init();
    }

    uint64_t irqflags = cache_lock.lock_irqsave();

    // 1. Lookup in cache
    BufHead* bh = hash_lookup(bdev, block_no, bdev->block_size);
    if (bh != nullptr) {
        bh->refcount.fetch_add(1, std::memory_order_relaxed);
        lru_touch(bh);
        stat_hits++;
        cache_lock.unlock_irqrestore(irqflags);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_READ_HIT, PERF_STARTED_US, 0, bdev->block_size);
        return bh;
    }

    // 2. Cache miss - allocate and read from disk
    stat_misses++;

    // Evict if necessary before allocating
    evict_lru();

    uint64_t const PERF_ALLOC_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC);
    bh = alloc_buffer(bdev, block_no);
    uint32_t const PERF_ALLOC_US = PERF_ALLOC_STARTED_US != 0 ? perf_elapsed_since_us(PERF_ALLOC_STARTED_US) : 0;
    if (bh == nullptr) {
        cache_lock.unlock_irqrestore(irqflags);
        if (PERF_ALLOC_STARTED_US != 0) {
            ker::mod::perf::record_local_xfs_summary(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC, -ENOMEM, PERF_ALLOC_US, true, 0);
        }
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_READ_MISS, PERF_STARTED_US, -ENOMEM, 0);
        log::error("OOM allocating buffer for block %lu", block_no);
        return nullptr;
    }

    // Drop lock during disk I/O (disk read may be slow)
    cache_lock.unlock_irqrestore(irqflags);
    if (PERF_ALLOC_STARTED_US != 0) {
        ker::mod::perf::record_local_xfs_summary(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC, 0, PERF_ALLOC_US, true, bdev->block_size);
    }
    perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_READ_MISS, PERF_STARTED_US, 0, bdev->block_size);

    int const RC = read_block_from_disk(bh);
    if (RC != 0) {
        irqflags = cache_lock.lock_irqsave();
        free_buffer(bh);
        cache_lock.unlock_irqrestore(irqflags);
        return nullptr;
    }

    irqflags = cache_lock.lock_irqsave();
    BufHead* existing = hash_lookup(bdev, block_no, bdev->block_size);
    if (existing != nullptr) {
        existing->refcount.fetch_add(1, std::memory_order_relaxed);
        lru_touch(existing);
        free_buffer(bh);
        cache_lock.unlock_irqrestore(irqflags);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_READ_HIT, PERF_STARTED_US, 0, bdev->block_size);
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
    uint64_t const PERF_STARTED_US = perf_xfs_started_us();

    // Lazily initialize the cache if not already done
    if (!cache_initialized) {
        buffer_cache_init();
    }

    size_t const BLK_SIZE = bdev->block_size;
    size_t const TOTAL_SIZE = BLK_SIZE * count;

    // Multi-block buffers are cached by their exact span. This avoids aliasing
    // a single-block and multi-block view of the same starting device block.
    uint64_t irqflags = cache_lock.lock_irqsave();

    BufHead* bh = hash_lookup(bdev, block_no, TOTAL_SIZE);
    if (bh != nullptr) {
        bh->refcount.fetch_add(1, std::memory_order_relaxed);
        lru_touch(bh);
        stat_hits++;
        cache_lock.unlock_irqrestore(irqflags);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_READ_HIT, PERF_STARTED_US, 0, TOTAL_SIZE);
        return bh;
    }

    stat_misses++;
    evict_lru();

    uint64_t const PERF_ALLOC_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC);
    bh = alloc_buffer_with_size(bdev, block_no, TOTAL_SIZE, 0);
    if (bh == nullptr) {
        cache_lock.unlock_irqrestore(irqflags);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC, PERF_ALLOC_STARTED_US, -ENOMEM, 0);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_READ_MISS, PERF_STARTED_US, -ENOMEM, 0);
        return nullptr;
    }
    uint32_t const PERF_ALLOC_US = PERF_ALLOC_STARTED_US != 0 ? perf_elapsed_since_us(PERF_ALLOC_STARTED_US) : 0;

    cache_lock.unlock_irqrestore(irqflags);
    if (PERF_ALLOC_STARTED_US != 0) {
        ker::mod::perf::record_local_xfs_summary(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC, 0, PERF_ALLOC_US, true, TOTAL_SIZE);
    }
    perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_READ_MISS, PERF_STARTED_US, 0, TOTAL_SIZE);

    // Read from disk outside the lock
    int const RC = read_blocks_with_retry(bdev, block_no, count, bh->data);
    if (RC != 0) {
        irqflags = cache_lock.lock_irqsave();
        free_buffer(bh);
        cache_lock.unlock_irqrestore(irqflags);
        return nullptr;
    }
    bh->flags |= BH_VALID;

    irqflags = cache_lock.lock_irqsave();
    BufHead* existing = hash_lookup(bdev, block_no, TOTAL_SIZE);
    if (existing != nullptr) {
        existing->refcount.fetch_add(1, std::memory_order_relaxed);
        lru_touch(existing);
        free_buffer(bh);
        cache_lock.unlock_irqrestore(irqflags);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_READ_HIT, PERF_STARTED_US, 0, TOTAL_SIZE);
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
        return -EINVAL;
    }

    uint64_t writeback_dirty_epoch = 0;
    uint64_t owned_writeback_epoch = 0;
    {
        uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
        mark_buffer_dirty_locked(bh);
        writeback_dirty_epoch = bh->dirty_epoch;
        if ((bh->flags & BH_WRITEBACK) == 0) {
            owned_writeback_epoch = allocate_writeback_epoch();
            mark_buffer_writeback(bh, owned_writeback_epoch);
        }
        cache_lock.unlock_irqrestore(IRQFLAGS);
    }

    return write_buffer_snapshot_for_epoch(bh, writeback_dirty_epoch, owned_writeback_epoch);
}

void bdirty(BufHead* bh) {
    if (bh == nullptr) {
        return;
    }

    uint64_t const PERF_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::BUF_DIRTY);
    bool became_dirty = false;
    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    became_dirty = mark_buffer_dirty_locked(bh);
    cache_lock.unlock_irqrestore(IRQFLAGS);
    if (became_dirty) {
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_DIRTY, PERF_STARTED_US, 0, bh->size);
    }
}

auto bget(dev::BlockDevice* bdev, uint64_t block_no) -> BufHead* {
    if (bdev == nullptr) {
        return nullptr;
    }
    uint64_t const PERF_STARTED_US = perf_xfs_started_us();

    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();

    // If the block is already cached, return it (caller will overwrite).
    BufHead* bh = hash_lookup(bdev, block_no, bdev->block_size);
    if (bh != nullptr) {
        bh->refcount.fetch_add(1, std::memory_order_relaxed);
        lru_touch(bh);
        cache_lock.unlock_irqrestore(IRQFLAGS);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_GET_HIT, PERF_STARTED_US, 0, bdev->block_size);
        return bh;
    }

    // Not cached - allocate a new buffer and insert without reading.
    evict_lru();
    uint64_t const PERF_ALLOC_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC);
    bh = alloc_buffer(bdev, block_no);
    uint32_t const PERF_ALLOC_US = PERF_ALLOC_STARTED_US != 0 ? perf_elapsed_since_us(PERF_ALLOC_STARTED_US) : 0;
    if (bh == nullptr) {
        cache_lock.unlock_irqrestore(IRQFLAGS);
        if (PERF_ALLOC_STARTED_US != 0) {
            ker::mod::perf::record_local_xfs_summary(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC, -ENOMEM, PERF_ALLOC_US, true, 0);
        }
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_GET_MISS, PERF_STARTED_US, -ENOMEM, 0);
        return nullptr;
    }
    bh->flags |= BH_VALID;  // Mark valid without disk read
    hash_insert(bh);
    lru_touch(bh);
    cache_lock.unlock_irqrestore(IRQFLAGS);
    if (PERF_ALLOC_STARTED_US != 0) {
        ker::mod::perf::record_local_xfs_summary(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC, 0, PERF_ALLOC_US, true, bdev->block_size);
    }
    perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_GET_MISS, PERF_STARTED_US, 0, bdev->block_size);
    return bh;
}

auto bget_multi(dev::BlockDevice* bdev, uint64_t block_no, size_t count) -> BufHead* {
    if (bdev == nullptr || count == 0) {
        return nullptr;
    }
    if (count == 1) {
        return bget(bdev, block_no);
    }
    uint64_t const PERF_STARTED_US = perf_xfs_started_us();

    size_t const BLK_SIZE = bdev->block_size;
    size_t const TOTAL_SIZE = BLK_SIZE * count;

    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();

    // If already cached, return it (caller will overwrite contents).
    BufHead* bh = hash_lookup(bdev, block_no, TOTAL_SIZE);
    if (bh != nullptr) {
        bh->refcount.fetch_add(1, std::memory_order_relaxed);
        lru_touch(bh);
        cache_lock.unlock_irqrestore(IRQFLAGS);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_GET_HIT, PERF_STARTED_US, 0, TOTAL_SIZE);
        return bh;
    }

    evict_lru();

    uint64_t const PERF_ALLOC_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC);
    bh = alloc_buffer_with_size(bdev, block_no, TOTAL_SIZE, BH_VALID);
    if (bh == nullptr) {
        cache_lock.unlock_irqrestore(IRQFLAGS);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC, PERF_ALLOC_STARTED_US, -ENOMEM, 0);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_GET_MISS, PERF_STARTED_US, -ENOMEM, 0);
        return nullptr;
    }
    uint32_t const PERF_ALLOC_US = PERF_ALLOC_STARTED_US != 0 ? perf_elapsed_since_us(PERF_ALLOC_STARTED_US) : 0;

    hash_insert(bh);
    lru_touch(bh);
    cache_lock.unlock_irqrestore(IRQFLAGS);
    if (PERF_ALLOC_STARTED_US != 0) {
        ker::mod::perf::record_local_xfs_summary(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC, 0, PERF_ALLOC_US, true, TOTAL_SIZE);
    }
    perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_GET_MISS, PERF_STARTED_US, 0, TOTAL_SIZE);

    return bh;
}

auto sync_blockdev(dev::BlockDevice* bdev) -> int {
    if (bdev == nullptr) {
        return -EINVAL;
    }

    uint64_t const STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::SYNC_BLOCKDEV);
    int result = 0;

    uint64_t irqflags = cache_lock.lock_irqsave();

    // Walk all hash buckets collecting dirty buffers for this device.
    // We bump refcount so they stay alive while we write with the lock dropped.
    // Use a fixed-size batch to avoid dynamic allocation.
    constexpr size_t BATCH_SIZE = 64;
    std::array<BufHead*, BATCH_SIZE> batch{};
    std::array<uint64_t, BATCH_SIZE> batch_dirty_epochs{};
    size_t batch_count = 0;
    bool more = true;
    uint64_t const WRITEBACK_EPOCH = allocate_writeback_epoch();

    while (more) {
        batch_count = 0;
        more = false;
        bool saw_foreign_writeback = false;

        for (auto* bucket_head : hash_buckets) {
            for (BufHead* bh = bucket_head; bh != nullptr && batch_count < BATCH_SIZE; bh = bh->hash_next) {
                if (bh->bdev != bdev || (bh->flags & BH_DIRTY) == 0) {
                    continue;
                }
                if ((bh->flags & BH_WRITEBACK) != 0) {
                    saw_foreign_writeback = saw_foreign_writeback || bh->writeback_epoch != WRITEBACK_EPOCH;
                    continue;
                }
                if (has_older_overlapping_dirty_buffer_locked(bh)) {
                    more = true;
                    continue;
                }
                {
                    mark_buffer_writeback(bh, WRITEBACK_EPOCH);
                    bh->refcount.fetch_add(1, std::memory_order_relaxed);
                    batch_dirty_epochs[batch_count] =
                        bh->writeback_dirty_epoch;  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
                    batch[batch_count++] = bh;      // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
                }
            }
            // If the batch is full, there may be more dirty buffers
            if (batch_count >= BATCH_SIZE) {
                more = true;
                break;
            }
        }
        if (saw_foreign_writeback) {
            more = true;
        }

        cache_lock.unlock_irqrestore(irqflags);

        // Write each dirty buffer with lock released
        for (size_t i = 0; i < batch_count; i++) {
            auto* bh = batch[i];  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
            int const RC = write_buffer_snapshot_for_epoch(bh, batch_dirty_epochs[i],
                                                           0);  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
            if (RC != 0) {
                result = RC;
            }
            brelse(bh);
        }

        if (batch_count == 0 && saw_foreign_writeback) {
            ker::mod::sched::kern_yield();
        }
        if (batch_count == 0 && !saw_foreign_writeback) {
            more = false;
        }

        if (more) {
            irqflags = cache_lock.lock_irqsave();
        }
    }

    irqflags = cache_lock.lock_irqsave();
    clear_writeback_for_bdev(bdev, WRITEBACK_EPOCH);
    cache_lock.unlock_irqrestore(irqflags);

    // Flush the device if it supports it
    if (bdev->flush != nullptr) {
        uint64_t const FLUSH_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::BUF_FLUSH);
        int const RC = dev::block_flush(bdev);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_FLUSH, FLUSH_STARTED_US, RC, 0);
        if (RC != 0) {
            result = RC;
        }
    }

    perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::SYNC_BLOCKDEV, STARTED_US, result, 0);
    return result;
}

auto has_dirty_bdev_range(dev::BlockDevice* bdev, uint64_t block_no, size_t count) -> bool {
    if (bdev == nullptr || count == 0) {
        return false;
    }

    if (!cache_initialized) {
        return false;
    }

    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    if (cache_dirty_buffers == 0) {
        cache_lock.unlock_irqrestore(IRQFLAGS);
        return false;
    }

    for (auto* bucket_head : hash_buckets) {
        for (BufHead* bh = bucket_head; bh != nullptr; bh = bh->hash_next) {
            if (buffer_overlaps_range(bh, bdev, block_no, count) && (bh->flags & BH_DIRTY) != 0) {
                cache_lock.unlock_irqrestore(IRQFLAGS);
                return true;
            }
        }
    }

    cache_lock.unlock_irqrestore(IRQFLAGS);
    return false;
}

auto has_dirty_bdev_aligned_spans(dev::BlockDevice* bdev, uint64_t block_no, size_t count, size_t span_blocks) -> bool {
    if (bdev == nullptr || bdev->block_size == 0 || count == 0 || span_blocks == 0) {
        return false;
    }

    if (!cache_initialized) {
        return false;
    }

    size_t const SPAN_SIZE = span_blocks * bdev->block_size;
    if (SPAN_SIZE == 0 || SPAN_SIZE / bdev->block_size != span_blocks) {
        return has_dirty_bdev_range(bdev, block_no, count);
    }

    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    if (cache_dirty_buffers == 0) {
        cache_lock.unlock_irqrestore(IRQFLAGS);
        return false;
    }

    uint64_t block = block_no;
    size_t remaining = count;
    while (remaining != 0) {
        BufHead* bh = hash_lookup(bdev, block, SPAN_SIZE);
        if (bh != nullptr && (bh->flags & BH_DIRTY) != 0) {
            cache_lock.unlock_irqrestore(IRQFLAGS);
            return true;
        }
        if (remaining <= span_blocks) {
            break;
        }
        remaining -= span_blocks;
        if (block > UINT64_MAX - static_cast<uint64_t>(span_blocks)) {
            break;
        }
        block += static_cast<uint64_t>(span_blocks);
    }

    cache_lock.unlock_irqrestore(IRQFLAGS);
    return false;
}

void throttle_dirty_buffer_cache(dev::BlockDevice* bdev) {
    if (bdev == nullptr || !cache_initialized) {
        return;
    }

    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    bool const SHOULD_THROTTLE = cache_dirty_bytes > dirty_throttle_high_bytes_locked();
    cache_lock.unlock_irqrestore(IRQFLAGS);
    if (!SHOULD_THROTTLE) {
        return;
    }

    bool expected = false;
    if (!dirty_throttle_active.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_relaxed)) {
        ker::mod::sched::kern_yield();
        return;
    }

    static_cast<void>(sync_blockdev(bdev));
    dirty_throttle_active.store(false, std::memory_order_release);
}

auto sync_bdev_range(dev::BlockDevice* bdev, uint64_t block_no, size_t count) -> int {
    if (bdev == nullptr) {
        return -EINVAL;
    }
    if (count == 0) {
        return 0;
    }

    if (!cache_initialized) {
        return 0;
    }

    int result = 0;
    bool wrote_dirty = false;
    uint64_t irqflags = cache_lock.lock_irqsave();
    if (cache_dirty_buffers == 0) {
        cache_lock.unlock_irqrestore(irqflags);
        return 0;
    }

    constexpr size_t BATCH_SIZE = 64;
    std::array<BufHead*, BATCH_SIZE> batch{};
    std::array<uint64_t, BATCH_SIZE> batch_dirty_epochs{};
    size_t batch_count = 0;
    bool more = true;
    bool waited_for_foreign_writeback = false;
    uint64_t const WRITEBACK_EPOCH = allocate_writeback_epoch();

    while (more) {
        batch_count = 0;
        more = false;
        bool saw_foreign_writeback = false;

        for (auto* bucket_head : hash_buckets) {
            for (BufHead* bh = bucket_head; bh != nullptr && batch_count < BATCH_SIZE; bh = bh->hash_next) {
                if (!buffer_overlaps_range(bh, bdev, block_no, count) || (bh->flags & BH_DIRTY) == 0) {
                    continue;
                }
                if ((bh->flags & BH_WRITEBACK) != 0) {
                    saw_foreign_writeback = saw_foreign_writeback || bh->writeback_epoch != WRITEBACK_EPOCH;
                    continue;
                }
                if (has_older_overlapping_dirty_buffer_locked(bh)) {
                    more = true;
                    continue;
                }
                {
                    mark_buffer_writeback(bh, WRITEBACK_EPOCH);
                    bh->refcount.fetch_add(1, std::memory_order_relaxed);
                    batch_dirty_epochs[batch_count] =
                        bh->writeback_dirty_epoch;  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
                    batch[batch_count++] = bh;      // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
                }
            }
            if (batch_count >= BATCH_SIZE) {
                more = true;
                break;
            }
        }
        if (saw_foreign_writeback) {
            waited_for_foreign_writeback = true;
            more = true;
        }

        cache_lock.unlock_irqrestore(irqflags);

        for (size_t i = 0; i < batch_count; i++) {
            auto* bh = batch[i];  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
            wrote_dirty = true;
            int const RC = write_buffer_snapshot_for_epoch(bh, batch_dirty_epochs[i],
                                                           0);  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
            if (RC != 0) {
                result = RC;
            }
            brelse(bh);
        }

        if (batch_count == 0 && saw_foreign_writeback) {
            ker::mod::sched::kern_yield();
        }
        if (batch_count == 0 && !saw_foreign_writeback) {
            more = false;
        }

        if (more) {
            irqflags = cache_lock.lock_irqsave();
        }
    }

    irqflags = cache_lock.lock_irqsave();
    clear_writeback_for_bdev_range(bdev, block_no, count, WRITEBACK_EPOCH);
    cache_lock.unlock_irqrestore(irqflags);

    if ((wrote_dirty || waited_for_foreign_writeback) && bdev->flush != nullptr) {
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
                if (bh->refcount.load(std::memory_order_relaxed) > 0) {
                    pp = &bh->hash_next;
                    continue;
                }

                *pp = bh->hash_next;
                bh->hash_next = nullptr;
                lru_remove(bh);

                free_unlinked_buffer(bh);
            } else {
                pp = &bh->hash_next;
            }
        }
    }

    cache_lock.unlock_irqrestore(IRQFLAGS);
}

void discard_bdev_range(dev::BlockDevice* bdev, uint64_t block_no, size_t count) {
    if (bdev == nullptr || count == 0) {
        return;
    }

    if (!cache_initialized) {
        buffer_cache_init();
    }

    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    uint64_t discarded_bytes = 0;

    for (auto& hash_bucket : hash_buckets) {
        BufHead** pp = &hash_bucket;
        while (*pp != nullptr) {
            BufHead* bh = *pp;
            if (!buffer_overlaps_range(bh, bdev, block_no, count) || bh->refcount.load(std::memory_order_relaxed) > 0) {
                pp = &bh->hash_next;
                continue;
            }

            *pp = bh->hash_next;
            bh->hash_next = nullptr;
            lru_remove(bh);

            discarded_bytes += bh->size;
            free_unlinked_buffer(bh);
        }
    }

    cache_lock.unlock_irqrestore(IRQFLAGS);
    if (discarded_bytes != 0) {
        perf_record_xfs_count(ker::mod::perf::WkiPerfLocalXfsOp::BUF_DISCARD, discarded_bytes);
    }
}

auto buffer_cache_stats() -> BufferCacheStats {
    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    BufferCacheStats s{};
    s.total_buffers = cache_total_buffers;
    s.dirty_buffers = cache_dirty_buffers;
    s.total_bytes = cache_total_bytes;
    s.dirty_bytes = cache_dirty_bytes;
    s.clean_bytes = cache_total_bytes >= cache_dirty_bytes ? cache_total_bytes - cache_dirty_bytes : 0;
    s.max_bytes = cache_max_bytes;
    s.hits = stat_hits;
    s.misses = stat_misses;
    cache_lock.unlock_irqrestore(IRQFLAGS);
    return s;
}

auto reclaim_clean_buffer_cache(size_t target_bytes) -> BufferCacheReclaimStats {
    BufferCacheReclaimStats stats{};
    if (!cache_initialized) {
        return stats;
    }

    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    stats.before_bytes = cache_total_bytes;
    while (cache_total_bytes > target_bytes) {
        BufHead* victim = find_reclaimable_lru_buffer();
        if (victim == nullptr) {
            break;
        }
        stats.freed_buffers++;
        stats.freed_bytes += victim->size;
        free_buffer(victim);
    }
    stats.after_bytes = cache_total_bytes;
    cache_lock.unlock_irqrestore(IRQFLAGS);
    return stats;
}

}  // namespace ker::vfs
