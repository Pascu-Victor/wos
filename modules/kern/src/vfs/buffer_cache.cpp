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

#include <algorithm>
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
#include <platform/sched/task.hpp>
#include <platform/sched/workqueue.hpp>
#include <util/smallvec.hpp>

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
std::atomic<bool> dirty_writeback_queued{false};
std::atomic<bool> dirty_writeback_wq_creating{false};

bool cache_initialized = false;

constexpr size_t DIRTY_HARD_LIMIT_MULTIPLIER = 4;
constexpr size_t DIRTY_WRITEBACK_BUDGET = 128;
constexpr size_t DIRTY_HARD_FALLBACK_BUDGET = 16;
constexpr size_t DIRTY_WAKE_BATCH = 32;
constexpr size_t DIRTY_TARGET_DIVISOR = 2;
constexpr size_t BUFFER_CACHE_MEMORY_DIVISOR = 32;
constexpr size_t BUFFER_CACHE_MAX_SIZE = static_cast<size_t>(512) * 1024 * 1024;

struct DirtyBdevState {
    dev::BlockDevice* bdev{};
    BufHead* tree_root{};
    BufHead* list_head{};
    BufHead* list_tail{};
    size_t dirty_buffers{};
    size_t dirty_bytes{};
};

struct DirtyWritebackFilter {
    dev::BlockDevice* bdev{};
    uint64_t block_no{};
    size_t count{};
    bool use_range{};
};

struct DirtyWritebackResult {
    bool wrote{};
    bool busy{};
    int status{};
};

struct DirtyWakeList {
    std::array<uint64_t, DIRTY_WAKE_BATCH> pids{};
    size_t count{};
};

ker::util::SmallVec<DirtyBdevState*, 8> dirty_bdev_states;
ker::util::SmallVec<uint64_t, 8> dirty_waiters;
bool dirty_index_degraded = false;
ker::mod::sched::Workqueue* dirty_writeback_wq = nullptr;

void clear_buffer_dirty_locked(BufHead* bh);
void dirty_writeback_worker(void* unused);
ker::mod::sched::WorkItem dirty_writeback_work{.fn = dirty_writeback_worker, .arg = nullptr, .next = nullptr};
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

auto dirty_target_bytes_locked() -> size_t {
    if (cache_max_bytes <= ker::mod::mm::paging::PAGE_SIZE) {
        return cache_max_bytes;
    }
    return std::max(cache_max_bytes / DIRTY_TARGET_DIVISOR, static_cast<size_t>(ker::mod::mm::paging::PAGE_SIZE));
}

auto dirty_hard_limit_bytes_locked() -> size_t {
    if (cache_max_bytes > SIZE_MAX / DIRTY_HARD_LIMIT_MULTIPLIER) {
        return SIZE_MAX;
    }
    return cache_max_bytes * DIRTY_HARD_LIMIT_MULTIPLIER;
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
        clear_buffer_dirty_locked(bh);
    }
    cache_total_bytes -= bh->size;
    cache_total_buffers--;

    free_buffer_data(bh);
    delete bh;
}

auto choose_buffer_cache_max_bytes() -> size_t {
    uint64_t const TOTAL_MEM = ker::mod::mm::phys::get_total_mem_bytes();
    if (TOTAL_MEM == 0) {
        return BUFFER_CACHE_DEFAULT_SIZE;
    }

    uint64_t const SCALED = TOTAL_MEM / BUFFER_CACHE_MEMORY_DIVISOR;
    uint64_t const CLAMPED = std::clamp<uint64_t>(SCALED, BUFFER_CACHE_DEFAULT_SIZE, BUFFER_CACHE_MAX_SIZE);
    auto const SIZE_MAX_U64 = static_cast<uint64_t>(SIZE_MAX);
    return static_cast<size_t>(std::min<uint64_t>(CLAMPED, SIZE_MAX_U64));
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
    bh->dirty_prev = nullptr;
    bh->dirty_next = nullptr;
    bh->dirty_left = nullptr;
    bh->dirty_right = nullptr;
    bh->dirty_parent = nullptr;
    bh->dirty_subtree_last_block = 0;

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

auto buffer_overlaps_range(const BufHead* bh, dev::BlockDevice* bdev, uint64_t block_no, size_t count) -> bool {
    if (bh == nullptr || bh->bdev != bdev) {
        return false;
    }
    return block_ranges_overlap(bh->block_no, buffer_block_count(bh), block_no, count);
}

auto dirty_buffer_last_block(const BufHead* bh) -> uint64_t { return block_range_last_block(bh->block_no, buffer_block_count(bh)); }

auto dirty_priority_mix(uint64_t value) -> uint64_t {
    value ^= value >> 33U;
    value *= 0xff51afd7ed558ccdULL;
    value ^= value >> 33U;
    value *= 0xc4ceb9fe1a85ec53ULL;
    value ^= value >> 33U;
    return value == 0 ? 1 : value;
}

auto dirty_priority(const BufHead* bh) -> uint64_t {
    auto value = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(bh));
    value ^= bh->block_no;
    value ^= static_cast<uint64_t>(bh->size) << 17U;
    value ^= static_cast<uint64_t>(reinterpret_cast<uintptr_t>(bh->bdev)) >> 4U;
    return dirty_priority_mix(value);
}

auto dirty_tree_less(const BufHead* lhs, const BufHead* rhs) -> bool {
    uint64_t const LHS_LAST = dirty_buffer_last_block(lhs);
    uint64_t const RHS_LAST = dirty_buffer_last_block(rhs);
    if (lhs->block_no != rhs->block_no) {
        return lhs->block_no < rhs->block_no;
    }
    if (LHS_LAST != RHS_LAST) {
        return LHS_LAST < RHS_LAST;
    }
    if (lhs->size != rhs->size) {
        return lhs->size < rhs->size;
    }
    return reinterpret_cast<uintptr_t>(lhs) < reinterpret_cast<uintptr_t>(rhs);
}

auto dirty_child_max_last(const BufHead* bh) -> uint64_t { return bh != nullptr ? bh->dirty_subtree_last_block : 0; }

void dirty_recompute(BufHead* bh) {
    if (bh == nullptr) {
        return;
    }
    uint64_t max_last = dirty_buffer_last_block(bh);
    max_last = std::max(max_last, dirty_child_max_last(bh->dirty_left));
    max_last = std::max(max_last, dirty_child_max_last(bh->dirty_right));
    bh->dirty_subtree_last_block = max_last;
}

void dirty_recompute_upwards(BufHead* bh) {
    while (bh != nullptr) {
        dirty_recompute(bh);
        bh = bh->dirty_parent;
    }
}

void dirty_replace_child(DirtyBdevState* state, BufHead* old_child, BufHead* new_child) {
    BufHead* parent = old_child->dirty_parent;
    if (parent == nullptr) {
        state->tree_root = new_child;
    } else if (parent->dirty_left == old_child) {
        parent->dirty_left = new_child;
    } else {
        parent->dirty_right = new_child;
    }
    if (new_child != nullptr) {
        new_child->dirty_parent = parent;
    }
}

void dirty_rotate_left(DirtyBdevState* state, BufHead* node) {
    BufHead* pivot = node->dirty_right;
    if (pivot == nullptr) {
        return;
    }

    node->dirty_right = pivot->dirty_left;
    if (pivot->dirty_left != nullptr) {
        pivot->dirty_left->dirty_parent = node;
    }

    dirty_replace_child(state, node, pivot);
    pivot->dirty_left = node;
    node->dirty_parent = pivot;

    dirty_recompute(node);
    dirty_recompute(pivot);
    dirty_recompute_upwards(pivot->dirty_parent);
}

void dirty_rotate_right(DirtyBdevState* state, BufHead* node) {
    BufHead* pivot = node->dirty_left;
    if (pivot == nullptr) {
        return;
    }

    node->dirty_left = pivot->dirty_right;
    if (pivot->dirty_right != nullptr) {
        pivot->dirty_right->dirty_parent = node;
    }

    dirty_replace_child(state, node, pivot);
    pivot->dirty_right = node;
    node->dirty_parent = pivot;

    dirty_recompute(node);
    dirty_recompute(pivot);
    dirty_recompute_upwards(pivot->dirty_parent);
}

auto find_dirty_bdev_state_locked(dev::BlockDevice* bdev) -> DirtyBdevState* {
    for (auto* state : dirty_bdev_states) {
        if (state != nullptr && state->bdev == bdev) {
            return state;
        }
    }
    return nullptr;
}

auto find_or_create_dirty_bdev_state_locked(dev::BlockDevice* bdev) -> DirtyBdevState* {
    DirtyBdevState* state = find_dirty_bdev_state_locked(bdev);
    if (state != nullptr) {
        return state;
    }

    state = new DirtyBdevState{};
    if (state == nullptr) {
        return nullptr;
    }
    state->bdev = bdev;
    if (!dirty_bdev_states.push_back(state)) {
        delete state;
        return nullptr;
    }
    return state;
}

void dirty_list_insert(DirtyBdevState* state, BufHead* bh) {
    bh->dirty_prev = state->list_tail;
    bh->dirty_next = nullptr;
    if (state->list_tail != nullptr) {
        state->list_tail->dirty_next = bh;
    } else {
        state->list_head = bh;
    }
    state->list_tail = bh;
}

void dirty_list_remove(DirtyBdevState* state, BufHead* bh) {
    if (bh->dirty_prev != nullptr) {
        bh->dirty_prev->dirty_next = bh->dirty_next;
    } else if (state->list_head == bh) {
        state->list_head = bh->dirty_next;
    }
    if (bh->dirty_next != nullptr) {
        bh->dirty_next->dirty_prev = bh->dirty_prev;
    } else if (state->list_tail == bh) {
        state->list_tail = bh->dirty_prev;
    }
    bh->dirty_prev = nullptr;
    bh->dirty_next = nullptr;
}

void dirty_tree_insert(DirtyBdevState* state, BufHead* bh) {
    bh->dirty_left = nullptr;
    bh->dirty_right = nullptr;
    bh->dirty_parent = nullptr;
    bh->dirty_subtree_last_block = dirty_buffer_last_block(bh);

    if (state->tree_root == nullptr) {
        state->tree_root = bh;
        return;
    }

    BufHead* parent = nullptr;
    BufHead* cur = state->tree_root;
    while (cur != nullptr) {
        parent = cur;
        if (dirty_tree_less(bh, cur)) {
            cur = cur->dirty_left;
        } else {
            cur = cur->dirty_right;
        }
    }

    bh->dirty_parent = parent;
    if (dirty_tree_less(bh, parent)) {
        parent->dirty_left = bh;
    } else {
        parent->dirty_right = bh;
    }
    dirty_recompute_upwards(parent);

    while (bh->dirty_parent != nullptr && dirty_priority(bh) < dirty_priority(bh->dirty_parent)) {
        if (bh->dirty_parent->dirty_left == bh) {
            dirty_rotate_right(state, bh->dirty_parent);
        } else {
            dirty_rotate_left(state, bh->dirty_parent);
        }
    }
}

void dirty_tree_remove(DirtyBdevState* state, BufHead* bh) {
    while (bh->dirty_left != nullptr || bh->dirty_right != nullptr) {
        if (bh->dirty_right == nullptr || (bh->dirty_left != nullptr && dirty_priority(bh->dirty_left) < dirty_priority(bh->dirty_right))) {
            dirty_rotate_right(state, bh);
        } else {
            dirty_rotate_left(state, bh);
        }
    }

    BufHead* parent = bh->dirty_parent;
    if (parent == nullptr) {
        state->tree_root = nullptr;
    } else if (parent->dirty_left == bh) {
        parent->dirty_left = nullptr;
    } else if (parent->dirty_right == bh) {
        parent->dirty_right = nullptr;
    }
    bh->dirty_parent = nullptr;
    bh->dirty_subtree_last_block = 0;
    dirty_recompute_upwards(parent);
}

auto dirty_index_insert_locked(BufHead* bh) -> bool {
    DirtyBdevState* state = find_or_create_dirty_bdev_state_locked(bh->bdev);
    if (state == nullptr) {
        dirty_index_degraded = true;
        return false;
    }

    dirty_list_insert(state, bh);
    dirty_tree_insert(state, bh);
    state->dirty_buffers++;
    state->dirty_bytes += bh->size;
    bh->flags |= BH_DIRTY_INDEXED;
    return true;
}

void dirty_index_remove_locked(BufHead* bh) {
    if ((bh->flags & BH_DIRTY_INDEXED) == 0) {
        return;
    }

    DirtyBdevState* state = find_dirty_bdev_state_locked(bh->bdev);
    if (state != nullptr) {
        dirty_tree_remove(state, bh);
        dirty_list_remove(state, bh);
        if (state->dirty_buffers != 0) {
            state->dirty_buffers--;
        }
        if (state->dirty_bytes >= bh->size) {
            state->dirty_bytes -= bh->size;
        } else {
            state->dirty_bytes = 0;
        }
    }
    bh->flags &= ~BH_DIRTY_INDEXED;
}

auto dirty_tree_overlaps(const BufHead* root, uint64_t block_no, size_t count) -> bool {
    if (root == nullptr || count == 0) {
        return false;
    }
    uint64_t const QUERY_LAST = block_range_last_block(block_no, count);
    const BufHead* cur = root;
    while (cur != nullptr) {
        if (cur->dirty_left != nullptr && cur->dirty_left->dirty_subtree_last_block >= block_no) {
            cur = cur->dirty_left;
            continue;
        }
        if (cur->block_no <= QUERY_LAST && dirty_buffer_last_block(cur) >= block_no) {
            return true;
        }
        if (cur->block_no > QUERY_LAST) {
            return false;
        }
        cur = cur->dirty_right;
    }
    return false;
}

auto dirty_state_oldest_epoch_locked(const DirtyBdevState* state) -> uint64_t {
    uint64_t oldest = 0;
    if (state == nullptr) {
        return oldest;
    }
    for (BufHead* bh = state->list_head; bh != nullptr; bh = bh->dirty_next) {
        if ((bh->flags & BH_DIRTY) == 0) {
            continue;
        }
        if (oldest == 0 || bh->dirty_epoch < oldest) {
            oldest = bh->dirty_epoch;
        }
    }
    return oldest;
}

auto dirty_bdev_count_locked() -> size_t {
    size_t count = 0;
    for (auto* state : dirty_bdev_states) {
        if (state != nullptr && state->dirty_buffers != 0) {
            count++;
        }
    }
    return count;
}

auto dirty_filter_matches(const BufHead* bh, const DirtyWritebackFilter& filter) -> bool {
    if (bh == nullptr || (bh->flags & BH_DIRTY) == 0) {
        return false;
    }
    if (filter.bdev != nullptr && bh->bdev != filter.bdev) {
        return false;
    }
    if (filter.use_range && !buffer_overlaps_range(bh, filter.bdev, filter.block_no, filter.count)) {
        return false;
    }
    return true;
}

auto dirty_filter_may_have_match_locked(const DirtyWritebackFilter& filter) -> bool {
    if (cache_dirty_buffers == 0) {
        return false;
    }
    if (dirty_index_degraded || filter.bdev == nullptr || !filter.use_range) {
        return true;
    }
    DirtyBdevState* state = find_dirty_bdev_state_locked(filter.bdev);
    return state != nullptr && dirty_tree_overlaps(state->tree_root, filter.block_no, filter.count);
}

void dirty_consider_candidate(BufHead* bh, const DirtyWritebackFilter& filter, uint64_t min_epoch_exclusive, BufHead*& best) {
    if (!dirty_filter_matches(bh, filter) || bh->dirty_epoch <= min_epoch_exclusive) {
        return;
    }
    if (best == nullptr || bh->dirty_epoch < best->dirty_epoch || (bh->dirty_epoch == best->dirty_epoch && dirty_tree_less(bh, best))) {
        best = bh;
    }
}

void dirty_tree_consider_overlapping(BufHead* root, const DirtyWritebackFilter& filter, uint64_t min_epoch_exclusive, BufHead*& best) {
    if (root == nullptr || !filter.use_range || filter.bdev == nullptr) {
        return;
    }

    uint64_t const QUERY_LAST = block_range_last_block(filter.block_no, filter.count);

    if (root->dirty_left != nullptr && root->dirty_left->dirty_subtree_last_block >= filter.block_no) {
        dirty_tree_consider_overlapping(root->dirty_left, filter, min_epoch_exclusive, best);
    }

    dirty_consider_candidate(root, filter, min_epoch_exclusive, best);

    if (root->block_no <= QUERY_LAST) {
        dirty_tree_consider_overlapping(root->dirty_right, filter, min_epoch_exclusive, best);
    }
}

auto find_oldest_matching_dirty_buffer_locked(const DirtyWritebackFilter& filter, uint64_t min_epoch_exclusive) -> BufHead* {
    if (!dirty_filter_may_have_match_locked(filter)) {
        return nullptr;
    }

    BufHead* best = nullptr;
    if (dirty_index_degraded) {
        for (auto* bucket_head : hash_buckets) {
            for (BufHead* bh = bucket_head; bh != nullptr; bh = bh->hash_next) {
                dirty_consider_candidate(bh, filter, min_epoch_exclusive, best);
            }
        }
        return best;
    }

    if (filter.bdev != nullptr && filter.use_range) {
        DirtyBdevState* state = find_dirty_bdev_state_locked(filter.bdev);
        if (state == nullptr || state->dirty_buffers == 0 || !dirty_tree_overlaps(state->tree_root, filter.block_no, filter.count)) {
            return nullptr;
        }
        dirty_tree_consider_overlapping(state->tree_root, filter, min_epoch_exclusive, best);
        return best;
    }

    for (auto* state : dirty_bdev_states) {
        if (state == nullptr || state->dirty_buffers == 0) {
            continue;
        }
        if (filter.bdev != nullptr && state->bdev != filter.bdev) {
            continue;
        }
        if (filter.use_range && !dirty_tree_overlaps(state->tree_root, filter.block_no, filter.count)) {
            continue;
        }
        for (BufHead* bh = state->list_head; bh != nullptr; bh = bh->dirty_next) {
            dirty_consider_candidate(bh, filter, min_epoch_exclusive, best);
        }
    }
    return best;
}

void clear_buffer_dirty_locked(BufHead* bh) {
    if (bh == nullptr || (bh->flags & BH_DIRTY) == 0) {
        return;
    }
    dirty_index_remove_locked(bh);
    bh->flags &= ~BH_DIRTY;
    cache_dirty_buffers--;
    cache_dirty_bytes -= bh->size;
}

void collect_dirty_waiters_locked(DirtyWakeList& wake_list) {
    if (cache_dirty_bytes > dirty_target_bytes_locked()) {
        return;
    }
    while (!dirty_waiters.empty() && wake_list.count < wake_list.pids.size()) {
        wake_list.pids.at(wake_list.count++) = dirty_waiters.at(0);
        static_cast<void>(dirty_waiters.remove_at(0));
    }
}

void wake_dirty_waiters(const DirtyWakeList& wake_list) {
    for (size_t i = 0; i < wake_list.count; ++i) {
        static_cast<void>(ker::mod::sched::wake_task_by_pid_from_event(wake_list.pids.at(i)));
    }
}

void register_current_dirty_waiter_locked() {
    if (!ker::mod::sched::can_query_current_task()) {
        return;
    }
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || task->pid == 0 || dirty_waiters.contains(task->pid)) {
        return;
    }
    static_cast<void>(dirty_waiters.push_back(task->pid));
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
    static_cast<void>(dirty_index_insert_locked(bh));
    return true;
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
    DirtyWakeList wake_list{};
    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    if (RC == 0 && (bh->flags & BH_DIRTY) != 0 && bh->dirty_epoch == writeback_dirty_epoch) {
        clear_buffer_dirty_locked(bh);
        collect_dirty_waiters_locked(wake_list);
    }
    if (owns_writeback_epoch(bh, owned_writeback_epoch)) {
        clear_buffer_writeback(bh);
    }
    cache_lock.unlock_irqrestore(IRQFLAGS);
    wake_dirty_waiters(wake_list);
    return RC;
}

auto writeback_dirty_one(const DirtyWritebackFilter& filter) -> DirtyWritebackResult {
    DirtyWritebackResult result{};
    uint64_t const WRITEBACK_EPOCH = allocate_writeback_epoch();
    uint64_t writeback_dirty_epoch = 0;
    BufHead* bh = nullptr;

    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    bh = find_oldest_matching_dirty_buffer_locked(filter, 0);
    if (bh == nullptr) {
        cache_lock.unlock_irqrestore(IRQFLAGS);
        return result;
    }
    if ((bh->flags & BH_WRITEBACK) != 0) {
        result.busy = true;
        cache_lock.unlock_irqrestore(IRQFLAGS);
        return result;
    }

    mark_buffer_writeback(bh, WRITEBACK_EPOCH);
    writeback_dirty_epoch = bh->writeback_dirty_epoch;
    bh->refcount.fetch_add(1, std::memory_order_relaxed);
    cache_lock.unlock_irqrestore(IRQFLAGS);

    result.status = write_buffer_snapshot_for_epoch(bh, writeback_dirty_epoch, WRITEBACK_EPOCH);
    result.wrote = true;
    brelse(bh);
    return result;
}

auto writeback_dirty_budgeted(const DirtyWritebackFilter& filter, size_t budget) -> int {
    int result = 0;
    for (size_t i = 0; i < budget; ++i) {
        DirtyWritebackResult const WB = writeback_dirty_one(filter);
        if (WB.wrote) {
            if (WB.status != 0) {
                result = WB.status;
                break;
            }
            continue;
        }
        if (WB.busy) {
            ker::mod::sched::kern_yield();
            continue;
        }
        break;
    }
    return result;
}

auto dirty_bytes_above_target_locked() -> bool { return cache_dirty_bytes > dirty_target_bytes_locked(); }

auto ensure_dirty_writeback_wq() -> ker::mod::sched::Workqueue* {
    if (dirty_writeback_wq != nullptr) {
        return dirty_writeback_wq;
    }
    if (!ker::mod::sched::has_run_queues()) {
        return nullptr;
    }

    bool expected = false;
    if (!dirty_writeback_wq_creating.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_relaxed)) {
        return dirty_writeback_wq;
    }

    auto* wq = ker::mod::sched::Workqueue::create("bcache_wb");
    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    if (dirty_writeback_wq == nullptr) {
        dirty_writeback_wq = wq;
    }
    cache_lock.unlock_irqrestore(IRQFLAGS);
    dirty_writeback_wq_creating.store(false, std::memory_order_release);
    return dirty_writeback_wq;
}

auto request_dirty_writeback() -> bool {
    auto* wq = ensure_dirty_writeback_wq();
    if (wq == nullptr) {
        return false;
    }

    bool expected = false;
    if (dirty_writeback_queued.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_relaxed)) {
        wq->enqueue(&dirty_writeback_work);
    }
    return true;
}

void dirty_writeback_worker(void* unused) {
    (void)unused;
    size_t write_count = 0;
    while (true) {
        uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
        bool const NEEDS_WRITEBACK = dirty_bytes_above_target_locked();
        cache_lock.unlock_irqrestore(IRQFLAGS);
        if (!NEEDS_WRITEBACK) {
            break;
        }

        DirtyWritebackResult const WB = writeback_dirty_one({});
        if (WB.wrote) {
            if (WB.status != 0) {
                break;
            }
            write_count++;
            if ((write_count % DIRTY_WRITEBACK_BUDGET) == 0) {
                ker::mod::sched::kern_yield();
            }
            continue;
        }
        if (WB.busy) {
            ker::mod::sched::kern_yield();
            continue;
        }
        break;
    }

    dirty_writeback_queued.store(false, std::memory_order_release);

    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    bool const STILL_DIRTY = dirty_bytes_above_target_locked();
    cache_lock.unlock_irqrestore(IRQFLAGS);
    if (STILL_DIRTY) {
        static_cast<void>(request_dirty_writeback());
    }
}

}  // anonymous namespace

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

void buffer_cache_init() {
    if (cache_initialized) {
        return;
    }
    cache_max_bytes = choose_buffer_cache_max_bytes();
    hash_buckets.fill(nullptr);
    lru_init();
    cache_total_bytes = 0;
    cache_total_buffers = 0;
    cache_dirty_buffers = 0;
    cache_dirty_bytes = 0;
    dirty_bdev_states.clear();
    dirty_waiters.clear();
    dirty_index_degraded = false;
    dirty_writeback_queued.store(false, std::memory_order_release);
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
        static_cast<void>(copy_dirty_bdev_range(bdev, block_no, 1, bh->data));
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
    static_cast<void>(copy_dirty_bdev_range(bdev, block_no, 1, bh->data));

    irqflags = cache_lock.lock_irqsave();
    BufHead* existing = hash_lookup(bdev, block_no, bdev->block_size);
    if (existing != nullptr) {
        existing->refcount.fetch_add(1, std::memory_order_relaxed);
        lru_touch(existing);
        free_buffer(bh);
        cache_lock.unlock_irqrestore(irqflags);
        static_cast<void>(copy_dirty_bdev_range(bdev, block_no, 1, existing->data));
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
        static_cast<void>(copy_dirty_bdev_range(bdev, block_no, count, bh->data));
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
    static_cast<void>(copy_dirty_bdev_range(bdev, block_no, count, bh->data));

    irqflags = cache_lock.lock_irqsave();
    BufHead* existing = hash_lookup(bdev, block_no, TOTAL_SIZE);
    if (existing != nullptr) {
        existing->refcount.fetch_add(1, std::memory_order_relaxed);
        lru_touch(existing);
        free_buffer(bh);
        cache_lock.unlock_irqrestore(irqflags);
        static_cast<void>(copy_dirty_bdev_range(bdev, block_no, count, existing->data));
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

    DirtyWritebackFilter filter{};
    filter.bdev = bdev;

    bool wrote_dirty = false;
    bool waited_for_writeback = false;
    while (true) {
        DirtyWritebackResult const WB = writeback_dirty_one(filter);
        if (WB.wrote) {
            wrote_dirty = true;
            if (WB.status != 0) {
                result = WB.status;
                break;
            }
            continue;
        }
        if (WB.busy) {
            waited_for_writeback = true;
            ker::mod::sched::kern_yield();
            continue;
        }
        break;
    }

    // Flush the device if it supports it
    if ((wrote_dirty || waited_for_writeback) && bdev->flush != nullptr) {
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

    if (dirty_index_degraded) {
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

    DirtyBdevState* state = find_dirty_bdev_state_locked(bdev);
    bool const DIRTY = state != nullptr && dirty_tree_overlaps(state->tree_root, block_no, count);
    cache_lock.unlock_irqrestore(IRQFLAGS);
    return DIRTY;
}

auto copy_dirty_bdev_range(dev::BlockDevice* bdev, uint64_t block_no, size_t count, uint8_t* dst) -> bool {
    if (bdev == nullptr || bdev->block_size == 0 || count == 0 || dst == nullptr || !cache_initialized) {
        return false;
    }
    if (count > SIZE_MAX / bdev->block_size) {
        return false;
    }

    DirtyWritebackFilter filter{};
    filter.bdev = bdev;
    filter.block_no = block_no;
    filter.count = count;
    filter.use_range = true;

    bool copied = false;
    uint64_t min_epoch = 0;
    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    while (true) {
        BufHead* bh = find_oldest_matching_dirty_buffer_locked(filter, min_epoch);
        if (bh == nullptr) {
            break;
        }
        min_epoch = bh->dirty_epoch;

        uint64_t const BUF_LAST = dirty_buffer_last_block(bh);
        uint64_t const RANGE_LAST = block_range_last_block(block_no, count);
        uint64_t const COPY_FIRST = std::max(bh->block_no, block_no);
        uint64_t const COPY_LAST = std::min(BUF_LAST, RANGE_LAST);
        if (COPY_FIRST > COPY_LAST) {
            continue;
        }

        size_t const BLOCK_SIZE = bdev->block_size;
        auto const COPY_BLOCKS = static_cast<size_t>(COPY_LAST - COPY_FIRST + 1);
        size_t copy_bytes = COPY_BLOCKS * BLOCK_SIZE;
        size_t const SRC_OFF = static_cast<size_t>(COPY_FIRST - bh->block_no) * BLOCK_SIZE;
        size_t const DST_OFF = static_cast<size_t>(COPY_FIRST - block_no) * BLOCK_SIZE;
        size_t const DST_SIZE = count * BLOCK_SIZE;
        if (SRC_OFF >= bh->size || DST_OFF >= DST_SIZE) {
            continue;
        }
        copy_bytes = std::min(copy_bytes, bh->size - SRC_OFF);
        copy_bytes = std::min(copy_bytes, DST_SIZE - DST_OFF);
        std::memcpy(dst + DST_OFF, bh->data + SRC_OFF, copy_bytes);
        copied = true;
    }
    cache_lock.unlock_irqrestore(IRQFLAGS);
    return copied;
}

void kick_dirty_buffer_cache_writeback(dev::BlockDevice* bdev) {
    if (bdev == nullptr || !cache_initialized) {
        return;
    }

    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    DirtyBdevState* state = find_dirty_bdev_state_locked(bdev);
    bool const SHOULD_WRITEBACK =
        cache_dirty_bytes > dirty_target_bytes_locked() && (dirty_index_degraded || (state != nullptr && state->dirty_buffers != 0));
    cache_lock.unlock_irqrestore(IRQFLAGS);
    if (SHOULD_WRITEBACK) {
        static_cast<void>(request_dirty_writeback());
    }
}

void throttle_dirty_buffer_cache(dev::BlockDevice* bdev) {
    if (bdev == nullptr || !cache_initialized) {
        return;
    }

    kick_dirty_buffer_cache_writeback(bdev);

    uint64_t irqflags = cache_lock.lock_irqsave();
    bool should_wait = cache_dirty_bytes > dirty_hard_limit_bytes_locked();
    cache_lock.unlock_irqrestore(irqflags);
    if (!should_wait) {
        return;
    }

    DirtyWritebackFilter fallback_filter{};

    while (true) {
        irqflags = cache_lock.lock_irqsave();
        if (cache_dirty_bytes <= dirty_target_bytes_locked()) {
            cache_lock.unlock_irqrestore(irqflags);
            return;
        }
        register_current_dirty_waiter_locked();
        cache_lock.unlock_irqrestore(irqflags);

        bool const HAVE_WORKER = request_dirty_writeback();
        if (!HAVE_WORKER) {
            static_cast<void>(writeback_dirty_budgeted(fallback_filter, DIRTY_HARD_FALLBACK_BUDGET));
        }
        ker::mod::sched::kern_yield();
    }
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
    bool waited_for_foreign_writeback = false;

    DirtyWritebackFilter filter{};
    filter.bdev = bdev;
    filter.block_no = block_no;
    filter.count = count;
    filter.use_range = true;

    while (true) {
        DirtyWritebackResult const WB = writeback_dirty_one(filter);
        if (WB.wrote) {
            wrote_dirty = true;
            if (WB.status != 0) {
                result = WB.status;
                break;
            }
            continue;
        }
        if (WB.busy) {
            waited_for_foreign_writeback = true;
            ker::mod::sched::kern_yield();
            continue;
        }
        break;
    }

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
    s.dirty_bdevs = dirty_bdev_count_locked();
    s.dirty_target_bytes = dirty_target_bytes_locked();
    s.dirty_hard_bytes = dirty_hard_limit_bytes_locked();
    s.dirty_waiters = dirty_waiters.size();
    s.hits = stat_hits;
    s.misses = stat_misses;
    cache_lock.unlock_irqrestore(IRQFLAGS);
    return s;
}

auto buffer_cache_bdev_stats(BufferCacheBdevStats* out, size_t capacity) -> size_t {
    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    size_t total = 0;
    for (auto* state : dirty_bdev_states) {
        if (state == nullptr || state->dirty_buffers == 0) {
            continue;
        }
        if (out != nullptr && total < capacity) {
            BufferCacheBdevStats& row = out[total];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            row.bdev = state->bdev;
            row.name = state->bdev != nullptr ? state->bdev->name.data() : nullptr;
            row.dirty_buffers = state->dirty_buffers;
            row.dirty_bytes = state->dirty_bytes;
            row.oldest_dirty_epoch = dirty_state_oldest_epoch_locked(state);
        }
        total++;
    }
    cache_lock.unlock_irqrestore(IRQFLAGS);
    return total;
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
