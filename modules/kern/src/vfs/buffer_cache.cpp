// Buffer Cache implementation.
// Provides bread/brelse/bwrite block caching for filesystem metadata I/O.
// Reference: Linux fs/buffer.c, xfs/xfs_buf.c
//
// Design:
//   - Hash table indexed by (bdev, block_no) for O(1) lookup.
//   - LRU doubly-linked list for eviction of unreferenced buffers.
//   - Configurable maximum cache size (64 MB minimum, RAM-scaled at init).
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
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
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
    return static_cast<size_t>(h) & (BUFFER_CACHE_HASH_BUCKETS - 1);
}

void range_index_insert_locked(BufHead* bh);
void range_index_remove_locked(BufHead* bh);

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
    range_index_insert_locked(bh);
}

void hash_remove(BufHead* bh) {
    range_index_remove_locked(bh);
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

void lru_insert_head(BufHead* bh) {
    // Insert after sentinel (= head)
    bh->lru_next = lru_sentinel.lru_next;
    bh->lru_prev = &lru_sentinel;
    lru_sentinel.lru_next->lru_prev = bh;
    lru_sentinel.lru_next = bh;
    lru_count++;
}

void lru_move_to_head(BufHead* bh) {
    if (bh->lru_prev == &lru_sentinel) {
        return;
    }
    if (bh->lru_prev != nullptr || bh->lru_next != nullptr) {
        lru_remove(bh);
    }
    lru_insert_head(bh);
}

// Newly cached buffers are inserted at the head. Existing hit buffers use a
// second-chance bit so hot cache hits avoid relinking the global LRU list.
void lru_touch(BufHead* bh) {
    if (bh->lru_prev == nullptr && bh->lru_next == nullptr) {
        lru_insert_head(bh);
        return;
    }
    if (bh->lru_prev == &lru_sentinel) {
        return;
    }
    bh->flags |= BH_LRU_REFERENCED;
}

void lru_second_chance(BufHead* bh) {
    bh->flags &= ~BH_LRU_REFERENCED;
    lru_move_to_head(bh);
}

// Get the least-recently-used buffer (tail of the list, just before sentinel)
auto lru_tail() -> BufHead* {
    BufHead* bh = lru_sentinel.lru_prev;
    if (bh == &lru_sentinel) {
        return nullptr;
    }
    return bh;  // NOLINT(clang-analyzer-cplusplus.NewDelete): lru_remove unlinks victims before free_buffer deletes them.
}

auto lru_head() -> BufHead* {
    BufHead* bh = lru_sentinel.lru_next;
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
size_t cache_dirty_bytes = 0;
size_t dirty_target_bytes = BUFFER_CACHE_DEFAULT_SIZE / 2;
size_t dirty_hard_limit_bytes = BUFFER_CACHE_DEFAULT_SIZE;

uint64_t stat_hits = 0;
uint64_t stat_misses = 0;
std::atomic<uint64_t> stat_disk_read_calls{0};
std::atomic<uint64_t> stat_disk_read_bytes{0};
std::atomic<uint64_t> stat_metadata_disk_read_calls{0};
std::atomic<uint64_t> stat_metadata_disk_read_bytes{0};
std::atomic<uint64_t> stat_data_disk_read_calls{0};
std::atomic<uint64_t> stat_data_disk_read_bytes{0};
std::atomic<uint64_t> stat_range_copy_attempts{0};
std::atomic<uint64_t> stat_range_copy_cover_hits{0};
std::atomic<uint64_t> stat_range_copy_overlap_hits{0};
std::atomic<uint64_t> stat_range_copy_no_state{0};
std::atomic<uint64_t> stat_range_copy_no_overlap{0};
std::atomic<uint64_t> stat_range_copy_incomplete{0};
std::atomic<uint64_t> stat_range_copy_overflow{0};
std::atomic<uint64_t> stat_range_copy_degraded{0};
std::atomic<uint64_t> next_writeback_epoch{1};
std::atomic<uint64_t> next_dirty_epoch{1};
std::atomic<bool> dirty_writeback_queued{false};
std::atomic<bool> dirty_writeback_wq_creating{false};

bool cache_initialized = false;

constexpr size_t DIRTY_WRITEBACK_BUDGET = 1024;
constexpr size_t DIRTY_WRITEBACK_YIELD_BYTES = size_t{16} * 1024 * 1024;
constexpr size_t DIRTY_HARD_FALLBACK_BUDGET = DIRTY_WRITEBACK_BUDGET * 4;
constexpr size_t DIRTY_HARD_FALLBACK_BYTES = size_t{64} * 1024 * 1024;
constexpr size_t DIRTY_WRITEBACK_RUN_MAX_BUFFERS = 1024;
constexpr size_t BUFFER_CACHE_CONTIG_ALLOC_MAX_BYTES = size_t{2} * 1024 * 1024;
#ifndef WOS_HOST_TEST
constexpr size_t BUFFER_CACHE_BUDDY_ALLOC_MAX_BYTES = size_t{64} * 1024;
#endif
constexpr size_t DIRTY_WRITEBACK_RUN_MAX_BYTES = BUFFER_CACHE_CONTIG_ALLOC_MAX_BYTES;
constexpr size_t DIRTY_WAKE_BATCH = 32;
constexpr size_t DIRTY_COPY_COVERAGE_MAX_INTERVALS = 64;
constexpr size_t RANGE_COPY_MAX_VISITS = 256;
constexpr size_t CLEAN_ALIAS_DISCARD_MAX_BUFFERS = 64;
constexpr size_t CLEAN_ALIAS_DISCARD_MAX_BYTES = size_t{4} * 1024 * 1024;
constexpr uint64_t DIRTY_THROTTLE_PARK_TIMEOUT_US = uint64_t{10} * 1000;
constexpr size_t DIRTY_TARGET_DIVISOR = 2;
constexpr size_t DIRTY_TARGET_MAX_NUMERATOR = 3;
constexpr size_t DIRTY_TARGET_MAX_DENOMINATOR = 4;
constexpr size_t DIRTY_THROTTLE_RESUME_NUMERATOR = 3;
constexpr size_t DIRTY_THROTTLE_RESUME_DENOMINATOR = 4;
// Let clean cache grow beyond dirty limits, closer to Linux's page cache shape.
// Large-memory self-host checkouts need enough dirty headroom to avoid forcing
// every few hundred MiB of file creation through synchronous writeback.
constexpr size_t BUFFER_CACHE_MEMORY_NUMERATOR = 7;
constexpr size_t BUFFER_CACHE_MEMORY_DENOMINATOR = 16;
constexpr size_t DIRTY_TARGET_MEMORY_DIVISOR = 8;
constexpr size_t DIRTY_TARGET_LARGE_MEMORY_DIVISOR = 4;
constexpr uint64_t DIRTY_TARGET_LARGE_MEMORY_THRESHOLD = uint64_t{8} * 1024 * 1024 * 1024;
constexpr size_t BUFFER_CACHE_MAX_SIZE = size_t{16} * 1024 * 1024 * 1024;

struct DirtyBdevState {
    dev::BlockDevice* bdev{};
    BufHead* tree_root{};
    BufHead* list_head{};
    BufHead* list_tail{};
    size_t dirty_buffers{};
    size_t dirty_bytes{};
};

struct RangeBdevState {
    dev::BlockDevice* bdev{};
    BufHead* tree_root{};
    size_t buffers{};
    size_t bytes{};
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
    uint64_t dirty_epoch{};
    size_t buffers{};
    size_t bytes{};
};

struct DirtyWakeList {
    std::array<uint64_t, DIRTY_WAKE_BATCH> pids{};
    size_t count{};
};

struct DirtyWritebackRun {
    std::array<BufHead*, DIRTY_WRITEBACK_RUN_MAX_BUFFERS> buffers{};
    std::array<uint64_t, DIRTY_WRITEBACK_RUN_MAX_BUFFERS> dirty_epochs{};
    dev::BlockDevice* bdev{};
    uint64_t block_no{};
    size_t block_count{};
    size_t bytes{};
    size_t count{};
    uint64_t writeback_epoch{};
};

struct DirtyCoverageInterval {
    uint64_t first{};
    uint64_t last{};
};

ker::util::SmallVec<DirtyBdevState*, 8> dirty_bdev_states;
ker::util::SmallVec<RangeBdevState*, 8> range_bdev_states;
ker::util::SmallVec<uint64_t, 8> dirty_waiters;
bool dirty_index_degraded = false;
bool range_index_degraded = false;
ker::mod::sched::Workqueue* dirty_writeback_wq = nullptr;

void clear_buffer_dirty_locked(BufHead* bh);
auto copy_dirty_bdev_range_locked(dev::BlockDevice* bdev, uint64_t block_no, size_t count, uint8_t* dst, uint64_t min_epoch_exclusive)
    -> bool;
void overlay_newer_cached_aliases(BufHead const* source, uint64_t source_epoch, uint8_t* dst);
void dirty_coverage_add(std::array<DirtyCoverageInterval, DIRTY_COPY_COVERAGE_MAX_INTERVALS>& intervals, size_t& interval_count,
                        DirtyCoverageInterval interval, bool& overflow);
auto dirty_coverage_complete(const std::array<DirtyCoverageInterval, DIRTY_COPY_COVERAGE_MAX_INTERVALS>& intervals, size_t interval_count,
                             uint64_t block_no, size_t count) -> bool;
void dirty_writeback_worker(void* unused);
ker::mod::sched::WorkItem dirty_writeback_work{.fn = dirty_writeback_worker, .arg = nullptr, .next = nullptr};
constexpr size_t HOT_EVICT_SCAN_BUDGET = 8192;
constexpr size_t HOT_EVICT_MAX_VICTIMS = 4096;
constexpr size_t HOT_EVICT_MAX_BYTES = size_t{16} * 1024 * 1024;
constexpr size_t BUFHEAD_ARENA_BYTES = size_t{256} * 1024;
constexpr size_t BUFHEAD_STRIDE = (sizeof(BufHead) + alignof(BufHead) - 1) & ~(alignof(BufHead) - 1);

struct BufHeadPool {
    ker::mod::sys::Spinlock lock;
    BufHead* free_list{};
};

BufHeadPool bufhead_pool{};

struct WritebackSnapshot {
    uint8_t* data = nullptr;
    uint32_t flags = 0;
    size_t size = 0;
};

auto perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp op) -> uint64_t {
    return ker::mod::perf::is_wki_scope_recording_enabled(ker::mod::perf::WkiPerfScope::LOCAL_XFS, static_cast<uint8_t>(op))
               ? ker::mod::time::get_us()
               : 0;
}

auto perf_xfs_started_us() -> uint64_t { return ker::mod::perf::is_local_xfs_recording_enabled() ? ker::mod::time::get_us() : 0; }

auto dirty_target_bytes_locked() -> size_t { return std::min(dirty_target_bytes, cache_max_bytes); }

auto dirty_hard_limit_bytes_locked() -> size_t { return std::min(dirty_hard_limit_bytes, cache_max_bytes); }

auto dirty_throttle_resume_bytes_locked() -> size_t {
    size_t const TARGET = dirty_target_bytes_locked();
    size_t const HARD = dirty_hard_limit_bytes_locked();
    if (HARD <= TARGET) {
        return TARGET;
    }
    return TARGET + (((HARD - TARGET) * DIRTY_THROTTLE_RESUME_NUMERATOR) / DIRTY_THROTTLE_RESUME_DENOMINATOR);
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

void add_bufhead_arena_to_pool_locked(void* arena, size_t bytes) {
    auto* next = static_cast<uint8_t*>(arena);
    size_t remaining = bytes;
    while (remaining >= BUFHEAD_STRIDE) {
        auto* bh = new (next) BufHead{};
        bh->hash_next = bufhead_pool.free_list;
        bufhead_pool.free_list = bh;
        next += BUFHEAD_STRIDE;
        remaining -= BUFHEAD_STRIDE;
    }
}

auto alloc_bufhead() -> BufHead* {
    auto pop_free = []() -> BufHead* {
        uint64_t const IRQFLAGS = bufhead_pool.lock.lock_irqsave();
        BufHead* bh = bufhead_pool.free_list;
        if (bh != nullptr) {
            bufhead_pool.free_list = bh->hash_next;
            bh->hash_next = nullptr;
        }
        bufhead_pool.lock.unlock_irqrestore(IRQFLAGS);
        return bh;
    };

    if (BufHead* bh = pop_free()) {
        return bh;
    }

    void* const ARENA = ker::mod::mm::phys::page_alloc_full_overwrite(BUFHEAD_ARENA_BYTES, "buffer_cache_bufheads");
    if (ARENA == nullptr) {
        return new (std::nothrow) BufHead{};
    }

    uint64_t const IRQFLAGS = bufhead_pool.lock.lock_irqsave();
    add_bufhead_arena_to_pool_locked(ARENA, BUFHEAD_ARENA_BYTES);
    BufHead* bh = bufhead_pool.free_list;
    if (bh != nullptr) {
        bufhead_pool.free_list = bh->hash_next;
        bh->hash_next = nullptr;
    }
    bufhead_pool.lock.unlock_irqrestore(IRQFLAGS);
    return bh;
}

void free_bufhead(BufHead* bh) {
    if (bh == nullptr) {
        return;
    }
    uint64_t const IRQFLAGS = bufhead_pool.lock.lock_irqsave();
    bh->hash_next = bufhead_pool.free_list;
    bufhead_pool.free_list = bh;
    bufhead_pool.lock.unlock_irqrestore(IRQFLAGS);
}

auto allocate_buffer_data(size_t size, uint32_t& flags) -> uint8_t* {
    flags &= ~(BH_DATA_PAGE_ALLOC | BH_DATA_VMAP);
    if (size > BUFFER_CACHE_CONTIG_ALLOC_MAX_BYTES) {
        return nullptr;
    }
#ifndef WOS_HOST_TEST
    if (size > BUFFER_CACHE_BUDDY_ALLOC_MAX_BYTES) {
        auto* data = static_cast<uint8_t*>(ker::mod::mm::virt::kernel_vmap_alloc(size, "buffer_cache"));
        if (data != nullptr) {
            flags |= BH_DATA_VMAP;
        }
        return data;
    }
#endif
    if (size >= ker::mod::mm::paging::PAGE_SIZE && (size % ker::mod::mm::paging::PAGE_SIZE) == 0) {
        auto* page_data = static_cast<uint8_t*>(ker::mod::mm::phys::page_alloc_full_overwrite_may_fail(size, "buffer_cache"));
        if (page_data != nullptr) {
            flags |= BH_DATA_PAGE_ALLOC;
            return page_data;
        }
#ifndef WOS_HOST_TEST
        // Small buffers normally use the buddy allocator to avoid vmap
        // lifecycle overhead. Under fragmentation, retain the same contiguous
        // virtual layout with independent order-0 backing pages.
        auto* data = static_cast<uint8_t*>(ker::mod::mm::virt::kernel_vmap_alloc(size, "buffer_cache"));
        if (data != nullptr) {
            flags |= BH_DATA_VMAP;
        }
        return data;
#else
        return nullptr;
#endif
    }
    return new uint8_t[size];
}

auto checked_block_span_size(const dev::BlockDevice* bdev, size_t count, size_t& total_size) -> bool {
    if (bdev == nullptr || bdev->block_size == 0 || count == 0 || count > SIZE_MAX / bdev->block_size) {
        return false;
    }
    total_size = bdev->block_size * count;
    return true;
}

void free_data_buffer(uint8_t* data, uint32_t flags, size_t size) {
    if (data == nullptr) {
        return;
    }
#ifndef WOS_HOST_TEST
    if ((flags & BH_DATA_VMAP) != 0) {
        ker::mod::mm::virt::kernel_vmap_free(data, size);
        return;
    }
#else
    (void)size;
#endif
    if ((flags & BH_DATA_PAGE_ALLOC) != 0) {
        ker::mod::mm::phys::page_free(data);
    } else {
        delete[] data;
    }
}

void drain_deferred_data_buffer_frees() {
#ifndef WOS_HOST_TEST
    ker::mod::mm::virt::drain_kernel_vmap_frees();
#endif
}

void drain_deferred_data_buffer_frees_if_over_limit() {
#ifndef WOS_HOST_TEST
    ker::mod::mm::virt::drain_kernel_vmap_frees_if_over_limit();
#endif
}

void free_buffer_data(BufHead* bh) {
    if (bh == nullptr || bh->data == nullptr) {
        return;
    }
    free_data_buffer(bh->data, bh->flags, bh->size);
    bh->data = nullptr;
    bh->flags &= ~(BH_DATA_PAGE_ALLOC | BH_DATA_VMAP);
}

auto make_writeback_snapshot(const BufHead* bh) -> WritebackSnapshot {
    WritebackSnapshot snapshot{};
    snapshot.data = allocate_buffer_data(bh->size, snapshot.flags);
    if (snapshot.data == nullptr) {
        return snapshot;
    }
    snapshot.size = bh->size;
    memcpy(snapshot.data, bh->data, bh->size);
    return snapshot;
}

auto make_writeback_snapshot_for_epoch(const BufHead* bh, uint64_t dirty_epoch) -> WritebackSnapshot {
    WritebackSnapshot snapshot = make_writeback_snapshot(bh);
    if (snapshot.data != nullptr) {
        overlay_newer_cached_aliases(bh, dirty_epoch, snapshot.data);
    }
    return snapshot;
}

void free_writeback_snapshot(WritebackSnapshot& snapshot) {
    free_data_buffer(snapshot.data, snapshot.flags, snapshot.size);
    snapshot.data = nullptr;
    snapshot.flags = 0;
    snapshot.size = 0;
}

void free_unlinked_buffer(BufHead* bh) {
    range_index_remove_locked(bh);
    if ((bh->flags & BH_DIRTY) != 0) {
        clear_buffer_dirty_locked(bh);
    }
    cache_total_bytes -= bh->size;
    cache_total_buffers--;

    free_buffer_data(bh);
    free_bufhead(bh);
}

auto clamp_u64_to_size(uint64_t value) -> size_t {
    auto const SIZE_MAX_U64 = static_cast<uint64_t>(SIZE_MAX);
    return static_cast<size_t>(std::min<uint64_t>(value, SIZE_MAX_U64));
}

auto choose_buffer_cache_max_bytes_for_total(uint64_t total_mem) -> size_t {
    if (total_mem == 0) {
        return BUFFER_CACHE_DEFAULT_SIZE;
    }

    uint64_t const SCALED = (total_mem / BUFFER_CACHE_MEMORY_DENOMINATOR) * BUFFER_CACHE_MEMORY_NUMERATOR;
    uint64_t const CLAMPED = std::clamp<uint64_t>(SCALED, BUFFER_CACHE_DEFAULT_SIZE, BUFFER_CACHE_MAX_SIZE);
    return clamp_u64_to_size(CLAMPED);
}

auto choose_dirty_target_bytes_for_total(uint64_t total_mem, size_t max_bytes) -> size_t {
    if (max_bytes <= ker::mod::mm::paging::PAGE_SIZE) {
        return max_bytes;
    }

    auto const FALLBACK = static_cast<uint64_t>(max_bytes / DIRTY_TARGET_DIVISOR);
    size_t const MEMORY_DIVISOR =
        total_mem >= DIRTY_TARGET_LARGE_MEMORY_THRESHOLD ? DIRTY_TARGET_LARGE_MEMORY_DIVISOR : DIRTY_TARGET_MEMORY_DIVISOR;
    uint64_t const SCALED = total_mem == 0 ? FALLBACK : total_mem / MEMORY_DIVISOR;
    uint64_t const MIN_TARGET = std::min<uint64_t>(ker::mod::mm::paging::PAGE_SIZE, max_bytes);
    uint64_t const MAX_TARGET =
        std::max<uint64_t>(MIN_TARGET, (static_cast<uint64_t>(max_bytes) * DIRTY_TARGET_MAX_NUMERATOR) / DIRTY_TARGET_MAX_DENOMINATOR);
    uint64_t const CLAMPED = std::clamp<uint64_t>(SCALED, MIN_TARGET, MAX_TARGET);
    return clamp_u64_to_size(CLAMPED);
}

auto choose_dirty_hard_limit_bytes(size_t target_bytes, size_t max_bytes) -> size_t {
    if (target_bytes == 0) {
        return 0;
    }
    return max_bytes;
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

auto buffer_tree_priority_mix(uint64_t value) -> uint64_t {
    value ^= value >> 33U;
    value *= 0xff51afd7ed558ccdULL;
    value ^= value >> 33U;
    value *= 0xc4ceb9fe1a85ec53ULL;
    value ^= value >> 33U;
    return value == 0 ? 1 : value;
}

auto buffer_tree_priority_for(const BufHead* bh) -> uint64_t {
    auto value = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(bh));
    value ^= bh->block_no;
    value ^= static_cast<uint64_t>(bh->size) << 17U;
    value ^= static_cast<uint64_t>(reinterpret_cast<uintptr_t>(bh->bdev)) >> 4U;
    return buffer_tree_priority_mix(value);
}

auto buffer_tree_priority(const BufHead* bh) -> uint64_t {
    return bh->tree_priority != 0 ? bh->tree_priority : buffer_tree_priority_for(bh);
}

auto find_reclaimable_lru_buffer(size_t scan_budget = SIZE_MAX, bool honor_second_chance = true) -> BufHead* {
    size_t scanned = 0;
    for (BufHead* cur = lru_tail(); cur != nullptr && cur != &lru_sentinel;) {
        if (scanned++ >= scan_budget) {
            break;
        }
        BufHead* prev = cur->lru_prev;
        if (is_reclaimable_clean_buffer(cur)) {  // NOLINT(clang-analyzer-cplusplus.NewDelete): victims are unlinked before delete.
            if (honor_second_chance && (cur->flags & BH_LRU_REFERENCED) != 0) {
                lru_second_chance(cur);
                cur = prev;
                continue;
            }
            return cur;
        }
        cur = prev;
    }
    return nullptr;
}

auto find_reclaimable_mru_buffer(size_t scan_budget = SIZE_MAX, bool honor_second_chance = true) -> BufHead* {
    size_t scanned = 0;
    for (BufHead* cur = lru_head(); cur != nullptr && cur != &lru_sentinel;) {
        if (scanned++ >= scan_budget) {
            break;
        }
        BufHead* next = cur->lru_next;
        if (is_reclaimable_clean_buffer(cur)) {  // NOLINT(clang-analyzer-cplusplus.NewDelete): victims are unlinked before delete.
            if (honor_second_chance && (cur->flags & BH_LRU_REFERENCED) != 0) {
                lru_second_chance(cur);
                cur = next;
                continue;
            }
            return cur;
        }
        cur = next;
    }
    return nullptr;
}

auto cache_allocation_would_exceed_limit_locked(size_t incoming_bytes) -> bool {
    if (incoming_bytes == 0) {
        return false;
    }
    if (incoming_bytes >= cache_max_bytes) {
        return true;
    }
    return cache_total_bytes > cache_max_bytes - incoming_bytes;
}

auto reclaim_clean_cache_locked(size_t target_bytes, size_t byte_budget, size_t victim_budget, size_t scan_budget, bool honor_second_chance)
    -> BufferCacheReclaimStats {
    BufferCacheReclaimStats stats{};
    stats.before_bytes = cache_total_bytes;
    while (cache_total_bytes > target_bytes && stats.freed_bytes < byte_budget && stats.freed_buffers < victim_budget) {
        BufHead* victim = find_reclaimable_lru_buffer(scan_budget, honor_second_chance);
        if (victim == nullptr) {
            // Write-heavy workloads can leave dirty/writeback buffers clustered
            // at the cold end. A bounded hot-end fallback prevents rescanning
            // the same unreclaimable tail while clean buffers elsewhere let the
            // cache run far past its target.
            victim = find_reclaimable_mru_buffer(scan_budget, honor_second_chance);
        }
        if (victim == nullptr) {
            break;
        }
        stats.freed_buffers++;
        stats.freed_bytes += victim->size;
        free_buffer(victim);
    }
    stats.after_bytes = cache_total_bytes;
    return stats;
}

auto reclaim_clean_cache_over_limit_locked() -> BufferCacheReclaimStats {
    if (cache_total_bytes <= cache_max_bytes) {
        return {};
    }
    return reclaim_clean_cache_locked(cache_max_bytes, HOT_EVICT_MAX_BYTES, HOT_EVICT_MAX_VICTIMS, HOT_EVICT_SCAN_BUDGET, false);
}

// Cache misses must keep the logical cache close to its cap. WOS does not yet
// have Linux-style global page-cache reclaim, so leaving old clean-cache debt
// resident can starve later contiguous kernel allocations and slow tree scans.
void evict_lru_for_allocation(size_t incoming_bytes) {
    if (!cache_allocation_would_exceed_limit_locked(incoming_bytes)) {
        return;
    }

    size_t const TARGET_BYTES = incoming_bytes >= cache_max_bytes ? 0 : cache_max_bytes - incoming_bytes;
    size_t const OVERAGE = cache_total_bytes > TARGET_BYTES ? cache_total_bytes - TARGET_BYTES : 0;
    size_t const BYTE_BUDGET = std::clamp(OVERAGE, incoming_bytes, HOT_EVICT_MAX_BYTES);
    static_cast<void>(reclaim_clean_cache_locked(TARGET_BYTES, BYTE_BUDGET, HOT_EVICT_MAX_VICTIMS, HOT_EVICT_SCAN_BUDGET, false));
}

auto alloc_detached_buffer_with_size(dev::BlockDevice* bdev, uint64_t block_no, size_t size, uint32_t initial_flags) -> BufHead* {
    auto* bh = alloc_bufhead();
    if (bh == nullptr) {
        return nullptr;
    }

    uint32_t data_flags = 0;
    auto* data = allocate_buffer_data(size, data_flags);
    if (data == nullptr) {
        free_bufhead(bh);
        return nullptr;
    }

    bh->data = data;
    bh->block_no = block_no;
    bh->bdev = bdev;
    bh->refcount.store(1, std::memory_order_relaxed);
    bh->journal_pending.store(0, std::memory_order_relaxed);
    bh->flags = initial_flags | data_flags;
    bh->size = size;
    bh->tree_priority = buffer_tree_priority_for(bh);
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
    bh->range_left = nullptr;
    bh->range_right = nullptr;
    bh->range_parent = nullptr;
    bh->range_subtree_last_block = 0;

    return bh;
}

void account_buffer_locked(BufHead* bh) {
    if (bh == nullptr) {
        return;
    }
    cache_total_bytes += bh->size;
    cache_total_buffers++;
}

void insert_allocated_buffer_locked(BufHead* bh) {
    account_buffer_locked(bh);
    hash_insert(bh);
    lru_touch(bh);
    reclaim_clean_cache_over_limit_locked();
}

void free_detached_buffer(BufHead* bh) {
    if (bh == nullptr) {
        return;
    }
    free_buffer_data(bh);
    free_bufhead(bh);
}

// Allocate a new BufHead with backing data buffer.
auto alloc_detached_buffer(dev::BlockDevice* bdev, uint64_t block_no) -> BufHead* {
    return alloc_detached_buffer_with_size(bdev, block_no, bdev->block_size, 0);
}

void account_disk_read(BufferReadClass read_class, uint64_t bytes) {
    stat_disk_read_calls.fetch_add(1, std::memory_order_relaxed);
    stat_disk_read_bytes.fetch_add(bytes, std::memory_order_relaxed);
    if (read_class == BufferReadClass::FILESYSTEM_METADATA) {
        stat_metadata_disk_read_calls.fetch_add(1, std::memory_order_relaxed);
        stat_metadata_disk_read_bytes.fetch_add(bytes, std::memory_order_relaxed);
    } else if (read_class == BufferReadClass::FILE_DATA) {
        stat_data_disk_read_calls.fetch_add(1, std::memory_order_relaxed);
        stat_data_disk_read_bytes.fetch_add(bytes, std::memory_order_relaxed);
    }
}

auto read_blocks_with_retry(dev::BlockDevice* bdev, uint64_t block_no, size_t count, uint8_t* data, BufferReadClass read_class) -> int {
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
    uint64_t const BYTES = count * static_cast<uint64_t>(bdev->block_size);
    account_disk_read(read_class, BYTES);
    perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_DISK_READ, STARTED_US, rc, BYTES);
    return rc;
}

// Read the block data from disk into an already-allocated buffer.
auto read_block_from_disk(BufHead* bh, BufferReadClass read_class) -> int {
    int const RC = read_blocks_with_retry(bh->bdev, bh->block_no, 1, bh->data, read_class);
    if (RC == 0) {
        bh->flags |= BH_VALID;
    }
    return RC;
}

auto write_blocks_to_disk(dev::BlockDevice* bdev, uint64_t block_no, size_t block_count, const uint8_t* data, size_t bytes) -> int {
    uint64_t const STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::BUF_DISK_WRITE);
    int const RC = dev::block_write(bdev, block_no, block_count, data);
    perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_DISK_WRITE, STARTED_US, RC, bytes);
    return RC;
}

// Write a stable buffer-data snapshot to disk.
auto write_block_to_disk(BufHead* bh, const uint8_t* data) -> int {
    size_t block_count = bh->size / bh->bdev->block_size;
    if (block_count == 0) {
        block_count = 1;
    }
    return write_blocks_to_disk(bh->bdev, bh->block_no, block_count, data, bh->size);
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

auto dirty_buffers_overlap(const BufHead* lhs, const BufHead* rhs) -> bool {
    if (lhs == nullptr || rhs == nullptr || lhs == rhs || lhs->bdev != rhs->bdev) {
        return false;
    }
    return block_ranges_overlap(lhs->block_no, buffer_block_count(lhs), rhs->block_no, buffer_block_count(rhs));
}

auto range_buffer_last_block(const BufHead* bh) -> uint64_t { return block_range_last_block(bh->block_no, buffer_block_count(bh)); }

auto range_tree_less(const BufHead* lhs, const BufHead* rhs) -> bool {
    uint64_t const LHS_LAST = range_buffer_last_block(lhs);
    uint64_t const RHS_LAST = range_buffer_last_block(rhs);
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

auto range_child_max_last(const BufHead* bh) -> uint64_t { return bh != nullptr ? bh->range_subtree_last_block : 0; }

void range_recompute(BufHead* bh) {
    if (bh == nullptr) {
        return;
    }
    uint64_t max_last = range_buffer_last_block(bh);
    max_last = std::max(max_last, range_child_max_last(bh->range_left));
    max_last = std::max(max_last, range_child_max_last(bh->range_right));
    bh->range_subtree_last_block = max_last;
}

void range_recompute_upwards(BufHead* bh) {
    while (bh != nullptr) {
        range_recompute(bh);
        bh = bh->range_parent;
    }
}

void range_replace_child(RangeBdevState* state, BufHead* old_child, BufHead* new_child) {
    BufHead* parent = old_child->range_parent;
    if (parent == nullptr) {
        state->tree_root = new_child;
    } else if (parent->range_left == old_child) {
        parent->range_left = new_child;
    } else {
        parent->range_right = new_child;
    }
    if (new_child != nullptr) {
        new_child->range_parent = parent;
    }
}

void range_rotate_left(RangeBdevState* state, BufHead* node) {
    BufHead* pivot = node->range_right;
    if (pivot == nullptr) {
        return;
    }

    node->range_right = pivot->range_left;
    if (pivot->range_left != nullptr) {
        pivot->range_left->range_parent = node;
    }

    range_replace_child(state, node, pivot);
    pivot->range_left = node;
    node->range_parent = pivot;

    range_recompute(node);
    range_recompute(pivot);
    range_recompute_upwards(pivot->range_parent);
}

void range_rotate_right(RangeBdevState* state, BufHead* node) {
    BufHead* pivot = node->range_left;
    if (pivot == nullptr) {
        return;
    }

    node->range_left = pivot->range_right;
    if (pivot->range_right != nullptr) {
        pivot->range_right->range_parent = node;
    }

    range_replace_child(state, node, pivot);
    pivot->range_right = node;
    node->range_parent = pivot;

    range_recompute(node);
    range_recompute(pivot);
    range_recompute_upwards(pivot->range_parent);
}

auto find_range_bdev_state_locked(dev::BlockDevice* bdev) -> RangeBdevState* {
    for (auto* state : range_bdev_states) {
        if (state != nullptr && state->bdev == bdev) {
            return state;
        }
    }
    return nullptr;
}

auto find_or_create_range_bdev_state_locked(dev::BlockDevice* bdev) -> RangeBdevState* {
    RangeBdevState* state = find_range_bdev_state_locked(bdev);
    if (state != nullptr) {
        return state;
    }

    state = new RangeBdevState{};
    if (state == nullptr) {
        return nullptr;
    }
    state->bdev = bdev;
    if (!range_bdev_states.push_back(state)) {
        delete state;
        return nullptr;
    }
    return state;
}

void range_tree_insert(RangeBdevState* state, BufHead* bh) {
    bh->range_left = nullptr;
    bh->range_right = nullptr;
    bh->range_parent = nullptr;
    bh->range_subtree_last_block = range_buffer_last_block(bh);

    if (state->tree_root == nullptr) {
        state->tree_root = bh;
        return;
    }

    BufHead* parent = nullptr;
    BufHead* cur = state->tree_root;
    while (cur != nullptr) {
        parent = cur;
        if (range_tree_less(bh, cur)) {
            cur = cur->range_left;
        } else {
            cur = cur->range_right;
        }
    }

    bh->range_parent = parent;
    if (range_tree_less(bh, parent)) {
        parent->range_left = bh;
    } else {
        parent->range_right = bh;
    }
    range_recompute_upwards(parent);

    while (bh->range_parent != nullptr && buffer_tree_priority(bh) < buffer_tree_priority(bh->range_parent)) {
        if (bh->range_parent->range_left == bh) {
            range_rotate_right(state, bh->range_parent);
        } else {
            range_rotate_left(state, bh->range_parent);
        }
    }
}

void range_tree_remove(RangeBdevState* state, BufHead* bh) {
    while (bh->range_left != nullptr || bh->range_right != nullptr) {
        if (bh->range_right == nullptr ||
            (bh->range_left != nullptr && buffer_tree_priority(bh->range_left) < buffer_tree_priority(bh->range_right))) {
            range_rotate_right(state, bh);
        } else {
            range_rotate_left(state, bh);
        }
    }

    BufHead* parent = bh->range_parent;
    if (parent == nullptr) {
        state->tree_root = nullptr;
    } else if (parent->range_left == bh) {
        parent->range_left = nullptr;
    } else if (parent->range_right == bh) {
        parent->range_right = nullptr;
    }
    bh->range_parent = nullptr;
    bh->range_subtree_last_block = 0;
    range_recompute_upwards(parent);
}

void range_index_insert_locked(BufHead* bh) {
    if (bh == nullptr || bh->bdev == nullptr || (bh->flags & BH_RANGE_INDEXED) != 0) {
        return;
    }

    RangeBdevState* state = find_or_create_range_bdev_state_locked(bh->bdev);
    if (state == nullptr) {
        range_index_degraded = true;
        return;
    }

    range_tree_insert(state, bh);
    state->buffers++;
    state->bytes += bh->size;
    bh->flags |= BH_RANGE_INDEXED;
}

void range_index_remove_locked(BufHead* bh) {
    if (bh == nullptr || (bh->flags & BH_RANGE_INDEXED) == 0) {
        return;
    }

    RangeBdevState* state = find_range_bdev_state_locked(bh->bdev);
    if (state != nullptr) {
        range_tree_remove(state, bh);
        if (state->buffers != 0) {
            state->buffers--;
        }
        if (state->bytes >= bh->size) {
            state->bytes -= bh->size;
        } else {
            state->bytes = 0;
        }
    }
    bh->flags &= ~BH_RANGE_INDEXED;
}

auto range_tree_overlaps(const BufHead* root, uint64_t block_no, size_t count) -> bool {
    if (root == nullptr || count == 0) {
        return false;
    }
    uint64_t const QUERY_LAST = block_range_last_block(block_no, count);
    const BufHead* cur = root;
    while (cur != nullptr) {
        if (cur->range_left != nullptr && cur->range_left->range_subtree_last_block >= block_no) {
            cur = cur->range_left;
            continue;
        }
        if (cur->block_no <= QUERY_LAST && range_buffer_last_block(cur) >= block_no) {
            return true;
        }
        if (cur->block_no > QUERY_LAST) {
            return false;
        }
        cur = cur->range_right;
    }
    return false;
}

auto range_find_discard_candidate_locked(BufHead* root, dev::BlockDevice* bdev, uint64_t block_no, size_t count) -> BufHead* {
    if (root == nullptr || bdev == nullptr || count == 0) {
        return nullptr;
    }

    uint64_t const QUERY_LAST = block_range_last_block(block_no, count);
    if (root->range_left != nullptr && root->range_left->range_subtree_last_block >= block_no) {
        if (BufHead* found = range_find_discard_candidate_locked(root->range_left, bdev, block_no, count)) {
            return found;
        }
    }

    if (root->bdev == bdev && root->refcount.load(std::memory_order_relaxed) == 0 && root->block_no <= QUERY_LAST &&
        range_buffer_last_block(root) >= block_no) {
        return root;
    }

    if (root->block_no <= QUERY_LAST) {
        return range_find_discard_candidate_locked(root->range_right, bdev, block_no, count);
    }
    return nullptr;
}

auto cached_buffer_copy_coverage(BufHead* bh, dev::BlockDevice* bdev, uint64_t block_no, size_t count, uint8_t* dst,
                                 std::array<DirtyCoverageInterval, DIRTY_COPY_COVERAGE_MAX_INTERVALS>& coverage, size_t& coverage_count,
                                 bool& coverage_overflow) -> bool {
    if (bh == nullptr || bh->bdev != bdev || bh->data == nullptr || (bh->flags & BH_VALID) == 0 ||
        bh->refcount.load(std::memory_order_relaxed) != 0 || !buffer_overlaps_range(bh, bdev, block_no, count)) {
        return false;
    }

    uint64_t const BUF_LAST = range_buffer_last_block(bh);
    uint64_t const RANGE_LAST = block_range_last_block(block_no, count);
    uint64_t const COPY_FIRST = std::max(bh->block_no, block_no);
    uint64_t const COPY_LAST = std::min(BUF_LAST, RANGE_LAST);
    if (COPY_FIRST > COPY_LAST) {
        return false;
    }

    size_t const BLOCK_SIZE = bdev->block_size;
    auto const COPY_BLOCKS = static_cast<size_t>(COPY_LAST - COPY_FIRST + 1);
    size_t copy_bytes = COPY_BLOCKS * BLOCK_SIZE;
    size_t const SRC_OFF = static_cast<size_t>(COPY_FIRST - bh->block_no) * BLOCK_SIZE;
    size_t const DST_OFF = static_cast<size_t>(COPY_FIRST - block_no) * BLOCK_SIZE;
    size_t const DST_SIZE = count * BLOCK_SIZE;
    if (SRC_OFF >= bh->size || DST_OFF >= DST_SIZE) {
        return false;
    }
    copy_bytes = std::min(copy_bytes, bh->size - SRC_OFF);
    copy_bytes = std::min(copy_bytes, DST_SIZE - DST_OFF);
    if (copy_bytes == 0) {
        return false;
    }

    std::memcpy(dst + DST_OFF, bh->data + SRC_OFF, copy_bytes);
    uint64_t const COVERED_FIRST = block_no + (DST_OFF / BLOCK_SIZE);
    uint64_t const COVERED_LAST = COVERED_FIRST + ((copy_bytes - 1) / BLOCK_SIZE);
    dirty_coverage_add(coverage, coverage_count, DirtyCoverageInterval{.first = COVERED_FIRST, .last = COVERED_LAST}, coverage_overflow);
    return !coverage_overflow;
}

auto cached_buffer_fully_covers(BufHead* bh, dev::BlockDevice* bdev, uint64_t block_no, size_t count) -> bool {
    if (bh == nullptr || bh->bdev != bdev || bh->data == nullptr || (bh->flags & BH_VALID) == 0 ||
        bh->refcount.load(std::memory_order_relaxed) != 0) {
        return false;
    }

    uint64_t const QUERY_LAST = block_range_last_block(block_no, count);
    return bh->block_no <= block_no && range_buffer_last_block(bh) >= QUERY_LAST;
}

void range_find_newest_covering_locked(BufHead* root, dev::BlockDevice* bdev, uint64_t block_no, size_t count, BufHead*& best) {
    if (root == nullptr) {
        return;
    }

    uint64_t const QUERY_LAST = block_range_last_block(block_no, count);
    if (root->range_left != nullptr && root->range_left->range_subtree_last_block >= QUERY_LAST) {
        range_find_newest_covering_locked(root->range_left, bdev, block_no, count, best);
    }

    if (cached_buffer_fully_covers(root, bdev, block_no, count) &&
        (best == nullptr || root->dirty_epoch > best->dirty_epoch || (root->dirty_epoch == best->dirty_epoch && root->size < best->size))) {
        best = root;
    }

    if (root->block_no <= block_no) {
        range_find_newest_covering_locked(root->range_right, bdev, block_no, count, best);
    }
}

void range_copy_cached_overlaps_locked(BufHead* root, dev::BlockDevice* bdev, uint64_t block_no, size_t count, uint8_t* dst,
                                       std::array<DirtyCoverageInterval, DIRTY_COPY_COVERAGE_MAX_INTERVALS>& coverage,
                                       size_t& coverage_count, size_t& visit_count, bool& copied, bool& coverage_overflow) {
    if (root == nullptr || coverage_overflow) {
        return;
    }
    if (visit_count >= RANGE_COPY_MAX_VISITS) {
        coverage_overflow = true;
        return;
    }
    ++visit_count;

    uint64_t const QUERY_LAST = block_range_last_block(block_no, count);

    if (root->range_left != nullptr && root->range_left->range_subtree_last_block >= block_no) {
        range_copy_cached_overlaps_locked(root->range_left, bdev, block_no, count, dst, coverage, coverage_count, visit_count, copied,
                                          coverage_overflow);
        if (coverage_overflow) {
            return;
        }
    }

    bool const DID_COPY = cached_buffer_copy_coverage(root, bdev, block_no, count, dst, coverage, coverage_count, coverage_overflow);
    copied = copied || DID_COPY;
    if (coverage_overflow) {
        return;
    }

    if (root->block_no <= QUERY_LAST) {
        range_copy_cached_overlaps_locked(root->range_right, bdev, block_no, count, dst, coverage, coverage_count, visit_count, copied,
                                          coverage_overflow);
    }
}

auto copy_cached_bdev_range_if_complete_internal(dev::BlockDevice* bdev, uint64_t block_no, size_t count, uint8_t* dst) -> bool {
    if (bdev == nullptr || bdev->block_size == 0 || count == 0 || dst == nullptr || !cache_initialized) {
        return false;
    }
    if (count > SIZE_MAX / bdev->block_size) {
        return false;
    }
    stat_range_copy_attempts.fetch_add(1, std::memory_order_relaxed);
    if (range_index_degraded) {
        stat_range_copy_degraded.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    RangeBdevState* state = find_range_bdev_state_locked(bdev);
    if (state == nullptr || state->buffers == 0) {
        stat_range_copy_no_state.fetch_add(1, std::memory_order_relaxed);
        cache_lock.unlock_irqrestore(IRQFLAGS);
        return false;
    }
    if (!range_tree_overlaps(state->tree_root, block_no, count)) {
        stat_range_copy_no_overlap.fetch_add(1, std::memory_order_relaxed);
        cache_lock.unlock_irqrestore(IRQFLAGS);
        return false;
    }

    BufHead* covering = nullptr;
    range_find_newest_covering_locked(state->tree_root, bdev, block_no, count, covering);
    if (covering != nullptr) {
        size_t const SRC_OFF = static_cast<size_t>(block_no - covering->block_no) * bdev->block_size;
        size_t const COPY_BYTES = count * bdev->block_size;
        std::memcpy(dst, covering->data + SRC_OFF, COPY_BYTES);
        if (cache_dirty_buffers != 0) {
            static_cast<void>(copy_dirty_bdev_range_locked(bdev, block_no, count, dst, covering->dirty_epoch));
        }
        stat_range_copy_cover_hits.fetch_add(1, std::memory_order_relaxed);
        cache_lock.unlock_irqrestore(IRQFLAGS);
        return true;
    }

    bool copied = false;
    bool coverage_overflow = false;
    std::array<DirtyCoverageInterval, DIRTY_COPY_COVERAGE_MAX_INTERVALS> coverage{};
    size_t coverage_count = 0;
    size_t visit_count = 0;
    range_copy_cached_overlaps_locked(state->tree_root, bdev, block_no, count, dst, coverage, coverage_count, visit_count, copied,
                                      coverage_overflow);
    bool const COMPLETE = copied && !coverage_overflow && dirty_coverage_complete(coverage, coverage_count, block_no, count);
    if (COMPLETE && cache_dirty_buffers != 0) {
        static_cast<void>(copy_dirty_bdev_range_locked(bdev, block_no, count, dst, 0));
    }
    if (COMPLETE) {
        stat_range_copy_overlap_hits.fetch_add(1, std::memory_order_relaxed);
    } else if (coverage_overflow) {
        stat_range_copy_overflow.fetch_add(1, std::memory_order_relaxed);
    } else {
        stat_range_copy_incomplete.fetch_add(1, std::memory_order_relaxed);
    }
    cache_lock.unlock_irqrestore(IRQFLAGS);
    return COMPLETE;
}

auto range_find_clean_overlapping_alias_locked(BufHead* root, const BufHead* source) -> BufHead* {
    if (root == nullptr || source == nullptr) {
        return nullptr;
    }
    uint64_t const SOURCE_LAST = range_buffer_last_block(source);
    if (root->range_left != nullptr && root->range_left->range_subtree_last_block >= source->block_no) {
        if (BufHead* found = range_find_clean_overlapping_alias_locked(root->range_left, source)) {
            return found;
        }
    }

    uint32_t constexpr BLOCKING_FLAGS = BH_DIRTY | BH_WRITEBACK;
    if (root != source && root->bdev == source->bdev && (root->flags & BLOCKING_FLAGS) == 0 &&
        root->refcount.load(std::memory_order_relaxed) == 0 &&
        block_ranges_overlap(root->block_no, buffer_block_count(root), source->block_no, buffer_block_count(source))) {
        return root;
    }

    if (root->block_no <= SOURCE_LAST) {
        return range_find_clean_overlapping_alias_locked(root->range_right, source);
    }
    return nullptr;
}

void discard_clean_overlapping_aliases_locked(BufHead* source) {
    if (source == nullptr || source->bdev == nullptr || range_index_degraded || (source->flags & BH_RANGE_INDEXED) == 0) {
        return;
    }

    RangeBdevState* state = find_range_bdev_state_locked(source->bdev);
    if (state == nullptr || state->buffers <= 1) {
        return;
    }

    uint64_t discarded_bytes = 0;
    size_t discarded_buffers = 0;
    while (BufHead* alias = range_find_clean_overlapping_alias_locked(state->tree_root, source)) {
        discarded_bytes += alias->size;
        free_buffer(alias);
        ++discarded_buffers;
        if (discarded_buffers >= CLEAN_ALIAS_DISCARD_MAX_BUFFERS || discarded_bytes >= CLEAN_ALIAS_DISCARD_MAX_BYTES) {
            break;
        }
        state = find_range_bdev_state_locked(source->bdev);
        if (state == nullptr || state->buffers <= 1) {
            break;
        }
    }
    if (discarded_bytes != 0) {
        perf_record_xfs_count(ker::mod::perf::WkiPerfLocalXfsOp::BUF_DISCARD, discarded_bytes);
    }
}

auto cached_alias_can_overlay_writeback(const BufHead* alias, const BufHead* source, uint64_t source_epoch) -> bool {
    if (alias == nullptr || source == nullptr || alias == source || alias->bdev != source->bdev || alias->data == nullptr ||
        source->bdev == nullptr || source->bdev->block_size == 0 || (alias->flags & BH_VALID) == 0 || alias->dirty_epoch <= source_epoch) {
        return false;
    }
    return block_ranges_overlap(alias->block_no, buffer_block_count(alias), source->block_no, buffer_block_count(source));
}

void overlay_cached_alias_into_snapshot(const BufHead* alias, const BufHead* source, uint8_t* dst) {
    if (dst == nullptr || !cached_alias_can_overlay_writeback(alias, source, 0)) {
        return;
    }

    uint64_t const ALIAS_LAST = range_buffer_last_block(alias);
    uint64_t const SOURCE_LAST = range_buffer_last_block(source);
    uint64_t const COPY_FIRST = std::max(alias->block_no, source->block_no);
    uint64_t const COPY_LAST = std::min(ALIAS_LAST, SOURCE_LAST);
    if (COPY_FIRST > COPY_LAST) {
        return;
    }

    size_t const BLOCK_SIZE = source->bdev->block_size;
    auto const COPY_BLOCKS = static_cast<size_t>(COPY_LAST - COPY_FIRST + 1);
    size_t copy_bytes = COPY_BLOCKS * BLOCK_SIZE;
    size_t const SRC_OFF = static_cast<size_t>(COPY_FIRST - alias->block_no) * BLOCK_SIZE;
    size_t const DST_OFF = static_cast<size_t>(COPY_FIRST - source->block_no) * BLOCK_SIZE;
    if (SRC_OFF >= alias->size || DST_OFF >= source->size) {
        return;
    }

    copy_bytes = std::min(copy_bytes, alias->size - SRC_OFF);
    copy_bytes = std::min(copy_bytes, source->size - DST_OFF);
    if (copy_bytes == 0) {
        return;
    }

    std::memcpy(dst + DST_OFF, alias->data + SRC_OFF, copy_bytes);
}

void consider_newer_cached_alias(const BufHead* alias, const BufHead* source, uint64_t min_epoch_exclusive, const BufHead*& best) {
    if (!cached_alias_can_overlay_writeback(alias, source, min_epoch_exclusive)) {
        return;
    }
    if (best == nullptr || alias->dirty_epoch < best->dirty_epoch ||
        (alias->dirty_epoch == best->dirty_epoch && range_tree_less(alias, best))) {
        best = alias;
    }
}

void range_consider_newer_cached_aliases_locked(BufHead* root, const BufHead* source, uint64_t min_epoch_exclusive, const BufHead*& best) {
    if (root == nullptr || source == nullptr) {
        return;
    }

    uint64_t const SOURCE_LAST = range_buffer_last_block(source);
    if (root->range_left != nullptr && root->range_left->range_subtree_last_block >= source->block_no) {
        range_consider_newer_cached_aliases_locked(root->range_left, source, min_epoch_exclusive, best);
    }

    consider_newer_cached_alias(root, source, min_epoch_exclusive, best);

    if (root->block_no <= SOURCE_LAST) {
        range_consider_newer_cached_aliases_locked(root->range_right, source, min_epoch_exclusive, best);
    }
}

auto find_oldest_newer_cached_alias_locked(const BufHead* source, uint64_t min_epoch_exclusive) -> const BufHead* {
    if (range_index_degraded) {
        const BufHead* best = nullptr;
        for (auto* bucket_head : hash_buckets) {
            for (BufHead* bh = bucket_head; bh != nullptr; bh = bh->hash_next) {
                consider_newer_cached_alias(bh, source, min_epoch_exclusive, best);
            }
        }
        return best;
    }

    RangeBdevState* state = find_range_bdev_state_locked(source->bdev);
    if (state == nullptr || state->buffers <= 1 || !range_tree_overlaps(state->tree_root, source->block_no, buffer_block_count(source))) {
        return nullptr;
    }

    const BufHead* best = nullptr;
    range_consider_newer_cached_aliases_locked(state->tree_root, source, min_epoch_exclusive, best);
    return best;
}

void overlay_newer_cached_aliases_locked(const BufHead* source, uint64_t source_epoch, uint8_t* dst) {
    if (source == nullptr || source->bdev == nullptr || dst == nullptr) {
        return;
    }

    uint64_t min_epoch = source_epoch;
    while (const BufHead* alias = find_oldest_newer_cached_alias_locked(source, min_epoch)) {
        overlay_cached_alias_into_snapshot(alias, source, dst);
        min_epoch = alias->dirty_epoch;
    }
}

void overlay_newer_cached_aliases(BufHead const* source, uint64_t source_epoch, uint8_t* dst) {
    if (source == nullptr || dst == nullptr || source_epoch == 0 || !cache_initialized) {
        return;
    }

    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    overlay_newer_cached_aliases_locked(source, source_epoch, dst);
    cache_lock.unlock_irqrestore(IRQFLAGS);
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

void dirty_list_move_tail(DirtyBdevState* state, BufHead* bh) {
    if (state == nullptr || bh == nullptr || state->list_tail == bh) {
        return;
    }
    dirty_list_remove(state, bh);
    dirty_list_insert(state, bh);
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

    while (bh->dirty_parent != nullptr && buffer_tree_priority(bh) < buffer_tree_priority(bh->dirty_parent)) {
        if (bh->dirty_parent->dirty_left == bh) {
            dirty_rotate_right(state, bh->dirty_parent);
        } else {
            dirty_rotate_left(state, bh->dirty_parent);
        }
    }
}

void dirty_tree_remove(DirtyBdevState* state, BufHead* bh) {
    while (bh->dirty_left != nullptr || bh->dirty_right != nullptr) {
        if (bh->dirty_right == nullptr ||
            (bh->dirty_left != nullptr && buffer_tree_priority(bh->dirty_left) < buffer_tree_priority(bh->dirty_right))) {
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

auto dirty_tree_has_older_overlapping(const BufHead* root, const BufHead* target) -> bool {
    if (root == nullptr || target == nullptr) {
        return false;
    }

    uint64_t const TARGET_LAST = dirty_buffer_last_block(target);
    if (root->dirty_left != nullptr && root->dirty_left->dirty_subtree_last_block >= target->block_no) {
        if (dirty_tree_has_older_overlapping(root->dirty_left, target)) {
            return true;
        }
    }

    if (dirty_buffers_overlap(root, target) && root->dirty_epoch < target->dirty_epoch) {
        return true;
    }

    if (root->block_no <= TARGET_LAST) {
        return dirty_tree_has_older_overlapping(root->dirty_right, target);
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
    if (bh == nullptr || (bh->flags & BH_DIRTY) == 0 || bh->journal_pending.load(std::memory_order_acquire) != 0) {
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

void dirty_consider_candidate(BufHead* bh, const DirtyWritebackFilter& filter, uint64_t min_epoch_exclusive, uint64_t max_epoch_inclusive,
                              BufHead*& best) {
    if (!dirty_filter_matches(bh, filter) || bh->dirty_epoch <= min_epoch_exclusive || bh->dirty_epoch > max_epoch_inclusive) {
        return;
    }
    if (best == nullptr || bh->dirty_epoch < best->dirty_epoch || (bh->dirty_epoch == best->dirty_epoch && dirty_tree_less(bh, best))) {
        best = bh;
    }
}

void dirty_tree_consider_overlapping(BufHead* root, const DirtyWritebackFilter& filter, uint64_t min_epoch_exclusive,
                                     uint64_t max_epoch_inclusive, BufHead*& best) {
    if (root == nullptr || !filter.use_range || filter.bdev == nullptr) {
        return;
    }

    uint64_t const QUERY_LAST = block_range_last_block(filter.block_no, filter.count);

    if (root->dirty_left != nullptr && root->dirty_left->dirty_subtree_last_block >= filter.block_no) {
        dirty_tree_consider_overlapping(root->dirty_left, filter, min_epoch_exclusive, max_epoch_inclusive, best);
    }

    dirty_consider_candidate(root, filter, min_epoch_exclusive, max_epoch_inclusive, best);

    if (root->block_no <= QUERY_LAST) {
        dirty_tree_consider_overlapping(root->dirty_right, filter, min_epoch_exclusive, max_epoch_inclusive, best);
    }
}

void dirty_tree_consider_starting_at(BufHead* root, uint64_t block_no, const DirtyWritebackFilter& filter, uint64_t min_epoch_exclusive,
                                     uint64_t max_epoch_inclusive, BufHead*& best) {
    if (root == nullptr) {
        return;
    }

    if (root->block_no >= block_no) {
        dirty_tree_consider_starting_at(root->dirty_left, block_no, filter, min_epoch_exclusive, max_epoch_inclusive, best);
    }

    if (root->block_no == block_no) {
        dirty_consider_candidate(root, filter, min_epoch_exclusive, max_epoch_inclusive, best);
    }

    if (root->block_no <= block_no) {
        dirty_tree_consider_starting_at(root->dirty_right, block_no, filter, min_epoch_exclusive, max_epoch_inclusive, best);
    }
}

auto first_matching_dirty_from_ordered_list_locked(DirtyBdevState* state, const DirtyWritebackFilter& filter, uint64_t min_epoch_exclusive,
                                                   uint64_t max_epoch_inclusive) -> BufHead* {
    if (state == nullptr || filter.use_range) {
        return nullptr;
    }
    for (BufHead* bh = state->list_head; bh != nullptr; bh = bh->dirty_next) {
        if (bh->dirty_epoch <= min_epoch_exclusive) {
            continue;
        }
        if (bh->dirty_epoch > max_epoch_inclusive) {
            return nullptr;
        }
        if (dirty_filter_matches(bh, filter)) {
            return bh;
        }
    }
    return nullptr;
}

auto find_oldest_matching_dirty_buffer_locked(const DirtyWritebackFilter& filter, uint64_t min_epoch_exclusive,
                                              uint64_t max_epoch_inclusive = UINT64_MAX) -> BufHead* {
    if (!dirty_filter_may_have_match_locked(filter)) {
        return nullptr;
    }

    BufHead* best = nullptr;
    if (dirty_index_degraded) {
        for (auto* bucket_head : hash_buckets) {
            for (BufHead* bh = bucket_head; bh != nullptr; bh = bh->hash_next) {
                dirty_consider_candidate(bh, filter, min_epoch_exclusive, max_epoch_inclusive, best);
            }
        }
        return best;
    }

    if (!filter.use_range) {
        BufHead* best = nullptr;
        if (filter.bdev != nullptr) {
            return first_matching_dirty_from_ordered_list_locked(find_dirty_bdev_state_locked(filter.bdev), filter, min_epoch_exclusive,
                                                                 max_epoch_inclusive);
        }
        for (auto* state : dirty_bdev_states) {
            BufHead* candidate = first_matching_dirty_from_ordered_list_locked(state, filter, min_epoch_exclusive, max_epoch_inclusive);
            if (best == nullptr || (candidate != nullptr && candidate->dirty_epoch < best->dirty_epoch)) {
                best = candidate;
            }
        }
        return best;
    }

    if (filter.bdev != nullptr && filter.use_range) {
        DirtyBdevState* state = find_dirty_bdev_state_locked(filter.bdev);
        if (state == nullptr || state->dirty_buffers == 0 || !dirty_tree_overlaps(state->tree_root, filter.block_no, filter.count)) {
            return nullptr;
        }
        dirty_tree_consider_overlapping(state->tree_root, filter, min_epoch_exclusive, max_epoch_inclusive, best);
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
            dirty_consider_candidate(bh, filter, min_epoch_exclusive, max_epoch_inclusive, best);
        }
    }
    return best;
}

auto max_matching_dirty_epoch_locked(const DirtyWritebackFilter& filter) -> uint64_t {
    uint64_t max_epoch = 0;

    if (dirty_index_degraded) {
        for (auto* bucket_head : hash_buckets) {
            for (BufHead* bh = bucket_head; bh != nullptr; bh = bh->hash_next) {
                if (dirty_filter_matches(bh, filter)) {
                    max_epoch = std::max(max_epoch, bh->dirty_epoch);
                }
            }
        }
        return max_epoch;
    }

    if (!filter.use_range) {
        if (filter.bdev != nullptr) {
            DirtyBdevState* state = find_dirty_bdev_state_locked(filter.bdev);
            return (state != nullptr && state->list_tail != nullptr) ? state->list_tail->dirty_epoch : 0;
        }
        for (auto* state : dirty_bdev_states) {
            if (state != nullptr && state->list_tail != nullptr) {
                max_epoch = std::max(max_epoch, state->list_tail->dirty_epoch);
            }
        }
        return max_epoch;
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
            if (dirty_filter_matches(bh, filter)) {
                max_epoch = std::max(max_epoch, bh->dirty_epoch);
            }
        }
    }
    return max_epoch;
}

auto max_matching_dirty_epoch(const DirtyWritebackFilter& filter) -> uint64_t {
    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    uint64_t const MAX_EPOCH = max_matching_dirty_epoch_locked(filter);
    cache_lock.unlock_irqrestore(IRQFLAGS);
    return MAX_EPOCH;
}

auto has_older_overlapping_dirty_buffer_locked(const BufHead* target) -> bool {
    if (target == nullptr || (target->flags & BH_DIRTY) == 0) {
        return false;
    }

    if (dirty_index_degraded) {
        for (auto* bucket_head : hash_buckets) {
            for (BufHead* bh = bucket_head; bh != nullptr; bh = bh->hash_next) {
                if ((bh->flags & BH_DIRTY) != 0 && dirty_buffers_overlap(bh, target) && bh->dirty_epoch < target->dirty_epoch) {
                    return true;
                }
            }
        }
        return false;
    }

    DirtyBdevState* state = find_dirty_bdev_state_locked(target->bdev);
    return state != nullptr && dirty_tree_has_older_overlapping(state->tree_root, target);
}

auto find_writeback_candidate_locked(const DirtyWritebackFilter& filter, uint64_t min_epoch_exclusive, uint64_t max_epoch_inclusive,
                                     bool skip_writeback) -> BufHead* {
    uint64_t search_min_epoch = min_epoch_exclusive;
    while (true) {
        BufHead* bh = find_oldest_matching_dirty_buffer_locked(filter, search_min_epoch, max_epoch_inclusive);
        if (bh == nullptr) {
            return nullptr;
        }
        if (skip_writeback && (bh->flags & BH_WRITEBACK) != 0) {
            search_min_epoch = bh->dirty_epoch;
            continue;
        }
        if (!has_older_overlapping_dirty_buffer_locked(bh)) {
            return bh;
        }
        search_min_epoch = bh->dirty_epoch;
    }
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
    if (cache_dirty_bytes > dirty_throttle_resume_bytes_locked()) {
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

auto register_current_dirty_waiter_locked() -> bool {
    if (!ker::mod::sched::can_query_current_task()) {
        return false;
    }
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || task->pid == 0 || task->type != ker::mod::sched::task::TaskType::PROCESS) {
        return false;
    }
    if (dirty_waiters.contains(task->pid)) {
        return true;
    }
    return dirty_waiters.push_back(task->pid);
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
    discard_clean_overlapping_aliases_locked(bh);
    if ((bh->flags & BH_DIRTY) != 0) {
        if ((bh->flags & BH_DIRTY_INDEXED) != 0) {
            dirty_list_move_tail(find_dirty_bdev_state_locked(bh->bdev), bh);
        }
        return false;
    }

    bh->flags |= BH_DIRTY;
    cache_dirty_buffers++;
    cache_dirty_bytes += bh->size;
    static_cast<void>(dirty_index_insert_locked(bh));
    return true;
}

auto write_buffer_snapshot_for_epoch(BufHead* bh, uint64_t writeback_dirty_epoch, uint64_t owned_writeback_epoch) -> int {
    WritebackSnapshot snapshot = make_writeback_snapshot_for_epoch(bh, writeback_dirty_epoch);
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

auto dirty_writeback_run_can_append_locked(const DirtyWritebackRun& run, const BufHead* bh, const DirtyWritebackFilter& filter,
                                           uint64_t min_epoch_exclusive, uint64_t max_epoch_inclusive) -> bool {
    if (bh == nullptr || !dirty_filter_matches(bh, filter) || bh->dirty_epoch <= min_epoch_exclusive ||
        bh->dirty_epoch > max_epoch_inclusive || (bh->flags & BH_WRITEBACK) != 0 || has_older_overlapping_dirty_buffer_locked(bh) ||
        bh->bdev == nullptr || bh->bdev->block_size == 0 || bh->data == nullptr || bh->size == 0 ||
        (bh->size % bh->bdev->block_size) != 0) {
        return false;
    }

    size_t const BLOCK_COUNT = bh->size / bh->bdev->block_size;
    if (BLOCK_COUNT == 0) {
        return false;
    }
    if (run.count == 0) {
        return true;
    }
    if (run.count >= run.buffers.size() || bh->bdev != run.bdev || run.block_count > UINT64_MAX - run.block_no) {
        return false;
    }
    uint64_t const NEXT_BLOCK = run.block_no + static_cast<uint64_t>(run.block_count);
    return bh->block_no == NEXT_BLOCK && run.bytes < DIRTY_WRITEBACK_RUN_MAX_BYTES &&
           bh->size <= DIRTY_WRITEBACK_RUN_MAX_BYTES - run.bytes && run.block_count <= SIZE_MAX - BLOCK_COUNT;
}

auto find_next_contiguous_dirty_buffer_locked(const DirtyWritebackRun& run, const DirtyWritebackFilter& filter,
                                              uint64_t min_epoch_exclusive, uint64_t max_epoch_inclusive) -> BufHead* {
    if (dirty_index_degraded || run.count == 0 || run.bdev == nullptr || run.block_count > UINT64_MAX - run.block_no) {
        return nullptr;
    }

    DirtyBdevState* state = find_dirty_bdev_state_locked(run.bdev);
    if (state == nullptr || state->dirty_buffers == 0) {
        return nullptr;
    }

    uint64_t const NEXT_BLOCK = run.block_no + static_cast<uint64_t>(run.block_count);
    BufHead* best = nullptr;
    dirty_tree_consider_starting_at(state->tree_root, NEXT_BLOCK, filter, min_epoch_exclusive, max_epoch_inclusive, best);
    if (!dirty_writeback_run_can_append_locked(run, best, filter, min_epoch_exclusive, max_epoch_inclusive)) {
        return nullptr;
    }
    return best;
}

void dirty_writeback_run_append_locked(DirtyWritebackRun& run, BufHead* bh, uint64_t writeback_epoch) {
    if (run.count == 0) {
        run.bdev = bh->bdev;
        run.block_no = bh->block_no;
        run.writeback_epoch = writeback_epoch;
    }

    size_t const BLOCK_COUNT = bh->size / bh->bdev->block_size;
    mark_buffer_writeback(bh, writeback_epoch);
    run.buffers.at(run.count) = bh;
    run.dirty_epochs.at(run.count) = bh->writeback_dirty_epoch;
    run.block_count += BLOCK_COUNT;
    run.bytes += bh->size;
    run.count++;
    bh->refcount.fetch_add(1, std::memory_order_relaxed);
}

auto collect_dirty_writeback_run_locked(const DirtyWritebackFilter& filter, uint64_t min_epoch_exclusive, uint64_t max_epoch_inclusive,
                                        DirtyWritebackRun& run, bool skip_writeback) -> DirtyWritebackResult {
    DirtyWritebackResult result{};
    BufHead* bh = find_writeback_candidate_locked(filter, min_epoch_exclusive, max_epoch_inclusive, skip_writeback);
    if (bh == nullptr) {
        return result;
    }
    if ((bh->flags & BH_WRITEBACK) != 0) {
        result.busy = true;
        return result;
    }
    uint64_t const WRITEBACK_EPOCH = allocate_writeback_epoch();
    if (!dirty_writeback_run_can_append_locked(run, bh, filter, min_epoch_exclusive, max_epoch_inclusive)) {
        return result;
    }
    dirty_writeback_run_append_locked(run, bh, WRITEBACK_EPOCH);

    if (dirty_index_degraded) {
        for (BufHead* next = bh->dirty_next; next != nullptr; next = next->dirty_next) {
            if (!dirty_writeback_run_can_append_locked(run, next, filter, min_epoch_exclusive, max_epoch_inclusive)) {
                break;
            }
            dirty_writeback_run_append_locked(run, next, WRITEBACK_EPOCH);
        }
    } else {
        while (BufHead* next = find_next_contiguous_dirty_buffer_locked(run, filter, min_epoch_exclusive, max_epoch_inclusive)) {
            dirty_writeback_run_append_locked(run, next, WRITEBACK_EPOCH);
        }
    }

    result.wrote = true;
    result.buffers = run.count;
    result.bytes = run.bytes;
    result.dirty_epoch = run.dirty_epochs.at(0);
    return result;
}

auto dirty_writeback_run_max_dirty_epoch(const DirtyWritebackRun& run) -> uint64_t {
    uint64_t max_epoch = 0;
    for (size_t i = 0; i < run.count; ++i) {
        max_epoch = std::max(max_epoch, run.dirty_epochs.at(i));
    }
    return max_epoch;
}

auto make_writeback_run_snapshot(const DirtyWritebackRun& run) -> WritebackSnapshot {
    WritebackSnapshot snapshot{};
    snapshot.data = allocate_buffer_data(run.bytes, snapshot.flags);
    if (snapshot.data == nullptr) {
        return snapshot;
    }
    snapshot.size = run.bytes;

    size_t offset = 0;
    for (size_t i = 0; i < run.count; ++i) {
        const BufHead* bh = run.buffers.at(i);
        memcpy(snapshot.data + offset, bh->data, bh->size);
        overlay_newer_cached_aliases(bh, run.dirty_epochs.at(i), snapshot.data + offset);
        offset += bh->size;
    }
    return snapshot;
}

void finish_dirty_writeback_run(DirtyWritebackRun& run, int rc) {
    DirtyWakeList wake_list{};
    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    for (size_t i = 0; i < run.count; ++i) {
        BufHead* bh = run.buffers.at(i);
        if (bh == nullptr) {
            continue;
        }
        if (rc == 0 && (bh->flags & BH_DIRTY) != 0 && bh->dirty_epoch == run.dirty_epochs.at(i)) {
            clear_buffer_dirty_locked(bh);
        }
        if (owns_writeback_epoch(bh, run.writeback_epoch)) {
            clear_buffer_writeback(bh);
        }
    }
    if (rc == 0) {
        collect_dirty_waiters_locked(wake_list);
    }
    cache_lock.unlock_irqrestore(IRQFLAGS);
    wake_dirty_waiters(wake_list);
}

auto write_dirty_run_buffers_individually(DirtyWritebackRun& run) -> int {
    int result = 0;
    DirtyWakeList wake_list{};
    for (size_t i = 0; i < run.count; ++i) {
        BufHead* bh = run.buffers.at(i);
        if (bh == nullptr) {
            continue;
        }

        int rc = -ENOMEM;
        WritebackSnapshot snapshot = make_writeback_snapshot_for_epoch(bh, run.dirty_epochs.at(i));
        if (snapshot.data != nullptr) {
            rc = write_block_to_disk(bh, snapshot.data);
            free_writeback_snapshot(snapshot);
        }
        if (rc != 0 && result == 0) {
            result = rc;
        }

        uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
        if (rc == 0 && (bh->flags & BH_DIRTY) != 0 && bh->dirty_epoch == run.dirty_epochs.at(i)) {
            clear_buffer_dirty_locked(bh);
            collect_dirty_waiters_locked(wake_list);
        }
        if (owns_writeback_epoch(bh, run.writeback_epoch)) {
            clear_buffer_writeback(bh);
        }
        cache_lock.unlock_irqrestore(IRQFLAGS);
    }
    wake_dirty_waiters(wake_list);
    return result;
}

auto write_dirty_run_snapshot(DirtyWritebackRun& run) -> int {
    WritebackSnapshot snapshot = make_writeback_run_snapshot(run);
    if (snapshot.data == nullptr) {
        if (run.count > 1) {
            return write_dirty_run_buffers_individually(run);
        }
        finish_dirty_writeback_run(run, -ENOMEM);
        return -ENOMEM;
    }

    int const RC = write_blocks_to_disk(run.bdev, run.block_no, run.block_count, snapshot.data, run.bytes);
    free_writeback_snapshot(snapshot);
    finish_dirty_writeback_run(run, RC);
    return RC;
}

auto writeback_dirty_one_after(const DirtyWritebackFilter& filter, uint64_t min_epoch_exclusive, uint64_t max_epoch_inclusive = UINT64_MAX,
                               bool skip_writeback = false) -> DirtyWritebackResult {
    DirtyWritebackRun run{};

    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    DirtyWritebackResult result = collect_dirty_writeback_run_locked(filter, min_epoch_exclusive, max_epoch_inclusive, run, skip_writeback);
    cache_lock.unlock_irqrestore(IRQFLAGS);

    if (!result.wrote) {
        return result;
    }

    result.status = write_dirty_run_snapshot(run);
    result.dirty_epoch = result.status == 0 ? run.dirty_epochs.at(0) : dirty_writeback_run_max_dirty_epoch(run);
    for (size_t i = 0; i < run.count; ++i) {
        brelse(run.buffers.at(i));
    }
    if (result.status == 0) {
        uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
        BufferCacheReclaimStats const RECLAIM = reclaim_clean_cache_over_limit_locked();
        cache_lock.unlock_irqrestore(IRQFLAGS);
        if (RECLAIM.freed_bytes != 0) {
            drain_deferred_data_buffer_frees();
        }
    }
    return result;
}

auto writeback_dirty_one(const DirtyWritebackFilter& filter) -> DirtyWritebackResult {
    return writeback_dirty_one_after(filter, 0, UINT64_MAX, true);
}

auto writeback_dirty_budgeted(const DirtyWritebackFilter& filter, size_t buffer_budget, size_t byte_budget) -> int {
    int result = 0;
    uint64_t min_epoch = 0;
    uint64_t const MAX_EPOCH = max_matching_dirty_epoch(filter);
    size_t written_buffers = 0;
    size_t written_bytes = 0;
    while (written_buffers < buffer_budget && written_bytes < byte_budget) {
        DirtyWritebackResult const WB = writeback_dirty_one_after(filter, min_epoch, MAX_EPOCH, true);
        if (WB.wrote) {
            min_epoch = WB.dirty_epoch;
            written_buffers += std::max(WB.buffers, static_cast<size_t>(1));
            written_bytes += WB.bytes;
            if (WB.status != 0) {
                result = WB.status;
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

auto copy_dirty_bdev_range_locked(dev::BlockDevice* bdev, uint64_t block_no, size_t count, uint8_t* dst, uint64_t min_epoch_exclusive)
    -> bool {
    DirtyWritebackFilter filter{};
    filter.bdev = bdev;
    filter.block_no = block_no;
    filter.count = count;
    filter.use_range = true;

    bool copied = false;
    uint64_t min_epoch = min_epoch_exclusive;
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
    return copied;
}

auto dirty_intervals_touch_or_overlap(const DirtyCoverageInterval& lhs, const DirtyCoverageInterval& rhs) -> bool {
    bool const LHS_BEFORE = lhs.last != UINT64_MAX && lhs.last + 1 < rhs.first;
    bool const RHS_BEFORE = rhs.last != UINT64_MAX && rhs.last + 1 < lhs.first;
    return !LHS_BEFORE && !RHS_BEFORE;
}

void dirty_coverage_add(std::array<DirtyCoverageInterval, DIRTY_COPY_COVERAGE_MAX_INTERVALS>& intervals, size_t& interval_count,
                        DirtyCoverageInterval interval, bool& overflow) {
    if (overflow || interval.first > interval.last) {
        return;
    }

    for (size_t i = 0; i < interval_count;) {
        if (!dirty_intervals_touch_or_overlap(intervals.at(i), interval)) {
            i++;
            continue;
        }
        interval.first = std::min(interval.first, intervals.at(i).first);
        interval.last = std::max(interval.last, intervals.at(i).last);
        intervals.at(i) = intervals.at(interval_count - 1);
        interval_count--;
    }

    if (interval_count >= intervals.size()) {
        overflow = true;
        return;
    }

    intervals.at(interval_count++) = interval;
}

auto dirty_coverage_complete(const std::array<DirtyCoverageInterval, DIRTY_COPY_COVERAGE_MAX_INTERVALS>& intervals, size_t interval_count,
                             uint64_t block_no, size_t count) -> bool {
    uint64_t const RANGE_LAST = block_range_last_block(block_no, count);
    for (size_t i = 0; i < interval_count; ++i) {
        if (intervals.at(i).first <= block_no && intervals.at(i).last >= RANGE_LAST) {
            return true;
        }
    }
    return false;
}

auto copy_dirty_bdev_range_if_complete_locked(dev::BlockDevice* bdev, uint64_t block_no, size_t count, uint8_t* dst) -> bool {
    if (dirty_index_degraded) {
        return false;
    }

    DirtyWritebackFilter filter{};
    filter.bdev = bdev;
    filter.block_no = block_no;
    filter.count = count;
    filter.use_range = true;

    bool copied = false;
    bool coverage_overflow = false;
    uint64_t min_epoch = 0;
    std::array<DirtyCoverageInterval, DIRTY_COPY_COVERAGE_MAX_INTERVALS> coverage{};
    size_t coverage_count = 0;

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
        if (copy_bytes == 0) {
            continue;
        }

        std::memcpy(dst + DST_OFF, bh->data + SRC_OFF, copy_bytes);
        copied = true;

        uint64_t const COVERED_FIRST = block_no + (DST_OFF / BLOCK_SIZE);
        uint64_t const COVERED_LAST = COVERED_FIRST + ((copy_bytes - 1) / BLOCK_SIZE);
        dirty_coverage_add(coverage, coverage_count, DirtyCoverageInterval{.first = COVERED_FIRST, .last = COVERED_LAST},
                           coverage_overflow);
        if (coverage_overflow) {
            return false;
        }
    }

    return copied && dirty_coverage_complete(coverage, coverage_count, block_no, count);
}

auto copy_dirty_bdev_range_after_epoch(dev::BlockDevice* bdev, uint64_t block_no, size_t count, uint8_t* dst, uint64_t min_epoch_exclusive)
    -> bool {
    if (bdev == nullptr || bdev->block_size == 0 || count == 0 || dst == nullptr || !cache_initialized) {
        return false;
    }
    if (count > SIZE_MAX / bdev->block_size) {
        return false;
    }

    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    bool const COPIED = copy_dirty_bdev_range_locked(bdev, block_no, count, dst, min_epoch_exclusive);
    cache_lock.unlock_irqrestore(IRQFLAGS);
    return COPIED;
}

auto copy_dirty_bdev_range_for_cached_buffer_locked(BufHead* bh, uint64_t block_no, size_t count) -> bool {
    if (bh == nullptr || bh->bdev == nullptr || bh->bdev->block_size == 0 || count == 0 || bh->data == nullptr || !cache_initialized) {
        return false;
    }
    if (count > SIZE_MAX / bh->bdev->block_size) {
        return false;
    }
    if (cache_dirty_buffers == 0 || (((bh->flags & BH_DIRTY) != 0) && cache_dirty_buffers == 1)) {
        return false;
    }

    uint64_t min_epoch = 0;
    if ((bh->flags & BH_DIRTY) != 0) {
        min_epoch = bh->dirty_epoch;
    }
    return copy_dirty_bdev_range_locked(bh->bdev, block_no, count, bh->data, min_epoch);
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
    size_t buffers_since_yield = 0;
    size_t bytes_since_yield = 0;
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
            buffers_since_yield += std::max(WB.buffers, static_cast<size_t>(1));
            bytes_since_yield += WB.bytes;
            if (buffers_since_yield >= DIRTY_WRITEBACK_BUDGET || bytes_since_yield >= DIRTY_WRITEBACK_YIELD_BYTES) {
                buffers_since_yield = 0;
                bytes_since_yield = 0;
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

    // Kernel producers do not necessarily cross a syscall-return boundary.
    // This worker is now outside the cache lock and any filesystem call, so it
    // is a safe fallback point for enforcing the deferred vmap-free cap.
    drain_deferred_data_buffer_frees_if_over_limit();
}

}  // anonymous namespace

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

void buffer_cache_init() {
    if (cache_initialized) {
        return;
    }
    uint64_t const TOTAL_MEM = ker::mod::mm::phys::get_total_mem_bytes();
    cache_max_bytes = choose_buffer_cache_max_bytes_for_total(TOTAL_MEM);
    dirty_target_bytes = choose_dirty_target_bytes_for_total(TOTAL_MEM, cache_max_bytes);
    dirty_hard_limit_bytes = choose_dirty_hard_limit_bytes(dirty_target_bytes, cache_max_bytes);
    hash_buckets.fill(nullptr);
    lru_init();
    cache_total_bytes = 0;
    cache_total_buffers = 0;
    cache_dirty_buffers = 0;
    cache_dirty_bytes = 0;
    dirty_bdev_states.clear();
    range_bdev_states.clear();
    dirty_waiters.clear();
    dirty_index_degraded = false;
    range_index_degraded = false;
    dirty_writeback_queued.store(false, std::memory_order_release);
    stat_hits = 0;
    stat_misses = 0;
    stat_disk_read_calls.store(0, std::memory_order_relaxed);
    stat_disk_read_bytes.store(0, std::memory_order_relaxed);
    stat_metadata_disk_read_calls.store(0, std::memory_order_relaxed);
    stat_metadata_disk_read_bytes.store(0, std::memory_order_relaxed);
    stat_data_disk_read_calls.store(0, std::memory_order_relaxed);
    stat_data_disk_read_bytes.store(0, std::memory_order_relaxed);
    stat_range_copy_attempts.store(0, std::memory_order_relaxed);
    stat_range_copy_cover_hits.store(0, std::memory_order_relaxed);
    stat_range_copy_overlap_hits.store(0, std::memory_order_relaxed);
    stat_range_copy_no_state.store(0, std::memory_order_relaxed);
    stat_range_copy_no_overlap.store(0, std::memory_order_relaxed);
    stat_range_copy_incomplete.store(0, std::memory_order_relaxed);
    stat_range_copy_overflow.store(0, std::memory_order_relaxed);
    stat_range_copy_degraded.store(0, std::memory_order_relaxed);
    cache_initialized = true;
    log::info("initialized (max %lu bytes)", static_cast<uint64_t>(cache_max_bytes));
}

auto bread(dev::BlockDevice* bdev, uint64_t block_no, BufferReadClass read_class) -> BufHead* {
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
        static_cast<void>(copy_dirty_bdev_range_for_cached_buffer_locked(bh, block_no, 1));
        cache_lock.unlock_irqrestore(irqflags);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_READ_HIT, PERF_STARTED_US, 0, bdev->block_size);
        return bh;
    }

    // 2. Cache miss - allocate and read from disk
    stat_misses++;

    evict_lru_for_allocation(bdev->block_size);
    cache_lock.unlock_irqrestore(irqflags);
    drain_deferred_data_buffer_frees_if_over_limit();

    uint64_t const PERF_ALLOC_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC);
    bh = alloc_detached_buffer(bdev, block_no);
    uint32_t const PERF_ALLOC_US = PERF_ALLOC_STARTED_US != 0 ? perf_elapsed_since_us(PERF_ALLOC_STARTED_US) : 0;
    if (bh == nullptr) {
        if (PERF_ALLOC_STARTED_US != 0) {
            ker::mod::perf::record_local_xfs_summary(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC, -ENOMEM, PERF_ALLOC_US, true, 0);
        }
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_READ_MISS, PERF_STARTED_US, -ENOMEM, 0);
        log::error("OOM allocating buffer for block %lu", block_no);
        return nullptr;
    }

    if (PERF_ALLOC_STARTED_US != 0) {
        ker::mod::perf::record_local_xfs_summary(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC, 0, PERF_ALLOC_US, true, bdev->block_size);
    }
    perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_READ_MISS, PERF_STARTED_US, 0, bdev->block_size);

    if (copy_cached_bdev_range_if_complete_internal(bdev, block_no, 1, bh->data) ||
        copy_dirty_bdev_range_if_complete(bdev, block_no, 1, bh->data)) {
        bh->flags |= BH_VALID;
    } else {
        int const RC = read_block_from_disk(bh, read_class);
        if (RC != 0) {
            free_detached_buffer(bh);
            return nullptr;
        }
        static_cast<void>(copy_dirty_bdev_range(bdev, block_no, 1, bh->data));
    }

    irqflags = cache_lock.lock_irqsave();
    BufHead* existing = hash_lookup(bdev, block_no, bdev->block_size);
    if (existing != nullptr) {
        existing->refcount.fetch_add(1, std::memory_order_relaxed);
        lru_touch(existing);
        static_cast<void>(copy_dirty_bdev_range_for_cached_buffer_locked(existing, block_no, 1));
        cache_lock.unlock_irqrestore(irqflags);
        free_detached_buffer(bh);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_READ_HIT, PERF_STARTED_US, 0, bdev->block_size);
        return existing;
    }

    insert_allocated_buffer_locked(bh);
    cache_lock.unlock_irqrestore(irqflags);
    drain_deferred_data_buffer_frees_if_over_limit();

    return bh;
}

auto bread_multi(dev::BlockDevice* bdev, uint64_t block_no, size_t count, BufferReadClass read_class) -> BufHead* {
    if (bdev == nullptr || count == 0) {
        return nullptr;
    }
    if (count == 1) {
        return bread(bdev, block_no, read_class);
    }
    uint64_t const PERF_STARTED_US = perf_xfs_started_us();

    // Lazily initialize the cache if not already done
    if (!cache_initialized) {
        buffer_cache_init();
    }

    size_t total_size = 0;
    if (!checked_block_span_size(bdev, count, total_size)) {
        return nullptr;
    }

    // Multi-block buffers are cached by their exact span. This avoids aliasing
    // a single-block and multi-block view of the same starting device block.
    uint64_t irqflags = cache_lock.lock_irqsave();

    BufHead* bh = hash_lookup(bdev, block_no, total_size);
    if (bh != nullptr) {
        bh->refcount.fetch_add(1, std::memory_order_relaxed);
        lru_touch(bh);
        stat_hits++;
        static_cast<void>(copy_dirty_bdev_range_for_cached_buffer_locked(bh, block_no, count));
        cache_lock.unlock_irqrestore(irqflags);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_READ_HIT, PERF_STARTED_US, 0, total_size);
        return bh;
    }

    stat_misses++;
    evict_lru_for_allocation(total_size);
    cache_lock.unlock_irqrestore(irqflags);
    drain_deferred_data_buffer_frees_if_over_limit();

    uint64_t const PERF_ALLOC_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC);
    bh = alloc_detached_buffer_with_size(bdev, block_no, total_size, 0);
    if (bh == nullptr) {
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC, PERF_ALLOC_STARTED_US, -ENOMEM, 0);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_READ_MISS, PERF_STARTED_US, -ENOMEM, 0);
        return nullptr;
    }
    uint32_t const PERF_ALLOC_US = PERF_ALLOC_STARTED_US != 0 ? perf_elapsed_since_us(PERF_ALLOC_STARTED_US) : 0;

    if (PERF_ALLOC_STARTED_US != 0) {
        ker::mod::perf::record_local_xfs_summary(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC, 0, PERF_ALLOC_US, true, total_size);
    }
    perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_READ_MISS, PERF_STARTED_US, 0, total_size);

    if (copy_cached_bdev_range_if_complete_internal(bdev, block_no, count, bh->data) ||
        copy_dirty_bdev_range_if_complete(bdev, block_no, count, bh->data)) {
        bh->flags |= BH_VALID;
    } else {
        // Read from disk outside the lock
        int const RC = read_blocks_with_retry(bdev, block_no, count, bh->data, read_class);
        if (RC != 0) {
            free_detached_buffer(bh);
            return nullptr;
        }
        bh->flags |= BH_VALID;
        static_cast<void>(copy_dirty_bdev_range(bdev, block_no, count, bh->data));
    }

    irqflags = cache_lock.lock_irqsave();
    BufHead* existing = hash_lookup(bdev, block_no, total_size);
    if (existing != nullptr) {
        existing->refcount.fetch_add(1, std::memory_order_relaxed);
        lru_touch(existing);
        static_cast<void>(copy_dirty_bdev_range_for_cached_buffer_locked(existing, block_no, count));
        cache_lock.unlock_irqrestore(irqflags);
        free_detached_buffer(bh);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_READ_HIT, PERF_STARTED_US, 0, total_size);
        return existing;
    }

    insert_allocated_buffer_locked(bh);
    cache_lock.unlock_irqrestore(irqflags);
    drain_deferred_data_buffer_frees_if_over_limit();

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

void bjournal_hold(BufHead* bh) {
    if (bh != nullptr) {
        bh->journal_pending.fetch_add(1, std::memory_order_acq_rel);
    }
}

void bjournal_release(BufHead* bh) {
    if (bh == nullptr) {
        return;
    }
    uint32_t const PREV = bh->journal_pending.fetch_sub(1, std::memory_order_acq_rel);
    if (PREV == 0) {
        bh->journal_pending.store(0, std::memory_order_release);
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
    bool should_writeback = false;
    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    became_dirty = mark_buffer_dirty_locked(bh);
    should_writeback = dirty_bytes_above_target_locked();
    cache_lock.unlock_irqrestore(IRQFLAGS);
    if (became_dirty) {
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_DIRTY, PERF_STARTED_US, 0, bh->size);
    }
    if (should_writeback) {
        static_cast<void>(request_dirty_writeback());
    }
}

namespace {
auto bget_impl(dev::BlockDevice* bdev, uint64_t block_no) -> BufHead* {
    if (bdev == nullptr) {
        return nullptr;
    }
    uint64_t const PERF_STARTED_US = perf_xfs_started_us();

    if (!cache_initialized) {
        buffer_cache_init();
    }

    uint64_t irqflags = cache_lock.lock_irqsave();

    // If the block is already cached, return it (caller will overwrite).
    BufHead* bh = hash_lookup(bdev, block_no, bdev->block_size);
    if (bh != nullptr) {
        bh->refcount.fetch_add(1, std::memory_order_relaxed);
        lru_touch(bh);
        cache_lock.unlock_irqrestore(irqflags);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_GET_HIT, PERF_STARTED_US, 0, bdev->block_size);
        return bh;
    }

    // Not cached - allocate a new buffer and insert without reading.
    evict_lru_for_allocation(bdev->block_size);
    cache_lock.unlock_irqrestore(irqflags);
    drain_deferred_data_buffer_frees_if_over_limit();

    uint64_t const PERF_ALLOC_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC);
    bh = alloc_detached_buffer(bdev, block_no);
    uint32_t const PERF_ALLOC_US = PERF_ALLOC_STARTED_US != 0 ? perf_elapsed_since_us(PERF_ALLOC_STARTED_US) : 0;
    if (bh == nullptr) {
        if (PERF_ALLOC_STARTED_US != 0) {
            ker::mod::perf::record_local_xfs_summary(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC, -ENOMEM, PERF_ALLOC_US, true, 0);
        }
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_GET_MISS, PERF_STARTED_US, -ENOMEM, 0);
        return nullptr;
    }
    bh->flags |= BH_VALID;  // Mark valid without disk read

    irqflags = cache_lock.lock_irqsave();
    BufHead* existing = hash_lookup(bdev, block_no, bdev->block_size);
    if (existing != nullptr) {
        existing->refcount.fetch_add(1, std::memory_order_relaxed);
        lru_touch(existing);
        cache_lock.unlock_irqrestore(irqflags);
        free_detached_buffer(bh);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_GET_HIT, PERF_STARTED_US, 0, bdev->block_size);
        return existing;
    }

    insert_allocated_buffer_locked(bh);
    cache_lock.unlock_irqrestore(irqflags);
    drain_deferred_data_buffer_frees_if_over_limit();
    if (PERF_ALLOC_STARTED_US != 0) {
        ker::mod::perf::record_local_xfs_summary(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC, 0, PERF_ALLOC_US, true, bdev->block_size);
    }
    perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_GET_MISS, PERF_STARTED_US, 0, bdev->block_size);
    return bh;
}

auto bget_multi_impl(dev::BlockDevice* bdev, uint64_t block_no, size_t count) -> BufHead* {
    if (bdev == nullptr || count == 0) {
        return nullptr;
    }
    if (count == 1) {
        return bget_impl(bdev, block_no);
    }
    uint64_t const PERF_STARTED_US = perf_xfs_started_us();

    if (!cache_initialized) {
        buffer_cache_init();
    }

    size_t total_size = 0;
    if (!checked_block_span_size(bdev, count, total_size)) {
        return nullptr;
    }

    uint64_t irqflags = cache_lock.lock_irqsave();

    // If already cached, return it (caller will overwrite contents).
    BufHead* bh = hash_lookup(bdev, block_no, total_size);
    if (bh != nullptr) {
        bh->refcount.fetch_add(1, std::memory_order_relaxed);
        lru_touch(bh);
        cache_lock.unlock_irqrestore(irqflags);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_GET_HIT, PERF_STARTED_US, 0, total_size);
        return bh;
    }

    evict_lru_for_allocation(total_size);
    cache_lock.unlock_irqrestore(irqflags);
    drain_deferred_data_buffer_frees_if_over_limit();

    uint64_t const PERF_ALLOC_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC);
    bh = alloc_detached_buffer_with_size(bdev, block_no, total_size, BH_VALID);
    if (bh == nullptr) {
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC, PERF_ALLOC_STARTED_US, -ENOMEM, 0);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_GET_MISS, PERF_STARTED_US, -ENOMEM, 0);
        return nullptr;
    }
    uint32_t const PERF_ALLOC_US = PERF_ALLOC_STARTED_US != 0 ? perf_elapsed_since_us(PERF_ALLOC_STARTED_US) : 0;

    irqflags = cache_lock.lock_irqsave();
    BufHead* existing = hash_lookup(bdev, block_no, total_size);
    if (existing != nullptr) {
        existing->refcount.fetch_add(1, std::memory_order_relaxed);
        lru_touch(existing);
        cache_lock.unlock_irqrestore(irqflags);
        free_detached_buffer(bh);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_GET_HIT, PERF_STARTED_US, 0, total_size);
        return existing;
    }

    insert_allocated_buffer_locked(bh);
    cache_lock.unlock_irqrestore(irqflags);
    drain_deferred_data_buffer_frees_if_over_limit();
    if (PERF_ALLOC_STARTED_US != 0) {
        ker::mod::perf::record_local_xfs_summary(ker::mod::perf::WkiPerfLocalXfsOp::BUF_ALLOC, 0, PERF_ALLOC_US, true, total_size);
    }
    perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUF_GET_MISS, PERF_STARTED_US, 0, total_size);

    return bh;
}

}  // namespace

auto bget(dev::BlockDevice* bdev, uint64_t block_no) -> BufHead* { return bget_impl(bdev, block_no); }

auto bget_multi(dev::BlockDevice* bdev, uint64_t block_no, size_t count) -> BufHead* { return bget_multi_impl(bdev, block_no, count); }

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
    uint64_t min_epoch = 0;
    uint64_t const MAX_EPOCH = max_matching_dirty_epoch(filter);
    while (true) {
        DirtyWritebackResult const WB = writeback_dirty_one_after(filter, min_epoch, MAX_EPOCH);
        if (WB.wrote) {
            wrote_dirty = true;
            min_epoch = WB.dirty_epoch;
            if (WB.status != 0) {
                result = WB.status;
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

auto has_cached_bdev_range(dev::BlockDevice* bdev, uint64_t block_no, size_t count) -> bool {
    if (bdev == nullptr || count == 0) {
        return false;
    }

    if (!cache_initialized) {
        return false;
    }

    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    bool cached = false;
    if (range_index_degraded) {
        for (auto* bucket_head : hash_buckets) {
            for (BufHead* bh = bucket_head; bh != nullptr; bh = bh->hash_next) {
                if (buffer_overlaps_range(bh, bdev, block_no, count)) {
                    cached = true;
                    break;
                }
            }
            if (cached) {
                break;
            }
        }
    } else {
        RangeBdevState* state = find_range_bdev_state_locked(bdev);
        cached = state != nullptr && state->buffers != 0 && range_tree_overlaps(state->tree_root, block_no, count);
    }
    cache_lock.unlock_irqrestore(IRQFLAGS);
    return cached;
}

auto copy_dirty_bdev_range(dev::BlockDevice* bdev, uint64_t block_no, size_t count, uint8_t* dst) -> bool {
    return copy_dirty_bdev_range_after_epoch(bdev, block_no, count, dst, 0);
}

auto copy_dirty_bdev_range_if_complete(dev::BlockDevice* bdev, uint64_t block_no, size_t count, uint8_t* dst) -> bool {
    if (bdev == nullptr || bdev->block_size == 0 || count == 0 || dst == nullptr || !cache_initialized) {
        return false;
    }
    if (count > SIZE_MAX / bdev->block_size) {
        return false;
    }

    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    bool const COPIED = cache_dirty_buffers != 0 && copy_dirty_bdev_range_if_complete_locked(bdev, block_no, count, dst);
    cache_lock.unlock_irqrestore(IRQFLAGS);
    return COPIED;
}

auto copy_cached_bdev_range_if_complete(dev::BlockDevice* bdev, uint64_t block_no, size_t count, uint8_t* dst) -> bool {
    return copy_cached_bdev_range_if_complete_internal(bdev, block_no, count, dst);
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

    uint64_t irqflags = cache_lock.lock_irqsave();
    DirtyBdevState* state = find_dirty_bdev_state_locked(bdev);
    bool const SHOULD_REQUEST_WRITEBACK =
        cache_dirty_bytes > dirty_target_bytes_locked() && (dirty_index_degraded || (state != nullptr && state->dirty_buffers != 0));
    bool const SHOULD_WAIT = cache_dirty_bytes > dirty_hard_limit_bytes_locked();
    cache_lock.unlock_irqrestore(irqflags);
    if (SHOULD_REQUEST_WRITEBACK) {
        static_cast<void>(request_dirty_writeback());
    }
    if (!SHOULD_WAIT) {
        return;
    }

    DirtyWritebackFilter fallback_filter{};
    fallback_filter.bdev = bdev;

    while (true) {
        irqflags = cache_lock.lock_irqsave();
        if (cache_dirty_bytes <= dirty_throttle_resume_bytes_locked()) {
            cache_lock.unlock_irqrestore(irqflags);
            return;
        }
        bool const CAN_PARK = register_current_dirty_waiter_locked();
        cache_lock.unlock_irqrestore(irqflags);

        static_cast<void>(request_dirty_writeback());
        static_cast<void>(writeback_dirty_budgeted(fallback_filter, DIRTY_HARD_FALLBACK_BUDGET, DIRTY_HARD_FALLBACK_BYTES));

        irqflags = cache_lock.lock_irqsave();
        bool const DRAINED_TO_RESUME = cache_dirty_bytes <= dirty_throttle_resume_bytes_locked();
        cache_lock.unlock_irqrestore(irqflags);
        if (DRAINED_TO_RESUME) {
            return;
        }

        if (CAN_PARK) {
            uint64_t const NOW_US = ker::mod::time::get_us();
            uint64_t const DEADLINE_US = ker::mod::sched::saturating_deadline_us(NOW_US, DIRTY_THROTTLE_PARK_TIMEOUT_US);
            ker::mod::sched::preemptible_syscall_park("dirty_bcache", DEADLINE_US);
            continue;
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
    uint64_t min_epoch = 0;

    DirtyWritebackFilter filter{};
    filter.bdev = bdev;
    filter.block_no = block_no;
    filter.count = count;
    filter.use_range = true;

    uint64_t const MAX_EPOCH = max_matching_dirty_epoch(filter);
    while (true) {
        DirtyWritebackResult const WB = writeback_dirty_one_after(filter, min_epoch, MAX_EPOCH);
        if (WB.wrote) {
            wrote_dirty = true;
            min_epoch = WB.dirty_epoch;
            if (WB.status != 0) {
                result = WB.status;
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
    drain_deferred_data_buffer_frees();
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

    if (!range_index_degraded) {
        RangeBdevState* state = find_range_bdev_state_locked(bdev);
        if (state == nullptr || state->buffers == 0 || !range_tree_overlaps(state->tree_root, block_no, count)) {
            cache_lock.unlock_irqrestore(IRQFLAGS);
            return;
        }

        while (state->buffers != 0 && range_tree_overlaps(state->tree_root, block_no, count)) {
            BufHead* victim = range_find_discard_candidate_locked(state->tree_root, bdev, block_no, count);
            if (victim == nullptr) {
                break;
            }
            discarded_bytes += victim->size;
            free_buffer(victim);
        }

        cache_lock.unlock_irqrestore(IRQFLAGS);
        drain_deferred_data_buffer_frees();
        if (discarded_bytes != 0) {
            perf_record_xfs_count(ker::mod::perf::WkiPerfLocalXfsOp::BUF_DISCARD, discarded_bytes);
        }
        return;
    }

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
    drain_deferred_data_buffer_frees();
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
    s.disk_read_calls = stat_disk_read_calls.load(std::memory_order_relaxed);
    s.disk_read_bytes = stat_disk_read_bytes.load(std::memory_order_relaxed);
    s.metadata_disk_read_calls = stat_metadata_disk_read_calls.load(std::memory_order_relaxed);
    s.metadata_disk_read_bytes = stat_metadata_disk_read_bytes.load(std::memory_order_relaxed);
    s.data_disk_read_calls = stat_data_disk_read_calls.load(std::memory_order_relaxed);
    s.data_disk_read_bytes = stat_data_disk_read_bytes.load(std::memory_order_relaxed);
    s.range_copy_attempts = stat_range_copy_attempts.load(std::memory_order_relaxed);
    s.range_copy_cover_hits = stat_range_copy_cover_hits.load(std::memory_order_relaxed);
    s.range_copy_overlap_hits = stat_range_copy_overlap_hits.load(std::memory_order_relaxed);
    s.range_copy_no_state = stat_range_copy_no_state.load(std::memory_order_relaxed);
    s.range_copy_no_overlap = stat_range_copy_no_overlap.load(std::memory_order_relaxed);
    s.range_copy_incomplete = stat_range_copy_incomplete.load(std::memory_order_relaxed);
    s.range_copy_overflow = stat_range_copy_overflow.load(std::memory_order_relaxed);
    s.range_copy_degraded = stat_range_copy_degraded.load(std::memory_order_relaxed);
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
    stats = reclaim_clean_cache_locked(target_bytes, SIZE_MAX, SIZE_MAX, SIZE_MAX, false);
    cache_lock.unlock_irqrestore(IRQFLAGS);
    drain_deferred_data_buffer_frees();
    return stats;
}

auto reclaim_clean_buffer_cache_for_pressure(size_t byte_budget) -> size_t {
    if (!cache_initialized || byte_budget == 0) {
        return 0;
    }

    uint64_t const IRQFLAGS = cache_lock.lock_irqsave();
    size_t const TARGET_BYTES = cache_total_bytes > byte_budget ? cache_total_bytes - byte_budget : 0;
    BufferCacheReclaimStats const STATS = reclaim_clean_cache_locked(TARGET_BYTES, byte_budget, SIZE_MAX, SIZE_MAX, false);
    cache_lock.unlock_irqrestore(IRQFLAGS);
    drain_deferred_data_buffer_frees();
    return STATS.freed_bytes;
}

#ifdef WOS_SELFTEST
auto buffer_cache_selftest_choose_cache_max_bytes(uint64_t total_mem) -> size_t {
    return choose_buffer_cache_max_bytes_for_total(total_mem);
}

auto buffer_cache_selftest_choose_dirty_target_bytes(uint64_t total_mem, size_t max_bytes) -> size_t {
    return choose_dirty_target_bytes_for_total(total_mem, max_bytes);
}

auto buffer_cache_selftest_choose_dirty_hard_bytes(size_t target_bytes, size_t max_bytes) -> size_t {
    return choose_dirty_hard_limit_bytes(target_bytes, max_bytes);
}
#endif

}  // namespace ker::vfs
