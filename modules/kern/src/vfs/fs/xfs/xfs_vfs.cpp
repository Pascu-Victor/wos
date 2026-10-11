// XFS VFS Integration - FileOperations implementation
//
// This module bridges the WOS VFS layer (FileOperations function pointers)
// to the XFS native implementation.  It implements open, read, write, seek,
// readdir, readlink, truncate, close, and stat for XFS.

#include <bits/off_t.h>
#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <dev/block_request.hpp>
#include <mod/io/serial/serial.hpp>
#include <new>
#include <optional>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/swap.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/file_growth.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/security/file_policy.hpp>
#include <platform/sys/mutex.hpp>
#include <platform/sys/spinlock.hpp>
#include <utility>
#include <vfs/buffer_cache.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/fs/xfs/xfs_alloc.hpp>
#include <vfs/fs/xfs/xfs_attr.hpp>
#include <vfs/fs/xfs/xfs_bmap.hpp>
#include <vfs/fs/xfs/xfs_dir2.hpp>
#include <vfs/fs/xfs/xfs_format.hpp>
#include <vfs/fs/xfs/xfs_ialloc.hpp>
#include <vfs/fs/xfs/xfs_inode.hpp>
#include <vfs/fs/xfs/xfs_log.hpp>
#include <vfs/fs/xfs/xfs_mount.hpp>
#include <vfs/fs/xfs/xfs_symlink.hpp>
#include <vfs/fs/xfs/xfs_trans.hpp>
#include <vfs/fs/xfs/xfs_vfs.hpp>
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

#include "dev/block_device.hpp"
#include "platform/dbg/dbg.hpp"
#ifdef XFS_BENCH
#include <atomic>

#include "platform/tsc/tsc.hpp"
#endif

namespace ker::vfs::xfs {

using log = ker::mod::dbg::logger<"xfs">;

namespace {

// Whole-path search admissions can finish on a different native mount.
std::atomic<uint64_t> xfs_search_authority_generation_value{1};

constexpr uint32_t XFS_SLOW_TRACE_US = 2048;
constexpr size_t XFS_READ_BATCH_MAX_BYTES = size_t{2} * 1024 * 1024;
constexpr size_t XFS_READ_CACHE_COPY_MAX_BYTES = size_t{512} * 1024;
constexpr size_t XFS_READ_CLUSTER_DEMAND_MAX_BYTES = size_t{512} * 1024;
constexpr size_t XFS_READ_CLUSTER_MAX_BYTES = size_t{4} * 1024 * 1024;
constexpr size_t XFS_READ_CLUSTER_MIN_BYTES = size_t{64} * 1024;
constexpr int64_t XFS_NSEC_PER_SEC_SIGNED = 1000000000LL;
constexpr uint64_t XFS_NSEC_PER_SEC = static_cast<uint64_t>(XFS_NSEC_PER_SEC_SIGNED);
constexpr int64_t XFS_BIGTIME_EPOCH_OFFSET = (1LL << 31);
// Maximum blocks to allocate per metadata transaction. Large user writes are
// still copied in buffered I/O batches below, but the extent allocation itself
// must leave enough log item headroom for AG free-space updates, bmap updates,
// and inode writeback during commit.
constexpr xfs_extlen_t XFS_WRITE_ALLOC_TRANSACTION_BLOCKS = 1024;  // up to 4 MiB at 4 KiB blocks
constexpr size_t XFS_BUFFERED_WRITE_BATCH_MAX_BYTES = size_t{256} * 1024;
constexpr size_t XFS_DIRTY_THROTTLE_INTERVAL_BYTES = size_t{4} * 1024 * 1024;
constexpr size_t XFS_STREAM_PREALLOC_TRIGGER_BYTES = size_t{8} * 1024;
constexpr xfs_extlen_t XFS_STREAM_PREALLOC_BLOCKS = 16;  // 64 KiB at 4 KiB blocks
constexpr uint32_t XFS_STREAM_PREALLOC_EXTENT_MARGIN = 8;
constexpr size_t XFS_PARENT_PATH_CACHE_PATH_MAX = 512;
// Large source-tree metadata scans revisit many more parent directories and
// path/inode pairs than a checkout's immediate hot set. Keep associativity fixed
// so lookup cost stays bounded while reducing set conflicts.
constexpr size_t XFS_PARENT_PATH_CACHE_SET_COUNT = 8192;
constexpr size_t XFS_PARENT_PATH_CACHE_WAYS = 4;
constexpr size_t XFS_PATH_INODE_CACHE_SET_COUNT = 32768;
constexpr size_t XFS_PATH_INODE_CACHE_WAYS = 4;
static_assert((XFS_PARENT_PATH_CACHE_SET_COUNT & (XFS_PARENT_PATH_CACHE_SET_COUNT - 1)) == 0);
static_assert((XFS_PATH_INODE_CACHE_SET_COUNT & (XFS_PATH_INODE_CACHE_SET_COUNT - 1)) == 0);

auto xfs_ialloc_conflicts_with_cached_inode(XfsMountContext* ctx, xfs_ino_t ino) -> bool {
    XfsInode* cached = xfs_inode_read_cached(ctx, ino);
    if (cached == nullptr) {
        return false;
    }

    mod::dbg::log("[xfs] inode allocator selected cached inode %lu (nlink=%u)", static_cast<unsigned long>(ino), cached->nlink);
    xfs_inode_release_metadata_locked(cached);
    return true;
}

using XfsPathBuffer = std::array<char, XFS_PARENT_PATH_CACHE_PATH_MAX>;

// ============================================================================
// Per-open-file state
// ============================================================================

// VFS read_dir_entries syscalls and mlibc readdir stage 16 KiB chunks. Cache
// enough minimum-sized records for one staged chunk so large source-tree scans
// avoid immediately re-walking the same directory from the beginning.
constexpr size_t XFS_READDIR_STAGE_BYTES = size_t{16} * 1024;
constexpr size_t XFS_READDIR_CACHE_ENTRIES = XFS_READDIR_STAGE_BYTES / DIRENT_MIN_RECLEN;
static_assert(XFS_READDIR_CACHE_ENTRIES >= 128);

struct XfsReaddirCacheEntry {
    size_t request_index{};
    DirEntry entry{};
};

struct XfsReaddirCache {
    uint64_t dir_generation{};
    size_t count{};
    std::array<XfsReaddirCacheEntry, XFS_READDIR_CACHE_ENTRIES> entries{};
};

struct XfsFileData {
    XfsMountContext* mount{};
    XfsInode* inode{};  // reference-counted inode
    XfsReaddirCache* readdir_cache{};
    XfsPathBuffer fs_path{};
    size_t fs_path_len{};
    ker::mod::sys::Spinlock recent_write_stat_lock;
    ker::vfs::Stat recent_write_stat{};
    bool has_fs_path{};
    bool recent_write_stat_valid{};
    bool may_have_eof_prealloc{};
    bool close_may_need_inode_commit{};
    bool swap_extent_pin{};
    // Offset prediction only; mappings and EOF are resolved again under io_lock.
    size_t read_next_offset{};
    bool read_sequence_valid{};
};

void xfs_recent_write_stat_invalidate(XfsFileData* xfd) {
    if (xfd == nullptr) {
        return;
    }
    uint64_t const IRQF = xfd->recent_write_stat_lock.lock_irqsave();
    xfd->recent_write_stat_valid = false;
    xfd->recent_write_stat_lock.unlock_irqrestore(IRQF);
}

void xfs_recent_write_stat_store(XfsFileData* xfd, const ker::vfs::Stat& statbuf) {
    if (xfd == nullptr) {
        return;
    }
    uint64_t const IRQF = xfd->recent_write_stat_lock.lock_irqsave();
    xfd->recent_write_stat = statbuf;
    xfd->recent_write_stat_valid = true;
    xfd->recent_write_stat_lock.unlock_irqrestore(IRQF);
}

struct XfsParentPathCacheEntry {
    XfsMountContext* mount{};
    std::array<char, XFS_PARENT_PATH_CACHE_PATH_MAX> path{};
    size_t path_len{};
    xfs_ino_t ino{};
    uint64_t hash{};
    uint64_t last_used{};
    bool valid{};
};

struct XfsParentPathCacheSet {
    ker::mod::sys::Spinlock lock;
    std::array<XfsParentPathCacheEntry, XFS_PARENT_PATH_CACHE_WAYS> ways{};
    uint64_t clock{};
};

struct XfsPathInodeCacheEntry {
    XfsMountContext* mount{};
    std::array<char, XFS_PARENT_PATH_CACHE_PATH_MAX> path{};
    size_t path_len{};
    xfs_ino_t ino{};
    uint64_t hash{};
    uint64_t generation{};
    uint64_t last_used{};
    uint8_t ftype{};
    bool valid{};
};

struct XfsPathInodeCacheSet {
    ker::mod::sys::Spinlock lock;
    std::array<XfsPathInodeCacheEntry, XFS_PATH_INODE_CACHE_WAYS> ways{};
    uint64_t clock{};
};

class XfsMetadataGuard {
   public:
    explicit XfsMetadataGuard(XfsMountContext* ctx, bool active = true, uint64_t callsite = 0)
        : ctx(active ? ctx : nullptr), callsite(callsite) {
        if (this->ctx != nullptr) {
            acquire();
        }
    }

    ~XfsMetadataGuard() { unlock(); }

    XfsMetadataGuard(const XfsMetadataGuard&) = delete;
    XfsMetadataGuard(XfsMetadataGuard&&) = delete;
    auto operator=(const XfsMetadataGuard&) -> XfsMetadataGuard& = delete;
    auto operator=(XfsMetadataGuard&&) -> XfsMetadataGuard& = delete;

    void lock() {
        if (ctx != nullptr && !locked) {
            acquire();
        }
    }

    void unlock() {
        if (ctx != nullptr && locked) {
            metadata_perf_record(ker::mod::perf::WkiPerfLocalXfsOp::METADATA_LOCK_HOLD, hold_started_us, callsite);
            hold_started_us = 0;
            ctx->metadata_lock.unlock();
            locked = false;
        }
    }

   private:
    static constexpr int ADAPTIVE_SPIN_LIMIT = 4096;

    auto try_adaptive_lock() -> bool {
        for (int i = 0; i < ADAPTIVE_SPIN_LIMIT; ++i) {
            if (ctx->metadata_lock.try_lock()) {
                return true;
            }
            asm volatile("pause" ::: "memory");
        }
        return false;
    }

    void acquire() {
        uint64_t const WAIT_STARTED_US = metadata_perf_started_us(ker::mod::perf::WkiPerfLocalXfsOp::METADATA_LOCK_WAIT);
        if (!try_adaptive_lock()) {
            ctx->metadata_lock.lock();
        }
        locked = true;
        metadata_perf_record(ker::mod::perf::WkiPerfLocalXfsOp::METADATA_LOCK_WAIT, WAIT_STARTED_US, callsite);
        hold_started_us = metadata_perf_started_us(ker::mod::perf::WkiPerfLocalXfsOp::METADATA_LOCK_HOLD);
    }

    static auto metadata_perf_started_us(ker::mod::perf::WkiPerfLocalXfsOp op) -> uint64_t {
        return ker::mod::perf::is_wki_scope_recording_enabled(ker::mod::perf::WkiPerfScope::LOCAL_XFS, static_cast<uint8_t>(op))
                   ? ker::mod::time::get_us()
                   : 0;
    }

    static auto metadata_perf_elapsed_us(uint64_t started_us) -> uint32_t {
        uint64_t const NOW_US = ker::mod::time::get_us();
        uint64_t const ELAPSED_US = NOW_US >= started_us ? NOW_US - started_us : 0;
        return ELAPSED_US > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(ELAPSED_US);
    }

    static void metadata_perf_record(ker::mod::perf::WkiPerfLocalXfsOp op, uint64_t started_us, uint64_t callsite) {
        if (started_us == 0) {
            return;
        }
        uint32_t const LATENCY_US = metadata_perf_elapsed_us(started_us);
        ker::mod::perf::record_local_xfs_summary(op, 0, LATENCY_US, true, 0);
        if (LATENCY_US < XFS_SLOW_TRACE_US) {
            return;
        }

        auto* task = ker::mod::sched::get_current_task();
        uint64_t const PID = task != nullptr ? task->pid : 0;
        uint32_t const CPU = task != nullptr ? static_cast<uint32_t>(task->load_cpu()) : 0U;
        ker::mod::perf::record_wki_event(CPU, PID, ker::mod::perf::WkiPerfScope::LOCAL_XFS, static_cast<uint8_t>(op),
                                         ker::mod::perf::WkiPerfPhase::END, 0, 0, ker::mod::perf::next_wki_trace_correlation(), 0,
                                         LATENCY_US, callsite);
    }

    XfsMountContext* ctx;
    uint64_t callsite;
    uint64_t hold_started_us = 0;
    bool locked = false;
};

auto perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp op) -> uint64_t {
    return ker::mod::perf::is_wki_scope_recording_enabled(ker::mod::perf::WkiPerfScope::LOCAL_XFS, static_cast<uint8_t>(op))
               ? ker::mod::time::get_us()
               : 0;
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

auto perf_record_xfs_stage_elapsed(ker::mod::perf::WkiPerfLocalXfsOp op, uint64_t started_us, int32_t status, uint64_t bytes) -> uint32_t {
    if (started_us == 0) {
        return 0;
    }
    uint32_t const ELAPSED_US = perf_elapsed_since_us(started_us);
    ker::mod::perf::record_local_xfs_summary(op, status, ELAPSED_US, true, bytes);
    return ELAPSED_US;
}

void perf_record_xfs_count(ker::mod::perf::WkiPerfLocalXfsOp op, uint64_t bytes = 0, int32_t status = 0) {
    if (!ker::mod::perf::is_wki_scope_recording_enabled(ker::mod::perf::WkiPerfScope::LOCAL_XFS, static_cast<uint8_t>(op))) {
        return;
    }
    ker::mod::perf::record_local_xfs_summary(op, status, 0, false, bytes);
}

auto perf_current_pid() -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    return task != nullptr ? task->pid : 0;
}

auto perf_current_cpu() -> uint32_t {
    auto* task = ker::mod::sched::get_current_task();
    return task != nullptr ? static_cast<uint32_t>(task->load_cpu()) : 0U;
}

auto perf_clamp_u16(uint64_t value) -> uint16_t { return value > UINT16_MAX ? UINT16_MAX : static_cast<uint16_t>(value); }

auto xfs_encode_epoch_timestamp(uint64_t epoch_ns, bool bigtime) -> uint64_t {
    uint64_t const SEC = epoch_ns / XFS_NSEC_PER_SEC;
    uint64_t const NSEC = epoch_ns % XFS_NSEC_PER_SEC;
    if (bigtime) {
        return ((SEC + static_cast<uint64_t>(XFS_BIGTIME_EPOCH_OFFSET)) * XFS_NSEC_PER_SEC) + NSEC;
    }
    return (SEC << 32U) | NSEC;
}

auto xfs_current_timestamp(const XfsInode* ip) -> uint64_t {
    bool const BIGTIME = ip != nullptr && (ip->flags2 & XFS_DIFLAG2_BIGTIME) != 0;
    return xfs_encode_epoch_timestamp(ker::mod::time::get_epoch_ns(), BIGTIME);
}

auto xfs_encode_timespec_timestamp(const Timespec& ts, bool bigtime, uint64_t* out) -> int {
    if (out == nullptr || ts.tv_sec < 0 || ts.tv_nsec < 0 || ts.tv_nsec >= XFS_NSEC_PER_SEC_SIGNED) {
        return -EINVAL;
    }

    auto const SEC = static_cast<uint64_t>(ts.tv_sec);
    auto const NSEC = static_cast<uint64_t>(ts.tv_nsec);
    if (bigtime) {
        uint64_t const MAX_SEC = (UINT64_MAX / XFS_NSEC_PER_SEC) - static_cast<uint64_t>(XFS_BIGTIME_EPOCH_OFFSET);
        if (SEC > MAX_SEC) {
            return -EOVERFLOW;
        }
        *out = ((SEC + static_cast<uint64_t>(XFS_BIGTIME_EPOCH_OFFSET)) * XFS_NSEC_PER_SEC) + NSEC;
        return 0;
    }

    if (SEC > UINT32_MAX) {
        return -EOVERFLOW;
    }
    *out = (SEC << 32U) | NSEC;
    return 0;
}

auto xfs_data_fork_record_capacity(const XfsInode* ip) -> uint32_t {
    if (ip == nullptr || ip->mount == nullptr || ip->mount->inode_size <= XFS_DINODE_SIZE_V3) {
        return 0;
    }

    size_t fork_size = ip->mount->inode_size - XFS_DINODE_SIZE_V3;
    if (ip->forkoff != 0) {
        fork_size = static_cast<size_t>(ip->forkoff) << 3U;
        if (XFS_DINODE_SIZE_V3 + fork_size > ip->mount->inode_size) {
            return 0;
        }
    }

    size_t const CAPACITY = fork_size / sizeof(XfsBmbtRec);
    return CAPACITY > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(CAPACITY);
}

auto xfs_has_inline_extent_pressure(const XfsInode* ip) -> bool {
    if (ip == nullptr || ip->data_fork.format != XFS_DINODE_FMT_EXTENTS) {
        return false;
    }

    uint32_t const CAPACITY = xfs_data_fork_record_capacity(ip);
    if (CAPACITY == 0) {
        return false;
    }

    uint32_t const COUNT = ip->data_fork.extents.count;
    return COUNT >= CAPACITY || CAPACITY - COUNT <= XFS_STREAM_PREALLOC_EXTENT_MARGIN;
}

auto xfs_hole_write_alloc_blocks(size_t write_pos, size_t block_off, size_t remaining_bytes, xfs_filblks_t hole_blocks, size_t block_size,
                                 uint32_t block_log, bool extent_pressure, bool sequential_append) -> xfs_extlen_t {
    size_t const BLOCKS_NEEDED = (block_off + remaining_bytes + block_size - 1) >> block_log;
    auto desired_blocks = static_cast<xfs_filblks_t>(BLOCKS_NEEDED);
    bool const STREAM_PREALLOC =
        sequential_append && (write_pos >= XFS_STREAM_PREALLOC_TRIGGER_BYTES || remaining_bytes >= XFS_STREAM_PREALLOC_TRIGGER_BYTES);
    if ((STREAM_PREALLOC || extent_pressure) && hole_blocks > desired_blocks) {
        desired_blocks = std::max(desired_blocks, static_cast<xfs_filblks_t>(XFS_STREAM_PREALLOC_BLOCKS));
    }
    auto alloc_blocks = static_cast<xfs_extlen_t>(std::min(hole_blocks, desired_blocks));
    alloc_blocks = std::min(alloc_blocks, XFS_WRITE_ALLOC_TRANSACTION_BLOCKS);
    return alloc_blocks == 0 ? 1 : alloc_blocks;
}

auto xfs_write_alloc_min_blocks(xfs_extlen_t max_blocks, bool extent_pressure, bool sequential_append) -> xfs_extlen_t {
    if (max_blocks <= 1) {
        return 1;
    }
    if (!extent_pressure && !sequential_append) {
        return 1;
    }
    return std::max<xfs_extlen_t>(1, std::min(max_blocks, XFS_STREAM_PREALLOC_BLOCKS));
}

auto xfs_write_alloc_next_min_blocks(xfs_extlen_t min_blocks) -> xfs_extlen_t {
    if (min_blocks <= 1) {
        return 1;
    }
    return std::max<xfs_extlen_t>(1, min_blocks / 2);
}

auto xfs_mapped_append_can_zero_without_read(size_t write_pos, uint64_t file_size, size_t block_size) -> bool {
    if (block_size == 0 || write_pos != file_size) {
        return false;
    }
    return (file_size % block_size) == 0;
}

void xfs_zero_fresh_block_unwritten_ranges(uint8_t* block, size_t block_size, size_t write_off, size_t write_len) {
    if (block == nullptr || block_size == 0) {
        return;
    }

    size_t const BOUNDED_WRITE_OFF = std::min(write_off, block_size);
    if (BOUNDED_WRITE_OFF > 0) {
        std::memset(block, 0, BOUNDED_WRITE_OFF);
    }

    size_t const BOUNDED_WRITE_LEN = std::min(write_len, block_size - BOUNDED_WRITE_OFF);
    size_t const WRITE_END = BOUNDED_WRITE_OFF + BOUNDED_WRITE_LEN;
    if (WRITE_END < block_size) {
        std::memset(block + WRITE_END, 0, block_size - WRITE_END);
    }
}

auto xfs_read_batch_max_bytes(size_t block_size) -> size_t {
    if (block_size == 0) {
        return XFS_READ_BATCH_MAX_BYTES;
    }
    size_t const MAX_BLOCKS = std::max<size_t>(1, XFS_READ_BATCH_MAX_BYTES / block_size);
    return MAX_BLOCKS * block_size;
}

auto xfs_read_kernel_destination_eligible(const void* destination, size_t bytes) -> bool {
#ifdef WOS_ASYNC_BLOCK_IO
    uint64_t const HHDM = ker::mod::mm::addr::get_hhdm_offset();
    uint64_t const START = reinterpret_cast<uint64_t>(destination);
    return HHDM != 0 && START >= HHDM && bytes != 0 && bytes - 1 <= UINT64_MAX - START;
#else
    static_cast<void>(destination);
    static_cast<void>(bytes);
    return false;
#endif
}

auto xfs_read_cluster_context_eligible() -> bool {
#ifdef WOS_ASYNC_BLOCK_IO
    namespace sched = ker::mod::sched;
    if (!sched::has_run_queues() || !sched::can_query_current_task() || ker::mod::io::serial::is_panic_mode() ||
        sched::kernel_threads_shutdown_requested()) {
        return false;
    }
    auto* task = sched::get_current_task();
    if (task == nullptr || task->type != sched::task::TaskType::PROCESS || task->preempt_disable_depth != 0) return false;
    bool const RETURN_YIELD = task->deferred_task_switch && task->yield_switch && task->wait_channel == nullptr &&
                              task->wait_channel_kind == sched::task::WaitChannelKind::NONE;
    return task->syscall_account_start_us != 0 && !task->scheduler_transition_active.load(std::memory_order_acquire) &&
           (!task->deferred_task_switch || RETURN_YIELD) && !task->wants_block && !task->is_voluntary_blocked() &&
           task->wki_proxy_task_id == 0 && !task->wait_channel_is(sched::task::WaitChannelKind::WKI_EXECVE_PROXY) &&
           !task->has_interrupting_signal_pending();
#else
    return false;
#endif
}

auto xfs_read_cluster_bytes(XfsMountContext* ctx, const XfsBmapResult& bmap, size_t read_offset, uint64_t file_size, size_t demand,
                            bool sequential) -> size_t {
    if (!sequential || demand < XFS_READ_CLUSTER_MIN_BYTES || demand >= XFS_READ_CLUSTER_DEMAND_MAX_BYTES || ctx == nullptr ||
        ctx->device == nullptr || ctx->block_size == 0 || demand % ctx->block_size != 0 || read_offset % ctx->block_size != 0 ||
        read_offset >= file_size || bmap.is_hole || bmap.unwritten || bmap.startblock == NULLFSBLOCK || bmap.blockcount == 0 ||
        ctx->ag_blk_log >= 64 || !xfs_read_cluster_context_eligible() || !dev::block_queue_available(ctx->device)) {
        return demand;
    }
    dev::BlockQueueLimits const LIMITS = dev::block_queue_limits(ctx->device);
    if (LIMITS.depth <= 1 || LIMITS.max_segments == 0 || LIMITS.max_requests == 0 || LIMITS.max_transfer_bytes == 0) return demand;
    auto const AG = bmap.startblock >> ctx->ag_blk_log;
    uint64_t const AG_BLOCK = bmap.startblock & ((uint64_t{1} << ctx->ag_blk_log) - 1);
    if (ctx->ag_blocks == 0 || ctx->ag_blocks > (uint64_t{1} << ctx->ag_blk_log) || AG >= ctx->ag_count || AG_BLOCK >= ctx->ag_blocks)
        return demand;
    uint64_t const AG_START = AG * static_cast<uint64_t>(ctx->ag_blocks);
    if (AG_START >= ctx->total_blocks) return demand;
    uint64_t const AG_SIZE = std::min<uint64_t>(ctx->ag_blocks, ctx->total_blocks - AG_START);
    if (AG_BLOCK >= AG_SIZE) return demand;
    uint64_t const BOUND_BLOCKS =
        std::min({static_cast<uint64_t>(bmap.blockcount), AG_SIZE - AG_BLOCK, (file_size - read_offset) / ctx->block_size});
    // Aggregate only this caller's bounded read; bread_multi retains and joins
    // every provider-sized slice before the inode mapping lock is released.
    size_t const SLOTS = std::min(LIMITS.depth, LIMITS.max_requests);
    size_t const TRANSFER_BUDGET = SLOTS > SIZE_MAX / LIMITS.max_transfer_bytes ? SIZE_MAX : LIMITS.max_transfer_bytes * SLOTS;
    size_t const BYTE_LIMIT = std::min({XFS_READ_CLUSTER_MAX_BYTES, TRANSFER_BUDGET, LIMITS.max_inflight_bytes});
    size_t const READ_BYTES = static_cast<size_t>(std::min<uint64_t>(BOUND_BLOCKS, BYTE_LIMIT / ctx->block_size)) * ctx->block_size;
    return std::max(demand, READ_BYTES);
}

void xfs_set_alloc_hint_from_fsb(XfsMountContext* ctx, xfs_fsblock_t fsb, XfsAllocReq* req) {
    if (ctx == nullptr || req == nullptr || fsb == NULLFSBLOCK) {
        return;
    }

    xfs_agnumber_t const AGNO = xfs_ag_number(fsb, ctx->ag_blk_log);
    xfs_agblock_t const AGBNO = xfs_ag_block(fsb, ctx->ag_blk_log);
    if (AGNO < ctx->ag_count && AGBNO < ctx->ag_blocks) {
        req->agno = AGNO;
        req->agbno = AGBNO;
    }
}

void xfs_set_sequential_alloc_hint(XfsInode* ip, XfsMountContext* ctx, xfs_fileoff_t file_block, XfsAllocReq* req) {
    if (ip == nullptr || ctx == nullptr || req == nullptr || file_block == 0) {
        return;
    }

    XfsBmapResult prev{};
    if (xfs_bmap_lookup(ip, file_block - 1, &prev) != 0 || prev.is_hole || prev.startblock == NULLFSBLOCK) {
        return;
    }

    xfs_fsblock_t const NEXT_FSB = prev.startblock + 1;
    if (NEXT_FSB <= prev.startblock) {
        return;
    }
    xfs_set_alloc_hint_from_fsb(ctx, NEXT_FSB, req);
}

auto xfs_truncate_zero_resets_data(uint64_t old_size, uint64_t nblocks) -> bool { return old_size != 0 || nblocks != 0; }

auto xfs_close_should_trim_prealloc(int open_flags, bool created_by_open, bool may_have_eof_prealloc) -> bool {
    if ((static_cast<unsigned>(open_flags) & 3U) == ker::vfs::FILE_ACCESS_SEARCH_ONLY) return false;
    if (created_by_open) {
        return false;
    }
    if ((open_flags & 3) != 0) {
        return may_have_eof_prealloc || !created_by_open;
    }
    if ((open_flags & (ker::vfs::O_CREAT | ker::vfs::O_TRUNC)) == 0) {
        return false;
    }
    return may_have_eof_prealloc || !created_by_open;
}

auto xfs_close_should_commit_inode(bool close_may_need_inode_commit, int open_flags, bool created_by_open, bool may_have_eof_prealloc)
    -> bool {
    return close_may_need_inode_commit || xfs_close_should_trim_prealloc(open_flags, created_by_open, may_have_eof_prealloc);
}

auto xfs_ftype_from_mode(uint16_t mode) -> uint8_t {
    switch (mode & 0170000) {
        case 0100000:
            return XFS_DIR3_FT_REG_FILE;
        case 0040000:
            return XFS_DIR3_FT_DIR;
        case 0120000:
            return XFS_DIR3_FT_SYMLINK;
        case 0020000:
            return XFS_DIR3_FT_CHRDEV;
        case 0060000:
            return XFS_DIR3_FT_BLKDEV;
        case 0010000:
            return XFS_DIR3_FT_FIFO;
        case 0140000:
            return XFS_DIR3_FT_SOCK;
        default:
            return XFS_DIR3_FT_UNKNOWN;
    }
}

auto xfs_ftype_from_inode(const XfsInode* ip) -> uint8_t { return ip != nullptr ? xfs_ftype_from_mode(ip->mode) : XFS_DIR3_FT_UNKNOWN; }

auto xfs_inode_has_eof_prealloc(const XfsInode* ip) -> bool {
    if (ip == nullptr || ip->mount == nullptr || ip->mount->block_size == 0 || ip->data_fork.format != XFS_DINODE_FMT_EXTENTS ||
        ip->data_fork.extents.count == 0) {
        return false;
    }
    if (ip->data_fork.extents.list == nullptr) {
        return true;
    }

    auto const KEEP_BLOCKS =
        static_cast<xfs_fileoff_t>((ip->size + static_cast<uint64_t>(ip->mount->block_size - 1)) >> ip->mount->block_log);
    for (uint32_t i = 0; i < ip->data_fork.extents.count; ++i) {
        XfsBmbtIrec const& rec = ip->data_fork.extents.list[i];
        if (rec.br_startoff >= KEEP_BLOCKS || rec.br_blockcount > KEEP_BLOCKS - rec.br_startoff) {
            return true;
        }
    }
    return false;
}

void xfs_stamp_new_inode(XfsInode* ip) {
    if (ip == nullptr) {
        return;
    }
    if (ip->mount != nullptr && xfs_has_bigtime(ip->mount)) {
        ip->flags2 |= XFS_DIFLAG2_BIGTIME;
    }
    if (ip->mount != nullptr && xfs_has_nrext64(ip->mount)) {
        ip->flags2 |= XFS_DIFLAG2_NREXT64;
    }
    uint64_t const NOW = xfs_current_timestamp(ip);
    ip->atime = NOW;
    ip->mtime = NOW;
    ip->ctime = NOW;
    ip->crtime = NOW;
}

void xfs_stamp_inode_data_change(XfsInode* ip) {
    if (ip == nullptr) {
        return;
    }
    uint64_t const NOW = xfs_current_timestamp(ip);
    ip->mtime = NOW;
    ip->ctime = NOW;
    ip->dirty = true;
}

void xfs_stamp_same_size_truncate(XfsFileData* xfd, XfsInode* ip) {
    if (xfd == nullptr || ip == nullptr) {
        return;
    }
    xfs_recent_write_stat_invalidate(xfd);
    xfs_stamp_inode_data_change(ip);
    xfd->close_may_need_inode_commit = true;
}

void perf_record_xfs_slow_event(ker::mod::perf::WkiPerfLocalXfsOp op, int32_t status, uint32_t latency_us, uint64_t bytes,
                                uint64_t callsite) {
    if (status >= 0 && latency_us < XFS_SLOW_TRACE_US) {
        return;
    }
    if (!ker::mod::perf::is_wki_scope_recording_enabled(ker::mod::perf::WkiPerfScope::LOCAL_XFS, static_cast<uint8_t>(op))) {
        return;
    }

    uint16_t const BYTES_KIB = perf_clamp_u16((bytes + 1023U) / 1024U);
    ker::mod::perf::record_wki_event(perf_current_cpu(), perf_current_pid(), ker::mod::perf::WkiPerfScope::LOCAL_XFS,
                                     static_cast<uint8_t>(op), ker::mod::perf::WkiPerfPhase::END, 0, BYTES_KIB,
                                     ker::mod::perf::next_wki_trace_correlation(), status, latency_us, callsite);
}

std::array<XfsParentPathCacheSet, XFS_PARENT_PATH_CACHE_SET_COUNT> g_xfs_parent_path_cache{};
std::array<XfsPathInodeCacheSet, XFS_PATH_INODE_CACHE_SET_COUNT> g_xfs_path_inode_cache{};
std::atomic<uint64_t> g_xfs_path_inode_generation{1};

auto xfs_parent_path_cache_hash(XfsMountContext* ctx, const char* path, size_t path_len) -> uint64_t {
    uint64_t hash = 1469598103934665603ULL ^ reinterpret_cast<uintptr_t>(ctx);
    for (size_t i = 0; i < path_len; ++i) {
        hash ^= static_cast<unsigned char>(path[i]);
        hash *= 1099511628211ULL;
    }
    hash ^= hash >> 33U;
    hash *= 0xff51afd7ed558ccdULL;
    hash ^= hash >> 33U;
    return hash == 0 ? 1 : hash;
}

auto xfs_path_inode_cache_hash(XfsMountContext* ctx, const char* path, size_t path_len) -> uint64_t {
    uint64_t const HASH = xfs_parent_path_cache_hash(ctx, path, path_len) ^ 0x8a5cd789635d2dffULL;
    return HASH == 0 ? 1 : HASH;
}

void xfs_path_inode_cache_bump_generation() { g_xfs_path_inode_generation.fetch_add(1, std::memory_order_acq_rel); }

auto xfs_path_inode_cache_lookup_ino(XfsMountContext* ctx, const char* path, size_t path_len, xfs_ino_t* ino_out,
                                     uint8_t* ftype_out = nullptr) -> bool {
    if (ctx == nullptr || path == nullptr || ino_out == nullptr || path_len == 0 || path_len >= XFS_PARENT_PATH_CACHE_PATH_MAX) {
        return false;
    }

    uint64_t const GENERATION = g_xfs_path_inode_generation.load(std::memory_order_acquire);
    uint64_t const HASH = xfs_path_inode_cache_hash(ctx, path, path_len);
    auto& set = g_xfs_path_inode_cache[HASH & (XFS_PATH_INODE_CACHE_SET_COUNT - 1)];

    uint64_t const IRQF = set.lock.lock_irqsave();
    for (auto& entry : set.ways) {
        if (!entry.valid) {
            continue;
        }
        if (entry.generation != GENERATION) {
            entry.valid = false;
            continue;
        }
        if (entry.mount != ctx || entry.hash != HASH || entry.path_len != path_len) {
            continue;
        }
        if (std::memcmp(entry.path.data(), path, path_len) != 0) {
            continue;
        }
        entry.last_used = ++set.clock;
        *ino_out = entry.ino;
        if (ftype_out != nullptr) {
            *ftype_out = entry.ftype;
        }
        set.lock.unlock_irqrestore(IRQF);
        return true;
    }
    set.lock.unlock_irqrestore(IRQF);
    return false;
}

void xfs_path_inode_cache_store(XfsMountContext* ctx, const char* path, size_t path_len, xfs_ino_t ino,
                                uint8_t ftype = XFS_DIR3_FT_UNKNOWN) {
    if (ctx == nullptr || path == nullptr || ino == NULLFSINO || path_len == 0 || path_len >= XFS_PARENT_PATH_CACHE_PATH_MAX) {
        return;
    }

    uint64_t const GENERATION = g_xfs_path_inode_generation.load(std::memory_order_acquire);
    uint64_t const HASH = xfs_path_inode_cache_hash(ctx, path, path_len);
    auto& set = g_xfs_path_inode_cache[HASH & (XFS_PATH_INODE_CACHE_SET_COUNT - 1)];

    uint64_t const IRQF = set.lock.lock_irqsave();
    uint64_t const USE_STAMP = ++set.clock;
    XfsPathInodeCacheEntry* victim = &set.ways.front();
    for (auto& entry : set.ways) {
        if (entry.valid && entry.generation == GENERATION && entry.mount == ctx && entry.hash == HASH && entry.path_len == path_len &&
            std::memcmp(entry.path.data(), path, path_len) == 0) {
            victim = &entry;
            break;
        }
        if (!entry.valid || entry.generation != GENERATION) {
            victim = &entry;
            break;
        }
        if (entry.last_used < victim->last_used) {
            victim = &entry;
        }
    }

    std::memcpy(victim->path.data(), path, path_len);
    victim->path.at(path_len) = '\0';
    victim->mount = ctx;
    victim->path_len = path_len;
    victim->ino = ino;
    victim->hash = HASH;
    victim->generation = GENERATION;
    victim->last_used = USE_STAMP;
    victim->ftype = ftype;
    victim->valid = true;
    set.lock.unlock_irqrestore(IRQF);
}

void xfs_path_inode_cache_invalidate_path(XfsMountContext* ctx, const char* path, size_t known_path_len) {
    if (ctx == nullptr || path == nullptr) {
        return;
    }

    size_t path_len = known_path_len;
    if (path_len == UNKNOWN_XFS_PATH_LEN) {
        path_len = 0;
        while (path[path_len] != '\0') {
            ++path_len;
        }
    }
    if (path_len == 0 || path_len >= XFS_PARENT_PATH_CACHE_PATH_MAX) {
        return;
    }

    uint64_t const HASH = xfs_path_inode_cache_hash(ctx, path, path_len);
    auto& set = g_xfs_path_inode_cache[HASH & (XFS_PATH_INODE_CACHE_SET_COUNT - 1)];

    uint64_t const IRQF = set.lock.lock_irqsave();
    for (auto& entry : set.ways) {
        if (!entry.valid || entry.mount != ctx || entry.hash != HASH || entry.path_len != path_len) {
            continue;
        }
        if (std::memcmp(entry.path.data(), path, path_len) != 0) {
            continue;
        }
        entry.valid = false;
    }
    set.lock.unlock_irqrestore(IRQF);
}

void xfs_path_inode_cache_purge_all_for_mount(XfsMountContext* ctx) {
    if (ctx == nullptr) {
        return;
    }

    for (auto& set : g_xfs_path_inode_cache) {
        uint64_t const IRQF = set.lock.lock_irqsave();
        for (auto& entry : set.ways) {
            if (entry.valid && entry.mount == ctx) {
                entry.valid = false;
            }
        }
        set.lock.unlock_irqrestore(IRQF);
    }
}

auto xfs_parent_path_cache_lookup_ino(XfsMountContext* ctx, const char* path, size_t path_len, xfs_ino_t* ino_out) -> bool {
    if (ctx == nullptr || path == nullptr || ino_out == nullptr || path_len == 0 || path_len >= XFS_PARENT_PATH_CACHE_PATH_MAX) {
        return false;
    }

    uint64_t const HASH = xfs_parent_path_cache_hash(ctx, path, path_len);
    auto& set = g_xfs_parent_path_cache[HASH & (XFS_PARENT_PATH_CACHE_SET_COUNT - 1)];

    uint64_t const IRQF = set.lock.lock_irqsave();
    for (auto& entry : set.ways) {
        if (!entry.valid || entry.mount != ctx || entry.hash != HASH || entry.path_len != path_len) {
            continue;
        }
        if (std::memcmp(entry.path.data(), path, path_len) != 0) {
            continue;
        }
        entry.last_used = ++set.clock;
        *ino_out = entry.ino;
        set.lock.unlock_irqrestore(IRQF);
        return true;
    }
    set.lock.unlock_irqrestore(IRQF);
    return false;
}

auto xfs_known_path_len(const char* path, size_t known_path_len) -> size_t {
    if (known_path_len != UNKNOWN_XFS_PATH_LEN) {
        return known_path_len;
    }

    size_t path_len = 0;
    while (path[path_len] != '\0') {
        ++path_len;
    }
    return path_len;
}

void xfs_file_data_set_fs_path(XfsFileData* xfd, const char* fs_path, size_t known_fs_path_len) {
    if (xfd == nullptr || fs_path == nullptr) {
        return;
    }

    size_t const PATH_LEN = xfs_known_path_len(fs_path, known_fs_path_len);
    if (PATH_LEN >= xfd->fs_path.size()) {
        return;
    }

    std::memcpy(xfd->fs_path.data(), fs_path, PATH_LEN);
    xfd->fs_path.at(PATH_LEN) = '\0';
    xfd->fs_path_len = PATH_LEN;
    xfd->has_fs_path = true;
}

void xfs_parent_path_cache_store(XfsMountContext* ctx, const char* path, size_t path_len, xfs_ino_t ino) {
    if (ctx == nullptr || path == nullptr || ino == NULLFSINO || path_len == 0 || path_len >= XFS_PARENT_PATH_CACHE_PATH_MAX) {
        return;
    }

    uint64_t const HASH = xfs_parent_path_cache_hash(ctx, path, path_len);
    auto& set = g_xfs_parent_path_cache[HASH & (XFS_PARENT_PATH_CACHE_SET_COUNT - 1)];

    uint64_t const IRQF = set.lock.lock_irqsave();
    uint64_t const USE_STAMP = ++set.clock;
    XfsParentPathCacheEntry* victim = &set.ways.front();
    for (auto& entry : set.ways) {
        if (entry.valid && entry.mount == ctx && entry.hash == HASH && entry.path_len == path_len &&
            std::memcmp(entry.path.data(), path, path_len) == 0) {
            victim = &entry;
            break;
        }
        if (!entry.valid) {
            victim = &entry;
            break;
        }
        if (entry.last_used < victim->last_used) {
            victim = &entry;
        }
    }

    std::memcpy(victim->path.data(), path, path_len);
    victim->path.at(path_len) = '\0';
    victim->mount = ctx;
    victim->path_len = path_len;
    victim->ino = ino;
    victim->hash = HASH;
    victim->last_used = USE_STAMP;
    victim->valid = true;
    set.lock.unlock_irqrestore(IRQF);
}

void xfs_parent_path_cache_store_directory(XfsMountContext* ctx, const char* path, size_t known_path_len, XfsInode* ip) {
    if (ctx == nullptr || path == nullptr || ip == nullptr || ip->mount != ctx || !xfs_inode_isdir(ip) || ip->nlink == 0) {
        return;
    }

    size_t const PATH_LEN = xfs_known_path_len(path, known_path_len);
    if (PATH_LEN == 0 || (PATH_LEN == 1 && path[0] == '/')) {
        return;
    }

    xfs_parent_path_cache_store(ctx, path, PATH_LEN, ip->ino);
}

void xfs_path_inode_cache_store_inode(XfsMountContext* ctx, const char* path, size_t known_path_len, XfsInode* ip) {
    if (ctx == nullptr || path == nullptr || ip == nullptr || ip->mount != ctx || ip->nlink == 0) {
        return;
    }

    size_t const PATH_LEN = xfs_known_path_len(path, known_path_len);
    if (PATH_LEN == 0 || (PATH_LEN == 1 && path[0] == '/')) {
        return;
    }

    xfs_path_inode_cache_store(ctx, path, PATH_LEN, ip->ino, xfs_ftype_from_inode(ip));
}

void xfs_parent_path_cache_store_directory_entry(XfsMountContext* ctx, const char* path, size_t known_path_len, const XfsDirEntry& entry) {
    if (ctx == nullptr || path == nullptr || entry.ino == NULLFSINO || entry.ftype != XFS_DIR3_FT_DIR) {
        return;
    }

    size_t const PATH_LEN = xfs_known_path_len(path, known_path_len);
    if (PATH_LEN == 0 || (PATH_LEN == 1 && path[0] == '/')) {
        return;
    }

    xfs_parent_path_cache_store(ctx, path, PATH_LEN, entry.ino);
}

void xfs_parent_path_cache_store_known_directory(XfsMountContext* ctx, const char* path, size_t known_path_len, xfs_ino_t ino,
                                                 uint8_t ftype) {
    if (ctx == nullptr || path == nullptr || ino == NULLFSINO || ftype != XFS_DIR3_FT_DIR) {
        return;
    }

    size_t const PATH_LEN = xfs_known_path_len(path, known_path_len);
    if (PATH_LEN == 0 || (PATH_LEN == 1 && path[0] == '/')) {
        return;
    }

    xfs_parent_path_cache_store(ctx, path, PATH_LEN, ino);
}

auto xfs_cached_directory_inode_for_path(XfsMountContext* ctx, const char* path, size_t path_len, bool* cache_hit_out) -> XfsInode* {
    if (ctx == nullptr || path == nullptr || path_len == 0 || path_len >= XFS_PARENT_PATH_CACHE_PATH_MAX) {
        return nullptr;
    }

    xfs_ino_t cached_ino = NULLFSINO;
    bool cache_hit = xfs_parent_path_cache_lookup_ino(ctx, path, path_len, &cached_ino);
    if (!cache_hit) {
        uint8_t cached_ftype = XFS_DIR3_FT_UNKNOWN;
        if (!xfs_path_inode_cache_lookup_ino(ctx, path, path_len, &cached_ino, &cached_ftype)) {
            return nullptr;
        }
        if (cached_ftype != XFS_DIR3_FT_UNKNOWN && cached_ftype != XFS_DIR3_FT_DIR) {
            return nullptr;
        }
        cache_hit = true;
    }

    XfsInode* ip = xfs_inode_read_known_allocated(ctx, cached_ino);
    if (ip == nullptr) {
        return nullptr;
    }
    if (ip->nlink == 0 || !xfs_inode_isdir(ip)) {
        if (ip->nlink != 0) {
            xfs_path_inode_cache_store(ctx, path, path_len, cached_ino, xfs_ftype_from_inode(ip));
        }
        xfs_inode_release(ip);
        return nullptr;
    }

    xfs_parent_path_cache_store(ctx, path, path_len, cached_ino);
    if (cache_hit_out != nullptr && cache_hit) {
        *cache_hit_out = true;
    }
    return ip;
}

auto xfs_readdir_child_path(const XfsFileData* xfd, const XfsDirEntry* entry, XfsPathBuffer& child_path, size_t* child_path_len_out)
    -> bool {
    if (xfd == nullptr || entry == nullptr || child_path_len_out == nullptr || !xfd->has_fs_path || entry->ino == NULLFSINO ||
        entry->namelen == 0) {
        return false;
    }
    if (entry->namelen == 1 && entry->name.at(0) == '.') {
        return false;
    }
    if (entry->namelen == 2 && entry->name.at(0) == '.' && entry->name.at(1) == '.') {
        return false;
    }

    size_t const PARENT_LEN = xfd->fs_path_len;
    size_t const NAME_LEN = entry->namelen;
    size_t const SEP_LEN = PARENT_LEN == 0 ? 0 : 1;
    size_t const CHILD_LEN = PARENT_LEN + SEP_LEN + NAME_LEN;
    if (CHILD_LEN >= child_path.size()) {
        return false;
    }

    size_t pos = 0;
    if (PARENT_LEN > 0) {
        std::memcpy(child_path.data(), xfd->fs_path.data(), PARENT_LEN);
        pos = PARENT_LEN;
        child_path.at(pos++) = '/';
    }
    std::memcpy(child_path.data() + pos, entry->name.data(), NAME_LEN);
    child_path.at(CHILD_LEN) = '\0';
    *child_path_len_out = CHILD_LEN;
    return true;
}

void xfs_readdir_seed_child_path_caches(XfsFileData* xfd, const XfsDirEntry* entry) {
    if (xfd == nullptr || xfd->mount == nullptr || entry == nullptr) {
        return;
    }

    XfsPathBuffer child_path{};
    size_t child_path_len = 0;
    if (!xfs_readdir_child_path(xfd, entry, child_path, &child_path_len)) {
        return;
    }

    xfs_path_inode_cache_store(xfd->mount, child_path.data(), child_path_len, entry->ino, entry->ftype);
    xfs_parent_path_cache_store_directory_entry(xfd->mount, child_path.data(), child_path_len, *entry);
}

void xfs_parent_path_cache_purge_all_for_mount(XfsMountContext* ctx) {
    if (ctx == nullptr) {
        return;
    }
    xfs_path_inode_cache_purge_all_for_mount(ctx);

    for (auto& set : g_xfs_parent_path_cache) {
        uint64_t const IRQF = set.lock.lock_irqsave();
        for (auto& entry : set.ways) {
            if (entry.valid && entry.mount == ctx) {
                entry.valid = false;
            }
        }
        set.lock.unlock_irqrestore(IRQF);
    }
}

auto xfs_commit_dirty_inode(XfsMountContext* ctx, XfsInode* ip, bool trim_eof_prealloc = false) -> int {
    bool const SHOULD_TRIM =
        ip != nullptr && trim_eof_prealloc && ip->exec_content.can_mutate() && !xfs_inode_isdir(ip) && xfs_inode_has_eof_prealloc(ip);
    if (ctx == nullptr || ip == nullptr || ctx->read_only || (!ip->dirty && !SHOULD_TRIM)) {
        return 0;
    }

    uint64_t const STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_ILOG);
    XfsTransaction* tp = xfs_trans_alloc(ctx, xfs_inode_isreg(ip) ? XfsLookupEffect::PRESERVE_FILE_DATA : XfsLookupEffect::INVALIDATE);
    if (tp == nullptr) {
        mod::dbg::log("[xfs] commit dirty inode: ino=%lu transaction allocation failed", static_cast<unsigned long>(ip->ino));
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_ILOG, STARTED_US, -ENOMEM, 0);
        return -ENOMEM;
    }
    if (SHOULD_TRIM) {
        int const TRIM_RET = xfs_inode_trim_data_to_size(ip, tp, ip->size);
        if (TRIM_RET != 0) {
            mod::dbg::log("[xfs] commit dirty inode: ino=%lu trim failed ret=%d size=%lu nblocks=%lu nextents=%u format=%u",
                          static_cast<unsigned long>(ip->ino), TRIM_RET, static_cast<unsigned long>(ip->size),
                          static_cast<unsigned long>(ip->nblocks), ip->nextents, static_cast<unsigned>(ip->data_fork.format));
            xfs_trans_cancel(tp);
            perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_ILOG, STARTED_US, TRIM_RET, 0);
            return TRIM_RET;
        }
    }
    if (!ip->dirty) {
        xfs_trans_cancel(tp);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_ILOG, STARTED_US, 0, 0);
        return 0;
    }
    xfs_trans_log_inode(tp, ip);
    int const RET = xfs_trans_commit(tp);
    if (RET != 0) {
        mod::dbg::log("[xfs] commit dirty inode: ino=%lu commit failed ret=%d size=%lu nblocks=%lu nextents=%u format=%u",
                      static_cast<unsigned long>(ip->ino), RET, static_cast<unsigned long>(ip->size),
                      static_cast<unsigned long>(ip->nblocks), ip->nextents, static_cast<unsigned>(ip->data_fork.format));
    }
    perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_ILOG, STARTED_US, RET, 0);
    return RET == 0 ? 0 : -EIO;
}

// ============================================================================
// Path walking
// ============================================================================

// Walk a filesystem-relative path and return the inode.
// An empty path or "/" refers to the root inode.
// Returns a reference-counted inode on success, nullptr on error.

auto xfs_fsblock_to_dev_block(XfsMountContext* ctx, xfs_fsblock_t fsbno) -> uint64_t {
    auto agno = xfs_ag_number(fsbno, ctx->ag_blk_log);
    auto agbno = xfs_ag_block(fsbno, ctx->ag_blk_log);
    uint64_t const LINEAR_BLOCK = (static_cast<uint64_t>(agno) * ctx->ag_blocks) + agbno;
    return LINEAR_BLOCK * (ctx->block_size / ctx->device->block_size);
}

auto xfs_fsb_to_dev_count(XfsMountContext* ctx, xfs_filblks_t fsb_count) -> size_t {
    return static_cast<size_t>(fsb_count) * (ctx->block_size / ctx->device->block_size);
}

template <typename Fn>
auto visit_inode_data_ranges(XfsMountContext* ctx, XfsInode* ip, uint64_t byte_count, Fn fn) -> int {
    if (ctx == nullptr || ctx->device == nullptr || ip == nullptr || byte_count == 0 || ip->data_fork.format == XFS_DINODE_FMT_LOCAL) {
        return 0;
    }

    auto const FILE_BLOCKS = static_cast<xfs_fileoff_t>((byte_count + ctx->block_size - 1) >> ctx->block_log);
    xfs_fileoff_t file_block = 0;
    int result = 0;

    while (file_block < FILE_BLOCKS) {
        XfsBmapResult bmap{};
        int const RET = xfs_bmap_lookup(ip, file_block, &bmap);
        if (RET < 0) {
            return RET;
        }
        if (bmap.blockcount == 0) {
            return result != 0 ? result : -EIO;
        }

        auto const REMAINING = static_cast<xfs_filblks_t>(FILE_BLOCKS - file_block);
        xfs_filblks_t const SPAN = std::min(bmap.blockcount, REMAINING);
        if (!bmap.is_hole && bmap.startblock != NULLFSBLOCK) {
            int const RC = fn(bmap.startblock, SPAN);
            if (RC != 0) {
                result = RC;
            }
        }
        file_block += SPAN;
    }

    return result;
}

auto sync_inode_data_buffers(XfsMountContext* ctx, XfsInode* ip, uint64_t byte_count) -> int {
    return visit_inode_data_ranges(ctx, ip, byte_count, [&](xfs_fsblock_t startblock, xfs_filblks_t blockcount) -> int {
        return sync_bdev_range(ctx->device, xfs_fsblock_to_dev_block(ctx, startblock), xfs_fsb_to_dev_count(ctx, blockcount));
    });
}

auto discard_inode_data_buffers(XfsMountContext* ctx, XfsInode* ip, uint64_t byte_count) -> int {
    return visit_inode_data_ranges(ctx, ip, byte_count, [&](xfs_fsblock_t startblock, xfs_filblks_t blockcount) -> int {
        discard_bdev_range(ctx->device, xfs_fsblock_to_dev_block(ctx, startblock), xfs_fsb_to_dev_count(ctx, blockcount));
        return 0;
    });
}

auto walk_path(XfsMountContext* ctx, const char* path, bool allow_dentry_cache = true) -> XfsInode* {
    // Start at the root inode
    XfsInode* ip = xfs_root_inode_read(ctx);
    if (ip == nullptr) {
        return nullptr;
    }

    // Empty path or "/" -> root
    if (path == nullptr || path[0] == '\0') {
        return ip;
    }

    const char* p = path;

    // Skip leading slash if present
    if (*p == '/') {
        p++;
    }

    while (*p != '\0') {
        // Skip consecutive slashes
        while (*p == '/') {
            p++;
        }
        if (*p == '\0') {
            break;
        }

        // Extract component
        const char* comp_start = p;
        while (*p != '\0' && *p != '/') {
            p++;
        }
        auto namelen = static_cast<uint16_t>(p - comp_start);

        if (namelen == 0) {
            continue;
        }

        // Current inode must be a directory
        if (!xfs_inode_isdir(ip)) {
            xfs_inode_release(ip);
            return nullptr;
        }

        // Look up the component
        XfsDirEntry de{};
        int const RET =
            allow_dentry_cache ? xfs_dir_lookup(ip, comp_start, namelen, &de) : xfs_dir_lookup_authoritative(ip, comp_start, namelen, &de);
        if (RET != 0) {
            xfs_inode_release(ip);
            return nullptr;
        }

        auto const COMPONENT_PATH_LEN = static_cast<size_t>(p - path);

        // Release parent, read child
        xfs_inode_release(ip);
        ip = xfs_inode_read_known_allocated(ctx, de.ino);
        if (ip == nullptr) {
            return nullptr;
        }
        uint8_t const FTYPE = de.ftype != XFS_DIR3_FT_UNKNOWN ? de.ftype : xfs_ftype_from_inode(ip);
        xfs_path_inode_cache_store(ctx, path, COMPONENT_PATH_LEN, de.ino, FTYPE);
        if (xfs_inode_isdir(ip)) {
            xfs_parent_path_cache_store(ctx, path, COMPONENT_PATH_LEN, ip->ino);
        }
    }

    return ip;
}

// ============================================================================
// FileOperations callbacks
// ============================================================================

auto xfs_vfs_close(File* f) -> int {
    if (f == nullptr) {
        return -EBADF;
    }
    int close_result = 0;
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd != nullptr) {
        delete xfd->readdir_cache;
        xfd->readdir_cache = nullptr;
        if (xfd->inode != nullptr) {
            if (f->exec_read_view != nullptr || xfd->swap_extent_pin) {
                ker::mod::sys::MutexGuard guard(xfd->inode->io_lock);
                if (f->exec_read_view != nullptr) xfd->inode->exec_content.release();
                if (xfd->swap_extent_pin) --xfd->inode->exec_content.direct_writers;
            }
            bool const TRIM_PREALLOC = xfs_close_should_trim_prealloc(f->open_flags, f->created_by_open, xfd->may_have_eof_prealloc);
            bool const COMMIT_NEEDED = xfs_close_should_commit_inode(xfd->close_may_need_inode_commit, f->open_flags, f->created_by_open,
                                                                     xfd->may_have_eof_prealloc);
            if (COMMIT_NEEDED) {
                ker::mod::sys::MutexGuard guard(xfd->inode->io_lock);
                XfsMetadataGuard metadata_guard(xfd->mount, true, WOS_PERF_CALLSITE());
                close_result = xfs_commit_dirty_inode(xfd->mount, xfd->inode, TRIM_PREALLOC);
                if (close_result != 0) {
                    mod::dbg::log("[xfs] close: path=%s ino=%lu ret=%d trim=%u open_flags=0x%x created=%u prealloc=%u dirty=%u",
                                  f->vfs_path != nullptr ? f->vfs_path : "?", static_cast<unsigned long>(xfd->inode->ino), close_result,
                                  TRIM_PREALLOC ? 1U : 0U, static_cast<unsigned>(f->open_flags), f->created_by_open ? 1U : 0U,
                                  xfd->may_have_eof_prealloc ? 1U : 0U, xfd->inode->dirty ? 1U : 0U);
                }
            }
            xfs_inode_release(xfd->inode);
        }
        delete xfd;
        f->private_data = nullptr;
    }
    return close_result;
}

auto xfs_vfs_read(File* f, void* buf, size_t count, size_t offset) -> ssize_t {
    if (f != nullptr && (static_cast<unsigned>(f->open_flags) & 3U) == ker::vfs::FILE_ACCESS_SEARCH_ONLY) return -EBADF;
    if (f == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }

    XfsInode* ip = xfd->inode;
    XfsMountContext* ctx = xfd->mount;

    // Can't read directories as files
    if (xfs_inode_isdir(ip)) {
        return -EISDIR;
    }

    ker::mod::sys::MutexGuard guard(ip->io_lock);
    // The inode I/O lock freezes both inline extents and B-tree roots for the
    // duration of this read. B-tree lookup only reads buffer-cache state, so it
    // does not need to serialize unrelated files through the mount mutex.

    bool const SEQUENTIAL = xfd->read_sequence_valid && xfd->read_next_offset == offset;
    xfd->read_sequence_valid = false;

    // Inline data (LOCAL format)
    if (ip->data_fork.format == XFS_DINODE_FMT_LOCAL) {
        if (offset >= ip->size) {
            return 0;
        }
        size_t const AVAIL = ip->size - offset;
        size_t const TO_COPY = count < AVAIL ? count : AVAIL;
        std::memcpy(buf, ip->data_fork.local.data + offset, TO_COPY);
        return static_cast<ssize_t>(TO_COPY);
    }

    // EXTENTS or BTREE - block-based read
    if (offset >= ip->size) {
        return 0;
    }
    size_t const AVAIL = ip->size - offset;
    size_t remaining = count < AVAIL ? count : AVAIL;
    size_t total_read = 0;
    auto* dst = static_cast<uint8_t*>(buf);
    uint64_t const PERF_READ_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::READ);
    uint32_t perf_accounted_us = 0;
    auto finish_read = [&](ssize_t result) -> ssize_t {
        if (result > 0 && static_cast<size_t>(result) <= SIZE_MAX - offset) {
            xfd->read_next_offset = offset + static_cast<size_t>(result);
            xfd->read_sequence_valid = true;
        }
        uint64_t const BYTES = result > 0 ? static_cast<uint64_t>(result) : static_cast<uint64_t>(total_read);
        int32_t const STATUS = result < 0 ? static_cast<int32_t>(result) : 0;
        if (PERF_READ_STARTED_US != 0) {
            uint32_t const LATENCY_US = perf_elapsed_since_us(PERF_READ_STARTED_US);
            ker::mod::perf::record_local_xfs_summary(ker::mod::perf::WkiPerfLocalXfsOp::READ, STATUS, LATENCY_US, true, BYTES);
            perf_record_xfs_slow_event(ker::mod::perf::WkiPerfLocalXfsOp::READ, STATUS, LATENCY_US, BYTES, WOS_PERF_CALLSITE());
            if (LATENCY_US > perf_accounted_us) {
                uint32_t const GAP_US = LATENCY_US - perf_accounted_us;
                ker::mod::perf::record_local_xfs_summary(ker::mod::perf::WkiPerfLocalXfsOp::READ_GAP, STATUS, GAP_US, true, BYTES);
                perf_record_xfs_slow_event(ker::mod::perf::WkiPerfLocalXfsOp::READ_GAP, STATUS, GAP_US, BYTES, WOS_PERF_CALLSITE());
            }
        }
        return result;
    };

#ifdef XFS_BENCH
    static std::atomic<uint64_t> s_read_calls{0};
    static std::atomic<uint64_t> s_read_ns_bmap{0};
    static std::atomic<uint64_t> s_read_ns_io{0};
    static std::atomic<uint64_t> s_read_bytes{0};
    uint64_t acc_bmap = 0;
    uint64_t acc_io = 0;
#endif

    while (remaining > 0) {
        // Compute the logical file block and byte offset within the block
        auto file_block = static_cast<xfs_fileoff_t>((offset + total_read) >> ctx->block_log);
        size_t const BLOCK_OFF = (offset + total_read) & (ctx->block_size - 1);

        XfsBmapResult bmap{};
        uint64_t const PERF_BMAP_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::READ_BMAP);
#ifdef XFS_BENCH
        uint64_t t0 = ker::mod::tsc::get_ns();
#endif
        int const RET = xfs_bmap_lookup(ip, file_block, &bmap);
        perf_accounted_us += perf_record_xfs_stage_elapsed(ker::mod::perf::WkiPerfLocalXfsOp::READ_BMAP, PERF_BMAP_STARTED_US, RET, 0);
#ifdef XFS_BENCH
        acc_bmap += ker::mod::tsc::get_ns() - t0;
#endif
        if (RET < 0) {
            return finish_read((total_read > 0) ? static_cast<ssize_t>(total_read) : RET);
        }
        if (bmap.blockcount == 0) {
            log::warn("read: zero-length bmap for ino=%lu off=%lu file_block=%lu fmt=%d size=%lu nblocks=%lu hole=%d startblk=%lu",
                      static_cast<unsigned long>(ip->ino), static_cast<unsigned long>(offset + total_read),
                      static_cast<unsigned long>(file_block), ip->data_fork.format, static_cast<unsigned long>(ip->size),
                      static_cast<unsigned long>(ip->nblocks), bmap.is_hole, static_cast<unsigned long>(bmap.startblock));
            return finish_read((total_read > 0) ? static_cast<ssize_t>(total_read) : -EIO);
        }

        if (bmap.is_hole || bmap.startblock == NULLFSBLOCK) {
            // Hole - return zeros
            size_t hole_bytes = (static_cast<size_t>(bmap.blockcount) * ctx->block_size) - BLOCK_OFF;
            if (hole_bytes == 0) {
                log::warn("read: zero-length hole span for ino=%lu off=%lu file_block=%lu blkcnt=%lu", static_cast<unsigned long>(ip->ino),
                          static_cast<unsigned long>(offset + total_read), static_cast<unsigned long>(file_block),
                          static_cast<unsigned long>(bmap.blockcount));
                return finish_read((total_read > 0) ? static_cast<ssize_t>(total_read) : -EIO);
            }
            hole_bytes = std::min(hole_bytes, remaining);
            uint64_t const PERF_ZERO_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::READ_ZERO);
            std::memset(dst + total_read, 0, hole_bytes);
            perf_accounted_us +=
                perf_record_xfs_stage_elapsed(ker::mod::perf::WkiPerfLocalXfsOp::READ_ZERO, PERF_ZERO_STARTED_US, 0, hole_bytes);
            total_read += hole_bytes;
            remaining -= hole_bytes;
            continue;
        }

        // Read contiguous blocks in bulk through the cache so dirty buffers stay
        // authoritative while adjacent mappings can still use one I/O.
        xfs_fsblock_t const DISK_BLOCK = bmap.startblock;

        // How many bytes does this extent cover from our current position?
        size_t const EXTENT_BYTES = (static_cast<size_t>(bmap.blockcount) * ctx->block_size) - BLOCK_OFF;
        size_t chunk = std::min(EXTENT_BYTES, remaining);
        if (chunk == 0) {
            log::warn(
                "read: zero-length extent span for ino=%lu off=%lu file_block=%lu startblk=%lu blkcnt=%lu block_off=%zu remaining=%zu",
                static_cast<unsigned long>(ip->ino), static_cast<unsigned long>(offset + total_read),
                static_cast<unsigned long>(file_block), static_cast<unsigned long>(bmap.startblock),
                static_cast<unsigned long>(bmap.blockcount), BLOCK_OFF, remaining);
            return finish_read((total_read > 0) ? static_cast<ssize_t>(total_read) : -EIO);
        }

        if (BLOCK_OFF == 0 && chunk >= ctx->block_size) {
            chunk &= ~(ctx->block_size - 1);
        }

        if (BLOCK_OFF == 0 && (chunk & (ctx->block_size - 1)) == 0) {
            size_t const READ_BATCH_MAX_BYTES = xfs_read_batch_max_bytes(ctx->block_size);
            chunk = std::min(chunk, READ_BATCH_MAX_BYTES);
            auto const START_AG = static_cast<xfs_agnumber_t>(DISK_BLOCK >> ctx->ag_blk_log);
            while (chunk < remaining && chunk < READ_BATCH_MAX_BYTES) {
                size_t const CURRENT_BLOCKS = chunk >> ctx->block_log;
                size_t const BATCH_REMAINING = std::min(remaining, READ_BATCH_MAX_BYTES);
                size_t const REMAINING_FULL_BLOCKS = (BATCH_REMAINING - chunk) >> ctx->block_log;
                if (CURRENT_BLOCKS == 0 || REMAINING_FULL_BLOCKS == 0) {
                    break;
                }

                XfsBmapResult next_bmap{};
                xfs_fileoff_t const NEXT_FILE_BLOCK = file_block + static_cast<xfs_fileoff_t>(CURRENT_BLOCKS);
                uint64_t const PERF_NEXT_BMAP_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::READ_BMAP);
                int const NEXT_RET = xfs_bmap_lookup(ip, NEXT_FILE_BLOCK, &next_bmap);
                perf_accounted_us +=
                    perf_record_xfs_stage_elapsed(ker::mod::perf::WkiPerfLocalXfsOp::READ_BMAP, PERF_NEXT_BMAP_STARTED_US, NEXT_RET, 0);
                if (NEXT_RET != 0 || next_bmap.blockcount == 0 || next_bmap.is_hole || next_bmap.startblock == NULLFSBLOCK ||
                    next_bmap.unwritten != bmap.unwritten) {
                    break;
                }

                auto const NEXT_AG = static_cast<xfs_agnumber_t>(next_bmap.startblock >> ctx->ag_blk_log);
                xfs_fsblock_t const EXPECTED_DISK_BLOCK = DISK_BLOCK + static_cast<xfs_fsblock_t>(CURRENT_BLOCKS);
                if (NEXT_AG != START_AG || next_bmap.startblock != EXPECTED_DISK_BLOCK) {
                    break;
                }

                size_t const ADD_BLOCKS = std::min(static_cast<size_t>(next_bmap.blockcount), REMAINING_FULL_BLOCKS);
                if (ADD_BLOCKS == 0) {
                    break;
                }
                chunk += ADD_BLOCKS << ctx->block_log;
            }
        }

        uint64_t const PERF_IO_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::READ_IO);
#ifdef XFS_BENCH
        t0 = ker::mod::tsc::get_ns();
#endif
        if (BLOCK_OFF == 0 && (chunk & (ctx->block_size - 1)) == 0) {
            // Cache the complete request span. Native bread_multi pipelines
            // bounded demand slices while this inode io_lock freezes mappings;
            // all slices retire before publication/copy. Dirty aliases remain
            // authoritative; no supplemental read escapes this inode lock.
            size_t const FSB_COUNT = chunk >> ctx->block_log;
            uint64_t const DEV_BLOCK = xfs_fsblock_to_dev_block(ctx, DISK_BLOCK);
            size_t const DEV_COUNT = xfs_fsb_to_dev_count(ctx, static_cast<xfs_filblks_t>(FSB_COUNT));
#ifdef XFS_BENCH
            acc_io += ker::mod::tsc::get_ns() - t0;
#endif
            bool const KERNEL_DESTINATION = xfs_read_kernel_destination_eligible(dst + total_read, chunk);
            bool const NATIVE_CACHE = KERNEL_DESTINATION && xfs_inode_isreg(ip) && !bmap.unwritten &&
                                      chunk <= XFS_READ_CACHE_COPY_MAX_BYTES && dev::block_queue_available(ctx->device) &&
                                      dev::block_queue_limits(ctx->device).depth > 1;
            if (NATIVE_CACHE && copy_cached_bdev_range_if_complete(ctx->device, DEV_BLOCK, DEV_COUNT, dst + total_read)) {
                perf_accounted_us +=
                    perf_record_xfs_stage_elapsed(ker::mod::perf::WkiPerfLocalXfsOp::READ_IO, PERF_IO_STARTED_US, 0, chunk);
                total_read += chunk;
                remaining -= chunk;
                continue;
            }
            // A cache miss can partially copy. The referenced demand read
            // below overwrites the complete returned prefix in that case.
            size_t const READ_BYTES = xfs_read_cluster_bytes(ctx, bmap, offset + total_read, ip->size, chunk,
                                                             KERNEL_DESTINATION && xfs_inode_isreg(ip) && SEQUENTIAL);
            size_t const READ_DEV_COUNT = xfs_fsb_to_dev_count(ctx, static_cast<xfs_filblks_t>(READ_BYTES >> ctx->block_log));
            BufHead* bp = bread_multi(ctx->device, DEV_BLOCK, READ_DEV_COUNT, BufferReadClass::FILE_DATA);
            if (bp == nullptr && READ_BYTES > chunk) {
                // bread_multi retires all native slices and detached backing
                // on failure. An unread future block must not fail demand.
                bp = bread_multi(ctx->device, DEV_BLOCK, DEV_COUNT, BufferReadClass::FILE_DATA);
            }
            perf_accounted_us += perf_record_xfs_stage_elapsed(ker::mod::perf::WkiPerfLocalXfsOp::READ_IO, PERF_IO_STARTED_US,
                                                               bp != nullptr ? 0 : -EIO, bp != nullptr ? chunk : 0);
            if (bp == nullptr) {
                return finish_read((total_read > 0) ? static_cast<ssize_t>(total_read) : -EIO);
            }
            uint64_t const PERF_COPY_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::READ_COPY);
            std::memcpy(dst + total_read, bp->data, chunk);
            perf_accounted_us +=
                perf_record_xfs_stage_elapsed(ker::mod::perf::WkiPerfLocalXfsOp::READ_COPY, PERF_COPY_STARTED_US, 0, chunk);
            brelse(bp);
        } else {
            // Partial or unaligned - fall back to single cached block.
            chunk = std::min(ctx->block_size - BLOCK_OFF, remaining);
            BufHead* bp = xfs_buf_read_data(ctx, DISK_BLOCK);
#ifdef XFS_BENCH
            acc_io += ker::mod::tsc::get_ns() - t0;
#endif
            perf_accounted_us += perf_record_xfs_stage_elapsed(ker::mod::perf::WkiPerfLocalXfsOp::READ_IO, PERF_IO_STARTED_US,
                                                               bp != nullptr ? 0 : -EIO, chunk);
            if (bp == nullptr) {
                return finish_read((total_read > 0) ? static_cast<ssize_t>(total_read) : -EIO);
            }
            uint64_t const PERF_COPY_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::READ_COPY);
            std::memcpy(dst + total_read, bp->data + BLOCK_OFF, chunk);
            perf_accounted_us +=
                perf_record_xfs_stage_elapsed(ker::mod::perf::WkiPerfLocalXfsOp::READ_COPY, PERF_COPY_STARTED_US, 0, chunk);
            brelse(bp);
        }

        total_read += chunk;
        remaining -= chunk;
    }

#ifdef XFS_BENCH
    s_read_ns_bmap.fetch_add(acc_bmap, std::memory_order_relaxed);
    s_read_ns_io.fetch_add(acc_io, std::memory_order_relaxed);
    s_read_bytes.fetch_add(total_read, std::memory_order_relaxed);
    uint64_t n = s_read_calls.fetch_add(1, std::memory_order_relaxed);
    if ((n & 63) == 63) {
        uint64_t nb = s_read_ns_bmap.exchange(0, std::memory_order_relaxed);
        uint64_t ni = s_read_ns_io.exchange(0, std::memory_order_relaxed);
        uint64_t by = s_read_bytes.exchange(0, std::memory_order_relaxed);
        uint64_t mbps = (ni > 0) ? (by * 1000ULL / ni) : 0;
        ker::mod::dbg::log("[XFS read bench] call#%lu: bmap=%luus io=%luus bytes=%lu io_MB/s~%lu\n", static_cast<unsigned long>(n),
                           static_cast<unsigned long>(nb / 1000ULL), static_cast<unsigned long>(ni / 1000ULL),
                           static_cast<unsigned long>(by), static_cast<unsigned long>(mbps));
    }
#endif

    return finish_read(static_cast<ssize_t>(total_read));
}

auto xfs_vfs_write_locked(File* f, const void* buf, size_t count, size_t offset, bool& throttle_after_unlock) -> ssize_t {
    if (f == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }

    XfsInode* ip = xfd->inode;
    XfsMountContext* ctx = xfd->mount;
    if (count != 0 && !ip->exec_content.can_mutate()) return -ETXTBSY;
    xfs_recent_write_stat_invalidate(xfd);

    if (ctx->read_only) {
        return -EROFS;
    }

    if (xfs_inode_isdir(ip)) {
        return -EISDIR;
    }

    if (count == 0) {
        return 0;
    }

    coherency::Publication publication(ip->coherency, coherency::Domain::CONTENT);
    const auto* src = static_cast<const uint8_t*>(buf);
    size_t total_written = 0;
    int write_error = 0;
    size_t bytes_since_pressure_check = 0;
    uint64_t const PERF_WRITE_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::WRITE);
    auto defer_dirty_throttle = [&]() {
        if (bytes_since_pressure_check >= XFS_DIRTY_THROTTLE_INTERVAL_BYTES) {
            bytes_since_pressure_check = 0;
            kick_dirty_buffer_cache_writeback(ctx->device);
            throttle_after_unlock = true;
        }
    };
    auto finish_write = [&](ssize_t result) -> ssize_t {
        uint64_t const BYTES = result > 0 ? static_cast<uint64_t>(result) : static_cast<uint64_t>(total_written);
        int32_t const STATUS = result < 0 ? static_cast<int32_t>(result) : 0;
        if (result < 0) {
            mod::dbg::log("[xfs] write: path=%s ino=%lu ret=%ld offset=%lu count=%lu written=%lu write_error=%d dirty=%u prealloc=%u",
                          f->vfs_path != nullptr ? f->vfs_path : "?", static_cast<unsigned long>(ip->ino), static_cast<long>(result),
                          static_cast<unsigned long>(offset), static_cast<unsigned long>(count), static_cast<unsigned long>(total_written),
                          write_error, ip->dirty ? 1U : 0U, xfd->may_have_eof_prealloc ? 1U : 0U);
        }
        if (PERF_WRITE_STARTED_US != 0) {
            uint32_t const LATENCY_US = perf_elapsed_since_us(PERF_WRITE_STARTED_US);
            ker::mod::perf::record_local_xfs_summary(ker::mod::perf::WkiPerfLocalXfsOp::WRITE, STATUS, LATENCY_US, true, BYTES);
            perf_record_xfs_slow_event(ker::mod::perf::WkiPerfLocalXfsOp::WRITE, STATUS, LATENCY_US, BYTES, WOS_PERF_CALLSITE());
        }
        return result;
    };

    auto buffered_write = [&](xfs_fsblock_t disk_block, size_t initial_block_off, size_t bytes, size_t src_offset,
                              bool fresh_allocation) -> bool {
        uint64_t const STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::BUFFERED_WRITE);
        size_t remaining_bytes = bytes;
        size_t block_off = initial_block_off;
        xfs_fsblock_t current_disk_block = disk_block;
        size_t current_src_offset = src_offset;
        bool ok = true;

        size_t const MAX_BATCH_BLOCKS = std::max<size_t>(1, XFS_BUFFERED_WRITE_BATCH_MAX_BYTES >> ctx->block_log);
        auto account_dirty_write = [&](size_t bytes_written) {
            bytes_since_pressure_check += bytes_written;
            defer_dirty_throttle();
        };

        while (remaining_bytes > 0) {
            if (block_off == 0 && remaining_bytes >= ctx->block_size) {
                size_t full_blocks = remaining_bytes >> ctx->block_log;
                while (full_blocks > 0) {
                    size_t const BATCH_BLOCKS = std::min(full_blocks, MAX_BATCH_BLOCKS);
                    size_t const BATCH_BYTES = BATCH_BLOCKS << ctx->block_log;
                    uint64_t const DEV_BLOCK = xfs_fsblock_to_dev_block(ctx, current_disk_block);
                    size_t const DEV_COUNT = xfs_fsb_to_dev_count(ctx, static_cast<xfs_filblks_t>(BATCH_BLOCKS));
                    BufHead* bp = bget_multi(ctx->device, DEV_BLOCK, DEV_COUNT, BufferReadClass::FILE_DATA);
                    if (bp == nullptr) {
                        ok = false;
                        break;
                    }

                    std::memcpy(bp->data, src + current_src_offset, BATCH_BYTES);
                    bdirty(bp);
                    brelse(bp);
                    account_dirty_write(BATCH_BYTES);

                    remaining_bytes -= BATCH_BYTES;
                    current_src_offset += BATCH_BYTES;
                    current_disk_block += static_cast<xfs_fsblock_t>(BATCH_BLOCKS);
                    full_blocks -= BATCH_BLOCKS;
                }
                if (!ok) {
                    break;
                }
                continue;
            }

            size_t const CHUNK = std::min(ctx->block_size - block_off, remaining_bytes);
            BufHead* bp = fresh_allocation ? xfs_buf_get_data(ctx, current_disk_block) : xfs_buf_read_data(ctx, current_disk_block);
            if (bp == nullptr) {
                ok = false;
                break;
            }

            if (fresh_allocation) {
                xfs_zero_fresh_block_unwritten_ranges(bp->data, ctx->block_size, block_off, CHUNK);
            }
            std::memcpy(bp->data + block_off, src + current_src_offset, CHUNK);
            bdirty(bp);
            brelse(bp);
            account_dirty_write(CHUNK);

            remaining_bytes -= CHUNK;
            current_src_offset += CHUNK;
            current_disk_block++;
            block_off = 0;
        }

        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::BUFFERED_WRITE, STARTED_US, ok ? 0 : -EIO, bytes - remaining_bytes);
        return ok;
    };

    auto write_extent_data = [&](xfs_fsblock_t disk_block, size_t initial_block_off, size_t bytes, size_t src_offset,
                                 bool fresh_allocation) -> bool {
        return buffered_write(disk_block, initial_block_off, bytes, src_offset, fresh_allocation);
    };

    auto try_mapped_write_with_existing_extent = [&]() -> ssize_t {
        if (ip->nblocks == 0) {
            return -EAGAIN;
        }

        size_t const WRITE_POS = offset;
        auto const FILE_BLOCK = static_cast<xfs_fileoff_t>(WRITE_POS >> ctx->block_log);
        size_t const BLOCK_OFF = WRITE_POS & (ctx->block_size - 1);

        XfsBmapResult bmap{};
        uint64_t const PERF_BMAP_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_BMAP);
        int const RET = xfs_bmap_lookup(ip, FILE_BLOCK, &bmap);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_BMAP, PERF_BMAP_STARTED_US, RET, 0);
        if (RET != 0) {
            return RET;
        }
        if (bmap.is_hole || bmap.startblock == NULLFSBLOCK || bmap.blockcount == 0 || bmap.unwritten) {
            return -EAGAIN;
        }

        size_t const EXTENT_BYTES = (static_cast<size_t>(bmap.blockcount) * ctx->block_size) - BLOCK_OFF;
        if (EXTENT_BYTES < count) {
            return -EAGAIN;
        }

        uint64_t const SIZE_BEFORE_WRITE = ip->size;
        uint64_t const MTIME_BEFORE_WRITE = ip->mtime;
        uint64_t const CTIME_BEFORE_WRITE = ip->ctime;
        bool const DIRTY_BEFORE_WRITE = ip->dirty;
        bool const APPEND_BLOCK_HAS_NO_OLD_BYTES = xfs_mapped_append_can_zero_without_read(WRITE_POS, ip->size, ctx->block_size);
        ip->size = std::max<uint64_t>(ip->size, offset + count);
        xfs_stamp_inode_data_change(ip);

        uint64_t const PERF_IO_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_IO);
        bool const WROTE = write_extent_data(bmap.startblock, BLOCK_OFF, count, 0, APPEND_BLOCK_HAS_NO_OLD_BYTES);
        if (!WROTE) {
            ip->size = SIZE_BEFORE_WRITE;
            ip->mtime = MTIME_BEFORE_WRITE;
            ip->ctime = CTIME_BEFORE_WRITE;
            ip->dirty = DIRTY_BEFORE_WRITE;
            perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_IO, PERF_IO_STARTED_US, -EIO, 0);
            return -EIO;
        }
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_IO, PERF_IO_STARTED_US, 0, count);

        total_written = count;
        if (ip->dirty) {
            xfd->close_may_need_inode_commit = true;
        }
        ctx->mutation_sequence.fetch_add(1, std::memory_order_release);
        if (!xfs_inode_isreg(ip)) ctx->lookup_sequence.fetch_add(1, std::memory_order_release);
        ker::vfs::Stat recent_write_stat{};
        if (xfs_inode_fill_stat(ip, &recent_write_stat) == 0) {
            xfs_recent_write_stat_store(xfd, recent_write_stat);
        }
        defer_dirty_throttle();
        return static_cast<ssize_t>(total_written);
    };

    // Even already-mapped writes can change inode size and timestamps.  Keep
    // them inside the mount mutation barrier so sync/unmount cannot publish a
    // clean journal between the inode scan and this fast path dirtying it.
    XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());
    ssize_t const MAPPED_FAST_RET = try_mapped_write_with_existing_extent();
    if (MAPPED_FAST_RET != -EAGAIN) {
        return finish_write(MAPPED_FAST_RET);
    }

    auto relock_if_more_metadata_needed = [&]() {
        if (total_written < count) {
            metadata_guard.lock();
        }
    };

#ifdef XFS_BENCH
    static std::atomic<uint64_t> s_wr_calls{0};
    static std::atomic<uint64_t> s_wr_ns_bmap{0};
    static std::atomic<uint64_t> s_wr_ns_alloc{0};
    static std::atomic<uint64_t> s_wr_ns_io{0};
    static std::atomic<uint64_t> s_wr_ns_ilog{0};
    static std::atomic<uint64_t> s_wr_bytes{0};
    static std::atomic<uint64_t> s_wr_hole_calls{0};
    static std::atomic<uint64_t> s_wr_map_calls{0};
    uint64_t acc_bmap = 0;
    uint64_t acc_alloc = 0;
    uint64_t acc_io = 0;
    uint64_t acc_ilog = 0;
    uint64_t loc_hole = 0;
    uint64_t loc_map = 0;
    uint64_t t0 = 0;
#endif
    while (total_written < count) {
        size_t const WRITE_POS = offset + total_written;
        auto file_block = static_cast<xfs_fileoff_t>(WRITE_POS >> ctx->block_log);
        size_t const BLOCK_OFF = WRITE_POS & (ctx->block_size - 1);

        XfsBmapResult bmap{};
        uint64_t const PERF_BMAP_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_BMAP);
#ifdef XFS_BENCH
        t0 = ker::mod::tsc::get_ns();
#endif
        int ret = xfs_bmap_lookup(ip, file_block, &bmap);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_BMAP, PERF_BMAP_STARTED_US, ret, 0);
#ifdef XFS_BENCH
        acc_bmap += ker::mod::tsc::get_ns() - t0;
#endif
        if (ret < 0) {
            write_error = ret;
            break;
        }

        if (bmap.is_hole || bmap.startblock == NULLFSBLOCK) {
            perf_record_xfs_count(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_HOLE_ITER);
#ifdef XFS_BENCH
            loc_hole++;
#endif
            // Hole: batch-allocate as many contiguous blocks as we can.
            size_t const REMAINING_BYTES = count - total_written;
            bool const EXTENT_PRESSURE = xfs_has_inline_extent_pressure(ip);
            bool const SEQUENTIAL_APPEND = WRITE_POS == ip->size;
            xfs_extlen_t const HOLE_BLOCKS =
                xfs_hole_write_alloc_blocks(WRITE_POS, BLOCK_OFF, REMAINING_BYTES, bmap.blockcount, ctx->block_size, ctx->block_log,
                                            EXTENT_PRESSURE, SEQUENTIAL_APPEND);

#ifdef XFS_BENCH
            t0 = ker::mod::tsc::get_ns();
#endif
            uint64_t const PERF_ALLOC_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_ALLOC);
            XfsTransaction* tp =
                xfs_trans_alloc(ctx, xfs_inode_isreg(ip) ? XfsLookupEffect::PRESERVE_FILE_DATA : XfsLookupEffect::INVALIDATE);
            if (tp == nullptr) {
                perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_ALLOC, PERF_ALLOC_STARTED_US, -ENOMEM, 0);
                write_error = -ENOMEM;
                break;
            }
            xfs_agnumber_t const PREF_AG = xfs_ino_ag(ip->ino, ctx->agino_log);

            XfsAllocReq req{};
            req.agno = PREF_AG;
            req.agbno = 0;
            req.maxlen = HOLE_BLOCKS;
            req.minlen = xfs_write_alloc_min_blocks(req.maxlen, EXTENT_PRESSURE, SEQUENTIAL_APPEND);
            req.alignment = 0;
            xfs_set_sequential_alloc_hint(ip, ctx, file_block, &req);

            XfsAllocResult alloc_result{};
            while (true) {
                ret = xfs_alloc_extent(ctx, tp, req, &alloc_result);
                if (ret != -ENOSPC || req.minlen == 1) {
                    break;
                }
                xfs_extlen_t const NEXT_MINLEN = xfs_write_alloc_next_min_blocks(req.minlen);
                if (NEXT_MINLEN >= req.minlen) {
                    req.minlen = 1;
                } else {
                    req.minlen = NEXT_MINLEN;
                }
            }
            if (ret != 0) {
                xfs_trans_cancel(tp);
                perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_ALLOC, PERF_ALLOC_STARTED_US, ret, 0);
                write_error = ret;
                break;
            }

            xfs_fsblock_t const DISK_BLOCK = xfs_agbno_to_fsbno(alloc_result.agno, alloc_result.agbno, ctx->ag_blk_log);

            XfsBmbtIrec new_ext{};
            new_ext.br_startoff = file_block;
            new_ext.br_startblock = DISK_BLOCK;
            new_ext.br_blockcount = alloc_result.len;
            new_ext.br_unwritten = false;

            ret = xfs_bmap_add_extent(ip, tp, new_ext);
            if (ret != 0) {
                xfs_trans_cancel(tp);
                perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_ALLOC, PERF_ALLOC_STARTED_US, ret, 0);
                write_error = ret;
                break;
            }

            // Write the data blocks covered by this allocation. Fresh blocks
            // use no-read buffer-cache gets because unwritten bytes are known-zero.
            uint64_t const SIZE_BEFORE_ALLOC = ip->size;
            uint64_t const MTIME_BEFORE_ALLOC = ip->mtime;
            uint64_t const CTIME_BEFORE_ALLOC = ip->ctime;
            bool const DIRTY_BEFORE_ALLOC = ip->dirty;
            size_t const EXTENT_BYTES = static_cast<size_t>(alloc_result.len) << ctx->block_log;
            size_t const WRITE_END = offset + count;
            size_t const EXTENT_START = static_cast<size_t>(file_block) << ctx->block_log;
            size_t const EXTENT_END = EXTENT_START + EXTENT_BYTES;

            // Compute the slice of this extent actually covered by [offset, offset+count).
            size_t const SLICE_START = std::max(EXTENT_START, offset + total_written);
            size_t const SLICE_END = std::min(EXTENT_END, WRITE_END);
            size_t const SLICE_BYTES = (SLICE_END > SLICE_START) ? (SLICE_END - SLICE_START) : 0;
            auto const WRITTEN_EOF_BLOCKS =
                static_cast<xfs_fileoff_t>((SLICE_END + static_cast<size_t>(ctx->block_size - 1)) >> ctx->block_log);
            if (file_block + alloc_result.len > WRITTEN_EOF_BLOCKS) {
                xfd->may_have_eof_prealloc = true;
                f->close_may_change_metadata = true;
            }

            ip->nblocks += alloc_result.len;
            if (SLICE_BYTES > 0 && SLICE_END > ip->size) {
                ip->size = SLICE_END;
            }
            ip->dirty = true;
            bool const LOGGED_DATA_CHANGE = SLICE_BYTES > 0;
            if (LOGGED_DATA_CHANGE) {
                xfs_stamp_inode_data_change(ip);
            }

            // Log inode into this allocation transaction so extent updates and
            // data-change metadata share one journal commit for
            // create/write/close-heavy workloads.
            xfs_trans_log_inode(tp, ip);

            ret = xfs_trans_commit(tp);
            perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_ALLOC, PERF_ALLOC_STARTED_US, ret,
                                  static_cast<uint64_t>(alloc_result.len) << ctx->block_log);
#ifdef XFS_BENCH
            acc_alloc += ker::mod::tsc::get_ns() - t0;
#endif
            if (ret != 0) {
                write_error = ret;
                break;
            }

            {
#ifdef XFS_BENCH
                t0 = ker::mod::tsc::get_ns();
#endif
                uint64_t const PERF_IO_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_IO);
                metadata_guard.unlock();
                if (SLICE_BYTES > 0) {
                    size_t const SLICE_BLOCK_OFF = SLICE_START - EXTENT_START;
                    bool const WROTE = write_extent_data(DISK_BLOCK + (SLICE_BLOCK_OFF >> ctx->block_log),
                                                         SLICE_BLOCK_OFF & (ctx->block_size - 1), SLICE_BYTES, SLICE_START - offset, true);
                    if (!WROTE) {
                        metadata_guard.lock();
                        ip->size = SIZE_BEFORE_ALLOC;
                        if (LOGGED_DATA_CHANGE) {
                            ip->mtime = MTIME_BEFORE_ALLOC;
                            ip->ctime = CTIME_BEFORE_ALLOC;
                        }
                        ip->dirty = DIRTY_BEFORE_ALLOC || LOGGED_DATA_CHANGE || ip->dirty;
                        static_cast<void>(xfs_commit_dirty_inode(ctx, ip, true));
#ifdef XFS_BENCH
                        acc_io += ker::mod::tsc::get_ns() - t0;
#endif
                        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_IO, PERF_IO_STARTED_US, -EIO, 0);
                        if (write_error == 0) {
                            write_error = -EIO;
                        }
                        goto write_done;
                    }
                    total_written += SLICE_BYTES;
                }
                perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_IO, PERF_IO_STARTED_US, 0, SLICE_BYTES);
#ifdef XFS_BENCH
                acc_io += ker::mod::tsc::get_ns() - t0;
#endif
                relock_if_more_metadata_needed();
            }
        } else {
            perf_record_xfs_count(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_MAP_ITER);
#ifdef XFS_BENCH
            loc_map++;
#endif
            // Block already mapped - write the contiguous extent in bulk.
            xfs_fsblock_t const DISK_BLOCK = bmap.startblock;

            size_t const EXTENT_BYTES = (static_cast<size_t>(bmap.blockcount) * ctx->block_size) - BLOCK_OFF;
            size_t const CHUNK = std::min(EXTENT_BYTES, count - total_written);
            bool const APPEND_BLOCK_HAS_NO_OLD_BYTES = xfs_mapped_append_can_zero_without_read(WRITE_POS, ip->size, ctx->block_size);
            uint64_t const SIZE_BEFORE_WRITE = ip->size;
            uint64_t const MTIME_BEFORE_WRITE = ip->mtime;
            uint64_t const CTIME_BEFORE_WRITE = ip->ctime;
            bool const DIRTY_BEFORE_WRITE = ip->dirty;
            ip->size = std::max<uint64_t>(ip->size, offset + total_written + CHUNK);
            xfs_stamp_inode_data_change(ip);

#ifdef XFS_BENCH
            t0 = ker::mod::tsc::get_ns();
#endif
            uint64_t const PERF_IO_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_IO);
            metadata_guard.unlock();
            bool const WROTE = write_extent_data(DISK_BLOCK, BLOCK_OFF, CHUNK, total_written, APPEND_BLOCK_HAS_NO_OLD_BYTES);
            if (!WROTE) {
                metadata_guard.lock();
                ip->size = SIZE_BEFORE_WRITE;
                ip->mtime = MTIME_BEFORE_WRITE;
                ip->ctime = CTIME_BEFORE_WRITE;
                ip->dirty = DIRTY_BEFORE_WRITE;
#ifdef XFS_BENCH
                acc_io += ker::mod::tsc::get_ns() - t0;
#endif
                perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_IO, PERF_IO_STARTED_US, -EIO, 0);
                if (write_error == 0) {
                    write_error = -EIO;
                }
                break;
            }
            perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_IO, PERF_IO_STARTED_US, 0, CHUNK);
#ifdef XFS_BENCH
            acc_io += ker::mod::tsc::get_ns() - t0;
#endif

            total_written += CHUNK;
            relock_if_more_metadata_needed();
        }
    }

write_done:
    if (total_written == 0) {
        if (ip->dirty) {
            xfd->close_may_need_inode_commit = true;
        }
        return finish_write(write_error != 0 ? write_error : -EIO);
    }

    // Update file size if we wrote past the end.
    ip->size = std::max<uint64_t>(ip->size, offset + total_written);
    if (ip->dirty) {
        xfd->close_may_need_inode_commit = true;
    }
    ker::vfs::Stat recent_write_stat{};
    if (xfs_inode_fill_stat(ip, &recent_write_stat) == 0) {
        xfs_recent_write_stat_store(xfd, recent_write_stat);
    }
    defer_dirty_throttle();

#ifdef XFS_BENCH
    s_wr_ns_bmap.fetch_add(acc_bmap, std::memory_order_relaxed);
    s_wr_ns_alloc.fetch_add(acc_alloc, std::memory_order_relaxed);
    s_wr_ns_io.fetch_add(acc_io, std::memory_order_relaxed);
    s_wr_ns_ilog.fetch_add(acc_ilog, std::memory_order_relaxed);
    s_wr_bytes.fetch_add(total_written, std::memory_order_relaxed);
    s_wr_hole_calls.fetch_add(loc_hole, std::memory_order_relaxed);
    s_wr_map_calls.fetch_add(loc_map, std::memory_order_relaxed);
    uint64_t n = s_wr_calls.fetch_add(1, std::memory_order_relaxed);
    if ((n & 63) == 63) {
        uint64_t nb = s_wr_ns_bmap.exchange(0, std::memory_order_relaxed);
        uint64_t na = s_wr_ns_alloc.exchange(0, std::memory_order_relaxed);
        uint64_t ni = s_wr_ns_io.exchange(0, std::memory_order_relaxed);
        uint64_t nil = s_wr_ns_ilog.exchange(0, std::memory_order_relaxed);
        uint64_t by = s_wr_bytes.exchange(0, std::memory_order_relaxed);
        uint64_t nhol = s_wr_hole_calls.exchange(0, std::memory_order_relaxed);
        uint64_t nmap = s_wr_map_calls.exchange(0, std::memory_order_relaxed);
        uint64_t mbps = (ni > 0) ? (by * 1000ULL / ni) : 0;
        ker::mod::dbg::log(
            "[XFS write bench] call#%lu: bmap=%luus alloc=%luus io=%luus ilog=%luus "
            "bytes=%lu io_MB/s~%lu hole_iters=%lu map_iters=%lu\n",
            static_cast<unsigned long>(n), static_cast<unsigned long>(nb / 1000ULL), static_cast<unsigned long>(na / 1000ULL),
            static_cast<unsigned long>(ni / 1000ULL), static_cast<unsigned long>(nil / 1000ULL), static_cast<unsigned long>(by),
            static_cast<unsigned long>(mbps), static_cast<unsigned long>(nhol), static_cast<unsigned long>(nmap));
    }
#endif

    return finish_write(static_cast<ssize_t>(total_written));
}

auto xfs_vfs_write(File* f, const void* buf, size_t count, size_t offset) -> ssize_t {
    if (f != nullptr && (static_cast<unsigned>(f->open_flags) & 3U) == ker::vfs::FILE_ACCESS_SEARCH_ONLY) return -EBADF;
    if (f == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }

    ker::mod::sched::resource::FileGrowth growth;
    if (!growth.bound_write(offset, count)) {
        return -EFBIG;
    }
    if (count == 0) {
        return 0;
    }
    bool throttle_after_unlock = false;
    ssize_t ret = 0;
    {
        ker::mod::sys::MutexGuard guard(xfd->inode->io_lock);
        ret = xfs_vfs_write_locked(f, buf, count, offset, throttle_after_unlock);
        if (ret > 0) {
            growth.committed(xfd->inode->size);
        }
    }
    if (throttle_after_unlock) {
        throttle_dirty_buffer_cache(xfd->mount->device);
    }
    return ret;
}

auto xfs_vfs_lseek(File* f, off_t offset, int whence) -> off_t {
    if (f == nullptr) {
        return -EBADF;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }

    off_t new_pos = 0;
    switch (whence) {
        case 0:  // SEEK_SET
            new_pos = offset;
            break;
        case 1:  // SEEK_CUR
            new_pos = f->pos + offset;
            break;
        case 2:  // SEEK_END
            new_pos = static_cast<off_t>(xfd->inode->size) + offset;
            break;
        default:
            return -EINVAL;
    }

    if (new_pos < 0) {
        return -EINVAL;
    }
    f->pos = new_pos;
    return new_pos;
}

auto xfs_vfs_isatty(File* /*f*/) -> bool { return false; }

// ============================================================================
// Readdir - uses xfs_dir_iterate
// ============================================================================

struct ReaddirCtx {
    XfsInode* parent{};
    XfsFileData* xfd{};
    DirEntry* entry{};       // output entry to fill
    size_t target_cookie{};  // next stable cookie requested by VFS
    size_t target_index{};   // legacy dense index requested by direct callers
    size_t current_index{};
    bool use_cookie{};
    bool found{};
    bool residual_seen{};
    int status{};
};

auto xfs_vfs_dirent_record_size(size_t name_len) -> uint16_t {
    size_t const BOUNDED_NAME_LEN = name_len < DIRENT_NAME_MAX - 1 ? name_len : DIRENT_NAME_MAX - 1;
    size_t const RECORD_SIZE =
        ((DIRENT_HEADER_SIZE + BOUNDED_NAME_LEN + 1 + DIRENT_RECORD_ALIGNMENT - 1) / DIRENT_RECORD_ALIGNMENT) * DIRENT_RECORD_ALIGNMENT;
    return static_cast<uint16_t>(RECORD_SIZE);
}

void xfs_readdir_fill_vfs_entry(const XfsDirEntry* xde, size_t current_index, bool use_cookie, DirEntry* entry) {
    entry->d_ino = xde->ino;
    entry->d_off = (current_index < 2 && !use_cookie) ? current_index + 1 : xde->cookie + 1;

    switch (xde->ftype) {
        case XFS_DIR3_FT_REG_FILE:
            entry->d_type = DT_REG;
            break;
        case XFS_DIR3_FT_DIR:
            entry->d_type = DT_DIR;
            break;
        case XFS_DIR3_FT_CHRDEV:
            entry->d_type = DT_CHR;
            break;
        case XFS_DIR3_FT_BLKDEV:
            entry->d_type = DT_BLK;
            break;
        case XFS_DIR3_FT_FIFO:
            entry->d_type = DT_FIFO;
            break;
        case XFS_DIR3_FT_SOCK:
            entry->d_type = DT_SOCK;
            break;
        case XFS_DIR3_FT_SYMLINK:
            entry->d_type = DT_LNK;
            break;
        default:
            entry->d_type = DT_UNKNOWN;
            break;
    }

    size_t const COPY_LEN = xde->namelen < DIRENT_NAME_MAX - 1 ? xde->namelen : DIRENT_NAME_MAX - 1;
    entry->d_reclen = xfs_vfs_dirent_record_size(COPY_LEN);
    std::memcpy(entry->d_name.data(), xde->name.data(), COPY_LEN);
    entry->d_name.at(COPY_LEN) = '\0';
}

auto readdir_entry_visibility(XfsInode* parent, const XfsDirEntry* entry) -> int {
    if (parent == nullptr || entry == nullptr) {
        return -EINVAL;
    }

    XfsDirEntry cached{};
    int cached_result = 0;
    bool may_have_removed_record = false;
    bool const CACHED = xfs_dentry_cache_lookup_parent(parent->mount, parent->ino, entry->name.data(), entry->namelen, &cached,
                                                       &cached_result, &may_have_removed_record);
    if (xfs_dir_index_known_complete(parent) && !may_have_removed_record && (!CACHED || cached_result != -ENOENT)) {
        return 1;
    }

    // Until a complete scan proves this generation, every data-area record
    // must agree with the pathname-authoritative index. Successful removal
    // also installs both a negative dentry and a per-directory summary bit,
    // which keeps verification enabled for residual records after a proof.
    return xfs_dir_entry_is_indexed(parent, entry);
}

auto readdir_callback(const XfsDirEntry* xde, void* ctx_ptr) -> int {
    auto* rctx = static_cast<ReaddirCtx*>(ctx_ptr);
    int const VISIBILITY = readdir_entry_visibility(rctx->parent, xde);
    if (VISIBILITY < 0) {
        rctx->status = VISIBILITY;
        return 1;
    }
    if (VISIBILITY == 0) {
        rctx->residual_seen = true;
        return 0;
    }

    bool const MATCH = rctx->use_cookie ? xde->cookie >= rctx->target_cookie : rctx->current_index == rctx->target_index;
    if (MATCH) {
        xfs_dir_observe_entry(rctx->parent, xde);
        xfs_readdir_seed_child_path_caches(rctx->xfd, xde);
        xfs_readdir_fill_vfs_entry(xde, rctx->current_index, rctx->use_cookie, rctx->entry);
        rctx->found = true;
        return 1;  // stop iteration
    }
    rctx->current_index++;
    return 0;  // continue
}

struct ReaddirBatchCtx {
    XfsInode* parent{};
    XfsFileData* xfd{};
    XfsReaddirCache* cache{};
    size_t target_cookie{};
    size_t target_index{};
    size_t current_index{};
    size_t next_request_index{};
    bool use_cookie{};
    bool residual_seen{};
    int status{};
};

auto xfs_readdir_cache_lookup(XfsFileData* xfd, size_t index, DirEntry* entry) -> bool {
    if (xfd == nullptr || xfd->inode == nullptr || xfd->readdir_cache == nullptr || entry == nullptr) {
        return false;
    }

    XfsReaddirCache* cache = xfd->readdir_cache;
    if (cache->dir_generation != xfd->inode->dir_generation) {
        cache->dir_generation = xfd->inode->dir_generation;
        cache->count = 0;
        return false;
    }

    for (size_t i = 0; i < cache->count; ++i) {
        XfsReaddirCacheEntry const& cached = cache->entries.at(i);
        if (cached.request_index == index) {
            *entry = cached.entry;
            return true;
        }
    }
    return false;
}

auto xfs_readdir_cache_ensure(XfsFileData* xfd) -> XfsReaddirCache* {
    if (xfd == nullptr || xfd->inode == nullptr) {
        return nullptr;
    }
    if (xfd->readdir_cache != nullptr) {
        return xfd->readdir_cache;
    }

    auto* cache = new (std::nothrow) XfsReaddirCache{};
    if (cache == nullptr) {
        return nullptr;
    }
    cache->dir_generation = xfd->inode->dir_generation;
    xfd->readdir_cache = cache;
    return cache;
}

auto readdir_batch_callback(const XfsDirEntry* xde, void* ctx_ptr) -> int {
    auto* ctx = static_cast<ReaddirBatchCtx*>(ctx_ptr);
    int const VISIBILITY = readdir_entry_visibility(ctx->parent, xde);
    if (VISIBILITY < 0) {
        ctx->status = VISIBILITY;
        return 1;
    }
    if (VISIBILITY == 0) {
        ctx->residual_seen = true;
        return 0;
    }

    bool const MATCH = ctx->use_cookie ? xde->cookie >= ctx->target_cookie : ctx->current_index >= ctx->target_index;
    if (!MATCH) {
        ctx->current_index++;
        return 0;
    }

    XfsReaddirCacheEntry& slot = ctx->cache->entries.at(ctx->cache->count);
    slot.request_index = ctx->next_request_index;
    xfs_dir_observe_entry(ctx->parent, xde);
    xfs_readdir_seed_child_path_caches(ctx->xfd, xde);
    xfs_readdir_fill_vfs_entry(xde, ctx->current_index, ctx->use_cookie, &slot.entry);
    ctx->next_request_index = slot.entry.d_off;
    ctx->cache->count++;
    ctx->current_index++;
    return ctx->cache->count == XFS_READDIR_CACHE_ENTRIES ? 1 : 0;
}

auto xfs_vfs_readdir(File* f, DirEntry* entry, size_t index) -> int {
    if (f != nullptr && (static_cast<unsigned>(f->open_flags) & 3U) == ker::vfs::FILE_ACCESS_SEARCH_ONLY) return -EBADF;
    if (f == nullptr || entry == nullptr) {
        return -EINVAL;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }

    if (!xfs_inode_isdir(xfd->inode)) {
        return -ENOTDIR;
    }

    XfsMetadataGuard metadata_guard(xfd->mount, true, WOS_PERF_CALLSITE());

    if (xfs_readdir_cache_lookup(xfd, index, entry)) {
        return 0;
    }

    XfsReaddirCache* cache = xfs_readdir_cache_ensure(xfd);
    if (cache == nullptr) {
        ReaddirCtx ctx{};
        ctx.parent = xfd->inode;
        ctx.xfd = xfd;
        ctx.entry = entry;
        ctx.target_cookie = index;
        ctx.target_index = index;
        ctx.current_index = 0;
        ctx.use_cookie = index >= XFS_READDIR_COOKIE_BASE;
        ctx.found = false;

        int const RET = xfs_dir_iterate(xfd->inode, readdir_callback, &ctx);
        if (RET < 0) {
            return RET;
        }
        if (ctx.status != 0) {
            return ctx.status;
        }
        if (!ctx.found && !ctx.residual_seen) {
            xfs_dir_index_note_complete(xfd->inode);
        }

        return ctx.found ? 0 : -1;  // -1 = no more entries
    }

    cache->dir_generation = xfd->inode->dir_generation;
    cache->count = 0;

    ReaddirBatchCtx batch_ctx{};
    batch_ctx.parent = xfd->inode;
    batch_ctx.xfd = xfd;
    batch_ctx.cache = cache;
    batch_ctx.target_cookie = index;
    batch_ctx.target_index = index;
    batch_ctx.current_index = 0;
    batch_ctx.next_request_index = index;
    batch_ctx.use_cookie = index >= XFS_READDIR_COOKIE_BASE;

    int const BATCH_RET = xfs_dir_iterate(xfd->inode, readdir_batch_callback, &batch_ctx);
    if (BATCH_RET < 0) {
        cache->count = 0;
        return BATCH_RET;
    }
    if (batch_ctx.status != 0) {
        cache->count = 0;
        return batch_ctx.status;
    }
    if (cache->count < XFS_READDIR_CACHE_ENTRIES && !batch_ctx.residual_seen) {
        xfs_dir_index_note_complete(xfd->inode);
    }
    if (xfs_readdir_cache_lookup(xfd, index, entry)) {
        return 0;
    }
    return -1;
}

// ============================================================================
// Readlink
// ============================================================================

auto xfs_vfs_readlink(File* f, char* buf, size_t bufsize) -> ssize_t {
    if (f == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }

    if (!xfs_inode_islnk(xfd->inode)) {
        return -EINVAL;
    }

    int const RET = xfs_readlink(xfd->inode, buf, bufsize);
    if (RET < 0) {
        return RET;
    }
    return static_cast<ssize_t>(RET);
}

// ============================================================================
// Truncate
// ============================================================================

auto xfs_vfs_truncate_impl(File* f, off_t length) -> int {
    if (f == nullptr) {
        return -EBADF;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }

    XfsInode* ip = xfd->inode;
    XfsMountContext* ctx = xfd->mount;

    if (ctx->read_only) {
        return -EROFS;
    }

    if (length < 0) {
        return -EINVAL;
    }

    ker::mod::sys::MutexGuard guard(ip->io_lock);
    if (!ip->exec_content.can_mutate()) return -ETXTBSY;
    coherency::Publication publication(ip->coherency, coherency::Domain::CONTENT);
    XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());

    auto new_size = static_cast<uint64_t>(length);

    uint64_t const OLD_SIZE = ip->size;
    bool const ZERO_TRUNCATE_RESETS_DATA = new_size == 0 && xfs_truncate_zero_resets_data(OLD_SIZE, ip->nblocks);
    if (new_size == OLD_SIZE && !ZERO_TRUNCATE_RESETS_DATA) {
        // Linux updates mtime/ctime for a successful truncate even when the
        // requested size already matches. Besides matching that observable
        // behavior, build tools use repeated O_TRUNC of a zero-byte lock file
        // as a cheap filesystem clock. Keep the data fork untouched, but
        // persist the timestamp through the ordinary close/fsync path.
        xfs_stamp_same_size_truncate(xfd, ip);
        return 0;
    }

    if (ZERO_TRUNCATE_RESETS_DATA) {
        if (OLD_SIZE != 0) {
            int const DISCARD_RET = discard_inode_data_buffers(ctx, ip, OLD_SIZE);
            if (DISCARD_RET < 0) {
                return DISCARD_RET;
            }
        }

        XfsTransaction* tp = xfs_trans_alloc(ctx, xfs_inode_isreg(ip) ? XfsLookupEffect::PRESERVE_FILE_DATA : XfsLookupEffect::INVALIDATE);
        if (tp == nullptr) {
            return -ENOMEM;
        }
        int const TRUNC_RET = xfs_inode_truncate_data(ip, tp);
        if (TRUNC_RET != 0) {
            xfs_trans_cancel(tp);
            return TRUNC_RET;
        }
        ip->size = 0;
        xfs_stamp_inode_data_change(ip);
        xfs_trans_log_inode(tp, ip);
        int const RET = xfs_trans_commit(tp);
        return (RET == 0) ? 0 : -EIO;
    }

    if (new_size < OLD_SIZE) {
        XfsTransaction* tp = xfs_trans_alloc(ctx, xfs_inode_isreg(ip) ? XfsLookupEffect::PRESERVE_FILE_DATA : XfsLookupEffect::INVALIDATE);
        if (tp == nullptr) {
            return -ENOMEM;
        }
        int const TRIM_RET = xfs_inode_trim_data_to_size(ip, tp, new_size);
        if (TRIM_RET != 0) {
            xfs_trans_cancel(tp);
            return TRIM_RET;
        }
        ip->size = new_size;
        xfs_stamp_inode_data_change(ip);
        xfs_trans_log_inode(tp, ip);
        int const RET = xfs_trans_commit(tp);
        return (RET == 0) ? 0 : -EIO;
    }

    // Update the inode size
    ip->size = new_size;
    xfs_stamp_inode_data_change(ip);

    XfsTransaction* tp = xfs_trans_alloc(ctx, xfs_inode_isreg(ip) ? XfsLookupEffect::PRESERVE_FILE_DATA : XfsLookupEffect::INVALIDATE);
    if (tp == nullptr) {
        return -ENOMEM;
    }
    xfs_trans_log_inode(tp, ip);
    int const RET = xfs_trans_commit(tp);
    return (RET == 0) ? 0 : -EIO;
}

auto xfs_vfs_truncate(File* f, off_t length) -> int {
    if (f != nullptr && (static_cast<unsigned>(f->open_flags) & 3U) == ker::vfs::FILE_ACCESS_SEARCH_ONLY) return -EBADF;
    if (length < 0) {
        return -EINVAL;
    }
    ker::mod::sched::resource::FileGrowth growth;
    if (!growth.allow_resize(static_cast<uint64_t>(length))) {
        return -EFBIG;
    }
    int const RET = xfs_vfs_truncate_impl(f, length);
    if (RET == 0) {
        growth.committed(static_cast<uint64_t>(length));
    }
    return RET;
}

// ============================================================================
// FileOperations vtable
// ============================================================================

FileOperations xfs_fops = {
    .vfs_open = nullptr,  // open is handled by xfs_open_path, not through fops
    .vfs_close = xfs_vfs_close,
    .vfs_read = xfs_vfs_read,
    .vfs_write = xfs_vfs_write,
    .vfs_lseek = xfs_vfs_lseek,
    .vfs_isatty = xfs_vfs_isatty,
    .vfs_readdir = xfs_vfs_readdir,
    .vfs_readlink = xfs_vfs_readlink,
    .vfs_truncate = xfs_vfs_truncate,
    .vfs_poll_check = nullptr,
    .vfs_poll_register_waiter = nullptr,
    .vfs_ioctl = nullptr,
};

}  // anonymous namespace

auto get_xfs_fops() -> FileOperations* { return &xfs_fops; }

void xfs_parent_path_cache_purge_mount(XfsMountContext* ctx) { xfs_parent_path_cache_purge_all_for_mount(ctx); }

namespace {

auto xfs_find_parent_and_name(const char* fs_path, XfsMountContext* ctx, XfsInode** parent_out, const char** name_out,
                              uint16_t* namelen_out, bool* cache_hit_out = nullptr, bool allow_parent_cache = true,
                              size_t known_fs_path_len = UNKNOWN_XFS_PATH_LEN, size_t* fs_path_len_out = nullptr) -> int;
auto xfs_lookup_with_cached_parent(const char* fs_path, XfsMountContext* ctx, XfsInode** ip_out,
                                   size_t known_fs_path_len = UNKNOWN_XFS_PATH_LEN, bool require_directory = false) -> int;

}  // namespace

#ifdef WOS_SELFTEST
auto xfs_selftest_hole_write_alloc_blocks(size_t block_off, size_t remaining_bytes, xfs_filblks_t hole_blocks, size_t block_size,
                                          uint32_t block_log, size_t write_pos, bool sequential_append) -> xfs_extlen_t {
    return xfs_hole_write_alloc_blocks(write_pos, block_off, remaining_bytes, hole_blocks, block_size, block_log, false, sequential_append);
}

auto xfs_selftest_write_alloc_min_blocks(xfs_extlen_t max_blocks, bool extent_pressure, bool sequential_append) -> xfs_extlen_t {
    return xfs_write_alloc_min_blocks(max_blocks, extent_pressure, sequential_append);
}

auto xfs_selftest_truncate_zero_resets_data(uint64_t old_size, uint64_t nblocks) -> bool {
    return xfs_truncate_zero_resets_data(old_size, nblocks);
}

auto xfs_selftest_same_size_truncate_stamps_metadata() -> bool {
    XfsFileData xfd{};
    XfsInode inode{};
    inode.mtime = 1;
    inode.ctime = 2;
    uint64_t const OLD_MTIME = inode.mtime;
    uint64_t const OLD_CTIME = inode.ctime;

    xfs_stamp_same_size_truncate(&xfd, &inode);

    return inode.dirty && xfd.close_may_need_inode_commit && inode.mtime != OLD_MTIME && inode.ctime != OLD_CTIME &&
           inode.mtime == inode.ctime;
}

auto xfs_selftest_close_should_trim_prealloc(int open_flags, bool created_by_open, bool may_have_eof_prealloc) -> bool {
    return xfs_close_should_trim_prealloc(open_flags, created_by_open, may_have_eof_prealloc);
}

auto xfs_selftest_close_should_commit_inode(bool close_may_need_inode_commit, int open_flags, bool created_by_open,
                                            bool may_have_eof_prealloc) -> bool {
    return xfs_close_should_commit_inode(close_may_need_inode_commit, open_flags, created_by_open, may_have_eof_prealloc);
}

auto xfs_selftest_inode_has_eof_prealloc() -> bool {
    constexpr size_t BLOCK_SIZE = 4096;
    constexpr uint32_t BLOCK_LOG = 12;

    XfsMountContext ctx{};
    ctx.block_size = BLOCK_SIZE;
    ctx.block_log = BLOCK_LOG;

    XfsInode ip{};
    ip.mount = &ctx;
    ip.data_fork.format = XFS_DINODE_FMT_EXTENTS;
    ip.data_fork.extents.count = 1;

    XfsBmbtIrec exact[]{{.br_startoff = 0, .br_startblock = 100, .br_blockcount = 2, .br_unwritten = false}};
    ip.size = 2 * BLOCK_SIZE;
    ip.data_fork.extents.list = exact;
    if (xfs_inode_has_eof_prealloc(&ip)) {
        return false;
    }

    ip.size = BLOCK_SIZE + 1;
    if (xfs_inode_has_eof_prealloc(&ip)) {
        return false;
    }

    XfsBmbtIrec tail[]{{.br_startoff = 0, .br_startblock = 100, .br_blockcount = 4, .br_unwritten = false}};
    ip.size = 2 * BLOCK_SIZE;
    ip.data_fork.extents.list = tail;
    if (!xfs_inode_has_eof_prealloc(&ip)) {
        return false;
    }

    XfsBmbtIrec after[]{{.br_startoff = 4, .br_startblock = 104, .br_blockcount = 1, .br_unwritten = false}};
    ip.data_fork.extents.list = after;
    if (!xfs_inode_has_eof_prealloc(&ip)) {
        return false;
    }

    ip.data_fork.extents.list = nullptr;
    return xfs_inode_has_eof_prealloc(&ip);
}

auto xfs_selftest_mapped_append_can_zero_without_read(size_t write_pos, uint64_t file_size, size_t block_size) -> bool {
    return xfs_mapped_append_can_zero_without_read(write_pos, file_size, block_size);
}

auto xfs_selftest_zero_fresh_block_preserves_write_range() -> bool {
    constexpr uint8_t SENTINEL = 0xA5;
    std::array<uint8_t, 16> block{};

    block.fill(SENTINEL);
    xfs_zero_fresh_block_unwritten_ranges(block.data(), block.size(), 4, 5);
    for (size_t i = 0; i < block.size(); ++i) {
        uint8_t const EXPECTED = (i >= 4 && i < 9) ? SENTINEL : 0;
        if (block.at(i) != EXPECTED) {
            return false;
        }
    }

    block.fill(SENTINEL);
    xfs_zero_fresh_block_unwritten_ranges(block.data(), block.size(), 0, block.size());
    for (uint8_t byte : block) {
        if (byte != SENTINEL) {
            return false;
        }
    }

    block.fill(SENTINEL);
    xfs_zero_fresh_block_unwritten_ranges(block.data(), block.size(), 12, 8);
    for (size_t i = 0; i < block.size(); ++i) {
        uint8_t const EXPECTED = i >= 12 ? SENTINEL : 0;
        if (block.at(i) != EXPECTED) {
            return false;
        }
    }

    return true;
}

auto xfs_selftest_read_batch_max_bytes(size_t block_size) -> size_t { return xfs_read_batch_max_bytes(block_size); }

auto xfs_selftest_parent_path_cache() -> bool {
    XfsMountContext mount_a{};
    XfsMountContext mount_b{};
    constexpr const char* PATH = "toolchain/src/llvm-project";
    constexpr const char* CHILD_PATH = "toolchain/src/llvm-project/llvm/lib/IR";
    size_t const PATH_LEN = std::strlen(PATH);
    constexpr xfs_ino_t INO = 42;

    xfs_parent_path_cache_purge_all_for_mount(&mount_a);
    xfs_parent_path_cache_purge_all_for_mount(&mount_b);

    xfs_ino_t ino = NULLFSINO;
    bool ok = !xfs_parent_path_cache_lookup_ino(&mount_a, PATH, PATH_LEN, &ino);
    xfs_parent_path_cache_store(&mount_a, PATH, PATH_LEN, INO);
    ok = ok && xfs_parent_path_cache_lookup_ino(&mount_a, PATH, PATH_LEN, &ino) && ino == INO;
    ok = ok && xfs_parent_path_cache_lookup_ino(&mount_a, CHILD_PATH, PATH_LEN, &ino) && ino == INO;
    ok = ok && !xfs_parent_path_cache_lookup_ino(&mount_b, PATH, PATH_LEN, &ino);
    xfs_parent_path_cache_purge_all_for_mount(&mount_a);
    ok = ok && !xfs_parent_path_cache_lookup_ino(&mount_a, PATH, PATH_LEN, &ino);
    return ok;
}

auto xfs_selftest_path_inode_cache_generation() -> bool {
    XfsMountContext mount{};
    constexpr const char* PATH = "dir/file";
    constexpr size_t PATH_LEN = 8;
    constexpr xfs_ino_t FIRST_INO = 42;
    constexpr xfs_ino_t SECOND_INO = 84;

    xfs_path_inode_cache_bump_generation();
    xfs_parent_path_cache_purge_all_for_mount(&mount);
    xfs_ino_t ino = NULLFSINO;
    uint8_t ftype = XFS_DIR3_FT_UNKNOWN;
    bool ok = !xfs_path_inode_cache_lookup_ino(&mount, PATH, PATH_LEN, &ino);
    xfs_path_inode_cache_store(&mount, PATH, PATH_LEN, FIRST_INO, XFS_DIR3_FT_REG_FILE);
    ok = ok && xfs_path_inode_cache_lookup_ino(&mount, PATH, PATH_LEN, &ino, &ftype) && ino == FIRST_INO && ftype == XFS_DIR3_FT_REG_FILE;
    ok = ok && xfs_path_exists(PATH, false, &mount, PATH_LEN) == 0;
    ok = ok && xfs_path_exists(PATH, true, &mount, PATH_LEN) == -ENOTDIR;
    xfs_path_inode_cache_bump_generation();
    ok = ok && !xfs_path_inode_cache_lookup_ino(&mount, PATH, PATH_LEN, &ino);
    xfs_path_inode_cache_store(&mount, PATH, PATH_LEN, SECOND_INO, XFS_DIR3_FT_DIR);
    ftype = XFS_DIR3_FT_UNKNOWN;
    ok = ok && xfs_path_inode_cache_lookup_ino(&mount, PATH, PATH_LEN, &ino, &ftype) && ino == SECOND_INO && ftype == XFS_DIR3_FT_DIR;
    ok = ok && xfs_path_exists(PATH, true, &mount, PATH_LEN) == 0;
    constexpr const char* PATH_CHILD = "dir/file/child";
    ok = ok && xfs_parent_path_cache_lookup_ino(&mount, PATH_CHILD, PATH_LEN, &ino) && ino == SECOND_INO;

    constexpr const char* OPENED_PATH = "dir/opened";
    constexpr size_t OPENED_PATH_LEN = sizeof("dir/opened") - 1;
    XfsInode opened{};
    opened.mount = &mount;
    opened.ino = 126;
    opened.mode = 0100644;
    opened.nlink = 1;
    xfs_path_inode_cache_store_inode(&mount, OPENED_PATH, OPENED_PATH_LEN, &opened);
    ftype = XFS_DIR3_FT_UNKNOWN;
    ok = ok && xfs_path_inode_cache_lookup_ino(&mount, OPENED_PATH, OPENED_PATH_LEN, &ino, &ftype) && ino == opened.ino &&
         ftype == XFS_DIR3_FT_REG_FILE;
    return ok;
}

auto xfs_selftest_path_inode_cache_exact_invalidate() -> bool {
    XfsMountContext mount{};
    constexpr const char* PATH = "dir/file";
    constexpr size_t PATH_LEN = sizeof("dir/file") - 1;
    constexpr const char* SIBLING_PATH = "dir/sibling";
    constexpr size_t SIBLING_PATH_LEN = sizeof("dir/sibling") - 1;
    constexpr xfs_ino_t PATH_INO = 42;
    constexpr xfs_ino_t SIBLING_INO = 84;

    xfs_path_inode_cache_bump_generation();
    xfs_ino_t ino = NULLFSINO;
    uint8_t ftype = XFS_DIR3_FT_UNKNOWN;
    xfs_path_inode_cache_store(&mount, PATH, PATH_LEN, PATH_INO, XFS_DIR3_FT_REG_FILE);
    xfs_path_inode_cache_store(&mount, SIBLING_PATH, SIBLING_PATH_LEN, SIBLING_INO, XFS_DIR3_FT_DIR);

    bool ok = xfs_path_inode_cache_lookup_ino(&mount, PATH, PATH_LEN, &ino, &ftype) && ino == PATH_INO && ftype == XFS_DIR3_FT_REG_FILE;
    ok = ok && xfs_path_inode_cache_lookup_ino(&mount, SIBLING_PATH, SIBLING_PATH_LEN, &ino, &ftype) && ino == SIBLING_INO &&
         ftype == XFS_DIR3_FT_DIR;

    xfs_path_inode_cache_invalidate_path(&mount, PATH, PATH_LEN);
    ok = ok && !xfs_path_inode_cache_lookup_ino(&mount, PATH, PATH_LEN, &ino, &ftype);
    ok = ok && xfs_path_inode_cache_lookup_ino(&mount, SIBLING_PATH, SIBLING_PATH_LEN, &ino, &ftype) && ino == SIBLING_INO &&
         ftype == XFS_DIR3_FT_DIR;
    return ok;
}

auto xfs_selftest_directory_lookup_seeds_parent_path_cache() -> bool {
    XfsMountContext mount{};
    mount.inode_size = 512;
    mount.feat_incompat = XFS_SB_FEAT_INCOMPAT_FTYPE;
    xfs_parent_path_cache_purge_all_for_mount(&mount);
    xfs_icache_purge(&mount);

    constexpr xfs_ino_t PARENT_INO = 100;
    constexpr xfs_ino_t CHILD_INO = 101;
    auto* parent_data = new (std::nothrow) uint8_t[32];
    auto* child_data = new (std::nothrow) uint8_t[sizeof(XfsDir2SfHdr)];
    auto* parent = new (std::nothrow) XfsInode{};
    auto* child = new (std::nothrow) XfsInode{};
    if (parent_data == nullptr || child_data == nullptr || parent == nullptr || child == nullptr) {
        delete[] parent_data;
        delete[] child_data;
        delete parent;
        delete child;
        return false;
    }

    std::memset(parent_data, 0, 32);
    auto* parent_hdr = reinterpret_cast<XfsDir2SfHdr*>(parent_data);
    parent_hdr->count = 1;
    parent_hdr->i8count = 0;
    parent_hdr->parent.at(3) = 7;
    uint8_t* p = parent_data + xfs_dir2_sf_hdr_size(parent_hdr);
    auto* sfep = reinterpret_cast<XfsDir2SfEntry*>(p);
    constexpr const char* CHILD_NAME = "child";
    constexpr uint8_t CHILD_NAME_LEN = 5;
    sfep->namelen = CHILD_NAME_LEN;
    sfep->offset.at(1) = 4;
    std::memcpy(xfs_dir2_sf_entry_name(sfep), CHILD_NAME, CHILD_NAME_LEN);
    uint8_t* ino_ptr = xfs_dir2_sf_entry_name(sfep) + CHILD_NAME_LEN;
    *ino_ptr++ = XFS_DIR3_FT_DIR;
    ino_ptr[0] = static_cast<uint8_t>((CHILD_INO >> 24U) & 0xffU);
    ino_ptr[1] = static_cast<uint8_t>((CHILD_INO >> 16U) & 0xffU);
    ino_ptr[2] = static_cast<uint8_t>((CHILD_INO >> 8U) & 0xffU);
    ino_ptr[3] = static_cast<uint8_t>(CHILD_INO & 0xffU);

    std::memset(child_data, 0, sizeof(XfsDir2SfHdr));
    auto* child_hdr = reinterpret_cast<XfsDir2SfHdr*>(child_data);
    child_hdr->i8count = 0;
    child_hdr->parent.at(3) = static_cast<uint8_t>(PARENT_INO);

    parent->ino = PARENT_INO;
    parent->mount = &mount;
    parent->mode = 0040755;
    parent->nlink = 2;
    parent->data_fork.format = XFS_DINODE_FMT_LOCAL;
    parent->data_fork.local.data = parent_data;
    parent->data_fork.local.size = static_cast<size_t>((ino_ptr + 4) - parent_data);
    parent->size = parent->data_fork.local.size;

    child->ino = CHILD_INO;
    child->mount = &mount;
    child->mode = 0040755;
    child->nlink = 2;
    child->data_fork.format = XFS_DINODE_FMT_LOCAL;
    child->data_fork.local.data = child_data;
    child->data_fork.local.size = 6;
    child->size = child->data_fork.local.size;

    if (xfs_inode_cache_new(parent) != 0) {
        delete[] parent_data;
        delete[] child_data;
        delete parent;
        delete child;
        return false;
    }
    if (xfs_inode_cache_new(child) != 0) {
        xfs_icache_purge(&mount);
        delete[] child_data;
        delete child;
        return false;
    }

    constexpr const char* PARENT_PATH = "cached-parent";
    xfs_parent_path_cache_store(&mount, PARENT_PATH, std::strlen(PARENT_PATH), PARENT_INO);

    XfsInode* result = nullptr;
    int const LOOKUP_RET = xfs_lookup_with_cached_parent("cached-parent/child", &mount, &result);
    bool ok = LOOKUP_RET == 0 && result != nullptr && result->ino == CHILD_INO;
    if (result != nullptr) {
        xfs_inode_release(result);
    }

    constexpr const char* DENTRY_PARENT_PATH = "dentry-parent";
    constexpr size_t DENTRY_PARENT_PATH_LEN = sizeof("dentry-parent") - 1;
    constexpr xfs_ino_t DENTRY_PARENT_INO = 202;
    XfsInode dentry_parent{};
    dentry_parent.mount = &mount;
    dentry_parent.ino = DENTRY_PARENT_INO;
    XfsDirEntry dentry_entry{};
    dentry_entry.ino = CHILD_INO;
    dentry_entry.ftype = XFS_DIR3_FT_DIR;
    dentry_entry.namelen = CHILD_NAME_LEN;
    std::memcpy(dentry_entry.name.data(), CHILD_NAME, CHILD_NAME_LEN);
    dentry_entry.name.at(CHILD_NAME_LEN) = '\0';
    xfs_parent_path_cache_store(&mount, DENTRY_PARENT_PATH, DENTRY_PARENT_PATH_LEN, DENTRY_PARENT_INO);
    xfs_dir_observe_entry(&dentry_parent, &dentry_entry);
    xfs_path_inode_cache_bump_generation();
    result = nullptr;
    int const DENTRY_LOOKUP_RET = xfs_lookup_with_cached_parent("dentry-parent/child", &mount, &result);
    ok = ok && DENTRY_LOOKUP_RET == 0 && result != nullptr && result->ino == CHILD_INO;
    if (result != nullptr) {
        xfs_inode_release(result);
    }

    xfs_path_inode_cache_bump_generation();
    XfsInode* dentry_grandchild_parent = nullptr;
    const char* dentry_grandchild_name = nullptr;
    uint16_t dentry_grandchild_name_len = 0;
    bool dentry_parent_cache_hit = false;
    int const DENTRY_PARENT_FIND_RET =
        xfs_find_parent_and_name("dentry-parent/child/grandchild", &mount, &dentry_grandchild_parent, &dentry_grandchild_name,
                                 &dentry_grandchild_name_len, &dentry_parent_cache_hit, true);
    ok = ok && DENTRY_PARENT_FIND_RET == 0 && dentry_parent_cache_hit && dentry_grandchild_parent != nullptr &&
         dentry_grandchild_parent->ino == CHILD_INO && dentry_grandchild_name_len == std::strlen("grandchild") &&
         std::strncmp(dentry_grandchild_name, "grandchild", dentry_grandchild_name_len) == 0;
    if (dentry_grandchild_parent != nullptr) {
        xfs_inode_release(dentry_grandchild_parent);
    }

    constexpr const char* MISSING_PARENT_NAME = "missing";
    constexpr uint16_t MISSING_PARENT_NAME_LEN = sizeof("missing") - 1;
    XfsDirEntry missing_entry{};
    int const MISSING_SEED_RET = xfs_dir_lookup(parent, MISSING_PARENT_NAME, MISSING_PARENT_NAME_LEN, &missing_entry);
    xfs_path_inode_cache_bump_generation();
    XfsDentryCacheStats before_missing_parent_lookup{};
    XfsDentryCacheStats after_missing_parent_lookup{};
    xfs_dentry_cache_stats(before_missing_parent_lookup);
    XfsInode* missing_grandchild_parent = nullptr;
    const char* missing_grandchild_name = nullptr;
    uint16_t missing_grandchild_name_len = 0;
    int const MISSING_PARENT_FIND_RET = xfs_find_parent_and_name("cached-parent/missing/grandchild", &mount, &missing_grandchild_parent,
                                                                 &missing_grandchild_name, &missing_grandchild_name_len, nullptr, true);
    xfs_dentry_cache_stats(after_missing_parent_lookup);
    ok = ok && MISSING_SEED_RET == -ENOENT && MISSING_PARENT_FIND_RET == -ENOENT && missing_grandchild_parent == nullptr &&
         after_missing_parent_lookup.hits > before_missing_parent_lookup.hits;
    if (missing_grandchild_parent != nullptr) {
        xfs_inode_release(missing_grandchild_parent);
    }

    XfsInode* grandchild_parent = nullptr;
    const char* grandchild_name = nullptr;
    uint16_t grandchild_name_len = 0;
    bool cache_hit = false;
    XfsDentryCacheStats before_direct_parent_lookup{};
    XfsDentryCacheStats after_direct_parent_lookup{};
    xfs_dentry_cache_stats(before_direct_parent_lookup);
    int const FIND_RET = xfs_find_parent_and_name("cached-parent/child/grandchild", &mount, &grandchild_parent, &grandchild_name,
                                                  &grandchild_name_len, &cache_hit, true);
    xfs_dentry_cache_stats(after_direct_parent_lookup);
    ok = ok && FIND_RET == 0 && cache_hit && grandchild_parent != nullptr && grandchild_parent->ino == CHILD_INO &&
         grandchild_name_len == std::strlen("grandchild") && std::strncmp(grandchild_name, "grandchild", grandchild_name_len) == 0;
    ok = ok && after_direct_parent_lookup.hits == before_direct_parent_lookup.hits &&
         after_direct_parent_lookup.misses == before_direct_parent_lookup.misses;
    if (grandchild_parent != nullptr) {
        xfs_inode_release(grandchild_parent);
    }

    constexpr const char* PATH_INODE_PARENT_PATH = "path-inode-parent";
    xfs_path_inode_cache_store(&mount, PATH_INODE_PARENT_PATH, std::strlen(PATH_INODE_PARENT_PATH), CHILD_INO, XFS_DIR3_FT_DIR);
    XfsInode* path_inode_parent = nullptr;
    const char* path_inode_child_name = nullptr;
    uint16_t path_inode_child_name_len = 0;
    bool path_inode_cache_hit = false;
    int const PATH_INODE_PARENT_RET =
        xfs_find_parent_and_name("path-inode-parent/grandchild", &mount, &path_inode_parent, &path_inode_child_name,
                                 &path_inode_child_name_len, &path_inode_cache_hit, true);
    ok = ok && PATH_INODE_PARENT_RET == 0 && path_inode_cache_hit && path_inode_parent != nullptr && path_inode_parent->ino == CHILD_INO &&
         path_inode_child_name_len == std::strlen("grandchild") &&
         std::strncmp(path_inode_child_name, "grandchild", path_inode_child_name_len) == 0;
    if (path_inode_parent != nullptr) {
        xfs_inode_release(path_inode_parent);
    }

    xfs_parent_path_cache_purge_all_for_mount(&mount);
    xfs_icache_purge(&mount);
    return ok;
}

auto xfs_selftest_walk_path_seeds_ancestor_parent_cache() -> bool {
    XfsMountContext mount{};
    mount.inode_size = 512;
    mount.feat_incompat = XFS_SB_FEAT_INCOMPAT_FTYPE;
    mount.root_ino = 100;
    xfs_parent_path_cache_purge_all_for_mount(&mount);
    xfs_icache_purge(&mount);

    auto make_dir = [&mount](xfs_ino_t ino, xfs_ino_t parent_ino, const char* child_name, xfs_ino_t child_ino) -> XfsInode* {
        size_t const CHILD_LEN = child_name != nullptr ? std::strlen(child_name) : 0;
        size_t const DATA_CAPACITY = CHILD_LEN == 0 ? 6 : 64;
        auto* data = new (std::nothrow) uint8_t[DATA_CAPACITY];
        auto* inode = new (std::nothrow) XfsInode{};
        if (data == nullptr || inode == nullptr || CHILD_LEN > UINT8_MAX) {
            delete[] data;
            delete inode;
            return nullptr;
        }

        std::memset(data, 0, DATA_CAPACITY);
        auto* hdr = reinterpret_cast<XfsDir2SfHdr*>(data);
        hdr->count = CHILD_LEN == 0 ? 0 : 1;
        hdr->i8count = 0;
        hdr->parent.at(0) = static_cast<uint8_t>((parent_ino >> 24U) & 0xffU);
        hdr->parent.at(1) = static_cast<uint8_t>((parent_ino >> 16U) & 0xffU);
        hdr->parent.at(2) = static_cast<uint8_t>((parent_ino >> 8U) & 0xffU);
        hdr->parent.at(3) = static_cast<uint8_t>(parent_ino & 0xffU);

        size_t data_size = xfs_dir2_sf_hdr_size(hdr);
        if (CHILD_LEN != 0) {
            uint8_t* p = data + data_size;
            auto* sfep = reinterpret_cast<XfsDir2SfEntry*>(p);
            sfep->namelen = static_cast<uint8_t>(CHILD_LEN);
            sfep->offset.at(1) = 4;
            std::memcpy(xfs_dir2_sf_entry_name(sfep), child_name, CHILD_LEN);
            uint8_t* ino_ptr = xfs_dir2_sf_entry_name(sfep) + CHILD_LEN;
            *ino_ptr++ = XFS_DIR3_FT_DIR;
            ino_ptr[0] = static_cast<uint8_t>((child_ino >> 24U) & 0xffU);
            ino_ptr[1] = static_cast<uint8_t>((child_ino >> 16U) & 0xffU);
            ino_ptr[2] = static_cast<uint8_t>((child_ino >> 8U) & 0xffU);
            ino_ptr[3] = static_cast<uint8_t>(child_ino & 0xffU);
            data_size = static_cast<size_t>((ino_ptr + 4) - data);
        }

        inode->ino = ino;
        inode->mount = &mount;
        inode->mode = 0040755;
        inode->nlink = 2;
        inode->data_fork.format = XFS_DINODE_FMT_LOCAL;
        inode->data_fork.local.data = data;
        inode->data_fork.local.size = data_size;
        inode->size = data_size;
        return inode;
    };
    auto destroy_uncached_dir = [](XfsInode* inode) {
        if (inode == nullptr) {
            return;
        }
        delete[] inode->data_fork.local.data;
        inode->data_fork.local.data = nullptr;
        delete inode;
    };

    constexpr xfs_ino_t ROOT_INO = 100;
    constexpr xfs_ino_t A_INO = 101;
    constexpr xfs_ino_t B_INO = 102;
    constexpr xfs_ino_t C_INO = 103;

    auto* root = make_dir(ROOT_INO, ROOT_INO, "a", A_INO);
    auto* dir_a = make_dir(A_INO, ROOT_INO, "b", B_INO);
    auto* dir_b = make_dir(B_INO, A_INO, "c", C_INO);
    auto* dir_c = make_dir(C_INO, B_INO, nullptr, NULLFSINO);
    if (root == nullptr || dir_a == nullptr || dir_b == nullptr || dir_c == nullptr) {
        destroy_uncached_dir(root);
        destroy_uncached_dir(dir_a);
        destroy_uncached_dir(dir_b);
        destroy_uncached_dir(dir_c);
        return false;
    }

    bool cached_root = false;
    bool cached_a = false;
    bool cached_b = false;
    bool cached_c = false;
    bool ok = xfs_inode_cache_new(root) == 0;
    cached_root = ok;
    ok = ok && xfs_inode_cache_new(dir_a) == 0;
    cached_a = ok;
    ok = ok && xfs_inode_cache_new(dir_b) == 0;
    cached_b = ok;
    ok = ok && xfs_inode_cache_new(dir_c) == 0;
    cached_c = ok;
    if (!ok) {
        xfs_icache_purge(&mount);
        if (!cached_root) {
            destroy_uncached_dir(root);
        }
        if (!cached_a) {
            destroy_uncached_dir(dir_a);
        }
        if (!cached_b) {
            destroy_uncached_dir(dir_b);
        }
        if (!cached_c) {
            destroy_uncached_dir(dir_c);
        }
        return false;
    }

    XfsInode* result = walk_path(&mount, "a/b/c");
    ok = result != nullptr && result->ino == C_INO;
    if (result != nullptr) {
        xfs_inode_release(result);
    }

    xfs_ino_t cached_ino = NULLFSINO;
    uint8_t cached_ftype = XFS_DIR3_FT_UNKNOWN;
    ok = ok && xfs_path_inode_cache_lookup_ino(&mount, "a", std::strlen("a"), &cached_ino, &cached_ftype) && cached_ino == A_INO &&
         cached_ftype == XFS_DIR3_FT_DIR;
    ok = ok && xfs_path_inode_cache_lookup_ino(&mount, "a/b", std::strlen("a/b"), &cached_ino, &cached_ftype) && cached_ino == B_INO &&
         cached_ftype == XFS_DIR3_FT_DIR;
    ok = ok && xfs_path_inode_cache_lookup_ino(&mount, "a/b/c", std::strlen("a/b/c"), &cached_ino, &cached_ftype) && cached_ino == C_INO &&
         cached_ftype == XFS_DIR3_FT_DIR;
    ok = ok && xfs_parent_path_cache_lookup_ino(&mount, "a", std::strlen("a"), &cached_ino) && cached_ino == A_INO;
    ok = ok && xfs_parent_path_cache_lookup_ino(&mount, "a/b", std::strlen("a/b"), &cached_ino) && cached_ino == B_INO;
    ok = ok && xfs_parent_path_cache_lookup_ino(&mount, "a/b/c", std::strlen("a/b/c"), &cached_ino) && cached_ino == C_INO;

    XfsInode* sibling_parent = nullptr;
    const char* sibling_name = nullptr;
    uint16_t sibling_name_len = 0;
    bool cache_hit = false;
    int const FIND_RET =
        xfs_find_parent_and_name("a/b/sibling", &mount, &sibling_parent, &sibling_name, &sibling_name_len, &cache_hit, true);
    ok = ok && FIND_RET == 0 && cache_hit && sibling_parent != nullptr && sibling_parent->ino == B_INO &&
         sibling_name_len == std::strlen("sibling") && std::strncmp(sibling_name, "sibling", sibling_name_len) == 0;
    if (sibling_parent != nullptr) {
        xfs_inode_release(sibling_parent);
    }

    xfs_inode_release(root);
    xfs_inode_release(dir_a);
    xfs_inode_release(dir_b);
    xfs_inode_release(dir_c);
    xfs_parent_path_cache_purge_all_for_mount(&mount);
    xfs_icache_purge(&mount);
    return ok;
}

auto xfs_selftest_readdir_cache_batches_sequential_scan() -> bool {
    XfsMountContext mount{};
    mount.inode_size = 512;
    mount.feat_incompat = XFS_SB_FEAT_INCOMPAT_FTYPE;
    xfs_dentry_cache_purge_mount(&mount);
    xfs_path_inode_cache_bump_generation();

    std::array<uint8_t, 192> data{};
    auto* hdr = reinterpret_cast<XfsDir2SfHdr*>(data.data());
    hdr->count = 6;
    hdr->i8count = 0;
    hdr->parent.at(3) = 7;

    uint8_t* p = data.data() + xfs_dir2_sf_hdr_size(hdr);
    auto append_entry = [&](const char* name, uint16_t stored_offset, uint32_t ino, uint8_t ftype = XFS_DIR3_FT_REG_FILE) {
        auto const name_len = static_cast<uint8_t>(std::strlen(name));
        auto* sfep = reinterpret_cast<XfsDir2SfEntry*>(p);
        sfep->namelen = name_len;
        sfep->offset.at(0) = static_cast<uint8_t>(stored_offset >> 8U);
        sfep->offset.at(1) = static_cast<uint8_t>(stored_offset & 0xffU);
        std::memcpy(xfs_dir2_sf_entry_name(sfep), name, name_len);
        uint8_t* ino_ptr = xfs_dir2_sf_entry_name(sfep) + name_len;
        *ino_ptr++ = ftype;
        ino_ptr[0] = static_cast<uint8_t>((ino >> 24U) & 0xffU);
        ino_ptr[1] = static_cast<uint8_t>((ino >> 16U) & 0xffU);
        ino_ptr[2] = static_cast<uint8_t>((ino >> 8U) & 0xffU);
        ino_ptr[3] = static_cast<uint8_t>(ino & 0xffU);
        p = ino_ptr + 4;
    };

    append_entry("f0", 4, 40);
    append_entry("f1", 5, 41, XFS_DIR3_FT_DIR);
    append_entry("f2", 6, 42);
    append_entry("f3", 7, 43);
    append_entry("f4", 8, 44);
    append_entry("f5", 9, 45);

    XfsInode dir{};
    dir.ino = 100;
    dir.mount = &mount;
    dir.mode = 0040755;
    dir.data_fork.format = XFS_DINODE_FMT_LOCAL;
    dir.data_fork.local.data = data.data();
    dir.data_fork.local.size = static_cast<size_t>(p - data.data());
    dir.size = dir.data_fork.local.size;
    // This cache-batching fixture has no backing inode table or mount
    // geometry. Model a directory whose index was already proven so the test
    // remains focused on batching and generation invalidation.
    xfs_dir_index_note_complete(&dir);

    XfsFileData xfd{};
    xfd.mount = &mount;
    xfd.inode = &dir;
    xfs_file_data_set_fs_path(&xfd, "src", sizeof("src") - 1);

    File file{};
    file.private_data = &xfd;
    file.is_directory = true;
    file.fs_type = FSType::XFS;

    auto cleanup = [&xfd]() {
        delete xfd.readdir_cache;
        xfd.readdir_cache = nullptr;
    };

    XfsDentryCacheStats before_readdir{};
    xfs_dentry_cache_stats(before_readdir);

    DirEntry dot{};
    if (xfs_vfs_readdir(&file, &dot, 0) != 0 || std::strcmp(dot.d_name.data(), ".") != 0) {
        cleanup();
        return false;
    }
    if (xfd.readdir_cache == nullptr || xfd.readdir_cache->count < 5) {
        cleanup();
        return false;
    }

    DirEntry dotdot{};
    if (!xfs_readdir_cache_lookup(&xfd, dot.d_off, &dotdot) || std::strcmp(dotdot.d_name.data(), "..") != 0) {
        cleanup();
        return false;
    }

    DirEntry first_real{};
    if (!xfs_readdir_cache_lookup(&xfd, dotdot.d_off, &first_real) || std::strcmp(first_real.d_name.data(), "f0") != 0) {
        cleanup();
        return false;
    }
    xfs_ino_t cached_f0_ino = NULLFSINO;
    uint8_t cached_f0_ftype = XFS_DIR3_FT_UNKNOWN;
    if (!xfs_path_inode_cache_lookup_ino(&mount, "src/f0", sizeof("src/f0") - 1, &cached_f0_ino, &cached_f0_ftype) || cached_f0_ino != 40 ||
        cached_f0_ftype != XFS_DIR3_FT_REG_FILE) {
        cleanup();
        return false;
    }

    DirEntry second_real{};
    if (!xfs_readdir_cache_lookup(&xfd, first_real.d_off, &second_real) || std::strcmp(second_real.d_name.data(), "f1") != 0) {
        cleanup();
        return false;
    }
    xfs_ino_t cached_f1_parent_ino = NULLFSINO;
    bool const F1_PARENT_CACHED =
        xfs_parent_path_cache_lookup_ino(&mount, "src/f1", sizeof("src/f1") - 1, &cached_f1_parent_ino) && cached_f1_parent_ino == 41;
    if (!F1_PARENT_CACHED) {
        cleanup();
        return false;
    }

    XfsDentryCacheStats after_readdir{};
    xfs_dentry_cache_stats(after_readdir);
    XfsDirEntry observed{};
    if (xfs_dir_lookup(&dir, "f0", 2, &observed) != 0 || observed.ino != 40) {
        cleanup();
        return false;
    }
    XfsDentryCacheStats after_lookup{};
    xfs_dentry_cache_stats(after_lookup);
    if (after_readdir.stores <= before_readdir.stores || after_lookup.hits <= after_readdir.hits) {
        cleanup();
        return false;
    }

    dir.dir_generation++;
    xfs_dir_index_note_complete(&dir);
    DirEntry stale{};
    if (xfs_readdir_cache_lookup(&xfd, first_real.d_off, &stale) || xfd.readdir_cache->count != 0 ||
        xfd.readdir_cache->dir_generation != dir.dir_generation) {
        cleanup();
        return false;
    }

    DirEntry refilled{};
    bool const OK = xfs_vfs_readdir(&file, &refilled, first_real.d_off) == 0 && std::strcmp(refilled.d_name.data(), "f1") == 0 &&
                    xfd.readdir_cache != nullptr && xfd.readdir_cache->count > 0;
    cleanup();
    return OK;
}

auto xfs_selftest_readlink_path_uses_dentry_type() -> bool {
    XfsMountContext mount{};
    mount.inode_size = 512;
    mount.feat_incompat = XFS_SB_FEAT_INCOMPAT_FTYPE;
    constexpr xfs_ino_t ROOT_INO = 100;
    mount.root_ino = ROOT_INO;

    xfs_dentry_cache_purge_mount(&mount);
    xfs_path_inode_cache_bump_generation();

    auto* data = new (std::nothrow) uint8_t[64];
    auto* dir_data = new (std::nothrow) uint8_t[sizeof(XfsDir2SfHdr)];
    if (data == nullptr || dir_data == nullptr) {
        delete[] data;
        delete[] dir_data;
        return false;
    }
    std::memset(data, 0, 64);
    auto* hdr = reinterpret_cast<XfsDir2SfHdr*>(data);
    hdr->count = 2;
    hdr->i8count = 0;
    hdr->parent.at(3) = 7;

    auto* sfep = reinterpret_cast<XfsDir2SfEntry*>(data + xfs_dir2_sf_hdr_size(hdr));
    constexpr const char* NAME = "regular";
    constexpr uint8_t NAME_LEN = 7;
    sfep->namelen = NAME_LEN;
    sfep->offset.at(0) = 0;
    sfep->offset.at(1) = 4;
    std::memcpy(xfs_dir2_sf_entry_name(sfep), NAME, NAME_LEN);
    uint8_t* ino_ptr = xfs_dir2_sf_entry_name(sfep) + NAME_LEN;
    *ino_ptr++ = XFS_DIR3_FT_REG_FILE;
    constexpr xfs_ino_t CHILD_INO = 4242;
    ino_ptr[0] = static_cast<uint8_t>((CHILD_INO >> 24U) & 0xffU);
    ino_ptr[1] = static_cast<uint8_t>((CHILD_INO >> 16U) & 0xffU);
    ino_ptr[2] = static_cast<uint8_t>((CHILD_INO >> 8U) & 0xffU);
    ino_ptr[3] = static_cast<uint8_t>(CHILD_INO & 0xffU);

    auto* dir_sfep = reinterpret_cast<XfsDir2SfEntry*>(ino_ptr + 4);
    constexpr const char* DIR_NAME = "dir";
    constexpr uint8_t DIR_NAME_LEN = 3;
    dir_sfep->namelen = DIR_NAME_LEN;
    dir_sfep->offset.at(1) = 5;
    std::memcpy(xfs_dir2_sf_entry_name(dir_sfep), DIR_NAME, DIR_NAME_LEN);
    uint8_t* dir_ino_ptr = xfs_dir2_sf_entry_name(dir_sfep) + DIR_NAME_LEN;
    *dir_ino_ptr++ = XFS_DIR3_FT_DIR;
    constexpr xfs_ino_t DIR_INO = 4243;
    dir_ino_ptr[0] = static_cast<uint8_t>((DIR_INO >> 24U) & 0xffU);
    dir_ino_ptr[1] = static_cast<uint8_t>((DIR_INO >> 16U) & 0xffU);
    dir_ino_ptr[2] = static_cast<uint8_t>((DIR_INO >> 8U) & 0xffU);
    dir_ino_ptr[3] = static_cast<uint8_t>(DIR_INO & 0xffU);

    std::memset(dir_data, 0, sizeof(XfsDir2SfHdr));
    auto* dir_hdr = reinterpret_cast<XfsDir2SfHdr*>(dir_data);
    dir_hdr->i8count = 0;
    dir_hdr->parent.at(3) = static_cast<uint8_t>(ROOT_INO);

    auto* root = new (std::nothrow) XfsInode{};
    auto* dir = new (std::nothrow) XfsInode{};
    if (root == nullptr || dir == nullptr) {
        delete root;
        delete dir;
        delete[] data;
        delete[] dir_data;
        return false;
    }
    root->ino = ROOT_INO;
    root->mount = &mount;
    root->mode = 0040755;
    root->nlink = 2;
    root->data_fork.format = XFS_DINODE_FMT_LOCAL;
    root->data_fork.local.data = data;
    root->data_fork.local.size = static_cast<size_t>((dir_ino_ptr + 4) - data);
    root->size = root->data_fork.local.size;

    dir->ino = DIR_INO;
    dir->mount = &mount;
    dir->mode = 0040755;
    dir->nlink = 2;
    dir->data_fork.format = XFS_DINODE_FMT_LOCAL;
    dir->data_fork.local.data = dir_data;
    dir->data_fork.local.size = 6;
    dir->size = dir->data_fork.local.size;

    int const CACHE_RET = xfs_inode_cache_new(root);
    if (CACHE_RET != 0) {
        delete root;
        delete dir;
        delete[] data;
        delete[] dir_data;
        return false;
    }
    int const DIR_CACHE_RET = xfs_inode_cache_new(dir);
    if (DIR_CACHE_RET != 0) {
        xfs_icache_purge(&mount);
        delete dir;
        delete[] dir_data;
        return false;
    }

    std::array<char, 64> target{};
    bool ok = xfs_readlink_path(NAME, target.data(), target.size(), &mount) == -EINVAL;
    ok = ok && xfs_readlink_path(DIR_NAME, target.data(), target.size(), &mount) == -EINVAL;

    constexpr const char* CACHED_PARENT_PATH = "cached-parent";
    constexpr size_t CACHED_PARENT_PATH_LEN = sizeof("cached-parent") - 1;
    constexpr xfs_ino_t CACHED_PARENT_INO = 5151;
    constexpr const char* CACHED_CHILD_NAME = "regular";
    constexpr uint16_t CACHED_CHILD_NAME_LEN = sizeof("regular") - 1;
    constexpr const char* CACHED_CHILD_PATH = "cached-parent/regular";
    constexpr size_t CACHED_CHILD_PATH_LEN = sizeof("cached-parent/regular") - 1;
    constexpr xfs_ino_t CACHED_CHILD_INO = 6161;
    XfsInode cached_parent{};
    cached_parent.mount = &mount;
    cached_parent.ino = CACHED_PARENT_INO;
    XfsDirEntry cached_entry{};
    cached_entry.ino = CACHED_CHILD_INO;
    cached_entry.ftype = XFS_DIR3_FT_REG_FILE;
    cached_entry.namelen = CACHED_CHILD_NAME_LEN;
    std::memcpy(cached_entry.name.data(), CACHED_CHILD_NAME, CACHED_CHILD_NAME_LEN);
    cached_entry.name.at(CACHED_CHILD_NAME_LEN) = '\0';
    xfs_parent_path_cache_store(&mount, CACHED_PARENT_PATH, CACHED_PARENT_PATH_LEN, CACHED_PARENT_INO);
    xfs_dir_observe_entry(&cached_parent, &cached_entry);
    xfs_path_inode_cache_bump_generation();
    xfs_ino_t cached_ino = NULLFSINO;
    uint8_t cached_ftype = XFS_DIR3_FT_UNKNOWN;
    ok = ok && xfs_readlink_path(CACHED_CHILD_PATH, target.data(), target.size(), &mount, CACHED_CHILD_PATH_LEN) == -EINVAL;
    ok = ok && xfs_path_inode_cache_lookup_ino(&mount, CACHED_CHILD_PATH, CACHED_CHILD_PATH_LEN, &cached_ino, &cached_ftype) &&
         cached_ino == CACHED_CHILD_INO && cached_ftype == XFS_DIR3_FT_REG_FILE;

    XfsInode* descendant_parent = nullptr;
    const char* descendant_name = nullptr;
    uint16_t descendant_name_len = 0;
    bool cache_hit = false;
    int const FIND_RET =
        xfs_find_parent_and_name("dir/grandchild", &mount, &descendant_parent, &descendant_name, &descendant_name_len, &cache_hit, true);
    ok = ok && FIND_RET == 0 && cache_hit && descendant_parent != nullptr && descendant_parent->ino == DIR_INO &&
         descendant_name_len == std::strlen("grandchild") && std::strncmp(descendant_name, "grandchild", descendant_name_len) == 0;
    if (descendant_parent != nullptr) {
        xfs_inode_release(descendant_parent);
    }

    xfs_inode_release(root);
    xfs_inode_release(dir);
    xfs_icache_purge(&mount);
    xfs_parent_path_cache_purge_all_for_mount(&mount);
    xfs_dentry_cache_purge_mount(&mount);
    return ok;
}

auto xfs_selftest_path_exists_uses_dentry_type() -> bool {
    XfsMountContext mount{};
    mount.inode_size = 512;
    mount.feat_incompat = XFS_SB_FEAT_INCOMPAT_FTYPE;
    constexpr xfs_ino_t ROOT_INO = 100;
    mount.root_ino = ROOT_INO;

    xfs_dentry_cache_purge_mount(&mount);

    auto* data = new (std::nothrow) uint8_t[64];
    if (data == nullptr) {
        return false;
    }
    std::memset(data, 0, 64);
    auto* hdr = reinterpret_cast<XfsDir2SfHdr*>(data);
    hdr->count = 2;
    hdr->i8count = 0;
    hdr->parent.at(3) = 7;

    auto* sfep = reinterpret_cast<XfsDir2SfEntry*>(data + xfs_dir2_sf_hdr_size(hdr));
    constexpr const char* NAME = "regular";
    constexpr uint8_t NAME_LEN = 7;
    sfep->namelen = NAME_LEN;
    sfep->offset.at(1) = 4;
    std::memcpy(xfs_dir2_sf_entry_name(sfep), NAME, NAME_LEN);
    uint8_t* ino_ptr = xfs_dir2_sf_entry_name(sfep) + NAME_LEN;
    *ino_ptr++ = XFS_DIR3_FT_REG_FILE;
    constexpr xfs_ino_t CHILD_INO = 4242;
    ino_ptr[0] = static_cast<uint8_t>((CHILD_INO >> 24U) & 0xffU);
    ino_ptr[1] = static_cast<uint8_t>((CHILD_INO >> 16U) & 0xffU);
    ino_ptr[2] = static_cast<uint8_t>((CHILD_INO >> 8U) & 0xffU);
    ino_ptr[3] = static_cast<uint8_t>(CHILD_INO & 0xffU);

    auto* dir_sfep = reinterpret_cast<XfsDir2SfEntry*>(ino_ptr + 4);
    constexpr const char* DIR_NAME = "dir";
    constexpr uint8_t DIR_NAME_LEN = 3;
    dir_sfep->namelen = DIR_NAME_LEN;
    dir_sfep->offset.at(1) = 5;
    std::memcpy(xfs_dir2_sf_entry_name(dir_sfep), DIR_NAME, DIR_NAME_LEN);
    uint8_t* dir_ino_ptr = xfs_dir2_sf_entry_name(dir_sfep) + DIR_NAME_LEN;
    *dir_ino_ptr++ = XFS_DIR3_FT_DIR;
    constexpr xfs_ino_t DIR_INO = 4243;
    dir_ino_ptr[0] = static_cast<uint8_t>((DIR_INO >> 24U) & 0xffU);
    dir_ino_ptr[1] = static_cast<uint8_t>((DIR_INO >> 16U) & 0xffU);
    dir_ino_ptr[2] = static_cast<uint8_t>((DIR_INO >> 8U) & 0xffU);
    dir_ino_ptr[3] = static_cast<uint8_t>(DIR_INO & 0xffU);

    auto* root = new (std::nothrow) XfsInode{};
    if (root == nullptr) {
        delete[] data;
        return false;
    }
    root->ino = ROOT_INO;
    root->mount = &mount;
    root->mode = 0040755;
    root->nlink = 2;
    root->data_fork.format = XFS_DINODE_FMT_LOCAL;
    root->data_fork.local.data = data;
    root->data_fork.local.size = static_cast<size_t>((dir_ino_ptr + 4) - data);
    root->size = root->data_fork.local.size;

    int const CACHE_RET = xfs_inode_cache_new(root);
    if (CACHE_RET != 0) {
        delete root;
        delete[] data;
        return false;
    }

    xfs_ino_t cached_ino = NULLFSINO;
    uint8_t cached_ftype = XFS_DIR3_FT_UNKNOWN;
    bool ok = xfs_path_exists(NAME, true, &mount, NAME_LEN) == -ENOTDIR;
    ok = ok && xfs_path_inode_cache_lookup_ino(&mount, NAME, NAME_LEN, &cached_ino, &cached_ftype) && cached_ino == CHILD_INO &&
         cached_ftype == XFS_DIR3_FT_REG_FILE;
    ok = ok && xfs_path_exists(NAME, false, &mount, NAME_LEN) == 0;
    ok = ok && xfs_path_exists(DIR_NAME, false, &mount) == 0;
    ok = ok && xfs_path_exists(DIR_NAME, true, &mount) == 0;
    ok = ok && xfs_path_exists("missing", false, &mount) == -ENOENT;

    constexpr const char* CACHED_PARENT_PATH = "cached-parent";
    constexpr size_t CACHED_PARENT_PATH_LEN = sizeof("cached-parent") - 1;
    constexpr xfs_ino_t CACHED_PARENT_INO = 5151;
    constexpr const char* CACHED_CHILD_NAME = "regular";
    constexpr uint16_t CACHED_CHILD_NAME_LEN = sizeof("regular") - 1;
    constexpr const char* CACHED_CHILD_PATH = "cached-parent/regular";
    constexpr size_t CACHED_CHILD_PATH_LEN = sizeof("cached-parent/regular") - 1;
    constexpr xfs_ino_t CACHED_CHILD_INO = 6161;
    XfsInode cached_parent{};
    cached_parent.mount = &mount;
    cached_parent.ino = CACHED_PARENT_INO;
    XfsDirEntry cached_entry{};
    cached_entry.ino = CACHED_CHILD_INO;
    cached_entry.ftype = XFS_DIR3_FT_REG_FILE;
    cached_entry.namelen = CACHED_CHILD_NAME_LEN;
    std::memcpy(cached_entry.name.data(), CACHED_CHILD_NAME, CACHED_CHILD_NAME_LEN);
    cached_entry.name.at(CACHED_CHILD_NAME_LEN) = '\0';
    xfs_parent_path_cache_store(&mount, CACHED_PARENT_PATH, CACHED_PARENT_PATH_LEN, CACHED_PARENT_INO);
    xfs_dir_observe_entry(&cached_parent, &cached_entry);
    xfs_path_inode_cache_bump_generation();
    cached_ino = NULLFSINO;
    cached_ftype = XFS_DIR3_FT_UNKNOWN;
    ok = ok && xfs_path_exists(CACHED_CHILD_PATH, true, &mount, CACHED_CHILD_PATH_LEN) == -ENOTDIR;
    ok = ok && xfs_path_inode_cache_lookup_ino(&mount, CACHED_CHILD_PATH, CACHED_CHILD_PATH_LEN, &cached_ino, &cached_ftype) &&
         cached_ino == CACHED_CHILD_INO && cached_ftype == XFS_DIR3_FT_REG_FILE;

    xfs_inode_release(root);
    xfs_icache_purge(&mount);
    xfs_parent_path_cache_purge_all_for_mount(&mount);
    xfs_dentry_cache_purge_mount(&mount);
    return ok;
}

auto xfs_selftest_stat_require_directory_uses_dentry_type() -> bool {
    XfsMountContext mount{};
    mount.inode_size = 512;
    mount.feat_incompat = XFS_SB_FEAT_INCOMPAT_FTYPE;
    constexpr xfs_ino_t ROOT_INO = 100;
    mount.root_ino = ROOT_INO;

    xfs_dentry_cache_purge_mount(&mount);
    xfs_path_inode_cache_bump_generation();

    auto* data = new (std::nothrow) uint8_t[32];
    if (data == nullptr) {
        return false;
    }
    std::memset(data, 0, 32);
    auto* hdr = reinterpret_cast<XfsDir2SfHdr*>(data);
    hdr->count = 1;
    hdr->i8count = 0;
    hdr->parent.at(3) = 7;

    auto* sfep = reinterpret_cast<XfsDir2SfEntry*>(data + xfs_dir2_sf_hdr_size(hdr));
    constexpr const char* NAME = "regular";
    constexpr uint8_t NAME_LEN = 7;
    sfep->namelen = NAME_LEN;
    sfep->offset.at(1) = 4;
    std::memcpy(xfs_dir2_sf_entry_name(sfep), NAME, NAME_LEN);
    uint8_t* ino_ptr = xfs_dir2_sf_entry_name(sfep) + NAME_LEN;
    *ino_ptr++ = XFS_DIR3_FT_REG_FILE;
    constexpr xfs_ino_t CHILD_INO = 4242;
    ino_ptr[0] = static_cast<uint8_t>((CHILD_INO >> 24U) & 0xffU);
    ino_ptr[1] = static_cast<uint8_t>((CHILD_INO >> 16U) & 0xffU);
    ino_ptr[2] = static_cast<uint8_t>((CHILD_INO >> 8U) & 0xffU);
    ino_ptr[3] = static_cast<uint8_t>(CHILD_INO & 0xffU);

    auto* root = new (std::nothrow) XfsInode{};
    if (root == nullptr) {
        delete[] data;
        return false;
    }
    root->ino = ROOT_INO;
    root->mount = &mount;
    root->mode = 0040755;
    root->nlink = 2;
    root->data_fork.format = XFS_DINODE_FMT_LOCAL;
    root->data_fork.local.data = data;
    root->data_fork.local.size = static_cast<size_t>((ino_ptr + 4) - data);
    root->size = root->data_fork.local.size;

    int const CACHE_RET = xfs_inode_cache_new(root);
    if (CACHE_RET != 0) {
        delete root;
        delete[] data;
        return false;
    }

    ker::vfs::Stat statbuf{};
    bool ok = xfs_stat(NAME, &statbuf, &mount, NAME_LEN, true) == -ENOTDIR;
    xfs_path_inode_cache_bump_generation();
    ok = ok && xfs_stat(NAME, &statbuf, &mount, NAME_LEN, true) == -ENOTDIR;

    constexpr const char* CACHED_NAME = "cached-regular";
    constexpr size_t CACHED_NAME_LEN = 14;
    xfs_path_inode_cache_store(&mount, CACHED_NAME, CACHED_NAME_LEN, CHILD_INO, XFS_DIR3_FT_REG_FILE);
    ok = ok && xfs_stat(CACHED_NAME, &statbuf, &mount, CACHED_NAME_LEN, true) == -ENOTDIR;

    xfs_inode_release(root);
    xfs_icache_purge(&mount);
    xfs_parent_path_cache_purge_all_for_mount(&mount);
    xfs_dentry_cache_purge_mount(&mount);
    return ok;
}

auto xfs_selftest_open_require_directory_uses_dentry_type() -> bool {
    XfsMountContext mount{};
    mount.inode_size = 512;
    mount.feat_incompat = XFS_SB_FEAT_INCOMPAT_FTYPE;
    constexpr xfs_ino_t ROOT_INO = 100;
    mount.root_ino = ROOT_INO;

    xfs_dentry_cache_purge_mount(&mount);
    xfs_path_inode_cache_bump_generation();

    auto* data = new (std::nothrow) uint8_t[32];
    if (data == nullptr) {
        return false;
    }
    std::memset(data, 0, 32);
    auto* hdr = reinterpret_cast<XfsDir2SfHdr*>(data);
    hdr->count = 1;
    hdr->i8count = 0;
    hdr->parent.at(3) = 7;

    auto* sfep = reinterpret_cast<XfsDir2SfEntry*>(data + xfs_dir2_sf_hdr_size(hdr));
    constexpr const char* NAME = "regular";
    constexpr uint8_t NAME_LEN = 7;
    sfep->namelen = NAME_LEN;
    sfep->offset.at(1) = 4;
    std::memcpy(xfs_dir2_sf_entry_name(sfep), NAME, NAME_LEN);
    uint8_t* ino_ptr = xfs_dir2_sf_entry_name(sfep) + NAME_LEN;
    *ino_ptr++ = XFS_DIR3_FT_REG_FILE;
    constexpr xfs_ino_t CHILD_INO = 4242;
    ino_ptr[0] = static_cast<uint8_t>((CHILD_INO >> 24U) & 0xffU);
    ino_ptr[1] = static_cast<uint8_t>((CHILD_INO >> 16U) & 0xffU);
    ino_ptr[2] = static_cast<uint8_t>((CHILD_INO >> 8U) & 0xffU);
    ino_ptr[3] = static_cast<uint8_t>(CHILD_INO & 0xffU);

    auto* root = new (std::nothrow) XfsInode{};
    if (root == nullptr) {
        delete[] data;
        return false;
    }
    root->ino = ROOT_INO;
    root->mount = &mount;
    root->mode = 0040755;
    root->nlink = 2;
    root->data_fork.format = XFS_DINODE_FMT_LOCAL;
    root->data_fork.local.data = data;
    root->data_fork.local.size = static_cast<size_t>((ino_ptr + 4) - data);
    root->size = root->data_fork.local.size;

    int const CACHE_RET = xfs_inode_cache_new(root);
    if (CACHE_RET != 0) {
        delete root;
        delete[] data;
        return false;
    }

    int open_result = 0;
    File* opened = xfs_open_path(ker::mod::security::Snapshot::bootstrap_root(), NAME, 0, 0, &mount, &open_result, NAME_LEN, true);
    bool ok = opened == nullptr && open_result == -ENOTDIR;
    if (opened != nullptr) {
        xfs_vfs_close(opened);
        delete opened;
    }

    constexpr const char* CACHED_NAME = "cached-regular";
    constexpr size_t CACHED_NAME_LEN = 14;
    xfs_path_inode_cache_store(&mount, CACHED_NAME, CACHED_NAME_LEN, CHILD_INO, XFS_DIR3_FT_REG_FILE);
    open_result = 0;
    opened = xfs_open_path(ker::mod::security::Snapshot::bootstrap_root(), CACHED_NAME, 0, 0, &mount, &open_result, CACHED_NAME_LEN, true);
    ok = ok && opened == nullptr && open_result == -ENOTDIR;
    if (opened != nullptr) {
        xfs_vfs_close(opened);
        delete opened;
    }

    xfs_inode_release(root);
    xfs_icache_purge(&mount);
    xfs_parent_path_cache_purge_all_for_mount(&mount);
    xfs_dentry_cache_purge_mount(&mount);
    return ok;
}

#endif

auto xfs_write_append(File* f, const void* buf, size_t count, size_t* offset_out) -> ssize_t {
    if (f == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }

    ker::mod::sched::resource::FileGrowth growth;
    bool throttle_after_unlock = false;
    size_t offset = 0;
    ssize_t ret = 0;
    {
        ker::mod::sys::MutexGuard guard(xfd->inode->io_lock);
        offset = static_cast<size_t>(xfd->inode->size);
        if (!growth.bound_write(offset, count)) {
            return -EFBIG;
        }
        ret = count == 0 ? 0 : xfs_vfs_write_locked(f, buf, count, offset, throttle_after_unlock);
        if (ret > 0) {
            growth.committed(xfd->inode->size);
        }
    }
    if (throttle_after_unlock) {
        throttle_dirty_buffer_cache(xfd->mount->device);
    }
    if (ret >= 0 && offset_out != nullptr) {
        *offset_out = offset;
    }
    return ret;
}

auto xfs_fsync(File* f) -> int {
    if (f == nullptr) {
        return -EBADF;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }
    ker::mod::sys::MutexGuard guard(xfd->inode->io_lock);
    int const DATA_RET = sync_inode_data_buffers(xfd->mount, xfd->inode, xfd->inode->size);
    if (DATA_RET != 0) {
        return DATA_RET;
    }
    XfsMetadataGuard metadata_guard(xfd->mount, true, WOS_PERF_CALLSITE());
    int const INODE_RET = xfs_commit_dirty_inode(xfd->mount, xfd->inode);
    if (INODE_RET != 0) {
        return INODE_RET;
    }
    return xfs_log_flush(xfd->mount);
}

auto xfs_sync_mount(XfsMountContext* ctx) -> int {
    if (ctx == nullptr || ctx->device == nullptr) {
        return -EINVAL;
    }

    // This context cannot own dirty state. Do not flush shared device buffers
    // or journal state on behalf of a read-only mount.
    if (ctx->read_only) {
        return 0;
    }

    for (;;) {
        uint64_t const MUTATION_SNAPSHOT = ctx->mutation_sequence.load(std::memory_order_acquire);
        int const INODE_RET = xfs_icache_sync_dirty(ctx);
        if (INODE_RET != 0) {
            return INODE_RET;
        }

        XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());
        if (ctx->mutation_sequence.load(std::memory_order_acquire) != MUTATION_SNAPSHOT) {
            // Dirty inode commits performed by the scan (or a write that won
            // the race before this barrier) joined the journal only after the
            // data-durability snapshot.  Repeat before checkpointing them.
            continue;
        }
        int const LOG_RET = xfs_log_flush(ctx);
        if (LOG_RET != 0) {
            return LOG_RET;
        }
        int const BLOCK_RET = sync_blockdev(ctx->device);
        if (BLOCK_RET != 0) {
            return BLOCK_RET;
        }
        int const SUPERBLOCK_RET = xfs_sync_superblock_counters(ctx);
        if (SUPERBLOCK_RET != 0) {
            return SUPERBLOCK_RET;
        }
        int const COUNTER_FLUSH_RET = flush_blockdev(ctx->device);
        if (COUNTER_FLUSH_RET != 0) {
            return COUNTER_FLUSH_RET;
        }
        int const CLEAN_RET = xfs_log_mark_clean(ctx);
        if (CLEAN_RET != 0) {
            return CLEAN_RET;
        }

        // metadata_lock excludes mutation while the checkpoint is published;
        // a mutation completed during the preceding inode scan advances the
        // sequence and forces another complete pass.
        if (ctx->mutation_sequence.load(std::memory_order_acquire) == MUTATION_SNAPSHOT) {
            return 0;
        }
    }
}

auto xfs_collect_swap_extents(File* f, ker::mod::mm::swap::SwapExtent** extents_out, size_t* extent_count_out) -> int {
    if (f == nullptr || extents_out == nullptr || extent_count_out == nullptr) {
        return -EINVAL;
    }
    *extents_out = nullptr;
    *extent_count_out = 0;

    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->mount == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }
    XfsMountContext* ctx = xfd->mount;
    XfsInode* ip = xfd->inode;
    if (ctx->read_only || ctx->device == nullptr || !xfs_inode_isreg(ip) || ctx->block_size != ker::mod::mm::paging::PAGE_SIZE ||
        dev::block_device_is_read_only(ctx->device) || ctx->device->block_size == 0 ||
        ker::mod::mm::paging::PAGE_SIZE % ctx->device->block_size != 0) {
        return -EINVAL;
    }

    ker::mod::sys::MutexGuard io_guard(ip->io_lock);
    if (!ip->exec_content.can_mutate()) return -ETXTBSY;
    XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());
    uint64_t const PAGE_COUNT = ip->size / ker::mod::mm::paging::PAGE_SIZE;
    if (PAGE_COUNT == 0 || PAGE_COUNT > SIZE_MAX) {
        return -EINVAL;
    }

    auto* extents = new ker::mod::mm::swap::SwapExtent[static_cast<size_t>(PAGE_COUNT)];
    if (extents == nullptr) {
        return -ENOMEM;
    }

    size_t extent_count = 0;
    xfs_fileoff_t file_block = 0;
    while (file_block < PAGE_COUNT) {
        XfsBmapResult bmap{};
        int const RET = xfs_bmap_lookup(ip, file_block, &bmap);
        if (RET < 0) {
            delete[] extents;
            return RET;
        }
        if (bmap.blockcount == 0 || bmap.is_hole || bmap.unwritten || bmap.startblock == NULLFSBLOCK) {
            delete[] extents;
            return -EINVAL;
        }

        auto const REMAINING = static_cast<xfs_filblks_t>(PAGE_COUNT - file_block);
        xfs_filblks_t const SPAN = std::min(bmap.blockcount, REMAINING);
        uint64_t const DEV_BLOCK = xfs_fsblock_to_dev_block(ctx, bmap.startblock);
        if (extent_count > 0) {
            auto& prev = extents[extent_count - 1];
            uint64_t const PREV_BLOCKS = prev.page_count * (ker::mod::mm::paging::PAGE_SIZE / prev.device->block_size);
            if (prev.device == ctx->device && prev.start_block + PREV_BLOCKS == DEV_BLOCK) {
                prev.page_count += SPAN;
                file_block += SPAN;
                continue;
            }
        }
        extents[extent_count++] = ker::mod::mm::swap::SwapExtent{.device = ctx->device, .start_block = DEV_BLOCK, .page_count = SPAN};
        file_block += SPAN;
    }

    ++ip->exec_content.direct_writers;
    xfd->swap_extent_pin = true;
    *extents_out = extents;
    *extent_count_out = extent_count;
    return 0;
}

// ============================================================================
// Open path
// ============================================================================

namespace {

auto xfs_cached_parent_dentry_lookup(const char* fs_path, XfsMountContext* ctx, XfsDirEntry* entry, int* lookup_result_out,
                                     size_t known_fs_path_len, size_t* fs_path_len_out) -> bool;

auto xfs_find_parent_and_name(const char* fs_path, XfsMountContext* ctx, XfsInode** parent_out, const char** name_out,
                              uint16_t* namelen_out, bool* cache_hit_out, bool allow_parent_cache, size_t known_fs_path_len,
                              size_t* fs_path_len_out) -> int {
    if (cache_hit_out != nullptr) {
        *cache_hit_out = false;
    }

    const char* last_slash = nullptr;
    size_t path_len = known_fs_path_len;
    if (path_len != UNKNOWN_XFS_PATH_LEN) {
        for (size_t i = path_len; i > 0; --i) {
            if (fs_path[i - 1] == '/') {
                last_slash = fs_path + i - 1;
                break;
            }
        }
    } else {
        path_len = 0;
        for (const char* p = fs_path; *p != '\0'; p++) {
            if (*p == '/') {
                last_slash = p;
            }
            path_len++;
        }
    }
    if (fs_path_len_out != nullptr) {
        *fs_path_len_out = path_len;
    }

    XfsInode* parent_ip = nullptr;
    const char* name = nullptr;
    size_t name_len = 0;

    if (last_slash == nullptr || last_slash == fs_path) {
        parent_ip = xfs_root_inode_read(ctx);
        name = (last_slash == fs_path) ? fs_path + 1 : fs_path;
        name_len = (last_slash == fs_path) ? path_len - 1 : path_len;
    } else {
        auto parent_len = static_cast<size_t>(last_slash - fs_path);
        constexpr size_t PARENT_PATH_CAPACITY = 512;
        if (parent_len >= PARENT_PATH_CAPACITY) {
            return -ENAMETOOLONG;
        }
        if (allow_parent_cache) {
            parent_ip = xfs_cached_directory_inode_for_path(ctx, fs_path, parent_len, cache_hit_out);
        }
        if (allow_parent_cache && parent_ip == nullptr) {
            XfsDirEntry cached_parent_entry{};
            int cached_parent_lookup_result = 0;
            if (xfs_cached_parent_dentry_lookup(fs_path, ctx, &cached_parent_entry, &cached_parent_lookup_result, parent_len, nullptr)) {
                if (cached_parent_lookup_result != 0) {
                    return cached_parent_lookup_result;
                }
                if (cached_parent_entry.ftype != XFS_DIR3_FT_UNKNOWN && cached_parent_entry.ftype != XFS_DIR3_FT_DIR) {
                    xfs_path_inode_cache_store(ctx, fs_path, parent_len, cached_parent_entry.ino, cached_parent_entry.ftype);
                    return -ENOTDIR;
                }

                parent_ip = xfs_inode_read_known_allocated(ctx, cached_parent_entry.ino);
                if (parent_ip == nullptr) {
                    return -ENOENT;
                }

                uint8_t const PARENT_FTYPE =
                    cached_parent_entry.ftype != XFS_DIR3_FT_UNKNOWN ? cached_parent_entry.ftype : xfs_ftype_from_inode(parent_ip);
                xfs_path_inode_cache_store(ctx, fs_path, parent_len, cached_parent_entry.ino, PARENT_FTYPE);
                if (PARENT_FTYPE != XFS_DIR3_FT_DIR) {
                    xfs_inode_release(parent_ip);
                    return -ENOTDIR;
                }
                xfs_parent_path_cache_store(ctx, fs_path, parent_len, parent_ip->ino);
                if (cache_hit_out != nullptr) {
                    *cache_hit_out = true;
                }
            }
        }
        if (parent_ip == nullptr) {
            char parent_path[PARENT_PATH_CAPACITY] = {};  // NOLINT
            std::memcpy(static_cast<char*>(parent_path), fs_path, parent_len);
            parent_path[parent_len] = '\0';
            auto const* parent_path_chars = static_cast<const char*>(parent_path);
            parent_ip = walk_path(ctx, parent_path_chars, allow_parent_cache);
            if (parent_ip != nullptr && xfs_inode_isdir(parent_ip)) {
                xfs_parent_path_cache_store(ctx, parent_path_chars, parent_len, parent_ip->ino);
            }
        }
        name = last_slash + 1;
        name_len = path_len - parent_len - 1;
    }

    if (parent_ip == nullptr) {
        return -ENOENT;
    }
    if (!xfs_inode_isdir(parent_ip)) {
        xfs_inode_release(parent_ip);
        return -ENOTDIR;
    }

    if (name_len == 0) {
        xfs_inode_release(parent_ip);
        return -EINVAL;
    }
    auto namelen = static_cast<uint16_t>(name_len);

    *parent_out = parent_ip;
    *name_out = name;
    *namelen_out = namelen;
    return 0;
}

auto xfs_lookup_namespace_entry_for_mutation(const char* fs_path, XfsMountContext* ctx, XfsInode** parent_out, const char** name_out,
                                             uint16_t* namelen_out, XfsDirEntry* entry_out, size_t known_fs_path_len = UNKNOWN_XFS_PATH_LEN,
                                             size_t* fs_path_len_out = nullptr) -> int {
    // Namespace transactions may roll back after publishing a conservative
    // negative dentry. Retry only that cache-derived miss without caches so a
    // later unlink/rmdir sees the restored on-disk entry.
    bool authoritative_lookup = false;
    int rc = xfs_find_parent_and_name(fs_path, ctx, parent_out, name_out, namelen_out, nullptr, true, known_fs_path_len, fs_path_len_out);
    if (rc == -ENOENT) {
        authoritative_lookup = true;
        rc = xfs_find_parent_and_name(fs_path, ctx, parent_out, name_out, namelen_out, nullptr, false, known_fs_path_len, fs_path_len_out);
    }
    if (rc != 0) {
        return rc;
    }

    rc = authoritative_lookup ? xfs_dir_lookup_authoritative(*parent_out, *name_out, *namelen_out, entry_out)
                              : xfs_dir_lookup(*parent_out, *name_out, *namelen_out, entry_out);
    if (rc == -ENOENT && !authoritative_lookup) {
        xfs_inode_release(*parent_out);
        *parent_out = nullptr;
        rc = xfs_find_parent_and_name(fs_path, ctx, parent_out, name_out, namelen_out, nullptr, false, known_fs_path_len, fs_path_len_out);
        if (rc == 0) {
            rc = xfs_dir_lookup_authoritative(*parent_out, *name_out, *namelen_out, entry_out);
        }
    }
    if (rc != 0 && *parent_out != nullptr) {
        xfs_inode_release(*parent_out);
        *parent_out = nullptr;
    }
    return rc;
}

#ifdef WOS_SELFTEST
auto xfs_selftest_namespace_mutation_lookup_repairs_stale_negative_impl() -> bool {
    XfsMountContext mount{};
    mount.inode_size = 512;
    mount.feat_incompat = XFS_SB_FEAT_INCOMPAT_FTYPE;
    constexpr xfs_ino_t ROOT_INO = 100;
    constexpr xfs_ino_t CHILD_INO = 4242;
    constexpr const char* NAME = "victim";
    constexpr uint16_t NAME_LEN = sizeof("victim") - 1;
    mount.root_ino = ROOT_INO;

    xfs_dentry_cache_purge_mount(&mount);
    auto* data = new (std::nothrow) uint8_t[32];
    auto* root = new (std::nothrow) XfsInode{};
    if (data == nullptr || root == nullptr) {
        delete[] data;
        delete root;
        return false;
    }
    std::memset(data, 0, 32);
    auto* hdr = reinterpret_cast<XfsDir2SfHdr*>(data);
    hdr->count = 0;
    hdr->i8count = 0;
    hdr->parent.at(3) = 7;

    root->ino = ROOT_INO;
    root->mount = &mount;
    root->mode = 0040755;
    root->nlink = 2;
    root->data_fork.format = XFS_DINODE_FMT_LOCAL;
    root->data_fork.local.data = data;
    root->data_fork.local.size = 6;
    root->size = root->data_fork.local.size;

    if (xfs_inode_cache_new(root) != 0) {
        delete root;
        delete[] data;
        return false;
    }

    XfsDirEntry stale_lookup{};
    bool ok = xfs_dir_lookup(root, NAME, NAME_LEN, &stale_lookup) == -ENOENT;

    hdr->count = 1;
    auto* sfep = reinterpret_cast<XfsDir2SfEntry*>(data + xfs_dir2_sf_hdr_size(hdr));
    sfep->namelen = NAME_LEN;
    sfep->offset.at(1) = 4;
    std::memcpy(xfs_dir2_sf_entry_name(sfep), NAME, NAME_LEN);
    uint8_t* ino_ptr = xfs_dir2_sf_entry_name(sfep) + NAME_LEN;
    *ino_ptr++ = XFS_DIR3_FT_DIR;
    ino_ptr[0] = static_cast<uint8_t>((CHILD_INO >> 24U) & 0xffU);
    ino_ptr[1] = static_cast<uint8_t>((CHILD_INO >> 16U) & 0xffU);
    ino_ptr[2] = static_cast<uint8_t>((CHILD_INO >> 8U) & 0xffU);
    ino_ptr[3] = static_cast<uint8_t>(CHILD_INO & 0xffU);
    root->data_fork.local.size = static_cast<size_t>((ino_ptr + 4) - data);
    root->size = root->data_fork.local.size;

    XfsInode* parent = nullptr;
    const char* name = nullptr;
    uint16_t namelen = 0;
    XfsDirEntry repaired{};
    int const LOOKUP_RET = xfs_lookup_namespace_entry_for_mutation(NAME, &mount, &parent, &name, &namelen, &repaired, NAME_LEN);
    ok = ok && LOOKUP_RET == 0 && parent != nullptr && parent->ino == ROOT_INO && name == NAME && namelen == NAME_LEN &&
         repaired.ino == CHILD_INO && repaired.ftype == XFS_DIR3_FT_DIR;

    if (parent != nullptr) {
        xfs_inode_release(parent);
    }
    xfs_inode_release(root);
    xfs_icache_purge(&mount);
    xfs_dentry_cache_purge_mount(&mount);
    return ok;
}
#endif

auto xfs_cached_parent_dentry_lookup(const char* fs_path, XfsMountContext* ctx, XfsDirEntry* entry, int* lookup_result_out,
                                     size_t known_fs_path_len, size_t* fs_path_len_out) -> bool {
    if (fs_path == nullptr || ctx == nullptr || entry == nullptr || lookup_result_out == nullptr) {
        return false;
    }

    const char* last_slash = nullptr;
    size_t path_len = known_fs_path_len;
    if (path_len != UNKNOWN_XFS_PATH_LEN) {
        for (size_t i = path_len; i > 0; --i) {
            if (fs_path[i - 1] == '/') {
                last_slash = fs_path + i - 1;
                break;
            }
        }
    } else {
        path_len = 0;
        for (const char* p = fs_path; *p != '\0'; ++p) {
            if (*p == '/') {
                last_slash = p;
            }
            ++path_len;
        }
    }
    if (fs_path_len_out != nullptr) {
        *fs_path_len_out = path_len;
    }
    if (path_len == 0) {
        return false;
    }

    xfs_ino_t parent_ino = NULLFSINO;
    const char* name = nullptr;
    size_t name_len = 0;
    if (last_slash == nullptr || last_slash == fs_path) {
        parent_ino = ctx->root_ino;
        name = (last_slash == fs_path) ? fs_path + 1 : fs_path;
        name_len = (last_slash == fs_path) ? path_len - 1 : path_len;
    } else {
        auto const PARENT_LEN = static_cast<size_t>(last_slash - fs_path);
        if (PARENT_LEN == 0 || PARENT_LEN >= XFS_PARENT_PATH_CACHE_PATH_MAX) {
            return false;
        }
        if (!xfs_parent_path_cache_lookup_ino(ctx, fs_path, PARENT_LEN, &parent_ino)) {
            uint8_t parent_ftype = XFS_DIR3_FT_UNKNOWN;
            if (!xfs_path_inode_cache_lookup_ino(ctx, fs_path, PARENT_LEN, &parent_ino, &parent_ftype)) {
                return false;
            }
            if (parent_ftype != XFS_DIR3_FT_UNKNOWN && parent_ftype != XFS_DIR3_FT_DIR) {
                return false;
            }
        }
        name = last_slash + 1;
        name_len = path_len - PARENT_LEN - 1;
    }

    if (parent_ino == NULLFSINO || name == nullptr || name_len == 0 || name_len > UINT16_MAX) {
        return false;
    }

    return xfs_dentry_cache_lookup_parent(ctx, parent_ino, name, static_cast<uint16_t>(name_len), entry, lookup_result_out);
}

auto xfs_lookup_with_cached_parent(const char* fs_path, XfsMountContext* ctx, XfsInode** ip_out, size_t known_fs_path_len,
                                   bool require_directory) -> int {
    if (ip_out == nullptr) {
        return -EINVAL;
    }
    *ip_out = nullptr;
    if (fs_path == nullptr || fs_path[0] == '\0' || (fs_path[0] == '/' && fs_path[1] == '\0')) {
        *ip_out = xfs_root_inode_read(ctx);
        return (*ip_out != nullptr) ? 0 : -ENOENT;
    }

    size_t const PATH_LEN = xfs_known_path_len(fs_path, known_fs_path_len);
    xfs_ino_t cached_ino = NULLFSINO;
    uint8_t cached_ftype = XFS_DIR3_FT_UNKNOWN;
    if (xfs_path_inode_cache_lookup_ino(ctx, fs_path, PATH_LEN, &cached_ino, &cached_ftype)) {
        if (require_directory && cached_ftype != XFS_DIR3_FT_UNKNOWN && cached_ftype != XFS_DIR3_FT_DIR) {
            return -ENOTDIR;
        }
        *ip_out = xfs_inode_read_known_allocated(ctx, cached_ino);
        if (*ip_out != nullptr) {
            if ((*ip_out)->nlink != 0 && (!require_directory || xfs_inode_isdir(*ip_out))) {
                xfs_parent_path_cache_store_directory(ctx, fs_path, PATH_LEN, *ip_out);
                return 0;
            }
            int const RET = (*ip_out)->nlink == 0 ? -ENOENT : -ENOTDIR;
            if ((*ip_out)->nlink != 0) {
                xfs_path_inode_cache_store(ctx, fs_path, PATH_LEN, cached_ino, xfs_ftype_from_inode(*ip_out));
            }
            xfs_inode_release(*ip_out);
            *ip_out = nullptr;
            return RET;
        }
    }

    XfsDirEntry cached_entry{};
    int cached_lookup_result = 0;
    size_t cached_fs_path_len = UNKNOWN_XFS_PATH_LEN;
    if (xfs_cached_parent_dentry_lookup(fs_path, ctx, &cached_entry, &cached_lookup_result, known_fs_path_len, &cached_fs_path_len)) {
        if (cached_lookup_result != 0) {
            return cached_lookup_result;
        }
        if (require_directory && cached_entry.ftype != XFS_DIR3_FT_UNKNOWN && cached_entry.ftype != XFS_DIR3_FT_DIR) {
            xfs_path_inode_cache_store(ctx, fs_path, cached_fs_path_len, cached_entry.ino, cached_entry.ftype);
            return -ENOTDIR;
        }

        *ip_out = xfs_inode_read_known_allocated(ctx, cached_entry.ino);
        if (*ip_out == nullptr) {
            return -ENOENT;
        }
        uint8_t const FTYPE = cached_entry.ftype != XFS_DIR3_FT_UNKNOWN ? cached_entry.ftype : xfs_ftype_from_inode(*ip_out);
        xfs_path_inode_cache_store(ctx, fs_path, cached_fs_path_len, cached_entry.ino, FTYPE);
        if (require_directory && FTYPE != XFS_DIR3_FT_DIR) {
            xfs_inode_release(*ip_out);
            *ip_out = nullptr;
            return -ENOTDIR;
        }
        xfs_parent_path_cache_store_directory(ctx, fs_path, cached_fs_path_len, *ip_out);
        return 0;
    }

    XfsInode* parent_ip = nullptr;
    const char* filename = nullptr;
    uint16_t filename_len = 0;
    size_t fs_path_len = UNKNOWN_XFS_PATH_LEN;
    int const PARENT_RET =
        xfs_find_parent_and_name(fs_path, ctx, &parent_ip, &filename, &filename_len, nullptr, true, known_fs_path_len, &fs_path_len);
    if (PARENT_RET != 0) {
        return PARENT_RET == -ENAMETOOLONG ? -EAGAIN : PARENT_RET;
    }

    XfsDirEntry entry{};
    int const LOOKUP_RET = xfs_dir_lookup(parent_ip, filename, filename_len, &entry);
    xfs_inode_release(parent_ip);
    if (LOOKUP_RET != 0) {
        return LOOKUP_RET;
    }

    if (require_directory && entry.ftype != XFS_DIR3_FT_UNKNOWN && entry.ftype != XFS_DIR3_FT_DIR) {
        xfs_path_inode_cache_store(ctx, fs_path, fs_path_len, entry.ino, entry.ftype);
        return -ENOTDIR;
    }

    *ip_out = xfs_inode_read_known_allocated(ctx, entry.ino);
    if (*ip_out == nullptr) {
        return -ENOENT;
    }
    uint8_t const FTYPE = entry.ftype != XFS_DIR3_FT_UNKNOWN ? entry.ftype : xfs_ftype_from_inode(*ip_out);
    xfs_path_inode_cache_store(ctx, fs_path, fs_path_len, entry.ino, FTYPE);
    if (require_directory && FTYPE != XFS_DIR3_FT_DIR) {
        xfs_inode_release(*ip_out);
        *ip_out = nullptr;
        return -ENOTDIR;
    }
    xfs_parent_path_cache_store_directory(ctx, fs_path, fs_path_len, *ip_out);
    return 0;
}

auto xfs_readlink_cached_inode(XfsMountContext* ctx, xfs_ino_t ino, const char* fs_path, size_t known_fs_path_len, char* buf,
                               size_t bufsize, ssize_t* result_out) -> bool {
    if (ctx == nullptr || ino == NULLFSINO || fs_path == nullptr || buf == nullptr || result_out == nullptr) {
        return false;
    }

    XfsInode* cached_ip = xfs_inode_read_cached(ctx, ino);
    if (cached_ip == nullptr) {
        return false;
    }

    ssize_t result = 0;
    {
        ker::mod::sys::MutexGuard guard(cached_ip->io_lock);
        if (cached_ip->nlink == 0) {
            result = -ENOENT;
        } else if (!xfs_inode_islnk(cached_ip)) {
            uint8_t const FTYPE = xfs_ftype_from_inode(cached_ip);
            xfs_path_inode_cache_store(ctx, fs_path, known_fs_path_len, ino, FTYPE);
            xfs_parent_path_cache_store_known_directory(ctx, fs_path, known_fs_path_len, ino, FTYPE);
            result = -EINVAL;
        } else {
            int const RET = xfs_readlink(cached_ip, buf, bufsize);
            xfs_path_inode_cache_store(ctx, fs_path, known_fs_path_len, ino, XFS_DIR3_FT_SYMLINK);
            result = static_cast<ssize_t>(RET);
        }
    }

    xfs_inode_release(cached_ip);
    *result_out = result;
    return true;
}

}  // namespace

XfsLookupHandle::XfsLookupHandle(XfsLookupHandle&& other) noexcept
    : mount_(other.mount_),
      parent_(other.parent_),
      target_(other.target_),
      leaf_(other.leaf_),
      path_(other.path_),
      leaf_length_(other.leaf_length_),
      path_length_(other.path_length_),
      expected_parent_generation_(other.expected_parent_generation_),
      expected_lookup_sequence_(other.expected_lookup_sequence_),
      target_existed_(other.target_existed_),
      root_target_(other.root_target_) {
    other.mount_ = nullptr;
    other.parent_ = nullptr;
    other.target_ = nullptr;
    other.leaf_length_ = 0;
    other.path_length_ = 0;
    other.expected_parent_generation_ = 0;
    other.expected_lookup_sequence_ = 0;
    other.target_existed_ = false;
    other.root_target_ = false;
}

auto XfsLookupHandle::operator=(XfsLookupHandle&& other) noexcept -> XfsLookupHandle& {
    if (this != &other) {
        reset();
        mount_ = other.mount_;
        parent_ = other.parent_;
        target_ = other.target_;
        leaf_ = other.leaf_;
        path_ = other.path_;
        leaf_length_ = other.leaf_length_;
        path_length_ = other.path_length_;
        expected_parent_generation_ = other.expected_parent_generation_;
        expected_lookup_sequence_ = other.expected_lookup_sequence_;
        target_existed_ = other.target_existed_;
        root_target_ = other.root_target_;
        other.mount_ = nullptr;
        other.parent_ = nullptr;
        other.target_ = nullptr;
        other.leaf_length_ = 0;
        other.path_length_ = 0;
        other.expected_parent_generation_ = 0;
        other.expected_lookup_sequence_ = 0;
        other.target_existed_ = false;
        other.root_target_ = false;
    }
    return *this;
}

XfsLookupHandle::~XfsLookupHandle() { reset(); }

void XfsLookupHandle::reset() {
    XfsInode* const TARGET = target_;
    XfsInode* const PARENT = parent_;
    mount_ = nullptr;
    parent_ = nullptr;
    target_ = nullptr;
    leaf_length_ = 0;
    path_length_ = 0;
    expected_parent_generation_ = 0;
    expected_lookup_sequence_ = 0;
    target_existed_ = false;
    root_target_ = false;
    xfs_inode_release(TARGET);
    xfs_inode_release(PARENT);
}

auto xfs_acquire_lookup(const ker::mod::security::Snapshot& credentials, const char* fs_path, XfsMountContext* ctx, XfsLookupHandle* out,
                        size_t known_fs_path_len) -> int {
    if (fs_path == nullptr || ctx == nullptr || out == nullptr) {
        return -EINVAL;
    }

    *out = XfsLookupHandle{};
    XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());

    size_t const INPUT_PATH_LENGTH = known_fs_path_len != UNKNOWN_XFS_PATH_LEN ? known_fs_path_len : std::strlen(fs_path);
    if (INPUT_PATH_LENGTH == 0 || (INPUT_PATH_LENGTH == 1 && fs_path[0] == '/')) {
        XfsInode* parent = xfs_root_inode_read(ctx);
        XfsInode* target = xfs_root_inode_read(ctx);
        if (parent == nullptr || target == nullptr) {
            xfs_inode_release_metadata_locked(target);
            xfs_inode_release_metadata_locked(parent);
            return -ENOENT;
        }
        XfsLookupHandle result{};
        result.mount_ = ctx;
        result.parent_ = parent;
        result.target_ = target;
        result.path_.front() = '/';
        result.path_.at(1) = '\0';
        result.path_length_ = 1;
        result.expected_parent_generation_ = parent->dir_generation;
        result.expected_lookup_sequence_ = ctx->lookup_sequence.load(std::memory_order_acquire);
        result.target_existed_ = true;
        result.root_target_ = true;
        *out = std::move(result);
        return 0;
    }

    if (INPUT_PATH_LENGTH >= XFS_LOOKUP_PATH_CAPACITY) return -ENAMETOOLONG;
    XfsInode* parent = xfs_root_inode_read(ctx);
    if (parent == nullptr) return -ENOENT;
    auto fail_parent = [&](int error) {
        xfs_inode_release_metadata_locked(parent);
        return error;
    };
    const char* name = fs_path;
    for (size_t index = 0; index < INPUT_PATH_LENGTH; ++index)
        if (fs_path[index] == '/') name = fs_path + index + 1;
    size_t const LEAF_LENGTH = INPUT_PATH_LENGTH - static_cast<size_t>(name - fs_path);
    if (LEAF_LENGTH == 0 || LEAF_LENGTH >= XFS_LOOKUP_LEAF_CAPACITY) return fail_parent(LEAF_LENGTH == 0 ? -EINVAL : -ENAMETOOLONG);
    uint16_t const name_length = static_cast<uint16_t>(LEAF_LENGTH);
    size_t const path_length = INPUT_PATH_LENGTH;
    const char* cursor = fs_path;
    while (cursor < name) {
        while (cursor < name && *cursor == '/') ++cursor;
        if (cursor == name) break;
        const char* component = cursor;
        while (cursor < name && *cursor != '/') ++cursor;
        size_t const LENGTH = static_cast<size_t>(cursor - component);
        if (LENGTH == 0 || LENGTH >= XFS_LOOKUP_LEAF_CAPACITY) return fail_parent(-ENAMETOOLONG);
        if (!xfs_inode_isdir(parent)) return fail_parent(-ENOTDIR);
        int const SEARCH = ker::mod::security::authorize_file_access(credentials, {parent->uid, parent->gid, parent->mode}, 1);
        if (SEARCH != 0) return fail_parent(SEARCH);
        XfsDirEntry entry{};
        int const LOOKUP = xfs_dir_lookup_authoritative(parent, component, static_cast<uint16_t>(LENGTH), &entry);
        if (LOOKUP != 0) return fail_parent(LOOKUP);
        auto* child = xfs_inode_read_known_allocated(ctx, entry.ino);
        if (child == nullptr) return fail_parent(-ENOENT);
        xfs_inode_release_metadata_locked(parent);
        parent = child;
    }
    if (!xfs_inode_isdir(parent)) return fail_parent(-ENOTDIR);
    int const SEARCH = ker::mod::security::authorize_file_access(credentials, {parent->uid, parent->gid, parent->mode}, 1);
    if (SEARCH != 0) return fail_parent(SEARCH);

    size_t actual_name_length = 0;
    while (name[actual_name_length] != '\0') {
        ++actual_name_length;
    }
    if (actual_name_length == 0 || actual_name_length >= XFS_LOOKUP_LEAF_CAPACITY || actual_name_length != name_length ||
        path_length >= XFS_LOOKUP_PATH_CAPACITY) {
        xfs_inode_release_metadata_locked(parent);
        return actual_name_length >= XFS_LOOKUP_LEAF_CAPACITY || path_length >= XFS_LOOKUP_PATH_CAPACITY ? -ENAMETOOLONG : -EINVAL;
    }

    XfsDirEntry entry{};
    int const LOOKUP_RET = xfs_dir_lookup_authoritative(parent, name, name_length, &entry);
    XfsInode* target = nullptr;
    if (LOOKUP_RET == 0) {
        target = xfs_inode_read_known_allocated(ctx, entry.ino);
        if (target == nullptr) {
            xfs_inode_release_metadata_locked(parent);
            return -ENOENT;
        }
    } else if (LOOKUP_RET != -ENOENT) {
        xfs_inode_release_metadata_locked(parent);
        return LOOKUP_RET;
    }

    XfsLookupHandle result{};
    result.mount_ = ctx;
    result.parent_ = parent;
    result.target_ = target;
    std::memcpy(result.leaf_.data(), name, actual_name_length);
    result.leaf_.at(actual_name_length) = '\0';
    std::memcpy(result.path_.data(), fs_path, path_length);
    result.path_.at(path_length) = '\0';
    result.leaf_length_ = name_length;
    result.path_length_ = path_length;
    result.expected_parent_generation_ = parent->dir_generation;
    result.expected_lookup_sequence_ = ctx->lookup_sequence.load(std::memory_order_acquire);
    result.target_existed_ = target != nullptr;
    *out = std::move(result);
    return 0;
}

auto xfs_directory_path(File* directory, char* out, size_t capacity) -> int {
    if (directory == nullptr || directory->fs_type != FSType::XFS || directory->private_data == nullptr) return -EBADF;
    if (!directory->is_directory) return -ENOTDIR;
    if (out == nullptr || capacity == 0) return -EINVAL;
    auto* xfd = static_cast<XfsFileData*>(directory->private_data);
    if (xfd->mount == nullptr || xfd->inode == nullptr) return -EBADF;
    return xfs_directory_path_at_inode(xfd->mount, xfd->inode, out, capacity);
}

namespace {
auto directory_path_at_inode(XfsMountContext* ctx, XfsInode* anchor, XfsInode* boundary, char* out, size_t capacity,
                             const ker::mod::security::Snapshot* credentials) -> int {
    if (ctx == nullptr || anchor == nullptr || out == nullptr || capacity == 0) return -EINVAL;
    XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());
    if (boundary != nullptr) {
        auto* retained = xfs_inode_read_cached(ctx, boundary->ino);
        int const VALID = retained == nullptr || retained != boundary ? -EAGAIN
                          : !xfs_inode_isdir(retained)                ? -ENOTDIR
                          : retained->nlink == 0                      ? -ENOENT
                                                                      : 0;
        xfs_inode_release_metadata_locked(retained);
        if (VALID != 0) return VALID;
    }
    auto const ROOT_INO = boundary != nullptr ? boundary->ino : ctx->root_ino;
    std::array<char, XFS_LOOKUP_PATH_CAPACITY> path{};
    size_t cursor = path.size() - 1;
    XfsInode* current = xfs_inode_read_cached(ctx, anchor->ino);
    if (current == nullptr || current != anchor) {
        xfs_inode_release_metadata_locked(current);
        return -EAGAIN;
    }
    auto fail = [&](int error) {
        xfs_inode_release_metadata_locked(current);
        return error;
    };
    if (credentials != nullptr) {
        if (!xfs_inode_isdir(current)) return fail(-ENOTDIR);
        if (current->nlink == 0) return fail(-ENOENT);
        int const SEARCH = ker::mod::security::authorize_file_access(*credentials, {current->uid, current->gid, current->mode}, 1);
        if (SEARCH != 0) return fail(SEARCH);
    }
    while (current->ino != ROOT_INO) {
        if (boundary != nullptr && current->ino == ctx->root_ino) return fail(-EPERM);
        if (!xfs_inode_isdir(current)) return fail(-ENOTDIR);
        if (current->nlink == 0) return fail(-ENOENT);
        XfsDirEntry parent_entry{};
        int const LOOKUP = xfs_dir_lookup_authoritative(current, "..", 2, &parent_entry);
        if (LOOKUP != 0) return fail(LOOKUP);
        if (parent_entry.ino == current->ino) return fail(-EIO);
        XfsInode* parent = xfs_inode_read_known_allocated(ctx, parent_entry.ino);
        if (parent == nullptr) return fail(-ENOENT);
        if (!xfs_inode_isdir(parent) || parent->nlink == 0) {
            xfs_inode_release_metadata_locked(parent);
            return fail(-ENOENT);
        }
        struct NameScan {
            XfsInode* parent;
            xfs_ino_t child;
            XfsDirEntry entry{};
            int status = -ENOENT;
        } scan{parent, current->ino};
        auto find_name = [](const XfsDirEntry* entry, void* opaque) -> int {
            auto& scan = *static_cast<NameScan*>(opaque);
            if (entry->ino != scan.child || (entry->namelen == 1 && entry->name[0] == '.') ||
                (entry->namelen == 2 && entry->name[0] == '.' && entry->name[1] == '.'))
                return 0;
            int const INDEXED = xfs_dir_entry_is_indexed(scan.parent, entry);
            if (INDEXED == 0) return 0;
            if (INDEXED < 0) {
                scan.status = INDEXED;
                return 1;
            }
            if (entry->namelen == 0 || entry->namelen >= entry->name.size()) {
                scan.status = -EIO;
                return 1;
            }
            for (size_t i = 0; i < entry->namelen; ++i) {
                if (entry->name[i] == '/' || entry->name[i] == '\0') {
                    scan.status = -EIO;
                    return 1;
                }
            }
            scan.entry = *entry;
            scan.status = 0;
            return 1;
        };
        int const ITERATE = xfs_dir_iterate(parent, find_name, &scan);
        if (ITERATE < 0 || scan.status != 0 || static_cast<size_t>(scan.entry.namelen) + 1 > cursor) {
            xfs_inode_release_metadata_locked(parent);
            return fail(ITERATE < 0 ? ITERATE : (scan.status != 0 ? scan.status : -ENAMETOOLONG));
        }
        cursor -= scan.entry.namelen;
        std::memcpy(path.data() + cursor, scan.entry.name.data(), scan.entry.namelen);
        path[--cursor] = '/';
        xfs_inode_release_metadata_locked(current);
        current = parent;
    }
    xfs_inode_release_metadata_locked(current);
    if (cursor == path.size() - 1) path[--cursor] = '/';
    size_t const LENGTH = path.size() - cursor;
    if (LENGTH > capacity) return -ENAMETOOLONG;
    std::memcpy(out, path.data() + cursor, LENGTH);
    return 0;
}
}  // namespace

auto xfs_directory_path_at_inode(XfsMountContext* ctx, XfsInode* anchor, char* out, size_t capacity) -> int {
    return directory_path_at_inode(ctx, anchor, nullptr, out, capacity, nullptr);
}

auto xfs_directory_path_beneath(File* root, File* directory, char* out, size_t capacity, const ker::mod::security::Snapshot* credentials)
    -> int {
    if (root == nullptr || directory == nullptr || root->fs_type != FSType::XFS || directory->fs_type != FSType::XFS ||
        root->private_data == nullptr || directory->private_data == nullptr)
        return -EBADF;
    if (!root->is_directory || !directory->is_directory) return -ENOTDIR;
    auto* root_data = static_cast<XfsFileData*>(root->private_data);
    auto* directory_data = static_cast<XfsFileData*>(directory->private_data);
    if (root_data->mount == nullptr || root_data->inode == nullptr || directory_data->mount == nullptr || directory_data->inode == nullptr)
        return -EBADF;
    if (root_data->mount != directory_data->mount) return -EXDEV;
    return directory_path_at_inode(root_data->mount, directory_data->inode, root_data->inode, out, capacity, credentials);
}

auto xfs_lookup_directory_path_beneath(File* root, const XfsLookupHandle& lookup, char* out, size_t capacity) -> int {
    auto* ctx = lookup.mount();
    if (ctx == nullptr) return -EBADF;
    XfsInode* boundary = nullptr;
    if (root != nullptr) {
        if (root->fs_type != FSType::XFS || root->private_data == nullptr) return -EBADF;
        if (!root->is_directory) return -ENOTDIR;
        auto* data = static_cast<XfsFileData*>(root->private_data);
        if (data->mount != ctx) return -EXDEV;
        if (data->inode == nullptr) return -EBADF;
        boundary = data->inode;
    }
    XfsInode* selected = nullptr;
    {
        XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());
        int const VALID = xfs_validate_lookup_locked(lookup);
        if (VALID != 0) return VALID;
        auto* target = lookup.target();
        selected = target != nullptr && xfs_inode_isdir(target) ? target : lookup.parent();
    }
    // The handle retains either selection; ancestry takes its own metadata guard.
    return directory_path_at_inode(ctx, selected, boundary, out, capacity, nullptr);
}

auto xfs_acquire_lookup_at_file(const ker::mod::security::Snapshot& credentials, File* directory, const char* relative_path,
                                XfsLookupHandle* out) -> int {
    if (directory == nullptr || out == nullptr) {
        return -EBADF;
    }
    if (relative_path == nullptr || relative_path[0] == '/') {
        return -EINVAL;
    }
    if (directory->fs_type != FSType::XFS || directory->private_data == nullptr) {
        return -EBADF;
    }
    if (!directory->is_directory) {
        return -ENOTDIR;
    }

    auto* xfd = static_cast<XfsFileData*>(directory->private_data);
    if (xfd->mount == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }
    return xfs_acquire_lookup_at_inode(credentials, xfd->mount, xfd->inode, relative_path, out);
}

auto xfs_acquire_lookup_at_inode(const ker::mod::security::Snapshot& credentials, XfsMountContext* ctx, XfsInode* anchor,
                                 const char* relative_path, XfsLookupHandle* out) -> int {
    if (ctx == nullptr || anchor == nullptr || out == nullptr) return -EBADF;
    if (relative_path == nullptr || relative_path[0] == '/') return -EINVAL;
    std::array<char, XFS_LOOKUP_PATH_CAPACITY> pending{};
    size_t input_length = 0;
    while (relative_path[input_length] != '\0') {
        if (input_length >= pending.size() - 1) {
            return -ENAMETOOLONG;
        }
        pending.at(input_length) = relative_path[input_length];
        ++input_length;
    }
    pending.at(input_length) = '\0';

    *out = XfsLookupHandle{};
    XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());

    XfsInode* current = xfs_inode_read_cached(ctx, anchor->ino);
    if (current == nullptr || current != anchor) {
        xfs_inode_release_metadata_locked(current);
        return -EAGAIN;
    }
    if (!xfs_inode_isdir(current)) {
        xfs_inode_release_metadata_locked(current);
        return -ENOTDIR;
    }
    if (current->nlink == 0) {
        xfs_inode_release_metadata_locked(current);
        return -ENOENT;
    }

    if (relative_path[0] == '\0') {
        auto* target = xfs_inode_read_cached(ctx, current->ino);
        if (target == nullptr || target != current) {
            xfs_inode_release_metadata_locked(target);
            xfs_inode_release_metadata_locked(current);
            return -EAGAIN;
        }
        XfsLookupHandle result{};
        result.mount_ = ctx;
        result.parent_ = current;
        result.target_ = target;
        result.expected_parent_generation_ = current->dir_generation;
        result.expected_lookup_sequence_ = ctx->lookup_sequence.load(std::memory_order_acquire);
        result.target_existed_ = true;
        result.root_target_ = true;
        *out = std::move(result);
        return 0;
    }

    constexpr unsigned MAX_LOOKUP_SYMLINKS = 8;
    const char* cursor = pending.data();
    size_t path_length = 0;
    unsigned symlink_depth = 0;
    for (;;) {
        while (*cursor == '/') {
            ++cursor;
            ++path_length;
        }
        const char* const COMPONENT = cursor;
        size_t component_length = 0;
        while (cursor[component_length] != '\0' && cursor[component_length] != '/') {
            if (component_length >= XFS_LOOKUP_LEAF_CAPACITY - 1 || path_length + component_length >= XFS_LOOKUP_PATH_CAPACITY - 1) {
                xfs_inode_release_metadata_locked(current);
                return -ENAMETOOLONG;
            }
            ++component_length;
        }
        if (component_length == 0) {
            xfs_inode_release_metadata_locked(current);
            return -EINVAL;
        }
        cursor += component_length;
        const char* rest = cursor;
        while (*rest == '/') {
            ++rest;
        }
        bool const FINAL_COMPONENT = *rest == '\0';
        auto const COMPONENT_LENGTH = static_cast<uint16_t>(component_length);

        int const SEARCH = ker::mod::security::authorize_file_access(credentials, {current->uid, current->gid, current->mode}, 1);
        if (SEARCH != 0) {
            xfs_inode_release_metadata_locked(current);
            return SEARCH;
        }

        XfsDirEntry entry{};
        int const LOOKUP_RET = xfs_dir_lookup_authoritative(current, COMPONENT, COMPONENT_LENGTH, &entry);
        XfsInode* target = nullptr;
        if (LOOKUP_RET == 0) {
            target = xfs_inode_read_known_allocated(ctx, entry.ino);
            if (target == nullptr) {
                xfs_inode_release_metadata_locked(current);
                return -ENOENT;
            }
        } else if (LOOKUP_RET != -ENOENT || !FINAL_COMPONENT) {
            xfs_inode_release_metadata_locked(current);
            return LOOKUP_RET;
        }

        if (!FINAL_COMPONENT) {
            if (xfs_inode_islnk(target)) {
                if (symlink_depth == MAX_LOOKUP_SYMLINKS) {
                    xfs_inode_release_metadata_locked(target);
                    xfs_inode_release_metadata_locked(current);
                    return -ELOOP;
                }
                ++symlink_depth;
                std::array<char, XFS_LOOKUP_PATH_CAPACITY> link_target{};
                int const READ_RESULT = xfs_readlink(target, link_target.data(), link_target.size() - 1);
                xfs_inode_release_metadata_locked(target);
                if (READ_RESULT < 0) {
                    xfs_inode_release_metadata_locked(current);
                    return READ_RESULT;
                }
                size_t const TARGET_LENGTH = static_cast<size_t>(READ_RESULT);
                if (TARGET_LENGTH == 0) {
                    xfs_inode_release_metadata_locked(current);
                    return -ENOENT;
                }
                link_target.at(TARGET_LENGTH) = '\0';
                size_t const REST_LENGTH = std::strlen(rest);
                bool const NEED_SEPARATOR = REST_LENGTH != 0 && link_target.at(TARGET_LENGTH - 1) != '/';
                if (TARGET_LENGTH + (NEED_SEPARATOR ? 1 : 0) + REST_LENGTH >= pending.size()) {
                    xfs_inode_release_metadata_locked(current);
                    return -ENAMETOOLONG;
                }
                std::array<char, XFS_LOOKUP_PATH_CAPACITY> replacement{};
                std::memcpy(replacement.data(), link_target.data(), TARGET_LENGTH);
                size_t replacement_length = TARGET_LENGTH;
                if (NEED_SEPARATOR) {
                    replacement.at(replacement_length++) = '/';
                }
                if (REST_LENGTH != 0) {
                    std::memcpy(replacement.data() + replacement_length, rest, REST_LENGTH);
                    replacement_length += REST_LENGTH;
                }
                replacement.at(replacement_length) = '\0';
                bool const ABSOLUTE_TARGET = replacement.front() == '/';
                pending = replacement;
                cursor = pending.data();
                path_length = 0;
                if (ABSOLUTE_TARGET) {
                    XfsInode* root = xfs_root_inode_read(ctx);
                    xfs_inode_release_metadata_locked(current);
                    if (root == nullptr) {
                        return -ENOENT;
                    }
                    current = root;
                }
                while (*cursor == '/') {
                    ++cursor;
                    ++path_length;
                }
                continue;
            }
            if (!xfs_inode_isdir(target)) {
                xfs_inode_release_metadata_locked(target);
                xfs_inode_release_metadata_locked(current);
                return -ENOTDIR;
            }
            xfs_inode_release_metadata_locked(current);
            current = target;
            cursor = rest;
            path_length = static_cast<size_t>(cursor - pending.data());
            continue;
        }

        XfsLookupHandle result{};
        result.mount_ = ctx;
        result.parent_ = current;
        result.target_ = target;
        std::memcpy(result.leaf_.data(), COMPONENT, component_length);
        result.leaf_.at(component_length) = '\0';
        result.leaf_length_ = COMPONENT_LENGTH;
        // A dirfd-relative handle intentionally has no textual path identity.
        // Mutation consumers conservatively invalidate mount path caches.
        result.path_.front() = '\0';
        result.path_length_ = 0;
        result.expected_parent_generation_ = current->dir_generation;
        result.expected_lookup_sequence_ = ctx->lookup_sequence.load(std::memory_order_acquire);
        result.target_existed_ = target != nullptr;
        *out = std::move(result);
        return 0;
    }
}

auto xfs_validate_lookup_locked(const XfsLookupHandle& handle) -> int {
    XfsMountContext* const MOUNT = handle.mount_;
    XfsInode* const PARENT = handle.parent_;
    if (MOUNT == nullptr || PARENT == nullptr || PARENT->mount != MOUNT || !xfs_inode_isdir(PARENT)) {
        return -EINVAL;
    }
    if (MOUNT->lookup_sequence.load(std::memory_order_acquire) != handle.expected_lookup_sequence_ ||
        PARENT->dir_generation != handle.expected_parent_generation_ || PARENT->nlink == 0) {
        return -EAGAIN;
    }
    if (handle.root_target_) {
        return handle.target_existed_ && handle.target_ == PARENT ? 0 : -EAGAIN;
    }

    XfsDirEntry current{};
    int const LOOKUP_RET = xfs_dir_lookup_authoritative(PARENT, handle.leaf_.data(), handle.leaf_length_, &current);
    if (!handle.target_existed_) {
        if (LOOKUP_RET == -ENOENT) {
            return 0;
        }
        return LOOKUP_RET == 0 ? -EAGAIN : LOOKUP_RET;
    }
    if (LOOKUP_RET != 0) {
        return LOOKUP_RET == -ENOENT ? -EAGAIN : LOOKUP_RET;
    }
    return handle.target_ != nullptr && current.ino == handle.target_->ino ? 0 : -EAGAIN;
}

auto xfs_validate_lookup(const XfsLookupHandle& handle) -> int {
    if (handle.mount() == nullptr) {
        return -EINVAL;
    }
    XfsMetadataGuard metadata_guard(handle.mount(), true, WOS_PERF_CALLSITE());
    return xfs_validate_lookup_locked(handle);
}

auto xfs_readlink_path(const char* fs_path, char* buf, size_t bufsize, XfsMountContext* ctx, size_t known_fs_path_len) -> ssize_t {
    if (fs_path == nullptr || buf == nullptr || bufsize == 0 || ctx == nullptr) {
        return -EINVAL;
    }
    if (fs_path[0] == '\0' || (fs_path[0] == '/' && fs_path[1] == '\0')) {
        return -EINVAL;
    }

    size_t const CACHE_PATH_LEN = xfs_known_path_len(fs_path, known_fs_path_len);
    xfs_ino_t cached_ino = NULLFSINO;
    uint8_t cached_ftype = XFS_DIR3_FT_UNKNOWN;
    bool const PATH_CACHE_HIT = xfs_path_inode_cache_lookup_ino(ctx, fs_path, CACHE_PATH_LEN, &cached_ino, &cached_ftype);
    if (PATH_CACHE_HIT && cached_ftype != XFS_DIR3_FT_UNKNOWN && cached_ftype != XFS_DIR3_FT_SYMLINK) {
        xfs_parent_path_cache_store_known_directory(ctx, fs_path, CACHE_PATH_LEN, cached_ino, cached_ftype);
        return -EINVAL;
    }
    ssize_t cached_result = 0;
    if (PATH_CACHE_HIT && xfs_readlink_cached_inode(ctx, cached_ino, fs_path, CACHE_PATH_LEN, buf, bufsize, &cached_result)) {
        return cached_result;
    }

    XfsDirEntry cached_entry{};
    int cached_lookup_result = 0;
    size_t cached_fs_path_len = UNKNOWN_XFS_PATH_LEN;
    bool const DENTRY_CACHE_HIT =
        xfs_cached_parent_dentry_lookup(fs_path, ctx, &cached_entry, &cached_lookup_result, known_fs_path_len, &cached_fs_path_len);
    if (DENTRY_CACHE_HIT) {
        if (cached_lookup_result != 0) {
            return cached_lookup_result;
        }
        if (cached_entry.ftype != XFS_DIR3_FT_UNKNOWN && cached_entry.ftype != XFS_DIR3_FT_SYMLINK) {
            xfs_path_inode_cache_store(ctx, fs_path, cached_fs_path_len, cached_entry.ino, cached_entry.ftype);
            xfs_parent_path_cache_store_directory_entry(ctx, fs_path, cached_fs_path_len, cached_entry);
            return -EINVAL;
        }
        if (xfs_readlink_cached_inode(ctx, cached_entry.ino, fs_path, cached_fs_path_len, buf, bufsize, &cached_result)) {
            return cached_result;
        }
    }

    XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());

    if (PATH_CACHE_HIT) {
        XfsInode* cached_ip = xfs_inode_read_known_allocated(ctx, cached_ino);
        if (cached_ip == nullptr) {
            return -ENOENT;
        }
        if (cached_ip->nlink == 0) {
            xfs_inode_release(cached_ip);
            return -ENOENT;
        }
        if (!xfs_inode_islnk(cached_ip)) {
            uint8_t const FTYPE = xfs_ftype_from_inode(cached_ip);
            xfs_path_inode_cache_store(ctx, fs_path, CACHE_PATH_LEN, cached_ino, FTYPE);
            xfs_parent_path_cache_store_known_directory(ctx, fs_path, CACHE_PATH_LEN, cached_ino, FTYPE);
            xfs_inode_release(cached_ip);
            return -EINVAL;
        }

        int const RET = xfs_readlink(cached_ip, buf, bufsize);
        xfs_path_inode_cache_store(ctx, fs_path, CACHE_PATH_LEN, cached_ino, XFS_DIR3_FT_SYMLINK);
        xfs_inode_release(cached_ip);
        return RET < 0 ? RET : static_cast<ssize_t>(RET);
    }

    if (DENTRY_CACHE_HIT) {
        if (cached_lookup_result != 0) {
            return cached_lookup_result;
        }
        if (cached_entry.ftype != XFS_DIR3_FT_UNKNOWN && cached_entry.ftype != XFS_DIR3_FT_SYMLINK) {
            xfs_path_inode_cache_store(ctx, fs_path, cached_fs_path_len, cached_entry.ino, cached_entry.ftype);
            xfs_parent_path_cache_store_directory_entry(ctx, fs_path, cached_fs_path_len, cached_entry);
            return -EINVAL;
        }

        XfsInode* cached_ip = xfs_inode_read_known_allocated(ctx, cached_entry.ino);
        if (cached_ip == nullptr) {
            return -ENOENT;
        }
        if (!xfs_inode_islnk(cached_ip)) {
            uint8_t const FTYPE = xfs_ftype_from_inode(cached_ip);
            xfs_path_inode_cache_store(ctx, fs_path, cached_fs_path_len, cached_entry.ino, FTYPE);
            xfs_parent_path_cache_store_known_directory(ctx, fs_path, cached_fs_path_len, cached_entry.ino, FTYPE);
            xfs_inode_release(cached_ip);
            return -EINVAL;
        }

        int const RET = xfs_readlink(cached_ip, buf, bufsize);
        xfs_path_inode_cache_store(ctx, fs_path, cached_fs_path_len, cached_entry.ino, XFS_DIR3_FT_SYMLINK);
        xfs_inode_release(cached_ip);
        return RET < 0 ? RET : static_cast<ssize_t>(RET);
    }

    XfsInode* parent_ip = nullptr;
    const char* filename = nullptr;
    uint16_t filename_len = 0;
    size_t fs_path_len = UNKNOWN_XFS_PATH_LEN;
    int const PARENT_RET =
        xfs_find_parent_and_name(fs_path, ctx, &parent_ip, &filename, &filename_len, nullptr, true, known_fs_path_len, &fs_path_len);
    if (PARENT_RET != 0) {
        return PARENT_RET;
    }

    XfsDirEntry entry{};
    int const LOOKUP_RET = xfs_dir_lookup(parent_ip, filename, filename_len, &entry);
    xfs_inode_release(parent_ip);
    if (LOOKUP_RET != 0) {
        return LOOKUP_RET;
    }

    if (entry.ftype != XFS_DIR3_FT_UNKNOWN && entry.ftype != XFS_DIR3_FT_SYMLINK) {
        xfs_path_inode_cache_store(ctx, fs_path, fs_path_len, entry.ino, entry.ftype);
        xfs_parent_path_cache_store_directory_entry(ctx, fs_path, fs_path_len, entry);
        return -EINVAL;
    }

    XfsInode* ip = xfs_inode_read_known_allocated(ctx, entry.ino);
    if (ip == nullptr) {
        return -ENOENT;
    }
    if (!xfs_inode_islnk(ip)) {
        uint8_t const FTYPE = xfs_ftype_from_inode(ip);
        xfs_path_inode_cache_store(ctx, fs_path, fs_path_len, entry.ino, FTYPE);
        xfs_parent_path_cache_store_known_directory(ctx, fs_path, fs_path_len, entry.ino, FTYPE);
        xfs_inode_release(ip);
        return -EINVAL;
    }

    int const RET = xfs_readlink(ip, buf, bufsize);
    xfs_path_inode_cache_store(ctx, fs_path, fs_path_len, entry.ino, XFS_DIR3_FT_SYMLINK);
    xfs_inode_release(ip);
    return RET < 0 ? RET : static_cast<ssize_t>(RET);
}

namespace {

auto xfs_path_exists_cached_unlocked(const char* fs_path, bool require_directory, XfsMountContext* ctx, size_t known_fs_path_len) -> int {
    if (fs_path == nullptr || ctx == nullptr || fs_path[0] == '\0' || (fs_path[0] == '/' && fs_path[1] == '\0')) {
        return -EAGAIN;
    }

    auto result_from_ftype = [&](uint8_t ftype) -> int {
        if (ftype == XFS_DIR3_FT_DIR) {
            return 0;
        }
        if (ftype != XFS_DIR3_FT_UNKNOWN) {
            return require_directory ? -ENOTDIR : 0;
        }
        return -EAGAIN;
    };

    size_t const CACHE_PATH_LEN = xfs_known_path_len(fs_path, known_fs_path_len);
    xfs_ino_t cached_ino = NULLFSINO;
    uint8_t cached_ftype = XFS_DIR3_FT_UNKNOWN;
    if (xfs_path_inode_cache_lookup_ino(ctx, fs_path, CACHE_PATH_LEN, &cached_ino, &cached_ftype)) {
        int const TYPE_RET = result_from_ftype(cached_ftype);
        if (TYPE_RET != -EAGAIN) {
            if (cached_ftype == XFS_DIR3_FT_DIR) {
                xfs_parent_path_cache_store(ctx, fs_path, CACHE_PATH_LEN, cached_ino);
            }
            return TYPE_RET;
        }

        ker::vfs::Stat statbuf{};
        int const STAT_RET = xfs_inode_stat_cached(ctx, cached_ino, &statbuf);
        if (STAT_RET != 0) {
            return STAT_RET;
        }

        uint8_t const FTYPE = xfs_ftype_from_mode(static_cast<uint16_t>(statbuf.st_mode));
        xfs_path_inode_cache_store(ctx, fs_path, CACHE_PATH_LEN, cached_ino, FTYPE);
        if (FTYPE == XFS_DIR3_FT_DIR) {
            xfs_parent_path_cache_store_known_directory(ctx, fs_path, CACHE_PATH_LEN, cached_ino, FTYPE);
            return 0;
        }
        return require_directory ? -ENOTDIR : 0;
    }

    XfsDirEntry cached_entry{};
    int cached_lookup_result = 0;
    size_t cached_fs_path_len = UNKNOWN_XFS_PATH_LEN;
    if (!xfs_cached_parent_dentry_lookup(fs_path, ctx, &cached_entry, &cached_lookup_result, known_fs_path_len, &cached_fs_path_len)) {
        return -EAGAIN;
    }
    if (cached_lookup_result != 0) {
        return cached_lookup_result;
    }

    int const TYPE_RET = result_from_ftype(cached_entry.ftype);
    if (TYPE_RET != -EAGAIN) {
        xfs_path_inode_cache_store(ctx, fs_path, cached_fs_path_len, cached_entry.ino, cached_entry.ftype);
        if (cached_entry.ftype == XFS_DIR3_FT_DIR) {
            xfs_parent_path_cache_store_directory_entry(ctx, fs_path, cached_fs_path_len, cached_entry);
        }
        return TYPE_RET;
    }

    ker::vfs::Stat statbuf{};
    int const STAT_RET = xfs_inode_stat_cached(ctx, cached_entry.ino, &statbuf);
    if (STAT_RET != 0) {
        return STAT_RET;
    }

    uint8_t const FTYPE = xfs_ftype_from_mode(static_cast<uint16_t>(statbuf.st_mode));
    xfs_path_inode_cache_store(ctx, fs_path, cached_fs_path_len, cached_entry.ino, FTYPE);
    if (FTYPE == XFS_DIR3_FT_DIR) {
        xfs_parent_path_cache_store(ctx, fs_path, cached_fs_path_len, cached_entry.ino);
        return 0;
    }
    return require_directory ? -ENOTDIR : 0;
}

}  // namespace

auto xfs_path_exists(const char* fs_path, bool require_directory, XfsMountContext* ctx, size_t known_fs_path_len) -> int {
    if (fs_path == nullptr || ctx == nullptr) {
        return -EINVAL;
    }

    int const CACHED_RET = xfs_path_exists_cached_unlocked(fs_path, require_directory, ctx, known_fs_path_len);
    if (CACHED_RET != -EAGAIN) {
        return CACHED_RET;
    }

    XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());

    if (fs_path[0] == '\0' || (fs_path[0] == '/' && fs_path[1] == '\0')) {
        XfsInode* root = xfs_root_inode_read(ctx);
        if (root == nullptr) {
            return -ENOENT;
        }
        bool const IS_DIR = xfs_inode_isdir(root);
        xfs_inode_release(root);
        return (!require_directory || IS_DIR) ? 0 : -ENOTDIR;
    }

    size_t const CACHE_PATH_LEN = xfs_known_path_len(fs_path, known_fs_path_len);
    xfs_ino_t cached_ino = NULLFSINO;
    uint8_t cached_ftype = XFS_DIR3_FT_UNKNOWN;
    if (xfs_path_inode_cache_lookup_ino(ctx, fs_path, CACHE_PATH_LEN, &cached_ino, &cached_ftype)) {
        if (cached_ftype == XFS_DIR3_FT_DIR) {
            xfs_parent_path_cache_store(ctx, fs_path, CACHE_PATH_LEN, cached_ino);
            return 0;
        }
        if (!require_directory) {
            return 0;
        }
        if (cached_ftype != XFS_DIR3_FT_UNKNOWN) {
            return require_directory ? -ENOTDIR : 0;
        }

        XfsInode* cached_ip = xfs_inode_read_known_allocated(ctx, cached_ino);
        if (cached_ip == nullptr) {
            return -ENOENT;
        }
        if (cached_ip->nlink == 0) {
            xfs_inode_release(cached_ip);
            return -ENOENT;
        }
        uint8_t const FTYPE = xfs_ftype_from_inode(cached_ip);
        xfs_path_inode_cache_store(ctx, fs_path, CACHE_PATH_LEN, cached_ino, FTYPE);
        bool const IS_DIR = FTYPE == XFS_DIR3_FT_DIR;
        if (IS_DIR) {
            xfs_parent_path_cache_store(ctx, fs_path, CACHE_PATH_LEN, cached_ino);
        }
        xfs_inode_release(cached_ip);
        return (!require_directory || IS_DIR) ? 0 : -ENOTDIR;
    }

    XfsDirEntry cached_entry{};
    int cached_lookup_result = 0;
    size_t cached_fs_path_len = UNKNOWN_XFS_PATH_LEN;
    if (xfs_cached_parent_dentry_lookup(fs_path, ctx, &cached_entry, &cached_lookup_result, known_fs_path_len, &cached_fs_path_len)) {
        if (cached_lookup_result != 0) {
            return cached_lookup_result;
        }

        if (cached_entry.ftype == XFS_DIR3_FT_DIR) {
            xfs_path_inode_cache_store(ctx, fs_path, cached_fs_path_len, cached_entry.ino, XFS_DIR3_FT_DIR);
            xfs_parent_path_cache_store_directory_entry(ctx, fs_path, cached_fs_path_len, cached_entry);
            return 0;
        }
        if (!require_directory) {
            xfs_path_inode_cache_store(ctx, fs_path, cached_fs_path_len, cached_entry.ino, cached_entry.ftype);
            return 0;
        }
        if (cached_entry.ftype != XFS_DIR3_FT_UNKNOWN) {
            xfs_path_inode_cache_store(ctx, fs_path, cached_fs_path_len, cached_entry.ino, cached_entry.ftype);
            return -ENOTDIR;
        }

        XfsInode* cached_ip = xfs_inode_read_known_allocated(ctx, cached_entry.ino);
        if (cached_ip == nullptr) {
            return -ENOENT;
        }
        uint8_t const FTYPE = xfs_ftype_from_inode(cached_ip);
        xfs_path_inode_cache_store(ctx, fs_path, cached_fs_path_len, cached_entry.ino, FTYPE);
        bool const IS_DIR = FTYPE == XFS_DIR3_FT_DIR;
        if (IS_DIR) {
            xfs_parent_path_cache_store(ctx, fs_path, cached_fs_path_len, cached_entry.ino);
        }
        xfs_inode_release(cached_ip);
        return IS_DIR ? 0 : -ENOTDIR;
    }

    XfsInode* parent_ip = nullptr;
    const char* filename = nullptr;
    uint16_t filename_len = 0;
    size_t fs_path_len = UNKNOWN_XFS_PATH_LEN;
    int const PARENT_RET =
        xfs_find_parent_and_name(fs_path, ctx, &parent_ip, &filename, &filename_len, nullptr, true, known_fs_path_len, &fs_path_len);
    if (PARENT_RET != 0) {
        return PARENT_RET;
    }

    XfsDirEntry entry{};
    int const LOOKUP_RET = xfs_dir_lookup(parent_ip, filename, filename_len, &entry);
    xfs_inode_release(parent_ip);
    if (LOOKUP_RET != 0) {
        return LOOKUP_RET;
    }

    if (entry.ftype == XFS_DIR3_FT_DIR) {
        xfs_path_inode_cache_store(ctx, fs_path, fs_path_len, entry.ino, XFS_DIR3_FT_DIR);
        xfs_parent_path_cache_store_directory_entry(ctx, fs_path, fs_path_len, entry);
        return 0;
    }
    if (!require_directory) {
        xfs_path_inode_cache_store(ctx, fs_path, fs_path_len, entry.ino, entry.ftype);
        return 0;
    }
    if (entry.ftype != XFS_DIR3_FT_UNKNOWN) {
        xfs_path_inode_cache_store(ctx, fs_path, fs_path_len, entry.ino, entry.ftype);
        return -ENOTDIR;
    }

    XfsInode* ip = xfs_inode_read_known_allocated(ctx, entry.ino);
    if (ip == nullptr) {
        return -ENOENT;
    }
    uint8_t const FTYPE = xfs_ftype_from_inode(ip);
    xfs_path_inode_cache_store(ctx, fs_path, fs_path_len, entry.ino, FTYPE);
    bool const IS_DIR = FTYPE == XFS_DIR3_FT_DIR;
    if (IS_DIR) {
        xfs_parent_path_cache_store(ctx, fs_path, fs_path_len, entry.ino);
    }
    xfs_inode_release(ip);
    return IS_DIR ? 0 : -ENOTDIR;
}

// ============================================================================
// Stat helpers
// ============================================================================

namespace {

void fill_stat(XfsInode* ip, ker::vfs::Stat* st) { static_cast<void>(xfs_inode_fill_stat(ip, st)); }

}  // anonymous namespace

namespace {
void xfs_set_open_result(int* result_out, int result) {
    if (result_out != nullptr) {
        *result_out = result;
    }
}

// Allocate fallible per-open storage before a create transaction publishes its
// inode/name. Neither object owns an inode reference until final adoption.
struct XfsOpenStorage {
    std::unique_ptr<File> file{};
    std::unique_ptr<XfsFileData> data{};
    auto prepare() -> bool {
        if (file == nullptr) file.reset(new (std::nothrow) File{});
        if (file != nullptr && data == nullptr) data.reset(new (std::nothrow) XfsFileData{});
        return file != nullptr && data != nullptr;
    }
};

auto xfs_file_from_inode(XfsOpenStorage& storage, XfsMountContext* ctx, XfsInode* ip, int flags, const char* fs_path,
                         size_t known_fs_path_len, bool created_by_open, const ker::vfs::Stat& opened_stat, int* result_out) -> File* {
    if (ctx == nullptr || ip == nullptr) {
        xfs_set_open_result(result_out, -EINVAL);
        return nullptr;
    }

    bool const IS_DIRECTORY = xfs_inode_isdir(ip);

    auto* f = storage.file.release();
    auto* xfd = storage.data.release();
    xfd->mount = ctx;
    xfd->inode = ip;
    if (IS_DIRECTORY && fs_path != nullptr) {
        xfs_file_data_set_fs_path(xfd, fs_path, known_fs_path_len);
    }

    f->private_data = xfd;
    f->fops = &xfs_fops;
    f->pos = 0;
    f->is_directory = IS_DIRECTORY;
    f->fs_type = FSType::XFS;
    f->refcount = 1;
    f->open_flags = flags & ~ker::vfs::O_WOS_KNOWN_ABSENT;
    f->fd_flags = 0;
    f->open_create_result_known = (flags & ker::vfs::O_CREAT) != 0;
    f->created_by_open = created_by_open;
    f->vfs_path = nullptr;
    f->dir_fs_count = static_cast<size_t>(-1);

    ker::vfs::vfs_prefill_file_stat_snapshot(f, opened_stat);

    xfs_set_open_result(result_out, 0);
    return f;
}

}  // namespace

auto xfs_open_path(const ker::mod::security::Snapshot& credentials, const char* fs_path, int flags, int mode, XfsMountContext* ctx,
                   int* result_out, size_t known_fs_path_len, bool require_directory) -> File* {
    constexpr int O_CREAT_FLAG = 0100;
    constexpr int O_EXCL_FLAG = ker::vfs::O_EXCL;
    constexpr int O_WOS_KNOWN_ABSENT_FLAG = ker::vfs::O_WOS_KNOWN_ABSENT;
    constexpr int TRUSTED_KNOWN_ABSENT_MASK = O_CREAT_FLAG | O_EXCL_FLAG | O_WOS_KNOWN_ABSENT_FLAG;

    if (ctx == nullptr) {
        xfs_set_open_result(result_out, -EINVAL);
        return nullptr;
    }
    if (fs_path == nullptr) {
        fs_path = "";
        known_fs_path_len = 0;
    }

    if ((flags & 3) == 3 || (flags & (ker::vfs::O_DIRECTORY | ker::vfs::O_CREAT)) == (ker::vfs::O_DIRECTORY | ker::vfs::O_CREAT)) {
        xfs_set_open_result(result_out, -EINVAL);
        return nullptr;
    }
    require_directory = require_directory || (flags & ker::vfs::O_DIRECTORY) != 0;
    XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());
    XfsOpenStorage storage;

    XfsInode* ip = nullptr;
    bool used_create_lookup = false;
    bool create_missing = false;
    bool repair_unreachable_create_entry = false;
    xfs_ino_t repair_unreachable_create_ino = NULLFSINO;
    bool created_by_open = false;
    bool path_cache_needs_store = true;
    XfsInode* create_parent_ip = nullptr;
    const char* create_filename = nullptr;
    uint16_t create_filename_len = 0;
    // Private checkout fast path: callers may hint that a fresh O_CREAT|O_EXCL
    // target is absent. XFS only trusts that hint when its own same-generation
    // dentry cache already proves absence.
    bool const TRUSTED_KNOWN_ABSENT_CREATE = (flags & TRUSTED_KNOWN_ABSENT_MASK) == TRUSTED_KNOWN_ABSENT_MASK;

    if ((flags & O_CREAT_FLAG) != 0 && !ctx->read_only) {
        uint64_t const PERF_CREATE_LOOKUP_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::CREATE_LOOKUP);
        bool create_lookup_recorded = false;
        auto record_create_lookup = [&](int32_t status) {
            if (!create_lookup_recorded) {
                perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::CREATE_LOOKUP, PERF_CREATE_LOOKUP_STARTED_US, status, 1);
                create_lookup_recorded = true;
            }
        };
        int parent_ret = xfs_find_parent_and_name(fs_path, ctx, &create_parent_ip, &create_filename, &create_filename_len, nullptr, true,
                                                  known_fs_path_len, nullptr);
        if (parent_ret == 0) {
            int const SEARCH = ker::mod::security::authorize_file_access(
                credentials, {create_parent_ip->uid, create_parent_ip->gid, create_parent_ip->mode}, 1);
            if (SEARCH != 0) {
                xfs_inode_release_metadata_locked(create_parent_ip);
                xfs_set_open_result(result_out, SEARCH);
                return nullptr;
            }
            used_create_lookup = true;
            bool create_lookup_handled = false;
            bool const EXCLUSIVE_CREATE = (flags & O_EXCL_FLAG) != 0;
            // O_EXCL is an existence assertion, so it must observe the
            // authoritative directory while metadata mutations are locked.
            // Negative filters and caches are only safe shortcuts for a
            // non-exclusive create whose result does not distinguish a race.
            if (!EXCLUSIVE_CREATE) {
                if (xfs_dir_name_filter_known_absent(create_parent_ip, create_filename, create_filename_len)) {
                    create_lookup_handled = true;
                    create_missing = true;
                    record_create_lookup(0);
                } else if (TRUSTED_KNOWN_ABSENT_CREATE) {
                    XfsDirEntry cached_existing{};
                    int cached_lookup_ret = 0;
                    if (xfs_dentry_cache_lookup_parent(ctx, create_parent_ip->ino, create_filename, create_filename_len, &cached_existing,
                                                       &cached_lookup_ret)) {
                        create_lookup_handled = true;
                        if (cached_lookup_ret == -ENOENT) {
                            create_missing = true;
                            record_create_lookup(0);
                        } else if (cached_lookup_ret == 0) {
                            xfs_inode_release(create_parent_ip);
                            create_parent_ip = nullptr;
                            record_create_lookup(-EEXIST);
                            xfs_set_open_result(result_out, -EEXIST);
                            return nullptr;
                        } else {
                            xfs_inode_release(create_parent_ip);
                            create_parent_ip = nullptr;
                            record_create_lookup(cached_lookup_ret);
                            xfs_set_open_result(result_out, cached_lookup_ret);
                            return nullptr;
                        }
                    }
                }
            }
            if (!create_lookup_handled) {
                XfsDirEntry existing{};
                int lookup_ret = xfs_dir_lookup(create_parent_ip, create_filename, create_filename_len, &existing);
                if (lookup_ret == 0) {
                    ip = xfs_inode_read_known_allocated(ctx, existing.ino);
                    if (ip != nullptr && ip->nlink == 0) {
                        xfs_inode_release_metadata_locked(ip);
                        ip = nullptr;
                        create_missing = true;
                        repair_unreachable_create_entry = true;
                        repair_unreachable_create_ino = existing.ino;
                        record_create_lookup(0);
                    } else if (ip == nullptr) {
                        ker::vfs::Stat existing_stat{};
                        int const TARGET_STATUS = xfs_inode_stat(ctx, existing.ino, &existing_stat, true, true);
                        if (TARGET_STATUS == -ENOENT) {
                            create_missing = true;
                            repair_unreachable_create_entry = true;
                            repair_unreachable_create_ino = existing.ino;
                            record_create_lookup(0);
                        } else {
                            xfs_inode_release(create_parent_ip);
                            create_parent_ip = nullptr;
                            int const OPEN_ERROR = TARGET_STATUS == 0 ? -EIO : TARGET_STATUS;
                            record_create_lookup(OPEN_ERROR);
                            xfs_set_open_result(result_out, OPEN_ERROR);
                            return nullptr;
                        }
                    } else if ((flags & O_EXCL_FLAG) != 0) {
                        xfs_inode_release_metadata_locked(ip);
                        ip = nullptr;
                        xfs_inode_release(create_parent_ip);
                        create_parent_ip = nullptr;
                        record_create_lookup(-EEXIST);
                        xfs_set_open_result(result_out, -EEXIST);
                        return nullptr;
                    } else {
                        xfs_inode_release(create_parent_ip);
                        create_parent_ip = nullptr;
                        record_create_lookup(0);
                    }
                } else if (lookup_ret == -ENOENT) {
                    create_missing = true;
                    record_create_lookup(0);
                } else if (lookup_ret != -ENOENT) {
                    xfs_inode_release(create_parent_ip);
                    record_create_lookup(lookup_ret);
                    xfs_set_open_result(result_out, lookup_ret);
                    return nullptr;
                }
            }
        } else if (parent_ret != -EINVAL) {
            record_create_lookup(parent_ret);
            xfs_set_open_result(result_out, parent_ret);
            return nullptr;
        } else {
            record_create_lookup(0);
        }
    }

    if (!used_create_lookup) {
        XfsInode* permission_parent = nullptr;
        const char* permission_name = nullptr;
        uint16_t permission_name_len = 0;
        int const PARENT_RET = xfs_find_parent_and_name(fs_path, ctx, &permission_parent, &permission_name, &permission_name_len, nullptr,
                                                        true, known_fs_path_len);
        if (PARENT_RET == 0) {
            int const SEARCH = ker::mod::security::authorize_file_access(
                credentials, {permission_parent->uid, permission_parent->gid, permission_parent->mode}, 1);
            xfs_inode_release_metadata_locked(permission_parent);
            if (SEARCH != 0) {
                xfs_set_open_result(result_out, SEARCH);
                return nullptr;
            }
        } else if (PARENT_RET != -EINVAL) {
            xfs_set_open_result(result_out, PARENT_RET);
            return nullptr;
        }

        int const CACHED_OPEN_RET = xfs_lookup_with_cached_parent(fs_path, ctx, &ip, known_fs_path_len, require_directory);
        if (CACHED_OPEN_RET == -EAGAIN) {
            ip = walk_path(ctx, fs_path);
        } else if (CACHED_OPEN_RET == 0) {
            path_cache_needs_store = false;
        } else {
            xfs_set_open_result(result_out, CACHED_OPEN_RET);
            return nullptr;
        }
    }

    if (create_missing) {
        ker::mod::security::FileIdentity identity{};
        int const CREATE_POLICY = require_directory
                                      ? -ENOENT
                                      : ker::mod::security::prepare_file_create(
                                            credentials, {create_parent_ip->uid, create_parent_ip->gid, create_parent_ip->mode},
                                            static_cast<uint32_t>(mode), identity);
        if (CREATE_POLICY != 0 || !storage.prepare()) {
            xfs_inode_release_metadata_locked(create_parent_ip);
            xfs_set_open_result(result_out, CREATE_POLICY != 0 ? CREATE_POLICY : -ENOMEM);
            return nullptr;
        }

        uint64_t const PERF_CREATE_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::OPEN_CREATE);
        bool create_recorded = false;
        auto record_open_create = [&](int32_t status) {
            if (!create_recorded) {
                perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::OPEN_CREATE, PERF_CREATE_STARTED_US, status, 1);
                create_recorded = true;
            }
        };

        // Allocate a new inode
        uint64_t const PERF_TRANS_ALLOC_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::CREATE_TRANS_ALLOC);
        XfsTransaction* tp = xfs_trans_alloc(ctx);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::CREATE_TRANS_ALLOC, PERF_TRANS_ALLOC_STARTED_US,
                              tp == nullptr ? -ENOMEM : 0, 1);
        if (tp == nullptr) {
            xfs_inode_release(create_parent_ip);
            record_open_create(-ENOMEM);
            xfs_set_open_result(result_out, -ENOMEM);
            return nullptr;
        }
        uint16_t const INODE_MODE = static_cast<uint16_t>(identity.mode);
        uint64_t const PERF_IALLOC_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::IALLOC);
        xfs_ino_t const NEW_INO = xfs_ialloc(ctx, tp, INODE_MODE);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::IALLOC, PERF_IALLOC_STARTED_US, NEW_INO == NULLFSINO ? -ENOSPC : 0, 1);
        if (NEW_INO == NULLFSINO) {
            xfs_trans_cancel(tp);
            xfs_inode_release(create_parent_ip);
            record_open_create(-ENOSPC);
            xfs_set_open_result(result_out, -ENOSPC);
            return nullptr;
        }
        if (xfs_ialloc_conflicts_with_cached_inode(ctx, NEW_INO)) {
            xfs_trans_cancel(tp);
            xfs_inode_release(create_parent_ip);
            record_open_create(-EIO);
            xfs_set_open_result(result_out, -EIO);
            return nullptr;
        }

        uint64_t const PERF_INODE_INIT_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::CREATE_INODE_INIT);
        auto* new_inode = xfs_inode_alloc_uninitialized_object();
        if (new_inode == nullptr) {
            perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::CREATE_INODE_INIT, PERF_INODE_INIT_STARTED_US, -ENOMEM, 1);
            xfs_trans_cancel(tp);
            xfs_inode_release(create_parent_ip);
            record_open_create(-ENOMEM);
            xfs_set_open_result(result_out, -ENOMEM);
            return nullptr;
        }
        new_inode->ino = NEW_INO;
        new_inode->mount = ctx;
        new_inode->agno = xfs_ino_ag(NEW_INO, ctx->agino_log);
        new_inode->agino = xfs_ag_ino(NEW_INO, ctx->agino_log);
        new_inode->mode = INODE_MODE;
        new_inode->uid = identity.uid;
        new_inode->gid = identity.gid;
        new_inode->nlink = 1;
        new_inode->size = 0;
        new_inode->nblocks = 0;
        new_inode->gen = 0;
        new_inode->flags = 0;
        new_inode->flags2 = 0;
        new_inode->atime = 0;
        new_inode->mtime = 0;
        new_inode->ctime = 0;
        new_inode->crtime = 0;
        new_inode->data_fork.format = XFS_DINODE_FMT_EXTENTS;
        new_inode->data_fork.extents.list = nullptr;
        new_inode->data_fork.extents.count = 0;
        new_inode->data_fork.extents.capacity = 0;
        new_inode->forkoff = 0;
        new_inode->attr_fork.format = XFS_DINODE_FMT_LOCAL;
        new_inode->attr_fork.local.data = nullptr;
        new_inode->attr_fork.local.size = 0;
        new_inode->has_attr_fork = false;
        new_inode->nextents = 0;
        new_inode->anextents = 0;
        new_inode->refcount = 0;
        new_inode->hash_next = nullptr;
        new_inode->inactivation_started = false;
        new_inode->dirty = false;
        new_inode->dir_generation = 0;
        new_inode->dir_leaf_index_complete_generation = 0;
        new_inode->dir_leaf_index_complete = false;
        for (auto& word : new_inode->dir_name_filter) {
            word = 0;
        }
        new_inode->dir_name_filter_complete = false;
        xfs_stamp_new_inode(new_inode);

        xfs_trans_log_inode(tp, new_inode);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::CREATE_INODE_INIT, PERF_INODE_INIT_STARTED_US, 0, 1);

        if (repair_unreachable_create_entry) {
            int const REMOVE_RC =
                xfs_dir_removename(create_parent_ip, create_filename, create_filename_len, tp, repair_unreachable_create_ino);
            if (REMOVE_RC != 0) {
                xfs_trans_cancel(tp);
                xfs_inode_free_uncached(new_inode);
                xfs_inode_release(create_parent_ip);
                record_open_create(REMOVE_RC);
                xfs_set_open_result(result_out, REMOVE_RC);
                return nullptr;
            }
        }

        // Add directory entry
        uint64_t const PERF_DIR_ADD_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::DIR_ADD);
        int rc = xfs_dir_addname(create_parent_ip, create_filename, create_filename_len, NEW_INO, XFS_DIR3_FT_REG_FILE, tp, true);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::DIR_ADD, PERF_DIR_ADD_STARTED_US, rc, 1);
        if (rc != 0) {
            xfs_trans_cancel(tp);
            xfs_inode_free_uncached(new_inode);
            xfs_inode_release(create_parent_ip);
            record_open_create(rc);
            xfs_set_open_result(result_out, rc);
            return nullptr;
        }

        uint64_t const PERF_COMMIT_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::OPEN_COMMIT);
        rc = xfs_trans_commit(tp);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::OPEN_COMMIT, PERF_COMMIT_STARTED_US, rc, 1);
        xfs_inode_release(create_parent_ip);
        create_parent_ip = nullptr;
        if (rc != 0) {
            xfs_inode_free_uncached(new_inode);
            record_open_create(-EIO);
            xfs_set_open_result(result_out, -EIO);
            return nullptr;
        }
        uint64_t const PERF_PATH_INVALIDATE_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::CREATE_PATH_INVALIDATE);
        xfs_path_inode_cache_invalidate_path(ctx, fs_path, known_fs_path_len);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::CREATE_PATH_INVALIDATE, PERF_PATH_INVALIDATE_STARTED_US, 0, 1);
        created_by_open = true;

        // The transaction has just written this inode.  Cache and use the
        // in-memory copy instead of immediately re-reading it from disk.
        uint64_t const PERF_ICACHE_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::CREATE_ICACHE);
        rc = xfs_inode_cache_new(new_inode);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::CREATE_ICACHE, PERF_ICACHE_STARTED_US, rc, 1);
        if (rc == 0) {
            ip = new_inode;
        } else {
            mod::dbg::log("[xfs] committed create inode %lu could not enter inode cache: %d", static_cast<unsigned long>(NEW_INO), rc);
            xfs_inode_free_uncached(new_inode);
            record_open_create(rc);
            xfs_set_open_result(result_out, rc);
            return nullptr;
        }
        record_open_create(0);
    }

    if (ip == nullptr) {
        xfs_set_open_result(result_out, -ENOENT);
        return nullptr;
    }
    if (require_directory && !xfs_inode_isdir(ip)) {
        xfs_path_inode_cache_store_inode(ctx, fs_path, known_fs_path_len, ip);
        xfs_inode_release(ip);
        xfs_set_open_result(result_out, -ENOTDIR);
        return nullptr;
    }
    if ((flags & ker::vfs::O_NOFOLLOW) != 0 && xfs_inode_islnk(ip)) {
        xfs_inode_release_metadata_locked(ip);
        xfs_set_open_result(result_out, -ELOOP);
        return nullptr;
    }
    int const OPEN_POLICY =
        created_by_open ? 0
                        : ker::mod::security::authorize_file_open(credentials, {ip->uid, ip->gid, ip->mode},
                                                                  static_cast<unsigned>(flags) & 3U, (flags & ker::vfs::O_TRUNC) != 0);
    if (OPEN_POLICY != 0 || !storage.prepare()) {
        xfs_inode_release_metadata_locked(ip);
        xfs_set_open_result(result_out, OPEN_POLICY != 0 ? OPEN_POLICY : -ENOMEM);
        return nullptr;
    }
    if (path_cache_needs_store) {
        xfs_path_inode_cache_store_inode(ctx, fs_path, known_fs_path_len, ip);
        xfs_parent_path_cache_store_directory(ctx, fs_path, known_fs_path_len, ip);
    }

    // Check for O_WRONLY/O_RDWR on a read-only mount
    int const ACCMODE = flags & 3;
    if (ACCMODE == 1 || ACCMODE == 2 || (flags & ker::vfs::O_TRUNC) != 0) {
        // Write access on read-only filesystem
        if (ctx->read_only) {
            xfs_inode_release(ip);
            xfs_set_open_result(result_out, -EROFS);
            return nullptr;
        }
    }

    ker::vfs::Stat opened_stat{};
    fill_stat(ip, &opened_stat);

    metadata_guard.unlock();
    return xfs_file_from_inode(storage, ctx, ip, flags, fs_path, known_fs_path_len, created_by_open, opened_stat, result_out);
}

namespace {

auto xfs_ftype_from_stat(const ker::vfs::Stat& statbuf) -> uint8_t { return xfs_ftype_from_mode(static_cast<uint16_t>(statbuf.st_mode)); }

auto xfs_stat_with_cached_parent(const char* fs_path, XfsMountContext* ctx, ker::vfs::Stat* statbuf, size_t known_fs_path_len,
                                 bool require_directory) -> int {
    if (statbuf == nullptr || ctx == nullptr) {
        return -EINVAL;
    }
    if (fs_path == nullptr || fs_path[0] == '\0' || (fs_path[0] == '/' && fs_path[1] == '\0')) {
        XfsInode* root = xfs_root_inode_read(ctx);
        if (root == nullptr) {
            return -ENOENT;
        }
        fill_stat(root, statbuf);
        xfs_inode_release(root);
        return 0;
    }

    size_t const PATH_LEN = xfs_known_path_len(fs_path, known_fs_path_len);
    xfs_ino_t cached_ino = NULLFSINO;
    uint8_t cached_ftype = XFS_DIR3_FT_UNKNOWN;
    if (xfs_path_inode_cache_lookup_ino(ctx, fs_path, PATH_LEN, &cached_ino, &cached_ftype)) {
        if (require_directory && cached_ftype != XFS_DIR3_FT_UNKNOWN && cached_ftype != XFS_DIR3_FT_DIR) {
            return -ENOTDIR;
        }
        int const RET = xfs_inode_stat(ctx, cached_ino, statbuf, true, true);
        if (RET != 0) {
            return RET;
        }
        uint8_t const FTYPE = cached_ftype != XFS_DIR3_FT_UNKNOWN ? cached_ftype : xfs_ftype_from_stat(*statbuf);
        if (cached_ftype == XFS_DIR3_FT_UNKNOWN) {
            xfs_path_inode_cache_store(ctx, fs_path, PATH_LEN, cached_ino, FTYPE);
        }
        if (FTYPE == XFS_DIR3_FT_DIR) {
            xfs_parent_path_cache_store_known_directory(ctx, fs_path, PATH_LEN, cached_ino, FTYPE);
        }
        return 0;
    }

    XfsDirEntry cached_entry{};
    int cached_lookup_result = 0;
    size_t cached_fs_path_len = UNKNOWN_XFS_PATH_LEN;
    if (xfs_cached_parent_dentry_lookup(fs_path, ctx, &cached_entry, &cached_lookup_result, known_fs_path_len, &cached_fs_path_len)) {
        if (cached_lookup_result != 0) {
            return cached_lookup_result;
        }
        if (require_directory && cached_entry.ftype != XFS_DIR3_FT_UNKNOWN && cached_entry.ftype != XFS_DIR3_FT_DIR) {
            xfs_path_inode_cache_store(ctx, fs_path, cached_fs_path_len, cached_entry.ino, cached_entry.ftype);
            return -ENOTDIR;
        }

        int const STAT_RET = xfs_inode_stat(ctx, cached_entry.ino, statbuf, true, true);
        if (STAT_RET != 0) {
            return STAT_RET;
        }

        uint8_t const FTYPE = cached_entry.ftype != XFS_DIR3_FT_UNKNOWN ? cached_entry.ftype : xfs_ftype_from_stat(*statbuf);
        xfs_path_inode_cache_store(ctx, fs_path, cached_fs_path_len, cached_entry.ino, FTYPE);
        if (FTYPE == XFS_DIR3_FT_DIR) {
            xfs_parent_path_cache_store(ctx, fs_path, cached_fs_path_len, cached_entry.ino);
        }
        return 0;
    }

    XfsInode* parent_ip = nullptr;
    const char* filename = nullptr;
    uint16_t filename_len = 0;
    size_t fs_path_len = UNKNOWN_XFS_PATH_LEN;
    int const PARENT_RET =
        xfs_find_parent_and_name(fs_path, ctx, &parent_ip, &filename, &filename_len, nullptr, true, known_fs_path_len, &fs_path_len);
    if (PARENT_RET != 0) {
        return PARENT_RET == -ENAMETOOLONG ? -EAGAIN : PARENT_RET;
    }

    XfsDirEntry entry{};
    int const LOOKUP_RET = xfs_dir_lookup(parent_ip, filename, filename_len, &entry);
    xfs_inode_release(parent_ip);
    if (LOOKUP_RET != 0) {
        return LOOKUP_RET;
    }

    if (require_directory && entry.ftype != XFS_DIR3_FT_UNKNOWN && entry.ftype != XFS_DIR3_FT_DIR) {
        xfs_path_inode_cache_store(ctx, fs_path, fs_path_len, entry.ino, entry.ftype);
        return -ENOTDIR;
    }

    int const STAT_RET = xfs_inode_stat(ctx, entry.ino, statbuf, true, true);
    if (STAT_RET != 0) {
        return STAT_RET;
    }

    uint8_t const FTYPE = entry.ftype != XFS_DIR3_FT_UNKNOWN ? entry.ftype : xfs_ftype_from_stat(*statbuf);
    xfs_path_inode_cache_store(ctx, fs_path, fs_path_len, entry.ino, FTYPE);
    if (FTYPE == XFS_DIR3_FT_DIR) {
        xfs_parent_path_cache_store(ctx, fs_path, fs_path_len, entry.ino);
    }
    return 0;
}

auto xfs_stat_cached_unlocked(const char* fs_path, XfsMountContext* ctx, ker::vfs::Stat* statbuf, size_t known_fs_path_len,
                              bool require_directory) -> int {
    if (statbuf == nullptr || ctx == nullptr || fs_path == nullptr || fs_path[0] == '\0' || (fs_path[0] == '/' && fs_path[1] == '\0')) {
        return -EAGAIN;
    }

    size_t const PATH_LEN = xfs_known_path_len(fs_path, known_fs_path_len);
    xfs_ino_t cached_ino = NULLFSINO;
    uint8_t cached_ftype = XFS_DIR3_FT_UNKNOWN;
    if (xfs_path_inode_cache_lookup_ino(ctx, fs_path, PATH_LEN, &cached_ino, &cached_ftype)) {
        if (require_directory && cached_ftype != XFS_DIR3_FT_UNKNOWN && cached_ftype != XFS_DIR3_FT_DIR) {
            return -ENOTDIR;
        }
        int const RET = xfs_inode_stat_cached(ctx, cached_ino, statbuf);
        if (RET != 0) {
            return RET;
        }
        uint8_t const FTYPE = cached_ftype != XFS_DIR3_FT_UNKNOWN ? cached_ftype : xfs_ftype_from_stat(*statbuf);
        if (cached_ftype == XFS_DIR3_FT_UNKNOWN) {
            xfs_path_inode_cache_store(ctx, fs_path, PATH_LEN, cached_ino, FTYPE);
        }
        if (require_directory && FTYPE != XFS_DIR3_FT_DIR) {
            return -ENOTDIR;
        }
        if (FTYPE == XFS_DIR3_FT_DIR) {
            xfs_parent_path_cache_store_known_directory(ctx, fs_path, PATH_LEN, cached_ino, FTYPE);
        }
        return 0;
    }

    XfsDirEntry cached_entry{};
    int cached_lookup_result = 0;
    size_t cached_fs_path_len = UNKNOWN_XFS_PATH_LEN;
    if (!xfs_cached_parent_dentry_lookup(fs_path, ctx, &cached_entry, &cached_lookup_result, known_fs_path_len, &cached_fs_path_len)) {
        return -EAGAIN;
    }
    if (cached_lookup_result != 0) {
        return cached_lookup_result;
    }
    if (require_directory && cached_entry.ftype != XFS_DIR3_FT_UNKNOWN && cached_entry.ftype != XFS_DIR3_FT_DIR) {
        xfs_path_inode_cache_store(ctx, fs_path, cached_fs_path_len, cached_entry.ino, cached_entry.ftype);
        return -ENOTDIR;
    }

    int const STAT_RET = xfs_inode_stat_cached(ctx, cached_entry.ino, statbuf);
    if (STAT_RET != 0) {
        return STAT_RET;
    }

    uint8_t const FTYPE = cached_entry.ftype != XFS_DIR3_FT_UNKNOWN ? cached_entry.ftype : xfs_ftype_from_stat(*statbuf);
    xfs_path_inode_cache_store(ctx, fs_path, cached_fs_path_len, cached_entry.ino, FTYPE);
    if (require_directory && FTYPE != XFS_DIR3_FT_DIR) {
        return -ENOTDIR;
    }
    if (FTYPE == XFS_DIR3_FT_DIR) {
        xfs_parent_path_cache_store(ctx, fs_path, cached_fs_path_len, cached_entry.ino);
    }
    return 0;
}

}  // namespace

#ifdef WOS_SELFTEST
auto xfs_selftest_namespace_mutation_lookup_repairs_stale_negative() -> bool {
    return xfs_selftest_namespace_mutation_lookup_repairs_stale_negative_impl();
}
#endif

auto xfs_stat(const char* fs_path, ker::vfs::Stat* statbuf, XfsMountContext* ctx, size_t known_fs_path_len, bool require_directory) -> int {
    if (statbuf == nullptr || ctx == nullptr) {
        return -EINVAL;
    }

    int const UNLOCKED_RET = xfs_stat_cached_unlocked(fs_path, ctx, statbuf, known_fs_path_len, require_directory);
    if (UNLOCKED_RET != -EAGAIN) {
        return UNLOCKED_RET;
    }

    XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());

    int const FAST_RET = xfs_stat_with_cached_parent(fs_path, ctx, statbuf, known_fs_path_len, require_directory);
    if (FAST_RET != -EAGAIN && FAST_RET != -EINVAL) {
        return FAST_RET;
    }

    XfsInode* ip = walk_path(ctx, fs_path);
    if (ip == nullptr) {
        return -ENOENT;
    }
    if (xfs_inode_isdir(ip)) {
        xfs_parent_path_cache_store_directory(ctx, fs_path, known_fs_path_len, ip);
    }

    fill_stat(ip, statbuf);
    xfs_inode_release(ip);
    return 0;
}

auto xfs_clone_exec_file(File* source, File** output) -> int {
    if (source == nullptr || output == nullptr || source->fops != get_xfs_fops()) return -EINVAL;
    auto* original = static_cast<XfsFileData*>(source->private_data);
    if (original == nullptr || original->inode == nullptr) return -EBADF;
    std::unique_ptr<File> file(new (std::nothrow) File{});
    std::unique_ptr<XfsFileData> data(new (std::nothrow) XfsFileData{});
    if (file == nullptr || data == nullptr) return -ENOMEM;
    file->exec_read_view.reset(new (std::nothrow) ExecReadView{});
    if (file->exec_read_view == nullptr) return -ENOMEM;
    XfsInode* inode = xfs_inode_read_cached(original->mount, original->inode->ino);
    if (inode == nullptr) return -EAGAIN;
    int result = 0;
    {
        ker::mod::sys::MutexGuard io_guard(inode->io_lock);
        if (inode != original->inode || !xfs_inode_isreg(inode)) {
            result = -ENOEXEC;
        } else {
            result = xfs_inode_fill_stat(inode, &file->exec_read_view->metadata);
            if (result == 0) result = inode->exec_content.acquire(*file->exec_read_view);
        }
    }
    if (result != 0) {
        xfs_inode_release(inode);
        return result;
    }
    data->mount = original->mount;
    data->inode = inode;
    file->private_data = data.release();
    file->fops = get_xfs_fops();
    file->fs_type = FSType::XFS;
    file->fd = -1;
    file->refcount = 1;
    file->open_flags = O_NO_CACHE;
    *output = file.release();
    return 0;
}

auto xfs_stat_file_native(File* file, ker::vfs::Stat* out) -> int {
    if (file == nullptr || out == nullptr || file->fops != get_xfs_fops()) return -EINVAL;
    auto* data = static_cast<XfsFileData*>(file->private_data);
    if (data == nullptr || data->inode == nullptr || data->mount == nullptr) return -EBADF;
    ker::mod::sys::MutexGuard io_guard(data->inode->io_lock);
    XfsMetadataGuard metadata_guard(data->mount, true, WOS_PERF_CALLSITE());
    fill_stat(data->inode, out);
    return 0;
}

auto xfs_fstat(File* f, ker::vfs::Stat* statbuf) -> int {
    if (f == nullptr || statbuf == nullptr) {
        return -EINVAL;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }

    ker::mod::sys::MutexGuard guard(xfd->inode->io_lock);
    fill_stat(xfd->inode, statbuf);
    return 0;
}

auto xfs_coherency_snapshot(File* file, coherency::Stamp& output) -> bool {
    if (file == nullptr || file->fops != get_xfs_fops()) return false;
    auto* data = static_cast<XfsFileData*>(file->private_data);
    return data != nullptr && data->inode != nullptr && data->inode->coherency.snapshot(output);
}

auto xfs_coherency_identity(File* file) -> uint64_t {
    if (file == nullptr || file->fops != get_xfs_fops()) return 0;
    auto* data = static_cast<XfsFileData*>(file->private_data);
    return data != nullptr && data->inode != nullptr ? data->inode->coherency.identity() : 0;
}

namespace {

constexpr size_t PUBLIC_XATTR_NAME_MAX = 255;
constexpr size_t PUBLIC_XATTR_SIZE_MAX = 65536;
constexpr int PUBLIC_XATTR_CREATE = 1;
constexpr int PUBLIC_XATTR_REPLACE = 2;
constexpr int XATTR_MISSING = -61;  // ENODATA / ENOATTR

using PublicXattrNamespace = ker::mod::security::FileXattrNamespace;

struct PublicXattrName {
    const uint8_t* suffix{};
    uint16_t suffix_len{};
    uint8_t disk_flags{};
    PublicXattrNamespace name_space{};
};

auto parse_public_xattr_name(const char* name, PublicXattrName* out) -> int {
    if (name == nullptr || out == nullptr) {
        return -EINVAL;
    }
    size_t const NAME_LEN = std::strlen(name);
    if (NAME_LEN == 0 || NAME_LEN > PUBLIC_XATTR_NAME_MAX) {
        return -ERANGE;
    }

    struct NamespacePrefix {
        const char* text;
        size_t length;
        uint8_t disk_flags;
        PublicXattrNamespace name_space;
    };
    constexpr std::array<NamespacePrefix, 3> PREFIXES{{
        {.text = "user.", .length = 5, .disk_flags = 0, .name_space = PublicXattrNamespace::USER},
        {.text = "trusted.", .length = 8, .disk_flags = XFS_ATTR_ROOT, .name_space = PublicXattrNamespace::TRUSTED},
        {.text = "security.", .length = 9, .disk_flags = XFS_ATTR_SECURE, .name_space = PublicXattrNamespace::SECURITY},
    }};
    for (auto const& prefix : PREFIXES) {
        if (NAME_LEN <= prefix.length || std::memcmp(name, prefix.text, prefix.length) != 0) {
            continue;
        }
        size_t const SUFFIX_LEN = NAME_LEN - prefix.length;
        if (SUFFIX_LEN > UINT8_MAX) {
            return -ERANGE;
        }
        *out = {.suffix = reinterpret_cast<const uint8_t*>(name + prefix.length),
                .suffix_len = static_cast<uint16_t>(SUFFIX_LEN),
                .disk_flags = prefix.disk_flags,
                .name_space = prefix.name_space};
        return 0;
    }
    return -EOPNOTSUPP;
}

auto check_public_xattr_permission(const ker::mod::security::Snapshot& credentials, const XfsInode* ip, PublicXattrNamespace name_space,
                                   bool write) -> int {
    if (ip == nullptr) return -EINVAL;
    return ker::mod::security::authorize_file_xattr(credentials, {ip->uid, ip->gid, ip->mode}, name_space, write);
}

auto lookup_xattr_path_inode(const char* fs_path, XfsMountContext* ctx, size_t known_fs_path_len) -> XfsInode* {
    XfsInode* ip = nullptr;
    int const LOOKUP_RET = xfs_lookup_with_cached_parent(fs_path, ctx, &ip, known_fs_path_len);
    if (LOOKUP_RET == -EAGAIN || LOOKUP_RET == -EINVAL) {
        ip = walk_path(ctx, fs_path);
    }
    return ip;
}

auto set_public_xattr_locked(const ker::mod::security::Snapshot& credentials, XfsMountContext* ctx, XfsInode* ip,
                             const PublicXattrName& parsed, const void* value, size_t size, int flags) -> int {
    if (ctx == nullptr || ip == nullptr || (size != 0 && value == nullptr)) {
        return -EINVAL;
    }
    if (size > PUBLIC_XATTR_SIZE_MAX) {
        return -E2BIG;
    }
    if ((flags & ~(PUBLIC_XATTR_CREATE | PUBLIC_XATTR_REPLACE)) != 0 ||
        (flags & (PUBLIC_XATTR_CREATE | PUBLIC_XATTR_REPLACE)) == (PUBLIC_XATTR_CREATE | PUBLIC_XATTR_REPLACE)) {
        return -EINVAL;
    }
    if (int const PERM_RET = check_public_xattr_permission(credentials, ip, parsed.name_space, true); PERM_RET < 0) {
        return PERM_RET;
    }

    int const EXISTING = xfs_attr_get(ip, parsed.suffix, parsed.suffix_len, parsed.disk_flags, nullptr, 0);
    if ((flags & PUBLIC_XATTR_CREATE) != 0 && EXISTING >= 0) {
        return -EEXIST;
    }
    if ((flags & PUBLIC_XATTR_REPLACE) != 0 && EXISTING == XATTR_MISSING) {
        return XATTR_MISSING;
    }
    if (EXISTING < 0 && EXISTING != XATTR_MISSING) {
        return EXISTING;
    }

    auto* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) {
        return -ENOMEM;
    }
    int const RET = xfs_attr_set(ip, tp, parsed.suffix, parsed.suffix_len, static_cast<const uint8_t*>(value), static_cast<uint32_t>(size),
                                 parsed.disk_flags);
    if (RET < 0) {
        xfs_trans_cancel(tp);
        return RET;
    }
    ip->ctime = xfs_current_timestamp(ip);
    ip->dirty = true;
    xfs_trans_log_inode(tp, ip);
    return xfs_trans_commit(tp) == 0 ? 0 : -EIO;
}

auto get_public_xattr_locked(const ker::mod::security::Snapshot& credentials, XfsInode* ip, const PublicXattrName& parsed, void* value,
                             size_t size) -> ssize_t {
    if (ip == nullptr || size > PUBLIC_XATTR_SIZE_MAX || (size != 0 && value == nullptr)) {
        return -EINVAL;
    }
    if (int const PERM_RET = check_public_xattr_permission(credentials, ip, parsed.name_space, false); PERM_RET < 0) {
        return PERM_RET;
    }
    return xfs_attr_get(ip, parsed.suffix, parsed.suffix_len, parsed.disk_flags, value, static_cast<uint32_t>(size));
}

struct PublicXattrListContext {
    char* list{};
    size_t capacity{};
    size_t used{};
    bool privileged{};
};

auto append_public_xattr_name(const XfsAttrEntry* entry, void* private_data) -> int {
    if (entry == nullptr || private_data == nullptr || entry->name == nullptr || entry->namelen == 0 ||
        std::memchr(entry->name, '\0', entry->namelen) != nullptr || (entry->flags & (XFS_ATTR_PARENT | XFS_ATTR_INCOMPLETE)) != 0) {
        return 0;
    }
    auto* context = static_cast<PublicXattrListContext*>(private_data);
    uint8_t const NAMESPACE = entry->flags & XFS_ATTR_NSP_ONDISK_MASK;
    const char* prefix = nullptr;
    size_t prefix_len = 0;
    if (NAMESPACE == 0) {
        prefix = "user.";
        prefix_len = 5;
    } else if (NAMESPACE == XFS_ATTR_ROOT && context->privileged) {
        prefix = "trusted.";
        prefix_len = 8;
    } else if (NAMESPACE == XFS_ATTR_SECURE && context->privileged) {
        prefix = "security.";
        prefix_len = 9;
    } else {
        return 0;
    }

    size_t const ENTRY_SIZE = prefix_len + entry->namelen + 1;
    if (ENTRY_SIZE > PUBLIC_XATTR_SIZE_MAX - context->used) {
        return -E2BIG;
    }
    if (context->list != nullptr) {
        if (ENTRY_SIZE > context->capacity - context->used) {
            return -ERANGE;
        }
        char* destination = context->list + context->used;
        std::memcpy(destination, prefix, prefix_len);
        std::memcpy(destination + prefix_len, entry->name, entry->namelen);
        destination[prefix_len + entry->namelen] = '\0';
    }
    context->used += ENTRY_SIZE;
    return 0;
}

auto list_public_xattrs_locked(const ker::mod::security::Snapshot& credentials, XfsInode* ip, char* list, size_t size) -> ssize_t {
    if (ip == nullptr || size > PUBLIC_XATTR_SIZE_MAX || (size != 0 && list == nullptr)) {
        return -EINVAL;
    }
    if (int const PERM_RET = ker::mod::security::authorize_file_access(credentials, {ip->uid, ip->gid, ip->mode}, 4); PERM_RET < 0) {
        return PERM_RET;
    }
    PublicXattrListContext context{
        .list = list, .capacity = size, .used = 0, .privileged = credentials.allows(ker::mod::security::Authority::FILE_OWNER)};
    int const RET = xfs_attr_list(ip, append_public_xattr_name, &context);
    return RET < 0 ? RET : static_cast<ssize_t>(context.used);
}

auto remove_public_xattr_locked(const ker::mod::security::Snapshot& credentials, XfsMountContext* ctx, XfsInode* ip,
                                const PublicXattrName& parsed) -> int {
    if (ctx == nullptr || ip == nullptr) {
        return -EINVAL;
    }
    if (int const PERM_RET = check_public_xattr_permission(credentials, ip, parsed.name_space, true); PERM_RET < 0) {
        return PERM_RET;
    }
    auto* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) {
        return -ENOMEM;
    }
    int const RET = xfs_attr_remove(ip, tp, parsed.suffix, parsed.suffix_len, parsed.disk_flags);
    if (RET < 0) {
        xfs_trans_cancel(tp);
        return RET;
    }
    ip->ctime = xfs_current_timestamp(ip);
    ip->dirty = true;
    xfs_trans_log_inode(tp, ip);
    return xfs_trans_commit(tp) == 0 ? 0 : -EIO;
}

}  // namespace

auto xfs_setxattr_path(const ker::mod::security::Snapshot& credentials, const char* fs_path, const char* name, const void* value,
                       size_t size, int flags, XfsMountContext* ctx, size_t known_fs_path_len) -> int {
    if (fs_path == nullptr || ctx == nullptr) return -EINVAL;
    if (ctx->read_only) return -EROFS;
    PublicXattrName parsed{};
    if (int const RET = parse_public_xattr_name(name, &parsed); RET < 0) return RET;
    XfsLookupHandle lookup;
    int const RET = xfs_acquire_lookup(credentials, fs_path, ctx, &lookup, known_fs_path_len);
    return RET != 0 ? RET : xfs_setxattr_lookup(credentials, lookup, name, value, size, flags);
}

auto xfs_fsetxattr(const ker::mod::security::Snapshot& credentials, File* f, const char* name, const void* value, size_t size, int flags)
    -> int {
    auto* xfd = f != nullptr ? static_cast<XfsFileData*>(f->private_data) : nullptr;
    if (xfd == nullptr || xfd->inode == nullptr || xfd->mount == nullptr) {
        return -EBADF;
    }
    if (xfd->mount->read_only) {
        return -EROFS;
    }
    PublicXattrName parsed{};
    if (int const RET = parse_public_xattr_name(name, &parsed); RET < 0) {
        return RET;
    }
    ker::mod::sys::MutexGuard io_guard(xfd->inode->io_lock);
    XfsMetadataGuard metadata_guard(xfd->mount, true, WOS_PERF_CALLSITE());
    return set_public_xattr_locked(credentials, xfd->mount, xfd->inode, parsed, value, size, flags);
}

auto xfs_getxattr_path(const ker::mod::security::Snapshot& credentials, const char* fs_path, const char* name, void* value, size_t size,
                       XfsMountContext* ctx, size_t known_fs_path_len) -> ssize_t {
    if (fs_path == nullptr || ctx == nullptr) {
        return -EINVAL;
    }
    PublicXattrName parsed{};
    if (int const RET = parse_public_xattr_name(name, &parsed); RET < 0) {
        return RET;
    }
    XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());
    XfsInode* ip = lookup_xattr_path_inode(fs_path, ctx, known_fs_path_len);
    if (ip == nullptr) {
        return -ENOENT;
    }
    ssize_t const RET = get_public_xattr_locked(credentials, ip, parsed, value, size);
    xfs_inode_release(ip);
    return RET;
}

auto xfs_fgetxattr(const ker::mod::security::Snapshot& credentials, File* f, const char* name, void* value, size_t size) -> ssize_t {
    auto* xfd = f != nullptr ? static_cast<XfsFileData*>(f->private_data) : nullptr;
    if (xfd == nullptr || xfd->inode == nullptr || xfd->mount == nullptr) {
        return -EBADF;
    }
    PublicXattrName parsed{};
    if (int const RET = parse_public_xattr_name(name, &parsed); RET < 0) {
        return RET;
    }
    XfsMetadataGuard metadata_guard(xfd->mount, true, WOS_PERF_CALLSITE());
    return get_public_xattr_locked(credentials, xfd->inode, parsed, value, size);
}

auto xfs_listxattr_path(const ker::mod::security::Snapshot& credentials, const char* fs_path, char* list, size_t size, XfsMountContext* ctx,
                        size_t known_fs_path_len) -> ssize_t {
    if (fs_path == nullptr || ctx == nullptr) {
        return -EINVAL;
    }
    XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());
    XfsInode* ip = lookup_xattr_path_inode(fs_path, ctx, known_fs_path_len);
    if (ip == nullptr) {
        return -ENOENT;
    }
    ssize_t const RET = list_public_xattrs_locked(credentials, ip, list, size);
    xfs_inode_release(ip);
    return RET;
}

auto xfs_flistxattr(const ker::mod::security::Snapshot& credentials, File* f, char* list, size_t size) -> ssize_t {
    auto* xfd = f != nullptr ? static_cast<XfsFileData*>(f->private_data) : nullptr;
    if (xfd == nullptr || xfd->inode == nullptr || xfd->mount == nullptr) {
        return -EBADF;
    }
    XfsMetadataGuard metadata_guard(xfd->mount, true, WOS_PERF_CALLSITE());
    return list_public_xattrs_locked(credentials, xfd->inode, list, size);
}

auto xfs_removexattr_path(const ker::mod::security::Snapshot& credentials, const char* fs_path, const char* name, XfsMountContext* ctx,
                          size_t known_fs_path_len) -> int {
    if (fs_path == nullptr || ctx == nullptr) return -EINVAL;
    if (ctx->read_only) return -EROFS;
    PublicXattrName parsed{};
    if (int const RET = parse_public_xattr_name(name, &parsed); RET < 0) return RET;
    XfsLookupHandle lookup;
    int const RET = xfs_acquire_lookup(credentials, fs_path, ctx, &lookup, known_fs_path_len);
    return RET != 0 ? RET : xfs_removexattr_lookup(credentials, lookup, name);
}

auto xfs_fremovexattr(const ker::mod::security::Snapshot& credentials, File* f, const char* name) -> int {
    auto* xfd = f != nullptr ? static_cast<XfsFileData*>(f->private_data) : nullptr;
    if (xfd == nullptr || xfd->inode == nullptr || xfd->mount == nullptr) {
        return -EBADF;
    }
    if (xfd->mount->read_only) {
        return -EROFS;
    }
    PublicXattrName parsed{};
    if (int const RET = parse_public_xattr_name(name, &parsed); RET < 0) {
        return RET;
    }
    ker::mod::sys::MutexGuard io_guard(xfd->inode->io_lock);
    XfsMetadataGuard metadata_guard(xfd->mount, true, WOS_PERF_CALLSITE());
    return remove_public_xattr_locked(credentials, xfd->mount, xfd->inode, parsed);
}

#ifdef WOS_SELFTEST
auto xfs_selftest_open_snapshot_denials() -> bool {
    auto const bootstrap = ker::mod::security::Snapshot::bootstrap_root();
    auto caller = bootstrap;
    if (caller.prepare({.change = ker::mod::security::Change::SET_UID, .real = 1000}, caller) != 0) return false;
    XfsMountContext mount{};
    mount.inode_size = 512;
    mount.feat_incompat = XFS_SB_FEAT_INCOMPAT_FTYPE;
    mount.root_ino = 100;
    auto* data = new (std::nothrow) uint8_t[32]{};
    auto* root = new (std::nothrow) XfsInode{};
    if (data == nullptr || root == nullptr) {
        delete[] data;
        delete root;
        return false;
    }
    root->ino = mount.root_ino;
    root->mount = &mount;
    root->mode = 0040755;
    root->nlink = 2;
    root->ctime = 123;
    root->data_fork.format = XFS_DINODE_FMT_LOCAL;
    root->data_fork.local.data = data;
    root->data_fork.local.size = xfs_dir2_sf_hdr_size(reinterpret_cast<XfsDir2SfHdr*>(data));
    root->size = root->data_fork.local.size;
    if (xfs_inode_cache_new(root) != 0) {
        delete[] data;
        delete root;
        return false;
    }
    auto denied = [](File* file, int result, int expected) {
        if (file != nullptr) {
            xfs_vfs_close(file);
            delete file;
            return false;
        }
        return result == expected;
    };
    bool ok = true;
    {
        XfsLookupHandle missing;
        XfsLookupHandle directory;
        ok = xfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), "missing", &mount, &missing) == 0 &&
             xfs_acquire_lookup(ker::mod::security::Snapshot::bootstrap_root(), "/", &mount, &directory) == 0;
        auto const GENERATION = root->dir_generation;
        auto const SIZE = root->size;
        int result = 123;
        File* file = xfs_open_path(caller, "missing", ker::vfs::O_CREAT | 2, 0000, &mount, &result);
        ok = denied(file, result, -EACCES) && ok;
        file = xfs_open_lookup(caller, missing, ker::vfs::O_CREAT | 2, 0000, false, &result);
        ok = denied(file, result, -EACCES) && ok;
        file = xfs_open_lookup(caller, directory, ker::vfs::O_TRUNC | 1, 0, false, &result);
        ok = denied(file, result, -EISDIR) && ok;
        root->mode = 0040700;
        file = xfs_open_path(caller, "missing", ker::vfs::O_CREAT | 2, 0000, &mount, &result);
        ok = denied(file, result, -EACCES) && ok;
        file = xfs_open_lookup(caller, missing, ker::vfs::O_CREAT | 2, 0000, false, &result);
        ok = denied(file, result, -EACCES) && ok;
        file = xfs_open_lookup(caller, directory, 0, 0, false, &result);
        ok = denied(file, result, -EACCES) && ok;
        ok = !root->dirty && root->ctime == 123 && root->size == SIZE && root->dir_generation == GENERATION && ok;
        ++root->dir_generation;
        file = xfs_open_lookup(bootstrap, missing, ker::vfs::O_CREAT | 2, 0000, false, &result);
        ok = denied(file, result, -EAGAIN) && ok;
    }
    xfs_inode_release(root);
    xfs_icache_purge(&mount);
    xfs_parent_path_cache_purge_all_for_mount(&mount);
    xfs_dentry_cache_purge_mount(&mount);
    xfs_path_inode_cache_bump_generation();
    return ok;
}

auto xfs_selftest_xattr_snapshot_denials() -> bool {
    auto const root = ker::mod::security::Snapshot::bootstrap_root();
    auto caller = root;
    if (caller.prepare({.change = ker::mod::security::Change::SET_UID, .real = 1000}, caller) != 0) return false;
    XfsMountContext mount{};
    XfsInode inode{};
    inode.mount = &mount;
    inode.mode = 0100600;
    inode.uid = 0;
    inode.gid = 0;
    inode.ctime = 123;
    XfsFileData data{};
    data.inode = &inode;
    data.mount = &mount;
    File file{};
    file.private_data = &data;
    std::array<char, 64> output{};
    output.fill('!');
    bool const DENIED = xfs_fsetxattr(caller, &file, "user.denied", "x", 1, 0) == -EACCES &&
                        xfs_fremovexattr(caller, &file, "user.denied") == -EACCES &&
                        xfs_fgetxattr(caller, &file, "user.denied", output.data(), output.size()) == -EACCES &&
                        xfs_flistxattr(caller, &file, output.data(), output.size()) == -EACCES;
    bool const PRIVILEGED_DENIED = xfs_fsetxattr(caller, &file, "trusted.denied", "x", 1, 0) == -EPERM &&
                                   xfs_fgetxattr(caller, &file, "security.denied", output.data(), output.size()) == -EPERM;
    bool const UNCHANGED = !inode.dirty && !inode.has_attr_fork && inode.ctime == 123 &&
                           std::all_of(output.begin(), output.end(), [](char value) { return value == '!'; });
    bool const ROOT_READS = xfs_fgetxattr(root, &file, "user.missing", output.data(), output.size()) == XATTR_MISSING &&
                            xfs_flistxattr(root, &file, output.data(), output.size()) == 0;
    file.private_data = nullptr;
    return DENIED && PRIVILEGED_DENIED && UNCHANGED && ROOT_READS;
}

auto xfs_selftest_attr_fork_format(const char* fs_path, XfsMountContext* ctx, uint8_t* format, uint16_t* extent_count) -> bool {
    if (fs_path == nullptr || ctx == nullptr || format == nullptr || extent_count == nullptr) {
        return false;
    }
    XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());
    XfsInode* ip = lookup_xattr_path_inode(fs_path, ctx, UNKNOWN_XFS_PATH_LEN);
    if (ip == nullptr) {
        return false;
    }
    *format = ip->has_attr_fork ? ip->attr_fork.format : XFS_DINODE_FMT_DEV;
    *extent_count = ip->anextents;
    xfs_inode_release(ip);
    return true;
}

auto xfs_selftest_cancel_setxattr_path(const char* fs_path, const char* name, const void* value, size_t size, XfsMountContext* ctx) -> int {
    if (fs_path == nullptr || ctx == nullptr || (size != 0 && value == nullptr) || size > PUBLIC_XATTR_SIZE_MAX) {
        return -EINVAL;
    }
    PublicXattrName parsed{};
    int rc = parse_public_xattr_name(name, &parsed);
    if (rc != 0) {
        return rc;
    }
    XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());
    XfsInode* ip = lookup_xattr_path_inode(fs_path, ctx, UNKNOWN_XFS_PATH_LEN);
    if (ip == nullptr) {
        return -ENOENT;
    }
    XfsTransaction* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) {
        xfs_inode_release(ip);
        return -ENOMEM;
    }
    rc = xfs_attr_set(ip, tp, parsed.suffix, parsed.suffix_len, static_cast<const uint8_t*>(value), static_cast<uint32_t>(size),
                      parsed.disk_flags);
    xfs_trans_cancel(tp);
    xfs_inode_release(ip);
    return rc;
}
#endif

auto xfs_snapshot_file_stat(File* f, ker::vfs::Stat* statbuf) -> int {
    if (f == nullptr || statbuf == nullptr) {
        return -EINVAL;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }

    ker::mod::sys::MutexGuard guard(xfd->inode->io_lock);
    fill_stat(xfd->inode, statbuf);
    return 0;
}

auto xfs_consume_recent_write_stat(File* f, ker::vfs::Stat* statbuf) -> bool {
    if (f == nullptr || statbuf == nullptr) {
        return false;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr) {
        return false;
    }

    uint64_t const IRQF = xfd->recent_write_stat_lock.lock_irqsave();
    if (!xfd->recent_write_stat_valid) {
        xfd->recent_write_stat_lock.unlock_irqrestore(IRQF);
        return false;
    }
    *statbuf = xfd->recent_write_stat;
    xfd->recent_write_stat_valid = false;
    xfd->recent_write_stat_lock.unlock_irqrestore(IRQF);
    return true;
}

#ifdef WOS_SELFTEST
auto xfs_selftest_cached_parent_missing_lookup_stays_negative() -> bool {
    XfsMountContext mount{};
    mount.inode_size = 512;
    mount.feat_incompat = XFS_SB_FEAT_INCOMPAT_FTYPE;
    xfs_parent_path_cache_purge_all_for_mount(&mount);

    constexpr xfs_ino_t PARENT_INO = 100;
    auto* data = new (std::nothrow) uint8_t[sizeof(XfsDir2SfHdr)];
    if (data == nullptr) {
        return false;
    }
    std::memset(data, 0, sizeof(XfsDir2SfHdr));
    auto* hdr = reinterpret_cast<XfsDir2SfHdr*>(data);
    hdr->count = 0;
    hdr->i8count = 0;
    hdr->parent.at(3) = 7;

    auto* parent = new (std::nothrow) XfsInode{};
    if (parent == nullptr) {
        delete[] data;
        return false;
    }
    parent->ino = PARENT_INO;
    parent->mount = &mount;
    parent->mode = 0040755;
    parent->nlink = 2;
    parent->data_fork.format = XFS_DINODE_FMT_LOCAL;
    parent->data_fork.local.data = data;
    parent->data_fork.local.size = 6;
    parent->size = parent->data_fork.local.size;

    int const CACHE_RET = xfs_inode_cache_new(parent);
    if (CACHE_RET != 0) {
        delete parent;
        delete[] data;
        return false;
    }

    constexpr const char* PARENT_PATH = "cached-parent";
    xfs_parent_path_cache_store(&mount, PARENT_PATH, std::strlen(PARENT_PATH), PARENT_INO);

    XfsInode* result = nullptr;
    int const LOOKUP_RET = xfs_lookup_with_cached_parent("cached-parent/missing", &mount, &result);
    bool const OK = LOOKUP_RET == -ENOENT && result == nullptr;

    if (result != nullptr) {
        xfs_inode_release(result);
    }
    XfsInode* create_parent = nullptr;
    const char* create_name = nullptr;
    uint16_t create_name_len = 0;
    bool create_parent_cache_hit = false;
    int const CREATE_PARENT_RET = xfs_find_parent_and_name("cached-parent/create-missing", &mount, &create_parent, &create_name,
                                                           &create_name_len, &create_parent_cache_hit);
    XfsDirEntry create_existing{};
    bool const CREATE_OK = CREATE_PARENT_RET == 0 && create_parent_cache_hit &&
                           xfs_dir_lookup(create_parent, create_name, create_name_len, &create_existing) == -ENOENT;
    if (create_parent != nullptr) {
        xfs_inode_release(create_parent);
    }

    xfs_inode_release(parent);
    xfs_icache_purge(&mount);
    xfs_parent_path_cache_purge_all_for_mount(&mount);
    return OK && CREATE_OK;
}
#endif

auto xfs_file_mount_context(File* f) -> XfsMountContext* {
    if (f == nullptr) {
        return nullptr;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    return xfd != nullptr ? xfd->mount : nullptr;
}

auto xfs_file_regular_identity(File* f, XfsMountContext** mount_out, uint64_t* ino_out) -> bool {
    if (mount_out != nullptr) {
        *mount_out = nullptr;
    }
    if (ino_out != nullptr) {
        *ino_out = 0;
    }
    if (f == nullptr) {
        return false;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->mount == nullptr || xfd->inode == nullptr || !xfs_inode_isreg(xfd->inode)) {
        return false;
    }
    if (mount_out != nullptr) {
        *mount_out = xfd->mount;
    }
    if (ino_out != nullptr) {
        *ino_out = xfd->inode->ino;
    }
    return true;
}

auto xfs_statvfs(XfsMountContext* ctx, ker::vfs::Statvfs* buf) -> int {
    if (ctx == nullptr || buf == nullptr) {
        return -EINVAL;
    }

    std::memset(buf, 0, sizeof(ker::vfs::Statvfs));

    uint64_t free_blocks = 0;
    uint64_t total_inodes = 0;
    uint64_t free_inodes = 0;
    for (xfs_agnumber_t i = 0; i < ctx->ag_count; i++) {
        free_blocks += ctx->per_ag[i].agf_freeblks;
        total_inodes += ctx->per_ag[i].agi_count;
        free_inodes += ctx->per_ag[i].agi_freecount;
    }

    // Derive a 64-bit fsid by XOR-folding the 128-bit UUID
    uint64_t fsid_lo = 0;
    uint64_t fsid_hi = 0;
    for (size_t i = 0; i < 8; i++) {
        fsid_lo |= static_cast<uint64_t>(ctx->uuid.b.at(i)) << (i * 8);
        fsid_hi |= static_cast<uint64_t>(ctx->uuid.b.at(i + 8)) << (i * 8);
    }

    buf->f_bsize = ctx->block_size;
    buf->f_frsize = ctx->block_size;
    buf->f_blocks = ctx->total_blocks;
    buf->f_bfree = free_blocks;
    buf->f_bavail = free_blocks;
    buf->f_files = total_inodes;
    buf->f_ffree = free_inodes;
    buf->f_favail = free_inodes;
    buf->f_fsid = fsid_lo ^ fsid_hi;
    buf->f_flag = ctx->read_only ? ker::vfs::ST_RDONLY : 0;
    buf->f_namemax = 255;
    return 0;
}

auto xfs_search_authority_generation() -> uint64_t { return xfs_search_authority_generation_value.load(std::memory_order_acquire); }

// ============================================================================
// chmod
// ============================================================================

namespace {
// Caller holds inode io_lock followed by mount metadata_lock. Deep transaction
// undo may replace fork allocations, so exclude readers as well as mutators.
auto commit_file_identity_locked(XfsInode* ip, const ker::mod::security::FileIdentity& prepared, Stat* statbuf) -> int {
    auto* tp = xfs_trans_alloc(ip->mount);
    if (tp == nullptr) return -ENOMEM;
    int const CAPTURE = xfs_trans_capture_inode(tp, ip);
    if (CAPTURE != 0) {
        xfs_trans_cancel(tp);
        return CAPTURE;
    }
    bool const SEARCH_AUTHORITY_CHANGED =
        xfs_inode_isdir(ip) && (ip->mode != prepared.mode || ip->uid != prepared.uid || ip->gid != prepared.gid);
    ip->mode = static_cast<uint16_t>(prepared.mode);
    ip->uid = prepared.uid;
    ip->gid = prepared.gid;
    ip->ctime = xfs_current_timestamp(ip);
    ip->dirty = true;
    xfs_trans_log_inode(tp, ip);
    int const RET = xfs_trans_commit(tp);
    // Failed commits restore inode authority. On success invalidate searched
    // ancestors before metadata_lock publishes the change, even when the
    // retained VFS lookup selected its final object on another mount.
    if (RET == 0 && SEARCH_AUTHORITY_CHANGED) xfs_search_authority_generation_value.fetch_add(1, std::memory_order_release);
    if (RET == 0 && statbuf != nullptr) fill_stat(ip, statbuf);
    return RET == 0 ? 0 : -EIO;
}

auto chmod_inode_locked(XfsInode* ip, const ker::mod::security::Snapshot& credentials, int mode, Stat* statbuf) -> int {
    ker::mod::security::FileIdentity prepared;
    int const RET =
        ker::mod::security::prepare_file_chmod(credentials, {ip->uid, ip->gid, ip->mode}, static_cast<uint32_t>(mode), prepared);
    return RET != 0 ? RET : commit_file_identity_locked(ip, prepared, statbuf);
}

auto chown_inode_locked(XfsInode* ip, const ker::mod::security::Snapshot& credentials, uint32_t uid, uint32_t gid, Stat* statbuf) -> int {
    ker::mod::security::FileIdentity prepared;
    int const RET = ker::mod::security::prepare_file_chown(credentials, {ip->uid, ip->gid, ip->mode}, uid, gid, prepared);
    return RET != 0 ? RET : commit_file_identity_locked(ip, prepared, statbuf);
}
}  // namespace

auto xfs_chmod_path(const char* fs_path, const ker::mod::security::Snapshot& credentials, int mode, XfsMountContext* ctx, Stat* statbuf,
                    size_t known_fs_path_len) -> int {
    if (fs_path == nullptr || ctx == nullptr) return -EINVAL;
    if (ctx->read_only) return -EROFS;
    XfsLookupHandle lookup;
    int const RET = xfs_acquire_lookup(credentials, fs_path, ctx, &lookup, known_fs_path_len);
    return RET != 0 ? RET : xfs_chmod_lookup(lookup, credentials, mode, statbuf);
}

auto xfs_chown_path(const char* fs_path, const ker::mod::security::Snapshot& credentials, uint32_t uid, uint32_t gid, XfsMountContext* ctx,
                    Stat* statbuf, size_t known_fs_path_len) -> int {
    if (fs_path == nullptr || ctx == nullptr) return -EINVAL;
    if (ctx->read_only) return -EROFS;
    XfsLookupHandle lookup;
    int const RET = xfs_acquire_lookup(credentials, fs_path, ctx, &lookup, known_fs_path_len);
    return RET != 0 ? RET : xfs_chown_lookup(lookup, credentials, uid, gid, statbuf);
}

auto xfs_fchmod(File* f, const ker::mod::security::Snapshot& credentials, int mode, Stat* statbuf) -> int {
    if (f == nullptr) return -EBADF;
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr || xfd->mount == nullptr) return -EBADF;
    if (xfd->mount->read_only) return -EROFS;
    ker::mod::sys::MutexGuard io_guard(xfd->inode->io_lock);
    XfsMetadataGuard metadata_guard(xfd->mount, true, WOS_PERF_CALLSITE());
    return chmod_inode_locked(xfd->inode, credentials, mode, statbuf);
}

auto xfs_fchown(File* f, const ker::mod::security::Snapshot& credentials, uint32_t uid, uint32_t gid, Stat* statbuf) -> int {
    if (f == nullptr) return -EBADF;
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr || xfd->mount == nullptr) return -EBADF;
    if (xfd->mount->read_only) return -EROFS;
    ker::mod::sys::MutexGuard io_guard(xfd->inode->io_lock);
    XfsMetadataGuard metadata_guard(xfd->mount, true, WOS_PERF_CALLSITE());
    return chown_inode_locked(xfd->inode, credentials, uid, gid, statbuf);
}

namespace {

auto xfs_set_inode_times(XfsMountContext* ctx, XfsInode* ip, const ker::mod::security::Snapshot& credentials, const TimestampUpdate& times,
                         Stat* statbuf) -> int {
    if (ctx == nullptr || ip == nullptr) return -EINVAL;
    int const POLICY = ker::mod::security::authorize_file_timestamps(credentials, {ip->uid, ip->gid, ip->mode}, times.intent());
    if (POLICY != 0) return POLICY;
    if (!times.changed()) {
        if (statbuf != nullptr) fill_stat(ip, statbuf);
        return 0;
    }
    bool const BIGTIME = (ip->flags2 & XFS_DIFLAG2_BIGTIME) != 0;
    uint64_t encoded_atime = 0;
    uint64_t encoded_mtime = 0;
    if (times.set_atime()) {
        if (int const RET = xfs_encode_timespec_timestamp(times.atime(), BIGTIME, &encoded_atime); RET < 0) return RET;
    }
    if (times.set_mtime()) {
        if (int const RET = xfs_encode_timespec_timestamp(times.mtime(), BIGTIME, &encoded_mtime); RET < 0) return RET;
    }
    auto* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) return -ENOMEM;
    int const CAPTURE = xfs_trans_capture_inode(tp, ip);
    if (CAPTURE != 0) {
        xfs_trans_cancel(tp);
        return CAPTURE;
    }
    if (times.set_atime()) ip->atime = encoded_atime;
    if (times.set_mtime()) ip->mtime = encoded_mtime;
    ip->ctime = xfs_current_timestamp(ip);
    ip->dirty = true;
    if (!xfs_inode_isdir(ip) && xfs_inode_has_eof_prealloc(ip)) {
        int const TRIM_RET = xfs_inode_trim_data_to_size(ip, tp, ip->size);
        if (TRIM_RET != 0) {
            xfs_trans_cancel(tp);
            return TRIM_RET;
        }
    }
    xfs_trans_log_inode(tp, ip);
    int const RET = xfs_trans_commit(tp);
    if (RET == 0 && statbuf != nullptr) fill_stat(ip, statbuf);
    return RET == 0 ? 0 : -EIO;
}

}  // namespace

auto xfs_set_times_path(const char* fs_path, const ker::mod::security::Snapshot& credentials, const TimestampUpdate& times,
                        XfsMountContext* ctx, Stat* statbuf, size_t known_fs_path_len) -> int {
    if (fs_path == nullptr || ctx == nullptr) return -EINVAL;
    if (ctx->read_only) return -EROFS;
    XfsLookupHandle lookup;
    int const RET = xfs_acquire_lookup(credentials, fs_path, ctx, &lookup, known_fs_path_len);
    return RET != 0 ? RET : xfs_utimens_lookup(lookup, credentials, times, statbuf);
}

auto xfs_set_times_file(File* f, const ker::mod::security::Snapshot& credentials, const TimestampUpdate& times, Stat* statbuf) -> int {
    if (f == nullptr) return -EBADF;
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr || xfd->mount == nullptr) return -EBADF;
    if (xfd->mount->read_only) return -EROFS;
    ker::mod::sys::MutexGuard io_guard(xfd->inode->io_lock);
    XfsMetadataGuard metadata_guard(xfd->mount, true, WOS_PERF_CALLSITE());
    return xfs_set_inode_times(xfd->mount, xfd->inode, credentials, times, statbuf);
}

// ============================================================================
// Mkdir
// ============================================================================

auto xfs_mkdir_path(const ker::mod::security::Snapshot& credentials, const char* fs_path, int mode, XfsMountContext* ctx,
                    ker::vfs::Stat* statbuf, size_t known_fs_path_len) -> int {
    if (fs_path == nullptr || ctx == nullptr) {
        return -EINVAL;
    }
    if (ctx->read_only) {
        return -EROFS;
    }

    XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());

    XfsInode* parent_ip = nullptr;
    const char* dirname = nullptr;
    uint16_t dirname_len = 0;
    int parent_ret = xfs_find_parent_and_name(fs_path, ctx, &parent_ip, &dirname, &dirname_len, nullptr, true, known_fs_path_len);
    if (parent_ret != 0) {
        if (parent_ret == -EINVAL) {
            XfsInode* existing = walk_path(ctx, fs_path);
            if (existing != nullptr) {
                bool const IS_DIR = xfs_inode_isdir(existing);
                if (IS_DIR && statbuf != nullptr) {
                    fill_stat(existing, statbuf);
                }
                xfs_inode_release(existing);
                return -EEXIST;
            }
        }
        if (parent_ret == -ENOTDIR) {
            return -ENOTDIR;
        }
        return parent_ret;
    }

    ker::mod::security::FileIdentity identity{};
    int const AUTH = ker::mod::security::prepare_directory_create(credentials, {parent_ip->uid, parent_ip->gid, parent_ip->mode},
                                                                  static_cast<uint32_t>(mode), identity);
    if (AUTH != 0) {
        xfs_inode_release(parent_ip);
        return AUTH;
    }
    XfsDirEntry existing{};
    int const LOOKUP_RET = xfs_dir_lookup(parent_ip, dirname, dirname_len, &existing);
    if (LOOKUP_RET == 0) {
        if (existing.ftype == XFS_DIR3_FT_DIR && statbuf != nullptr) {
            XfsInode* existing_ip = xfs_inode_read_known_allocated(ctx, existing.ino);
            if (existing_ip != nullptr) {
                fill_stat(existing_ip, statbuf);
                xfs_inode_release(existing_ip);
            }
        }
        xfs_inode_release(parent_ip);
        return -EEXIST;
    }
    if (LOOKUP_RET != -ENOENT) {
        xfs_inode_release(parent_ip);
        return LOOKUP_RET;
    }
    if (parent_ip->nlink == UINT32_MAX) {
        xfs_inode_release(parent_ip);
        return -EMLINK;
    }

    // Save before parent_ip is released
    xfs_ino_t const PARENT_INO = parent_ip->ino;

    uint16_t const INODE_MODE = static_cast<uint16_t>(identity.mode);
    bool const USE_I8 = (PARENT_INO > 0xFFFFFFFFULL);
    size_t const INO_BYTES = USE_I8 ? 8 : 4;
    size_t const SF_SIZE = 2 + INO_BYTES;

    auto* new_inode = xfs_inode_alloc_zeroed_object();
    auto* sf_data = new (std::nothrow) uint8_t[SF_SIZE];
    if (new_inode == nullptr || sf_data == nullptr) {
        xfs_inode_free_uncached(new_inode);
        delete[] sf_data;
        xfs_inode_release(parent_ip);
        return -ENOMEM;
    }

    // Allocate new inode
    XfsTransaction* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) {
        delete[] sf_data;
        xfs_inode_free_uncached(new_inode);
        xfs_inode_release(parent_ip);
        return -ENOMEM;
    }

    xfs_ino_t const NEW_INO = xfs_ialloc(ctx, tp, INODE_MODE);
    if (NEW_INO == NULLFSINO) {
        xfs_trans_cancel(tp);
        delete[] sf_data;
        xfs_inode_free_uncached(new_inode);
        xfs_inode_release(parent_ip);
        return -ENOSPC;
    }
    if (xfs_ialloc_conflicts_with_cached_inode(ctx, NEW_INO)) {
        xfs_trans_cancel(tp);
        delete[] sf_data;
        xfs_inode_free_uncached(new_inode);
        xfs_inode_release(parent_ip);
        return -EIO;
    }

    // Build minimal shortform directory: count=0, parent=parent_ino
    sf_data[0] = 0;               // count
    sf_data[1] = USE_I8 ? 1 : 0;  // i8count
    if (USE_I8) {
        for (int i = 7; i >= 0; i--) {
            sf_data[2 + (7 - i)] = static_cast<uint8_t>((PARENT_INO >> (i * 8)) & 0xFF);
        }
    } else {
        auto p32 = static_cast<uint32_t>(PARENT_INO);
        for (int i = 3; i >= 0; i--) {
            sf_data[2 + (3 - i)] = static_cast<uint8_t>((p32 >> (i * 8)) & 0xFF);
        }
    }

    new_inode->ino = NEW_INO;
    new_inode->mount = ctx;
    new_inode->agno = xfs_ino_ag(NEW_INO, ctx->agino_log);
    new_inode->agino = xfs_ag_ino(NEW_INO, ctx->agino_log);
    new_inode->mode = INODE_MODE;
    new_inode->uid = identity.uid;
    new_inode->gid = identity.gid;
    new_inode->size = SF_SIZE;
    new_inode->nlink = 2;  // . and parent ref
    new_inode->data_fork.format = XFS_DINODE_FMT_LOCAL;
    new_inode->data_fork.local.data = sf_data;
    new_inode->data_fork.local.size = SF_SIZE;
    xfs_dir_name_filter_init_empty(new_inode);
    xfs_stamp_new_inode(new_inode);
    xfs_trans_log_inode(tp, new_inode);

    // Add entry in parent
    int rc = xfs_dir_addname(parent_ip, dirname, dirname_len, NEW_INO, XFS_DIR3_FT_DIR, tp, true);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        xfs_inode_free_uncached(new_inode);
        xfs_inode_release(parent_ip);
        return rc;
    }
    // Each child directory contributes its ".." reference to the parent.
    // xfs_dir_addname() captured the parent before mutating its data fork, so
    // this link-count update shares the same rollback snapshot.
    parent_ip->nlink++;
    parent_ip->dirty = true;
    xfs_trans_log_inode(tp, parent_ip);

    rc = xfs_trans_commit(tp);
    xfs_inode_release(parent_ip);
    if (rc != 0) {
        xfs_inode_free_uncached(new_inode);
        return rc;
    }
    // An inode number may have belonged to a previously removed directory.
    // Start the new directory incarnation with a fresh dentry generation so
    // cached child names from that old incarnation cannot become visible.
    xfs_dentry_cache_invalidate_dir(new_inode);
    rc = xfs_inode_cache_new(new_inode);
    if (rc != 0) {
        mod::dbg::log("[xfs] committed mkdir inode %lu could not enter inode cache: %d", static_cast<unsigned long>(NEW_INO), rc);
        xfs_inode_free_uncached(new_inode);
        return rc;
    }
    if (statbuf != nullptr) {
        fill_stat(new_inode, statbuf);
    }
    size_t const DIR_PATH_LEN = xfs_known_path_len(fs_path, known_fs_path_len);
    xfs_path_inode_cache_invalidate_path(ctx, fs_path, DIR_PATH_LEN);
    xfs_path_inode_cache_store(ctx, fs_path, DIR_PATH_LEN, NEW_INO, XFS_DIR3_FT_DIR);
    xfs_parent_path_cache_store(ctx, fs_path, DIR_PATH_LEN, NEW_INO);
    xfs_inode_release_metadata_locked(new_inode);
    return 0;
}

// ============================================================================
// Rmdir
// ============================================================================

namespace {

struct RmdirDirectoryScan {
    XfsInode* directory{};
    int status{};
    bool indexed_entry_found{};
};

auto scan_indexed_directory_entries(const XfsDirEntry* entry, void* ctx) -> int {
    auto* scan = static_cast<RmdirDirectoryScan*>(ctx);
    if (entry->name.at(0) == '.' && (entry->namelen == 1 || (entry->namelen == 2 && entry->name.at(1) == '.'))) {
        return 0;
    }

    int const MEMBERSHIP = xfs_dir_entry_is_indexed(scan->directory, entry);
    if (MEMBERSHIP < 0) {
        scan->status = MEMBERSHIP;
        return 1;
    }
    if (MEMBERSHIP == 0) {
        // A data-only record cannot be reached by pathname lookup. Reclaim it
        // with the directory inode instead of making rmdir fail forever.
        return 0;
    }
    scan->indexed_entry_found = true;
    return 1;
}

}  // anonymous namespace

auto xfs_symlink_path(const ker::mod::security::Snapshot& credentials, const char* target, const char* fs_path, XfsMountContext* ctx,
                      ker::vfs::Stat* statbuf, size_t known_fs_path_len) -> int {
    if (target == nullptr || fs_path == nullptr || ctx == nullptr) {
        return -EINVAL;
    }
    if (ctx->read_only) {
        return -EROFS;
    }
    if (fs_path[0] == '\0' || (fs_path[0] == '/' && fs_path[1] == '\0')) {
        return -EEXIST;
    }

    size_t target_len = 0;
    while (target[target_len] != '\0') {
        ++target_len;
    }

    size_t const INLINE_CAPACITY = ctx->inode_size > XFS_DINODE_SIZE_V3 ? ctx->inode_size - XFS_DINODE_SIZE_V3 : 0;
    if (target_len > INLINE_CAPACITY) {
        return -ENAMETOOLONG;
    }

    XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());

    XfsInode* parent_ip = nullptr;
    const char* name = nullptr;
    uint16_t namelen = 0;
    int rc = xfs_find_parent_and_name(fs_path, ctx, &parent_ip, &name, &namelen, nullptr, true, known_fs_path_len);
    if (rc != 0) {
        return rc;
    }

    ker::mod::security::FileIdentity identity{};
    rc = ker::mod::security::prepare_symlink_create(credentials, {parent_ip->uid, parent_ip->gid, parent_ip->mode}, identity);
    if (rc != 0) {
        xfs_inode_release(parent_ip);
        return rc;
    }
    XfsDirEntry existing{};
    rc = xfs_dir_lookup(parent_ip, name, namelen, &existing);
    if (rc == 0) {
        xfs_inode_release(parent_ip);
        return -EEXIST;
    }
    if (rc != -ENOENT) {
        xfs_inode_release(parent_ip);
        return rc;
    }

    auto* target_data = new (std::nothrow) uint8_t[target_len == 0 ? 1 : target_len];
    auto* new_inode = xfs_inode_alloc_zeroed_object();
    if (target_data == nullptr || new_inode == nullptr) {
        delete[] target_data;
        xfs_inode_free_uncached(new_inode);
        xfs_inode_release(parent_ip);
        return -ENOMEM;
    }
    if (target_len != 0) {
        std::memcpy(target_data, target, target_len);
    }

    XfsTransaction* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) {
        delete[] target_data;
        xfs_inode_free_uncached(new_inode);
        xfs_inode_release(parent_ip);
        return -ENOMEM;
    }

    constexpr uint16_t SYMLINK_MODE = 0120000 | 0777;
    xfs_ino_t const NEW_INO = xfs_ialloc(ctx, tp, SYMLINK_MODE);
    if (NEW_INO == NULLFSINO) {
        xfs_trans_cancel(tp);
        delete[] target_data;
        xfs_inode_free_uncached(new_inode);
        xfs_inode_release(parent_ip);
        return -ENOSPC;
    }
    if (xfs_ialloc_conflicts_with_cached_inode(ctx, NEW_INO)) {
        xfs_trans_cancel(tp);
        delete[] target_data;
        xfs_inode_free_uncached(new_inode);
        xfs_inode_release(parent_ip);
        return -EIO;
    }

    new_inode->ino = NEW_INO;
    new_inode->mount = ctx;
    new_inode->agno = xfs_ino_ag(NEW_INO, ctx->agino_log);
    new_inode->agino = xfs_ag_ino(NEW_INO, ctx->agino_log);
    new_inode->mode = SYMLINK_MODE;
    new_inode->uid = identity.uid;
    new_inode->gid = identity.gid;
    new_inode->size = target_len;
    new_inode->nlink = 1;
    new_inode->data_fork.format = XFS_DINODE_FMT_LOCAL;
    new_inode->data_fork.local.data = target_data;
    new_inode->data_fork.local.size = target_len;
    xfs_stamp_new_inode(new_inode);
    xfs_trans_log_inode(tp, new_inode);

    rc = xfs_dir_addname(parent_ip, name, namelen, NEW_INO, XFS_DIR3_FT_SYMLINK, tp, true);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        xfs_inode_free_uncached(new_inode);
        xfs_inode_release(parent_ip);
        return rc;
    }

    rc = xfs_trans_commit(tp);
    if (rc != 0) {
        xfs_inode_free_uncached(new_inode);
        xfs_inode_release(parent_ip);
        return -EIO;
    }
    rc = xfs_inode_cache_new(new_inode);
    if (rc != 0) {
        mod::dbg::log("[xfs] committed symlink inode %lu could not enter inode cache: %d", static_cast<unsigned long>(NEW_INO), rc);
        xfs_inode_free_uncached(new_inode);
        xfs_inode_release(parent_ip);
        return rc;
    }
    if (statbuf != nullptr) {
        fill_stat(new_inode, statbuf);
    }
    size_t const LINK_PATH_LEN = xfs_known_path_len(fs_path, known_fs_path_len);
    xfs_path_inode_cache_invalidate_path(ctx, fs_path, LINK_PATH_LEN);
    xfs_path_inode_cache_store(ctx, fs_path, LINK_PATH_LEN, NEW_INO, XFS_DIR3_FT_SYMLINK);
    xfs_inode_release_metadata_locked(new_inode);
    xfs_inode_release(parent_ip);
    return 0;
}

auto xfs_rmdir_path(const ker::mod::security::Snapshot& credentials, const char* fs_path, XfsMountContext* ctx, size_t known_fs_path_len)
    -> int {
    if (fs_path == nullptr || ctx == nullptr) {
        return -EINVAL;
    }
    if (ctx->read_only) {
        return -EROFS;
    }
    if (fs_path[0] == '\0' || (fs_path[0] == '/' && fs_path[1] == '\0')) {
        return -EBUSY;
    }

    XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());

    XfsInode* parent_ip = nullptr;
    const char* name = nullptr;
    uint16_t namelen = 0;
    XfsDirEntry de{};
    int rc = xfs_lookup_namespace_entry_for_mutation(fs_path, ctx, &parent_ip, &name, &namelen, &de, known_fs_path_len);
    if (rc != 0) {
        return rc;
    }
    if (de.ftype != XFS_DIR3_FT_DIR) {
        xfs_inode_release(parent_ip);
        return -ENOTDIR;
    }
    if (parent_ip->nlink <= 2) {
        xfs_inode_release(parent_ip);
        return -EIO;
    }

    XfsInode* dir_ip = xfs_inode_read_known_allocated(ctx, de.ino);
    if (dir_ip == nullptr) {
        xfs_inode_release(parent_ip);
        return -ENOENT;
    }

    rc = ker::mod::security::authorize_file_removal(credentials, {parent_ip->uid, parent_ip->gid, parent_ip->mode},
                                                    {dir_ip->uid, dir_ip->gid, dir_ip->mode});
    if (rc != 0) {
        xfs_inode_release(dir_ip);
        xfs_inode_release(parent_ip);
        return rc;
    }

    RmdirDirectoryScan scan{.directory = dir_ip};
    int const ITERATE_RET = xfs_dir_iterate(dir_ip, scan_indexed_directory_entries, &scan);
    if (ITERATE_RET < 0 || scan.status != 0) {
        xfs_inode_release(dir_ip);
        xfs_inode_release(parent_ip);
        return ITERATE_RET < 0 ? ITERATE_RET : scan.status;
    }
    if (scan.indexed_entry_found) {
        xfs_inode_release(dir_ip);
        xfs_inode_release(parent_ip);
        return -ENOTEMPTY;
    }

    XfsTransaction* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) {
        xfs_inode_release(dir_ip);
        xfs_inode_release(parent_ip);
        return -ENOMEM;
    }

    rc = xfs_trans_capture_inode(tp, dir_ip);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        xfs_inode_release(dir_ip);
        xfs_inode_release(parent_ip);
        return rc;
    }

    rc = xfs_dir_removename(parent_ip, name, namelen, tp, de.ino);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        xfs_inode_release(dir_ip);
        xfs_inode_release(parent_ip);
        return rc;
    }

    parent_ip->nlink--;
    parent_ip->dirty = true;
    xfs_trans_log_inode(tp, parent_ip);
    // The removed directory loses both the parent entry and its self-reference.
    dir_ip->nlink = 0;
    dir_ip->dirty = true;
    xfs_trans_log_inode(tp, dir_ip);

    rc = xfs_trans_commit(tp);
    if (rc == 0) {
        xfs_path_inode_cache_bump_generation();
        xfs_parent_path_cache_purge_all_for_mount(ctx);
    }
    xfs_inode_release_metadata_locked(dir_ip);
    xfs_inode_release_metadata_locked(parent_ip);
    return (rc == 0) ? 0 : -EIO;
}

// ============================================================================
// Link
// ============================================================================

auto xfs_link_path(const ker::mod::security::Snapshot& credentials, const char* old_fs_path, const char* new_fs_path, XfsMountContext* ctx,
                   ker::vfs::Stat* statbuf, size_t known_old_fs_path_len, size_t known_new_fs_path_len) -> int {
    if (old_fs_path == nullptr || new_fs_path == nullptr || ctx == nullptr) {
        return -EINVAL;
    }
    if (ctx->read_only) {
        return -EROFS;
    }
    if (new_fs_path[0] == '\0' || (new_fs_path[0] == '/' && new_fs_path[1] == '\0')) {
        return -EEXIST;
    }

    XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());

    XfsInode* old_parent = nullptr;
    const char* old_name = nullptr;
    uint16_t old_namelen = 0;
    size_t old_fs_path_len = UNKNOWN_XFS_PATH_LEN;
    int rc = xfs_find_parent_and_name(old_fs_path, ctx, &old_parent, &old_name, &old_namelen, nullptr, true, known_old_fs_path_len,
                                      &old_fs_path_len);
    if (rc != 0) {
        return rc;
    }

    ker::mod::security::FileIdentity const SOURCE_PARENT{old_parent->uid, old_parent->gid, old_parent->mode};
    rc = ker::mod::security::authorize_file_access(credentials, SOURCE_PARENT, 1);
    if (rc != 0) {
        xfs_inode_release(old_parent);
        return rc;
    }
    XfsDirEntry old_de{};
    rc = xfs_dir_lookup(old_parent, old_name, old_namelen, &old_de);
    xfs_inode_release(old_parent);
    if (rc != 0) {
        return rc;
    }

    XfsInode* source_ip = xfs_inode_read_known_allocated(ctx, old_de.ino);
    if (source_ip == nullptr) {
        return -ENOENT;
    }
    if (source_ip->nlink == 0) {
        xfs_inode_release(source_ip);
        return -ENOENT;
    }
    if (old_de.ftype == XFS_DIR3_FT_DIR || xfs_inode_isdir(source_ip)) {
        xfs_inode_release(source_ip);
        return -EPERM;
    }
    if (source_ip->nlink == UINT32_MAX) {
        xfs_inode_release(source_ip);
        return -EMLINK;
    }

    XfsInode* new_parent = nullptr;
    const char* new_name = nullptr;
    uint16_t new_namelen = 0;
    size_t new_fs_path_len = UNKNOWN_XFS_PATH_LEN;
    rc = xfs_find_parent_and_name(new_fs_path, ctx, &new_parent, &new_name, &new_namelen, nullptr, true, known_new_fs_path_len,
                                  &new_fs_path_len);
    if (rc != 0) {
        xfs_inode_release(source_ip);
        return rc;
    }

    rc = ker::mod::security::authorize_file_link(credentials, SOURCE_PARENT, {new_parent->uid, new_parent->gid, new_parent->mode},
                                                 {source_ip->uid, source_ip->gid, source_ip->mode});
    if (rc != 0) {
        xfs_inode_release(new_parent);
        xfs_inode_release(source_ip);
        return rc;
    }
    XfsDirEntry existing{};
    rc = xfs_dir_lookup(new_parent, new_name, new_namelen, &existing);
    if (rc == 0) {
        xfs_inode_release(new_parent);
        xfs_inode_release(source_ip);
        return -EEXIST;
    }
    if (rc != -ENOENT) {
        xfs_inode_release(new_parent);
        xfs_inode_release(source_ip);
        return rc;
    }

    XfsTransaction* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) {
        xfs_inode_release(new_parent);
        xfs_inode_release(source_ip);
        return -ENOMEM;
    }

    uint8_t const SOURCE_FTYPE = old_de.ftype != XFS_DIR3_FT_UNKNOWN ? old_de.ftype : xfs_ftype_from_inode(source_ip);
    rc = xfs_trans_capture_inode(tp, source_ip);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        xfs_inode_release(new_parent);
        xfs_inode_release(source_ip);
        return rc;
    }
    source_ip->nlink++;
    source_ip->dirty = true;
    xfs_trans_log_inode(tp, source_ip);

    rc = xfs_dir_addname(new_parent, new_name, new_namelen, old_de.ino, SOURCE_FTYPE, tp, true);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        xfs_inode_release(new_parent);
        xfs_inode_release(source_ip);
        return rc;
    }

    new_parent->dirty = true;
    xfs_trans_log_inode(tp, new_parent);

    rc = xfs_trans_commit(tp);
    if (rc == 0) {
        if (statbuf != nullptr) {
            fill_stat(source_ip, statbuf);
        }
        xfs_path_inode_cache_invalidate_path(ctx, new_fs_path, new_fs_path_len);
        xfs_path_inode_cache_store(ctx, new_fs_path, new_fs_path_len, old_de.ino, SOURCE_FTYPE);
        xfs_path_inode_cache_store(ctx, old_fs_path, old_fs_path_len, old_de.ino, SOURCE_FTYPE);
    }

    xfs_inode_release_metadata_locked(new_parent);
    xfs_inode_release_metadata_locked(source_ip);
    return (rc == 0) ? 0 : -EIO;
}

// ============================================================================
// Rename
// ============================================================================

auto xfs_rename_path(const ker::mod::security::Snapshot& credentials, const char* old_fs_path, const char* new_fs_path,
                     XfsMountContext* ctx, Stat* statbuf, size_t known_old_fs_path_len, size_t known_new_fs_path_len,
                     bool source_requires_directory, bool destination_requires_directory, bool* namespace_changed,
                     bool* displaced_hardlink_count_changed) -> int {
    if (old_fs_path == nullptr || new_fs_path == nullptr || ctx == nullptr) return -EINVAL;
    if (ctx->read_only) return -EROFS;
    std::array<char, XFS_LOOKUP_PATH_CAPACITY> old_path{}, new_path{};
    auto copy = [](const char* input, size_t known_length, auto& output, bool& requires_directory) -> int {
        size_t length = known_length == UNKNOWN_XFS_PATH_LEN ? std::strlen(input) : known_length;
        if (length >= output.size()) return -ENAMETOOLONG;
        requires_directory = requires_directory || (length > 1 && input[length - 1] == '/');
        while (length > 1 && input[length - 1] == '/') --length;
        size_t start = length;
        while (start > 0 && input[start - 1] != '/') --start;
        if ((length - start == 1 && input[start] == '.') || (length - start == 2 && input[start] == '.' && input[start + 1] == '.'))
            return -EINVAL;
        std::memcpy(output.data(), input, length);
        output[length] = '\0';
        return 0;
    };
    int rc = copy(old_fs_path, known_old_fs_path_len, old_path, source_requires_directory);
    if (rc != 0) return rc;
    rc = copy(new_fs_path, known_new_fs_path_len, new_path, destination_requires_directory);
    if (rc != 0) return rc;
    // Acquisition and consumption each take metadata_lock; retained inodes
    // bridge those scopes and pair validation rejects intervening mutation.
    for (unsigned attempt = 0; attempt < 4; ++attempt) {
        XfsLookupHandle source{}, destination{};
        rc = xfs_acquire_lookup(credentials, old_path.data(), ctx, &source);
        if (rc == 0) rc = xfs_acquire_lookup(credentials, new_path.data(), ctx, &destination);
        if (rc == 0)
            rc = xfs_rename_lookup(credentials, source, destination, statbuf, source_requires_directory, destination_requires_directory,
                                   namespace_changed, displaced_hardlink_count_changed);
        if (rc != -EAGAIN) return rc;
    }
    return -EAGAIN;
}

// ============================================================================
// Unlink / remove
// ============================================================================

auto xfs_unlink_path(const ker::mod::security::Snapshot& credentials, const char* fs_path, XfsMountContext* ctx, size_t known_fs_path_len)
    -> int {
    if (fs_path == nullptr || ctx == nullptr) {
        return -EINVAL;
    }

    // Cannot unlink root
    if (fs_path[0] == '\0' || (fs_path[0] == '/' && fs_path[1] == '\0')) {
        return -EBUSY;
    }

    if (ctx->read_only) {
        return -EROFS;
    }

    XfsMetadataGuard metadata_guard(ctx, true, WOS_PERF_CALLSITE());

    XfsInode* parent_ip = nullptr;
    const char* filename = nullptr;
    uint16_t filename_len = 0;
    size_t fs_path_len = UNKNOWN_XFS_PATH_LEN;
    XfsDirEntry de{};
    int rc =
        xfs_lookup_namespace_entry_for_mutation(fs_path, ctx, &parent_ip, &filename, &filename_len, &de, known_fs_path_len, &fs_path_len);
    if (rc != 0) {
        return rc;
    }

    // Cannot unlink a directory with unlink (should use rmdir)
    if (de.ftype == XFS_DIR3_FT_DIR) {
        xfs_inode_release(parent_ip);
        return -EISDIR;
    }

    // Retain and authorize the exact inode before starting a removal transaction.
    XfsInode* target_ip = xfs_inode_read_known_allocated(ctx, de.ino);
    if (target_ip == nullptr) {
        xfs_inode_release(parent_ip);
        return -ENOENT;
    }
    rc = ker::mod::security::authorize_file_removal(credentials, {parent_ip->uid, parent_ip->gid, parent_ip->mode},
                                                    {target_ip->uid, target_ip->gid, target_ip->mode});
    if (rc != 0) {
        xfs_inode_release(target_ip);
        xfs_inode_release(parent_ip);
        return rc;
    }
    XfsTransaction* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) {
        xfs_inode_release(target_ip);
        xfs_inode_release(parent_ip);
        return -ENOMEM;
    }
    rc = xfs_trans_capture_inode(tp, target_ip);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        xfs_inode_release(target_ip);
        xfs_inode_release(parent_ip);
        return rc;
    }

    // Remove from parent directory
    rc = xfs_dir_removename(parent_ip, filename, filename_len, tp, de.ino);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        xfs_inode_release(target_ip);
        xfs_inode_release(parent_ip);
        return rc;
    }

    parent_ip->dirty = true;
    xfs_trans_log_inode(tp, parent_ip);

    if (target_ip->nlink > 0) {
        target_ip->nlink--;
    }
    target_ip->dirty = true;
    xfs_trans_log_inode(tp, target_ip);

    rc = xfs_trans_commit(tp);
    if (rc == 0) {
        xfs_path_inode_cache_invalidate_path(ctx, fs_path, fs_path_len);
    }

    xfs_inode_release_metadata_locked(target_ip);
    xfs_inode_release_metadata_locked(parent_ip);

    return (rc == 0) ? 0 : -EIO;
}

namespace {

auto xfs_lookup_target_locked(const XfsLookupHandle& handle, XfsInode** target_out) -> int {
    int const VALIDATE_RET = xfs_validate_lookup_locked(handle);
    if (VALIDATE_RET != 0) {
        return VALIDATE_RET;
    }
    if (!handle.target_existed() || handle.target() == nullptr) {
        return -ENOENT;
    }
    *target_out = handle.target();
    return 0;
}

auto xfs_validate_lookup_pair_locked(const XfsLookupHandle& source, const XfsLookupHandle& destination) -> int {
    if (source.mount() == nullptr || destination.mount() == nullptr || source.mount() != destination.mount()) {
        return -EXDEV;
    }
    int const SOURCE_RET = xfs_validate_lookup_locked(source);
    if (SOURCE_RET != 0) {
        return SOURCE_RET;
    }
    return xfs_validate_lookup_locked(destination);
}

void xfs_invalidate_lookup_path(const XfsLookupHandle& handle) {
    if (handle.path_length() == 0) {
        xfs_path_inode_cache_bump_generation();
        return;
    }
    xfs_path_inode_cache_invalidate_path(handle.mount(), handle.path(), handle.path_length());
}

}  // namespace

auto xfs_open_lookup(const ker::mod::security::Snapshot& credentials, const XfsLookupHandle& handle, int flags, int mode,
                     bool require_directory, int* result_out) -> File* {
    XfsMountContext* const MOUNT = handle.mount();
    if (MOUNT == nullptr) {
        xfs_set_open_result(result_out, -EINVAL);
        return nullptr;
    }

    if ((flags & 3) == 3 || (flags & (ker::vfs::O_DIRECTORY | ker::vfs::O_CREAT)) == (ker::vfs::O_DIRECTORY | ker::vfs::O_CREAT)) {
        xfs_set_open_result(result_out, -EINVAL);
        return nullptr;
    }
    require_directory = require_directory || (flags & ker::vfs::O_DIRECTORY) != 0;
    XfsMetadataGuard metadata_guard(MOUNT, true, WOS_PERF_CALLSITE());
    XfsOpenStorage storage;
    int const VALIDATE_RET = xfs_validate_lookup_locked(handle);
    if (VALIDATE_RET != 0) {
        xfs_set_open_result(result_out, VALIDATE_RET);
        return nullptr;
    }

    if (!handle.root_target()) {
        auto* parent = handle.parent();
        int const SEARCH = ker::mod::security::authorize_file_access(credentials, {parent->uid, parent->gid, parent->mode}, 1);
        if (SEARCH != 0) {
            xfs_set_open_result(result_out, SEARCH);
            return nullptr;
        }
    }
    XfsInode* inode = nullptr;
    bool created_by_open = false;
    if (handle.target_existed()) {
        if ((flags & (ker::vfs::O_CREAT | ker::vfs::O_EXCL)) == (ker::vfs::O_CREAT | ker::vfs::O_EXCL)) {
            xfs_set_open_result(result_out, -EEXIST);
            return nullptr;
        }
        inode = xfs_inode_read_cached(MOUNT, handle.target()->ino);
        if (inode == nullptr || inode != handle.target()) {
            xfs_inode_release_metadata_locked(inode);
            xfs_set_open_result(result_out, -EAGAIN);
            return nullptr;
        }
    } else {
        if ((flags & ker::vfs::O_CREAT) == 0) {
            xfs_set_open_result(result_out, -ENOENT);
            return nullptr;
        }
        if (MOUNT->read_only) {
            xfs_set_open_result(result_out, -EROFS);
            return nullptr;
        }

        ker::mod::security::FileIdentity identity{};
        auto* parent = handle.parent();
        int const CREATE_POLICY = require_directory
                                      ? -ENOENT
                                      : ker::mod::security::prepare_file_create(credentials, {parent->uid, parent->gid, parent->mode},
                                                                                static_cast<uint32_t>(mode), identity);
        if (CREATE_POLICY != 0 || !storage.prepare()) {
            xfs_set_open_result(result_out, CREATE_POLICY != 0 ? CREATE_POLICY : -ENOMEM);
            return nullptr;
        }
        XfsTransaction* transaction = xfs_trans_alloc(MOUNT);
        if (transaction == nullptr) {
            xfs_set_open_result(result_out, -ENOMEM);
            return nullptr;
        }
        uint16_t const INODE_MODE = static_cast<uint16_t>(identity.mode);
        xfs_ino_t const NEW_INO = xfs_ialloc(MOUNT, transaction, INODE_MODE);
        if (NEW_INO == NULLFSINO) {
            xfs_trans_cancel(transaction);
            xfs_set_open_result(result_out, -ENOSPC);
            return nullptr;
        }
        if (xfs_ialloc_conflicts_with_cached_inode(MOUNT, NEW_INO)) {
            xfs_trans_cancel(transaction);
            xfs_set_open_result(result_out, -EIO);
            return nullptr;
        }

        auto* new_inode = xfs_inode_alloc_uninitialized_object();
        if (new_inode == nullptr) {
            xfs_trans_cancel(transaction);
            xfs_set_open_result(result_out, -ENOMEM);
            return nullptr;
        }
        new_inode->ino = NEW_INO;
        new_inode->mount = MOUNT;
        new_inode->agno = xfs_ino_ag(NEW_INO, MOUNT->agino_log);
        new_inode->agino = xfs_ag_ino(NEW_INO, MOUNT->agino_log);
        new_inode->mode = INODE_MODE;
        new_inode->uid = identity.uid;
        new_inode->gid = identity.gid;
        new_inode->nlink = 1;
        new_inode->size = 0;
        new_inode->nblocks = 0;
        new_inode->gen = 0;
        new_inode->flags = 0;
        new_inode->flags2 = 0;
        new_inode->atime = 0;
        new_inode->mtime = 0;
        new_inode->ctime = 0;
        new_inode->crtime = 0;
        new_inode->data_fork.format = XFS_DINODE_FMT_EXTENTS;
        new_inode->data_fork.extents.list = nullptr;
        new_inode->data_fork.extents.count = 0;
        new_inode->data_fork.extents.capacity = 0;
        new_inode->forkoff = 0;
        new_inode->attr_fork.format = XFS_DINODE_FMT_LOCAL;
        new_inode->attr_fork.local.data = nullptr;
        new_inode->attr_fork.local.size = 0;
        new_inode->has_attr_fork = false;
        new_inode->nextents = 0;
        new_inode->anextents = 0;
        new_inode->refcount = 0;
        new_inode->hash_next = nullptr;
        new_inode->inactivation_started = false;
        new_inode->dirty = false;
        new_inode->dir_generation = 0;
        new_inode->dir_leaf_index_complete_generation = 0;
        new_inode->dir_leaf_index_complete = false;
        for (auto& word : new_inode->dir_name_filter) {
            word = 0;
        }
        new_inode->dir_name_filter_complete = false;
        xfs_stamp_new_inode(new_inode);
        xfs_trans_log_inode(transaction, new_inode);

        int result =
            xfs_dir_addname(handle.parent(), handle.leaf(), handle.leaf_length(), NEW_INO, XFS_DIR3_FT_REG_FILE, transaction, true);
        if (result != 0) {
            xfs_trans_cancel(transaction);
            xfs_inode_free_uncached(new_inode);
            xfs_set_open_result(result_out, result);
            return nullptr;
        }
        result = xfs_trans_commit(transaction);
        if (result != 0) {
            xfs_inode_free_uncached(new_inode);
            xfs_set_open_result(result_out, -EIO);
            return nullptr;
        }
        result = xfs_inode_cache_new(new_inode);
        if (result != 0) {
            mod::dbg::log("[xfs] committed retained-open inode %lu could not enter inode cache: %d", static_cast<unsigned long>(NEW_INO),
                          result);
            xfs_inode_free_uncached(new_inode);
            xfs_set_open_result(result_out, result);
            return nullptr;
        }
        xfs_invalidate_lookup_path(handle);
        inode = new_inode;
        created_by_open = true;
    }

    if (require_directory && !xfs_inode_isdir(inode)) {
        xfs_inode_release_metadata_locked(inode);
        xfs_set_open_result(result_out, -ENOTDIR);
        return nullptr;
    }
    if ((flags & ker::vfs::O_NOFOLLOW) != 0 && xfs_inode_islnk(inode)) {
        xfs_inode_release_metadata_locked(inode);
        xfs_set_open_result(result_out, -ELOOP);
        return nullptr;
    }
    int const OPEN_POLICY =
        created_by_open ? 0
                        : ker::mod::security::authorize_file_open(credentials, {inode->uid, inode->gid, inode->mode},
                                                                  static_cast<unsigned>(flags) & 3U, (flags & ker::vfs::O_TRUNC) != 0);
    if (OPEN_POLICY != 0 || !storage.prepare()) {
        xfs_inode_release_metadata_locked(inode);
        xfs_set_open_result(result_out, OPEN_POLICY != 0 ? OPEN_POLICY : -ENOMEM);
        return nullptr;
    }
    int const ACCESS_MODE = flags & 3;
    if ((ACCESS_MODE == 1 || ACCESS_MODE == 2 || (flags & ker::vfs::O_TRUNC) != 0) && MOUNT->read_only) {
        xfs_inode_release_metadata_locked(inode);
        xfs_set_open_result(result_out, -EROFS);
        return nullptr;
    }

    Stat opened_stat{};
    fill_stat(inode, &opened_stat);
    const char* const HANDLE_PATH = handle.path_length() == 0 ? nullptr : handle.path();
    metadata_guard.unlock();
    return xfs_file_from_inode(storage, MOUNT, inode, flags, HANDLE_PATH, handle.path_length(), created_by_open, opened_stat, result_out);
}

auto xfs_pin_directory_lookup(const ker::mod::security::Snapshot& credentials, const XfsLookupHandle& handle, int* result_out,
                              bool require_target_search) -> File* {
    auto fail = [&](int error) -> File* {
        xfs_set_open_result(result_out, error);
        return nullptr;
    };
    auto* mount = handle.mount();
    if (mount == nullptr) return fail(-EINVAL);
    XfsMetadataGuard guard(mount, true, WOS_PERF_CALLSITE());
    int const VALIDATE = xfs_validate_lookup_locked(handle);
    if (VALIDATE != 0) return fail(VALIDATE);
    auto* target = handle.target();
    if (target == nullptr) return fail(-ENOENT);
    if (!xfs_inode_isdir(target)) return fail(-ENOTDIR);
    if (!handle.root_target()) {
        auto* parent = handle.parent();
        int const SEARCH = ker::mod::security::authorize_file_access(credentials, {parent->uid, parent->gid, parent->mode}, 1);
        if (SEARCH != 0) return fail(SEARCH);
    }
    if (require_target_search) {
        int const SEARCH = ker::mod::security::authorize_file_access(credentials, {target->uid, target->gid, target->mode}, 1);
        if (SEARCH != 0) return fail(SEARCH);
    }
    auto* inode = xfs_inode_read_cached(mount, target->ino);
    if (inode == nullptr || inode != target) {
        xfs_inode_release_metadata_locked(inode);
        return fail(-EAGAIN);
    }
    XfsOpenStorage storage;
    if (!storage.prepare()) {
        xfs_inode_release_metadata_locked(inode);
        return fail(-ENOMEM);
    }
    Stat metadata{};
    fill_stat(inode, &metadata);
    guard.unlock();
    return xfs_file_from_inode(storage, mount, inode, ker::vfs::O_DIRECTORY | ker::vfs::FILE_ACCESS_SEARCH_ONLY | ker::vfs::O_NO_CACHE,
                               nullptr, 0, false, metadata, result_out);
}

auto xfs_stat_lookup(const XfsLookupHandle& handle, bool require_directory, Stat* statbuf) -> int {
    if (handle.mount() == nullptr || statbuf == nullptr) {
        return -EINVAL;
    }
    XfsMetadataGuard metadata_guard(handle.mount(), true, WOS_PERF_CALLSITE());
    XfsInode* target = nullptr;
    int const LOOKUP_RET = xfs_lookup_target_locked(handle, &target);
    if (LOOKUP_RET != 0) {
        return LOOKUP_RET;
    }
    if (require_directory && !xfs_inode_isdir(target)) {
        return -ENOTDIR;
    }
    fill_stat(target, statbuf);
    return 0;
}

auto xfs_readlink_lookup(const XfsLookupHandle& handle, char* buf, size_t bufsize) -> ssize_t {
    if (handle.mount() == nullptr || buf == nullptr || bufsize == 0) {
        return -EINVAL;
    }
    XfsMetadataGuard metadata_guard(handle.mount(), true, WOS_PERF_CALLSITE());
    XfsInode* target = nullptr;
    int const LOOKUP_RET = xfs_lookup_target_locked(handle, &target);
    if (LOOKUP_RET != 0) {
        return LOOKUP_RET;
    }
    if (!xfs_inode_islnk(target)) {
        return -EINVAL;
    }
    int const RET = xfs_readlink(target, buf, bufsize);
    return RET < 0 ? RET : static_cast<ssize_t>(RET);
}

auto xfs_chmod_lookup(const XfsLookupHandle& handle, const ker::mod::security::Snapshot& credentials, int mode, Stat* statbuf,
                      bool read_only) -> int {
    XfsMountContext* const MOUNT = handle.mount();
    if (MOUNT == nullptr) return -EINVAL;
    if (read_only || MOUNT->read_only) return -EROFS;
    // Retained target can be absent; still validate first so a stale negative
    // lookup returns EAGAIN rather than inventing a definitive ENOENT.
    std::optional<ker::mod::sys::MutexGuard> io_guard;
    if (handle.target() != nullptr) io_guard.emplace(handle.target()->io_lock);
    XfsMetadataGuard metadata_guard(MOUNT, true, WOS_PERF_CALLSITE());
    XfsInode* target = nullptr;
    int const RET = xfs_lookup_target_locked(handle, &target);
    return RET != 0 ? RET : chmod_inode_locked(target, credentials, mode, statbuf);
}

auto xfs_chown_lookup(const XfsLookupHandle& handle, const ker::mod::security::Snapshot& credentials, uint32_t uid, uint32_t gid,
                      Stat* statbuf, bool read_only) -> int {
    XfsMountContext* const MOUNT = handle.mount();
    if (MOUNT == nullptr) return -EINVAL;
    if (read_only || MOUNT->read_only) return -EROFS;
    std::optional<ker::mod::sys::MutexGuard> io_guard;
    if (handle.target() != nullptr) io_guard.emplace(handle.target()->io_lock);
    XfsMetadataGuard metadata_guard(MOUNT, true, WOS_PERF_CALLSITE());
    XfsInode* target = nullptr;
    int const RET = xfs_lookup_target_locked(handle, &target);
    return RET != 0 ? RET : chown_inode_locked(target, credentials, uid, gid, statbuf);
}

auto xfs_utimens_lookup(const XfsLookupHandle& handle, const ker::mod::security::Snapshot& credentials, const TimestampUpdate& times,
                        Stat* statbuf, bool read_only) -> int {
    XfsMountContext* const MOUNT = handle.mount();
    if (MOUNT == nullptr) return -EINVAL;
    if (read_only || MOUNT->read_only) return -EROFS;
    std::optional<ker::mod::sys::MutexGuard> io_guard;
    if (handle.target() != nullptr) io_guard.emplace(handle.target()->io_lock);
    XfsMetadataGuard metadata_guard(MOUNT, true, WOS_PERF_CALLSITE());
    XfsInode* target = nullptr;
    int const RET = xfs_lookup_target_locked(handle, &target);
    return RET != 0 ? RET : xfs_set_inode_times(MOUNT, target, credentials, times, statbuf);
}

auto xfs_setxattr_lookup(const ker::mod::security::Snapshot& credentials, const XfsLookupHandle& handle, const char* name,
                         const void* value, size_t size, int flags, bool read_only) -> int {
    XfsMountContext* const MOUNT = handle.mount();
    if (MOUNT == nullptr) {
        return -EINVAL;
    }
    if (read_only || MOUNT->read_only) {
        return -EROFS;
    }
    PublicXattrName parsed{};
    if (int const RET = parse_public_xattr_name(name, &parsed); RET < 0) {
        return RET;
    }
    std::optional<ker::mod::sys::MutexGuard> io_guard;
    if (handle.target() != nullptr) io_guard.emplace(handle.target()->io_lock);
    XfsMetadataGuard metadata_guard(MOUNT, true, WOS_PERF_CALLSITE());
    XfsInode* target = nullptr;
    int const LOOKUP_RET = xfs_lookup_target_locked(handle, &target);
    return LOOKUP_RET != 0 ? LOOKUP_RET : set_public_xattr_locked(credentials, MOUNT, target, parsed, value, size, flags);
}

auto xfs_getxattr_lookup(const ker::mod::security::Snapshot& credentials, const XfsLookupHandle& handle, const char* name, void* value,
                         size_t size) -> ssize_t {
    if (handle.mount() == nullptr) {
        return -EINVAL;
    }
    PublicXattrName parsed{};
    if (int const RET = parse_public_xattr_name(name, &parsed); RET < 0) {
        return RET;
    }
    XfsMetadataGuard metadata_guard(handle.mount(), true, WOS_PERF_CALLSITE());
    XfsInode* target = nullptr;
    int const LOOKUP_RET = xfs_lookup_target_locked(handle, &target);
    return LOOKUP_RET != 0 ? LOOKUP_RET : get_public_xattr_locked(credentials, target, parsed, value, size);
}

auto xfs_listxattr_lookup(const ker::mod::security::Snapshot& credentials, const XfsLookupHandle& handle, char* list, size_t size)
    -> ssize_t {
    if (handle.mount() == nullptr) {
        return -EINVAL;
    }
    XfsMetadataGuard metadata_guard(handle.mount(), true, WOS_PERF_CALLSITE());
    XfsInode* target = nullptr;
    int const LOOKUP_RET = xfs_lookup_target_locked(handle, &target);
    return LOOKUP_RET != 0 ? LOOKUP_RET : list_public_xattrs_locked(credentials, target, list, size);
}

auto xfs_removexattr_lookup(const ker::mod::security::Snapshot& credentials, const XfsLookupHandle& handle, const char* name,
                            bool read_only) -> int {
    XfsMountContext* const MOUNT = handle.mount();
    if (MOUNT == nullptr) {
        return -EINVAL;
    }
    if (read_only || MOUNT->read_only) {
        return -EROFS;
    }
    PublicXattrName parsed{};
    if (int const RET = parse_public_xattr_name(name, &parsed); RET < 0) {
        return RET;
    }
    std::optional<ker::mod::sys::MutexGuard> io_guard;
    if (handle.target() != nullptr) io_guard.emplace(handle.target()->io_lock);
    XfsMetadataGuard metadata_guard(MOUNT, true, WOS_PERF_CALLSITE());
    XfsInode* target = nullptr;
    int const LOOKUP_RET = xfs_lookup_target_locked(handle, &target);
    return LOOKUP_RET != 0 ? LOOKUP_RET : remove_public_xattr_locked(credentials, MOUNT, target, parsed);
}

auto xfs_mkdir_lookup(const ker::mod::security::Snapshot& credentials, const XfsLookupHandle& handle, int mode, Stat* statbuf,
                      bool read_only) -> int {
    XfsMountContext* const MOUNT = handle.mount();
    if (MOUNT == nullptr) {
        return -EINVAL;
    }
    if (read_only || MOUNT->read_only) {
        return -EROFS;
    }
    XfsMetadataGuard metadata_guard(MOUNT, true, WOS_PERF_CALLSITE());
    int const VALIDATE_RET = xfs_validate_lookup_locked(handle);
    if (VALIDATE_RET != 0) {
        return VALIDATE_RET;
    }
    XfsInode* const PARENT = handle.parent();
    ker::mod::security::FileIdentity identity{};
    int const AUTH = ker::mod::security::prepare_directory_create(credentials, {PARENT->uid, PARENT->gid, PARENT->mode},
                                                                  static_cast<uint32_t>(mode), identity);
    if (AUTH != 0) return AUTH;
    if (handle.target_existed()) {
        if (statbuf != nullptr && handle.target() != nullptr && xfs_inode_isdir(handle.target())) {
            fill_stat(handle.target(), statbuf);
        }
        return -EEXIST;
    }
    if (PARENT->nlink == UINT32_MAX) {
        return -EMLINK;
    }

    xfs_ino_t const PARENT_INO = PARENT->ino;
    uint16_t const INODE_MODE = static_cast<uint16_t>(identity.mode);
    bool const USE_I8 = PARENT_INO > 0xFFFFFFFFULL;
    size_t const INO_BYTES = USE_I8 ? 8 : 4;
    size_t const SF_SIZE = 2 + INO_BYTES;
    auto* new_inode = xfs_inode_alloc_zeroed_object();
    auto* sf_data = new (std::nothrow) uint8_t[SF_SIZE];
    if (new_inode == nullptr || sf_data == nullptr) {
        xfs_inode_free_uncached(new_inode);
        delete[] sf_data;
        return -ENOMEM;
    }

    XfsTransaction* tp = xfs_trans_alloc(MOUNT);
    if (tp == nullptr) {
        delete[] sf_data;
        xfs_inode_free_uncached(new_inode);
        return -ENOMEM;
    }
    xfs_ino_t const NEW_INO = xfs_ialloc(MOUNT, tp, INODE_MODE);
    if (NEW_INO == NULLFSINO) {
        xfs_trans_cancel(tp);
        delete[] sf_data;
        xfs_inode_free_uncached(new_inode);
        return -ENOSPC;
    }
    if (xfs_ialloc_conflicts_with_cached_inode(MOUNT, NEW_INO)) {
        xfs_trans_cancel(tp);
        delete[] sf_data;
        xfs_inode_free_uncached(new_inode);
        return -EIO;
    }

    sf_data[0] = 0;
    sf_data[1] = USE_I8 ? 1 : 0;
    if (USE_I8) {
        for (int i = 7; i >= 0; --i) {
            sf_data[2 + (7 - i)] = static_cast<uint8_t>((PARENT_INO >> (i * 8)) & 0xFF);
        }
    } else {
        auto const PARENT_32 = static_cast<uint32_t>(PARENT_INO);
        for (int i = 3; i >= 0; --i) {
            sf_data[2 + (3 - i)] = static_cast<uint8_t>((PARENT_32 >> (i * 8)) & 0xFF);
        }
    }

    new_inode->ino = NEW_INO;
    new_inode->mount = MOUNT;
    new_inode->agno = xfs_ino_ag(NEW_INO, MOUNT->agino_log);
    new_inode->agino = xfs_ag_ino(NEW_INO, MOUNT->agino_log);
    new_inode->mode = INODE_MODE;
    new_inode->uid = identity.uid;
    new_inode->gid = identity.gid;
    new_inode->size = SF_SIZE;
    new_inode->nlink = 2;
    new_inode->data_fork.format = XFS_DINODE_FMT_LOCAL;
    new_inode->data_fork.local.data = sf_data;
    new_inode->data_fork.local.size = SF_SIZE;
    xfs_dir_name_filter_init_empty(new_inode);
    xfs_stamp_new_inode(new_inode);
    xfs_trans_log_inode(tp, new_inode);

    int rc = xfs_dir_addname(PARENT, handle.leaf(), handle.leaf_length(), NEW_INO, XFS_DIR3_FT_DIR, tp, true);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        xfs_inode_free_uncached(new_inode);
        return rc;
    }
    PARENT->nlink++;
    PARENT->dirty = true;
    xfs_trans_log_inode(tp, PARENT);
    rc = xfs_trans_commit(tp);
    if (rc != 0) {
        xfs_inode_free_uncached(new_inode);
        return rc;
    }
    xfs_dentry_cache_invalidate_dir(new_inode);
    rc = xfs_inode_cache_new(new_inode);
    if (rc != 0) {
        mod::dbg::log("[xfs] committed mkdir inode %lu could not enter inode cache: %d", static_cast<unsigned long>(NEW_INO), rc);
        xfs_inode_free_uncached(new_inode);
        return rc;
    }
    if (statbuf != nullptr) {
        fill_stat(new_inode, statbuf);
    }
    xfs_invalidate_lookup_path(handle);
    if (handle.path_length() != 0) {
        xfs_path_inode_cache_store(MOUNT, handle.path(), handle.path_length(), NEW_INO, XFS_DIR3_FT_DIR);
        xfs_parent_path_cache_store(MOUNT, handle.path(), handle.path_length(), NEW_INO);
    }
    xfs_inode_release_metadata_locked(new_inode);
    return 0;
}

auto xfs_symlink_lookup(const ker::mod::security::Snapshot& credentials, const XfsLookupHandle& handle, const char* target, Stat* statbuf,
                        bool require_directory, bool read_only) -> int {
    XfsMountContext* const MOUNT = handle.mount();
    if (MOUNT == nullptr || target == nullptr) {
        return -EINVAL;
    }
    if (read_only || MOUNT->read_only) {
        return -EROFS;
    }
    size_t target_length = 0;
    while (target[target_length] != '\0') {
        ++target_length;
    }
    size_t const INLINE_CAPACITY = MOUNT->inode_size > XFS_DINODE_SIZE_V3 ? MOUNT->inode_size - XFS_DINODE_SIZE_V3 : 0;
    if (target_length > INLINE_CAPACITY) {
        return -ENAMETOOLONG;
    }

    XfsMetadataGuard metadata_guard(MOUNT, true, WOS_PERF_CALLSITE());
    int const VALIDATE_RET = xfs_validate_lookup_locked(handle);
    if (VALIDATE_RET != 0) {
        return VALIDATE_RET;
    }
    ker::mod::security::FileIdentity identity{};
    int const AUTH = ker::mod::security::prepare_symlink_create(
        credentials, {handle.parent()->uid, handle.parent()->gid, handle.parent()->mode}, identity);
    if (AUTH != 0) return AUTH;
    if (handle.target_existed()) {
        return -EEXIST;
    }
    if (require_directory) return -ENOENT;
    auto* target_data = new (std::nothrow) uint8_t[target_length == 0 ? 1 : target_length];
    auto* new_inode = xfs_inode_alloc_zeroed_object();
    if (target_data == nullptr || new_inode == nullptr) {
        delete[] target_data;
        xfs_inode_free_uncached(new_inode);
        return -ENOMEM;
    }
    if (target_length != 0) {
        std::memcpy(target_data, target, target_length);
    }
    XfsTransaction* tp = xfs_trans_alloc(MOUNT);
    if (tp == nullptr) {
        delete[] target_data;
        xfs_inode_free_uncached(new_inode);
        return -ENOMEM;
    }
    constexpr uint16_t SYMLINK_MODE = 0120000 | 0777;
    xfs_ino_t const NEW_INO = xfs_ialloc(MOUNT, tp, SYMLINK_MODE);
    if (NEW_INO == NULLFSINO) {
        xfs_trans_cancel(tp);
        delete[] target_data;
        xfs_inode_free_uncached(new_inode);
        return -ENOSPC;
    }
    if (xfs_ialloc_conflicts_with_cached_inode(MOUNT, NEW_INO)) {
        xfs_trans_cancel(tp);
        delete[] target_data;
        xfs_inode_free_uncached(new_inode);
        return -EIO;
    }
    new_inode->ino = NEW_INO;
    new_inode->mount = MOUNT;
    new_inode->agno = xfs_ino_ag(NEW_INO, MOUNT->agino_log);
    new_inode->agino = xfs_ag_ino(NEW_INO, MOUNT->agino_log);
    new_inode->mode = SYMLINK_MODE;
    new_inode->uid = identity.uid;
    new_inode->gid = identity.gid;
    new_inode->size = target_length;
    new_inode->nlink = 1;
    new_inode->data_fork.format = XFS_DINODE_FMT_LOCAL;
    new_inode->data_fork.local.data = target_data;
    new_inode->data_fork.local.size = target_length;
    xfs_stamp_new_inode(new_inode);
    xfs_trans_log_inode(tp, new_inode);
    int rc = xfs_dir_addname(handle.parent(), handle.leaf(), handle.leaf_length(), NEW_INO, XFS_DIR3_FT_SYMLINK, tp, true);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        xfs_inode_free_uncached(new_inode);
        return rc;
    }
    rc = xfs_trans_commit(tp);
    if (rc != 0) {
        xfs_inode_free_uncached(new_inode);
        return -EIO;
    }
    rc = xfs_inode_cache_new(new_inode);
    if (rc != 0) {
        mod::dbg::log("[xfs] committed symlink inode %lu could not enter inode cache: %d", static_cast<unsigned long>(NEW_INO), rc);
        xfs_inode_free_uncached(new_inode);
        return rc;
    }
    if (statbuf != nullptr) {
        fill_stat(new_inode, statbuf);
    }
    xfs_invalidate_lookup_path(handle);
    if (handle.path_length() != 0) {
        xfs_path_inode_cache_store(MOUNT, handle.path(), handle.path_length(), NEW_INO, XFS_DIR3_FT_SYMLINK);
    }
    xfs_inode_release_metadata_locked(new_inode);
    return 0;
}

auto xfs_unlink_lookup(const ker::mod::security::Snapshot& credentials, const XfsLookupHandle& handle, bool read_only) -> int {
    XfsMountContext* const MOUNT = handle.mount();
    if (MOUNT == nullptr) {
        return -EINVAL;
    }
    if (read_only || MOUNT->read_only) {
        return -EROFS;
    }
    XfsMetadataGuard metadata_guard(MOUNT, true, WOS_PERF_CALLSITE());
    XfsInode* target = nullptr;
    int rc = xfs_lookup_target_locked(handle, &target);
    if (rc != 0) {
        return rc;
    }
    if (xfs_inode_isdir(target)) {
        return -EISDIR;
    }
    rc = ker::mod::security::authorize_file_removal(credentials, {handle.parent()->uid, handle.parent()->gid, handle.parent()->mode},
                                                    {target->uid, target->gid, target->mode});
    if (rc != 0) return rc;
    XfsTransaction* tp = xfs_trans_alloc(MOUNT);
    if (tp == nullptr) {
        return -ENOMEM;
    }
    rc = xfs_trans_capture_inode(tp, target);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        return rc;
    }
    rc = xfs_dir_removename(handle.parent(), handle.leaf(), handle.leaf_length(), tp, target->ino);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        return rc;
    }
    handle.parent()->dirty = true;
    xfs_trans_log_inode(tp, handle.parent());
    if (target->nlink > 0) {
        target->nlink--;
    }
    target->dirty = true;
    xfs_trans_log_inode(tp, target);
    rc = xfs_trans_commit(tp);
    if (rc == 0) {
        xfs_invalidate_lookup_path(handle);
    }
    return rc == 0 ? 0 : -EIO;
}

auto xfs_rmdir_lookup(const ker::mod::security::Snapshot& credentials, const XfsLookupHandle& handle, bool read_only) -> int {
    XfsMountContext* const MOUNT = handle.mount();
    if (MOUNT == nullptr) {
        return -EINVAL;
    }
    if (read_only || MOUNT->read_only) {
        return -EROFS;
    }
    XfsMetadataGuard metadata_guard(MOUNT, true, WOS_PERF_CALLSITE());
    XfsInode* directory = nullptr;
    int rc = xfs_lookup_target_locked(handle, &directory);
    if (rc != 0) {
        return rc;
    }
    if (!xfs_inode_isdir(directory)) {
        return -ENOTDIR;
    }
    rc = ker::mod::security::authorize_file_removal(credentials, {handle.parent()->uid, handle.parent()->gid, handle.parent()->mode},
                                                    {directory->uid, directory->gid, directory->mode});
    if (rc != 0) return rc;
    if (handle.parent()->nlink <= 2) {
        return -EIO;
    }
    RmdirDirectoryScan scan{.directory = directory};
    int const ITERATE_RET = xfs_dir_iterate(directory, scan_indexed_directory_entries, &scan);
    if (ITERATE_RET < 0 || scan.status != 0) {
        return ITERATE_RET < 0 ? ITERATE_RET : scan.status;
    }
    if (scan.indexed_entry_found) {
        return -ENOTEMPTY;
    }
    XfsTransaction* tp = xfs_trans_alloc(MOUNT);
    if (tp == nullptr) {
        return -ENOMEM;
    }
    rc = xfs_trans_capture_inode(tp, directory);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        return rc;
    }
    rc = xfs_dir_removename(handle.parent(), handle.leaf(), handle.leaf_length(), tp, directory->ino);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        return rc;
    }
    handle.parent()->nlink--;
    handle.parent()->dirty = true;
    xfs_trans_log_inode(tp, handle.parent());
    directory->nlink = 0;
    directory->dirty = true;
    xfs_trans_log_inode(tp, directory);
    rc = xfs_trans_commit(tp);
    if (rc == 0) {
        xfs_path_inode_cache_bump_generation();
        xfs_parent_path_cache_purge_all_for_mount(MOUNT);
    }
    return rc == 0 ? 0 : -EIO;
}

auto xfs_link_lookup(const ker::mod::security::Snapshot& credentials, const XfsLookupHandle& source, const XfsLookupHandle& destination,
                     Stat* statbuf, bool source_requires_directory, bool destination_requires_directory, bool read_only) -> int {
    XfsMountContext* const MOUNT = source.mount();
    if (MOUNT == nullptr || destination.mount() == nullptr) {
        return -EINVAL;
    }
    if (MOUNT != destination.mount()) {
        return -EXDEV;
    }
    if (read_only || MOUNT->read_only) {
        return -EROFS;
    }
    XfsMetadataGuard metadata_guard(MOUNT, true, WOS_PERF_CALLSITE());
    int rc = xfs_validate_lookup_pair_locked(source, destination);
    if (rc != 0) {
        return rc;
    }
    XfsInode* const TARGET = source.target();
    if (!source.target_existed() || TARGET == nullptr) {
        return -ENOENT;
    }
    if (xfs_inode_isdir(TARGET)) {
        return -EPERM;
    }
    int const AUTH = ker::mod::security::authorize_file_link(
        credentials, {source.parent()->uid, source.parent()->gid, source.parent()->mode},
        {destination.parent()->uid, destination.parent()->gid, destination.parent()->mode}, {TARGET->uid, TARGET->gid, TARGET->mode});
    if (AUTH != 0) return AUTH;
    if (source_requires_directory) return -ENOTDIR;
    if (TARGET->nlink == UINT32_MAX) {
        return -EMLINK;
    }
    if (destination.target_existed()) {
        return -EEXIST;
    }
    if (destination_requires_directory) return -ENOENT;
    XfsTransaction* tp = xfs_trans_alloc(MOUNT);
    if (tp == nullptr) {
        return -ENOMEM;
    }
    rc = xfs_trans_capture_inode(tp, TARGET);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        return rc;
    }
    TARGET->nlink++;
    TARGET->dirty = true;
    xfs_trans_log_inode(tp, TARGET);
    uint8_t const FTYPE = xfs_ftype_from_inode(TARGET);
    rc = xfs_dir_addname(destination.parent(), destination.leaf(), destination.leaf_length(), TARGET->ino, FTYPE, tp, true);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        return rc;
    }
    destination.parent()->dirty = true;
    xfs_trans_log_inode(tp, destination.parent());
    rc = xfs_trans_commit(tp);
    if (rc == 0) {
        if (statbuf != nullptr) {
            fill_stat(TARGET, statbuf);
        }
        if (source.path_length() == 0 || destination.path_length() == 0) {
            xfs_path_inode_cache_bump_generation();
        } else {
            xfs_path_inode_cache_invalidate_path(MOUNT, destination.path(), destination.path_length());
            xfs_path_inode_cache_store(MOUNT, destination.path(), destination.path_length(), TARGET->ino, FTYPE);
            xfs_path_inode_cache_store(MOUNT, source.path(), source.path_length(), TARGET->ino, FTYPE);
        }
    }
    return rc == 0 ? 0 : -EIO;
}

auto xfs_rename_lookup(const ker::mod::security::Snapshot& credentials, const XfsLookupHandle& source, const XfsLookupHandle& destination,
                       Stat* statbuf, bool source_requires_directory, bool destination_requires_directory, bool* namespace_changed,
                       bool* displaced_hardlink_count_changed, bool read_only) -> int {
    XfsMountContext* const MOUNT = source.mount();
    if (MOUNT == nullptr || destination.mount() == nullptr) return -EINVAL;
    if (MOUNT != destination.mount()) return -EXDEV;
    if (read_only || MOUNT->read_only) return -EROFS;
    XfsMetadataGuard metadata_guard(MOUNT, true, WOS_PERF_CALLSITE());
    int rc = xfs_validate_lookup_pair_locked(source, destination);
    if (rc != 0) return rc;
    XfsInode* const MOVED = source.target();
    XfsInode* const DISPLACED = destination.target();
    if (!source.target_existed() || MOVED == nullptr) return -ENOENT;
    bool const CROSS_PARENT = source.parent() != destination.parent();
    auto const DISPLACED_IDENTITY = DISPLACED != nullptr ? ker::mod::security::FileIdentity{DISPLACED->uid, DISPLACED->gid, DISPLACED->mode}
                                                         : ker::mod::security::FileIdentity{};
    rc = ker::mod::security::authorize_file_rename(credentials, {source.parent()->uid, source.parent()->gid, source.parent()->mode},
                                                   {MOVED->uid, MOVED->gid, MOVED->mode},
                                                   {destination.parent()->uid, destination.parent()->gid, destination.parent()->mode},
                                                   DISPLACED != nullptr ? &DISPLACED_IDENTITY : nullptr, CROSS_PARENT);
    if (rc != 0) return rc;
    if (source.root_target() || destination.root_target() || MOVED->ino == MOUNT->root_ino ||
        (DISPLACED != nullptr && DISPLACED->ino == MOUNT->root_ino))
        return -EBUSY;
    auto special = [](const XfsLookupHandle& lookup) {
        return (lookup.leaf_length() == 1 && lookup.leaf()[0] == '.') ||
               (lookup.leaf_length() == 2 && lookup.leaf()[0] == '.' && lookup.leaf()[1] == '.');
    };
    if (special(source) || special(destination)) return -EINVAL;
    bool const DIRECTORY = xfs_inode_isdir(MOVED);
    bool const REPLACE_DIRECTORY = DISPLACED != nullptr && xfs_inode_isdir(DISPLACED);
    if (source_requires_directory && !DIRECTORY) return -ENOTDIR;
    if (destination_requires_directory) {
        if (DISPLACED == nullptr) return -ENOENT;
        if (!REPLACE_DIRECTORY) return -ENOTDIR;
        if (!DIRECTORY) return -EISDIR;
    }
    if (DISPLACED == MOVED) {
        if (statbuf != nullptr) fill_stat(MOVED, statbuf);
        if (namespace_changed != nullptr) *namespace_changed = false;
        if (displaced_hardlink_count_changed != nullptr) *displaced_hardlink_count_changed = false;
        return 0;
    }
    if (DISPLACED != nullptr) {
        if (DIRECTORY && !REPLACE_DIRECTORY) return -ENOTDIR;
        if (!DIRECTORY && REPLACE_DIRECTORY) return -EISDIR;
        if (DISPLACED->nlink == 0) return -EIO;
        if (REPLACE_DIRECTORY) {
            RmdirDirectoryScan scan{.directory = DISPLACED};
            rc = xfs_dir_iterate(DISPLACED, scan_indexed_directory_entries, &scan);
            if (rc < 0 || scan.status != 0) return rc < 0 ? rc : scan.status;
            if (scan.indexed_entry_found) return -ENOTEMPTY;
        }
    }
    if (DIRECTORY) {
        XfsDirEntry parent{};
        rc = xfs_dir_lookup_authoritative(MOVED, "..", 2, &parent);
        if (rc != 0) return rc;
        if (parent.ino != source.parent()->ino) return -EIO;
        xfs_ino_t ancestor = destination.parent()->ino;
        for (unsigned depth = 0;; ++depth) {
            if (ancestor == MOVED->ino) return -EINVAL;
            if (ancestor == MOUNT->root_ino) break;
            if (depth == 1024) return -ELOOP;
            auto* inode = xfs_inode_read_known_allocated(MOUNT, ancestor);
            if (inode == nullptr) return -EIO;
            rc = xfs_inode_isdir(inode) ? xfs_dir_lookup_authoritative(inode, "..", 2, &parent) : -ENOTDIR;
            xfs_inode_release_metadata_locked(inode);
            if (rc != 0) return rc;
            if (parent.ino == ancestor || parent.ino == NULLFSINO) return -EIO;
            ancestor = parent.ino;
        }
    }
    int const OLD_DELTA = DIRECTORY && CROSS_PARENT ? -1 : 0;
    int const NEW_DELTA = (DIRECTORY && CROSS_PARENT ? 1 : 0) - (REPLACE_DIRECTORY ? 1 : 0);
    if ((OLD_DELTA < 0 && source.parent()->nlink <= 2) || (NEW_DELTA < 0 && destination.parent()->nlink <= 2)) return -EIO;
    if (NEW_DELTA > 0 && destination.parent()->nlink == UINT32_MAX) return -EMLINK;
    bool const LINKS_CHANGED = DISPLACED != nullptr && !REPLACE_DIRECTORY && DISPLACED->nlink > 1;
    XfsTransaction* tp = xfs_trans_alloc(MOUNT);
    if (tp == nullptr) return -ENOMEM;
    auto fail = [tp](int error) {
        xfs_trans_cancel(tp);
        return error;
    };
    for (auto* inode : {MOVED, DISPLACED, source.parent(), destination.parent()}) {
        if (inode != nullptr) {
            rc = xfs_trans_capture_inode(tp, inode);
            if (rc != 0) return fail(rc);
        }
    }
    if (DISPLACED != nullptr) {
        rc = xfs_dir_removename(destination.parent(), destination.leaf(), destination.leaf_length(), tp, DISPLACED->ino);
        if (rc != 0) return fail(rc);
        if (REPLACE_DIRECTORY)
            DISPLACED->nlink = 0;
        else
            DISPLACED->nlink--;
        DISPLACED->ctime = xfs_current_timestamp(DISPLACED);
        DISPLACED->dirty = true;
        xfs_trans_log_inode(tp, DISPLACED);
    }
    uint8_t const FTYPE = xfs_ftype_from_inode(MOVED);
    rc = xfs_dir_addname(destination.parent(), destination.leaf(), destination.leaf_length(), MOVED->ino, FTYPE, tp);
    if (rc != 0) return fail(rc);
    rc = xfs_dir_removename(source.parent(), source.leaf(), source.leaf_length(), tp, MOVED->ino);
    if (rc != 0) return fail(rc);
    if (DIRECTORY && CROSS_PARENT) {
        rc = xfs_dir_reparent(MOVED, source.parent()->ino, destination.parent()->ino, tp);
        if (rc != 0) return fail(rc);
    }
    if (OLD_DELTA < 0) source.parent()->nlink--;
    if (NEW_DELTA < 0)
        destination.parent()->nlink--;
    else if (NEW_DELTA > 0)
        destination.parent()->nlink++;
    xfs_stamp_inode_data_change(source.parent());
    xfs_stamp_inode_data_change(destination.parent());
    MOVED->ctime = xfs_current_timestamp(MOVED);
    MOVED->dirty = true;
    xfs_trans_log_inode(tp, MOVED);
    xfs_trans_log_inode(tp, source.parent());
    xfs_trans_log_inode(tp, destination.parent());
    rc = xfs_trans_commit(tp);
    if (rc != 0) return -EIO;
    if (DIRECTORY || REPLACE_DIRECTORY || source.path_length() == 0 || destination.path_length() == 0) {
        xfs_path_inode_cache_bump_generation();
        xfs_parent_path_cache_purge_all_for_mount(MOUNT);
    } else {
        xfs_path_inode_cache_invalidate_path(MOUNT, source.path(), source.path_length());
        xfs_path_inode_cache_invalidate_path(MOUNT, destination.path(), destination.path_length());
        xfs_path_inode_cache_store(MOUNT, destination.path(), destination.path_length(), MOVED->ino, FTYPE);
    }
    if (statbuf != nullptr) fill_stat(MOVED, statbuf);
    if (namespace_changed != nullptr) *namespace_changed = true;
    if (displaced_hardlink_count_changed != nullptr) *displaced_hardlink_count_changed = LINKS_CHANGED;
    return 0;
}

// ============================================================================
// Mount / init
// ============================================================================

auto xfs_vfs_init_device(dev::BlockDevice* device, bool read_only) -> XfsMountContext* {
    if (device == nullptr) {
        return nullptr;
    }

    XfsMountContext* ctx = nullptr;
    int ret = xfs_mount(device, read_only || dev::block_device_is_read_only(device), &ctx);
    if (ret != 0) {
        log::error("mount failed with error %d", ret);
        return nullptr;
    }

    if (xfs_log_needs_recovery(ctx)) {
        log::warn("journal remains dirty after log mount/recovery");
    }

    log::info("mounted successfully (%s)", ctx->read_only ? "read-only" : "read-write");
    return ctx;
}

void register_xfs() {
    xfs_icache_init();
    log::info("filesystem driver registered");
}

}  // namespace ker::vfs::xfs
