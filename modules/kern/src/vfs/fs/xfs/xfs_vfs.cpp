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
#include <new>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/swap.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sys/mutex.hpp>
#include <vfs/buffer_cache.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/fs/xfs/xfs_alloc.hpp>
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

#include "dev/block_device.hpp"
#include "platform/dbg/dbg.hpp"
#ifdef XFS_BENCH
#include <atomic>

#include "platform/tsc/tsc.hpp"
#endif

namespace ker::vfs::xfs {

using log = ker::mod::dbg::logger<"xfs">;

namespace {

constexpr uint32_t XFS_SLOW_TRACE_US = 2048;
constexpr size_t XFS_DIRECT_READ_MIN_BYTES = size_t{4} * 1024;
constexpr size_t XFS_DIRECT_READ_BATCH_MAX_BYTES = size_t{1024} * 1024;
constexpr uint64_t XFS_NSEC_PER_SEC = 1000000000ULL;
constexpr int64_t XFS_BIGTIME_EPOCH_OFFSET = (1LL << 31);
// Maximum blocks to allocate per metadata transaction.
// Each transaction covers one contiguous extent allocation, so the actual
// number of transactions for a sequential write is (total_blocks / batch).
constexpr xfs_extlen_t XFS_WRITE_BATCH_BLOCKS = 65536;  // up to 256 MiB per transaction
constexpr size_t XFS_BUFFERED_WRITE_BATCH_MAX_BYTES = size_t{4} * 1024 * 1024;
constexpr size_t XFS_DIRTY_THROTTLE_INTERVAL_BYTES = size_t{4} * 1024 * 1024;
constexpr size_t XFS_DIRECT_FRESH_PARTIAL_MAX_BYTES = 4096;
constexpr size_t XFS_STREAM_PREALLOC_TRIGGER_BYTES = size_t{512} * 1024;
constexpr xfs_extlen_t XFS_STREAM_PREALLOC_BLOCKS = 1024;  // 4 MiB
constexpr uint32_t XFS_STREAM_PREALLOC_EXTENT_MARGIN = 8;
constexpr size_t XFS_PARENT_PATH_CACHE_PATH_MAX = 512;
constexpr size_t XFS_PARENT_PATH_CACHE_SET_COUNT = 1024;
constexpr size_t XFS_PARENT_PATH_CACHE_WAYS = 4;
static_assert((XFS_PARENT_PATH_CACHE_SET_COUNT & (XFS_PARENT_PATH_CACHE_SET_COUNT - 1)) == 0);

// ============================================================================
// Per-open-file state
// ============================================================================

struct XfsFileData {
    XfsMountContext* mount;
    XfsInode* inode;  // reference-counted inode
};

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
    std::array<XfsParentPathCacheEntry, XFS_PARENT_PATH_CACHE_WAYS> ways{};
};

class XfsMetadataGuard {
   public:
    explicit XfsMetadataGuard(XfsMountContext* ctx, bool active = true) : ctx(active ? ctx : nullptr) {
        if (this->ctx != nullptr) {
            this->ctx->metadata_lock.lock();
            locked = true;
        }
    }

    ~XfsMetadataGuard() {
        if (ctx != nullptr && locked) {
            ctx->metadata_lock.unlock();
        }
    }

    XfsMetadataGuard(const XfsMetadataGuard&) = delete;
    XfsMetadataGuard(XfsMetadataGuard&&) = delete;
    auto operator=(const XfsMetadataGuard&) -> XfsMetadataGuard& = delete;
    auto operator=(XfsMetadataGuard&&) -> XfsMetadataGuard& = delete;

    void lock() {
        if (ctx != nullptr && !locked) {
            ctx->metadata_lock.lock();
            locked = true;
        }
    }

    void unlock() {
        if (ctx != nullptr && locked) {
            ctx->metadata_lock.unlock();
            locked = false;
        }
    }

   private:
    XfsMountContext* ctx;
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
    return task != nullptr ? static_cast<uint32_t>(task->cpu) : 0U;
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
                                 uint32_t block_log, bool extent_pressure, bool /*sequential_append*/) -> xfs_extlen_t {
    size_t const BLOCKS_NEEDED = (block_off + remaining_bytes + block_size - 1) >> block_log;
    auto desired_blocks = static_cast<xfs_filblks_t>(BLOCKS_NEEDED);
    if ((write_pos >= XFS_STREAM_PREALLOC_TRIGGER_BYTES || extent_pressure) && hole_blocks > desired_blocks) {
        desired_blocks = std::max(desired_blocks, static_cast<xfs_filblks_t>(XFS_STREAM_PREALLOC_BLOCKS));
    }
    auto alloc_blocks = static_cast<xfs_extlen_t>(std::min(hole_blocks, desired_blocks));
    alloc_blocks = std::min(alloc_blocks, XFS_WRITE_BATCH_BLOCKS);
    return alloc_blocks == 0 ? 1 : alloc_blocks;
}

auto xfs_mapped_append_can_zero_without_read(size_t write_pos, uint64_t file_size, size_t block_size) -> bool {
    if (block_size == 0 || write_pos != file_size) {
        return false;
    }
    return (write_pos % block_size) == 0;
}

auto xfs_direct_read_batch_max_bytes(size_t block_size) -> size_t {
    if (block_size == 0) {
        return XFS_DIRECT_READ_BATCH_MAX_BYTES;
    }
    size_t const MAX_BLOCKS = std::max<size_t>(1, XFS_DIRECT_READ_BATCH_MAX_BYTES / block_size);
    return MAX_BLOCKS * block_size;
}

void xfs_set_sequential_alloc_hint(XfsInode* ip, XfsMountContext* ctx, xfs_fileoff_t file_block, XfsAllocReq* req) {
    if (ip == nullptr || ctx == nullptr || req == nullptr || ip->data_fork.format != XFS_DINODE_FMT_EXTENTS) {
        return;
    }

    XfsIforkExtents const& extents = ip->data_fork.extents;
    if (extents.list == nullptr || extents.count == 0) {
        return;
    }

    for (uint32_t i = extents.count; i > 0; --i) {
        XfsBmbtIrec const& ext = extents.list[i - 1];
        if (ext.br_startoff + ext.br_blockcount != file_block) {
            continue;
        }

        xfs_fsblock_t const NEXT_FSB = ext.br_startblock + ext.br_blockcount;
        xfs_agnumber_t const AGNO = xfs_ag_number(NEXT_FSB, ctx->ag_blk_log);
        xfs_agblock_t const AGBNO = xfs_ag_block(NEXT_FSB, ctx->ag_blk_log);
        if (AGNO < ctx->ag_count && AGBNO < ctx->ag_blocks) {
            req->agno = AGNO;
            req->agbno = AGBNO;
        }
        return;
    }
}

auto xfs_truncate_zero_resets_data(uint64_t old_size, uint64_t nblocks) -> bool { return old_size != 0 || nblocks != 0; }

auto xfs_close_should_trim_prealloc(int open_flags) -> bool {
    if ((open_flags & 3) != 0) {
        return true;
    }
    return (open_flags & (ker::vfs::O_CREAT | ker::vfs::O_TRUNC)) != 0;
}

void xfs_stamp_new_inode(XfsInode* ip) {
    if (ip == nullptr) {
        return;
    }
    uint64_t const NOW = xfs_current_timestamp(ip);
    ip->atime = NOW;
    ip->mtime = NOW;
    ip->ctime = NOW;
    ip->crtime = NOW;
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

class XfsMetadataUnlockedScope {
   public:
    explicit XfsMetadataUnlockedScope(XfsMetadataGuard& guard) : guard(guard) { guard.unlock(); }
    ~XfsMetadataUnlockedScope() { guard.lock(); }

    XfsMetadataUnlockedScope(const XfsMetadataUnlockedScope&) = delete;
    XfsMetadataUnlockedScope(XfsMetadataUnlockedScope&&) = delete;
    auto operator=(const XfsMetadataUnlockedScope&) -> XfsMetadataUnlockedScope& = delete;
    auto operator=(XfsMetadataUnlockedScope&&) -> XfsMetadataUnlockedScope& = delete;

   private:
    XfsMetadataGuard& guard;
};

std::array<XfsParentPathCacheSet, XFS_PARENT_PATH_CACHE_SET_COUNT> g_xfs_parent_path_cache{};
ker::mod::sys::Mutex g_xfs_parent_path_cache_lock;
std::atomic<uint64_t> g_xfs_parent_path_cache_clock{0};

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

auto xfs_parent_path_cache_lookup_ino(XfsMountContext* ctx, const char* path, size_t path_len, xfs_ino_t* ino_out) -> bool {
    if (ctx == nullptr || path == nullptr || ino_out == nullptr || path_len == 0 || path_len >= XFS_PARENT_PATH_CACHE_PATH_MAX) {
        return false;
    }

    uint64_t const HASH = xfs_parent_path_cache_hash(ctx, path, path_len);
    auto& set = g_xfs_parent_path_cache[HASH & (XFS_PARENT_PATH_CACHE_SET_COUNT - 1)];

    ker::mod::sys::MutexGuard guard(g_xfs_parent_path_cache_lock);
    for (auto& entry : set.ways) {
        if (!entry.valid || entry.mount != ctx || entry.hash != HASH || entry.path_len != path_len) {
            continue;
        }
        if (std::memcmp(entry.path.data(), path, path_len + 1) != 0) {
            continue;
        }
        entry.last_used = g_xfs_parent_path_cache_clock.fetch_add(1, std::memory_order_relaxed) + 1;
        *ino_out = entry.ino;
        return true;
    }
    return false;
}

void xfs_parent_path_cache_store(XfsMountContext* ctx, const char* path, size_t path_len, xfs_ino_t ino) {
    if (ctx == nullptr || path == nullptr || ino == NULLFSINO || path_len == 0 || path_len >= XFS_PARENT_PATH_CACHE_PATH_MAX) {
        return;
    }

    uint64_t const HASH = xfs_parent_path_cache_hash(ctx, path, path_len);
    uint64_t const USE_STAMP = g_xfs_parent_path_cache_clock.fetch_add(1, std::memory_order_relaxed) + 1;
    auto& set = g_xfs_parent_path_cache[HASH & (XFS_PARENT_PATH_CACHE_SET_COUNT - 1)];

    ker::mod::sys::MutexGuard guard(g_xfs_parent_path_cache_lock);
    XfsParentPathCacheEntry* victim = &set.ways.front();
    for (auto& entry : set.ways) {
        if (entry.valid && entry.mount == ctx && entry.hash == HASH && entry.path_len == path_len &&
            std::memcmp(entry.path.data(), path, path_len + 1) == 0) {
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

    victim->path.fill('\0');
    std::memcpy(victim->path.data(), path, path_len + 1);
    victim->mount = ctx;
    victim->path_len = path_len;
    victim->ino = ino;
    victim->hash = HASH;
    victim->last_used = USE_STAMP;
    victim->valid = true;
}

void xfs_parent_path_cache_purge_all_for_mount(XfsMountContext* ctx) {
    if (ctx == nullptr) {
        return;
    }

    ker::mod::sys::MutexGuard guard(g_xfs_parent_path_cache_lock);
    for (auto& set : g_xfs_parent_path_cache) {
        for (auto& entry : set.ways) {
            if (entry.valid && entry.mount == ctx) {
                entry.valid = false;
            }
        }
    }
}

auto xfs_commit_dirty_inode(XfsMountContext* ctx, XfsInode* ip, bool trim_eof_prealloc = false) -> int {
    if (ctx == nullptr || ip == nullptr || ctx->read_only || (!ip->dirty && !trim_eof_prealloc)) {
        return 0;
    }

    uint64_t const STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_ILOG);
    XfsTransaction* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) {
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_ILOG, STARTED_US, -ENOMEM, 0);
        return -ENOMEM;
    }
    if (trim_eof_prealloc && !xfs_inode_isdir(ip)) {
        int const TRIM_RET = xfs_inode_trim_data_to_size(ip, tp, ip->size);
        if (TRIM_RET != 0) {
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

auto xfs_block_read_with_retry(dev::BlockDevice* bdev, uint64_t block_no, size_t count, uint8_t* buffer) -> int {
    constexpr int MAX_ATTEMPTS = 3;
    uint64_t const STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::DIRECT_READ);
    int rc = -EIO;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; ++attempt) {
        int const RC = dev::block_read(bdev, block_no, count, buffer);
        if (RC == 0) {
            rc = 0;
            break;
        }
        if (attempt < MAX_ATTEMPTS) {
            log::warn("transient extent read failure on %s block=%llu count=%zu rc=%d attempt=%d/%d", bdev->name.data(),
                      static_cast<unsigned long long>(block_no), count, RC, attempt, MAX_ATTEMPTS);
            ker::mod::sched::kern_yield();
            continue;
        }
        log::warn("extent read failed on %s block=%llu count=%zu rc=%d after %d attempts", bdev->name.data(),
                  static_cast<unsigned long long>(block_no), count, RC, MAX_ATTEMPTS);
        rc = RC;
    }
    perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::DIRECT_READ, STARTED_US, rc, count * static_cast<uint64_t>(bdev->block_size));
    return rc;
}

auto xfs_direct_write_fresh_blocks(XfsMountContext* ctx, xfs_fsblock_t fsbno, const uint8_t* src, size_t bytes) -> int {
    if (ctx == nullptr || ctx->device == nullptr || src == nullptr || bytes == 0 || ctx->block_size == 0 || ctx->device->block_size == 0) {
        return -EINVAL;
    }

    bool const INVALID_BLOCK_RATIO = ctx->block_size < ctx->device->block_size || (ctx->block_size % ctx->device->block_size) != 0;
    bool const INVALID_WRITE_SIZE = (bytes % ctx->block_size) != 0 || (bytes % ctx->device->block_size) != 0;
    if (INVALID_BLOCK_RATIO || INVALID_WRITE_SIZE) {
        return -EINVAL;
    }

    uint64_t const DEV_BLOCK = xfs_fsblock_to_dev_block(ctx, fsbno);
    size_t const DEV_COUNT = bytes / ctx->device->block_size;

    uint64_t const STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::DIRECT_WRITE);
    int const RC = dev::block_write(ctx->device, DEV_BLOCK, DEV_COUNT, src);
    perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::DIRECT_WRITE, STARTED_US, RC, bytes);
    if (RC == 0) {
        discard_bdev_range(ctx->device, DEV_BLOCK, DEV_COUNT);
    }
    return RC;
}

auto xfs_direct_write_fresh_partial_block(XfsMountContext* ctx, xfs_fsblock_t fsbno, size_t block_off, const uint8_t* src, size_t bytes)
    -> int {
    if (ctx == nullptr || src == nullptr || ctx->block_size == 0 || block_off >= ctx->block_size || bytes == 0 ||
        bytes > ctx->block_size - block_off || ctx->block_size > XFS_DIRECT_FRESH_PARTIAL_MAX_BYTES) {
        return -EINVAL;
    }

    std::array<uint8_t, XFS_DIRECT_FRESH_PARTIAL_MAX_BYTES> block{};
    std::memcpy(block.data() + block_off, src, bytes);
    return xfs_direct_write_fresh_blocks(ctx, fsbno, block.data(), ctx->block_size);
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

auto walk_path(XfsMountContext* ctx, const char* path) -> XfsInode* {
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
        int const RET = xfs_dir_lookup(ip, comp_start, namelen, &de);
        if (RET != 0) {
            xfs_inode_release(ip);
            return nullptr;
        }

        // Release parent, read child
        xfs_inode_release(ip);
        ip = xfs_inode_read(ctx, de.ino);
        if (ip == nullptr) {
            return nullptr;
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
        if (xfd->inode != nullptr) {
            XfsMetadataGuard metadata_guard(xfd->mount);
            {
                ker::mod::sys::MutexGuard guard(xfd->inode->io_lock);
                close_result = xfs_commit_dirty_inode(xfd->mount, xfd->inode, xfs_close_should_trim_prealloc(f->open_flags));
            }
            xfs_inode_release(xfd->inode);
        }
        delete xfd;
        f->private_data = nullptr;
    }
    return close_result;
}

auto xfs_vfs_read(File* f, void* buf, size_t count, size_t offset) -> ssize_t {
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
            size_t const READ_BATCH_MAX_BYTES = xfs_direct_read_batch_max_bytes(ctx->block_size);
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
            // Full-block read. Large reads use direct I/O, then overlay any
            // dirty cached intervals so overlapping size-keyed buffers remain
            // authoritative over the disk snapshot.
            size_t const FSB_COUNT = chunk >> ctx->block_log;

            auto read_cached_blocks = [&]() -> bool {
                bool ok = true;
                for (size_t i = 0; i < FSB_COUNT; ++i) {
                    BufHead* bp = xfs_buf_read(ctx, DISK_BLOCK + static_cast<xfs_fsblock_t>(i));
                    if (bp == nullptr) {
                        ok = false;
                        break;
                    }
                    std::memcpy(dst + total_read + (i << ctx->block_log), bp->data, ctx->block_size);
                    brelse(bp);
                }
                return ok;
            };

            bool read_from_cache = chunk < XFS_DIRECT_READ_MIN_BYTES;
            uint64_t dev_block = 0;
            size_t dev_count = 0;
            if (!read_from_cache) {
                dev_block = xfs_fsblock_to_dev_block(ctx, DISK_BLOCK);
                dev_count = xfs_fsb_to_dev_count(ctx, static_cast<xfs_filblks_t>(FSB_COUNT));
            }

#ifdef XFS_BENCH
            acc_io += ker::mod::tsc::get_ns() - t0;
#endif
            if (read_from_cache) {
                bool const OK = read_cached_blocks();
                perf_accounted_us += perf_record_xfs_stage_elapsed(ker::mod::perf::WkiPerfLocalXfsOp::READ_IO, PERF_IO_STARTED_US,
                                                                   OK ? 0 : -EIO, OK ? chunk : 0);
                if (!OK) {
                    return finish_read((total_read > 0) ? static_cast<ssize_t>(total_read) : -EIO);
                }
                uint64_t const PERF_COPY_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::READ_COPY);
                perf_accounted_us +=
                    perf_record_xfs_stage_elapsed(ker::mod::perf::WkiPerfLocalXfsOp::READ_COPY, PERF_COPY_STARTED_US, 0, chunk);
            } else {
                uint64_t const PERF_COPY_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::READ_COPY);
                bool const COPIED_CACHED = copy_cached_bdev_range_if_complete(ctx->device, dev_block, dev_count, dst + total_read);
                if (COPIED_CACHED) {
                    perf_accounted_us +=
                        perf_record_xfs_stage_elapsed(ker::mod::perf::WkiPerfLocalXfsOp::READ_COPY, PERF_COPY_STARTED_US, 0, chunk);
                } else {
                    int const RC = xfs_block_read_with_retry(ctx->device, dev_block, dev_count, dst + total_read);
                    if (RC != 0) {
                        perf_accounted_us +=
                            perf_record_xfs_stage_elapsed(ker::mod::perf::WkiPerfLocalXfsOp::READ_IO, PERF_IO_STARTED_US, RC, chunk);
                        return finish_read((total_read > 0) ? static_cast<ssize_t>(total_read) : -EIO);
                    }
                    perf_accounted_us +=
                        perf_record_xfs_stage_elapsed(ker::mod::perf::WkiPerfLocalXfsOp::READ_IO, PERF_IO_STARTED_US, 0, chunk);
                    bool const OVERLAYED = copy_dirty_bdev_range(ctx->device, dev_block, dev_count, dst + total_read);
                    if (OVERLAYED) {
                        perf_accounted_us +=
                            perf_record_xfs_stage_elapsed(ker::mod::perf::WkiPerfLocalXfsOp::READ_COPY, PERF_COPY_STARTED_US, 0, chunk);
                    }
                }
            }
        } else {
            // Partial or unaligned - fall back to single cached block.
            chunk = std::min(ctx->block_size - BLOCK_OFF, remaining);
            BufHead* bp = xfs_buf_read(ctx, DISK_BLOCK);
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

auto xfs_vfs_write_locked(File* f, const void* buf, size_t count, size_t offset, XfsMetadataGuard& metadata_guard) -> ssize_t {
    if (f == nullptr || buf == nullptr) {
        return -EINVAL;
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

    if (xfs_inode_isdir(ip)) {
        return -EISDIR;
    }

    if (count == 0) {
        return 0;
    }

    const auto* src = static_cast<const uint8_t*>(buf);
    size_t total_written = 0;
    int write_error = 0;
    uint64_t const PERF_WRITE_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::WRITE);
    auto finish_write = [&](ssize_t result) -> ssize_t {
        uint64_t const BYTES = result > 0 ? static_cast<uint64_t>(result) : static_cast<uint64_t>(total_written);
        int32_t const STATUS = result < 0 ? static_cast<int32_t>(result) : 0;
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
        size_t bytes_since_pressure_check = 0;
        bool ok = true;

        size_t const MAX_BATCH_BLOCKS = std::max<size_t>(1, XFS_BUFFERED_WRITE_BATCH_MAX_BYTES >> ctx->block_log);
        auto account_dirty_write = [&](size_t bytes_written) {
            bytes_since_pressure_check += bytes_written;
            if (bytes_since_pressure_check >= XFS_DIRTY_THROTTLE_INTERVAL_BYTES) {
                throttle_dirty_buffer_cache(ctx->device);
                bytes_since_pressure_check = 0;
            }
        };

        while (remaining_bytes > 0) {
            if (block_off == 0 && remaining_bytes >= ctx->block_size) {
                size_t full_blocks = remaining_bytes >> ctx->block_log;
                while (full_blocks > 0) {
                    size_t const BATCH_BLOCKS = std::min(full_blocks, MAX_BATCH_BLOCKS);
                    size_t const BATCH_BYTES = BATCH_BLOCKS << ctx->block_log;
                    if (fresh_allocation) {
                        int const RC = xfs_direct_write_fresh_blocks(ctx, current_disk_block, src + current_src_offset, BATCH_BYTES);
                        if (RC != 0) {
                            ok = false;
                            break;
                        }
                    } else {
                        uint64_t const DEV_BLOCK = xfs_fsblock_to_dev_block(ctx, current_disk_block);
                        size_t const DEV_COUNT = xfs_fsb_to_dev_count(ctx, static_cast<xfs_filblks_t>(BATCH_BLOCKS));
                        BufHead* bp = bget_multi(ctx->device, DEV_BLOCK, DEV_COUNT);
                        if (bp == nullptr) {
                            ok = false;
                            break;
                        }

                        std::memcpy(bp->data, src + current_src_offset, BATCH_BYTES);
                        bdirty(bp);
                        brelse(bp);
                        account_dirty_write(BATCH_BYTES);
                    }

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
            if (fresh_allocation && ctx->block_size <= XFS_DIRECT_FRESH_PARTIAL_MAX_BYTES) {
                int const RC = xfs_direct_write_fresh_partial_block(ctx, current_disk_block, block_off, src + current_src_offset, CHUNK);
                if (RC != 0) {
                    ok = false;
                    break;
                }
            } else {
                BufHead* bp = fresh_allocation ? xfs_buf_get(ctx, current_disk_block) : xfs_buf_read(ctx, current_disk_block);
                if (bp == nullptr) {
                    ok = false;
                    break;
                }

                if (fresh_allocation) {
                    std::memset(bp->data, 0, ctx->block_size);
                }
                std::memcpy(bp->data + block_off, src + current_src_offset, CHUNK);
                bdirty(bp);
                brelse(bp);
                account_dirty_write(CHUNK);
            }

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
            bool const SEQUENTIAL_APPEND = WRITE_POS == ip->size && WRITE_POS != 0;
            xfs_extlen_t const HOLE_BLOCKS =
                xfs_hole_write_alloc_blocks(WRITE_POS, BLOCK_OFF, REMAINING_BYTES, bmap.blockcount, ctx->block_size, ctx->block_log,
                                            EXTENT_PRESSURE, SEQUENTIAL_APPEND);

#ifdef XFS_BENCH
            t0 = ker::mod::tsc::get_ns();
#endif
            uint64_t const PERF_ALLOC_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_ALLOC);
            XfsTransaction* tp = xfs_trans_alloc(ctx);
            if (tp == nullptr) {
                perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_ALLOC, PERF_ALLOC_STARTED_US, -ENOMEM, 0);
                write_error = -ENOMEM;
                break;
            }

            xfs_agnumber_t const PREF_AG = xfs_ino_ag(ip->ino, ctx->agino_log);

            XfsAllocReq req{};
            req.agno = PREF_AG;
            req.agbno = 0;
            req.minlen = 1;
            req.maxlen = HOLE_BLOCKS;
            req.alignment = 0;
            xfs_set_sequential_alloc_hint(ip, ctx, file_block, &req);

            XfsAllocResult alloc_result{};
            ret = xfs_alloc_extent(ctx, tp, req, &alloc_result);
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

            // Write the data blocks covered by this allocation, using direct
            // I/O for fresh full blocks and bounded fresh partial blocks.
            uint64_t const SIZE_BEFORE_ALLOC = ip->size;
            bool const DIRTY_BEFORE_ALLOC = ip->dirty;
            size_t const EXTENT_BYTES = static_cast<size_t>(alloc_result.len) << ctx->block_log;
            size_t const WRITE_END = offset + count;
            size_t const EXTENT_START = static_cast<size_t>(file_block) << ctx->block_log;
            size_t const EXTENT_END = EXTENT_START + EXTENT_BYTES;

            // Compute the slice of this extent actually covered by [offset, offset+count).
            size_t const SLICE_START = std::max(EXTENT_START, offset + total_written);
            size_t const SLICE_END = std::min(EXTENT_END, WRITE_END);
            size_t const SLICE_BYTES = (SLICE_END > SLICE_START) ? (SLICE_END - SLICE_START) : 0;

            ip->nblocks += alloc_result.len;
            if (SLICE_BYTES > 0 && SLICE_END > ip->size) {
                ip->size = SLICE_END;
            }
            ip->dirty = true;

            // Log inode into this allocation transaction so extent updates and
            // EOF growth share one journal commit for create/write/close-heavy
            // workloads.
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
                if (SLICE_BYTES > 0) {
                    size_t const SLICE_BLOCK_OFF = SLICE_START - EXTENT_START;
                    bool wrote = false;
                    {
                        XfsMetadataUnlockedScope unlocked(metadata_guard);
                        wrote = write_extent_data(DISK_BLOCK + (SLICE_BLOCK_OFF >> ctx->block_log), SLICE_BLOCK_OFF & (ctx->block_size - 1),
                                                  SLICE_BYTES, SLICE_START - offset, true);
                    }
                    bool const WROTE = wrote;
                    if (!WROTE) {
                        ip->size = SIZE_BEFORE_ALLOC;
                        ip->dirty = DIRTY_BEFORE_ALLOC || ip->dirty;
                        static_cast<void>(xfs_commit_dirty_inode(ctx, ip, true));
#ifdef XFS_BENCH
                        acc_io += ker::mod::tsc::get_ns() - t0;
#endif
                        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_IO, PERF_IO_STARTED_US, -EIO, 0);
                        write_error = -EIO;
                        goto write_done;
                    }
                    total_written += SLICE_BYTES;
                }
                perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_IO, PERF_IO_STARTED_US, 0, SLICE_BYTES);
#ifdef XFS_BENCH
                acc_io += ker::mod::tsc::get_ns() - t0;
#endif
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

#ifdef XFS_BENCH
            t0 = ker::mod::tsc::get_ns();
#endif
            uint64_t const PERF_IO_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_IO);
            bool wrote = false;
            {
                XfsMetadataUnlockedScope unlocked(metadata_guard);
                wrote = write_extent_data(DISK_BLOCK, BLOCK_OFF, CHUNK, total_written, APPEND_BLOCK_HAS_NO_OLD_BYTES);
            }
            bool const WROTE = wrote;
            if (!WROTE) {
#ifdef XFS_BENCH
                acc_io += ker::mod::tsc::get_ns() - t0;
#endif
                perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_IO, PERF_IO_STARTED_US, -EIO, 0);
                write_error = -EIO;
                break;
            }
            perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_IO, PERF_IO_STARTED_US, 0, CHUNK);
#ifdef XFS_BENCH
            acc_io += ker::mod::tsc::get_ns() - t0;
#endif

            total_written += CHUNK;
        }
    }

write_done:
    if (total_written == 0) {
        return finish_write(write_error != 0 ? write_error : -EIO);
    }

    // Update file size if we wrote past the end
    if (offset + total_written > ip->size) {
        ip->size = offset + total_written;
        ip->dirty = true;
    }

    throttle_dirty_buffer_cache(ctx->device);

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
    if (f == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }

    XfsMetadataGuard metadata_guard(xfd->mount);
    ker::mod::sys::MutexGuard guard(xfd->inode->io_lock);
    return xfs_vfs_write_locked(f, buf, count, offset, metadata_guard);
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
    DirEntry* entry;       // output entry to fill
    size_t target_cookie;  // next stable cookie requested by VFS
    size_t target_index;   // legacy dense index requested by direct callers
    size_t current_index;
    bool use_cookie;
    bool found;
};

auto readdir_callback(const XfsDirEntry* xde, void* ctx_ptr) -> int {
    auto* rctx = static_cast<ReaddirCtx*>(ctx_ptr);
    bool const MATCH = rctx->use_cookie ? xde->cookie >= rctx->target_cookie : rctx->current_index == rctx->target_index;
    if (MATCH) {
        // Fill the VFS DirEntry
        rctx->entry->d_ino = xde->ino;
        rctx->entry->d_off = (rctx->current_index < 2 && !rctx->use_cookie) ? rctx->current_index + 1 : xde->cookie + 1;
        rctx->entry->d_reclen = sizeof(DirEntry);

        // Convert XFS ftype to VFS d_type
        switch (xde->ftype) {
            case XFS_DIR3_FT_REG_FILE:
                rctx->entry->d_type = DT_REG;
                break;
            case XFS_DIR3_FT_DIR:
                rctx->entry->d_type = DT_DIR;
                break;
            case XFS_DIR3_FT_CHRDEV:
                rctx->entry->d_type = DT_CHR;
                break;
            case XFS_DIR3_FT_BLKDEV:
                rctx->entry->d_type = DT_BLK;
                break;
            case XFS_DIR3_FT_FIFO:
                rctx->entry->d_type = DT_FIFO;
                break;
            case XFS_DIR3_FT_SOCK:
                rctx->entry->d_type = DT_SOCK;
                break;
            case XFS_DIR3_FT_SYMLINK:
                rctx->entry->d_type = DT_LNK;
                break;
            default:
                rctx->entry->d_type = DT_UNKNOWN;
                break;
        }

        // Copy name
        size_t const COPY_LEN = xde->namelen < DIRENT_NAME_MAX - 1 ? xde->namelen : DIRENT_NAME_MAX - 1;
        std::memcpy(rctx->entry->d_name.data(), xde->name.data(), COPY_LEN);
        rctx->entry->d_name.at(COPY_LEN) = '\0';

        rctx->found = true;
        return 1;  // stop iteration
    }
    rctx->current_index++;
    return 0;  // continue
}

auto xfs_vfs_readdir(File* f, DirEntry* entry, size_t index) -> int {
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

    XfsMetadataGuard metadata_guard(xfd->mount);

    ReaddirCtx ctx{};
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

    return ctx.found ? 0 : -1;  // -1 = no more entries
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

auto xfs_vfs_truncate(File* f, off_t length) -> int {
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

    XfsMetadataGuard metadata_guard(ctx);
    ker::mod::sys::MutexGuard guard(ip->io_lock);

    auto new_size = static_cast<uint64_t>(length);

    uint64_t const OLD_SIZE = ip->size;
    bool const ZERO_TRUNCATE_RESETS_DATA = new_size == 0 && xfs_truncate_zero_resets_data(OLD_SIZE, ip->nblocks);
    if (new_size == OLD_SIZE && !ZERO_TRUNCATE_RESETS_DATA) {
        return 0;  // no change
    }

    if (ZERO_TRUNCATE_RESETS_DATA) {
        if (OLD_SIZE != 0) {
            int const DISCARD_RET = discard_inode_data_buffers(ctx, ip, OLD_SIZE);
            if (DISCARD_RET < 0) {
                return DISCARD_RET;
            }
        }

        XfsTransaction* tp = xfs_trans_alloc(ctx);
        if (tp == nullptr) {
            return -ENOMEM;
        }
        int const TRUNC_RET = xfs_inode_truncate_data(ip, tp);
        if (TRUNC_RET != 0) {
            xfs_trans_cancel(tp);
            return TRUNC_RET;
        }
        ip->size = 0;
        xfs_trans_log_inode(tp, ip);
        int const RET = xfs_trans_commit(tp);
        return (RET == 0) ? 0 : -EIO;
    }

    if (new_size < OLD_SIZE) {
        XfsTransaction* tp = xfs_trans_alloc(ctx);
        if (tp == nullptr) {
            return -ENOMEM;
        }
        int const TRIM_RET = xfs_inode_trim_data_to_size(ip, tp, new_size);
        if (TRIM_RET != 0) {
            xfs_trans_cancel(tp);
            return TRIM_RET;
        }
        ip->size = new_size;
        ip->dirty = true;
        xfs_trans_log_inode(tp, ip);
        int const RET = xfs_trans_commit(tp);
        return (RET == 0) ? 0 : -EIO;
    }

    // Update the inode size
    ip->size = new_size;
    ip->dirty = true;

    XfsTransaction* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) {
        return -ENOMEM;
    }
    xfs_trans_log_inode(tp, ip);
    int const RET = xfs_trans_commit(tp);
    return (RET == 0) ? 0 : -EIO;
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

#ifdef WOS_SELFTEST
struct XfsDirectWriteSelftestState {
    size_t write_calls{};
    uint64_t last_block{};
    size_t last_count{};
    std::array<uint8_t, 4096> last_data{};
};

auto xfs_direct_write_selftest_write(dev::BlockDevice* bdev, uint64_t block, size_t count, const void* buffer) -> int {
    if (bdev == nullptr || buffer == nullptr || bdev->block_size == 0 || count > 4096 / bdev->block_size) {
        return -EIO;
    }

    auto* state = static_cast<XfsDirectWriteSelftestState*>(bdev->private_data);
    if (state == nullptr) {
        return -EIO;
    }

    state->write_calls++;
    state->last_block = block;
    state->last_count = count;
    std::memcpy(state->last_data.data(), buffer, count * bdev->block_size);
    return 0;
}
#endif

}  // anonymous namespace

auto get_xfs_fops() -> FileOperations* { return &xfs_fops; }

void xfs_parent_path_cache_purge_mount(XfsMountContext* ctx) { xfs_parent_path_cache_purge_all_for_mount(ctx); }

#ifdef WOS_SELFTEST
auto xfs_selftest_hole_write_alloc_blocks(size_t block_off, size_t remaining_bytes, xfs_filblks_t hole_blocks, size_t block_size,
                                          uint32_t block_log, size_t write_pos, bool sequential_append) -> xfs_extlen_t {
    return xfs_hole_write_alloc_blocks(write_pos, block_off, remaining_bytes, hole_blocks, block_size, block_log, false, sequential_append);
}

auto xfs_selftest_truncate_zero_resets_data(uint64_t old_size, uint64_t nblocks) -> bool {
    return xfs_truncate_zero_resets_data(old_size, nblocks);
}

auto xfs_selftest_close_should_trim_prealloc(int open_flags) -> bool { return xfs_close_should_trim_prealloc(open_flags); }

auto xfs_selftest_mapped_append_can_zero_without_read(size_t write_pos, uint64_t file_size, size_t block_size) -> bool {
    return xfs_mapped_append_can_zero_without_read(write_pos, file_size, block_size);
}

auto xfs_selftest_direct_read_batch_max_bytes(size_t block_size) -> size_t { return xfs_direct_read_batch_max_bytes(block_size); }

auto xfs_selftest_direct_fresh_write_discards_cache() -> bool {
    constexpr size_t DEVICE_BLOCK_SIZE = 512;
    constexpr size_t FS_BLOCK_SIZE = 4096;
    constexpr xfs_fsblock_t FSB = 8;
    constexpr xfs_fsblock_t PARTIAL_FSB = 9;
    constexpr size_t PARTIAL_OFF = 17;
    constexpr size_t PARTIAL_BYTES = 123;

    XfsDirectWriteSelftestState state{};
    dev::BlockDevice dev{};
    dev.block_size = DEVICE_BLOCK_SIZE;
    dev.total_blocks = 1024;
    dev.write_blocks = xfs_direct_write_selftest_write;
    dev.private_data = &state;

    XfsMountContext ctx{};
    ctx.device = &dev;
    ctx.block_size = FS_BLOCK_SIZE;
    ctx.block_log = 12;
    ctx.ag_blocks = 1024;
    ctx.ag_blk_log = 10;

    uint64_t const DEV_BLOCK = xfs_fsblock_to_dev_block(&ctx, FSB);
    size_t const DEV_COUNT = xfs_fsb_to_dev_count(&ctx, 1);
    BufHead* stale = bget_multi(&dev, DEV_BLOCK, DEV_COUNT);
    if (stale == nullptr) {
        return false;
    }
    std::memset(stale->data, 0xA5, stale->size);
    bdirty(stale);
    brelse(stale);
    if (!has_dirty_bdev_range(&dev, DEV_BLOCK, DEV_COUNT)) {
        invalidate_bdev(&dev);
        return false;
    }

    std::array<uint8_t, FS_BLOCK_SIZE> src{};
    for (size_t i = 0; i < src.size(); ++i) {
        src.at(i) = static_cast<uint8_t>(i & 0xFFU);
    }

    int const RC = xfs_direct_write_fresh_blocks(&ctx, FSB, src.data(), src.size());
    bool const OK = RC == 0 && state.write_calls == 1 && state.last_block == DEV_BLOCK && state.last_count == DEV_COUNT &&
                    std::memcmp(state.last_data.data(), src.data(), src.size()) == 0 && !has_dirty_bdev_range(&dev, DEV_BLOCK, DEV_COUNT);
    if (!OK) {
        invalidate_bdev(&dev);
        return false;
    }

    uint64_t const PARTIAL_DEV_BLOCK = xfs_fsblock_to_dev_block(&ctx, PARTIAL_FSB);
    BufHead* partial_stale = bget_multi(&dev, PARTIAL_DEV_BLOCK, DEV_COUNT);
    if (partial_stale == nullptr) {
        invalidate_bdev(&dev);
        return false;
    }
    std::memset(partial_stale->data, 0x5A, partial_stale->size);
    bdirty(partial_stale);
    brelse(partial_stale);
    if (!has_dirty_bdev_range(&dev, PARTIAL_DEV_BLOCK, DEV_COUNT)) {
        invalidate_bdev(&dev);
        return false;
    }

    state = {};
    std::array<uint8_t, PARTIAL_BYTES> partial_src{};
    for (size_t i = 0; i < partial_src.size(); ++i) {
        partial_src.at(i) = static_cast<uint8_t>(0x80U | (i & 0x7FU));
    }

    int const PARTIAL_RC = xfs_direct_write_fresh_partial_block(&ctx, PARTIAL_FSB, PARTIAL_OFF, partial_src.data(), partial_src.size());
    bool partial_ok = PARTIAL_RC == 0 && state.write_calls == 1 && state.last_block == PARTIAL_DEV_BLOCK && state.last_count == DEV_COUNT &&
                      !has_dirty_bdev_range(&dev, PARTIAL_DEV_BLOCK, DEV_COUNT);
    for (size_t i = 0; partial_ok && i < state.last_data.size(); ++i) {
        uint8_t const EXPECTED =
            (i >= PARTIAL_OFF && i < PARTIAL_OFF + PARTIAL_BYTES) ? partial_src.at(i - PARTIAL_OFF) : static_cast<uint8_t>(0);
        partial_ok = state.last_data.at(i) == EXPECTED;
    }

    invalidate_bdev(&dev);
    return partial_ok;
}

auto xfs_selftest_parent_path_cache() -> bool {
    XfsMountContext mount_a{};
    XfsMountContext mount_b{};
    constexpr const char* PATH = "toolchain/src/llvm-project";
    size_t const PATH_LEN = std::strlen(PATH);
    constexpr xfs_ino_t INO = 42;

    xfs_parent_path_cache_purge_all_for_mount(&mount_a);
    xfs_parent_path_cache_purge_all_for_mount(&mount_b);

    xfs_ino_t ino = NULLFSINO;
    bool ok = !xfs_parent_path_cache_lookup_ino(&mount_a, PATH, PATH_LEN, &ino);
    xfs_parent_path_cache_store(&mount_a, PATH, PATH_LEN, INO);
    ok = ok && xfs_parent_path_cache_lookup_ino(&mount_a, PATH, PATH_LEN, &ino) && ino == INO;
    ok = ok && !xfs_parent_path_cache_lookup_ino(&mount_b, PATH, PATH_LEN, &ino);
    xfs_parent_path_cache_purge_all_for_mount(&mount_a);
    ok = ok && !xfs_parent_path_cache_lookup_ino(&mount_a, PATH, PATH_LEN, &ino);
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

    XfsMetadataGuard metadata_guard(xfd->mount);
    ker::mod::sys::MutexGuard guard(xfd->inode->io_lock);
    auto const OFFSET = static_cast<size_t>(xfd->inode->size);
    ssize_t const RET = xfs_vfs_write_locked(f, buf, count, OFFSET, metadata_guard);
    if (RET >= 0 && offset_out != nullptr) {
        *offset_out = OFFSET;
    }
    return RET;
}

auto xfs_fsync(File* f) -> int {
    if (f == nullptr) {
        return -EBADF;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }
    XfsMetadataGuard metadata_guard(xfd->mount);
    ker::mod::sys::MutexGuard guard(xfd->inode->io_lock);
    int const DATA_RET = sync_inode_data_buffers(xfd->mount, xfd->inode, xfd->inode->size);
    int const INODE_RET = xfs_commit_dirty_inode(xfd->mount, xfd->inode);
    return (DATA_RET != 0) ? DATA_RET : INODE_RET;
}

auto xfs_sync_mount(XfsMountContext* ctx) -> int {
    if (ctx == nullptr || ctx->device == nullptr) {
        return -EINVAL;
    }

    XfsMetadataGuard metadata_guard(ctx);
    int const INODE_RET = xfs_icache_sync_dirty(ctx);
    int const BLOCK_RET = sync_blockdev(ctx->device);
    return (INODE_RET != 0) ? INODE_RET : BLOCK_RET;
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

    XfsMetadataGuard metadata_guard(ctx);
    ker::mod::sys::MutexGuard io_guard(ip->io_lock);
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

    *extents_out = extents;
    *extent_count_out = extent_count;
    return 0;
}

// ============================================================================
// Open path
// ============================================================================

namespace {

auto xfs_find_parent_and_name(const char* fs_path, XfsMountContext* ctx, XfsInode** parent_out, const char** name_out,
                              uint16_t* namelen_out) -> int {
    const char* last_slash = nullptr;
    for (const char* p = fs_path; *p != '\0'; p++) {
        if (*p == '/') {
            last_slash = p;
        }
    }

    XfsInode* parent_ip = nullptr;
    const char* name = nullptr;

    if (last_slash == nullptr || last_slash == fs_path) {
        parent_ip = xfs_root_inode_read(ctx);
        name = (last_slash == fs_path) ? fs_path + 1 : fs_path;
    } else {
        auto parent_len = static_cast<size_t>(last_slash - fs_path);
        char parent_path[512] = {};  // NOLINT
        if (parent_len >= sizeof(parent_path)) {
            return -ENAMETOOLONG;
        }
        std::memcpy(static_cast<char*>(parent_path), fs_path, parent_len);
        parent_path[parent_len] = '\0';
        xfs_ino_t cached_parent_ino = NULLFSINO;
        auto const* parent_path_chars = static_cast<const char*>(parent_path);
        if (xfs_parent_path_cache_lookup_ino(ctx, parent_path_chars, parent_len, &cached_parent_ino)) {
            parent_ip = xfs_inode_read(ctx, cached_parent_ino);
            if (parent_ip != nullptr && !xfs_inode_isdir(parent_ip)) {
                xfs_inode_release(parent_ip);
                parent_ip = nullptr;
            }
        }
        if (parent_ip == nullptr) {
            parent_ip = walk_path(ctx, parent_path_chars);
            if (parent_ip != nullptr && xfs_inode_isdir(parent_ip)) {
                xfs_parent_path_cache_store(ctx, parent_path_chars, parent_len, parent_ip->ino);
            }
        }
        name = last_slash + 1;
    }

    if (parent_ip == nullptr) {
        return -ENOENT;
    }
    if (!xfs_inode_isdir(parent_ip)) {
        xfs_inode_release(parent_ip);
        return -ENOTDIR;
    }

    uint16_t namelen = 0;
    for (const char* p = name; *p != '\0'; p++) {
        namelen++;
    }
    if (namelen == 0) {
        xfs_inode_release(parent_ip);
        return -EINVAL;
    }

    *parent_out = parent_ip;
    *name_out = name;
    *namelen_out = namelen;
    return 0;
}

auto xfs_lookup_with_cached_parent(const char* fs_path, XfsMountContext* ctx, XfsInode** ip_out) -> int {
    if (ip_out == nullptr) {
        return -EINVAL;
    }
    *ip_out = nullptr;
    if (fs_path == nullptr || fs_path[0] == '\0' || (fs_path[0] == '/' && fs_path[1] == '\0')) {
        *ip_out = xfs_root_inode_read(ctx);
        return (*ip_out != nullptr) ? 0 : -ENOENT;
    }

    XfsInode* parent_ip = nullptr;
    const char* filename = nullptr;
    uint16_t filename_len = 0;
    int const PARENT_RET = xfs_find_parent_and_name(fs_path, ctx, &parent_ip, &filename, &filename_len);
    if (PARENT_RET != 0) {
        return PARENT_RET == -ENAMETOOLONG ? -EAGAIN : PARENT_RET;
    }

    XfsDirEntry entry{};
    int const LOOKUP_RET = xfs_dir_lookup(parent_ip, filename, filename_len, &entry);
    xfs_inode_release(parent_ip);
    if (LOOKUP_RET != 0) {
        return LOOKUP_RET;
    }

    *ip_out = xfs_inode_read(ctx, entry.ino);
    return (*ip_out != nullptr) ? 0 : -ENOENT;
}

}  // namespace

auto xfs_open_path(const char* fs_path, int flags, int mode, XfsMountContext* ctx) -> File* {
    constexpr int O_CREAT_FLAG = 0100;

    if (ctx == nullptr) {
        return nullptr;
    }

    XfsMetadataGuard metadata_guard(ctx);

    XfsInode* ip = nullptr;
    bool used_create_lookup = false;
    bool create_missing = false;
    XfsInode* create_parent_ip = nullptr;
    const char* create_filename = nullptr;
    uint16_t create_filename_len = 0;

    if ((flags & O_CREAT_FLAG) != 0 && !ctx->read_only) {
        int parent_ret = xfs_find_parent_and_name(fs_path, ctx, &create_parent_ip, &create_filename, &create_filename_len);
        if (parent_ret == 0) {
            used_create_lookup = true;
            XfsDirEntry existing{};
            int const LOOKUP_RET = xfs_dir_lookup(create_parent_ip, create_filename, create_filename_len, &existing);
            if (LOOKUP_RET == 0) {
                xfs_inode_release(create_parent_ip);
                create_parent_ip = nullptr;
                ip = xfs_inode_read(ctx, existing.ino);
            } else if (LOOKUP_RET == -ENOENT) {
                create_missing = true;
            } else if (LOOKUP_RET != -ENOENT) {
                xfs_inode_release(create_parent_ip);
                return nullptr;
            }
        } else if (parent_ret != -EINVAL) {
            return nullptr;
        }
    }

    if (!used_create_lookup) {
        int const CACHED_OPEN_RET = xfs_lookup_with_cached_parent(fs_path, ctx, &ip);
        if (CACHED_OPEN_RET == -EAGAIN) {
            ip = walk_path(ctx, fs_path);
        }
    }

    if (create_missing) {
        // Allocate a new inode
        XfsTransaction* tp = xfs_trans_alloc(ctx);
        if (tp == nullptr) {
            xfs_inode_release(create_parent_ip);
            return nullptr;
        }

        // Apply default mode if not specified (standard POSIX: 0666 for files)
        int const FILE_MODE = (mode == 0) ? 0666 : mode;
        uint16_t const INODE_MODE = static_cast<uint16_t>(FILE_MODE & 0xFFF) | 0100000;  // S_IFREG | mode
        xfs_ino_t const NEW_INO = xfs_ialloc(ctx, tp, INODE_MODE);
        if (NEW_INO == NULLFSINO) {
            xfs_trans_cancel(tp);
            xfs_inode_release(create_parent_ip);
            return nullptr;
        }

        auto* new_inode = new (std::nothrow) XfsInode{};
        if (new_inode == nullptr) {
            xfs_trans_cancel(tp);
            xfs_inode_release(create_parent_ip);
            return nullptr;
        }
        new_inode->ino = NEW_INO;
        new_inode->mount = ctx;
        new_inode->agno = xfs_ino_ag(NEW_INO, ctx->agino_log);
        new_inode->agino = xfs_ag_ino(NEW_INO, ctx->agino_log);
        new_inode->mode = INODE_MODE;
        new_inode->nlink = 1;
        new_inode->data_fork.format = XFS_DINODE_FMT_EXTENTS;
        xfs_stamp_new_inode(new_inode);
        xfs_trans_log_inode(tp, new_inode);

        // Add directory entry
        int rc = xfs_dir_addname(create_parent_ip, create_filename, create_filename_len, NEW_INO, XFS_DIR3_FT_REG_FILE, tp);
        if (rc != 0) {
            xfs_trans_cancel(tp);
            delete new_inode;
            xfs_inode_release(create_parent_ip);
            return nullptr;
        }

        rc = xfs_trans_commit(tp);
        xfs_inode_release(create_parent_ip);
        create_parent_ip = nullptr;
        if (rc != 0) {
            delete new_inode;
            return nullptr;
        }

        // The transaction has just written this inode.  Cache and use the
        // in-memory copy instead of immediately re-reading it from disk.
        rc = xfs_inode_cache_new(new_inode);
        if (rc == 0) {
            ip = new_inode;
        } else {
            delete new_inode;
            if (rc != -EEXIST) {
                return nullptr;
            }
            ip = xfs_inode_read(ctx, NEW_INO);
            if (ip == nullptr) {
                return nullptr;
            }
        }
    }

    if (ip == nullptr) {
        return nullptr;
    }

    // Check for O_WRONLY/O_RDWR on a read-only mount
    int const ACCMODE = flags & 3;
    if (ACCMODE == 1 || ACCMODE == 2) {
        // Write access on read-only filesystem
        if (ctx->read_only) {
            xfs_inode_release(ip);
            return nullptr;
        }
    }

    // Allocate File handle
    auto* f = new (std::nothrow) File{};
    if (f == nullptr) {
        xfs_inode_release(ip);
        return nullptr;
    }

    auto* xfd = new XfsFileData;
    xfd->mount = ctx;
    xfd->inode = ip;  // transfers reference to the file handle

    f->private_data = xfd;
    f->fops = &xfs_fops;
    f->pos = 0;
    f->is_directory = xfs_inode_isdir(ip);
    f->fs_type = FSType::XFS;
    f->refcount = 1;
    f->open_flags = flags;
    f->fd_flags = 0;
    f->vfs_path = nullptr;
    f->dir_fs_count = static_cast<size_t>(-1);

    return f;
}

// ============================================================================
// Stat helpers
// ============================================================================

namespace {

void fill_stat(XfsInode* ip, ker::vfs::Stat* st) {
    st->st_dev = ip->mount->dev_id;
    st->st_ino = ip->ino;
    st->st_nlink = ip->nlink;
    st->st_mode = ip->mode;
    st->st_uid = ip->uid;
    st->st_gid = ip->gid;
    st->pad0 = 0;
    st->st_rdev = 0;
    st->st_size = static_cast<off_t>(ip->size);
    st->st_blksize = static_cast<blksize_t>(ip->mount->block_size);
    st->st_blocks = static_cast<blkcnt_t>(ip->nblocks * (ip->mount->block_size / 512));

    // Timestamps: bigtime uses nanoseconds since Dec 13, 1901 packed into
    // a uint64; legacy uses upper-32 = seconds, lower-32 = nanoseconds.
    bool const BIGTIME = (ip->flags2 & XFS_DIFLAG2_BIGTIME) != 0;
    if (BIGTIME) {
        // XFS bigtime epoch: nanoseconds offset from the legacy min timestamp.
        // Linux: XFS_BIGTIME_EPOCH_OFFSET = -(int64_t)S32_MIN = 2^31 = 2147483648
        constexpr int64_t XFS_BIGTIME_EPOCH_OFFSET = (1LL << 31);
        constexpr uint64_t NSEC_PER_SEC = 1000000000ULL;
        auto decode = [&](uint64_t raw, struct Timespec& ts) {
            ts.tv_sec = static_cast<int64_t>(raw / NSEC_PER_SEC) - XFS_BIGTIME_EPOCH_OFFSET;
            ts.tv_nsec = static_cast<int64_t>(raw % NSEC_PER_SEC);
        };
        decode(ip->atime, st->st_atim);
        decode(ip->mtime, st->st_mtim);
        decode(ip->ctime, st->st_ctim);
    } else {
        st->st_atim.tv_sec = static_cast<int64_t>(ip->atime >> 32);
        st->st_atim.tv_nsec = static_cast<int64_t>(ip->atime & 0xFFFFFFFF);
        st->st_mtim.tv_sec = static_cast<int64_t>(ip->mtime >> 32);
        st->st_mtim.tv_nsec = static_cast<int64_t>(ip->mtime & 0xFFFFFFFF);
        st->st_ctim.tv_sec = static_cast<int64_t>(ip->ctime >> 32);
        st->st_ctim.tv_nsec = static_cast<int64_t>(ip->ctime & 0xFFFFFFFF);
    }
}

}  // anonymous namespace

auto xfs_stat(const char* fs_path, ker::vfs::Stat* statbuf, XfsMountContext* ctx) -> int {
    if (statbuf == nullptr || ctx == nullptr) {
        return -EINVAL;
    }

    XfsMetadataGuard metadata_guard(ctx);

    auto* ip = walk_path(ctx, fs_path);
    if (ip == nullptr) {
        return -ENOENT;
    }

    std::memset(statbuf, 0, sizeof(ker::vfs::Stat));
    fill_stat(ip, statbuf);
    xfs_inode_release(ip);
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

    XfsMetadataGuard metadata_guard(xfd->mount);

    std::memset(statbuf, 0, sizeof(ker::vfs::Stat));
    fill_stat(xfd->inode, statbuf);
    return 0;
}

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

// ============================================================================
// chmod
// ============================================================================

auto xfs_chmod_path(const char* fs_path, int mode, XfsMountContext* ctx) -> int {
    if (fs_path == nullptr || ctx == nullptr) {
        return -EINVAL;
    }
    if (ctx->read_only) {
        return -EROFS;
    }

    XfsMetadataGuard metadata_guard(ctx);
    auto* ip = walk_path(ctx, fs_path);
    if (ip == nullptr) {
        return -ENOENT;
    }

    static constexpr uint16_t XFS_IFMT = 0xF000;
    static constexpr uint16_t XFS_PERM_MASK = 07777;
    ip->mode = (ip->mode & XFS_IFMT) | (static_cast<uint16_t>(mode) & XFS_PERM_MASK);
    ip->dirty = true;

    auto* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) {
        xfs_inode_release(ip);
        return -ENOMEM;
    }
    xfs_trans_log_inode(tp, ip);
    int const RET = xfs_trans_commit(tp);
    xfs_inode_release(ip);
    return (RET == 0) ? 0 : -EIO;
}

auto xfs_fchmod(File* f, int mode) -> int {
    if (f == nullptr) {
        return -EBADF;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr || xfd->mount == nullptr) {
        return -EBADF;
    }
    if (xfd->mount->read_only) {
        return -EROFS;
    }

    XfsMetadataGuard metadata_guard(xfd->mount);
    static constexpr uint16_t XFS_IFMT = 0xF000;
    static constexpr uint16_t XFS_PERM_MASK = 07777;
    auto* ip = xfd->inode;
    ip->mode = (ip->mode & XFS_IFMT) | (static_cast<uint16_t>(mode) & XFS_PERM_MASK);
    ip->dirty = true;

    auto* tp = xfs_trans_alloc(xfd->mount);
    if (tp == nullptr) {
        return -ENOMEM;
    }
    xfs_trans_log_inode(tp, ip);
    int const RET = xfs_trans_commit(tp);
    return (RET == 0) ? 0 : -EIO;
}

// ============================================================================
// Mkdir
// ============================================================================

auto xfs_mkdir_path(const char* fs_path, int mode, XfsMountContext* ctx) -> int {
    if (fs_path == nullptr || ctx == nullptr) {
        return -EINVAL;
    }
    if (ctx->read_only) {
        return -EROFS;
    }

    XfsMetadataGuard metadata_guard(ctx);

    XfsInode* parent_ip = nullptr;
    const char* dirname = nullptr;
    uint16_t dirname_len = 0;
    int parent_ret = xfs_find_parent_and_name(fs_path, ctx, &parent_ip, &dirname, &dirname_len);
    if (parent_ret != 0) {
        if (parent_ret == -EINVAL) {
            XfsInode* existing = walk_path(ctx, fs_path);
            if (existing != nullptr) {
                bool const IS_DIR = xfs_inode_isdir(existing);
                xfs_inode_release(existing);
                return IS_DIR ? -EEXIST : -ENOTDIR;
            }
        }
        if (parent_ret == -ENOTDIR) {
            return -ENOENT;
        }
        return parent_ret;
    }

    XfsDirEntry existing{};
    int const LOOKUP_RET = xfs_dir_lookup(parent_ip, dirname, dirname_len, &existing);
    if (LOOKUP_RET == 0) {
        xfs_inode_release(parent_ip);
        return existing.ftype == XFS_DIR3_FT_DIR ? -EEXIST : -ENOTDIR;
    }
    if (LOOKUP_RET != -ENOENT) {
        xfs_inode_release(parent_ip);
        return LOOKUP_RET;
    }

    // Save before parent_ip is released
    xfs_ino_t const PARENT_INO = parent_ip->ino;

    // Allocate new inode
    XfsTransaction* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) {
        xfs_inode_release(parent_ip);
        return -ENOMEM;
    }

    int const DIR_MODE = (mode == 0) ? 0755 : mode;
    uint16_t const INODE_MODE = static_cast<uint16_t>(DIR_MODE & 0xFFF) | 0040000;  // S_IFDIR
    xfs_ino_t const NEW_INO = xfs_ialloc(ctx, tp, INODE_MODE);
    if (NEW_INO == NULLFSINO) {
        xfs_trans_cancel(tp);
        xfs_inode_release(parent_ip);
        return -ENOSPC;
    }

    // Build minimal shortform directory: count=0, parent=parent_ino
    bool const USE_I8 = (PARENT_INO > 0xFFFFFFFFULL);
    size_t const INO_BYTES = USE_I8 ? 8 : 4;
    size_t const SF_SIZE = 2 + INO_BYTES;
    std::array<uint8_t, 10> sf_data{};
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

    XfsInode new_inode{};
    new_inode.ino = NEW_INO;
    new_inode.mount = ctx;
    new_inode.agno = xfs_ino_ag(NEW_INO, ctx->agino_log);
    new_inode.agino = xfs_ag_ino(NEW_INO, ctx->agino_log);
    new_inode.mode = INODE_MODE;
    new_inode.size = SF_SIZE;
    new_inode.nlink = 2;  // . and parent ref
    new_inode.data_fork.format = XFS_DINODE_FMT_LOCAL;
    new_inode.data_fork.local.data = sf_data.data();
    new_inode.data_fork.local.size = SF_SIZE;
    xfs_stamp_new_inode(&new_inode);
    xfs_trans_log_inode(tp, &new_inode);

    // Add entry in parent
    int rc = xfs_dir_addname(parent_ip, dirname, dirname_len, NEW_INO, XFS_DIR3_FT_DIR, tp);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        xfs_inode_release(parent_ip);
        return rc;
    }

    rc = xfs_trans_commit(tp);
    xfs_inode_release(parent_ip);
    if (rc != 0) {
        return rc;
    }
    return 0;
}

// ============================================================================
// Rmdir
// ============================================================================

namespace {

auto count_real_entries(const XfsDirEntry* entry, void* ctx) -> int {
    auto* count = static_cast<int*>(ctx);
    if (entry->name.at(0) == '.' && (entry->namelen == 1 || (entry->namelen == 2 && entry->name.at(1) == '.'))) {
        return 0;
    }
    (*count)++;
    return 0;
}

}  // anonymous namespace

auto xfs_symlink_path(const char* target, const char* fs_path, XfsMountContext* ctx) -> int {
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

    XfsMetadataGuard metadata_guard(ctx);

    XfsInode* parent_ip = nullptr;
    const char* name = nullptr;
    uint16_t namelen = 0;
    int rc = xfs_find_parent_and_name(fs_path, ctx, &parent_ip, &name, &namelen);
    if (rc != 0) {
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
    if (target_data == nullptr) {
        xfs_inode_release(parent_ip);
        return -ENOMEM;
    }
    if (target_len != 0) {
        std::memcpy(target_data, target, target_len);
    }

    XfsTransaction* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) {
        delete[] target_data;
        xfs_inode_release(parent_ip);
        return -ENOMEM;
    }

    constexpr uint16_t SYMLINK_MODE = 0120000 | 0777;
    xfs_ino_t const NEW_INO = xfs_ialloc(ctx, tp, SYMLINK_MODE);
    if (NEW_INO == NULLFSINO) {
        xfs_trans_cancel(tp);
        delete[] target_data;
        xfs_inode_release(parent_ip);
        return -ENOSPC;
    }

    XfsInode new_inode{};
    new_inode.ino = NEW_INO;
    new_inode.mount = ctx;
    new_inode.agno = xfs_ino_ag(NEW_INO, ctx->agino_log);
    new_inode.agino = xfs_ag_ino(NEW_INO, ctx->agino_log);
    new_inode.mode = SYMLINK_MODE;
    new_inode.size = target_len;
    new_inode.nlink = 1;
    new_inode.data_fork.format = XFS_DINODE_FMT_LOCAL;
    new_inode.data_fork.local.data = target_data;
    new_inode.data_fork.local.size = target_len;
    xfs_stamp_new_inode(&new_inode);
    xfs_trans_log_inode(tp, &new_inode);

    rc = xfs_dir_addname(parent_ip, name, namelen, NEW_INO, XFS_DIR3_FT_SYMLINK, tp);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        delete[] target_data;
        xfs_inode_release(parent_ip);
        return rc;
    }

    rc = xfs_trans_commit(tp);
    delete[] target_data;
    xfs_inode_release(parent_ip);
    return (rc == 0) ? 0 : -EIO;
}

auto xfs_rmdir_path(const char* fs_path, XfsMountContext* ctx) -> int {
    if (fs_path == nullptr || ctx == nullptr) {
        return -EINVAL;
    }
    if (ctx->read_only) {
        return -EROFS;
    }
    if (fs_path[0] == '\0' || (fs_path[0] == '/' && fs_path[1] == '\0')) {
        return -EBUSY;
    }

    XfsMetadataGuard metadata_guard(ctx);

    XfsInode* parent_ip = nullptr;
    const char* name = nullptr;
    uint16_t namelen = 0;
    int rc = xfs_find_parent_and_name(fs_path, ctx, &parent_ip, &name, &namelen);
    if (rc != 0) {
        return rc;
    }

    XfsDirEntry de{};
    rc = xfs_dir_lookup(parent_ip, name, namelen, &de);
    if (rc != 0) {
        xfs_inode_release(parent_ip);
        return rc;
    }
    if (de.ftype != XFS_DIR3_FT_DIR) {
        xfs_inode_release(parent_ip);
        return -ENOTDIR;
    }

    XfsInode* dir_ip = xfs_inode_read(ctx, de.ino);
    if (dir_ip == nullptr) {
        xfs_inode_release(parent_ip);
        return -ENOENT;
    }

    int entry_count = 0;
    xfs_dir_iterate(dir_ip, count_real_entries, &entry_count);
    if (entry_count > 0) {
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

    rc = xfs_dir_removename(parent_ip, name, namelen, tp);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        xfs_inode_release(dir_ip);
        xfs_inode_release(parent_ip);
        return rc;
    }

    parent_ip->dirty = true;
    xfs_trans_log_inode(tp, parent_ip);
    // The removed directory loses both the parent entry and its self-reference.
    dir_ip->nlink = 0;
    dir_ip->dirty = true;
    xfs_trans_log_inode(tp, dir_ip);

    rc = xfs_trans_commit(tp);
    if (rc == 0) {
        xfs_parent_path_cache_purge_all_for_mount(ctx);
    }
    xfs_inode_release(dir_ip);
    xfs_inode_release(parent_ip);
    return (rc == 0) ? 0 : -EIO;
}

// ============================================================================
// Rename
// ============================================================================

auto xfs_rename_path(const char* old_fs_path, const char* new_fs_path, XfsMountContext* ctx) -> int {
    if (old_fs_path == nullptr || new_fs_path == nullptr || ctx == nullptr) {
        return -EINVAL;
    }
    if (ctx->read_only) {
        return -EROFS;
    }

    XfsMetadataGuard metadata_guard(ctx);

    XfsInode* old_parent = nullptr;
    const char* old_name = nullptr;
    uint16_t old_namelen = 0;
    int rc = xfs_find_parent_and_name(old_fs_path, ctx, &old_parent, &old_name, &old_namelen);
    if (rc != 0) {
        return rc;
    }

    XfsDirEntry old_de{};
    rc = xfs_dir_lookup(old_parent, old_name, old_namelen, &old_de);
    if (rc != 0) {
        xfs_inode_release(old_parent);
        return rc;
    }
    bool purge_parent_path_cache = old_de.ftype == XFS_DIR3_FT_DIR;

    XfsInode* new_parent = nullptr;
    const char* new_name = nullptr;
    uint16_t new_namelen = 0;
    rc = xfs_find_parent_and_name(new_fs_path, ctx, &new_parent, &new_name, &new_namelen);
    if (rc != 0) {
        xfs_inode_release(old_parent);
        return rc;
    }

    XfsTransaction* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) {
        xfs_inode_release(new_parent);
        xfs_inode_release(old_parent);
        return -ENOMEM;
    }

    // If destination exists, remove it first. Keep the displaced inode
    // referenced until the transaction no longer points at it.
    XfsDirEntry new_de{};
    XfsInode* displaced = nullptr;
    if (xfs_dir_lookup(new_parent, new_name, new_namelen, &new_de) == 0) {
        purge_parent_path_cache = purge_parent_path_cache || new_de.ftype == XFS_DIR3_FT_DIR;
        rc = xfs_dir_removename(new_parent, new_name, new_namelen, tp);
        if (rc != 0) {
            xfs_trans_cancel(tp);
            xfs_inode_release(new_parent);
            xfs_inode_release(old_parent);
            return rc;
        }
        // Decrement nlink on the displaced inode
        displaced = xfs_inode_read(ctx, new_de.ino);
        if (displaced != nullptr) {
            if (displaced->nlink > 0) {
                displaced->nlink--;
            }
            displaced->dirty = true;
            xfs_trans_log_inode(tp, displaced);
        }
    }

    // Add entry at new location
    rc = xfs_dir_addname(new_parent, new_name, new_namelen, old_de.ino, old_de.ftype, tp);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        if (displaced != nullptr) {
            xfs_inode_release(displaced);
        }
        xfs_inode_release(new_parent);
        xfs_inode_release(old_parent);
        return rc;
    }

    // Remove old entry
    rc = xfs_dir_removename(old_parent, old_name, old_namelen, tp);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        if (displaced != nullptr) {
            xfs_inode_release(displaced);
        }
        xfs_inode_release(new_parent);
        xfs_inode_release(old_parent);
        return rc;
    }

    old_parent->dirty = true;
    new_parent->dirty = true;
    xfs_trans_log_inode(tp, old_parent);
    xfs_trans_log_inode(tp, new_parent);

    rc = xfs_trans_commit(tp);
    if (rc == 0 && purge_parent_path_cache) {
        xfs_parent_path_cache_purge_all_for_mount(ctx);
    }
    if (displaced != nullptr) {
        xfs_inode_release(displaced);
    }
    xfs_inode_release(new_parent);
    xfs_inode_release(old_parent);
    return (rc == 0) ? 0 : -EIO;
}

// ============================================================================
// Unlink / remove
// ============================================================================

auto xfs_unlink_path(const char* fs_path, XfsMountContext* ctx) -> int {
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

    XfsMetadataGuard metadata_guard(ctx);

    XfsInode* parent_ip = nullptr;
    const char* filename = nullptr;
    uint16_t filename_len = 0;
    int rc = xfs_find_parent_and_name(fs_path, ctx, &parent_ip, &filename, &filename_len);
    if (rc != 0) {
        return rc;
    }

    // Look up the entry to be deleted (must verify it exists and is a regular file)
    XfsDirEntry de{};
    rc = xfs_dir_lookup(parent_ip, filename, filename_len, &de);
    if (rc != 0) {
        xfs_inode_release(parent_ip);
        return rc;
    }

    // Cannot unlink a directory with unlink (should use rmdir)
    if (de.ftype == XFS_DIR3_FT_DIR) {
        xfs_inode_release(parent_ip);
        return -EISDIR;
    }

    // Create transaction for removal
    XfsTransaction* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) {
        xfs_inode_release(parent_ip);
        return -ENOMEM;
    }

    // Remove from parent directory
    rc = xfs_dir_removename(parent_ip, filename, filename_len, tp);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        xfs_inode_release(parent_ip);
        return rc;
    }

    parent_ip->dirty = true;
    xfs_trans_log_inode(tp, parent_ip);

    // Read the target inode and decrement its nlink. Keep it referenced until
    // after transaction commit because xfs_trans_log_inode stores a raw pointer.
    XfsInode* target_ip = xfs_inode_read(ctx, de.ino);
    if (target_ip != nullptr) {
        if (target_ip->nlink > 0) {
            target_ip->nlink--;
        }
        target_ip->dirty = true;
        xfs_trans_log_inode(tp, target_ip);
    }

    rc = xfs_trans_commit(tp);

    if (target_ip != nullptr) {
        xfs_inode_release(target_ip);
    }
    xfs_inode_release(parent_ip);

    return (rc == 0) ? 0 : -EIO;
}

// ============================================================================
// Mount / init
// ============================================================================

auto xfs_vfs_init_device(dev::BlockDevice* device) -> XfsMountContext* {
    if (device == nullptr) {
        return nullptr;
    }

    XfsMountContext* ctx = nullptr;
    int ret = xfs_mount(device, dev::block_device_is_read_only(device), &ctx);
    if (ret != 0) {
        log::error("mount failed with error %d", ret);
        return nullptr;
    }

    // Initialize the log
    ret = xfs_log_mount(ctx);
    if (ret != 0) {
        log::error("log mount failed");
        xfs_unmount(ctx);
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
