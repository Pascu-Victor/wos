// XFS Inode read / cache implementation.
//
// Reads inodes from disk via the buffer cache, validates CRC, parses the
// xfs_dinode into the in-memory XfsInode struct, and caches them in a hash
// table for reuse.
//
// Reference: reference/xfs/libxfs/xfs_inode_buf.c, reference/xfs/xfs_icache.c

#include "xfs_inode.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sys/mutex.hpp>
#include <platform/sys/spinlock.hpp>
#include <util/crc32c.hpp>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_alloc.hpp>
#include <vfs/fs/xfs/xfs_bmap.hpp>
#include <vfs/fs/xfs/xfs_ialloc.hpp>
#include <vfs/fs/xfs/xfs_trans.hpp>

#include "net/endian.hpp"
#include "vfs/fs/xfs/xfs_format.hpp"
#include "vfs/fs/xfs/xfs_mount.hpp"

namespace ker::vfs::xfs {

#ifdef WOS_KASAN
// NOLINTNEXTLINE(readability-identifier-naming)
extern "C" auto __asan_region_is_poisoned(uintptr_t beg, size_t size) -> uintptr_t;
#endif

// ============================================================================
// Inode cache - simple hash table
// ============================================================================

namespace {

constexpr size_t ICACHE_BUCKETS = 1024;
constexpr size_t ICACHE_HASH_MASK = ICACHE_BUCKETS - 1;
constexpr size_t ICACHE_IDLE_RETAIN_LIMIT = 65536;
constexpr size_t ICACHE_RECLAIM_BATCH = 256;
constexpr uint64_t ALLOC_LOOKUP_WARN_INTERVAL = 4096;

struct IcacheBucket {
    XfsInode* head{};
    mod::sys::Spinlock lock;
};

std::array<IcacheBucket, ICACHE_BUCKETS> icache;
bool icache_inited = false;
std::atomic<uint64_t> alloc_lookup_failure_count{0};
std::atomic<size_t> icache_idle_count{0};

auto icache_hash(const XfsMountContext* mount, xfs_ino_t ino) -> size_t {
    auto const MOUNT_BITS = reinterpret_cast<uintptr_t>(mount) >> 6;
    auto const MIXED = (ino * 2654435761ULL) ^ static_cast<uint64_t>(MOUNT_BITS);
    return static_cast<size_t>(MIXED & ICACHE_HASH_MASK);
}

auto perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp op) -> uint64_t {
    return ker::mod::perf::is_wki_scope_recording_enabled(ker::mod::perf::WkiPerfScope::LOCAL_XFS, static_cast<uint8_t>(op))
               ? ker::mod::time::get_us()
               : 0;
}

auto perf_xfs_started_us() -> uint64_t { return ker::mod::perf::is_local_xfs_recording_enabled() ? ker::mod::time::get_us() : 0; }

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

void icache_decrement_idle_count() {
    size_t current = icache_idle_count.load(std::memory_order_relaxed);
    while (current != 0) {
        if (icache_idle_count.compare_exchange_weak(current, current - 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
            return;
        }
    }
}

// Look up an inode in the cache.  Returns with bucket locked and refcount
// incremented if found.  If the matching inode is in final inactivation,
// returns nullptr and sets unavailable so the caller will not read a second
// copy from disk.
auto icache_lookup_locked(const XfsMountContext* mount, xfs_ino_t ino, size_t bucket, bool* unavailable = nullptr) -> XfsInode* {
    if (unavailable != nullptr) {
        *unavailable = false;
    }
    XfsInode* ip = icache.at(bucket).head;
    while (ip != nullptr) {
        if (ip->ino == ino && ip->mount == mount) {
            if (ip->inactivation_started) {
                if (unavailable != nullptr) {
                    *unavailable = true;
                }
                return nullptr;
            }
            bool const WAS_IDLE = ip->refcount == 0;
            ip->refcount++;
            if (WAS_IDLE) {
                icache_decrement_idle_count();
            }
            return ip;
        }
        ip = ip->hash_next;
    }
    return nullptr;
}

// Insert an inode into the cache.  Caller must hold bucket lock.
void icache_insert_locked(XfsInode* ip, size_t bucket) {
    ip->hash_next = icache.at(bucket).head;
    icache.at(bucket).head = ip;
}

// Remove an inode from the cache.  Caller must hold bucket lock.
void icache_remove_locked(XfsInode* ip, size_t bucket) {
    XfsInode** pp = &icache.at(bucket).head;
    while (*pp != nullptr) {
        if (*pp == ip) {
            *pp = ip->hash_next;
            ip->hash_next = nullptr;
            return;
        }
        pp = &(*pp)->hash_next;
    }
}

// Free the fork data for an inode
void free_ifork(XfsIfork* fork) {
    switch (fork->format) {
        case XFS_DINODE_FMT_LOCAL:
            if (fork->local.data != nullptr) {
                delete[] fork->local.data;
                fork->local.data = nullptr;
            }
            break;
        case XFS_DINODE_FMT_EXTENTS:
            if (fork->extents.list != nullptr) {
                delete[] fork->extents.list;
                fork->extents.list = nullptr;
            }
            break;
        case XFS_DINODE_FMT_BTREE:
            if (fork->btree.root != nullptr) {
                delete[] fork->btree.root;
                fork->btree.root = nullptr;
            }
            break;
        default:
            break;
    }
}

// Free an XfsInode and all its data
void free_inode(XfsInode* ip) {
    free_ifork(&ip->data_fork);
    if (ip->has_attr_fork) {
        free_ifork(&ip->attr_fork);
    }
    delete ip;
}

void reclaim_idle_inodes() {
    if (icache_idle_count.load(std::memory_order_relaxed) <= ICACHE_IDLE_RETAIN_LIMIT) {
        return;
    }

    std::array<XfsInode*, ICACHE_RECLAIM_BATCH> victims{};
    size_t victim_count = 0;

    for (auto& bucket : icache) {
        uint64_t const FLAGS = bucket.lock.lock_irqsave();
        XfsInode** pp = &bucket.head;
        while (*pp != nullptr && victim_count < victims.size() &&
               icache_idle_count.load(std::memory_order_relaxed) > ICACHE_IDLE_RETAIN_LIMIT) {
            XfsInode* ip = *pp;
            if (ip->refcount == 0 && ip->nlink != 0 && !ip->dirty && !ip->inactivation_started) {
                *pp = ip->hash_next;
                ip->hash_next = nullptr;
                icache_decrement_idle_count();
                victims.at(victim_count++) = ip;
                continue;
            }
            pp = &ip->hash_next;
        }
        bucket.lock.unlock_irqrestore(FLAGS);

        if (victim_count == victims.size() || icache_idle_count.load(std::memory_order_relaxed) <= ICACHE_IDLE_RETAIN_LIMIT) {
            break;
        }
    }

    for (size_t i = 0; i < victim_count; ++i) {
        free_inode(victims.at(i));
    }
}

auto inode_fsblock_to_dev_block(XfsMountContext* ctx, xfs_fsblock_t fsbno) -> uint64_t {
    auto agno = xfs_ag_number(fsbno, ctx->ag_blk_log);
    auto agbno = xfs_ag_block(fsbno, ctx->ag_blk_log);
    uint64_t const LINEAR_BLOCK = (static_cast<uint64_t>(agno) * ctx->ag_blocks) + agbno;
    return LINEAR_BLOCK * (ctx->block_size / ctx->device->block_size);
}

auto inode_fsb_to_dev_count(XfsMountContext* ctx, xfs_filblks_t fsb_count) -> size_t {
    return static_cast<size_t>(fsb_count) * (ctx->block_size / ctx->device->block_size);
}

auto free_inode_data_extent(XfsInode* ip, XfsTransaction* tp, xfs_fsblock_t startblock, xfs_filblks_t blockcount) -> int {
    if (ip == nullptr || ip->mount == nullptr || tp == nullptr || startblock == NULLFSBLOCK || blockcount == 0) {
        return 0;
    }

    auto* mount = ip->mount;
    if (mount->device != nullptr) {
        discard_bdev_range(mount->device, inode_fsblock_to_dev_block(mount, startblock), inode_fsb_to_dev_count(mount, blockcount));
    }

    xfs_agnumber_t agno = xfs_ag_number(startblock, mount->ag_blk_log);
    xfs_agblock_t agbno = xfs_ag_block(startblock, mount->ag_blk_log);
    xfs_filblks_t remaining = blockcount;

    while (remaining > 0) {
        if (agno >= mount->ag_count || agbno >= mount->ag_blocks) {
            return -EIO;
        }

        auto const AG_REMAINING = static_cast<xfs_filblks_t>(mount->ag_blocks - agbno);
        xfs_filblks_t const SPAN = std::min(remaining, AG_REMAINING);
        if (SPAN == 0 || SPAN > static_cast<xfs_filblks_t>(UINT32_MAX)) {
            return -EIO;
        }

        int const RC = xfs_free_extent(mount, tp, agno, agbno, static_cast<xfs_extlen_t>(SPAN));
        if (RC != 0) {
            return RC;
        }

        remaining -= SPAN;
        agno++;
        agbno = 0;
    }

    return 0;
}

auto free_inode_extent_records(XfsInode* ip, XfsTransaction* tp, const XfsBmbtIrec* extents, uint32_t count) -> int {
    if (extents == nullptr || count == 0) {
        return 0;
    }

    for (uint32_t i = 0; i < count; i++) {
        int const RC = free_inode_data_extent(ip, tp, extents[i].br_startblock, extents[i].br_blockcount);
        if (RC != 0) {
            return RC;
        }
    }
    return 0;
}

auto free_inode_data_extents(XfsInode* ip, XfsTransaction* tp) -> int {
    if (ip == nullptr || ip->nblocks == 0 || ip->data_fork.format == XFS_DINODE_FMT_LOCAL) {
        return 0;
    }

    if (ip->data_fork.format == XFS_DINODE_FMT_EXTENTS) {
        if (ip->data_fork.extents.count != 0 && ip->data_fork.extents.list == nullptr) {
            return -EIO;
        }
        return free_inode_extent_records(ip, tp, ip->data_fork.extents.list, ip->data_fork.extents.count);
    }

    if (ip->data_fork.format != XFS_DINODE_FMT_BTREE) {
        return 0;
    }
    if (ip->nextents == 0) {
        return 0;
    }

    auto* extents = new (std::nothrow) XfsBmbtIrec[ip->nextents];
    if (extents == nullptr) {
        return -ENOMEM;
    }

    int rc = xfs_bmap_list_extents(ip, extents, ip->nextents);
    if (rc >= 0) {
        rc = free_inode_extent_records(ip, tp, extents, static_cast<uint32_t>(rc));
    }

    delete[] extents;
    return rc;
}

auto inactivate_unlinked_inode(XfsInode* ip) -> int {
    if (ip == nullptr || ip->mount == nullptr || ip->mount->read_only || ip->nlink != 0) {
        return 0;
    }

    int const ALLOCATED = xfs_inode_allocated(ip->mount, ip->ino);
    if (ALLOCATED <= 0) {
        if (ALLOCATED < 0) {
            mod::dbg::logger<"xfs">::error("xfs_inode_release: failed to verify inode %lu allocation state rc=%d",
                                           static_cast<unsigned long>(ip->ino), ALLOCATED);
            return ALLOCATED;
        }
        mod::dbg::logger<"xfs">::debug("xfs_inode_release: inode %lu already free during inactivation",
                                       static_cast<unsigned long>(ip->ino));
        return 0;
    }

    auto* tp = xfs_trans_alloc(ip->mount);
    if (tp == nullptr) {
        mod::dbg::logger<"xfs">::error("xfs_inode_release: failed to allocate inactivation transaction for inode %lu",
                                       static_cast<unsigned long>(ip->ino));
        return -ENOMEM;
    }

    int rc = free_inode_data_extents(ip, tp);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        mod::dbg::logger<"xfs">::error("xfs_inode_release: failed to free data extents for inode %lu rc=%d",
                                       static_cast<unsigned long>(ip->ino), rc);
        return rc;
    }

    rc = xfs_ifree(ip->mount, tp, ip->ino);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        if (rc == -EEXIST) {
            mod::dbg::logger<"xfs">::debug("xfs_inode_release: inode %lu already free during inactivation",
                                           static_cast<unsigned long>(ip->ino));
            return 0;
        }
        mod::dbg::logger<"xfs">::error("xfs_inode_release: deferred ifree failed for inode %lu rc=%d", static_cast<unsigned long>(ip->ino),
                                       rc);
        return rc;
    }

    rc = xfs_trans_commit(tp);
    if (rc != 0) {
        mod::dbg::logger<"xfs">::error("xfs_inode_release: deferred ifree commit failed for inode %lu rc=%d",
                                       static_cast<unsigned long>(ip->ino), rc);
    }
    return rc;
}

auto count_cached_mount_inodes(const XfsMountContext* mount) -> size_t {
    size_t count = 0;
    for (auto& bucket : icache) {
        uint64_t const FLAGS = bucket.lock.lock_irqsave();
        for (XfsInode* ip = bucket.head; ip != nullptr; ip = ip->hash_next) {
            if (ip->mount == mount && !ip->inactivation_started) {
                ++count;
            }
        }
        bucket.lock.unlock_irqrestore(FLAGS);
    }
    return count;
}

auto collect_cached_mount_inodes(const XfsMountContext* mount, XfsInode** inodes, size_t capacity) -> size_t {
    if (inodes == nullptr || capacity == 0) {
        return 0;
    }

    size_t count = 0;
    for (auto& bucket : icache) {
        uint64_t const FLAGS = bucket.lock.lock_irqsave();
        for (XfsInode* ip = bucket.head; ip != nullptr && count < capacity; ip = ip->hash_next) {
            if (ip->mount == mount && !ip->inactivation_started) {
                bool const WAS_IDLE = ip->refcount == 0;
                ip->refcount++;
                if (WAS_IDLE) {
                    icache_decrement_idle_count();
                }
                inodes[count++] = ip;  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            }
        }
        bucket.lock.unlock_irqrestore(FLAGS);
        if (count == capacity) {
            break;
        }
    }
    return count;
}

auto xfs_commit_cached_dirty_inode(XfsMountContext* mount, XfsInode* ip) -> int {
    if (mount == nullptr || ip == nullptr || mount->read_only || !ip->dirty) {
        return 0;
    }

    uint64_t const STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_ILOG);
    XfsTransaction* tp = xfs_trans_alloc(mount);
    if (tp == nullptr) {
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_ILOG, STARTED_US, -ENOMEM, 0);
        return -ENOMEM;
    }

    xfs_trans_log_inode(tp, ip);
    int const RET = xfs_trans_commit(tp);
    perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::WRITE_ILOG, STARTED_US, RET, 0);
    return RET == 0 ? 0 : -EIO;
}

// Parse a fork from the on-disk inode. data_size is the available fork space;
// local_size is the logical byte count for LOCAL forks.
auto parse_ifork(XfsIfork* fork, uint8_t fmt, const uint8_t* data_ptr, size_t data_size, size_t local_size, uint32_t nextents) -> int {
    auto format = static_cast<xfs_dinode_fmt>(fmt);
    fork->format = format;

    switch (format) {
        case XFS_DINODE_FMT_LOCAL: {
            size_t const LOCAL_SIZE = std::min(local_size, data_size);
            fork->local.size = LOCAL_SIZE;
            if (LOCAL_SIZE == 0) {
                fork->local.data = nullptr;
                break;
            }
            fork->local.data = new (std::nothrow) uint8_t[LOCAL_SIZE];
            if (fork->local.data == nullptr) {
                return -ENOMEM;
            }
            __builtin_memcpy(fork->local.data, data_ptr, LOCAL_SIZE);
            break;
        }

        case XFS_DINODE_FMT_EXTENTS: {
            fork->extents.count = nextents;
            if (nextents == 0) {
                fork->extents.list = nullptr;
                break;
            }
            fork->extents.list = new (std::nothrow) XfsBmbtIrec[nextents];
            if (fork->extents.list == nullptr) {
                return -ENOMEM;
            }

            // Decode the on-disk extent records
            const auto* recs = reinterpret_cast<const XfsBmbtRec*>(data_ptr);
            for (uint32_t i = 0; i < nextents; i++) {
                fork->extents.list[i] = xfs_bmbt_rec_unpack(&recs[i]);
            }
            break;
        }

        case XFS_DINODE_FMT_BTREE: {
            // The fork contains a bmdr_block header + keys + pointers
            if (data_size < sizeof(XfsBmdrBlock)) {
                return -EINVAL;
            }
            const auto* bmdr = reinterpret_cast<const XfsBmdrBlock*>(data_ptr);
            fork->btree.level = bmdr->bb_level.to_cpu();
            fork->btree.numrecs = bmdr->bb_numrecs.to_cpu();
            // Copy the entire fork data (header + keys + ptrs) for later traversal
            fork->btree.root_size = data_size;
            fork->btree.root = new (std::nothrow) uint8_t[data_size];
            if (fork->btree.root == nullptr) {
                return -ENOMEM;
            }
            __builtin_memcpy(fork->btree.root, data_ptr, data_size);
            break;
        }

        case XFS_DINODE_FMT_DEV:
            // Device inodes: store the 4-byte dev_t in the local data
            fork->local.size = data_size < 4 ? data_size : 4;
            fork->local.data = new (std::nothrow) uint8_t[4];
            if (fork->local.data == nullptr) {
                return -ENOMEM;
            }
            __builtin_memcpy(fork->local.data, data_ptr, fork->local.size);
            break;

        default:
            mod::dbg::log("[xfs] unknown fork format %d", fmt);
            return -EINVAL;
    }

    return 0;
}

}  // anonymous namespace

// ============================================================================
// Public API
// ============================================================================

void xfs_icache_init() {
    if (icache_inited) {
        return;
    }
    for (auto& i : icache) {
        i.head = nullptr;
    }
    icache_inited = true;
}

void xfs_icache_purge(XfsMountContext* mount) {
    for (auto& i : icache) {
        XfsInode* purge_list = nullptr;
        uint64_t const FLAGS = i.lock.lock_irqsave();
        XfsInode** pp = &i.head;
        while (*pp != nullptr) {
            XfsInode* ip = *pp;
            if (ip->mount == mount) {
                *pp = ip->hash_next;
                ip->hash_next = purge_list;
                purge_list = ip;
                if (ip->refcount == 0 && !ip->inactivation_started) {
                    icache_decrement_idle_count();
                }
            } else {
                pp = &ip->hash_next;
            }
        }
        i.lock.unlock_irqrestore(FLAGS);

        while (purge_list != nullptr) {
            XfsInode* ip = purge_list;
            purge_list = ip->hash_next;
            ip->hash_next = nullptr;
            free_inode(ip);
        }
    }
}

auto xfs_icache_sync_dirty(XfsMountContext* mount) -> int {
    if (mount == nullptr || mount->read_only) {
        return 0;
    }

    xfs_icache_init();

    size_t const INODE_COUNT = count_cached_mount_inodes(mount);
    if (INODE_COUNT == 0) {
        return 0;
    }

    auto** inodes = new (std::nothrow) XfsInode*[INODE_COUNT];
    if (inodes == nullptr) {
        return -ENOMEM;
    }

    size_t const COLLECTED = std::min(collect_cached_mount_inodes(mount, inodes, INODE_COUNT), INODE_COUNT);
    int result = 0;

    for (size_t i = 0; i < COLLECTED; ++i) {
        auto* ip = inodes[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        {
            mod::sys::MutexGuard guard(ip->io_lock);
            if (ip->mount == mount && !ip->inactivation_started) {
                int const RET = xfs_commit_cached_dirty_inode(mount, ip);
                if (RET != 0 && result == 0) {
                    result = RET;
                }
            }
        }
        xfs_inode_release(ip);
    }

    delete[] inodes;
    return result;
}

auto xfs_inode_truncate_data(XfsInode* ip, XfsTransaction* tp) -> int {
    if (ip == nullptr || ip->mount == nullptr || tp == nullptr) {
        return -EINVAL;
    }

    int const RC = free_inode_data_extents(ip, tp);
    if (RC != 0) {
        return RC;
    }

    free_ifork(&ip->data_fork);
    ip->data_fork.format = XFS_DINODE_FMT_EXTENTS;
    ip->data_fork.extents.list = nullptr;
    ip->data_fork.extents.count = 0;
    ip->data_fork.extents.capacity = 0;
    ip->nextents = 0;
    ip->nblocks = 0;
    ip->dirty = true;
    return 0;
}

auto xfs_inode_read(XfsMountContext* mount, xfs_ino_t ino) -> XfsInode* {
    uint64_t const PERF_FETCH_STARTED_US = perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp::INODE_FETCH);
    auto finish_inode_fetch = [&](XfsInode* result, int32_t status) -> XfsInode* {
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::INODE_FETCH, PERF_FETCH_STARTED_US, status, result != nullptr ? 1 : 0);
        return result;
    };

    if (mount == nullptr || ino == NULLFSINO) {
        return finish_inode_fetch(nullptr, -EINVAL);
    }

    xfs_icache_init();

    size_t const BUCKET = icache_hash(mount, ino);

    // Check cache first
    uint64_t const PERF_CACHE_STARTED_US = perf_xfs_started_us();
    uint64_t flags = icache.at(BUCKET).lock.lock_irqsave();
    bool unavailable = false;
    XfsInode* ip = icache_lookup_locked(mount, ino, BUCKET, &unavailable);
    if (ip != nullptr) {
        icache.at(BUCKET).lock.unlock_irqrestore(flags);
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::INODE_CACHE_HIT, PERF_CACHE_STARTED_US, 0, 1);
        return finish_inode_fetch(ip, 0);
    }
    icache.at(BUCKET).lock.unlock_irqrestore(flags);
    if (unavailable) {
        perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::INODE_UNAVAILABLE, PERF_CACHE_STARTED_US, -ENOENT, 1);
        return finish_inode_fetch(nullptr, -ENOENT);
    }
    perf_record_xfs_stage(ker::mod::perf::WkiPerfLocalXfsOp::INODE_CACHE_MISS, PERF_CACHE_STARTED_US, 0, 1);

    int const ALLOCATED = xfs_inode_allocated(mount, ino);
    if (ALLOCATED == 0) {
        mod::dbg::logger<"xfs">::debug("xfs_inode_read: inode %lu is marked free", static_cast<unsigned long>(ino));
        return finish_inode_fetch(nullptr, -ENOENT);
    }
    if (ALLOCATED < 0) {
        uint64_t const COUNT = alloc_lookup_failure_count.fetch_add(1, std::memory_order_relaxed) + 1;
        if ((COUNT % ALLOC_LOOKUP_WARN_INTERVAL) == 1) {
            mod::dbg::logger<"xfs">::warn(
                "xfs_inode_read: allocation lookup failed for inode %lu rc=%d; validating dinode directly (count=%lu)",
                static_cast<unsigned long>(ino), ALLOCATED, COUNT);
        }
    }

    // Not in cache - read from disk
    xfs_fsblock_t const BLOCK = xfs_inode_block(mount, ino);
    size_t const OFFSET = xfs_inode_offset(mount, ino);

    BufHead* bh = xfs_buf_read(mount, BLOCK);
    if (bh == nullptr) {
        mod::dbg::log("[xfs] failed to read inode %lu block %lu", static_cast<unsigned long>(ino), static_cast<unsigned long>(BLOCK));
        return finish_inode_fetch(nullptr, -EIO);
    }

    if (OFFSET + mount->inode_size > bh->size) {
        mod::dbg::log("[xfs] inode %lu: offset %lu + size %u exceeds block size %lu", static_cast<unsigned long>(ino),
                      static_cast<unsigned long>(OFFSET), mount->inode_size, static_cast<unsigned long>(bh->size));
        brelse(bh);
        return finish_inode_fetch(nullptr, -EINVAL);
    }

    const auto* dip = reinterpret_cast<const XfsDinode*>(bh->data + OFFSET);

    // Validate magic
    if (dip->di_magic.to_cpu() != XFS_DINODE_MAGIC) {
        mod::dbg::log("[xfs] inode %lu: bad magic 0x%x", static_cast<unsigned long>(ino), dip->di_magic.to_cpu());
        brelse(bh);
        return finish_inode_fetch(nullptr, -EINVAL);
    }

    // Verify version
    if (dip->di_version != 3) {
        mod::dbg::log("[xfs] inode %lu: unsupported version %u", static_cast<unsigned long>(ino), dip->di_version);
        brelse(bh);
        return finish_inode_fetch(nullptr, -EINVAL);
    }

#ifdef WOS_KASAN
    if (uintptr_t const POISONED = __asan_region_is_poisoned(reinterpret_cast<uintptr_t>(dip), mount->inode_size); POISONED != 0) {
        mod::dbg::log("[xfs] inode %lu: poisoned inode buffer bh=%p data=%p size=%lu block=%lu offset=%lu bad=0x%lx",
                      static_cast<unsigned long>(ino), reinterpret_cast<void*>(bh), reinterpret_cast<void*>(bh->data),
                      static_cast<unsigned long>(bh->size), static_cast<unsigned long>(BLOCK), static_cast<unsigned long>(OFFSET),
                      static_cast<unsigned long>(POISONED));
        brelse(bh);
        return finish_inode_fetch(nullptr, -EINVAL);
    }
#endif
    // Verify CRC
    uint32_t const COMPUTED = util::crc32c_block_with_cksum(dip, mount->inode_size, XFS_DINODE_CRC_OFF);
    if (COMPUTED != dip->di_crc) {
        mod::dbg::log("[xfs] inode %lu: CRC mismatch (computed 0x%x, on-disk 0x%x)", static_cast<unsigned long>(ino), COMPUTED,
                      dip->di_crc);
        brelse(bh);
        return finish_inode_fetch(nullptr, -EINVAL);
    }

    // Allocate and parse the in-memory inode
    ip = new XfsInode{};
    ip->ino = ino;
    ip->mount = mount;
    ip->agno = xfs_ino_ag(ino, mount->agino_log);
    ip->agino = xfs_ag_ino(ino, mount->agino_log);

    // Core fields
    ip->mode = dip->di_mode.to_cpu();
    ip->uid = dip->di_uid.to_cpu();
    ip->gid = dip->di_gid.to_cpu();
    ip->nlink = dip->di_nlink.to_cpu();
    ip->size = dip->di_size.to_cpu();
    ip->nblocks = dip->di_nblocks.to_cpu();
    ip->gen = dip->di_gen.to_cpu();
    ip->flags = dip->di_flags.to_cpu();
    ip->flags2 = dip->di_flags2.to_cpu();

    ip->atime = dip->di_atime.to_cpu();
    ip->mtime = dip->di_mtime.to_cpu();
    ip->ctime = dip->di_ctime.to_cpu();
    ip->crtime = dip->di_crtime.to_cpu();

    // NREXT64: extent counts are stored in different fields
    if (xfs_has_nrext64(mount)) {
        // Data extent count in di_pad[0..7] as Be64 (di_big_nextents)
        Be64 big_nextents_be{};
        __builtin_memcpy(&big_nextents_be, dip->di_pad.data(), sizeof(Be64));
        ip->nextents = static_cast<uint32_t>(big_nextents_be.to_cpu());
        // Attr extent count in di_nextents as Be32 (di_big_anextents)
        ip->anextents = dip->di_nextents.to_cpu();
    } else {
        ip->nextents = dip->di_nextents.to_cpu();
        ip->anextents = dip->di_anextents.to_cpu();
    }
    ip->forkoff = dip->di_forkoff;

    // Compute fork sizes
    size_t const INODE_CORE_SIZE = xfs_dinode_size(dip->di_version);
    size_t const INODE_TOTAL = mount->inode_size;
    size_t const DATA_FORK_START = INODE_CORE_SIZE;
    size_t const ATTR_FORK_START = (dip->di_forkoff != 0) ? xfs_dinode_attr_fork_off(dip) : INODE_TOTAL;
    size_t const DATA_FORK_SIZE = ATTR_FORK_START - DATA_FORK_START;
    size_t const ATTR_FORK_SIZE = INODE_TOTAL - ATTR_FORK_START;

    const uint8_t* inode_data = bh->data + OFFSET;

    size_t data_local_size = DATA_FORK_SIZE;
    if (dip->di_format == XFS_DINODE_FMT_LOCAL) {
        data_local_size = static_cast<size_t>(std::min<uint64_t>(ip->size, DATA_FORK_SIZE));
        if (data_local_size == 0 && xfs_inode_isdir(ip) && DATA_FORK_SIZE >= 2) {
            const auto* sf_hdr = reinterpret_cast<const XfsDir2SfHdr*>(inode_data + DATA_FORK_START);
            size_t const SF_HDR_SIZE = xfs_dir2_sf_hdr_size(sf_hdr);
            if (SF_HDR_SIZE <= DATA_FORK_SIZE) {
                data_local_size = SF_HDR_SIZE;
            }
        }
    }

    // Parse data fork
    int rc = parse_ifork(&ip->data_fork, dip->di_format, inode_data + DATA_FORK_START, DATA_FORK_SIZE, data_local_size, ip->nextents);
    if (rc != 0) {
        mod::dbg::log("[xfs] inode %lu: failed to parse data fork (%d)", static_cast<unsigned long>(ino), rc);
        brelse(bh);
        delete ip;
        return finish_inode_fetch(nullptr, rc);
    }

    // Parse attribute fork (if present)
    ip->has_attr_fork = (dip->di_forkoff != 0);
    if (ip->has_attr_fork && ATTR_FORK_SIZE > 0) {
        rc = parse_ifork(&ip->attr_fork, static_cast<uint8_t>(dip->di_aformat), inode_data + ATTR_FORK_START, ATTR_FORK_SIZE,
                         ATTR_FORK_SIZE, ip->anextents);
        if (rc != 0) {
            mod::dbg::log("[xfs] inode %lu: failed to parse attr fork (%d)", static_cast<unsigned long>(ino), rc);
            free_ifork(&ip->data_fork);
            brelse(bh);
            delete ip;
            return finish_inode_fetch(nullptr, rc);
        }
    } else {
        ip->attr_fork.format = XFS_DINODE_FMT_LOCAL;
        ip->attr_fork.local.data = nullptr;
        ip->attr_fork.local.size = 0;
    }

    brelse(bh);

    // Insert into cache
    ip->refcount = 1;
    ip->hash_next = nullptr;
    ip->inactivation_started = false;
    ip->dirty = false;

    flags = icache.at(BUCKET).lock.lock_irqsave();
    // Check for a race - another thread might have loaded the same inode
    unavailable = false;
    XfsInode* existing = icache_lookup_locked(mount, ino, BUCKET, &unavailable);
    if (existing != nullptr) {
        icache.at(BUCKET).lock.unlock_irqrestore(flags);
        free_inode(ip);
        perf_record_xfs_count(ker::mod::perf::WkiPerfLocalXfsOp::INODE_CACHE_HIT, 1);
        return finish_inode_fetch(existing, 0);
    }
    if (unavailable) {
        icache.at(BUCKET).lock.unlock_irqrestore(flags);
        free_inode(ip);
        perf_record_xfs_count(ker::mod::perf::WkiPerfLocalXfsOp::INODE_UNAVAILABLE, 1, -ENOENT);
        return finish_inode_fetch(nullptr, -ENOENT);
    }
    icache_insert_locked(ip, BUCKET);
    icache.at(BUCKET).lock.unlock_irqrestore(flags);

    return finish_inode_fetch(ip, 0);
}

void xfs_inode_release(XfsInode* ip) {
    if (ip == nullptr) {
        return;
    }

    size_t const BUCKET = icache_hash(ip->mount, ip->ino);
    uint64_t flags = icache.at(BUCKET).lock.lock_irqsave();

    if (ip->refcount <= 0) {
        icache.at(BUCKET).lock.unlock_irqrestore(flags);
        return;
    }

    ip->refcount--;
    if (ip->refcount <= 0) {
        bool const NEEDS_INACTIVATION = (ip->nlink == 0 && !ip->inactivation_started);
        if (NEEDS_INACTIVATION) {
            ip->inactivation_started = true;
            icache.at(BUCKET).lock.unlock_irqrestore(flags);
            static_cast<void>(inactivate_unlinked_inode(ip));
            flags = icache.at(BUCKET).lock.lock_irqsave();
        }

        if (ip->nlink != 0) {
            ip->refcount = 0;
            size_t const IDLE_COUNT = icache_idle_count.fetch_add(1, std::memory_order_relaxed) + 1;
            icache.at(BUCKET).lock.unlock_irqrestore(flags);
            if (IDLE_COUNT > ICACHE_IDLE_RETAIN_LIMIT) {
                reclaim_idle_inodes();
            }
            return;
        }

        icache_remove_locked(ip, BUCKET);
        icache.at(BUCKET).lock.unlock_irqrestore(flags);
        free_inode(ip);
        return;
    }

    icache.at(BUCKET).lock.unlock_irqrestore(flags);
}

auto xfs_inode_write(XfsInode* ip, XfsTransaction* tp) -> int {
    if (ip == nullptr || ip->mount == nullptr) {
        return -EINVAL;
    }

    XfsMountContext* mount = ip->mount;
    xfs_fsblock_t const BLOCK = xfs_inode_block(mount, ip->ino);
    size_t const OFFSET = xfs_inode_offset(mount, ip->ino);

    BufHead* bh = xfs_buf_read(mount, BLOCK);
    if (bh == nullptr) {
        mod::dbg::log("[xfs] inode write: failed to read block %lu", static_cast<unsigned long>(BLOCK));
        return -EIO;
    }

    if (OFFSET + mount->inode_size > bh->size) {
        brelse(bh);
        return -EIO;
    }

    auto* dip = reinterpret_cast<XfsDinode*>(bh->data + OFFSET);

    // Serialize core fields back to on-disk format
    dip->di_mode = Be16::from_cpu(ip->mode);
    dip->di_uid = Be32::from_cpu(ip->uid);
    dip->di_gid = Be32::from_cpu(ip->gid);
    dip->di_nlink = Be32::from_cpu(ip->nlink);
    dip->di_size = Be64::from_cpu(ip->size);
    dip->di_nblocks = Be64::from_cpu(ip->nblocks);
    dip->di_gen = Be32::from_cpu(ip->gen);
    dip->di_flags = Be16::from_cpu(ip->flags);
    dip->di_flags2 = Be64::from_cpu(ip->flags2);

    dip->di_atime = xfs_timestamp_t::from_cpu(ip->atime);
    dip->di_mtime = xfs_timestamp_t::from_cpu(ip->mtime);
    dip->di_ctime = xfs_timestamp_t::from_cpu(ip->ctime);
    dip->di_crtime = xfs_timestamp_t::from_cpu(ip->crtime);

    // NREXT64: extent counts are stored in different fields (mirrors read path)
    if (xfs_has_nrext64(mount)) {
        // Data extent count => di_pad[0..7] as Be64
        Be64 big_nextents_be = Be64::from_cpu(static_cast<uint64_t>(ip->nextents));
        __builtin_memcpy(dip->di_pad.data(), &big_nextents_be, sizeof(Be64));
        // Attr extent count => di_nextents as Be32
        dip->di_nextents = Be32::from_cpu(static_cast<uint32_t>(ip->anextents));
    } else {
        dip->di_nextents = Be32::from_cpu(ip->nextents);
        dip->di_anextents = Be16::from_cpu(ip->anextents);
    }
    dip->di_forkoff = ip->forkoff;
    dip->di_format = static_cast<uint8_t>(ip->data_fork.format);

    // Serialize data fork
    size_t const INODE_CORE_SIZE = xfs_dinode_size(dip->di_version);
    uint8_t* fork_data = bh->data + OFFSET + INODE_CORE_SIZE;

    switch (ip->data_fork.format) {
        case XFS_DINODE_FMT_LOCAL:
            if (ip->data_fork.local.data != nullptr && ip->data_fork.local.size > 0) {
                __builtin_memcpy(fork_data, ip->data_fork.local.data, ip->data_fork.local.size);
            }
            break;

        case XFS_DINODE_FMT_EXTENTS: {
            auto* recs = reinterpret_cast<XfsBmbtRec*>(fork_data);
            for (uint32_t i = 0; i < ip->data_fork.extents.count; i++) {
                recs[i] = xfs_bmbt_rec_pack(ip->data_fork.extents.list[i]);
            }
            break;
        }

        case XFS_DINODE_FMT_BTREE:
            // Copy the btree root data back
            if (ip->data_fork.btree.root != nullptr && ip->data_fork.btree.root_size > 0) {
                __builtin_memcpy(fork_data, ip->data_fork.btree.root, ip->data_fork.btree.root_size);
            }
            break;

        default:
            break;
    }

    // Serialize attribute fork (if present)
    if (ip->has_attr_fork && ip->forkoff > 0) {
        dip->di_aformat = static_cast<int8_t>(ip->attr_fork.format);
        size_t const ATTR_FORK_OFFSET = static_cast<size_t>(ip->forkoff) << 3;
        uint8_t* attr_data = fork_data + ATTR_FORK_OFFSET;

        switch (ip->attr_fork.format) {
            case XFS_DINODE_FMT_LOCAL:
                if (ip->attr_fork.local.data != nullptr && ip->attr_fork.local.size > 0) {
                    __builtin_memcpy(attr_data, ip->attr_fork.local.data, ip->attr_fork.local.size);
                }
                break;

            case XFS_DINODE_FMT_EXTENTS: {
                auto* recs = reinterpret_cast<XfsBmbtRec*>(attr_data);
                for (uint32_t i = 0; i < ip->attr_fork.extents.count; i++) {
                    recs[i] = xfs_bmbt_rec_pack(ip->attr_fork.extents.list[i]);
                }
                break;
            }

            case XFS_DINODE_FMT_BTREE:
                if (ip->attr_fork.btree.root != nullptr && ip->attr_fork.btree.root_size > 0) {
                    __builtin_memcpy(attr_data, ip->attr_fork.btree.root, ip->attr_fork.btree.root_size);
                }
                break;

            default:
                break;
        }
    }

    // Recompute the CRC over the entire inode
    dip->di_crc = 0;
    uint32_t const CRC = util::crc32c_block_with_cksum(dip, mount->inode_size, XFS_DINODE_CRC_OFF);
    dip->di_crc = CRC;  // di_crc is stored little-endian on disk

    // Log the inode buffer through the transaction
    if (tp != nullptr) {
        xfs_trans_log_buf(tp, bh, static_cast<uint32_t>(OFFSET), static_cast<uint32_t>(mount->inode_size));
    }
    brelse(bh);

    ip->dirty = false;

    return 0;
}

}  // namespace ker::vfs::xfs
