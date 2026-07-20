// XFS Inode read / cache implementation.
//
// Reads inodes from disk via the buffer cache, validates CRC, parses the
// xfs_dinode into the in-memory XfsInode struct, and caches them in a hash
// table for reuse.
//
// Reference: reference/xfs/libxfs/xfs_inode_buf.c, reference/xfs/xfs_icache.c

#include "xfs_inode.hpp"

#include <bits/off_t.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/phys.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sys/mutex.hpp>
#include <platform/sys/spinlock.hpp>
#include <util/crc32c.hpp>
#include <utility>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_alloc.hpp>
#include <vfs/fs/xfs/xfs_bmap.hpp>
#include <vfs/fs/xfs/xfs_btree.hpp>
#include <vfs/fs/xfs/xfs_ialloc.hpp>
#include <vfs/fs/xfs/xfs_trans.hpp>
#include <vfs/stat.hpp>

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

constexpr size_t ICACHE_BUCKETS = 16384;
constexpr size_t ICACHE_HASH_MASK = ICACHE_BUCKETS - 1;
static_assert((ICACHE_BUCKETS & (ICACHE_BUCKETS - 1)) == 0);
constexpr size_t ICACHE_IDLE_RETAIN_MIN = 65536;
constexpr size_t ICACHE_IDLE_RETAIN_MAX = 524288;
constexpr uint64_t ICACHE_IDLE_RETAIN_BYTES_PER_INODE = 32ULL * 1024ULL;
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
std::atomic<size_t> icache_idle_retain_limit_cached{0};

constexpr size_t XFS_INODE_ARENA_BYTES = size_t{256} * 1024;
constexpr size_t XFS_INODE_STRIDE = (sizeof(XfsInode) + alignof(XfsInode) - 1) & ~(alignof(XfsInode) - 1);
static_assert(XFS_INODE_STRIDE >= sizeof(XfsInode));

struct XfsInodePoolNode {
    XfsInodePoolNode* next;
};

struct XfsInodeObjectPool {
    mod::sys::Spinlock lock;
    XfsInodePoolNode* free_list{};
};

XfsInodeObjectPool inode_object_pool{};

void xfs_inode_pool_add_arena_locked(void* arena, size_t bytes) {
    auto* next = static_cast<uint8_t*>(arena);
    size_t remaining = bytes;
    while (remaining >= XFS_INODE_STRIDE) {
        auto* node = reinterpret_cast<XfsInodePoolNode*>(next);
        node->next = inode_object_pool.free_list;
        inode_object_pool.free_list = node;
        next += XFS_INODE_STRIDE;
        remaining -= XFS_INODE_STRIDE;
    }
}

auto xfs_inode_pool_pop() -> XfsInodePoolNode* {
    uint64_t const IRQF = inode_object_pool.lock.lock_irqsave();
    XfsInodePoolNode* node = inode_object_pool.free_list;
    if (node != nullptr) {
        inode_object_pool.free_list = node->next;
        node->next = nullptr;
    }
    inode_object_pool.lock.unlock_irqrestore(IRQF);
    return node;
}

auto xfs_inode_pool_alloc_slot() -> void* {
    if (XfsInodePoolNode* node = xfs_inode_pool_pop()) {
        return node;
    }

    void* const ARENA = ker::mod::mm::phys::page_alloc_full_overwrite(XFS_INODE_ARENA_BYTES, "xfs_inodes");
    if (ARENA == nullptr) {
        return nullptr;
    }

    uint64_t const IRQF = inode_object_pool.lock.lock_irqsave();
    xfs_inode_pool_add_arena_locked(ARENA, XFS_INODE_ARENA_BYTES);
    XfsInodePoolNode* node = inode_object_pool.free_list;
    if (node != nullptr) {
        inode_object_pool.free_list = node->next;
        node->next = nullptr;
    }
    inode_object_pool.lock.unlock_irqrestore(IRQF);
    return node;
}

void xfs_inode_pool_release_slot(XfsInode* ip) {
    if (ip == nullptr) {
        return;
    }
    ip->~XfsInode();
    auto* node = reinterpret_cast<XfsInodePoolNode*>(ip);
    uint64_t const IRQF = inode_object_pool.lock.lock_irqsave();
    node->next = inode_object_pool.free_list;
    inode_object_pool.free_list = node;
    inode_object_pool.lock.unlock_irqrestore(IRQF);
}

auto icache_hash(const XfsMountContext* mount, xfs_ino_t ino) -> size_t {
    auto const MOUNT_BITS = reinterpret_cast<uintptr_t>(mount) >> 6;
    auto const MIXED = (ino * 2654435761ULL) ^ static_cast<uint64_t>(MOUNT_BITS);
    return static_cast<size_t>(MIXED & ICACHE_HASH_MASK);
}

auto icache_idle_retain_limit() -> size_t {
    size_t cached = icache_idle_retain_limit_cached.load(std::memory_order_acquire);
    if (cached != 0) {
        return cached;
    }

    uint64_t const TOTAL_MEM = ker::mod::mm::phys::get_total_mem_bytes();
    uint64_t scaled = TOTAL_MEM / ICACHE_IDLE_RETAIN_BYTES_PER_INODE;
    if (scaled == 0) {
        scaled = ICACHE_IDLE_RETAIN_MIN;
    }
    uint64_t const RETAIN_LIMIT = std::clamp<uint64_t>(scaled, ICACHE_IDLE_RETAIN_MIN, ICACHE_IDLE_RETAIN_MAX);
    cached = static_cast<size_t>(std::min<uint64_t>(RETAIN_LIMIT, static_cast<uint64_t>(SIZE_MAX)));
    icache_idle_retain_limit_cached.store(cached, std::memory_order_release);
    return cached;
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

auto extent_record_capacity(size_t fork_size) -> uint32_t {
    size_t const CAPACITY = fork_size / sizeof(XfsBmbtRec);
    return CAPACITY > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(CAPACITY);
}

auto bmdr_record_capacity(size_t fork_size) -> uint32_t {
    if (fork_size < sizeof(XfsBmdrBlock)) {
        return 0;
    }
    size_t const CAPACITY = (fork_size - sizeof(XfsBmdrBlock)) / (sizeof(XfsBmbtKey) + sizeof(Be64));
    return CAPACITY > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(CAPACITY);
}

auto bmdr_root_min_size(uint32_t maxrecs, uint16_t numrecs) -> size_t {
    if (numrecs > maxrecs) {
        return SIZE_MAX;
    }
    return sizeof(XfsBmdrBlock) + (static_cast<size_t>(maxrecs) * sizeof(XfsBmbtKey)) + (static_cast<size_t>(numrecs) * sizeof(Be64));
}

auto validate_bmdr_root(const uint8_t* root, size_t root_size, size_t fork_size, uint32_t nextents) -> int {
    if (root == nullptr || root_size < sizeof(XfsBmdrBlock)) {
        return -EIO;
    }

    uint32_t const MAXRECS = bmdr_record_capacity(fork_size);
    if (MAXRECS == 0) {
        return -EIO;
    }

    const auto* bmdr = reinterpret_cast<const XfsBmdrBlock*>(root);
    uint16_t const LEVEL = bmdr->bb_level.to_cpu();
    uint16_t const NUMRECS = bmdr->bb_numrecs.to_cpu();
    if (NUMRECS > MAXRECS) {
        return -EIO;
    }
    size_t const MIN_SIZE = bmdr_root_min_size(MAXRECS, NUMRECS);
    if (MIN_SIZE == SIZE_MAX || root_size < MIN_SIZE) {
        return -EIO;
    }
    if (nextents != 0 && (NUMRECS == 0 || LEVEL == 0 || LEVEL > XFS_BTREE_MAXLEVELS)) {
        return -EIO;
    }
    if (nextents == 0 && NUMRECS != 0 && (LEVEL == 0 || LEVEL > XFS_BTREE_MAXLEVELS)) {
        return -EIO;
    }
    return 0;
}

auto data_fork_size_for_write(const XfsInode* ip, size_t inode_core_size) -> size_t {
    if (ip == nullptr || ip->mount == nullptr || ip->mount->inode_size <= inode_core_size) {
        return 0;
    }
    size_t const INODE_TOTAL = ip->mount->inode_size;
    if (ip->forkoff == 0) {
        return INODE_TOTAL - inode_core_size;
    }
    size_t const ATTR_FORK_START = inode_core_size + (static_cast<size_t>(ip->forkoff) << 3U);
    if (ATTR_FORK_START < inode_core_size || ATTR_FORK_START > INODE_TOTAL) {
        return 0;
    }
    return ATTR_FORK_START - inode_core_size;
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

auto retain_active_cached_inode(XfsInode* ip) -> XfsInode* {
    if (ip == nullptr || ip->mount == nullptr || ip->ino == NULLFSINO) {
        return nullptr;
    }

    size_t const BUCKET = icache_hash(ip->mount, ip->ino);
    uint64_t const FLAGS = icache.at(BUCKET).lock.lock_irqsave();
    if (ip->refcount <= 0 || ip->inactivation_started) {
        icache.at(BUCKET).lock.unlock_irqrestore(FLAGS);
        return nullptr;
    }

    ip->refcount++;
    icache.at(BUCKET).lock.unlock_irqrestore(FLAGS);
    return ip;
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
                if (!xfs_ifork_extents_uses_inline(fork->extents)) {
                    delete[] fork->extents.list;
                }
                fork->extents.list = nullptr;
            }
            fork->extents.count = 0;
            fork->extents.capacity = 0;
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
    xfs_inode_pool_release_slot(ip);
}

void reclaim_idle_inodes() {
    size_t const RETAIN_LIMIT = icache_idle_retain_limit();
    if (icache_idle_count.load(std::memory_order_relaxed) <= RETAIN_LIMIT) {
        return;
    }

    std::array<XfsInode*, ICACHE_RECLAIM_BATCH> victims{};
    size_t victim_count = 0;

    for (auto& bucket : icache) {
        uint64_t const FLAGS = bucket.lock.lock_irqsave();
        XfsInode** pp = &bucket.head;
        while (*pp != nullptr && victim_count < victims.size() && icache_idle_count.load(std::memory_order_relaxed) > RETAIN_LIMIT) {
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

        if (victim_count == victims.size() || icache_idle_count.load(std::memory_order_relaxed) <= RETAIN_LIMIT) {
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

auto inode_data_extent_record_valid(const XfsBmbtIrec& rec) -> bool {
    constexpr xfs_fileoff_t MAX_STARTOFF = 0x3FFFFFFFFFFFFFULL;
    constexpr xfs_fsblock_t MAX_STARTBLOCK = 0xFFFFFFFFFFFFFULL;
    constexpr xfs_filblks_t MAX_BLOCKCOUNT = 0x1FFFFFULL;

    if (rec.br_blockcount == 0 || rec.br_blockcount > MAX_BLOCKCOUNT || rec.br_startoff > MAX_STARTOFF ||
        rec.br_startblock == NULLFSBLOCK || rec.br_startblock > MAX_STARTBLOCK) {
        return false;
    }

    return rec.br_blockcount <= (MAX_STARTOFF - rec.br_startoff) + 1 && rec.br_blockcount <= (MAX_STARTBLOCK - rec.br_startblock) + 1;
}

auto validate_inode_data_extent(XfsInode* ip, xfs_fsblock_t startblock, xfs_filblks_t blockcount) -> int {
    if (ip == nullptr || ip->mount == nullptr) {
        return -EINVAL;
    }
    if (startblock == NULLFSBLOCK || blockcount == 0) {
        return -EIO;
    }

    auto* mount = ip->mount;
    xfs_agnumber_t agno = xfs_ag_number(startblock, mount->ag_blk_log);
    xfs_agblock_t agbno = xfs_ag_block(startblock, mount->ag_blk_log);
    xfs_filblks_t remaining = blockcount;

    while (remaining > 0) {
        if (agno >= mount->ag_count || agbno >= mount->ag_blocks) {
            mod::dbg::logger<"xfs">::error(
                "xfs inode extent validation failed ino=%lu startblock=%lu blockcount=%lu agno=%u agbno=%u remaining=%lu",
                static_cast<unsigned long>(ip->ino), static_cast<unsigned long>(startblock), static_cast<unsigned long>(blockcount), agno,
                agbno, static_cast<unsigned long>(remaining));
            return -EIO;
        }

        auto const AG_REMAINING = static_cast<xfs_filblks_t>(mount->ag_blocks - agbno);
        xfs_filblks_t const SPAN = std::min(remaining, AG_REMAINING);
        if (SPAN == 0 || SPAN > static_cast<xfs_filblks_t>(UINT32_MAX)) {
            return -EIO;
        }

        int const RC = xfs_validate_allocated_extent(mount, agno, agbno, static_cast<xfs_extlen_t>(SPAN));
        if (RC != 0) {
            mod::dbg::logger<"xfs">::error(
                "xfs inode extent overlaps free space ino=%lu startblock=%lu blockcount=%lu agno=%u agbno=%u span=%lu rc=%d",
                static_cast<unsigned long>(ip->ino), static_cast<unsigned long>(startblock), static_cast<unsigned long>(blockcount), agno,
                agbno, static_cast<unsigned long>(SPAN), RC);
            return RC;
        }

        remaining -= SPAN;
        agno++;
        agbno = 0;
    }

    return 0;
}

auto validate_inode_extent_records(XfsInode* ip, const XfsBmbtIrec* extents, uint32_t count) -> int {
    if (extents == nullptr && count != 0) {
        return -EIO;
    }

    xfs_fileoff_t previous_end = 0;
    bool have_previous = false;
    for (uint32_t i = 0; i < count; i++) {
        XfsBmbtIrec const& rec = extents[i];
        if (!inode_data_extent_record_valid(rec)) {
            mod::dbg::logger<"xfs">::error("xfs inode extent record invalid ino=%lu index=%u startoff=%lu startblock=%lu blockcount=%lu",
                                           static_cast<unsigned long>(ip != nullptr ? ip->ino : 0), i,
                                           static_cast<unsigned long>(rec.br_startoff), static_cast<unsigned long>(rec.br_startblock),
                                           static_cast<unsigned long>(rec.br_blockcount));
            return -EIO;
        }
        if (have_previous && rec.br_startoff < previous_end) {
            mod::dbg::logger<"xfs">::error("xfs inode extent records overlap logically ino=%lu index=%u startoff=%lu previous_end=%lu",
                                           static_cast<unsigned long>(ip != nullptr ? ip->ino : 0), i,
                                           static_cast<unsigned long>(rec.br_startoff), static_cast<unsigned long>(previous_end));
            return -EIO;
        }

        int const RC = validate_inode_data_extent(ip, rec.br_startblock, rec.br_blockcount);
        if (RC != 0) {
            return RC;
        }

        previous_end = rec.br_startoff + rec.br_blockcount;
        have_previous = true;
    }

    return 0;
}

auto free_inode_data_extent(XfsInode* ip, XfsTransaction* tp, xfs_fsblock_t startblock, xfs_filblks_t blockcount) -> int {
    if (ip == nullptr || ip->mount == nullptr || tp == nullptr || startblock == NULLFSBLOCK || blockcount == 0) {
        return 0;
    }

    auto* mount = ip->mount;
    xfs_agnumber_t agno = xfs_ag_number(startblock, mount->ag_blk_log);
    xfs_agblock_t agbno = xfs_ag_block(startblock, mount->ag_blk_log);
    xfs_filblks_t remaining = blockcount;
    uint64_t const DEV_BLOCK = inode_fsblock_to_dev_block(mount, startblock);
    size_t const DEV_COUNT = inode_fsb_to_dev_count(mount, blockcount);

    if (mount->device != nullptr) {
        discard_bdev_range(mount->device, DEV_BLOCK, DEV_COUNT);
        if (has_dirty_bdev_range(mount->device, DEV_BLOCK, DEV_COUNT)) {
            int const SYNC_RC = sync_bdev_range(mount->device, DEV_BLOCK, DEV_COUNT);
            if (SYNC_RC != 0) {
                return SYNC_RC;
            }
            discard_bdev_range(mount->device, DEV_BLOCK, DEV_COUNT);
        }
    }

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

    if (mount->device != nullptr) {
        discard_bdev_range(mount->device, DEV_BLOCK, DEV_COUNT);
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
    if (ip == nullptr || ip->data_fork.format == XFS_DINODE_FMT_LOCAL) {
        return 0;
    }

    if (ip->data_fork.format == XFS_DINODE_FMT_EXTENTS) {
        if (ip->data_fork.extents.count != 0 && ip->data_fork.extents.list == nullptr) {
            return -EIO;
        }
        int const VALID_RC = validate_inode_extent_records(ip, ip->data_fork.extents.list, ip->data_fork.extents.count);
        if (VALID_RC != 0) {
            return VALID_RC;
        }
        return free_inode_extent_records(ip, tp, ip->data_fork.extents.list, ip->data_fork.extents.count);
    }

    if (ip->data_fork.format != XFS_DINODE_FMT_BTREE) {
        return 0;
    }

    int rc = 0;
    if (ip->nextents != 0) {
        if (ip->nextents == UINT32_MAX) {
            return -EFBIG;
        }
        uint32_t const LIST_CAPACITY = ip->nextents + 1;
        auto* extents = new (std::nothrow) XfsBmbtIrec[LIST_CAPACITY];
        if (extents == nullptr) {
            return -ENOMEM;
        }

        rc = xfs_bmap_list_extents(ip, extents, LIST_CAPACITY);
        if (rc >= 0) {
            if (std::cmp_not_equal(rc, ip->nextents)) {
                mod::dbg::logger<"xfs">::error("xfs inode btree extent count mismatch ino=%lu expected=%u listed=%d",
                                               static_cast<unsigned long>(ip->ino), ip->nextents, rc);
                rc = -EIO;
            } else {
                rc = validate_inode_extent_records(ip, extents, static_cast<uint32_t>(rc));
            }
        }
        if (rc >= 0) {
            rc = free_inode_extent_records(ip, tp, extents, static_cast<uint32_t>(rc));
        }

        delete[] extents;
        if (rc < 0) {
            return rc;
        }
    }

    return xfs_bmap_free_btree_blocks(ip, tp);
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
            uint32_t const CAPACITY = extent_record_capacity(data_size);
            if (nextents > CAPACITY) {
                mod::dbg::log("[xfs] inode fork extents exceed inline capacity: nextents=%u capacity=%u data_size=%lu", nextents, CAPACITY,
                              static_cast<unsigned long>(data_size));
                return -EFBIG;
            }
            fork->extents.count = nextents;
            if (nextents == 0) {
                fork->extents.list = nullptr;
                fork->extents.capacity = 0;
                break;
            }
            if (nextents <= XFS_IFORK_INLINE_EXTENT_CAPACITY) {
                fork->extents.list = xfs_ifork_extents_inline_data(fork->extents);
                fork->extents.capacity = XFS_IFORK_INLINE_EXTENT_CAPACITY;
            } else {
                fork->extents.capacity = nextents;
                fork->extents.list = new (std::nothrow) XfsBmbtIrec[nextents];
                if (fork->extents.list == nullptr) {
                    return -ENOMEM;
                }
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
            int const VALID_ROOT = validate_bmdr_root(data_ptr, data_size, data_size, nextents);
            if (VALID_ROOT != 0) {
                return VALID_ROOT;
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
                mount->metadata_lock.lock();
                int const RET = xfs_commit_cached_dirty_inode(mount, ip);
                mount->metadata_lock.unlock();
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

auto xfs_inode_trim_data_to_size(XfsInode* ip, XfsTransaction* tp, uint64_t new_size) -> int {
    if (ip == nullptr || ip->mount == nullptr || tp == nullptr) {
        return -EINVAL;
    }
    if (ip->data_fork.format != XFS_DINODE_FMT_EXTENTS || ip->data_fork.extents.count == 0) {
        return 0;
    }

    XfsIforkExtents& extents = ip->data_fork.extents;
    if (extents.list == nullptr) {
        if (extents.count > XFS_IFORK_INLINE_EXTENT_CAPACITY) {
            return -EIO;
        }
        extents.list = xfs_ifork_extents_inline_data(extents);
        extents.capacity = XFS_IFORK_INLINE_EXTENT_CAPACITY;
    }

    auto* mount = ip->mount;
    auto const KEEP_BLOCKS = static_cast<xfs_fileoff_t>((new_size + mount->block_size - 1) >> mount->block_log);
    uint64_t freed_blocks = 0;

    uint32_t out = 0;
    for (uint32_t i = 0; i < extents.count; ++i) {
        XfsBmbtIrec rec = extents.list[i];
        xfs_fileoff_t const REC_END = rec.br_startoff + rec.br_blockcount;
        if (rec.br_startoff >= KEEP_BLOCKS) {
            int const RC = free_inode_data_extent(ip, tp, rec.br_startblock, rec.br_blockcount);
            if (RC != 0) {
                return RC;
            }
            freed_blocks += rec.br_blockcount;
            continue;
        }

        if (REC_END > KEEP_BLOCKS) {
            auto const KEEP_LEN = static_cast<xfs_filblks_t>(KEEP_BLOCKS - rec.br_startoff);
            xfs_filblks_t const FREE_LEN = rec.br_blockcount - KEEP_LEN;
            xfs_fsblock_t const FREE_START = rec.br_startblock + KEEP_LEN;
            int const RC = free_inode_data_extent(ip, tp, FREE_START, FREE_LEN);
            if (RC != 0) {
                return RC;
            }
            rec.br_blockcount = KEEP_LEN;
            freed_blocks += FREE_LEN;
        }

        if (rec.br_blockcount != 0) {
            extents.list[out++] = rec;
        }
    }

    if (freed_blocks == 0) {
        return 0;
    }

    extents.count = out;
    ip->nextents = out;
    ip->nblocks = ip->nblocks > freed_blocks ? ip->nblocks - freed_blocks : 0;
    ip->dirty = true;
    return 0;
}

namespace {
auto xfs_inode_read_impl(XfsMountContext* mount, xfs_ino_t ino, bool allocation_known) -> XfsInode* {
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

    if (!allocation_known) {
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
    ip = xfs_inode_alloc_zeroed_object();
    if (ip == nullptr) {
        brelse(bh);
        return finish_inode_fetch(nullptr, -ENOMEM);
    }
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

    if (allocation_known && ip->nlink == 0) {
        brelse(bh);
        free_inode(ip);
        return finish_inode_fetch(nullptr, -ENOENT);
    }

    ip->atime = dip->di_atime.to_cpu();
    ip->mtime = dip->di_mtime.to_cpu();
    ip->ctime = dip->di_ctime.to_cpu();
    ip->crtime = dip->di_crtime.to_cpu();

    // The filesystem feature only permits large counters. Each inode selects
    // the large-counter union fields independently with XFS_DIFLAG2_NREXT64.
    bool const LARGE_EXTENT_COUNTS = (ip->flags2 & XFS_DIFLAG2_NREXT64) != 0;
    if (LARGE_EXTENT_COUNTS && !xfs_has_nrext64(mount)) {
        brelse(bh);
        free_inode(ip);
        return finish_inode_fetch(nullptr, -EINVAL);
    }
    if (LARGE_EXTENT_COUNTS) {
        // Data extent count in di_pad[0..7] as Be64 (di_big_nextents)
        Be64 big_nextents_be{};
        __builtin_memcpy(&big_nextents_be, dip->di_pad.data(), sizeof(Be64));
        uint64_t const BIG_NEXTENTS = big_nextents_be.to_cpu();
        uint32_t const BIG_ANEXTENTS = dip->di_nextents.to_cpu();
        if (BIG_NEXTENTS > UINT32_MAX || BIG_ANEXTENTS > UINT16_MAX || dip->di_anextents.to_cpu() != 0) {
            brelse(bh);
            free_inode(ip);
            return finish_inode_fetch(nullptr, -EFBIG);
        }
        ip->nextents = static_cast<uint32_t>(BIG_NEXTENTS);
        // Attr extent count in di_nextents as Be32 (di_big_anextents)
        ip->anextents = static_cast<uint16_t>(BIG_ANEXTENTS);
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
    if (INODE_CORE_SIZE > INODE_TOTAL || ATTR_FORK_START < DATA_FORK_START || ATTR_FORK_START > INODE_TOTAL) {
        mod::dbg::log("[xfs] inode %lu: invalid fork geometry core=%lu attr=%lu total=%lu", static_cast<unsigned long>(ino),
                      static_cast<unsigned long>(INODE_CORE_SIZE), static_cast<unsigned long>(ATTR_FORK_START),
                      static_cast<unsigned long>(INODE_TOTAL));
        brelse(bh);
        free_inode(ip);
        return finish_inode_fetch(nullptr, -EINVAL);
    }
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
        free_inode(ip);
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
            free_inode(ip);
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
}  // namespace

auto xfs_inode_alloc_zeroed_object() -> XfsInode* {
    void* const SLOT = xfs_inode_pool_alloc_slot();
    if (SLOT == nullptr) {
        return nullptr;
    }
    return new (SLOT) XfsInode{};
}

auto xfs_inode_alloc_uninitialized_object() -> XfsInode* {
    void* const SLOT = xfs_inode_pool_alloc_slot();
    if (SLOT == nullptr) {
        return nullptr;
    }
    return new (SLOT) XfsInode;
}

void xfs_inode_free_uncached(XfsInode* ip) {
    if (ip == nullptr) {
        return;
    }
    free_inode(ip);
}

auto xfs_inode_read(XfsMountContext* mount, xfs_ino_t ino) -> XfsInode* { return xfs_inode_read_impl(mount, ino, false); }

auto xfs_inode_read_known_allocated(XfsMountContext* mount, xfs_ino_t ino) -> XfsInode* { return xfs_inode_read_impl(mount, ino, true); }

auto xfs_inode_read_cached(XfsMountContext* mount, xfs_ino_t ino) -> XfsInode* {
    if (mount == nullptr || ino == NULLFSINO) {
        return nullptr;
    }

    xfs_icache_init();

    size_t const BUCKET = icache_hash(mount, ino);
    uint64_t const FLAGS = icache.at(BUCKET).lock.lock_irqsave();
    XfsInode* ip = icache_lookup_locked(mount, ino, BUCKET);
    icache.at(BUCKET).lock.unlock_irqrestore(FLAGS);
    return ip;
}

auto xfs_root_inode_read(XfsMountContext* mount) -> XfsInode* {
    if (mount == nullptr) {
        return nullptr;
    }
    if (mount->root_inode == nullptr) {
        return xfs_inode_read(mount, mount->root_ino);
    }
    return retain_active_cached_inode(mount->root_inode);
}

auto xfs_inode_cache_new(XfsInode* ip) -> int {
    if (ip == nullptr || ip->mount == nullptr || ip->ino == NULLFSINO) {
        return -EINVAL;
    }

    xfs_icache_init();

    size_t const BUCKET = icache_hash(ip->mount, ip->ino);
    uint64_t const FLAGS = icache.at(BUCKET).lock.lock_irqsave();
    for (XfsInode* existing = icache.at(BUCKET).head; existing != nullptr; existing = existing->hash_next) {
        if (existing->mount == ip->mount && existing->ino == ip->ino) {
            icache.at(BUCKET).lock.unlock_irqrestore(FLAGS);
            return -EEXIST;
        }
    }

    ip->refcount = 1;
    ip->hash_next = nullptr;
    ip->inactivation_started = false;
    ip->dirty = false;
    icache_insert_locked(ip, BUCKET);
    icache.at(BUCKET).lock.unlock_irqrestore(FLAGS);
    return 0;
}

namespace {

void release_inode_reference(XfsInode* ip, bool metadata_locked) {
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
        int inactivation_rc = 0;
        if (NEEDS_INACTIVATION) {
            ip->inactivation_started = true;
            icache.at(BUCKET).lock.unlock_irqrestore(flags);
            if (!metadata_locked && ip->mount != nullptr) {
                ip->io_lock.lock();
                ip->mount->metadata_lock.lock();
            } else {
                ip->io_lock.lock();
            }
            inactivation_rc = inactivate_unlinked_inode(ip);
            ip->io_lock.unlock();
            if (!metadata_locked && ip->mount != nullptr) {
                ip->mount->metadata_lock.unlock();
            }
            flags = icache.at(BUCKET).lock.lock_irqsave();
        }

        if (inactivation_rc != 0 && ip->nlink == 0) {
            ip->inactivation_started = false;
            ip->refcount = 0;
            icache.at(BUCKET).lock.unlock_irqrestore(flags);
            return;
        }

        if (ip->nlink != 0) {
            ip->refcount = 0;
            size_t const IDLE_COUNT = icache_idle_count.fetch_add(1, std::memory_order_relaxed) + 1;
            icache.at(BUCKET).lock.unlock_irqrestore(flags);
            if (IDLE_COUNT > icache_idle_retain_limit()) {
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

}  // namespace

void xfs_inode_release(XfsInode* ip) { release_inode_reference(ip, false); }

void xfs_inode_release_metadata_locked(XfsInode* ip) { release_inode_reference(ip, true); }

namespace {
void clear_stat_abi_tail(ker::vfs::Stat* st) {
    st->unused[0] = 0;
    st->unused[1] = 0;
    st->unused[2] = 0;
}
}  // namespace

auto xfs_inode_fill_stat(const XfsInode* ip, ker::vfs::Stat* st) -> int {
    if (ip == nullptr || ip->mount == nullptr || st == nullptr) {
        return -EINVAL;
    }

    st->st_dev = ip->mount->dev_id;
    st->st_ino = ip->ino;
    st->st_nlink = ip->nlink;
    st->st_mode = ip->mode;
    st->st_uid = ip->uid;
    st->st_gid = ip->gid;
    st->pad0 = 0;
    st->st_rdev = 0;
    st->st_size = static_cast<off_t>(ip->size);
    st->st_blksize = static_cast<ker::vfs::blksize_t>(ip->mount->block_size);
    st->st_blocks = static_cast<ker::vfs::blkcnt_t>(ip->nblocks * (ip->mount->block_size / 512));

    bool const BIGTIME = (ip->flags2 & XFS_DIFLAG2_BIGTIME) != 0;
    if (BIGTIME) {
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
    clear_stat_abi_tail(st);

    return 0;
}

namespace {
void fill_stat_from_dinode(XfsMountContext* mount, xfs_ino_t ino, const XfsDinode* dip, ker::vfs::Stat* st) {
    st->st_dev = mount->dev_id;
    st->st_ino = ino;
    st->st_nlink = dip->di_nlink.to_cpu();
    st->st_mode = dip->di_mode.to_cpu();
    st->st_uid = dip->di_uid.to_cpu();
    st->st_gid = dip->di_gid.to_cpu();
    st->pad0 = 0;
    st->st_rdev = 0;
    st->st_size = static_cast<off_t>(dip->di_size.to_cpu());
    st->st_blksize = static_cast<ker::vfs::blksize_t>(mount->block_size);
    st->st_blocks = static_cast<ker::vfs::blkcnt_t>(dip->di_nblocks.to_cpu() * (mount->block_size / 512));

    uint64_t const FLAGS2 = dip->di_flags2.to_cpu();
    bool const BIGTIME = (FLAGS2 & XFS_DIFLAG2_BIGTIME) != 0;
    if (BIGTIME) {
        constexpr int64_t XFS_BIGTIME_EPOCH_OFFSET = (1LL << 31);
        constexpr uint64_t NSEC_PER_SEC = 1000000000ULL;
        auto decode = [&](uint64_t raw, struct Timespec& ts) {
            ts.tv_sec = static_cast<int64_t>(raw / NSEC_PER_SEC) - XFS_BIGTIME_EPOCH_OFFSET;
            ts.tv_nsec = static_cast<int64_t>(raw % NSEC_PER_SEC);
        };
        decode(dip->di_atime.to_cpu(), st->st_atim);
        decode(dip->di_mtime.to_cpu(), st->st_mtim);
        decode(dip->di_ctime.to_cpu(), st->st_ctim);
    } else {
        uint64_t const ATIME = dip->di_atime.to_cpu();
        uint64_t const MTIME = dip->di_mtime.to_cpu();
        uint64_t const CTIME = dip->di_ctime.to_cpu();
        st->st_atim.tv_sec = static_cast<int64_t>(ATIME >> 32);
        st->st_atim.tv_nsec = static_cast<int64_t>(ATIME & 0xFFFFFFFF);
        st->st_mtim.tv_sec = static_cast<int64_t>(MTIME >> 32);
        st->st_mtim.tv_nsec = static_cast<int64_t>(MTIME & 0xFFFFFFFF);
        st->st_ctim.tv_sec = static_cast<int64_t>(CTIME >> 32);
        st->st_ctim.tv_nsec = static_cast<int64_t>(CTIME & 0xFFFFFFFF);
    }
    clear_stat_abi_tail(st);
}
}  // namespace

auto xfs_inode_stat(XfsMountContext* mount, xfs_ino_t ino, ker::vfs::Stat* statbuf, bool metadata_locked, bool allocation_known) -> int {
    if (mount == nullptr || ino == NULLFSINO || statbuf == nullptr) {
        return -EINVAL;
    }

    xfs_icache_init();

    size_t const BUCKET = icache_hash(mount, ino);
    uint64_t flags = icache.at(BUCKET).lock.lock_irqsave();
    bool unavailable = false;
    XfsInode* ip = icache_lookup_locked(mount, ino, BUCKET, &unavailable);
    icache.at(BUCKET).lock.unlock_irqrestore(flags);
    if (ip != nullptr) {
        int const RET = (ip->nlink != 0) ? xfs_inode_fill_stat(ip, statbuf) : -ENOENT;
        if (metadata_locked) {
            xfs_inode_release_metadata_locked(ip);
        } else {
            xfs_inode_release(ip);
        }
        return RET;
    }
    if (unavailable) {
        return -ENOENT;
    }

    if (!allocation_known) {
        int const ALLOCATED = xfs_inode_allocated(mount, ino);
        if (ALLOCATED == 0) {
            return -ENOENT;
        }
        if (ALLOCATED < 0) {
            uint64_t const COUNT = alloc_lookup_failure_count.fetch_add(1, std::memory_order_relaxed) + 1;
            if ((COUNT % ALLOC_LOOKUP_WARN_INTERVAL) == 1) {
                mod::dbg::logger<"xfs">::warn(
                    "xfs_inode_stat: allocation lookup failed for inode %lu rc=%d; validating dinode directly (count=%lu)",
                    static_cast<unsigned long>(ino), ALLOCATED, COUNT);
            }
        }
    }

    xfs_fsblock_t const BLOCK = xfs_inode_block(mount, ino);
    size_t const OFFSET = xfs_inode_offset(mount, ino);

    BufHead* bh = xfs_buf_read(mount, BLOCK);
    if (bh == nullptr) {
        return -EIO;
    }

    if (OFFSET + mount->inode_size > bh->size || OFFSET + sizeof(XfsDinode) > bh->size) {
        brelse(bh);
        return -EINVAL;
    }

    const auto* dip = reinterpret_cast<const XfsDinode*>(bh->data + OFFSET);
    if (dip->di_magic.to_cpu() != XFS_DINODE_MAGIC || dip->di_version != 3) {
        brelse(bh);
        return -EINVAL;
    }

#ifdef WOS_KASAN
    if (uintptr_t const POISONED = __asan_region_is_poisoned(reinterpret_cast<uintptr_t>(dip), mount->inode_size); POISONED != 0) {
        brelse(bh);
        return -EINVAL;
    }
#endif

    uint32_t const COMPUTED = util::crc32c_block_with_cksum(dip, mount->inode_size, XFS_DINODE_CRC_OFF);
    if (COMPUTED != dip->di_crc) {
        brelse(bh);
        return -EINVAL;
    }

    if (dip->di_nlink.to_cpu() == 0) {
        brelse(bh);
        return -ENOENT;
    }

    fill_stat_from_dinode(mount, ino, dip, statbuf);
    brelse(bh);
    return 0;
}

auto xfs_inode_stat_cached(XfsMountContext* mount, xfs_ino_t ino, ker::vfs::Stat* statbuf) -> int {
    if (mount == nullptr || ino == NULLFSINO || statbuf == nullptr) {
        return -EINVAL;
    }

    xfs_icache_init();

    size_t const BUCKET = icache_hash(mount, ino);
    uint64_t flags = icache.at(BUCKET).lock.lock_irqsave();
    bool unavailable = false;
    XfsInode* ip = icache_lookup_locked(mount, ino, BUCKET, &unavailable);
    icache.at(BUCKET).lock.unlock_irqrestore(flags);
    if (ip == nullptr) {
        return unavailable ? -ENOENT : -EAGAIN;
    }

    int const RET = (ip->nlink != 0) ? xfs_inode_fill_stat(ip, statbuf) : -ENOENT;
    xfs_inode_release(ip);
    return RET;
}

auto xfs_inode_write(XfsInode* ip, XfsTransaction* tp) -> int {
    if (ip == nullptr || ip->mount == nullptr) {
        return -EINVAL;
    }

    XfsMountContext* mount = ip->mount;
    bool const LARGE_EXTENT_COUNTS = (ip->flags2 & XFS_DIFLAG2_NREXT64) != 0;
    if (LARGE_EXTENT_COUNTS && !xfs_has_nrext64(mount)) {
        return -EINVAL;
    }
    xfs_fsblock_t const BLOCK = xfs_inode_block(mount, ip->ino);
    size_t const OFFSET = xfs_inode_offset(mount, ip->ino);

    BufHead* bh = xfs_buf_read(mount, BLOCK);
    if (bh == nullptr) {
        mod::dbg::log("[xfs] inode write: ino=%lu failed to read block %lu", static_cast<unsigned long>(ip->ino),
                      static_cast<unsigned long>(BLOCK));
        return -EIO;
    }

    if (OFFSET + mount->inode_size > bh->size) {
        mod::dbg::log("[xfs] inode write: ino=%lu invalid slot offset=%lu inode_size=%lu buf_size=%lu", static_cast<unsigned long>(ip->ino),
                      static_cast<unsigned long>(OFFSET), static_cast<unsigned long>(mount->inode_size),
                      static_cast<unsigned long>(bh->size));
        brelse(bh);
        return -EIO;
    }

    auto* dip = reinterpret_cast<XfsDinode*>(bh->data + OFFSET);
    __builtin_memset(dip, 0, mount->inode_size);

    dip->di_magic = Be16::from_cpu(XFS_DINODE_MAGIC);
    dip->di_version = 3;
    dip->di_metatype = Be16::from_cpu(0);
    dip->di_aformat = static_cast<int8_t>(XFS_DINODE_FMT_EXTENTS);
    dip->di_next_unlinked = Be32::from_cpu(UINT32_MAX);
    dip->di_ino = Be64::from_cpu(ip->ino);
    __builtin_memcpy(&dip->di_uuid, &mount->uuid, sizeof(XfsUuidT));

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

    // NREXT64 is selected per inode, not merely by the superblock feature.
    if (LARGE_EXTENT_COUNTS) {
        // Data extent count => di_pad[0..7] as Be64
        Be64 big_nextents_be = Be64::from_cpu(static_cast<uint64_t>(ip->nextents));
        __builtin_memcpy(dip->di_pad.data(), &big_nextents_be, sizeof(Be64));
        // Attr extent count => di_nextents as Be32
        dip->di_nextents = Be32::from_cpu(static_cast<uint32_t>(ip->anextents));
        dip->di_anextents = Be16{0};
    } else {
        dip->di_nextents = Be32::from_cpu(ip->nextents);
        dip->di_anextents = Be16::from_cpu(ip->anextents);
    }
    dip->di_forkoff = ip->forkoff;
    dip->di_format = static_cast<uint8_t>(ip->data_fork.format);

    // Serialize data fork
    size_t const INODE_CORE_SIZE = xfs_dinode_size(dip->di_version);
    size_t const DATA_FORK_SIZE = data_fork_size_for_write(ip, INODE_CORE_SIZE);
    if (INODE_CORE_SIZE > mount->inode_size ||
        (DATA_FORK_SIZE == 0 && (ip->data_fork.format != XFS_DINODE_FMT_EXTENTS || ip->data_fork.extents.count != 0))) {
        mod::dbg::log("[xfs] inode write: ino=%lu invalid data fork core=%lu inode_size=%lu fork_size=%lu format=%u extents=%u",
                      static_cast<unsigned long>(ip->ino), static_cast<unsigned long>(INODE_CORE_SIZE),
                      static_cast<unsigned long>(mount->inode_size), static_cast<unsigned long>(DATA_FORK_SIZE),
                      static_cast<unsigned>(ip->data_fork.format), ip->data_fork.extents.count);
        brelse(bh);
        return -EFBIG;
    }
    uint8_t* fork_data = bh->data + OFFSET + INODE_CORE_SIZE;
    __builtin_memset(fork_data, 0, mount->inode_size - INODE_CORE_SIZE);

    switch (ip->data_fork.format) {
        case XFS_DINODE_FMT_LOCAL:
            if (ip->data_fork.local.size > DATA_FORK_SIZE) {
                mod::dbg::log("[xfs] inode write: ino=%lu local data fork too large size=%lu capacity=%lu",
                              static_cast<unsigned long>(ip->ino), static_cast<unsigned long>(ip->data_fork.local.size),
                              static_cast<unsigned long>(DATA_FORK_SIZE));
                brelse(bh);
                return -EFBIG;
            }
            if (ip->data_fork.local.data != nullptr && ip->data_fork.local.size > 0) {
                __builtin_memcpy(fork_data, ip->data_fork.local.data, ip->data_fork.local.size);
            }
            break;

        case XFS_DINODE_FMT_EXTENTS: {
            uint32_t const CAPACITY = extent_record_capacity(DATA_FORK_SIZE);
            if (ip->data_fork.extents.count > CAPACITY) {
                mod::dbg::log("[xfs] inode %lu: %u extents exceed inline capacity %u", static_cast<unsigned long>(ip->ino),
                              ip->data_fork.extents.count, CAPACITY);
                brelse(bh);
                return -EFBIG;
            }
            if (ip->data_fork.extents.count != 0 && ip->data_fork.extents.list == nullptr) {
                mod::dbg::log("[xfs] inode write: ino=%lu data extents missing list count=%u capacity=%u ifork_capacity=%u",
                              static_cast<unsigned long>(ip->ino), ip->data_fork.extents.count, ip->data_fork.extents.capacity,
                              XFS_IFORK_INLINE_EXTENT_CAPACITY);
                brelse(bh);
                return -EIO;
            }
            auto* recs = reinterpret_cast<XfsBmbtRec*>(fork_data);
            for (uint32_t i = 0; i < ip->data_fork.extents.count; i++) {
                recs[i] = xfs_bmbt_rec_pack(ip->data_fork.extents.list[i]);
            }
            break;
        }

        case XFS_DINODE_FMT_BTREE:
            // Copy the btree root data back
            if (ip->data_fork.btree.root_size > DATA_FORK_SIZE) {
                mod::dbg::log("[xfs] inode write: ino=%lu data btree root too large size=%lu capacity=%lu nextents=%u",
                              static_cast<unsigned long>(ip->ino), static_cast<unsigned long>(ip->data_fork.btree.root_size),
                              static_cast<unsigned long>(DATA_FORK_SIZE), ip->nextents);
                brelse(bh);
                return -EFBIG;
            }
            if (validate_bmdr_root(ip->data_fork.btree.root, ip->data_fork.btree.root_size, DATA_FORK_SIZE, ip->nextents) != 0) {
                mod::dbg::log("[xfs] inode write: ino=%lu invalid data btree root ptr=%p size=%lu capacity=%lu nextents=%u",
                              static_cast<unsigned long>(ip->ino), static_cast<void*>(ip->data_fork.btree.root),
                              static_cast<unsigned long>(ip->data_fork.btree.root_size), static_cast<unsigned long>(DATA_FORK_SIZE),
                              ip->nextents);
                brelse(bh);
                return -EIO;
            }
            __builtin_memcpy(fork_data, ip->data_fork.btree.root, ip->data_fork.btree.root_size);
            break;

        default:
            break;
    }

    // Serialize attribute fork (if present)
    if (ip->has_attr_fork && ip->forkoff > 0) {
        dip->di_aformat = static_cast<int8_t>(ip->attr_fork.format);
        size_t const ATTR_FORK_OFFSET = static_cast<size_t>(ip->forkoff) << 3;
        if (INODE_CORE_SIZE + ATTR_FORK_OFFSET > mount->inode_size) {
            mod::dbg::log("[xfs] inode write: ino=%lu invalid attr fork offset=%lu core=%lu inode_size=%lu forkoff=%u",
                          static_cast<unsigned long>(ip->ino), static_cast<unsigned long>(ATTR_FORK_OFFSET),
                          static_cast<unsigned long>(INODE_CORE_SIZE), static_cast<unsigned long>(mount->inode_size), ip->forkoff);
            brelse(bh);
            return -EFBIG;
        }
        uint8_t* attr_data = fork_data + ATTR_FORK_OFFSET;
        size_t const ATTR_FORK_SIZE = mount->inode_size - INODE_CORE_SIZE - ATTR_FORK_OFFSET;

        switch (ip->attr_fork.format) {
            case XFS_DINODE_FMT_LOCAL:
                if (ip->attr_fork.local.size > ATTR_FORK_SIZE) {
                    mod::dbg::log("[xfs] inode write: ino=%lu local attr fork too large size=%lu capacity=%lu",
                                  static_cast<unsigned long>(ip->ino), static_cast<unsigned long>(ip->attr_fork.local.size),
                                  static_cast<unsigned long>(ATTR_FORK_SIZE));
                    brelse(bh);
                    return -EFBIG;
                }
                if (ip->attr_fork.local.data != nullptr && ip->attr_fork.local.size > 0) {
                    __builtin_memcpy(attr_data, ip->attr_fork.local.data, ip->attr_fork.local.size);
                }
                break;

            case XFS_DINODE_FMT_EXTENTS: {
                uint32_t const CAPACITY = extent_record_capacity(ATTR_FORK_SIZE);
                if (ip->attr_fork.extents.count > CAPACITY) {
                    mod::dbg::log("[xfs] inode write: ino=%lu attr extents exceed inline capacity count=%u capacity=%u",
                                  static_cast<unsigned long>(ip->ino), ip->attr_fork.extents.count, CAPACITY);
                    brelse(bh);
                    return -EFBIG;
                }
                if (ip->attr_fork.extents.count != 0 && ip->attr_fork.extents.list == nullptr) {
                    mod::dbg::log("[xfs] inode write: ino=%lu attr extents missing list count=%u capacity=%u",
                                  static_cast<unsigned long>(ip->ino), ip->attr_fork.extents.count, ip->attr_fork.extents.capacity);
                    brelse(bh);
                    return -EIO;
                }
                auto* recs = reinterpret_cast<XfsBmbtRec*>(attr_data);
                for (uint32_t i = 0; i < ip->attr_fork.extents.count; i++) {
                    recs[i] = xfs_bmbt_rec_pack(ip->attr_fork.extents.list[i]);
                }
                break;
            }

            case XFS_DINODE_FMT_BTREE:
                if (ip->attr_fork.btree.root_size > ATTR_FORK_SIZE) {
                    mod::dbg::log("[xfs] inode write: ino=%lu attr btree root too large size=%lu capacity=%lu anextents=%u",
                                  static_cast<unsigned long>(ip->ino), static_cast<unsigned long>(ip->attr_fork.btree.root_size),
                                  static_cast<unsigned long>(ATTR_FORK_SIZE), ip->anextents);
                    brelse(bh);
                    return -EFBIG;
                }
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
