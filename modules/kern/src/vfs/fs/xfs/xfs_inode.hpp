#pragma once

// XFS Inode - in-memory representation and disk I/O.
//
// XfsInode is the in-memory parsed form of xfs_dinode.  It is cached in a
// hash table keyed by (mount, inode_number) with reference counting.
//
// Reference: reference/xfs/xfs_inode.h, reference/xfs/libxfs/xfs_inode_buf.c,
//            reference/xfs/libxfs/xfs_inode_fork.c

#include <array>
#include <cstddef>
#include <cstdint>
#include <platform/sys/mutex.hpp>
#include <platform/sys/spinlock.hpp>
#include <vfs/fs/xfs/xfs_format.hpp>
#include <vfs/fs/xfs/xfs_mount.hpp>
#include <vfs/stat.hpp>

namespace ker::vfs::xfs {

// ============================================================================
// In-memory inode
// ============================================================================

constexpr uint32_t XFS_IFORK_INLINE_EXTENT_CAPACITY = 4;
constexpr size_t XFS_DIR_NAME_FILTER_WORDS = 4;

// Parsed extent list (copied from on-disk fork for EXTENTS format)
struct XfsIforkExtents {
    XfsBmbtIrec* list;  // decoded extent records (inline_list or heap)
    uint32_t count;     // number of extents
    uint32_t capacity;  // allocated capacity (for amortised growth)
    XfsBmbtIrec inline_list[XFS_IFORK_INLINE_EXTENT_CAPACITY];
};

inline auto xfs_ifork_extents_inline_data(XfsIforkExtents& extents) -> XfsBmbtIrec* { return &extents.inline_list[0]; }
inline auto xfs_ifork_extents_inline_data(const XfsIforkExtents& extents) -> const XfsBmbtIrec* { return &extents.inline_list[0]; }
inline auto xfs_ifork_extents_uses_inline(const XfsIforkExtents& extents) -> bool {
    return extents.list == xfs_ifork_extents_inline_data(extents);
}

// B+tree root in inode fork (for BTREE format)
struct XfsIforkBtree {
    uint16_t level;    // btree depth
    uint16_t numrecs;  // number of records/keys in root
    uint8_t* root;     // copy of in-fork btree root data (keys + ptrs)
    size_t root_size;  // size of the root data in bytes
};

// LOCAL: inline data in the inode
struct XfsIforkLocal {
    uint8_t* data;
    size_t size;
};

// Data fork representation - depends on di_format
struct XfsIfork {
    xfs_dinode_fmt format;
    union {
        // LOCAL: inline data in the inode
        XfsIforkLocal local;

        // EXTENTS: flat extent list
        XfsIforkExtents extents;

        // BTREE: B+tree root
        XfsIforkBtree btree;
    };
};

struct XfsInode {
    // Identity
    xfs_ino_t ino;        // absolute inode number
    xfs_agnumber_t agno;  // AG containing this inode
    xfs_agino_t agino;    // AG-relative inode number
    XfsMountContext* mount;

    // Core inode fields (parsed from xfs_dinode)
    uint16_t mode;  // POSIX file mode & type
    uint32_t uid;
    uint32_t gid;
    uint32_t nlink;
    uint64_t size;     // file size in bytes
    uint64_t nblocks;  // total blocks allocated
    uint32_t gen;      // generation number
    uint16_t flags;    // XFS_DIFLAG_*
    uint64_t flags2;   // XFS_DIFLAG2_*

    // Timestamps (raw on-disk format - decode later if needed)
    uint64_t atime;
    uint64_t mtime;
    uint64_t ctime;
    uint64_t crtime;  // creation time (v3)

    // Data fork
    XfsIfork data_fork;

    // Attribute fork (optional)
    uint8_t forkoff;  // attr fork offset in 8-byte units
    XfsIfork attr_fork;
    bool has_attr_fork;

    // Extent count
    uint32_t nextents;   // data fork extents
    uint16_t anextents;  // attr fork extents

    // Cache management
    int refcount;             // active reference count; 0 means cached idle
    XfsInode* hash_next;      // hash chain link
    mod::sys::Spinlock lock;  // per-inode lock
    mod::sys::Mutex io_lock;  // sleeping lock for data/metadata I/O mutation

    // Final zero-link teardown is in progress.  The inode remains in the
    // cache with refcount 0 while the on-disk free transaction runs so stale
    // path lookups cannot instantiate a second in-memory copy.
    bool inactivation_started;

    // Dirty flag (set when in-memory changes need writeback)
    bool dirty;

    // In-memory generation for directory entry mutations.  Per-open readdir
    // caches use this to discard bounded batches after add/remove/rename.
    uint64_t dir_generation;

    // A full fallback scan can prove that every data entry is represented in
    // the leaf index.  While this marker matches dir_generation, full leaf
    // index misses can avoid rescanning all data blocks.
    uint64_t dir_leaf_index_complete_generation;
    bool dir_leaf_index_complete;

    // New directories begin with a complete, empty name set. This bounded
    // filter can prove misses without retaining every positive dentry; hash
    // collisions only force the normal directory lookup.
    std::array<uint64_t, XFS_DIR_NAME_FILTER_WORDS> dir_name_filter;
    bool dir_name_filter_complete;
};

// Allocate an inode object with all members value-initialized. Use this for
// parsed on-disk inodes and tests that expect zero defaults.
auto xfs_inode_alloc_zeroed_object() -> XfsInode*;

// Allocate an inode object with lock members constructed but scalar/fork fields
// left for the caller to initialize explicitly. Use only on tightly controlled
// create paths that set every persisted and cache-management field.
auto xfs_inode_alloc_uninitialized_object() -> XfsInode*;

// Free an inode object that is not linked in the inode cache, including any
// heap-owned fork storage.
void xfs_inode_free_uncached(XfsInode* ip);

// ============================================================================
// Inode operations
// ============================================================================

// Read an inode from disk.  Returns a reference-counted XfsInode*, or nullptr
// on error.  Uses an inode cache - repeated reads of the same inode return the
// same pointer with incremented refcount.
auto xfs_inode_read(XfsMountContext* mount, xfs_ino_t ino) -> XfsInode*;

// Read an inode when a current namespace entry already proved the inode number
// is allocated.  This skips the allocation-btree probe on cache misses while
// still rejecting zero-link disk inodes before caching them.
auto xfs_inode_read_known_allocated(XfsMountContext* mount, xfs_ino_t ino) -> XfsInode*;

// Return an active reference only if the inode is already in the in-memory
// cache.  This never reads disk and is suitable for cache-hit fast paths that
// must avoid mount-wide metadata lock contention.
auto xfs_inode_read_cached(XfsMountContext* mount, xfs_ino_t ino) -> XfsInode*;

// Fill a kernel stat structure from an already referenced in-memory inode.
auto xfs_inode_fill_stat(const XfsInode* ip, ker::vfs::Stat* statbuf) -> int;

// Fill stat for an inode number. Cached dirty/active inodes are honored first;
// otherwise only the on-disk dinode core is read, avoiding full fork parsing.
// Pass allocation_known only when a current namespace entry already proved the
// inode number; free dinodes are still rejected by the nlink check.
auto xfs_inode_stat(XfsMountContext* mount, xfs_ino_t ino, ker::vfs::Stat* statbuf, bool metadata_locked = false,
                    bool allocation_known = false) -> int;

// Fill stat from the in-memory inode cache only. Returns -EAGAIN when the inode
// is not cached so callers can fall back to the normal metadata-locked path.
auto xfs_inode_stat_cached(XfsMountContext* mount, xfs_ino_t ino, ker::vfs::Stat* statbuf) -> int;

// Return a new active reference to the mounted root inode.  Uses the pinned
// mount reference when present, falling back to a normal inode read during
// early setup paths.
auto xfs_root_inode_read(XfsMountContext* mount) -> XfsInode*;

// Insert a newly-created inode into the cache with one active reference.
// The inode must already be committed to disk and must not already have a cache
// entry for (mount, ino).  On success, ownership transfers to the inode cache
// and the caller owns the active reference.
auto xfs_inode_cache_new(XfsInode* ip) -> int;

// Release a reference to an inode. When refcount drops to 0, linked inodes
// remain cached for reuse; unlinked inodes are physically freed at that final
// inactivation point.
void xfs_inode_release(XfsInode* ip);

// Release a reference while the caller already holds mount->metadata_lock.
// This avoids recursively taking the mount-wide metadata mutex when dropping
// the final reference to a zero-link inode from unlink/rmdir/rename paths.
void xfs_inode_release_metadata_locked(XfsInode* ip);

// Write back a dirty inode to disk.  Serializes the in-memory XfsInode back
// to the on-disk XfsDinode, recomputes CRC, and logs through the transaction.
// Returns 0 on success, negative errno on failure.
struct XfsTransaction;
auto xfs_inode_write(XfsInode* ip, XfsTransaction* tp) -> int;

// Free all data extents for a regular file and reset the in-memory data fork.
// Caller must hold the inode's I/O lock and commit the supplied transaction.
auto xfs_inode_truncate_data(XfsInode* ip, XfsTransaction* tp) -> int;

// Free whole blocks past new_size for inline extent-format regular files.
// Caller must hold the inode's I/O lock and log/commit the supplied transaction.
auto xfs_inode_trim_data_to_size(XfsInode* ip, XfsTransaction* tp, uint64_t new_size) -> int;

// Initialize the inode cache (call once at mount time).
void xfs_icache_init();

// Purge all cached inodes for a given mount (call at unmount time).
void xfs_icache_purge(XfsMountContext* mount);

// Commit dirty cached inodes for a mount.  Used by mount-level sync before the
// block cache is flushed.
auto xfs_icache_sync_dirty(XfsMountContext* mount) -> int;

// Helper: compute the filesystem block containing a given inode
inline auto xfs_inode_block(const XfsMountContext* ctx, xfs_ino_t ino) -> xfs_fsblock_t {
    xfs_agnumber_t const AGNO = xfs_ino_ag(ino, ctx->agino_log);
    xfs_agino_t const AGINO = xfs_ag_ino(ino, ctx->agino_log);
    xfs_agblock_t const AGBNO = AGINO / ctx->inodes_per_block;
    return xfs_agbno_to_fsbno(AGNO, AGBNO, ctx->ag_blk_log);
}

// Helper: compute the byte offset within the block for the inode
inline auto xfs_inode_offset(const XfsMountContext* ctx, xfs_ino_t ino) -> size_t {
    xfs_agino_t const AGINO = xfs_ag_ino(ino, ctx->agino_log);
    return static_cast<size_t>(AGINO % ctx->inodes_per_block) * ctx->inode_size;
}

// Check if inode is a directory
inline auto xfs_inode_isdir(const XfsInode* ip) -> bool {
    return (ip->mode & 0170000) == 0040000;  // S_IFDIR
}

// Check if inode is a regular file
inline auto xfs_inode_isreg(const XfsInode* ip) -> bool {
    return (ip->mode & 0170000) == 0100000;  // S_IFREG
}

// Check if inode is a symlink
inline auto xfs_inode_islnk(const XfsInode* ip) -> bool {
    return (ip->mode & 0170000) == 0120000;  // S_IFLNK
}

}  // namespace ker::vfs::xfs
