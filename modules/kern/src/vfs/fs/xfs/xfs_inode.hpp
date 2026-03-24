#pragma once

// XFS Inode — in-memory representation and disk I/O.
//
// XfsInode is the in-memory parsed form of xfs_dinode.  It is cached in a
// hash table keyed by (mount, inode_number) with reference counting.
//
// Reference: reference/xfs/xfs_inode.h, reference/xfs/libxfs/xfs_inode_buf.c,
//            reference/xfs/libxfs/xfs_inode_fork.c

#include <cstddef>
#include <cstdint>
#include <platform/sys/spinlock.hpp>
#include <vfs/fs/xfs/xfs_format.hpp>
#include <vfs/fs/xfs/xfs_mount.hpp>

namespace ker::vfs::xfs {

// ============================================================================
// In-memory inode
// ============================================================================

// Parsed extent list (copied from on-disk fork for EXTENTS format)
struct XfsIforkExtents {
    XfsBmbtIrec* list;  // decoded extent records (heap-allocated)
    uint32_t count;     // number of extents
    uint32_t capacity;  // allocated capacity (for amortised growth)
};

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

// Data fork representation — depends on di_format
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

    // Timestamps (raw on-disk format — decode later if needed)
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
    int refcount;             // reference count
    XfsInode* hash_next;      // hash chain link
    mod::sys::Spinlock lock;  // per-inode lock

    // Dirty flag (set when in-memory changes need writeback)
    bool dirty;
};

// ============================================================================
// Inode operations
// ============================================================================

// Read an inode from disk.  Returns a reference-counted XfsInode*, or nullptr
// on error.  Uses an inode cache — repeated reads of the same inode return the
// same pointer with incremented refcount.
auto xfs_inode_read(XfsMountContext* mount, xfs_ino_t ino) -> XfsInode*;

// Release a reference to an inode.  When refcount drops to 0, the inode may
// be evicted from the cache (but is not necessarily freed immediately).
void xfs_inode_release(XfsInode* ip);

// Write back a dirty inode to disk.  Serializes the in-memory XfsInode back
// to the on-disk XfsDinode, recomputes CRC, and logs through the transaction.
// Returns 0 on success, negative errno on failure.
struct XfsTransaction;
auto xfs_inode_write(XfsInode* ip, XfsTransaction* tp) -> int;

// Initialize the inode cache (call once at mount time).
void xfs_icache_init();

// Purge all cached inodes for a given mount (call at unmount time).
void xfs_icache_purge(XfsMountContext* mount);

// Helper: compute the filesystem block containing a given inode
inline auto xfs_inode_block(const XfsMountContext* ctx, xfs_ino_t ino) -> xfs_fsblock_t {
    xfs_agnumber_t agno = xfs_ino_ag(ino, ctx->agino_log);
    xfs_agino_t agino = xfs_ag_ino(ino, ctx->agino_log);
    xfs_agblock_t agbno = agino / ctx->inodes_per_block;
    return xfs_agbno_to_fsbno(agno, agbno, ctx->ag_blk_log);
}

// Helper: compute the byte offset within the block for the inode
inline auto xfs_inode_offset(const XfsMountContext* ctx, xfs_ino_t ino) -> size_t {
    xfs_agino_t agino = xfs_ag_ino(ino, ctx->agino_log);
    return static_cast<size_t>(agino % ctx->inodes_per_block) * ctx->inode_size;
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
