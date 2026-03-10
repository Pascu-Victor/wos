#pragma once

// XFS Mount Context — in-memory representation of a mounted XFS filesystem.
//
// Holds the parsed superblock geometry, per-AG state, buffer cache reference,
// and journal state.  All XFS operations take an XfsMountContext* as their
// first parameter.
//
// Reference: reference/xfs/xfs_mount.h, reference/xfs/xfs_mount.c,
//            reference/xfs/libxfs/xfs_sb.c, reference/xfs/xfs_super.c

#include <cstddef>
#include <cstdint>
#include <dev/block_device.hpp>
#include <platform/sys/spinlock.hpp>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_format.hpp>

namespace ker::vfs::xfs {

// Per-AG in-memory state (read from AGF + AGI at mount time)
struct XfsPerAG {
    xfs_agnumber_t agno;

    // From AGF
    xfs_agblock_t agf_length;    // AG size in blocks
    xfs_agblock_t agf_bno_root;  // bnobt root block
    xfs_agblock_t agf_cnt_root;  // cntbt root block
    uint32_t agf_bno_level;      // bnobt levels
    uint32_t agf_cnt_level;      // cntbt levels
    uint32_t agf_freeblks;       // total free blocks
    uint32_t agf_longest;        // longest free extent
    uint32_t agf_flcount;        // freelist block count

    // From AGI
    uint32_t agi_count;           // allocated inodes
    xfs_agblock_t agi_root;       // inobt root block
    uint32_t agi_level;           // inobt levels
    uint32_t agi_freecount;       // free inodes
    xfs_agblock_t agi_free_root;  // free inobt root (v5)
    uint32_t agi_free_level;      // free inobt levels (v5)

    mod::sys::Spinlock lock;  // Per-AG lock for concurrent access
};

// Mount context — one per mounted XFS filesystem
struct XfsMountContext {
    dev::BlockDevice* device;  // Underlying block device

    // --- Parsed superblock geometry ---
    uint32_t block_size;        // filesystem block size (bytes)
    uint8_t block_log;          // log2(block_size)
    uint64_t total_blocks;      // total data blocks (sb_dblocks)
    xfs_ino_t root_ino;         // root directory inode number
    uint16_t inode_size;        // on-disk inode size (bytes)
    uint8_t inode_log;          // log2(inode_size)
    uint16_t inodes_per_block;  // inodes per filesystem block
    uint8_t inopb_log;          // log2(inodes_per_block)

    // AG geometry
    xfs_agnumber_t ag_count;  // number of allocation groups
    xfs_agblock_t ag_blocks;  // blocks per AG (last AG may be smaller)
    uint8_t ag_blk_log;       // log2(ag_blocks), for fsblock->ag conversion

    // AG-inode geometry
    // agino_log = ag_blk_log + inopb_log (bits for AG-relative inode number)
    uint32_t agino_log;

    // Directory block size
    uint8_t dir_blk_log;    // log2 of dir block size in fs blocks
    uint32_t dir_blk_size;  // directory block size in bytes

    // Sector size
    uint16_t sect_size;  // volume sector size (bytes)
    uint8_t sect_log;    // log2(sect_size)

    // Log (journal) location
    xfs_fsblock_t log_start;  // starting block of internal log
    uint32_t log_blocks;      // number of log blocks

    // Feature flags
    uint32_t feat_incompat;
    uint32_t feat_ro_compat;

    // Per-AG state array (ag_count elements, heap-allocated)
    XfsPerAG* per_ag;

    // Filesystem UUIDs
    XfsUuidT uuid;
    XfsUuidT meta_uuid;

    // Raw copy of the on-disk superblock for CRC re-verification
    XfsDsb raw_sb;

    // Mount flags
    bool read_only;
    bool mounted;
};

// Mount an XFS filesystem from the given block device.
// Returns 0 on success, negative errno on failure.
// On success, *ctx_out is set to a heap-allocated XfsMountContext.
auto xfs_mount(dev::BlockDevice* device, bool read_only, XfsMountContext** ctx_out) -> int;

// Read one XFS filesystem block.  Converts XFS block numbers to device block
// numbers and reads the appropriate number of contiguous device blocks to
// cover a full XFS block.  Caller must call brelse() on the returned buffer.
auto xfs_buf_read(XfsMountContext* ctx, uint64_t xfs_block) -> BufHead*;

// Read `count` contiguous XFS filesystem blocks.
auto xfs_buf_read_multi(XfsMountContext* ctx, uint64_t xfs_block, size_t count) -> BufHead*;

// Unmount — flush dirty buffers, close journal, free mount context.
void xfs_unmount(XfsMountContext* ctx);

// Check if a feature is enabled
inline bool xfs_has_ftype(const XfsMountContext* ctx) { return (ctx->feat_incompat & XFS_SB_FEAT_INCOMPAT_FTYPE) != 0; }
inline bool xfs_has_finobt(const XfsMountContext* ctx) { return (ctx->feat_ro_compat & XFS_SB_FEAT_RO_COMPAT_FINOBT) != 0; }
inline bool xfs_has_nrext64(const XfsMountContext* ctx) { return (ctx->feat_incompat & XFS_SB_FEAT_INCOMPAT_NREXT64) != 0; }
inline bool xfs_has_parent(const XfsMountContext* ctx) { return (ctx->feat_incompat & XFS_SB_FEAT_INCOMPAT_PARENT) != 0; }
inline bool xfs_has_exchrange(const XfsMountContext* ctx) { return (ctx->feat_incompat & XFS_SB_FEAT_INCOMPAT_EXCHRANGE) != 0; }

}  // namespace ker::vfs::xfs
