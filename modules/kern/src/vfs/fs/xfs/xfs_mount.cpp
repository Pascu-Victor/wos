// XFS Mount / Unmount implementation.
//
// xfs_mount():
//   1. Read superblock at sector 0.
//   2. Validate magic ('XFSB'), verify CRC32C, check version == 5.
//   3. Parse geometry fields into XfsMountContext.
//   4. Read AGF/AGI for each allocation group into per-AG state.
//
// Reference: reference/xfs/xfs_mount.c, reference/xfs/libxfs/xfs_sb.c

#include "xfs_mount.hpp"

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <util/crc32c.hpp>
#include <vfs/buffer_cache.hpp>

#include "dev/block_device.hpp"
#include "vfs/fs/xfs/xfs_format.hpp"

namespace ker::vfs::xfs {

namespace {

// Verify the superblock CRC.  The CRC field itself is stored in little-endian
// (unusual for XFS), and the check must treat it as zero during computation.
auto verify_sb_crc(const XfsDsb* sb, size_t len) -> bool {
    uint32_t const COMPUTED = util::crc32c_block_with_cksum(sb, len, XFS_SB_CRC_OFF);
    return COMPUTED == sb->sb_crc;  // sb_crc is already little-endian
}

// Verify an AG header CRC (AGF or AGI).
auto verify_ag_crc(const void* buf, size_t len, size_t crc_off) -> bool {
    const auto* bytes = static_cast<const uint8_t*>(buf);
    uint32_t on_disk_crc = 0;
    __builtin_memcpy(&on_disk_crc, bytes + crc_off, sizeof(uint32_t));
    uint32_t const COMPUTED = util::crc32c_block_with_cksum(buf, len, crc_off);
    return COMPUTED == on_disk_crc;
}

}  // namespace

// Read one XFS filesystem block.  The block device may have a smaller sector
// size (typically 512), so we convert the XFS block number to device blocks
// and use bread_multi to get a contiguous buffer covering the full XFS block.
//
// xfs_block is an ENCODED xfs_fsblock_t: (agno << ag_blk_log) | agbno.
// We decode it to a linear block number before multiplying by the ratio.
auto xfs_buf_read(XfsMountContext* ctx, uint64_t xfs_block) -> BufHead* {
    // Decode encoded FSB -> linear block
    auto agno = static_cast<xfs_agnumber_t>(xfs_block >> ctx->ag_blk_log);
    auto agbno = static_cast<xfs_agblock_t>(xfs_block & ((1ULL << ctx->ag_blk_log) - 1));
    uint64_t const LINEAR_BLOCK = (static_cast<uint64_t>(agno) * ctx->ag_blocks) + agbno;

    size_t const DEV_BLK_SIZE = ctx->device->block_size;
    size_t const RATIO = ctx->block_size / DEV_BLK_SIZE;  // e.g. 4096/512 = 8
    uint64_t const DEV_BLOCK = LINEAR_BLOCK * RATIO;
    if (RATIO <= 1) {
        return bread(ctx->device, DEV_BLOCK);
    }
    return bread_multi(ctx->device, DEV_BLOCK, RATIO);
}

// Read multiple contiguous XFS filesystem blocks.
auto xfs_buf_read_multi(XfsMountContext* ctx, uint64_t xfs_block, size_t count) -> BufHead* {
    // Decode encoded FSB -> linear block
    auto agno = static_cast<xfs_agnumber_t>(xfs_block >> ctx->ag_blk_log);
    auto agbno = static_cast<xfs_agblock_t>(xfs_block & ((1ULL << ctx->ag_blk_log) - 1));
    uint64_t const LINEAR_BLOCK = (static_cast<uint64_t>(agno) * ctx->ag_blocks) + agbno;

    size_t const DEV_BLK_SIZE = ctx->device->block_size;
    size_t const RATIO = ctx->block_size / DEV_BLK_SIZE;
    uint64_t const DEV_BLOCK = LINEAR_BLOCK * RATIO;
    size_t const DEV_COUNT = count * RATIO;
    if (DEV_COUNT <= 1) {
        return bread(ctx->device, DEV_BLOCK);
    }
    return bread_multi(ctx->device, DEV_BLOCK, DEV_COUNT);
}

auto xfs_buf_get(XfsMountContext* ctx, uint64_t xfs_block) -> BufHead* {
    auto agno = static_cast<xfs_agnumber_t>(xfs_block >> ctx->ag_blk_log);
    auto agbno = static_cast<xfs_agblock_t>(xfs_block & ((1ULL << ctx->ag_blk_log) - 1));
    uint64_t const LINEAR_BLOCK = (static_cast<uint64_t>(agno) * ctx->ag_blocks) + agbno;

    size_t const DEV_BLK_SIZE = ctx->device->block_size;
    size_t const RATIO = ctx->block_size / DEV_BLK_SIZE;
    uint64_t const DEV_BLOCK = LINEAR_BLOCK * RATIO;
    if (RATIO <= 1) {
        return bget(ctx->device, DEV_BLOCK);
    }
    return bget_multi(ctx->device, DEV_BLOCK, RATIO);
}

namespace {

// Read the AGF for a given AG and populate per_ag fields.
auto read_agf(XfsMountContext* ctx, xfs_agnumber_t agno) -> int {
    // AGF lives at sector 1 of the AG (second sector).
    // All four AG headers (SB copy, AGF, AGI, AGFL) fit within the first
    // XFS filesystem block of each AG.  Read that full block and index in.
    uint64_t const AG_START_BLOCK = xfs_agbno_to_fsbno(agno, 0, ctx->ag_blk_log);
    BufHead* bh = xfs_buf_read(ctx, AG_START_BLOCK);
    if (bh == nullptr) {
        mod::dbg::log("[xfs] failed to read AG %u block 0 for AGF", agno);
        return -EIO;
    }

    // AGF is at sector 1 within the first block
    size_t const AGF_OFFSET = ctx->sect_size;  // sector 1
    if (AGF_OFFSET + sizeof(XfsAgf) > bh->size) {
        brelse(bh);
        mod::dbg::log("[xfs] AG %u: AGF offset %lu exceeds block size %lu", agno, static_cast<unsigned long>(AGF_OFFSET),
                      static_cast<unsigned long>(bh->size));
        return -EINVAL;
    }

    const auto* agf = reinterpret_cast<const XfsAgf*>(bh->data + AGF_OFFSET);

    // Validate magic
    if (agf->agf_magicnum.to_cpu() != XFS_AGF_MAGIC) {
        brelse(bh);
        mod::dbg::log("[xfs] AG %u: bad AGF magic 0x%x", agno, agf->agf_magicnum.to_cpu());
        return -EINVAL;
    }

    // Verify CRC - covers the full sector, not just the struct
    if (!verify_ag_crc(bh->data + AGF_OFFSET, ctx->sect_size, XFS_AGF_CRC_OFF)) {
        brelse(bh);
        mod::dbg::log("[xfs] AG %u: AGF CRC mismatch", agno);
        return -EINVAL;
    }

    XfsPerAG* pag = &ctx->per_ag[agno];
    pag->agno = agno;
    pag->agf_length = agf->agf_length.to_cpu();
    pag->agf_bno_root = agf->agf_bno_root.to_cpu();
    pag->agf_cnt_root = agf->agf_cnt_root.to_cpu();
    pag->agf_bno_level = agf->agf_bno_level.to_cpu();
    pag->agf_cnt_level = agf->agf_cnt_level.to_cpu();
    pag->agf_freeblks = agf->agf_freeblks.to_cpu();
    pag->agf_longest = agf->agf_longest.to_cpu();
    pag->agf_flcount = agf->agf_flcount.to_cpu();
    pag->agf_flfirst = agf->agf_flfirst.to_cpu();
    pag->agf_fllast = agf->agf_fllast.to_cpu();

    mod::dbg::log("[xfs] AG %u read_agf: flfirst=%u fllast=%u flcount=%u bno_root=%u cnt_root=%u", agno, pag->agf_flfirst, pag->agf_fllast,
                  pag->agf_flcount, pag->agf_bno_root, pag->agf_cnt_root);

    brelse(bh);
    return 0;
}

// Read the AGI for a given AG and populate per_ag fields.
auto read_agi(XfsMountContext* ctx, xfs_agnumber_t agno) -> int {
    uint64_t const AG_START_BLOCK = xfs_agbno_to_fsbno(agno, 0, ctx->ag_blk_log);
    BufHead* bh = xfs_buf_read(ctx, AG_START_BLOCK);
    if (bh == nullptr) {
        mod::dbg::log("[xfs] failed to read AG %u block 0 for AGI", agno);
        return -EIO;
    }

    // AGI is at sector 2 within the first block
    auto agi_offset = static_cast<size_t>(ctx->sect_size * 2);
    if (agi_offset + sizeof(XfsAgi) > bh->size) {
        brelse(bh);
        mod::dbg::log("[xfs] AG %u: AGI offset %lu exceeds block size %lu", agno, static_cast<unsigned long>(agi_offset),
                      static_cast<unsigned long>(bh->size));
        return -EINVAL;
    }

    const auto* agi = reinterpret_cast<const XfsAgi*>(bh->data + agi_offset);

    // Validate magic
    if (agi->agi_magicnum.to_cpu() != XFS_AGI_MAGIC) {
        brelse(bh);
        mod::dbg::log("[xfs] AG %u: bad AGI magic 0x%x", agno, agi->agi_magicnum.to_cpu());
        return -EINVAL;
    }

    // Verify CRC - covers the full sector, not just the struct
    if (!verify_ag_crc(bh->data + agi_offset, ctx->sect_size, XFS_AGI_CRC_OFF)) {
        brelse(bh);
        mod::dbg::log("[xfs] AG %u: AGI CRC mismatch", agno);
        return -EINVAL;
    }

    XfsPerAG* pag = &ctx->per_ag[agno];
    pag->agi_count = agi->agi_count.to_cpu();
    pag->agi_root = agi->agi_root.to_cpu();
    pag->agi_level = agi->agi_level.to_cpu();
    pag->agi_freecount = agi->agi_freecount.to_cpu();
    pag->agi_free_root = agi->agi_free_root.to_cpu();
    pag->agi_free_level = agi->agi_free_level.to_cpu();

    brelse(bh);
    return 0;
}

}  // anonymous namespace

auto xfs_mount(dev::BlockDevice* device, bool read_only, XfsMountContext** ctx_out) -> int {
    if (device == nullptr || ctx_out == nullptr) {
        return -EINVAL;
    }

    // Ensure buffer cache is initialised
    buffer_cache_init();

    // Read block 0 which contains the primary superblock
    BufHead* bh = bread(device, 0);
    if (bh == nullptr) {
        mod::dbg::log("[xfs] failed to read superblock (block 0)");
        return -EIO;
    }

    const auto* dsb = reinterpret_cast<const XfsDsb*>(bh->data);

    // --- Validate superblock ---

    // Magic
    if (dsb->sb_magicnum.to_cpu() != XFS_SB_MAGIC) {
        mod::dbg::log("[xfs] bad superblock magic: 0x%x (expected 0x%x)", dsb->sb_magicnum.to_cpu(), XFS_SB_MAGIC);
        brelse(bh);
        return -EINVAL;
    }

    // Version: we only support v5
    uint16_t const VERSION = dsb->sb_versionnum.to_cpu();
    if ((VERSION & 0xF) != XFS_SB_VERSION_5) {
        mod::dbg::log("[xfs] unsupported version %u (only v5 supported)", VERSION);
        brelse(bh);
        return -EINVAL;
    }

    // CRC verification
    // The on-disk superblock CRC covers the entire sector (typically 512 bytes),
    // not just the defined structure fields.  Use the buffer size from bread()
    // which matches the device sector size.
    if (!verify_sb_crc(dsb, bh->size)) {
        mod::dbg::log("[xfs] superblock CRC mismatch");
        brelse(bh);
        return -EINVAL;
    }

    // --- Parse geometry ---

    auto* ctx = new XfsMountContext{};
    ctx->device = device;
    ctx->read_only = read_only;

    __builtin_memcpy(&ctx->raw_sb, dsb, sizeof(XfsDsb));

    ctx->block_size = dsb->sb_blocksize.to_cpu();
    ctx->block_log = dsb->sb_blocklog;
    ctx->total_blocks = dsb->sb_dblocks.to_cpu();
    ctx->root_ino = dsb->sb_rootino.to_cpu();
    ctx->inode_size = dsb->sb_inodesize.to_cpu();
    ctx->inode_log = dsb->sb_inodelog;
    ctx->inodes_per_block = dsb->sb_inopblock.to_cpu();
    ctx->inopb_log = dsb->sb_inopblog;

    ctx->ag_count = dsb->sb_agcount.to_cpu();
    ctx->ag_blocks = dsb->sb_agblocks.to_cpu();
    ctx->ag_blk_log = dsb->sb_agblklog;

    // agino_log: the number of bits for an AG-relative inode number
    // This is ag_blk_log + inopb_log (blocks in AG × inodes per block)
    ctx->agino_log = ctx->ag_blk_log + ctx->inopb_log;

    ctx->dir_blk_log = dsb->sb_dirblklog;
    ctx->dir_blk_size = ctx->block_size << ctx->dir_blk_log;

    ctx->sect_size = dsb->sb_sectsize.to_cpu();
    ctx->sect_log = dsb->sb_sectlog;

    ctx->log_start = dsb->sb_logstart.to_cpu();
    ctx->log_blocks = dsb->sb_logblocks.to_cpu();

    ctx->feat_incompat = dsb->sb_features_incompat.to_cpu();
    ctx->feat_ro_compat = dsb->sb_features_ro_compat.to_cpu();

    __builtin_memcpy(&ctx->uuid, &dsb->sb_uuid, sizeof(XfsUuidT));
    __builtin_memcpy(&ctx->meta_uuid, &dsb->sb_meta_uuid, sizeof(XfsUuidT));

    brelse(bh);

    mod::dbg::log("[xfs] superblock: blocksize=%u agcount=%u agblocks=%u rootino=%lu", ctx->block_size, ctx->ag_count, ctx->ag_blocks,
                  static_cast<unsigned long>(ctx->root_ino));
    mod::dbg::log("[xfs]   inodesize=%u sect=%u log_start=%lu log_blocks=%u", ctx->inode_size, ctx->sect_size,
                  static_cast<unsigned long>(ctx->log_start), ctx->log_blocks);

    // --- Sanity checks ---
    if (ctx->block_size < 512 || ctx->block_size > 65536) {
        mod::dbg::log("[xfs] invalid block size %u", ctx->block_size);
        delete ctx;
        return -EINVAL;
    }
    if (ctx->ag_count == 0 || ctx->ag_blocks == 0) {
        mod::dbg::log("[xfs] invalid AG geometry");
        delete ctx;
        return -EINVAL;
    }
    if (ctx->inode_size < 256) {
        mod::dbg::log("[xfs] inode size %u too small", ctx->inode_size);
        delete ctx;
        return -EINVAL;
    }

    // Check for unsupported incompat features (reflink, rmap, etc.)
    constexpr uint32_t SUPPORTED_INCOMPAT = XFS_SB_FEAT_INCOMPAT_FTYPE | XFS_SB_FEAT_INCOMPAT_SPINODES | XFS_SB_FEAT_INCOMPAT_BIGTIME |
                                            XFS_SB_FEAT_INCOMPAT_NREXT64 | XFS_SB_FEAT_INCOMPAT_EXCHRANGE | XFS_SB_FEAT_INCOMPAT_PARENT;
    uint32_t const UNSUPPORTED = ctx->feat_incompat & ~SUPPORTED_INCOMPAT;
    if (UNSUPPORTED != 0) {
        mod::dbg::log("[xfs] unsupported incompat features: 0x%x", UNSUPPORTED);
        if (!read_only) {
            delete ctx;
            return -EINVAL;
        }
        mod::dbg::log("[xfs]   mounting read-only anyway");
    }

    // --- Allocate per-AG state and read AGF/AGI headers ---
    ctx->per_ag = new (std::nothrow) XfsPerAG[ctx->ag_count]{};
    if (ctx->per_ag == nullptr) {
        mod::dbg::log("[xfs] OOM allocating per-AG state (%u AGs)", ctx->ag_count);
        delete ctx;
        return -ENOMEM;
    }

    for (xfs_agnumber_t ag = 0; ag < ctx->ag_count; ag++) {
        int rc = read_agf(ctx, ag);
        if (rc != 0) {
            mod::dbg::log("[xfs] failed to read AGF for AG %u", ag);
            delete[] ctx->per_ag;
            delete ctx;
            return rc;
        }

        rc = read_agi(ctx, ag);
        if (rc != 0) {
            mod::dbg::log("[xfs] failed to read AGI for AG %u", ag);
            delete[] ctx->per_ag;
            delete ctx;
            return rc;
        }
    }

    ctx->mounted = true;
    *ctx_out = ctx;

    // Count total free blocks and inodes across all AGs for a summary log
    uint64_t total_free = 0;
    uint64_t total_inodes = 0;
    uint64_t free_inodes = 0;
    for (xfs_agnumber_t ag = 0; ag < ctx->ag_count; ag++) {
        total_free += ctx->per_ag[ag].agf_freeblks;
        total_inodes += ctx->per_ag[ag].agi_count;
        free_inodes += ctx->per_ag[ag].agi_freecount;
    }
    mod::dbg::log("[xfs] mounted: %lu free blocks, %lu/%lu inodes free", static_cast<unsigned long>(total_free),
                  static_cast<unsigned long>(free_inodes), static_cast<unsigned long>(total_inodes));

    return 0;
}

void xfs_unmount(XfsMountContext* ctx) {
    if (ctx == nullptr) {
        return;
    }

    if (!ctx->read_only) {
        // Flush all dirty buffers
        sync_blockdev(ctx->device);
    }

    // Invalidate cached buffers
    invalidate_bdev(ctx->device);

    // Free per-AG state
    if (ctx->per_ag != nullptr) {
        delete[] ctx->per_ag;
        ctx->per_ag = nullptr;
    }

    ctx->mounted = false;
    mod::dbg::log("[xfs] unmounted");

    delete ctx;
}

}  // namespace ker::vfs::xfs
