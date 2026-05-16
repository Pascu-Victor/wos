// XFS Free Space Allocation implementation.
//
// Allocates blocks from AG free space B+trees (bnobt/cntbt).  The allocation
// strategy is:
// 1. If a preferred AG is given, try that AG first.
// 2. Search the cntbt (by-count tree) for the smallest free extent >= minlen.
// 3. If alignment is required, round up the starting block.
// 4. Split the free extent: allocate the requested portion, leave the rest.
// 5. On free, coalesce immediately adjacent extents before re-inserting.
//
// Reference: reference/xfs/libxfs/xfs_alloc.c

#include "xfs_alloc.hpp"

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <platform/dbg/dbg.hpp>
#include <util/crc32c.hpp>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_btree.hpp>

#include "net/endian.hpp"
#include "vfs/fs/xfs/xfs_format.hpp"
#include "vfs/fs/xfs/xfs_mount.hpp"
#include "vfs/fs/xfs/xfs_trans.hpp"

namespace ker::vfs::xfs {

namespace {

// Minimum number of blocks the AGFL should hold before we perform allocations
// that may trigger btree splits.  Each split consumes at most one AGFL block,
// and a two-level tree can have at most two simultaneous splits (leaf + root),
// so 4 is a safe minimum.
constexpr uint32_t XFS_AGFL_MIN = 4;

// Top up the AGFL for the given AG to at least XFS_AGFL_MIN blocks.
// Called at the start of alloc_ag_by_size when the AGFL is running low,
// BEFORE any cursor is opened on the free space trees.  This is the only
// safe point to do this - once a cursor is open and a split is in progress,
// calling xfs_alloc_extent would re-enter and corrupt the tree.
void agfl_refill(XfsMountContext* mount, XfsTransaction* tp, xfs_agnumber_t agno) {
    XfsPerAG* pag = &mount->per_ag[agno];

    while (pag->agf_flcount < XFS_AGFL_MIN && pag->agf_freeblks > 0) {
        // Find and delete the smallest available extent (size >= 1)
        XfsBtreeCursor<XfsCntbtTraits> cur;
        cur.mount = mount;
        cur.agno = agno;
        XfsCntbtTraits::IRec const TARGET{.blockcount = 1, .startblock = 0};
        if (xfs_btree_lookup(&cur, pag->agf_cnt_root, pag->agf_cnt_level, TARGET, XfsBtreeLookup::GE) != 0) {
            break;
        }
        XfsCntbtTraits::IRec const FOUND = xfs_btree_get_rec(&cur);
        if (FOUND.blockcount == 0) {
            break;
        }

        // Delete from cntbt
        uint64_t new_cnt_root = pag->agf_cnt_root;
        uint8_t new_cnt_lvl = pag->agf_cnt_level;
        if (xfs_btree_delete(&cur, tp) != 0) {
            break;
        }
        // Note: btree_delete may shrink the root; pick up updated root after
        // the delete via the cursor's updated nlevels/levels - but the pag
        // fields are updated when we write the AGF below.

        // Delete from bnobt
        XfsBtreeCursor<XfsBnobtTraits> bno_cur;
        bno_cur.mount = mount;
        bno_cur.agno = agno;
        XfsBnobtTraits::IRec const BNO_TARGET{.startblock = FOUND.startblock, .blockcount = 0};
        if (xfs_btree_lookup(&bno_cur, pag->agf_bno_root, pag->agf_bno_level, BNO_TARGET, XfsBtreeLookup::GE) != 0) {
            break;
        }
        if (xfs_btree_delete(&bno_cur, tp) != 0) {
            break;
        }

        xfs_extlen_t const EXT_LEN = FOUND.blockcount;
        // Steal the LAST block of the extent so the remainder keeps its original
        // startblock - this preserves large contiguous free runs for data allocation.
        xfs_agblock_t const STOLEN = FOUND.startblock + EXT_LEN - 1;

        // Re-insert the remainder (all but the stolen last block)
        if (EXT_LEN > 1) {
            xfs_agblock_t const REM_START = FOUND.startblock;
            xfs_extlen_t const REM_LEN = EXT_LEN - 1;
            uint64_t new_bno_root = pag->agf_bno_root;
            uint8_t new_bno_lvl = pag->agf_bno_level;

            XfsBnobtTraits::IRec const REM_BNO{.startblock = REM_START, .blockcount = REM_LEN};
            if (xfs_btree_insert(&bno_cur, tp, REM_BNO, pag->agf_bno_root, pag->agf_bno_level, &new_bno_root, &new_bno_lvl) != 0) {
                break;
            }
            pag->agf_bno_root = static_cast<xfs_agblock_t>(new_bno_root);
            pag->agf_bno_level = new_bno_lvl;

            XfsCntbtTraits::IRec const REM_CNT{.startblock = REM_START, .blockcount = REM_LEN};
            if (xfs_btree_insert(&cur, tp, REM_CNT, pag->agf_cnt_root, pag->agf_cnt_level, &new_cnt_root, &new_cnt_lvl) != 0) {
                break;
            }
            pag->agf_cnt_root = static_cast<xfs_agblock_t>(new_cnt_root);
            pag->agf_cnt_level = new_cnt_lvl;
        }

        pag->agf_freeblks--;

        // Push the stolen block to the AGFL
        xfs_alloc_put_freelist(mount, tp, agno, STOLEN);

        // Write updated AGF (roots, levels, freeblks) to disk
        uint64_t const AG0 = xfs_agbno_to_fsbno(agno, 0, mount->ag_blk_log);
        BufHead* agf_bh = xfs_buf_read(mount, AG0);
        if (agf_bh != nullptr) {
            size_t const AGF_OFF = mount->sect_size;
            auto* agf = reinterpret_cast<XfsAgf*>(agf_bh->data + AGF_OFF);
            agf->agf_freeblks = Be32::from_cpu(pag->agf_freeblks);
            agf->agf_bno_root = Be32::from_cpu(pag->agf_bno_root);
            agf->agf_cnt_root = Be32::from_cpu(pag->agf_cnt_root);
            agf->agf_bno_level = Be32::from_cpu(pag->agf_bno_level);
            agf->agf_cnt_level = Be32::from_cpu(pag->agf_cnt_level);
            agf->agf_crc = Be32{0};
            uint32_t crc = util::crc32c_block_with_cksum(agf, sizeof(XfsAgf), XFS_AGF_CRC_OFF);
            __builtin_memcpy(&agf->agf_crc, &crc, sizeof(crc));
            xfs_trans_log_buf(tp, agf_bh, static_cast<uint32_t>(AGF_OFF), static_cast<uint32_t>(sizeof(XfsAgf)));
            brelse(agf_bh);
        }
    }
}

// Try to allocate from a specific AG using the cntbt (by-count tree)
auto alloc_ag_by_size(XfsMountContext* mount, XfsTransaction* tp, xfs_agnumber_t agno, const XfsAllocReq& req, XfsAllocResult* result)
    -> int {
    XfsPerAG* pag = &mount->per_ag[agno];

    // Quick check: does this AG have enough free blocks?
    if (pag->agf_freeblks < req.minlen) {
        return -ENOSPC;
    }

    // Ensure the AGFL has enough blocks to cover any btree splits that may
    // occur during this allocation, without re-entering xfs_alloc_extent.
    if (pag->agf_flcount < XFS_AGFL_MIN) {
        agfl_refill(mount, tp, agno);
    }

    // Look up the cntbt for the smallest extent >= minlen
    XfsBtreeCursor<XfsCntbtTraits> cur;
    cur.mount = mount;
    cur.agno = agno;

    XfsCntbtTraits::IRec target{};
    target.blockcount = req.minlen;
    target.startblock = 0;

    int rc = xfs_btree_lookup(&cur, pag->agf_cnt_root, pag->agf_cnt_level, target, XfsBtreeLookup::GE);
    if (rc != 0) {
        return -ENOSPC;
    }

    XfsCntbtTraits::IRec const FOUND = xfs_btree_get_rec(&cur);

    if (FOUND.blockcount < req.minlen) {
        return -ENOSPC;
    }

    // Compute actual allocation length
    xfs_extlen_t alloc_len = FOUND.blockcount;
    alloc_len = std::min(alloc_len, req.maxlen);

    xfs_agblock_t alloc_start = FOUND.startblock;

    // Apply alignment if requested
    if (req.alignment > 1) {
        xfs_agblock_t const ALIGNED = (alloc_start + req.alignment - 1) & ~(static_cast<xfs_agblock_t>(req.alignment) - 1);
        xfs_extlen_t const LOST = ALIGNED - alloc_start;
        if (LOST >= FOUND.blockcount) {
            return -ENOSPC;
        }
        alloc_start = ALIGNED;
        xfs_extlen_t const REMAINING = FOUND.blockcount - LOST;
        if (REMAINING < req.minlen) {
            return -ENOSPC;
        }
        alloc_len = std::min(alloc_len, REMAINING);
    }

    // ---- 1. Remove the old free extent from both cntbt and bnobt ----

    // Delete from cntbt (cursor already positioned)
    int delrc = xfs_btree_delete(&cur, tp);
    if (delrc != 0) {
        return delrc;
    }

    // Locate and delete from bnobt
    XfsBtreeCursor<XfsBnobtTraits> bno_cur;
    bno_cur.mount = mount;
    bno_cur.agno = agno;

    XfsBnobtTraits::IRec bno_target{};
    bno_target.startblock = FOUND.startblock;
    bno_target.blockcount = 0;

    rc = xfs_btree_lookup(&bno_cur, pag->agf_bno_root, pag->agf_bno_level, bno_target, XfsBtreeLookup::GE);
    if (rc != 0) {
        return -EIO;  // Tree inconsistency
    }
    delrc = xfs_btree_delete(&bno_cur, tp);
    if (delrc != 0) {
        return delrc;
    }

    // ---- 2 & 3. Re-insert remainder extents ----

    // Left remainder: blocks before alloc_start
    if (alloc_start > FOUND.startblock) {
        xfs_extlen_t const LEFT_LEN = alloc_start - FOUND.startblock;
        uint64_t new_bno_root = pag->agf_bno_root;
        uint8_t new_bno_lvl = pag->agf_bno_level;
        uint64_t new_cnt_root = pag->agf_cnt_root;
        uint8_t new_cnt_lvl = pag->agf_cnt_level;

        XfsBnobtTraits::IRec const LEFT_BNO{.startblock = FOUND.startblock, .blockcount = LEFT_LEN};
        rc = xfs_btree_insert(&bno_cur, tp, LEFT_BNO, pag->agf_bno_root, pag->agf_bno_level, &new_bno_root, &new_bno_lvl);
        if (rc != 0) {
            return rc;
        }
        pag->agf_bno_root = static_cast<xfs_agblock_t>(new_bno_root);
        pag->agf_bno_level = new_bno_lvl;

        XfsCntbtTraits::IRec const LEFT_CNT{.startblock = FOUND.startblock, .blockcount = LEFT_LEN};
        rc = xfs_btree_insert(&cur, tp, LEFT_CNT, pag->agf_cnt_root, pag->agf_cnt_level, &new_cnt_root, &new_cnt_lvl);
        if (rc != 0) {
            return rc;
        }
        pag->agf_cnt_root = static_cast<xfs_agblock_t>(new_cnt_root);
        pag->agf_cnt_level = new_cnt_lvl;
    }

    // Right remainder: blocks after alloc_start+alloc_len
    xfs_agblock_t const ALLOC_END = alloc_start + alloc_len;
    xfs_agblock_t const ORIG_END = FOUND.startblock + FOUND.blockcount;
    if (ALLOC_END < ORIG_END) {
        xfs_extlen_t const RIGHT_LEN = ORIG_END - ALLOC_END;
        uint64_t new_bno_root = pag->agf_bno_root;
        uint8_t new_bno_lvl = pag->agf_bno_level;
        uint64_t new_cnt_root = pag->agf_cnt_root;
        uint8_t new_cnt_lvl = pag->agf_cnt_level;

        XfsBnobtTraits::IRec const RIGHT_BNO{.startblock = ALLOC_END, .blockcount = RIGHT_LEN};
        rc = xfs_btree_insert(&bno_cur, tp, RIGHT_BNO, pag->agf_bno_root, pag->agf_bno_level, &new_bno_root, &new_bno_lvl);
        if (rc != 0) {
            return rc;
        }
        pag->agf_bno_root = static_cast<xfs_agblock_t>(new_bno_root);
        pag->agf_bno_level = new_bno_lvl;

        XfsCntbtTraits::IRec const RIGHT_CNT{.startblock = ALLOC_END, .blockcount = RIGHT_LEN};
        rc = xfs_btree_insert(&cur, tp, RIGHT_CNT, pag->agf_cnt_root, pag->agf_cnt_level, &new_cnt_root, &new_cnt_lvl);
        if (rc != 0) {
            return rc;
        }
        pag->agf_cnt_root = static_cast<xfs_agblock_t>(new_cnt_root);
        pag->agf_cnt_level = new_cnt_lvl;
    }

    // ---- 4. Update AGF free block count and btree roots ----
    pag->agf_freeblks -= alloc_len;

    // Read the on-disk AGF block and update it (freeblks + possibly new roots/levels)
    uint64_t const AG_START_BLOCK = xfs_agbno_to_fsbno(agno, 0, mount->ag_blk_log);
    BufHead* agf_bh = xfs_buf_read(mount, AG_START_BLOCK);
    if (agf_bh != nullptr) {
        size_t const AGF_OFFSET = mount->sect_size;
        auto* agf = reinterpret_cast<XfsAgf*>(agf_bh->data + AGF_OFFSET);
        agf->agf_freeblks = Be32::from_cpu(pag->agf_freeblks);
        agf->agf_bno_root = Be32::from_cpu(pag->agf_bno_root);
        agf->agf_cnt_root = Be32::from_cpu(pag->agf_cnt_root);
        agf->agf_bno_level = Be32::from_cpu(pag->agf_bno_level);
        agf->agf_cnt_level = Be32::from_cpu(pag->agf_cnt_level);
        // Recompute CRC
        agf->agf_crc = Be32{0};
        uint32_t crc = util::crc32c_block_with_cksum(agf, sizeof(XfsAgf), XFS_AGF_CRC_OFF);
        __builtin_memcpy(&agf->agf_crc, &crc, sizeof(crc));
        // ---- 5. Log the AGF buffer ----
        xfs_trans_log_buf(tp, agf_bh, static_cast<uint32_t>(AGF_OFFSET), static_cast<uint32_t>(sizeof(XfsAgf)));
        brelse(agf_bh);
    }

    result->agno = agno;
    result->agbno = alloc_start;
    result->len = alloc_len;
#ifdef XFS_DEBUG
    mod::dbg::log("[xfs alloc] allocated AG %u block %u len %u\n", agno, alloc_start, alloc_len);
#endif
    return 0;
}

void log_agf_free_space_roots(XfsMountContext* mount, XfsTransaction* tp, xfs_agnumber_t agno) {
    XfsPerAG const* pag = &mount->per_ag[agno];
    uint64_t const AG_START_BLOCK = xfs_agbno_to_fsbno(agno, 0, mount->ag_blk_log);
    BufHead* agf_bh = xfs_buf_read(mount, AG_START_BLOCK);
    if (agf_bh == nullptr) {
        return;
    }

    size_t const AGF_OFFSET = mount->sect_size;
    auto* agf = reinterpret_cast<XfsAgf*>(agf_bh->data + AGF_OFFSET);
    agf->agf_freeblks = Be32::from_cpu(pag->agf_freeblks);
    agf->agf_bno_root = Be32::from_cpu(pag->agf_bno_root);
    agf->agf_cnt_root = Be32::from_cpu(pag->agf_cnt_root);
    agf->agf_bno_level = Be32::from_cpu(pag->agf_bno_level);
    agf->agf_cnt_level = Be32::from_cpu(pag->agf_cnt_level);
    agf->agf_crc = Be32{0};
    uint32_t crc = util::crc32c_block_with_cksum(agf, sizeof(XfsAgf), XFS_AGF_CRC_OFF);
    __builtin_memcpy(&agf->agf_crc, &crc, sizeof(crc));
    xfs_trans_log_buf(tp, agf_bh, static_cast<uint32_t>(AGF_OFFSET), static_cast<uint32_t>(sizeof(XfsAgf)));
    brelse(agf_bh);
}

auto delete_free_extent_record(XfsMountContext* mount, XfsTransaction* tp, xfs_agnumber_t agno, xfs_agblock_t startblock,
                               xfs_extlen_t blockcount) -> int {
    XfsPerAG const* pag = &mount->per_ag[agno];

    XfsBtreeCursor<XfsBnobtTraits> bno_cur;
    bno_cur.mount = mount;
    bno_cur.agno = agno;
    XfsBnobtTraits::IRec const BNO_TARGET{.startblock = startblock, .blockcount = 0};
    int rc = xfs_btree_lookup(&bno_cur, pag->agf_bno_root, pag->agf_bno_level, BNO_TARGET, XfsBtreeLookup::EQ);
    if (rc != 0) {
        return rc;
    }
    XfsBnobtTraits::IRec const BNO_REC = xfs_btree_get_rec(&bno_cur);
    if (BNO_REC.blockcount != blockcount) {
        return -EIO;
    }
    rc = xfs_btree_delete(&bno_cur, tp);
    if (rc != 0) {
        return rc;
    }

    XfsBtreeCursor<XfsCntbtTraits> cnt_cur;
    cnt_cur.mount = mount;
    cnt_cur.agno = agno;
    XfsCntbtTraits::IRec const CNT_TARGET{.startblock = startblock, .blockcount = blockcount};
    rc = xfs_btree_lookup(&cnt_cur, pag->agf_cnt_root, pag->agf_cnt_level, CNT_TARGET, XfsBtreeLookup::EQ);
    if (rc != 0) {
        return rc;
    }
    return xfs_btree_delete(&cnt_cur, tp);
}

}  // anonymous namespace

auto xfs_alloc_extent(XfsMountContext* mount, XfsTransaction* tp, const XfsAllocReq& req, XfsAllocResult* result) -> int {
    if (mount == nullptr || result == nullptr) {
        return -EINVAL;
    }
    if (req.minlen == 0) {
        return -EINVAL;
    }

    // Try preferred AG first
    if (req.agno != NULLAGNUMBER && req.agno < mount->ag_count) {
        int const RC = alloc_ag_by_size(mount, tp, req.agno, req, result);
        if (RC == 0) {
            return 0;
        }
    }

    // Round-robin through all AGs looking for space
    for (xfs_agnumber_t ag = 0; ag < mount->ag_count; ag++) {
        if (ag == req.agno) {
            continue;  // already tried
        }
        int const RC = alloc_ag_by_size(mount, tp, ag, req, result);
        if (RC == 0) {
            return 0;
        }
    }

    return -ENOSPC;
}

auto xfs_free_extent(XfsMountContext* mount, XfsTransaction* tp, xfs_agnumber_t agno, xfs_agblock_t agbno, xfs_extlen_t len) -> int {
    if (mount == nullptr) {
        return -EINVAL;
    }
    if (agno >= mount->ag_count) {
        return -EINVAL;
    }
    if (len == 0) {
        return -EINVAL;
    }

    XfsPerAG* pag = &mount->per_ag[agno];

    xfs_agblock_t merged_start = agbno;
    xfs_extlen_t merged_len = len;

    XfsBtreeCursor<XfsBnobtTraits> prev_cur;
    prev_cur.mount = mount;
    prev_cur.agno = agno;
    XfsBnobtTraits::IRec const PREV_TARGET{.startblock = agbno, .blockcount = 0};
    bool merge_prev = false;
    XfsBnobtTraits::IRec prev_rec{};
    int rc = xfs_btree_lookup(&prev_cur, pag->agf_bno_root, pag->agf_bno_level, PREV_TARGET, XfsBtreeLookup::LE);
    if (rc == 0) {
        prev_rec = xfs_btree_get_rec(&prev_cur);
        xfs_agblock_t const PREV_END = prev_rec.startblock + prev_rec.blockcount;
        if (PREV_END > agbno) {
            return -EIO;
        }
        if (PREV_END == agbno) {
            merge_prev = true;
            merged_start = prev_rec.startblock;
            merged_len += prev_rec.blockcount;
        }
    } else if (rc != -ENOENT) {
        return rc;
    }

    xfs_agblock_t const FREE_END = agbno + len;
    XfsBtreeCursor<XfsBnobtTraits> next_cur;
    next_cur.mount = mount;
    next_cur.agno = agno;
    XfsBnobtTraits::IRec const NEXT_TARGET{.startblock = FREE_END, .blockcount = 0};
    bool merge_next = false;
    XfsBnobtTraits::IRec next_rec{};
    rc = xfs_btree_lookup(&next_cur, pag->agf_bno_root, pag->agf_bno_level, NEXT_TARGET, XfsBtreeLookup::GE);
    if (rc == 0) {
        next_rec = xfs_btree_get_rec(&next_cur);
        if (next_rec.startblock < FREE_END) {
            return -EIO;
        }
        if (next_rec.startblock == FREE_END) {
            merge_next = true;
            merged_len += next_rec.blockcount;
        }
    } else if (rc != -ENOENT) {
        return rc;
    }

    if (merge_prev) {
        rc = delete_free_extent_record(mount, tp, agno, prev_rec.startblock, prev_rec.blockcount);
        if (rc != 0) {
            return rc;
        }
    }
    if (merge_next) {
        rc = delete_free_extent_record(mount, tp, agno, next_rec.startblock, next_rec.blockcount);
        if (rc != 0) {
            return rc;
        }
    }

    // Insert into bnobt
    XfsBtreeCursor<XfsBnobtTraits> bno_cur;
    bno_cur.mount = mount;
    bno_cur.agno = agno;

    uint64_t new_bno_root = pag->agf_bno_root;
    uint8_t new_bno_lvl = pag->agf_bno_level;
    XfsBnobtTraits::IRec const BNO_REC{.startblock = merged_start, .blockcount = merged_len};
    rc = xfs_btree_insert(&bno_cur, tp, BNO_REC, pag->agf_bno_root, pag->agf_bno_level, &new_bno_root, &new_bno_lvl);
    if (rc != 0) {
        return rc;
    }
    pag->agf_bno_root = static_cast<xfs_agblock_t>(new_bno_root);
    pag->agf_bno_level = new_bno_lvl;

    // Insert into cntbt
    XfsBtreeCursor<XfsCntbtTraits> cnt_cur;
    cnt_cur.mount = mount;
    cnt_cur.agno = agno;

    uint64_t new_cnt_root = pag->agf_cnt_root;
    uint8_t new_cnt_lvl = pag->agf_cnt_level;
    XfsCntbtTraits::IRec const CNT_REC{.startblock = merged_start, .blockcount = merged_len};
    rc = xfs_btree_insert(&cnt_cur, tp, CNT_REC, pag->agf_cnt_root, pag->agf_cnt_level, &new_cnt_root, &new_cnt_lvl);
    if (rc != 0) {
        return rc;
    }
    pag->agf_cnt_root = static_cast<xfs_agblock_t>(new_cnt_root);
    pag->agf_cnt_level = new_cnt_lvl;

    // Update AGF free block count and possibly new btree roots
    pag->agf_freeblks += len;

    log_agf_free_space_roots(mount, tp, agno);

#ifdef XFS_DEBUG
    mod::dbg::log("[xfs free] freed AG %u block %u len %u (merged to block %u len %u)\n", agno, agbno, len, merged_start, merged_len);
#endif
    return 0;
}

// ============================================================================
// AGFL - AG Free List get/put
// ============================================================================
//
// The AGFL is a circular array of pre-reserved blocks stored in sector 3 of
// AG block 0.  It is used by btree split/merge paths to obtain or return
// single blocks without touching the free space btrees, breaking the recursion:
//   xfs_btree_delete -> (old: xfs_free_extent -> xfs_btree_insert ->
//                        btree_alloc_new_block -> xfs_alloc_extent -> recurse)
//   xfs_btree_delete -> (new: xfs_alloc_put_freelist - no btree ops)
//   btree_alloc_new_block -> (new: xfs_alloc_get_freelist - no btree ops)

namespace {

// Write the updated AGF fields (flfirst, fllast, flcount) back to the on-disk
// AGF and log the buffer.  Recomputes the AGF CRC.
void log_agf_freelist(XfsMountContext* mount, XfsTransaction* tp, xfs_agnumber_t agno, BufHead* ag0_bh) {
    XfsPerAG const* pag = &mount->per_ag[agno];
    size_t const AGF_OFF = mount->sect_size;
    auto* agf = reinterpret_cast<XfsAgf*>(ag0_bh->data + AGF_OFF);
    agf->agf_flfirst = Be32::from_cpu(pag->agf_flfirst);
    agf->agf_fllast = Be32::from_cpu(pag->agf_fllast);
    agf->agf_flcount = Be32::from_cpu(pag->agf_flcount);
    agf->agf_crc = Be32{0};
    uint32_t crc = util::crc32c_block_with_cksum(agf, sizeof(XfsAgf), XFS_AGF_CRC_OFF);
    __builtin_memcpy(&agf->agf_crc, &crc, sizeof(crc));
    xfs_trans_log_buf(tp, ag0_bh, static_cast<uint32_t>(AGF_OFF), static_cast<uint32_t>(sizeof(XfsAgf)));
}

}  // anonymous namespace

auto xfs_alloc_get_freelist(XfsMountContext* mount, XfsTransaction* tp, xfs_agnumber_t agno, xfs_agblock_t* out_bno) -> int {
    if (mount == nullptr || out_bno == nullptr || agno >= mount->ag_count) {
        return -EINVAL;
    }

    XfsPerAG* pag = &mount->per_ag[agno];
    if (pag->agf_flcount == 0) {
        return -ENOSPC;
    }

    uint64_t const AG0 = xfs_agbno_to_fsbno(agno, 0, mount->ag_blk_log);
    BufHead* bh = xfs_buf_read(mount, AG0);
    if (bh == nullptr) {
        return -EIO;
    }

    size_t const AGFL_OFF = static_cast<size_t>(mount->sect_size) * 3UL;
    auto* agfl = reinterpret_cast<XfsAgfl*>(bh->data + AGFL_OFF);
    if (agfl->agfl_magicnum.to_cpu() != XFS_AGFL_MAGIC) {
        brelse(bh);
        return -EIO;
    }

    auto* agfl_bno = reinterpret_cast<Be32*>(reinterpret_cast<uint8_t*>(agfl) + sizeof(XfsAgfl));
    uint32_t const AGFL_SZ = xfs_agfl_size(mount);

    // Drain any corrupted NULLAGBLOCK slots from the head of the freelist.
    // This matches Linux's xfs_verify_agbno check in xfs_alloc_get_freelist().
    while (pag->agf_flcount > 0) {
        uint32_t const BNO_RAW = agfl_bno[pag->agf_flfirst].to_cpu();
        if (BNO_RAW != 0xFFFFFFFFU && BNO_RAW < mount->ag_blocks) {
            // Valid entry found.
            break;
        }
        mod::dbg::log("[xfs agfl] get_freelist: agno=%u skipping corrupt slot[%u]=%u (fllast=%u flcount=%u)\n", agno, pag->agf_flfirst,
                      BNO_RAW, pag->agf_fllast, pag->agf_flcount);
        pag->agf_flfirst = (pag->agf_flfirst + 1) % AGFL_SZ;
        pag->agf_flcount--;
    }

    if (pag->agf_flcount == 0) {
        // All slots were corrupt or the list was already empty.
        log_agf_freelist(mount, tp, agno, bh);
        brelse(bh);
        return -ENOSPC;
    }

    *out_bno = agfl_bno[pag->agf_flfirst].to_cpu();

    pag->agf_flfirst = (pag->agf_flfirst + 1) % AGFL_SZ;
    pag->agf_flcount--;

    log_agf_freelist(mount, tp, agno, bh);
    brelse(bh);
    return 0;
}

auto xfs_alloc_put_freelist(XfsMountContext* mount, XfsTransaction* tp, xfs_agnumber_t agno, xfs_agblock_t bno) -> int {
    if (mount == nullptr || agno >= mount->ag_count) {
        return -EINVAL;
    }

    if (bno <= 4) {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wframe-address"
        mod::dbg::log("[xfs agfl] put_freelist: SUSPICIOUS agno=%u bno=%u caller=%p caller2=%p\n", agno, bno, __builtin_return_address(0),
                      __builtin_return_address(1));
#pragma clang diagnostic pop
    }

    XfsPerAG* pag = &mount->per_ag[agno];

    // If the AGFL is full, fall back to returning the block to the free space
    // trees.  This path is safe: xfs_alloc_put_freelist is only called from
    // xfs_btree_delete (not from xfs_btree_insert), so xfs_free_extent won't
    // re-enter the delete path.
    if (pag->agf_flcount >= xfs_agfl_size(mount)) {
        return xfs_free_extent(mount, tp, agno, bno, 1);
    }

    uint64_t const AG0 = xfs_agbno_to_fsbno(agno, 0, mount->ag_blk_log);
    BufHead* bh = xfs_buf_read(mount, AG0);
    if (bh == nullptr) {
        return -EIO;
    }

    size_t const AGFL_OFF2 = static_cast<size_t>(mount->sect_size) * 3UL;
    auto* agfl = reinterpret_cast<XfsAgfl*>(bh->data + AGFL_OFF2);
    if (agfl->agfl_magicnum.to_cpu() != XFS_AGFL_MAGIC) {
        brelse(bh);
        return xfs_free_extent(mount, tp, agno, bno, 1);
    }

    auto* agfl_bno = reinterpret_cast<Be32*>(reinterpret_cast<uint8_t*>(agfl) + sizeof(XfsAgfl));

    if (bno == 0xFFFFFFFFU || bno >= mount->ag_blocks) {
        mod::dbg::log("[xfs agfl] put_freelist: agno=%u BAD bno=%u flfirst=%u fllast=%u flcount=%u\n", agno, bno, pag->agf_flfirst,
                      pag->agf_fllast, pag->agf_flcount);
    }

    // Advance tail and write the block number.
    if (pag->agf_flcount == 0) {
        // Empty list: write at slot 0, reset both pointers.
        pag->agf_flfirst = 0;
        pag->agf_fllast = 0;
    } else {
        pag->agf_fllast = (pag->agf_fllast + 1) % xfs_agfl_size(mount);
    }
    agfl_bno[pag->agf_fllast] = Be32::from_cpu(bno);
    pag->agf_flcount++;

    // Log the modified slot in the AGFL sector.
    size_t const SLOT_OFF = AGFL_OFF2 + sizeof(XfsAgfl) + (static_cast<size_t>(pag->agf_fllast) * sizeof(uint32_t));
    xfs_trans_log_buf(tp, bh, static_cast<uint32_t>(SLOT_OFF), static_cast<uint32_t>(sizeof(uint32_t)));

    // Also update and log the AGF (fllast, flcount changed).
    log_agf_freelist(mount, tp, agno, bh);
    brelse(bh);
    return 0;
}

}  // namespace ker::vfs::xfs
