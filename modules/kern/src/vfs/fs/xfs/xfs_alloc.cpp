// XFS Free Space Allocation implementation.
//
// Allocates blocks from AG free space B+trees (bnobt/cntbt).  The allocation
// strategy is:
// 1. If a preferred AG is given, try that AG first.
// 2. Search the cntbt (by-count tree) for the smallest free extent >= minlen.
// 3. If alignment is required, round up the starting block.
// 4. Split the free extent: allocate the requested portion, leave the rest.
//
// Reference: reference/xfs/libxfs/xfs_alloc.c

#include "xfs_alloc.hpp"

#include <algorithm>
#include <cerrno>
#include <platform/dbg/dbg.hpp>
#include <util/crc32c.hpp>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_btree.hpp>

namespace ker::vfs::xfs {

namespace {

// Try to allocate from a specific AG using the cntbt (by-count tree)
auto alloc_ag_by_size(XfsMountContext* mount, XfsTransaction* tp, xfs_agnumber_t agno, const XfsAllocReq& req, XfsAllocResult* result)
    -> int {
    XfsPerAG* pag = &mount->per_ag[agno];

    // Quick check: does this AG have enough free blocks?
    if (pag->agf_freeblks < req.minlen) {
        return -ENOSPC;
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

    XfsCntbtTraits::IRec found = xfs_btree_get_rec(&cur);

    if (found.blockcount < req.minlen) {
        return -ENOSPC;
    }

    // Compute actual allocation length
    xfs_extlen_t alloc_len = found.blockcount;
    alloc_len = std::min(alloc_len, req.maxlen);

    xfs_agblock_t alloc_start = found.startblock;

    // Apply alignment if requested
    if (req.alignment > 1) {
        xfs_agblock_t aligned = (alloc_start + req.alignment - 1) & ~(static_cast<xfs_agblock_t>(req.alignment) - 1);
        xfs_extlen_t lost = aligned - alloc_start;
        if (lost >= found.blockcount) {
            return -ENOSPC;
        }
        alloc_start = aligned;
        xfs_extlen_t remaining = found.blockcount - lost;
        if (remaining < req.minlen) {
            return -ENOSPC;
        }
        alloc_len = std::min(alloc_len, remaining);
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
    bno_target.startblock = found.startblock;
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
    if (alloc_start > found.startblock) {
        xfs_extlen_t left_len = alloc_start - found.startblock;

        XfsBnobtTraits::IRec left_bno{found.startblock, left_len};
        rc = xfs_btree_insert(&bno_cur, tp, left_bno, pag->agf_bno_root, pag->agf_bno_level, nullptr, nullptr);
        if (rc != 0) {
            return rc;
        }

        XfsCntbtTraits::IRec left_cnt{found.startblock, left_len};
        rc = xfs_btree_insert(&cur, tp, left_cnt, pag->agf_cnt_root, pag->agf_cnt_level, nullptr, nullptr);
        if (rc != 0) {
            return rc;
        }
    }

    // Right remainder: blocks after alloc_start+alloc_len
    xfs_agblock_t alloc_end = alloc_start + alloc_len;
    xfs_agblock_t orig_end = found.startblock + found.blockcount;
    if (alloc_end < orig_end) {
        xfs_extlen_t right_len = orig_end - alloc_end;

        XfsBnobtTraits::IRec right_bno{alloc_end, right_len};
        rc = xfs_btree_insert(&bno_cur, tp, right_bno, pag->agf_bno_root, pag->agf_bno_level, nullptr, nullptr);
        if (rc != 0) {
            return rc;
        }

        XfsCntbtTraits::IRec right_cnt{alloc_end, right_len};
        rc = xfs_btree_insert(&cur, tp, right_cnt, pag->agf_cnt_root, pag->agf_cnt_level, nullptr, nullptr);
        if (rc != 0) {
            return rc;
        }
    }

    // ---- 4. Update AGF free block count ----
    pag->agf_freeblks -= alloc_len;

    // Read the on-disk AGF block and update it
    uint64_t ag_start_block = xfs_agbno_to_fsbno(agno, 0, mount->ag_blk_log);
    BufHead* agf_bh = xfs_buf_read(mount, ag_start_block);
    if (agf_bh != nullptr) {
        size_t agf_offset = mount->sect_size;
        auto* agf = reinterpret_cast<XfsAgf*>(agf_bh->data + agf_offset);
        agf->agf_freeblks = __be32::from_cpu(pag->agf_freeblks);
        // Recompute CRC
        agf->agf_crc = __be32{0};
        uint32_t crc = util::crc32c_block_with_cksum(agf, sizeof(XfsAgf), XFS_AGF_CRC_OFF);
        __builtin_memcpy(&agf->agf_crc, &crc, sizeof(crc));
        // ---- 5. Log the AGF buffer ----
        xfs_trans_log_buf(tp, agf_bh, static_cast<uint32_t>(agf_offset), static_cast<uint32_t>(sizeof(XfsAgf)));
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
        int rc = alloc_ag_by_size(mount, tp, req.agno, req, result);
        if (rc == 0) {
            return 0;
        }
    }

    // Round-robin through all AGs looking for space
    for (xfs_agnumber_t ag = 0; ag < mount->ag_count; ag++) {
        if (ag == req.agno) {
            continue;  // already tried
        }
        int rc = alloc_ag_by_size(mount, tp, ag, req, result);
        if (rc == 0) {
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

    // NOTE: A production implementation would merge with adjacent free
    // extents (coalescing).  For now we insert directly into both btrees.

    XfsPerAG* pag = &mount->per_ag[agno];

    // Insert into bnobt
    XfsBtreeCursor<XfsBnobtTraits> bno_cur;
    bno_cur.mount = mount;
    bno_cur.agno = agno;

    XfsBnobtTraits::IRec bno_rec{agbno, len};
    int rc = xfs_btree_insert(&bno_cur, tp, bno_rec, pag->agf_bno_root, pag->agf_bno_level, nullptr, nullptr);
    if (rc != 0) {
        return rc;
    }

    // Insert into cntbt
    XfsBtreeCursor<XfsCntbtTraits> cnt_cur;
    cnt_cur.mount = mount;
    cnt_cur.agno = agno;

    XfsCntbtTraits::IRec cnt_rec{agbno, len};
    rc = xfs_btree_insert(&cnt_cur, tp, cnt_rec, pag->agf_cnt_root, pag->agf_cnt_level, nullptr, nullptr);
    if (rc != 0) {
        return rc;
    }

    // Update AGF free block count
    pag->agf_freeblks += len;

    uint64_t ag_start_block = xfs_agbno_to_fsbno(agno, 0, mount->ag_blk_log);
    BufHead* agf_bh = xfs_buf_read(mount, ag_start_block);
    if (agf_bh != nullptr) {
        size_t agf_offset = mount->sect_size;
        auto* agf = reinterpret_cast<XfsAgf*>(agf_bh->data + agf_offset);
        agf->agf_freeblks = __be32::from_cpu(pag->agf_freeblks);
        // Recompute CRC
        agf->agf_crc = __be32{0};
        uint32_t crc = util::crc32c_block_with_cksum(agf, sizeof(XfsAgf), XFS_AGF_CRC_OFF);
        __builtin_memcpy(&agf->agf_crc, &crc, sizeof(crc));
        // Log the AGF buffer
        xfs_trans_log_buf(tp, agf_bh, static_cast<uint32_t>(agf_offset), static_cast<uint32_t>(sizeof(XfsAgf)));
        brelse(agf_bh);
    }

    mod::dbg::log("[xfs free] freed AG %u block %u len %u\n", agno, agbno, len);
    return 0;
}

}  // namespace ker::vfs::xfs
