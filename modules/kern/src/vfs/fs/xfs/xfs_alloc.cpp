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

// Minimum number of blocks the AGFL should hold before we perform allocations
// that may trigger btree splits.  Each split consumes at most one AGFL block,
// and a two-level tree can have at most two simultaneous splits (leaf + root),
// so 4 is a safe minimum.
constexpr uint32_t XFS_AGFL_MIN = 4;

// Top up the AGFL for the given AG to at least XFS_AGFL_MIN blocks.
// Called at the start of alloc_ag_by_size when the AGFL is running low,
// BEFORE any cursor is opened on the free space trees.  This is the only
// safe point to do this — once a cursor is open and a split is in progress,
// calling xfs_alloc_extent would re-enter and corrupt the tree.
void agfl_refill(XfsMountContext* mount, XfsTransaction* tp, xfs_agnumber_t agno) {
    XfsPerAG* pag = &mount->per_ag[agno];


    while (pag->agf_flcount < XFS_AGFL_MIN && pag->agf_freeblks > 0) {
        // Find and delete the smallest available extent (size >= 1)
        XfsBtreeCursor<XfsCntbtTraits> cur;
        cur.mount = mount;
        cur.agno = agno;
        XfsCntbtTraits::IRec target{.blockcount = 1, .startblock = 0};
        if (xfs_btree_lookup(&cur, pag->agf_cnt_root, pag->agf_cnt_level, target, XfsBtreeLookup::GE) != 0) {
            break;
        }
        XfsCntbtTraits::IRec found = xfs_btree_get_rec(&cur);
        if (found.blockcount == 0) { break; }

        // Delete from cntbt
        uint64_t new_cnt_root = pag->agf_cnt_root; uint8_t new_cnt_lvl = pag->agf_cnt_level;
        if (xfs_btree_delete(&cur, tp) != 0) { break; }
        // Note: btree_delete may shrink the root; pick up updated root after
        // the delete via the cursor's updated nlevels/levels — but the pag
        // fields are updated when we write the AGF below.

        // Delete from bnobt
        XfsBtreeCursor<XfsBnobtTraits> bno_cur;
        bno_cur.mount = mount;
        bno_cur.agno = agno;
        XfsBnobtTraits::IRec bno_target{.startblock = found.startblock, .blockcount = 0};
        if (xfs_btree_lookup(&bno_cur, pag->agf_bno_root, pag->agf_bno_level, bno_target, XfsBtreeLookup::GE) != 0) { break; }
        if (xfs_btree_delete(&bno_cur, tp) != 0) { break; }

        xfs_extlen_t  ext_len = found.blockcount;
        // Steal the LAST block of the extent so the remainder keeps its original
        // startblock — this preserves large contiguous free runs for data allocation.
        xfs_agblock_t stolen = found.startblock + ext_len - 1;

        // Re-insert the remainder (all but the stolen last block)
        if (ext_len > 1) {
            xfs_agblock_t rem_start = found.startblock;
            xfs_extlen_t  rem_len   = ext_len - 1;
            uint64_t new_bno_root = pag->agf_bno_root; uint8_t new_bno_lvl = pag->agf_bno_level;

            XfsBnobtTraits::IRec rem_bno{.startblock = rem_start, .blockcount = rem_len};
            if (xfs_btree_insert(&bno_cur, tp, rem_bno, pag->agf_bno_root, pag->agf_bno_level,
                                 &new_bno_root, &new_bno_lvl) != 0) { break; }
            pag->agf_bno_root = static_cast<xfs_agblock_t>(new_bno_root);
            pag->agf_bno_level = new_bno_lvl;

            XfsCntbtTraits::IRec rem_cnt{.startblock = rem_start, .blockcount = rem_len};
            if (xfs_btree_insert(&cur, tp, rem_cnt, pag->agf_cnt_root, pag->agf_cnt_level,
                                 &new_cnt_root, &new_cnt_lvl) != 0) { break; }
            pag->agf_cnt_root = static_cast<xfs_agblock_t>(new_cnt_root);
            pag->agf_cnt_level = new_cnt_lvl;
        }

        pag->agf_freeblks--;

        // Push the stolen block to the AGFL
        xfs_alloc_put_freelist(mount, tp, agno, stolen);

        // Write updated AGF (roots, levels, freeblks) to disk
        uint64_t ag0 = xfs_agbno_to_fsbno(agno, 0, mount->ag_blk_log);
        BufHead* agf_bh = xfs_buf_read(mount, ag0);
        if (agf_bh != nullptr) {
            size_t agf_off = mount->sect_size;
            auto* agf = reinterpret_cast<XfsAgf*>(agf_bh->data + agf_off);
            agf->agf_freeblks  = __be32::from_cpu(pag->agf_freeblks);
            agf->agf_bno_root  = __be32::from_cpu(pag->agf_bno_root);
            agf->agf_cnt_root  = __be32::from_cpu(pag->agf_cnt_root);
            agf->agf_bno_level = __be32::from_cpu(pag->agf_bno_level);
            agf->agf_cnt_level = __be32::from_cpu(pag->agf_cnt_level);
            agf->agf_crc = __be32{0};
            uint32_t crc = util::crc32c_block_with_cksum(agf, sizeof(XfsAgf), XFS_AGF_CRC_OFF);
            __builtin_memcpy(&agf->agf_crc, &crc, sizeof(crc));
            xfs_trans_log_buf(tp, agf_bh, static_cast<uint32_t>(agf_off), static_cast<uint32_t>(sizeof(XfsAgf)));
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
        uint64_t new_bno_root = pag->agf_bno_root; uint8_t new_bno_lvl = pag->agf_bno_level;
        uint64_t new_cnt_root = pag->agf_cnt_root; uint8_t new_cnt_lvl = pag->agf_cnt_level;

        XfsBnobtTraits::IRec left_bno{.startblock = found.startblock, .blockcount = left_len};
        rc = xfs_btree_insert(&bno_cur, tp, left_bno, pag->agf_bno_root, pag->agf_bno_level,
                              &new_bno_root, &new_bno_lvl);
        if (rc != 0) { return rc; }
        pag->agf_bno_root = static_cast<xfs_agblock_t>(new_bno_root);
        pag->agf_bno_level = new_bno_lvl;

        XfsCntbtTraits::IRec left_cnt{.startblock = found.startblock, .blockcount = left_len};
        rc = xfs_btree_insert(&cur, tp, left_cnt, pag->agf_cnt_root, pag->agf_cnt_level,
                              &new_cnt_root, &new_cnt_lvl);
        if (rc != 0) { return rc; }
        pag->agf_cnt_root = static_cast<xfs_agblock_t>(new_cnt_root);
        pag->agf_cnt_level = new_cnt_lvl;
    }

    // Right remainder: blocks after alloc_start+alloc_len
    xfs_agblock_t alloc_end = alloc_start + alloc_len;
    xfs_agblock_t orig_end = found.startblock + found.blockcount;
    if (alloc_end < orig_end) {
        xfs_extlen_t right_len = orig_end - alloc_end;
        uint64_t new_bno_root = pag->agf_bno_root; uint8_t new_bno_lvl = pag->agf_bno_level;
        uint64_t new_cnt_root = pag->agf_cnt_root; uint8_t new_cnt_lvl = pag->agf_cnt_level;

        XfsBnobtTraits::IRec right_bno{.startblock = alloc_end, .blockcount = right_len};
        rc = xfs_btree_insert(&bno_cur, tp, right_bno, pag->agf_bno_root, pag->agf_bno_level,
                              &new_bno_root, &new_bno_lvl);
        if (rc != 0) { return rc; }
        pag->agf_bno_root = static_cast<xfs_agblock_t>(new_bno_root);
        pag->agf_bno_level = new_bno_lvl;

        XfsCntbtTraits::IRec right_cnt{.startblock = alloc_end, .blockcount = right_len};
        rc = xfs_btree_insert(&cur, tp, right_cnt, pag->agf_cnt_root, pag->agf_cnt_level,
                              &new_cnt_root, &new_cnt_lvl);
        if (rc != 0) { return rc; }
        pag->agf_cnt_root = static_cast<xfs_agblock_t>(new_cnt_root);
        pag->agf_cnt_level = new_cnt_lvl;
    }

    // ---- 4. Update AGF free block count and btree roots ----
    pag->agf_freeblks -= alloc_len;

    // Read the on-disk AGF block and update it (freeblks + possibly new roots/levels)
    uint64_t ag_start_block = xfs_agbno_to_fsbno(agno, 0, mount->ag_blk_log);
    BufHead* agf_bh = xfs_buf_read(mount, ag_start_block);
    if (agf_bh != nullptr) {
        size_t agf_offset = mount->sect_size;
        auto* agf = reinterpret_cast<XfsAgf*>(agf_bh->data + agf_offset);
        agf->agf_freeblks  = __be32::from_cpu(pag->agf_freeblks);
        agf->agf_bno_root  = __be32::from_cpu(pag->agf_bno_root);
        agf->agf_cnt_root  = __be32::from_cpu(pag->agf_cnt_root);
        agf->agf_bno_level = __be32::from_cpu(pag->agf_bno_level);
        agf->agf_cnt_level = __be32::from_cpu(pag->agf_cnt_level);
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

    uint64_t new_bno_root = pag->agf_bno_root; uint8_t new_bno_lvl = pag->agf_bno_level;
    XfsBnobtTraits::IRec bno_rec{agbno, len};
    int rc = xfs_btree_insert(&bno_cur, tp, bno_rec, pag->agf_bno_root, pag->agf_bno_level,
                              &new_bno_root, &new_bno_lvl);
    if (rc != 0) { return rc; }
    pag->agf_bno_root = static_cast<xfs_agblock_t>(new_bno_root);
    pag->agf_bno_level = new_bno_lvl;

    // Insert into cntbt
    XfsBtreeCursor<XfsCntbtTraits> cnt_cur;
    cnt_cur.mount = mount;
    cnt_cur.agno = agno;

    uint64_t new_cnt_root = pag->agf_cnt_root; uint8_t new_cnt_lvl = pag->agf_cnt_level;
    XfsCntbtTraits::IRec cnt_rec{agbno, len};
    rc = xfs_btree_insert(&cnt_cur, tp, cnt_rec, pag->agf_cnt_root, pag->agf_cnt_level,
                          &new_cnt_root, &new_cnt_lvl);
    if (rc != 0) { return rc; }
    pag->agf_cnt_root = static_cast<xfs_agblock_t>(new_cnt_root);
    pag->agf_cnt_level = new_cnt_lvl;

    // Update AGF free block count and possibly new btree roots
    pag->agf_freeblks += len;

    uint64_t ag_start_block = xfs_agbno_to_fsbno(agno, 0, mount->ag_blk_log);
    BufHead* agf_bh = xfs_buf_read(mount, ag_start_block);
    if (agf_bh != nullptr) {
        size_t agf_offset = mount->sect_size;
        auto* agf = reinterpret_cast<XfsAgf*>(agf_bh->data + agf_offset);
        agf->agf_freeblks  = __be32::from_cpu(pag->agf_freeblks);
        agf->agf_bno_root  = __be32::from_cpu(pag->agf_bno_root);
        agf->agf_cnt_root  = __be32::from_cpu(pag->agf_cnt_root);
        agf->agf_bno_level = __be32::from_cpu(pag->agf_bno_level);
        agf->agf_cnt_level = __be32::from_cpu(pag->agf_cnt_level);
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

// ============================================================================
// AGFL — AG Free List get/put
// ============================================================================
//
// The AGFL is a circular array of pre-reserved blocks stored in sector 3 of
// AG block 0.  It is used by btree split/merge paths to obtain or return
// single blocks without touching the free space btrees, breaking the recursion:
//   xfs_btree_delete → (old: xfs_free_extent → xfs_btree_insert →
//                        btree_alloc_new_block → xfs_alloc_extent → recurse)
//   xfs_btree_delete → (new: xfs_alloc_put_freelist — no btree ops)
//   btree_alloc_new_block → (new: xfs_alloc_get_freelist — no btree ops)

namespace {

// Write the updated AGF fields (flfirst, fllast, flcount) back to the on-disk
// AGF and log the buffer.  Recomputes the AGF CRC.
void log_agf_freelist(XfsMountContext* mount, XfsTransaction* tp, xfs_agnumber_t agno, BufHead* ag0_bh) {
    XfsPerAG* pag = &mount->per_ag[agno];
    size_t agf_off = mount->sect_size;
    auto* agf = reinterpret_cast<XfsAgf*>(ag0_bh->data + agf_off);
    agf->agf_flfirst = __be32::from_cpu(pag->agf_flfirst);
    agf->agf_fllast  = __be32::from_cpu(pag->agf_fllast);
    agf->agf_flcount = __be32::from_cpu(pag->agf_flcount);
    agf->agf_crc = __be32{0};
    uint32_t crc = util::crc32c_block_with_cksum(agf, sizeof(XfsAgf), XFS_AGF_CRC_OFF);
    __builtin_memcpy(&agf->agf_crc, &crc, sizeof(crc));
    xfs_trans_log_buf(tp, ag0_bh, static_cast<uint32_t>(agf_off), static_cast<uint32_t>(sizeof(XfsAgf)));
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

    uint64_t ag0 = xfs_agbno_to_fsbno(agno, 0, mount->ag_blk_log);
    BufHead* bh = xfs_buf_read(mount, ag0);
    if (bh == nullptr) {
        return -EIO;
    }

    size_t agfl_off = static_cast<size_t>(mount->sect_size) * 3UL;
    auto* agfl = reinterpret_cast<XfsAgfl*>(bh->data + agfl_off);
    if (agfl->agfl_magicnum.to_cpu() != XFS_AGFL_MAGIC) {
        brelse(bh);
        return -EIO;
    }

    auto* agfl_bno = reinterpret_cast<__be32*>(reinterpret_cast<uint8_t*>(agfl) + sizeof(XfsAgfl));
    uint32_t agfl_sz = xfs_agfl_size(mount);

    // Drain any corrupted NULLAGBLOCK slots from the head of the freelist.
    // This matches Linux's xfs_verify_agbno check in xfs_alloc_get_freelist().
    while (pag->agf_flcount > 0) {
        uint32_t bno_raw = agfl_bno[pag->agf_flfirst].to_cpu();
        if (bno_raw != 0xFFFFFFFFU && bno_raw < mount->ag_blocks) {
            // Valid entry found.
            break;
        }
        mod::dbg::log("[xfs agfl] get_freelist: agno=%u skipping corrupt slot[%u]=%u (fllast=%u flcount=%u)\n",
                      agno, pag->agf_flfirst, bno_raw, pag->agf_fllast, pag->agf_flcount);
        pag->agf_flfirst = (pag->agf_flfirst + 1) % agfl_sz;
        pag->agf_flcount--;
    }

    if (pag->agf_flcount == 0) {
        // All slots were corrupt or the list was already empty.
        log_agf_freelist(mount, tp, agno, bh);
        brelse(bh);
        return -ENOSPC;
    }

    *out_bno = agfl_bno[pag->agf_flfirst].to_cpu();

    pag->agf_flfirst = (pag->agf_flfirst + 1) % agfl_sz;
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
        mod::dbg::log("[xfs agfl] put_freelist: SUSPICIOUS agno=%u bno=%u caller=%p caller2=%p\n",
                      agno, bno, __builtin_return_address(0), __builtin_return_address(1));
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

    uint64_t ag0 = xfs_agbno_to_fsbno(agno, 0, mount->ag_blk_log);
    BufHead* bh = xfs_buf_read(mount, ag0);
    if (bh == nullptr) {
        return -EIO;
    }

    size_t agfl_off2 = static_cast<size_t>(mount->sect_size) * 3UL;
    auto* agfl = reinterpret_cast<XfsAgfl*>(bh->data + agfl_off2);
    if (agfl->agfl_magicnum.to_cpu() != XFS_AGFL_MAGIC) {
        brelse(bh);
        return xfs_free_extent(mount, tp, agno, bno, 1);
    }

    auto* agfl_bno = reinterpret_cast<__be32*>(reinterpret_cast<uint8_t*>(agfl) + sizeof(XfsAgfl));

    if (bno == 0xFFFFFFFFU || bno >= mount->ag_blocks) {
        mod::dbg::log("[xfs agfl] put_freelist: agno=%u BAD bno=%u flfirst=%u fllast=%u flcount=%u\n",
                      agno, bno, pag->agf_flfirst, pag->agf_fllast, pag->agf_flcount);
    }

    // Advance tail and write the block number.
    if (pag->agf_flcount == 0) {
        // Empty list: write at slot 0, reset both pointers.
        pag->agf_flfirst = 0;
        pag->agf_fllast  = 0;
    } else {
        pag->agf_fllast = (pag->agf_fllast + 1) % xfs_agfl_size(mount);
    }
    agfl_bno[pag->agf_fllast] = __be32::from_cpu(bno);
    pag->agf_flcount++;

    // Log the modified slot in the AGFL sector.
    size_t slot_off = agfl_off2 + sizeof(XfsAgfl) + (static_cast<size_t>(pag->agf_fllast) * sizeof(uint32_t));
    xfs_trans_log_buf(tp, bh, static_cast<uint32_t>(slot_off), static_cast<uint32_t>(sizeof(uint32_t)));

    // Also update and log the AGF (fllast, flcount changed).
    log_agf_freelist(mount, tp, agno, bh);
    brelse(bh);
    return 0;
}

}  // namespace ker::vfs::xfs
