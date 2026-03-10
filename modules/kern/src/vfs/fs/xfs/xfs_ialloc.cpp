// XFS Inode Allocation implementation.
//
// Allocates inodes from the AG inode B+trees.  Each inobt record tracks a
// chunk of 64 inodes with a 64-bit free bitmask.  To allocate an inode:
// 1. Optionally use the finobt (free inode btree) to find a chunk with free inodes.
// 2. Scan the inobt for a record with free inodes (ir_freecount > 0).
// 3. Find the first free bit in ir_free bitmask, mark it allocated.
// 4. Compute the absolute inode number from the AG + chunk start + bit position.
//
// Reference: reference/xfs/libxfs/xfs_ialloc.c

#include "xfs_ialloc.hpp"

#include <cerrno>
#include <platform/dbg/dbg.hpp>
#include <util/crc32c.hpp>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_btree.hpp>

namespace ker::vfs::xfs {

namespace {

constexpr uint32_t XFS_INODES_PER_CHUNK = 64;

// Find the first set bit (least significant) in a 64-bit value
auto ffs64(uint64_t val) -> int {
    if (val == 0) {
        return -1;
    }
    int bit = 0;
    if ((val & 0xFFFFFFFF) == 0) {
        bit += 32;
        val >>= 32;
    }
    if ((val & 0xFFFF) == 0) {
        bit += 16;
        val >>= 16;
    }
    if ((val & 0xFF) == 0) {
        bit += 8;
        val >>= 8;
    }
    if ((val & 0xF) == 0) {
        bit += 4;
        val >>= 4;
    }
    if ((val & 0x3) == 0) {
        bit += 2;
        val >>= 2;
    }
    if ((val & 0x1) == 0) {
        bit += 1;
    }
    return bit;
}

// Try to allocate an inode from a specific AG
auto ialloc_ag(XfsMountContext* mount, XfsTransaction* tp, xfs_agnumber_t agno) -> xfs_ino_t {
    XfsPerAG* pag = &mount->per_ag[agno];

    if (pag->agi_freecount == 0) {
        return NULLFSINO;
    }

    // Always search the inobt directly.  Both inobt and finobt records
    // carry freecount/free_mask, but the finobt uses a different on-disk
    // magic number (FIB3 vs IAB3).  Using XfsBtreeCursor<XfsInobtTraits>
    // on finobt blocks causes a magic-validation failure.  Scanning the
    // inobt is slightly slower but correct; the update path below still
    // maintains both trees when finobt is present.
    xfs_agblock_t root = pag->agi_root;
    uint32_t level = pag->agi_level;

    // Set up cursor and search for any record (start from the beginning)
    XfsBtreeCursor<XfsInobtTraits> cur;
    cur.mount = mount;
    cur.agno = agno;

    XfsInobtTraits::IRec target{};
    target.startino = 0;  // start from smallest inode number

    int rc = xfs_btree_lookup(&cur, root, level, target, XfsBtreeLookup::GE);
    if (rc != 0) {
        return NULLFSINO;
    }

    // Scan forward to find a record with free inodes
    while (true) {
        XfsInobtTraits::IRec rec = xfs_btree_get_rec(&cur);

        if (rec.freecount > 0 && rec.free_mask != 0) {
            // Found a chunk with free inodes
            int bit = ffs64(rec.free_mask);
            if (bit >= 0 && bit < static_cast<int>(XFS_INODES_PER_CHUNK)) {
                xfs_agino_t agino = rec.startino + static_cast<xfs_agino_t>(bit);
                xfs_ino_t ino = (static_cast<xfs_ino_t>(agno) << mount->agino_log) | agino;

                // 1. Clear the bit in ir_free and decrement ir_freecount
                rec.free_mask &= ~(static_cast<uint64_t>(1) << bit);
                rec.freecount--;

                // 2-3. Update the inobt record on disk
                // We need to look up again via the inobt if we used finobt
                XfsBtreeCursor<XfsInobtTraits> ino_cur;
                ino_cur.mount = mount;
                ino_cur.agno = agno;

                XfsInobtTraits::IRec ino_target{};
                ino_target.startino = rec.startino;

                int urc = xfs_btree_lookup(&ino_cur, pag->agi_root, pag->agi_level, ino_target, XfsBtreeLookup::GE);
                if (urc == 0) {
                    xfs_btree_update(&ino_cur, tp, rec);
                }

                // 4. Update the finobt if present
                if (xfs_has_finobt(mount) && pag->agi_free_root != 0) {
                    XfsBtreeCursor<XfsFinobtTraits> fi_cur;
                    fi_cur.mount = mount;
                    fi_cur.agno = agno;

                    XfsFinobtTraits::IRec fi_target{};
                    fi_target.startino = rec.startino;

                    urc = xfs_btree_lookup(&fi_cur, pag->agi_free_root, pag->agi_free_level, fi_target, XfsBtreeLookup::GE);
                    if (urc == 0) {
                        if (rec.freecount == 0) {
                            // No more free inodes in this chunk — remove from finobt
                            xfs_btree_delete(&fi_cur, tp);
                        } else {
                            xfs_btree_update(&fi_cur, tp, rec);
                        }
                    }
                }

                // 5. Update AGI counters
                pag->agi_freecount--;

                uint64_t ag_start_block = xfs_agbno_to_fsbno(agno, 0, mount->ag_blk_log);
                BufHead* agi_bh = xfs_buf_read(mount, ag_start_block);
                if (agi_bh != nullptr) {
                    auto agi_offset = static_cast<size_t>(mount->sect_size * 2);
                    auto* agi = reinterpret_cast<XfsAgi*>(agi_bh->data + agi_offset);
                    agi->agi_freecount = __be32::from_cpu(pag->agi_freecount);
                    agi->agi_newino = __be32::from_cpu(agino);
                    // Recompute CRC
                    agi->agi_crc = __be32{0};
                    uint32_t crc = util::crc32c_block_with_cksum(agi, sizeof(XfsAgi), XFS_AGI_CRC_OFF);
                    __builtin_memcpy(&agi->agi_crc, &crc, sizeof(crc));
                    xfs_trans_log_buf(tp, agi_bh, static_cast<uint32_t>(agi_offset), static_cast<uint32_t>(sizeof(XfsAgi)));
                    brelse(agi_bh);
                }

                // 6. On-disk inode initialization happens via xfs_inode_write() at commit

                mod::dbg::log("[xfs ialloc] allocated inode %lu (AG %u agino %u)\n", (unsigned long)ino, agno, agino);
                return ino;
            }
        }

        rc = xfs_btree_increment(&cur);
        if (rc != 0) {
            break;
        }
    }

    return NULLFSINO;
}

}  // anonymous namespace

auto xfs_ialloc(XfsMountContext* mount, XfsTransaction* tp, [[maybe_unused]] uint16_t mode) -> xfs_ino_t {
    if (mount == nullptr) {
        return NULLFSINO;
    }

    // Try each AG in round-robin order
    for (xfs_agnumber_t ag = 0; ag < mount->ag_count; ag++) {
        xfs_ino_t ino = ialloc_ag(mount, tp, ag);
        if (ino != NULLFSINO) {
            return ino;
        }
    }

    mod::dbg::log("[xfs ialloc] no free inodes\n");
    return NULLFSINO;
}

auto xfs_ifree(XfsMountContext* mount, XfsTransaction* tp, xfs_ino_t ino) -> int {
    if (mount == nullptr || ino == NULLFSINO) {
        return -EINVAL;
    }

    // Decompose inode number into AG + agino
    xfs_agnumber_t agno = static_cast<xfs_agnumber_t>(ino >> mount->agino_log);
    xfs_agino_t agino = static_cast<xfs_agino_t>(ino & ((1ULL << mount->agino_log) - 1));

    if (agno >= mount->ag_count) {
        return -EINVAL;
    }

    XfsPerAG* pag = &mount->per_ag[agno];

    // Compute the chunk start inode (aligned to 64-inode boundary)
    xfs_agino_t chunk_start = agino & ~static_cast<xfs_agino_t>(XFS_INODES_PER_CHUNK - 1);
    uint32_t bit = agino - chunk_start;

    // 1. Find the inobt record for this inode's chunk
    XfsBtreeCursor<XfsInobtTraits> cur;
    cur.mount = mount;
    cur.agno = agno;

    XfsInobtTraits::IRec target{};
    target.startino = chunk_start;

    int rc = xfs_btree_lookup(&cur, pag->agi_root, pag->agi_level, target, XfsBtreeLookup::GE);
    if (rc != 0) {
        return -EIO;
    }

    XfsInobtTraits::IRec rec = xfs_btree_get_rec(&cur);
    if (rec.startino != chunk_start) {
        mod::dbg::log("[xfs ifree] inobt record mismatch: expected startino %u, got %u\n", chunk_start, rec.startino);
        return -EIO;
    }

    // 2-3. Set the free bit and increment freecount
    bool was_fully_allocated = (rec.freecount == 0);
    rec.free_mask |= (static_cast<uint64_t>(1) << bit);
    rec.freecount++;

    // 4. Update inobt on disk
    rc = xfs_btree_update(&cur, tp, rec);
    if (rc != 0) {
        return rc;
    }

    // 5. If finobt present, insert/update record
    if (xfs_has_finobt(mount) && pag->agi_free_root != 0) {
        XfsBtreeCursor<XfsFinobtTraits> fi_cur;
        fi_cur.mount = mount;
        fi_cur.agno = agno;

        XfsFinobtTraits::IRec fi_target{};
        fi_target.startino = chunk_start;

        int frc = xfs_btree_lookup(&fi_cur, pag->agi_free_root, pag->agi_free_level, fi_target, XfsBtreeLookup::GE);

        if (was_fully_allocated || frc != 0) {
            // Chunk wasn't in finobt — insert it
            xfs_btree_insert(&fi_cur, tp, rec, pag->agi_free_root, pag->agi_free_level, nullptr, nullptr);
        } else {
            // Update existing finobt record
            xfs_btree_update(&fi_cur, tp, rec);
        }
    }

    // 6. Update AGI counters
    pag->agi_freecount++;

    uint64_t ag_start_block = xfs_agbno_to_fsbno(agno, 0, mount->ag_blk_log);
    BufHead* agi_bh = xfs_buf_read(mount, ag_start_block);
    if (agi_bh != nullptr) {
        auto agi_offset = static_cast<size_t>(mount->sect_size * 2);
        auto* agi = reinterpret_cast<XfsAgi*>(agi_bh->data + agi_offset);
        agi->agi_freecount = __be32::from_cpu(pag->agi_freecount);
        // Recompute CRC
        agi->agi_crc = __be32{0};
        uint32_t crc = util::crc32c_block_with_cksum(agi, sizeof(XfsAgi), XFS_AGI_CRC_OFF);
        __builtin_memcpy(&agi->agi_crc, &crc, sizeof(crc));
        xfs_trans_log_buf(tp, agi_bh, static_cast<uint32_t>(agi_offset), static_cast<uint32_t>(sizeof(XfsAgi)));
        brelse(agi_bh);
    }

    mod::dbg::log("[xfs ifree] freed inode %lu\n", (unsigned long)ino);
    return 0;
}

}  // namespace ker::vfs::xfs
