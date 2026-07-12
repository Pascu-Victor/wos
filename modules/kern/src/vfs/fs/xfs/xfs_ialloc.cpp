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
#include <cstddef>
#include <cstdint>
#include <platform/dbg/dbg.hpp>
#include <util/crc32c.hpp>
#include <utility>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_alloc.hpp>
#include <vfs/fs/xfs/xfs_btree.hpp>

#include "net/endian.hpp"
#include "vfs/fs/xfs/xfs_format.hpp"
#include "vfs/fs/xfs/xfs_mount.hpp"
#include "vfs/fs/xfs/xfs_trans.hpp"

namespace ker::vfs::xfs {

namespace {

constexpr uint32_t XFS_INODES_PER_CHUNK = 64;
constexpr uint32_t XFS_INODES_PER_HOLEMASK_BIT = 4;
constexpr uint16_t XFS_INOBT_HOLEMASK_FULL = 0;
constexpr uint64_t XFS_INOBT_ALL_FREE = ~uint64_t{0};
constexpr uint32_t XFS_DINODE_NULL_UNLINKED = 0xFFFFFFFFU;

auto ialloc_ag(XfsMountContext* mount, XfsTransaction* tp, xfs_agnumber_t agno) -> xfs_ino_t;

auto xfs_has_sparse_inodes(const XfsMountContext* mount) -> bool {
    return mount != nullptr && (mount->feat_incompat & XFS_SB_FEAT_INCOMPAT_SPINODES) != 0;
}

auto inode_chunk_blocks(const XfsMountContext* mount) -> xfs_extlen_t {
    if (mount == nullptr || mount->inodes_per_block == 0) {
        return 0;
    }
    uint32_t const BLOCKS = (XFS_INODES_PER_CHUNK + mount->inodes_per_block - 1) / mount->inodes_per_block;
    return BLOCKS == 0 ? 1 : BLOCKS;
}

auto inobt_record_contains(const XfsInobtTraits::IRec& rec, xfs_agino_t agino) -> bool {
    if (agino < rec.startino) {
        return false;
    }
    xfs_agino_t const BIT = agino - rec.startino;
    if (BIT >= XFS_INODES_PER_CHUNK) {
        return false;
    }
    if (!rec.sparse_format) {
        return true;
    }
    uint32_t const HOLE_BIT = BIT / XFS_INODES_PER_HOLEMASK_BIT;
    return (rec.holemask & (static_cast<uint16_t>(1U) << HOLE_BIT)) == 0;
}

auto inobt_free_mask_for_bit(uint32_t bit) -> uint64_t {
    if (bit >= XFS_INODES_PER_CHUNK) {
        return 0;
    }
    return static_cast<uint64_t>(1) << bit;
}

void log_agi_state(XfsMountContext* mount, XfsTransaction* tp, xfs_agnumber_t agno, xfs_agino_t newino, bool update_newino) {
    XfsPerAG const* pag = &mount->per_ag[agno];
    uint64_t const AG_START_BLOCK = xfs_agbno_to_fsbno(agno, 0, mount->ag_blk_log);
    BufHead* agi_bh = xfs_buf_read(mount, AG_START_BLOCK);
    if (agi_bh == nullptr) {
        return;
    }

    auto agi_offset = static_cast<size_t>(mount->sect_size * 2);
    auto* agi = reinterpret_cast<XfsAgi*>(agi_bh->data + agi_offset);
    agi->agi_count = Be32::from_cpu(pag->agi_count);
    agi->agi_root = Be32::from_cpu(pag->agi_root);
    agi->agi_level = Be32::from_cpu(pag->agi_level);
    agi->agi_freecount = Be32::from_cpu(pag->agi_freecount);
    if (update_newino) {
        agi->agi_newino = Be32::from_cpu(newino);
    }
    agi->agi_free_root = Be32::from_cpu(pag->agi_free_root);
    agi->agi_free_level = Be32::from_cpu(pag->agi_free_level);
    agi->agi_crc = Be32{0};
    uint32_t crc = util::crc32c_block_with_cksum(agi, mount->sect_size, XFS_AGI_CRC_OFF);
    __builtin_memcpy(&agi->agi_crc, &crc, sizeof(crc));
    xfs_trans_log_buf(tp, agi_bh, static_cast<uint32_t>(agi_offset), static_cast<uint32_t>(sizeof(XfsAgi)));
    brelse(agi_bh);
}

void init_free_dinode(XfsMountContext* mount, XfsDinode* dip, xfs_ino_t ino) {
    __builtin_memset(dip, 0, mount->inode_size);
    dip->di_magic = Be16::from_cpu(XFS_DINODE_MAGIC);
    dip->di_version = 3;
    dip->di_format = static_cast<uint8_t>(XFS_DINODE_FMT_EXTENTS);
    dip->di_next_unlinked = Be32::from_cpu(XFS_DINODE_NULL_UNLINKED);
    dip->di_ino = Be64::from_cpu(ino);
    __builtin_memcpy(&dip->di_uuid, &mount->uuid, sizeof(XfsUuidT));

    dip->di_crc = 0;
    uint32_t const CRC = util::crc32c_block_with_cksum(dip, mount->inode_size, XFS_DINODE_CRC_OFF);
    dip->di_crc = CRC;
}

auto init_inode_chunk(XfsMountContext* mount, XfsTransaction* tp, xfs_agnumber_t agno, xfs_agino_t startino) -> int {
    uint32_t initialized = 0;
    while (initialized < XFS_INODES_PER_CHUNK) {
        xfs_agino_t const AGINO = startino + initialized;
        xfs_agblock_t const AGBNO = AGINO / mount->inodes_per_block;
        xfs_fsblock_t const FSBNO = xfs_agbno_to_fsbno(agno, AGBNO, mount->ag_blk_log);

        BufHead* bh = xfs_buf_get(mount, FSBNO);
        if (bh == nullptr) {
            return -EIO;
        }
        __builtin_memset(bh->data, 0, bh->size);

        while (initialized < XFS_INODES_PER_CHUNK && ((startino + initialized) / mount->inodes_per_block) == AGBNO) {
            xfs_agino_t const CUR_AGINO = startino + initialized;
            size_t const OFFSET = static_cast<size_t>(CUR_AGINO % mount->inodes_per_block) * mount->inode_size;
            if (OFFSET + mount->inode_size > bh->size) {
                brelse(bh);
                return -EIO;
            }

            auto* dip = reinterpret_cast<XfsDinode*>(bh->data + OFFSET);
            xfs_ino_t const INO = (static_cast<xfs_ino_t>(agno) << mount->agino_log) | CUR_AGINO;
            init_free_dinode(mount, dip, INO);
            initialized++;
        }

        xfs_trans_log_buf_full(tp, bh);
        brelse(bh);
    }

    return 0;
}

auto allocate_inode_chunk(XfsMountContext* mount, XfsTransaction* tp) -> xfs_ino_t {
    xfs_extlen_t const CHUNK_BLOCKS = inode_chunk_blocks(mount);
    if (CHUNK_BLOCKS == 0) {
        return NULLFSINO;
    }

    XfsAllocReq req{};
    req.agno = NULLAGNUMBER;
    req.minlen = CHUNK_BLOCKS;
    req.maxlen = CHUNK_BLOCKS;
    req.alignment = CHUNK_BLOCKS;

    XfsAllocResult result{};
    int rc = xfs_alloc_extent(mount, tp, req, &result);
    if (rc != 0 || result.len != CHUNK_BLOCKS || result.agno >= mount->ag_count) {
        return NULLFSINO;
    }

    xfs_agino_t const STARTINO = result.agbno * mount->inodes_per_block;
    if ((STARTINO & (XFS_INODES_PER_CHUNK - 1)) != 0) {
        mod::dbg::logger<"xfs">::error("xfs_ialloc: allocated unaligned inode chunk ag=%u agbno=%u startino=%u", result.agno, result.agbno,
                                       STARTINO);
        return NULLFSINO;
    }

    rc = init_inode_chunk(mount, tp, result.agno, STARTINO);
    if (rc != 0) {
        return NULLFSINO;
    }

    XfsPerAG* pag = &mount->per_ag[result.agno];
    XfsInobtTraits::IRec rec{};
    rec.startino = STARTINO;
    rec.holemask = XFS_INOBT_HOLEMASK_FULL;
    rec.count = XFS_INODES_PER_CHUNK;
    rec.freecount = XFS_INODES_PER_CHUNK;
    rec.free_mask = XFS_INOBT_ALL_FREE;
    rec.sparse_format = xfs_has_sparse_inodes(mount);

    XfsBtreeCursor<XfsInobtTraits> ino_cur;
    ino_cur.mount = mount;
    ino_cur.agno = result.agno;
    uint64_t new_ino_root = pag->agi_root;
    auto new_ino_level = static_cast<uint8_t>(pag->agi_level);
    rc = xfs_btree_insert(&ino_cur, tp, rec, pag->agi_root, pag->agi_level, &new_ino_root, &new_ino_level);
    if (rc != 0) {
        return NULLFSINO;
    }
    pag->agi_root = static_cast<xfs_agblock_t>(new_ino_root);
    pag->agi_level = new_ino_level;

    if (xfs_has_finobt(mount) && pag->agi_free_root != 0) {
        XfsBtreeCursor<XfsFinobtTraits> fi_cur;
        fi_cur.mount = mount;
        fi_cur.agno = result.agno;
        uint64_t new_fi_root = pag->agi_free_root;
        auto new_fi_level = static_cast<uint8_t>(pag->agi_free_level);
        rc = xfs_btree_insert(&fi_cur, tp, rec, pag->agi_free_root, pag->agi_free_level, &new_fi_root, &new_fi_level);
        if (rc != 0) {
            return NULLFSINO;
        }
        pag->agi_free_root = static_cast<xfs_agblock_t>(new_fi_root);
        pag->agi_free_level = new_fi_level;
    }

    pag->agi_count += XFS_INODES_PER_CHUNK;
    pag->agi_freecount += XFS_INODES_PER_CHUNK;
    log_agi_state(mount, tp, result.agno, STARTINO, true);

    return ialloc_ag(mount, tp, result.agno);
}

auto first_free_inode_bit(uint64_t free_mask) -> int {
    if (free_mask == 0) {
        return -1;
    }
    return __builtin_ctzll(free_mask);
}

// Try to allocate an inode from a specific AG
auto ialloc_ag(XfsMountContext* mount, XfsTransaction* tp, xfs_agnumber_t agno) -> xfs_ino_t {
    XfsPerAG* pag = &mount->per_ag[agno];

    if (pag->agi_freecount == 0) {
        return NULLFSINO;
    }

    bool const TRY_HINT = pag->ialloc_hint_valid;
    for (int pass = 0; pass < (TRY_HINT ? 2 : 1); ++pass) {
        xfs_agino_t const SEARCH_START = (pass == 0 && TRY_HINT) ? pag->ialloc_hint_startino : 0;

        // Always search the inobt directly.  Both inobt and finobt records
        // carry freecount/free_mask, but the finobt uses a different on-disk
        // magic number (FIB3 vs IAB3).  Using XfsBtreeCursor<XfsInobtTraits>
        // on finobt blocks causes a magic-validation failure.  Scanning the
        // inobt is slightly slower but correct; the update path below still
        // maintains both trees when finobt is present.
        XfsBtreeCursor<XfsInobtTraits> cur;
        cur.mount = mount;
        cur.agno = agno;

        XfsInobtTraits::IRec target{};
        target.startino = SEARCH_START;

        int rc = xfs_btree_lookup(&cur, pag->agi_root, pag->agi_level, target, XfsBtreeLookup::GE);
        if (rc != 0) {
            if (SEARCH_START != 0) {
                continue;
            }
            return NULLFSINO;
        }

        // Scan forward to find a record with free inodes
        while (true) {
            XfsInobtTraits::IRec rec = xfs_btree_get_rec(&cur);

            if (rec.freecount > 0 && rec.free_mask != 0) {
                // Found a chunk with free inodes
                int const BIT = first_free_inode_bit(rec.free_mask);
                if (BIT >= 0 && std::cmp_less(BIT, XFS_INODES_PER_CHUNK)) {
                    xfs_agino_t const AGINO = rec.startino + static_cast<xfs_agino_t>(BIT);
                    xfs_ino_t const INO = (static_cast<xfs_ino_t>(agno) << mount->agino_log) | AGINO;

                    // 1. Clear the bit in ir_free and decrement ir_freecount
                    rec.free_mask &= ~inobt_free_mask_for_bit(static_cast<uint32_t>(BIT));
                    rec.freecount--;
                    pag->ialloc_hint_valid = true;
                    pag->ialloc_hint_startino =
                        rec.freecount != 0 ? rec.startino : rec.startino + static_cast<xfs_agino_t>(XFS_INODES_PER_CHUNK);

                    // 2-3. The inobt cursor already identifies this exact
                    // record; update it directly instead of traversing the
                    // same tree a second time.
                    int urc = xfs_btree_update(&cur, tp, rec);
                    if (urc != 0) {
                        return NULLFSINO;
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
                                // No more free inodes in this chunk - remove from finobt
                                xfs_btree_delete(&fi_cur, tp);
                            } else {
                                xfs_btree_update(&fi_cur, tp, rec);
                            }
                        }
                    }

                    // 5. Update AGI counters
                    pag->agi_freecount--;
                    log_agi_state(mount, tp, agno, AGINO, true);

                    // The create/mkdir/symlink caller logs the initialized
                    // inode in this same transaction before the inode becomes
                    // reachable from any committed directory entry. Avoid
                    // writing a temporary free dinode core that would be
                    // overwritten during transaction commit.
#ifdef XFS_DEBUG
                    mod::dbg::log("[xfs ialloc] allocated inode %lu (AG %u agino %u)\n", static_cast<unsigned long>(ino), agno, agino);
#endif
                    return INO;
                }
            }

            rc = xfs_btree_increment(&cur);
            if (rc != 0) {
                break;
            }
        }

        if (SEARCH_START == 0) {
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
        xfs_ino_t const INO = ialloc_ag(mount, tp, ag);
        if (INO != NULLFSINO) {
            return INO;
        }
    }

    xfs_ino_t const GROWN_INO = allocate_inode_chunk(mount, tp);
    if (GROWN_INO != NULLFSINO) {
        return GROWN_INO;
    }

    mod::dbg::logger<"xfs">::error("xfs_ialloc: no free inodes available in any AG");
    return NULLFSINO;
}

auto xfs_inode_allocated(XfsMountContext* mount, xfs_ino_t ino) -> int {
    if (mount == nullptr || ino == NULLFSINO) {
        return -EINVAL;
    }

    auto const AGNO = static_cast<xfs_agnumber_t>(ino >> mount->agino_log);
    auto const AGINO = static_cast<xfs_agino_t>(ino & ((1ULL << mount->agino_log) - 1));

    if (AGNO >= mount->ag_count) {
        return -EINVAL;
    }

    XfsPerAG* pag = &mount->per_ag[AGNO];

    XfsBtreeCursor<XfsInobtTraits> cur;
    cur.mount = mount;
    cur.agno = AGNO;

    XfsInobtTraits::IRec target{};
    target.startino = AGINO;

    int const RC = xfs_btree_lookup(&cur, pag->agi_root, pag->agi_level, target, XfsBtreeLookup::LE);
    if (RC != 0) {
        if (RC == -ENOENT) {
            return 0;
        }
        return -EIO;
    }

    XfsInobtTraits::IRec const REC = xfs_btree_get_rec(&cur);
    if (!inobt_record_contains(REC, AGINO)) {
        return 0;
    }

    uint32_t const BIT = AGINO - REC.startino;
    uint64_t const INODE_BIT = inobt_free_mask_for_bit(BIT);
    return (REC.free_mask & INODE_BIT) == 0 ? 1 : 0;
}

auto xfs_ifree(XfsMountContext* mount, XfsTransaction* tp, xfs_ino_t ino) -> int {
    if (mount == nullptr || ino == NULLFSINO) {
        return -EINVAL;
    }

    // Decompose inode number into AG + agino
    auto const AGNO = static_cast<xfs_agnumber_t>(ino >> mount->agino_log);
    auto const AGINO = static_cast<xfs_agino_t>(ino & ((1ULL << mount->agino_log) - 1));

    if (AGNO >= mount->ag_count) {
        return -EINVAL;
    }

    XfsPerAG* pag = &mount->per_ag[AGNO];

    // Compute the chunk start inode (aligned to 64-inode boundary)
    xfs_agino_t const CHUNK_START = AGINO & ~static_cast<xfs_agino_t>(XFS_INODES_PER_CHUNK - 1);
    uint32_t const BIT = AGINO - CHUNK_START;

    // 1. Find the inobt record for this inode's chunk
    XfsBtreeCursor<XfsInobtTraits> cur;
    cur.mount = mount;
    cur.agno = AGNO;

    XfsInobtTraits::IRec target{};
    target.startino = CHUNK_START;

    int rc = xfs_btree_lookup(&cur, pag->agi_root, pag->agi_level, target, XfsBtreeLookup::GE);
    if (rc != 0) {
        return -EIO;
    }

    XfsInobtTraits::IRec rec = xfs_btree_get_rec(&cur);
    if (rec.startino != CHUNK_START) {
        mod::dbg::log("[xfs ifree] inobt record mismatch: expected startino %u, got %u\n", CHUNK_START, rec.startino);
        return -EIO;
    }

    uint64_t const INODE_BIT = inobt_free_mask_for_bit(BIT);
    if ((rec.free_mask & INODE_BIT) != 0) {
        mod::dbg::logger<"xfs">::warn("xfs_ifree: inode %lu already free (ag=%u agino=%u chunk_start=%u freecount=%u)",
                                      static_cast<unsigned long>(ino), AGNO, AGINO, CHUNK_START, rec.freecount);
        return -EEXIST;
    }

    // 2-3. Set the free bit and increment freecount
    bool const WAS_FULLY_ALLOCATED = (rec.freecount == 0);
    rec.free_mask |= INODE_BIT;
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
        fi_cur.agno = AGNO;

        XfsFinobtTraits::IRec fi_target{};
        fi_target.startino = CHUNK_START;

        int const FRC = xfs_btree_lookup(&fi_cur, pag->agi_free_root, pag->agi_free_level, fi_target, XfsBtreeLookup::GE);

        if (WAS_FULLY_ALLOCATED || FRC != 0) {
            // Chunk wasn't in finobt - insert it
            xfs_btree_insert(&fi_cur, tp, rec, pag->agi_free_root, pag->agi_free_level, nullptr, nullptr);
        } else {
            // Update existing finobt record
            xfs_btree_update(&fi_cur, tp, rec);
        }
    }

    // 6. Update AGI counters
    pag->agi_freecount++;
    log_agi_state(mount, tp, AGNO, 0, false);
#ifdef XFS_DEBUG
    mod::dbg::logger<"xfs">::debug("xfs_ifree: freed inode %lu", static_cast<unsigned long>(ino));
#endif
    return 0;
}

}  // namespace ker::vfs::xfs
