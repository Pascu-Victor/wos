// XFS Block Map (bmap) implementation.
//
// Maps logical file block offsets to physical disk blocks.  Supports all three
// data fork formats: inline (LOCAL), extent list (EXTENTS), and B+tree (BTREE).
//
// For EXTENTS format: binary search through the decoded extent array stored
// in the inode fork.
//
// For BTREE format: use the generic B+tree cursor to traverse the bmbt.
// The root of the bmbt is embedded in the inode fork as a bmdr_block
// (compact format without sibling/CRC fields).  The first-level child blocks
// are full xfs_btree_lblock nodes.
//
// Reference: reference/xfs/libxfs/xfs_bmap.c

#include "xfs_bmap.hpp"

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <platform/dbg/dbg.hpp>
#include <span>
#include <utility>
#include <vfs/fs/xfs/xfs_btree.hpp>
#include <vfs/fs/xfs/xfs_trans.hpp>

#include "net/endian.hpp"
#include "vfs/fs/xfs/xfs_format.hpp"
#include "vfs/fs/xfs/xfs_inode.hpp"
#include "vfs/fs/xfs/xfs_mount.hpp"

namespace ker::vfs::xfs {

// ============================================================================
// EXTENTS format - binary search in decoded extent list
// ============================================================================

namespace {

auto bmap_lookup_extents(XfsInode* ip, xfs_fileoff_t file_block, XfsBmapResult* result) -> int {
    const XfsIforkExtents& ext = ip->data_fork.extents;

    if (ext.count == 0) {
        result->is_hole = true;
        result->startblock = NULLFSBLOCK;
        // No extents at all - the entire address space is a hole.  Return a
        // large blockcount so the caller can allocate in large batches.
        result->blockcount = ~static_cast<xfs_filblks_t>(0);
        result->unwritten = false;
        return 0;
    }

    // Binary search for the extent containing file_block
    int lo = 0;
    int hi = static_cast<int>(ext.count) - 1;
    int found = -1;

    while (lo <= hi) {
        int const MID = (lo + hi) / 2;
        const XfsBmbtIrec& e = ext.list[MID];

        if (file_block < e.br_startoff) {
            hi = MID - 1;
        } else if (file_block >= e.br_startoff + e.br_blockcount) {
            lo = MID + 1;
        } else {
            // file_block is within this extent
            found = MID;
            break;
        }
    }

    if (found >= 0) {
        const XfsBmbtIrec& e = ext.list[found];
        xfs_filblks_t const OFFSET_IN_EXTENT = file_block - e.br_startoff;
        result->startblock = e.br_startblock + OFFSET_IN_EXTENT;
        result->blockcount = e.br_blockcount - OFFSET_IN_EXTENT;
        result->unwritten = e.br_unwritten;
        result->is_hole = false;
    } else {
        // file_block falls in a hole between extents
        result->is_hole = true;
        result->startblock = NULLFSBLOCK;
        result->unwritten = false;

        // Compute the size of the hole (distance to next extent)
        // lo now points to the first extent starting after file_block
        if (std::cmp_less(lo, ext.count)) {
            result->blockcount = ext.list[lo].br_startoff - file_block;
        } else {
            // Past the last extent - unbounded hole to EOF.  Return a large
            // blockcount so callers can allocate in large batches.
            result->blockcount = ~static_cast<xfs_filblks_t>(0);
        }
    }

    return 0;
}

// ============================================================================
// BTREE format - use bmbt cursor
// ============================================================================

auto bmap_lookup_btree(XfsInode* ip, xfs_fileoff_t file_block, XfsBmapResult* result) -> int {
    XfsMountContext* mount = ip->mount;
    const XfsIforkBtree& bt = ip->data_fork.btree;

    if (bt.root == nullptr || bt.root_size < sizeof(XfsBmdrBlock)) {
        return -EINVAL;
    }

    // The bmdr_block is the compact root.  We need to do the first-level
    // lookup manually (keys + long-form pointers within the inode fork data),
    // then hand off to the btree cursor for sub-trees.
    const auto* bmdr = reinterpret_cast<const XfsBmdrBlock*>(bt.root);
    uint16_t const LEVEL = bmdr->bb_level.to_cpu();
    uint16_t const NUMRECS = bmdr->bb_numrecs.to_cpu();

    if (NUMRECS == 0) {
        result->is_hole = true;
        result->startblock = NULLFSBLOCK;
        result->blockcount = ~static_cast<xfs_filblks_t>(0);
        result->unwritten = false;
        return 0;
    }

    // Keys start after the bmdr_block header (4 bytes)
    const uint8_t* keys_base = bt.root + sizeof(XfsBmdrBlock);
    // Pointers start after all keys
    const uint8_t* ptrs_base = keys_base + (static_cast<size_t>(NUMRECS) * sizeof(XfsBmbtKey));

    // Binary search within the root keys to find the right child pointer
    int lo = 0;
    int hi = NUMRECS - 1;
    int keyno = 0;  // 0-based for root node

    while (lo <= hi) {
        int const MID = (lo + hi) / 2;
        const auto* key = reinterpret_cast<const XfsBmbtKey*>(keys_base + (static_cast<size_t>(MID) * sizeof(XfsBmbtKey)));
        uint64_t const STARTOFF = key->br_startoff.to_cpu();

        if (STARTOFF <= file_block) {
            keyno = MID;
            lo = MID + 1;
        } else {
            hi = MID - 1;
        }
    }

    // Get the child pointer (long-form: 8 bytes, absolute fsblock)
    Be64 ptr_val{};
    __builtin_memcpy(&ptr_val, ptrs_base + (static_cast<size_t>(keyno) * sizeof(Be64)), sizeof(Be64));
    uint64_t const CHILD_BLOCK = ptr_val.to_cpu();

    if (LEVEL == 1) {
        // Child is a leaf - we can read it directly and do the lookup
        // Set up a btree cursor positioned at the child block
        XfsBtreeCursor<XfsBmbtTraits> cur;
        cur.mount = mount;
        cur.nlevels = 1;

        int const RC = cur.read_block(0, CHILD_BLOCK);
        if (RC != 0) {
            return RC;
        }

        // Binary search within the leaf for file_block
        int const NR = cur.numrecs(0);
        int found_idx = -1;

        lo = 1;
        hi = NR;
        while (lo <= hi) {
            int const MID = (lo + hi) / 2;
            const auto* rec = cur.rec_at(MID);
            XfsBmbtIrec const IREC = xfs_bmbt_rec_unpack(rec);

            if (file_block < IREC.br_startoff) {
                hi = MID - 1;
            } else if (file_block >= IREC.br_startoff + IREC.br_blockcount) {
                lo = MID + 1;
            } else {
                found_idx = MID;
                break;
            }
        }

        if (found_idx >= 0) {
            const auto* rec = cur.rec_at(found_idx);
            XfsBmbtIrec const IREC = xfs_bmbt_rec_unpack(rec);
            xfs_filblks_t const OFFSET_IN = file_block - IREC.br_startoff;
            result->startblock = IREC.br_startblock + OFFSET_IN;
            result->blockcount = IREC.br_blockcount - OFFSET_IN;
            result->unwritten = IREC.br_unwritten;
            result->is_hole = false;
        } else {
            result->is_hole = true;
            result->startblock = NULLFSBLOCK;
            result->blockcount = 1;
            result->unwritten = false;
        }
        return 0;
    }

    // Multi-level btree: use the full cursor mechanism
    // We start from the child block at (level - 1) and descend
    XfsBtreeCursor<XfsBmbtTraits> cur;
    cur.mount = mount;
    cur.nlevels = LEVEL;  // depth from this child down

    // Target for lookup
    XfsBmbtIrec target{};
    target.br_startoff = file_block;

    int rc = xfs_btree_lookup(&cur, CHILD_BLOCK, LEVEL, target, XfsBtreeLookup::LE);
    if (rc == -ENOENT) {
        result->is_hole = true;
        result->startblock = NULLFSBLOCK;
        result->blockcount = 1;
        result->unwritten = false;
        return 0;
    }
    if (rc != 0) {
        return rc;
    }

    XfsBmbtIrec const IREC = xfs_btree_get_rec(&cur);

    // Check if file_block falls within this extent
    if (file_block >= IREC.br_startoff && file_block < IREC.br_startoff + IREC.br_blockcount) {
        xfs_filblks_t const OFFSET_IN = file_block - IREC.br_startoff;
        result->startblock = IREC.br_startblock + OFFSET_IN;
        result->blockcount = IREC.br_blockcount - OFFSET_IN;
        result->unwritten = IREC.br_unwritten;
        result->is_hole = false;
    } else {
        result->is_hole = true;
        result->startblock = NULLFSBLOCK;
        result->unwritten = false;
        // Distance to next extent
        rc = xfs_btree_increment(&cur);
        if (rc == 0) {
            XfsBmbtIrec const NEXT = xfs_btree_get_rec(&cur);
            result->blockcount = NEXT.br_startoff - file_block;
        } else {
            result->blockcount = 1;
        }
    }

    return 0;
}

}  // anonymous namespace

// ============================================================================
// Public API
// ============================================================================

auto xfs_bmap_lookup(XfsInode* ip, xfs_fileoff_t file_block, XfsBmapResult* result) -> int {
    if (ip == nullptr || result == nullptr) {
        return -EINVAL;
    }

    switch (ip->data_fork.format) {
        case XFS_DINODE_FMT_LOCAL:
            // Inline data - no block mapping.  The caller should read data
            // directly from the inode fork.
            result->is_hole = false;
            result->startblock = NULLFSBLOCK;
            result->blockcount = 0;
            result->unwritten = false;
            return 0;

        case XFS_DINODE_FMT_EXTENTS:
            return bmap_lookup_extents(ip, file_block, result);

        case XFS_DINODE_FMT_BTREE:
            return bmap_lookup_btree(ip, file_block, result);

        default:
            mod::dbg::log("[xfs] bmap: unsupported fork format %d for inode %lu", ip->data_fork.format,
                          static_cast<unsigned long>(ip->ino));
            return -EINVAL;
    }
}

auto xfs_bmap_list_extents(XfsInode* ip, XfsBmbtIrec* extents, uint32_t max_extents) -> int {
    if (ip == nullptr || extents == nullptr || max_extents == 0) {
        return -EINVAL;
    }

    if (ip->data_fork.format == XFS_DINODE_FMT_EXTENTS) {
        // Direct copy from the decoded extent list
        uint32_t count = ip->data_fork.extents.count;
        count = std::min(count, max_extents);
        for (uint32_t i = 0; i < count; i++) {
            extents[i] = ip->data_fork.extents.list[i];
        }
        return static_cast<int>(count);
    }

    if (ip->data_fork.format == XFS_DINODE_FMT_BTREE) {
        // Walk the btree from leftmost leaf to rightmost, collecting records
        const XfsIforkBtree& bt = ip->data_fork.btree;
        if (bt.root == nullptr) {
            return 0;
        }

        const auto* bmdr = reinterpret_cast<const XfsBmdrBlock*>(bt.root);
        uint16_t const LEVEL = bmdr->bb_level.to_cpu();
        uint16_t const NUMRECS = bmdr->bb_numrecs.to_cpu();
        if (NUMRECS == 0) {
            return 0;
        }

        // Get the leftmost child pointer (first key, first ptr)
        const uint8_t* ptrs_base = bt.root + sizeof(XfsBmdrBlock) + (static_cast<size_t>(NUMRECS) * sizeof(XfsBmbtKey));
        Be64 ptr_val{};
        __builtin_memcpy(&ptr_val, ptrs_base, sizeof(Be64));
        uint64_t const CHILD_BLOCK = ptr_val.to_cpu();

        // Use a cursor positioned at the very beginning
        XfsBtreeCursor<XfsBmbtTraits> cur;
        cur.mount = ip->mount;

        XfsBmbtIrec target{};
        target.br_startoff = 0;  // start from the very beginning

        int rc = xfs_btree_lookup(&cur, CHILD_BLOCK, LEVEL, target, XfsBtreeLookup::GE);
        if (rc == -ENOENT) {
            return 0;
        }
        if (rc != 0) {
            return rc;
        }

        uint32_t count = 0;
        while (count < max_extents) {
            extents[count] = xfs_btree_get_rec(&cur);
            count++;
            rc = xfs_btree_increment(&cur);
            if (rc != 0) {
                break;  // no more records
            }
        }

        return static_cast<int>(count);
    }

    if (ip->data_fork.format == XFS_DINODE_FMT_LOCAL) {
        return 0;  // inline - no extents
    }

    return -EINVAL;
}

// ============================================================================
// Add extent - insert a new extent mapping into the inode's data fork
// ============================================================================

auto xfs_bmap_add_extent(XfsInode* ip, XfsTransaction* tp, const XfsBmbtIrec& new_ext) -> int {
    if (ip == nullptr || tp == nullptr) {
        return -EINVAL;
    }

    // Only support EXTENTS format for now (most common for small/medium files).
    // An empty file (LOCAL format with 0 extents) can be promoted to EXTENTS.
    if (ip->data_fork.format == XFS_DINODE_FMT_LOCAL) {
        // Promote to EXTENTS format - free inline data if any
        if (ip->data_fork.local.data != nullptr) {
            delete[] ip->data_fork.local.data;
            ip->data_fork.local.data = nullptr;
            ip->data_fork.local.size = 0;
        }
        ip->data_fork.format = XFS_DINODE_FMT_EXTENTS;
        ip->data_fork.extents.list = nullptr;
        ip->data_fork.extents.count = 0;
    }

    if (ip->data_fork.format != XFS_DINODE_FMT_EXTENTS) {
        mod::dbg::log("[xfs] bmap_add_extent: unsupported fork format %d for inode %lu", ip->data_fork.format,
                      static_cast<unsigned long>(ip->ino));
        return -EOPNOTSUPP;
    }

    XfsIforkExtents& ext = ip->data_fork.extents;

    // Find insertion point (keep sorted by br_startoff)
    uint32_t insert_at = 0;
    for (uint32_t i = 0; i < ext.count; i++) {
        if (ext.list[i].br_startoff >= new_ext.br_startoff) {
            break;
        }
        insert_at = i + 1;
    }

    // Try to merge with the previous extent
    if (insert_at > 0) {
        XfsBmbtIrec& prev = ext.list[insert_at - 1];
        if (prev.br_startoff + prev.br_blockcount == new_ext.br_startoff &&
            prev.br_startblock + prev.br_blockcount == new_ext.br_startblock && prev.br_unwritten == new_ext.br_unwritten) {
            prev.br_blockcount += new_ext.br_blockcount;
            ip->nextents = ext.count;
            ip->dirty = true;
            xfs_trans_log_inode(tp, ip);
            return 0;
        }
    }

    // Try to merge with the next extent
    if (insert_at < ext.count) {
        XfsBmbtIrec& next = ext.list[insert_at];
        if (new_ext.br_startoff + new_ext.br_blockcount == next.br_startoff &&
            new_ext.br_startblock + new_ext.br_blockcount == next.br_startblock && new_ext.br_unwritten == next.br_unwritten) {
            next.br_startoff = new_ext.br_startoff;
            next.br_startblock = new_ext.br_startblock;
            next.br_blockcount += new_ext.br_blockcount;
            ip->nextents = ext.count;
            ip->dirty = true;
            xfs_trans_log_inode(tp, ip);
            return 0;
        }
    }

    // No merge - insert new extent.
    // Grow the list with capacity doubling to avoid O(N²) reallocations.
    uint32_t const NEW_COUNT = ext.count + 1;
    if (NEW_COUNT > ext.capacity) {
        uint32_t new_cap = ext.capacity == 0 ? 4 : ext.capacity * 2;
        new_cap = std::max(new_cap, NEW_COUNT);
        auto* new_list = new XfsBmbtIrec[new_cap];
        if (new_list == nullptr) {
            return -ENOMEM;
        }
        for (uint32_t i = 0; i < ext.count; i++) {
            new_list[i] = ext.list[i];
        }
        delete[] ext.list;
        ext.list = new_list;
        ext.capacity = new_cap;
    }

    // Shift entries after insertion point right by one
    if (ext.list == nullptr || ext.count >= ext.capacity) {
        return -EIO;
    }
    std::span<XfsBmbtIrec> const EXTENTS{ext.list, ext.capacity};
    std::move_backward(EXTENTS.begin() + insert_at, EXTENTS.begin() + ext.count, EXTENTS.begin() + NEW_COUNT);
    *(EXTENTS.begin() + insert_at) = new_ext;
    ext.count = NEW_COUNT;
    ip->nextents = NEW_COUNT;
    ip->dirty = true;
    xfs_trans_log_inode(tp, ip);

    return 0;
}

}  // namespace ker::vfs::xfs
