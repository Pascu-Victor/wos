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
#include <new>
#include <platform/dbg/dbg.hpp>
#include <util/crc32c.hpp>
#include <utility>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_alloc.hpp>
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

auto data_fork_record_capacity(const XfsInode* ip) -> uint32_t {
    if (ip == nullptr || ip->mount == nullptr || ip->mount->inode_size <= XFS_DINODE_SIZE_V3) {
        return 0;
    }

    size_t fork_size = ip->mount->inode_size - XFS_DINODE_SIZE_V3;
    if (ip->forkoff != 0) {
        fork_size = static_cast<size_t>(ip->forkoff) << 3U;
        if (XFS_DINODE_SIZE_V3 + fork_size > ip->mount->inode_size) {
            return 0;
        }
    }

    size_t const CAPACITY = fork_size / sizeof(XfsBmbtRec);
    return CAPACITY > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(CAPACITY);
}

auto data_fork_bmdr_capacity(const XfsInode* ip) -> uint32_t {
    if (ip == nullptr || ip->mount == nullptr || ip->mount->inode_size <= XFS_DINODE_SIZE_V3) {
        return 0;
    }

    size_t fork_size = ip->mount->inode_size - XFS_DINODE_SIZE_V3;
    if (ip->forkoff != 0) {
        fork_size = static_cast<size_t>(ip->forkoff) << 3U;
        if (XFS_DINODE_SIZE_V3 + fork_size > ip->mount->inode_size) {
            return 0;
        }
    }

    if (fork_size < sizeof(XfsBmdrBlock)) {
        return 0;
    }

    size_t const CAPACITY = (fork_size - sizeof(XfsBmdrBlock)) / (sizeof(XfsBmbtKey) + sizeof(Be64));
    return CAPACITY > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(CAPACITY);
}

auto bmbt_leaf_max_recs(const XfsMountContext* mount) -> uint32_t {
    if (mount == nullptr || mount->block_size <= XFS_BTREE_LBLOCK_CRC_LEN) {
        return 0;
    }
    size_t const CAPACITY = (mount->block_size - XFS_BTREE_LBLOCK_CRC_LEN) / sizeof(XfsBmbtRec);
    return CAPACITY > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(CAPACITY);
}

auto bmbt_node_max_keys(const XfsMountContext* mount) -> uint32_t {
    if (mount == nullptr || mount->block_size <= XFS_BTREE_LBLOCK_CRC_LEN) {
        return 0;
    }
    size_t const CAPACITY = (mount->block_size - XFS_BTREE_LBLOCK_CRC_LEN) / (sizeof(XfsBmbtKey) + sizeof(Be64));
    return CAPACITY > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(CAPACITY);
}

auto bmbt_node_ptr_offset(const XfsMountContext* mount, size_t idx) -> size_t {
    return XFS_BTREE_LBLOCK_CRC_LEN + (static_cast<size_t>(bmbt_node_max_keys(mount)) * sizeof(XfsBmbtKey)) + (idx * sizeof(Be64));
}

void bmbt_update_crc(BufHead* bp) {
    auto* hdr = reinterpret_cast<XfsBtreeLblock*>(bp->data);
    hdr->bb_crc = 0;
    uint32_t crc = util::crc32c_block_with_cksum(bp->data, bp->size, offsetof(XfsBtreeLblock, bb_crc));
    __builtin_memcpy(&hdr->bb_crc, &crc, sizeof(crc));
}

auto bmbt_alloc_block(XfsInode* ip, XfsTransaction* tp, uint16_t level, BufHead** out_bh, xfs_fsblock_t* out_fsb) -> int {
    if (ip == nullptr || ip->mount == nullptr || tp == nullptr || out_bh == nullptr || out_fsb == nullptr) {
        return -EINVAL;
    }
    *out_bh = nullptr;
    *out_fsb = NULLFSBLOCK;

    xfs_agblock_t agbno = NULLAGBLOCK;
    int const RC = xfs_alloc_get_freelist(ip->mount, tp, ip->agno, &agbno);
    if (RC != 0) {
        return RC;
    }
    if (agbno == NULLAGBLOCK || agbno >= ip->mount->ag_blocks) {
        return -EIO;
    }

    xfs_fsblock_t const FSB = xfs_agbno_to_fsbno(ip->agno, agbno, ip->mount->ag_blk_log);
    BufHead* bh = xfs_buf_read(ip->mount, FSB);
    if (bh == nullptr) {
        static_cast<void>(xfs_alloc_put_freelist(ip->mount, tp, ip->agno, agbno));
        return -EIO;
    }

    __builtin_memset(bh->data, 0, bh->size);
    auto* hdr = reinterpret_cast<XfsBtreeLblock*>(bh->data);
    hdr->bb_magic = Be32::from_cpu(XFS_BMAP_CRC_MAGIC);
    hdr->bb_level = Be16::from_cpu(level);
    hdr->bb_numrecs = Be16::from_cpu(0);
    hdr->bb_leftsib = Be64::from_cpu(NULLFSBLOCK);
    hdr->bb_rightsib = Be64::from_cpu(NULLFSBLOCK);
    hdr->bb_blkno = Be64::from_cpu(FSB * (ip->mount->block_size / ip->mount->sect_size));
    hdr->bb_owner = Be64::from_cpu(ip->ino);
    hdr->bb_uuid = ip->mount->uuid;
    bmbt_update_crc(bh);

    *out_bh = bh;
    *out_fsb = FSB;
    return 0;
}

auto bmbt_return_block(XfsMountContext* mount, XfsTransaction* tp, xfs_fsblock_t fsb) -> int {
    if (mount == nullptr || fsb == NULLFSBLOCK) {
        return -EINVAL;
    }
    xfs_agnumber_t const AGNO = xfs_ag_number(fsb, mount->ag_blk_log);
    xfs_agblock_t const AGBNO = xfs_ag_block(fsb, mount->ag_blk_log);
    if (AGNO >= mount->ag_count || AGBNO >= mount->ag_blocks) {
        return -EIO;
    }
    return xfs_alloc_put_freelist(mount, tp, AGNO, AGBNO);
}

auto extent_can_merge_left(const XfsBmbtIrec& left, const XfsBmbtIrec& right) -> bool {
    return left.br_startoff + left.br_blockcount == right.br_startoff && left.br_startblock + left.br_blockcount == right.br_startblock &&
           left.br_unwritten == right.br_unwritten;
}

auto insert_or_merge_extent(const XfsBmbtIrec* old_list, uint32_t old_count, const XfsBmbtIrec& new_ext, XfsBmbtIrec* out_list,
                            uint32_t* out_count) -> int {
    if (out_list == nullptr || out_count == nullptr || (old_count != 0 && old_list == nullptr)) {
        return -EINVAL;
    }

    uint32_t insert_at = 0;
    while (insert_at < old_count && old_list[insert_at].br_startoff < new_ext.br_startoff) {
        out_list[insert_at] = old_list[insert_at];
        insert_at++;
    }

    XfsBmbtIrec pending = new_ext;
    uint32_t written = insert_at;
    if (written > 0 && extent_can_merge_left(out_list[written - 1], pending)) {
        out_list[written - 1].br_blockcount += pending.br_blockcount;
    } else {
        out_list[written++] = pending;
    }

    for (uint32_t i = insert_at; i < old_count; i++) {
        XfsBmbtIrec rec = old_list[i];
        if (written > 0 && extent_can_merge_left(out_list[written - 1], rec)) {
            out_list[written - 1].br_blockcount += rec.br_blockcount;
            continue;
        }
        out_list[written++] = rec;
    }

    *out_count = written;
    return 0;
}

struct BmbtRootBuild {
    uint8_t* root{};
    size_t root_size{};
    uint16_t level{};
    uint16_t numrecs{};
    uint32_t metadata_blocks{};
};

void free_bmbt_root(BmbtRootBuild* root) {
    if (root == nullptr) {
        return;
    }
    delete[] root->root;
    root->root = nullptr;
    root->root_size = 0;
    root->level = 0;
    root->numrecs = 0;
    root->metadata_blocks = 0;
}

auto bmbt_free_subtree(XfsMountContext* mount, XfsTransaction* tp, xfs_fsblock_t root_block, uint8_t nlevels, uint32_t* freed) -> int {
    if (mount == nullptr || tp == nullptr || root_block == NULLFSBLOCK || nlevels == 0) {
        return -EINVAL;
    }

    BufHead* bh = xfs_buf_read(mount, root_block);
    if (bh == nullptr) {
        return -EIO;
    }

    const auto* hdr = reinterpret_cast<const XfsBtreeLblock*>(bh->data);
    if (hdr->bb_magic.to_cpu() != XFS_BMAP_CRC_MAGIC) {
        brelse(bh);
        return -EIO;
    }

    uint16_t const NUMRECS = hdr->bb_numrecs.to_cpu();
    if (nlevels > 1) {
        uint32_t const NODE_CAPACITY = bmbt_node_max_keys(mount);
        if (NUMRECS > NODE_CAPACITY) {
            brelse(bh);
            return -EIO;
        }

        for (uint16_t i = 0; i < NUMRECS; i++) {
            Be64 ptr_val{};
            __builtin_memcpy(&ptr_val, bh->data + bmbt_node_ptr_offset(mount, i), sizeof(ptr_val));
            xfs_fsblock_t const CHILD = ptr_val.to_cpu();
            int const RC = bmbt_free_subtree(mount, tp, CHILD, static_cast<uint8_t>(nlevels - 1), freed);
            if (RC != 0) {
                brelse(bh);
                return RC;
            }
        }
    }

    brelse(bh);
    int const RC = bmbt_return_block(mount, tp, root_block);
    if (RC == 0 && freed != nullptr) {
        (*freed)++;
    }
    return RC;
}

auto bmbt_free_root_bytes(XfsMountContext* mount, XfsTransaction* tp, const uint8_t* root, size_t root_size, uint32_t* freed) -> int {
    if (freed != nullptr) {
        *freed = 0;
    }
    if (mount == nullptr || tp == nullptr || root == nullptr || root_size < sizeof(XfsBmdrBlock)) {
        return 0;
    }
    const auto* bmdr = reinterpret_cast<const XfsBmdrBlock*>(root);
    uint16_t const LEVEL = bmdr->bb_level.to_cpu();
    uint16_t const NUMRECS = bmdr->bb_numrecs.to_cpu();
    if (LEVEL == 0 || NUMRECS == 0) {
        return 0;
    }

    size_t const MIN_ROOT_SIZE =
        sizeof(XfsBmdrBlock) + (static_cast<size_t>(NUMRECS) * sizeof(XfsBmbtKey)) + (static_cast<size_t>(NUMRECS) * sizeof(Be64));
    if (root_size < MIN_ROOT_SIZE) {
        return -EIO;
    }

    const uint8_t* ptrs_base = root + sizeof(XfsBmdrBlock) + (static_cast<size_t>(NUMRECS) * sizeof(XfsBmbtKey));
    for (uint16_t i = 0; i < NUMRECS; i++) {
        Be64 ptr_val{};
        __builtin_memcpy(&ptr_val, ptrs_base + (static_cast<size_t>(i) * sizeof(Be64)), sizeof(ptr_val));
        int const RC = bmbt_free_subtree(mount, tp, ptr_val.to_cpu(), static_cast<uint8_t>(LEVEL), freed);
        if (RC != 0) {
            return RC;
        }
    }
    return 0;
}

auto bmbt_free_fork_blocks(XfsInode* ip, XfsTransaction* tp, uint32_t* freed) -> int {
    if (freed != nullptr) {
        *freed = 0;
    }
    if (ip == nullptr || tp == nullptr || ip->data_fork.format != XFS_DINODE_FMT_BTREE) {
        return 0;
    }

    return bmbt_free_root_bytes(ip->mount, tp, ip->data_fork.btree.root, ip->data_fork.btree.root_size, freed);
}

void fill_bmdr_root(uint8_t* root, uint16_t level, uint16_t numrecs, const XfsBmbtIrec* first_recs, const xfs_fsblock_t* ptrs) {
    auto* bmdr = reinterpret_cast<XfsBmdrBlock*>(root);
    bmdr->bb_level = Be16::from_cpu(level);
    bmdr->bb_numrecs = Be16::from_cpu(numrecs);

    uint8_t* keys_base = root + sizeof(XfsBmdrBlock);
    uint8_t* ptrs_base = keys_base + (static_cast<size_t>(numrecs) * sizeof(XfsBmbtKey));
    for (uint16_t i = 0; i < numrecs; i++) {
        XfsBmbtKey key{};
        key.br_startoff = Be64::from_cpu(first_recs[i].br_startoff);
        __builtin_memcpy(keys_base + (static_cast<size_t>(i) * sizeof(XfsBmbtKey)), &key, sizeof(key));

        Be64 ptr = Be64::from_cpu(ptrs[i]);
        __builtin_memcpy(ptrs_base + (static_cast<size_t>(i) * sizeof(ptr)), &ptr, sizeof(ptr));
    }
}

auto build_bmbt_tree(XfsInode* ip, XfsTransaction* tp, const XfsBmbtIrec* extents, uint32_t extent_count, BmbtRootBuild* out) -> int {
    if (ip == nullptr || ip->mount == nullptr || tp == nullptr || extents == nullptr || out == nullptr || extent_count == 0) {
        return -EINVAL;
    }
    if (data_fork_bmdr_capacity(ip) == 0) {
        return -EFBIG;
    }

    uint32_t const LEAF_CAPACITY = bmbt_leaf_max_recs(ip->mount);
    if (LEAF_CAPACITY == 0) {
        return -EIO;
    }

    uint32_t const LEAF_COUNT = (extent_count + LEAF_CAPACITY - 1) / LEAF_CAPACITY;
    uint32_t const NODE_CAPACITY = bmbt_node_max_keys(ip->mount);
    if (LEAF_COUNT == 0 || (LEAF_COUNT > 1 && LEAF_COUNT > NODE_CAPACITY)) {
        return -EFBIG;
    }

    auto* leaf_blocks = new (std::nothrow) xfs_fsblock_t[LEAF_COUNT];
    auto** leaf_bufs = new (std::nothrow) BufHead*[LEAF_COUNT];
    if (leaf_blocks == nullptr || leaf_bufs == nullptr) {
        delete[] leaf_blocks;
        delete[] leaf_bufs;
        return -ENOMEM;
    }
    for (uint32_t i = 0; i < LEAF_COUNT; i++) {
        leaf_blocks[i] = NULLFSBLOCK;
        leaf_bufs[i] = nullptr;
    }

    auto cleanup_leaf_blocks = [&]() {
        for (uint32_t i = 0; i < LEAF_COUNT; i++) {
            if (leaf_bufs[i] != nullptr) {
                brelse(leaf_bufs[i]);
                leaf_bufs[i] = nullptr;
            }
            if (leaf_blocks[i] != NULLFSBLOCK) {
                static_cast<void>(bmbt_return_block(ip->mount, tp, leaf_blocks[i]));
                leaf_blocks[i] = NULLFSBLOCK;
            }
        }
    };

    uint32_t extent_index = 0;
    for (uint32_t leaf = 0; leaf < LEAF_COUNT; leaf++) {
        int rc = bmbt_alloc_block(ip, tp, 0, &leaf_bufs[leaf], &leaf_blocks[leaf]);
        if (rc != 0) {
            cleanup_leaf_blocks();
            delete[] leaf_blocks;
            delete[] leaf_bufs;
            return rc;
        }

        uint32_t const RECS = std::min<uint32_t>(LEAF_CAPACITY, extent_count - extent_index);
        auto* hdr = reinterpret_cast<XfsBtreeLblock*>(leaf_bufs[leaf]->data);
        hdr->bb_numrecs = Be16::from_cpu(static_cast<uint16_t>(RECS));
        if (leaf > 0) {
            hdr->bb_leftsib = Be64::from_cpu(leaf_blocks[leaf - 1]);
        }
        if (leaf + 1 < LEAF_COUNT) {
            // Filled after the next block has been allocated.
            hdr->bb_rightsib = Be64::from_cpu(NULLFSBLOCK);
        }

        auto* recs = reinterpret_cast<XfsBmbtRec*>(leaf_bufs[leaf]->data + XFS_BTREE_LBLOCK_CRC_LEN);
        for (uint32_t r = 0; r < RECS; r++) {
            recs[r] = xfs_bmbt_rec_pack(extents[extent_index++]);
        }

        if (leaf > 0) {
            auto* prev_hdr = reinterpret_cast<XfsBtreeLblock*>(leaf_bufs[leaf - 1]->data);
            prev_hdr->bb_rightsib = Be64::from_cpu(leaf_blocks[leaf]);
        }
    }

    BufHead* root_bh = nullptr;
    xfs_fsblock_t root_block = NULLFSBLOCK;
    uint16_t bmdr_level = 1;
    xfs_fsblock_t bmdr_ptr = leaf_blocks[0];
    uint32_t metadata_blocks = LEAF_COUNT;

    if (LEAF_COUNT > 1) {
        int const RC = bmbt_alloc_block(ip, tp, 1, &root_bh, &root_block);
        if (RC != 0) {
            cleanup_leaf_blocks();
            delete[] leaf_blocks;
            delete[] leaf_bufs;
            return RC;
        }

        auto* root_hdr = reinterpret_cast<XfsBtreeLblock*>(root_bh->data);
        root_hdr->bb_numrecs = Be16::from_cpu(static_cast<uint16_t>(LEAF_COUNT));
        for (uint32_t i = 0; i < LEAF_COUNT; i++) {
            uint32_t const FIRST_INDEX = i * LEAF_CAPACITY;
            if (FIRST_INDEX >= extent_count) {
                cleanup_leaf_blocks();
                brelse(root_bh);
                static_cast<void>(bmbt_return_block(ip->mount, tp, root_block));
                delete[] leaf_blocks;
                delete[] leaf_bufs;
                return -EIO;
            }

            uint8_t* key_addr = root_bh->data + XFS_BTREE_LBLOCK_CRC_LEN + (static_cast<size_t>(i) * sizeof(XfsBmbtKey));
            XfsBmbtKey key{};
            key.br_startoff = Be64::from_cpu(extents[FIRST_INDEX].br_startoff);
            __builtin_memcpy(key_addr, &key, sizeof(key));

            Be64 ptr = Be64::from_cpu(leaf_blocks[i]);
            __builtin_memcpy(root_bh->data + bmbt_node_ptr_offset(ip->mount, i), &ptr, sizeof(ptr));
        }
        bmbt_update_crc(root_bh);

        bmdr_level = 2;
        bmdr_ptr = root_block;
        metadata_blocks++;
    }

    constexpr uint16_t BMDR_RECS = 1;
    size_t const ROOT_SIZE = sizeof(XfsBmdrBlock) + sizeof(XfsBmbtKey) + sizeof(Be64);
    auto* root = new (std::nothrow) uint8_t[ROOT_SIZE];
    if (root == nullptr) {
        cleanup_leaf_blocks();
        if (root_bh != nullptr) {
            brelse(root_bh);
            root_bh = nullptr;
        }
        if (root_block != NULLFSBLOCK) {
            static_cast<void>(bmbt_return_block(ip->mount, tp, root_block));
        }
        delete[] leaf_blocks;
        delete[] leaf_bufs;
        return -ENOMEM;
    }
    __builtin_memset(root, 0, ROOT_SIZE);

    XfsBmbtIrec const FIRST = extents[0];
    fill_bmdr_root(root, bmdr_level, BMDR_RECS, &FIRST, &bmdr_ptr);

    for (uint32_t i = 0; i < LEAF_COUNT; i++) {
        bmbt_update_crc(leaf_bufs[i]);
        xfs_trans_log_buf_full(tp, leaf_bufs[i]);
        brelse(leaf_bufs[i]);
        leaf_bufs[i] = nullptr;
    }
    if (root_bh != nullptr) {
        xfs_trans_log_buf_full(tp, root_bh);
        brelse(root_bh);
        root_bh = nullptr;
    }

    out->root = root;
    out->root_size = ROOT_SIZE;
    out->level = bmdr_level;
    out->numrecs = BMDR_RECS;
    out->metadata_blocks = metadata_blocks;

    delete[] leaf_blocks;
    delete[] leaf_bufs;
    return 0;
}

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
        ip->data_fork.extents.capacity = 0;
    }

    if (ip->data_fork.format != XFS_DINODE_FMT_EXTENTS && ip->data_fork.format != XFS_DINODE_FMT_BTREE) {
        mod::dbg::log("[xfs] bmap_add_extent: unsupported fork format %d for inode %lu", ip->data_fork.format,
                      static_cast<unsigned long>(ip->ino));
        return -EOPNOTSUPP;
    }

    XfsBmbtIrec* old_extents = nullptr;
    uint32_t old_count = 0;
    bool const WAS_BTREE = ip->data_fork.format == XFS_DINODE_FMT_BTREE;

    if (WAS_BTREE) {
        old_count = ip->nextents;
        if (old_count != 0) {
            old_extents = new (std::nothrow) XfsBmbtIrec[old_count];
            if (old_extents == nullptr) {
                return -ENOMEM;
            }
            int const LIST_RC = xfs_bmap_list_extents(ip, old_extents, old_count);
            if (LIST_RC < 0) {
                delete[] old_extents;
                return LIST_RC;
            }
            if (std::cmp_not_equal(LIST_RC, old_count)) {
                delete[] old_extents;
                return -EIO;
            }
            old_count = static_cast<uint32_t>(LIST_RC);
        }
    } else {
        XfsIforkExtents& ext = ip->data_fork.extents;
        old_extents = ext.list;
        old_count = ext.count;
        if (old_count != 0 && old_extents == nullptr) {
            return -EIO;
        }
    }
    if (old_count == UINT32_MAX) {
        if (WAS_BTREE) {
            delete[] old_extents;
        }
        return -EFBIG;
    }

    uint32_t const NEW_EXTENTS_CAPACITY = old_count + 1;
    auto* new_extents = new (std::nothrow) XfsBmbtIrec[NEW_EXTENTS_CAPACITY];
    if (new_extents == nullptr) {
        if (WAS_BTREE) {
            delete[] old_extents;
        }
        return -ENOMEM;
    }

    uint32_t new_count = 0;
    int rc = insert_or_merge_extent(old_extents, old_count, new_ext, new_extents, &new_count);
    if (WAS_BTREE) {
        delete[] old_extents;
        old_extents = nullptr;
    }
    if (rc != 0) {
        delete[] new_extents;
        return rc;
    }
    if (new_count > NEW_EXTENTS_CAPACITY) {
        delete[] new_extents;
        return -EIO;
    }

    uint32_t const INLINE_CAPACITY = data_fork_record_capacity(ip);
    if (!WAS_BTREE && new_count <= INLINE_CAPACITY) {
        XfsIforkExtents& ext = ip->data_fork.extents;
        if (new_count > ext.capacity) {
            uint32_t new_cap = ext.capacity == 0 ? 4 : ext.capacity * 2;
            new_cap = std::max(new_cap, new_count);
            auto* new_list = new (std::nothrow) XfsBmbtIrec[new_cap];
            if (new_list == nullptr) {
                delete[] new_extents;
                return -ENOMEM;
            }
            delete[] ext.list;
            ext.list = new_list;
            ext.capacity = new_cap;
        }
        if (new_count != 0 && ext.list == nullptr) {
            delete[] new_extents;
            return -EIO;
        }
        for (uint32_t i = 0; i < new_count; i++) {
            ext.list[i] = new_extents[i];
        }
        ext.count = new_count;
        ip->nextents = new_count;
        ip->dirty = true;
        xfs_trans_log_inode(tp, ip);
        delete[] new_extents;
        return 0;
    }

    BmbtRootBuild new_root{};
    rc = build_bmbt_tree(ip, tp, new_extents, new_count, &new_root);
    if (rc != 0) {
        delete[] new_extents;
        return rc;
    }

    uint32_t old_metadata_blocks = 0;
    uint8_t* old_root = nullptr;
    size_t old_root_size = 0;
    if (WAS_BTREE) {
        old_root = ip->data_fork.btree.root;
        old_root_size = ip->data_fork.btree.root_size;
        rc = bmbt_free_root_bytes(ip->mount, tp, old_root, old_root_size, &old_metadata_blocks);
        if (rc != 0) {
            static_cast<void>(bmbt_free_root_bytes(ip->mount, tp, new_root.root, new_root.root_size, nullptr));
            free_bmbt_root(&new_root);
            delete[] new_extents;
            return rc;
        }
    }

    if (!WAS_BTREE) {
        delete[] ip->data_fork.extents.list;
    } else {
        delete[] old_root;
    }

    ip->data_fork.format = XFS_DINODE_FMT_BTREE;
    ip->data_fork.btree.level = new_root.level;
    ip->data_fork.btree.numrecs = new_root.numrecs;
    ip->data_fork.btree.root = new_root.root;
    ip->data_fork.btree.root_size = new_root.root_size;
    ip->nextents = new_count;
    if (WAS_BTREE) {
        if (ip->nblocks >= old_metadata_blocks) {
            ip->nblocks = ip->nblocks - old_metadata_blocks + new_root.metadata_blocks;
        } else {
            ip->nblocks += new_root.metadata_blocks;
        }
    } else {
        ip->nblocks += new_root.metadata_blocks;
    }
    ip->dirty = true;
    xfs_trans_log_inode(tp, ip);

    delete[] new_extents;

    return 0;
}

auto xfs_bmap_free_btree_blocks(XfsInode* ip, XfsTransaction* tp, uint32_t* freed_blocks) -> int {
    return bmbt_free_fork_blocks(ip, tp, freed_blocks);
}

}  // namespace ker::vfs::xfs
