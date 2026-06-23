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
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <dev/block_device.hpp>
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

auto data_fork_payload_size(const XfsInode* ip) -> size_t {
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

    return fork_size;
}

auto data_fork_record_capacity(const XfsInode* ip) -> uint32_t {
    size_t const FORK_SIZE = data_fork_payload_size(ip);
    size_t const CAPACITY = FORK_SIZE / sizeof(XfsBmbtRec);
    return CAPACITY > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(CAPACITY);
}

auto data_fork_bmdr_capacity(const XfsInode* ip) -> uint32_t {
    size_t const FORK_SIZE = data_fork_payload_size(ip);
    if (FORK_SIZE < sizeof(XfsBmdrBlock)) {
        return 0;
    }

    size_t const CAPACITY = (FORK_SIZE - sizeof(XfsBmdrBlock)) / (sizeof(XfsBmbtKey) + sizeof(Be64));
    return CAPACITY > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(CAPACITY);
}

auto bmdr_root_min_size(uint32_t maxrecs, uint16_t numrecs) -> size_t {
    if (numrecs > maxrecs) {
        return SIZE_MAX;
    }
    return sizeof(XfsBmdrBlock) + (static_cast<size_t>(maxrecs) * sizeof(XfsBmbtKey)) + (static_cast<size_t>(numrecs) * sizeof(Be64));
}

auto bmdr_root_layout(const XfsInode* ip, size_t root_size, uint16_t numrecs, uint32_t* maxrecs_out) -> int {
    if (maxrecs_out != nullptr) {
        *maxrecs_out = 0;
    }
    uint32_t const MAXRECS = data_fork_bmdr_capacity(ip);
    if (MAXRECS == 0 || numrecs > MAXRECS) {
        return -EIO;
    }
    size_t const MIN_SIZE = bmdr_root_min_size(MAXRECS, numrecs);
    if (MIN_SIZE == SIZE_MAX || root_size < MIN_SIZE) {
        return -EIO;
    }
    if (maxrecs_out != nullptr) {
        *maxrecs_out = MAXRECS;
    }
    return 0;
}

auto bmdr_key_addr(uint8_t* root, uint16_t idx) -> uint8_t* {
    return root + sizeof(XfsBmdrBlock) + (static_cast<size_t>(idx) * sizeof(XfsBmbtKey));
}

auto bmdr_key_addr(const uint8_t* root, uint16_t idx) -> const uint8_t* {
    return root + sizeof(XfsBmdrBlock) + (static_cast<size_t>(idx) * sizeof(XfsBmbtKey));
}

auto bmdr_ptr_addr(uint8_t* root, uint32_t maxrecs, uint16_t idx) -> uint8_t* {
    return root + sizeof(XfsBmdrBlock) + (static_cast<size_t>(maxrecs) * sizeof(XfsBmbtKey)) + (static_cast<size_t>(idx) * sizeof(Be64));
}

auto bmdr_ptr_addr(const uint8_t* root, uint32_t maxrecs, uint16_t idx) -> const uint8_t* {
    return root + sizeof(XfsBmdrBlock) + (static_cast<size_t>(maxrecs) * sizeof(XfsBmbtKey)) + (static_cast<size_t>(idx) * sizeof(Be64));
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

    XfsAllocReq req{};
    req.agno = ip->agno;
    req.agbno = 0;
    req.minlen = 1;
    req.maxlen = 1;
    req.alignment = 0;

    XfsAllocResult alloc{};
    int const RC = xfs_alloc_extent(ip->mount, tp, req, &alloc);
    if (RC != 0) {
        return RC;
    }
    if (alloc.agno >= ip->mount->ag_count || alloc.agbno == NULLAGBLOCK || alloc.agbno >= ip->mount->ag_blocks || alloc.len != 1) {
        if (alloc.agno < ip->mount->ag_count && alloc.agbno != NULLAGBLOCK && alloc.agbno < ip->mount->ag_blocks && alloc.len != 0) {
            static_cast<void>(xfs_free_extent(ip->mount, tp, alloc.agno, alloc.agbno, alloc.len));
        }
        return -EIO;
    }

    xfs_fsblock_t const FSB = xfs_agbno_to_fsbno(alloc.agno, alloc.agbno, ip->mount->ag_blk_log);
    BufHead* bh = xfs_buf_get(ip->mount, FSB);
    if (bh == nullptr) {
        static_cast<void>(xfs_free_extent(ip->mount, tp, alloc.agno, alloc.agbno, 1));
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
    if (mount == nullptr || tp == nullptr || fsb == NULLFSBLOCK) {
        return -EINVAL;
    }
    xfs_agnumber_t const AGNO = xfs_ag_number(fsb, mount->ag_blk_log);
    xfs_agblock_t const AGBNO = xfs_ag_block(fsb, mount->ag_blk_log);
    if (AGNO >= mount->ag_count || AGBNO >= mount->ag_blocks) {
        return -EIO;
    }
    return xfs_free_extent(mount, tp, AGNO, AGBNO, 1);
}

auto bmbt_valid_irec(const XfsBmbtIrec& rec) -> bool {
    constexpr xfs_fileoff_t MAX_STARTOFF = 0x3FFFFFFFFFFFFFULL;
    constexpr xfs_fsblock_t MAX_STARTBLOCK = 0xFFFFFFFFFFFFFULL;
    constexpr xfs_filblks_t MAX_BLOCKCOUNT = 0x1FFFFFULL;

    if (rec.br_blockcount == 0 || rec.br_blockcount > MAX_BLOCKCOUNT || rec.br_startoff > MAX_STARTOFF ||
        rec.br_startblock == NULLFSBLOCK || rec.br_startblock > MAX_STARTBLOCK) {
        return false;
    }

    return rec.br_blockcount <= (MAX_STARTOFF - rec.br_startoff) + 1 && rec.br_blockcount <= (MAX_STARTBLOCK - rec.br_startblock) + 1;
}

auto bmbt_file_end_exclusive(const XfsBmbtIrec& rec) -> xfs_fileoff_t { return rec.br_startoff + rec.br_blockcount; }

void set_hole_result(XfsBmapResult* result, xfs_filblks_t blockcount) {
    result->is_hole = true;
    result->startblock = NULLFSBLOCK;
    result->blockcount = blockcount;
    result->unwritten = false;
}

auto bmbt_extent_list_is_ordered(const XfsBmbtIrec* list, uint32_t count) -> bool {
    if (count != 0 && list == nullptr) {
        return false;
    }
    for (uint32_t i = 0; i < count; i++) {
        if (!bmbt_valid_irec(list[i])) {
            return false;
        }
        if (i != 0 && bmbt_file_end_exclusive(list[i - 1]) > list[i].br_startoff) {
            return false;
        }
    }
    return true;
}

auto extent_can_merge_left(const XfsBmbtIrec& left, const XfsBmbtIrec& right) -> bool {
    constexpr xfs_filblks_t MAX_BLOCKCOUNT = 0x1FFFFFULL;
    if (left.br_blockcount > MAX_BLOCKCOUNT - right.br_blockcount) {
        return false;
    }
    return left.br_startoff + left.br_blockcount == right.br_startoff && left.br_startblock + left.br_blockcount == right.br_startblock &&
           left.br_unwritten == right.br_unwritten;
}

auto transaction_has_inode_item(const XfsTransaction* tp, const XfsInode* ip) -> bool {
    if (tp == nullptr || ip == nullptr) {
        return false;
    }
    for (int i = 0; i < tp->item_count; i++) {
        XfsTransItem const& item = tp->items.at(static_cast<size_t>(i));
        if (item.type == XfsLogItemType::INODE && item.inode.ip == ip) {
            return true;
        }
    }
    return false;
}

auto ensure_inode_logged(XfsTransaction* tp, XfsInode* ip) -> int {
    if (tp == nullptr || ip == nullptr) {
        return -EINVAL;
    }
    if (transaction_has_inode_item(tp, ip)) {
        return 0;
    }
    if (tp->item_count >= XFS_TRANS_MAX_ITEMS) {
        return -EFBIG;
    }
    xfs_trans_log_inode(tp, ip);
    return transaction_has_inode_item(tp, ip) ? 0 : -EFBIG;
}

auto insert_or_merge_extent(const XfsBmbtIrec* old_list, uint32_t old_count, const XfsBmbtIrec& new_ext, XfsBmbtIrec* out_list,
                            uint32_t* out_count) -> int {
    if (out_list == nullptr || out_count == nullptr || !bmbt_valid_irec(new_ext) || !bmbt_extent_list_is_ordered(old_list, old_count)) {
        return -EINVAL;
    }

    uint32_t insert_at = 0;
    while (insert_at < old_count && old_list[insert_at].br_startoff < new_ext.br_startoff) {
        out_list[insert_at] = old_list[insert_at];
        insert_at++;
    }

    XfsBmbtIrec pending = new_ext;
    uint32_t written = insert_at;
    if (written > 0 && bmbt_file_end_exclusive(out_list[written - 1]) > pending.br_startoff) {
        return -EIO;
    }
    if (written > 0 && extent_can_merge_left(out_list[written - 1], pending)) {
        out_list[written - 1].br_blockcount += pending.br_blockcount;
    } else {
        out_list[written++] = pending;
    }

    for (uint32_t i = insert_at; i < old_count; i++) {
        XfsBmbtIrec rec = old_list[i];
        if (written > 0 && bmbt_file_end_exclusive(out_list[written - 1]) > rec.br_startoff) {
            return -EIO;
        }
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
    if (nlevels > XFS_BTREE_MAXLEVELS || hdr->bb_level.to_cpu() != static_cast<uint16_t>(nlevels - 1)) {
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
            if (CHILD == NULLFSBLOCK) {
                brelse(bh);
                return -EIO;
            }
            int const RC = bmbt_free_subtree(mount, tp, CHILD, static_cast<uint8_t>(nlevels - 1), freed);
            if (RC != 0) {
                brelse(bh);
                return RC;
            }
        }
    } else if (NUMRECS > bmbt_leaf_max_recs(mount)) {
        brelse(bh);
        return -EIO;
    }

    brelse(bh);
    int const RC = bmbt_return_block(mount, tp, root_block);
    if (RC == 0 && freed != nullptr) {
        (*freed)++;
    }
    return RC;
}

auto bmbt_free_root_bytes(XfsInode* ip, XfsTransaction* tp, const uint8_t* root, size_t root_size, uint32_t* freed) -> int {
    if (freed != nullptr) {
        *freed = 0;
    }
    if (ip == nullptr || ip->mount == nullptr || tp == nullptr || root == nullptr || root_size < sizeof(XfsBmdrBlock)) {
        return 0;
    }
    const auto* bmdr = reinterpret_cast<const XfsBmdrBlock*>(root);
    uint16_t const LEVEL = bmdr->bb_level.to_cpu();
    uint16_t const NUMRECS = bmdr->bb_numrecs.to_cpu();
    if (LEVEL == 0 || NUMRECS == 0) {
        return 0;
    }
    if (LEVEL > XFS_BTREE_MAXLEVELS) {
        return -EIO;
    }

    uint32_t root_maxrecs = 0;
    int rc = bmdr_root_layout(ip, root_size, NUMRECS, &root_maxrecs);
    if (rc != 0) {
        return rc;
    }
    for (uint16_t i = 0; i < NUMRECS; i++) {
        Be64 ptr_val{};
        __builtin_memcpy(&ptr_val, bmdr_ptr_addr(root, root_maxrecs, i), sizeof(ptr_val));
        int const RC = bmbt_free_subtree(ip->mount, tp, ptr_val.to_cpu(), static_cast<uint8_t>(LEVEL), freed);
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

    return bmbt_free_root_bytes(ip, tp, ip->data_fork.btree.root, ip->data_fork.btree.root_size, freed);
}

void fill_bmdr_root(uint8_t* root, uint32_t maxrecs, uint16_t level, uint16_t numrecs, const XfsBmbtIrec* first_recs,
                    const xfs_fsblock_t* ptrs) {
    auto* bmdr = reinterpret_cast<XfsBmdrBlock*>(root);
    bmdr->bb_level = Be16::from_cpu(level);
    bmdr->bb_numrecs = Be16::from_cpu(numrecs);

    for (uint16_t i = 0; i < numrecs; i++) {
        XfsBmbtKey key{};
        key.br_startoff = Be64::from_cpu(first_recs[i].br_startoff);
        __builtin_memcpy(bmdr_key_addr(root, i), &key, sizeof(key));

        Be64 ptr = Be64::from_cpu(ptrs[i]);
        __builtin_memcpy(bmdr_ptr_addr(root, maxrecs, i), &ptr, sizeof(ptr));
    }
}

auto build_bmbt_tree(XfsInode* ip, XfsTransaction* tp, const XfsBmbtIrec* extents, uint32_t extent_count, BmbtRootBuild* out) -> int {
    if (ip == nullptr || ip->mount == nullptr || tp == nullptr || extents == nullptr || out == nullptr || extent_count == 0) {
        return -EINVAL;
    }
    if (!bmbt_extent_list_is_ordered(extents, extent_count)) {
        return -EINVAL;
    }
    uint32_t const BMDR_CAPACITY = data_fork_bmdr_capacity(ip);
    if (BMDR_CAPACITY == 0) {
        return -EFBIG;
    }

    uint32_t const LEAF_CAPACITY = bmbt_leaf_max_recs(ip->mount);
    if (LEAF_CAPACITY == 0) {
        return -EIO;
    }

    uint32_t const LEAF_COUNT = 1U + ((extent_count - 1U) / LEAF_CAPACITY);
    uint32_t const NODE_CAPACITY = bmbt_node_max_keys(ip->mount);
    if (LEAF_COUNT == 0 || (LEAF_COUNT > 1 && LEAF_COUNT > NODE_CAPACITY)) {
        return -EFBIG;
    }
    uint32_t const REQUIRED_METADATA_BLOCKS = LEAF_COUNT + (LEAF_COUNT > 1 ? 1U : 0U);
    uint32_t const AVAILABLE_TRANSACTION_ITEMS =
        tp->item_count >= XFS_TRANS_MAX_ITEMS ? 0U : static_cast<uint32_t>(XFS_TRANS_MAX_ITEMS - tp->item_count);
    if (REQUIRED_METADATA_BLOCKS > AVAILABLE_TRANSACTION_ITEMS) {
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
    size_t const ROOT_SIZE = bmdr_root_min_size(BMDR_CAPACITY, BMDR_RECS);
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
    fill_bmdr_root(root, BMDR_CAPACITY, bmdr_level, BMDR_RECS, &FIRST, &bmdr_ptr);

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
        // No extents at all - the entire address space is a hole.  Return a
        // large blockcount so the caller can allocate in large batches.
        set_hole_result(result, ~static_cast<xfs_filblks_t>(0));
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
        // Compute the size of the hole (distance to next extent)
        // lo now points to the first extent starting after file_block
        if (std::cmp_less(lo, ext.count)) {
            set_hole_result(result, ext.list[lo].br_startoff - file_block);
        } else {
            // Past the last extent - unbounded hole to EOF.  Return a large
            // blockcount so callers can allocate in large batches.
            set_hole_result(result, ~static_cast<xfs_filblks_t>(0));
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
        set_hole_result(result, ~static_cast<xfs_filblks_t>(0));
        return 0;
    }

    if (LEVEL == 0 || LEVEL > XFS_BTREE_MAXLEVELS) {
        return -EIO;
    }
    uint32_t root_maxrecs = 0;
    int layout_rc = bmdr_root_layout(ip, bt.root_size, NUMRECS, &root_maxrecs);
    if (layout_rc != 0) {
        return layout_rc;
    }

    // Binary search within the root keys to find the right child pointer
    int lo = 0;
    int hi = NUMRECS - 1;
    int keyno = 0;  // 0-based for root node

    while (lo <= hi) {
        int const MID = (lo + hi) / 2;
        const auto* key =
            reinterpret_cast<const XfsBmbtKey*>(bmdr_key_addr(static_cast<const uint8_t*>(bt.root), static_cast<uint16_t>(MID)));
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
    __builtin_memcpy(&ptr_val, bmdr_ptr_addr(bt.root, root_maxrecs, static_cast<uint16_t>(keyno)), sizeof(Be64));
    uint64_t const CHILD_BLOCK = ptr_val.to_cpu();

    if (LEVEL == 1) {
        // Child is a leaf - we can read it directly and do the lookup
        // Set up a btree cursor positioned at the child block
        XfsBtreeCursor<XfsBmbtTraits> cur;
        cur.mount = mount;
        cur.nlevels = 1;
        cur.owner = ip->ino;

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
            if (lo <= NR) {
                const auto* rec = cur.rec_at(lo);
                XfsBmbtIrec const NEXT = xfs_bmbt_rec_unpack(rec);
                set_hole_result(result, NEXT.br_startoff > file_block ? NEXT.br_startoff - file_block : 1);
            } else {
                set_hole_result(result, ~static_cast<xfs_filblks_t>(0));
            }
        }
        return 0;
    }

    // Multi-level btree: use the full cursor mechanism
    // We start from the child block at (level - 1) and descend
    XfsBtreeCursor<XfsBmbtTraits> cur;
    cur.mount = mount;
    cur.nlevels = LEVEL;  // depth from this child down
    cur.owner = ip->ino;

    // Target for lookup
    XfsBmbtIrec target{};
    target.br_startoff = file_block;

    int rc = xfs_btree_lookup(&cur, CHILD_BLOCK, LEVEL, target, XfsBtreeLookup::LE);
    if (rc == -ENOENT) {
        XfsBtreeCursor<XfsBmbtTraits> next_cur;
        next_cur.mount = mount;
        next_cur.nlevels = LEVEL;
        next_cur.owner = ip->ino;

        int const NEXT_RC = xfs_btree_lookup(&next_cur, CHILD_BLOCK, LEVEL, target, XfsBtreeLookup::GE);
        if (NEXT_RC == 0) {
            XfsBmbtIrec const NEXT = xfs_btree_get_rec(&next_cur);
            set_hole_result(result, NEXT.br_startoff > file_block ? NEXT.br_startoff - file_block : 1);
        } else if (NEXT_RC == -ENOENT) {
            set_hole_result(result, ~static_cast<xfs_filblks_t>(0));
        } else {
            return NEXT_RC;
        }
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
        // Distance to next extent
        rc = xfs_btree_increment(&cur);
        if (rc == 0) {
            XfsBmbtIrec const NEXT = xfs_btree_get_rec(&cur);
            set_hole_result(result, NEXT.br_startoff > file_block ? NEXT.br_startoff - file_block : 1);
        } else if (rc == -ENOENT) {
            set_hole_result(result, ~static_cast<xfs_filblks_t>(0));
        } else {
            return rc;
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
        if (LEVEL == 0 || LEVEL > XFS_BTREE_MAXLEVELS) {
            return -EIO;
        }
        uint32_t root_maxrecs = 0;
        int const LAYOUT_RC = bmdr_root_layout(ip, bt.root_size, NUMRECS, &root_maxrecs);
        if (LAYOUT_RC != 0) {
            return LAYOUT_RC;
        }

        // Get the leftmost child pointer (first key, first ptr)
        Be64 ptr_val{};
        __builtin_memcpy(&ptr_val, bmdr_ptr_addr(bt.root, root_maxrecs, 0), sizeof(Be64));
        uint64_t const CHILD_BLOCK = ptr_val.to_cpu();

        // Use a cursor positioned at the very beginning
        XfsBtreeCursor<XfsBmbtTraits> cur;
        cur.mount = ip->mount;
        cur.owner = ip->ino;

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
    if (!bmbt_valid_irec(new_ext)) {
        return -EINVAL;
    }

    bool const WAS_LOCAL = ip->data_fork.format == XFS_DINODE_FMT_LOCAL;
    bool const WAS_BTREE = ip->data_fork.format == XFS_DINODE_FMT_BTREE;
    if (!WAS_LOCAL && ip->data_fork.format != XFS_DINODE_FMT_EXTENTS && !WAS_BTREE) {
        mod::dbg::log("[xfs] bmap_add_extent: unsupported fork format %d for inode %lu", ip->data_fork.format,
                      static_cast<unsigned long>(ip->ino));
        return -EOPNOTSUPP;
    }
    int rc = ensure_inode_logged(tp, ip);
    if (rc != 0) {
        return rc;
    }

    XfsBmbtIrec* old_extents = nullptr;
    uint32_t old_count = 0;

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
    } else if (!WAS_LOCAL) {
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
    rc = insert_or_merge_extent(old_extents, old_count, new_ext, new_extents, &new_count);
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
        uint32_t old_capacity = 0;
        if (!WAS_LOCAL) {
            old_capacity = ip->data_fork.extents.capacity;
        }

        XfsBmbtIrec* target_list = WAS_LOCAL ? nullptr : ip->data_fork.extents.list;
        uint32_t target_capacity = old_capacity;
        if (new_count > target_capacity) {
            uint32_t new_cap = target_capacity == 0 ? 4 : target_capacity * 2;
            new_cap = std::max(new_cap, new_count);
            auto* new_list = new (std::nothrow) XfsBmbtIrec[new_cap];
            if (new_list == nullptr) {
                delete[] new_extents;
                return -ENOMEM;
            }
            if (!WAS_LOCAL) {
                delete[] ip->data_fork.extents.list;
            }
            target_list = new_list;
            target_capacity = new_cap;
        }
        if (new_count != 0 && target_list == nullptr) {
            delete[] new_extents;
            return -EIO;
        }
        for (uint32_t i = 0; i < new_count; i++) {
            target_list[i] = new_extents[i];
        }

        if (WAS_LOCAL) {
            delete[] ip->data_fork.local.data;
            ip->data_fork.format = XFS_DINODE_FMT_EXTENTS;
        }
        ip->data_fork.extents.list = target_list;
        ip->data_fork.extents.count = new_count;
        ip->data_fork.extents.capacity = target_capacity;
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
        rc = bmbt_free_root_bytes(ip, tp, old_root, old_root_size, &old_metadata_blocks);
        if (rc != 0) {
            static_cast<void>(bmbt_free_root_bytes(ip, tp, new_root.root, new_root.root_size, nullptr));
            free_bmbt_root(&new_root);
            delete[] new_extents;
            return rc;
        }
    }

    if (WAS_LOCAL) {
        delete[] ip->data_fork.local.data;
    } else if (!WAS_BTREE) {
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

auto xfs_selftest_bmap_insert_merge_cases() -> bool {
    std::array<XfsBmbtIrec, 2> old_split{{
        {.br_startoff = 0, .br_startblock = 100, .br_blockcount = 2, .br_unwritten = false},
        {.br_startoff = 4, .br_startblock = 104, .br_blockcount = 2, .br_unwritten = false},
    }};
    std::array<XfsBmbtIrec, 3> merged{};
    uint32_t merged_count = 0;
    XfsBmbtIrec const BRIDGE{.br_startoff = 2, .br_startblock = 102, .br_blockcount = 2, .br_unwritten = false};
    if (insert_or_merge_extent(old_split.data(), 2, BRIDGE, merged.data(), &merged_count) != 0) {
        return false;
    }
    if (merged_count != 1 || merged[0].br_startoff != 0 || merged[0].br_startblock != 100 || merged[0].br_blockcount != 6) {
        return false;
    }

    std::array<XfsBmbtIrec, 21> old_many{};
    for (uint32_t i = 0; i < 21; i++) {
        old_many[i] = {.br_startoff = static_cast<xfs_fileoff_t>(i * 2),
                       .br_startblock = static_cast<xfs_fsblock_t>(1000 + (i * 3)),
                       .br_blockcount = 1,
                       .br_unwritten = false};
    }
    std::array<XfsBmbtIrec, 22> expanded{};
    uint32_t expanded_count = 0;
    XfsBmbtIrec const DISJOINT{.br_startoff = 41, .br_startblock = 4096, .br_blockcount = 1, .br_unwritten = false};
    if (insert_or_merge_extent(old_many.data(), 21, DISJOINT, expanded.data(), &expanded_count) != 0) {
        return false;
    }
    if (expanded_count != 22) {
        return false;
    }
    for (uint32_t i = 1; i < expanded_count; i++) {
        if (expanded[i - 1].br_startoff >= expanded[i].br_startoff) {
            return false;
        }
    }
    return true;
}

namespace {

auto bmap_selftest_read(ker::dev::BlockDevice* dev, uint64_t /*block*/, size_t count, void* buffer) -> int {
    __builtin_memset(buffer, 0, count * dev->block_size);
    return 0;
}

auto bmap_selftest_write(ker::dev::BlockDevice* /*dev*/, uint64_t /*block*/, size_t /*count*/, const void* /*buffer*/) -> int { return 0; }

void bmap_selftest_init_free_root(XfsMountContext* mount, xfs_agblock_t root_agbno, uint32_t magic, xfs_agblock_t free_start,
                                  xfs_extlen_t free_len) {
    BufHead* root = xfs_buf_get(mount, xfs_agbno_to_fsbno(0, root_agbno, mount->ag_blk_log));
    if (root == nullptr) {
        return;
    }

    __builtin_memset(root->data, 0, root->size);
    auto* hdr = reinterpret_cast<XfsBtreeSblock*>(root->data);
    hdr->bb_magic = Be32::from_cpu(magic);
    hdr->bb_level = Be16::from_cpu(0);
    hdr->bb_numrecs = Be16::from_cpu(1);
    hdr->bb_leftsib = Be32::from_cpu(NULLAGBLOCK);
    hdr->bb_rightsib = Be32::from_cpu(NULLAGBLOCK);
    hdr->bb_blkno = Be64::from_cpu(static_cast<uint64_t>(root_agbno) * (mount->block_size / mount->sect_size));
    hdr->bb_owner = Be32::from_cpu(0);
    hdr->bb_uuid = mount->uuid;

    auto* rec = reinterpret_cast<XfsAllocRec*>(root->data + XFS_BTREE_SBLOCK_CRC_LEN);
    rec->ar_startblock = Be32::from_cpu(free_start);
    rec->ar_blockcount = Be32::from_cpu(free_len);
    brelse(root);
}

}  // namespace

auto xfs_selftest_bmap_synthetic_btree_lookup() -> bool {
    ker::dev::BlockDevice dev{};
    dev.block_size = 512;
    dev.total_blocks = 4096;
    dev.read_blocks = bmap_selftest_read;
    dev.write_blocks = bmap_selftest_write;

    XfsMountContext mount{};
    mount.device = &dev;
    mount.block_size = 512;
    mount.block_log = 9;
    mount.total_blocks = 4096;
    mount.inode_size = 512;
    mount.ag_count = 1;
    mount.ag_blocks = 4096;
    mount.ag_blk_log = 12;
    mount.sect_size = 512;
    mount.sect_log = 9;

    constexpr xfs_ino_t INO = 42;
    constexpr xfs_fsblock_t LEAF_FSB = 7;
    BufHead* leaf = xfs_buf_get(&mount, LEAF_FSB);
    if (leaf == nullptr) {
        return false;
    }
    __builtin_memset(leaf->data, 0, leaf->size);
    auto* hdr = reinterpret_cast<XfsBtreeLblock*>(leaf->data);
    hdr->bb_magic = Be32::from_cpu(XFS_BMAP_CRC_MAGIC);
    hdr->bb_level = Be16::from_cpu(0);
    hdr->bb_numrecs = Be16::from_cpu(3);
    hdr->bb_leftsib = Be64::from_cpu(NULLFSBLOCK);
    hdr->bb_rightsib = Be64::from_cpu(NULLFSBLOCK);
    hdr->bb_blkno = Be64::from_cpu(LEAF_FSB);
    hdr->bb_owner = Be64::from_cpu(INO);
    hdr->bb_uuid = mount.uuid;

    auto* recs = reinterpret_cast<XfsBmbtRec*>(leaf->data + XFS_BTREE_LBLOCK_CRC_LEN);
    std::array<XfsBmbtIrec, 3> const RECORDS{{
        {.br_startoff = 0, .br_startblock = 100, .br_blockcount = 2, .br_unwritten = false},
        {.br_startoff = 5, .br_startblock = 200, .br_blockcount = 3, .br_unwritten = false},
        {.br_startoff = 12, .br_startblock = 300, .br_blockcount = 1, .br_unwritten = true},
    }};
    for (uint32_t i = 0; i < 3; i++) {
        recs[i] = xfs_bmbt_rec_pack(RECORDS[i]);
    }
    bmbt_update_crc(leaf);
    brelse(leaf);

    constexpr uint32_t BMDR_CAPACITY = (512 - XFS_DINODE_SIZE_V3 - sizeof(XfsBmdrBlock)) / (sizeof(XfsBmbtKey) + sizeof(Be64));
    constexpr size_t ROOT_SIZE = sizeof(XfsBmdrBlock) + (static_cast<size_t>(BMDR_CAPACITY) * sizeof(XfsBmbtKey)) + sizeof(Be64);
    std::array<uint8_t, ROOT_SIZE> root{};
    fill_bmdr_root(root.data(), BMDR_CAPACITY, 1, 1, RECORDS.data(), &LEAF_FSB);

    XfsInode inode{};
    inode.ino = INO;
    inode.mount = &mount;
    inode.data_fork.format = XFS_DINODE_FMT_BTREE;
    inode.data_fork.btree.level = 1;
    inode.data_fork.btree.numrecs = 1;
    inode.data_fork.btree.root = root.data();
    inode.data_fork.btree.root_size = root.size();
    inode.nextents = 3;

    auto finish = [&](bool ok) -> bool {
        invalidate_bdev(&dev);
        return ok;
    };

    XfsBmapResult result{};
    if (xfs_bmap_lookup(&inode, 0, &result) != 0 || result.is_hole || result.startblock != 100 || result.blockcount != 2) {
        return finish(false);
    }
    if (xfs_bmap_lookup(&inode, 2, &result) != 0 || !result.is_hole || result.blockcount != 3) {
        return finish(false);
    }
    if (xfs_bmap_lookup(&inode, 6, &result) != 0 || result.is_hole || result.startblock != 201 || result.blockcount != 2) {
        return finish(false);
    }
    if (xfs_bmap_lookup(&inode, 12, &result) != 0 || result.is_hole || result.startblock != 300 || !result.unwritten) {
        return finish(false);
    }

    std::array<XfsBmbtIrec, 3> listed{};
    int const LISTED = xfs_bmap_list_extents(&inode, listed.data(), 3);
    if (LISTED != 3) {
        return finish(false);
    }
    for (uint32_t i = 0; i < 3; i++) {
        if (listed[i].br_startoff != RECORDS[i].br_startoff || listed[i].br_startblock != RECORDS[i].br_startblock ||
            listed[i].br_blockcount != RECORDS[i].br_blockcount || listed[i].br_unwritten != RECORDS[i].br_unwritten) {
            return finish(false);
        }
    }

    return finish(true);
}

auto xfs_selftest_bmap_extent_promotion() -> bool {
    ker::dev::BlockDevice dev{};
    dev.block_size = 512;
    dev.total_blocks = 4096;
    dev.read_blocks = bmap_selftest_read;
    dev.write_blocks = bmap_selftest_write;

    XfsPerAG pag{};
    pag.agno = 0;
    pag.agf_length = 4096;
    pag.agf_bno_root = 1;
    pag.agf_cnt_root = 2;
    pag.agf_bno_level = 1;
    pag.agf_cnt_level = 1;
    pag.agf_freeblks = 256;
    pag.agf_longest = 256;
    pag.agf_flcount = 4;

    XfsMountContext mount{};
    mount.device = &dev;
    mount.block_size = 512;
    mount.block_log = 9;
    mount.total_blocks = 4096;
    mount.inode_size = 512;
    mount.ag_count = 1;
    mount.ag_blocks = 4096;
    mount.ag_blk_log = 12;
    mount.sect_size = 512;
    mount.sect_log = 9;
    mount.per_ag = &pag;

    bmap_selftest_init_free_root(&mount, pag.agf_bno_root, XFS_ABTB_CRC_MAGIC, 100, 256);
    bmap_selftest_init_free_root(&mount, pag.agf_cnt_root, XFS_ABTC_CRC_MAGIC, 100, 256);

    XfsInode inode{};
    inode.ino = 84;
    inode.agno = 0;
    inode.mount = &mount;
    inode.data_fork.format = XFS_DINODE_FMT_LOCAL;
    inode.data_fork.local.data = nullptr;
    inode.data_fork.local.size = 0;

    auto cleanup = [&](bool ok) -> bool {
        if (inode.data_fork.format == XFS_DINODE_FMT_EXTENTS) {
            delete[] inode.data_fork.extents.list;
            inode.data_fork.extents.list = nullptr;
        } else if (inode.data_fork.format == XFS_DINODE_FMT_BTREE) {
            delete[] inode.data_fork.btree.root;
            inode.data_fork.btree.root = nullptr;
        }
        invalidate_bdev(&dev);
        return ok;
    };

    uint32_t const INLINE_CAPACITY = data_fork_record_capacity(&inode);
    if (INLINE_CAPACITY == 0) {
        return cleanup(false);
    }
    uint32_t const LEAF_CAPACITY = bmbt_leaf_max_recs(&mount);
    if (LEAF_CAPACITY <= INLINE_CAPACITY) {
        return cleanup(false);
    }
    auto extent_startoff = [](uint32_t idx) -> xfs_fileoff_t { return (static_cast<xfs_fileoff_t>(idx) + 1) * 2; };
    auto expected_metadata_blocks = [LEAF_CAPACITY](uint32_t extent_count) -> uint64_t {
        uint32_t const LEAF_COUNT = 1U + ((extent_count - 1U) / LEAF_CAPACITY);
        return LEAF_COUNT + (LEAF_COUNT > 1 ? 1U : 0U);
    };

    for (uint32_t i = 0; i < INLINE_CAPACITY + 1; i++) {
        XfsTransaction* tp = xfs_trans_alloc(&mount);
        if (tp == nullptr) {
            return cleanup(false);
        }
        XfsBmbtIrec const REC{.br_startoff = extent_startoff(i),
                              .br_startblock = static_cast<xfs_fsblock_t>(1000 + (i * 3)),
                              .br_blockcount = 1,
                              .br_unwritten = false};
        int const RC = xfs_bmap_add_extent(&inode, tp, REC);
        xfs_trans_cancel(tp);
        if (RC != 0) {
            return cleanup(false);
        }
        if (i + 1 == INLINE_CAPACITY &&
            (inode.data_fork.format != XFS_DINODE_FMT_EXTENTS || inode.nextents != INLINE_CAPACITY || inode.nblocks != 0)) {
            return cleanup(false);
        }
    }

    if (inode.data_fork.format != XFS_DINODE_FMT_BTREE || inode.nextents != INLINE_CAPACITY + 1 ||
        inode.nblocks != expected_metadata_blocks(INLINE_CAPACITY + 1)) {
        return cleanup(false);
    }
    if (inode.data_fork.btree.level != 1 || inode.data_fork.btree.numrecs != 1) {
        return cleanup(false);
    }
    uint32_t const BMDR_MAXRECS = data_fork_bmdr_capacity(&inode);
    size_t const EXPECTED_ROOT_SIZE = bmdr_root_min_size(BMDR_MAXRECS, 1);
    if (BMDR_MAXRECS == 0 || inode.data_fork.btree.root_size < EXPECTED_ROOT_SIZE || inode.data_fork.btree.root == nullptr) {
        return cleanup(false);
    }
    Be64 promoted_child{};
    __builtin_memcpy(&promoted_child, bmdr_ptr_addr(inode.data_fork.btree.root, BMDR_MAXRECS, 0), sizeof(promoted_child));
    if (promoted_child.to_cpu() == NULLFSBLOCK) {
        return cleanup(false);
    }
    if (BMDR_MAXRECS > 1) {
        Be64 compact_slot{};
        __builtin_memcpy(&compact_slot, inode.data_fork.btree.root + sizeof(XfsBmdrBlock) + sizeof(XfsBmbtKey), sizeof(compact_slot));
        if (compact_slot.to_cpu() == promoted_child.to_cpu()) {
            return cleanup(false);
        }
    }
    XfsBmapResult promoted_leading_hole{};
    if (xfs_bmap_lookup(&inode, 0, &promoted_leading_hole) != 0 || !promoted_leading_hole.is_hole ||
        promoted_leading_hole.blockcount != extent_startoff(0)) {
        return cleanup(false);
    }

    uint32_t const TARGET_EXTENTS = LEAF_CAPACITY + 2;
    for (uint32_t i = INLINE_CAPACITY + 1; i < TARGET_EXTENTS; i++) {
        XfsTransaction* tp = xfs_trans_alloc(&mount);
        if (tp == nullptr) {
            return cleanup(false);
        }
        XfsBmbtIrec const REC{.br_startoff = extent_startoff(i),
                              .br_startblock = static_cast<xfs_fsblock_t>(1000 + (i * 3)),
                              .br_blockcount = 1,
                              .br_unwritten = false};
        int const RC = xfs_bmap_add_extent(&inode, tp, REC);
        xfs_trans_cancel(tp);
        if (RC != 0 || inode.data_fork.format != XFS_DINODE_FMT_BTREE || inode.nextents != i + 1 ||
            inode.nblocks != expected_metadata_blocks(i + 1)) {
            return cleanup(false);
        }
    }

    if (inode.data_fork.btree.level != 2 || inode.data_fork.btree.numrecs != 1 || inode.nblocks != 3) {
        return cleanup(false);
    }
    Be64 internal_root_ptr{};
    __builtin_memcpy(&internal_root_ptr, bmdr_ptr_addr(inode.data_fork.btree.root, BMDR_MAXRECS, 0), sizeof(internal_root_ptr));
    if (internal_root_ptr.to_cpu() == NULLFSBLOCK) {
        return cleanup(false);
    }
    BufHead* internal_root = xfs_buf_read(&mount, internal_root_ptr.to_cpu());
    if (internal_root == nullptr) {
        return cleanup(false);
    }
    auto* internal_hdr = reinterpret_cast<XfsBtreeLblock*>(internal_root->data);
    uint32_t const INTERNAL_MAGIC = internal_hdr->bb_magic.to_cpu();
    uint16_t const INTERNAL_LEVEL = internal_hdr->bb_level.to_cpu();
    uint16_t const INTERNAL_RECS = internal_hdr->bb_numrecs.to_cpu();
    brelse(internal_root);
    if (INTERNAL_MAGIC != XFS_BMAP_CRC_MAGIC || INTERNAL_LEVEL != 1 || INTERNAL_RECS != 2) {
        return cleanup(false);
    }
    XfsBmapResult leading_hole{};
    if (xfs_bmap_lookup(&inode, 0, &leading_hole) != 0 || !leading_hole.is_hole || leading_hole.blockcount != extent_startoff(0)) {
        return cleanup(false);
    }

    uint32_t const EXPECTED_EXTENTS = TARGET_EXTENTS;
    auto* listed = new (std::nothrow) XfsBmbtIrec[EXPECTED_EXTENTS];
    if (listed == nullptr) {
        return cleanup(false);
    }
    auto cleanup_listed = [&](bool ok) -> bool {
        delete[] listed;
        return cleanup(ok);
    };

    int const LISTED = xfs_bmap_list_extents(&inode, listed, EXPECTED_EXTENTS);
    if (std::cmp_not_equal(LISTED, EXPECTED_EXTENTS)) {
        return cleanup_listed(false);
    }

    for (uint32_t i = 0; i < EXPECTED_EXTENTS; i++) {
        auto const EXPECTED_OFF = extent_startoff(i);
        xfs_fsblock_t const EXPECTED_BLOCK = 1000 + (i * 3);
        if (listed[i].br_startoff != EXPECTED_OFF || listed[i].br_startblock != EXPECTED_BLOCK || listed[i].br_blockcount != 1) {
            return cleanup_listed(false);
        }

        XfsBmapResult mapped{};
        if (xfs_bmap_lookup(&inode, EXPECTED_OFF, &mapped) != 0 || mapped.is_hole || mapped.startblock != EXPECTED_BLOCK ||
            mapped.blockcount != 1) {
            return cleanup_listed(false);
        }
        XfsBmapResult hole{};
        xfs_filblks_t const EXPECTED_HOLE_BLOCKS =
            i + 1 < EXPECTED_EXTENTS ? extent_startoff(i + 1) - (EXPECTED_OFF + 1) : ~static_cast<xfs_filblks_t>(0);
        if (xfs_bmap_lookup(&inode, EXPECTED_OFF + 1, &hole) != 0 || !hole.is_hole || hole.blockcount != EXPECTED_HOLE_BLOCKS) {
            return cleanup_listed(false);
        }
    }

    return cleanup_listed(true);
}

}  // namespace ker::vfs::xfs
