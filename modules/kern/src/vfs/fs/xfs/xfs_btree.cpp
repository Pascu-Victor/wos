// XFS B+Tree infrastructure - generic traversal and lookup engine.
//
// Implements cursor-based B+tree operations for all XFS btree types.
// The algorithm follows the Linux XFS implementation in
// reference/xfs/libxfs/xfs_btree.c but expressed as C++ templates.
//
// Key design:
//   - Template parameter Traits provides key/record types and comparison.
//   - Cursor holds per-level buffer references and 1-based record index.
//   - Lookup does binary search at each level from root to leaf.
//   - Increment/decrement walk up then back down the tree.

#include "xfs_btree.hpp"

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <util/crc32c.hpp>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_alloc.hpp>
#include <vfs/fs/xfs/xfs_trans.hpp>

#include "net/endian.hpp"
#include "vfs/fs/xfs/xfs_format.hpp"
#include "vfs/fs/xfs/xfs_mount.hpp"

namespace ker::vfs::xfs {

namespace {

auto valid_btree_level(int level) -> bool { return level >= 0 && level < XFS_BTREE_MAXLEVELS; }

auto valid_btree_depth(uint8_t nlevels) -> bool { return nlevels > 0 && nlevels <= XFS_BTREE_MAXLEVELS; }

template <typename Traits>
auto btree_node_max_keys(uint32_t block_size) -> int {
    return static_cast<int>((block_size - Traits::HDR_LEN) / (Traits::KEY_LEN + Traits::PTR_LEN));
}

template <typename Traits>
auto btree_node_ptr_off(uint32_t block_size, size_t n) -> size_t {
    return Traits::HDR_LEN + (static_cast<size_t>(btree_node_max_keys<Traits>(block_size)) * Traits::KEY_LEN) + (n * Traits::PTR_LEN);
}

template <typename Traits>
auto btree_owner(const XfsBtreeCursor<Traits>* cur) -> uint64_t {
    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        return cur->agno;
    } else {
        return cur->owner;
    }
}

}  // namespace

// ============================================================================
// Cursor helper methods
// ============================================================================

template <typename Traits>
auto XfsBtreeCursor<Traits>::numrecs(int level) const -> int {
    if (!valid_btree_level(level)) {
        return 0;
    }
    if (level_at(level).bp == nullptr) {
        return 0;
    }
    const auto* data = level_at(level).bp->data;

    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        const auto* hdr = reinterpret_cast<const XfsBtreeSblock*>(data);
        return hdr->bb_numrecs.to_cpu();
    } else {
        const auto* hdr = reinterpret_cast<const XfsBtreeLblock*>(data);
        return hdr->bb_numrecs.to_cpu();
    }
}

template <typename Traits>
auto XfsBtreeCursor<Traits>::key_at(int level, int idx) const -> const Key* {
    // Keys start right after the header, at 1-based index idx.
    const uint8_t* base = level_at(level).bp->data + Traits::HDR_LEN;
    return reinterpret_cast<const Key*>(base + (static_cast<size_t>(idx - 1) * Traits::KEY_LEN));
}

template <typename Traits>
auto XfsBtreeCursor<Traits>::rec_at(int idx) const -> const Rec* {
    // Records are at leaf level (level 0), starting after header
    const uint8_t* base = level_at(0).bp->data + Traits::HDR_LEN;
    return reinterpret_cast<const Rec*>(base + (static_cast<size_t>(idx - 1) * Traits::REC_LEN));
}

template <typename Traits>
auto XfsBtreeCursor<Traits>::ptr_at(int level, int idx) const -> uint64_t {
    // XFS stores node pointers after the maximum key array, not after the
    // current bb_numrecs key count.
    const uint8_t* ptr_addr = level_at(level).bp->data + btree_node_ptr_off<Traits>(mount->block_size, static_cast<size_t>(idx - 1));

    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        Be32 val{};
        __builtin_memcpy(&val, ptr_addr, 4);
        return val.to_cpu();
    } else {
        Be64 val{};
        __builtin_memcpy(&val, ptr_addr, 8);
        return val.to_cpu();
    }
}

template <typename Traits>
auto XfsBtreeCursor<Traits>::read_block(int level, uint64_t blockno) -> int {
    if (!valid_btree_level(level)) {
        return -EINVAL;
    }

    // Release existing buffer at this level
    if (level_at(level).bp != nullptr) {
        brelse(level_at(level).bp);
        level_at(level).bp = nullptr;
    }

    // For short-form (AG) btrees, convert AG-relative block to absolute
    // NOLINTNEXTLINE(misc-const-correctness)
    uint64_t abs_block = blockno;
    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        abs_block = xfs_agbno_to_fsbno(agno, static_cast<xfs_agblock_t>(blockno), mount->ag_blk_log);
    }

    BufHead* bh = xfs_buf_read(mount, abs_block);
    if (bh == nullptr) {
        return -EIO;
    }

    // Validate magic number
    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        const auto* hdr = reinterpret_cast<const XfsBtreeSblock*>(bh->data);
        if (hdr->bb_magic.to_cpu() != Traits::MAGIC) {
            mod::dbg::log("[xfs btree] bad magic 0x%x (expected 0x%x) at block %lu level %d\n", hdr->bb_magic.to_cpu(), Traits::MAGIC,
                          static_cast<unsigned long>(abs_block), level);
            brelse(bh);
            return -EINVAL;
        }
    } else {
        const auto* hdr = reinterpret_cast<const XfsBtreeLblock*>(bh->data);
        if (hdr->bb_magic.to_cpu() != Traits::MAGIC) {
            mod::dbg::log("[xfs btree] bad magic 0x%x (expected 0x%x) at block %lu level %d\n", hdr->bb_magic.to_cpu(), Traits::MAGIC,
                          static_cast<unsigned long>(abs_block), level);
            brelse(bh);
            return -EINVAL;
        }
    }

    level_at(level).bp = bh;
    return 0;
}

template <typename Traits>
auto XfsBtreeCursor<Traits>::left_sibling(int level) const -> uint64_t {
    if (!valid_btree_level(level)) {
        return (Traits::TYPE == XfsBtreeType::SHORT) ? NULLAGBLOCK : NULLFSBLOCK;
    }
    if (level_at(level).bp == nullptr) {
        return (Traits::TYPE == XfsBtreeType::SHORT) ? NULLAGBLOCK : NULLFSBLOCK;
    }
    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        const auto* hdr = reinterpret_cast<const XfsBtreeSblock*>(level_at(level).bp->data);
        return hdr->bb_leftsib.to_cpu();
    } else {
        const auto* hdr = reinterpret_cast<const XfsBtreeLblock*>(level_at(level).bp->data);
        return hdr->bb_leftsib.to_cpu();
    }
}

template <typename Traits>
auto XfsBtreeCursor<Traits>::right_sibling(int level) const -> uint64_t {
    if (!valid_btree_level(level)) {
        return (Traits::TYPE == XfsBtreeType::SHORT) ? NULLAGBLOCK : NULLFSBLOCK;
    }
    if (level_at(level).bp == nullptr) {
        return (Traits::TYPE == XfsBtreeType::SHORT) ? NULLAGBLOCK : NULLFSBLOCK;
    }
    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        const auto* hdr = reinterpret_cast<const XfsBtreeSblock*>(level_at(level).bp->data);
        return hdr->bb_rightsib.to_cpu();
    } else {
        const auto* hdr = reinterpret_cast<const XfsBtreeLblock*>(level_at(level).bp->data);
        return hdr->bb_rightsib.to_cpu();
    }
}

// ============================================================================
// Lookup
// ============================================================================

template <typename Traits>
auto xfs_btree_lookup(XfsBtreeCursor<Traits>* cur, uint64_t root_block, uint8_t nlevels, const typename Traits::IRec& target,
                      XfsBtreeLookup dir) -> int {
    using Key = Traits::Key;

    if (cur == nullptr || cur->mount == nullptr || !valid_btree_depth(nlevels)) {
        return -EINVAL;
    }

    cur->nlevels = nlevels;

    // Read the root block
    int rc = cur->read_block(nlevels - 1, root_block);
    if (rc != 0) {
        return rc;
    }

    // Descend from root (level nlevels-1) to leaf (level 0)
    int cmp_r = 0;  // result of last comparison
    for (int level = nlevels - 1; level >= 0; level--) {
        int const NR = cur->numrecs(level);
        if (NR == 0) {
            // Empty block - tree is empty or corrupted
            cur->level_at(level).ptr = 0;
            return -ENOENT;
        }

        // Binary search within this block
        int low = 1;
        int high = NR;
        int keyno = 1;
        cmp_r = 0;

        while (low <= high) {
            keyno = (low + high) / 2;

            const Key* search_key = nullptr;
            Key synth_key;

            if (level == 0) {
                // At leaf: synthesize key from record
                const auto* rec = cur->rec_at(keyno);
                Traits::init_key_from_rec(&synth_key, rec);
                search_key = &synth_key;
            } else {
                // At internal node: use stored key directly
                search_key = cur->key_at(level, keyno);
            }

            cmp_r = Traits::cmp_key(search_key, target);

            if (cmp_r < 0) {
                low = keyno + 1;
            } else if (cmp_r > 0) {
                high = keyno - 1;
            } else {
                // Exact match
                break;
            }
        }

        // Position cursor: at internal nodes, if we overshot (cmp_r > 0),
        // we want the key just before this one (the last key <= target).
        if (level > 0) {
            if (cmp_r > 0 && keyno > 1) {
                keyno--;
            }
            cur->level_at(level).ptr = keyno;

            // Follow child pointer down to next level
            uint64_t const CHILD_BLOCK = cur->ptr_at(level, keyno);
            rc = cur->read_block(level - 1, CHILD_BLOCK);
            if (rc != 0) {
                return rc;
            }
        } else {
            // Leaf level: set position based on direction
            if (cmp_r > 0) {
                // key > target: the record at keyno is bigger than what we want
                keyno--;
            }
            cur->level_at(0).ptr = keyno;
        }
    }

    // Adjust position based on lookup direction
    int ptr = cur->level_at(0).ptr;
    int nr = cur->numrecs(0);

    switch (dir) {
        case XfsBtreeLookup::LE:
            // We want the largest record <= target
            if (ptr < 1) {
                return -ENOENT;
            }
            if (ptr > nr) {
                cur->level_at(0).ptr = nr;
            }
            break;

        case XfsBtreeLookup::EQ:
            // Must be exact match
            if (ptr < 1 || ptr > nr) {
                return -ENOENT;
            }
            // Verify it's actually an exact match
            {
                Key synth_key;
                Traits::init_key_from_rec(&synth_key, cur->rec_at(ptr));
                if (Traits::cmp_key(&synth_key, target) != 0) {
                    return -ENOENT;
                }
            }
            break;

        case XfsBtreeLookup::GE:
            // We want the smallest record >= target
            if (ptr < 1) {
                ptr = 1;
                cur->level_at(0).ptr = 1;
            }
            if (ptr > nr) {
                // Need to move to next block
                rc = xfs_btree_increment(cur);
                if (rc != 0) {
                    return rc;
                }
            } else {
                // Check if current record is actually >= target
                Key synth_key;
                Traits::init_key_from_rec(&synth_key, cur->rec_at(ptr));
                if (Traits::cmp_key(&synth_key, target) < 0) {
                    // Current record is less than target, advance
                    if (ptr < nr) {
                        cur->level_at(0).ptr = ptr + 1;
                    } else {
                        rc = xfs_btree_increment(cur);
                        if (rc != 0) {
                            return rc;
                        }
                    }
                }
            }
            break;
    }

    // Final validation: cursor must point to a valid record
    ptr = cur->level_at(0).ptr;
    nr = cur->numrecs(0);
    if (ptr < 1 || ptr > nr) {
        return -ENOENT;
    }

    return 0;
}

// ============================================================================
// Increment - move to next record
// ============================================================================

template <typename Traits>
auto xfs_btree_increment(XfsBtreeCursor<Traits>* cur) -> int {
    // Try to advance within the current leaf block
    int ptr = cur->level_at(0).ptr + 1;
    if (ptr <= cur->numrecs(0)) {
        cur->level_at(0).ptr = ptr;
        return 0;
    }

    // Walk up the tree until we find a level where we can advance
    int level = 0;
    for (level = 1; level < cur->nlevels; level++) {
        ptr = cur->level_at(level).ptr + 1;
        if (ptr <= cur->numrecs(level)) {
            cur->level_at(level).ptr = ptr;
            break;
        }
    }

    if (level >= cur->nlevels) {
        // We've exhausted the entire tree
        return -ENOENT;
    }

    // Walk back down from 'level' to leaf, always taking the leftmost child
    for (int lev = level - 1; lev >= 0; lev--) {
        uint64_t const CHILD_BLOCK = cur->ptr_at(lev + 1, cur->level_at(lev + 1).ptr);
        int const RC = cur->read_block(lev, CHILD_BLOCK);
        if (RC != 0) {
            return RC;
        }
        cur->level_at(lev).ptr = 1;  // leftmost entry
    }

    return 0;
}

// ============================================================================
// Decrement - move to previous record
// ============================================================================

template <typename Traits>
auto xfs_btree_decrement(XfsBtreeCursor<Traits>* cur) -> int {
    // Try to back up within the current leaf block
    int ptr = cur->level_at(0).ptr - 1;
    if (ptr >= 1) {
        cur->level_at(0).ptr = ptr;
        return 0;
    }

    // Walk up the tree until we find a level where we can back up
    int level = 0;
    for (level = 1; level < cur->nlevels; level++) {
        ptr = cur->level_at(level).ptr - 1;
        if (ptr >= 1) {
            cur->level_at(level).ptr = ptr;
            break;
        }
    }

    if (level >= cur->nlevels) {
        // At the very beginning of the tree
        return -ENOENT;
    }

    // Walk back down from 'level' to leaf, always taking the rightmost child
    for (int lev = level - 1; lev >= 0; lev--) {
        uint64_t const CHILD_BLOCK = cur->ptr_at(lev + 1, cur->level_at(lev + 1).ptr);
        int const RC = cur->read_block(lev, CHILD_BLOCK);
        if (RC != 0) {
            return RC;
        }
        cur->level_at(lev).ptr = cur->numrecs(lev);  // rightmost entry
    }

    return 0;
}

// ============================================================================
// Get record at current cursor position
// ============================================================================

template <typename Traits>
auto xfs_btree_get_rec(const XfsBtreeCursor<Traits>* cur) -> Traits::IRec {
    int const PTR = cur->level_at(0).ptr;
    const auto* rec = cur->rec_at(PTR);
    return Traits::decode_rec(rec);
}

// ============================================================================
// Mutable cursor helpers
// ============================================================================

template <typename Traits>
auto XfsBtreeCursor<Traits>::rec_at_mut(int idx) -> Rec* {
    uint8_t* base = level_at(0).bp->data + Traits::HDR_LEN;
    return reinterpret_cast<Rec*>(base + (static_cast<size_t>(idx - 1) * Traits::REC_LEN));
}

template <typename Traits>
auto XfsBtreeCursor<Traits>::key_at_mut(int level, int idx) -> Key* {
    uint8_t* base = level_at(level).bp->data + Traits::HDR_LEN;
    return reinterpret_cast<Key*>(base + (static_cast<size_t>(idx - 1) * Traits::KEY_LEN));
}

namespace {

template <typename Traits>
auto checked_key_at_mut(XfsBtreeCursor<Traits>* cur, int level, int idx, typename Traits::Key** out_key) -> int {
    if (out_key == nullptr) {
        return -EINVAL;
    }
    *out_key = nullptr;
    if (cur == nullptr || !valid_btree_level(level)) {
        return -EIO;
    }
    if (cur->level_at(level).bp == nullptr) {
        return -EIO;
    }
    int const NR = cur->numrecs(level);
    if (idx < 1 || idx > NR) {
        return -EIO;
    }
    *out_key = cur->key_at_mut(level, idx);
    return 0;
}

// Update the CRC field of a btree block.
template <typename Traits>
void btree_update_crc(BufHead* bp) {
    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        auto* hdr = reinterpret_cast<XfsBtreeSblock*>(bp->data);
        hdr->bb_crc = 0;
        uint32_t crc = util::crc32c_block_with_cksum(bp->data, bp->size, offsetof(XfsBtreeSblock, bb_crc));
        __builtin_memcpy(&hdr->bb_crc, &crc, sizeof(crc));
    } else {
        auto* hdr = reinterpret_cast<XfsBtreeLblock*>(bp->data);
        hdr->bb_crc = 0;
        uint32_t crc = util::crc32c_block_with_cksum(bp->data, bp->size, offsetof(XfsBtreeLblock, bb_crc));
        __builtin_memcpy(&hdr->bb_crc, &crc, sizeof(crc));
    }
}

template <typename Traits>
auto btree_propagate_first_key(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp, int child_level, const typename Traits::Key& key) -> int {
    if (cur == nullptr || tp == nullptr || child_level < 0 || child_level >= cur->nlevels) {
        return -EINVAL;
    }

    for (int lev = child_level + 1; lev < cur->nlevels; lev++) {
        int const CAPTURE_RC = xfs_trans_capture_buf(tp, cur->level_at(lev).bp);
        if (CAPTURE_RC != 0) {
            return CAPTURE_RC;
        }
        int const PARENT_PTR = cur->level_at(lev).ptr;
        typename Traits::Key* pkey = nullptr;
        int const KEY_RC = checked_key_at_mut(cur, lev, PARENT_PTR, &pkey);
        if (KEY_RC != 0) {
            return KEY_RC;
        }
        __builtin_memcpy(pkey, &key, Traits::KEY_LEN);
        btree_update_crc<Traits>(cur->level_at(lev).bp);
        xfs_trans_log_buf_full(tp, cur->level_at(lev).bp);
        if (PARENT_PTR != 1) {
            break;
        }
    }

    return 0;
}

}  // anonymous namespace

template <typename Traits>
auto XfsBtreeCursor<Traits>::ptr_addr(int level, int idx) -> uint8_t* {
    return level_at(level).bp->data + btree_node_ptr_off<Traits>(mount->block_size, static_cast<size_t>(idx - 1));
}

template <typename Traits>
void XfsBtreeCursor<Traits>::set_ptr(int level, int idx, uint64_t blockno) {
    uint8_t* p = level_at(level).bp->data + btree_node_ptr_off<Traits>(mount->block_size, static_cast<size_t>(idx - 1));

    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        Be32 val = Be32::from_cpu(static_cast<uint32_t>(blockno));
        __builtin_memcpy(p, &val, 4);
    } else {
        Be64 val = Be64::from_cpu(blockno);
        __builtin_memcpy(p, &val, 8);
    }
}

template <typename Traits>
void XfsBtreeCursor<Traits>::set_numrecs(int level, int nrecs) {
    if (level_at(level).bp == nullptr) {
        return;
    }
    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        auto* hdr = reinterpret_cast<XfsBtreeSblock*>(level_at(level).bp->data);
        hdr->bb_numrecs = Be16::from_cpu(static_cast<uint16_t>(nrecs));
    } else {
        auto* hdr = reinterpret_cast<XfsBtreeLblock*>(level_at(level).bp->data);
        hdr->bb_numrecs = Be16::from_cpu(static_cast<uint16_t>(nrecs));
    }
}

// ============================================================================
// B+tree update - overwrite record at current cursor position
// ============================================================================

template <typename Traits>
auto xfs_btree_update(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp, const typename Traits::IRec& irec) -> int {
    int const PTR = cur->level_at(0).ptr;
    int const NR = cur->numrecs(0);
    if (PTR < 1 || PTR > NR || cur->level_at(0).bp == nullptr) {
        return -EINVAL;
    }

    int capture_rc = xfs_trans_capture_buf(tp, cur->level_at(0).bp);
    if (capture_rc != 0) {
        return capture_rc;
    }

    // Encode the in-memory record to on-disk format
    auto* rec = cur->rec_at_mut(PTR);
    typename Traits::Rec new_rec{};
    Traits::encode_rec(&new_rec, irec);
    __builtin_memcpy(rec, &new_rec, Traits::REC_LEN);

    // Update the key at parent levels if the first record was modified
    if (PTR == 1) {
        typename Traits::Key key;
        Traits::init_key_from_rec(&key, rec);
        for (int lev = 1; lev < cur->nlevels; lev++) {
            capture_rc = xfs_trans_capture_buf(tp, cur->level_at(lev).bp);
            if (capture_rc != 0) {
                return capture_rc;
            }
            int const PARENT_PTR = cur->level_at(lev).ptr;
            typename Traits::Key* pkey = nullptr;
            int const KEY_RC = checked_key_at_mut(cur, lev, PARENT_PTR, &pkey);
            if (KEY_RC != 0) {
                return KEY_RC;
            }
            __builtin_memcpy(pkey, &key, Traits::KEY_LEN);
            btree_update_crc<Traits>(cur->level_at(lev).bp);
            xfs_trans_log_buf_full(tp, cur->level_at(lev).bp);
            if (PARENT_PTR != 1) {
                break;  // only propagate if we changed the leftmost key
            }
        }
    }

    btree_update_crc<Traits>(cur->level_at(0).bp);
    xfs_trans_log_buf_full(tp, cur->level_at(0).bp);
    return 0;
}

// ============================================================================
// B+tree insert - add a new record in sorted order
// ============================================================================

namespace {

// Compute max records per leaf block
template <typename Traits>
auto btree_max_recs_leaf(uint32_t block_size) -> int {
    return static_cast<int>((block_size - Traits::HDR_LEN) / Traits::REC_LEN);
}

// Compute max keys+ptrs per internal block
template <typename Traits>
auto btree_max_keys_node(uint32_t block_size) -> int {
    return btree_node_max_keys<Traits>(block_size);
}

// Helper: byte offset from block start to the N-th key (0-based) in a btree block.
// Returns the offset as size_t to avoid repeated cast noise at call sites.
template <typename Traits>
auto btree_key_off(size_t n) -> size_t {
    return Traits::HDR_LEN + (n * Traits::KEY_LEN);
}

// Helper: byte offset from block start to the N-th pointer (0-based) in an
// internal node that currently holds `nrecs` keys.
template <typename Traits>
auto btree_ptr_off(uint32_t block_size, size_t n) -> size_t {
    return btree_node_ptr_off<Traits>(block_size, n);
}

// Set numrecs directly in the on-disk header without going through the cursor.
template <typename Traits>
void btree_set_numrecs_raw(uint8_t* block_data, int nrecs) {
    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        auto* hdr = reinterpret_cast<XfsBtreeSblock*>(block_data);
        hdr->bb_numrecs = Be16::from_cpu(static_cast<uint16_t>(nrecs));
    } else {
        auto* hdr = reinterpret_cast<XfsBtreeLblock*>(block_data);
        hdr->bb_numrecs = Be16::from_cpu(static_cast<uint16_t>(nrecs));
    }
}

// Allocate a single filesystem block for a new btree node/leaf, zero-initialize
// it, and write the btree block header.  Returns the block number in the same
// coordinate space used by the tree (AG-relative for SHORT, absolute for LONG).
// On success, *out_bh is set to a held buffer (caller must brelse).
// Returns NULLAGBLOCK / NULLFSBLOCK on failure.
template <typename Traits>
auto btree_alloc_new_block(XfsMountContext* mount, XfsTransaction* tp, xfs_agnumber_t agno, uint8_t level, uint64_t owner, BufHead** out_bh)
    -> uint64_t {
    if (out_bh == nullptr || !valid_btree_level(level)) {
        if (out_bh != nullptr) {
            *out_bh = nullptr;
        }
        if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
            return NULLAGBLOCK;
        } else {
            return NULLFSBLOCK;
        }
    }

    // Always use the AGFL for btree block allocation.  Falling back to
    // xfs_alloc_extent here would re-enter the bnobt/cntbt insert path
    // while we are already mid-split, invalidating the caller's cursor
    // buffers and corrupting the trees.  mkfs pre-fills the AGFL exactly
    // to cover the worst-case split depth, so AGFL-empty means something
    // is seriously wrong and we should fail cleanly.
    xfs_agblock_t agbno = NULLAGBLOCK;
    if (xfs_alloc_get_freelist(mount, tp, agno, &agbno) != 0) {
        mod::dbg::log("[xfs btree] btree_alloc_new_block: agno=%u AGFL empty (flcount=0)\n", agno);
        *out_bh = nullptr;
        if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
            return NULLAGBLOCK;
        } else {
            return NULLFSBLOCK;
        }
    }

    if (agbno == NULLAGBLOCK || agbno >= mount->ag_blocks) {
        mod::dbg::log("[xfs btree] btree_alloc_new_block: bad agbno=0x%x from AGFL (ag_blocks=%u agno=%u) - dropping\n", agbno,
                      mount->ag_blocks, agno);
        // Do not put the bad block back - it would corrupt the free space trees.
        *out_bh = nullptr;
        if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
            return NULLAGBLOCK;
        } else {
            return NULLFSBLOCK;
        }
    }

    uint64_t const ABS_BLOCK = xfs_agbno_to_fsbno(agno, agbno, mount->ag_blk_log);
    BufHead* bh = xfs_buf_get(mount, ABS_BLOCK);
    if (bh == nullptr) {
        xfs_alloc_put_freelist(mount, tp, agno, agbno);
        *out_bh = nullptr;
        if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
            return NULLAGBLOCK;
        } else {
            return NULLFSBLOCK;
        }
    }

    // Zero the block and write the header
    __builtin_memset(bh->data, 0, bh->size);

    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        auto* hdr = reinterpret_cast<XfsBtreeSblock*>(bh->data);
        hdr->bb_magic = Be32::from_cpu(Traits::MAGIC);
        hdr->bb_level = Be16::from_cpu(level);
        hdr->bb_numrecs = Be16::from_cpu(0);
        hdr->bb_leftsib = Be32::from_cpu(NULLAGBLOCK);
        hdr->bb_rightsib = Be32::from_cpu(NULLAGBLOCK);
        hdr->bb_blkno = Be64::from_cpu(ABS_BLOCK * (mount->block_size / mount->sect_size));
        hdr->bb_owner = Be32::from_cpu(static_cast<uint32_t>(owner));
        hdr->bb_uuid = mount->uuid;
    } else {
        auto* hdr = reinterpret_cast<XfsBtreeLblock*>(bh->data);
        hdr->bb_magic = Be32::from_cpu(Traits::MAGIC);
        hdr->bb_level = Be16::from_cpu(level);
        hdr->bb_numrecs = Be16::from_cpu(0);
        hdr->bb_leftsib = Be64::from_cpu(NULLFSBLOCK);
        hdr->bb_rightsib = Be64::from_cpu(NULLFSBLOCK);
        hdr->bb_blkno = Be64::from_cpu(ABS_BLOCK * (mount->block_size / mount->sect_size));
        hdr->bb_owner = Be64::from_cpu(owner);
        hdr->bb_uuid = mount->uuid;
    }

    btree_update_crc<Traits>(bh);
    *out_bh = bh;

    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        return agbno;
    } else {
        return ABS_BLOCK;
    }
}

// Set the left/right sibling field in a btree block header.
template <typename Traits>
void btree_set_leftsib(BufHead* bp, uint64_t sib) {
    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        reinterpret_cast<XfsBtreeSblock*>(bp->data)->bb_leftsib = Be32::from_cpu(static_cast<uint32_t>(sib));
    } else {
        reinterpret_cast<XfsBtreeLblock*>(bp->data)->bb_leftsib = Be64::from_cpu(sib);
    }
}

template <typename Traits>
void btree_set_rightsib(BufHead* bp, uint64_t sib) {
    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        reinterpret_cast<XfsBtreeSblock*>(bp->data)->bb_rightsib = Be32::from_cpu(static_cast<uint32_t>(sib));
    } else {
        reinterpret_cast<XfsBtreeLblock*>(bp->data)->bb_rightsib = Be64::from_cpu(sib);
    }
}

// Write a btree child pointer at position idx (1-based) in an internal node,
// using an explicit nrecs value for the layout calculation.
template <typename Traits>
void btree_write_ptr(uint8_t* block_data, uint32_t block_size, int idx, uint64_t blockno) {
    uint8_t* p = block_data + btree_ptr_off<Traits>(block_size, static_cast<size_t>(idx - 1));
    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        Be32 val = Be32::from_cpu(static_cast<uint32_t>(blockno));
        __builtin_memcpy(p, &val, 4);
    } else {
        Be64 val = Be64::from_cpu(blockno);
        __builtin_memcpy(p, &val, 8);
    }
}

template <typename Traits>
auto btree_blockno(const XfsBtreeCursor<Traits>* cur, const BufHead* bp) -> uint64_t {
    uint64_t const ABS_BLOCK = bp->block_no / (cur->mount->block_size / cur->mount->sect_size);
    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        return ABS_BLOCK % cur->mount->ag_blocks;
    } else {
        return ABS_BLOCK;
    }
}

// Forward declaration for mutual recursion
template <typename Traits>
auto btree_insert_into_parent(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp, int lev, const typename Traits::Key& new_key,
                              uint64_t left_ptr, uint64_t new_ptr, uint64_t root_block, uint8_t nlevels, uint64_t* new_root,
                              uint8_t* new_nlevels) -> int;

// Split a full internal node at level `lev`.  The cursor's level_at(lev).ptr
// indicates the current child position.  After the split the new key/pointer
// are inserted into the right or left half as appropriate, and the promoted
// middle key plus the new sibling's block number are passed up.
template <typename Traits>
auto btree_split_internal(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp, int lev, int insert_pos, const typename Traits::Key& insert_key,
                          uint64_t insert_ptr, uint64_t root_block, uint8_t nlevels, uint64_t* new_root, uint8_t* new_nlevels) -> int {
    BufHead* left_bp = cur->level_at(lev).bp;
    if (left_bp == nullptr) {
        return -EIO;
    }
    int capture_rc = xfs_trans_capture_buf(tp, left_bp);
    if (capture_rc != 0) {
        return capture_rc;
    }
    int const NR = cur->numrecs(lev);  // should == max_keys (full)
    int const MID = NR / 2;            // left keeps [1..mid], right gets [mid+1..nr]

    // Determine owner for new block (AG number for SHORT, inode for LONG)
    uint64_t const OWNER = btree_owner(cur);

    BufHead* right_bp = nullptr;
    uint64_t const RIGHT_BLOCKNO = btree_alloc_new_block<Traits>(cur->mount, tp, cur->agno, static_cast<uint8_t>(lev), OWNER, &right_bp);
    if (right_bp == nullptr) {
        return -ENOSPC;
    }

    uint8_t* left_data = left_bp->data;
    uint8_t* right_data = right_bp->data;

    // Copy keys [mid+1..nr] => right block keys [1..nr-mid]
    int const RIGHT_NR = NR - MID;
    __builtin_memcpy(right_data + Traits::HDR_LEN, left_data + btree_key_off<Traits>(static_cast<size_t>(MID)),
                     static_cast<size_t>(RIGHT_NR) * Traits::KEY_LEN);

    // Copy pointers [mid+1..nr] => right block pointers [1..nr-mid].
    const uint8_t* left_ptr_base = left_data + btree_ptr_off<Traits>(cur->mount->block_size, 0);
    uint8_t* right_ptr_base = right_data + btree_ptr_off<Traits>(cur->mount->block_size, 0);
    __builtin_memcpy(right_ptr_base, left_ptr_base + (static_cast<size_t>(MID) * Traits::PTR_LEN),
                     static_cast<size_t>(RIGHT_NR) * Traits::PTR_LEN);

    btree_set_numrecs_raw<Traits>(left_data, MID);
    btree_set_numrecs_raw<Traits>(right_data, RIGHT_NR);

    // Fixup sibling chain
    uint64_t const OLD_RIGHT_SIB = [&]() -> uint64_t {
        if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
            return reinterpret_cast<XfsBtreeSblock*>(left_data)->bb_rightsib.to_cpu();
        } else {
            return reinterpret_cast<XfsBtreeLblock*>(left_data)->bb_rightsib.to_cpu();
        }
    }();

    // Derive left block's own block number for right->leftsib
    uint64_t const LEFT_BLOCKNO = btree_blockno<Traits>(cur, left_bp);

    btree_set_rightsib<Traits>(left_bp, RIGHT_BLOCKNO);
    btree_set_leftsib<Traits>(right_bp, LEFT_BLOCKNO);
    btree_set_rightsib<Traits>(right_bp, OLD_RIGHT_SIB);

    // Update old right sibling's leftsib to point to the new right block
    constexpr uint64_t NULL_SIB =
        (Traits::TYPE == XfsBtreeType::SHORT) ? static_cast<uint64_t>(NULLAGBLOCK) : static_cast<uint64_t>(NULLFSBLOCK);
    if (OLD_RIGHT_SIB != NULL_SIB) {
        uint64_t const ABS_OLD = (Traits::TYPE == XfsBtreeType::SHORT)
                                     ? xfs_agbno_to_fsbno(cur->agno, static_cast<xfs_agblock_t>(OLD_RIGHT_SIB), cur->mount->ag_blk_log)
                                     : OLD_RIGHT_SIB;
        BufHead* old_right_bh = xfs_buf_read(cur->mount, ABS_OLD);
        if (old_right_bh != nullptr) {
            capture_rc = xfs_trans_capture_buf(tp, old_right_bh);
            if (capture_rc != 0) {
                brelse(old_right_bh);
                brelse(right_bp);
                return capture_rc;
            }
            btree_set_leftsib<Traits>(old_right_bh, RIGHT_BLOCKNO);
            btree_update_crc<Traits>(old_right_bh);
            xfs_trans_log_buf_full(tp, old_right_bh);
            brelse(old_right_bh);
        }
    }

    // Insert the new key/ptr into the correct half
    if (insert_pos <= MID + 1) {
        // Insert into left half
        int const LEFT_NR_CUR = MID;
        uint8_t* lp_base = left_data + btree_ptr_off<Traits>(cur->mount->block_size, 0);
        // Step 1: shift keys right to make room at insert_pos-1 (0-based)
        if (insert_pos <= LEFT_NR_CUR) {
            std::memmove(left_data + btree_key_off<Traits>(static_cast<size_t>(insert_pos)),
                         left_data + btree_key_off<Traits>(static_cast<size_t>(insert_pos - 1)),
                         static_cast<size_t>(LEFT_NR_CUR - insert_pos + 1) * Traits::KEY_LEN);
        }
        // Step 2: shift ptrs to make room at insert_pos-1 (0-based)
        if (insert_pos <= LEFT_NR_CUR) {
            std::memmove(lp_base + (static_cast<size_t>(insert_pos) * Traits::PTR_LEN),
                         lp_base + (static_cast<size_t>(insert_pos - 1) * Traits::PTR_LEN),
                         static_cast<size_t>(LEFT_NR_CUR - insert_pos + 1) * Traits::PTR_LEN);
        }
        // Step 3: write new key and ptr
        __builtin_memcpy(left_data + btree_key_off<Traits>(static_cast<size_t>(insert_pos - 1)), &insert_key, Traits::KEY_LEN);
        btree_write_ptr<Traits>(left_data, cur->mount->block_size, insert_pos, insert_ptr);
        btree_set_numrecs_raw<Traits>(left_data, LEFT_NR_CUR + 1);
    } else {
        // Insert into right half
        int const RIGHT_INSERT = insert_pos - MID;  // 1-based in right block
        int const RIGHT_NR_CUR = RIGHT_NR;
        uint8_t* rp_base = right_data + btree_ptr_off<Traits>(cur->mount->block_size, 0);
        // Step 1: shift keys right in right block
        if (RIGHT_INSERT <= RIGHT_NR_CUR) {
            std::memmove(right_data + btree_key_off<Traits>(static_cast<size_t>(RIGHT_INSERT)),
                         right_data + btree_key_off<Traits>(static_cast<size_t>(RIGHT_INSERT - 1)),
                         static_cast<size_t>(RIGHT_NR_CUR - RIGHT_INSERT + 1) * Traits::KEY_LEN);
        }
        // Step 2: shift ptrs to make room at right_insert-1 (0-based)
        if (RIGHT_INSERT <= RIGHT_NR_CUR) {
            std::memmove(rp_base + (static_cast<size_t>(RIGHT_INSERT) * Traits::PTR_LEN),
                         rp_base + (static_cast<size_t>(RIGHT_INSERT - 1) * Traits::PTR_LEN),
                         static_cast<size_t>(RIGHT_NR_CUR - RIGHT_INSERT + 1) * Traits::PTR_LEN);
        }
        // Step 3: write new key and ptr
        __builtin_memcpy(right_data + btree_key_off<Traits>(static_cast<size_t>(RIGHT_INSERT - 1)), &insert_key, Traits::KEY_LEN);
        btree_write_ptr<Traits>(right_data, cur->mount->block_size, RIGHT_INSERT, insert_ptr);
        btree_set_numrecs_raw<Traits>(right_data, RIGHT_NR_CUR + 1);
    }

    typename Traits::Key promoted_key;
    __builtin_memcpy(&promoted_key, right_data + Traits::HDR_LEN, Traits::KEY_LEN);

    if (insert_pos == 1 && lev + 1 < cur->nlevels) {
        typename Traits::Key left_first_key;
        __builtin_memcpy(&left_first_key, left_data + Traits::HDR_LEN, Traits::KEY_LEN);
        int const FIRST_KEY_RC = btree_propagate_first_key<Traits>(cur, tp, lev, left_first_key);
        if (FIRST_KEY_RC != 0) {
            brelse(right_bp);
            return FIRST_KEY_RC;
        }
    }

    btree_update_crc<Traits>(left_bp);
    btree_update_crc<Traits>(right_bp);
    xfs_trans_log_buf_full(tp, left_bp);
    xfs_trans_log_buf_full(tp, right_bp);
    brelse(right_bp);

    // Propagate promoted key and new right sibling block number up
    return btree_insert_into_parent<Traits>(cur, tp, lev + 1, promoted_key, LEFT_BLOCKNO, RIGHT_BLOCKNO, root_block, nlevels, new_root,
                                            new_nlevels);
}

// Insert a new key/pointer pair into the parent node at level `lev`.
// If the parent is also full, splits it recursively.  If lev == nlevels,
// we have exhausted all existing levels and need to grow a new root.
template <typename Traits>
auto btree_insert_into_parent(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp, int lev, const typename Traits::Key& new_key,
                              uint64_t left_ptr, uint64_t new_ptr, uint64_t root_block, uint8_t nlevels, uint64_t* new_root,
                              uint8_t* new_nlevels) -> int {
    if (left_ptr == new_ptr) {
        mod::dbg::log("[xfs btree] split child aliases new child magic=0x%x block=%lu level=%d\n", Traits::MAGIC,
                      static_cast<unsigned long>(left_ptr), lev);
        return -EIO;
    }
    if (lev == cur->nlevels) {
        if (nlevels >= XFS_BTREE_MAXLEVELS) {
            return -EIO;
        }

        // Need a new root one level above the current root.
        BufHead const* old_root_bp = cur->level_at(nlevels - 1).bp;
        uint64_t const OWNER = btree_owner(cur);

        BufHead* new_root_bp = nullptr;
        uint64_t const NEW_ROOT_BLOCKNO = btree_alloc_new_block<Traits>(cur->mount, tp, cur->agno, nlevels, OWNER, &new_root_bp);
        if (new_root_bp == nullptr) {
            return -ENOSPC;
        }

        uint8_t* nr_data = new_root_bp->data;

        // First key = minimum key of old root (left subtree).
        // If the old root was a leaf (nlevels == 1), its first entry is a record;
        // synthesize the key.  If it was an internal node, the first entry IS
        // already a key (no synthesis needed).
        typename Traits::Key first_key{};
        if (nlevels == 1) {
            const auto* first_rec = reinterpret_cast<const Traits::Rec*>(old_root_bp->data + Traits::HDR_LEN);
            Traits::init_key_from_rec(&first_key, first_rec);
        } else {
            __builtin_memcpy(&first_key, old_root_bp->data + Traits::HDR_LEN, Traits::KEY_LEN);
        }
        __builtin_memcpy(nr_data + Traits::HDR_LEN, &first_key, Traits::KEY_LEN);
        __builtin_memcpy(nr_data + btree_key_off<Traits>(1), &new_key, Traits::KEY_LEN);

        // Two pointers (layout: after 2 keys)
        if (left_ptr != root_block) {
            brelse(new_root_bp);
            return -EIO;
        }
        btree_write_ptr<Traits>(nr_data, cur->mount->block_size, 1, left_ptr);
        btree_write_ptr<Traits>(nr_data, cur->mount->block_size, 2, new_ptr);

        btree_set_numrecs_raw<Traits>(nr_data, 2);
        btree_update_crc<Traits>(new_root_bp);
        xfs_trans_log_buf_full(tp, new_root_bp);
        brelse(new_root_bp);

        if (new_root != nullptr) {
            *new_root = NEW_ROOT_BLOCKNO;
        }
        if (new_nlevels != nullptr) {
            *new_nlevels = static_cast<uint8_t>(nlevels + 1);
        }
        cur->nlevels = static_cast<uint8_t>(nlevels + 1);
        return 0;
    }

    // We have a parent block at level_at(lev)
    BufHead* parent_bp = cur->level_at(lev).bp;
    if (parent_bp == nullptr) {
        return -EIO;
    }
    int const CAPTURE_RC = xfs_trans_capture_buf(tp, parent_bp);
    if (CAPTURE_RC != 0) {
        return CAPTURE_RC;
    }
    int const PARENT_NR = cur->numrecs(lev);
    int const MAX_KEYS = btree_max_keys_node<Traits>(cur->mount->block_size);

    // A split can allocate and touch other metadata buffers before it reaches
    // the parent. Do not assume the cursor slot still identifies the block
    // that was split: find that exact child and insert immediately after it.
    // This also prevents a stale slot from duplicating new_ptr while orphaning
    // the real left child from the parent index.
    int left_pos = 0;
    for (int pos = 1; pos <= PARENT_NR; ++pos) {
        uint64_t const CHILD = cur->ptr_at(lev, pos);
        if (CHILD == new_ptr) {
            mod::dbg::log("[xfs btree] parent already references new child magic=0x%x block=%lu level=%d pos=%d\n", Traits::MAGIC,
                          static_cast<unsigned long>(new_ptr), lev, pos);
            return -EIO;
        }
        if (CHILD == left_ptr) {
            if (left_pos != 0) {
                return -EIO;
            }
            left_pos = pos;
        }
    }
    if (left_pos == 0) {
        return -EIO;
    }
    cur->level_at(lev).ptr = left_pos;
    int const INSERT_POS = left_pos + 1;

    if (PARENT_NR < MAX_KEYS) {
        uint8_t* p_data = parent_bp->data;

        uint8_t* ptr_base = p_data + btree_ptr_off<Traits>(cur->mount->block_size, 0);

        // Shift keys [insert_pos..parent_nr] => [insert_pos+1..parent_nr+1]
        if (INSERT_POS <= PARENT_NR) {
            std::memmove(p_data + btree_key_off<Traits>(static_cast<size_t>(INSERT_POS)),
                         p_data + btree_key_off<Traits>(static_cast<size_t>(INSERT_POS - 1)),
                         static_cast<size_t>(PARENT_NR - INSERT_POS + 1) * Traits::KEY_LEN);
        }
        __builtin_memcpy(p_data + btree_key_off<Traits>(static_cast<size_t>(INSERT_POS - 1)), &new_key, Traits::KEY_LEN);

        // Shift pointers within the new location to make room
        if (INSERT_POS <= PARENT_NR) {
            std::memmove(ptr_base + (static_cast<size_t>(INSERT_POS) * Traits::PTR_LEN),
                         ptr_base + (static_cast<size_t>(INSERT_POS - 1) * Traits::PTR_LEN),
                         static_cast<size_t>(PARENT_NR - INSERT_POS + 1) * Traits::PTR_LEN);
        }
        btree_write_ptr<Traits>(p_data, cur->mount->block_size, INSERT_POS, new_ptr);

        cur->set_numrecs(lev, PARENT_NR + 1);
        btree_update_crc<Traits>(parent_bp);
        xfs_trans_log_buf_full(tp, parent_bp);
        return 0;
    }

    // Parent is full - split it
    return btree_split_internal<Traits>(cur, tp, lev, INSERT_POS, new_key, new_ptr, root_block, nlevels, new_root, new_nlevels);
}

// Remove the key/pointer at cursor position from the parent at level `lev`
// after the child block at level `lev-1` was freed.  Recurses upward if the
// parent also becomes empty.
template <typename Traits>
auto btree_remove_from_parent(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp, int lev) -> int {
    if (lev >= cur->nlevels) {
        return 0;
    }

    BufHead* parent_bp = cur->level_at(lev).bp;
    if (parent_bp == nullptr) {
        return -EIO;
    }
    int capture_rc = xfs_trans_capture_buf(tp, parent_bp);
    if (capture_rc != 0) {
        return capture_rc;
    }

    int const REMOVE_POS = cur->level_at(lev).ptr;
    int const PARENT_NR = cur->numrecs(lev);
    uint8_t* p_data = parent_bp->data;

    // Shift keys left to fill gap at remove_pos
    if (REMOVE_POS < PARENT_NR) {
        std::memmove(p_data + btree_key_off<Traits>(static_cast<size_t>(REMOVE_POS - 1)),
                     p_data + btree_key_off<Traits>(static_cast<size_t>(REMOVE_POS)),
                     static_cast<size_t>(PARENT_NR - REMOVE_POS) * Traits::KEY_LEN);
    }

    // Shift pointers left.
    uint8_t* ptr_base = p_data + btree_ptr_off<Traits>(cur->mount->block_size, 0);
    if (REMOVE_POS < PARENT_NR) {
        std::memmove(ptr_base + (static_cast<size_t>(REMOVE_POS - 1) * Traits::PTR_LEN),
                     ptr_base + (static_cast<size_t>(REMOVE_POS) * Traits::PTR_LEN),
                     static_cast<size_t>(PARENT_NR - REMOVE_POS) * Traits::PTR_LEN);
    }

    int const NEW_NR = PARENT_NR - 1;
    cur->set_numrecs(lev, NEW_NR);

    if (NEW_NR == 0) {
        if (lev + 1 >= cur->nlevels) {
            // This IS the root level - don't free it. Leave an empty root in place.
            // Freeing the root block would corrupt pag->agf_cnt_root / agf_bno_root.
            btree_update_crc<Traits>(parent_bp);
            xfs_trans_log_buf_full(tp, parent_bp);
            return 0;
        }
        // Detach the empty internal node, but do not recycle it yet.
        // Transaction cancellation has no buffer before-images, so AGFL
        // publication could let another tree reuse a still-visible block.
        btree_update_crc<Traits>(parent_bp);
        xfs_trans_log_buf_full(tp, parent_bp);
        return btree_remove_from_parent<Traits>(cur, tp, lev + 1);
    }

    // If we removed the first key, propagate new first key to grandparent
    if (REMOVE_POS == 1 && NEW_NR > 0) {
        typename Traits::Key new_first_key;
        __builtin_memcpy(&new_first_key, p_data + Traits::HDR_LEN, Traits::KEY_LEN);
        for (int glev = lev + 1; glev < cur->nlevels; glev++) {
            capture_rc = xfs_trans_capture_buf(tp, cur->level_at(glev).bp);
            if (capture_rc != 0) {
                return capture_rc;
            }
            int const GPTR = cur->level_at(glev).ptr;
            typename Traits::Key* gkey = nullptr;
            int const KEY_RC = checked_key_at_mut(cur, glev, GPTR, &gkey);
            if (KEY_RC != 0) {
                return KEY_RC;
            }
            __builtin_memcpy(gkey, &new_first_key, Traits::KEY_LEN);
            btree_update_crc<Traits>(cur->level_at(glev).bp);
            xfs_trans_log_buf_full(tp, cur->level_at(glev).bp);
            if (GPTR != 1) {
                break;
            }
        }
    }

    btree_update_crc<Traits>(parent_bp);
    xfs_trans_log_buf_full(tp, parent_bp);
    return 0;
}

}  // anonymous namespace

template <typename Traits>
auto xfs_btree_insert(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp, const typename Traits::IRec& irec, uint64_t root_block,
                      uint8_t nlevels, uint64_t* new_root, uint8_t* new_nlevels) -> int {
    using Key = Traits::Key;
    using Rec = Traits::Rec;

    if (cur == nullptr || cur->mount == nullptr || !valid_btree_depth(nlevels)) {
        return -EINVAL;
    }

    if (new_root != nullptr) {
        *new_root = root_block;
    }
    if (new_nlevels != nullptr) {
        *new_nlevels = nlevels;
    }

    // First, lookup the position where the record should be inserted (GE)
    int const RC = xfs_btree_lookup(cur, root_block, nlevels, irec, XfsBtreeLookup::GE);
    int insert_ptr = 0;
    if (RC == -ENOENT) {
        if (cur->level_at(0).bp == nullptr) {
            return -EIO;
        }
        // All records are less than irec - insert at end
        insert_ptr = cur->numrecs(0) + 1;
    } else if (RC == 0) {
        // Cursor is at GE position - insert before it
        insert_ptr = cur->level_at(0).ptr;
        Key existing_key{};
        Traits::init_key_from_rec(&existing_key, cur->rec_at(insert_ptr));
        if (Traits::cmp_key(&existing_key, irec) == 0) {
            mod::dbg::log("[xfs btree] refusing duplicate key magic=0x%x root=%lu level=%u leaf_ptr=%d\n", Traits::MAGIC,
                          static_cast<unsigned long>(root_block), nlevels, insert_ptr);
            return -EEXIST;
        }
    } else {
        return RC;
    }

    // Encode the new record
    Rec new_rec{};
    Traits::encode_rec(&new_rec, irec);

    int const NR = cur->numrecs(0);
    int const MAX_RECS = btree_max_recs_leaf<Traits>(cur->mount->block_size);

    int capture_rc = xfs_trans_capture_buf(tp, cur->level_at(0).bp);
    if (capture_rc != 0) {
        return capture_rc;
    }

    // Reserve the entire split chain before changing the leaf. Running out of
    // AGFL blocks after a leaf split would leave partial topology behind, and
    // transaction cancellation cannot currently restore buffer contents.
    if (NR >= MAX_RECS) {
        uint32_t required_blocks = 1;  // right leaf
        bool split_reaches_root = true;
        for (int lev = 1; lev < cur->nlevels; ++lev) {
            if (cur->numrecs(lev) < btree_max_keys_node<Traits>(cur->mount->block_size)) {
                split_reaches_root = false;
                break;
            }
            required_blocks++;
        }
        if (split_reaches_root) {
            required_blocks++;  // new root
        }
        if (cur->mount->per_ag == nullptr || cur->agno >= cur->mount->ag_count ||
            cur->mount->per_ag[cur->agno].agf_flcount < required_blocks) {
            return -ENOSPC;
        }
    }

    if (NR < MAX_RECS) {
        // Room in current leaf - shift records right and insert
        uint8_t* base = cur->level_at(0).bp->data + Traits::HDR_LEN;
        if (insert_ptr <= NR) {
            // Shift records from insert_ptr..nr to insert_ptr+1..nr+1
            std::memmove(base + (static_cast<size_t>(insert_ptr) * Traits::REC_LEN),
                         base + (static_cast<size_t>(insert_ptr - 1) * Traits::REC_LEN),
                         static_cast<size_t>(NR - insert_ptr + 1) * Traits::REC_LEN);
        }
        // Write the new record
        __builtin_memcpy(base + (static_cast<size_t>(insert_ptr - 1) * Traits::REC_LEN), &new_rec, Traits::REC_LEN);
        cur->set_numrecs(0, NR + 1);
        cur->level_at(0).ptr = insert_ptr;

        btree_update_crc<Traits>(cur->level_at(0).bp);
        xfs_trans_log_buf_full(tp, cur->level_at(0).bp);

        // Update parent keys if we inserted at position 1
        if (insert_ptr == 1) {
            Key key;
            Traits::init_key_from_rec(&key, reinterpret_cast<const Rec*>(base));
            for (int lev = 1; lev < cur->nlevels; lev++) {
                capture_rc = xfs_trans_capture_buf(tp, cur->level_at(lev).bp);
                if (capture_rc != 0) {
                    return capture_rc;
                }
                int const PP = cur->level_at(lev).ptr;
                typename Traits::Key* pkey = nullptr;
                int const KEY_RC = checked_key_at_mut(cur, lev, PP, &pkey);
                if (KEY_RC != 0) {
                    return KEY_RC;
                }
                __builtin_memcpy(pkey, &key, Traits::KEY_LEN);
                btree_update_crc<Traits>(cur->level_at(lev).bp);
                xfs_trans_log_buf_full(tp, cur->level_at(lev).bp);
                if (PP != 1) {
                    break;
                }
            }
        }
        return 0;
    }

    // Leaf is full - split it.
    BufHead* left_bp = cur->level_at(0).bp;
    int const MID = NR / 2;  // left keeps [1..mid], right gets [mid+1..nr]

    uint64_t const OWNER = btree_owner(cur);
    BufHead* right_bp = nullptr;
    uint64_t const RIGHT_BLOCKNO = btree_alloc_new_block<Traits>(cur->mount, tp, cur->agno, 0, OWNER, &right_bp);
    if (right_bp == nullptr) {
        return -ENOSPC;
    }

    uint8_t* left_data = left_bp->data;
    uint8_t* right_data = right_bp->data;

    // Move right half of records to the new block
    int const RIGHT_NR = NR - MID;
    __builtin_memcpy(right_data + Traits::HDR_LEN, left_data + Traits::HDR_LEN + (static_cast<size_t>(MID) * Traits::REC_LEN),
                     static_cast<size_t>(RIGHT_NR) * Traits::REC_LEN);
    btree_set_numrecs_raw<Traits>(left_data, MID);
    btree_set_numrecs_raw<Traits>(right_data, RIGHT_NR);

    // Stitch sibling pointers
    uint64_t const OLD_RIGHT_SIB = [&]() -> uint64_t {
        if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
            return reinterpret_cast<XfsBtreeSblock*>(left_data)->bb_rightsib.to_cpu();
        } else {
            return reinterpret_cast<XfsBtreeLblock*>(left_data)->bb_rightsib.to_cpu();
        }
    }();

    uint64_t const LEFT_BLOCKNO = btree_blockno<Traits>(cur, left_bp);

    btree_set_rightsib<Traits>(left_bp, RIGHT_BLOCKNO);
    btree_set_leftsib<Traits>(right_bp, LEFT_BLOCKNO);
    btree_set_rightsib<Traits>(right_bp, OLD_RIGHT_SIB);

    constexpr uint64_t NULL_SIB =
        (Traits::TYPE == XfsBtreeType::SHORT) ? static_cast<uint64_t>(NULLAGBLOCK) : static_cast<uint64_t>(NULLFSBLOCK);
    if (OLD_RIGHT_SIB != NULL_SIB) {
        uint64_t const ABS_OLD = (Traits::TYPE == XfsBtreeType::SHORT)
                                     ? xfs_agbno_to_fsbno(cur->agno, static_cast<xfs_agblock_t>(OLD_RIGHT_SIB), cur->mount->ag_blk_log)
                                     : OLD_RIGHT_SIB;
        BufHead* old_right_bh = xfs_buf_read(cur->mount, ABS_OLD);
        if (old_right_bh != nullptr) {
            capture_rc = xfs_trans_capture_buf(tp, old_right_bh);
            if (capture_rc != 0) {
                brelse(old_right_bh);
                brelse(right_bp);
                return capture_rc;
            }
            btree_set_leftsib<Traits>(old_right_bh, RIGHT_BLOCKNO);
            btree_update_crc<Traits>(old_right_bh);
            xfs_trans_log_buf_full(tp, old_right_bh);
            brelse(old_right_bh);
        }
    }

    // Insert the new record into the correct half
    if (insert_ptr <= MID + 1) {
        // Insert into left half
        uint8_t* base = left_data + Traits::HDR_LEN;
        int const LEFT_NR = MID;
        if (insert_ptr <= LEFT_NR) {
            std::memmove(base + (static_cast<size_t>(insert_ptr) * Traits::REC_LEN),
                         base + (static_cast<size_t>(insert_ptr - 1) * Traits::REC_LEN),
                         static_cast<size_t>(LEFT_NR - insert_ptr + 1) * Traits::REC_LEN);
        }
        __builtin_memcpy(base + (static_cast<size_t>(insert_ptr - 1) * Traits::REC_LEN), &new_rec, Traits::REC_LEN);
        btree_set_numrecs_raw<Traits>(left_data, LEFT_NR + 1);
        cur->level_at(0).ptr = insert_ptr;
    } else {
        // Insert into right half
        int const RIGHT_INSERT = insert_ptr - MID;  // 1-based in right block
        uint8_t* base = right_data + Traits::HDR_LEN;
        if (RIGHT_INSERT <= RIGHT_NR) {
            std::memmove(base + (static_cast<size_t>(RIGHT_INSERT) * Traits::REC_LEN),
                         base + (static_cast<size_t>(RIGHT_INSERT - 1) * Traits::REC_LEN),
                         static_cast<size_t>(RIGHT_NR - RIGHT_INSERT + 1) * Traits::REC_LEN);
        }
        __builtin_memcpy(base + (static_cast<size_t>(RIGHT_INSERT - 1) * Traits::REC_LEN), &new_rec, Traits::REC_LEN);
        btree_set_numrecs_raw<Traits>(right_data, RIGHT_NR + 1);
        cur->level_at(0).ptr = RIGHT_INSERT;
    }

    // Derive the first key of the right block before releasing the buffer.
    Key right_first_key;
    Traits::init_key_from_rec(&right_first_key, reinterpret_cast<const Rec*>(right_data + Traits::HDR_LEN));

    if (insert_ptr == 1 && cur->nlevels > 1) {
        Key left_first_key;
        Traits::init_key_from_rec(&left_first_key, reinterpret_cast<const Rec*>(left_data + Traits::HDR_LEN));
        int const FIRST_KEY_RC = btree_propagate_first_key<Traits>(cur, tp, 0, left_first_key);
        if (FIRST_KEY_RC != 0) {
            brelse(right_bp);
            return FIRST_KEY_RC;
        }
    }

    btree_update_crc<Traits>(left_bp);
    btree_update_crc<Traits>(right_bp);
    xfs_trans_log_buf_full(tp, left_bp);
    xfs_trans_log_buf_full(tp, right_bp);
    brelse(right_bp);

    // Propagate the split upward
    return btree_insert_into_parent<Traits>(cur, tp, 1, right_first_key, LEFT_BLOCKNO, RIGHT_BLOCKNO, root_block, nlevels, new_root,
                                            new_nlevels);
}

// ============================================================================
// B+tree delete - remove record at current cursor position
// ============================================================================

template <typename Traits>
auto xfs_btree_delete(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp) -> int {
    using Key = Traits::Key;
    using Rec = Traits::Rec;

    int const PTR = cur->level_at(0).ptr;
    int const NR = cur->numrecs(0);
    if (PTR < 1 || PTR > NR || cur->level_at(0).bp == nullptr) {
        return -EINVAL;
    }

    int capture_rc = xfs_trans_capture_buf(tp, cur->level_at(0).bp);
    if (capture_rc != 0) {
        return capture_rc;
    }

    // A multi-level tree can shrink to one record without collapsing its
    // root.  Keep that final leaf linked when deleting the record so a
    // replacement insert still has a valid path from the fixed AGF root.
    // Otherwise btree_remove_from_parent leaves a zero-record internal root,
    // and all subsequent lookups fail before reaching the empty leaf.
    bool preserve_only_leaf = NR == 1 && cur->nlevels > 1;
    for (int lev = 1; preserve_only_leaf && lev < cur->nlevels; ++lev) {
        preserve_only_leaf = cur->numrecs(lev) == 1;
    }

    // Shift records left to fill the gap
    uint8_t* base = cur->level_at(0).bp->data + Traits::HDR_LEN;
    if (PTR < NR) {
        std::memmove(base + (static_cast<size_t>(PTR - 1) * Traits::REC_LEN), base + (static_cast<size_t>(PTR) * Traits::REC_LEN),
                     static_cast<size_t>(NR - PTR) * Traits::REC_LEN);
    }
    cur->set_numrecs(0, NR - 1);

    btree_update_crc<Traits>(cur->level_at(0).bp);
    xfs_trans_log_buf_full(tp, cur->level_at(0).bp);

    // Update parent keys if we deleted the first record
    if (PTR == 1 && NR > 1) {
        Key key;
        Traits::init_key_from_rec(&key, reinterpret_cast<const Rec*>(base));
        for (int lev = 1; lev < cur->nlevels; lev++) {
            capture_rc = xfs_trans_capture_buf(tp, cur->level_at(lev).bp);
            if (capture_rc != 0) {
                return capture_rc;
            }
            int const PP = cur->level_at(lev).ptr;
            typename Traits::Key* pkey = nullptr;
            int const KEY_RC = checked_key_at_mut(cur, lev, PP, &pkey);
            if (KEY_RC != 0) {
                return KEY_RC;
            }
            __builtin_memcpy(pkey, &key, Traits::KEY_LEN);
            btree_update_crc<Traits>(cur->level_at(lev).bp);
            xfs_trans_log_buf_full(tp, cur->level_at(lev).bp);
            if (PP != 1) {
                break;
            }
        }
    }

    if (NR - 1 == 0) {
        // Leaf is now empty.
        BufHead* leaf_bp = cur->level_at(0).bp;

        if (cur->nlevels == 1) {
            // Root leaf - don't free it, just leave it empty.
            // Freeing block 2 (cntbt root) or block 1 (bnobt root) would
            // catastrophically corrupt the free space trees.
            btree_update_crc<Traits>(leaf_bp);
            xfs_trans_log_buf_full(tp, leaf_bp);
            return 0;
        }

        if (preserve_only_leaf) {
            // Every internal level has exactly one child, so unlinking this
            // leaf would empty the root.  The next insert can descend through
            // the retained path and will refresh all first keys.
            return 0;
        }

        // Non-root leaf - unlink it from the sibling chain and free it.
        uint64_t const LEFT_SIB = cur->left_sibling(0);
        uint64_t const RIGHT_SIB = cur->right_sibling(0);

        constexpr uint64_t NULL_SIB =
            (Traits::TYPE == XfsBtreeType::SHORT) ? static_cast<uint64_t>(NULLAGBLOCK) : static_cast<uint64_t>(NULLFSBLOCK);

        if (LEFT_SIB != NULL_SIB) {
            uint64_t const ABS_LEFT = (Traits::TYPE == XfsBtreeType::SHORT)
                                          ? xfs_agbno_to_fsbno(cur->agno, static_cast<xfs_agblock_t>(LEFT_SIB), cur->mount->ag_blk_log)
                                          : LEFT_SIB;
            BufHead* lbh = xfs_buf_read(cur->mount, ABS_LEFT);
            if (lbh != nullptr) {
                capture_rc = xfs_trans_capture_buf(tp, lbh);
                if (capture_rc != 0) {
                    brelse(lbh);
                    return capture_rc;
                }
                btree_set_rightsib<Traits>(lbh, RIGHT_SIB);
                btree_update_crc<Traits>(lbh);
                xfs_trans_log_buf_full(tp, lbh);
                brelse(lbh);
            }
        }

        if (RIGHT_SIB != NULL_SIB) {
            uint64_t const ABS_RIGHT = (Traits::TYPE == XfsBtreeType::SHORT)
                                           ? xfs_agbno_to_fsbno(cur->agno, static_cast<xfs_agblock_t>(RIGHT_SIB), cur->mount->ag_blk_log)
                                           : RIGHT_SIB;
            BufHead* rbh = xfs_buf_read(cur->mount, ABS_RIGHT);
            if (rbh != nullptr) {
                capture_rc = xfs_trans_capture_buf(tp, rbh);
                if (capture_rc != 0) {
                    brelse(rbh);
                    return capture_rc;
                }
                btree_set_leftsib<Traits>(rbh, LEFT_SIB);
                btree_update_crc<Traits>(rbh);
                xfs_trans_log_buf_full(tp, rbh);
                brelse(rbh);
            }
        }

        // Keep the retired leaf out of the AGFL until transaction-safe
        // deferred retirement exists. Leaking an unreachable metadata block
        // is preferable to cross-tree reuse after an error/cancel.
        btree_update_crc<Traits>(leaf_bp);
        xfs_trans_log_buf_full(tp, leaf_bp);

        // Remove the corresponding key/pointer from the parent
        return btree_remove_from_parent<Traits>(cur, tp, 1);
    }

    // Adjust cursor position
    if (cur->level_at(0).ptr > NR - 1 && NR - 1 > 0) {
        cur->level_at(0).ptr = NR - 1;
    }

    return 0;
}

// ============================================================================
// Explicit template instantiations for all XFS btree types
// ============================================================================

// Cursor class
template class XfsBtreeCursor<XfsBnobtTraits>;
template class XfsBtreeCursor<XfsCntbtTraits>;
template class XfsBtreeCursor<XfsInobtTraits>;
template class XfsBtreeCursor<XfsFinobtTraits>;
template class XfsBtreeCursor<XfsBmbtTraits>;

// Lookup
template auto xfs_btree_lookup<XfsBnobtTraits>(XfsBtreeCursor<XfsBnobtTraits>*, uint64_t, uint8_t, const XfsBnobtTraits::IRec&,
                                               XfsBtreeLookup) -> int;
template auto xfs_btree_lookup<XfsCntbtTraits>(XfsBtreeCursor<XfsCntbtTraits>*, uint64_t, uint8_t, const XfsCntbtTraits::IRec&,
                                               XfsBtreeLookup) -> int;
template auto xfs_btree_lookup<XfsInobtTraits>(XfsBtreeCursor<XfsInobtTraits>*, uint64_t, uint8_t, const XfsInobtTraits::IRec&,
                                               XfsBtreeLookup) -> int;
template auto xfs_btree_lookup<XfsFinobtTraits>(XfsBtreeCursor<XfsFinobtTraits>*, uint64_t, uint8_t, const XfsFinobtTraits::IRec&,
                                                XfsBtreeLookup) -> int;
template auto xfs_btree_lookup<XfsBmbtTraits>(XfsBtreeCursor<XfsBmbtTraits>*, uint64_t, uint8_t, const XfsBmbtTraits::IRec&, XfsBtreeLookup)
    -> int;

// Increment
template auto xfs_btree_increment<XfsBnobtTraits>(XfsBtreeCursor<XfsBnobtTraits>*) -> int;
template auto xfs_btree_increment<XfsCntbtTraits>(XfsBtreeCursor<XfsCntbtTraits>*) -> int;
template auto xfs_btree_increment<XfsInobtTraits>(XfsBtreeCursor<XfsInobtTraits>*) -> int;
template auto xfs_btree_increment<XfsFinobtTraits>(XfsBtreeCursor<XfsFinobtTraits>*) -> int;
template auto xfs_btree_increment<XfsBmbtTraits>(XfsBtreeCursor<XfsBmbtTraits>*) -> int;

// Decrement
template auto xfs_btree_decrement<XfsBnobtTraits>(XfsBtreeCursor<XfsBnobtTraits>*) -> int;
template auto xfs_btree_decrement<XfsCntbtTraits>(XfsBtreeCursor<XfsCntbtTraits>*) -> int;
template auto xfs_btree_decrement<XfsInobtTraits>(XfsBtreeCursor<XfsInobtTraits>*) -> int;
template auto xfs_btree_decrement<XfsFinobtTraits>(XfsBtreeCursor<XfsFinobtTraits>*) -> int;
template auto xfs_btree_decrement<XfsBmbtTraits>(XfsBtreeCursor<XfsBmbtTraits>*) -> int;

// Get record
template auto xfs_btree_get_rec<XfsBnobtTraits>(const XfsBtreeCursor<XfsBnobtTraits>*) -> XfsBnobtTraits::IRec;
template auto xfs_btree_get_rec<XfsCntbtTraits>(const XfsBtreeCursor<XfsCntbtTraits>*) -> XfsCntbtTraits::IRec;
template auto xfs_btree_get_rec<XfsInobtTraits>(const XfsBtreeCursor<XfsInobtTraits>*) -> XfsInobtTraits::IRec;
template auto xfs_btree_get_rec<XfsFinobtTraits>(const XfsBtreeCursor<XfsFinobtTraits>*) -> XfsFinobtTraits::IRec;
template auto xfs_btree_get_rec<XfsBmbtTraits>(const XfsBtreeCursor<XfsBmbtTraits>*) -> XfsBmbtTraits::IRec;

// Update
template auto xfs_btree_update<XfsBnobtTraits>(XfsBtreeCursor<XfsBnobtTraits>*, XfsTransaction*, const XfsBnobtTraits::IRec&) -> int;
template auto xfs_btree_update<XfsCntbtTraits>(XfsBtreeCursor<XfsCntbtTraits>*, XfsTransaction*, const XfsCntbtTraits::IRec&) -> int;
template auto xfs_btree_update<XfsInobtTraits>(XfsBtreeCursor<XfsInobtTraits>*, XfsTransaction*, const XfsInobtTraits::IRec&) -> int;
template auto xfs_btree_update<XfsFinobtTraits>(XfsBtreeCursor<XfsFinobtTraits>*, XfsTransaction*, const XfsFinobtTraits::IRec&) -> int;
template auto xfs_btree_update<XfsBmbtTraits>(XfsBtreeCursor<XfsBmbtTraits>*, XfsTransaction*, const XfsBmbtTraits::IRec&) -> int;

// Insert
template auto xfs_btree_insert<XfsBnobtTraits>(XfsBtreeCursor<XfsBnobtTraits>*, XfsTransaction*, const XfsBnobtTraits::IRec&, uint64_t,
                                               uint8_t, uint64_t*, uint8_t*) -> int;
template auto xfs_btree_insert<XfsCntbtTraits>(XfsBtreeCursor<XfsCntbtTraits>*, XfsTransaction*, const XfsCntbtTraits::IRec&, uint64_t,
                                               uint8_t, uint64_t*, uint8_t*) -> int;
template auto xfs_btree_insert<XfsInobtTraits>(XfsBtreeCursor<XfsInobtTraits>*, XfsTransaction*, const XfsInobtTraits::IRec&, uint64_t,
                                               uint8_t, uint64_t*, uint8_t*) -> int;
template auto xfs_btree_insert<XfsFinobtTraits>(XfsBtreeCursor<XfsFinobtTraits>*, XfsTransaction*, const XfsFinobtTraits::IRec&, uint64_t,
                                                uint8_t, uint64_t*, uint8_t*) -> int;
template auto xfs_btree_insert<XfsBmbtTraits>(XfsBtreeCursor<XfsBmbtTraits>*, XfsTransaction*, const XfsBmbtTraits::IRec&, uint64_t,
                                              uint8_t, uint64_t*, uint8_t*) -> int;

// Delete
template auto xfs_btree_delete<XfsBnobtTraits>(XfsBtreeCursor<XfsBnobtTraits>*, XfsTransaction*) -> int;
template auto xfs_btree_delete<XfsCntbtTraits>(XfsBtreeCursor<XfsCntbtTraits>*, XfsTransaction*) -> int;
template auto xfs_btree_delete<XfsInobtTraits>(XfsBtreeCursor<XfsInobtTraits>*, XfsTransaction*) -> int;
template auto xfs_btree_delete<XfsFinobtTraits>(XfsBtreeCursor<XfsFinobtTraits>*, XfsTransaction*) -> int;
template auto xfs_btree_delete<XfsBmbtTraits>(XfsBtreeCursor<XfsBmbtTraits>*, XfsTransaction*) -> int;

}  // namespace ker::vfs::xfs
