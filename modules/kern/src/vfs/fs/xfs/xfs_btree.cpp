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
        hdr->bb_lsn = Be64{};
        hdr->bb_crc = 0;
        uint32_t crc = util::crc32c_block_with_cksum(bp->data, bp->size, offsetof(XfsBtreeSblock, bb_crc));
        __builtin_memcpy(&hdr->bb_crc, &crc, sizeof(crc));
    } else {
        auto* hdr = reinterpret_cast<XfsBtreeLblock*>(bp->data);
        hdr->bb_lsn = Be64{};
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

    // The AGFL is persistent state and may contain a stale alias after an
    // interrupted or previously buggy metadata update.  Capture before
    // treating the block as fresh so transaction cancellation can restore a
    // live cached node even if the reachability checks missed an owner.
    if (xfs_trans_capture_buf(tp, bh) != 0) {
        brelse(bh);
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
        mod::dbg::logger<"xfs">::error("btree parent insert failed reason=child-alias magic=0x%x child=%lu level=%d", Traits::MAGIC,
                                       static_cast<unsigned long>(left_ptr), lev);
        return -EIO;
    }
    if (lev == cur->nlevels) {
        if (nlevels >= XFS_BTREE_MAXLEVELS) {
            mod::dbg::logger<"xfs">::error("btree parent insert failed reason=max-depth magic=0x%x root=%lu levels=%u", Traits::MAGIC,
                                           static_cast<unsigned long>(root_block), nlevels);
            return -EIO;
        }

        // Need a new root one level above the current root.
        BufHead const* old_root_bp = cur->level_at(nlevels - 1).bp;
        if (old_root_bp == nullptr) {
            mod::dbg::logger<"xfs">::error("btree parent insert failed reason=missing-old-root magic=0x%x root=%lu levels=%u",
                                           Traits::MAGIC, static_cast<unsigned long>(root_block), nlevels);
            return -EIO;
        }
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
            mod::dbg::logger<"xfs">::error(
                "btree parent insert failed reason=root-child-mismatch magic=0x%x root=%lu left=%lu right=%lu levels=%u", Traits::MAGIC,
                static_cast<unsigned long>(root_block), static_cast<unsigned long>(left_ptr), static_cast<unsigned long>(new_ptr), nlevels);
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
        mod::dbg::logger<"xfs">::error("btree parent insert failed reason=missing-parent magic=0x%x root=%lu level=%d levels=%u",
                                       Traits::MAGIC, static_cast<unsigned long>(root_block), lev, nlevels);
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
            mod::dbg::logger<"xfs">::error(
                "btree parent insert failed reason=new-child-already-indexed magic=0x%x root=%lu child=%lu level=%d pos=%d records=%d",
                Traits::MAGIC, static_cast<unsigned long>(root_block), static_cast<unsigned long>(new_ptr), lev, pos, PARENT_NR);
            return -EIO;
        }
        if (CHILD == left_ptr) {
            if (left_pos != 0) {
                mod::dbg::logger<"xfs">::error(
                    "btree parent insert failed reason=left-child-duplicated magic=0x%x root=%lu child=%lu level=%d first=%d second=%d "
                    "records=%d",
                    Traits::MAGIC, static_cast<unsigned long>(root_block), static_cast<unsigned long>(left_ptr), lev, left_pos, pos,
                    PARENT_NR);
                return -EIO;
            }
            left_pos = pos;
        }
    }
    if (left_pos == 0) {
        mod::dbg::logger<"xfs">::error(
            "btree parent insert failed reason=left-child-missing magic=0x%x root=%lu left=%lu right=%lu level=%d records=%d",
            Traits::MAGIC, static_cast<unsigned long>(root_block), static_cast<unsigned long>(left_ptr),
            static_cast<unsigned long>(new_ptr), lev, PARENT_NR);
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

template <typename Traits>
auto btree_level_maxrecs(const XfsBtreeCursor<Traits>* cur, int level) -> int {
    return level == 0 ? btree_max_recs_leaf<Traits>(cur->mount->block_size) : btree_max_keys_node<Traits>(cur->mount->block_size);
}

template <typename Traits>
auto btree_level_minrecs(const XfsBtreeCursor<Traits>* cur, int level) -> int {
    return btree_level_maxrecs(cur, level) / 2;
}

template <typename Traits>
auto btree_disk_level(const BufHead* bp) -> uint16_t {
    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        return reinterpret_cast<const XfsBtreeSblock*>(bp->data)->bb_level.to_cpu();
    } else {
        return reinterpret_cast<const XfsBtreeLblock*>(bp->data)->bb_level.to_cpu();
    }
}

template <typename Traits>
auto btree_disk_numrecs(const BufHead* bp) -> int {
    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        return reinterpret_cast<const XfsBtreeSblock*>(bp->data)->bb_numrecs.to_cpu();
    } else {
        return reinterpret_cast<const XfsBtreeLblock*>(bp->data)->bb_numrecs.to_cpu();
    }
}

template <typename Traits>
auto btree_disk_rightsib(const BufHead* bp) -> uint64_t {
    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        return reinterpret_cast<const XfsBtreeSblock*>(bp->data)->bb_rightsib.to_cpu();
    } else {
        return reinterpret_cast<const XfsBtreeLblock*>(bp->data)->bb_rightsib.to_cpu();
    }
}

template <typename Traits>
auto btree_read_level_block(XfsBtreeCursor<Traits>* cur, int level, uint64_t blockno, BufHead** out_bp) -> int {
    if (cur == nullptr || out_bp == nullptr || !valid_btree_level(level)) {
        return -EINVAL;
    }
    *out_bp = nullptr;
    uint64_t const ABS_BLOCK = Traits::TYPE == XfsBtreeType::SHORT
                                   ? xfs_agbno_to_fsbno(cur->agno, static_cast<xfs_agblock_t>(blockno), cur->mount->ag_blk_log)
                                   : blockno;
    BufHead* bp = xfs_buf_read(cur->mount, ABS_BLOCK);
    if (bp == nullptr) {
        return -EIO;
    }
    uint32_t const MAGIC = reinterpret_cast<const XfsBtreeSblock*>(bp->data)->bb_magic.to_cpu();
    int const NR = btree_disk_numrecs<Traits>(bp);
    if (MAGIC != Traits::MAGIC || btree_disk_level<Traits>(bp) != static_cast<uint16_t>(level) || NR < 0 ||
        NR > btree_level_maxrecs(cur, level)) {
        brelse(bp);
        return -EIO;
    }
    *out_bp = bp;
    return 0;
}

template <typename Traits>
void btree_set_numrecs(BufHead* bp, int nrecs) {
    btree_set_numrecs_raw<Traits>(bp->data, nrecs);
}

template <typename Traits>
void btree_copy_entries(XfsBtreeCursor<Traits>* cur, int level, BufHead* dst, int dst_pos, const BufHead* src, int src_pos, int count) {
    if (count <= 0) {
        return;
    }
    if (level == 0) {
        __builtin_memcpy(dst->data + Traits::HDR_LEN + (static_cast<size_t>(dst_pos - 1) * Traits::REC_LEN),
                         src->data + Traits::HDR_LEN + (static_cast<size_t>(src_pos - 1) * Traits::REC_LEN),
                         static_cast<size_t>(count) * Traits::REC_LEN);
        return;
    }

    __builtin_memcpy(dst->data + btree_key_off<Traits>(static_cast<size_t>(dst_pos - 1)),
                     src->data + btree_key_off<Traits>(static_cast<size_t>(src_pos - 1)), static_cast<size_t>(count) * Traits::KEY_LEN);
    __builtin_memcpy(dst->data + btree_ptr_off<Traits>(cur->mount->block_size, static_cast<size_t>(dst_pos - 1)),
                     src->data + btree_ptr_off<Traits>(cur->mount->block_size, static_cast<size_t>(src_pos - 1)),
                     static_cast<size_t>(count) * Traits::PTR_LEN);
}

template <typename Traits>
void btree_shift_entries_right(XfsBtreeCursor<Traits>* cur, int level, BufHead* bp, int count, int shift) {
    if (count <= 0 || shift <= 0) {
        return;
    }
    if (level == 0) {
        uint8_t* base = bp->data + Traits::HDR_LEN;
        std::memmove(base + (static_cast<size_t>(shift) * Traits::REC_LEN), base, static_cast<size_t>(count) * Traits::REC_LEN);
        return;
    }

    std::memmove(bp->data + btree_key_off<Traits>(static_cast<size_t>(shift)), bp->data + Traits::HDR_LEN,
                 static_cast<size_t>(count) * Traits::KEY_LEN);
    uint8_t* ptr_base = bp->data + btree_ptr_off<Traits>(cur->mount->block_size, 0);
    std::memmove(ptr_base + (static_cast<size_t>(shift) * Traits::PTR_LEN), ptr_base, static_cast<size_t>(count) * Traits::PTR_LEN);
}

template <typename Traits>
void btree_shift_entries_left(XfsBtreeCursor<Traits>* cur, int level, BufHead* bp, int remove_count, int old_count) {
    int const KEEP = old_count - remove_count;
    if (KEEP <= 0 || remove_count <= 0) {
        return;
    }
    if (level == 0) {
        uint8_t* base = bp->data + Traits::HDR_LEN;
        std::memmove(base, base + (static_cast<size_t>(remove_count) * Traits::REC_LEN), static_cast<size_t>(KEEP) * Traits::REC_LEN);
        return;
    }

    std::memmove(bp->data + Traits::HDR_LEN, bp->data + btree_key_off<Traits>(static_cast<size_t>(remove_count)),
                 static_cast<size_t>(KEEP) * Traits::KEY_LEN);
    uint8_t* ptr_base = bp->data + btree_ptr_off<Traits>(cur->mount->block_size, 0);
    std::memmove(ptr_base, ptr_base + (static_cast<size_t>(remove_count) * Traits::PTR_LEN), static_cast<size_t>(KEEP) * Traits::PTR_LEN);
}

template <typename Traits>
auto btree_first_key(const BufHead* bp, int level, typename Traits::Key* key) -> int {
    if (bp == nullptr || key == nullptr || btree_disk_numrecs<Traits>(bp) <= 0) {
        return -EIO;
    }
    if (level == 0) {
        Traits::init_key_from_rec(key, reinterpret_cast<const Traits::Rec*>(bp->data + Traits::HDR_LEN));
    } else {
        __builtin_memcpy(key, bp->data + Traits::HDR_LEN, Traits::KEY_LEN);
    }
    return 0;
}

template <typename Traits>
auto btree_update_parent_key(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp, int parent_level, int child_pos,
                             const typename Traits::Key& key) -> int {
    BufHead* parent_bp = cur->level_at(parent_level).bp;
    if (parent_bp == nullptr || child_pos < 1 || child_pos > cur->numrecs(parent_level)) {
        return -EIO;
    }
    int const CAPTURE_RC = xfs_trans_capture_buf(tp, parent_bp);
    if (CAPTURE_RC != 0) {
        return CAPTURE_RC;
    }
    __builtin_memcpy(cur->key_at_mut(parent_level, child_pos), &key, Traits::KEY_LEN);
    btree_update_crc<Traits>(parent_bp);
    xfs_trans_log_buf_full(tp, parent_bp);
    if (child_pos == 1) {
        return btree_propagate_first_key(cur, tp, parent_level, key);
    }
    return 0;
}

template <typename Traits>
auto btree_rebalance_level(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp, int level) -> int;

template <typename Traits>
auto btree_remove_internal_entry(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp, int level, int remove_pos, int survivor_pos) -> int {
    if (level <= 0 || level >= cur->nlevels) {
        return -EINVAL;
    }
    BufHead* bp = cur->level_at(level).bp;
    int const NR = cur->numrecs(level);
    if (bp == nullptr || remove_pos < 1 || remove_pos > NR || survivor_pos < 1 || survivor_pos >= NR) {
        return -EIO;
    }
    int const CAPTURE_RC = xfs_trans_capture_buf(tp, bp);
    if (CAPTURE_RC != 0) {
        return CAPTURE_RC;
    }

    uint8_t* data = bp->data;
    if (remove_pos < NR) {
        std::memmove(data + btree_key_off<Traits>(static_cast<size_t>(remove_pos - 1)),
                     data + btree_key_off<Traits>(static_cast<size_t>(remove_pos)), static_cast<size_t>(NR - remove_pos) * Traits::KEY_LEN);
        uint8_t* ptr_base = data + btree_ptr_off<Traits>(cur->mount->block_size, 0);
        std::memmove(ptr_base + (static_cast<size_t>(remove_pos - 1) * Traits::PTR_LEN),
                     ptr_base + (static_cast<size_t>(remove_pos) * Traits::PTR_LEN),
                     static_cast<size_t>(NR - remove_pos) * Traits::PTR_LEN);
    }
    btree_set_numrecs<Traits>(bp, NR - 1);
    cur->level_at(level).ptr = survivor_pos;
    btree_update_crc<Traits>(bp);
    xfs_trans_log_buf_full(tp, bp);

    if (level == cur->nlevels - 1 || NR - 1 >= btree_level_minrecs(cur, level)) {
        return 0;
    }
    return btree_rebalance_level(cur, tp, level);
}

template <typename Traits>
auto btree_rebalance_level(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp, int level) -> int {
    if (cur == nullptr || tp == nullptr || level < 0 || level >= cur->nlevels - 1) {
        return -EINVAL;
    }

    BufHead* current = cur->level_at(level).bp;
    int const CURRENT_NR = cur->numrecs(level);
    int const MIN_RECS = btree_level_minrecs(cur, level);
    if (current == nullptr || CURRENT_NR < 0) {
        return -EIO;
    }
    if (CURRENT_NR >= MIN_RECS) {
        return 0;
    }

    int const PARENT_LEVEL = level + 1;
    int const CHILD_POS = cur->level_at(PARENT_LEVEL).ptr;
    int const PARENT_NR = cur->numrecs(PARENT_LEVEL);
    if (CHILD_POS < 1 || CHILD_POS > PARENT_NR) {
        return -EIO;
    }
    if (PARENT_NR == 1) {
        // The external root will collapse onto this child after deletion.
        return PARENT_LEVEL == cur->nlevels - 1 ? 0 : -EIO;
    }

    auto load_sibling = [&](int sibling_pos, BufHead** sibling) -> int {
        uint64_t const BLOCK = cur->ptr_at(PARENT_LEVEL, sibling_pos);
        return btree_read_level_block(cur, level, BLOCK, sibling);
    };

    // Prefer borrowing from the right so the current record position remains
    // stable. A valid tree can be short by only one record after a deletion,
    // but move the full deficit to fail safely if older media is encountered.
    if (CHILD_POS < PARENT_NR) {
        BufHead* right = nullptr;
        int rc = load_sibling(CHILD_POS + 1, &right);
        if (rc != 0) {
            return rc;
        }
        int const RIGHT_NR = btree_disk_numrecs<Traits>(right);
        int const NEEDED = MIN_RECS - CURRENT_NR;
        int const MOVE = RIGHT_NR - MIN_RECS >= NEEDED ? NEEDED : 0;
        if (MOVE > 0) {
            rc = xfs_trans_capture_buf(tp, current);
            if (rc == 0) {
                rc = xfs_trans_capture_buf(tp, right);
            }
            if (rc != 0) {
                brelse(right);
                return rc;
            }
            btree_copy_entries(cur, level, current, CURRENT_NR + 1, right, 1, MOVE);
            btree_shift_entries_left(cur, level, right, MOVE, RIGHT_NR);
            btree_set_numrecs<Traits>(current, CURRENT_NR + MOVE);
            btree_set_numrecs<Traits>(right, RIGHT_NR - MOVE);

            typename Traits::Key right_key{};
            rc = btree_first_key<Traits>(right, level, &right_key);
            if (rc == 0) {
                rc = btree_update_parent_key(cur, tp, PARENT_LEVEL, CHILD_POS + 1, right_key);
            }
            if (rc == 0) {
                btree_update_crc<Traits>(current);
                btree_update_crc<Traits>(right);
                xfs_trans_log_buf_full(tp, current);
                xfs_trans_log_buf_full(tp, right);
            }
            brelse(right);
            return rc;
        }
        brelse(right);
    }

    if (CHILD_POS > 1) {
        BufHead* left = nullptr;
        int rc = load_sibling(CHILD_POS - 1, &left);
        if (rc != 0) {
            return rc;
        }
        int const LEFT_NR = btree_disk_numrecs<Traits>(left);
        int const NEEDED = MIN_RECS - CURRENT_NR;
        int const MOVE = LEFT_NR - MIN_RECS >= NEEDED ? NEEDED : 0;
        if (MOVE > 0) {
            rc = xfs_trans_capture_buf(tp, current);
            if (rc == 0) {
                rc = xfs_trans_capture_buf(tp, left);
            }
            if (rc != 0) {
                brelse(left);
                return rc;
            }
            btree_shift_entries_right(cur, level, current, CURRENT_NR, MOVE);
            btree_copy_entries(cur, level, current, 1, left, LEFT_NR - MOVE + 1, MOVE);
            btree_set_numrecs<Traits>(current, CURRENT_NR + MOVE);
            btree_set_numrecs<Traits>(left, LEFT_NR - MOVE);
            cur->level_at(level).ptr += MOVE;

            typename Traits::Key current_key{};
            rc = btree_first_key<Traits>(current, level, &current_key);
            if (rc == 0) {
                rc = btree_update_parent_key(cur, tp, PARENT_LEVEL, CHILD_POS, current_key);
            }
            if (rc == 0) {
                btree_update_crc<Traits>(current);
                btree_update_crc<Traits>(left);
                xfs_trans_log_buf_full(tp, current);
                xfs_trans_log_buf_full(tp, left);
            }
            brelse(left);
            return rc;
        }
        brelse(left);
    }

    // Neither neighbor can spare an entry, so two minimum-sized siblings fit
    // in one block. Keep the left block and detach the right block; detached
    // metadata is deliberately not returned to the AGFL until deferred,
    // transaction-safe btree block retirement exists.
    bool const MERGE_INTO_LEFT = CHILD_POS > 1;
    int const SIBLING_POS = MERGE_INTO_LEFT ? CHILD_POS - 1 : CHILD_POS + 1;
    BufHead* sibling = nullptr;
    int rc = load_sibling(SIBLING_POS, &sibling);
    if (rc != 0) {
        return rc;
    }

    BufHead* left = MERGE_INTO_LEFT ? sibling : current;
    BufHead* right = MERGE_INTO_LEFT ? current : sibling;
    int const LEFT_NR = btree_disk_numrecs<Traits>(left);
    int const RIGHT_NR = btree_disk_numrecs<Traits>(right);
    if (LEFT_NR + RIGHT_NR > btree_level_maxrecs(cur, level)) {
        brelse(sibling);
        return -EIO;
    }

    rc = xfs_trans_capture_buf(tp, left);
    if (rc == 0) {
        rc = xfs_trans_capture_buf(tp, right);
    }
    if (rc != 0) {
        brelse(sibling);
        return rc;
    }
    btree_copy_entries(cur, level, left, LEFT_NR + 1, right, 1, RIGHT_NR);
    btree_set_numrecs<Traits>(left, LEFT_NR + RIGHT_NR);

    uint64_t const NEXT_BLOCK = btree_disk_rightsib<Traits>(right);
    btree_set_rightsib<Traits>(left, NEXT_BLOCK);
    constexpr uint64_t NULL_SIB =
        Traits::TYPE == XfsBtreeType::SHORT ? static_cast<uint64_t>(NULLAGBLOCK) : static_cast<uint64_t>(NULLFSBLOCK);
    if (NEXT_BLOCK != NULL_SIB) {
        BufHead* next = nullptr;
        rc = btree_read_level_block(cur, level, NEXT_BLOCK, &next);
        if (rc == 0) {
            rc = xfs_trans_capture_buf(tp, next);
        }
        if (rc == 0) {
            btree_set_leftsib<Traits>(next, btree_blockno<Traits>(cur, left));
            btree_update_crc<Traits>(next);
            xfs_trans_log_buf_full(tp, next);
        }
        if (next != nullptr) {
            brelse(next);
        }
        if (rc != 0) {
            brelse(sibling);
            return rc;
        }
    }

    btree_update_crc<Traits>(left);
    xfs_trans_log_buf_full(tp, left);

    int remove_pos = 0;
    int survivor_pos = 0;
    if (MERGE_INTO_LEFT) {
        remove_pos = CHILD_POS;
        survivor_pos = CHILD_POS - 1;
        cur->level_at(level).ptr += LEFT_NR;
        brelse(cur->level_at(level).bp);
        cur->level_at(level).bp = sibling;
        sibling = nullptr;
    } else {
        remove_pos = CHILD_POS + 1;
        survivor_pos = CHILD_POS;
        typename Traits::Key survivor_key{};
        rc = btree_first_key<Traits>(left, level, &survivor_key);
        if (rc == 0) {
            rc = btree_update_parent_key(cur, tp, PARENT_LEVEL, survivor_pos, survivor_key);
        }
        if (rc != 0) {
            brelse(sibling);
            return rc;
        }
    }

    if (sibling != nullptr) {
        brelse(sibling);
    }
    return btree_remove_internal_entry(cur, tp, PARENT_LEVEL, remove_pos, survivor_pos);
}

template <typename Traits>
auto btree_finish_delete(XfsBtreeCursor<Traits>* cur, uint64_t root_block, uint8_t nlevels, uint64_t* new_root, uint8_t* new_nlevels)
    -> int {
    if (cur == nullptr || !valid_btree_depth(nlevels) || new_root == nullptr || new_nlevels == nullptr) {
        return -EINVAL;
    }

    *new_root = root_block;
    *new_nlevels = nlevels;

    // External XFS btree roots are ordinary blocks. Once an internal root has
    // only one child, make that child the new root instead of retaining a
    // one-child parent that must later be grown in place. Retired roots stay
    // out of the AGFL until transaction-safe metadata recycling exists.
    uint64_t collapsed_root = root_block;
    uint8_t collapsed_levels = nlevels;
    while (collapsed_levels > 1) {
        int const ROOT_LEVEL = static_cast<int>(collapsed_levels - 1);
        BufHead* root_bp = cur->level_at(ROOT_LEVEL).bp;
        if (root_bp == nullptr || btree_blockno<Traits>(cur, root_bp) != collapsed_root) {
            return -EIO;
        }

        uint16_t disk_level = 0;
        if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
            disk_level = reinterpret_cast<const XfsBtreeSblock*>(root_bp->data)->bb_level.to_cpu();
        } else {
            disk_level = reinterpret_cast<const XfsBtreeLblock*>(root_bp->data)->bb_level.to_cpu();
        }
        if (disk_level != static_cast<uint16_t>(ROOT_LEVEL)) {
            return -EIO;
        }
        if (cur->numrecs(ROOT_LEVEL) != 1) {
            break;
        }

        uint64_t const CHILD = cur->ptr_at(ROOT_LEVEL, 1);
        constexpr uint64_t NULL_PTR =
            (Traits::TYPE == XfsBtreeType::SHORT) ? static_cast<uint64_t>(NULLAGBLOCK) : static_cast<uint64_t>(NULLFSBLOCK);
        if (CHILD == NULL_PTR || CHILD == collapsed_root) {
            return -EIO;
        }
        BufHead* child_bp = cur->level_at(ROOT_LEVEL - 1).bp;
        if (child_bp == nullptr || btree_blockno<Traits>(cur, child_bp) != CHILD) {
            // Deleting the cursor's leaf can leave a different sibling as the
            // root's sole child. Rebind this cursor level to the surviving
            // child before validating or attempting another collapse.
            int const READ_RC = cur->read_block(ROOT_LEVEL - 1, CHILD);
            if (READ_RC != 0) {
                return READ_RC;
            }
            child_bp = cur->level_at(ROOT_LEVEL - 1).bp;
        }

        uint16_t child_disk_level = 0;
        if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
            child_disk_level = reinterpret_cast<const XfsBtreeSblock*>(child_bp->data)->bb_level.to_cpu();
        } else {
            child_disk_level = reinterpret_cast<const XfsBtreeLblock*>(child_bp->data)->bb_level.to_cpu();
        }
        if (child_disk_level != static_cast<uint16_t>(ROOT_LEVEL - 1)) {
            return -EIO;
        }

        collapsed_root = CHILD;
        collapsed_levels--;
    }

    *new_root = collapsed_root;
    *new_nlevels = collapsed_levels;
    cur->nlevels = collapsed_levels;
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
auto xfs_btree_delete(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp, uint64_t root_block, uint8_t nlevels, uint64_t* new_root,
                      uint8_t* new_nlevels) -> int {
    using Key = Traits::Key;
    using Rec = Traits::Rec;

    if (cur == nullptr || tp == nullptr || cur->mount == nullptr || !valid_btree_depth(nlevels) || cur->nlevels != nlevels ||
        new_root == nullptr || new_nlevels == nullptr) {
        return -EINVAL;
    }
    *new_root = root_block;
    *new_nlevels = nlevels;

    int const PTR = cur->level_at(0).ptr;
    int const NR = cur->numrecs(0);
    if (PTR < 1 || PTR > NR || cur->level_at(0).bp == nullptr) {
        return -EINVAL;
    }

    int capture_rc = xfs_trans_capture_buf(tp, cur->level_at(0).bp);
    if (capture_rc != 0) {
        return capture_rc;
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

    // Adjust cursor position
    if (cur->level_at(0).ptr > NR - 1 && NR - 1 > 0) {
        cur->level_at(0).ptr = NR - 1;
    }

    if (cur->nlevels > 1 && NR - 1 < btree_level_minrecs(cur, 0)) {
        int const REBALANCE_RC = btree_rebalance_level(cur, tp, 0);
        if (REBALANCE_RC != 0) {
            return REBALANCE_RC;
        }
    }

    return btree_finish_delete<Traits>(cur, root_block, nlevels, new_root, new_nlevels);
}

#ifdef WOS_SELFTEST
auto xfs_selftest_btree_collapses_single_child_root() -> bool {
    constexpr uint32_t BLOCK_SIZE = 1024;
    constexpr uint32_t SECTOR_SIZE = 256;
    constexpr uint32_t AG_BLOCKS = 4096;
    constexpr xfs_agblock_t ROOT_BLOCK = 10;
    constexpr xfs_agblock_t CHILD_BLOCK = 11;

    XfsMountContext mount{};
    mount.block_size = BLOCK_SIZE;
    mount.sect_size = SECTOR_SIZE;
    mount.ag_blocks = AG_BLOCKS;

    std::array<uint8_t, BLOCK_SIZE> root_data{};
    std::array<uint8_t, BLOCK_SIZE> child_data{};
    BufHead root_bp{};
    root_bp.data = root_data.data();
    root_bp.size = root_data.size();
    root_bp.block_no = static_cast<uint64_t>(ROOT_BLOCK) * (BLOCK_SIZE / SECTOR_SIZE);
    BufHead child_bp{};
    child_bp.data = child_data.data();
    child_bp.size = child_data.size();
    child_bp.block_no = static_cast<uint64_t>(CHILD_BLOCK) * (BLOCK_SIZE / SECTOR_SIZE);

    auto* root_hdr = reinterpret_cast<XfsBtreeSblock*>(root_data.data());
    root_hdr->bb_magic = Be32::from_cpu(XfsFinobtTraits::MAGIC);
    root_hdr->bb_level = Be16::from_cpu(1);
    root_hdr->bb_numrecs = Be16::from_cpu(1);
    btree_write_ptr<XfsFinobtTraits>(root_data.data(), BLOCK_SIZE, 1, CHILD_BLOCK);

    auto* child_hdr = reinterpret_cast<XfsBtreeSblock*>(child_data.data());
    child_hdr->bb_magic = Be32::from_cpu(XfsFinobtTraits::MAGIC);
    child_hdr->bb_level = Be16::from_cpu(0);
    child_hdr->bb_numrecs = Be16::from_cpu(1);

    XfsBtreeCursor<XfsFinobtTraits> cur;
    cur.mount = &mount;
    cur.agno = 0;
    cur.nlevels = 2;
    cur.level_at(1).bp = &root_bp;
    cur.level_at(1).ptr = 1;
    cur.level_at(0).bp = &child_bp;
    cur.level_at(0).ptr = 1;

    uint64_t new_root = ROOT_BLOCK;
    uint8_t new_nlevels = 2;
    int const RC = btree_finish_delete(&cur, ROOT_BLOCK, 2, &new_root, &new_nlevels);
    cur.level_at(1).bp = nullptr;
    cur.level_at(0).bp = nullptr;
    return RC == 0 && new_root == CHILD_BLOCK && new_nlevels == 1 && cur.nlevels == 1;
}

namespace {

auto xfs_btree_selftest_read(ker::dev::BlockDevice* dev, uint64_t /*block*/, size_t count, void* buffer) -> int {
    __builtin_memset(buffer, 0, count * dev->block_size);
    return 0;
}

auto xfs_btree_selftest_write(ker::dev::BlockDevice* /*dev*/, uint64_t /*block*/, size_t /*count*/, const void* /*buffer*/) -> int {
    return 0;
}

}  // namespace

auto xfs_selftest_btree_delete_rebalances() -> bool {
    constexpr uint32_t BLOCK_SIZE = 4096;
    constexpr uint32_t SECTOR_SIZE = 512;
    constexpr uint32_t AG_BLOCKS = 4096;
    constexpr xfs_agblock_t ROOT_BLOCK = 10;
    constexpr xfs_agblock_t LEFT_BLOCK = 11;
    constexpr xfs_agblock_t RIGHT_BLOCK = 12;
    constexpr xfs_agblock_t LEFT_START = 100;
    constexpr xfs_agblock_t RIGHT_START = 1000;
    constexpr int MAX_RECS = static_cast<int>((BLOCK_SIZE - XfsBnobtTraits::HDR_LEN) / XfsBnobtTraits::REC_LEN);
    constexpr int MIN_RECS = MAX_RECS / 2;

    auto run_case = [&](int right_recs, bool expect_merge) -> bool {
        ker::dev::BlockDevice dev{};
        dev.block_size = SECTOR_SIZE;
        dev.total_blocks = static_cast<uint64_t>(AG_BLOCKS) * (BLOCK_SIZE / SECTOR_SIZE);
        dev.read_blocks = xfs_btree_selftest_read;
        dev.write_blocks = xfs_btree_selftest_write;
        invalidate_bdev(&dev);

        XfsMountContext mount{};
        mount.device = &dev;
        mount.block_size = BLOCK_SIZE;
        mount.block_log = 12;
        mount.sect_size = SECTOR_SIZE;
        mount.ag_blocks = AG_BLOCKS;
        mount.ag_blk_log = 12;

        BufHead* root = xfs_buf_get(&mount, ROOT_BLOCK);
        BufHead* left = xfs_buf_get(&mount, LEFT_BLOCK);
        BufHead* right = xfs_buf_get(&mount, RIGHT_BLOCK);
        if (root == nullptr || left == nullptr || right == nullptr) {
            if (root != nullptr) {
                brelse(root);
            }
            if (left != nullptr) {
                brelse(left);
            }
            if (right != nullptr) {
                brelse(right);
            }
            invalidate_bdev(&dev);
            return false;
        }

        __builtin_memset(root->data, 0, root->size);
        __builtin_memset(left->data, 0, left->size);
        __builtin_memset(right->data, 0, right->size);

        auto init_header = [&](BufHead* bp, uint16_t level, uint16_t numrecs, xfs_agblock_t leftsib, xfs_agblock_t rightsib) {
            auto* hdr = reinterpret_cast<XfsBtreeSblock*>(bp->data);
            hdr->bb_magic = Be32::from_cpu(XfsBnobtTraits::MAGIC);
            hdr->bb_level = Be16::from_cpu(level);
            hdr->bb_numrecs = Be16::from_cpu(numrecs);
            hdr->bb_leftsib = Be32::from_cpu(leftsib);
            hdr->bb_rightsib = Be32::from_cpu(rightsib);
        };
        init_header(root, 1, 2, NULLAGBLOCK, NULLAGBLOCK);
        init_header(left, 0, MIN_RECS, NULLAGBLOCK, RIGHT_BLOCK);
        init_header(right, 0, static_cast<uint16_t>(right_recs), LEFT_BLOCK, NULLAGBLOCK);

        for (int i = 0; i < MIN_RECS; ++i) {
            XfsBnobtTraits::IRec const IREC{.startblock = LEFT_START + static_cast<xfs_agblock_t>(i), .blockcount = 1};
            XfsBnobtTraits::encode_rec(reinterpret_cast<XfsBnobtTraits::Rec*>(left->data + XfsBnobtTraits::HDR_LEN) + i, IREC);
        }
        for (int i = 0; i < right_recs; ++i) {
            XfsBnobtTraits::IRec const IREC{.startblock = RIGHT_START + static_cast<xfs_agblock_t>(i), .blockcount = 1};
            XfsBnobtTraits::encode_rec(reinterpret_cast<XfsBnobtTraits::Rec*>(right->data + XfsBnobtTraits::HDR_LEN) + i, IREC);
        }

        auto* root_keys = reinterpret_cast<XfsBnobtTraits::Key*>(root->data + XfsBnobtTraits::HDR_LEN);
        root_keys[0].ar_startblock = Be32::from_cpu(LEFT_START);
        root_keys[0].ar_blockcount = Be32::from_cpu(1);
        root_keys[1].ar_startblock = Be32::from_cpu(RIGHT_START);
        root_keys[1].ar_blockcount = Be32::from_cpu(1);
        btree_write_ptr<XfsBnobtTraits>(root->data, BLOCK_SIZE, 1, LEFT_BLOCK);
        btree_write_ptr<XfsBnobtTraits>(root->data, BLOCK_SIZE, 2, RIGHT_BLOCK);

        bool ok = true;
        XfsTransaction* tp = xfs_trans_alloc(&mount);
        if (tp == nullptr) {
            ok = false;
        } else {
            XfsBtreeCursor<XfsBnobtTraits> cur;
            cur.mount = &mount;
            cur.agno = 0;
            XfsBnobtTraits::IRec const TARGET{
                .startblock = LEFT_START + static_cast<xfs_agblock_t>(MIN_RECS - 1),
                .blockcount = 1,
            };
            int rc = xfs_btree_lookup(&cur, ROOT_BLOCK, 2, TARGET, XfsBtreeLookup::EQ);
            uint64_t new_root = ROOT_BLOCK;
            uint8_t new_nlevels = 2;
            if (rc == 0) {
                rc = xfs_btree_delete(&cur, tp, ROOT_BLOCK, 2, &new_root, &new_nlevels);
            }
            ok = rc == 0;
            if (ok && expect_merge) {
                ok = new_root == LEFT_BLOCK && new_nlevels == 1 && btree_disk_numrecs<XfsBnobtTraits>(left) == (2 * MIN_RECS) - 1 &&
                     btree_disk_rightsib<XfsBnobtTraits>(left) == NULLAGBLOCK;
            } else if (ok) {
                ok = new_root == ROOT_BLOCK && new_nlevels == 2 && btree_disk_numrecs<XfsBnobtTraits>(left) == MIN_RECS &&
                     btree_disk_numrecs<XfsBnobtTraits>(right) == MIN_RECS && root_keys[1].ar_startblock.to_cpu() == RIGHT_START + 1;
            }
        }

        if (tp != nullptr) {
            xfs_trans_cancel(tp);
        }
        brelse(root);
        brelse(left);
        brelse(right);
        invalidate_bdev(&dev);
        return ok;
    };

    return run_case(MIN_RECS + 1, false) && run_case(MIN_RECS, true);
}
#endif

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
template auto xfs_btree_delete<XfsBnobtTraits>(XfsBtreeCursor<XfsBnobtTraits>*, XfsTransaction*, uint64_t, uint8_t, uint64_t*, uint8_t*)
    -> int;
template auto xfs_btree_delete<XfsCntbtTraits>(XfsBtreeCursor<XfsCntbtTraits>*, XfsTransaction*, uint64_t, uint8_t, uint64_t*, uint8_t*)
    -> int;
template auto xfs_btree_delete<XfsInobtTraits>(XfsBtreeCursor<XfsInobtTraits>*, XfsTransaction*, uint64_t, uint8_t, uint64_t*, uint8_t*)
    -> int;
template auto xfs_btree_delete<XfsFinobtTraits>(XfsBtreeCursor<XfsFinobtTraits>*, XfsTransaction*, uint64_t, uint8_t, uint64_t*, uint8_t*)
    -> int;
template auto xfs_btree_delete<XfsBmbtTraits>(XfsBtreeCursor<XfsBmbtTraits>*, XfsTransaction*, uint64_t, uint8_t, uint64_t*, uint8_t*)
    -> int;

}  // namespace ker::vfs::xfs
