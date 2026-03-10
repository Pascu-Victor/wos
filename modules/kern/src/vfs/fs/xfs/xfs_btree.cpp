// XFS B+Tree infrastructure — generic traversal and lookup engine.
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
#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <vfs/fs/xfs/xfs_trans.hpp>

namespace ker::vfs::xfs {

// ============================================================================
// Cursor helper methods
// ============================================================================

template <typename Traits>
auto XfsBtreeCursor<Traits>::numrecs(int level) const -> int {
    if (levels[level].bp == nullptr) {
        return 0;
    }
    const auto* data = levels[level].bp->data;

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
    const uint8_t* base = levels[level].bp->data + Traits::HDR_LEN;
    return reinterpret_cast<const Key*>(base + (static_cast<size_t>(idx - 1) * Traits::KEY_LEN));
}

template <typename Traits>
auto XfsBtreeCursor<Traits>::rec_at(int idx) const -> const Rec* {
    // Records are at leaf level (level 0), starting after header
    const uint8_t* base = levels[0].bp->data + Traits::HDR_LEN;
    return reinterpret_cast<const Rec*>(base + (static_cast<size_t>(idx - 1) * Traits::REC_LEN));
}

template <typename Traits>
auto XfsBtreeCursor<Traits>::ptr_at(int level, int idx) const -> uint64_t {
    // Pointers are stored after all keys in an internal node.
    // Layout: [header][key0][key1]...[keyN][ptr0][ptr1]...[ptrN]
    int nrecs = numrecs(level);
    const uint8_t* base = levels[level].bp->data + Traits::HDR_LEN;
    const uint8_t* ptr_base = base + (static_cast<size_t>(nrecs) * Traits::KEY_LEN);
    const uint8_t* ptr_addr = ptr_base + (static_cast<size_t>(idx - 1) * Traits::PTR_LEN);

    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        __be32 val{};
        __builtin_memcpy(&val, ptr_addr, 4);
        return val.to_cpu();
    } else {
        __be64 val{};
        __builtin_memcpy(&val, ptr_addr, 8);
        return val.to_cpu();
    }
}

template <typename Traits>
auto XfsBtreeCursor<Traits>::read_block(int level, uint64_t blockno) -> int {
    // Release existing buffer at this level
    if (levels[level].bp != nullptr) {
        brelse(levels[level].bp);
        levels[level].bp = nullptr;
    }

    // For short-form (AG) btrees, convert AG-relative block to absolute
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

    levels[level].bp = bh;
    return 0;
}

template <typename Traits>
auto XfsBtreeCursor<Traits>::left_sibling(int level) const -> uint64_t {
    if (levels[level].bp == nullptr) {
        return (Traits::TYPE == XfsBtreeType::SHORT) ? NULLAGBLOCK : NULLFSBLOCK;
    }
    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        const auto* hdr = reinterpret_cast<const XfsBtreeSblock*>(levels[level].bp->data);
        return hdr->bb_leftsib.to_cpu();
    } else {
        const auto* hdr = reinterpret_cast<const XfsBtreeLblock*>(levels[level].bp->data);
        return hdr->bb_leftsib.to_cpu();
    }
}

template <typename Traits>
auto XfsBtreeCursor<Traits>::right_sibling(int level) const -> uint64_t {
    if (levels[level].bp == nullptr) {
        return (Traits::TYPE == XfsBtreeType::SHORT) ? NULLAGBLOCK : NULLFSBLOCK;
    }
    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        const auto* hdr = reinterpret_cast<const XfsBtreeSblock*>(levels[level].bp->data);
        return hdr->bb_rightsib.to_cpu();
    } else {
        const auto* hdr = reinterpret_cast<const XfsBtreeLblock*>(levels[level].bp->data);
        return hdr->bb_rightsib.to_cpu();
    }
}

// ============================================================================
// Lookup
// ============================================================================

template <typename Traits>
auto xfs_btree_lookup(XfsBtreeCursor<Traits>* cur, uint64_t root_block, uint8_t nlevels, const typename Traits::IRec& target,
                      XfsBtreeLookup dir) -> int {
    using Key = typename Traits::Key;

    cur->nlevels = nlevels;

    // Read the root block
    int rc = cur->read_block(nlevels - 1, root_block);
    if (rc != 0) {
        return rc;
    }

    // Descend from root (level nlevels-1) to leaf (level 0)
    int cmp_r = 0;  // result of last comparison
    for (int level = nlevels - 1; level >= 0; level--) {
        int nr = cur->numrecs(level);
        if (nr == 0) {
            // Empty block — tree is empty or corrupted
            cur->levels[level].ptr = 0;
            return -ENOENT;
        }

        // Binary search within this block
        int low = 1;
        int high = nr;
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
            cur->levels[level].ptr = keyno;

            // Follow child pointer down to next level
            uint64_t child_block = cur->ptr_at(level, keyno);
            rc = cur->read_block(level - 1, child_block);
            if (rc != 0) {
                return rc;
            }
        } else {
            // Leaf level: set position based on direction
            if (cmp_r > 0) {
                // key > target: the record at keyno is bigger than what we want
                keyno--;
            }
            cur->levels[0].ptr = keyno;
        }
    }

    // Adjust position based on lookup direction
    int ptr = cur->levels[0].ptr;
    int nr = cur->numrecs(0);

    switch (dir) {
        case XfsBtreeLookup::LE:
            // We want the largest record <= target
            if (ptr < 1) {
                return -ENOENT;
            }
            if (ptr > nr) {
                cur->levels[0].ptr = nr;
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
                cur->levels[0].ptr = 1;
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
                        cur->levels[0].ptr = ptr + 1;
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
    ptr = cur->levels[0].ptr;
    nr = cur->numrecs(0);
    if (ptr < 1 || ptr > nr) {
        return -ENOENT;
    }

    return 0;
}

// ============================================================================
// Increment — move to next record
// ============================================================================

template <typename Traits>
auto xfs_btree_increment(XfsBtreeCursor<Traits>* cur) -> int {
    constexpr uint64_t NULL_SIB = (Traits::TYPE == XfsBtreeType::SHORT) ? static_cast<uint64_t>(NULLAGBLOCK) : NULLFSBLOCK;

    // Try to advance within the current leaf block
    int ptr = cur->levels[0].ptr + 1;
    if (ptr <= cur->numrecs(0)) {
        cur->levels[0].ptr = ptr;
        return 0;
    }

    // Walk up the tree until we find a level where we can advance
    int level = 0;
    for (level = 1; level < cur->nlevels; level++) {
        ptr = cur->levels[level].ptr + 1;
        if (ptr <= cur->numrecs(level)) {
            cur->levels[level].ptr = ptr;
            break;
        }
    }

    if (level >= cur->nlevels) {
        // We've exhausted the entire tree
        return -ENOENT;
    }

    // Walk back down from 'level' to leaf, always taking the leftmost child
    for (int lev = level - 1; lev >= 0; lev--) {
        uint64_t child_block = cur->ptr_at(lev + 1, cur->levels[lev + 1].ptr);
        int rc = cur->read_block(lev, child_block);
        if (rc != 0) {
            return rc;
        }
        cur->levels[lev].ptr = 1;  // leftmost entry
    }

    return 0;
}

// ============================================================================
// Decrement — move to previous record
// ============================================================================

template <typename Traits>
auto xfs_btree_decrement(XfsBtreeCursor<Traits>* cur) -> int {
    // Try to back up within the current leaf block
    int ptr = cur->levels[0].ptr - 1;
    if (ptr >= 1) {
        cur->levels[0].ptr = ptr;
        return 0;
    }

    // Walk up the tree until we find a level where we can back up
    int level = 0;
    for (level = 1; level < cur->nlevels; level++) {
        ptr = cur->levels[level].ptr - 1;
        if (ptr >= 1) {
            cur->levels[level].ptr = ptr;
            break;
        }
    }

    if (level >= cur->nlevels) {
        // At the very beginning of the tree
        return -ENOENT;
    }

    // Walk back down from 'level' to leaf, always taking the rightmost child
    for (int lev = level - 1; lev >= 0; lev--) {
        uint64_t child_block = cur->ptr_at(lev + 1, cur->levels[lev + 1].ptr);
        int rc = cur->read_block(lev, child_block);
        if (rc != 0) {
            return rc;
        }
        cur->levels[lev].ptr = cur->numrecs(lev);  // rightmost entry
    }

    return 0;
}

// ============================================================================
// Get record at current cursor position
// ============================================================================

template <typename Traits>
auto xfs_btree_get_rec(const XfsBtreeCursor<Traits>* cur) -> typename Traits::IRec {
    int ptr = cur->levels[0].ptr;
    const auto* rec = cur->rec_at(ptr);
    return Traits::decode_rec(rec);
}

// ============================================================================
// Mutable cursor helpers
// ============================================================================

template <typename Traits>
auto XfsBtreeCursor<Traits>::rec_at_mut(int idx) -> Rec* {
    uint8_t* base = levels[0].bp->data + Traits::HDR_LEN;
    return reinterpret_cast<Rec*>(base + (static_cast<size_t>(idx - 1) * Traits::REC_LEN));
}

template <typename Traits>
auto XfsBtreeCursor<Traits>::key_at_mut(int level, int idx) -> Key* {
    uint8_t* base = levels[level].bp->data + Traits::HDR_LEN;
    return reinterpret_cast<Key*>(base + (static_cast<size_t>(idx - 1) * Traits::KEY_LEN));
}

template <typename Traits>
auto XfsBtreeCursor<Traits>::ptr_addr(int level, int idx) -> uint8_t* {
    int nrecs = numrecs(level);
    uint8_t* base = levels[level].bp->data + Traits::HDR_LEN;
    uint8_t* ptr_base = base + (static_cast<size_t>(nrecs) * Traits::KEY_LEN);
    return ptr_base + (static_cast<size_t>(idx - 1) * Traits::PTR_LEN);
}

template <typename Traits>
void XfsBtreeCursor<Traits>::set_ptr(int level, int idx, uint64_t blockno) {
    // Pointers are after keys. But we need to compute based on the current numrecs
    // which includes the pointers area layout. We access the raw pointer slot directly.
    int nrecs = numrecs(level);
    uint8_t* base = levels[level].bp->data + Traits::HDR_LEN;
    uint8_t* ptr_base = base + (static_cast<size_t>(nrecs) * Traits::KEY_LEN);
    uint8_t* p = ptr_base + (static_cast<size_t>(idx - 1) * Traits::PTR_LEN);

    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        __be32 val = __be32::from_cpu(static_cast<uint32_t>(blockno));
        __builtin_memcpy(p, &val, 4);
    } else {
        __be64 val = __be64::from_cpu(blockno);
        __builtin_memcpy(p, &val, 8);
    }
}

template <typename Traits>
void XfsBtreeCursor<Traits>::set_numrecs(int level, int nrecs) {
    if (levels[level].bp == nullptr) {
        return;
    }
    if constexpr (Traits::TYPE == XfsBtreeType::SHORT) {
        auto* hdr = reinterpret_cast<XfsBtreeSblock*>(levels[level].bp->data);
        hdr->bb_numrecs = __be16::from_cpu(static_cast<uint16_t>(nrecs));
    } else {
        auto* hdr = reinterpret_cast<XfsBtreeLblock*>(levels[level].bp->data);
        hdr->bb_numrecs = __be16::from_cpu(static_cast<uint16_t>(nrecs));
    }
}

// ============================================================================
// B+tree update — overwrite record at current cursor position
// ============================================================================

template <typename Traits>
auto xfs_btree_update(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp, const typename Traits::IRec& irec) -> int {
    int ptr = cur->levels[0].ptr;
    int nr = cur->numrecs(0);
    if (ptr < 1 || ptr > nr || cur->levels[0].bp == nullptr) {
        return -EINVAL;
    }

    // Encode the in-memory record to on-disk format
    auto* rec = cur->rec_at_mut(ptr);
    typename Traits::Rec new_rec{};
    Traits::encode_rec(&new_rec, irec);
    __builtin_memcpy(rec, &new_rec, Traits::REC_LEN);

    // Log the modified record
    auto rec_offset = static_cast<uint32_t>(reinterpret_cast<uint8_t*>(rec) - cur->levels[0].bp->data);
    xfs_trans_log_buf(tp, cur->levels[0].bp, rec_offset, static_cast<uint32_t>(Traits::REC_LEN));

    // Update the key at parent levels if the first record was modified
    if (ptr == 1) {
        typename Traits::Key key;
        Traits::init_key_from_rec(&key, rec);
        for (int lev = 1; lev < cur->nlevels; lev++) {
            int parent_ptr = cur->levels[lev].ptr;
            auto* pkey = cur->key_at_mut(lev, parent_ptr);
            __builtin_memcpy(pkey, &key, Traits::KEY_LEN);
            auto key_off = static_cast<uint32_t>(reinterpret_cast<uint8_t*>(pkey) - cur->levels[lev].bp->data);
            xfs_trans_log_buf(tp, cur->levels[lev].bp, key_off, static_cast<uint32_t>(Traits::KEY_LEN));
            if (parent_ptr != 1) {
                break;  // only propagate if we changed the leftmost key
            }
        }
    }

    return 0;
}

// ============================================================================
// B+tree insert — add a new record in sorted order
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
    return static_cast<int>((block_size - Traits::HDR_LEN) / (Traits::KEY_LEN + Traits::PTR_LEN));
}

}  // anonymous namespace

template <typename Traits>
auto xfs_btree_insert(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp, const typename Traits::IRec& irec, uint64_t root_block,
                      uint8_t nlevels, uint64_t* new_root, uint8_t* new_nlevels) -> int {
    using Key = typename Traits::Key;
    using Rec = typename Traits::Rec;

    if (new_root != nullptr) *new_root = root_block;
    if (new_nlevels != nullptr) *new_nlevels = nlevels;

    // First, lookup the position where the record should be inserted (GE)
    int rc = xfs_btree_lookup(cur, root_block, nlevels, irec, XfsBtreeLookup::GE);
    int insert_ptr = 0;
    if (rc == -ENOENT) {
        // All records are less than irec — insert at end
        insert_ptr = cur->numrecs(0) + 1;
    } else if (rc == 0) {
        // Cursor is at GE position — insert before it
        insert_ptr = cur->levels[0].ptr;
    } else {
        return rc;
    }

    // Encode the new record
    Rec new_rec{};
    Traits::encode_rec(&new_rec, irec);

    int nr = cur->numrecs(0);
    int max_recs = btree_max_recs_leaf<Traits>(cur->mount->block_size);

    if (nr < max_recs) {
        // Room in current leaf — shift records right and insert
        uint8_t* base = cur->levels[0].bp->data + Traits::HDR_LEN;
        if (insert_ptr <= nr) {
            // Shift records from insert_ptr..nr to insert_ptr+1..nr+1
            std::memmove(base + (static_cast<size_t>(insert_ptr) * Traits::REC_LEN),
                         base + (static_cast<size_t>(insert_ptr - 1) * Traits::REC_LEN),
                         static_cast<size_t>(nr - insert_ptr + 1) * Traits::REC_LEN);
        }
        // Write the new record
        __builtin_memcpy(base + (static_cast<size_t>(insert_ptr - 1) * Traits::REC_LEN), &new_rec, Traits::REC_LEN);
        cur->set_numrecs(0, nr + 1);
        cur->levels[0].ptr = insert_ptr;

        // Log the entire modified region
        xfs_trans_log_buf_full(tp, cur->levels[0].bp);

        // Update parent keys if we inserted at position 1
        if (insert_ptr == 1) {
            Key key;
            Traits::init_key_from_rec(&key, reinterpret_cast<const Rec*>(base));
            for (int lev = 1; lev < cur->nlevels; lev++) {
                int pp = cur->levels[lev].ptr;
                auto* pkey = cur->key_at_mut(lev, pp);
                __builtin_memcpy(pkey, &key, Traits::KEY_LEN);
                xfs_trans_log_buf_full(tp, cur->levels[lev].bp);
                if (pp != 1) {
                    break;
                }
            }
        }
        return 0;
    }

    // Leaf is full — need to split.
    // For now, implement a simplified split: allocate a new block, move the
    // right half of records there, insert the new record in the appropriate half,
    // and propagate the new key/pointer up to the parent.
    // This is a simplified version; a full implementation would handle recursive
    // splits and root growth.
    mod::dbg::log("[xfs btree] leaf split needed but not yet fully implemented\n");
    return -ENOSPC;
}

// ============================================================================
// B+tree delete — remove record at current cursor position
// ============================================================================

template <typename Traits>
auto xfs_btree_delete(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp) -> int {
    using Key = typename Traits::Key;
    using Rec = typename Traits::Rec;

    int ptr = cur->levels[0].ptr;
    int nr = cur->numrecs(0);
    if (ptr < 1 || ptr > nr || cur->levels[0].bp == nullptr) {
        return -EINVAL;
    }

    // Shift records left to fill the gap
    uint8_t* base = cur->levels[0].bp->data + Traits::HDR_LEN;
    if (ptr < nr) {
        std::memmove(base + (static_cast<size_t>(ptr - 1) * Traits::REC_LEN), base + (static_cast<size_t>(ptr) * Traits::REC_LEN),
                     static_cast<size_t>(nr - ptr) * Traits::REC_LEN);
    }
    cur->set_numrecs(0, nr - 1);

    // Log the entire modified leaf
    xfs_trans_log_buf_full(tp, cur->levels[0].bp);

    // Update parent keys if we deleted the first record
    if (ptr == 1 && nr > 1) {
        Key key;
        Traits::init_key_from_rec(&key, reinterpret_cast<const Rec*>(base));
        for (int lev = 1; lev < cur->nlevels; lev++) {
            int pp = cur->levels[lev].ptr;
            auto* pkey = cur->key_at_mut(lev, pp);
            __builtin_memcpy(pkey, &key, Traits::KEY_LEN);
            xfs_trans_log_buf_full(tp, cur->levels[lev].bp);
            if (pp != 1) {
                break;
            }
        }
    }

    // If the leaf became empty, we'd need to remove it from the parent
    // and potentially rebalance.  For now, leave the empty block in place.
    if (nr - 1 == 0) {
        mod::dbg::log("[xfs btree] leaf block is now empty — block removal not yet implemented\n");
    }

    // Adjust cursor position
    if (cur->levels[0].ptr > nr - 1 && nr - 1 > 0) {
        cur->levels[0].ptr = nr - 1;
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
