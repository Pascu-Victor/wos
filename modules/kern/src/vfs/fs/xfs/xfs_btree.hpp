#pragma once

// XFS B+Tree infrastructure — generic traversal and lookup engine.
//
// XFS uses B+trees for:
//   - Free space allocation (bnobt / cntbt) — short-form, AG-rooted
//   - Inode allocation (inobt / finobt)   — short-form, AG-rooted
//   - Extent mapping (bmbt)               — long-form, inode-rooted
//   - Directory hash index (DA btree)     — handled separately in xfs_dir2.cpp
//
// This implements a C++ template-based B+tree cursor with generic lookup,
// increment, and decrement operations.  Type-specific traits structs provide
// the key comparison, record decoding, and block I/O.
//
// Reference: reference/xfs/libxfs/xfs_btree.h, reference/xfs/libxfs/xfs_btree.c

#include <array>
#include <cstddef>
#include <cstdint>
#include <dev/block_device.hpp>
#include <net/endian.hpp>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_format.hpp>
#include <vfs/fs/xfs/xfs_mount.hpp>

#include "vfs/fs/xfs/xfs_trans.hpp"

namespace ker::vfs::xfs {

// ============================================================================
// Btree cursor — tracks position at each level during tree traversal
// ============================================================================

constexpr int XFS_BTREE_MAXLEVELS = 9;  // sufficient for any XFS btree

// Per-level state in the cursor
struct XfsBtreeLevel {
    BufHead* bp;  // buffer for the block at this level (nullptr if not loaded)
    int ptr;      // 1-based index into records/keys at this level
};

// Lookup direction
enum class XfsBtreeLookup : uint8_t {
    LE = 0,  // less than or equal
    EQ = 1,  // exact match
    GE = 2,  // greater than or equal
};

// B+tree type — determines pointer format
enum class XfsBtreeType : uint8_t {
    SHORT = 0,  // AG btrees (bnobt, cntbt, inobt) — __be32 pointers
    LONG = 1,   // inode btrees (bmbt) — __be64 pointers
};

// ============================================================================
// Btree traits — type-specific operations as template parameter
// ============================================================================

// Each btree type provides a traits struct with these members:
//
//   using Key = ...;      // on-disk key type (e.g. xfs_alloc_key)
//   using Rec = ...;      // on-disk record type (e.g. xfs_alloc_rec)
//   using IRec = ...;     // in-memory decoded record
//
//   static constexpr XfsBtreeType type = ...;
//   static constexpr uint32_t magic = ...;          // expected bb_magic
//   static constexpr size_t key_len = sizeof(Key);
//   static constexpr size_t rec_len = sizeof(Rec);
//   static constexpr size_t ptr_len = ...;          // 4 or 8
//
//   // Compare a key against the search target.  Returns <0, 0, >0.
//   static auto cmp_key(const Key* key, const IRec& target) -> int;
//
//   // Extract key from a record.
//   static void init_key_from_rec(Key* key, const Rec* rec);
//
//   // Decode on-disk record to in-memory form.
//   static auto decode_rec(const Rec* rec) -> IRec;
//
//   // Header size for this btree's blocks (v5 / CRC).
//   static constexpr size_t hdr_len = ...;
//

// ============================================================================
// Btree traits implementations for each XFS btree type
// ============================================================================

// --- bnobt (free space by block number) ---
struct XfsBnobtTraits {
    using Key = XfsAllocKey;
    using Rec = XfsAllocRec;
    struct IRec {
        xfs_agblock_t startblock;
        xfs_extlen_t blockcount;
    };

    static constexpr XfsBtreeType TYPE = XfsBtreeType::SHORT;
    static constexpr uint32_t MAGIC = XFS_ABTB_CRC_MAGIC;
    static constexpr size_t KEY_LEN = sizeof(Key);
    static constexpr size_t REC_LEN = sizeof(Rec);
    static constexpr size_t PTR_LEN = 4;
    static constexpr size_t HDR_LEN = XFS_BTREE_SBLOCK_CRC_LEN;

    static auto cmp_key(const Key* key, const IRec& target) -> int {
        uint32_t k = key->ar_startblock.to_cpu();
        if (k < target.startblock) {
            return -1;
        }
        if (k > target.startblock) {
            return 1;
        }
        return 0;
    }

    static void init_key_from_rec(Key* key, const Rec* rec) {
        key->ar_startblock = rec->ar_startblock;
        key->ar_blockcount = rec->ar_blockcount;
    }

    static auto decode_rec(const Rec* rec) -> IRec { return {rec->ar_startblock.to_cpu(), rec->ar_blockcount.to_cpu()}; }

    static void encode_rec(Rec* rec, const IRec& irec) {
        rec->ar_startblock = __be32::from_cpu(irec.startblock);
        rec->ar_blockcount = __be32::from_cpu(irec.blockcount);
    }
};

// --- cntbt (free space by count, then block) ---
struct XfsCntbtTraits {
    using Key = XfsAllocKey;
    using Rec = XfsAllocRec;
    struct IRec {
        xfs_agblock_t startblock;
        xfs_extlen_t blockcount;
    };

    static constexpr XfsBtreeType TYPE = XfsBtreeType::SHORT;
    static constexpr uint32_t MAGIC = XFS_ABTC_CRC_MAGIC;
    static constexpr size_t KEY_LEN = sizeof(Key);
    static constexpr size_t REC_LEN = sizeof(Rec);
    static constexpr size_t PTR_LEN = 4;
    static constexpr size_t HDR_LEN = XFS_BTREE_SBLOCK_CRC_LEN;

    // cntbt is sorted by (blockcount, startblock)
    static auto cmp_key(const Key* key, const IRec& target) -> int {
        uint32_t kcount = key->ar_blockcount.to_cpu();
        if (kcount < target.blockcount) {
            return -1;
        }
        if (kcount > target.blockcount) {
            return 1;
        }
        uint32_t kblock = key->ar_startblock.to_cpu();
        if (kblock < target.startblock) {
            return -1;
        }
        if (kblock > target.startblock) {
            return 1;
        }
        return 0;
    }

    static void init_key_from_rec(Key* key, const Rec* rec) {
        key->ar_startblock = rec->ar_startblock;
        key->ar_blockcount = rec->ar_blockcount;
    }

    static auto decode_rec(const Rec* rec) -> IRec {
        return {.startblock = rec->ar_startblock.to_cpu(), .blockcount = rec->ar_blockcount.to_cpu()};
    }

    static void encode_rec(Rec* rec, const IRec& irec) {
        rec->ar_startblock = __be32::from_cpu(irec.startblock);
        rec->ar_blockcount = __be32::from_cpu(irec.blockcount);
    }
};

// --- inobt (inode allocation btree) ---
struct XfsInobtTraits {
    using Key = XfsInobtKey;
    using Rec = XfsInobtRec;
    struct IRec {
        xfs_agino_t startino;
        uint32_t freecount;
        uint64_t free_mask;  // 64-bit free inode bitmask
    };

    static constexpr XfsBtreeType TYPE = XfsBtreeType::SHORT;
    static constexpr uint32_t MAGIC = XFS_IBT_CRC_MAGIC;
    static constexpr size_t KEY_LEN = sizeof(Key);
    static constexpr size_t REC_LEN = sizeof(Rec);
    static constexpr size_t PTR_LEN = 4;
    static constexpr size_t HDR_LEN = XFS_BTREE_SBLOCK_CRC_LEN;

    static auto cmp_key(const Key* key, const IRec& target) -> int {
        uint32_t k = key->ir_startino.to_cpu();
        if (k < target.startino) {
            return -1;
        }
        if (k > target.startino) {
            return 1;
        }
        return 0;
    }

    static void init_key_from_rec(Key* key, const Rec* rec) { key->ir_startino = rec->ir_startino; }

    static auto decode_rec(const Rec* rec) -> IRec {
        return {
            .startino = rec->ir_startino.to_cpu(),
            .freecount = rec->ir_u.f.ir_freecount.to_cpu(),
            .free_mask = rec->ir_free.to_cpu(),
        };
    }

    static void encode_rec(Rec* rec, const IRec& irec) {
        rec->ir_startino = __be32::from_cpu(irec.startino);
        rec->ir_u.f.ir_freecount = __be32::from_cpu(irec.freecount);
        rec->ir_free = __be64::from_cpu(irec.free_mask);
    }
};

// --- finobt (free inode btree) — same format, different magic ---
struct XfsFinobtTraits : XfsInobtTraits {
    static constexpr uint32_t MAGIC = XFS_FIBT_CRC_MAGIC;
};

// --- bmbt (extent map B+tree, long-form) ---
struct XfsBmbtTraits {
    using Key = XfsBmbtKey;
    using Rec = XfsBmbtRec;
    using IRec = XfsBmbtIrec;

    static constexpr XfsBtreeType TYPE = XfsBtreeType::LONG;
    static constexpr uint32_t MAGIC = XFS_BMAP_CRC_MAGIC;
    static constexpr size_t KEY_LEN = sizeof(Key);
    static constexpr size_t REC_LEN = sizeof(Rec);
    static constexpr size_t PTR_LEN = 8;
    static constexpr size_t HDR_LEN = XFS_BTREE_LBLOCK_CRC_LEN;

    static auto cmp_key(const Key* key, const IRec& target) -> int {
        uint64_t k = key->br_startoff.to_cpu();
        if (k < target.br_startoff) {
            return -1;
        }
        if (k > target.br_startoff) {
            return 1;
        }
        return 0;
    }

    static void init_key_from_rec(Key* key, const Rec* rec) {
        // Extract startoff from the packed record
        uint64_t l0 = rec->l0.to_cpu();
        uint64_t startoff = (l0 >> 9) & 0x3FFFFFFFFFFFFFULL;
        key->br_startoff = __be64::from_cpu(startoff);
    }

    static auto decode_rec(const Rec* rec) -> IRec { return xfs_bmbt_rec_unpack(rec); }

    static void encode_rec(Rec* rec, const IRec& irec) { *rec = xfs_bmbt_rec_pack(irec); }
};

// ============================================================================
// Generic B+tree cursor class
// ============================================================================

template <typename Traits>
class XfsBtreeCursor {
   public:
    using Key = typename Traits::Key;
    using Rec = typename Traits::Rec;
    using IRec = typename Traits::IRec;

    XfsMountContext* mount{};
    uint8_t nlevels{};  // actual depth of tree
    std::array<XfsBtreeLevel, XFS_BTREE_MAXLEVELS> levels{};

    // For AG btrees: which AG and its start block
    xfs_agnumber_t agno{};

    XfsBtreeCursor() {
        for (auto& l : levels) {
            l.bp = nullptr;
            l.ptr = 0;
        }
    }

    ~XfsBtreeCursor() { release(); }

    // Release all held buffers
    void release() {
        for (auto& level : levels) {
            if (level.bp != nullptr) {
                brelse(level.bp);
                level.bp = nullptr;
            }
            level.ptr = 0;
        }
    }

    // Non-copyable
    XfsBtreeCursor(const XfsBtreeCursor&) = delete;
    auto operator=(const XfsBtreeCursor&) -> XfsBtreeCursor& = delete;

    // Get number of records in the block at a given level
    auto numrecs(int level) const -> int;

    // Get pointer to the i-th key at a given level (1-based)
    auto key_at(int level, int idx) const -> const Key*;

    // Get pointer to the i-th record at leaf level (1-based)
    auto rec_at(int idx) const -> const Rec*;

    // Get child pointer at an internal level (1-based)
    auto ptr_at(int level, int idx) const -> uint64_t;

    // Mutable access to leaf record at 1-based index
    auto rec_at_mut(int idx) -> Rec*;

    // Mutable access to key at a given level at 1-based index
    auto key_at_mut(int level, int idx) -> Key*;

    // Get pointer address at an internal level (1-based) for writing
    auto ptr_addr(int level, int idx) -> uint8_t*;

    // Set child pointer at an internal level (1-based)
    void set_ptr(int level, int idx, uint64_t blockno);

    // Set the numrecs in the header at a given level
    void set_numrecs(int level, int nrecs);

    // Read a btree block into the cursor at the given level
    auto read_block(int level, uint64_t blockno) -> int;

    // Get the left/right sibling block number at a given level.
    // Returns NULLFSBLOCK / NULLAGBLOCK if no sibling.
    auto left_sibling(int level) const -> uint64_t;
    auto right_sibling(int level) const -> uint64_t;
};

// ============================================================================
// Generic B+tree operations (lookup, increment, decrement, get_rec)
// ============================================================================

// Lookup a record in the btree.
// root_block: the block number of the root (AG-relative for short, absolute for long)
// nlevels: the depth of the tree
// target: the search key in decoded form
// dir: lookup direction (LE, EQ, GE)
// Returns 0 on success (cursor positioned at result), -ENOENT if not found,
// or negative errno on I/O error.
template <typename Traits>
auto xfs_btree_lookup(XfsBtreeCursor<Traits>* cur, uint64_t root_block, uint8_t nlevels, const typename Traits::IRec& target,
                      XfsBtreeLookup dir) -> int;

// Move cursor to the next record.  Returns 0 on success, -ENOENT if at end.
template <typename Traits>
auto xfs_btree_increment(XfsBtreeCursor<Traits>* cur) -> int;

// Move cursor to the previous record.  Returns 0 on success, -ENOENT if at start.
template <typename Traits>
auto xfs_btree_decrement(XfsBtreeCursor<Traits>* cur) -> int;

// Get the record at the current cursor position.
template <typename Traits>
auto xfs_btree_get_rec(const XfsBtreeCursor<Traits>* cur) -> typename Traits::IRec;

// ============================================================================
// B+tree mutation operations (write path)
// ============================================================================

// Update the record at the current cursor position in-place.
// Encodes irec to on-disk format and overwrites the leaf record.
// Logs the modified region through the transaction.
// Returns 0 on success, negative errno on failure.
template <typename Traits>
auto xfs_btree_update(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp, const typename Traits::IRec& irec) -> int;

// Insert a new record into the btree at the correct sorted position.
// The cursor is first positioned via lookup.  If the leaf is full, the block
// is split and the new key propagated upward (possibly growing the tree).
// Returns 0 on success, negative errno on failure.
template <typename Traits>
auto xfs_btree_insert(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp, const typename Traits::IRec& irec, uint64_t root_block,
                      uint8_t nlevels, uint64_t* new_root, uint8_t* new_nlevels) -> int;

// Delete the record at the current cursor position.
// Shifts remaining records left.  If a block becomes empty, it is freed and
// the parent key/pointer is removed (recursively if needed).
// Returns 0 on success, negative errno on failure.
template <typename Traits>
auto xfs_btree_delete(XfsBtreeCursor<Traits>* cur, XfsTransaction* tp) -> int;

}  // namespace ker::vfs::xfs
