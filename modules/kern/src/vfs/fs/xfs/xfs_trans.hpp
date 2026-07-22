#pragma once

// XFS Transaction API - accumulate metadata modifications and commit them
// atomically to the write-ahead log.
//
// Reference: reference/xfs/xfs_trans.h, reference/xfs/xfs_trans.c

#include <array>
#include <cstddef>
#include <cstdint>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_format.hpp>
#include <vfs/fs/xfs/xfs_inode.hpp>
#include <vfs/fs/xfs/xfs_mount.hpp>

namespace ker::vfs::xfs {

// ============================================================================
// Transaction constants
// ============================================================================

// BMBT rebuilds for large fragmented files can log hundreds of metadata blocks.
// Keep that headroom available, but don't make every tiny create/write
// transaction reserve and zero the full item array.
constexpr int XFS_TRANS_MAX_ITEMS = 512;  // max logged items per transaction
constexpr int XFS_TRANS_INLINE_ITEMS = 32;

// Log item types
enum class XfsLogItemType : uint8_t {
    NONE = 0,
    BUFFER = 1,
    INODE = 2,
};

// A logged buffer region - records which part of a buffer was modified
struct XfsTransBufItem {
    BufHead* bp{};
    uint32_t offset{};  // byte offset of modified region within buffer
    uint32_t len{};     // byte length of modified region
    bool dirty{};
};

// A logged inode
struct XfsTransInodeItem {
    XfsInode* ip{};
};

// A logged item (union of buffer and inode)
struct XfsTransItem {
    XfsLogItemType type{XfsLogItemType::NONE};
    union {
        XfsTransBufItem buf;
        XfsTransInodeItem inode;
    };

    constexpr XfsTransItem() : buf{} {}
};

// Full-buffer before-image retained until the transaction commits.  Captures
// happen before the first metadata write so cancellation can restore cache
// contents without allocating memory or issuing I/O.
struct XfsTransBufUndo {
    BufHead* bp{};
    uint8_t* before_image{};
    size_t size{};
    bool journal_held{};
    XfsTransBufUndo* next{};
};

// Mutable per-AG fields.  The embedded lock is deliberately excluded.
struct XfsTransPerAgState {
    xfs_agblock_t agf_bno_root{};
    xfs_agblock_t agf_cnt_root{};
    uint32_t agf_bno_level{};
    uint32_t agf_cnt_level{};
    uint32_t agf_freeblks{};
    uint32_t agf_longest{};
    uint32_t agf_flcount{};
    uint32_t agf_flfirst{};
    uint32_t agf_fllast{};
    uint32_t agi_count{};
    xfs_agblock_t agi_root{};
    uint32_t agi_level{};
    uint32_t agi_freecount{};
    xfs_agblock_t agi_free_root{};
    uint32_t agi_free_level{};
    xfs_agino_t ialloc_hint_startino{};
    bool ialloc_hint_valid{};
};

struct XfsTransPerAgUndo {
    xfs_agnumber_t agno{};
    XfsTransPerAgState before{};
    XfsTransPerAgUndo* next{};
};

// Deep snapshot of the mutable data-fork state used by block-map updates.
struct XfsTransInodeUndo {
    XfsInode* ip{};
    XfsIfork data_fork{};
    uint64_t size{};
    uint64_t nblocks{};
    uint32_t nextents{};
    uint32_t nlink{};
    bool dirty{};
    uint64_t dir_generation{};
    uint64_t dir_leaf_index_complete_generation{};
    bool dir_leaf_index_complete{};
    std::array<uint64_t, XFS_DIR_NAME_FILTER_WORDS> dir_name_filter{};
    bool dir_name_filter_complete{};
    bool owns_data_fork{};
    XfsTransInodeUndo* next{};
};

// Buffer-cache ranges whose old contents become unreachable only when the
// transaction commits.  Keep them cached on cancellation so the restored
// metadata can still use them; discard them after a successful commit and
// before the caller can release the filesystem metadata lock.
struct XfsTransRetiredRange {
    uint64_t block_no{};
    size_t count{};
};

// Transaction structure
struct XfsTransaction {
    XfsTransaction() = default;
    ~XfsTransaction();

    XfsTransaction(const XfsTransaction&) = delete;
    auto operator=(const XfsTransaction&) -> XfsTransaction& = delete;
    XfsTransaction(XfsTransaction&&) = delete;
    auto operator=(XfsTransaction&&) -> XfsTransaction& = delete;

    XfsMountContext* mount{};
    XfsTransaction* pool_next{};
    std::array<XfsTransItem, XFS_TRANS_INLINE_ITEMS> inline_items{};
    XfsTransItem* items{inline_items.data()};
    int item_capacity{XFS_TRANS_INLINE_ITEMS};
    int item_count{};
    int error{};
    bool overflowed{};
    bool committed{};
    bool cancelled{};
    XfsTransBufUndo* buf_undo{};
    XfsTransPerAgUndo* perag_undo{};
    XfsTransInodeUndo* inode_undo{};
    std::array<XfsTransRetiredRange, XFS_TRANS_MAX_ITEMS> retired_ranges{};
    size_t retired_range_count{};
};

// ============================================================================
// Transaction API
// ============================================================================

// Allocate a new transaction.  Returns nullptr on failure.
auto xfs_trans_alloc(XfsMountContext* mount) -> XfsTransaction*;

// Log a modified buffer region within a transaction.
// The buffer's refcount is held by the transaction until commit/cancel.
void xfs_trans_log_buf(XfsTransaction* tp, BufHead* bp, uint32_t offset, uint32_t len);

// Log an entire buffer as modified.
void xfs_trans_log_buf_full(XfsTransaction* tp, BufHead* bp);

// Log a modified inode within a transaction.
void xfs_trans_log_inode(XfsTransaction* tp, XfsInode* ip);

// Capture transaction-local state before its first mutation.  These helpers
// may allocate and therefore must be called before modifying the object.
auto xfs_trans_capture_buf(XfsTransaction* tp, BufHead* bp) -> int;
auto xfs_trans_capture_perag(XfsTransaction* tp, xfs_agnumber_t agno) -> int;
auto xfs_trans_capture_inode(XfsTransaction* tp, XfsInode* ip) -> int;

// Retire a device-block range only after a successful commit.  This prevents
// stale metadata aliases from contaminating a later owner of recycled blocks.
auto xfs_trans_retire_bdev_range(XfsTransaction* tp, uint64_t block_no, size_t count) -> int;

// Commit the transaction - write log entries and mark buffers dirty.
// Returns 0 on success.
auto xfs_trans_commit(XfsTransaction* tp) -> int;

// Cancel a transaction - discard all changes.
void xfs_trans_cancel(XfsTransaction* tp);

#ifdef WOS_SELFTEST
auto xfs_selftest_transaction_cancel_restores_nlink() -> bool;
auto xfs_selftest_transaction_retired_ranges_commit_only() -> bool;
#endif

}  // namespace ker::vfs::xfs
