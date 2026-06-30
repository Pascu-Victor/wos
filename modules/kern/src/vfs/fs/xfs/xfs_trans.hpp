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
// XfsTransaction is heap-allocated, so keep enough headroom for selfhost Git
// workloads without putting this array on kernel stacks.
constexpr int XFS_TRANS_MAX_ITEMS = 512;  // max logged items per transaction

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

// Transaction structure
struct XfsTransaction {
    XfsMountContext* mount{};
    std::array<XfsTransItem, XFS_TRANS_MAX_ITEMS> items{};
    int item_count{};
    bool overflowed{};
    bool committed{};
    bool cancelled{};
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

// Commit the transaction - write log entries and mark buffers dirty.
// Returns 0 on success.
auto xfs_trans_commit(XfsTransaction* tp) -> int;

// Cancel a transaction - discard all changes.
void xfs_trans_cancel(XfsTransaction* tp);

}  // namespace ker::vfs::xfs
