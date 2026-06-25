// XFS Transaction implementation.
//
// Provides transactional metadata modification.  The current model writes one
// compact log record per committed transaction, then marks metadata buffers
// dirty for deferred writeback.
//
// Reference: reference/xfs/xfs_trans.c, reference/xfs/xfs_trans_buf.c

#include "xfs_trans.hpp"

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <platform/dbg/dbg.hpp>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_log.hpp>

#include "vfs/fs/xfs/xfs_inode.hpp"
#include "vfs/fs/xfs/xfs_mount.hpp"

namespace ker::vfs::xfs {

auto xfs_trans_alloc(XfsMountContext* mount) -> XfsTransaction* {
    if (mount == nullptr) {
        return nullptr;
    }
    if (mount->read_only) {
        mod::dbg::log("[xfs trans] cannot allocate transaction on read-only mount\n");
        return nullptr;
    }

    auto* tp = new XfsTransaction{};
    tp->mount = mount;
    return tp;
}

void xfs_trans_log_buf(XfsTransaction* tp, BufHead* bp, uint32_t offset, uint32_t len) {
    if (tp == nullptr || bp == nullptr) {
        return;
    }

    // Check if this buffer (or a different buffer for the same disk block)
    // is already logged in this transaction.
    for (int i = 0; i < tp->item_count; i++) {
        XfsTransItem& item = tp->items.at(static_cast<size_t>(i));
        if (item.type != XfsLogItemType::BUFFER || item.buf.bp == nullptr) {
            continue;
        }
        BufHead const* existing = item.buf.bp;

        if (existing == bp) {
            // Same buffer pointer - extend the logged region.
            uint32_t const OLD_END = item.buf.offset + item.buf.len;
            uint32_t const NEW_END = offset + len;
            uint32_t const START = (offset < item.buf.offset) ? offset : item.buf.offset;
            uint32_t const END = (NEW_END > OLD_END) ? NEW_END : OLD_END;
            item.buf.offset = START;
            item.buf.len = END - START;
            item.buf.dirty = true;
            return;
        }

        // Different buffer for the same disk block (e.g. two bread_multi
        // calls for the same AG header block - one modifying the AGI, the
        // other the AGF).  Merge the dirty region from the new buffer into
        // the existing one so that a single write carries all changes.
        if (existing->bdev == bp->bdev && existing->block_no == bp->block_no && existing->size == bp->size) {
            __builtin_memcpy(existing->data + offset, bp->data + offset, len);
            uint32_t const OLD_END = item.buf.offset + item.buf.len;
            uint32_t const NEW_END = offset + len;
            uint32_t const START = (offset < item.buf.offset) ? offset : item.buf.offset;
            uint32_t const END = (NEW_END > OLD_END) ? NEW_END : OLD_END;
            item.buf.offset = START;
            item.buf.len = END - START;
            item.buf.dirty = true;
            // No refcount bump - the caller will release their own reference
            // to bp normally (via brelse or cursor destructor).
            return;
        }
    }

    if (tp->item_count >= XFS_TRANS_MAX_ITEMS) {
        mod::dbg::log("[xfs trans] too many items in transaction\n");
        return;
    }

    // The transaction takes its own reference on the buffer.  This
    // prevents use-after-free when other holders (e.g. btree cursors)
    // call brelse() before the transaction commits.
    bp->refcount.fetch_add(1, std::memory_order_relaxed);

    XfsTransItem& item = tp->items.at(static_cast<size_t>(tp->item_count++));
    item.type = XfsLogItemType::BUFFER;
    item.buf.bp = bp;
    item.buf.offset = offset;
    item.buf.len = len;
    item.buf.dirty = true;
}

void xfs_trans_log_buf_full(XfsTransaction* tp, BufHead* bp) {
    if (bp == nullptr) {
        return;
    }
    xfs_trans_log_buf(tp, bp, 0, static_cast<uint32_t>(bp->size));
}

void xfs_trans_log_inode(XfsTransaction* tp, XfsInode* ip) {
    if (tp == nullptr || ip == nullptr) {
        return;
    }

    // Check if already logged
    for (int i = 0; i < tp->item_count; i++) {
        XfsTransItem const& item = tp->items.at(static_cast<size_t>(i));
        if (item.type == XfsLogItemType::INODE && item.inode.ip == ip) {
            return;  // already tracked
        }
    }

    if (tp->item_count >= XFS_TRANS_MAX_ITEMS) {
        mod::dbg::log("[xfs trans] too many items in transaction\n");
        return;
    }

    XfsTransItem& item = tp->items.at(static_cast<size_t>(tp->item_count++));
    item.type = XfsLogItemType::INODE;
    item.inode.ip = ip;
}

auto xfs_trans_commit(XfsTransaction* tp) -> int {
    if (tp == nullptr) {
        return -EINVAL;
    }
    if (tp->committed || tp->cancelled) {
        return -EINVAL;
    }

    // Phase 1: Write dirty inodes back to their buffers so the buffer
    // data is up-to-date before we write the log record.
    for (int i = 0; i < tp->item_count; i++) {
        XfsTransItem const& item = tp->items.at(static_cast<size_t>(i));
        if (item.type == XfsLogItemType::INODE && item.inode.ip != nullptr) {
            int const WRC = xfs_inode_write(item.inode.ip, tp);
            if (WRC != 0) {
                mod::dbg::log("[xfs trans] inode %lu write-back failed: %d\n", static_cast<unsigned long>(item.inode.ip->ino), WRC);
            }
        }
    }

    // Phase 2: Write-ahead log - serialize all buffer modifications to the
    // journal before flushing any data.  This ensures recoverability.
    int const LOG_RC = xfs_log_write(tp->mount, tp->items.data(), tp->item_count);
    if (LOG_RC != 0 && LOG_RC != -EINVAL) {
        // Log write failure is not fatal if the log isn't active (read-only
        // or no log area), but is serious otherwise.
        mod::dbg::log("[xfs trans] log write failed: %d\n", LOG_RC);
    }

    // Phase 3: Mark all dirty metadata buffers as dirty in the cache.
    // The journal (written above) ensures recoverability; the buffers will
    // be flushed to disk by LRU writeback, avoiding per-transaction I/O.
    int const RC = 0;
    for (int i = 0; i < tp->item_count; i++) {
        XfsTransItem& item = tp->items.at(static_cast<size_t>(i));
        if (item.type == XfsLogItemType::BUFFER && item.buf.dirty && item.buf.bp != nullptr) {
            bdirty(item.buf.bp);
            brelse(item.buf.bp);
            item.buf.bp = nullptr;
        }
    }

    auto* mount = tp->mount;
    tp->committed = true;
    delete tp;
    if (mount != nullptr) {
        kick_dirty_buffer_cache_writeback(mount->device);
    }
    return RC;
}

void xfs_trans_cancel(XfsTransaction* tp) {
    if (tp == nullptr) {
        return;
    }
    if (tp->committed || tp->cancelled) {
        return;
    }

    // Release the transaction's buffer references (taken in xfs_trans_log_buf).
    for (int i = 0; i < tp->item_count; i++) {
        XfsTransItem& item = tp->items.at(static_cast<size_t>(i));
        if (item.type == XfsLogItemType::BUFFER && item.buf.bp != nullptr) {
            brelse(item.buf.bp);
            item.buf.bp = nullptr;
        }
    }

    tp->cancelled = true;
    delete tp;
}

}  // namespace ker::vfs::xfs
