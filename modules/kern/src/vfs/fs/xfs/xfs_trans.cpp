// XFS Transaction implementation.
//
// Provides transactional metadata modification.  Currently implements a
// simplified version that logs buffer and inode modifications and commits
// them synchronously to disk.  A full implementation would batch transactions
// into log records and write them to the circular journal area.
//
// Reference: reference/xfs/xfs_trans.c, reference/xfs/xfs_trans_buf.c

#include "xfs_trans.hpp"

#include <cerrno>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_log.hpp>

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
    tp->item_count = 0;
    tp->committed = false;
    tp->cancelled = false;
    return tp;
}

void xfs_trans_log_buf(XfsTransaction* tp, BufHead* bp, uint32_t offset, uint32_t len) {
    if (tp == nullptr || bp == nullptr) {
        return;
    }
    if (tp->item_count >= XFS_TRANS_MAX_ITEMS) {
        mod::dbg::log("[xfs trans] too many items in transaction\n");
        return;
    }

    // Check if this buffer (or a different buffer for the same disk block)
    // is already logged in this transaction.
    for (int i = 0; i < tp->item_count; i++) {
        if (tp->items[i].type != XfsLogItemType::Buffer || tp->items[i].buf.bp == nullptr) {
            continue;
        }
        BufHead* existing = tp->items[i].buf.bp;

        if (existing == bp) {
            // Same buffer pointer — extend the logged region.
            uint32_t old_end = tp->items[i].buf.offset + tp->items[i].buf.len;
            uint32_t new_end = offset + len;
            uint32_t start = (offset < tp->items[i].buf.offset) ? offset : tp->items[i].buf.offset;
            uint32_t end = (new_end > old_end) ? new_end : old_end;
            tp->items[i].buf.offset = start;
            tp->items[i].buf.len = end - start;
            tp->items[i].buf.dirty = true;
            return;
        }

        // Different buffer for the same disk block (e.g. two bread_multi
        // calls for the same AG header block — one modifying the AGI, the
        // other the AGF).  Merge the dirty region from the new buffer into
        // the existing one so that a single write carries all changes.
        if (existing->bdev == bp->bdev && existing->block_no == bp->block_no && existing->size == bp->size) {
            __builtin_memcpy(existing->data + offset, bp->data + offset, len);
            uint32_t old_end = tp->items[i].buf.offset + tp->items[i].buf.len;
            uint32_t new_end = offset + len;
            uint32_t start = (offset < tp->items[i].buf.offset) ? offset : tp->items[i].buf.offset;
            uint32_t end = (new_end > old_end) ? new_end : old_end;
            tp->items[i].buf.offset = start;
            tp->items[i].buf.len = end - start;
            tp->items[i].buf.dirty = true;
            // No refcount bump — the caller will release their own reference
            // to bp normally (via brelse or cursor destructor).
            return;
        }
    }

    // The transaction takes its own reference on the buffer.  This
    // prevents use-after-free when other holders (e.g. btree cursors)
    // call brelse() before the transaction commits.
    bp->refcount.fetch_add(1, std::memory_order_relaxed);

    XfsTransItem& item = tp->items[tp->item_count++];
    item.type = XfsLogItemType::Buffer;
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
    if (tp->item_count >= XFS_TRANS_MAX_ITEMS) {
        mod::dbg::log("[xfs trans] too many items in transaction\n");
        return;
    }

    // Check if already logged
    for (int i = 0; i < tp->item_count; i++) {
        if (tp->items[i].type == XfsLogItemType::Inode && tp->items[i].inode.ip == ip) {
            return;  // already tracked
        }
    }

    XfsTransItem& item = tp->items[tp->item_count++];
    item.type = XfsLogItemType::Inode;
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
        XfsTransItem& item = tp->items[i];
        if (item.type == XfsLogItemType::Inode && item.inode.ip != nullptr) {
            int wrc = xfs_inode_write(item.inode.ip, tp);
            if (wrc != 0) {
                mod::dbg::log("[xfs trans] inode %lu write-back failed: %d\n", (unsigned long)item.inode.ip->ino, wrc);
            }
        }
    }

    // Phase 2: Write-ahead log — serialize all buffer modifications to the
    // journal before flushing any data.  This ensures recoverability.
    int log_rc = xfs_log_write(tp->mount, tp->items.data(), tp->item_count);
    if (log_rc != 0 && log_rc != -EINVAL) {
        // Log write failure is not fatal if the log isn't active (read-only
        // or no log area), but is serious otherwise.
        mod::dbg::log("[xfs trans] log write failed: %d\n", log_rc);
    }

    // Phase 3: Flush all dirty buffers synchronously to disk.
    int rc = 0;
    for (int i = 0; i < tp->item_count; i++) {
        XfsTransItem& item = tp->items[i];
        if (item.type == XfsLogItemType::Buffer && item.buf.dirty && item.buf.bp != nullptr) {
#ifdef XFS_DEBUG
            mod::dbg::log("[xfs trans] commit: write buf blk=%lu off=%u len=%u size=%lu\\n", (unsigned long)item.buf.bp->block_no,
                          item.buf.offset, item.buf.len, (unsigned long)item.buf.bp->size);
#endif
            bdirty(item.buf.bp);
            int wrc = bwrite(item.buf.bp);
            if (wrc != 0 && rc == 0) {
                rc = wrc;
            }
            // Release the buffer reference to prevent memory leaks
            // (especially for multi-block buffers not in the cache).
            brelse(item.buf.bp);
            item.buf.bp = nullptr;
        }
    }

    tp->committed = true;
    delete tp;
    return rc;
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
        XfsTransItem& item = tp->items[i];
        if (item.type == XfsLogItemType::Buffer && item.buf.bp != nullptr) {
            brelse(item.buf.bp);
            item.buf.bp = nullptr;
        }
    }

    tp->cancelled = true;
    delete tp;
}

}  // namespace ker::vfs::xfs
