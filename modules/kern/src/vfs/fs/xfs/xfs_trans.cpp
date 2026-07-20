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
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sys/spinlock.hpp>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_dir2.hpp>
#include <vfs/fs/xfs/xfs_format.hpp>
#include <vfs/fs/xfs/xfs_log.hpp>

#include "vfs/fs/xfs/xfs_inode.hpp"
#include "vfs/fs/xfs/xfs_mount.hpp"

namespace ker::vfs::xfs {

namespace {

constexpr size_t XFS_TRANS_ARENA_BYTES = size_t{256} * 1024;
constexpr size_t XFS_TRANS_STRIDE = (sizeof(XfsTransaction) + alignof(XfsTransaction) - 1) & ~(alignof(XfsTransaction) - 1);

struct XfsTransactionPool {
    ker::mod::sys::Spinlock lock;
    XfsTransaction* free_list{};
};

XfsTransactionPool transaction_pool{};

void xfs_trans_free_data_fork(XfsIfork* fork) {
    if (fork == nullptr) {
        return;
    }
    switch (fork->format) {
        case XFS_DINODE_FMT_LOCAL:
            delete[] fork->local.data;
            fork->local.data = nullptr;
            fork->local.size = 0;
            break;
        case XFS_DINODE_FMT_EXTENTS:
            if (!xfs_ifork_extents_uses_inline(fork->extents)) {
                delete[] fork->extents.list;
            }
            fork->extents.list = nullptr;
            fork->extents.count = 0;
            fork->extents.capacity = 0;
            break;
        case XFS_DINODE_FMT_BTREE:
            delete[] fork->btree.root;
            fork->btree.root = nullptr;
            fork->btree.root_size = 0;
            break;
        default:
            break;
    }
}

void xfs_trans_discard_undo(XfsTransaction* tp) {
    while (tp->buf_undo != nullptr) {
        XfsTransBufUndo* undo = tp->buf_undo;
        tp->buf_undo = undo->next;
        if (undo->bp != nullptr) {
            if (undo->journal_held) {
                bjournal_release(undo->bp);
            }
            brelse(undo->bp);
        }
        delete[] undo->before_image;
        delete undo;
    }
    while (tp->perag_undo != nullptr) {
        XfsTransPerAgUndo* undo = tp->perag_undo;
        tp->perag_undo = undo->next;
        delete undo;
    }
    while (tp->inode_undo != nullptr) {
        XfsTransInodeUndo* undo = tp->inode_undo;
        tp->inode_undo = undo->next;
        if (undo->owns_data_fork) {
            xfs_trans_free_data_fork(&undo->data_fork);
        }
        delete undo;
    }
}

void xfs_trans_restore_undo(XfsTransaction* tp) {
    for (XfsTransBufUndo* undo = tp->buf_undo; undo != nullptr; undo = undo->next) {
        if (undo->bp == nullptr || undo->before_image == nullptr || undo->bp->data == nullptr || undo->bp->size != undo->size) {
            continue;
        }
        __builtin_memcpy(undo->bp->data, undo->before_image, undo->size);
        for (int i = 0; i < tp->item_count; ++i) {
            XfsTransItem& item = tp->items[i];
            if (item.type != XfsLogItemType::BUFFER || item.buf.bp == nullptr || item.buf.bp == undo->bp) {
                continue;
            }
            if (item.buf.bp->bdev == undo->bp->bdev && item.buf.bp->block_no == undo->bp->block_no && item.buf.bp->size == undo->size) {
                __builtin_memcpy(item.buf.bp->data, undo->before_image, undo->size);
            }
        }
    }

    for (XfsTransPerAgUndo* undo = tp->perag_undo; undo != nullptr; undo = undo->next) {
        if (tp->mount == nullptr || tp->mount->per_ag == nullptr || undo->agno >= tp->mount->ag_count) {
            continue;
        }
        XfsPerAG* pag = &tp->mount->per_ag[undo->agno];
        pag->agf_bno_root = undo->before.agf_bno_root;
        pag->agf_cnt_root = undo->before.agf_cnt_root;
        pag->agf_bno_level = undo->before.agf_bno_level;
        pag->agf_cnt_level = undo->before.agf_cnt_level;
        pag->agf_freeblks = undo->before.agf_freeblks;
        pag->agf_longest = undo->before.agf_longest;
        pag->agf_flcount = undo->before.agf_flcount;
        pag->agf_flfirst = undo->before.agf_flfirst;
        pag->agf_fllast = undo->before.agf_fllast;
        pag->agi_count = undo->before.agi_count;
        pag->agi_root = undo->before.agi_root;
        pag->agi_level = undo->before.agi_level;
        pag->agi_freecount = undo->before.agi_freecount;
        pag->agi_free_root = undo->before.agi_free_root;
        pag->agi_free_level = undo->before.agi_free_level;
        pag->ialloc_hint_startino = undo->before.ialloc_hint_startino;
        pag->ialloc_hint_valid = undo->before.ialloc_hint_valid;
    }

    for (XfsTransInodeUndo* undo = tp->inode_undo; undo != nullptr; undo = undo->next) {
        if (undo->ip == nullptr) {
            continue;
        }
        xfs_trans_free_data_fork(&undo->ip->data_fork);
        undo->ip->data_fork = undo->data_fork;
        if (undo->ip->data_fork.format == XFS_DINODE_FMT_EXTENTS &&
            undo->data_fork.extents.list == xfs_ifork_extents_inline_data(undo->data_fork.extents)) {
            undo->ip->data_fork.extents.list = xfs_ifork_extents_inline_data(undo->ip->data_fork.extents);
        }
        undo->ip->size = undo->size;
        undo->ip->nblocks = undo->nblocks;
        undo->ip->nextents = undo->nextents;
        undo->ip->dirty = undo->dirty;
        undo->ip->dir_generation = undo->dir_generation;
        undo->ip->dir_leaf_index_complete_generation = undo->dir_leaf_index_complete_generation;
        undo->ip->dir_leaf_index_complete = undo->dir_leaf_index_complete;
        undo->ip->dir_name_filter = undo->dir_name_filter;
        undo->ip->dir_name_filter_complete = undo->dir_name_filter_complete;
        if (xfs_inode_isdir(undo->ip)) {
            xfs_dentry_cache_invalidate_dir(undo->ip);
        }
        undo->owns_data_fork = false;
    }
}

void xfs_trans_mark_overflowed(XfsTransaction* tp) {
    if (tp == nullptr) {
        return;
    }
    if (!tp->overflowed) {
        mod::dbg::log("[xfs trans] too many items in transaction (%d max)", XFS_TRANS_MAX_ITEMS);
    }
    tp->overflowed = true;
}

auto xfs_trans_ensure_item_capacity(XfsTransaction* tp) -> bool {
    if (tp == nullptr) {
        return false;
    }
    if (tp->item_count < tp->item_capacity) {
        return true;
    }
    if (tp->item_capacity >= XFS_TRANS_MAX_ITEMS) {
        xfs_trans_mark_overflowed(tp);
        return false;
    }

    auto* expanded = new (std::nothrow) XfsTransItem[XFS_TRANS_MAX_ITEMS];
    if (expanded == nullptr) {
        xfs_trans_mark_overflowed(tp);
        return false;
    }
    for (int i = 0; i < tp->item_count; ++i) {
        expanded[i] = tp->items[i];
    }
    tp->items = expanded;
    tp->item_capacity = XFS_TRANS_MAX_ITEMS;
    return true;
}

void xfs_trans_reset_for_reuse(XfsTransaction* tp) {
    if (tp == nullptr) {
        return;
    }
    xfs_trans_discard_undo(tp);
    if (tp->items != tp->inline_items.data()) {
        delete[] tp->items;
    }
    tp->mount = nullptr;
    tp->pool_next = nullptr;
    tp->items = tp->inline_items.data();
    tp->item_capacity = XFS_TRANS_INLINE_ITEMS;
    tp->item_count = 0;
    tp->error = 0;
    tp->overflowed = false;
    tp->committed = false;
    tp->cancelled = false;
}

void xfs_trans_add_arena_locked(void* arena, size_t bytes) {
    auto* next = static_cast<uint8_t*>(arena);
    size_t remaining = bytes;
    while (remaining >= XFS_TRANS_STRIDE) {
        auto* tp = new (next) XfsTransaction{};
        tp->pool_next = transaction_pool.free_list;
        transaction_pool.free_list = tp;
        next += XFS_TRANS_STRIDE;
        remaining -= XFS_TRANS_STRIDE;
    }
}

auto xfs_trans_pool_pop() -> XfsTransaction* {
    uint64_t const IRQF = transaction_pool.lock.lock_irqsave();
    XfsTransaction* tp = transaction_pool.free_list;
    if (tp != nullptr) {
        transaction_pool.free_list = tp->pool_next;
        tp->pool_next = nullptr;
    }
    transaction_pool.lock.unlock_irqrestore(IRQF);
    return tp;
}

auto xfs_trans_pool_alloc() -> XfsTransaction* {
    if (XfsTransaction* tp = xfs_trans_pool_pop()) {
        xfs_trans_reset_for_reuse(tp);
        return tp;
    }

    void* const ARENA = ker::mod::mm::phys::page_alloc_full_overwrite(XFS_TRANS_ARENA_BYTES, "xfs_transactions");
    if (ARENA != nullptr) {
        uint64_t const IRQF = transaction_pool.lock.lock_irqsave();
        xfs_trans_add_arena_locked(ARENA, XFS_TRANS_ARENA_BYTES);
        XfsTransaction* tp = transaction_pool.free_list;
        if (tp != nullptr) {
            transaction_pool.free_list = tp->pool_next;
            tp->pool_next = nullptr;
        }
        transaction_pool.lock.unlock_irqrestore(IRQF);
        if (tp != nullptr) {
            xfs_trans_reset_for_reuse(tp);
        }
        return tp;
    }

    return new (std::nothrow) XfsTransaction{};
}

void xfs_trans_release(XfsTransaction* tp) {
    if (tp == nullptr) {
        return;
    }
    xfs_trans_reset_for_reuse(tp);
    uint64_t const IRQF = transaction_pool.lock.lock_irqsave();
    tp->pool_next = transaction_pool.free_list;
    transaction_pool.free_list = tp;
    transaction_pool.lock.unlock_irqrestore(IRQF);
}

}  // namespace

XfsTransaction::~XfsTransaction() {
    xfs_trans_discard_undo(this);
    if (items != inline_items.data()) {
        delete[] items;
    }
}

auto xfs_trans_alloc(XfsMountContext* mount) -> XfsTransaction* {
    if (mount == nullptr) {
        return nullptr;
    }
    if (mount->read_only) {
        mod::dbg::log("[xfs trans] cannot allocate transaction on read-only mount");
        return nullptr;
    }

    auto* tp = xfs_trans_pool_alloc();
    if (tp == nullptr) {
        return nullptr;
    }
    tp->mount = mount;
    return tp;
}

void xfs_trans_log_buf(XfsTransaction* tp, BufHead* bp, uint32_t offset, uint32_t len) {
    if (tp == nullptr || bp == nullptr) {
        return;
    }
    if (tp->overflowed) {
        return;
    }

    // Check if this buffer (or a different buffer for the same disk block)
    // is already logged in this transaction.
    for (int i = 0; i < tp->item_count; i++) {
        XfsTransItem& item = tp->items[i];
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

    if (!xfs_trans_ensure_item_capacity(tp)) {
        return;
    }

    // The transaction takes its own reference on the buffer.  This
    // prevents use-after-free when other holders (e.g. btree cursors)
    // call brelse() before the transaction commits.
    bp->refcount.fetch_add(1, std::memory_order_relaxed);

    XfsTransItem& item = tp->items[tp->item_count++];
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
    if (tp->overflowed) {
        return;
    }
    // Check if already logged
    for (int i = 0; i < tp->item_count; i++) {
        XfsTransItem const& item = tp->items[i];
        if (item.type == XfsLogItemType::INODE && item.inode.ip == ip) {
            return;  // already tracked
        }
    }

    if (!xfs_trans_ensure_item_capacity(tp)) {
        return;
    }

    XfsTransItem& item = tp->items[tp->item_count++];
    item.type = XfsLogItemType::INODE;
    item.inode.ip = ip;
}

auto xfs_trans_capture_buf(XfsTransaction* tp, BufHead* bp) -> int {
    if (tp == nullptr || bp == nullptr || bp->data == nullptr || bp->size == 0 || tp->committed || tp->cancelled) {
        return -EINVAL;
    }
    for (XfsTransBufUndo const* undo = tp->buf_undo; undo != nullptr; undo = undo->next) {
        if (undo->bp == bp) {
            return 0;
        }
    }

    auto* undo = new (std::nothrow) XfsTransBufUndo{};
    if (undo == nullptr) {
        tp->error = -ENOMEM;
        return -ENOMEM;
    }
    undo->before_image = new (std::nothrow) uint8_t[bp->size];
    if (undo->before_image == nullptr) {
        delete undo;
        tp->error = -ENOMEM;
        return -ENOMEM;
    }
    bjournal_hold(bp);
    undo->journal_held = true;
    __builtin_memcpy(undo->before_image, bp->data, bp->size);
    bp->refcount.fetch_add(1, std::memory_order_relaxed);
    undo->bp = bp;
    undo->size = bp->size;
    undo->next = tp->buf_undo;
    tp->buf_undo = undo;
    return 0;
}

auto xfs_trans_capture_perag(XfsTransaction* tp, xfs_agnumber_t agno) -> int {
    if (tp == nullptr || tp->mount == nullptr || tp->mount->per_ag == nullptr || agno >= tp->mount->ag_count || tp->committed ||
        tp->cancelled) {
        return -EINVAL;
    }
    for (XfsTransPerAgUndo const* undo = tp->perag_undo; undo != nullptr; undo = undo->next) {
        if (undo->agno == agno) {
            return 0;
        }
    }
    auto* undo = new (std::nothrow) XfsTransPerAgUndo{};
    if (undo == nullptr) {
        tp->error = -ENOMEM;
        return -ENOMEM;
    }
    XfsPerAG const* pag = &tp->mount->per_ag[agno];
    undo->agno = agno;
    undo->before = {
        .agf_bno_root = pag->agf_bno_root,
        .agf_cnt_root = pag->agf_cnt_root,
        .agf_bno_level = pag->agf_bno_level,
        .agf_cnt_level = pag->agf_cnt_level,
        .agf_freeblks = pag->agf_freeblks,
        .agf_longest = pag->agf_longest,
        .agf_flcount = pag->agf_flcount,
        .agf_flfirst = pag->agf_flfirst,
        .agf_fllast = pag->agf_fllast,
        .agi_count = pag->agi_count,
        .agi_root = pag->agi_root,
        .agi_level = pag->agi_level,
        .agi_freecount = pag->agi_freecount,
        .agi_free_root = pag->agi_free_root,
        .agi_free_level = pag->agi_free_level,
        .ialloc_hint_startino = pag->ialloc_hint_startino,
        .ialloc_hint_valid = pag->ialloc_hint_valid,
    };
    undo->next = tp->perag_undo;
    tp->perag_undo = undo;
    return 0;
}

auto xfs_trans_capture_inode(XfsTransaction* tp, XfsInode* ip) -> int {
    if (tp == nullptr || ip == nullptr || tp->committed || tp->cancelled) {
        return -EINVAL;
    }
    for (XfsTransInodeUndo const* undo = tp->inode_undo; undo != nullptr; undo = undo->next) {
        if (undo->ip == ip) {
            return 0;
        }
    }
    auto* undo = new (std::nothrow) XfsTransInodeUndo{};
    if (undo == nullptr) {
        tp->error = -ENOMEM;
        return -ENOMEM;
    }
    undo->ip = ip;
    undo->data_fork = ip->data_fork;
    undo->size = ip->size;
    undo->nblocks = ip->nblocks;
    undo->nextents = ip->nextents;
    undo->dirty = ip->dirty;
    undo->dir_generation = ip->dir_generation;
    undo->dir_leaf_index_complete_generation = ip->dir_leaf_index_complete_generation;
    undo->dir_leaf_index_complete = ip->dir_leaf_index_complete;
    undo->dir_name_filter = ip->dir_name_filter;
    undo->dir_name_filter_complete = ip->dir_name_filter_complete;
    undo->owns_data_fork = true;

    switch (ip->data_fork.format) {
        case XFS_DINODE_FMT_LOCAL:
            undo->data_fork.local.data = nullptr;
            if (ip->data_fork.local.size != 0) {
                if (ip->data_fork.local.data == nullptr) {
                    delete undo;
                    return -EIO;
                }
                undo->data_fork.local.data = new (std::nothrow) uint8_t[ip->data_fork.local.size];
                if (undo->data_fork.local.data == nullptr) {
                    delete undo;
                    tp->error = -ENOMEM;
                    return -ENOMEM;
                }
                __builtin_memcpy(undo->data_fork.local.data, ip->data_fork.local.data, ip->data_fork.local.size);
            }
            break;
        case XFS_DINODE_FMT_EXTENTS:
            if (ip->data_fork.extents.count > ip->data_fork.extents.capacity ||
                (ip->data_fork.extents.count != 0 && ip->data_fork.extents.list == nullptr)) {
                delete undo;
                return -EIO;
            }
            if (xfs_ifork_extents_uses_inline(ip->data_fork.extents)) {
                undo->data_fork.extents.list = xfs_ifork_extents_inline_data(undo->data_fork.extents);
            } else {
                undo->data_fork.extents.list = nullptr;
                if (ip->data_fork.extents.capacity != 0) {
                    undo->data_fork.extents.list = new (std::nothrow) XfsBmbtIrec[ip->data_fork.extents.capacity];
                    if (undo->data_fork.extents.list == nullptr) {
                        delete undo;
                        tp->error = -ENOMEM;
                        return -ENOMEM;
                    }
                    for (uint32_t i = 0; i < ip->data_fork.extents.count; ++i) {
                        undo->data_fork.extents.list[i] = ip->data_fork.extents.list[i];
                    }
                }
            }
            break;
        case XFS_DINODE_FMT_BTREE:
            undo->data_fork.btree.root = nullptr;
            if (ip->data_fork.btree.root_size != 0) {
                if (ip->data_fork.btree.root == nullptr) {
                    delete undo;
                    return -EIO;
                }
                undo->data_fork.btree.root = new (std::nothrow) uint8_t[ip->data_fork.btree.root_size];
                if (undo->data_fork.btree.root == nullptr) {
                    delete undo;
                    tp->error = -ENOMEM;
                    return -ENOMEM;
                }
                __builtin_memcpy(undo->data_fork.btree.root, ip->data_fork.btree.root, ip->data_fork.btree.root_size);
            }
            break;
        default:
            delete undo;
            return -EOPNOTSUPP;
    }

    undo->next = tp->inode_undo;
    tp->inode_undo = undo;
    return 0;
}

auto xfs_trans_commit(XfsTransaction* tp) -> int {
    if (tp == nullptr) {
        return -EINVAL;
    }
    if (tp->committed || tp->cancelled) {
        return -EINVAL;
    }
    if (tp->error != 0) {
        int const ERROR = tp->error;
        xfs_trans_cancel(tp);
        return ERROR;
    }
    if (tp->overflowed) {
        mod::dbg::log("[xfs trans] refusing to commit overfull transaction");
        xfs_trans_cancel(tp);
        return -EFBIG;
    }

    // Phase 1: Write dirty inodes back to their buffers so the buffer
    // data is up-to-date before we write the log record.
    for (int i = 0; i < tp->item_count; i++) {
        XfsTransItem const& item = tp->items[i];
        if (item.type == XfsLogItemType::INODE && item.inode.ip != nullptr) {
            int const WRC = xfs_inode_write(item.inode.ip, tp);
            if (WRC != 0) {
                mod::dbg::log("[xfs trans] inode %lu write-back failed: %d", static_cast<unsigned long>(item.inode.ip->ino), WRC);
                xfs_trans_cancel(tp);
                return WRC;
            }
        }
    }
    if (tp->overflowed) {
        mod::dbg::log("[xfs trans] refusing to commit transaction after inode write-back overflow");
        xfs_trans_cancel(tp);
        return -EFBIG;
    }

    // Phase 2: Write-ahead log - serialize all buffer modifications to the
    // journal before flushing any data.  This ensures recoverability.
    bool log_owns_metadata = false;
    int const LOG_RC = xfs_log_write(tp->mount, tp->items, tp->item_count, &log_owns_metadata);
    if (LOG_RC != 0 && LOG_RC != -ENODEV) {
        mod::dbg::log("[xfs trans] log write failed: %d", LOG_RC);
        if (!log_owns_metadata) {
            xfs_trans_cancel(tp);
            return LOG_RC;
        }
    }

    // Phase 3: Mark all dirty metadata buffers as dirty in the cache.
    // The journal (written above) ensures recoverability; the buffers will
    // be flushed to disk by LRU writeback, avoiding per-transaction I/O.
    int const RC = 0;
    for (int i = 0; i < tp->item_count; i++) {
        XfsTransItem& item = tp->items[i];
        if (item.type == XfsLogItemType::BUFFER && item.buf.dirty && item.buf.bp != nullptr) {
            if (!log_owns_metadata) {
                bdirty(item.buf.bp);
            }
            brelse(item.buf.bp);
            item.buf.bp = nullptr;
        }
    }

    tp->committed = true;
    xfs_trans_discard_undo(tp);
    xfs_trans_release(tp);
    return RC;
}

void xfs_trans_cancel(XfsTransaction* tp) {
    if (tp == nullptr) {
        return;
    }
    if (tp->committed || tp->cancelled) {
        return;
    }

    xfs_trans_restore_undo(tp);

    // Release the transaction's buffer references (taken in xfs_trans_log_buf).
    for (int i = 0; i < tp->item_count; i++) {
        XfsTransItem& item = tp->items[i];
        if (item.type == XfsLogItemType::BUFFER && item.buf.bp != nullptr) {
            brelse(item.buf.bp);
            item.buf.bp = nullptr;
        }
    }

    tp->cancelled = true;
    xfs_trans_release(tp);
}

}  // namespace ker::vfs::xfs
