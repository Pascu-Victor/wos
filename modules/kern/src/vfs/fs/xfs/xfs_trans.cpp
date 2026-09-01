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
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sched/scheduler.hpp>
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

struct XfsTransactionArena {
    XfsTransactionArena* next{};
    size_t total_slots{};
    size_t free_slots{};
    bool permanent_reserve{};
};

struct XfsTransactionPool {
    ker::mod::sys::Spinlock lock;
    XfsTransaction* free_list{};
    XfsTransactionArena* arenas{};
    size_t arena_count{};
};

XfsTransactionPool transaction_pool{};

void xfs_trans_free_ifork(XfsIfork* fork) {
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

auto xfs_trans_clone_ifork(const XfsIfork& source, XfsIfork* destination) -> int {
    if (destination == nullptr) {
        return -EINVAL;
    }

    destination->format = source.format;
    switch (source.format) {
        case XFS_DINODE_FMT_LOCAL:
            destination->local.data = nullptr;
            destination->local.size = source.local.size;
            if (source.local.size == 0) {
                return 0;
            }
            if (source.local.data == nullptr) {
                return -EIO;
            }
            destination->local.data = new (std::nothrow) uint8_t[source.local.size];
            if (destination->local.data == nullptr) {
                return -ENOMEM;
            }
            __builtin_memcpy(destination->local.data, source.local.data, source.local.size);
            return 0;

        case XFS_DINODE_FMT_EXTENTS:
            destination->extents.count = source.extents.count;
            destination->extents.capacity = source.extents.capacity;
            destination->extents.list = nullptr;
            if (source.extents.count > source.extents.capacity || (source.extents.count != 0 && source.extents.list == nullptr)) {
                return -EIO;
            }
            if (xfs_ifork_extents_uses_inline(source.extents)) {
                destination->extents.list = xfs_ifork_extents_inline_data(destination->extents);
            } else if (source.extents.capacity != 0) {
                destination->extents.list = new (std::nothrow) XfsBmbtIrec[source.extents.capacity];
                if (destination->extents.list == nullptr) {
                    return -ENOMEM;
                }
            }
            for (uint32_t i = 0; i < source.extents.count; ++i) {
                destination->extents.list[i] = source.extents.list[i];
            }
            return 0;

        case XFS_DINODE_FMT_BTREE:
            destination->btree.level = source.btree.level;
            destination->btree.numrecs = source.btree.numrecs;
            destination->btree.root = nullptr;
            destination->btree.root_size = source.btree.root_size;
            if (source.btree.root_size == 0) {
                return 0;
            }
            if (source.btree.root == nullptr) {
                return -EIO;
            }
            destination->btree.root = new (std::nothrow) uint8_t[source.btree.root_size];
            if (destination->btree.root == nullptr) {
                return -ENOMEM;
            }
            __builtin_memcpy(destination->btree.root, source.btree.root, source.btree.root_size);
            return 0;

        default:
            return -EOPNOTSUPP;
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
            xfs_trans_free_ifork(&undo->data_fork);
        }
        if (undo->owns_attr_fork) {
            xfs_trans_free_ifork(&undo->attr_fork);
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
        xfs_trans_free_ifork(&undo->ip->data_fork);
        undo->ip->data_fork = undo->data_fork;
        if (undo->ip->data_fork.format == XFS_DINODE_FMT_EXTENTS &&
            undo->data_fork.extents.list == xfs_ifork_extents_inline_data(undo->data_fork.extents)) {
            undo->ip->data_fork.extents.list = xfs_ifork_extents_inline_data(undo->ip->data_fork.extents);
        }
        xfs_trans_free_ifork(&undo->ip->attr_fork);
        undo->ip->attr_fork = undo->attr_fork;
        if (undo->ip->attr_fork.format == XFS_DINODE_FMT_EXTENTS &&
            undo->attr_fork.extents.list == xfs_ifork_extents_inline_data(undo->attr_fork.extents)) {
            undo->ip->attr_fork.extents.list = xfs_ifork_extents_inline_data(undo->ip->attr_fork.extents);
        }
        undo->ip->size = undo->size;
        undo->ip->nblocks = undo->nblocks;
        undo->ip->atime = undo->atime;
        undo->ip->mtime = undo->mtime;
        undo->ip->ctime = undo->ctime;
        undo->ip->crtime = undo->crtime;
        undo->ip->nextents = undo->nextents;
        undo->ip->anextents = undo->anextents;
        undo->ip->mode = undo->mode;
        undo->ip->nlink = undo->nlink;
        undo->ip->forkoff = undo->forkoff;
        undo->ip->has_attr_fork = undo->has_attr_fork;
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
        undo->owns_attr_fork = false;
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
    tp->retired_range_count = 0;
}

void xfs_trans_discard_retired_ranges(XfsTransaction* tp) {
    if (tp == nullptr || tp->mount == nullptr || tp->mount->device == nullptr) {
        return;
    }

    for (size_t i = 0; i < tp->retired_range_count; ++i) {
        XfsTransRetiredRange const& range = tp->retired_ranges.at(i);
        // Ordinary holders are detached immediately and free themselves on
        // their final brelse(). Only an already-running device write requires
        // a retry before the metadata lock may permit block reuse.
        while (!retire_bdev_range(tp->mount->device, range.block_no, range.count)) {
            ker::mod::sched::kern_yield();
        }
    }
}

void xfs_trans_add_arena_locked(void* arena, size_t bytes) {
    auto* header = new (arena) XfsTransactionArena{};
    header->permanent_reserve = transaction_pool.arena_count == 0;
    if (header->permanent_reserve &&
        !ker::mod::mm::phys::page_reassign_owner(arena, ker::mod::mm::PhysicalPageOwner::XFS_TRANSACTION_METADATA_RESERVE)) {
        ker::mod::dbg::panic_handler("XFS failed to account permanent transaction arena");
    }
    header->next = transaction_pool.arenas;
    transaction_pool.arenas = header;
    transaction_pool.arena_count++;
    constexpr size_t HEADER_BYTES = (sizeof(XfsTransactionArena) + alignof(XfsTransaction) - 1) & ~(alignof(XfsTransaction) - 1);
    auto* next = static_cast<uint8_t*>(arena) + HEADER_BYTES;
    size_t remaining = bytes - HEADER_BYTES;
    while (remaining >= XFS_TRANS_STRIDE) {
        auto* tp = new (next) XfsTransaction{};
        tp->pool_arena = header;
        tp->pool_next = transaction_pool.free_list;
        transaction_pool.free_list = tp;
        header->total_slots++;
        header->free_slots++;
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
        static_cast<XfsTransactionArena*>(tp->pool_arena)->free_slots--;
    }
    transaction_pool.lock.unlock_irqrestore(IRQF);
    return tp;
}

auto xfs_trans_pool_alloc() -> XfsTransaction* {
    if (XfsTransaction* tp = xfs_trans_pool_pop()) {
        xfs_trans_reset_for_reuse(tp);
        return tp;
    }

    void* const ARENA = ker::mod::mm::phys::page_alloc_full_overwrite(ker::mod::mm::PhysicalPageOwner::XFS_TRANSACTION_METADATA,
                                                                      XFS_TRANS_ARENA_BYTES, "xfs_transactions");
    if (ARENA != nullptr) {
        uint64_t const IRQF = transaction_pool.lock.lock_irqsave();
        xfs_trans_add_arena_locked(ARENA, XFS_TRANS_ARENA_BYTES);
        XfsTransaction* tp = transaction_pool.free_list;
        if (tp != nullptr) {
            transaction_pool.free_list = tp->pool_next;
            tp->pool_next = nullptr;
            static_cast<XfsTransactionArena*>(tp->pool_arena)->free_slots--;
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
    auto* arena = static_cast<XfsTransactionArena*>(tp->pool_arena);
    if (arena == nullptr) {
        delete tp;
        return;
    }
    void* retired_arena = nullptr;
    uint64_t const IRQF = transaction_pool.lock.lock_irqsave();
    tp->pool_next = transaction_pool.free_list;
    transaction_pool.free_list = tp;
    arena->free_slots++;
    if (arena->free_slots == arena->total_slots && !arena->permanent_reserve) {
        XfsTransaction** link = &transaction_pool.free_list;
        while (*link != nullptr) {
            if ((*link)->pool_arena == arena) {
                *link = (*link)->pool_next;
            } else {
                link = &(*link)->pool_next;
            }
        }
        XfsTransactionArena** arena_link = &transaction_pool.arenas;
        while (*arena_link != nullptr && *arena_link != arena) {
            arena_link = &(*arena_link)->next;
        }
        if (*arena_link == arena) {
            *arena_link = arena->next;
            transaction_pool.arena_count--;
            retired_arena = arena;
        }
    }
    transaction_pool.lock.unlock_irqrestore(IRQF);
    if (retired_arena != nullptr) {
        ker::mod::mm::phys::page_free(retired_arena);
    }
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
    int const PREPARE_RC = xfs_log_prepare_transaction(mount);
    // A published read-write mount must never mutate metadata without its
    // journal.  Unmounted synthetic contexts are retained for focused helper
    // tests that exercise transaction rollback without an on-disk log.
    if (PREPARE_RC != 0 && (PREPARE_RC != -ENODEV || mount->mounted)) {
        mod::dbg::log("[xfs trans] cannot prepare log transaction: %d", PREPARE_RC);
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
    XfsTransBufUndo const* span_undo = nullptr;
    for (XfsTransBufUndo const* undo = tp->buf_undo; undo != nullptr; undo = undo->next) {
        if (undo->bp == bp) {
            // A retired transaction buffer can be reintroduced as a distinct
            // cache object for the same device span. Logging through that
            // alias merges into the transaction's first (canonical) buffer,
            // but later changes through the canonical buffer do not
            // automatically update the alias. Capture is the before-mutation
            // boundary, so refresh a reused alias here before its caller can
            // overwrite newer metadata with stale contents.
            for (int i = 0; i < tp->item_count; ++i) {
                XfsTransItem const& item = tp->items[i];
                if (item.type != XfsLogItemType::BUFFER || item.buf.bp == nullptr || item.buf.bp == bp || item.buf.bp->bdev != bp->bdev ||
                    item.buf.bp->block_no != bp->block_no || item.buf.bp->size != bp->size) {
                    continue;
                }
                if (item.buf.bp->data == nullptr) {
                    tp->error = -EIO;
                    return -EIO;
                }
                __builtin_memcpy(bp->data, item.buf.bp->data, bp->size);
                break;
            }
            return 0;
        }
        if (span_undo == nullptr && undo->bp != nullptr && undo->bp->bdev == bp->bdev && undo->bp->block_no == bp->block_no &&
            undo->size == bp->size) {
            span_undo = undo;
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

    // Prevent writeback from observing either the capture copy or alias
    // synchronization below. The hold remains owned by the undo record until
    // commit/cancel discards it.
    bjournal_hold(bp);
    undo->journal_held = true;

    // A pinned metadata buffer can be retired from the cache while a
    // transaction still owns it, allowing a replacement BufHead for the same
    // device span.  Every alias must share the first before-image, otherwise
    // cancellation can restore a mixture of transaction moments.  Seed the
    // replacement with the transaction's current canonical contents before
    // its caller mutates it.
    if (span_undo != nullptr) {
        BufHead const* current = span_undo->bp;
        for (int i = 0; i < tp->item_count; ++i) {
            XfsTransItem const& item = tp->items[i];
            if (item.type == XfsLogItemType::BUFFER && item.buf.bp != nullptr && item.buf.bp->bdev == bp->bdev &&
                item.buf.bp->block_no == bp->block_no && item.buf.bp->size == bp->size) {
                current = item.buf.bp;
                break;
            }
        }
        if (current == nullptr || current->data == nullptr || span_undo->before_image == nullptr) {
            bjournal_release(bp);
            undo->journal_held = false;
            delete[] undo->before_image;
            delete undo;
            tp->error = -EIO;
            return -EIO;
        }
        __builtin_memcpy(undo->before_image, span_undo->before_image, bp->size);
        __builtin_memcpy(bp->data, current->data, bp->size);
    } else {
        __builtin_memcpy(undo->before_image, bp->data, bp->size);
    }

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
    undo->size = ip->size;
    undo->nblocks = ip->nblocks;
    undo->atime = ip->atime;
    undo->mtime = ip->mtime;
    undo->ctime = ip->ctime;
    undo->crtime = ip->crtime;
    undo->nextents = ip->nextents;
    undo->anextents = ip->anextents;
    undo->mode = ip->mode;
    undo->nlink = ip->nlink;
    undo->forkoff = ip->forkoff;
    undo->has_attr_fork = ip->has_attr_fork;
    undo->dirty = ip->dirty;
    undo->dir_generation = ip->dir_generation;
    undo->dir_leaf_index_complete_generation = ip->dir_leaf_index_complete_generation;
    undo->dir_leaf_index_complete = ip->dir_leaf_index_complete;
    undo->dir_name_filter = ip->dir_name_filter;
    undo->dir_name_filter_complete = ip->dir_name_filter_complete;
    int rc = xfs_trans_clone_ifork(ip->data_fork, &undo->data_fork);
    if (rc != 0) {
        delete undo;
        if (rc == -ENOMEM) {
            tp->error = rc;
        }
        return rc;
    }
    undo->owns_data_fork = true;

    if (ip->has_attr_fork) {
        rc = xfs_trans_clone_ifork(ip->attr_fork, &undo->attr_fork);
    } else {
        undo->attr_fork.format = XFS_DINODE_FMT_LOCAL;
        undo->attr_fork.local.data = nullptr;
        undo->attr_fork.local.size = 0;
        rc = 0;
    }
    if (rc != 0) {
        xfs_trans_free_ifork(&undo->data_fork);
        undo->owns_data_fork = false;
        delete undo;
        if (rc == -ENOMEM) {
            tp->error = rc;
        }
        return rc;
    }
    undo->owns_attr_fork = true;

    undo->next = tp->inode_undo;
    tp->inode_undo = undo;
    return 0;
}

auto xfs_trans_retire_bdev_range(XfsTransaction* tp, uint64_t block_no, size_t count) -> int {
    if (tp == nullptr || tp->mount == nullptr || tp->mount->device == nullptr || count == 0 || tp->committed || tp->cancelled ||
        count - 1 > UINT64_MAX - block_no) {
        return -EINVAL;
    }

    for (size_t i = 0; i < tp->retired_range_count; ++i) {
        XfsTransRetiredRange const& range = tp->retired_ranges.at(i);
        if (range.block_no == block_no && range.count == count) {
            return 0;
        }
    }
    if (tp->retired_range_count >= tp->retired_ranges.size()) {
        tp->error = -EFBIG;
        return -EFBIG;
    }

    tp->retired_ranges.at(tp->retired_range_count++) = XfsTransRetiredRange{.block_no = block_no, .count = count};
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
    if (LOG_RC != 0 && (LOG_RC != -ENODEV || tp->mount->mounted)) {
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
    tp->mount->mutation_sequence.fetch_add(1, std::memory_order_release);
    xfs_trans_discard_undo(tp);
    int const CHECKPOINT_RC = xfs_log_checkpoint_if_needed(tp->mount);
    if (CHECKPOINT_RC != 0) {
        // The transaction is already owned by the retained log batch. Keep
        // its checkpoint state for the next explicit flush/reservation retry.
        mod::dbg::log("[xfs trans] deferred ordered checkpoint failed: %d", CHECKPOINT_RC);
    }
    xfs_trans_discard_retired_ranges(tp);
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

#ifdef WOS_SELFTEST
namespace {

auto xfs_retirement_selftest_read(ker::dev::BlockDevice* dev, uint64_t /*block*/, size_t count, void* buffer) -> int {
    __builtin_memset(buffer, 0, count * dev->block_size);
    return 0;
}

auto xfs_retirement_selftest_write(ker::dev::BlockDevice* /*dev*/, uint64_t /*block*/, size_t /*count*/, const void* /*buffer*/) -> int {
    return 0;
}

}  // namespace

auto xfs_selftest_transaction_cancel_restores_nlink() -> bool {
    XfsMountContext mount{};
    XfsInode inode{};
    inode.mount = &mount;
    inode.nlink = 2;
    inode.data_fork.format = XFS_DINODE_FMT_EXTENTS;

    XfsTransaction* tp = xfs_trans_alloc(&mount);
    if (tp == nullptr || xfs_trans_capture_inode(tp, &inode) != 0) {
        if (tp != nullptr) {
            xfs_trans_cancel(tp);
        }
        return false;
    }

    inode.nlink = 0;
    inode.dirty = true;
    xfs_trans_cancel(tp);
    return inode.nlink == 2 && !inode.dirty;
}

auto xfs_selftest_transaction_cancel_restores_attr_fork() -> bool {
    XfsMountContext mount{};
    XfsInode inode{};
    inode.mount = &mount;
    inode.data_fork.format = XFS_DINODE_FMT_EXTENTS;
    inode.attr_fork.format = XFS_DINODE_FMT_LOCAL;
    inode.attr_fork.local.size = 4;
    inode.attr_fork.local.data = new (std::nothrow) uint8_t[4]{0, 4, 0, 0};
    inode.has_attr_fork = inode.attr_fork.local.data != nullptr;
    inode.forkoff = 24;
    inode.anextents = 0;
    inode.atime = 11;
    inode.mtime = 22;
    inode.ctime = 33;
    inode.crtime = 44;
    if (!inode.has_attr_fork) {
        return false;
    }

    XfsTransaction* tp = xfs_trans_alloc(&mount);
    if (tp == nullptr || xfs_trans_capture_inode(tp, &inode) != 0) {
        if (tp != nullptr) {
            xfs_trans_cancel(tp);
        }
        delete[] inode.attr_fork.local.data;
        return false;
    }

    delete[] inode.attr_fork.local.data;
    inode.attr_fork.format = XFS_DINODE_FMT_EXTENTS;
    inode.attr_fork.extents.list = xfs_ifork_extents_inline_data(inode.attr_fork.extents);
    inode.attr_fork.extents.count = 1;
    inode.attr_fork.extents.capacity = XFS_IFORK_INLINE_EXTENT_CAPACITY;
    inode.attr_fork.extents.list[0] = {.br_startoff = 0, .br_startblock = 77, .br_blockcount = 1, .br_unwritten = false};
    inode.forkoff = 16;
    inode.anextents = 1;
    inode.nblocks = 1;
    inode.atime = 111;
    inode.mtime = 222;
    inode.ctime = 333;
    inode.crtime = 444;
    inode.dirty = true;

    xfs_trans_cancel(tp);
    bool const OK = inode.has_attr_fork && inode.attr_fork.format == XFS_DINODE_FMT_LOCAL && inode.attr_fork.local.data != nullptr &&
                    inode.attr_fork.local.size == 4 && inode.attr_fork.local.data[0] == 0 && inode.attr_fork.local.data[1] == 4 &&
                    inode.forkoff == 24 && inode.anextents == 0 && inode.nblocks == 0 && inode.atime == 11 && inode.mtime == 22 &&
                    inode.ctime == 33 && inode.crtime == 44 && !inode.dirty;
    delete[] inode.attr_fork.local.data;
    return OK;
}

auto xfs_selftest_transaction_retired_ranges_commit_only() -> bool {
    constexpr uint64_t BLOCK = 96;
    constexpr size_t COUNT = 8;

    ker::dev::BlockDevice dev{};
    dev.block_size = 512;
    dev.total_blocks = 1024;
    dev.read_blocks = xfs_retirement_selftest_read;
    dev.write_blocks = xfs_retirement_selftest_write;

    XfsMountContext mount{};
    mount.device = &dev;

    auto cache_range = [&]() -> BufHead* {
        BufHead* bh = bget_multi(&dev, BLOCK, COUNT);
        if (bh == nullptr) {
            return nullptr;
        }
        __builtin_memset(bh->data, 0xA5, bh->size);
        bdirty(bh);
        return bh;
    };

    BufHead* cancelled_pinned = cache_range();
    bool ok = cancelled_pinned != nullptr && has_cached_bdev_range(&dev, BLOCK, COUNT);
    XfsTransaction* cancelled = xfs_trans_alloc(&mount);
    bool const CANCEL_REGISTERED = cancelled != nullptr && xfs_trans_retire_bdev_range(cancelled, BLOCK, COUNT) == 0;
    ok = ok && CANCEL_REGISTERED;
    if (cancelled != nullptr) {
        xfs_trans_cancel(cancelled);
    }
    ok = ok && has_cached_bdev_range(&dev, BLOCK, COUNT);
    brelse(cancelled_pinned);

    discard_bdev_range(&dev, BLOCK, COUNT);
    BufHead* committed_pinned = cache_range();
    ok = ok && committed_pinned != nullptr && has_cached_bdev_range(&dev, BLOCK, COUNT);
    XfsTransaction* committed = xfs_trans_alloc(&mount);
    bool const COMMIT_REGISTERED = committed != nullptr && xfs_trans_retire_bdev_range(committed, BLOCK, COUNT) == 0;
    ok = ok && COMMIT_REGISTERED;
    if (committed != nullptr) {
        int const COMMIT_RC = xfs_trans_commit(committed);
        ok = ok && COMMIT_RC == 0;
    }
    ok = ok && !has_cached_bdev_range(&dev, BLOCK, COUNT);
    brelse(committed_pinned);

    invalidate_bdev(&dev);
    return ok;
}

auto xfs_selftest_transaction_cancel_restores_replaced_buffer_alias() -> bool {
    constexpr uint64_t BLOCK = 144;
    constexpr size_t COUNT = 8;
    constexpr uint8_t ORIGINAL = 0x31;
    constexpr uint8_t FIRST_MUTATION = 0x52;
    constexpr uint8_t SECOND_MUTATION = 0x73;
    constexpr uint8_t THIRD_MUTATION = 0x94;

    ker::dev::BlockDevice dev{};
    dev.block_size = 512;
    dev.total_blocks = 1024;
    dev.read_blocks = xfs_retirement_selftest_read;
    dev.write_blocks = xfs_retirement_selftest_write;
    invalidate_bdev(&dev);

    XfsMountContext mount{};
    mount.device = &dev;

    auto filled_with = [](BufHead const* bh, uint8_t value) -> bool {
        if (bh == nullptr || bh->data == nullptr) {
            return false;
        }
        for (size_t i = 0; i < bh->size; ++i) {
            if (bh->data[i] != value) {
                return false;
            }
        }
        return true;
    };

    BufHead* original = bget_multi(&dev, BLOCK, COUNT);
    if (original == nullptr) {
        invalidate_bdev(&dev);
        return false;
    }
    __builtin_memset(original->data, ORIGINAL, original->size);

    XfsTransaction* tp = xfs_trans_alloc(&mount);
    bool ok = tp != nullptr && xfs_trans_capture_buf(tp, original) == 0;
    if (ok) {
        __builtin_memset(original->data, FIRST_MUTATION, original->size);
        xfs_trans_log_buf_full(tp, original);
        ok = retire_bdev_range(&dev, BLOCK, COUNT);
    }

    BufHead* replacement = ok ? bget_multi(&dev, BLOCK, COUNT) : nullptr;
    ok = ok && replacement != nullptr && replacement != original;
    if (ok) {
        ok = xfs_trans_capture_buf(tp, replacement) == 0 && filled_with(replacement, FIRST_MUTATION);
    }
    if (ok) {
        __builtin_memset(replacement->data, SECOND_MUTATION, replacement->size);
        xfs_trans_log_buf_full(tp, replacement);
    }
    if (ok) {
        ok = xfs_trans_capture_buf(tp, original) == 0 && filled_with(original, SECOND_MUTATION);
    }
    if (ok) {
        original->data[0] = THIRD_MUTATION;
        xfs_trans_log_buf(tp, original, 0, 1);
        ok = xfs_trans_capture_buf(tp, replacement) == 0 && replacement->data[0] == THIRD_MUTATION;
    }

    if (tp != nullptr) {
        xfs_trans_cancel(tp);
    }
    ok = ok && filled_with(original, ORIGINAL) && filled_with(replacement, ORIGINAL);

    if (replacement != nullptr) {
        brelse(replacement);
    }
    brelse(original);
    invalidate_bdev(&dev);
    return ok;
}
#endif

}  // namespace ker::vfs::xfs
