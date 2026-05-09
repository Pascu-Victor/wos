// XFS VFS Integration - FileOperations implementation
//
// This module bridges the WOS VFS layer (FileOperations function pointers)
// to the XFS native implementation.  It implements open, read, write, seek,
// readdir, readlink, truncate, close, and stat for XFS.

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/mm/paging.hpp>
#include <vfs/buffer_cache.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/fs/xfs/xfs_alloc.hpp>
#include <vfs/fs/xfs/xfs_bmap.hpp>
#include <vfs/fs/xfs/xfs_dir2.hpp>
#include <vfs/fs/xfs/xfs_format.hpp>
#include <vfs/fs/xfs/xfs_ialloc.hpp>
#include <vfs/fs/xfs/xfs_inode.hpp>
#include <vfs/fs/xfs/xfs_log.hpp>
#include <vfs/fs/xfs/xfs_mount.hpp>
#include <vfs/fs/xfs/xfs_symlink.hpp>
#include <vfs/fs/xfs/xfs_trans.hpp>
#include <vfs/fs/xfs/xfs_vfs.hpp>
#include <vfs/stat.hpp>

#include "dev/block_device.hpp"
#include "platform/dbg/dbg.hpp"
#ifdef XFS_BENCH
#include "platform/tsc/tsc.hpp"
#endif

namespace ker::vfs::xfs {

using log = ker::mod::dbg::logger<"xfs">;

// ============================================================================
// Per-open-file state
// ============================================================================

struct XfsFileData {
    XfsMountContext* mount;
    XfsInode* inode;  // reference-counted inode
};

// ============================================================================
// Path walking
// ============================================================================

// Walk a filesystem-relative path and return the inode.
// An empty path or "/" refers to the root inode.
// Returns a reference-counted inode on success, nullptr on error.

namespace {

auto pointer_looks_like_kernel_object(const void* ptr) -> bool {
    auto addr = reinterpret_cast<uint64_t>(ptr);
    bool in_hhdm = addr >= 0xffff800000000000ULL && addr < 0xffff900000000000ULL;
    bool in_kernel_static = addr >= 0xffffffff80000000ULL && addr < 0xffffffffc0000000ULL;
    return (in_hhdm || in_kernel_static) && ((addr & 0x7ULL) == 0);
}

auto xfs_fsblock_to_dev_block(XfsMountContext* ctx, xfs_fsblock_t fsbno) -> uint64_t {
    auto agno = xfs_ag_number(fsbno, ctx->ag_blk_log);
    auto agbno = xfs_ag_block(fsbno, ctx->ag_blk_log);
    uint64_t linear_block = (static_cast<uint64_t>(agno) * ctx->ag_blocks) + agbno;
    return linear_block * (ctx->block_size / ctx->device->block_size);
}

auto walk_path(XfsMountContext* ctx, const char* path) -> XfsInode* {
    // Start at the root inode
    XfsInode* ip = xfs_inode_read(ctx, ctx->root_ino);
    if (ip == nullptr) {
        return nullptr;
    }

    // Empty path or "/" -> root
    if (path == nullptr || path[0] == '\0') {
        return ip;
    }

    const char* p = path;

    // Skip leading slash if present
    if (*p == '/') {
        p++;
    }

    while (*p != '\0') {
        // Skip consecutive slashes
        while (*p == '/') {
            p++;
        }
        if (*p == '\0') {
            break;
        }

        // Extract component
        const char* comp_start = p;
        while (*p != '\0' && *p != '/') {
            p++;
        }
        auto namelen = static_cast<uint16_t>(p - comp_start);

        if (namelen == 0) {
            continue;
        }

        // Current inode must be a directory
        if (!xfs_inode_isdir(ip)) {
            xfs_inode_release(ip);
            return nullptr;
        }

        // Look up the component
        XfsDirEntry de{};
        int ret = xfs_dir_lookup(ip, comp_start, namelen, &de);
        if (ret != 0) {
            xfs_inode_release(ip);
            return nullptr;
        }

        // Release parent, read child
        xfs_inode_release(ip);
        ip = xfs_inode_read(ctx, de.ino);
        if (ip == nullptr) {
            return nullptr;
        }
    }

    return ip;
}

// ============================================================================
// FileOperations callbacks
// ============================================================================

auto xfs_vfs_close(File* f) -> int {
    if (f == nullptr) {
        return -EBADF;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd != nullptr) {
        // Remote VFS executable loads open files read-only; syncing the whole
        // filesystem on every close stalls those fetches and can trip WKI fencing.
        constexpr int XFS_O_TRUNC_FLAG = 01000;
        int accmode = f->open_flags & 3;
        bool write_like_close = (accmode == 1 || accmode == 2 || (f->open_flags & XFS_O_TRUNC_FLAG) != 0);
        if (write_like_close && pointer_looks_like_kernel_object(xfd->mount)) {
            auto* mount = xfd->mount;
            if (pointer_looks_like_kernel_object(mount->device)) {
                sync_blockdev(mount->device);
            }
        }
        if (xfd->inode != nullptr) {
            xfs_inode_release(xfd->inode);
        }
        delete xfd;
        f->private_data = nullptr;
    }
    return 0;
}

auto xfs_vfs_read(File* f, void* buf, size_t count, size_t offset) -> ssize_t {
    if (f == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }

    XfsInode* ip = xfd->inode;
    XfsMountContext* ctx = xfd->mount;

    // Can't read directories as files
    if (xfs_inode_isdir(ip)) {
        return -EISDIR;
    }

    // Inline data (LOCAL format)
    if (ip->data_fork.format == XFS_DINODE_FMT_LOCAL) {
        if (offset >= ip->size) {
            return 0;
        }
        size_t avail = ip->size - offset;
        size_t to_copy = count < avail ? count : avail;
        std::memcpy(buf, ip->data_fork.local.data + offset, to_copy);
        return static_cast<ssize_t>(to_copy);
    }

    // EXTENTS or BTREE - block-based read
    if (offset >= ip->size) {
        return 0;
    }
    size_t avail = ip->size - offset;
    size_t remaining = count < avail ? count : avail;
    size_t total_read = 0;
    auto* dst = static_cast<uint8_t*>(buf);

#ifdef XFS_BENCH
    static std::atomic<uint64_t> s_read_calls{0};
    static std::atomic<uint64_t> s_read_ns_bmap{0};
    static std::atomic<uint64_t> s_read_ns_io{0};
    static std::atomic<uint64_t> s_read_bytes{0};
    uint64_t acc_bmap = 0, acc_io = 0;
#endif

    while (remaining > 0) {
        // Compute the logical file block and byte offset within the block
        auto file_block = static_cast<xfs_fileoff_t>((offset + total_read) >> ctx->block_log);
        size_t block_off = (offset + total_read) & (ctx->block_size - 1);

        XfsBmapResult bmap{};
#ifdef XFS_BENCH
        uint64_t t0 = ker::mod::tsc::get_ns();
#endif
        int ret = xfs_bmap_lookup(ip, file_block, &bmap);
#ifdef XFS_BENCH
        acc_bmap += ker::mod::tsc::get_ns() - t0;
#endif
        if (ret < 0) {
            return (total_read > 0) ? static_cast<ssize_t>(total_read) : ret;
        }

        if (bmap.is_hole || bmap.startblock == NULLFSBLOCK) {
            // Hole - return zeros
            size_t hole_bytes = (static_cast<size_t>(bmap.blockcount) * ctx->block_size) - block_off;
            hole_bytes = std::min(hole_bytes, remaining);
            std::memset(dst + total_read, 0, hole_bytes);
            total_read += hole_bytes;
            remaining -= hole_bytes;
            continue;
        }

        // Read contiguous blocks in bulk, bypassing the buffer cache.
        xfs_fsblock_t disk_block = bmap.startblock;

        // How many bytes does this extent cover from our current position?
        size_t extent_bytes = (static_cast<size_t>(bmap.blockcount) * ctx->block_size) - block_off;
        size_t chunk = std::min(extent_bytes, remaining);

#ifdef XFS_BENCH
        t0 = ker::mod::tsc::get_ns();
#endif
        bool dma_safe_dst = ((reinterpret_cast<uintptr_t>(dst + total_read) & (ker::mod::mm::paging::PAGE_SIZE - 1)) == 0);
        if (block_off == 0 && (chunk & (ctx->block_size - 1)) == 0 && dma_safe_dst) {
            // Aligned, full-block read.  Check the buffer cache first: a dirty
            // buffer contains data not yet flushed to disk, so a direct read
            // would return stale on-disk bytes instead of the intended content.
            uint64_t dev_block = xfs_fsblock_to_dev_block(ctx, disk_block);
            size_t dev_count = chunk / ctx->device->block_size;
            BufHead* cache_bp = (dev_count <= 1) ? bread(ctx->device, dev_block) : bread_multi(ctx->device, dev_block, dev_count);
#ifdef XFS_BENCH
            acc_io += ker::mod::tsc::get_ns() - t0;
#endif
            if (cache_bp != nullptr) {
                std::memcpy(dst + total_read, cache_bp->data, chunk);
                brelse(cache_bp);
            } else {
                int rc = dev::block_read(ctx->device, dev_block, dev_count, dst + total_read);
                if (rc != 0) {
                    return (total_read > 0) ? static_cast<ssize_t>(total_read) : -EIO;
                }
            }
        } else {
            // Partial or unaligned - fall back to single cached block.
            chunk = std::min(ctx->block_size - block_off, remaining);
            BufHead* bp = xfs_buf_read(ctx, disk_block);
#ifdef XFS_BENCH
            acc_io += ker::mod::tsc::get_ns() - t0;
#endif
            if (bp == nullptr) {
                return (total_read > 0) ? static_cast<ssize_t>(total_read) : -EIO;
            }
            std::memcpy(dst + total_read, bp->data + block_off, chunk);
            brelse(bp);
        }

        total_read += chunk;
        remaining -= chunk;
    }

#ifdef XFS_BENCH
    s_read_ns_bmap.fetch_add(acc_bmap, std::memory_order_relaxed);
    s_read_ns_io.fetch_add(acc_io, std::memory_order_relaxed);
    s_read_bytes.fetch_add(total_read, std::memory_order_relaxed);
    uint64_t n = s_read_calls.fetch_add(1, std::memory_order_relaxed);
    if ((n & 63) == 63) {
        uint64_t nb = s_read_ns_bmap.exchange(0, std::memory_order_relaxed);
        uint64_t ni = s_read_ns_io.exchange(0, std::memory_order_relaxed);
        uint64_t by = s_read_bytes.exchange(0, std::memory_order_relaxed);
        uint64_t mbps = (ni > 0) ? (by * 1000ULL / ni) : 0;
        ker::mod::dbg::log("[XFS read bench] call#%lu: bmap=%luus io=%luus bytes=%lu io_MB/s~%lu\n", (unsigned long)n,
                           (unsigned long)(nb / 1000ULL), (unsigned long)(ni / 1000ULL), (unsigned long)by, (unsigned long)mbps);
    }
#endif

    // Diagnostic: detect all-zero reads and dump bmap details
    if (total_read > 0 && offset == 0) {
        auto* dbg_dst = static_cast<const uint8_t*>(buf);
        bool all_zero = true;
        for (size_t i = 0; i < std::min(total_read, static_cast<size_t>(64)); i++) {
            if (dbg_dst[i] != 0) {
                all_zero = false;
                break;
            }
        }
        if (all_zero) {
            XfsBmapResult dbg_bmap{};
            int dbg_rc = xfs_bmap_lookup(ip, 0, &dbg_bmap);
            ker::mod::dbg::log(
                "[XFS-DIAG] ZERO READ: ino=%lu size=%lu nextents=%u fmt=%d nblocks=%lu"
                " bmap_rc=%d hole=%d startblk=%lu blkcnt=%lu unwritten=%d",
                (unsigned long)ip->ino, (unsigned long)ip->size, ip->nextents, ip->data_fork.format, (unsigned long)ip->nblocks, dbg_rc,
                dbg_bmap.is_hole, (unsigned long)dbg_bmap.startblock, (unsigned long)dbg_bmap.blockcount, dbg_bmap.unwritten);
        }
    }

    return static_cast<ssize_t>(total_read);
}

// Maximum blocks to allocate per metadata transaction.
// Each transaction covers one contiguous extent allocation, so the actual
// number of transactions for a sequential write is (total_blocks / batch).
constexpr xfs_extlen_t XFS_WRITE_BATCH_BLOCKS = 65536;  // up to 256 MiB per transaction

auto xfs_vfs_write(File* f, const void* buf, size_t count, size_t offset) -> ssize_t {
    if (f == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }

    XfsInode* ip = xfd->inode;
    XfsMountContext* ctx = xfd->mount;

    if (ctx->read_only) {
        return -EROFS;
    }

    if (xfs_inode_isdir(ip)) {
        return -EISDIR;
    }

    if (count == 0) {
        return 0;
    }

    const auto* src = static_cast<const uint8_t*>(buf);
    size_t total_written = 0;

    auto buffered_write = [&](xfs_fsblock_t disk_block, size_t initial_block_off, size_t bytes, size_t src_offset) -> bool {
        size_t remaining_bytes = bytes;
        size_t block_off = initial_block_off;
        xfs_fsblock_t current_disk_block = disk_block;
        size_t current_src_offset = src_offset;

        while (remaining_bytes > 0) {
            size_t chunk = std::min(ctx->block_size - block_off, remaining_bytes);
            BufHead* bp =
                (block_off == 0 && chunk == ctx->block_size) ? xfs_buf_get(ctx, current_disk_block) : xfs_buf_read(ctx, current_disk_block);
            if (bp == nullptr) {
                return false;
            }

            std::memcpy(bp->data + block_off, src + current_src_offset, chunk);
            bdirty(bp);
            brelse(bp);

            remaining_bytes -= chunk;
            current_src_offset += chunk;
            current_disk_block++;
            block_off = 0;
        }

        return true;
    };

#ifdef XFS_BENCH
    static std::atomic<uint64_t> s_wr_calls{0};
    static std::atomic<uint64_t> s_wr_ns_bmap{0};
    static std::atomic<uint64_t> s_wr_ns_alloc{0};
    static std::atomic<uint64_t> s_wr_ns_io{0};
    static std::atomic<uint64_t> s_wr_ns_ilog{0};
    static std::atomic<uint64_t> s_wr_bytes{0};
    static std::atomic<uint64_t> s_wr_hole_calls{0};
    static std::atomic<uint64_t> s_wr_map_calls{0};
    uint64_t acc_bmap = 0, acc_alloc = 0, acc_io = 0, acc_ilog = 0;
    uint64_t loc_hole = 0, loc_map = 0;
    uint64_t t0 = 0;
#endif

    while (total_written < count) {
        size_t write_pos = offset + total_written;
        auto file_block = static_cast<xfs_fileoff_t>(write_pos >> ctx->block_log);
        size_t block_off = write_pos & (ctx->block_size - 1);

        XfsBmapResult bmap{};
#ifdef XFS_BENCH
        t0 = ker::mod::tsc::get_ns();
#endif
        int ret = xfs_bmap_lookup(ip, file_block, &bmap);
#ifdef XFS_BENCH
        acc_bmap += ker::mod::tsc::get_ns() - t0;
#endif
        if (ret < 0) {
            break;
        }

        if (bmap.is_hole || bmap.startblock == NULLFSBLOCK) {
#ifdef XFS_BENCH
            loc_hole++;
#endif
            // Hole: batch-allocate as many contiguous blocks as we can.
            // bmap.blockcount tells us how many blocks remain in this hole.
            size_t remaining_bytes = count - total_written;
            // Blocks needed to cover remaining write (may start mid-block)
            size_t blocks_needed = (block_off + remaining_bytes + ctx->block_size - 1) >> ctx->block_log;
            // Always allocate at least 1024 blocks (4MB) to amortise per-transaction overhead
            // across many small write() syscalls. The extra blocks become pre-allocated extent
            // that subsequent writes find already mapped, skipping allocation entirely.
            constexpr xfs_extlen_t XFS_WRITE_MIN_ALLOC = 1024;
            blocks_needed = std::max(blocks_needed, static_cast<size_t>(XFS_WRITE_MIN_ALLOC));
            // Cap to hole size and batch limit
            auto hole_blocks = static_cast<xfs_extlen_t>(std::min(bmap.blockcount, static_cast<xfs_filblks_t>(blocks_needed)));
            hole_blocks = std::min(hole_blocks, XFS_WRITE_BATCH_BLOCKS);
            if (hole_blocks == 0) {
                hole_blocks = 1;
            }

#ifdef XFS_BENCH
            t0 = ker::mod::tsc::get_ns();
#endif
            XfsTransaction* tp = xfs_trans_alloc(ctx);
            if (tp == nullptr) {
                break;
            }

            xfs_agnumber_t pref_ag = xfs_ino_ag(ip->ino, ctx->agino_log);

            XfsAllocReq req{};
            req.agno = pref_ag;
            req.agbno = 0;
            req.minlen = 1;
            req.maxlen = hole_blocks;
            req.alignment = 0;

            XfsAllocResult alloc_result{};
            ret = xfs_alloc_extent(ctx, tp, req, &alloc_result);
            if (ret != 0) {
                xfs_trans_cancel(tp);
                break;
            }

            xfs_fsblock_t disk_block = xfs_agbno_to_fsbno(alloc_result.agno, alloc_result.agbno, ctx->ag_blk_log);

            XfsBmbtIrec new_ext{};
            new_ext.br_startoff = file_block;
            new_ext.br_startblock = disk_block;
            new_ext.br_blockcount = alloc_result.len;
            new_ext.br_unwritten = false;

            ret = xfs_bmap_add_extent(ip, tp, new_ext);
            if (ret != 0) {
                xfs_trans_cancel(tp);
                break;
            }

            ip->nblocks += alloc_result.len;
            ip->dirty = true;

            // Log inode into this allocation transaction so the inode commit
            // is piggybacked onto the alloc commit - avoids a second journal
            // write at the end of the write syscall.
            xfs_trans_log_inode(tp, ip);

            ret = xfs_trans_commit(tp);
#ifdef XFS_BENCH
            acc_alloc += ker::mod::tsc::get_ns() - t0;
#endif
            if (ret != 0) {
                break;
            }

            // Write the data blocks covered by this allocation through the
            // buffer cache so subsequent cached reads observe the new contents.
            {
                size_t extent_bytes = static_cast<size_t>(alloc_result.len) << ctx->block_log;
                size_t write_end = offset + count;
                size_t extent_start = static_cast<size_t>(file_block) << ctx->block_log;
                size_t extent_end = extent_start + extent_bytes;

                // Compute the slice of this extent actually covered by [offset, offset+count)
                size_t slice_start = std::max(extent_start, offset + total_written);
                size_t slice_end = std::min(extent_end, write_end);
                size_t slice_bytes = (slice_end > slice_start) ? (slice_end - slice_start) : 0;

#ifdef XFS_BENCH
                t0 = ker::mod::tsc::get_ns();
#endif
                if (slice_bytes > 0) {
                    size_t slice_block_off = slice_start - extent_start;
                    if (!buffered_write(disk_block, slice_block_off, slice_bytes, slice_start - offset)) {
#ifdef XFS_BENCH
                        acc_io += ker::mod::tsc::get_ns() - t0;
#endif
                        goto write_done;
                    }
                    total_written += slice_bytes;
                }
#ifdef XFS_BENCH
                acc_io += ker::mod::tsc::get_ns() - t0;
#endif
            }
        } else {
#ifdef XFS_BENCH
            loc_map++;
#endif
            // Block already mapped - write the contiguous extent in bulk.
            xfs_fsblock_t disk_block = bmap.startblock;

            size_t extent_bytes = (static_cast<size_t>(bmap.blockcount) * ctx->block_size) - block_off;
            size_t chunk = std::min(extent_bytes, count - total_written);

#ifdef XFS_BENCH
            t0 = ker::mod::tsc::get_ns();
#endif
            if (!buffered_write(disk_block, block_off, chunk, total_written)) {
#ifdef XFS_BENCH
                acc_io += ker::mod::tsc::get_ns() - t0;
#endif
                break;
            }
#ifdef XFS_BENCH
            acc_io += ker::mod::tsc::get_ns() - t0;
#endif

            total_written += chunk;
        }
    }

write_done:
    if (total_written == 0) {
        return -EIO;
    }

    // Update file size if we wrote past the end
    if (offset + total_written > ip->size) {
        ip->size = offset + total_written;
        ip->dirty = true;
    }

    if (ip->dirty) {
#ifdef XFS_BENCH
        t0 = ker::mod::tsc::get_ns();
#endif
        XfsTransaction* tp = xfs_trans_alloc(ctx);
        if (tp != nullptr) {
            xfs_trans_log_inode(tp, ip);
            xfs_trans_commit(tp);
        }
#ifdef XFS_BENCH
        acc_ilog = ker::mod::tsc::get_ns() - t0;
#endif
    }

#ifdef XFS_BENCH
    s_wr_ns_bmap.fetch_add(acc_bmap, std::memory_order_relaxed);
    s_wr_ns_alloc.fetch_add(acc_alloc, std::memory_order_relaxed);
    s_wr_ns_io.fetch_add(acc_io, std::memory_order_relaxed);
    s_wr_ns_ilog.fetch_add(acc_ilog, std::memory_order_relaxed);
    s_wr_bytes.fetch_add(total_written, std::memory_order_relaxed);
    s_wr_hole_calls.fetch_add(loc_hole, std::memory_order_relaxed);
    s_wr_map_calls.fetch_add(loc_map, std::memory_order_relaxed);
    uint64_t n = s_wr_calls.fetch_add(1, std::memory_order_relaxed);
    if ((n & 63) == 63) {
        uint64_t nb = s_wr_ns_bmap.exchange(0, std::memory_order_relaxed);
        uint64_t na = s_wr_ns_alloc.exchange(0, std::memory_order_relaxed);
        uint64_t ni = s_wr_ns_io.exchange(0, std::memory_order_relaxed);
        uint64_t nil = s_wr_ns_ilog.exchange(0, std::memory_order_relaxed);
        uint64_t by = s_wr_bytes.exchange(0, std::memory_order_relaxed);
        uint64_t nhol = s_wr_hole_calls.exchange(0, std::memory_order_relaxed);
        uint64_t nmap = s_wr_map_calls.exchange(0, std::memory_order_relaxed);
        uint64_t mbps = (ni > 0) ? (by * 1000ULL / ni) : 0;
        ker::mod::dbg::log(
            "[XFS write bench] call#%lu: bmap=%luus alloc=%luus io=%luus ilog=%luus "
            "bytes=%lu io_MB/s~%lu hole_iters=%lu map_iters=%lu\n",
            (unsigned long)n, (unsigned long)(nb / 1000ULL), (unsigned long)(na / 1000ULL), (unsigned long)(ni / 1000ULL),
            (unsigned long)(nil / 1000ULL), (unsigned long)by, (unsigned long)mbps, (unsigned long)nhol, (unsigned long)nmap);
    }
#endif

    return static_cast<ssize_t>(total_written);
}

auto xfs_vfs_lseek(File* f, off_t offset, int whence) -> off_t {
    if (f == nullptr) {
        return -EBADF;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }

    off_t new_pos = 0;
    switch (whence) {
        case 0:  // SEEK_SET
            new_pos = offset;
            break;
        case 1:  // SEEK_CUR
            new_pos = f->pos + offset;
            break;
        case 2:  // SEEK_END
            new_pos = static_cast<off_t>(xfd->inode->size) + offset;
            break;
        default:
            return -EINVAL;
    }

    if (new_pos < 0) {
        return -EINVAL;
    }
    f->pos = new_pos;
    return new_pos;
}

auto xfs_vfs_isatty(File* /*f*/) -> bool { return false; }

// ============================================================================
// Readdir - uses xfs_dir_iterate
// ============================================================================

struct ReaddirCtx {
    DirEntry* entry;       // output entry to fill
    size_t target_index;   // which entry we want
    size_t current_index;  // current iteration counter
    bool found;
};

auto readdir_callback(const XfsDirEntry* xde, void* ctx_ptr) -> int {
    auto* rctx = static_cast<ReaddirCtx*>(ctx_ptr);
    if (rctx->current_index == rctx->target_index) {
        // Fill the VFS DirEntry
        rctx->entry->d_ino = xde->ino;
        rctx->entry->d_off = static_cast<uint64_t>(rctx->target_index + 1);
        rctx->entry->d_reclen = sizeof(DirEntry);

        // Convert XFS ftype to VFS d_type
        switch (xde->ftype) {
            case XFS_DIR3_FT_REG_FILE:
                rctx->entry->d_type = DT_REG;
                break;
            case XFS_DIR3_FT_DIR:
                rctx->entry->d_type = DT_DIR;
                break;
            case XFS_DIR3_FT_CHRDEV:
                rctx->entry->d_type = DT_CHR;
                break;
            case XFS_DIR3_FT_BLKDEV:
                rctx->entry->d_type = DT_BLK;
                break;
            case XFS_DIR3_FT_FIFO:
                rctx->entry->d_type = DT_FIFO;
                break;
            case XFS_DIR3_FT_SOCK:
                rctx->entry->d_type = DT_SOCK;
                break;
            case XFS_DIR3_FT_SYMLINK:
                rctx->entry->d_type = DT_LNK;
                break;
            default:
                rctx->entry->d_type = DT_UNKNOWN;
                break;
        }

        // Copy name
        size_t copy_len = xde->namelen < DIRENT_NAME_MAX - 1 ? xde->namelen : DIRENT_NAME_MAX - 1;
        std::memcpy(rctx->entry->d_name.data(), xde->name.data(), copy_len);
        rctx->entry->d_name[copy_len] = '\0';

        rctx->found = true;
        return 1;  // stop iteration
    }
    rctx->current_index++;
    return 0;  // continue
}

auto xfs_vfs_readdir(File* f, DirEntry* entry, size_t index) -> int {
    if (f == nullptr || entry == nullptr) {
        return -EINVAL;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }

    if (!xfs_inode_isdir(xfd->inode)) {
        return -ENOTDIR;
    }

    ReaddirCtx ctx{};
    ctx.entry = entry;
    ctx.target_index = index;
    ctx.current_index = 0;
    ctx.found = false;

    int ret = xfs_dir_iterate(xfd->inode, readdir_callback, &ctx);
    if (ret < 0) {
        return ret;
    }

    return ctx.found ? 0 : -1;  // -1 = no more entries
}

// ============================================================================
// Readlink
// ============================================================================

auto xfs_vfs_readlink(File* f, char* buf, size_t bufsize) -> ssize_t {
    if (f == nullptr || buf == nullptr) {
        return -EINVAL;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }

    if (!xfs_inode_islnk(xfd->inode)) {
        return -EINVAL;
    }

    int ret = xfs_readlink(xfd->inode, buf, bufsize);
    if (ret < 0) {
        return ret;
    }
    return static_cast<ssize_t>(ret);
}

// ============================================================================
// Truncate
// ============================================================================

auto xfs_vfs_truncate(File* f, off_t length) -> int {
    if (f == nullptr) {
        return -EBADF;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }

    XfsInode* ip = xfd->inode;
    XfsMountContext* ctx = xfd->mount;

    if (ctx->read_only) {
        return -EROFS;
    }

    if (length < 0) {
        return -EINVAL;
    }

    // For now, only support truncating to 0 (common case) or extending.
    // Shrinking to an arbitrary size would require freeing blocks.
    auto new_size = static_cast<uint64_t>(length);

    if (new_size == ip->size) {
        return 0;  // no change
    }

    // Update the inode size
    ip->size = new_size;
    ip->dirty = true;

    XfsTransaction* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) {
        return -ENOMEM;
    }
    xfs_trans_log_inode(tp, ip);
    int ret = xfs_trans_commit(tp);
    return (ret == 0) ? 0 : -EIO;
}

// ============================================================================
// FileOperations vtable
// ============================================================================

FileOperations xfs_fops = {
    .vfs_open = nullptr,  // open is handled by xfs_open_path, not through fops
    .vfs_close = xfs_vfs_close,
    .vfs_read = xfs_vfs_read,
    .vfs_write = xfs_vfs_write,
    .vfs_lseek = xfs_vfs_lseek,
    .vfs_isatty = xfs_vfs_isatty,
    .vfs_readdir = xfs_vfs_readdir,
    .vfs_readlink = xfs_vfs_readlink,
    .vfs_truncate = xfs_vfs_truncate,
    .vfs_poll_check = nullptr,
};

}  // anonymous namespace

auto get_xfs_fops() -> FileOperations* { return &xfs_fops; }

// ============================================================================
// Open path
// ============================================================================

auto xfs_open_path(const char* fs_path, int flags, int mode, XfsMountContext* ctx) -> File* {
    constexpr int O_CREAT_FLAG = 0100;
    constexpr int O_TRUNC_FLAG = 01000;

    if (ctx == nullptr) {
        return nullptr;
    }

    auto* ip = walk_path(ctx, fs_path);

    if (ip == nullptr && (flags & O_CREAT_FLAG) != 0 && !ctx->read_only) {
        // File doesn't exist and O_CREAT is set - create it.
        // Find the parent directory and the filename component.
        const char* last_slash = nullptr;
        for (const char* p = fs_path; *p != '\0'; p++) {
            if (*p == '/') {
                last_slash = p;
            }
        }

        XfsInode* parent_ip = nullptr;
        const char* filename = nullptr;
        uint16_t filename_len = 0;

        if (last_slash == nullptr || last_slash == fs_path) {
            // File is in the root directory
            parent_ip = xfs_inode_read(ctx, ctx->root_ino);
            filename = (last_slash == fs_path) ? fs_path + 1 : fs_path;
        } else {
            // Extract parent path
            size_t parent_len = static_cast<size_t>(last_slash - fs_path);
            char parent_path[512] = {};  // NOLINT
            if (parent_len >= sizeof(parent_path)) {
                return nullptr;
            }
            std::memcpy(static_cast<char*>(parent_path), fs_path, parent_len);
            parent_path[parent_len] = '\0';
            parent_ip = walk_path(ctx, static_cast<const char*>(parent_path));
            filename = last_slash + 1;
        }

        if (parent_ip == nullptr || !xfs_inode_isdir(parent_ip)) {
            if (parent_ip != nullptr) {
                xfs_inode_release(parent_ip);
            }
            return nullptr;
        }

        filename_len = 0;
        for (const char* p = filename; *p != '\0'; p++) {
            filename_len++;
        }
        if (filename_len == 0) {
            xfs_inode_release(parent_ip);
            return nullptr;
        }

        // Allocate a new inode
        XfsTransaction* tp = xfs_trans_alloc(ctx);
        if (tp == nullptr) {
            xfs_inode_release(parent_ip);
            return nullptr;
        }

        // Apply default mode if not specified (standard POSIX: 0666 for files)
        int file_mode = (mode == 0) ? 0666 : mode;
        uint16_t inode_mode = static_cast<uint16_t>(file_mode & 0xFFF) | 0100000;  // S_IFREG | mode
        xfs_ino_t new_ino = xfs_ialloc(ctx, tp, inode_mode);
        if (new_ino == NULLFSINO) {
            xfs_trans_cancel(tp);
            xfs_inode_release(parent_ip);
            return nullptr;
        }

        // Add directory entry
        int rc = xfs_dir_addname(parent_ip, filename, filename_len, new_ino, XFS_DIR3_FT_REG_FILE, tp);
        if (rc != 0) {
            xfs_trans_cancel(tp);
            xfs_inode_release(parent_ip);
            return nullptr;
        }

        rc = xfs_trans_commit(tp);
        xfs_inode_release(parent_ip);
        if (rc != 0) {
            return nullptr;
        }

        // Now read the newly created inode
        ip = xfs_inode_read(ctx, new_ino);
        if (ip == nullptr) {
            return nullptr;
        }

        // Initialize the new inode's in-memory state
        ip->mode = inode_mode;
        ip->size = 0;
        ip->nlink = 1;
        ip->nblocks = 0;
        ip->data_fork.format = XFS_DINODE_FMT_EXTENTS;
        ip->data_fork.extents.list = nullptr;
        ip->data_fork.extents.count = 0;
        ip->nextents = 0;
        ip->dirty = true;

        // Commit the new inode's core fields to disk so they persist.
        XfsTransaction* tp2 = xfs_trans_alloc(ctx);
        if (tp2 != nullptr) {
            xfs_trans_log_inode(tp2, ip);
            xfs_trans_commit(tp2);
        }
    }

    if (ip == nullptr) {
        return nullptr;
    }

    // Check for O_WRONLY/O_RDWR on a read-only mount
    int accmode = flags & 3;
    if (accmode == 1 || accmode == 2) {
        // Write access on read-only filesystem
        if (ctx->read_only) {
            xfs_inode_release(ip);
            return nullptr;
        }
    }

    // Handle O_TRUNC on regular files
    if ((flags & O_TRUNC_FLAG) != 0 && xfs_inode_isreg(ip) && !ctx->read_only) {
#ifdef XFS_DEBUG
        mod::dbg::log("[xfs] O_TRUNC: ino=%lu old_size=%lu -> 0", (unsigned long)ip->ino, (unsigned long)ip->size);
#endif
        ip->size = 0;
        ip->dirty = true;
        XfsTransaction* tp = xfs_trans_alloc(ctx);
        if (tp != nullptr) {
            xfs_trans_log_inode(tp, ip);
            int trc = xfs_trans_commit(tp);
#ifdef XFS_DEBUG
            mod::dbg::log("[xfs] O_TRUNC: commit rc=%d", trc);
#endif
            if (trc != 0) {
                xfs_inode_release(ip);
                return nullptr;
            }
        }
    }

    // Allocate File handle
    auto* f = new (std::nothrow) File{};
    if (f == nullptr) {
        xfs_inode_release(ip);
        return nullptr;
    }

    auto* xfd = new XfsFileData;
    xfd->mount = ctx;
    xfd->inode = ip;  // transfers reference to the file handle

    f->private_data = xfd;
    f->fops = &xfs_fops;
    f->pos = 0;
    f->is_directory = xfs_inode_isdir(ip);
    f->fs_type = FSType::XFS;
    f->refcount = 1;
    f->open_flags = flags;
    f->fd_flags = 0;
    f->vfs_path = nullptr;
    f->dir_fs_count = static_cast<size_t>(-1);

    return f;
}

// ============================================================================
// Stat helpers
// ============================================================================

namespace {

void fill_stat(XfsInode* ip, ker::vfs::stat* st) {
    st->st_dev = 0;
    st->st_ino = ip->ino;
    st->st_nlink = ip->nlink;
    st->st_mode = ip->mode;
    st->st_uid = ip->uid;
    st->st_gid = ip->gid;
    st->__pad0 = 0;
    st->st_rdev = 0;
    st->st_size = static_cast<off_t>(ip->size);
    st->st_blksize = static_cast<blksize_t>(ip->mount->block_size);
    st->st_blocks = static_cast<blkcnt_t>(ip->nblocks * (ip->mount->block_size / 512));

    // Timestamps: bigtime uses nanoseconds since Dec 13, 1901 packed into
    // a uint64; legacy uses upper-32 = seconds, lower-32 = nanoseconds.
    bool bigtime = (ip->flags2 & XFS_DIFLAG2_BIGTIME) != 0;
    if (bigtime) {
        // XFS bigtime epoch: nanoseconds offset from the legacy min timestamp.
        // Linux: XFS_BIGTIME_EPOCH_OFFSET = -(int64_t)S32_MIN = 2^31 = 2147483648
        constexpr int64_t XFS_BIGTIME_EPOCH_OFFSET = (1LL << 31);
        constexpr uint64_t NSEC_PER_SEC = 1000000000ULL;
        auto decode = [&](uint64_t raw, struct timespec& ts) {
            ts.tv_sec = static_cast<int64_t>(raw / NSEC_PER_SEC) - XFS_BIGTIME_EPOCH_OFFSET;
            ts.tv_nsec = static_cast<int64_t>(raw % NSEC_PER_SEC);
        };
        decode(ip->atime, st->st_atim);
        decode(ip->mtime, st->st_mtim);
        decode(ip->ctime, st->st_ctim);
    } else {
        st->st_atim.tv_sec = static_cast<int64_t>(ip->atime >> 32);
        st->st_atim.tv_nsec = static_cast<int64_t>(ip->atime & 0xFFFFFFFF);
        st->st_mtim.tv_sec = static_cast<int64_t>(ip->mtime >> 32);
        st->st_mtim.tv_nsec = static_cast<int64_t>(ip->mtime & 0xFFFFFFFF);
        st->st_ctim.tv_sec = static_cast<int64_t>(ip->ctime >> 32);
        st->st_ctim.tv_nsec = static_cast<int64_t>(ip->ctime & 0xFFFFFFFF);
    }
}

}  // anonymous namespace

auto xfs_stat(const char* fs_path, ker::vfs::stat* statbuf, XfsMountContext* ctx) -> int {
    if (statbuf == nullptr || ctx == nullptr) {
        return -EINVAL;
    }

    auto* ip = walk_path(ctx, fs_path);
    if (ip == nullptr) {
        return -ENOENT;
    }

    std::memset(statbuf, 0, sizeof(ker::vfs::stat));
    fill_stat(ip, statbuf);
    xfs_inode_release(ip);
    return 0;
}

auto xfs_fstat(File* f, ker::vfs::stat* statbuf) -> int {
    if (f == nullptr || statbuf == nullptr) {
        return -EINVAL;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr) {
        return -EBADF;
    }

    std::memset(statbuf, 0, sizeof(ker::vfs::stat));
    fill_stat(xfd->inode, statbuf);
    return 0;
}

auto xfs_statvfs(XfsMountContext* ctx, ker::vfs::statvfs* buf) -> int {
    if (ctx == nullptr || buf == nullptr) {
        return -EINVAL;
    }

    std::memset(buf, 0, sizeof(ker::vfs::statvfs));

    uint64_t free_blocks = 0;
    uint64_t total_inodes = 0;
    uint64_t free_inodes = 0;
    for (xfs_agnumber_t i = 0; i < ctx->ag_count; i++) {
        free_blocks += ctx->per_ag[i].agf_freeblks;
        total_inodes += ctx->per_ag[i].agi_count;
        free_inodes += ctx->per_ag[i].agi_freecount;
    }

    // Derive a 64-bit fsid by XOR-folding the 128-bit UUID
    uint64_t fsid_lo = 0;
    uint64_t fsid_hi = 0;
    for (int i = 0; i < 8; i++) {
        fsid_lo |= static_cast<uint64_t>(ctx->uuid.b[i]) << (i * 8);
        fsid_hi |= static_cast<uint64_t>(ctx->uuid.b[i + 8]) << (i * 8);
    }

    buf->f_bsize = ctx->block_size;
    buf->f_frsize = ctx->block_size;
    buf->f_blocks = ctx->total_blocks;
    buf->f_bfree = free_blocks;
    buf->f_bavail = free_blocks;
    buf->f_files = total_inodes;
    buf->f_ffree = free_inodes;
    buf->f_favail = free_inodes;
    buf->f_fsid = fsid_lo ^ fsid_hi;
    buf->f_flag = ctx->read_only ? ker::vfs::ST_RDONLY : 0;
    buf->f_namemax = 255;
    return 0;
}

// ============================================================================
// chmod
// ============================================================================

auto xfs_chmod_path(const char* fs_path, int mode, XfsMountContext* ctx) -> int {
    if (fs_path == nullptr || ctx == nullptr) {
        return -EINVAL;
    }
    if (ctx->read_only) {
        return -EROFS;
    }

    auto* ip = walk_path(ctx, fs_path);
    if (ip == nullptr) {
        return -ENOENT;
    }

    static constexpr uint16_t XFS_IFMT = 0xF000;
    static constexpr uint16_t XFS_PERM_MASK = 07777;
    ip->mode = (ip->mode & XFS_IFMT) | (static_cast<uint16_t>(mode) & XFS_PERM_MASK);
    ip->dirty = true;

    auto* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) {
        xfs_inode_release(ip);
        return -ENOMEM;
    }
    xfs_trans_log_inode(tp, ip);
    int ret = xfs_trans_commit(tp);
    xfs_inode_release(ip);
    return (ret == 0) ? 0 : -EIO;
}

auto xfs_fchmod(File* f, int mode) -> int {
    if (f == nullptr) {
        return -EBADF;
    }
    auto* xfd = static_cast<XfsFileData*>(f->private_data);
    if (xfd == nullptr || xfd->inode == nullptr || xfd->mount == nullptr) {
        return -EBADF;
    }
    if (xfd->mount->read_only) {
        return -EROFS;
    }

    static constexpr uint16_t XFS_IFMT = 0xF000;
    static constexpr uint16_t XFS_PERM_MASK = 07777;
    auto* ip = xfd->inode;
    ip->mode = (ip->mode & XFS_IFMT) | (static_cast<uint16_t>(mode) & XFS_PERM_MASK);
    ip->dirty = true;

    auto* tp = xfs_trans_alloc(xfd->mount);
    if (tp == nullptr) {
        return -ENOMEM;
    }
    xfs_trans_log_inode(tp, ip);
    int ret = xfs_trans_commit(tp);
    return (ret == 0) ? 0 : -EIO;
}

// ============================================================================
// Mkdir
// ============================================================================

auto xfs_mkdir_path(const char* fs_path, int mode, XfsMountContext* ctx) -> int {
    if (fs_path == nullptr || ctx == nullptr) {
        return -EINVAL;
    }
    if (ctx->read_only) {
        return -EROFS;
    }

    // Already exists?
    XfsInode* existing = walk_path(ctx, fs_path);
    if (existing != nullptr) {
        bool is_dir = xfs_inode_isdir(existing);
        xfs_inode_release(existing);
        return is_dir ? -EEXIST : -ENOTDIR;
    }

    // Find parent directory and new name
    const char* last_slash = nullptr;
    for (const char* p = fs_path; *p != '\0'; p++) {
        if (*p == '/') {
            last_slash = p;
        }
    }

    XfsInode* parent_ip = nullptr;
    const char* dirname = nullptr;
    uint16_t dirname_len = 0;

    if (last_slash == nullptr || last_slash == fs_path) {
        parent_ip = xfs_inode_read(ctx, ctx->root_ino);
        dirname = (last_slash == fs_path) ? fs_path + 1 : fs_path;
    } else {
        auto parent_len = static_cast<size_t>(last_slash - fs_path);
        char parent_path[512] = {};  // NOLINT
        if (parent_len >= sizeof(parent_path)) {
            return -ENAMETOOLONG;
        }
        std::memcpy(static_cast<char*>(parent_path), fs_path, parent_len);
        parent_path[parent_len] = '\0';
        parent_ip = walk_path(ctx, static_cast<const char*>(parent_path));
        dirname = last_slash + 1;
    }

    if (parent_ip == nullptr || !xfs_inode_isdir(parent_ip)) {
        if (parent_ip != nullptr) {
            xfs_inode_release(parent_ip);
        }
        return -ENOENT;
    }

    for (const char* p = dirname; *p != '\0'; p++) {
        dirname_len++;
    }
    if (dirname_len == 0) {
        xfs_inode_release(parent_ip);
        return -EINVAL;
    }

    // Save before parent_ip is released
    xfs_ino_t parent_ino = parent_ip->ino;

    // Allocate new inode
    XfsTransaction* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) {
        xfs_inode_release(parent_ip);
        return -ENOMEM;
    }

    int dir_mode = (mode == 0) ? 0755 : mode;
    uint16_t inode_mode = static_cast<uint16_t>(dir_mode & 0xFFF) | 0040000;  // S_IFDIR
    xfs_ino_t new_ino = xfs_ialloc(ctx, tp, inode_mode);
    if (new_ino == NULLFSINO) {
        xfs_trans_cancel(tp);
        xfs_inode_release(parent_ip);
        return -ENOSPC;
    }

    // Add entry in parent
    int rc = xfs_dir_addname(parent_ip, dirname, dirname_len, new_ino, XFS_DIR3_FT_DIR, tp);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        xfs_inode_release(parent_ip);
        return rc;
    }

    rc = xfs_trans_commit(tp);
    xfs_inode_release(parent_ip);
    if (rc != 0) {
        return rc;
    }

    // Initialize the new directory inode with a minimal shortform header
    XfsInode* new_ip = xfs_inode_read(ctx, new_ino);
    if (new_ip == nullptr) {
        return -EIO;
    }

    new_ip->mode = inode_mode;
    new_ip->size = 0;
    new_ip->nlink = 2;  // . and parent ref
    new_ip->nblocks = 0;
    new_ip->nextents = 0;

    // Build minimal shortform directory: count=0, parent=parent_ino
    bool use_i8 = (parent_ino > 0xFFFFFFFFULL);
    size_t ino_bytes = use_i8 ? 8 : 4;
    size_t sf_size = 2 + ino_bytes;
    auto* sf_data = new (std::nothrow) uint8_t[sf_size];
    if (sf_data == nullptr) {
        xfs_inode_release(new_ip);
        return -ENOMEM;
    }
    sf_data[0] = 0;               // count
    sf_data[1] = use_i8 ? 1 : 0;  // i8count
    if (use_i8) {
        for (int i = 7; i >= 0; i--) {
            sf_data[2 + (7 - i)] = static_cast<uint8_t>((parent_ino >> (i * 8)) & 0xFF);
        }
    } else {
        auto p32 = static_cast<uint32_t>(parent_ino);
        for (int i = 3; i >= 0; i--) {
            sf_data[2 + (3 - i)] = static_cast<uint8_t>((p32 >> (i * 8)) & 0xFF);
        }
    }

    new_ip->data_fork.format = XFS_DINODE_FMT_LOCAL;
    new_ip->data_fork.local.data = sf_data;
    new_ip->data_fork.local.size = static_cast<uint32_t>(sf_size);
    new_ip->dirty = true;

    XfsTransaction* tp2 = xfs_trans_alloc(ctx);
    if (tp2 != nullptr) {
        xfs_trans_log_inode(tp2, new_ip);
        xfs_trans_commit(tp2);
    }
    xfs_inode_release(new_ip);
    return 0;
}

// ============================================================================
// Rmdir
// ============================================================================

namespace {

auto xfs_find_parent_and_name(const char* fs_path, XfsMountContext* ctx, XfsInode** parent_out, const char** name_out,
                              uint16_t* namelen_out) -> int {
    const char* last_slash = nullptr;
    for (const char* p = fs_path; *p != '\0'; p++) {
        if (*p == '/') {
            last_slash = p;
        }
    }

    XfsInode* parent_ip = nullptr;
    const char* name = nullptr;

    if (last_slash == nullptr || last_slash == fs_path) {
        parent_ip = xfs_inode_read(ctx, ctx->root_ino);
        name = (last_slash == fs_path) ? fs_path + 1 : fs_path;
    } else {
        auto parent_len = static_cast<size_t>(last_slash - fs_path);
        char parent_path[512] = {};  // NOLINT
        if (parent_len >= sizeof(parent_path)) {
            return -ENAMETOOLONG;
        }
        std::memcpy(static_cast<char*>(parent_path), fs_path, parent_len);
        parent_path[parent_len] = '\0';
        parent_ip = walk_path(ctx, static_cast<const char*>(parent_path));
        name = last_slash + 1;
    }

    if (parent_ip == nullptr || !xfs_inode_isdir(parent_ip)) {
        if (parent_ip != nullptr) {
            xfs_inode_release(parent_ip);
        }
        return -ENOENT;
    }

    uint16_t namelen = 0;
    for (const char* p = name; *p != '\0'; p++) {
        namelen++;
    }
    if (namelen == 0) {
        xfs_inode_release(parent_ip);
        return -EINVAL;
    }

    *parent_out = parent_ip;
    *name_out = name;
    *namelen_out = namelen;
    return 0;
}

auto count_real_entries(const XfsDirEntry* entry, void* ctx) -> int {
    auto* count = static_cast<int*>(ctx);
    if (entry->name[0] == '.' && (entry->namelen == 1 || (entry->namelen == 2 && entry->name[1] == '.'))) {
        return 0;
    }
    (*count)++;
    return 0;
}

}  // anonymous namespace

auto xfs_rmdir_path(const char* fs_path, XfsMountContext* ctx) -> int {
    if (fs_path == nullptr || ctx == nullptr) {
        return -EINVAL;
    }
    if (ctx->read_only) {
        return -EROFS;
    }
    if (fs_path[0] == '\0' || (fs_path[0] == '/' && fs_path[1] == '\0')) {
        return -EBUSY;
    }

    XfsInode* parent_ip = nullptr;
    const char* name = nullptr;
    uint16_t namelen = 0;
    int rc = xfs_find_parent_and_name(fs_path, ctx, &parent_ip, &name, &namelen);
    if (rc != 0) {
        return rc;
    }

    XfsDirEntry de{};
    rc = xfs_dir_lookup(parent_ip, name, namelen, &de);
    if (rc != 0) {
        xfs_inode_release(parent_ip);
        return rc;
    }
    if (de.ftype != XFS_DIR3_FT_DIR) {
        xfs_inode_release(parent_ip);
        return -ENOTDIR;
    }

    XfsInode* dir_ip = xfs_inode_read(ctx, de.ino);
    if (dir_ip == nullptr) {
        xfs_inode_release(parent_ip);
        return -ENOENT;
    }

    int entry_count = 0;
    xfs_dir_iterate(dir_ip, count_real_entries, &entry_count);
    if (entry_count > 0) {
        xfs_inode_release(dir_ip);
        xfs_inode_release(parent_ip);
        return -ENOTEMPTY;
    }

    XfsTransaction* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) {
        xfs_inode_release(dir_ip);
        xfs_inode_release(parent_ip);
        return -ENOMEM;
    }

    rc = xfs_dir_removename(parent_ip, name, namelen, tp);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        xfs_inode_release(dir_ip);
        xfs_inode_release(parent_ip);
        return rc;
    }

    parent_ip->dirty = true;
    xfs_trans_log_inode(tp, parent_ip);
    // The removed directory loses both the parent entry and its self-reference.
    dir_ip->nlink = 0;
    dir_ip->dirty = true;
    xfs_trans_log_inode(tp, dir_ip);

    rc = xfs_trans_commit(tp);
    xfs_inode_release(dir_ip);
    xfs_inode_release(parent_ip);
    return (rc == 0) ? 0 : -EIO;
}

// ============================================================================
// Rename
// ============================================================================

auto xfs_rename_path(const char* old_fs_path, const char* new_fs_path, XfsMountContext* ctx) -> int {
    if (old_fs_path == nullptr || new_fs_path == nullptr || ctx == nullptr) {
        return -EINVAL;
    }
    if (ctx->read_only) {
        return -EROFS;
    }

    XfsInode* old_parent = nullptr;
    const char* old_name = nullptr;
    uint16_t old_namelen = 0;
    int rc = xfs_find_parent_and_name(old_fs_path, ctx, &old_parent, &old_name, &old_namelen);
    if (rc != 0) {
        return rc;
    }

    XfsDirEntry old_de{};
    rc = xfs_dir_lookup(old_parent, old_name, old_namelen, &old_de);
    if (rc != 0) {
        xfs_inode_release(old_parent);
        return rc;
    }

    XfsInode* new_parent = nullptr;
    const char* new_name = nullptr;
    uint16_t new_namelen = 0;
    rc = xfs_find_parent_and_name(new_fs_path, ctx, &new_parent, &new_name, &new_namelen);
    if (rc != 0) {
        xfs_inode_release(old_parent);
        return rc;
    }

    XfsTransaction* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) {
        xfs_inode_release(new_parent);
        xfs_inode_release(old_parent);
        return -ENOMEM;
    }

    // If destination exists, remove it first. Keep the displaced inode
    // referenced until the transaction no longer points at it.
    XfsDirEntry new_de{};
    XfsInode* displaced = nullptr;
    if (xfs_dir_lookup(new_parent, new_name, new_namelen, &new_de) == 0) {
        rc = xfs_dir_removename(new_parent, new_name, new_namelen, tp);
        if (rc != 0) {
            xfs_trans_cancel(tp);
            xfs_inode_release(new_parent);
            xfs_inode_release(old_parent);
            return rc;
        }
        // Decrement nlink on the displaced inode
        displaced = xfs_inode_read(ctx, new_de.ino);
        if (displaced != nullptr) {
            if (displaced->nlink > 0) {
                displaced->nlink--;
            }
            displaced->dirty = true;
            xfs_trans_log_inode(tp, displaced);
        }
    }

    // Add entry at new location
    rc = xfs_dir_addname(new_parent, new_name, new_namelen, old_de.ino, old_de.ftype, tp);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        if (displaced != nullptr) {
            xfs_inode_release(displaced);
        }
        xfs_inode_release(new_parent);
        xfs_inode_release(old_parent);
        return rc;
    }

    // Remove old entry
    rc = xfs_dir_removename(old_parent, old_name, old_namelen, tp);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        if (displaced != nullptr) {
            xfs_inode_release(displaced);
        }
        xfs_inode_release(new_parent);
        xfs_inode_release(old_parent);
        return rc;
    }

    old_parent->dirty = true;
    new_parent->dirty = true;
    xfs_trans_log_inode(tp, old_parent);
    xfs_trans_log_inode(tp, new_parent);

    rc = xfs_trans_commit(tp);
    if (displaced != nullptr) {
        xfs_inode_release(displaced);
    }
    xfs_inode_release(new_parent);
    xfs_inode_release(old_parent);
    return (rc == 0) ? 0 : -EIO;
}

// ============================================================================
// Unlink / remove
// ============================================================================

auto xfs_unlink_path(const char* fs_path, XfsMountContext* ctx) -> int {
    if (fs_path == nullptr || ctx == nullptr) {
        return -EINVAL;
    }

    // Cannot unlink root
    if (fs_path[0] == '\0' || (fs_path[0] == '/' && fs_path[1] == '\0')) {
        return -EBUSY;
    }

    if (ctx->read_only) {
        return -EROFS;
    }

    // Find the parent directory and extract the filename
    const char* last_slash = nullptr;
    for (const char* p = fs_path; *p != '\0'; p++) {
        if (*p == '/') {
            last_slash = p;
        }
    }

    XfsInode* parent_ip = nullptr;
    const char* filename = nullptr;
    uint16_t filename_len = 0;

    if (last_slash == nullptr || last_slash == fs_path) {
        // File is in the root directory
        parent_ip = xfs_inode_read(ctx, ctx->root_ino);
        filename = (last_slash == fs_path) ? fs_path + 1 : fs_path;
    } else {
        // Extract parent path
        size_t parent_len = static_cast<size_t>(last_slash - fs_path);
        char parent_path[512] = {};  // NOLINT
        if (parent_len >= sizeof(parent_path)) {
            return -ENAMETOOLONG;
        }
        std::memcpy(static_cast<char*>(parent_path), fs_path, parent_len);
        parent_path[parent_len] = '\0';
        parent_ip = walk_path(ctx, static_cast<const char*>(parent_path));
        filename = last_slash + 1;
    }

    if (parent_ip == nullptr) {
        return -ENOENT;
    }

    if (!xfs_inode_isdir(parent_ip)) {
        xfs_inode_release(parent_ip);
        return -ENOTDIR;
    }

    // Calculate filename length
    for (const char* p = filename; *p != '\0'; p++) {
        filename_len++;
    }
    if (filename_len == 0) {
        xfs_inode_release(parent_ip);
        return -EINVAL;
    }

    // Look up the entry to be deleted (must verify it exists and is a regular file)
    XfsDirEntry de{};
    int rc = xfs_dir_lookup(parent_ip, filename, filename_len, &de);
    if (rc != 0) {
        xfs_inode_release(parent_ip);
        return rc;
    }

    // Cannot unlink a directory with unlink (should use rmdir)
    if (de.ftype == XFS_DIR3_FT_DIR) {
        xfs_inode_release(parent_ip);
        return -EISDIR;
    }

    // Create transaction for removal
    XfsTransaction* tp = xfs_trans_alloc(ctx);
    if (tp == nullptr) {
        xfs_inode_release(parent_ip);
        return -ENOMEM;
    }

    // Remove from parent directory
    rc = xfs_dir_removename(parent_ip, filename, filename_len, tp);
    if (rc != 0) {
        xfs_trans_cancel(tp);
        xfs_inode_release(parent_ip);
        return rc;
    }

    parent_ip->dirty = true;
    xfs_trans_log_inode(tp, parent_ip);

    // Read the target inode and decrement its nlink. Keep it referenced until
    // after transaction commit because xfs_trans_log_inode stores a raw pointer.
    XfsInode* target_ip = xfs_inode_read(ctx, de.ino);
    if (target_ip != nullptr) {
        if (target_ip->nlink > 0) {
            target_ip->nlink--;
        }
        target_ip->dirty = true;
        xfs_trans_log_inode(tp, target_ip);
    }

    rc = xfs_trans_commit(tp);

    if (target_ip != nullptr) {
        xfs_inode_release(target_ip);
    }
    xfs_inode_release(parent_ip);

    return (rc == 0) ? 0 : -EIO;
}

// ============================================================================
// Mount / init
// ============================================================================

auto xfs_vfs_init_device(dev::BlockDevice* device) -> XfsMountContext* {
    if (device == nullptr) {
        return nullptr;
    }

    XfsMountContext* ctx = nullptr;
    int ret = xfs_mount(device, false /* read_write */, &ctx);
    if (ret != 0) {
        log::error("mount failed with error %d", ret);
        return nullptr;
    }

    // Initialize the log
    ret = xfs_log_mount(ctx);
    if (ret != 0) {
        log::error("log mount failed");
        xfs_unmount(ctx);
        return nullptr;
    }

    if (xfs_log_needs_recovery(ctx)) {
        log::warn("journal is dirty, recovery not implemented");
    }

    log::info("mounted successfully (%s)", ctx->read_only ? "read-only" : "read-write");
    return ctx;
}

void register_xfs() {
    xfs_icache_init();
    log::info("filesystem driver registered");
}

}  // namespace ker::vfs::xfs
