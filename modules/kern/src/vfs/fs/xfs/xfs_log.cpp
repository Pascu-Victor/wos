// XFS Log implementation — journal recovery and basic log management.
//
// On mount, the log is scanned to find the head and tail.  If they differ,
// recovery is needed: log records are replayed to bring the filesystem back
// to a consistent state.
//
// The current implementation handles log head/tail detection and basic
// clean/dirty state.  Full transaction log writing is deferred to a
// dedicated kernel thread (not yet implemented).
//
// Reference: reference/xfs/xfs_log.c, reference/xfs/xfs_log_recover.c

#include "xfs_log.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <util/crc32c.hpp>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_trans.hpp>

namespace ker::vfs::xfs {

namespace {

// Scan the log to find head and tail positions.
// Returns 0 on success, fills head/tail_cycle/block fields in the log struct.
auto xfs_log_find_head_tail(XfsLog* log) -> int {
    XfsMountContext* ctx = log->mount;

    // Read the first log sector to check if the log has any valid records
    BufHead* bh = xfs_buf_read(ctx, log->log_start);
    if (bh == nullptr) {
        mod::dbg::log("[xfs log] failed to read log block 0\n");
        return -EIO;
    }

    const auto* hdr = reinterpret_cast<const XlogRecHeader*>(bh->data);

    if (hdr->h_magicno.to_cpu() != XLOG_HEADER_MAGIC_NUM) {
        // No valid log record at position 0 — log is clean (never used or
        // was cleanly unmounted with log cleared)
        log->head_cycle = 1;
        log->head_block = 0;
        log->tail_cycle = 1;
        log->tail_block = 0;
        log->clean = true;
        brelse(bh);
        return 0;
    }

    // Read head and tail from the first valid log record
    uint32_t head_cycle = hdr->h_cycle.to_cpu();
    uint64_t tail_lsn = hdr->h_tail_lsn.to_cpu();
    auto tail_cycle = static_cast<uint32_t>(tail_lsn >> 32);
    auto tail_block = static_cast<uint32_t>(tail_lsn);

    brelse(bh);

    // Scan forward to find the actual log head (last valid record)
    // For now, do a simplified scan: check if the last block has the same cycle
    uint32_t last_block = log->log_blocks - 1;
    BufHead* last_bh = xfs_buf_read(ctx, log->log_start + last_block);
    if (last_bh == nullptr) {
        mod::dbg::log("[xfs log] failed to read last log block\n");
        return -EIO;
    }

    // The first 4 bytes of each log block contain the cycle number
    __be32 last_cycle_be{};
    __builtin_memcpy(&last_cycle_be, last_bh->data, sizeof(__be32));
    uint32_t last_cycle = last_cycle_be.to_cpu();
    brelse(last_bh);

    if (last_cycle == head_cycle) {
        // Log wraps but head is somewhere; for simplicity, treat as needing
        // a more thorough scan.
        log->head_cycle = head_cycle;
        log->head_block = 0;
        log->tail_cycle = tail_cycle;
        log->tail_block = tail_block;
        log->clean = (head_cycle == tail_cycle && tail_block == 0);
    } else {
        // Head and last block have different cycles — log has wrapped
        log->head_cycle = last_cycle;
        log->head_block = 0;
        log->tail_cycle = tail_cycle;
        log->tail_block = tail_block;
        log->clean = false;
    }

    return 0;
}

// Global log state (one per mounted XFS filesystem at this time)
XfsLog* active_log = nullptr;

// Replay a single log record starting at the given block.
// Reads the header, then reads body blocks and replays buffer modifications.
// Returns the block number after this record (next record start), or -1 on error.
auto replay_log_record(XfsLog* log, uint32_t block) -> int {
    XfsMountContext* ctx = log->mount;
    uint32_t block_size = ctx->block_size;

    uint64_t disk_block = log->log_start + block;
    BufHead* hdr_bh = xfs_buf_read(ctx, disk_block);
    if (hdr_bh == nullptr) {
        mod::dbg::log("[xfs log recover] failed to read log block %u\n", block);
        return -1;
    }

    const auto* hdr = reinterpret_cast<const XlogRecHeader*>(hdr_bh->data);
    if (hdr->h_magicno.to_cpu() != XLOG_HEADER_MAGIC_NUM) {
        brelse(hdr_bh);
        return -1;
    }

    uint32_t body_size = hdr->h_len.to_cpu();
    uint32_t num_logops = hdr->h_num_logops.to_cpu();
    brelse(hdr_bh);

    if (body_size == 0 || num_logops == 0) {
        // Empty record — advance past header
        return static_cast<int>((block + 1) % log->log_blocks);
    }

    // Read body data
    uint32_t data_blocks = (body_size + block_size - 1) / block_size;
    auto* body_buf = static_cast<uint8_t*>(mod::mm::dyn::kmalloc::malloc(body_size));
    if (body_buf == nullptr) {
        return -1;
    }

    uint32_t cur_block = (block + 1) % log->log_blocks;
    uint32_t body_offset = 0;

    for (uint32_t b = 0; b < data_blocks; b++) {
        uint64_t data_disk_block = log->log_start + cur_block;
        BufHead* data_bh = xfs_buf_read(ctx, data_disk_block);
        if (data_bh == nullptr) {
            mod::mm::dyn::kmalloc::free(body_buf);
            return -1;
        }

        uint32_t chunk = body_size - body_offset;
        chunk = std::min(chunk, block_size);
        __builtin_memcpy(body_buf + body_offset, data_bh->data, chunk);
        brelse(data_bh);

        body_offset += chunk;
        cur_block = (cur_block + 1) % log->log_blocks;
    }

    // Replay buffer modifications from the body
    // Format per buffer item: block_no(8B) + offset(4B) + len(4B) + data(lenB)
    // Format per inode item: ino(8B) — we skip inode items during recovery
    uint32_t pos = 0;
    uint32_t replayed = 0;

    while (pos < body_size && replayed < num_logops) {
        // Peek ahead to determine item type
        // Buffer items: need at least 16B header (8+4+4)
        // Inode items: need at least 8B
        if (pos + 16 <= body_size) {
            // Try as buffer item
            uint64_t blk_no = 0;
            uint32_t off = 0;
            uint32_t len = 0;
            __builtin_memcpy(&blk_no, body_buf + pos, 8);
            __builtin_memcpy(&off, body_buf + pos + 8, 4);
            __builtin_memcpy(&len, body_buf + pos + 12, 4);

            // Sanity check: len should be reasonable
            if (len > 0 && len <= block_size * 4 && pos + 16 + len <= body_size) {
                // Replay this buffer modification
                BufHead* target_bh = bread(ctx->device, blk_no);
                if (target_bh != nullptr) {
                    if (off + len <= target_bh->size) {
                        __builtin_memcpy(target_bh->data + off, body_buf + pos + 16, len);
                        bdirty(target_bh);
                        int wrc = bwrite(target_bh);
                        if (wrc == 0) {
                            replayed++;
                        }
                    }
                }
                pos += 16 + len;
                continue;
            }
        }

        // Otherwise, treat as inode item (8 bytes) or skip
        if (pos + 8 <= body_size) {
            pos += 8;
            replayed++;
        } else {
            break;
        }
    }

    mod::mm::dyn::kmalloc::free(body_buf);

    mod::dbg::log("[xfs log recover] replayed %u/%u ops from log block %u\n", replayed, num_logops, block);

    return static_cast<int>(cur_block);
}

// Recover the log by scanning from tail to head and replaying all records.
auto xfs_log_recover(XfsLog* log) -> int {
    mod::dbg::log("[xfs log recover] starting recovery: tail=%u.%u head=%u.%u\n", log->tail_cycle, log->tail_block, log->head_cycle,
                  log->head_block);

    uint32_t cur_block = log->tail_block;
    uint32_t cur_cycle = log->tail_cycle;
    int records = 0;

    while (cur_cycle < log->head_cycle || (cur_cycle == log->head_cycle && cur_block < log->head_block)) {
        int next = replay_log_record(log, cur_block);
        if (next < 0) {
            mod::dbg::log("[xfs log recover] failed at block %u, stopping\n", cur_block);
            break;
        }
        records++;

        auto next_block = static_cast<uint32_t>(next);
        if (next_block < cur_block) {
            cur_cycle++;  // wrapped around
        }
        cur_block = next_block;

        // Safety: don't loop forever
        if (records > static_cast<int>(log->log_blocks)) {
            mod::dbg::log("[xfs log recover] too many records, aborting\n");
            break;
        }
    }

    mod::dbg::log("[xfs log recover] recovery complete: %d records replayed\n", records);

    // Mark log as clean
    log->clean = true;
    log->tail_block = log->head_block;
    log->tail_cycle = log->head_cycle;

    return 0;
}

}  // anonymous namespace

auto xfs_log_mount(XfsMountContext* mount) -> int {
    if (mount == nullptr) {
        return -EINVAL;
    }
    if (mount->log_blocks == 0) {
        mod::dbg::log("[xfs log] no log area configured\n");
        return -EINVAL;
    }

    auto* log = new XfsLog{};
    log->mount = mount;
    log->log_start = mount->log_start;
    log->log_blocks = mount->log_blocks;
    log->sect_size = mount->sect_size;
    log->clean = true;
    log->active = false;

    int rc = xfs_log_find_head_tail(log);
    if (rc != 0) {
        delete log;
        return rc;
    }

    if (!log->clean) {
        if (mount->read_only) {
            mod::dbg::log(
                "[xfs log] log is dirty but mount is read-only — "
                "recovery deferred\n");
        } else {
            mod::dbg::log("[xfs log] log is dirty — recovery needed\n");
            int rrc = xfs_log_recover(log);
            if (rrc != 0) {
                mod::dbg::log("[xfs log] recovery failed: %d\n", rrc);
                delete log;
                return rrc;
            }
        }
    } else {
        mod::dbg::log("[xfs log] log is clean\n");
    }

    log->active = true;
    active_log = log;
    return 0;
}

void xfs_log_unmount(XfsMountContext* mount) {
    if (active_log == nullptr || active_log->mount != mount) {
        return;
    }

    active_log->active = false;
    mod::dbg::log("[xfs log] log unmounted\n");

    delete active_log;
    active_log = nullptr;
}

auto xfs_log_needs_recovery(XfsMountContext* mount) -> bool {
    if (active_log == nullptr || active_log->mount != mount) {
        return false;
    }
    return !active_log->clean;
}

auto xfs_log_write(XfsMountContext* mount, const XfsTransItem* items, int item_count) -> int {
    if (mount == nullptr || active_log == nullptr || active_log->mount != mount) {
        return -EINVAL;
    }
    if (!active_log->active) {
        return -EINVAL;
    }

    XfsLog* log = active_log;

    // Compute total body size: for each buffer item, store block_no (8B) +
    // offset (4B) + len (4B) + data (len B).  Inode items store ino (8B).
    uint32_t body_size = 0;
    uint32_t num_logops = 0;
    for (int i = 0; i < item_count; i++) {
        if (items[i].type == XfsLogItemType::Buffer && items[i].buf.dirty) {
            body_size += 8 + 4 + 4 + items[i].buf.len;
            num_logops++;
        } else if (items[i].type == XfsLogItemType::Inode) {
            body_size += 8;
            num_logops++;
        }
    }

    if (num_logops == 0) {
        return 0;  // nothing to log
    }

    // Number of blocks needed: header (1) + data blocks
    uint32_t block_size = mount->block_size;
    uint32_t data_blocks = (body_size + block_size - 1) / block_size;
    uint32_t total_blocks = 1 + data_blocks;  // header block + data

    if (total_blocks > log->log_blocks) {
        mod::dbg::log("[xfs log] transaction too large for log (%u blocks needed, %u available)\n", total_blocks, log->log_blocks);
        return -ENOSPC;
    }

    // Check if we have enough space before wrapping
    uint32_t head = log->head_block;
    uint32_t avail = 0;
    if (head >= log->tail_block) {
        avail = log->log_blocks - head + log->tail_block;
    } else {
        avail = log->tail_block - head;
    }
    if (total_blocks >= avail) {
        mod::dbg::log("[xfs log] log full (%u blocks needed, %u available)\n", total_blocks, avail);
        return -ENOSPC;
    }

    // Build the log record header
    XlogRecHeader hdr{};
    hdr.h_magicno = __be32::from_cpu(XLOG_HEADER_MAGIC_NUM);
    hdr.h_cycle = __be32::from_cpu(log->head_cycle);
    hdr.h_version = __be32::from_cpu(2);
    hdr.h_len = __be32::from_cpu(body_size);

    uint64_t lsn = (static_cast<uint64_t>(log->head_cycle) << 32) | head;
    hdr.h_lsn = __be64::from_cpu(lsn);

    uint64_t tail_lsn = (static_cast<uint64_t>(log->tail_cycle) << 32) | log->tail_block;
    hdr.h_tail_lsn = __be64::from_cpu(tail_lsn);
    hdr.h_num_logops = __be32::from_cpu(num_logops);
    hdr.h_fmt = __be32::from_cpu(1);  // XLOG_FMT_LINUX_LE

    // Copy filesystem UUID
    hdr.h_fs_uuid = mount->uuid;
    hdr.h_size = __be32::from_cpu(block_size);

    // Write header block
    uint64_t hdr_disk_block = log->log_start + head;
    BufHead* hdr_bh = xfs_buf_read(mount, hdr_disk_block);
    if (hdr_bh == nullptr) {
        return -EIO;
    }

    // Zero the block and write the header (header is 1052B, but we write
    // just the first XLOG_HEADER_SIZE=512 bytes per the on-disk format;
    // the h_cycle_data array extends beyond but is part of the same struct)
    __builtin_memset(hdr_bh->data, 0, block_size);
    size_t write_len = sizeof(XlogRecHeader);
    write_len = std::min<size_t>(write_len, block_size);
    __builtin_memcpy(hdr_bh->data, &hdr, write_len);

    // Compute CRC over the header
    auto* on_disk_hdr = reinterpret_cast<XlogRecHeader*>(hdr_bh->data);
    on_disk_hdr->h_crc = 0;
    uint32_t hdr_crc = util::crc32c_compute(hdr_bh->data, write_len);
    on_disk_hdr->h_crc = hdr_crc;  // little-endian on disk

    int wrc = bwrite(hdr_bh);
    if (wrc != 0) {
        return wrc;
    }

    // Write body data blocks
    uint32_t cur_block = (head + 1) % log->log_blocks;
    uint32_t body_offset = 0;

    // Temporary buffer for assembling body data
    auto* body_buf = static_cast<uint8_t*>(mod::mm::dyn::kmalloc::malloc(body_size));
    if (body_buf == nullptr) {
        return -ENOMEM;
    }

    // Serialize log items into body buffer
    uint32_t pos = 0;
    for (int i = 0; i < item_count; i++) {
        if (items[i].type == XfsLogItemType::Buffer && items[i].buf.dirty) {
            uint64_t blk = items[i].buf.bp->block_no;
            uint32_t off = items[i].buf.offset;
            uint32_t len = items[i].buf.len;
            __builtin_memcpy(body_buf + pos, &blk, 8);
            pos += 8;
            __builtin_memcpy(body_buf + pos, &off, 4);
            pos += 4;
            __builtin_memcpy(body_buf + pos, &len, 4);
            pos += 4;
            __builtin_memcpy(body_buf + pos, items[i].buf.bp->data + off, len);
            pos += len;
        } else if (items[i].type == XfsLogItemType::Inode && items[i].inode.ip != nullptr) {
            uint64_t ino = items[i].inode.ip->ino;
            __builtin_memcpy(body_buf + pos, &ino, 8);
            pos += 8;
        }
    }

    // Write body data block by block
    while (body_offset < body_size) {
        uint64_t data_disk_block = log->log_start + cur_block;
        BufHead* data_bh = xfs_buf_read(mount, data_disk_block);
        if (data_bh == nullptr) {
            mod::mm::dyn::kmalloc::free(body_buf);
            return -EIO;
        }

        uint32_t chunk = body_size - body_offset;
        chunk = std::min(chunk, block_size);
        __builtin_memset(data_bh->data, 0, block_size);
        __builtin_memcpy(data_bh->data, body_buf + body_offset, chunk);

        wrc = bwrite(data_bh);
        if (wrc != 0) {
            mod::mm::dyn::kmalloc::free(body_buf);
            return wrc;
        }

        body_offset += chunk;
        cur_block = (cur_block + 1) % log->log_blocks;
    }

    mod::mm::dyn::kmalloc::free(body_buf);

    // Advance the log head
    log->tail_block = head;
    log->tail_cycle = log->head_cycle;
    log->head_block = cur_block;
    if (cur_block < head) {
        log->head_cycle++;
    }
    log->clean = false;

    return 0;
}

}  // namespace ker::vfs::xfs
