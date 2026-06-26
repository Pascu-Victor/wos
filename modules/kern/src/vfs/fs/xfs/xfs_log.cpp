// XFS Log implementation - journal recovery and basic log management.
//
// On mount, the log is scanned to find the head and tail.  If they differ,
// recovery is needed: log records are replayed to bring the filesystem back
// to a consistent state.
//
// This implementation handles log head/tail detection, basic recovery for the
// compact WOS log item format, and synchronous transaction log writes.
//
// Reference: reference/xfs/xfs_log.c, reference/xfs/xfs_log_recover.c

#include "xfs_log.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <util/crc32c.hpp>
#include <utility>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_trans.hpp>

#include "net/endian.hpp"
#include "vfs/fs/xfs/xfs_format.hpp"
#include "vfs/fs/xfs/xfs_mount.hpp"

namespace ker::vfs::xfs {

namespace {

constexpr size_t XFS_LOG_STACK_BODY_MAX_BYTES = 4096;

auto xfs_log_serialize_body(const XfsTransItem* items, int item_count, uint8_t* body_buf, size_t body_capacity) -> int {
    if (items == nullptr || body_buf == nullptr || item_count < 0) {
        return -EINVAL;
    }

    size_t pos = 0;
    for (int i = 0; i < item_count; i++) {
        if (items[i].type == XfsLogItemType::BUFFER && items[i].buf.dirty) {
            uint64_t blk = items[i].buf.bp->block_no;
            uint32_t off = items[i].buf.offset;
            uint32_t len = items[i].buf.len;
            size_t const ITEM_BYTES = 8 + 4 + 4 + static_cast<size_t>(len);
            if (pos > body_capacity || ITEM_BYTES > body_capacity - pos) {
                return -EOVERFLOW;
            }
            __builtin_memcpy(body_buf + pos, &blk, 8);
            pos += 8;
            __builtin_memcpy(body_buf + pos, &off, 4);
            pos += 4;
            __builtin_memcpy(body_buf + pos, &len, 4);
            pos += 4;
            __builtin_memcpy(body_buf + pos, items[i].buf.bp->data + off, len);
            pos += len;
        }
    }
    return 0;
}

auto xfs_log_write_header_bytes(const XlogRecHeader& hdr, uint8_t* dst, uint32_t block_size) -> int {
    if (dst == nullptr || block_size == 0) {
        return -EINVAL;
    }

    __builtin_memset(dst, 0, block_size);
    size_t write_len = sizeof(XlogRecHeader);
    write_len = std::min<size_t>(write_len, block_size);
    __builtin_memcpy(dst, &hdr, write_len);

    auto* on_disk_hdr = reinterpret_cast<XlogRecHeader*>(dst);
    on_disk_hdr->h_crc = 0;
    uint32_t const HDR_CRC = util::crc32c_compute(dst, write_len);
    on_disk_hdr->h_crc = HDR_CRC;  // little-endian on disk
    return 0;
}

void xfs_log_advance_head(XfsLog* log, uint32_t old_head, uint32_t new_head) {
    if (log == nullptr) {
        return;
    }

    log->tail_block = old_head;
    log->tail_cycle = log->head_cycle;
    log->head_block = new_head;
    if (new_head < old_head) {
        log->head_cycle++;
    }
    log->clean = false;
}

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
        // No valid log record at position 0 - log is clean (never used or
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
    uint32_t const HEAD_CYCLE = hdr->h_cycle.to_cpu();
    uint64_t const TAIL_LSN = hdr->h_tail_lsn.to_cpu();
    auto tail_cycle = static_cast<uint32_t>(TAIL_LSN >> 32);
    auto tail_block = static_cast<uint32_t>(TAIL_LSN);

    brelse(bh);

    // Scan forward to find the actual log head (last valid record)
    // For now, do a simplified scan: check if the last block has the same cycle
    uint32_t const LAST_BLOCK = log->log_blocks - 1;
    BufHead* last_bh = xfs_buf_read(ctx, log->log_start + LAST_BLOCK);
    if (last_bh == nullptr) {
        mod::dbg::log("[xfs log] failed to read last log block\n");
        return -EIO;
    }

    // The first 4 bytes of each log block contain the cycle number
    Be32 last_cycle_be{};
    __builtin_memcpy(&last_cycle_be, last_bh->data, sizeof(Be32));
    uint32_t const LAST_CYCLE = last_cycle_be.to_cpu();
    brelse(last_bh);

    if (LAST_CYCLE == HEAD_CYCLE) {
        // Log wraps but head is somewhere; for simplicity, treat as needing
        // a more thorough scan.
        log->head_cycle = HEAD_CYCLE;
        log->head_block = 0;
        log->tail_cycle = tail_cycle;
        log->tail_block = tail_block;
        log->clean = (HEAD_CYCLE == tail_cycle && tail_block == 0);
    } else {
        // Head and last block have different cycles - log has wrapped
        log->head_cycle = LAST_CYCLE;
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
    uint32_t const BLOCK_SIZE = ctx->block_size;

    uint64_t const DISK_BLOCK = log->log_start + block;
    BufHead* hdr_bh = xfs_buf_read(ctx, DISK_BLOCK);
    if (hdr_bh == nullptr) {
        mod::dbg::log("[xfs log recover] failed to read log block %u", block);
        return -1;
    }

    const auto* hdr = reinterpret_cast<const XlogRecHeader*>(hdr_bh->data);
    if (hdr->h_magicno.to_cpu() != XLOG_HEADER_MAGIC_NUM) {
        brelse(hdr_bh);
        return -1;
    }

    uint32_t const BODY_SIZE = hdr->h_len.to_cpu();
    uint32_t const NUM_LOGOPS = hdr->h_num_logops.to_cpu();
    brelse(hdr_bh);

    if (BODY_SIZE == 0 || NUM_LOGOPS == 0) {
        // Empty record - advance past header
        return static_cast<int>((block + 1) % log->log_blocks);
    }

    // Read body data
    uint32_t const DATA_BLOCKS = (BODY_SIZE + BLOCK_SIZE - 1) / BLOCK_SIZE;
    auto* body_buf = new (std::nothrow) uint8_t[static_cast<size_t>(DATA_BLOCKS) * BLOCK_SIZE];
    if (body_buf == nullptr) {
        return -1;
    }

    uint32_t cur_block = (block + 1) % log->log_blocks;
    uint32_t body_offset = 0;

    for (uint32_t b = 0; b < DATA_BLOCKS; b++) {
        uint64_t const DATA_DISK_BLOCK = log->log_start + cur_block;
        BufHead* data_bh = xfs_buf_read(ctx, DATA_DISK_BLOCK);
        if (data_bh == nullptr) {
            delete[] body_buf;
            return -1;
        }

        uint32_t chunk = BODY_SIZE - body_offset;
        chunk = std::min(chunk, BLOCK_SIZE);
        __builtin_memcpy(body_buf + body_offset, data_bh->data, chunk);
        brelse(data_bh);

        body_offset += chunk;
        cur_block = (cur_block + 1) % log->log_blocks;
    }

    // Replay buffer modifications from the body
    // Format per buffer item: block_no(8B) + offset(4B) + len(4B) + data(lenB)
    // Format per inode item: ino(8B) - we skip inode items during recovery
    uint32_t pos = 0;
    uint32_t replayed = 0;

    while (pos < BODY_SIZE && replayed < NUM_LOGOPS) {
        // Peek ahead to determine item type
        // Buffer items: need at least 16B header (8+4+4)
        // Inode items: need at least 8B
        if (pos + 16 <= BODY_SIZE) {
            // Try as buffer item
            uint64_t blk_no = 0;
            uint32_t off = 0;
            uint32_t len = 0;
            __builtin_memcpy(&blk_no, body_buf + pos, 8);
            __builtin_memcpy(&off, body_buf + pos + 8, 4);
            __builtin_memcpy(&len, body_buf + pos + 12, 4);

            // Sanity check: len should be reasonable
            if (len > 0 && len <= BLOCK_SIZE * 4 && pos + 16 + len <= BODY_SIZE) {
                // Replay this buffer modification
                BufHead* target_bh = bread(ctx->device, blk_no);
                if (target_bh != nullptr) {
                    if (off + len <= target_bh->size) {
                        __builtin_memcpy(target_bh->data + off, body_buf + pos + 16, len);
                        bdirty(target_bh);
                        int const WRC = bwrite(target_bh);
                        if (WRC == 0) {
                            replayed++;
                        }
                    }
                }
                pos += 16 + len;
                continue;
            }
        }

        // Otherwise, treat as inode item (8 bytes) or skip
        if (pos + 8 <= BODY_SIZE) {
            pos += 8;
            replayed++;
        } else {
            break;
        }
    }

    delete[] body_buf;

    mod::dbg::log("[xfs log recover] replayed %u/%u ops from log block %u", replayed, NUM_LOGOPS, block);

    return static_cast<int>(cur_block);
}

// Recover the log by scanning from tail to head and replaying all records.
auto xfs_log_recover(XfsLog* log) -> int {
    mod::dbg::log("[xfs log recover] starting recovery: tail=%u.%u head=%u.%u", log->tail_cycle, log->tail_block, log->head_cycle,
                  log->head_block);

    uint32_t cur_block = log->tail_block;
    uint32_t cur_cycle = log->tail_cycle;
    int records = 0;

    while (cur_cycle < log->head_cycle || (cur_cycle == log->head_cycle && cur_block < log->head_block)) {
        int const NEXT = replay_log_record(log, cur_block);
        if (NEXT < 0) {
            mod::dbg::log("[xfs log recover] failed at block %u, stopping", cur_block);
            break;
        }
        records++;

        auto next_block = static_cast<uint32_t>(NEXT);
        if (next_block < cur_block) {
            cur_cycle++;  // wrapped around
        }
        cur_block = next_block;

        // Safety: don't loop forever
        if (std::cmp_greater(records, log->log_blocks)) {
            mod::dbg::log("[xfs log recover] too many records, aborting");
            break;
        }
    }

    mod::dbg::log("[xfs log recover] recovery complete: %d records replayed", records);

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
        mod::dbg::log("[xfs log] no log area configured");
        return -EINVAL;
    }

    auto* log = new XfsLog{};
    log->mount = mount;
    log->log_start = mount->log_start;
    log->log_blocks = mount->log_blocks;
    log->sect_size = mount->sect_size;
    log->clean = true;
    log->active = false;

    int const RC = xfs_log_find_head_tail(log);
    if (RC != 0) {
        delete log;
        return RC;
    }

    if (!log->clean) {
        if (mount->read_only) {
            mod::dbg::log("[xfs log] log is dirty but mount is read-only - recovery deferred");
        } else {
            mod::dbg::log("[xfs log] log is dirty - recovery needed");
            int const RRC = xfs_log_recover(log);
            if (RRC != 0) {
                mod::dbg::log("[xfs log] recovery failed: %d", RRC);
                delete log;
                return RRC;
            }
        }
    } else {
        mod::dbg::log("[xfs log] log is clean");
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
    mod::dbg::log("[xfs log] log unmounted");

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
    // offset (4B) + len (4B) + data (len B). INODE transaction items are
    // commit-time inputs only: xfs_inode_write() logs their concrete inode
    // buffers before this point, and recovery replays those buffer records.
    uint32_t body_size = 0;
    uint32_t num_logops = 0;
    for (int i = 0; i < item_count; i++) {
        if (items[i].type == XfsLogItemType::BUFFER && items[i].buf.dirty) {
            body_size += 8 + 4 + 4 + items[i].buf.len;
            num_logops++;
        }
    }

    if (num_logops == 0) {
        return 0;  // nothing to log
    }

    // Number of blocks needed: header (1) + data blocks
    uint32_t const BLOCK_SIZE = mount->block_size;
    uint32_t const DATA_BLOCKS = (body_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    uint32_t const TOTAL_BLOCKS = 1 + DATA_BLOCKS;  // header block + data

    if (TOTAL_BLOCKS > log->log_blocks) {
        mod::dbg::log("[xfs log] transaction too large for log (%u blocks needed, %u available)", TOTAL_BLOCKS, log->log_blocks);
        return -ENOSPC;
    }

    // Check if we have enough space before wrapping
    uint32_t const HEAD = log->head_block;
    uint32_t avail = 0;
    if (HEAD >= log->tail_block) {
        avail = log->log_blocks - HEAD + log->tail_block;
    } else {
        avail = log->tail_block - HEAD;
    }
    if (TOTAL_BLOCKS >= avail) {
        mod::dbg::log("[xfs log] log full (%u blocks needed, %u available)", TOTAL_BLOCKS, avail);
        return -ENOSPC;
    }

    // Build the log record header
    XlogRecHeader hdr{};
    hdr.h_magicno = Be32::from_cpu(XLOG_HEADER_MAGIC_NUM);
    hdr.h_cycle = Be32::from_cpu(log->head_cycle);
    hdr.h_version = Be32::from_cpu(2);
    hdr.h_len = Be32::from_cpu(body_size);

    uint64_t const LSN = (static_cast<uint64_t>(log->head_cycle) << 32) | HEAD;
    hdr.h_lsn = Be64::from_cpu(LSN);

    uint64_t const TAIL_LSN = (static_cast<uint64_t>(log->tail_cycle) << 32) | log->tail_block;
    hdr.h_tail_lsn = Be64::from_cpu(TAIL_LSN);
    hdr.h_num_logops = Be32::from_cpu(num_logops);
    hdr.h_fmt = Be32::from_cpu(1);  // XLOG_FMT_LINUX_LE

    // Copy filesystem UUID
    hdr.h_fs_uuid = mount->uuid;
    hdr.h_size = Be32::from_cpu(BLOCK_SIZE);

    uint64_t const HDR_DISK_BLOCK = log->log_start + HEAD;
    uint32_t cur_block = (HEAD + 1) % log->log_blocks;
    uint32_t body_offset = 0;

    if (DATA_BLOCKS == 1 && HEAD <= log->log_blocks - TOTAL_BLOCKS) {
        BufHead* record_bh = xfs_buf_get_multi(mount, HDR_DISK_BLOCK, TOTAL_BLOCKS);
        if (record_bh == nullptr) {
            return -EIO;
        }

        int rc = xfs_log_write_header_bytes(hdr, record_bh->data, BLOCK_SIZE);
        if (rc == 0) {
            uint8_t* body_dst = record_bh->data + BLOCK_SIZE;
            __builtin_memset(body_dst, 0, BLOCK_SIZE);
            rc = xfs_log_serialize_body(items, item_count, body_dst, BLOCK_SIZE);
        }
        if (rc != 0) {
            brelse(record_bh);
            return rc;
        }

        bdirty(record_bh);
        brelse(record_bh);
        cur_block = (HEAD + TOTAL_BLOCKS) % log->log_blocks;
        xfs_log_advance_head(log, HEAD, cur_block);
        return 0;
    }

    // Write header block - use xfs_buf_get (not xfs_buf_read) since we
    // immediately zero and overwrite the entire block; no need to read from disk.
    BufHead* hdr_bh = xfs_buf_get(mount, HDR_DISK_BLOCK);
    if (hdr_bh == nullptr) {
        return -EIO;
    }

    int const HEADER_RC = xfs_log_write_header_bytes(hdr, hdr_bh->data, BLOCK_SIZE);
    if (HEADER_RC != 0) {
        brelse(hdr_bh);
        return HEADER_RC;
    }

    bdirty(hdr_bh);
    brelse(hdr_bh);

    if (DATA_BLOCKS == 1) {
        uint64_t const DATA_DISK_BLOCK = log->log_start + cur_block;
        BufHead* data_bh = xfs_buf_get(mount, DATA_DISK_BLOCK);
        if (data_bh == nullptr) {
            return -EIO;
        }
        __builtin_memset(data_bh->data, 0, BLOCK_SIZE);
        int const SERIALIZE_RC = xfs_log_serialize_body(items, item_count, data_bh->data, BLOCK_SIZE);
        if (SERIALIZE_RC != 0) {
            brelse(data_bh);
            return SERIALIZE_RC;
        }
        bdirty(data_bh);
        brelse(data_bh);
        cur_block = (cur_block + 1) % log->log_blocks;
        xfs_log_advance_head(log, HEAD, cur_block);
        return 0;
    }

    if (DATA_BLOCKS != 0 && BLOCK_SIZE > SIZE_MAX / DATA_BLOCKS) {
        return -EOVERFLOW;
    }

    size_t const BODY_BUFFER_BYTES = static_cast<size_t>(DATA_BLOCKS) * BLOCK_SIZE;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init): serialized before any byte is read.
    std::array<uint8_t, XFS_LOG_STACK_BODY_MAX_BYTES> stack_body_buf;
    auto* body_buf = stack_body_buf.data();
    bool heap_body_buf = false;
    if (BODY_BUFFER_BYTES > stack_body_buf.size()) {
        body_buf = new (std::nothrow) uint8_t[BODY_BUFFER_BYTES];
        heap_body_buf = true;
    }
    if (body_buf == nullptr) {
        return -ENOMEM;
    }

    int const SERIALIZE_RC = xfs_log_serialize_body(items, item_count, body_buf, BODY_BUFFER_BYTES);
    if (SERIALIZE_RC != 0) {
        if (heap_body_buf) {
            delete[] body_buf;
        }
        return SERIALIZE_RC;
    }

    // Write body data block by block
    while (body_offset < body_size) {
        uint64_t const DATA_DISK_BLOCK = log->log_start + cur_block;
        // Use xfs_buf_get - we zero+overwrite the full block, no read needed.
        BufHead* data_bh = xfs_buf_get(mount, DATA_DISK_BLOCK);
        if (data_bh == nullptr) {
            if (heap_body_buf) {
                delete[] body_buf;
            }
            return -EIO;
        }

        uint32_t chunk = body_size - body_offset;
        chunk = std::min(chunk, BLOCK_SIZE);
        __builtin_memset(data_bh->data, 0, BLOCK_SIZE);
        __builtin_memcpy(data_bh->data, body_buf + body_offset, chunk);

        bdirty(data_bh);
        brelse(data_bh);

        body_offset += chunk;
        cur_block = (cur_block + 1) % log->log_blocks;
    }

    if (heap_body_buf) {
        delete[] body_buf;
    }

    // Advance the log head
    xfs_log_advance_head(log, HEAD, cur_block);

    return 0;
}

}  // namespace ker::vfs::xfs
