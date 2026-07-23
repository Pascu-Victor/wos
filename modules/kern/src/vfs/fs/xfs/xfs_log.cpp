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
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sys/mutex.hpp>
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
constexpr size_t XFS_LOG_BATCH_MAX_ITEMS = 8192;
constexpr uint32_t XFS_LOG_BATCH_MAX_TRANSACTIONS = 256;
constexpr size_t XFS_LOG_BATCH_MAX_BODY_BYTES = size_t{4} * 1024 * 1024;
ker::mod::sys::Mutex log_write_lock;

class XfsLogWriteGuard {
   public:
    XfsLogWriteGuard() {
        constexpr int ADAPTIVE_SPIN_LIMIT = 4096;
        for (int i = 0; i < ADAPTIVE_SPIN_LIMIT; ++i) {
            if (log_write_lock.try_lock()) {
                return;
            }
            asm volatile("pause" ::: "memory");
        }
        log_write_lock.lock();
    }

    ~XfsLogWriteGuard() { log_write_lock.unlock(); }

    XfsLogWriteGuard(const XfsLogWriteGuard&) = delete;
    XfsLogWriteGuard(XfsLogWriteGuard&&) = delete;
    auto operator=(const XfsLogWriteGuard&) -> XfsLogWriteGuard& = delete;
    auto operator=(XfsLogWriteGuard&&) -> XfsLogWriteGuard& = delete;
};

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

struct XfsLogBatch {
    XfsMountContext* mount{};
    std::array<XfsTransItem, XFS_LOG_BATCH_MAX_ITEMS> items{};
    size_t item_count{};
    size_t body_bytes{};
    uint32_t transactions{};
};

XfsLogBatch* active_batch = nullptr;

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

    auto* batch = new (std::nothrow) XfsLogBatch{};
    if (batch == nullptr) {
        delete log;
        return -ENOMEM;
    }
    batch->mount = mount;
    log->active = true;
    active_log = log;
    active_batch = batch;
    return 0;
}

void xfs_log_unmount(XfsMountContext* mount) {
    static_cast<void>(xfs_log_flush(mount));
    XfsLogWriteGuard guard;
    if (active_log == nullptr || active_log->mount != mount) {
        return;
    }

    active_log->active = false;
    mod::dbg::log("[xfs log] log unmounted");

    delete active_log;
    active_log = nullptr;
    delete active_batch;
    active_batch = nullptr;
}

auto xfs_log_needs_recovery(XfsMountContext* mount) -> bool {
    if (active_log == nullptr || active_log->mount != mount) {
        return false;
    }
    return !active_log->clean;
}

namespace {

auto xfs_log_write_record_locked(XfsMountContext* mount, const XfsTransItem* items, int item_count) -> int {
    uint64_t const PERF_STARTED_US =
        ker::mod::perf::is_wki_scope_recording_enabled(ker::mod::perf::WkiPerfScope::LOCAL_XFS,
                                                       static_cast<uint8_t>(ker::mod::perf::WkiPerfLocalXfsOp::LOG_WRITE))
            ? ker::mod::time::get_us()
            : 0;
    if (mount == nullptr || active_log == nullptr || active_log->mount != mount) {
        return -EINVAL;
    }
    if (!active_log->active) {
        return -EINVAL;
    }
    XfsLog* log = active_log;
    uint64_t perf_body_bytes = 0;
    uint64_t perf_block_bytes = 0;
    auto finish_log_write = [&](int status) -> int {
        if (PERF_STARTED_US != 0) {
            uint64_t const NOW_US = ker::mod::time::get_us();
            uint64_t const ELAPSED_US = NOW_US >= PERF_STARTED_US ? NOW_US - PERF_STARTED_US : 0;
            auto const CLAMPED_US = static_cast<uint32_t>(std::min<uint64_t>(ELAPSED_US, UINT32_MAX));
            ker::mod::perf::record_local_xfs_summary(ker::mod::perf::WkiPerfLocalXfsOp::LOG_WRITE, status, CLAMPED_US, true,
                                                     perf_body_bytes);
            ker::mod::perf::record_local_xfs_summary(ker::mod::perf::WkiPerfLocalXfsOp::LOG_BLOCKS, status, 0, false, perf_block_bytes);
        }
        return status;
    };

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
        return finish_log_write(0);  // nothing to log
    }
    perf_body_bytes = body_size;

    // Number of blocks needed: header (1) + data blocks
    uint32_t const BLOCK_SIZE = mount->block_size;
    uint32_t const DATA_BLOCKS = (body_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    uint32_t const TOTAL_BLOCKS = 1 + DATA_BLOCKS;  // header block + data
    perf_block_bytes = static_cast<uint64_t>(TOTAL_BLOCKS) * BLOCK_SIZE;

    if (TOTAL_BLOCKS > log->log_blocks) {
        mod::dbg::log("[xfs log] transaction too large for log (%u blocks needed, %u available)", TOTAL_BLOCKS, log->log_blocks);
        return finish_log_write(-ENOSPC);
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
        return finish_log_write(-ENOSPC);
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
            return finish_log_write(-EIO);
        }

        int rc = xfs_log_write_header_bytes(hdr, record_bh->data, BLOCK_SIZE);
        if (rc == 0) {
            uint8_t* body_dst = record_bh->data + BLOCK_SIZE;
            rc = xfs_log_serialize_body(items, item_count, body_dst, BLOCK_SIZE);
        }
        if (rc != 0) {
            brelse(record_bh);
            return finish_log_write(rc);
        }

        bdirty(record_bh);
        brelse(record_bh);
        cur_block = (HEAD + TOTAL_BLOCKS) % log->log_blocks;
        xfs_log_advance_head(log, HEAD, cur_block);
        return finish_log_write(0);
    }

    // Write header block - use xfs_buf_get (not xfs_buf_read) since we
    // immediately zero and overwrite the entire block; no need to read from disk.
    BufHead* hdr_bh = xfs_buf_get(mount, HDR_DISK_BLOCK);
    if (hdr_bh == nullptr) {
        return finish_log_write(-EIO);
    }

    int const HEADER_RC = xfs_log_write_header_bytes(hdr, hdr_bh->data, BLOCK_SIZE);
    if (HEADER_RC != 0) {
        brelse(hdr_bh);
        return finish_log_write(HEADER_RC);
    }

    bdirty(hdr_bh);
    brelse(hdr_bh);

    if (DATA_BLOCKS == 1) {
        uint64_t const DATA_DISK_BLOCK = log->log_start + cur_block;
        BufHead* data_bh = xfs_buf_get(mount, DATA_DISK_BLOCK);
        if (data_bh == nullptr) {
            return finish_log_write(-EIO);
        }
        int const SERIALIZE_RC = xfs_log_serialize_body(items, item_count, data_bh->data, BLOCK_SIZE);
        if (SERIALIZE_RC != 0) {
            brelse(data_bh);
            return finish_log_write(SERIALIZE_RC);
        }
        bdirty(data_bh);
        brelse(data_bh);
        cur_block = (cur_block + 1) % log->log_blocks;
        xfs_log_advance_head(log, HEAD, cur_block);
        return finish_log_write(0);
    }

    if (DATA_BLOCKS != 0 && BLOCK_SIZE > SIZE_MAX / DATA_BLOCKS) {
        return finish_log_write(-EOVERFLOW);
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
        return finish_log_write(-ENOMEM);
    }

    int const SERIALIZE_RC = xfs_log_serialize_body(items, item_count, body_buf, BODY_BUFFER_BYTES);
    if (SERIALIZE_RC != 0) {
        if (heap_body_buf) {
            delete[] body_buf;
        }
        return finish_log_write(SERIALIZE_RC);
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
            return finish_log_write(-EIO);
        }

        uint32_t chunk = body_size - body_offset;
        chunk = std::min(chunk, BLOCK_SIZE);
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

    return finish_log_write(0);
}

auto xfs_log_item_matches_buffer(const XfsTransItem& item, const BufHead* bp) -> bool {
    if (item.type != XfsLogItemType::BUFFER || item.buf.bp == nullptr || bp == nullptr) {
        return false;
    }
    BufHead const* existing = item.buf.bp;
    // A retired buffer describes the previous owner of this physical range.
    // If the allocator has already recycled the range, the new live buffer
    // must remain a separate batch item so flush can dirty its home image.
    if (existing->retired.load(std::memory_order_acquire) || bp->retired.load(std::memory_order_acquire)) {
        return false;
    }
    return existing == bp || (existing->bdev == bp->bdev && existing->block_no == bp->block_no && existing->size == bp->size);
}

auto xfs_log_batch_find_item(XfsLogBatch* batch, const BufHead* bp) -> XfsTransItem* {
    if (batch == nullptr || bp == nullptr) {
        return nullptr;
    }
    for (size_t i = 0; i < batch->item_count; ++i) {
        if (xfs_log_item_matches_buffer(batch->items.at(i), bp)) {
            return &batch->items.at(i);
        }
    }
    return nullptr;
}

auto xfs_log_transaction_shape(const XfsTransItem* items, int item_count, size_t* dirty_items_out, size_t* body_bytes_out) -> int {
    if (items == nullptr || item_count < 0 || dirty_items_out == nullptr || body_bytes_out == nullptr) {
        return -EINVAL;
    }
    size_t dirty_items = 0;
    size_t body_bytes = 0;
    for (int i = 0; i < item_count; ++i) {
        if (items[i].type != XfsLogItemType::BUFFER || !items[i].buf.dirty) {
            continue;
        }
        BufHead const* bp = items[i].buf.bp;
        uint32_t const OFFSET = items[i].buf.offset;
        uint32_t const LEN = items[i].buf.len;
        if (bp == nullptr || bp->retired.load(std::memory_order_acquire) || OFFSET > bp->size || LEN > bp->size - OFFSET ||
            body_bytes > SIZE_MAX - 16U - LEN) {
            return -EIO;
        }
        dirty_items++;
        body_bytes += 16U + LEN;
    }
    *dirty_items_out = dirty_items;
    *body_bytes_out = body_bytes;
    return 0;
}

void xfs_log_batch_reset(XfsLogBatch* batch) {
    if (batch == nullptr) {
        return;
    }
    batch->item_count = 0;
    batch->body_bytes = 0;
    batch->transactions = 0;
}

auto xfs_log_batch_flush_locked(XfsMountContext* mount) -> int {
    if (active_batch == nullptr || active_batch->mount != mount) {
        return -EINVAL;
    }
    if (active_batch->item_count == 0) {
        xfs_log_batch_reset(active_batch);
        return 0;
    }

    int const RC = xfs_log_write_record_locked(mount, active_batch->items.data(), static_cast<int>(active_batch->item_count));
    if (RC != 0) {
        // Keep the batch and its journal holds intact.  Metadata must not
        // become writeback-eligible until a WAL retry succeeds.
        return RC;
    }
    for (size_t i = 0; i < active_batch->item_count; ++i) {
        XfsTransItem& item = active_batch->items.at(i);
        if (item.type != XfsLogItemType::BUFFER || item.buf.bp == nullptr) {
            continue;
        }
        bdirty(item.buf.bp);
        bjournal_release(item.buf.bp);
        brelse(item.buf.bp);
        item.buf.bp = nullptr;
        item.type = XfsLogItemType::NONE;
    }
    xfs_log_batch_reset(active_batch);
    return RC;
}

auto xfs_log_batch_add_locked(const XfsTransItem* items, int item_count) -> int {
    if (active_batch == nullptr || items == nullptr || item_count < 0) {
        return -EINVAL;
    }

    for (int i = 0; i < item_count; ++i) {
        if (items[i].type != XfsLogItemType::BUFFER || !items[i].buf.dirty || items[i].buf.bp == nullptr) {
            continue;
        }

        BufHead* bp = items[i].buf.bp;
        uint32_t const OFFSET = items[i].buf.offset;
        uint32_t const LEN = items[i].buf.len;
        if (bp->retired.load(std::memory_order_acquire)) {
            return -EIO;
        }
        if (XfsTransItem* existing = xfs_log_batch_find_item(active_batch, bp)) {
            if (existing->buf.bp != bp) {
                __builtin_memcpy(existing->buf.bp->data + OFFSET, bp->data + OFFSET, LEN);
            }
            uint32_t const OLD_LEN = existing->buf.len;
            uint32_t const OLD_END = existing->buf.offset + OLD_LEN;
            uint32_t const NEW_END = OFFSET + LEN;
            uint32_t const START = std::min(existing->buf.offset, OFFSET);
            uint32_t const END = std::max(OLD_END, NEW_END);
            existing->buf.offset = START;
            existing->buf.len = END - START;
            active_batch->body_bytes += existing->buf.len - OLD_LEN;
            continue;
        }

        if (active_batch->item_count >= active_batch->items.size()) {
            return -E2BIG;
        }
        bp->refcount.fetch_add(1, std::memory_order_relaxed);
        bjournal_hold(bp);
        XfsTransItem& dst = active_batch->items.at(active_batch->item_count++);
        dst.type = XfsLogItemType::BUFFER;
        dst.buf = items[i].buf;
        active_batch->body_bytes += 16U + LEN;
    }
    active_batch->transactions++;
    return 0;
}

}  // anonymous namespace

auto xfs_log_write(XfsMountContext* mount, const XfsTransItem* items, int item_count, bool* owns_metadata_out) -> int {
    if (owns_metadata_out != nullptr) {
        *owns_metadata_out = false;
    }

    size_t dirty_items = 0;
    size_t transaction_body_bytes = 0;
    int const SHAPE_RC = xfs_log_transaction_shape(items, item_count, &dirty_items, &transaction_body_bytes);
    if (SHAPE_RC != 0) {
        return SHAPE_RC;
    }
    if (dirty_items == 0) {
        return 0;
    }

    XfsLogWriteGuard guard;
    if (mount == nullptr || active_log == nullptr || active_log->mount != mount || !active_log->active || active_batch == nullptr ||
        active_batch->mount != mount) {
        return -ENODEV;
    }

    int result = 0;
    bool const FLUSH_BEFORE_ADD =
        active_batch->item_count != 0 && (active_batch->item_count + dirty_items > active_batch->items.size() ||
                                          active_batch->body_bytes + transaction_body_bytes > XFS_LOG_BATCH_MAX_BODY_BYTES);
    if (FLUSH_BEFORE_ADD) {
        result = xfs_log_batch_flush_locked(mount);
        if (result != 0) {
            return result;
        }
    }

    if (dirty_items > active_batch->items.size()) {
        return result != 0 ? result : -E2BIG;
    }
    int const ADD_RC = xfs_log_batch_add_locked(items, item_count);
    if (ADD_RC != 0) {
        return result != 0 ? result : ADD_RC;
    }
    if (owns_metadata_out != nullptr) {
        *owns_metadata_out = true;
    }

    if (active_batch->transactions >= XFS_LOG_BATCH_MAX_TRANSACTIONS || active_batch->body_bytes >= XFS_LOG_BATCH_MAX_BODY_BYTES) {
        int const FLUSH_RC = xfs_log_batch_flush_locked(mount);
        if (FLUSH_RC != 0) {
            // The transaction has already been accepted into the retained
            // batch.  Its metadata remains journal-held for a later retry.
            mod::dbg::log("[xfs log] deferred batch flush failed: %d", FLUSH_RC);
        }
    }
    return result;
}

auto xfs_log_flush(XfsMountContext* mount) -> int {
    XfsLogWriteGuard guard;
    if (mount == nullptr || active_log == nullptr || active_log->mount != mount || active_batch == nullptr ||
        active_batch->mount != mount) {
        return -EINVAL;
    }
    return xfs_log_batch_flush_locked(mount);
}

#ifdef WOS_SELFTEST
namespace {

auto xfs_log_recycle_selftest_read(ker::dev::BlockDevice* dev, uint64_t /*block*/, size_t count, void* buffer) -> int {
    __builtin_memset(buffer, 0, count * dev->block_size);
    return 0;
}

auto xfs_log_recycle_selftest_write(ker::dev::BlockDevice* /*dev*/, uint64_t /*block*/, size_t /*count*/, const void* /*buffer*/) -> int {
    return 0;
}

}  // namespace

auto xfs_selftest_log_recycled_buffer_is_distinct() -> bool {
    constexpr uint64_t BLOCK = 128;
    constexpr size_t COUNT = 8;

    ker::dev::BlockDevice dev{};
    dev.block_size = 512;
    dev.total_blocks = 1024;
    dev.read_blocks = xfs_log_recycle_selftest_read;
    dev.write_blocks = xfs_log_recycle_selftest_write;
    invalidate_bdev(&dev);

    BufHead* old = bget_multi(&dev, BLOCK, COUNT);
    if (old == nullptr) {
        return false;
    }
    __builtin_memset(old->data, 0xA5, old->size);

    // Mirror an active batch's reference and writeback hold.
    old->refcount.fetch_add(1, std::memory_order_relaxed);
    bjournal_hold(old);
    XfsLogBatch batch{};
    batch.items.at(0).type = XfsLogItemType::BUFFER;
    batch.items.at(0).buf = {
        .bp = old,
        .offset = 0,
        .len = static_cast<uint32_t>(old->size),
        .dirty = true,
    };
    batch.item_count = 1;

    bool ok = xfs_log_batch_find_item(&batch, old) == &batch.items.at(0) && retire_bdev_range(&dev, BLOCK, COUNT) &&
              old->retired.load(std::memory_order_acquire);

    BufHead* replacement = bget_multi(&dev, BLOCK, COUNT);
    ok = ok && replacement != nullptr && replacement != old && !replacement->retired.load(std::memory_order_acquire) &&
         xfs_log_batch_find_item(&batch, replacement) == nullptr;

    if (replacement != nullptr) {
        __builtin_memset(replacement->data, 0x3A, replacement->size);
        XfsTransItem replacement_item{};
        replacement_item.type = XfsLogItemType::BUFFER;
        replacement_item.buf = {
            .bp = replacement,
            .offset = 0,
            .len = static_cast<uint32_t>(replacement->size),
            .dirty = true,
        };

        size_t dirty_items = 0;
        size_t body_bytes = 0;
        ok = ok && xfs_log_transaction_shape(&replacement_item, 1, &dirty_items, &body_bytes) == 0 && dirty_items == 1 &&
             body_bytes == 16U + replacement->size;
        brelse(replacement);
    }

    size_t dirty_items = 0;
    size_t body_bytes = 0;
    ok = ok && xfs_log_transaction_shape(batch.items.data(), 1, &dirty_items, &body_bytes) == -EIO;

    bjournal_release(old);
    brelse(old);
    brelse(old);
    invalidate_bdev(&dev);
    return ok;
}
#endif

}  // namespace ker::vfs::xfs
