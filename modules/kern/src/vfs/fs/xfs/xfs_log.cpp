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
#include <dev/block_device.hpp>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sys/mutex.hpp>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_trans.hpp>

#include "net/endian.hpp"
#include "vfs/fs/xfs/xfs_format.hpp"
#include "vfs/fs/xfs/xfs_log_codec.hpp"
#include "vfs/fs/xfs/xfs_mount.hpp"

namespace ker::vfs::xfs {

enum class XfsLogBatchPhase : uint8_t {
    COLLECTING,
    LOG_STAGED,
    LOG_DURABLE,
    HOME_RELEASED,
};

struct XfsLogBatch {
    XfsMountContext* mount{};
    std::array<XfsTransItem, 8192> items{};
    size_t item_count{};
    size_t body_bytes{};
    XfsLogBatchPhase phase{XfsLogBatchPhase::COLLECTING};
};

namespace {

constexpr size_t XFS_LOG_STACK_BODY_MAX_BYTES = 4096;
constexpr size_t XFS_LOG_BATCH_MAX_BODY_BYTES = size_t{4} * 1024 * 1024;
constexpr size_t XFS_LOG_MAX_BODY_BYTES = XFS_LOG_BATCH_MAX_BODY_BYTES + sizeof(WosLogBodyHeader);

class XfsLogWriteGuard {
   public:
    explicit XfsLogWriteGuard(XfsMountContext* mount) : lock(mount != nullptr ? &mount->journal_lock : nullptr) {
        if (lock == nullptr) {
            return;
        }
        constexpr int ADAPTIVE_SPIN_LIMIT = 4096;
        for (int i = 0; i < ADAPTIVE_SPIN_LIMIT; ++i) {
            if (lock->try_lock()) {
                return;
            }
            asm volatile("pause" ::: "memory");
        }
        lock->lock();
    }

    ~XfsLogWriteGuard() {
        if (lock != nullptr) {
            lock->unlock();
        }
    }

    XfsLogWriteGuard(const XfsLogWriteGuard&) = delete;
    XfsLogWriteGuard(XfsLogWriteGuard&&) = delete;
    auto operator=(const XfsLogWriteGuard&) -> XfsLogWriteGuard& = delete;
    auto operator=(XfsLogWriteGuard&&) -> XfsLogWriteGuard& = delete;

   private:
    ker::mod::sys::Mutex* lock;
};

auto xfs_uuid_equal(const XfsUuidT& lhs, const XfsUuidT& rhs) -> bool {
    return __builtin_memcmp(lhs.b.data(), rhs.b.data(), lhs.b.size()) == 0;
}

auto xfs_log_blocks_per_fs_block(const XfsMountContext* mount) -> uint32_t {
    if (mount == nullptr || mount->block_size < XLOG_HEADER_SIZE || mount->block_size % XLOG_HEADER_SIZE != 0) {
        return 0;
    }
    return mount->block_size / XLOG_HEADER_SIZE;
}

auto xfs_log_make_lsn(const XfsLog* log, uint32_t cycle, uint32_t fs_block) -> uint64_t {
    uint32_t const RATIO = xfs_log_blocks_per_fs_block(log != nullptr ? log->mount : nullptr);
    return (static_cast<uint64_t>(cycle) << 32) | (static_cast<uint64_t>(fs_block) * RATIO);
}

auto xfs_log_lsn_to_position(const XfsLog* log, uint64_t lsn, uint32_t* cycle, uint32_t* fs_block) -> int {
    uint32_t const RATIO = xfs_log_blocks_per_fs_block(log != nullptr ? log->mount : nullptr);
    uint32_t const BASIC_BLOCK = static_cast<uint32_t>(lsn);
    if (RATIO == 0 || cycle == nullptr || fs_block == nullptr || BASIC_BLOCK % RATIO != 0 || BASIC_BLOCK / RATIO >= log->log_blocks ||
        static_cast<uint32_t>(lsn >> 32) == 0) {
        return -EUCLEAN;
    }
    *cycle = static_cast<uint32_t>(lsn >> 32);
    *fs_block = BASIC_BLOCK / RATIO;
    return 0;
}

auto xfs_log_record_fs_blocks(const XfsMountContext* mount, size_t body_bytes, uint32_t* blocks_out) -> int {
    if (mount == nullptr || blocks_out == nullptr || mount->block_size < XLOG_HEADER_SIZE || body_bytes > SIZE_MAX - XLOG_HEADER_SIZE) {
        return -EINVAL;
    }
    size_t const RECORD_BYTES = XLOG_HEADER_SIZE + body_bytes;
    size_t const BLOCKS = (RECORD_BYTES + mount->block_size - 1) / mount->block_size;
    if (BLOCKS == 0 || BLOCKS > UINT32_MAX) {
        return -EOVERFLOW;
    }
    *blocks_out = static_cast<uint32_t>(BLOCKS);
    return 0;
}

auto xfs_log_serialize_body(const XfsTransItem* items, int item_count, uint64_t lsn, uint8_t* body, size_t body_bytes, uint32_t logops)
    -> int {
    int rc = wos_log_body_begin(body, body_bytes, 0, logops, lsn, body_bytes);
    if (rc != 0) {
        return rc;
    }
    size_t offset = sizeof(WosLogBodyHeader);
    uint32_t emitted = 0;
    for (int i = 0; i < item_count; ++i) {
        if (items[i].type != XfsLogItemType::BUFFER || !items[i].buf.dirty) {
            continue;
        }
        BufHead const* bp = items[i].buf.bp;
        if (bp == nullptr || bp->bdev == nullptr || bp->bdev->block_size == 0 || bp->size == 0 || bp->size % bp->bdev->block_size != 0 ||
            bp->size / bp->bdev->block_size > UINT32_MAX) {
            return -EIO;
        }
        rc = wos_log_body_append_buffer(body, body_bytes, &offset, bp->block_no, static_cast<uint32_t>(bp->size / bp->bdev->block_size),
                                        items[i].buf.offset, bp->data + items[i].buf.offset, items[i].buf.len);
        if (rc != 0) {
            return rc;
        }
        emitted++;
    }
    if (emitted != logops) {
        return -EUCLEAN;
    }
    return wos_log_body_finalize(body, body_bytes, offset);
}

void xfs_log_advance_head(XfsLog* log, uint32_t old_head, uint32_t new_head) {
    if (log == nullptr) {
        return;
    }

    log->tail_block = old_head;
    log->tail_cycle = log->head_cycle;
    log->previous_block = old_head * xfs_log_blocks_per_fs_block(log->mount);
    log->head_block = new_head;
    if (new_head < old_head) {
        log->head_cycle++;
    }
    log->clean = false;
}

enum class XfsLoadedLogKind : uint8_t {
    WOS_DIRTY,
    WOS_CLEAN,
    STANDARD_CLEAN,
    FOREIGN_DIRTY,
};

struct XfsLoadedLogRecord {
    XlogRecHeader header{};
    uint8_t* body{};
    size_t body_bytes{};
    uint64_t start_byte{};
    uint32_t record_blocks{};
    uint32_t next_cycle{};
    uint32_t next_block{};
    XfsLoadedLogKind kind{XfsLoadedLogKind::FOREIGN_DIRTY};
};

void xfs_log_release_record(XfsLoadedLogRecord& record) {
    delete[] record.body;
    record.body = nullptr;
    record.body_bytes = 0;
}

auto xfs_log_copy_bytes(XfsLog* log, uint64_t start_byte, void* destination, size_t bytes) -> int {
    if (log == nullptr || log->mount == nullptr || destination == nullptr || log->mount->block_size == 0 || log->log_blocks == 0 ||
        log->log_blocks > UINT64_MAX / log->mount->block_size) {
        return -EINVAL;
    }
    uint64_t const LOG_BYTES = static_cast<uint64_t>(log->log_blocks) * log->mount->block_size;
    if (start_byte >= LOG_BYTES || bytes > LOG_BYTES) {
        return -EUCLEAN;
    }
    auto* output = static_cast<uint8_t*>(destination);
    size_t copied = 0;
    while (copied < bytes) {
        uint64_t const POSITION = (start_byte + copied) % LOG_BYTES;
        uint32_t const FS_BLOCK = static_cast<uint32_t>(POSITION / log->mount->block_size);
        size_t const BLOCK_OFFSET = static_cast<size_t>(POSITION % log->mount->block_size);
        size_t const CHUNK = std::min(bytes - copied, static_cast<size_t>(log->mount->block_size) - BLOCK_OFFSET);
        BufHead* bh = xfs_buf_read(log->mount, log->log_start + FS_BLOCK);
        if (bh == nullptr || bh->data == nullptr || bh->size < log->mount->block_size) {
            brelse(bh);
            return -EIO;
        }
        __builtin_memcpy(output + copied, bh->data + BLOCK_OFFSET, CHUNK);
        brelse(bh);
        copied += CHUNK;
    }
    return 0;
}

auto xfs_log_standard_clean_unmount(const XfsLoadedLogRecord& record) -> bool {
    if (record.header.h_num_logops.to_cpu() != 1 || record.body_bytes != XLOG_HEADER_SIZE || record.body == nullptr) {
        return false;
    }
    Be32 packed_cycle{};
    Be32 operation_bytes{};
    Be16 operation_reserved{};
    __builtin_memcpy(&packed_cycle, record.body, sizeof(packed_cycle));
    __builtin_memcpy(&operation_bytes, record.body + 4, sizeof(operation_bytes));
    __builtin_memcpy(&operation_reserved, record.body + 10, sizeof(operation_reserved));
    uint16_t unmount_magic = 0;
    __builtin_memcpy(&unmount_magic, record.body + 12, sizeof(unmount_magic));
    // h_cycle_data[0] preserves the original operation transaction id while
    // the first payload word is overwritten with the cycle.  The id is
    // deliberately opaque (mkfs.xfs commonly uses 0xb0c0d0d0 and the kernel
    // uses a random ticket id), so it is not part of the clean predicate.
    uint32_t const OPERATION_BYTES = operation_bytes.to_cpu();
    return packed_cycle.to_cpu() == record.header.h_cycle.to_cpu() && (OPERATION_BYTES == 0 || OPERATION_BYTES == 8) &&
           record.body[8] == XFS_LOG && record.body[9] == XLOG_UNMOUNT_TRANS && operation_reserved.to_cpu() == 0 &&
           unmount_magic == XLOG_UNMOUNT_TYPE;
}

auto xfs_log_stale_header(const XfsLog* log, const XlogRecHeader& header, uint64_t start_byte) -> bool {
    if (log == nullptr || log->mount == nullptr || header.h_magicno.to_cpu() != XLOG_HEADER_MAGIC_NUM ||
        header.h_version.to_cpu() != XLOG_VERSION_2 || header.h_len.to_cpu() != 0 || header.h_crc != 0 ||
        header.h_prev_block.to_cpu() != 0 || header.h_num_logops.to_cpu() != 0 || header.h_fmt.to_cpu() != XLOG_FMT_LINUX_LE ||
        !xfs_uuid_equal(header.h_fs_uuid, log->mount->uuid) || header.h_size.to_cpu() != 0 || header.h_pad0 != 0 ||
        static_cast<uint32_t>(header.h_lsn.to_cpu() >> 32) != header.h_cycle.to_cpu() ||
        static_cast<uint32_t>(header.h_lsn.to_cpu()) != start_byte / XLOG_HEADER_SIZE) {
        return false;
    }
    uint64_t const LOG_BASIC_BLOCKS = static_cast<uint64_t>(log->log_blocks) * xfs_log_blocks_per_fs_block(log->mount);
    uint64_t const TAIL_LSN = header.h_tail_lsn.to_cpu();
    if (static_cast<uint32_t>(TAIL_LSN >> 32) == 0 || static_cast<uint32_t>(TAIL_LSN) >= LOG_BASIC_BLOCKS) {
        return false;
    }
    for (Be32 const& cycle_data : header.h_cycle_data) {
        if (cycle_data.to_cpu() != 0) {
            return false;
        }
    }
    for (uint8_t const byte : header.h_reserved) {
        if (byte != 0) {
            return false;
        }
    }
    return true;
}

auto xfs_log_load_record(XfsLog* log, uint64_t start_byte, XfsLoadedLogRecord* record) -> int {
    if (log == nullptr || log->mount == nullptr || record == nullptr || start_byte % XLOG_HEADER_SIZE != 0) {
        return -EINVAL;
    }
    *record = {};
    record->start_byte = start_byte;
    int rc = xfs_log_copy_bytes(log, start_byte, &record->header, sizeof(record->header));
    if (rc != 0) {
        return rc;
    }
    XlogRecHeader const& HEADER = record->header;
    uint32_t const RATIO = xfs_log_blocks_per_fs_block(log->mount);
    if (RATIO == 0 || HEADER.h_magicno.to_cpu() != XLOG_HEADER_MAGIC_NUM || HEADER.h_version.to_cpu() != XLOG_VERSION_2 ||
        HEADER.h_fmt.to_cpu() != XLOG_FMT_LINUX_LE || !xfs_uuid_equal(HEADER.h_fs_uuid, log->mount->uuid) || HEADER.h_cycle.to_cpu() == 0 ||
        static_cast<uint32_t>(HEADER.h_lsn.to_cpu() >> 32) != HEADER.h_cycle.to_cpu() ||
        static_cast<uint32_t>(HEADER.h_lsn.to_cpu()) != start_byte / XLOG_HEADER_SIZE) {
        return -EUCLEAN;
    }
    uint64_t const LOG_BASIC_BLOCKS = static_cast<uint64_t>(log->log_blocks) * RATIO;
    uint64_t const TAIL_LSN = HEADER.h_tail_lsn.to_cpu();
    if (static_cast<uint32_t>(TAIL_LSN >> 32) == 0 || static_cast<uint32_t>(TAIL_LSN) >= LOG_BASIC_BLOCKS) {
        return -EUCLEAN;
    }
    size_t const BODY_BYTES = HEADER.h_len.to_cpu();
    uint64_t const LOG_BYTES = static_cast<uint64_t>(log->log_blocks) * log->mount->block_size;
    if (BODY_BYTES == 0 || BODY_BYTES > XFS_LOG_MAX_BODY_BYTES || BODY_BYTES > LOG_BYTES - XLOG_HEADER_SIZE) {
        return -EUCLEAN;
    }
    record->body = new (std::nothrow) uint8_t[BODY_BYTES];
    if (record->body == nullptr) {
        return -ENOMEM;
    }
    record->body_bytes = BODY_BYTES;
    rc = xfs_log_copy_bytes(log, (start_byte + XLOG_HEADER_SIZE) % LOG_BYTES, record->body, BODY_BYTES);
    if (rc != 0) {
        xfs_log_release_record(*record);
        return rc;
    }

    Be32 body_magic{};
    if (BODY_BYTES >= sizeof(body_magic)) {
        __builtin_memcpy(&body_magic, record->body, sizeof(body_magic));
    }
    if (body_magic.to_cpu() == WOS_XLOG_BODY_MAGIC) {
        if (start_byte % log->mount->block_size != 0 || HEADER.h_crc == 0) {
            xfs_log_release_record(*record);
            return -EUCLEAN;
        }
        uint32_t record_blocks = 0;
        rc = xfs_log_record_fs_blocks(log->mount, BODY_BYTES, &record_blocks);
        if (rc != 0 || record_blocks >= log->log_blocks || HEADER.h_size.to_cpu() != record_blocks * log->mount->block_size ||
            wos_log_record_crc(HEADER, record->body, BODY_BYTES) != HEADER.h_crc) {
            xfs_log_release_record(*record);
            return -EUCLEAN;
        }
        WosLogBodyCursor cursor{};
        rc = wos_log_body_cursor_init(record->body, BODY_BYTES, HEADER.h_lsn.to_cpu(), &cursor);
        if (rc == 0 && cursor.item_count != HEADER.h_num_logops.to_cpu()) {
            rc = -EUCLEAN;
        }
        while (rc == 0 && cursor.emitted < cursor.item_count) {
            WosLogBufferItemView item{};
            rc = wos_log_body_cursor_next(&cursor, &item);
        }
        if (rc == 0) {
            rc = wos_log_body_cursor_finish(&cursor);
        }
        if (rc != 0) {
            xfs_log_release_record(*record);
            return rc;
        }
        record->kind = (cursor.flags & WOS_XLOG_BODY_FLAG_CLEAN) != 0 ? XfsLoadedLogKind::WOS_CLEAN : XfsLoadedLogKind::WOS_DIRTY;
        record->record_blocks = record_blocks;
    } else {
        if (HEADER.h_crc != 0 && wos_log_record_crc(HEADER, record->body, BODY_BYTES) != HEADER.h_crc) {
            xfs_log_release_record(*record);
            return -EUCLEAN;
        }
        record->kind = xfs_log_standard_clean_unmount(*record) ? XfsLoadedLogKind::STANDARD_CLEAN : XfsLoadedLogKind::FOREIGN_DIRTY;
        rc = xfs_log_record_fs_blocks(log->mount, BODY_BYTES, &record->record_blocks);
        if (rc != 0) {
            xfs_log_release_record(*record);
            return rc;
        }
    }

    uint32_t const START_BLOCK = static_cast<uint32_t>(start_byte / log->mount->block_size);
    uint64_t const UNWRAPPED_NEXT = static_cast<uint64_t>(START_BLOCK) + record->record_blocks;
    record->next_cycle = HEADER.h_cycle.to_cpu() + static_cast<uint32_t>(UNWRAPPED_NEXT / log->log_blocks);
    record->next_block = static_cast<uint32_t>(UNWRAPPED_NEXT % log->log_blocks);
    return 0;
}

// Scan every XFS basic-block boundary. A dirty/unknown latest record is never
// inferred clean, and an erased log is accepted only when every byte is zero.
// Linux may place exact empty headers in front of the head to invalidate stale
// writes; those markers are not records and are recognized byte-strictly.
auto xfs_log_find_head_tail(XfsLog* log) -> int {
    if (log == nullptr || log->mount == nullptr || log->log_blocks == 0 || xfs_log_blocks_per_fs_block(log->mount) == 0) {
        return -EINVAL;
    }
    bool any_nonzero = false;
    bool found = false;
    uint64_t latest_lsn = 0;
    uint32_t latest_start_block = UINT32_MAX;
    XfsLoadedLogKind latest_kind = XfsLoadedLogKind::FOREIGN_DIRTY;
    uint32_t latest_next_cycle = 1;
    uint32_t latest_next_block = 0;
    uint64_t latest_tail_lsn = 0;

    for (uint32_t fs_block = 0; fs_block < log->log_blocks; ++fs_block) {
        BufHead* bh = xfs_buf_read(log->mount, log->log_start + fs_block);
        if (bh == nullptr || bh->data == nullptr || bh->size < log->mount->block_size) {
            brelse(bh);
            return -EIO;
        }
        for (size_t i = 0; i < log->mount->block_size; ++i) {
            any_nonzero = any_nonzero || bh->data[i] != 0;
        }
        for (size_t offset = 0; offset < log->mount->block_size; offset += XLOG_HEADER_SIZE) {
            Be32 magic{};
            __builtin_memcpy(&magic, bh->data + offset, sizeof(magic));
            if (magic.to_cpu() != XLOG_HEADER_MAGIC_NUM) {
                continue;
            }
            uint64_t const START_BYTE = static_cast<uint64_t>(fs_block) * log->mount->block_size + offset;
            XlogRecHeader raw_header{};
            __builtin_memcpy(&raw_header, bh->data + offset, sizeof(raw_header));
            if (xfs_log_stale_header(log, raw_header, START_BYTE)) {
                continue;
            }
            brelse(bh);
            XfsLoadedLogRecord candidate{};
            int const RC = xfs_log_load_record(log, START_BYTE, &candidate);
            if (RC != 0) {
                mod::dbg::log("[xfs log] invalid record at byte %lu: %d", static_cast<unsigned long>(START_BYTE), RC);
                return RC;
            }
            uint64_t const LSN = candidate.header.h_lsn.to_cpu();
            if (!found || LSN > latest_lsn) {
                found = true;
                latest_lsn = LSN;
                latest_start_block = static_cast<uint32_t>(START_BYTE / XLOG_HEADER_SIZE);
                latest_kind = candidate.kind;
                latest_next_cycle = candidate.next_cycle;
                latest_next_block = candidate.next_block;
                latest_tail_lsn = candidate.header.h_tail_lsn.to_cpu();
            }
            xfs_log_release_record(candidate);
            bh = xfs_buf_read(log->mount, log->log_start + fs_block);
            if (bh == nullptr || bh->data == nullptr || bh->size < log->mount->block_size) {
                brelse(bh);
                return -EIO;
            }
        }
        brelse(bh);
    }

    if (!found) {
        if (any_nonzero) {
            return -EUCLEAN;
        }
        log->head_cycle = 1;
        log->head_block = 0;
        log->tail_cycle = 1;
        log->tail_block = 0;
        log->previous_block = UINT32_MAX;
        log->clean = true;
        return 0;
    }
    if (latest_kind == XfsLoadedLogKind::FOREIGN_DIRTY) {
        return -EOPNOTSUPP;
    }
    log->head_cycle = latest_next_cycle;
    log->head_block = latest_next_block;
    log->previous_block = latest_start_block;
    if (latest_kind == XfsLoadedLogKind::WOS_CLEAN || latest_kind == XfsLoadedLogKind::STANDARD_CLEAN) {
        log->tail_cycle = log->head_cycle;
        log->tail_block = log->head_block;
        log->clean = true;
        return 0;
    }
    int const TAIL_RC = xfs_log_lsn_to_position(log, latest_tail_lsn, &log->tail_cycle, &log->tail_block);
    if (TAIL_RC != 0) {
        return TAIL_RC;
    }
    log->clean = false;
    return 0;
}

auto xfs_log_device_span(const XfsLog* log, uint64_t* start, size_t* count) -> int {
    if (log == nullptr || log->mount == nullptr || start == nullptr || count == nullptr || log->mount->device == nullptr ||
        log->mount->device->block_size == 0 || log->mount->block_size == 0 ||
        log->mount->block_size % log->mount->device->block_size != 0 || log->mount->ag_blocks == 0 || log->mount->ag_blk_log >= 64) {
        return -EINVAL;
    }
    XfsMountContext const* mount = log->mount;
    auto const AGNO = static_cast<xfs_agnumber_t>(log->log_start >> mount->ag_blk_log);
    auto const AGBNO = static_cast<xfs_agblock_t>(log->log_start & ((uint64_t{1} << mount->ag_blk_log) - 1));
    if (AGBNO >= mount->ag_blocks || log->log_blocks > mount->ag_blocks - AGBNO ||
        static_cast<uint64_t>(AGNO) > UINT64_MAX / mount->ag_blocks) {
        return -EOVERFLOW;
    }
    uint64_t const AG_BASE = static_cast<uint64_t>(AGNO) * mount->ag_blocks;
    size_t const RATIO = mount->block_size / mount->device->block_size;
    if (AGBNO > UINT64_MAX - AG_BASE || AG_BASE + AGBNO > UINT64_MAX / RATIO || log->log_blocks > SIZE_MAX / RATIO) {
        return -EOVERFLOW;
    }
    *start = (AG_BASE + AGBNO) * RATIO;
    *count = static_cast<size_t>(log->log_blocks) * RATIO;
    return 0;
}

auto xfs_log_item_is_valid_target(const XfsLog* log, const WosLogBufferItemView& item) -> bool {
    if (log == nullptr || log->mount == nullptr || log->mount->device == nullptr || log->mount->device->block_size == 0 ||
        item.target_blocks == 0 || item.target_block > log->mount->device->total_blocks ||
        item.target_blocks > log->mount->device->total_blocks - item.target_block ||
        item.target_blocks > SIZE_MAX / log->mount->device->block_size) {
        return false;
    }
    size_t const TARGET_BYTES = static_cast<size_t>(item.target_blocks) * log->mount->device->block_size;
    if (item.data_offset > TARGET_BYTES || item.data_bytes > TARGET_BYTES - item.data_offset) {
        return false;
    }
    uint64_t log_start = 0;
    size_t log_count = 0;
    if (xfs_log_device_span(log, &log_start, &log_count) != 0 || log_count > UINT64_MAX - log_start) {
        return false;
    }
    uint64_t const ITEM_END = item.target_block + item.target_blocks;
    uint64_t const LOG_END = log_start + log_count;
    return ITEM_END <= log_start || item.target_block >= LOG_END;
}

auto xfs_log_process_record(XfsLog* log, XfsLoadedLogRecord& record, bool replay) -> int {
    if (record.kind != XfsLoadedLogKind::WOS_DIRTY) {
        return -EUCLEAN;
    }
    WosLogBodyCursor cursor{};
    int rc = wos_log_body_cursor_init(record.body, record.body_bytes, record.header.h_lsn.to_cpu(), &cursor);
    while (rc == 0 && cursor.emitted < cursor.item_count) {
        WosLogBufferItemView item{};
        rc = wos_log_body_cursor_next(&cursor, &item);
        if (rc != 0) {
            break;
        }
        if (!xfs_log_item_is_valid_target(log, item)) {
            rc = -EUCLEAN;
            break;
        }
        if (!replay) {
            continue;
        }
        BufHead* target = item.target_blocks == 1 ? bread(log->mount->device, item.target_block)
                                                  : bread_multi(log->mount->device, item.target_block, item.target_blocks);
        if (target == nullptr || target->data == nullptr ||
            target->size != static_cast<size_t>(item.target_blocks) * log->mount->device->block_size) {
            brelse(target);
            rc = -EIO;
            break;
        }
        __builtin_memcpy(target->data + item.data_offset, item.data, item.data_bytes);
        bdirty(target);
        rc = bwrite(target);
        brelse(target);
    }
    return rc != 0 ? rc : wos_log_body_cursor_finish(&cursor);
}

auto xfs_log_walk_recovery(XfsLog* log, bool replay) -> int {
    uint32_t cycle = log->tail_cycle;
    uint32_t block = log->tail_block;
    uint32_t records = 0;
    while (cycle != log->head_cycle || block != log->head_block) {
        XfsLoadedLogRecord record{};
        int rc = xfs_log_load_record(log, static_cast<uint64_t>(block) * log->mount->block_size, &record);
        if (rc != 0) {
            return rc;
        }
        if (record.header.h_lsn.to_cpu() != xfs_log_make_lsn(log, cycle, block)) {
            xfs_log_release_record(record);
            return -EUCLEAN;
        }
        rc = xfs_log_process_record(log, record, replay);
        if (rc == 0) {
            cycle = record.next_cycle;
            block = record.next_block;
        }
        xfs_log_release_record(record);
        if (rc != 0) {
            return rc;
        }
        records++;
        if (records > log->log_blocks) {
            return -EUCLEAN;
        }
    }
    return records == 0 ? -EUCLEAN : 0;
}

auto xfs_log_write_image(XfsLog* log, const uint8_t* image, uint32_t record_blocks) -> int {
    if (log == nullptr || log->mount == nullptr || image == nullptr || record_blocks == 0) {
        return -EINVAL;
    }
    uint64_t device_start = 0;
    size_t device_count = 0;
    int const SPAN_RC = xfs_log_device_span(log, &device_start, &device_count);
    if (SPAN_RC != 0) {
        return SPAN_RC;
    }
    size_t const RATIO = log->mount->block_size / log->mount->device->block_size;
    if (RATIO == 0 || record_blocks > SIZE_MAX / RATIO || static_cast<size_t>(record_blocks) * RATIO > device_count) {
        return -EOVERFLOW;
    }

    uint32_t remaining = record_blocks;
    uint32_t image_block = 0;
    uint32_t log_block = log->head_block;
    while (remaining != 0) {
        uint32_t const CHUNK_BLOCKS = std::min(remaining, log->log_blocks - log_block);
        size_t const DEVICE_BLOCKS = static_cast<size_t>(CHUNK_BLOCKS) * RATIO;
        uint64_t const DEVICE_BLOCK = device_start + static_cast<uint64_t>(log_block) * RATIO;
        int const RC = ker::dev::block_write(log->mount->device, DEVICE_BLOCK, DEVICE_BLOCKS,
                                             image + static_cast<size_t>(image_block) * log->mount->block_size);
        if (RC != 0) {
            return RC;
        }
        remaining -= CHUNK_BLOCKS;
        image_block += CHUNK_BLOCKS;
        log_block = 0;
    }
    return 0;
}

auto xfs_log_write_record(XfsLog* log, const uint8_t* body, size_t body_bytes, uint32_t logops, bool enforce_space,
                          bool standard_clean = false) -> int {
    if (log == nullptr || log->mount == nullptr || body == nullptr || body_bytes == 0 || body_bytes > UINT32_MAX) {
        return -EINVAL;
    }
    uint32_t record_blocks = 0;
    int rc = xfs_log_record_fs_blocks(log->mount, body_bytes, &record_blocks);
    if (rc != 0 || record_blocks >= log->log_blocks) {
        return rc != 0 ? rc : -ENOSPC;
    }
    uint32_t const HEAD = log->head_block;
    if (enforce_space) {
        uint32_t const AVAILABLE = HEAD >= log->tail_block ? log->log_blocks - HEAD + log->tail_block : log->tail_block - HEAD;
        if (record_blocks >= AVAILABLE) {
            return -ENOSPC;
        }
    }
    size_t const IMAGE_BYTES = static_cast<size_t>(record_blocks) * log->mount->block_size;
    auto* image = new (std::nothrow) uint8_t[IMAGE_BYTES];
    if (image == nullptr) {
        return -ENOMEM;
    }
    __builtin_memset(image, 0, IMAGE_BYTES);
    XlogRecHeader header{};
    uint64_t const LSN = xfs_log_make_lsn(log, log->head_cycle, HEAD);
    header.h_magicno = Be32::from_cpu(XLOG_HEADER_MAGIC_NUM);
    header.h_cycle = Be32::from_cpu(log->head_cycle);
    header.h_version = Be32::from_cpu(XLOG_VERSION_2);
    header.h_len = Be32::from_cpu(static_cast<uint32_t>(body_bytes));
    header.h_lsn = Be64::from_cpu(LSN);
    header.h_tail_lsn = Be64::from_cpu(xfs_log_make_lsn(log, log->tail_cycle, log->tail_block));
    header.h_prev_block = Be32::from_cpu(log->previous_block);
    header.h_num_logops = Be32::from_cpu(logops);
    header.h_fmt = Be32::from_cpu(XLOG_FMT_LINUX_LE);
    header.h_fs_uuid = log->mount->uuid;
    // Linux normally uses a 32KiB v2 iclog. The log head is still advanced by
    // the one header BB plus h_len data BBs; h_size describes the reusable
    // in-core buffer capacity, not the bytes written for this record.
    constexpr uint32_t STANDARD_ICLOG_BYTES = 32U * 1024U;
    header.h_size = Be32::from_cpu(standard_clean ? STANDARD_ICLOG_BYTES : static_cast<uint32_t>(IMAGE_BYTES));
    if (standard_clean) {
        header.h_cycle_data.at(0) = Be32{};
    }
    header.h_crc = wos_log_record_crc(header, body, body_bytes);
    __builtin_memcpy(image, &header, sizeof(header));
    __builtin_memcpy(image + XLOG_HEADER_SIZE, body, body_bytes);

    rc = xfs_log_write_image(log, image, record_blocks);
    delete[] image;
    if (rc != 0) {
        return rc;
    }
    uint32_t const NEXT = (HEAD + record_blocks) % log->log_blocks;
    xfs_log_advance_head(log, HEAD, NEXT);
    return 0;
}

auto xfs_log_erase_device(XfsLog* log) -> int {
    if (log == nullptr || log->mount == nullptr || log->mount->device == nullptr || log->mount->device->block_size == 0) {
        return -EINVAL;
    }
    uint64_t device_start = 0;
    size_t device_count = 0;
    int const SPAN_RC = xfs_log_device_span(log, &device_start, &device_count);
    if (SPAN_RC != 0) {
        return SPAN_RC;
    }

    constexpr size_t CLEAR_CHUNK_BYTES = size_t{64} * 1024U;
    size_t const DEVICE_BLOCK_SIZE = log->mount->device->block_size;
    size_t const CHUNK_BLOCKS = std::min(device_count, std::max<size_t>(1, CLEAR_CHUNK_BYTES / DEVICE_BLOCK_SIZE));
    if (CHUNK_BLOCKS == 0 || CHUNK_BLOCKS > SIZE_MAX / DEVICE_BLOCK_SIZE) {
        return -EOVERFLOW;
    }
    auto* zeroes = new (std::nothrow) uint8_t[CHUNK_BLOCKS * DEVICE_BLOCK_SIZE];
    if (zeroes == nullptr) {
        return -ENOMEM;
    }
    __builtin_memset(zeroes, 0, CHUNK_BLOCKS * DEVICE_BLOCK_SIZE);

    int rc = 0;
    for (size_t offset = 0; offset < device_count; offset += CHUNK_BLOCKS) {
        size_t const BLOCKS = std::min(CHUNK_BLOCKS, device_count - offset);
        rc = ker::dev::block_write(log->mount->device, device_start + offset, BLOCKS, zeroes);
        if (rc != 0) {
            break;
        }
    }
    delete[] zeroes;
    return rc;
}

auto xfs_log_clear_clean(XfsMountContext* mount, XfsLog* log) -> int {
    if (mount == nullptr || log == nullptr || log->mount != mount) {
        return -EINVAL;
    }
    // WOS crash-recovery records deliberately use a private body encoding.
    // Once every covered home image is durable, retire that private history
    // before publishing the standard XFS unmount operation.  This leaves a
    // byte-canonical clean log that Linux can inspect without attempting to
    // interpret private records as multi-header v2 iclogs.
    int rc = xfs_log_erase_device(log);
    if (rc == 0) {
        rc = flush_blockdev(mount->device);
    }
    if (rc != 0) {
        log->clean = false;
        return rc;
    }
    log->head_block = 0;
    log->tail_block = 0;
    log->tail_cycle = log->head_cycle;
    log->previous_block = UINT32_MAX;

    std::array<uint8_t, XLOG_HEADER_SIZE> body{};
    // Standard v2 XFS unmount operation. xlog_pack_data replaces the first
    // payload word (oh_tid) with the BE cycle and stores the original word in
    // h_cycle_data[0]. We use an all-zero transaction id, so the saved word is
    // already represented by the zeroed outer header.
    Be32 const PACKED_CYCLE = Be32::from_cpu(log->head_cycle);
    Be32 const PAYLOAD_BYTES = Be32::from_cpu(8);
    __builtin_memcpy(body.data(), &PACKED_CYCLE, sizeof(PACKED_CYCLE));
    __builtin_memcpy(body.data() + 4, &PAYLOAD_BYTES, sizeof(PAYLOAD_BYTES));
    body.at(8) = XFS_LOG;
    body.at(9) = XLOG_UNMOUNT_TRANS;
    uint16_t const UNMOUNT_MAGIC = XLOG_UNMOUNT_TYPE;
    __builtin_memcpy(body.data() + 12, &UNMOUNT_MAGIC, sizeof(UNMOUNT_MAGIC));
    rc = xfs_log_write_record(log, body.data(), body.size(), 1, false, true);
    if (rc == 0) {
        rc = flush_blockdev(mount->device);
    }
    if (rc != 0) {
        log->clean = false;
        return rc;
    }
    log->tail_cycle = log->head_cycle;
    log->tail_block = log->head_block;
    log->clean = true;
    return 0;
}

// Validate the complete active chain before touching home metadata, then replay
// the exact same bounded chain.  Raw post-images are inherently idempotent.
auto xfs_log_recover(XfsLog* log) -> int {
    mod::dbg::log("[xfs log recover] starting recovery: tail=%u.%u head=%u.%u", log->tail_cycle, log->tail_block, log->head_cycle,
                  log->head_block);
    int rc = xfs_log_walk_recovery(log, false);
    if (rc == 0) {
        rc = xfs_log_walk_recovery(log, true);
    }
    if (rc == 0) {
        rc = flush_blockdev(log->mount->device);
    }
    if (rc == 0) {
        rc = xfs_log_clear_clean(log->mount, log);
    }
    if (rc != 0) {
        log->clean = false;
        mod::dbg::log("[xfs log recover] recovery failed closed: %d", rc);
        return rc;
    }
    mod::dbg::log("[xfs log recover] recovery complete");
    return 0;
}

// Drop mount-owned journal state without initiating any I/O.  A batch retains
// one buffer reference per item, plus a journal hold until HOME_RELEASED.
void xfs_log_abandon_locked(XfsMountContext* mount) {
    if (mount == nullptr) {
        return;
    }
    XfsLogBatch* batch = mount->log_batch;
    if (batch != nullptr) {
        bool const HAS_JOURNAL_HOLDS = batch->phase != XfsLogBatchPhase::HOME_RELEASED;
        for (size_t i = 0; i < batch->item_count; ++i) {
            XfsTransItem& item = batch->items.at(i);
            if (item.type != XfsLogItemType::BUFFER || item.buf.bp == nullptr) {
                continue;
            }
            if (HAS_JOURNAL_HOLDS) {
                bjournal_release(item.buf.bp);
            }
            brelse(item.buf.bp);
            item.buf.bp = nullptr;
            item.type = XfsLogItemType::NONE;
        }
        delete batch;
        mount->log_batch = nullptr;
    }
    if (mount->log != nullptr) {
        mount->log->active = false;
        delete mount->log;
        mount->log = nullptr;
    }
}

auto xfs_log_mark_clean_locked(XfsMountContext* mount) -> int {
    if (mount == nullptr || mount->log == nullptr || mount->log->mount != mount || mount->log_batch == nullptr ||
        mount->log_batch->mount != mount || !mount->log->active) {
        return -ENODEV;
    }
    if (mount->log_batch->item_count != 0 || mount->log_batch->phase != XfsLogBatchPhase::COLLECTING) {
        return -EBUSY;
    }
    // Even an already-clean log needs the caller-visible final durability
    // barrier.  For a dirty log, xfs_log_clear_clean includes that barrier.
    return mount->log->clean ? flush_blockdev(mount->device) : xfs_log_clear_clean(mount, mount->log);
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
    XfsLogWriteGuard guard(mount);
    if (mount->log != nullptr || mount->log_batch != nullptr) {
        return -EBUSY;
    }

    auto* log = new XfsLog{};
    log->mount = mount;
    log->log_start = mount->log_start;
    log->log_blocks = mount->log_blocks;
    log->sect_size = mount->sect_size;
    log->clean = true;
    log->active = false;
    log->previous_block = UINT32_MAX;

    int const RC = xfs_log_find_head_tail(log);
    if (RC != 0) {
        delete log;
        return RC;
    }

    if (!log->clean) {
        if (mount->read_only) {
            mod::dbg::log("[xfs log] rejecting dirty read-only mount");
            delete log;
            return -EROFS;
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
    mount->log = log;
    mount->log_batch = batch;
    return 0;
}

auto xfs_log_unmount(XfsMountContext* mount, bool home_metadata_clean) -> int {
    if (mount == nullptr) {
        return -EINVAL;
    }
    if (home_metadata_clean) {
        int const FLUSH_RC = xfs_log_flush(mount);
        if (FLUSH_RC != 0) {
            return FLUSH_RC;
        }
    }

    XfsLogWriteGuard guard(mount);
    if (mount->log == nullptr || mount->log->mount != mount || mount->log_batch == nullptr || mount->log_batch->mount != mount) {
        return !mount->mounted ? 0 : -ENODEV;
    }
    if (home_metadata_clean) {
        int const CLEAN_RC = xfs_log_mark_clean_locked(mount);
        if (CLEAN_RC != 0) {
            // Preserve all state so the caller can retry the failed sync or
            // clean-marker publication.  Never erase a dirty log on error.
            return CLEAN_RC;
        }
    }
    xfs_log_abandon_locked(mount);
    mod::dbg::log("[xfs log] log unmounted");
    return 0;
}

auto xfs_log_mark_clean(XfsMountContext* mount) -> int {
    XfsLogWriteGuard guard(mount);
    return xfs_log_mark_clean_locked(mount);
}

auto xfs_log_needs_recovery(XfsMountContext* mount) -> bool {
    XfsLogWriteGuard guard(mount);
    if (mount == nullptr || mount->log == nullptr || mount->log->mount != mount) {
        return false;
    }
    return !mount->log->clean || (mount->log_batch != nullptr && mount->log_batch->item_count != 0);
}

namespace {

auto xfs_log_write_record_locked(XfsMountContext* mount, const XfsTransItem* items, int item_count) -> int {
    uint64_t const PERF_STARTED_US =
        ker::mod::perf::is_wki_scope_recording_enabled(ker::mod::perf::WkiPerfScope::LOCAL_XFS,
                                                       static_cast<uint8_t>(ker::mod::perf::WkiPerfLocalXfsOp::LOG_WRITE))
            ? ker::mod::time::get_us()
            : 0;
    if (mount == nullptr || mount->log == nullptr || mount->log->mount != mount) {
        return -EINVAL;
    }
    if (!mount->log->active) {
        return -EINVAL;
    }
    XfsLog* log = mount->log;
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

    // INODE transaction items are commit-time inputs only: xfs_inode_write()
    // logs their concrete inode buffers before this point, and recovery
    // replays those buffer records.
    size_t body_size = sizeof(WosLogBodyHeader);
    uint32_t num_logops = 0;
    for (int i = 0; i < item_count; i++) {
        if (items[i].type != XfsLogItemType::BUFFER || !items[i].buf.dirty) {
            continue;
        }
        BufHead const* bp = items[i].buf.bp;
        uint32_t const OFFSET = items[i].buf.offset;
        uint32_t const LEN = items[i].buf.len;
        if (bp == nullptr || OFFSET > bp->size || LEN > bp->size - OFFSET || body_size > SIZE_MAX - sizeof(WosLogBufferItemHeader) - LEN) {
            return finish_log_write(-EIO);
        }
        body_size += sizeof(WosLogBufferItemHeader) + LEN;
        ++num_logops;
    }

    if (num_logops == 0) {
        return finish_log_write(0);  // nothing to log
    }
    if (body_size > XFS_LOG_MAX_BODY_BYTES || body_size > UINT32_MAX) {
        return finish_log_write(-E2BIG);
    }
    perf_body_bytes = body_size;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init): serialized before any byte is read.
    std::array<uint8_t, XFS_LOG_STACK_BODY_MAX_BYTES> stack_body_buf;
    auto* body_buf = stack_body_buf.data();
    bool heap_body_buf = false;
    if (body_size > stack_body_buf.size()) {
        body_buf = new (std::nothrow) uint8_t[body_size];
        heap_body_buf = true;
    }
    if (body_buf == nullptr) {
        return finish_log_write(-ENOMEM);
    }

    uint64_t const LSN = xfs_log_make_lsn(log, log->head_cycle, log->head_block);
    int rc = xfs_log_serialize_body(items, item_count, LSN, body_buf, body_size, num_logops);
    if (rc == 0) {
        rc = xfs_log_write_record(log, body_buf, body_size, num_logops, true);
    }

    if (heap_body_buf) {
        delete[] body_buf;
    }
    if (rc == 0) {
        uint32_t record_blocks = 0;
        if (xfs_log_record_fs_blocks(mount, body_size, &record_blocks) == 0) {
            perf_block_bytes = static_cast<uint64_t>(record_blocks) * mount->block_size;
        }
    }
    return finish_log_write(rc);
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
            body_bytes > SIZE_MAX - sizeof(WosLogBufferItemHeader) - LEN) {
            return -EIO;
        }
        dirty_items++;
        body_bytes += sizeof(WosLogBufferItemHeader) + LEN;
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
    batch->phase = XfsLogBatchPhase::COLLECTING;
}

auto xfs_log_writeback_home_item(const XfsTransItem& item) -> int {
    if (item.type != XfsLogItemType::BUFFER || item.buf.bp == nullptr) {
        return 0;
    }
    BufHead* bp = item.buf.bp;
    if (bp->bdev == nullptr || bp->bdev->block_size == 0 || bp->size == 0 || bp->size % bp->bdev->block_size != 0) {
        return -EIO;
    }

    // The batch already pins the exact home buffer, so write it directly
    // instead of searching the dirty range tree again for every item. A
    // retired buffer names storage that may have been recycled since the
    // transaction committed; retain range lookup for that case so the live
    // replacement, rather than the stale retired image, reaches disk.
    if (!bp->retired.load(std::memory_order_acquire)) {
        return bwrite(bp);
    }
    return writeback_bdev_range(bp->bdev, bp->block_no, bp->size / bp->bdev->block_size);
}

auto xfs_log_batch_flush_locked(XfsMountContext* mount) -> int {
    XfsLogBatch* batch = mount != nullptr ? mount->log_batch : nullptr;
    if (batch == nullptr || batch->mount != mount) {
        return -EINVAL;
    }
    if (batch->item_count == 0) {
        xfs_log_batch_reset(batch);
        return 0;
    }

    if (batch->phase == XfsLogBatchPhase::COLLECTING) {
        // Persist file data while journal holds still exclude every home
        // metadata buffer from writeback.  This preserves ordered-data
        // semantics before the WAL is made durable.
        int const DATA_RC = sync_blockdev_file_data(mount->device);
        if (DATA_RC != 0) {
            return DATA_RC;
        }
        int const RC = xfs_log_write_record_locked(mount, batch->items.data(), static_cast<int>(batch->item_count));
        if (RC != 0) {
            // Keep the batch and its journal holds intact. Metadata must not
            // become writeback-eligible until a WAL retry succeeds.
            return RC;
        }
        batch->phase = XfsLogBatchPhase::LOG_STAGED;
    }

    if (batch->phase == XfsLogBatchPhase::LOG_STAGED) {
        int const FLUSH_RC = flush_blockdev(mount->device);
        if (FLUSH_RC != 0) {
            // LOG_STAGED avoids appending a duplicate record on retry. An
            // unconditional device flush makes a retry valid even when the
            // earlier write completed but its cache flush failed.
            return FLUSH_RC;
        }
        batch->phase = XfsLogBatchPhase::LOG_DURABLE;
    }

    if (batch->phase == XfsLogBatchPhase::LOG_DURABLE) {
        // The WAL is stable. Release every home-buffer hold before selecting
        // any range for writeback so overlapping aliases cannot deadlock one
        // another during the checkpoint.
        for (size_t i = 0; i < batch->item_count; ++i) {
            XfsTransItem& item = batch->items.at(i);
            if (item.type != XfsLogItemType::BUFFER || item.buf.bp == nullptr) {
                continue;
            }
            bdirty(item.buf.bp);
            bjournal_release(item.buf.bp);
        }
        batch->phase = XfsLogBatchPhase::HOME_RELEASED;
    }

    for (size_t i = 0; i < batch->item_count; ++i) {
        int const RC = xfs_log_writeback_home_item(batch->items.at(i));
        if (RC != 0) {
            // Do not submit or flush later home images after an earlier
            // checkpoint write failed. The durable WAL remains authoritative,
            // and a retry restarts the retained HOME_RELEASED batch.
            return RC;
        }
    }
    int const FLUSH_RC = flush_blockdev(mount->device);
    if (FLUSH_RC != 0) {
        // Keep references to every checkpoint range. Successful writes are
        // already clean; failed ranges remain dirty and are retried before a
        // later transaction may advance the compact log tail.
        return FLUSH_RC;
    }

    // Every home image covered by the retained WAL is now durable.  Future
    // records may reuse that ring space; their on-disk tail LSN will still
    // point at their own start so a crash before the next home checkpoint
    // replays exactly the newest outstanding batch.
    mount->log->tail_cycle = mount->log->head_cycle;
    mount->log->tail_block = mount->log->head_block;

    for (size_t i = 0; i < batch->item_count; ++i) {
        XfsTransItem& item = batch->items.at(i);
        if (item.type == XfsLogItemType::BUFFER && item.buf.bp != nullptr) {
            brelse(item.buf.bp);
            item.buf.bp = nullptr;
            item.type = XfsLogItemType::NONE;
        }
    }
    xfs_log_batch_reset(batch);
    return 0;
}

auto xfs_log_batch_add_locked(XfsMountContext* mount, const XfsTransItem* items, int item_count) -> int {
    XfsLogBatch* batch = mount != nullptr ? mount->log_batch : nullptr;
    if (batch == nullptr || batch->phase != XfsLogBatchPhase::COLLECTING || items == nullptr || item_count < 0) {
        return -EINVAL;
    }

    // Validate the complete input before acquiring any reference or journal
    // hold.  After this preflight, the loop below has no failure path, so the
    // transaction either transfers all metadata to the batch or none of it.
    size_t dirty_items = 0;
    size_t ignored_body_bytes = 0;
    int const SHAPE_RC = xfs_log_transaction_shape(items, item_count, &dirty_items, &ignored_body_bytes);
    if (SHAPE_RC != 0) {
        return SHAPE_RC;
    }
    if (dirty_items > batch->items.size() - batch->item_count) {
        return -E2BIG;
    }
    size_t worst_case_growth = 0;
    for (int i = 0; i < item_count; ++i) {
        if (items[i].type != XfsLogItemType::BUFFER || !items[i].buf.dirty) {
            continue;
        }
        BufHead const* bp = items[i].buf.bp;
        if (bp->size > SIZE_MAX - sizeof(WosLogBufferItemHeader) ||
            worst_case_growth > SIZE_MAX - sizeof(WosLogBufferItemHeader) - bp->size) {
            return -E2BIG;
        }
        worst_case_growth += sizeof(WosLogBufferItemHeader) + bp->size;
    }
    if (batch->body_bytes > XFS_LOG_BATCH_MAX_BODY_BYTES || worst_case_growth > XFS_LOG_BATCH_MAX_BODY_BYTES - batch->body_bytes) {
        return -E2BIG;
    }

    for (int i = 0; i < item_count; ++i) {
        if (items[i].type != XfsLogItemType::BUFFER || !items[i].buf.dirty || items[i].buf.bp == nullptr) {
            continue;
        }

        BufHead* bp = items[i].buf.bp;
        uint32_t const OFFSET = items[i].buf.offset;
        uint32_t const LEN = items[i].buf.len;
        if (XfsTransItem* existing = xfs_log_batch_find_item(batch, bp)) {
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
            batch->body_bytes += existing->buf.len - OLD_LEN;
            continue;
        }

        bp->refcount.fetch_add(1, std::memory_order_relaxed);
        bjournal_hold(bp);
        XfsTransItem& dst = batch->items.at(batch->item_count++);
        dst.type = XfsLogItemType::BUFFER;
        dst.buf = items[i].buf;
        batch->body_bytes += sizeof(WosLogBufferItemHeader) + LEN;
    }
    return 0;
}

auto xfs_log_batch_should_checkpoint(const XfsLogBatch* batch) -> bool {
    if (batch == nullptr || batch->phase != XfsLogBatchPhase::COLLECTING || batch->item_count == 0) {
        return batch != nullptr && batch->phase != XfsLogBatchPhase::COLLECTING;
    }
    constexpr size_t BODY_RESERVE = XFS_LOG_BATCH_MAX_BODY_BYTES / 2;
    return batch->body_bytes >= BODY_RESERVE || batch->item_count + XFS_TRANS_MAX_ITEMS > batch->items.size();
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

    XfsLogWriteGuard guard(mount);
    XfsLogBatch* batch = mount != nullptr ? mount->log_batch : nullptr;
    if (mount == nullptr || mount->log == nullptr || mount->log->mount != mount || !mount->log->active || batch == nullptr ||
        batch->mount != mount) {
        return -ENODEV;
    }

    if (batch->phase != XfsLogBatchPhase::COLLECTING) {
        // xfs_log_prepare_transaction() resolves pending checkpoints before
        // transaction mutation. Do not attempt one here: the current
        // transaction still owns undo holds on its metadata buffers.
        return -EAGAIN;
    }

    if (dirty_items > batch->items.size() - batch->item_count) {
        return -E2BIG;
    }
    if (batch->body_bytes > XFS_LOG_BATCH_MAX_BODY_BYTES || transaction_body_bytes > XFS_LOG_BATCH_MAX_BODY_BYTES - batch->body_bytes) {
        return -E2BIG;
    }
    int const ADD_RC = xfs_log_batch_add_locked(mount, items, item_count);
    if (ADD_RC != 0) {
        return ADD_RC;
    }
    if (owns_metadata_out != nullptr) {
        *owns_metadata_out = true;
    }
    return 0;
}

auto xfs_log_prepare_transaction(XfsMountContext* mount) -> int {
    XfsLogWriteGuard guard(mount);
    if (mount == nullptr) {
        return -EINVAL;
    }
    if (mount->log == nullptr || mount->log->mount != mount || mount->log_batch == nullptr || mount->log_batch->mount != mount) {
        return mount->mounted ? -ENODEV : 0;
    }
    return xfs_log_batch_should_checkpoint(mount->log_batch) ? xfs_log_batch_flush_locked(mount) : 0;
}

auto xfs_log_checkpoint_if_needed(XfsMountContext* mount) -> int {
    // Post-commit checkpointing can publish allocation metadata before the
    // caller copies newly allocated file data.  The next reservation or an
    // explicit sync performs the ordered data-sync/checkpoint instead.
    return mount == nullptr ? -EINVAL : 0;
}

auto xfs_log_flush(XfsMountContext* mount) -> int {
    XfsLogWriteGuard guard(mount);
    if (mount == nullptr || mount->log == nullptr || mount->log->mount != mount || mount->log_batch == nullptr ||
        mount->log_batch->mount != mount) {
        return -EINVAL;
    }
    return xfs_log_batch_flush_locked(mount);
}

#ifdef WOS_SELFTEST
auto xfs_selftest_log_crash_forget(XfsMountContext* mount) -> bool {
    XfsLogWriteGuard guard(mount);
    if (mount == nullptr || mount->log == nullptr || mount->log->mount != mount || mount->log_batch == nullptr ||
        mount->log_batch->mount != mount) {
        return false;
    }
    // Model sudden power loss: abandon volatile journal ownership without
    // checkpoint, clean-marker publication, writeback, or device flush.
    xfs_log_abandon_locked(mount);
    return true;
}

namespace {

auto xfs_log_recycle_selftest_read(ker::dev::BlockDevice* dev, uint64_t /*block*/, size_t count, void* buffer) -> int {
    __builtin_memset(buffer, 0, count * dev->block_size);
    return 0;
}

auto xfs_log_recycle_selftest_write(ker::dev::BlockDevice* /*dev*/, uint64_t /*block*/, size_t /*count*/, const void* /*buffer*/) -> int {
    return 0;
}

enum class XfsLogCheckpointSelftestEvent : uint8_t {
    WRITE,
    FLUSH,
};

struct XfsLogCheckpointSelftestState {
    std::array<XfsLogCheckpointSelftestEvent, 16> events{};
    std::array<uint64_t, 16> blocks{};
    size_t event_count{};
};

auto xfs_log_checkpoint_selftest_write(ker::dev::BlockDevice* dev, uint64_t block, size_t /*count*/, const void* /*buffer*/) -> int {
    auto* state = static_cast<XfsLogCheckpointSelftestState*>(dev->private_data);
    if (state == nullptr || state->event_count >= state->events.size()) {
        return -EIO;
    }
    size_t const INDEX = state->event_count++;
    state->events.at(INDEX) = XfsLogCheckpointSelftestEvent::WRITE;
    state->blocks.at(INDEX) = block;
    return 0;
}

auto xfs_log_checkpoint_selftest_flush(ker::dev::BlockDevice* dev) -> int {
    auto* state = static_cast<XfsLogCheckpointSelftestState*>(dev->private_data);
    if (state == nullptr || state->event_count >= state->events.size()) {
        return -EIO;
    }
    state->events.at(state->event_count++) = XfsLogCheckpointSelftestEvent::FLUSH;
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
             body_bytes == sizeof(WosLogBufferItemHeader) + replacement->size;
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

auto xfs_selftest_log_checkpoint_is_ordered_and_bounded() -> bool {
    constexpr uint64_t HOME_BLOCK = 10;
    constexpr uint64_t UNRELATED_BLOCK = 20;
    constexpr uint64_t DATA_BLOCK = 30;
    constexpr xfs_fsblock_t LOG_START = 128;
    constexpr uint32_t LOG_BLOCKS = 64;

    XfsLogCheckpointSelftestState state{};
    ker::dev::BlockDevice dev{};
    dev.block_size = 512;
    dev.total_blocks = 1024;
    dev.read_blocks = xfs_log_recycle_selftest_read;
    dev.write_blocks = xfs_log_checkpoint_selftest_write;
    dev.flush = xfs_log_checkpoint_selftest_flush;
    dev.private_data = &state;
    invalidate_bdev(&dev);

    XfsMountContext mount{};
    mount.device = &dev;
    mount.block_size = 512;
    mount.block_log = 9;
    mount.ag_blocks = 1024;
    mount.ag_blk_log = 10;
    mount.log_start = LOG_START;
    mount.log_blocks = LOG_BLOCKS;
    mount.sect_size = 512;

    bool ok = xfs_log_mount(&mount) == 0;
    BufHead* home = ok ? bget(&dev, HOME_BLOCK) : nullptr;
    BufHead* unrelated = ok ? bget(&dev, UNRELATED_BLOCK) : nullptr;
    BufHead* data = ok ? bget(&dev, DATA_BLOCK, BufferReadClass::FILE_DATA) : nullptr;
    ok = ok && home != nullptr && unrelated != nullptr && data != nullptr;

    if (ok) {
        home->data[0] = 0xA5;
        unrelated->data[0] = 0x5A;
        data->data[0] = 0x3C;
        bdirty(unrelated);
        bdirty(data);

        XfsTransItem item{};
        item.type = XfsLogItemType::BUFFER;
        item.buf = {
            .bp = home,
            .offset = 0,
            .len = static_cast<uint32_t>(home->size),
            .dirty = true,
        };
        bool owns_metadata = false;
        ok = xfs_log_write(&mount, &item, 1, &owns_metadata) == 0 && owns_metadata;
        brelse(home);
        home = nullptr;

        int const FLUSH_RC = xfs_log_flush(&mount);
        bool const HOME_DIRTY = has_dirty_bdev_range(&dev, HOME_BLOCK, 1);
        bool const UNRELATED_DIRTY = has_dirty_bdev_range(&dev, UNRELATED_BLOCK, 1);
        bool const DATA_DIRTY = has_dirty_bdev_range(&dev, DATA_BLOCK, 1);
        ok = ok && FLUSH_RC == 0 && !HOME_DIRTY && UNRELATED_DIRTY && !DATA_DIRTY;

        size_t data_write_index = SIZE_MAX;
        size_t data_flush_index = SIZE_MAX;
        size_t log_write_index = SIZE_MAX;
        size_t first_flush_index = SIZE_MAX;
        size_t home_write_index = SIZE_MAX;
        size_t second_flush_index = SIZE_MAX;
        for (size_t i = 0; i < state.event_count; ++i) {
            if (state.events.at(i) == XfsLogCheckpointSelftestEvent::WRITE && state.blocks.at(i) == DATA_BLOCK &&
                data_write_index == SIZE_MAX) {
                data_write_index = i;
            } else if (state.events.at(i) == XfsLogCheckpointSelftestEvent::FLUSH && data_write_index != SIZE_MAX &&
                       data_flush_index == SIZE_MAX) {
                data_flush_index = i;
            } else if (state.events.at(i) == XfsLogCheckpointSelftestEvent::WRITE && state.blocks.at(i) >= LOG_START &&
                       state.blocks.at(i) < LOG_START + LOG_BLOCKS && log_write_index == SIZE_MAX) {
                log_write_index = i;
            } else if (state.events.at(i) == XfsLogCheckpointSelftestEvent::FLUSH && log_write_index != SIZE_MAX &&
                       first_flush_index == SIZE_MAX) {
                first_flush_index = i;
            } else if (state.events.at(i) == XfsLogCheckpointSelftestEvent::WRITE && state.blocks.at(i) == HOME_BLOCK &&
                       first_flush_index != SIZE_MAX && home_write_index == SIZE_MAX) {
                home_write_index = i;
            } else if (state.events.at(i) == XfsLogCheckpointSelftestEvent::FLUSH && home_write_index != SIZE_MAX) {
                second_flush_index = i;
                break;
            }
        }
        ok = ok && data_write_index < data_flush_index && data_flush_index < log_write_index && log_write_index < first_flush_index &&
             first_flush_index < home_write_index && home_write_index < second_flush_index;
        if (!ok) {
            mod::dbg::log(
                "[xfs log selftest] flush=%d home_dirty=%u unrelated_dirty=%u data_dirty=%u events=%lu data_write=%lu "
                "data_flush=%lu log_write=%lu flush1=%lu home_write=%lu flush2=%lu",
                FLUSH_RC, static_cast<unsigned>(HOME_DIRTY), static_cast<unsigned>(UNRELATED_DIRTY), static_cast<unsigned>(DATA_DIRTY),
                static_cast<unsigned long>(state.event_count), static_cast<unsigned long>(data_write_index),
                static_cast<unsigned long>(data_flush_index), static_cast<unsigned long>(log_write_index),
                static_cast<unsigned long>(first_flush_index), static_cast<unsigned long>(home_write_index),
                static_cast<unsigned long>(second_flush_index));
            for (size_t i = 0; i < state.event_count; ++i) {
                mod::dbg::log("[xfs log selftest] event[%lu]=%s block=%lu", static_cast<unsigned long>(i),
                              state.events.at(i) == XfsLogCheckpointSelftestEvent::WRITE ? "write" : "flush",
                              static_cast<unsigned long>(state.blocks.at(i)));
            }
        }
    }

    brelse(home);
    brelse(unrelated);
    brelse(data);
    if (mount.log != nullptr && mount.log->mount == &mount) {
        xfs_log_unmount(&mount, false);
    }
    invalidate_bdev(&dev);
    return ok;
}
#endif

}  // namespace ker::vfs::xfs
