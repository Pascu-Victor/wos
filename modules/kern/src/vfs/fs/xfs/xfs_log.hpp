#pragma once

// XFS Log (Write-Ahead Log) - journal management.
//
// The XFS log is a circular buffer on disk used to ensure metadata consistency.
// On mount, the log is scanned for uncommitted records and replayed.
//
// Reference: reference/xfs/xfs_log.h, reference/xfs/xfs_log_priv.h,
//            reference/xfs/xfs_log_recover.c

#include <cstdint>
#include <vfs/fs/xfs/xfs_format.hpp>
#include <vfs/fs/xfs/xfs_mount.hpp>

namespace ker::vfs::xfs {

// Log state
struct XfsLog {
    XfsMountContext* mount;

    // Circular log geometry
    xfs_fsblock_t log_start;  // first block of the log
    uint32_t log_blocks;      // number of log blocks
    uint32_t sect_size;       // log sector size

    // Current position
    uint32_t head_cycle;  // current write cycle
    uint32_t head_block;  // current write position (block)
    uint32_t tail_cycle;  // oldest active record cycle
    uint32_t tail_block;  // oldest active record block

    // State
    bool clean;   // true if log is clean (no recovery needed)
    bool active;  // true if log writer is running
};

// Initialize and recover the log (call during mount, after superblock is read).
// Returns 0 on success, negative errno on failure.
auto xfs_log_mount(XfsMountContext* mount) -> int;

// Shut down the log (call during unmount).
// Tear down the active log. When home_metadata_clean is true, discard the
// compact WOS journal after all home metadata has reached disk.
void xfs_log_unmount(XfsMountContext* mount, bool home_metadata_clean);

// Check if the log needs recovery (contains uncommitted records).
auto xfs_log_needs_recovery(XfsMountContext* mount) -> bool;

// Write a log record to the journal for a set of buffer modifications.
// Each entry describes a modified buffer region.  The log record is written
// synchronously before the buffers are flushed (write-ahead guarantee).
// Returns 0 on success, negative errno on failure.
struct XfsTransItem;
auto xfs_log_write(XfsMountContext* mount, const XfsTransItem* items, int item_count, bool* owns_metadata_out = nullptr) -> int;

// Before a transaction mutates metadata, make sure a previous failed
// checkpoint is resolved and the current batch has room for a worst-case
// transaction.
auto xfs_log_prepare_transaction(XfsMountContext* mount) -> int;

// After a committed transaction releases its undo holds, checkpoint a batch
// that has reached its bounded item/body capacity threshold.
auto xfs_log_checkpoint_if_needed(XfsMountContext* mount) -> int;

// Commit the current metadata group with ordered durability: persist the WAL,
// then checkpoint and persist only the group's home metadata before allowing
// a later transaction to advance the compact log tail.
auto xfs_log_flush(XfsMountContext* mount) -> int;

#ifdef WOS_SELFTEST
auto xfs_selftest_log_recycled_buffer_is_distinct() -> bool;
auto xfs_selftest_log_checkpoint_is_ordered_and_bounded() -> bool;
#endif

}  // namespace ker::vfs::xfs
