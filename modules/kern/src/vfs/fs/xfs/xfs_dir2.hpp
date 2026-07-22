#pragma once

// XFS Directory operations - lookup, iterate, add, remove.
//
// Supports all four directory formats:
//   1. Shortform  - inline in inode data fork (small directories)
//   2. Block      - single directory block with data + leaf entries + tail
//   3. Leaf       - multiple data blocks + a single leaf index block
//   4. Node       - multiple data blocks + multi-level hash B+tree index
//
// Reference: reference/xfs/libxfs/xfs_dir2.h, reference/xfs/libxfs/xfs_dir2.c

#include <array>
#include <cstddef>
#include <cstdint>
#include <vfs/fs/xfs/xfs_format.hpp>
#include <vfs/fs/xfs/xfs_inode.hpp>

namespace ker::vfs::xfs {

// ============================================================================
// Directory constants
// ============================================================================

// Cookies above this value are opaque XFS readdir cursors; lower values remain
// available for legacy dense-index callers that bypass VFS d_off handling.
constexpr uint64_t XFS_READDIR_COOKIE_BASE = 1ULL << 32U;
constexpr uint32_t XFS_DIR2_DATA_ALIGN_LOG = 3;
constexpr uint32_t XFS_DIR2_DATA_ALIGN = (1U << XFS_DIR2_DATA_ALIGN_LOG);
constexpr uint64_t XFS_DIR2_SPACE_SIZE = (1ULL << 35);          // 32 GB per space
constexpr uint64_t XFS_DIR2_DATA_OFFSET = 0;                    // data space start
constexpr uint64_t XFS_DIR2_LEAF_OFFSET = XFS_DIR2_SPACE_SIZE;  // leaf space
constexpr uint64_t XFS_DIR2_FREE_OFFSET = 2 * XFS_DIR2_SPACE_SIZE;
constexpr uint32_t XFS_DIR2_NULL_DATAPTR = 0;

// Type aliases for directory addressing
using xfs_dir2_dataptr_t = uint32_t;
using xfs_dir2_data_off_t = uint16_t;
using xfs_dir2_db_t = uint32_t;
using xfs_dahash_t = uint32_t;

// ============================================================================
// Directory entry (result of lookup / iteration)
// ============================================================================

struct XfsDirEntry {
    xfs_ino_t ino;               // inode number
    uint8_t ftype;               // file type (XFS_DIR3_FT_*)
    uint16_t namelen;            // name length
    uint64_t cookie;             // opaque VFS readdir cursor for this entry
    std::array<char, 256> name;  // null-terminated name
};

// Callback for directory iteration.
// Return 0 to continue, non-zero to stop.
using XfsDirIterFn = int (*)(const XfsDirEntry* entry, void* ctx);

// ============================================================================
// Directory operations
// ============================================================================

// Look up a name in a directory.  Returns 0 on success (entry filled in),
// -ENOENT if not found, or negative errno on I/O error.
auto xfs_dir_lookup(XfsInode* dp, const char* name, uint16_t namelen, XfsDirEntry* entry) -> int;

// Bypass a cached answer and consult the directory data/index directly. This
// is reserved for retrying namespace mutations after a cache-assisted lookup
// reports ENOENT; successful and negative results still repair the cache.
auto xfs_dir_lookup_authoritative(XfsInode* dp, const char* name, uint16_t namelen, XfsDirEntry* entry) -> int;

// Look up a name already observed for a parent inode without loading that
// parent inode.  Returns true only when the dentry cache can answer.
auto xfs_dentry_cache_lookup_parent(XfsMountContext* mount, xfs_ino_t parent_ino, const char* name, uint16_t namelen, XfsDirEntry* entry,
                                    int* result) -> bool;

// Initialize the no-false-negative name filter for a newly created empty
// directory, or use it to prove that a name cannot exist. Loaded directories
// remain incomplete and always fall back to the on-disk lookup.
void xfs_dir_name_filter_init_empty(XfsInode* dp);
auto xfs_dir_name_filter_known_absent(const XfsInode* dp, const char* name, uint16_t namelen) -> bool;

// Seed the shared dentry cache with an entry observed while iterating a
// directory.  This is equivalent to caching a successful lookup.
void xfs_dir_observe_entry(XfsInode* dp, const XfsDirEntry* entry);

// Iterate over all entries in a directory.
// Calls fn(entry, ctx) for each entry.  Returns 0 on success.
auto xfs_dir_iterate(XfsInode* dp, XfsDirIterFn fn, void* ctx) -> int;

// Add a new name to a directory.
// dp: directory inode, name/namelen: entry name, ino: target inode number,
// ftype: XFS_DIR3_FT_* file type, tp: enclosing transaction.
// name_known_absent may only be true when the caller has just observed
// -ENOENT for the same parent/name under the same metadata lock, or when a
// valid same-generation dentry cache entry proves the name is absent.
// Returns 0 on success, negative errno on failure.
// Supports shortform and block-format directories. Leaf/node directories can
// reuse leaf slots while available and fall back to data-block scans for
// entries appended after the current single-leaf index is saturated.
struct XfsTransaction;
auto xfs_dir_addname(XfsInode* dp, const char* name, uint16_t namelen, xfs_ino_t ino, uint8_t ftype, XfsTransaction* tp,
                     bool name_known_absent = false) -> int;

// Remove a name from a directory.
// dp: directory inode, name/namelen: entry name, tp: enclosing transaction.
// Returns 0 on success, -ENOENT if not found, negative errno on failure.
// Supports shortform, block-format, and leaf/node directories.
auto xfs_dir_removename(XfsInode* dp, const char* name, uint16_t namelen, XfsTransaction* tp) -> int;

struct XfsDentryCacheStats {
    uint64_t hits{};
    uint64_t misses{};
    uint64_t stores{};
    uint64_t invalidations{};
};

void xfs_dentry_cache_stats(XfsDentryCacheStats& out);
void xfs_dentry_cache_purge_mount(XfsMountContext* mount);
// Invalidate entries published by a directory mutation whose transaction was
// subsequently cancelled.
void xfs_dentry_cache_invalidate_dir(XfsInode* dp);

#ifdef WOS_SELFTEST
auto xfs_selftest_dentry_cache_shortform() -> bool;
auto xfs_selftest_authoritative_lookup_repairs_stale_negative() -> bool;
auto xfs_selftest_block_lookup_uses_leaf_index_for_misses() -> bool;
auto xfs_selftest_leaf_index_complete_marker() -> bool;
auto xfs_selftest_directory_name_filter() -> bool;
auto xfs_selftest_dentry_cache_keeps_unrelated_dir_hot() -> bool;
auto xfs_selftest_dentry_cache_add_keeps_sibling_hot() -> bool;
auto xfs_selftest_dentry_cache_remove_keeps_sibling_hot() -> bool;
auto xfs_selftest_shortform_readdir_cookies_are_monotonic() -> bool;
auto xfs_selftest_shortform_offsets_match_data_layout() -> bool;
auto xfs_selftest_shortform_readdir_resume_after_removals() -> bool;
auto xfs_selftest_node_directory_growth_layout() -> bool;
auto xfs_selftest_node_directory_stale_compaction() -> bool;
auto xfs_selftest_node_directory_free_layout() -> bool;
#endif

// ============================================================================
// Hash function
// ============================================================================

// XFS directory name hash (same as xfs_da_hashname in the Linux source).
auto xfs_da_hashname(const uint8_t* name, int namelen) -> xfs_dahash_t;

}  // namespace ker::vfs::xfs
