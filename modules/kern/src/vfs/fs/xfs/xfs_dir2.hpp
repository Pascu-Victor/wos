#pragma once

// XFS Directory operations — lookup, iterate, add, remove.
//
// Supports all four directory formats:
//   1. Shortform  — inline in inode data fork (small directories)
//   2. Block      — single directory block with data + leaf entries + tail
//   3. Leaf       — multiple data blocks + a single leaf index block
//   4. Node       — multiple data blocks + multi-level hash B+tree index
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

constexpr uint32_t XFS_DIR2_DATA_ALIGN_LOG = 3;
constexpr uint32_t XFS_DIR2_DATA_ALIGN = (1u << XFS_DIR2_DATA_ALIGN_LOG);
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

// Iterate over all entries in a directory.
// Calls fn(entry, ctx) for each entry.  Returns 0 on success.
auto xfs_dir_iterate(XfsInode* dp, XfsDirIterFn fn, void* ctx) -> int;

// Add a new name to a directory.
// dp: directory inode, name/namelen: entry name, ino: target inode number,
// ftype: XFS_DIR3_FT_* file type, tp: enclosing transaction.
// Returns 0 on success, negative errno on failure.
// Currently supports shortform and block-format directories.
struct XfsTransaction;
auto xfs_dir_addname(XfsInode* dp, const char* name, uint16_t namelen, xfs_ino_t ino, uint8_t ftype, XfsTransaction* tp) -> int;

// Remove a name from a directory.
// dp: directory inode, name/namelen: entry name, tp: enclosing transaction.
// Returns 0 on success, -ENOENT if not found, negative errno on failure.
// Currently supports shortform and block-format directories.
auto xfs_dir_removename(XfsInode* dp, const char* name, uint16_t namelen, XfsTransaction* tp) -> int;

// ============================================================================
// Hash function
// ============================================================================

// XFS directory name hash (same as xfs_da_hashname in the Linux source).
auto xfs_da_hashname(const uint8_t* name, int namelen) -> xfs_dahash_t;

}  // namespace ker::vfs::xfs
