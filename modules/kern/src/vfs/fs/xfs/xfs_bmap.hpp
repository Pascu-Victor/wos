#pragma once

// XFS Block Map (bmap) — extent mapping from logical file blocks to physical
// disk blocks.
//
// Handles all three inode data fork formats:
//   - LOCAL (inline data — no block mapping needed)
//   - EXTENTS (flat extent list stored in inode)
//   - BTREE (B+tree of extent records for large files)
//
// Reference: reference/xfs/libxfs/xfs_bmap.h, reference/xfs/libxfs/xfs_bmap.c

#include <cstdint>
#include <vfs/fs/xfs/xfs_format.hpp>
#include <vfs/fs/xfs/xfs_inode.hpp>

namespace ker::vfs::xfs {

// Result of a block map lookup
struct XfsBmapResult {
    xfs_fsblock_t startblock;  // physical starting block (NULLFSBLOCK if hole)
    xfs_filblks_t blockcount;  // number of contiguous blocks
    bool unwritten;            // preallocated/unwritten extent
    bool is_hole;              // file offset maps to a hole (no data on disk)
};

// Look up the physical block(s) corresponding to a logical file block offset.
// file_block: logical block offset within the file
// Returns 0 on success (result filled in), negative errno on error.
auto xfs_bmap_lookup(XfsInode* ip, xfs_fileoff_t file_block, XfsBmapResult* result) -> int;

// Look up and return all extents for a file (for readdir, etc.)
// Fills the caller's array up to max_extents.  Returns the number of extents
// found, or negative errno on error.
auto xfs_bmap_list_extents(XfsInode* ip, XfsBmbtIrec* extents, uint32_t max_extents) -> int;

// Add a new extent mapping to the inode's data fork.
// Currently supports EXTENTS format only (inserts into the sorted extent list).
// The caller must have already allocated the physical blocks and must commit
// the transaction after calling this.
// Returns 0 on success, negative errno on failure.
struct XfsTransaction;
auto xfs_bmap_add_extent(XfsInode* ip, XfsTransaction* tp, const XfsBmbtIrec& new_extent) -> int;

}  // namespace ker::vfs::xfs
