#pragma once

// XFS Parent Pointer Operations.
//
// Parent pointers are stored as extended attributes on each non-directory inode
// (and on sub-directories).  The xattr namespace flag is XFS_ATTR_PARENT.
//
//   xattr name:  XfsParentRec { __be64 p_ino; __be32 p_gen }  (12 bytes)
//   xattr value: directory entry name (variable length, NOT NUL-terminated)
//
// This allows reverse-mapping: given an inode, discover which directory(/ies)
// reference it and under what name.
//
// Reference: reference/xfs/libxfs/xfs_parent.c

#include <cstdint>
#include <vfs/fs/xfs/xfs_format.hpp>

namespace ker::vfs::xfs {

struct XfsInode;
struct XfsTransaction;

// ============================================================================
// Parent pointer query
// ============================================================================

// Retrieve the first parent pointer for an inode.
// On success, fills *parent_ino, *parent_gen, stores the directory entry name
// into name_buf (up to name_buflen bytes) and returns the actual name length.
// Returns negative errno on failure (-ENOATTR if no parent pointers exist).
auto xfs_parent_get(XfsInode* ip, xfs_ino_t* parent_ino, uint32_t* parent_gen, uint8_t* name_buf, uint32_t name_buflen) -> int;

// ============================================================================
// Parent pointer modification (requires a transaction)
// ============================================================================

// Add a parent pointer to the child inode.
// parent_ino/parent_gen identify the parent directory.
// name/namelen is the directory entry name being created.
auto xfs_parent_add(XfsInode* child, XfsTransaction* tp, xfs_ino_t parent_ino, uint32_t parent_gen, const uint8_t* name, uint16_t namelen)
    -> int;

// Remove a parent pointer from the child inode.
auto xfs_parent_remove(XfsInode* child, XfsTransaction* tp, xfs_ino_t parent_ino, uint32_t parent_gen, const uint8_t* name,
                       uint16_t namelen) -> int;

// Replace a parent pointer (used during rename operations).
// Removes the old parent pointer and adds the new one.
auto xfs_parent_replace(XfsInode* child, XfsTransaction* tp, xfs_ino_t old_pino, uint32_t old_pgen, const uint8_t* old_name,
                        uint16_t old_namelen, xfs_ino_t new_pino, uint32_t new_pgen, const uint8_t* new_name, uint16_t new_namelen) -> int;

}  // namespace ker::vfs::xfs
