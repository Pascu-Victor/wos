#pragma once

// XFS Extended Attribute subsystem - read, write, list, and remove xattrs.
//
// Supports shortform (inline in inode attr fork), leaf (single block), and
// node (multi-level DA btree) attribute formats.
//
// Reference: reference/xfs/libxfs/xfs_attr_sf.h,
//            reference/xfs/libxfs/xfs_attr_leaf.h,
//            reference/xfs/libxfs/xfs_da_format.h

#include <cstddef>
#include <cstdint>
#include <vfs/fs/xfs/xfs_format.hpp>
#include <vfs/fs/xfs/xfs_inode.hpp>
#include <vfs/fs/xfs/xfs_mount.hpp>
#include <vfs/fs/xfs/xfs_trans.hpp>

namespace ker::vfs::xfs {

// ============================================================================
// Attribute entry - in-memory representation returned by lookups/iterations
// ============================================================================

struct XfsAttrEntry {
    const uint8_t* name;   // pointer to name bytes (NOT null-terminated)
    uint16_t namelen;      // length of name
    const uint8_t* value;  // pointer to value bytes
    uint32_t valuelen;     // length of value
    uint8_t flags;         // XFS_ATTR_* namespace flags
    xfs_dahash_t hash;     // validated stored leaf hash (computed for shortform)
};

// Callback for iterating attributes.
// Returns 0 to continue, non-zero to stop.
using XfsAttrIterFn = int (*)(const XfsAttrEntry* entry, void* private_data);

// ============================================================================
// Attribute lookup / iterate
// ============================================================================

// Look up a single extended attribute by name and namespace flags.
// Copies the value into the caller-provided buffer.
// Returns the value length on success, -ENOATTR if not found, -ERANGE if
// buffer too small, or other negative errno on error.
auto xfs_attr_get(XfsInode* ip, const uint8_t* name, uint16_t namelen, uint8_t flags, void* value, uint32_t valuelen) -> int;

// Iterate over all extended attributes on an inode.
// Calls `fn` for each attribute.  Returns 0 on success.
auto xfs_attr_list(XfsInode* ip, XfsAttrIterFn fn, void* private_data) -> int;

// ============================================================================
// Attribute set / remove (requires transaction)
// ============================================================================

// Set (create or replace) an extended attribute.
// Supports inline shortform attrs, local and remote leaf entries, multi-level
// DA trees, and extent-list or BMBT attr-fork mappings.
auto xfs_attr_set(XfsInode* ip, XfsTransaction* tp, const uint8_t* name, uint16_t namelen, const uint8_t* value, uint32_t valuelen,
                  uint8_t flags) -> int;

// Remove an extended attribute.
// Returns 0 on success, -ENOATTR if not found.
auto xfs_attr_remove(XfsInode* ip, XfsTransaction* tp, const uint8_t* name, uint16_t namelen, uint8_t flags) -> int;

// Free every attr-fork data/DA/remote/BMBT block during inode inactivation.
auto xfs_attr_teardown(XfsInode* ip, XfsTransaction* tp) -> int;

#ifdef WOS_SELFTEST
// Force the copy-on-write builder to retain one mapping record per block. This
// yields a valid fragmented fork and deterministically exercises BMBT
// promotion/collapse on the disposable KTEST filesystem.
void xfs_selftest_attr_fragment_mappings(bool enabled);
auto xfs_selftest_attr_parent_hash_preserved() -> bool;
auto xfs_selftest_attr_incomplete_detected() -> bool;
auto xfs_selftest_attr_node_sibling_links() -> bool;
#endif

}  // namespace ker::vfs::xfs
