#pragma once

// XFS Symlink support — read symlink targets.
//
// Short symlinks are stored inline in the inode data fork (LOCAL format).
// Long symlinks are stored in data blocks (EXTENTS/BTREE format).
//
// Reference: reference/xfs/xfs_symlink.c, reference/xfs/libxfs/xfs_symlink_remote.c

#include <vfs/fs/xfs/xfs_inode.hpp>

namespace ker::vfs::xfs {

// Read the symlink target into the provided buffer.
// buf must be at least buflen bytes.  The target is null-terminated.
// Returns the length of the target (excluding null), or negative errno.
auto xfs_readlink(XfsInode* ip, char* buf, size_t buflen) -> int;

}  // namespace ker::vfs::xfs
