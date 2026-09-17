#pragma once

// XFS Symlink support - read symlink targets.
//
// Short symlinks are stored inline in the inode data fork (LOCAL format).
// Long symlinks are stored in data blocks (EXTENTS/BTREE format).
//
// Reference: reference/xfs/xfs_symlink.c, reference/xfs/libxfs/xfs_symlink_remote.c

#include <vfs/fs/xfs/xfs_inode.hpp>

namespace ker::vfs::xfs {

// Read the symlink target into the provided buffer.
// Copy up to buflen bytes without a null terminator, as required by readlink.
// Returns the number of copied bytes, or negative errno. String consumers must
// reserve space and append their own terminator after a successful read.
auto xfs_readlink(XfsInode* ip, char* buf, size_t buflen) -> int;

}  // namespace ker::vfs::xfs
