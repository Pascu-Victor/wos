#pragma once

// XFS Inode Allocation — allocate and free inodes from the AG inode B+trees.
//
// Reference: reference/xfs/libxfs/xfs_ialloc.h, reference/xfs/libxfs/xfs_ialloc.c

#include <cstdint>
#include <vfs/fs/xfs/xfs_format.hpp>
#include <vfs/fs/xfs/xfs_mount.hpp>
#include <vfs/fs/xfs/xfs_trans.hpp>

namespace ker::vfs::xfs {

// Allocate a new inode.
// mode: POSIX file type & mode bits (S_IFREG, S_IFDIR, etc.)
// Returns the allocated inode number, or NULLFSINO on failure.
auto xfs_ialloc(XfsMountContext* mount, XfsTransaction* tp, uint16_t mode) -> xfs_ino_t;

// Free an inode.  Marks it as free in the AG inode btree.
// Returns 0 on success.
auto xfs_ifree(XfsMountContext* mount, XfsTransaction* tp, xfs_ino_t ino) -> int;

}  // namespace ker::vfs::xfs
