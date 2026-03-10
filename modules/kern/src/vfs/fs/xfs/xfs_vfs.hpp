#pragma once

// XFS VFS Integration — connects the XFS native implementation to the
// WOS Virtual Filesystem layer via FileOperations function pointers.
//
// Provides:
//   - xfs_vfs_init_device()  — mount XFS on a block device, return context
//   - xfs_open_path()        — open a file/directory by path
//   - xfs_stat()             — stat by path
//   - xfs_fstat()            — fstat on open file
//   - get_xfs_fops()         — return the XFS FileOperations vtable

#include <dev/block_device.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/fs/xfs/xfs_mount.hpp>
#include <vfs/stat.hpp>

namespace ker::vfs::xfs {

// Initialize an XFS filesystem on the given block device.
// Returns a heap-allocated XfsMountContext on success, nullptr on failure.
auto xfs_vfs_init_device(dev::BlockDevice* device) -> XfsMountContext*;

// Open a file or directory by filesystem-relative path.
// Returns a heap-allocated File* on success, nullptr on error.
auto xfs_open_path(const char* fs_path, int flags, int mode, XfsMountContext* ctx) -> File*;

// Stat a file by filesystem-relative path.
auto xfs_stat(const char* fs_path, ker::vfs::stat* statbuf, XfsMountContext* ctx) -> int;

// Fstat an open file descriptor.
auto xfs_fstat(File* f, ker::vfs::stat* statbuf) -> int;

// Return the global XFS FileOperations vtable.
auto get_xfs_fops() -> FileOperations*;

// Register the XFS filesystem driver (call once during VFS init).
void register_xfs();

}  // namespace ker::vfs::xfs
