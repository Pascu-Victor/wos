#pragma once

// XFS VFS Integration - connects the XFS native implementation to the
// WOS Virtual Filesystem layer via FileOperations function pointers.
//
// Provides:
//   - xfs_vfs_init_device()  - mount XFS on a block device, return context
//   - xfs_open_path()        - open a file/directory by path
//   - xfs_stat()             - stat by path
//   - xfs_fstat()            - fstat on open file
//   - get_xfs_fops()         - return the XFS FileOperations vtable

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

// Filesystem statistics (statvfs).
auto xfs_statvfs(XfsMountContext* ctx, ker::vfs::statvfs* buf) -> int;

// Change the permission bits of a file by filesystem-relative path.
auto xfs_chmod_path(const char* fs_path, int mode, XfsMountContext* ctx) -> int;

// Change the permission bits of an open file descriptor.
auto xfs_fchmod(File* f, int mode) -> int;

// Create a directory by filesystem-relative path.
auto xfs_mkdir_path(const char* fs_path, int mode, XfsMountContext* ctx) -> int;

// Remove a file by filesystem-relative path.
// Returns 0 on success, negative errno on failure.
// Cannot be used to remove directories (use xfs_rmdir_path for that).
auto xfs_unlink_path(const char* fs_path, XfsMountContext* ctx) -> int;

// Remove an empty directory by filesystem-relative path.
auto xfs_rmdir_path(const char* fs_path, XfsMountContext* ctx) -> int;

// Rename/move a file or directory within the same XFS mount.
auto xfs_rename_path(const char* old_fs_path, const char* new_fs_path, XfsMountContext* ctx) -> int;

// Return the global XFS FileOperations vtable.
auto get_xfs_fops() -> FileOperations*;

// Register the XFS filesystem driver (call once during VFS init).
void register_xfs();

}  // namespace ker::vfs::xfs
