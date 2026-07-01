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

#include <bits/ssize_t.h>

#include <dev/block_device.hpp>
#include <platform/mm/swap.hpp>
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
auto xfs_stat(const char* fs_path, ker::vfs::Stat* statbuf, XfsMountContext* ctx) -> int;

// Fstat an open file descriptor.
auto xfs_fstat(File* f, ker::vfs::Stat* statbuf) -> int;

// Return the mount context backing an open XFS file.
auto xfs_file_mount_context(File* f) -> XfsMountContext*;

// Return the stream-cache identity pieces for an open regular XFS file.
auto xfs_file_regular_identity(File* f, XfsMountContext** mount_out, uint64_t* ino_out) -> bool;

// Atomically append to an open file and return the starting append offset.
auto xfs_write_append(File* f, const void* buf, size_t count, size_t* offset_out) -> ssize_t;

// Commit dirty inode metadata for an open file.
auto xfs_fsync(File* f) -> int;

// Commit dirty mount metadata and flush the backing block device.
auto xfs_sync_mount(XfsMountContext* ctx) -> int;

// Drop path-derived caches associated with an XFS mount.
void xfs_parent_path_cache_purge_mount(XfsMountContext* ctx);

// Collect stable direct block extents for an already-open regular file used as
// swap backing. The caller owns the returned array and must delete[] it.
auto xfs_collect_swap_extents(File* f, ker::mod::mm::swap::SwapExtent** extents_out, size_t* extent_count_out) -> int;

#ifdef WOS_SELFTEST
auto xfs_selftest_hole_write_alloc_blocks(size_t block_off, size_t remaining_bytes, xfs_filblks_t hole_blocks, size_t block_size,
                                          uint32_t block_log, size_t write_pos = 0, bool sequential_append = false) -> xfs_extlen_t;
auto xfs_selftest_write_alloc_min_blocks(xfs_extlen_t max_blocks, bool extent_pressure = false, bool sequential_append = false)
    -> xfs_extlen_t;
auto xfs_selftest_truncate_zero_resets_data(uint64_t old_size, uint64_t nblocks) -> bool;
auto xfs_selftest_close_should_trim_prealloc(int open_flags, bool created_by_open = false, bool may_have_eof_prealloc = false) -> bool;
auto xfs_selftest_close_should_commit_inode(bool close_may_need_inode_commit, int open_flags, bool created_by_open = false,
                                            bool may_have_eof_prealloc = false) -> bool;
auto xfs_selftest_inode_has_eof_prealloc() -> bool;
auto xfs_selftest_mapped_append_can_zero_without_read(size_t write_pos, uint64_t file_size, size_t block_size) -> bool;
auto xfs_selftest_direct_read_batch_max_bytes(size_t block_size) -> size_t;
auto xfs_selftest_parent_path_cache() -> bool;
#endif

// Filesystem statistics (statvfs).
auto xfs_statvfs(XfsMountContext* ctx, ker::vfs::Statvfs* buf) -> int;

// Change the permission bits of a file by filesystem-relative path.
auto xfs_chmod_path(const char* fs_path, int mode, XfsMountContext* ctx) -> int;

// Change the permission bits of an open file descriptor.
auto xfs_fchmod(File* f, int mode) -> int;

// Update atime/mtime by filesystem-relative path or open file.
auto xfs_set_times_path(const char* fs_path, const Timespec& atime, const Timespec& mtime, bool set_atime, bool set_mtime,
                        XfsMountContext* ctx) -> int;
auto xfs_set_times_file(File* f, const Timespec& atime, const Timespec& mtime, bool set_atime, bool set_mtime) -> int;

// Create a directory by filesystem-relative path.
auto xfs_mkdir_path(const char* fs_path, int mode, XfsMountContext* ctx) -> int;

// Create an inline symlink by filesystem-relative path.
auto xfs_symlink_path(const char* target, const char* fs_path, XfsMountContext* ctx) -> int;

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
