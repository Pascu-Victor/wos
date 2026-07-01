#pragma once

#include <cstddef>
#include <cstdint>
#include <platform/dbg/dbg.hpp>
#include <string_view>
#include <vfs/stat.hpp>

#include "bits/off_t.h"
#include "bits/ssize_t.h"

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::vfs {

// VFS logging control - define VFS_DEBUG to enable debug logging
// Helper inline functions for logging (optimizes away when VFS_DEBUG is not defined)
inline void vfs_debug_log(const char* msg) {
#ifdef VFS_DEBUG
    ker::mod::dbg::logger<"vfs">::debug("%s", msg);
#else
    (void)msg;
#endif
}

inline void vfs_debug_log_hex(uint64_t value) {
#ifdef VFS_DEBUG
    ker::mod::dbg::logger<"vfs">::debug("0x%llx", static_cast<unsigned long long>(value));
#else
    (void)value;
#endif
}

enum class vfs_node_type : uint8_t { FILE, DIRECTORY, DEVICE, SOCKET, SYMLINK };

struct File;

struct VNode {
    const char* name{};
    vfs_node_type type{};
    void* private_data{};
};

// Open a path and return a file descriptor-like opaque pointer
auto vfs_open(std::string_view path, int flags, int mode) -> int;

// Open a path and return a File* directly (no FD allocation, no task context).
// Used by server-side subsystems (e.g. WKI remote VFS) that operate on files
// outside of any userspace task context.
auto vfs_open_file(const char* path, int flags, int mode) -> File*;
// Open an already resolved absolute backing path without consulting the current
// task's root or WKI routing policy.
auto vfs_open_file_resolved(const char* path, int flags, int mode) -> File*;
// Close and destroy a File* that was opened without FD allocation.
auto vfs_close_file(File* file) -> int;
auto vfs_close(int fd) -> int;
auto vfs_read(int fd, void* buf, size_t count, size_t* actual_size = nullptr) -> ssize_t;
auto vfs_write(int fd, const void* buf, size_t count, size_t* actual_size = nullptr) -> ssize_t;
auto vfs_write_file(File* file, const void* buf, size_t count, size_t* actual_size = nullptr) -> ssize_t;
auto vfs_lseek(int fd, off_t offset, int whence) -> off_t;
auto vfs_isatty(int fd) -> bool;
auto vfs_read_dir_entries(int fd, void* buffer, std::size_t max_size) -> ssize_t;
auto vfs_sendfile(int outfd, int infd, off_t* offset, size_t count) -> ssize_t;

// Symlink operations
auto vfs_symlink(const char* target, const char* linkpath) -> int;
auto vfs_readlink(const char* path, char* buf, size_t bufsize) -> ssize_t;
auto vfs_readlink_resolved(const char* path, char* buf, size_t bufsize) -> ssize_t;
auto vfs_realpath(const char* path, char* buf, size_t bufsize) -> int;

// Stat operations
auto vfs_stat(const char* path, Stat* statbuf) -> int;
auto vfs_lstat(const char* path, Stat* statbuf) -> int;
auto vfs_stat_resolved(const char* path, Stat* statbuf) -> int;
auto vfs_fstat(int fd, Stat* statbuf) -> int;
// Stat an already-open file without allocating an FD or consulting task state.
auto vfs_fstat_file(File* file, Stat* statbuf) -> int;
// Backend open paths may seed a freshly-read stat snapshot before VFS attaches
// the absolute path. VFS will promote it only if metadata invalidation still
// proves that the path has not changed.
void vfs_prefill_file_stat_snapshot(File* file, const Stat& statbuf);

// Filesystem statistics
auto vfs_statvfs(const char* path, Statvfs* buf) -> int;
auto vfs_fstatvfs(int fd, Statvfs* buf) -> int;

// Directory operations
auto vfs_mkdir(const char* path, int mode) -> int;

// Mount operations (called from userspace via syscall)
auto vfs_mount(const char* source, const char* target, const char* fstype, unsigned long flags = 0, const char* data = nullptr) -> int;
auto vfs_umount(const char* target) -> int;
auto vfs_pivot_root(const char* new_root, const char* put_old) -> int;

// FD duplication
auto vfs_dup(int oldfd) -> int;
auto vfs_dup2(int oldfd, int newfd, int flags = 0) -> int;

// Working directory
auto vfs_getcwd(char* buf, size_t size) -> int;
auto vfs_chdir(const char* path) -> int;

// Access check
auto vfs_access(const char* path, int mode) -> int;

// Permission check helper: checks if current task can perform requested access
// on a file with the given mode/uid/gid. access_bits are R_OK/W_OK/X_OK.
// Returns 0 on success, -EACCES on failure.
auto vfs_check_permission(uint32_t file_mode, uint32_t file_uid, uint32_t file_gid, int access_bits) -> int;

// Positional I/O (does not modify file position)
auto vfs_pread(int fd, void* buf, size_t count, off_t offset) -> ssize_t;
auto vfs_pread_file(File* file, void* buf, size_t count, off_t offset) -> ssize_t;
auto vfs_pread_file_direct(File* file, void* buf, size_t count, off_t offset) -> ssize_t;
auto vfs_pwrite(int fd, const void* buf, size_t count, off_t offset) -> ssize_t;

// File removal / rename
auto vfs_unlink(const char* path) -> int;
auto vfs_rmdir(const char* path) -> int;
auto vfs_rename(const char* oldpath, const char* newpath) -> int;

// Permissions
auto vfs_chmod(const char* path, int mode) -> int;
auto vfs_fchmod(int fd, int mode) -> int;
auto vfs_chown(const char* path, uint32_t owner, uint32_t group) -> int;
auto vfs_fchown(int fd, uint32_t owner, uint32_t group) -> int;
auto vfs_utimensat(int dirfd, const char* pathname, const Timespec* times, int flags) -> int;
auto vfs_futimens(int fd, const Timespec* times) -> int;

// Truncate
auto vfs_ftruncate(int fd, off_t length) -> int;

// Pipe
auto vfs_pipe(int pipefd[2], int flags = 0) -> int;  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
auto vfs_pipe_reserve_capacity(File* file, size_t capacity) -> bool;

struct LocalPipePerfSnapshot {
    uint64_t active_pipes{};
    uint64_t created_since_reset{};
    uint64_t peak_pipes{};
    uint64_t capacity_bytes{};
    uint64_t peak_capacity_bytes{};
    uint64_t buffered_bytes{};
    uint64_t reader_waiters{};
    uint64_t writer_waiters{};
    uint64_t poll_waiters{};
    uint64_t direct_writes{};
    uint64_t read_closed{};
    uint64_t write_closed{};
    uint64_t approx_alloc_bytes{};
};

void vfs_get_local_pipe_perf_snapshot(LocalPipePerfSnapshot& out);
void vfs_reset_local_pipe_perf_counters();
auto vfs_generate_local_pipe_diag(char* buf, size_t bufsz) -> size_t;

struct VfsCachePerfSnapshot {
    uint64_t metadata_hits{};
    uint64_t metadata_misses{};
    uint64_t metadata_stores{};
    uint64_t metadata_miss_empty{};
    uint64_t metadata_miss_invalidated{};
    uint64_t metadata_miss_stale_generation{};
    uint64_t metadata_miss_conflict{};
    uint64_t metadata_path_invalidations{};
    uint64_t metadata_generation_resets{};
    uint64_t symlink_hits{};
    uint64_t symlink_misses{};
    uint64_t symlink_stores{};
    uint64_t stream_hits{};
    uint64_t stream_misses{};
    uint64_t stream_backend_reads{};
    uint64_t stream_backend_bytes{};
    uint64_t stream_copied_bytes{};
    uint64_t stream_invalidate_empty_skips{};
    uint64_t fstat_snapshot_hits{};
    uint64_t fstat_snapshot_misses{};
    uint64_t fstat_snapshot_stores{};
    uint64_t fstat_snapshot_miss_uncacheable{};
    uint64_t fstat_snapshot_miss_bad_args{};
    uint64_t fstat_snapshot_miss_no_cache{};
    uint64_t fstat_snapshot_miss_pathless{};
    uint64_t fstat_snapshot_miss_fs{};
    uint64_t fstat_snapshot_miss_empty{};
    uint64_t fstat_snapshot_miss_generation{};
    uint64_t fstat_snapshot_miss_invalidated{};
};

void vfs_get_cache_perf_snapshot(VfsCachePerfSnapshot& out);
auto vfs_cache_epoch_snapshot() -> uint64_t;

// Sync
auto vfs_fsync(int fd) -> int;
auto vfs_sync() -> int;
auto vfs_shutdown_sync() -> int;
auto vfs_shutdown_unmount_all(const char* root_path) -> int;

// Hard link
auto vfs_link(const char* oldpath, const char* newpath) -> int;

// WKI task-local VFS policy
auto vfs_wki_rule_add(const char* prefix, uint32_t route) -> int;
auto vfs_wki_rule_get(uint32_t index, char* prefix_buf, size_t prefix_buf_size, uint32_t* route_out) -> int;
auto vfs_wki_default_rule_get(uint32_t index, char* prefix_buf, size_t prefix_buf_size, uint32_t* route_out) -> int;
auto vfs_wki_effective_route_for_path(const ker::mod::sched::task::Task* task, const char* path, uint32_t* route_out) -> int;
auto vfs_wki_rule_clear() -> int;
void vfs_wki_load_default_rules();

// File control
auto vfs_fcntl(int fd, int cmd, uint64_t arg) -> int;

// IPC file identity helpers (used by WKI remote IPC proxy)
auto vfs_is_pipe_file(const File* f) -> bool;
auto vfs_is_epoll_file(const File* f) -> bool;
auto vfs_is_socket_file(const File* f) -> bool;

// FD helpers used by Task
auto vfs_alloc_fd(ker::mod::sched::task::Task* task, struct File* file) -> int;
auto vfs_get_file(ker::mod::sched::task::Task* task, int fd) -> struct File*;
auto vfs_get_file_retain(ker::mod::sched::task::Task* task, int fd) -> struct File*;
void vfs_retain_file(struct File* f);
void vfs_put_file(struct File* f);
auto vfs_release_fd(ker::mod::sched::task::Task* task, int fd) -> int;

// Resolve a dirfd-relative pathname to an absolute path.
// AT_FDCWD (-100) uses task->cwd. Absolute pathnames ignore dirfd.
// Returns 0 on success, negative errno on failure.
constexpr int AT_FDCWD = -100;
constexpr int AT_SYMLINK_NOFOLLOW = 0x100;
constexpr int AT_EMPTY_PATH = 0x1000;
auto vfs_resolve_dirfd(ker::mod::sched::task::Task* task, int dirfd, const char* pathname, char* resolved, size_t resolved_size) -> int;

// Initialize VFS (register tmpfs, devfs, etc.)
void init();

// Stream-cache teardown helper for REMOTE mounts.
void vfs_stream_cache_invalidate_remote_scope(const void* remote_scope);

// Cache-notify helpers shared with WKI remote VFS.
void vfs_cache_notify_register_open_file(File* file);
void vfs_cache_notify_invalidate_path(const char* vfs_path);
void vfs_cache_notify_path_changed(const char* old_vfs_path, const char* new_vfs_path);
void vfs_cache_notify_file_changed(File* file);
void vfs_cache_notify_file_data_changed(File* file);
auto vfs_cache_notify_file_dirty(File* file) -> bool;
void vfs_cache_notify_acknowledge_file(File* file);

#ifdef WOS_SELFTEST
auto vfs_selftest_fd_install_failure_closes_file() -> bool;
auto vfs_selftest_dup2_replace_preserves_newfd_on_failure() -> bool;
auto vfs_selftest_pipe_failure_unwinds() -> bool;
auto vfs_selftest_pipe_flags() -> bool;
auto vfs_selftest_anonymous_fstat_snapshot_hits() -> bool;
auto vfs_selftest_remote_fstat_snapshot_cacheable() -> bool;
auto vfs_selftest_file_path_storage() -> bool;
auto vfs_selftest_file_data_write_invalidates_path_stat() -> bool;
auto vfs_selftest_file_data_write_skips_uncached_path_invalidation() -> bool;
auto vfs_selftest_file_metadata_change_invalidates_path_stat() -> bool;
auto vfs_selftest_open_create_metadata_hint() -> bool;
auto vfs_selftest_metadata_cache_rejects_stale_negative_store() -> bool;
auto vfs_selftest_fcntl_setfl_preserves_open_policy_flags() -> bool;
auto vfs_selftest_stream_cache_read_eligibility() -> bool;
auto vfs_selftest_stream_cache_local_detached_ttl() -> bool;
auto vfs_selftest_fd_allocation_caps_cloexec_range() -> bool;
#endif

}  // namespace ker::vfs
