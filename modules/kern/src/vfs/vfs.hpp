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
auto vfs_openat(ker::mod::sched::task::Task* task, int dirfd, const char* pathname, int flags, int mode) -> int;

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
// Create a symlink at an already-resolved absolute backing path without
// consulting the current task's root or WKI routing policy.
auto vfs_symlink_resolved(const char* target, const char* linkpath) -> int;
auto vfs_symlinkat(ker::mod::sched::task::Task* task, const char* target, int dirfd, const char* linkpath) -> int;
auto vfs_readlink(const char* path, char* buf, size_t bufsize) -> ssize_t;
auto vfs_readlinkat(ker::mod::sched::task::Task* task, int dirfd, const char* pathname, char* buf, size_t bufsize) -> ssize_t;
auto vfs_readlink_resolved(const char* path, char* buf, size_t bufsize) -> ssize_t;
auto vfs_realpath(const char* path, char* buf, size_t bufsize, size_t* len_out = nullptr) -> int;

// Stat operations
auto vfs_stat(const char* path, Stat* statbuf) -> int;
auto vfs_lstat(const char* path, Stat* statbuf) -> int;
auto vfs_stat_resolved(const char* path, Stat* statbuf) -> int;
auto vfs_statat(ker::mod::sched::task::Task* task, int dirfd, const char* pathname, int flags, Stat* statbuf) -> int;
auto vfs_fstat(int fd, Stat* statbuf) -> int;
// Stat an already-open file without allocating an FD or consulting task state.
auto vfs_fstat_file(File* file, Stat* statbuf) -> int;
// Fast cached fstat path for syscall dispatch. Returns -EAGAIN when the caller
// must fall back to vfs_fstat_file() with a retained file reference.
auto vfs_fstat_snapshot_fast(ker::mod::sched::task::Task* task, int fd, Stat* statbuf) -> int;
// Consume fd while returning independent fstat and close results. A valid fd
// is removed before any backend fstat or close work runs.
auto vfs_fstat_close_for_task(ker::mod::sched::task::Task* task, int fd, Stat* statbuf, int* stat_result) -> int;
// Backend open paths may seed a freshly-read stat snapshot before VFS attaches
// the absolute path. VFS will promote it only if metadata invalidation still
// proves that the path has not changed; created-by-open files may also seed
// their initial empty inode state until the first write invalidates it.
void vfs_prefill_file_stat_snapshot(File* file, const Stat& statbuf);

// Filesystem statistics
auto vfs_statvfs(const char* path, Statvfs* buf) -> int;
auto vfs_fstatvfs(int fd, Statvfs* buf) -> int;

// Directory operations
auto vfs_mkdir(const char* path, int mode) -> int;
auto vfs_mkdirat(ker::mod::sched::task::Task* task, int dirfd, const char* pathname, int mode) -> int;

// Mount operations (called from userspace via syscall)
auto vfs_mount(const char* source, const char* target, const char* fstype, unsigned long flags = 0, const char* data = nullptr) -> int;
auto vfs_umount(const char* target) -> int;
auto vfs_pivot_root(const char* new_root, const char* put_old) -> int;

// FD duplication
auto vfs_dup(int oldfd) -> int;
auto vfs_dup2(int oldfd, int newfd, int flags = 0) -> int;

// Working directory
auto vfs_getcwd(char* buf, size_t size, size_t* len_out = nullptr) -> int;
auto vfs_chdir(const char* path) -> int;
auto vfs_fchdir(ker::mod::sched::task::Task* task, int fd) -> int;

// Access check
auto vfs_access(const char* path, int mode) -> int;
auto vfs_faccessat(ker::mod::sched::task::Task* task, int dirfd, const char* pathname, int mode, int flags) -> int;

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
auto vfs_unlinkat(ker::mod::sched::task::Task* task, int dirfd, const char* pathname, int flags) -> int;
auto vfs_rename(const char* oldpath, const char* newpath) -> int;
// Rename between already-resolved absolute backing paths without consulting
// the current task's root or WKI routing policy.
auto vfs_rename_resolved(const char* oldpath, const char* newpath) -> int;
auto vfs_renameat(ker::mod::sched::task::Task* task, int olddirfd, const char* oldpath, int newdirfd, const char* newpath) -> int;

// Permissions
auto vfs_chmod(const char* path, int mode) -> int;
// Change permissions on an already-resolved absolute backing path without
// consulting the current task's root or WKI routing policy.
auto vfs_chmod_resolved(const char* path, int mode, bool follow_final_symlink) -> int;
auto vfs_fchmod(int fd, int mode) -> int;
auto vfs_fchmodat(ker::mod::sched::task::Task* task, int dirfd, const char* pathname, int mode, int flags) -> int;
auto vfs_chown(const char* path, uint32_t owner, uint32_t group) -> int;
auto vfs_fchown(int fd, uint32_t owner, uint32_t group) -> int;
auto vfs_fchownat(ker::mod::sched::task::Task* task, int dirfd, const char* pathname, uint32_t owner, uint32_t group, int flags) -> int;
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
    uint64_t existence_hits{};
    uint64_t existence_misses{};
    uint64_t existence_stores{};
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
auto vfs_fsync_file(File* file) -> int;
auto vfs_sync() -> int;
auto vfs_shutdown_sync() -> int;
auto vfs_shutdown_unmount_all(const char* root_path) -> int;

// Hard link
auto vfs_link(const char* oldpath, const char* newpath) -> int;
auto vfs_linkat(ker::mod::sched::task::Task* task, int olddirfd, const char* oldpath, int newdirfd, const char* newpath, int flags) -> int;

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
constexpr int AT_REMOVEDIR = 0x200;
constexpr int AT_SYMLINK_FOLLOW = 0x400;
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
auto vfs_selftest_fstat_snapshot_fast_path_hits() -> bool;
auto vfs_selftest_fstat_close_combines_fd_removal() -> bool;
auto vfs_selftest_remote_fstat_snapshot_cacheable() -> bool;
auto vfs_selftest_fstat_seeds_path_metadata_cache() -> bool;
auto vfs_selftest_file_path_storage() -> bool;
auto vfs_selftest_file_data_write_invalidates_path_stat() -> bool;
auto vfs_selftest_file_data_write_skips_uncached_path_invalidation() -> bool;
auto vfs_selftest_file_data_close_refreshes_created_path_stat() -> bool;
auto vfs_selftest_created_open_prefill_seeds_path_stat() -> bool;
auto vfs_selftest_file_metadata_change_invalidates_path_stat() -> bool;
auto vfs_selftest_open_create_metadata_hint() -> bool;
auto vfs_selftest_open_missing_uses_metadata_cache() -> bool;
auto vfs_selftest_open_success_seeds_metadata_cache() -> bool;
auto vfs_selftest_open_write_success_seeds_metadata_hints() -> bool;
auto vfs_selftest_metadata_cache_stores_enotdir() -> bool;
auto vfs_selftest_mkdir_seeds_metadata_cache() -> bool;
auto vfs_selftest_removed_paths_seed_missing_metadata_cache() -> bool;
auto vfs_selftest_openat_dirfd_installs_open_file() -> bool;
auto vfs_selftest_openat_at_fdcwd_uses_supplied_task() -> bool;
auto vfs_selftest_fchdir_changes_supplied_task_cwd() -> bool;
auto vfs_selftest_unlinkat_renameat_dirfd_mutations() -> bool;
auto vfs_selftest_rename_seeds_metadata_cache() -> bool;
auto vfs_selftest_metadata_cache_rejects_stale_negative_store() -> bool;
auto vfs_selftest_resolved_stat_cache_rejects_mount_generation_change() -> bool;
auto vfs_selftest_path_text_scan_matches_helpers() -> bool;
auto vfs_selftest_wki_host_alias_overlap() -> bool;
auto vfs_selftest_wki_host_root_mount_gate_matches_task_root() -> bool;
auto vfs_selftest_resolved_wki_entry_uses_task_root_view() -> bool;
auto vfs_selftest_absolute_local_stat_fast_path_gate() -> bool;
auto vfs_selftest_common_local_relative_resolver_fast_path() -> bool;
auto vfs_selftest_statat_dirfd_metadata_cache() -> bool;
auto vfs_selftest_statat_at_fdcwd_uses_supplied_task() -> bool;
auto vfs_selftest_statat_root_cwd_relative_paths() -> bool;
auto vfs_selftest_faccessat_dirfd_metadata_cache() -> bool;
auto vfs_selftest_faccessat_at_fdcwd_uses_supplied_task() -> bool;
auto vfs_selftest_faccessat_f_ok_existence_cache_invalidates() -> bool;
auto vfs_selftest_faccessat_f_ok_skips_known_non_symlink_probe() -> bool;
auto vfs_selftest_faccessat_flags() -> bool;
auto vfs_selftest_mkdirat_dirfd_creates_relative_directory() -> bool;
auto vfs_selftest_readlinkat_dirfd_reads_relative_symlink() -> bool;
auto vfs_selftest_symlinkat_dirfd_creates_relative_symlink() -> bool;
auto vfs_selftest_linkat_dirfd_creates_relative_hardlink() -> bool;
auto vfs_selftest_chdir_common_local_fast_path_uses_metadata_cache() -> bool;
auto vfs_selftest_fchmodat_dirfd_changes_relative_file_mode() -> bool;
auto vfs_selftest_fchownat_dirfd_changes_relative_file_owner() -> bool;
auto vfs_selftest_stat_lstat_share_non_symlink_cache() -> bool;
auto vfs_selftest_readdir_seeds_non_symlink_hints() -> bool;
auto vfs_selftest_readlink_uses_metadata_negative_cache() -> bool;
auto vfs_selftest_missing_prefix_short_circuits_symlink_walk() -> bool;
auto vfs_selftest_symlink_prefix_cache_skips_known_parent() -> bool;
auto vfs_selftest_procfs_fd_link_probe_gate() -> bool;
auto vfs_selftest_packed_dirent_records() -> bool;
auto vfs_selftest_fcntl_setfl_preserves_open_policy_flags() -> bool;
auto vfs_selftest_stream_cache_read_eligibility() -> bool;
auto vfs_selftest_remote_read_bounce_window() -> bool;
auto vfs_selftest_stream_cache_local_detached_ttl() -> bool;
auto vfs_selftest_fd_allocation_caps_cloexec_range() -> bool;
#endif

}  // namespace ker::vfs
