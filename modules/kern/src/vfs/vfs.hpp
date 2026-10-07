#pragma once

#include <cstddef>
#include <cstdint>
#include <platform/dbg/dbg.hpp>
#include <string_view>
#include <vfs/coherency.hpp>
#include <vfs/stat.hpp>

#include "bits/off_t.h"
#include "bits/ssize_t.h"

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::mod::security {
class Snapshot;
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
class DirectoryViewRef;
enum class MetadataBatchOperation : uint8_t {
    INVALID = 0,
    CREATE_CLOSE = 1,
    STAT_FOLLOW = 2,
    UNLINK = 3,
    RENAME = 4,
};

struct MetadataBatchEntry {
    const char* path{};
    const char* second_path{};
};

struct MetadataBatchResult {
    int32_t status{};
    Stat stat{};
};

struct VNode {
    const char* name{};
    vfs_node_type type{};
    void* private_data{};
};

// Open a path and return a file descriptor-like opaque pointer
auto vfs_open(std::string_view path, int flags, int mode) -> int;
auto vfs_openat(ker::mod::sched::task::Task* task, int dirfd, const char* pathname, int flags, int mode) -> int;

// Synchronous regular-file read using one explicit request credential. The
// caller retains path_context throughout; no process credentials are changed.
// Local TMPFS/XFS/FAT32 or negotiated owner-mapped WKI regular reads; other backends
// return -ENOTSUP. No ambient fallback or FD installation; error leaves output unchanged.
auto vfs_open_read_file_as(ker::mod::sched::task::Task& path_context, const ker::mod::security::Snapshot& credentials, const char* path,
                           bool apply_task_route, File** output) -> int;
// Owner-side physical path variant. The retained target must stay beneath the
// export; no task-root remapping or task routing is applied. TMPFS/XFS/FAT32 only.
// Optional absolute_symlink_root is the captured absolute visible export name:
// absolute link targets must stay below it and restart at the physical export.
// Null preserves physical owner-link semantics. Dot components in the supplied
// boundary are invalid; raw link components always undergo the native walk.
auto vfs_open_read_file_resolved_beneath_as(ker::mod::sched::task::Task& path_context, const ker::mod::security::Snapshot& credentials,
                                            const char* confinement_root, const char* path, File** output,
                                            const char* absolute_symlink_root = nullptr) -> int;
// Confined native file/directory open with one explicit snapshot, including
// creation/truncation. Caller supplies the already umask-adjusted mode. No
// worker credential changes, synthetic/device opens, or remote traversal.
// Failure leaves output unchanged; backend authorization precedes mutation.
// If directory is supplied, the caller retains it and path must be relative.
// Its TMPFS/XFS inode grants the starting directory, subject to export confinement;
// every directory searched below it uses the supplied snapshot.
auto vfs_open_file_resolved_beneath_as(ker::mod::sched::task::Task& path_context, const ker::mod::security::Snapshot& credentials,
                                       const char* confinement_root, const char* path, int flags, int mode, File** output,
                                       File* directory = nullptr, const char* absolute_symlink_root = nullptr) -> int;
// Native owner-relative OPEN under a complete retained view. Directory Files
// own independent root/cwd identities; regular Files preserve inherited policy.
// No routing/text fallback. Failure leaves output unchanged; caller owns success.
auto vfs_open_directory_view_as(ker::mod::sched::task::Task& task, const ker::mod::security::Snapshot& credentials,
                                const DirectoryViewRef& view, const char* relative_path, int flags, int mode, File** output) -> int;
// Search-only native File through an exact retained root/cwd view. The result
// owns independent detached identities, preserves inherited policy, and grants
// no data/enumeration access. Failure preserves output; release outside locks.
// Paired export path/name optionally maps visible absolute links; relative paths
// always start from retained identities, including after root/cwd rename.
auto vfs_pin_directory_view_as(ker::mod::sched::task::Task& task, const ker::mod::security::Snapshot& credentials,
                               const DirectoryViewRef& view, const char* relative_path, File** output, const char* export_path = nullptr,
                               const char* export_name = nullptr) -> int;
// Pin a native TMPFS/XFS directory using search permission and one explicit
// Snapshot. No data or enumeration grant, descriptor installation, or namespace
// mutation. Caller releases the result with vfs_put_file; failure preserves output.
// Optional directory anchors a relative path; otherwise path is physical/absolute.
// require_target_search=false is restricted to pinning the exact export boundary
// as metadata. It grants no traversal; later view admission checks cwd search.
auto vfs_pin_directory_resolved_beneath_as(ker::mod::sched::task::Task& path_context, const ker::mod::security::Snapshot& credentials,
                                           const char* confinement_root, const char* path, File** output, File* directory = nullptr,
                                           bool require_target_search = true) -> int;
// Recover an already-retained native directory's current export-relative name.
// This grants no access and performs no new DAC check, like native getcwd.
// Caller retains directory and must not hold namespace publication. Errors leave
// buffer and length unchanged; success includes a leading slash and trailing NUL.
auto vfs_directory_path_beneath(File* directory, const char* confinement_root, char* buffer, size_t size, size_t* length = nullptr) -> int;
// Recover relative to an exact retained native root, following proven native
// covered-mount identities when necessary. Missing cover proof returns ENOTSUP;
// renamed/hidden mount attachments return EAGAIN. Caller retains both Files and
// holds no namespace publication lock. This tests membership, not DAC or a
// transferable grant. Errors preserve buffer/length; no pathname cache selects
// either object. All temporary native File references retire after unlock.
auto vfs_directory_path_beneath_directory(File* directory, File* root, char* buffer, size_t size, size_t* length = nullptr) -> int;
// Open a path and return a File* directly without FD allocation.
auto vfs_open_file(const char* path, int flags, int mode) -> File*;
// Replace an owned TMPFS/XFS or supported remote read handle with an independent
// stable image view. Failure preserves the caller's handle. Remote peers must
// implement the versioned owner-held execution view; there is no read fallback.
auto vfs_stabilize_exec_file(File** owned) -> int;
// Read-only kernel input under one explicit credential snapshot and retained
// component walk. Preserves procfs descriptor grants; never installs an FD.
// Accepts only O_LOCAL, O_NOTIFY_CACHE_CHANGE and O_NO_CACHE; follows links.
// Caller retains path_context; failure leaves output unchanged. This context
// supplies path state only, not process-specific device/terminal authority.
auto vfs_open_file_as(ker::mod::sched::task::Task& path_context, const ker::mod::security::Snapshot& credentials, const char* path,
                      int flags, File** output) -> int;
// Open an already resolved absolute backing path without consulting the current
// task's root or WKI routing policy.
auto vfs_open_file_resolved(const char* path, int flags, int mode) -> File*;
auto vfs_open_file_resolved_beneath(const char* confinement_root, const char* path, int flags, int mode) -> File*;
// Close and destroy a File* that was opened without FD allocation.
auto vfs_close_file(File* file) -> int;
// Complete an already-started final close after the backend has been retired.
// Caller exclusively owns the consumed File; shared stream/cache/advisory
// detachment has run, private_data is cleared, and no backend close is repeated.
// The metadata flag was captured before backend close. Releases path/mount/File.
// This is the task-exit continuation for a backend whose close stack was abandoned.
auto vfs_finish_file_close(File* file, bool close_may_change_metadata, int close_result) -> int;
// Link the ordinary backend's fatal-close tracker before transferring the
// generic final-close prefix. The backend does not hold its registry lock here.
void vfs_delegate_ordinary_file_close(File* file);
// Fatal stack cleanup only, after WKI proxy/wait references have quiesced and
// before draining backend close trackers. Finishes owned prefix/tail stages.
void vfs_cleanup_ordinary_final_closes_for_task(uint64_t pid);
auto vfs_close(int fd) -> int;
auto vfs_read(int fd, void* buf, size_t count, size_t* actual_size = nullptr) -> ssize_t;
auto vfs_write(int fd, const void* buf, size_t count, size_t* actual_size = nullptr) -> ssize_t;
auto vfs_write_file(File* file, const void* buf, size_t count, size_t* actual_size = nullptr) -> ssize_t;
// Exact native owner File; never emulate append with SEEK_END plus WRITE.
auto vfs_append_file_native(File* file, const void* buf, size_t count, size_t* offset) -> ssize_t;
auto vfs_lseek(int fd, off_t offset, int whence) -> off_t;
auto vfs_isatty(int fd) -> bool;
// Optional explicit filesystem owner supports kernel callers without a running Task.
auto vfs_read_dir_entries(int fd, void* buffer, std::size_t max_size, ker::mod::sched::task::Task* task = nullptr) -> ssize_t;
auto vfs_sendfile(int outfd, int infd, off_t* offset, size_t count) -> ssize_t;

// Mapped owner mutations accept the same optional captured absolute-symlink
// namespace as owner OPEN; native callers retain physical-path defaults.
// Only traversed links are mapped, never the literal target stored by SYMLINK.
// Symlink operations
auto vfs_symlink(const char* target, const char* linkpath) -> int;
// Create a symlink at an already-resolved absolute backing path without
// consulting the current task's root or WKI routing policy.
auto vfs_symlink_resolved(const char* target, const char* linkpath) -> int;
auto vfs_symlink_resolved_beneath(const char* confinement_root, const char* target, const char* linkpath) -> int;
auto vfs_symlink_resolved_beneath_as(ker::mod::sched::task::Task& path_context, const ker::mod::security::Snapshot& credentials,
                                     const char* confinement_root, const char* target, const char* linkpath, bool require_directory,
                                     File* directory = nullptr, const char* absolute_symlink_root = nullptr) -> int;
auto vfs_symlinkat(ker::mod::sched::task::Task* task, const char* target, int dirfd, const char* linkpath) -> int;
auto vfs_readlink(const char* path, char* buf, size_t bufsize) -> ssize_t;
auto vfs_readlinkat(ker::mod::sched::task::Task* task, int dirfd, const char* pathname, char* buf, size_t bufsize) -> ssize_t;
auto vfs_readlink_resolved(const char* path, char* buf, size_t bufsize) -> ssize_t;
auto vfs_readlink_resolved_beneath(const char* confinement_root, const char* path, char* buf, size_t bufsize) -> ssize_t;
// Explicit owner authority; buf is kernel-owned and bufsize is at most 512.
// Errors preserve buf; success copies only the returned bytes, without a NUL.
// Optional directory and absolute_symlink_root use the retained grant and
// captured visible namespace described for owner OPEN.
auto vfs_readlink_resolved_beneath_as(ker::mod::sched::task::Task& path_context, const ker::mod::security::Snapshot& credentials,
                                      const char* confinement_root, const char* path, char* buf, size_t bufsize,
                                      bool require_directory = false, File* directory = nullptr,
                                      const char* absolute_symlink_root = nullptr) -> ssize_t;
auto vfs_realpath(const char* path, char* buf, size_t bufsize, size_t* len_out = nullptr) -> int;
// Resolves under a physical export and stages canonical export-relative text.
// Optional absolute_symlink_root maps visible absolute links before confinement.
auto vfs_realpath_resolved_beneath_as(ker::mod::sched::task::Task& task, const ker::mod::security::Snapshot& credentials, const char* root,
                                      const char* path, char* output, size_t capacity, size_t* length, bool require_directory = false,
                                      File* directory = nullptr, const char* absolute_symlink_root = nullptr) -> int;

// Stat operations
auto vfs_stat(const char* path, Stat* statbuf) -> int;
auto vfs_lstat(const char* path, Stat* statbuf) -> int;
auto vfs_stat_resolved(const char* path, Stat* statbuf) -> int;
auto vfs_stat_resolved_beneath(const char* confinement_root, const char* path, Stat* statbuf) -> int;
// Owner metadata under one admitted snapshot. Physical paths are confined to
// the export; no worker credentials, task routing or shared stat cache are used.
// Supports TMPFS/XFS/FAT32 and DEVFS metadata. PROCFS identity remains separate.
// Optional directory and absolute_symlink_root use the retained grant and
// captured visible namespace described for owner OPEN.
auto vfs_stat_resolved_beneath_as(ker::mod::sched::task::Task& path_context, const ker::mod::security::Snapshot& credentials,
                                  const char* confinement_root, const char* path, Stat* statbuf, bool follow_final_symlink = true,
                                  bool require_directory = false, File* directory = nullptr, const char* absolute_symlink_root = nullptr)
    -> int;
auto vfs_statat(ker::mod::sched::task::Task* task, int dirfd, const char* pathname, int flags, Stat* statbuf) -> int;
auto vfs_fstat(int fd, Stat* statbuf) -> int;
// Stat an already-open file without allocating an FD or consulting task state.
auto vfs_fstat_file(File* file, Stat* statbuf) -> int;
// Exact retained TMPFS/XFS description, bypassing generic metadata caches.
// Native sleeping serialization runs outside registry locks; errors preserve out.
auto vfs_fstat_file_native(File* file, Stat* out) -> int;
// Native directory observation with backend namespace lifetime protection.
// Caller owns a retained File and holds no backend/registry lock.
auto vfs_readdir_file_native(File* file, struct DirEntry* out, size_t index) -> int;
// Caller retains the exact native File. Failure preserves output and never
// authorizes a cached result; unsupported/pending authority needs fresh I/O.
auto vfs_file_coherency_snapshot(File* file, coherency::Stamp& output) -> int;
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
// Caller retains the File; reports its original mount across namespace changes.
auto vfs_fstatvfs_file(File* file, Statvfs* buf) -> int;

// Extended attributes. Path operations resolve intermediate symlinks and
// follow the final symlink only when requested. Other filesystems may return
// -EOPNOTSUPP.
enum class XattrOperation : uint8_t { SET, GET, LIST, REMOVE };
// Explicit owner APIs use the supplied caller throughout native lookup/commit.
// Physical absolute paths remain confined to export_root; optional directory
// Files instead anchor relative paths. The caller retains every File argument.
auto vfs_xattr_resolved_beneath_as(ker::mod::sched::task::Task& path_context, const ker::mod::security::Snapshot& credentials,
                                   const char* export_root, const char* path, XattrOperation operation, const char* name, void* value,
                                   size_t size, int flags, bool follow_final_symlink, bool require_directory = false,
                                   File* directory = nullptr, const char* absolute_symlink_root = nullptr) -> ssize_t;
// Metadata DAC uses this caller, not the description's data-access grant.
// REMOTE descriptions are refused to prevent recursive owner dispatch.
auto vfs_xattr_file_as(const ker::mod::security::Snapshot& credentials, File* file, XattrOperation operation, const char* name, void* value,
                       size_t size, int flags) -> ssize_t;
auto vfs_setxattr(const char* path, const char* name, const void* value, size_t size, int flags, bool follow_final_symlink) -> int;
// Beneath variants accept an authorized canonical export root and a relative
// path. They never apply worker task path policy, reject REMOTE mounts, and
// confine every intermediate/final symlink resolution to export_root.
auto vfs_setxattr_beneath(const char* export_root, const char* relative_path, const char* name, const void* value, size_t size, int flags,
                          bool follow_final_symlink) -> int;
auto vfs_fsetxattr(int fd, const char* name, const void* value, size_t size, int flags) -> int;
// File* variants require a retained reference and never consult an fd table.
// These legacy wrappers use ambient credentials. Mapped server work must use
// vfs_xattr_file_as with the request caller instead.
auto vfs_fsetxattr_file(File* file, const char* name, const void* value, size_t size, int flags) -> int;
auto vfs_getxattr(const char* path, const char* name, void* value, size_t size, bool follow_final_symlink) -> ssize_t;
auto vfs_getxattr_beneath(const char* export_root, const char* relative_path, const char* name, void* value, size_t size,
                          bool follow_final_symlink) -> ssize_t;
auto vfs_fgetxattr(int fd, const char* name, void* value, size_t size) -> ssize_t;
auto vfs_fgetxattr_file(File* file, const char* name, void* value, size_t size) -> ssize_t;
auto vfs_listxattr(const char* path, char* list, size_t size, bool follow_final_symlink) -> ssize_t;
auto vfs_listxattr_beneath(const char* export_root, const char* relative_path, char* list, size_t size, bool follow_final_symlink)
    -> ssize_t;
auto vfs_flistxattr(int fd, char* list, size_t size) -> ssize_t;
auto vfs_flistxattr_file(File* file, char* list, size_t size) -> ssize_t;
auto vfs_removexattr(const char* path, const char* name, bool follow_final_symlink) -> int;
auto vfs_removexattr_beneath(const char* export_root, const char* relative_path, const char* name, bool follow_final_symlink) -> int;
auto vfs_fremovexattr(int fd, const char* name) -> int;
auto vfs_fremovexattr_file(File* file, const char* name) -> int;

// Directory operations
auto vfs_mkdir(const char* path, int mode) -> int;
auto vfs_mkdirat(ker::mod::sched::task::Task* task, int dirfd, const char* pathname, int mode) -> int;
auto vfs_mkdir_resolved_beneath_as(ker::mod::sched::task::Task& path_context, const ker::mod::security::Snapshot& credentials,
                                   const char* confinement_root, const char* path, int mode, File* directory = nullptr,
                                   const char* absolute_symlink_root = nullptr) -> int;
auto vfs_mkdir_resolved_beneath(const char* confinement_root, const char* path, int mode) -> int;

// Mount operations (called from userspace via syscall)
auto vfs_mount(const char* source, const char* target, const char* fstype, unsigned long flags = 0, const char* data = nullptr) -> int;
auto vfs_umount(const char* target) -> int;
auto vfs_pivot_root(const char* new_root, const char* put_old) -> int;

// FD duplication
auto vfs_dup(int oldfd) -> int;
auto vfs_dup2(int oldfd, int newfd, int flags = 0) -> int;

// Metadata-only subject-root queries; caller must authorize private observation.
// Supplied snapshot/view output requires outside-publication context. A legacy
// null-snapshot caller refuses owned roots before borrowing references. Root text
// is the current physical spelling; a retained root view returns length zero.
// Failure preserves all outputs; the subject must remain retained throughout.
auto vfs_getroot_for_task(ker::mod::sched::task::Task* task, char* buffer, size_t capacity, size_t* length = nullptr,
                          const ker::mod::security::Snapshot* caller = nullptr, DirectoryViewRef* directory_view = nullptr) -> int;
// Intersect an existing native view with the subject's owned root, preserving
// narrower boundaries and union policy without new DAC. Outside publication;
// source may alias output. A null owned root leaves source scope unchanged.
auto vfs_scope_directory_view_for_task(ker::mod::sched::task::Task& task, const ker::mod::security::Snapshot& caller,
                                       const DirectoryViewRef& source, DirectoryViewRef& output) -> int;

// Caller retains a directory File and authorizes subject observation; outside
// publication. Existing views are scoped to the subject root. An ordinary
// native File under an owned root yields an identity pin without path reopen or
// new DAC. Null owned root returns an empty view for legacy text handling.
// Failure preserves output; original File flags/position/policy/view are unchanged.
auto vfs_directory_view_from_file_for_task(ker::mod::sched::task::Task& task, const ker::mod::security::Snapshot& caller, File* directory,
                                           DirectoryViewRef& output) -> int;

// Metadata-only process-relative name for a retained native directory under an
// owned root. Caller authorizes observation and retains Task/File. An explicit
// Snapshot requires outside-publication context; null refuses owned roots before
// borrowing references. Existing narrower views do not change the displayed root.
// Success writes NUL-terminated text/length; errors and legacy decline preserve
// outputs. DIRECTORY_PATH_LEGACY means no owned root and permits legacy handling.
inline constexpr int DIRECTORY_PATH_LEGACY = 1;
auto vfs_directory_path_for_task(ker::mod::sched::task::Task& task, File* directory, char* buffer, size_t capacity,
                                 size_t* length = nullptr, const ker::mod::security::Snapshot* caller = nullptr) -> int;

// Working directory
auto vfs_getcwd(char* buf, size_t size, size_t* len_out = nullptr) -> int;
// Retained subject required; output remains unchanged on failure. No namespace mutex recursion.
// Remote name recovery is allowed only with an explicit caller snapshot and
// outside namespace publication; text-only/serialized callers leave it null.
auto vfs_getcwd_for_task(ker::mod::sched::task::Task* task, char* buf, size_t size, size_t* len_out = nullptr,
                         const ker::mod::security::Snapshot* remote_credentials = nullptr, DirectoryViewRef* directory_view = nullptr)
    -> int;
// A non-null remote_credentials permits blocking owner recovery and requires
// outside-publication context. Native mixed-mount text recovery uses that same
// context guarantee to retain its proof through an internal publication guard.
// Null-snapshot text callers may already hold publication: mixed anchors are
// refused before retention. Owned-root recovery also requires a supplied snapshot
// and outside-publication context, including view output; null-snapshot callers
// refuse before borrowing root/cwd anchors. No caller may use this query as a DAC admission.
// Optional directory_view returns the complete scope instead of text (length
// zero) when present. Both alternatives use one captured cwd/anchor. A caller
// requesting scope must run outside publication and authorize private access.
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
// Caller retains the File; positional writes preserve pos and enforce its view policy.
auto vfs_pwrite_file(File* file, const void* buf, size_t count, off_t offset) -> ssize_t;

// File removal / rename
auto vfs_unlink(const char* path) -> int;
auto vfs_rmdir(const char* path) -> int;
auto vfs_unlinkat(ker::mod::sched::task::Task* task, int dirfd, const char* pathname, int flags) -> int;
auto vfs_rename(const char* oldpath, const char* newpath) -> int;
// Rename between already-resolved absolute backing paths without consulting
// the current task's root or WKI routing policy.
auto vfs_rename_resolved(const char* oldpath, const char* newpath) -> int;
auto vfs_rename_resolved_beneath_as(ker::mod::sched::task::Task& path_context, const ker::mod::security::Snapshot& credentials,
                                    const char* confinement_root, const char* oldpath, const char* newpath, bool source_requires_directory,
                                    bool destination_requires_directory, File* source_directory = nullptr,
                                    File* destination_directory = nullptr, const char* absolute_symlink_root = nullptr) -> int;
auto vfs_rename_resolved_beneath(const char* confinement_root, const char* oldpath, const char* newpath) -> int;
// Unlink an already-resolved absolute backing path without consulting the
// current task's root or WKI routing policy.
auto vfs_unlink_resolved(const char* path) -> int;
auto vfs_unlink_resolved_beneath_as(ker::mod::sched::task::Task& path_context, const ker::mod::security::Snapshot& credentials,
                                    const char* confinement_root, const char* path, bool remove_directory, bool require_directory,
                                    File* directory = nullptr, const char* absolute_symlink_root = nullptr) -> int;
auto vfs_unlink_resolved_beneath(const char* confinement_root, const char* path, bool directory) -> int;
auto vfs_renameat(ker::mod::sched::task::Task* task, int olddirfd, const char* oldpath, int newdirfd, const char* newpath) -> int;

// Execute one uniform metadata operation over an already bounded item set.
// The implementation preflights every path onto one negotiated REMOTE mount
// and sends every mutation in one server-preflighted request before any item
// can have an effect. On other negative returns, completed results and
// EINPROGRESS entries describe partial or ambiguous completion and must not be
// replayed. EOPNOTSUPP is reserved for a no-request-attempt fallback.
auto vfs_metadata_batch(ker::mod::sched::task::Task* task, MetadataBatchOperation operation, uint32_t mode,
                        const MetadataBatchEntry* entries, size_t count, MetadataBatchResult* results) -> int;

// Permissions
// Explicit native metadata adapters: caller retains File through return and
// supplies one admitted Snapshot. They never resolve cached path text, read
// worker credentials, consume File references, or grant data access. Call only
// from a blocking context outside namespace/FD-table locks. REMOTE remains
// unsupported here; public descriptor wrappers dispatch mapped remote calls
// separately, outside publication locks.
auto vfs_fchmod_file_as(File* file, const ker::mod::security::Snapshot& credentials, int mode) -> int;
auto vfs_fchown_file_as(File* file, const ker::mod::security::Snapshot& credentials, uint32_t owner, uint32_t group) -> int;
auto vfs_futimens_file_as(File* file, const ker::mod::security::Snapshot& credentials, const Timespec* times) -> int;
auto vfs_chmod(const char* path, int mode) -> int;
// Change permissions on an already-resolved absolute backing path without
// consulting the current task's root or WKI routing policy.
auto vfs_chmod_resolved(const char* path, int mode, bool follow_final_symlink) -> int;
auto vfs_chmod_resolved_beneath_as(ker::mod::sched::task::Task& path_context, const ker::mod::security::Snapshot& credentials,
                                   const char* confinement_root, const char* path, int mode, bool follow_final_symlink,
                                   bool require_directory = false, File* directory = nullptr, const char* absolute_symlink_root = nullptr)
    -> int;
auto vfs_chown_resolved_beneath_as(ker::mod::sched::task::Task& path_context, const ker::mod::security::Snapshot& credentials,
                                   const char* confinement_root, const char* path, uint32_t owner, uint32_t group,
                                   bool follow_final_symlink, bool require_directory = false, File* directory = nullptr,
                                   const char* absolute_symlink_root = nullptr) -> int;
auto vfs_chmod_resolved_beneath(const char* confinement_root, const char* path, int mode, bool follow_final_symlink) -> int;
auto vfs_fchmod(int fd, int mode) -> int;
auto vfs_fchmodat(ker::mod::sched::task::Task* task, int dirfd, const char* pathname, int mode, int flags) -> int;
auto vfs_chown(const char* path, uint32_t owner, uint32_t group) -> int;
auto vfs_fchown(int fd, uint32_t owner, uint32_t group) -> int;
auto vfs_fchownat(ker::mod::sched::task::Task* task, int dirfd, const char* pathname, uint32_t owner, uint32_t group, int flags) -> int;
// Change timestamps on an already-resolved absolute backing path while keeping
// every symlink and mount traversal beneath a local export root. This bypasses
// the current task's root and WKI routing policy.
auto vfs_utimens_resolved_beneath_as(ker::mod::sched::task::Task& path_context, const ker::mod::security::Snapshot& credentials,
                                     const char* confinement_root, const char* path, const Timespec* times, bool follow_final_symlink,
                                     bool require_directory = false, File* directory = nullptr, const char* absolute_symlink_root = nullptr)
    -> int;
auto vfs_utimens_resolved_beneath(const char* confinement_root, const char* path, const Timespec* times, bool follow_final_symlink) -> int;
auto vfs_utimensat(int dirfd, const char* pathname, const Timespec* times, int flags) -> int;
auto vfs_futimens(int fd, const Timespec* times) -> int;

// Truncate
// Caller holds a File reference; validates its existing write grant before callback.
auto vfs_ftruncate_file(File* file, off_t length) -> int;
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

// File control. Pointer-bearing record-lock commands receive a kernel-owned
// snapshot; scalar commands continue to use arg and leave flock null.
struct VfsFlockAbi {
    int16_t l_type = 0;
    int16_t l_whence = 0;
    off_t l_start = 0;
    off_t l_len = 0;
    int32_t l_pid = 0;
};
static_assert(sizeof(VfsFlockAbi) == 32);
auto vfs_fcntl(int fd, int cmd, uint64_t arg, VfsFlockAbi* flock = nullptr) -> int;

// IPC file identity helpers (used by WKI remote IPC proxy)
auto vfs_is_pipe_file(const File* f) -> bool;
auto vfs_is_epoll_file(const File* f) -> bool;
auto vfs_is_socket_file(const File* f) -> bool;

// FD helpers used by Task
auto vfs_alloc_fd(ker::mod::sched::task::Task* task, struct File* file) -> int;
auto vfs_get_file(ker::mod::sched::task::Task* task, int fd) -> struct File*;
auto vfs_get_file_retain(ker::mod::sched::task::Task* task, int fd) -> struct File*;
void vfs_retain_file(struct File* f);
// Drop one owned reference. Return the backend close result if this was the
// final reference, otherwise zero. Final destruction can block; call unlocked.
auto vfs_put_file_result(struct File* f) -> int;
void vfs_put_file(struct File* f);
auto vfs_release_fd(ker::mod::sched::task::Task* task, int fd) -> int;

// Resolve a dirfd-relative pathname to an absolute path.
// AT_FDCWD (-100) uses task->cwd. Absolute pathnames ignore dirfd.
// Returns 0 on success, negative errno on failure.
constexpr int AT_FDCWD = -100;
constexpr int AT_SYMLINK_NOFOLLOW = 0x100;
constexpr int AT_EACCESS = 0x200;
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
auto vfs_selftest_serialized_local_lookup() -> bool;
auto vfs_selftest_serialized_lookup_retirement(bool disk) -> bool;
auto vfs_selftest_realpath_native_credentials() -> bool;
auto vfs_selftest_realpath_procfs_credentials() -> bool;
auto vfs_selftest_realpath_owner_credentials() -> bool;
auto vfs_selftest_mkdir_owner_credentials() -> bool;
auto vfs_selftest_rename_owner_credentials() -> bool;
auto vfs_selftest_pair_preserves_admitted_credentials() -> bool;
auto vfs_selftest_linkat_admitted_snapshot(bool use_xfs) -> bool;
auto vfs_selftest_remove_owner_credentials() -> bool;
auto vfs_selftest_symlink_owner_credentials() -> bool;
auto vfs_selftest_utimens_owner_credentials() -> bool;
auto vfs_selftest_remote_readonly_resolved(const ker::mod::security::Snapshot& credentials, const char* path, int expected) -> int;
auto vfs_selftest_metadata_retry_preserves_credentials() -> bool;
auto vfs_selftest_chmod_owner_credentials() -> bool;
auto vfs_selftest_fd_install_failure_closes_file() -> bool;
auto vfs_selftest_dup2_replace_preserves_newfd_on_failure() -> bool;
auto vfs_selftest_create_pipe(ker::mod::sched::task::Task& task, int* descriptors) -> int;
auto vfs_selftest_pipe_failure_unwinds() -> bool;
auto vfs_selftest_pipe_flags() -> bool;
auto vfs_selftest_mount_admitted_snapshot() -> bool;
auto vfs_selftest_mount_physical_target() -> bool;
auto vfs_selftest_mount_target_refresh() -> bool;
auto vfs_selftest_mount_procfs_binding() -> bool;
auto vfs_selftest_mount_devfs_binding() -> bool;
auto vfs_selftest_mount_source_binding() -> bool;
auto vfs_selftest_mount_fat_metadata() -> bool;
class LookupHandle;
auto vfs_selftest_mount_readonly_target(ker::mod::sched::task::Task& task, const ker::mod::security::Snapshot& credentials,
                                        const LookupHandle& handle) -> int;
auto vfs_selftest_anonymous_fstat_snapshot_hits() -> bool;
auto vfs_selftest_fstat_snapshot_fast_path_hits() -> bool;
auto vfs_selftest_fstat_close_combines_fd_removal() -> bool;
auto vfs_selftest_remote_fstat_rejects_path_snapshot() -> bool;
auto vfs_selftest_fstat_seeds_path_metadata_cache() -> bool;
auto vfs_selftest_file_path_storage() -> bool;
auto vfs_selftest_file_data_write_invalidates_path_stat() -> bool;
auto vfs_selftest_file_data_write_skips_uncached_path_invalidation() -> bool;
auto vfs_selftest_file_data_close_refreshes_created_path_stat() -> bool;
auto vfs_selftest_file_data_close_does_not_resurrect_removed_path() -> bool;
// Consumes one unique ordinary remote File for prefix/tail/delegated fatal-close
// qualification. The optional callback abandons an actual backend close owner.
auto vfs_selftest_ordinary_final_close_lifetime(File* file, uint64_t pid, bool backend_complete, bool (*abandon_backend)(File*, uint64_t))
    -> bool;
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
auto vfs_selftest_lookup_cwd_identity(bool disk, bool root_identity = false) -> bool;
auto vfs_selftest_directory_view(bool disk) -> bool;
auto vfs_selftest_directory_view_publication(bool disk, bool procfs_paths = false) -> bool;
auto vfs_selftest_prepared_filesystem_context(bool disk, bool return_owner_cwd = false, bool independent_root = false) -> bool;
auto vfs_selftest_xfs_search_authority() -> bool;
auto vfs_selftest_native_mount_cover(bool disk) -> bool;
auto vfs_selftest_native_mount_membership(bool disk, bool prepare_contexts = false, bool reuse_membership = false,
                                          bool lookup_membership = false, bool native_crossing = false, bool anchor_paths = false,
                                          bool cwd_recovery = false, bool scoped_walk = false, bool filesystem_stats = false) -> bool;
auto vfs_selftest_statvfs_authorization(bool disk) -> bool;
auto vfs_selftest_directory_view_admission(bool disk) -> bool;
auto vfs_selftest_directory_view_rebase(bool disk) -> bool;
auto vfs_selftest_directory_view_lookup(bool disk) -> bool;
auto vfs_selftest_filesystem_context_snapshot() -> bool;
auto vfs_selftest_remote_cwd_root_admission() -> bool;
auto vfs_selftest_owned_root_absolute_walk(bool disk, bool relative_paths = false, bool cwd_metadata = false, bool visible_paths = false,
                                           bool procfs_roots = false, bool plain_directories = false, bool directory_text = false,
                                           bool mount_metadata = false) -> bool;
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
auto vfs_selftest_readlink_follows_intermediate_symlink() -> bool;
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
auto vfs_selftest_advisory_remote_shared() -> bool;
auto vfs_selftest_advisory_remote_waits() -> bool;
auto vfs_selftest_advisory_remote_shared_waiters() -> bool;
auto vfs_selftest_advisory_remote_grant_receipts() -> bool;
auto vfs_selftest_advisory_remote_scopes() -> bool;
auto vfs_selftest_advisory_remote_scope_capacity() -> bool;
auto vfs_selftest_stream_cache_read_eligibility() -> bool;
auto vfs_selftest_remote_read_bounce_window() -> bool;
auto vfs_selftest_stream_cache_local_detached_ttl() -> bool;
auto vfs_selftest_fd_allocation_caps_cloexec_range() -> bool;
#endif

}  // namespace ker::vfs
