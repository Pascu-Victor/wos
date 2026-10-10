#pragma once

#include <cstddef>
#include <cstdint>
#include <platform/sched/pid_namespace.hpp>
#include <platform/security/credentials.hpp>
#include <platform/sys/mutex.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/stat.hpp>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::vfs::procfs {

// Procfs node types
enum class ProcNodeType : uint8_t {
    ROOT_DIR,             // /proc
    PID_DIR,              // /proc/<pid>
    TASK_DIR,             // /proc/<pid>/task
    TASK_TID_DIR,         // /proc/<pid>/task/<tid>
    SELF_LINK,            // /proc/self -> /proc/<pid>
    EXE_LINK,             // /proc/<pid>/exe -> exe_path
    CWD_LINK,             // /proc/<pid>/cwd -> cwd
    ROOT_LINK,            // /proc/<pid>/root -> root
    FD_DIR,               // /proc/<pid>/fd
    FD_LINK,              // /proc/<pid>/fd/<fd> -> open file path
    STATUS_FILE,          // /proc/<pid>/status
    MOUNTS_FILE,          // /proc/mounts
    STAT_FILE,            // /proc/<pid>/stat
    STATM_FILE,           // /proc/<pid>/statm
    CMDLINE_FILE,         // /proc/<pid>/cmdline
    MAPS_FILE,            // /proc/<pid>/maps
    IMAGES_FILE,          // /proc/<pid>/images
    UPTIME_FILE,          // /proc/uptime
    CPU_STAT_FILE,        // /proc/stat
    LOADAVG_FILE,         // /proc/loadavg
    MEMINFO_FILE,         // /proc/meminfo
    VERSION_FILE,         // /proc/version
    KPERF_FILE,           // /proc/kperf    -> drain kernel perf ring buffer as text events
    KWKISTAT_FILE,        // /proc/kwkistat -> recording-scoped WKI summary statistics
    KCPUSTAT_FILE,        // /proc/kcpustat -> per-CPU aggregate scheduler statistics
    KCPUSTATE_FILE,       // /proc/kcpustate -> per-CPU scheduler current/runqueue state
    KPERFCTL_FILE,        // /proc/kperfctl -> write "enable"/"disable" to control recording
    KCONTSTAT_FILE,       // /proc/kcontstat -> per-subsystem container statistics
    KIPCSTAT_FILE,        // /proc/kipcstat -> WKI IPC memory/queue snapshot
    WKI_LAUNCHER_FILE,    // /proc/<pid>/wki_launcher -> hostname of the node that launched the process
    WKI_RUNNER_FILE,      // /proc/<pid>/wki_runner   -> hostname of the node currently running the process
    WKI_REMOTE_PID_FILE,  // /proc/<pid>/wki_remote_pid -> remote execution PID for this process
    WKI_DIR,              // /proc/wki
    WKI_PEERS_FILE,       // /proc/wki/peers -> WKI topology rows
    WKI_AUTH_FILE,        // /proc/wki/auth -> non-secret session/policy status
    WKI_NETDIAG_FILE,     // /proc/wki/netdiag -> network packet/listener/channel diagnostics
    WKI_PIPES_FILE,       // /proc/wki/pipes -> local pipe owner diagnostics
    WKI_CHAOS_FILE,       // /proc/wki/chaos -> deterministic WKI fault injection control and trace
    // /proc/wki/chaos_workload -> gated live-scenario workload bridge
    WKI_CHAOS_WORKLOAD_FILE,
    MEMACC_DIR,          // /proc/memacc
    MEMACC_TRACK_DIR,    // /proc/memacc/track
    MEMACC_RECLAIM_DIR,  // /proc/memacc/reclaim
    MEMACC_SUMMARY_FILE,
    MEMACC_ZONES_FILE,
    MEMACC_PROCS_FILE,
    MEMACC_DEAD_FILE,
    MEMACC_ALLOC_TOTALS_FILE,
    MEMACC_SLABS_FILE,
    MEMACC_KMALLOC_LIVE_FILE,
    MEMACC_KMALLOC_CALLERS_FILE,
    MEMACC_PAGE_CALLERS_FILE,
    MEMACC_FEATURES_FILE,
    MEMACC_TRACK_PAGE_CALLERS_FILE,
    MEMACC_TRACK_KMALLOC_DEBUG_FILE,
    MEMACC_RECLAIM_BUFFER_CACHE_FILE,
    MEMACC_RECLAIM_PACKET_POOL_FILE,
    MEMACC_RECLAIM_XFS_INODE_FILE,
    MEMACC_RECLAIM_FILE_MMAP_CACHE_FILE,
    MEMACC_RECLAIM_ANONYMOUS_SWAP_FILE,
    MEMACC_RECLAIM_COORDINATOR_FILE,
    LIMITS_FILE,       // /proc/<pid>/limits and task aliases
    TIMEKEEPING_FILE,  // /proc/timekeeping -> clocksource and wall-clock discipline
    KPERFSTATUS_FILE,  // /proc/kperfstatus -> recording session/loss/consumer counters
    WKI_NAPI_FILE,     // /proc/wki/napi -> bounded admitted virtio queue diagnostics
    BLOCK_QUEUE_FILE,  // /proc/blockq -> bounded block request/latency counters
};

struct ProcNode {
    ProcNodeType type;
    uint64_t pid;  // physical backend identity; display names use the retained view
    uint64_t fd;
    bool thread_view;
};

struct ProcFileData {
    ProcFileData() = default;
    ~ProcFileData();
    ProcFileData(const ProcFileData&) = delete;
    auto operator=(const ProcFileData&) -> ProcFileData& = delete;
    ker::mod::sched::task::Task* target{};
    // Captured at binding; inherited Files and clones retain this exact view.
    // Empty is Host, independent of the worker reading the File later.
    ker::mod::sched::pid_ns::NamespaceRef pid_view{};
    uint64_t content_credential_generation{};
    ProcNode node;
    char* content;  // generated content (lazily allocated)
    size_t content_len;
    ker::mod::sys::Mutex limits_content_lock;
};

// Initialize and mount procfs at /proc
void procfs_init();

// Open a procfs path (relative to /proc mount)
auto procfs_open_path(const ker::mod::security::Snapshot& credentials, const char* path, int flags, int mode, int* result_out = nullptr,
                      bool require_directory = false) -> File*;
// Explicit request identity; compatibility wrapper above uses the current process PID.
auto procfs_open_path_as(const ker::mod::security::Snapshot& credentials, uint64_t caller_pid, const char* path, int flags, int mode,
                         int* result_out = nullptr, bool require_directory = false, ker::mod::sched::task::Task* caller_task = nullptr)
    -> File*;
// Clone only retained metadata/target identity. Lazy content is never copied.
auto procfs_open_lookup_as(const ker::mod::security::Snapshot& credentials, uint64_t caller_pid, const File* retained, int flags,
                           bool require_directory, int* result_out, uint64_t* target_generation) -> File*;
auto procfs_lookup_is_symlink(const File* file) -> bool;
auto procfs_lookup_is_fd_link(const File* file) -> bool;
// Duplicate the descriptor from the pinned process, retaining its actual File.
// May release a borrowed remote descriptor on failure; run outside publication.
auto procfs_open_fd_lookup_as(const ker::mod::security::Snapshot& credentials, uint64_t caller_pid, const File* retained, int* result_out,
                              uint64_t* target_generation) -> File*;
// No VFS calls or descriptor release; valid during final FD publication.
auto procfs_revalidate_open_result(const File* file, uint64_t target_generation) -> int;
// Metadata parser only; FileOperations still authorize data access per operation.
auto procfs_lookup_path(const char* path, int flags = 0) -> File*;
// Metadata parser for a retained request context; does not authorize data access.
// caller_task is retained by the caller and may be an unpublished private child.
// caller_pid is trusted physical canonical identity; parsed names use its view.
// Null Task is reserved for explicit trusted Host/boot/model observations.
auto procfs_lookup_path_as(const char* path, uint64_t caller_pid, int flags = 0, ker::mod::sched::task::Task* caller_task = nullptr,
                           int* result_out = nullptr) -> File*;

// Syntactic detection only: no Task/File lookup or authority is conferred.
auto procfs_path_is_fd_link(const char* path) -> bool;
// Open /proc/<pid>/fd/<fd> by retaining the referenced open file.
// result_out is 0 for a non-FD path, negative for an authoritative failure.
auto procfs_open_fd_link_path(const ker::mod::security::Snapshot& credentials, uint64_t caller_pid, const char* path, int* result_out,
                              ker::mod::sched::task::Task* caller_task = nullptr) -> File*;

// Fill stat metadata for an already-open procfs file.
auto procfs_fill_stat(File* f, Stat* statbuf, dev_t dev_id, uint64_t* target_generation = nullptr) -> int;
// Validate the retained backend and PID view for synthetic lookup membership.
// Bounded, nonallocating and safe under VFS publication. This grants no private
// content authority; each operation retains its own authorization/revalidation.
auto procfs_revalidate_lookup_result(const File* file) -> int;
// Revalidate only the retained owner's credential observation; public metadata
// does not confer private-content authority or require an ACTIVE target.
auto procfs_revalidate_metadata_result(const File* file, uint64_t target_generation) -> int;

// Per-read mount metadata, with one explicit credential snapshot and retained
// caller/File. Outside publication/content locks; no persistent content cache.
// Root/mount changes during collection return EAGAIN without output mutation.
// Null Task is reserved for explicit global-root boot/model observations.
auto procfs_read_mounts_as(ker::mod::sched::task::Task* task, const ker::mod::security::Snapshot& credentials, File* file, void* buffer,
                           size_t count, size_t offset) -> ssize_t;

// Same caller/context contract; SEEK_END measures the current listing outside
// content locks. Failed seeks preserve the File cursor and cached content.
auto procfs_lseek_mounts_as(ker::mod::sched::task::Task* task, const ker::mod::security::Snapshot& credentials, File* file, off_t offset,
                            int whence) -> off_t;

// Get procfs FileOperations
auto get_procfs_fops() -> FileOperations*;
// Component walks carry one caller snapshot across native and procfs symlinks.
// The caller retains file; the call may release a borrowed remote FD and must
// run outside VFS namespace publication.
auto procfs_readlink_as(File* file, char* buffer, size_t capacity, const ker::mod::security::Snapshot& credentials, uint64_t caller_pid,
                        uint64_t* target_generation = nullptr, bool allow_remote_cwd = false,
                        const ker::mod::sched::task::Task* caller_task = nullptr) -> ssize_t;
// Component traversal captures either text or an owned directory scope from
// one selected FD/cwd, never two reads. Scope success returns zero text bytes.
// Preserve outputs on failure; run outside publication and retain the origin
// metadata/generation through commit. Raw READLINK keeps text-only semantics.
auto procfs_readlink_view_as(File* file, char* buffer, size_t capacity, const ker::mod::security::Snapshot& credentials,
                             uint64_t caller_pid, DirectoryViewRef& directory_view, uint64_t* target_generation,
                             const ker::mod::sched::task::Task* caller_task = nullptr) -> ssize_t;
// Revalidate a successful staged read using its retained File. No VFS calls or
// borrowed-FD release; safe during namespace publication. SELF uses generation 0.
auto procfs_revalidate_readlink_result(File* file, uint64_t target_generation) -> int;

#ifdef WOS_SELFTEST
extern void (*mounts_snapshot_test_hook)(ker::mod::sched::task::Task*);
auto selftest_limits_authorization() -> bool;
auto selftest_limits_seek_authorization() -> bool;
auto selftest_fd_authorization() -> bool;
auto selftest_private_authorization() -> bool;
auto selftest_canonical_filesystem_root() -> bool;
auto selftest_public_snapshots() -> bool;
auto selftest_public_retained_subject() -> bool;
auto selftest_wki_identity_snapshots() -> bool;
auto selftest_global_authorization() -> bool;
auto selftest_cpu_observation() -> bool;
#endif
}  // namespace ker::vfs::procfs
