#pragma once

#include <cstddef>
#include <cstdint>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/stat.hpp>

namespace ker::vfs::procfs {

// Procfs node types
enum class ProcNodeType : uint8_t {
    ROOT_DIR,             // /proc
    PID_DIR,              // /proc/<pid>
    TASK_DIR,             // /proc/<pid>/task
    TASK_TID_DIR,         // /proc/<pid>/task/<tid>
    SELF_LINK,            // /proc/self -> /proc/<pid>
    EXE_LINK,             // /proc/<pid>/exe -> exe_path
    STATUS_FILE,          // /proc/<pid>/status
    MOUNTS_FILE,          // /proc/mounts
    STAT_FILE,            // /proc/<pid>/stat
    STATM_FILE,           // /proc/<pid>/statm
    CMDLINE_FILE,         // /proc/<pid>/cmdline
    MAPS_FILE,            // /proc/<pid>/maps
    UPTIME_FILE,          // /proc/uptime
    CPU_STAT_FILE,        // /proc/stat
    LOADAVG_FILE,         // /proc/loadavg
    MEMINFO_FILE,         // /proc/meminfo
    VERSION_FILE,         // /proc/version
    KPERF_FILE,           // /proc/kperf    -> drain kernel perf ring buffer as text events
    KWKISTAT_FILE,        // /proc/kwkistat -> recording-scoped WKI summary statistics
    KCPUSTAT_FILE,        // /proc/kcpustat -> per-CPU aggregate scheduler statistics
    KPERFCTL_FILE,        // /proc/kperfctl -> write "enable"/"disable" to control recording
    KCONTSTAT_FILE,       // /proc/kcontstat -> per-subsystem container statistics
    KIPCSTAT_FILE,        // /proc/kipcstat -> WKI IPC memory/queue snapshot
    WKI_LAUNCHER_FILE,    // /proc/<pid>/wki_launcher -> hostname of the node that launched the process
    WKI_RUNNER_FILE,      // /proc/<pid>/wki_runner   -> hostname of the node currently running the process
    WKI_REMOTE_PID_FILE,  // /proc/<pid>/wki_remote_pid -> remote execution PID for this process
    WKI_DIR,              // /proc/wki
    WKI_PEERS_FILE,       // /proc/wki/peers -> WKI topology rows
    WKI_NETDIAG_FILE,     // /proc/wki/netdiag -> network packet/listener/channel diagnostics
    WKI_PIPES_FILE,       // /proc/wki/pipes -> local pipe owner diagnostics
    MEMACC_DIR,           // /proc/memacc
    MEMACC_TRACK_DIR,     // /proc/memacc/track
    MEMACC_RECLAIM_DIR,   // /proc/memacc/reclaim
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
};

struct ProcNode {
    ProcNodeType type;
    uint64_t pid;  // process ID, task ID, or thread-group ID depending on node type
    bool thread_view;
};

struct ProcFileData {
    ProcNode node;
    char* content;  // generated content (lazily allocated)
    size_t content_len;
};

// Initialize and mount procfs at /proc
void procfs_init();

// Open a procfs path (relative to /proc mount)
auto procfs_open_path(const char* path, int flags, int mode) -> File*;

// Fill stat metadata for an already-open procfs file.
auto procfs_fill_stat(File* f, Stat* statbuf, dev_t dev_id) -> int;

// Get procfs FileOperations
auto get_procfs_fops() -> FileOperations*;

}  // namespace ker::vfs::procfs
