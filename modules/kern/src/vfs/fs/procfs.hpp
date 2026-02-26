#pragma once

#include <cstdint>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>

namespace ker::vfs::procfs {

// Procfs node types
enum class ProcNodeType : uint8_t {
    ROOT_DIR,      // /proc
    PID_DIR,       // /proc/<pid>
    SELF_LINK,     // /proc/self → /proc/<pid>
    EXE_LINK,      // /proc/<pid>/exe → exe_path
    STATUS_FILE,   // /proc/<pid>/status
    MOUNTS_FILE,   // /proc/mounts
    STAT_FILE,     // /proc/<pid>/stat
    CMDLINE_FILE,  // /proc/<pid>/cmdline
};

struct ProcNode {
    ProcNodeType type;
    uint64_t pid;  // relevant for PID_DIR, EXE_LINK, STATUS_FILE
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

// Get procfs FileOperations
auto get_procfs_fops() -> FileOperations*;

}  // namespace ker::vfs::procfs
