#include "procfs.hpp"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sched/task.hpp>
#include <vfs/file.hpp>
#include <vfs/mount.hpp>
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

#include "platform/sched/scheduler.hpp"
#include "vfs/file_operations.hpp"

namespace ker::vfs::procfs {

namespace {

// Helper: int to decimal string
void int_to_str(uint64_t val, char* buf, size_t bufsz) {
    if (bufsz == 0) {
        return;
    }
    std::array<char, 24> tmp{};
    int pos = 0;
    if (val == 0) {
        tmp[pos++] = '0';
    } else {
        while (val > 0 && pos < 22) {
            tmp[pos++] = static_cast<char>('0' + static_cast<unsigned char>(val % 10));
            val /= 10;
        }
    }
    // reverse
    int len = pos;
    for (int i = 0; i < len && static_cast<size_t>(i) < bufsz - 1; ++i) {
        buf[i] = tmp[len - 1 - i];
    }
    buf[len < static_cast<int>(bufsz) ? len : static_cast<int>(bufsz - 1)] = '\0';
}

auto procfs_readdir(File* f, DirEntry* buf, size_t count) -> int {
    if (f == nullptr || f->private_data == nullptr) {
        return -1;
    }
    auto* pfd = static_cast<ProcFileData*>(f->private_data);

    // Synthetic . and ..
    if (count == 0) {
        buf->d_ino = 1;
        buf->d_off = 1;
        buf->d_reclen = sizeof(DirEntry);
        buf->d_type = DT_DIR;
        buf->d_name[0] = '.';
        buf->d_name[1] = '\0';
        return 0;
    }
    if (count == 1) {
        buf->d_ino = 1;
        buf->d_off = 2;
        buf->d_reclen = sizeof(DirEntry);
        buf->d_type = DT_DIR;
        buf->d_name[0] = '.';
        buf->d_name[1] = '.';
        buf->d_name[2] = '\0';
        return 0;
    }

    if (pfd->node.type == ProcNodeType::ROOT_DIR) {
        // /proc root: fixed entries then PID directories
        // index 2 = "self", 3 = "mounts", 4+ = active task PIDs
        if (count == 2) {
            buf->d_ino = 2;
            buf->d_off = 3;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_LNK;
            std::memcpy(buf->d_name.data(), "self", 5);
            return 0;
        }
        if (count == 3) {
            buf->d_ino = 3;
            buf->d_off = 4;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "mounts", 7);
            return 0;
        }
        // PID directories from active task list
        size_t pid_index = count - 4;
        uint32_t task_count = ker::mod::sched::get_active_task_count();
        if (pid_index >= task_count) {
            return -1;  // No more entries
        }
        auto* task = ker::mod::sched::get_active_task_at(static_cast<uint32_t>(pid_index));
        if (task == nullptr) {
            return -1;
        }
        buf->d_ino = task->pid + 100;
        buf->d_off = count + 1;
        buf->d_reclen = sizeof(DirEntry);
        buf->d_type = DT_DIR;
        int_to_str(task->pid, buf->d_name.data(), buf->d_name.size());
        return 0;
    }

    if (pfd->node.type == ProcNodeType::PID_DIR) {
        // /proc/<pid>: index 2 = "stat", 3 = "status", 4 = "cmdline", 5 = "exe"
        if (count == 2) {
            buf->d_ino = 10;
            buf->d_off = 3;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "stat", 5);
            return 0;
        }
        if (count == 3) {
            buf->d_ino = 11;
            buf->d_off = 4;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "status", 7);
            return 0;
        }
        if (count == 4) {
            buf->d_ino = 12;
            buf->d_off = 5;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_REG;
            std::memcpy(buf->d_name.data(), "cmdline", 8);
            return 0;
        }
        if (count == 5) {
            buf->d_ino = 13;
            buf->d_off = 6;
            buf->d_reclen = sizeof(DirEntry);
            buf->d_type = DT_LNK;
            std::memcpy(buf->d_name.data(), "exe", 4);
            return 0;
        }
        return -1;  // No more entries
    }

    return -1;
}

// Helper to parse a PID from a path component
auto parse_pid(const char* s) -> int64_t {
    if (s == nullptr || *s == '\0') {
        return -1;
    }
    int64_t val = 0;
    for (const char* p = s; *p != 0; ++p) {
        if (*p < '0' || *p > '9') {
            return -1;
        }
        val = (val * 10) + (*p - '0');
    }
    return val;
}

// Generate content for /proc/<pid>/status
auto generate_status(uint64_t pid, char* buf, size_t bufsz) -> size_t {
    auto* task = ker::mod::sched::find_task_by_pid(pid);
    if (task == nullptr) {
        return 0;
    }

    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };
    auto append_int = [&](uint64_t v) {
        std::array<char, 24> tmp{};
        int_to_str(v, tmp.data(), tmp.size());
        append(tmp.data());
    };

    append("Name:\t");
    append((task->exe_path[0] != 0) ? static_cast<const char*>(task->exe_path) : "(unknown)");
    append("\nPid:\t");
    append_int(task->pid);
    append("\nPPid:\t");
    append_int(task->parentPid);
    append("\nUid:\t");
    append_int(task->uid);
    append("\t");
    append_int(task->euid);
    append("\t");
    append_int(task->suid);
    append("\t");
    append_int(task->uid);
    append("\nGid:\t");
    append_int(task->gid);
    append("\t");
    append_int(task->egid);
    append("\t");
    append_int(task->sgid);
    append("\t");
    append_int(task->gid);
    append("\n");

    buf[off] = '\0';
    return off;
}

// Generate content for /proc/<pid>/stat (Linux-compatible format)
// Format: pid (comm) state ppid pgid sid tty tpgid flags minflt cminflt majflt cmajflt
//         utime stime cutime cstime priority nice nthreads itrealvalue starttime vsize rss ...
auto generate_stat(uint64_t pid, char* buf, size_t bufsz) -> size_t {
    auto* task = ker::mod::sched::find_task_by_pid(pid);
    if (task == nullptr) {
        return 0;
    }

    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };
    auto append_int = [&](uint64_t v) {
        std::array<char, 24> tmp{};
        int_to_str(v, tmp.data(), tmp.size());
        append(tmp.data());
    };

    // Extract comm (basename of exe_path)
    const auto* comm = static_cast<const char*>(task->exe_path);
    if (comm[0] != '\0') {
        const char* p = comm;
        while (*p != 0) {
            if (*p == '/') {
                comm = p + 1;
            }
            p++;
        }
    }
    if (comm[0] == '\0') {
        comm = (task->name != nullptr) ? task->name : "unknown";
    }

    // Determine state character
    char state = 'S';  // Default sleeping
    auto ts = task->state.load(std::memory_order_acquire);
    if (ts == ker::mod::sched::task::TaskState::DEAD || ts == ker::mod::sched::task::TaskState::EXITING || task->hasExited) {
        state = 'Z';
    }
    // Could refine: R for running, but S is a safe default

    // pid (comm) state ppid pgid sid tty_nr tpgid flags
    append_int(task->pid);
    append(" (");
    append(comm);
    append(") ");
    buf[off++] = state;
    append(" ");
    append_int(task->parentPid);  // ppid
    append(" ");
    append_int(task->pgid != 0 ? task->pgid : task->pid);  // pgrp
    append(" ");
    append_int(task->session_id != 0 ? task->session_id : task->pid);  // session
    append(" ");
    append_int(task->controlling_tty >= 0 ? static_cast<uint64_t>(task->controlling_tty) : 0);  // tty_nr
    append(" ");
    append_int(0);  // tpgid
    append(" ");
    append_int(0);  // flags
    append(" ");

    // minflt cminflt majflt cmajflt
    append("0 ");  // minflt
    append("0 ");  // cminflt
    append("0 ");  // majflt
    append("0 ");  // cmajflt

    // utime stime cutime cstime (in clock ticks, CLK_TCK=100, so 1 tick=10000us)
    append_int(task->user_time_us / 10000);  // utime
    append(" ");
    append_int(task->system_time_us / 10000);  // stime
    append(" ");
    append("0 ");  // cutime (children)
    append("0 ");  // cstime (children)

    // priority nice num_threads itrealvalue starttime
    append("20 ");                            // priority
    append("0 ");                             // nice
    append("1 ");                             // num_threads
    append("0 ");                             // itrealvalue
    append_int(task->start_time_us / 10000);  // starttime (in ticks since boot)
    append(" ");

    // vsize rss
    append("0 ");  // vsize
    append("0");   // rss

    append("\n");
    buf[off] = '\0';
    return off;
}

// Generate content for /proc/<pid>/cmdline (NUL-separated argv)
auto generate_cmdline(uint64_t pid, char* buf, size_t bufsz) -> size_t {
    auto* task = ker::mod::sched::find_task_by_pid(pid);
    if (task == nullptr) {
        return 0;
    }

    // Use exe_path as fallback (most processes won't have a saved argv)
    const auto* path = static_cast<const char*>(task->exe_path);
    if (path[0] == '\0') {
        path = (task->name != nullptr) ? task->name : "";
    }
    size_t len = strlen(path);
    if (len >= bufsz) {
        len = bufsz - 1;
    }
    memcpy(buf, path, len);
    buf[len] = '\0';  // NUL terminator (cmdline ends with NUL)
    return len + 1;   // Include the trailing NUL as part of content length
}

// Generate content for /proc/mounts
auto generate_mounts(char* buf, size_t bufsz) -> size_t {
    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) {
            buf[off++] = *s++;
        }
    };

    // Iterate mount table
    for (size_t i = 0; i < ker::vfs::get_mount_count(); ++i) {
        auto* m = ker::vfs::get_mount_at(i);
        if (m == nullptr) {
            continue;
        }
        append((m->fstype != nullptr) ? m->fstype : "none");
        append(" ");
        append((m->path != nullptr) ? m->path : "/");
        append(" ");
        append((m->fstype != nullptr) ? m->fstype : "none");
        append(" rw 0 0\n");
    }

    buf[off] = '\0';
    return off;
}

// Procfs stores generated content in the File's private_data as ProcFileData.

// --- FileOperations ---

auto procfs_read(File* f, void* buf, size_t count, size_t offset) -> ssize_t {
    if (f == nullptr || f->private_data == nullptr) {
        return -EINVAL;
    }
    auto* pfd = static_cast<ProcFileData*>(f->private_data);

    // Lazily generate content
    if (pfd->content == nullptr) {
        constexpr size_t MAX_PROCFS_BUF = 4096;
        pfd->content = static_cast<char*>(ker::mod::mm::dyn::kmalloc::malloc(MAX_PROCFS_BUF));
        if (pfd->content == nullptr) {
            return -ENOMEM;
        }

        switch (pfd->node.type) {
            case ProcNodeType::STATUS_FILE:
                pfd->content_len = generate_status(pfd->node.pid, pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::STAT_FILE:
                pfd->content_len = generate_stat(pfd->node.pid, pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::CMDLINE_FILE:
                pfd->content_len = generate_cmdline(pfd->node.pid, pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::MOUNTS_FILE:
                pfd->content_len = generate_mounts(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::EXE_LINK: {
                auto* task = ker::mod::sched::find_task_by_pid(pfd->node.pid);
                if (task != nullptr && task->exe_path[0] != '\0') {
                    size_t len = strlen(static_cast<const char*>(task->exe_path));
                    memcpy(pfd->content, static_cast<const char*>(task->exe_path), len);
                    pfd->content[len] = '\0';
                    pfd->content_len = len;
                } else {
                    pfd->content[0] = '\0';
                    pfd->content_len = 0;
                }
                break;
            }
            default:
                pfd->content[0] = '\0';
                pfd->content_len = 0;
                break;
        }
    }

    if (offset >= pfd->content_len) {
        return 0;
    }
    size_t avail = pfd->content_len - offset;
    count = std::min(count, avail);
    memcpy(buf, pfd->content + offset, count);
    return static_cast<ssize_t>(count);
}

auto procfs_close(File* f) -> int {
    if (f == nullptr) {
        return -EINVAL;
    }
    if (f->private_data != nullptr) {
        auto* pfd = static_cast<ProcFileData*>(f->private_data);
        if (pfd->content != nullptr) {
            ker::mod::mm::dyn::kmalloc::free(pfd->content);
        }
        delete pfd;
        f->private_data = nullptr;
    }
    return 0;
}

auto procfs_lseek(File* f, off_t offset, int whence) -> off_t {
    if (f == nullptr) {
        return -EINVAL;
    }
    auto* pfd = static_cast<ProcFileData*>(f->private_data);
    off_t new_pos = f->pos;
    switch (whence) {
        case 0:
            new_pos = offset;
            break;  // SEEK_SET
        case 1:
            new_pos = f->pos + offset;
            break;  // SEEK_CUR
        case 2:
            new_pos = static_cast<off_t>(pfd->content_len) + offset;
            break;  // SEEK_END
        default:
            return -EINVAL;
    }
    if (new_pos < 0) {
        return -EINVAL;
    }
    f->pos = new_pos;
    return new_pos;
}

auto procfs_readlink(File* f, char* buf, size_t bufsz) -> ssize_t {
    if (f == nullptr || f->private_data == nullptr) {
        return -EINVAL;
    }
    auto* pfd = static_cast<ProcFileData*>(f->private_data);
    if (pfd->node.type != ProcNodeType::EXE_LINK && pfd->node.type != ProcNodeType::SELF_LINK) {
        return -EINVAL;
    }

    if (pfd->node.type == ProcNodeType::SELF_LINK) {
        auto* task = ker::mod::sched::get_current_task();
        if (task == nullptr) {
            return -ESRCH;
        }
        std::array<char, 24> tmp{};
        int_to_str(task->pid, tmp.data(), tmp.size());
        std::array<char, 64> link{};
        memcpy(link.data(), "/proc/", 6);
        size_t off = 6;
        for (const char* p = tmp.data(); (*p != 0) && off < link.size() - 1; ++p, ++off) {
            link[off] = *p;
        }
        link[off] = '\0';
        size_t len = off;
        len = std::min(len, bufsz);
        memcpy(buf, link.data(), len);
        return static_cast<ssize_t>(len);
    }

    // EXE_LINK
    auto* task = ker::mod::sched::find_task_by_pid(pfd->node.pid);
    if (task == nullptr) {
        return -ESRCH;
    }
    size_t len = strlen(static_cast<const char*>(task->exe_path));
    len = std::min(len, bufsz);
    memcpy(buf, static_cast<const char*>(task->exe_path), len);
    return static_cast<ssize_t>(len);
}

FileOperations procfs_fops_instance = {
    .vfs_open = nullptr,
    .vfs_close = procfs_close,
    .vfs_read = procfs_read,
    .vfs_write = nullptr,
    .vfs_lseek = procfs_lseek,
    .vfs_isatty = nullptr,
    .vfs_readdir = procfs_readdir,
    .vfs_readlink = procfs_readlink,
    .vfs_truncate = nullptr,
    .vfs_poll_check = nullptr,
};

}  // namespace

auto get_procfs_fops() -> FileOperations* { return &procfs_fops_instance; }

// --- Open a procfs path ---

auto procfs_open_path(const char* path, int flags, int mode) -> File* {
    (void)flags;
    (void)mode;

    if (path == nullptr) {
        return nullptr;
    }

    // Skip leading /
    while (*path == '/') {
        path++;
    }

    auto* task = ker::mod::sched::get_current_task();
    uint64_t self_pid = (task != nullptr) ? task->pid : 0;

    auto make_file = [](ProcNodeType type, uint64_t pid, bool is_dir) -> File* {
        auto* pfd = new ProcFileData;
        pfd->node.type = type;
        pfd->node.pid = pid;
        pfd->content = nullptr;
        pfd->content_len = 0;

        auto* f = new File;
        f->private_data = pfd;
        f->fd = -1;
        f->pos = 0;
        f->is_directory = is_dir;
        f->fs_type = FSType::PROCFS;
        f->refcount = 1;
        f->fops = nullptr;  // set by caller
        return f;
    };

    // /proc (root)
    if (*path == '\0') {
        return make_file(ProcNodeType::ROOT_DIR, 0, true);
    }

    // /proc/mounts
    if (strcmp(path, "mounts") == 0) {
        return make_file(ProcNodeType::MOUNTS_FILE, 0, false);
    }

    // /proc/self → symlink to /proc/<pid>
    if (strcmp(path, "self") == 0) {
        return make_file(ProcNodeType::SELF_LINK, self_pid, false);
    }
    // /proc/self/exe
    if (strcmp(path, "self/exe") == 0) {
        return make_file(ProcNodeType::EXE_LINK, self_pid, false);
    }
    // /proc/self/status
    if (strcmp(path, "self/status") == 0) {
        return make_file(ProcNodeType::STATUS_FILE, self_pid, false);
    }
    // /proc/self/stat
    if (strcmp(path, "self/stat") == 0) {
        return make_file(ProcNodeType::STAT_FILE, self_pid, false);
    }
    // /proc/self/cmdline
    if (strcmp(path, "self/cmdline") == 0) {
        return make_file(ProcNodeType::CMDLINE_FILE, self_pid, false);
    }

    // /proc/<pid>
    // Find first / in path
    const char* slash = nullptr;
    for (const char* p = path; *p != 0; ++p) {
        if (*p == '/') {
            slash = p;
            break;
        }
    }

    if (slash == nullptr) {
        // Just /proc/<pid>
        int64_t pid = parse_pid(path);
        if (pid < 0) {
            return nullptr;
        }
        // Check if task exists
        if (ker::mod::sched::find_task_by_pid(static_cast<uint64_t>(pid)) == nullptr) {
            return nullptr;
        }
        return make_file(ProcNodeType::PID_DIR, static_cast<uint64_t>(pid), true);
    }

    // /proc/<pid>/<subpath>
    std::array<char, 24> pid_str{};
    auto pid_len = static_cast<size_t>(slash - path);
    if (pid_len >= pid_str.size()) {
        return nullptr;
    }
    memcpy(pid_str.data(), path, pid_len);
    pid_str[pid_len] = '\0';
    int64_t pid = parse_pid(pid_str.data());
    if (pid < 0) {
        return nullptr;
    }
    if (ker::mod::sched::find_task_by_pid(static_cast<uint64_t>(pid)) == nullptr) {
        return nullptr;
    }

    const char* sub = slash + 1;
    if (strcmp(sub, "exe") == 0) {
        return make_file(ProcNodeType::EXE_LINK, static_cast<uint64_t>(pid), false);
    }
    if (strcmp(sub, "status") == 0) {
        return make_file(ProcNodeType::STATUS_FILE, static_cast<uint64_t>(pid), false);
    }
    if (strcmp(sub, "stat") == 0) {
        return make_file(ProcNodeType::STAT_FILE, static_cast<uint64_t>(pid), false);
    }
    if (strcmp(sub, "cmdline") == 0) {
        return make_file(ProcNodeType::CMDLINE_FILE, static_cast<uint64_t>(pid), false);
    }

    return nullptr;  // Unknown procfs path
}

void procfs_init() {
    // Mount procfs at /proc
    ker::vfs::mount_filesystem("/proc", "procfs", nullptr);
}

}  // namespace ker::vfs::procfs
