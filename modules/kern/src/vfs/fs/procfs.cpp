#include "procfs.hpp"

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

// Helper to parse a PID from a path component
static auto parse_pid(const char* s) -> int64_t {
    if (s == nullptr || *s == '\0') return -1;
    int64_t val = 0;
    for (const char* p = s; *p; ++p) {
        if (*p < '0' || *p > '9') return -1;
        val = val * 10 + (*p - '0');
    }
    return val;
}

// Helper: int to decimal string
static void int_to_str(uint64_t val, char* buf, size_t bufsz) {
    if (bufsz == 0) return;
    char tmp[24];
    int pos = 0;
    if (val == 0) {
        tmp[pos++] = '0';
    } else {
        while (val > 0 && pos < 22) {
            tmp[pos++] = '0' + static_cast<char>(val % 10);
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

// Generate content for /proc/<pid>/status
static auto generate_status(uint64_t pid, char* buf, size_t bufsz) -> size_t {
    auto* task = ker::mod::sched::find_task_by_pid(pid);
    if (task == nullptr) return 0;

    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) buf[off++] = *s++;
    };
    auto append_int = [&](uint64_t v) {
        char tmp[24];
        int_to_str(v, tmp, sizeof(tmp));
        append(tmp);
    };

    append("Name:\t");
    append(task->exe_path[0] ? task->exe_path : "(unknown)");
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

// Generate content for /proc/mounts
static auto generate_mounts(char* buf, size_t bufsz) -> size_t {
    size_t off = 0;
    auto append = [&](const char* s) {
        while (*s && off < bufsz - 1) buf[off++] = *s++;
    };

    // Iterate mount table
    for (size_t i = 0; i < ker::vfs::get_mount_count(); ++i) {
        auto* m = ker::vfs::get_mount_at(i);
        if (m == nullptr) continue;
        append(m->fstype ? m->fstype : "none");
        append(" ");
        append(m->path ? m->path : "/");
        append(" ");
        append(m->fstype ? m->fstype : "none");
        append(" rw 0 0\n");
    }

    buf[off] = '\0';
    return off;
}

// Procfs stores generated content in the File's private_data as ProcFileData.

// --- FileOperations ---

static auto procfs_read(File* f, void* buf, size_t count, size_t offset) -> ssize_t {
    if (f == nullptr || f->private_data == nullptr) return -EINVAL;
    auto* pfd = static_cast<ProcFileData*>(f->private_data);

    // Lazily generate content
    if (pfd->content == nullptr) {
        constexpr size_t MAX_PROCFS_BUF = 4096;
        pfd->content = static_cast<char*>(ker::mod::mm::dyn::kmalloc::malloc(MAX_PROCFS_BUF));
        if (pfd->content == nullptr) return -ENOMEM;

        switch (pfd->node.type) {
            case ProcNodeType::STATUS_FILE:
                pfd->content_len = generate_status(pfd->node.pid, pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::MOUNTS_FILE:
                pfd->content_len = generate_mounts(pfd->content, MAX_PROCFS_BUF);
                break;
            case ProcNodeType::EXE_LINK: {
                auto* task = ker::mod::sched::find_task_by_pid(pfd->node.pid);
                if (task != nullptr && task->exe_path[0] != '\0') {
                    size_t len = strlen(task->exe_path);
                    memcpy(pfd->content, task->exe_path, len);
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

    if (offset >= pfd->content_len) return 0;
    size_t avail = pfd->content_len - offset;
    if (count > avail) count = avail;
    memcpy(buf, pfd->content + offset, count);
    return static_cast<ssize_t>(count);
}

static auto procfs_close(File* f) -> int {
    if (f == nullptr) return -EINVAL;
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

static auto procfs_lseek(File* f, off_t offset, int whence) -> off_t {
    if (f == nullptr) return -EINVAL;
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
    if (new_pos < 0) return -EINVAL;
    f->pos = new_pos;
    return new_pos;
}

static auto procfs_readlink(File* f, char* buf, size_t bufsz) -> ssize_t {
    if (f == nullptr || f->private_data == nullptr) return -EINVAL;
    auto* pfd = static_cast<ProcFileData*>(f->private_data);
    if (pfd->node.type != ProcNodeType::EXE_LINK && pfd->node.type != ProcNodeType::SELF_LINK) return -EINVAL;

    if (pfd->node.type == ProcNodeType::SELF_LINK) {
        auto* task = ker::mod::sched::get_current_task();
        if (task == nullptr) return -ESRCH;
        char tmp[24];
        int_to_str(task->pid, tmp, sizeof(tmp));
        char link[64] = "/proc/";
        size_t off = 6;
        for (const char* p = tmp; *p && off < sizeof(link) - 1; ++p, ++off) link[off] = *p;
        link[off] = '\0';
        size_t len = off;
        if (len > bufsz) len = bufsz;
        memcpy(buf, link, len);
        return static_cast<ssize_t>(len);
    }

    // EXE_LINK
    auto* task = ker::mod::sched::find_task_by_pid(pfd->node.pid);
    if (task == nullptr) return -ESRCH;
    size_t len = strlen(task->exe_path);
    if (len > bufsz) len = bufsz;
    memcpy(buf, task->exe_path, len);
    return static_cast<ssize_t>(len);
}

static auto procfs_readdir(File* f, DirEntry* buf, size_t count) -> int {
    // Minimal readdir — just return 0 for now (directory listing not critical)
    (void)f;
    (void)buf;
    (void)count;
    return 0;
}

static FileOperations procfs_fops_instance = {
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

auto get_procfs_fops() -> FileOperations* { return &procfs_fops_instance; }

// --- Open a procfs path ---

auto procfs_open_path(const char* path, int flags, int mode) -> File* {
    (void)flags;
    (void)mode;

    if (path == nullptr) return nullptr;

    // Skip leading /
    while (*path == '/') path++;

    auto* task = ker::mod::sched::get_current_task();
    uint64_t self_pid = task ? task->pid : 0;

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

    // /proc/<pid>
    // Find first / in path
    const char* slash = nullptr;
    for (const char* p = path; *p; ++p) {
        if (*p == '/') {
            slash = p;
            break;
        }
    }

    if (slash == nullptr) {
        // Just /proc/<pid>
        int64_t pid = parse_pid(path);
        if (pid < 0) return nullptr;
        // Check if task exists
        if (ker::mod::sched::find_task_by_pid(static_cast<uint64_t>(pid)) == nullptr) return nullptr;
        return make_file(ProcNodeType::PID_DIR, static_cast<uint64_t>(pid), true);
    }

    // /proc/<pid>/<subpath>
    char pid_str[24];
    size_t pid_len = static_cast<size_t>(slash - path);
    if (pid_len >= sizeof(pid_str)) return nullptr;
    memcpy(pid_str, path, pid_len);
    pid_str[pid_len] = '\0';
    int64_t pid = parse_pid(pid_str);
    if (pid < 0) return nullptr;
    if (ker::mod::sched::find_task_by_pid(static_cast<uint64_t>(pid)) == nullptr) return nullptr;

    const char* sub = slash + 1;
    if (strcmp(sub, "exe") == 0) {
        return make_file(ProcNodeType::EXE_LINK, static_cast<uint64_t>(pid), false);
    }
    if (strcmp(sub, "status") == 0) {
        return make_file(ProcNodeType::STATUS_FILE, static_cast<uint64_t>(pid), false);
    }

    return nullptr;  // Unknown procfs path
}

void procfs_init() {
    // Mount procfs at /proc
    ker::vfs::mount_filesystem("/proc", "procfs", nullptr);
}

}  // namespace ker::vfs::procfs
