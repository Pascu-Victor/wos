#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <string_view>
#include <vfs/fs/fat32.hpp>
#include <vfs/fs/tmpfs.hpp>

#include "file.hpp"
#include "fs/fat32.hpp"
#include "fs/tmpfs.hpp"
#include "vfs.hpp"

namespace ker::vfs {

void init();

// Function to mark FAT32 as mounted (called from main after successful mount)
namespace {
bool fat32_mounted = false;
}

void set_fat32_mounted(bool mounted) { fat32_mounted = mounted; }

namespace {
// Check if path starts with a FAT32 mount point
auto is_fat32_path(const char* path) -> bool {
    // For now, only support /mnt/ and /fat32/ as FAT32 mount points
    if (path == nullptr) {
        return false;
    }
    if (path[0] != '/') {
        return false;
    }
    return (path[1] == 'm' && path[2] == 'n' && path[3] == 't' && path[4] == '/') ||
           (path[1] == 'f' && path[2] == 'a' && path[3] == 't' && path[4] == '3' && path[5] == '2' && path[6] == '/');
}
}  // namespace

auto vfs_open(std::string_view path, int flags, int mode) -> int {
    mod::io::serial::write("vfs_open: opening file\n");

    // Convert string_view to null-terminated string
    constexpr size_t MAX_PATH_LEN = 512;
    char pathBuffer[MAX_PATH_LEN];
    if (path.size() >= MAX_PATH_LEN) {
        return -1;  // Path too long
    }
    std::memcpy(pathBuffer, path.data(), path.size());
    pathBuffer[path.size()] = '\0';

    auto current = ker::mod::sched::getCurrentTask();
    if (!current) {
        mod::io::serial::write("vfs_open: no current task\n");
        return -1;
    }

    ker::vfs::File* f = nullptr;

    // Route to the appropriate filesystem driver
    if (fat32_mounted && is_fat32_path(pathBuffer)) {
        // FAT32 path
        f = ker::vfs::fat32::fat32_open_path(pathBuffer, flags, mode);
        if (f) {
            f->fops = ker::vfs::fat32::get_fat32_fops();
        }
    } else {
        // Default to tmpfs
        f = ker::vfs::tmpfs::tmpfs_open_path(pathBuffer, flags, mode);
        if (f) {
            f->fops = ker::vfs::tmpfs::get_tmpfs_fops();
        }
    }

    if (!f) {
        mod::io::serial::write("vfs_open: failed to open file\n");
        return -1;
    }

    int fd = vfs_alloc_fd(current, f);
    if (fd < 0) return -1;
    return fd;
}

int vfs_close(int fd) {
    // Release FD from current task
    ker::mod::sched::task::Task* t = ker::mod::sched::getCurrentTask();
    if (!t) return -1;
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (!f) return -1;
    if (f->fops && f->fops->vfs_close) {
        f->fops->vfs_close(f);
    }
    // Release the FD from the task's file descriptor table
    vfs_release_fd(t, fd);
    // Free the File descriptor object (just the handle/wrapper)
    // but keep the underlying fs node (f->private_data) intact
    // so the file can be reopened later
    ker::mod::mm::dyn::kmalloc::free((void*)f);
    return 0;
}

ssize_t vfs_read(int fd, void* buf, size_t count) {
    ker::mod::sched::task::Task* t = ker::mod::sched::getCurrentTask();
    if (!t) return -1;
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (!f) return -1;
    if (!f->fops || !f->fops->vfs_read) return -1;
    ssize_t r = f->fops->vfs_read(f, buf, count, (size_t)f->pos);
    if (r > 0) f->pos += r;
    return r;
}

ssize_t vfs_write(int fd, const void* buf, size_t count) {
    ker::mod::sched::task::Task* t = ker::mod::sched::getCurrentTask();
    if (!t) return -1;
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (!f) return -1;
    if (!f->fops || !f->fops->vfs_write) return -1;
    ssize_t r = f->fops->vfs_write(f, buf, count, (size_t)f->pos);
    if (r > 0) f->pos += r;
    return r;
}

off_t vfs_lseek(int fd, off_t offset, int whence) {
    ker::mod::sched::task::Task* t = ker::mod::sched::getCurrentTask();
    if (!t) return -1;
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (!f) return -1;
    if (!f->fops || !f->fops->vfs_lseek) return -1;
    return f->fops->vfs_lseek(f, offset, whence);
}

int vfs_alloc_fd(ker::mod::sched::task::Task* task, struct File* file) {
    if (!task || !file) return -1;
    for (unsigned i = 0; i < ker::mod::sched::task::Task::FD_TABLE_SIZE; ++i) {
        if (task->fds[i] == nullptr) {
            task->fds[i] = file;
            file->fd = (int)i;
            return (int)i;
        }
    }
    return -1;  // EMFILE (too many open files)
}

struct File* vfs_get_file(ker::mod::sched::task::Task* task, int fd) {
    if (!task) return nullptr;
    if (fd < 0 || (unsigned)fd >= ker::mod::sched::task::Task::FD_TABLE_SIZE) return nullptr;
    return reinterpret_cast<struct File*>(task->fds[fd]);
}

int vfs_release_fd(ker::mod::sched::task::Task* task, int fd) {
    if (!task) return -1;
    if (fd < 0 || (unsigned)fd >= ker::mod::sched::task::Task::FD_TABLE_SIZE) return -1;
    task->fds[fd] = nullptr;
    return 0;
}

void init() {
    mod::io::serial::write("vfs: init\n");
    // Register tmpfs as a minimal root filesystem
    ker::vfs::tmpfs::register_tmpfs();
    // Register FAT32 driver (will be mounted when a disk is available)
    ker::vfs::fat32::register_fat32();
}

}  // namespace ker::vfs
