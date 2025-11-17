#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <string_view>
#include <vfs/fs/devfs.hpp>
#include <vfs/fs/fat32.hpp>
#include <vfs/fs/tmpfs.hpp>
#include <vfs/mount.hpp>

#include "file.hpp"
#include "fs/devfs.hpp"
#include "fs/fat32.hpp"
#include "fs/tmpfs.hpp"
#include "platform/mm/dyn/kmalloc.hpp"
#include "vfs.hpp"

namespace ker::vfs {

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

    auto* current = ker::mod::sched::getCurrentTask();
    if (current == nullptr) {
        mod::io::serial::write("vfs_open: no current task\n");
        return -1;
    }

    // Find the mount point for this path
    MountPoint* mount = find_mount_point(pathBuffer);
    if (mount == nullptr) {
        mod::io::serial::write("vfs_open: no mount point found for path\n");
        return -1;
    }

    ker::vfs::File* f = nullptr;

    // Route to the appropriate filesystem driver based on mount point
    switch (mount->fs_type) {
        case FSType::DEVFS:
            f = ker::vfs::devfs::devfs_open_path(pathBuffer, flags, mode);
            if (f != nullptr) {
                f->fops = ker::vfs::devfs::get_devfs_fops();
                f->fs_type = FSType::DEVFS;
            }
            break;
        case FSType::FAT32:
            f = ker::vfs::fat32::fat32_open_path(pathBuffer, flags, mode);
            if (f != nullptr) {
                f->fops = ker::vfs::fat32::get_fat32_fops();
                f->fs_type = FSType::FAT32;
            }
            break;
        case FSType::TMPFS:
            f = ker::vfs::tmpfs::tmpfs_open_path(pathBuffer, flags, mode);
            if (f != nullptr) {
                f->fops = ker::vfs::tmpfs::get_tmpfs_fops();
                f->fs_type = FSType::TMPFS;
            }
            break;
        default:
            mod::io::serial::write("vfs_open: unknown filesystem type\n");
            return -1;
    }

    if (f == nullptr) {
        mod::io::serial::write("vfs_open: failed to open file\n");
        return -1;
    }

    int fd = vfs_alloc_fd(current, f);
    if (fd < 0) {
        return -1;
    }
    return fd;
}

auto vfs_close(int fd) -> int {
    // Release FD from current task
    ker::mod::sched::task::Task* t = ker::mod::sched::getCurrentTask();
    if (t == nullptr) {
        return -1;
    }
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (f == nullptr) {
        return -1;
    }

    // Decrement reference count
    f->refcount--;

    // Release the FD from the task's file descriptor table
    vfs_release_fd(t, fd);

    // Only call close and free if no more references
    if (f->refcount <= 0) {
        if ((f->fops != nullptr) && (f->fops->vfs_close != nullptr)) {
            f->fops->vfs_close(f);
        }
        // Free the File descriptor object (just the handle/wrapper)
        // but keep the underlying fs node (f->private_data) intact
        // so the file can be reopened later
        ker::mod::mm::dyn::kmalloc::free((void*)f);
    }

    return 0;
}

auto vfs_read(int fd, void* buf, size_t count) -> ssize_t {
    ker::mod::sched::task::Task* t = ker::mod::sched::getCurrentTask();
    if (t == nullptr) {
        return -1;
    }
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (f == nullptr) {
        return -1;
    }
    if ((f->fops == nullptr) || (f->fops->vfs_read == nullptr)) {
        return -1;
    }
    ssize_t r = f->fops->vfs_read(f, buf, count, (size_t)f->pos);
    if (r > 0) {
        f->pos += r;
    }
    return r;
}

auto vfs_write(int fd, const void* buf, size_t count) -> ssize_t {
    ker::mod::sched::task::Task* t = ker::mod::sched::getCurrentTask();
    if (t == nullptr) {
        return -1;
    }
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (f == nullptr) {
        return -1;
    }
    if ((f->fops == nullptr) || (f->fops->vfs_write == nullptr)) {
        return -1;
    }
    ssize_t r = f->fops->vfs_write(f, buf, count, (size_t)f->pos);
    if (r > 0) {
        f->pos += r;
    }
    return r;
}

auto vfs_lseek(int fd, off_t offset, int whence) -> off_t {
    ker::mod::sched::task::Task* t = ker::mod::sched::getCurrentTask();
    if (t == nullptr) {
        return -1;
    }
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (f == nullptr) {
        return -1;
    }
    if ((f->fops == nullptr) || (f->fops->vfs_lseek == nullptr)) {
        return -1;
    }
    return f->fops->vfs_lseek(f, offset, whence);
}

auto vfs_alloc_fd(ker::mod::sched::task::Task* task, struct File* file) -> int {
    if ((task == nullptr) || (file == nullptr)) {
        return -1;
    }
    for (unsigned i = 0; i < ker::mod::sched::task::Task::FD_TABLE_SIZE; ++i) {
        if (task->fds[i] == nullptr) {
            task->fds[i] = file;
            file->fd = (int)i;
            return (int)i;
        }
    }
    return -1;  // EMFILE (too many open files)
}

auto vfs_get_file(ker::mod::sched::task::Task* task, int fd) -> struct File* {
    if (task == nullptr) {
        return nullptr;
    }
    if (fd < 0 || (unsigned)fd >= ker::mod::sched::task::Task::FD_TABLE_SIZE) {
        return nullptr;
    }
    return reinterpret_cast<struct File*>(task->fds[fd]);
}

auto vfs_release_fd(ker::mod::sched::task::Task* task, int fd) -> int {
    if (task == nullptr) {
        return -1;
    }
    if (fd < 0 || (unsigned)fd >= ker::mod::sched::task::Task::FD_TABLE_SIZE) {
        return -1;
    }
    task->fds[fd] = nullptr;
    return 0;
}

auto vfs_isatty(int fd) -> bool {
    ker::mod::sched::task::Task* t = ker::mod::sched::getCurrentTask();
    if (t == nullptr) {
        return false;
    }
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (f == nullptr) {
        return false;
    }
    if ((f->fops == nullptr) || (f->fops->vfs_isatty == nullptr)) {
        return false;
    }
    return f->fops->vfs_isatty(f);
}

auto vfs_read_dir_entries(int fd, void* buffer, size_t max_size) -> ssize_t {
    ker::mod::sched::task::Task* t = ker::mod::sched::getCurrentTask();
    if (t == nullptr) {
        return -1;
    }
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (f == nullptr) {
        return -1;
    }

    // Check if this is a directory
    if (!f->is_directory) {
        return -ENOTDIR;
    }

    if ((f->fops == nullptr) || (f->fops->vfs_readdir == nullptr)) {
        return -ENOSYS;
    }

    // Buffer must be large enough for at least one DirEntry
    if (buffer == nullptr || max_size < sizeof(DirEntry)) {
        return -EINVAL;
    }

    auto* entries = static_cast<DirEntry*>(buffer);
    size_t max_entries = max_size / sizeof(DirEntry);
    size_t entries_read = 0;

    // Read directory entries using the current position as index
    size_t start_index = static_cast<size_t>(f->pos);

    for (size_t i = 0; i < max_entries; ++i) {
        int ret = f->fops->vfs_readdir(f, &entries[i], start_index + i);
        if (ret != 0) {
            // End of directory or error
            break;
        }
        entries_read++;
    }

    // Update file position
    f->pos += static_cast<off_t>(entries_read);

    return static_cast<ssize_t>(entries_read * sizeof(DirEntry));
}

void init() {
    mod::io::serial::write("vfs: init\n");
    // Register tmpfs as a minimal root filesystem
    ker::vfs::tmpfs::register_tmpfs();
    // Mount tmpfs at root
    mount_filesystem("/", "tmpfs", nullptr);

    // Register FAT32 driver (will be mounted when a disk is available)
    ker::vfs::fat32::register_fat32();

    // Register and mount devfs for device files
    ker::vfs::devfs::devfs_init();
    mount_filesystem("/dev", "devfs", nullptr);
}

}  // namespace ker::vfs
