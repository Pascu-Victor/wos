#include <mod/io/serial/serial.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <vfs/fs/tmpfs.hpp>

#include "file.hpp"
#include "fs/tmpfs.hpp"
#include "vfs.hpp"

namespace ker::vfs {

void init();

int vfs_open(const char* path, int flags, int mode) {
    mod::io::serial::write("vfs_open: creating root file\n");
    // Resolve path using tmpfs simple path resolver (root-level names)
    ker::vfs::File* f = ker::vfs::tmpfs::tmpfs_open_path(path, flags, mode);
    auto current = ker::mod::sched::getCurrentTask();
    if (!current) {
        mod::io::serial::write("vfs_open: no current task\n");
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
    // Release the FD from the task's file descriptor table
    vfs_release_fd(t, fd);
    // Free the File descriptor object (just the handle/wrapper)
    // but keep the underlying tmpfs node (f->private_data) intact
    // so the file can be reopened later
    ker::mod::mm::dyn::kmalloc::free((void*)f);
    return 0;
}

ssize_t vfs_read(int fd, void* buf, size_t count) {
    ker::mod::sched::task::Task* t = ker::mod::sched::getCurrentTask();
    if (!t) return -1;
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (!f) return -1;
    ssize_t r = ker::vfs::tmpfs::tmpfs_read(f, buf, count, (size_t)f->pos);
    if (r > 0) f->pos += r;
    return r;
}

ssize_t vfs_write(int fd, const void* buf, size_t count) {
    ker::mod::sched::task::Task* t = ker::mod::sched::getCurrentTask();
    if (!t) return -1;
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (!f) return -1;
    ssize_t r = ker::vfs::tmpfs::tmpfs_write(f, buf, count, (size_t)f->pos);
    if (r > 0) f->pos += r;
    return r;
}

off_t vfs_lseek(int fd, off_t offset, int whence) {
    ker::mod::sched::task::Task* t = ker::mod::sched::getCurrentTask();
    if (!t) return -1;
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (!f) return -1;
    // Only support SEEK_SET/SEEK_CUR/SEEK_END on tmpfs root
    size_t nsize = ker::vfs::tmpfs::tmpfs_get_size(f);
    off_t newpos = f->pos;
    switch (whence) {
        case 0:  // SEEK_SET
            newpos = offset;
            break;
        case 1:  // SEEK_CUR
            newpos = f->pos + offset;
            break;
        case 2:  // SEEK_END
            newpos = (off_t)nsize + offset;
            break;
        default:
            return -1;
    }
    if (newpos < 0) return -1;
    f->pos = newpos;
    return f->pos;
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
    // Register tmpfs as a minimal root filesystem for now.
    ker::vfs::tmpfs::register_tmpfs();
}

}  // namespace ker::vfs
