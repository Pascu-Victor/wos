#include "sys_vfs.hpp"

#include <abi/callnums/vfs.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <mod/io/serial/serial.hpp>
#include <platform/sched/scheduler.hpp>
#include <vfs/epoll.hpp>
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

namespace ker::syscall::vfs {
using ker::abi::vfs::ops;

auto sys_vfs(uint64_t op_raw, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4) -> int64_t {
    ops op = static_cast<ops>(op_raw);
    switch (op) {
        case ops::open: {
            const char* path = reinterpret_cast<const char*>(a1);
            int flags = static_cast<int>(a2);
            int mode = static_cast<int>(a3);
            int fd = ker::vfs::vfs_open(path, flags, mode);
            if (fd < 0) {
                return static_cast<int64_t>(fd);
            }
            return static_cast<int64_t>(fd);
        }
        case ops::read: {
            int fd = static_cast<int>(a1);
            void* buf = reinterpret_cast<void*>(a2);
            auto len = static_cast<size_t>(a3);
            auto* actual_size = reinterpret_cast<size_t*>(a4);
            ssize_t ret = ker::vfs::vfs_read(fd, buf, len, actual_size);
            if (ret < 0) {
                return static_cast<int64_t>(ret);
            }
            return static_cast<int64_t>(ret);
        }
        case ops::write: {
            int fd = static_cast<int>(a1);
            const void* buf = reinterpret_cast<const void*>(a2);
            auto len = static_cast<size_t>(a3);
            auto* actual_size = reinterpret_cast<size_t*>(a4);
            ssize_t ret = ker::vfs::vfs_write(fd, buf, len, actual_size);
            if (ret < 0) {
                return static_cast<int64_t>(ret);
            }
            return static_cast<int64_t>(ret);
        }
        case ops::close: {
            int fd = static_cast<int>(a1);
            int ret = ker::vfs::vfs_close(fd);
            if (ret < 0) {
                return static_cast<int64_t>(ret);
            }
            return static_cast<int64_t>(ret);
        }
        case ops::lseek: {
            int fd = static_cast<int>(a1);
            auto offset = static_cast<off_t>(a2);
            int whence = static_cast<int>(a3);
            auto* new_offset = reinterpret_cast<off_t*>(a4);
            off_t ret = ker::vfs::vfs_lseek(fd, offset, whence);
            if (ret < 0) {
                return static_cast<int64_t>(ret);
            }
            *new_offset = ret;
            return 0;
        }
        case ops::isatty: {
            int fd = static_cast<int>(a1);
            bool is_tty = ker::vfs::vfs_isatty(fd);
            return is_tty ? 1 : 0;
        }
        case ops::read_dir_entries: {
            int fd = static_cast<int>(a1);
            void* buffer = reinterpret_cast<void*>(a2);
            auto max_size = static_cast<size_t>(a3);
            ssize_t ret = ker::vfs::vfs_read_dir_entries(fd, buffer, max_size);
            if (ret < 0) {
                return static_cast<int64_t>(ret);
            }
            return static_cast<int64_t>(ret);
        }
        case ops::mount: {
            const auto* source = reinterpret_cast<const char*>(a1);
            const auto* target = reinterpret_cast<const char*>(a2);
            const auto* fstype = reinterpret_cast<const char*>(a3);
            int ret = ker::vfs::vfs_mount(source, target, fstype);
            return static_cast<int64_t>(ret);
        }
        case ops::mkdir: {
            const auto* path = reinterpret_cast<const char*>(a1);
            int mode = static_cast<int>(a2);
            int ret = ker::vfs::vfs_mkdir(path, mode);
            return static_cast<int64_t>(ret);
        }
        case ops::readlink: {
            const auto* path = reinterpret_cast<const char*>(a1);
            auto* buf = reinterpret_cast<char*>(a2);
            auto bufsize = static_cast<size_t>(a3);
            ssize_t ret = ker::vfs::vfs_readlink(path, buf, bufsize);
            return static_cast<int64_t>(ret);
        }
        case ops::symlink: {
            const auto* target = reinterpret_cast<const char*>(a1);
            const auto* linkpath = reinterpret_cast<const char*>(a2);
            int ret = ker::vfs::vfs_symlink(target, linkpath);
            return static_cast<int64_t>(ret);
        }
        case ops::sendfile: {
            int outfd = static_cast<int>(a1);
            int infd = static_cast<int>(a2);
            auto* offset = reinterpret_cast<off_t*>(a3);
            auto count = static_cast<size_t>(a4);
            ssize_t ret = ker::vfs::vfs_sendfile(outfd, infd, offset, count);
            return static_cast<int64_t>(ret);
        }
        case ops::stat: {
            const auto* path = reinterpret_cast<const char*>(a1);
            auto* statbuf = reinterpret_cast<ker::vfs::stat*>(a2);
            int ret = ker::vfs::vfs_stat(path, statbuf);
            return static_cast<int64_t>(ret);
        }
        case ops::fstat: {
            int fd = static_cast<int>(a1);
            auto* statbuf = reinterpret_cast<ker::vfs::stat*>(a2);
            int ret = ker::vfs::vfs_fstat(fd, statbuf);
            return static_cast<int64_t>(ret);
        }
        case ops::umount: {
            const auto* target = reinterpret_cast<const char*>(a1);
            return static_cast<int64_t>(ker::vfs::vfs_umount(target));
        }
        case ops::dup: {
            int oldfd = static_cast<int>(a1);
            return static_cast<int64_t>(ker::vfs::vfs_dup(oldfd));
        }
        case ops::dup2: {
            int oldfd = static_cast<int>(a1);
            int newfd = static_cast<int>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_dup2(oldfd, newfd));
        }
        case ops::getcwd: {
            auto* buf = reinterpret_cast<char*>(a1);
            auto size = static_cast<size_t>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_getcwd(buf, size));
        }
        case ops::chdir: {
            const auto* path = reinterpret_cast<const char*>(a1);
            return static_cast<int64_t>(ker::vfs::vfs_chdir(path));
        }
        case ops::access: {
            const auto* path = reinterpret_cast<const char*>(a1);
            int mode = static_cast<int>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_access(path, mode));
        }
        case ops::unlink: {
            const auto* path = reinterpret_cast<const char*>(a1);
            return static_cast<int64_t>(ker::vfs::vfs_unlink(path));
        }
        case ops::rmdir: {
            const auto* path = reinterpret_cast<const char*>(a1);
            return static_cast<int64_t>(ker::vfs::vfs_rmdir(path));
        }
        case ops::rename: {
            const auto* oldpath = reinterpret_cast<const char*>(a1);
            const auto* newpath = reinterpret_cast<const char*>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_rename(oldpath, newpath));
        }
        case ops::chmod: {
            const auto* path = reinterpret_cast<const char*>(a1);
            int mode = static_cast<int>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_chmod(path, mode));
        }
        case ops::truncate: {
            int fd = static_cast<int>(a1);
            auto length = static_cast<off_t>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_ftruncate(fd, length));
        }
        case ops::pipe: {
            auto* pipefd = reinterpret_cast<int*>(a1);
            return static_cast<int64_t>(ker::vfs::vfs_pipe(pipefd));
        }
        case ops::pread: {
            int fd = static_cast<int>(a1);
            auto* buf = reinterpret_cast<void*>(a2);
            auto count = static_cast<size_t>(a3);
            auto offset = static_cast<off_t>(a4);
            return static_cast<int64_t>(ker::vfs::vfs_pread(fd, buf, count, offset));
        }
        case ops::pwrite: {
            int fd = static_cast<int>(a1);
            const auto* buf = reinterpret_cast<const void*>(a2);
            auto count = static_cast<size_t>(a3);
            auto offset = static_cast<off_t>(a4);
            return static_cast<int64_t>(ker::vfs::vfs_pwrite(fd, buf, count, offset));
        }
        case ops::fcntl: {
            int fd = static_cast<int>(a1);
            int cmd = static_cast<int>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_fcntl(fd, cmd, a3));
        }
        case ops::fchmod: {
            int fd = static_cast<int>(a1);
            int mode = static_cast<int>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_fchmod(fd, mode));
        }
        case ops::chown: {
            const auto* path = reinterpret_cast<const char*>(a1);
            auto owner = static_cast<uint32_t>(a2);
            auto group = static_cast<uint32_t>(a3);
            return static_cast<int64_t>(ker::vfs::vfs_chown(path, owner, group));
        }
        case ops::fchown: {
            int fd = static_cast<int>(a1);
            auto owner = static_cast<uint32_t>(a2);
            auto group = static_cast<uint32_t>(a3);
            return static_cast<int64_t>(ker::vfs::vfs_fchown(fd, owner, group));
        }
        case ops::faccessat: {
            int dirfd = static_cast<int>(a1);
            const auto* pathname = reinterpret_cast<const char*>(a2);
            int mode = static_cast<int>(a3);
            // Resolve dirfd-relative path
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) return -ESRCH;
            char resolved[512];
            int res = ker::vfs::vfs_resolve_dirfd(task, dirfd, pathname, resolved, sizeof(resolved));
            if (res < 0) return static_cast<int64_t>(res);
            return static_cast<int64_t>(ker::vfs::vfs_access(resolved, mode));
        }
        case ops::unlinkat: {
            int dirfd = static_cast<int>(a1);
            const auto* pathname = reinterpret_cast<const char*>(a2);
            int flags = static_cast<int>(a3);
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) return -ESRCH;
            char resolved[512];
            int res = ker::vfs::vfs_resolve_dirfd(task, dirfd, pathname, resolved, sizeof(resolved));
            if (res < 0) return static_cast<int64_t>(res);
            if (flags & 0x200) {  // AT_REMOVEDIR
                return static_cast<int64_t>(ker::vfs::vfs_rmdir(resolved));
            }
            return static_cast<int64_t>(ker::vfs::vfs_unlink(resolved));
        }
        case ops::renameat: {
            int olddirfd = static_cast<int>(a1);
            const auto* oldpath = reinterpret_cast<const char*>(a2);
            int newdirfd = static_cast<int>(a3);
            const auto* newpath = reinterpret_cast<const char*>(a4);
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) return -ESRCH;
            char resolved_old[512], resolved_new[512];
            int res = ker::vfs::vfs_resolve_dirfd(task, olddirfd, oldpath, resolved_old, sizeof(resolved_old));
            if (res < 0) return static_cast<int64_t>(res);
            res = ker::vfs::vfs_resolve_dirfd(task, newdirfd, newpath, resolved_new, sizeof(resolved_new));
            if (res < 0) return static_cast<int64_t>(res);
            return static_cast<int64_t>(ker::vfs::vfs_rename(resolved_old, resolved_new));
        }
        case ops::epoll_create: {
            int flags = static_cast<int>(a1);
            return static_cast<int64_t>(ker::vfs::epoll_create(flags));
        }
        case ops::epoll_ctl: {
            int epfd = static_cast<int>(a1);
            int op = static_cast<int>(a2);
            int fd = static_cast<int>(a3);
            auto* event = reinterpret_cast<ker::vfs::EpollEvent*>(a4);
            return static_cast<int64_t>(ker::vfs::epoll_ctl(epfd, op, fd, event));
        }
        case ops::epoll_pwait: {
            int epfd = static_cast<int>(a1);
            auto* events = reinterpret_cast<ker::vfs::EpollEvent*>(a2);
            int maxevents = static_cast<int>(a3);
            int timeout = static_cast<int>(static_cast<int64_t>(a4));
            return static_cast<int64_t>(ker::vfs::epoll_pwait(epfd, events, maxevents, timeout));
        }
        default:
            ker::vfs::vfs_debug_log("sys_vfs: unknown op\n");
            return static_cast<int64_t>(ENOSYS);
    }
}

}  // namespace ker::syscall::vfs
