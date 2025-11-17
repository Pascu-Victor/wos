#include "sys_vfs.hpp"

#include <abi/callnums/vfs.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <mod/io/serial/serial.hpp>
#include <vfs/vfs.hpp>

namespace ker::syscall::vfs {
using ker::abi::vfs::ops;

auto sys_vfs(uint64_t op_raw, uint64_t a1, uint64_t a2, uint64_t a3) -> uint64_t {
    ops op = static_cast<ops>(op_raw);
    switch (op) {
        case ops::open: {
            const char* path = reinterpret_cast<const char*>(a1);
            int flags = static_cast<int>(a2);
            int mode = static_cast<int>(a3);
            int fd = ker::vfs::vfs_open(path, flags, mode);
            if (fd < 0) {
                return static_cast<uint64_t>(-fd);
            }
            return static_cast<uint64_t>(fd);
        }
        case ops::read: {
            int fd = static_cast<int>(a1);
            void* buf = reinterpret_cast<void*>(a2);
            auto len = static_cast<size_t>(a3);
            ssize_t ret = ker::vfs::vfs_read(fd, buf, len);
            if (ret < 0) {
                return static_cast<uint64_t>(-ret);
            }
            return static_cast<uint64_t>(ret);
        }
        case ops::write: {
            int fd = static_cast<int>(a1);
            const void* buf = reinterpret_cast<const void*>(a2);
            auto len = static_cast<size_t>(a3);
            ssize_t ret = ker::vfs::vfs_write(fd, buf, len);
            if (ret < 0) {
                return static_cast<uint64_t>(-ret);
            }
            return static_cast<uint64_t>(ret);
        }
        case ops::close: {
            int fd = static_cast<int>(a1);
            int ret = ker::vfs::vfs_close(fd);
            if (ret < 0) {
                return static_cast<uint64_t>(-ret);
            }
            return static_cast<uint64_t>(ret);
        }
        case ops::lseek: {
            int fd = static_cast<int>(a1);
            auto offset = static_cast<off_t>(a2);
            int whence = static_cast<int>(a3);
            off_t ret = ker::vfs::vfs_lseek(fd, offset, whence);
            if (ret < 0) {
                return static_cast<uint64_t>(-ret);
            }
            return static_cast<uint64_t>(ret);
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
                return static_cast<uint64_t>(-ret);
            }
            return static_cast<uint64_t>(ret);
        }
        default:
            mod::io::serial::write("sys_vfs: unknown op\n");
            return static_cast<uint64_t>(ENOSYS);
    }
}

}  // namespace ker::syscall::vfs
