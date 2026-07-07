#include "sys_vfs.hpp"

#include <abi/callnums/vfs.h>
#include <bits/off_t.h>
#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/usercopy.hpp>
#include <vfs/epoll.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/fs/devfs.hpp>
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

namespace ker::syscall::vfs {
using ker::abi::vfs::ops;

namespace {
template <typename T>
auto copy_value_to_user(T* user_ptr, T value) -> int {
    if (user_ptr == nullptr) {
        return 0;
    }

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || task->pagemap == nullptr) {
        return -EFAULT;
    }

    return ker::mod::sys::usercopy::copy_value_to_task(*task, reinterpret_cast<uint64_t>(user_ptr), value) ? 0 : -EFAULT;
}

auto copy_buffer_to_user(void* user_ptr, const void* src, size_t size) -> int {
    if (size == 0) {
        return 0;
    }
    if (user_ptr == nullptr || src == nullptr) {
        return -EFAULT;
    }

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || task->pagemap == nullptr) {
        return -EFAULT;
    }

    return ker::mod::sys::usercopy::copy_to_task(*task, reinterpret_cast<uint64_t>(user_ptr), src, size) ? 0 : -EFAULT;
}

template <typename T>
auto copy_value_from_user(const T* user_ptr, T* out) -> int {
    if (user_ptr == nullptr || out == nullptr) {
        return -EFAULT;
    }

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || task->pagemap == nullptr) {
        return -EFAULT;
    }

    return ker::mod::sys::usercopy::copy_value_from_task(*task, reinterpret_cast<uint64_t>(user_ptr), *out) ? 0 : -EFAULT;
}

auto copy_stat_result_to_user(int result, ker::vfs::Stat* user_statbuf, const ker::vfs::Stat& kernel_statbuf) -> int64_t {
    if (result < 0) {
        return static_cast<int64_t>(result);
    }
    if (user_statbuf == nullptr) {
        return -EFAULT;
    }
    if (int const COPY_RET = copy_value_to_user(user_statbuf, kernel_statbuf); COPY_RET < 0) {
        return static_cast<int64_t>(COPY_RET);
    }
    return static_cast<int64_t>(result);
}

auto copy_statvfs_result_to_user(int result, ker::vfs::Statvfs* user_buf, const ker::vfs::Statvfs& kernel_buf) -> int64_t {
    if (result < 0) {
        return static_cast<int64_t>(result);
    }
    if (user_buf == nullptr) {
        return -EFAULT;
    }
    if (int const COPY_RET = copy_value_to_user(user_buf, kernel_buf); COPY_RET < 0) {
        return static_cast<int64_t>(COPY_RET);
    }
    return static_cast<int64_t>(result);
}

auto copy_wki_rule_to_user(uint32_t index, char* prefix_buf, size_t prefix_buf_size, uint32_t* route_out, bool default_rules) -> int64_t {
    std::array<char, ker::mod::sched::task::Task::CWD_MAX> kernel_prefix{};
    uint32_t route = 0;
    char* prefix_arg = prefix_buf != nullptr ? kernel_prefix.data() : nullptr;
    size_t const PREFIX_ARG_SIZE = prefix_buf != nullptr ? std::min(prefix_buf_size, kernel_prefix.size()) : static_cast<size_t>(0);
    uint32_t* route_arg = route_out != nullptr ? &route : nullptr;

    int const RET = default_rules ? ker::vfs::vfs_wki_default_rule_get(index, prefix_arg, PREFIX_ARG_SIZE, route_arg)
                                  : ker::vfs::vfs_wki_rule_get(index, prefix_arg, PREFIX_ARG_SIZE, route_arg);
    if (RET < 0) {
        return static_cast<int64_t>(RET);
    }

    if (prefix_buf != nullptr) {
        size_t const COPY_SIZE = static_cast<size_t>(RET) + 1;
        if (int const COPY_RET = copy_buffer_to_user(prefix_buf, kernel_prefix.data(), COPY_SIZE); COPY_RET < 0) {
            return static_cast<int64_t>(COPY_RET);
        }
    }
    if (int const COPY_RET = copy_value_to_user(route_out, route); COPY_RET < 0) {
        return static_cast<int64_t>(COPY_RET);
    }
    return static_cast<int64_t>(RET);
}
}  // namespace

auto sys_vfs(uint64_t op_raw, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4, uint64_t a5) -> int64_t {
    ops op = static_cast<ops>(op_raw);
    switch (op) {
        case ops::OPEN: {
            const char* path = reinterpret_cast<const char*>(a1);
            if (path == nullptr) {
                return -EFAULT;
            }
            int const FLAGS = static_cast<int>(a2);
            int const MODE = static_cast<int>(a3);
            int const FD = ker::vfs::vfs_open(path, FLAGS, MODE);
            if (FD < 0) {
                return static_cast<int64_t>(FD);
            }
            return static_cast<int64_t>(FD);
        }
        case ops::OPENAT: {
            int const DIRFD = static_cast<int>(a1);
            const auto* pathname = reinterpret_cast<const char*>(a2);
            if (pathname == nullptr) {
                return -EFAULT;
            }
            int const FLAGS = static_cast<int>(a3);
            int const MODE = static_cast<int>(a4);
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return -ESRCH;
            }
            if (DIRFD == ker::vfs::AT_FDCWD) {
                return static_cast<int64_t>(ker::vfs::vfs_open(pathname, FLAGS, MODE));
            }
            std::array<char, 512> resolved{};
            int const RES = ker::vfs::vfs_resolve_dirfd(task, DIRFD, pathname, resolved.data(), resolved.size());
            if (RES < 0) {
                return static_cast<int64_t>(RES);
            }
            return static_cast<int64_t>(ker::vfs::vfs_open(resolved.data(), FLAGS, MODE));
        }
        case ops::READ: {
            int const FD = static_cast<int>(a1);
            void* buf = reinterpret_cast<void*>(a2);
            auto len = static_cast<size_t>(a3);
            auto* actual_size = reinterpret_cast<size_t*>(a4);
            size_t actual = 0;
            ssize_t const RET = ker::vfs::vfs_read(FD, buf, len, actual_size != nullptr ? &actual : nullptr);
            if (RET < 0) {
                return static_cast<int64_t>(RET);
            }
            if (int const COPY_RET = copy_value_to_user(actual_size, actual); COPY_RET < 0) {
                return COPY_RET;
            }
            return static_cast<int64_t>(RET);
        }
        case ops::WRITE: {
            int const FD = static_cast<int>(a1);
            const void* buf = reinterpret_cast<const void*>(a2);
            auto len = static_cast<size_t>(a3);
            auto* actual_size = reinterpret_cast<size_t*>(a4);
            size_t actual = 0;
            ssize_t const RET = ker::vfs::vfs_write(FD, buf, len, actual_size != nullptr ? &actual : nullptr);
            if (RET < 0) {
                return static_cast<int64_t>(RET);
            }
            if (int const COPY_RET = copy_value_to_user(actual_size, actual); COPY_RET < 0) {
                return COPY_RET;
            }
            return static_cast<int64_t>(RET);
        }
        case ops::CLOSE: {
            int const FD = static_cast<int>(a1);
            int const RET = ker::vfs::vfs_close(FD);
            if (RET < 0) {
                return static_cast<int64_t>(RET);
            }
            return static_cast<int64_t>(RET);
        }
        case ops::LSEEK: {
            int const FD = static_cast<int>(a1);
            auto offset = static_cast<off_t>(a2);
            int const WHENCE = static_cast<int>(a3);
            auto* new_offset = reinterpret_cast<off_t*>(a4);
            off_t const RET = ker::vfs::vfs_lseek(FD, offset, WHENCE);
            if (RET < 0) {
                return static_cast<int64_t>(RET);
            }
            if (int const COPY_RET = copy_value_to_user(new_offset, RET); COPY_RET < 0) {
                return COPY_RET;
            }
            return static_cast<int64_t>(RET);
        }
        case ops::ISATTY: {
            int const FD = static_cast<int>(a1);
            bool const IS_TTY = ker::vfs::vfs_isatty(FD);
            return IS_TTY ? 1 : 0;
        }
        case ops::READ_DIR_ENTRIES: {
            int const FD = static_cast<int>(a1);
            void* buffer = reinterpret_cast<void*>(a2);
            auto max_size = static_cast<size_t>(a3);
            if (buffer == nullptr || max_size < sizeof(ker::vfs::DirEntry)) {
                return static_cast<int64_t>(ker::vfs::vfs_read_dir_entries(FD, buffer, max_size));
            }
            auto* kernel_buffer = new (std::nothrow) uint8_t[max_size];
            if (kernel_buffer == nullptr) {
                return -ENOMEM;
            }
            ssize_t const RET = ker::vfs::vfs_read_dir_entries(FD, kernel_buffer, max_size);
            if (RET < 0) {
                delete[] kernel_buffer;
                return static_cast<int64_t>(RET);
            }
            if (RET > 0) {
                int const COPY_RET = copy_buffer_to_user(buffer, kernel_buffer, static_cast<size_t>(RET));
                delete[] kernel_buffer;
                if (COPY_RET < 0) {
                    return static_cast<int64_t>(COPY_RET);
                }
            } else {
                delete[] kernel_buffer;
            }
            return static_cast<int64_t>(RET);
        }
        case ops::MOUNT: {
            const auto* source = reinterpret_cast<const char*>(a1);
            const auto* target = reinterpret_cast<const char*>(a2);
            const auto* fstype = reinterpret_cast<const char*>(a3);
            unsigned long const FLAGS = static_cast<unsigned long>(a4);
            const auto* data = reinterpret_cast<const char*>(a5);
            int const RET = ker::vfs::vfs_mount(source, target, fstype, FLAGS, data);
            return static_cast<int64_t>(RET);
        }
        case ops::MKDIR: {
            const auto* path = reinterpret_cast<const char*>(a1);
            int const MODE = static_cast<int>(a2);
            int const RET = ker::vfs::vfs_mkdir(path, MODE);
            return static_cast<int64_t>(RET);
        }
        case ops::READLINK: {
            const auto* path = reinterpret_cast<const char*>(a1);
            auto* buf = reinterpret_cast<char*>(a2);
            auto bufsize = static_cast<size_t>(a3);
            if (buf == nullptr || bufsize == 0) {
                return static_cast<int64_t>(ker::vfs::vfs_readlink(path, buf, bufsize));
            }
            auto* kernel_buf = new (std::nothrow) char[bufsize];
            if (kernel_buf == nullptr) {
                return -ENOMEM;
            }
            ssize_t const RET = ker::vfs::vfs_readlink(path, kernel_buf, bufsize);
            if (RET > 0) {
                int const COPY_RET = copy_buffer_to_user(buf, kernel_buf, static_cast<size_t>(RET));
                delete[] kernel_buf;
                if (COPY_RET < 0) {
                    return static_cast<int64_t>(COPY_RET);
                }
            } else {
                delete[] kernel_buf;
            }
            return static_cast<int64_t>(RET);
        }
        case ops::SYMLINK: {
            const auto* target = reinterpret_cast<const char*>(a1);
            const auto* linkpath = reinterpret_cast<const char*>(a2);
            int const RET = ker::vfs::vfs_symlink(target, linkpath);
            return static_cast<int64_t>(RET);
        }
        case ops::SENDFILE: {
            int const OUTFD = static_cast<int>(a1);
            int const INFD = static_cast<int>(a2);
            auto* user_offset = reinterpret_cast<off_t*>(a3);
            off_t kernel_offset = 0;
            auto* offset = user_offset;
            if (user_offset != nullptr) {
                if (int const COPY_RET = copy_value_from_user(user_offset, &kernel_offset); COPY_RET < 0) {
                    return COPY_RET;
                }
                offset = &kernel_offset;
            }
            auto count = static_cast<size_t>(a4);
            ssize_t const RET = ker::vfs::vfs_sendfile(OUTFD, INFD, offset, count);
            if (RET >= 0 && user_offset != nullptr) {
                if (int const COPY_RET = copy_value_to_user(user_offset, kernel_offset); COPY_RET < 0) {
                    return COPY_RET;
                }
            }
            return static_cast<int64_t>(RET);
        }
        case ops::STAT: {
            const auto* path = reinterpret_cast<const char*>(a1);
            auto* statbuf = reinterpret_cast<ker::vfs::Stat*>(a2);
            ker::vfs::Stat kernel_statbuf{};
            int const RET = ker::vfs::vfs_stat(path, &kernel_statbuf);
            return copy_stat_result_to_user(RET, statbuf, kernel_statbuf);
        }
        case ops::LSTAT: {
            const auto* path = reinterpret_cast<const char*>(a1);
            auto* statbuf = reinterpret_cast<ker::vfs::Stat*>(a2);
            ker::vfs::Stat kernel_statbuf{};
            int const RET = ker::vfs::vfs_lstat(path, &kernel_statbuf);
            return copy_stat_result_to_user(RET, statbuf, kernel_statbuf);
        }
        case ops::FSTAT: {
            int const FD = static_cast<int>(a1);
            auto* statbuf = reinterpret_cast<ker::vfs::Stat*>(a2);
            ker::vfs::Stat kernel_statbuf{};
            int const RET = ker::vfs::vfs_fstat(FD, &kernel_statbuf);
            return copy_stat_result_to_user(RET, statbuf, kernel_statbuf);
        }
        case ops::STATAT: {
            constexpr int AT_SYMLINK_NOFOLLOW_FLAG = 0x100;
            constexpr int AT_EMPTY_PATH_FLAG = 0x1000;
            int const DIRFD = static_cast<int>(a1);
            const auto* pathname = reinterpret_cast<const char*>(a2);
            auto* statbuf = reinterpret_cast<ker::vfs::Stat*>(a3);
            int const FLAGS = static_cast<int>(a4);
            if (pathname == nullptr || statbuf == nullptr) {
                return -EFAULT;
            }
            ker::vfs::Stat kernel_statbuf{};
            if ((FLAGS & AT_EMPTY_PATH_FLAG) != 0 && pathname[0] == '\0') {
                int const RET = ker::vfs::vfs_fstat(DIRFD, &kernel_statbuf);
                return copy_stat_result_to_user(RET, statbuf, kernel_statbuf);
            }
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return -ESRCH;
            }
            if (DIRFD == ker::vfs::AT_FDCWD) {
                int ret = 0;
                if ((FLAGS & AT_SYMLINK_NOFOLLOW_FLAG) != 0) {
                    ret = ker::vfs::vfs_lstat(pathname, &kernel_statbuf);
                } else {
                    ret = ker::vfs::vfs_stat(pathname, &kernel_statbuf);
                }
                return copy_stat_result_to_user(ret, statbuf, kernel_statbuf);
            }
            std::array<char, 512> resolved{};
            int const RES = ker::vfs::vfs_resolve_dirfd(task, DIRFD, pathname, resolved.data(), resolved.size());
            if (RES < 0) {
                return static_cast<int64_t>(RES);
            }
            int ret = 0;
            if ((FLAGS & AT_SYMLINK_NOFOLLOW_FLAG) != 0) {
                ret = ker::vfs::vfs_lstat(resolved.data(), &kernel_statbuf);
            } else {
                ret = ker::vfs::vfs_stat(resolved.data(), &kernel_statbuf);
            }
            return copy_stat_result_to_user(ret, statbuf, kernel_statbuf);
        }
        case ops::UTIMENSAT: {
            int const DIRFD = static_cast<int>(a1);
            const auto* pathname = reinterpret_cast<const char*>(a2);
            const auto* user_times = reinterpret_cast<const ker::vfs::Timespec*>(a3);
            int const FLAGS = static_cast<int>(a4);
            if (pathname == nullptr) {
                return -EFAULT;
            }

            std::array<ker::vfs::Timespec, 2> kernel_times{};
            const ker::vfs::Timespec* times = nullptr;
            if (user_times != nullptr) {
                if (int const COPY_RET = copy_value_from_user(user_times, &kernel_times.at(0)); COPY_RET < 0) {
                    return COPY_RET;
                }
                if (int const COPY_RET = copy_value_from_user(user_times + 1, &kernel_times.at(1)); COPY_RET < 0) {
                    return COPY_RET;
                }
                times = kernel_times.data();
            }
            return static_cast<int64_t>(ker::vfs::vfs_utimensat(DIRFD, pathname, times, FLAGS));
        }
        case ops::UMOUNT: {
            const auto* target = reinterpret_cast<const char*>(a1);
            return static_cast<int64_t>(ker::vfs::vfs_umount(target));
        }
        case ops::DUP: {
            int const OLDFD = static_cast<int>(a1);
            return static_cast<int64_t>(ker::vfs::vfs_dup(OLDFD));
        }
        case ops::DUP2: {
            int const OLDFD = static_cast<int>(a1);
            int const NEWFD = static_cast<int>(a2);
            int const FLAGS = static_cast<int>(a3);
            return static_cast<int64_t>(ker::vfs::vfs_dup2(OLDFD, NEWFD, FLAGS));
        }
        case ops::GETCWD: {
            auto* buf = reinterpret_cast<char*>(a1);
            auto size = static_cast<size_t>(a2);
            std::array<char, ker::mod::sched::task::Task::CWD_MAX> kernel_buf{};
            int const RET = ker::vfs::vfs_getcwd(kernel_buf.data(), size);
            if (RET < 0) {
                return static_cast<int64_t>(RET);
            }
            size_t const LEN = std::strlen(kernel_buf.data()) + 1;
            if (int const COPY_RET = copy_buffer_to_user(buf, kernel_buf.data(), LEN); COPY_RET < 0) {
                return COPY_RET;
            }
            return static_cast<int64_t>(RET);
        }
        case ops::CHDIR: {
            const auto* path = reinterpret_cast<const char*>(a1);
            return static_cast<int64_t>(ker::vfs::vfs_chdir(path));
        }
        case ops::ACCESS: {
            const auto* path = reinterpret_cast<const char*>(a1);
            int const MODE = static_cast<int>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_access(path, MODE));
        }
        case ops::UNLINK: {
            const auto* path = reinterpret_cast<const char*>(a1);
            return static_cast<int64_t>(ker::vfs::vfs_unlink(path));
        }
        case ops::RMDIR: {
            const auto* path = reinterpret_cast<const char*>(a1);
            return static_cast<int64_t>(ker::vfs::vfs_rmdir(path));
        }
        case ops::RENAME: {
            const auto* oldpath = reinterpret_cast<const char*>(a1);
            const auto* newpath = reinterpret_cast<const char*>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_rename(oldpath, newpath));
        }
        case ops::CHMOD: {
            const auto* path = reinterpret_cast<const char*>(a1);
            int const MODE = static_cast<int>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_chmod(path, MODE));
        }
        case ops::TRUNCATE: {
            int const FD = static_cast<int>(a1);
            auto length = static_cast<off_t>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_ftruncate(FD, length));
        }
        case ops::PIPE: {
            auto* pipefd = reinterpret_cast<int*>(a1);
            int const FLAGS = static_cast<int>(a2);
            std::array<int, 2> kernel_pipefd = {-1, -1};
            int const RET = ker::vfs::vfs_pipe(kernel_pipefd.data(), FLAGS);
            if (RET < 0) {
                return static_cast<int64_t>(RET);
            }
            if (int const COPY_RET = copy_value_to_user(pipefd, kernel_pipefd.at(0)); COPY_RET < 0) {
                static_cast<void>(ker::vfs::vfs_close(kernel_pipefd.at(0)));
                static_cast<void>(ker::vfs::vfs_close(kernel_pipefd.at(1)));
                return static_cast<int64_t>(COPY_RET);
            }
            if (int const COPY_RET = copy_value_to_user(pipefd + 1, kernel_pipefd.at(1)); COPY_RET < 0) {
                static_cast<void>(ker::vfs::vfs_close(kernel_pipefd.at(0)));
                static_cast<void>(ker::vfs::vfs_close(kernel_pipefd.at(1)));
                return static_cast<int64_t>(COPY_RET);
            }
            return static_cast<int64_t>(RET);
        }
        case ops::PREAD: {
            int const FD = static_cast<int>(a1);
            auto* buf = reinterpret_cast<void*>(a2);
            auto count = static_cast<size_t>(a3);
            auto offset = static_cast<off_t>(a4);
            return static_cast<int64_t>(ker::vfs::vfs_pread(FD, buf, count, offset));
        }
        case ops::PWRITE: {
            int const FD = static_cast<int>(a1);
            const auto* buf = reinterpret_cast<const void*>(a2);
            auto count = static_cast<size_t>(a3);
            auto offset = static_cast<off_t>(a4);
            return static_cast<int64_t>(ker::vfs::vfs_pwrite(FD, buf, count, offset));
        }
        case ops::FCNTL: {
            int const FD = static_cast<int>(a1);
            int const CMD = static_cast<int>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_fcntl(FD, CMD, a3));
        }
        case ops::FCHMOD: {
            int const FD = static_cast<int>(a1);
            int const MODE = static_cast<int>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_fchmod(FD, MODE));
        }
        case ops::CHOWN: {
            const auto* path = reinterpret_cast<const char*>(a1);
            auto owner = static_cast<uint32_t>(a2);
            auto group = static_cast<uint32_t>(a3);
            return static_cast<int64_t>(ker::vfs::vfs_chown(path, owner, group));
        }
        case ops::FCHOWN: {
            int const FD = static_cast<int>(a1);
            auto owner = static_cast<uint32_t>(a2);
            auto group = static_cast<uint32_t>(a3);
            return static_cast<int64_t>(ker::vfs::vfs_fchown(FD, owner, group));
        }
        case ops::FACCESSAT: {
            int const DIRFD = static_cast<int>(a1);
            const auto* pathname = reinterpret_cast<const char*>(a2);
            int const MODE = static_cast<int>(a3);
            // Resolve dirfd-relative path
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return -ESRCH;
            }
            if (DIRFD == ker::vfs::AT_FDCWD) {
                return static_cast<int64_t>(ker::vfs::vfs_access(pathname, MODE));
            }
            std::array<char, 512> resolved{};
            int const RES = ker::vfs::vfs_resolve_dirfd(task, DIRFD, pathname, resolved.data(), resolved.size());
            if (RES < 0) {
                return static_cast<int64_t>(RES);
            }
            return static_cast<int64_t>(ker::vfs::vfs_access(resolved.data(), MODE));
        }
        case ops::UNLINKAT: {
            int const DIRFD = static_cast<int>(a1);
            const auto* pathname = reinterpret_cast<const char*>(a2);
            int const FLAGS = static_cast<int>(a3);
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return -ESRCH;
            }
            if (DIRFD == ker::vfs::AT_FDCWD) {
                if ((FLAGS & 0x200) != 0) {  // AT_REMOVEDIR
                    return static_cast<int64_t>(ker::vfs::vfs_rmdir(pathname));
                }
                return static_cast<int64_t>(ker::vfs::vfs_unlink(pathname));
            }
            std::array<char, 512> resolved{};
            int const RES = ker::vfs::vfs_resolve_dirfd(task, DIRFD, pathname, resolved.data(), resolved.size());
            if (RES < 0) {
                return static_cast<int64_t>(RES);
            }
            if ((FLAGS & 0x200) != 0) {  // AT_REMOVEDIR
                return static_cast<int64_t>(ker::vfs::vfs_rmdir(resolved.data()));
            }
            return static_cast<int64_t>(ker::vfs::vfs_unlink(resolved.data()));
        }
        case ops::RENAMEAT: {
            int const OLDDIRFD = static_cast<int>(a1);
            const auto* oldpath = reinterpret_cast<const char*>(a2);
            int const NEWDIRFD = static_cast<int>(a3);
            const auto* newpath = reinterpret_cast<const char*>(a4);
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return -ESRCH;
            }
            if (OLDDIRFD == ker::vfs::AT_FDCWD && NEWDIRFD == ker::vfs::AT_FDCWD) {
                return static_cast<int64_t>(ker::vfs::vfs_rename(oldpath, newpath));
            }
            std::array<char, 512> resolved_old{};
            std::array<char, 512> resolved_new{};
            int res = ker::vfs::vfs_resolve_dirfd(task, OLDDIRFD, oldpath, resolved_old.data(), resolved_old.size());
            if (res < 0) {
                return static_cast<int64_t>(res);
            }
            res = ker::vfs::vfs_resolve_dirfd(task, NEWDIRFD, newpath, resolved_new.data(), resolved_new.size());
            if (res < 0) {
                return static_cast<int64_t>(res);
            }
            return static_cast<int64_t>(ker::vfs::vfs_rename(resolved_old.data(), resolved_new.data()));
        }
        case ops::EPOLL_CREATE: {
            int const FLAGS = static_cast<int>(a1);
            return static_cast<int64_t>(ker::vfs::epoll_create(FLAGS));
        }
        case ops::EPOLL_CTL: {
            int const EPFD = static_cast<int>(a1);
            int const OP = static_cast<int>(a2);
            int const FD = static_cast<int>(a3);
            auto* event = reinterpret_cast<ker::vfs::EpollEvent*>(a4);
            ker::vfs::EpollEvent kernel_event{};
            ker::vfs::EpollEvent* event_arg = nullptr;
            if (event != nullptr) {
                if (int const COPY_RET = copy_value_from_user(event, &kernel_event); COPY_RET < 0) {
                    return COPY_RET;
                }
                event_arg = &kernel_event;
            }
            return static_cast<int64_t>(ker::vfs::epoll_ctl(EPFD, OP, FD, event_arg));
        }
        case ops::EPOLL_PWAIT: {
            int const EPFD = static_cast<int>(a1);
            auto* events = reinterpret_cast<ker::vfs::EpollEvent*>(a2);
            int const MAXEVENTS = static_cast<int>(a3);
            int const TIMEOUT = static_cast<int>(static_cast<int64_t>(a4));
            if (events == nullptr || MAXEVENTS <= 0) {
                return static_cast<int64_t>(ker::vfs::epoll_pwait(EPFD, events, MAXEVENTS, TIMEOUT));
            }
            auto const EVENT_COUNT = static_cast<size_t>(MAXEVENTS);
            auto* kernel_events = new (std::nothrow) ker::vfs::EpollEvent[EVENT_COUNT];
            if (kernel_events == nullptr) {
                return -ENOMEM;
            }
            int const RET = ker::vfs::epoll_pwait(EPFD, kernel_events, MAXEVENTS, TIMEOUT);
            if (RET > 0) {
                size_t const COPY_SIZE = static_cast<size_t>(RET) * sizeof(ker::vfs::EpollEvent);
                int const COPY_RET = copy_buffer_to_user(events, kernel_events, COPY_SIZE);
                delete[] kernel_events;
                if (COPY_RET < 0) {
                    return static_cast<int64_t>(COPY_RET);
                }
            } else {
                delete[] kernel_events;
            }
            return static_cast<int64_t>(RET);
        }
        case ops::IOCTL: {
            int const FD = static_cast<int>(a1);
            auto cmd = static_cast<unsigned long>(a2);
            auto arg = static_cast<unsigned long>(a3);
            // Get the file for the fd
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return -ESRCH;
            }
            if (FD < 0) {
                return -EBADF;
            }
            auto* file = ker::vfs::vfs_get_file_retain(task, FD);
            if (file == nullptr) {
                return -EBADF;
            }
            if (file->fs_type == ker::vfs::FSType::DEVFS) {
                auto const RESULT = static_cast<int64_t>(ker::vfs::devfs::devfs_ioctl(file, cmd, arg));
                ker::vfs::vfs_put_file(file);
                return RESULT;
            }
            // Fallback: fops-backed ioctl (e.g. remote PTY proxy)
            if (file->fops != nullptr && file->fops->vfs_ioctl != nullptr) {
                auto const RESULT = static_cast<int64_t>(file->fops->vfs_ioctl(file, cmd, arg));
                ker::vfs::vfs_put_file(file);
                return RESULT;
            }
            ker::vfs::vfs_put_file(file);
            return -ENOTTY;
        }
        case ops::FSYNC: {
            int const FD = static_cast<int>(a1);
            return static_cast<int64_t>(ker::vfs::vfs_fsync(FD));
        }
        case ops::SYNC: {
            return static_cast<int64_t>(ker::vfs::vfs_sync());
        }
        case ops::LINK: {
            const auto* oldpath = reinterpret_cast<const char*>(a1);
            const auto* newpath = reinterpret_cast<const char*>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_link(oldpath, newpath));
        }
        case ops::WKI_RULE_ADD: {
            const auto* prefix = reinterpret_cast<const char*>(a1);
            auto route = static_cast<uint32_t>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_wki_rule_add(prefix, route));
        }
        case ops::WKI_RULE_GET: {
            auto index = static_cast<uint32_t>(a1);
            auto* prefix_buf = reinterpret_cast<char*>(a2);
            auto prefix_buf_size = static_cast<size_t>(a3);
            auto* route_out = reinterpret_cast<uint32_t*>(a4);
            return copy_wki_rule_to_user(index, prefix_buf, prefix_buf_size, route_out, false);
        }
        case ops::WKI_RULE_GET_DEFAULT: {
            auto index = static_cast<uint32_t>(a1);
            auto* prefix_buf = reinterpret_cast<char*>(a2);
            auto prefix_buf_size = static_cast<size_t>(a3);
            auto* route_out = reinterpret_cast<uint32_t*>(a4);
            return copy_wki_rule_to_user(index, prefix_buf, prefix_buf_size, route_out, true);
        }
        case ops::WKI_RULE_CLEAR: {
            return static_cast<int64_t>(ker::vfs::vfs_wki_rule_clear());
        }
        case ops::PIVOT_ROOT: {
            const auto* new_root = reinterpret_cast<const char*>(a1);
            const auto* put_old = reinterpret_cast<const char*>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_pivot_root(new_root, put_old));
        }
        case ops::STATVFS: {
            const auto* path = reinterpret_cast<const char*>(a1);
            auto* buf = reinterpret_cast<ker::vfs::Statvfs*>(a2);
            ker::vfs::Statvfs kernel_buf{};
            int const RET = ker::vfs::vfs_statvfs(path, &kernel_buf);
            return copy_statvfs_result_to_user(RET, buf, kernel_buf);
        }
        case ops::FSTATVFS: {
            int const FD = static_cast<int>(a1);
            auto* buf = reinterpret_cast<ker::vfs::Statvfs*>(a2);
            ker::vfs::Statvfs kernel_buf{};
            int const RET = ker::vfs::vfs_fstatvfs(FD, &kernel_buf);
            return copy_statvfs_result_to_user(RET, buf, kernel_buf);
        }
        case ops::REALPATH: {
            const auto* path = reinterpret_cast<const char*>(a1);
            auto* buf = reinterpret_cast<char*>(a2);
            auto bufsize = static_cast<size_t>(a3);
            if (buf == nullptr || bufsize == 0) {
                return static_cast<int64_t>(ker::vfs::vfs_realpath(path, buf, bufsize));
            }
            auto* kernel_buf = new (std::nothrow) char[bufsize];
            if (kernel_buf == nullptr) {
                return -ENOMEM;
            }
            int const RET = ker::vfs::vfs_realpath(path, kernel_buf, bufsize);
            if (RET < 0) {
                delete[] kernel_buf;
                return static_cast<int64_t>(RET);
            }
            size_t const LEN = std::strlen(kernel_buf) + 1;
            int const COPY_RET = copy_buffer_to_user(buf, kernel_buf, LEN);
            delete[] kernel_buf;
            if (COPY_RET < 0) {
                return static_cast<int64_t>(COPY_RET);
            }
            return static_cast<int64_t>(RET);
        }
        default:
            ker::vfs::vfs_debug_log("sys_vfs: unknown op\n");
            return static_cast<int64_t>(-ENOSYS);
    }
}

}  // namespace ker::syscall::vfs
