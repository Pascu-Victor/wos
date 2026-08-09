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
#include <dev/pty.hpp>
#include <new>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/usercopy.hpp>
#include <utility>
#include <vfs/epoll.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/fs/devfs.hpp>
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

namespace ker::syscall::vfs {
using ker::abi::vfs::ops;

namespace {
// Match libc's DIR refill size while keeping stack use small relative to the
// 512 KiB syscall kernel stack.
constexpr size_t READ_DIR_STACK_BUFFER_SIZE = size_t{16} * 1024;
constexpr size_t READLINK_STACK_BUFFER_SIZE = 512;
constexpr size_t REALPATH_STACK_BUFFER_SIZE = 512;
constexpr size_t VFS_PATH_BUFFER_SIZE = 512;
using KernelPath = std::array<char, VFS_PATH_BUFFER_SIZE>;

struct MetadataBatchUserEntry {
    const char* path;
    const char* second_path;
};

struct MetadataBatchUserResult {
    int32_t status;
    uint32_t reserved;
    ker::vfs::Stat stat;
};

struct IoctlUserMarshal {
    size_t size{};
    bool copy_in{};
    bool copy_out{};
    bool supported{};
};

auto pty_ioctl_user_marshal(unsigned long cmd) -> IoctlUserMarshal {
    using namespace ker::dev::pty;
    switch (cmd) {
        case TIOCGPTN:
            return {.size = sizeof(int), .copy_in = false, .copy_out = true, .supported = true};
        case TIOCSPTLCK:
            return {.size = sizeof(int), .copy_in = true, .copy_out = false, .supported = true};
        case TIOCGWINSZ:
            return {.size = sizeof(Winsize), .copy_in = false, .copy_out = true, .supported = true};
        case TIOCSWINSZ:
            return {.size = sizeof(Winsize), .copy_in = true, .copy_out = false, .supported = true};
        case TIOCGPGRP:
            return {.size = sizeof(int64_t), .copy_in = false, .copy_out = true, .supported = true};
        case TIOCSPGRP:
            return {.size = sizeof(int64_t), .copy_in = true, .copy_out = false, .supported = true};
        case TCGETS:
            return {.size = sizeof(KTermios), .copy_in = false, .copy_out = true, .supported = true};
        case TCSETS:
        case TCSETSW:
        case TCSETSF:
            return {.size = sizeof(KTermios), .copy_in = true, .copy_out = false, .supported = true};
        case TIOCSCTTY:
        case TIOCNOTTY:
        case TCFLSH:
            return {.supported = true};
        default:
            return {};
    }
}

static_assert(sizeof(MetadataBatchUserEntry) == 16);
static_assert(offsetof(MetadataBatchUserResult, stat) == 8);
static_assert(sizeof(MetadataBatchUserResult) == 152);

auto metadata_batch_operation_to_vfs(ker::abi::vfs::metadata_batch_operation operation, ker::vfs::MetadataBatchOperation* out) -> bool {
    if (out == nullptr) {
        return false;
    }
    switch (operation) {
        case ker::abi::vfs::metadata_batch_operation::INVALID:
            return false;
        case ker::abi::vfs::metadata_batch_operation::CREATE_CLOSE:
            *out = ker::vfs::MetadataBatchOperation::CREATE_CLOSE;
            return true;
        case ker::abi::vfs::metadata_batch_operation::STAT_FOLLOW:
            *out = ker::vfs::MetadataBatchOperation::STAT_FOLLOW;
            return true;
        case ker::abi::vfs::metadata_batch_operation::UNLINK:
            *out = ker::vfs::MetadataBatchOperation::UNLINK;
            return true;
        case ker::abi::vfs::metadata_batch_operation::RENAME:
            *out = ker::vfs::MetadataBatchOperation::RENAME;
            return true;
    }
    return false;
}

template <typename T>
auto copy_value_to_user_for_task(ker::mod::sched::task::Task* task, T* user_ptr, const T& value) -> int {
    if (user_ptr == nullptr) {
        return 0;
    }

    if (task == nullptr || task->pagemap == nullptr) {
        return -EFAULT;
    }

    return ker::mod::sys::usercopy::copy_value_to_task(*task, reinterpret_cast<uint64_t>(user_ptr), value) ? 0 : -EFAULT;
}

template <typename T>
auto copy_value_to_user(T* user_ptr, const T& value) -> int {
    return copy_value_to_user_for_task(ker::mod::sched::get_current_task(), user_ptr, value);
}

auto user_io_buffer_has_bounded_user_range(uint64_t user_addr, size_t size) -> bool {
    // Preserve zero-length I/O and let VFS retain its descriptor-first error
    // ordering for null, non-empty buffers. Every other non-empty range must
    // be eligible for the VFS usercopy bounce path.
    if (size == 0 || user_addr == 0) {
        return true;
    }

    auto* task = ker::mod::sched::get_current_task();
    return task != nullptr && task->pagemap != nullptr && ker::mod::sys::usercopy::range_valid(user_addr, size);
}

auto preflight_optional_user_output(uint64_t user_addr, size_t size) -> bool {
    if (user_addr == 0) {
        return true;
    }

    auto* task = ker::mod::sched::get_current_task();
    return task != nullptr && task->pagemap != nullptr && ker::mod::sys::usercopy::ensure_writable(*task, user_addr, size);
}

auto copy_path_from_user(uint64_t user_addr, KernelPath& path) -> int {
    if (user_addr == 0) {
        return -EFAULT;
    }
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -EFAULT;
    }

    auto const STATUS = ker::mod::sys::usercopy::copy_cstring_from_task_status(*task, user_addr, path.data(), path.size());
    if (STATUS == ker::mod::sys::usercopy::CStringCopyStatus::FAULT) {
        return -EFAULT;
    }
    if (STATUS == ker::mod::sys::usercopy::CStringCopyStatus::TOO_LONG) {
        return -ENAMETOOLONG;
    }
    return 0;
}

auto copy_optional_path_from_user(uint64_t user_addr, KernelPath& path, const char*& kernel_arg) -> int {
    kernel_arg = nullptr;
    if (user_addr == 0) {
        return 0;
    }
    int const RET = copy_path_from_user(user_addr, path);
    if (RET == 0) {
        kernel_arg = path.data();
    }
    return RET;
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

auto copy_stat_result_to_user_for_task(ker::mod::sched::task::Task* task, int result, ker::vfs::Stat* user_statbuf,
                                       const ker::vfs::Stat& kernel_statbuf) -> int64_t {
    if (result < 0) {
        return static_cast<int64_t>(result);
    }
    if (user_statbuf == nullptr) {
        return -EFAULT;
    }
    if (int const COPY_RET = copy_value_to_user_for_task(task, user_statbuf, kernel_statbuf); COPY_RET < 0) {
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
    if ((prefix_buf != nullptr &&
         !preflight_optional_user_output(reinterpret_cast<uint64_t>(prefix_buf), std::min(prefix_buf_size, kernel_prefix.size()))) ||
        !preflight_optional_user_output(reinterpret_cast<uint64_t>(route_out), sizeof(uint32_t))) {
        return -EFAULT;
    }
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
        if (static_cast<size_t>(RET) >= PREFIX_ARG_SIZE || static_cast<size_t>(RET) >= kernel_prefix.size()) {
            return -EOVERFLOW;
        }
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
            KernelPath path{};
            if (int const COPY_RET = copy_path_from_user(a1, path); COPY_RET < 0) {
                return COPY_RET;
            }
            int const FLAGS = static_cast<int>(a2);
            int const MODE = static_cast<int>(a3);
            int const FD = ker::vfs::vfs_open(path.data(), FLAGS, MODE);
            if (FD < 0) {
                return static_cast<int64_t>(FD);
            }
            return static_cast<int64_t>(FD);
        }
        case ops::OPENAT: {
            int const DIRFD = static_cast<int>(a1);
            KernelPath pathname{};
            if (int const COPY_RET = copy_path_from_user(a2, pathname); COPY_RET < 0) {
                return COPY_RET;
            }
            int const FLAGS = static_cast<int>(a3);
            int const MODE = static_cast<int>(a4);
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return -ESRCH;
            }
            return static_cast<int64_t>(ker::vfs::vfs_openat(task, DIRFD, pathname.data(), FLAGS, MODE));
        }
        case ops::READ: {
            int const FD = static_cast<int>(a1);
            void* buf = reinterpret_cast<void*>(a2);
            auto len = static_cast<size_t>(a3);
            auto* actual_size = reinterpret_cast<size_t*>(a4);
            if (!user_io_buffer_has_bounded_user_range(a2, len) || !preflight_optional_user_output(a4, sizeof(size_t))) {
                return -EFAULT;
            }
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
            if (!user_io_buffer_has_bounded_user_range(a2, len) || !preflight_optional_user_output(a4, sizeof(size_t))) {
                return -EFAULT;
            }
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
            if (!preflight_optional_user_output(a4, sizeof(off_t))) {
                return -EFAULT;
            }
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
            auto max_size = static_cast<size_t>(a3);
            if (max_size < ker::vfs::DIRENT_MIN_RECLEN) {
                return static_cast<int64_t>(ker::vfs::vfs_read_dir_entries(FD, nullptr, max_size));
            }
            if (a2 == 0) {
                ssize_t const RET = ker::vfs::vfs_read_dir_entries(FD, nullptr, max_size);
                return RET == -EINVAL ? -EFAULT : static_cast<int64_t>(RET);
            }

            std::array<uint8_t, READ_DIR_STACK_BUFFER_SIZE> stack_buffer;  // NOLINT(cppcoreguidelines-pro-type-member-init)
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return -ESRCH;
            }
            size_t total = 0;
            while (max_size - total >= ker::vfs::DIRENT_MIN_RECLEN) {
                size_t const CHUNK_SIZE = std::min(stack_buffer.size(), max_size - total);
                uint64_t user_chunk = 0;
                if (__builtin_add_overflow(a2, static_cast<uint64_t>(total), &user_chunk) ||
                    !ker::mod::sys::usercopy::ensure_writable(*task, user_chunk, CHUNK_SIZE)) {
                    return total > 0 ? static_cast<int64_t>(total) : -EFAULT;
                }
                ssize_t const RET = ker::vfs::vfs_read_dir_entries(FD, stack_buffer.data(), CHUNK_SIZE);
                if (RET < 0) {
                    return total > 0 ? static_cast<int64_t>(total) : static_cast<int64_t>(RET);
                }
                if (RET == 0) {
                    break;
                }
                if (static_cast<size_t>(RET) > CHUNK_SIZE) {
                    return total > 0 ? static_cast<int64_t>(total) : -EOVERFLOW;
                }

                if (!ker::mod::sys::usercopy::copy_to_task(*task, user_chunk, stack_buffer.data(), static_cast<size_t>(RET))) {
                    return total > 0 ? static_cast<int64_t>(total) : -EFAULT;
                }

                total += static_cast<size_t>(RET);
            }
            return static_cast<int64_t>(total);
        }
        case ops::MOUNT: {
            KernelPath source{};
            KernelPath target{};
            KernelPath fstype{};
            KernelPath data{};
            const char* source_arg = nullptr;
            const char* data_arg = nullptr;
            if (int const COPY_RET = copy_optional_path_from_user(a1, source, source_arg); COPY_RET < 0) {
                return COPY_RET;
            }
            if (int const COPY_RET = copy_path_from_user(a2, target); COPY_RET < 0) {
                return COPY_RET;
            }
            if (int const COPY_RET = copy_path_from_user(a3, fstype); COPY_RET < 0) {
                return COPY_RET;
            }
            if (int const COPY_RET = copy_optional_path_from_user(a5, data, data_arg); COPY_RET < 0) {
                return COPY_RET;
            }
            unsigned long const FLAGS = static_cast<unsigned long>(a4);
            int const RET = ker::vfs::vfs_mount(source_arg, target.data(), fstype.data(), FLAGS, data_arg);
            return static_cast<int64_t>(RET);
        }
        case ops::MKDIR: {
            KernelPath path{};
            if (int const COPY_RET = copy_path_from_user(a1, path); COPY_RET < 0) {
                return COPY_RET;
            }
            int const MODE = static_cast<int>(a2);
            int const RET = ker::vfs::vfs_mkdir(path.data(), MODE);
            return static_cast<int64_t>(RET);
        }
        case ops::MKDIRAT: {
            int const DIRFD = static_cast<int>(a1);
            KernelPath pathname{};
            if (int const COPY_RET = copy_path_from_user(a2, pathname); COPY_RET < 0) {
                return COPY_RET;
            }
            int const MODE = static_cast<int>(a3);
            auto* task = ker::mod::sched::get_current_task();
            return static_cast<int64_t>(ker::vfs::vfs_mkdirat(task, DIRFD, pathname.data(), MODE));
        }
        case ops::READLINK: {
            KernelPath path{};
            if (int const COPY_RET = copy_path_from_user(a1, path); COPY_RET < 0) {
                return COPY_RET;
            }
            auto bufsize = static_cast<size_t>(a3);
            if (bufsize == 0) {
                return static_cast<int64_t>(ker::vfs::vfs_readlink(path.data(), nullptr, 0));
            }
            if (a2 == 0) {
                return -EFAULT;
            }

            std::array<char, READLINK_STACK_BUFFER_SIZE> stack_buf{};
            size_t read_size = std::min(bufsize, stack_buf.size());
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr || !ker::mod::sys::usercopy::ensure_writable(*task, a2, read_size)) {
                return -EFAULT;
            }
            ssize_t const RET = ker::vfs::vfs_readlink(path.data(), stack_buf.data(), read_size);

            if (RET > 0) {
                if (static_cast<size_t>(RET) > read_size) {
                    return -EOVERFLOW;
                }
                if (!ker::mod::sys::usercopy::copy_to_task(*task, a2, stack_buf.data(), static_cast<size_t>(RET))) {
                    return -EFAULT;
                }
            }
            return static_cast<int64_t>(RET);
        }
        case ops::READLINKAT: {
            int const DIRFD = static_cast<int>(a1);
            KernelPath pathname{};
            if (int const COPY_RET = copy_path_from_user(a2, pathname); COPY_RET < 0) {
                return COPY_RET;
            }
            auto bufsize = static_cast<size_t>(a4);
            auto* task = ker::mod::sched::get_current_task();
            if (bufsize == 0) {
                return static_cast<int64_t>(ker::vfs::vfs_readlinkat(task, DIRFD, pathname.data(), nullptr, 0));
            }
            if (a3 == 0) {
                return -EFAULT;
            }

            std::array<char, READLINK_STACK_BUFFER_SIZE> stack_buf{};
            size_t read_size = std::min(bufsize, stack_buf.size());
            if (task == nullptr || !ker::mod::sys::usercopy::ensure_writable(*task, a3, read_size)) {
                return -EFAULT;
            }
            ssize_t const RET = ker::vfs::vfs_readlinkat(task, DIRFD, pathname.data(), stack_buf.data(), read_size);

            if (RET > 0) {
                if (static_cast<size_t>(RET) > read_size) {
                    return -EOVERFLOW;
                }
                if (!ker::mod::sys::usercopy::copy_to_task(*task, a3, stack_buf.data(), static_cast<size_t>(RET))) {
                    return -EFAULT;
                }
            }
            return static_cast<int64_t>(RET);
        }
        case ops::SYMLINK: {
            KernelPath target{};
            KernelPath linkpath{};
            if (int const COPY_RET = copy_path_from_user(a1, target); COPY_RET < 0) {
                return COPY_RET;
            }
            if (int const COPY_RET = copy_path_from_user(a2, linkpath); COPY_RET < 0) {
                return COPY_RET;
            }
            int const RET = ker::vfs::vfs_symlink(target.data(), linkpath.data());
            return static_cast<int64_t>(RET);
        }
        case ops::SYMLINKAT: {
            int const DIRFD = static_cast<int>(a2);
            KernelPath target{};
            KernelPath linkpath{};
            if (int const COPY_RET = copy_path_from_user(a1, target); COPY_RET < 0) {
                return COPY_RET;
            }
            if (int const COPY_RET = copy_path_from_user(a3, linkpath); COPY_RET < 0) {
                return COPY_RET;
            }
            auto* task = ker::mod::sched::get_current_task();
            return static_cast<int64_t>(ker::vfs::vfs_symlinkat(task, target.data(), DIRFD, linkpath.data()));
        }
        case ops::SENDFILE: {
            int const OUTFD = static_cast<int>(a1);
            int const INFD = static_cast<int>(a2);
            auto* user_offset = reinterpret_cast<off_t*>(a3);
            off_t kernel_offset = 0;
            auto* offset = user_offset;
            if (user_offset != nullptr) {
                if (!preflight_optional_user_output(a3, sizeof(off_t))) {
                    return -EFAULT;
                }
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
            KernelPath path{};
            if (int const COPY_RET = copy_path_from_user(a1, path); COPY_RET < 0) {
                return COPY_RET;
            }
            auto* statbuf = reinterpret_cast<ker::vfs::Stat*>(a2);
            ker::vfs::Stat kernel_statbuf{};
            auto* task = ker::mod::sched::get_current_task();
            int const RET = task != nullptr ? ker::vfs::vfs_statat(task, ker::vfs::AT_FDCWD, path.data(), 0, &kernel_statbuf) : -ESRCH;
            return copy_stat_result_to_user_for_task(task, RET, statbuf, kernel_statbuf);
        }
        case ops::LSTAT: {
            KernelPath path{};
            if (int const COPY_RET = copy_path_from_user(a1, path); COPY_RET < 0) {
                return COPY_RET;
            }
            auto* statbuf = reinterpret_cast<ker::vfs::Stat*>(a2);
            ker::vfs::Stat kernel_statbuf{};
            auto* task = ker::mod::sched::get_current_task();
            int const RET = task != nullptr ? ker::vfs::vfs_statat(task, ker::vfs::AT_FDCWD, path.data(), ker::vfs::AT_SYMLINK_NOFOLLOW,
                                                                   &kernel_statbuf)
                                            : -ESRCH;
            return copy_stat_result_to_user_for_task(task, RET, statbuf, kernel_statbuf);
        }
        case ops::FSTAT: {
            int const FD = static_cast<int>(a1);
            auto* statbuf = reinterpret_cast<ker::vfs::Stat*>(a2);
            ker::vfs::Stat kernel_statbuf{};
            auto* task = ker::mod::sched::get_current_task();
            int ret = -ESRCH;
            if (task != nullptr) {
                ret = ker::vfs::vfs_fstat_snapshot_fast(task, FD, &kernel_statbuf);
                if (ret == -EAGAIN) {
                    auto* file = ker::vfs::vfs_get_file_retain(task, FD);
                    if (file == nullptr) {
                        ret = -EBADF;
                    } else {
                        ret = ker::vfs::vfs_fstat_file(file, &kernel_statbuf);
                        ker::vfs::vfs_put_file(file);
                    }
                } else {
                    // The fast path returns only a complete cached stat result
                    // or a terminal fd/task error.
                }
            }
            int const RET = ret;
            return copy_stat_result_to_user_for_task(task, RET, statbuf, kernel_statbuf);
        }
        case ops::FSTAT_CLOSE: {
            int const FD = static_cast<int>(a1);
            auto* statbuf = reinterpret_cast<ker::vfs::Stat*>(a2);
            auto* stat_result_out = reinterpret_cast<int*>(a3);
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return -ESRCH;
            }
            if (statbuf == nullptr || stat_result_out == nullptr ||
                !ker::mod::sys::usercopy::ensure_writable(*task, a2, sizeof(ker::vfs::Stat)) ||
                !ker::mod::sys::usercopy::ensure_writable(*task, a3, sizeof(int))) {
                return -EFAULT;
            }

            ker::vfs::Stat kernel_statbuf{};
            int stat_result = -EIO;
            int const CLOSE_RESULT = ker::vfs::vfs_fstat_close_for_task(task, FD, &kernel_statbuf, &stat_result);
            if (stat_result == 0) {
                if (int const COPY_RET = copy_value_to_user_for_task(task, statbuf, kernel_statbuf); COPY_RET < 0) {
                    return COPY_RET;
                }
            }
            if (int const COPY_RET = copy_value_to_user_for_task(task, stat_result_out, stat_result); COPY_RET < 0) {
                return COPY_RET;
            }
            return static_cast<int64_t>(CLOSE_RESULT);
        }
        case ops::METADATA_BATCH: {
            auto const* user_header = reinterpret_cast<const ker::abi::vfs::metadata_batch_header*>(a1);
            auto const* user_entries = reinterpret_cast<const MetadataBatchUserEntry*>(a2);
            auto* user_results = reinterpret_cast<MetadataBatchUserResult*>(a3);
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return -ESRCH;
            }

            ker::abi::vfs::metadata_batch_header header{};
            if (copy_value_from_user(user_header, &header) < 0) {
                return -EFAULT;
            }
            if (header.version != ker::abi::vfs::METADATA_BATCH_VERSION || header.count == 0 ||
                header.count > ker::abi::vfs::METADATA_BATCH_MAX_ITEMS) {
                return -EINVAL;
            }

            ker::vfs::MetadataBatchOperation operation{};
            if (!metadata_batch_operation_to_vfs(header.operation, &operation) ||
                (operation != ker::vfs::MetadataBatchOperation::CREATE_CLOSE && header.mode != 0)) {
                return -EINVAL;
            }
            if (user_entries == nullptr || user_results == nullptr) {
                return -EFAULT;
            }

            size_t const ENTRY_BYTES = sizeof(MetadataBatchUserEntry) * header.count;
            size_t const RESULT_BYTES = sizeof(MetadataBatchUserResult) * header.count;
            if (!ker::mod::sys::usercopy::ensure_writable(*task, a3, RESULT_BYTES)) {
                return -EFAULT;
            }

            std::array<MetadataBatchUserEntry, ker::abi::vfs::METADATA_BATCH_MAX_ITEMS> copied_entries{};
            if (!ker::mod::sys::usercopy::copy_from_task(*task, a2, copied_entries.data(), ENTRY_BYTES)) {
                return -EFAULT;
            }

            constexpr size_t PATH_SCRATCH_SIZE = ker::abi::vfs::METADATA_BATCH_MAX_PATH_CHARS + 2;
            std::array<std::array<char, PATH_SCRATCH_SIZE>, ker::abi::vfs::METADATA_BATCH_MAX_ITEMS> primary_paths{};
            std::array<std::array<char, PATH_SCRATCH_SIZE>, ker::abi::vfs::METADATA_BATCH_MAX_ITEMS> secondary_paths{};
            std::array<ker::vfs::MetadataBatchEntry, ker::abi::vfs::METADATA_BATCH_MAX_ITEMS> kernel_entries{};
            std::array<ker::vfs::MetadataBatchResult, ker::abi::vfs::METADATA_BATCH_MAX_ITEMS> kernel_results{};

            for (size_t index = 0; index < header.count; ++index) {
                auto const& copied = copied_entries.at(index);
                if (copied.path == nullptr) {
                    return -EFAULT;
                }
                auto const PRIMARY_STATUS = ker::mod::sys::usercopy::copy_cstring_from_task_status(
                    *task, reinterpret_cast<uint64_t>(copied.path), primary_paths.at(index).data(), PATH_SCRATCH_SIZE);
                if (PRIMARY_STATUS == ker::mod::sys::usercopy::CStringCopyStatus::FAULT) {
                    return -EFAULT;
                }
                if (PRIMARY_STATUS == ker::mod::sys::usercopy::CStringCopyStatus::TOO_LONG) {
                    return -ENAMETOOLONG;
                }
                size_t const PRIMARY_LEN = std::strlen(primary_paths.at(index).data());
                if (PRIMARY_LEN > ker::abi::vfs::METADATA_BATCH_MAX_PATH_CHARS) {
                    return -ENAMETOOLONG;
                }
                if (PRIMARY_LEN == 0) {
                    return -ENOENT;
                }

                bool const RENAME = operation == ker::vfs::MetadataBatchOperation::RENAME;
                if ((!RENAME && copied.second_path != nullptr) || (RENAME && copied.second_path == nullptr)) {
                    return -EINVAL;
                }
                if (RENAME) {
                    auto const SECONDARY_STATUS = ker::mod::sys::usercopy::copy_cstring_from_task_status(
                        *task, reinterpret_cast<uint64_t>(copied.second_path), secondary_paths.at(index).data(), PATH_SCRATCH_SIZE);
                    if (SECONDARY_STATUS == ker::mod::sys::usercopy::CStringCopyStatus::FAULT) {
                        return -EFAULT;
                    }
                    if (SECONDARY_STATUS == ker::mod::sys::usercopy::CStringCopyStatus::TOO_LONG) {
                        return -ENAMETOOLONG;
                    }
                    size_t const SECONDARY_LEN = std::strlen(secondary_paths.at(index).data());
                    if (SECONDARY_LEN > ker::abi::vfs::METADATA_BATCH_MAX_PATH_CHARS) {
                        return -ENAMETOOLONG;
                    }
                    if (SECONDARY_LEN == 0) {
                        return -ENOENT;
                    }
                }

                kernel_entries.at(index) = {.path = primary_paths.at(index).data(),
                                            .second_path = RENAME ? secondary_paths.at(index).data() : nullptr};
                kernel_results.at(index).status = -EINPROGRESS;
            }

            int const BATCH_RESULT =
                ker::vfs::vfs_metadata_batch(task, operation, header.mode, kernel_entries.data(), header.count, kernel_results.data());

            std::array<MetadataBatchUserResult, ker::abi::vfs::METADATA_BATCH_MAX_ITEMS> copied_results{};
            for (size_t index = 0; index < header.count; ++index) {
                copied_results.at(index).status = kernel_results.at(index).status;
                copied_results.at(index).stat = kernel_results.at(index).stat;
            }
            if (!ker::mod::sys::usercopy::copy_to_task(*task, a3, copied_results.data(), RESULT_BYTES)) {
                return -EFAULT;
            }
            return static_cast<int64_t>(BATCH_RESULT);
        }
        case ops::STATAT: {
            int const DIRFD = static_cast<int>(a1);
            KernelPath pathname{};
            auto* statbuf = reinterpret_cast<ker::vfs::Stat*>(a3);
            int const FLAGS = static_cast<int>(a4);
            if (statbuf == nullptr) {
                return -EFAULT;
            }
            if (int const COPY_RET = copy_path_from_user(a2, pathname); COPY_RET < 0) {
                return COPY_RET;
            }
            ker::vfs::Stat kernel_statbuf;  // NOLINT(cppcoreguidelines-pro-type-member-init)
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return -ESRCH;
            }
            int const RET = ker::vfs::vfs_statat(task, DIRFD, pathname.data(), FLAGS, &kernel_statbuf);
            return copy_stat_result_to_user_for_task(task, RET, statbuf, kernel_statbuf);
        }
        case ops::UTIMENSAT: {
            int const DIRFD = static_cast<int>(a1);
            int const FLAGS = static_cast<int>(a4);
            KernelPath pathname{};
            if (int const COPY_RET = copy_path_from_user(a2, pathname); COPY_RET < 0) {
                return COPY_RET;
            }

            std::array<ker::vfs::Timespec, 2> kernel_times{};
            const ker::vfs::Timespec* times = nullptr;
            if (a3 != 0) {
                auto* task = ker::mod::sched::get_current_task();
                if (task == nullptr || !ker::mod::sys::usercopy::copy_from_task(*task, a3, kernel_times.data(), sizeof(kernel_times))) {
                    return -EFAULT;
                }
                times = kernel_times.data();
            }
            return static_cast<int64_t>(ker::vfs::vfs_utimensat(DIRFD, pathname.data(), times, FLAGS));
        }
        case ops::UMOUNT: {
            KernelPath target{};
            if (int const COPY_RET = copy_path_from_user(a1, target); COPY_RET < 0) {
                return COPY_RET;
            }
            return static_cast<int64_t>(ker::vfs::vfs_umount(target.data()));
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
            if (size != 0 && (a1 == 0 || !preflight_optional_user_output(
                                             a1, std::min(size, static_cast<size_t>(ker::mod::sched::task::Task::CWD_MAX))))) {
                return -EFAULT;
            }
            std::array<char, ker::mod::sched::task::Task::CWD_MAX> kernel_buf;  // NOLINT(cppcoreguidelines-pro-type-member-init)
            size_t len = 0;
            int const RET = ker::vfs::vfs_getcwd(kernel_buf.data(), std::min(size, kernel_buf.size()), &len);
            if (RET < 0) {
                return static_cast<int64_t>(RET);
            }
            size_t const KERNEL_CAPACITY = std::min(size, kernel_buf.size());
            if (len >= KERNEL_CAPACITY) {
                return -EOVERFLOW;
            }
            if (int const COPY_RET = copy_buffer_to_user(buf, kernel_buf.data(), len + 1); COPY_RET < 0) {
                return COPY_RET;
            }
            return static_cast<int64_t>(RET);
        }
        case ops::CHDIR: {
            KernelPath path{};
            if (int const COPY_RET = copy_path_from_user(a1, path); COPY_RET < 0) {
                return COPY_RET;
            }
            return static_cast<int64_t>(ker::vfs::vfs_chdir(path.data()));
        }
        case ops::FCHDIR: {
            int const FD = static_cast<int>(a1);
            auto* task = ker::mod::sched::get_current_task();
            return static_cast<int64_t>(ker::vfs::vfs_fchdir(task, FD));
        }
        case ops::ACCESS: {
            KernelPath path{};
            if (int const COPY_RET = copy_path_from_user(a1, path); COPY_RET < 0) {
                return COPY_RET;
            }
            int const MODE = static_cast<int>(a2);
            auto* task = ker::mod::sched::get_current_task();
            if (task != nullptr) {
                return static_cast<int64_t>(ker::vfs::vfs_faccessat(task, ker::vfs::AT_FDCWD, path.data(), MODE, 0));
            }
            return -ESRCH;
        }
        case ops::UNLINK: {
            KernelPath path{};
            if (int const COPY_RET = copy_path_from_user(a1, path); COPY_RET < 0) {
                return COPY_RET;
            }
            return static_cast<int64_t>(ker::vfs::vfs_unlink(path.data()));
        }
        case ops::RMDIR: {
            KernelPath path{};
            if (int const COPY_RET = copy_path_from_user(a1, path); COPY_RET < 0) {
                return COPY_RET;
            }
            return static_cast<int64_t>(ker::vfs::vfs_rmdir(path.data()));
        }
        case ops::RENAME: {
            KernelPath oldpath{};
            KernelPath newpath{};
            if (int const COPY_RET = copy_path_from_user(a1, oldpath); COPY_RET < 0) {
                return COPY_RET;
            }
            if (int const COPY_RET = copy_path_from_user(a2, newpath); COPY_RET < 0) {
                return COPY_RET;
            }
            return static_cast<int64_t>(ker::vfs::vfs_rename(oldpath.data(), newpath.data()));
        }
        case ops::CHMOD: {
            KernelPath path{};
            if (int const COPY_RET = copy_path_from_user(a1, path); COPY_RET < 0) {
                return COPY_RET;
            }
            int const MODE = static_cast<int>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_chmod(path.data(), MODE));
        }
        case ops::TRUNCATE: {
            int const FD = static_cast<int>(a1);
            auto length = static_cast<off_t>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_ftruncate(FD, length));
        }
        case ops::PIPE: {
            int const FLAGS = static_cast<int>(a2);
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr || !ker::mod::sys::usercopy::ensure_writable(*task, a1, sizeof(int) * 2)) {
                return -EFAULT;
            }
            std::array<int, 2> kernel_pipefd = {-1, -1};
            int const RET = ker::vfs::vfs_pipe(kernel_pipefd.data(), FLAGS);
            if (RET < 0) {
                return static_cast<int64_t>(RET);
            }
            if (!ker::mod::sys::usercopy::copy_to_task(*task, a1, kernel_pipefd.data(), sizeof(kernel_pipefd))) {
                static_cast<void>(ker::vfs::vfs_close(kernel_pipefd.at(0)));
                static_cast<void>(ker::vfs::vfs_close(kernel_pipefd.at(1)));
                return -EFAULT;
            }
            return static_cast<int64_t>(RET);
        }
        case ops::PREAD: {
            int const FD = static_cast<int>(a1);
            auto* buf = reinterpret_cast<void*>(a2);
            auto count = static_cast<size_t>(a3);
            auto offset = static_cast<off_t>(a4);
            if (!user_io_buffer_has_bounded_user_range(a2, count)) {
                return -EFAULT;
            }
            return static_cast<int64_t>(ker::vfs::vfs_pread(FD, buf, count, offset));
        }
        case ops::PWRITE: {
            int const FD = static_cast<int>(a1);
            const auto* buf = reinterpret_cast<const void*>(a2);
            auto count = static_cast<size_t>(a3);
            auto offset = static_cast<off_t>(a4);
            if (!user_io_buffer_has_bounded_user_range(a2, count)) {
                return -EFAULT;
            }
            return static_cast<int64_t>(ker::vfs::vfs_pwrite(FD, buf, count, offset));
        }
        case ops::FCNTL: {
            int const FD = static_cast<int>(a1);
            int const CMD = static_cast<int>(a2);
            constexpr int F_GETLK_CMD = 5;
            constexpr int F_SETLK_CMD = 6;
            constexpr int F_SETLKW_CMD = 7;
            constexpr int F_OFD_GETLK_CMD = 36;
            constexpr int F_OFD_SETLK_CMD = 37;
            constexpr int F_OFD_SETLKW_CMD = 38;
            bool const FLOCK_INPUT = CMD == F_GETLK_CMD || CMD == F_SETLK_CMD || CMD == F_SETLKW_CMD || CMD == F_OFD_GETLK_CMD ||
                                     CMD == F_OFD_SETLK_CMD || CMD == F_OFD_SETLKW_CMD;
            bool const FLOCK_OUTPUT = CMD == F_GETLK_CMD || CMD == F_OFD_GETLK_CMD;
            if (!FLOCK_INPUT) {
                return static_cast<int64_t>(ker::vfs::vfs_fcntl(FD, CMD, a3));
            }

            auto* task = ker::mod::sched::get_current_task();
            ker::vfs::VfsFlockAbi flock{};
            if (task == nullptr || !ker::mod::sys::usercopy::copy_value_from_task(*task, a3, flock) ||
                (FLOCK_OUTPUT && !ker::mod::sys::usercopy::ensure_writable(*task, a3, sizeof(flock)))) {
                return -EFAULT;
            }
            int const RET = ker::vfs::vfs_fcntl(FD, CMD, 0, &flock);
            if (RET >= 0 && FLOCK_OUTPUT && !ker::mod::sys::usercopy::copy_value_to_task(*task, a3, flock)) {
                return -EFAULT;
            }
            return static_cast<int64_t>(RET);
        }
        case ops::FCHMOD: {
            int const FD = static_cast<int>(a1);
            int const MODE = static_cast<int>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_fchmod(FD, MODE));
        }
        case ops::FCHMODAT: {
            int const DIRFD = static_cast<int>(a1);
            KernelPath pathname{};
            if (int const COPY_RET = copy_path_from_user(a2, pathname); COPY_RET < 0) {
                return COPY_RET;
            }
            int const MODE = static_cast<int>(a3);
            int const FLAGS = static_cast<int>(a4);
            auto* task = ker::mod::sched::get_current_task();
            return static_cast<int64_t>(ker::vfs::vfs_fchmodat(task, DIRFD, pathname.data(), MODE, FLAGS));
        }
        case ops::CHOWN: {
            KernelPath path{};
            if (int const COPY_RET = copy_path_from_user(a1, path); COPY_RET < 0) {
                return COPY_RET;
            }
            auto owner = static_cast<uint32_t>(a2);
            auto group = static_cast<uint32_t>(a3);
            return static_cast<int64_t>(ker::vfs::vfs_chown(path.data(), owner, group));
        }
        case ops::FCHOWN: {
            int const FD = static_cast<int>(a1);
            auto owner = static_cast<uint32_t>(a2);
            auto group = static_cast<uint32_t>(a3);
            return static_cast<int64_t>(ker::vfs::vfs_fchown(FD, owner, group));
        }
        case ops::FCHOWNAT: {
            int const DIRFD = static_cast<int>(a1);
            KernelPath pathname{};
            if (int const COPY_RET = copy_path_from_user(a2, pathname); COPY_RET < 0) {
                return COPY_RET;
            }
            auto owner = static_cast<uint32_t>(a3);
            auto group = static_cast<uint32_t>(a4);
            int const FLAGS = static_cast<int>(a5);
            auto* task = ker::mod::sched::get_current_task();
            return static_cast<int64_t>(ker::vfs::vfs_fchownat(task, DIRFD, pathname.data(), owner, group, FLAGS));
        }
        case ops::FACCESSAT: {
            int const DIRFD = static_cast<int>(a1);
            KernelPath pathname{};
            if (int const COPY_RET = copy_path_from_user(a2, pathname); COPY_RET < 0) {
                return COPY_RET;
            }
            int const MODE = static_cast<int>(a3);
            int const FLAGS = static_cast<int>(a4);
            auto* task = ker::mod::sched::get_current_task();
            return static_cast<int64_t>(ker::vfs::vfs_faccessat(task, DIRFD, pathname.data(), MODE, FLAGS));
        }
        case ops::UNLINKAT: {
            int const DIRFD = static_cast<int>(a1);
            KernelPath pathname{};
            if (int const COPY_RET = copy_path_from_user(a2, pathname); COPY_RET < 0) {
                return COPY_RET;
            }
            int const FLAGS = static_cast<int>(a3);
            auto* task = ker::mod::sched::get_current_task();
            return static_cast<int64_t>(ker::vfs::vfs_unlinkat(task, DIRFD, pathname.data(), FLAGS));
        }
        case ops::RENAMEAT: {
            int const OLDDIRFD = static_cast<int>(a1);
            int const NEWDIRFD = static_cast<int>(a3);
            KernelPath oldpath{};
            KernelPath newpath{};
            if (int const COPY_RET = copy_path_from_user(a2, oldpath); COPY_RET < 0) {
                return COPY_RET;
            }
            if (int const COPY_RET = copy_path_from_user(a4, newpath); COPY_RET < 0) {
                return COPY_RET;
            }
            auto* task = ker::mod::sched::get_current_task();
            return static_cast<int64_t>(ker::vfs::vfs_renameat(task, OLDDIRFD, oldpath.data(), NEWDIRFD, newpath.data()));
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
            int const MAXEVENTS = static_cast<int>(a3);
            int const TIMEOUT = static_cast<int>(static_cast<int64_t>(a4));
            if (a2 == 0 || MAXEVENTS <= 0) {
                return static_cast<int64_t>(ker::vfs::epoll_pwait(EPFD, nullptr, MAXEVENTS, TIMEOUT));
            }
            size_t const EVENT_COUNT = std::min(static_cast<size_t>(MAXEVENTS), ker::vfs::EPOLL_MAX_INTEREST);
            size_t const OUTPUT_BYTES = EVENT_COUNT * sizeof(ker::vfs::EpollEvent);
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr || !ker::mod::sys::usercopy::ensure_writable(*task, a2, OUTPUT_BYTES)) {
                return -EFAULT;
            }
            std::array<ker::vfs::EpollEvent, ker::vfs::EPOLL_MAX_INTEREST> kernel_events{};
            int const RET = ker::vfs::epoll_pwait(EPFD, kernel_events.data(), static_cast<int>(EVENT_COUNT), TIMEOUT);
            if (RET > 0) {
                if (static_cast<size_t>(RET) > EVENT_COUNT) {
                    return -EOVERFLOW;
                }
                size_t const COPY_SIZE = static_cast<size_t>(RET) * sizeof(ker::vfs::EpollEvent);
                if (!ker::mod::sys::usercopy::copy_to_task(*task, a2, kernel_events.data(), COPY_SIZE)) {
                    return -EFAULT;
                }
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
            IoctlUserMarshal const MARSHAL = pty_ioctl_user_marshal(cmd);
            if (!MARSHAL.supported) {
                ker::vfs::vfs_put_file(file);
                return -ENOTTY;
            }
            std::array<uint8_t, sizeof(ker::dev::pty::KTermios)> kernel_arg{};
            unsigned long effective_arg = arg;
            if (MARSHAL.size != 0) {
                if ((MARSHAL.copy_in && !ker::mod::sys::usercopy::copy_from_task(*task, a3, kernel_arg.data(), MARSHAL.size)) ||
                    (MARSHAL.copy_out && !ker::mod::sys::usercopy::ensure_writable(*task, a3, MARSHAL.size))) {
                    ker::vfs::vfs_put_file(file);
                    return -EFAULT;
                }
                effective_arg = reinterpret_cast<unsigned long>(kernel_arg.data());
            }

            int64_t result = -ENOTTY;
            if (file->fs_type == ker::vfs::FSType::DEVFS) {
                result = static_cast<int64_t>(ker::vfs::devfs::devfs_ioctl(file, cmd, effective_arg));
            } else if (file->fops != nullptr && file->fops->vfs_ioctl != nullptr) {
                // Fallback: fops-backed ioctl (e.g. remote PTY proxy).
                result = static_cast<int64_t>(file->fops->vfs_ioctl(file, cmd, effective_arg));
            }
            ker::vfs::vfs_put_file(file);
            if (result >= 0 && MARSHAL.copy_out && !ker::mod::sys::usercopy::copy_to_task(*task, a3, kernel_arg.data(), MARSHAL.size)) {
                return -EFAULT;
            }
            return result;
        }
        case ops::FSYNC: {
            int const FD = static_cast<int>(a1);
            return static_cast<int64_t>(ker::vfs::vfs_fsync(FD));
        }
        case ops::SYNC: {
            return static_cast<int64_t>(ker::vfs::vfs_sync());
        }
        case ops::LINK: {
            KernelPath oldpath{};
            KernelPath newpath{};
            if (int const COPY_RET = copy_path_from_user(a1, oldpath); COPY_RET < 0) {
                return COPY_RET;
            }
            if (int const COPY_RET = copy_path_from_user(a2, newpath); COPY_RET < 0) {
                return COPY_RET;
            }
            return static_cast<int64_t>(ker::vfs::vfs_link(oldpath.data(), newpath.data()));
        }
        case ops::LINKAT: {
            int const OLDDIRFD = static_cast<int>(a1);
            int const NEWDIRFD = static_cast<int>(a3);
            KernelPath oldpath{};
            KernelPath newpath{};
            if (int const COPY_RET = copy_path_from_user(a2, oldpath); COPY_RET < 0) {
                return COPY_RET;
            }
            if (int const COPY_RET = copy_path_from_user(a4, newpath); COPY_RET < 0) {
                return COPY_RET;
            }
            int const FLAGS = static_cast<int>(a5);
            auto* task = ker::mod::sched::get_current_task();
            return static_cast<int64_t>(ker::vfs::vfs_linkat(task, OLDDIRFD, oldpath.data(), NEWDIRFD, newpath.data(), FLAGS));
        }
        case ops::WKI_RULE_ADD: {
            KernelPath prefix{};
            if (int const COPY_RET = copy_path_from_user(a1, prefix); COPY_RET < 0) {
                return COPY_RET;
            }
            auto route = static_cast<uint32_t>(a2);
            return static_cast<int64_t>(ker::vfs::vfs_wki_rule_add(prefix.data(), route));
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
            KernelPath new_root{};
            KernelPath put_old{};
            if (int const COPY_RET = copy_path_from_user(a1, new_root); COPY_RET < 0) {
                return COPY_RET;
            }
            if (int const COPY_RET = copy_path_from_user(a2, put_old); COPY_RET < 0) {
                return COPY_RET;
            }
            return static_cast<int64_t>(ker::vfs::vfs_pivot_root(new_root.data(), put_old.data()));
        }
        case ops::STATVFS: {
            KernelPath path{};
            if (int const COPY_RET = copy_path_from_user(a1, path); COPY_RET < 0) {
                return COPY_RET;
            }
            auto* buf = reinterpret_cast<ker::vfs::Statvfs*>(a2);
            ker::vfs::Statvfs kernel_buf{};
            int const RET = ker::vfs::vfs_statvfs(path.data(), &kernel_buf);
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
            KernelPath path{};
            if (int const COPY_RET = copy_path_from_user(a1, path); COPY_RET < 0) {
                return COPY_RET;
            }
            auto bufsize = static_cast<size_t>(a3);
            if (bufsize == 0) {
                return static_cast<int64_t>(ker::vfs::vfs_realpath(path.data(), nullptr, 0));
            }
            if (a2 == 0) {
                return -EFAULT;
            }

            std::array<char, REALPATH_STACK_BUFFER_SIZE> stack_buf{};
            size_t kernel_bufsize = std::min(bufsize, stack_buf.size());
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr || !ker::mod::sys::usercopy::ensure_writable(*task, a2, kernel_bufsize)) {
                return -EFAULT;
            }
            size_t len = 0;
            int const RET = ker::vfs::vfs_realpath(path.data(), stack_buf.data(), kernel_bufsize, &len);
            if (RET < 0) {
                return static_cast<int64_t>(RET);
            }
            if (len >= kernel_bufsize) {
                return -EOVERFLOW;
            }
            if (!ker::mod::sys::usercopy::copy_to_task(*task, a2, stack_buf.data(), len + 1)) {
                return -EFAULT;
            }
            return static_cast<int64_t>(RET);
        }
        default:
            ker::vfs::vfs_debug_log("sys_vfs: unknown op\n");
            return static_cast<int64_t>(-ENOSYS);
    }
}

}  // namespace ker::syscall::vfs
