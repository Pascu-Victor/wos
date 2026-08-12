#include "sys_net.hpp"

#include <abi/callnums/net.h>
#include <bits/posix/posix_string.h>
#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <net/backlog.hpp>
#include <net/endian.hpp>
#include <net/netdevice.hpp>
#include <net/netif.hpp>
#include <net/netpoll.hpp>
#include <net/route.hpp>
#include <net/route6.hpp>
#include <net/socket.hpp>
#include <net/wki/dev_server.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/remote_ipc.hpp>
#include <new>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/usercopy.hpp>
#include <span>
#include <string_view>
#include <utility>
#include <vfs/file.hpp>
#include <vfs/vfs.hpp>

#include "platform/ktime/ktime.hpp"
#include "vfs/file_operations.hpp"

// The net syscall layer mirrors user-visible ABI records and POSIX poll/ioctl
// buffers, so several fixed C-style arrays and indexed fd buffers are kept by
// design at this boundary.
// NOLINTBEGIN(cppcoreguidelines-avoid-c-arrays, modernize-avoid-c-arrays, cppcoreguidelines-pro-bounds-array-to-pointer-decay,
// cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
namespace ker::syscall::net {

namespace {
constexpr int WOS_O_NONBLOCK = 04000;
constexpr int WOS_O_RDWR = 2;
constexpr int WOS_POLLIN = 0x0001;
constexpr int WOS_POLLPRI = 0x0002;
constexpr int WOS_POLLOUT = 0x0004;
constexpr int WOS_POLLERR = 0x0008;
constexpr int WOS_POLLHUP = 0x0010;
constexpr int WOS_POLLNVAL = 0x0020;
constexpr uint64_t USEC_PER_MSEC = 1000;
constexpr size_t WOS_FD_SETSIZE = 1024;
constexpr size_t WOS_FD_SET_BYTES = WOS_FD_SETSIZE / 8;
constexpr int64_t SELECT_USEC_PER_SEC = 1000000;
constexpr int SELECT_TIMEOUT_MAX_MS = 0x7fffffff;
constexpr size_t SOCKET_IO_BOUNCE_STACK_CHUNK = 4096;
constexpr size_t SOCKET_IO_BOUNCE_MAX_CHUNK = size_t{256} * 1024;
// Match the userspace sockaddr_storage ABI. Individual protocol parsers still
// require their exact minimum/family-specific lengths.
constexpr size_t SOCKADDR_STORAGE_MAX = 128;
constexpr size_t SOCKET_OPTION_MAX = (size_t{64} * 1024) - 16;

struct KPollFd {
    int32_t fd;
    int16_t events;
    int16_t revents;
};

struct KSelectTimeval {
    int64_t tv_sec;
    int64_t tv_usec;
};
static_assert(sizeof(KSelectTimeval) == 16);

auto poll_timeout_us_from_ms(int timeout_ms) -> uint64_t {
    auto const TIMEOUT_MS = static_cast<uint64_t>(timeout_ms);
    if (TIMEOUT_MS > UINT64_MAX / USEC_PER_MSEC) {
        return UINT64_MAX;
    }
    return TIMEOUT_MS * USEC_PER_MSEC;
}

auto poll_deadline_after_ms(int timeout_ms) -> uint64_t {
    uint64_t const TIMEOUT_US = poll_timeout_us_from_ms(timeout_ms);
    uint64_t const NOW_US = ker::mod::time::get_us();
    if (UINT64_MAX - NOW_US < TIMEOUT_US) {
        return UINT64_MAX;
    }
    return NOW_US + TIMEOUT_US;
}

auto register_poll_waiter(ker::vfs::File* file, uint64_t pid) -> int {
    if (file == nullptr) {
        return 0;
    }
    if (file->fs_type == ker::vfs::FSType::SOCKET) {
        auto* sock = static_cast<ker::net::Socket*>(file->private_data);
        if (sock == nullptr) {
            return 0;
        }
        return ker::net::socket_register_waiter(sock, pid) ? 1 : -ENOMEM;
    }
    if (file->fops != nullptr && file->fops->vfs_poll_register_waiter != nullptr) {
        return file->fops->vfs_poll_register_waiter(file, pid) ? 1 : 0;
    }
    return 0;
}

auto poll_wait_kind_for_file(ker::vfs::File* file) -> ker::mod::sched::task::WaitChannelKind {
    if (file == nullptr || file->fops == nullptr || file->fops->vfs_poll_wait_kind == nullptr) {
        return ker::mod::sched::task::WaitChannelKind::GENERIC;
    }
    return file->fops->vfs_poll_wait_kind(file);
}

auto merge_poll_wait_kind(ker::mod::sched::task::WaitChannelKind current, ker::mod::sched::task::WaitChannelKind candidate)
    -> ker::mod::sched::task::WaitChannelKind {
    if (current == ker::mod::sched::task::WaitChannelKind::GENERIC || current == ker::mod::sched::task::WaitChannelKind::NONE) {
        return candidate;
    }
    return current;
}

auto begin_poll_timeout(ker::mod::sched::task::Task* task, int timeout_ms) -> uint64_t {
    if (task == nullptr || timeout_ms <= 0) {
        return 0;
    }
    if (task->poll_wait_deadline_us == 0) {
        task->poll_wait_deadline_us = poll_deadline_after_ms(timeout_ms);
    }
    return task->poll_wait_deadline_us;
}

void clear_poll_timeout(ker::mod::sched::task::Task* task) {
    if (task != nullptr) {
        task->poll_wait_deadline_us = 0;
    }
}

auto current_task_has_deliverable_signal() -> bool {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return false;
    }
    return task->has_interrupting_signal_pending();
}

auto clamp_io_count(ssize_t result, size_t requested) -> ssize_t {
    if (result <= 0 || std::cmp_less_equal(result, requested)) {
        return result;
    }
    return static_cast<ssize_t>(requested);
}

void drain_network_rx_work() {
    ker::net::napi_poll_all_pending();
    ker::net::backlog_drain_all_pending_inline();
}

auto poll_ready_for_file(ker::vfs::File* file, int16_t events) -> int16_t {
    if (file == nullptr) {
        return static_cast<int16_t>(WOS_POLLNVAL);
    }
    if (file->fs_type == ker::vfs::FSType::SOCKET) {
        auto* sock = static_cast<ker::net::Socket*>(file->private_data);
        if (sock != nullptr && sock->proto_ops != nullptr && sock->proto_ops->poll_check != nullptr) {
            return static_cast<int16_t>(sock->proto_ops->poll_check(sock, events));
        }
        return 0;
    }
    if (file->fops != nullptr && file->fops->vfs_poll_check != nullptr) {
        return static_cast<int16_t>(file->fops->vfs_poll_check(file, events));
    }
    // Non-socket fds without poll_check (regular files, devices) are always
    // ready for the regular read/write readiness classes they requested.
    return static_cast<int16_t>(events & (WOS_POLLIN | WOS_POLLOUT));
}

auto scan_poll_fds(ker::mod::sched::task::Task* task, KPollFd* fds, size_t nfds) -> int {
    int num_events = 0;
    for (size_t i = 0; i < nfds; i++) {
        fds[i].revents = 0;

        if (fds[i].fd < 0) {
            continue;
        }

        auto* file = ker::vfs::vfs_get_file_retain(task, fds[i].fd);
        if (file == nullptr) {
            fds[i].revents = static_cast<int16_t>(WOS_POLLNVAL);
            num_events++;
            continue;
        }

        fds[i].revents = poll_ready_for_file(file, fds[i].events);
        ker::vfs::vfs_put_file(file);

        if (fds[i].revents != 0) {
            num_events++;
        }
    }
    return num_events;
}

auto register_poll_waiters(KPollFd* fds, size_t nfds, ker::mod::sched::task::Task* task,
                           ker::mod::sched::task::WaitChannelKind& poll_wait_kind) -> int {
    bool can_block = (nfds > 0);
    if (!can_block) {
        return 0;
    }
    for (size_t i = 0; i < nfds; i++) {
        if (fds[i].fd < 0) {
            continue;
        }
        auto* file = ker::vfs::vfs_get_file_retain(task, fds[i].fd);
        int const REGISTERED = (file != nullptr) ? register_poll_waiter(file, task->pid) : 0;
        if (REGISTERED > 0) {
            poll_wait_kind = merge_poll_wait_kind(poll_wait_kind, poll_wait_kind_for_file(file));
        }
        if (file != nullptr) {
            ker::vfs::vfs_put_file(file);
        }
        if (REGISTERED < 0) {
            return REGISTERED;
        }
        if (REGISTERED == 0) {
            can_block = false;
            break;
        }
    }
    return can_block ? 1 : 0;
}

auto run_poll_wait(KPollFd* fds, size_t nfds, int timeout, const char* wait_channel) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || (fds == nullptr && nfds != 0)) {
        return -EINVAL;
    }

    for (;;) {
        uint64_t const DEADLINE_US = begin_poll_timeout(task, timeout);

        drain_network_rx_work();

        int const NUM_EVENTS = scan_poll_fds(task, fds, nfds);
        if (NUM_EVENTS > 0 || timeout == 0) {
            clear_poll_timeout(task);
            return NUM_EVENTS;
        }

        if (DEADLINE_US != 0 && ker::mod::time::get_us() >= DEADLINE_US) {
            clear_poll_timeout(task);
            return 0;
        }

        if (current_task_has_deliverable_signal()) {
            clear_poll_timeout(task);
            return -EINTR;
        }

        auto poll_wait_kind = ker::mod::sched::task::WaitChannelKind::GENERIC;
        int const CAN_BLOCK = register_poll_waiters(fds, nfds, task, poll_wait_kind);
        if (CAN_BLOCK < 0) {
            clear_poll_timeout(task);
            return CAN_BLOCK;
        }

        if (CAN_BLOCK != 0) {
            drain_network_rx_work();

            // Re-check fds after waiter registration to close the race window
            // where an event fires between the initial scan and sleep.
            int const RECHECK = scan_poll_fds(task, fds, nfds);
            if (RECHECK > 0) {
                clear_poll_timeout(task);
                return RECHECK;
            }

            if (current_task_has_deliverable_signal()) {
                clear_poll_timeout(task);
                return -EINTR;
            }

            ker::mod::sched::preemptible_syscall_park(wait_channel, poll_wait_kind, DEADLINE_US);
        } else {
            ker::mod::sched::kern_yield();
        }
    }
}

auto select_fd_is_set(size_t fd, const uint8_t* set) -> bool {
    return set != nullptr && (set[fd / 8] & static_cast<uint8_t>(1U << (fd % 8))) != 0;
}

void select_fd_set_bit(size_t fd, uint8_t* set) {
    if (set != nullptr) {
        set[fd / 8] |= static_cast<uint8_t>(1U << (fd % 8));
    }
}

void select_fd_zero(uint8_t* set) {
    if (set != nullptr) {
        std::memset(set, 0, WOS_FD_SET_BYTES);
    }
}

auto select_timeout_ms(const KSelectTimeval* timeout, int& timeout_ms) -> int {
    timeout_ms = -1;
    if (timeout == nullptr) {
        return 0;
    }
    if (timeout->tv_sec < 0 || timeout->tv_usec < 0 || timeout->tv_usec >= SELECT_USEC_PER_SEC) {
        return -EINVAL;
    }
    if (timeout->tv_sec > SELECT_TIMEOUT_MAX_MS / 1000) {
        timeout_ms = SELECT_TIMEOUT_MAX_MS;
        return 0;
    }

    int64_t timeout_ms64 = timeout->tv_sec * 1000;
    timeout_ms64 += (timeout->tv_usec + static_cast<int64_t>(USEC_PER_MSEC - 1)) / static_cast<int64_t>(USEC_PER_MSEC);
    if (timeout_ms64 > SELECT_TIMEOUT_MAX_MS) {
        timeout_ms = SELECT_TIMEOUT_MAX_MS;
        return 0;
    }
    timeout_ms = static_cast<int>(timeout_ms64);
    return 0;
}

auto select_events_for_fd(size_t fd, const uint8_t* readfds, const uint8_t* writefds, const uint8_t* exceptfds) -> int16_t {
    int16_t events = 0;
    if (select_fd_is_set(fd, readfds)) {
        events = static_cast<int16_t>(events | WOS_POLLIN);
    }
    if (select_fd_is_set(fd, writefds)) {
        events = static_cast<int16_t>(events | WOS_POLLOUT);
    }
    if (select_fd_is_set(fd, exceptfds)) {
        events = static_cast<int16_t>(events | WOS_POLLPRI);
    }
    return events;
}

auto run_select(size_t nfds, uint8_t* readfds, uint8_t* writefds, uint8_t* exceptfds, const KSelectTimeval* timeout) -> int {
    int timeout_ms = -1;
    int const TIMEOUT_RET = select_timeout_ms(timeout, timeout_ms);
    if (TIMEOUT_RET < 0) {
        return TIMEOUT_RET;
    }

    size_t watched_fds = 0;
    for (size_t fd = 0; fd < nfds; fd++) {
        if (select_events_for_fd(fd, readfds, writefds, exceptfds) != 0) {
            watched_fds++;
        }
    }

    KPollFd* poll_fds = nullptr;
    if (watched_fds != 0) {
        poll_fds = new (std::nothrow) KPollFd[watched_fds]{};
        if (poll_fds == nullptr) {
            return -ENOMEM;
        }
    }

    size_t out = 0;
    for (size_t fd = 0; fd < nfds; fd++) {
        int16_t const EVENTS = select_events_for_fd(fd, readfds, writefds, exceptfds);
        if (EVENTS == 0) {
            continue;
        }
        poll_fds[out++] = KPollFd{.fd = static_cast<int32_t>(fd), .events = EVENTS, .revents = 0};
    }

    int const READY = run_poll_wait(poll_fds, watched_fds, timeout_ms, "select");
    if (READY <= 0) {
        delete[] poll_fds;
        if (READY == 0) {
            select_fd_zero(readfds);
            select_fd_zero(writefds);
            select_fd_zero(exceptfds);
        }
        return READY;
    }

    for (size_t i = 0; i < watched_fds; i++) {
        if ((poll_fds[i].revents & WOS_POLLNVAL) != 0) {
            delete[] poll_fds;
            return -EBADF;
        }
    }

    select_fd_zero(readfds);
    select_fd_zero(writefds);
    select_fd_zero(exceptfds);

    int selected = 0;
    for (size_t i = 0; i < watched_fds; i++) {
        size_t const FD = static_cast<size_t>(poll_fds[i].fd);
        int16_t const EVENTS = poll_fds[i].events;
        int16_t const REVENTS = poll_fds[i].revents;
        if ((EVENTS & WOS_POLLIN) != 0 && (REVENTS & (WOS_POLLIN | WOS_POLLHUP | WOS_POLLERR)) != 0) {
            select_fd_set_bit(FD, readfds);
            selected++;
        }
        if ((EVENTS & WOS_POLLOUT) != 0 && (REVENTS & WOS_POLLOUT) != 0) {
            select_fd_set_bit(FD, writefds);
            selected++;
        }
        if ((EVENTS & WOS_POLLPRI) != 0 && (REVENTS & (WOS_POLLPRI | WOS_POLLHUP | WOS_POLLERR)) != 0) {
            select_fd_set_bit(FD, exceptfds);
            selected++;
        }
    }

    delete[] poll_fds;
    return selected;
}

auto socket_call_effective_nonblock(const ker::vfs::File* file, const ker::net::Socket* sock, int call_flags) -> bool {
    bool const FILE_NONBLOCK = file != nullptr && (file->open_flags & WOS_O_NONBLOCK) != 0;
    bool const MSG_NONBLOCK = (call_flags & ker::net::SOCKET_MSG_DONTWAIT) != 0;
    bool const SOCKET_NONBLOCK = sock != nullptr && sock->nonblock;
    return FILE_NONBLOCK || MSG_NONBLOCK || SOCKET_NONBLOCK;
}

void checkpoint_blocking_socket_send_progress(ker::vfs::File* file, ker::net::Socket* sock, int call_flags, ssize_t result) {
    if (result <= 0 || socket_call_effective_nonblock(file, sock, call_flags)) {
        return;
    }

    drain_network_rx_work();
}

template <typename T, typename Fn>
auto run_socket_call(ker::vfs::File* file, ker::net::Socket* sock, int call_flags, Fn&& fn) -> T {
    bool const EFFECTIVE_NONBLOCK = socket_call_effective_nonblock(file, sock, call_flags);
    int const EFFECTIVE_FLAGS = EFFECTIVE_NONBLOCK ? (call_flags | ker::net::SOCKET_MSG_DONTWAIT) : call_flags;
    for (;;) {
        T const RESULT = fn(EFFECTIVE_FLAGS);
        bool const RETRYABLE = RESULT == static_cast<T>(-EAGAIN) || RESULT == static_cast<T>(-EINPROGRESS);
        if (!RETRYABLE) {
            return RESULT;
        }
        auto* task = ker::mod::sched::get_current_task();
        if (task == nullptr) {
            return RESULT;
        }
        if (EFFECTIVE_NONBLOCK) {
            drain_network_rx_work();
            return fn(EFFECTIVE_FLAGS);
        }
        char const* wait_channel = task->wait_channel != nullptr ? task->wait_channel : "sock_wait";
        auto const WAIT_CHANNEL_KIND =
            task->wait_channel != nullptr ? task->wait_channel_kind : ker::mod::sched::task::WaitChannelKind::GENERIC;
        if (current_task_has_deliverable_signal()) {
            task->clear_wait_channel();
            return static_cast<T>(-EINTR);
        }

        drain_network_rx_work();
        T const AFTER_DRAIN = fn(EFFECTIVE_FLAGS);
        if (AFTER_DRAIN != static_cast<T>(-EAGAIN) && AFTER_DRAIN != static_cast<T>(-EINPROGRESS)) {
            return AFTER_DRAIN;
        }

        ker::mod::sched::preemptible_syscall_park(wait_channel, WAIT_CHANNEL_KIND);
        if (current_task_has_deliverable_signal()) {
            return static_cast<T>(-EINTR);
        }
    }
}

auto socket_bounce_heap_or_stack(std::unique_ptr<uint8_t[]>& heap, uint8_t* stack, size_t stack_size, size_t size) -> uint8_t* {
    if (size <= stack_size) {
        return stack;
    }

    heap.reset(new (std::nothrow) uint8_t[size]);
    return heap.get();
}

template <typename Fn>
auto socket_send_user_bounced(ker::vfs::File* file, ker::net::Socket* sock, uint64_t user_addr, size_t count, int call_flags, Fn&& fn)
    -> ssize_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }
    if (count != 0 && !ker::mod::sys::usercopy::range_valid(user_addr, count)) {
        return -EFAULT;
    }
    if (count == 0) {
        return run_socket_call<ssize_t>(file, sock, call_flags, [&](int flags) { return fn(nullptr, size_t{0}, flags); });
    }

    uint8_t stack_bounce[SOCKET_IO_BOUNCE_STACK_CHUNK];
    size_t const TO_COPY = std::min(count, SOCKET_IO_BOUNCE_MAX_CHUNK);
    std::unique_ptr<uint8_t[]> heap_bounce{};
    uint8_t* const bounce = socket_bounce_heap_or_stack(heap_bounce, stack_bounce, sizeof(stack_bounce), TO_COPY);
    if (bounce == nullptr) {
        return -ENOMEM;
    }
    if (!ker::mod::sys::usercopy::copy_from_task(*task, user_addr, bounce, TO_COPY)) {
        return -EFAULT;
    }

    ssize_t const RESULT =
        clamp_io_count(run_socket_call<ssize_t>(file, sock, call_flags, [&](int flags) { return fn(bounce, TO_COPY, flags); }), TO_COPY);
    checkpoint_blocking_socket_send_progress(file, sock, call_flags, RESULT);
    return RESULT;
}

template <typename Fn>
auto socket_recv_user_bounced(ker::vfs::File* file, ker::net::Socket* sock, uint64_t user_addr, size_t count, int call_flags, Fn&& fn)
    -> ssize_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }
    if (count == 0) {
        return 0;
    }
    if (!ker::mod::sys::usercopy::range_valid(user_addr, count)) {
        return -EFAULT;
    }

    uint8_t stack_bounce[SOCKET_IO_BOUNCE_STACK_CHUNK];
    size_t const TO_READ = std::min(count, SOCKET_IO_BOUNCE_MAX_CHUNK);
    if (!ker::mod::sys::usercopy::ensure_writable(*task, user_addr, TO_READ)) {
        return -EFAULT;
    }

    std::unique_ptr<uint8_t[]> heap_bounce{};
    uint8_t* const bounce = socket_bounce_heap_or_stack(heap_bounce, stack_bounce, sizeof(stack_bounce), TO_READ);
    if (bounce == nullptr) {
        return -ENOMEM;
    }

    ssize_t const RESULT =
        clamp_io_count(run_socket_call<ssize_t>(file, sock, call_flags, [&](int flags) { return fn(bounce, TO_READ, flags); }), TO_READ);
    if (RESULT <= 0) {
        return RESULT;
    }

    auto const BYTES_READ = static_cast<size_t>(RESULT);
    auto const OUTPUT = ker::mod::sys::usercopy::copy_to_task_partial(*task, user_addr, bounce, BYTES_READ);
    if (!OUTPUT.complete(BYTES_READ)) {
        return OUTPUT.bytes_copied != 0 ? static_cast<ssize_t>(OUTPUT.bytes_copied) : static_cast<ssize_t>(-EFAULT);
    }
    return RESULT;
}

// Socket file operations (integrate sockets with VFS fd table)
int socket_fops_close(ker::vfs::File* f) {
    if (f == nullptr || f->private_data == nullptr) {
        return -EINVAL;
    }
    auto* sock = static_cast<ker::net::Socket*>(f->private_data);
    ker::net::socket_destroy(sock);
    f->private_data = nullptr;
    return 0;
}

ssize_t socket_fops_read(ker::vfs::File* f, void* buf, size_t count, size_t /*unused*/) {
    if (f == nullptr || f->private_data == nullptr) {
        return -EINVAL;
    }
    auto* sock = static_cast<ker::net::Socket*>(f->private_data);
    if (sock->proto_ops == nullptr || sock->proto_ops->recv == nullptr) {
        return -ENOSYS;
    }
    return clamp_io_count(run_socket_call<ssize_t>(f, sock, 0, [&](int flags) { return sock->proto_ops->recv(sock, buf, count, flags); }),
                          count);
}

ssize_t socket_fops_write(ker::vfs::File* f, const void* buf, size_t count, size_t /*unused*/) {
    if (f == nullptr || f->private_data == nullptr) {
        return -EINVAL;
    }
    auto* sock = static_cast<ker::net::Socket*>(f->private_data);
    if (sock->proto_ops == nullptr || sock->proto_ops->send == nullptr) {
        return -ENOSYS;
    }
    ssize_t const RESULT = clamp_io_count(
        run_socket_call<ssize_t>(f, sock, 0, [&](int flags) { return sock->proto_ops->send(sock, buf, count, flags); }), count);
    checkpoint_blocking_socket_send_progress(f, sock, 0, RESULT);
    return RESULT;
}

ker::vfs::FileOperations socket_fops = {
    .vfs_open = nullptr,
    .vfs_close = socket_fops_close,
    .vfs_read = socket_fops_read,
    .vfs_write = socket_fops_write,
    .vfs_lseek = nullptr,
    .vfs_isatty = nullptr,
    .vfs_readdir = nullptr,
    .vfs_readlink = nullptr,
    .vfs_truncate = nullptr,
    .vfs_poll_check = nullptr,  // Sockets use SocketProtoOps::poll_check instead
    .vfs_poll_register_waiter = nullptr,
    .vfs_ioctl = nullptr,
};

struct SocketHandle {
    ker::vfs::File* file = nullptr;
    ker::net::Socket* sock = nullptr;

    ~SocketHandle() {
        if (file != nullptr) {
            ker::vfs::vfs_put_file(file);
        }
    }

    SocketHandle() = default;
    SocketHandle(const SocketHandle&) = delete;
    auto operator=(const SocketHandle&) -> SocketHandle& = delete;
    SocketHandle(SocketHandle&& other) noexcept : file(std::exchange(other.file, nullptr)), sock(std::exchange(other.sock, nullptr)) {}
    auto operator=(SocketHandle&& other) noexcept -> SocketHandle& {
        if (this != &other) {
            if (file != nullptr) {
                ker::vfs::vfs_put_file(file);
            }
            file = std::exchange(other.file, nullptr);
            sock = std::exchange(other.sock, nullptr);
        }
        return *this;
    }
};

struct FileHandle {
    ker::vfs::File* file = nullptr;

    ~FileHandle() {
        if (file != nullptr) {
            ker::vfs::vfs_put_file(file);
        }
    }

    FileHandle() = default;
    FileHandle(const FileHandle&) = delete;
    auto operator=(const FileHandle&) -> FileHandle& = delete;
    FileHandle(FileHandle&& other) noexcept : file(std::exchange(other.file, nullptr)) {}
    auto operator=(FileHandle&& other) noexcept -> FileHandle& {
        if (this != &other) {
            if (file != nullptr) {
                ker::vfs::vfs_put_file(file);
            }
            file = std::exchange(other.file, nullptr);
        }
        return *this;
    }
};

// Get socket from fd using VFS helpers
auto fd_to_socket(uint64_t fd_num) -> SocketHandle {
    SocketHandle handle;
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return handle;
    }

    handle.file = ker::vfs::vfs_get_file_retain(task, static_cast<int>(fd_num));
    if (handle.file == nullptr || handle.file->fs_type != ker::vfs::FSType::SOCKET) {
        if (handle.file != nullptr) {
            ker::vfs::vfs_put_file(handle.file);
            handle.file = nullptr;
        }
        return handle;
    }

    handle.sock = static_cast<ker::net::Socket*>(handle.file->private_data);
    return handle;
}

auto fd_to_file(uint64_t fd_num) -> FileHandle {
    FileHandle handle;
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return handle;
    }

    handle.file = ker::vfs::vfs_get_file_retain(task, static_cast<int>(fd_num));
    return handle;
}

// Allocate fd for a socket using VFS helpers
auto allocate_socket_fd(ker::net::Socket* sock) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -1;
    }

    auto* file = new (std::nothrow) ker::vfs::File{};
    if (file == nullptr) {
        return -1;
    }

    file->private_data = sock;
    file->fops = &socket_fops;
    file->pos = 0;
    file->open_flags = WOS_O_RDWR | (sock->nonblock ? WOS_O_NONBLOCK : 0);
    file->is_directory = false;
    file->fs_type = ker::vfs::FSType::SOCKET;
    file->refcount = 1;

    int const FD = ker::vfs::vfs_alloc_fd(task, file);
    if (FD < 0) {
        delete file;
        return -1;
    }
    return FD;
}

// Determine address length from socket domain
auto addr_len_for_domain(int domain) -> size_t {
    if (domain == 10) {  // AF_INET6
        return 28;       // sizeof(sockaddr_in6)
    }
    return 16;  // sizeof(sockaddr_in) with padding
}

auto copy_sockaddr_io_v1_from_user(ker::mod::sched::task::Task& task, uint64_t user_addr, ker::abi::net::SockaddrIoV1& io) -> int {
    if (user_addr == 0 || !ker::mod::sys::usercopy::copy_value_from_task(task, user_addr, io)) {
        return -EFAULT;
    }
    if (io.size < sizeof(io) || io.version != ker::abi::net::SOCKADDR_IO_VERSION_1 || io.reserved != 0 ||
        io.address_length > SOCKADDR_STORAGE_MAX) {
        return -EINVAL;
    }
    if (io.address == 0 && io.address_length != 0) {
        return -EINVAL;
    }
    return 0;
}

template <typename Fn>
auto socket_name_to_user(uint64_t addr_out_addr, uint64_t addr_len_addr, size_t default_capacity, Fn&& fn) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }
    if (addr_out_addr == 0 || addr_len_addr == 0) {
        return -EFAULT;
    }

    size_t user_capacity = default_capacity;
    if (!ker::mod::sys::usercopy::copy_value_from_task(*task, addr_len_addr, user_capacity) ||
        !ker::mod::sys::usercopy::ensure_writable(*task, addr_len_addr, sizeof(user_capacity))) {
        return -EFAULT;
    }

    std::array<uint8_t, SOCKADDR_STORAGE_MAX> address{};
    size_t const SNAPSHOT_LEN = std::min(user_capacity, address.size());
    if (SNAPSHOT_LEN != 0 && (!ker::mod::sys::usercopy::ensure_writable(*task, addr_out_addr, SNAPSHOT_LEN) ||
                              !ker::mod::sys::usercopy::copy_from_task(*task, addr_out_addr, address.data(), SNAPSHOT_LEN))) {
        return -EFAULT;
    }

    // Backends treat *addr_len as the writable capacity.  Never advertise the
    // (possibly enormous) userspace capacity for our fixed kernel snapshot.
    // They may still replace it with the full sockaddr size for copy-back.
    size_t returned_len = SNAPSHOT_LEN;
    int const RESULT = fn(address.data(), &returned_len);
    if (RESULT < 0) {
        return RESULT;
    }

    size_t const COPY_LEN = std::min({user_capacity, returned_len, address.size()});
    if ((COPY_LEN != 0 && !ker::mod::sys::usercopy::copy_to_task(*task, addr_out_addr, address.data(), COPY_LEN)) ||
        !ker::mod::sys::usercopy::copy_value_to_task(*task, addr_len_addr, returned_len)) {
        return -EFAULT;
    }
    return 0;
}

constexpr size_t WOS_NET_IF_NAME_LEN = 16;
constexpr size_t WOS_NET_HWADDR_LEN = 8;
constexpr size_t WOS_NET_ADDR_LEN = 16;

constexpr uint32_t IFF_UP = 0x0001;
constexpr uint32_t IFF_BROADCAST = 0x0002;
constexpr uint32_t IFF_LOOPBACK = 0x0008;
constexpr uint32_t IFF_RUNNING = 0x0040;
constexpr uint32_t IFF_NOARP = 0x0080;
constexpr uint32_t IFF_PROMISC = 0x0100;
constexpr uint32_t IFF_MULTICAST = 0x1000;
constexpr uint32_t IFF_LOWER_UP = 0x10000;

constexpr uint16_t WOS_AF_INET = 2;
constexpr uint16_t WOS_AF_INET6 = 10;
constexpr int WOS_SOCK_STREAM = 1;
constexpr int WOS_SOCK_DGRAM = 2;
constexpr int WOS_SOCK_RAW = 3;
constexpr int WOS_SOCK_TYPE_MASK = 0x0F;
constexpr int WOS_IPPROTO_TCP = 6;
constexpr int WOS_IPPROTO_UDP = 17;
constexpr uint16_t WOS_ARPHRD_ETHER = 1;
constexpr uint16_t WOS_ARPHRD_LOOPBACK = 772;
constexpr uint32_t WOS_IFA_F_DADFAILED = 0x08;
constexpr uint32_t WOS_IFA_F_DEPRECATED = 0x20;
constexpr uint32_t WOS_IFA_F_TENTATIVE = 0x40;
constexpr uint32_t WOS_IFA_F_PERMANENT = 0x80;
constexpr uint32_t WOS_IFA_F_NOPREFIXROUTE = 0x200;
constexpr uint32_t WOS_IFA_F_AUTOCONF = 0x00010000;
constexpr uint32_t WOS_IFA_F_NODAD = 0x00020000;
constexpr uint32_t WOS_IPV6_ADDR_STATE_FLAGS = WOS_IFA_F_DADFAILED | WOS_IFA_F_DEPRECATED | WOS_IFA_F_TENTATIVE;
constexpr uint32_t WOS_IPV6_ADDR_INPUT_FLAGS = WOS_IFA_F_PERMANENT | WOS_IFA_F_NOPREFIXROUTE | WOS_IFA_F_AUTOCONF | WOS_IFA_F_NODAD;

constexpr uint16_t WOS_NETCTL_VERSION_1 = 1;
constexpr uint32_t WOS_NET_ROUTE_F_GATEWAY = 1U << 0;
constexpr uint32_t WOS_NET_ROUTE_F_AUTOCONF = 1U << 1;

constexpr uint32_t WOS_NET_LINK_SET_FLAGS = 1U << 0;
constexpr uint32_t WOS_NET_LINK_SET_MTU = 1U << 1;
constexpr uint32_t WOS_NET_LINK_SET_TXQLEN = 1U << 2;
constexpr uint32_t WOS_NET_LINK_SET_NAME = 1U << 3;
constexpr uint32_t WOS_NET_LINK_SET_HWADDR = 1U << 4;

template <size_t N>
void copy_cstr_trunc(std::span<char, N> dest, std::string_view src) {
    static_assert(N > 0);
    std::ranges::fill(dest, '\0');
    std::ranges::copy_n(src.begin(), std::min(src.size(), N - 1), dest.begin());
}

template <typename T>
auto load_unaligned(const uint8_t* src) -> T {
    T value{};
    std::memcpy(&value, src, sizeof(value));
    return value;
}

template <typename T>
void store_unaligned(uint8_t* dest, T value) {
    std::memcpy(dest, &value, sizeof(value));
}

struct WosNetIfInfo {
    uint32_t ifindex;
    char name[WOS_NET_IF_NAME_LEN];
    uint32_t flags;
    uint32_t mtu;
    uint32_t tx_queue_len;
    uint16_t type;
    uint8_t addr_len;
    uint8_t operstate;
    uint8_t addr[WOS_NET_HWADDR_LEN];
    uint8_t broadcast[WOS_NET_HWADDR_LEN];
};

struct WosNetAddrInfo {
    uint32_t ifindex;
    uint16_t family;
    uint8_t prefix_len;
    uint8_t scope;
    uint32_t flags;
    char label[WOS_NET_IF_NAME_LEN];
    uint8_t address[WOS_NET_ADDR_LEN];
    uint8_t local[WOS_NET_ADDR_LEN];
    uint8_t broadcast[WOS_NET_ADDR_LEN];
};

struct WosNetAddrReq {
    uint32_t ifindex;
    uint16_t family;
    uint8_t prefix_len;
    uint8_t scope;
    uint32_t flags;
    uint8_t address[WOS_NET_ADDR_LEN];
    uint8_t local[WOS_NET_ADDR_LEN];
    uint8_t replace;
};

struct WosNetAddrReqV2 {
    uint32_t size;
    uint16_t version;
    uint16_t reserved;
    WosNetAddrReq address;
    uint32_t preferred_lifetime_s;
    uint32_t valid_lifetime_s;
};

struct WosNetRouteRecord {
    uint32_t size;
    uint16_t version;
    uint16_t family;
    uint32_t ifindex;
    uint32_t metric;
    uint32_t flags;
    uint8_t prefix_len;
    uint8_t scope;
    uint16_t reserved;
    uint8_t destination[WOS_NET_ADDR_LEN];
    uint8_t gateway[WOS_NET_ADDR_LEN];
    uint32_t lifetime_s;
    uint32_t reserved2;
};

struct WosNetLinkSetReq {
    uint32_t ifindex;
    char ifname[WOS_NET_IF_NAME_LEN];
    uint32_t fields;
    uint32_t flags;
    uint32_t flag_mask;
    uint32_t mtu;
    uint32_t tx_queue_len;
    char new_name[WOS_NET_IF_NAME_LEN];
    uint8_t hwaddr[WOS_NET_HWADDR_LEN];
    uint8_t hwaddr_len;
};

// Keep the kernel's private spellings mechanically locked to the public
// <wos/netctl.h> wire records. These are copied as complete fixed-size values.
static_assert(sizeof(WosNetIfInfo) == 52);
static_assert(sizeof(WosNetAddrInfo) == 76);
static_assert(sizeof(WosNetAddrReq) == 48);
static_assert(sizeof(WosNetAddrReqV2) == 64);
static_assert(sizeof(WosNetRouteRecord) == 64);
static_assert(sizeof(WosNetLinkSetReq) == 68);

auto prefix_to_mask(uint8_t prefix) -> uint32_t {
    if (prefix == 0) {
        return 0;
    }
    if (prefix >= 32) {
        return 0xFFFFFFFFU;
    }
    return 0xFFFFFFFFU << (32 - prefix);
}

auto mask_to_prefix(uint32_t mask) -> uint8_t {
    uint8_t prefix = 0;
    while ((mask & 0x80000000U) != 0) {
        prefix++;
        mask <<= 1;
    }
    return prefix;
}

auto netctl_lifetime_deadline_ms(uint32_t lifetime_s) -> uint64_t {
    if (lifetime_s == UINT32_MAX) {
        return UINT64_MAX;
    }
    uint64_t const NOW_MS = ker::mod::time::get_ms();
    uint64_t const DELTA_MS = static_cast<uint64_t>(lifetime_s) * 1000U;
    return UINT64_MAX - NOW_MS < DELTA_MS ? UINT64_MAX : NOW_MS + DELTA_MS;
}

auto netctl_remaining_lifetime_s(uint64_t expires_at_ms, uint64_t now_ms) -> uint32_t {
    if (expires_at_ms == UINT64_MAX) {
        return UINT32_MAX;
    }
    if (expires_at_ms <= now_ms) {
        return 0;
    }
    uint64_t const REMAINING_MS = expires_at_ms - now_ms;
    uint64_t const ROUNDED_SECONDS = (REMAINING_MS + 999U) / 1000U;
    return static_cast<uint32_t>(std::min<uint64_t>(ROUNDED_SECONDS, UINT32_MAX - 1U));
}

auto netctl_bytes_all_zero(const uint8_t* bytes, size_t length) -> bool {
    return std::all_of(bytes, bytes + length, [](uint8_t byte) { return byte == 0; });
}

auto netctl_ipv6_scope(const ker::net::proto::IPv6Address& address) -> uint8_t {
    if (address.is_loopback()) {
        return 254;
    }
    return address.is_link_local_unicast() ? 253 : 0;
}

auto netctl_parse_ipv6_address_request(const WosNetAddrReq& request, ker::net::proto::IPv6Address& address) -> int {
    bool const ADDRESS_PRESENT = !netctl_bytes_all_zero(request.address, WOS_NET_ADDR_LEN);
    bool const LOCAL_PRESENT = !netctl_bytes_all_zero(request.local, WOS_NET_ADDR_LEN);
    if (request.prefix_len > 128 || request.replace > 1 || (request.flags & WOS_IPV6_ADDR_STATE_FLAGS) != 0 ||
        (request.flags & ~WOS_IPV6_ADDR_INPUT_FLAGS) != 0 ||
        (ADDRESS_PRESENT && LOCAL_PRESENT && std::memcmp(request.address, request.local, WOS_NET_ADDR_LEN) != 0)) {
        return -EINVAL;
    }

    std::memcpy(address.data(), LOCAL_PRESENT ? request.local : request.address, address.size());
    if (address.is_unspecified() || address.is_multicast() || request.scope != netctl_ipv6_scope(address)) {
        return -EINVAL;
    }
    return 0;
}

auto netctl_ipv6_route_scope(const ker::net::proto::IPv6Address& destination, uint8_t prefix_len,
                             const ker::net::proto::IPv6Address& gateway) -> uint8_t {
    if ((prefix_len == 128 && destination.is_loopback()) || gateway.is_loopback()) {
        return 254;
    }
    if (destination.is_link_local_unicast() || gateway.is_link_local_unicast()) {
        return 253;
    }
    return 0;
}

auto socket_protocol_supported(int type, int protocol) -> bool {
    switch (type & WOS_SOCK_TYPE_MASK) {
        case WOS_SOCK_STREAM:
            return protocol == 0 || protocol == WOS_IPPROTO_TCP;
        case WOS_SOCK_DGRAM:
            return protocol == 0 || protocol == WOS_IPPROTO_UDP;
        case WOS_SOCK_RAW:
            return protocol > 0 && protocol <= UINT8_MAX;
        default:
            return false;
    }
}

auto effective_ifflags(ker::net::NetDevice* dev) -> uint32_t {
    if (dev == nullptr) {
        return 0;
    }
    uint32_t flags = dev->link_flags;
    if (dev->state != 0) {
        flags |= IFF_UP | IFF_RUNNING | IFF_LOWER_UP;
    }
    return flags;
}

void apply_ifflags(ker::net::NetDevice* dev, uint32_t flags, uint32_t mask) {
    if (dev == nullptr) {
        return;
    }

    if ((mask & IFF_UP) != 0) {
        if ((flags & IFF_UP) != 0) {
            dev->state = 1;
            if (dev->ops != nullptr && dev->ops->open != nullptr) {
                dev->ops->open(dev);
            }
        } else {
            dev->state = 0;
            if (dev->ops != nullptr && dev->ops->close != nullptr) {
                dev->ops->close(dev);
            }
        }
    }

    constexpr uint32_t STORED_MASK = IFF_BROADCAST | IFF_LOOPBACK | IFF_NOARP | IFF_PROMISC | IFF_MULTICAST;
    uint32_t const STORE = mask & STORED_MASK;
    dev->link_flags &= ~STORE;
    dev->link_flags |= flags & STORE;
    ker::net::wki::wki_dev_server_notify_net_changed(dev);
    ker::net::wki::wki_remotable_notify_net_changed(dev);
}

void fill_if_info(WosNetIfInfo& out, ker::net::NetDevice* dev) {
    out = {};
    if (dev == nullptr) {
        return;
    }
    out.ifindex = dev->ifindex;
    copy_cstr_trunc(std::span<char, WOS_NET_IF_NAME_LEN>{out.name}, dev->name.data());
    out.flags = effective_ifflags(dev);
    out.mtu = dev->mtu;
    out.tx_queue_len = dev->tx_queue_len;
    out.type = ((out.flags & IFF_LOOPBACK) != 0) ? WOS_ARPHRD_LOOPBACK : WOS_ARPHRD_ETHER;
    out.addr_len = 6;
    out.operstate = dev->state != 0 ? 6 : 2;  // Linux IF_OPER_UP / IF_OPER_DOWN
    std::copy_n(dev->mac.data(), 6, out.addr);
    std::fill_n(out.broadcast, 6, 0xff);
}

auto find_dev_by_ifindex(uint32_t ifindex) -> ker::net::NetDeviceRef {
    for (size_t i = 0; i < ker::net::netdev_count(); i++) {
        auto dev_ref = ker::net::netdev_at_ref(i);
        if (dev_ref && dev_ref->ifindex == ifindex) {
            return dev_ref;
        }
    }
    return {};
}

auto find_dev_for_link_req(const WosNetLinkSetReq* req) -> ker::net::NetDeviceRef {
    if (req == nullptr) {
        return {};
    }
    if (req->ifindex != 0) {
        return find_dev_by_ifindex(req->ifindex);
    }
    return ker::net::netdev_find_by_name_ref(std::string_view(req->ifname, strnlen(req->ifname, WOS_NET_IF_NAME_LEN)));
}

auto rename_netdev(ker::net::NetDevice* dev, const char* new_name) -> int {
    size_t const LEN = strnlen(new_name, WOS_NET_IF_NAME_LEN);
    if (dev == nullptr || LEN == 0 || LEN >= WOS_NET_IF_NAME_LEN) {
        return -EINVAL;
    }
    auto existing_ref = ker::net::netdev_find_by_name_ref(std::string_view(new_name, LEN));
    if (existing_ref && existing_ref.get() != dev) {
        return -EEXIST;
    }
    dev->name.fill('\0');
    std::copy_n(new_name, LEN, dev->name.data());
    return 0;
}

auto set_netdev_hwaddr(ker::net::NetDevice* dev, const uint8_t* hwaddr, uint8_t len) -> int {
    if (dev == nullptr || hwaddr == nullptr || len != 6) {
        return -EINVAL;
    }
    if (dev->ops != nullptr && dev->ops->set_mac != nullptr) {
        dev->ops->set_mac(dev, hwaddr);
    } else {
        std::copy_n(hwaddr, 6, dev->mac.data());
    }
    ker::net::wki::wki_dev_server_notify_net_changed(dev);
    ker::net::wki::wki_remotable_notify_net_changed(dev);
    return 0;
}

void notify_netdev_l3_changed(ker::net::NetDevice* dev) {
    ker::net::wki::wki_dev_server_notify_net_changed(dev);
    ker::net::wki::wki_remotable_notify_net_changed(dev);
}
}  // namespace

uint64_t sys_net(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4, uint64_t a5) {
    auto net_op = static_cast<ker::abi::net::ops>(op);

    switch (net_op) {
        case ker::abi::net::ops::SOCKET: {
            // a1=domain, a2=type, a3=protocol
            int const DOMAIN = static_cast<int>(a1);
            int const TYPE = static_cast<int>(a2);
            int const PROTOCOL = static_cast<int>(a3);

            if (DOMAIN != WOS_AF_INET && DOMAIN != WOS_AF_INET6) {
                return static_cast<uint64_t>(-EAFNOSUPPORT);
            }
            if (!socket_protocol_supported(TYPE, PROTOCOL)) {
                return static_cast<uint64_t>(-EPROTONOSUPPORT);
            }

            auto* sock = ker::net::socket_create(DOMAIN, TYPE, PROTOCOL);
            if (sock == nullptr) {
                return static_cast<uint64_t>(-ENOMEM);
            }

            // Set owner PID so wake_socket() can find and wake this task
            auto* task = ker::mod::sched::get_current_task();
            if (task != nullptr) {
                sock->owner_pid = task->pid;
            }

            int const FD = allocate_socket_fd(sock);
            if (FD < 0) {
                ker::net::socket_destroy(sock);
                return static_cast<uint64_t>(-EMFILE);
            }

            return static_cast<uint64_t>(FD);
        }

        case ker::abi::net::ops::BIND: {
            // a1=fd, a2=addr_ptr, a3=addr_len
            auto handle = fd_to_socket(a1);
            auto* sock = handle.sock;
            if (sock == nullptr) {
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->bind == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            size_t const ADDR_LEN = static_cast<size_t>(a3);
            if (ADDR_LEN > SOCKADDR_STORAGE_MAX) {
                return static_cast<uint64_t>(-EINVAL);
            }
            std::array<uint8_t, SOCKADDR_STORAGE_MAX> address{};
            auto* task = ker::mod::sched::get_current_task();
            if (ADDR_LEN != 0 && (task == nullptr || !ker::mod::sys::usercopy::copy_from_task(*task, a2, address.data(), ADDR_LEN))) {
                return static_cast<uint64_t>(task == nullptr ? -ESRCH : -EFAULT);
            }
            const void* const ADDR = a2 != 0 ? address.data() : nullptr;
            int const RESULT = sock->proto_ops->bind(sock, ADDR, ADDR_LEN);
            return static_cast<uint64_t>(RESULT);
        }

        case ker::abi::net::ops::LISTEN: {
            // a1=fd, a2=backlog
            auto handle = fd_to_socket(a1);
            auto* sock = handle.sock;
            if (sock == nullptr) {
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->listen == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            int const RESULT = sock->proto_ops->listen(sock, static_cast<int>(a2));
            return static_cast<uint64_t>(RESULT);
        }

        case ker::abi::net::ops::ACCEPT: {
            // a1=fd, a2=addr_ptr, a3=addr_len_ptr
            auto handle = fd_to_socket(a1);
            auto* sock = handle.sock;
            if (sock == nullptr) {
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->accept == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            std::array<uint8_t, SOCKADDR_STORAGE_MAX> address{};
            size_t user_addr_capacity = 0;
            size_t kernel_addr_capacity = 0;
            bool const WANTS_ADDRESS = a2 != 0;
            if (WANTS_ADDRESS) {
                if (a3 == 0 || !ker::mod::sys::usercopy::copy_value_from_task(*task, a3, user_addr_capacity) ||
                    !ker::mod::sys::usercopy::ensure_writable(*task, a3, sizeof(user_addr_capacity))) {
                    return static_cast<uint64_t>(-EFAULT);
                }
                kernel_addr_capacity = std::min(user_addr_capacity, address.size());
                if (kernel_addr_capacity != 0 &&
                    (!ker::mod::sys::usercopy::ensure_writable(*task, a2, kernel_addr_capacity) ||
                     !ker::mod::sys::usercopy::copy_from_task(*task, a2, address.data(), kernel_addr_capacity))) {
                    return static_cast<uint64_t>(-EFAULT);
                }
            }
            ker::net::Socket* new_sock = nullptr;
            int const RESULT = run_socket_call<int>(handle.file, sock, 0, [&](int) {
                return sock->proto_ops->accept(sock, &new_sock, WANTS_ADDRESS ? address.data() : nullptr,
                                               WANTS_ADDRESS ? &kernel_addr_capacity : nullptr);
            });
            if (RESULT < 0 || new_sock == nullptr) {
                return static_cast<uint64_t>(RESULT);
            }
            if (WANTS_ADDRESS) {
                size_t const COPY_LEN = std::min({user_addr_capacity, kernel_addr_capacity, address.size()});
                if ((COPY_LEN != 0 && !ker::mod::sys::usercopy::copy_to_task(*task, a2, address.data(), COPY_LEN)) ||
                    !ker::mod::sys::usercopy::copy_value_to_task(*task, a3, kernel_addr_capacity)) {
                    ker::net::socket_destroy(new_sock);
                    return static_cast<uint64_t>(-EFAULT);
                }
            }
            // Set owner PID on accepted socket for wake_socket()
            new_sock->owner_pid = task->pid;

            int const NEW_FD = allocate_socket_fd(new_sock);
            if (NEW_FD < 0) {
                ker::net::socket_destroy(new_sock);
                return static_cast<uint64_t>(-EMFILE);
            }
            return static_cast<uint64_t>(NEW_FD);
        }

        case ker::abi::net::ops::CONNECT: {
            // a1=fd, a2=addr_ptr, a3=addr_len
            auto handle = fd_to_socket(a1);
            auto* sock = handle.sock;
            if (sock == nullptr) {
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->connect == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            size_t const ADDR_LEN = static_cast<size_t>(a3);
            if (ADDR_LEN > SOCKADDR_STORAGE_MAX) {
                return static_cast<uint64_t>(-EINVAL);
            }
            std::array<uint8_t, SOCKADDR_STORAGE_MAX> address{};
            auto* task = ker::mod::sched::get_current_task();
            if (ADDR_LEN != 0 && (task == nullptr || !ker::mod::sys::usercopy::copy_from_task(*task, a2, address.data(), ADDR_LEN))) {
                return static_cast<uint64_t>(task == nullptr ? -ESRCH : -EFAULT);
            }
            const void* const ADDR = a2 != 0 ? address.data() : nullptr;
            int const RESULT = run_socket_call<int>(handle.file, sock, 0,
                                                    [&](int flags) { return sock->proto_ops->connect(sock, ADDR, ADDR_LEN, flags); });
            return static_cast<uint64_t>(RESULT);
        }

        case ker::abi::net::ops::SEND: {
            // a1=fd, a2=buf, a3=len, a4=flags
            auto handle = fd_to_socket(a1);
            auto* sock = handle.sock;
            if (sock == nullptr) {
                auto file_handle = fd_to_file(a1);
                if (file_handle.file != nullptr && ker::net::wki::wki_ipc_is_socket_proxy_file(file_handle.file)) {
                    if (static_cast<int>(a4) != 0) {
                        return static_cast<uint64_t>(-EOPNOTSUPP);
                    }
                    if (file_handle.file->fops == nullptr || file_handle.file->fops->vfs_write == nullptr) {
                        return static_cast<uint64_t>(-ENOSYS);
                    }
                    auto result = socket_send_user_bounced(
                        file_handle.file, nullptr, a2, static_cast<size_t>(a3), 0,
                        [&](const void* buf, size_t len, int) { return file_handle.file->fops->vfs_write(file_handle.file, buf, len, 0); });
                    return static_cast<uint64_t>(result);
                }
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->send == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            ssize_t const RESULT = socket_send_user_bounced(
                handle.file, sock, a2, static_cast<size_t>(a3), static_cast<int>(a4),
                [&](const void* buf, size_t len, int flags) { return sock->proto_ops->send(sock, buf, len, flags); });
            return static_cast<uint64_t>(RESULT);
        }

        case ker::abi::net::ops::RECV: {
            // a1=fd, a2=buf, a3=len, a4=flags
            auto handle = fd_to_socket(a1);
            auto* sock = handle.sock;
            if (sock == nullptr) {
                auto file_handle = fd_to_file(a1);
                if (file_handle.file != nullptr && ker::net::wki::wki_ipc_is_socket_proxy_file(file_handle.file)) {
                    if (static_cast<int>(a4) != 0) {
                        return static_cast<uint64_t>(-EOPNOTSUPP);
                    }
                    if (file_handle.file->fops == nullptr || file_handle.file->fops->vfs_read == nullptr) {
                        return static_cast<uint64_t>(-ENOSYS);
                    }
                    auto result = socket_recv_user_bounced(
                        file_handle.file, nullptr, a2, static_cast<size_t>(a3), 0,
                        [&](void* buf, size_t len, int) { return file_handle.file->fops->vfs_read(file_handle.file, buf, len, 0); });
                    return static_cast<uint64_t>(result);
                }
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->recv == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            ssize_t const RESULT =
                socket_recv_user_bounced(handle.file, sock, a2, static_cast<size_t>(a3), static_cast<int>(a4),
                                         [&](void* buf, size_t len, int flags) { return sock->proto_ops->recv(sock, buf, len, flags); });
            return static_cast<uint64_t>(RESULT);
        }

        case ker::abi::net::ops::CLOSE: {
            // a1=fd - use VFS close which calls socket_fops_close
            int const RESULT = ker::vfs::vfs_close(static_cast<int>(a1));
            return static_cast<uint64_t>(RESULT);
        }

        case ker::abi::net::ops::SENDTO: {
            // a1=fd, a2=buf, a3=len, a4=flags, a5=addr_ptr
            auto handle = fd_to_socket(a1);
            auto* sock = handle.sock;
            if (sock == nullptr) {
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->sendto == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            std::array<uint8_t, SOCKADDR_STORAGE_MAX> addr_storage{};
            size_t const ALEN = addr_len_for_domain(sock->domain);
            const void* addr_ptr = nullptr;
            if (a5 != 0) {
                auto* task = ker::mod::sched::get_current_task();
                if (task == nullptr) {
                    return static_cast<uint64_t>(-ESRCH);
                }
                if (ALEN > addr_storage.size() || !ker::mod::sys::usercopy::copy_from_task(*task, a5, addr_storage.data(), ALEN)) {
                    return static_cast<uint64_t>(-EFAULT);
                }
                addr_ptr = addr_storage.data();
            }

            ssize_t const RESULT = socket_send_user_bounced(
                handle.file, sock, a2, static_cast<size_t>(a3), static_cast<int>(a4),
                [&](const void* buf, size_t len, int flags) { return sock->proto_ops->sendto(sock, buf, len, flags, addr_ptr, ALEN); });
            return static_cast<uint64_t>(RESULT);
        }

        case ker::abi::net::ops::RECVFROM: {
            // a1=fd, a2=buf, a3=len, a4=flags, a5=addr_out_ptr
            auto handle = fd_to_socket(a1);
            auto* sock = handle.sock;
            if (sock == nullptr) {
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->recvfrom == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            std::array<uint8_t, SOCKADDR_STORAGE_MAX> addr_storage{};
            size_t alen = addr_len_for_domain(sock->domain);
            void* addr_ptr = a5 != 0 ? addr_storage.data() : nullptr;
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            if (a5 != 0 && !ker::mod::sys::usercopy::ensure_writable(*task, a5, alen)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            ssize_t const RESULT = socket_recv_user_bounced(
                handle.file, sock, a2, static_cast<size_t>(a3), static_cast<int>(a4),
                [&](void* buf, size_t len, int flags) { return sock->proto_ops->recvfrom(sock, buf, len, flags, addr_ptr, &alen); });
            if (RESULT >= 0 && a5 != 0) {
                if (alen > addr_storage.size()) {
                    return static_cast<uint64_t>(-EOVERFLOW);
                }
                if (!ker::mod::sys::usercopy::copy_to_task(*task, a5, addr_storage.data(), alen)) {
                    return static_cast<uint64_t>(-EFAULT);
                }
            }
            return static_cast<uint64_t>(RESULT);
        }

        case ker::abi::net::ops::SENDTO_EX: {
            // a1=fd, a2=buf, a3=len, a4=flags, a5=SockaddrIoV1*
            auto handle = fd_to_socket(a1);
            auto* sock = handle.sock;
            if (sock == nullptr) {
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->sendto == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }

            ker::abi::net::SockaddrIoV1 io{};
            int const IO_RESULT = copy_sockaddr_io_v1_from_user(*task, a5, io);
            if (IO_RESULT < 0) {
                return static_cast<uint64_t>(IO_RESULT);
            }

            std::array<uint8_t, SOCKADDR_STORAGE_MAX> address{};
            const void* address_ptr = nullptr;
            size_t const ADDRESS_LEN = static_cast<size_t>(io.address_length);
            if (ADDRESS_LEN != 0) {
                if (!ker::mod::sys::usercopy::copy_from_task(*task, io.address, address.data(), ADDRESS_LEN)) {
                    return static_cast<uint64_t>(-EFAULT);
                }
                address_ptr = address.data();
            }

            ssize_t const RESULT = socket_send_user_bounced(
                handle.file, sock, a2, static_cast<size_t>(a3), static_cast<int>(a4), [&](const void* buf, size_t len, int flags) {
                    return sock->proto_ops->sendto(sock, buf, len, flags, address_ptr, ADDRESS_LEN);
                });
            return static_cast<uint64_t>(RESULT);
        }

        case ker::abi::net::ops::RECVFROM_EX: {
            // a1=fd, a2=buf, a3=len, a4=flags, a5=SockaddrIoV1*
            auto handle = fd_to_socket(a1);
            auto* sock = handle.sock;
            if (sock == nullptr) {
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->recvfrom == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }

            ker::abi::net::SockaddrIoV1 io{};
            int const IO_RESULT = copy_sockaddr_io_v1_from_user(*task, a5, io);
            if (IO_RESULT < 0) {
                return static_cast<uint64_t>(IO_RESULT);
            }
            if (io.address != 0 && io.result_length == 0) {
                return static_cast<uint64_t>(-EINVAL);
            }

            size_t const ADDRESS_CAPACITY = static_cast<size_t>(io.address_length);
            if ((ADDRESS_CAPACITY != 0 && !ker::mod::sys::usercopy::ensure_writable(*task, io.address, ADDRESS_CAPACITY)) ||
                (io.result_length != 0 && !ker::mod::sys::usercopy::ensure_writable(*task, io.result_length, sizeof(uint64_t)))) {
                return static_cast<uint64_t>(-EFAULT);
            }

            std::array<uint8_t, SOCKADDR_STORAGE_MAX> address{};
            size_t address_len = ADDRESS_CAPACITY;
            void* address_ptr = io.address != 0 ? address.data() : nullptr;
            ssize_t const RESULT = socket_recv_user_bounced(
                handle.file, sock, a2, static_cast<size_t>(a3), static_cast<int>(a4), [&](void* buf, size_t len, int flags) {
                    return sock->proto_ops->recvfrom(sock, buf, len, flags, address_ptr, &address_len);
                });
            if (RESULT < 0) {
                return static_cast<uint64_t>(RESULT);
            }

            size_t const COPY_LEN = std::min(ADDRESS_CAPACITY, address_len);
            uint64_t const FULL_LEN = address_len;
            if ((COPY_LEN != 0 && !ker::mod::sys::usercopy::copy_to_task(*task, io.address, address.data(), COPY_LEN)) ||
                (io.result_length != 0 && !ker::mod::sys::usercopy::copy_value_to_task(*task, io.result_length, FULL_LEN))) {
                return static_cast<uint64_t>(-EFAULT);
            }
            return static_cast<uint64_t>(RESULT);
        }

        case ker::abi::net::ops::SETSOCKOPT: {
            // a1=fd, a2=level, a3=optname, a4=optval_ptr, a5=optlen
            auto handle = fd_to_socket(a1);
            auto* sock = handle.sock;
            FileHandle proxy_file;
            if (sock == nullptr) {
                proxy_file = fd_to_file(a1);
                if (proxy_file.file == nullptr || !ker::net::wki::wki_ipc_is_socket_proxy_file(proxy_file.file)) {
                    return static_cast<uint64_t>(-EBADF);
                }
            } else if (sock->proto_ops == nullptr || sock->proto_ops->setsockopt == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }

            size_t const OPTLEN = static_cast<size_t>(a5);
            if (OPTLEN > SOCKET_OPTION_MAX) {
                return static_cast<uint64_t>(-EINVAL);
            }
            if (OPTLEN != 0 && a4 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            uint8_t stack_option[SOCKET_IO_BOUNCE_STACK_CHUNK];
            std::unique_ptr<uint8_t[]> heap_option{};
            uint8_t* const OPTION =
                OPTLEN != 0 ? socket_bounce_heap_or_stack(heap_option, stack_option, sizeof(stack_option), OPTLEN) : nullptr;
            if ((OPTLEN != 0 && OPTION == nullptr) ||
                (OPTLEN != 0 && !ker::mod::sys::usercopy::copy_from_task(*task, a4, OPTION, OPTLEN))) {
                return static_cast<uint64_t>(OPTION == nullptr ? -ENOMEM : -EFAULT);
            }

            int const RESULT =
                sock != nullptr
                    ? sock->proto_ops->setsockopt(sock, static_cast<int>(a2), static_cast<int>(a3), OPTION, OPTLEN)
                    : ker::net::wki::wki_ipc_socket_setsockopt(proxy_file.file, static_cast<int>(a2), static_cast<int>(a3), OPTION, OPTLEN);
            return static_cast<uint64_t>(RESULT);
        }

        case ker::abi::net::ops::GETSOCKOPT: {
            // a1=fd, a2=level, a3=optname, a4=optval_ptr, a5=optlen_ptr
            auto handle = fd_to_socket(a1);
            auto* sock = handle.sock;
            FileHandle proxy_file;
            if (sock == nullptr) {
                proxy_file = fd_to_file(a1);
                if (proxy_file.file == nullptr || !ker::net::wki::wki_ipc_is_socket_proxy_file(proxy_file.file)) {
                    return static_cast<uint64_t>(-EBADF);
                }
            } else if (sock->proto_ops == nullptr || sock->proto_ops->getsockopt == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }

            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            size_t option_len = 0;
            if (!ker::mod::sys::usercopy::copy_value_from_task(*task, a5, option_len) || option_len > SOCKET_OPTION_MAX ||
                (option_len != 0 && a4 == 0) || !ker::mod::sys::usercopy::ensure_writable(*task, a5, sizeof(option_len)) ||
                (option_len != 0 && !ker::mod::sys::usercopy::ensure_writable(*task, a4, option_len))) {
                return static_cast<uint64_t>(option_len > SOCKET_OPTION_MAX ? -EINVAL : -EFAULT);
            }
            size_t const OPTION_CAPACITY = option_len;

            uint8_t stack_option[SOCKET_IO_BOUNCE_STACK_CHUNK];
            std::unique_ptr<uint8_t[]> heap_option{};
            uint8_t* const OPTION =
                option_len != 0 ? socket_bounce_heap_or_stack(heap_option, stack_option, sizeof(stack_option), option_len) : nullptr;
            if ((option_len != 0 && OPTION == nullptr) ||
                (option_len != 0 && !ker::mod::sys::usercopy::copy_from_task(*task, a4, OPTION, option_len))) {
                return static_cast<uint64_t>(OPTION == nullptr ? -ENOMEM : -EFAULT);
            }

            int const RESULT = sock != nullptr
                                   ? sock->proto_ops->getsockopt(sock, static_cast<int>(a2), static_cast<int>(a3), OPTION, &option_len)
                                   : ker::net::wki::wki_ipc_socket_getsockopt(proxy_file.file, static_cast<int>(a2), static_cast<int>(a3),
                                                                              OPTION, &option_len);
            if (RESULT < 0) {
                return static_cast<uint64_t>(RESULT);
            }
            if (option_len > OPTION_CAPACITY) {
                return static_cast<uint64_t>(-EOVERFLOW);
            }
            if ((option_len != 0 && !ker::mod::sys::usercopy::copy_to_task(*task, a4, OPTION, option_len)) ||
                !ker::mod::sys::usercopy::copy_value_to_task(*task, a5, option_len)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            return static_cast<uint64_t>(RESULT);
        }

        case ker::abi::net::ops::SHUTDOWN: {
            // a1=fd, a2=how
            auto handle = fd_to_socket(a1);
            auto* sock = handle.sock;
            if (sock == nullptr) {
                auto file_handle = fd_to_file(a1);
                if (file_handle.file != nullptr && ker::net::wki::wki_ipc_is_socket_proxy_file(file_handle.file)) {
                    int const RESULT = ker::net::wki::wki_ipc_socket_shutdown(file_handle.file, static_cast<int>(a2));
                    return static_cast<uint64_t>(RESULT);
                }
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->shutdown == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            int const RESULT = sock->proto_ops->shutdown(sock, static_cast<int>(a2));
            return static_cast<uint64_t>(RESULT);
        }

        case ker::abi::net::ops::GETPEERNAME: {
            // a1=fd, a2=addr_out, a3=addr_len_ptr
            auto handle = fd_to_socket(a1);
            auto* sock = handle.sock;
            if (sock == nullptr) {
                auto file_handle = fd_to_file(a1);
                if (file_handle.file != nullptr && ker::net::wki::wki_ipc_is_socket_proxy_file(file_handle.file)) {
                    int const RESULT = socket_name_to_user(a2, a3, ker::net::SOCKADDR_V4_LEN, [&](void* addr, size_t* len) {
                        return ker::net::wki::wki_ipc_socket_getpeername(file_handle.file, addr, len);
                    });
                    return static_cast<uint64_t>(RESULT);
                }
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->remote.port == 0 && sock->remote.is_unspecified()) {
                return static_cast<uint64_t>(-ENOTCONN);
            }
            size_t const DEFAULT_CAPACITY = sock->domain == WOS_AF_INET6 ? ker::net::SOCKADDR_V6_LEN : ker::net::SOCKADDR_V4_LEN;
            int const RESULT = socket_name_to_user(a2, a3, DEFAULT_CAPACITY, [&](void* addr, size_t* len) {
                size_t const CAPACITY = *len;
                return ker::net::socket_fill_sockaddr(sock->remote, addr, CAPACITY, len);
            });
            return static_cast<uint64_t>(RESULT);
        }

        case ker::abi::net::ops::GETSOCKNAME: {
            // a1=fd, a2=addr_out, a3=addr_len_ptr
            auto handle = fd_to_socket(a1);
            auto* sock = handle.sock;
            if (sock == nullptr) {
                return static_cast<uint64_t>(-EBADF);
            }
            size_t const DEFAULT_CAPACITY = sock->domain == WOS_AF_INET6 ? ker::net::SOCKADDR_V6_LEN : ker::net::SOCKADDR_V4_LEN;
            int const RESULT = socket_name_to_user(a2, a3, DEFAULT_CAPACITY, [&](void* addr, size_t* len) {
                size_t const CAPACITY = *len;
                return ker::net::socket_fill_sockaddr(sock->local, addr, CAPACITY, len);
            });
            return static_cast<uint64_t>(RESULT);
        }

        case ker::abi::net::ops::IOCTL_NET: {
            // a1=request, a2=arg_ptr
            auto request = static_cast<uint32_t>(a1);

            // ifreq layout: name[16] + union[16+]
            // sockaddr_in within ifreq: offset 16=sa_family(2), 18=sin_port(2), 20=sin_addr(4)
            constexpr uint32_t SIOC_GIFFLAGS = 0x8913;
            constexpr uint32_t SIOC_SIFFLAGS = 0x8914;
            constexpr uint32_t SIOC_GIFADDR = 0x8915;
            constexpr uint32_t SIOC_SIFADDR = 0x8916;
            constexpr uint32_t SIOC_GIFNETMASK = 0x891B;
            constexpr uint32_t SIOC_SIFNETMASK = 0x891C;
            constexpr uint32_t SIOC_GIFMTU = 0x8921;
            constexpr uint32_t SIOC_SIFMTU = 0x8922;
            constexpr uint32_t SIOC_SIFNAME = 0x8923;
            constexpr uint32_t SIOC_SIFHWADDR = 0x8924;
            constexpr uint32_t SIOC_SIFHWBROADCAST = 0x8937;
            constexpr uint32_t SIOC_GIFHWADDR = 0x8927;
            constexpr uint32_t SIOC_GIFINDEX = 0x8933;
            constexpr uint32_t SIOC_GIFTXQLEN = 0x8942;
            constexpr uint32_t SIOC_SIFTXQLEN = 0x8943;
            constexpr uint32_t SIOC_ADDRT = 0x890B;
            constexpr uint32_t SIOC_DELRT = 0x890C;

            constexpr size_t IFREQ_BYTES = 40;
            constexpr size_t ROUTE_INPUT_BYTES = 48;
            std::array<uint8_t, ROUTE_INPUT_BYTES> arg_storage{};
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            size_t const ARG_BYTES = (request == SIOC_ADDRT || request == SIOC_DELRT) ? ROUTE_INPUT_BYTES : IFREQ_BYTES;
            if (!ker::mod::sys::usercopy::copy_from_task(*task, a2, arg_storage.data(), ARG_BYTES)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            bool const COPIES_OUT = request == SIOC_GIFFLAGS || request == SIOC_GIFADDR || request == SIOC_GIFNETMASK ||
                                    request == SIOC_GIFHWADDR || request == SIOC_GIFMTU || request == SIOC_GIFINDEX ||
                                    request == SIOC_GIFTXQLEN;
            if (COPIES_OUT && !ker::mod::sys::usercopy::ensure_writable(*task, a2, IFREQ_BYTES)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            auto* arg = arg_storage.data();
            auto copy_ifreq_out = [&]() -> uint64_t {
                return ker::mod::sys::usercopy::copy_to_task(*task, a2, arg, IFREQ_BYTES) ? 0 : static_cast<uint64_t>(-EFAULT);
            };

            if (request == SIOC_ADDRT || request == SIOC_DELRT) {
                // rtentry layout (x86_64):
                // offset 8: rt_dst sockaddr (sin_addr at offset 12)
                // offset 24: rt_gateway sockaddr (sin_addr at offset 28)
                // offset 40: rt_genmask sockaddr (sin_addr at offset 44)
                // offset 56: rt_flags (uint16_t)
                auto* rt = arg;
                auto dst = load_unaligned<uint32_t>(rt + 12);
                auto gw = load_unaligned<uint32_t>(rt + 28);
                auto mask = load_unaligned<uint32_t>(rt + 44);
                // Convert from network byte order
                dst = ker::net::ntohl(dst);
                gw = ker::net::ntohl(gw);
                mask = ker::net::ntohl(mask);

                if (request == SIOC_ADDRT) {
                    // Find device for the route.
                    // For gateway routes, prefer a non-loopback UP device that
                    // has an IP on the same subnet as the gateway.
                    // For direct routes (gw == 0), prefer a non-loopback UP
                    // device whose configured IPv4 subnet matches the route's
                    // destination/mask. Falling back to an arbitrary UP device
                    // can bind the route to a proxy NIC and hijack local
                    // traffic on overlapping subnets.
                    ker::net::NetDeviceRef rdev_ref{};
                    if (gw != 0) {
                        for (size_t i = 0; i < ker::net::netdev_count(); i++) {
                            auto candidate = ker::net::netdev_at_ref(i);
                            auto* d = candidate.get();
                            if (d == nullptr || d->state != 1 || std::strcmp(d->name.data(), "lo") == 0) {
                                continue;
                            }
                            auto* nif = ker::net::netif_find_by_dev(d);
                            if (nif != nullptr && nif->ipv4_addr_count > 0) {
                                uint32_t const DEV_IP = nif->ipv4_addrs[0].addr;
                                uint32_t const DEV_MASK = nif->ipv4_addrs[0].netmask;
                                if ((DEV_IP & DEV_MASK) == (gw & DEV_MASK)) {
                                    rdev_ref = std::move(candidate);
                                    break;
                                }
                            }
                        }
                    } else {
                        for (size_t i = 0; i < ker::net::netdev_count(); i++) {
                            auto candidate = ker::net::netdev_at_ref(i);
                            auto* d = candidate.get();
                            if (d == nullptr || d->state != 1 || std::strcmp(d->name.data(), "lo") == 0) {
                                continue;
                            }
                            auto* nif = ker::net::netif_find_by_dev(d);
                            if (nif == nullptr || nif->ipv4_addr_count == 0) {
                                continue;
                            }
                            for (size_t j = 0; j < nif->ipv4_addr_count; j++) {
                                uint32_t const DEV_IP = nif->ipv4_addrs[j].addr;
                                if ((DEV_IP & mask) == (dst & mask)) {
                                    rdev_ref = std::move(candidate);
                                    break;
                                }
                            }
                            if (rdev_ref) {
                                break;
                            }
                        }
                    }
                    // Gateway routes can still fall back to the first
                    // non-loopback UP device if there is only one usable path.
                    if (!rdev_ref) {
                        if (gw != 0) {
                            for (size_t i = 0; i < ker::net::netdev_count(); i++) {
                                auto candidate = ker::net::netdev_at_ref(i);
                                auto* d = candidate.get();
                                if (d != nullptr && d->state == 1 && std::strcmp(d->name.data(), "lo") != 0) {
                                    rdev_ref = std::move(candidate);
                                    break;
                                }
                            }
                        }
                    }
                    if (!rdev_ref) {
                        return static_cast<uint64_t>(-ENODEV);
                    }
                    int const RET = ker::net::route_add(dst, mask, gw, 0, rdev_ref.get());
                    return static_cast<uint64_t>(RET);
                }
                int const RET = ker::net::route_del(dst, mask);
                return static_cast<uint64_t>(RET);
            }

            // All other SIOC* ioctls use ifreq: name at offset 0, data at offset 16
            std::string_view const IFNAME(reinterpret_cast<char*>(arg), strnlen(reinterpret_cast<char*>(arg), 16));

            auto dev_ref = ker::net::netdev_find_by_name_ref(IFNAME);
            if (!dev_ref) {
                return static_cast<uint64_t>(-ENODEV);
            }
            auto* dev = dev_ref.get();

            switch (request) {
                case SIOC_GIFFLAGS: {
                    store_unaligned<int16_t>(arg + 16, static_cast<int16_t>(effective_ifflags(dev)));
                    return copy_ifreq_out();
                }
                case SIOC_SIFFLAGS: {
                    auto flags = static_cast<uint32_t>(load_unaligned<uint16_t>(arg + 16));
                    apply_ifflags(dev, flags, IFF_UP | IFF_NOARP | IFF_PROMISC | IFF_MULTICAST);
                    return 0;
                }
                case SIOC_GIFADDR: {
                    auto* nif = ker::net::netif_find_by_dev(dev);
                    if (nif == nullptr || nif->ipv4_addr_count == 0) {
                        return static_cast<uint64_t>(-EADDRNOTAVAIL);
                    }
                    // Fill sockaddr_in at offset 16
                    std::memset(arg + 16, 0, 16);
                    store_unaligned<uint16_t>(arg + 16, 2);  // AF_INET
                    store_unaligned<uint32_t>(arg + 20, ker::net::htonl(nif->ipv4_addrs[0].addr));
                    return copy_ifreq_out();
                }
                case SIOC_SIFADDR: {
                    uint32_t const ADDR = ker::net::ntohl(load_unaligned<uint32_t>(arg + 20));
                    // Check if interface already has addresses; if so, update first one
                    auto* nif = ker::net::netif_get(dev);
                    if (nif == nullptr) {
                        return static_cast<uint64_t>(-ENOMEM);
                    }
                    if (nif->ipv4_addr_count > 0) {
                        nif->ipv4_addrs[0].addr = ADDR;
                        notify_netdev_l3_changed(dev);
                    } else {
                        return static_cast<uint64_t>(ker::net::netif_add_ipv4(dev, ADDR, 0xFFFFFF00));  // default /24
                    }
                    return 0;
                }
                case SIOC_GIFNETMASK: {
                    auto* nif = ker::net::netif_find_by_dev(dev);
                    if (nif == nullptr || nif->ipv4_addr_count == 0) {
                        return static_cast<uint64_t>(-EADDRNOTAVAIL);
                    }
                    std::memset(arg + 16, 0, 16);
                    store_unaligned<uint16_t>(arg + 16, 2);  // AF_INET
                    store_unaligned<uint32_t>(arg + 20, ker::net::htonl(nif->ipv4_addrs[0].netmask));
                    return copy_ifreq_out();
                }
                case SIOC_SIFNETMASK: {
                    uint32_t const MASK = ker::net::ntohl(load_unaligned<uint32_t>(arg + 20));
                    auto* nif = ker::net::netif_get(dev);
                    if (nif == nullptr) {
                        return static_cast<uint64_t>(-ENOMEM);
                    }
                    if (nif->ipv4_addr_count == 0) {
                        return static_cast<uint64_t>(-EADDRNOTAVAIL);
                    }
                    nif->ipv4_addrs[0].netmask = MASK;
                    notify_netdev_l3_changed(dev);
                    return 0;
                }
                case SIOC_GIFHWADDR: {
                    // sa_family = ARPHRD_ETHER (1), then 6 bytes of MAC
                    std::memset(arg + 16, 0, 16);
                    store_unaligned<uint16_t>(arg + 16, (dev->link_flags & IFF_LOOPBACK) != 0 ? WOS_ARPHRD_LOOPBACK : WOS_ARPHRD_ETHER);
                    std::memcpy(arg + 18, dev->mac.data(), 6);
                    return copy_ifreq_out();
                }
                case SIOC_GIFMTU: {
                    store_unaligned<int32_t>(arg + 16, static_cast<int32_t>(dev->mtu));
                    return copy_ifreq_out();
                }
                case SIOC_SIFMTU: {
                    auto const MTU = load_unaligned<int32_t>(arg + 16);
                    if (MTU <= 0) {
                        return static_cast<uint64_t>(-EINVAL);
                    }
                    dev->mtu = static_cast<uint32_t>(MTU);
                    return 0;
                }
                case SIOC_GIFTXQLEN: {
                    store_unaligned<int32_t>(arg + 16, static_cast<int32_t>(dev->tx_queue_len));
                    return copy_ifreq_out();
                }
                case SIOC_SIFTXQLEN: {
                    auto const QLEN = load_unaligned<int32_t>(arg + 16);
                    if (QLEN < 0) {
                        return static_cast<uint64_t>(-EINVAL);
                    }
                    dev->tx_queue_len = static_cast<uint32_t>(QLEN);
                    return 0;
                }
                case SIOC_SIFHWADDR: {
                    auto* sa = arg + 16;
                    return static_cast<uint64_t>(set_netdev_hwaddr(dev, sa + 2, 6));
                }
                case SIOC_SIFHWBROADCAST: {
                    return 0;
                }
                case SIOC_SIFNAME: {
                    return static_cast<uint64_t>(rename_netdev(dev, reinterpret_cast<const char*>(arg + 16)));
                }
                case SIOC_GIFINDEX: {
                    store_unaligned<int32_t>(arg + 16, static_cast<int32_t>(dev->ifindex));
                    return copy_ifreq_out();
                }
                default:
                    return static_cast<uint64_t>(-ENOSYS);
            }
        }

        case ker::abi::net::ops::SET_DEV_CPU_AFFINITY: {
            // a1 = ptr to { char ifname[16]; uint64_t cpu_mask }
            // cpu_mask: each set bit (from LSB) is the CPU for the next queue pair:
            //   pair 0 ← CPU of lowest set bit, pair 1 ← CPU of next set bit, ...
            std::array<uint8_t, 24> request{};
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            if (!ker::mod::sys::usercopy::copy_from_task(*task, a1, request.data(), request.size())) {
                return static_cast<uint64_t>(-EFAULT);
            }
            std::string_view const IFNAME(reinterpret_cast<char*>(request.data()), strnlen(reinterpret_cast<char*>(request.data()), 16));
            auto const CPU_MASK = load_unaligned<uint64_t>(request.data() + 16);
            auto dev_ref = ker::net::netdev_find_by_name_ref(IFNAME);
            if (!dev_ref) {
                return static_cast<uint64_t>(-ENODEV);
            }
            auto* dev = dev_ref.get();
            if (dev->ops == nullptr || dev->ops->set_queue_cpu == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            uint32_t pair_idx = 0;
            for (uint64_t tmp = CPU_MASK; tmp != 0 && pair_idx < 8; tmp &= tmp - 1, ++pair_idx) {
                auto const CPU = static_cast<uint64_t>(__builtin_ctzll(tmp));
                int const RET = dev->ops->set_queue_cpu(dev, pair_idx, CPU);
                if (RET != 0) {
                    return static_cast<uint64_t>(RET);
                }
            }
            return 0;
        }

        case ker::abi::net::ops::NETCTL_IF_LIST: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            size_t capacity = 0;
            if (!ker::mod::sys::usercopy::copy_value_from_task(*task, a2, capacity) ||
                !ker::mod::sys::usercopy::ensure_writable(*task, a2, sizeof(capacity))) {
                return static_cast<uint64_t>(-EFAULT);
            }
            size_t const TOTAL = ker::net::netdev_count();
            size_t const EMIT = a1 != 0 ? std::min(capacity, TOTAL) : 0;
            size_t const OUTPUT_BYTES = EMIT * sizeof(WosNetIfInfo);
            if (OUTPUT_BYTES != 0 && !ker::mod::sys::usercopy::ensure_writable(*task, a1, OUTPUT_BYTES)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            std::array<WosNetIfInfo, ker::net::MAX_NET_DEVICES> output{};
            for (size_t i = 0; i < EMIT; i++) {
                auto dev_ref = ker::net::netdev_at_ref(i);
                fill_if_info(output.at(i), dev_ref.get());
            }
            if ((OUTPUT_BYTES != 0 && !ker::mod::sys::usercopy::copy_to_task(*task, a1, output.data(), OUTPUT_BYTES)) ||
                !ker::mod::sys::usercopy::copy_value_to_task(*task, a2, TOTAL)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            return 0;
        }

        case ker::abi::net::ops::NETCTL_ADDR_LIST: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            size_t capacity = 0;
            if (!ker::mod::sys::usercopy::copy_value_from_task(*task, a2, capacity) ||
                !ker::mod::sys::usercopy::ensure_writable(*task, a2, sizeof(capacity))) {
                return static_cast<uint64_t>(-EFAULT);
            }

            std::array<WosNetAddrInfo, ker::net::MAX_NET_DEVICES * ker::net::MAX_ADDRS_PER_IF * 2> output{};
            size_t total = 0;
            for (size_t i = 0; i < ker::net::netdev_count(); i++) {
                auto dev_ref = ker::net::netdev_at_ref(i);
                auto* dev = dev_ref.get();
                auto* nif = ker::net::netif_find_by_dev(dev);
                if (dev == nullptr || nif == nullptr) {
                    continue;
                }
                for (size_t j = 0; j < nif->ipv4_addr_count; j++) {
                    auto& info = output.at(total);
                    info.ifindex = dev->ifindex;
                    info.family = WOS_AF_INET;
                    info.prefix_len = mask_to_prefix(nif->ipv4_addrs[j].netmask);
                    info.scope = (nif->ipv4_addrs[j].addr >> 24) == 127 ? 254 : 0;  // RT_SCOPE_HOST/global
                    info.flags = WOS_IFA_F_PERMANENT;
                    copy_cstr_trunc(std::span<char, WOS_NET_IF_NAME_LEN>{info.label}, dev->name.data());
                    uint32_t addr_be = ker::net::htonl(nif->ipv4_addrs[j].addr);
                    uint32_t brd_be = ker::net::htonl(nif->ipv4_addrs[j].addr | ~nif->ipv4_addrs[j].netmask);
                    std::memcpy(info.address, &addr_be, sizeof(addr_be));
                    std::memcpy(info.local, &addr_be, sizeof(addr_be));
                    std::memcpy(info.broadcast, &brd_be, sizeof(brd_be));
                    total++;
                }

                std::array<ker::net::IPv6Addr, ker::net::MAX_ADDRS_PER_IF> ipv6{};
                size_t const IPV6_COUNT = ker::net::netif_ipv6_snapshot(dev, ipv6.data(), ipv6.size());
                for (size_t j = 0; j < std::min(IPV6_COUNT, ipv6.size()); ++j) {
                    auto& info = output.at(total);
                    auto const& address = ipv6.at(j);
                    info.ifindex = dev->ifindex;
                    info.family = WOS_AF_INET6;
                    info.prefix_len = address.prefix_len;
                    info.scope = address.addr.is_loopback() ? 254 : (address.addr.is_link_local() ? 253 : 0);
                    info.flags = address.flags;
                    copy_cstr_trunc(std::span<char, WOS_NET_IF_NAME_LEN>{info.label}, dev->name.data());
                    std::memcpy(info.address, address.addr.data(), address.addr.size());
                    std::memcpy(info.local, address.addr.data(), address.addr.size());
                    total++;
                }
            }

            size_t const EMIT = a1 != 0 ? std::min(capacity, total) : 0;
            size_t const OUTPUT_BYTES = EMIT * sizeof(WosNetAddrInfo);
            if ((OUTPUT_BYTES != 0 && (!ker::mod::sys::usercopy::ensure_writable(*task, a1, OUTPUT_BYTES) ||
                                       !ker::mod::sys::usercopy::copy_to_task(*task, a1, output.data(), OUTPUT_BYTES))) ||
                !ker::mod::sys::usercopy::copy_value_to_task(*task, a2, total)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            return 0;
        }

        case ker::abi::net::ops::NETCTL_ADDR_SET: {
            auto* task = ker::mod::sched::get_current_task();
            WosNetAddrReq req{};
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            if (!ker::mod::sys::usercopy::copy_value_from_task(*task, a1, req)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            auto dev_ref = find_dev_by_ifindex(req.ifindex);
            if (!dev_ref) {
                return static_cast<uint64_t>(-ENODEV);
            }
            auto* dev = dev_ref.get();
            if (req.family == WOS_AF_INET6) {
                ker::net::proto::IPv6Address address{};
                int const PARSE_RESULT = netctl_parse_ipv6_address_request(req, address);
                if (PARSE_RESULT < 0) {
                    return static_cast<uint64_t>(PARSE_RESULT);
                }
                uint32_t flags = req.flags;
                if (flags == 0) {
                    flags = WOS_IFA_F_PERMANENT | WOS_IFA_F_NODAD;
                }
                int const RET = ker::net::netif_set_ipv6(dev, address, req.prefix_len, flags, UINT64_MAX, UINT64_MAX, req.replace != 0);
                return static_cast<uint64_t>(RET);
            }
            if (req.family != WOS_AF_INET || req.prefix_len > 32) {
                return static_cast<uint64_t>(-EINVAL);
            }
            uint32_t addr_be = 0;
            std::memcpy(&addr_be, req.local, sizeof(addr_be));
            if (addr_be == 0) {
                std::memcpy(&addr_be, req.address, sizeof(addr_be));
            }
            uint32_t const ADDR = ker::net::ntohl(addr_be);
            uint32_t const MASK = prefix_to_mask(req.prefix_len);
            int const RET = ker::net::netif_set_ipv4(dev, ADDR, MASK, req.replace != 0);
            return static_cast<uint64_t>(RET);
        }

        case ker::abi::net::ops::NETCTL_ADDR_DEL: {
            auto* task = ker::mod::sched::get_current_task();
            WosNetAddrReq req{};
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            if (!ker::mod::sys::usercopy::copy_value_from_task(*task, a1, req)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            auto dev_ref = find_dev_by_ifindex(req.ifindex);
            if (!dev_ref) {
                return static_cast<uint64_t>(-ENODEV);
            }
            auto* dev = dev_ref.get();
            if (req.family == WOS_AF_INET6) {
                ker::net::proto::IPv6Address address{};
                int const PARSE_RESULT = netctl_parse_ipv6_address_request(req, address);
                if (PARSE_RESULT < 0) {
                    return static_cast<uint64_t>(PARSE_RESULT);
                }
                int const RET = ker::net::netif_del_ipv6(dev, address, req.prefix_len);
                return static_cast<uint64_t>(RET);
            }
            if (req.family != WOS_AF_INET || req.prefix_len > 32) {
                return static_cast<uint64_t>(-EINVAL);
            }
            uint32_t addr_be = 0;
            std::memcpy(&addr_be, req.local, sizeof(addr_be));
            if (addr_be == 0) {
                std::memcpy(&addr_be, req.address, sizeof(addr_be));
            }
            uint32_t const ADDR = ker::net::ntohl(addr_be);
            uint32_t const MASK = prefix_to_mask(req.prefix_len);
            int const RET = ker::net::netif_del_ipv4(dev, ADDR, MASK);
            return static_cast<uint64_t>(RET);
        }

        case ker::abi::net::ops::NETCTL_ADDR_SET_V2: {
            auto* task = ker::mod::sched::get_current_task();
            WosNetAddrReqV2 req{};
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            if (!ker::mod::sys::usercopy::copy_value_from_task(*task, a1, req)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            if (req.size != sizeof(req) || req.version != WOS_NETCTL_VERSION_1 || req.reserved != 0 || req.address.family != WOS_AF_INET6 ||
                req.address.prefix_len > 128 || req.preferred_lifetime_s > req.valid_lifetime_s) {
                return static_cast<uint64_t>(-EINVAL);
            }
            auto dev_ref = find_dev_by_ifindex(req.address.ifindex);
            if (!dev_ref) {
                return static_cast<uint64_t>(-ENODEV);
            }
            ker::net::proto::IPv6Address address{};
            int const PARSE_RESULT = netctl_parse_ipv6_address_request(req.address, address);
            if (PARSE_RESULT < 0) {
                return static_cast<uint64_t>(PARSE_RESULT);
            }
            uint64_t const PREFERRED_UNTIL = netctl_lifetime_deadline_ms(req.preferred_lifetime_s);
            uint64_t const VALID_UNTIL = netctl_lifetime_deadline_ms(req.valid_lifetime_s);
            int const RET = ker::net::netif_set_ipv6(dev_ref.get(), address, req.address.prefix_len, req.address.flags, PREFERRED_UNTIL,
                                                     VALID_UNTIL, req.address.replace != 0);
            return static_cast<uint64_t>(RET);
        }

        case ker::abi::net::ops::NETCTL_ROUTE_LIST: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            size_t capacity = 0;
            if (!ker::mod::sys::usercopy::copy_value_from_task(*task, a2, capacity) ||
                !ker::mod::sys::usercopy::ensure_writable(*task, a2, sizeof(capacity))) {
                return static_cast<uint64_t>(-EFAULT);
            }

            std::array<ker::net::IPv6RouteSnapshot, ker::net::MAX_IPV6_ROUTES> snapshots{};
            size_t const TOTAL = ker::net::route6_snapshot(snapshots.data(), snapshots.size());
            size_t const EMIT = a1 != 0 ? std::min({capacity, TOTAL, snapshots.size()}) : 0;
            size_t const OUTPUT_BYTES = EMIT * sizeof(WosNetRouteRecord);
            if (OUTPUT_BYTES != 0 && !ker::mod::sys::usercopy::ensure_writable(*task, a1, OUTPUT_BYTES)) {
                return static_cast<uint64_t>(-EFAULT);
            }

            std::array<WosNetRouteRecord, ker::net::MAX_IPV6_ROUTES> output{};
            uint64_t const NOW_MS = ker::mod::time::get_ms();
            for (size_t i = 0; i < EMIT; ++i) {
                auto& row = output.at(i);
                auto const& route = snapshots.at(i);
                row.size = sizeof(row);
                row.version = WOS_NETCTL_VERSION_1;
                row.family = WOS_AF_INET6;
                row.ifindex = route.ifindex;
                row.metric = route.metric;
                row.flags = route.flags & (WOS_NET_ROUTE_F_GATEWAY | WOS_NET_ROUTE_F_AUTOCONF);
                row.prefix_len = route.prefix_len;
                row.scope = netctl_ipv6_route_scope(route.prefix, route.prefix_len, route.gateway);
                std::memcpy(row.destination, route.prefix.data(), route.prefix.size());
                std::memcpy(row.gateway, route.gateway.data(), route.gateway.size());
                row.lifetime_s = netctl_remaining_lifetime_s(route.expires_at_ms, NOW_MS);
            }
            if ((OUTPUT_BYTES != 0 && !ker::mod::sys::usercopy::copy_to_task(*task, a1, output.data(), OUTPUT_BYTES)) ||
                !ker::mod::sys::usercopy::copy_value_to_task(*task, a2, TOTAL)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            return 0;
        }

        case ker::abi::net::ops::NETCTL_ROUTE_SET:
        case ker::abi::net::ops::NETCTL_ROUTE_DEL: {
            auto* task = ker::mod::sched::get_current_task();
            WosNetRouteRecord req{};
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            if (!ker::mod::sys::usercopy::copy_value_from_task(*task, a1, req)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            constexpr uint32_t ALLOWED_FLAGS = WOS_NET_ROUTE_F_GATEWAY | WOS_NET_ROUTE_F_AUTOCONF;
            if (req.size != sizeof(req) || req.version != WOS_NETCTL_VERSION_1 || req.family != WOS_AF_INET6 || req.prefix_len > 128 ||
                req.reserved != 0 || req.reserved2 != 0 || (req.flags & ~ALLOWED_FLAGS) != 0) {
                return static_cast<uint64_t>(-EINVAL);
            }
            auto dev_ref = find_dev_by_ifindex(req.ifindex);
            if (!dev_ref) {
                return static_cast<uint64_t>(-ENODEV);
            }
            ker::net::proto::IPv6Address destination{};
            ker::net::proto::IPv6Address gateway{};
            std::memcpy(destination.data(), req.destination, destination.size());
            std::memcpy(gateway.data(), req.gateway, gateway.size());
            bool const HAS_GATEWAY = (req.flags & WOS_NET_ROUTE_F_GATEWAY) != 0;
            uint8_t const EXPECTED_SCOPE = netctl_ipv6_route_scope(destination, req.prefix_len, gateway);
            if (destination.is_multicast() || gateway.is_multicast() || HAS_GATEWAY == gateway.is_unspecified() ||
                destination != destination.masked(req.prefix_len) || req.scope != EXPECTED_SCOPE) {
                return static_cast<uint64_t>(-EINVAL);
            }
            ker::net::IPv6RouteSpec spec{.prefix = destination,
                                         .gateway = gateway,
                                         .prefix_len = req.prefix_len,
                                         .metric = req.metric,
                                         .flags = req.flags,
                                         .expires_at_ms = netctl_lifetime_deadline_ms(req.lifetime_s),
                                         .dev_identity = dev_ref.identity()};
            int const RET = net_op == ker::abi::net::ops::NETCTL_ROUTE_SET ? ker::net::route6_add(spec) : ker::net::route6_del(spec);
            return static_cast<uint64_t>(RET);
        }

        case ker::abi::net::ops::NETCTL_LINK_SET: {
            auto* task = ker::mod::sched::get_current_task();
            WosNetLinkSetReq req{};
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            if (!ker::mod::sys::usercopy::copy_value_from_task(*task, a1, req)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            auto dev_ref = find_dev_for_link_req(&req);
            if (!dev_ref) {
                return static_cast<uint64_t>(-ENODEV);
            }
            auto* dev = dev_ref.get();

            if ((req.fields & WOS_NET_LINK_SET_MTU) != 0 && req.mtu == 0) {
                return static_cast<uint64_t>(-EINVAL);
            }
            if ((req.fields & WOS_NET_LINK_SET_HWADDR) != 0 && req.hwaddr_len != 6) {
                return static_cast<uint64_t>(-EINVAL);
            }
            if ((req.fields & WOS_NET_LINK_SET_NAME) != 0) {
                size_t const NAME_LEN = strnlen(req.new_name, WOS_NET_IF_NAME_LEN);
                if (NAME_LEN == 0 || NAME_LEN >= WOS_NET_IF_NAME_LEN) {
                    return static_cast<uint64_t>(-EINVAL);
                }
                auto existing_ref = ker::net::netdev_find_by_name_ref(std::string_view(req.new_name, NAME_LEN));
                if (existing_ref && existing_ref.get() != dev) {
                    return static_cast<uint64_t>(-EEXIST);
                }
            }

            if ((req.fields & WOS_NET_LINK_SET_FLAGS) != 0) {
                apply_ifflags(dev, req.flags, req.flag_mask);
            }
            if ((req.fields & WOS_NET_LINK_SET_MTU) != 0) {
                dev->mtu = req.mtu;
            }
            if ((req.fields & WOS_NET_LINK_SET_TXQLEN) != 0) {
                dev->tx_queue_len = req.tx_queue_len;
            }
            if ((req.fields & WOS_NET_LINK_SET_HWADDR) != 0) {
                int const RET = set_netdev_hwaddr(dev, req.hwaddr, req.hwaddr_len);
                if (RET < 0) {
                    return static_cast<uint64_t>(RET);
                }
            }
            if ((req.fields & WOS_NET_LINK_SET_NAME) != 0) {
                int const RET = rename_netdev(dev, req.new_name);
                if (RET < 0) {
                    return static_cast<uint64_t>(RET);
                }
            }
            if ((req.fields & (WOS_NET_LINK_SET_MTU | WOS_NET_LINK_SET_NAME)) != 0) {
                ker::net::wki::wki_dev_server_notify_net_changed(dev);
                ker::net::wki::wki_remotable_notify_net_changed(dev);
            }
            return 0;
        }

        case ker::abi::net::ops::SELECT: {
            // a1=nfds, a2=readfds, a3=writefds, a4=exceptfds, a5=timeval timeout
            if (a1 > WOS_FD_SETSIZE) {
                return static_cast<uint64_t>(-EINVAL);
            }
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            std::array<uint8_t, WOS_FD_SET_BYTES> readfds{};
            std::array<uint8_t, WOS_FD_SET_BYTES> writefds{};
            std::array<uint8_t, WOS_FD_SET_BYTES> exceptfds{};
            KSelectTimeval timeout{};

            auto snapshot_fd_set = [&](uint64_t user_addr, std::array<uint8_t, WOS_FD_SET_BYTES>& set) -> bool {
                return user_addr == 0 || (ker::mod::sys::usercopy::ensure_writable(*task, user_addr, set.size()) &&
                                          ker::mod::sys::usercopy::copy_from_task(*task, user_addr, set.data(), set.size()));
            };
            if (!snapshot_fd_set(a2, readfds) || !snapshot_fd_set(a3, writefds) || !snapshot_fd_set(a4, exceptfds) ||
                (a5 != 0 && !ker::mod::sys::usercopy::copy_value_from_task(*task, a5, timeout))) {
                return static_cast<uint64_t>(-EFAULT);
            }

            int const RESULT = run_select(static_cast<size_t>(a1), a2 != 0 ? readfds.data() : nullptr, a3 != 0 ? writefds.data() : nullptr,
                                          a4 != 0 ? exceptfds.data() : nullptr, a5 != 0 ? &timeout : nullptr);
            if (RESULT >= 0 && ((a2 != 0 && !ker::mod::sys::usercopy::copy_to_task(*task, a2, readfds.data(), readfds.size())) ||
                                (a3 != 0 && !ker::mod::sys::usercopy::copy_to_task(*task, a3, writefds.data(), writefds.size())) ||
                                (a4 != 0 && !ker::mod::sys::usercopy::copy_to_task(*task, a4, exceptfds.data(), exceptfds.size())))) {
                return static_cast<uint64_t>(-EFAULT);
            }
            return static_cast<uint64_t>(RESULT);
        }

        case ker::abi::net::ops::POLL: {
            // a1=pollfd_array_ptr, a2=nfds, a3=timeout_ms (-1=block, 0=immediate)
            size_t const NFDS = static_cast<size_t>(a2);
            if (NFDS > ker::mod::sched::task::Task::FD_TABLE_SIZE) {
                return static_cast<uint64_t>(-EINVAL);
            }
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            size_t const FDS_BYTES = NFDS * sizeof(KPollFd);
            if (FDS_BYTES != 0 &&
                (!ker::mod::sys::usercopy::ensure_writable(*task, a1, FDS_BYTES) || !ker::mod::sys::usercopy::range_valid(a1, FDS_BYTES))) {
                return static_cast<uint64_t>(-EFAULT);
            }
            auto* fds = NFDS != 0 ? new (std::nothrow) KPollFd[NFDS] : nullptr;
            if (NFDS != 0 && fds == nullptr) {
                return static_cast<uint64_t>(-ENOMEM);
            }
            if (FDS_BYTES != 0 && !ker::mod::sys::usercopy::copy_from_task(*task, a1, fds, FDS_BYTES)) {
                delete[] fds;
                return static_cast<uint64_t>(-EFAULT);
            }
            auto timeout = static_cast<int>(static_cast<int64_t>(a3));
            int const RESULT = run_poll_wait(fds, NFDS, timeout, "poll");
            if (RESULT >= 0 && FDS_BYTES != 0 && !ker::mod::sys::usercopy::copy_to_task(*task, a1, fds, FDS_BYTES)) {
                delete[] fds;
                return static_cast<uint64_t>(-EFAULT);
            }
            delete[] fds;
            return static_cast<uint64_t>(RESULT);
        }

        default:
            return static_cast<uint64_t>(-ENOSYS);
    }
}

}  // namespace ker::syscall::net
// NOLINTEND(cppcoreguidelines-avoid-c-arrays, modernize-avoid-c-arrays, cppcoreguidelines-pro-bounds-array-to-pointer-decay,
// cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
