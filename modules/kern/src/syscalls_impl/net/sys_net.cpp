#include "sys_net.hpp"

#include <abi/callnums/net.h>
#include <bits/posix/posix_string.h>
#include <bits/ssize_t.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <net/backlog.hpp>
#include <net/endian.hpp>
#include <net/netdevice.hpp>
#include <net/netif.hpp>
#include <net/netpoll.hpp>
#include <net/route.hpp>
#include <net/socket.hpp>
#include <net/wki/dev_server.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/remote_ipc.hpp>
#include <new>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
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
constexpr int WOS_MSG_DONTWAIT = 0x0040;
constexpr int WOS_O_NONBLOCK = 04000;
constexpr int WOS_O_RDWR = 2;
constexpr uint64_t USEC_PER_MSEC = 1000;

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

auto register_poll_waiter(ker::vfs::File* file, uint64_t pid) -> bool {
    if (file == nullptr) {
        return false;
    }
    if (file->fs_type == ker::vfs::FSType::SOCKET) {
        auto* sock = static_cast<ker::net::Socket*>(file->private_data);
        if (sock == nullptr) {
            return false;
        }
        sock->owner_pid = pid;
        return true;
    }
    if (file->fops != nullptr && file->fops->vfs_poll_register_waiter != nullptr) {
        return file->fops->vfs_poll_register_waiter(file, pid);
    }
    return false;
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

auto socket_effective_nonblock(const ker::vfs::File* file, const ker::net::Socket* sock, int call_flags) -> bool {
    bool const FILE_NONBLOCK = file != nullptr && (file->open_flags & WOS_O_NONBLOCK) != 0;
    bool const MSG_NONBLOCK = (call_flags & WOS_MSG_DONTWAIT) != 0;
    bool const SOCKET_NONBLOCK = sock != nullptr && sock->nonblock;
    return FILE_NONBLOCK || MSG_NONBLOCK || SOCKET_NONBLOCK;
}

struct SocketNonblockGuard {
    ker::net::Socket* sock;
    bool saved_nonblock;

    SocketNonblockGuard(ker::net::Socket* sock, bool effective_nonblock)
        : sock(sock), saved_nonblock(sock != nullptr ? sock->nonblock : false) {
        if (sock != nullptr) {
            sock->nonblock = effective_nonblock;
        }
    }

    ~SocketNonblockGuard() {
        if (sock != nullptr) {
            sock->nonblock = saved_nonblock;
        }
    }
};

template <typename T, typename Fn>
auto run_socket_call(ker::vfs::File* file, ker::net::Socket* sock, int call_flags, Fn&& fn) -> T {
    bool const EFFECTIVE_NONBLOCK = socket_effective_nonblock(file, sock, call_flags);
    SocketNonblockGuard const GUARD(sock, EFFECTIVE_NONBLOCK);
    for (;;) {
        T const RESULT = fn();
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
            return fn();
        }
        char const* wait_channel = task->wait_channel != nullptr ? task->wait_channel : "sock_wait";
        if (current_task_has_deliverable_signal()) {
            task->wait_channel = nullptr;
            return static_cast<T>(-EINTR);
        }

        drain_network_rx_work();
        T const AFTER_DRAIN = fn();
        if (AFTER_DRAIN != static_cast<T>(-EAGAIN) && AFTER_DRAIN != static_cast<T>(-EINPROGRESS)) {
            return AFTER_DRAIN;
        }

        ker::mod::sched::preemptible_syscall_park(wait_channel);
        if (current_task_has_deliverable_signal()) {
            return static_cast<T>(-EINTR);
        }
    }
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
    return clamp_io_count(run_socket_call<ssize_t>(f, sock, 0, [&]() { return sock->proto_ops->recv(sock, buf, count, 0); }), count);
}

ssize_t socket_fops_write(ker::vfs::File* f, const void* buf, size_t count, size_t /*unused*/) {
    if (f == nullptr || f->private_data == nullptr) {
        return -EINVAL;
    }
    auto* sock = static_cast<ker::net::Socket*>(f->private_data);
    if (sock->proto_ops == nullptr || sock->proto_ops->send == nullptr) {
        return -ENOSYS;
    }
    return clamp_io_count(run_socket_call<ssize_t>(f, sock, 0, [&]() { return sock->proto_ops->send(sock, buf, count, 0); }), count);
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
};

struct FileHandle {
    ker::vfs::File* file = nullptr;

    ~FileHandle() {
        if (file != nullptr) {
            ker::vfs::vfs_put_file(file);
        }
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

// Fill a sockaddr_in from socket local address
void fill_sockaddr_v4(void* addr_out, size_t* addr_len, uint32_t ip, uint16_t port) {
    size_t max_len = ker::net::SOCKADDR_V4_LEN;
    if (addr_len != nullptr) {
        max_len = *addr_len;
    }
    ker::net::socket_fill_sockaddr_v4(addr_out, max_len, addr_len, ip, port);
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
constexpr uint16_t WOS_ARPHRD_ETHER = 1;
constexpr uint16_t WOS_ARPHRD_LOOPBACK = 772;
constexpr uint32_t WOS_IFA_F_PERMANENT = 0x80;

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

auto find_dev_by_ifindex(uint32_t ifindex) -> ker::net::NetDevice* {
    for (size_t i = 0; i < ker::net::netdev_count(); i++) {
        auto* dev = ker::net::netdev_at(i);
        if (dev != nullptr && dev->ifindex == ifindex) {
            return dev;
        }
    }
    return nullptr;
}

auto find_dev_for_link_req(const WosNetLinkSetReq* req) -> ker::net::NetDevice* {
    if (req == nullptr) {
        return nullptr;
    }
    if (req->ifindex != 0) {
        return find_dev_by_ifindex(req->ifindex);
    }
    return ker::net::netdev_find_by_name(std::string_view(req->ifname, strnlen(req->ifname, WOS_NET_IF_NAME_LEN)));
}

auto rename_netdev(ker::net::NetDevice* dev, const char* new_name) -> int {
    size_t const LEN = strnlen(new_name, WOS_NET_IF_NAME_LEN);
    if (dev == nullptr || LEN == 0 || LEN >= WOS_NET_IF_NAME_LEN) {
        return -EINVAL;
    }
    if (ker::net::netdev_find_by_name(std::string_view(new_name, LEN)) != nullptr) {
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
            int const RESULT = sock->proto_ops->bind(sock, reinterpret_cast<const void*>(a2), static_cast<size_t>(a3));
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
            ker::net::Socket* new_sock = nullptr;
            auto* addr_len_ptr = reinterpret_cast<size_t*>(a3);
            int const RESULT = run_socket_call<int>(handle.file, sock, 0, [&]() {
                return sock->proto_ops->accept(sock, &new_sock, reinterpret_cast<void*>(a2), addr_len_ptr);
            });
            if (RESULT < 0 || new_sock == nullptr) {
                return static_cast<uint64_t>(RESULT);
            }
            // Set owner PID on accepted socket for wake_socket()
            auto* cur_task = ker::mod::sched::get_current_task();
            if (cur_task != nullptr) {
                new_sock->owner_pid = cur_task->pid;
            }

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
            int const RESULT = run_socket_call<int>(handle.file, sock, 0, [&]() {
                return sock->proto_ops->connect(sock, reinterpret_cast<const void*>(a2), static_cast<size_t>(a3));
            });
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
                    auto result = clamp_io_count(
                        file_handle.file->fops->vfs_write(file_handle.file, reinterpret_cast<const void*>(a2), static_cast<size_t>(a3), 0),
                        static_cast<size_t>(a3));
                    return static_cast<uint64_t>(result);
                }
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->send == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            auto const RESULT = run_socket_call<ssize_t>(handle.file, sock, static_cast<int>(a4), [&]() {
                return sock->proto_ops->send(sock, reinterpret_cast<const void*>(a2), static_cast<size_t>(a3), static_cast<int>(a4));
            });
            return static_cast<uint64_t>(clamp_io_count(RESULT, static_cast<size_t>(a3)));
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
                    auto result = clamp_io_count(
                        file_handle.file->fops->vfs_read(file_handle.file, reinterpret_cast<void*>(a2), static_cast<size_t>(a3), 0),
                        static_cast<size_t>(a3));
                    return static_cast<uint64_t>(result);
                }
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->recv == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            auto const RESULT = run_socket_call<ssize_t>(handle.file, sock, static_cast<int>(a4), [&]() {
                return sock->proto_ops->recv(sock, reinterpret_cast<void*>(a2), static_cast<size_t>(a3), static_cast<int>(a4));
            });
            return static_cast<uint64_t>(clamp_io_count(RESULT, static_cast<size_t>(a3)));
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
            size_t alen = addr_len_for_domain(sock->domain);
            auto const RESULT = run_socket_call<ssize_t>(handle.file, sock, static_cast<int>(a4), [&]() {
                return sock->proto_ops->sendto(sock, reinterpret_cast<const void*>(a2), static_cast<size_t>(a3), static_cast<int>(a4),
                                               reinterpret_cast<const void*>(a5), alen);
            });
            return static_cast<uint64_t>(clamp_io_count(RESULT, static_cast<size_t>(a3)));
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
            size_t alen = addr_len_for_domain(sock->domain);
            auto const RESULT = run_socket_call<ssize_t>(handle.file, sock, static_cast<int>(a4), [&]() {
                return sock->proto_ops->recvfrom(sock, reinterpret_cast<void*>(a2), static_cast<size_t>(a3), static_cast<int>(a4),
                                                 reinterpret_cast<void*>(a5), &alen);
            });
            return static_cast<uint64_t>(clamp_io_count(RESULT, static_cast<size_t>(a3)));
        }

        case ker::abi::net::ops::SETSOCKOPT: {
            // a1=fd, a2=level, a3=optname, a4=optval_ptr, a5=optlen
            auto handle = fd_to_socket(a1);
            auto* sock = handle.sock;
            if (sock == nullptr) {
                auto file_handle = fd_to_file(a1);
                if (file_handle.file != nullptr && ker::net::wki::wki_ipc_is_socket_proxy_file(file_handle.file)) {
                    int const RESULT =
                        ker::net::wki::wki_ipc_socket_setsockopt(file_handle.file, static_cast<int>(a2), static_cast<int>(a3),
                                                                 reinterpret_cast<const void*>(a4), static_cast<size_t>(a5));
                    return static_cast<uint64_t>(RESULT);
                }
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->setsockopt == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            int const RESULT = sock->proto_ops->setsockopt(sock, static_cast<int>(a2), static_cast<int>(a3),
                                                           reinterpret_cast<const void*>(a4), static_cast<size_t>(a5));
            return static_cast<uint64_t>(RESULT);
        }

        case ker::abi::net::ops::GETSOCKOPT: {
            // a1=fd, a2=level, a3=optname, a4=optval_ptr, a5=optlen_ptr
            auto handle = fd_to_socket(a1);
            auto* sock = handle.sock;
            if (sock == nullptr) {
                auto file_handle = fd_to_file(a1);
                if (file_handle.file != nullptr && ker::net::wki::wki_ipc_is_socket_proxy_file(file_handle.file)) {
                    int const RESULT =
                        ker::net::wki::wki_ipc_socket_getsockopt(file_handle.file, static_cast<int>(a2), static_cast<int>(a3),
                                                                 reinterpret_cast<void*>(a4), reinterpret_cast<size_t*>(a5));
                    return static_cast<uint64_t>(RESULT);
                }
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->getsockopt == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            int const RESULT = sock->proto_ops->getsockopt(sock, static_cast<int>(a2), static_cast<int>(a3), reinterpret_cast<void*>(a4),
                                                           reinterpret_cast<size_t*>(a5));
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
                    int const RESULT = ker::net::wki::wki_ipc_socket_getpeername(file_handle.file, reinterpret_cast<void*>(a2),
                                                                                 reinterpret_cast<size_t*>(a3));
                    return static_cast<uint64_t>(RESULT);
                }
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->domain == 2) {  // AF_INET
                if (sock->remote_v4.port == 0 && sock->remote_v4.addr == 0) {
                    return static_cast<uint64_t>(-ENOTCONN);
                }
                auto* len_ptr = reinterpret_cast<size_t*>(a3);
                fill_sockaddr_v4(reinterpret_cast<void*>(a2), len_ptr, sock->remote_v4.addr, sock->remote_v4.port);
            } else {
                return static_cast<uint64_t>(-EAFNOSUPPORT);
            }
            return 0;
        }

        case ker::abi::net::ops::GETSOCKNAME: {
            // a1=fd, a2=addr_out, a3=addr_len_ptr
            auto handle = fd_to_socket(a1);
            auto* sock = handle.sock;
            if (sock == nullptr) {
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->domain == 2) {  // AF_INET
                auto* len_ptr = reinterpret_cast<size_t*>(a3);
                fill_sockaddr_v4(reinterpret_cast<void*>(a2), len_ptr, sock->local_v4.addr, sock->local_v4.port);
            } else {
                return static_cast<uint64_t>(-EAFNOSUPPORT);
            }
            return 0;
        }

        case ker::abi::net::ops::IOCTL_NET: {
            // a1=request, a2=arg_ptr
            auto request = static_cast<uint32_t>(a1);
            auto* arg = reinterpret_cast<uint8_t*>(a2);

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
                    ker::net::NetDevice* rdev = nullptr;
                    if (gw != 0) {
                        for (size_t i = 0; i < ker::net::netdev_count(); i++) {
                            auto* d = ker::net::netdev_at(i);
                            if (d == nullptr || d->state != 1 || std::strcmp(d->name.data(), "lo") == 0) {
                                continue;
                            }
                            auto* nif = ker::net::netif_get(d);
                            if (nif != nullptr && nif->ipv4_addr_count > 0) {
                                uint32_t const DEV_IP = nif->ipv4_addrs[0].addr;
                                uint32_t const DEV_MASK = nif->ipv4_addrs[0].netmask;
                                if ((DEV_IP & DEV_MASK) == (gw & DEV_MASK)) {
                                    rdev = d;
                                    break;
                                }
                            }
                        }
                    } else {
                        for (size_t i = 0; i < ker::net::netdev_count(); i++) {
                            auto* d = ker::net::netdev_at(i);
                            if (d == nullptr || d->state != 1 || std::strcmp(d->name.data(), "lo") == 0) {
                                continue;
                            }
                            auto* nif = ker::net::netif_get(d);
                            if (nif == nullptr || nif->ipv4_addr_count == 0) {
                                continue;
                            }
                            for (size_t j = 0; j < nif->ipv4_addr_count; j++) {
                                uint32_t const DEV_IP = nif->ipv4_addrs[j].addr;
                                if ((DEV_IP & mask) == (dst & mask)) {
                                    rdev = d;
                                    break;
                                }
                            }
                            if (rdev != nullptr) {
                                break;
                            }
                        }
                    }
                    // Gateway routes can still fall back to the first
                    // non-loopback UP device if there is only one usable path.
                    if (rdev == nullptr) {
                        if (gw != 0) {
                            for (size_t i = 0; i < ker::net::netdev_count(); i++) {
                                auto* d = ker::net::netdev_at(i);
                                if (d != nullptr && d->state == 1 && std::strcmp(d->name.data(), "lo") != 0) {
                                    rdev = d;
                                    break;
                                }
                            }
                        }
                    }
                    if (rdev == nullptr) {
                        return static_cast<uint64_t>(-ENODEV);
                    }
                    int const RET = ker::net::route_add(dst, mask, gw, 0, rdev);
                    return static_cast<uint64_t>(RET);
                }
                int const RET = ker::net::route_del(dst, mask);
                return static_cast<uint64_t>(RET);
            }

            // All other SIOC* ioctls use ifreq: name at offset 0, data at offset 16
            std::string_view const IFNAME(reinterpret_cast<char*>(arg), strnlen(reinterpret_cast<char*>(arg), 16));

            auto* dev = ker::net::netdev_find_by_name(IFNAME);
            if (dev == nullptr) {
                return static_cast<uint64_t>(-ENODEV);
            }

            switch (request) {
                case SIOC_GIFFLAGS: {
                    store_unaligned<int16_t>(arg + 16, static_cast<int16_t>(effective_ifflags(dev)));
                    return 0;
                }
                case SIOC_SIFFLAGS: {
                    auto flags = static_cast<uint32_t>(load_unaligned<uint16_t>(arg + 16));
                    apply_ifflags(dev, flags, IFF_UP | IFF_NOARP | IFF_PROMISC | IFF_MULTICAST);
                    return 0;
                }
                case SIOC_GIFADDR: {
                    auto* nif = ker::net::netif_get(dev);
                    if (nif == nullptr || nif->ipv4_addr_count == 0) {
                        return static_cast<uint64_t>(-EADDRNOTAVAIL);
                    }
                    // Fill sockaddr_in at offset 16
                    std::memset(arg + 16, 0, 16);
                    store_unaligned<uint16_t>(arg + 16, 2);  // AF_INET
                    store_unaligned<uint32_t>(arg + 20, ker::net::htonl(nif->ipv4_addrs[0].addr));
                    return 0;
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
                    auto* nif = ker::net::netif_get(dev);
                    if (nif == nullptr || nif->ipv4_addr_count == 0) {
                        return static_cast<uint64_t>(-EADDRNOTAVAIL);
                    }
                    std::memset(arg + 16, 0, 16);
                    store_unaligned<uint16_t>(arg + 16, 2);  // AF_INET
                    store_unaligned<uint32_t>(arg + 20, ker::net::htonl(nif->ipv4_addrs[0].netmask));
                    return 0;
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
                    return 0;
                }
                case SIOC_GIFMTU: {
                    store_unaligned<int32_t>(arg + 16, static_cast<int32_t>(dev->mtu));
                    return 0;
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
                    return 0;
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
                    return 0;
                }
                default:
                    return static_cast<uint64_t>(-ENOSYS);
            }
        }

        case ker::abi::net::ops::SET_DEV_CPU_AFFINITY: {
            // a1 = ptr to { char ifname[16]; uint64_t cpu_mask }
            // cpu_mask: each set bit (from LSB) is the CPU for the next queue pair:
            //   pair 0 ← CPU of lowest set bit, pair 1 ← CPU of next set bit, ...
            auto* req = reinterpret_cast<uint8_t*>(a1);
            if (req == nullptr) {
                return static_cast<uint64_t>(-EINVAL);
            }
            std::string_view const IFNAME(reinterpret_cast<char*>(req), strnlen(reinterpret_cast<char*>(req), 16));
            auto const CPU_MASK = load_unaligned<uint64_t>(req + 16);
            auto* dev = ker::net::netdev_find_by_name(IFNAME);
            if (dev == nullptr) {
                return static_cast<uint64_t>(-ENODEV);
            }
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
            auto* out = reinterpret_cast<WosNetIfInfo*>(a1);
            auto* count_ptr = reinterpret_cast<size_t*>(a2);
            if (count_ptr == nullptr) {
                return static_cast<uint64_t>(-EINVAL);
            }
            size_t const CAP = *count_ptr;
            size_t const TOTAL = ker::net::netdev_count();
            size_t const EMIT = (out != nullptr) ? std::min(CAP, TOTAL) : 0;
            for (size_t i = 0; i < EMIT; i++) {
                fill_if_info(out[i], ker::net::netdev_at(i));
            }
            *count_ptr = TOTAL;
            return 0;
        }

        case ker::abi::net::ops::NETCTL_ADDR_LIST: {
            auto* out = reinterpret_cast<WosNetAddrInfo*>(a1);
            auto* count_ptr = reinterpret_cast<size_t*>(a2);
            if (count_ptr == nullptr) {
                return static_cast<uint64_t>(-EINVAL);
            }

            size_t const CAP = *count_ptr;
            size_t total = 0;
            size_t written = 0;
            for (size_t i = 0; i < ker::net::netdev_count(); i++) {
                auto* dev = ker::net::netdev_at(i);
                auto* nif = ker::net::netif_get(dev);
                if (dev == nullptr || nif == nullptr) {
                    continue;
                }
                for (size_t j = 0; j < nif->ipv4_addr_count; j++) {
                    if (out != nullptr && written < CAP) {
                        auto& info = out[written];
                        info = {};
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
                        written++;
                    }
                    total++;
                }
            }
            *count_ptr = total;
            return 0;
        }

        case ker::abi::net::ops::NETCTL_ADDR_SET: {
            const auto* req = reinterpret_cast<const WosNetAddrReq*>(a1);
            if (req == nullptr || req->family != WOS_AF_INET) {
                return static_cast<uint64_t>(-EINVAL);
            }
            auto* dev = find_dev_by_ifindex(req->ifindex);
            if (dev == nullptr) {
                return static_cast<uint64_t>(-ENODEV);
            }
            uint32_t addr_be = 0;
            std::memcpy(&addr_be, req->local, sizeof(addr_be));
            if (addr_be == 0) {
                std::memcpy(&addr_be, req->address, sizeof(addr_be));
            }
            uint32_t const ADDR = ker::net::ntohl(addr_be);
            uint32_t const MASK = prefix_to_mask(req->prefix_len);
            int const RET = ker::net::netif_set_ipv4(dev, ADDR, MASK, req->replace != 0);
            return static_cast<uint64_t>(RET);
        }

        case ker::abi::net::ops::NETCTL_ADDR_DEL: {
            const auto* req = reinterpret_cast<const WosNetAddrReq*>(a1);
            if (req == nullptr || req->family != WOS_AF_INET) {
                return static_cast<uint64_t>(-EINVAL);
            }
            auto* dev = find_dev_by_ifindex(req->ifindex);
            if (dev == nullptr) {
                return static_cast<uint64_t>(-ENODEV);
            }
            uint32_t addr_be = 0;
            std::memcpy(&addr_be, req->local, sizeof(addr_be));
            if (addr_be == 0) {
                std::memcpy(&addr_be, req->address, sizeof(addr_be));
            }
            uint32_t const ADDR = ker::net::ntohl(addr_be);
            uint32_t const MASK = prefix_to_mask(req->prefix_len);
            int const RET = ker::net::netif_del_ipv4(dev, ADDR, MASK);
            return static_cast<uint64_t>(RET);
        }

        case ker::abi::net::ops::NETCTL_LINK_SET: {
            const auto* req = reinterpret_cast<const WosNetLinkSetReq*>(a1);
            auto* dev = find_dev_for_link_req(req);
            if (dev == nullptr) {
                return static_cast<uint64_t>(-ENODEV);
            }

            if ((req->fields & WOS_NET_LINK_SET_FLAGS) != 0) {
                apply_ifflags(dev, req->flags, req->flag_mask);
            }
            if ((req->fields & WOS_NET_LINK_SET_MTU) != 0) {
                if (req->mtu == 0) {
                    return static_cast<uint64_t>(-EINVAL);
                }
                dev->mtu = req->mtu;
            }
            if ((req->fields & WOS_NET_LINK_SET_TXQLEN) != 0) {
                dev->tx_queue_len = req->tx_queue_len;
            }
            if ((req->fields & WOS_NET_LINK_SET_HWADDR) != 0) {
                int const RET = set_netdev_hwaddr(dev, req->hwaddr, req->hwaddr_len);
                if (RET < 0) {
                    return static_cast<uint64_t>(RET);
                }
            }
            if ((req->fields & WOS_NET_LINK_SET_NAME) != 0) {
                int const RET = rename_netdev(dev, req->new_name);
                if (RET < 0) {
                    return static_cast<uint64_t>(RET);
                }
            }
            if ((req->fields & (WOS_NET_LINK_SET_MTU | WOS_NET_LINK_SET_NAME)) != 0) {
                ker::net::wki::wki_dev_server_notify_net_changed(dev);
                ker::net::wki::wki_remotable_notify_net_changed(dev);
            }
            return 0;
        }

        case ker::abi::net::ops::POLL: {
            // a1=pollfd_array_ptr, a2=nfds, a3=timeout_ms (-1=block, 0=immediate)
            struct KPollFd {
                int32_t fd;
                int16_t events;
                int16_t revents;
            };
            auto* fds = reinterpret_cast<KPollFd*>(a1);
            auto nfds = static_cast<size_t>(a2);
            auto timeout = static_cast<int>(static_cast<int64_t>(a3));

            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-EINVAL);
            }

            for (;;) {
                uint64_t const DEADLINE_US = begin_poll_timeout(task, timeout);

                drain_network_rx_work();

                int num_events = 0;
                for (size_t i = 0; i < nfds; i++) {
                    fds[i].revents = 0;

                    if (fds[i].fd < 0) {
                        continue;
                    }

                    auto* file = ker::vfs::vfs_get_file_retain(task, fds[i].fd);
                    if (file == nullptr) {
                        fds[i].revents = 0x0020;  // POLLNVAL
                        num_events++;
                        continue;
                    }

                    if (file->fs_type == ker::vfs::FSType::SOCKET) {
                        auto* sock = static_cast<ker::net::Socket*>(file->private_data);
                        if (sock != nullptr && sock->proto_ops != nullptr && sock->proto_ops->poll_check != nullptr) {
                            fds[i].revents = static_cast<int16_t>(sock->proto_ops->poll_check(sock, fds[i].events));
                        }
                    } else if (file->fops != nullptr && file->fops->vfs_poll_check != nullptr) {
                        // Use per-file poll_check callback (pipes, etc.)
                        fds[i].revents = static_cast<int16_t>(file->fops->vfs_poll_check(file, fds[i].events));
                    } else {
                        // Non-socket fds without poll_check (regular files, devices) are always ready
                        fds[i].revents = static_cast<int16_t>(fds[i].events & (0x0001 | 0x0004));  // POLLIN|POLLOUT
                    }
                    ker::vfs::vfs_put_file(file);

                    if (fds[i].revents != 0) {
                        num_events++;
                    }
                }

                if (num_events > 0 || timeout == 0) {
                    clear_poll_timeout(task);
                    return static_cast<uint64_t>(num_events);
                }

                if (DEADLINE_US != 0 && ker::mod::time::get_us() >= DEADLINE_US) {
                    clear_poll_timeout(task);
                    return 0;
                }

                if (current_task_has_deliverable_signal()) {
                    clear_poll_timeout(task);
                    return static_cast<uint64_t>(-EINTR);
                }

                bool can_block = (nfds > 0);
                if (can_block) {
                    for (size_t i = 0; i < nfds; i++) {
                        if (fds[i].fd < 0) {
                            continue;
                        }
                        auto* file = ker::vfs::vfs_get_file_retain(task, fds[i].fd);
                        bool const OK = (file != nullptr) && register_poll_waiter(file, task->pid);
                        if (file != nullptr) {
                            ker::vfs::vfs_put_file(file);
                        }
                        if (!OK) {
                            can_block = false;
                            break;
                        }
                    }
                }

                if (can_block) {
                    drain_network_rx_work();

                    // Re-check fds after waiter registration to close the race
                    // window where an event fires between the initial scan and sleep.
                    int recheck = 0;
                    for (size_t i = 0; i < nfds; i++) {
                        fds[i].revents = 0;
                        if (fds[i].fd < 0) {
                            continue;
                        }
                        auto* rf = ker::vfs::vfs_get_file_retain(task, fds[i].fd);
                        if (rf == nullptr) {
                            continue;
                        }
                        if (rf->fs_type == ker::vfs::FSType::SOCKET) {
                            auto* sock = static_cast<ker::net::Socket*>(rf->private_data);
                            if (sock != nullptr && sock->proto_ops != nullptr && sock->proto_ops->poll_check != nullptr) {
                                fds[i].revents = static_cast<int16_t>(sock->proto_ops->poll_check(sock, fds[i].events));
                            }
                        } else if (rf->fops != nullptr && rf->fops->vfs_poll_check != nullptr) {
                            fds[i].revents = static_cast<int16_t>(rf->fops->vfs_poll_check(rf, fds[i].events));
                        } else {
                            fds[i].revents = static_cast<int16_t>(fds[i].events & (0x0001 | 0x0004));
                        }
                        ker::vfs::vfs_put_file(rf);
                        if (fds[i].revents != 0) {
                            recheck++;
                        }
                    }
                    if (recheck > 0) {
                        clear_poll_timeout(task);
                        return static_cast<uint64_t>(recheck);
                    }

                    ker::mod::sched::preemptible_syscall_park("poll", DEADLINE_US);
                } else {
                    ker::mod::sched::kern_yield();
                }
            }
        }

        default:
            return static_cast<uint64_t>(-ENOSYS);
    }
}

}  // namespace ker::syscall::net
// NOLINTEND(cppcoreguidelines-avoid-c-arrays, modernize-avoid-c-arrays, cppcoreguidelines-pro-bounds-array-to-pointer-decay,
// cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
