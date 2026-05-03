#include "sys_net.hpp"

#include <abi/callnums/net.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <net/endian.hpp>
#include <net/netdevice.hpp>
#include <net/netif.hpp>
#include <net/route.hpp>
#include <net/socket.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <vfs/file.hpp>
#include <vfs/vfs.hpp>

namespace ker::syscall::net {

namespace {
constexpr int WOS_ERESTARTSYS = 512;
constexpr uint64_t USEC_PER_MSEC = 1000;

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
    if (task->pollWaitDeadlineUs == 0) {
        task->pollWaitDeadlineUs = ker::mod::time::getUs() + (static_cast<uint64_t>(timeout_ms) * USEC_PER_MSEC);
    }
    return task->pollWaitDeadlineUs;
}

void clear_poll_timeout(ker::mod::sched::task::Task* task) {
    if (task != nullptr) {
        task->pollWaitDeadlineUs = 0;
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

ssize_t socket_fops_read(ker::vfs::File* f, void* buf, size_t count, size_t) {
    if (f == nullptr || f->private_data == nullptr) {
        return -EINVAL;
    }
    auto* sock = static_cast<ker::net::Socket*>(f->private_data);
    if (sock->proto_ops == nullptr || sock->proto_ops->recv == nullptr) {
        return -ENOSYS;
    }
    return sock->proto_ops->recv(sock, buf, count, 0);
}

ssize_t socket_fops_write(ker::vfs::File* f, const void* buf, size_t count, size_t) {
    if (f == nullptr || f->private_data == nullptr) {
        return -EINVAL;
    }
    auto* sock = static_cast<ker::net::Socket*>(f->private_data);
    if (sock->proto_ops == nullptr || sock->proto_ops->send == nullptr) {
        return -ENOSYS;
    }
    return sock->proto_ops->send(sock, buf, count, 0);
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
};

// Get socket from fd using VFS helpers
auto fd_to_socket(uint64_t fd_num) -> ker::net::Socket* {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return nullptr;
    }

    auto* file = ker::vfs::vfs_get_file(task, static_cast<int>(fd_num));
    if (file == nullptr || file->fs_type != ker::vfs::FSType::SOCKET) {
        return nullptr;
    }

    return static_cast<ker::net::Socket*>(file->private_data);
}

// Allocate fd for a socket using VFS helpers
auto allocate_socket_fd(ker::net::Socket* sock) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -1;
    }

    auto* file = static_cast<ker::vfs::File*>(ker::mod::mm::dyn::kmalloc::calloc(1, sizeof(ker::vfs::File)));
    if (file == nullptr) {
        return -1;
    }

    file->private_data = sock;
    file->fops = &socket_fops;
    file->pos = 0;
    file->is_directory = false;
    file->fs_type = ker::vfs::FSType::SOCKET;
    file->refcount = 1;

    int fd = ker::vfs::vfs_alloc_fd(task, file);
    if (fd < 0) {
        ker::mod::mm::dyn::kmalloc::free(static_cast<void*>(file));
        return -1;
    }
    return fd;
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
    auto* out = static_cast<uint8_t*>(addr_out);
    std::memset(out, 0, 16);
    *reinterpret_cast<uint16_t*>(out) = 2;  // AF_INET
    *reinterpret_cast<uint16_t*>(out + 2) = ker::net::htons(port);
    *reinterpret_cast<uint32_t*>(out + 4) = ker::net::htonl(ip);
    if (addr_len != nullptr) {
        *addr_len = 16;
    }
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

constexpr uint32_t WOS_NET_LINK_SET_FLAGS = 1u << 0;
constexpr uint32_t WOS_NET_LINK_SET_MTU = 1u << 1;
constexpr uint32_t WOS_NET_LINK_SET_TXQLEN = 1u << 2;
constexpr uint32_t WOS_NET_LINK_SET_NAME = 1u << 3;
constexpr uint32_t WOS_NET_LINK_SET_HWADDR = 1u << 4;

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
        return 0xFFFFFFFFu;
    }
    return 0xFFFFFFFFu << (32 - prefix);
}

auto mask_to_prefix(uint32_t mask) -> uint8_t {
    uint8_t prefix = 0;
    while ((mask & 0x80000000u) != 0) {
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

    constexpr uint32_t stored_mask = IFF_BROADCAST | IFF_LOOPBACK | IFF_NOARP | IFF_PROMISC | IFF_MULTICAST;
    uint32_t store = mask & stored_mask;
    dev->link_flags &= ~store;
    dev->link_flags |= flags & store;
}

void fill_if_info(WosNetIfInfo& out, ker::net::NetDevice* dev) {
    std::memset(&out, 0, sizeof(out));
    if (dev == nullptr) {
        return;
    }
    out.ifindex = dev->ifindex;
    std::strncpy(out.name, dev->name.data(), sizeof(out.name) - 1);
    out.flags = effective_ifflags(dev);
    out.mtu = dev->mtu;
    out.tx_queue_len = dev->tx_queue_len;
    out.type = ((out.flags & IFF_LOOPBACK) != 0) ? WOS_ARPHRD_LOOPBACK : WOS_ARPHRD_ETHER;
    out.addr_len = 6;
    out.operstate = dev->state != 0 ? 6 : 2;  // Linux IF_OPER_UP / IF_OPER_DOWN
    std::memcpy(out.addr, dev->mac.data(), 6);
    std::memset(out.broadcast, 0xff, 6);
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
    size_t len = strnlen(new_name, WOS_NET_IF_NAME_LEN);
    if (dev == nullptr || len == 0 || len >= WOS_NET_IF_NAME_LEN) {
        return -EINVAL;
    }
    if (ker::net::netdev_find_by_name(std::string_view(new_name, len)) != nullptr) {
        return -EEXIST;
    }
    std::memset(dev->name.data(), 0, dev->name.size());
    std::memcpy(dev->name.data(), new_name, len);
    return 0;
}

auto set_netdev_hwaddr(ker::net::NetDevice* dev, const uint8_t* hwaddr, uint8_t len) -> int {
    if (dev == nullptr || hwaddr == nullptr || len != 6) {
        return -EINVAL;
    }
    if (dev->ops != nullptr && dev->ops->set_mac != nullptr) {
        dev->ops->set_mac(dev, hwaddr);
    } else {
        std::memcpy(dev->mac.data(), hwaddr, 6);
    }
    return 0;
}
}  // namespace

uint64_t sys_net(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4, uint64_t a5) {
    auto net_op = static_cast<ker::abi::net::ops>(op);

    switch (net_op) {
        case ker::abi::net::ops::SOCKET: {
            // a1=domain, a2=type, a3=protocol
            int domain = static_cast<int>(a1);
            int type = static_cast<int>(a2);
            int protocol = static_cast<int>(a3);

            auto* sock = ker::net::socket_create(domain, type, protocol);
            if (sock == nullptr) {
                return static_cast<uint64_t>(-ENOMEM);
            }

            // Set owner PID so wake_socket() can find and wake this task
            auto* task = ker::mod::sched::get_current_task();
            if (task != nullptr) {
                sock->owner_pid = task->pid;
            }

            int fd = allocate_socket_fd(sock);
            if (fd < 0) {
                ker::net::socket_destroy(sock);
                return static_cast<uint64_t>(-EMFILE);
            }

            return static_cast<uint64_t>(fd);
        }

        case ker::abi::net::ops::BIND: {
            // a1=fd, a2=addr_ptr, a3=addr_len
            auto* sock = fd_to_socket(a1);
            if (sock == nullptr) {
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->bind == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            int result = sock->proto_ops->bind(sock, reinterpret_cast<const void*>(a2), static_cast<size_t>(a3));
            return static_cast<uint64_t>(result);
        }

        case ker::abi::net::ops::LISTEN: {
            // a1=fd, a2=backlog
            auto* sock = fd_to_socket(a1);
            if (sock == nullptr) {
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->listen == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            int result = sock->proto_ops->listen(sock, static_cast<int>(a2));
            return static_cast<uint64_t>(result);
        }

        case ker::abi::net::ops::ACCEPT: {
            // a1=fd, a2=addr_ptr, a3=addr_len_ptr
            auto* sock = fd_to_socket(a1);
            if (sock == nullptr) {
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->accept == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            ker::net::Socket* new_sock = nullptr;
            auto* addr_len_ptr = reinterpret_cast<size_t*>(a3);
            int result = sock->proto_ops->accept(sock, &new_sock, reinterpret_cast<void*>(a2), addr_len_ptr);
            if (result < 0 || new_sock == nullptr) {
                return static_cast<uint64_t>(result);
            }
            // Set owner PID on accepted socket for wake_socket()
            auto* cur_task = ker::mod::sched::get_current_task();
            if (cur_task != nullptr) {
                new_sock->owner_pid = cur_task->pid;
            }

            int new_fd = allocate_socket_fd(new_sock);
            if (new_fd < 0) {
                ker::net::socket_destroy(new_sock);
                return static_cast<uint64_t>(-EMFILE);
            }
            return static_cast<uint64_t>(new_fd);
        }

        case ker::abi::net::ops::CONNECT: {
            // a1=fd, a2=addr_ptr, a3=addr_len
            auto* sock = fd_to_socket(a1);
            if (sock == nullptr) {
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->connect == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            int result = sock->proto_ops->connect(sock, reinterpret_cast<const void*>(a2), static_cast<size_t>(a3));
            return static_cast<uint64_t>(result);
        }

        case ker::abi::net::ops::SEND: {
            // a1=fd, a2=buf, a3=len, a4=flags
            auto* sock = fd_to_socket(a1);
            if (sock == nullptr) {
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->send == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            ssize_t result = sock->proto_ops->send(sock, reinterpret_cast<const void*>(a2), static_cast<size_t>(a3), static_cast<int>(a4));
            return static_cast<uint64_t>(result);
        }

        case ker::abi::net::ops::RECV: {
            // a1=fd, a2=buf, a3=len, a4=flags
            auto* sock = fd_to_socket(a1);
            if (sock == nullptr) {
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->recv == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            ssize_t result = sock->proto_ops->recv(sock, reinterpret_cast<void*>(a2), static_cast<size_t>(a3), static_cast<int>(a4));
            return static_cast<uint64_t>(result);
        }

        case ker::abi::net::ops::CLOSE: {
            // a1=fd - use VFS close which calls socket_fops_close
            int result = ker::vfs::vfs_close(static_cast<int>(a1));
            return static_cast<uint64_t>(result);
        }

        case ker::abi::net::ops::SENDTO: {
            // a1=fd, a2=buf, a3=len, a4=flags, a5=addr_ptr
            auto* sock = fd_to_socket(a1);
            if (sock == nullptr) {
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->sendto == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            size_t alen = addr_len_for_domain(sock->domain);
            ssize_t result = sock->proto_ops->sendto(sock, reinterpret_cast<const void*>(a2), static_cast<size_t>(a3), static_cast<int>(a4),
                                                     reinterpret_cast<const void*>(a5), alen);
            return static_cast<uint64_t>(result);
        }

        case ker::abi::net::ops::RECVFROM: {
            // a1=fd, a2=buf, a3=len, a4=flags, a5=addr_out_ptr
            auto* sock = fd_to_socket(a1);
            if (sock == nullptr) {
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->recvfrom == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            size_t alen = addr_len_for_domain(sock->domain);
            ssize_t result = sock->proto_ops->recvfrom(sock, reinterpret_cast<void*>(a2), static_cast<size_t>(a3), static_cast<int>(a4),
                                                       reinterpret_cast<void*>(a5), &alen);
            return static_cast<uint64_t>(result);
        }

        case ker::abi::net::ops::SETSOCKOPT: {
            // a1=fd, a2=level, a3=optname, a4=optval_ptr, a5=optlen
            auto* sock = fd_to_socket(a1);
            if (sock == nullptr) {
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->setsockopt == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            int result = sock->proto_ops->setsockopt(sock, static_cast<int>(a2), static_cast<int>(a3), reinterpret_cast<const void*>(a4),
                                                     static_cast<size_t>(a5));
            return static_cast<uint64_t>(result);
        }

        case ker::abi::net::ops::GETSOCKOPT: {
            // a1=fd, a2=level, a3=optname, a4=optval_ptr, a5=optlen_ptr
            auto* sock = fd_to_socket(a1);
            if (sock == nullptr) {
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->getsockopt == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            int result = sock->proto_ops->getsockopt(sock, static_cast<int>(a2), static_cast<int>(a3), reinterpret_cast<void*>(a4),
                                                     reinterpret_cast<size_t*>(a5));
            return static_cast<uint64_t>(result);
        }

        case ker::abi::net::ops::SHUTDOWN: {
            // a1=fd, a2=how
            auto* sock = fd_to_socket(a1);
            if (sock == nullptr) {
                return static_cast<uint64_t>(-EBADF);
            }
            if (sock->proto_ops == nullptr || sock->proto_ops->shutdown == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            int result = sock->proto_ops->shutdown(sock, static_cast<int>(a2));
            return static_cast<uint64_t>(result);
        }

        case ker::abi::net::ops::GETPEERNAME: {
            // a1=fd, a2=addr_out, a3=addr_len_ptr
            auto* sock = fd_to_socket(a1);
            if (sock == nullptr) {
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
            auto* sock = fd_to_socket(a1);
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
                uint32_t dst = *reinterpret_cast<uint32_t*>(rt + 12);
                uint32_t gw = *reinterpret_cast<uint32_t*>(rt + 28);
                uint32_t mask = *reinterpret_cast<uint32_t*>(rt + 44);
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
                                uint32_t dev_ip = nif->ipv4_addrs[0].addr;
                                uint32_t dev_mask = nif->ipv4_addrs[0].netmask;
                                if ((dev_ip & dev_mask) == (gw & dev_mask)) {
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
                                uint32_t dev_ip = nif->ipv4_addrs[j].addr;
                                if ((dev_ip & mask) == (dst & mask)) {
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
                    int ret = ker::net::route_add(dst, mask, gw, 0, rdev);
                    return static_cast<uint64_t>(ret);
                }
                int ret = ker::net::route_del(dst, mask);
                return static_cast<uint64_t>(ret);
            }

            // All other SIOC* ioctls use ifreq: name at offset 0, data at offset 16
            std::string_view ifname(reinterpret_cast<char*>(arg), strnlen(reinterpret_cast<char*>(arg), 16));

            auto* dev = ker::net::netdev_find_by_name(ifname);
            if (dev == nullptr) {
                return static_cast<uint64_t>(-ENODEV);
            }

            switch (request) {
                case SIOC_GIFFLAGS: {
                    *reinterpret_cast<int16_t*>(arg + 16) = static_cast<int16_t>(effective_ifflags(dev));
                    return 0;
                }
                case SIOC_SIFFLAGS: {
                    auto flags = static_cast<uint32_t>(*reinterpret_cast<uint16_t*>(arg + 16));
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
                    *reinterpret_cast<uint16_t*>(arg + 16) = 2;  // AF_INET
                    *reinterpret_cast<uint32_t*>(arg + 20) = ker::net::htonl(nif->ipv4_addrs[0].addr);
                    return 0;
                }
                case SIOC_SIFADDR: {
                    uint32_t addr = ker::net::ntohl(*reinterpret_cast<uint32_t*>(arg + 20));
                    // Check if interface already has addresses; if so, update first one
                    auto* nif = ker::net::netif_get(dev);
                    if (nif != nullptr && nif->ipv4_addr_count > 0) {
                        nif->ipv4_addrs[0].addr = addr;
                    } else {
                        ker::net::netif_add_ipv4(dev, addr, 0xFFFFFF00);  // default /24
                    }
                    return 0;
                }
                case SIOC_GIFNETMASK: {
                    auto* nif = ker::net::netif_get(dev);
                    if (nif == nullptr || nif->ipv4_addr_count == 0) {
                        return static_cast<uint64_t>(-EADDRNOTAVAIL);
                    }
                    std::memset(arg + 16, 0, 16);
                    *reinterpret_cast<uint16_t*>(arg + 16) = 2;  // AF_INET
                    *reinterpret_cast<uint32_t*>(arg + 20) = ker::net::htonl(nif->ipv4_addrs[0].netmask);
                    return 0;
                }
                case SIOC_SIFNETMASK: {
                    uint32_t mask = ker::net::ntohl(*reinterpret_cast<uint32_t*>(arg + 20));
                    auto* nif = ker::net::netif_get(dev);
                    if (nif != nullptr && nif->ipv4_addr_count > 0) {
                        nif->ipv4_addrs[0].netmask = mask;
                    }
                    return 0;
                }
                case SIOC_GIFHWADDR: {
                    // sa_family = ARPHRD_ETHER (1), then 6 bytes of MAC
                    std::memset(arg + 16, 0, 16);
                    *reinterpret_cast<uint16_t*>(arg + 16) =
                        (dev->link_flags & IFF_LOOPBACK) != 0 ? WOS_ARPHRD_LOOPBACK : WOS_ARPHRD_ETHER;
                    std::memcpy(arg + 18, dev->mac.data(), 6);
                    return 0;
                }
                case SIOC_GIFMTU: {
                    *reinterpret_cast<int32_t*>(arg + 16) = static_cast<int32_t>(dev->mtu);
                    return 0;
                }
                case SIOC_SIFMTU: {
                    int32_t mtu = *reinterpret_cast<int32_t*>(arg + 16);
                    if (mtu <= 0) {
                        return static_cast<uint64_t>(-EINVAL);
                    }
                    dev->mtu = static_cast<uint32_t>(mtu);
                    return 0;
                }
                case SIOC_GIFTXQLEN: {
                    *reinterpret_cast<int32_t*>(arg + 16) = static_cast<int32_t>(dev->tx_queue_len);
                    return 0;
                }
                case SIOC_SIFTXQLEN: {
                    int32_t qlen = *reinterpret_cast<int32_t*>(arg + 16);
                    if (qlen < 0) {
                        return static_cast<uint64_t>(-EINVAL);
                    }
                    dev->tx_queue_len = static_cast<uint32_t>(qlen);
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
                    *reinterpret_cast<int32_t*>(arg + 16) = static_cast<int32_t>(dev->ifindex);
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
            std::string_view ifname(reinterpret_cast<char*>(req), strnlen(reinterpret_cast<char*>(req), 16));
            uint64_t cpu_mask = *reinterpret_cast<uint64_t*>(req + 16);
            auto* dev = ker::net::netdev_find_by_name(ifname);
            if (dev == nullptr) {
                return static_cast<uint64_t>(-ENODEV);
            }
            if (dev->ops == nullptr || dev->ops->set_queue_cpu == nullptr) {
                return static_cast<uint64_t>(-ENOSYS);
            }
            uint32_t pair_idx = 0;
            for (uint64_t tmp = cpu_mask; tmp != 0 && pair_idx < 8; tmp &= tmp - 1, ++pair_idx) {
                uint64_t cpu = static_cast<uint64_t>(__builtin_ctzll(tmp));
                int ret = dev->ops->set_queue_cpu(dev, pair_idx, cpu);
                if (ret != 0) {
                    return static_cast<uint64_t>(ret);
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
            size_t cap = *count_ptr;
            size_t total = ker::net::netdev_count();
            size_t emit = (out != nullptr) ? std::min(cap, total) : 0;
            for (size_t i = 0; i < emit; i++) {
                fill_if_info(out[i], ker::net::netdev_at(i));
            }
            *count_ptr = total;
            return 0;
        }

        case ker::abi::net::ops::NETCTL_ADDR_LIST: {
            auto* out = reinterpret_cast<WosNetAddrInfo*>(a1);
            auto* count_ptr = reinterpret_cast<size_t*>(a2);
            if (count_ptr == nullptr) {
                return static_cast<uint64_t>(-EINVAL);
            }

            size_t cap = *count_ptr;
            size_t total = 0;
            size_t written = 0;
            for (size_t i = 0; i < ker::net::netdev_count(); i++) {
                auto* dev = ker::net::netdev_at(i);
                auto* nif = ker::net::netif_get(dev);
                if (dev == nullptr || nif == nullptr) {
                    continue;
                }
                for (size_t j = 0; j < nif->ipv4_addr_count; j++) {
                    if (out != nullptr && written < cap) {
                        auto& info = out[written];
                        std::memset(&info, 0, sizeof(info));
                        info.ifindex = dev->ifindex;
                        info.family = WOS_AF_INET;
                        info.prefix_len = mask_to_prefix(nif->ipv4_addrs[j].netmask);
                        info.scope = (nif->ipv4_addrs[j].addr >> 24) == 127 ? 254 : 0;  // RT_SCOPE_HOST/global
                        info.flags = WOS_IFA_F_PERMANENT;
                        std::strncpy(info.label, dev->name.data(), sizeof(info.label) - 1);
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
            auto* req = reinterpret_cast<const WosNetAddrReq*>(a1);
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
            uint32_t addr = ker::net::ntohl(addr_be);
            uint32_t mask = prefix_to_mask(req->prefix_len);
            int ret = ker::net::netif_set_ipv4(dev, addr, mask, req->replace != 0);
            return static_cast<uint64_t>(ret);
        }

        case ker::abi::net::ops::NETCTL_ADDR_DEL: {
            auto* req = reinterpret_cast<const WosNetAddrReq*>(a1);
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
            uint32_t addr = ker::net::ntohl(addr_be);
            uint32_t mask = prefix_to_mask(req->prefix_len);
            int ret = ker::net::netif_del_ipv4(dev, addr, mask);
            return static_cast<uint64_t>(ret);
        }

        case ker::abi::net::ops::NETCTL_LINK_SET: {
            auto* req = reinterpret_cast<const WosNetLinkSetReq*>(a1);
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
                int ret = set_netdev_hwaddr(dev, req->hwaddr, req->hwaddr_len);
                if (ret < 0) {
                    return static_cast<uint64_t>(ret);
                }
            }
            if ((req->fields & WOS_NET_LINK_SET_NAME) != 0) {
                int ret = rename_netdev(dev, req->new_name);
                if (ret < 0) {
                    return static_cast<uint64_t>(ret);
                }
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

            uint64_t deadline_us = begin_poll_timeout(task, timeout);

            int num_events = 0;
            for (size_t i = 0; i < nfds; i++) {
                fds[i].revents = 0;

                if (fds[i].fd < 0) {
                    continue;
                }

                auto* file = ker::vfs::vfs_get_file(task, fds[i].fd);
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

                if (fds[i].revents != 0) {
                    num_events++;
                }
            }

            if (num_events > 0 || timeout == 0) {
                clear_poll_timeout(task);
                return static_cast<uint64_t>(num_events);
            }

            if (deadline_us != 0 && ker::mod::time::getUs() >= deadline_us) {
                clear_poll_timeout(task);
                return 0;
            }

            // No events - check for deliverable signals before retrying.
            {
                uint64_t deliverable = task->sigPending & ~task->sigMask;
                if (deliverable != 0) {
                    clear_poll_timeout(task);
                    return static_cast<uint64_t>(-EINTR);
                }
            }

            bool can_block = (nfds > 0);
            if (can_block) {
                for (size_t i = 0; i < nfds; i++) {
                    if (fds[i].fd < 0) {
                        continue;
                    }
                    auto* file = ker::vfs::vfs_get_file(task, fds[i].fd);
                    if (file == nullptr || !register_poll_waiter(file, task->pid)) {
                        can_block = false;
                        break;
                    }
                }
            }

            if (can_block) {
                task->wakeAtUs = deadline_us;
                task->wait_channel = "poll";
                task->deferredTaskSwitch = true;

                // Re-check fds after registration + deferred mark to close
                // the race window where an event fires between the initial
                // poll_check and waiter registration.  With deferredTaskSwitch
                // already set, any concurrent wake_waiters will clear it, so
                // syscall.asm will skip the block and retry the syscall.
                int recheck = 0;
                for (size_t i = 0; i < nfds; i++) {
                    fds[i].revents = 0;
                    if (fds[i].fd < 0) continue;
                    auto* rf = ker::vfs::vfs_get_file(task, fds[i].fd);
                    if (rf == nullptr) continue;
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
                    if (fds[i].revents != 0) recheck++;
                }
                if (recheck > 0) {
                    task->deferredTaskSwitch = false;
                    task->wakeAtUs = 0;
                    task->wait_channel = nullptr;
                    clear_poll_timeout(task);
                    return static_cast<uint64_t>(recheck);
                }

                return static_cast<uint64_t>(-WOS_ERESTARTSYS);
            }

            if (timeout < 0) {
                clear_poll_timeout(task);
            }
            ker::mod::sched::kern_yield();
            return static_cast<uint64_t>(-WOS_ERESTARTSYS);
        }

        default:
            return static_cast<uint64_t>(-ENOSYS);
    }
}

}  // namespace ker::syscall::net
