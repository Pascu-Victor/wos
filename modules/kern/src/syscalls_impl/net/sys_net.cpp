#include "sys_net.hpp"

#include <abi/callnums/net.h>

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
}  // namespace

uint64_t sys_net(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4, uint64_t a5) {
    auto net_op = static_cast<ker::abi::net::ops>(op);

    switch (net_op) {
        case ker::abi::net::ops::socket: {
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

        case ker::abi::net::ops::bind: {
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

        case ker::abi::net::ops::listen: {
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

        case ker::abi::net::ops::accept: {
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

        case ker::abi::net::ops::connect: {
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

        case ker::abi::net::ops::send: {
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

        case ker::abi::net::ops::recv: {
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

        case ker::abi::net::ops::close: {
            // a1=fd - use VFS close which calls socket_fops_close
            int result = ker::vfs::vfs_close(static_cast<int>(a1));
            return static_cast<uint64_t>(result);
        }

        case ker::abi::net::ops::sendto: {
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

        case ker::abi::net::ops::recvfrom: {
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

        case ker::abi::net::ops::setsockopt: {
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

        case ker::abi::net::ops::getsockopt: {
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

        case ker::abi::net::ops::shutdown: {
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

        case ker::abi::net::ops::getpeername: {
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

        case ker::abi::net::ops::getsockname: {
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

        case ker::abi::net::ops::ioctl_net: {
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
            constexpr uint32_t SIOC_GIFHWADDR = 0x8927;
            constexpr uint32_t SIOC_GIFINDEX = 0x8933;
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
                    // Find device for the route: prefer non-loopback UP device
                    // that has an IP on the same subnet as the gateway
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
                    }
                    // Fallback: first non-loopback UP device
                    if (rdev == nullptr) {
                        for (size_t i = 0; i < ker::net::netdev_count(); i++) {
                            auto* d = ker::net::netdev_at(i);
                            if (d != nullptr && d->state == 1 && std::strcmp(d->name.data(), "lo") != 0) {
                                rdev = d;
                                break;
                            }
                        }
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
                    int16_t flags = 0;
                    if (dev->state != 0) {
                        flags |= 0x0001;  // IFF_UP
                    }
                    flags |= 0x0040;  // IFF_RUNNING
                    *reinterpret_cast<int16_t*>(arg + 16) = flags;
                    return 0;
                }
                case SIOC_SIFFLAGS: {
                    int16_t flags = *reinterpret_cast<int16_t*>(arg + 16);
                    if ((flags & 0x0001) != 0) {  // IFF_UP
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
                    *reinterpret_cast<uint16_t*>(arg + 16) = 1;  // ARPHRD_ETHER
                    std::memcpy(arg + 18, dev->mac.data(), 6);
                    return 0;
                }
                case SIOC_GIFINDEX: {
                    *reinterpret_cast<int32_t*>(arg + 16) = static_cast<int32_t>(dev->ifindex);
                    return 0;
                }
                default:
                    return static_cast<uint64_t>(-ENOSYS);
            }
        }

        case ker::abi::net::ops::poll: {
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
                } else {
                    // Non-socket fds (regular files, devices) are always ready
                    fds[i].revents = static_cast<int16_t>(fds[i].events & (0x0001 | 0x0004));  // POLLIN|POLLOUT
                }

                if (fds[i].revents != 0) {
                    num_events++;
                }
            }

            if (num_events > 0 || timeout == 0) {
                return static_cast<uint64_t>(num_events);
            }

            // No events and timeout != 0: block via deferred task switch
            return static_cast<uint64_t>(-EAGAIN);
        }

        default:
            return static_cast<uint64_t>(-ENOSYS);
    }
}

}  // namespace ker::syscall::net
