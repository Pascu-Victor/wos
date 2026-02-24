#pragma once

#include <cstdint>
#include <vfs/file.hpp>

namespace ker::vfs {

// Matches Linux/mlibc EPOLL_CTL_* constants
constexpr int EPOLL_CTL_ADD = 1;
constexpr int EPOLL_CTL_DEL = 2;
constexpr int EPOLL_CTL_MOD = 3;

// Key EPOLL event flags (match Linux/mlibc definitions)
constexpr uint32_t EPOLLIN = 0x001;
constexpr uint32_t EPOLLPRI = 0x002;
constexpr uint32_t EPOLLOUT = 0x004;
constexpr uint32_t EPOLLERR = 0x008;
constexpr uint32_t EPOLLHUP = 0x010;
constexpr uint32_t EPOLLRDHUP = 0x2000;
constexpr uint32_t EPOLLONESHOT = (1U << 30);
constexpr uint32_t EPOLLET = (1U << 31);

// User-kernel ABI struct — must match mlibc's struct epoll_event layout
struct __attribute__((__packed__)) EpollEvent {
    uint32_t events;
    union {
        void* ptr;
        int fd;
        uint32_t u32;
        uint64_t u64;
    } data;
};

// Single entry in the interest list
struct EpollInterest {
    int fd;           // watched file descriptor
    uint32_t events;  // requested event mask
    uint64_t data;    // user data (epoll_data_t.u64)
    bool active;      // slot in use?
};

// Maximum number of watched fds per epoll instance
constexpr size_t EPOLL_MAX_INTEREST = 64;

// Per-epoll-instance state, stored as File::private_data
struct EpollInstance {
    EpollInterest interests[EPOLL_MAX_INTEREST];
    size_t count;  // number of active entries
};

// Kernel epoll API — called from sys_vfs dispatcher
auto epoll_create(int flags) -> int;  // returns fd, or negative errno
auto epoll_ctl(int epfd, int op, int fd, EpollEvent* event) -> int;
auto epoll_pwait(int epfd, EpollEvent* events, int maxevents,
                 int timeout_ms) -> int;  // returns # ready, or negative errno

}  // namespace ker::vfs
