#include <cerrno>
#include <cstdint>
#include <cstring>
#include <net/backlog.hpp>
#include <net/netpoll.hpp>
#include <net/socket.hpp>
#include <net/wki/remote_ipc.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <vfs/epoll.hpp>
#include <vfs/vfs.hpp>

#include "platform/ktime/ktime.hpp"
#include "vfs/file.hpp"
#include "vfs/file_operations.hpp"

namespace ker::vfs {

namespace {
constexpr uint64_t USEC_PER_MSEC = 1000;
constexpr int EPOLL_CLOEXEC_FLAG = 02000000;

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

auto register_poll_waiter(File* file, uint64_t pid) -> bool {
    if (file == nullptr) {
        return false;
    }
    if (file->fs_type == FSType::SOCKET) {
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

auto poll_wait_kind_for_file(File* file) -> ker::mod::sched::task::WaitChannelKind {
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

void drain_network_rx_work() {
    ker::net::napi_poll_all_pending();
    ker::net::backlog_drain_all_pending_inline();
}

// -- FileOperations for epoll fds ---------------------------------------------

auto epoll_close(File* f) -> int {
    if ((f != nullptr) && (f->private_data != nullptr)) {
        auto* inst = static_cast<EpollInstance*>(f->private_data);
        delete inst;
        f->private_data = nullptr;
    }
    return 0;
}

FileOperations epoll_fops = {
    .vfs_open = nullptr,
    .vfs_close = epoll_close,
    .vfs_read = nullptr,
    .vfs_write = nullptr,
    .vfs_lseek = nullptr,
    .vfs_isatty = nullptr,
    .vfs_readdir = nullptr,
    .vfs_readlink = nullptr,
    .vfs_truncate = nullptr,
    .vfs_poll_check = nullptr,
    .vfs_poll_register_waiter = nullptr,
    .vfs_ioctl = nullptr,
};

// -- Helper: poll a single fd and return its ready mask -----------------------

auto poll_fd(File* file, uint32_t events) -> uint32_t {
    if (file == nullptr) {
        return 0;
    }

    // Convert EPOLL* flags to POLL* flags (they are numerically identical for
    // the low bits: POLLIN=0x001, POLLOUT=0x004, POLLERR=0x008, POLLHUP=0x010)
    const int POLL_EVENTS = static_cast<int>(events & 0xFFFF);

    if (file->fs_type == FSType::SOCKET) {
        auto* sock = static_cast<ker::net::Socket*>(file->private_data);
        if ((sock != nullptr) && (sock->proto_ops != nullptr) && (sock->proto_ops->poll_check != nullptr)) {
            return static_cast<uint32_t>(sock->proto_ops->poll_check(sock, POLL_EVENTS));
        }
        // Socket without poll_check - assume ready
        return events & (EPOLLIN | EPOLLOUT);
    }

    if ((file->fops != nullptr) && (file->fops->vfs_poll_check != nullptr)) {
        return static_cast<uint32_t>(file->fops->vfs_poll_check(file, POLL_EVENTS));
    }

    // Regular files / devices without poll_check are always ready for I/O
    return events & (EPOLLIN | EPOLLOUT);
}
}  // namespace

// -- epoll_create -------------------------------------------------------------

auto epoll_create(int flags) -> int {
    (void)flags;  // EPOLL_CLOEXEC handled below

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }

    auto* inst = new EpollInstance{};
    inst->count = 0;
    for (auto& e : inst->interests) {
        e.active = false;
    }

    // Allocate a File for the epoll instance
    auto* file = new File{};
    file->fd = -1;
    file->private_data = inst;
    file->fops = &epoll_fops;
    file->pos = 0;
    file->is_directory = false;
    file->fs_type = FSType::TMPFS;  // reuse TMPFS type - it's just an in-memory object
    file->refcount = 1;
    file->open_flags = 0;
    file->fd_flags = 0;  // CLOEXEC is per-fd in task bitmap
    file->vfs_path = nullptr;
    file->dir_fs_count = 0;

    int const FD = vfs_alloc_fd(task, file);
    if (FD < 0) {
        delete inst;
        delete file;
        return FD;
    }
    file->fd = FD;
    if ((flags & EPOLL_CLOEXEC_FLAG) != 0) {
        task->set_fd_cloexec(static_cast<unsigned>(FD));
    }
    return FD;
}

// -- epoll_ctl ----------------------------------------------------------------

auto epoll_ctl(int epfd, int op, int fd, EpollEvent* event) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }

    auto* epfile = vfs_get_file_retain(task, epfd);
    if (epfile == nullptr) {
        return -EBADF;
    }

    auto* inst = static_cast<EpollInstance*>(epfile->private_data);
    if (inst == nullptr) {
        vfs_put_file(epfile);
        return -EINVAL;
    }

    // IPC_EPOLL proxy fd: forward control op to home node.
    {
        uint32_t const EV = (event != nullptr) ? event->events : 0;
        uint64_t const USER_DATA = (event != nullptr) ? event->data.u64 : 0;
        int const REMOTE_RC = ker::net::wki::wki_ipc_epoll_ctl_forward(epfile, op, fd, EV, USER_DATA);
        if (REMOTE_RC != -EOPNOTSUPP) {
            vfs_put_file(epfile);
            return REMOTE_RC;
        }
    }

    // Validate target fd exists (except for DEL, where we allow stale)
    if (op != EPOLL_CTL_DEL) {
        auto* target = vfs_get_file_retain(task, fd);
        if (target == nullptr) {
            vfs_put_file(epfile);
            return -EBADF;
        }
        vfs_put_file(target);
    }

    switch (op) {
        case EPOLL_CTL_ADD: {
            // Check for duplicate
            for (auto& interest : inst->interests) {
                if (interest.active && interest.fd == fd) {
                    vfs_put_file(epfile);
                    return -EEXIST;
                }
            }
            // Find free slot
            for (auto& interest : inst->interests) {
                if (!interest.active) {
                    interest.fd = fd;
                    interest.events = (event != nullptr) ? event->events : 0;
                    interest.data = (event != nullptr) ? event->data.u64 : 0;
                    interest.active = true;
                    inst->count++;
                    vfs_put_file(epfile);
                    return 0;
                }
            }
            vfs_put_file(epfile);
            return -ENOMEM;  // interest list full
        }

        case EPOLL_CTL_MOD: {
            for (auto& interest : inst->interests) {
                if (interest.active && interest.fd == fd) {
                    interest.events = (event != nullptr) ? event->events : 0;
                    interest.data = (event != nullptr) ? event->data.u64 : 0;
                    vfs_put_file(epfile);
                    return 0;
                }
            }
            vfs_put_file(epfile);
            return -ENOENT;
        }

        case EPOLL_CTL_DEL: {
            for (auto& interest : inst->interests) {
                if (interest.active && interest.fd == fd) {
                    interest.active = false;
                    inst->count--;
                    vfs_put_file(epfile);
                    return 0;
                }
            }
            vfs_put_file(epfile);
            return -ENOENT;
        }

        default:
            vfs_put_file(epfile);
            return -EINVAL;
    }
}

// -- epoll_pwait --------------------------------------------------------------

auto epoll_pwait(int epfd, EpollEvent* events, int maxevents, int timeout_ms) -> int {
    if (maxevents <= 0 || events == nullptr) {
        return -EINVAL;
    }

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }

    auto* epfile = vfs_get_file_retain(task, epfd);
    if (epfile == nullptr) {
        return -EBADF;
    }

    auto* inst = static_cast<EpollInstance*>(epfile->private_data);
    if (inst == nullptr) {
        vfs_put_file(epfile);
        return -EINVAL;
    }

    for (;;) {
        uint64_t const DEADLINE_US = begin_poll_timeout(task, timeout_ms);

        drain_network_rx_work();

        // Scan all interest entries and collect ready events
        int ready = 0;
        for (auto& interest : inst->interests) {
            if (ready >= maxevents) {
                break;
            }
            if (!interest.active) {
                continue;
            }

            auto* target = vfs_get_file_retain(task, interest.fd);
            if (target == nullptr) {
                // fd was closed - auto-remove from interest list
                interest.active = false;
                inst->count--;
                continue;
            }

            uint32_t const REVENTS = poll_fd(target, interest.events);
            vfs_put_file(target);
            if (REVENTS != 0) {
                events[ready].events = REVENTS;
                events[ready].data.u64 = interest.data;
                ready++;

                // EPOLLONESHOT: disable after reporting
                if ((interest.events & EPOLLONESHOT) != 0U) {
                    interest.events = 0;
                }
            }
        }

        if (ready > 0 || timeout_ms == 0) {
            clear_poll_timeout(task);
            vfs_put_file(epfile);
            return ready;
        }

        if (DEADLINE_US != 0 && ker::mod::time::get_us() >= DEADLINE_US) {
            clear_poll_timeout(task);
            vfs_put_file(epfile);
            return 0;
        }

        uint64_t const DELIVERABLE = task->sig_pending & ~task->sig_mask;
        if (DELIVERABLE != 0) {
            clear_poll_timeout(task);
            vfs_put_file(epfile);
            return -EINTR;
        }

        bool can_block = (inst->count > 0);
        auto poll_wait_kind = ker::mod::sched::task::WaitChannelKind::GENERIC;
        if (can_block) {
            for (auto& interest : inst->interests) {
                if (!interest.active) {
                    continue;
                }
                auto* f = vfs_get_file_retain(task, interest.fd);
                bool const OK = (f != nullptr) && register_poll_waiter(f, task->pid);
                if (OK) {
                    poll_wait_kind = merge_poll_wait_kind(poll_wait_kind, poll_wait_kind_for_file(f));
                }
                if (f != nullptr) {
                    vfs_put_file(f);
                }
                if (!OK) {
                    can_block = false;
                    break;
                }
            }
        }

        if (can_block) {
            drain_network_rx_work();

            // Re-check interests after waiter registration to close the race
            // window where an event fires between the initial scan and sleep.
            int recheck = 0;
            for (auto& interest : inst->interests) {
                if (recheck >= maxevents) {
                    break;
                }
                if (!interest.active) {
                    continue;
                }
                auto* target = vfs_get_file_retain(task, interest.fd);
                if (target == nullptr) {
                    continue;
                }
                uint32_t const REVENTS = poll_fd(target, interest.events);
                vfs_put_file(target);
                if (REVENTS != 0) {
                    events[recheck].events = REVENTS;
                    events[recheck].data.u64 = interest.data;
                    recheck++;
                    if ((interest.events & EPOLLONESHOT) != 0U) {
                        interest.events = 0;
                    }
                }
            }
            if (recheck > 0) {
                clear_poll_timeout(task);
                vfs_put_file(epfile);
                return recheck;
            }

            ker::mod::sched::preemptible_syscall_park("epoll_wait", poll_wait_kind, DEADLINE_US);
        } else {
            ker::mod::sched::kern_yield();
        }
    }
}

auto vfs_is_epoll_file(const File* f) -> bool { return f != nullptr && f->fops == &epoll_fops; }

}  // namespace ker::vfs
