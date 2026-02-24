#include <cerrno>
#include <cstring>
#include <net/socket.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <vfs/epoll.hpp>
#include <vfs/vfs.hpp>

namespace ker::vfs {

// ── FileOperations for epoll fds ─────────────────────────────────────────────

static auto epoll_close(File* f) -> int {
    if (f && f->private_data) {
        auto* inst = static_cast<EpollInstance*>(f->private_data);
        delete inst;
        f->private_data = nullptr;
    }
    return 0;
}

static FileOperations epoll_fops = {
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
};

// ── Helper: poll a single fd and return its ready mask ───────────────────────

static auto poll_fd(File* file, uint32_t events) -> uint32_t {
    if (file == nullptr) return 0;

    // Convert EPOLL* flags to POLL* flags (they are numerically identical for
    // the low bits: POLLIN=0x001, POLLOUT=0x004, POLLERR=0x008, POLLHUP=0x010)
    int poll_events = static_cast<int>(events & 0xFFFF);

    if (file->fs_type == FSType::SOCKET) {
        auto* sock = static_cast<ker::net::Socket*>(file->private_data);
        if (sock && sock->proto_ops && sock->proto_ops->poll_check) {
            return static_cast<uint32_t>(sock->proto_ops->poll_check(sock, poll_events));
        }
        // Socket without poll_check — assume ready
        return events & (EPOLLIN | EPOLLOUT);
    }

    if (file->fops && file->fops->vfs_poll_check) {
        return static_cast<uint32_t>(file->fops->vfs_poll_check(file, poll_events));
    }

    // Regular files / devices without poll_check are always ready for I/O
    return events & (EPOLLIN | EPOLLOUT);
}

// ── epoll_create ─────────────────────────────────────────────────────────────

auto epoll_create(int flags) -> int {
    (void)flags;  // EPOLL_CLOEXEC handled below

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;

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
    file->fs_type = FSType::TMPFS;  // reuse TMPFS type — it's just an in-memory object
    file->refcount = 1;
    file->open_flags = 0;
    file->fd_flags = (flags & 02000000) ? FD_CLOEXEC : 0;  // EPOLL_CLOEXEC
    file->vfs_path = nullptr;
    file->dir_fs_count = 0;

    int fd = vfs_alloc_fd(task, file);
    if (fd < 0) {
        delete inst;
        delete file;
        return fd;
    }
    file->fd = fd;
    return fd;
}

// ── epoll_ctl ────────────────────────────────────────────────────────────────

auto epoll_ctl(int epfd, int op, int fd, EpollEvent* event) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;

    auto* epfile = vfs_get_file(task, epfd);
    if (epfile == nullptr) return -EBADF;

    auto* inst = static_cast<EpollInstance*>(epfile->private_data);
    if (inst == nullptr) return -EINVAL;

    // Validate target fd exists (except for DEL, where we allow stale)
    if (op != EPOLL_CTL_DEL) {
        auto* target = vfs_get_file(task, fd);
        if (target == nullptr) return -EBADF;
    }

    switch (op) {
        case EPOLL_CTL_ADD: {
            // Check for duplicate
            for (size_t i = 0; i < EPOLL_MAX_INTEREST; i++) {
                if (inst->interests[i].active && inst->interests[i].fd == fd) {
                    return -EEXIST;
                }
            }
            // Find free slot
            for (size_t i = 0; i < EPOLL_MAX_INTEREST; i++) {
                if (!inst->interests[i].active) {
                    inst->interests[i].fd = fd;
                    inst->interests[i].events = event ? event->events : 0;
                    inst->interests[i].data = event ? event->data.u64 : 0;
                    inst->interests[i].active = true;
                    inst->count++;
                    return 0;
                }
            }
            return -ENOMEM;  // interest list full
        }

        case EPOLL_CTL_MOD: {
            for (size_t i = 0; i < EPOLL_MAX_INTEREST; i++) {
                if (inst->interests[i].active && inst->interests[i].fd == fd) {
                    inst->interests[i].events = event ? event->events : 0;
                    inst->interests[i].data = event ? event->data.u64 : 0;
                    return 0;
                }
            }
            return -ENOENT;
        }

        case EPOLL_CTL_DEL: {
            for (size_t i = 0; i < EPOLL_MAX_INTEREST; i++) {
                if (inst->interests[i].active && inst->interests[i].fd == fd) {
                    inst->interests[i].active = false;
                    inst->count--;
                    return 0;
                }
            }
            return -ENOENT;
        }

        default:
            return -EINVAL;
    }
}

// ── epoll_pwait ──────────────────────────────────────────────────────────────

auto epoll_pwait(int epfd, EpollEvent* events, int maxevents, int timeout_ms) -> int {
    if (maxevents <= 0 || events == nullptr) return -EINVAL;

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;

    auto* epfile = vfs_get_file(task, epfd);
    if (epfile == nullptr) return -EBADF;

    auto* inst = static_cast<EpollInstance*>(epfile->private_data);
    if (inst == nullptr) return -EINVAL;

    // Scan all interest entries and collect ready events
    int ready = 0;
    for (size_t i = 0; i < EPOLL_MAX_INTEREST && ready < maxevents; i++) {
        if (!inst->interests[i].active) continue;

        auto* target = vfs_get_file(task, inst->interests[i].fd);
        if (target == nullptr) {
            // fd was closed — auto-remove from interest list
            inst->interests[i].active = false;
            inst->count--;
            continue;
        }

        uint32_t revents = poll_fd(target, inst->interests[i].events);
        if (revents != 0) {
            events[ready].events = revents;
            events[ready].data.u64 = inst->interests[i].data;
            ready++;

            // EPOLLONESHOT: disable after reporting
            if (inst->interests[i].events & EPOLLONESHOT) {
                inst->interests[i].events = 0;
            }
        }
    }

    if (ready > 0 || timeout_ms == 0) {
        return ready;
    }

    // No events ready and non-zero timeout: return -EAGAIN so the userspace
    // wrapper can spin-retry (same pattern as poll()).
    return -EAGAIN;
}

}  // namespace ker::vfs
