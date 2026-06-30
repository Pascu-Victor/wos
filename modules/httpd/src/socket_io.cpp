#include "httpd/socket_io.hpp"

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>

#include <array>
#include <cerrno>

#include "httpd/config.hpp"
#include "httpd/time.hpp"

namespace httpd {

auto wait_fd_ready_until(int fd, short events, int64_t deadline_ms, int fallback_timeout_ms) -> int {
    for (;;) {
        int const TIMEOUT_MS = remaining_ms_until(deadline_ms, fallback_timeout_ms);
        if (TIMEOUT_MS <= 0) {
            return 0;
        }

        struct pollfd pfd{
            .fd = fd,
            .events = events,
            .revents = 0,
        };
        int const READY = poll(&pfd, 1, TIMEOUT_MS);
        if (READY < 0 && errno == EINTR) {
            continue;
        }
        if (READY == 0) {
            errno = ETIMEDOUT;
        }
        return READY;
    }
}

auto set_nonblocking_for_timeout(int fd, int& old_flags) -> bool {
    old_flags = fcntl(fd, F_GETFL, 0);
    if (old_flags < 0) {
        return false;
    }
    if ((old_flags & O_NONBLOCK) != 0) {
        return true;
    }
    return fcntl(fd, F_SETFL, old_flags | O_NONBLOCK) == 0;
}

void restore_fd_flags(int fd, int old_flags) {
    if (old_flags >= 0) {
        (void)fcntl(fd, F_SETFL, old_flags);
    }
}

void set_fd_cloexec_best_effort(int fd) {
    int const FLAGS = fcntl(fd, F_GETFD, 0);
    if (FLAGS >= 0) {
        (void)fcntl(fd, F_SETFD, FLAGS | FD_CLOEXEC);
    }
}

namespace {

auto socket_error_from_result(ssize_t result) -> int {
    if (result < -1) {
        return static_cast<int>(-result);
    }
    return errno;
}

auto retryable_socket_error(int err) -> bool {
    return err == EAGAIN || err == EWOULDBLOCK || err == EINTR || err == EINPROGRESS || err == EALREADY;
}

}  // namespace

auto retryable_socket_result(ssize_t result) -> bool { return result < 0 && retryable_socket_error(socket_error_from_result(result)); }

auto send_all_timeout(int fd, const void* data, size_t len, int timeout_ms) -> ssize_t {
    int old_flags = -1;
    if (!set_nonblocking_for_timeout(fd, old_flags)) {
        return -1;
    }

    const auto* ptr = static_cast<const char*>(data);
    size_t remaining = len;
    size_t total_sent = 0;
    int64_t const DEADLINE_MS = deadline_after_ms(timeout_ms);

    while (remaining > 0) {
        if (wait_fd_ready_until(fd, POLLOUT, DEADLINE_MS, timeout_ms) <= 0) {
            restore_fd_flags(fd, old_flags);
            return -1;
        }

        errno = 0;
        ssize_t const SENT = send(fd, ptr, remaining, 0);
        if (SENT < 0) {
            if (retryable_socket_result(SENT)) {
                continue;
            }
            restore_fd_flags(fd, old_flags);
            return -1;
        }
        if (SENT == 0) {
            errno = ETIMEDOUT;
            restore_fd_flags(fd, old_flags);
            return -1;
        }
        ptr += SENT;
        remaining -= SENT;
        total_sent += SENT;
    }

    restore_fd_flags(fd, old_flags);
    return static_cast<ssize_t>(total_sent);
}

auto send_all(int fd, const void* data, size_t len) -> ssize_t { return send_all_timeout(fd, data, len, CLIENT_IO_TIMEOUT_MS); }

void drain_client_input_timeout(int fd, int timeout_ms) {
    int old_flags = -1;
    if (!set_nonblocking_for_timeout(fd, old_flags)) {
        return;
    }

    std::array<char, 512> drain{};
    int64_t const DEADLINE_MS = deadline_after_ms(timeout_ms);
    for (;;) {
        int const READY = wait_fd_ready_until(fd, POLLIN, DEADLINE_MS, timeout_ms);
        if (READY <= 0) {
            break;
        }

        errno = 0;
        ssize_t const RECEIVED = recv(fd, drain.data(), drain.size(), 0);
        if (RECEIVED > 0) {
            continue;
        }
        if (RECEIVED < 0 && retryable_socket_result(RECEIVED)) {
            continue;
        }
        break;
    }

    restore_fd_flags(fd, old_flags);
}

}  // namespace httpd
