#include "testd.hpp"

static auto parse_int_arg(const char* text, int fallback = -1) -> int {
    if (text == nullptr) {
        return fallback;
    }
    errno = 0;
    char* end = nullptr;
    long const VALUE = std::strtol(text, &end, 10);
    if (end == text || *end != '\0' || errno != 0 || VALUE < INT_MIN || VALUE > INT_MAX) {
        return fallback;
    }
    return static_cast<int>(VALUE);
}

auto run_remote_helper(int argc, char** argv) -> int {
    const char* mode = argv[2];
    int const FD = parse_int_arg(argv[3]);
    if (FD < 0) {
        return 1;
    }
    if (std::strcmp(mode, "pipe-write") == 0) {
        ssize_t const N = write(FD, RH_PIPE_WRITE_MSG.data(), RH_PIPE_WRITE_MSG.size());
        close(FD);
        return (std::cmp_equal(N, RH_PIPE_WRITE_MSG.size())) ? 0 : 1;
    }
    if (std::strcmp(mode, "pipe-read") == 0) {
        std::array<char, 32> buf{};
        ssize_t const N = read(FD, buf.data(), buf.size() - 1);
        close(FD);
        if (N <= 0) {
            return 1;
        }
        std::string_view const GOT{buf.data(), static_cast<size_t>(N)};
        return (GOT == RH_PIPE_READ_EXPECT) ? 0 : 1;
    }
    if (std::strcmp(mode, "pty-ioctl") == 0) {
        struct winsize ws{};
        int const RC = ioctl(FD, TIOCGWINSZ, &ws);
        close(FD);
        return (RC == 0) ? 0 : 1;
    }
    if (std::strcmp(mode, "pty-write") == 0) {
        ssize_t const N = write(FD, RH_PTY_WRITE_MSG.data(), RH_PTY_WRITE_MSG.size());
        close(FD);
        return (std::cmp_equal(N, RH_PTY_WRITE_MSG.size())) ? 0 : 1;
    }
    if (std::strcmp(mode, "sock-write") == 0) {
        ssize_t const N = send(FD, RH_SOCKET_WRITE_MSG.data(), RH_SOCKET_WRITE_MSG.size(), 0);
        close(FD);
        return (std::cmp_equal(N, RH_SOCKET_WRITE_MSG.size())) ? 0 : 1;
    }
    if (std::strcmp(mode, "sock-ctrl") == 0) {
        if (argc < 5) {
            close(FD);
            return 1;
        }
        int const EXPECTED_PORT = parse_int_arg(argv[4]);
        if (EXPECTED_PORT < 0) {
            close(FD);
            return 1;
        }
        struct sockaddr_in peer{};
        socklen_t peer_len = sizeof(peer);
        if (getpeername(FD, reinterpret_cast<struct sockaddr*>(&peer), &peer_len) != 0) {
            close(FD);
            return 1;
        }
        if (peer_len != sizeof(peer) || peer.sin_family != AF_INET || ntohl(peer.sin_addr.s_addr) != INADDR_LOOPBACK ||
            std::cmp_not_equal(ntohs(peer.sin_port), EXPECTED_PORT)) {
            close(FD);
            return 1;
        }

        int rcvbuf = RH_SOCKET_CTRL_RCVBUF;
        if (setsockopt(FD, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf)) != 0) {
            close(FD);
            return 1;
        }

        int got_rcvbuf = 0;
        socklen_t got_rcvbuf_len = sizeof(got_rcvbuf);
        if (getsockopt(FD, SOL_SOCKET, SO_RCVBUF, &got_rcvbuf, &got_rcvbuf_len) != 0) {
            close(FD);
            return 1;
        }
        if (got_rcvbuf_len != sizeof(got_rcvbuf) || got_rcvbuf != RH_SOCKET_CTRL_RCVBUF) {
            close(FD);
            return 1;
        }

        int keepalive = 1;
        if (setsockopt(FD, SOL_SOCKET, SO_KEEPALIVE, &keepalive, sizeof(keepalive)) != 0) {
            close(FD);
            return 1;
        }

        if (shutdown(FD, SHUT_WR) != 0) {
            close(FD);
            return 1;
        }

        usleep(100000);
        close(FD);
        return 0;
    }
    if (std::strcmp(mode, "epoll-add") == 0) {
        if (argc < 5) {
            close(FD);
            return 1;
        }
        int const TARGET_FD = parse_int_arg(argv[4]);
        if (TARGET_FD < 0) {
            close(FD);
            return 1;
        }
        struct epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = TARGET_FD;
        int const RC = epoll_ctl(FD, EPOLL_CTL_ADD, TARGET_FD, &ev);
        close(FD);
        return (RC == 0) ? 0 : 1;
    }
    if (std::strcmp(mode, "poll-wait") == 0) {
        if (argc < 5) {
            close(FD);
            return 1;
        }
        int const READY_FD = parse_int_arg(argv[4]);
        if (READY_FD < 0) {
            close(FD);
            return 1;
        }
        struct pollfd pfd{
            .fd = FD,
            .events = POLLIN,
            .revents = 0,
        };

        struct pollfd preflight = pfd;
        int const PRE_READY = poll(&preflight, 1, 0);
        if (PRE_READY != 0) {
            close(READY_FD);
            close(FD);
            return 1;
        }
        if (!signal_remote_wait_ready(READY_FD)) {
            close(FD);
            return 1;
        }

        int const RC = poll(&pfd, 1, 1000);
        if (RC != 1 || pfd.fd != FD || (pfd.revents & POLLIN) == 0) {
            close(FD);
            return 1;
        }

        char byte = 0;
        ssize_t const NR = read(FD, &byte, sizeof(byte));
        close(FD);
        return (NR == 1 && byte == 'P') ? 0 : 1;
    }
    if (std::strcmp(mode, "poll-preclosed-hup") == 0) {
        struct pollfd pfd{
            .fd = FD,
            .events = POLLIN,
            .revents = 0,
        };

        int const RC = poll(&pfd, 1, 1000);
        if (RC != 1 || pfd.fd != FD || (pfd.revents & POLLHUP) == 0) {
            close(FD);
            return 1;
        }

        char byte = 0;
        ssize_t const NR = read(FD, &byte, sizeof(byte));
        close(FD);
        return (NR == 0) ? 0 : 1;
    }
    if (std::strcmp(mode, "poll-wait-hup") == 0) {
        if (argc < 5) {
            close(FD);
            return 1;
        }
        int const READY_FD = parse_int_arg(argv[4]);
        if (READY_FD < 0) {
            close(FD);
            return 1;
        }
        struct pollfd pfd{
            .fd = FD,
            .events = POLLIN,
            .revents = 0,
        };

        struct pollfd preflight = pfd;
        int const PRE_READY = poll(&preflight, 1, 0);
        if (PRE_READY != 0) {
            close(READY_FD);
            close(FD);
            return 1;
        }
        if (!signal_remote_wait_ready(READY_FD)) {
            close(FD);
            return 1;
        }

        int const RC = poll(&pfd, 1, 1000);
        if (RC != 1 || pfd.fd != FD || (pfd.revents & POLLHUP) == 0) {
            close(FD);
            return 1;
        }

        char byte = 0;
        ssize_t const NR = read(FD, &byte, sizeof(byte));
        close(FD);
        return (NR == 0) ? 0 : 1;
    }
    if (std::strcmp(mode, "poll-drain-hup") == 0) {
        if (argc < 5) {
            close(FD);
            return 1;
        }
        int const READY_FD = parse_int_arg(argv[4]);
        if (READY_FD < 0) {
            close(FD);
            return 1;
        }
        struct pollfd pfd{
            .fd = FD,
            .events = POLLIN,
            .revents = 0,
        };

        struct pollfd preflight = pfd;
        int const PRE_READY = poll(&preflight, 1, 0);
        if (PRE_READY != 0) {
            close(READY_FD);
            close(FD);
            return 1;
        }
        if (!signal_remote_wait_ready(READY_FD)) {
            close(FD);
            return 1;
        }

        int const READY = poll(&pfd, 1, 1000);
        if (READY != 1 || pfd.fd != FD || (pfd.revents & POLLIN) == 0) {
            close(FD);
            return 1;
        }

        char byte = 0;
        ssize_t const NR = read(FD, &byte, sizeof(byte));
        if (NR != 1 || byte != 'D') {
            close(FD);
            return 1;
        }

        pfd.revents = 0;
        int const HUP_READY = poll(&pfd, 1, 1000);
        if (HUP_READY != 1 || pfd.fd != FD || (pfd.revents & POLLHUP) == 0) {
            close(FD);
            return 1;
        }

        char eof_probe = 0;
        ssize_t const EOF_NR = read(FD, &eof_probe, sizeof(eof_probe));
        close(FD);
        return (EOF_NR == 0) ? 0 : 1;
    }
    if (std::strcmp(mode, "epoll-wait") == 0) {
        if (argc < 5) {
            close(FD);
            return 1;
        }
        int const READY_FD = parse_int_arg(argv[4]);
        if (READY_FD < 0) {
            close(FD);
            return 1;
        }
        int const EPFD = epoll_create1(0);
        if (EPFD < 0) {
            close(READY_FD);
            close(FD);
            return 1;
        }

        struct epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = FD;
        if (epoll_ctl(EPFD, EPOLL_CTL_ADD, FD, &ev) != 0) {
            close(READY_FD);
            close(EPFD);
            close(FD);
            return 1;
        }

        struct epoll_event preflight{};
        int const PRE_READY = epoll_wait(EPFD, &preflight, 1, 0);
        if (PRE_READY != 0) {
            close(READY_FD);
            close(EPFD);
            close(FD);
            return 1;
        }
        if (!signal_remote_wait_ready(READY_FD)) {
            close(EPFD);
            close(FD);
            return 1;
        }

        struct epoll_event out{};
        int const RC = epoll_wait(EPFD, &out, 1, 1000);
        if (RC != 1 || out.data.fd != FD || (out.events & EPOLLIN) == 0) {
            close(EPFD);
            close(FD);
            return 1;
        }

        char byte = 0;
        ssize_t const NR = read(FD, &byte, sizeof(byte));
        close(EPFD);
        close(FD);
        return (NR == 1 && byte == 'E') ? 0 : 1;
    }
    if (std::strcmp(mode, "epoll-wait-hup") == 0) {
        if (argc < 5) {
            close(FD);
            return 1;
        }
        int const READY_FD = parse_int_arg(argv[4]);
        if (READY_FD < 0) {
            close(FD);
            return 1;
        }
        int const EPFD = epoll_create1(0);
        if (EPFD < 0) {
            close(READY_FD);
            close(FD);
            return 1;
        }

        struct epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = FD;
        if (epoll_ctl(EPFD, EPOLL_CTL_ADD, FD, &ev) != 0) {
            close(READY_FD);
            close(EPFD);
            close(FD);
            return 1;
        }

        struct epoll_event preflight{};
        int const PRE_READY = epoll_wait(EPFD, &preflight, 1, 0);
        if (PRE_READY != 0) {
            close(READY_FD);
            close(EPFD);
            close(FD);
            return 1;
        }
        if (!signal_remote_wait_ready(READY_FD)) {
            close(EPFD);
            close(FD);
            return 1;
        }

        struct epoll_event out{};
        int const RC = epoll_wait(EPFD, &out, 1, 1000);
        if (RC != 1 || out.data.fd != FD || (out.events & EPOLLHUP) == 0) {
            close(EPFD);
            close(FD);
            return 1;
        }

        char byte = 0;
        ssize_t const NR = read(FD, &byte, sizeof(byte));
        close(EPFD);
        close(FD);
        return (NR == 0) ? 0 : 1;
    }
    if (std::strcmp(mode, "epoll-preclosed-hup") == 0) {
        int const EPFD = epoll_create1(0);
        if (EPFD < 0) {
            close(FD);
            return 1;
        }

        struct epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = FD;
        if (epoll_ctl(EPFD, EPOLL_CTL_ADD, FD, &ev) != 0) {
            close(EPFD);
            close(FD);
            return 1;
        }

        struct epoll_event out{};
        int const RC = epoll_wait(EPFD, &out, 1, 1000);
        if (RC != 1 || out.data.fd != FD || (out.events & EPOLLHUP) == 0) {
            close(EPFD);
            close(FD);
            return 1;
        }

        char byte = 0;
        ssize_t const NR = read(FD, &byte, sizeof(byte));
        close(EPFD);
        close(FD);
        return (NR == 0) ? 0 : 1;
    }
    if (std::strcmp(mode, "epoll-drain-hup") == 0) {
        if (argc < 5) {
            close(FD);
            return 1;
        }
        int const READY_FD = parse_int_arg(argv[4]);
        if (READY_FD < 0) {
            close(FD);
            return 1;
        }
        int const EPFD = epoll_create1(0);
        if (EPFD < 0) {
            close(READY_FD);
            close(FD);
            return 1;
        }

        struct epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = FD;
        if (epoll_ctl(EPFD, EPOLL_CTL_ADD, FD, &ev) != 0) {
            close(READY_FD);
            close(EPFD);
            close(FD);
            return 1;
        }

        struct epoll_event preflight{};
        int const PRE_READY = epoll_wait(EPFD, &preflight, 1, 0);
        if (PRE_READY != 0) {
            close(READY_FD);
            close(EPFD);
            close(FD);
            return 1;
        }
        if (!signal_remote_wait_ready(READY_FD)) {
            close(EPFD);
            close(FD);
            return 1;
        }

        struct epoll_event out{};
        int const READY = epoll_wait(EPFD, &out, 1, 1000);
        if (READY != 1 || out.data.fd != FD || (out.events & EPOLLIN) == 0) {
            close(EPFD);
            close(FD);
            return 1;
        }

        char byte = 0;
        ssize_t const NR = read(FD, &byte, sizeof(byte));
        if (NR != 1 || byte != 'G') {
            close(EPFD);
            close(FD);
            return 1;
        }

        struct epoll_event hup_out{};
        int const HUP_READY = epoll_wait(EPFD, &hup_out, 1, 1000);
        if (HUP_READY != 1 || hup_out.data.fd != FD || (hup_out.events & EPOLLHUP) == 0) {
            close(EPFD);
            close(FD);
            return 1;
        }

        char eof_probe = 0;
        ssize_t const EOF_NR = read(FD, &eof_probe, sizeof(eof_probe));
        close(EPFD);
        close(FD);
        return (EOF_NR == 0) ? 0 : 1;
    }
    return RH_EXIT_UNKNOWN_MODE;
}
