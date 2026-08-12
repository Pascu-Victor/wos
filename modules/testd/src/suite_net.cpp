#include "testd.hpp"

// ---------------------------------------------------------------------------
// B3: TCP loopback socket tests
// ---------------------------------------------------------------------------

constexpr uint16_t TESTD_TCP_PORT = 19876;
constexpr uint16_t TESTD_TCP_DUAL_STACK_PORT = 19878;
constexpr uint16_t TESTD_TCP_IPV6_PORT = 19879;
constexpr uint16_t TESTD_UDP_IPV6_PORT = 19880;
constexpr uint16_t TESTD_TCP_V6ONLY_PORT = 19881;
constexpr uint16_t TESTD_UDP_DUAL_STACK_PORT = 19882;

// Server accepts one connection, echoes data back, closes.
// Runs in a forked child.
void tcp_echo_server(int ready_fd) {
    int const SRV = socket(AF_INET, SOCK_STREAM, 0);
    if (SRV < 0) {
        _exit(1);
    }

    int one = 1;
    setsockopt(SRV, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(TESTD_TCP_PORT);

    if (bind(SRV, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
        close(SRV);
        _exit(2);
    }
    if (listen(SRV, 1) != 0) {
        close(SRV);
        _exit(3);
    }

    // Signal to parent that we are listening
    if (ready_fd >= 0) {
        char byte = 1;
        write(ready_fd, &byte, 1);
        close(ready_fd);
    }

    int const CLI = accept_timeout(SRV, nullptr, nullptr, REMOTE_IPC_TIMEOUT_MS);
    if (CLI < 0) {
        close(SRV);
        _exit(4);
    }

    // Echo loop
    std::array<char, 512> buf{};
    ssize_t n = 0;
    int recv_errno = 0;
    while ((n = recv_once_timeout(CLI, buf.data(), buf.size(), 0, REMOTE_IPC_TIMEOUT_MS)) > 0) {
        ssize_t const SENT = send_all_timeout(CLI, buf.data(), static_cast<size_t>(n), 0, REMOTE_IPC_TIMEOUT_MS);
        if (SENT != n) {
            n = -1;
            break;
        }
    }
    if (n < 0) {
        recv_errno = errno;
        testd_logf("[TESTD] INFO: tcp_echo_server recv ended rc=%zd errno=%d", n, recv_errno);
    }
    close(CLI);
    close(SRV);
    _exit(0);
}

auto is_v4_mapped_loopback(const sockaddr_in6& address) -> bool {
    for (size_t i = 0; i < 10; ++i) {
        if (address.sin6_addr.s6_addr[i] != 0) {
            return false;
        }
    }
    return address.sin6_addr.s6_addr[10] == 0xFF && address.sin6_addr.s6_addr[11] == 0xFF && address.sin6_addr.s6_addr[12] == 127 &&
           address.sin6_addr.s6_addr[13] == 0 && address.sin6_addr.s6_addr[14] == 0 && address.sin6_addr.s6_addr[15] == 1;
}

void tcp_dual_stack_server(int ready_fd) {
    int const SRV = socket(AF_INET6, SOCK_STREAM, 0);
    if (SRV < 0) {
        _exit(10);
    }
    int zero = 0;
    int one = 1;
    if (setsockopt(SRV, IPPROTO_IPV6, IPV6_V6ONLY, &zero, sizeof(zero)) != 0 ||
        setsockopt(SRV, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0) {
        close(SRV);
        _exit(11);
    }

    sockaddr_in6 local{};
    local.sin6_family = AF_INET6;
    local.sin6_addr = in6addr_any;
    local.sin6_port = htons(TESTD_TCP_DUAL_STACK_PORT);
    if (bind(SRV, reinterpret_cast<sockaddr*>(&local), sizeof(local)) != 0 || listen(SRV, 1) != 0) {
        close(SRV);
        _exit(12);
    }
    char ready = 1;
    if (write(ready_fd, &ready, sizeof(ready)) != sizeof(ready)) {
        close(ready_fd);
        close(SRV);
        _exit(13);
    }
    close(ready_fd);

    sockaddr_in6 accepted_peer{};
    socklen_t accepted_len = sizeof(accepted_peer);
    int const CLI = accept_timeout(SRV, reinterpret_cast<sockaddr*>(&accepted_peer), &accepted_len, REMOTE_IPC_TIMEOUT_MS);
    if (CLI < 0 || accepted_len != sizeof(accepted_peer) || accepted_peer.sin6_family != AF_INET6 ||
        !is_v4_mapped_loopback(accepted_peer)) {
        close(CLI);
        close(SRV);
        _exit(14);
    }

    sockaddr_in6 named_peer{};
    socklen_t named_len = sizeof(named_peer);
    if (getpeername(CLI, reinterpret_cast<sockaddr*>(&named_peer), &named_len) != 0 || named_len != sizeof(named_peer) ||
        named_peer.sin6_family != AF_INET6 || !is_v4_mapped_loopback(named_peer) || named_peer.sin6_port != accepted_peer.sin6_port) {
        close(CLI);
        close(SRV);
        _exit(15);
    }

    char payload = 0;
    if (recv_once_timeout(CLI, &payload, sizeof(payload), 0, REMOTE_IPC_TIMEOUT_MS) != sizeof(payload) || payload != '6' ||
        send_all_timeout(CLI, &payload, sizeof(payload), 0, REMOTE_IPC_TIMEOUT_MS) != sizeof(payload)) {
        close(CLI);
        close(SRV);
        _exit(16);
    }
    close(CLI);
    close(SRV);
    _exit(0);
}

auto ipv6_loopback_endpoint(uint16_t port) -> sockaddr_in6 {
    sockaddr_in6 endpoint{};
    endpoint.sin6_family = AF_INET6;
    endpoint.sin6_port = htons(port);
    endpoint.sin6_addr.s6_addr[15] = 1;
    return endpoint;
}

auto is_ipv6_loopback_endpoint(const sockaddr_in6& endpoint) -> bool {
    if (endpoint.sin6_family != AF_INET6 || endpoint.sin6_port == 0 || endpoint.sin6_scope_id != 0 || endpoint.sin6_addr.s6_addr[15] != 1) {
        return false;
    }
    for (size_t i = 0; i < 15; ++i) {
        if (endpoint.sin6_addr.s6_addr[i] != 0) {
            return false;
        }
    }
    return true;
}

auto same_ipv6_endpoint(const sockaddr_in6& left, const sockaddr_in6& right) -> bool {
    return left.sin6_family == AF_INET6 && right.sin6_family == AF_INET6 && left.sin6_port == right.sin6_port &&
           left.sin6_scope_id == right.sin6_scope_id && std::memcmp(&left.sin6_addr, &right.sin6_addr, sizeof(left.sin6_addr)) == 0;
}

auto ipv6_socket_name(int fd, bool peer, sockaddr_in6& address) -> bool {
    address = {};
    socklen_t length = sizeof(address);
    int const RC = peer ? getpeername(fd, reinterpret_cast<sockaddr*>(&address), &length)
                        : getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length);
    return RC == 0 && length == sizeof(address) && address.sin6_family == AF_INET6;
}

void tcp_ipv6_loopback_server(int ready_fd) {
    int const SRV = socket(AF_INET6, SOCK_STREAM, 0);
    int const ONE = 1;
    sockaddr_in6 const BIND_ADDRESS = ipv6_loopback_endpoint(TESTD_TCP_IPV6_PORT);
    if (SRV < 0 || setsockopt(SRV, IPPROTO_IPV6, IPV6_V6ONLY, &ONE, sizeof(ONE)) != 0 ||
        setsockopt(SRV, SOL_SOCKET, SO_REUSEADDR, &ONE, sizeof(ONE)) != 0 ||
        bind(SRV, reinterpret_cast<const sockaddr*>(&BIND_ADDRESS), sizeof(BIND_ADDRESS)) != 0 || listen(SRV, 1) != 0) {
        close(SRV);
        _exit(20);
    }
    sockaddr_in6 listening_address{};
    if (!ipv6_socket_name(SRV, false, listening_address) || !same_ipv6_endpoint(listening_address, BIND_ADDRESS)) {
        close(SRV);
        _exit(21);
    }
    char const READY = 1;
    if (write(ready_fd, &READY, sizeof(READY)) != sizeof(READY)) {
        close(ready_fd);
        close(SRV);
        _exit(22);
    }
    close(ready_fd);

    sockaddr_in6 accepted_peer{};
    socklen_t accepted_length = sizeof(accepted_peer);
    int const CLI = accept_timeout(SRV, reinterpret_cast<sockaddr*>(&accepted_peer), &accepted_length, REMOTE_IPC_TIMEOUT_MS);
    sockaddr_in6 named_peer{};
    sockaddr_in6 named_local{};
    if (CLI < 0 || accepted_length != sizeof(accepted_peer) || !is_ipv6_loopback_endpoint(accepted_peer) ||
        !ipv6_socket_name(CLI, true, named_peer) || !ipv6_socket_name(CLI, false, named_local) ||
        !same_ipv6_endpoint(named_peer, accepted_peer) || !same_ipv6_endpoint(named_local, BIND_ADDRESS)) {
        close(CLI);
        close(SRV);
        _exit(23);
    }

    std::array<char, 16384> payload{};
    if (recv_expected_bytes_timeout(CLI, payload.data(), payload.size(), REMOTE_IPC_TIMEOUT_MS) != static_cast<ssize_t>(payload.size()) ||
        send_all_timeout(CLI, payload.data(), payload.size(), 0, REMOTE_IPC_TIMEOUT_MS) != static_cast<ssize_t>(payload.size())) {
        close(CLI);
        close(SRV);
        _exit(24);
    }
    close(CLI);
    close(SRV);
    _exit(0);
}

TESTD_RUN(test_tcp_loopback) {
    constexpr int CHILD_EXIT_CODE = 99;
    // Use a pipe as a ready-signal from server to client.
    std::array<int, 2> ready_pipe = {-1, -1};
    if (pipe(ready_pipe.data()) != 0) {
        fail("tcp_ready_pipe", "pipe failed");
        return;
    }

    pid_t const SRV_PID = fork();
    if (SRV_PID < 0) {
        close(ready_pipe[0]);
        close(ready_pipe[1]);
        fail("tcp_fork_server", "fork failed");
        return;
    }

    if (SRV_PID == 0) {
        close(ready_pipe[0]);
        tcp_echo_server(ready_pipe[1]);  // doesn't return
        _exit(CHILD_EXIT_CODE);
    }

    close(ready_pipe[1]);

    // Block until server signals it is listening
    char byte = 0;
    ssize_t const READY_NR = read_expected_bytes_timeout(ready_pipe[0], &byte, 1, REMOTE_IPC_TIMEOUT_MS);
    close(ready_pipe[0]);
    if (READY_NR != 1 || byte != 1) {
        int status = 0;
        (void)waitpid_timeout(SRV_PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("tcp_ready_signal", "server did not signal listen readiness");
        return;
    }

    // Connect as client
    int const CLI = socket(AF_INET, SOCK_STREAM, 0);
    if (CLI < 0) {
        int status = 0;
        (void)waitpid_timeout(SRV_PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("tcp_client_socket", "socket failed");
        return;
    }

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(TESTD_TCP_PORT);

    if (connect_timeout(CLI, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr), REMOTE_IPC_TIMEOUT_MS) != 0) {
        close(CLI);
        int status = 0;
        (void)waitpid_timeout(SRV_PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("tcp_connect", "connect failed");
        return;
    }
    TESTD_PASS("tcp_connect");

    // Send 1 KB of data
    constexpr size_t DATA_SIZE = 1024;
    std::array<char, DATA_SIZE> send_buf{};
    constexpr uint8_t PATTERN_MASK = 0xFF;
    for (size_t i = 0; i < DATA_SIZE; ++i) {
        send_buf[i] = static_cast<char>(i & PATTERN_MASK);
    }

    ssize_t const TOTAL_SENT = send_all_timeout(CLI, send_buf.data(), DATA_SIZE, 0, REMOTE_IPC_TIMEOUT_MS);

    if (std::cmp_not_equal(TOTAL_SENT, DATA_SIZE)) {
        close(CLI);
        int status = 0;
        (void)waitpid_timeout(SRV_PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("tcp_send", "short send");
        return;
    }
    TESTD_PASS("tcp_send");

    // Receive echoed data
    std::array<char, DATA_SIZE> recv_buf{};
    shutdown(CLI, SHUT_WR);  // signal EOF to server
    ssize_t const TOTAL_RECV = recv_expected_bytes_timeout(CLI, recv_buf.data(), DATA_SIZE, REMOTE_IPC_TIMEOUT_MS);
    int const LAST_RECV_ERRNO = errno;
    close(CLI);

    int srv_status = 0;
    bool const SERVER_EXITED = waitpid_timeout(SRV_PID, &srv_status, REMOTE_IPC_TIMEOUT_MS);

    if (!SERVER_EXITED) {
        testd_logf("[TESTD] INFO: tcp_server_exit timed out status=%d errno=%d", srv_status, errno);
        fail("tcp_server_exit", "server did not exit after loopback exchange");
        return;
    }
    if (std::cmp_not_equal(TOTAL_RECV, DATA_SIZE)) {
        testd_logf("[TESTD] INFO: tcp_recv short total_recv=%zd expected=%d errno=%d srv_status=%d", TOTAL_RECV, DATA_SIZE, LAST_RECV_ERRNO,
                   srv_status);
        fail("tcp_recv", "short recv");
        return;
    }
    if (std::string_view(send_buf.data(), send_buf.size()) != std::string_view(recv_buf.data(), recv_buf.size())) {
        fail("tcp_echo_verify", "data mismatch");
        return;
    }
    TESTD_PASS("tcp_loopback_echo");

    std::array<int, 2> dual_ready_pipe = {-1, -1};
    if (pipe(dual_ready_pipe.data()) != 0) {
        fail("tcp_dual_stack_pipe", "pipe failed");
        return;
    }
    pid_t const DUAL_PID = fork();
    if (DUAL_PID < 0) {
        close(dual_ready_pipe[0]);
        close(dual_ready_pipe[1]);
        fail("tcp_dual_stack_fork", "fork failed");
        return;
    }
    if (DUAL_PID == 0) {
        close(dual_ready_pipe[0]);
        tcp_dual_stack_server(dual_ready_pipe[1]);
        _exit(CHILD_EXIT_CODE);
    }
    close(dual_ready_pipe[1]);

    char dual_ready = 0;
    ssize_t const DUAL_READY_NR = read_expected_bytes_timeout(dual_ready_pipe[0], &dual_ready, 1, REMOTE_IPC_TIMEOUT_MS);
    close(dual_ready_pipe[0]);
    if (DUAL_READY_NR != 1 || dual_ready != 1) {
        int status = 0;
        static_cast<void>(waitpid_timeout(DUAL_PID, &status, REMOTE_IPC_TIMEOUT_MS));
        fail("tcp_dual_stack_ready", "server did not signal listen readiness");
        return;
    }

    int const DUAL_CLIENT = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in dual_destination{};
    dual_destination.sin_family = AF_INET;
    dual_destination.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    dual_destination.sin_port = htons(TESTD_TCP_DUAL_STACK_PORT);
    if (DUAL_CLIENT < 0 || connect_timeout(DUAL_CLIENT, reinterpret_cast<sockaddr*>(&dual_destination), sizeof(dual_destination),
                                           REMOTE_IPC_TIMEOUT_MS) != 0) {
        close(DUAL_CLIENT);
        int status = 0;
        static_cast<void>(waitpid_timeout(DUAL_PID, &status, REMOTE_IPC_TIMEOUT_MS));
        fail("tcp_dual_stack_connect", "IPv4 client could not connect to AF_INET6 listener");
        return;
    }

    char dual_payload = '6';
    char dual_echo = 0;
    ssize_t const DUAL_SENT = send_all_timeout(DUAL_CLIENT, &dual_payload, 1, 0, REMOTE_IPC_TIMEOUT_MS);
    ssize_t const DUAL_RECEIVED = recv_once_timeout(DUAL_CLIENT, &dual_echo, 1, 0, REMOTE_IPC_TIMEOUT_MS);
    close(DUAL_CLIENT);
    int dual_status = 0;
    bool const DUAL_EXITED = waitpid_timeout(DUAL_PID, &dual_status, REMOTE_IPC_TIMEOUT_MS);
    if (DUAL_SENT != 1 || DUAL_RECEIVED != 1 || dual_echo != dual_payload || !DUAL_EXITED || !WIFEXITED(dual_status) ||
        WEXITSTATUS(dual_status) != 0) {
        testd_logf("[TESTD] INFO: tcp dual-stack sent=%zd recv=%zd byte=%d status=%d", DUAL_SENT, DUAL_RECEIVED,
                   static_cast<int>(dual_echo), dual_status);
        fail("tcp_dual_stack_tuple", "mapped established lookup, accept address, or getpeername failed");
        return;
    }
    TESTD_PASS("tcp_dual_stack_ipv4_client");
}
TESTD_RUN_END(test_tcp_loopback)

TESTD_RUN(test_ipv6_loopback) {
    int const V6ONLY_TCP = socket(AF_INET6, SOCK_STREAM, 0);
    int const IPV4_TCP = socket(AF_INET, SOCK_STREAM, 0);
    int const ONE = 1;
    sockaddr_in6 v6only_local{};
    v6only_local.sin6_family = AF_INET6;
    v6only_local.sin6_addr = in6addr_any;
    v6only_local.sin6_port = htons(TESTD_TCP_V6ONLY_PORT);
    sockaddr_in ipv4_local{};
    ipv4_local.sin_family = AF_INET;
    ipv4_local.sin_addr.s_addr = htonl(INADDR_ANY);
    ipv4_local.sin_port = htons(TESTD_TCP_V6ONLY_PORT);
    if (V6ONLY_TCP < 0 || IPV4_TCP < 0 || setsockopt(V6ONLY_TCP, IPPROTO_IPV6, IPV6_V6ONLY, &ONE, sizeof(ONE)) != 0 ||
        bind(V6ONLY_TCP, reinterpret_cast<const sockaddr*>(&v6only_local), sizeof(v6only_local)) != 0 ||
        bind(IPV4_TCP, reinterpret_cast<const sockaddr*>(&ipv4_local), sizeof(ipv4_local)) != 0 || listen(V6ONLY_TCP, 1) != 0 ||
        listen(IPV4_TCP, 1) != 0) {
        close(V6ONLY_TCP);
        close(IPV4_TCP);
        fail("tcp_v6only_wildcard", "AF_INET6 V6ONLY and AF_INET listeners could not coexist on one port");
        return;
    }
    close(V6ONLY_TCP);
    close(IPV4_TCP);
    TESTD_PASS("tcp_v6only_wildcard_isolation");

    std::array<int, 2> ready_pipe = {-1, -1};
    if (pipe(ready_pipe.data()) != 0) {
        fail("ipv6_tcp_ready_pipe", "pipe failed");
        return;
    }
    pid_t const SERVER_PID = fork();
    if (SERVER_PID < 0) {
        close(ready_pipe[0]);
        close(ready_pipe[1]);
        fail("ipv6_tcp_fork", "fork failed");
        return;
    }
    if (SERVER_PID == 0) {
        close(ready_pipe[0]);
        tcp_ipv6_loopback_server(ready_pipe[1]);
        _exit(99);
    }
    close(ready_pipe[1]);
    char ready = 0;
    ssize_t const READY_BYTES = read_expected_bytes_timeout(ready_pipe[0], &ready, sizeof(ready), REMOTE_IPC_TIMEOUT_MS);
    close(ready_pipe[0]);
    if (READY_BYTES != sizeof(ready) || ready != 1) {
        int status = 0;
        static_cast<void>(waitpid_timeout(SERVER_PID, &status, REMOTE_IPC_TIMEOUT_MS));
        fail("ipv6_tcp_ready", "server did not become ready");
        return;
    }

    int const TCP_CLIENT = socket(AF_INET6, SOCK_STREAM, 0);
    sockaddr_in6 const TCP_SERVER = ipv6_loopback_endpoint(TESTD_TCP_IPV6_PORT);
    if (TCP_CLIENT < 0 || setsockopt(TCP_CLIENT, IPPROTO_IPV6, IPV6_V6ONLY, &ONE, sizeof(ONE)) != 0 ||
        connect_timeout(TCP_CLIENT, reinterpret_cast<const sockaddr*>(&TCP_SERVER), sizeof(TCP_SERVER), REMOTE_IPC_TIMEOUT_MS) != 0) {
        close(TCP_CLIENT);
        int status = 0;
        static_cast<void>(waitpid_timeout(SERVER_PID, &status, REMOTE_IPC_TIMEOUT_MS));
        fail("ipv6_tcp_connect", "native IPv6 loopback connect failed");
        return;
    }
    sockaddr_in6 tcp_local{};
    sockaddr_in6 tcp_peer{};
    std::array<char, 16384> sent{};
    std::array<char, 16384> echoed{};
    for (size_t i = 0; i < sent.size(); ++i) {
        sent[i] = static_cast<char>((i * 29U + 6U) & 0xFFU);
    }
    ssize_t const TCP_SENT = send_all_timeout(TCP_CLIENT, sent.data(), sent.size(), 0, REMOTE_IPC_TIMEOUT_MS);
    ssize_t const TCP_RECEIVED = recv_expected_bytes_timeout(TCP_CLIENT, echoed.data(), echoed.size(), REMOTE_IPC_TIMEOUT_MS);
    bool const TCP_NAMES_VALID = ipv6_socket_name(TCP_CLIENT, false, tcp_local) && ipv6_socket_name(TCP_CLIENT, true, tcp_peer) &&
                                 is_ipv6_loopback_endpoint(tcp_local) && same_ipv6_endpoint(tcp_peer, TCP_SERVER);
    close(TCP_CLIENT);
    int tcp_server_status = 0;
    bool const TCP_SERVER_EXITED = waitpid_timeout(SERVER_PID, &tcp_server_status, REMOTE_IPC_TIMEOUT_MS);
    if (!TCP_NAMES_VALID || TCP_SENT != static_cast<ssize_t>(sent.size()) || TCP_RECEIVED != static_cast<ssize_t>(echoed.size()) ||
        sent != echoed || !TCP_SERVER_EXITED || !WIFEXITED(tcp_server_status) || WEXITSTATUS(tcp_server_status) != 0) {
        testd_logf("[TESTD] INFO: ipv6 tcp sent=%zd recv=%zd status=%d", TCP_SENT, TCP_RECEIVED, tcp_server_status);
        fail("ipv6_tcp_loopback", "tuple or payload integrity check failed");
        return;
    }
    TESTD_PASS("ipv6_tcp_loopback");

    int const UDP_SERVER_FD = socket(AF_INET6, SOCK_DGRAM, 0);
    int const UDP_CLIENT_FD = socket(AF_INET6, SOCK_DGRAM, 0);
    sockaddr_in6 const UDP_SERVER = ipv6_loopback_endpoint(TESTD_UDP_IPV6_PORT);
    if (UDP_SERVER_FD < 0 || UDP_CLIENT_FD < 0 || setsockopt(UDP_SERVER_FD, IPPROTO_IPV6, IPV6_V6ONLY, &ONE, sizeof(ONE)) != 0 ||
        setsockopt(UDP_CLIENT_FD, IPPROTO_IPV6, IPV6_V6ONLY, &ONE, sizeof(ONE)) != 0 ||
        bind(UDP_SERVER_FD, reinterpret_cast<const sockaddr*>(&UDP_SERVER), sizeof(UDP_SERVER)) != 0 ||
        connect_timeout(UDP_CLIENT_FD, reinterpret_cast<const sockaddr*>(&UDP_SERVER), sizeof(UDP_SERVER), REMOTE_IPC_TIMEOUT_MS) != 0) {
        close(UDP_SERVER_FD);
        close(UDP_CLIENT_FD);
        fail("ipv6_udp_setup", "native IPv6 UDP setup failed");
        return;
    }
    sockaddr_in6 udp_server_local{};
    sockaddr_in6 udp_client_local{};
    sockaddr_in6 udp_client_peer{};
    bool const UDP_NAMES_VALID =
        ipv6_socket_name(UDP_SERVER_FD, false, udp_server_local) && ipv6_socket_name(UDP_CLIENT_FD, false, udp_client_local) &&
        ipv6_socket_name(UDP_CLIENT_FD, true, udp_client_peer) && same_ipv6_endpoint(udp_server_local, UDP_SERVER) &&
        is_ipv6_loopback_endpoint(udp_client_local) && same_ipv6_endpoint(udp_client_peer, UDP_SERVER);

    std::array<char, 96> udp_sent{};
    std::array<char, 96> udp_server_received{};
    std::array<char, 96> udp_echoed{};
    for (size_t i = 0; i < udp_sent.size(); ++i) {
        udp_sent[i] = static_cast<char>((i * 47U + 17U) & 0xFFU);
    }
    ssize_t const UDP_SENT = send(UDP_CLIENT_FD, udp_sent.data(), udp_sent.size(), 0);
    sockaddr_in6 udp_source{};
    socklen_t udp_source_length = sizeof(udp_source);
    ssize_t UDP_SERVER_RECEIVED = -1;
    if (wait_fd_ready(UDP_SERVER_FD, POLLIN, REMOTE_IPC_TIMEOUT_MS) > 0) {
        UDP_SERVER_RECEIVED = recvfrom(UDP_SERVER_FD, udp_server_received.data(), udp_server_received.size(), 0,
                                       reinterpret_cast<sockaddr*>(&udp_source), &udp_source_length);
    }
    ssize_t UDP_ECHO_SENT = -1;
    if (UDP_SERVER_RECEIVED == static_cast<ssize_t>(udp_server_received.size()) && udp_source_length == sizeof(udp_source) &&
        same_ipv6_endpoint(udp_source, udp_client_local) && udp_server_received == udp_sent) {
        UDP_ECHO_SENT = sendto(UDP_SERVER_FD, udp_server_received.data(), udp_server_received.size(), 0,
                               reinterpret_cast<const sockaddr*>(&udp_source), sizeof(udp_source));
    }
    sockaddr_in6 udp_echo_source{};
    socklen_t udp_echo_source_length = sizeof(udp_echo_source);
    ssize_t UDP_ECHO_RECEIVED = -1;
    if (UDP_ECHO_SENT == static_cast<ssize_t>(udp_server_received.size()) &&
        wait_fd_ready(UDP_CLIENT_FD, POLLIN, REMOTE_IPC_TIMEOUT_MS) > 0) {
        UDP_ECHO_RECEIVED = recvfrom(UDP_CLIENT_FD, udp_echoed.data(), udp_echoed.size(), 0, reinterpret_cast<sockaddr*>(&udp_echo_source),
                                     &udp_echo_source_length);
    }
    close(UDP_SERVER_FD);
    close(UDP_CLIENT_FD);
    if (!UDP_NAMES_VALID || UDP_SENT != static_cast<ssize_t>(udp_sent.size()) ||
        UDP_SERVER_RECEIVED != static_cast<ssize_t>(udp_server_received.size()) || UDP_ECHO_SENT != UDP_SERVER_RECEIVED ||
        UDP_ECHO_RECEIVED != static_cast<ssize_t>(udp_echoed.size()) || udp_echo_source_length != sizeof(udp_echo_source) ||
        !same_ipv6_endpoint(udp_echo_source, UDP_SERVER) || udp_echoed != udp_sent) {
        testd_logf("[TESTD] INFO: ipv6 udp sent=%zd server_recv=%zd echo_sent=%zd echo_recv=%zd", UDP_SENT, UDP_SERVER_RECEIVED,
                   UDP_ECHO_SENT, UDP_ECHO_RECEIVED);
        fail("ipv6_udp_loopback", "tuple or payload integrity check failed");
        return;
    }
    TESTD_PASS("ipv6_udp_loopback");

    int const UDP_DUAL_SERVER = socket(AF_INET6, SOCK_DGRAM, 0);
    int const UDP_IPV4_CLIENT = socket(AF_INET, SOCK_DGRAM, 0);
    int const ZERO = 0;
    sockaddr_in6 udp_dual_local{};
    udp_dual_local.sin6_family = AF_INET6;
    udp_dual_local.sin6_addr = in6addr_any;
    udp_dual_local.sin6_port = htons(TESTD_UDP_DUAL_STACK_PORT);
    sockaddr_in udp_dual_destination{};
    udp_dual_destination.sin_family = AF_INET;
    udp_dual_destination.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    udp_dual_destination.sin_port = htons(TESTD_UDP_DUAL_STACK_PORT);
    std::array<char, 256> const UDP_DUAL_SENT = [] {
        std::array<char, 256> value{};
        for (size_t i = 0; i < value.size(); ++i) {
            value[i] = static_cast<char>(((i * 31U) + 7U) & 0xFFU);
        }
        return value;
    }();
    bool udp_dual_ok =
        UDP_DUAL_SERVER >= 0 && UDP_IPV4_CLIENT >= 0 && setsockopt(UDP_DUAL_SERVER, IPPROTO_IPV6, IPV6_V6ONLY, &ZERO, sizeof(ZERO)) == 0 &&
        bind(UDP_DUAL_SERVER, reinterpret_cast<const sockaddr*>(&udp_dual_local), sizeof(udp_dual_local)) == 0 &&
        sendto(UDP_IPV4_CLIENT, UDP_DUAL_SENT.data(), UDP_DUAL_SENT.size(), 0, reinterpret_cast<const sockaddr*>(&udp_dual_destination),
               sizeof(udp_dual_destination)) == static_cast<ssize_t>(UDP_DUAL_SENT.size()) &&
        wait_fd_ready(UDP_DUAL_SERVER, POLLIN, REMOTE_IPC_TIMEOUT_MS) > 0;
    std::array<char, 256> udp_dual_received{};
    sockaddr_in6 udp_mapped_source{};
    socklen_t udp_mapped_source_length = sizeof(udp_mapped_source);
    ssize_t udp_dual_received_length = -1;
    if (udp_dual_ok) {
        udp_dual_received_length = recvfrom(UDP_DUAL_SERVER, udp_dual_received.data(), udp_dual_received.size(), 0,
                                            reinterpret_cast<sockaddr*>(&udp_mapped_source), &udp_mapped_source_length);
        udp_dual_ok = udp_dual_received_length == static_cast<ssize_t>(udp_dual_received.size()) &&
                      udp_mapped_source_length == sizeof(udp_mapped_source) && is_v4_mapped_loopback(udp_mapped_source) &&
                      udp_mapped_source.sin6_port != 0 && udp_dual_received == UDP_DUAL_SENT;
    }
    if (udp_dual_ok) {
        udp_dual_ok = sendto(UDP_DUAL_SERVER, udp_dual_received.data(), udp_dual_received.size(), 0,
                             reinterpret_cast<const sockaddr*>(&udp_mapped_source),
                             sizeof(udp_mapped_source)) == static_cast<ssize_t>(udp_dual_received.size()) &&
                      wait_fd_ready(UDP_IPV4_CLIENT, POLLIN, REMOTE_IPC_TIMEOUT_MS) > 0;
    }
    std::array<char, 256> udp_dual_echoed{};
    sockaddr_in udp_dual_echo_source{};
    socklen_t udp_dual_echo_source_length = sizeof(udp_dual_echo_source);
    ssize_t udp_dual_echo_length = -1;
    if (udp_dual_ok) {
        udp_dual_echo_length = recvfrom(UDP_IPV4_CLIENT, udp_dual_echoed.data(), udp_dual_echoed.size(), 0,
                                        reinterpret_cast<sockaddr*>(&udp_dual_echo_source), &udp_dual_echo_source_length);
        udp_dual_ok = udp_dual_echo_length == static_cast<ssize_t>(udp_dual_echoed.size()) &&
                      udp_dual_echo_source_length == sizeof(udp_dual_echo_source) && udp_dual_echo_source.sin_family == AF_INET &&
                      udp_dual_echo_source.sin_addr.s_addr == htonl(INADDR_LOOPBACK) &&
                      udp_dual_echo_source.sin_port == htons(TESTD_UDP_DUAL_STACK_PORT) && udp_dual_echoed == UDP_DUAL_SENT;
    }
    close(UDP_DUAL_SERVER);
    close(UDP_IPV4_CLIENT);
    if (!udp_dual_ok) {
        testd_logf("[TESTD] INFO: UDP dual-stack mapped recv=%zd echo=%zd source_len=%u", udp_dual_received_length, udp_dual_echo_length,
                   static_cast<unsigned>(udp_mapped_source_length));
        fail("udp_dual_stack_mapped", "mapped IPv4 datagram delivery or sockaddr_in6 source failed");
        return;
    }
    TESTD_PASS("udp_dual_stack_ipv4_client");
}
TESTD_RUN_END(test_ipv6_loopback)

TESTD_RUN(test_tcp_nonblocking_connect_refused) {
    constexpr uint16_t TCP_PORT = 19877;  // no server listens here
    int const FD = socket(AF_INET, SOCK_STREAM, 0);
    if (FD < 0) {
        fail("tcp_refused_socket", "socket failed");
        return;
    }

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(TCP_PORT);  // no server on this port

    // Blocking connect to a closed port must fail with ECONNREFUSED
    int const RC = connect_timeout(FD, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr), REMOTE_IPC_TIMEOUT_MS);
    int saved_errno = errno;
    close(FD);

    if (RC == 0) {
        fail("tcp_refused", "expected ECONNREFUSED");
        return;
    }
    if (saved_errno != ECONNREFUSED) {
        testd_logf("[TESTD] WARN: tcp_refused got errno=%d (expected %d)", saved_errno, ECONNREFUSED);
    }
    TESTD_PASS("tcp_connect_refused");
}
TESTD_RUN_END(test_tcp_nonblocking_connect_refused)
