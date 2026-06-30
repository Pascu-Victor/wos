#include "testd.hpp"

// ---------------------------------------------------------------------------
// B3: TCP loopback socket tests
// ---------------------------------------------------------------------------

constexpr uint16_t TESTD_TCP_PORT = 19876;

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
}
TESTD_RUN_END(test_tcp_loopback)

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
