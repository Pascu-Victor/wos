#include "testd.hpp"

// ---------------------------------------------------------------------------
// Remote IPC tests - exercise WKI IPC proxy by spawning a child on a remote
// node that operates on inherited IPC file descriptors.
// ---------------------------------------------------------------------------

// Child writes through an inherited pipe write-end (IPC_PIPE proxy on remote).
TESTD_RUN(test_remote_ipc_pipe_child_write) {
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        fail("remote_pipe_create", "pipe failed");
        return;
    }
    TESTD_PASS("remote_pipe_create");

    // Set REMOTE so the child's exec is routed to a remote node.
    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_helper("pipe-write", fds[1], fds[0]);
    // Restore flags on the parent immediately after fork.
    ker::process::setwkitarget(nullptr, 0, 0);

    if (PID < 0) {
        close(fds[0]);
        close(fds[1]);
        fail("remote_pipe_fork", "fork failed");
        return;
    }
    // Parent holds the read end; close the write end so we get EOF when child exits.
    close(fds[1]);

    std::array<char, 64> buf{};
    ssize_t const NR = read_expected_bytes(fds[0], buf.data(), RH_PIPE_WRITE_MSG.size());
    close(fds[0]);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_pipe_child_exit", "child timed out or waitpid failed");
        return;
    }

    if (std::cmp_not_equal(NR, RH_PIPE_WRITE_MSG.size()) || std::string_view(buf.data(), static_cast<size_t>(NR)) != RH_PIPE_WRITE_MSG) {
        fail("remote_pipe_child_write", "data mismatch or short read");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_pipe_child_exit", "child exited with error");
        return;
    }
    TESTD_PASS("remote_pipe_child_write");
}
TESTD_RUN_END(test_remote_ipc_pipe_child_write)

// Parent writes through its own pipe write-end; remote child reads (IPC_PIPE proxy on remote).
TESTD_RUN(test_remote_ipc_pipe_parent_write) {
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        fail("remote_pipe_parent_create", "pipe failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_helper("pipe-read", fds[0], fds[1]);
    ker::process::setwkitarget(nullptr, 0, 0);

    if (PID < 0) {
        close(fds[0]);
        close(fds[1]);
        fail("remote_pipe_parent_fork", "fork failed");
        return;
    }
    // Parent holds the write end; child holds the read end.
    close(fds[0]);

    ssize_t const NW = write(fds[1], RH_PIPE_READ_EXPECT.data(), RH_PIPE_READ_EXPECT.size());
    close(fds[1]);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_pipe_parent_write", "child timed out or waitpid failed");
        return;
    }

    if (std::cmp_not_equal(NW, RH_PIPE_READ_EXPECT.size())) {
        fail("remote_pipe_parent_write", "short write");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_pipe_parent_write", "child rejected data or errored");
        return;
    }
    TESTD_PASS("remote_pipe_parent_write");
}
TESTD_RUN_END(test_remote_ipc_pipe_parent_write)

// Remote child writes through an inherited PTY slave fd (IPC_PTY proxy) and
// the local PTY master receives the bytes.
TESTD_RUN(test_remote_ipc_pty_child_write) {
    int const MASTER_FD = open("/dev/ptmx", O_RDWR);
    if (MASTER_FD < 0) {
        fail("remote_pty_data_open_master", "open /dev/ptmx failed");
        return;
    }

    unsigned int pty_num = 0;
    if (ioctl(MASTER_FD, TIOCGPTN, &pty_num) != 0) {
        close(MASTER_FD);
        fail("remote_pty_data_tiocgptn", "TIOCGPTN failed");
        return;
    }

    int unlock = 0;
    if (ioctl(MASTER_FD, TIOCSPTLCK, &unlock) != 0) {
        close(MASTER_FD);
        fail("remote_pty_data_unlock_slave", "TIOCSPTLCK unlock failed");
        return;
    }

    std::array<char, 32> slave_path{};
    (void)testd_format_to_array(slave_path, "/dev/pts/%u", pty_num);
    int const SLAVE_FD = open(slave_path.data(), O_RDWR);
    if (SLAVE_FD < 0) {
        close(MASTER_FD);
        fail("remote_pty_data_open_slave", "open slave failed");
        return;
    }
    if (!make_pty_raw(SLAVE_FD)) {
        close(SLAVE_FD);
        close(MASTER_FD);
        fail("remote_pty_data_raw", "failed to set raw PTY mode");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_helper("pty-write", SLAVE_FD, MASTER_FD);
    ker::process::setwkitarget(nullptr, 0, 0);

    if (PID < 0) {
        close(MASTER_FD);
        close(SLAVE_FD);
        fail("remote_pty_data_fork", "fork failed");
        return;
    }

    close(SLAVE_FD);

    std::array<char, 64> buf{};
    ssize_t const N = read_expected_bytes(MASTER_FD, buf.data(), RH_PTY_WRITE_MSG.size());

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        close(MASTER_FD);
        fail("remote_pty_child_exit", "child timed out or waitpid failed");
        return;
    }
    close(MASTER_FD);

    if (std::cmp_not_equal(N, RH_PTY_WRITE_MSG.size()) || std::string_view(buf.data(), static_cast<size_t>(N)) != RH_PTY_WRITE_MSG) {
        fail("remote_pty_data", "PTY payload mismatch");
        return;
    }

    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_pty_child_exit", "child exited with error");
        return;
    }

    TESTD_PASS("remote_pty_data");
}
TESTD_RUN_END(test_remote_ipc_pty_child_write)

// Remote child inherits a PTY slave fd and performs TIOCGWINSZ ioctl through
// the IPC_PTY proxy.
TESTD_RUN(test_remote_ipc_pty_ioctl) {
    int const MASTER_FD = open("/dev/ptmx", O_RDWR);
    if (MASTER_FD < 0) {
        fail("remote_pty_open_master", "open /dev/ptmx failed");
        return;
    }

    // Get the PTY number so we can open the slave.
    unsigned int pty_num = 0;
    if (ioctl(MASTER_FD, TIOCGPTN, &pty_num) != 0) {
        close(MASTER_FD);
        fail("remote_pty_tiocgptn", "TIOCGPTN failed");
        return;
    }

    // PTY slave starts locked; unlock before opening /dev/pts/<n>.
    int unlock = 0;
    if (ioctl(MASTER_FD, TIOCSPTLCK, &unlock) != 0) {
        close(MASTER_FD);
        fail("remote_pty_unlock_slave", "TIOCSPTLCK unlock failed");
        return;
    }

    std::array<char, 32> slave_path{};
    (void)testd_format_to_array(slave_path, "/dev/pts/%u", pty_num);
    int const SLAVE_FD = open(slave_path.data(), O_RDWR);
    if (SLAVE_FD < 0) {
        close(MASTER_FD);
        fail("remote_pty_open_slave", "open slave failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_helper("pty-ioctl", SLAVE_FD, MASTER_FD);
    ker::process::setwkitarget(nullptr, 0, 0);

    if (PID < 0) {
        close(MASTER_FD);
        close(SLAVE_FD);
        fail("remote_pty_fork", "fork failed");
        return;
    }
    // Parent keeps master; child gets slave via WKI proxy.
    close(SLAVE_FD);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        close(MASTER_FD);
        fail("remote_pty_ioctl", "child timed out or waitpid failed");
        return;
    }
    close(MASTER_FD);

    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_pty_ioctl", "child ioctl failed or exited with error");
        return;
    }
    TESTD_PASS("remote_pty_ioctl");
}
TESTD_RUN_END(test_remote_ipc_pty_ioctl)

// Remote child writes on an inherited connected TCP socket (IPC_SOCKET proxy).
TESTD_RUN(test_remote_ipc_socket_child_write) {
    int const LISTEN_FD = socket(AF_INET, SOCK_STREAM, 0);
    if (LISTEN_FD < 0) {
        fail("remote_sock_listen_socket", "socket failed");
        return;
    }
    int one = 1;
    setsockopt(LISTEN_FD, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in bind_addr{};
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bind_addr.sin_port = 0;  // ephemeral port
    if (bind(LISTEN_FD, reinterpret_cast<struct sockaddr*>(&bind_addr), sizeof(bind_addr)) != 0) {
        close(LISTEN_FD);
        fail("remote_sock_bind", "bind failed");
        return;
    }
    if (listen(LISTEN_FD, 1) != 0) {
        close(LISTEN_FD);
        fail("remote_sock_listen", "listen failed");
        return;
    }

    struct sockaddr_in got_addr{};
    socklen_t got_len = sizeof(got_addr);
    if (getsockname(LISTEN_FD, reinterpret_cast<struct sockaddr*>(&got_addr), &got_len) != 0) {
        close(LISTEN_FD);
        fail("remote_sock_getsockname", "getsockname failed");
        return;
    }

    int const CLIENT_FD = socket(AF_INET, SOCK_STREAM, 0);
    if (CLIENT_FD < 0) {
        close(LISTEN_FD);
        fail("remote_sock_client_socket", "socket failed");
        return;
    }
    if (connect_timeout(CLIENT_FD, reinterpret_cast<struct sockaddr*>(&got_addr), sizeof(got_addr), REMOTE_IPC_TIMEOUT_MS) != 0) {
        close(CLIENT_FD);
        close(LISTEN_FD);
        fail("remote_sock_connect", "connect failed");
        return;
    }

    int const SERVER_FD = accept_timeout(LISTEN_FD, nullptr, nullptr, REMOTE_IPC_TIMEOUT_MS);
    close(LISTEN_FD);
    if (SERVER_FD < 0) {
        close(CLIENT_FD);
        fail("remote_sock_accept", "accept failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_helper("sock-write", CLIENT_FD, SERVER_FD);
    ker::process::setwkitarget(nullptr, 0, 0);

    if (PID < 0) {
        close(CLIENT_FD);
        close(SERVER_FD);
        fail("remote_sock_fork", "fork failed");
        return;
    }

    close(CLIENT_FD);

    std::array<char, 64> recv_buf{};
    ssize_t const NR = recv_expected_bytes(SERVER_FD, recv_buf.data(), RH_SOCKET_WRITE_MSG.size());
    close(SERVER_FD);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_sock_child_exit", "child timed out or waitpid failed");
        return;
    }

    if (std::cmp_not_equal(NR, RH_SOCKET_WRITE_MSG.size()) ||
        std::string_view(recv_buf.data(), static_cast<size_t>(NR)) != RH_SOCKET_WRITE_MSG) {
        fail("remote_sock_child_write", "socket payload mismatch");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_sock_child_exit", "child exited with error");
        return;
    }
    TESTD_PASS("remote_sock_child_write");
}
TESTD_RUN_END(test_remote_ipc_socket_child_write)

// Remote child performs socket control ops on an inherited connected TCP
// socket: getpeername, setsockopt, getsockopt, and shutdown.
TESTD_RUN(test_remote_ipc_socket_control_ops) {
    int const LISTEN_FD = socket(AF_INET, SOCK_STREAM, 0);
    if (LISTEN_FD < 0) {
        fail("remote_sock_ctrl_listen_socket", "socket failed");
        return;
    }
    int one = 1;
    setsockopt(LISTEN_FD, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in bind_addr{};
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bind_addr.sin_port = 0;
    if (bind(LISTEN_FD, reinterpret_cast<struct sockaddr*>(&bind_addr), sizeof(bind_addr)) != 0) {
        close(LISTEN_FD);
        fail("remote_sock_ctrl_bind", "bind failed");
        return;
    }
    if (listen(LISTEN_FD, 1) != 0) {
        close(LISTEN_FD);
        fail("remote_sock_ctrl_listen", "listen failed");
        return;
    }

    struct sockaddr_in got_addr{};
    socklen_t got_len = sizeof(got_addr);
    if (getsockname(LISTEN_FD, reinterpret_cast<struct sockaddr*>(&got_addr), &got_len) != 0) {
        close(LISTEN_FD);
        fail("remote_sock_ctrl_getsockname", "getsockname failed");
        return;
    }

    int const CLIENT_FD = socket(AF_INET, SOCK_STREAM, 0);
    if (CLIENT_FD < 0) {
        close(LISTEN_FD);
        fail("remote_sock_ctrl_client_socket", "socket failed");
        return;
    }
    if (connect_timeout(CLIENT_FD, reinterpret_cast<struct sockaddr*>(&got_addr), sizeof(got_addr), REMOTE_IPC_TIMEOUT_MS) != 0) {
        close(CLIENT_FD);
        close(LISTEN_FD);
        fail("remote_sock_ctrl_connect", "connect failed");
        return;
    }

    int const SERVER_FD = accept_timeout(LISTEN_FD, nullptr, nullptr, REMOTE_IPC_TIMEOUT_MS);
    close(LISTEN_FD);
    if (SERVER_FD < 0) {
        close(CLIENT_FD);
        fail("remote_sock_ctrl_accept", "accept failed");
        return;
    }

    std::array<char, 16> port_buf{};
    (void)testd_format_to_array(port_buf, "%u", static_cast<unsigned>(ntohs(got_addr.sin_port)));

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_helper_arg("sock-ctrl", CLIENT_FD, SERVER_FD, port_buf.data());
    ker::process::setwkitarget(nullptr, 0, 0);

    if (PID < 0) {
        close(CLIENT_FD);
        close(SERVER_FD);
        fail("remote_sock_ctrl_fork", "fork failed");
        return;
    }

    close(CLIENT_FD);

    char eof_probe = 0;
    ssize_t const NR = recv_once_timeout(SERVER_FD, &eof_probe, sizeof(eof_probe), 0, REMOTE_IPC_TIMEOUT_MS);
    close(SERVER_FD);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_sock_ctrl_ops", "child timed out or waitpid failed");
        return;
    }

    if (NR != 0) {
        fail("remote_sock_ctrl_shutdown", "expected EOF after remote shutdown");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_sock_ctrl_ops", "child control-op checks failed");
        return;
    }
    TESTD_PASS("remote_ipc_socket_control_ops");
}
TESTD_RUN_END(test_remote_ipc_socket_control_ops)

// Remote child blocks in poll() on an inherited IPC pipe proxy and wakes on parent write.
TESTD_RUN(test_remote_ipc_poll_wait_pipe_readable) {
    std::array<int, 2> pipe_fds = {-1, -1};
    if (pipe(pipe_fds.data()) != 0) {
        fail("remote_poll_wait_pipe", "pipe failed");
        return;
    }
    std::array<int, 2> ready_pipe = {-1, -1};
    if (pipe(ready_pipe.data()) != 0) {
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        fail("remote_poll_wait_ready_pipe", "ready pipe failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_wait_helper("poll-wait", pipe_fds[0], pipe_fds[1], ready_pipe[1], ready_pipe[0]);
    ker::process::setwkitarget(nullptr, 0, 0);

    close(pipe_fds[0]);
    close(ready_pipe[1]);

    if (PID < 0) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        fail("remote_poll_wait_fork", "fork failed");
        return;
    }

    if (!wait_remote_waiter_ready(ready_pipe[0])) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        int status = 0;
        (void)waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("remote_poll_wait_ready", "remote poll helper did not reach wait point");
        return;
    }
    close(ready_pipe[0]);

    std::string_view const MSG = "P";
    if (write(pipe_fds[1], MSG.data(), MSG.size()) != static_cast<ssize_t>(MSG.size())) {
        close(pipe_fds[1]);
        int status = 0;
        (void)waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("remote_poll_wait_write", "write failed");
        return;
    }
    close(pipe_fds[1]);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_poll_wait_child", "remote poll child timed out or waitpid failed");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_poll_wait_child", "remote poll child failed");
        return;
    }

    TESTD_PASS("remote_ipc_poll_wait_pipe_readable");
}
TESTD_RUN_END(test_remote_ipc_poll_wait_pipe_readable)

// Remote child blocks in poll() on an inherited IPC pipe proxy and wakes with
// HUP/EOF when the home-side writer closes without sending data.
TESTD_RUN(test_remote_ipc_poll_wait_pipe_hup) {
    std::array<int, 2> pipe_fds = {-1, -1};
    if (pipe(pipe_fds.data()) != 0) {
        fail("remote_poll_hup_pipe", "pipe failed");
        return;
    }
    std::array<int, 2> ready_pipe = {-1, -1};
    if (pipe(ready_pipe.data()) != 0) {
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        fail("remote_poll_hup_ready_pipe", "ready pipe failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_wait_helper("poll-wait-hup", pipe_fds[0], pipe_fds[1], ready_pipe[1], ready_pipe[0]);
    ker::process::setwkitarget(nullptr, 0, 0);

    close(pipe_fds[0]);
    close(ready_pipe[1]);

    if (PID < 0) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        fail("remote_poll_hup_fork", "fork failed");
        return;
    }

    if (!wait_remote_waiter_ready(ready_pipe[0])) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        int status = 0;
        (void)waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("remote_poll_hup_ready", "remote poll HUP helper did not reach wait point");
        return;
    }
    close(ready_pipe[0]);
    close(pipe_fds[1]);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_poll_hup_child", "remote poll HUP child timed out or waitpid failed");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_poll_hup_child", "remote poll HUP child failed");
        return;
    }

    TESTD_PASS("remote_ipc_poll_wait_pipe_hup");
}
TESTD_RUN_END(test_remote_ipc_poll_wait_pipe_hup)

// Remote poll waiter must still observe HUP/EOF when the home-side writer is
// already closed before the helper has definitely registered its waiter.
TESTD_RUN(test_remote_ipc_poll_pipe_preclosed_hup) {
    std::array<int, 2> pipe_fds = {-1, -1};
    if (pipe(pipe_fds.data()) != 0) {
        fail("remote_poll_preclosed_hup_pipe", "pipe failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = fork();
    if (PID == 0) {
        close(pipe_fds[1]);

        std::array<char, 16> fd_str{};
        (void)testd_format_to_array(fd_str, "%d", pipe_fds[0]);

        auto exec_path = std::to_array("/usr/bin/testd");
        auto rh_flag = std::to_array("--rh");
        auto mode_buf = std::to_array("poll-preclosed-hup");
        std::array<char*, 5> child_argv = {
            exec_path.data(), rh_flag.data(), mode_buf.data(), fd_str.data(), nullptr,
        };
        execve("/usr/bin/testd", child_argv.data(), nullptr);
        _exit(RH_EXIT_EXEC_FAILED);
    }
    ker::process::setwkitarget(nullptr, 0, 0);

    close(pipe_fds[0]);

    if (PID < 0) {
        close(pipe_fds[1]);
        fail("remote_poll_preclosed_hup_fork", "fork failed");
        return;
    }

    close(pipe_fds[1]);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_poll_preclosed_hup_child", "remote preclosed poll HUP child timed out or waitpid failed");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_poll_preclosed_hup_child", "remote preclosed poll HUP child failed");
        return;
    }

    TESTD_PASS("remote_ipc_poll_pipe_preclosed_hup");
}
TESTD_RUN_END(test_remote_ipc_poll_pipe_preclosed_hup)

// Remote child must not lose data when the home-side writer sends one byte and
// then closes. This guards pending-data versus close ordering.
TESTD_RUN(test_remote_ipc_poll_pipe_read_then_hup) {
    std::array<int, 2> pipe_fds = {-1, -1};
    if (pipe(pipe_fds.data()) != 0) {
        fail("remote_poll_drain_hup_pipe", "pipe failed");
        return;
    }
    std::array<int, 2> ready_pipe = {-1, -1};
    if (pipe(ready_pipe.data()) != 0) {
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        fail("remote_poll_drain_hup_ready_pipe", "ready pipe failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_wait_helper("poll-drain-hup", pipe_fds[0], pipe_fds[1], ready_pipe[1], ready_pipe[0]);
    ker::process::setwkitarget(nullptr, 0, 0);

    close(pipe_fds[0]);
    close(ready_pipe[1]);

    if (PID < 0) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        fail("remote_poll_drain_hup_fork", "fork failed");
        return;
    }

    if (!wait_remote_waiter_ready(ready_pipe[0])) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        int status = 0;
        (void)waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("remote_poll_drain_hup_ready", "remote poll drain/HUP helper did not reach wait point");
        return;
    }
    close(ready_pipe[0]);

    std::string_view const MSG = "D";
    if (write(pipe_fds[1], MSG.data(), MSG.size()) != static_cast<ssize_t>(MSG.size())) {
        close(pipe_fds[1]);
        int status = 0;
        (void)waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("remote_poll_drain_hup_write", "write failed");
        return;
    }
    close(pipe_fds[1]);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_poll_drain_hup_child", "remote poll drain/HUP child timed out or waitpid failed");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_poll_drain_hup_child", "remote poll drain/HUP child failed");
        return;
    }

    TESTD_PASS("remote_ipc_poll_pipe_read_then_hup");
}
TESTD_RUN_END(test_remote_ipc_poll_pipe_read_then_hup)

// Remote child blocks in epoll_wait() on an inherited IPC pipe proxy and wakes on parent write.
TESTD_RUN(test_remote_ipc_epoll_wait_pipe_readable) {
    std::array<int, 2> pipe_fds = {-1, -1};
    if (pipe(pipe_fds.data()) != 0) {
        fail("remote_epoll_wait_pipe", "pipe failed");
        return;
    }
    std::array<int, 2> ready_pipe = {-1, -1};
    if (pipe(ready_pipe.data()) != 0) {
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        fail("remote_epoll_wait_ready_pipe", "ready pipe failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_wait_helper("epoll-wait", pipe_fds[0], pipe_fds[1], ready_pipe[1], ready_pipe[0]);
    ker::process::setwkitarget(nullptr, 0, 0);

    close(pipe_fds[0]);
    close(ready_pipe[1]);

    if (PID < 0) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        fail("remote_epoll_wait_fork", "fork failed");
        return;
    }

    if (!wait_remote_waiter_ready(ready_pipe[0])) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        int status = 0;
        (void)waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("remote_epoll_wait_ready", "remote epoll helper did not reach wait point");
        return;
    }
    close(ready_pipe[0]);

    std::string_view const MSG = "E";
    if (write(pipe_fds[1], MSG.data(), MSG.size()) != static_cast<ssize_t>(MSG.size())) {
        close(pipe_fds[1]);
        int status = 0;
        (void)waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("remote_epoll_wait_write", "write failed");
        return;
    }
    close(pipe_fds[1]);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_epoll_wait_child", "remote epoll_wait child timed out or waitpid failed");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_epoll_wait_child", "remote epoll_wait child failed");
        return;
    }

    TESTD_PASS("remote_ipc_epoll_wait_pipe_readable");
}
TESTD_RUN_END(test_remote_ipc_epoll_wait_pipe_readable)

// Remote child blocks in epoll_wait() on an inherited IPC pipe proxy and wakes
// with HUP/EOF when the home-side writer closes without sending data.
TESTD_RUN(test_remote_ipc_epoll_wait_pipe_hup) {
    std::array<int, 2> pipe_fds = {-1, -1};
    if (pipe(pipe_fds.data()) != 0) {
        fail("remote_epoll_hup_pipe", "pipe failed");
        return;
    }
    std::array<int, 2> ready_pipe = {-1, -1};
    if (pipe(ready_pipe.data()) != 0) {
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        fail("remote_epoll_hup_ready_pipe", "ready pipe failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_wait_helper("epoll-wait-hup", pipe_fds[0], pipe_fds[1], ready_pipe[1], ready_pipe[0]);
    ker::process::setwkitarget(nullptr, 0, 0);

    close(pipe_fds[0]);
    close(ready_pipe[1]);

    if (PID < 0) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        fail("remote_epoll_hup_fork", "fork failed");
        return;
    }

    if (!wait_remote_waiter_ready(ready_pipe[0])) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        int status = 0;
        (void)waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("remote_epoll_hup_ready", "remote epoll HUP helper did not reach wait point");
        return;
    }
    close(ready_pipe[0]);
    close(pipe_fds[1]);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_epoll_hup_child", "remote epoll_wait HUP child timed out or waitpid failed");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_epoll_hup_child", "remote epoll_wait HUP child failed");
        return;
    }

    TESTD_PASS("remote_ipc_epoll_wait_pipe_hup");
}
TESTD_RUN_END(test_remote_ipc_epoll_wait_pipe_hup)

// Remote epoll waiter must still observe HUP/EOF when the home-side writer is
// already closed before the helper has definitely registered its waiter.
TESTD_RUN(test_remote_ipc_epoll_pipe_preclosed_hup) {
    std::array<int, 2> pipe_fds = {-1, -1};
    if (pipe(pipe_fds.data()) != 0) {
        fail("remote_epoll_preclosed_hup_pipe", "pipe failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = fork();
    if (PID == 0) {
        close(pipe_fds[1]);

        std::array<char, 16> fd_str{};
        (void)testd_format_to_array(fd_str, "%d", pipe_fds[0]);

        auto exec_path = std::to_array("/usr/bin/testd");
        auto rh_flag = std::to_array("--rh");
        auto mode_buf = std::to_array("epoll-preclosed-hup");
        std::array<char*, 5> child_argv = {
            exec_path.data(), rh_flag.data(), mode_buf.data(), fd_str.data(), nullptr,
        };
        execve("/usr/bin/testd", child_argv.data(), nullptr);
        _exit(RH_EXIT_EXEC_FAILED);
    }
    ker::process::setwkitarget(nullptr, 0, 0);

    close(pipe_fds[0]);

    if (PID < 0) {
        close(pipe_fds[1]);
        fail("remote_epoll_preclosed_hup_fork", "fork failed");
        return;
    }

    close(pipe_fds[1]);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_epoll_preclosed_hup_child", "remote preclosed epoll HUP child timed out or waitpid failed");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_epoll_preclosed_hup_child", "remote preclosed epoll HUP child failed");
        return;
    }

    TESTD_PASS("remote_ipc_epoll_pipe_preclosed_hup");
}
TESTD_RUN_END(test_remote_ipc_epoll_pipe_preclosed_hup)

// Remote child must not lose data when epoll observes a proxy pipe whose
// home-side writer sends one byte and then closes.
TESTD_RUN(test_remote_ipc_epoll_pipe_read_then_hup) {
    std::array<int, 2> pipe_fds = {-1, -1};
    if (pipe(pipe_fds.data()) != 0) {
        fail("remote_epoll_drain_hup_pipe", "pipe failed");
        return;
    }
    std::array<int, 2> ready_pipe = {-1, -1};
    if (pipe(ready_pipe.data()) != 0) {
        close(pipe_fds[0]);
        close(pipe_fds[1]);
        fail("remote_epoll_drain_hup_ready_pipe", "ready pipe failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = spawn_remote_wait_helper("epoll-drain-hup", pipe_fds[0], pipe_fds[1], ready_pipe[1], ready_pipe[0]);
    ker::process::setwkitarget(nullptr, 0, 0);

    close(pipe_fds[0]);
    close(ready_pipe[1]);

    if (PID < 0) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        fail("remote_epoll_drain_hup_fork", "fork failed");
        return;
    }

    if (!wait_remote_waiter_ready(ready_pipe[0])) {
        close(pipe_fds[1]);
        close(ready_pipe[0]);
        int status = 0;
        (void)waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("remote_epoll_drain_hup_ready", "remote epoll drain/HUP helper did not reach wait point");
        return;
    }
    close(ready_pipe[0]);

    std::string_view const MSG = "G";
    if (write(pipe_fds[1], MSG.data(), MSG.size()) != static_cast<ssize_t>(MSG.size())) {
        close(pipe_fds[1]);
        int status = 0;
        (void)waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS);
        fail("remote_epoll_drain_hup_write", "write failed");
        return;
    }
    close(pipe_fds[1]);

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        fail("remote_epoll_drain_hup_child", "remote epoll drain/HUP child timed out or waitpid failed");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fail("remote_epoll_drain_hup_child", "remote epoll drain/HUP child failed");
        return;
    }

    TESTD_PASS("remote_ipc_epoll_pipe_read_then_hup");
}
TESTD_RUN_END(test_remote_ipc_epoll_pipe_read_then_hup)

// Remote child performs epoll_ctl(ADD) on inherited epoll fd (IPC_EPOLL proxy).
TESTD_RUN(test_remote_ipc_epoll_ctl_add) {
    std::array<int, 2> fds = {-1, -1};
    if (pipe(fds.data()) != 0) {
        fail("remote_epoll_pipe", "pipe failed");
        return;
    }
    int const EPFD = epoll_create1(0);
    if (EPFD < 0) {
        close(fds[0]);
        close(fds[1]);
        fail("remote_epoll_create", "epoll_create1 failed");
        return;
    }

    ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    pid_t const PID = fork();
    if (PID == 0) {
        close(fds[1]);
        std::array<char, 16> epfd_str{};
        std::array<char, 16> rfd_str{};
        (void)testd_format_to_array(epfd_str, "%d", EPFD);
        (void)testd_format_to_array(rfd_str, "%d", fds[0]);

        auto exec_path = std::to_array("/usr/bin/testd");
        auto rh_flag = std::to_array("--rh");
        auto mode_buf = std::to_array("epoll-add");
        std::array<char*, 6> child_argv = {
            exec_path.data(), rh_flag.data(), mode_buf.data(), epfd_str.data(), rfd_str.data(), nullptr,
        };
        execve("/usr/bin/testd", child_argv.data(), nullptr);
        _exit(RH_EXIT_EXEC_FAILED);
    }
    ker::process::setwkitarget(nullptr, 0, 0);

    if (PID < 0) {
        close(fds[0]);
        close(fds[1]);
        close(EPFD);
        fail("remote_epoll_fork", "fork failed");
        return;
    }

    int status = 0;
    if (!waitpid_timeout(PID, &status, REMOTE_IPC_TIMEOUT_MS)) {
        close(fds[0]);
        close(fds[1]);
        close(EPFD);
        fail("remote_epoll_ctl_add", "remote epoll child timed out or waitpid failed");
        return;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        close(fds[0]);
        close(fds[1]);
        close(EPFD);
        fail("remote_epoll_ctl_add", "remote epoll_ctl add failed");
        return;
    }

    std::string_view const MSG = "E";
    if (write(fds[1], MSG.data(), MSG.size()) != static_cast<ssize_t>(MSG.size())) {
        close(fds[0]);
        close(fds[1]);
        close(EPFD);
        fail("remote_epoll_write", "write failed");
        return;
    }

    struct epoll_event ev{};
    int const READY = epoll_wait(EPFD, &ev, 1, 1000);
    close(fds[0]);
    close(fds[1]);
    close(EPFD);

    if (READY != 1 || ev.data.fd != fds[0] || (ev.events & EPOLLIN) == 0) {
        fail("remote_epoll_wait", "expected EPOLLIN event on pipe read fd");
        return;
    }
    TESTD_PASS("remote_epoll_ctl_add");
}
TESTD_RUN_END(test_remote_ipc_epoll_ctl_add)
