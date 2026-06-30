#include "httpd/server.hpp"

#include <abi-bits/in.h>
#include <abi-bits/socket.h>
#include <abi-bits/socklen_t.h>
#include <arpa/inet.h>
#include <sys/multiproc.h>
#include <sys/process.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <string>
#include <string_view>

#include "httpd/config.hpp"
#include "httpd/log.hpp"
#include "httpd/request.hpp"
#include "httpd/routes.hpp"
#include "httpd/socket_io.hpp"

namespace httpd {

auto run_server() -> int {
    auto pid = ker::process::getpid();
    auto tid = ker::multiproc::currentThreadId();

    log_message("httpd[t:{},p:{}]: Starting HTTP server on 0.0.0.0:{}", tid, pid, HTTP_PORT);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        log_message("httpd[t:{},p:{}]: Failed to create socket: {}", tid, pid, server_fd);
        return 1;
    }
    set_fd_cloexec_best_effort(server_fd);

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        log_message("httpd[t:{},p:{}]: Failed to set SO_REUSEADDR", tid, pid);
    }

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(HTTP_PORT);

    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
        log_message("httpd[t:{},p:{}]: Failed to bind to port {}", tid, pid, HTTP_PORT);
        close(server_fd);
        return 1;
    }

    log_message("httpd[t:{},p:{}]: Successfully bound to 0.0.0.0:{}", tid, pid, HTTP_PORT);

    if (listen(server_fd, MAX_PENDING_CONNECTIONS) < 0) {
        log_message("httpd[t:{},p:{}]: Failed to listen on socket", tid, pid);
        close(server_fd);
        return 1;
    }

    log_message("httpd[t:{},p:{}]: Listening for connections (backlog={})", tid, pid, MAX_PENDING_CONNECTIONS);

    for (;;) {
        struct sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(server_fd, reinterpret_cast<struct sockaddr*>(&client_addr), &client_len);
        if (client_fd < 0) {
            log_message("httpd[t:{},p:{}]: Failed to accept connection: {}", tid, pid, client_fd);
            continue;
        }
        set_fd_cloexec_best_effort(client_fd);

        std::array<char, INET_ADDRSTRLEN> client_ip{};
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip.data(), client_ip.size());
        uint16_t const CLIENT_PORT = ntohs(client_addr.sin_port);

        log_message("httpd[t:{},p:{}]: Accepted connection from {}:{}", tid, pid, std::string_view(client_ip.data()), CLIENT_PORT);

        std::string request;
        if (!read_request_timeout(client_fd, request, CLIENT_IO_TIMEOUT_MS)) {
            int const READ_ERRNO = errno;
            log_message("httpd[t:{},p:{}]: Failed to read complete request: {}", tid, pid, READ_ERRNO);
            close(client_fd);
            continue;
        }

        std::string_view const REQUEST(request.data(), request.size());

        log_message("httpd[t:{},p:{}]: Received {} bytes from {}:{}", tid, pid, request.size(), std::string_view(client_ip.data()),
                    CLIENT_PORT);

        handle_request(client_fd, REQUEST);

        shutdown(client_fd, SHUT_WR);
        drain_client_input_timeout(client_fd, CLIENT_DRAIN_TIMEOUT_MS);
        close(client_fd);
        log_message("httpd[t:{},p:{}]: Connection closed", tid, pid);
    }

    close(server_fd);
    return 0;
}

}  // namespace httpd
