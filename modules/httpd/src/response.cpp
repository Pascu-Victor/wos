#include "httpd/response.hpp"

#include <format>
#include <string>

#include "httpd/socket_io.hpp"

namespace httpd {

auto send_response(int client_fd, int status_code, const char* status_text, const char* content_type, const void* body, size_t body_len)
    -> ssize_t {
    std::string const HEADER = std::format(
        "HTTP/1.1 {} {}\r\n"
        "Content-Type: {}\r\n"
        "Content-Length: {}\r\n"
        "Connection: close\r\n"
        "Server: WOS-httpd/1.0\r\n"
        "\r\n",
        status_code, status_text, content_type, body_len);

    ssize_t sent = send_all(client_fd, HEADER.data(), HEADER.size());
    if (sent < 0) {
        return sent;
    }

    if (body != nullptr && body_len > 0) {
        sent = send_all(client_fd, body, body_len);
    }
    return sent;
}

}  // namespace httpd
