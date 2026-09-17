#include "httpd/routes.hpp"

#include <sys/multiproc.h>
#include <sys/process.h>

#include <cstdint>
#include <format>
#include <string>
#include <string_view>

#include "httpd/config.hpp"
#include "httpd/filesystem.hpp"
#include "httpd/log.hpp"
#include "httpd/request.hpp"
#include "httpd/response.hpp"
#include "httpd/socket_io.hpp"

namespace httpd {

void handle_request(int client_fd, std::string_view request) {
    auto path = parse_request_path(request);

    auto pid = ker::process::getpid();
    auto tid = ker::multiproc::currentThreadId();

    log_message("httpd[t:{},p:{}]: Request for path: {}", tid, pid, path);

    std::string decoded_path = url_decode(path);

    if (!is_safe_path(decoded_path)) {
        log_message("httpd[t:{},p:{}]: Rejected unsafe path: {}", tid, pid, decoded_path);
        send_all(client_fd, HTTP_403_RESPONSE.data(), HTTP_403_RESPONSE.size());
        return;
    }

    auto method = parse_request_method(request);

    if (method == "POST" && decoded_path == "/api/mount") {
        send_all(client_fd, HTTP_403_RESPONSE.data(), HTTP_403_RESPONSE.size());
        return;
    }

    if (decoded_path == "/health") {
        constexpr std::string_view HEALTH_BODY =
            "<html><head><title>Health Check</title></head><body>"
            "<h1>OK</h1><p>Server is healthy</p>"
            "<hr><p><em>WOS-httpd/1.0</em></p></body></html>\r\n";
        send_response(client_fd, 200, "OK", "text/html; charset=utf-8", HEALTH_BODY.data(), HEALTH_BODY.size());
        log_message("httpd[t:{},p:{}]: Served /health", tid, pid);
        return;
    }

    if (decoded_path == "/info") {
        std::string const INFO_BODY = std::format(
            "<html><head><title>Server Info</title></head><body>"
            "<h1>Server Information</h1>"
            "<ul>"
            "<li><strong>Process ID:</strong> {}</li>"
            "<li><strong>Thread ID:</strong> {}</li>"
            "<li><strong>Server:</strong> WOS-httpd/1.0</li>"
            "<li><strong>Port:</strong> {}</li>"
            "<li><strong>Document Root:</strong> {}</li>"
            "</ul>"
            "<hr><p><em>WOS-httpd/1.0</em></p></body></html>\r\n",
            pid, tid, HTTP_PORT, SERVE_ROOT);
        send_response(client_fd, 200, "OK", "text/html; charset=utf-8", INFO_BODY.data(), INFO_BODY.size());
        log_message("httpd[t:{},p:{}]: Served /info", tid, pid);
        return;
    }

    std::string fs_path = SERVE_ROOT;
    if (decoded_path.empty() || decoded_path == "/") {
    } else {
        if (decoded_path.front() != '/') {
            fs_path += '/';
        }
        fs_path += decoded_path;
    }

    bool const HAD_TRAILING_SLASH = !fs_path.empty() && fs_path.back() == '/';
    while (fs_path.size() > 1 && fs_path.back() == '/') {
        fs_path.pop_back();
    }

    std::string url_for_listing = decoded_path;
    if (HAD_TRAILING_SLASH && !url_for_listing.empty() && url_for_listing.back() != '/') {
        url_for_listing += '/';
    }

    serve_file(client_fd, fs_path, url_for_listing, tid, pid);
}

}  // namespace httpd
