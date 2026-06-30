#include "httpd/routes.hpp"

#include <sys/multiproc.h>
#include <sys/process.h>
#include <sys/vfs.h>

#include <array>
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
#include "httpd/time.hpp"

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
        auto body = parse_request_body(request);
        auto device = get_form_field(body, "device");
        auto mount_path = get_form_field(body, "path");
        auto fstype = get_form_field(body, "fstype");
        if (fstype.empty()) {
            fstype = "fat32";
        }

        if (device.empty() || mount_path.empty()) {
            constexpr std::string_view ERR_BODY =
                "<html><head><title>Mount Error</title></head><body>"
                "<h1>Error</h1><p>Missing device or path parameter.</p>"
                "<hr><p><a href=\"/dev/\">Back to /dev/</a></p>"
                "<p><em>WOS-httpd/1.0</em></p></body></html>\r\n";
            send_response(client_fd, 400, "Bad Request", "text/html; charset=utf-8", ERR_BODY.data(), ERR_BODY.size());
            return;
        }

        ker::abi::vfs::mkdir(mount_path.c_str(), 0755);

        std::array<const char*, 6> argv = {"/bin/mount", "-t", fstype.c_str(), device.c_str(), mount_path.c_str(), nullptr};
        constexpr std::array<const char*, 1> ENVP = {nullptr};
        auto exec_res = ker::process::exec("/bin/mount", argv.data(), ENVP.data());

        if (exec_res == 0) {
            log_message("httpd[t:{},p:{}]: Failed to exec for mount {} at {}", tid, pid, device, mount_path);
            std::string result_html;
            result_html.reserve(512);
            result_html += "<html><head><title>Mount Error</title></head><body>";
            result_html += "<h1>Error</h1><p>Failed to fork mount process for <b>";
            result_html += device;
            result_html += "</b> at <b>";
            result_html += mount_path;
            result_html += "</b></p>";
            result_html += "<hr><p><a href=\"/dev/\">Back to /dev/</a></p>";
            result_html += "<p><em>WOS-httpd/1.0</em></p></body></html>\r\n";
            send_response(client_fd, 500, "Internal Server Error", "text/html; charset=utf-8", result_html.data(), result_html.size());
            return;
        }

        int32_t exit_code = 0;
        bool const MOUNT_COMPLETED = wait_for_child_timeout(static_cast<int64_t>(exec_res), &exit_code, MOUNT_CHILD_TIMEOUT_MS);

        if (MOUNT_COMPLETED && exit_code == 0) {
            log_message("httpd[t:{},p:{}]: Mounted {} at {} ({})", tid, pid, device, mount_path, fstype);
            auto redirect = std::format(
                "HTTP/1.1 303 See Other\r\n"
                "Location: {}/\r\n"
                "Connection: close\r\n"
                "Server: WOS-httpd/1.0\r\n"
                "\r\n",
                mount_path);
            send_all(client_fd, redirect.data(), redirect.size());
        } else {
            log_message("httpd[t:{},p:{}]: Failed to mount {} at {} ({}): status {}", tid, pid, device, mount_path, fstype, exit_code);
            std::string result_html;
            result_html.reserve(512);
            result_html += "<html><head><title>Mount Error</title></head><body>";
            result_html += "<h1>Error</h1><p>Failed to mount <b>";
            result_html += device;
            result_html += "</b> at <b>";
            result_html += mount_path;
            result_html += "</b> (status ";
            result_html += std::to_string(exit_code);
            result_html += ")</p>";
            result_html += "<hr><p><a href=\"/dev/\">Back to /dev/</a></p>";
            result_html += "<p><em>WOS-httpd/1.0</em></p></body></html>\r\n";
            send_response(client_fd, 500, "Internal Server Error", "text/html; charset=utf-8", result_html.data(), result_html.size());
        }
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
