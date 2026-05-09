#include <abi-bits/fcntl.h>
#include <abi-bits/in.h>
#include <abi-bits/socket.h>
#include <abi-bits/socklen_t.h>
#include <abi-bits/stat.h>
#include <arpa/inet.h>
#include <bits/posix/stat.h>
#include <bits/ssize_t.h>
#include <dirent.h>
#include <fcntl.h>
#include <sched.h>
#include <sys/logging.h>
#include <sys/multiproc.h>
#include <sys/process.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <format>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

constexpr uint16_t HTTP_PORT = 80;
constexpr const char* SERVE_ROOT = "/";

using httpd_log = wos::journal<"httpd">;

// Logging utility
template <typename... Args>
void log_message(std::format_string<Args...> fmt, Args&&... args) {
    std::string const MSG = std::format(fmt, std::forward<Args>(args)...);
    httpd_log::info("%s", MSG.c_str());
}
constexpr size_t REQUEST_BUFFER_SIZE = 4096;
constexpr size_t FILE_STREAM_BUFFER_SIZE = static_cast<size_t>(4096) * 1024;
constexpr int MAX_PENDING_CONNECTIONS = 128;

// MIME type lookup based on file extension
auto get_mime_type(std::string_view path) -> const char* {
    auto dot_pos = path.rfind('.');
    if (dot_pos == std::string_view::npos) {
        return "application/octet-stream";
    }
    auto ext = path.substr(dot_pos);

    // Common MIME types
    if (ext == ".html" || ext == ".htm") {
        return "text/html; charset=utf-8";
    }
    if (ext == ".css") {
        return "text/css; charset=utf-8";
    }
    if (ext == ".js") {
        return "application/javascript; charset=utf-8";
    }
    if (ext == ".json") {
        return "application/json; charset=utf-8";
    }
    if (ext == ".xml") {
        return "application/xml; charset=utf-8";
    }
    if (ext == ".txt") {
        return "text/plain; charset=utf-8";
    }
    if (ext == ".md") {
        return "text/markdown; charset=utf-8";
    }
    if (ext == ".csv") {
        return "text/csv; charset=utf-8";
    }
    // Images
    if (ext == ".png") {
        return "image/png";
        return "image/png";
    }
    if (ext == ".jpg" || ext == ".jpeg") {
        return "image/jpeg";
    }
    if (ext == ".gif") {
        return "image/gif";
    }
    if (ext == ".svg") {
        return "image/svg+xml";
    }
    if (ext == ".ico") {
        return "image/x-icon";
    }
    if (ext == ".webp") {
        return "image/webp";
    }
    if (ext == ".bmp") {
        return "image/bmp";
    }

    // Fonts
    if (ext == ".woff") {
        return "font/woff";
    }
    if (ext == ".woff2") {
        return "font/woff2";
    }
    if (ext == ".ttf") {
        return "font/ttf";
    }
    if (ext == ".otf") {
        return "font/otf";
    }

    // Documents
    if (ext == ".pdf") {
        return "application/pdf";
    }
    if (ext == ".zip") {
        return "application/zip";
    }
    if (ext == ".gz" || ext == ".gzip") {
        return "application/gzip";
    }
    if (ext == ".tar") {
        return "application/x-tar";
    }

    // Audio/Video
    if (ext == ".mp3") {
        return "audio/mpeg";
    }
    if (ext == ".wav") {
        return "audio/wav";
    }
    if (ext == ".mp4") {
        return "video/mp4";
    }
    if (ext == ".webm") {
        return "video/webm";
    }

    return "application/octet-stream";
}

// URL decode a path (handle %XX encoding)
auto url_decode(std::string_view encoded) -> std::string {
    std::string result;
    result.reserve(encoded.size());

    for (size_t i = 0; i < encoded.size(); ++i) {
        if (encoded[i] == '%' && i + 2 < encoded.size()) {
            std::array<char, 3> hex = {encoded[i + 1], encoded[i + 2], '\0'};
            char* end = nullptr;
            long const VAL = strtol(hex.data(), &end, 16);
            if (end == hex.data() + 2) {
                result += static_cast<char>(VAL);
                i += 2;
                continue;
            }
        } else if (encoded[i] == '+') {
            result += ' ';
            continue;
        }
        result += encoded[i];
    }
    return result;
}

// Check if path is safe (no directory traversal)
auto is_safe_path(std::string_view path) -> bool {
    // Reject paths with ..
    return !path.contains("..");
}

// Format file size for display
auto format_size(size_t size) -> std::string {
    if (size < 1024) {
        return std::format("{} B", size);
    }

    auto format_tenths = [](size_t value, size_t unit, const char* suffix) -> std::string {
        size_t whole = value / unit;
        size_t tenth = ((value % unit) * 10) / unit;
        return std::format("{}.{} {}", whole, tenth, suffix);
    };

    if (size < static_cast<size_t>(1024 * 1024)) {
        return format_tenths(size, 1024, "KB");
    }
    return format_tenths(size, 1024 * 1024, "MB");
}

// Simple HTTP response templates
constexpr std::string_view HTTP_404_RESPONSE =
    "HTTP/1.1 404 Not Found\r\n"
    "Content-Type: text/html; charset=utf-8\r\n"
    "Connection: close\r\n"
    "Server: WOS-httpd/1.0\r\n"
    "\r\n"
    "<html><head><title>404 Not Found</title></head><body>"
    "<h1>404 Not Found</h1><p>The requested resource was not found on this server.</p>"
    "<hr><p><em>WOS-httpd/1.0</em></p></body></html>\r\n";

constexpr std::string_view HTTP_403_RESPONSE =
    "HTTP/1.1 403 Forbidden\r\n"
    "Content-Type: text/html; charset=utf-8\r\n"
    "Connection: close\r\n"
    "Server: WOS-httpd/1.0\r\n"
    "\r\n"
    "<html><head><title>403 Forbidden</title></head><body>"
    "<h1>403 Forbidden</h1><p>Access to this resource is denied.</p>"
    "<hr><p><em>WOS-httpd/1.0</em></p></body></html>\r\n";

[[maybe_unused]]
constexpr std::string_view HTTP_500_RESPONSE =
    "HTTP/1.1 500 Internal Server Error\r\n"
    "Content-Type: text/html; charset=utf-8\r\n"
    "Connection: close\r\n"
    "Server: WOS-httpd/1.0\r\n"
    "\r\n"
    "<html><head><title>500 Internal Server Error</title></head><body>"
    "<h1>500 Internal Server Error</h1><p>An error occurred processing your request.</p>"
    "<hr><p><em>WOS-httpd/1.0</em></p></body></html>\r\n";

// Generate directory listing HTML
auto generate_directory_listing(const std::string& fs_path, std::string_view url_path) -> std::string {
    std::string html;
    html.reserve(8192);

    // HTML header
    html += "<html>\r\n<head>\r\n";
    html += "<title>Index of ";
    html += url_path;
    html += "</title>\r\n";
    html += "<style>\r\n";
    html += "body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 40px; }\r\n";
    html += "h1 { color: #333; border-bottom: 1px solid #ccc; padding-bottom: 10px; }\r\n";
    html += "table { border-collapse: collapse; width: 100%; max-width: 800px; }\r\n";
    html += "th, td { text-align: left; padding: 8px 12px; border-bottom: 1px solid #eee; }\r\n";
    html += "th { background: #f5f5f5; font-weight: 600; }\r\n";
    html += "a { color: #0066cc; text-decoration: none; }\r\n";
    html += "a:hover { text-decoration: underline; }\r\n";
    html += ".icon { margin-right: 8px; }\r\n";
    html += ".size { color: #666; }\r\n";
    html += ".type { color: #888; font-size: 0.9em; }\r\n";
    html += "hr { border: none; border-top: 1px solid #ccc; margin: 20px 0; }\r\n";
    html += "</style>\r\n";
    html += "</head>\r\n<body>\r\n";

    html += "<h1>Index of ";
    html += url_path;
    html += "</h1>\r\n";

    html += "<table>\r\n";
    html += "<tr><th>Name</th><th>Mode</th><th>Owner</th><th>Size</th><th>Type</th><th></th></tr>\r\n";

    // Parent directory link (if not root)
    if (url_path != "/" && url_path.size() > 1) {
        std::string parent = std::string(url_path);
        if (parent.back() == '/') {
            parent.pop_back();
        }
        auto last_slash = parent.rfind('/');
        if (last_slash != std::string::npos && last_slash > 0) {
            parent = parent.substr(0, last_slash + 1);
        } else {
            parent = "/";
        }
        html += "<tr><td><span class='icon'>📁</span><a href=\"";
        html += parent;
        html += "\">..</a></td><td>-</td><td>-</td><td class='size'>-</td><td class='type'>Parent Directory</td><td></td></tr>\r\n";
    }

    // Read directory entries
    DIR* dir = opendir(fs_path.c_str());
    if (dir == nullptr) {
        html += "<tr><td colspan='3'>Error reading directory</td></tr>\r\n";
    } else {
        struct DirEntry {
            std::string name;
            bool is_dir;
            bool is_blk;
            uint32_t mode;
            uint32_t uid;
            uint32_t gid;
        };
        std::vector<DirEntry> entries;

        struct dirent* entry = nullptr;
        while ((entry = readdir(dir)) != nullptr) {
            std::string const NAME{static_cast<const char*>(entry->d_name)};
            if (NAME == "." || NAME == "..") {
                continue;
            }

            // Check if it's a directory or block device
            std::string full_path = fs_path;
            if (full_path.back() != '/') {
                full_path += '/';
            }
            full_path += NAME;

            struct stat st{};
            bool is_dir = false;
            bool is_blk = false;
            if (stat(full_path.c_str(), &st) == 0) {
                is_dir = S_ISDIR(st.st_mode);
                is_blk = S_ISBLK(st.st_mode);
            }

            entries.push_back({.name = NAME,
                               .is_dir = is_dir,
                               .is_blk = is_blk,
                               .mode = static_cast<uint32_t>(st.st_mode),
                               .uid = static_cast<uint32_t>(st.st_uid),
                               .gid = static_cast<uint32_t>(st.st_gid)});
        }
        closedir(dir);

        // Sort: directories first, then alphabetically
        std::ranges::sort(entries, [](const DirEntry& a, const DirEntry& b) {
            if (a.is_dir != b.is_dir) {
                return a.is_dir > b.is_dir;  // dirs first
            }
            return a.name < b.name;
        });

        for (const auto& ent : entries) {
            std::string full_path = fs_path;
            if (full_path.back() != '/') {
                full_path += '/';
            }
            full_path += ent.name;

            std::string url = std::string(url_path);
            if (url.back() != '/') {
                url += '/';
            }
            url += ent.name;
            if (ent.is_dir) {
                url += '/';
            }

            struct stat st{};
            size_t size = 0;
            if (stat(full_path.c_str(), &st) == 0) {
                size = st.st_size;
            }

            html += "<tr><td><span class='icon'>";
            if (ent.is_dir) {
                html += "📁";
            } else if (ent.is_blk) {
                html += "💾";
            } else {
                html += "📄";
            }
            html += "</span><a href=\"";
            html += url;
            html += "\">";
            html += ent.name;
            if (ent.is_dir) {
                html += "/";
            }
            html += "</a></td><td>";
            // Format mode as octal
            {
                char mode_str[32];
                // rwx string
                const char* rwx = "rwxrwxrwx";
                char perms[11];
                perms[0] = ent.is_dir ? 'd' : (ent.is_blk ? 'b' : '-');
                for (int b = 0; b < 9; b++) {
                    perms[1 + b] = ((ent.mode & (1 << (8 - b))) != 0U) ? rwx[b] : '-';
                }
                perms[10] = '\0';
                int const N = snprintf(mode_str, sizeof(mode_str), "%s (%04o)", perms, ent.mode & 07777);
                if (N > 0 && std::cmp_less(N, sizeof(mode_str))) {
                    html.append(mode_str, N);
                }
            }
            html += "</td><td>";
            {
                char owner_str[32];
                int const N = snprintf(owner_str, sizeof(owner_str), "%u:%u", ent.uid, ent.gid);
                html.append(owner_str, N);
            }
            html += "</td><td class='size'>";
            html += ent.is_dir ? "-" : format_size(size);
            html += "</td><td class='type'>";
            if (ent.is_blk) {
                html += "Block Device";
                html += "</td><td>";
                // Mount form for block devices
                html += "<form method='POST' action='/api/mount' style='display:inline;margin:0;'>";
                html += "<input type='hidden' name='device' value='";
                html += url;
                html += "'/>";
                html += "<input type='text' name='path' placeholder='/mnt/disk' style='width:120px;margin-right:4px;'/>";
                html += "<select name='fstype' style='margin-right:4px;'>";
                html += "<option value='fat32' selected>fat32</option>";
                html += "<option value='tmpfs'>tmpfs</option>";
                html += "</select>";
                html += "<button type='submit'>Mount</button>";
                html += "</form>";
            } else {
                html += ent.is_dir ? "Directory" : get_mime_type(ent.name);
                html += "</td><td>";
            }
            html += "</td></tr>\r\n";
        }
    }

    html += "</table>\r\n";
    html += "<hr>\r\n";
    html += "<p><em>WOS-httpd/1.0</em></p>\r\n";
    html += "</body>\r\n</html>\r\n";

    return html;
}

// Send all data, handling partial sends and EAGAIN
auto send_all(int fd, const void* data, size_t len) -> ssize_t {
    const auto* ptr = static_cast<const char*>(data);
    size_t remaining = len;
    size_t total_sent = 0;
    int retries = 0;
    constexpr int MAX_SEND_RETRIES = 10000;

    while (remaining > 0) {
        ssize_t const SENT = send(fd, ptr, remaining, 0);
        if (SENT < 0) {
            // EAGAIN (-11): window full or buffer exhaustion - retry
            if (SENT == -11 && retries < MAX_SEND_RETRIES) {
                retries++;
                sched_yield();
                continue;
            }
            // Other error or too many retries
            if (total_sent > 0) {
                return static_cast<ssize_t>(total_sent);
            }
            return SENT;
        }
        if (SENT == 0) {
            // Connection closed
            break;
        }
        ptr += SENT;
        remaining -= SENT;
        total_sent += SENT;
        retries = 0;  // Reset retry counter on progress
    }

    return static_cast<ssize_t>(total_sent);
}

// Send an HTTP response with custom headers
auto send_response(int client_fd, int status_code, const char* status_text, const char* content_type, const void* body, size_t body_len)
    -> ssize_t {
    // Build header
    std::array<char, 512> header{};
    int const HEADER_LEN = snprintf(header.data(), header.size(),
                                    "HTTP/1.1 %d %s\r\n"
                                    "Content-Type: %s\r\n"
                                    "Content-Length: %zu\r\n"
                                    "Connection: close\r\n"
                                    "Server: WOS-httpd/1.0\r\n"
                                    "\r\n",
                                    status_code, status_text, content_type, body_len);

    // Send header (using send_all to handle partial sends)
    ssize_t sent = send_all(client_fd, header.data(), HEADER_LEN);
    if (sent < 0) {
        return sent;
    }

    // Send body (using send_all to handle partial sends)
    if (body != nullptr && body_len > 0) {
        sent = send_all(client_fd, body, body_len);
    }
    return sent;
}

// Serve a file from the filesystem
auto serve_file(int client_fd, const std::string& fs_path, std::string_view url_path, auto tid, auto pid) -> bool {
    // Check if path exists and get info
    struct stat st{};
    if (stat(fs_path.c_str(), &st) != 0) {
        log_message("httpd[t:{},p:{}]: File not found: {}", tid, pid, fs_path);
        send(client_fd, HTTP_404_RESPONSE.data(), HTTP_404_RESPONSE.size(), 0);
        return false;
    }

    // Handle directory
    if (S_ISDIR(st.st_mode)) {
        // Check for index.html
        std::string index_path = fs_path;
        if (index_path.back() != '/') {
            index_path += '/';
        }
        index_path += "index.html";

        struct stat index_st{};
        if (stat(index_path.c_str(), &index_st) == 0 && S_ISREG(index_st.st_mode)) {
            // Serve index.html
            return serve_file(client_fd, index_path, url_path, tid, pid);
        }

        // Generate directory listing
        std::string listing = generate_directory_listing(fs_path, url_path);
        send_response(client_fd, 200, "OK", "text/html; charset=utf-8", listing.data(), listing.size());
        log_message("httpd[t:{},p:{}]: Served directory listing: {}", tid, pid, fs_path);
        return true;
    }

    // Regular file
    if (!S_ISREG(st.st_mode)) {
        send(client_fd, HTTP_403_RESPONSE.data(), HTTP_403_RESPONSE.size(), 0);
        return false;
    }

    // Read file
    int const FD = open(fs_path.c_str(), O_RDONLY);
    if (FD < 0) {
        log_message("httpd[t:{},p:{}]: Failed to open file: {}", tid, pid, fs_path);
        send(client_fd, HTTP_500_RESPONSE.data(), HTTP_500_RESPONSE.size(), 0);
        return false;
    }

    // Send response header first, then stream file body in chunks.
    const char* mime_type = get_mime_type(fs_path);
    ssize_t const HEADER_SENT = send_response(client_fd, 200, "OK", mime_type, nullptr, static_cast<size_t>(st.st_size));
    if (HEADER_SENT < 0) {
        close(FD);
        return false;
    }

    std::vector<char> io_buf(FILE_STREAM_BUFFER_SIZE);
    ssize_t total_sent = 0;
    for (;;) {
        ssize_t const BYTES_READ = read(FD, io_buf.data(), io_buf.size());
        if (BYTES_READ < 0) {
            log_message("httpd[t:{},p:{}]: Failed to read file: {}", tid, pid, fs_path);
            close(FD);
            return false;
        }
        if (BYTES_READ == 0) {
            break;
        }

        ssize_t const BYTES_SENT = send_all(client_fd, io_buf.data(), static_cast<size_t>(BYTES_READ));
        if (BYTES_SENT < 0) {
            close(FD);
            return false;
        }
        total_sent += BYTES_SENT;
    }

    close(FD);

    log_message("httpd[t:{},p:{}]: Served file: {} ({} bytes, {})", tid, pid, fs_path, total_sent, mime_type);
    return true;
}

// Parse HTTP method from request line
auto parse_request_method(std::string_view request) -> std::string_view {
    auto first_space = request.find(' ');
    if (first_space == std::string_view::npos) {
        return "GET";
    }
    return request.substr(0, first_space);
}

// Parse HTTP request to extract the path
auto parse_request_path(std::string_view request) -> std::string_view {
    // Find the first line (GET /path HTTP/1.1)
    auto line_end = request.find("\r\n");
    if (line_end == std::string_view::npos) {
        return "/";
    }
    auto first_line = request.substr(0, line_end);

    // Find the method and path
    auto first_space = first_line.find(' ');
    if (first_space == std::string_view::npos) {
        return "/";
    }

    auto second_space = first_line.find(' ', first_space + 1);
    if (second_space == std::string_view::npos) {
        return "/";
    }

    return first_line.substr(first_space + 1, second_space - first_space - 1);
}

// Extract POST body from HTTP request (content after \r\n\r\n)
auto parse_request_body(std::string_view request) -> std::string_view {
    auto body_start = request.find("\r\n\r\n");
    if (body_start == std::string_view::npos) {
        return {};
    }
    return request.substr(body_start + 4);
}

// Extract a URL-encoded form field value by key from a body like "key1=val1&key2=val2"
auto get_form_field(std::string_view body, std::string_view key) -> std::string {
    std::string search_key{key};
    search_key += '=';

    size_t pos = 0;
    while (pos < body.size()) {
        // Check if this position starts with the key
        if (body.substr(pos).starts_with(search_key)) {
            pos += search_key.size();
            auto end = body.find('&', pos);
            if (end == std::string_view::npos) {
                end = body.size();
            }
            return url_decode(body.substr(pos, end - pos));
        }
        // Skip to next field
        auto amp = body.find('&', pos);
        if (amp == std::string_view::npos) {
            break;
        }
        pos = amp + 1;
    }
    return {};
}

// Handle an HTTP request and send response
auto handle_request(int client_fd, std::string_view request) -> void {
    auto path = parse_request_path(request);

    auto pid = ker::process::getpid();
    auto tid = ker::multiproc::currentThreadId();

    log_message("httpd[t:{},p:{}]: Request for path: {}", tid, pid, path);

    // URL decode the path
    std::string decoded_path = url_decode(path);

    // Security check
    if (!is_safe_path(decoded_path)) {
        log_message("httpd[t:{},p:{}]: Rejected unsafe path: {}", tid, pid, decoded_path);
        send(client_fd, HTTP_403_RESPONSE.data(), HTTP_403_RESPONSE.size(), 0);
        return;
    }

    auto method = parse_request_method(request);

    // Handle POST /api/mount
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

        // Create the mount point directory, then delegate to busybox mount
        ker::abi::vfs::mkdir(mount_path.c_str(), 0755);

        std::array<const char*, 6> argv = {"/bin/mount", "-t", fstype.c_str(), device.c_str(), mount_path.c_str(), nullptr};
        constexpr std::array<const char*, 1> ENVP = {nullptr};
        auto exec_res = ker::process::exec("/bin/mount", argv.data(), ENVP.data());

        if (exec_res < 0) {
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

        // Parent: wait for mount to complete
        int32_t exit_code = 0;
        ker::process::waitpid(exec_res, &exit_code, 0, nullptr);

        if (exit_code == 0) {
            log_message("httpd[t:{},p:{}]: Mounted {} at {} ({})", tid, pid, device, mount_path, fstype);
            // Redirect to the newly mounted path so the browser sees the result
            auto redirect = std::format(
                "HTTP/1.1 303 See Other\r\n"
                "Location: {}/\r\n"
                "Connection: close\r\n"
                "Server: WOS-httpd/1.0\r\n"
                "\r\n",
                mount_path);
            send_all(client_fd, redirect.data(), redirect.size());
        } else {
            log_message("httpd[t:{},p:{}]: Failed to mount {} at {} ({}): exit code {}", tid, pid, device, mount_path, fstype, exit_code);
            std::string result_html;
            result_html.reserve(512);
            result_html += "<html><head><title>Mount Error</title></head><body>";
            result_html += "<h1>Error</h1><p>Failed to mount <b>";
            result_html += device;
            result_html += "</b> at <b>";
            result_html += mount_path;
            result_html += "</b> (exit code ";
            result_html += std::to_string(exit_code);
            result_html += ")</p>";
            result_html += "<hr><p><a href=\"/dev/\">Back to /dev/</a></p>";
            result_html += "<p><em>WOS-httpd/1.0</em></p></body></html>\r\n";
            send_response(client_fd, 500, "Internal Server Error", "text/html; charset=utf-8", result_html.data(), result_html.size());
        }
        return;
    }

    // Handle special built-in routes first
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
        std::array<char, 1024> info_body{};
        int const BODY_LEN = snprintf(info_body.data(), info_body.size(),
                                      "<html><head><title>Server Info</title></head><body>"
                                      "<h1>Server Information</h1>"
                                      "<ul>"
                                      "<li><strong>Process ID:</strong> %lu</li>"
                                      "<li><strong>Thread ID:</strong> %lu</li>"
                                      "<li><strong>Server:</strong> WOS-httpd/1.0</li>"
                                      "<li><strong>Port:</strong> %d</li>"
                                      "<li><strong>Document Root:</strong> %s</li>"
                                      "</ul>"
                                      "<hr><p><em>WOS-httpd/1.0</em></p></body></html>\r\n",
                                      pid, tid, HTTP_PORT, SERVE_ROOT);
        send_response(client_fd, 200, "OK", "text/html; charset=utf-8", info_body.data(), BODY_LEN);
        log_message("httpd[t:{},p:{}]: Served /info", tid, pid);
        return;
    }

    // Map URL path to filesystem path
    std::string fs_path = SERVE_ROOT;
    if (decoded_path.empty() || decoded_path == "/") {
        // Root - serve from SERVE_ROOT
    } else {
        if (decoded_path[0] != '/') {
            fs_path += '/';
        }
        fs_path += decoded_path;
    }

    // Remove trailing slash for stat (but keep track for directory handling)
    bool const HAD_TRAILING_SLASH = !fs_path.empty() && fs_path.back() == '/';
    while (fs_path.size() > 1 && fs_path.back() == '/') {
        fs_path.pop_back();
    }

    // Ensure URL path for directory listing has trailing slash
    std::string url_for_listing = decoded_path;
    if (HAD_TRAILING_SLASH && !url_for_listing.empty() && url_for_listing.back() != '/') {
        url_for_listing += '/';
    }

    // Serve the file or directory
    serve_file(client_fd, fs_path, url_for_listing, tid, pid);
}

}  // namespace

auto main(int argc, char** argv) -> int {
    (void)argc;
    (void)argv;

    auto pid = ker::process::getpid();
    auto tid = ker::multiproc::currentThreadId();

    log_message("httpd[t:{},p:{}]: Starting HTTP server on 0.0.0.0:{}", tid, pid, HTTP_PORT);

    // Create socket
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        log_message("httpd[t:{},p:{}]: Failed to create socket: {}", tid, pid, server_fd);
        return 1;
    }

    // Set socket options (SO_REUSEADDR)
    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        log_message("httpd[t:{},p:{}]: Failed to set SO_REUSEADDR", tid, pid);
    }

    // Bind to 0.0.0.0:80
    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;  // 0.0.0.0
    server_addr.sin_port = htons(HTTP_PORT);

    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
        log_message("httpd[t:{},p:{}]: Failed to bind to port {}", tid, pid, HTTP_PORT);
        close(server_fd);
        return 1;
    }

    log_message("httpd[t:{},p:{}]: Successfully bound to 0.0.0.0:{}", tid, pid, HTTP_PORT);

    // Listen for connections
    if (listen(server_fd, MAX_PENDING_CONNECTIONS) < 0) {
        log_message("httpd[t:{},p:{}]: Failed to listen on socket", tid, pid);
        close(server_fd);
        return 1;
    }

    log_message("httpd[t:{},p:{}]: Listening for connections (backlog={})", tid, pid, MAX_PENDING_CONNECTIONS);

    // Main server loop
    std::array<char, REQUEST_BUFFER_SIZE> buffer{};

    for (;;) {
        struct sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        // Accept connection
        int client_fd = accept(server_fd, reinterpret_cast<struct sockaddr*>(&client_addr), &client_len);
        if (client_fd < 0) {
            log_message("httpd[t:{},p:{}]: Failed to accept connection: {}", tid, pid, client_fd);
            continue;
        }

        // Get client IP
        std::array<char, INET_ADDRSTRLEN> client_ip{};
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip.data(), client_ip.size());
        uint16_t client_port = ntohs(client_addr.sin_port);

        log_message("httpd[t:{},p:{}]: Accepted connection from {}:{}", tid, pid, std::string_view(client_ip.data()), client_port);

        // Read request
        ssize_t received = recv(client_fd, buffer.data(), REQUEST_BUFFER_SIZE - 1, 0);
        if (received < 0) {
            log_message("httpd[t:{},p:{}]: Failed to read request: {}", tid, pid, received);
            close(client_fd);
            continue;
        }

        if (received == 0) {
            log_message("httpd[t:{},p:{}]: Client closed connection", tid, pid);
            close(client_fd);
            continue;
        }

        buffer[received] = '\0';
        std::string_view const REQUEST(buffer.data(), received);

        log_message("httpd[t:{},p:{}]: Received {} bytes from {}:{}", tid, pid, received, std::string_view(client_ip.data()), client_port);

        // Handle request and send response
        handle_request(client_fd, REQUEST);

        // Graceful close: shut down the write side first so the client
        // receives FIN, then drain any unread data the client may have
        // sent (e.g. the browser may pipeline or send trailing bytes).
        // Without this, close() on a socket with unread data sends RST
        // instead of FIN, causing NS_ERROR_NET_RESET in the browser.
        shutdown(client_fd, SHUT_WR);
        {
            std::array<char, 512> drain{};
            while (recv(client_fd, drain.data(), drain.size(), 0) > 0) {
                // discard
            }
        }
        close(client_fd);
        log_message("httpd[t:{},p:{}]: Connection closed", tid, pid);
    }

    // Cleanup (unreachable in practice)
    close(server_fd);
    return 0;
}
