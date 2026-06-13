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
#include <poll.h>
#include <signal.h>  // NOLINT(modernize-deprecated-headers): WOS signal constants live here.
#include <sys/logging.h>
#include <sys/multiproc.h>
#include <sys/process.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <climits>
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
constexpr size_t MAX_REQUEST_BYTES = static_cast<size_t>(64) * 1024;
constexpr size_t MAX_CONTENT_LENGTH = static_cast<size_t>(32) * 1024;
constexpr size_t FILE_STREAM_BUFFER_SIZE = static_cast<size_t>(4096) * 1024;
constexpr std::string_view HEADER_TERMINATOR = "\r\n\r\n";
constexpr int MAX_PENDING_CONNECTIONS = 128;
constexpr int CLIENT_IO_TIMEOUT_MS = 30000;
constexpr int CLIENT_DRAIN_TIMEOUT_MS = 1000;
constexpr int CHILD_WAIT_POLL_US = 1000;
constexpr int MOUNT_CHILD_TIMEOUT_MS = 30000;
constexpr int MOUNT_CHILD_REAP_RETRIES = 1000;
constexpr int MSEC_PER_SEC = 1000;
constexpr int NSEC_PER_MSEC = 1000000;
constexpr int64_t NSEC_PER_SEC = 1000LL * NSEC_PER_MSEC;
constexpr int USEC_PER_MSEC = 1000;

auto monotonic_now_ms() -> int64_t {
    timespec ts{};
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return -1;
    }

    if (ts.tv_sec < 0 || ts.tv_nsec < 0 || ts.tv_nsec >= NSEC_PER_SEC) {
        return -1;
    }

    int64_t const NSEC_MS = static_cast<int64_t>(ts.tv_nsec) / NSEC_PER_MSEC;
    auto const SEC = static_cast<int64_t>(ts.tv_sec);
    if (SEC > (INT64_MAX - NSEC_MS) / MSEC_PER_SEC) {
        return INT64_MAX;
    }

    return (SEC * MSEC_PER_SEC) + NSEC_MS;
}

auto deadline_after_ms(int timeout_ms) -> int64_t {
    int64_t const NOW_MS = monotonic_now_ms();
    if (NOW_MS < 0) {
        return -1;
    }
    if (timeout_ms <= 0) {
        return NOW_MS;
    }

    auto const TIMEOUT_MS = static_cast<int64_t>(timeout_ms);
    if (INT64_MAX - NOW_MS < TIMEOUT_MS) {
        return INT64_MAX;
    }
    return NOW_MS + TIMEOUT_MS;
}

auto remaining_ms_until(int64_t deadline_ms, int fallback_timeout_ms) -> int {
    if (deadline_ms < 0) {
        return fallback_timeout_ms;
    }
    int64_t const NOW_MS = monotonic_now_ms();
    if (NOW_MS < 0) {
        return fallback_timeout_ms;
    }
    if (deadline_ms <= NOW_MS) {
        errno = ETIMEDOUT;
        return 0;
    }
    int64_t const REMAINING_MS = deadline_ms - NOW_MS;
    return REMAINING_MS > INT_MAX ? INT_MAX : static_cast<int>(REMAINING_MS);
}

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

auto socket_error_from_result(ssize_t result) -> int {
    if (result < -1) {
        return static_cast<int>(-result);
    }
    return errno;
}

auto retryable_socket_error(int err) -> bool {
    return err == EAGAIN || err == EWOULDBLOCK || err == EINTR || err == EINPROGRESS || err == EALREADY;
}

auto retryable_socket_result(ssize_t result) -> bool { return result < 0 && retryable_socket_error(socket_error_from_result(result)); }

auto child_wait_timed_out(int64_t deadline_ms, uint64_t waited_us, int timeout_ms) -> bool {
    if (deadline_ms >= 0) {
        int64_t const NOW_MS = monotonic_now_ms();
        if (NOW_MS >= 0) {
            return NOW_MS >= deadline_ms;
        }
    }
    return waited_us >= static_cast<uint64_t>(timeout_ms) * USEC_PER_MSEC;
}

void reap_child_after_timeout(int64_t pid) {
    if (pid <= 0) {
        return;
    }

    (void)ker::process::kill(pid, SIGKILL);
    for (int retry = 0; retry < MOUNT_CHILD_REAP_RETRIES; ++retry) {
        int32_t reap_status = 0;
        int64_t const REAPED = ker::process::waitpid(pid, &reap_status, WNOHANG, nullptr);
        if (REAPED == pid || (REAPED < 0 && REAPED != -EINTR)) {
            return;
        }
        usleep(CHILD_WAIT_POLL_US);
    }
}

auto wait_for_child_timeout(int64_t pid, int32_t* status, int timeout_ms) -> bool {
    if (pid <= 0) {
        if (status != nullptr) {
            *status = -EINVAL;
        }
        return false;
    }

    int64_t const DEADLINE_MS = deadline_after_ms(timeout_ms);
    uint64_t waited_us = 0;

    for (;;) {
        int64_t const WAITED = ker::process::waitpid(pid, status, WNOHANG, nullptr);
        if (WAITED == pid) {
            return true;
        }
        if (WAITED < 0 && WAITED != -EINTR) {
            if (status != nullptr) {
                *status = static_cast<int32_t>(WAITED);
            }
            return false;
        }
        if (child_wait_timed_out(DEADLINE_MS, waited_us, timeout_ms)) {
            break;
        }
        usleep(CHILD_WAIT_POLL_US);
        waited_us += CHILD_WAIT_POLL_US;
    }

    reap_child_after_timeout(pid);
    if (status != nullptr) {
        *status = -ETIMEDOUT;
    }
    return false;
}

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

// URL decode a path or form component (handle %XX encoding)
auto url_decode(std::string_view encoded, bool plus_as_space = false) -> std::string {
    std::string result;
    result.reserve(encoded.size());

    for (size_t i = 0; i < encoded.size(); ++i) {
        char const CURRENT = encoded.at(i);
        if (CURRENT == '%' && i + 2 < encoded.size()) {
            std::array<char, 3> hex = {encoded.at(i + 1), encoded.at(i + 2), '\0'};
            char* end = nullptr;
            long const VAL = strtol(hex.data(), &end, 16);
            if (end == hex.data() + 2) {
                result += static_cast<char>(VAL);
                i += 2;
                continue;
            }
        } else if (CURRENT == '+' && plus_as_space) {
            result += ' ';
            continue;
        }
        result += CURRENT;
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

    constexpr size_t KIB = 1024;
    constexpr size_t MIB = KIB * 1024;
    if (size < MIB) {
        return format_tenths(size, 1024, "KB");
    }
    return format_tenths(size, MIB, "MB");
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
                // rwx string
                constexpr std::string_view RWX = "rwxrwxrwx";
                std::array<char, 11> perms{};
                char type_char = '-';
                if (ent.is_dir) {
                    type_char = 'd';
                } else if (ent.is_blk) {
                    type_char = 'b';
                }
                perms.at(0) = type_char;
                for (int b = 0; b < 9; b++) {
                    perms.at(1 + static_cast<size_t>(b)) = ((ent.mode & (1 << (8 - b))) != 0U) ? RWX.at(static_cast<size_t>(b)) : '-';
                }
                perms.at(10) = '\0';
                html += std::format("{} ({:04o})", std::string_view(perms.data(), 10), ent.mode & 07777);
            }
            html += "</td><td>";
            html += std::format("{}:{}", ent.uid, ent.gid);
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

// Send all data without allowing a stalled peer to spin or block this server forever.
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

auto ascii_lower(char ch) -> char {
    if (ch >= 'A' && ch <= 'Z') {
        return static_cast<char>(ch - 'A' + 'a');
    }
    return ch;
}

auto ascii_iequals(std::string_view lhs, std::string_view rhs) -> bool {
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.size(); ++i) {
        if (ascii_lower(lhs.at(i)) != ascii_lower(rhs.at(i))) {
            return false;
        }
    }
    return true;
}

auto trim_optional_whitespace(std::string_view value) -> std::string_view {
    while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) {
        value.remove_prefix(1);
    }
    while (!value.empty() && (value.back() == ' ' || value.back() == '\t')) {
        value.remove_suffix(1);
    }
    return value;
}

auto parse_content_length_value(std::string_view value, size_t& content_length) -> bool {
    value = trim_optional_whitespace(value);
    if (value.empty()) {
        return false;
    }

    size_t parsed = 0;
    for (char const CH : value) {
        if (CH < '0' || CH > '9') {
            return false;
        }
        auto const DIGIT = static_cast<size_t>(CH - '0');
        if (parsed > (MAX_CONTENT_LENGTH - DIGIT) / 10) {
            return false;
        }
        parsed = (parsed * 10) + DIGIT;
    }

    content_length = parsed;
    return true;
}

auto parse_content_length(std::string_view headers, size_t& content_length) -> bool {
    content_length = 0;
    bool found = false;

    size_t line_start = headers.find("\r\n");
    if (line_start == std::string_view::npos) {
        return true;
    }
    line_start += 2;

    while (line_start < headers.size()) {
        size_t line_end = headers.find("\r\n", line_start);
        if (line_end == std::string_view::npos) {
            line_end = headers.size();
        }

        std::string_view const LINE = headers.substr(line_start, line_end - line_start);
        if (LINE.empty()) {
            break;
        }

        size_t const COLON = LINE.find(':');
        if (COLON != std::string_view::npos && ascii_iequals(trim_optional_whitespace(LINE.substr(0, COLON)), "Content-Length")) {
            size_t parsed = 0;
            if (!parse_content_length_value(LINE.substr(COLON + 1), parsed)) {
                errno = EINVAL;
                return false;
            }
            if (found && parsed != content_length) {
                errno = EINVAL;
                return false;
            }
            content_length = parsed;
            found = true;
        }

        line_start = line_end + 2;
    }

    return true;
}

auto read_request_timeout(int fd, std::string& request, int timeout_ms) -> bool {
    int old_flags = -1;
    if (!set_nonblocking_for_timeout(fd, old_flags)) {
        return false;
    }

    request.clear();
    request.reserve(REQUEST_BUFFER_SIZE);

    std::array<char, REQUEST_BUFFER_SIZE> buffer{};
    int64_t const DEADLINE_MS = deadline_after_ms(timeout_ms);
    for (;;) {
        size_t const HEADER_END = request.find(HEADER_TERMINATOR);
        if (HEADER_END != std::string::npos) {
            size_t content_length = 0;
            if (!parse_content_length(request.substr(0, HEADER_END), content_length)) {
                restore_fd_flags(fd, old_flags);
                return false;
            }

            size_t const HEADER_SIZE = HEADER_END + HEADER_TERMINATOR.size();
            if (HEADER_SIZE > MAX_REQUEST_BYTES || content_length > MAX_REQUEST_BYTES - HEADER_SIZE) {
                errno = EMSGSIZE;
                restore_fd_flags(fd, old_flags);
                return false;
            }

            size_t const EXPECTED_SIZE = HEADER_SIZE + content_length;
            if (request.size() >= EXPECTED_SIZE) {
                request.resize(EXPECTED_SIZE);
                restore_fd_flags(fd, old_flags);
                return true;
            }
        }

        if (request.size() >= MAX_REQUEST_BYTES) {
            errno = EMSGSIZE;
            restore_fd_flags(fd, old_flags);
            return false;
        }

        size_t const ROOM = MAX_REQUEST_BYTES - request.size();
        size_t const CHUNK_LEN = std::min(buffer.size(), ROOM);
        if (wait_fd_ready_until(fd, POLLIN, DEADLINE_MS, timeout_ms) <= 0) {
            restore_fd_flags(fd, old_flags);
            return false;
        }

        errno = 0;
        ssize_t const RECEIVED = recv(fd, buffer.data(), CHUNK_LEN, 0);
        if (RECEIVED > 0) {
            request.append(buffer.data(), static_cast<size_t>(RECEIVED));
            continue;
        }
        if (RECEIVED == 0) {
            errno = ECONNRESET;
            restore_fd_flags(fd, old_flags);
            return false;
        }
        if (retryable_socket_result(RECEIVED)) {
            continue;
        }

        restore_fd_flags(fd, old_flags);
        return false;
    }
}

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

// Send an HTTP response with custom headers
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

    // Send header (using send_all to handle partial sends)
    ssize_t sent = send_all(client_fd, HEADER.data(), HEADER.size());
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
        send_all(client_fd, HTTP_404_RESPONSE.data(), HTTP_404_RESPONSE.size());
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
        send_all(client_fd, HTTP_403_RESPONSE.data(), HTTP_403_RESPONSE.size());
        return false;
    }

    // Read file
    int const FD = open(fs_path.c_str(), O_RDONLY);
    if (FD < 0) {
        log_message("httpd[t:{},p:{}]: Failed to open file: {}", tid, pid, fs_path);
        send_all(client_fd, HTTP_500_RESPONSE.data(), HTTP_500_RESPONSE.size());
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

// Parse HTTP request to extract the path without query parameters
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

    auto target = first_line.substr(first_space + 1, second_space - first_space - 1);
    auto query_start = target.find('?');
    if (query_start != std::string_view::npos) {
        return target.substr(0, query_start);
    }
    return target;
}

// Extract POST body from HTTP request (content after \r\n\r\n)
auto parse_request_body(std::string_view request) -> std::string_view {
    auto body_start = request.find(HEADER_TERMINATOR);
    if (body_start == std::string_view::npos) {
        return {};
    }
    return request.substr(body_start + HEADER_TERMINATOR.size());
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
            return url_decode(body.substr(pos, end - pos), true);
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
        send_all(client_fd, HTTP_403_RESPONSE.data(), HTTP_403_RESPONSE.size());
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

        // Parent: wait for mount to complete without wedging the request loop forever.
        int32_t exit_code = 0;
        bool const MOUNT_COMPLETED = wait_for_child_timeout(static_cast<int64_t>(exec_res), &exit_code, MOUNT_CHILD_TIMEOUT_MS);

        if (MOUNT_COMPLETED && exit_code == 0) {
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

    // Map URL path to filesystem path
    std::string fs_path = SERVE_ROOT;
    if (decoded_path.empty() || decoded_path == "/") {
        // Root - serve from SERVE_ROOT
    } else {
        if (decoded_path.front() != '/') {
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

// std::format can theoretically throw, but WOS service entry points use the normal process boundary for fatal failures.
// NOLINTNEXTLINE(bugprone-exception-escape)
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
    set_fd_cloexec_best_effort(server_fd);

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
    for (;;) {
        struct sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        // Accept connection
        int client_fd = accept(server_fd, reinterpret_cast<struct sockaddr*>(&client_addr), &client_len);
        if (client_fd < 0) {
            log_message("httpd[t:{},p:{}]: Failed to accept connection: {}", tid, pid, client_fd);
            continue;
        }
        set_fd_cloexec_best_effort(client_fd);

        // Get client IP
        std::array<char, INET_ADDRSTRLEN> client_ip{};
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip.data(), client_ip.size());
        uint16_t const CLIENT_PORT = ntohs(client_addr.sin_port);

        log_message("httpd[t:{},p:{}]: Accepted connection from {}:{}", tid, pid, std::string_view(client_ip.data()), CLIENT_PORT);

        // Read request
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

        // Handle request and send response
        handle_request(client_fd, REQUEST);

        // Graceful close: shut down the write side first so the client
        // receives FIN, then drain any unread data the client may have
        // sent (e.g. the browser may pipeline or send trailing bytes).
        // Without this, close() on a socket with unread data sends RST
        // instead of FIN, causing NS_ERROR_NET_RESET in the browser.
        shutdown(client_fd, SHUT_WR);
        drain_client_input_timeout(client_fd, CLIENT_DRAIN_TIMEOUT_MS);
        close(client_fd);
        log_message("httpd[t:{},p:{}]: Connection closed", tid, pid);
    }

    // Cleanup (unreachable in practice)
    close(server_fd);
    return 0;
}
