#define _DEFAULT_SOURCE 1
#include <arpa/inet.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/logging.h>
#include <sys/multiproc.h>
#include <sys/process.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <format>
#include <fstream>
#include <print>
#include <string>
#include <string_view>
#include <vector>

namespace {

constexpr uint16_t HTTP_PORT = 80;
constexpr const char* LOG_FILE = "/mnt/disk/httpd.log";
constexpr const char* SERVE_ROOT = "/mnt/disk/srv";

// Logging utility
template <typename... Args>
void log_message(std::format_string<Args...> fmt, Args&&... args) {
    std::ofstream log_stream(LOG_FILE, std::ios::app | std::ios::out);
    if (log_stream.is_open()) {
        log_stream << std::format(fmt, std::forward<Args>(args)...) << '\n';
        log_stream.close();
    }
}
constexpr size_t BUFFER_SIZE = 4096;
constexpr size_t MAX_FILE_SIZE = static_cast<const size_t>(1024 * 1024);  // 1MB max file size
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
            long val = strtol(hex.data(), &end, 16);
            if (end == hex.data() + 2) {
                result += static_cast<char>(val);
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
    return path.find("..") == std::string_view::npos;
}

// Format file size for display
auto format_size(size_t size) -> std::string {
    if (size < 1024) {
        return std::format("{} B", size);
    }
    if (size < static_cast<size_t>(1024 * 1024)) {
        return std::format("{:.1f} KB", static_cast<double>(size) / 1024.0);
    }
    return std::format("{:.1f} MB", static_cast<double>(size) / (1024.0 * 1024.0));
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
    html += "<tr><th>Name</th><th>Size</th><th>Type</th></tr>\r\n";

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
        html += "\">..</a></td><td class='size'>-</td><td class='type'>Parent Directory</td></tr>\r\n";
    }

    // Read directory entries
    DIR* dir = opendir(fs_path.c_str());
    if (dir == nullptr) {
        html += "<tr><td colspan='3'>Error reading directory</td></tr>\r\n";
    } else {
        std::vector<std::pair<std::string, bool>> entries;  // name, is_directory

        struct dirent* entry = nullptr;
        while ((entry = readdir(dir)) != nullptr) {
            std::string name{static_cast<const char*>(entry->d_name)};
            if (name == "." || name == "..") {
                continue;
            }

            // Check if it's a directory
            std::string full_path = fs_path;
            if (full_path.back() != '/') {
                full_path += '/';
            }
            full_path += name;

            struct stat st{};
            bool is_dir = false;
            if (stat(full_path.c_str(), &st) == 0) {
                is_dir = S_ISDIR(st.st_mode);
            }

            entries.emplace_back(name, is_dir);
        }
        closedir(dir);

        // Sort: directories first, then alphabetically
        std::ranges::sort(entries, [](const auto& a, const auto& b) {
            if (a.second != b.second) {
                return a.second > b.second;  // dirs first
            }
            return a.first < b.first;
        });

        for (const auto& [name, is_dir] : entries) {
            std::string full_path = fs_path;
            if (full_path.back() != '/') {
                full_path += '/';
            }
            full_path += name;

            std::string url = std::string(url_path);
            if (url.back() != '/') {
                url += '/';
            }
            url += name;
            if (is_dir) {
                url += '/';
            }

            struct stat st{};
            size_t size = 0;
            if (stat(full_path.c_str(), &st) == 0) {
                size = st.st_size;
            }

            html += "<tr><td><span class='icon'>";
            html += is_dir ? "📁" : "📄";
            html += "</span><a href=\"";
            html += url;
            html += "\">";
            html += name;
            if (is_dir) {
                html += "/";
            }
            html += "</a></td><td class='size'>";
            html += is_dir ? "-" : format_size(size);
            html += "</td><td class='type'>";
            html += is_dir ? "Directory" : get_mime_type(name);
            html += "</td></tr>\r\n";
        }
    }

    html += "</table>\r\n";
    html += "<hr>\r\n";
    html += "<p><em>WOS-httpd/1.0</em></p>\r\n";
    html += "</body>\r\n</html>\r\n";

    return html;
}

// Send an HTTP response with custom headers
auto send_response(int client_fd, int status_code, const char* status_text, const char* content_type, const void* body, size_t body_len)
    -> ssize_t {
    // Build header
    std::array<char, 512> header{};
    int header_len = snprintf(header.data(), header.size(),
                              "HTTP/1.1 %d %s\r\n"
                              "Content-Type: %s\r\n"
                              "Content-Length: %zu\r\n"
                              "Connection: close\r\n"
                              "Server: WOS-httpd/1.0\r\n"
                              "\r\n",
                              status_code, status_text, content_type, body_len);

    // Send header
    ssize_t sent = send(client_fd, header.data(), header_len, 0);
    if (sent < 0) {
        return sent;
    }

    // Send body
    if (body != nullptr && body_len > 0) {
        sent = send(client_fd, body, body_len, 0);
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

    // Check file size
    if (static_cast<size_t>(st.st_size) > MAX_FILE_SIZE) {
        log_message("httpd[t:{},p:{}]: File too large: {} ({} bytes)", tid, pid, fs_path, st.st_size);
        send(client_fd, HTTP_500_RESPONSE.data(), HTTP_500_RESPONSE.size(), 0);
        return false;
    }

    // Read file
    int fd = open(fs_path.c_str(), O_RDONLY);
    if (fd < 0) {
        log_message("httpd[t:{},p:{}]: Failed to open file: {}", tid, pid, fs_path);
        send(client_fd, HTTP_500_RESPONSE.data(), HTTP_500_RESPONSE.size(), 0);
        return false;
    }

    std::vector<char> file_buf(st.st_size);
    ssize_t bytes_read = read(fd, file_buf.data(), st.st_size);
    close(fd);

    if (bytes_read < 0) {
        log_message("httpd[t:{},p:{}]: Failed to read file: {}", tid, pid, fs_path);
        send(client_fd, HTTP_500_RESPONSE.data(), HTTP_500_RESPONSE.size(), 0);
        return false;
    }

    // Get MIME type and send response
    const char* mime_type = get_mime_type(fs_path);
    send_response(client_fd, 200, "OK", mime_type, file_buf.data(), bytes_read);
    log_message("httpd[t:{},p:{}]: Served file: {} ({} bytes, {})", tid, pid, fs_path, bytes_read, mime_type);
    return true;
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
        int body_len = snprintf(info_body.data(), info_body.size(),
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
        send_response(client_fd, 200, "OK", "text/html; charset=utf-8", info_body.data(), body_len);
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
    bool had_trailing_slash = !fs_path.empty() && fs_path.back() == '/';
    while (fs_path.size() > 1 && fs_path.back() == '/') {
        fs_path.pop_back();
    }

    // Ensure URL path for directory listing has trailing slash
    std::string url_for_listing = decoded_path;
    if (had_trailing_slash && !url_for_listing.empty() && url_for_listing.back() != '/') {
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
    std::println("httpd[t:{},p:{}]: Starting HTTP server on 0.0.0.0:{}", tid, pid, HTTP_PORT);

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
    std::array<char, BUFFER_SIZE> buffer{};

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

        log_message("httpd[t:{},p:{}]: Accepted connection from {}:{}", tid, pid, client_ip, client_port);

        // Read request
        ssize_t received = recv(client_fd, buffer.data(), BUFFER_SIZE - 1, 0);
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
        std::string_view request(buffer.data(), received);

        log_message("httpd[t:{},p:{}]: Received {} bytes from {}:{}", tid, pid, received, client_ip, client_port);

        // Handle request and send response
        handle_request(client_fd, request);

        // Close connection
        close(client_fd);
        log_message("httpd[t:{},p:{}]: Connection closed", tid, pid);
    }

    // Cleanup (unreachable in practice)
    close(server_fd);
    return 0;
}
