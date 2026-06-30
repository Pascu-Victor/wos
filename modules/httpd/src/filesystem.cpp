#include "httpd/filesystem.hpp"

#include <abi-bits/stat.h>
#include <bits/posix/stat.h>
#include <bits/ssize_t.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <format>
#include <string>
#include <string_view>
#include <vector>

#include "httpd/config.hpp"
#include "httpd/log.hpp"
#include "httpd/response.hpp"
#include "httpd/socket_io.hpp"

namespace httpd {

auto get_mime_type(std::string_view path) -> const char* {
    auto dot_pos = path.rfind('.');
    if (dot_pos == std::string_view::npos) {
        return "application/octet-stream";
    }
    auto ext = path.substr(dot_pos);

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

auto generate_directory_listing(const std::string& fs_path, std::string_view url_path) -> std::string {
    std::string html;
    html.reserve(8192);

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

        std::ranges::sort(entries, [](const DirEntry& a, const DirEntry& b) {
            if (a.is_dir != b.is_dir) {
                return a.is_dir > b.is_dir;
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
            {
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

auto serve_file(int client_fd, const std::string& fs_path, std::string_view url_path, uint64_t tid, uint64_t pid) -> bool {
    struct stat st{};
    if (stat(fs_path.c_str(), &st) != 0) {
        log_message("httpd[t:{},p:{}]: File not found: {}", tid, pid, fs_path);
        send_all(client_fd, HTTP_404_RESPONSE.data(), HTTP_404_RESPONSE.size());
        return false;
    }

    if (S_ISDIR(st.st_mode)) {
        std::string index_path = fs_path;
        if (index_path.back() != '/') {
            index_path += '/';
        }
        index_path += "index.html";

        struct stat index_st{};
        if (stat(index_path.c_str(), &index_st) == 0 && S_ISREG(index_st.st_mode)) {
            return serve_file(client_fd, index_path, url_path, tid, pid);
        }

        std::string listing = generate_directory_listing(fs_path, url_path);
        send_response(client_fd, 200, "OK", "text/html; charset=utf-8", listing.data(), listing.size());
        log_message("httpd[t:{},p:{}]: Served directory listing: {}", tid, pid, fs_path);
        return true;
    }

    if (!S_ISREG(st.st_mode)) {
        send_all(client_fd, HTTP_403_RESPONSE.data(), HTTP_403_RESPONSE.size());
        return false;
    }

    int const FD = open(fs_path.c_str(), O_RDONLY);
    if (FD < 0) {
        log_message("httpd[t:{},p:{}]: Failed to open file: {}", tid, pid, fs_path);
        send_all(client_fd, HTTP_500_RESPONSE.data(), HTTP_500_RESPONSE.size());
        return false;
    }

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

}  // namespace httpd
