#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

namespace {

constexpr uint32_t MAX_PACKET_SIZE = 16U * 1024U * 1024U;
constexpr uint32_t MAX_READ_SIZE = 256U * 1024U;
constexpr uint32_t READDIR_BATCH = 64;
constexpr uint64_t WRITE_RESERVE_CHUNK = 8ULL * 1024ULL * 1024ULL;

enum PacketType : uint8_t {
    SSH_FXP_INIT = 1,
    SSH_FXP_VERSION = 2,
    SSH_FXP_OPEN = 3,
    SSH_FXP_CLOSE = 4,
    SSH_FXP_READ = 5,
    SSH_FXP_WRITE = 6,
    SSH_FXP_LSTAT = 7,
    SSH_FXP_FSTAT = 8,
    SSH_FXP_SETSTAT = 9,
    SSH_FXP_FSETSTAT = 10,
    SSH_FXP_OPENDIR = 11,
    SSH_FXP_READDIR = 12,
    SSH_FXP_REMOVE = 13,
    SSH_FXP_MKDIR = 14,
    SSH_FXP_RMDIR = 15,
    SSH_FXP_REALPATH = 16,
    SSH_FXP_STAT = 17,
    SSH_FXP_RENAME = 18,
    SSH_FXP_READLINK = 19,
    SSH_FXP_SYMLINK = 20,
    SSH_FXP_STATUS = 101,
    SSH_FXP_HANDLE = 102,
    SSH_FXP_DATA = 103,
    SSH_FXP_NAME = 104,
    SSH_FXP_ATTRS = 105,
    SSH_FXP_EXTENDED = 200,
};

enum StatusCode : uint32_t {
    SSH_FX_OK = 0,
    SSH_FX_EOF = 1,
    SSH_FX_NO_SUCH_FILE = 2,
    SSH_FX_PERMISSION_DENIED = 3,
    SSH_FX_FAILURE = 4,
    SSH_FX_BAD_MESSAGE = 5,
    SSH_FX_OP_UNSUPPORTED = 8,
};

enum AttrFlags : uint32_t {
    SSH_FILEXFER_ATTR_SIZE = 0x00000001,
    SSH_FILEXFER_ATTR_UIDGID = 0x00000002,
    SSH_FILEXFER_ATTR_PERMISSIONS = 0x00000004,
    SSH_FILEXFER_ATTR_ACMODTIME = 0x00000008,
    SSH_FILEXFER_ATTR_EXTENDED = 0x80000000,
};

enum OpenFlags : uint32_t {
    SSH_FXF_READ = 0x00000001,
    SSH_FXF_WRITE = 0x00000002,
    SSH_FXF_APPEND = 0x00000004,
    SSH_FXF_CREAT = 0x00000008,
    SSH_FXF_TRUNC = 0x00000010,
    SSH_FXF_EXCL = 0x00000020,
};

struct Attributes {
    uint32_t flags = 0;
    uint64_t size = 0;
    uint32_t uid = 0;
    uint32_t gid = 0;
    uint32_t permissions = 0;
    uint32_t atime = 0;
    uint32_t mtime = 0;
};

struct DirEntry {
    std::string name;
    std::string longname;
    Attributes attrs;
};

struct FileHandle {
    int fd = -1;
    uint64_t logical_size = 0;
    uint64_t reserved_size = 0;
    bool reserve_writes = false;
    bool sendfile_reads = false;
};

struct DirectoryHandle {
    std::vector<DirEntry> entries;
    size_t index = 0;
};

using Handle = std::variant<FileHandle, DirectoryHandle>;

struct PacketReader {
    std::span<const uint8_t> data;
    size_t pos = 0;
    bool ok = true;

    auto u8() -> uint8_t {
        if (pos + 1 > data.size()) {
            ok = false;
            return 0;
        }
        return data[pos++];
    }

    auto u32() -> uint32_t {
        if (pos + 4 > data.size()) {
            ok = false;
            return 0;
        }
        uint32_t value = (static_cast<uint32_t>(data[pos]) << 24) | (static_cast<uint32_t>(data[pos + 1]) << 16) |
                         (static_cast<uint32_t>(data[pos + 2]) << 8) | static_cast<uint32_t>(data[pos + 3]);
        pos += 4;
        return value;
    }

    auto u64() -> uint64_t {
        uint64_t high = u32();
        uint64_t low = u32();
        return (high << 32) | low;
    }

    auto byte_span() -> std::span<const uint8_t> {
        uint32_t len = u32();
        if (!ok || pos + len > data.size()) {
            ok = false;
            return {};
        }
        std::span<const uint8_t> out = data.subspan(pos, len);
        pos += len;
        return out;
    }

    auto bytes() -> std::vector<uint8_t> {
        std::span<const uint8_t> raw = byte_span();
        return {raw.begin(), raw.end()};
    }

    auto string() -> std::string {
        std::span<const uint8_t> raw = byte_span();
        if (raw.empty()) {
            return {};
        }
        return std::string(reinterpret_cast<const char*>(raw.data()), raw.size());
    }

    auto attrs() -> Attributes {
        Attributes out{};
        out.flags = u32();
        if ((out.flags & SSH_FILEXFER_ATTR_SIZE) != 0) {
            out.size = u64();
        }
        if ((out.flags & SSH_FILEXFER_ATTR_UIDGID) != 0) {
            out.uid = u32();
            out.gid = u32();
        }
        if ((out.flags & SSH_FILEXFER_ATTR_PERMISSIONS) != 0) {
            out.permissions = u32();
        }
        if ((out.flags & SSH_FILEXFER_ATTR_ACMODTIME) != 0) {
            out.atime = u32();
            out.mtime = u32();
        }
        if ((out.flags & SSH_FILEXFER_ATTR_EXTENDED) != 0) {
            uint32_t count = u32();
            for (uint32_t i = 0; ok && i < count; ++i) {
                (void)string();
                (void)string();
            }
        }
        return out;
    }
};

struct PacketWriter {
    std::vector<uint8_t> body;

    explicit PacketWriter(uint8_t type) { u8(type); }

    void u8(uint8_t value) { body.push_back(value); }

    void u32(uint32_t value) {
        body.push_back(static_cast<uint8_t>((value >> 24) & 0xffU));
        body.push_back(static_cast<uint8_t>((value >> 16) & 0xffU));
        body.push_back(static_cast<uint8_t>((value >> 8) & 0xffU));
        body.push_back(static_cast<uint8_t>(value & 0xffU));
    }

    void u64(uint64_t value) {
        u32(static_cast<uint32_t>(value >> 32));
        u32(static_cast<uint32_t>(value & 0xffffffffULL));
    }

    void string(std::string_view value) {
        u32(static_cast<uint32_t>(value.size()));
        body.insert(body.end(), value.begin(), value.end());
    }

    void bytes(std::span<const uint8_t> value) {
        u32(static_cast<uint32_t>(value.size()));
        body.insert(body.end(), value.begin(), value.end());
    }

    void attrs(const Attributes& value) {
        u32(value.flags & ~SSH_FILEXFER_ATTR_EXTENDED);
        if ((value.flags & SSH_FILEXFER_ATTR_SIZE) != 0) {
            u64(value.size);
        }
        if ((value.flags & SSH_FILEXFER_ATTR_UIDGID) != 0) {
            u32(value.uid);
            u32(value.gid);
        }
        if ((value.flags & SSH_FILEXFER_ATTR_PERMISSIONS) != 0) {
            u32(value.permissions);
        }
        if ((value.flags & SSH_FILEXFER_ATTR_ACMODTIME) != 0) {
            u32(value.atime);
            u32(value.mtime);
        }
    }
};

auto wait_for_fd(int fd, short events) -> bool {
    pollfd pfd{
        .fd = fd,
        .events = events,
        .revents = 0,
    };

    while (true) {
        int rc = poll(&pfd, 1, -1);
        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (rc == 0) {
            continue;
        }
        return (pfd.revents & (events | POLLHUP | POLLERR)) != 0;
    }
}

auto write_all(int fd, const void* data, size_t size) -> bool {
    const auto* bytes = static_cast<const uint8_t*>(data);
    size_t done = 0;
    while (done < size) {
        ssize_t written = write(fd, bytes + done, size - done);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (wait_for_fd(fd, POLLOUT)) {
                    continue;
                }
            }
            return false;
        }
        if (written == 0) {
            return false;
        }
        done += static_cast<size_t>(written);
    }
    return true;
}

auto read_all(int fd, void* data, size_t size) -> bool {
    auto* bytes = static_cast<uint8_t*>(data);
    size_t done = 0;
    while (done < size) {
        ssize_t count = read(fd, bytes + done, size - done);
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (wait_for_fd(fd, POLLIN)) {
                    continue;
                }
            }
            return false;
        }
        if (count == 0) {
            return false;
        }
        done += static_cast<size_t>(count);
    }
    return true;
}

auto read_packet(std::vector<uint8_t>& out) -> bool {
    std::array<uint8_t, 4> len_bytes{};
    if (!read_all(STDIN_FILENO, len_bytes.data(), len_bytes.size())) {
        return false;
    }
    uint32_t len = (static_cast<uint32_t>(len_bytes[0]) << 24) | (static_cast<uint32_t>(len_bytes[1]) << 16) |
                   (static_cast<uint32_t>(len_bytes[2]) << 8) | static_cast<uint32_t>(len_bytes[3]);
    if (len == 0 || len > MAX_PACKET_SIZE) {
        return false;
    }
    out.resize(len);
    return read_all(STDIN_FILENO, out.data(), out.size());
}

auto send_packet(const PacketWriter& packet) -> bool {
    uint32_t len = static_cast<uint32_t>(packet.body.size());
    std::array<uint8_t, 4> len_bytes = {
        static_cast<uint8_t>((len >> 24) & 0xffU),
        static_cast<uint8_t>((len >> 16) & 0xffU),
        static_cast<uint8_t>((len >> 8) & 0xffU),
        static_cast<uint8_t>(len & 0xffU),
    };
    return write_all(STDOUT_FILENO, len_bytes.data(), len_bytes.size()) && write_all(STDOUT_FILENO, packet.body.data(), packet.body.size());
}

auto send_data_packet(uint32_t id, std::span<const uint8_t> data) -> bool {
    uint32_t len = static_cast<uint32_t>(1U + 4U + 4U + data.size());
    std::array<uint8_t, 13> header = {
        static_cast<uint8_t>((len >> 24) & 0xffU),
        static_cast<uint8_t>((len >> 16) & 0xffU),
        static_cast<uint8_t>((len >> 8) & 0xffU),
        static_cast<uint8_t>(len & 0xffU),
        SSH_FXP_DATA,
        static_cast<uint8_t>((id >> 24) & 0xffU),
        static_cast<uint8_t>((id >> 16) & 0xffU),
        static_cast<uint8_t>((id >> 8) & 0xffU),
        static_cast<uint8_t>(id & 0xffU),
        static_cast<uint8_t>((data.size() >> 24) & 0xffU),
        static_cast<uint8_t>((data.size() >> 16) & 0xffU),
        static_cast<uint8_t>((data.size() >> 8) & 0xffU),
        static_cast<uint8_t>(data.size() & 0xffU),
    };
    return write_all(STDOUT_FILENO, header.data(), header.size()) && write_all(STDOUT_FILENO, data.data(), data.size());
}

auto send_file_data_packet(uint32_t id, int fd, uint64_t offset, size_t size) -> bool {
    auto const LEN = static_cast<uint32_t>(1U + 4U + 4U + size);
    std::array<uint8_t, 13> header = {
        static_cast<uint8_t>((LEN >> 24) & 0xffU),
        static_cast<uint8_t>((LEN >> 16) & 0xffU),
        static_cast<uint8_t>((LEN >> 8) & 0xffU),
        static_cast<uint8_t>(LEN & 0xffU),
        SSH_FXP_DATA,
        static_cast<uint8_t>((id >> 24) & 0xffU),
        static_cast<uint8_t>((id >> 16) & 0xffU),
        static_cast<uint8_t>((id >> 8) & 0xffU),
        static_cast<uint8_t>(id & 0xffU),
        static_cast<uint8_t>((size >> 24) & 0xffU),
        static_cast<uint8_t>((size >> 16) & 0xffU),
        static_cast<uint8_t>((size >> 8) & 0xffU),
        static_cast<uint8_t>(size & 0xffU),
    };
    if (!write_all(STDOUT_FILENO, header.data(), header.size())) {
        return false;
    }

    auto pos = static_cast<off_t>(offset);
    size_t done = 0;
    while (done < size) {
        ssize_t sent = sendfile(STDOUT_FILENO, fd, &pos, size - done);
        if (sent < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (wait_for_fd(STDOUT_FILENO, POLLOUT)) {
                    continue;
                }
            }
            return false;
        }
        if (sent == 0) {
            return false;
        }
        done += static_cast<size_t>(sent);
    }
    return true;
}

auto status_from_errno(int err) -> uint32_t {
    switch (err) {
        case 0:
            return SSH_FX_OK;
        case ENOENT:
        case ENOTDIR:
            return SSH_FX_NO_SUCH_FILE;
        case EACCES:
        case EPERM:
            return SSH_FX_PERMISSION_DENIED;
        default:
            return SSH_FX_FAILURE;
    }
}

auto status_text(uint32_t status) -> std::string_view {
    switch (status) {
        case SSH_FX_OK:
            return "OK";
        case SSH_FX_EOF:
            return "End of file";
        case SSH_FX_NO_SUCH_FILE:
            return "No such file";
        case SSH_FX_PERMISSION_DENIED:
            return "Permission denied";
        case SSH_FX_BAD_MESSAGE:
            return "Bad message";
        case SSH_FX_OP_UNSUPPORTED:
            return "Operation unsupported";
        default:
            return "Failure";
    }
}

void send_status(uint32_t id, uint32_t status) {
    PacketWriter out(SSH_FXP_STATUS);
    out.u32(id);
    out.u32(status);
    out.string(status_text(status));
    out.string("");
    (void)send_packet(out);
}

void send_errno(uint32_t id, int err) { send_status(id, status_from_errno(err)); }

auto attrs_from_stat(const struct stat& st) -> Attributes {
    Attributes attrs{};
    attrs.flags = SSH_FILEXFER_ATTR_SIZE | SSH_FILEXFER_ATTR_UIDGID | SSH_FILEXFER_ATTR_PERMISSIONS | SSH_FILEXFER_ATTR_ACMODTIME;
    attrs.size = static_cast<uint64_t>(st.st_size);
    attrs.uid = static_cast<uint32_t>(st.st_uid);
    attrs.gid = static_cast<uint32_t>(st.st_gid);
    attrs.permissions = static_cast<uint32_t>(st.st_mode);
    attrs.atime = static_cast<uint32_t>(st.st_atim.tv_sec);
    attrs.mtime = static_cast<uint32_t>(st.st_mtim.tv_sec);
    return attrs;
}

auto mode_string(mode_t mode) -> std::string {
    std::string out(10, '-');
    if (S_ISDIR(mode)) {
        out[0] = 'd';
    } else if (S_ISLNK(mode)) {
        out[0] = 'l';
    } else if (S_ISCHR(mode)) {
        out[0] = 'c';
    } else if (S_ISBLK(mode)) {
        out[0] = 'b';
    } else if (S_ISFIFO(mode)) {
        out[0] = 'p';
    } else if (S_ISSOCK(mode)) {
        out[0] = 's';
    }

    constexpr std::array<std::pair<mode_t, char>, 9> BITS = {{
        {S_IRUSR, 'r'},
        {S_IWUSR, 'w'},
        {S_IXUSR, 'x'},
        {S_IRGRP, 'r'},
        {S_IWGRP, 'w'},
        {S_IXGRP, 'x'},
        {S_IROTH, 'r'},
        {S_IWOTH, 'w'},
        {S_IXOTH, 'x'},
    }};

    for (size_t i = 0; i < BITS.size(); ++i) {
        if ((mode & BITS[i].first) != 0) {
            out[i + 1] = BITS[i].second;
        }
    }
    return out;
}

auto make_longname(std::string_view name, const struct stat& st) -> std::string {
    std::array<char, 512> buffer{};
    std::string modes = mode_string(st.st_mode);
    int written = snprintf(buffer.data(), buffer.size(), "%s %3lu %-8u %-8u %12lld Jan  1  1970 %.*s", modes.c_str(),
                           static_cast<unsigned long>(st.st_nlink), static_cast<unsigned>(st.st_uid), static_cast<unsigned>(st.st_gid),
                           static_cast<long long>(st.st_size), static_cast<int>(name.size()), name.data());
    if (written < 0) {
        return std::string{name};
    }
    if (static_cast<size_t>(written) >= buffer.size()) {
        return std::string(buffer.data(), buffer.size() - 1);
    }
    return std::string(buffer.data(), static_cast<size_t>(written));
}

auto has_nul(std::string_view path) -> bool { return std::find(path.begin(), path.end(), '\0') != path.end(); }

auto round_up(uint64_t value, uint64_t alignment) -> uint64_t {
    if (alignment == 0) {
        return value;
    }
    uint64_t const REMAINDER = value % alignment;
    if (REMAINDER == 0) {
        return value;
    }
    return value + (alignment - REMAINDER);
}

auto get_cwd() -> std::string {
    std::array<char, PATH_MAX> buffer{};
    if (getcwd(buffer.data(), buffer.size()) == nullptr) {
        return "/";
    }
    return buffer.data();
}

auto normalize_path(std::string_view input) -> std::string {
    std::string path;
    if (input.empty()) {
        input = ".";
    }

    if (input.front() == '/') {
        path.assign(input);
    } else {
        path = get_cwd();
        if (path.empty() || path.back() != '/') {
            path.push_back('/');
        }
        path.append(input);
    }

    std::vector<std::string> parts;
    size_t start = 0;
    while (start <= path.size()) {
        size_t slash = path.find('/', start);
        std::string_view part(path.data() + start, (slash == std::string::npos ? path.size() : slash) - start);
        if (part.empty() || part == ".") {
            // Skip.
        } else if (part == "..") {
            if (!parts.empty()) {
                parts.pop_back();
            }
        } else {
            parts.emplace_back(part);
        }
        if (slash == std::string::npos) {
            break;
        }
        start = slash + 1;
    }

    std::string out = "/";
    for (size_t i = 0; i < parts.size(); ++i) {
        if (i != 0) {
            out.push_back('/');
        }
        out += parts[i];
    }
    return out;
}

auto join_path(std::string_view base, std::string_view name) -> std::string {
    if (base == "/") {
        std::string out = "/";
        out.append(name);
        return out;
    }
    std::string out(base);
    if (out.empty() || out.back() != '/') {
        out.push_back('/');
    }
    out.append(name);
    return out;
}

auto path_for_client(std::string path) -> std::string {
    if (path.empty()) {
        return ".";
    }
    return path;
}

void send_handle(uint32_t id, char kind, uint32_t handle_id) {
    PacketWriter out(SSH_FXP_HANDLE);
    out.u32(id);
    std::string encoded;
    encoded.push_back(kind);
    encoded += std::to_string(handle_id);
    out.string(encoded);
    (void)send_packet(out);
}

auto decode_handle(std::string_view encoded, char expected_kind, uint32_t& id) -> bool {
    if (encoded.size() < 2 || encoded.front() != expected_kind) {
        return false;
    }
    uint32_t value = 0;
    for (size_t i = 1; i < encoded.size(); ++i) {
        char ch = encoded[i];
        if (ch < '0' || ch > '9') {
            return false;
        }
        uint32_t digit = static_cast<uint32_t>(ch - '0');
        if (value > (UINT32_MAX - digit) / 10U) {
            return false;
        }
        value = value * 10U + digit;
    }
    id = value;
    return true;
}

void send_attrs(uint32_t id, const Attributes& attrs) {
    PacketWriter out(SSH_FXP_ATTRS);
    out.u32(id);
    out.attrs(attrs);
    (void)send_packet(out);
}

void send_name(uint32_t id, std::span<const DirEntry> entries) {
    PacketWriter out(SSH_FXP_NAME);
    out.u32(id);
    out.u32(static_cast<uint32_t>(entries.size()));
    for (const auto& entry : entries) {
        out.string(entry.name);
        out.string(entry.longname);
        out.attrs(entry.attrs);
    }
    (void)send_packet(out);
}

auto stat_entry(std::string_view path, std::string_view name) -> DirEntry {
    DirEntry entry{};
    entry.name = name;
    struct stat st{};
    std::string full_path = join_path(path, name);
    if (lstat(full_path.c_str(), &st) == 0) {
        entry.attrs = attrs_from_stat(st);
        entry.longname = make_longname(name, st);
    } else {
        entry.longname = std::string{name};
    }
    return entry;
}

// Some SFTP clients upload to a partial path and then rename over an existing
// symlink. If that symlink is dangling, retry by replacing the link itself.
auto retry_rename_over_symlink(const std::string& old_path, const std::string& new_path, int original_error) -> int {
    switch (original_error) {
        case EEXIST:
        case EISDIR:
        case ELOOP:
        case ENOENT:
        case ENOTDIR:
            break;
        default:
            return original_error;
    }

    struct stat old_st{};
    struct stat new_st{};
    if (lstat(old_path.c_str(), &old_st) != 0 || lstat(new_path.c_str(), &new_st) != 0) {
        return original_error;
    }
    if ((!S_ISREG(old_st.st_mode) && !S_ISLNK(old_st.st_mode)) || !S_ISLNK(new_st.st_mode)) {
        return original_error;
    }

    std::array<char, PATH_MAX> target{};
    ssize_t target_len = readlink(new_path.c_str(), target.data(), target.size() - 1);
    std::string old_target;
    if (target_len >= 0) {
        old_target.assign(target.data(), static_cast<size_t>(target_len));
    }

    if (unlink(new_path.c_str()) != 0) {
        return original_error;
    }
    if (rename(old_path.c_str(), new_path.c_str()) == 0) {
        return 0;
    }

    int const RETRY_ERROR = errno;
    if (!old_target.empty()) {
        (void)symlink(old_target.c_str(), new_path.c_str());
    }
    return RETRY_ERROR;
}

auto apply_path_attrs(const std::string& path, const Attributes& attrs) -> int {
    if ((attrs.flags & SSH_FILEXFER_ATTR_PERMISSIONS) != 0 && chmod(path.c_str(), static_cast<mode_t>(attrs.permissions)) != 0) {
        return errno;
    }
    if ((attrs.flags & SSH_FILEXFER_ATTR_UIDGID) != 0 &&
        chown(path.c_str(), static_cast<uid_t>(attrs.uid), static_cast<gid_t>(attrs.gid)) != 0) {
        return errno;
    }
    if ((attrs.flags & SSH_FILEXFER_ATTR_SIZE) != 0) {
        if (attrs.size > static_cast<uint64_t>(LLONG_MAX)) {
            return EFBIG;
        }
        if (truncate(path.c_str(), static_cast<off_t>(attrs.size)) != 0) {
            return errno;
        }
    }
    if ((attrs.flags & SSH_FILEXFER_ATTR_ACMODTIME) != 0) {
        std::array<timespec, 2> times = {{
            {.tv_sec = static_cast<time_t>(attrs.atime), .tv_nsec = 0},
            {.tv_sec = static_cast<time_t>(attrs.mtime), .tv_nsec = 0},
        }};
        if (utimensat(AT_FDCWD, path.c_str(), times.data(), 0) != 0) {
            return errno;
        }
    }
    return 0;
}

auto apply_fd_attrs(int fd, const Attributes& attrs) -> int {
    if ((attrs.flags & SSH_FILEXFER_ATTR_PERMISSIONS) != 0 && fchmod(fd, static_cast<mode_t>(attrs.permissions)) != 0) {
        return errno;
    }
    if ((attrs.flags & SSH_FILEXFER_ATTR_UIDGID) != 0 && fchown(fd, static_cast<uid_t>(attrs.uid), static_cast<gid_t>(attrs.gid)) != 0) {
        return errno;
    }
    if ((attrs.flags & SSH_FILEXFER_ATTR_SIZE) != 0) {
        if (attrs.size > static_cast<uint64_t>(LLONG_MAX)) {
            return EFBIG;
        }
        if (ftruncate(fd, static_cast<off_t>(attrs.size)) != 0) {
            return errno;
        }
    }
    return 0;
}

class Server {
   public:
    ~Server() { close_all_handles(); }

    void run() {
        std::vector<uint8_t> packet;
        if (!read_packet(packet)) {
            return;
        }

        PacketReader init{std::span<const uint8_t>(packet.data(), packet.size())};
        if (init.u8() != SSH_FXP_INIT || !init.ok) {
            return;
        }

        PacketWriter version(SSH_FXP_VERSION);
        version.u32(3);
        if (!send_packet(version)) {
            return;
        }

        while (read_packet(packet)) {
            handle_packet(packet);
        }
    }

   private:
    std::unordered_map<uint32_t, Handle> handles_;
    std::vector<uint8_t> read_buffer_;
    uint32_t next_handle_ = 1;

    auto add_handle(Handle handle) -> uint32_t {
        uint32_t id = next_handle_++;
        if (next_handle_ == 0) {
            next_handle_ = 1;
        }
        handles_.emplace(id, std::move(handle));
        return id;
    }

    auto find_file(std::string_view encoded) -> FileHandle* {
        uint32_t id = 0;
        if (!decode_handle(encoded, 'F', id)) {
            return nullptr;
        }
        auto it = handles_.find(id);
        if (it == handles_.end()) {
            return nullptr;
        }
        return std::get_if<FileHandle>(&it->second);
    }

    auto find_dir(std::string_view encoded) -> DirectoryHandle* {
        uint32_t id = 0;
        if (!decode_handle(encoded, 'D', id)) {
            return nullptr;
        }
        auto it = handles_.find(id);
        if (it == handles_.end()) {
            return nullptr;
        }
        return std::get_if<DirectoryHandle>(&it->second);
    }

    auto close_handle(std::string_view encoded) -> bool {
        if (encoded.size() < 2) {
            return false;
        }
        uint32_t id = 0;
        char kind = encoded.front();
        if ((kind != 'F' && kind != 'D') || !decode_handle(encoded, kind, id)) {
            return false;
        }
        auto it = handles_.find(id);
        if (it == handles_.end()) {
            return false;
        }
        if (auto* file = std::get_if<FileHandle>(&it->second); file != nullptr) {
            if (!close_file(*file)) {
                return false;
            }
        }
        handles_.erase(it);
        return true;
    }

    static auto close_file(FileHandle& file) -> bool {
        if (file.fd < 0) {
            return true;
        }
        bool ok = true;
        if (file.reserve_writes && file.reserved_size != file.logical_size &&
            ftruncate(file.fd, static_cast<off_t>(file.logical_size)) != 0) {
            ok = false;
        }
        if (close(file.fd) != 0) {
            ok = false;
        }
        file.fd = -1;
        return ok;
    }

    void close_all_handles() {
        for (auto& [id, handle] : handles_) {
            (void)id;
            if (auto* file = std::get_if<FileHandle>(&handle); file != nullptr) {
                (void)close_file(*file);
            }
        }
        handles_.clear();
    }

    static auto reserve_for_write(FileHandle& file, uint64_t end_offset) -> int {
        if (!file.reserve_writes || end_offset <= file.reserved_size) {
            return 0;
        }

        uint64_t const TARGET = round_up(end_offset, WRITE_RESERVE_CHUNK);
        if (TARGET < end_offset || TARGET > static_cast<uint64_t>(LLONG_MAX)) {
            return EFBIG;
        }
        if (ftruncate(file.fd, static_cast<off_t>(TARGET)) != 0) {
            return errno;
        }
        file.reserved_size = TARGET;
        return 0;
    }

    void handle_packet(const std::vector<uint8_t>& packet) {
        PacketReader reader{std::span<const uint8_t>(packet.data(), packet.size())};
        uint8_t type = reader.u8();
        uint32_t id = reader.u32();
        if (!reader.ok) {
            send_status(0, SSH_FX_BAD_MESSAGE);
            return;
        }

        switch (type) {
            case SSH_FXP_OPEN:
                handle_open(id, reader);
                break;
            case SSH_FXP_CLOSE:
                handle_close(id, reader);
                break;
            case SSH_FXP_READ:
                handle_read(id, reader);
                break;
            case SSH_FXP_WRITE:
                handle_write(id, reader);
                break;
            case SSH_FXP_LSTAT:
                handle_stat(id, reader, false);
                break;
            case SSH_FXP_STAT:
                handle_stat(id, reader, true);
                break;
            case SSH_FXP_FSTAT:
                handle_fstat(id, reader);
                break;
            case SSH_FXP_SETSTAT:
                handle_setstat(id, reader);
                break;
            case SSH_FXP_FSETSTAT:
                handle_fsetstat(id, reader);
                break;
            case SSH_FXP_OPENDIR:
                handle_opendir(id, reader);
                break;
            case SSH_FXP_READDIR:
                handle_readdir(id, reader);
                break;
            case SSH_FXP_REMOVE:
                handle_remove(id, reader);
                break;
            case SSH_FXP_MKDIR:
                handle_mkdir(id, reader);
                break;
            case SSH_FXP_RMDIR:
                handle_rmdir(id, reader);
                break;
            case SSH_FXP_REALPATH:
                handle_realpath(id, reader);
                break;
            case SSH_FXP_RENAME:
                handle_rename(id, reader);
                break;
            case SSH_FXP_READLINK:
                handle_readlink(id, reader);
                break;
            case SSH_FXP_SYMLINK:
                handle_symlink(id, reader);
                break;
            case SSH_FXP_EXTENDED:
                handle_extended(id, reader);
                break;
            default:
                send_status(id, SSH_FX_OP_UNSUPPORTED);
                break;
        }
    }

    void handle_open(uint32_t id, PacketReader& reader) {
        std::string path = reader.string();
        uint32_t pflags = reader.u32();
        Attributes attrs = reader.attrs();
        if (!reader.ok || has_nul(path)) {
            send_status(id, SSH_FX_BAD_MESSAGE);
            return;
        }

        int flags = 0;
        bool want_read = (pflags & SSH_FXF_READ) != 0;
        bool want_write = (pflags & SSH_FXF_WRITE) != 0;
        if (want_read && want_write) {
            flags |= O_RDWR;
        } else if (want_write) {
            flags |= O_WRONLY;
        } else {
            flags |= O_RDONLY;
        }
        if ((pflags & SSH_FXF_APPEND) != 0) {
            flags |= O_APPEND;
        }
        if ((pflags & SSH_FXF_CREAT) != 0) {
            flags |= O_CREAT;
        }
        if ((pflags & SSH_FXF_TRUNC) != 0) {
            flags |= O_TRUNC;
        }
        if ((pflags & SSH_FXF_EXCL) != 0) {
            flags |= O_EXCL;
        }

        mode_t mode = (attrs.flags & SSH_FILEXFER_ATTR_PERMISSIONS) != 0 ? static_cast<mode_t>(attrs.permissions) : 0644;
        int fd = open(path.c_str(), flags, mode);
        if (fd < 0) {
            send_errno(id, errno);
            return;
        }
        uint64_t logical_size = 0;
        bool sendfile_reads = false;
        struct stat st{};
        if (fstat(fd, &st) == 0) {
            sendfile_reads = want_read && !want_write && S_ISREG(st.st_mode);
            if (st.st_size > 0) {
                logical_size = static_cast<uint64_t>(st.st_size);
            }
        }
        if (want_write && (attrs.flags & SSH_FILEXFER_ATTR_SIZE) != 0) {
            if (attrs.size > static_cast<uint64_t>(LLONG_MAX) || ftruncate(fd, static_cast<off_t>(attrs.size)) != 0) {
                int const SAVED_ERRNO = attrs.size > static_cast<uint64_t>(LLONG_MAX) ? EFBIG : errno;
                close(fd);
                send_errno(id, SAVED_ERRNO);
                return;
            }
            logical_size = attrs.size;
        }
        bool const RESERVE_WRITES = want_write && !want_read && (pflags & SSH_FXF_APPEND) == 0;
        send_handle(id, 'F',
                    add_handle(FileHandle{
                        .fd = fd,
                        .logical_size = logical_size,
                        .reserved_size = logical_size,
                        .reserve_writes = RESERVE_WRITES,
                        .sendfile_reads = sendfile_reads,
                    }));
    }

    void handle_close(uint32_t id, PacketReader& reader) {
        std::string handle = reader.string();
        if (!reader.ok || !close_handle(handle)) {
            send_status(id, SSH_FX_FAILURE);
            return;
        }
        send_status(id, SSH_FX_OK);
    }

    void handle_read(uint32_t id, PacketReader& reader) {
        std::string handle = reader.string();
        uint64_t offset = reader.u64();
        uint32_t len = reader.u32();
        if (!reader.ok) {
            send_status(id, SSH_FX_BAD_MESSAGE);
            return;
        }
        FileHandle* file = find_file(handle);
        if (file == nullptr) {
            send_status(id, SSH_FX_FAILURE);
            return;
        }
        len = std::min(len, MAX_READ_SIZE);
        if (file->sendfile_reads) {
            struct stat st{};
            if (fstat(file->fd, &st) != 0) {
                send_errno(id, errno);
                return;
            }
            if (st.st_size <= 0) {
                send_status(id, SSH_FX_EOF);
                return;
            }
            auto const FILE_SIZE = static_cast<uint64_t>(st.st_size);
            if (offset >= FILE_SIZE) {
                send_status(id, SSH_FX_EOF);
                return;
            }
            uint64_t const AVAILABLE = FILE_SIZE - offset;
            auto const COUNT = static_cast<size_t>(std::min<uint64_t>(len, AVAILABLE));
            (void)send_file_data_packet(id, file->fd, offset, COUNT);
            return;
        }
        read_buffer_.resize(len);
        ssize_t count = pread(file->fd, read_buffer_.data(), read_buffer_.size(), static_cast<off_t>(offset));
        if (count < 0) {
            send_errno(id, errno);
            return;
        }
        if (count == 0) {
            send_status(id, SSH_FX_EOF);
            return;
        }
        (void)send_data_packet(id, std::span<const uint8_t>(read_buffer_.data(), static_cast<size_t>(count)));
    }

    void handle_write(uint32_t id, PacketReader& reader) {
        std::string handle = reader.string();
        uint64_t offset = reader.u64();
        std::span<const uint8_t> data = reader.byte_span();
        if (!reader.ok) {
            send_status(id, SSH_FX_BAD_MESSAGE);
            return;
        }
        FileHandle* file = find_file(handle);
        if (file == nullptr) {
            send_status(id, SSH_FX_FAILURE);
            return;
        }
        uint64_t const END_OFFSET = offset + data.size();
        if (END_OFFSET < offset) {
            send_status(id, SSH_FX_FAILURE);
            return;
        }
        int const RESERVE_ERR = reserve_for_write(*file, END_OFFSET);
        if (RESERVE_ERR != 0) {
            send_errno(id, RESERVE_ERR);
            return;
        }

        size_t done = 0;
        while (done < data.size()) {
            ssize_t written = pwrite(file->fd, data.data() + done, data.size() - done, static_cast<off_t>(offset + done));
            if (written < 0) {
                if (errno == EINTR) {
                    continue;
                }
                send_errno(id, errno);
                return;
            }
            if (written == 0) {
                send_status(id, SSH_FX_FAILURE);
                return;
            }
            done += static_cast<size_t>(written);
        }
        file->logical_size = std::max(file->logical_size, END_OFFSET);
        send_status(id, SSH_FX_OK);
    }

    void handle_stat(uint32_t id, PacketReader& reader, bool follow) {
        std::string path = reader.string();
        if (!reader.ok || has_nul(path)) {
            send_status(id, SSH_FX_BAD_MESSAGE);
            return;
        }
        struct stat st{};
        int rc = follow ? stat(path.c_str(), &st) : lstat(path.c_str(), &st);
        if (rc != 0) {
            send_errno(id, errno);
            return;
        }
        send_attrs(id, attrs_from_stat(st));
    }

    void handle_fstat(uint32_t id, PacketReader& reader) {
        std::string handle = reader.string();
        if (!reader.ok) {
            send_status(id, SSH_FX_BAD_MESSAGE);
            return;
        }
        FileHandle* file = find_file(handle);
        if (file == nullptr) {
            send_status(id, SSH_FX_FAILURE);
            return;
        }
        struct stat st{};
        if (fstat(file->fd, &st) != 0) {
            send_errno(id, errno);
            return;
        }
        Attributes attrs = attrs_from_stat(st);
        if (file->reserve_writes && file->reserved_size != file->logical_size) {
            attrs.size = file->logical_size;
        }
        send_attrs(id, attrs);
    }

    void handle_setstat(uint32_t id, PacketReader& reader) {
        std::string path = reader.string();
        Attributes attrs = reader.attrs();
        if (!reader.ok || has_nul(path)) {
            send_status(id, SSH_FX_BAD_MESSAGE);
            return;
        }
        int err = apply_path_attrs(path, attrs);
        if (err != 0) {
            send_errno(id, err);
            return;
        }
        send_status(id, SSH_FX_OK);
    }

    void handle_fsetstat(uint32_t id, PacketReader& reader) {
        std::string handle = reader.string();
        Attributes attrs = reader.attrs();
        if (!reader.ok) {
            send_status(id, SSH_FX_BAD_MESSAGE);
            return;
        }
        FileHandle* file = find_file(handle);
        if (file == nullptr) {
            send_status(id, SSH_FX_FAILURE);
            return;
        }
        int err = apply_fd_attrs(file->fd, attrs);
        if (err != 0) {
            send_errno(id, err);
            return;
        }
        if ((attrs.flags & SSH_FILEXFER_ATTR_SIZE) != 0) {
            file->logical_size = attrs.size;
            file->reserved_size = attrs.size;
        }
        send_status(id, SSH_FX_OK);
    }

    void handle_opendir(uint32_t id, PacketReader& reader) {
        std::string path = reader.string();
        if (!reader.ok || has_nul(path)) {
            send_status(id, SSH_FX_BAD_MESSAGE);
            return;
        }
        DIR* dir = opendir(path.c_str());
        if (dir == nullptr) {
            send_errno(id, errno);
            return;
        }

        DirectoryHandle handle{};
        while (dirent* ent = readdir(dir)) {
            handle.entries.push_back(stat_entry(path, ent->d_name));
        }
        closedir(dir);
        send_handle(id, 'D', add_handle(std::move(handle)));
    }

    void handle_readdir(uint32_t id, PacketReader& reader) {
        std::string handle = reader.string();
        if (!reader.ok) {
            send_status(id, SSH_FX_BAD_MESSAGE);
            return;
        }
        DirectoryHandle* dir = find_dir(handle);
        if (dir == nullptr) {
            send_status(id, SSH_FX_FAILURE);
            return;
        }
        if (dir->index >= dir->entries.size()) {
            send_status(id, SSH_FX_EOF);
            return;
        }
        size_t begin = dir->index;
        size_t end = std::min(dir->entries.size(), begin + READDIR_BATCH);
        dir->index = end;
        send_name(id, std::span<const DirEntry>(dir->entries.data() + begin, end - begin));
    }

    void handle_remove(uint32_t id, PacketReader& reader) {
        std::string path = reader.string();
        if (!reader.ok || has_nul(path)) {
            send_status(id, SSH_FX_BAD_MESSAGE);
            return;
        }
        if (unlink(path.c_str()) != 0) {
            send_errno(id, errno);
            return;
        }
        send_status(id, SSH_FX_OK);
    }

    void handle_mkdir(uint32_t id, PacketReader& reader) {
        std::string path = reader.string();
        Attributes attrs = reader.attrs();
        if (!reader.ok || has_nul(path)) {
            send_status(id, SSH_FX_BAD_MESSAGE);
            return;
        }
        mode_t mode = (attrs.flags & SSH_FILEXFER_ATTR_PERMISSIONS) != 0 ? static_cast<mode_t>(attrs.permissions) : 0755;
        if (mkdir(path.c_str(), mode) != 0) {
            send_errno(id, errno);
            return;
        }
        send_status(id, SSH_FX_OK);
    }

    void handle_rmdir(uint32_t id, PacketReader& reader) {
        std::string path = reader.string();
        if (!reader.ok || has_nul(path)) {
            send_status(id, SSH_FX_BAD_MESSAGE);
            return;
        }
        if (rmdir(path.c_str()) != 0) {
            send_errno(id, errno);
            return;
        }
        send_status(id, SSH_FX_OK);
    }

    void handle_realpath(uint32_t id, PacketReader& reader) {
        std::string path = reader.string();
        if (!reader.ok || has_nul(path)) {
            send_status(id, SSH_FX_BAD_MESSAGE);
            return;
        }
        std::string normalized = path_for_client(normalize_path(path));
        DirEntry entry{};
        entry.name = normalized;
        struct stat st{};
        if (stat(normalized.c_str(), &st) == 0) {
            entry.attrs = attrs_from_stat(st);
            entry.longname = make_longname(normalized, st);
        } else {
            entry.longname = normalized;
        }
        std::array<DirEntry, 1> entries = {std::move(entry)};
        send_name(id, std::span<const DirEntry>(entries.data(), entries.size()));
    }

    void handle_rename(uint32_t id, PacketReader& reader) {
        std::string old_path = reader.string();
        std::string new_path = reader.string();
        if (!reader.ok || has_nul(old_path) || has_nul(new_path)) {
            send_status(id, SSH_FX_BAD_MESSAGE);
            return;
        }
        if (rename(old_path.c_str(), new_path.c_str()) != 0) {
            int err = retry_rename_over_symlink(old_path, new_path, errno);
            if (err != 0) {
                send_errno(id, err);
                return;
            }
            send_status(id, SSH_FX_OK);
            return;
        }
        send_status(id, SSH_FX_OK);
    }

    void handle_readlink(uint32_t id, PacketReader& reader) {
        std::string path = reader.string();
        if (!reader.ok || has_nul(path)) {
            send_status(id, SSH_FX_BAD_MESSAGE);
            return;
        }
        std::array<char, PATH_MAX> target{};
        ssize_t len = readlink(path.c_str(), target.data(), target.size() - 1);
        if (len < 0) {
            send_errno(id, errno);
            return;
        }
        std::string target_path(target.data(), static_cast<size_t>(len));
        DirEntry entry{};
        entry.name = target_path;
        entry.longname = target_path;
        std::array<DirEntry, 1> entries = {std::move(entry)};
        send_name(id, std::span<const DirEntry>(entries.data(), entries.size()));
    }

    void handle_symlink(uint32_t id, PacketReader& reader) {
        std::string target_path = reader.string();
        std::string link_path = reader.string();
        if (!reader.ok || has_nul(target_path) || has_nul(link_path)) {
            send_status(id, SSH_FX_BAD_MESSAGE);
            return;
        }

        if (symlink(target_path.c_str(), link_path.c_str()) != 0) {
            send_errno(id, errno);
            return;
        }
        send_status(id, SSH_FX_OK);
    }

    void handle_extended(uint32_t id, PacketReader& reader) {
        std::string request = reader.string();
        if (!reader.ok) {
            send_status(id, SSH_FX_BAD_MESSAGE);
            return;
        }

        if (request == "posix-rename@openssh.com") {
            handle_rename(id, reader);
            return;
        }
        if (request == "expand-path@openssh.com") {
            handle_realpath(id, reader);
            return;
        }
        if (request == "fsync@openssh.com") {
            std::string handle = reader.string();
            if (!reader.ok) {
                send_status(id, SSH_FX_BAD_MESSAGE);
                return;
            }
            FileHandle* file = find_file(handle);
            if (file == nullptr) {
                send_status(id, SSH_FX_FAILURE);
                return;
            }
            if (fsync(file->fd) != 0) {
                send_errno(id, errno);
                return;
            }
            send_status(id, SSH_FX_OK);
            return;
        }

        send_status(id, SSH_FX_OP_UNSUPPORTED);
    }
};

}  // namespace

auto main() -> int {
    signal(SIGPIPE, SIG_IGN);
    Server server;
    server.run();
    return 0;
}
