#include "filesystem.hpp"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <string>
#include <utility>
#include <vector>

namespace sftpserver {
namespace {

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

auto get_cwd() -> std::string {
    std::array<char, PATH_MAX> buffer{};
    if (getcwd(buffer.data(), buffer.size()) == nullptr) {
        return "/";
    }
    return buffer.data();
}

}  // namespace

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

}  // namespace sftpserver
