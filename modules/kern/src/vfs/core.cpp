#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <dev/block_device.hpp>
#include <dev/device.hpp>
#include <mod/io/serial/serial.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/remote_vfs.hpp>
#include <net/wki/wki.hpp>
#include <platform/mm/virt.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/rtc/rtc.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>
#include <string_view>
#include <util/smallvec.hpp>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/devfs.hpp>
#include <vfs/fs/fat32.hpp>
#include <vfs/fs/procfs.hpp>
#include <vfs/fs/tmpfs.hpp>
#include <vfs/fs/xfs/xfs_vfs.hpp>
#include <vfs/mount.hpp>
#include <vfs/stat.hpp>

#include "file.hpp"
#include "fs/devfs.hpp"
#include "fs/fat32.hpp"
#include "fs/tmpfs.hpp"
#include "platform/mm/dyn/kmalloc.hpp"
#include "vfs.hpp"

namespace ker::vfs {

auto vfs_readlink(const char* path, char* buf, size_t bufsize) -> ssize_t;
auto readlink_resolved(const char* absPath, char* buf, size_t bufsize) -> ssize_t;

// Keep in sync with userspace fcntl.h (Linux-compatible octal values)
constexpr int O_NONBLOCK = 04000;

namespace {
constexpr size_t MAX_PATH_LEN = 512;
constexpr int MAX_SYMLINK_DEPTH = 8;
constexpr size_t MAX_COMPONENTS = 64;
constexpr size_t MAX_VFSTAB_BYTES = 4096;
constexpr size_t WKI_PATH_PREFIX_LEN = 5;

auto make_absolute(const char* path, char* out, size_t outsize) -> int;
auto canonicalize_path(char* path, size_t bufsize) -> int;
auto normalize_task_path_inplace(char* path, size_t bufsize) -> int;

ker::util::SmallVec<ker::mod::sched::task::WkiVfsRule, 8> g_default_vfs_rules;

struct VfsRouteDecision {
    uint8_t route = static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL);
    size_t prefix_len = 0;
};

auto copy_path_string(const char* src, char* dst, size_t dst_size) -> int {
    if (src == nullptr || dst == nullptr || dst_size == 0) {
        return -EINVAL;
    }

    size_t len = std::strlen(src);
    if (len + 1 > dst_size) {
        return -ENAMETOOLONG;
    }

    std::memcpy(dst, src, len + 1);
    return 0;
}

auto task_submitter_hostname(const ker::mod::sched::task::Task* task) -> const char* {
    if (task != nullptr && task->wki_submitter_hostname[0] != '\0') {
        return task->wki_submitter_hostname;
    }
    return ker::net::wki::g_wki.local_hostname;
}

auto path_prefix_matches(const char* path, const char* prefix, size_t prefix_len) -> bool {
    if (path == nullptr || prefix == nullptr || prefix_len == 0) {
        return false;
    }

    if (prefix_len == 1 && prefix[0] == '/') {
        return path[0] == '/';
    }

    if (std::strncmp(path, prefix, prefix_len) != 0) {
        return false;
    }

    return path[prefix_len] == '\0' || path[prefix_len] == '/';
}

auto strip_mount_prefix(const MountPoint* mount, const char* path) -> const char* {
    if (mount == nullptr || path == nullptr) {
        return path;
    }

    size_t mount_len = 0;
    while (mount->path[mount_len] != '\0') {
        mount_len++;
    }

    if (mount_len == 1 && mount->path[0] == '/') {
        return path + 1;
    }
    if (path[mount_len] == '/') {
        return path + mount_len + 1;
    }
    if (path[mount_len] == '\0') {
        return "";
    }
    return path + mount_len;
}

auto build_wki_host_path(const char* hostname, const char* suffix, char* out, size_t out_size) -> int {
    if (hostname == nullptr || hostname[0] == '\0') {
        return -ENOENT;
    }

    const char* trimmed_suffix = suffix;
    while (trimmed_suffix != nullptr && *trimmed_suffix == '/') {
        trimmed_suffix++;
    }

    size_t host_len = std::strlen(hostname);
    size_t suffix_len = (trimmed_suffix != nullptr) ? std::strlen(trimmed_suffix) : 0;
    size_t total = WKI_PATH_PREFIX_LEN + host_len + (suffix_len > 0 ? 1 + suffix_len : 0) + 1;
    if (total > out_size) {
        return -ENAMETOOLONG;
    }

    size_t pos = 0;
    std::memcpy(out + pos, "/wki/", WKI_PATH_PREFIX_LEN);
    pos += WKI_PATH_PREFIX_LEN;
    std::memcpy(out + pos, hostname, host_len);
    pos += host_len;
    if (suffix_len > 0) {
        out[pos++] = '/';
        std::memcpy(out + pos, trimmed_suffix, suffix_len);
        pos += suffix_len;
    }
    out[pos] = '\0';
    return 0;
}

auto rewrite_wki_host_alias(const ker::mod::sched::task::Task* task, const char* path, char* out, size_t out_size) -> int {
    constexpr char host_prefix[] = "/wki/host";
    constexpr size_t host_prefix_len = sizeof(host_prefix) - 1;

    if (path == nullptr) {
        return copy_path_string(path, out, out_size);
    }

    if (task == nullptr) {
        return copy_path_string(path, out, out_size);
    }

    char current[MAX_PATH_LEN] = {};
    int copy_result = copy_path_string(path, current, sizeof(current));
    if (copy_result < 0) {
        return copy_result;
    }

    for (int depth = 0; depth < MAX_SYMLINK_DEPTH; ++depth) {
        if (std::strncmp(current, host_prefix, host_prefix_len) != 0 ||
            (current[host_prefix_len] != '\0' && current[host_prefix_len] != '/')) {
            break;
        }

        const char* submitter = task_submitter_hostname(task);
        if (submitter != nullptr && submitter[0] != '\0' && std::strcmp(submitter, ker::net::wki::g_wki.local_hostname) == 0) {
            const char* suffix = current + host_prefix_len;
            while (*suffix == '/') {
                suffix++;
            }

            if (*suffix == '\0') {
                copy_result = copy_path_string("/", current, sizeof(current));
            } else {
                size_t suffix_len = std::strlen(suffix);
                if (suffix_len + 2 > sizeof(current)) {
                    return -ENAMETOOLONG;
                }

                current[0] = '/';
                std::memcpy(current + 1, suffix, suffix_len + 1);
                copy_result = 0;
            }
        } else {
            copy_result = build_wki_host_path(submitter, current + host_prefix_len, current, sizeof(current));
        }

        if (copy_result < 0) {
            return copy_result;
        }
    }

    return copy_path_string(current, out, out_size);
}

// Re-apply the calling task's root prefix after following an absolute symlink.
// Without this, absolute symlink targets (e.g. /usr/sbin) escape the pivoted
// root and resolve against the global root instead of the task's root.
auto reapply_root_prefix(char* path, size_t bufsize) -> int {
    if (!ker::mod::sched::has_run_queues()) return 0;
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return 0;
    size_t root_len = std::strlen(task->root);
    if (root_len <= 1) return 0;  // root == "/"
    size_t path_len = std::strlen(path);
    if (root_len + path_len + 1 > bufsize) return -ENAMETOOLONG;
    std::memmove(path + root_len, path, path_len + 1);
    std::memcpy(path, task->root, root_len);
    return 0;
}

auto splice_symlink_target(const char* original_path, size_t prefix_len, const char* target, char* out, size_t out_size) -> int {
    if (original_path == nullptr || target == nullptr || out == nullptr || out_size == 0) {
        return -EINVAL;
    }

    const char* remainder = original_path + prefix_len;
    while (*remainder == '/') {
        remainder++;
    }

    size_t remainder_len = std::strlen(remainder);
    size_t target_len = std::strlen(target);
    size_t pos = 0;

    if (target[0] == '/') {
        if (target_len + 1 > out_size) {
            return -ENAMETOOLONG;
        }
        std::memcpy(out, target, target_len);
        pos = target_len;
    } else {
        size_t parent_len = 1;
        for (size_t i = 1; i < prefix_len; ++i) {
            if (original_path[i] == '/') {
                parent_len = i;
            }
        }

        if (parent_len + 1 > out_size) {
            return -ENAMETOOLONG;
        }

        std::memcpy(out, original_path, parent_len);
        pos = parent_len;
        if (pos == 0 || out[pos - 1] != '/') {
            out[pos++] = '/';
        }

        if (pos + target_len + 1 > out_size) {
            return -ENAMETOOLONG;
        }
        std::memcpy(out + pos, target, target_len);
        pos += target_len;
    }

    if (remainder_len > 0) {
        if (pos == 0 || out[pos - 1] != '/') {
            if (pos + 1 >= out_size) {
                return -ENAMETOOLONG;
            }
            out[pos++] = '/';
        }
        if (pos + remainder_len + 1 > out_size) {
            return -ENAMETOOLONG;
        }
        std::memcpy(out + pos, remainder, remainder_len + 1);
    } else {
        if (pos >= out_size) {
            return -ENAMETOOLONG;
        }
        out[pos] = '\0';
    }

    return canonicalize_path(out, out_size);
}

auto resolve_prefix_symlink_once(char* path, size_t bufsize, bool apply_task_policy) -> int {
    if (path == nullptr || bufsize == 0) {
        return -EINVAL;
    }

    for (size_t end = 1;; ++end) {
        char ch = path[end];
        if (ch != '/' && ch != '\0') {
            continue;
        }
        if (end == 1) {
            if (ch == '\0') {
                break;
            }
            continue;
        }

        char prefix[MAX_PATH_LEN] = {};
        if (end + 1 > sizeof(prefix)) {
            return -ENAMETOOLONG;
        }
        std::memcpy(prefix, path, end);
        prefix[end] = '\0';

        char linkbuf[MAX_PATH_LEN] = {};
        ssize_t link_len = readlink_resolved(prefix, linkbuf, sizeof(linkbuf) - 1);
        if (link_len > 0) {
            linkbuf[link_len] = '\0';

            char substituted[MAX_PATH_LEN] = {};
            int splice_result = splice_symlink_target(path, end, linkbuf, substituted, sizeof(substituted));
            if (splice_result < 0) {
                return splice_result;
            }

            int copy_result = copy_path_string(substituted, path, bufsize);
            if (copy_result < 0) {
                return copy_result;
            }

            // Absolute symlink targets must stay within the task's root.
            if (linkbuf[0] == '/') {
                int rr = reapply_root_prefix(path, bufsize);
                if (rr < 0) return rr;
            }

            if (apply_task_policy) {
                int normalize = normalize_task_path_inplace(path, bufsize);
                if (normalize < 0) {
                    return normalize;
                }
            }

            return 1;
        }

        if (ch == '\0') {
            break;
        }
    }

    return 0;
}

auto choose_task_route(const ker::mod::sched::task::Task* task, const char* path) -> VfsRouteDecision {
    VfsRouteDecision best = {};

    if (task != nullptr) {
        for (size_t i = 0; i < task->wki_vfs_rules.size(); ++i) {
            const auto& rule = task->wki_vfs_rules[i];
            if (rule.prefix_len == 0 || !path_prefix_matches(path, rule.prefix, rule.prefix_len)) {
                continue;
            }
            if (rule.prefix_len > best.prefix_len) {
                best.route = rule.route;
                best.prefix_len = rule.prefix_len;
            }
        }
    }

    for (size_t i = 0; i < g_default_vfs_rules.size(); ++i) {
        const auto& rule = g_default_vfs_rules[i];
        if (rule.prefix_len == 0 || !path_prefix_matches(path, rule.prefix, rule.prefix_len)) {
            continue;
        }
        if (rule.prefix_len > best.prefix_len) {
            best.route = rule.route;
            best.prefix_len = rule.prefix_len;
        }
    }

    return best;
}

auto apply_task_vfs_route(const ker::mod::sched::task::Task* task, const char* path, char* out, size_t out_size) -> int {
    if (path == nullptr || out == nullptr) {
        return -EINVAL;
    }

    if (task == nullptr) {
        return copy_path_string(path, out, out_size);
    }

    char aliased[MAX_PATH_LEN] = {};
    int alias_result = rewrite_wki_host_alias(task, path, aliased, sizeof(aliased));
    if (alias_result < 0) {
        return alias_result;
    }

    VfsRouteDecision decision = choose_task_route(task, aliased);
    if (decision.route != static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::HOST)) {
        return copy_path_string(aliased, out, out_size);
    }

    const char* submitter = task_submitter_hostname(task);
    if (submitter == nullptr || submitter[0] == '\0' || std::strcmp(submitter, ker::net::wki::g_wki.local_hostname) == 0) {
        return copy_path_string(aliased, out, out_size);
    }

    return build_wki_host_path(submitter, aliased, out, out_size);
}

auto normalize_task_path_inplace(char* path, size_t bufsize) -> int {
    if (path == nullptr) {
        return -EINVAL;
    }

    int canonical = canonicalize_path(path, bufsize);
    if (canonical < 0) {
        return canonical;
    }

    // Clamp: if ".." escaped above task->root, force path back to root.
    if (ker::mod::sched::has_run_queues()) {
        auto* task = ker::mod::sched::get_current_task();
        if (task != nullptr) {
            size_t root_len = std::strlen(task->root);
            if (root_len > 1) {
                if (std::strncmp(path, task->root, root_len) != 0) {
                    std::memcpy(path, task->root, root_len);
                    path[root_len] = '/';
                    path[root_len + 1] = '\0';
                }
            }
        }
    }

    char routed[MAX_PATH_LEN] = {};
    ker::mod::sched::task::Task* current_task = nullptr;
    if (ker::mod::sched::has_run_queues()) {
        current_task = ker::mod::sched::get_current_task();
    }

    int route_result = apply_task_vfs_route(current_task, path, routed, sizeof(routed));
    if (route_result < 0) {
        return route_result;
    }

    return copy_path_string(routed, path, bufsize);
}

auto resolve_task_path_raw(const char* path, char* out, size_t outsize) -> int {
    int absolute = make_absolute(path, out, outsize);
    if (absolute < 0) {
        return absolute;
    }

    // Prepend per-process root prefix when it differs from "/".
    // This makes pivot_root transparent: after pivot_root("/rootfs", ...),
    // task->root becomes "/rootfs" and all absolute paths get prefixed.
    if (ker::mod::sched::has_run_queues()) {
        auto* task = ker::mod::sched::get_current_task();
        if (task != nullptr) {
            size_t root_len = std::strlen(task->root);
            if (root_len > 1) {  // root != "/"
                size_t path_len = std::strlen(out);
                if (root_len + path_len + 1 > outsize) {
                    return -ENAMETOOLONG;
                }
                // Shift existing path right to make room for root prefix
                std::memmove(out + root_len, out, path_len + 1);
                std::memcpy(out, task->root, root_len);
            }
        }
    }

    return normalize_task_path_inplace(out, outsize);
}

auto add_default_vfs_rule(const char* prefix, uint8_t route) -> int {
    if (prefix == nullptr || prefix[0] != '/') {
        return -EINVAL;
    }
    if (route != static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL) &&
        route != static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::HOST)) {
        return -EINVAL;
    }

    char canonical[MAX_PATH_LEN] = {};
    int copy_result = copy_path_string(prefix, canonical, sizeof(canonical));
    if (copy_result < 0) {
        return copy_result;
    }

    int canonical_result = canonicalize_path(canonical, sizeof(canonical));
    if (canonical_result < 0) {
        return canonical_result;
    }

    size_t prefix_len = std::strlen(canonical);
    if (prefix_len == 0 || prefix_len >= ker::mod::sched::task::WkiVfsRule::PREFIX_MAX) {
        return -ENAMETOOLONG;
    }

    for (size_t i = 0; i < g_default_vfs_rules.size(); ++i) {
        auto& rule = g_default_vfs_rules[i];
        if (rule.prefix_len == prefix_len && std::strncmp(rule.prefix, canonical, prefix_len) == 0) {
            std::memcpy(rule.prefix, canonical, prefix_len + 1);
            rule.prefix_len = static_cast<uint16_t>(prefix_len);
            rule.route = route;
            rule.reserved = 0;
            return 0;
        }
    }

    ker::mod::sched::task::WkiVfsRule new_rule{};
    std::memcpy(new_rule.prefix, canonical, prefix_len + 1);
    new_rule.prefix_len = static_cast<uint16_t>(prefix_len);
    new_rule.route = route;
    new_rule.reserved = 0;
    if (!g_default_vfs_rules.push_back(new_rule)) {
        return -ENOMEM;
    }
    return 0;
}

void install_builtin_vfs_rules() {
    g_default_vfs_rules.clear();
    add_default_vfs_rule("/wki", static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL));
    add_default_vfs_rule("/proc", static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL));
    add_default_vfs_rule("/dev", static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL));
    add_default_vfs_rule("/tmp", static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL));
    add_default_vfs_rule("/run", static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL));
    add_default_vfs_rule("/", static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::HOST));
}

auto parse_vfs_route(const char* route_text, uint8_t* route_out) -> bool {
    if (route_text == nullptr || route_out == nullptr) {
        return false;
    }

    if (std::strcmp(route_text, "local") == 0) {
        *route_out = static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL);
        return true;
    }
    if (std::strcmp(route_text, "host") == 0) {
        *route_out = static_cast<uint8_t>(ker::mod::sched::task::WkiVfsRoute::HOST);
        return true;
    }
    return false;
}

void release_open_file(File* file) {
    if (file == nullptr) {
        return;
    }

    if (file->fops != nullptr && file->fops->vfs_close != nullptr) {
        file->fops->vfs_close(file);
    }
    if (file->vfs_path != nullptr) {
        ker::mod::mm::dyn::kmalloc::free((void*)file->vfs_path);
    }
    delete file;
}

void load_vfs_rules_from_buffer(char* buffer) {
    if (buffer == nullptr) {
        return;
    }

    char* line = buffer;
    while (*line != '\0') {
        char* line_end = line;
        while (*line_end != '\0' && *line_end != '\n' && *line_end != '\r') {
            line_end++;
        }

        char* next_line = line_end;
        while (*next_line == '\n' || *next_line == '\r') {
            *next_line = '\0';
            next_line++;
        }
        if (*line_end != '\0') {
            *line_end = '\0';
        }

        while (*line == ' ' || *line == '\t') {
            line++;
        }

        if (*line != '\0' && *line != '#') {
            char* prefix = line;
            while (*line != '\0' && *line != ' ' && *line != '\t') {
                line++;
            }

            if (*line != '\0') {
                *line++ = '\0';
                while (*line == ' ' || *line == '\t') {
                    line++;
                }

                char* route_text = line;
                while (*line != '\0' && *line != ' ' && *line != '\t') {
                    line++;
                }
                *line = '\0';

                uint8_t route = 0;
                if (parse_vfs_route(route_text, &route)) {
                    add_default_vfs_rule(prefix, route);
                }
            }
        }

        line = next_line;
    }
}

// Convert a possibly-relative path to an absolute path by prepending CWD.
// If the path is already absolute, copies it as-is.
// Returns 0 on success, negative on error.
auto make_absolute(const char* path, char* out, size_t outsize) -> int {
    if (path == nullptr || out == nullptr || outsize == 0) return -EINVAL;
    size_t plen = std::strlen(path);
    if (plen == 0) return -EINVAL;

    if (path[0] == '/') {
        if (plen + 1 > outsize) return -ENAMETOOLONG;
        std::memcpy(out, path, plen + 1);
        return 0;
    }

    // Relative path - prepend task CWD
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;

    size_t cwdlen = std::strlen(task->cwd);
    // Need: cwd + "/" + path + '\0'
    bool need_sep = (cwdlen > 1);  // Root "/" doesn't need extra /
    size_t total = cwdlen + (need_sep ? 1 : 0) + plen + 1;
    if (total > outsize) return -ENAMETOOLONG;

    std::memcpy(out, task->cwd, cwdlen);
    if (need_sep) {
        out[cwdlen] = '/';
        std::memcpy(out + cwdlen + 1, path, plen + 1);
    } else {
        std::memcpy(out + cwdlen, path, plen + 1);
    }
    return 0;
}

// Canonicalize a path in place: resolve ".", "..", and collapse "//".
// The path must be absolute (start with "/").
// Returns 0 on success, -ENAMETOOLONG if the path is too long.
auto canonicalize_path(char* path, size_t bufsize) -> int {
    if (path == nullptr || bufsize == 0 || path[0] != '/') {
        return -1;
    }

    // Split into components, resolving . and ..
    const char* components[MAX_COMPONENTS];  // NOLINT
    size_t num_components = 0;

    char* p = path + 1;  // skip leading /
    while (*p != '\0') {
        // Skip slashes
        while (*p == '/') {
            p++;
        }
        if (*p == '\0') {
            break;
        }

        // Find end of component
        char* comp_start = p;
        while (*p != '\0' && *p != '/') {
            p++;
        }

        // Null-terminate this component in the buffer
        char saved = *p;
        *p = '\0';

        if (comp_start[0] == '.' && comp_start[1] == '\0') {
            // "." - skip
        } else if (comp_start[0] == '.' && comp_start[1] == '.' && comp_start[2] == '\0') {
            // ".." - pop last component
            if (num_components > 0) {
                num_components--;
            }
        } else {
            if (num_components >= MAX_COMPONENTS) {
                return -ENAMETOOLONG;
            }
            components[num_components++] = comp_start;
        }

        // Keep the null terminator in place - the component pointers
        // stored in components[] depend on it for correct strlen/memcpy
        // during reconstruction.  Parsing still works because we advance
        // p past the '\0' below.
        if (saved == '/') {
            p++;
        }
    }

    // Reconstruct canonical path
    char result[MAX_PATH_LEN];  // NOLINT
    size_t pos = 0;
    result[pos++] = '/';

    for (size_t i = 0; i < num_components; ++i) {
        if (i > 0) {
            if (pos >= MAX_PATH_LEN - 1) {
                return -ENAMETOOLONG;
            }
            result[pos++] = '/';
        }
        size_t comp_len = std::strlen(components[i]);
        if (pos + comp_len >= MAX_PATH_LEN) {
            return -ENAMETOOLONG;
        }
        std::memcpy(static_cast<char*>(result) + pos, components[i], comp_len);
        pos += comp_len;
    }
    result[pos] = '\0';

    if (pos >= bufsize) {
        return -ENAMETOOLONG;
    }
    std::memcpy(path, static_cast<const char*>(result), pos + 1);
    return 0;
}

// Resolve symlinks in a path. The resolved path is written to resolved_buf.
// Returns 0 on success, -ELOOP on too many symlinks, -1 on other errors.
auto resolve_symlinks(const char* path, char* resolved_buf, size_t bufsize, bool apply_task_policy = false) -> int {
    if (path == nullptr || resolved_buf == nullptr || bufsize == 0) {
        return -1;
    }

    // Copy initial path to working buffer
    size_t path_len = 0;
    while (path[path_len] != '\0' && path_len < bufsize - 1) {
        resolved_buf[path_len] = path[path_len];
        path_len++;
    }
    resolved_buf[path_len] = '\0';

    if (apply_task_policy) {
        int normalize = normalize_task_path_inplace(resolved_buf, bufsize);
        if (normalize < 0) {
            return normalize;
        }
    }

    for (int depth = 0; depth < MAX_SYMLINK_DEPTH; ++depth) {
        int prefix_result = resolve_prefix_symlink_once(resolved_buf, bufsize, apply_task_policy);
        if (prefix_result < 0) {
            return prefix_result;
        }
        if (prefix_result > 0) {
            continue;
        }

        MountPoint* mount = find_mount_point(resolved_buf);
        if (mount == nullptr) {
            return 0;
        }

        if (mount->fs_type == FSType::PROCFS) {
            // Handle procfs symlinks (e.g., /proc/self -> /proc/<pid>)
            size_t mount_len = 0;
            while (mount->path[mount_len] != '\0') mount_len++;
            const char* fs_path = resolved_buf;
            if (mount_len == 1 && mount->path[0] == '/')
                fs_path = resolved_buf + 1;
            else if (resolved_buf[mount_len] == '/')
                fs_path = resolved_buf + mount_len + 1;
            else if (resolved_buf[mount_len] == '\0')
                fs_path = "";
            else
                fs_path = resolved_buf + mount_len;

            auto* f = ker::vfs::procfs::procfs_open_path(fs_path, 0, 0);
            if (f == nullptr) return 0;
            auto* pfd = static_cast<ker::vfs::procfs::ProcFileData*>(f->private_data);
            bool is_symlink = (pfd != nullptr && (pfd->node.type == ker::vfs::procfs::ProcNodeType::SELF_LINK ||
                                                  pfd->node.type == ker::vfs::procfs::ProcNodeType::EXE_LINK));
            if (!is_symlink) {
                ker::vfs::procfs::get_procfs_fops()->vfs_close(f);
                delete f;
                return 0;
            }
            char linkbuf[MAX_PATH_LEN];
            ssize_t link_len = ker::vfs::procfs::get_procfs_fops()->vfs_readlink(f, linkbuf, MAX_PATH_LEN);
            ker::vfs::procfs::get_procfs_fops()->vfs_close(f);
            delete f;
            if (link_len <= 0) return 0;
            linkbuf[link_len] = '\0';
            if (linkbuf[0] == '/') {
                if (static_cast<size_t>(link_len) >= bufsize) return -1;
                memcpy(resolved_buf, linkbuf, link_len + 1);
                int rr = reapply_root_prefix(resolved_buf, bufsize);
                if (rr < 0) return rr;
            } else {
                size_t last_slash = 0;
                bool found_slash = false;
                for (size_t i = 0; resolved_buf[i] != '\0'; ++i) {
                    if (resolved_buf[i] == '/') {
                        last_slash = i;
                        found_slash = true;
                    }
                }
                size_t prefix_len = found_slash ? last_slash + 1 : 0;
                if (prefix_len + static_cast<size_t>(link_len) >= bufsize) return -1;
                char new_path[MAX_PATH_LEN];
                memcpy(new_path, resolved_buf, prefix_len);
                memcpy(new_path + prefix_len, linkbuf, link_len);
                new_path[prefix_len + link_len] = '\0';
                memcpy(resolved_buf, new_path, prefix_len + link_len + 1);
            }
            if (apply_task_policy) {
                int normalize = normalize_task_path_inplace(resolved_buf, bufsize);
                if (normalize < 0) {
                    return normalize;
                }
            }
            continue;  // re-resolve after substitution
        }

        // Remote mounts: ask the server to resolve symlinks
        if (mount->fs_type == FSType::REMOTE) {
            size_t mount_len = 0;
            while (mount->path[mount_len] != '\0') mount_len++;
            const char* fs_path = resolved_buf;
            if (mount_len == 1 && mount->path[0] == '/')
                fs_path = resolved_buf + 1;
            else if (resolved_buf[mount_len] == '/')
                fs_path = resolved_buf + mount_len + 1;
            else if (resolved_buf[mount_len] == '\0')
                fs_path = "";
            else
                fs_path = resolved_buf + mount_len;

            char linkbuf[MAX_PATH_LEN];
            ssize_t link_len = ker::net::wki::wki_remote_vfs_readlink_path(mount->private_data, fs_path, linkbuf, MAX_PATH_LEN - 1);
            if (link_len <= 0) {
                return 0;  // Not a symlink or readlink failed - resolution complete
            }
            linkbuf[link_len] = '\0';

            if (linkbuf[0] == '/') {
                // Absolute symlink target - replace entire path
                if (static_cast<size_t>(link_len) >= bufsize) return -1;
                memcpy(resolved_buf, linkbuf, link_len + 1);
                int rr = reapply_root_prefix(resolved_buf, bufsize);
                if (rr < 0) return rr;
            } else {
                // Relative symlink target - replace last component
                size_t last_slash = 0;
                bool found_slash = false;
                for (size_t i = 0; resolved_buf[i] != '\0'; ++i) {
                    if (resolved_buf[i] == '/') {
                        last_slash = i;
                        found_slash = true;
                    }
                }
                size_t prefix_len = found_slash ? last_slash + 1 : 0;
                if (prefix_len + static_cast<size_t>(link_len) >= bufsize) return -1;
                char new_path[MAX_PATH_LEN];
                memcpy(new_path, resolved_buf, prefix_len);
                memcpy(new_path + prefix_len, linkbuf, link_len);
                new_path[prefix_len + link_len] = '\0';
                memcpy(resolved_buf, new_path, prefix_len + link_len + 1);
            }
            if (apply_task_policy) {
                int normalize = normalize_task_path_inplace(resolved_buf, bufsize);
                if (normalize < 0) {
                    return normalize;
                }
            }
            continue;  // Re-resolve after substitution
        }

        // Only tmpfs and XFS support symlinks currently
        if (mount->fs_type != FSType::TMPFS && mount->fs_type != FSType::XFS) {
            return 0;
        }

        // XFS: resolve symlinks via xfs_readlink
        if (mount->fs_type == FSType::XFS) {
            size_t mount_len = 0;
            while (mount->path[mount_len] != '\0') mount_len++;
            const char* fs_path = resolved_buf;
            if (mount_len == 1 && mount->path[0] == '/')
                fs_path = resolved_buf + 1;
            else if (resolved_buf[mount_len] == '/')
                fs_path = resolved_buf + mount_len + 1;
            else if (resolved_buf[mount_len] == '\0')
                fs_path = "";
            else
                fs_path = resolved_buf + mount_len;

            // Open the path to check if it's a symlink
            auto* xctx = static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data);
            auto* f = ker::vfs::xfs::xfs_open_path(fs_path, 0, 0, xctx);
            if (f == nullptr) return 0;
            if (f->fops == nullptr || f->fops->vfs_readlink == nullptr) {
                ker::vfs::xfs::get_xfs_fops()->vfs_close(f);
                ker::mod::mm::dyn::kmalloc::free(f);
                return 0;
            }
            char linkbuf[MAX_PATH_LEN];
            ssize_t link_len = f->fops->vfs_readlink(f, linkbuf, MAX_PATH_LEN - 1);
            ker::vfs::xfs::get_xfs_fops()->vfs_close(f);
            ker::mod::mm::dyn::kmalloc::free(f);
            if (link_len <= 0) return 0;  // Not a symlink or error
            linkbuf[link_len] = '\0';
            if (linkbuf[0] == '/') {
                if (static_cast<size_t>(link_len) >= bufsize) return -1;
                memcpy(resolved_buf, linkbuf, link_len + 1);
                int rr = reapply_root_prefix(resolved_buf, bufsize);
                if (rr < 0) return rr;
            } else {
                size_t last_slash = 0;
                bool found_slash = false;
                for (size_t i = 0; resolved_buf[i] != '\0'; ++i) {
                    if (resolved_buf[i] == '/') {
                        last_slash = i;
                        found_slash = true;
                    }
                }
                size_t prefix_len = found_slash ? last_slash + 1 : 0;
                if (prefix_len + static_cast<size_t>(link_len) >= bufsize) return -1;
                char new_path[MAX_PATH_LEN];
                memcpy(new_path, resolved_buf, prefix_len);
                memcpy(new_path + prefix_len, linkbuf, link_len);
                new_path[prefix_len + link_len] = '\0';
                memcpy(resolved_buf, new_path, prefix_len + link_len + 1);
            }
            continue;
        }

        // Strip mount prefix to get fs-relative path
        size_t mount_len = 0;
        while (mount->path[mount_len] != '\0') {
            mount_len++;
        }

        const char* fs_path = resolved_buf;
        if (mount_len == 1 && mount->path[0] == '/') {
            fs_path = resolved_buf + 1;
        } else if (resolved_buf[mount_len] == '/') {
            fs_path = resolved_buf + mount_len + 1;
        } else if (resolved_buf[mount_len] == '\0') {
            fs_path = "";
        } else {
            fs_path = resolved_buf + mount_len;
        }

        // Walk the tmpfs path to find the node
        auto* node = ker::vfs::tmpfs::tmpfs_walk_path(fs_path, false);
        if (node == nullptr) {
            return 0;  // Path doesn't exist yet (might be created with O_CREAT)
        }

        if (node->type != ker::vfs::tmpfs::TmpNodeType::SYMLINK) {
            return 0;  // Not a symlink, resolution complete
        }

        if (node->symlink_target == nullptr) {
            return -1;
        }

        // Build the new path
        char new_path[MAX_PATH_LEN];  // NOLINT
        size_t target_len = 0;
        while (node->symlink_target[target_len] != '\0') {
            target_len++;
        }

        if (node->symlink_target[0] == '/') {
            // Absolute symlink target - replace entire path
            if (target_len >= bufsize) {
                return -1;
            }
            memcpy(resolved_buf, node->symlink_target, target_len + 1);
            int rr = reapply_root_prefix(resolved_buf, bufsize);
            if (rr < 0) return rr;
        } else {
            // Relative symlink target - replace last component
            size_t last_slash = 0;
            bool found_slash = false;
            for (size_t i = 0; resolved_buf[i] != '\0'; ++i) {
                if (resolved_buf[i] == '/') {
                    last_slash = i;
                    found_slash = true;
                }
            }

            size_t prefix_len = found_slash ? last_slash + 1 : 0;
            if (prefix_len + target_len >= bufsize) {
                return -1;
            }
            memcpy(new_path, resolved_buf, prefix_len);                       // NOLINT
            memcpy(new_path + prefix_len, node->symlink_target, target_len);  // NOLINT
            new_path[prefix_len + target_len] = '\0';                         // NOLINT
            memcpy(resolved_buf, new_path, prefix_len + target_len + 1);
        }

        if (apply_task_policy) {
            int normalize = normalize_task_path_inplace(resolved_buf, bufsize);
            if (normalize < 0) {
                return normalize;
            }
        }
    }

    return -ELOOP;
}
}  // namespace

auto vfs_open(std::string_view path, int flags, int mode) -> int {
    vfs_debug_log("vfs_open: opening file\n");

    // Apply umask on creation
    if (flags & ker::vfs::O_CREAT) {
        auto* task = ker::mod::sched::get_current_task();
        if (task != nullptr) {
            mode = mode & ~static_cast<int>(task->umask);
        }
    }

    // Convert string_view to null-terminated string
    char rawPath[MAX_PATH_LEN];  // NOLINT
    if (path.size() >= MAX_PATH_LEN) {
        return -ENAMETOOLONG;
    }
    std::memcpy(rawPath, path.data(), path.size());
    rawPath[path.size()] = '\0';

    char pathBuffer[MAX_PATH_LEN];  // NOLINT
    if (resolve_task_path_raw(rawPath, pathBuffer, MAX_PATH_LEN) < 0) {
        return -ENOENT;
    }

    // Resolve symlinks in the path
    char resolved[MAX_PATH_LEN];  // NOLINT
    int resolve_ret = resolve_symlinks(pathBuffer, resolved, MAX_PATH_LEN, true);
    if (resolve_ret == -ELOOP) {
        ker::mod::io::serial::write("vfs_open: too many symlink levels\n");
        return -ELOOP;
    }
    if (resolve_ret == 0) {
        // Use the resolved path
        std::memcpy(pathBuffer, resolved, MAX_PATH_LEN);
    }

    auto* current = ker::mod::sched::get_current_task();
    if (current == nullptr) {
        vfs_debug_log("vfs_open: no current task\n");
        return -1;
    }

    // Find the mount point for this path
    MountPoint* mount = find_mount_point(pathBuffer);
    if (mount == nullptr) {
        vfs_debug_log("vfs_open: no mount point found for path\n");
        ker::mod::io::serial::write("vfs_open: no mount point found for path: ");
        ker::mod::io::serial::write(pathBuffer);
        ker::mod::io::serial::write("\n");
        return -1;
    }

    const char* fs_relative_path = strip_mount_prefix(mount, pathBuffer);

    ker::vfs::File* f = nullptr;

    // Route to the appropriate filesystem driver based on mount point
    switch (mount->fs_type) {
        case FSType::DEVFS:
            f = ker::vfs::devfs::devfs_open_path(fs_relative_path, flags, mode);
            if (f != nullptr) {
                f->fops = ker::vfs::devfs::get_devfs_fops();
                f->fs_type = FSType::DEVFS;
            }
            break;
        case FSType::FAT32:
            f = ker::vfs::fat32::fat32_open_path(fs_relative_path, flags, mode,
                                                 static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data));
            if (f != nullptr) {
                f->fops = ker::vfs::fat32::get_fat32_fops();
                f->fs_type = FSType::FAT32;
            } else {
                ker::mod::io::serial::write("vfs_open: fat32_open_path failed for '");
                ker::mod::io::serial::write(fs_relative_path);
                ker::mod::io::serial::write("' (mount='");
                ker::mod::io::serial::write(mount->path);
                ker::mod::io::serial::write("', original path='");
                ker::mod::io::serial::write(pathBuffer);
                ker::mod::io::serial::write("')\n");
            }
            break;
        case FSType::TMPFS:
            f = ker::vfs::tmpfs::tmpfs_open_path(fs_relative_path, flags, mode);
            if (f != nullptr) {
                f->fops = ker::vfs::tmpfs::get_tmpfs_fops();
                f->fs_type = FSType::TMPFS;
            }
            break;
        case FSType::REMOTE:
            f = ker::net::wki::wki_remote_vfs_open_path(fs_relative_path, flags, mode, mount->private_data);
            break;
        case FSType::PROCFS:
            f = ker::vfs::procfs::procfs_open_path(fs_relative_path, flags, mode);
            if (f != nullptr) {
                f->fops = ker::vfs::procfs::get_procfs_fops();
                f->fs_type = FSType::PROCFS;
            }
            break;
        case FSType::XFS:
            f = ker::vfs::xfs::xfs_open_path(fs_relative_path, flags, mode,
                                             static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data));
            if (f != nullptr) {
                f->fops = ker::vfs::xfs::get_xfs_fops();
                f->fs_type = FSType::XFS;
            }
            break;
        default:
            vfs_debug_log("vfs_open: unknown filesystem type\n");
            return -1;
    }

    if (f == nullptr) {
        vfs_debug_log("vfs_open: failed to open file\n");
        return -ENOENT;
    }

    // Store the absolute VFS path for mount-overlay directory listing
    size_t path_len = std::strlen(pathBuffer);
    auto* path_copy = static_cast<char*>(ker::mod::mm::dyn::kmalloc::malloc(path_len + 1));
    if (path_copy != nullptr) {
        std::memcpy(path_copy, pathBuffer, path_len + 1);
        f->vfs_path = path_copy;
    } else {
        f->vfs_path = nullptr;
    }
    f->dir_fs_count = static_cast<size_t>(-1);
    f->open_flags = flags;
    f->fd_flags = 0;  // fd_flags on File is legacy; CLOEXEC is per-fd in task bitmap

    // Permission check: verify R/W access based on open flags
    // Build required access bits from open flags
    int required_access = 0;
    int accmode = flags & 3;                                 // O_RDONLY=0, O_WRONLY=1, O_RDWR=2
    if (accmode == 0 || accmode == 2) required_access |= 4;  // R_OK
    if (accmode == 1 || accmode == 2) required_access |= 2;  // W_OK

    // Get the file's mode/uid/gid for permission check
    if (required_access != 0 && f->fs_type == FSType::TMPFS) {
        auto* node = static_cast<ker::vfs::tmpfs::TmpNode*>(f->private_data);
        if (node != nullptr) {
            int perm_ret = vfs_check_permission(node->mode, node->uid, node->gid, required_access);
            if (perm_ret < 0) {
                // Permission denied - clean up and return
                if (f->vfs_path != nullptr) ker::mod::mm::dyn::kmalloc::free(const_cast<char*>(f->vfs_path));
                ker::mod::mm::dyn::kmalloc::free(f);
                return perm_ret;
            }
        }
    }

    int fd = vfs_alloc_fd(current, f);
    if (fd < 0) {
        return -1;
    }
    if (flags & ker::vfs::O_CLOEXEC) {
        current->set_fd_cloexec(static_cast<unsigned>(fd));
    }
    return fd;
}

auto vfs_close(int fd) -> int {
    // Release FD from current task
    ker::mod::sched::task::Task* t = ker::mod::sched::get_current_task();
    if (t == nullptr) {
        return -ESRCH;
    }
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (f == nullptr) {
        return -EBADF;
    }

    // Decrement reference count and check if we need to free the file
    int new_rc = __atomic_sub_fetch(&f->refcount, 1, __ATOMIC_ACQ_REL);

    // Release the FD from the task's file descriptor table
    vfs_release_fd(t, fd);
    t->clear_fd_cloexec(static_cast<unsigned>(fd));

    // Only call close and free if no more references
    if (new_rc <= 0) {
        if ((f->fops != nullptr) && (f->fops->vfs_close != nullptr)) {
            f->fops->vfs_close(f);
        }
        // Free the VFS path string if allocated
        if (f->vfs_path != nullptr) {
            ker::mod::mm::dyn::kmalloc::free(const_cast<char*>(f->vfs_path));
        }
        // Free the File descriptor object (just the handle/wrapper)
        // but keep the underlying fs node (f->private_data) intact
        // so the file can be reopened later
        ker::mod::mm::dyn::kmalloc::free((void*)f);
    }

    return 0;
}

auto vfs_read(int fd, void* buf, size_t count, size_t* actual_size) -> ssize_t {
    ker::mod::sched::task::Task* t = ker::mod::sched::get_current_task();
    if (t == nullptr) {
        return -ESRCH;
    }
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (f == nullptr) {
        return -EBADF;
    }
    if ((f->fops == nullptr) || (f->fops->vfs_read == nullptr)) {
        return -EINVAL;
    }
    ssize_t r = f->fops->vfs_read(f, buf, count, (size_t)f->pos);
    if (r >= 0) {
        f->pos += r;
        if (actual_size != nullptr) {
            *actual_size = static_cast<size_t>(r);
        }
        return r;
    }
    return r;
}

auto vfs_write(int fd, const void* buf, size_t count, size_t* actual_size) -> ssize_t {
    ker::mod::sched::task::Task* t = ker::mod::sched::get_current_task();
    if (t == nullptr) {
        return -ESRCH;
    }
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (f == nullptr) {
        return -EBADF;
    }
    if ((f->fops == nullptr) || (f->fops->vfs_write == nullptr)) {
        return -EINVAL;
    }
    ssize_t r = f->fops->vfs_write(f, buf, count, (size_t)f->pos);
    if (r >= 0) {
        f->pos += r;
        if (actual_size != nullptr) {
            *actual_size = static_cast<size_t>(r);
        }
        return r;
    }
    return r;
}

auto vfs_lseek(int fd, off_t offset, int whence) -> off_t {
    ker::mod::sched::task::Task* t = ker::mod::sched::get_current_task();
    if (t == nullptr) {
        return -ESRCH;
    }
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (f == nullptr) {
        return -EBADF;
    }
    if ((f->fops == nullptr) || (f->fops->vfs_lseek == nullptr)) {
        return -ESPIPE;
    }
    return f->fops->vfs_lseek(f, offset, whence);
}

auto vfs_alloc_fd(ker::mod::sched::task::Task* task, struct File* file) -> int {
    if ((task == nullptr) || (file == nullptr)) {
        return -1;
    }
    uint64_t slot = task->fd_table.find_first_unset(0);
    if (slot == UINT64_MAX || !task->fd_table.insert(slot, file)) {
        return -1;  // EMFILE (too many open files) or OOM
    }
    file->fd = static_cast<int>(slot);
    ker::mod::perf::record_container_stat(0, task->pid, ker::mod::perf::PerfSubsystem::FD_TABLE, 0, ker::mod::perf::PERF_FLAG_CT_INSERT,
                                          static_cast<int64_t>(task->fd_table.size()), 0,
                                          reinterpret_cast<uint64_t>(__builtin_return_address(0)));
    return static_cast<int>(slot);
}

auto vfs_get_file(ker::mod::sched::task::Task* task, int fd) -> struct File* {
    if (task == nullptr || fd < 0) {
        return nullptr;
    }
    return reinterpret_cast<struct File*>(task->fd_table.lookup(static_cast<uint64_t>(fd)));
}

auto vfs_release_fd(ker::mod::sched::task::Task* task, int fd) -> int {
    if (task == nullptr || fd < 0) {
        return -1;
    }
    task->fd_table.remove(static_cast<uint64_t>(fd));
    ker::mod::perf::record_container_stat(0, task->pid, ker::mod::perf::PerfSubsystem::FD_TABLE, 0, ker::mod::perf::PERF_FLAG_CT_REMOVE,
                                          static_cast<int64_t>(task->fd_table.size()), 0,
                                          reinterpret_cast<uint64_t>(__builtin_return_address(0)));
    return 0;
}

auto vfs_resolve_dirfd(ker::mod::sched::task::Task* task, int dirfd, const char* pathname, char* resolved, size_t resolved_size) -> int {
    if (task == nullptr || pathname == nullptr || resolved == nullptr || resolved_size == 0) {
        return -EINVAL;
    }

    // Absolute pathnames ignore dirfd entirely
    if (pathname[0] == '/') {
        size_t len = strlen(pathname);
        if (len >= resolved_size) return -ENAMETOOLONG;
        memcpy(resolved, pathname, len + 1);
        return 0;
    }

    // Determine the base directory path
    const char* base = nullptr;
    if (dirfd == AT_FDCWD) {
        base = task->cwd;
    } else {
        auto* file = vfs_get_file(task, dirfd);
        if (file == nullptr) return -EBADF;
        if (!file->is_directory) return -ENOTDIR;
        base = file->vfs_path;
        if (base == nullptr) return -EBADF;
    }

    // Concatenate base + "/" + pathname
    size_t base_len = strlen(base);
    size_t path_len = strlen(pathname);

    // Strip trailing slash from base
    while (base_len > 1 && base[base_len - 1] == '/') {
        base_len--;
    }

    // Need: base + "/" + pathname + '\0'
    if (base_len + 1 + path_len + 1 > resolved_size) {
        return -ENAMETOOLONG;
    }

    memcpy(resolved, base, base_len);
    resolved[base_len] = '/';
    memcpy(resolved + base_len + 1, pathname, path_len + 1);
    return 0;
}

auto vfs_isatty(int fd) -> bool {
    ker::mod::sched::task::Task* t = ker::mod::sched::get_current_task();
    if (t == nullptr) {
        return false;
    }
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (f == nullptr) {
        return false;
    }
    if ((f->fops == nullptr) || (f->fops->vfs_isatty == nullptr)) {
        return false;
    }
    return f->fops->vfs_isatty(f);
}

auto vfs_read_dir_entries(int fd, void* buffer, size_t max_size) -> ssize_t {
    ker::mod::sched::task::Task* t = ker::mod::sched::get_current_task();
    if (t == nullptr) {
        return -ESRCH;
    }
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (f == nullptr) {
        return -EBADF;
    }

    // Check if this is a directory
    if (!f->is_directory) {
        return -ENOTDIR;
    }

    // Buffer must be large enough for at least one DirEntry
    if (buffer == nullptr || max_size < sizeof(DirEntry)) {
        return -EINVAL;
    }

    // We allow vfs_readdir to be null - the directory may contain only mount children
    bool has_fs_readdir = (f->fops != nullptr) && (f->fops->vfs_readdir != nullptr);

    auto* entries = static_cast<DirEntry*>(buffer);
    size_t max_entries = max_size / sizeof(DirEntry);
    size_t entries_read = 0;

    // Read directory entries using the current position as index
    size_t start_index = static_cast<size_t>(f->pos);

    for (size_t i = 0; i < max_entries; ++i) {
        size_t actual_index = start_index + i;

        // Phase 1: try filesystem readdir
        if (has_fs_readdir && (f->dir_fs_count == static_cast<size_t>(-1) || actual_index < f->dir_fs_count)) {
            int ret = f->fops->vfs_readdir(f, &entries[entries_read], actual_index);
            if (ret == 0) {
                entries_read++;
                continue;
            }
            // FS entries exhausted at this index
            f->dir_fs_count = actual_index;
        }

        // Phase 2: inject synthetic task-aware aliases and mount-point children.
        // For each mount whose path starts with vfs_path, extract the first
        // path component after vfs_path as a child directory name.
        // Deduplicate against FS entries and against earlier mounts that
        // yield the same child component.
        bool found_mount_child = false;
        if (f->vfs_path != nullptr) {
            size_t fs_count = has_fs_readdir ? f->dir_fs_count : 0;
            size_t synthetic_index = actual_index - fs_count;

            bool inject_host_alias = std::strcmp(f->vfs_path, "/wki") == 0;
            if (inject_host_alias && has_fs_readdir && fs_count > 0) {
                DirEntry probe = {};
                for (size_t pi = 0; pi < fs_count; ++pi) {
                    int pret = f->fops->vfs_readdir(f, &probe, pi);
                    if (pret != 0) {
                        break;
                    }
                    if (std::strcmp(probe.d_name.data(), "host") == 0) {
                        inject_host_alias = false;
                        break;
                    }
                }
            }

            if (inject_host_alias) {
                if (synthetic_index == 0) {
                    entries[entries_read].d_ino = 0x574b49486f7374ULL;
                    entries[entries_read].d_off = actual_index + 1;
                    entries[entries_read].d_reclen = sizeof(DirEntry);
                    entries[entries_read].d_type = DT_DIR | DT_WOSLINK;
                    std::memcpy(entries[entries_read].d_name.data(), "host", 5);
                    entries_read++;
                    continue;
                }
                synthetic_index--;
            }

            size_t mount_idx = synthetic_index;

            size_t dir_len = std::strlen(f->vfs_path);
            size_t child_count = 0;

            for (size_t mi = 0; mi < get_mount_count(); ++mi) {
                MountPoint* mp = get_mount_at(mi);
                if (mp == nullptr || mp->path == nullptr) {
                    continue;
                }

                size_t mp_len = std::strlen(mp->path);
                const char* child_start = nullptr;
                size_t child_len = 0;

                if (dir_len == 1 && f->vfs_path[0] == '/') {
                    // Root directory: child is first component of "/xxx[/...]"
                    if (mp_len > 1 && mp->path[0] == '/') {
                        child_start = mp->path + 1;
                    }
                } else {
                    // Non-root: mount must start with dir_path + "/"
                    if (mp_len > dir_len && std::memcmp(mp->path, f->vfs_path, dir_len) == 0 && mp->path[dir_len] == '/') {
                        child_start = mp->path + dir_len + 1;
                    }
                }

                if (child_start == nullptr || *child_start == '\0') continue;

                // Extract only the first path component
                const char* p = child_start;
                while (*p != '\0' && *p != '/') p++;
                child_len = static_cast<size_t>(p - child_start);
                if (child_len == 0) continue;

                // Dedup against earlier mounts that yield the same child name
                bool dup_mount = false;
                for (size_t mj = 0; mj < mi; ++mj) {
                    MountPoint* mp2 = get_mount_at(mj);
                    if (mp2 == nullptr || mp2->path == nullptr) continue;
                    size_t mp2_len = std::strlen(mp2->path);
                    const char* c2 = nullptr;

                    if (dir_len == 1 && f->vfs_path[0] == '/') {
                        if (mp2_len > 1 && mp2->path[0] == '/') c2 = mp2->path + 1;
                    } else {
                        if (mp2_len > dir_len && std::memcmp(mp2->path, f->vfs_path, dir_len) == 0 && mp2->path[dir_len] == '/') {
                            c2 = mp2->path + dir_len + 1;
                        }
                    }
                    if (c2 == nullptr || *c2 == '\0') continue;

                    const char* p2 = c2;
                    while (*p2 != '\0' && *p2 != '/') p2++;
                    size_t c2_len = static_cast<size_t>(p2 - c2);

                    if (c2_len == child_len && std::memcmp(child_start, c2, child_len) == 0) {
                        dup_mount = true;
                        break;
                    }
                }
                if (dup_mount) continue;

                // Dedup against FS readdir entries
                if (has_fs_readdir && fs_count > 0) {
                    bool already_in_fs = false;
                    DirEntry probe = {};
                    for (size_t pi = 0; pi < fs_count; ++pi) {
                        int pret = f->fops->vfs_readdir(f, &probe, pi);
                        if (pret != 0) break;
                        size_t dn_len = std::strlen(probe.d_name.data());
                        if (dn_len == child_len && std::memcmp(probe.d_name.data(), child_start, child_len) == 0) {
                            already_in_fs = true;
                            break;
                        }
                    }
                    if (already_in_fs) continue;
                }

                if (child_count == mount_idx) {
                    // Fill the synthetic DirEntry
                    entries[entries_read].d_ino = reinterpret_cast<uint64_t>(mp);
                    entries[entries_read].d_off = actual_index + 1;
                    entries[entries_read].d_reclen = sizeof(DirEntry);

                    // Mark WKI entries with WOSLINK flag for recursion prevention:
                    // - listing /wki: all mount children (wos-0, wos-1, ...) are WOSLINK
                    // - listing /: the "wki" mount child is WOSLINK
                    if (std::strcmp(f->vfs_path, "/wki") == 0 ||
                        (dir_len == 1 && f->vfs_path[0] == '/' && child_len == 3 && std::memcmp(child_start, "wki", 3) == 0)) {
                        entries[entries_read].d_type = DT_DIR | DT_WOSLINK;
                    } else {
                        entries[entries_read].d_type = DT_DIR;
                    }

                    size_t copy_len = child_len < DIRENT_NAME_MAX - 1 ? child_len : DIRENT_NAME_MAX - 1;
                    std::memcpy(entries[entries_read].d_name.data(), child_start, copy_len);
                    entries[entries_read].d_name[copy_len] = '\0';

                    entries_read++;
                    found_mount_child = true;
                    break;
                }
                child_count++;
            }
        }

        if (found_mount_child) {
            continue;
        }

        // No more entries from either FS or mount children
        break;
    }

    // Update file position
    f->pos += static_cast<off_t>(entries_read);

    return static_cast<ssize_t>(entries_read * sizeof(DirEntry));
}

// --- Symlink / mkdir / mount operations ---

auto vfs_symlink(const char* target, const char* linkpath) -> int {
    if (target == nullptr || linkpath == nullptr) {
        return -EINVAL;
    }

    char absLinkpath[MAX_PATH_LEN];
    if (resolve_task_path_raw(linkpath, absLinkpath, MAX_PATH_LEN) < 0) return -ENOENT;

    // Find mount point for the linkpath
    MountPoint* mount = find_mount_point(absLinkpath);
    if (mount == nullptr) {
        return -ENOENT;
    }

    // Only tmpfs supports symlinks
    if (mount->fs_type != FSType::TMPFS) {
        return -ENOSYS;
    }

    const char* fs_path = strip_mount_prefix(mount, absLinkpath);

    // Split into parent path and link name
    const char* last_slash = nullptr;
    for (const char* p = fs_path; *p != '\0'; ++p) {
        if (*p == '/') {
            last_slash = p;
        }
    }

    ker::vfs::tmpfs::TmpNode* parent = nullptr;
    const char* link_name = nullptr;

    if (last_slash == nullptr) {
        parent = ker::vfs::tmpfs::get_root_node();
        link_name = fs_path;
    } else {
        std::array<char, MAX_PATH_LEN> parent_path{};
        auto parent_len = static_cast<size_t>(last_slash - fs_path);
        if (parent_len >= MAX_PATH_LEN) {
            return -ENAMETOOLONG;
        }
        memcpy(parent_path.data(), fs_path, parent_len);
        parent_path[parent_len] = '\0';
        parent = ker::vfs::tmpfs::tmpfs_walk_path(parent_path.data(), true);
        link_name = last_slash + 1;
    }

    if (parent == nullptr || link_name == nullptr || *link_name == '\0') {
        return -ENOENT;
    }

    auto* node = ker::vfs::tmpfs::tmpfs_create_symlink(parent, link_name, target);
    return (node != nullptr) ? 0 : -1;
}

// Internal readlink operating on an already-resolved absolute path (no root
// prefix applied).  Used by resolve_prefix_symlink_once which works on paths
// that already include the task root.
auto readlink_resolved(const char* absPath, char* buf, size_t bufsize) -> ssize_t {
    if (absPath == nullptr || buf == nullptr || bufsize == 0) {
        return -EINVAL;
    }

    MountPoint* mount = find_mount_point(absPath);
    if (mount == nullptr) {
        return -ENOENT;
    }

    if (mount->fs_type == FSType::PROCFS) {
        const char* fsp = strip_mount_prefix(mount, absPath);

        auto* f = ker::vfs::procfs::procfs_open_path(fsp, 0, 0);
        if (f == nullptr) return -ENOENT;
        ssize_t ret = ker::vfs::procfs::get_procfs_fops()->vfs_readlink(f, buf, bufsize);
        ker::vfs::procfs::get_procfs_fops()->vfs_close(f);
        ker::mod::mm::dyn::kmalloc::free(f);
        return ret;
    }

    if (mount->fs_type == FSType::REMOTE) {
        const char* fs_path = strip_mount_prefix(mount, absPath);
        return ker::net::wki::wki_remote_vfs_readlink_path(mount->private_data, fs_path, buf, bufsize);
    }

    if (mount->fs_type == FSType::XFS) {
        const char* fs_path = strip_mount_prefix(mount, absPath);
        auto* xctx = static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data);
        auto* f = ker::vfs::xfs::xfs_open_path(fs_path, 0, 0, xctx);
        if (f == nullptr) return -ENOENT;
        ssize_t ret = ker::vfs::xfs::get_xfs_fops()->vfs_readlink(f, buf, bufsize);
        ker::vfs::xfs::get_xfs_fops()->vfs_close(f);
        ker::mod::mm::dyn::kmalloc::free(f);
        return ret;
    }

    if (mount->fs_type == FSType::DEVFS) {
        const char* fs_path = strip_mount_prefix(mount, absPath);

        auto* f = ker::vfs::devfs::devfs_open_path(fs_path, 0, 0);
        if (f == nullptr) {
            return -ENOENT;
        }

        if (f->fops == nullptr || f->fops->vfs_readlink == nullptr) {
            if (f->fops != nullptr && f->fops->vfs_close != nullptr) {
                f->fops->vfs_close(f);
            }
            ker::mod::mm::dyn::kmalloc::free(f);
            return -ENOSYS;
        }

        ssize_t ret = f->fops->vfs_readlink(f, buf, bufsize);
        if (f->fops->vfs_close != nullptr) {
            f->fops->vfs_close(f);
        }
        ker::mod::mm::dyn::kmalloc::free(f);
        return ret;
    }

    if (mount->fs_type != FSType::TMPFS) {
        return -ENOSYS;
    }

    const char* fs_path = strip_mount_prefix(mount, absPath);

    auto* node = ker::vfs::tmpfs::tmpfs_walk_path(fs_path, false);
    if (node == nullptr) {
        return -ENOENT;
    }

    if (node->type != ker::vfs::tmpfs::TmpNodeType::SYMLINK || node->symlink_target == nullptr) {
        return -EINVAL;
    }

    size_t len = 0;
    while (node->symlink_target[len] != '\0') {
        len++;
    }
    size_t to_copy = (len < bufsize) ? len : bufsize;
    memcpy(buf, node->symlink_target, to_copy);
    return static_cast<ssize_t>(to_copy);
}

auto vfs_readlink(const char* path, char* buf, size_t bufsize) -> ssize_t {
    if (path == nullptr || buf == nullptr || bufsize == 0) {
        return -EINVAL;
    }

    char absPath[MAX_PATH_LEN];
    if (resolve_task_path_raw(path, absPath, MAX_PATH_LEN) < 0) return -ENOENT;

    return readlink_resolved(absPath, buf, bufsize);
}

auto vfs_mkdir(const char* path, int mode) -> int {
    (void)mode;
    if (path == nullptr) {
        return -EINVAL;
    }

    char absPath[MAX_PATH_LEN];
    if (resolve_task_path_raw(path, absPath, MAX_PATH_LEN) < 0) return -ENOENT;

    MountPoint* mount = find_mount_point(absPath);
    if (mount == nullptr) {
        return -ENOENT;
    }

    if (mount->fs_type != FSType::TMPFS) {
        return -ENOSYS;  // Only tmpfs supports mkdir for now
    }

    const char* fs_path = strip_mount_prefix(mount, absPath);

    auto* node = ker::vfs::tmpfs::tmpfs_walk_path(fs_path, true);
    return (node != nullptr) ? 0 : -1;
}

auto vfs_stat(const char* path, stat* statbuf) -> int {
    if (path == nullptr || statbuf == nullptr) {
        return -EINVAL;
    }

    // WOSLINK detection: compute canonical pre-rewrite path to detect /wki
    // entries before resolve_task_path_raw rewrites them (e.g., /wki/host → /).
    bool is_wki_entry = false;
    {
        char pre_rewrite[MAX_PATH_LEN] = {};
        if (make_absolute(path, pre_rewrite, MAX_PATH_LEN) == 0 && canonicalize_path(pre_rewrite, MAX_PATH_LEN) == 0) {
            if (std::strcmp(pre_rewrite, "/wki") == 0) {
                is_wki_entry = true;
            } else if (std::strncmp(pre_rewrite, "/wki/", 5) == 0) {
                // Direct child of /wki (one component, no further slashes)
                const char* child = pre_rewrite + 5;
                bool has_slash = false;
                for (const char* p = child; *p != '\0'; ++p) {
                    if (*p == '/') {
                        has_slash = true;
                        break;
                    }
                }
                if (*child != '\0' && !has_slash) {
                    is_wki_entry = true;
                }
            }
        }
    }

    char pathBuffer[MAX_PATH_LEN];  // NOLINT
    if (resolve_task_path_raw(path, pathBuffer, MAX_PATH_LEN) < 0) {
        return -ENOENT;
    }

    char resolved[MAX_PATH_LEN];  // NOLINT
    int resolve_ret = resolve_symlinks(pathBuffer, resolved, MAX_PATH_LEN, true);
    if (resolve_ret == -ELOOP) {
        return -ELOOP;
    }
    if (resolve_ret < 0) {
        return resolve_ret;
    }
    std::memcpy(pathBuffer, resolved, MAX_PATH_LEN);

    // Post-rewrite WOSLINK check: after host alias rewriting, deeper paths
    // like /wki/host/wki resolve to /wki, and /wki/host/wki/wos-1 resolves
    // to /wki/wos-1.  Catch these by examining the resolved pathBuffer.
    if (!is_wki_entry) {
        if (std::strcmp(pathBuffer, "/wki") == 0) {
            is_wki_entry = true;
        } else if (std::strncmp(pathBuffer, "/wki/", 5) == 0) {
            const char* child = pathBuffer + 5;
            bool has_slash = false;
            for (const char* p = child; *p != '\0'; ++p) {
                if (*p == '/') {
                    has_slash = true;
                    break;
                }
            }
            if (*child != '\0' && !has_slash) {
                is_wki_entry = true;
            }
        }
    }

    // Find mount point
    MountPoint* mount = find_mount_point(pathBuffer);
    if (mount == nullptr) {
        return -ENOENT;
    }

    const char* fs_path = strip_mount_prefix(mount, pathBuffer);

    // Initialize stat buffer
    memset(statbuf, 0, sizeof(stat));

    int result = -ENOSYS;

    switch (mount->fs_type) {
        case FSType::TMPFS: {
            auto* node = ker::vfs::tmpfs::tmpfs_walk_path(fs_path, false);
            if (node == nullptr) {
                return -ENOENT;
            }
            statbuf->st_dev = 0;
            statbuf->st_ino = reinterpret_cast<ino_t>(node);  // Use node address as inode
            statbuf->st_nlink = 1;
            statbuf->st_uid = node->uid;
            statbuf->st_gid = node->gid;
            statbuf->st_rdev = 0;
            statbuf->st_size = static_cast<off_t>(node->size);
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = (node->size + 511) / 512;
            switch (node->type) {
                case ker::vfs::tmpfs::TmpNodeType::FILE:
                    statbuf->st_mode = S_IFREG | node->mode;
                    break;
                case ker::vfs::tmpfs::TmpNodeType::DIRECTORY:
                    statbuf->st_mode = S_IFDIR | node->mode;
                    break;
                case ker::vfs::tmpfs::TmpNodeType::SYMLINK:
                    statbuf->st_mode = S_IFLNK | node->mode;
                    break;
            }
            {
                uint64_t now_ns = ker::mod::rtc::getEpochNs();
                statbuf->st_atim.tv_sec = static_cast<int64_t>(now_ns / 1000000000ULL);
                statbuf->st_atim.tv_nsec = static_cast<int64_t>(now_ns % 1000000000ULL);
                statbuf->st_mtim = statbuf->st_atim;
                statbuf->st_ctim = statbuf->st_atim;
            }
            result = 0;
            break;
        }
        case FSType::FAT32: {
            result = ker::vfs::fat32::fat32_stat(fs_path, statbuf, static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data));
            break;
        }
        case FSType::DEVFS: {
            // Walk devfs tree to determine if directory or device
            auto* node = ker::vfs::devfs::devfs_walk_path(fs_path);
            if (node == nullptr) {
                return -ENOENT;
            }
            statbuf->st_dev = 0;
            statbuf->st_ino = reinterpret_cast<ino_t>(node);
            statbuf->st_nlink = 1;
            statbuf->st_uid = node->uid;
            statbuf->st_gid = node->gid;
            statbuf->st_rdev = 0;
            statbuf->st_size = 0;
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = 0;

            // Set mode based on node type
            if (node->type == ker::vfs::devfs::DevFSNodeType::DIRECTORY) {
                statbuf->st_mode = S_IFDIR | node->mode;
            } else if (node->type == ker::vfs::devfs::DevFSNodeType::SYMLINK) {
                statbuf->st_mode = S_IFLNK | 0777;
            } else if (node->device != nullptr && node->device->type == ker::dev::DeviceType::BLOCK) {
                statbuf->st_mode = S_IFBLK | node->mode;
            } else {
                statbuf->st_mode = S_IFCHR | node->mode;
            }
            {
                uint64_t now_ns = ker::mod::rtc::getEpochNs();
                statbuf->st_atim.tv_sec = static_cast<int64_t>(now_ns / 1000000000ULL);
                statbuf->st_atim.tv_nsec = static_cast<int64_t>(now_ns % 1000000000ULL);
                statbuf->st_mtim = statbuf->st_atim;
                statbuf->st_ctim = statbuf->st_atim;
            }
            result = 0;
            break;
        }
        case FSType::REMOTE: {
            result = ker::net::wki::wki_remote_vfs_stat(mount->private_data, fs_path, statbuf);
            break;
        }
        case FSType::PROCFS: {
            // procfs: open the path, check if it exists
            auto* f = ker::vfs::procfs::procfs_open_path(fs_path, 0, 0);
            if (f == nullptr) return -ENOENT;
            statbuf->st_dev = 0;
            statbuf->st_ino = 1;
            statbuf->st_nlink = 1;
            statbuf->st_uid = 0;
            statbuf->st_gid = 0;
            statbuf->st_rdev = 0;
            statbuf->st_size = 0;
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = 0;
            auto* pfd = static_cast<ker::vfs::procfs::ProcFileData*>(f->private_data);
            // Set uid/gid to the actual process owner for PID-based entries
            if (pfd != nullptr && pfd->node.pid != 0) {
                auto* ptask = ker::mod::sched::find_task_by_pid(pfd->node.pid);
                if (ptask != nullptr) {
                    statbuf->st_uid = ptask->uid;
                    statbuf->st_gid = ptask->gid;
                }
            }
            if (f->is_directory) {
                statbuf->st_mode = S_IFDIR | 0555;
            } else {
                if (pfd != nullptr && (pfd->node.type == ker::vfs::procfs::ProcNodeType::EXE_LINK ||
                                       pfd->node.type == ker::vfs::procfs::ProcNodeType::SELF_LINK)) {
                    statbuf->st_mode = S_IFLNK | 0777;
                } else {
                    statbuf->st_mode = S_IFREG | 0444;
                }
            }
            {
                uint64_t now_ns = ker::mod::rtc::getEpochNs();
                statbuf->st_atim.tv_sec = static_cast<int64_t>(now_ns / 1000000000ULL);
                statbuf->st_atim.tv_nsec = static_cast<int64_t>(now_ns % 1000000000ULL);
                statbuf->st_mtim = statbuf->st_atim;
                statbuf->st_ctim = statbuf->st_atim;
            }
            // Clean up temporary file (allocated with new in procfs_open_path)
            ker::vfs::procfs::get_procfs_fops()->vfs_close(f);
            delete f;
            result = 0;
            break;
        }
        case FSType::XFS: {
            result = ker::vfs::xfs::xfs_stat(fs_path, statbuf, static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data));
            break;
        }
        default:
            return -ENOSYS;
    }

    // WOSLINK post-processing: mark WKI entry directories with S_WOSLINK
    // so userspace tools (e.g., ls -R) can avoid infinite recursion through
    // /wki/host/wki/host/... or /wki/node-1/wki/node-0/wki/node-1/...
    if (result == 0 && is_wki_entry && (statbuf->st_mode & S_IFMT) == S_IFDIR) {
        statbuf->st_mode |= S_WOSLINK;
    }

    return result;
}

auto vfs_fstat(int fd, stat* statbuf) -> int {
    if (statbuf == nullptr) {
        return -EINVAL;
    }

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }

    auto* file = vfs_get_file(task, fd);
    if (file == nullptr) {
        return -EBADF;
    }

    // Initialize stat buffer
    memset(statbuf, 0, sizeof(stat));

    switch (file->fs_type) {
        case FSType::TMPFS: {
            auto* node = static_cast<ker::vfs::tmpfs::TmpNode*>(file->private_data);
            if (node == nullptr) {
                return -EBADF;
            }
            statbuf->st_dev = 0;
            statbuf->st_ino = reinterpret_cast<ino_t>(node);
            statbuf->st_nlink = 1;
            statbuf->st_uid = node->uid;
            statbuf->st_gid = node->gid;
            statbuf->st_rdev = 0;
            statbuf->st_size = static_cast<off_t>(node->size);
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = (node->size + 511) / 512;
            switch (node->type) {
                case ker::vfs::tmpfs::TmpNodeType::FILE:
                    statbuf->st_mode = S_IFREG | node->mode;
                    break;
                case ker::vfs::tmpfs::TmpNodeType::DIRECTORY:
                    statbuf->st_mode = S_IFDIR | node->mode;
                    break;
                case ker::vfs::tmpfs::TmpNodeType::SYMLINK:
                    statbuf->st_mode = S_IFLNK | node->mode;
                    break;
            }
            {
                uint64_t now_ns = ker::mod::rtc::getEpochNs();
                statbuf->st_atim.tv_sec = static_cast<int64_t>(now_ns / 1000000000ULL);
                statbuf->st_atim.tv_nsec = static_cast<int64_t>(now_ns % 1000000000ULL);
                statbuf->st_mtim = statbuf->st_atim;
                statbuf->st_ctim = statbuf->st_atim;
            }
            return 0;
        }
        case FSType::FAT32: {
            return ker::vfs::fat32::fat32_fstat(file, statbuf);
        }
        case FSType::DEVFS: {
            auto* node = static_cast<ker::vfs::devfs::DevFSNode*>(file->private_data);
            statbuf->st_dev = 0;
            statbuf->st_ino = node ? reinterpret_cast<ino_t>(node) : 1;
            statbuf->st_nlink = 1;
            statbuf->st_uid = node ? node->uid : 0;
            statbuf->st_gid = node ? node->gid : 0;
            statbuf->st_rdev = 0;
            statbuf->st_size = 0;
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = 0;

            // Set mode based on whether this is a directory or device
            if (file->is_directory) {
                statbuf->st_mode = S_IFDIR | (node ? node->mode : 0755);
            } else {
                statbuf->st_mode = S_IFCHR | (node ? node->mode : 0666);
            }
            {
                uint64_t now_ns = ker::mod::rtc::getEpochNs();
                statbuf->st_atim.tv_sec = static_cast<int64_t>(now_ns / 1000000000ULL);
                statbuf->st_atim.tv_nsec = static_cast<int64_t>(now_ns % 1000000000ULL);
                statbuf->st_mtim = statbuf->st_atim;
                statbuf->st_ctim = statbuf->st_atim;
            }
            return 0;
        }
        case FSType::SOCKET: {
            statbuf->st_dev = 0;
            statbuf->st_ino = 1;
            statbuf->st_nlink = 1;
            statbuf->st_mode = S_IFSOCK | 0666;
            statbuf->st_uid = 0;
            statbuf->st_gid = 0;
            statbuf->st_rdev = 0;
            statbuf->st_size = 0;
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = 0;
            return 0;
        }
        case FSType::REMOTE: {
            if (file->vfs_path != nullptr) {
                MountPoint* mount = find_mount_point(file->vfs_path);
                if (mount != nullptr && mount->fs_type == FSType::REMOTE) {
                    const char* fs_path = strip_mount_prefix(mount, file->vfs_path);
                    int ret = ker::net::wki::wki_remote_vfs_stat(mount->private_data, fs_path, statbuf);
                    if (ret == 0) {
                        return 0;
                    }
                }
            }

            // Fall back to a synthetic stat if path-based remote metadata lookup fails.
            statbuf->st_dev = 0;
            statbuf->st_ino = 1;
            statbuf->st_nlink = 1;
            statbuf->st_uid = 0;
            statbuf->st_gid = 0;
            statbuf->st_rdev = 0;
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = 0;
            if (file->is_directory) {
                statbuf->st_mode = S_IFDIR | S_WOSLINK | 0755;
                statbuf->st_size = 0;
            } else {
                statbuf->st_mode = S_IFREG | 0644;
                statbuf->st_size = 0;
            }
            return 0;
        }
        case FSType::XFS: {
            return ker::vfs::xfs::xfs_fstat(file, statbuf);
        }
        default:
            return -ENOSYS;
    }
}

// --- umount ---
auto vfs_umount(const char* target) -> int {
    // Resolve path through the task's root prefix.
    char resolved[MAX_PATH_LEN];
    if (resolve_task_path_raw(target, resolved, MAX_PATH_LEN) < 0) {
        return -ENAMETOOLONG;
    }
    return unmount_filesystem(resolved);
}

// --- pivot_root ---
auto vfs_pivot_root(const char* new_root, const char* put_old) -> int {
    if (new_root == nullptr || put_old == nullptr) {
        return -EINVAL;
    }

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }

    // Validate that new_root is an existing mount point
    MountPoint* new_mount = find_mount_point(new_root);
    if (new_mount == nullptr) {
        ker::mod::dbg::log("pivot_root: new_root '%s' is not a mount point", new_root);
        return -EINVAL;
    }

    // Check that new_root path matches the mount point exactly
    if (std::strcmp(new_mount->path, new_root) != 0) {
        ker::mod::dbg::log("pivot_root: new_root '%s' is not an exact mount point (found '%s')", new_root, new_mount->path);
        return -EINVAL;
    }

    // Rename the old root mount from "/" to put_old so it can be accessed
    // (and unmounted) under the new root.
    MountPoint* old_root_mount = find_mount_point("/");
    if (old_root_mount != nullptr && std::strcmp(old_root_mount->path, "/") == 0) {
        size_t put_old_len = std::strlen(put_old);
        auto* new_path = static_cast<char*>(ker::mod::mm::dyn::kmalloc::malloc(put_old_len + 1));
        if (new_path != nullptr) {
            std::memcpy(new_path, put_old, put_old_len + 1);
            ker::mod::mm::dyn::kmalloc::free(const_cast<char*>(old_root_mount->path));
            old_root_mount->path = new_path;
        }
    }

    // Remap child mounts of the old root so they remain accessible under
    // the new root.  After pivot_root("/rootfs", ...), a devfs at "/dev"
    // must become "/rootfs/dev" because task path resolution will prepend
    // the root prefix, turning user "/dev/urandom" into "/rootfs/dev/urandom".
    size_t new_root_len = std::strlen(new_root);
    size_t mount_count = get_mount_count();
    for (size_t i = 0; i < mount_count; ++i) {
        MountPoint* mp = get_mount_at(i);
        if (mp == nullptr || mp->path == nullptr) continue;

        // Skip the new root mount itself
        if (std::strcmp(mp->path, new_root) == 0) continue;

        // Skip mounts already under the new root (including the just-renamed old root)
        if (std::strncmp(mp->path, new_root, new_root_len) == 0 && (mp->path[new_root_len] == '/' || mp->path[new_root_len] == '\0'))
            continue;

        // This mount is under the old root — prefix it with new_root
        size_t mp_len = std::strlen(mp->path);
        auto* remapped = static_cast<char*>(ker::mod::mm::dyn::kmalloc::malloc(new_root_len + mp_len + 1));
        if (remapped != nullptr) {
            std::memcpy(remapped, new_root, new_root_len);
            std::memcpy(remapped + new_root_len, mp->path, mp_len + 1);
            ker::mod::dbg::log("pivot_root: remapped mount '%s' -> '%s'", mp->path, remapped);
            ker::mod::mm::dyn::kmalloc::free(const_cast<char*>(mp->path));
            mp->path = remapped;
        }
    }

    // Set the calling task's root to new_root.
    // After this, all absolute path resolution for this task (and its children)
    // will prepend this prefix, effectively making new_root the apparent "/".
    if (new_root_len >= ker::mod::sched::task::Task::CWD_MAX) {
        return -ENAMETOOLONG;
    }
    std::memcpy(task->root, new_root, new_root_len + 1);

    ker::mod::dbg::log("pivot_root: task '%s' (pid %x) root set to '%s'", task->name, task->pid, new_root);

    return 0;
}

// --- dup / dup2 ---
auto vfs_dup(int oldfd) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;
    auto* f = vfs_get_file(task, oldfd);
    if (f == nullptr) return -EBADF;
    __atomic_add_fetch(&f->refcount, 1, __ATOMIC_ACQ_REL);
    int newfd = vfs_alloc_fd(task, f);
    if (newfd < 0) {
        __atomic_sub_fetch(&f->refcount, 1, __ATOMIC_ACQ_REL);
        return -EMFILE;
    }
    return newfd;
}

auto vfs_dup2(int oldfd, int newfd) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;
    if (newfd < 0 || static_cast<unsigned>(newfd) >= ker::mod::sched::task::Task::FD_TABLE_SIZE) return -EBADF;
    auto* f = vfs_get_file(task, oldfd);
    if (f == nullptr) return -EBADF;
    if (oldfd == newfd) return newfd;
    // Close newfd if it's open
    auto* existing = vfs_get_file(task, newfd);
    if (existing != nullptr) {
        vfs_close(newfd);
    }
    __atomic_add_fetch(&f->refcount, 1, __ATOMIC_ACQ_REL);
    task->fd_table.insert(static_cast<uint64_t>(newfd), f);
    // POSIX: dup2 does NOT inherit close-on-exec from the source fd
    task->clear_fd_cloexec(static_cast<unsigned>(newfd));
    return newfd;
}

// --- getcwd / chdir ---
auto vfs_getcwd(char* buf, size_t size) -> int {
    if (buf == nullptr || size == 0) return -EINVAL;
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;
    size_t len = std::strlen(task->cwd);
    if (len + 1 > size) return -ERANGE;
    std::memcpy(buf, task->cwd, len + 1);
    return 0;
}

auto vfs_chdir(const char* path) -> int {
    if (path == nullptr) return -EINVAL;
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;

    char logical[MAX_PATH_LEN] = {};
    int absolute = make_absolute(path, logical, MAX_PATH_LEN);
    if (absolute < 0) {
        return absolute;
    }

    int canonical = canonicalize_path(logical, MAX_PATH_LEN);
    if (canonical < 0) {
        return canonical;
    }

    // Verify the path is a directory.  vfs_stat handles root-prefix
    // resolution internally, so pass the logical (user-visible) path.
    ker::vfs::stat st{};
    int ret = vfs_stat(logical, &st);
    if (ret < 0) return ret;
    if ((st.st_mode & S_IFDIR) == 0) return -ENOTDIR;

    // Copy to task cwd
    size_t rlen = std::strlen(logical);
    if (rlen + 1 > ker::mod::sched::task::Task::CWD_MAX) return -ENAMETOOLONG;
    std::memcpy(task->cwd, logical, rlen + 1);
    return 0;
}

// --- access ---
// R_OK=4, W_OK=2, X_OK=1, F_OK=0
auto vfs_check_permission(uint32_t file_mode, uint32_t file_uid, uint32_t file_gid, int access_bits) -> int {
    if (access_bits == 0) return 0;  // F_OK - existence only

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;

    // Root can do anything (except execute if no execute bit set anywhere)
    if (task->euid == 0) {
        if ((access_bits & 1) && !(file_mode & 0111)) {
            return -EACCES;  // No execute bit set at all
        }
        return 0;
    }

    uint32_t perm_bits;
    if (task->euid == file_uid) {
        perm_bits = (file_mode >> 6) & 7;  // Owner bits
    } else if (task->egid == file_gid) {
        perm_bits = (file_mode >> 3) & 7;  // Group bits
    } else {
        perm_bits = file_mode & 7;  // Other bits
    }

    if ((access_bits & 4) && !(perm_bits & 4)) return -EACCES;  // R_OK
    if ((access_bits & 2) && !(perm_bits & 2)) return -EACCES;  // W_OK
    if ((access_bits & 1) && !(perm_bits & 1)) return -EACCES;  // X_OK
    return 0;
}

auto vfs_access(const char* path, int mode) -> int {
    if (path == nullptr) {
        return -EINVAL;
    }

    // Check existence first
    ker::vfs::stat st{};
    int ret = vfs_stat(path, &st);
    if (ret < 0) {
        return ret;
    }

    if (mode == 0) {
        return 0;  // F_OK - just existence check
    }

    // st_mode already has the full mode bits from stat
    return vfs_check_permission(st.st_mode & 07777, st.st_uid, st.st_gid, mode);
}

// --- pread / pwrite ---
auto vfs_pread(int fd, void* buf, size_t count, off_t offset) -> ssize_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;
    auto* f = vfs_get_file(task, fd);
    if (f == nullptr) return -EBADF;
    if (f->fops == nullptr || f->fops->vfs_read == nullptr) return -ENOSYS;
    // Read at given offset without modifying file position
    return f->fops->vfs_read(f, buf, count, static_cast<size_t>(offset));
}

auto vfs_pwrite(int fd, const void* buf, size_t count, off_t offset) -> ssize_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;
    auto* f = vfs_get_file(task, fd);
    if (f == nullptr) return -EBADF;
    if (f->fops == nullptr || f->fops->vfs_write == nullptr) return -ENOSYS;
    return f->fops->vfs_write(f, buf, count, static_cast<size_t>(offset));
}

// --- unlink ---
auto vfs_unlink(const char* path) -> int {
    if (path == nullptr) return -EINVAL;

    char path_buf[MAX_PATH_LEN];
    if (resolve_task_path_raw(path, path_buf, MAX_PATH_LEN) < 0) return -ENAMETOOLONG;

    MountPoint* mount = find_mount_point(path_buf);
    if (mount == nullptr) return -ENOENT;

    if (mount->fs_type == FSType::XFS) {
        const char* fs_path = strip_mount_prefix(mount, path_buf);
        return ker::vfs::xfs::xfs_unlink_path(fs_path, static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data));
    }

    if (mount->fs_type == FSType::FAT32) {
        const char* fs_path = strip_mount_prefix(mount, path_buf);
        return ker::vfs::fat32::fat32_unlink_path(static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data), fs_path);
    }

    if (mount->fs_type == FSType::REMOTE) {
        const char* fs_path = strip_mount_prefix(mount, path_buf);
        return ker::net::wki::wki_remote_vfs_unlink(mount->private_data, fs_path);
    }

    if (mount->fs_type != FSType::TMPFS) return -ENOSYS;

    const char* fs_path = strip_mount_prefix(mount, path_buf);

    // Walk to parent, then find child
    const char* last_slash = nullptr;
    for (const char* p = fs_path; *p; ++p) {
        if (*p == '/') last_slash = p;
    }

    ker::vfs::tmpfs::TmpNode* parent = nullptr;
    const char* name = nullptr;

    if (last_slash == nullptr) {
        parent = ker::vfs::tmpfs::get_root_node();
        name = fs_path;
    } else {
        char parent_path[MAX_PATH_LEN];
        auto paren_len = static_cast<size_t>(last_slash - fs_path);
        if (paren_len >= MAX_PATH_LEN) return -ENAMETOOLONG;
        std::memcpy(parent_path, fs_path, paren_len);
        parent_path[paren_len] = '\0';
        parent = ker::vfs::tmpfs::tmpfs_walk_path(parent_path, false);
        name = last_slash + 1;
    }

    if (parent == nullptr || name == nullptr || *name == '\0') return -ENOENT;

    auto* child = ker::vfs::tmpfs::tmpfs_lookup(parent, name);
    if (child == nullptr) return -ENOENT;
    if (child->type == ker::vfs::tmpfs::TmpNodeType::DIRECTORY) return -EISDIR;

    // Remove child from parent's children array
    for (size_t i = 0; i < parent->children_count; ++i) {
        if (parent->children[i] == child) {
            // Shift remaining children down
            for (size_t j = i; j + 1 < parent->children_count; ++j) {
                parent->children[j] = parent->children[j + 1];
            }
            parent->children_count--;
            parent->children[parent->children_count] = nullptr;
            // Free node data
            delete[] child->data;
            delete[] child->symlink_target;
            delete child;
            return 0;
        }
    }
    return -ENOENT;
}

// --- rmdir ---
auto vfs_rmdir(const char* path) -> int {
    if (path == nullptr) return -EINVAL;

    char pathBuf[MAX_PATH_LEN];
    if (resolve_task_path_raw(path, pathBuf, MAX_PATH_LEN) < 0) return -ENAMETOOLONG;

    MountPoint* mount = find_mount_point(pathBuf);
    if (mount == nullptr) return -ENOENT;

    if (mount->fs_type == FSType::FAT32) {
        const char* fs_path = strip_mount_prefix(mount, pathBuf);
        return ker::vfs::fat32::fat32_rmdir_path(static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data), fs_path);
    }

    if (mount->fs_type == FSType::REMOTE) {
        const char* fs_path = strip_mount_prefix(mount, pathBuf);
        return ker::net::wki::wki_remote_vfs_rmdir(mount->private_data, fs_path);
    }

    if (mount->fs_type != FSType::TMPFS) return -ENOSYS;

    const char* fs_path = strip_mount_prefix(mount, pathBuf);

    const char* last_slash = nullptr;
    for (const char* p = fs_path; *p; ++p) {
        if (*p == '/') last_slash = p;
    }

    ker::vfs::tmpfs::TmpNode* parent = nullptr;
    const char* name = nullptr;

    if (last_slash == nullptr) {
        parent = ker::vfs::tmpfs::get_root_node();
        name = fs_path;
    } else {
        char parent_path[MAX_PATH_LEN];
        auto paren_len = static_cast<size_t>(last_slash - fs_path);
        if (paren_len >= MAX_PATH_LEN) return -ENAMETOOLONG;
        std::memcpy(parent_path, fs_path, paren_len);
        parent_path[paren_len] = '\0';
        parent = ker::vfs::tmpfs::tmpfs_walk_path(parent_path, false);
        name = last_slash + 1;
    }

    if (parent == nullptr || name == nullptr || *name == '\0') return -ENOENT;

    auto* child = ker::vfs::tmpfs::tmpfs_lookup(parent, name);
    if (child == nullptr) return -ENOENT;
    if (child->type != ker::vfs::tmpfs::TmpNodeType::DIRECTORY) return -ENOTDIR;
    if (child->children_count > 0) return -ENOTEMPTY;

    for (size_t i = 0; i < parent->children_count; ++i) {
        if (parent->children[i] == child) {
            for (size_t j = i; j + 1 < parent->children_count; ++j) {
                parent->children[j] = parent->children[j + 1];
            }
            parent->children_count--;
            parent->children[parent->children_count] = nullptr;
            delete[] child->children;
            delete child;
            return 0;
        }
    }
    return -ENOENT;
}

// --- rename ---
auto vfs_rename(const char* oldpath, const char* newpath) -> int {
    if (oldpath == nullptr || newpath == nullptr) return -EINVAL;

    char oldBuf[MAX_PATH_LEN], newBuf[MAX_PATH_LEN];
    if (resolve_task_path_raw(oldpath, oldBuf, MAX_PATH_LEN) < 0) return -ENAMETOOLONG;
    if (resolve_task_path_raw(newpath, newBuf, MAX_PATH_LEN) < 0) return -ENAMETOOLONG;

    MountPoint* oldMount = find_mount_point(oldBuf);
    MountPoint* newMount = find_mount_point(newBuf);
    if (!oldMount || !newMount) return -ENOENT;

    if (oldMount->fs_type == FSType::FAT32 && newMount->fs_type == FSType::FAT32 && oldMount == newMount) {
        return ker::vfs::fat32::fat32_rename_path(static_cast<ker::vfs::fat32::FAT32MountContext*>(oldMount->private_data),
                                                  strip_mount_prefix(oldMount, oldBuf), strip_mount_prefix(newMount, newBuf));
    }

    if (oldMount->fs_type == FSType::REMOTE && newMount->fs_type == FSType::REMOTE && oldMount == newMount) {
        return ker::net::wki::wki_remote_vfs_rename(oldMount->private_data, strip_mount_prefix(oldMount, oldBuf),
                                                    strip_mount_prefix(newMount, newBuf));
    }

    if (oldMount->fs_type != FSType::TMPFS || newMount->fs_type != FSType::TMPFS) return -ENOSYS;

    // Helper lambda to strip mount prefix
    auto strip_mount = [](const char* buf, MountPoint* m) -> const char* {
        size_t ml = std::strlen(m->path);
        if (ml == 1 && m->path[0] == '/') return buf + 1;
        if (buf[ml] == '/') return buf + ml + 1;
        return buf + ml;
    };

    const char* oldFs = strip_mount(oldBuf, oldMount);
    const char* newFs = strip_mount(newBuf, newMount);

    // Lookup old node
    auto* oldNode = ker::vfs::tmpfs::tmpfs_walk_path(oldFs, false);
    if (oldNode == nullptr) return -ENOENT;

    // Find old parent
    auto* oldParent = oldNode->parent;
    if (oldParent == nullptr) return -EINVAL;  // Can't rename root

    // Walk to new parent, extract new name
    const char* newLastSlash = nullptr;
    for (const char* p = newFs; *p; ++p) {
        if (*p == '/') newLastSlash = p;
    }

    ker::vfs::tmpfs::TmpNode* newParent = nullptr;
    const char* newName = nullptr;

    if (newLastSlash == nullptr) {
        newParent = ker::vfs::tmpfs::get_root_node();
        newName = newFs;
    } else {
        char parentPath[MAX_PATH_LEN];
        auto plen = static_cast<size_t>(newLastSlash - newFs);
        if (plen >= MAX_PATH_LEN) return -ENAMETOOLONG;
        std::memcpy(parentPath, newFs, plen);
        parentPath[plen] = '\0';
        newParent = ker::vfs::tmpfs::tmpfs_walk_path(parentPath, false);
        newName = newLastSlash + 1;
    }

    if (newParent == nullptr || newName == nullptr || *newName == '\0') return -ENOENT;

    // If destination exists, remove it
    auto* existing = ker::vfs::tmpfs::tmpfs_lookup(newParent, newName);
    if (existing != nullptr) {
        // Remove existing from parent
        for (size_t i = 0; i < newParent->children_count; ++i) {
            if (newParent->children[i] == existing) {
                for (size_t j = i; j + 1 < newParent->children_count; ++j) {
                    newParent->children[j] = newParent->children[j + 1];
                }
                newParent->children_count--;
                newParent->children[newParent->children_count] = nullptr;
                delete[] existing->data;
                delete[] existing->symlink_target;
                delete[] existing->children;
                delete existing;
                break;
            }
        }
    }

    // Remove old node from old parent
    for (size_t i = 0; i < oldParent->children_count; ++i) {
        if (oldParent->children[i] == oldNode) {
            for (size_t j = i; j + 1 < oldParent->children_count; ++j) {
                oldParent->children[j] = oldParent->children[j + 1];
            }
            oldParent->children_count--;
            oldParent->children[oldParent->children_count] = nullptr;
            break;
        }
    }

    // Rename and reparent
    size_t nn_len = std::strlen(newName);
    size_t copy_len = nn_len < ker::vfs::tmpfs::TMPFS_NAME_MAX - 1 ? nn_len : ker::vfs::tmpfs::TMPFS_NAME_MAX - 1;
    std::memcpy(oldNode->name.data(), newName, copy_len);
    oldNode->name[copy_len] = '\0';

    // Add to new parent (inline - avoid circular include of tmpfs internal helpers)
    // Grow children array if needed
    if (newParent->children_count >= newParent->children_capacity) {
        size_t new_cap = (newParent->children_capacity == 0) ? 8 : newParent->children_capacity * 2;
        auto** new_arr = new ker::vfs::tmpfs::TmpNode*[new_cap];
        for (size_t i = 0; i < newParent->children_count; ++i) new_arr[i] = newParent->children[i];
        for (size_t i = newParent->children_count; i < new_cap; ++i) new_arr[i] = nullptr;
        delete[] newParent->children;
        newParent->children = new_arr;
        newParent->children_capacity = new_cap;
    }
    newParent->children[newParent->children_count++] = oldNode;
    oldNode->parent = newParent;

    return 0;
}

// --- chmod (stub) ---
auto vfs_chmod(const char* path, int mode) -> int {
    if (path == nullptr) return -EINVAL;

    char pathBuffer[MAX_PATH_LEN];
    if (resolve_task_path_raw(path, pathBuffer, MAX_PATH_LEN) < 0) return -ENAMETOOLONG;

    auto* mount = find_mount_point(pathBuffer);
    if (mount == nullptr) return -ENOENT;

    const char* fs_path = strip_mount_prefix(mount, pathBuffer);

    switch (mount->fs_type) {
        case FSType::TMPFS: {
            auto* node = ker::vfs::tmpfs::tmpfs_walk_path(fs_path, false);
            if (node == nullptr) return -ENOENT;
            node->mode = static_cast<uint32_t>(mode) & 07777;
            return 0;
        }
        case FSType::DEVFS: {
            auto* node = ker::vfs::devfs::devfs_walk_path(fs_path);
            if (node == nullptr) return -ENOENT;
            node->mode = static_cast<uint32_t>(mode) & 07777;
            return 0;
        }
        case FSType::FAT32:
        case FSType::XFS:
            // FAT32/XFS have no on-disk permission model or are read-only; accept silently
            return 0;
        default:
            return -ENOSYS;
    }
}

auto vfs_fchmod(int fd, int mode) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;
    auto* f = vfs_get_file(task, fd);
    if (f == nullptr) return -EBADF;

    switch (f->fs_type) {
        case FSType::TMPFS: {
            auto* node = static_cast<ker::vfs::tmpfs::TmpNode*>(f->private_data);
            if (node == nullptr) return -EBADF;
            node->mode = static_cast<uint32_t>(mode) & 07777;
            return 0;
        }
        case FSType::DEVFS:
        case FSType::FAT32:
        case FSType::XFS:
            return 0;  // Accept silently
        default:
            return -ENOSYS;
    }
}

auto vfs_chown(const char* path, uint32_t owner, uint32_t group) -> int {
    if (path == nullptr) return -EINVAL;

    char pathBuffer[MAX_PATH_LEN];
    if (resolve_task_path_raw(path, pathBuffer, MAX_PATH_LEN) < 0) return -ENAMETOOLONG;

    auto* mount = find_mount_point(pathBuffer);
    if (mount == nullptr) return -ENOENT;

    const char* fs_path = strip_mount_prefix(mount, pathBuffer);

    switch (mount->fs_type) {
        case FSType::TMPFS: {
            auto* node = ker::vfs::tmpfs::tmpfs_walk_path(fs_path, false);
            if (node == nullptr) return -ENOENT;
            if (owner != static_cast<uint32_t>(-1)) node->uid = owner;
            if (group != static_cast<uint32_t>(-1)) node->gid = group;
            return 0;
        }
        case FSType::DEVFS: {
            auto* node = ker::vfs::devfs::devfs_walk_path(fs_path);
            if (node == nullptr) return -ENOENT;
            if (owner != static_cast<uint32_t>(-1)) node->uid = owner;
            if (group != static_cast<uint32_t>(-1)) node->gid = group;
            return 0;
        }
        case FSType::FAT32:
        case FSType::XFS:
            return 0;  // Accept silently
        default:
            return -ENOSYS;
    }
}

auto vfs_fchown(int fd, uint32_t owner, uint32_t group) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;
    auto* f = vfs_get_file(task, fd);
    if (f == nullptr) return -EBADF;

    switch (f->fs_type) {
        case FSType::TMPFS: {
            auto* node = static_cast<ker::vfs::tmpfs::TmpNode*>(f->private_data);
            if (node == nullptr) return -EBADF;
            if (owner != static_cast<uint32_t>(-1)) node->uid = owner;
            if (group != static_cast<uint32_t>(-1)) node->gid = group;
            return 0;
        }
        case FSType::DEVFS:
        case FSType::FAT32:
        case FSType::XFS:
            return 0;  // Accept silently
        default:
            return -ENOSYS;
    }
}

// --- ftruncate ---
auto vfs_ftruncate(int fd, off_t length) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;
    auto* f = vfs_get_file(task, fd);
    if (f == nullptr) return -EBADF;
    if (f->fops == nullptr || f->fops->vfs_truncate == nullptr) return -ENOSYS;
    return f->fops->vfs_truncate(f, length);
}

// --- fcntl ---
auto vfs_fcntl(int fd, int cmd, uint64_t arg) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;
    auto* f = vfs_get_file(task, fd);
    if (f == nullptr) return -EBADF;

    // F_DUPFD=0, F_GETFD=1, F_SETFD=2, F_GETFL=3, F_SETFL=4 (Linux values)
    switch (cmd) {
        case 0: {  // F_DUPFD - dup to fd >= arg
            __atomic_add_fetch(&f->refcount, 1, __ATOMIC_ACQ_REL);
            uint64_t slot = task->fd_table.find_first_unset(static_cast<uint64_t>(arg));
            if (slot == UINT64_MAX || !task->fd_table.insert(slot, f)) {
                __atomic_sub_fetch(&f->refcount, 1, __ATOMIC_ACQ_REL);
                return -EMFILE;
            }
            task->clear_fd_cloexec(static_cast<unsigned>(slot));
            return static_cast<int>(slot);
        }
        case 1:  // F_GETFD
            return task->get_fd_cloexec(static_cast<unsigned>(fd)) ? 1 : 0;
        case 2:  // F_SETFD
            if (arg & 1) {
                task->set_fd_cloexec(static_cast<unsigned>(fd));
            } else {
                task->clear_fd_cloexec(static_cast<unsigned>(fd));
            }
            return 0;
        case 3:  // F_GETFL
            return f->open_flags;
        case 4:  // F_SETFL
            f->open_flags = static_cast<int>(arg);
            return 0;
        case 1030: {  // F_DUPFD_CLOEXEC - dup to fd >= arg, set close-on-exec
            __atomic_add_fetch(&f->refcount, 1, __ATOMIC_ACQ_REL);
            uint64_t slot = task->fd_table.find_first_unset(static_cast<uint64_t>(arg));
            if (slot == UINT64_MAX || !task->fd_table.insert(slot, f)) {
                __atomic_sub_fetch(&f->refcount, 1, __ATOMIC_ACQ_REL);
                return -EMFILE;
            }
            task->set_fd_cloexec(static_cast<unsigned>(slot));
            return static_cast<int>(slot);
        }
        default:
            return -EINVAL;
    }
}

// --- pipe ---

// PipeState is shared between both ends. It includes wait queues for blocking.
static constexpr ssize_t PIPE_WOS_ERESTARTSYS = 512;

struct PipeState {
    char* buf;
    size_t capacity;
    size_t head;   // write position
    size_t tail;   // read position
    size_t count;  // bytes in buffer
    bool write_closed;
    bool read_closed;
    ker::mod::sys::Spinlock lock;

    // Wait queues for blocking pipe I/O
    ker::util::SmallVec<uint64_t, 2> readers_waiting;
    ker::util::SmallVec<uint64_t, 2> writers_waiting;

    ker::util::SmallVec<uint64_t, 2> read_poll_waiting;
    ker::util::SmallVec<uint64_t, 2> write_poll_waiting;
};

static auto pipe_register_waiter(ker::util::SmallVec<uint64_t, 2>& waiters, uint64_t pid) -> bool {
    for (size_t i = 0; i < waiters.size(); i++) {
        if (waiters[i] == pid) {
            return true;
        }
    }
    return waiters.push_back(pid);
}

static auto pipe_register_poll_waiter(ker::util::SmallVec<uint64_t, 2>& waiters, uint64_t pid) -> bool {
    return pipe_register_waiter(waiters, pid);
}

static void pipe_collect_waiters_locked(ker::util::SmallVec<uint64_t, 2>& waiters, uint64_t* pending, size_t* pending_count) {
    *pending_count = std::min(waiters.size(), size_t{32});
    for (size_t i = 0; i < *pending_count; i++) {
        pending[i] = waiters[i];
    }
    waiters.clear();
}

static void pipe_reschedule_waiters(const uint64_t* waiters, size_t waiter_count, bool sigpipe = false) {
    for (size_t i = 0; i < waiter_count; i++) {
        auto* waiter = ker::mod::sched::find_task_by_pid_safe(waiters[i]);
        if (waiter == nullptr) {
            continue;
        }

        if (sigpipe) {
            waiter->sigPending |= (1ULL << (13 - 1));
        }

        waiter->deferredTaskSwitch = false;
        uint64_t target_cpu = waiter->cpu;
        if (waiter->schedQueue == ker::mod::sched::task::Task::SchedQueue::WAITING || waiter->voluntaryBlock) {
            target_cpu = ker::mod::sched::get_least_loaded_cpu();
        }
        ker::mod::sched::reschedule_task_for_cpu(target_cpu, waiter);
        waiter->release();
    }
}

auto vfs_pipe(int pipefd[2]) -> int {
    if (pipefd == nullptr) return -EINVAL;
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;

    // Keep a moderate default capacity so simple producer/consumer pipelines do
    // not bounce through the scheduler every 4 KiB.
    constexpr size_t PIPE_BUF_SIZE = 64UL * 1024UL;
    auto* pipe_buf = new char[PIPE_BUF_SIZE];

    auto* ps = new PipeState{};
    ps->buf = pipe_buf;
    ps->capacity = PIPE_BUF_SIZE;
    ps->head = 0;
    ps->tail = 0;
    ps->count = 0;
    ps->write_closed = false;
    ps->read_closed = false;

    // Pipe fops - static lambdas converted to function pointers
    static auto pipe_read = [](File* f, void* buf, size_t count, size_t /*offset*/) -> ssize_t {
        auto* st = static_cast<PipeState*>(f->private_data);
        if (st == nullptr) return -EBADF;

        uint64_t pending_writers[32]{};
        size_t pending_writers_count = 0;
        uint64_t pending_write_pollers[32]{};
        size_t pending_write_pollers_count = 0;
        ssize_t result = 0;

        uint64_t irqf = st->lock.lock_irqsave();

        if (st->count > 0) {
            size_t to_read = count < st->count ? count : st->count;
            auto* dst = static_cast<char*>(buf);
            size_t first = st->capacity - st->tail;
            if (first >= to_read) {
                std::memcpy(dst, st->buf + st->tail, to_read);
            } else {
                std::memcpy(dst, st->buf + st->tail, first);
                std::memcpy(dst + first, st->buf, to_read - first);
            }
            st->tail = (st->tail + to_read) % st->capacity;
            st->count -= to_read;

            if (st->writers_waiting.size() > 0) {
                pipe_collect_waiters_locked(st->writers_waiting, pending_writers, &pending_writers_count);
            }
            if (st->write_poll_waiting.size() > 0) {
                pipe_collect_waiters_locked(st->write_poll_waiting, pending_write_pollers, &pending_write_pollers_count);
            }

            result = static_cast<ssize_t>(to_read);
            st->lock.unlock_irqrestore(irqf);
            pipe_reschedule_waiters(pending_writers, pending_writers_count);
            pipe_reschedule_waiters(pending_write_pollers, pending_write_pollers_count);
            return result;
        }

        if (st->write_closed) {
            st->lock.unlock_irqrestore(irqf);
            return 0;
        }

        if (f->open_flags & O_NONBLOCK) {
            st->lock.unlock_irqrestore(irqf);
            return -EAGAIN;
        }

        auto* currentTask = ker::mod::sched::get_current_task();
        if (currentTask == nullptr) {
            st->lock.unlock_irqrestore(irqf);
            return -ESRCH;
        }

        if (pipe_register_waiter(st->readers_waiting, currentTask->pid)) {
            currentTask->wait_channel = "pipe_read";
            currentTask->deferredTaskSwitch = true;
            st->lock.unlock_irqrestore(irqf);
            return -PIPE_WOS_ERESTARTSYS;
        }

        st->lock.unlock_irqrestore(irqf);
        return -EAGAIN;
    };

    static auto pipe_write = [](File* f, const void* buf, size_t count, size_t /*offset*/) -> ssize_t {
        auto* st = static_cast<PipeState*>(f->private_data);
        if (st == nullptr) return -EBADF;

        uint64_t pending_readers[32]{};
        size_t pending_readers_count = 0;
        uint64_t pending_read_pollers[32]{};
        size_t pending_read_pollers_count = 0;

        uint64_t irqf = st->lock.lock_irqsave();
        if (st->read_closed) {
            st->lock.unlock_irqrestore(irqf);
            // Send SIGPIPE to the writing process (signal 13)
            auto* task = ker::mod::sched::get_current_task();
            if (task) task->sigPending |= (1ULL << (13 - 1));
            return -EPIPE;
        }

        size_t avail = st->capacity - st->count;
        if (avail > 0) {
            size_t to_write = count < avail ? count : avail;
            auto* src = static_cast<const char*>(buf);
            // Bulk copy into ring buffer (at most 2 memcpy segments for wraparound)
            size_t first = st->capacity - st->head;
            if (first >= to_write) {
                std::memcpy(st->buf + st->head, src, to_write);
            } else {
                std::memcpy(st->buf + st->head, src, first);
                std::memcpy(st->buf, src + first, to_write - first);
            }
            st->head = (st->head + to_write) % st->capacity;
            st->count += to_write;

            if (st->readers_waiting.size() > 0) {
                pipe_collect_waiters_locked(st->readers_waiting, pending_readers, &pending_readers_count);
            }
            if (st->read_poll_waiting.size() > 0) {
                pipe_collect_waiters_locked(st->read_poll_waiting, pending_read_pollers, &pending_read_pollers_count);
            }

            st->lock.unlock_irqrestore(irqf);
            pipe_reschedule_waiters(pending_readers, pending_readers_count);
            pipe_reschedule_waiters(pending_read_pollers, pending_read_pollers_count);
            return static_cast<ssize_t>(to_write);
        }

        if (f->open_flags & O_NONBLOCK) {
            st->lock.unlock_irqrestore(irqf);
            return -EAGAIN;
        }

        auto* currentTask = ker::mod::sched::get_current_task();
        if (currentTask == nullptr) {
            st->lock.unlock_irqrestore(irqf);
            return -ESRCH;
        }

        if (pipe_register_waiter(st->writers_waiting, currentTask->pid)) {
            currentTask->wait_channel = "pipe_write";
            currentTask->deferredTaskSwitch = true;
            st->lock.unlock_irqrestore(irqf);
            return -PIPE_WOS_ERESTARTSYS;
        }

        st->lock.unlock_irqrestore(irqf);
        return -EAGAIN;
    };

    static auto pipe_close_read = [](File* f) -> int {
        auto* st = static_cast<PipeState*>(f->private_data);
        uint64_t pending_writers[32]{};
        size_t pending_writers_count = 0;
        uint64_t pending_write_pollers[32]{};
        size_t pending_write_pollers_count = 0;
        bool should_free = false;
        if (st != nullptr) {
            uint64_t irqf = st->lock.lock_irqsave();
            st->read_closed = true;
            if (st->writers_waiting.size() > 0) {
                pipe_collect_waiters_locked(st->writers_waiting, pending_writers, &pending_writers_count);
            }
            if (st->write_poll_waiting.size() > 0) {
                pipe_collect_waiters_locked(st->write_poll_waiting, pending_write_pollers, &pending_write_pollers_count);
            }
            should_free = st->write_closed;
            st->lock.unlock_irqrestore(irqf);
        }
        pipe_reschedule_waiters(pending_writers, pending_writers_count, true);
        pipe_reschedule_waiters(pending_write_pollers, pending_write_pollers_count);
        if (st != nullptr && should_free) {
            delete[] st->buf;
            delete st;
        }
        return 0;
    };

    static auto pipe_close_write = [](File* f) -> int {
        auto* st = static_cast<PipeState*>(f->private_data);
        uint64_t pending_readers[32]{};
        size_t pending_readers_count = 0;
        uint64_t pending_read_pollers[32]{};
        size_t pending_read_pollers_count = 0;
        bool should_free = false;
        if (st != nullptr) {
            uint64_t irqf = st->lock.lock_irqsave();
            st->write_closed = true;
            if (st->readers_waiting.size() > 0) {
                pipe_collect_waiters_locked(st->readers_waiting, pending_readers, &pending_readers_count);
            }
            if (st->read_poll_waiting.size() > 0) {
                pipe_collect_waiters_locked(st->read_poll_waiting, pending_read_pollers, &pending_read_pollers_count);
            }
            should_free = st->read_closed;
            st->lock.unlock_irqrestore(irqf);
        }
        pipe_reschedule_waiters(pending_readers, pending_readers_count);
        pipe_reschedule_waiters(pending_read_pollers, pending_read_pollers_count);
        if (st != nullptr && should_free) {
            delete[] st->buf;
            delete st;
        }
        return 0;
    };

    static FileOperations pipe_read_fops = {
        .vfs_open = nullptr,
        .vfs_close = pipe_close_read,
        .vfs_read = pipe_read,
        .vfs_write = nullptr,
        .vfs_lseek = nullptr,
        .vfs_isatty = nullptr,
        .vfs_readdir = nullptr,
        .vfs_readlink = nullptr,
        .vfs_truncate = nullptr,
        .vfs_poll_check = [](File* f, int events) -> int {
            auto* st = static_cast<PipeState*>(f->private_data);
            if (st == nullptr) return 0;
            int ready = 0;
            uint64_t irqf = st->lock.lock_irqsave();
            if ((events & 0x0001) && (st->count > 0 || st->write_closed))  // POLLIN
                ready |= 0x0001;
            if (st->write_closed && st->count == 0)  // POLLHUP
                ready |= 0x0010;
            st->lock.unlock_irqrestore(irqf);
            return ready;
        },
        .vfs_poll_register_waiter = [](File* f, uint64_t pid) -> bool {
            auto* st = static_cast<PipeState*>(f->private_data);
            if (st == nullptr) return false;
            uint64_t irqf = st->lock.lock_irqsave();
            bool ok = pipe_register_poll_waiter(st->read_poll_waiting, pid);
            st->lock.unlock_irqrestore(irqf);
            return ok;
        },
    };

    static FileOperations pipe_write_fops = {
        .vfs_open = nullptr,
        .vfs_close = pipe_close_write,
        .vfs_read = nullptr,
        .vfs_write = pipe_write,
        .vfs_lseek = nullptr,
        .vfs_isatty = nullptr,
        .vfs_readdir = nullptr,
        .vfs_readlink = nullptr,
        .vfs_truncate = nullptr,
        .vfs_poll_check = [](File* f, int events) -> int {
            auto* st = static_cast<PipeState*>(f->private_data);
            if (st == nullptr) return 0;
            int ready = 0;
            uint64_t irqf = st->lock.lock_irqsave();
            if ((events & 0x0004) && (st->count < st->capacity || st->read_closed))  // POLLOUT
                ready |= 0x0004;
            if (st->read_closed)  // POLLERR (broken pipe)
                ready |= 0x0008;
            st->lock.unlock_irqrestore(irqf);
            return ready;
        },
        .vfs_poll_register_waiter = [](File* f, uint64_t pid) -> bool {
            auto* st = static_cast<PipeState*>(f->private_data);
            if (st == nullptr) return false;
            uint64_t irqf = st->lock.lock_irqsave();
            bool ok = pipe_register_poll_waiter(st->write_poll_waiting, pid);
            st->lock.unlock_irqrestore(irqf);
            return ok;
        },
    };

    // Create read-end File
    auto* rf = new File;
    rf->private_data = ps;
    rf->fops = &pipe_read_fops;
    rf->pos = 0;
    rf->is_directory = false;
    rf->fs_type = FSType::TMPFS;  // pseudo-type
    rf->refcount = 1;
    rf->open_flags = 0;  // O_RDONLY
    rf->fd_flags = 0;
    rf->vfs_path = nullptr;
    rf->dir_fs_count = 0;

    // Create write-end File
    auto* wf = new File;
    wf->private_data = ps;
    wf->fops = &pipe_write_fops;
    wf->pos = 0;
    wf->is_directory = false;
    wf->fs_type = FSType::TMPFS;
    wf->refcount = 1;
    wf->open_flags = 1;  // O_WRONLY
    wf->fd_flags = 0;
    wf->vfs_path = nullptr;
    wf->dir_fs_count = 0;

    int rfd = vfs_alloc_fd(task, rf);
    if (rfd < 0) {
        delete rf;
        delete wf;
        delete[] pipe_buf;
        delete ps;
        return -EMFILE;
    }
    int wfd = vfs_alloc_fd(task, wf);
    if (wfd < 0) {
        vfs_release_fd(task, rfd);
        delete rf;
        delete wf;
        delete[] pipe_buf;
        delete ps;
        return -EMFILE;
    }

    pipefd[0] = rfd;
    pipefd[1] = wfd;
    return 0;
}

auto vfs_mount(const char* source, const char* target, const char* fstype) -> int {
    if (target == nullptr) {
        return -EINVAL;
    }

    // Default fstype to "fat32" when not specified (auto-detect for block devices)
    const char* effective_fstype = fstype;
    if (effective_fstype == nullptr || effective_fstype[0] == '\0') {
        effective_fstype = "fat32";
    }

    ker::dev::BlockDevice* bdev = nullptr;

    if (source != nullptr) {
        // V2: Handle wki://<hostname>/<export> URIs
        if (source[0] == 'w' && source[1] == 'k' && source[2] == 'i' && source[3] == ':' && source[4] == '/' && source[5] == '/') {
            const char* host_start = source + 6;
            const char* slash = host_start;
            while (*slash != '\0' && *slash != '/') {
                slash++;
            }
            size_t host_len = static_cast<size_t>(slash - host_start);
            if (host_len == 0 || host_len >= 64) {
                return -EINVAL;
            }

            char hostname[64] = {};  // NOLINT(modernize-avoid-c-arrays)
            memcpy(hostname, host_start, host_len);
            hostname[host_len] = '\0';

            const char* export_name = (*slash == '/') ? slash + 1 : "";
            if (export_name[0] == '\0') {
                return -EINVAL;
            }

            // Resolve hostname to node_id
            uint16_t node_id = ker::net::wki::wki_peer_find_by_hostname(hostname);
            if (node_id == 0) {
                return -ENODEV;
            }
            auto* peer = ker::net::wki::wki_peer_find(node_id);
            if (peer == nullptr || peer->state != ker::net::wki::PeerState::CONNECTED) {
                return -EHOSTUNREACH;
            }

            // Find matching VFS resource from discovered table
            struct VfsFindCtx {
                uint16_t node_id;
                const char* export_name;
                ker::net::wki::DiscoveredResource* result;
            };
            VfsFindCtx find_ctx = {node_id, export_name, nullptr};
            ker::net::wki::wki_resource_foreach(
                [](const ker::net::wki::DiscoveredResource& r, void* ctx_ptr) {
                    auto* fc = static_cast<VfsFindCtx*>(ctx_ptr);
                    if (fc->result != nullptr) return;
                    if (r.node_id == fc->node_id && r.resource_type == ker::net::wki::ResourceType::VFS &&
                        strncmp(static_cast<const char*>(r.name), fc->export_name, ker::net::wki::DISCOVERED_RESOURCE_NAME_LEN) == 0) {
                        fc->result = const_cast<ker::net::wki::DiscoveredResource*>(&r);
                    }
                },
                &find_ctx);

            if (find_ctx.result == nullptr) {
                return -ENXIO;
            }

            // Create mount target directory
            vfs_mkdir(target, 0755);

            return ker::net::wki::wki_remote_vfs_mount(node_id, find_ctx.result->resource_id, target);
        }

        // Check for PARTUUID= prefix
        constexpr size_t PARTUUID_PREFIX_LEN = 9;  // "PARTUUID="
        bool is_partuuid = (source[0] == 'P' && source[1] == 'A' && source[2] == 'R' && source[3] == 'T' && source[4] == 'U' &&
                            source[5] == 'U' && source[6] == 'I' && source[7] == 'D' && source[8] == '=');

        if (is_partuuid) {
            bdev = ker::dev::block_device_find_by_partuuid(source + PARTUUID_PREFIX_LEN);
            if (bdev == nullptr) {
                ker::mod::io::serial::write("vfs_mount: PARTUUID not found: ");
                ker::mod::io::serial::write(source + PARTUUID_PREFIX_LEN);
                ker::mod::io::serial::write("\n");
                return -ENOENT;
            }
        } else if (source[0] == '/' && source[1] == 'd' && source[2] == 'e' && source[3] == 'v' && source[4] == '/') {
            // /dev/XXX - lookup by device name
            bdev = ker::dev::block_device_find_by_name(source + 5);
            if (bdev == nullptr) {
                // Walk devfs tree - handles subdirectory paths like wki/block/<name>
                // and triggers WKI proxy attach for remote block devices
                bdev = ker::vfs::devfs::devfs_resolve_block_device(source + 5);
            }
            if (bdev == nullptr) {
                ker::mod::io::serial::write("vfs_mount: device not found: ");
                ker::mod::io::serial::write(source);
                ker::mod::io::serial::write("\n");
                return -ENOENT;
            }
        }
    }

    // Auto-detect filesystem type when a block device is present and the
    // caller did NOT supply an explicit fstype (i.e. fstype was NULL or
    // empty, which we defaulted to "fat32" above).  Probe the first sector
    // for known superblock magic so the correct driver is selected.
    if (bdev != nullptr && (fstype == nullptr || fstype[0] == '\0')) {
        ker::vfs::buffer_cache_init();
        auto* probe_buf = ker::vfs::bread(bdev, 0);
        if (probe_buf != nullptr) {
            // XFS superblock magic at offset 0: 0x58465342 ('XFSB') big-endian
            if (probe_buf->size >= 4) {
                uint32_t magic = (static_cast<uint32_t>(probe_buf->data[0]) << 24) | (static_cast<uint32_t>(probe_buf->data[1]) << 16) |
                                 (static_cast<uint32_t>(probe_buf->data[2]) << 8) | (static_cast<uint32_t>(probe_buf->data[3]));
                if (magic == 0x58465342) {  // XFS_SB_MAGIC
                    effective_fstype = "xfs";
                    ker::mod::io::serial::write("vfs_mount: auto-detected XFS filesystem\n");
                }
            }
            ker::vfs::brelse(probe_buf);
        }
    }

    // Create mount point directory in tmpfs if needed
    vfs_mkdir(target, 0755);

    return mount_filesystem(target, effective_fstype, bdev);
}

void init() {
    vfs_debug_log("vfs: init\n");
    // Register tmpfs as a minimal root filesystem
    ker::vfs::tmpfs::register_tmpfs();
    // Mount tmpfs at root
    mount_filesystem("/", "tmpfs", nullptr);

    // Register FAT32 driver (will be mounted when a disk is available)
    ker::vfs::fat32::register_fat32();

    // Register XFS driver
    ker::vfs::xfs::register_xfs();

    // Register and mount devfs for device files
    ker::vfs::devfs::devfs_init();
    mount_filesystem("/dev", "devfs", nullptr);

    // Register and mount procfs for process information
    ker::vfs::procfs::procfs_init();

    install_builtin_vfs_rules();
}

void vfs_wki_load_default_rules() {
    install_builtin_vfs_rules();

    ker::vfs::stat st{};
    if (vfs_stat("/etc/vfstab", &st) < 0 || st.st_size <= 0) {
        return;
    }

    size_t bytes_to_read = std::min<size_t>(static_cast<size_t>(st.st_size), MAX_VFSTAB_BYTES);
    auto* file = vfs_open_file("/etc/vfstab", 0, 0);
    if (file == nullptr || file->fops == nullptr || file->fops->vfs_read == nullptr) {
        release_open_file(file);
        return;
    }

    auto* buffer = new char[bytes_to_read + 1];
    if (buffer == nullptr) {
        release_open_file(file);
        return;
    }

    ssize_t bytes_read = file->fops->vfs_read(file, buffer, bytes_to_read, 0);
    release_open_file(file);
    if (bytes_read <= 0) {
        delete[] buffer;
        return;
    }

    buffer[static_cast<size_t>(bytes_read)] = '\0';
    load_vfs_rules_from_buffer(buffer);
    delete[] buffer;
}

auto vfs_wki_rule_add(const char* prefix, uint32_t route) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || prefix == nullptr) {
        return -EINVAL;
    }
    if (route != static_cast<uint32_t>(ker::mod::sched::task::WkiVfsRoute::LOCAL) &&
        route != static_cast<uint32_t>(ker::mod::sched::task::WkiVfsRoute::HOST)) {
        return -EINVAL;
    }

    char canonical[MAX_PATH_LEN] = {};
    int absolute = make_absolute(prefix, canonical, sizeof(canonical));
    if (absolute < 0) {
        return absolute;
    }

    int canonical_result = canonicalize_path(canonical, sizeof(canonical));
    if (canonical_result < 0) {
        return canonical_result;
    }

    size_t prefix_len = std::strlen(canonical);
    if (prefix_len == 0 || prefix_len >= ker::mod::sched::task::WkiVfsRule::PREFIX_MAX) {
        return -ENAMETOOLONG;
    }

    for (size_t i = 0; i < task->wki_vfs_rules.size(); ++i) {
        auto& rule = task->wki_vfs_rules[i];
        if (rule.prefix_len == prefix_len && std::strncmp(rule.prefix, canonical, prefix_len) == 0) {
            std::memcpy(rule.prefix, canonical, prefix_len + 1);
            rule.prefix_len = static_cast<uint16_t>(prefix_len);
            rule.route = static_cast<uint8_t>(route);
            rule.reserved = 0;
            return 0;
        }
    }

    mod::sched::task::WkiVfsRule new_rule{};
    std::memcpy(new_rule.prefix, canonical, prefix_len + 1);
    new_rule.prefix_len = static_cast<uint16_t>(prefix_len);
    new_rule.route = static_cast<uint8_t>(route);
    new_rule.reserved = 0;
    if (!task->wki_vfs_rules.push_back(new_rule)) {
        return -ENOMEM;
    }
    return 0;
}

auto vfs_wki_rule_get(uint32_t index, char* prefix_buf, size_t prefix_buf_size, uint32_t* route_out) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -EINVAL;
    }

    if (index >= task->wki_vfs_rules.size()) {
        return -ENOENT;
    }

    const auto& rule = task->wki_vfs_rules[index];
    if (prefix_buf != nullptr) {
        if (rule.prefix_len + 1 > prefix_buf_size) {
            return -ERANGE;
        }
        std::memcpy(prefix_buf, rule.prefix, rule.prefix_len + 1);
    }
    if (route_out != nullptr) {
        *route_out = rule.route;
    }
    return static_cast<int>(rule.prefix_len);
}

auto vfs_wki_rule_clear() -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return -EINVAL;
    }

    task->wki_vfs_rules.clear();
    return 0;
}

auto vfs_open_file(const char* path, int flags, int mode) -> File* {
    if (path == nullptr) {
        return nullptr;
    }

    // Resolve relative path and canonicalize
    char pathBuffer[MAX_PATH_LEN];  // NOLINT
    if (make_absolute(path, pathBuffer, MAX_PATH_LEN) < 0) {
        return nullptr;
    }

    canonicalize_path(static_cast<char*>(pathBuffer), MAX_PATH_LEN);

    // Resolve symlinks
    char resolved[MAX_PATH_LEN];  // NOLINT
    int resolve_ret = resolve_symlinks(pathBuffer, resolved, MAX_PATH_LEN);
    if (resolve_ret == 0) {
        std::memcpy(pathBuffer, resolved, MAX_PATH_LEN);
    }

    // Find mount point
    MountPoint* mount = find_mount_point(pathBuffer);
    if (mount == nullptr) {
        return nullptr;
    }

    // Strip mount prefix
    const char* fs_relative_path = pathBuffer;
    size_t mount_len = 0;
    while (mount->path[mount_len] != '\0') {
        mount_len++;
    }

    if (mount_len > 0 && pathBuffer[mount_len - 1] == '/' && mount_len == 1) {
        fs_relative_path = pathBuffer + 1;
    } else if (pathBuffer[mount_len] == '/') {
        fs_relative_path = pathBuffer + mount_len + 1;
    } else if (pathBuffer[mount_len] == '\0') {
        fs_relative_path = "";
    } else {
        fs_relative_path = pathBuffer + mount_len;
    }

    File* f = nullptr;

    switch (mount->fs_type) {
        case FSType::DEVFS:
            f = ker::vfs::devfs::devfs_open_path(fs_relative_path, flags, mode);
            if (f != nullptr) {
                f->fops = ker::vfs::devfs::get_devfs_fops();
                f->fs_type = FSType::DEVFS;
            }
            break;
        case FSType::FAT32:
            f = ker::vfs::fat32::fat32_open_path(fs_relative_path, flags, mode,
                                                 static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data));
            if (f != nullptr) {
                f->fops = ker::vfs::fat32::get_fat32_fops();
                f->fs_type = FSType::FAT32;
            }
            break;
        case FSType::TMPFS:
            f = ker::vfs::tmpfs::tmpfs_open_path(fs_relative_path, flags, mode);
            if (f != nullptr) {
                f->fops = ker::vfs::tmpfs::get_tmpfs_fops();
                f->fs_type = FSType::TMPFS;
            }
            break;
        case FSType::XFS:
            f = ker::vfs::xfs::xfs_open_path(fs_relative_path, flags, mode,
                                             static_cast<ker::vfs::xfs::XfsMountContext*>(mount->private_data));
            if (f != nullptr) {
                f->fops = ker::vfs::xfs::get_xfs_fops();
                f->fs_type = FSType::XFS;
            }
            break;
        case FSType::PROCFS:
            f = ker::vfs::procfs::procfs_open_path(fs_relative_path, flags, mode);
            if (f != nullptr) {
                f->fops = ker::vfs::procfs::get_procfs_fops();
                f->fs_type = FSType::PROCFS;
            }
            break;
        default:
            return nullptr;
    }

    // Store the absolute VFS path for mount-overlay directory listing
    if (f != nullptr) {
        size_t pl = std::strlen(pathBuffer);
        auto* pc = static_cast<char*>(ker::mod::mm::dyn::kmalloc::malloc(pl + 1));
        if (pc != nullptr) {
            std::memcpy(pc, pathBuffer, pl + 1);
            f->vfs_path = pc;
        } else {
            f->vfs_path = nullptr;
        }
        f->dir_fs_count = static_cast<size_t>(-1);
    }

    return f;
}

auto vfs_sendfile(int outfd, int infd, off_t* offset, size_t count) -> ssize_t {
    // Get the current task
    auto* task = mod::sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }

    // Get input file
    auto* infile = vfs_get_file(task, infd);
    if (infile == nullptr) {
        return -EBADF;
    }

    // Get output file
    auto* outfile = vfs_get_file(task, outfd);
    if (outfile == nullptr) {
        return -EBADF;
    }

    // Keep the staging buffer modest so blocking outputs do not cause sendfile()
    // to pre-read large chunks that cannot be written in the same call.
    constexpr size_t BUF_SIZE = 64UL * 1024UL;
    auto* buffer = static_cast<char*>(ker::mod::mm::dyn::kmalloc::malloc(BUF_SIZE));
    if (buffer == nullptr) {
        return -ENOMEM;
    }

    ssize_t total_sent = 0;
    off_t source_offset = offset != nullptr ? *offset : infile->pos;
    size_t remaining = count;

    while (remaining > 0) {
        size_t to_read = remaining > BUF_SIZE ? BUF_SIZE : remaining;
        ssize_t read_result = vfs_pread(infd, buffer, to_read, source_offset);
        if (read_result < 0) {
            if (total_sent == 0) {
                ker::mod::mm::dyn::kmalloc::free(buffer);
                return read_result;
            }
            break;
        }

        if (read_result == 0) {
            break;
        }

        size_t chunk_size = static_cast<size_t>(read_result);
        size_t chunk_offset = 0;
        while (chunk_offset < chunk_size) {
            size_t bytes_written = 0;
            ssize_t write_result = vfs_write(outfd, buffer + chunk_offset, chunk_size - chunk_offset, &bytes_written);
            if (write_result < 0) {
                auto* current = mod::sched::get_current_task();
                if (current != nullptr && current->deferredTaskSwitch) {
                    ker::mod::mm::dyn::kmalloc::free(buffer);
                    if (offset != nullptr) {
                        *offset = source_offset;
                    } else {
                        infile->pos = source_offset;
                    }
                    return total_sent > 0 ? total_sent : write_result;
                }
                if (total_sent == 0) {
                    ker::mod::mm::dyn::kmalloc::free(buffer);
                    return write_result;
                }
                chunk_offset = chunk_size;
                remaining = 0;
                break;
            }

            if (bytes_written == 0) {
                chunk_offset = chunk_size;
                remaining = 0;
                break;
            }

            chunk_offset += bytes_written;
            total_sent += static_cast<ssize_t>(bytes_written);
            source_offset += static_cast<off_t>(bytes_written);
            remaining -= bytes_written;
        }
    }

    if (offset != nullptr) {
        *offset = source_offset;
    } else {
        infile->pos = source_offset;
    }

    ker::mod::mm::dyn::kmalloc::free(buffer);
    return total_sent;
}

auto vfs_fsync(int fd) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;
    auto* file = vfs_get_file(task, fd);
    if (file == nullptr) return -EBADF;

    switch (file->fs_type) {
        case FSType::FAT32:
            return ker::vfs::fat32::fat32_fsync(file);
        case FSType::XFS:
        case FSType::TMPFS:
        case FSType::DEVFS:
        case FSType::PROCFS:
            return 0;  // No-op for in-memory or read-only filesystems
        default:
            return 0;
    }
}

auto vfs_link(const char* oldpath, const char* newpath) -> int {
    if (oldpath == nullptr || newpath == nullptr) return -EINVAL;

    char oldBuf[MAX_PATH_LEN], newBuf[MAX_PATH_LEN];
    if (resolve_task_path_raw(oldpath, oldBuf, MAX_PATH_LEN) < 0) return -ENAMETOOLONG;
    if (resolve_task_path_raw(newpath, newBuf, MAX_PATH_LEN) < 0) return -ENAMETOOLONG;

    MountPoint* oldMount = find_mount_point(oldBuf);
    MountPoint* newMount = find_mount_point(newBuf);
    if (!oldMount || !newMount) return -ENOENT;

    // Cross-filesystem link is not allowed
    if (oldMount != newMount) return -EXDEV;

    // FAT32 does not support hard links
    if (oldMount->fs_type == FSType::FAT32) return -EPERM;

    if (oldMount->fs_type != FSType::TMPFS) return -ENOSYS;

    // --- tmpfs hard link (data-copy) ---
    const char* oldFs = strip_mount_prefix(oldMount, oldBuf);
    const char* newFs = strip_mount_prefix(newMount, newBuf);

    // Look up the source node
    auto* srcNode = ker::vfs::tmpfs::tmpfs_walk_path(oldFs, false);
    if (srcNode == nullptr) return -ENOENT;

    // Cannot hard link directories
    if (srcNode->type == ker::vfs::tmpfs::TmpNodeType::DIRECTORY) return -EPERM;

    // Walk to new parent, extract new name
    const char* newLastSlash = nullptr;
    for (const char* p = newFs; *p; ++p) {
        if (*p == '/') newLastSlash = p;
    }

    ker::vfs::tmpfs::TmpNode* newParent = nullptr;
    const char* newName = nullptr;

    if (newLastSlash == nullptr) {
        newParent = ker::vfs::tmpfs::get_root_node();
        newName = newFs;
    } else {
        char parentPath[MAX_PATH_LEN];
        auto plen = static_cast<size_t>(newLastSlash - newFs);
        if (plen >= MAX_PATH_LEN) return -ENAMETOOLONG;
        std::memcpy(parentPath, newFs, plen);
        parentPath[plen] = '\0';
        newParent = ker::vfs::tmpfs::tmpfs_walk_path(parentPath, false);
        newName = newLastSlash + 1;
    }

    if (newParent == nullptr || newName == nullptr || *newName == '\0') return -ENOENT;
    if (newParent->type != ker::vfs::tmpfs::TmpNodeType::DIRECTORY) return -ENOTDIR;

    // Destination must not already exist
    if (ker::vfs::tmpfs::tmpfs_lookup(newParent, newName) != nullptr) return -EEXIST;

    // Create the new node as a copy of the source
    if (srcNode->type == ker::vfs::tmpfs::TmpNodeType::SYMLINK) {
        // Copy symlink
        ker::vfs::tmpfs::tmpfs_create_symlink(newParent, newName, srcNode->symlink_target);
    } else {
        // Regular file - copy data
        auto* dst = ker::vfs::tmpfs::tmpfs_create_file(newParent, newName, srcNode->mode);
        if (dst == nullptr) return -ENOMEM;
        if (srcNode->data != nullptr && srcNode->size > 0) {
            dst->data = new char[srcNode->size];
            std::memcpy(dst->data, srcNode->data, srcNode->size);
            dst->size = srcNode->size;
            dst->capacity = srcNode->size;
        }
        dst->uid = srcNode->uid;
        dst->gid = srcNode->gid;
    }

    return 0;
}

}  // namespace ker::vfs
