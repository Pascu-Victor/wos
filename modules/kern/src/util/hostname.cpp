#include "hostname.hpp"

#include <cerrno>
#include <cstring>
#include <vfs/file.hpp>
#include <vfs/vfs.hpp>

#include "platform/dbg/dbg.hpp"

namespace ker::util::hostname {

static char s_hostname[HOSTNAME_MAX] = "wos";

static auto is_valid_char(char c) -> bool {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-';
}

void init() {
    auto* f = ker::vfs::vfs_open_file("/etc/hostname", 0, 0);
    if (f == nullptr || f->fops == nullptr || f->fops->vfs_read == nullptr) {
        if (f != nullptr && f->fops != nullptr && f->fops->vfs_close != nullptr) {
            f->fops->vfs_close(f);
        }
        ker::mod::dbg::log("[hostname] No /etc/hostname, using default '%s'", s_hostname);
        return;
    }

    char buf[HOSTNAME_MAX + 16] = {};
    ssize_t n = f->fops->vfs_read(f, buf, sizeof(buf) - 1, 0);
    if (f->fops->vfs_close != nullptr) {
        f->fops->vfs_close(f);
    }
    if (n <= 0) {
        return;
    }
    buf[n] = '\0';

    // Strip whitespace
    char* start = buf;
    while (*start == ' ' || *start == '\t' || *start == '\n' || *start == '\r') {
        start++;
    }
    char* end = start + strlen(start);
    while (end > start && (end[-1] == ' ' || end[-1] == '\t' || end[-1] == '\n' || end[-1] == '\r')) {
        end--;
    }
    *end = '\0';

    size_t len = static_cast<size_t>(end - start);
    if (len == 0 || len >= HOSTNAME_MAX) {
        return;
    }
    if (start[0] == '-' || start[len - 1] == '-') {
        return;
    }
    for (size_t i = 0; i < len; i++) {
        if (!is_valid_char(start[i])) {
            return;
        }
    }

    memcpy(s_hostname, start, len + 1);
    ker::mod::dbg::log("[hostname] Loaded '%s' from /etc/hostname", s_hostname);
}

auto get() -> const char* { return s_hostname; }

auto set(const char* name, size_t len) -> int {
    if (name == nullptr || len == 0 || len >= HOSTNAME_MAX) {
        return -EINVAL;
    }
    if (name[0] == '-' || name[len - 1] == '-') {
        return -EINVAL;
    }
    for (size_t i = 0; i < len; i++) {
        if (!is_valid_char(name[i])) {
            return -EINVAL;
        }
    }
    memcpy(s_hostname, name, len);
    s_hostname[len] = '\0';
    return 0;
}

}  // namespace ker::util::hostname
