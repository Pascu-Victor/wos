#include "hostname.hpp"

#include <cerrno>
#include <cstring>
#include <platform/fw/qemu_fw_cfg.hpp>
#include <vfs/file.hpp>
#include <vfs/vfs.hpp>

#include "platform/dbg/dbg.hpp"

namespace ker::util::hostname {

static char s_hostname[HOSTNAME_MAX] = "wos";

static auto is_valid_char(char c) -> bool { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-'; }

static auto try_set_hostname(const char* src, size_t max_len, const char* source_label) -> bool {
    // Strip whitespace
    const char* start = src;
    while (*start == ' ' || *start == '\t' || *start == '\n' || *start == '\r') {
        start++;
    }
    const char* end = start;
    size_t len = 0;
    while (len < max_len && end[len] != '\0' && end[len] != '\n' && end[len] != '\r') {
        len++;
    }
    // Trim trailing whitespace
    while (len > 0 && (start[len - 1] == ' ' || start[len - 1] == '\t')) {
        len--;
    }

    if (len == 0 || len >= HOSTNAME_MAX) {
        return false;
    }
    if (start[0] == '-' || start[len - 1] == '-') {
        return false;
    }
    for (size_t i = 0; i < len; i++) {
        if (!is_valid_char(start[i])) {
            return false;
        }
    }

    memcpy(s_hostname, start, len);
    s_hostname[len] = '\0';
    ker::mod::dbg::log("[hostname] Loaded '%s' from %s", s_hostname, source_label);
    return true;
}

void init() {
    // Priority 1: QEMU fw_cfg (opt/wos/hostname) — per-VM override
    char fw_buf[HOSTNAME_MAX] = {};
    int fw_len = ker::platform::fw::fw_cfg_read_file("opt/wos/hostname", fw_buf, sizeof(fw_buf) - 1);
    if (fw_len > 0) {
        fw_buf[fw_len] = '\0';
        if (try_set_hostname(fw_buf, static_cast<size_t>(fw_len), "fw_cfg")) {
            return;
        }
    }

    // Priority 2: /etc/hostname from VFS
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

    if (!try_set_hostname(buf, static_cast<size_t>(n), "/etc/hostname")) {
        ker::mod::dbg::log("[hostname] Invalid /etc/hostname content, using default '%s'", s_hostname);
    }
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
