#include "hostname.hpp"

#include <bits/ssize_t.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <platform/fw/qemu_fw_cfg.hpp>
#include <vfs/file.hpp>
#include <vfs/vfs.hpp>

#include "platform/dbg/dbg.hpp"

namespace ker::util::hostname {

namespace {

std::array<char, HOSTNAME_MAX> s_hostname{'w', 'o', 's', '\0'};

auto is_valid_char(char c) -> bool { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-'; }

auto try_set_hostname(const char* src, size_t max_len, const char* source_label) -> bool {
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

    std::memcpy(s_hostname.data(), start, len);
    s_hostname.at(len) = '\0';
    ker::mod::dbg::log("[hostname] Loaded '%s' from %s", s_hostname.data(), source_label);
    return true;
}

}  // namespace

void init() {
    // Priority 1: QEMU fw_cfg (opt/wos/hostname) — per-VM override
    std::array<char, HOSTNAME_MAX> fw_buf{};
    int const FW_LEN = ker::platform::fw::fw_cfg_read_file("opt/wos/hostname", fw_buf.data(), fw_buf.size() - 1);
    if (FW_LEN > 0) {
        fw_buf.at(static_cast<size_t>(FW_LEN)) = '\0';
        if (try_set_hostname(fw_buf.data(), static_cast<size_t>(FW_LEN), "fw_cfg")) {
            return;
        }
    }

    // Priority 2: /etc/hostname from VFS
    auto* f = ker::vfs::vfs_open_file("/etc/hostname", 0, 0);
    if (f == nullptr || f->fops == nullptr || f->fops->vfs_read == nullptr) {
        if (f != nullptr && f->fops != nullptr && f->fops->vfs_close != nullptr) {
            f->fops->vfs_close(f);
        }
        ker::mod::dbg::log("[hostname] No /etc/hostname, using default '%s'", s_hostname.data());
        return;
    }

    std::array<char, HOSTNAME_MAX + 16> buf{};
    ssize_t const N = f->fops->vfs_read(f, buf.data(), buf.size() - 1, 0);
    if (f->fops->vfs_close != nullptr) {
        f->fops->vfs_close(f);
    }
    if (N <= 0) {
        return;
    }
    buf.at(static_cast<size_t>(N)) = '\0';

    if (!try_set_hostname(buf.data(), static_cast<size_t>(N), "/etc/hostname")) {
        ker::mod::dbg::log("[hostname] Invalid /etc/hostname content, using default '%s'", s_hostname.data());
    }
}

auto get() -> const char* { return s_hostname.data(); }

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
    std::memcpy(s_hostname.data(), name, len);
    s_hostname.at(len) = '\0';
    return 0;
}

}  // namespace ker::util::hostname
