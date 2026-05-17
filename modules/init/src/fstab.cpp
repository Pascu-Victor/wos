#include "fstab.h"

#include <abi-bits/mode_t.h>
#include <bits/ssize_t.h>
#include <sys/logging.h>
#include <sys/vfs.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "sys/multiproc.h"

namespace {
constexpr size_t FSTAB_BUF_SIZE = 4096;
constexpr size_t FIELD_MAX = 256;
constexpr mode_t DIR_MODE = 0755;
constexpr const char* REQUIRED_TMP_PATH = "/tmp";
constexpr const char* REQUIRED_TMP_SOURCE = "tmpfs";
constexpr const char* REQUIRED_TMP_FSTYPE = "tmpfs";
using init_log = wos::journal<"init">;

auto parse_field(const char*& cursor, std::array<char, FIELD_MAX>& out) -> bool {
    size_t field_index = 0;
    while (*cursor != '\0' && *cursor != ' ' && *cursor != '\t' && field_index < out.size() - 1) {
        out.at(field_index++) = *cursor++;
    }
    out.at(field_index) = '\0';
    return out.front() != '\0';
}

auto is_required_tmp_mountpoint(const char* mountpoint) -> bool { return std::strcmp(mountpoint, REQUIRED_TMP_PATH) == 0; }

auto mount_required_tmpfs(uint64_t cpuno) -> bool {
    ker::abi::vfs::mkdir(REQUIRED_TMP_PATH, DIR_MODE);

    int const RET = ker::abi::vfs::mount(REQUIRED_TMP_SOURCE, REQUIRED_TMP_PATH, REQUIRED_TMP_FSTYPE);
    if (RET == 0) {
        init_log::info("init[%llu]: mounted required tmpfs at %s", static_cast<unsigned long long>(cpuno), REQUIRED_TMP_PATH);
        return true;
    }

    init_log::error("init[%llu]: failed to mount required tmpfs at %s: error %d", static_cast<unsigned long long>(cpuno), REQUIRED_TMP_PATH,
                    RET);
    return false;
}
}  // namespace

void mount_filesystems() {
    uint64_t const CPUNO = ker::multiproc::currentThreadId();
    bool const REQUIRED_TMPFS_MOUNTED = mount_required_tmpfs(CPUNO);

    int const FSTAB_FD = ker::abi::vfs::open("/etc/fstab", 0, 0);
    if (FSTAB_FD < 0) {
        init_log::info("init[%llu]: no /etc/fstab found, skipping fstab mounts", static_cast<unsigned long long>(CPUNO));
        return;
    }

    std::array<char, FSTAB_BUF_SIZE> fstab_buf{};
    ssize_t const BYTES_READ = ker::abi::vfs::read(FSTAB_FD, fstab_buf.data(), FSTAB_BUF_SIZE - 1);
    ker::abi::vfs::close(FSTAB_FD);

    if (BYTES_READ <= 0) {
        init_log::info("init[%llu]: /etc/fstab is empty", static_cast<unsigned long long>(CPUNO));
        return;
    }

    fstab_buf.at(static_cast<size_t>(BYTES_READ)) = '\0';
    init_log::info("init[%llu]: parsing /etc/fstab (%ld bytes)", static_cast<unsigned long long>(CPUNO), static_cast<long>(BYTES_READ));

    // Parse line by line
    char* line_start = fstab_buf.data();
    while (*line_start != '\0') {
        // Find end of line
        char* line_end = line_start;
        while (*line_end != '\0' && *line_end != '\n') {
            line_end++;
        }
        char const SAVED = *line_end;
        *line_end = '\0';

        // Skip whitespace
        char const* p = line_start;
        while (*p == ' ' || *p == '\t') {
            p++;
        }

        // Skip comments and empty lines
        if (*p != '#' && *p != '\0') {
            // Extract fields: device mountpoint fstype options
            std::array<char, FIELD_MAX> device{};
            std::array<char, FIELD_MAX> mountpoint{};
            std::array<char, FIELD_MAX> fstype{};

            // Parse device field
            bool const HAS_DEVICE = parse_field(p, device);

            // Skip whitespace
            while (*p == ' ' || *p == '\t') {
                p++;
            }

            // Parse mountpoint field
            bool const HAS_MOUNTPOINT = parse_field(p, mountpoint);

            // Skip whitespace
            while (*p == ' ' || *p == '\t') {
                p++;
            }

            // Parse fstype field
            bool const HAS_FSTYPE = parse_field(p, fstype);

            if (HAS_DEVICE && HAS_MOUNTPOINT && HAS_FSTYPE) {
                if (REQUIRED_TMPFS_MOUNTED && is_required_tmp_mountpoint(mountpoint.data())) {
                    if (std::strcmp(fstype.data(), REQUIRED_TMP_FSTYPE) != 0) {
                        init_log::warn("init[%llu]: ignoring /etc/fstab entry for %s (%s); required tmpfs is already mounted",
                                       static_cast<unsigned long long>(CPUNO), REQUIRED_TMP_PATH, fstype.data());
                    }
                    continue;
                }

                // Create mount point directory
                ker::abi::vfs::mkdir(mountpoint.data(), DIR_MODE);

                // Mount filesystem
                int const RET = ker::abi::vfs::mount(device.data(), mountpoint.data(), fstype.data());
                if (RET == 0) {
                    init_log::info("init[%llu]: mounted %s at %s (%s)", static_cast<unsigned long long>(CPUNO), device.data(),
                                   mountpoint.data(), fstype.data());
                } else {
                    init_log::error("init[%llu]: failed to mount %s at %s (%s): error %d", static_cast<unsigned long long>(CPUNO),
                                    device.data(), mountpoint.data(), fstype.data(), RET);
                }
            }
        }

        // Advance to next line
        if (SAVED == '\0') {
            break;
        }
        line_start = line_end + 1;
    }
}
