#include "fstab.h"

#include <sys/vfs.h>
#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <print>

#include "init_log.h"
#include "sys/multiproc.h"

namespace {
constexpr size_t FSTAB_BUF_SIZE = 4096;
constexpr size_t FIELD_MAX = 256;
}  // namespace

void mount_filesystems() {
    uint64_t cpuno = ker::multiproc::currentThreadId();

    int fstab_fd = ker::abi::vfs::open("/etc/fstab", 0, 0);
    if (fstab_fd < 0) {
        init_info("init[%llu]: no /etc/fstab found, skipping mounts", static_cast<unsigned long long>(cpuno));
        return;
    }

    std::array<char, FSTAB_BUF_SIZE> fstab_buf{};
    ssize_t bytes_read = ker::abi::vfs::read(fstab_fd, fstab_buf.data(), FSTAB_BUF_SIZE - 1);
    ker::abi::vfs::close(fstab_fd);

    if (bytes_read <= 0) {
        init_info("init[%llu]: /etc/fstab is empty", static_cast<unsigned long long>(cpuno));
        return;
    }

    fstab_buf[static_cast<size_t>(bytes_read)] = '\0';
    init_info("init[%llu]: parsing /etc/fstab (%ld bytes)", static_cast<unsigned long long>(cpuno), static_cast<long>(bytes_read));

    // Parse line by line
    char* line_start = fstab_buf.data();
    while (*line_start != '\0') {
        // Find end of line
        char* line_end = line_start;
        while (*line_end != '\0' && *line_end != '\n') {
            line_end++;
        }
        char saved = *line_end;
        *line_end = '\0';

        // Skip whitespace
        char* p = line_start;
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
            size_t fi = 0;
            while (*p != '\0' && *p != ' ' && *p != '\t' && fi < FIELD_MAX - 1) {
                device[fi++] = *p++;
            }
            device[fi] = '\0';

            // Skip whitespace
            while (*p == ' ' || *p == '\t') {
                p++;
            }

            // Parse mountpoint field
            fi = 0;
            while (*p != '\0' && *p != ' ' && *p != '\t' && fi < FIELD_MAX - 1) {
                mountpoint[fi++] = *p++;
            }
            mountpoint[fi] = '\0';

            // Skip whitespace
            while (*p == ' ' || *p == '\t') {
                p++;
            }

            // Parse fstype field
            fi = 0;
            while (*p != '\0' && *p != ' ' && *p != '\t' && fi < FIELD_MAX - 1) {
                fstype[fi++] = *p++;
            }
            fstype[fi] = '\0';

            if (device[0] != '\0' && mountpoint[0] != '\0' && fstype[0] != '\0') {
                // Create mount point directory
                constexpr mode_t DIR_MODE = 0755;
                ker::abi::vfs::mkdir(mountpoint.data(), DIR_MODE);

                // Mount filesystem
                int ret = ker::abi::vfs::mount(device.data(), mountpoint.data(), fstype.data());
                if (ret == 0) {
                    init_info("init[%llu]: mounted %s at %s (%s)", static_cast<unsigned long long>(cpuno), device.data(), mountpoint.data(),
                              fstype.data());
                } else {
                    init_error("init[%llu]: failed to mount %s at %s (%s): error %d", static_cast<unsigned long long>(cpuno), device.data(),
                               mountpoint.data(), fstype.data(), ret);
                }
            }
        }

        // Advance to next line
        if (saved == '\0') {
            break;
        }
        line_start = line_end + 1;
    }
}
