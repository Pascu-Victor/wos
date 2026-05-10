#include "netdevconf.hpp"

#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <net/netdevice.hpp>
#include <string_view>
#include <vfs/file.hpp>
#include <vfs/vfs.hpp>

#include "platform/dbg/dbg.hpp"

namespace ker::util::netdevconf {

constexpr const char* NETDEVS_PATH = "/etc/netdevs";
constexpr size_t BUF_SIZE = 512;

auto find_device(const char* driver) -> net::NetDevice* {
    if (driver == nullptr) {
        return nullptr;
    }
    std::string_view const DRIVER_NAME{driver};
    auto* f = ker::vfs::vfs_open_file(NETDEVS_PATH, 0, 0);
    if (f == nullptr || f->fops == nullptr || f->fops->vfs_read == nullptr) {
        if (f != nullptr && f->fops != nullptr && f->fops->vfs_close != nullptr) {
            f->fops->vfs_close(f);
        }
        ker::mod::dbg::log("[netdevconf] %s not found, using hardcoded defaults", NETDEVS_PATH);
        return nullptr;
    }

    std::array<char, BUF_SIZE> buf{};
    ssize_t const N = f->fops->vfs_read(f, buf.data(), buf.size() - 1, 0);
    if (f->fops->vfs_close != nullptr) {
        f->fops->vfs_close(f);
    }
    if (N <= 0) {
        return nullptr;
    }
    buf[static_cast<size_t>(N)] = '\0';  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)

    // Parse line by line: "<ifname> <driver>"
    const char* pos = buf.data();
    while (*pos != '\0') {
        // Skip whitespace and blank lines
        while (*pos == ' ' || *pos == '\t' || *pos == '\r' || *pos == '\n') {
            pos++;
        }
        if (*pos == '\0') {
            break;
        }
        // Skip comment lines
        if (*pos == '#') {
            while (*pos != '\0' && *pos != '\n') {
                pos++;
            }
            continue;
        }

        // Read ifname token
        const char* ifname_start = pos;
        while (*pos != '\0' && *pos != ' ' && *pos != '\t' && *pos != '\n') {
            pos++;
        }
        auto const IFNAME_LEN = static_cast<size_t>(pos - ifname_start);

        // Skip whitespace between tokens
        while (*pos == ' ' || *pos == '\t') {
            pos++;
        }

        // Read driver token
        const char* driver_start = pos;
        while (*pos != '\0' && *pos != ' ' && *pos != '\t' && *pos != '\n' && *pos != '\r') {
            pos++;
        }
        auto const DRIVER_LEN = static_cast<size_t>(pos - driver_start);

        // Skip to end of line
        while (*pos != '\0' && *pos != '\n') {
            pos++;
        }

        if (IFNAME_LEN == 0 || DRIVER_LEN == 0) {
            continue;
        }

        std::string_view const DRIVER_TOKEN{driver_start, DRIVER_LEN};
        if (DRIVER_TOKEN == DRIVER_NAME) {
            // Null-terminate ifname for lookup
            std::array<char, 32> ifname{};
            size_t const COPY_LEN = IFNAME_LEN < ifname.size() - 1 ? IFNAME_LEN : ifname.size() - 1;
            std::copy_n(ifname_start, COPY_LEN, ifname.data());
            ifname[COPY_LEN] = '\0';  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)

            auto* dev = net::netdev_find_by_name(ifname.data());
            if (dev != nullptr) {
                ker::mod::dbg::log("[netdevconf] assigned %s -> driver '%s'", ifname.data(), driver);
            } else {
                ker::mod::dbg::log("[netdevconf] %s: device '%s' not found", NETDEVS_PATH, ifname.data());
            }
            return dev;
        }
    }

    ker::mod::dbg::log("[netdevconf] no entry for driver '%s' in %s", driver, NETDEVS_PATH);
    return nullptr;
}

}  // namespace ker::util::netdevconf
