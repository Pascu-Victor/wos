#include "netdevconf.hpp"

#include <cstring>
#include <net/netdevice.hpp>
#include <vfs/file.hpp>
#include <vfs/vfs.hpp>

#include "platform/dbg/dbg.hpp"

namespace ker::util::netdevconf {

static constexpr const char* NETDEVS_PATH = "/etc/netdevs";
static constexpr size_t BUF_SIZE = 512;

auto find_device(const char* driver) -> net::NetDevice* {
    auto* f = ker::vfs::vfs_open_file(NETDEVS_PATH, 0, 0);
    if (f == nullptr || f->fops == nullptr || f->fops->vfs_read == nullptr) {
        if (f != nullptr && f->fops != nullptr && f->fops->vfs_close != nullptr) {
            f->fops->vfs_close(f);
        }
        ker::mod::dbg::log("[netdevconf] %s not found, using hardcoded defaults", NETDEVS_PATH);
        return nullptr;
    }

    char buf[BUF_SIZE] = {};
    ssize_t n = f->fops->vfs_read(f, buf, sizeof(buf) - 1, 0);
    if (f->fops->vfs_close != nullptr) {
        f->fops->vfs_close(f);
    }
    if (n <= 0) {
        return nullptr;
    }
    buf[n] = '\0';

    // Parse line by line: "<ifname> <driver>"
    const char* pos = buf;
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
        size_t ifname_len = static_cast<size_t>(pos - ifname_start);

        // Skip whitespace between tokens
        while (*pos == ' ' || *pos == '\t') {
            pos++;
        }

        // Read driver token
        const char* driver_start = pos;
        while (*pos != '\0' && *pos != ' ' && *pos != '\t' && *pos != '\n' && *pos != '\r') {
            pos++;
        }
        size_t driver_len = static_cast<size_t>(pos - driver_start);

        // Skip to end of line
        while (*pos != '\0' && *pos != '\n') {
            pos++;
        }

        if (ifname_len == 0 || driver_len == 0) {
            continue;
        }

        // Compare driver token
        if (driver_len == std::strlen(driver) && std::strncmp(driver_start, driver, driver_len) == 0) {
            // Null-terminate ifname for lookup
            char ifname[32] = {};
            size_t copy_len = ifname_len < sizeof(ifname) - 1 ? ifname_len : sizeof(ifname) - 1;
            std::memcpy(ifname, ifname_start, copy_len);
            ifname[copy_len] = '\0';

            auto* dev = net::netdev_find_by_name(ifname);
            if (dev != nullptr) {
                ker::mod::dbg::log("[netdevconf] assigned %s -> driver '%s'", ifname, driver);
            } else {
                ker::mod::dbg::log("[netdevconf] %s: device '%s' not found", NETDEVS_PATH, ifname);
            }
            return dev;
        }
    }

    ker::mod::dbg::log("[netdevconf] no entry for driver '%s' in %s", driver, NETDEVS_PATH);
    return nullptr;
}

}  // namespace ker::util::netdevconf
