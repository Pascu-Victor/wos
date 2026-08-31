#include "netdevconf.hpp"

#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <net/netdevice.hpp>
#include <platform/fw/qemu_fw_cfg.hpp>
#include <span>
#include <string_view>
#include <utility>
#include <vfs/file.hpp>
#include <vfs/vfs.hpp>

#include "platform/dbg/dbg.hpp"

namespace ker::util::netdevconf {

constexpr const char* NETDEVS_PATH = "/etc/netdevs";
constexpr const char* NETDEVS_FW_CFG_PATH = "opt/wos/netdevs";
constexpr size_t BUF_SIZE = 512;

auto find_devices(const char* driver, std::span<net::NetDeviceRef> devices) -> size_t {
    if (driver == nullptr || devices.empty()) {
        return 0;
    }
    std::string_view const DRIVER_NAME{driver};
    std::array<char, BUF_SIZE> buf{};
    ssize_t bytes_read = ker::platform::fw::fw_cfg_read_file(NETDEVS_FW_CFG_PATH, buf.data(), buf.size() - 1);
    if (bytes_read > 0) {
        ker::mod::dbg::log("[netdevconf] loading per-node policy from fw_cfg");
    } else {
        auto* f = ker::vfs::vfs_open_file(NETDEVS_PATH, 0, 0);
        if (f == nullptr || f->fops == nullptr || f->fops->vfs_read == nullptr) {
            if (f != nullptr) {
                static_cast<void>(ker::vfs::vfs_close_file(f));
            }
            ker::mod::dbg::log("[netdevconf] %s not found, using hardcoded defaults", NETDEVS_PATH);
            return 0;
        }
        bytes_read = f->fops->vfs_read(f, buf.data(), buf.size() - 1, 0);
        static_cast<void>(ker::vfs::vfs_close_file(f));
    }
    if (bytes_read <= 0) {
        return 0;
    }
    auto const CONFIG_LEN = static_cast<size_t>(bytes_read);
    buf.at(CONFIG_LEN) = '\0';
    for (size_t i = 0; i != CONFIG_LEN; ++i) {
        if (buf.at(i) == ';') {
            buf.at(i) = '\n';
        }
    }

    // Parse line by line: "<ifname> <driver>"
    size_t device_count = 0;
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

            auto dev_ref = net::netdev_find_by_name_ref(ifname.data());
            if (dev_ref) {
                ker::mod::dbg::log("[netdevconf] assigned %s -> driver '%s'", ifname.data(), driver);
                if (device_count == devices.size()) {
                    ker::mod::dbg::log("[netdevconf] too many entries for driver '%s' in %s", driver, NETDEVS_PATH);
                    break;
                }
                devices[device_count++] = std::move(dev_ref);
            } else {
                ker::mod::dbg::log("[netdevconf] %s: device '%s' not found", NETDEVS_PATH, ifname.data());
            }
        }
    }

    if (device_count == 0) {
        ker::mod::dbg::log("[netdevconf] no live entry for driver '%s' in %s", driver, NETDEVS_PATH);
    }
    return device_count;
}

}  // namespace ker::util::netdevconf
