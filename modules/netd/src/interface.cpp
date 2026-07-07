#include "netd/interface.hpp"

#include <abi-bits/in.h>
#include <abi-bits/ioctls.h>
#include <abi-bits/route.h>
#include <abi-bits/socket.h>
#include <arpa/inet.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <span>

#include "netd/log.hpp"
#include "netd/resolver.hpp"

namespace netd {
namespace {

void copy_ifreq_name(struct ifreq& ifr, const char* ifname) {
    auto dest = std::span<char, IFNAMSIZ>(ifr.ifr_name);
    std::ranges::fill(dest, '\0');
    if (ifname == nullptr) {
        return;
    }

    size_t const LEN = std::min(std::strlen(ifname), dest.size() - 1);
    std::copy_n(ifname, LEN, dest.data());
}

void boot_trace(const char* message) {
    if (message == nullptr) {
        return;
    }
    size_t len = 0;
    while (message[len] != '\0') {
        len++;
    }
    (void)::write(STDERR_FILENO, message, len);
}

}  // namespace

auto get_mac(int sock, const char* ifname, std::array<uint8_t, 6>& mac) -> bool {
    ifreq ifr{};
    copy_ifreq_name(ifr, ifname);
    if (ioctl(sock, SIOCGIFHWADDR, &ifr) != 0) {
        return false;
    }

    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay): sockaddr is a C ioctl ABI.
    std::memcpy(mac.data(), ifr.ifr_hwaddr.sa_data, mac.size());
    return true;
}

auto apply_lease(const char* ifname, const DhcpLease& lease) -> bool {
    boot_trace("netd-boot: apply_lease entered\n");
    int const SOCK = socket(AF_INET, SOCK_DGRAM, 0);
    boot_trace("netd-boot: apply socket returned\n");
    if (SOCK < 0) {
        logger::error("netd: failed to open lease apply socket: errno=%d", errno);
        return false;
    }

    bool applied = true;

    {
        ifreq ifr{};
        copy_ifreq_name(ifr, ifname);
        auto* addr = reinterpret_cast<struct sockaddr_in*>(&ifr.ifr_addr);
        addr->sin_family = AF_INET;
        addr->sin_addr.s_addr = htonl(lease.your_ip);
        boot_trace("netd-boot: before SIOCSIFADDR\n");
        int const RET = ioctl(SOCK, SIOCSIFADDR, &ifr);
        boot_trace("netd-boot: after SIOCSIFADDR\n");
        if (RET != 0) {
            logger::error("netd: failed to set %s IPv4 address: ret=%d errno=%d", ifname, RET, errno);
            applied = false;
        }
    }

    if (lease.subnet_mask != 0) {
        ifreq ifr{};
        copy_ifreq_name(ifr, ifname);
        auto* addr = reinterpret_cast<struct sockaddr_in*>(&ifr.ifr_netmask);
        addr->sin_family = AF_INET;
        addr->sin_addr.s_addr = htonl(lease.subnet_mask);
        boot_trace("netd-boot: before SIOCSIFNETMASK\n");
        int const RET = ioctl(SOCK, SIOCSIFNETMASK, &ifr);
        boot_trace("netd-boot: after SIOCSIFNETMASK\n");
        if (RET != 0) {
            logger::error("netd: failed to set %s IPv4 netmask: ret=%d errno=%d", ifname, RET, errno);
            applied = false;
        }
    }

    if (lease.subnet_mask != 0) {
        struct rtentry rt{};
        rt.rt_flags = RTF_UP;
        auto* dst = reinterpret_cast<struct sockaddr_in*>(&rt.rt_dst);
        dst->sin_family = AF_INET;
        dst->sin_addr.s_addr = htonl(lease.your_ip & lease.subnet_mask);
        auto* genmask = reinterpret_cast<struct sockaddr_in*>(&rt.rt_genmask);
        genmask->sin_family = AF_INET;
        genmask->sin_addr.s_addr = htonl(lease.subnet_mask);
        boot_trace("netd-boot: before local SIOCADDRT\n");
        int const RET = ioctl(SOCK, SIOCADDRT, &rt);
        boot_trace("netd-boot: after local SIOCADDRT\n");
        if (RET != 0) {
            logger::warn("netd: failed to add local subnet route for %s: ret=%d errno=%d", ifname, RET, errno);
        }
    }

    if (lease.router != 0) {
        rtentry rt{};
        rt.rt_flags = RTF_UP | RTF_GATEWAY;
        auto* gateway = reinterpret_cast<struct sockaddr_in*>(&rt.rt_gateway);
        gateway->sin_family = AF_INET;
        gateway->sin_addr.s_addr = htonl(lease.router);
        boot_trace("netd-boot: before default SIOCADDRT\n");
        int const RET = ioctl(SOCK, SIOCADDRT, &rt);
        boot_trace("netd-boot: after default SIOCADDRT\n");
        if (RET != 0) {
            logger::warn("netd: failed to add default route for %s: ret=%d errno=%d", ifname, RET, errno);
        }
    }

    boot_trace("netd-boot: before close apply socket\n");
    close(SOCK);
    boot_trace("netd-boot: after close apply socket\n");
    if (applied) {
        boot_trace("netd-boot: before write_resolv_conf\n");
        write_resolv_conf(lease);
        boot_trace("netd-boot: after write_resolv_conf\n");
    }
    boot_trace("netd-boot: apply_lease returning\n");
    return applied;
}

}  // namespace netd
