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
    int const SOCK = socket(AF_INET, SOCK_DGRAM, 0);
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
        int const RET = ioctl(SOCK, SIOCSIFADDR, &ifr);
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
        int const RET = ioctl(SOCK, SIOCSIFNETMASK, &ifr);
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
        int const RET = ioctl(SOCK, SIOCADDRT, &rt);
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
        int const RET = ioctl(SOCK, SIOCADDRT, &rt);
        if (RET != 0) {
            logger::warn("netd: failed to add default route for %s: ret=%d errno=%d", ifname, RET, errno);
        }
    }

    close(SOCK);
    if (applied) {
        write_resolv_conf(lease);
    }
    return applied;
}

}  // namespace netd
