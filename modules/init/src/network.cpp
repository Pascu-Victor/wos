#include "network.h"

#include <abi-bits/in.h>
#include <abi-bits/ioctls.h>
#include <abi-bits/socket.h>
#include <arpa/inet.h>
#include <net/if.h>
#include <sched.h>
#include <sys/ioctl.h>
#include <sys/logging.h>
#include <sys/process.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <ctime>

#include "sys/multiproc.h"

namespace {
using init_log = wos::journal<"init">;
}

void start_network() {
    int const CPUNO = ker::multiproc::currentThreadId();

    init_log::info("init[%d]: spawning netd (DHCP daemon)", CPUNO);
    std::array<const char*, 2> netd_argv = {"/sbin/netd", nullptr};
    std::array<const char*, 1> netd_envp = {nullptr};
    uint64_t const NETD_PID = ker::process::exec("/sbin/netd", netd_argv.data(), netd_envp.data());
    if (NETD_PID == 0) {
        init_log::error("init[%d]: failed to spawn netd", CPUNO);
    } else {
        init_log::info("init[%d]: netd spawned as PID %llu", CPUNO, static_cast<unsigned long long>(NETD_PID));
    }

    // Poll eth0 for IP address readiness (wait for DHCP to complete)
    int const POLL_SOCK = socket(AF_INET, SOCK_DGRAM, 0);
    if (POLL_SOCK >= 0) {
        struct ifreq ifr{};
        strncpy(ifr.ifr_name, "eth0", IFNAMSIZ - 1);

        constexpr long POLL_TIMEOUT_SECS = 10;
        struct timespec poll_start{};
        clock_gettime(CLOCK_MONOTONIC, &poll_start);
        bool net_ready = false;
        for (;;) {
            if (ioctl(POLL_SOCK, SIOCGIFADDR, &ifr) == 0) {
                auto* addr = reinterpret_cast<struct sockaddr_in*>(&ifr.ifr_addr);
                if (addr->sin_addr.s_addr != 0) {
                    std::array<char, INET_ADDRSTRLEN> ip_str{};
                    inet_ntop(AF_INET, &addr->sin_addr, ip_str.data(), ip_str.size());
                    init_log::info("init[%d]: eth0 configured with IP %s", CPUNO, ip_str.data());
                    net_ready = true;
                    break;
                }
            }
            struct timespec now{};
            clock_gettime(CLOCK_MONOTONIC, &now);
            if (now.tv_sec - poll_start.tv_sec >= POLL_TIMEOUT_SECS) {
                break;
            }
            sched_yield();
        }
        close(POLL_SOCK);
        if (!net_ready) {
            init_log::warn("init[%d]: eth0 not configured after polling, continuing anyway", CPUNO);
        }
    }
}
