#include "network.h"

#include <arpa/inet.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sched.h>
#include <sys/ioctl.h>
#include <sys/process.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cstring>
#include <print>

#include "init_log.h"
#include "sys/multiproc.h"

void start_network() {
    int cpuno = ker::multiproc::currentThreadId();

    init_info("init[%d]: spawning netd (DHCP daemon)", cpuno);
    std::array<const char*, 2> netd_argv = {"/sbin/netd", nullptr};
    std::array<const char*, 1> netd_envp = {nullptr};
    uint64_t netd_pid = ker::process::exec("/sbin/netd", netd_argv.data(), netd_envp.data());
    if (netd_pid == 0) {
        init_error("init[%d]: failed to spawn netd", cpuno);
    } else {
        init_info("init[%d]: netd spawned as PID %llu", cpuno, static_cast<unsigned long long>(netd_pid));
    }

    // Poll eth0 for IP address readiness (wait for DHCP to complete)
    int poll_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (poll_sock >= 0) {
        struct ifreq ifr{};
        strncpy(ifr.ifr_name, "eth0", IFNAMSIZ - 1);

        constexpr long POLL_TIMEOUT_SECS = 10;
        struct timespec poll_start{};
        clock_gettime(CLOCK_MONOTONIC, &poll_start);
        bool net_ready = false;
        for (;;) {
            if (ioctl(poll_sock, SIOCGIFADDR, &ifr) == 0) {
                auto* addr = reinterpret_cast<struct sockaddr_in*>(&ifr.ifr_addr);
                if (addr->sin_addr.s_addr != 0) {
                    std::array<char, INET_ADDRSTRLEN> ip_str{};
                    inet_ntop(AF_INET, &addr->sin_addr, ip_str.data(), ip_str.size());
                    init_info("init[%d]: eth0 configured with IP %s", cpuno, ip_str.data());
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
        close(poll_sock);
        if (!net_ready) {
            init_warn("init[%d]: eth0 not configured after polling, continuing anyway", cpuno);
        }
    }
}
