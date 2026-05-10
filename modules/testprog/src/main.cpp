#include <abi-bits/access.h>
#include <abi-bits/in.h>
#include <abi-bits/ioctls.h>
#include <abi-bits/socket.h>
#include <abi-bits/socklen_t.h>
#include <abi-bits/stat.h>
#include <arpa/inet.h>
#include <bits/ssize_t.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/logging.h>
#include <sys/multiproc.h>
#include <sys/process.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <print>

#include "fsbench.hpp"
#include "mandelbench/config.hpp"
#include "mandelbench/mandelbench_wki.hpp"
#include "netbench.hpp"
#include "perfbench.hpp"

using tprog_log = wos::journal<"tprog">;

// ICMP Echo Request structure
struct IcmpHeader {
    uint8_t type;
    uint8_t code;
    uint16_t checksum;
    uint16_t id;
    uint16_t sequence;
};

// Calculate checksum for ICMP
static auto icmp_checksum(void* data, size_t len) -> uint16_t {
    uint32_t sum = 0;
    auto* ptr = static_cast<uint16_t*>(data);

    while (len > 1) {
        sum += *ptr++;
        len -= 2;
    }

    if (len == 1) {
        sum += *reinterpret_cast<uint8_t*>(ptr);
    }

    sum = (sum >> 16) + (sum & 0xFFFF);
    sum += (sum >> 16);

    return static_cast<uint16_t>(~sum);
}

// Ping a specific IP address
static auto ping(const char* ip_str) -> bool {
    int const PID = ker::process::getpid();
    int const TID = ker::multiproc::currentThreadId();

    // std::println("testprog[t:{},p:{}]: Pinging {}...", tid, pid, ip_str);

    // Create raw socket for ICMP
    int const SOCK = socket(AF_INET, SOCK_RAW, 1);  // 1 = IPPROTO_ICMP
    if (SOCK < 0) {
        tprog_log::error("testprog[t:%d,p:%d]: failed to create socket: %d", TID, PID, SOCK);
        return false;
    }

    // Set up destination address
    struct sockaddr_in dest_addr{};
    dest_addr.sin_family = AF_INET;
    inet_pton(AF_INET, ip_str, &dest_addr.sin_addr);

    // Prepare ICMP packet
    constexpr size_t PACKET_SIZE = sizeof(IcmpHeader) + 32;  // 32 bytes of data
    std::array<uint8_t, PACKET_SIZE> packet{};
    auto* icmp = reinterpret_cast<IcmpHeader*>(packet.data());

    icmp->type = 8;  // Echo Request
    icmp->code = 0;
    icmp->id = htons(static_cast<uint16_t>(PID));
    icmp->sequence = htons(1);
    icmp->checksum = 0;

    // Fill data with pattern
    for (size_t i = sizeof(IcmpHeader); i < PACKET_SIZE; i++) {
        packet[i] = static_cast<uint8_t>(i);
    }

    // Calculate checksum
    icmp->checksum = icmp_checksum(packet.data(), PACKET_SIZE);

    // Send packet
    ssize_t const SENT = sendto(SOCK, packet.data(), PACKET_SIZE, 0, reinterpret_cast<struct sockaddr*>(&dest_addr), sizeof(dest_addr));

    if (SENT < 0) {
        tprog_log::error("testprog[t:%d,p:%d]: failed to send ping: %lld", TID, PID, static_cast<long long>(SENT));
        close(SOCK);
        return false;
    }

    // std::println("testprog[t:{},p:{}]: Sent {} bytes to {}", tid, pid, sent, ip_str);

    // Try to receive response (with polling retry for EAGAIN)
    std::array<uint8_t, 1024> recv_buf{};
    struct sockaddr_in from_addr{};
    socklen_t from_len = sizeof(from_addr);

    ssize_t received = -1;
    constexpr int MAX_RETRIES = 4;
    for (int retry = 0; retry < MAX_RETRIES; ++retry) {
        received = recvfrom(SOCK, recv_buf.data(), recv_buf.size(), 0, reinterpret_cast<struct sockaddr*>(&from_addr), &from_len);
        if (received != -11) {  // -11 is EAGAIN
            break;
        }
    }

    close(SOCK);

    if (received > 0) {
        return true;
    }
    tprog_log::warn("testprog[t:%d,p:%d]: no response from %s (received=%lld)", TID, PID, ip_str, static_cast<long long>(received));
    return false;
}

// Get network interface information
static auto get_interface_info(const char* ifname) -> bool {
    int const PID = ker::process::getpid();
    int const TID = ker::multiproc::currentThreadId();

    // std::println("testprog[t:{},p:{}]: Getting info for interface {}...", tid, pid, ifname);

    int const SOCK = socket(AF_INET, SOCK_DGRAM, 0);
    if (SOCK < 0) {
        tprog_log::error("testprog[t:%d,p:%d]: failed to create socket for ioctl", TID, PID);
        return false;
    }

    // Get interface address
    struct ifreq ifr{};
    strncpy(ifr.ifr_name, ifname, IFNAMSIZ - 1);

    uint32_t ip_addr = 0;
    if (ioctl(SOCK, SIOCGIFADDR, &ifr) == 0) {
        auto* addr = reinterpret_cast<struct sockaddr_in*>(&ifr.ifr_addr);
        ip_addr = ntohl(addr->sin_addr.s_addr);
        char ip_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &addr->sin_addr, ip_str, sizeof(ip_str));
        // std::println("testprog[t:{},p:{}]:   IP address: {}", tid, pid, ip_str);
    } else {
        tprog_log::error("testprog[t:%d,p:%d]: failed to get IP address", TID, PID);
        close(SOCK);
        return false;
    }

    // Get netmask and calculate gateway
    strncpy(ifr.ifr_name, ifname, IFNAMSIZ - 1);
    if (ioctl(SOCK, SIOCGIFNETMASK, &ifr) == 0) {
        auto* addr = reinterpret_cast<struct sockaddr_in*>(&ifr.ifr_netmask);
        char mask_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &addr->sin_addr, mask_str, sizeof(mask_str));
        // std::println("testprog[t:{},p:{}]:   Netmask: {}", tid, pid, mask_str);

        // Calculate gateway (QEMU user-mode uses .2 as gateway, not .1)
        uint32_t const MASK = ntohl(addr->sin_addr.s_addr);
        uint32_t const GATEWAY = (ip_addr & MASK) | 0x00000001;  // Set last byte to .2 for QEMU

        struct in_addr gw_addr{};
        gw_addr.s_addr = htonl(GATEWAY);
        char gw_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &gw_addr, gw_str, sizeof(gw_str));
        // std::println("testprog[t:{},p:{}]:   Gateway (assumed): {}", tid, pid, gw_str);

        close(SOCK);

        // Now ping the gateway
        for (int i = 0; i < 100; i++) {
            ping(gw_str);
        }
        return true;
    }

    close(SOCK);
    return false;
}

static auto print_wki_target_state() -> int {
    char hostname[64] = {};
    uint32_t flags = 0;
    int64_t result = ker::process::getwkitarget(hostname, sizeof(hostname), &flags);
    if (result < 0) {
        std::println("wki-target: get failed: {}", result);
        return 1;
    }

    if (result == 0) {
        std::println("wki-target: <unset> flags=0x{:x}", flags);
        return 0;
    }

    std::println("wki-target: host='{}' flags=0x{:x}", hostname, flags);
    return 0;
}

static auto parse_target_flags(const char* text, uint32_t* flags_out) -> bool {
    if (text == nullptr || flags_out == nullptr) {
        return false;
    }
    if (std::strcmp(text, "strict") == 0) {
        *flags_out = ker::process::WKI_TARGET_FLAG_STRICT;
        return true;
    }
    if (std::strcmp(text, "fallback") == 0 || std::strcmp(text, "best-effort") == 0 || std::strcmp(text, "none") == 0) {
        *flags_out = 0;
        return true;
    }
    return false;
}

static auto handle_wki_target_command(int argc, char** argv) -> int {
    if (argc < 3) {
        std::println("usage: testprog --wki-target <show|clear|set>");
        return 1;
    }

    const char* action = argv[2];
    if (std::strcmp(action, "show") == 0) {
        return print_wki_target_state();
    }

    if (std::strcmp(action, "clear") == 0) {
        int64_t result = ker::process::setwkitarget(nullptr, 0, 0);
        if (result < 0) {
            std::println("wki-target: clear failed: {}", result);
            return 1;
        }
        return print_wki_target_state();
    }

    if (std::strcmp(action, "set") == 0) {
        if (argc < 4) {
            std::println("usage: testprog --wki-target set <hostname> [strict|fallback]");
            return 1;
        }

        uint32_t flags = 0;
        if (argc >= 5 && !parse_target_flags(argv[4], &flags)) {
            std::println("wki-target: invalid mode '{}', expected strict or fallback", argv[4]);
            return 1;
        }

        const char* hostname = argv[3];
        int64_t result = ker::process::setwkitarget(hostname, std::strlen(hostname), flags);
        if (result < 0) {
            std::println("wki-target: set failed: {}", result);
            return 1;
        }
        return print_wki_target_state();
    }

    std::println("wki-target: unknown action '{}'", action);
    return 1;
}

static auto parse_vfs_route(const char* text, uint32_t* route_out) -> bool {
    if (text == nullptr || route_out == nullptr) {
        return false;
    }
    if (std::strcmp(text, "local") == 0) {
        *route_out = ker::abi::vfs::WKI_VFS_ROUTE_LOCAL;
        return true;
    }
    if (std::strcmp(text, "host") == 0) {
        *route_out = ker::abi::vfs::WKI_VFS_ROUTE_HOST;
        return true;
    }
    return false;
}

static auto print_vfs_route(uint32_t route) -> const char* {
    switch (route) {
        case ker::abi::vfs::WKI_VFS_ROUTE_LOCAL:
            return "local";
        case ker::abi::vfs::WKI_VFS_ROUTE_HOST:
            return "host";
        default:
            return "unknown";
    }
}

static auto list_wki_vfs_rules() -> int {
    char prefix[128] = {};
    uint32_t route = 0;
    bool any = false;
    for (uint32_t index = 0;; ++index) {
        int rc = ker::abi::vfs::wki_rule_get_vfs(index, prefix, sizeof(prefix), &route);
        if (rc == -2) {
            break;
        }
        if (rc < 0) {
            std::println("wki-vfs: get rule {} failed: {}", index, rc);
            return 1;
        }
        std::println("wki-vfs[{}]: {} -> {}", index, prefix, print_vfs_route(route));
        any = true;
    }

    if (!any) {
        std::println("wki-vfs: no explicit task-local rules");
    }
    return 0;
}

static auto probe_wki_path(const char* path) -> int {
    char cwd_before[512] = {};
    char cwd_after[512] = {};
    if (getcwd(cwd_before, sizeof(cwd_before)) == nullptr) {
        std::strcpy(cwd_before, "<getcwd failed>");
    }

    struct stat st{};
    errno = 0;
    int stat_rc = stat(path, &st);
    std::println("wki-vfs-probe: path='{}' stat={} errno={}", path, stat_rc, errno);
    if (stat_rc == 0) {
        std::println("wki-vfs-probe: mode=0{:o} size={} ino={}", st.st_mode, static_cast<long long>(st.st_size),
                     static_cast<long long>(st.st_ino));
    }

    errno = 0;
    int access_rc = access(path, F_OK);
    std::println("wki-vfs-probe: access(F_OK)={} errno={}", access_rc, errno);

    errno = 0;
    ssize_t link_len = readlink(path, cwd_after, sizeof(cwd_after) - 1);
    if (link_len >= 0) {
        cwd_after[link_len] = '\0';
        std::println("wki-vfs-probe: readlink='{}'", cwd_after);
    } else {
        std::println("wki-vfs-probe: readlink={} errno={}", static_cast<int>(link_len), errno);
    }

    errno = 0;
    if (chdir(path) == 0) {
        if (getcwd(cwd_after, sizeof(cwd_after)) == nullptr) {
            std::strcpy(cwd_after, "<getcwd failed>");
        }
        std::println("wki-vfs-probe: chdir succeeded cwd='{}'", cwd_after);
        chdir(cwd_before);
    } else {
        std::println("wki-vfs-probe: chdir failed errno={}", errno);
    }

    std::println("wki-vfs-probe: cwd-before='{}'", cwd_before);
    return 0;
}

static auto handle_wki_vfs_command(int argc, char** argv) -> int {
    if (argc < 3) {
        std::println("usage: testprog --wki-vfs <list|clear|add|probe>");
        return 1;
    }

    const char* action = argv[2];
    if (std::strcmp(action, "list") == 0) {
        return list_wki_vfs_rules();
    }

    if (std::strcmp(action, "clear") == 0) {
        int rc = ker::abi::vfs::wki_rule_clear_vfs();
        if (rc < 0) {
            std::println("wki-vfs: clear failed: {}", rc);
            return 1;
        }
        return list_wki_vfs_rules();
    }

    if (std::strcmp(action, "add") == 0) {
        if (argc < 5) {
            std::println("usage: testprog --wki-vfs add <prefix> <local|host>");
            return 1;
        }

        uint32_t route = 0;
        if (!parse_vfs_route(argv[4], &route)) {
            std::println("wki-vfs: invalid route '{}'", argv[4]);
            return 1;
        }

        int rc = ker::abi::vfs::wki_rule_add_vfs(argv[3], route);
        if (rc < 0) {
            std::println("wki-vfs: add failed: {}", rc);
            return 1;
        }
        return list_wki_vfs_rules();
    }

    if (std::strcmp(action, "probe") == 0) {
        if (argc < 4) {
            std::println("usage: testprog --wki-vfs probe <path>");
            return 1;
        }
        return probe_wki_path(argv[3]);
    }

    std::println("wki-vfs: unknown action '{}'", action);
    return 1;
}

auto main(int argc, char** argv, char** envp) -> int {
    int pid = ker::process::getpid();
    (void)envp;
    int tid = ker::multiproc::currentThreadId();

    char launcher[64] = {};
    char runner[64] = {};
    ker::process::wki_launcher_node(launcher, sizeof(launcher));
    ker::process::wki_runner_node(runner, sizeof(runner));

    const char* command = nullptr;
    if (argc > 1) {
        command = argv[1];
        if (command[0] == '-' && command[1] == '-') {
            command += 2;
        }
    }

    if (command != nullptr && (std::strcmp(command, "vfsbench-read") == 0 || std::strcmp(command, "vfsbench-stat") == 0)) {
        return run_fsbench(argc - 1, argv + 1);
    }

    if (command != nullptr && (std::strcmp(command, "netbench-server") == 0 || std::strcmp(command, "netbench-client") == 0)) {
        return run_netbench(argc - 1, argv + 1);
    }

    if (command != nullptr && std::strcmp(command, "wki-target") == 0) {
        return handle_wki_target_command(argc, argv);
    }

    if (command != nullptr && std::strcmp(command, "wki-vfs") == 0) {
        return handle_wki_vfs_command(argc, argv);
    }

    if (command != nullptr && std::strcmp(command, "perf") == 0) {
        return run_perf(argc - 2, argv + 2);
    }

    if (command != nullptr && std::strcmp(command, "mandelbench") == 0) {
        int width = WIDTH;
        int height = HEIGHT;
        int max_iter = MAX_ITERATION;
        int threads = THREADS;
        int repeat = REPEAT;

        for (int i = 2; i < argc; i++) {
            if (std::strcmp(argv[i], "--width") == 0 && i + 1 < argc) {
                width = std::atoi(argv[++i]);
            } else if (std::strcmp(argv[i], "--height") == 0 && i + 1 < argc) {
                height = std::atoi(argv[++i]);
            } else if (std::strcmp(argv[i], "--max-iter") == 0 && i + 1 < argc) {
                max_iter = std::atoi(argv[++i]);
            } else if (std::strcmp(argv[i], "--threads") == 0 && i + 1 < argc) {
                threads = std::atoi(argv[++i]);
            } else if (std::strcmp(argv[i], "--repeat") == 0 && i + 1 < argc) {
                repeat = std::atoi(argv[++i]);
            }
        }
        if (MANDELBENCH_DEBUG_ENABLED) {
            std::println("testprog[t:{},p:{}]: Running mandelbench with width={}, height={}, max_iter={}, threads={}, repeat={}", tid, pid,
                         width, height, max_iter, threads, repeat);
        }

        return mandelbench_wki(width, height, max_iter, threads, repeat, nullptr);
    }

    if (command != nullptr && std::strcmp(command, "mandelbench-worker") == 0) {
        return mandelbench_worker(argc, argv);
    }

    std::println("testprog[t:{},p:{},launcher:{},runner:{}]: argc = {}", tid, pid, launcher, runner, argc);

    // Test 1: Ping loopback
    // std::println("testprog[t:{},p:{}]: === Test 1: Ping loopback ===", tid, pid);
    ping("127.0.0.1");

    // // Test 2: Get eth0 info and ping gateway
    // std::println("testprog[t:{},p:{}]: === Test 2: Get eth0 info ===", tid, pid);
    (void)get_interface_info;

    std::println("testprog[t:{},p:{},launcher:{},runner:{}]: Network tests complete", tid, pid, launcher, runner);

    return 0;
}
