#define _DEFAULT_SOURCE 1
#include <abi/callnums/sys_log.h>
#include <arpa/inet.h>
#include <dirent.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sched.h>
#include <sys/ioctl.h>
#include <sys/logging.h>
#include <sys/mman.h>
#include <sys/multiproc.h>
#include <sys/process.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/vfs.h>
#include <unistd.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <print>

// ICMP Echo Request structure
struct icmp_header {
    uint8_t type;
    uint8_t code;
    uint16_t checksum;
    uint16_t id;
    uint16_t sequence;
};

// Calculate checksum for ICMP
auto icmp_checksum(void* data, size_t len) -> uint16_t {
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
auto ping(const char* ip_str) -> bool {
    int pid = ker::process::getpid();
    int tid = ker::multiproc::currentThreadId();

    // std::println("testprog[t:{},p:{}]: Pinging {}...", tid, pid, ip_str);

    // Create raw socket for ICMP
    int sock = socket(AF_INET, SOCK_RAW, 1);  // 1 = IPPROTO_ICMP
    if (sock < 0) {
        std::println("testprog[t:{},p:{}]: Failed to create socket: {}", tid, pid, sock);
        return false;
    }

    // Set up destination address
    struct sockaddr_in dest_addr{};
    dest_addr.sin_family = AF_INET;
    inet_pton(AF_INET, ip_str, &dest_addr.sin_addr);

    // Prepare ICMP packet
    constexpr size_t packet_size = sizeof(icmp_header) + 32;  // 32 bytes of data
    std::array<uint8_t, packet_size> packet{};
    auto* icmp = reinterpret_cast<icmp_header*>(packet.data());

    icmp->type = 8;  // Echo Request
    icmp->code = 0;
    icmp->id = htons(static_cast<uint16_t>(pid));
    icmp->sequence = htons(1);
    icmp->checksum = 0;

    // Fill data with pattern
    for (size_t i = sizeof(icmp_header); i < packet_size; i++) {
        packet[i] = static_cast<uint8_t>(i);
    }

    // Calculate checksum
    icmp->checksum = icmp_checksum(packet.data(), packet_size);

    // Send packet
    ssize_t sent = sendto(sock, packet.data(), packet_size, 0, reinterpret_cast<struct sockaddr*>(&dest_addr), sizeof(dest_addr));

    if (sent < 0) {
        std::println("testprog[t:{},p:{}]: Failed to send ping: {}", tid, pid, sent);
        close(sock);
        return false;
    }

    // std::println("testprog[t:{},p:{}]: Sent {} bytes to {}", tid, pid, sent, ip_str);

    // Try to receive response (with polling retry for EAGAIN)
    std::array<uint8_t, 1024> recv_buf{};
    struct sockaddr_in from_addr{};
    socklen_t from_len = sizeof(from_addr);

    ssize_t received = -1;
    constexpr int max_retries = 4;
    for (int retry = 0; retry < max_retries; ++retry) {
        received = recvfrom(sock, recv_buf.data(), recv_buf.size(), 0, reinterpret_cast<struct sockaddr*>(&from_addr), &from_len);
        if (received != -11) {  // -11 is EAGAIN
            break;
        }
    }

    close(sock);

    if (received > 0) {
        return true;
    }
    std::println("testprog[t:{},p:{}]: No response from {} (received={})", tid, pid, ip_str, received);
    return false;
}

// Get network interface information
auto get_interface_info(const char* ifname) -> bool {
    int pid = ker::process::getpid();
    int tid = ker::multiproc::currentThreadId();

    // std::println("testprog[t:{},p:{}]: Getting info for interface {}...", tid, pid, ifname);

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        std::println("testprog[t:{},p:{}]: Failed to create socket for ioctl", tid, pid);
        return false;
    }

    // Get interface address
    struct ifreq ifr{};
    strncpy(ifr.ifr_name, ifname, IFNAMSIZ - 1);

    uint32_t ip_addr = 0;
    if (ioctl(sock, SIOCGIFADDR, &ifr) == 0) {
        auto* addr = reinterpret_cast<struct sockaddr_in*>(&ifr.ifr_addr);
        ip_addr = ntohl(addr->sin_addr.s_addr);
        char ip_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &addr->sin_addr, ip_str, sizeof(ip_str));
        // std::println("testprog[t:{},p:{}]:   IP address: {}", tid, pid, ip_str);
    } else {
        std::println("testprog[t:{},p:{}]:   Failed to get IP address", tid, pid);
        close(sock);
        return false;
    }

    // Get netmask and calculate gateway
    strncpy(ifr.ifr_name, ifname, IFNAMSIZ - 1);
    if (ioctl(sock, SIOCGIFNETMASK, &ifr) == 0) {
        auto* addr = reinterpret_cast<struct sockaddr_in*>(&ifr.ifr_netmask);
        char mask_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &addr->sin_addr, mask_str, sizeof(mask_str));
        // std::println("testprog[t:{},p:{}]:   Netmask: {}", tid, pid, mask_str);

        // Calculate gateway (QEMU user-mode uses .2 as gateway, not .1)
        uint32_t mask = ntohl(addr->sin_addr.s_addr);
        uint32_t gateway = (ip_addr & mask) | 0x00000001;  // Set last byte to .2 for QEMU

        struct in_addr gw_addr{};
        gw_addr.s_addr = htonl(gateway);
        char gw_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &gw_addr, gw_str, sizeof(gw_str));
        // std::println("testprog[t:{},p:{}]:   Gateway (assumed): {}", tid, pid, gw_str);

        close(sock);

        // Now ping the gateway
        for (int i = 0; i < 100; i++) {
            ping(gw_str);
        }
        return true;
    }

    close(sock);
    return false;
}

auto main(int argc, char** argv, char** envp) -> int {
    int pid = ker::process::getpid();
    (void)envp;
    (void)argv;
    int tid = ker::multiproc::currentThreadId();

    std::println("testprog[t:{},p:{}]: argc = {}", tid, pid, argc);

    // Test 1: Ping loopback
    // std::println("testprog[t:{},p:{}]: === Test 1: Ping loopback ===", tid, pid);
    ping("127.0.0.1");

    // // Test 2: Get eth0 info and ping gateway
    // std::println("testprog[t:{},p:{}]: === Test 2: Get eth0 info ===", tid, pid);
    // get_interface_info("eth0");

    std::println("testprog[t:{},p:{}]: Network tests complete", tid, pid);

    return pid;
}
