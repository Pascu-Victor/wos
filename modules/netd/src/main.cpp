#define _DEFAULT_SOURCE 1

#include <arpa/inet.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sched.h>
#include <sys/ioctl.h>
#include <sys/net.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <print>

namespace {

// ---- DHCP constants ----

constexpr uint8_t DHCP_OP_REQUEST = 1;
constexpr uint8_t DHCP_OP_REPLY = 2;
constexpr uint8_t DHCP_HTYPE_ETHER = 1;
constexpr uint8_t DHCP_HLEN_ETHER = 6;

// Message types (option 53)
constexpr uint8_t DHCPDISCOVER = 1;
constexpr uint8_t DHCPOFFER = 2;
constexpr uint8_t DHCPREQUEST = 3;
constexpr uint8_t DHCPACK = 5;
constexpr uint8_t DHCPNAK = 6;

// Option codes
constexpr uint8_t OPT_SUBNET_MASK = 1;
constexpr uint8_t OPT_ROUTER = 3;
constexpr uint8_t OPT_DNS = 6;
constexpr uint8_t OPT_REQUESTED_IP = 50;
constexpr uint8_t OPT_LEASE_TIME = 51;
constexpr uint8_t OPT_MSG_TYPE = 53;
constexpr uint8_t OPT_SERVER_ID = 54;
constexpr uint8_t OPT_PARAM_LIST = 55;
constexpr uint8_t OPT_END = 255;

constexpr uint8_t MAGIC_COOKIE[4] = {99, 130, 83, 99};

constexpr int RECV_TIMEOUT_SECS = 5;  // real-time timeout for DHCP responses
constexpr int MAX_DISCOVER_RETRIES = 5;
constexpr int MAX_REQUEST_RETRIES = 3;
constexpr int MAX_NAK_RESTARTS = 3;  // restart from DISCOVER on NAK

// ---- BOOTP/DHCP packet ----

struct DhcpPacket {
    uint8_t op;
    uint8_t htype;
    uint8_t hlen;
    uint8_t hops;
    uint32_t xid;
    uint16_t secs;
    uint16_t flags;
    uint32_t ciaddr;
    uint32_t yiaddr;
    uint32_t siaddr;
    uint32_t giaddr;
    uint8_t chaddr[16];
    uint8_t sname[64];
    uint8_t file[128];
    uint8_t options[312];
};

struct DhcpLease {
    uint32_t your_ip;      // host order
    uint32_t server_ip;    // host order
    uint32_t subnet_mask;  // host order
    uint32_t router;       // host order
    uint32_t dns;          // host order
    uint32_t lease_time;   // seconds
};

// ---- Helpers ----

auto get_mac(int sock, const char* ifname, std::array<uint8_t, 6>& mac) -> bool {
    struct ifreq ifr{};
    std::strncpy(ifr.ifr_name, ifname, IFNAMSIZ - 1);
    if (ioctl(sock, SIOCGIFHWADDR, &ifr) != 0) {
        return false;
    }
    std::memcpy(mac.data(), ifr.ifr_hwaddr.sa_data, 6);
    return true;
}

void ip_to_str(uint32_t ip_host, char* buf, size_t len) {
    struct in_addr a{};
    a.s_addr = htonl(ip_host);
    inet_ntop(AF_INET, &a, buf, static_cast<socklen_t>(len));
}

auto build_discover(DhcpPacket* pkt, const uint8_t* mac, uint32_t xid) -> size_t {
    std::memset(pkt, 0, sizeof(DhcpPacket));
    pkt->op = DHCP_OP_REQUEST;
    pkt->htype = DHCP_HTYPE_ETHER;
    pkt->hlen = DHCP_HLEN_ETHER;
    pkt->xid = htonl(xid);
    pkt->flags = htons(0x8000);  // broadcast flag
    std::memcpy(pkt->chaddr, mac, 6);

    uint8_t* opt = pkt->options;
    std::memcpy(opt, MAGIC_COOKIE, 4);
    opt += 4;
    // Message type = DISCOVER
    *opt++ = OPT_MSG_TYPE;
    *opt++ = 1;
    *opt++ = DHCPDISCOVER;
    // Parameter request list
    *opt++ = OPT_PARAM_LIST;
    *opt++ = 4;
    *opt++ = OPT_SUBNET_MASK;
    *opt++ = OPT_ROUTER;
    *opt++ = OPT_DNS;
    *opt++ = OPT_LEASE_TIME;
    // End
    *opt++ = OPT_END;

    // RFC 2131: DHCP messages MUST be at least 300 bytes (padding is already zeroed)
    size_t used = sizeof(DhcpPacket) - sizeof(pkt->options) + static_cast<size_t>(opt - pkt->options);
    return used < 300 ? 300 : used;
}

auto build_request(DhcpPacket* pkt, const uint8_t* mac, uint32_t xid, uint32_t requested_ip_net, uint32_t server_ip_net) -> size_t {
    std::memset(pkt, 0, sizeof(DhcpPacket));
    pkt->op = DHCP_OP_REQUEST;
    pkt->htype = DHCP_HTYPE_ETHER;
    pkt->hlen = DHCP_HLEN_ETHER;
    pkt->xid = htonl(xid);
    pkt->flags = htons(0x8000);
    std::memcpy(pkt->chaddr, mac, 6);

    uint8_t* opt = pkt->options;
    std::memcpy(opt, MAGIC_COOKIE, 4);
    opt += 4;
    // Message type = REQUEST
    *opt++ = OPT_MSG_TYPE;
    *opt++ = 1;
    *opt++ = DHCPREQUEST;
    // Requested IP
    *opt++ = OPT_REQUESTED_IP;
    *opt++ = 4;
    std::memcpy(opt, &requested_ip_net, 4);
    opt += 4;
    // Server identifier
    *opt++ = OPT_SERVER_ID;
    *opt++ = 4;
    std::memcpy(opt, &server_ip_net, 4);
    opt += 4;
    // Parameter request list
    *opt++ = OPT_PARAM_LIST;
    *opt++ = 4;
    *opt++ = OPT_SUBNET_MASK;
    *opt++ = OPT_ROUTER;
    *opt++ = OPT_DNS;
    *opt++ = OPT_LEASE_TIME;
    // End
    *opt++ = OPT_END;

    size_t used = sizeof(DhcpPacket) - sizeof(pkt->options) + static_cast<size_t>(opt - pkt->options);
    return used < 300 ? 300 : used;
}

auto build_renewal(DhcpPacket* pkt, const uint8_t* mac, uint32_t xid, uint32_t client_ip_host) -> size_t {
    std::memset(pkt, 0, sizeof(DhcpPacket));
    pkt->op = DHCP_OP_REQUEST;
    pkt->htype = DHCP_HTYPE_ETHER;
    pkt->hlen = DHCP_HLEN_ETHER;
    pkt->xid = htonl(xid);
    pkt->ciaddr = htonl(client_ip_host);  // We have an address now
    std::memcpy(pkt->chaddr, mac, 6);

    uint8_t* opt = pkt->options;
    std::memcpy(opt, MAGIC_COOKIE, 4);
    opt += 4;
    *opt++ = OPT_MSG_TYPE;
    *opt++ = 1;
    *opt++ = DHCPREQUEST;
    *opt++ = OPT_END;

    size_t used = sizeof(DhcpPacket) - sizeof(pkt->options) + static_cast<size_t>(opt - pkt->options);
    return used < 300 ? 300 : used;
}

auto parse_reply(const uint8_t* data, size_t len, uint32_t expected_xid, DhcpLease* lease) -> uint8_t {
    if (len < 240) {
        return 0;  // 236 fixed + 4 magic cookie minimum
    }

    auto* pkt = reinterpret_cast<const DhcpPacket*>(data);
    if (pkt->op != DHCP_OP_REPLY) {
        return 0;
    }
    if (ntohl(pkt->xid) != expected_xid) {
        return 0;
    }

    lease->your_ip = ntohl(pkt->yiaddr);
    lease->server_ip = ntohl(pkt->siaddr);

    // Verify magic cookie
    if (std::memcmp(pkt->options, MAGIC_COOKIE, 4) != 0) {
        return 0;
    }

    // Parse TLV options
    uint8_t msg_type = 0;
    const uint8_t* opt = pkt->options + 4;
    const uint8_t* end = data + len;

    while (opt < end && *opt != OPT_END) {
        if (*opt == 0) {
            opt++;  // pad
            continue;
        }
        uint8_t code = *opt++;
        if (opt >= end) {
            break;
        }
        uint8_t olen = *opt++;
        if (opt + olen > end) {
            break;
        }

        switch (code) {
            case OPT_MSG_TYPE:
                if (olen >= 1) msg_type = opt[0];
                break;
            case OPT_SUBNET_MASK:
                if (olen >= 4) lease->subnet_mask = ntohl(*reinterpret_cast<const uint32_t*>(opt));
                break;
            case OPT_ROUTER:
                if (olen >= 4) lease->router = ntohl(*reinterpret_cast<const uint32_t*>(opt));
                break;
            case OPT_DNS:
                if (olen >= 4) lease->dns = ntohl(*reinterpret_cast<const uint32_t*>(opt));
                break;
            case OPT_SERVER_ID:
                if (olen >= 4) lease->server_ip = ntohl(*reinterpret_cast<const uint32_t*>(opt));
                break;
            case OPT_LEASE_TIME:
                if (olen >= 4) lease->lease_time = ntohl(*reinterpret_cast<const uint32_t*>(opt));
                break;
            default:
                break;
        }
        opt += olen;
    }

    return msg_type;
}

void apply_lease(const char* ifname, const DhcpLease& lease) {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        return;
    }

    // Set IP address
    {
        struct ifreq ifr{};
        std::strncpy(ifr.ifr_name, ifname, IFNAMSIZ - 1);
        auto* addr = reinterpret_cast<struct sockaddr_in*>(&ifr.ifr_addr);
        addr->sin_family = AF_INET;
        addr->sin_addr.s_addr = htonl(lease.your_ip);
        ioctl(sock, SIOCSIFADDR, &ifr);
    }

    // Set netmask
    if (lease.subnet_mask != 0) {
        struct ifreq ifr{};
        std::strncpy(ifr.ifr_name, ifname, IFNAMSIZ - 1);
        auto* addr = reinterpret_cast<struct sockaddr_in*>(&ifr.ifr_netmask);
        addr->sin_family = AF_INET;
        addr->sin_addr.s_addr = htonl(lease.subnet_mask);
        ioctl(sock, SIOCSIFNETMASK, &ifr);
    }

    // Add default route via router
    if (lease.router != 0) {
        // rtentry layout: offsets 12=dst(sin_addr), 28=gateway(sin_addr), 44=genmask(sin_addr)
        uint8_t rt[64]{};
        // dst = 0.0.0.0 (default route) -- already zeroed
        // gateway at offset 28
        *reinterpret_cast<uint32_t*>(rt + 28) = htonl(lease.router);
        // genmask = 0.0.0.0 -- already zeroed
        ioctl(sock, SIOCADDRT, rt);
    }

    close(sock);
}

auto recv_with_timeout(int sock, uint8_t* buf, size_t len, int timeout_secs) -> ssize_t {
    struct timespec start{};
    clock_gettime(CLOCK_MONOTONIC, &start);
    bool logged_err = false;

    for (;;) {
        ssize_t n = ker::abi::net::recvfrom(sock, buf, len, 0, nullptr);
        if (n > 0) {
            return n;  // got data
        }
        // Log unexpected errors once (not EAGAIN -11)
        if (n < 0 && n != -11 && !logged_err) {
            std::println("netd: recvfrom returned {}, retrying...", n);
            logged_err = true;
        }

        struct timespec now{};
        clock_gettime(CLOCK_MONOTONIC, &now);
        long elapsed = now.tv_sec - start.tv_sec;
        if (elapsed >= timeout_secs) {
            return -1;  // timeout
        }

        sched_yield();
    }
}

}  // anonymous namespace

auto main(int argc, char** argv) -> int {
    (void)argc;
    (void)argv;

    const char* ifname = "eth0";
    std::println("netd: starting DHCP client for {}", ifname);

    // Create UDP socket for DHCP
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        std::println("netd: failed to create socket: {}", sock);
        return 1;
    }

    // Bind to 0.0.0.0:68 (DHCP client port)
    struct sockaddr_in bind_addr{};
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_port = htons(68);
    bind_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(sock, reinterpret_cast<struct sockaddr*>(&bind_addr), sizeof(bind_addr)) < 0) {
        std::println("netd: failed to bind to port 68");
        close(sock);
        return 1;
    }

    // Get our MAC address
    std::array<uint8_t, 6> mac{};
    {
        int tmp = socket(AF_INET, SOCK_DGRAM, 0);
        if (tmp >= 0) {
            get_mac(tmp, ifname, mac);
            close(tmp);
        }
    }
    std::println("netd: MAC = {:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}", mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);

    // DHCP destination: 255.255.255.255:67
    struct sockaddr_in dst_addr{};
    dst_addr.sin_family = AF_INET;
    dst_addr.sin_port = htons(67);
    dst_addr.sin_addr.s_addr = htonl(0xFFFFFFFF);

    // Derive xid from MAC address to ensure uniqueness across VMs
    uint32_t xid = (static_cast<uint32_t>(mac[2]) << 24) | (static_cast<uint32_t>(mac[3]) << 16) |
                   (static_cast<uint32_t>(mac[4]) << 8) | static_cast<uint32_t>(mac[5]);
    DhcpPacket pkt{};
    std::array<uint8_t, 1500> recv_buf{};
    DhcpLease lease{};
    int nak_restarts = 0;

nak_restart:
    // === DHCP DISCOVER ===
    bool got_offer = false;
    lease = {};  // reset lease on restart
    for (int attempt = 0; attempt < MAX_DISCOVER_RETRIES && !got_offer; attempt++) {
        std::println("netd: sending DISCOVER (attempt {}/{})", attempt + 1, MAX_DISCOVER_RETRIES);
        size_t pkt_len = build_discover(&pkt, mac.data(), xid);
        sendto(sock, &pkt, pkt_len, 0, reinterpret_cast<struct sockaddr*>(&dst_addr), sizeof(dst_addr));

        // Keep receiving until timeout, draining stale packets with wrong xid
        while (!got_offer) {
            ssize_t n = recv_with_timeout(sock, recv_buf.data(), recv_buf.size(), RECV_TIMEOUT_SECS);
            if (n <= 0) {
                break;  // timeout, try next attempt
            }
            uint8_t msg = parse_reply(recv_buf.data(), static_cast<size_t>(n), xid, &lease);
            if (msg == DHCPOFFER) {
                got_offer = true;
                char ip_str[16], mask_str[16], gw_str[16];
                ip_to_str(lease.your_ip, ip_str, sizeof(ip_str));
                ip_to_str(lease.subnet_mask, mask_str, sizeof(mask_str));
                ip_to_str(lease.router, gw_str, sizeof(gw_str));
                std::println("netd: received OFFER: ip={} mask={} gw={}", ip_str, mask_str, gw_str);
            }
            // msg == 0 or other types: wrong xid or irrelevant, keep trying until timeout
        }
    }

    if (!got_offer) {
        std::println("netd: no DHCP offer received, exiting");
        close(sock);
        return 1;
    }

    // === DHCP REQUEST ===
    bool got_ack = false;
    for (int attempt = 0; attempt < MAX_REQUEST_RETRIES && !got_ack; attempt++) {
        std::println("netd: sending REQUEST (attempt {}/{})", attempt + 1, MAX_REQUEST_RETRIES);
        size_t pkt_len = build_request(&pkt, mac.data(), xid, htonl(lease.your_ip), htonl(lease.server_ip));
        sendto(sock, &pkt, pkt_len, 0, reinterpret_cast<struct sockaddr*>(&dst_addr), sizeof(dst_addr));

        // Keep receiving until timeout, draining stale packets with wrong xid
        while (!got_ack) {
            ssize_t n = recv_with_timeout(sock, recv_buf.data(), recv_buf.size(), RECV_TIMEOUT_SECS);
            if (n <= 0) {
                break;  // timeout, try next attempt
            }
            DhcpLease ack_lease{};
            uint8_t msg = parse_reply(recv_buf.data(), static_cast<size_t>(n), xid, &ack_lease);
            if (msg == DHCPACK) {
                // ACK may refine lease parameters
                if (ack_lease.subnet_mask != 0) lease.subnet_mask = ack_lease.subnet_mask;
                if (ack_lease.router != 0) lease.router = ack_lease.router;
                if (ack_lease.dns != 0) lease.dns = ack_lease.dns;
                if (ack_lease.lease_time != 0) lease.lease_time = ack_lease.lease_time;
                if (ack_lease.your_ip != 0) lease.your_ip = ack_lease.your_ip;
                got_ack = true;
            } else if (msg == DHCPNAK) {
                nak_restarts++;
                if (nak_restarts < MAX_NAK_RESTARTS) {
                    std::println("netd: received NAK, restarting from DISCOVER (attempt {}/{})", nak_restarts, MAX_NAK_RESTARTS);
                    ++xid;  // use new transaction ID on restart
                    goto nak_restart;
                }
                std::println("netd: received NAK, max restarts exceeded");
                goto request_failed;
            }
            // msg == 0 means wrong xid or invalid packet, keep trying until timeout
        }
    }
request_failed:

    if (!got_ack) {
        std::println("netd: DHCP failed - no ACK received");
        close(sock);
        return 1;
    }

    // === APPLY CONFIGURATION ===
    std::println("netd: DHCP ACK received, applying configuration");
    apply_lease(ifname, lease);

    {
        char ip_str[16], mask_str[16], gw_str[16], dns_str[16];
        ip_to_str(lease.your_ip, ip_str, sizeof(ip_str));
        ip_to_str(lease.subnet_mask, mask_str, sizeof(mask_str));
        ip_to_str(lease.router, gw_str, sizeof(gw_str));
        ip_to_str(lease.dns, dns_str, sizeof(dns_str));
        std::println("netd: {} configured: ip={} mask={} gw={} dns={} lease={}s", ifname, ip_str, mask_str, gw_str, dns_str,
                     lease.lease_time);
    }

    // === LEASE RENEWAL LOOP ===
    // T1 = lease_time / 2 (standard renewal time)

    if (lease.lease_time == 0) {
        std::println("netd: infinite lease, sleeping forever");
        for (;;) {
            sched_yield();
        }
    }

    uint32_t t1_seconds = lease.lease_time / 2;
    std::println("netd: will renew in ~{} seconds (T1)", t1_seconds);

    for (;;) {
        // Sleep until T1 using real clock
        struct timespec sleep_start{};
        clock_gettime(CLOCK_MONOTONIC, &sleep_start);
        while (true) {
            sched_yield();
            struct timespec now{};
            clock_gettime(CLOCK_MONOTONIC, &now);
            if (static_cast<uint32_t>(now.tv_sec - sleep_start.tv_sec) >= t1_seconds) {
                break;
            }
        }

        std::println("netd: T1 reached, sending renewal REQUEST");
        ++xid;

        size_t pkt_len = build_renewal(&pkt, mac.data(), xid, lease.your_ip);

        // Renewal: unicast to DHCP server
        struct sockaddr_in server{};
        server.sin_family = AF_INET;
        server.sin_port = htons(67);
        server.sin_addr.s_addr = htonl(lease.server_ip);

        sendto(sock, &pkt, pkt_len, 0, reinterpret_cast<struct sockaddr*>(&server), sizeof(server));

        ssize_t n = recv_with_timeout(sock, recv_buf.data(), recv_buf.size(), RECV_TIMEOUT_SECS);
        if (n > 0) {
            DhcpLease renew_lease{};
            uint8_t msg = parse_reply(recv_buf.data(), static_cast<size_t>(n), xid, &renew_lease);
            if (msg == DHCPACK) {
                if (renew_lease.lease_time != 0) {
                    lease.lease_time = renew_lease.lease_time;
                }
                t1_seconds = lease.lease_time / 2;
                std::println("netd: lease renewed, next renewal in ~{}s", t1_seconds);
            } else {
                std::println("netd: renewal failed (msg={}), will retry", msg);
            }
        } else {
            std::println("netd: renewal timeout, will retry");
        }
    }

    close(sock);
    return 0;
}
