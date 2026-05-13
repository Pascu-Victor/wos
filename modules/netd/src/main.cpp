#include <abi-bits/in.h>
#include <abi-bits/ioctls.h>
#include <abi-bits/socket.h>
#include <abi-bits/socklen_t.h>
#include <arpa/inet.h>
#include <bits/ssize_t.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/logging.h>
#include <sys/net.h>
#include <sys/socket.h>
#include <time.h>  // NOLINT(modernize-deprecated-headers): sysroot exposes POSIX time APIs here.
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <span>
#include <utility>

#include "abi-bits/route.h"

namespace {
using logger = wos::journal<"netd">;

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
constexpr uint8_t OPT_HOST_NAME = 12;
constexpr uint8_t OPT_DOMAIN_NAME = 15;
constexpr uint8_t OPT_FQDN = 81;
constexpr uint8_t OPT_REQUESTED_IP = 50;
constexpr uint8_t OPT_LEASE_TIME = 51;
constexpr uint8_t OPT_MSG_TYPE = 53;
constexpr uint8_t OPT_SERVER_ID = 54;
constexpr uint8_t OPT_PARAM_LIST = 55;
constexpr uint8_t OPT_DOMAIN_SEARCH = 119;
constexpr uint8_t OPT_END = 255;

constexpr std::array<uint8_t, 4> MAGIC_COOKIE = {99, 130, 83, 99};

constexpr int RECV_TIMEOUT_SECS = 5;  // real-time timeout for DHCP responses
constexpr uint32_t RECV_POLL_INTERVAL_US = 25'000;
constexpr int MAX_DISCOVER_RETRIES = 5;
constexpr int MAX_REQUEST_RETRIES = 3;
constexpr int MAX_NAK_RESTARTS = 3;  // restart from DISCOVER on NAK
constexpr uint32_t RENEWAL_FAILURE_LOG_INTERVAL = 10;
constexpr uint32_t DHCP_RENEWAL_RETRY_SECS = 60;
constexpr uint32_t USEC_PER_SEC = 1000000;

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
    std::array<uint8_t, 16> chaddr;
    std::array<uint8_t, 64> sname;
    std::array<uint8_t, 128> file;
    std::array<uint8_t, 312> options;
};
static_assert(sizeof(DhcpPacket) == 548);

struct DhcpLease {
    uint32_t your_ip;      // host order
    uint32_t server_ip;    // host order
    uint32_t subnet_mask;  // host order
    uint32_t router;       // host order
    uint32_t dns;          // host order
    uint32_t lease_time;   // seconds
    std::array<char, 256> domain_name;
    std::array<char, 256> search_domains;
};

// ---- Helpers ----

void copy_ifreq_name(struct ifreq& ifr, const char* ifname) {
    auto dest = std::span<char, IFNAMSIZ>(ifr.ifr_name);
    std::ranges::fill(dest, '\0');
    if (ifname == nullptr) {
        return;
    }

    size_t const LEN = std::min(std::strlen(ifname), dest.size() - 1);
    std::copy_n(ifname, LEN, dest.data());
}

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

auto load_network_u32(const uint8_t* data) -> uint32_t {
    uint32_t value = 0;
    std::memcpy(&value, data, sizeof(value));
    return ntohl(value);
}

void ip_to_str(uint32_t ip_host, char* buf, size_t len) {
    struct in_addr a{};
    a.s_addr = htonl(ip_host);
    inet_ntop(AF_INET, &a, buf, static_cast<socklen_t>(len));
}

void copy_dhcp_string(std::span<char> dest, const uint8_t* data, size_t len) {
    if (dest.empty()) {
        return;
    }

    std::ranges::fill(dest, '\0');
    size_t out = 0;
    for (size_t i = 0; i < len && out + 1 < dest.size(); i++) {
        unsigned char const CH = data[i];
        if (CH == '\0' || CH == '\n' || CH == '\r') {
            break;
        }
        if (!std::isprint(CH)) {
            continue;
        }
        dest[out++] = static_cast<char>(CH);
    }
    dest[out] = '\0';
}

void copy_plain_string(std::span<char> dest, const char* source) {
    if (dest.empty()) {
        return;
    }

    std::ranges::fill(dest, '\0');
    if (source == nullptr) {
        return;
    }

    size_t out = 0;
    for (size_t i = 0; source[i] != '\0' && out + 1 < dest.size(); i++) {
        unsigned char const CH = static_cast<unsigned char>(source[i]);
        if (!std::isprint(CH) || std::isspace(CH)) {
            continue;
        }
        dest[out++] = static_cast<char>(CH);
    }
    dest[out] = '\0';
}

void trim_hostname_to_label(std::span<char> hostname) {
    char* dot = std::strchr(hostname.data(), '.');
    if (dot != nullptr) {
        *dot = '\0';
    }
}

auto get_primary_search_domain(const DhcpLease& lease, std::span<char> dest) -> bool {
    if (dest.empty()) {
        return false;
    }

    std::ranges::fill(dest, '\0');
    if (lease.domain_name[0] != '\0') {
        copy_plain_string(dest, lease.domain_name.data());
        return dest[0] != '\0';
    }

    if (lease.search_domains[0] == '\0') {
        return false;
    }

    size_t out = 0;
    for (size_t i = 0; lease.search_domains[i] != '\0' && out + 1 < dest.size(); i++) {
        unsigned char const CH = static_cast<unsigned char>(lease.search_domains[i]);
        if (std::isspace(CH)) {
            break;
        }
        dest[out++] = static_cast<char>(CH);
    }
    dest[out] = '\0';
    return out != 0;
}

auto read_local_hostname(std::span<char> dest) -> bool {
    if (dest.size() < 2) {
        return false;
    }

    std::ranges::fill(dest, '\0');
    if (gethostname(dest.data(), dest.size() - 1) != 0) {
        return false;
    }
    dest.back() = '\0';
    trim_hostname_to_label(dest);
    return dest[0] != '\0';
}

auto build_client_fqdn(const char* hostname, const DhcpLease& lease, std::span<char> dest) -> bool {
    if (dest.size() < 4 || hostname == nullptr || hostname[0] == '\0') {
        return false;
    }

    std::array<char, 256> domain{};
    if (!get_primary_search_domain(lease, domain)) {
        return false;
    }

    int const WRITTEN = std::snprintf(dest.data(), dest.size(), "%s.%s", hostname, domain.data());
    if (WRITTEN <= 0 || static_cast<size_t>(WRITTEN) >= dest.size()) {
        dest[0] = '\0';
        return false;
    }
    return true;
}

auto append_client_identity_options(uint8_t* opt, const char* hostname, const char* fqdn) -> uint8_t* {
    if (hostname != nullptr && hostname[0] != '\0') {
        size_t const HOSTNAME_LEN = std::min(std::strlen(hostname), static_cast<size_t>(255));
        *opt++ = OPT_HOST_NAME;
        *opt++ = static_cast<uint8_t>(HOSTNAME_LEN);
        std::memcpy(opt, hostname, HOSTNAME_LEN);
        opt += HOSTNAME_LEN;
    }

    if (fqdn != nullptr && fqdn[0] != '\0') {
        size_t const FQDN_LEN = std::min(std::strlen(fqdn), static_cast<size_t>(252));
        *opt++ = OPT_FQDN;
        *opt++ = static_cast<uint8_t>(FQDN_LEN + 3);
        *opt++ = 0x1;  // Request the server to perform A/PTR DNS updates.
        *opt++ = 0;
        *opt++ = 0;
        std::memcpy(opt, fqdn, FQDN_LEN);
        opt += FQDN_LEN;
    }

    return opt;
}

auto decode_domain_search_option(std::span<char> dest, const uint8_t* data, size_t len) -> bool {
    if (dest.empty()) {
        return false;
    }

    std::ranges::fill(dest, '\0');
    size_t out = 0;
    bool need_dot = false;
    bool have_domain = false;
    bool domain_started = false;

    for (size_t i = 0; i < len;) {
        uint8_t const LABEL_LEN = data[i++];
        if (LABEL_LEN == 0) {
            if (domain_started) {
                have_domain = true;
                need_dot = false;
                domain_started = false;
            }
            continue;
        }

        if ((LABEL_LEN & 0xC0) != 0 || i + LABEL_LEN > len) {
            return false;
        }

        if (have_domain) {
            if (out + 1 >= dest.size()) {
                return false;
            }
            dest[out++] = ' ';
            have_domain = false;
        } else if (need_dot) {
            if (out + 1 >= dest.size()) {
                return false;
            }
            dest[out++] = '.';
        }

        for (size_t j = 0; j < LABEL_LEN; j++) {
            unsigned char const CH = data[i + j];
            if (!std::isprint(CH) || std::isspace(CH)) {
                return false;
            }
            if (out + 1 >= dest.size()) {
                return false;
            }
            dest[out++] = static_cast<char>(CH);
        }

        i += LABEL_LEN;
        need_dot = true;
        domain_started = true;
    }

    dest[out] = '\0';
    return out != 0;
}

void write_resolv_conf(const DhcpLease& lease) {
    FILE* file = fopen("/etc/resolv.conf", "w");
    if (file == nullptr) {
        logger::warn("netd: failed to update /etc/resolv.conf: %d", errno);
        return;
    }

    fputs("# Managed by netd via DHCP\n", file);
    if (lease.dns != 0) {
        std::array<char, 16> dns_str{};
        ip_to_str(lease.dns, dns_str.data(), dns_str.size());
        fprintf(file, "nameserver %s\n", dns_str.data());
    }

    if (lease.search_domains[0] != '\0') {
        fprintf(file, "search %s\n", lease.search_domains.data());
    } else if (lease.domain_name[0] != '\0') {
        fprintf(file, "domain %s\n", lease.domain_name.data());
    }

    fclose(file);
}

auto build_discover(DhcpPacket* pkt, std::span<const uint8_t, 6> mac, uint32_t xid, const char* hostname, const char* fqdn) -> size_t {
    *pkt = {};
    pkt->op = DHCP_OP_REQUEST;
    pkt->htype = DHCP_HTYPE_ETHER;
    pkt->hlen = DHCP_HLEN_ETHER;
    pkt->xid = htonl(xid);
    pkt->flags = htons(0x8000);  // broadcast flag
    std::ranges::copy(mac, pkt->chaddr.begin());

    uint8_t* opt = pkt->options.data();
    opt = std::ranges::copy(MAGIC_COOKIE, opt).out;
    // Message type = DISCOVER
    *opt++ = OPT_MSG_TYPE;
    *opt++ = 1;
    *opt++ = DHCPDISCOVER;
    opt = append_client_identity_options(opt, hostname, fqdn);
    // Parameter request list
    *opt++ = OPT_PARAM_LIST;
    *opt++ = 6;
    *opt++ = OPT_SUBNET_MASK;
    *opt++ = OPT_ROUTER;
    *opt++ = OPT_DNS;
    *opt++ = OPT_DOMAIN_NAME;
    *opt++ = OPT_DOMAIN_SEARCH;
    *opt++ = OPT_LEASE_TIME;
    // End
    *opt++ = OPT_END;

    // RFC 2131: DHCP messages MUST be at least 300 bytes (padding is already zeroed)
    size_t const USED = sizeof(DhcpPacket) - pkt->options.size() + static_cast<size_t>(opt - pkt->options.data());
    return USED < 300 ? 300 : USED;
}

auto build_request(DhcpPacket* pkt, std::span<const uint8_t, 6> mac, uint32_t xid, uint32_t requested_ip_net, uint32_t server_ip_net,
                   const char* hostname, const char* fqdn) -> size_t {
    *pkt = {};
    pkt->op = DHCP_OP_REQUEST;
    pkt->htype = DHCP_HTYPE_ETHER;
    pkt->hlen = DHCP_HLEN_ETHER;
    pkt->xid = htonl(xid);
    pkt->flags = htons(0x8000);
    std::ranges::copy(mac, pkt->chaddr.begin());

    uint8_t* opt = pkt->options.data();
    opt = std::ranges::copy(MAGIC_COOKIE, opt).out;
    // Message type = REQUEST
    *opt++ = OPT_MSG_TYPE;
    *opt++ = 1;
    *opt++ = DHCPREQUEST;
    opt = append_client_identity_options(opt, hostname, fqdn);
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
    *opt++ = 6;
    *opt++ = OPT_SUBNET_MASK;
    *opt++ = OPT_ROUTER;
    *opt++ = OPT_DNS;
    *opt++ = OPT_DOMAIN_NAME;
    *opt++ = OPT_DOMAIN_SEARCH;
    *opt++ = OPT_LEASE_TIME;
    // End
    *opt++ = OPT_END;

    size_t const USED = sizeof(DhcpPacket) - pkt->options.size() + static_cast<size_t>(opt - pkt->options.data());
    return USED < 300 ? 300 : USED;
}

auto build_renewal(DhcpPacket* pkt, std::span<const uint8_t, 6> mac, uint32_t xid, uint32_t client_ip_host, const char* hostname,
                   const char* fqdn) -> size_t {
    *pkt = {};
    pkt->op = DHCP_OP_REQUEST;
    pkt->htype = DHCP_HTYPE_ETHER;
    pkt->hlen = DHCP_HLEN_ETHER;
    pkt->xid = htonl(xid);
    pkt->ciaddr = htonl(client_ip_host);  // We have an address now
    std::ranges::copy(mac, pkt->chaddr.begin());

    uint8_t* opt = pkt->options.data();
    opt = std::ranges::copy(MAGIC_COOKIE, opt).out;
    *opt++ = OPT_MSG_TYPE;
    *opt++ = 1;
    *opt++ = DHCPREQUEST;
    opt = append_client_identity_options(opt, hostname, fqdn);
    *opt++ = OPT_END;

    size_t const USED = sizeof(DhcpPacket) - pkt->options.size() + static_cast<size_t>(opt - pkt->options.data());
    return USED < 300 ? 300 : USED;
}

auto parse_reply(const uint8_t* data, size_t len, uint32_t expected_xid, DhcpLease* lease) -> uint8_t {
    if (len < 240) {
        return 0;  // 236 fixed + 4 magic cookie minimum
    }

    const auto* pkt = reinterpret_cast<const DhcpPacket*>(data);
    if (pkt->op != DHCP_OP_REPLY) {
        return 0;
    }
    if (ntohl(pkt->xid) != expected_xid) {
        return 0;
    }

    lease->your_ip = ntohl(pkt->yiaddr);
    lease->server_ip = ntohl(pkt->siaddr);

    // Verify magic cookie
    if (std::memcmp(pkt->options.data(), MAGIC_COOKIE.data(), MAGIC_COOKIE.size()) != 0) {
        return 0;
    }

    // Parse TLV options
    uint8_t msg_type = 0;
    const uint8_t* opt = pkt->options.data() + MAGIC_COOKIE.size();
    const uint8_t* end = data + len;

    while (opt < end && *opt != OPT_END) {
        if (*opt == 0) {
            opt++;  // pad
            continue;
        }
        uint8_t const CODE = *opt++;
        if (opt >= end) {
            break;
        }
        uint8_t const OLEN = *opt++;
        if (opt + OLEN > end) {
            break;
        }

        switch (CODE) {
            case OPT_MSG_TYPE:
                if (OLEN >= 1) {
                    msg_type = *opt;
                }
                break;
            case OPT_SUBNET_MASK:
                if (OLEN >= 4) {
                    lease->subnet_mask = load_network_u32(opt);
                }
                break;
            case OPT_ROUTER:
                if (OLEN >= 4) {
                    lease->router = load_network_u32(opt);
                }
                break;
            case OPT_DNS:
                if (OLEN >= 4) {
                    lease->dns = load_network_u32(opt);
                }
                break;
            case OPT_DOMAIN_NAME:
                copy_dhcp_string(lease->domain_name, opt, OLEN);
                break;
            case OPT_DOMAIN_SEARCH:
                decode_domain_search_option(lease->search_domains, opt, OLEN);
                break;
            case OPT_SERVER_ID:
                if (OLEN >= 4) {
                    lease->server_ip = load_network_u32(opt);
                }
                break;
            case OPT_LEASE_TIME:
                if (OLEN >= 4) {
                    lease->lease_time = load_network_u32(opt);
                }
                break;
            default:
                break;
        }
        opt += OLEN;
    }

    return msg_type;
}

void apply_lease(const char* ifname, const DhcpLease& lease) {
    int const SOCK = socket(AF_INET, SOCK_DGRAM, 0);
    if (SOCK < 0) {
        return;
    }

    // Set IP address
    {
        ifreq ifr{};
        copy_ifreq_name(ifr, ifname);
        auto* addr = reinterpret_cast<struct sockaddr_in*>(&ifr.ifr_addr);
        addr->sin_family = AF_INET;
        addr->sin_addr.s_addr = htonl(lease.your_ip);
        ioctl(SOCK, SIOCSIFADDR, &ifr);
    }

    // Set netmask
    if (lease.subnet_mask != 0) {
        ifreq ifr{};
        copy_ifreq_name(ifr, ifname);
        auto* addr = reinterpret_cast<struct sockaddr_in*>(&ifr.ifr_netmask);
        addr->sin_family = AF_INET;
        addr->sin_addr.s_addr = htonl(lease.subnet_mask);
        ioctl(SOCK, SIOCSIFNETMASK, &ifr);
    }

    // Add local subnet route (direct, no gateway) so on-subnet traffic is
    // not sent through the default gateway.
    if (lease.subnet_mask != 0) {
        struct rtentry rt{};
        rt.rt_flags = RTF_UP;
        auto* dst = reinterpret_cast<struct sockaddr_in*>(&rt.rt_dst);
        dst->sin_family = AF_INET;
        dst->sin_addr.s_addr = htonl(lease.your_ip & lease.subnet_mask);
        auto* genmask = reinterpret_cast<struct sockaddr_in*>(&rt.rt_genmask);
        genmask->sin_family = AF_INET;
        genmask->sin_addr.s_addr = htonl(lease.subnet_mask);
        ioctl(SOCK, SIOCADDRT, &rt);
    }

    // Add default route via router
    if (lease.router != 0) {
        rtentry rt{};
        rt.rt_flags = RTF_UP | RTF_GATEWAY;
        auto* gateway = reinterpret_cast<struct sockaddr_in*>(&rt.rt_gateway);
        gateway->sin_family = AF_INET;
        gateway->sin_addr.s_addr = htonl(lease.router);
        ioctl(SOCK, SIOCADDRT, &rt);
    }

    close(SOCK);
    write_resolv_conf(lease);
}

auto monotonic_now_us() -> uint64_t {
    struct timespec now{};
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (static_cast<uint64_t>(now.tv_sec) * USEC_PER_SEC) + static_cast<uint64_t>(now.tv_nsec / 1000);
}

void sleep_until_us(uint64_t deadline_us) {
    for (;;) {
        uint64_t const NOW_US = monotonic_now_us();
        if (NOW_US >= deadline_us) {
            return;
        }

        uint64_t const REMAINING_US = deadline_us - NOW_US;
        struct timespec ts{};
        ts.tv_sec = static_cast<time_t>(REMAINING_US / USEC_PER_SEC);
        ts.tv_nsec = static_cast<long>((REMAINING_US % USEC_PER_SEC) * 1000);

        if (nanosleep(&ts, &ts) == -1 && errno == EINTR) {
            continue;
        }
    }
}

void sleep_for_seconds(uint32_t seconds) { sleep_until_us(monotonic_now_us() + (static_cast<uint64_t>(seconds) * USEC_PER_SEC)); }

void sleep_until_next_recv_poll(uint64_t deadline_us) {
    uint64_t const NOW_US = monotonic_now_us();
    if (NOW_US >= deadline_us) {
        return;
    }
    sleep_until_us(std::min(deadline_us, NOW_US + RECV_POLL_INTERVAL_US));
}

auto recv_with_timeout(int sock, uint8_t* buf, size_t len, int timeout_secs) -> ssize_t {
    uint64_t const DEADLINE_US = monotonic_now_us() + (static_cast<uint64_t>(timeout_secs) * USEC_PER_SEC);
    while (monotonic_now_us() < DEADLINE_US) {
        ssize_t const N = ker::abi::net::recvfrom(sock, buf, len, MSG_DONTWAIT, nullptr);
        if (N > 0) {
            return N;
        }
        if (N == -EINTR || N == -EAGAIN) {
            sleep_until_next_recv_poll(DEADLINE_US);
            continue;
        }
        if (N < 0) {
            logger::warn("netd: recvfrom returned %zd while waiting for DHCP packet", N);
        }
        sleep_until_next_recv_poll(DEADLINE_US);
    }
    return -1;
}

auto recv_dhcp_reply_until_timeout(int sock, uint8_t* buf, size_t len, uint32_t expected_xid, int timeout_secs, DhcpLease* lease)
    -> uint8_t {
    uint64_t const DEADLINE_US = monotonic_now_us() + (static_cast<uint64_t>(timeout_secs) * USEC_PER_SEC);
    while (monotonic_now_us() < DEADLINE_US) {
        ssize_t const N = ker::abi::net::recvfrom(sock, buf, len, MSG_DONTWAIT, nullptr);
        if (N == -EINTR || N == -EAGAIN) {
            sleep_until_next_recv_poll(DEADLINE_US);
            continue;
        }
        if (N < 0) {
            logger::warn("netd: recvfrom returned %zd while waiting for DHCP reply", N);
            sleep_until_next_recv_poll(DEADLINE_US);
            continue;
        }
        if (N == 0) {
            sleep_until_next_recv_poll(DEADLINE_US);
            continue;
        }

        uint8_t const MSG = parse_reply(buf, static_cast<size_t>(N), expected_xid, lease);
        if (MSG != 0) {
            return MSG;
        }
    }

    return 0;
}

// Parse /etc/netdevs and return the first ifname assigned to the given driver.
// On failure, returns the provided fallback.
auto netdevs_find_ifname(const char* driver, const char* fallback) -> const char* {
    static std::array<char, 16> s_ifname{};

    FILE* f = fopen("/etc/netdevs", "r");
    if (f == nullptr) {
        return fallback;
    }

    std::array<char, 128> line{};
    while (fgets(line.data(), static_cast<int>(line.size()), f) != nullptr) {
        // Skip comments and blank lines
        char const* p = line.data();
        while (*p == ' ' || *p == '\t') {
            p++;
        }
        if (*p == '#' || *p == '\n' || *p == '\r' || *p == '\0') {
            continue;
        }

        // Parse "<ifname> <driver>"
        std::array<char, 16> tok_ifname{};
        std::array<char, 32> tok_driver{};
        if (sscanf(p, "%15s %31s", tok_ifname.data(), tok_driver.data()) != 2) {
            continue;
        }

        if (std::strcmp(tok_driver.data(), driver) == 0) {
            fclose(f);
            std::ranges::fill(s_ifname, '\0');
            size_t const LEN = std::min(std::strlen(tok_ifname.data()), s_ifname.size() - 1);
            std::copy_n(tok_ifname.data(), LEN, s_ifname.data());
            return s_ifname.data();
        }
    }
    fclose(f);
    return fallback;
}

}  // namespace

auto main(int argc, char** argv) -> int {
    (void)argc;
    (void)argv;

    const char* ifname = netdevs_find_ifname("dhcp", "eth0");
    logger::info("netd: starting DHCP client for %s", ifname);

    // Create UDP socket for DHCP
    int const SOCK = socket(AF_INET, SOCK_DGRAM, 0);
    if (SOCK < 0) {
        logger::error("netd: failed to create socket: %d", SOCK);
        return 1;
    }

    // Bind to 0.0.0.0:68 (DHCP client port)
    struct sockaddr_in bind_addr{};
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_port = htons(68);
    bind_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(SOCK, reinterpret_cast<struct sockaddr*>(&bind_addr), sizeof(bind_addr)) < 0) {
        logger::error("netd: failed to bind to port 68");
        close(SOCK);
        return 1;
    }

    // Get our MAC address
    std::array<uint8_t, 6> mac{};
    {
        int const TMP = socket(AF_INET, SOCK_DGRAM, 0);
        if (TMP >= 0) {
            get_mac(TMP, ifname, mac);
            close(TMP);
        }
    }
    logger::info("netd: MAC = %02x:%02x:%02x:%02x:%02x:%02x", std::get<0>(mac), std::get<1>(mac), std::get<2>(mac), std::get<3>(mac),
                 std::get<4>(mac), std::get<5>(mac));

    // DHCP destination: 255.255.255.255:67
    struct sockaddr_in dst_addr{};
    dst_addr.sin_family = AF_INET;
    dst_addr.sin_port = htons(67);
    dst_addr.sin_addr.s_addr = htonl(0xFFFFFFFF);

    // Derive xid from the MAC and current boot time. vm0's MAC tail is all
    // zeroes, so a MAC-only xid becomes 0x00000000 and makes packet captures
    // harder to disambiguate across retries and reboots.
    uint64_t const XID_TIME = monotonic_now_us();
    uint32_t xid = 0x574f5300U ^ (static_cast<uint32_t>(std::get<2>(mac)) << 24) ^ (static_cast<uint32_t>(std::get<3>(mac)) << 16) ^
                   (static_cast<uint32_t>(std::get<4>(mac)) << 8) ^ static_cast<uint32_t>(std::get<5>(mac)) ^
                   static_cast<uint32_t>(XID_TIME) ^ static_cast<uint32_t>(XID_TIME >> 32);
    if (xid == 0) {
        xid = 0x574f5301U;
    }
    logger::debug("netd: DHCP transaction id 0x%x", xid);
    DhcpPacket pkt{};
    std::array<uint8_t, 1500> recv_buf{};
    DhcpLease lease{};
    int nak_restarts = 0;
    std::array<char, 256> local_hostname{};
    read_local_hostname(local_hostname);
    if (local_hostname[0] != '\0') {
        logger::info("netd: DHCP client hostname: %s", local_hostname.data());
    }

nak_restart:
    // === DHCP DISCOVER ===
    bool got_offer = false;
    lease = {};  // reset lease on restart
    for (int attempt = 0; attempt < MAX_DISCOVER_RETRIES && !got_offer; attempt++) {
        logger::debug("netd: sending DISCOVER (attempt %d/%d)", attempt + 1, MAX_DISCOVER_RETRIES);
        size_t const PKT_LEN = build_discover(&pkt, mac, xid, local_hostname.data(), nullptr);
        ssize_t const SENT = sendto(SOCK, &pkt, PKT_LEN, 0, reinterpret_cast<struct sockaddr*>(&dst_addr), sizeof(dst_addr));
        if (std::cmp_not_equal(SENT, PKT_LEN)) {
            logger::warn("netd: DISCOVER sendto returned %zd expected %zu errno=%d", SENT, PKT_LEN, errno);
        }

        // Keep receiving until timeout, draining stale packets with wrong xid
        while (!got_offer) {
            ssize_t const N = recv_with_timeout(SOCK, recv_buf.data(), recv_buf.size(), RECV_TIMEOUT_SECS);
            if (N <= 0) {
                break;  // timeout, try next attempt
            }
            uint8_t const MSG = parse_reply(recv_buf.data(), static_cast<size_t>(N), xid, &lease);
            if (MSG == DHCPOFFER) {
                got_offer = true;
                std::array<char, 16> ip_str{};
                std::array<char, 16> mask_str{};
                std::array<char, 16> gw_str{};
                ip_to_str(lease.your_ip, ip_str.data(), ip_str.size());
                ip_to_str(lease.subnet_mask, mask_str.data(), mask_str.size());
                ip_to_str(lease.router, gw_str.data(), gw_str.size());
                logger::info("netd: received OFFER: ip=%s mask=%s gw=%s", ip_str.data(), mask_str.data(), gw_str.data());
            }
            // msg == 0 or other types: wrong xid or irrelevant, keep trying until timeout
        }
    }

    if (!got_offer) {
        logger::error("netd: no DHCP offer received, exiting");
        close(SOCK);
        return 1;
    }

    // === DHCP REQUEST ===
    bool got_ack = false;
    for (int attempt = 0; attempt < MAX_REQUEST_RETRIES && !got_ack; attempt++) {
        logger::debug("netd: sending REQUEST (attempt %d/%d)", attempt + 1, MAX_REQUEST_RETRIES);
        std::array<char, 512> local_fqdn{};
        char const* fqdn = build_client_fqdn(local_hostname.data(), lease, local_fqdn) ? local_fqdn.data() : nullptr;
        size_t const PKT_LEN = build_request(&pkt, mac, xid, htonl(lease.your_ip), htonl(lease.server_ip), local_hostname.data(), fqdn);
        ssize_t const SENT = sendto(SOCK, &pkt, PKT_LEN, 0, reinterpret_cast<struct sockaddr*>(&dst_addr), sizeof(dst_addr));
        if (std::cmp_not_equal(SENT, PKT_LEN)) {
            logger::warn("netd: REQUEST sendto returned %zd expected %zu errno=%d", SENT, PKT_LEN, errno);
        }

        // Keep receiving until timeout, draining stale packets with wrong xid
        while (!got_ack) {
            ssize_t const N = recv_with_timeout(SOCK, recv_buf.data(), recv_buf.size(), RECV_TIMEOUT_SECS);
            if (N <= 0) {
                break;  // timeout, try next attempt
            }
            DhcpLease ack_lease{};
            uint8_t const MSG = parse_reply(recv_buf.data(), static_cast<size_t>(N), xid, &ack_lease);
            if (MSG == DHCPACK) {
                // ACK may refine lease parameters
                if (ack_lease.subnet_mask != 0) {
                    lease.subnet_mask = ack_lease.subnet_mask;
                }
                if (ack_lease.router != 0) {
                    lease.router = ack_lease.router;
                }
                if (ack_lease.dns != 0) {
                    lease.dns = ack_lease.dns;
                }
                if (ack_lease.lease_time != 0) {
                    lease.lease_time = ack_lease.lease_time;
                }
                if (ack_lease.your_ip != 0) {
                    lease.your_ip = ack_lease.your_ip;
                }
                if (ack_lease.domain_name[0] != '\0') {
                    lease.domain_name = ack_lease.domain_name;
                }
                if (ack_lease.search_domains[0] != '\0') {
                    lease.search_domains = ack_lease.search_domains;
                }
                got_ack = true;
            } else if (MSG == DHCPNAK) {
                nak_restarts++;
                if (nak_restarts < MAX_NAK_RESTARTS) {
                    logger::warn("netd: received NAK, restarting from DISCOVER (attempt %d/%d)", nak_restarts, MAX_NAK_RESTARTS);
                    ++xid;  // use new transaction ID on restart
                    goto nak_restart;
                }
                logger::error("netd: received NAK, max restarts exceeded");
                goto request_failed;
            }
            // msg == 0 means wrong xid or invalid packet, keep trying until timeout
        }
    }
request_failed:

    if (!got_ack) {
        logger::error("netd: DHCP failed - no ACK received");
        close(SOCK);
        return 1;
    }

    // === APPLY CONFIGURATION ===
    logger::info("netd: DHCP ACK received, applying configuration");
    apply_lease(ifname, lease);

    {
        std::array<char, 16> ip_str{};
        std::array<char, 16> mask_str{};
        std::array<char, 16> gw_str{};
        std::array<char, 16> dns_str{};
        ip_to_str(lease.your_ip, ip_str.data(), ip_str.size());
        ip_to_str(lease.subnet_mask, mask_str.data(), mask_str.size());
        ip_to_str(lease.router, gw_str.data(), gw_str.size());
        ip_to_str(lease.dns, dns_str.data(), dns_str.size());
        logger::info("netd: %s configured: ip=%s mask=%s gw=%s dns=%s lease=%us", ifname, ip_str.data(), mask_str.data(), gw_str.data(),
                     dns_str.data(), lease.lease_time);
        if (lease.search_domains[0] != '\0') {
            logger::info("netd: resolver search domains: %s", lease.search_domains.data());
        } else if (lease.domain_name[0] != '\0') {
            logger::info("netd: resolver domain: %s", lease.domain_name.data());
        }
    }

    // === LEASE RENEWAL LOOP ===
    // T1 = lease_time / 2 (standard renewal time)

    if (lease.lease_time == 0) {
        logger::info("netd: infinite lease, sleeping forever");
        for (;;) {
            // Sleep for a very long time instead of busy-spinning.
            sleep_for_seconds(86400);  // 24 hours
        }
    }

    uint32_t t1_seconds = lease.lease_time / 2;
    logger::debug("netd: will renew in ~%u seconds (T1)", t1_seconds);
    uint32_t consecutive_renewal_failures = 0;

    for (;;) {
        // Sleep until T1, even if interrupted by signals.
        sleep_for_seconds(t1_seconds);

        if (consecutive_renewal_failures == 0) {
            logger::debug("netd: T1 reached, sending renewal REQUEST");
        }
        ++xid;

        std::array<char, 512> local_fqdn{};
        char const* fqdn = build_client_fqdn(local_hostname.data(), lease, local_fqdn) ? local_fqdn.data() : nullptr;
        size_t const PKT_LEN = build_renewal(&pkt, mac, xid, lease.your_ip, local_hostname.data(), fqdn);

        // Renewal: unicast to DHCP server
        struct sockaddr_in server{};
        server.sin_family = AF_INET;
        server.sin_port = htons(67);
        server.sin_addr.s_addr = htonl(lease.server_ip);

        ssize_t const SENT = sendto(SOCK, &pkt, PKT_LEN, 0, reinterpret_cast<struct sockaddr*>(&server), sizeof(server));
        if (std::cmp_not_equal(SENT, PKT_LEN)) {
            logger::warn("netd: renewal REQUEST sendto returned %zd expected %zu errno=%d", SENT, PKT_LEN, errno);
        }

        DhcpLease renew_lease{};
        uint8_t const MSG = recv_dhcp_reply_until_timeout(SOCK, recv_buf.data(), recv_buf.size(), xid, RECV_TIMEOUT_SECS, &renew_lease);
        if (MSG == DHCPACK) {
            bool network_changed = false;
            bool resolver_changed = false;
            if (renew_lease.subnet_mask != 0 && renew_lease.subnet_mask != lease.subnet_mask) {
                lease.subnet_mask = renew_lease.subnet_mask;
                network_changed = true;
            }
            if (renew_lease.router != 0 && renew_lease.router != lease.router) {
                lease.router = renew_lease.router;
                network_changed = true;
            }
            if (renew_lease.dns != 0 && renew_lease.dns != lease.dns) {
                lease.dns = renew_lease.dns;
                resolver_changed = true;
            }
            if (renew_lease.lease_time != 0) {
                lease.lease_time = renew_lease.lease_time;
            }
            if (renew_lease.domain_name[0] != '\0' && renew_lease.domain_name != lease.domain_name) {
                lease.domain_name = renew_lease.domain_name;
                resolver_changed = true;
            }
            if (renew_lease.search_domains[0] != '\0' && renew_lease.search_domains != lease.search_domains) {
                lease.search_domains = renew_lease.search_domains;
                resolver_changed = true;
            }
            if (network_changed) {
                apply_lease(ifname, lease);
            } else if (resolver_changed) {
                write_resolv_conf(lease);
            }
            t1_seconds = lease.lease_time / 2;
            if (consecutive_renewal_failures > 0) {
                logger::info("netd: lease renewed after %u failed attempt(s), next renewal in ~%us", consecutive_renewal_failures,
                             t1_seconds);
                consecutive_renewal_failures = 0;
            } else {
                logger::debug("netd: lease renewed, next renewal in ~%us", t1_seconds);
            }
        } else if (MSG == DHCPNAK) {
            logger::warn("netd: renewal received NAK, restarting from DISCOVER");
            close(SOCK);
            return 1;
        } else {
            ++consecutive_renewal_failures;
            if (consecutive_renewal_failures == 1 || consecutive_renewal_failures % RENEWAL_FAILURE_LOG_INTERVAL == 0) {
                logger::warn("netd: renewal timed out, retrying in %us (%u consecutive failures)", DHCP_RENEWAL_RETRY_SECS,
                             consecutive_renewal_failures);
            }
            t1_seconds = DHCP_RENEWAL_RETRY_SECS;
        }
    }

    close(SOCK);
    return 0;
}
