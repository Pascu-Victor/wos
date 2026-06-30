#include "netd/dhcp.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdio>
#include <cstring>

namespace netd {
namespace {

constexpr uint8_t DHCP_OP_REQUEST = 1;
constexpr uint8_t DHCP_OP_REPLY = 2;
constexpr uint8_t DHCP_HTYPE_ETHER = 1;
constexpr uint8_t DHCP_HLEN_ETHER = 6;

constexpr uint8_t DHCPDISCOVER = 1;
constexpr uint8_t DHCPREQUEST = 3;

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

auto load_network_u32(const uint8_t* data) -> uint32_t {
    uint32_t value = 0;
    std::memcpy(&value, data, sizeof(value));
    return ntohl(value);
}

void remember_dns_server(DhcpLease& lease, uint32_t dns_host) {
    if (dns_host == 0) {
        return;
    }

    for (size_t i = 0; i < lease.dns_count; i++) {
        if (lease.dns_servers[i] == dns_host) {
            return;
        }
    }

    if (lease.dns_count >= lease.dns_servers.size()) {
        return;
    }

    lease.dns_servers[lease.dns_count++] = dns_host;
    if (lease.dns == 0) {
        lease.dns = dns_host;
    }
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

}  // namespace

void ip_to_str(uint32_t ip_host, char* buf, size_t len) {
    struct in_addr a{};
    a.s_addr = htonl(ip_host);
    inet_ntop(AF_INET, &a, buf, static_cast<socklen_t>(len));
}

auto has_dns_servers(const DhcpLease& lease) -> bool { return lease.dns_count != 0 || lease.dns != 0; }

auto same_dns_servers(const DhcpLease& a, const DhcpLease& b) -> bool {
    if (a.dns_count != b.dns_count) {
        return false;
    }
    if (a.dns_count == 0 || b.dns_count == 0) {
        return a.dns == b.dns;
    }

    for (size_t i = 0; i < a.dns_count; i++) {
        if (a.dns_servers[i] != b.dns_servers[i]) {
            return false;
        }
    }
    return true;
}

void copy_dns_servers(DhcpLease& dest, const DhcpLease& src) {
    dest.dns = src.dns;
    dest.dns_servers = src.dns_servers;
    dest.dns_count = src.dns_count;
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
    *opt++ = OPT_MSG_TYPE;
    *opt++ = 1;
    *opt++ = DHCPDISCOVER;
    opt = append_client_identity_options(opt, hostname, fqdn);
    *opt++ = OPT_PARAM_LIST;
    *opt++ = 6;
    *opt++ = OPT_SUBNET_MASK;
    *opt++ = OPT_ROUTER;
    *opt++ = OPT_DNS;
    *opt++ = OPT_DOMAIN_NAME;
    *opt++ = OPT_DOMAIN_SEARCH;
    *opt++ = OPT_LEASE_TIME;
    *opt++ = OPT_END;

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
    *opt++ = OPT_MSG_TYPE;
    *opt++ = 1;
    *opt++ = DHCPREQUEST;
    opt = append_client_identity_options(opt, hostname, fqdn);
    *opt++ = OPT_REQUESTED_IP;
    *opt++ = 4;
    std::memcpy(opt, &requested_ip_net, 4);
    opt += 4;
    *opt++ = OPT_SERVER_ID;
    *opt++ = 4;
    std::memcpy(opt, &server_ip_net, 4);
    opt += 4;
    *opt++ = OPT_PARAM_LIST;
    *opt++ = 6;
    *opt++ = OPT_SUBNET_MASK;
    *opt++ = OPT_ROUTER;
    *opt++ = OPT_DNS;
    *opt++ = OPT_DOMAIN_NAME;
    *opt++ = OPT_DOMAIN_SEARCH;
    *opt++ = OPT_LEASE_TIME;
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
    pkt->ciaddr = htonl(client_ip_host);
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
        return 0;
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

    if (std::memcmp(pkt->options.data(), MAGIC_COOKIE.data(), MAGIC_COOKIE.size()) != 0) {
        return 0;
    }

    uint8_t msg_type = 0;
    const uint8_t* opt = pkt->options.data() + MAGIC_COOKIE.size();
    const uint8_t* end = data + len;

    while (opt < end && *opt != OPT_END) {
        if (*opt == 0) {
            opt++;
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
                for (size_t offset = 0; offset + sizeof(uint32_t) <= OLEN; offset += sizeof(uint32_t)) {
                    remember_dns_server(*lease, load_network_u32(opt + offset));
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

}  // namespace netd
