#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

namespace netd {

constexpr uint8_t DHCPOFFER = 2;
constexpr uint8_t DHCPACK = 5;
constexpr uint8_t DHCPNAK = 6;

constexpr int RECV_TIMEOUT_SECS = 5;
constexpr int MAX_DISCOVER_RETRIES = 5;
constexpr int MAX_REQUEST_RETRIES = 3;
constexpr int MAX_NAK_RESTARTS = 3;
constexpr uint32_t RENEWAL_FAILURE_LOG_INTERVAL = 10;
constexpr uint32_t DHCP_RENEWAL_RETRY_SECS = 60;
constexpr size_t MAX_DNS_SERVERS = 3;

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
    uint32_t your_ip;
    uint32_t server_ip;
    uint32_t subnet_mask;
    uint32_t router;
    uint32_t dns;
    std::array<uint32_t, MAX_DNS_SERVERS> dns_servers;
    size_t dns_count;
    uint32_t lease_time;
    std::array<char, 256> domain_name;
    std::array<char, 256> search_domains;
};

void ip_to_str(uint32_t ip_host, char* buf, size_t len);
auto has_dns_servers(const DhcpLease& lease) -> bool;
auto same_dns_servers(const DhcpLease& a, const DhcpLease& b) -> bool;
void copy_dns_servers(DhcpLease& dest, const DhcpLease& src);
auto read_local_hostname(std::span<char> dest) -> bool;
auto build_client_fqdn(const char* hostname, const DhcpLease& lease, std::span<char> dest) -> bool;
auto build_discover(DhcpPacket* pkt, std::span<const uint8_t, 6> mac, uint32_t xid, const char* hostname, const char* fqdn) -> size_t;
auto build_request(DhcpPacket* pkt, std::span<const uint8_t, 6> mac, uint32_t xid, uint32_t requested_ip_net, uint32_t server_ip_net,
                   const char* hostname, const char* fqdn) -> size_t;
auto build_renewal(DhcpPacket* pkt, std::span<const uint8_t, 6> mac, uint32_t xid, uint32_t client_ip_host, const char* hostname,
                   const char* fqdn) -> size_t;
auto parse_reply(const uint8_t* data, size_t len, uint32_t expected_xid, DhcpLease* lease) -> uint8_t;

}  // namespace netd
