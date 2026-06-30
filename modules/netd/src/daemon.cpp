#include "netd/daemon.hpp"

#include <abi-bits/in.h>
#include <abi-bits/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <utility>

#include "netd/config.hpp"
#include "netd/dhcp.hpp"
#include "netd/interface.hpp"
#include "netd/log.hpp"
#include "netd/recv.hpp"
#include "netd/resolver.hpp"
#include "netd/time.hpp"

namespace netd {
namespace {

#ifndef SO_BINDTODEVICE
constexpr int SO_BINDTODEVICE = 25;
#endif

}  // namespace

auto run_dhcp_client() -> int {
    const char* ifname = find_ifname_for_driver("dhcp", "eth0");
    logger::info("netd: starting DHCP client for %s", ifname);

    int const SOCK = socket(AF_INET, SOCK_DGRAM, 0);
    if (SOCK < 0) {
        logger::error("netd: failed to create socket: %d", SOCK);
        return 1;
    }

    if (setsockopt(SOCK, SOL_SOCKET, SO_BINDTODEVICE, ifname, std::strlen(ifname) + 1) != 0) {
        logger::error("netd: failed to bind DHCP socket to %s", ifname);
        close(SOCK);
        return 1;
    }

    struct sockaddr_in bind_addr{};
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_port = htons(68);
    bind_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(SOCK, reinterpret_cast<struct sockaddr*>(&bind_addr), sizeof(bind_addr)) < 0) {
        logger::error("netd: failed to bind to port 68");
        close(SOCK);
        return 1;
    }

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

    struct sockaddr_in dst_addr{};
    dst_addr.sin_family = AF_INET;
    dst_addr.sin_port = htons(67);
    dst_addr.sin_addr.s_addr = htonl(0xFFFFFFFF);

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
    bool got_offer = false;
    lease = {};
    for (int attempt = 0; attempt < MAX_DISCOVER_RETRIES && !got_offer; attempt++) {
        logger::debug("netd: sending DISCOVER (attempt %d/%d)", attempt + 1, MAX_DISCOVER_RETRIES);
        size_t const PKT_LEN = build_discover(&pkt, mac, xid, local_hostname.data(), nullptr);
        ssize_t const SENT = sendto(SOCK, &pkt, PKT_LEN, 0, reinterpret_cast<struct sockaddr*>(&dst_addr), sizeof(dst_addr));
        if (std::cmp_not_equal(SENT, PKT_LEN)) {
            logger::warn("netd: DISCOVER sendto returned %zd expected %zu errno=%d", SENT, PKT_LEN, errno);
        }

        while (!got_offer) {
            ssize_t const N = recv_with_timeout(SOCK, recv_buf.data(), recv_buf.size(), RECV_TIMEOUT_SECS);
            if (N <= 0) {
                break;
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
        }
    }

    if (!got_offer) {
        logger::error("netd: no DHCP offer received, exiting");
        close(SOCK);
        return 1;
    }

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

        while (!got_ack) {
            ssize_t const N = recv_with_timeout(SOCK, recv_buf.data(), recv_buf.size(), RECV_TIMEOUT_SECS);
            if (N <= 0) {
                break;
            }
            DhcpLease ack_lease{};
            uint8_t const MSG = parse_reply(recv_buf.data(), static_cast<size_t>(N), xid, &ack_lease);
            if (MSG == DHCPACK) {
                if (ack_lease.subnet_mask != 0) {
                    lease.subnet_mask = ack_lease.subnet_mask;
                }
                if (ack_lease.router != 0) {
                    lease.router = ack_lease.router;
                }
                if (has_dns_servers(ack_lease)) {
                    copy_dns_servers(lease, ack_lease);
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
                    ++xid;
                    goto nak_restart;
                }
                logger::error("netd: received NAK, max restarts exceeded");
                goto request_failed;
            }
        }
    }
request_failed:

    if (!got_ack) {
        logger::error("netd: DHCP failed - no ACK received");
        close(SOCK);
        return 1;
    }

    logger::info("netd: DHCP ACK received, applying configuration");
    if (!apply_lease(ifname, lease)) {
        logger::error("netd: DHCP lease apply failed, exiting");
        close(SOCK);
        return 1;
    }

    {
        std::array<char, 16> ip_str{};
        std::array<char, 16> mask_str{};
        std::array<char, 16> gw_str{};
        std::array<char, 16> dns_str{};
        ip_to_str(lease.your_ip, ip_str.data(), ip_str.size());
        ip_to_str(lease.subnet_mask, mask_str.data(), mask_str.size());
        ip_to_str(lease.router, gw_str.data(), gw_str.size());
        if (lease.dns_count != 0) {
            ip_to_str(lease.dns_servers[0], dns_str.data(), dns_str.size());
        } else {
            ip_to_str(lease.dns, dns_str.data(), dns_str.size());
        }
        logger::info("netd: %s configured: ip=%s mask=%s gw=%s dns=%s lease=%us", ifname, ip_str.data(), mask_str.data(), gw_str.data(),
                     dns_str.data(), lease.lease_time);
        if (lease.dns_count > 1) {
            logger::info("netd: resolver has %zu DNS servers", lease.dns_count);
        }
        if (lease.search_domains[0] != '\0') {
            logger::info("netd: resolver search domains: %s", lease.search_domains.data());
        } else if (lease.domain_name[0] != '\0') {
            logger::info("netd: resolver domain: %s", lease.domain_name.data());
        }
    }

    if (lease.lease_time == 0) {
        logger::info("netd: infinite lease, sleeping forever");
        for (;;) {
            sleep_for_seconds(86400);
        }
    }

    uint32_t t1_seconds = lease.lease_time / 2;
    logger::debug("netd: will renew in ~%u seconds (T1)", t1_seconds);
    uint32_t consecutive_renewal_failures = 0;

    for (;;) {
        sleep_for_seconds(t1_seconds);

        if (consecutive_renewal_failures == 0) {
            logger::debug("netd: T1 reached, sending renewal REQUEST");
        }
        ++xid;

        std::array<char, 512> local_fqdn{};
        char const* fqdn = build_client_fqdn(local_hostname.data(), lease, local_fqdn) ? local_fqdn.data() : nullptr;
        size_t const PKT_LEN = build_renewal(&pkt, mac, xid, lease.your_ip, local_hostname.data(), fqdn);

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
            if (has_dns_servers(renew_lease) && !same_dns_servers(renew_lease, lease)) {
                copy_dns_servers(lease, renew_lease);
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
                if (!apply_lease(ifname, lease)) {
                    logger::warn("netd: renewal lease apply failed, retrying renewal path");
                    consecutive_renewal_failures++;
                    t1_seconds = DHCP_RENEWAL_RETRY_SECS;
                    continue;
                }
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

}  // namespace netd
