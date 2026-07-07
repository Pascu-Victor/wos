#include "netd/resolver.hpp"

#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>

#include "netd/log.hpp"

namespace netd {
namespace {

constexpr auto RESOLVER_OPTIONS = "options timeout:2 attempts:4\n";
constexpr auto RESOLV_CONF_PATH = "/etc/resolv.conf";
constexpr auto RESOLV_CONF_TMP_PATH = "/etc/resolv.conf.tmp";

void boot_trace(const char* message) {
    if (message == nullptr) {
        return;
    }
    size_t len = 0;
    while (message[len] != '\0') {
        len++;
    }
    (void)::write(STDERR_FILENO, message, len);
}

}  // namespace

void write_resolv_conf(const DhcpLease& lease) {
    boot_trace("netd-boot: resolver before fopen\n");
    FILE* file = fopen(RESOLV_CONF_TMP_PATH, "w");
    boot_trace("netd-boot: resolver after fopen\n");
    if (file == nullptr) {
        logger::warn("netd: failed to update /etc/resolv.conf: %d", errno);
        return;
    }

    boot_trace("netd-boot: resolver before writes\n");
    fputs("# Managed by netd via DHCP\n", file);
    fputs(RESOLVER_OPTIONS, file);
    if (lease.dns_count != 0) {
        for (size_t i = 0; i < lease.dns_count; i++) {
            std::array<char, 16> dns_str{};
            ip_to_str(lease.dns_servers[i], dns_str.data(), dns_str.size());
            fprintf(file, "nameserver %s\n", dns_str.data());
        }
    } else if (lease.dns != 0) {
        std::array<char, 16> dns_str{};
        ip_to_str(lease.dns, dns_str.data(), dns_str.size());
        fprintf(file, "nameserver %s\n", dns_str.data());
    }

    if (lease.search_domains[0] != '\0') {
        fprintf(file, "search %s\n", lease.search_domains.data());
    } else if (lease.domain_name[0] != '\0') {
            fprintf(file, "domain %s\n", lease.domain_name.data());
    }

    boot_trace("netd-boot: resolver after writes\n");
    bool ok = ferror(file) == 0;
    boot_trace("netd-boot: resolver before fclose\n");
    if (fclose(file) != 0) {
        ok = false;
    }
    boot_trace("netd-boot: resolver after fclose\n");
    if (!ok) {
        logger::warn("netd: failed to write %s: %d", RESOLV_CONF_TMP_PATH, errno);
        unlink(RESOLV_CONF_TMP_PATH);
        return;
    }

    boot_trace("netd-boot: resolver before rename\n");
    if (rename(RESOLV_CONF_TMP_PATH, RESOLV_CONF_PATH) != 0) {
        logger::warn("netd: failed to replace %s: %d", RESOLV_CONF_PATH, errno);
        unlink(RESOLV_CONF_TMP_PATH);
    }
    boot_trace("netd-boot: resolver after rename\n");
}

}  // namespace netd
