#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>

#include "netd/resolver.hpp"

namespace netd {
namespace {
constexpr auto RESOLVER_OPTIONS = "options timeout:2 attempts:4\n";
constexpr auto RESOLV_CONF_PATH = "resolv.conf";
constexpr auto RESOLV_CONF_TMP_PATH = "resolv.conf.tmp";

auto owned_file(const struct stat& state, bool temporary) -> bool {
    return state.st_uid == ::geteuid() && state.st_gid == ::getegid() && state.st_nlink == 1 &&
           (state.st_mode == (S_IFREG | 0644) || (temporary && state.st_mode == (S_IFREG | 0600)));
}
}  // namespace

// Exactly one DHCP thread writes this namespace. Restart may leave a previous
// exclusive temporary file; validate it before unlinking, never follow/truncate it.
auto write_resolv_conf_at(int directory, const DhcpLease& lease) -> int {
    if (lease.dns_count > lease.dns_servers.size() || std::memchr(lease.domain_name.data(), 0, lease.domain_name.size()) == nullptr ||
        std::memchr(lease.search_domains.data(), 0, lease.search_domains.size()) == nullptr)
        return EINVAL;
    struct stat state{};
    if (::fstat(directory, &state) != 0) return errno;
    if (state.st_mode != (S_IFDIR | 0755) || state.st_uid != ::geteuid() || state.st_gid != ::getegid()) return EPERM;
    if (::fstatat(directory, RESOLV_CONF_PATH, &state, AT_SYMLINK_NOFOLLOW) != 0) return errno;
    if (!owned_file(state, false)) return EPERM;
    if (::fstatat(directory, RESOLV_CONF_TMP_PATH, &state, AT_SYMLINK_NOFOLLOW) == 0) {
        if (!owned_file(state, true)) return EPERM;
        if (::unlinkat(directory, RESOLV_CONF_TMP_PATH, 0) != 0) return errno;
    } else if (errno != ENOENT)
        return errno;

    int const FD = ::openat(directory, RESOLV_CONF_TMP_PATH, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600);
    if (FD < 0) return errno;
    FILE* file = ::fdopen(FD, "w");
    if (file == nullptr) {
        int const ERROR = errno;
        ::close(FD);
        ::unlinkat(directory, RESOLV_CONF_TMP_PATH, 0);
        return ERROR;
    }
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
    int error = ferror(file) != 0 ? EIO : 0;
    if (fflush(file) != 0 && error == 0) error = errno;
    if (error == 0 && ::fchmod(FD, 0644) != 0) error = errno;
    if (fclose(file) != 0 && error == 0) error = errno;
    if (error == 0 && ::renameat(directory, RESOLV_CONF_TMP_PATH, directory, RESOLV_CONF_PATH) != 0) error = errno;
    if (error != 0) ::unlinkat(directory, RESOLV_CONF_TMP_PATH, 0);
    return error;
}
}  // namespace netd
