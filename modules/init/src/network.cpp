#include "network.h"

#include <abi-bits/in.h>
#include <abi-bits/ioctls.h>
#include <abi-bits/socket.h>
#include <arpa/inet.h>
#include <bits/ssize_t.h>
#include <callnums/sys_log.h>
#include <fcntl.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <sys/logging.h>
#include <sys/socket.h>
#include <unistd.h>
#include <wos/netctl.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>

namespace {
using init_log = wos::journal<"init">;
using init_net_log = wos::journal<"init_net">;

constexpr size_t IF_DEBUG_CAP = 16;
constexpr size_t IPV4_ADDRS_PER_INTERFACE_CAP = 8;
constexpr size_t IPV6_ADDRS_PER_INTERFACE_CAP = 8;
constexpr size_t ADDR_DEBUG_CAP = IF_DEBUG_CAP * (IPV4_ADDRS_PER_INTERFACE_CAP + IPV6_ADDRS_PER_INTERFACE_CAP);
static_assert(ADDR_DEBUG_CAP == 256);
constexpr size_t JOURNAL_READ_BATCH = 16;
constexpr size_t JOURNAL_DUMP_RECORD_CAP = 4096;
constexpr size_t JOURNAL_RECORD_SIZE = sizeof(ker::abi::sys_log::JournalRecord);
constexpr size_t NETDEV_STATS_BUF_SIZE = 1024;

void copy_ifreq_name(struct ifreq& ifr, const char* ifname) {
    std::memset(&ifr, 0, sizeof(ifr));
    // ifreq::ifr_name is a POSIX ABI raw array.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    std::strncpy(ifr.ifr_name, ifname, IFNAMSIZ - 1);
    ifr.ifr_name[IFNAMSIZ - 1] = '\0';
}

auto bounded_string_length(const char* text, size_t limit) -> size_t {
    if (text == nullptr) {
        return 0;
    }
    size_t len = 0;
    while (len < limit && text[len] != '\0') {
        len++;
    }
    return len;
}

auto nonzero_address(const uint8_t* address, size_t length) -> bool {
    for (size_t i = 0; i < length; ++i) {
        if (address[i] != 0) {
            return true;
        }
    }
    return false;
}

auto interface_has_usable_global_ipv6(const char* interface_name) -> bool {
    std::array<wos_net_if_info, IF_DEBUG_CAP> interfaces{};
    size_t interface_count = interfaces.size();
    if (wos_net_if_list(interfaces.data(), &interface_count) != 0) {
        return false;
    }

    uint32_t ifindex = 0;
    for (size_t i = 0; i < std::min(interface_count, interfaces.size()); ++i) {
        if (std::strncmp(interfaces[i].name, interface_name, WOS_NET_IF_NAME_LEN) == 0) {
            ifindex = interfaces[i].ifindex;
            break;
        }
    }
    if (ifindex == 0) {
        return false;
    }

    std::array<wos_net_addr_info, ADDR_DEBUG_CAP> addresses{};
    size_t address_count = addresses.size();
    if (wos_net_addr_list(addresses.data(), &address_count) != 0) {
        return false;
    }
    for (size_t i = 0; i < std::min(address_count, addresses.size()); ++i) {
        auto const& address = addresses[i];
        const uint8_t* local = nonzero_address(address.local, WOS_NET_ADDR_LEN) ? address.local : address.address;
        if (address.ifindex == ifindex && address.family == AF_INET6 && address.scope == 0 &&
            (address.flags & (WOS_NET_ADDR_F_TENTATIVE | WOS_NET_ADDR_F_DADFAILED)) == 0 && nonzero_address(local, WOS_NET_ADDR_LEN)) {
            return true;
        }
    }
    return false;
}

auto journal_level_name(uint8_t level) -> const char* {
    switch (level) {
        case 0:
            return "trace";
        case 1:
            return "debug";
        case 2:
            return "info";
        case 3:
            return "notice";
        case 4:
            return "warn";
        case 5:
            return "error";
        case 6:
            return "critical";
        case 7:
            return "panic";
        default:
            return "unknown";
    }
}

auto valid_journal_record(const ker::abi::sys_log::JournalRecord& rec) -> bool {
    if (rec.magic != ker::abi::sys_log::JOURNAL_RECORD_MAGIC || rec.version != ker::abi::sys_log::JOURNAL_RECORD_VERSION ||
        rec.message_len >= ker::abi::sys_log::JOURNAL_MESSAGE_MAX) {
        return false;
    }
    if (bounded_string_length(rec.module, ker::abi::sys_log::JOURNAL_MODULE_MAX) >= ker::abi::sys_log::JOURNAL_MODULE_MAX) {
        return false;
    }
    return bounded_string_length(rec.message, static_cast<size_t>(rec.message_len) + 1) == rec.message_len;
}

void replay_journal_record(const ker::abi::sys_log::JournalRecord& rec) {
    size_t const MODULE_LEN = bounded_string_length(rec.module, ker::abi::sys_log::JOURNAL_MODULE_MAX);
    std::array<char, ker::abi::sys_log::JOURNAL_MESSAGE_MAX> line{};
    int const PREFIX_LEN = std::snprintf(
        line.data(), line.size(), "journal[%llu.%03llu #%llu %-8s %.*s]: ", static_cast<unsigned long long>(rec.monotonic_us / 1000000ULL),
        ((rec.monotonic_us / 1000ULL) % 1000ULL), static_cast<unsigned long long>(rec.sequence), journal_level_name(rec.level),
        static_cast<int>(MODULE_LEN), rec.module);
    if (PREFIX_LEN < 0 || static_cast<size_t>(PREFIX_LEN) >= line.size()) {
        return;
    }

    size_t cursor = static_cast<size_t>(PREFIX_LEN);
    size_t const COPY_LEN = std::min(static_cast<size_t>(rec.message_len), line.size() - cursor - 1);
    std::memcpy(line.data() + cursor, rec.message, COPY_LEN);
    cursor += COPY_LEN;
    line.at(cursor) = '\0';
    init_net_log::critical(line.data(), static_cast<uint64_t>(cursor));
}

void dump_journal_snapshot() {
    int fd = ::open("/dev/journal", O_RDONLY | O_CLOEXEC | O_NONBLOCK);
    if (fd < 0) {
        int const ERR = errno;
        init_net_log::critical("unable to open /dev/journal for failure dump: errno=%d (%s)", ERR, strerror(ERR));
        return;
    }

    uint64_t latest_sequence = 0;
    uint64_t record_count = 0;
    size_t records_scanned = 0;
    std::array<ker::abi::sys_log::JournalRecord, JOURNAL_READ_BATCH> batch{};
    while (records_scanned < JOURNAL_DUMP_RECORD_CAP) {
        ssize_t const N = ::read(fd, batch.data(), batch.size() * JOURNAL_RECORD_SIZE);
        if (N <= 0) {
            break;
        }
        size_t const RECORDS = std::min(static_cast<size_t>(N) / JOURNAL_RECORD_SIZE, JOURNAL_DUMP_RECORD_CAP - records_scanned);
        if (RECORDS == 0) {
            break;
        }
        records_scanned += RECORDS;
        for (size_t i = 0; i < RECORDS; i++) {
            auto const& rec = batch.at(i);
            if (!valid_journal_record(rec)) {
                continue;
            }
            uint64_t const SEQUENCE = rec.sequence;
            latest_sequence = std::max(SEQUENCE, latest_sequence);
            record_count++;
        }
    }
    ::close(fd);

    init_net_log::critical("replaying %llu journal record(s) up to sequence %llu", static_cast<unsigned long long>(record_count),
                           static_cast<unsigned long long>(latest_sequence));
    if (latest_sequence == 0) {
        return;
    }

    fd = ::open("/dev/journal", O_RDONLY | O_CLOEXEC | O_NONBLOCK);
    if (fd < 0) {
        int const ERR = errno;
        init_net_log::critical("unable to reopen /dev/journal for failure dump: errno=%d (%s)", ERR, strerror(ERR));
        return;
    }

    records_scanned = 0;
    while (records_scanned < JOURNAL_DUMP_RECORD_CAP) {
        ssize_t const N = ::read(fd, batch.data(), batch.size() * JOURNAL_RECORD_SIZE);
        if (N <= 0) {
            break;
        }
        size_t const RECORDS = std::min(static_cast<size_t>(N) / JOURNAL_RECORD_SIZE, JOURNAL_DUMP_RECORD_CAP - records_scanned);
        if (RECORDS == 0) {
            break;
        }
        records_scanned += RECORDS;
        for (size_t i = 0; i < RECORDS; i++) {
            auto const& rec = batch.at(i);
            if (!valid_journal_record(rec) || rec.sequence > latest_sequence) {
                continue;
            }
            replay_journal_record(rec);
        }
    }
    ::close(fd);
}

void dump_ioctl_state(int sock, const char* interface_name) {
    if (sock < 0) {
        init_net_log::critical("no AF_INET/SOCK_DGRAM socket available for ioctl probes");
        return;
    }
    if (interface_name == nullptr || interface_name[0] == '\0') {
        init_net_log::critical("no interface name available for ioctl probes");
        return;
    }

    struct ifreq ifr{};
    copy_ifreq_name(ifr, interface_name);
    if (ioctl(sock, SIOCGIFFLAGS, &ifr) == 0) {
        init_net_log::critical("%s SIOCGIFFLAGS flags=0x%x", interface_name, static_cast<unsigned>(ifr.ifr_flags));
    } else {
        int const ERR = errno;
        init_net_log::critical("%s SIOCGIFFLAGS failed errno=%d (%s)", interface_name, ERR, strerror(ERR));
    }

    copy_ifreq_name(ifr, interface_name);
    if (ioctl(sock, SIOCGIFINDEX, &ifr) == 0) {
        init_net_log::critical("%s SIOCGIFINDEX ifindex=%d", interface_name, ifr.ifr_ifindex);
    } else {
        int const ERR = errno;
        init_net_log::critical("%s SIOCGIFINDEX failed errno=%d (%s)", interface_name, ERR, strerror(ERR));
    }

    copy_ifreq_name(ifr, interface_name);
    if (ioctl(sock, SIOCGIFMTU, &ifr) == 0) {
        init_net_log::critical("%s SIOCGIFMTU mtu=%d", interface_name, ifr.ifr_mtu);
    } else {
        int const ERR = errno;
        init_net_log::critical("%s SIOCGIFMTU failed errno=%d (%s)", interface_name, ERR, strerror(ERR));
    }

    copy_ifreq_name(ifr, interface_name);
    if (ioctl(sock, SIOCGIFHWADDR, &ifr) == 0) {
        auto const* mac = reinterpret_cast<const unsigned char*>(ifr.ifr_hwaddr.sa_data);
        init_net_log::critical("%s SIOCGIFHWADDR family=%u mac=%02x:%02x:%02x:%02x:%02x:%02x", interface_name,
                               static_cast<unsigned>(ifr.ifr_hwaddr.sa_family), static_cast<unsigned>(mac[0]),
                               static_cast<unsigned>(mac[1]), static_cast<unsigned>(mac[2]), static_cast<unsigned>(mac[3]),
                               static_cast<unsigned>(mac[4]), static_cast<unsigned>(mac[5]));
    } else {
        int const ERR = errno;
        init_net_log::critical("%s SIOCGIFHWADDR failed errno=%d (%s)", interface_name, ERR, strerror(ERR));
    }

    copy_ifreq_name(ifr, interface_name);
    if (ioctl(sock, SIOCGIFADDR, &ifr) == 0) {
        auto* addr = reinterpret_cast<struct sockaddr_in*>(&ifr.ifr_addr);
        std::array<char, INET_ADDRSTRLEN> ip_str{};
        inet_ntop(AF_INET, &addr->sin_addr, ip_str.data(), ip_str.size());
        init_net_log::critical("%s SIOCGIFADDR family=%u addr=%s raw=0x%x", interface_name, static_cast<unsigned>(addr->sin_family),
                               ip_str.data(), static_cast<unsigned>(ntohl(addr->sin_addr.s_addr)));
    } else {
        int const ERR = errno;
        init_net_log::critical("%s SIOCGIFADDR failed errno=%d (%s)", interface_name, ERR, strerror(ERR));
    }

    copy_ifreq_name(ifr, interface_name);
    if (ioctl(sock, SIOCGIFNETMASK, &ifr) == 0) {
        auto* addr = reinterpret_cast<struct sockaddr_in*>(&ifr.ifr_netmask);
        std::array<char, INET_ADDRSTRLEN> mask_str{};
        inet_ntop(AF_INET, &addr->sin_addr, mask_str.data(), mask_str.size());
        init_net_log::critical("%s SIOCGIFNETMASK family=%u mask=%s raw=0x%x", interface_name, static_cast<unsigned>(addr->sin_family),
                               mask_str.data(), static_cast<unsigned>(ntohl(addr->sin_addr.s_addr)));
    } else {
        int const ERR = errno;
        init_net_log::critical("%s SIOCGIFNETMASK failed errno=%d (%s)", interface_name, ERR, strerror(ERR));
    }
}

void dump_netctl_state() {
    std::array<wos_net_if_info, IF_DEBUG_CAP> ifs{};
    size_t if_count = ifs.size();
    if (wos_net_if_list(ifs.data(), &if_count) != 0) {
        int const ERR = errno;
        init_net_log::critical("wos_net_if_list failed errno=%d (%s)", ERR, strerror(ERR));
    } else {
        size_t const WRITTEN = std::min(if_count, ifs.size());
        init_net_log::critical("netctl reports %llu interface(s), dumping %llu", static_cast<unsigned long long>(if_count),
                               static_cast<unsigned long long>(WRITTEN));
        for (size_t i = 0; i < WRITTEN; i++) {
            auto const& info = ifs.at(i);
            init_log::critical(
                "if_net[%llu] index=%u name=%.*s flags=0x%x mtu=%u txqlen=%u type=%u operstate=%u addr_len=%u "
                "mac=%02x:%02x:%02x:%02x:%02x:%02x",
                static_cast<unsigned long long>(i), info.ifindex, WOS_NET_IF_NAME_LEN, info.name, info.flags, info.mtu, info.tx_queue_len,
                static_cast<unsigned>(info.type), static_cast<unsigned>(info.operstate), static_cast<unsigned>(info.addr_len),
                static_cast<unsigned>(info.addr[0]), static_cast<unsigned>(info.addr[1]), static_cast<unsigned>(info.addr[2]),
                static_cast<unsigned>(info.addr[3]), static_cast<unsigned>(info.addr[4]), static_cast<unsigned>(info.addr[5]));
        }
    }

    std::array<wos_net_addr_info, ADDR_DEBUG_CAP> addrs{};
    size_t addr_count = addrs.size();
    if (wos_net_addr_list(addrs.data(), &addr_count) != 0) {
        int const ERR = errno;
        init_net_log::critical("wos_net_addr_list failed errno=%d (%s)", ERR, strerror(ERR));
        return;
    }

    size_t const WRITTEN = std::min(addr_count, addrs.size());
    init_net_log::critical("netctl reports %llu address(es), dumping %llu", static_cast<unsigned long long>(addr_count),
                           static_cast<unsigned long long>(WRITTEN));
    for (size_t i = 0; i < WRITTEN; i++) {
        auto const& addr = addrs.at(i);
        if (addr.family == AF_INET) {
            std::array<char, INET_ADDRSTRLEN> local{};
            std::array<char, INET_ADDRSTRLEN> broadcast{};
            inet_ntop(AF_INET, addr.local, local.data(), local.size());
            inet_ntop(AF_INET, addr.broadcast, broadcast.data(), broadcast.size());
            init_log::critical("add_netr[%llu] ifindex=%u label=%.*s family=AF_INET prefix=%u scope=%u flags=0x%x local=%s brd=%s",
                               static_cast<unsigned long long>(i), addr.ifindex, WOS_NET_IF_NAME_LEN, addr.label,
                               static_cast<unsigned>(addr.prefix_len), static_cast<unsigned>(addr.scope), addr.flags, local.data(),
                               broadcast.data());
        } else if (addr.family == AF_INET6) {
            std::array<char, INET6_ADDRSTRLEN> local{};
            const uint8_t* raw = nonzero_address(addr.local, WOS_NET_ADDR_LEN) ? addr.local : addr.address;
            inet_ntop(AF_INET6, raw, local.data(), local.size());
            init_net_log::critical("addr[%llu] ifindex=%u label=%.*s family=AF_INET6 prefix=%u scope=%u flags=0x%x local=%s",
                                   static_cast<unsigned long long>(i), addr.ifindex, WOS_NET_IF_NAME_LEN, addr.label,
                                   static_cast<unsigned>(addr.prefix_len), static_cast<unsigned>(addr.scope), addr.flags, local.data());
        } else {
            init_net_log::critical("addr[%llu] ifindex=%u label=%.*s family=%u prefix=%u scope=%u flags=0x%x",
                                   static_cast<unsigned long long>(i), addr.ifindex, WOS_NET_IF_NAME_LEN, addr.label,
                                   static_cast<unsigned>(addr.family), static_cast<unsigned>(addr.prefix_len),
                                   static_cast<unsigned>(addr.scope), addr.flags);
        }
    }
}

void dump_netdev_stats_file(const char* path) {
    int const FD = ::open(path, O_RDONLY);
    if (FD < 0) {
        int const ERR = errno;
        init_net_log::critical("unable to open %s: errno=%d (%s)", path, ERR, strerror(ERR));
        return;
    }

    std::array<char, NETDEV_STATS_BUF_SIZE> buf{};
    ssize_t const N = ::read(FD, buf.data(), buf.size() - 1);
    ::close(FD);
    if (N < 0) {
        int const ERR = errno;
        init_net_log::critical("unable to read %s: errno=%d (%s)", path, ERR, strerror(ERR));
        return;
    }
    if (N == 0) {
        init_net_log::critical("%s is empty", path);
        return;
    }

    size_t line_start = 0;
    size_t const LEN = static_cast<size_t>(N);
    for (size_t i = 0; i <= LEN; i++) {
        if (i != LEN && buf.at(i) != '\n') {
            continue;
        }
        size_t const LINE_LEN = i - line_start;
        if (LINE_LEN != 0) {
            init_net_log::critical("%s: %.*s", path, static_cast<int>(LINE_LEN), buf.data() + line_start);
        }
        line_start = i + 1;
    }
}

void dump_netdev_stats(const char* interface_name) {
    if (interface_name == nullptr || interface_name[0] == '\0') {
        init_net_log::critical("no interface name available for /dev/net diagnostics");
        return;
    }

    std::array<char, sizeof("/dev/net/") + IFNAMSIZ> path{};
    int const WRITTEN = std::snprintf(path.data(), path.size(), "/dev/net/%s", interface_name);
    if (WRITTEN < 0 || static_cast<size_t>(WRITTEN) >= path.size()) {
        init_net_log::critical("interface name is too long for /dev/net diagnostics");
        return;
    }
    dump_netdev_stats_file(path.data());
}

}  // namespace

auto network_probe_begin(NetworkProbe& probe, const char* interface_name) -> bool {
    network_probe_reset(probe);

    size_t const NAME_LEN = bounded_string_length(interface_name, probe.interface_name.size());
    if (interface_name == nullptr || NAME_LEN == 0) {
        probe.open_errno = EINVAL;
        errno = probe.open_errno;
        return false;
    }
    if (NAME_LEN >= probe.interface_name.size()) {
        probe.open_errno = ENAMETOOLONG;
        errno = probe.open_errno;
        return false;
    }
    std::memcpy(probe.interface_name.data(), interface_name, NAME_LEN);
    probe.interface_name.at(NAME_LEN) = '\0';

    int const SOCKET_FD = socket(AF_INET, SOCK_DGRAM, 0);
    if (SOCKET_FD < 0) {
        probe.open_errno = errno;
        return false;
    }

    int const FD_FLAGS = fcntl(SOCKET_FD, F_GETFD);
    if (FD_FLAGS < 0 || fcntl(SOCKET_FD, F_SETFD, FD_FLAGS | FD_CLOEXEC) < 0) {
        int const ERR = errno;
        (void)::close(SOCKET_FD);
        probe.open_errno = ERR;
        errno = ERR;
        return false;
    }

    probe.socket_fd = SOCKET_FD;
    return true;
}

auto network_probe_poll(NetworkProbe& probe) -> NetworkProbeResult {
    if (probe.socket_fd < 0 || probe.interface_name.front() == '\0') {
        int const ERR = probe.open_errno != 0 ? probe.open_errno : EBADF;
        if (probe.first_errno == 0) {
            probe.first_errno = ERR;
        }
        probe.last_errno = ERR;
        errno = ERR;
        return NetworkProbeResult::ERROR;
    }

    probe.attempts++;
    struct ifreq ifr{};
    copy_ifreq_name(ifr, probe.interface_name.data());
    if (ioctl(probe.socket_fd, SIOCGIFADDR, &ifr) != 0) {
        int const ERR = errno;
        if (interface_has_usable_global_ipv6(probe.interface_name.data())) {
            return NetworkProbeResult::READY;
        }
        probe.addr_failures++;
        if (probe.first_errno == 0) {
            probe.first_errno = ERR;
        }
        probe.last_errno = ERR;
        return NetworkProbeResult::ERROR;
    }

    probe.addr_successes++;
    auto const* addr = reinterpret_cast<const struct sockaddr_in*>(&ifr.ifr_addr);
    probe.last_ipv4 = addr->sin_addr.s_addr;
    if (probe.last_ipv4 == 0) {
        probe.addr_zero_results++;
        if (interface_has_usable_global_ipv6(probe.interface_name.data())) {
            return NetworkProbeResult::READY;
        }
        return NetworkProbeResult::PENDING;
    }
    return NetworkProbeResult::READY;
}

void network_probe_dump_diagnostics(const NetworkProbe& probe, const char* reason, uint64_t service_pid) {
    init_net_log::critical("=== begin network readiness diagnostic dump ===");
    init_net_log::critical("reason=%s service_pid=%llu interface=%s poll_socket=%d open_errno=%d (%s)",
                           reason != nullptr ? reason : "(unknown)", static_cast<unsigned long long>(service_pid),
                           probe.interface_name.front() != '\0' ? probe.interface_name.data() : "(unset)", probe.socket_fd,
                           probe.open_errno, probe.open_errno != 0 ? strerror(probe.open_errno) : "none");

    std::array<char, INET_ADDRSTRLEN> last_ip{};
    struct in_addr last_addr{};
    last_addr.s_addr = probe.last_ipv4;
    inet_ntop(AF_INET, &last_addr, last_ip.data(), last_ip.size());
    init_log::critical("network poll attempts=%llu addr_successes=%llu zero_addr_results=%llu addr_failures=%llu last_addr=%s raw=0x%x",
                       static_cast<unsigned long long>(probe.attempts), static_cast<unsigned long long>(probe.addr_successes),
                       static_cast<unsigned long long>(probe.addr_zero_results), static_cast<unsigned long long>(probe.addr_failures),
                       last_ip.data(), static_cast<unsigned>(ntohl(probe.last_ipv4)));
    init_net_log::critical("poll first_errno=%d (%s)", probe.first_errno, probe.first_errno != 0 ? strerror(probe.first_errno) : "none");
    init_net_log::critical("poll last_errno=%d (%s)", probe.last_errno, probe.last_errno != 0 ? strerror(probe.last_errno) : "none");

    dump_ioctl_state(probe.socket_fd, probe.interface_name.data());
    dump_netctl_state();
    dump_netdev_stats(probe.interface_name.data());
    dump_journal_snapshot();
    init_net_log::critical("=== end network readiness diagnostic dump ===");
}

void network_probe_close(NetworkProbe& probe) {
    int const SOCKET_FD = probe.socket_fd;
    probe.socket_fd = -1;
    if (SOCKET_FD >= 0) {
        (void)::close(SOCKET_FD);
    }
}

void network_probe_reset(NetworkProbe& probe) {
    network_probe_close(probe);
    probe = NetworkProbe{};
}
