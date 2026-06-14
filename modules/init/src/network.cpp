#include "network.h"

#include <abi-bits/in.h>
#include <abi-bits/ioctls.h>
#include <abi-bits/socket.h>
#include <arpa/inet.h>
#include <bits/ssize_t.h>
#include <callnums/sys_log.h>
#include <fcntl.h>
#include <net/if.h>
#include <signal.h>  // NOLINT(modernize-deprecated-headers): WOS signal constants live here.
#include <sys/ioctl.h>
#include <sys/logging.h>
#include <sys/process.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <time.h>  // NOLINT(modernize-deprecated-headers): WOS POSIX clock declarations live here.
#include <unistd.h>
#include <wos/netctl.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>

#include "env.h"
#include "services.h"
#include "sys/multiproc.h"

namespace {
using init_log = wos::journal<"init">;

constexpr const char* NET_IFNAME = "eth0";
constexpr long POLL_FIRST_DIAGNOSTIC_SECS = 45;
constexpr long POLL_STATUS_INTERVAL_SECS = 60;
constexpr long POLL_FAILURE_TIMEOUT_SECS = 180;
constexpr long POLL_INTERVAL_MS = 50;
constexpr uint32_t NETD_KILL_REAP_RETRIES = 1000;
constexpr size_t IF_DEBUG_CAP = 16;
constexpr size_t ADDR_DEBUG_CAP = 32;
constexpr size_t JOURNAL_READ_BATCH = 16;
constexpr size_t JOURNAL_RECORD_SIZE = sizeof(ker::abi::sys_log::JournalRecord);
constexpr size_t NETDEV_STATS_BUF_SIZE = 1024;

struct PollDebug {
    uint64_t attempts = 0;
    uint64_t addr_successes = 0;
    uint64_t addr_zero_results = 0;
    uint64_t addr_failures = 0;
    int first_errno = 0;
    int last_errno = 0;
    uint32_t last_addr = 0;
};

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
    ker::logging::logEx("init", ker::abi::sys_log::sys_log_level::CRITICAL, line.data(), static_cast<uint64_t>(cursor));
}

void dump_journal_snapshot() {
    int fd = ::open("/dev/journal", O_RDONLY);
    if (fd < 0) {
        int const ERR = errno;
        init_log::critical("network debug: unable to open /dev/journal for failure dump: errno=%d (%s)", ERR, strerror(ERR));
        return;
    }

    uint64_t latest_sequence = 0;
    uint64_t record_count = 0;
    std::array<ker::abi::sys_log::JournalRecord, JOURNAL_READ_BATCH> batch{};
    for (;;) {
        ssize_t const N = ::read(fd, batch.data(), batch.size() * JOURNAL_RECORD_SIZE);
        if (N <= 0) {
            break;
        }
        size_t const RECORDS = static_cast<size_t>(N) / JOURNAL_RECORD_SIZE;
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

    init_log::critical("network debug: replaying %llu journal record(s) up to sequence %llu", static_cast<unsigned long long>(record_count),
                       static_cast<unsigned long long>(latest_sequence));
    if (latest_sequence == 0) {
        return;
    }

    fd = ::open("/dev/journal", O_RDONLY);
    if (fd < 0) {
        int const ERR = errno;
        init_log::critical("network debug: unable to reopen /dev/journal for failure dump: errno=%d (%s)", ERR, strerror(ERR));
        return;
    }

    for (;;) {
        ssize_t const N = ::read(fd, batch.data(), batch.size() * JOURNAL_RECORD_SIZE);
        if (N <= 0) {
            break;
        }
        size_t const RECORDS = static_cast<size_t>(N) / JOURNAL_RECORD_SIZE;
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

void dump_ioctl_state(int sock) {
    if (sock < 0) {
        init_log::critical("network debug: no AF_INET/SOCK_DGRAM socket available for ioctl probes");
        return;
    }

    struct ifreq ifr{};
    copy_ifreq_name(ifr, NET_IFNAME);
    if (ioctl(sock, SIOCGIFFLAGS, &ifr) == 0) {
        init_log::critical("network debug: %s SIOCGIFFLAGS flags=0x%x", NET_IFNAME, static_cast<unsigned>(ifr.ifr_flags));
    } else {
        int const ERR = errno;
        init_log::critical("network debug: %s SIOCGIFFLAGS failed errno=%d (%s)", NET_IFNAME, ERR, strerror(ERR));
    }

    copy_ifreq_name(ifr, NET_IFNAME);
    if (ioctl(sock, SIOCGIFINDEX, &ifr) == 0) {
        init_log::critical("network debug: %s SIOCGIFINDEX ifindex=%d", NET_IFNAME, ifr.ifr_ifindex);
    } else {
        int const ERR = errno;
        init_log::critical("network debug: %s SIOCGIFINDEX failed errno=%d (%s)", NET_IFNAME, ERR, strerror(ERR));
    }

    copy_ifreq_name(ifr, NET_IFNAME);
    if (ioctl(sock, SIOCGIFMTU, &ifr) == 0) {
        init_log::critical("network debug: %s SIOCGIFMTU mtu=%d", NET_IFNAME, ifr.ifr_mtu);
    } else {
        int const ERR = errno;
        init_log::critical("network debug: %s SIOCGIFMTU failed errno=%d (%s)", NET_IFNAME, ERR, strerror(ERR));
    }

    copy_ifreq_name(ifr, NET_IFNAME);
    if (ioctl(sock, SIOCGIFHWADDR, &ifr) == 0) {
        auto const* mac = reinterpret_cast<const unsigned char*>(ifr.ifr_hwaddr.sa_data);
        init_log::critical("network debug: %s SIOCGIFHWADDR family=%u mac=%02x:%02x:%02x:%02x:%02x:%02x", NET_IFNAME,
                           static_cast<unsigned>(ifr.ifr_hwaddr.sa_family), static_cast<unsigned>(mac[0]), static_cast<unsigned>(mac[1]),
                           static_cast<unsigned>(mac[2]), static_cast<unsigned>(mac[3]), static_cast<unsigned>(mac[4]),
                           static_cast<unsigned>(mac[5]));
    } else {
        int const ERR = errno;
        init_log::critical("network debug: %s SIOCGIFHWADDR failed errno=%d (%s)", NET_IFNAME, ERR, strerror(ERR));
    }

    copy_ifreq_name(ifr, NET_IFNAME);
    if (ioctl(sock, SIOCGIFADDR, &ifr) == 0) {
        auto* addr = reinterpret_cast<struct sockaddr_in*>(&ifr.ifr_addr);
        std::array<char, INET_ADDRSTRLEN> ip_str{};
        inet_ntop(AF_INET, &addr->sin_addr, ip_str.data(), ip_str.size());
        init_log::critical("network debug: %s SIOCGIFADDR family=%u addr=%s raw=0x%x", NET_IFNAME, static_cast<unsigned>(addr->sin_family),
                           ip_str.data(), static_cast<unsigned>(ntohl(addr->sin_addr.s_addr)));
    } else {
        int const ERR = errno;
        init_log::critical("network debug: %s SIOCGIFADDR failed errno=%d (%s)", NET_IFNAME, ERR, strerror(ERR));
    }

    copy_ifreq_name(ifr, NET_IFNAME);
    if (ioctl(sock, SIOCGIFNETMASK, &ifr) == 0) {
        auto* addr = reinterpret_cast<struct sockaddr_in*>(&ifr.ifr_netmask);
        std::array<char, INET_ADDRSTRLEN> mask_str{};
        inet_ntop(AF_INET, &addr->sin_addr, mask_str.data(), mask_str.size());
        init_log::critical("network debug: %s SIOCGIFNETMASK family=%u mask=%s raw=0x%x", NET_IFNAME,
                           static_cast<unsigned>(addr->sin_family), mask_str.data(), static_cast<unsigned>(ntohl(addr->sin_addr.s_addr)));
    } else {
        int const ERR = errno;
        init_log::critical("network debug: %s SIOCGIFNETMASK failed errno=%d (%s)", NET_IFNAME, ERR, strerror(ERR));
    }
}

void dump_netctl_state() {
    std::array<wos_net_if_info, IF_DEBUG_CAP> ifs{};
    size_t if_count = ifs.size();
    if (wos_net_if_list(ifs.data(), &if_count) != 0) {
        int const ERR = errno;
        init_log::critical("network debug: wos_net_if_list failed errno=%d (%s)", ERR, strerror(ERR));
    } else {
        size_t const WRITTEN = std::min(if_count, ifs.size());
        init_log::critical("network debug: netctl reports %llu interface(s), dumping %llu", static_cast<unsigned long long>(if_count),
                           static_cast<unsigned long long>(WRITTEN));
        for (size_t i = 0; i < WRITTEN; i++) {
            auto const& info = ifs.at(i);
            init_log::critical(
                "network debug: if[%llu] index=%u name=%.*s flags=0x%x mtu=%u txqlen=%u type=%u operstate=%u addr_len=%u "
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
        init_log::critical("network debug: wos_net_addr_list failed errno=%d (%s)", ERR, strerror(ERR));
        return;
    }

    size_t const WRITTEN = std::min(addr_count, addrs.size());
    init_log::critical("network debug: netctl reports %llu address(es), dumping %llu", static_cast<unsigned long long>(addr_count),
                       static_cast<unsigned long long>(WRITTEN));
    for (size_t i = 0; i < WRITTEN; i++) {
        auto const& addr = addrs.at(i);
        if (addr.family == AF_INET) {
            std::array<char, INET_ADDRSTRLEN> local{};
            std::array<char, INET_ADDRSTRLEN> broadcast{};
            inet_ntop(AF_INET, addr.local, local.data(), local.size());
            inet_ntop(AF_INET, addr.broadcast, broadcast.data(), broadcast.size());
            init_log::critical(
                "network debug: addr[%llu] ifindex=%u label=%.*s family=AF_INET prefix=%u scope=%u flags=0x%x local=%s brd=%s",
                static_cast<unsigned long long>(i), addr.ifindex, WOS_NET_IF_NAME_LEN, addr.label, static_cast<unsigned>(addr.prefix_len),
                static_cast<unsigned>(addr.scope), addr.flags, local.data(), broadcast.data());
        } else {
            init_log::critical("network debug: addr[%llu] ifindex=%u label=%.*s family=%u prefix=%u scope=%u flags=0x%x",
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
        init_log::critical("network debug: unable to open %s: errno=%d (%s)", path, ERR, strerror(ERR));
        return;
    }

    std::array<char, NETDEV_STATS_BUF_SIZE> buf{};
    ssize_t const N = ::read(FD, buf.data(), buf.size() - 1);
    ::close(FD);
    if (N < 0) {
        int const ERR = errno;
        init_log::critical("network debug: unable to read %s: errno=%d (%s)", path, ERR, strerror(ERR));
        return;
    }
    if (N == 0) {
        init_log::critical("network debug: %s is empty", path);
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
            init_log::critical("network debug: %s: %.*s", path, static_cast<int>(LINE_LEN), buf.data() + line_start);
        }
        line_start = i + 1;
    }
}

void dump_netdev_stats() {
    dump_netdev_stats_file("/dev/net/eth0");
    dump_netdev_stats_file("/dev/net/eth1");
}

void dump_network_failure_debug(const char* reason, uint64_t netd_pid, const PollDebug* poll, int sock) {
    init_log::critical("network debug: === begin network startup diagnostic dump ===");
    init_log::critical("network debug: reason=%s netd_pid=%llu poll_socket=%d", reason != nullptr ? reason : "(unknown)",
                       static_cast<unsigned long long>(netd_pid), sock);
    if (poll != nullptr) {
        std::array<char, INET_ADDRSTRLEN> last_ip{};
        struct in_addr last_addr{};
        last_addr.s_addr = poll->last_addr;
        inet_ntop(AF_INET, &last_addr, last_ip.data(), last_ip.size());
        init_log::critical(
            "network debug: poll attempts=%llu addr_successes=%llu zero_addr_results=%llu addr_failures=%llu last_addr=%s raw=0x%x "
            "first_diagnostic_secs=%ld",
            static_cast<unsigned long long>(poll->attempts), static_cast<unsigned long long>(poll->addr_successes),
            static_cast<unsigned long long>(poll->addr_zero_results), static_cast<unsigned long long>(poll->addr_failures), last_ip.data(),
            static_cast<unsigned>(ntohl(poll->last_addr)), POLL_FIRST_DIAGNOSTIC_SECS);
        init_log::critical("network debug: poll first_errno=%d (%s)", poll->first_errno,
                           poll->first_errno != 0 ? strerror(poll->first_errno) : "none");
        init_log::critical("network debug: poll last_errno=%d (%s)", poll->last_errno,
                           poll->last_errno != 0 ? strerror(poll->last_errno) : "none");
    }
    dump_ioctl_state(sock);
    dump_netctl_state();
    dump_netdev_stats();
    dump_journal_snapshot();
    init_log::critical("network debug: === end network startup diagnostic dump ===");
}

void terminate_netd_after_startup_timeout(int64_t netd_pid) {
    if (netd_pid <= 0) {
        return;
    }

    (void)ker::process::kill(netd_pid, SIGKILL);
    for (uint32_t retry = 0; retry < NETD_KILL_REAP_RETRIES; retry++) {
        int32_t status = 0;
        int64_t const REAPED = ker::process::waitpid(netd_pid, &status, WNOHANG, nullptr);
        if (REAPED == netd_pid || (REAPED < 0 && REAPED != -EINTR)) {
            return;
        }
        struct timespec const POLL_SLEEP{
            .tv_sec = 0,
            .tv_nsec = POLL_INTERVAL_MS * 1000L * 1000L,
        };
        nanosleep(&POLL_SLEEP, nullptr);
    }
}
}  // namespace

auto start_network() -> bool {
    uint64_t const CPUNO = ker::multiproc::currentThreadId();

    init_log::info("init[%llu]: spawning netd (DHCP daemon)", static_cast<unsigned long long>(CPUNO));
    std::array<const char*, 2> netd_argv = {"/sbin/netd", nullptr};
    InitEnv netd_env = make_init_env();
    uint64_t const NETD_PID = spawn_local_service("/sbin/netd", netd_argv.data(), netd_env.envp.data());
    if (NETD_PID == 0) {
        init_log::error("init[%llu]: failed to spawn netd", static_cast<unsigned long long>(CPUNO));
        dump_network_failure_debug("failed to spawn /sbin/netd", NETD_PID, nullptr, -1);
        return false;
    }
    init_log::info("init[%llu]: netd spawned as PID %llu", static_cast<unsigned long long>(CPUNO),
                   static_cast<unsigned long long>(NETD_PID));

    // Poll eth0 for IP address readiness (wait for DHCP to complete)
    int const POLL_SOCK = socket(AF_INET, SOCK_DGRAM, 0);
    if (POLL_SOCK < 0) {
        int const ERR = errno;
        init_log::error("init[%llu]: failed to create network poll socket: errno=%d (%s)", static_cast<unsigned long long>(CPUNO), ERR,
                        strerror(ERR));
        dump_network_failure_debug("failed to create AF_INET/SOCK_DGRAM poll socket", NETD_PID, nullptr, -1);
        return false;
    }

    struct timespec poll_start{};
    if (clock_gettime(CLOCK_MONOTONIC, &poll_start) != 0) {
        int const ERR = errno;
        init_log::error("init[%llu]: failed to read network poll start time: errno=%d (%s)", static_cast<unsigned long long>(CPUNO), ERR,
                        strerror(ERR));
        dump_network_failure_debug("clock_gettime(CLOCK_MONOTONIC) failed before network poll", NETD_PID, nullptr, POLL_SOCK);
        close(POLL_SOCK);
        return false;
    }

    PollDebug poll{};
    bool diagnostic_dumped = false;
    long next_status_secs = POLL_FIRST_DIAGNOSTIC_SECS + POLL_STATUS_INTERVAL_SECS;
    for (;;) {
        poll.attempts++;

        struct ifreq ifr{};
        copy_ifreq_name(ifr, NET_IFNAME);
        if (ioctl(POLL_SOCK, SIOCGIFADDR, &ifr) == 0) {
            poll.addr_successes++;
            auto* addr = reinterpret_cast<struct sockaddr_in*>(&ifr.ifr_addr);
            poll.last_addr = addr->sin_addr.s_addr;
            if (addr->sin_addr.s_addr != 0) {
                std::array<char, INET_ADDRSTRLEN> ip_str{};
                inet_ntop(AF_INET, &addr->sin_addr, ip_str.data(), ip_str.size());
                init_log::info("init[%llu]: eth0 configured with IP %s", static_cast<unsigned long long>(CPUNO), ip_str.data());
                break;
            }
            poll.addr_zero_results++;
        } else {
            int const ERR = errno;
            poll.addr_failures++;
            if (poll.first_errno == 0) {
                poll.first_errno = ERR;
            }
            poll.last_errno = ERR;
        }

        struct timespec now{};
        if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
            int const ERR = errno;
            init_log::error("init[%llu]: failed to read network poll time: errno=%d (%s)", static_cast<unsigned long long>(CPUNO), ERR,
                            strerror(ERR));
            dump_network_failure_debug("clock_gettime(CLOCK_MONOTONIC) failed during network poll", NETD_PID, &poll, POLL_SOCK);
            close(POLL_SOCK);
            return false;
        }
        long const ELAPSED_SECS = now.tv_sec - poll_start.tv_sec;
        if (!diagnostic_dumped && ELAPSED_SECS >= POLL_FIRST_DIAGNOSTIC_SECS) {
            init_log::warn("init[%llu]: eth0 not configured after polling for %ld seconds; continuing to wait",
                           static_cast<unsigned long long>(CPUNO), POLL_FIRST_DIAGNOSTIC_SECS);
            dump_network_failure_debug("eth0 has not received a non-zero IPv4 address yet; continuing to wait", NETD_PID, &poll, POLL_SOCK);
            diagnostic_dumped = true;
        } else if (diagnostic_dumped && ELAPSED_SECS >= next_status_secs) {
            init_log::warn("init[%llu]: still waiting for eth0 IPv4 configuration after %ld seconds",
                           static_cast<unsigned long long>(CPUNO), ELAPSED_SECS);
            next_status_secs += POLL_STATUS_INTERVAL_SECS;
        }
        if (ELAPSED_SECS >= POLL_FAILURE_TIMEOUT_SECS) {
            init_log::critical("init[%llu]: eth0 not configured after %ld seconds; failing network startup",
                               static_cast<unsigned long long>(CPUNO), POLL_FAILURE_TIMEOUT_SECS);
            dump_network_failure_debug("eth0 did not receive a non-zero IPv4 address before the startup timeout", NETD_PID, &poll,
                                       POLL_SOCK);
            terminate_netd_after_startup_timeout(static_cast<int64_t>(NETD_PID));
            close(POLL_SOCK);
            return false;
        }

        int32_t netd_status = 0;
        auto const NETD_PID_SIGNED = static_cast<int64_t>(NETD_PID);
        int64_t const NETD_REAPED = ker::process::waitpid(NETD_PID_SIGNED, &netd_status, WNOHANG, nullptr);
        if (NETD_REAPED == NETD_PID_SIGNED) {
            init_log::critical("init[%llu]: netd exited before eth0 IPv4 configuration (status=%d)", static_cast<unsigned long long>(CPUNO),
                               netd_status);
            dump_network_failure_debug("netd exited before eth0 received a non-zero IPv4 address", NETD_PID, &poll, POLL_SOCK);
            close(POLL_SOCK);
            return false;
        }

        struct timespec const POLL_SLEEP{
            .tv_sec = 0,
            .tv_nsec = POLL_INTERVAL_MS * 1000L * 1000L,
        };
        nanosleep(&POLL_SLEEP, nullptr);
    }
    close(POLL_SOCK);
    return true;
}
