#include "ntp.hpp"
#ifdef WOS_SELFTEST
#include <test/compartment_lifecycle_runtime.hpp>
#endif

#include <bits/ssize_t.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <net/socket.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/rtc/rtc.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <syscalls_impl/process/exit.hpp>
#include <utility>

namespace ker::mod::ntp {

namespace {

using log = ker::mod::dbg::logger<"ntp">;

[[noreturn]] void terminate_ntp_thread() {
    ker::syscall::process::wos_proc_exit(0);
    for (;;) {
        asm volatile("hlt");
    }
}

// ---------------------------------------------------------------------------
// SNTP (RFC 4330) constants
// ---------------------------------------------------------------------------

constexpr size_t NTP_PACKET_LEN = 48;
constexpr size_t SOCKADDR_V4_LEN = 16;
constexpr uint64_t RECEIVE_TIMEOUT_US = 1000000;
constexpr int MAX_ATTEMPTS = 6;  // 6 * 5 s = 30 s total
constexpr int RETRY_DELAY_MS = 5000;
constexpr uint16_t NTP_PORT = 123;
constexpr uint64_t NS_PER_SEC = 1000000000ULL;
// Seconds between NTP epoch (1900-01-01) and Unix epoch (1970-01-01).
constexpr uint64_t NTP_EPOCH_DELTA = 2208988800ULL;
constexpr int64_t MAX_SERVER_SKEW_NS = 24LL * 60LL * 60LL * static_cast<int64_t>(NS_PER_SEC);

// NTP server - local router (10.10.0.1).
constexpr std::array<uint32_t, 1> NTP_SERVERS{
    (10U << 24) | (10U << 16) | (0U << 8) | 1U,  // 10.10.0.1 (local router)
};

// ---------------------------------------------------------------------------
// Sync implementation
// ---------------------------------------------------------------------------

auto read_be32(const uint8_t* data) -> uint32_t {
    return (static_cast<uint32_t>(data[0]) << 24) | (static_cast<uint32_t>(data[1]) << 16) | (static_cast<uint32_t>(data[2]) << 8) |
           static_cast<uint32_t>(data[3]);
}

void write_be32(uint8_t* data, uint32_t value) {
    data[0] = static_cast<uint8_t>(value >> 24);
    data[1] = static_cast<uint8_t>(value >> 16);
    data[2] = static_cast<uint8_t>(value >> 8);
    data[3] = static_cast<uint8_t>(value);
}

auto unix_ns_to_ntp(uint64_t unix_ns, uint8_t* output) -> bool {
    uint64_t const UNIX_SEC = unix_ns / NS_PER_SEC;
    uint64_t const NTP_SEC = UNIX_SEC + NTP_EPOCH_DELTA;
    if (NTP_SEC > UINT32_MAX) {
        return false;
    }
    uint64_t const UNIX_FRACTION_NS = unix_ns % NS_PER_SEC;
    auto const NTP_FRACTION = static_cast<uint32_t>((UNIX_FRACTION_NS << 32U) / NS_PER_SEC);
    write_be32(output, static_cast<uint32_t>(NTP_SEC));
    write_be32(output + 4, NTP_FRACTION);
    return true;
}

auto ntp_to_unix_ns(const uint8_t* input, int64_t* output) -> bool {
    uint32_t const NTP_SEC = read_be32(input);
    if (NTP_SEC < NTP_EPOCH_DELTA) {
        return false;
    }
    uint32_t const NTP_FRACTION = read_be32(input + 4);
    uint64_t const UNIX_SEC = static_cast<uint64_t>(NTP_SEC) - NTP_EPOCH_DELTA;
    uint64_t const FRACTION_NS = (static_cast<uint64_t>(NTP_FRACTION) * NS_PER_SEC) >> 32U;
    uint64_t const UNIX_NS = (UNIX_SEC * NS_PER_SEC) + FRACTION_NS;
    if (UNIX_NS > static_cast<uint64_t>(INT64_MAX)) {
        return false;
    }
    *output = static_cast<int64_t>(UNIX_NS);
    return true;
}

auto timestamps_are_near(int64_t lhs, int64_t rhs) -> bool {
    if (lhs < 0 || rhs < 0) return false;
    return (lhs >= rhs ? lhs - rhs : rhs - lhs) <= MAX_SERVER_SKEW_NS;
}

// Attempt to synchronise with a single NTP server.
// Returns true if sync succeeded and the RTC offset has been updated.
auto try_sync(uint32_t server_ip) -> bool {
    auto* sock = ker::net::socket_create(2 /*AF_INET*/, 2 /*SOCK_DGRAM*/, 0);
    if (sock == nullptr) {
        return false;
    }

    // Build destination sockaddr: family(2B) + port(2B) + addr(4B) + padding(8B)
    std::array<uint8_t, SOCKADDR_V4_LEN> remote{};
    ker::net::socket_fill_sockaddr_v4(remote.data(), remote.size(), nullptr, server_ip, NTP_PORT);

    // Build SNTP client request (48 bytes, all zero except first byte).
    // Byte 0: LI=0, VN=4, Mode=3 (client) -> 0b 00 100 011 = 0x23
    std::array<uint8_t, NTP_PACKET_LEN> req{};
    req.at(0) = 0x23;

    uint64_t const CLIENT_TX_NS = rtc::get_epoch_ns();
    if (!unix_ns_to_ntp(CLIENT_TX_NS, req.data() + 40)) {
        ker::net::socket_destroy(sock);
        return false;
    }

    if (sock->proto_ops == nullptr || sock->proto_ops->sendto == nullptr || sock->proto_ops->recv == nullptr) {
        ker::net::socket_destroy(sock);
        return false;
    }

    ssize_t const SENT = sock->proto_ops->sendto(sock, req.data(), req.size(), 0, remote.data(), remote.size());
    if (std::cmp_not_equal(SENT, req.size())) {
        ker::net::socket_destroy(sock);
        return false;
    }

    // Use elapsed monotonic time, not a scheduler-dependent yield count.
    std::array<uint8_t, NTP_PACKET_LEN> resp{};
    bool got = false;
    uint64_t receive_epoch_ns = 0;
    auto const DEADLINE = time::arithmetic::saturating_add(time::get_us(), RECEIVE_TIMEOUT_US);
    while (!got && time::get_us() < DEADLINE) {
        ssize_t const N = sock->proto_ops->recv(sock, resp.data(), resp.size(), 0);
        if (std::cmp_equal(N, resp.size())) {
            receive_epoch_ns = rtc::get_epoch_ns();
            got = true;
        } else {
            ker::mod::sched::kern_sleep_us(1000);
        }
    }

    ker::net::socket_destroy(sock);

    if (!got) {
        return false;
    }

    uint64_t const CLIENT_RX_NS = receive_epoch_ns;

    auto const LEAP = resp.at(0) >> 6;
    auto const VERSION = (resp.at(0) >> 3) & 7U;
    auto const MODE = resp.at(0) & 7U;
    if (LEAP == 3 || (VERSION != 3 && VERSION != 4) || MODE != 4 || resp.at(1) == 0 || resp.at(1) > 15 ||
        CLIENT_TX_NS > static_cast<uint64_t>(INT64_MAX) || CLIENT_RX_NS > static_cast<uint64_t>(INT64_MAX) || CLIENT_RX_NS < CLIENT_TX_NS ||
        CLIENT_RX_NS - CLIENT_TX_NS > RECEIVE_TIMEOUT_US * 1000) {
        return false;
    }

    // A server must echo the client's transmit timestamp as its originate
    // timestamp. Reject unrelated or stale UDP responses.
    for (size_t i = 0; i < 8; ++i) {
        if (resp.at(24 + i) != req.at(40 + i)) {
            return false;
        }
    }

    int64_t server_rx_ns = 0;
    int64_t server_tx_ns = 0;
    auto const CLIENT_TX = static_cast<int64_t>(CLIENT_TX_NS);
    auto const CLIENT_RX = static_cast<int64_t>(CLIENT_RX_NS);
    if (!ntp_to_unix_ns(resp.data() + 32, &server_rx_ns) || !ntp_to_unix_ns(resp.data() + 40, &server_tx_ns) ||
        !timestamps_are_near(server_rx_ns, CLIENT_RX) || !timestamps_are_near(server_tx_ns, CLIENT_RX)) {
        return false;
    }

    // Standard NTP offset: ((T2 - T1) + (T3 - T4)) / 2.
    int64_t const OFFSET_NS = ((server_rx_ns - CLIENT_TX) + (server_tx_ns - CLIENT_RX)) / 2;
    int64_t const ROUND_TRIP_NS = (CLIENT_RX - CLIENT_TX) - (server_tx_ns - server_rx_ns);
    if (ROUND_TRIP_NS < 0 || ROUND_TRIP_NS > static_cast<int64_t>(RECEIVE_TIMEOUT_US * 1000) || server_tx_ns < server_rx_ns ||
        !rtc::adjust_offset_ns(OFFSET_NS)) {
        return false;
    }
    log::info("synced to %lu.%lu.%lu.%lu - offset %ld ns, round-trip %ld ns", static_cast<unsigned long>((server_ip >> 24) & 0xFF),
              static_cast<unsigned long>((server_ip >> 16) & 0xFF), static_cast<unsigned long>((server_ip >> 8) & 0xFF),
              static_cast<unsigned long>(server_ip & 0xFF), static_cast<long>(OFFSET_NS), static_cast<long>(ROUND_TRIP_NS));
    return true;
}

// ---------------------------------------------------------------------------
// Kernel thread body
// ---------------------------------------------------------------------------

void ntp_sync_thread() {
    // Wait up to 30 seconds for IPv4 routing to be configured by userspace
    // (DHCP client or static ifconfig).  We probe by trying each server
    // every 5 seconds.  If no sync succeeds we give up gracefully.
    for (int attempt = 0; attempt < MAX_ATTEMPTS; ++attempt) {
        // Sleep 5 s between attempts so the network has time to come up.
        // (On the first pass this also lets drivers finish their init.)
        ker::mod::sched::kern_sleep_us(static_cast<uint64_t>(RETRY_DELAY_MS) * 1000);

        for (uint32_t const SERVER_IP : NTP_SERVERS) {
            if (try_sync(SERVER_IP)) {
                log::info("sync successful on attempt %d", attempt + 1);
                // Subsequent measurements replace the remaining slew target;
                // only the first accepted correction may step the wall clock.
#ifdef WOS_SELFTEST
                ker::test::lifecycle60::ntp_boot_periodic();
#endif
                for (;;) {
                    ker::mod::sched::kern_sleep_us(60000000);
                    if (!try_sync(SERVER_IP)) log::debug("periodic sync failed; retaining disciplined wall clock");
                }
            }
        }

        log::debug("sync attempt %d/%d failed - retrying", attempt + 1, MAX_ATTEMPTS);
    }

    log::warn("all sync attempts exhausted; using RTC wall clock only");
#ifdef WOS_SELFTEST
    ker::test::lifecycle60::ntp_boot_exiting();
#endif
    terminate_ntp_thread();
}

}  // namespace

// ---------------------------------------------------------------------------
// Public init
// ---------------------------------------------------------------------------

void init() {
#ifdef WOS_SELFTEST
    ker::test::lifecycle60::ntp_boot_initializing();
#endif
    auto* task = ker::mod::sched::task::Task::create_kernel_thread("ntp_sync", ntp_sync_thread);
    if (task == nullptr) {
        log::error("failed to create sync thread (OOM)");
#ifdef WOS_SELFTEST
        ker::test::lifecycle60::ntp_boot_creation_failed();
#endif
        return;
    }
#ifdef WOS_SELFTEST
    uint64_t const PUBLISHED_PID = task->pid;  // Caller still owns the unpublished Task.
    ker::test::lifecycle60::ntp_boot_will_post(*task);
    bool const POSTED = ker::mod::sched::post_task_balanced(task);
    ker::test::lifecycle60::ntp_boot_posted(POSTED);
    if (!POSTED) {
        // Failed publication leaves this exact private Task with its caller.
        ker::mod::sched::task::destroy_unpublished_user_thread(task);
        return;
    }
#else
    ker::mod::sched::post_task_balanced(task);
#endif
#ifdef WOS_SELFTEST
    log::info("sync thread started (PID %d)", static_cast<int>(PUBLISHED_PID));
#else
    log::info("sync thread started (PID %d)", static_cast<int>(task->pid));
#endif
}

}  // namespace ker::mod::ntp
