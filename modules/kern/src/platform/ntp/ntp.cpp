#include "ntp.hpp"

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
constexpr int RECEIVE_POLL_LIMIT = 200;
constexpr int MAX_ATTEMPTS = 6;  // 6 * 5 s = 30 s total
constexpr int RETRY_DELAY_MS = 5000;
constexpr uint16_t NTP_PORT = 123;
// Seconds between NTP epoch (1900-01-01) and Unix epoch (1970-01-01).
constexpr uint64_t NTP_EPOCH_DELTA = 2208988800ULL;

// NTP server - local router (10.10.0.1).
constexpr std::array<uint32_t, 1> NTP_SERVERS{
    (10U << 24) | (10U << 16) | (0U << 8) | 1U,  // 10.10.0.1 (local router)
};

// ---------------------------------------------------------------------------
// Sync implementation
// ---------------------------------------------------------------------------

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

    if (sock->proto_ops == nullptr || sock->proto_ops->sendto == nullptr || sock->proto_ops->recv == nullptr) {
        ker::net::socket_destroy(sock);
        return false;
    }

    ssize_t const SENT = sock->proto_ops->sendto(sock, req.data(), req.size(), 0, remote.data(), remote.size());
    if (std::cmp_not_equal(SENT, req.size())) {
        ker::net::socket_destroy(sock);
        return false;
    }

    // Poll the receive ring buffer for up to ~1 second (200 yields * ~5 ms each).
    std::array<uint8_t, NTP_PACKET_LEN> resp{};
    bool got = false;
    for (int r = 0; r < RECEIVE_POLL_LIMIT && !got; ++r) {
        ssize_t const N = sock->proto_ops->recv(sock, resp.data(), resp.size(), 0);
        if (std::cmp_equal(N, resp.size())) {
            got = true;
        } else {
            ker::mod::sched::kern_yield();
        }
    }

    ker::net::socket_destroy(sock);

    if (!got) {
        return false;
    }

    // Transmit Timestamp is at bytes 40–47 (big-endian seconds + fraction).
    uint32_t const NTP_SEC = (static_cast<uint32_t>(resp.at(40)) << 24) | (static_cast<uint32_t>(resp.at(41)) << 16) |
                             (static_cast<uint32_t>(resp.at(42)) << 8) | static_cast<uint32_t>(resp.at(43));

    if (NTP_SEC == 0) {
        return false;  // server didn't fill in the transmit timestamp
    }

    int64_t const UNIX_SEC = static_cast<int64_t>(NTP_SEC) - static_cast<int64_t>(NTP_EPOCH_DELTA);
    auto const RTC_NOW = static_cast<int64_t>(rtc::get_epoch_sec());
    int64_t const DELTA = UNIX_SEC - RTC_NOW;

    rtc::set_offset(DELTA);
    log::info("synced to %lu.%lu.%lu.%lu - offset %ld s", static_cast<unsigned long>((server_ip >> 24) & 0xFF),
              static_cast<unsigned long>((server_ip >> 16) & 0xFF), static_cast<unsigned long>((server_ip >> 8) & 0xFF),
              static_cast<unsigned long>(server_ip & 0xFF), static_cast<long>(DELTA));
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
        time::sleep(RETRY_DELAY_MS);

        for (uint32_t const SERVER_IP : NTP_SERVERS) {
            if (try_sync(SERVER_IP)) {
                log::info("sync successful on attempt %d", attempt + 1);
                terminate_ntp_thread();
            }
        }

        log::debug("sync attempt %d/%d failed - retrying", attempt + 1, MAX_ATTEMPTS);
    }

    log::warn("all sync attempts exhausted; using RTC wall clock only");
    terminate_ntp_thread();
}

}  // namespace

// ---------------------------------------------------------------------------
// Public init
// ---------------------------------------------------------------------------

void init() {
    auto* task = ker::mod::sched::task::Task::create_kernel_thread("ntp_sync", ntp_sync_thread);
    if (task == nullptr) {
        log::error("failed to create sync thread (OOM)");
        return;
    }
    ker::mod::sched::post_task_balanced(task);
    log::info("sync thread started (PID %d)", static_cast<int>(task->pid));
}

}  // namespace ker::mod::ntp
