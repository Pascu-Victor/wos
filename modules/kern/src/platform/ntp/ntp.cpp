#include "ntp.hpp"

#include <cstring>
#include <net/endian.hpp>
#include <net/socket.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/rtc/rtc.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <syscalls_impl/process/exit.hpp>

namespace ker::mod::ntp {

[[noreturn]] static void terminate_ntp_thread() {
    ker::syscall::process::wos_proc_exit(0);
    for (;;) {
        asm volatile("hlt");
    }
}

// ---------------------------------------------------------------------------
// SNTP (RFC 4330) constants
// ---------------------------------------------------------------------------

static constexpr size_t NTP_PACKET_LEN = 48;
static constexpr uint16_t NTP_PORT = 123;
// Seconds between NTP epoch (1900-01-01) and Unix epoch (1970-01-01).
static constexpr uint64_t NTP_EPOCH_DELTA = 2208988800ULL;

// NTP server — local router (10.10.0.1).
static constexpr uint32_t NTP_SERVERS[] = {
    (10U << 24) | (10U << 16) | (0U << 8) | 1U,  // 10.10.0.1 (local router)
};
static constexpr size_t NTP_SERVER_COUNT = sizeof(NTP_SERVERS) / sizeof(NTP_SERVERS[0]);

// ---------------------------------------------------------------------------
// Sync implementation
// ---------------------------------------------------------------------------

// Attempt to synchronise with a single NTP server.
// Returns true if sync succeeded and the RTC offset has been updated.
static bool try_sync(uint32_t server_ip) {
    auto* sock = ker::net::socket_create(2 /*AF_INET*/, 2 /*SOCK_DGRAM*/, 0);
    if (sock == nullptr) {
        return false;
    }

    // Build destination sockaddr: family(2B) + port(2B) + addr(4B) + padding(8B)
    uint8_t remote[16] = {};
    *reinterpret_cast<uint16_t*>(remote + 0) = 2;                           // AF_INET
    *reinterpret_cast<uint16_t*>(remote + 2) = ker::net::htons(NTP_PORT);   // network byte order
    *reinterpret_cast<uint32_t*>(remote + 4) = ker::net::htonl(server_ip);  // network byte order

    // Build SNTP client request (48 bytes, all zero except first byte).
    // Byte 0: LI=0, VN=4, Mode=3 (client) → 0b 00 100 011 = 0x23
    uint8_t req[NTP_PACKET_LEN] = {};
    req[0] = 0x23;

    if (sock->proto_ops == nullptr || sock->proto_ops->sendto == nullptr) {
        ker::net::socket_destroy(sock);
        return false;
    }

    ssize_t sent = sock->proto_ops->sendto(sock, req, NTP_PACKET_LEN, 0, remote, sizeof(remote));
    if (sent != static_cast<ssize_t>(NTP_PACKET_LEN)) {
        ker::net::socket_destroy(sock);
        return false;
    }

    // Poll the receive ring buffer for up to ~1 second (200 yields * ~5 ms each).
    uint8_t resp[NTP_PACKET_LEN] = {};
    bool got = false;
    for (int r = 0; r < 200 && !got; ++r) {
        ssize_t n = sock->proto_ops->recv(sock, resp, NTP_PACKET_LEN, 0);
        if (n == static_cast<ssize_t>(NTP_PACKET_LEN)) {
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
    uint32_t ntp_sec = (static_cast<uint32_t>(resp[40]) << 24) | (static_cast<uint32_t>(resp[41]) << 16) |
                       (static_cast<uint32_t>(resp[42]) << 8) | static_cast<uint32_t>(resp[43]);

    if (ntp_sec == 0) {
        return false;  // server didn't fill in the transmit timestamp
    }

    int64_t unix_sec = static_cast<int64_t>(ntp_sec) - static_cast<int64_t>(NTP_EPOCH_DELTA);
    int64_t rtc_now = static_cast<int64_t>(rtc::getEpochSec());
    int64_t delta = unix_sec - rtc_now;

    rtc::setOffset(delta);
    dbg::log("ntp: synced to %lu.%lu.%lu.%lu — offset %ld s", (unsigned long)((server_ip >> 24) & 0xFF),
             (unsigned long)((server_ip >> 16) & 0xFF), (unsigned long)((server_ip >> 8) & 0xFF), (unsigned long)(server_ip & 0xFF),
             (long)delta);
    return true;
}

// ---------------------------------------------------------------------------
// Kernel thread body
// ---------------------------------------------------------------------------

static void ntp_sync_thread() {
    // Wait up to 30 seconds for IPv4 routing to be configured by userspace
    // (DHCP client or static ifconfig).  We probe by trying each server
    // every 5 seconds.  If no sync succeeds we give up gracefully.
    constexpr int MAX_ATTEMPTS = 6;  // 6 * 5 s = 30 s total
    for (int attempt = 0; attempt < MAX_ATTEMPTS; ++attempt) {
        // Sleep 5 s between attempts so the network has time to come up.
        // (On the first pass this also lets drivers finish their init.)
        time::sleep(5000);

        for (unsigned int i : NTP_SERVERS) {
            if (try_sync(i)) {
                dbg::log("ntp: sync successful on attempt %d", attempt + 1);
                terminate_ntp_thread();
            }
        }

        dbg::log("ntp: sync attempt %d/%d failed — retrying", attempt + 1, MAX_ATTEMPTS);
    }

    dbg::log("ntp: all sync attempts exhausted; using RTC wall clock only");
    terminate_ntp_thread();
}

// ---------------------------------------------------------------------------
// Public init
// ---------------------------------------------------------------------------

void init() {
    auto* task = ker::mod::sched::task::Task::createKernelThread("ntp_sync", ntp_sync_thread);
    if (task == nullptr) {
        dbg::log("ntp: failed to create sync thread (OOM)");
        return;
    }
    ker::mod::sched::post_task_balanced(task);
    dbg::log("ntp: sync thread started (PID %d)", (int)task->pid);
}

}  // namespace ker::mod::ntp
