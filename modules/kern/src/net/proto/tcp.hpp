#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <net/address.hpp>
#include <net/netdevice.hpp>
#include <net/packet.hpp>
#include <net/socket.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::proto {

struct TcpHeader {
    uint16_t src_port{};
    uint16_t dst_port{};
    uint32_t seq{};
    uint32_t ack{};
    uint8_t data_offset{};  // upper 4 bits = header length in 32-bit words
    uint8_t flags{};
    uint16_t window{};
    uint16_t checksum{};
    uint16_t urgent_ptr{};
} __attribute__((packed));
static_assert(sizeof(TcpHeader) == 20);

constexpr uint8_t TCP_FIN = 0x01;
constexpr uint8_t TCP_SYN = 0x02;
constexpr uint8_t TCP_RST = 0x04;
constexpr uint8_t TCP_PSH = 0x08;
constexpr uint8_t TCP_ACK = 0x10;
constexpr uint8_t TCP_URG = 0x20;

enum class TcpState : uint8_t {
    CLOSED,
    LISTEN,
    SYN_SENT,
    SYN_RECEIVED,
    ESTABLISHED,
    FIN_WAIT_1,
    FIN_WAIT_2,
    CLOSE_WAIT,
    CLOSING,
    LAST_ACK,
    TIME_WAIT,
};

struct RetransmitEntry {
    PacketBuffer* pkt{};
    uint32_t seq{};
    size_t len{};
    uint64_t send_time_ms{};
    uint8_t retries{};
    RetransmitEntry* next{};
};

struct TcpOutOfOrderSegment {
    uint32_t seq{};
    size_t len{};
    size_t allocated_len{};
    uint8_t* data{};
    TcpOutOfOrderSegment* next{};
};

constexpr uint8_t TCP_KEEPALIVE_PROBES_DEFAULT = 9;
constexpr uint64_t TCP_KEEPALIVE_IDLE_MS_DEFAULT = 7200000;  // 2 hours
constexpr uint64_t TCP_KEEPALIVE_INTVL_MS_DEFAULT = 75000;   // 75 seconds
// Per-send syscall burst before returning a partial count; not an in-flight cap.
constexpr uint32_t TCP_SEND_BURST_BYTES = 1024U * 1024U;

struct TcpCB {
    TcpState state = TcpState::CLOSED;

    // Reference count.
    std::atomic<uint32_t> refcnt = 1;

    uint32_t local_ip = 0;
    uint32_t remote_ip = 0;
    uint16_t local_port = 0;
    uint16_t remote_port = 0;

    uint32_t snd_una = 0;  // oldest unACKed sequence number
    uint32_t snd_nxt = 0;  // next sequence number to send
    uint32_t snd_wnd = 0;  // send window
    uint32_t iss = 0;      // initial send sequence number

    uint32_t rcv_nxt = 0;  // next expected receive sequence number
    uint32_t rcv_wnd = 0;  // receive window
    uint32_t irs = 0;      // initial receive sequence number

    uint16_t snd_mss = 536;   // default MSS (536 for IPv4 without options)
    uint16_t rcv_mss = 1460;  // our MSS (1500 - 20 IP - 20 TCP)

    // Window scaling.
    uint8_t rcv_wscale = 0;   // shift we advertise in SYN/SYN-ACK
    uint8_t snd_wscale = 0;   // shift peer advertised (parsed from SYN/SYN-ACK)
    bool ws_enabled = false;  // true once both sides have negotiated scaling

    // RTT estimation.
    uint64_t rto_ms = 200;   // retransmit timeout (ms) - starts at 200 ms floor
    uint64_t srtt_ms = 0;    // smoothed RTT
    uint64_t rttvar_ms = 0;  // RTT variance

    RetransmitEntry* retransmit_head = nullptr;
    RetransmitEntry* retransmit_tail = nullptr;
    uint64_t retransmit_deadline = 0;

    TcpOutOfOrderSegment* ooo_head = nullptr;
    size_t ooo_allocated_bytes = 0;
    std::atomic<size_t> ooo_bytes{0};

    uint64_t time_wait_deadline = 0;

    // Pending ACK retry flag.
    bool ack_pending = false;

    // Delayed ACK state.
    uint8_t segs_pending_ack = 0;       // data segments received since last ACK
    uint64_t delayed_ack_deadline = 0;  // ms timestamp; 0 = no pending delayed ACK

    // Keepalive state.
    bool keepalive_enabled = false;
    uint8_t keepalive_count = 0;  // probes sent without reply
    uint8_t keepalive_probes_max = TCP_KEEPALIVE_PROBES_DEFAULT;
    uint64_t keepalive_idle_ms = TCP_KEEPALIVE_IDLE_MS_DEFAULT;
    uint64_t keepalive_intvl_ms = TCP_KEEPALIVE_INTVL_MS_DEFAULT;
    uint64_t keepalive_deadline = 0;  // next probe time; 0 = not armed

    Socket* socket = nullptr;

    TcpCB* next = nullptr;

    // Hash bucket chain.
    TcpCB* hash_next = nullptr;

    // Timer list chain.
    TcpCB* timer_next = nullptr;
    bool on_timer_list = false;

    ker::mod::sys::Spinlock lock;
};

constexpr size_t TCB_HASH_SIZE = 256;
constexpr size_t LISTENER_HASH_SIZE = 64;

struct TcpHashBucket {
    TcpCB* head = nullptr;
    ker::mod::sys::Spinlock lock;
};

inline auto tcp_hash_4tuple(uint32_t lip, uint16_t lp, uint32_t rip, uint16_t rp) -> uint32_t {
    // Exclude local_ip so INADDR_ANY lookups and inserts share buckets.
    (void)lip;
    uint32_t h = rip ^ (static_cast<uint32_t>(lp) << 16 | rp);
    h *= 0x9e3779b9U;
    return h;
}

inline auto tcp_hash_listener(uint16_t lp) -> uint32_t { return static_cast<uint32_t>(lp) * 0x9e3779b9U; }

constexpr size_t MAX_TCP_BINDINGS = 128;
constexpr size_t TCP_LISTENER_SNAPSHOT_MAX = 64;

struct TcpListenerSnapshot {
    uint32_t local_ip = 0;
    uint16_t local_port = 0;
    uint8_t state = 0;
    uint64_t owner_pid = 0;
    size_t accept_queue = 0;
    int backlog = 0;
    size_t rcvbuf_used = 0;
    size_t rcvbuf_capacity = 0;
    uint32_t rcv_wnd = 0;
    uint32_t refcount = 0;
};

void tcp_rx(NetDevice* dev, PacketBuffer* pkt, IPv4Address src_ip, IPv4Address dst_ip);
void tcp_timer_tick(uint64_t now_ms);
[[noreturn]] void tcp_timer_thread();
void tcp_timer_thread_start();

// Add TCB to timer list; cb->lock must be held.
void tcp_timer_arm(TcpCB* cb);
// Remove TCB from timer list.
void tcp_timer_disarm(TcpCB* cb);
auto get_tcp_proto_ops() -> SocketProtoOps*;

auto tcp_send_segment(TcpCB* cb, uint8_t flags, const void* data, size_t len) -> bool;
void tcp_send_rst(IPv4Address src_ip, IPv4Address dst_ip, uint16_t src_port, uint16_t dst_port, uint32_t seq, uint32_t ack, uint8_t flags);
auto tcp_send_ack(TcpCB* cb) -> bool;

// Build ACK without sending; caller holds cb->lock.
auto tcp_build_ack(TcpCB* cb, uint32_t* out_local, uint32_t* out_remote) -> PacketBuffer*;
// Build a keepalive probe (ACK with seq = snd_una - 1); caller holds cb->lock.
auto tcp_build_keepalive_probe(TcpCB* cb, uint32_t* out_local, uint32_t* out_remote) -> PacketBuffer*;

void tcp_process_segment(TcpCB* cb, const TcpHeader* hdr, const uint8_t* payload, size_t payload_len, IPv4Address src_ip,
                         IPv4Address dst_ip);

auto tcp_alloc_cb() -> TcpCB*;
void tcp_insert_cb(TcpCB* cb);
void tcp_insert_listener(TcpCB* cb);
void tcp_remove_listener(TcpCB* cb);
void tcp_free_cb(TcpCB* cb);
void tcp_cb_acquire(TcpCB* cb);
void tcp_cb_release(TcpCB* cb);
auto tcp_find_cb(uint32_t local_ip, uint16_t local_port, uint32_t remote_ip, uint16_t remote_port) -> TcpCB*;
auto tcp_find_listener(uint32_t local_ip, uint16_t local_port) -> TcpCB*;
auto tcp_listener_snapshot(TcpListenerSnapshot* out, size_t max) -> size_t;

inline auto tcp_seq_before(uint32_t a, uint32_t b) -> bool { return static_cast<int32_t>(a - b) < 0; }
inline auto tcp_seq_after(uint32_t a, uint32_t b) -> bool { return tcp_seq_before(b, a); }
inline auto tcp_seq_between(uint32_t seq, uint32_t low, uint32_t high) -> bool {
    return !tcp_seq_before(seq, low) && tcp_seq_before(seq, high);
}

inline auto tcp_send_available_bytes(const TcpCB* cb) -> uint32_t {
    if (cb == nullptr) {
        return 0;
    }

    uint32_t const IN_FLIGHT = cb->snd_nxt - cb->snd_una;
    if (IN_FLIGHT >= cb->snd_wnd) {
        return 0;
    }
    return cb->snd_wnd - IN_FLIGHT;
}

constexpr auto tcp_saturating_add_ms(uint64_t lhs_ms, uint64_t rhs_ms) -> uint64_t {
    if (UINT64_MAX - lhs_ms < rhs_ms) {
        return UINT64_MAX;
    }
    return lhs_ms + rhs_ms;
}

constexpr auto tcp_deadline_after_ms(uint64_t now_ms, uint64_t delay_ms) -> uint64_t { return tcp_saturating_add_ms(now_ms, delay_ms); }

inline auto tcp_receive_window_space(const TcpCB* cb, const Socket* sock) -> uint32_t {
    if (sock == nullptr) {
        return 0;
    }

    size_t const FREE_SPACE = sock->rcvbuf.free_space();
    size_t const OOO_BYTES = cb != nullptr ? cb->ooo_bytes.load(std::memory_order_acquire) : 0;
    return static_cast<uint32_t>(FREE_SPACE > OOO_BYTES ? FREE_SPACE - OOO_BYTES : 0);
}

inline void tcp_refresh_receive_window(TcpCB* cb) {
    if (cb != nullptr && cb->socket != nullptr) {
        cb->rcv_wnd = tcp_receive_window_space(cb, cb->socket);
    }
}

// Minimum shift so advertised window fits in 16 bits.
constexpr auto tcp_wscale_for_buf(size_t buf_size) -> uint8_t {
    uint8_t s = 0;
    while (s < 14 && (buf_size >> s) > 65535U) {
        ++s;
    }
    return s;
}

auto tcp_now_ms() -> uint64_t;

}  // namespace ker::net::proto
