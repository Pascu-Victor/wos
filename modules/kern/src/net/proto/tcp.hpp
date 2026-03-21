#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <net/netdevice.hpp>
#include <net/packet.hpp>
#include <net/socket.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::proto {

struct TcpHeader {
    uint16_t src_port;
    uint16_t dst_port;
    uint32_t seq;
    uint32_t ack;
    uint8_t data_offset;  // upper 4 bits = header length in 32-bit words
    uint8_t flags;
    uint16_t window;
    uint16_t checksum;
    uint16_t urgent_ptr;
} __attribute__((packed));

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
    PacketBuffer* pkt;
    uint32_t seq;
    size_t len;
    uint64_t send_time_ms;
    uint8_t retries;
    RetransmitEntry* next;
};

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
    uint64_t rto_ms = 200;   // retransmit timeout (ms) — starts at 200 ms floor
    uint64_t srtt_ms = 0;    // smoothed RTT
    uint64_t rttvar_ms = 0;  // RTT variance

    RetransmitEntry* retransmit_head = nullptr;
    RetransmitEntry* retransmit_tail = nullptr;
    uint64_t retransmit_deadline = 0;

    uint64_t time_wait_deadline = 0;

    // Pending ACK retry flag.
    bool ack_pending = false;

    // Delayed ACK state.
    uint8_t segs_pending_ack = 0;       // data segments received since last ACK
    uint64_t delayed_ack_deadline = 0;  // ms timestamp; 0 = no pending delayed ACK

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

void tcp_rx(NetDevice* dev, PacketBuffer* pkt, uint32_t src_ip, uint32_t dst_ip);
void tcp_timer_tick(uint64_t now_ms);
[[noreturn]] void tcp_timer_thread();
void tcp_timer_thread_start();

// Add TCB to timer list; cb->lock must be held.
void tcp_timer_arm(TcpCB* cb);
// Remove TCB from timer list.
void tcp_timer_disarm(TcpCB* cb);
auto get_tcp_proto_ops() -> SocketProtoOps*;

bool tcp_send_segment(TcpCB* cb, uint8_t flags, const void* data, size_t len);
void tcp_send_rst(uint32_t src_ip, uint32_t dst_ip, uint16_t src_port, uint16_t dst_port, uint32_t seq, uint32_t ack, uint8_t flags);
bool tcp_send_ack(TcpCB* cb);

// Build ACK without sending; caller holds cb->lock.
auto tcp_build_ack(TcpCB* cb, uint32_t* out_local, uint32_t* out_remote) -> PacketBuffer*;

void tcp_process_segment(TcpCB* cb, const TcpHeader* hdr, const uint8_t* payload, size_t payload_len, uint32_t src_ip, uint32_t dst_ip);

auto tcp_alloc_cb() -> TcpCB*;
void tcp_insert_cb(TcpCB* cb);
void tcp_insert_listener(TcpCB* cb);
void tcp_remove_listener(TcpCB* cb);
void tcp_free_cb(TcpCB* cb);
void tcp_cb_acquire(TcpCB* cb);
void tcp_cb_release(TcpCB* cb);
auto tcp_find_cb(uint32_t local_ip, uint16_t local_port, uint32_t remote_ip, uint16_t remote_port) -> TcpCB*;
auto tcp_find_listener(uint32_t local_ip, uint16_t local_port) -> TcpCB*;

inline bool tcp_seq_before(uint32_t a, uint32_t b) { return static_cast<int32_t>(a - b) < 0; }
inline bool tcp_seq_after(uint32_t a, uint32_t b) { return tcp_seq_before(b, a); }
inline bool tcp_seq_between(uint32_t seq, uint32_t low, uint32_t high) { return !tcp_seq_before(seq, low) && tcp_seq_before(seq, high); }

// Minimum shift so advertised window fits in 16 bits.
constexpr uint8_t tcp_wscale_for_buf(size_t buf_size) {
    uint8_t s = 0;
    while (s < 14 && (buf_size >> s) > 65535U) {
        ++s;
    }
    return s;
}

auto tcp_now_ms() -> uint64_t;

}  // namespace ker::net::proto
