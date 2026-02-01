#pragma once

#include <cstddef>
#include <cstdint>
#include <net/netdevice.hpp>
#include <net/packet.hpp>
#include <net/socket.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::proto {

// TCP header
struct TcpHeader {
    uint16_t src_port;
    uint16_t dst_port;
    uint32_t seq;
    uint32_t ack;
    uint8_t data_offset;   // upper 4 bits = header length in 32-bit words
    uint8_t flags;
    uint16_t window;
    uint16_t checksum;
    uint16_t urgent_ptr;
} __attribute__((packed));

// TCP flags
constexpr uint8_t TCP_FIN = 0x01;
constexpr uint8_t TCP_SYN = 0x02;
constexpr uint8_t TCP_RST = 0x04;
constexpr uint8_t TCP_PSH = 0x08;
constexpr uint8_t TCP_ACK = 0x10;
constexpr uint8_t TCP_URG = 0x20;

// TCP states (RFC 793)
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

// Retransmit queue entry
struct RetransmitEntry {
    PacketBuffer* pkt;
    uint32_t seq;
    size_t len;
    uint64_t send_time_ms;
    uint8_t retries;
    RetransmitEntry* next;
};

// TCP Control Block (per-connection state)
struct TcpCB {
    TcpState state = TcpState::CLOSED;

    // Local/remote endpoints
    uint32_t local_ip = 0;
    uint32_t remote_ip = 0;
    uint16_t local_port = 0;
    uint16_t remote_port = 0;

    // Send sequence space
    uint32_t snd_una = 0;     // oldest unACKed sequence number
    uint32_t snd_nxt = 0;     // next sequence number to send
    uint32_t snd_wnd = 0;     // send window
    uint32_t iss = 0;         // initial send sequence number

    // Receive sequence space
    uint32_t rcv_nxt = 0;     // next expected receive sequence number
    uint32_t rcv_wnd = 0;     // receive window
    uint32_t irs = 0;         // initial receive sequence number

    // MSS
    uint16_t snd_mss = 536;   // default MSS (536 for IPv4 without options)
    uint16_t rcv_mss = 1460;  // our MSS (1500 - 20 IP - 20 TCP)

    // Congestion control (simple)
    uint32_t cwnd = 1460;     // congestion window
    uint32_t ssthresh = 65535;

    // RTT estimation (Jacobson/Karels)
    uint64_t rto_ms = 1000;   // retransmit timeout (ms)
    uint64_t srtt_ms = 0;     // smoothed RTT
    uint64_t rttvar_ms = 0;   // RTT variance

    // Retransmit queue
    RetransmitEntry* retransmit_head = nullptr;
    uint64_t retransmit_deadline = 0;

    // TIME_WAIT timer
    uint64_t time_wait_deadline = 0;

    // Back-pointer to socket
    Socket* socket = nullptr;

    // Linked list for global TCB tracking
    TcpCB* next = nullptr;

    ker::mod::sys::Spinlock lock;
};

constexpr size_t MAX_TCP_BINDINGS = 128;

// TCP protocol layer
void tcp_rx(NetDevice* dev, PacketBuffer* pkt, uint32_t src_ip, uint32_t dst_ip);
void tcp_timer_tick(uint64_t now_ms);
[[noreturn]] void tcp_timer_thread();
void tcp_timer_thread_start();
auto get_tcp_proto_ops() -> SocketProtoOps*;

// TCP output helpers (used by tcp.cpp and tcp_input.cpp)
void tcp_send_segment(TcpCB* cb, uint8_t flags, const void* data, size_t len);
void tcp_send_rst(uint32_t src_ip, uint32_t dst_ip, uint16_t src_port, uint16_t dst_port,
                  uint32_t seq, uint32_t ack, uint8_t flags);
void tcp_send_ack(TcpCB* cb);

// TCP input processing
void tcp_process_segment(TcpCB* cb, const TcpHeader* hdr, const uint8_t* payload,
                         size_t payload_len, uint32_t src_ip, uint32_t dst_ip);

// TCB management
auto tcp_alloc_cb() -> TcpCB*;
void tcp_free_cb(TcpCB* cb);
auto tcp_find_cb(uint32_t local_ip, uint16_t local_port,
                 uint32_t remote_ip, uint16_t remote_port) -> TcpCB*;
auto tcp_find_listener(uint32_t local_ip, uint16_t local_port) -> TcpCB*;

// Sequence number helpers
inline bool tcp_seq_before(uint32_t a, uint32_t b) {
    return static_cast<int32_t>(a - b) < 0;
}
inline bool tcp_seq_after(uint32_t a, uint32_t b) {
    return tcp_seq_before(b, a);
}
inline bool tcp_seq_between(uint32_t seq, uint32_t low, uint32_t high) {
    return !tcp_seq_before(seq, low) && tcp_seq_before(seq, high);
}

// Current time in milliseconds (provided by APIC timer)
auto tcp_now_ms() -> uint64_t;

}  // namespace ker::net::proto
