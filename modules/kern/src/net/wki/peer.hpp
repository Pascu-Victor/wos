#pragma once

#include <net/wki/wki.hpp>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Peer subsystem API
// -----------------------------------------------------------------------------

// Send HELLO broadcast on all transports (called during init and periodically)
void wki_peer_send_hello_broadcast();

// Send HELLO to a specific neighbor via a transport
void wki_peer_send_hello(WkiTransport* transport, uint16_t dst_node);

// Send HELLO_ACK to a specific peer
void wki_peer_send_hello_ack(WkiPeer* peer);

// Send a heartbeat to all CONNECTED peers
void wki_peer_send_heartbeats();

// Periodic timer — check heartbeat timeouts, resend HELLOs, fence dead peers
void wki_peer_timer_tick(uint64_t now_us);

// Fence a peer (immediate) — fails in-flight ops, notifies resource layers
void wki_peer_fence(WkiPeer* peer);

// WKI timer kernel thread — calls wki_peer_timer_tick() in a loop (~10ms cadence).
// Must be started after scheduler is running (from smt.cpp, like tcp_timer_thread).
[[noreturn]] void wki_timer_thread();
void wki_timer_thread_start();

}  // namespace ker::net::wki
