#pragma once

#include <net/address.hpp>
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

// Learn a direct peer from any received Ethernet WKI frame so later control
// traffic is not dropped if HELLO/HELLO_ACK delivery was asymmetric.
void wki_peer_note_rx_contact(WkiTransport* transport, uint16_t peer_node, const proto::MacAddress& mac);

// Send a heartbeat to all CONNECTED peers
void wki_peer_send_heartbeats();

// Periodic timer - check heartbeat timeouts, resend HELLOs, fence dead peers
void wki_peer_timer_tick(uint64_t now_us);

// Fence a peer (immediate) - fails in-flight ops, notifies resource layers
void wki_peer_fence(WkiPeer* peer);

// Gracefully detach a peer that intentionally left the mesh.
void wki_peer_graceful_leave(WkiPeer* peer);

// WKI timer kernel thread - calls wki_peer_timer_tick() in a loop (~10ms cadence).
// Must be started after scheduler is running (from smt.cpp, like tcp_timer_thread).
[[noreturn]] void wki_timer_thread();
void wki_timer_thread_start();

// Wake the timer thread after arming earlier retransmit/ACK/timeout work.
void wki_timer_notify();

#ifdef WOS_SELFTEST
auto wki_peer_selftest_hello_ack_state_transition() -> bool;
auto wki_peer_selftest_hello_epoch_words_are_independent() -> bool;
auto wki_peer_selftest_remote_boot_epoch_detects_restart() -> bool;
#endif

}  // namespace ker::net::wki
