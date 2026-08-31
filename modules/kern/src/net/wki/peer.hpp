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

// Send the existing HELLO payload through the routing table. This establishes
// session/hostname identity for an LSA-reachable non-direct peer without
// changing the HELLO or LSA wire formats.
void wki_peer_send_routed_hello(uint16_t dst_node);

// Send HELLO_ACK to a specific peer
void wki_peer_send_hello_ack(WkiPeer* peer);

// Ethernet contact learning runs before forwarding. A unicast frame whose
// default TTL has already been decremented came from a routed origin, not the
// immediate Ethernet neighbor represented by its WKI src_node.
constexpr auto wki_peer_frame_was_forwarded(const WkiHeader* header) -> bool {
    return header != nullptr && header->dst_node != WKI_NODE_BROADCAST && header->hop_ttl < WKI_DEFAULT_TTL;
}

enum class WkiPeerHelloPath : uint8_t {
    DIRECT,
    ROUTED,
    UNRESOLVED,
};

// Direct targeted HELLOs intentionally use TTL=1. A known direct peer makes
// that unambiguous; otherwise reciprocal SPF knowledge must exist before a
// decremented-TTL origin can be accepted. UNRESOLVED prevents a routed origin
// that races ahead of the reverse LSA from being promoted to an L2 neighbor.
constexpr auto wki_peer_hello_path(const WkiHeader* header, bool route_valid, uint8_t route_hops, bool known_direct) -> WkiPeerHelloPath {
    if (!wki_peer_frame_was_forwarded(header)) {
        return WkiPeerHelloPath::DIRECT;
    }
    if (header->hop_ttl == 1 && known_direct) {
        return WkiPeerHelloPath::DIRECT;
    }
    if (route_valid && route_hops > 1) {
        return WkiPeerHelloPath::ROUTED;
    }
    return WkiPeerHelloPath::UNRESOLVED;
}

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
auto wki_peer_selftest_boot_epoch_advances_local_channel_epoch() -> bool;
auto wki_peer_selftest_initial_channel_epoch_fences_pre_handshake_stream() -> bool;
auto wki_peer_selftest_initial_epoch_observation_preserves_acked_stream() -> bool;
auto wki_peer_selftest_fenced_hostname_retires_to_node_identity() -> bool;
#endif

}  // namespace ker::net::wki
