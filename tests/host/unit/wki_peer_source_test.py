#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PEER_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "peer.cpp"
WKI_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.cpp"
WKI_PEER_LIVENESS_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "wki_peer_liveness_ktest.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\b(?:void|auto)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{", source)
    if match is None:
        fail(f"missing function {name}")

    depth = 1
    pos = match.end()
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated function {name}")
    return source[match.end() : pos - 1]


def require_order(body: str, before: str, after: str, context: str) -> None:
    before_pos = body.find(before)
    after_pos = body.find(after)
    if before_pos < 0 or after_pos < 0 or before_pos >= after_pos:
        fail(f"{context}: expected {before!r} before {after!r}")


def test_hello_ack_reconnects_fenced_peer_outside_peer_lock() -> None:
    source = PEER_CPP.read_text()
    transition = function_body(source, "apply_hello_ack_peer_state_locked")
    body = function_body(source, "handle_hello_ack")

    required_transition_tokens = [
        "peer->state == PeerState::FENCED",
        "peer->state = PeerState::RECONNECTING;",
        "return HelloAckPeerTransition::RECONNECTING;",
    ]
    missing = [token for token in required_transition_tokens if token not in transition]
    if missing:
        fail("HELLO_ACK transition helper must mark fenced peers as reconnecting: " + ", ".join(missing))

    required_body_tokens = [
        "STATE_TRANSITION == HelloAckPeerTransition::RECONNECTING",
        "wki_channels_close_for_peer(peer_node);",
        "peer->state == PeerState::RECONNECTING",
        "peer->state = PeerState::CONNECTED;",
        "wki_dev_proxy_resume_for_peer(peer_node);",
        "wki_lsa_generate_and_flood();",
        "wki_resource_advertise_to_peer(peer_node);",
        "wki_event_publish(EVENT_CLASS_SYSTEM, EVENT_SYSTEM_NODE_JOIN",
    ]
    missing = [token for token in required_body_tokens if token not in body]
    if missing:
        fail("handle_hello_ack() is missing fenced reconnect work: " + ", ".join(missing))

    require_order(body, "g_wki.peer_lock.unlock();", "wki_channels_close_for_peer(peer_node);", "HELLO_ACK channel reset lock boundary")
    require_order(body, "wki_channels_close_for_peer(peer_node);", "peer->state = PeerState::CONNECTED;", "HELLO_ACK reconnect connect")
    require_order(body, "g_wki.peer_lock.unlock();", "wki_dev_proxy_resume_for_peer(peer_node);", "HELLO_ACK proxy resume lock boundary")
    require_order(body, "wki_dev_proxy_resume_for_peer(peer_node);", "wki_lsa_generate_and_flood();", "HELLO_ACK topology refresh")


def test_hello_boot_epoch_fences_connected_broadcast_restarts() -> None:
    source = PEER_CPP.read_text()
    wki_source = WKI_CPP.read_text()
    handle_hello = function_body(source, "handle_hello")
    handle_hello_ack = function_body(source, "handle_hello_ack")
    broadcast = function_body(source, "wki_peer_send_hello_broadcast")
    direct_hello = function_body(source, "wki_peer_send_hello")
    hello_ack = function_body(source, "wki_peer_send_hello_ack")

    required_source_tokens = [
        "constexpr size_t HELLO_BOOT_EPOCH_OFFSET = HELLO_CHANNEL_EPOCH_OFFSET + sizeof(uint32_t);",
        "void hello_set_boot_epoch(HelloPayload* hello, uint32_t epoch)",
        "auto hello_boot_epoch(const HelloPayload* hello) -> uint32_t",
        "auto peer_note_remote_boot_epoch_locked(WkiPeer* peer, uint32_t remote_epoch) -> bool",
        "auto hello_boot_epoch_matches_peer(const WkiPeer* peer, uint32_t remote_epoch) -> bool",
    ]
    missing = [token for token in required_source_tokens if token not in source]
    if missing:
        fail("HELLO boot epoch helpers are missing: " + ", ".join(missing))

    for name, body in [
        ("broadcast HELLO", broadcast),
        ("direct HELLO", direct_hello),
        ("HELLO_ACK", hello_ack),
    ]:
        if "hello_set_boot_epoch(" not in body or "g_wki.local_boot_epoch" not in body:
            fail(f"{name} must advertise g_wki.local_boot_epoch")

    require_order(
        handle_hello,
        "uint32_t const REMOTE_BOOT_EPOCH = hello_boot_epoch(hello)",
        "hello_boot_epoch_matches_peer(peer, REMOTE_BOOT_EPOCH)",
        "broadcast fast path must compare boot epoch",
    )
    require_order(
        handle_hello,
        "hello_boot_epoch_matches_peer(peer, REMOTE_BOOT_EPOCH)",
        "wki_eth_neighbor_add(peer_node, hello->mac_addr)",
        "matching broadcast boot epoch may refresh and return",
    )
    require_order(
        handle_hello,
        "remote_boot_epoch_changed = peer_note_remote_boot_epoch_locked(peer, REMOTE_BOOT_EPOCH)",
        "if (remote_boot_epoch_changed)",
        "HELLO must record boot epoch before reset decision",
    )
    require_order(
        handle_hello,
        "if (remote_boot_epoch_changed)",
        "wki_channels_close_for_peer(peer_node)",
        "HELLO boot epoch change must close stale channels",
    )
    require_order(
        handle_hello,
        "resync_connected_peer = !newly_connected && !WAS_FENCED",
        "if (newly_connected || resync_connected_peer)",
        "HELLO boot epoch restart must re-advertise state",
    )
    require_order(
        handle_hello_ack,
        "remote_boot_epoch_changed = peer_note_remote_boot_epoch_locked(peer, REMOTE_BOOT_EPOCH)",
        "if (remote_boot_epoch_changed && !WAS_FENCED)",
        "HELLO_ACK must record boot epoch before reset decision",
    )
    require_order(
        handle_hello_ack,
        "if (remote_boot_epoch_changed && !WAS_FENCED)",
        "wki_channels_close_for_peer(peer_node)",
        "HELLO_ACK boot epoch change must close stale channels",
    )
    require_order(
        handle_hello_ack,
        "resync_connected_peer = !newly_connected",
        "if (newly_connected || resync_connected_peer)",
        "HELLO_ACK boot epoch restart must re-advertise state",
    )

    for token in [
        "uint32_t remote_boot_epoch = 0;",
        "uint32_t local_boot_epoch = 1;",
    ]:
        if token not in (ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.hpp").read_text():
            fail(f"WKI state is missing {token}")

    if "g_wki.local_boot_epoch = wki_nonzero_epoch_from_seed" not in wki_source:
        fail("wki_init must assign a nonzero local boot epoch")

    ktest_source = WKI_PEER_LIVENESS_KTEST.read_text()
    for token in [
        "HelloEpochWordsAreIndependent",
        "RemoteBootEpochDetectsRestart",
        "wki_peer_selftest_hello_epoch_words_are_independent",
        "wki_peer_selftest_remote_boot_epoch_detects_restart",
    ]:
        if token not in ktest_source:
            fail(f"peer boot epoch KTEST is missing {token}")


def test_reliable_rx_rejects_fenced_peer_before_channel_lookup() -> None:
    source = WKI_CPP.read_text()
    body = function_body(source, "wki_rx")

    helper_tokens = [
        "auto reliable_rx_peer_state_accepts(PeerState state) -> bool",
        "return state == PeerState::CONNECTED",
        "auto reliable_rx_peer_accepts(uint16_t src_node) -> bool",
        "wki_selftest_reliable_rx_peer_state_accepts",
    ]
    missing = [token for token in helper_tokens if token not in source]
    if missing:
        fail("reliable RX peer-state gate is missing helper token(s): " + ", ".join(missing))

    required_body_tokens = [
        "bool const RELIABLE_RX_ACCEPTED = reliable_rx_peer_accepts(hdr->src_node)",
        "WKI_FLAG_ACK_PRESENT) != 0 && RELIABLE_RX_ACCEPTED",
        "if (!RELIABLE_RX_ACCEPTED)",
        "wki_channel_get_for_reliable_rx(hdr->src_node, hdr->channel_id)",
    ]
    missing = [token for token in required_body_tokens if token not in body]
    if missing:
        fail("wki_rx reliable peer-state gate is missing token(s): " + ", ".join(missing))

    require_order(
        body,
        "bool const RELIABLE_RX_ACCEPTED = reliable_rx_peer_accepts(hdr->src_node)",
        "wki_channel_lookup(hdr->src_node, ACK_CHANNEL_ID)",
        "reliable ACK peer-state gate",
    )
    require_order(
        body,
        "if (!RELIABLE_RX_ACCEPTED)",
        "wki_channel_get_for_reliable_rx(hdr->src_node, hdr->channel_id)",
        "reliable payload peer-state gate",
    )

    ktest_source = WKI_PEER_LIVENESS_KTEST.read_text()
    for token in [
        "ReliableRxAcceptsOnlyConnectedPeers",
        "PeerState::CONNECTED",
        "PeerState::FENCED",
        "PeerState::RECONNECTING",
    ]:
        if token not in ktest_source:
            fail(f"reliable RX fence KTEST is missing {token}")


def test_fence_notify_rejects_invalid_targets_before_routing() -> None:
    source = PEER_CPP.read_text()
    helper = function_body(source, "fence_notify_target_is_valid")
    handle_body = function_body(source, "handle_fence_notify")
    queue_body = function_body(source, "queue_pending_fence_notify")
    drain_body = function_body(source, "drain_pending_fence_notifies")

    for token in [
        "fenced_node != WKI_NODE_INVALID",
        "fenced_node != WKI_NODE_BROADCAST",
        "fenced_node != g_wki.my_node_id",
    ]:
        if token not in helper:
            fail(f"FENCE_NOTIFY target helper is missing guard: {token}")

    for name, body in [
        ("queue_pending_fence_notify", queue_body),
        ("drain_pending_fence_notifies", drain_body),
    ]:
        if "!fence_notify_target_is_valid(" not in body:
            fail(f"{name} must reject invalid/self/broadcast FENCE_NOTIFY targets")

    guard = "if (!fence_notify_target_is_valid(fn->fenced_node))"
    if guard not in handle_body:
        fail("handle_fence_notify must reject invalid/self/broadcast targets")
    require_order(
        handle_body,
        guard,
        "wki_routing_invalidate_node(fn->fenced_node)",
        "FENCE_NOTIFY target validation before route invalidation",
    )
    guarded_tail = handle_body[handle_body.find(guard) : handle_body.find("wki_routing_invalidate_node(fn->fenced_node)")]
    if "return;" not in guarded_tail:
        fail("handle_fence_notify invalid target guard must return before route invalidation")


def main() -> None:
    test_hello_ack_reconnects_fenced_peer_outside_peer_lock()
    test_hello_boot_epoch_fences_connected_broadcast_restarts()
    test_reliable_rx_rejects_fenced_peer_before_channel_lookup()
    test_fence_notify_rejects_invalid_targets_before_routing()
    print("WKI peer source invariants hold")


if __name__ == "__main__":
    main()
