#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PEER_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "peer.cpp"
WKI_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.cpp"
WKI_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.hpp"
WIRE_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wire.hpp"
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


def require_token_after(body: str, anchor: str, token: str, context: str) -> None:
    anchor_pos = body.find(anchor)
    if anchor_pos < 0:
        fail(f"{context}: missing anchor {anchor!r}")
    if body.find(token, anchor_pos) < 0:
        fail(f"{context}: expected {token!r} after {anchor!r}")


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


def test_shutdown_uses_graceful_goodbye_not_fence() -> None:
    peer_source = PEER_CPP.read_text()
    wki_source = WKI_CPP.read_text()
    wire_source = WIRE_HPP.read_text()
    shutdown_body = function_body(wki_source, "wki_shutdown")
    rx_body = function_body(wki_source, "wki_rx")
    disconnect_body = function_body(peer_source, "wki_peer_disconnect_impl")
    goodbye_handler = function_body(peer_source, "handle_peer_goodbye")
    goodbye_drain = function_body(peer_source, "drain_pending_peer_goodbyes")
    goodbye_queue = function_body(peer_source, "queue_pending_peer_goodbye")

    for token in [
        "PEER_GOODBYE = 0x0C",
        "struct PeerGoodbyePayload",
        "WKI_GOODBYE_REASON_SHUTDOWN",
        "static_assert(sizeof(PeerGoodbyePayload) == 8",
    ]:
        if token not in wire_source:
            fail(f"graceful goodbye wire ABI is missing {token}")

    for token in [
        "PeerGoodbyePayload goodbye = {}",
        "goodbye.leaving_node = g_wki.my_node_id",
        "goodbye.reason = WKI_GOODBYE_REASON_SHUTDOWN",
        "wki_send_raw(connected_peers.at(i), MsgType::PEER_GOODBYE",
        "wki_peer_graceful_leave(peer);",
    ]:
        if token not in shutdown_body:
            fail(f"wki_shutdown must use graceful goodbye path: missing {token}")
    if "wki_peer_fence(" in shutdown_body:
        fail("wki_shutdown must not fence healthy peers during normal local shutdown")

    require_order(
        shutdown_body,
        "wki_send_raw(connected_peers.at(i), MsgType::PEER_GOODBYE",
        "wki_peer_graceful_leave(peer);",
        "shutdown must send goodbye before local peer cleanup closes channels/transports",
    )

    for token in [
        "enum class PeerDisconnectKind",
        "PeerDisconnectKind::FENCE",
        "PeerDisconnectKind::GRACEFUL_LEAVE",
        "Peer 0x%04x left gracefully",
        "kind == PeerDisconnectKind::FENCE && notify_connected_peers",
    ]:
        if token not in peer_source:
            fail(f"peer disconnect helper is missing graceful/fence split: {token}")

    require_order(
        rx_body,
        "case MsgType::PEER_GOODBYE:",
        "Reliable control messages - check seq ordering",
        "PEER_GOODBYE must stay on the bounded raw-control path",
    )
    if "detail::handle_peer_goodbye(hdr, payload, PAYLOAD_LEN);" not in rx_body:
        fail("wki_rx must dispatch PEER_GOODBYE to handle_peer_goodbye")

    for name, body in [
        ("handle_peer_goodbye", goodbye_handler),
        ("queue_pending_peer_goodbye", goodbye_queue),
        ("drain_pending_peer_goodbyes", goodbye_drain),
    ]:
        if "peer_goodbye_target_is_valid(" not in body:
            fail(f"{name} must validate PEER_GOODBYE target/source")

    require_order(
        goodbye_handler,
        "if (!peer_goodbye_target_is_valid(goodbye->leaving_node, hdr->src_node))",
        "queue_pending_peer_goodbye(*goodbye, hdr->src_node)",
        "PEER_GOODBYE handler must validate before queueing cleanup",
    )
    require_order(
        goodbye_handler,
        "queue_pending_peer_goodbye(*goodbye, hdr->src_node)",
        "wki_routing_invalidate_node(goodbye->leaving_node)",
        "PEER_GOODBYE route invalidation must follow accepted queue attempt",
    )
    if "wki_peer_disconnect_impl(leaving_peer, PeerDisconnectKind::GRACEFUL_LEAVE, false);" not in goodbye_drain:
        fail("deferred PEER_GOODBYE drain must use graceful leave, not fence")
    if "wki_peer_disconnect_impl(fenced_peer, PeerDisconnectKind::FENCE, false);" not in peer_source:
        fail("deferred FENCE_NOTIFY must keep fence semantics")


def test_ack_only_frames_do_not_refresh_peer_liveness() -> None:
    source = WKI_CPP.read_text()
    peer_source = PEER_CPP.read_text()
    body = function_body(source, "wki_rx")
    heartbeat_ack = function_body(peer_source, "handle_heartbeat_ack")

    first_rx_progress = body.find("mark_peer_rx_progress(hdr->src_node);")
    if first_rx_progress < 0:
        fail("wki_rx must refresh peer liveness on real receive progress")
    ack_progress_pos = body.find("if (ack_progress)")
    if ack_progress_pos < 0:
        fail("wki_rx must gate ACK-only liveness on ack_progress")
    if first_rx_progress < ack_progress_pos:
        fail("wki_rx must not refresh peer liveness before ACK progress filtering")

    require_order(
        body,
        "if (ack_progress)",
        "mark_peer_rx_progress(hdr->src_node);",
        "ACK liveness refresh must be gated by tx ACK progress",
    )
    require_order(
        body,
        "bool const RELIABLE_RX_ACCEPTED = reliable_rx_peer_accepts(hdr->src_node)",
        "if (ack_progress)",
        "ACK liveness refresh must happen after reliable peer-state filtering",
    )
    require_token_after(
        body,
        "if (hdr->seq_num == ch->rx_seq)",
        "mark_peer_rx_progress(hdr->src_node);",
        "in-order reliable payloads still refresh liveness",
    )
    require_token_after(
        body,
        "if (!already_buffered)",
        "mark_peer_rx_progress(hdr->src_node);",
        "new out-of-order reliable payloads still refresh liveness",
    )
    require_order(
        heartbeat_ack,
        "if (payload_len < sizeof(HeartbeatPayload))",
        "peer->last_rx_activity = peer->last_heartbeat;",
        "zero-payload ACK carriers must not refresh heartbeat ACK liveness",
    )

    pre_dispatch = body[: body.find("// Forwarding: if this packet is not for us, forward it")]
    if "mark_peer_rx_progress" in pre_dispatch:
        fail("wki_rx must not refresh peer liveness for every validated frame before ACK filtering")


def test_forwarding_recomputes_checksum_after_ttl_decrement() -> None:
    source = WKI_CPP.read_text()
    checksum_body = function_body(source, "wki_frame_checksum")
    body = function_body(source, "wki_rx")

    require_order(
        checksum_body,
        "WkiHeader hdr_copy = hdr",
        "hdr_copy.checksum = 0",
        "checksum helper must zero the checksum field before computing CRC",
    )
    require_order(
        checksum_body,
        "uint32_t crc = wki_crc32(&hdr_copy, WKI_HEADER_SIZE)",
        "crc = wki_crc32_continue(crc, payload, hdr.payload_len)",
        "checksum helper must cover header and payload",
    )
    require_order(
        body,
        "if (hdr->checksum != 0)",
        "uint32_t const CRC = wki_frame_checksum(*hdr, static_cast<const uint8_t*>(data) + WKI_HEADER_SIZE)",
        "RX checksum validation must use the shared WKI frame checksum helper",
    )
    require_order(
        body,
        "fwd_hdr->hop_ttl--",
        "if (fwd_hdr->checksum != 0)",
        "forwarding must test whether a checksum needs refreshing after TTL mutation",
    )
    require_order(
        body,
        "if (fwd_hdr->checksum != 0)",
        "fwd_hdr->checksum = wki_frame_checksum(*fwd_hdr, fwd_frame + WKI_HEADER_SIZE)",
        "forwarding must recompute checksums after TTL mutation",
    )
    require_order(
        body,
        "fwd_hdr->checksum = wki_frame_checksum(*fwd_hdr, fwd_frame + WKI_HEADER_SIZE)",
        "fwd_transport->tx(fwd_transport, NEXT_HOP, fwd_frame, len)",
        "forwarding must transmit only after checksum refresh",
    )


def test_ipc_data_acks_after_ordered_dispatch() -> None:
    source = WKI_CPP.read_text()
    body = function_body(source, "wki_rx")
    required = [
        "ch->channel_id == WKI_CHAN_IPC_DATA",
        "bool const IMM_ACK = ((ch->priority == PriorityClass::LATENCY || ch->channel_id == WKI_CHAN_IPC_DATA) && ch->ack_pending);",
        "wki_dispatch_reliable_msg_ordered(ch, msg, hdr, payload, PAYLOAD_LEN);",
        "wki_dispatch_reliable_msg_ordered(ch, RO_MSG, &RO_HDR, ro_data, RO_LEN);",
        "complete_ack_transmit_for_generation_locked(ch, imm_ack_generation, imm_ack_num, tx_ret, notify_timer);",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("IPC_DATA post-dispatch ACK path is missing token(s): " + ", ".join(missing))
    require_order(
        body,
        "wki_dispatch_reliable_msg_ordered(ch, msg, hdr, payload, PAYLOAD_LEN);",
        "bool const IMM_ACK = ((ch->priority == PriorityClass::LATENCY || ch->channel_id == WKI_CHAN_IPC_DATA) && ch->ack_pending);",
        "IPC_DATA ACK gating must be computed after ordered local dispatch",
    )
    require_order(
        body,
        "wki_dispatch_reliable_msg_ordered(ch, RO_MSG, &RO_HDR, ro_data, RO_LEN);",
        "bool const IMM_ACK = ((ch->priority == PriorityClass::LATENCY || ch->channel_id == WKI_CHAN_IPC_DATA) && ch->ack_pending);",
        "reordered IPC_DATA delivery must complete before the immediate ACK decision",
    )
    require_order(
        body,
        "bool const IMM_ACK = ((ch->priority == PriorityClass::LATENCY || ch->channel_id == WKI_CHAN_IPC_DATA) && ch->ack_pending);",
        "complete_ack_transmit_for_generation_locked(ch, imm_ack_generation, imm_ack_num, tx_ret, notify_timer);",
        "IPC_DATA immediate ACK completion must follow the post-dispatch ACK decision",
    )


def test_ipc_data_ordered_dispatch_wait_uses_explicit_daemon_wake() -> None:
    source = WKI_CPP.read_text()
    header = WKI_HPP.read_text()

    required_header = [
        "WKI_RX_DISPATCH_WAITER_SLOTS",
        "rx_dispatch_waiters",
    ]
    missing_header = [token for token in required_header if token not in header]
    if missing_header:
        fail("ordered IPC_DATA dispatch wait is missing channel waiter storage token(s): " + ", ".join(missing_header))

    record_body = function_body(source, "record_reliable_dispatch_waiter_locked")
    required_record = [
        "waiter == task",
        "waiter == nullptr",
        "waiter = task",
    ]
    missing_record = [token for token in required_record if token not in record_body]
    if missing_record:
        fail("ordered IPC_DATA dispatch wait is missing waiter registration token(s): " + ", ".join(missing_record))

    body = function_body(source, "wait_for_reliable_dispatch_turn")
    required = [
        "uint32_t const GENERATION = ch->generation",
        "ch->active && ch->generation == GENERATION",
        "current_task->type == ker::mod::sched::task::TaskType::DAEMON",
        "record_reliable_dispatch_waiter_locked(ch, current_task)",
        "ker::mod::sched::kern_block()",
        "ker::mod::sched::kern_yield()",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("ordered IPC_DATA dispatch wait is missing explicit wake wait token(s): " + ", ".join(missing))

    finish_body = function_body(source, "finish_reliable_dispatch_turn")
    required_finish = [
        "DispatchWaiterList waiters{}",
        "rx_dispatch_seq++",
        "drain_reliable_dispatch_waiters_locked(ch, waiters)",
        "wake_reliable_dispatch_waiters(waiters)",
    ]
    missing_finish = [token for token in required_finish if token not in finish_body]
    if missing_finish:
        fail("ordered IPC_DATA dispatch wait is missing wake completion token(s): " + ", ".join(missing_finish))
    require_order(
        finish_body,
        "drain_reliable_dispatch_waiters_locked(ch, waiters)",
        "ch->lock.unlock()",
        "ordered IPC_DATA dispatch wait must drain waiters before unlocking channel",
    )
    require_order(
        finish_body,
        "ch->lock.unlock()",
        "wake_reliable_dispatch_waiters(waiters)",
        "ordered IPC_DATA dispatch wait must wake after dropping channel lock",
    )


def test_cross_channel_ack_scan_uses_allocated_range_bounds() -> None:
    source = WKI_CPP.read_text()
    header = WKI_HPP.read_text()
    for token in [
        "std::atomic<uint16_t>::is_always_lock_free",
        '"WKI channel scan bounds must stay lock-free"',
        "std::atomic<uint16_t> ordinary_channel_scan_limit{0};",
        "std::atomic<uint16_t> reserved_channel_scan_limit{WKI_CHAN_DYNAMIC_RESERVED_BASE};",
    ]:
        if token not in header:
            fail(f"per-peer ACK scan bound is missing {token}")

    allocator = function_body(source, "channel_pool_alloc")
    for token in [
        "chan_id < WKI_CHAN_DYNAMIC_RESERVED_BASE",
        "peer->ordinary_channel_scan_limit",
        "peer->reserved_channel_scan_limit",
        "std::max(scan_limit.load(std::memory_order_relaxed), PUBLISHED_LIMIT)",
        "std::memory_order_release",
    ]:
        if token not in allocator:
            fail(f"channel publication is missing ACK scan bound update {token}")
    require_order(
        allocator,
        "peer->channels.at(chan_id) = ch;",
        "scan_limit.store(",
        "channel pointer must be published before its ACK scan bound",
    )
    require_order(
        allocator,
        "scan_limit.store(",
        "ch->lock.unlock();",
        "ACK scan bound must be published before the initialized channel is unlocked",
    )

    capture = function_body(source, "capture_pending_peer_ack_for_tx_locked")
    if "for (WkiChannel* candidate : peer->channels)" in capture:
        fail("cross-channel ACK scan must not walk the full per-peer table")
    for token in [
        "auto capture_in_range = [peer, tx_ch](uint16_t first, uint16_t limit)",
        "peer->channels.at(channel_id)",
        "ordinary_channel_scan_limit.load(std::memory_order_acquire)",
        "reserved_channel_scan_limit.load(std::memory_order_acquire)",
        "capture_in_range(0, ORDINARY_LIMIT)",
        "capture_in_range(WKI_CHAN_DYNAMIC_RESERVED_BASE, RESERVED_LIMIT)",
    ]:
        if token not in capture:
            fail(f"bounded cross-channel ACK scan is missing {token}")
    require_order(
        capture,
        "capture_in_range(0, ORDINARY_LIMIT)",
        "reserved_channel_scan_limit.load(std::memory_order_acquire)",
        "ordinary ACK channels must retain priority over reserved channels",
    )
    for close_function in ["wki_channel_close", "wki_channels_close_for_peer"]:
        close_body = function_body(source, close_function)
        if "channel_scan_limit" in close_body:
            fail(f"{close_function} must not lower monotonic ACK scan bounds")


def test_fence_drains_deferred_vfs_bindings_before_remote_fd_cleanup() -> None:
    body = function_body(PEER_CPP.read_text(), "wki_peer_disconnect_impl")
    require_order(
        body,
        "wki_dev_server_detach_all_for_peer(fenced_id)",
        "wki_remote_vfs_cleanup_for_peer(fenced_id)",
        "peer fence must drain retained VFS handlers before closing their remote FDs",
    )


def main() -> None:
    test_hello_ack_reconnects_fenced_peer_outside_peer_lock()
    test_hello_boot_epoch_fences_connected_broadcast_restarts()
    test_reliable_rx_rejects_fenced_peer_before_channel_lookup()
    test_fence_notify_rejects_invalid_targets_before_routing()
    test_shutdown_uses_graceful_goodbye_not_fence()
    test_ack_only_frames_do_not_refresh_peer_liveness()
    test_forwarding_recomputes_checksum_after_ttl_decrement()
    test_ipc_data_acks_after_ordered_dispatch()
    test_ipc_data_ordered_dispatch_wait_uses_explicit_daemon_wake()
    test_cross_channel_ack_scan_uses_allocated_range_bounds()
    test_fence_drains_deferred_vfs_bindings_before_remote_fd_cleanup()
    print("WKI peer source invariants hold")


if __name__ == "__main__":
    main()
