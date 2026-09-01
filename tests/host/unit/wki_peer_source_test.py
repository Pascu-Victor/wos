#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PEER_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "peer.cpp"
WKI_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.cpp"
WKI_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.hpp"
REMOTABLE_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remotable.cpp"
REMOTE_VFS_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_vfs.cpp"
ROUTING_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "routing.cpp"
TRANSPORT_ETH_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "transport_eth.cpp"
TRANSPORT_IVSHMEM_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "transport_ivshmem.cpp"
NETDEVCONF_CPP = ROOT / "modules" / "kern" / "src" / "util" / "netdevconf.cpp"
INIT_WRAPPERS_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "init" / "init_wrappers.cpp"
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
        "peer->block_resume_pending.store(true, std::memory_order_release);",
        "wki_timer_notify();",
        "wki_lsa_generate_and_flood();",
        "wki_resource_advertise_to_peer(peer_node);",
        "wki_event_publish(EVENT_CLASS_SYSTEM, EVENT_SYSTEM_NODE_JOIN",
    ]
    missing = [token for token in required_body_tokens if token not in body]
    if missing:
        fail("handle_hello_ack() is missing fenced reconnect work: " + ", ".join(missing))

    require_order(body, "g_wki.peer_lock.unlock();", "wki_channels_close_for_peer(peer_node);", "HELLO_ACK channel reset lock boundary")
    require_order(body, "wki_channels_close_for_peer(peer_node);", "peer->state = PeerState::CONNECTED;", "HELLO_ACK reconnect connect")
    require_order(
        body,
        "g_wki.peer_lock.unlock();",
        "peer->block_resume_pending.store(true, std::memory_order_release);",
        "HELLO_ACK deferred proxy resume lock boundary",
    )
    require_order(
        body,
        "peer->block_resume_pending.store(true, std::memory_order_release);",
        "wki_lsa_generate_and_flood();",
        "HELLO_ACK queues proxy resume before topology refresh",
    )


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
        "auto peer_remote_epoch_reset_is_proven_locked(",
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
        "remote_session_epoch_reset = peer_remote_epoch_reset_is_proven_locked(peer, REMOTE_BOOT_EPOCH, remote_channel_epoch)",
        "peer_note_remote_boot_epoch_locked(peer, REMOTE_BOOT_EPOCH)",
        "HELLO must classify the prior session before recording its first epoch",
    )
    require_order(
        handle_hello,
        "if (remote_session_epoch_reset)",
        "wki_channels_close_for_peer(peer_node)",
        "HELLO proven session change must close stale channels",
    )
    require_order(
        handle_hello,
        "peer_advance_local_channel_epoch(peer)",
        "if (!session_confirmed)",
        "HELLO boot epoch reset must publish a reciprocal channel epoch before confirmation",
    )
    require_order(
        handle_hello,
        "resync_connected_peer = !newly_connected && !WAS_FENCED",
        "if (newly_connected || resync_connected_peer)",
        "HELLO boot epoch restart must re-advertise state",
    )
    require_order(
        handle_hello_ack,
        "remote_session_epoch_reset = peer_remote_epoch_reset_is_proven_locked(peer, REMOTE_BOOT_EPOCH, remote_channel_epoch)",
        "peer_note_remote_boot_epoch_locked(peer, REMOTE_BOOT_EPOCH)",
        "HELLO_ACK must classify the prior session before recording its first epoch",
    )
    require_order(
        handle_hello_ack,
        "if (remote_session_epoch_reset && !WAS_FENCED)",
        "wki_channels_close_for_peer(peer_node)",
        "HELLO_ACK proven session change must close stale channels",
    )
    require_order(
        handle_hello_ack,
        "peer_advance_local_channel_epoch(peer)",
        "wki_peer_send_hello(transport, peer_node)",
        "HELLO_ACK boot epoch reset must publish a reciprocal channel epoch",
    )
    require_order(
        handle_hello_ack,
        "resync_connected_peer = !newly_connected",
        "if (newly_connected || resync_connected_peer)",
        "HELLO_ACK boot epoch restart must re-advertise state",
    )
    require_order(
        handle_hello_ack,
        "if (newly_connected || resync_connected_peer)",
        "A HELLO_ACK confirms that the peer has processed our latest HELLO",
        "HELLO_ACK resource replay must remain outside transition-only work",
    )
    require_order(
        handle_hello_ack,
        "A HELLO_ACK confirms that the peer has processed our latest HELLO",
        "wki_resource_advertise_to_peer(peer_node)",
        "every valid HELLO_ACK must replay the current resource snapshot",
    )
    if handle_hello_ack.count("wki_resource_advertise_to_peer(peer_node)") != 1:
        fail("HELLO_ACK must have exactly one unconditional resource snapshot replay")

    for token in [
        "uint32_t remote_boot_epoch = 0;",
        "uint32_t local_boot_epoch = 1;",
    ]:
        if token not in (ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.hpp").read_text():
            fail(f"WKI state is missing {token}")

    require_order(
        function_body(wki_source, "wki_init"),
        "ker::mod::random::entropy::get_bytes(&boot_epoch",
        "g_wki.local_boot_epoch = boot_epoch == 0 ? 1 : boot_epoch",
        "wki_init must assign a hardware-random nonzero local boot epoch",
    )

    ktest_source = WKI_PEER_LIVENESS_KTEST.read_text()
    for token in [
        "HelloEpochWordsAreIndependent",
        "RemoteBootEpochDetectsRestart",
        "BootEpochAdvancesLocalChannelEpoch",
        "InitialChannelEpochFencesPreHandshakeStream",
        "InitialEpochObservationPreservesAckedStream",
        "wki_peer_selftest_hello_epoch_words_are_independent",
        "wki_peer_selftest_remote_boot_epoch_detects_restart",
        "wki_peer_selftest_boot_epoch_advances_local_channel_epoch",
        "wki_peer_selftest_initial_channel_epoch_fences_pre_handshake_stream",
        "wki_peer_selftest_initial_epoch_observation_preserves_acked_stream",
    ]:
        if token not in ktest_source:
            fail(f"peer boot epoch KTEST is missing {token}")

    for handler_name in ["handle_hello", "handle_hello_ack"]:
        body = function_body(source, handler_name)
        channel_reset = body.find("if (remote_session_epoch_reset")
        if channel_reset < 0 or body.find("resync_connected_peer = !newly_connected", channel_reset) < 0:
            fail(f"{handler_name} proven epoch reset must re-advertise current resources")

    require_order(
        handle_hello,
        "if (newly_connected || resync_connected_peer)",
        "Every HELLO that reaches the full handshake path",
        "HELLO resource replay must remain outside transition-only work",
    )
    require_order(
        handle_hello,
        "Every HELLO that reaches the full handshake path",
        "wki_resource_advertise_to_peer(peer_node)",
        "a post-cleanup direct HELLO must request a current resource snapshot",
    )
    if handle_hello.count("wki_resource_advertise_to_peer(peer_node)") != 1:
        fail("HELLO must have exactly one unconditional resource snapshot replay")


def test_connected_epoch_reset_retires_vfs_before_channel_reuse() -> None:
    source = PEER_CPP.read_text()
    for handler_name in ["handle_hello", "handle_hello_ack"]:
        body = function_body(source, handler_name)
        required = [
            "!WAS_FENCED && remote_session_epoch_reset",
            "wki_dev_server_mark_epoch_reset(peer_node)",
            "wki_remote_vfs_mark_epoch_reset(peer_node)",
            "peer->vfs_reset_invalidate_discovery.store(true, std::memory_order_release)",
            "peer->vfs_reset_rebind_pending.store(true, std::memory_order_release)",
            "wki_timer_notify()",
            "wki_channels_close_for_peer(peer_node)",
        ]
        missing = [token for token in required if token not in body]
        if missing:
            fail(f"{handler_name} connected epoch reset is missing: " + ", ".join(missing))
        require_order(
            body,
            "wki_dev_server_mark_epoch_reset(peer_node)",
            "wki_remote_vfs_mark_epoch_reset(peer_node)",
            f"{handler_name} must retire owner bindings before consumer proxies",
        )
        require_order(
            body,
            "wki_remote_vfs_mark_epoch_reset(peer_node)",
            "wki_channels_close_for_peer(peer_node)",
            f"{handler_name} must stop VFS operations before channel reuse",
        )

    drain_body = function_body(source, "drain_pending_epoch_reset_cleanups")
    required_drain = [
        "peer.vfs_reset_rebind_pending.load(std::memory_order_acquire)",
        "lifecycle.acquire(&peer)",
        "wki_dev_server_cleanup_epoch_reset_for_peer(peer.node_id)",
        "wki_remote_vfs_cleanup_for_peer(peer.node_id, OWNER_REBOOT_PROVEN)",
        "bool const CONNECTED = peer.state == PeerState::CONNECTED",
        "peer.vfs_reset_invalidate_discovery.exchange(false, std::memory_order_acq_rel)",
        "wki_resources_invalidate_for_peer(peer.node_id)",
        "wki_resources_rebind_vfs_for_peer(peer.node_id)",
        "peer.vfs_reset_rebind_pending.store(false, std::memory_order_release)",
    ]
    missing = [token for token in required_drain if token not in drain_body]
    if missing:
        fail("task-context VFS epoch cleanup/rebind is missing: " + ", ".join(missing))
    require_order(
        drain_body,
        "wki_dev_server_cleanup_epoch_reset_for_peer(peer.node_id)",
        "wki_remote_vfs_cleanup_for_peer(peer.node_id, OWNER_REBOOT_PROVEN)",
        "server binding drain must precede RemoteVfsFd cleanup",
    )
    require_order(
        drain_body,
        "wki_resources_rebind_vfs_for_peer(peer.node_id)",
        "peer.vfs_reset_rebind_pending.store(false, std::memory_order_release)",
        "epoch admission must remain closed through reconciliation",
    )
    require_order(
        drain_body,
        "peer.vfs_reset_rebind_pending.store(false, std::memory_order_release)",
        "wki_peer_send_hello(resource_resync_transport, resource_resync_peer)",
        "post-epoch resource resync must start only after reliable admission reopens",
    )

    marker_body = function_body(REMOTE_VFS_CPP.read_text(), "wki_remote_vfs_mark_epoch_reset")
    if "std::deque" in marker_body or "delete " in marker_body or "finish_" in marker_body:
        fail("HELLO RX VFS epoch marker must remain bounded and nonblocking")
    for token in ["s_vfs_lock.lock()", "state->active = false", "state->epoch_reset_pending = true"]:
        if token not in marker_body:
            fail(f"HELLO RX VFS epoch marker is missing {token}")

    rebind_body = function_body(REMOTABLE_CPP.read_text(), "wki_resources_rebind_vfs_for_peer")
    for token in [
        "superseded.valid = false",
        "resource.generation = next_resource_generation_locked()",
        "queue_vfs_mount_locked(node_id, resource.resource_id, resource.generation",
        "g_discovered.push_back(superseded)",
    ]:
        if token not in rebind_body:
            fail(f"cached VFS epoch rebind is missing {token}")

    wki_source = WKI_CPP.read_text()
    lifecycle_body = function_body(wki_source, "message_uses_device_session_lifecycle")
    for token in ["DEV_ATTACH_REQ", "DEV_ATTACH_ACK", "DEV_DETACH", "dev_op_uses_ipc_lifecycle(type, payload, payload_len)"]:
        if token not in lifecycle_body:
            fail(f"device session lifecycle admission is missing {token}")
    gate_body = function_body(wki_source, "epoch_reset_blocks_message")
    for token in ["message_uses_deferred_remotable_rx", "message_is_epoch_reset_sensitive", "vfs_reset_rebind_pending.load"]:
        if token not in gate_body:
            fail(f"epoch reset reliable admission gate is missing {token}")
    rx_body = function_body(wki_source, "wki_rx")
    require_order(
        rx_body,
        "if (epoch_reset_blocks_message(hdr->src_node, msg))",
        "bool const RELIABLE_RX_ACCEPTED",
        "epoch reset gate must drop before ACK advancement",
    )


def test_dev_op_lifecycle_is_payload_aware_but_epoch_gate_is_not() -> None:
    source = WKI_CPP.read_text()
    ipc_classifier = function_body(source, "dev_op_uses_ipc_lifecycle")
    require_tokens = [
        "type == MsgType::DEV_OP_REQ",
        "payload_len < sizeof(DevOpReqPayload)",
        "type == MsgType::DEV_OP_RESP",
        "payload_len < sizeof(DevOpRespPayload)",
        "op_id >= IPC_DEV_OP_MIN && op_id <= IPC_DEV_OP_MAX",
    ]
    missing = [token for token in require_tokens if token not in ipc_classifier]
    if missing:
        fail("payload-aware IPC DEV_OP lifecycle classifier is missing: " + ", ".join(missing))
    for token in [
        "type != MsgType::DEV_OP_REQ && type != MsgType::DEV_OP_RESP",
        "if (payload == nullptr)",
        "return true",
    ]:
        if token not in ipc_classifier:
            fail(f"malformed DEV_OP lifecycle classification is not conservative: missing {token}")

    lifecycle_body = function_body(source, "message_uses_device_session_lifecycle")
    if "return dev_op_uses_ipc_lifecycle(type, payload, payload_len);" not in lifecycle_body:
        fail("DEV_OP peer lifecycle must be limited to payload-classified IPC operations")

    epoch_sensitive = function_body(source, "message_is_epoch_reset_sensitive")
    for token in ["DEV_ATTACH_REQ", "DEV_ATTACH_ACK", "DEV_DETACH", "DEV_OP_REQ", "DEV_OP_RESP"]:
        if token not in epoch_sensitive:
            fail(f"epoch reset admission must remain type-wide for {token}")
    if "dev_op_uses_ipc_lifecycle" in epoch_sensitive:
        fail("epoch reset admission must not be narrowed to IPC DEV_OP")

    rx_body = function_body(source, "wki_rx")
    for token in [
        "message_uses_rx_peer_lifecycle(msg, payload, PAYLOAD_LEN)",
        "message_uses_rx_peer_lifecycle(RO_MSG, ch->reorder_head->data, ch->reorder_head->len)",
    ]:
        if token not in rx_body:
            fail(f"reliable RX must use payload-aware lifecycle classification: missing {token}")
    require_order(
        rx_body,
        "message_uses_rx_peer_lifecycle(msg, payload, PAYLOAD_LEN)",
        "if (epoch_reset_blocks_message(hdr->src_node, msg))",
        "DEV_OP lifecycle and epoch-reset admission order",
    )


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
        "wki_transport_send(fwd_transport, NEXT_HOP, fwd_frame, len)",
        "forwarding must transmit only after checksum refresh",
    )


def test_routed_lsa_peer_identity_handshake_does_not_promote_origin_to_neighbor() -> None:
    peer_source = PEER_CPP.read_text()
    routing_source = ROUTING_CPP.read_text()
    transport_source = TRANSPORT_ETH_CPP.read_text()
    wki_source = WKI_CPP.read_text()

    routed_hello = function_body(peer_source, "wki_peer_send_routed_hello")
    for token in [
        "hello.hostname = g_wki.local_hostname",
        "hello_set_boot_epoch(&hello, g_wki.local_boot_epoch)",
        "wki_send_raw(dst_node, MsgType::HELLO, &hello, sizeof(hello), WKI_FLAG_PRIORITY)",
    ]:
        if token not in routed_hello:
            fail(f"routed HELLO must reuse the established identity payload and raw routed send: {token}")

    handle_lsa = function_body(routing_source, "handle_lsa")
    require_order(
        handle_lsa,
        "wki_routing_recompute();",
        "probe_routed_peer(lsa->origin_node);",
        "LSA must compute SPF before probing the far endpoint identity",
    )
    timer = function_body(routing_source, "wki_routing_timer_tick")
    for token in ["ROUTED_HELLO_PROBE_INTERVAL_US", "probe_routed_peer(probe_nodes.at(i))"]:
        if token not in timer:
            fail(f"lost routed HELLOs must have a bounded periodic retry: {token}")

    contact = function_body(transport_source, "eth_rx_needs_peer_contact_update")
    require_order(
        contact,
        "wki_peer_frame_was_forwarded(hdr)",
        "static_cast<MsgType>(hdr->msg_type)",
        "forwarded origins must be rejected before direct Ethernet contact learning",
    )

    rx = function_body(wki_source, "wki_rx")
    for token in [
        "AUTHENTICATED_HELLO",
        "hdr->hop_ttl == 1",
        "wki_peer_note_rx_contact(transport, hdr->src_node, metadata->src_mac)",
    ]:
        if token not in rx:
            fail(f"authenticated TTL=1 HELLO direct-contact proof is missing: {token}")
    require_order(
        rx,
        "wki_auth_verify_frame",
        "wki_peer_note_rx_contact(transport, hdr->src_node, metadata->src_mac)",
        "direct contact must be learned only after frame authentication",
    )

    source_mac = function_body(transport_source, "wki_eth_transport_source_mac")
    for token in ["transport->tx != eth_wki_tx", "priv->netdev->mac"]:
        if token not in source_mac:
            fail(f"direct HELLO source-MAC selection is missing: {token}")
    direct_hello = function_body(peer_source, "wki_peer_send_hello")
    if "wki_eth_transport_source_mac(transport, hello.mac_addr)" not in direct_hello:
        fail("direct HELLO must advertise the selected Ethernet transport MAC")
    hello_ack = function_body(peer_source, "wki_peer_send_hello_ack")
    if "wki_eth_transport_source_mac(peer->transport, ack.mac_addr)" not in hello_ack:
        fail("direct HELLO_ACK must advertise the selected Ethernet transport MAC")

    find_transport = function_body(wki_source, "find_transport_for_peer")
    if "peer->is_direct && (peer->transport != nullptr)" not in find_transport:
        fail("a routed peer must not bypass current-SPF next-hop transport selection")

    for handler_name in ["handle_hello", "handle_hello_ack"]:
        handler = function_body(peer_source, handler_name)
        for token in [
            "wki_peer_hello_path(hdr, ROUTE_VALID, route.hop_count, peer != nullptr && peer->is_direct)",
            "HELLO_PATH == WkiPeerHelloPath::UNRESOLVED",
            "HELLO_PATH == WkiPeerHelloPath::ROUTED",
            "peer->next_hop = route.next_hop",
            "peer->hop_count = route.hop_count",
            "if (peer_is_direct)",
        ]:
            if token not in handler:
                fail(f"{handler_name} is missing routed peer classification token: {token}")
        require_order(
            handler,
            "HELLO_PATH == WkiPeerHelloPath::UNRESOLVED",
            "if (peer == nullptr)",
            f"{handler_name} must reject an early routed HELLO before allocating or promoting a peer",
        )


def test_all_configured_wki_nics_are_claimed_before_peer_discovery() -> None:
    config_source = NETDEVCONF_CPP.read_text()
    init_body = function_body(INIT_WRAPPERS_CPP.read_text(), "wki_eth_transport_init")
    transport_source = TRANSPORT_ETH_CPP.read_text()

    find_devices = function_body(config_source, "find_devices")
    for token in [
        'fw_cfg_read_file(NETDEVS_FW_CFG_PATH, buf.data(), buf.size() - 1)',
        "if (bytes_read > 0)",
        "while (*pos != '\\0')",
        "if (DRIVER_TOKEN == DRIVER_NAME)",
        "devices[device_count++] = std::move(dev_ref);",
        "return device_count;",
    ]:
        if token not in find_devices:
            fail(f"multi-NIC /etc/netdevs enumeration is missing {token}")
    if "return dev_ref;" in find_devices:
        fail("multi-NIC /etc/netdevs enumeration must not stop after the first match")
    require_order(
        find_devices,
        "fw_cfg_read_file(NETDEVS_FW_CFG_PATH, buf.data(), buf.size() - 1)",
        "vfs_open_file(NETDEVS_PATH, 0, 0)",
        "per-node fw_cfg NIC policy must take precedence over the shared initramfs fallback",
    )

    for token in [
        'find_devices("wki", wki_dev_refs)',
        "for (size_t i = 0; i < wki_dev_count; ++i)",
        "wki_eth_transport_claim(wki_dev_refs.at(i).get());",
        "wki_eth_transport_init(wki_dev_refs.front().get());",
        "wki_peer_send_hello_broadcast();",
    ]:
        if token not in init_body:
            fail(f"multi-NIC WKI boot claim is missing {token}")
    require_order(
        init_body,
        "wki_eth_transport_claim(wki_dev_refs.at(i).get());",
        "wki_eth_transport_init(wki_dev_refs.front().get());",
        "all configured WKI NICs must be claimed before primary transport initialization",
    )
    require_order(
        init_body,
        "wki_eth_transport_init(wki_dev_refs.front().get());",
        "wki_peer_send_hello_broadcast();",
        "peer discovery must begin only after all WKI NIC ownership is established",
    )

    claim = function_body(transport_source, "wki_eth_transport_claim")
    for token in ["netdev->remotable = nullptr;", "netdev->wki_transport = true;"]:
        if token not in claim:
            fail(f"WKI NIC claim is missing {token}")


def test_ipc_data_acks_after_ordered_dispatch() -> None:
    source = WKI_CPP.read_text()
    body = function_body(source, "wki_rx")
    required = [
        "ch->channel_id == WKI_CHAN_IPC_DATA",
        "bool const IMM_ACK = ((ch->priority == PriorityClass::LATENCY || ch->channel_id == WKI_CHAN_IPC_DATA) && ch->ack_pending);",
        "wki_dispatch_reliable_msg_ordered(ch, RX_CHANNEL_GENERATION, msg, hdr, payload, PAYLOAD_LEN);",
        "wki_dispatch_reliable_msg_ordered(ch, RO_CHANNEL_GENERATION, RO_MSG, &RO_HDR, ro_data, RO_LEN);",
        "complete_ack_transmit_for_generation_locked(ch, imm_ack_generation, imm_ack_num, tx_ret, notify_timer);",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("IPC_DATA post-dispatch ACK path is missing token(s): " + ", ".join(missing))
    require_order(
        body,
        "wki_dispatch_reliable_msg_ordered(ch, RX_CHANNEL_GENERATION, msg, hdr, payload, PAYLOAD_LEN);",
        "bool const IMM_ACK = ((ch->priority == PriorityClass::LATENCY || ch->channel_id == WKI_CHAN_IPC_DATA) && ch->ack_pending);",
        "IPC_DATA ACK gating must be computed after ordered local dispatch",
    )
    require_order(
        body,
        "wki_dispatch_reliable_msg_ordered(ch, RO_CHANNEL_GENERATION, RO_MSG, &RO_HDR, ro_data, RO_LEN);",
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
        "bool const ACTIVE = ch->active && ch->generation == generation",
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
        "ch->active && ch->generation == generation && ch->rx_dispatch_seq == seq",
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
        "wki_remote_vfs_cleanup_for_peer(fenced_id, owner_identity_replaced)",
        "peer fence must drain retained VFS handlers before closing their remote FDs",
    )


def test_fenced_logical_successor_terminalizes_old_detach_identity() -> None:
    peer_source = PEER_CPP.read_text()
    disconnect = function_body(peer_source, "wki_peer_disconnect_impl")
    for token in [
        "peer->retired_hostname = fenced_hostname",
        "reconcile_fenced_hostname_replacements_locked(fenced_hostname)",
        "owner_identity_replaced = peer->replacement_node_id.load(std::memory_order_acquire) != WKI_NODE_INVALID",
        "wki_remote_vfs_cleanup_for_peer(fenced_id, owner_identity_replaced)",
        "wki_remote_net_cleanup_for_peer(fenced_id, owner_identity_replaced)",
    ]:
        if token not in disconnect:
            fail(f"peer replacement cleanup is missing {token}")

    invalidation = function_body(WKI_CPP.read_text(), "wki_peer_remote_boot_epoch_invalidated")
    for token in [
        "PEER->replacement_node_id.load(std::memory_order_acquire)",
        "REPLACEMENT_NODE != WKI_NODE_INVALID",
        "CURRENT_EPOCH != expected_epoch",
    ]:
        if token not in invalidation:
            fail(f"deferred detach identity invalidation is missing {token}")


def test_authenticated_hello_requires_session_key_confirmation() -> None:
    auth = (ROOT / "modules/kern/src/net/wki/auth.cpp").read_text()
    rx = function_body(WKI_CPP.read_text(), "wki_rx")

    hello_case = rx[rx.index("case MsgType::HELLO:") : rx.index("case MsgType::HELLO_ACK:")]
    ack_case = rx[rx.index("case MsgType::HELLO_ACK:") : rx.index("case MsgType::HELLO_CONFIRM:")]
    confirm_case = rx[rx.index("case MsgType::HELLO_CONFIRM:") : rx.index("case MsgType::HEARTBEAT:")]

    require_order(
        hello_case,
        "wki_auth_prepare_peer_session(peer",
        "wki_peer_send_hello_ack(peer)",
        "a new HELLO must stage responder keys before emitting its challenge response",
    )
    if "if (SESSION_CURRENT)" not in hello_case:
        fail("a pending HELLO must not publish topology or resources before confirmation")
    for before, after in [
        ("wki_auth_install_peer_session(peer", "wki_peer_send_hello_confirm(peer)"),
        ("wki_peer_send_hello_confirm(peer)", "wki_lsa_replay_to_peer(peer->node_id)"),
    ]:
        require_order(
            ack_case,
            before,
            after,
            "the initiator must install the derived key and confirm it before publication",
        )
    for before, after in [
        ("AUTH_RESULT == WkiAuthFrameResult::PENDING_CONFIRMATION", "wki_auth_confirm_peer_session(peer)"),
        ("wki_auth_confirm_peer_session(peer)", "detail::handle_hello(transport, hdr, payload, PAYLOAD_LEN, true)"),
        ("detail::handle_hello(transport, hdr, payload, PAYLOAD_LEN, true)", "wki_lsa_replay_to_peer(peer->node_id)"),
    ]:
        require_order(
            confirm_case,
            before,
            after,
            "the responder must verify and promote pending keys before becoming connected",
        )

    verify = function_body(auth, "wki_auth_verify_frame")
    for token in [
        "MsgType::HELLO_CONFIRM",
        "peer->auth_session.pending",
        "trailer.session_id == peer->auth_session.pending_session_id",
        "trailer.counter == 1",
        "WkiAuthFrameResult::PENDING_CONFIRMATION",
    ]:
        if token not in verify:
            fail(f"pending-session confirmation gate is missing {token}")

    prepare = function_body(auth, "wki_auth_prepare_peer_session")
    for token in [
        "peer->auth_session.retired_session_valid",
        "erase_pending_session(peer->auth_session)",
        "peer->auth_session.pending = true",
    ]:
        if token not in prepare:
            fail(f"pending-session rollback protection is missing {token}")

    retire = function_body(auth, "wki_auth_retire_peer_session")
    if "erase_pending_session(peer->auth_session)" not in retire:
        fail("peer teardown must erase unconfirmed session keys")


def test_cluster_ivshmem_ownership_is_selected_before_generic_probe() -> None:
    ivshmem_init = function_body(INIT_WRAPPERS_CPP.read_text(), "ivshmem_init")
    for token in [
        'cmdline_has_token(get_kernel_cmdline(), "wki.ivshmem")',
        'fw_cfg_flag_enabled("opt/wos/wki-ivshmem")',
        "return;",
        "dev::ivshmem::ivshmem_net_init()",
    ]:
        if token not in ivshmem_init:
            fail(f"early WKI ivshmem ownership selection is missing {token}")
    require_order(
        ivshmem_init,
        'fw_cfg_flag_enabled("opt/wos/wki-ivshmem")',
        "dev::ivshmem::ivshmem_net_init()",
        "WKI ownership must be decided before the generic ivshmem probe",
    )


def test_ivshmem_direct_identity_is_bounded_and_authenticated() -> None:
    wki_rx = function_body(WKI_CPP.read_text(), "wki_rx")
    transport_source = TRANSPORT_IVSHMEM_CPP.read_text()
    drain = function_body(transport_source, "drain_rx_ring")

    require_order(
        wki_rx,
        "wki_version(hdr->version_flags) != WKI_VERSION",
        "wki_auth_verify_frame",
        "unsupported wire versions must fail before authentication",
    )
    require_order(
        wki_rx,
        "wki_auth_verify_frame",
        "metadata->has_direct_peer",
        "ivshmem contact metadata must be consumed only after authentication",
    )
    for token in [
        "metadata->direct_peer == hdr->src_node",
        "AUTHENTICATED_HELLO",
        "hdr->hop_ttl == 1",
        "wki_peer_note_rx_contact(transport, hdr->src_node, hello->mac_addr, false)",
    ]:
        if token not in wki_rx:
            fail(f"ivshmem direct-contact proof is missing {token}")

    for token in [
        "budget == 0",
        "priv->rx_drain_active.test_and_set(std::memory_order_acquire)",
        "processed < budget",
        ".has_direct_peer = priv->direct_peer != WKI_NODE_INVALID",
        ".direct_peer = priv->direct_peer",
        "priv->rx_drain_active.clear(std::memory_order_release)",
    ]:
        if token not in drain:
            fail(f"bounded single-drainer ivshmem receive path is missing {token}")

    poll = function_body(transport_source, "wki_ivshmem_transport_poll")
    if "drain_rx_ring(&s_ivshmem_priv, budget)" not in poll:
        fail("rootless ivshmem polling must share the bounded IRQ receive path")


def main() -> None:
    test_hello_ack_reconnects_fenced_peer_outside_peer_lock()
    test_hello_boot_epoch_fences_connected_broadcast_restarts()
    test_connected_epoch_reset_retires_vfs_before_channel_reuse()
    test_dev_op_lifecycle_is_payload_aware_but_epoch_gate_is_not()
    test_reliable_rx_rejects_fenced_peer_before_channel_lookup()
    test_fence_notify_rejects_invalid_targets_before_routing()
    test_shutdown_uses_graceful_goodbye_not_fence()
    test_ack_only_frames_do_not_refresh_peer_liveness()
    test_forwarding_recomputes_checksum_after_ttl_decrement()
    test_routed_lsa_peer_identity_handshake_does_not_promote_origin_to_neighbor()
    test_all_configured_wki_nics_are_claimed_before_peer_discovery()
    test_ipc_data_acks_after_ordered_dispatch()
    test_ipc_data_ordered_dispatch_wait_uses_explicit_daemon_wake()
    test_cross_channel_ack_scan_uses_allocated_range_bounds()
    test_fence_drains_deferred_vfs_bindings_before_remote_fd_cleanup()
    test_fenced_logical_successor_terminalizes_old_detach_identity()
    test_authenticated_hello_requires_session_key_confirmation()
    test_cluster_ivshmem_ownership_is_selected_before_generic_probe()
    test_ivshmem_direct_identity_is_bounded_and_authenticated()
    print("WKI peer source invariants hold")


if __name__ == "__main__":
    main()
