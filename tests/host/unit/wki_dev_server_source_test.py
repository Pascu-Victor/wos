#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DEV_SERVER_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "dev_server.cpp"
DEV_SERVER_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "dev_server.hpp"
REMOTE_NET_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_net.cpp"
REMOTE_NET_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_net.hpp"
REMOTE_VFS_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_vfs.hpp"
WIRE_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wire.hpp"
WKI_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.cpp"
WKI_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.hpp"
TRANSPORT_ETH_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "transport_eth.cpp"
TRANSPORT_IVSHMEM_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "transport_ivshmem.cpp"
TRANSPORT_ROCE_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "transport_roce.cpp"
WKI_DEV_SERVER_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "wki_dev_server_ktest.cpp"


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


def block_body_after(source: str, marker: str) -> str:
    marker_pos = source.find(marker)
    if marker_pos < 0:
        fail(f"missing block marker {marker}")
    brace_pos = source.find("{", marker_pos + len(marker))
    if brace_pos < 0:
        fail(f"missing block after {marker}")

    depth = 1
    pos = brace_pos + 1
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated block after {marker}")
    return source[brace_pos + 1 : pos - 1]


def require_order(body: str, before: str, after: str, context: str) -> None:
    before_pos = body.find(before)
    after_pos = body.find(after)
    if before_pos < 0 or after_pos < 0 or before_pos >= after_pos:
        fail(f"{context}: expected {before!r} before {after!r}")


def test_rx_forward_does_not_spend_credits_before_delivery_is_possible() -> None:
    body = function_body(DEV_SERVER_CPP.read_text(), "wki_dev_server_forward_net_rx")
    required = [
        "DST == dev->mac",
        "if (IS_OWNER_UNICAST)",
        "return;",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("RX forwarding must skip owner-local unicast before mirroring into WKI: " + ", ".join(missing))
    require_order(body, "if (IS_OWNER_UNICAST)", "s_server_lock.lock_irqsave", "owner-local unicast skip before binding scan")
    require_order(body, "req_total > WKI_ETH_MAX_PAYLOAD", "s_server_lock.lock_irqsave", "RX forwarding size gate")
    require_order(body, "IS_BROADCAST && !b.net_rx_filter.accept_broadcast", "b.net_rx_credits--", "broadcast filter")
    require_order(body, "IS_MULTICAST && !b.net_rx_filter.accept_multicast", "b.net_rx_credits--", "multicast filter")
    require_order(body, "target_count < MAX_RX_TARGETS", "b.net_rx_credits--", "target capacity")


def test_rx_forward_sends_notify_cookie_envelope() -> None:
    body = function_body(DEV_SERVER_CPP.read_text(), "wki_dev_server_forward_net_rx")
    required = [
        "sizeof(NetNotifyHeader)",
        ".attach_cookie = b.attach_cookie",
        "notify->magic = WKI_NET_NOTIFY_MAGIC",
        "notify->attach_cookie = target.attach_cookie",
        "notify->reserved = 0",
        "notify->data_len = pkt_data_len",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("RX forwarding is missing NET notify cookie envelope tokens: " + ", ".join(missing))


def test_state_notify_sends_notify_cookie_envelope() -> None:
    body = function_body(DEV_SERVER_CPP.read_text(), "wki_dev_server_notify_net_changed")
    required = [
        "sizeof(NetNotifyHeader)",
        ".attach_cookie = b.attach_cookie",
        "notify->magic = WKI_NET_NOTIFY_MAGIC",
        "notify->attach_cookie = target.attach_cookie",
        "notify->data_len = sizeof(NetStateNotifyPayload)",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("state notify is missing NET notify cookie envelope tokens: " + ", ".join(missing))


def test_net_dispatch_does_not_pass_server_binding_pointer() -> None:
    remote_header = REMOTE_NET_HPP.read_text()
    remote_source = REMOTE_NET_CPP.read_text()
    server_body = function_body(DEV_SERVER_CPP.read_text(), "handle_dev_op_req")

    if "binding_ptr" in remote_header or "binding_ptr" in remote_source:
        fail("remote NET handler must not accept an erasable DevServerBinding pointer")
    expected_call = "detail::handle_net_op(hdr, CHANNEL_IDENTITY, binding_net_dev, req->op_id, req_data, REQ_DATA_LEN);"
    if expected_call not in server_body:
        fail("handle_dev_op_req() must dispatch NET ops with the exact channel identity and snapshotted net_dev")


def test_vfs_attach_uses_export_snapshot_not_unlocked_pointer() -> None:
    remote_header = REMOTE_VFS_HPP.read_text()
    attach_body = function_body(DEV_SERVER_CPP.read_text(), "handle_dev_attach_req")

    if "wki_remote_vfs_find_export(uint32_t resource_id) -> VfsExport*" in remote_header:
        fail("remote VFS export lookup must not expose unlocked VfsExport pointers")
    if "wki_remote_vfs_find_export(req->resource_id)" in attach_body or "exp->" in attach_body:
        fail("VFS attach must consume a copied export snapshot instead of an unlocked export pointer")

    required = [
        "VfsExport exp = {};",
        "if (!wki_remote_vfs_find_export_snapshot(req->resource_id, &exp))",
        "memcpy(static_cast<void*>(binding.vfs_export_path), static_cast<const void*>(exp.export_path),",
        "std::min(sizeof(binding.vfs_export_path), sizeof(exp.export_path))",
        "memcpy(static_cast<void*>(binding.vfs_export_name), static_cast<const void*>(exp.name),",
        "std::min(sizeof(binding.vfs_export_name), sizeof(exp.name))",
        "binding.vfs_export_path[sizeof(binding.vfs_export_path) - 1] = '\\0';",
        "binding.vfs_export_name[sizeof(binding.vfs_export_name) - 1] = '\\0';",
        "static_cast<const char*>(exp.name)",
    ]
    missing = [token for token in required if token not in attach_body]
    if missing:
        fail("VFS attach is missing export snapshot/copy tokens: " + ", ".join(missing))

    require_order(
        attach_body,
        "VfsExport exp = {};",
        "if (!wki_remote_vfs_find_export_snapshot(req->resource_id, &exp))",
        "VFS attach snapshot declaration before lookup",
    )
    require_order(
        attach_body,
        "if (!wki_remote_vfs_find_export_snapshot(req->resource_id, &exp))",
        "reserve_attach_channel(hdr->src_node, req->requested_channel, PriorityClass::LATENCY",
        "VFS attach validates export before reserving channel",
    )
    require_order(
        attach_body,
        "wki_remote_vfs_find_export_snapshot(req->resource_id, &exp)",
        "memcpy(static_cast<void*>(binding.vfs_export_path), static_cast<const void*>(exp.export_path),",
        "VFS attach copies from snapshot",
    )


def test_vfs_notify_coalesces_by_mount_anchor_with_conservative_fallback() -> None:
    source = DEV_SERVER_CPP.read_text()
    body = function_body(source, "wki_dev_server_send_vfs_notify")
    attach = function_body(source, "handle_dev_attach_req")
    header = DEV_SERVER_HPP.read_text()

    header_tokens = [
        "bool vfs_lane_anchor = true;",
        "vfs_lane_anchor(o.vfs_lane_anchor)",
        "vfs_lane_anchor = o.vfs_lane_anchor;",
    ]
    missing = [token for token in header_tokens if token not in header]
    if missing:
        fail("VFS binding anchor identity must survive binding moves: " + ", ".join(missing))
    require_order(
        attach,
        "wki_peer_capability_negotiated(hdr->src_node, WKI_CAP_VFS_MULTI_RDMA_LANES)",
        "binding.vfs_lane_anchor = wki_vfs_attach_lane_is_anchor(req->attach_mode, MULTI_RDMA_LANES)",
        "VFS attach classifies the logical notification anchor through negotiated wire semantics",
    )

    required = [
        "uint16_t consumer_node",
        "WkiChannelIdentity channel_identity",
        "bool lane_anchor",
        ".consumer_node = binding.consumer_node",
        ".channel_identity = binding.channel_identity",
        ".lane_anchor = binding.vfs_lane_anchor",
        "auto consumer_has_anchor",
        "std::ranges::any_of(targets",
        "candidate.consumer_node == consumer_node && candidate.lane_anchor",
        "!target.lane_anchor",
        "!consumer_has_anchor(target.consumer_node)",
        "fallback.lane_anchor",
        "fallback.consumer_node != target.consumer_node",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("VFS invalidation targets must retain anchor and conservative fallback identity: " + ", ".join(missing))

    require_order(body, "s_server_lock.lock_irqsave", "for (const auto& binding : g_bindings)", "VFS notify snapshot lock")
    require_order(body, ".lane_anchor = binding.vfs_lane_anchor", "s_server_lock.unlock_irqrestore", "VFS notify snapshot unlock")
    require_order(body, "s_server_lock.unlock_irqrestore", "auto consumer_has_anchor", "anchor lookup uses the unlocked snapshot")
    require_order(body, "auto consumer_has_anchor", "for (const auto& target : targets)", "VFS notify sends after anchor lookup")
    require_order(
        body,
        "if (!consumer_has_anchor(target.consumer_node))",
        "wki_send_on_channel_identity(target.channel_identity",
        "orphaned auxiliary lane notification",
    )
    require_order(
        body,
        "wki_send_on_channel_identity(target.channel_identity",
        "for (const auto& fallback : targets)",
        "fallback only after the primary anchor send",
    )
    require_order(
        body,
        "fallback.consumer_node != target.consumer_node",
        "wki_send_on_channel_identity(fallback.channel_identity",
        "same-consumer auxiliary fallback",
    )

    if body.count("wki_send_on_channel_identity(") != 3:
        fail("VFS invalidation must send from orphaned auxiliaries, anchors, and failed-anchor fallback sites")
    anchor_lookup = re.search(
        r"auto\s+consumer_has_anchor\s*=.*?std::ranges::any_of\(\s*targets\s*,.*?"
        r"return\s+candidate\.consumer_node\s*==\s*consumer_node\s*&&\s*candidate\.lane_anchor\s*;",
        body,
        re.DOTALL,
    )
    if anchor_lookup is None:
        fail("auxiliary teardown handling must find a surviving same-consumer anchor in the target snapshot")
    auxiliary_branch = block_body_after(body, "if (!target.lane_anchor)")
    orphaned_auxiliary = block_body_after(auxiliary_branch, "if (!consumer_has_anchor(target.consumer_node))")
    direct_send = "wki_send_on_channel_identity(target.channel_identity"
    if direct_send not in orphaned_auxiliary or auxiliary_branch.count(direct_send) != 1:
        fail("an auxiliary lane must notify directly only when its consumer has no surviving anchor")
    if not auxiliary_branch.strip().endswith("continue;"):
        fail("auxiliary notification handling must not fall through to the anchor send path")
    successful_anchor = re.search(
        r"if\s*\(\s*wki_send_on_channel_identity\(\s*target\.channel_identity.*?==\s*WKI_OK\s*\)\s*\{\s*continue\s*;\s*\}",
        body,
        re.DOTALL,
    )
    if successful_anchor is None:
        fail("a successful VFS anchor notification must suppress auxiliary fallback notifications")
    fallback_filter = re.search(
        r"for\s*\(\s*const auto& fallback\s*:\s*targets\s*\)\s*\{\s*"
        r"if\s*\(\s*fallback\.lane_anchor\s*\|\|\s*fallback\.consumer_node\s*!=\s*target\.consumer_node\s*\)\s*"
        r"\{\s*continue\s*;\s*\}.*?wki_send_on_channel_identity\(\s*fallback\.channel_identity",
        body,
        re.DOTALL,
    )
    if fallback_filter is None:
        fail("failed anchor notifications must fan out to every auxiliary lane of the same consumer")


def test_duplicate_net_attach_does_not_rewrite_binding_cookie() -> None:
    helper = function_body(DEV_SERVER_CPP.read_text(), "find_existing_net_binding")
    server_body = function_body(DEV_SERVER_CPP.read_text(), "handle_dev_attach_req")
    if "binding.attach_cookie =" in helper:
        fail("duplicate NET attach lookup must not rewrite the existing binding cookie")
    required = [
        "binding.attach_cookie != attach_cookie",
        "info.attach_cookie = binding.attach_cookie",
        "find_existing_net_binding(hdr->src_node, req->resource_id, req->attach_cookie)",
        "existing_ack.attach_cookie = existing.attach_cookie",
    ]
    missing = [token for token in required if token not in helper + server_body]
    if missing:
        fail("duplicate NET attach must echo the existing binding cookie: " + ", ".join(missing))


def test_net_binding_state_mutation_revalidates_under_server_lock() -> None:
    body = function_body(REMOTE_NET_CPP.read_text(), "handle_net_op")
    required = [
        "wki_dev_server_mark_net_opened(channel_identity, net_dev, true)",
        "wki_dev_server_mark_net_opened(channel_identity, net_dev, false)",
        "wki_dev_server_add_net_rx_credits(channel_identity, net_dev, credits)",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("handle_net_op() is missing locked server binding helper calls: " + ", ".join(missing))
    forbidden = ["binding->net_nic_opened", "binding->net_rx_credits"]
    present = [token for token in forbidden if token in body]
    if present:
        fail("handle_net_op() still mutates DevServerBinding directly: " + ", ".join(present))


def test_net_notify_handlers_validate_cookie_envelope() -> None:
    source = REMOTE_NET_CPP.read_text()
    for name in ("handle_net_rx_notify", "handle_net_state_notify"):
        body = function_body(source, name)
        required = [
            "NetNotifyHeader",
            "wki_net_notify_header_matches_expected(state->attach_cookie, *notify)",
            "wki_net_notify_payload_fits(data_len, *notify)",
        ]
        missing = [token for token in required if token not in body]
        if missing:
            fail(f"{name} is missing NET notify cookie validation tokens: " + ", ".join(missing))


def test_net_rx_credit_accounting_uses_proxy_lock() -> None:
    source = REMOTE_NET_CPP.read_text()
    source_without_helpers = source
    for helper_name in ("set_net_rx_credits_locked", "consume_net_rx_credit_locked"):
        source_without_helpers = source_without_helpers.replace(function_body(source, helper_name), "")
    helper = function_body(source, "consume_net_rx_credit_locked")
    rx_body = function_body(source, "handle_net_rx_notify")
    open_body = function_body(source, "proxy_net_open")

    if "state->rx_credits_remaining--" not in helper:
        fail("consume_net_rx_credit_locked() must be the only RX credit decrement site")
    if re.search(r"rx_credits_remaining\s*=", source_without_helpers):
        fail("RX credits must be assigned through the proxy-lock helper")
    for body_name, body in (("handle_net_rx_notify", rx_body), ("proxy_net_open", open_body)):
        if "rx_credits_remaining--" in body:
            fail(f"{body_name} must not decrement RX credits outside the proxy-lock helper")

    require_order(
        rx_body,
        "state->lock.lock();\n    uint16_t const REPLENISH = consume_net_rx_credit_locked(state);",
        "state->lock.unlock();\n    if (REPLENISH != 0)",
        "RX notify credit replenishment",
    )
    require_order(
        open_body,
        "state->lock.lock();\n        uint16_t const INITIAL_CREDITS = set_net_rx_credits_locked(state, WKI_NET_RX_CREDITS);",
        "state->lock.unlock();",
        "initial RX credit grant",
    )


def test_net_error_responses_echo_payload_cookie() -> None:
    body = function_body(DEV_SERVER_CPP.read_text(), "handle_dev_op_req")
    required = [
        "req->op_id >= OP_NET_XMIT && req->op_id <= OP_NET_STATE_NOTIFY",
        "net_request_cookie_from_payload(req_data, REQ_DATA_LEN, REQUEST_COOKIE)",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("handle_dev_op_req() is missing NET payload-cookie error response handling: " + ", ".join(missing))


def test_proxy_net_xmit_rejects_oversize_before_uint16_truncation() -> None:
    header = REMOTE_NET_HPP.read_text()
    source = REMOTE_NET_CPP.read_text()
    size_body = function_body(header, "wki_remote_net_xmit_request_size")
    xmit_body = function_body(source, "proxy_net_xmit")

    required_size_tokens = [
        "constexpr size_t HEADER_LEN = sizeof(DevOpReqPayload)",
        "packet_len > WKI_ETH_MAX_PAYLOAD - HEADER_LEN",
        "static_cast<uint16_t>(HEADER_LEN + packet_len)",
    ]
    missing = [token for token in required_size_tokens if token not in size_body]
    if missing:
        fail("remote NET xmit request size helper is missing overflow-safe tokens: " + ", ".join(missing))
    if "static_cast<uint16_t>(sizeof(DevOpReqPayload) + packet_len)" in size_body:
        fail("remote NET xmit size helper must not truncate before validating packet_len")

    required_xmit_tokens = [
        "WkiRemoteNetXmitRequestSize const REQ_SIZE = wki_remote_net_xmit_request_size(pkt->len)",
        "if (!REQ_SIZE.ok)",
        "uint16_t const REQ_TOTAL = REQ_SIZE.total_len",
        "new (std::nothrow) uint8_t[REQ_TOTAL]",
        "wki_send_on_channel_identity(state->channel_identity, MsgType::DEV_OP_REQ, req_buf, REQ_TOTAL)",
    ]
    missing = [token for token in required_xmit_tokens if token not in xmit_body]
    if missing:
        fail("proxy_net_xmit must validate total request length before allocation/copy: " + ", ".join(missing))
    if "static_cast<uint16_t>(sizeof(DevOpReqPayload) + pkt->len)" in xmit_body:
        fail("proxy_net_xmit must not cast the packet request length before checking bounds")
    require_order(xmit_body, "if (!REQ_SIZE.ok)", "new (std::nothrow) uint8_t[REQ_TOTAL]", "remote NET xmit size gate")


def test_detach_cookie_and_incarnation_are_exact_matches() -> None:
    wire = WIRE_HPP.read_text()
    cookie_match = function_body(wire, "wki_dev_detach_cookie_matches_binding")
    incarnation_match = function_body(wire, "wki_dev_detach_incarnation_matches_binding")
    source = DEV_SERVER_CPP.read_text()
    decode = function_body(source, "decode_detach_request")
    admit = function_body(source, "wki_dev_server_admit_detach_rx")

    if "return binding_attach_cookie == request_cookie;" not in cookie_match:
        fail("detach cookie matching must not treat zero as a wildcard for nonzero-cookie bindings")
    for token in [
        "type == ResourceType::BLOCK || type == ResourceType::VFS",
        "wki_resource_incarnation_valid(request_incarnation)",
        "wki_resource_incarnation_equal(binding_incarnation, request_incarnation)",
    ]:
        if token not in wire + incarnation_match:
            fail(f"detach incarnation matching is missing {token!r}")

    required = [
        "request.detach.target_node != g_wki.my_node_id",
        "auto const DETACH_TYPE = static_cast<ResourceType>(request.detach.resource_type)",
        "wki_resource_incarnation_negotiated(hdr->src_node, DETACH_TYPE)",
        "wki_dev_detach_payload_size_matches(payload_len, WITH_INCARNATION)",
        "payload + WKI_DEV_DETACH_INCARNATION_OFFSET",
        "wki_dev_detach_cookie_matches_binding(binding.attach_cookie, request.attach_cookie)",
        "wki_dev_detach_incarnation_matches_binding(binding.resource_incarnation, request.incarnation",
    ]
    missing = [token for token in required if token not in decode + admit]
    if missing:
        fail("DEV_DETACH must validate its exact negotiated form and binding identity: " + ", ".join(missing))
    require_order(decode, "wki_dev_detach_payload_size_matches", "*request_out = request", "detach length gate before admission")
    require_order(
        admit,
        "wki_dev_detach_incarnation_matches_binding",
        "mark_binding_retiring_locked(binding)",
        "detach incarnation gate before retirement",
    )


def test_detach_admission_is_napi_safe_and_cleanup_is_deferred() -> None:
    source = DEV_SERVER_CPP.read_text()
    header = DEV_SERVER_HPP.read_text()
    admit = function_body(source, "wki_dev_server_admit_detach_rx")
    worker = function_body(source, "wki_dev_server_process_pending_detaches")

    for token in [
        "mark_binding_retiring_locked(binding)",
        "binding.detach_cleanup_pending = true",
        "binding.detach_cleanup_claimed = false",
    ]:
        if token not in admit:
            fail(f"DEV_DETACH admission is missing {token!r}")
    for forbidden in [
        "new ",
        "delete",
        "kern_yield",
        "wait_for_binding_refs_to_drain",
        "wki_remote_vfs_mark_server_fds_for_channel",
        "wki_zone_destroy",
        "on_remote_detach",
        "wki_channel_close",
        "wki_resource_advertise_all",
        "dbg::log",
    ]:
        if forbidden in admit:
            fail(f"DEV_DETACH admission must remain NAPI-safe: found {forbidden!r}")

    for token in [
        "binding.detach_cleanup_pending",
        "binding.detach_cleanup_claimed",
        "wait_for_binding_refs_to_drain(item.binding)",
        "wki_remote_vfs_mark_server_fds_for_channel(item.channel_identity)",
        "release_vfs_rdma_buffers(&item.vfs_rdma_buffers)",
        "wki_zone_destroy(item.blk_zone_id)",
        "item.block_dev->remotable->on_remote_detach(item.consumer_node)",
        "item.net_dev->remotable->on_remote_detach(item.consumer_node)",
        "wki_channel_close_generation(item.channel_identity.channel",
        "erase_retired_binding_locked(item.binding)",
        "wki_resource_advertise_all()",
    ]:
        if token not in worker:
            fail(f"task-context DEV_DETACH cleanup is missing {token!r}")
    require_order(worker, "binding.detach_cleanup_claimed = true", "wait_for_binding_refs_to_drain(item.binding)", "detach claim")
    require_order(
        worker,
        "wait_for_binding_refs_to_drain(item.binding)",
        "release_vfs_rdma_buffers(&item.vfs_rdma_buffers)",
        "detach VFS RDMA ref drain",
    )
    require_order(worker, "wki_channel_close_generation(item.channel_identity.channel", "erase_retired_binding_locked(item.binding)", "detach erase")

    if "handle_dev_detach" in source or "handle_dev_detach" in header:
        fail("DEV_DETACH must not retain a later blocking inline dispatch handler")
    for token in [
        "auto wki_dev_server_admit_detach_rx(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) -> bool;",
        "void wki_dev_server_process_pending_detaches();",
    ]:
        if token not in header:
            fail(f"dev-server detach API declaration is missing {token!r}")


def test_replacement_attach_waits_for_pending_detach_cleanup() -> None:
    source = DEV_SERVER_CPP.read_text()
    header = DEV_SERVER_HPP.read_text()
    gate = function_body(source, "wki_dev_server_attach_blocked_by_pending_detach")

    required = [
        "request.target_node != g_wki.my_node_id",
        "decode_attach_resource_incarnation(hdr->src_node, RESOURCE_TYPE, payload, payload_len",
        "s_server_lock.lock_irqsave()",
        "!binding.active",
        "binding.retiring.load(std::memory_order_acquire)",
        "binding.detach_cleanup_pending",
        "binding.consumer_node == hdr->src_node",
        "binding.resource_type == RESOURCE_TYPE",
        "binding.resource_id == request.resource_id",
    ]
    missing = [token for token in required if token not in gate]
    if missing:
        fail("replacement attach gate is missing pending-detach identity checks: " + ", ".join(missing))
    for forbidden in ["new ", "delete", "kern_yield", "wki_send", "wki_channel_close", "wki_zone_destroy"]:
        if forbidden in gate:
            fail(f"replacement attach gate must remain fixed-lock/nonblocking: found {forbidden!r}")
    if "wki_dev_server_attach_blocked_by_pending_detach" not in header:
        fail("replacement attach gate must be public for reliable RX pre-ACK admission")


def test_retirement_has_one_cleanup_owner_and_preserves_block_writer_exclusion() -> None:
    source = DEV_SERVER_CPP.read_text()
    header = DEV_SERVER_HPP.read_text()
    ktest = WKI_DEV_SERVER_KTEST.read_text()
    vfs_claim = function_body(source, "vfs_reconciliation_may_claim_binding")
    vfs_finish = function_body(source, "wki_dev_server_finish_vfs_export_reconciliation")
    writer_reservation = function_body(source, "binding_reserves_block_writer")
    writer_scan = function_body(source, "block_has_remote_writer_locked")
    detach_claim = function_body(source, "detach_all_may_claim_binding")
    detach_all = function_body(source, "wki_dev_server_detach_all_for_peer")

    for token in [
        "binding.resource_type == ResourceType::VFS",
        "binding.active",
        "!binding.retiring.load(std::memory_order_acquire)",
        "binding.vfs_export_revision_seen != target_even_revision",
    ]:
        if token not in vfs_claim:
            fail(f"VFS reconciliation cleanup claim is missing {token!r}")
    if "vfs_reconciliation_may_claim_binding(binding, target_even_revision)" not in vfs_finish:
        fail("VFS export reconciliation must not claim a binding already owned by retirement cleanup")
    require_order(
        vfs_finish,
        "vfs_reconciliation_may_claim_binding(binding, target_even_revision) || binding.vfs_lane_anchor",
        "if (retired == nullptr)",
        "VFS reconciliation scans auxiliary bindings before anchors",
    )
    require_order(
        vfs_finish,
        "if (retired == nullptr)",
        "mark_binding_retiring_locked(*retired)",
        "VFS reconciliation preserves an anchor until auxiliary rows retire",
    )

    for token in ["binding.resource_type == ResourceType::BLOCK", "!binding.block_read_only"]:
        if token not in writer_reservation:
            fail(f"BLOCK writer reservation is missing {token!r}")
    for forbidden in ["binding.active", "binding.retiring"]:
        if forbidden in writer_reservation:
            fail(f"BLOCK writer reservation must persist through retirement: found {forbidden!r}")
    if "binding_reserves_block_writer(binding)" not in writer_scan:
        fail("BLOCK overlap scan must use the through-erasure writer reservation")

    for token in ["binding.detach_cleanup_pending", "binding.epoch_reset_pending", "binding.active", "binding.retiring.load"]:
        if token not in detach_claim:
            fail(f"peer detach-all cleanup-owner classifier is missing {token!r}")

    for token in [
        "wki_dev_server_process_pending_detaches()",
        "peer_has_binding_locked(node_id)",
        "peer_binding_remains",
        "detach_all_may_claim_binding(b, node_id)",
        "ker::mod::sched::kern_yield()",
        "wki_remote_vfs_process_pending_server_fd_cleanup()",
    ]:
        if token not in detach_all:
            fail(f"peer detach-all must join every same-peer cleanup owner: missing {token!r}")
    require_order(
        detach_all,
        "wki_dev_server_process_pending_detaches()",
        "// Collect work items and cleanup info under the lock",
        "drive pending cleanup before peer-owned work",
    )
    require_order(
        detach_all,
        "erase_retired_binding_locked(work.at(i).binding)",
        "wki_remote_vfs_process_pending_server_fd_cleanup()",
        "all binding owners must finish before marked VFS FD drain",
    )
    if "RECONCILIATION_OWNER_JOINED" not in source:
        fail("retirement selftest must cover reconciliation-owned peer rows")

    if "auto wki_dev_server_selftest_retirement_ownership_guards() -> bool;" not in header:
        fail("retirement ownership selftest must be declared")
    for token in [
        "KTEST(WkiDevServerBinding, RetirementOwnershipAndWriterReservationPersistUntilErase)",
        "wki_dev_server_selftest_retirement_ownership_guards()",
    ]:
        if token not in ktest:
            fail(f"retirement ownership KTEST coverage is missing {token!r}")


def test_block_attach_transfers_persistent_writer_lease() -> None:
    source = DEV_SERVER_CPP.read_text()
    header = DEV_SERVER_HPP.read_text()
    ktest = WKI_DEV_SERVER_KTEST.read_text()
    attach = function_body(source, "handle_dev_attach_req")

    for token in [
        "dev::BlockWriterLease block_writer_lease{}",
        "block_writer_lease(std::move(o.block_writer_lease))",
        "block_writer_lease = std::move(o.block_writer_lease)",
    ]:
        if token not in header:
            fail(f"DevServerBinding writer lease move lifecycle is missing {token!r}")
    for token in [
        "ker::dev::BlockWriterLease block_writer_lease",
        "block_writer_lease.try_acquire(bdev, ker::dev::BlockWriterLeaseOwner::REMOTE_BINDING)",
        "binding.block_writer_lease = std::move(block_writer_lease)",
        "g_bindings.push_back(std::move(binding))",
    ]:
        if token not in attach:
            fail(f"BLOCK attach writer lease publication is missing {token!r}")
    require_order(attach, "block_writer_lease.try_acquire", "reserve_attach_channel", "writer lease before channel/callback setup")
    require_order(attach, "binding.block_writer_lease = std::move(block_writer_lease)", "g_bindings.push_back(std::move(binding))", "lease transfer before binding publication")
    if "mounted_block_device_overlaps(bdev)" in attach:
        fail("BLOCK attach must not retain the split mount-table TOCTOU check")
    for token in [
        "KTEST(WkiDevServerBinding, MoveTransfersBlockWriterLeaseExactlyOnce)",
        "wki_dev_server_selftest_block_writer_lease_transfer()",
    ]:
        if token not in ktest:
            fail(f"DevServerBinding writer lease transfer KTEST is missing {token!r}")


def test_net_forward_hook_removal_revalidates_after_cleanup() -> None:
    source = DEV_SERVER_CPP.read_text()
    helper = function_body(source, "uninstall_net_rx_forward_if_unused")
    worker = function_body(source, "wki_dev_server_process_pending_detaches")
    attach = function_body(source, "handle_dev_attach_req")

    for token in [
        "s_server_lock.lock_irqsave()",
        "!has_net_binding_for_dev(dev)",
        "dev->wki_rx_forward.load(std::memory_order_acquire) == wki_dev_server_forward_net_rx",
        "dev->wki_rx_forward.store(nullptr, std::memory_order_release)",
    ]:
        if token not in helper:
            fail(f"NET hook revalidation helper is missing {token!r}")
    require_order(worker, "erase_retired_binding_locked(item.binding)", "uninstall_net_rx_forward_if_unused(item.net_dev)", "NET hook apply")

    net_publication = attach[attach.find("binding.net_dev = ndev;") :]
    require_order(
        net_publication,
        "g_bindings.push_back(std::move(binding))",
        "ndev->wki_rx_forward.store(wki_dev_server_forward_net_rx, std::memory_order_release)",
        "NET hook publication after binding publication",
    )
    require_order(
        net_publication,
        "ndev->wki_rx_forward.store(wki_dev_server_forward_net_rx, std::memory_order_release)",
        "s_server_lock.unlock_irqrestore(SRV_FLAGS)",
        "NET hook publication under server lock",
    )

    for name in ("wki_dev_server_cleanup_epoch_reset_for_peer", "wki_dev_server_detach_all_for_peer"):
        body = function_body(source, name)
        if "uninstall_net_rx_forward_if_unused(work.at(i).net_dev)" not in body:
            fail(f"{name} must revalidate NET hook removal after cleanup")
        if "->wki_rx_forward.store(nullptr" in body or "->wki_rx_forward = nullptr" in body:
            fail(f"{name} must not apply a stale pre-cleanup NET hook decision")


def test_channel_close_serializes_pool_reuse_until_reset_complete() -> None:
    body = function_body(WKI_CPP.read_text(), "wki_channel_close")
    require_order(body, "s_channel_pool_lock.lock();", "ch->lock.lock();", "channel close lock order")
    require_order(body, "ch->active = false;", "wki_channel_reset(ch);", "channel close reset")
    require_order(body, "wki_channel_reset(ch);", "peer->channels.at(CHANNEL_ID) = nullptr;", "channel close peer index cleanup")
    require_order(body, "ch->lock.unlock();", "s_channel_pool_lock.unlock();", "channel close unlock order")


def test_peer_channel_close_clears_index_before_unlocking_pool() -> None:
    body = function_body(WKI_CPP.read_text(), "wki_channels_close_for_peer")
    require_order(body, "s_channel_pool_lock.lock();", "peer->channels.fill(nullptr);", "peer close index cleanup lock")
    require_order(body, "peer->channels.fill(nullptr);", "s_channel_pool_lock.unlock();", "peer close index cleanup unlock")
    require_order(body, "ch->active = false;", "wki_channel_reset(ch);", "peer close reset")
    if body.find("s_channel_pool_lock.unlock();") < body.find("peer->channels.fill(nullptr);"):
        fail("peer-wide channel close must not clear peer->channels after dropping the pool lock")


def test_reliable_rx_does_not_autocreate_allocated_dynamic_channels() -> None:
    source = WKI_CPP.read_text()
    policy = function_body(source, "channel_requires_existing_rx_reservation")
    helper = function_body(source, "wki_channel_get_for_reliable_rx")
    rx_body = function_body(source, "wki_rx")

    required = [
        "channel_id >= WKI_CHAN_DYNAMIC_BASE && channel_id < WKI_CHAN_DYNAMIC_RESERVED_BASE",
        "return wki_channel_lookup(peer_node, channel_id);",
        "return wki_channel_get(peer_node, channel_id);",
        "wki_channel_get_for_reliable_rx(hdr->src_node, hdr->channel_id)",
    ]
    missing = [token for token in required if token not in policy + helper + rx_body]
    if missing:
        fail("reliable RX must require existing reservations for allocated dynamic channels: " + ", ".join(missing))


def test_channel_reuse_generation_guards_unlock_tx_relock_paths() -> None:
    source = WKI_CPP.read_text()
    channel_alloc_body = function_body(source, "channel_pool_alloc")
    channel_init_body = function_body(source, "channel_init")
    timer_single_body = function_body(source, "wki_timer_tick_single")
    timer_body = function_body(source, "wki_timer_tick")

    if "uint32_t generation = 0;" not in (ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.hpp").read_text():
        fail("WkiChannel must carry a pool-slot generation for reuse guards")
    if "ch->generation++;" not in channel_init_body:
        fail("channel_init() must advance the pool-slot generation on every allocation")
    require_order(channel_alloc_body, "ch->lock.lock();", "channel_init(ch", "channel allocation init lock")
    require_order(channel_alloc_body, "channel_init(ch", "peer->channels.at(chan_id) = ch;", "channel allocation publish")
    require_order(channel_alloc_body, "peer->channels.at(chan_id) = ch;", "ch->lock.unlock();", "channel allocation unlock")
    if "ack.generation = ch->generation;" not in source:
        fail("ACK snapshots must capture the channel generation")
    if "ch->generation == ack.generation" not in source:
        fail("post-unlock ACK completion must reject stale channel generations")
    if "capture_retransmit_head_snapshot(ch, wki_now_us(), false, true, fast_retransmit_expected_seq, fast_retransmit)" not in source:
        fail("fast retransmit must capture a stable retransmit snapshot")
    if "ch->generation == fast_retransmit.generation" not in source:
        fail("fast retransmit completion must reject stale channel generations")

    for name, body in (("wki_timer_tick_single", timer_single_body), ("wki_timer_tick", timer_body)):
        missing = [
            token
            for token in (
                "RetransmitSnapshot retransmit = {};",
                "capture_retransmit_head_snapshot(ch, now_us, true, false, 0, retransmit)",
                "ch->generation == retransmit.generation",
                "ack_generation = ch->generation;",
                "complete_ack_transmit_for_generation_locked(ch, ack_generation",
            )
            if token not in body
        ]
        if missing:
            fail(f"{name} is missing channel reuse generation guards: " + ", ".join(missing))


def test_block_ring_binding_lifetime_is_retained_outside_server_lock() -> None:
    source = DEV_SERVER_CPP.read_text()
    header = (ROOT / "modules" / "kern" / "src" / "net" / "wki" / "dev_server.hpp").read_text()
    post_body = function_body(source, "blk_zone_post_handler")
    pending_body = function_body(source, "wki_dev_server_process_pending_zones")
    poll_body = function_body(source, "wki_dev_server_poll_rings")

    required = [
        "std::list<DevServerBinding> g_bindings",
        "retain_binding_locked",
        "release_binding",
        "wait_for_binding_refs_to_drain",
        "erase_retired_binding_locked",
    ]
    missing = [token for token in required if token not in source]
    if missing:
        fail("dev-server bindings need stable storage and retain/release helpers: " + ", ".join(missing))
    for token in [
        "std::atomic<uint32_t> refs",
        "std::atomic<bool> retiring",
        "bool epoch_reset_pending = false",
        "bool detach_cleanup_pending = false",
        "bool detach_cleanup_claimed = false",
        "WkiChannelIdentity channel_identity{}",
    ]:
        if token not in header:
            fail(f"DevServerBinding lifecycle state is missing {token}")

    require_order(post_body, "binding = find_binding_by_zone_id(zone_id);", "retain_binding_locked(binding)", "zone post retain")
    require_order(post_body, "retain_binding_locked(binding)", "blk_ring_server_poll(binding);", "zone post poll")
    require_order(post_body, "blk_ring_server_poll(binding);", "release_binding(binding);", "zone post release")

    require_order(pending_body, "retain_binding_locked(&b)", "wki_zone_create", "pending zone retain before blocking create")
    require_order(pending_body, "wki_zone_create", "binding_still_active", "pending zone revalidation after blocking create")
    if "if (!binding_still_active)" not in pending_body or "wki_zone_destroy(item.blk_zone_id);" not in pending_body:
        fail("deferred zone creation must destroy a zone created for a retired binding")

    require_order(poll_body, "retain_binding_locked(&b)", "rings.at(ring_count++) = &b;", "ring poll retain")
    require_order(poll_body, "blk_ring_server_poll(binding);", "release_binding(binding);", "ring poll release")


def test_deferred_vfs_ops_retain_their_binding_through_blocking_work() -> None:
    source = DEV_SERVER_CPP.read_text()
    header = DEV_SERVER_HPP.read_text()
    ktest = WKI_DEV_SERVER_KTEST.read_text()
    queue_body = function_body(source, "queue_vfs_op")
    run_body = function_body(source, "run_deferred_vfs_op")
    alloc_body = function_body(source, "deferred_vfs_op_alloc")
    release_body = function_body(source, "deferred_vfs_op_release")
    queue_required = [
        "DevServerBinding* retained_binding = nullptr",
        "find_binding_by_channel_identity(channel_identity)",
        "retain_binding_locked(binding)",
        "retained_binding = binding",
        "if (retained_binding == nullptr)",
        "op->retained_binding = retained_binding",
    ]
    missing = [token for token in queue_required if token not in queue_body]
    if missing:
        fail("deferred VFS enqueue must retain its exact binding: " + ", ".join(missing))

    run_required = [
        "DevServerBinding* const RETAINED_BINDING = op->retained_binding",
        "RETAINED_BINDING != nullptr",
        "channel_identity_matches(RETAINED_BINDING->channel_identity, op->channel_identity)",
        "static_cast<const char*>(RETAINED_BINDING->vfs_export_path)",
        "static_cast<const char*>(RETAINED_BINDING->vfs_export_name)",
        "detail::handle_vfs_op",
        "op->channel_identity",
        "release_binding(RETAINED_BINDING)",
    ]
    missing = [token for token in run_required if token not in run_body]
    if missing:
        fail("deferred VFS worker must consume the retained binding: " + ", ".join(missing))

    require_order(queue_body, "retain_binding_locked(binding)", "deferred_vfs_op_alloc(req_data_len)", "VFS retain before allocation/enqueue")
    require_order(queue_body, "retain_binding_locked(binding)", "op->retained_binding = retained_binding", "VFS retained pointer transfer")
    require_order(queue_body, "deferred_vfs_op_alloc(req_data_len)", "std::memcpy(op->req_data", "VFS allocation before request copy")
    require_order(queue_body, "std::memcpy(op->req_data", "shard->lock.lock_irqsave()", "VFS request copy before FIFO publication")
    require_order(
        run_body,
        "channel_identity_matches(RETAINED_BINDING->channel_identity, op->channel_identity)",
        "detail::handle_vfs_op",
        "retained VFS generation validation",
    )
    require_order(run_body, "detail::handle_vfs_op", "release_binding(RETAINED_BINDING)", "deferred VFS release after handler")
    require_order(run_body, "release_binding(RETAINED_BINDING)", "deferred_vfs_op_release(op)", "deferred VFS release before request cleanup")
    for forbidden in [
        "s_server_lock.lock_irqsave()",
        "std::array<char, sizeof(DevServerBinding::vfs_export_path)>",
        "std::array<char, sizeof(DevServerBinding::vfs_export_name)>",
        "std::memcpy(export_path.data()",
        "std::memcpy(export_name.data()",
    ]:
        if forbidden in run_body:
            fail(f"retained VFS binding identity must be consumed directly without a locked snapshot: found {forbidden}")

    for token in [
        "::operator new(sizeof(DeferredVfsOp) + req_data_len, std::nothrow)",
        "new (STORAGE) DeferredVfsOp{}",
        "op->req_data = reinterpret_cast<uint8_t*>(op + 1)",
    ]:
        if token not in alloc_body:
            fail(f"deferred VFS coallocation helper is missing {token}")
    require_order(release_body, "op->~DeferredVfsOp()", "::operator delete(op)", "deferred VFS placement destruction")
    for forbidden in ["new (std::nothrow) DeferredVfsOp", "new (std::nothrow) uint8_t[req_data_len]"]:
        if forbidden in queue_body:
            fail(f"deferred VFS enqueue must use one exact coallocation: found {forbidden}")
    for forbidden in ["delete[] op->req_data", "delete op"]:
        if forbidden in run_body:
            fail(f"deferred VFS worker must use the common coallocation release: found {forbidden}")

    selftest_token = "wki_dev_server_selftest_deferred_vfs_storage_is_coallocated"
    if f"auto {selftest_token}() -> bool" not in source or f"auto {selftest_token}() -> bool;" not in header:
        fail("deferred VFS coallocation selftest must be implemented and declared")
    for token in ["DeferredRequestStorageIsCoallocated", selftest_token]:
        if token not in ktest:
            fail(f"deferred VFS coallocation KTEST coverage is missing {token}")


def test_detach_waits_for_binding_refs_before_cleanup_and_erase() -> None:
    detach_all_body = function_body(DEV_SERVER_CPP.read_text(), "wki_dev_server_detach_all_for_peer")
    detach_body = function_body(DEV_SERVER_CPP.read_text(), "wki_dev_server_process_pending_detaches")

    require_order(detach_all_body, "mark_binding_retiring_locked(b);", "wait_for_binding_refs_to_drain(item.binding);", "fence retire wait")
    require_order(
        detach_all_body,
        "wait_for_binding_refs_to_drain(item.binding);",
        "release_vfs_rdma_buffers(&item.vfs_rdma_buffers);",
        "fence wait before VFS RDMA unregister/free",
    )
    require_order(detach_all_body, "wait_for_binding_refs_to_drain(item.binding);", "wki_zone_destroy(item.blk_zone_id);", "fence wait before zone destroy")
    require_order(detach_all_body, "wki_zone_destroy(item.blk_zone_id);", "erase_retired_binding_locked(work.at(i).binding);", "fence erase after cleanup")

    require_order(
        detach_body,
        "binding.detach_cleanup_claimed = true;",
        "wait_for_binding_refs_to_drain(item.binding);",
        "detach claim before wait",
    )
    require_order(
        detach_body,
        "wait_for_binding_refs_to_drain(item.binding);",
        "release_vfs_rdma_buffers(&item.vfs_rdma_buffers);",
        "detach wait before VFS RDMA unregister/free",
    )
    require_order(
        detach_body,
        "wait_for_binding_refs_to_drain(item.binding);",
        "wki_zone_destroy(item.blk_zone_id);",
        "detach wait before zone destroy",
    )
    require_order(
        detach_body,
        "wki_zone_destroy(item.blk_zone_id);",
        "erase_retired_binding_locked(item.binding);",
        "detach erase after cleanup",
    )


def test_epoch_cleanup_and_deferred_vfs_are_channel_generation_fenced() -> None:
    source = DEV_SERVER_CPP.read_text()
    header = DEV_SERVER_HPP.read_text()
    wki_source = WKI_CPP.read_text()
    marker = function_body(source, "wki_dev_server_mark_epoch_reset")
    cleanup = function_body(source, "wki_dev_server_cleanup_epoch_reset_for_peer")
    handler = function_body(source, "handle_dev_op_req")
    queue = function_body(source, "queue_vfs_op")
    worker = function_body(source, "run_deferred_vfs_op")
    worker_index = function_body(source, "vfs_worker_index")
    worker_dequeue = function_body(source, "vfs_worker_dequeue")
    detach_all = function_body(source, "wki_dev_server_detach_all_for_peer")

    for token in ["binding.detach_cleanup_pending", "binding.epoch_reset_pending = true", "mark_binding_retiring_locked(binding)"]:
        if token not in marker:
            fail(f"nonblocking server epoch marker is missing {token}")
    for forbidden in ["new ", "delete ", "kern_yield", "wki_channel_close"]:
        if forbidden in marker:
            fail(f"server epoch marker must remain nonblocking: found {forbidden}")

    for token in [
        "binding.epoch_reset_pending",
        ".channel_identity = binding.channel_identity",
        "wait_for_binding_refs_to_drain(item.binding)",
        "wki_channel_close_generation(item.channel_identity.channel",
        "erase_retired_binding_locked(work.at(i).binding)",
    ]:
        if token not in cleanup:
            fail(f"task-context server epoch cleanup is missing {token}")
    require_order(
        cleanup,
        "wait_for_binding_refs_to_drain(item.binding)",
        "release_vfs_rdma_buffers(&item.vfs_rdma_buffers)",
        "epoch cleanup must drain workers before releasing VFS staging",
    )
    require_order(
        cleanup,
        "wki_channel_close_generation(item.channel_identity.channel",
        "erase_retired_binding_locked(work.at(i).binding)",
        "epoch cleanup exact close before erase",
    )

    if source.count("binding.channel_identity = channel_identity") < 3:
        fail("BLOCK, VFS, and NET server bindings must capture their allocation identity")
    if source.count("reserve_attach_channel(hdr->src_node, req->requested_channel") < 3 or source.count("&channel_identity") < 3:
        fail("all attach branches must reserve channels with an immutable identity output")

    for token in [
        "WkiChannelIdentity const CHANNEL_IDENTITY",
        ".channel = rx_channel",
        ".generation = rx_channel_generation",
        "find_binding_by_channel_identity(CHANNEL_IDENTITY)",
        "queue_vfs_op(hdr, CHANNEL_IDENTITY",
    ]:
        if token not in handler:
            fail(f"DEV_OP_REQ generation admission is missing {token}")
    for token in ["op->channel_identity = channel_identity", "DeferredVfsOp"]:
        if token not in queue and token not in source:
            fail(f"deferred VFS queue token capture is missing {token}")
    for token in ["src_node", "channel_id", "VFS_OP_WORKER_COUNT"]:
        if token not in worker_index:
            fail(f"VFS worker shard identity is missing {token}")
    require_order(queue, "auto* shard = vfs_worker_for(hdr)", "shard->tail->next = op", "VFS per-channel FIFO enqueue")
    require_order(worker_dequeue, "DeferredVfsOp* op = shard.head", "shard.head = op->next", "VFS FIFO dequeue")
    if "op_id >= OP_VFS_OPEN && op_id <= OP_VFS_READ_BULK" not in source or "constexpr uint16_t OP_VFS_CLOSE" not in WIRE_HPP.read_text():
        fail("OP_VFS_CLOSE must share the per-channel deferred VFS FIFO with read/write operations")
    for token in [
        "DevServerBinding* const RETAINED_BINDING = op->retained_binding",
        "channel_identity_matches(RETAINED_BINDING->channel_identity, op->channel_identity)",
        "detail::handle_vfs_op(&op->hdr, op->channel_identity",
        "send_vfs_error_response(op->channel_identity",
    ]:
        if token not in worker:
            fail(f"deferred VFS worker generation validation is missing {token}")

    if "detail::handle_dev_op_req(hdr, payload, payload_len, rx_channel, rx_channel_generation)" not in wki_source:
        fail("reliable dispatch must pass the captured RX channel generation to DEV_OP_REQ")
    if "wki_channel_get(item.consumer_node, item.assigned_channel)" in detach_all:
        fail("peer detach cleanup must never allocate a replacement channel")
    detach_claim = function_body(source, "detach_all_may_claim_binding")
    if "binding.detach_cleanup_pending" not in detach_claim or "detach_all_may_claim_binding(b, node_id)" not in detach_all:
        fail("peer detach cleanup must not double-consume an explicitly admitted detach")
    if "wki_channel_close_generation(item.channel_identity.channel" not in detach_all:
        fail("peer detach cleanup must close only the captured channel generation")
    for token in [
        "void wki_dev_server_mark_epoch_reset(uint16_t node_id);",
        "void wki_dev_server_cleanup_epoch_reset_for_peer(uint16_t node_id);",
    ]:
        if token not in header:
            fail(f"dev-server epoch API declaration is missing {token}")


def test_attach_ack_failure_defers_exact_cleanup_outside_rx() -> None:
    source = DEV_SERVER_CPP.read_text()
    header = DEV_SERVER_HPP.read_text()
    ktest = WKI_DEV_SERVER_KTEST.read_text()
    attach_body = function_body(source, "handle_dev_attach_req")
    staging = function_body(source, "stage_attach_ack_failure_cleanup_locked")
    admission = function_body(source, "defer_attach_ack_failure_cleanup")
    worker = function_body(source, "wki_dev_server_process_pending_detaches")
    selftest = function_body(source, "wki_dev_server_selftest_attach_ack_failure_defers_cleanup")

    required = [
        "auto defer_attach_ack_failure_cleanup(uint16_t consumer_node, ResourceType resource_type, uint32_t resource_id,",
        "defer_attach_ack_failure_cleanup(hdr->src_node, ResourceType::BLOCK, req->resource_id, channel_identity)",
        "defer_attach_ack_failure_cleanup(hdr->src_node, ResourceType::VFS, req->resource_id, channel_identity)",
        "defer_attach_ack_failure_cleanup(hdr->src_node, ResourceType::NET, req->resource_id, channel_identity)",
        "auto wki_dev_server_selftest_attach_ack_failure_defers_cleanup() -> bool",
    ]
    missing = [token for token in required if token not in source]
    if missing:
        fail("attach ACK failure deferred-cleanup scaffolding is missing: " + ", ".join(missing))

    for token in [
        "binding.active",
        "binding.retiring.load(std::memory_order_acquire)",
        "binding.consumer_node != consumer_node",
        "binding.resource_type != resource_type",
        "binding.resource_id != resource_id",
        "channel_identity_matches(binding.channel_identity, channel_identity)",
        "mark_binding_retiring_locked(binding)",
        "binding.detach_cleanup_pending = true",
        "binding.detach_cleanup_claimed = false",
    ]:
        if token not in staging:
            fail(f"attach ACK failure locked staging is missing {token!r}")
    for token in [
        "s_server_lock.lock_irqsave()",
        "stage_attach_ack_failure_cleanup_locked(consumer_node, resource_type, resource_id, channel_identity)",
        "s_server_lock.unlock_irqrestore(SRV_FLAGS)",
        "wki_deferred_work_notify()",
    ]:
        if token not in admission:
            fail(f"attach ACK failure admission wrapper is missing {token!r}")
    for forbidden in [
        "wait_for_binding_refs_to_drain",
        "delete",
        "wki_zone_destroy",
        "on_remote_detach",
        "wki_channel_close",
        "erase_retired_binding_locked",
        "uninstall_net_rx_forward_if_unused",
    ]:
        if forbidden in staging or forbidden in admission:
            fail(f"attach ACK failure RX admission must not perform cleanup: found {forbidden!r}")

    for token in [
        "wait_for_binding_refs_to_drain(item.binding)",
        "item.resource_type == ResourceType::VFS",
        "item.block_dev->remotable->on_remote_detach(item.consumer_node)",
        "item.net_dev->remotable->on_remote_detach(item.consumer_node)",
        "wki_channel_close_generation(item.channel_identity.channel",
        "erase_retired_binding_locked(item.binding)",
        "uninstall_net_rx_forward_if_unused(item.net_dev)",
    ]:
        if token not in worker:
            fail(f"deferred detach worker must own ACK-failure cleanup: missing {token!r}")

    if "rollback_attach_ack_failure" in source or "rollback_net_binding" in source:
        fail("attach ACK failure must not retain inline cleanup helpers")
    for token in [
        "s_server_lock.lock_irqsave()",
        "stage_attach_ack_failure_cleanup_locked(BLOCK_NODE",
        "stage_attach_ack_failure_cleanup_locked(VFS_NODE",
        "stage_attach_ack_failure_cleanup_locked(NET_NODE",
        "g_bindings.erase(it)",
        "s_server_lock.unlock_irqrestore(flags)",
    ]:
        if token not in selftest:
            fail(f"attach ACK failure selftest must stage and inspect under one server-lock span: missing {token!r}")
    require_order(selftest, "s_server_lock.lock_irqsave()", "stage_attach_ack_failure_cleanup_locked(BLOCK_NODE", "selftest lock before staging")
    require_order(selftest, "stage_attach_ack_failure_cleanup_locked(NET_NODE", "g_bindings.erase(it)", "selftest staging before erase")
    require_order(selftest, "g_bindings.erase(it)", "s_server_lock.unlock_irqrestore(flags)", "selftest erase before unlock")
    for forbidden in ["defer_attach_ack_failure_cleanup(", "wki_deferred_work_notify()"]:
        if forbidden in selftest:
            fail(f"attach ACK failure selftest must not wake the production worker: found {forbidden!r}")
    if "auto wki_dev_server_selftest_attach_ack_failure_defers_cleanup() -> bool;" not in header:
        fail("dev-server deferred attach ACK failure selftest must be declared")
    for token in [
        "KTEST(WkiDevServerAttachAckFailure, DefersExactBlockVfsAndNetCleanupOutsideRx)",
        "wki_dev_server_selftest_attach_ack_failure_defers_cleanup()",
        "WRONG_GENERATION_REJECTED",
        "NET_DEFERRED",
    ]:
        if token not in ktest and token not in source:
            fail(f"dev-server deferred attach ACK cleanup coverage is missing {token!r}")


def test_detach_admission_has_ktest_coverage() -> None:
    source = DEV_SERVER_CPP.read_text()
    header = DEV_SERVER_HPP.read_text()
    ktest = WKI_DEV_SERVER_KTEST.read_text()
    for token in [
        "auto wki_dev_server_selftest_detach_admission_lifecycle() -> bool",
        "WRONG_COOKIE_REJECTED",
        "FIRST_ADMITTED",
        "DUPLICATE_IDEMPOTENT",
        "REPLACEMENT_BLOCKED",
        "UNRELATED_ATTACH_ALLOWED",
    ]:
        if token not in source:
            fail(f"detach admission selftest is missing {token!r}")
    if "auto wki_dev_server_selftest_detach_admission_lifecycle() -> bool;" not in header:
        fail("detach admission selftest must be declared")
    for token in [
        "KTEST(WkiDevServerDetach, AdmissionIsExactIdempotentAndBlocksReplacement)",
        "wki_dev_server_selftest_detach_admission_lifecycle()",
    ]:
        if token not in ktest:
            fail(f"detach admission KTEST coverage is missing {token!r}")


def test_vfs_two_lane_rdma_keeps_anchor_identity_independent_and_tears_down_regions() -> None:
    source = DEV_SERVER_CPP.read_text()
    header = DEV_SERVER_HPP.read_text()
    wire = WIRE_HPP.read_text()
    attach = function_body(source, "handle_dev_attach_req")
    release = function_body(source, "release_vfs_rdma_buffers")

    for token in [
        "constexpr uint16_t WKI_CAP_VFS_MULTI_RDMA_LANES = 0x0008;",
        "constexpr uint8_t DEV_ATTACH_DISABLE_RDMA = 0x40;",
        "constexpr uint8_t DEV_ATTACH_VFS_AUX_LANE = 0x80;",
        "auto wki_vfs_proxy_attach_mode(",
        "auto wki_vfs_attach_lane_is_anchor(",
        "static_assert(sizeof(DevAttachReqPayload) == 12",
        "static_assert(sizeof(HelloPayload) == 96",
    ]:
        if token not in wire:
            fail(f"VFS two-lane RDMA must preserve the attach ABI: missing {token!r}")
    for token in [
        "WkiTransport* vfs_rdma_transport = nullptr;",
        "vfs_rdma_transport(o.vfs_rdma_transport)",
        "vfs_rdma_transport = o.vfs_rdma_transport;",
    ]:
        if token not in header:
            fail(f"VFS binding must retain the provider that owns every registered region: missing {token!r}")
    require_order(
        attach,
        "wki_peer_capability_negotiated(hdr->src_node, WKI_CAP_VFS_MULTI_RDMA_LANES)",
        "binding.vfs_lane_anchor = wki_vfs_attach_lane_is_anchor(req->attach_mode, MULTI_RDMA_LANES)",
        "explicit auxiliary identity is interpreted only after capability negotiation",
    )
    require_order(
        attach,
        "binding.vfs_lane_anchor = wki_vfs_attach_lane_is_anchor(req->attach_mode, MULTI_RDMA_LANES)",
        "if ((req->attach_mode & DEV_ATTACH_DISABLE_RDMA) == 0)",
        "notification-anchor identity is independent from RDMA buffer allocation",
    )
    require_order(
        attach,
        "if ((req->attach_mode & DEV_ATTACH_DISABLE_RDMA) == 0)",
        "binding.vfs_rdma_transport = peer->rdma_transport;",
        "owner RDMA setup remains inside the opt-in attach branch",
    )
    vfs_rdm_attach = attach[attach.find("if ((req->attach_mode & DEV_ATTACH_DISABLE_RDMA) == 0)") :]
    require_order(
        vfs_rdm_attach,
        "binding.vfs_rdma_transport = peer->rdma_transport;",
        "->rdma_register_region(",
        "owner selects the provider before registering VFS buffers",
    )

    for token in [
        "buffers->transport->rdma_unregister_region",
        "buffers->write_rkey, VFS_RDMA_WRITE_SIZE",
        "buffers->read_staging_rkey,",
        "buffers->bulk_staging_rkey,",
        "delete[] buffers->write_buf",
        "delete[] buffers->read_staging_buf",
        "delete[] buffers->bulk_staging_buf",
    ]:
        if token not in release:
            fail(f"VFS RDMA teardown is missing {token!r}")
    require_order(
        release,
        "buffers->transport->rdma_unregister_region",
        "delete[] buffers->write_buf",
        "VFS region metadata must be removed before backing memory is freed",
    )

    for name in [
        "wki_dev_server_finish_vfs_export_reconciliation",
        "wki_dev_server_process_pending_detaches",
        "wki_dev_server_cleanup_epoch_reset_for_peer",
        "wki_dev_server_detach_all_for_peer",
    ]:
        body = function_body(source, name)
        require_order(
            body,
            "wait_for_binding_refs_to_drain",
            "release_vfs_rdma_buffers",
            f"{name} releases VFS regions only after binding refs drain",
        )
    require_order(
        attach,
        "take_vfs_rdma_buffers_locked(*provisional_binding, &vfs_rdma_buffers);",
        "release_vfs_rdma_buffers(&vfs_rdma_buffers);",
        "failed provisional VFS attaches release registered regions",
    )

    wki = WKI_HPP.read_text()
    for token in [
        "int (*rdma_unregister_region)(WkiTransport* self, uint32_t rkey, uint32_t size);",
        "rdma_unregister_region = nullptr;",
    ]:
        if token not in wki and token not in TRANSPORT_ETH_CPP.read_text():
            fail(f"transport region teardown API is missing {token!r}")
    if "roce_rdma_unregister_region" not in TRANSPORT_ROCE_CPP.read_text():
        fail("RoCE must remove persistent VFS regions from its rkey registry")
    ivshmem = TRANSPORT_IVSHMEM_CPP.read_text()
    if "rdma_unregister_region = nullptr;" not in ivshmem or "no remote revocation" not in ivshmem:
        fail("ivshmem must keep raw RDMA offset keys pinned without a remote-revocation protocol")


def main() -> None:
    test_rx_forward_does_not_spend_credits_before_delivery_is_possible()
    test_rx_forward_sends_notify_cookie_envelope()
    test_state_notify_sends_notify_cookie_envelope()
    test_net_dispatch_does_not_pass_server_binding_pointer()
    test_vfs_attach_uses_export_snapshot_not_unlocked_pointer()
    test_vfs_notify_coalesces_by_mount_anchor_with_conservative_fallback()
    test_duplicate_net_attach_does_not_rewrite_binding_cookie()
    test_net_binding_state_mutation_revalidates_under_server_lock()
    test_net_notify_handlers_validate_cookie_envelope()
    test_net_rx_credit_accounting_uses_proxy_lock()
    test_net_error_responses_echo_payload_cookie()
    test_proxy_net_xmit_rejects_oversize_before_uint16_truncation()
    test_detach_cookie_and_incarnation_are_exact_matches()
    test_detach_admission_is_napi_safe_and_cleanup_is_deferred()
    test_replacement_attach_waits_for_pending_detach_cleanup()
    test_retirement_has_one_cleanup_owner_and_preserves_block_writer_exclusion()
    test_block_attach_transfers_persistent_writer_lease()
    test_net_forward_hook_removal_revalidates_after_cleanup()
    test_channel_close_serializes_pool_reuse_until_reset_complete()
    test_peer_channel_close_clears_index_before_unlocking_pool()
    test_reliable_rx_does_not_autocreate_allocated_dynamic_channels()
    test_channel_reuse_generation_guards_unlock_tx_relock_paths()
    test_block_ring_binding_lifetime_is_retained_outside_server_lock()
    test_deferred_vfs_ops_retain_their_binding_through_blocking_work()
    test_detach_waits_for_binding_refs_before_cleanup_and_erase()
    test_epoch_cleanup_and_deferred_vfs_are_channel_generation_fenced()
    test_attach_ack_failure_defers_exact_cleanup_outside_rx()
    test_detach_admission_has_ktest_coverage()
    test_vfs_two_lane_rdma_keeps_anchor_identity_independent_and_tears_down_regions()
    print("WKI dev server source invariants hold")


if __name__ == "__main__":
    main()
