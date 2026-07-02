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
    expected_call = "detail::handle_net_op(hdr, hdr->channel_id, binding_net_dev, req->op_id, req_data, REQ_DATA_LEN);"
    if expected_call not in server_body:
        fail("handle_dev_op_req() must dispatch NET ops with the snapshotted net_dev only")


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
        "WkiChannel const* ch = reserve_attach_channel",
        "VFS attach validates export before reserving channel",
    )
    require_order(
        attach_body,
        "wki_remote_vfs_find_export_snapshot(req->resource_id, &exp)",
        "memcpy(static_cast<void*>(binding.vfs_export_path), static_cast<const void*>(exp.export_path),",
        "VFS attach copies from snapshot",
    )


def test_duplicate_net_attach_does_not_rewrite_binding_cookie() -> None:
    helper = function_body(DEV_SERVER_CPP.read_text(), "find_existing_net_binding")
    server_body = function_body(DEV_SERVER_CPP.read_text(), "handle_dev_attach_req")
    if "binding->attach_cookie =" in helper:
        fail("duplicate NET attach lookup must not rewrite the existing binding cookie")
    required = [
        "info.attach_cookie = binding->attach_cookie",
        "existing_ack.attach_cookie = existing.attach_cookie",
    ]
    missing = [token for token in required if token not in helper + server_body]
    if missing:
        fail("duplicate NET attach must echo the existing binding cookie: " + ", ".join(missing))


def test_net_binding_state_mutation_revalidates_under_server_lock() -> None:
    body = function_body(REMOTE_NET_CPP.read_text(), "handle_net_op")
    required = [
        "wki_dev_server_mark_net_opened(hdr->src_node, channel_id, net_dev, true)",
        "wki_dev_server_mark_net_opened(hdr->src_node, channel_id, net_dev, false)",
        "wki_dev_server_add_net_rx_credits(hdr->src_node, channel_id, net_dev, credits)",
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
        "wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, req_buf, REQ_TOTAL)",
    ]
    missing = [token for token in required_xmit_tokens if token not in xmit_body]
    if missing:
        fail("proxy_net_xmit must validate total request length before allocation/copy: " + ", ".join(missing))
    if "static_cast<uint16_t>(sizeof(DevOpReqPayload) + pkt->len)" in xmit_body:
        fail("proxy_net_xmit must not cast the packet request length before checking bounds")
    require_order(xmit_body, "if (!REQ_SIZE.ok)", "new (std::nothrow) uint8_t[REQ_TOTAL]", "remote NET xmit size gate")


def test_detach_cookie_is_exact_match() -> None:
    body = function_body(WIRE_HPP.read_text(), "wki_dev_detach_cookie_matches_binding")
    if "return binding_attach_cookie == request_cookie;" not in body:
        fail("detach cookie matching must not treat zero as a wildcard for nonzero-cookie bindings")


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
    if "fast_retransmit_generation = ch->generation;" not in source:
        fail("fast retransmit snapshots must capture the channel generation")
    if "ch->generation == fast_retransmit_generation" not in source:
        fail("fast retransmit completion must reject stale channel generations")

    for name, body in (("wki_timer_tick_single", timer_single_body), ("wki_timer_tick", timer_body)):
        missing = [
            token
            for token in (
                "retransmit_generation = ch->generation;",
                "ack_generation = ch->generation;",
                "ch->generation == retransmit_generation",
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
    if "std::atomic<uint32_t> refs" not in header or "std::atomic<bool> retiring" not in header:
        fail("DevServerBinding must carry atomic refs and retiring lifecycle state")

    require_order(post_body, "binding = find_binding_by_zone_id(zone_id);", "retain_binding_locked(binding)", "zone post retain")
    require_order(post_body, "retain_binding_locked(binding)", "blk_ring_server_poll(binding);", "zone post poll")
    require_order(post_body, "blk_ring_server_poll(binding);", "release_binding(binding);", "zone post release")

    require_order(pending_body, "retain_binding_locked(&b)", "wki_zone_create", "pending zone retain before blocking create")
    require_order(pending_body, "wki_zone_create", "binding_still_active", "pending zone revalidation after blocking create")
    if "if (!binding_still_active)" not in pending_body or "wki_zone_destroy(item.blk_zone_id);" not in pending_body:
        fail("deferred zone creation must destroy a zone created for a retired binding")

    require_order(poll_body, "retain_binding_locked(&b)", "rings.at(ring_count++) = &b;", "ring poll retain")
    require_order(poll_body, "blk_ring_server_poll(binding);", "release_binding(binding);", "ring poll release")


def test_detach_waits_for_binding_refs_before_cleanup_and_erase() -> None:
    detach_all_body = function_body(DEV_SERVER_CPP.read_text(), "wki_dev_server_detach_all_for_peer")
    detach_body = function_body(DEV_SERVER_CPP.read_text(), "handle_dev_detach")

    require_order(detach_all_body, "mark_binding_retiring_locked(b);", "wait_for_binding_refs_to_drain(item.binding);", "fence retire wait")
    require_order(detach_all_body, "wait_for_binding_refs_to_drain(item.binding);", "delete[] item.vfs_rdma_write_buf;", "fence wait before free")
    require_order(detach_all_body, "wait_for_binding_refs_to_drain(item.binding);", "wki_zone_destroy(item.blk_zone_id);", "fence wait before zone destroy")
    require_order(detach_all_body, "wki_zone_destroy(item.blk_zone_id);", "erase_retired_binding_locked(work.at(i).binding);", "fence erase after cleanup")

    require_order(detach_body, "mark_binding_retiring_locked(binding);", "wait_for_binding_refs_to_drain(info.binding);", "detach retire wait")
    require_order(detach_body, "wait_for_binding_refs_to_drain(info.binding);", "delete[] info.vfs_rdma_write_buf;", "detach wait before free")
    require_order(detach_body, "wait_for_binding_refs_to_drain(info.binding);", "wki_zone_destroy(info.blk_zone_id);", "detach wait before zone destroy")
    require_order(detach_body, "wki_zone_destroy(info.blk_zone_id);", "erase_retired_binding_locked(info.binding);", "detach erase after cleanup")


def test_attach_ack_failure_rolls_back_block_and_vfs_bindings() -> None:
    source = DEV_SERVER_CPP.read_text()
    header = DEV_SERVER_HPP.read_text()
    ktest = WKI_DEV_SERVER_KTEST.read_text()
    attach_body = function_body(source, "handle_dev_attach_req")
    rollback_body = function_body(source, "rollback_attach_ack_failure")

    required = [
        "void rollback_attach_ack_failure(uint16_t consumer_node, ResourceType resource_type, uint32_t resource_id, uint16_t assigned_channel)",
        "rollback_attach_ack_failure(hdr->src_node, ResourceType::BLOCK, req->resource_id, ch->channel_id);",
        "rollback_attach_ack_failure(hdr->src_node, ResourceType::VFS, req->resource_id, ch->channel_id);",
        "auto wki_dev_server_selftest_attach_ack_failure_rolls_back_binding() -> bool",
    ]
    missing = [token for token in required if token not in source]
    if missing:
        fail("attach ACK failure rollback scaffolding is missing: " + ", ".join(missing))

    for token in [
        "binding.consumer_node != consumer_node",
        "binding.resource_type != resource_type",
        "binding.resource_id != resource_id",
        "binding.assigned_channel != assigned_channel",
        "mark_binding_retiring_locked(binding)",
        "wait_for_binding_refs_to_drain(info.binding)",
        "delete[] info.vfs_rdma_write_buf",
        "delete[] info.vfs_rdma_read_staging_buf",
        "delete[] info.vfs_rdma_bulk_staging_buf",
        "wki_zone_destroy(info.blk_zone_id)",
        "info.block_dev->remotable->on_remote_detach(info.consumer_node)",
        "wki_channel_lookup(info.consumer_node, info.assigned_channel)",
        "wki_channel_close(ch)",
        "erase_retired_binding_locked(info.binding)",
    ]:
        if token not in rollback_body:
            fail(f"attach ACK failure rollback helper is missing {token!r}")
    if "wki_channel_get(info.consumer_node, info.assigned_channel)" in rollback_body:
        fail("attach ACK rollback must not allocate a channel while cleaning up a failed attach")

    require_order(
        attach_body,
        "int const ACK_RET = wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));",
        "rollback_attach_ack_failure(hdr->src_node, ResourceType::BLOCK, req->resource_id, ch->channel_id);",
        "BLOCK attach ACK rollback",
    )
    require_order(
        attach_body,
        "int const ACK_RET = wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));",
        "rollback_attach_ack_failure(hdr->src_node, ResourceType::VFS, req->resource_id, ch->channel_id);",
        "VFS attach ACK rollback",
    )
    require_order(
        rollback_body,
        "mark_binding_retiring_locked(binding)",
        "wait_for_binding_refs_to_drain(info.binding)",
        "rollback retires before waiting",
    )
    require_order(
        rollback_body,
        "wait_for_binding_refs_to_drain(info.binding)",
        "delete[] info.vfs_rdma_write_buf",
        "rollback waits before freeing VFS buffers",
    )
    require_order(
        rollback_body,
        "wait_for_binding_refs_to_drain(info.binding)",
        "wki_zone_destroy(info.blk_zone_id)",
        "rollback waits before destroying block zone",
    )
    require_order(
        rollback_body,
        "wki_channel_close(ch)",
        "erase_retired_binding_locked(info.binding)",
        "rollback erases after channel close",
    )

    if "auto wki_dev_server_selftest_attach_ack_failure_rolls_back_binding() -> bool;" not in header:
        fail("dev-server attach ACK rollback selftest must be declared")
    for token in [
        "KTEST(WkiDevServerAttachAckFailure, RollsBackBlockAndVfsBindings)",
        "wki_dev_server_selftest_attach_ack_failure_rolls_back_binding()",
    ]:
        if token not in ktest:
            fail(f"dev-server attach ACK rollback KTEST coverage is missing {token!r}")


def main() -> None:
    test_rx_forward_does_not_spend_credits_before_delivery_is_possible()
    test_rx_forward_sends_notify_cookie_envelope()
    test_state_notify_sends_notify_cookie_envelope()
    test_net_dispatch_does_not_pass_server_binding_pointer()
    test_vfs_attach_uses_export_snapshot_not_unlocked_pointer()
    test_duplicate_net_attach_does_not_rewrite_binding_cookie()
    test_net_binding_state_mutation_revalidates_under_server_lock()
    test_net_notify_handlers_validate_cookie_envelope()
    test_net_rx_credit_accounting_uses_proxy_lock()
    test_net_error_responses_echo_payload_cookie()
    test_proxy_net_xmit_rejects_oversize_before_uint16_truncation()
    test_detach_cookie_is_exact_match()
    test_channel_close_serializes_pool_reuse_until_reset_complete()
    test_peer_channel_close_clears_index_before_unlocking_pool()
    test_reliable_rx_does_not_autocreate_allocated_dynamic_channels()
    test_channel_reuse_generation_guards_unlock_tx_relock_paths()
    test_block_ring_binding_lifetime_is_retained_outside_server_lock()
    test_detach_waits_for_binding_refs_before_cleanup_and_erase()
    test_attach_ack_failure_rolls_back_block_and_vfs_bindings()
    print("WKI dev server source invariants hold")


if __name__ == "__main__":
    main()
