#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
REMOTE_NET_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_net.cpp"
REMOTE_NET_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "remote_net.hpp"
REMOTE_NET_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "wki_remote_net_ktest.cpp"


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


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def braced_block_after(source: str, token: str) -> str:
    start = source.find(token)
    if start < 0:
        fail(f"missing block token {token!r}")

    brace = source.find("{", start)
    if brace < 0:
        fail(f"missing block after {token!r}")

    depth = 1
    pos = brace + 1
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated block after {token!r}")
    return source[brace + 1 : pos - 1]


def test_net_proxy_op_slot_wait_is_bounded() -> None:
    source = REMOTE_NET_CPP.read_text()
    require_tokens(
        source,
        [
            "#include <net/wki/timer_math.hpp>",
            "constexpr uint64_t NET_PROXY_SLOT_WAIT_TIMEOUT_US = WKI_OP_TIMEOUT_US;",
            "auto acquire_net_op_slot_locked(ProxyNetState* state, uint64_t start_us) -> int",
        ],
        "remote net slot timeout scaffolding",
    )

    acquire_body = function_body(source, "acquire_net_op_slot_locked")
    require_tokens(
        acquire_body,
        [
            "uint64_t const DEADLINE_US = wki_future_deadline_us(start_us, NET_PROXY_SLOT_WAIT_TIMEOUT_US)",
            "state->lock.lock()",
            "state->retiring.load(std::memory_order_acquire)",
            "return WKI_ERR_PEER_FENCED",
            "net_proxy_op_slot_busy(state) && net_stats_poll_owns_slot(state)",
            "clear_net_op_state_locked(state, WKI_ERR_TIMEOUT)",
            "if (!net_proxy_op_slot_busy(state))",
            "return WKI_OK",
            "state->lock.unlock()",
            "continue",
            "if (wki_now_us() >= DEADLINE_US)",
            "return WKI_ERR_TIMEOUT",
            "ker::mod::sched::kern_sleep_us(NET_PROXY_CONTENTION_SLEEP_US)",
        ],
        "remote net slot acquisition",
    )
    require_order(acquire_body, "state->lock.lock()", "return WKI_OK", "slot lock success path")
    require_order(acquire_body, "state->lock.unlock()", "return WKI_ERR_PEER_FENCED", "fence path unlock")
    require_order(
        acquire_body,
        "state->lock.unlock();\n        if (!net_proxy_op_slot_busy(state))",
        "if (wki_now_us() >= DEADLINE_US)",
        "slot timeout path unlock",
    )

    prepare_body = function_body(source, "prepare_net_op_wait")
    require_order(
        prepare_body,
        "int const SLOT_RET = acquire_net_op_slot_locked(state, wki_now_us())",
        "uint16_t const COOKIE = allocate_net_op_cookie_locked(state)",
        "prepare wait allocates cookie only after slot acquisition",
    )
    require_order(prepare_body, "if (SLOT_RET != WKI_OK)", "return SLOT_RET", "prepare wait propagates slot timeout")
    require_order(
        prepare_body,
        "uint16_t const COOKIE = allocate_net_op_cookie_locked(state)",
        "state->op_wait_entry = &wait",
        "prepare wait publishes only after cookie allocation",
    )
    if "ker::mod::sched::kern_sleep_us(NET_PROXY_CONTENTION_SLEEP_US)" in prepare_body:
        fail("prepare_net_op_wait must not contain its own unbounded contention sleep loop")


def test_cancel_op_waiter_preserves_successor_slot() -> None:
    source = REMOTE_NET_CPP.read_text()
    body = function_body(source, "cancel_op_waiter")
    owner_block = braced_block_after(body, "if (state->op_wait_entry == &wait)")

    require_tokens(
        owner_block,
        [
            "state->op_wait_entry = nullptr",
            "clear_net_op_state_locked(state, result)",
        ],
        "remote net cancel owned slot cleanup",
    )
    if body.count("clear_net_op_state_locked(state, result)") != 1:
        fail("cancel_op_waiter must clear op state only when the canceled waiter still owns the slot")
    require_order(
        body,
        "if (state->op_wait_entry == &wait)",
        "claimed = wki_claim_op(&wait)",
        "cancel still completes the canceled waiter after ownership check",
    )


def test_cancel_op_waiter_has_ktest_coverage() -> None:
    source = REMOTE_NET_CPP.read_text()
    header = REMOTE_NET_HPP.read_text()
    ktest = REMOTE_NET_KTEST.read_text()

    require_tokens(
        source,
        ["auto wki_remote_net_selftest_cancel_preserves_successor_op() -> bool"],
        "remote net cancel selftest implementation",
    )
    require_tokens(
        header,
        ["auto wki_remote_net_selftest_cancel_preserves_successor_op() -> bool;"],
        "remote net cancel selftest declaration",
    )
    require_tokens(
        ktest,
        [
            "KTEST(WkiRemoteNetOpCancel, PreservesSuccessorSlot)",
            "wki_remote_net_selftest_cancel_preserves_successor_op()",
        ],
        "remote net cancel KTEST coverage",
    )


def test_dynamic_channel_uses_exact_allocation_identity() -> None:
    source = REMOTE_NET_CPP.read_text()
    header = REMOTE_NET_HPP.read_text()

    require_tokens(
        header,
        [
            "WkiChannelIdentity channel_identity{};",
            "uint64_t resource_generation = 0;",
            "void wki_remote_net_mark_epoch_reset(uint16_t node_id);",
            "void wki_remote_net_cleanup_epoch_reset_for_peer(uint16_t node_id, bool owner_reboot_proven);",
            "void wki_remote_net_detach_resource_generation(uint16_t owner_node, uint32_t resource_id, uint64_t resource_generation);",
        ],
        "remote net exact channel state",
    )
    require_tokens(
        source,
        [
            "wki_channel_alloc(owner_node, PriorityClass::THROUGHPUT, &reserved_channel_identity)",
            "wki_channel_reserve(owner_node, ATTACH_CHANNEL, PriorityClass::THROUGHPUT, &reserved_channel_identity)",
            "wki_send_on_channel_identity(state->channel_identity, MsgType::DEV_OP_REQ",
            "wki_send_on_channel_identity(target.channel_identity, MsgType::DEV_OP_REQ",
            "close_net_channel_identity(channel_identity)",
            "close_net_channel_identity(entry.channel_identity)",
        ],
        "remote net exact dynamic channel operations",
    )

    forbidden = [
        "wki_send(state->owner_node, state->assigned_channel",
        "wki_channel_lookup(owner_node, assigned_channel)",
        "wki_channel_lookup(entry.owner_node, entry.channel)",
        "wki_channel_close(ch)",
    ]
    present = [token for token in forbidden if token in source]
    if present:
        fail(f"remote net retains ID-only dynamic channel operation: {', '.join(present)}")


def test_net_rx_dispatch_matches_exact_generation() -> None:
    source = REMOTE_NET_CPP.read_text()
    header = REMOTE_NET_HPP.read_text()

    for handler in ["handle_net_op_resp", "handle_net_rx_notify", "handle_net_state_notify"]:
        declaration = re.search(rf"void {handler}\([^;]+;", header)
        if declaration is None or "const WkiChannelIdentity& channel_identity" not in declaration.group(0):
            fail(f"{handler} must accept exact RX channel identity")
        body = function_body(source, handler)
        require_tokens(
            body,
            [
                "net_channel_identity_matches_header(hdr, channel_identity)",
                "acquire_net_proxy_by_channel(channel_identity)",
            ],
            f"{handler} exact RX lookup",
        )

    server_body = function_body(source, "handle_net_op")
    require_tokens(
        server_body,
        [
            "net_channel_identity_matches_header(hdr, channel_identity)",
            "wki_send_on_channel_identity(channel_identity, MsgType::DEV_OP_RESP",
            "wki_dev_server_mark_net_opened(channel_identity, net_dev, true)",
            "wki_dev_server_add_net_rx_credits(channel_identity, net_dev, credits)",
            "legacy DEV_OP_REQ wire format has no binding",
        ],
        "remote net server exact dispatch",
    )


def test_epoch_reset_marker_defers_blocking_cleanup() -> None:
    source = REMOTE_NET_CPP.read_text()

    marker = function_body(source, "wki_remote_net_mark_epoch_reset")
    require_tokens(
        marker,
        [
            "s_net_proxy_lock.lock()",
            "state->active = false",
            "state->epoch_reset_pending = true",
            "state->retiring.store(true, std::memory_order_release)",
            "state->netdev.state = 0",
            "s_net_proxy_lock.unlock()",
        ],
        "remote net epoch RX marker",
    )
    for forbidden in ["new (", "push_back", "wait_for_", "wki_channel_close", "netdev_unregister", "wki_send("]:
        if forbidden in marker:
            fail(f"remote net epoch RX marker contains blocking/allocating action {forbidden!r}")

    cleanup = function_body(source, "cleanup_net_proxies_for_peer")
    require_tokens(
        cleanup,
        [
            "p->epoch_reset_pending",
            "p->netdev.state = 0",
            "claim_and_clear_waiter_locked",
            "wait_for_net_proxy_refs_to_drain(entry.state)",
            "unpublish_proxy_netdev(entry.state)",
            "close_net_channel_identity(entry.channel_identity)",
        ],
        "remote net deferred epoch cleanup",
    )


def test_attach_ack_lookup_includes_cookie() -> None:
    source = REMOTE_NET_CPP.read_text()

    allocator = function_body(source, "allocate_net_attach_cookie_locked")
    require_tokens(
        allocator,
        [
            "attempt < UINT8_MAX",
            "proxy->owner_node == owner_node",
            "proxy->resource_id == resource_id",
            "proxy->attach_cookie == cookie",
            "return 0",
        ],
        "remote net attach-cookie wrap exclusion",
    )
    attach = function_body(source, "wki_remote_net_attach")
    require_order(
        attach,
        "attach_cookie = allocate_net_attach_cookie_locked(owner_node, resource_id)",
        "g_net_proxies.push_back(state)",
        "remote net cookie reservation publishes atomically",
    )
    require_tokens(attach, ["if (attach_cookie == 0)", "return nullptr"], "remote net cookie exhaustion")

    lookup = function_body(source, "find_net_proxy_by_attach")
    require_tokens(
        lookup,
        [
            "p->owner_node == owner_node",
            "p->resource_id == resource_id",
            "p->attach_cookie == attach_cookie",
        ],
        "remote net attach ACK lookup",
    )
    ack = function_body(source, "handle_net_attach_ack")
    require_tokens(
        ack,
        [
            "acquire_net_proxy_by_attach(hdr->src_node, ack->resource_id, ack->reserved)",
            "wki_dev_attach_ack_matches_expected(state->attach_expected_cookie, *ack)",
        ],
        "remote net attach ACK cookie routing",
    )
    mismatch = braced_block_after(ack, "if (!state->attach_pending.load(std::memory_order_acquire)")
    require_order(mismatch, "state->lock.unlock()", "release_net_proxy(state)", "stale attach ACK unlock")
    require_order(mismatch, "release_net_proxy(state)", "return", "stale attach ACK exits immediately")


def test_published_proxy_storage_and_network_publications_are_retired_safely() -> None:
    source = REMOTE_NET_CPP.read_text()
    header = REMOTE_NET_HPP.read_text()

    require_tokens(header, ["bool ever_published = false;"], "permanent remote NET publication marker")

    attach = function_body(source, "wki_remote_net_attach")
    require_order(
        attach,
        "state->ever_published = true",
        "ker::net::netdev_register(&state->netdev)",
        "proxy storage pinned before netdev registry publication",
    )
    require_tokens(
        attach,
        ["if (!publish_proxy)", "unpublish_proxy_netdev(state)"],
        "post-registration epoch-race rollback",
    )

    erase = function_body(source, "erase_net_proxy")
    require_order(
        erase,
        "std::erase(g_net_proxies, state)",
        "state->cleanup_complete = true",
        "retired proxies leave the bounded live lookup index",
    )
    storage_erase = function_body(source, "erase_unpublished_net_proxy_storage_locked")
    require_tokens(
        storage_erase,
        ["state->ever_published", "state->detach_pending", "!state->cleanup_complete", "g_net_proxy_storage.erase(it)"],
        "only completed never-published teardown without a pending detach releases raw NetDevice storage",
    )
    require_tokens(
        source,
        [
            "std::deque<std::unique_ptr<ProxyNetState>> g_net_proxy_storage",
            "std::deque<ProxyNetState*> g_net_proxies",
            "g_net_proxy_storage.push_back(std::make_unique<ProxyNetState>())",
            "g_net_proxies.push_back(state)",
        ],
        "separate permanent storage and live proxy index",
    )

    unpublish = function_body(source, "unpublish_proxy_netdev")
    require_order(
        unpublish,
        "ker::net::route_del_for_dev(&state->netdev)",
        "ker::net::netdev_unregister(&state->netdev)",
        "route removal precedes registry unregister",
    )
    require_order(
        unpublish,
        "ker::net::netdev_unregister(&state->netdev)",
        "ker::net::netif_del_for_dev(&state->netdev)",
        "registry unregister prevents stale-route interface recreation before interface detach",
    )
    if source.count("ker::net::netdev_unregister(&state->netdev)") != 1:
        fail("every remote NET unregister must use the exact route/netif cleanup helper")

    detach = function_body(source, "wki_remote_net_detach")
    require_order(
        detach,
        "stage_net_detach_locked(state, owner_node, resource_id, attach_cookie, false)",
        "state->lock.unlock();\n    s_net_proxy_lock.unlock()",
        "ordinary detach reserves before releasing the retired registry transition",
    )
    require_order(
        detach,
        "wait_for_net_proxy_refs_to_drain(state)",
        "unpublish_proxy_netdev(state)",
        "ordinary detach drains proxy users before unpublication",
    )
    cleanup = function_body(source, "cleanup_net_proxies_for_peer")
    require_order(
        cleanup,
        "wait_for_net_proxy_refs_to_drain(entry.state)",
        "unpublish_proxy_netdev(entry.state)",
        "peer/epoch cleanup drains proxy users before unpublication",
    )


def test_net_attach_and_withdraw_are_exact_resource_generation_operations() -> None:
    source = REMOTE_NET_CPP.read_text()
    header = REMOTE_NET_HPP.read_text()

    require_tokens(
        header,
        [
            "uint64_t expected_resource_generation = 0",
            "uint64_t resource_generation = 0;",
            "wki_remote_net_detach_resource_generation",
        ],
        "remote NET generation-qualified public API",
    )

    attach = function_body(source, "wki_remote_net_attach")
    require_tokens(
        attach,
        [
            "NetPeerLifecycleLease ATTACH_PEER_LIFECYCLE(PEER)",
            "ATTACH_PEER_LIFECYCLE.release()",
            "NetPeerLifecycleLease PUBLICATION_PEER_LIFECYCLE",
            "while (!PUBLICATION_PEER_LIFECYCLE.try_acquire(PEER))",
            "PEER->state != PeerState::CONNECTED",
            "PEER->vfs_reset_rebind_pending.load(std::memory_order_acquire)",
            "wki_resource_generation_snapshot(owner_node, ResourceType::NET, resource_id)",
            "wki_resource_observation_snapshot(owner_node, ResourceType::NET, resource_id, RESOURCE_GENERATION",
            "state->resource_generation = RESOURCE_GENERATION",
            "wki_resource_observation_is_live(owner_node, ResourceType::NET, resource_id, RESOURCE_GENERATION, owner_incarnation)",
            "wki_channel_generation_is_live(reserved_channel_identity.channel",
            "bool const PROXY_STILL_PUBLISHED",
            "wki_remote_net_detach_resource_generation(owner_node, resource_id, RESOURCE_GENERATION)",
        ],
        "attach exact observation and peer-epoch validation",
    )
    require_order(
        attach,
        "int const SEND_RET = wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_REQ",
        "ATTACH_PEER_LIFECYCLE.release()",
        "request is sent while the initial lifecycle gate is held",
    )
    require_order(
        attach,
        "ATTACH_PEER_LIFECYCLE.release()",
        "wki_wait_for_op(&wait, WKI_DEV_PROXY_TIMEOUT_US)",
        "peer lifecycle gate is released before the synchronous attach wait",
    )
    require_order(
        attach,
        "wki_wait_for_op(&wait, WKI_DEV_PROXY_TIMEOUT_US)",
        "while (!PUBLICATION_PEER_LIFECYCLE.try_acquire(PEER))",
        "final publication reacquires the peer lifecycle gate after ACK dispatch",
    )
    require_order(
        attach,
        "wki_resource_observation_is_live(owner_node, ResourceType::NET, resource_id, RESOURCE_GENERATION, owner_incarnation)",
        "ker::net::netdev_register(&state->netdev)",
        "exact observation checked immediately before registry publication",
    )

    exact_detach = function_body(source, "wki_remote_net_detach_resource_generation")
    require_tokens(
        exact_detach,
        [
            "proxy->owner_node != owner_node",
            "proxy->resource_id != resource_id",
            "proxy->resource_generation != resource_generation",
            "state->retiring.store(true, std::memory_order_release)",
            "state->netdev.state = 0",
            "claim_and_clear_waiter_locked",
            "stage_net_detach_locked(state, owner_node, resource_id, attach_cookie, false)",
            "wait_for_net_proxy_refs_to_drain(state)",
            "unpublish_proxy_netdev(state)",
            "wki_deferred_work_notify()",
            "close_net_channel_identity(channel_identity)",
        ],
        "withdraw/replacement exact proxy retirement",
    )
    require_order(
        exact_detach,
        "stage_net_detach_locked(state, owner_node, resource_id, attach_cookie, false)",
        "s_net_proxy_lock.unlock()",
        "generation detach reserves before leaving its registry transition",
    )
    require_order(
        exact_detach,
        "wait_for_net_proxy_refs_to_drain(state)",
        "unpublish_proxy_netdev(state)",
        "generation detach drains raw users before network unpublication",
    )


def test_net_detach_remains_exact_cookie_only_form() -> None:
    body = function_body(REMOTE_NET_CPP.read_text(), "send_net_detach")
    require_tokens(
        body,
        [
            "wki_dev_detach_payload_size(false)",
            "det_buf.at(WKI_DEV_DETACH_COOKIE_OFFSET) = attach_cookie",
            "wki_send_tracked(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, det_buf.data()",
            "static_cast<uint16_t>(det_buf.size())",
            "tx_token_out",
        ],
        "NET detach exact base-plus-cookie form",
    )
    if "if (attach_cookie" in body:
        fail("NET detach must not fall back to a base-only legacy frame")


def main() -> None:
    test_net_proxy_op_slot_wait_is_bounded()
    test_cancel_op_waiter_preserves_successor_slot()
    test_cancel_op_waiter_has_ktest_coverage()
    test_dynamic_channel_uses_exact_allocation_identity()
    test_net_rx_dispatch_matches_exact_generation()
    test_epoch_reset_marker_defers_blocking_cleanup()
    test_attach_ack_lookup_includes_cookie()
    test_published_proxy_storage_and_network_publications_are_retired_safely()
    test_net_attach_and_withdraw_are_exact_resource_generation_operations()
    test_net_detach_remains_exact_cookie_only_form()
    print("WKI remote net source invariants hold")


if __name__ == "__main__":
    main()
