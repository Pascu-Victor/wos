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
    owner_block = braced_block_after(body, "if (state->op_wait_entry == &wait ||")

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
        "if (state->op_wait_entry == &wait ||",
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
            "publish_retiring_net_state(state)",
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
            "publish_retiring_net_state(p)",
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
            "publish_retiring_net_state(state)",
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


def test_open_distinguishes_transport_failures_from_owner_errno() -> None:
    source = REMOTE_NET_CPP.read_text()
    wrapper = function_body(source, "proxy_net_open_as")
    require_tokens(wrapper, ["return -ENODEV", "open_retained_net_proxy(state, caller)", "release_net_proxy(state)", "return STATUS"],
                   "retained open callback")
    require_order(wrapper, "acquire_net_proxy_by_dev(dev)", "open_retained_net_proxy(state, caller)", "open retains before operation")
    require_order(wrapper, "open_retained_net_proxy(state, caller)", "release_net_proxy(state)", "open releases after operation")
    body = function_body(source, "open_retained_net_proxy")
    require_tokens(body, ["net_open_transport_errno(PREP_RET)", "net_open_transport_errno(SEND_RET)",
                          "net_open_transport_errno(WAIT_RC)", "return op_status"], "separate error domains")
    if "net_open_transport_errno(op_status)" in body or "release_net_proxy" in body or "return -1" in body:
        fail("retained open must preserve owner errno and its caller's pin")
    require_order(body, "cancel_op_waiter(state, wait, SEND_RET)", "return net_open_transport_errno(SEND_RET)", "send failure drain")
    require_order(body, "cancel_op_waiter(state, wait, WAIT_RC)", "return net_open_transport_errno(WAIT_RC)", "wait failure drain")
    require_order(body, "if (op_status == 0)", "dev->state = 1", "open publication follows owner success")
    require_order(body, "if (op_status == 0)", "set_net_rx_credits_locked", "RX credits follow owner success")
    tests = REMOTE_NET_KTEST.read_text()
    require_tokens(tests, ["wki_remote_net_selftest_open_transport_failures()", "wki_remote_net_selftest_open_error_mapping()"],
                   "real failure and timeout KTESTs")


def test_completed_result_is_owned_by_the_call_and_slot_release_is_exact() -> None:
    source = REMOTE_NET_CPP.read_text()
    require_tokens(REMOTE_NET_HPP.read_text(), ["struct NetOpCompletion", "NetOpCompletion* op_completion = nullptr"], "call result owner")
    prepare = function_body(source, "prepare_net_op_wait")
    require_order(prepare, "state->op_completion = &completion", "state->op_pending.store(true", "owner before slot publication")
    handler = function_body(source, "handle_net_op_resp")
    require_order(handler, "wki_dev_op_response_matches_expected", "publish_net_op_response_locked(state, *resp, resp_data)",
                  "exact op/cookie match before response publication")
    require_order(handler, "publish_net_op_response_locked(state, *resp, resp_data)", "finish_claimed_waiter(wait_entry, 0)",
                  "result copy before wake")
    publish = function_body(source, "publish_net_op_response_locked")
    require_order(publish, "state->op_wait_entry != &completion->wait", "claim_and_clear_waiter_locked", "duplicate reply cannot release slot")
    require_order(publish, "if (wait_entry == nullptr)", "completion->status = response.status", "claim before result write")
    require_order(publish, "if (wait_entry == nullptr)", "memcpy(state->op_resp_buf", "claim before output write")
    require_tokens(publish, ["completion->response_length = state->op_resp_len"], "call-owned response length")
    consume = function_body(source, "consume_net_op_result")
    require_tokens(consume, ["int const STATUS = completion.status", "uint16_t const RESP_LEN = completion.response_length"], "immutable result consumption")
    owner = braced_block_after(consume, "if (state->op_completion == &completion)")
    require_tokens(owner, ["state->op_wait_entry = nullptr", "clear_net_op_state_locked(state, STATUS)"], "exact release")
    if "state->op_status" in consume or "state->op_resp_len" in consume:
        fail("completed result consumption must not read mutable proxy result fields")
    require_tokens(function_body(source, "clear_net_op_state_locked"), ["state->op_completion = nullptr"], "retirement removes stack owner reference")
    for callback in ["open_retained_net_proxy", "close_retained_net_proxy", "apply_retained_net_link"]:
        require_tokens(function_body(source, callback), ["NetOpCompletion completion", "auto& wait = completion.wait",
                       "consume_net_op_result(state, completion,"], "all synchronous callers own completion")
    require_tokens(REMOTE_NET_KTEST.read_text(), ["wki_remote_net_selftest_completed_result_survives_retirement()",
                   "wki_remote_net_selftest_completion_ownership()"], "completion lifetime KTEST coverage")


def test_net_open_identity_has_no_legacy_fallback() -> None:
    source = REMOTE_NET_CPP.read_text()
    legacy = function_body(source, "proxy_net_open")
    require_tokens(legacy, ["return -ENOTSUP"], "legacy consumer open refusal")
    if "wki_send" in legacy or "open_retained" in legacy:
        fail("credentialless open must not send")
    wrapper = function_body(source, "proxy_net_open_as")
    require_order(wrapper, "authorize_network_administration(caller)", "acquire_net_proxy_by_dev(dev)", "local policy before lookup")
    body = function_body(source, "open_retained_net_proxy")
    require_order(body, "!state->open_identity_supported", "prepare_net_op_wait", "unsupported owner before operation side effects")
    require_tokens(body, ["OP_NET_OPEN_AS", "credential_record::encode(caller)", "WKI_NET_OPEN_IDENTITY_VERSION"], "complete captured caller record")
    owner = function_body(source, "net_control_caller_status")
    require_order(owner, "credential_record::decode", "map_net_control_identity", "decode before configured mapping")
    require_order(owner, "map_net_control_identity", "authorize_network_administration(mapped)", "mapped caller policy")
    require_order(function_body(source, "open_net_device_as"), "if (status != 0) return status", "dev->ops->open(dev)", "owner refusal before native open")
    if "get_current_task" in owner or "bootstrap_root" in owner:
        fail("owner open must not use ambient credentials")
    handler = function_body(source, "handle_net_open_as")
    require_order(handler, "if (STATUS == 0)", "wki_dev_server_mark_net_opened", "subscription only on successful authorized open")
    old = braced_block_after(function_body(source, "handle_net_op"), "case OP_NET_OPEN:")
    require_tokens(old, ["-ENOTSUP"], "legacy owner refusal")
    if "ops->open" in old or "mark_net_opened" in old:
        fail("legacy NET OPEN cannot mutate owner hardware or subscription")


def test_close_caller_mapping_preserves_errors_and_denied_state() -> None:
    source = REMOTE_NET_CPP.read_text()
    wrapper = function_body(source, "proxy_net_close_as")
    require_order(wrapper, "authorize_network_administration(caller)", "acquire_net_proxy_by_dev", "close local actor admission")
    require_order(wrapper, "close_retained_net_proxy(state, caller)", "release_net_proxy", "retained close lifetime")
    require_tokens(wrapper, ["return -ENODEV", "return STATUS"], "close lookup and result")
    body = function_body(source, "close_retained_net_proxy")
    require_order(body, "!state->close_identity_supported", "prepare_net_op_wait", "unsupported close before operation")
    require_tokens(body, ["OP_NET_CLOSE_AS", "credential_record::encode(caller)", "WKI_NET_CLOSE_IDENTITY_VERSION",
                         "net_open_transport_errno(PREP_RET)", "net_open_transport_errno(SEND_RET)",
                         "net_open_transport_errno(WAIT_RC)", "return op_status"], "close framing and errno domains")
    for failure in ["SEND_RET", "WAIT_RC"]:
        require_order(body, f"cancel_op_waiter(state, wait, {failure})", f"return net_open_transport_errno({failure})", "close drains waiter")
    require_order(body, "if (op_status == 0)", "state->netdev.state = 0", "close publishes only owner success")
    if "net_open_transport_errno(op_status)" in body or body.count("state->netdev.state = 0") != 1:
        fail("close must preserve owner errors and denied UP state")
    owner = function_body(source, "close_net_device_as")
    require_order(owner, "net_control_caller_status", "if (STATUS != 0) return STATUS", "close validates mapped caller")
    require_order(owner, "if (STATUS != 0) return STATUS", "dev->ops->close(dev)", "close refuses before driver")
    handler = function_body(source, "handle_net_close_as")
    require_order(handler, "if (STATUS == 0)", "wki_dev_server_mark_net_opened", "close subscription only after success")
    require_tokens(handler, ["OP_NET_CLOSE_AS", "read_net_op_cookie"], "exact close response cookie")
    legacy = braced_block_after(function_body(source, "handle_net_op"), "case OP_NET_CLOSE:")
    require_tokens(legacy, ["-ENOTSUP"], "legacy close refusal")
    if "ops->close" in legacy or "mark_net_opened" in legacy or function_body(source, "proxy_net_close").strip():
        fail("callerless close must not alter state or acquire hardware authority")
    require_tokens(REMOTE_NET_KTEST.read_text(), ["wki_remote_net_selftest_close_identity_owner()",
                   "wki_remote_net_selftest_close_transport_failures()"], "close runtime coverage")


def test_combined_link_uses_one_mapped_driver_transaction() -> None:
    source = REMOTE_NET_CPP.read_text()
    wrapper = function_body(source, "proxy_net_apply_link_as")
    require_order(wrapper, "authorize_network_administration(caller)", "acquire_net_proxy_by_dev", "link actor before proxy lookup")
    require_order(wrapper, "apply_retained_net_link(state, control, caller)", "release_net_proxy", "link proxy lifetime")
    body = function_body(source, "apply_retained_net_link")
    require_order(body, "!state->link_identity_supported", "prepare_net_op_wait", "link support before send")
    require_tokens(body, ["OP_NET_LINK_AS", "credential_record::encode(caller)", "WKI_NET_LINK_IDENTITY_SIZE", "NetLinkControlPayload"], "complete combined record")
    require_order(body, "if (status == 0)", "state->netdev.state", "link state after owner success")
    require_order(body, "if (status == 0)", "state->netdev.mac.bytes.begin()", "MAC after owner success")
    require_order(body, "if (control.change_state && control.up)", "set_net_rx_credits_locked", "combined UP grants initial RX credits")
    for phase in ["PREP_RET", "SEND_RET", "WAIT_RC"]:
        require_tokens(body, [f"net_open_transport_errno({phase})"], "transport error conversion")
    if "net_open_transport_errno(status)" in body or "proxy_net_open" in body or "proxy_net_close" in body:
        fail("combined operation cannot replay separate driver mutations or translate owner errno")
    owner = function_body(source, "apply_net_link_as")
    for name, callback in [("open_net_device_as", "dev->ops->open"), ("close_net_device_as", "dev->ops->close"),
                           ("apply_net_link_as", "netdev_check_mac")]:
        helper = function_body(source, name)
        require_order(helper, "net_control_caller_status", "control_guard.try_acquire(dev)", "identity before control contention")
        require_order(helper, "if (CONTROL_ADMISSION != 0) return", callback, "busy owner cannot invoke driver")
    for operation, helper in [("open", "open_net_device_as"), ("close", "close_net_device_as"), ("link", "apply_net_link_as")]:
        handler = function_body(source, f"handle_net_{operation}_as")
        require_order(handler, "NetDeviceControlGuard control_guard", helper, "handler owns control through subscription update")
        require_order(handler, "identity_policy, control_guard)", "wki_dev_server_mark_net_opened", "same guard survives owner commit")
    require_order(owner, "net_control_caller_status", "netdev_check_mac", "mapped actor before native admission")
    require_order(owner, "netdev_check_mac", "dev->ops->open", "MAC admission before fallible driver effect")
    require_order(owner, "if (status != 0) return status < 0", "dev->ops->set_mac", "open refusal before MAC commit")
    if "get_current_task" in owner or "bootstrap_root" in owner:
        fail("combined owner must not use ambient identity")
    old = braced_block_after(function_body(source, "handle_net_op"), "case OP_NET_SET_MAC:")
    require_tokens(old, ["-ENOTSUP"], "legacy MAC refusal")
    if "ops->set_mac" in old or function_body(source, "proxy_net_set_mac").strip():
        fail("callerless MAC cannot acquire hardware authority")


def test_control_and_notification_publication_share_handoff() -> None:
    source = REMOTE_NET_CPP.read_text()
    notify = function_body(source, "handle_net_state_notify")
    require_order(notify, "control_publication_lock.lock_irqsave()", "state->lock.lock()", "publication before proxy lock")
    require_order(notify, "wki_net_notify_header_matches_expected", "apply_owner_net_state", "validate before cache publication")
    if notify.count("state->lock.unlock()") != notify.count("control_publication_lock.unlock_irqrestore(PUBLICATION_FLAGS)"):
        fail("every notification exit must release both locks")
    apply = function_body(source, "apply_owner_net_state")
    require_order(apply, "retiring.load", "state->owner_link_state =", "retired notification cannot overwrite cached state")
    require_order(apply, "state->owner_state_pending = true", "control_busy.load", "pending notification retained while busy")
    require_tokens(apply, ["owner_mtu_pending |= notify.mtu != 0", "publish_owner_link_state_locked"], "zero MTU preserves prior update")
    finish = function_body(source, "proxy_net_finish_control")
    require_order(finish, "retiring.load", "else if (state->owner_state_pending)", "retirement wins over pending UP")
    require_tokens(finish, ["dev->state = 0", "state->owner_state_pending = false", "state->owner_mtu_pending = false"], "retired completion drains projection")
    require_tokens(function_body(source, "publish_retiring_net_state"), ["netdev_mark_control_retiring(&state->netdev)"], "proxy shares generic retirement fence")
    device = (ROOT / "modules/kern/src/net/netdevice.cpp").read_text()
    retire = function_body(device, "netdev_mark_control_retiring")
    require_order(retire, "control_publication_lock.lock_irqsave()", "control_busy.load", "retirement joins publication interval")
    require_tokens(retire, ["control_retiring = true", "control_before.state = 0", "dev->state = 0"], "retirement fences admission and visible snapshots")
    release = braced_block_after(device, "void NetDeviceControlGuard::finish()")
    require_order(release, "control_publication_lock.lock_irqsave()", "finish_control(DEV)", "flush under publication lock")
    require_order(release, "finish_control(DEV)", "control_busy.store(false", "flush before releasing admission")
    require_order(release, "control_busy.store(false", "control_publication_lock.unlock_irqrestore", "no lost notification during handoff")
    require_tokens(release, ["uint8_t const NOTIFICATIONS", "NetDeviceControlNotify::OWNER_STATE", "NetDeviceControlNotify::RESOURCE_ADVERT"], "completion preserves requested notification kinds")
    for operation in ("open", "close", "link"):
        handler = function_body(source, f"handle_net_{operation}_as")
        require_order(handler, "wki_dev_server_notify_net_changed", "control_guard.finish()", "owner requests only state notification before finish")
        require_order(handler, "control_guard.finish()", "send_net_control", "completed owner state before response")
        require_tokens(handler, ["binding_retiring", "MsgType::DEV_OP_RESP"], "retained binding bounds response retries")
        if "wki_remotable_notify_net_changed" in handler:
            fail("mapped owner control must not trigger resource replacement adverts")
    require_tokens(REMOTE_NET_KTEST.read_text(), ["wki_remote_net_selftest_control_state_publication()"], "runtime publication coverage")


def test_owner_l3_policy_uses_one_retained_snapshot() -> None:
    source = REMOTE_NET_CPP.read_text()
    public = function_body(source, "wki_remote_net_owner_l3_snapshot")
    require_order(public, "acquire_net_proxy_by_dev", "snapshot_owner_l3", "snapshot owns exact proxy")
    require_order(public, "snapshot_owner_l3", "release_net_proxy", "pin outlives owned copy")
    copy = function_body(source, "snapshot_owner_l3")
    require_order(copy, "lock.lock_irqsave()", "retiring.load", "retirement checked under state lock")
    require_order(copy, "owner_ipv6", "unlock_irqrestore", "whole address record copied before unlock")
    policy = (ROOT / "modules/kern/src/net/wki/remotable.cpp").read_text()
    if "proxy_state->owner_" in policy or "static_cast<ProxyNetState*>" in policy:
        fail("attachment policy must not access mutable owner cache through private_data")
    require_tokens(policy, ["auto const OWNER_L3 = wki_remote_net_owner_l3_snapshot(proxy_dev)",
                           "if (!OWNER_L3.valid)", "has_local_subnet_overlap(OWNER_L3.ipv4_addr, OWNER_L3.ipv4_mask)",
                           "wki_net_ipv6_state_has_usable_address(OWNER_L3.ipv6)",
                           "route_add(REMOTE_SUBNET, OWNER_L3.ipv4_mask"], "one post-attach owner observation")
    require_tokens(REMOTE_NET_KTEST.read_text(), ["wki_remote_net_selftest_owner_l3_snapshot()"], "owned receive-state runtime check")


def main() -> None:
    test_owner_l3_policy_uses_one_retained_snapshot()
    test_control_and_notification_publication_share_handoff()
    test_combined_link_uses_one_mapped_driver_transaction()
    test_close_caller_mapping_preserves_errors_and_denied_state()
    test_net_open_identity_has_no_legacy_fallback()
    test_completed_result_is_owned_by_the_call_and_slot_release_is_exact()
    test_open_distinguishes_transport_failures_from_owner_errno()
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
