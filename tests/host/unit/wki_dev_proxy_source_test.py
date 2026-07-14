#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DEV_PROXY_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "dev_proxy.cpp"
DEV_PROXY_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "dev_proxy.hpp"
DEV_SERVER_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "dev_server.cpp"
WKI_DEV_PROXY_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "wki_dev_proxy_ktest.cpp"


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


def test_lifecycle_flags_are_atomic() -> None:
    header = DEV_PROXY_HPP.read_text()
    required = [
        "std::atomic<bool> active{false}",
        "std::atomic<bool> fenced{false}",
    ]
    missing = [token for token in required if token not in header]
    if missing:
        fail("ProxyBlockState lifecycle flags must be atomic: " + ", ".join(missing))


def test_lifecycle_helpers_use_acquire_release() -> None:
    source = DEV_PROXY_CPP.read_text()
    required = [
        ("proxy_block_active", "state->active.load(std::memory_order_acquire)"),
        ("proxy_block_fenced", "state->fenced.load(std::memory_order_acquire)"),
        ("set_proxy_block_active", "state->active.store(value, std::memory_order_release)"),
        ("set_proxy_block_fenced", "state->fenced.store(value, std::memory_order_release)"),
    ]
    for function, token in required:
        if token not in function_body(source, function):
            fail(f"{function} must contain {token!r}")


def test_lifecycle_flags_are_only_accessed_through_helpers() -> None:
    allowed = [
        "state->active.load(std::memory_order_acquire)",
        "state->fenced.load(std::memory_order_acquire)",
        "state->active.store(value, std::memory_order_release)",
        "state->fenced.store(value, std::memory_order_release)",
    ]
    offenders = []
    for line_no, line in enumerate(DEV_PROXY_CPP.read_text().splitlines(), start=1):
        if "state->active" not in line and "state->fenced" not in line:
            continue
        if any(token in line for token in allowed):
            continue
        offenders.append(f"{line_no}: {line.strip()}")
    if offenders:
        fail("dev_proxy.cpp must not access active/fenced outside lifecycle helpers: " + "; ".join(offenders))


def test_proxy_op_slot_wait_is_bounded() -> None:
    source = DEV_PROXY_CPP.read_text()
    require_tokens(
        source,
        [
            "#include <net/wki/timer_math.hpp>",
            "constexpr uint64_t DEV_PROXY_SLOT_WAIT_TIMEOUT_US = WKI_DEV_PROXY_TIMEOUT_US;",
            "auto acquire_block_op_slot_locked(ProxyBlockState* state, uint64_t start_us) -> int",
        ],
        "dev proxy slot timeout scaffolding",
    )

    acquire_body = function_body(source, "acquire_block_op_slot_locked")
    require_tokens(
        acquire_body,
        [
            "uint64_t const DEADLINE_US = wki_future_deadline_us(start_us, DEV_PROXY_SLOT_WAIT_TIMEOUT_US)",
            "state->lock.lock()",
            "if (!block_proxy_op_slot_busy(state))",
            "return WKI_OK",
            "state->lock.unlock()",
            "if (!block_proxy_op_slot_busy(state))",
            "continue",
            "if (wki_now_us() >= DEADLINE_US)",
            "return WKI_ERR_TIMEOUT",
            "ker::mod::sched::kern_sleep_us(DEV_PROXY_CONTENTION_SLEEP_US)",
        ],
        "dev proxy slot acquisition",
    )
    require_order(acquire_body, "state->lock.lock()", "return WKI_OK", "slot lock success path")
    require_order(acquire_body, "state->lock.unlock()", "return WKI_ERR_TIMEOUT", "slot timeout unlock path")

    prepare_body = function_body(source, "prepare_block_op_wait")
    require_order(
        prepare_body,
        "int const SLOT_RET = acquire_block_op_slot_locked(state, wki_now_us())",
        "state->op_wait_entry = &wait",
        "prepare wait publishes only after slot acquisition",
    )
    require_order(
        prepare_body,
        "if (SLOT_RET != WKI_OK)",
        "return SLOT_RET",
        "prepare wait propagates slot timeout",
    )
    if "ker::mod::sched::kern_sleep_us(DEV_PROXY_CONTENTION_SLEEP_US)" in prepare_body:
        fail("prepare_block_op_wait must not contain its own unbounded contention sleep loop")


def test_rdmaring_wait_deadlines_are_saturating() -> None:
    source = DEV_PROXY_CPP.read_text()
    require_tokens(
        source,
        [
            "constexpr uint64_t DEV_PROXY_BULK_WAIT_TIMEOUT_US = wki_saturating_mul_us(WKI_DEV_PROXY_TIMEOUT_US, 4);",
            "wki_future_deadline_us(wki_now_us(), WKI_DEV_PROXY_TIMEOUT_US)",
            "wki_future_deadline_us(wki_now_us(), DEV_PROXY_BULK_WAIT_TIMEOUT_US)",
        ],
        "dev proxy RDMA deadline helper use",
    )

    forbidden = [
        "wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US",
        "wki_now_us() + (WKI_DEV_PROXY_TIMEOUT_US * 4)",
        "WKI_DEV_PROXY_TIMEOUT_US * 4",
    ]
    present = [token for token in forbidden if token in source]
    if present:
        fail("dev proxy waits must not use wrapping deadline arithmetic: " + ", ".join(present))

    for function in [
        "remote_block_read_rdma",
        "remote_block_write_rdma",
        "remote_block_flush_rdma",
        "rdma_batch_collect",
        "remote_block_bulk_read_rdma",
        "remote_block_bulk_write_rdma",
    ]:
        body = function_body(source, function)
        if "uint64_t const DEADLINE = wki_future_deadline_us(wki_now_us()," not in body:
            fail(f"{function} must build wait deadlines with wki_future_deadline_us")

    attach_body = function_body(source, "wki_dev_proxy_attach_block")
    for token in [
        "uint64_t const ZONE_DEADLINE = wki_future_deadline_us(wki_now_us(), WKI_DEV_PROXY_TIMEOUT_US)",
        "uint64_t const RKEY_DEADLINE = wki_future_deadline_us(wki_now_us(), WKI_DEV_PROXY_TIMEOUT_US)",
        "uint64_t const READY_DEADLINE = wki_future_deadline_us(wki_now_us(), WKI_DEV_PROXY_TIMEOUT_US)",
    ]:
        if token not in attach_body:
            fail(f"wki_dev_proxy_attach_block is missing saturating deadline: {token}")


def test_rdmaring_sq_space_waits_are_bounded() -> None:
    source = DEV_PROXY_CPP.read_text()
    helper_body = function_body(source, "wait_for_rdma_sq_space")
    require_tokens(
        helper_body,
        [
            "while (blk_sq_full(ring_hdr))",
            "if (!proxy_block_active(state) || proxy_block_fenced(state))",
            "if (wki_now_us() >= deadline_us)",
            "rdma_drain_cq(state)",
            "wki_spin_yield_channel_identity(channel_identity)",
            "return true",
        ],
        "bounded RDMA SQ-space wait helper",
    )
    if source.count("while (blk_sq_full(ring_hdr))") != 1:
        fail("dev proxy RDMA SQ-space waits must go through wait_for_rdma_sq_space")

    for function in [
        "remote_block_read_rdma",
        "remote_block_write_rdma",
        "remote_block_flush_rdma",
        "rdma_batch_submit",
        "remote_block_bulk_read_rdma",
        "remote_block_bulk_write_rdma",
    ]:
        body = function_body(source, function)
        if "wait_for_rdma_sq_space(state, ring_hdr, CHANNEL_IDENTITY, DEADLINE)" not in body:
            fail(f"{function} must bound SQ-full waits with wait_for_rdma_sq_space")


def test_fence_wait_uses_ordered_lifecycle_helpers() -> None:
    body = function_body(DEV_PROXY_CPP.read_text(), "wait_for_fence_lift")
    required = [
        "while (proxy_block_fenced(state))",
        "if (!proxy_block_active(state))",
        "return proxy_block_active(state)",
        "ker::mod::sched::kern_yield()",
    ]
    missing = [token for token in required if token not in body]
    if missing:
        fail("fence wait must use ordered lifecycle helpers and yield: " + ", ".join(missing))
    forbidden = ["state->active", "state->fenced"]
    present = [token for token in forbidden if token in body]
    if present:
        fail("fence wait still has raw lifecycle access: " + ", ".join(present))


def test_timeout_marks_inactive_with_release_store() -> None:
    source = DEV_PROXY_CPP.read_text()
    body = function_body(source, "wki_dev_proxy_fence_timeout_tick")
    cleanup = function_body(source, "wki_dev_proxy_cleanup_epoch_reset_for_peer")
    require_tokens(
        body,
        [
            "proxy->epoch_reset_pending = true",
            "wki_dev_proxy_cleanup_epoch_reset_for_peer(owner_node, true, false)",
        ],
        "fence timeout deferred teardown",
    )
    if "mfence" in body:
        fail("fence timeout must not rely on an x86 mfence instead of atomic ordering")
    require_order(
        cleanup,
        "set_proxy_block_active(state, false)",
        "ker::dev::block_device_unregister(&state->bdev)",
        "timeout cleanup marks inactive before unregistering",
    )


def test_dev_proxy_selftest_declared() -> None:
    ktest = WKI_DEV_PROXY_KTEST.read_text()
    required = [
        "KTEST(WkiDevProxyFenceFlags, LifecycleFlagsAreAtomic)",
        "KTEST(WkiDevProxyAttachAck, CookieFencesStaleBlockCompletion)",
        "KTEST(WkiDevProxyRdmaSqWait, StopsOnFenceOrInactive)",
        "std::is_same_v<decltype(std::declval<State&>().active), std::atomic<bool>>",
        "std::is_same_v<decltype(std::declval<State&>().fenced), std::atomic<bool>>",
        "wki_dev_proxy_selftest_attach_ack_cookie_fences_stale_completion()",
        "wki_dev_proxy_selftest_rdma_sq_wait_stops_on_fence()",
    ]
    missing = [token for token in required if token not in ktest]
    if missing:
        fail("missing dev proxy lifecycle KTEST coverage: " + ", ".join(missing))


def test_attach_ack_requires_expected_cookie_before_completion() -> None:
    header = DEV_PROXY_HPP.read_text()
    source = DEV_PROXY_CPP.read_text()
    require_tokens(
        header,
        [
            "uint8_t attach_expected_cookie = 0;",
            "auto wki_dev_proxy_selftest_attach_ack_cookie_fences_stale_completion() -> bool;",
            "auto wki_dev_proxy_selftest_rdma_sq_wait_stops_on_fence() -> bool;",
        ],
        "dev proxy attach-cookie state",
    )
    require_tokens(
        source,
        [
            "uint8_t g_block_attach_next_cookie = 1;",
            "auto allocate_block_attach_cookie_locked(uint16_t owner_node, uint32_t resource_id,",
            "auto block_attach_ack_matches_pending_locked(ProxyBlockState const* state, const DevAttachAckPayload& ack, const uint8_t* payload,",
            "state->attach_expected_cookie == 0",
            "wki_dev_attach_ack_matches_expected(state->attach_expected_cookie, ack)",
            "attach_req.attach_cookie = attach_cookie",
        ],
        "dev proxy attach-cookie scaffolding",
    )

    attach_body = function_body(source, "wki_dev_proxy_attach_block")
    require_order(
        attach_body,
        "attach_req.attach_cookie = attach_cookie",
        "state->attach_expected_cookie = attach_cookie",
        "block attach publishes cookie before send",
    )
    require_order(
        attach_body,
        "state->attach_expected_cookie = attach_cookie",
        "wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_REQ",
        "block attach sends only after expected cookie is armed",
    )

    resume_body = function_body(source, "wki_dev_proxy_resume_for_peer")
    require_order(
        resume_body,
        "attach_req.attach_cookie = attach_cookie",
        "p->attach_expected_cookie = attach_cookie",
        "block resume publishes cookie before send",
    )
    require_order(
        resume_body,
        "p->attach_expected_cookie = attach_cookie",
        "wki_send(node_id, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_REQ",
        "block resume sends only after expected cookie is armed",
    )

    ack_body = function_body(source, "handle_dev_attach_ack")
    require_order(
        ack_body,
        "if (!block_attach_ack_matches_pending_locked(state, *ack, payload, payload_len))",
        "state->attach_status = ack->status",
        "block attach ACK validates cookie before publishing status",
    )
    require_order(
        ack_body,
        "state->attach_expected_cookie = 0",
        "state->attach_pending.store(false, std::memory_order_release)",
        "block attach ACK clears cookie before completing",
    )

    allocator = function_body(source, "allocate_block_attach_cookie_locked")
    require_tokens(
        allocator,
        [
            "attempt < UINT8_MAX",
            "state->owner_node != owner_node",
            "state->resource_id != resource_id",
            "state->binding_attach_cookie == cookie",
            "wki_resource_incarnation_equal(EXISTING_INCARNATION, owner_incarnation)",
            "return 0",
        ],
        "block attach-cookie wrap exclusion",
    )
    require_order(
        attach_body,
        "allocate_block_attach_cookie_locked(owner_node, resource_id, expected_owner_incarnation)",
        "g_routable_proxies.push_back(state)",
        "initial block cookie reservation publishes atomically",
    )
    require_order(
        resume_body,
        "allocate_block_attach_cookie_locked(node_id, p->resource_id, resume_incarnation)",
        "p->binding_attach_cookie = attach_cookie",
        "resumed block cookie reservation publishes atomically",
    )


def test_failed_attach_erases_exact_proxy_not_tail() -> None:
    header = DEV_PROXY_HPP.read_text()
    source = DEV_PROXY_CPP.read_text()
    attach_body = function_body(source, "wki_dev_proxy_attach_block")
    cleanup_body = function_body(source, "cleanup_failed_block_attach")
    erase_body = function_body(source, "erase_proxy_exact_locked")

    require_tokens(
        header,
        ["auto wki_dev_proxy_selftest_failed_attach_erases_exact_proxy() -> bool;"],
        "dev proxy failed attach exact erase declaration",
    )
    require_tokens(
        source,
        [
            "auto erase_proxy_exact_locked(ProxyBlockState* state) -> bool",
            "void cleanup_failed_block_attach(ProxyBlockState* state, const WkiChannelIdentity& channel_to_close, bool send_detach)",
            "auto wki_dev_proxy_selftest_failed_attach_erases_exact_proxy() -> bool",
        ],
        "dev proxy failed attach exact erase scaffolding",
    )
    require_tokens(
        erase_body,
        [
            "it->get() == state",
            "g_proxies.erase(it)",
            "return true",
        ],
        "dev proxy exact erase helper",
    )
    require_tokens(
        cleanup_body,
        [
            "stage_block_detach_locked(state, owner_node, resource_id, attach_cookie, detach_incarnation, false)",
            "wki_channel_close_generation(channel_to_close.channel, channel_to_close.peer_node_id, channel_to_close.channel_id,",
            "wki_zone_destroy(rdma_zone_id)",
            "delete[] ra_buf",
            "delete[] bulk_buf",
            "erase_proxy_exact(state)",
            "wki_deferred_work_notify()",
        ],
        "dev proxy failed attach cleanup",
    )
    require_order(
        cleanup_body,
        "stage_block_detach_locked(state, owner_node, resource_id, attach_cookie, detach_incarnation, false)",
        "set_proxy_block_active(state, false)",
        "failed attach reserves the exact BLOCK tuple before becoming inactive",
    )
    require_order(
        cleanup_body,
        "s_proxy_lock.unlock()",
        "wki_deferred_work_notify()",
        "failed attach notifies deferred work only after releasing the registry lock",
    )
    if "send_or_defer_block_detach" in cleanup_body:
        fail("failed block attach cleanup must stage inside its registry transition")
    if re.search(
        r"detach_incarnation\s*=\s*wki_resource_incarnation_valid\(state->binding_incarnation\)", cleanup_body
    ) is None:
        fail("dev proxy failed attach cleanup must preserve the exact binding incarnation")
    if "g_proxies.pop_back()" in attach_body:
        fail("failed block attach cleanup must erase the exact proxy, not the current deque tail")
    if attach_body.count("cleanup_failed_block_attach(state,") < 8:
        fail("all block attach failure exits must use cleanup_failed_block_attach")

    ktest = WKI_DEV_PROXY_KTEST.read_text()
    require_tokens(
        ktest,
        [
            "KTEST(WkiDevProxyAttachFailure, ErasesExactProxy)",
            "wki_dev_proxy_selftest_failed_attach_erases_exact_proxy()",
        ],
        "dev proxy failed attach exact erase KTEST coverage",
    )


def test_block_io_and_resume_are_serialized_against_cleanup() -> None:
    header = DEV_PROXY_HPP.read_text()
    source = DEV_PROXY_CPP.read_text()
    require_tokens(
        header,
        [
            "mod::sys::Mutex io_lock;",
            "bool cleanup_in_progress = false;",
            "bool resume_in_progress = false;",
        ],
        "block I/O and lifecycle serialization fields",
    )

    lease_body = function_body(source, "wait_for_block_io_quiescence")
    require_tokens(
        lease_body,
        ["state->io_lock.lock()", "state->io_lock.unlock()"],
        "block cleanup I/O drain",
    )

    detach_body = function_body(source, "wki_dev_proxy_detach_block")
    require_tokens(
        detach_body,
        [
            "if (state->resume_in_progress)",
            "state->epoch_reset_pending = false",
            "state->cleanup_in_progress = true",
            "wait_for_block_io_quiescence(state)",
            "state->cleanup_in_progress = false",
        ],
        "ordinary detach single-owner claim",
    )
    require_order(
        detach_body,
        "ker::dev::block_device_unregister(proxy_bdev)",
        "state->cleanup_in_progress = false",
        "ordinary detach holds cleanup claim through unregister",
    )

    cleanup_body = function_body(source, "wki_dev_proxy_cleanup_epoch_reset_for_peer")
    require_tokens(
        cleanup_body,
        [
            "!proxy->epoch_reset_pending || proxy->cleanup_in_progress",
            "if (proxy->resume_in_progress)",
            "state->cleanup_in_progress = true",
            "wait_for_block_io_quiescence(state)",
            "state->cleanup_in_progress = false",
        ],
        "epoch cleanup single-owner claim",
    )
    require_order(
        cleanup_body,
        "ker::dev::block_device_unregister(&state->bdev)",
        "state->cleanup_in_progress = false",
        "epoch cleanup holds claim through unregister",
    )

    resume_body = function_body(source, "wki_dev_proxy_resume_for_peer")
    require_tokens(
        resume_body,
        [
            "p->resume_in_progress = true",
            "BlockResumeClaim const RESUME_CLAIM(p)",
            "bool attach_request_sent = false",
            "attach_request_sent = true",
            "send_or_defer_block_detach(p, node_id, p->resource_id, attach_cookie, p->attach_expected_incarnation)",
            "BlockAttachIdentityLease RESUME_PUBLICATION_LEASE",
            "RESUME_PUBLICATION_LEASE.try_acquire(",
            "LEASE_RESULT == BlockAttachLeaseTryResult::STALE",
            "cancelled = p->epoch_reset_pending || p->cleanup_in_progress || !proxy_block_active(p)",
        ],
        "resume lifecycle claim",
    )
    require_order(
        resume_body,
        "setup_block_bulk_staging(p)",
        "BlockAttachIdentityLease RESUME_PUBLICATION_LEASE",
        "final resource lease follows all blocking resume setup",
    )
    require_order(
        resume_body,
        "RESUME_PUBLICATION_LEASE.try_acquire(",
        "set_proxy_block_fenced(p, false)",
        "exact resource lease covers resume fence publication",
    )

    attach_gate = function_body(source, "block_attach_blocked_by_retiring_binding_locked")
    require_tokens(
        attach_gate,
        [
            "bool const ACTIVE = proxy_block_active(state)",
            "!ACTIVE",
            "state->cleanup_in_progress",
            "state->epoch_reset_pending",
            "state->resume_in_progress",
            "ACTIVE && proxy_block_fenced(state)",
        ],
        "BLOCK attach admission fences indexed inactive and reconnect-retiring rows",
    )


def test_block_detach_uses_exact_negotiated_incarnation_form() -> None:
    source = DEV_PROXY_CPP.read_text()
    body = function_body(source, "send_block_detach")
    require_tokens(
        body,
        [
            "wki_dev_detach_payload_size(true)",
            "det_buf.at(WKI_DEV_DETACH_COOKIE_OFFSET) = attach_cookie",
            "wki_resource_incarnation_negotiated(owner_node, ResourceType::BLOCK)",
            "wki_resource_incarnation_valid(resource_incarnation)",
            "det_buf.data() + WKI_DEV_DETACH_INCARNATION_OFFSET",
            "wki_dev_detach_payload_size(WITH_INCARNATION)",
            "wki_send_tracked(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, det_buf.data(), DETACH_SIZE, tx_token_out)",
        ],
        "negotiated BLOCK detach suffix",
    )
    require_order(body, "wki_dev_detach_payload_size(WITH_INCARNATION)", "wki_send_tracked(owner_node", "exact BLOCK detach length")

    detach_body = function_body(source, "wki_dev_proxy_detach_block")
    require_tokens(
        detach_body,
        [
            "ResourceIncarnationToken const BINDING_INCARNATION = state->binding_incarnation",
            "stage_block_detach_locked(state, OWNER_NODE, RESOURCE_ID, BINDING_ATTACH_COOKIE, BINDING_INCARNATION, false)",
        ],
        "normal BLOCK detach binding snapshot",
    )
    require_order(
        detach_body,
        "stage_block_detach_locked(state, OWNER_NODE, RESOURCE_ID, BINDING_ATTACH_COOKIE, BINDING_INCARNATION, false)",
        "unindex_proxy_locked(state)",
        "normal BLOCK detach reserves before leaving the routable index",
    )


def test_published_tombstones_are_not_in_the_ingress_index() -> None:
    source = DEV_PROXY_CPP.read_text()
    require_tokens(
        source,
        [
            "std::deque<std::unique_ptr<ProxyBlockState>> g_proxies",
            "std::deque<ProxyBlockState*> g_routable_proxies",
            "void unindex_proxy_locked(ProxyBlockState* state)",
            "g_routable_proxies.push_back(state)",
        ],
        "permanent storage with bounded ingress index",
    )
    require_tokens(
        function_body(source, "erase_proxy_exact_locked"),
        ["state->ever_published", "state->detach_pending", "state->cleanup_in_progress"],
        "published, pending-detach, and in-cleanup BLOCK state must remain pinned",
    )
    for helper in ["find_proxy_by_bdev", "find_proxy_by_channel", "find_proxy_by_attach"]:
        body = function_body(source, helper)
        if "g_routable_proxies" not in body or "g_proxies" in body:
            fail(f"{helper} must search only live/pending proxy pointers")

    mark_body = function_body(source, "wki_dev_proxy_mark_epoch_reset")
    require_tokens(
        mark_body,
        [
            "for (auto* state : g_routable_proxies)",
            "state->epoch_wake_next = wake_head",
            "while (wake_head != nullptr)",
        ],
        "single-pass epoch ingress marker",
    )
    if "for (auto&" in mark_body and "g_proxies" in mark_body:
        fail("epoch marker must not scan retained historical storage")


def test_bulk_write_uses_server_pull_and_resume_restores_staging() -> None:
    source = DEV_PROXY_CPP.read_text()
    write_body = function_body(source, "remote_block_bulk_write_rdma")
    require_tokens(
        write_body,
        [
            "memcpy(state->bulk_staging_buf, src, static_cast<size_t>(chunk_bytes))",
            "sqe.data_slot = state->bulk_staging_rkey",
        ],
        "bulk-write consumer staging publication",
    )
    corrupting_push = (
        "rdma_write(state->rdma_transport, state->owner_node, state->rdma_remote_rkey, 0, "
        "state->bulk_staging_buf"
    )
    if corrupting_push in re.sub(r"\s+", " ", write_body):
        fail("bulk write must not overwrite the server ring header with consumer staging data")

    server_poll = function_body(DEV_SERVER_CPP.read_text(), "blk_ring_server_poll")
    require_tokens(
        server_poll,
        [
            "uint32_t const CONSUMER_RKEY = sqe->data_slot",
            "rdma_read(binding->blk_rdma_transport, binding->consumer_node, CONSUMER_RKEY, 0, staging,",
            "ker::dev::block_write(binding->block_dev, sqe->lba, sqe->block_count, staging)",
        ],
        "bulk-write owner pull",
    )
    require_order(
        server_poll,
        "rdma_read(binding->blk_rdma_transport, binding->consumer_node, CONSUMER_RKEY, 0, staging,",
        "ker::dev::block_write(binding->block_dev, sqe->lba, sqe->block_count, staging)",
        "owner pulls consumer staging before block write",
    )

    setup_body = function_body(source, "setup_block_bulk_staging")
    require_tokens(
        setup_body,
        [
            "state->rdma_transport->rdma_register_region",
            "state->bulk_staging_rkey = rkey",
            "state->bdev.capabilities |= ker::dev::BDEV_CAP_BULK_RDMA",
        ],
        "bulk staging setup",
    )
    resume_body = function_body(source, "wki_dev_proxy_resume_for_peer")
    require_order(
        resume_body,
        "static_cast<void>(setup_block_bulk_staging(p))",
        "set_proxy_block_fenced(p, false)",
        "resume restores bulk staging before lifting fence",
    )


def main() -> None:
    test_lifecycle_flags_are_atomic()
    test_lifecycle_helpers_use_acquire_release()
    test_lifecycle_flags_are_only_accessed_through_helpers()
    test_proxy_op_slot_wait_is_bounded()
    test_rdmaring_wait_deadlines_are_saturating()
    test_rdmaring_sq_space_waits_are_bounded()
    test_fence_wait_uses_ordered_lifecycle_helpers()
    test_timeout_marks_inactive_with_release_store()
    test_dev_proxy_selftest_declared()
    test_attach_ack_requires_expected_cookie_before_completion()
    test_failed_attach_erases_exact_proxy_not_tail()
    test_block_io_and_resume_are_serialized_against_cleanup()
    test_block_detach_uses_exact_negotiated_incarnation_form()
    test_published_tombstones_are_not_in_the_ingress_index()
    test_bulk_write_uses_server_pull_and_resume_restores_staging()
    print("WKI dev proxy source invariants hold")


if __name__ == "__main__":
    main()
