#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
ZONE_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "zone.cpp"
ZONE_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "zone.hpp"
WKI_WAIT_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "wki_wait_ktest.cpp"
WKI_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "wki.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:void|auto)\s+{name}\([^)]*\)\s*(?:const\s*)?(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
        source,
        flags=re.DOTALL,
    )
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


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def reject_tokens(source: str, tokens: list[str], context: str) -> None:
    present = [token for token in tokens if token in source]
    if present:
        fail(f"{context}: forbidden {', '.join(present)}")


def require_order(body: str, before: str, after: str, context: str) -> None:
    before_pos = body.find(before)
    after_pos = body.find(after)
    if before_pos < 0 or after_pos < 0 or before_pos >= after_pos:
        fail(f"{context}: expected {before!r} before {after!r}")


def require_token_after(body: str, anchor: str, token: str, context: str) -> None:
    anchor_pos = body.find(anchor)
    if anchor_pos < 0:
        fail(f"{context}: missing anchor {anchor!r}")
    if token not in body[anchor_pos:]:
        fail(f"{context}: missing {token!r} after {anchor!r}")


def test_timeout_retirement_helper_marks_inactive_under_table_lock() -> None:
    body = function_body(ZONE_CPP.read_text(), "retire_zone_after_timeout")
    require_order(body, "s_zone_table_lock.lock()", "mark_zone_retiring_locked(zone)", "zone timeout retire lock")
    require_order(body, "mark_zone_retiring_locked(zone)", "s_zone_table_lock.unlock()", "zone timeout retire unlock")


def test_timeout_cleanup_clears_pending_waiters() -> None:
    source = ZONE_CPP.read_text()
    read_body = function_body(source, "clear_read_waiter_after_wait")
    write_body = function_body(source, "clear_write_waiter_after_wait")

    require_tokens(
        read_body,
        [
            "zone->read_wait_entry = nullptr",
            "wait_result == WKI_ERR_TIMEOUT",
            "zone->read_status = WKI_ERR_ZONE_TIMEOUT",
            "zone->read_pending.store(false, std::memory_order_release)",
            "zone->read_expected_cookie = 0",
        ],
        "zone read timeout cleanup",
    )
    require_tokens(
        write_body,
        [
            "zone->write_wait_entry = nullptr",
            "wait_result == WKI_ERR_TIMEOUT",
            "zone->write_status = WKI_ERR_ZONE_TIMEOUT",
            "zone->write_pending.store(false, std::memory_order_release)",
            "zone->write_expected_offset = 0",
            "zone->write_expected_len = 0",
            "zone->write_expected_cookie = 0",
        ],
        "zone write timeout cleanup",
    )


def test_message_read_write_timeouts_retire_zone_before_release() -> None:
    source = ZONE_CPP.read_text()
    read_body = function_body(source, "read_zone")
    write_body = function_body(source, "write_zone")

    require_order(read_body, "clear_read_waiter_after_wait(zone, read_wait, WAIT_RC)", "retire_zone_after_timeout(zone)", "read timeout retire")
    require_token_after(read_body, "retire_zone_after_timeout(zone)", "release_zone(zone)", "read timeout release")
    require_order(
        write_body,
        "clear_write_waiter_after_wait(zone, write_wait, WAIT_RC)",
        "retire_zone_after_timeout(zone)",
        "write timeout retire",
    )
    require_token_after(write_body, "retire_zone_after_timeout(zone)", "release_zone(zone)", "write timeout release")


def test_late_completion_handlers_reject_retired_or_inactive_zones() -> None:
    source = ZONE_CPP.read_text()
    read_resp_body = function_body(source, "handle_zone_read_resp")
    write_ack_body = function_body(source, "handle_zone_write_ack")

    require_tokens(
        read_resp_body,
        [
            "zone->peer_node_id != hdr->src_node",
            "zone->retiring.load(std::memory_order_acquire)",
            "zone->state != ZoneState::ACTIVE",
            "!zone->read_pending.load(std::memory_order_acquire)",
        ],
        "late zone read response fence",
    )
    require_tokens(
        write_ack_body,
        [
            "zone->peer_node_id != hdr->src_node",
            "zone->retiring.load(std::memory_order_acquire)",
            "zone->state != ZoneState::ACTIVE",
            "!zone->write_pending.load(std::memory_order_acquire)",
        ],
        "late zone write ACK fence",
    )


def test_zone_ranges_and_read_responses_are_validated_without_wrapping() -> None:
    source = ZONE_CPP.read_text()
    header = ZONE_HPP.read_text()

    require_tokens(
        header,
        [
            "uint32_t next_op_cookie = 1",
            "uint32_t read_expected_offset = 0",
            "uint32_t read_expected_len = 0",
            "uint32_t read_expected_cookie = 0",
            "uint32_t write_expected_offset = 0",
            "uint32_t write_expected_len = 0",
            "uint32_t write_expected_cookie = 0",
        ],
        "zone pending operation metadata",
    )

    range_body = function_body(source, "zone_range_valid")
    require_tokens(range_body, ["offset <= size", "len <= size - offset"], "zone range helper")
    reject_tokens(range_body, ["offset + len"], "zone range helper")

    match_body = function_body(source, "zone_read_response_matches_pending")
    require_tokens(
        match_body,
        [
            "resp.offset == zone.read_expected_offset",
            "resp.length == zone.read_expected_len",
            "resp.op_cookie == zone.read_expected_cookie",
        ],
        "zone read response matcher",
    )
    write_match_body = function_body(source, "zone_write_ack_matches_pending")
    require_tokens(
        write_match_body,
        [
            "ack.offset == zone.write_expected_offset",
            "ack.length == zone.write_expected_len",
            "ack.op_cookie == zone.write_expected_cookie",
        ],
        "zone write ACK matcher",
    )

    read_body = function_body(source, "read_zone")
    write_body = function_body(source, "write_zone")
    read_req_body = function_body(source, "handle_zone_read_req")
    write_req_body = function_body(source, "handle_zone_write_req")
    read_resp_body = function_body(source, "handle_zone_read_resp")

    require_tokens(
        read_body,
        [
            "!zone_range_valid(offset, len, zone->size)",
            "if (zone->read_pending.load(std::memory_order_acquire))",
            "return WKI_ERR_BUSY",
            "uint32_t const OP_COOKIE = allocate_zone_op_cookie_locked(zone)",
            "zone->read_expected_offset = cur_offset",
            "zone->read_expected_len = chunk",
            "zone->read_expected_cookie = OP_COOKIE",
            "req.op_cookie = OP_COOKIE",
        ],
        "zone read initiator range and response tracking",
    )
    require_order(
        read_body,
        "if (zone->read_pending.load(std::memory_order_acquire))",
        "zone->read_wait_entry = &read_wait",
        "zone read slot busy check",
    )
    require_order(
        read_body,
        "zone->read_expected_cookie = OP_COOKIE",
        "zone->read_pending.store(true, std::memory_order_release)",
        "zone read metadata before pending publish",
    )
    require_tokens(
        write_body,
        [
            "!zone_range_valid(offset, len, zone->size)",
            "if (zone->write_pending.load(std::memory_order_acquire))",
            "return WKI_ERR_BUSY",
            "uint32_t const OP_COOKIE = allocate_zone_op_cookie_locked(zone)",
            "zone->write_expected_offset = cur_offset",
            "zone->write_expected_len = chunk",
            "zone->write_expected_cookie = OP_COOKIE",
            "req->op_cookie = OP_COOKIE",
        ],
        "zone write initiator range and ACK tracking",
    )
    require_order(
        write_body,
        "if (zone->write_pending.load(std::memory_order_acquire))",
        "zone->write_wait_entry = &write_wait",
        "zone write slot busy check",
    )
    require_order(
        write_body,
        "zone->write_expected_cookie = OP_COOKIE",
        "zone->write_pending.store(true, std::memory_order_release)",
        "zone write metadata before pending publish",
    )
    reject_tokens(read_body + write_body, ["offset + len > zone->size"], "zone initiator range checks")

    require_tokens(
        read_req_body,
        [
            "req->length > WKI_ZONE_MAX_MSG_DATA",
            "!zone_range_valid(req->offset, req->length, zone->size)",
            "resp->op_cookie = req->op_cookie",
        ],
        "zone read request bounds",
    )
    require_tokens(
        write_req_body,
        [
            "req->length > WKI_ZONE_MAX_MSG_DATA",
            "!zone_range_valid(req->offset, req->length, zone->size)",
            "req->length > payload_len - sizeof(ZoneWriteReqPayload)",
            "ack.offset = req->offset",
            "ack.length = req->length",
            "ack.op_cookie = req->op_cookie",
        ],
        "zone write request bounds",
    )
    reject_tokens(read_req_body + write_req_body, ["req->offset + req->length"], "zone RX range checks")

    require_tokens(
        read_resp_body,
        [
            "resp->length > payload_len - sizeof(ZoneReadRespPayload)",
            "!zone_read_response_matches_pending(*zone, *resp)",
            "zone->read_dest_buf = nullptr",
            "zone->read_expected_offset = 0",
            "zone->read_expected_len = 0",
            "zone->read_expected_cookie = 0",
        ],
        "zone read response bounds",
    )
    require_order(
        read_resp_body,
        "if (!MATCHED_ERROR &&",
        "claim_and_clear_waiter_locked(zone->read_wait_entry)",
        "zone read response validates before claiming waiter",
    )
    reject_tokens(read_resp_body, ["zone->read_status = WKI_ERR_INVALID"], "stale read response handling")

    write_ack_body = function_body(source, "handle_zone_write_ack")
    require_tokens(
        write_ack_body,
        [
            "!zone_write_ack_matches_pending(*zone, *ack)",
            "zone->write_expected_offset = 0",
            "zone->write_expected_len = 0",
            "zone->write_expected_cookie = 0",
        ],
        "zone write ACK identity fence",
    )
    require_order(
        write_ack_body,
        "if (!zone_write_ack_matches_pending(*zone, *ack))",
        "claim_and_clear_waiter_locked(zone->write_wait_entry)",
        "zone write ACK validates before claiming waiter",
    )


def test_timeout_retirement_is_declared_and_covered_by_ktest() -> None:
    header = ZONE_HPP.read_text()
    ktest = WKI_WAIT_KTEST.read_text()
    token = "wki_zone_selftest_timeout_retirement_fences_stale_completion"

    if token not in header:
        fail(f"missing selftest declaration {token}")
    require_tokens(
        ktest,
        [
            "TimeoutRetiresZoneToFenceLateCompletion",
            token,
        ],
        "zone timeout retirement KTEST",
    )


def test_range_validation_is_declared_and_covered_by_ktest() -> None:
    header = ZONE_HPP.read_text()
    ktest = WKI_WAIT_KTEST.read_text()
    token = "wki_zone_selftest_range_and_read_response_validation"

    if token not in header:
        fail(f"missing selftest declaration {token}")
    require_tokens(
        ktest,
        [
            "RejectsWrappedRangesAndMismatchedReadResponses",
            token,
        ],
        "zone range validation KTEST",
    )


def test_operation_cookie_validation_is_declared_and_covered_by_ktest() -> None:
    header = ZONE_HPP.read_text()
    ktest = WKI_WAIT_KTEST.read_text()
    token = "wki_zone_selftest_waiter_slots_and_cookies"

    if token not in header:
        fail(f"missing selftest declaration {token}")
    require_tokens(
        ktest,
        [
            "RejectsStaleZoneOperationCookies",
            token,
        ],
        "zone operation cookie KTEST",
    )


def test_zone_create_backing_setup_is_bounded_out_of_rx_context() -> None:
    source = ZONE_CPP.read_text()
    header = ZONE_HPP.read_text()
    wki_source = WKI_CPP.read_text()
    request_rx = function_body(source, "handle_zone_create_req")
    ack_rx = function_body(source, "handle_zone_create_ack")
    request_worker = function_body(source, "process_zone_create_request")
    ack_worker = function_body(source, "process_zone_create_ack")
    zero_backing = function_body(source, "prepare_zone_backing")

    require_tokens(
        source,
        [
            "constexpr size_t WKI_ZONE_CREATE_WORK_CAPACITY = WKI_MAX_ZONES * 2",
            "std::array<ZoneCreateWork, WKI_ZONE_CREATE_WORK_CAPACITY>",
            "s_zone_create_work_lock.lock_irqsave()",
            "s_zone_create_work_pending.store(true, std::memory_order_release)",
        ],
        "fixed bounded zone-create work admission",
    )
    require_tokens(request_rx, ["enqueue_zone_create_work(work)"], "zone-create request RX admission")
    require_tokens(ack_rx, ["enqueue_zone_create_work(work)"], "zone-create ACK RX admission")
    reject_tokens(
        request_rx + ack_rx,
        ["prepare_zone_backing", "allocate_zone_backing", "memset(", "s_zone_table_lock.lock()", "wki_wait_for_op"],
        "zone-create RX allocation/blocking boundary",
    )

    require_order(
        request_worker,
        "s_zone_table_lock.unlock()",
        "prepare_zone_backing(SRC_NODE, request.size, true)",
        "responder backing allocation outside table lock",
    )
    require_order(
        ack_worker,
        "s_zone_table_lock.unlock()",
        "prepare_zone_backing(header.src_node, zone_size, ack_payload.rkey != 0)",
        "initiator backing allocation outside table lock",
    )
    require_tokens(zero_backing, ["allocate_zone_backing(size)", "std::memset(result.ptr, 0, size)"], "private page security clear")
    reject_tokens(source, ["wki_ivshmem_rdma_", "allocate_rdma_zone_backing", "allocate_roce_zone_backing",
                           "->rdma_register_region", "->rdma_write", "->rdma_read"], "no raw or peer BAR zone authority")
    require_tokens(header, ["void wki_zone_worker_start();"], "zone worker API")
    require_tokens(
        function_body(source, "wki_zone_worker_start"),
        ["task->slice_ns = WKI_ZONE_WORK_SLICE_NS", "mod::sched::set_task_nice(task, WKI_ZONE_WORK_NICE)"],
        "zone protocol-service worker priority",
    )
    require_tokens(
        function_body(wki_source, "wki_deferred_work_thread_start"),
        ["wki_zone_worker_start()"],
        "zone worker startup",
    )


def test_zone_authority_keeps_the_admitted_channel() -> None:
    source = ZONE_CPP.read_text()
    header = ZONE_HPP.read_text()
    core = WKI_CPP.read_text()
    peer = (ROOT / "modules/kern/src/net/wki/peer.cpp").read_text()
    require_tokens(header, ["WkiChannelIdentity channel_identity{}"], "retained zone channel")
    require_tokens(source, ["WkiChannelIdentity channel_identity{}"], "queued setup channel")
    require_tokens(function_body(source, "same_zone_channel"),
                   ["left.channel == right.channel", "left.peer_node_id == right.peer_node_id",
                    "left.channel_id == right.channel_id", "left.generation == right.generation"], "allocation identity")
    work = function_body(source, "process_zone_create_work")
    require_order(work, "lifecycle.acquire(work.header.src_node)", "process_zone_create_request(", "setup lifecycle before effects")
    require_order(work, "!zone_channel_live(work.channel_identity)", "process_zone_create_ack(", "retired ACK before waiter claim")
    create = function_body(source, "wki_zone_create")
    require_order(create, "lifecycle.try_acquire(peer)", "wki_channel_get(peer, WKI_CHAN_ZONE_MGMT)", "initiator capture lease")
    require_order(create, "zone->channel_identity = identity", "wki_send_on_channel_identity(identity,", "initiator retained identity")
    require_order(create, "lifecycle.release()", "wki_wait_for_op(&create_wait", "no ACK wait under lifecycle lease")
    for name in ["create_req", "create_ack", "destroy", "notify_pre", "notify_post", "read_req", "read_resp", "write_req", "write_ack"]:
        body = function_body(source, "handle_zone_" + name)
        require_tokens(body, ["!zone_channel_live(identity)", "identity.peer_node_id != hdr->src_node"], name + " ingress token")
        require_tokens(core, ["detail::handle_zone_" + name + "(hdr, payload, payload_len, RX_CHANNEL_IDENTITY)"], name + " dispatch identity")
        if name in ["create_req", "create_ack"]:
            require_order(body, "work.channel_identity = identity", "enqueue_zone_create_work(work)", name + " copied queue ownership")
        else:
            require_tokens(body, ["!same_zone_channel(zone->channel_identity, identity)"], name + " retained owner identity")
    require_tokens(function_body(source, "process_zone_create_ack"),
                   ["!same_zone_channel(zone->channel_identity, identity)"], "ACK matches original initiating allocation")
    reject_tokens(source, ["wki_send("], "zone sends cannot select numeric replacement channels")
    reset = function_body(core, "message_is_epoch_reset_sensitive")
    for name in ["CREATE_REQ", "CREATE_ACK", "DESTROY", "NOTIFY_PRE", "NOTIFY_POST", "READ_REQ", "READ_RESP", "WRITE_REQ", "WRITE_ACK"]:
        require_tokens(reset, ["MsgType::ZONE_" + name], "zone reset admission")
    cleanup = function_body(peer, "drain_pending_epoch_reset_cleanups")
    require_order(cleanup, "wki_dev_proxy_cleanup_epoch_reset_for_peer", "wki_zones_destroy_for_peer", "drain backing users first")
    require_order(cleanup, "wki_zones_destroy_for_peer", "peer.vfs_reset_rebind_pending.store(false", "retire zones before admission reopens")
    require_tokens(WKI_WAIT_KTEST.read_text(), ["DeferredSetupAndMessagesKeepOriginalChannel", "wki_zone_selftest_deferred_channel_retirement"], "actual lifecycle coverage")


def test_local_backing_uses_retained_grants() -> None:
    source = ZONE_CPP.read_text()
    server = (ROOT / "modules/kern/src/net/wki/dev_server.cpp").read_text()
    reject_tokens(source + ZONE_HPP.read_text(), ["wki_zone_get_ptr"], "no unpinned local backing API")
    reject_tokens(server, ["wki_zone_find(", "wki_zone_destroy(item.blk_zone_id)"], "server keeps exact zone ownership")
    create = function_body(source, "wki_zone_create")
    require_tokens(create, ["WkiZoneRef::retain(zone, peer, LOCAL_ACCESS)"], "create transfers original slot")
    require_tokens(server, ["while (!binding->retiring.load(std::memory_order_acquire) && binding->blk_zone_ref.live())", "retire_binding_zone(item.binding)"], "retirement admission and release")
    cleanup = function_body(server, "retire_binding_zone")
    require_order(cleanup, "binding->blk_zone_ptr = nullptr", "s_server_lock.unlock_irqrestore(FLAGS)", "withdraw diagnostic pointer before unlock")
    require_order(cleanup, "s_server_lock.unlock_irqrestore(FLAGS)", "zone_ref.reset()", "release backing outside registry lock")
    pending = function_body(server, "wki_dev_server_process_pending_zones")
    require_order(pending, "ring_hdr->server_ready = 1", "binding->blk_zone_ref = std::move(zone_ref)", "initialize before publication")
    require_tokens(WKI_WAIT_KTEST.read_text(), ["LocalBackingAndBlockRingRetainExactGrant", "wki_zone_selftest_local_backing_retention"], "actual retained grant coverage")


def test_device_notifications_keep_retained_zone_identity() -> None:
    source = ZONE_CPP.read_text()
    proxy = (ROOT / "modules/kern/src/net/wki/dev_proxy.cpp").read_text()
    server = (ROOT / "modules/kern/src/net/wki/dev_server.cpp").read_text()
    prepare = function_body(source, "prepare_zone_post_notification")
    require_tokens(prepare, ["prepared.channel = zone->channel_identity", "prepared.payload.zone_id = zone->zone_id",
                             "zone_channel_live(zone->channel_identity)", "op_type > 1"], "retained wake envelope")
    for text, name, call in [(proxy, "rdma_signal_server_unchecked", "state->rdma_zone_ref.notify_post(1)"),
                             (server, "blk_ring_signal_consumer_unchecked", "binding->blk_zone_ref.notify_post(0)")]:
        body = function_body(text, name)
        reject_tokens(body, ["wki_send(", "ZoneNotifyPayload"], "device wake cannot reselect numeric channels")
        require_tokens(body, [call], "wake through retained zone")
    require_tokens(function_body(server, "blk_ring_signal_consumer_unchecked"), ["!binding->blk_zone_ref.live()"], "raw completion admission")
    require_tokens(WKI_WAIT_KTEST.read_text(), ["NotificationsKeepOriginalGrantThroughRetirement"], "actual notification lifetime coverage")


def test_secure_zone_reservation_and_retirement_boundaries() -> None:
    source = ZONE_CPP.read_text()
    header = ZONE_HPP.read_text()
    require_tokens(header, ["uint64_t binding_generation = 0", "secure_descriptor{}", "bool secure_registered = false",
                           "bool memory_reserved = false", "bool secure_reserved = false"], "private secure zone authority")
    register = function_body(source, "register_zone_backing")
    require_tokens(register, ["zone->type_hint != ZoneTypeHint::BUFFER", "rdma_protocol::Service::ZONE",
                              "zone->binding_generation", "zone_secure_arm, zone_secure_effect, zone_secure_release"], "independent ZONE enrollment")
    for name in ["process_zone_create_request", "process_zone_create_ack"]:
        body = function_body(source, name)
        require_order(body, "setup.release()", "register_zone_backing(zone, backing)", "core registration outside setup lease")
        require_order(body, "register_zone_backing(zone, backing)", "publication.try_acquire(", "nonblocking publication revalidation")
    require_order(register, "s_zone_table_lock.unlock()", "wki_secure_rdma_register_region(", "core registration outside native gates")
    require_tokens(register, ["zone->secure_peer_compatible = true"], "task-captured format compatibility")
    arm = function_body(source, "zone_secure_arm")
    require_order(arm, "else if (zone->memory_reserved)", "zone->memory_reserved = true", "exclusive native reservation")
    require_tokens(arm, ["zone->state != ZoneState::ACTIVE", "zone_secure_request_valid_locked(zone, request)",
                         "retain_zone(zone)", "consumer_generation = zone->binding_generation"], "native arm and pin")
    validation = function_body(source, "zone_secure_request_valid_locked")
    require_tokens(validation, ["std::memcmp(&request.grant, &zone->secure_descriptor", "request.peer_node != zone->peer_node_id",
                                "request.operation_id == 0", "request.receive_ticket == 0", "request.intent_length != 0",
                                "request.length > zone->size - request.offset"], "complete consumer intent authority")
    effect = function_body(source, "zone_secure_effect")
    require_order(effect, "zone->lock.unlock()", "std::memcpy(", "native copy outside locks")
    require_tokens(effect, ["same_secure_request(zone->secure_request, request)", "backing.data() == zone->local_vaddr",
                           "input.size() == request.length", "output.size() == request.length"], "exact backing and coverage boundary")
    reclaim = function_body(source, "process_zone_retirement_work")
    require_order(reclaim, "s_zone_table_lock.unlock()", "wki_secure_rdma_unregister_region(descriptor)", "core reclaim outside native gates")
    require_tokens(reclaim, ["if (RESULT == WKI_OK)", "retry = true", "release_zone(&zone)"], "BUSY retains registration owner")
    release = function_body(source, "zone_secure_release")
    require_order(release, "zone->lock.unlock()", "release_zone(zone)", "release independent of native guard")
    for name in ["read_zone", "write_zone", "handle_zone_read_req", "handle_zone_write_req"]:
        require_tokens(function_body(source, name), ["zone->type_hint == ZoneTypeHint::MSG_QUEUE"], "generic ZONE cannot alter BLOCK rings")
    for name in ["handle_zone_read_req", "handle_zone_write_req"]:
        body = function_body(source, name)
        require_order(body, "reserve_zone_message_memory(zone)", "memcpy(", "message copy shares reservation")
        require_tokens(body, ["release_zone_message_memory(zone)", "WKI_ERR_BUSY"], "bounded BUSY before effect")
    require_tokens(function_body(source, "WkiZoneRef::retain"), ["zone->secure_registered ? nullptr : zone->local_vaddr"], "raw pointer refusal")
    require_tokens(function_body(source, "WkiZoneRef::snapshot_memory"), ["!zone_->secure_registered"], "snapshot pointer refusal")
    for name in ["read_zone", "write_zone"]:
        body = function_body(source, name)
        require_tokens(body, ["RESULT.outcome != WkiSecureRdmaOutcome::NO_EFFECT", "WKI_ERR_ZONE_UNCERTAIN"], "NO_EFFECT only fallback")


def test_secure_read_error_extension_is_exact_and_correlated() -> None:
    source = ZONE_CPP.read_text()
    read = function_body(source, "handle_zone_read_resp")
    require_tokens(read, ["payload_len == sizeof(ZoneReadRespPayload) + sizeof(int32_t)",
                         "zone->secure_peer_compatible", "response_status < 0",
                         "zone->read_expected_len != 0", "resp->offset == zone->read_expected_offset",
                         "resp->op_cookie == zone->read_expected_cookie", "if (!MATCHED_ERROR &&",
                         "resp->length == 0 && payload_len != sizeof(ZoneReadRespPayload)",
                         "if (!MATCHED_ERROR && zone->read_dest_buf != nullptr", "MATCHED_ERROR ? response_status : 0"], "bounded negative READ encoding")
    for name in ["handle_zone_notify_post", "handle_zone_read_req", "handle_zone_read_resp"]:
        reject_tokens(function_body(source, name), ["wki_secure_rdma_available("], "no fresh peer reader under exclusive ordinary RX")
    require_order(read, "bool const MATCHED_ERROR", "claim_and_clear_waiter_locked", "error validates before waiter claim")
    require_order(read, "claim_and_clear_waiter_locked", "if (!MATCHED_ERROR && zone->read_dest_buf", "no error data publication")


def test_exact_ref_transfers_never_select_successors() -> None:
    source = ZONE_CPP.read_text()
    acquire = function_body(source, "acquire_zone_slot")
    require_order(acquire, "s_zone_table_lock.lock()", "expected == nullptr || zone == expected", "allocation match under table gate")
    require_order(acquire, "expected == nullptr || zone == expected", "retain_zone(zone)", "exact selection before pin")
    for op in ["read", "write"]:
        require_tokens(function_body(source, "WkiZoneRef::" + op), [op + "_zone(zone_->zone_id, offset, buf, len, zone_)"], "exact retained ref")
        require_tokens(function_body(source, "wki_zone_" + op), [op + "_zone(zone_id, offset, buf, len, nullptr)"], "numeric API preserved")
        require_tokens(function_body(source, op + "_zone"), ["acquire_zone_slot(zone_id, expected)"], "exact transfer pin")
    ready = function_body(source, "WkiZoneRef::secure_ready")
    require_tokens(ready, ["if (!live()) return false", "rdma_protocol::PERMIT_READ | rdma_protocol::PERMIT_WRITE",
                          "wki_secure_rdma_available(zone_->channel_identity, rdma_protocol::Service::ZONE)",
                          "zone_remote_secure_grant(zone_, remote, PERMISSIONS) && live()"], "exact observational secure readiness")
    reject_tokens(ready, ["wki_zone_find", "acquire_zone_slot", "local_vaddr", "wki_secure_rdma_write", "wki_secure_rdma_read"],
                   "readiness cannot select successors, expose memory, or perform effects")


def test_shutdown_hands_native_owners_to_task_drain() -> None:
    source = ZONE_CPP.read_text()
    worker = function_body(source, "zone_create_worker_body")
    require_order(worker, "hold_kernel_shutdown()", "kernel_threads_shutdown_requested()", "zone shutdown hold")
    require_order(worker, "std::exchange(s_zone_create_worker_task, nullptr)", "WORKER->release()", "zone publication retirement")
    require_order(worker, "WORKER->release()", "s_zone_worker_stopped.store(true", "zone stopped publication")
    entry = function_body(source, "zone_create_worker_thread")
    require_order(entry, "zone_create_worker_body()", "exit_current_kernel_thread()", "zone clean worker exit")
    wake = function_body(source, "wake_zone_worker")
    require_order(wake, "retain_zone_worker_locked()", "s_zone_create_work_lock.unlock_irqrestore", "zone retained wake snapshot")
    require_order(wake, "s_zone_create_work_lock.unlock_irqrestore", "wake_retained_zone_worker", "zone wake outside guard")
    drain = function_body(source, "wki_zone_shutdown_drain")
    require_tokens(drain, ["s_zone_worker_stopped.load", "s_zone_worker_starting", "s_zone_create_worker_task", "process_zone_retirement_work()"], "zone drain ownership certificate")
    require_order(drain, "if (!STOPPED)", "process_zone_retirement_work()", "zone stopped owner before drain")
    require_tokens(ZONE_HPP.read_text(), ["wki_zone_shutdown_drain() -> bool"], "zone drain declaration")


def main() -> None:
    test_shutdown_hands_native_owners_to_task_drain()
    test_exact_ref_transfers_never_select_successors()
    test_secure_zone_reservation_and_retirement_boundaries()
    test_secure_read_error_extension_is_exact_and_correlated()
    test_device_notifications_keep_retained_zone_identity()
    test_local_backing_uses_retained_grants()
    test_zone_authority_keeps_the_admitted_channel()
    test_timeout_retirement_helper_marks_inactive_under_table_lock()
    test_timeout_cleanup_clears_pending_waiters()
    test_message_read_write_timeouts_retire_zone_before_release()
    test_late_completion_handlers_reject_retired_or_inactive_zones()
    test_zone_ranges_and_read_responses_are_validated_without_wrapping()
    test_timeout_retirement_is_declared_and_covered_by_ktest()
    test_range_validation_is_declared_and_covered_by_ktest()
    test_operation_cookie_validation_is_declared_and_covered_by_ktest()
    test_zone_create_backing_setup_is_bounded_out_of_rx_context()
    print("WKI zone source invariants hold")


if __name__ == "__main__":
    main()
