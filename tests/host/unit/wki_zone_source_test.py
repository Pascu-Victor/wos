#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
ZONE_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "zone.cpp"
ZONE_HPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "zone.hpp"
WKI_WAIT_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "wki_wait_ktest.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:void|auto)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>,\s*&]+)?\s*\{{",
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
    read_body = function_body(source, "wki_zone_read")
    write_body = function_body(source, "wki_zone_write")

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

    read_body = function_body(source, "wki_zone_read")
    write_body = function_body(source, "wki_zone_write")
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
        "if (resp->length > payload_len - sizeof(ZoneReadRespPayload) || !zone_read_response_matches_pending(*zone, *resp))",
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


def main() -> None:
    test_timeout_retirement_helper_marks_inactive_under_table_lock()
    test_timeout_cleanup_clears_pending_waiters()
    test_message_read_write_timeouts_retire_zone_before_release()
    test_late_completion_handlers_reject_retired_or_inactive_zones()
    test_zone_ranges_and_read_responses_are_validated_without_wrapping()
    test_timeout_retirement_is_declared_and_covered_by_ktest()
    test_range_validation_is_declared_and_covered_by_ktest()
    test_operation_cookie_validation_is_declared_and_covered_by_ktest()
    print("WKI zone source invariants hold")


if __name__ == "__main__":
    main()
