#!/usr/bin/env python3
"""Source invariants for generation-qualified reliable TX acknowledgement."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WKI_DIR = ROOT / "modules" / "kern" / "src" / "net" / "wki"
WKI_HPP = WKI_DIR / "wki.hpp"
WKI_CPP = WKI_DIR / "wki.cpp"
WKI_CHANNEL_KTEST = ROOT / "modules" / "kern" / "src" / "test" / "wki_channel_ktest.cpp"


def function_body(source: str, name: str) -> str:
    start = source.find(f"{name}(")
    assert start >= 0, f"missing function {name}"
    brace = source.find("{", start)
    assert brace >= 0, f"missing body for {name}"
    depth = 0
    for index in range(brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[brace : index + 1]
    raise AssertionError(f"unterminated body for {name}")


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    assert not missing, f"{context}: missing {', '.join(missing)}"


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        position = source.find(token, cursor)
        assert position >= 0, f"{context}: missing ordered token {token}"
        cursor = position + len(token)


def test_tracking_api_carries_exact_channel_allocation_and_sequence() -> None:
    header = WKI_HPP.read_text()
    require_tokens(
        header,
        [
            "struct WkiReliableTxToken",
            "WkiChannelIdentity channel_identity = {};",
            "uint32_t sequence = 0;",
            "enum class WkiReliableTxStatus : uint8_t",
            "INVALID = 0",
            "PENDING = 1",
            "ACKED = 2",
            "RETIRED = 3",
            "auto wki_send_tracked(",
            "auto wki_reliable_tx_status(const WkiReliableTxToken& token) -> WkiReliableTxStatus;",
        ],
        "public reliable TX tracking API",
    )


def test_tracked_send_holds_peer_lifecycle_and_rejects_rebind_window() -> None:
    source = WKI_CPP.read_text()
    tracked = function_body(source, "wki_send_tracked")
    require_order(
        tracked,
        [
            "*token_out = {}",
            "wki_peer_lifecycle_acquire(peer)",
            "peer->state == PeerState::CONNECTED",
            "!peer->vfs_reset_rebind_pending.load(std::memory_order_acquire)",
            "wki_send_impl(",
            "wki_peer_lifecycle_release(peer)",
        ],
        "tracked task-context send lifecycle",
    )


def test_token_is_published_only_after_retransmit_recovery_exists() -> None:
    source = WKI_CPP.read_text()
    send_impl = function_body(source, "wki_send_impl")
    require_tokens(
        send_impl,
        [
            "uint16_t const FRAME_LEN = WKI_HEADER_SIZE + wire_payload_len",
            "perf_record_transport_begin(dst_node, channel_id, TRACE_CORRELATION, wire_payload_len",
            "hdr->payload_len = wire_payload_len",
            "perf_record_transport_end(dst_node, channel_id, TRACE_CORRELATION, WKI_ERR_NO_MEM, wire_payload_len",
            "ch->bytes_sent += wire_payload_len",
        ],
        "split send combined-length consumers",
    )
    require_order(
        send_impl,
        [
            "rt_entry->seq = ch->tx_seq",
            "ch->retransmit_tail = rt_entry",
            "ch->tx_seq++",
            "*tx_token_out =",
            ".channel = ch",
            ".generation = ch->generation",
            ".sequence = TRACE_CORRELATION",
            "ch->lock.unlock()",
            "return WKI_OK",
        ],
        "recoverable publication and token capture",
    )

    ordinary = function_body(source, "wki_send")
    identity = function_body(source, "wki_send_on_channel_identity")
    require_tokens(ordinary, ["nullptr, 0, nullptr"], "ordinary send remains untracked")
    require_tokens(identity, ["identity.generation", "nullptr"], "identity send remains untracked")


def test_heap_retransmit_storage_is_prepared_once_outside_channel_lock() -> None:
    source = WKI_CPP.read_text()
    send_impl = function_body(source, "wki_send_impl")
    require_order(
        send_impl,
        [
            "for (;;)",
            "ch->generation != heap_rt_channel_generation",
            "ch->tx_credits == 0",
            "heap_rt_channel_generation = ch->generation",
            "ch->lock.unlock()",
            "heap_rt_entry = wki_retransmit_entry_alloc(FRAME_LEN)",
            "ch->lock.lock()",
            "rt_data = heap_rt_entry->data",
            "memcpy(rt_data, frame, FRAME_LEN)",
        ],
        "single-allocation retransmit preparation and revalidation",
    )
    assert "new (std::nothrow) WkiRetransmitEntry" not in send_impl
    assert "new (std::nothrow) uint8_t[FRAME_LEN]" not in send_impl


def test_status_is_generation_qualified_and_uses_wrap_safe_cumulative_ack() -> None:
    source = WKI_CPP.read_text()
    status = function_body(source, "wki_reliable_tx_status")
    require_order(
        status,
        [
            "CHANNEL->lock.lock()",
            "WkiReliableTxStatus status = WkiReliableTxStatus::RETIRED",
            "CHANNEL->active",
            "CHANNEL->peer_node_id == identity.peer_node_id",
            "CHANNEL->channel_id == identity.channel_id",
            "CHANNEL->generation == identity.generation",
            "seq_after(CHANNEL->tx_seq, token.sequence)",
            "seq_after(CHANNEL->tx_ack, token.sequence)",
            "status = WkiReliableTxStatus::ACKED",
            "CHANNEL->lock.unlock()",
        ],
        "tracked TX status",
    )
    assert "CHANNEL->tx_ack > token.sequence" not in status
    assert "CHANNEL->tx_ack >= token.sequence" not in status


def test_split_send_validates_and_flattens_before_reliable_publication() -> None:
    header = WKI_HPP.read_text()
    source = WKI_CPP.read_text()
    ktest = WKI_CHANNEL_KTEST.read_text()
    require_tokens(
        header,
        [
            "auto wki_send_on_channel_identity_split(",
            "const void* payload_tail",
            "uint16_t payload_tail_len",
            "wki_selftest_split_payload_validation_and_copy()",
        ],
        "split reliable send API",
    )

    validate = function_body(source, "wki_payload_segments_total_len")
    require_order(
        validate,
        [
            "payload_len != 0 && payload == nullptr",
            "payload_tail_len != 0 && payload_tail == nullptr",
            "static_cast<size_t>(payload_len) + payload_tail_len",
            "TOTAL_LEN > WKI_ETH_MAX_PAYLOAD",
            "*total_len_out = static_cast<uint16_t>(TOTAL_LEN)",
        ],
        "split payload validation",
    )

    copy = function_body(source, "copy_wki_payload_segments")
    require_order(
        copy,
        [
            "memcpy(dest, payload, payload_len)",
            "memcpy(dest + payload_len, payload_tail, payload_tail_len)",
        ],
        "split payload flattening order",
    )

    send_impl = function_body(source, "wki_send_impl")
    require_order(
        send_impl,
        [
            "wki_payload_segments_total_len(",
            "net::pkt_alloc_tx()",
            "copy_wki_payload_segments(frame + WKI_HEADER_SIZE",
            "wki_frame_checksum(*hdr, frame + WKI_HEADER_SIZE)",
            "memcpy(rt_data, frame, FRAME_LEN)",
            "ch->tx_seq++",
        ],
        "split send synchronous ownership",
    )

    split = function_body(source, "wki_send_on_channel_identity_split")
    require_order(
        split,
        [
            "identity.channel == nullptr",
            "identity.generation == 0",
            "wki_send_impl(",
            "identity.channel",
            "identity.generation",
            "nullptr, payload_tail, payload_tail_len",
        ],
        "exact-channel split send",
    )
    require_tokens(
        ktest,
        [
            "KTEST(WkiSend, SplitPayloadValidatesAndFlattensInOrder)",
            "wki_selftest_split_payload_validation_and_copy()",
        ],
        "split payload KTEST",
    )


if __name__ == "__main__":
    test_tracking_api_carries_exact_channel_allocation_and_sequence()
    test_tracked_send_holds_peer_lifecycle_and_rejects_rebind_window()
    test_token_is_published_only_after_retransmit_recovery_exists()
    test_heap_retransmit_storage_is_prepared_once_outside_channel_lock()
    test_status_is_generation_qualified_and_uses_wrap_safe_cumulative_ack()
    test_split_send_validates_and_flattens_before_reliable_publication()
    print("WKI reliable TX tracking source invariants hold")
