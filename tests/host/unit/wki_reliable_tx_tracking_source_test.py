#!/usr/bin/env python3
"""Source invariants for generation-qualified reliable TX acknowledgement."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WKI_DIR = ROOT / "modules" / "kern" / "src" / "net" / "wki"
WKI_HPP = WKI_DIR / "wki.hpp"
WKI_CPP = WKI_DIR / "wki.cpp"


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
