#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
VIRTIO_NET_CPP = ROOT / "modules" / "kern" / "src" / "dev" / "virtio" / "virtio_net.cpp"
VIRTIO_NET_HPP = ROOT / "modules" / "kern" / "src" / "dev" / "virtio" / "virtio_net.hpp"
VIRTIO_CPP = ROOT / "modules" / "kern" / "src" / "dev" / "virtio" / "virtio.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    start = source.find(f"auto {name}(")
    if start < 0:
        fail(f"{name} function not found")
    brace = source.find("{", start)
    if brace < 0:
        fail(f"{name} function body not found")
    depth = 0
    for pos in range(brace, len(source)):
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
            if depth == 0:
                return source[brace + 1 : pos]
    fail(f"{name} function body is unterminated")


def require_order(source: str, *snippets: str) -> None:
    cursor = -1
    for snippet in snippets:
        pos = source.find(snippet, cursor + 1)
        if pos < 0:
            fail(f"missing ordered snippet: {snippet}")
        cursor = pos


def test_ctrl_ack_deadline_is_saturating() -> None:
    source = VIRTIO_NET_CPP.read_text()
    helper_body = function_body(source, "future_deadline_ms")
    send_body = function_body(source, "send_mq_ctrl_cmd")

    require_order(
        helper_body,
        "uint64_t const NOW_MS = ker::mod::time::get_ms()",
        "if (UINT64_MAX - NOW_MS < timeout_ms)",
        "return UINT64_MAX",
        "return NOW_MS + timeout_ms",
    )
    require_order(
        send_body,
        "constexpr uint64_t CTRL_ACK_TIMEOUT_MS = 5000",
        "uint64_t const DEADLINE_MS = future_deadline_ms(CTRL_ACK_TIMEOUT_MS)",
        "while (ker::mod::time::get_ms() < DEADLINE_MS)",
        "__atomic_thread_fence(__ATOMIC_ACQUIRE)",
        'net_log::warn("ctrl-queue MQ ack timeout")',
    )

    if "ker::mod::time::get_ms() + CTRL_ACK_TIMEOUT_MS" in send_body:
        fail("virtio-net control ACK wait must not use wrapping deadline arithmetic")


def test_used_ring_idx_is_acquired_before_empty_check() -> None:
    body = function_body(VIRTIO_CPP.read_text(), "virtq_get_buf")

    require_order(
        body,
        "__atomic_thread_fence(__ATOMIC_ACQUIRE)",
        "uint16_t const USED_RING_IDX = vq->used->idx",
        "if (vq->last_used_idx == USED_RING_IDX)",
        "uint16_t const USED_IDX = vq->last_used_idx % vq->size",
    )


def test_rx_pending_self_rearm_is_bounded_without_progress() -> None:
    source = VIRTIO_NET_CPP.read_text()
    header = VIRTIO_NET_HPP.read_text()
    helper_body = function_body(source, "should_rearm_rx_after_complete")

    require_order(
        helper_body,
        "bool const HAS_PENDING = rx_pending_after_irq_enable(dev, pair)",
        "if (!HAS_PENDING)",
        "pair->rx_empty_pending_rearms = 0",
        "if (processed != 0)",
        "pair->rx_empty_pending_rearms = 0",
        "if (pair->rx_empty_pending_rearms < RX_EMPTY_PENDING_REARM_LIMIT)",
        "pair->rx_empty_pending_rearms++",
        "return true",
        "return false",
    )

    require_order(
        source,
        "constexpr uint8_t RX_EMPTY_PENDING_REARM_LIMIT = 1",
        "if (should_rearm_rx_after_complete(dev, pair, processed))",
        "virtio_net_irq_disable_pair(dev, pair->index)",
        "ker::net::napi_schedule(napi)",
    )
    if "uint8_t rx_empty_pending_rearms{}" not in header:
        fail("virtio queue-pair must persist empty pending rearm state")


def main() -> None:
    test_ctrl_ack_deadline_is_saturating()
    test_used_ring_idx_is_acquired_before_empty_check()
    test_rx_pending_self_rearm_is_bounded_without_progress()
    print("virtio-net polling guards are source covered")


if __name__ == "__main__":
    main()
