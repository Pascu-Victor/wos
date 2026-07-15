#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
VIRTIO_NET_CPP = ROOT / "modules" / "kern" / "src" / "dev" / "virtio" / "virtio_net.cpp"
VIRTIO_NET_HPP = ROOT / "modules" / "kern" / "src" / "dev" / "virtio" / "virtio_net.hpp"
VIRTIO_CPP = ROOT / "modules" / "kern" / "src" / "dev" / "virtio" / "virtio.cpp"
VIRTIO_HPP = ROOT / "modules" / "kern" / "src" / "dev" / "virtio" / "virtio.hpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\b(?:auto|int|void)\s+{re.escape(name)}\s*\(", source)
    if match is None:
        fail(f"{name} function not found")
    start = match.start()
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


def test_split_ring_notification_rearm_is_race_safe_and_bounded() -> None:
    source = VIRTIO_NET_CPP.read_text()
    header = VIRTIO_NET_HPP.read_text()
    virtio_header = VIRTIO_HPP.read_text()
    rearm_body = function_body(source, "virtio_net_rearm_pair_notifications_locked")
    queue_toggle_body = function_body(source, "virtio_net_set_queue_notifications")
    pair_toggle_body = function_body(source, "virtio_net_set_pair_notifications")
    begin_body = function_body(source, "virtio_net_begin_poll")
    complete_body = function_body(source, "virtio_net_complete_poll")
    pending_body = function_body(source, "virtio_net_pair_pending")
    bounded_body = function_body(source, "should_reschedule_after_notification_rearm")
    poll_body = function_body(source, "virtio_net_poll")
    irq_body = function_body(source, "virtio_net_irq")

    if "constexpr uint16_t VIRTQ_AVAIL_F_NO_INTERRUPT = 1" not in virtio_header:
        fail("split-ring notification suppression flag is missing")

    require_order(
        queue_toggle_body,
        "reinterpret_cast<uint16_t*>(vq->avail)",
        "enabled ? uint16_t{0} : VIRTQ_AVAIL_F_NO_INTERRUPT",
        "__atomic_store_n(flags, VALUE, __ATOMIC_RELEASE)",
    )
    require_order(
        pair_toggle_body,
        "virtio_net_set_queue_notifications(pair->rxq, enabled)",
        "virtio_net_set_queue_notifications(pair->txq, enabled)",
    )

    require_order(
        rearm_body,
        "virtio_net_set_pair_notifications(pair, true)",
        "__atomic_thread_fence(__ATOMIC_SEQ_CST)",
        "should_reschedule_after_notification_rearm(dev, pair, rx_processed)",
        "pair->napi.state.load(std::memory_order_acquire)",
        "if (SHOULD_RESCHEDULE || !NAPI_IDLE)",
        "virtio_net_set_pair_notifications(pair, false)",
    )
    require_order(
        pending_body,
        "VirtIONetPending pending{.rx = virtq_has_pending(pair->rxq)}",
        "pair->txq->lock.lock_irqsave()",
        "pending.tx = virtq_has_pending(pair->txq)",
        "pair->txq->lock.unlock_irqrestore(TX_FLAGS)",
        "dev->configured_queue_pairs > dev->num_queue_pairs",
    )
    require_order(
        begin_body,
        "pair->notification_lock.lock_irqsave()",
        "virtio_net_set_pair_notifications(pair, false)",
        "pair->notification_lock.unlock_irqrestore(FLAGS)",
    )
    require_order(
        complete_body,
        "pair->notification_lock.lock_irqsave()",
        "ker::net::napi_complete(napi)",
        "virtio_net_rearm_pair_notifications_locked(dev, pair, rx_processed)",
        "pair->notification_lock.unlock_irqrestore(FLAGS)",
        "ker::net::napi_schedule(napi)",
    )
    require_order(
        poll_body,
        "virtio_net_begin_poll(pair)",
        "pair->txq->lock.lock_irqsave()",
        "process_rx_budget_for(dev, pair->rxq, budget)",
        "virtio_net_complete_poll(dev, pair, napi, processed)",
    )
    require_order(
        irq_body,
        "pair->notification_lock.lock_irqsave()",
        "virtio_net_set_pair_notifications(pair, false)",
        "pair->notification_lock.unlock_irqrestore(FLAGS)",
        "ker::net::napi_schedule(&pair->napi)",
    )
    if "queue_msix_vector" in poll_body or "VIRTIO_MSI_QUEUE_VECTOR" in poll_body:
        fail("NAPI poll must not rewrite queue-vector selectors")
    if "queue_msix_vector" in irq_body or "VIRTIO_MSI_QUEUE_VECTOR" in irq_body:
        fail("IRQ hot path must not rewrite queue-vector selectors")

    require_order(
        bounded_body,
        "VirtIONetPending const PENDING = virtio_net_pair_pending(dev, pair)",
        "if (rx_processed != 0)",
        "pair->rx_empty_pending_rearms = 0",
        "if (PENDING.tx)",
        "return true",
        "if (!PENDING.rx)",
        "pair->rx_empty_pending_rearms = 0",
        "if (pair->rx_empty_pending_rearms < RX_EMPTY_PENDING_REARM_LIMIT)",
        "pair->rx_empty_pending_rearms++",
        "return true",
        "return false",
    )

    if "constexpr uint8_t RX_EMPTY_PENDING_REARM_LIMIT = 1" not in source:
        fail("empty pending self-rearm must remain bounded")
    if "uint8_t rx_empty_pending_rearms{}" not in header:
        fail("virtio queue-pair must persist empty pending rearm state")
    if "ker::mod::sys::Spinlock notification_lock" not in header:
        fail("virtio queue-pair must serialize IRQ/NAPI notification handoff")


def test_queues_start_suppressed_and_napi_precedes_irq_registration() -> None:
    source = VIRTIO_NET_CPP.read_text()
    header = VIRTIO_NET_HPP.read_text()
    modern_body = function_body(source, "init_device_modern")
    legacy_body = function_body(source, "init_device")
    unassign_body = function_body(source, "virtio_net_unassign_pair_vectors")

    require_order(
        modern_body,
        "auto* vq = virtq_alloc(SIZE)",
        "virtio_net_set_queue_notifications(vq, false)",
        "cfg->queue_desc = virt_to_phys(vq->desc)",
        "ker::net::napi_init(&pair.napi, &dev->netdev, virtio_net_poll, 64)",
        "ker::mod::gates::request_irq",
        "ker::net::napi_enable(&pair.napi",
        "pair.napi.worker != nullptr",
        "ker::net::napi_schedule(&pair.napi)",
    )
    require_order(
        legacy_body,
        "pair0.rxq = virtq_alloc(rxq_size)",
        "virtio_net_set_queue_notifications(pair0.rxq, false)",
        "pair0.txq = virtq_alloc(txq_size)",
        "virtio_net_set_queue_notifications(pair0.txq, false)",
        "ker::net::napi_init(&pair.napi, &dev->netdev, virtio_net_poll, 64)",
        "ker::mod::gates::request_irq",
        "ker::net::napi_enable(&pair.napi",
        "pair.napi.worker != nullptr",
        "ker::net::napi_schedule(&pair.napi)",
    )
    if "VIRTIO_MSI_NO_VECTOR" not in unassign_body:
        fail("inactive MQ pairs must still be detached from MSI-X vectors")
    if "irq_lock" in header:
        fail("removed queue-selector hot-path lock must not remain in the device")


def main() -> None:
    test_ctrl_ack_deadline_is_saturating()
    test_used_ring_idx_is_acquired_before_empty_check()
    test_split_ring_notification_rearm_is_race_safe_and_bounded()
    test_queues_start_suppressed_and_napi_precedes_irq_registration()
    print("virtio-net polling guards are source covered")


if __name__ == "__main__":
    main()
