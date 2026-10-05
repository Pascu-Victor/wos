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
    match = re.search(rf"\b(?:auto|int|void)\s+{re.escape(name)}\s*\([^;{{]*{{", source)
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
        "spins < 100'000'000 && ker::mod::time::get_ms() < DEADLINE_MS",
        "static_cast<volatile VirtqUsed*>(ctrlq->used)->idx",
        "__atomic_thread_fence(__ATOMIC_ACQUIRE)",
        'net_log::warn("ctrl-queue MQ ack timeout")',
    )

    if "ker::mod::time::get_ms() + CTRL_ACK_TIMEOUT_MS" in send_body:
        fail("virtio-net control ACK wait must not use wrapping deadline arithmetic")


def test_used_ring_idx_is_acquired_before_empty_check() -> None:
    body = function_body(VIRTIO_CPP.read_text(), "virtq_get_buf")

    require_order(
        body,
        "uint16_t const USED_RING_IDX = static_cast<volatile VirtqUsed*>(vq->used)->idx",
        "__atomic_thread_fence(__ATOMIC_ACQUIRE)",
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
        "ker::net::napi_schedule(napi, ker::net::NapiScheduleSource::REARM)",
    )
    require_order(
        poll_body,
        "virtio_net_begin_poll(pair)",
        "pair->txq->lock.lock_irqsave()",
        "process_rx_budget_for(dev, pair->rxq, budget, source)",
        "virtio_net_complete_poll(dev, pair, napi, processed)",
    )
    require_order(
        irq_body,
        "pair->notification_lock.lock_irqsave()",
        "virtio_net_set_pair_notifications(pair, false)",
        "pair->notification_lock.unlock_irqrestore(FLAGS)",
        "ker::net::napi_schedule(&pair->napi, ker::net::NapiScheduleSource::IRQ)",
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
        "auto* vq = virtq_alloc(pci_dev, SIZE, kind)",
        "virtio_net_set_queue_notifications(vq, false)",
        "cfg->queue_desc = vq->ring.submission.address",
        "if (!publish_irqs(dev)) return fail_published_probe(dev)",
        "ker::net::napi_enable(&pair.napi",
        "pair.napi.worker != nullptr",
        "ker::net::napi_schedule(&pair.napi, ker::net::NapiScheduleSource::STARTUP)",
    )
    require_order(
        legacy_body,
        "pair0.rxq = virtq_alloc(pci_dev, rxq_size, QueueKind::receive, managed::LEGACY_QUEUE_MASK)",
        "virtio_net_set_queue_notifications(pair0.rxq, false)",
        "pair0.txq = virtq_alloc(pci_dev, txq_size, QueueKind::transmit, managed::LEGACY_QUEUE_MASK)",
        "virtio_net_set_queue_notifications(pair0.txq, false)",
        "if (!publish_irqs(dev)) return fail_published_probe(dev)",
        "ker::net::napi_enable(&pair.napi",
        "pair.napi.worker != nullptr",
        "ker::net::napi_schedule(&pair.napi, ker::net::NapiScheduleSource::STARTUP)",
    )
    if "VIRTIO_MSI_NO_VECTOR" not in unassign_body:
        fail("inactive MQ pairs must still be detached from MSI-X vectors")
    if "irq_lock" in header:
        fail("removed queue-selector hot-path lock must not remain in the device")


def test_rx_refill_growth_target_does_not_ratchet_from_free_snapshot() -> None:
    source = VIRTIO_NET_CPP.read_text()
    body = function_body(source, "fill_rx_queue_for")

    require_order(
        body,
        "if (rxq->num_free > 0)",
        "static_cast<size_t>(rxq->num_free) + ker::net::PKT_POOL_RX_REFILL_RESERVE",
        "ker::net::pkt_pool_ensure_free(TARGET_FREE)",
        "size_t const BEFORE_FREE = ker::net::pkt_pool_free_count()",
        "ker::net::pkt_alloc_rx()",
    )
    if "std::max(TARGET_FREE" in body or re.search(
        r"\b(?:TARGET_FREE|target_free)\s*=\s*ker::net::pkt_pool_free_count\(\)", body
    ):
        fail("RX refill target must not inherit a racy packet-pool free-count snapshot")
    for entry in ("init_device_modern", "init_device"):
        require_order(function_body(source, entry), "pkt_pool_reserve_for_rx_descriptors", "fill_rx_queue_for")


def test_mac_admission_precedes_any_legacy_port_or_state_write() -> None:
    source = VIRTIO_NET_CPP.read_text()
    check = function_body(source, "virtio_net_check_mac")
    require_order(check, "netdev == nullptr || mac == nullptr", "dev == nullptr", "CallbackGuard", "!GUARD", "std::equal", "dev->modern_cfg != nullptr",
                  "dev->io_base == 0", "dev->negotiated_features & VIRTIO_NET_F_MAC", "return -ENOTSUP", "return 0")
    if "outb" in check or "memcpy" in check or re.search(r"mac\.at\([^)]*\)\s*=", check):
        fail("MAC admission must not write hardware or local state")
    commit = function_body(source, "virtio_net_set_mac")
    require_order(commit, "virtio_net_check_mac(netdev, mac)", "std::equal", "return;", "::outb", "netdev->mac.at(i) = mac[i]")
    if ".check_mac = virtio_net_check_mac" not in source:
        fail("virtio MAC admission must be exposed through NetDeviceOps")
    core = (ROOT / "modules/kern/src/net/netdevice.cpp").read_text()
    admission = function_body(core, "netdev_check_mac")
    require_order(admission, "std::equal", "dev->ops->set_mac == nullptr", "dev->ops->check_mac == nullptr", "return -ENOTSUP",
                  "dev->ops->check_mac(dev, mac)")
    for path in ["net/loopback.cpp", "dev/e1000e/e1000e.cpp", "dev/ivshmem/ivshmem_net.cpp", "dev/usb/cdc_ether.cpp"]:
        if ".check_mac = ker::net::netdev_check_fixed_mac" not in (ROOT / "modules/kern/src" / path).read_text():
            fail(f"fixed-address driver must explicitly refuse MAC changes: {path}")


def test_managed_probe_owns_exposed_storage_until_reset() -> None:
    source = VIRTIO_NET_CPP.read_text()
    modern = function_body(source, "init_device_modern")
    legacy = function_body(source, "init_device")
    unwind = function_body(source, "fail_probe")
    require_order(modern, "prepare_probe(dev)", "virtq_alloc(pci_dev, SIZE, kind)",
                  "vq->ring.expose()", "cfg->queue_desc = vq->ring.submission.address",
                  "dev->control_buffer.prepare(api)", "pci::pci_set_bus_master(pci_dev, true)",
                  "if (!publish_irqs(dev)) return fail_published_probe(dev)")
    require_order(legacy, "MODERN != -ENOTSUP", "prepare_probe(dev)",
                  "managed::LEGACY_QUEUE_MASK", "pair0.rxq->ring.expose()",
                  "pair0.txq->ring.expose()", "pci::pci_set_bus_master(pci_dev, true)",
                  "if (!publish_irqs(dev)) return fail_published_probe(dev)")
    require_order(unwind, "dev->callbacks_published", "reset_transport(dev)",
                  "pci::pci_set_bus_master(dev->pci, false)", "reset && DISABLED",
                  "if (MASKED) drain_interrupts(dev)", "retire_regions(dev, QUIESCED)",
                  "if (released)", "delete dev", "failed_probes = dev")
    require_order(function_body(source, "publish_irqs"), "dev->netdev.private_data = dev", "dev->callbacks_published = true",
                  "ker::net::napi_init", "ker::mod::gates::request_irq", "pair.irq_owned = true")
    require_order(function_body(source, "retire_regions"), "virtq_retire(pair.rxq, quiesced)",
                  "virtq_retire(pair.txq, quiesced)", "virtq_retire(dev->ctrlq, quiesced)", "dev->control_buffer.retire(api, quiesced)")
    ctrl = function_body(source, "send_mq_ctrl_cmd")
    if "page_alloc" in ctrl or "virt_to_phys" in ctrl or "dma::release" in ctrl:
        fail("MQ command storage must remain managed through late completion/reset")
    require_order(ctrl, "control_buffer.expose()", "control_buffer.submission.address", "ctrlq->avail->idx = 1")
    for body in (modern, legacy):
        if "delete dev" in body or "virt_to_phys(" in body:
            fail("probe must not discard an exposed owner or program raw queue addresses")


def test_packet_dma_is_preallocated_and_completed_before_reuse() -> None:
    queue = VIRTIO_CPP.read_text()
    driver = VIRTIO_NET_CPP.read_text()
    allocate = function_body(queue, "virtq_alloc")
    submit = function_body(queue, "virtq_add_buf")
    complete = function_body(queue, "virtq_get_buf")
    require_order(allocate, "vq->ring.allocate", "dma::allocate_streaming", "vq->ring.prepare")
    require_order(submit, "dma::bind_streaming", "dma::prepare", "slot.submission = SUBMISSION.submission",
                  "vq->desc[IDX].addr = slot.submission.address", "__atomic_thread_fence(__ATOMIC_RELEASE)", "vq->avail->idx =")
    require_order(complete, "static_cast<volatile VirtqUsed*>(vq->used)->idx", "__atomic_thread_fence(__ATOMIC_ACQUIRE)",
                  "PENDING > vq->size - vq->num_free", "DESC_ID >= vq->size", "LENGTH > slot.submission.size",
                  "dma::complete", "slot.submission = {}", "++vq->last_used_idx", "vq->free_head = IDX")
    if "virt_to_phys" in driver or "page_alloc" in driver:
        fail("virtio-net must obtain every DMA address from the managed API")
    for body in (submit, complete):
        if "allocate_streaming" in body or "dma::release" in body:
            fail("packet completion/submission must reuse preallocated DMA mappings")
    require_order(function_body(driver, "virtio_net_set_queue_notifications"), "vq->failed.load", "__atomic_store_n")
    require_order(function_body(driver, "init_device_modern"), "VIRTIO_NET_HDR_SIZE_MODERN", "virtq_alloc")
    require_order(function_body(driver, "init_device"), "!virtio_net_mrg_rxbuf_disabled()", "virtio_net_hdr_size_for_features(our_features)")


def test_terminal_callbacks_reject_before_touching_queue_or_transport() -> None:
    source = VIRTIO_NET_CPP.read_text()
    checks = {
        "virtio_net_poll": "queue_pair_for_napi(dev, napi)",
        "virtio_net_irq": "!dev->msix_enabled",
        "virtio_net_start_xmit": "active_tx_pair_count(dev)",
        "send_mq_ctrl_cmd": "dev->ctrlq == nullptr",
        "virtio_net_open": "netdev->state = 1",
        "virtio_net_set_queue_cpu": "pair_idx >= dev->num_queue_pairs",
        "virtio_net_set_queue_cpu_mask": "return apply_queue_cpu_mask",
        "virtio_net_check_mac": "std::equal",
        "virtio_net_set_mac": "virtio_net_check_mac(netdev, mac)",
        "virtio_net_diag_snapshot": "dev->configured_queue_pairs",
    }
    for name, access in checks.items():
        require_order(function_body(source, name), "CallbackGuard const GUARD", "!GUARD", access)
    retire = function_body(source, "retire_device")
    require_order(retire, "compare_exchange_strong", "lifecycle::retire(backend)", "dev->retirement.store")
    if "delete dev" in retire:
        fail("published NAPI/WKI references require stable terminal device storage")
    shutdown = function_body(source, "virtio_net_shutdown")
    require_order(shutdown, "can_wait_for_reclaim", "shutdown_started.store", "retire_device(devices[i])")
    require_order(function_body(source, "init_device"), "shutdown_started.load", "devices[i]->admission.closed()", "init_device_modern")
    require_order(function_body(source, "drain_interrupts"), "!pair.irq_owned", "free_irq(pair.irq_vector)", "pair.irq_owned = false")


def test_callback_exit_and_worker_storage_ownership() -> None:
    source = VIRTIO_NET_CPP.read_text()
    guard = source[source.index("class CallbackGuard"):source.index("constexpr size_t MAX_VIRTIO_NET_DEVICES")]
    require_order(guard, "KernelShutdownHold shutdown_", "lifecycle::Guard callback_")
    require_order(function_body(source, "retire_device"), "hold_kernel_shutdown()", "compare_exchange_strong")
    require_order(function_body(source, "selftest_live_runner"), "request_kernel_threads_shutdown(1000)",
                  "protected_yields.load", "release_holder.store(true")
    napi = (ROOT / "modules/kern/src/net/netpoll.cpp").read_text()
    enable = function_body(napi, "napi_enable")
    require_order(enable, "napi->worker != nullptr", "create_kernel_thread", "napi->worker->try_acquire()", "post_task_pinned_cpu")
    require_order(function_body(napi, "find_napi_for_current_task"), "g_registry_lock.lock_irqsave()",
                  "for (auto* napi : g_napi_registry)", "g_registry_lock.unlock_irqrestore(FLAGS)")


def test_platform_negotiation_gates_bus_mastering_and_reset_revokes_it() -> None:
    source = VIRTIO_NET_CPP.read_text()
    modern = function_body(source, "init_device_modern")
    require_order(modern, "prepare_probe(dev)", "platform_features(cfg->device_feature, TRANSLATED)",
                  "cfg->driver_feature = *UPPER", "if ((cfg->device_status & VIRTIO_STATUS_FEATURES_OK) == 0)",
                  "iommu::set_platform_dma_ready(pci_dev, PLATFORM)", "setup_queue", "pci::pci_set_bus_master(pci_dev, true)")
    reset = function_body(source, "reset_transport")
    require_order(reset, "iommu::set_platform_dma_ready(dev->pci, false)", "managed::reset")
    init = function_body(source, "init_device")
    require_order(init, "MODERN != -ENOTSUP", "iommu::DomainMode::translated", "ACCESS_PLATFORM unavailable", "auto io_base")


def main() -> None:
    test_platform_negotiation_gates_bus_mastering_and_reset_revokes_it()
    test_callback_exit_and_worker_storage_ownership()
    test_terminal_callbacks_reject_before_touching_queue_or_transport()
    test_packet_dma_is_preallocated_and_completed_before_reuse()
    test_managed_probe_owns_exposed_storage_until_reset()
    test_mac_admission_precedes_any_legacy_port_or_state_write()
    test_ctrl_ack_deadline_is_saturating()
    test_used_ring_idx_is_acquired_before_empty_check()
    test_split_ring_notification_rearm_is_race_safe_and_bounded()
    test_queues_start_suppressed_and_napi_precedes_irq_registration()
    test_rx_refill_growth_target_does_not_ratchet_from_free_snapshot()
    print("virtio-net polling guards are source covered")


if __name__ == "__main__":
    main()
