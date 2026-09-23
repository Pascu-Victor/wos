#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
XHCI_CPP = ROOT / "modules" / "kern" / "src" / "dev" / "usb" / "xhci.cpp"
CDC_CPP = ROOT / "modules" / "kern" / "src" / "dev" / "usb" / "cdc_ether.cpp"
GATES_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "interrupt" / "gates.cpp"
NETDEVICE_CPP = ROOT / "modules" / "kern" / "src" / "net" / "netdevice.cpp"
WKI_DEV_SERVER_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "dev_server.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:\[\[noreturn\]\]\s*)?(?:auto|void|int)\s+{name}\([^)]*\)\s*(?:->\s*[^{{]+)?\s*\{{",
        source,
        flags=re.DOTALL,
    )
    if match is None:
        fail(f"missing function {name}")

    depth = 1
    pos = match.end()
    while pos < len(source) and depth:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth:
        fail(f"unterminated function {name}")
    return source[match.end() : pos - 1]


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        position = source.find(token, cursor)
        if position < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = position + len(token)


def test_irq_is_bounded_and_only_publishes() -> None:
    source = XHCI_CPP.read_text()
    irq = function_body(source, "xhci_irq")
    events = function_body(source, "process_events")
    event = function_body(source, "process_event")

    require_order(
        irq,
        [
            "read32(hc->op, XHCI_OP_USBSTS)",
            "write32(hc->op, XHCI_OP_USBSTS, ACK)",
            "write32(hc->rt, XHCI_RT_IMAN, IMAN | XHCI_IMAN_IP)",
            "process_events(*hc)",
        ],
        "xHCI IRQ acknowledgement",
    )
    require_tokens(
        events,
        [
            "consume_event_ring",
            "EVENT_RING_SIZE",
            "hc.evt_dequeue = cursor.dequeue",
            "hc.evt_cycle = cursor.cycle",
            "ERDP | (uint64_t{1} << 3U)",
        ],
        "bounded event-ring drain and ERDP/EHB update",
    )
    require_tokens(
        event,
        [
            "request_table(hc).complete_event(event)",
            "compose_portsc_ack_write(PORTSC)",
            "queue_port_snapshot(*hc, PORT_ID, PORTSC)",
        ],
        "IRQ event publication",
    )
    forbidden = [
        "enumerate_device(",
        "teardown_device(",
        "page_alloc(",
        "kern_yield(",
        "kern_block(",
        "send_command(",
        "xhci_control_transfer(",
        "new (",
        "delete ",
    ]
    combined = irq + events + event
    present = [token for token in forbidden if token in combined]
    if present:
        fail(f"hard IRQ path contains task-context work: {', '.join(present)}")


def test_teardown_orders_admission_hardware_barrier_and_release() -> None:
    body = function_body(XHCI_CPP.read_text(), "teardown_device")
    require_order(
        body,
        [
            "endpoint.accepting.store(false",
            "bound_driver->quiesce",
            "mark_endpoint_request_cancelled",
            "stop_endpoint",
            "drop_non_control_endpoints",
            "disable_slot",
            "bound_driver->detach",
            "retire_endpoint_request_after_barrier",
            "hc.dcbaap[dev.slot_id] = 0",
            "endpoint.ring_dma.retire(dma_api, true)",
            "dev.input_ctx_dma.retire(dma_api, true)",
            "dev.dev_ctx_dma.retire(dma_api, true)",
            "new (&dev) UsbDevice{}",
        ],
        "xHCI disconnect ownership order",
    )
    require_tokens(
        body,
        [
            "hc.command_quarantined.load(std::memory_order_acquire)",
            "return quarantine_slot(dev)",
        ],
        "unproven controller retirement quarantine",
    )

    configure_bulk = function_body(XHCI_CPP.read_text(), "xhci_configure_bulk_endpoints")
    require_order(
        configure_bulk,
        [
            "configure_endpoint",
            "in.configured = true",
            "out.configured = true",
            "in.accepting.store(true, std::memory_order_release)",
            "out.accepting.store(true, std::memory_order_release)",
        ],
        "bulk endpoint admission after hardware configuration",
    )


def test_live_qemu_interrupt_and_cdc_compatibility() -> None:
    xhci = function_body(XHCI_CPP.read_text(), "init_controller")
    require_order(
        xhci,
        [
            "pci::pci_enable_msix(pci_device, vector)",
            "pci::pci_enable_msi(pci_device, vector)",
            "pci_device->interrupt_line + 32U",
        ],
        "xHCI MSI-X, MSI, and INTx fallback order",
    )

    cdc = CDC_CPP.read_text()
    publish = function_body(cdc, "cdc_publish")
    require_tokens(
        cdc,
        [
            "QEMU_USB_NET_VENDOR = 0x0525",
            "QEMU_USB_NET_PRODUCT = 0xA4A2",
        ],
        "QEMU usb-net identity",
    )
    require_order(
        publish,
        [
            "xhci_control_transfer",
            "FILTER_RESULT != 0 && !QEMU_USB_NET",
            "net::netdev_register",
        ],
        "QEMU usb-net default packet-filter compatibility",
    )

    enumerate_device = function_body(XHCI_CPP.read_text(), "enumerate_device")
    require_order(
        enumerate_device,
        [
            "descriptor.b_num_configurations",
            "read_configuration",
            "configuration_has_class_driver",
            "probe_class_driver",
            "full_config->b_configuration_value",
        ],
        "class-compatible USB configuration selection",
    )


def test_cdc_and_wki_readers_are_retired_before_slot_reuse() -> None:
    cdc = CDC_CPP.read_text()
    quiesce = function_body(cdc, "cdc_quiesce")
    detach = function_body(cdc, "cdc_detach")
    require_order(quiesce, ["state.exchange(CdcEtherState::RETIRING", "netdev_mark_control_retiring", "netdev_unregister_begin"], "CDC fences control before unpublication")
    if "netdev.remotable = nullptr" in quiesce:
        fail("CDC must keep immutable operations through retained control completion")
    require_order(detach, ["netdev_unregister_wait", "netdev.remotable = nullptr", "state.store(CdcEtherState::FREE"], "CDC clears operations after retained readers drain")
    require_order(
        quiesce,
        [
            "state.exchange(CdcEtherState::RETIRING",
            "netdev_unregister_begin",
            "wki_remotable_withdraw_net",
            "wki_dev_server_detach_all_for_netdev",
            "route_del_for_dev",
            "netif_del_for_dev",
            "arp_forget_device",
            "quiesce_proven = true",
        ],
        "CDC external unpublication",
    )
    require_order(
        detach,
        [
            "io_readers.load(std::memory_order_acquire)",
            "netdev_unregister_wait",
            "dev->driver_data = nullptr",
            "state.store(CdcEtherState::FREE",
        ],
        "CDC reader drain before static-slot reuse",
    )

    wki = WKI_DEV_SERVER_CPP.read_text()
    require_tokens(
        wki,
        [
            "ker::net::NetDeviceRef ndev_ref = find_net_device_by_resource_id(req->resource_id)",
            "ker::net::NetDeviceRegistryLease const REGISTRATION",
            "REGISTRATION.contains(ndev)",
            "binding.net_dev_ref = std::move(ndev_ref)",
        ],
        "WKI NET binding registration pin",
    )


def test_irq_and_netdevice_unregistration_have_reader_barriers() -> None:
    gates = GATES_CPP.read_text()
    free_irq = function_body(gates, "free_irq")
    require_order(
        free_irq,
        [
            "state = VectorState::RETIRING",
            "context.lifecycle.fetch_or(IRQ_CONTEXT_RETIRING",
            "wait_for_irq_context_readers(context)",
            "context.handler = nullptr",
            "context.data = nullptr",
            "vector_states.at(vector) = VectorState::FREE",
        ],
        "IRQ private-data retirement",
    )

    netdevice = NETDEVICE_CPP.read_text()
    unregister = function_body(netdevice, "netdev_unregister_begin")
    wait = function_body(netdevice, "netdev_unregister_wait")
    require_order(
        unregister,
        [
            "lifetime_readers.fetch_or(NETDEV_LIFETIME_RETIRING",
            "token.identity =",
            "devices.at(j - 1) = devices.at(j)",
        ],
        "netdevice admission close before registry removal",
    )
    require_tokens(
        wait,
        [
            "token.identity.generation",
            "NETDEV_LIFETIME_READER_MASK",
            "kern_yield()",
        ],
        "generation-qualified netdevice reader drain",
    )


def test_controller_dma_publication_and_cleanup() -> None:
    source = XHCI_CPP.read_text()
    probe = function_body(source, "init_controller")
    require_order(probe, ["pci_set_bus_master(pci_device, false)", "allocate_controller_dma(hc, xhci_dma::DCBAA",
                         "allocate_controller_dma(hc, xhci_dma::COMMAND", "request_irq", "region.expose()",
                         "pci_set_bus_master(pci_device, true)", "XHCI_OP_DCBAAP", "XHCI_RT_ERSTBA", "XHCI_CMD_RUN | XHCI_CMD_INTE", "XHCI_STS_HCE"], "DMA before BME and RUN")
    cleanup = function_body(source, "cleanup_controller")
    require_order(cleanup, ["XHCI_CMD_INTE | XHCI_CMD_RUN", "halt_controller(hc)", "pci_set_bus_master(hc->pci, false)",
                           "free_irq", "region.retire", "if (!released", "hc->~XhciController"], "halt/readers before release")
    require_tokens(probe, ["HCCPARAMS1 & 1U", "XHCI_OP_PAGESIZE", "if (vector_reserved)"], "mask, page and IRQ ownership checks")


def test_slot_dma_and_retirement_fail_closed() -> None:
    source = XHCI_CPP.read_text()
    allocate = function_body(source, "allocate_slot_dma")
    require_order(allocate, ["dma::allocate_coherent", "region.allocation =", "region.prepare", "region.submission.address"], "managed slot address")
    enumeration = function_body(source, "enumerate_device")
    require_order(enumeration, ["dev.controller != nullptr", "dev.~UsbDevice", "dev.dev_ctx_dma", "dev.dev_ctx_dma.expose()", "hc.dcbaap[dev.slot_id] =", "dev.input_ctx_dma", "ring_dma", "ep0.ring_dma.expose()", "address_device"], "mapping and exposure before slot publication")
    teardown = function_body(source, "teardown_device")
    require_order(teardown, ["ring_lock.lock_irqsave", "endpoint.accepting.store(false", "ring_lock.unlock_irqrestore", "mark_endpoint_request_cancelled", "endpoint_stop_action", "stop_endpoint", "drop_non_control_endpoints(hc, dev) != 0", "disable_slot(&hc, dev.slot_id) != 0", "bound_driver->detach", "ring_dma.retire"], "close publication, stop and drain before unmap")
    if "static_cast<void>(stop_endpoint" in teardown or "static_cast<void>(drop_non_control_endpoints" in teardown:
        fail("slot retirement ignores an endpoint stop/drop failure")
    publication = function_body(source, "submit_transfer")
    require_order(publication, ["endpoint.accepting.load", "return {}", "request_table(&hc).reserve"], "final transfer eligibility before publication")
    require_tokens(function_body(source, "wait_for_request"), ["request::await_result"], "shared tested waiter")
    waiter = function_body((XHCI_CPP.parent / "xhci_request.hpp").read_text(), "await_result")
    require_order(waiter, ["!requests.snapshot(handle, snapshot)", "TimeoutDisposition::STALE_HANDLE", "snapshot.state = RequestState::TIMED_OUT", "return invalid_result", "yield", "continue"], "unavailable snapshot never grants completion")
    require_tokens(waiter, ["if (requests.snapshot(handle, snapshot)) return snapshot.result"], "timeout requires published result")
    bulk = function_body(source, "xhci_configure_bulk_endpoints")
    require_order(bulk, ["allocate_slot_dma", "in.ring_dma.expose()", "out.ring_dma.expose()", "configure_endpoint"], "bulk mappings before configure command")
    if "page_free" in bulk or "alloc_pages" in bulk:
        fail("endpoint rings bypass managed mapping or release")


def test_streaming_ownership_and_terminal_completion() -> None:
    source = XHCI_CPP.read_text()
    if "virt_to_phys" in source or "alloc_pages(" in source or "page_free(bounce" in source or "page_free(endpoint.request_dma" in source:
        fail("xHCI still bypasses the DMA API")
    prepare = function_body(source, "prepare_transfer_dma")
    require_order(prepare, ["dma::allocate_streaming", "region.allocation =", "region.allocation.cpu", "region.prepare"], "owned streaming storage before publication")
    submit = function_body(source, "submit_transfer")
    require_order(submit, ["endpoint.accepting.load", "request_table(&hc).reserve", "endpoint.transfer_dma.expose()", "request_index.store"], "final acceptance and DMA exposure")
    finish = function_body(source, "finish_transfer")
    require_order(finish, ["wait_for_request", "RequestState::TIMED_OUT", "snapshot.event_uses_data", "transfer_dma.complete", "std::memcpy", "transfer_dma.release", "request_table(hc).release", "request_busy.store(false"], "terminal event then synchronization/copy/release")
    bulk = function_body(source, "xhci_bulk_transfer")
    require_order(bulk, ["prepare_transfer_dma", "plan_transfer", "submit_transfer", "TRB_NORMAL | TRB_CHAIN", "TRB_EVENT_DATA | TRB_IOC", "ring_doorbell"], "bounded segments and terminal Event Data")
    control = function_body(source, "xhci_control_transfer")
    require_tokens(control, ["PLAN.count + 2", "identities.at(PLAN.count + 1)", "TRB_STATUS", "TRB_ISP"], "control Status retirement and short progress")
    require_tokens(function_body(source, "ring_enqueue"), ["(control & TRB_CHAIN)"], "Link retains TD Chain")


def test_terminal_shutdown_owns_readers_and_retains_metadata() -> None:
    source = XHCI_CPP.read_text()
    cdc = CDC_CPP.read_text()
    for name in ("xhci_control_transfer", "xhci_bulk_transfer", "xhci_configure_bulk_endpoints", "send_command_result"):
        require_order(function_body(source, name), ["CallbackGuard const GUARD", "if (!GUARD"], f"{name} terminal admission")
    require_order(function_body(source, "process_pending_ports"), ["CallbackGuard const GUARD", "if (!GUARD)", "handle_port"], "lifecycle mutation gate")
    require_order(function_body(source, "ensure_usb_worker"), ["create_kernel_thread", "try_acquire", "post_task_balanced"], "retained USB worker pointer")
    require_order(function_body(cdc, "cdc_ether_init"), ["create_kernel_thread", "try_acquire", "post_task_balanced"], "retained CDC worker pointer")
    for name in ("cdc_start_xmit", "cdc_rx_worker"):
        require_order(function_body(cdc, name), ["hold_kernel_shutdown", "cdc_io_try_acquire", "release_cdc_io"], f"{name} cooperative ownership")
    terminal = function_body(cdc, "cdc_terminal_drain")
    require_tokens(terminal, ["CdcEtherState::RETIRING", "quiesce_proven", "get_ms() - START < 5000", "io_readers.load"], "bounded class drain")
    if "netdev_unregister_wait" in terminal or "CdcEtherState::FREE" in terminal:
        fail("terminal class metadata is recycled or waits for arbitrary external references")
    require_tokens(function_body(source, "retire_controller"), ["hold_kernel_shutdown", "compare_exchange_strong", "xhci_terminal::retire", "State::retained"], "serialized terminal ownership")
    power = (XHCI_CPP.parents[2] / "platform/power/power.cpp").read_text()
    require_order(function_body(power, "run_finalizer"), ["wki_shutdown", "vfs_shutdown_unmount_all", "xhci_shutdown", "halt_other_cores"], "power finalizer USB order")


def main() -> None:
    test_terminal_shutdown_owns_readers_and_retains_metadata()
    test_streaming_ownership_and_terminal_completion()
    test_slot_dma_and_retirement_fail_closed()
    test_controller_dma_publication_and_cleanup()
    test_irq_is_bounded_and_only_publishes()
    test_teardown_orders_admission_hardware_barrier_and_release()
    test_live_qemu_interrupt_and_cdc_compatibility()
    test_cdc_and_wki_readers_are_retired_before_slot_reuse()
    test_irq_and_netdevice_unregistration_have_reader_barriers()
    print("xhci lifecycle source tests passed")


if __name__ == "__main__":
    main()
