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
            "page_free(ring)",
            "free_page(dev.input_ctx)",
            "free_page(dev.dev_ctx)",
            "new (&dev) UsbDevice{}",
        ],
        "xHCI disconnect ownership order",
    )
    require_tokens(
        body,
        [
            "hc.command_quarantined.load(std::memory_order_acquire)",
            "dev.state.store(UsbDeviceState::FAILED",
            "return false",
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


def main() -> None:
    test_irq_is_bounded_and_only_publishes()
    test_teardown_orders_admission_hardware_barrier_and_release()
    test_live_qemu_interrupt_and_cdc_compatibility()
    test_cdc_and_wki_readers_are_retired_before_slot_reuse()
    test_irq_and_netdevice_unregistration_have_reader_barriers()
    print("xhci lifecycle source tests passed")


if __name__ == "__main__":
    main()
