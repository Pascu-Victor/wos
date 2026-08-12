#include "cdc_ether.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/netdevice.hpp>
#include <net/netif.hpp>
#include <net/packet.hpp>
#include <net/proto/arp.hpp>
#include <net/route.hpp>
#include <net/wki/dev_server.hpp>
#include <net/wki/remotable.hpp>
#include <new>  // IWYU pragma: keep
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <utility>

#include "dev/usb/xhci.hpp"

namespace ker::dev::usb {

using log = ker::mod::dbg::logger<"cdc">;

namespace {

constexpr size_t MAX_CDC_DEVICES = 4;
constexpr uint8_t USB_REQ_SET_INTERFACE = 0x0B;
constexpr uint8_t CDC_REQ_SET_ETHERNET_PACKET_FILTER = 0x43;
constexpr uint16_t CDC_PACKET_FILTER_ALL = 0x000F;
constexpr uint16_t QEMU_USB_NET_VENDOR = 0x0525;
constexpr uint16_t QEMU_USB_NET_PRODUCT = 0xA4A2;
constexpr size_t ETHERNET_FRAME_OVERHEAD = 18;

std::array<CdcEtherDevice, MAX_CDC_DEVICES> cdc_devices{};
std::array<mod::sched::task::Task*, MAX_CDC_DEVICES> cdc_rx_tasks{};
std::array<std::atomic<bool>, MAX_CDC_DEVICES> cdc_rx_pending{};

auto remotable_can_remote() -> bool { return true; }
auto remotable_can_share() -> bool { return true; }
auto remotable_can_passthrough() -> bool { return false; }
auto remotable_on_attach(uint16_t node_id) -> int {
    log::trace("remote attach from 0x%04x", node_id);
    return 0;
}
void remotable_on_detach(uint16_t node_id) { log::trace("remote detach from 0x%04x", node_id); }
void remotable_on_fault(uint16_t node_id) { log::trace("remote fault for 0x%04x", node_id); }

const net::wki::RemotableOps S_REMOTABLE_OPS = {
    .can_remote = remotable_can_remote,
    .can_share = remotable_can_share,
    .can_passthrough = remotable_can_passthrough,
    .on_remote_attach = remotable_on_attach,
    .on_remote_detach = remotable_on_detach,
    .on_remote_fault = remotable_on_fault,
};

void release_cdc_io(CdcEtherDevice& cdc) {
    if (!cdc_io_release(cdc)) {
        log::error("unbalanced CDC I/O release");
    }
}

void wake_cdc_rx(size_t index) {
    cdc_rx_pending.at(index).store(true, std::memory_order_release);
    if (cdc_rx_tasks.at(index) != nullptr) {
        mod::sched::wake_task_from_event(cdc_rx_tasks.at(index));
    }
}

auto cdc_index(const CdcEtherDevice& cdc) -> size_t { return static_cast<size_t>(&cdc - cdc_devices.data()); }

int cdc_open(net::NetDevice* netdev) {
    auto* cdc = netdev != nullptr ? static_cast<CdcEtherDevice*>(netdev->private_data) : nullptr;
    if (cdc == nullptr || cdc->state.load(std::memory_order_acquire) != CdcEtherState::LIVE) {
        return -ENODEV;
    }
    netdev->state = 1;
    wake_cdc_rx(cdc_index(*cdc));
    return 0;
}

void cdc_close(net::NetDevice* netdev) {
    if (netdev != nullptr) {
        netdev->state = 0;
    }
}

int cdc_start_xmit(net::NetDevice* netdev, net::PacketBuffer* pkt) {
    if (pkt == nullptr) {
        return -EINVAL;
    }
    auto* cdc = netdev != nullptr ? static_cast<CdcEtherDevice*>(netdev->private_data) : nullptr;
    net::NetDeviceRef netdev_ref = net::netdev_retain_registered(netdev);
    if (cdc == nullptr || !netdev_ref || !cdc_io_try_acquire(*cdc)) {
        net::pkt_free(pkt);
        return -ENODEV;
    }

    XhciController* const HC = cdc->hc;
    UsbDevice* const USB_DEV = cdc->usb_dev;
    UsbEndpoint* const BULK_OUT = cdc->bulk_out;
    size_t const LENGTH = pkt->len;
    int result = -ENODEV;
    if (HC != nullptr && USB_DEV != nullptr && BULK_OUT != nullptr) {
        result = xhci_bulk_transfer(HC, USB_DEV->slot_id, BULK_OUT, pkt->data, LENGTH);
    }
    if (result == 0) {
        netdev->tx_packets++;
        netdev->tx_bytes += LENGTH;
    } else {
        netdev->tx_dropped++;
    }

    release_cdc_io(*cdc);
    net::pkt_free(pkt);
    return result;
}

void cdc_set_mac(net::NetDevice* /*unused*/, const uint8_t* /*unused*/) {}

const net::NetDeviceOps CDC_OPS = {
    .open = cdc_open,
    .close = cdc_close,
    .start_xmit = cdc_start_xmit,
    .set_mac = cdc_set_mac,
    .set_queue_cpu = nullptr,
};

struct BulkEndpointPair {
    UsbEndpointDescriptor in{};
    UsbEndpointDescriptor out{};
    uint8_t alternate_setting{};
    bool found_in{};
    bool found_out{};
};

auto find_control_interface(const uint8_t* config_data, size_t config_len, uint8_t fallback) -> uint8_t {
    size_t offset = 0;
    while (offset + 2 <= config_len) {
        uint8_t const LENGTH = config_data[offset];
        uint8_t const TYPE = config_data[offset + 1];
        if (LENGTH < 2 || offset + LENGTH > config_len) {
            break;
        }
        if (TYPE == USB_DESC_INTERFACE && LENGTH >= sizeof(UsbInterfaceDescriptor)) {
            auto const* interface = reinterpret_cast<const UsbInterfaceDescriptor*>(config_data + offset);
            if (interface->b_interface_class == USB_CLASS_CDC &&
                (interface->b_interface_sub_class == CDC_SUBCLASS_ECM || interface->b_interface_sub_class == CDC_SUBCLASS_NCM)) {
                return interface->b_interface_number;
            }
        }
        offset += LENGTH;
    }
    return fallback;
}

auto find_data_interface(const uint8_t* config_data, size_t config_len, uint8_t control_iface) -> uint8_t {
    size_t offset = 0;
    while (offset + 2 <= config_len) {
        uint8_t const LENGTH = config_data[offset];
        uint8_t const TYPE = config_data[offset + 1];
        if (LENGTH < 2 || offset + LENGTH > config_len) {
            break;
        }
        if (TYPE == CDC_CS_INTERFACE && LENGTH >= 5 && config_data[offset + 2] == CDC_UNION_TYPE &&
            config_data[offset + 3] == control_iface) {
            return config_data[offset + 4];
        }
        offset += LENGTH;
    }
    return static_cast<uint8_t>(control_iface + 1U);
}

auto find_bulk_endpoints(const uint8_t* config_data, size_t config_len, uint8_t data_iface) -> BulkEndpointPair {
    BulkEndpointPair pair{};
    bool target_interface = false;
    uint8_t alternate_setting = 0;
    size_t offset = 0;
    while (offset + 2 <= config_len) {
        uint8_t const LENGTH = config_data[offset];
        uint8_t const TYPE = config_data[offset + 1];
        if (LENGTH < 2 || offset + LENGTH > config_len) {
            break;
        }
        if (TYPE == USB_DESC_INTERFACE && LENGTH >= sizeof(UsbInterfaceDescriptor)) {
            if (pair.found_in && pair.found_out) {
                return pair;
            }
            auto const* interface = reinterpret_cast<const UsbInterfaceDescriptor*>(config_data + offset);
            target_interface = interface->b_interface_number == data_iface;
            alternate_setting = interface->b_alternate_setting;
            if (target_interface) {
                pair = {.alternate_setting = alternate_setting};
            }
        } else if (TYPE == USB_DESC_ENDPOINT && target_interface && LENGTH >= sizeof(UsbEndpointDescriptor)) {
            auto const* endpoint = reinterpret_cast<const UsbEndpointDescriptor*>(config_data + offset);
            if ((endpoint->bm_attributes & USB_EP_TYPE_MASK) == USB_EP_TYPE_BULK) {
                if ((endpoint->b_endpoint_address & USB_EP_DIR_IN) != 0) {
                    pair.in = *endpoint;
                    pair.found_in = true;
                } else {
                    pair.out = *endpoint;
                    pair.found_out = true;
                }
                pair.alternate_setting = alternate_setting;
            }
        }
        offset += LENGTH;
    }
    return pair;
}

auto cdc_probe(UsbDevice* dev, UsbInterfaceDescriptor* interface) -> bool {
    if (dev == nullptr || interface == nullptr) {
        return false;
    }
    if (interface->b_interface_class == USB_CLASS_CDC &&
        (interface->b_interface_sub_class == CDC_SUBCLASS_ECM || interface->b_interface_sub_class == CDC_SUBCLASS_NCM)) {
        return true;
    }
    return dev->vendor_id == 0x0BDA && dev->product_id == 0x8153;
}

auto reserve_cdc_slot() -> CdcEtherDevice* {
    for (size_t index = 0; index < cdc_devices.size(); ++index) {
        if (cdc_rx_tasks.at(index) == nullptr) {
            continue;
        }
        auto& cdc = cdc_devices.at(index);
        CdcEtherState expected = CdcEtherState::FREE;
        if (!cdc.state.compare_exchange_strong(expected, CdcEtherState::PREPARING, std::memory_order_acq_rel, std::memory_order_acquire)) {
            continue;
        }
        cdc.netdev.~NetDevice();
        new (&cdc.netdev) net::NetDevice{};
        cdc.usb_dev = nullptr;
        cdc.hc = nullptr;
        cdc.bulk_in = nullptr;
        cdc.bulk_out = nullptr;
        cdc.retire_token = {};
        cdc.io_readers.store(0, std::memory_order_relaxed);
        cdc.control_iface = 0;
        cdc.data_iface = 0;
        cdc.data_alt = 0;
        cdc.quiesce_proven = false;
        return &cdc;
    }
    return nullptr;
}

auto cdc_attach(UsbDevice* dev, UsbInterfaceDescriptor* interface, uint8_t* config_data, size_t config_len) -> int {
    if (dev == nullptr || dev->controller == nullptr || interface == nullptr || config_data == nullptr) {
        return -EINVAL;
    }
    CdcEtherDevice* const CDC = reserve_cdc_slot();
    if (CDC == nullptr) {
        return -ENOSPC;
    }

    CDC->usb_dev = dev;
    CDC->hc = dev->controller;
    CDC->control_iface = find_control_interface(config_data, config_len, interface->b_interface_number);
    CDC->data_iface = find_data_interface(config_data, config_len, CDC->control_iface);
    BulkEndpointPair const ENDPOINTS = find_bulk_endpoints(config_data, config_len, CDC->data_iface);
    if (!ENDPOINTS.found_in || !ENDPOINTS.found_out) {
        CDC->state.store(CdcEtherState::FREE, std::memory_order_release);
        return -ENODEV;
    }

    int const RESULT = xhci_configure_bulk_endpoints(dev, ENDPOINTS.in, ENDPOINTS.out, &CDC->bulk_in, &CDC->bulk_out);
    if (RESULT != 0) {
        CDC->state.store(CdcEtherState::FREE, std::memory_order_release);
        return RESULT;
    }
    CDC->data_alt = ENDPOINTS.alternate_setting;

    CDC->netdev.mac.at(0) = 0x02;
    CDC->netdev.mac.at(1) = static_cast<uint8_t>(dev->vendor_id >> 8U);
    CDC->netdev.mac.at(2) = static_cast<uint8_t>(dev->vendor_id);
    CDC->netdev.mac.at(3) = static_cast<uint8_t>(dev->product_id >> 8U);
    CDC->netdev.mac.at(4) = static_cast<uint8_t>(dev->product_id);
    CDC->netdev.mac.at(5) = static_cast<uint8_t>(cdc_index(*CDC));
    CDC->netdev.ops = &CDC_OPS;
    CDC->netdev.mtu = 1500;
    CDC->netdev.private_data = CDC;
    CDC->netdev.remotable = &S_REMOTABLE_OPS;
    dev->driver_data = CDC;
    CDC->state.store(CdcEtherState::PREPARED, std::memory_order_release);
    return 0;
}

auto cdc_publish(UsbDevice* dev) -> int {
    auto* cdc = dev != nullptr ? static_cast<CdcEtherDevice*>(dev->driver_data) : nullptr;
    if (cdc == nullptr || cdc->state.load(std::memory_order_acquire) != CdcEtherState::PREPARED) {
        return -EINVAL;
    }

    if (cdc->data_alt != 0) {
        UsbSetupPacket set_interface = {
            .bm_request_type = 0x01,
            .b_request = USB_REQ_SET_INTERFACE,
            .w_value = cdc->data_alt,
            .w_index = cdc->data_iface,
            .w_length = 0,
        };
        int const RESULT = xhci_control_transfer(cdc->hc, dev->slot_id, &set_interface, nullptr, 0, false);
        if (RESULT != 0) {
            return RESULT;
        }
    }

    UsbSetupPacket packet_filter = {
        .bm_request_type = 0x21,
        .b_request = CDC_REQ_SET_ETHERNET_PACKET_FILTER,
        .w_value = CDC_PACKET_FILTER_ALL,
        .w_index = cdc->control_iface,
        .w_length = 0,
    };
    int const FILTER_RESULT = xhci_control_transfer(cdc->hc, dev->slot_id, &packet_filter, nullptr, 0, false);
    bool const QEMU_USB_NET = dev->vendor_id == QEMU_USB_NET_VENDOR && dev->product_id == QEMU_USB_NET_PRODUCT;
    if (FILTER_RESULT != 0 && !QEMU_USB_NET) {
        return FILTER_RESULT;
    }
    if (FILTER_RESULT != 0) {
        log::warn("QEMU usb-net rejected SET_ETHERNET_PACKET_FILTER; continuing with its default filter");
    }

    if (net::netdev_register(&cdc->netdev) != 0) {
        return -ENOSPC;
    }
    cdc->netdev.state = 1;
    cdc->state.store(CdcEtherState::LIVE, std::memory_order_release);
    wake_cdc_rx(cdc_index(*cdc));
    log::info("%s MAC=%02x:%02x:%02x:%02x:%02x:%02x ready", cdc->netdev.name.data(), cdc->netdev.mac.at(0), cdc->netdev.mac.at(1),
              cdc->netdev.mac.at(2), cdc->netdev.mac.at(3), cdc->netdev.mac.at(4), cdc->netdev.mac.at(5));
    return 0;
}

auto cdc_quiesce(UsbDevice* dev) -> bool {
    auto* cdc = dev != nullptr ? static_cast<CdcEtherDevice*>(dev->driver_data) : nullptr;
    if (cdc == nullptr) {
        return true;
    }
    CdcEtherState const PREVIOUS = cdc->state.exchange(CdcEtherState::RETIRING, std::memory_order_acq_rel);
    if (PREVIOUS == CdcEtherState::FREE || PREVIOUS == CdcEtherState::RETIRING) {
        return PREVIOUS == CdcEtherState::FREE || cdc->quiesce_proven;
    }
    if (PREVIOUS != CdcEtherState::LIVE) {
        cdc->quiesce_proven = true;
        return true;
    }

    cdc->netdev.state = 0;
    if (cdc->netdev.wki_transport) {
        log::error("%s is an active WKI transport; quarantining its USB slot", cdc->netdev.name.data());
        return false;
    }
    uint32_t const IFINDEX = cdc->netdev.ifindex;
    if (net::netdev_unregister_begin(&cdc->netdev, cdc->retire_token) != 0) {
        log::error("%s failed to begin netdevice retirement", cdc->netdev.name.data());
        return false;
    }
    net::wki::wki_remotable_withdraw_net(IFINDEX);
    net::wki::wki_dev_server_detach_all_for_netdev(&cdc->netdev);
    cdc->netdev.wki_rx_forward.store(nullptr, std::memory_order_release);
    cdc->netdev.remotable = nullptr;
    static_cast<void>(net::route_del_for_dev(&cdc->netdev));
    static_cast<void>(net::netif_del_for_dev(&cdc->netdev));
    net::proto::arp_forget_device(cdc->retire_token.identity);
    cdc->quiesce_proven = true;
    return true;
}

void cdc_detach(UsbDevice* dev) {
    auto* cdc = dev != nullptr ? static_cast<CdcEtherDevice*>(dev->driver_data) : nullptr;
    if (cdc == nullptr) {
        return;
    }
    while (cdc->io_readers.load(std::memory_order_acquire) != 0) {
        mod::sched::kern_yield();
    }
    if (cdc->retire_token.valid()) {
        net::netdev_unregister_wait(cdc->retire_token);
    }

    log::info("%s detached", cdc->netdev.name.data());
    dev->driver_data = nullptr;
    cdc->usb_dev = nullptr;
    cdc->hc = nullptr;
    cdc->bulk_in = nullptr;
    cdc->bulk_out = nullptr;
    cdc->retire_token = {};
    cdc->quiesce_proven = false;
    cdc->state.store(CdcEtherState::FREE, std::memory_order_release);
}

template <size_t Index>
[[noreturn]] void cdc_rx_worker() {
    auto& cdc = cdc_devices.at(Index);
    for (;;) {
        if (cdc.state.load(std::memory_order_acquire) != CdcEtherState::LIVE) {
            if (!cdc_rx_pending.at(Index).exchange(false, std::memory_order_acq_rel)) {
                mod::sched::kern_block();
            }
            continue;
        }
        if (!cdc_io_try_acquire(cdc)) {
            mod::sched::kern_yield();
            continue;
        }

        net::NetDeviceRef netdev_ref = net::netdev_retain_registered(&cdc.netdev);
        net::PacketBuffer* packet = net::pkt_alloc_rx();
        int result = -ENODEV;
        size_t actual = 0;
        if (netdev_ref && packet != nullptr && cdc.hc != nullptr && cdc.usb_dev != nullptr && cdc.bulk_in != nullptr) {
            size_t const CAPACITY = std::min(packet->tailroom(), static_cast<size_t>(cdc.netdev.mtu) + ETHERNET_FRAME_OVERHEAD);
            result = xhci_bulk_transfer(cdc.hc, cdc.usb_dev->slot_id, cdc.bulk_in, packet->data, CAPACITY, &actual, true);
        }

        bool const PUBLISH = result == 0 && actual != 0 && cdc.state.load(std::memory_order_acquire) == CdcEtherState::LIVE && netdev_ref;
        if (PUBLISH) {
            packet->len = actual;
            if (net::pkt_adopt_netdev_ref(packet, std::move(netdev_ref))) {
                net::netdev_rx(&cdc.netdev, packet);
                packet = nullptr;
            }
        }
        if (packet != nullptr) {
            net::pkt_free(packet);
        }
        release_cdc_io(cdc);
        if (result != 0 && cdc.state.load(std::memory_order_acquire) == CdcEtherState::LIVE) {
            mod::sched::kern_yield();
        }
    }
}

using WorkerEntry = void (*)();
constexpr std::array<WorkerEntry, MAX_CDC_DEVICES> CDC_RX_WORKERS = {
    cdc_rx_worker<0>,
    cdc_rx_worker<1>,
    cdc_rx_worker<2>,
    cdc_rx_worker<3>,
};

UsbClassDriver cdc_driver = {
    .name = "cdc-ether",
    .probe = cdc_probe,
    .attach = cdc_attach,
    .publish = cdc_publish,
    .quiesce = cdc_quiesce,
    .detach = cdc_detach,
    .next = nullptr,
};

}  // namespace

void cdc_ether_init() {
    size_t workers = 0;
    for (size_t index = 0; index < cdc_rx_tasks.size(); ++index) {
        auto* task = mod::sched::task::Task::create_kernel_thread("cdc_rx", CDC_RX_WORKERS.at(index));
        if (task == nullptr || !mod::sched::post_task_balanced(task)) {
            log::error("failed to start CDC RX worker %zu", index);
            continue;
        }
        cdc_rx_tasks.at(index) = task;
        ++workers;
    }
    if (workers != 0) {
        usb_register_class_driver(&cdc_driver);
    }
}

}  // namespace ker::dev::usb
