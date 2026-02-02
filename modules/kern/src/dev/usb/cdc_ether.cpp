#include "cdc_ether.hpp"

#include <array>
#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <net/netdevice.hpp>
#include <net/packet.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>

namespace ker::dev::usb {

// External references to xhci.cpp globals (must be outside anonymous namespace)
extern XhciController* controllers[];
extern size_t controller_count;

namespace {
constexpr size_t MAX_CDC_DEVICES = 4;
std::array<CdcEtherDevice, MAX_CDC_DEVICES> cdc_devices = {};
size_t cdc_count = 0;

auto virt_to_phys(void* v) -> uint64_t {
    auto addr = reinterpret_cast<uint64_t>(v);
    if (addr >= 0xffffffff80000000ULL) {
        return ker::mod::mm::virt::translate(ker::mod::mm::virt::getKernelPagemap(), addr);
    }
    return reinterpret_cast<uint64_t>(ker::mod::mm::addr::getPhysPointer(addr));
}

// NetDevice operations for CDC Ethernet
int cdc_open(ker::net::NetDevice* netdev) {
    netdev->state = 1;
    return 0;
}

void cdc_close(ker::net::NetDevice* netdev) { netdev->state = 0; }

int cdc_start_xmit(ker::net::NetDevice* netdev, ker::net::PacketBuffer* pkt) {
    auto* cdc = static_cast<CdcEtherDevice*>(netdev->private_data);
    if (cdc == nullptr || !cdc->active || pkt == nullptr) {
        if (pkt != nullptr) {
            ker::net::pkt_free(pkt);
        }
        return -1;
    }

    // Submit bulk OUT transfer
    int ret = xhci_bulk_transfer(cdc->hc, cdc->usb_dev->slot_id, &cdc->bulk_out, pkt->data, pkt->len);
    if (ret == 0) {
        netdev->tx_packets++;
        netdev->tx_bytes += pkt->len;
    } else {
        netdev->tx_dropped++;
    }

    ker::net::pkt_free(pkt);
    return ret;
}

void cdc_set_mac(ker::net::NetDevice*, const uint8_t*) {
    // MAC is read from device descriptor
}

ker::net::NetDeviceOps const cdc_ops = {
    .open = cdc_open,
    .close = cdc_close,
    .start_xmit = cdc_start_xmit,
    .set_mac = cdc_set_mac,
};

// Find bulk IN and OUT endpoints from config descriptor
bool find_bulk_endpoints(uint8_t* config_data, size_t config_len, uint8_t data_iface, UsbEndpointDescriptor** ep_in,
                         UsbEndpointDescriptor** ep_out) {
    *ep_in = nullptr;
    *ep_out = nullptr;

    bool in_target_iface = false;
    size_t offset = 0;

    while (offset + 2 <= config_len) {
        uint8_t len = config_data[offset];
        uint8_t type = config_data[offset + 1];
        if (len == 0) {
            break;
        }
        if (offset + len > config_len) {
            break;
        }

        if (type == USB_DESC_INTERFACE) {
            auto* iface = reinterpret_cast<UsbInterfaceDescriptor*>(config_data + offset);
            in_target_iface = (iface->bInterfaceNumber == data_iface);
        } else if (type == USB_DESC_ENDPOINT && in_target_iface) {
            auto* ep = reinterpret_cast<UsbEndpointDescriptor*>(config_data + offset);
            uint8_t ep_type = ep->bmAttributes & USB_EP_TYPE_MASK;
            if (ep_type == USB_EP_TYPE_BULK) {
                if ((ep->bEndpointAddress & USB_EP_DIR_IN) != 0) {
                    *ep_in = ep;
                } else {
                    *ep_out = ep;
                }
            }
        }

        offset += len;
    }

    return (*ep_in != nullptr) && (*ep_out != nullptr);
}

// Find CDC Ethernet functional descriptor to get MAC address
bool find_cdc_ether_desc(const uint8_t* config_data, size_t config_len, uint8_t* mac_string_idx) {
    size_t offset = 0;
    while (offset + 2 <= config_len) {
        uint8_t len = config_data[offset];
        uint8_t type = config_data[offset + 1];
        if (len == 0) {
            break;
        }
        if (offset + len > config_len) {
            break;
        }

        if (type == CDC_CS_INTERFACE && len >= 4) {
            uint8_t subtype = config_data[offset + 2];
            if (subtype == CDC_ETHERNET_TYPE && len >= 6) {
                // iMACAddress is at offset+3
                *mac_string_idx = config_data[offset + 3];
                return true;
            }
        }

        offset += len;
    }
    return false;
}

// Find the data interface number from CDC Union descriptor
uint8_t find_data_interface(const uint8_t* config_data, size_t config_len, uint8_t control_iface) {
    size_t offset = 0;
    while (offset + 2 <= config_len) {
        uint8_t len = config_data[offset];
        uint8_t type = config_data[offset + 1];
        if (len == 0) {
            break;
        }
        if (offset + len > config_len) {
            break;
        }

        if (type == CDC_CS_INTERFACE && len >= 5) {
            uint8_t subtype = config_data[offset + 2];
            if (subtype == CDC_UNION_TYPE) {
                uint8_t master = config_data[offset + 3];
                uint8_t slave = config_data[offset + 4];
                if (master == control_iface) {
                    return slave;
                }
            }
        }

        offset += len;
    }
    // Fallback: data interface is typically control + 1
    return control_iface + 1;
}

// Set up xHCI transfer ring for a bulk endpoint
void setup_bulk_ep(XhciController* hc, UsbDevice* dev, UsbEndpoint* ep, UsbEndpointDescriptor* ep_desc) {
    ep->address = ep_desc->bEndpointAddress;
    ep->type = USB_EP_TYPE_BULK;
    ep->max_packet = ep_desc->wMaxPacketSize;
    ep->interval = ep_desc->bInterval;

    // Allocate transfer ring
    size_t ring_bytes = XFER_RING_SIZE * sizeof(Trb);
    void* ring_virt = ker::mod::mm::phys::pageAlloc(ring_bytes);
    if (ring_virt == nullptr) {
        return;
    }
    std::memset(ring_virt, 0, ring_bytes);

    ep->ring = static_cast<Trb*>(ring_virt);
    ep->ring_phys = virt_to_phys(ring_virt);
    ep->ring_enqueue = 0;
    ep->ring_cycle = true;

    // Configure endpoint in xHCI input context
    uint8_t dci = ((ep->address & 0x80) != 0) ? ((2 * (ep->address & 0x0F)) + 1) : (2 * (ep->address & 0x0F));

    auto* ictx = dev->input_ctx;
    std::memset(ictx, 0, sizeof(InputContext));
    ictx->add_flags = (1 << 0) | (1U << dci);  // Slot + this EP
    ictx->drop_flags = 0;

    // Copy current slot context
    std::memcpy(&ictx->slot, &dev->dev_ctx->slot, sizeof(SlotContext));
    // Update context entries to include this DCI
    uint32_t ctx_entries = (ictx->slot.data[0] >> 27) & 0x1F;
    if (dci > ctx_entries) {
        ictx->slot.data[0] &= ~(0x1FU << 27);
        ictx->slot.data[0] |= (static_cast<uint32_t>(dci) << 27);
    }

    // Set up endpoint context
    auto* ep_ctx = &ictx->ep[dci - 1];
    // EP Type: Bulk OUT = 2, Bulk IN = 6
    uint32_t ep_type = ((ep->address & 0x80) != 0) ? 6U : 2U;
    ep_ctx->data[1] = (ep_type << 3) | (static_cast<uint32_t>(ep->max_packet) << 16);
    ep_ctx->data[2] = static_cast<uint32_t>(ep->ring_phys) | 1;  // DCS=1
    ep_ctx->data[3] = static_cast<uint32_t>(ep->ring_phys >> 32);
    ep_ctx->data[4] = ep->max_packet;  // Average TRB length

    configure_endpoint(hc, dev->slot_id, dev->input_ctx_phys);
}

// CDC Ethernet class driver probe
bool cdc_probe(UsbDevice* dev, UsbInterfaceDescriptor* iface) {
    // Match CDC Communications class with ECM/NCM subclass
    if (iface->bInterfaceClass == USB_CLASS_CDC &&
        (iface->bInterfaceSubClass == CDC_SUBCLASS_ECM || iface->bInterfaceSubClass == CDC_SUBCLASS_NCM)) {
        return true;
    }
    // Also match CDC Data class (some devices present data interface first)
    // Match QEMU usb-net which uses vendor=0x0525 product=0xa4a2
    if (dev->vendor_id == 0x0525 && dev->product_id == 0xa4a2) {
        return true;
    }
    // RTL8153: vendor=0x0BDA product=0x8153
    if (dev->vendor_id == 0x0BDA && dev->product_id == 0x8153) {
        return true;
    }
    return false;
}

// CDC Ethernet class driver attach
int cdc_attach(UsbDevice* dev, UsbInterfaceDescriptor* iface, uint8_t* config_data, size_t config_len) {
    if (cdc_count >= MAX_CDC_DEVICES) {
        return -1;
    }

    // Find controller — use the first one from the global list
    if (controller_count == 0) {
        return -1;
    }
    auto* hc = controllers[0];

    auto* cdc = &cdc_devices[cdc_count];
    std::memset(cdc, 0, sizeof(CdcEtherDevice));
    cdc->usb_dev = dev;
    cdc->hc = hc;

    // Find data interface
    uint8_t data_iface = find_data_interface(config_data, config_len, iface->bInterfaceNumber);
    cdc->data_iface = data_iface;

    // Find bulk endpoints on the data interface
    UsbEndpointDescriptor* ep_in = nullptr;
    UsbEndpointDescriptor* ep_out = nullptr;
    if (!find_bulk_endpoints(config_data, config_len, data_iface, &ep_in, &ep_out)) {
        ker::mod::io::serial::write("cdc-ether: no bulk endpoints found\n");
        return -1;
    }

    ker::mod::io::serial::write("cdc-ether: bulk_in=");
    ker::mod::io::serial::writeHex(ep_in->bEndpointAddress);
    ker::mod::io::serial::write(" bulk_out=");
    ker::mod::io::serial::writeHex(ep_out->bEndpointAddress);
    ker::mod::io::serial::write("\n");

    // Set up bulk endpoints
    setup_bulk_ep(hc, dev, &cdc->bulk_in, ep_in);
    setup_bulk_ep(hc, dev, &cdc->bulk_out, ep_out);

    // Generate a MAC address (use device VID/PID + index for uniqueness)
    cdc->netdev.mac[0] = 0x02;  // locally administered
    cdc->netdev.mac[1] = static_cast<uint8_t>(dev->vendor_id >> 8);
    cdc->netdev.mac[2] = static_cast<uint8_t>(dev->vendor_id);
    cdc->netdev.mac[3] = static_cast<uint8_t>(dev->product_id >> 8);
    cdc->netdev.mac[4] = static_cast<uint8_t>(dev->product_id);
    cdc->netdev.mac[5] = static_cast<uint8_t>(cdc_count);

    // Register as network device
    cdc->netdev.ops = &cdc_ops;
    cdc->netdev.mtu = 1500;
    cdc->netdev.state = 1;
    cdc->netdev.private_data = cdc;
    cdc->netdev.name[0] = '\0';  // Auto-assign
    cdc->active = true;

    ker::net::netdev_register(&cdc->netdev);

    ker::mod::io::serial::write("cdc-ether: ");
    ker::mod::io::serial::write(cdc->netdev.name);
    ker::mod::io::serial::write(" MAC=");
    for (int i = 0; i < 6; i++) {
        if (i > 0) ker::mod::io::serial::write(":");
        ker::mod::io::serial::writeHex(cdc->netdev.mac[i]);
    }
    ker::mod::io::serial::write(" ready\n");

    cdc_count++;
    return 0;
}

void cdc_detach(UsbDevice*) {
    // TODO: tear down
}

UsbClassDriver cdc_driver = {
    .name = "cdc-ether",
    .probe = cdc_probe,
    .attach = cdc_attach,
    .detach = cdc_detach,
    .next = nullptr,
};

}  // namespace

void cdc_ether_init() { usb_register_class_driver(&cdc_driver); }

}  // namespace ker::dev::usb
