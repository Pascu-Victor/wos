#pragma once

#include <dev/usb/xhci.hpp>
#include <net/netdevice.hpp>

namespace ker::dev::usb {

// CDC Ethernet class driver
// Matches CDC ECM (Ethernet Control Model) or QEMU usb-net (RNDIS/CDC)

constexpr uint8_t CDC_SUBCLASS_ECM     = 0x06;
constexpr uint8_t CDC_SUBCLASS_NCM     = 0x0D;

// CDC functional descriptor types
constexpr uint8_t CDC_CS_INTERFACE     = 0x24;
constexpr uint8_t CDC_HEADER_TYPE      = 0x00;
constexpr uint8_t CDC_UNION_TYPE       = 0x06;
constexpr uint8_t CDC_ETHERNET_TYPE    = 0x0F;

struct CdcEtherDevice {
    ker::net::NetDevice netdev;
    UsbDevice* usb_dev;
    XhciController* hc;
    UsbEndpoint bulk_in;
    UsbEndpoint bulk_out;
    uint8_t data_iface;
    bool active;
};

void cdc_ether_init();

}  // namespace ker::dev::usb
