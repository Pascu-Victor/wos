#pragma once

#include <atomic>
#include <dev/usb/xhci.hpp>
#include <net/netdevice.hpp>

namespace ker::dev::usb {

// CDC Ethernet class driver
// Matches CDC ECM (Ethernet Control Model) or QEMU usb-net (RNDIS/CDC)

constexpr uint8_t CDC_SUBCLASS_ECM = 0x06;
constexpr uint8_t CDC_SUBCLASS_NCM = 0x0D;

// CDC functional descriptor types
constexpr uint8_t CDC_CS_INTERFACE = 0x24;
constexpr uint8_t CDC_HEADER_TYPE = 0x00;
constexpr uint8_t CDC_UNION_TYPE = 0x06;
constexpr uint8_t CDC_ETHERNET_TYPE = 0x0F;

enum class CdcEtherState : uint8_t {
    FREE,
    PREPARING,
    PREPARED,
    LIVE,
    RETIRING,
};

struct CdcEtherDevice {
    ker::net::NetDevice netdev;
    UsbDevice* usb_dev{};
    XhciController* hc{};
    UsbEndpoint* bulk_in{};
    UsbEndpoint* bulk_out{};
    ker::net::NetDeviceRetireToken retire_token{};
    std::atomic<CdcEtherState> state{CdcEtherState::FREE};
    std::atomic<uint32_t> io_readers{0};
    uint8_t control_iface{};
    uint8_t data_iface{};
    uint8_t data_alt{};
    bool quiesce_proven{};
};

static_assert(std::atomic<CdcEtherState>::is_always_lock_free, "CDC state admission must stay lock-free");
static_assert(std::atomic<uint32_t>::is_always_lock_free, "CDC I/O admission must stay lock-free");

[[nodiscard]] inline auto cdc_io_try_acquire(CdcEtherDevice& cdc) -> bool {
    if (cdc.state.load(std::memory_order_acquire) != CdcEtherState::LIVE) {
        return false;
    }
    cdc.io_readers.fetch_add(1, std::memory_order_acq_rel);
    if (cdc.state.load(std::memory_order_acquire) == CdcEtherState::LIVE) {
        return true;
    }
    cdc.io_readers.fetch_sub(1, std::memory_order_release);
    return false;
}

[[nodiscard]] inline auto cdc_io_release(CdcEtherDevice& cdc) -> bool {
    uint32_t readers = cdc.io_readers.load(std::memory_order_acquire);
    while (readers != 0) {
        if (cdc.io_readers.compare_exchange_weak(readers, readers - 1U, std::memory_order_release, std::memory_order_acquire)) {
            return true;
        }
    }
    return false;
}

void cdc_ether_init();

}  // namespace ker::dev::usb
