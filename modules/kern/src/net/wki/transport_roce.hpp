#pragma once

#include <cstdint>

namespace ker::net {
struct NetDevice;
struct PacketBuffer;
}  // namespace ker::net

namespace ker::net::wki {

struct WkiTransport;

// Initialize the RoCE RDMA transport (L2, MAC-based GIDs, EtherType 0x88B8).
// Called from wki_init(). The RoCE transport is an RDMA-only overlay — it does
// not carry WKI control messages (those go over transport_eth or transport_ivshmem).
void wki_roce_transport_init();

// RX entry point — called from ethernet.cpp's eth_rx() for EtherType 0x88B8.
void roce_rx(ker::net::NetDevice* dev, ker::net::PacketBuffer* pkt);

// Get the singleton RoCE transport (nullptr if not initialized).
auto wki_roce_transport_get() -> WkiTransport*;

}  // namespace ker::net::wki
