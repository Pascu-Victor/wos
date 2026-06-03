#pragma once

#include <cstdint>

namespace ker::net {
struct NetDevice;
struct PacketBuffer;
}  // namespace ker::net

namespace ker::net::wki {

struct WkiTransport;

// Initialize the RoCE RDMA transport (L2, MAC-based GIDs, EtherType 0x88B8).
// Called from wki_init(). The RoCE transport is an RDMA-only overlay - it does
// not carry WKI control messages (those go over transport_eth or transport_ivshmem).
void wki_roce_transport_init();

// RX entry point - called from ethernet.cpp's eth_rx() for EtherType 0x88B8.
void roce_rx(ker::net::NetDevice* dev, ker::net::PacketBuffer* pkt);

// Get the singleton RoCE transport (nullptr if not initialized).
auto wki_roce_transport_get() -> WkiTransport*;

// Reset/read the receive-progress counter for a registered RoCE region.
// Used by control-first protocols where the receiver already knows the exact
// byte count and does not need a separate doorbell to define completion.
auto wki_roce_region_reset_received(uint32_t rkey) -> bool;
auto wki_roce_region_wait_received(uint32_t rkey, uint32_t len, uint64_t timeout_us) -> bool;

// Cookie-tagged writes are used by protocols that reuse one receive rkey at
// offset 0. Tagged accounting lets data that overtakes the control message
// remain associated with the eventual request instead of being cleared by the
// receiver's prepare step.
auto wki_roce_region_prepare_tagged_write(uint32_t rkey, uint16_t cookie) -> bool;
auto wki_roce_region_wait_tagged_write(uint32_t rkey, uint16_t cookie, uint32_t len, uint64_t timeout_us) -> bool;
auto wki_roce_rdma_write_tagged(uint16_t neighbor_id, uint32_t rkey, uint64_t remote_offset, const void* local_buf, uint32_t len,
                                uint16_t cookie) -> int;

}  // namespace ker::net::wki
