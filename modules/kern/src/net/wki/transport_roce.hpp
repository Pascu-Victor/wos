#pragma once

#include <cstdint>

namespace ker::net {
struct NetDevice;
struct PacketBuffer;
}  // namespace ker::net

namespace ker::net::wki {

struct WkiTransport;

// Legacy initialization is a permanent no-op. Secure ciphertext staging uses
// the exact registered Ethernet transport; no RDMA overlay is created.
void wki_roce_transport_init();

// RX entry point - called from ethernet.cpp's eth_rx() for EtherType 0x88B8.
// Lookup-only secure-record ingress. Copies into common admission before
// consuming the packet once; all legacy raw records are rejected.
void roce_rx(ker::net::NetDevice* dev, ker::net::PacketBuffer* pkt);

// Legacy overlay lookup always returns nullptr.
auto wki_roce_transport_get() -> WkiTransport*;

// Legacy raw progress/tagged-write APIs reject immediately without touching
// memory, counters, callbacks or transport state, even for zero-length calls.
auto wki_roce_region_reset_received(uint32_t rkey) -> bool;
auto wki_roce_region_wait_received(uint32_t rkey, uint32_t len, uint64_t timeout_us) -> bool;

auto wki_roce_region_prepare_tagged_write(uint32_t rkey, uint16_t cookie) -> bool;
auto wki_roce_region_tagged_write_complete(uint32_t rkey, uint16_t cookie, uint32_t len) -> bool;
auto wki_roce_region_wait_tagged_write(uint32_t rkey, uint16_t cookie, uint32_t len, uint64_t timeout_us) -> bool;
void wki_roce_region_finish_tagged_write(uint32_t rkey, uint16_t cookie);
auto wki_roce_rdma_write_tagged(uint16_t neighbor_id, uint32_t rkey, uint64_t remote_offset, const void* local_buf, uint32_t len,
                                uint16_t cookie) -> int;

#ifdef WOS_SELFTEST
auto wki_roce_selftest_uninitialized_receive_preserves_region() -> bool;
#endif

}  // namespace ker::net::wki
