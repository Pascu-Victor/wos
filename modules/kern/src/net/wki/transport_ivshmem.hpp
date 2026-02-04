#pragma once

#include <cstdint>

namespace ker::net::wki {

// Initialize the WKI ivshmem RDMA transport.
// Probes PCI for ivshmem-plain devices not already claimed by ivshmem_net.
// If a suitable device is found, registers it as a WkiTransport.
void wki_ivshmem_transport_init();

// Allocate a region from the ivshmem RDMA pool.
// Returns the offset within the RDMA region, or -1 on failure.
// The offset can be used as an rkey for RDMA operations.
auto wki_ivshmem_rdma_alloc(uint32_t size) -> int64_t;

// Free a previously allocated RDMA region.
void wki_ivshmem_rdma_free(int64_t offset, uint32_t size);

// Get a local virtual pointer for an RDMA offset within the shared memory.
auto wki_ivshmem_rdma_ptr(int64_t offset) -> void*;

// D4: Write IRQ forwarding data to the shared-memory mailbox.
// Called before ringing the doorbell so the peer can decode the IRQ info.
struct WkiTransport;
void wki_ivshmem_irq_mailbox_write(WkiTransport* transport,
                                    uint16_t device_id, uint16_t irq_vector,
                                    uint32_t irq_status);

}  // namespace ker::net::wki
