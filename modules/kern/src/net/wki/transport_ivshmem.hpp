#pragma once

#include <cstdint>

namespace ker::net::wki {

// Initialize the WKI ivshmem ordinary-message transport.
// Probes revision-1 PCI devices not already claimed by ivshmem_net.
// If a suitable device is found, registers it as a WkiTransport.
void wki_ivshmem_transport_init();
// Terminal closure: reject new callbacks, drain existing users and IRQs.
// BAR mappings and all escaped raw RDMA pointers remain pinned.
auto wki_ivshmem_transport_shutdown() -> int;

// Drain a bounded number of message-ring frames from task context. This is a
// required task path for both plain and doorbell deployments; IRQ never
// dispatches WKI protocol callbacks directly.
auto wki_ivshmem_transport_poll(uint16_t budget) -> uint16_t;

// Task-only cadence hint. A live admitted ordinary-message ring needs bounded
// polling even when no reliable deadline is pending; this grants no authority.
auto wki_ivshmem_transport_poll_required() -> bool;

#ifdef WOS_SELFTEST
auto wki_ivshmem_selftest_complete_frames() -> bool;
#endif

// Legacy raw BAR APIs fail closed. No authenticated service backing or
// remotely reusable authority is exposed by an ivshmem offset.
// Allocation always returns -1; pointer lookup always returns nullptr.
auto wki_ivshmem_rdma_alloc(uint32_t size) -> int64_t;

// No-op: raw offsets cannot allocate or recycle authority.
void wki_ivshmem_rdma_free(int64_t offset, uint32_t size);

// No BAR pointer is returned, including before initialization or after shutdown.
auto wki_ivshmem_rdma_ptr(int64_t offset) -> void*;

// Legacy mailbox publication is a no-op. Shared bytes cannot authorize IRQ
// callbacks; ordinary message TX retains only the hardware wake hint.
struct WkiTransport;
void wki_ivshmem_irq_mailbox_write(WkiTransport* transport, uint16_t device_id, uint16_t irq_vector, uint32_t irq_status);

}  // namespace ker::net::wki
