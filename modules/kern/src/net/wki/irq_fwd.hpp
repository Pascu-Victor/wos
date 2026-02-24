#pragma once

#include <cstdint>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// IRQ Forwarding Binding — one per registered remote interrupt
// -----------------------------------------------------------------------------

using IrqFwdHandlerFn = void (*)(uint8_t vector, void* data);

struct IrqFwdBinding {
    bool active = false;
    uint16_t remote_node = WKI_NODE_INVALID;
    uint16_t device_id = 0;
    uint16_t remote_vector = 0;
    uint8_t local_vector = 0;
    IrqFwdHandlerFn handler = nullptr;
    void* handler_data = nullptr;

    // D4: Doorbell optimization for RDMA-zone peers (direct ivshmem neighbors)
    bool use_doorbell = false;
    WkiTransport* doorbell_transport = nullptr;
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Initialize the IRQ forwarding subsystem. Called from wki_init().
void wki_irq_fwd_init();

// Register a local handler for a forwarded remote interrupt.
// Allocates a local vector and associates it with the remote (node, device_id, vector).
// Returns the allocated local vector, or 0 on failure.
auto wki_irq_fwd_register(uint16_t remote_node, uint16_t device_id, uint16_t remote_vector, IrqFwdHandlerFn handler, void* data) -> uint8_t;

// Unregister an IRQ forwarding binding by local vector.
void wki_irq_fwd_unregister(uint8_t local_vector);

// Send a DEV_IRQ_FWD message to a remote node (fire-and-forget).
void wki_irq_fwd_send(uint16_t dst_node, uint16_t device_id, uint16_t irq_vector, uint32_t irq_status);

// Remove all IRQ forwarding bindings for a fenced peer.
void wki_irq_fwd_cleanup_for_peer(uint16_t node_id);

// D4: Called from ivshmem IRQ handler when a doorbell-based IRQ forward arrives.
// Decodes the mailbox data and invokes the registered handler.
void wki_irq_fwd_doorbell_rx(uint16_t src_node, uint16_t device_id, uint16_t irq_vector, uint32_t irq_status);

// -----------------------------------------------------------------------------
// Internal — RX message handler (called from wki.cpp dispatch)
// -----------------------------------------------------------------------------

namespace detail {

void handle_dev_irq_fwd(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

}  // namespace detail

}  // namespace ker::net::wki
