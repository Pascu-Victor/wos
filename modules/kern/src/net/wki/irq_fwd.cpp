#include "irq_fwd.hpp"

#include <deque>
#include <net/wki/transport_ivshmem.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Storage
// -----------------------------------------------------------------------------

namespace {
std::deque<IrqFwdBinding> g_irq_bindings;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_irq_fwd_initialized = false;        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
}  // namespace

// -----------------------------------------------------------------------------
// Init
// -----------------------------------------------------------------------------

void wki_irq_fwd_init() {
    if (g_irq_fwd_initialized) {
        return;
    }
    g_irq_fwd_initialized = true;
    ker::mod::dbg::log("[WKI] IRQ forwarding subsystem initialized");
}

// -----------------------------------------------------------------------------
// Register / Unregister
// -----------------------------------------------------------------------------

auto wki_irq_fwd_register(uint16_t remote_node, uint16_t device_id, uint16_t remote_vector, IrqFwdHandlerFn handler, void* data)
    -> uint8_t {
    if (!g_irq_fwd_initialized || handler == nullptr) {
        return 0;
    }

    // Allocate a local vector number (not a real hardware vector — just an ID
    // for the binding table). Start from 1 and find the next unused.
    uint8_t local_vec = 1;
    for (const auto& b : g_irq_bindings) {
        if (b.active && b.local_vector >= local_vec) {
            local_vec = b.local_vector + 1;
        }
    }
    if (local_vec == 0) {
        // Overflow (255 bindings — unlikely)
        return 0;
    }

    IrqFwdBinding binding;
    binding.active = true;
    binding.remote_node = remote_node;
    binding.device_id = device_id;
    binding.remote_vector = remote_vector;
    binding.local_vector = local_vec;
    binding.handler = handler;
    binding.handler_data = data;

    // D4: Check if the peer is reachable via an RDMA-capable transport with doorbell support
    WkiPeer* peer = wki_peer_find(remote_node);
    if (peer != nullptr && peer->transport != nullptr && peer->transport->rdma_capable && peer->transport->doorbell != nullptr) {
        binding.use_doorbell = true;
        binding.doorbell_transport = peer->transport;
    }

    g_irq_bindings.push_back(binding);

    ker::mod::dbg::log("[WKI] IRQ fwd registered: node=0x%04x dev=%u vec=%u -> local_vec=%u doorbell=%d", remote_node, device_id,
                       remote_vector, local_vec, static_cast<int>(binding.use_doorbell));

    return local_vec;
}

void wki_irq_fwd_unregister(uint8_t local_vector) {
    std::erase_if(g_irq_bindings, [local_vector](const IrqFwdBinding& b) { return b.local_vector == local_vector; });
}

// -----------------------------------------------------------------------------
// Send — fire-and-forget IRQ forward to a remote node
// -----------------------------------------------------------------------------

void wki_irq_fwd_send(uint16_t dst_node, uint16_t device_id, uint16_t irq_vector, uint32_t irq_status) {
    if (!g_irq_fwd_initialized) {
        return;
    }

    // D4: Check if we can use doorbell path for this (device_id, dst_node) pair
    for (const auto& b : g_irq_bindings) {
        if (b.active && b.use_doorbell && b.remote_node == dst_node && b.device_id == device_id && b.remote_vector == irq_vector &&
            b.doorbell_transport != nullptr) {
            // Use the transport's shared-memory mailbox + doorbell for near-zero latency.
            // Encode: (irq_vector << 16) | (irq_status & 0xFFFF) as the doorbell value.
            // The ivshmem transport writes this to a shared-memory mailbox slot before
            // ringing the doorbell, so the peer can decode it.
            wki_ivshmem_irq_mailbox_write(b.doorbell_transport, device_id, irq_vector, irq_status);
            b.doorbell_transport->doorbell(b.doorbell_transport, dst_node, 0);
            return;
        }
    }

    // Fallback: send via WKI message (works for any transport, including routed peers)
    DevIrqFwdPayload fwd = {};
    fwd.device_id = device_id;
    fwd.irq_vector = irq_vector;
    fwd.irq_status = irq_status;

    // Send on RESOURCE channel with PRIORITY flag (via reliable send).
    // The PRIORITY flag is set by the channel's priority class — RESOURCE channel
    // defaults to LATENCY which sets PRIORITY automatically.
    wki_send(dst_node, WKI_CHAN_RESOURCE, MsgType::DEV_IRQ_FWD, &fwd, sizeof(fwd));
}

// -----------------------------------------------------------------------------
// Fencing cleanup
// -----------------------------------------------------------------------------

void wki_irq_fwd_cleanup_for_peer(uint16_t node_id) {
    std::erase_if(g_irq_bindings, [node_id](const IrqFwdBinding& b) { return b.remote_node == node_id; });
}

// -----------------------------------------------------------------------------
// RX handler
// -----------------------------------------------------------------------------

namespace detail {

void handle_dev_irq_fwd(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(DevIrqFwdPayload)) {
        return;
    }

    const auto* fwd = reinterpret_cast<const DevIrqFwdPayload*>(payload);

    // Find matching binding
    for (const auto& b : g_irq_bindings) {
        if (!b.active) {
            continue;
        }
        if (b.remote_node != hdr->src_node) {
            continue;
        }
        if (b.device_id != fwd->device_id) {
            continue;
        }
        if (b.remote_vector != fwd->irq_vector) {
            continue;
        }

        // Invoke the handler
        if (b.handler != nullptr) {
            b.handler(b.local_vector, b.handler_data);
        }
        return;
    }

    // No matching binding — ignore silently
}

}  // namespace detail

// -----------------------------------------------------------------------------
// D4: Doorbell-based IRQ forwarding RX
// Called from ivshmem ISR when the shared-memory mailbox has data.
// -----------------------------------------------------------------------------

void wki_irq_fwd_doorbell_rx(uint16_t /*src_node*/, uint16_t device_id, uint16_t irq_vector, uint32_t irq_status) {
    (void)irq_status;  // available for future use

    // Match by (device_id, irq_vector) — with ivshmem there's only one peer,
    // so src_node is redundant. All doorbell bindings are for direct ivshmem peers.
    for (const auto& b : g_irq_bindings) {
        if (!b.active || !b.use_doorbell) {
            continue;
        }
        if (b.device_id != device_id || b.remote_vector != irq_vector) {
            continue;
        }

        if (b.handler != nullptr) {
            b.handler(b.local_vector, b.handler_data);
        }
        return;
    }
}

}  // namespace ker::net::wki
