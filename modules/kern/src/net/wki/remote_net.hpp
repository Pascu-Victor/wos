#pragma once

#include <atomic>
#include <cstdint>
#include <net/netdevice.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// ProxyNetState (consumer side) — per-remote-NIC proxy state
// -----------------------------------------------------------------------------

struct ProxyNetState {
    bool active = false;
    uint16_t owner_node = WKI_NODE_INVALID;
    uint16_t assigned_channel = 0;
    uint32_t resource_id = 0;
    uint16_t max_op_size = 0;

    std::atomic<bool> op_pending{false};
    int16_t op_status = 0;
    void* op_resp_buf = nullptr;  // D13: response data buffer for synchronous ops
    uint16_t op_resp_len = 0;
    uint16_t op_resp_max = 0;
    WkiWaitEntry* op_wait_entry = nullptr;  // V2 I-4: async wait for DEV_OP_RESP

    std::atomic<bool> attach_pending{false};
    uint8_t attach_status = 0;
    uint16_t attach_channel = 0;
    uint16_t attach_max_op_size = 0;
    WkiWaitEntry* attach_wait_entry = nullptr;  // V2 I-4: async wait for DEV_ATTACH_ACK

    // V2: Extended attach info from NET ACK [V2§A5.3]
    uint32_t owner_ipv4_addr = 0;
    uint32_t owner_ipv4_mask = 0;
    std::array<uint8_t, 6> owner_real_mac = {};
    uint16_t owner_link_state = 0;

    // V2: RX backpressure credit tracking [V2§A5.6]
    uint16_t rx_credits_remaining = 0;  // credits granted to server (server-side tracking)

    net::NetDevice netdev;
    mod::sys::Spinlock lock;
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Initialize the remote NIC subsystem. Called from wki_init().
void wki_remote_net_init();

// Consumer side: attach to a remote NIC and register a proxy NetDevice
auto wki_remote_net_attach(uint16_t owner_node, uint32_t resource_id, const char* local_name) -> net::NetDevice*;

// Consumer side: detach from a remote NIC
void wki_remote_net_detach(net::NetDevice* proxy_dev);

// Fencing cleanup — remove all state for a fenced peer
void wki_remote_net_cleanup_for_peer(uint16_t node_id);

// D13: Poll stats from real NIC and update proxy counters (called from timer tick)
void wki_remote_net_poll_stats();

// -----------------------------------------------------------------------------
// Internal — RX message handlers
// -----------------------------------------------------------------------------

namespace detail {

// Server side: handle NET operations (called from dev_server handle_dev_op_req)
void handle_net_op(const WkiHeader* hdr, uint16_t channel_id, net::NetDevice* net_dev, uint16_t op_id, const uint8_t* data,
                   uint16_t data_len, void* binding_ptr = nullptr);

// Consumer side: handle DEV_OP_RESP for NET proxy (SET_MAC, GET_STATS)
void handle_net_op_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

// D11: Consumer side: handle OP_NET_RX_NOTIFY (owner forwarding received packets)
void handle_net_rx_notify(const WkiHeader* hdr, const uint8_t* data, uint16_t data_len);

// Consumer side: handle DEV_ATTACH_ACK for NET proxy
void handle_net_attach_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

}  // namespace detail

}  // namespace ker::net::wki
