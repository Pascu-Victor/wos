#pragma once

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

    volatile bool op_pending = false;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
    int16_t op_status = 0;
    void* op_resp_buf = nullptr;  // D13: response data buffer for synchronous ops
    uint16_t op_resp_len = 0;
    uint16_t op_resp_max = 0;

    volatile bool attach_pending = false;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
    uint8_t attach_status = 0;
    uint16_t attach_channel = 0;
    uint16_t attach_max_op_size = 0;

    ker::net::NetDevice netdev;
    ker::mod::sys::Spinlock lock;
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Initialize the remote NIC subsystem. Called from wki_init().
void wki_remote_net_init();

// Consumer side: attach to a remote NIC and register a proxy NetDevice
auto wki_remote_net_attach(uint16_t owner_node, uint32_t resource_id, const char* local_name) -> ker::net::NetDevice*;

// Consumer side: detach from a remote NIC
void wki_remote_net_detach(ker::net::NetDevice* proxy_dev);

// Fencing cleanup — remove all state for a fenced peer
void wki_remote_net_cleanup_for_peer(uint16_t node_id);

// D13: Poll stats from real NIC and update proxy counters (called from timer tick)
void wki_remote_net_poll_stats();

// -----------------------------------------------------------------------------
// Internal — RX message handlers
// -----------------------------------------------------------------------------

namespace detail {

// Server side: handle NET operations (called from dev_server handle_dev_op_req)
void handle_net_op(const WkiHeader* hdr, uint16_t channel_id, ker::net::NetDevice* net_dev, uint16_t op_id, const uint8_t* data,
                   uint16_t data_len);

// Consumer side: handle DEV_OP_RESP for NET proxy (SET_MAC, GET_STATS)
void handle_net_op_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

// D11: Consumer side: handle OP_NET_RX_NOTIFY (owner forwarding received packets)
void handle_net_rx_notify(const WkiHeader* hdr, const uint8_t* data, uint16_t data_len);

// Consumer side: handle DEV_ATTACH_ACK for NET proxy
void handle_net_attach_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

}  // namespace detail

}  // namespace ker::net::wki
