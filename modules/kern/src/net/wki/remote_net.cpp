#include "remote_net.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <deque>
#include <memory>
#include <net/netdevice.hpp>
#include <net/packet.hpp>
#include <net/wki/dev_proxy.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/dyn/kmalloc.hpp>

namespace ker::net::wki {

// ═══════════════════════════════════════════════════════════════════════════════
// Storage
// ═══════════════════════════════════════════════════════════════════════════════

namespace {

// unique_ptr indirection: ProxyNetState contains Spinlock with deleted move-assignment
std::deque<std::unique_ptr<ProxyNetState>> g_net_proxies;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_remote_net_initialized = false;                     // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto find_net_proxy_by_channel(uint16_t owner_node, uint16_t channel_id) -> ProxyNetState* {
    for (auto& p : g_net_proxies) {
        if (p->active && p->owner_node == owner_node && p->assigned_channel == channel_id) {
            return p.get();
        }
    }
    return nullptr;
}

auto find_net_proxy_by_attach(uint16_t owner_node) -> ProxyNetState* {
    for (auto& p : g_net_proxies) {
        if (p->attach_pending && p->owner_node == owner_node) {
            return p.get();
        }
    }
    return nullptr;
}

auto find_net_proxy_by_dev(ker::net::NetDevice* dev) -> ProxyNetState* {
    for (auto& p : g_net_proxies) {
        if (p->active && &p->netdev == dev) {
            return p.get();
        }
    }
    return nullptr;
}

// -----------------------------------------------------------------------------
// Consumer-side NetDeviceOps
// -----------------------------------------------------------------------------

auto proxy_net_open(ker::net::NetDevice* dev) -> int {
    (void)dev;
    return 0;
}

void proxy_net_close(ker::net::NetDevice* dev) { (void)dev; }

auto proxy_net_xmit(ker::net::NetDevice* dev, ker::net::PacketBuffer* pkt) -> int {
    auto* state = find_net_proxy_by_dev(dev);
    if (state == nullptr || !state->active || pkt == nullptr) {
        return -1;
    }

    // Fire-and-forget: send OP_NET_XMIT, don't wait for response
    // Request data: raw packet bytes (pkt->data, pkt->len)
    auto req_total = static_cast<uint16_t>(sizeof(DevOpReqPayload) + pkt->len);

    // Check MTU
    if (req_total > WKI_ETH_MAX_PAYLOAD) {
        return -1;
    }

    auto* req_buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(req_total));
    if (req_buf == nullptr) {
        return -1;
    }

    auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf);
    req->op_id = OP_NET_XMIT;
    req->data_len = static_cast<uint16_t>(pkt->len);
    memcpy(req_buf + sizeof(DevOpReqPayload), pkt->data, pkt->len);

    int send_ret = wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, req_buf, req_total);
    ker::mod::mm::dyn::kmalloc::free(req_buf);

    if (send_ret == WKI_OK) {
        dev->tx_packets++;
        dev->tx_bytes += pkt->len;
    } else {
        dev->tx_dropped++;
    }

    // Free the packet buffer (we've copied the data into the WKI message)
    ker::net::pkt_free(pkt);
    return (send_ret == WKI_OK) ? 0 : -1;
}

void proxy_net_set_mac(ker::net::NetDevice* dev, const uint8_t* mac) {
    auto* state = find_net_proxy_by_dev(dev);
    if (state == nullptr || !state->active || mac == nullptr) {
        return;
    }

    // Synchronous: send OP_NET_SET_MAC and wait for response
    auto req_total = static_cast<uint16_t>(sizeof(DevOpReqPayload) + 6);

    std::array<uint8_t, sizeof(DevOpReqPayload) + 6> req_buf{};

    auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf.data());
    req->op_id = OP_NET_SET_MAC;
    req->data_len = 6;
    memcpy(req_buf.data() + sizeof(DevOpReqPayload), mac, 6);

    state->lock.lock();
    state->op_pending = true;
    state->op_status = 0;
    state->lock.unlock();

    int send_ret = wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, req_buf.data(), req_total);

    if (send_ret != WKI_OK) {
        state->op_pending = false;
        return;
    }

    // Spin-wait for response with memory fence
    uint64_t deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;
    while (state->op_pending) {
        asm volatile("mfence" ::: "memory");
        if (!state->op_pending) {
            break;
        }
        if (wki_now_us() >= deadline) {
            state->op_pending = false;
            return;
        }
        for (int i = 0; i < 1000; i++) {
            asm volatile("pause" ::: "memory");
        }
    }

    // Update local MAC on success
    if (state->op_status == 0) {
        memcpy(dev->mac.data(), mac, 6);
    }
}

// Static NetDeviceOps for proxy NIC
const ker::net::NetDeviceOps g_proxy_net_ops = {
    .open = proxy_net_open,
    .close = proxy_net_close,
    .start_xmit = proxy_net_xmit,
    .set_mac = proxy_net_set_mac,
};

}  // namespace

// ═══════════════════════════════════════════════════════════════════════════════
// Init
// ═══════════════════════════════════════════════════════════════════════════════

void wki_remote_net_init() {
    if (g_remote_net_initialized) {
        return;
    }
    g_remote_net_initialized = true;
    ker::mod::dbg::log("[WKI] Remote NIC subsystem initialized");
}

// ═══════════════════════════════════════════════════════════════════════════════
// Server Side — NET Operation Handlers
// ═══════════════════════════════════════════════════════════════════════════════

namespace detail {

void handle_net_op(const WkiHeader* hdr, uint16_t channel_id, ker::net::NetDevice* net_dev, uint16_t op_id, const uint8_t* data,
                   uint16_t data_len) {
    switch (op_id) {
        case OP_NET_XMIT: {
            // Fire-and-forget: no response sent
            if (data_len == 0 || net_dev->ops == nullptr || net_dev->ops->start_xmit == nullptr) {
                return;
            }

            // Allocate PacketBuffer, copy data, call start_xmit
            ker::net::PacketBuffer* pkt = ker::net::pkt_alloc();
            if (pkt == nullptr) {
                return;
            }

            // Copy packet data into the buffer
            size_t copy_len = data_len;
            copy_len = std::min(copy_len, ker::net::PKT_BUF_SIZE - ker::net::PKT_HEADROOM);
            memcpy(pkt->data, data, copy_len);
            pkt->len = copy_len;
            pkt->dev = net_dev;

            int ret = net_dev->ops->start_xmit(net_dev, pkt);
            if (ret != 0) {
                ker::net::pkt_free(pkt);
            }
            // No response for fire-and-forget XMIT
            break;
        }

        case OP_NET_SET_MAC: {
            // Request: {mac:u8[6]}
            if (data_len < 6) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_NET_SET_MAC;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            if (net_dev->ops != nullptr && net_dev->ops->set_mac != nullptr) {
                net_dev->ops->set_mac(net_dev, data);
            }

            DevOpRespPayload resp = {};
            resp.op_id = OP_NET_SET_MAC;
            resp.status = 0;
            resp.data_len = 0;
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            break;
        }

        case OP_NET_RX_NOTIFY: {
            // D11: This op is sent by the server (owner) to the consumer.
            // It should not arrive at the server-side handler. Ignore.
            break;
        }

        case OP_NET_GET_STATS: {
            // D13: Response: {rx_pkt:u64, tx_pkt:u64, rx_bytes:u64, tx_bytes:u64, rx_drop:u64, tx_drop:u64} = 48 bytes
            constexpr uint16_t STATS_DATA_LEN = 48;
            std::array<uint8_t, sizeof(DevOpRespPayload) + STATS_DATA_LEN> resp_buf{};

            auto* resp = reinterpret_cast<DevOpRespPayload*>(resp_buf.data());
            resp->op_id = OP_NET_GET_STATS;
            resp->status = 0;
            resp->data_len = STATS_DATA_LEN;

            auto* stats_data = resp_buf.data() + sizeof(DevOpRespPayload);
            memcpy(stats_data, &net_dev->rx_packets, sizeof(uint64_t));
            memcpy(stats_data + 8, &net_dev->tx_packets, sizeof(uint64_t));
            memcpy(stats_data + 16, &net_dev->rx_bytes, sizeof(uint64_t));
            memcpy(stats_data + 24, &net_dev->tx_bytes, sizeof(uint64_t));
            memcpy(stats_data + 32, &net_dev->rx_dropped, sizeof(uint64_t));
            memcpy(stats_data + 40, &net_dev->tx_dropped, sizeof(uint64_t));

            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, resp_buf.data(),
                     static_cast<uint16_t>(sizeof(DevOpRespPayload) + STATS_DATA_LEN));
            break;
        }

        default: {
            DevOpRespPayload resp = {};
            resp.op_id = op_id;
            resp.status = -1;
            resp.data_len = 0;
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            break;
        }
    }
}

}  // namespace detail

// ═══════════════════════════════════════════════════════════════════════════════
// Consumer Side — Attach / Response Handlers
// ═══════════════════════════════════════════════════════════════════════════════

auto wki_remote_net_attach(uint16_t owner_node, uint32_t resource_id, const char* local_name) -> ker::net::NetDevice* {
    if (local_name == nullptr) {
        return nullptr;
    }

    // Allocate proxy state
    g_net_proxies.push_back(std::make_unique<ProxyNetState>());
    auto* state = g_net_proxies.back().get();

    state->owner_node = owner_node;
    state->resource_id = resource_id;

    // Send DEV_ATTACH_REQ
    DevAttachReqPayload attach_req = {};
    attach_req.target_node = owner_node;
    attach_req.resource_type = static_cast<uint16_t>(ResourceType::NET);
    attach_req.resource_id = resource_id;
    attach_req.attach_mode = static_cast<uint8_t>(AttachMode::PROXY);
    attach_req.requested_channel = 0;

    state->attach_pending = true;
    state->attach_status = 0;
    state->attach_channel = 0;

    int send_ret = wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_REQ, &attach_req, sizeof(attach_req));
    if (send_ret != WKI_OK) {
        g_net_proxies.pop_back();
        return nullptr;
    }

    // Spin-wait for attach ACK with memory fence
    uint64_t deadline = wki_now_us() + WKI_DEV_PROXY_TIMEOUT_US;
    while (state->attach_pending) {
        asm volatile("mfence" ::: "memory");
        if (!state->attach_pending) {
            break;
        }
        if (wki_now_us() >= deadline) {
            state->attach_pending = false;
            g_net_proxies.pop_back();
            ker::mod::dbg::log("[WKI] Remote NIC attach timeout: node=0x%04x res_id=%u", owner_node, resource_id);
            return nullptr;
        }
        for (int i = 0; i < 1000; i++) {
            asm volatile("pause" ::: "memory");
        }
    }

    if (state->attach_status != static_cast<uint8_t>(DevAttachStatus::OK)) {
        ker::mod::dbg::log("[WKI] Remote NIC attach rejected: status=%u", state->attach_status);
        g_net_proxies.pop_back();
        return nullptr;
    }

    state->assigned_channel = state->attach_channel;
    state->max_op_size = state->attach_max_op_size;
    state->active = true;

    // Populate proxy NetDevice
    size_t name_len = strlen(local_name);
    if (name_len >= ker::net::NETDEV_NAME_LEN) {
        name_len = ker::net::NETDEV_NAME_LEN - 1;
    }
    memcpy(state->netdev.name.data(), local_name, name_len);
    state->netdev.name[name_len] = '\0';
    state->netdev.ops = &g_proxy_net_ops;
    state->netdev.private_data = state;
    state->netdev.mtu = 1500;
    state->netdev.state = 1;  // up

    // Register in the netdev subsystem
    ker::net::netdev_register(&state->netdev);

    ker::mod::dbg::log("[WKI] Remote NIC attached: %s -> node=0x%04x res_id=%u ch=%u", local_name, owner_node, resource_id,
                       state->assigned_channel);

    return &state->netdev;
}

void wki_remote_net_detach(ker::net::NetDevice* proxy_dev) {
    auto* state = find_net_proxy_by_dev(proxy_dev);
    if (state == nullptr) {
        return;
    }

    // Send DEV_DETACH
    DevDetachPayload det = {};
    det.target_node = state->owner_node;
    det.resource_type = static_cast<uint16_t>(ResourceType::NET);
    det.resource_id = state->resource_id;
    wki_send(state->owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, &det, sizeof(det));

    // Close the dynamic channel
    WkiChannel* ch = wki_channel_get(state->owner_node, state->assigned_channel);
    if (ch != nullptr) {
        wki_channel_close(ch);
    }

    state->active = false;

    // Remove inactive
    for (auto it = g_net_proxies.begin(); it != g_net_proxies.end();) {
        if (!(*it)->active) {
            it = g_net_proxies.erase(it);
        } else {
            ++it;
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Consumer Side — RX Handlers
// ═══════════════════════════════════════════════════════════════════════════════

namespace detail {

void handle_net_attach_ack(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(DevAttachAckPayload)) {
        return;
    }

    const auto* ack = reinterpret_cast<const DevAttachAckPayload*>(payload);

    ProxyNetState* state = find_net_proxy_by_attach(hdr->src_node);
    if (state == nullptr) {
        return;
    }

    state->attach_status = ack->status;
    state->attach_channel = ack->assigned_channel;
    state->attach_max_op_size = ack->max_op_size;

    asm volatile("" ::: "memory");
    state->attach_pending = false;
}

void handle_net_op_resp(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(DevOpRespPayload)) {
        return;
    }

    const auto* resp = reinterpret_cast<const DevOpRespPayload*>(payload);
    const uint8_t* resp_data = payload + sizeof(DevOpRespPayload);
    uint16_t resp_data_len = resp->data_len;

    if (sizeof(DevOpRespPayload) + resp_data_len > payload_len) {
        return;
    }

    // Find NET proxy by (src_node, channel_id)
    ProxyNetState* state = find_net_proxy_by_channel(hdr->src_node, hdr->channel_id);
    if (state == nullptr || !state->op_pending) {
        return;
    }

    state->lock.lock();
    state->op_status = resp->status;

    // D13: For GET_STATS, update proxy NIC counters directly (no caller waiting)
    constexpr uint16_t STATS_RESP_LEN = 48;
    if (resp->op_id == OP_NET_GET_STATS && resp->status == 0 && resp_data_len >= STATS_RESP_LEN) {
        memcpy(&state->netdev.rx_packets, resp_data, sizeof(uint64_t));
        memcpy(&state->netdev.tx_packets, resp_data + 8, sizeof(uint64_t));
        memcpy(&state->netdev.rx_bytes, resp_data + 16, sizeof(uint64_t));
        memcpy(&state->netdev.tx_bytes, resp_data + 24, sizeof(uint64_t));
        memcpy(&state->netdev.rx_dropped, resp_data + 32, sizeof(uint64_t));
        memcpy(&state->netdev.tx_dropped, resp_data + 40, sizeof(uint64_t));
    } else if (resp_data_len > 0 && state->op_resp_buf != nullptr) {
        // Copy response data for other synchronous ops (e.g. SET_MAC)
        uint16_t copy_len = (resp_data_len > state->op_resp_max) ? state->op_resp_max : resp_data_len;
        memcpy(state->op_resp_buf, resp_data, copy_len);
        state->op_resp_len = copy_len;
    } else {
        state->op_resp_len = 0;
    }

    state->lock.unlock();

    asm volatile("" ::: "memory");
    state->op_pending = false;
}

// D11: Consumer receives forwarded packets from the owner NIC
void handle_net_rx_notify(const WkiHeader* hdr, const uint8_t* data, uint16_t data_len) {
    if (data_len == 0) {
        return;
    }

    ProxyNetState* state = find_net_proxy_by_channel(hdr->src_node, hdr->channel_id);
    if (state == nullptr || !state->active) {
        return;
    }

    ker::net::PacketBuffer* pkt = ker::net::pkt_alloc();
    if (pkt == nullptr) {
        state->netdev.rx_dropped++;
        return;
    }

    size_t copy_len = std::min<size_t>(data_len, ker::net::PKT_BUF_SIZE - ker::net::PKT_HEADROOM);
    memcpy(pkt->data, data, copy_len);
    pkt->len = copy_len;
    pkt->dev = &state->netdev;

    // Feed into the local network stack
    ker::net::netdev_rx(&state->netdev, pkt);
}

}  // namespace detail

// ═══════════════════════════════════════════════════════════════════════════════
// D13: Periodic stats polling (non-blocking, called from timer tick)
// ═══════════════════════════════════════════════════════════════════════════════

void wki_remote_net_poll_stats() {
    if (!g_remote_net_initialized) {
        return;
    }

    for (auto& p : g_net_proxies) {
        if (!p->active) {
            continue;
        }

        // Skip if another operation is already pending
        if (p->op_pending) {
            continue;
        }

        // Send OP_NET_GET_STATS request (fire-and-forget; response handled
        // inline by handle_net_op_resp which updates netdev stats directly)
        DevOpReqPayload req = {};
        req.op_id = OP_NET_GET_STATS;
        req.data_len = 0;

        p->op_pending = true;
        p->op_status = 0;
        p->op_resp_buf = nullptr;
        p->op_resp_len = 0;

        int send_ret = wki_send(p->owner_node, p->assigned_channel, MsgType::DEV_OP_REQ, &req, sizeof(req));
        if (send_ret != WKI_OK) {
            p->op_pending = false;
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Fencing Cleanup
// ═══════════════════════════════════════════════════════════════════════════════

void wki_remote_net_cleanup_for_peer(uint16_t node_id) {
    for (auto& p : g_net_proxies) {
        if (!p->active || p->owner_node != node_id) {
            continue;
        }

        if (p->op_pending) {
            p->op_status = -1;
            p->op_pending = false;
        }

        WkiChannel* ch = wki_channel_get(p->owner_node, p->assigned_channel);
        if (ch != nullptr) {
            wki_channel_close(ch);
        }

        ker::mod::dbg::log("[WKI] Remote NIC proxy fenced: node=0x%04x", node_id);
        p->active = false;
    }

    for (auto it = g_net_proxies.begin(); it != g_net_proxies.end();) {
        if (!(*it)->active) {
            it = g_net_proxies.erase(it);
        } else {
            ++it;
        }
    }
}

}  // namespace ker::net::wki
