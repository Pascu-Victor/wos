#include "remote_net.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <deque>
#include <memory>
#include <net/netdevice.hpp>
#include <net/netif.hpp>
#include <net/packet.hpp>
#include <net/route.hpp>
#include <net/wki/dev_proxy.hpp>
#include <net/wki/dev_server.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sched/scheduler.hpp>

namespace ker::net::wki {

// ═══════════════════════════════════════════════════════════════════════════════
// Storage
// ═══════════════════════════════════════════════════════════════════════════════

namespace {

// unique_ptr indirection: ProxyNetState contains Spinlock with deleted move-assignment
std::deque<std::unique_ptr<ProxyNetState>> g_net_proxies;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_remote_net_initialized = false;                     // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_net_proxy_lock;                  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// s_net_proxy_lock must be held by caller
auto find_net_proxy_by_channel(uint16_t owner_node, uint16_t channel_id) -> ProxyNetState* {
    for (auto& p : g_net_proxies) {
        if (p->active && p->owner_node == owner_node && p->assigned_channel == channel_id) {
            return p.get();
        }
    }
    return nullptr;
}

// s_net_proxy_lock must be held by caller
auto find_net_proxy_by_attach(uint16_t owner_node) -> ProxyNetState* {
    for (auto& p : g_net_proxies) {
        if (p->attach_pending && p->owner_node == owner_node) {
            return p.get();
        }
    }
    return nullptr;
}

// s_net_proxy_lock must be held by caller
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
    s_net_proxy_lock.lock();
    auto* state = find_net_proxy_by_dev(dev);
    if (state == nullptr || !state->active) {
        s_net_proxy_lock.unlock();
        return -1;
    }
    s_net_proxy_lock.unlock();

    // V2: Send OP_NET_OPEN to owner node
    DevOpReqPayload req = {};
    req.op_id = OP_NET_OPEN;
    req.data_len = 0;

    WkiWaitEntry wait = {};
    state->lock.lock();
    state->op_wait_entry = &wait;
    state->op_pending.store(true, std::memory_order_release);
    state->op_status = 0;
    state->lock.unlock();

    int send_ret = wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, &req, sizeof(req));
    if (send_ret != WKI_OK) {
        state->op_wait_entry = nullptr;
        state->op_pending.store(false, std::memory_order_relaxed);
        return -1;
    }

    int wait_rc = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    state->op_wait_entry = nullptr;
    if (wait_rc == WKI_ERR_TIMEOUT) {
        state->op_pending.store(false, std::memory_order_relaxed);
        ker::mod::dbg::log("[WKI] proxy_net_open timeout: node=0x%04x", state->owner_node);
        return -1;
    }

    if (state->op_status == 0) {
        dev->state = 1;

        // V2: Send initial RX credit grant to server
        state->rx_credits_remaining = WKI_NET_RX_CREDITS;
        std::array<uint8_t, sizeof(DevOpReqPayload) + sizeof(uint16_t)> credit_buf{};
        auto* credit_req = reinterpret_cast<DevOpReqPayload*>(credit_buf.data());
        credit_req->op_id = OP_NET_RX_CREDIT;
        credit_req->data_len = sizeof(uint16_t);
        memcpy(credit_buf.data() + sizeof(DevOpReqPayload), &state->rx_credits_remaining, sizeof(uint16_t));
        wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, credit_buf.data(),
                 static_cast<uint16_t>(sizeof(DevOpReqPayload) + sizeof(uint16_t)));

        ker::mod::dbg::log("[WKI] proxy_net_open success: %s", dev->name.data());
    }
    return state->op_status;
}

void proxy_net_close(ker::net::NetDevice* dev) {
    s_net_proxy_lock.lock();
    auto* state = find_net_proxy_by_dev(dev);
    if (state == nullptr || !state->active) {
        s_net_proxy_lock.unlock();
        return;
    }
    s_net_proxy_lock.unlock();

    // V2: Send OP_NET_CLOSE to owner node
    DevOpReqPayload req = {};
    req.op_id = OP_NET_CLOSE;
    req.data_len = 0;

    WkiWaitEntry wait = {};
    state->lock.lock();
    state->op_wait_entry = &wait;
    state->op_pending.store(true, std::memory_order_release);
    state->op_status = 0;
    state->lock.unlock();

    int send_ret = wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, &req, sizeof(req));
    if (send_ret != WKI_OK) {
        state->op_wait_entry = nullptr;
        state->op_pending.store(false, std::memory_order_relaxed);
        dev->state = 0;
        return;
    }

    int wait_rc = wki_wait_for_op(&wait, WKI_OP_TIMEOUT_US);
    state->op_wait_entry = nullptr;
    if (wait_rc == WKI_ERR_TIMEOUT) {
        state->op_pending.store(false, std::memory_order_relaxed);
        ker::mod::dbg::log("[WKI] proxy_net_close timeout: node=0x%04x", state->owner_node);
    }

    dev->state = 0;
    ker::mod::dbg::log("[WKI] proxy_net_close: %s", dev->name.data());
}

auto proxy_net_xmit(ker::net::NetDevice* dev, ker::net::PacketBuffer* pkt) -> int {
    s_net_proxy_lock.lock();
    auto* state = find_net_proxy_by_dev(dev);
    if (state == nullptr || !state->active || pkt == nullptr) {
        s_net_proxy_lock.unlock();
        return -1;
    }
    s_net_proxy_lock.unlock();

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
    s_net_proxy_lock.lock();
    auto* state = find_net_proxy_by_dev(dev);
    if (state == nullptr || !state->active || mac == nullptr) {
        s_net_proxy_lock.unlock();
        return;
    }
    s_net_proxy_lock.unlock();

    // Synchronous: send OP_NET_SET_MAC and wait for response
    auto req_total = static_cast<uint16_t>(sizeof(DevOpReqPayload) + 6);

    std::array<uint8_t, sizeof(DevOpReqPayload) + 6> req_buf{};

    auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf.data());
    req->op_id = OP_NET_SET_MAC;
    req->data_len = 6;
    memcpy(req_buf.data() + sizeof(DevOpReqPayload), mac, 6);

    WkiWaitEntry wait = {};
    state->lock.lock();
    state->op_wait_entry = &wait;
    state->op_pending.store(true, std::memory_order_release);
    state->op_status = 0;
    state->lock.unlock();

    int send_ret = wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, req_buf.data(), req_total);

    if (send_ret != WKI_OK) {
        state->op_wait_entry = nullptr;
        state->op_pending.store(false, std::memory_order_relaxed);
        return;
    }

    int wait_rc = wki_wait_for_op(&wait, WKI_DEV_PROXY_TIMEOUT_US);
    state->op_wait_entry = nullptr;
    if (wait_rc == WKI_ERR_TIMEOUT) {
        state->op_pending.store(false, std::memory_order_relaxed);
        ker::mod::dbg::log("[WKI] proxy_net_set_mac timeout: node=0x%04x", state->owner_node);
        return;
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
                   uint16_t data_len, void* binding_ptr) {
    // V2: Access DevServerBinding for credit tracking
    auto* binding = static_cast<DevServerBinding*>(binding_ptr);

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

        case OP_NET_OPEN: {
            // V2: Bring up the NIC and subscribe consumer to RX forwarding
            int status = 0;
            if (net_dev->ops != nullptr && net_dev->ops->open != nullptr) {
                status = net_dev->ops->open(net_dev);
            }

            // Mark the binding as opened for RX forwarding
            if (binding != nullptr && status == 0) {
                binding->net_nic_opened = true;
                // Accept broadcast/multicast by default once opened
                binding->net_rx_filter.accept_broadcast = true;
                binding->net_rx_filter.accept_multicast = true;
            }

            DevOpRespPayload resp = {};
            resp.op_id = OP_NET_OPEN;
            resp.status = static_cast<int16_t>(status);
            resp.data_len = 0;
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            break;
        }

        case OP_NET_CLOSE: {
            // V2: Shut down NIC and unsubscribe consumer from RX forwarding
            if (net_dev->ops != nullptr && net_dev->ops->close != nullptr) {
                net_dev->ops->close(net_dev);
            }

            // Mark the binding as closed — stop RX forwarding for this consumer
            if (binding != nullptr) {
                binding->net_nic_opened = false;
            }

            DevOpRespPayload resp = {};
            resp.op_id = OP_NET_CLOSE;
            resp.status = 0;
            resp.data_len = 0;
            wki_send(hdr->src_node, channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            break;
        }

        case OP_NET_RX_CREDIT: {
            // V2: Consumer replenishes RX forwarding credits
            if (binding != nullptr && data_len >= sizeof(uint16_t)) {
                uint16_t credits = 0;
                memcpy(&credits, data, sizeof(uint16_t));
                binding->net_rx_credits += credits;
            }
            // No response — fire-and-forget credit replenishment
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

    // Allocate proxy state (lock protects deque mutation)
    s_net_proxy_lock.lock();
    g_net_proxies.push_back(std::make_unique<ProxyNetState>());
    auto* state = g_net_proxies.back().get();
    state->owner_node = owner_node;
    state->resource_id = resource_id;
    s_net_proxy_lock.unlock();

    // Send DEV_ATTACH_REQ
    DevAttachReqPayload attach_req = {};
    attach_req.target_node = owner_node;
    attach_req.resource_type = static_cast<uint16_t>(ResourceType::NET);
    attach_req.resource_id = resource_id;
    attach_req.attach_mode = static_cast<uint8_t>(AttachMode::PROXY);
    attach_req.requested_channel = 0;

    WkiWaitEntry wait = {};
    state->attach_wait_entry = &wait;
    state->attach_pending.store(true, std::memory_order_release);
    state->attach_status = 0;
    state->attach_channel = 0;

    int send_ret = wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_REQ, &attach_req, sizeof(attach_req));
    if (send_ret != WKI_OK) {
        state->attach_wait_entry = nullptr;
        s_net_proxy_lock.lock();
        g_net_proxies.pop_back();
        s_net_proxy_lock.unlock();
        return nullptr;
    }

    int wait_rc = wki_wait_for_op(&wait, WKI_DEV_PROXY_TIMEOUT_US);
    state->attach_wait_entry = nullptr;
    if (wait_rc == WKI_ERR_TIMEOUT) {
        state->attach_pending.store(false, std::memory_order_relaxed);
        s_net_proxy_lock.lock();
        g_net_proxies.pop_back();
        s_net_proxy_lock.unlock();
        ker::mod::dbg::log("[WKI] Remote NIC attach timeout: node=0x%04x res_id=%u", owner_node, resource_id);
        return nullptr;
    }

    if (state->attach_status != static_cast<uint8_t>(DevAttachStatus::OK)) {
        ker::mod::dbg::log("[WKI] Remote NIC attach rejected: status=%u", state->attach_status);
        s_net_proxy_lock.lock();
        g_net_proxies.pop_back();
        s_net_proxy_lock.unlock();
        return nullptr;
    }

    state->assigned_channel = state->attach_channel;
    state->max_op_size = state->attach_max_op_size;
    state->active = true;

    // V2: Populate proxy NetDevice with proper name and MAC
    // Name: "wki-<hostname>" truncated to NETDEV_NAME_LEN-1
    size_t name_len = strlen(local_name);
    if (name_len >= ker::net::NETDEV_NAME_LEN) {
        name_len = ker::net::NETDEV_NAME_LEN - 1;
    }
    memcpy(state->netdev.name.data(), local_name, name_len);
    state->netdev.name[name_len] = '\0';

    // V2: Locally-administered MAC: 02:57:4B:xx:yy:zz
    // 0x57,0x4B = "WK", xx:yy = local node_id (big-endian), zz = sequential index
    state->netdev.mac[0] = 0x02;
    state->netdev.mac[1] = 0x57;
    state->netdev.mac[2] = 0x4B;
    state->netdev.mac[3] = static_cast<uint8_t>((g_wki.my_node_id >> 8) & 0xFF);
    state->netdev.mac[4] = static_cast<uint8_t>(g_wki.my_node_id & 0xFF);
    state->netdev.mac[5] = g_wki.proxy_nic_index++;

    state->netdev.ops = &g_proxy_net_ops;
    state->netdev.private_data = state;
    state->netdev.mtu = 1500;
    state->netdev.state = static_cast<uint8_t>(state->owner_link_state != 0 ? 1 : 0);

    // V2: Initialize RX backpressure credits
    state->rx_credits_remaining = WKI_NET_RX_CREDITS;

    // Register in the netdev subsystem
    ker::net::netdev_register(&state->netdev);

    ker::mod::dbg::log("[WKI] Remote NIC attached: %s -> node=0x%04x res_id=%u ch=%u mac=%02x:%02x:%02x:%02x:%02x:%02x", local_name,
                       owner_node, resource_id, state->assigned_channel, state->netdev.mac[0], state->netdev.mac[1], state->netdev.mac[2],
                       state->netdev.mac[3], state->netdev.mac[4], state->netdev.mac[5]);

    return &state->netdev;
}

void wki_remote_net_detach(ker::net::NetDevice* proxy_dev) {
    uint16_t owner_node{};
    uint32_t resource_id{};
    uint16_t assigned_channel{};

    // Lock: find proxy, copy info, mark inactive, erase from deque
    s_net_proxy_lock.lock();
    auto* state = find_net_proxy_by_dev(proxy_dev);
    if (state == nullptr) {
        s_net_proxy_lock.unlock();
        return;
    }

    owner_node = state->owner_node;
    resource_id = state->resource_id;
    assigned_channel = state->assigned_channel;
    state->active = false;

    for (auto it = g_net_proxies.begin(); it != g_net_proxies.end();) {
        if (!(*it)->active) {
            it = g_net_proxies.erase(it);
        } else {
            ++it;
        }
    }
    s_net_proxy_lock.unlock();

    // Send DEV_DETACH outside lock
    DevDetachPayload det = {};
    det.target_node = owner_node;
    det.resource_type = static_cast<uint16_t>(ResourceType::NET);
    det.resource_id = resource_id;
    wki_send(owner_node, WKI_CHAN_RESOURCE, MsgType::DEV_DETACH, &det, sizeof(det));

    // Close the dynamic channel outside lock
    WkiChannel* ch = wki_channel_get(owner_node, assigned_channel);
    if (ch != nullptr) {
        wki_channel_close(ch);
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

    s_net_proxy_lock.lock();
    ProxyNetState* state = find_net_proxy_by_attach(hdr->src_node);
    s_net_proxy_lock.unlock();
    if (state == nullptr) {
        return;
    }

    state->attach_status = ack->status;
    state->attach_channel = ack->assigned_channel;
    state->attach_max_op_size = ack->max_op_size;

    // V2: Extract NET extension fields if present
    if (payload_len >= sizeof(DevAttachAckNetPayload)) {
        const auto* net_ack = reinterpret_cast<const DevAttachAckNetPayload*>(payload);
        state->owner_ipv4_addr = net_ack->ipv4_addr;
        state->owner_ipv4_mask = net_ack->ipv4_mask;
        state->owner_real_mac = net_ack->real_mac;
        state->owner_link_state = net_ack->link_state;
    }

    state->attach_pending.store(false, std::memory_order_release);
    if (state->attach_wait_entry != nullptr) {
        wki_wake_op(state->attach_wait_entry, 0);
    }
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
    s_net_proxy_lock.lock();
    ProxyNetState* state = find_net_proxy_by_channel(hdr->src_node, hdr->channel_id);
    s_net_proxy_lock.unlock();
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

    state->op_pending.store(false, std::memory_order_release);
    if (state->op_wait_entry != nullptr) {
        wki_wake_op(state->op_wait_entry, 0);
    }
}

// D11: Consumer receives forwarded packets from the owner NIC
void handle_net_rx_notify(const WkiHeader* hdr, const uint8_t* data, uint16_t data_len) {
    if (data_len == 0) {
        return;
    }

    s_net_proxy_lock.lock();
    ProxyNetState* state = find_net_proxy_by_channel(hdr->src_node, hdr->channel_id);
    s_net_proxy_lock.unlock();
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

    // V2: Replenish RX credits periodically
    // Send a credit batch every 16 packets to amortize overhead
    state->rx_credits_remaining--;
    if (state->rx_credits_remaining <= (WKI_NET_RX_CREDITS / 4)) {
        uint16_t replenish = WKI_NET_RX_CREDITS - state->rx_credits_remaining;
        state->rx_credits_remaining = WKI_NET_RX_CREDITS;

        std::array<uint8_t, sizeof(DevOpReqPayload) + sizeof(uint16_t)> credit_buf{};
        auto* credit_req = reinterpret_cast<DevOpReqPayload*>(credit_buf.data());
        credit_req->op_id = OP_NET_RX_CREDIT;
        credit_req->data_len = sizeof(uint16_t);
        memcpy(credit_buf.data() + sizeof(DevOpReqPayload), &replenish, sizeof(uint16_t));
        wki_send(state->owner_node, state->assigned_channel, MsgType::DEV_OP_REQ, credit_buf.data(),
                 static_cast<uint16_t>(sizeof(DevOpReqPayload) + sizeof(uint16_t)));
    }
}

}  // namespace detail

// ═══════════════════════════════════════════════════════════════════════════════
// D13: Periodic stats polling (non-blocking, called from timer tick)
// ═══════════════════════════════════════════════════════════════════════════════

void wki_remote_net_poll_stats() {
    if (!g_remote_net_initialized) {
        return;
    }

    // Collect proxies to poll under lock
    struct PollTarget {
        ProxyNetState* state;
        uint16_t owner_node;
        uint16_t channel;
    };
    constexpr size_t MAX_POLL_TARGETS = 32;
    std::array<PollTarget, MAX_POLL_TARGETS> targets{};
    size_t target_count = 0;

    s_net_proxy_lock.lock();
    for (auto& p : g_net_proxies) {
        if (!p->active || p->op_pending) {
            continue;
        }
        if (target_count < MAX_POLL_TARGETS) {
            targets[target_count++] = {p.get(), p->owner_node, p->assigned_channel};
        }
    }
    s_net_proxy_lock.unlock();

    // Send stats requests outside lock
    for (size_t i = 0; i < target_count; i++) {
        auto* st = targets[i].state;

        DevOpReqPayload req = {};
        req.op_id = OP_NET_GET_STATS;
        req.data_len = 0;

        st->op_pending = true;
        st->op_status = 0;
        st->op_resp_buf = nullptr;
        st->op_resp_len = 0;

        int send_ret = wki_send(targets[i].owner_node, targets[i].channel, MsgType::DEV_OP_REQ, &req, sizeof(req));
        if (send_ret != WKI_OK) {
            st->op_pending = false;
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Fencing Cleanup
// ═══════════════════════════════════════════════════════════════════════════════

void wki_remote_net_cleanup_for_peer(uint16_t node_id) {
    // Collect channels to close under lock
    struct CleanupEntry {
        uint16_t owner_node;
        uint16_t channel;
    };
    constexpr size_t MAX_CLEANUP = 32;
    std::array<CleanupEntry, MAX_CLEANUP> to_close{};
    size_t close_count = 0;

    s_net_proxy_lock.lock();
    for (auto& p : g_net_proxies) {
        if (!p->active || p->owner_node != node_id) {
            continue;
        }

        if (p->op_pending.load(std::memory_order_acquire)) {
            p->op_status = -1;
            p->op_pending.store(false, std::memory_order_release);
            if (p->op_wait_entry != nullptr) {
                wki_wake_op(p->op_wait_entry, -1);
            }
            if (p->attach_wait_entry != nullptr) {
                wki_wake_op(p->attach_wait_entry, -1);
            }
        }

        if (close_count < MAX_CLEANUP) {
            to_close[close_count++] = {p->owner_node, p->assigned_channel};
        }

        p->active = false;
    }

    for (auto it = g_net_proxies.begin(); it != g_net_proxies.end();) {
        if (!(*it)->active) {
            it = g_net_proxies.erase(it);
        } else {
            ++it;
        }
    }
    s_net_proxy_lock.unlock();

    // Close channels and log outside lock
    for (size_t i = 0; i < close_count; i++) {
        WkiChannel* ch = wki_channel_get(to_close[i].owner_node, to_close[i].channel);
        if (ch != nullptr) {
            wki_channel_close(ch);
        }
        ker::mod::dbg::log("[WKI] Remote NIC proxy fenced: node=0x%04x", node_id);
    }
}

}  // namespace ker::net::wki
