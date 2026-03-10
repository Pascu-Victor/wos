#include "dev_server.hpp"

#include <array>
#include <cstring>
#include <deque>
#include <dev/block_device.hpp>
#include <net/netdevice.hpp>
#include <net/netif.hpp>
#include <net/packet.hpp>
#include <net/wki/blk_ring.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/remote_net.hpp>
#include <net/wki/remote_vfs.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <net/wki/zone.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Storage
// -----------------------------------------------------------------------------

namespace {
std::deque<DevServerBinding> g_bindings;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_dev_server_initialized = false;    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock s_server_lock;    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// s_server_lock must be held by caller
auto find_binding_by_channel(uint16_t consumer_node, uint16_t channel_id) -> DevServerBinding* {
    for (auto& b : g_bindings) {
        if (b.active && b.consumer_node == consumer_node && b.assigned_channel == channel_id) {
            return &b;
        }
    }
    return nullptr;
}

// s_server_lock must be held by caller
auto find_binding_by_zone_id(uint32_t zone_id) -> DevServerBinding* {
    for (auto& b : g_bindings) {
        if (b.active && b.blk_rdma_active && b.blk_zone_id == zone_id) {
            return &b;
        }
    }
    return nullptr;
}

auto find_block_device_by_resource_id(uint32_t resource_id) -> ker::dev::BlockDevice* {
    size_t count = ker::dev::block_device_count();
    for (size_t i = 0; i < count; i++) {
        ker::dev::BlockDevice* bdev = ker::dev::block_device_at(i);
        if (bdev != nullptr && static_cast<uint32_t>(bdev->minor) == resource_id) {
            return bdev;
        }
    }
    return nullptr;
}

auto find_net_device_by_resource_id(uint32_t resource_id) -> ker::net::NetDevice* {
    size_t count = ker::net::netdev_count();
    for (size_t i = 0; i < count; i++) {
        ker::net::NetDevice* ndev = ker::net::netdev_at(i);
        if (ndev != nullptr && ndev->ifindex == resource_id) {
            return ndev;
        }
    }
    return nullptr;
}

}  // namespace

// -----------------------------------------------------------------------------
// Init
// -----------------------------------------------------------------------------

void wki_dev_server_init() {
    if (g_dev_server_initialized) {
        return;
    }
    g_dev_server_initialized = true;
    ker::mod::dbg::log("[WKI] Dev server subsystem initialized");
}

// -----------------------------------------------------------------------------
// D11: Check if any active NET binding still references a given device
// -----------------------------------------------------------------------------

namespace {

// s_server_lock must be held by caller
auto has_net_binding_for_dev(ker::net::NetDevice* dev) -> bool {
    for (const auto& b : g_bindings) {
        if (b.active && b.resource_type == ResourceType::NET && b.net_dev == dev) {
            return true;
        }
    }
    return false;
}

}  // namespace

// -----------------------------------------------------------------------------
// D11: RX forward — called from netdev_rx() on the owner's NIC
// -----------------------------------------------------------------------------

void wki_dev_server_forward_net_rx(ker::net::NetDevice* dev, ker::net::PacketBuffer* pkt) {
    if (dev == nullptr || pkt == nullptr || pkt->len == 0) {
        return;
    }

    // D12: Determine packet type from destination MAC (first 6 bytes of Ethernet frame)
    bool is_broadcast = false;
    bool is_multicast = false;
    if (pkt->len >= 6) {
        is_broadcast = (pkt->data[0] == 0xFF && pkt->data[1] == 0xFF && pkt->data[2] == 0xFF && pkt->data[3] == 0xFF &&
                        pkt->data[4] == 0xFF && pkt->data[5] == 0xFF);
        is_multicast = (!is_broadcast) && ((pkt->data[0] & 0x01) != 0);
    }

    // Collect matching bindings under the lock, then send outside
    struct RxTarget {
        uint16_t consumer_node;
        uint16_t assigned_channel;
    };
    constexpr size_t MAX_RX_TARGETS = 32;
    std::array<RxTarget, MAX_RX_TARGETS> targets = {};
    size_t target_count = 0;

    s_server_lock.lock();
    for (auto& b : g_bindings) {
        if (!b.active || b.resource_type != ResourceType::NET || b.net_dev != dev) {
            continue;
        }
        if (!b.net_nic_opened) {
            continue;
        }
        if (b.net_rx_credits == 0) {
            b.net_dev->rx_dropped++;
            continue;
        }
        b.net_rx_credits--;
        if (is_broadcast && !b.net_rx_filter.accept_broadcast) {
            continue;
        }
        if (is_multicast && !b.net_rx_filter.accept_multicast) {
            continue;
        }
        if (target_count < MAX_RX_TARGETS) {
            targets[target_count++] = {b.consumer_node, b.assigned_channel};
        }
    }
    s_server_lock.unlock();

    // Build and send OP_NET_RX_NOTIFY outside the lock (fire-and-forget)
    auto pkt_data_len = static_cast<uint16_t>(pkt->len);
    auto req_total = static_cast<uint16_t>(sizeof(DevOpReqPayload) + pkt_data_len);
    if (req_total > WKI_ETH_MAX_PAYLOAD) {
        return;  // Packet too large for a single WKI message
    }

    for (size_t i = 0; i < target_count; i++) {
        auto* req_buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(req_total));
        if (req_buf == nullptr) {
            continue;
        }
        auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf);
        req->op_id = OP_NET_RX_NOTIFY;
        req->data_len = pkt_data_len;
        memcpy(req_buf + sizeof(DevOpReqPayload), pkt->data, pkt_data_len);
        wki_send(targets[i].consumer_node, targets[i].assigned_channel, MsgType::DEV_OP_REQ, req_buf, req_total);
        ker::mod::mm::dyn::kmalloc::free(req_buf);
    }
}

// -----------------------------------------------------------------------------
// Fencing cleanup
// -----------------------------------------------------------------------------

void wki_dev_server_detach_all_for_peer(uint16_t node_id) {
    // Collect work items and cleanup info under the lock
    struct DetachWork {
        uint32_t blk_zone_id;
        bool blk_rdma_active;
        ker::dev::BlockDevice* block_dev;
        ker::net::NetDevice* net_dev;
        uint16_t consumer_node;
        uint16_t assigned_channel;
    };
    constexpr size_t MAX_DETACH = 32;
    std::array<DetachWork, MAX_DETACH> work = {};
    size_t work_count = 0;

    constexpr size_t MAX_NET_DEVS = 16;
    std::array<ker::net::NetDevice*, MAX_NET_DEVS> uninstall_devs = {};
    size_t uninstall_count = 0;

    s_server_lock.lock();
    for (auto& b : g_bindings) {
        if (!b.active || b.consumer_node != node_id) {
            continue;
        }
        if (work_count < MAX_DETACH) {
            work[work_count++] = {.blk_zone_id = b.blk_zone_id,
                                  .blk_rdma_active = b.blk_rdma_active,
                                  .block_dev = b.block_dev,
                                  .net_dev = b.net_dev,
                                  .consumer_node = b.consumer_node,
                                  .assigned_channel = b.assigned_channel};
        }
        b.active = false;
    }
    // TODO: RAII for this \/
    //  Free VFS RDMA write buffers before erasing (DevServerBinding has no destructor)
    for (auto& b : g_bindings) {
        if (!b.active && b.vfs_rdma_write_buf != nullptr) {
            ker::mod::mm::dyn::kmalloc::free(b.vfs_rdma_write_buf);
            b.vfs_rdma_write_buf = nullptr;
        }
    }

    // Remove inactive entries
    std::erase_if(g_bindings, [](const DevServerBinding& b) { return !b.active; });

    // Determine which NET devices need their RX forward hook uninstalled
    for (size_t i = 0; i < work_count; i++) {
        if (work[i].net_dev == nullptr) {
            continue;
        }
        bool dup = false;
        for (size_t j = 0; j < uninstall_count; j++) {
            if (uninstall_devs[j] == work[i].net_dev) {
                dup = true;
                break;
            }
        }
        if (!dup && uninstall_count < MAX_NET_DEVS && !has_net_binding_for_dev(work[i].net_dev)) {
            uninstall_devs[uninstall_count++] = work[i].net_dev;
        }
    }
    s_server_lock.unlock();

    // Perform cleanup outside the lock
    for (size_t i = 0; i < work_count; i++) {
        if (work[i].blk_rdma_active && work[i].blk_zone_id != 0) {
            wki_zone_destroy(work[i].blk_zone_id);
        }
        if (work[i].block_dev != nullptr && work[i].block_dev->remotable != nullptr) {
            work[i].block_dev->remotable->on_remote_fault(node_id);
        }
        if (work[i].net_dev != nullptr && work[i].net_dev->remotable != nullptr) {
            work[i].net_dev->remotable->on_remote_fault(node_id);
        }
        WkiChannel* ch = wki_channel_get(work[i].consumer_node, work[i].assigned_channel);
        if (ch != nullptr) {
            wki_channel_close(ch);
        }
    }

    // D11: Uninstall RX forward hooks for NET devices that no longer have bindings
    for (size_t i = 0; i < uninstall_count; i++) {
        uninstall_devs[i]->wki_rx_forward = nullptr;
    }
}

// -----------------------------------------------------------------------------
// RX handlers
// -----------------------------------------------------------------------------

namespace detail {

void handle_dev_attach_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(DevAttachReqPayload)) {
        return;
    }

    const auto* req = reinterpret_cast<const DevAttachReqPayload*>(payload);

    // Prepare response
    DevAttachAckPayload ack = {};

    auto res_type = static_cast<ResourceType>(req->resource_type);

    if (res_type == ResourceType::BLOCK) {
        // Find the block device
        ker::dev::BlockDevice* bdev = find_block_device_by_resource_id(req->resource_id);
        if (bdev == nullptr) {
            ack.status = static_cast<uint8_t>(DevAttachStatus::NOT_FOUND);
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
            return;
        }

        // Check remotable
        if (bdev->remotable == nullptr || !bdev->remotable->can_remote()) {
            ack.status = static_cast<uint8_t>(DevAttachStatus::NOT_REMOTABLE);
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
            return;
        }

        // Allocate a dynamic channel for this binding
        WkiChannel* ch = wki_channel_alloc(hdr->src_node, PriorityClass::THROUGHPUT);
        if (ch == nullptr) {
            ack.status = static_cast<uint8_t>(DevAttachStatus::BUSY);
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
            return;
        }

        // Call on_remote_attach
        int attach_ret = bdev->remotable->on_remote_attach(hdr->src_node);
        if (attach_ret != 0) {
            wki_channel_close(ch);
            ack.status = static_cast<uint8_t>(DevAttachStatus::BUSY);
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
            return;
        }

        // Create binding
        DevServerBinding binding;
        binding.active = true;
        binding.consumer_node = hdr->src_node;
        binding.assigned_channel = ch->channel_id;
        binding.resource_type = ResourceType::BLOCK;
        binding.resource_id = req->resource_id;
        binding.block_dev = bdev;

        // Compute the RDMA zone ID for the block ring.  Zone creation is
        // deferred to the timer tick (wki_dev_server_process_pending_zones)
        // because wki_zone_create() blocks on a spin-wait for the zone ACK,
        // and we are currently inside the NAPI poll handler — calling
        // napi_poll_inline() re-entrantly returns 0, so the ACK can never
        // be received here.  Instead, send the ACK optimistically with the
        // zone_id; the consumer already has a timeout loop waiting for the
        // zone to appear.
        uint32_t blk_zone_id = (static_cast<uint32_t>(hdr->src_node) << 16) | req->resource_id;
        binding.blk_zone_id = blk_zone_id;
        binding.blk_zone_pending = true;  // processed by wki_dev_server_process_pending_zones()
        binding.blk_rdma_active = false;  // not yet — set when deferred creation succeeds

        s_server_lock.lock();
        g_bindings.push_back(std::move(binding));
        s_server_lock.unlock();

        // Send success ACK with the proposed RDMA zone info.
        // The consumer will wait for the zone to appear + server_ready.
        ack.status = static_cast<uint8_t>(DevAttachStatus::OK);
        ack.assigned_channel = ch->channel_id;
        ack.max_op_size = static_cast<uint16_t>(WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload));
        ack.rdma_flags = DEV_ATTACH_RDMA_BLK_RING;
        ack.blk_zone_id = blk_zone_id;

        // Advertise streaming bulk RDMA transfer support when the peer has an
        // RDMA-capable transport (ivshmem or RoCE).  The consumer will allocate
        // and register a staging buffer for direct large-range transfers.
        {
            WkiPeer* peer = wki_peer_find(hdr->src_node);
            if (peer != nullptr && peer->rdma_transport != nullptr) {
                ack.rdma_flags |= DEV_ATTACH_RDMA_BULK;
            }
        }

        ker::mod::dbg::log("[WKI] Dev attach: node=0x%04x res_id=%u ch=%u rdma=deferred zone=0x%08x", hdr->src_node, req->resource_id,
                           ch->channel_id, blk_zone_id);

        // Send ACK on WKI_CHAN_RESOURCE (the same channel the request arrived on) so that
        // the piggybacked ACK properly drains the client's retransmit queue for channel 3.
        // The dynamic channel ID is communicated via ack.assigned_channel in the payload.
        int ack_ret = wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
        if (ack_ret != WKI_OK) {
            ker::mod::dbg::log("[WKI] Dev attach ACK send failed: node=0x%04x err=%d", hdr->src_node, ack_ret);
        }
    } else if (res_type == ResourceType::VFS) {
        // Find the VFS export
        VfsExport* exp = wki_remote_vfs_find_export(req->resource_id);
        if (exp == nullptr) {
            ack.status = static_cast<uint8_t>(DevAttachStatus::NOT_FOUND);
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
            return;
        }

        // Allocate a dynamic channel
        WkiChannel* ch = wki_channel_alloc(hdr->src_node, PriorityClass::THROUGHPUT);
        if (ch == nullptr) {
            ack.status = static_cast<uint8_t>(DevAttachStatus::BUSY);
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
            return;
        }

        // Create binding with VFS export path
        DevServerBinding binding;
        binding.active = true;
        binding.consumer_node = hdr->src_node;
        binding.assigned_channel = ch->channel_id;
        binding.resource_type = ResourceType::VFS;
        binding.resource_id = req->resource_id;
        memcpy(static_cast<void*>(binding.vfs_export_path), static_cast<const void*>(exp->export_path), sizeof(binding.vfs_export_path));

        // RDMA-backed VFS writes: pre-register a server-side receive buffer so the
        // consumer can rdma_write file data here, avoiding embedding data in messages.
        // rdma_register_region is a local-only operation — safe to call in NAPI context.
        {
            WkiPeer* peer = wki_peer_find(hdr->src_node);
            if (peer != nullptr && peer->rdma_transport != nullptr && peer->rdma_transport->rdma_register_region != nullptr) {
                constexpr uint32_t VFS_WRITE_BUF = 65536;
                auto* wbuf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(VFS_WRITE_BUF));
                if (wbuf != nullptr) {
                    uint32_t rkey = 0;
                    int reg_ret = peer->rdma_transport->rdma_register_region(peer->rdma_transport, reinterpret_cast<uint64_t>(wbuf),
                                                                             VFS_WRITE_BUF, &rkey);
                    if (reg_ret == 0 && rkey != 0) {
                        binding.vfs_rdma_write_buf = wbuf;
                        binding.vfs_rdma_write_rkey = rkey;
                        ack.rdma_flags |= DEV_ATTACH_RDMA_VFS;
                        ack.blk_zone_id = rkey;  // carry server write-recv rkey to consumer
                    } else {
                        ker::mod::mm::dyn::kmalloc::free(wbuf);
                    }
                }
            }
        }

        s_server_lock.lock();
        g_bindings.push_back(std::move(binding));
        s_server_lock.unlock();

        ack.status = static_cast<uint8_t>(DevAttachStatus::OK);
        ack.assigned_channel = ch->channel_id;
        ack.max_op_size = static_cast<uint16_t>(WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload));

        ker::mod::dbg::log("[WKI] VFS attach: node=0x%04x res_id=%u ch=%u path=%s rdma=%s", hdr->src_node, req->resource_id, ch->channel_id,
                           static_cast<const char*>(exp->export_path), ((ack.rdma_flags & DEV_ATTACH_RDMA_VFS) != 0) ? "yes" : "no");

        // Send ACK on WKI_CHAN_RESOURCE (the same channel the request arrived on) so that
        // the piggybacked ACK properly drains the client's retransmit queue for channel 3.
        // The dynamic channel ID is communicated via ack.assigned_channel in the payload.
        int ack_ret = wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
        if (ack_ret != WKI_OK) {
            ker::mod::dbg::log("[WKI] VFS attach ACK send failed: node=0x%04x err=%d", hdr->src_node, ack_ret);
        }
    } else if (res_type == ResourceType::NET) {
        // Find the net device
        ker::net::NetDevice* ndev = find_net_device_by_resource_id(req->resource_id);
        if (ndev == nullptr) {
            ack.status = static_cast<uint8_t>(DevAttachStatus::NOT_FOUND);
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
            return;
        }

        // Check remotable
        if (ndev->remotable == nullptr || !ndev->remotable->can_remote()) {
            ack.status = static_cast<uint8_t>(DevAttachStatus::NOT_REMOTABLE);
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
            return;
        }

        // Allocate a dynamic channel
        WkiChannel* ch = wki_channel_alloc(hdr->src_node, PriorityClass::THROUGHPUT);
        if (ch == nullptr) {
            ack.status = static_cast<uint8_t>(DevAttachStatus::BUSY);
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
            return;
        }

        // Call on_remote_attach
        int attach_ret = ndev->remotable->on_remote_attach(hdr->src_node);
        if (attach_ret != 0) {
            wki_channel_close(ch);
            ack.status = static_cast<uint8_t>(DevAttachStatus::BUSY);
            wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
            return;
        }

        // Create binding
        DevServerBinding binding;
        binding.active = true;
        binding.consumer_node = hdr->src_node;
        binding.assigned_channel = ch->channel_id;
        binding.resource_type = ResourceType::NET;
        binding.resource_id = req->resource_id;
        binding.net_dev = ndev;

        s_server_lock.lock();
        g_bindings.push_back(std::move(binding));
        s_server_lock.unlock();

        // D11: Install RX forward hook on the NIC so received packets are forwarded
        ndev->wki_rx_forward = wki_dev_server_forward_net_rx;

        // V2: Send extended NET attach ACK with owner NIC info
        DevAttachAckNetPayload net_ack = {};
        net_ack.status = static_cast<uint8_t>(DevAttachStatus::OK);
        net_ack.assigned_channel = ch->channel_id;
        net_ack.max_op_size = static_cast<uint16_t>(WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload));
        net_ack.real_mac = ndev->mac;
        net_ack.link_state = ndev->state;

        // Populate IPv4 info from the network interface config
        auto* nif = ker::net::netif_get(ndev);
        if (nif != nullptr && nif->ipv4_addr_count > 0) {
            net_ack.ipv4_addr = nif->ipv4_addrs[0].addr;
            net_ack.ipv4_mask = nif->ipv4_addrs[0].netmask;
        }

        ker::mod::dbg::log("[WKI] NET attach: node=0x%04x res_id=%u ch=%u ip=0x%08x mask=0x%08x", hdr->src_node, req->resource_id,
                           ch->channel_id, net_ack.ipv4_addr, net_ack.ipv4_mask);

        int ack_ret = wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &net_ack, sizeof(net_ack));
        if (ack_ret != WKI_OK) {
            ker::mod::dbg::log("[WKI] NET attach ACK send failed: node=0x%04x err=%d", hdr->src_node, ack_ret);
        }
    } else {
        ack.status = static_cast<uint8_t>(DevAttachStatus::NOT_FOUND);
        wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
    }
}

void handle_dev_detach(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(DevDetachPayload)) {
        return;
    }

    const auto* det = reinterpret_cast<const DevDetachPayload*>(payload);

    // Collect binding info under the lock, erase from deque, then cleanup outside
    struct DetachInfo {
        uint32_t blk_zone_id;
        bool blk_rdma_active;
        ker::dev::BlockDevice* block_dev;
        ker::net::NetDevice* net_dev;
        uint16_t consumer_node;
        uint16_t assigned_channel;
        bool found;
        bool uninstall_rx_forward;
    };
    DetachInfo info = {};

    s_server_lock.lock();
    for (auto it = g_bindings.begin(); it != g_bindings.end(); ++it) {
        if (!it->active || it->consumer_node != hdr->src_node || it->resource_id != det->resource_id) {
            continue;
        }
        info.found = true;
        info.blk_zone_id = it->blk_zone_id;
        info.blk_rdma_active = it->blk_rdma_active;
        info.block_dev = it->block_dev;
        info.net_dev = it->net_dev;
        info.consumer_node = it->consumer_node;
        info.assigned_channel = it->assigned_channel;
        g_bindings.erase(it);
        if (info.net_dev != nullptr) {
            info.uninstall_rx_forward = !has_net_binding_for_dev(info.net_dev);
        }
        break;
    }
    s_server_lock.unlock();

    if (!info.found) {
        return;
    }

    // Cleanup outside the lock
    if (info.blk_rdma_active && info.blk_zone_id != 0) {
        wki_zone_destroy(info.blk_zone_id);
    }
    if (info.block_dev != nullptr && info.block_dev->remotable != nullptr) {
        info.block_dev->remotable->on_remote_detach(hdr->src_node);
    }
    if (info.net_dev != nullptr && info.net_dev->remotable != nullptr) {
        info.net_dev->remotable->on_remote_detach(hdr->src_node);
    }
    WkiChannel* ch = wki_channel_get(info.consumer_node, info.assigned_channel);
    if (ch != nullptr) {
        wki_channel_close(ch);
    }
    ker::mod::dbg::log("[WKI] Dev detach: node=0x%04x res_id=%u", hdr->src_node, det->resource_id);
    if (info.uninstall_rx_forward) {
        info.net_dev->wki_rx_forward = nullptr;
    }
}

void handle_dev_op_req(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(DevOpReqPayload)) {
        return;
    }

    const auto* req = reinterpret_cast<const DevOpReqPayload*>(payload);
    const uint8_t* req_data = payload + sizeof(DevOpReqPayload);
    uint16_t req_data_len = req->data_len;

    // Verify data fits
    if (sizeof(DevOpReqPayload) + req_data_len > payload_len) {
        return;
    }

    // Find binding by (src_node, channel_id) — pointer is stable (std::deque reference stability)
    s_server_lock.lock();
    DevServerBinding* binding = find_binding_by_channel(hdr->src_node, hdr->channel_id);
    s_server_lock.unlock();

    if (binding == nullptr) {
        // D11: OP_NET_RX_NOTIFY is sent server→consumer on the dynamic channel.
        // The consumer has a proxy (not a server binding), so route to the
        // consumer-side handler instead of returning an error.
        if (req->op_id == OP_NET_RX_NOTIFY) {
            detail::handle_net_rx_notify(hdr, req_data, req_data_len);
            return;
        }

        // Send error response
        DevOpRespPayload resp = {};
        resp.op_id = req->op_id;
        resp.status = -1;
        resp.data_len = 0;
        wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
        return;
    }

    // Dispatch VFS operations to remote_vfs handler.
    // Upper bound is OP_VFS_READ_BULK (0x0413), the highest VFS op code.
    // OP_VFS_READ_RDMA (0x0410), OP_VFS_WRITE_RDMA (0x0411),
    // OP_VFS_READDIR_BATCH (0x0412), and OP_VFS_READ_BULK (0x0413) all
    // sit above OP_VFS_SEEK_END (0x040E).
    if (req->op_id >= OP_VFS_OPEN && req->op_id <= OP_VFS_READ_BULK) {
        detail::handle_vfs_op(hdr, hdr->channel_id, static_cast<const char*>(binding->vfs_export_path), req->op_id, req_data, req_data_len);
        return;
    }

    // Dispatch NET operations to remote_net handler
    if (req->op_id >= OP_NET_XMIT && req->op_id <= OP_NET_RX_CREDIT) {
        if (binding->net_dev == nullptr) {
            DevOpRespPayload resp = {};
            resp.op_id = req->op_id;
            resp.status = -1;
            resp.data_len = 0;
            wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            return;
        }
        detail::handle_net_op(hdr, hdr->channel_id, binding->net_dev, req->op_id, req_data, req_data_len, binding);
        return;
    }

    // Block device operations require block_dev
    if (binding->block_dev == nullptr) {
        DevOpRespPayload resp = {};
        resp.op_id = req->op_id;
        resp.status = -1;
        resp.data_len = 0;
        wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
        return;
    }

    ker::dev::BlockDevice* bdev = binding->block_dev;

    switch (req->op_id) {
        case OP_BLOCK_INFO: {
            // Response: {block_size:u64, total_blocks:u64}
            constexpr uint16_t INFO_DATA_LEN = 16;
            std::array<uint8_t, sizeof(DevOpRespPayload) + INFO_DATA_LEN> buf = {};

            auto* resp = reinterpret_cast<DevOpRespPayload*>(buf.data());
            resp->op_id = OP_BLOCK_INFO;
            resp->status = 0;
            resp->data_len = INFO_DATA_LEN;

            auto* info_data = buf.data() + sizeof(DevOpRespPayload);
            uint64_t bs = bdev->block_size;
            uint64_t tb = bdev->total_blocks;
            memcpy(info_data, &bs, sizeof(uint64_t));
            memcpy(info_data + sizeof(uint64_t), &tb, sizeof(uint64_t));

            wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, buf.data(),
                     static_cast<uint16_t>(sizeof(DevOpRespPayload) + INFO_DATA_LEN));
            break;
        }

        case OP_BLOCK_READ: {
            // Request data: {lba:u64, count:u32} = 12 bytes
            if (req_data_len < 12) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_BLOCK_READ;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            uint64_t lba = 0;
            uint32_t count = 0;
            memcpy(&lba, req_data, sizeof(uint64_t));
            memcpy(&count, req_data + sizeof(uint64_t), sizeof(uint32_t));

            // Allocate read buffer
            auto data_bytes = static_cast<uint32_t>(count * bdev->block_size);
            auto max_resp_data = static_cast<uint16_t>(WKI_ETH_MAX_PAYLOAD - sizeof(DevOpRespPayload));

            // Clamp to max payload
            if (data_bytes > max_resp_data) {
                data_bytes = max_resp_data;
                count = data_bytes / static_cast<uint32_t>(bdev->block_size);
                if (count == 0) {
                    DevOpRespPayload resp = {};
                    resp.op_id = OP_BLOCK_READ;
                    resp.status = -1;
                    resp.data_len = 0;
                    wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                    break;
                }
                data_bytes = count * static_cast<uint32_t>(bdev->block_size);
            }

            auto resp_total = static_cast<uint16_t>(sizeof(DevOpRespPayload) + data_bytes);
            auto* buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(resp_total));
            if (buf == nullptr) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_BLOCK_READ;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            auto* resp = reinterpret_cast<DevOpRespPayload*>(buf);
            uint8_t* read_buf = buf + sizeof(DevOpRespPayload);

            int ret = ker::dev::block_read(bdev, lba, count, read_buf);

            resp->op_id = OP_BLOCK_READ;
            resp->status = static_cast<int16_t>(ret);
            resp->data_len = (ret == 0) ? static_cast<uint16_t>(data_bytes) : 0;
            resp->reserved = 0;

            uint16_t send_len = (ret == 0) ? resp_total : static_cast<uint16_t>(sizeof(DevOpRespPayload));
            wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, buf, send_len);

            ker::mod::mm::dyn::kmalloc::free(buf);
            break;
        }

        case OP_BLOCK_WRITE: {
            // Request data: {lba:u64, count:u32, data[...]} = 12 + data bytes
            if (req_data_len < 12) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_BLOCK_WRITE;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            uint64_t lba = 0;
            uint32_t count = 0;
            memcpy(&lba, req_data, sizeof(uint64_t));
            memcpy(&count, req_data + sizeof(uint64_t), sizeof(uint32_t));

            const uint8_t* write_data = req_data + 12;
            auto write_data_len = static_cast<uint16_t>(req_data_len - 12);

            // Validate data length
            auto expected = static_cast<uint32_t>(count * bdev->block_size);
            if (write_data_len < expected) {
                DevOpRespPayload resp = {};
                resp.op_id = OP_BLOCK_WRITE;
                resp.status = -1;
                resp.data_len = 0;
                wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
                break;
            }

            int ret = ker::dev::block_write(bdev, lba, count, write_data);

            DevOpRespPayload resp = {};
            resp.op_id = OP_BLOCK_WRITE;
            resp.status = static_cast<int16_t>(ret);
            resp.data_len = 0;
            wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            break;
        }

        case OP_BLOCK_FLUSH: {
            int ret = ker::dev::block_flush(bdev);

            DevOpRespPayload resp = {};
            resp.op_id = OP_BLOCK_FLUSH;
            resp.status = static_cast<int16_t>(ret);
            resp.data_len = 0;
            wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            break;
        }

        default: {
            // Unknown op
            mod::dbg::log("[WKI] Unknown block op: node=0x%04x ch=%u op_id=0x%04x", hdr->src_node, hdr->channel_id, req->op_id);
            DevOpRespPayload resp = {};
            resp.op_id = req->op_id;
            resp.status = -1;
            resp.data_len = 0;
            wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            break;
        }
    }
}

}  // namespace detail

// -----------------------------------------------------------------------------
// Block RDMA ring — tiered signaling (server → consumer)
// -----------------------------------------------------------------------------

namespace {

void blk_ring_signal_consumer(DevServerBinding* binding) {
    WkiPeer* peer = wki_peer_find(binding->consumer_node);
    if (peer == nullptr) {
        return;
    }

    // Tier 1: ivshmem doorbell (near-zero latency)
    if (peer->transport != nullptr && peer->transport->doorbell != nullptr) {
        peer->transport->doorbell(peer->transport, binding->consumer_node, binding->blk_zone_id);
        return;
    }

    // Tier 2: RoCE doorbell (if RDMA overlay transport has doorbell)
    if (peer->rdma_transport != nullptr && peer->rdma_transport->doorbell != nullptr) {
        peer->rdma_transport->doorbell(peer->rdma_transport, binding->consumer_node, binding->blk_zone_id);
        return;
    }

    // Tier 3: WKI ZONE_NOTIFY_POST message (reliable, higher latency)
    ZoneNotifyPayload notify = {};
    notify.zone_id = binding->blk_zone_id;
    notify.op_type = 0;  // completion notification
    wki_send(binding->consumer_node, WKI_CHAN_ZONE_MGMT, MsgType::ZONE_NOTIFY_POST, &notify, sizeof(notify));
}

}  // namespace

// -----------------------------------------------------------------------------
// Block RDMA ring — SQ poll (server side)
// -----------------------------------------------------------------------------

namespace {

// RoCE helper: push CQ entries, data slots, and updated header to proxy.
// Only the single new CQ entry is pushed instead of the entire CQ region to
// minimise RDMA bytes and frame count.
void roce_push_completions(DevServerBinding* binding, uint32_t data_slot, uint32_t data_bytes, uint32_t cq_idx) {
    if (!binding->blk_roce || binding->blk_rdma_transport == nullptr) {
        return;
    }
    auto* hdr = blk_ring_header(binding->blk_zone_ptr);

    // Push the data slot (block data for reads) to the proxy
    if (data_bytes > 0 && data_slot < hdr->data_slot_count) {
        uint32_t slot_offset = blk_ring_data_offset(hdr->sq_depth, hdr->cq_depth) + (data_slot * hdr->data_slot_size);
        binding->blk_rdma_transport->rdma_write(binding->blk_rdma_transport, binding->consumer_node, binding->blk_remote_rkey, slot_offset,
                                                blk_data_slot(binding->blk_zone_ptr, hdr, data_slot), data_bytes);
    }

    // Push only the single new CQ entry (16 bytes) instead of the full CQ region
    uint32_t cq_base = blk_ring_cq_offset(hdr->sq_depth);
    uint32_t entry_off = cq_base + (cq_idx * static_cast<uint32_t>(sizeof(BlkCqEntry)));
    binding->blk_rdma_transport->rdma_write(binding->blk_rdma_transport, binding->consumer_node, binding->blk_remote_rkey, entry_off,
                                            static_cast<uint8_t*>(binding->blk_zone_ptr) + entry_off, sizeof(BlkCqEntry));

    // Push updated header (sq_tail, cq_head changed)
    binding->blk_rdma_transport->rdma_write(binding->blk_rdma_transport, binding->consumer_node, binding->blk_remote_rkey, 0,
                                            binding->blk_zone_ptr, BLK_RING_HEADER_SIZE);
}

// Track accumulated RoCE completions for batch push after draining the SQ loop.
struct BatchCqPush {
    uint32_t data_slot;
    uint32_t data_bytes;
    uint32_t cq_idx;
};

// RoCE helper: push all accumulated completions in a single burst.
// Data slots are pushed individually (each may be large), but CQ entries and
// the header are pushed once at the end — amortizing per-completion frame overhead.
void roce_push_completions_batch(DevServerBinding* binding, const BatchCqPush* entries, uint32_t count) {
    if (!binding->blk_roce || binding->blk_rdma_transport == nullptr || count == 0) {
        return;
    }
    auto* hdr = blk_ring_header(binding->blk_zone_ptr);

    // 1. Push all data slots (each slot may be up to 64KB — individual writes)
    for (uint32_t i = 0; i < count; i++) {
        if (entries[i].data_bytes > 0 && entries[i].data_slot < hdr->data_slot_count) {
            uint32_t slot_offset = blk_ring_data_offset(hdr->sq_depth, hdr->cq_depth) + (entries[i].data_slot * hdr->data_slot_size);
            binding->blk_rdma_transport->rdma_write(binding->blk_rdma_transport, binding->consumer_node, binding->blk_remote_rkey,
                                                    slot_offset, blk_data_slot(binding->blk_zone_ptr, hdr, entries[i].data_slot),
                                                    entries[i].data_bytes);
        }
    }

    // 2. Push the entire CQ region in one write (64 entries * 16 bytes = 1024 bytes)
    //    This is cheaper than count individual 16-byte writes when count > 1.
    uint32_t cq_base = blk_ring_cq_offset(hdr->sq_depth);
    uint32_t cq_total = blk_ring_cq_size(hdr->cq_depth);
    binding->blk_rdma_transport->rdma_write(binding->blk_rdma_transport, binding->consumer_node, binding->blk_remote_rkey, cq_base,
                                            static_cast<uint8_t*>(binding->blk_zone_ptr) + cq_base, cq_total);

    // 3. Push updated header last (contains cq_head + sq_tail pointers)
    binding->blk_rdma_transport->rdma_write(binding->blk_rdma_transport, binding->consumer_node, binding->blk_remote_rkey, 0,
                                            binding->blk_zone_ptr, BLK_RING_HEADER_SIZE);
}

void blk_ring_server_poll(DevServerBinding* binding) {
    if (!binding->blk_rdma_active || binding->blk_zone_ptr == nullptr) {
        return;
    }

    // Guard against concurrent poll from timer thread + post_handler on different CPUs.
    // Use atomic test-and-set to ensure only one caller processes the ring at a time.
    bool expected = false;
    if (!binding->blk_poll_active.compare_exchange_strong(expected, true, std::memory_order_acquire, std::memory_order_relaxed)) {
        return;  // another CPU is already polling this ring
    }

    auto* hdr = blk_ring_header(binding->blk_zone_ptr);
    if (hdr->server_ready == 0) {
        binding->blk_poll_active.store(false, std::memory_order_release);
        return;
    }

    // For RoCE zones the consumer already pushes the SQ region into our local
    // copy via rdma_write (roce_push_sq) before sending the doorbell/notification.
    // No need to do an RDMA_READ here — the data is already in blk_zone_ptr.

    bool posted_cqe = false;

    // Accumulate RoCE completions for batch push (avoids per-CQE RDMA writes)
    constexpr uint32_t MAX_BATCH_CQ = 64;
    std::array<BatchCqPush, MAX_BATCH_CQ> batch_pushes = {};
    uint32_t batch_count = 0;

    while (!blk_sq_empty(hdr)) {
        // Check CQ has space before processing
        if (blk_cq_full(hdr)) {
            break;
        }

        uint32_t sq_idx = hdr->sq_tail % hdr->sq_depth;
        auto* sqe = &blk_sq_entries(binding->blk_zone_ptr)[sq_idx];

        BlkCqEntry cqe = {};
        cqe.tag = sqe->tag;
        cqe.data_slot = sqe->data_slot;
        uint32_t data_bytes = 0;

        switch (static_cast<BlkOpcode>(sqe->opcode)) {
            case BlkOpcode::READ: {
                if (sqe->data_slot >= hdr->data_slot_count) {
                    cqe.status = -1;
                    cqe.bytes_transferred = 0;
                    break;
                }
                uint8_t* dest = blk_data_slot(binding->blk_zone_ptr, hdr, sqe->data_slot);
                int ret = ker::dev::block_read(binding->block_dev, sqe->lba, sqe->block_count, dest);
                cqe.status = ret;
                cqe.bytes_transferred = (ret == 0) ? sqe->block_count * hdr->block_size : 0;
                data_bytes = cqe.bytes_transferred;
                break;
            }
            case BlkOpcode::WRITE: {
                if (sqe->data_slot >= hdr->data_slot_count) {
                    cqe.status = -1;
                    cqe.bytes_transferred = 0;
                    break;
                }
                // For RoCE writes: pull the data slot from proxy before writing to disk
                if (binding->blk_roce && binding->blk_rdma_transport != nullptr) {
                    uint32_t slot_offset = blk_ring_data_offset(hdr->sq_depth, hdr->cq_depth) + (sqe->data_slot * hdr->data_slot_size);
                    uint32_t slot_bytes = sqe->block_count * hdr->block_size;
                    binding->blk_rdma_transport->rdma_read(binding->blk_rdma_transport, binding->consumer_node, binding->blk_remote_rkey,
                                                           slot_offset, blk_data_slot(binding->blk_zone_ptr, hdr, sqe->data_slot),
                                                           slot_bytes);
                }
                const uint8_t* src = blk_data_slot(binding->blk_zone_ptr, hdr, sqe->data_slot);
                int ret = ker::dev::block_write(binding->block_dev, sqe->lba, sqe->block_count, src);
                cqe.status = ret;
                cqe.bytes_transferred = 0;
                break;
            }
            case BlkOpcode::FLUSH: {
                cqe.status = ker::dev::block_flush(binding->block_dev);
                cqe.bytes_transferred = 0;
                break;
            }
            case BlkOpcode::BULK_READ: {
                // Streaming bulk read: read blocks from device into a temporary
                // staging buffer, then RDMA-write the entire range to the
                // consumer's pre-registered staging buffer (rkey in data_slot).
                uint32_t consumer_rkey = sqe->data_slot;
                uint32_t total_bytes = sqe->block_count * hdr->block_size;
                auto* staging = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(total_bytes));
                if (staging == nullptr) {
                    cqe.status = -12;  // ENOMEM
                    cqe.bytes_transferred = 0;
                    break;
                }

                int ret = ker::dev::block_read(binding->block_dev, sqe->lba, sqe->block_count, staging);
                if (ret == 0 && binding->blk_rdma_transport != nullptr) {
                    // RDMA-write directly into consumer's staging buffer at offset 0
                    binding->blk_rdma_transport->rdma_write(binding->blk_rdma_transport, binding->consumer_node, consumer_rkey, 0, staging,
                                                            total_bytes);
                    cqe.bytes_transferred = total_bytes;
                } else {
                    cqe.bytes_transferred = 0;
                }
                cqe.status = ret;
                ker::mod::mm::dyn::kmalloc::free(staging);
                // Bulk ops push data directly via RDMA — no ring data slot involved
                data_bytes = 0;
                break;
            }
            case BlkOpcode::BULK_WRITE: {
                // Streaming bulk write: RDMA-read entire range from consumer's
                // registered staging buffer, then write blocks to device.
                uint32_t consumer_rkey = sqe->data_slot;
                uint32_t total_bytes = sqe->block_count * hdr->block_size;
                auto* staging = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(total_bytes));
                if (staging == nullptr) {
                    cqe.status = -12;  // ENOMEM
                    cqe.bytes_transferred = 0;
                    break;
                }

                int ret = 0;
                if (binding->blk_rdma_transport != nullptr) {
                    // RDMA-read from consumer's staging buffer at offset 0
                    binding->blk_rdma_transport->rdma_read(binding->blk_rdma_transport, binding->consumer_node, consumer_rkey, 0, staging,
                                                           total_bytes);
                    ret = ker::dev::block_write(binding->block_dev, sqe->lba, sqe->block_count, staging);
                } else {
                    ret = -1;
                }
                cqe.status = ret;
                cqe.bytes_transferred = 0;
                ker::mod::mm::dyn::kmalloc::free(staging);
                data_bytes = 0;
                break;
            }
            default: {
                cqe.status = -1;
                cqe.bytes_transferred = 0;
                break;
            }
        }

        // Advance SQ tail (consume entry)
        asm volatile("" ::: "memory");
        hdr->sq_tail = (hdr->sq_tail + 1) % hdr->sq_depth;

        // Post CQ entry
        uint32_t cq_idx = hdr->cq_head % hdr->cq_depth;
        blk_cq_entries(binding->blk_zone_ptr, hdr)[cq_idx] = cqe;
        asm volatile("" ::: "memory");
        hdr->cq_head = (hdr->cq_head + 1) % hdr->cq_depth;

        // Accumulate for batch RoCE push (instead of per-CQE push)
        if (batch_count < MAX_BATCH_CQ) {
            batch_pushes[batch_count++] = {sqe->data_slot, data_bytes, cq_idx};
        }

        posted_cqe = true;
    }

    // Batch-push all accumulated RoCE completions in one burst:
    // - Data slots are pushed individually (each up to 64KB)
    // - CQ region + header pushed once at the end
    // For single completions this falls back to the legacy per-CQE path
    // (equivalent cost: N data + 1 CQ region + 1 header, vs N * (1 data + 1 CQ entry + 1 header)).
    if (batch_count == 1) {
        roce_push_completions(binding, batch_pushes[0].data_slot, batch_pushes[0].data_bytes, batch_pushes[0].cq_idx);
    } else if (batch_count > 1) {
        roce_push_completions_batch(binding, batch_pushes.data(), batch_count);
    }

    // Release poll guard so other CPUs can poll this ring
    binding->blk_poll_active.store(false, std::memory_order_release);

    if (posted_cqe) {
        blk_ring_signal_consumer(binding);
    }
}

}  // namespace

// -----------------------------------------------------------------------------
// Zone post_handler — called when the consumer sends ZONE_NOTIFY_POST
// Triggers immediate ring polling instead of waiting for the timer tick.
// -----------------------------------------------------------------------------

namespace {

void blk_zone_post_handler(uint32_t zone_id, uint32_t /*offset*/, uint32_t /*length*/, uint8_t /*op_type*/) {
    s_server_lock.lock();
    auto* binding = find_binding_by_zone_id(zone_id);
    if (binding != nullptr) {
        binding->blk_sq_notified = true;
    }
    s_server_lock.unlock();

    // blk_ring_server_poll may do block I/O — call outside the lock
    if (binding != nullptr) {
        blk_ring_server_poll(binding);
    }
}

}  // namespace

// -----------------------------------------------------------------------------
// Deferred zone creation — runs from wki_timer_tick, outside NAPI poll context
// -----------------------------------------------------------------------------

void wki_dev_server_process_pending_zones() {
    // Collect pending bindings under the lock, then process outside
    constexpr size_t MAX_PENDING = 32;
    std::array<DevServerBinding*, MAX_PENDING> pending = {};
    size_t pending_count = 0;

    s_server_lock.lock();
    for (auto& b : g_bindings) {
        if (!b.active || !b.blk_zone_pending) {
            continue;
        }
        b.blk_zone_pending = false;  // only attempt once
        if (pending_count < MAX_PENDING) {
            pending[pending_count++] = &b;
        }
    }
    s_server_lock.unlock();

    // Process each pending binding outside the lock (wki_zone_create may block)
    for (size_t pi = 0; pi < pending_count; pi++) {
        auto& b = *pending[pi];

        uint32_t zone_sz = blk_ring_default_zone_size();
        uint8_t zone_access = ZONE_ACCESS_LOCAL_READ | ZONE_ACCESS_LOCAL_WRITE | ZONE_ACCESS_REMOTE_READ | ZONE_ACCESS_REMOTE_WRITE;

        int zone_ret =
            wki_zone_create(b.consumer_node, b.blk_zone_id, zone_sz, zone_access, ZoneNotifyMode::POST_ONLY, ZoneTypeHint::MSG_QUEUE);
        if (zone_ret != WKI_OK) {
            ker::mod::dbg::log("[WKI] Deferred block RDMA ring creation failed (err=%d) for zone 0x%08x — consumer falls back to msg path",
                               zone_ret, b.blk_zone_id);
            b.blk_zone_id = 0;
            continue;
        }

        b.blk_zone_ptr = wki_zone_get_ptr(b.blk_zone_id);
        b.blk_rdma_active = (b.blk_zone_ptr != nullptr);

        if (!b.blk_rdma_active) {
            ker::mod::dbg::log("[WKI] Deferred block RDMA zone ptr null for zone 0x%08x", b.blk_zone_id);
            continue;
        }

        // Check if the zone is RoCE-backed (needs explicit rdma_write/read sync)
        WkiZone* blk_zone = wki_zone_find(b.blk_zone_id);
        if (blk_zone != nullptr && blk_zone->is_roce) {
            b.blk_roce = true;
            b.blk_remote_rkey = blk_zone->remote_rkey;
            b.blk_rdma_transport = blk_zone->rdma_transport;
        }

        // Initialize the ring header in local memory
        auto* ring_hdr = blk_ring_header(b.blk_zone_ptr);
        ring_hdr->sq_head = 0;
        ring_hdr->sq_tail = 0;
        ring_hdr->cq_head = 0;
        ring_hdr->cq_tail = 0;
        ring_hdr->sq_depth = BLK_RING_DEFAULT_SQ_DEPTH;
        ring_hdr->cq_depth = BLK_RING_DEFAULT_CQ_DEPTH;
        ring_hdr->data_slot_count = BLK_RING_DEFAULT_DATA_SLOTS;
        ring_hdr->data_slot_size = BLK_RING_DEFAULT_DATA_SLOT_SIZE;
        ring_hdr->block_size = static_cast<uint32_t>(b.block_dev->block_size);
        ring_hdr->total_blocks = b.block_dev->total_blocks;
        // Compiler barrier: ensure all fields are visible before server_ready
        asm volatile("" ::: "memory");
        ring_hdr->server_ready = 1;

        // For RoCE zones: push the entire ring header to the proxy so it
        // can see server_ready and device parameters
        if (b.blk_roce && b.blk_rdma_transport != nullptr && b.blk_remote_rkey != 0) {
            b.blk_rdma_transport->rdma_write(b.blk_rdma_transport, b.consumer_node, b.blk_remote_rkey, 0, ring_hdr, sizeof(BlkRingHeader));
        }

        ker::mod::dbg::log("[WKI] Deferred block RDMA ring created: zone=0x%08x size=%u roce=%d", b.blk_zone_id, zone_sz,
                           b.blk_roce ? 1 : 0);

        // Register a zone post_handler so that ZONE_NOTIFY_POST from the consumer
        // triggers immediate ring polling instead of waiting for the ~10ms timer tick.
        wki_zone_set_handlers(b.blk_zone_id, nullptr, blk_zone_post_handler);
    }
}

// -----------------------------------------------------------------------------
// Block RDMA ring — periodic poll (called from wki_timer_tick)
// -----------------------------------------------------------------------------

auto wki_dev_server_get_vfs_write_buf(uint16_t consumer_node, uint16_t channel_id) -> uint8_t* {
    s_server_lock.lock();
    for (auto& b : g_bindings) {
        if (b.active && b.resource_type == ResourceType::VFS && b.consumer_node == consumer_node && b.assigned_channel == channel_id) {
            uint8_t* buf = b.vfs_rdma_write_buf;
            s_server_lock.unlock();
            return buf;
        }
    }
    s_server_lock.unlock();
    return nullptr;
}

void wki_dev_server_poll_rings() {
    // Collect bindings to poll under the lock, then poll outside
    constexpr size_t MAX_RINGS = 32;
    std::array<DevServerBinding*, MAX_RINGS> rings = {};
    size_t ring_count = 0;

    s_server_lock.lock();
    for (auto& b : g_bindings) {
        if (!b.active || !b.blk_rdma_active) {
            continue;
        }
        // For RoCE bindings, only poll when the consumer has signalled new work.
        // The consumer pushes the SQ via RDMA_WRITE then sends a doorbell;
        // there is nothing to do until that notification arrives.
        // Non-RoCE (ivshmem) bindings use shared memory — polling is cheap.
        if (b.blk_roce && !b.blk_sq_notified) {
            continue;
        }
        b.blk_sq_notified = false;
        if (ring_count < MAX_RINGS) {
            rings[ring_count++] = &b;
        }
    }
    s_server_lock.unlock();

    // blk_ring_server_poll may do block I/O — call outside the lock
    for (size_t i = 0; i < ring_count; i++) {
        blk_ring_server_poll(rings[i]);
    }
}

}  // namespace ker::net::wki
