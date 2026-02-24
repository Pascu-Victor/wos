#include "dev_server.hpp"

#include <array>
#include <cstring>
#include <deque>
#include <dev/block_device.hpp>
#include <net/netdevice.hpp>
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

auto find_binding_by_channel(uint16_t consumer_node, uint16_t channel_id) -> DevServerBinding* {
    for (auto& b : g_bindings) {
        if (b.active && b.consumer_node == consumer_node && b.assigned_channel == channel_id) {
            return &b;
        }
    }
    return nullptr;
}

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

auto has_net_binding_for_dev(ker::net::NetDevice* dev) -> bool {
    for (const auto& b : g_bindings) {
        if (b.active && b.resource_type == ResourceType::NET && b.net_dev == dev) {
            return true;
        }
    }
    return false;
}

// D11: Uninstall the RX forward hook if no more NET bindings reference this device
void maybe_uninstall_rx_forward(ker::net::NetDevice* dev) {
    if (dev != nullptr && !has_net_binding_for_dev(dev)) {
        dev->wki_rx_forward = nullptr;
    }
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

    for (auto& b : g_bindings) {
        if (!b.active || b.resource_type != ResourceType::NET || b.net_dev != dev) {
            continue;
        }

        // D12: Apply RX filter
        if (is_broadcast && !b.net_rx_filter.accept_broadcast) {
            continue;
        }
        if (is_multicast && !b.net_rx_filter.accept_multicast) {
            continue;
        }
        // Unicast: always accepted (accept_unicast defaults to true)

        // Build and send OP_NET_RX_NOTIFY (fire-and-forget, no response expected)
        auto pkt_data_len = static_cast<uint16_t>(pkt->len);
        auto req_total = static_cast<uint16_t>(sizeof(DevOpReqPayload) + pkt_data_len);

        if (req_total > WKI_ETH_MAX_PAYLOAD) {
            continue;  // Packet too large for a single WKI message
        }

        auto* req_buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(req_total));
        if (req_buf == nullptr) {
            continue;
        }

        auto* req = reinterpret_cast<DevOpReqPayload*>(req_buf);
        req->op_id = OP_NET_RX_NOTIFY;
        req->data_len = pkt_data_len;
        memcpy(req_buf + sizeof(DevOpReqPayload), pkt->data, pkt_data_len);

        wki_send(b.consumer_node, b.assigned_channel, MsgType::DEV_OP_REQ, req_buf, req_total);
        ker::mod::mm::dyn::kmalloc::free(req_buf);
    }
}

// -----------------------------------------------------------------------------
// Fencing cleanup
// -----------------------------------------------------------------------------

void wki_dev_server_detach_all_for_peer(uint16_t node_id) {
    for (auto& b : g_bindings) {
        if (!b.active || b.consumer_node != node_id) {
            continue;
        }

        // Destroy RDMA block ring zone if active
        if (b.blk_rdma_active && b.blk_zone_id != 0) {
            wki_zone_destroy(b.blk_zone_id);
            b.blk_rdma_active = false;
            b.blk_zone_ptr = nullptr;
            b.blk_zone_id = 0;
        }

        // Call on_remote_fault if the device supports it
        if (b.block_dev != nullptr && b.block_dev->remotable != nullptr) {
            b.block_dev->remotable->on_remote_fault(node_id);
        }
        if (b.net_dev != nullptr && b.net_dev->remotable != nullptr) {
            b.net_dev->remotable->on_remote_fault(node_id);
        }

        // Close the dynamic channel
        WkiChannel* ch = wki_channel_get(b.consumer_node, b.assigned_channel);
        if (ch != nullptr) {
            wki_channel_close(ch);
        }

        b.active = false;
    }

    // D11: Uninstall RX forward hooks for NET devices that no longer have bindings
    for (size_t i = 0; i < ker::net::netdev_count(); i++) {
        ker::net::NetDevice* dev = ker::net::netdev_at(i);
        if (dev != nullptr) {
            maybe_uninstall_rx_forward(dev);
        }
    }

    // Clean up inactive entries
    std::erase_if(g_bindings, [](const DevServerBinding& b) { return !b.active; });
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

        g_bindings.push_back(binding);

        // Send success ACK with the proposed RDMA zone info.
        // The consumer will wait for the zone to appear + server_ready.
        ack.status = static_cast<uint8_t>(DevAttachStatus::OK);
        ack.assigned_channel = ch->channel_id;
        ack.max_op_size = static_cast<uint16_t>(WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload));
        ack.rdma_flags = DEV_ATTACH_RDMA_BLK_RING;
        ack.blk_zone_id = blk_zone_id;

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

        g_bindings.push_back(binding);

        ack.status = static_cast<uint8_t>(DevAttachStatus::OK);
        ack.assigned_channel = ch->channel_id;
        ack.max_op_size = static_cast<uint16_t>(WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload));

        ker::mod::dbg::log("[WKI] VFS attach: node=0x%04x res_id=%u ch=%u path=%s", hdr->src_node, req->resource_id, ch->channel_id,
                           static_cast<const char*>(exp->export_path));

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

        g_bindings.push_back(binding);

        // D11: Install RX forward hook on the NIC so received packets are forwarded
        ndev->wki_rx_forward = wki_dev_server_forward_net_rx;

        ack.status = static_cast<uint8_t>(DevAttachStatus::OK);
        ack.assigned_channel = ch->channel_id;
        ack.max_op_size = static_cast<uint16_t>(WKI_ETH_MAX_PAYLOAD - sizeof(DevOpReqPayload));

        ker::mod::dbg::log("[WKI] NET attach: node=0x%04x res_id=%u ch=%u", hdr->src_node, req->resource_id, ch->channel_id);

        // Send ACK on WKI_CHAN_RESOURCE (the same channel the request arrived on) so that
        // the piggybacked ACK properly drains the client's retransmit queue for channel 3.
        // The dynamic channel ID is communicated via ack.assigned_channel in the payload.
        int ack_ret = wki_send(hdr->src_node, WKI_CHAN_RESOURCE, MsgType::DEV_ATTACH_ACK, &ack, sizeof(ack));
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

    // Find and remove the binding
    for (auto it = g_bindings.begin(); it != g_bindings.end(); ++it) {
        if (!it->active) {
            continue;
        }
        if (it->consumer_node != hdr->src_node) {
            continue;
        }
        if (it->resource_id != det->resource_id) {
            continue;
        }

        // Destroy RDMA block ring zone if active
        if (it->blk_rdma_active && it->blk_zone_id != 0) {
            wki_zone_destroy(it->blk_zone_id);
            it->blk_rdma_active = false;
            it->blk_zone_ptr = nullptr;
            it->blk_zone_id = 0;
        }

        // Call on_remote_detach
        if (it->block_dev != nullptr && it->block_dev->remotable != nullptr) {
            it->block_dev->remotable->on_remote_detach(hdr->src_node);
        }
        if (it->net_dev != nullptr && it->net_dev->remotable != nullptr) {
            it->net_dev->remotable->on_remote_detach(hdr->src_node);
        }

        // D11: Save net_dev before erasing so we can check if hook should be removed
        ker::net::NetDevice* detached_ndev = it->net_dev;

        // Close channel
        WkiChannel* ch = wki_channel_get(it->consumer_node, it->assigned_channel);
        if (ch != nullptr) {
            wki_channel_close(ch);
        }

        ker::mod::dbg::log("[WKI] Dev detach: node=0x%04x res_id=%u", hdr->src_node, det->resource_id);

        g_bindings.erase(it);

        // D11: Uninstall RX forward hook if no more NET bindings reference this device
        maybe_uninstall_rx_forward(detached_ndev);

        return;
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

    // Find binding by (src_node, channel_id)
    DevServerBinding* binding = find_binding_by_channel(hdr->src_node, hdr->channel_id);
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

    // Dispatch VFS operations to remote_vfs handler
    if (req->op_id >= OP_VFS_OPEN && req->op_id <= OP_VFS_SYMLINK) {
        detail::handle_vfs_op(hdr, hdr->channel_id, static_cast<const char*>(binding->vfs_export_path), req->op_id, req_data, req_data_len);
        return;
    }

    // Dispatch NET operations to remote_net handler
    if (req->op_id >= OP_NET_XMIT && req->op_id <= OP_NET_GET_STATS) {
        if (binding->net_dev == nullptr) {
            DevOpRespPayload resp = {};
            resp.op_id = req->op_id;
            resp.status = -1;
            resp.data_len = 0;
            wki_send(hdr->src_node, hdr->channel_id, MsgType::DEV_OP_RESP, &resp, sizeof(resp));
            return;
        }
        detail::handle_net_op(hdr, hdr->channel_id, binding->net_dev, req->op_id, req_data, req_data_len);
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

void blk_ring_server_poll(DevServerBinding* binding) {
    if (!binding->blk_rdma_active || binding->blk_zone_ptr == nullptr) {
        return;
    }

    // Guard against concurrent poll from timer thread + post_handler on different CPUs.
    // Use atomic test-and-set to ensure only one caller processes the ring at a time.
    bool expected = false;
    if (!__atomic_compare_exchange_n(&binding->blk_poll_active, &expected, true, false, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED)) {
        return;  // another CPU is already polling this ring
    }

    auto* hdr = blk_ring_header(binding->blk_zone_ptr);
    if (hdr->server_ready == 0) {
        __atomic_store_n(&binding->blk_poll_active, false, __ATOMIC_RELEASE);
        return;
    }

    // For RoCE zones the consumer already pushes the SQ region into our local
    // copy via rdma_write (roce_push_sq) before sending the doorbell/notification.
    // No need to do an RDMA_READ here — the data is already in blk_zone_ptr.

    bool posted_cqe = false;

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

        // For RoCE: push completions (data slot + new CQ entry + header) to proxy
        roce_push_completions(binding, sqe->data_slot, data_bytes, cq_idx);

        posted_cqe = true;
    }

    // Release poll guard so other CPUs can poll this ring
    __atomic_store_n(&binding->blk_poll_active, false, __ATOMIC_RELEASE);

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
    auto* binding = find_binding_by_zone_id(zone_id);
    if (binding != nullptr) {
        binding->blk_sq_notified = true;
        blk_ring_server_poll(binding);
    }
}

}  // namespace

// -----------------------------------------------------------------------------
// Deferred zone creation — runs from wki_timer_tick, outside NAPI poll context
// -----------------------------------------------------------------------------

void wki_dev_server_process_pending_zones() {
    for (auto& b : g_bindings) {
        if (!b.active || !b.blk_zone_pending) {
            continue;
        }

        b.blk_zone_pending = false;  // only attempt once

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

void wki_dev_server_poll_rings() {
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
        blk_ring_server_poll(&b);
    }
}

}  // namespace ker::net::wki
