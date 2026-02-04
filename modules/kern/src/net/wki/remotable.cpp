#include "remotable.hpp"

#include <cstring>
#include <deque>
#include <dev/block_device.hpp>
#include <net/netdevice.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>

namespace ker::net::wki {

// ─────────────────────────────────────────────────────────────────────────────
// Storage — discovered resources from remote peers
// ─────────────────────────────────────────────────────────────────────────────

namespace {
std::deque<DiscoveredResource> g_discovered;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_remotable_initialized = false;         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
}  // namespace

// ─────────────────────────────────────────────────────────────────────────────
// Init
// ─────────────────────────────────────────────────────────────────────────────

void wki_remotable_init() {
    if (g_remotable_initialized) {
        return;
    }
    g_remotable_initialized = true;
    ker::mod::dbg::log("[WKI] Remotable subsystem initialized");
}

// ─────────────────────────────────────────────────────────────────────────────
// Local resource advertisement — iterate block devices with remotable != nullptr
// ─────────────────────────────────────────────────────────────────────────────

namespace {

void send_resource_advert_to_peer(uint16_t peer_node, ker::dev::BlockDevice* bdev, uint32_t resource_id) {
    // Build ResourceAdvertPayload + name
    uint8_t name_len = 0;
    while (name_len < 63 && bdev->name[name_len] != '\0') {
        name_len++;
    }

    auto total_len = static_cast<uint16_t>(sizeof(ResourceAdvertPayload) + name_len);
    std::array<uint8_t, sizeof(ResourceAdvertPayload) + 64> buf{};

    auto* adv = reinterpret_cast<ResourceAdvertPayload*>(buf.data());
    adv->node_id = g_wki.my_node_id;
    adv->resource_type = static_cast<uint16_t>(ResourceType::BLOCK);
    adv->resource_id = resource_id;
    adv->flags = 0;

    if (bdev->remotable != nullptr) {
        if (bdev->remotable->can_share()) {
            adv->flags |= RESOURCE_FLAG_SHAREABLE;
        }
        if (bdev->remotable->can_passthrough()) {
            adv->flags |= RESOURCE_FLAG_PASSTHROUGH_CAPABLE;
        }
    }

    adv->name_len = name_len;
    memcpy(buf.data() + sizeof(ResourceAdvertPayload), bdev->name.data(), name_len);

    wki_send(peer_node, WKI_CHAN_CONTROL, MsgType::RESOURCE_ADVERT, buf.data(), total_len);
}

void send_net_resource_advert_to_peer(uint16_t peer_node, ker::net::NetDevice* ndev, uint32_t resource_id) {
    uint8_t name_len = 0;
    while (name_len < 63 && ndev->name[name_len] != '\0') {
        name_len++;
    }

    auto total_len = static_cast<uint16_t>(sizeof(ResourceAdvertPayload) + name_len);
    std::array<uint8_t, sizeof(ResourceAdvertPayload) + 64> buf{};

    auto* adv = reinterpret_cast<ResourceAdvertPayload*>(buf.data());
    adv->node_id = g_wki.my_node_id;
    adv->resource_type = static_cast<uint16_t>(ResourceType::NET);
    adv->resource_id = resource_id;
    adv->flags = 0;

    if (ndev->remotable != nullptr) {
        if (ndev->remotable->can_share()) {
            adv->flags |= RESOURCE_FLAG_SHAREABLE;
        }
        if (ndev->remotable->can_passthrough()) {
            adv->flags |= RESOURCE_FLAG_PASSTHROUGH_CAPABLE;
        }
    }

    adv->name_len = name_len;
    memcpy(buf.data() + sizeof(ResourceAdvertPayload), ndev->name.data(), name_len);

    wki_send(peer_node, WKI_CHAN_CONTROL, MsgType::RESOURCE_ADVERT, buf.data(), total_len);
}

}  // namespace

void wki_resource_advertise_all() {
    if (!g_remotable_initialized) {
        return;
    }

    // Iterate all registered block devices
    size_t count = ker::dev::block_device_count();
    for (size_t i = 0; i < count; i++) {
        ker::dev::BlockDevice* bdev = ker::dev::block_device_at(i);
        if (bdev == nullptr || bdev->remotable == nullptr) {
            continue;
        }
        if (!bdev->remotable->can_remote()) {
            continue;
        }

        // resource_id = minor number (unique per block device)
        auto resource_id = static_cast<uint32_t>(bdev->minor);

        // Send to all CONNECTED peers
        for (size_t p = 0; p < WKI_MAX_PEERS; p++) {
            WkiPeer* peer = &g_wki.peers[p];
            if (peer->node_id == WKI_NODE_INVALID) {
                continue;
            }
            if (peer->state != PeerState::CONNECTED) {
                continue;
            }

            send_resource_advert_to_peer(peer->node_id, bdev, resource_id);
        }
    }

    // Iterate all registered net devices
    size_t ndev_count = ker::net::netdev_count();
    for (size_t i = 0; i < ndev_count; i++) {
        ker::net::NetDevice* ndev = ker::net::netdev_at(i);
        if (ndev == nullptr || ndev->remotable == nullptr) {
            continue;
        }
        if (!ndev->remotable->can_remote()) {
            continue;
        }

        // resource_id = ifindex (unique per net device)
        auto net_resource_id = ndev->ifindex;

        // Send to all CONNECTED peers
        for (size_t p = 0; p < WKI_MAX_PEERS; p++) {
            WkiPeer* peer = &g_wki.peers[p];
            if (peer->node_id == WKI_NODE_INVALID) {
                continue;
            }
            if (peer->state != PeerState::CONNECTED) {
                continue;
            }

            send_net_resource_advert_to_peer(peer->node_id, ndev, net_resource_id);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Discovered resource table
// ─────────────────────────────────────────────────────────────────────────────

auto wki_resource_find(uint16_t node_id, ResourceType type, uint32_t resource_id) -> DiscoveredResource* {
    for (auto& res : g_discovered) {
        if (res.valid && res.node_id == node_id && res.resource_type == type && res.resource_id == resource_id) {
            return &res;
        }
    }
    return nullptr;
}

auto wki_resource_find_by_name(const char* name) -> DiscoveredResource* {
    if (name == nullptr) {
        return nullptr;
    }
    for (auto& res : g_discovered) {
        if (res.valid && strncmp(static_cast<const char*>(res.name), name, DISCOVERED_RESOURCE_NAME_LEN) == 0) {
            return &res;
        }
    }
    return nullptr;
}

void wki_resources_invalidate_for_peer(uint16_t node_id) {
    // Use erase-remove idiom to remove all entries from the fenced peer
    std::erase_if(g_discovered, [node_id](const DiscoveredResource& res) { return res.node_id == node_id; });
}

// ─────────────────────────────────────────────────────────────────────────────
// RX handlers
// ─────────────────────────────────────────────────────────────────────────────

namespace detail {

void handle_resource_advert(const WkiHeader* /*hdr*/, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(ResourceAdvertPayload)) {
        return;
    }

    const auto* adv = reinterpret_cast<const ResourceAdvertPayload*>(payload);

    // Validate name_len
    if (sizeof(ResourceAdvertPayload) + adv->name_len > payload_len) {
        return;
    }

    // Ignore adverts from ourselves
    if (adv->node_id == g_wki.my_node_id) {
        return;
    }

    auto type = static_cast<ResourceType>(adv->resource_type);

    // Check if we already have this resource (upsert)
    DiscoveredResource* existing = wki_resource_find(adv->node_id, type, adv->resource_id);
    if (existing != nullptr) {
        // Update flags
        existing->flags = adv->flags;
        return;
    }

    // Add new entry
    DiscoveredResource res;
    res.node_id = adv->node_id;
    res.resource_type = type;
    res.resource_id = adv->resource_id;
    res.flags = adv->flags;
    res.valid = true;

    // Copy name
    uint8_t copy_len = adv->name_len;
    if (copy_len >= DISCOVERED_RESOURCE_NAME_LEN) {
        copy_len = DISCOVERED_RESOURCE_NAME_LEN - 1;
    }
    memcpy(static_cast<void*>(res.name), resource_advert_name(adv), copy_len);
    res.name[copy_len] = '\0';

    g_discovered.push_back(res);

    ker::mod::dbg::log("[WKI] Discovered resource: node=0x%04x type=%u id=%u name=%s", adv->node_id, adv->resource_type, adv->resource_id,
                       static_cast<const char*>(res.name));
}

void handle_resource_withdraw(const WkiHeader* /*hdr*/, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(ResourceAdvertPayload)) {
        return;
    }

    const auto* adv = reinterpret_cast<const ResourceAdvertPayload*>(payload);
    auto type = static_cast<ResourceType>(adv->resource_type);

    // Remove from discovered table
    std::erase_if(g_discovered, [&](const DiscoveredResource& res) {
        return res.node_id == adv->node_id && res.resource_type == type && res.resource_id == adv->resource_id;
    });

    ker::mod::dbg::log("[WKI] Resource withdrawn: node=0x%04x type=%u id=%u", adv->node_id, adv->resource_type, adv->resource_id);
}

}  // namespace detail

}  // namespace ker::net::wki
