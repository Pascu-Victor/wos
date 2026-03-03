#include "remotable.hpp"

#include <cstdio>
#include <cstring>
#include <deque>
#include <dev/block_device.hpp>
#include <net/netdevice.hpp>
#include <net/netif.hpp>
#include <net/route.hpp>
#include <net/wki/remote_net.hpp>
#include <net/wki/remote_vfs.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <vfs/fs/devfs.hpp>
#include <vfs/vfs.hpp>

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Storage — discovered resources from remote peers
// -----------------------------------------------------------------------------

namespace {
std::deque<DiscoveredResource> g_discovered;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_remotable_initialized = false;         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Deferred VFS mount queue — wki_remote_vfs_mount() spin-waits for an attach
// ACK, but handle_resource_advert() runs inside the NAPI poll handler where
// napi_poll_inline() re-entrantly returns 0.  Queue the mount and process
// it from the timer tick (outside NAPI context).
struct PendingVfsMount {
    uint16_t node_id;
    uint32_t resource_id;
    char mount_path[384];
};
std::deque<PendingVfsMount> g_pending_vfs_mounts;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// V2: Deferred NET attach queue — same NAPI context issue as VFS mounts
struct PendingNetAttach {
    uint16_t node_id;
    uint32_t resource_id;
    char nic_name[64];
    char hostname[64];
};
std::deque<PendingNetAttach> g_pending_net_attaches;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

ker::mod::sys::Spinlock s_remotable_lock;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Unlocked helper — caller must hold s_remotable_lock
auto find_resource_unlocked(uint16_t node_id, ResourceType type, uint32_t resource_id) -> DiscoveredResource* {
    for (auto& res : g_discovered) {
        if (res.valid && res.node_id == node_id && res.resource_type == type && res.resource_id == resource_id) {
            return &res;
        }
    }
    return nullptr;
}

// V2: ATTACH_SPARSE subnet overlap check
// Returns true if a local NIC already covers the same subnet as (remote_ip & remote_mask)
auto has_local_subnet_overlap(uint32_t remote_ip, uint32_t remote_mask) -> bool {
    if (remote_ip == 0 && remote_mask == 0) {
        return false;  // No IP info available — no overlap can be determined
    }
    uint32_t remote_subnet = remote_ip & remote_mask;

    size_t ndev_count = ker::net::netdev_count();
    for (size_t i = 0; i < ndev_count; i++) {
        ker::net::NetDevice* dev = ker::net::netdev_at(i);
        if (dev == nullptr) continue;

        auto* nif = ker::net::netif_get(dev);
        if (nif == nullptr) continue;

        for (size_t j = 0; j < nif->ipv4_addr_count; j++) {
            uint32_t local_subnet = nif->ipv4_addrs[j].addr & nif->ipv4_addrs[j].netmask;
            if (local_subnet == remote_subnet && nif->ipv4_addrs[j].netmask == remote_mask) {
                return true;
            }
        }
    }
    return false;
}

// Build the auto-mount path for a VFS export:
//   export "/"      -> "/wki/<hostname>"
//   export "/proc"  -> "/wki/<hostname>/proc"
//   export "data"   -> "/wki/<hostname>/data"
void build_vfs_mount_path(char* out, size_t out_size, const char* hostname, const char* export_name) {
    // Strip leading '/' from export name
    const char* stripped = export_name;
    while (*stripped == '/') stripped++;

    if (*stripped == '\0') {
        // Root export — mount directly at /wki/<hostname>
        snprintf(out, out_size, "/wki/%s", hostname);
    } else {
        snprintf(out, out_size, "/wki/%s/%s", hostname, stripped);
    }
}

}  // namespace

// -----------------------------------------------------------------------------
// Init
// -----------------------------------------------------------------------------

void wki_remotable_init() {
    if (g_remotable_initialized) {
        return;
    }
    g_remotable_initialized = true;
    ker::mod::dbg::log("[WKI] Remotable subsystem initialized");
}

// -----------------------------------------------------------------------------
// Local resource advertisement — iterate block devices with remotable != nullptr
// -----------------------------------------------------------------------------

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

    // V2: Also advertise VFS exports
    wki_remote_vfs_auto_discover();
    wki_remote_vfs_advertise_exports();

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

// -----------------------------------------------------------------------------
// Discovered resource table
// -----------------------------------------------------------------------------

auto wki_resource_find(uint16_t node_id, ResourceType type, uint32_t resource_id) -> DiscoveredResource* {
    s_remotable_lock.lock();
    auto* result = find_resource_unlocked(node_id, type, resource_id);
    s_remotable_lock.unlock();
    return result;
}

auto wki_resource_find_by_name(const char* name) -> DiscoveredResource* {
    if (name == nullptr) {
        return nullptr;
    }
    s_remotable_lock.lock();
    for (auto& res : g_discovered) {
        if (res.valid && strncmp(static_cast<const char*>(res.name), name, DISCOVERED_RESOURCE_NAME_LEN) == 0) {
            s_remotable_lock.unlock();
            return &res;
        }
    }
    s_remotable_lock.unlock();
    return nullptr;
}

void wki_resources_invalidate_for_peer(uint16_t node_id) {
    // Remove from devfs /dev/wki/ tree before erasing from discovered table
    ker::vfs::devfs::devfs_wki_remove_peer_resources(node_id);

    s_remotable_lock.lock();
    // Use erase-remove idiom to remove all entries from the fenced peer
    std::erase_if(g_discovered, [node_id](const DiscoveredResource& res) { return res.node_id == node_id; });
    s_remotable_lock.unlock();
}

void wki_resource_foreach(ResourceVisitor visitor, void* ctx) {
    s_remotable_lock.lock();
    for (const auto& res : g_discovered) {
        if (res.valid) {
            visitor(res, ctx);
        }
    }
    s_remotable_lock.unlock();
}

// -----------------------------------------------------------------------------
// RX handlers
// -----------------------------------------------------------------------------

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

    // Build the resource entry on-stack before locking
    DiscoveredResource res;
    res.node_id = adv->node_id;
    res.resource_type = type;
    res.resource_id = adv->resource_id;
    res.flags = adv->flags;
    res.valid = true;

    uint8_t copy_len = adv->name_len;
    if (copy_len >= DISCOVERED_RESOURCE_NAME_LEN) {
        copy_len = DISCOVERED_RESOURCE_NAME_LEN - 1;
    }
    memcpy(static_cast<void*>(res.name), resource_advert_name(adv), copy_len);
    res.name[copy_len] = '\0';

    s_remotable_lock.lock();

    // Check if we already have this resource (upsert)
    DiscoveredResource* existing = find_resource_unlocked(adv->node_id, type, adv->resource_id);
    if (existing != nullptr) {
        // Update flags
        existing->flags = adv->flags;
        s_remotable_lock.unlock();
        return;
    }

    g_discovered.push_back(res);

    // V2: Queue deferred VFS auto-mount (cannot spin-wait inside NAPI context)
    if (type == ResourceType::VFS) {
        const char* hostname = wki_peer_get_hostname(adv->node_id);
        if (hostname != nullptr && hostname[0] != '\0') {
            PendingVfsMount pending = {};
            pending.node_id = adv->node_id;
            pending.resource_id = adv->resource_id;
            build_vfs_mount_path(pending.mount_path, sizeof(pending.mount_path), hostname, static_cast<const char*>(res.name));
            g_pending_vfs_mounts.push_back(pending);
        }
    }

    // V2: Queue deferred NET auto-attach based on NIC policy
    if (type == ResourceType::NET && g_wki.nic_policy != WkiNicPolicy::MANUAL) {
        const char* hostname = wki_peer_get_hostname(adv->node_id);
        if (hostname != nullptr && hostname[0] != '\0') {
            PendingNetAttach pending = {};
            pending.node_id = adv->node_id;
            pending.resource_id = adv->resource_id;
            // Build proxy NIC name: "wki-<hostname>" truncated to 15 chars
            snprintf(pending.nic_name, sizeof(pending.nic_name), "wki-%s", hostname);
            if (strlen(pending.nic_name) >= ker::net::NETDEV_NAME_LEN) {
                pending.nic_name[ker::net::NETDEV_NAME_LEN - 1] = '\0';
            }
            strncpy(pending.hostname, hostname, sizeof(pending.hostname) - 1);
            pending.hostname[sizeof(pending.hostname) - 1] = '\0';
            g_pending_net_attaches.push_back(pending);
        }
    }

    s_remotable_lock.unlock();

    // Update devfs /dev/wki/ tree (outside lock — not a WKI container)
    ker::vfs::devfs::devfs_wki_add_resource(adv->node_id, adv->resource_type, adv->resource_id, adv->flags,
                                            static_cast<const char*>(res.name));

    ker::mod::dbg::log("[WKI] Discovered resource: node=0x%04x type=%u id=%u name=%s", adv->node_id, adv->resource_type, adv->resource_id,
                       static_cast<const char*>(res.name));
}

void handle_resource_withdraw(const WkiHeader* /*hdr*/, const uint8_t* payload, uint16_t payload_len) {
    if (payload_len < sizeof(ResourceAdvertPayload)) {
        return;
    }

    const auto* adv = reinterpret_cast<const ResourceAdvertPayload*>(payload);
    auto type = static_cast<ResourceType>(adv->resource_type);

    // V2: Unmount VFS resources on withdraw — copy mount path under lock, unmount outside
    char mount_path[384] = {};  // NOLINT(modernize-avoid-c-arrays)
    bool do_vfs_unmount = false;
    if (type == ResourceType::VFS) {
        const char* hostname = wki_peer_get_hostname(adv->node_id);
        if (hostname != nullptr && hostname[0] != '\0') {
            s_remotable_lock.lock();
            DiscoveredResource* existing = find_resource_unlocked(adv->node_id, type, adv->resource_id);
            if (existing != nullptr) {
                build_vfs_mount_path(mount_path, sizeof(mount_path), hostname, static_cast<const char*>(existing->name));
                do_vfs_unmount = true;
            }
            s_remotable_lock.unlock();
        }
    }

    // Blocking VFS unmount — outside lock
    if (do_vfs_unmount) {
        wki_remote_vfs_unmount(mount_path);
    }

    // V2: Detach proxy NIC on NET resource withdraw
    if (type == ResourceType::NET) {
        // Find and detach the proxy NIC for this resource
        // The proxy is identified by owner_node + resource_id stored in ProxyNetState
        // wki_remote_net_cleanup_for_peer handles by node, but we need per-resource detach
        // For now, log the withdrawal — full cleanup happens on fencing
        ker::mod::dbg::log("[WKI] NET resource withdrawn: node=0x%04x res_id=%u", adv->node_id, adv->resource_id);
    }

    // Remove from devfs /dev/wki/ tree (external, not a WKI container)
    ker::vfs::devfs::devfs_wki_remove_resource(adv->node_id, adv->resource_type, adv->resource_id);

    // Remove from discovered table
    s_remotable_lock.lock();
    std::erase_if(g_discovered, [&](const DiscoveredResource& res) {
        return res.node_id == adv->node_id && res.resource_type == type && res.resource_id == adv->resource_id;
    });
    s_remotable_lock.unlock();

    ker::mod::dbg::log("[WKI] Resource withdrawn: node=0x%04x type=%u id=%u", adv->node_id, adv->resource_type, adv->resource_id);
}

}  // namespace detail

// -----------------------------------------------------------------------------
// Deferred VFS mount processing — called from timer tick (outside NAPI context)
// -----------------------------------------------------------------------------

void wki_remotable_process_pending_mounts() {
    while (true) {
        s_remotable_lock.lock();
        if (g_pending_vfs_mounts.empty()) {
            s_remotable_lock.unlock();
            break;
        }
        PendingVfsMount pending = g_pending_vfs_mounts.front();
        g_pending_vfs_mounts.pop_front();
        s_remotable_lock.unlock();

        // Create intermediate directories (/wki/<hostname>)
        // Find the second '/' after "/wki/" to get host dir
        char host_dir[256] = {};                 // NOLINT(modernize-avoid-c-arrays)
        const char* p = pending.mount_path + 5;  // skip "/wki/"
        const char* slash = p;
        while (*slash != '\0' && *slash != '/') slash++;
        auto host_dir_len = static_cast<size_t>(slash - pending.mount_path);
        if (host_dir_len < sizeof(host_dir)) {
            memcpy(host_dir, pending.mount_path, host_dir_len);
            host_dir[host_dir_len] = '\0';
            ker::vfs::vfs_mkdir(host_dir, 0755);
        }

        // Create the full mount path directory
        ker::vfs::vfs_mkdir(pending.mount_path, 0755);

        int ret = wki_remote_vfs_mount(pending.node_id, pending.resource_id, pending.mount_path);
        if (ret == 0) {
            ker::mod::dbg::log("[WKI] Auto-mounted VFS: %s -> node=0x%04x", pending.mount_path, pending.node_id);
        } else {
            ker::mod::dbg::log("[WKI] VFS auto-mount failed: %s (ret=%d)", pending.mount_path, ret);
        }
    }
}

// V2: Process deferred NET auto-attaches — called from timer tick
void wki_remotable_process_pending_net_attaches() {
    while (true) {
        s_remotable_lock.lock();
        if (g_pending_net_attaches.empty()) {
            s_remotable_lock.unlock();
            break;
        }
        PendingNetAttach pending = g_pending_net_attaches.front();
        g_pending_net_attaches.pop_front();
        s_remotable_lock.unlock();

        // Check if peer is still connected
        WkiPeer* peer = wki_peer_find(pending.node_id);
        if (peer == nullptr || peer->state != PeerState::CONNECTED) {
            ker::mod::dbg::log("[WKI] NET auto-attach skipped: peer 0x%04x not connected", pending.node_id);
            continue;
        }

        // ATTACH_SPARSE: subnet overlap check is done after attach (since we need
        // the ACK to get IP info). For ATTACH_ALL, skip the check entirely.
        // The actual subnet check happens post-attach: if overlap is found, we detach.
        ker::net::NetDevice* proxy_dev = wki_remote_net_attach(pending.node_id, pending.resource_id, pending.nic_name);
        if (proxy_dev == nullptr) {
            ker::mod::dbg::log("[WKI] NET auto-attach failed: %s -> node=0x%04x", pending.nic_name, pending.node_id);
            continue;
        }

        // V2: ATTACH_SPARSE post-attach subnet overlap check
        if (g_wki.nic_policy == WkiNicPolicy::ATTACH_SPARSE) {
            // Get the ProxyNetState to read owner IP info from the extended ACK
            auto* state = static_cast<ProxyNetState*>(proxy_dev->private_data);
            if (state != nullptr && state->owner_ipv4_addr != 0) {
                if (has_local_subnet_overlap(state->owner_ipv4_addr, state->owner_ipv4_mask)) {
                    ker::mod::dbg::log("[WKI] Skipping remote NIC %s from %s: subnet overlap", pending.nic_name, pending.hostname);
                    wki_remote_net_detach(proxy_dev);
                    continue;
                }
            }
        }

        ker::mod::dbg::log("[WKI] NET auto-attached: %s -> node=0x%04x", pending.nic_name, pending.node_id);

        // V2: Install route for the remote subnet through the proxy NIC
        auto* proxy_state = static_cast<ProxyNetState*>(proxy_dev->private_data);
        if (proxy_state != nullptr && proxy_state->owner_ipv4_addr != 0 && proxy_state->owner_ipv4_mask != 0) {
            uint32_t remote_subnet = proxy_state->owner_ipv4_addr & proxy_state->owner_ipv4_mask;
            int route_ret = ker::net::route_add(remote_subnet, proxy_state->owner_ipv4_mask, 0 /*gateway*/, 100 /*metric*/, proxy_dev);
            if (route_ret == 0) {
                ker::mod::dbg::log("[WKI] Route installed: subnet=0x%08x mask=0x%08x via %s", remote_subnet, proxy_state->owner_ipv4_mask,
                                   pending.nic_name);
            }
        }
    }
}

}  // namespace ker::net::wki
