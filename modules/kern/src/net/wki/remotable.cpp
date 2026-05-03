#include "remotable.hpp"

#include <cstddef>
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

using log = ker::mod::dbg::logger<"wki">;

// -----------------------------------------------------------------------------
// Storage - discovered resources from remote peers
// -----------------------------------------------------------------------------

namespace {
std::deque<DiscoveredResource> g_discovered;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_remotable_initialized = false;         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Deferred VFS mount queue - wki_remote_vfs_mount() spin-waits for an attach
// ACK, but handle_resource_advert() runs inside the NAPI poll handler where
// napi_poll_inline() re-entrantly returns 0.  Queue the mount and process
// it from the timer tick (outside NAPI context).
struct PendingVfsMount {
    uint16_t node_id;
    uint32_t resource_id;
    char mount_path[384];
    bool force_remount = false;
    uint8_t retry_count = 0;
    uint64_t next_attempt_us = 0;
};
std::deque<PendingVfsMount> g_pending_vfs_mounts;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
constexpr uint8_t VFS_AUTO_MOUNT_MAX_RETRIES = 8;
constexpr uint64_t VFS_AUTO_MOUNT_RETRY_BASE_US = 250000;
constexpr uint64_t VFS_AUTO_MOUNT_RETRY_MAX_US = 2000000;

// V2: Deferred NET attach queue - same NAPI context issue as VFS mounts
struct PendingNetAttach {
    uint16_t node_id;
    uint32_t resource_id;
    char nic_name[64];
    char hostname[64];
    char remote_name[64];
    uint8_t retry_count = 0;
    uint64_t next_attempt_us = 0;
};
std::deque<PendingNetAttach> g_pending_net_attaches;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
constexpr uint8_t NET_AUTO_ATTACH_MAX_RETRIES = 8;
constexpr uint64_t NET_AUTO_ATTACH_RETRY_BASE_US = 1000000;
constexpr uint64_t NET_AUTO_ATTACH_RETRY_MAX_US = 5000000;

ker::mod::sys::Spinlock s_remotable_lock;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Unlocked helper - caller must hold s_remotable_lock
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
        return false;  // No IP info available - no overlap can be determined
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
        // Root export - mount directly at /wki/<hostname>
        snprintf(out, out_size, "/wki/%s", hostname);
    } else {
        snprintf(out, out_size, "/wki/%s/%s", hostname, stripped);
    }
}

auto pending_vfs_mount_is_live_locked(const PendingVfsMount& pending) -> bool {
    return find_resource_unlocked(pending.node_id, ResourceType::VFS, pending.resource_id) != nullptr;
}

auto pending_net_attach_is_live_locked(const PendingNetAttach& pending) -> bool {
    return find_resource_unlocked(pending.node_id, ResourceType::NET, pending.resource_id) != nullptr;
}

void queue_vfs_mount_locked(uint16_t node_id, uint32_t resource_id, const char* mount_path, bool force_remount) {
    if (mount_path == nullptr || mount_path[0] == '\0') {
        return;
    }

    for (auto& pending : g_pending_vfs_mounts) {
        if (pending.node_id != node_id) {
            continue;
        }

        bool same_resource = pending.resource_id == resource_id;
        bool same_mount_path = std::strncmp(pending.mount_path, mount_path, sizeof(pending.mount_path)) == 0;
        if (!same_resource && !same_mount_path) {
            continue;
        }

        pending.resource_id = resource_id;
        std::snprintf(pending.mount_path, sizeof(pending.mount_path), "%s", mount_path);
        pending.force_remount = pending.force_remount || force_remount;
        pending.retry_count = 0;
        pending.next_attempt_us = 0;
        return;
    }

    PendingVfsMount pending = {};
    pending.node_id = node_id;
    pending.resource_id = resource_id;
    pending.force_remount = force_remount;
    std::snprintf(pending.mount_path, sizeof(pending.mount_path), "%s", mount_path);
    g_pending_vfs_mounts.push_back(pending);
}

void build_net_proxy_name(char* out, size_t out_size, const char* hostname, const char* remote_name, uint32_t resource_id) {
    if (out == nullptr || out_size == 0) {
        return;
    }

    if (remote_name != nullptr && remote_name[0] == 'e' && remote_name[1] == 't' && remote_name[2] == 'h' && remote_name[3] != '\0' &&
        remote_name[4] == '\0') {
        std::snprintf(out, out_size, "wki-%s-e%c", hostname, remote_name[3]);
    } else {
        std::snprintf(out, out_size, "wki-%s-%u", hostname, resource_id);
    }

    if (std::strlen(out) >= ker::net::NETDEV_NAME_LEN) {
        out[ker::net::NETDEV_NAME_LEN - 1] = '\0';
    }
}

void queue_net_attach_locked(uint16_t node_id, uint32_t resource_id, const char* nic_name, const char* hostname, const char* remote_name,
                             uint8_t retry_count = 0, uint64_t next_attempt_us = 0) {
    if (nic_name == nullptr || nic_name[0] == '\0' || hostname == nullptr || hostname[0] == '\0') {
        return;
    }

    for (auto& pending : g_pending_net_attaches) {
        if (pending.node_id != node_id) {
            continue;
        }

        bool same_resource = pending.resource_id == resource_id;
        bool same_name = std::strncmp(pending.nic_name, nic_name, sizeof(pending.nic_name)) == 0;
        if (!same_resource && !same_name) {
            continue;
        }

        pending.resource_id = resource_id;
        std::snprintf(pending.nic_name, sizeof(pending.nic_name), "%s", nic_name);
        std::snprintf(pending.hostname, sizeof(pending.hostname), "%s", hostname);
        std::snprintf(pending.remote_name, sizeof(pending.remote_name), "%s", remote_name != nullptr ? remote_name : "");
        pending.retry_count = retry_count;
        pending.next_attempt_us = next_attempt_us;
        return;
    }

    PendingNetAttach pending = {};
    pending.node_id = node_id;
    pending.resource_id = resource_id;
    pending.retry_count = retry_count;
    pending.next_attempt_us = next_attempt_us;
    std::snprintf(pending.nic_name, sizeof(pending.nic_name), "%s", nic_name);
    std::snprintf(pending.hostname, sizeof(pending.hostname), "%s", hostname);
    std::snprintf(pending.remote_name, sizeof(pending.remote_name), "%s", remote_name != nullptr ? remote_name : "");
    g_pending_net_attaches.push_back(pending);
}

auto requeue_net_attach(PendingNetAttach& pending) -> bool {
    if (pending.retry_count + 1 >= NET_AUTO_ATTACH_MAX_RETRIES) {
        return false;
    }

    pending.retry_count++;
    uint64_t delay_us = NET_AUTO_ATTACH_RETRY_BASE_US << (pending.retry_count - 1);
    if (delay_us > NET_AUTO_ATTACH_RETRY_MAX_US) {
        delay_us = NET_AUTO_ATTACH_RETRY_MAX_US;
    }
    pending.next_attempt_us = wki_now_us() + delay_us;

    s_remotable_lock.lock();
    queue_net_attach_locked(pending.node_id, pending.resource_id, pending.nic_name, pending.hostname, pending.remote_name,
                            pending.retry_count, pending.next_attempt_us);
    s_remotable_lock.unlock();
    return true;
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
// Local resource advertisement - iterate block devices with remotable != nullptr
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
    std::erase_if(g_pending_vfs_mounts, [node_id](const PendingVfsMount& pending) { return pending.node_id == node_id; });
    std::erase_if(g_pending_net_attaches, [node_id](const PendingNetAttach& pending) { return pending.node_id == node_id; });
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

    char desired_mount_path[384] = {};
    bool queue_vfs_mount = false;
    s_remotable_lock.lock();

    // Check if we already have this resource (upsert)
    DiscoveredResource* existing = find_resource_unlocked(adv->node_id, type, adv->resource_id);
    if (existing != nullptr) {
        bool name_changed =
            std::strncmp(static_cast<const char*>(existing->name), static_cast<const char*>(res.name), sizeof(existing->name)) != 0;

        // Refresh mutable fields so reconnect/re-advertisement can correct a
        // stale visible name without requiring a reboot.
        existing->flags = adv->flags;
        memcpy(static_cast<void*>(existing->name), static_cast<const void*>(res.name), sizeof(existing->name));

        if (type == ResourceType::VFS) {
            const char* hostname = wki_peer_get_hostname(adv->node_id);
            if (hostname != nullptr && hostname[0] != '\0') {
                build_vfs_mount_path(desired_mount_path, sizeof(desired_mount_path), hostname, static_cast<const char*>(res.name));
                queue_vfs_mount_locked(adv->node_id, adv->resource_id, desired_mount_path, true);
                queue_vfs_mount = true;
            }
        } else if (type == ResourceType::NET && g_wki.nic_policy != WkiNicPolicy::MANUAL) {
            const char* hostname = wki_peer_get_hostname(adv->node_id);
            if (hostname != nullptr && hostname[0] != '\0') {
                char nic_name[64] = {};
                build_net_proxy_name(nic_name, sizeof(nic_name), hostname, static_cast<const char*>(res.name), adv->resource_id);
                queue_net_attach_locked(adv->node_id, adv->resource_id, nic_name, hostname, static_cast<const char*>(res.name));
            }
        }

        s_remotable_lock.unlock();

        if (queue_vfs_mount && name_changed) {
            ker::mod::dbg::log("[WKI] VFS resource renamed: node=0x%04x res_id=%u -> %s", adv->node_id, adv->resource_id,
                               desired_mount_path);
        }

        ker::vfs::devfs::devfs_wki_remove_resource(adv->node_id, adv->resource_type, adv->resource_id);
        ker::vfs::devfs::devfs_wki_add_resource(adv->node_id, adv->resource_type, adv->resource_id, adv->flags,
                                                static_cast<const char*>(res.name));
        return;
    }

    g_discovered.push_back(res);

    // V2: Queue deferred VFS auto-mount (cannot spin-wait inside NAPI context)
    if (type == ResourceType::VFS) {
        const char* hostname = wki_peer_get_hostname(adv->node_id);
        if (hostname != nullptr && hostname[0] != '\0') {
            build_vfs_mount_path(desired_mount_path, sizeof(desired_mount_path), hostname, static_cast<const char*>(res.name));
            queue_vfs_mount_locked(adv->node_id, adv->resource_id, desired_mount_path, false);
        }
    }

    // V2: Queue deferred NET auto-attach based on NIC policy
    if (type == ResourceType::NET && g_wki.nic_policy != WkiNicPolicy::MANUAL) {
        const char* hostname = wki_peer_get_hostname(adv->node_id);
        if (hostname != nullptr && hostname[0] != '\0') {
            char nic_name[64] = {};
            build_net_proxy_name(nic_name, sizeof(nic_name), hostname, static_cast<const char*>(res.name), adv->resource_id);
            queue_net_attach_locked(adv->node_id, adv->resource_id, nic_name, hostname, static_cast<const char*>(res.name));
        }
    }

    s_remotable_lock.unlock();

    // Update devfs /dev/wki/ tree (outside lock - not a WKI container)
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

    // V2: Unmount VFS resources on withdraw - copy mount path under lock, unmount outside
    char mount_path[384] = {};  // NOLINT(modernize-avoid-c-arrays)
    bool do_vfs_unmount = false;
    if (type == ResourceType::VFS) {
        do_vfs_unmount = wki_remote_vfs_find_mount_for_resource(adv->node_id, adv->resource_id, mount_path, sizeof(mount_path));
    }

    // Blocking VFS unmount - outside lock
    if (do_vfs_unmount) {
        wki_remote_vfs_unmount(mount_path);
    }

    // V2: Detach proxy NIC on NET resource withdraw
    if (type == ResourceType::NET) {
        // Find and detach the proxy NIC for this resource
        // The proxy is identified by owner_node + resource_id stored in ProxyNetState
        // wki_remote_net_cleanup_for_peer handles by node, but we need per-resource detach
        // For now, log the withdrawal - full cleanup happens on fencing
        ker::mod::dbg::log("[WKI] NET resource withdrawn: node=0x%04x res_id=%u", adv->node_id, adv->resource_id);
    }

    // Remove from devfs /dev/wki/ tree (external, not a WKI container)
    ker::vfs::devfs::devfs_wki_remove_resource(adv->node_id, adv->resource_type, adv->resource_id);

    // Remove from discovered table
    size_t dropped_pending_mounts = 0;
    size_t dropped_pending_net_attaches = 0;
    s_remotable_lock.lock();
    std::erase_if(g_discovered, [&](const DiscoveredResource& res) {
        return res.node_id == adv->node_id && res.resource_type == type && res.resource_id == adv->resource_id;
    });
    if (type == ResourceType::VFS) {
        size_t pending_before = g_pending_vfs_mounts.size();
        std::erase_if(g_pending_vfs_mounts, [&](const PendingVfsMount& pending) {
            return pending.node_id == adv->node_id && pending.resource_id == adv->resource_id;
        });
        dropped_pending_mounts = pending_before - g_pending_vfs_mounts.size();
    } else if (type == ResourceType::NET) {
        size_t pending_before = g_pending_net_attaches.size();
        std::erase_if(g_pending_net_attaches, [&](const PendingNetAttach& pending) {
            return pending.node_id == adv->node_id && pending.resource_id == adv->resource_id;
        });
        dropped_pending_net_attaches = pending_before - g_pending_net_attaches.size();
    }
    s_remotable_lock.unlock();

    if (dropped_pending_mounts != 0) {
        log::debug("Dropped %llu stale VFS auto-mount(s): node=0x%04x res_id=%u",
                   static_cast<unsigned long long>(dropped_pending_mounts), adv->node_id, adv->resource_id);
    }
    if (dropped_pending_net_attaches != 0) {
        log::debug("Dropped %llu stale NET auto-attach(es): node=0x%04x res_id=%u",
                   static_cast<unsigned long long>(dropped_pending_net_attaches), adv->node_id, adv->resource_id);
    }

    ker::mod::dbg::log("[WKI] Resource withdrawn: node=0x%04x type=%u id=%u", adv->node_id, adv->resource_type, adv->resource_id);
}

}  // namespace detail

// -----------------------------------------------------------------------------
// Deferred VFS mount processing - called from timer tick (outside NAPI context)
// -----------------------------------------------------------------------------

void wki_remotable_process_pending_mounts() {
    while (true) {
        uint64_t now_us = wki_now_us();

        s_remotable_lock.lock();
        size_t pending_count = g_pending_vfs_mounts.size();
        if (pending_count == 0) {
            s_remotable_lock.unlock();
            break;
        }
        s_remotable_lock.unlock();

        bool processed_any = false;
        for (size_t i = 0; i < pending_count; i++) {
            s_remotable_lock.lock();
            if (g_pending_vfs_mounts.empty()) {
                s_remotable_lock.unlock();
                break;
            }
            PendingVfsMount pending = g_pending_vfs_mounts.front();
            g_pending_vfs_mounts.pop_front();
            s_remotable_lock.unlock();

            if (pending.next_attempt_us != 0 && now_us < pending.next_attempt_us) {
                s_remotable_lock.lock();
                g_pending_vfs_mounts.push_back(pending);
                s_remotable_lock.unlock();
                continue;
            }

            processed_any = true;

            WkiPeer* peer = wki_peer_find(pending.node_id);
            if (peer == nullptr || peer->state != PeerState::CONNECTED) {
                continue;
            }

            s_remotable_lock.lock();
            bool resource_live = pending_vfs_mount_is_live_locked(pending);
            s_remotable_lock.unlock();
            if (!resource_live) {
                log::debug("Skipping stale VFS auto-mount: node=0x%04x res_id=%u path=%s", pending.node_id, pending.resource_id,
                           pending.mount_path);
                continue;
            }

            char existing_mount_path[384] = {};
            bool mount_exists = wki_remote_vfs_find_mount_for_resource(pending.node_id, pending.resource_id, existing_mount_path,
                                                                       sizeof(existing_mount_path));
            if (mount_exists) {
                bool same_path = std::strncmp(existing_mount_path, pending.mount_path, sizeof(existing_mount_path)) == 0;
                if (!pending.force_remount && same_path) {
                    continue;
                }

                wki_remote_vfs_unmount(existing_mount_path);
            }

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
                continue;
            }

            if (pending.retry_count + 1 < VFS_AUTO_MOUNT_MAX_RETRIES) {
                pending.retry_count++;
                uint64_t delay_us = VFS_AUTO_MOUNT_RETRY_BASE_US << (pending.retry_count - 1);
                if (delay_us > VFS_AUTO_MOUNT_RETRY_MAX_US) {
                    delay_us = VFS_AUTO_MOUNT_RETRY_MAX_US;
                }
                pending.next_attempt_us = wki_now_us() + delay_us;

                s_remotable_lock.lock();
                g_pending_vfs_mounts.push_back(pending);
                s_remotable_lock.unlock();

                ker::mod::dbg::log("[WKI] VFS auto-mount retry %u/%u queued: %s (ret=%d)", pending.retry_count,
                                   VFS_AUTO_MOUNT_MAX_RETRIES - 1, pending.mount_path, ret);
            } else {
                ker::mod::dbg::log("[WKI] VFS auto-mount failed: %s (ret=%d)", pending.mount_path, ret);
            }
        }

        if (!processed_any) {
            break;
        }
    }
}

// V2: Process deferred NET auto-attaches - called from timer tick
void wki_remotable_process_pending_net_attaches() {
    while (true) {
        uint64_t now_us = wki_now_us();

        s_remotable_lock.lock();
        size_t pending_count = g_pending_net_attaches.size();
        if (pending_count == 0) {
            s_remotable_lock.unlock();
            break;
        }
        s_remotable_lock.unlock();

        bool processed_any = false;
        for (size_t i = 0; i < pending_count; i++) {
            s_remotable_lock.lock();
            if (g_pending_net_attaches.empty()) {
                s_remotable_lock.unlock();
                break;
            }
            PendingNetAttach pending = g_pending_net_attaches.front();
            g_pending_net_attaches.pop_front();
            s_remotable_lock.unlock();

            if (pending.next_attempt_us != 0 && now_us < pending.next_attempt_us) {
                s_remotable_lock.lock();
                g_pending_net_attaches.push_back(pending);
                s_remotable_lock.unlock();
                continue;
            }

            processed_any = true;

            // Check if peer is still connected
            WkiPeer* peer = wki_peer_find(pending.node_id);
            if (peer == nullptr || peer->state != PeerState::CONNECTED) {
                ker::mod::dbg::log("[WKI] NET auto-attach skipped: peer 0x%04x not connected", pending.node_id);
                continue;
            }

            s_remotable_lock.lock();
            bool resource_live = pending_net_attach_is_live_locked(pending);
            s_remotable_lock.unlock();
            if (!resource_live) {
                log::debug("Skipping stale NET auto-attach: node=0x%04x res_id=%u nic=%s", pending.node_id, pending.resource_id,
                           pending.nic_name);
                continue;
            }

            if (wki_remote_net_has_proxy(pending.node_id, pending.resource_id)) {
                continue;
            }

            if (ker::net::netdev_find_by_name(pending.nic_name) != nullptr) {
                log::debug("Skipping duplicate NET auto-attach name: %s node=0x%04x res_id=%u", pending.nic_name, pending.node_id,
                           pending.resource_id);
                continue;
            }

            // ATTACH_SPARSE: subnet overlap check is done after attach (since we need
            // the ACK to get IP info). For ATTACH_ALL, skip the check entirely.
            // The actual subnet check happens post-attach: if overlap is found, we detach.
            ker::net::NetDevice* proxy_dev = wki_remote_net_attach(pending.node_id, pending.resource_id, pending.nic_name);
            if (proxy_dev == nullptr) {
                bool requeued = requeue_net_attach(pending);
                ker::mod::dbg::log("[WKI] NET auto-attach failed: %s -> node=0x%04x%s", pending.nic_name, pending.node_id,
                                   requeued ? " (retry queued)" : "");
                continue;
            }

            auto* proxy_state = static_cast<ProxyNetState*>(proxy_dev->private_data);

            // V2: ATTACH_SPARSE post-attach subnet overlap check
            if (g_wki.nic_policy == WkiNicPolicy::ATTACH_SPARSE) {
                // A zero owner IP means the remote NIC was advertised before its
                // address was configured. Do not leave a useless proxy around;
                // detach and retry while DHCP/static config has a chance to settle.
                if (proxy_state == nullptr || proxy_state->owner_ipv4_addr == 0 || proxy_state->owner_ipv4_mask == 0) {
                    wki_remote_net_detach(proxy_dev);
                    bool requeued = requeue_net_attach(pending);
                    if (requeued) {
                        log::debug("NET auto-attach deferred: %s from %s/%s has no IPv4 yet (retry %u/%u)", pending.nic_name,
                                   pending.hostname, pending.remote_name, pending.retry_count, NET_AUTO_ATTACH_MAX_RETRIES - 1);
                    } else {
                        ker::mod::dbg::log("[WKI] NET auto-attach skipped: %s from %s/%s has no IPv4", pending.nic_name,
                                           pending.hostname, pending.remote_name);
                    }
                    continue;
                }

                if (has_local_subnet_overlap(proxy_state->owner_ipv4_addr, proxy_state->owner_ipv4_mask)) {
                    ker::mod::dbg::log("[WKI] Skipping remote NIC %s from %s: subnet overlap", pending.nic_name, pending.hostname);
                    wki_remote_net_detach(proxy_dev);
                    continue;
                }
            }

            ker::mod::dbg::log("[WKI] NET auto-attached: %s -> node=0x%04x", pending.nic_name, pending.node_id);

            // In sparse mode we intentionally avoid auto-installing routes.
            // Remote NIC discovery happens before local DHCP/static configuration
            // is necessarily complete, so eagerly adding a same-subnet route can
            // steal local traffic (e.g. SSH/NTP) until the real NIC is configured.
            // ATTACH_ALL keeps the old behavior and auto-routes every remote NIC.
            if (g_wki.nic_policy == WkiNicPolicy::ATTACH_ALL && proxy_state != nullptr && proxy_state->owner_ipv4_addr != 0 &&
                proxy_state->owner_ipv4_mask != 0) {
                uint32_t remote_subnet = proxy_state->owner_ipv4_addr & proxy_state->owner_ipv4_mask;
                int route_ret = ker::net::route_add(remote_subnet, proxy_state->owner_ipv4_mask, 0 /*gateway*/, 100 /*metric*/, proxy_dev);
                if (route_ret == 0) {
                    ker::mod::dbg::log("[WKI] Route installed: subnet=0x%08x mask=0x%08x via %s", remote_subnet,
                                       proxy_state->owner_ipv4_mask, pending.nic_name);
                }
            }
        }

        if (!processed_any) {
            break;
        }
    }
}

}  // namespace ker::net::wki
