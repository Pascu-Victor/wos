#include "remotable.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <dev/block_device.hpp>
#include <net/address.hpp>
#include <net/netdevice.hpp>
#include <net/netif.hpp>
#include <net/route.hpp>
#include <net/wki/dev_server.hpp>
#include <net/wki/remote_net.hpp>
#include <net/wki/remote_vfs.hpp>
#include <net/wki/timer_math.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <vfs/fs/devfs.hpp>
#include <vfs/mount.hpp>
#include <vfs/vfs.hpp>

#include "platform/sys/spinlock.hpp"

namespace ker::net::wki {

using log = ker::mod::dbg::logger<"wki">;

// -----------------------------------------------------------------------------
// Storage - discovered resources from remote peers
// -----------------------------------------------------------------------------

namespace {
std::deque<DiscoveredResource> g_discovered;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
bool g_remotable_initialized = false;         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
constexpr size_t VFS_MOUNT_PATH_LEN = 384;
constexpr size_t NET_AUTO_ATTACH_NAME_LEN = 64;
constexpr size_t VFS_HOST_DIR_LEN = 256;

// Deferred VFS mount queue - wki_remote_vfs_mount() spin-waits for an attach
// ACK, but handle_resource_advert() runs inside the NAPI poll handler where
// inline NAPI draining cannot re-enter the current poll handler. Queue the
// mount and process it from the timer tick (outside NAPI context).
struct PendingVfsMount {
    uint16_t node_id;
    uint32_t resource_id;
    std::array<char, VFS_MOUNT_PATH_LEN> mount_path{};
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
    std::array<char, NET_AUTO_ATTACH_NAME_LEN> nic_name{};
    std::array<char, NET_AUTO_ATTACH_NAME_LEN> hostname{};
    std::array<char, NET_AUTO_ATTACH_NAME_LEN> remote_name{};
    uint8_t retry_count = 0;
    uint64_t next_attempt_us = 0;
};
std::deque<PendingNetAttach> g_pending_net_attaches;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
constexpr uint8_t NET_AUTO_ATTACH_MAX_RETRIES = 8;
constexpr uint64_t NET_AUTO_ATTACH_RETRY_BASE_US = 1000000;
constexpr uint64_t NET_AUTO_ATTACH_RETRY_MAX_US = 5000000;
constexpr uint64_t NET_AUTO_ATTACH_LOCAL_IPV4_RETRY_US = 500000;

ker::mod::sys::Spinlock s_remotable_lock;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

struct NetAdvertState {
    uint32_t ipv4_addr = 0;
    uint32_t ipv4_mask = 0;
    proto::MacAddress real_mac;
    uint16_t link_state = 0;
    uint32_t mtu = 1500;
};

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
    uint32_t const REMOTE_SUBNET = remote_ip & remote_mask;

    size_t const NDEV_COUNT = ker::net::netdev_count();
    for (size_t i = 0; i < NDEV_COUNT; i++) {
        ker::net::NetDevice* dev = ker::net::netdev_at(i);
        if (dev == nullptr) {
            continue;
        }

        auto* nif = ker::net::netif_get(dev);
        if (nif == nullptr) {
            continue;
        }

        for (size_t j = 0; j < nif->ipv4_addr_count; j++) {
            auto const LOCAL_ADDR = nif->ipv4_addrs.at(j).addr.to_host_order();
            auto const LOCAL_MASK = nif->ipv4_addrs.at(j).netmask.to_host_order();
            uint32_t const LOCAL_SUBNET = LOCAL_ADDR & LOCAL_MASK;
            if (LOCAL_SUBNET == REMOTE_SUBNET && LOCAL_MASK == remote_mask) {
                return true;
            }
        }
    }
    return false;
}

auto is_local_l3_candidate(ker::net::NetDevice const* dev) -> bool {
    if (dev == nullptr || dev->wki_transport) {
        return false;
    }
    if (std::strncmp(dev->name.data(), "lo", ker::net::NETDEV_NAME_LEN) == 0) {
        return false;
    }
    return std::strncmp(dev->name.data(), "wki-", 4) != 0;
}

auto local_ipv4_configuration_pending() -> bool {
    bool found_candidate = false;
    size_t const NDEV_COUNT = ker::net::netdev_count();
    for (size_t i = 0; i < NDEV_COUNT; i++) {
        ker::net::NetDevice* dev = ker::net::netdev_at(i);
        if (!is_local_l3_candidate(dev)) {
            continue;
        }

        found_candidate = true;
        auto* nif = ker::net::netif_get(dev);
        if (nif != nullptr && nif->ipv4_addr_count != 0) {
            return false;
        }
    }
    return found_candidate;
}

// Build the auto-mount path for a VFS export:
//   export "/"      -> "/wki/<hostname>"
//   export "/proc"  -> "/wki/<hostname>/proc"
//   export "data"   -> "/wki/<hostname>/data"
void build_vfs_mount_path(char* out, size_t out_size, const char* hostname, const char* export_name) {
    // Strip leading '/' from export name
    const char* stripped = export_name;
    while (*stripped == '/') {
        stripped++;
    }

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

auto pending_vfs_mount_matches_resource_locked(const PendingVfsMount& pending, const DiscoveredResource& res) -> bool {
    const char* hostname = wki_peer_get_hostname(pending.node_id);
    if (hostname == nullptr || hostname[0] == '\0') {
        return false;
    }

    std::array<char, VFS_MOUNT_PATH_LEN> expected_mount_path{};
    build_vfs_mount_path(expected_mount_path.data(), expected_mount_path.size(), hostname, static_cast<const char*>(res.name));
    return std::strncmp(expected_mount_path.data(), pending.mount_path.data(), expected_mount_path.size()) == 0;
}

auto forget_stale_vfs_resource_after_not_found(const PendingVfsMount& pending) -> bool {
    bool removed = false;
    s_remotable_lock.lock();
    auto* resource = find_resource_unlocked(pending.node_id, ResourceType::VFS, pending.resource_id);
    if (resource != nullptr && pending_vfs_mount_matches_resource_locked(pending, *resource)) {
        std::erase_if(g_discovered, [&](const DiscoveredResource& res) {
            return res.node_id == pending.node_id && res.resource_type == ResourceType::VFS && res.resource_id == pending.resource_id;
        });
        std::erase_if(g_pending_vfs_mounts, [&](const PendingVfsMount& queued) {
            return queued.node_id == pending.node_id && queued.resource_id == pending.resource_id &&
                   std::strncmp(queued.mount_path.data(), pending.mount_path.data(), queued.mount_path.size()) == 0;
        });
        removed = true;
    }
    s_remotable_lock.unlock();

    if (removed) {
        ker::vfs::devfs::devfs_wki_remove_resource(pending.node_id, static_cast<uint16_t>(ResourceType::VFS), pending.resource_id);
    }
    return removed;
}

auto capture_net_advert_state(ker::net::NetDevice* dev) -> NetAdvertState {
    NetAdvertState state = {};
    if (dev == nullptr) {
        return state;
    }

    auto* nif = ker::net::netif_get(dev);
    if (nif != nullptr && nif->ipv4_addr_count != 0) {
        state.ipv4_addr = nif->ipv4_addrs.front().addr;
        state.ipv4_mask = nif->ipv4_addrs.front().netmask;
    }

    state.real_mac = dev->mac;
    state.link_state = static_cast<uint16_t>(dev->state != 0 ? 1 : 0);
    state.mtu = dev->mtu != 0 ? dev->mtu : 1500;
    return state;
}

auto net_resource_ready(const DiscoveredResource& res) -> bool { return res.net_ipv4_addr != 0 && res.net_ipv4_mask != 0; }

void update_net_resource_fields(DiscoveredResource& res, const ResourceAdvertNetPayload& adv) {
    res.net_ipv4_addr = adv.ipv4_addr;
    res.net_ipv4_mask = adv.ipv4_mask;
    res.net_real_mac = adv.real_mac;
    res.net_link_state = adv.link_state;
    res.net_mtu = adv.mtu != 0 ? adv.mtu : 1500;
}

void queue_vfs_mount_locked(uint16_t node_id, uint32_t resource_id, const char* mount_path, bool force_remount) {
    if (mount_path == nullptr || mount_path[0] == '\0') {
        return;
    }

    for (auto& pending : g_pending_vfs_mounts) {
        if (pending.node_id != node_id) {
            continue;
        }

        bool const SAME_RESOURCE = pending.resource_id == resource_id;
        bool const SAME_MOUNT_PATH = std::strncmp(pending.mount_path.data(), mount_path, pending.mount_path.size()) == 0;
        if (!SAME_RESOURCE && !SAME_MOUNT_PATH) {
            continue;
        }

        pending.resource_id = resource_id;
        std::snprintf(pending.mount_path.data(), pending.mount_path.size(), "%s", mount_path);
        pending.force_remount = pending.force_remount || force_remount;
        pending.retry_count = 0;
        pending.next_attempt_us = 0;
        return;
    }

    PendingVfsMount pending = {};
    pending.node_id = node_id;
    pending.resource_id = resource_id;
    pending.force_remount = force_remount;
    std::snprintf(pending.mount_path.data(), pending.mount_path.size(), "%s", mount_path);
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

        bool const SAME_RESOURCE = pending.resource_id == resource_id;
        bool const SAME_NAME = std::strncmp(pending.nic_name.data(), nic_name, pending.nic_name.size()) == 0;
        if (!SAME_RESOURCE && !SAME_NAME) {
            continue;
        }

        pending.resource_id = resource_id;
        std::snprintf(pending.nic_name.data(), pending.nic_name.size(), "%s", nic_name);
        std::snprintf(pending.hostname.data(), pending.hostname.size(), "%s", hostname);
        std::snprintf(pending.remote_name.data(), pending.remote_name.size(), "%s", remote_name != nullptr ? remote_name : "");
        pending.retry_count = retry_count;
        pending.next_attempt_us = next_attempt_us;
        return;
    }

    PendingNetAttach pending = {};
    pending.node_id = node_id;
    pending.resource_id = resource_id;
    pending.retry_count = retry_count;
    pending.next_attempt_us = next_attempt_us;
    std::snprintf(pending.nic_name.data(), pending.nic_name.size(), "%s", nic_name);
    std::snprintf(pending.hostname.data(), pending.hostname.size(), "%s", hostname);
    std::snprintf(pending.remote_name.data(), pending.remote_name.size(), "%s", remote_name != nullptr ? remote_name : "");
    g_pending_net_attaches.push_back(pending);
}

auto requeue_net_attach(PendingNetAttach& pending) -> bool {
    if (pending.retry_count + 1 >= NET_AUTO_ATTACH_MAX_RETRIES) {
        return false;
    }

    pending.retry_count++;
    uint64_t delay_us = NET_AUTO_ATTACH_RETRY_BASE_US << (pending.retry_count - 1);
    delay_us = std::min(delay_us, NET_AUTO_ATTACH_RETRY_MAX_US);
    pending.next_attempt_us = wki_future_deadline_us(wki_now_us(), delay_us);

    s_remotable_lock.lock();
    queue_net_attach_locked(pending.node_id, pending.resource_id, pending.nic_name.data(), pending.hostname.data(),
                            pending.remote_name.data(), pending.retry_count, pending.next_attempt_us);
    s_remotable_lock.unlock();
    return true;
}

void defer_net_attach_for_local_ipv4(PendingNetAttach& pending) {
    pending.next_attempt_us = wki_future_deadline_us(wki_now_us(), NET_AUTO_ATTACH_LOCAL_IPV4_RETRY_US);

    s_remotable_lock.lock();
    queue_net_attach_locked(pending.node_id, pending.resource_id, pending.nic_name.data(), pending.hostname.data(),
                            pending.remote_name.data(), pending.retry_count, pending.next_attempt_us);
    s_remotable_lock.unlock();
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
    while (name_len < DISCOVERED_RESOURCE_NAME_LEN - 1 && bdev->name.at(name_len) != '\0') {
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
    adv->flags |= RESOURCE_FLAG_READABLE;
    if (ker::vfs::mounted_block_device_overlaps(bdev) || wki_dev_server_block_has_remote_writer(bdev)) {
        adv->flags |= RESOURCE_FLAG_OCCUPIED;
    } else if (!ker::dev::block_device_is_read_only(bdev)) {
        adv->flags |= RESOURCE_FLAG_WRITABLE;
    }

    adv->name_len = name_len;
    memcpy(buf.data() + sizeof(ResourceAdvertPayload), bdev->name.data(), name_len);

    wki_send(peer_node, WKI_CHAN_CONTROL, MsgType::RESOURCE_ADVERT, buf.data(), total_len);
}

void send_net_resource_advert_to_peer(uint16_t peer_node, ker::net::NetDevice* ndev, uint32_t resource_id) {
    uint8_t name_len = 0;
    while (name_len < DISCOVERED_RESOURCE_NAME_LEN - 1 && ndev->name.at(name_len) != '\0') {
        name_len++;
    }

    auto total_len = static_cast<uint16_t>(sizeof(ResourceAdvertNetPayload) + name_len);
    std::array<uint8_t, sizeof(ResourceAdvertNetPayload) + 64> buf{};

    auto* adv = reinterpret_cast<ResourceAdvertNetPayload*>(buf.data());
    adv->node_id = g_wki.my_node_id;
    adv->resource_type = static_cast<uint16_t>(ResourceType::NET);
    adv->resource_id = resource_id;
    adv->flags = 0;
    adv->reserved = 0;

    if (ndev->remotable != nullptr) {
        if (ndev->remotable->can_share()) {
            adv->flags |= RESOURCE_FLAG_SHAREABLE;
        }
        if (ndev->remotable->can_passthrough()) {
            adv->flags |= RESOURCE_FLAG_PASSTHROUGH_CAPABLE;
        }
    }

    NetAdvertState const STATE = capture_net_advert_state(ndev);
    adv->ipv4_addr = STATE.ipv4_addr;
    adv->ipv4_mask = STATE.ipv4_mask;
    adv->real_mac = STATE.real_mac;
    adv->link_state = STATE.link_state;
    adv->mtu = STATE.mtu;

    adv->name_len = name_len;
    memcpy(buf.data() + sizeof(ResourceAdvertNetPayload), ndev->name.data(), name_len);

    wki_send(peer_node, WKI_CHAN_CONTROL, MsgType::RESOURCE_ADVERT, buf.data(), total_len);
}

void advertise_resources_to_peer(uint16_t peer_node) {
    WkiPeer const* peer = wki_peer_find(peer_node);
    if (peer == nullptr || peer->state != PeerState::CONNECTED) {
        return;
    }

    // Iterate all registered block devices
    size_t const COUNT = ker::dev::block_device_count();
    for (size_t i = 0; i < COUNT; i++) {
        ker::dev::BlockDevice* bdev = ker::dev::block_device_at(i);
        if (bdev == nullptr || bdev->remotable == nullptr) {
            continue;
        }
        if (!bdev->remotable->can_remote()) {
            continue;
        }

        auto resource_id = static_cast<uint32_t>(i);
        send_resource_advert_to_peer(peer_node, bdev, resource_id);
    }

    wki_remote_vfs_advertise_exports_to_peer(peer_node);

    // Iterate all registered net devices
    size_t const NDEV_COUNT = ker::net::netdev_count();
    for (size_t i = 0; i < NDEV_COUNT; i++) {
        ker::net::NetDevice* ndev = ker::net::netdev_at(i);
        if (ndev == nullptr || ndev->remotable == nullptr) {
            continue;
        }
        if (!ndev->remotable->can_remote()) {
            continue;
        }

        auto net_resource_id = ndev->ifindex;
        send_net_resource_advert_to_peer(peer_node, ndev, net_resource_id);
    }
}

}  // namespace

void wki_resource_advertise_all() {
    if (!g_remotable_initialized) {
        return;
    }

    // Keep the export set up to date before broadcasting it.
    wki_remote_vfs_refresh_exports();

    for (size_t p = 0; p < WKI_MAX_PEERS; p++) {
        WkiPeer const* peer = &g_wki.peers.at(p);
        if (peer->node_id == WKI_NODE_INVALID || peer->state != PeerState::CONNECTED) {
            continue;
        }
        advertise_resources_to_peer(peer->node_id);
    }
}

void wki_resource_advertise_to_peer(uint16_t peer_node) {
    if (!g_remotable_initialized || peer_node == WKI_NODE_INVALID) {
        return;
    }

    wki_remote_vfs_refresh_exports();
    advertise_resources_to_peer(peer_node);
}

void wki_remotable_notify_net_changed(ker::net::NetDevice* dev) {
    if (!g_remotable_initialized || dev == nullptr || dev->remotable == nullptr || !dev->remotable->can_remote()) {
        return;
    }

    for (size_t p = 0; p < WKI_MAX_PEERS; p++) {
        WkiPeer const* peer = &g_wki.peers.at(p);
        if (peer->node_id == WKI_NODE_INVALID || peer->state != PeerState::CONNECTED) {
            continue;
        }
        send_net_resource_advert_to_peer(peer->node_id, dev, dev->ifindex);
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

    // Ignore adverts from ourselves
    if (adv->node_id == g_wki.my_node_id) {
        return;
    }

    auto type = static_cast<ResourceType>(adv->resource_type);
    const ResourceAdvertNetPayload* net_adv = nullptr;
    size_t header_size = sizeof(ResourceAdvertPayload);
    if (type == ResourceType::NET && payload_len >= sizeof(ResourceAdvertNetPayload)) {
        net_adv = reinterpret_cast<const ResourceAdvertNetPayload*>(payload);
        header_size = sizeof(ResourceAdvertNetPayload);
    }

    if (header_size + adv->name_len > payload_len) {
        return;
    }

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
    if (net_adv != nullptr) {
        update_net_resource_fields(res, *net_adv);
        memcpy(static_cast<void*>(res.name), resource_advert_name(net_adv), copy_len);
    } else {
        memcpy(static_cast<void*>(res.name), resource_advert_name(adv), copy_len);
    }
    res.name[copy_len] = '\0';

    std::array<char, VFS_MOUNT_PATH_LEN> desired_mount_path{};
    bool queue_vfs_mount = false;
    s_remotable_lock.lock();

    // Check if we already have this resource (upsert)
    DiscoveredResource* existing = find_resource_unlocked(adv->node_id, type, adv->resource_id);
    if (existing != nullptr) {
        bool const NET_READY_BEFORE = type == ResourceType::NET ? net_resource_ready(*existing) : false;
        bool const NAME_CHANGED =
            std::strncmp(static_cast<const char*>(existing->name), static_cast<const char*>(res.name), sizeof(existing->name)) != 0;
        bool const FLAGS_CHANGED = existing->flags != adv->flags;
        bool net_fields_changed = false;
        if (type == ResourceType::NET) {
            net_fields_changed = existing->net_ipv4_addr != res.net_ipv4_addr || existing->net_ipv4_mask != res.net_ipv4_mask ||
                                 existing->net_real_mac != res.net_real_mac || existing->net_link_state != res.net_link_state ||
                                 existing->net_mtu != res.net_mtu;
        }

        // Refresh mutable fields so reconnect/re-advertisement can correct a
        // stale visible name without requiring a reboot.
        existing->flags = adv->flags;
        memcpy(static_cast<void*>(existing->name), static_cast<const void*>(res.name), sizeof(existing->name));
        if (type == ResourceType::NET) {
            existing->net_ipv4_addr = res.net_ipv4_addr;
            existing->net_ipv4_mask = res.net_ipv4_mask;
            existing->net_real_mac = res.net_real_mac;
            existing->net_link_state = res.net_link_state;
            existing->net_mtu = res.net_mtu;
        }

        if (!NAME_CHANGED && !FLAGS_CHANGED && !net_fields_changed) {
            s_remotable_lock.unlock();
            return;
        }

        if (type == ResourceType::VFS) {
            const char* hostname = wki_peer_get_hostname(adv->node_id);
            if (hostname != nullptr && hostname[0] != '\0') {
                build_vfs_mount_path(desired_mount_path.data(), desired_mount_path.size(), hostname, static_cast<const char*>(res.name));
                queue_vfs_mount_locked(adv->node_id, adv->resource_id, desired_mount_path.data(), true);
                queue_vfs_mount = true;
            }
        } else if (type == ResourceType::NET && g_wki.nic_policy != WkiNicPolicy::MANUAL) {
            const char* hostname = wki_peer_get_hostname(adv->node_id);
            bool const NET_READY_AFTER = net_resource_ready(*existing);
            bool const SHOULD_ATTACH = !wki_remote_net_has_proxy(adv->node_id, adv->resource_id) && NET_READY_AFTER &&
                                       (NAME_CHANGED || net_fields_changed || !NET_READY_BEFORE);
            if (hostname != nullptr && hostname[0] != '\0' && SHOULD_ATTACH) {
                std::array<char, NET_AUTO_ATTACH_NAME_LEN> nic_name{};
                build_net_proxy_name(nic_name.data(), nic_name.size(), hostname, static_cast<const char*>(res.name), adv->resource_id);
                queue_net_attach_locked(adv->node_id, adv->resource_id, nic_name.data(), hostname, static_cast<const char*>(res.name));
            }
        }

        s_remotable_lock.unlock();

        if (queue_vfs_mount && NAME_CHANGED) {
            ker::mod::dbg::log("[WKI] VFS resource renamed: node=0x%04x res_id=%u -> %s", adv->node_id, adv->resource_id,
                               desired_mount_path.data());
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
            build_vfs_mount_path(desired_mount_path.data(), desired_mount_path.size(), hostname, static_cast<const char*>(res.name));
            queue_vfs_mount_locked(adv->node_id, adv->resource_id, desired_mount_path.data(), false);
        }
    }

    // V2: Queue deferred NET auto-attach based on NIC policy
    if (type == ResourceType::NET && g_wki.nic_policy != WkiNicPolicy::MANUAL && net_resource_ready(res)) {
        const char* hostname = wki_peer_get_hostname(adv->node_id);
        if (hostname != nullptr && hostname[0] != '\0') {
            std::array<char, NET_AUTO_ATTACH_NAME_LEN> nic_name{};
            build_net_proxy_name(nic_name.data(), nic_name.size(), hostname, static_cast<const char*>(res.name), adv->resource_id);
            queue_net_attach_locked(adv->node_id, adv->resource_id, nic_name.data(), hostname, static_cast<const char*>(res.name));
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
    std::array<char, VFS_MOUNT_PATH_LEN> mount_path{};
    bool do_vfs_unmount = false;
    if (type == ResourceType::VFS) {
        do_vfs_unmount = wki_remote_vfs_find_mount_for_resource(adv->node_id, adv->resource_id, mount_path.data(), mount_path.size());
    }

    // Blocking VFS unmount - outside lock
    if (do_vfs_unmount) {
        wki_remote_vfs_unmount(mount_path.data());
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
        size_t const PENDING_BEFORE = g_pending_vfs_mounts.size();
        std::erase_if(g_pending_vfs_mounts, [&](const PendingVfsMount& pending) {
            return pending.node_id == adv->node_id && pending.resource_id == adv->resource_id;
        });
        dropped_pending_mounts = PENDING_BEFORE - g_pending_vfs_mounts.size();
    } else if (type == ResourceType::NET) {
        size_t const PENDING_BEFORE = g_pending_net_attaches.size();
        std::erase_if(g_pending_net_attaches, [&](const PendingNetAttach& pending) {
            return pending.node_id == adv->node_id && pending.resource_id == adv->resource_id;
        });
        dropped_pending_net_attaches = PENDING_BEFORE - g_pending_net_attaches.size();
    }
    s_remotable_lock.unlock();

    if (dropped_pending_mounts != 0) {
        log::debug("Dropped %llu stale VFS auto-mount(s): node=0x%04x res_id=%u", static_cast<unsigned long long>(dropped_pending_mounts),
                   adv->node_id, adv->resource_id);
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
        uint64_t const NOW_US = wki_now_us();

        s_remotable_lock.lock();
        size_t const PENDING_COUNT = g_pending_vfs_mounts.size();
        if (PENDING_COUNT == 0) {
            s_remotable_lock.unlock();
            break;
        }
        s_remotable_lock.unlock();

        bool processed_any = false;
        for (size_t i = 0; i < PENDING_COUNT; i++) {
            s_remotable_lock.lock();
            if (g_pending_vfs_mounts.empty()) {
                s_remotable_lock.unlock();
                break;
            }
            PendingVfsMount pending = g_pending_vfs_mounts.front();
            g_pending_vfs_mounts.pop_front();
            s_remotable_lock.unlock();

            if (pending.next_attempt_us != 0 && NOW_US < pending.next_attempt_us) {
                s_remotable_lock.lock();
                g_pending_vfs_mounts.push_back(pending);
                s_remotable_lock.unlock();
                continue;
            }

            processed_any = true;

            WkiPeer const* peer = wki_peer_find(pending.node_id);
            if (peer == nullptr || peer->state != PeerState::CONNECTED) {
                continue;
            }

            s_remotable_lock.lock();
            bool const RESOURCE_LIVE = pending_vfs_mount_is_live_locked(pending);
            s_remotable_lock.unlock();
            if (!RESOURCE_LIVE) {
                log::debug("Skipping stale VFS auto-mount: node=0x%04x res_id=%u path=%s", pending.node_id, pending.resource_id,
                           pending.mount_path.data());
                continue;
            }

            std::array<char, VFS_MOUNT_PATH_LEN> existing_mount_path{};
            bool const MOUNT_EXISTS = wki_remote_vfs_find_mount_for_resource(pending.node_id, pending.resource_id,
                                                                             existing_mount_path.data(), existing_mount_path.size());
            if (MOUNT_EXISTS) {
                bool const SAME_PATH = std::strncmp(existing_mount_path.data(), pending.mount_path.data(), existing_mount_path.size()) == 0;
                if (!pending.force_remount && SAME_PATH) {
                    continue;
                }

                wki_remote_vfs_unmount(existing_mount_path.data());
            } else {
                uint32_t mounted_resource_id = 0;
                if (wki_remote_vfs_find_resource_for_mount(pending.node_id, pending.mount_path.data(), &mounted_resource_id) &&
                    mounted_resource_id != pending.resource_id) {
                    log::debug("Replacing VFS auto-mount: node=0x%04x path=%s old_res_id=%u new_res_id=%u", pending.node_id,
                               pending.mount_path.data(), mounted_resource_id, pending.resource_id);
                    wki_remote_vfs_unmount(pending.mount_path.data());
                }
            }

            // Create intermediate directories (/wki/<hostname>)
            // Find the second '/' after "/wki/" to get host dir
            std::array<char, VFS_HOST_DIR_LEN> host_dir{};
            const char* mount_path = pending.mount_path.data();
            const char* p = mount_path + 5;  // skip "/wki/"
            const char* slash = p;
            while (*slash != '\0' && *slash != '/') {
                slash++;
            }
            auto host_dir_len = static_cast<size_t>(slash - mount_path);
            if (host_dir_len < host_dir.size()) {
                std::copy_n(mount_path, host_dir_len, host_dir.data());
                host_dir.at(host_dir_len) = '\0';
                ker::vfs::vfs_mkdir(host_dir.data(), 0755);
            }

            // Create the full mount path directory
            ker::vfs::vfs_mkdir(pending.mount_path.data(), 0755);

            int const RET = wki_remote_vfs_mount(pending.node_id, pending.resource_id, pending.mount_path.data());
            if (RET == 0) {
                ker::mod::dbg::log("[WKI] Auto-mounted VFS: %s -> node=0x%04x", pending.mount_path.data(), pending.node_id);
                continue;
            }

            if (RET == -ENOENT) {
                if (forget_stale_vfs_resource_after_not_found(pending)) {
                    log::debug("Dropped stale VFS resource after attach NOT_FOUND: node=0x%04x res_id=%u path=%s", pending.node_id,
                               pending.resource_id, pending.mount_path.data());
                } else {
                    log::debug("Skipping VFS auto-mount after attach NOT_FOUND: node=0x%04x res_id=%u path=%s", pending.node_id,
                               pending.resource_id, pending.mount_path.data());
                }
                continue;
            }

            if (pending.retry_count + 1 < VFS_AUTO_MOUNT_MAX_RETRIES) {
                pending.retry_count++;
                uint64_t delay_us = VFS_AUTO_MOUNT_RETRY_BASE_US << (pending.retry_count - 1);
                delay_us = std::min(delay_us, VFS_AUTO_MOUNT_RETRY_MAX_US);
                pending.next_attempt_us = wki_future_deadline_us(wki_now_us(), delay_us);

                s_remotable_lock.lock();
                g_pending_vfs_mounts.push_back(pending);
                s_remotable_lock.unlock();

                ker::mod::dbg::log("[WKI] VFS auto-mount retry %u/%u queued: %s (ret=%d)", pending.retry_count,
                                   VFS_AUTO_MOUNT_MAX_RETRIES - 1, pending.mount_path.data(), RET);
            } else {
                ker::mod::dbg::log("[WKI] VFS auto-mount failed: %s (ret=%d)", pending.mount_path.data(), RET);
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
        uint64_t const NOW_US = wki_now_us();

        s_remotable_lock.lock();
        size_t const PENDING_COUNT = g_pending_net_attaches.size();
        if (PENDING_COUNT == 0) {
            s_remotable_lock.unlock();
            break;
        }
        s_remotable_lock.unlock();

        bool processed_any = false;
        for (size_t i = 0; i < PENDING_COUNT; i++) {
            s_remotable_lock.lock();
            if (g_pending_net_attaches.empty()) {
                s_remotable_lock.unlock();
                break;
            }
            PendingNetAttach pending = g_pending_net_attaches.front();
            g_pending_net_attaches.pop_front();
            s_remotable_lock.unlock();

            if (pending.next_attempt_us != 0 && NOW_US < pending.next_attempt_us) {
                s_remotable_lock.lock();
                g_pending_net_attaches.push_back(pending);
                s_remotable_lock.unlock();
                continue;
            }

            processed_any = true;

            // Check if peer is still connected
            WkiPeer const* peer = wki_peer_find(pending.node_id);
            if (peer == nullptr || peer->state != PeerState::CONNECTED) {
                ker::mod::dbg::log("[WKI] NET auto-attach skipped: peer 0x%04x not connected", pending.node_id);
                continue;
            }

            uint32_t remote_ipv4_addr = 0;
            uint32_t remote_ipv4_mask = 0;
            s_remotable_lock.lock();
            DiscoveredResource const* live_resource = find_resource_unlocked(pending.node_id, ResourceType::NET, pending.resource_id);
            bool const RESOURCE_LIVE = live_resource != nullptr;
            bool const RESOURCE_READY = RESOURCE_LIVE && net_resource_ready(*live_resource);
            if (RESOURCE_READY) {
                remote_ipv4_addr = live_resource->net_ipv4_addr;
                remote_ipv4_mask = live_resource->net_ipv4_mask;
            }
            s_remotable_lock.unlock();
            if (!RESOURCE_LIVE) {
                log::debug("Skipping stale NET auto-attach: node=0x%04x res_id=%u nic=%s", pending.node_id, pending.resource_id,
                           pending.nic_name.data());
                continue;
            }
            if (!RESOURCE_READY) {
                log::debug("Skipping unready NET auto-attach: node=0x%04x res_id=%u nic=%s", pending.node_id, pending.resource_id,
                           pending.nic_name.data());
                continue;
            }

            if (wki_remote_net_has_proxy(pending.node_id, pending.resource_id)) {
                continue;
            }

            if (ker::net::netdev_find_by_name(pending.nic_name.data()) != nullptr) {
                log::debug("Skipping duplicate NET auto-attach name: %s node=0x%04x res_id=%u", pending.nic_name.data(), pending.node_id,
                           pending.resource_id);
                continue;
            }

            if (g_wki.nic_policy == WkiNicPolicy::ATTACH_SPARSE && local_ipv4_configuration_pending()) {
                defer_net_attach_for_local_ipv4(pending);
                log::debug("Deferring NET auto-attach: %s waits for local IPv4 configuration", pending.nic_name.data());
                continue;
            }

            if (g_wki.nic_policy == WkiNicPolicy::ATTACH_SPARSE && has_local_subnet_overlap(remote_ipv4_addr, remote_ipv4_mask)) {
                ker::mod::dbg::log("[WKI] Skipping remote NIC %s from %s: subnet overlap", pending.nic_name.data(),
                                   pending.hostname.data());
                continue;
            }

            ker::net::NetDevice* proxy_dev = wki_remote_net_attach(pending.node_id, pending.resource_id, pending.nic_name.data());
            if (proxy_dev == nullptr) {
                bool const REQUEUED = requeue_net_attach(pending);
                ker::mod::dbg::log("[WKI] NET auto-attach failed: %s -> node=0x%04x%s", pending.nic_name.data(), pending.node_id,
                                   REQUEUED ? " (retry queued)" : "");
                continue;
            }

            auto* proxy_state = static_cast<ProxyNetState*>(proxy_dev->private_data);

            // V2: ATTACH_SPARSE post-attach subnet overlap check
            if (g_wki.nic_policy == WkiNicPolicy::ATTACH_SPARSE) {
                if (proxy_state == nullptr || proxy_state->owner_ipv4_addr == 0 || proxy_state->owner_ipv4_mask == 0) {
                    wki_remote_net_detach(proxy_dev);
                    log::debug("Dropping stale NET auto-attach: %s from %s/%s is still missing IPv4 after ready advert",
                               pending.nic_name.data(), pending.hostname.data(), pending.remote_name.data());
                    continue;
                }

                if (has_local_subnet_overlap(proxy_state->owner_ipv4_addr, proxy_state->owner_ipv4_mask)) {
                    ker::mod::dbg::log("[WKI] Skipping remote NIC %s from %s: subnet overlap", pending.nic_name.data(),
                                       pending.hostname.data());
                    wki_remote_net_detach(proxy_dev);
                    continue;
                }
            }

            ker::mod::dbg::log("[WKI] NET auto-attached: %s -> node=0x%04x", pending.nic_name.data(), pending.node_id);

            // In sparse mode we intentionally avoid auto-installing routes.
            // Remote NIC discovery happens before local DHCP/static configuration
            // is necessarily complete, so eagerly adding a same-subnet route can
            // steal local traffic (e.g. SSH/NTP) until the real NIC is configured.
            // ATTACH_ALL keeps the old behavior and auto-routes every remote NIC.
            if (g_wki.nic_policy == WkiNicPolicy::ATTACH_ALL && proxy_state != nullptr && proxy_state->owner_ipv4_addr != 0 &&
                proxy_state->owner_ipv4_mask != 0) {
                uint32_t const REMOTE_SUBNET = proxy_state->owner_ipv4_addr & proxy_state->owner_ipv4_mask;
                int const ROUTE_RET =
                    ker::net::route_add(REMOTE_SUBNET, proxy_state->owner_ipv4_mask, 0 /*gateway*/, 100 /*metric*/, proxy_dev);
                if (ROUTE_RET == 0) {
                    ker::mod::dbg::log("[WKI] Route installed: subnet=0x%08x mask=0x%08x via %s", REMOTE_SUBNET,
                                       proxy_state->owner_ipv4_mask, pending.nic_name.data());
                }
            }
        }

        if (!processed_any) {
            break;
        }
    }
}

}  // namespace ker::net::wki
