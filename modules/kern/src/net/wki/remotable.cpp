#include "remotable.hpp"

#include <algorithm>
#include <array>
#include <atomic>
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
#include <net/wki/channel.hpp>
#include <net/wki/dev_proxy.hpp>
#include <net/wki/dev_server.hpp>
#include <net/wki/peer.hpp>
#include <net/wki/remote_net.hpp>
#include <net/wki/remote_vfs.hpp>
#include <net/wki/timer_math.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/sys/mutex.hpp>
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

// Resource RX is first deferred into task context. Keep mount handshakes on
// that worker as a second stage because they wait for an attach ACK and may
// recursively drive WKI progress.
struct PendingVfsMount {
    uint16_t node_id;
    uint32_t resource_id;
    uint64_t resource_generation = 0;
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
    uint64_t resource_generation = 0;
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
constexpr uint64_t NET_AUTO_ATTACH_EPOCH_RETRY_US = 250000;

// Resource mutation is task-context only. Reliable RX first copies adverts and
// withdrawals into the fixed ingress ring below, so deque growth and devfs
// callbacks may safely sleep outside hard IRQ/NAPI context.
ker::mod::sys::Mutex s_remotable_lock;    // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
uint64_t g_next_resource_generation = 1;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

class PeerLifecycleLease {
   public:
    PeerLifecycleLease() = default;
    PeerLifecycleLease(const PeerLifecycleLease&) = delete;
    auto operator=(const PeerLifecycleLease&) -> PeerLifecycleLease& = delete;
    ~PeerLifecycleLease() { wki_peer_lifecycle_release(peer); }

    auto acquire(WkiPeer* candidate) -> bool {
        if (peer != nullptr || !wki_peer_lifecycle_acquire(candidate)) {
            return false;
        }
        peer = candidate;
        return true;
    }

   private:
    WkiPeer* peer = nullptr;
};

constexpr size_t REMOTABLE_RX_QUEUE_CAPACITY = WKI_CREDITS_CONTROL;
constexpr size_t REMOTABLE_RX_PAYLOAD_MAX = sizeof(ResourceAdvertNetPayload) + UINT8_MAX;
struct PendingResourceRx {
    MsgType type = MsgType::RESOURCE_ADVERT;
    WkiHeader hdr = {};
    WkiChannel* rx_channel = nullptr;
    uint32_t rx_channel_generation = 0;
    uint16_t payload_len = 0;
    std::array<uint8_t, REMOTABLE_RX_PAYLOAD_MAX> payload{};
};
std::array<PendingResourceRx, REMOTABLE_RX_QUEUE_CAPACITY> g_pending_resource_rx{};  // NOLINT
size_t g_pending_resource_rx_head = 0;                                               // NOLINT
size_t g_pending_resource_rx_tail = 0;                                               // NOLINT
size_t g_pending_resource_rx_count = 0;                                              // NOLINT
ker::mod::sys::Spinlock s_resource_rx_lock;                                          // NOLINT

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

// Unlocked helper - caller must hold s_remotable_lock. Only a negotiated
// owner incarnation can prove that an invalid BLOCK observation still names
// the same backing after a transient peer fence.
auto find_matching_block_tombstone_unlocked(uint16_t node_id, uint32_t resource_id, const ResourceIncarnationToken& owner_incarnation)
    -> DiscoveredResource* {
    if (!wki_resource_incarnation_valid(owner_incarnation)) {
        return nullptr;
    }
    for (auto& res : g_discovered) {
        if (!res.valid && res.node_id == node_id && res.resource_type == ResourceType::BLOCK && res.resource_id == resource_id &&
            wki_resource_incarnation_valid(res.owner_incarnation) &&
            wki_resource_incarnation_equal(res.owner_incarnation, owner_incarnation)) {
            return &res;
        }
    }
    return nullptr;
}

auto next_resource_generation_locked() -> uint64_t {
    uint64_t generation = g_next_resource_generation++;
    if (generation == 0) {
        generation = g_next_resource_generation++;
    }
    return generation;
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

        auto* nif = ker::net::netif_find_by_dev(dev);
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
        auto* nif = ker::net::netif_find_by_dev(dev);
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
    auto const* resource = find_resource_unlocked(pending.node_id, ResourceType::VFS, pending.resource_id);
    return resource != nullptr && resource->generation == pending.resource_generation;
}

auto pending_vfs_mount_matches_resource_locked(const PendingVfsMount& pending, const DiscoveredResource& res) -> bool {
    if (res.generation != pending.resource_generation) {
        return false;
    }
    const char* hostname = wki_peer_get_hostname(pending.node_id);
    if (hostname == nullptr || hostname[0] == '\0') {
        return false;
    }

    std::array<char, VFS_MOUNT_PATH_LEN> expected_mount_path{};
    build_vfs_mount_path(expected_mount_path.data(), expected_mount_path.size(), hostname, static_cast<const char*>(res.name));
    return std::strncmp(expected_mount_path.data(), pending.mount_path.data(), expected_mount_path.size()) == 0;
}

void sync_vfs_devfs_resource(uint16_t node_id, uint32_t resource_id, uint64_t retired_generation);

auto forget_stale_vfs_resource_after_not_found(const PendingVfsMount& pending) -> bool {
    bool removed = false;
    s_remotable_lock.lock();
    auto* resource = find_resource_unlocked(pending.node_id, ResourceType::VFS, pending.resource_id);
    if (resource != nullptr && pending_vfs_mount_matches_resource_locked(pending, *resource)) {
        std::erase_if(g_discovered, [&](const DiscoveredResource& res) {
            return res.node_id == pending.node_id && res.resource_type == ResourceType::VFS && res.resource_id == pending.resource_id &&
                   res.generation == pending.resource_generation;
        });
        std::erase_if(g_pending_vfs_mounts, [&](const PendingVfsMount& queued) {
            return queued.node_id == pending.node_id && queued.resource_id == pending.resource_id &&
                   queued.resource_generation == pending.resource_generation &&
                   std::strncmp(queued.mount_path.data(), pending.mount_path.data(), queued.mount_path.size()) == 0;
        });
        removed = true;
    }
    s_remotable_lock.unlock();

    if (removed) {
        sync_vfs_devfs_resource(pending.node_id, pending.resource_id, pending.resource_generation);
    }
    return removed;
}

void sync_vfs_devfs_resource(uint16_t node_id, uint32_t resource_id, uint64_t retired_generation) {
    PeerLifecycleLease lifecycle;
    WkiPeer* peer = wki_peer_find(node_id);
    if (peer != nullptr && !lifecycle.acquire(peer)) {
        return;
    }

    for (;;) {
        DiscoveredResource snapshot = {};
        uint64_t snapshot_generation = 0;
        s_remotable_lock.lock();
        auto const* current = find_resource_unlocked(node_id, ResourceType::VFS, resource_id);
        if (current != nullptr) {
            snapshot = *current;
            snapshot_generation = current->generation;
        }
        s_remotable_lock.unlock();

        if (snapshot_generation == 0) {
            ker::vfs::devfs::devfs_wki_remove_resource(node_id, static_cast<uint16_t>(ResourceType::VFS), resource_id, retired_generation);
        } else {
            ker::vfs::devfs::devfs_wki_add_resource(snapshot.node_id, static_cast<uint16_t>(snapshot.resource_type), snapshot.resource_id,
                                                    snapshot.generation, snapshot.flags, static_cast<const char*>(snapshot.name));
        }

        // An advert may commit between the snapshot and devfs upsert. Retry
        // until the identity view corresponds to the current live generation;
        // a later advert performs its own upsert after this check.
        if (wki_resource_generation_snapshot(node_id, ResourceType::VFS, resource_id) == snapshot_generation) {
            return;
        }
    }
}

auto capture_net_advert_state(ker::net::NetDevice* dev) -> NetAdvertState {
    NetAdvertState state = {};
    if (dev == nullptr) {
        return state;
    }

    auto* nif = ker::net::netif_find_by_dev(dev);
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

auto decode_resource_incarnation(uint16_t peer_node, ResourceType type, const uint8_t* payload, uint16_t payload_len,
                                 size_t base_and_name_size, ResourceIncarnationToken* token_out) -> bool {
    if (payload == nullptr || token_out == nullptr) {
        return false;
    }
    *token_out = {};

    bool const WITH_INCARNATION = wki_resource_incarnation_negotiated(peer_node, type);
    size_t const EXPECTED_SIZE = base_and_name_size + (WITH_INCARNATION ? sizeof(ResourceIncarnationToken) : 0);
    if (payload_len != EXPECTED_SIZE) {
        return false;
    }
    if (!WITH_INCARNATION) {
        return true;
    }

    std::memcpy(token_out, payload + base_and_name_size, sizeof(*token_out));
    WkiPeer const* peer = wki_peer_find(peer_node);
    return wki_resource_incarnation_valid(*token_out) && peer != nullptr && token_out->owner_boot_epoch == peer->remote_boot_epoch;
}

void queue_vfs_mount_locked(uint16_t node_id, uint32_t resource_id, uint64_t resource_generation, const char* mount_path,
                            bool force_remount) {
    if (mount_path == nullptr || mount_path[0] == '\0') {
        return;
    }

    for (auto& pending : g_pending_vfs_mounts) {
        if (pending.node_id != node_id) {
            continue;
        }

        bool const SAME_RESOURCE = pending.resource_id == resource_id && pending.resource_generation == resource_generation;
        bool const SAME_MOUNT_PATH = std::strncmp(pending.mount_path.data(), mount_path, pending.mount_path.size()) == 0;
        if (!SAME_RESOURCE && !SAME_MOUNT_PATH) {
            continue;
        }

        pending.resource_id = resource_id;
        pending.resource_generation = resource_generation;
        std::snprintf(pending.mount_path.data(), pending.mount_path.size(), "%s", mount_path);
        pending.force_remount = pending.force_remount || force_remount;
        pending.retry_count = 0;
        pending.next_attempt_us = 0;
        return;
    }

    PendingVfsMount pending = {};
    pending.node_id = node_id;
    pending.resource_id = resource_id;
    pending.resource_generation = resource_generation;
    pending.force_remount = force_remount;
    std::snprintf(pending.mount_path.data(), pending.mount_path.size(), "%s", mount_path);
    g_pending_vfs_mounts.push_back(pending);
}

void defer_vfs_mount_for_detach(PendingVfsMount& pending) {
    pending.next_attempt_us = wki_future_deadline_us(wki_now_us(), VFS_AUTO_MOUNT_RETRY_BASE_US);

    s_remotable_lock.lock();
    g_pending_vfs_mounts.push_back(pending);
    s_remotable_lock.unlock();
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

void queue_net_attach_locked(uint16_t node_id, uint32_t resource_id, uint64_t resource_generation, const char* nic_name,
                             const char* hostname, const char* remote_name, uint8_t retry_count = 0, uint64_t next_attempt_us = 0) {
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
        pending.resource_generation = resource_generation;
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
    pending.resource_generation = resource_generation;
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
    queue_net_attach_locked(pending.node_id, pending.resource_id, pending.resource_generation, pending.nic_name.data(),
                            pending.hostname.data(), pending.remote_name.data(), pending.retry_count, pending.next_attempt_us);
    s_remotable_lock.unlock();
    return true;
}

void defer_net_attach_for_local_ipv4(PendingNetAttach& pending) {
    pending.next_attempt_us = wki_future_deadline_us(wki_now_us(), NET_AUTO_ATTACH_LOCAL_IPV4_RETRY_US);

    s_remotable_lock.lock();
    queue_net_attach_locked(pending.node_id, pending.resource_id, pending.resource_generation, pending.nic_name.data(),
                            pending.hostname.data(), pending.remote_name.data(), pending.retry_count, pending.next_attempt_us);
    s_remotable_lock.unlock();
}

void defer_net_attach_for_epoch(PendingNetAttach& pending) {
    pending.next_attempt_us = wki_future_deadline_us(wki_now_us(), NET_AUTO_ATTACH_EPOCH_RETRY_US);

    s_remotable_lock.lock();
    queue_net_attach_locked(pending.node_id, pending.resource_id, pending.resource_generation, pending.nic_name.data(),
                            pending.hostname.data(), pending.remote_name.data(), pending.retry_count, pending.next_attempt_us);
    s_remotable_lock.unlock();
}

void defer_net_attach_for_detach(PendingNetAttach& pending) {
    pending.next_attempt_us = wki_future_deadline_us(wki_now_us(), NET_AUTO_ATTACH_RETRY_BASE_US);

    s_remotable_lock.lock();
    queue_net_attach_locked(pending.node_id, pending.resource_id, pending.resource_generation, pending.nic_name.data(),
                            pending.hostname.data(), pending.remote_name.data(), pending.retry_count, pending.next_attempt_us);
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

auto wki_resource_incarnation_negotiated(uint16_t peer_node, ResourceType type) -> bool {
    if (!wki_resource_type_uses_incarnation(type) || (g_wki.capabilities & WKI_CAP_RESOURCE_INCARNATION) == 0) {
        return false;
    }
    WkiPeer const* peer = wki_peer_find(peer_node);
    return peer != nullptr && (peer->capabilities & WKI_CAP_RESOURCE_INCARNATION) != 0;
}

auto wki_block_resource_incarnation_token(uint32_t resource_id) -> ResourceIncarnationToken {
    // Remotable block devices are registered during block-init phase 3 and
    // remain registered through WKI phase 5/runtime. Runtime-unregistered WKI
    // proxy bdevs never set .remotable. Thus the registry index identifies one
    // immutable backing for this boot; +1 reserves zero for legacy traffic.
    uint32_t const INCARNATION = resource_id == UINT32_MAX ? UINT32_MAX : resource_id + 1U;
    return {.owner_boot_epoch = g_wki.local_boot_epoch, .resource_incarnation = INCARNATION};
}

// -----------------------------------------------------------------------------
// Local resource advertisement - iterate block devices with remotable != nullptr
// -----------------------------------------------------------------------------

namespace {

auto send_resource_advert_to_peer(uint16_t peer_node, ker::dev::BlockDevice* bdev, uint32_t resource_id) -> int {
    // Build ResourceAdvertPayload + name
    uint8_t name_len = 0;
    while (name_len < DISCOVERED_RESOURCE_NAME_LEN - 1 && bdev->name.at(name_len) != '\0') {
        name_len++;
    }

    bool const WITH_INCARNATION = wki_resource_incarnation_negotiated(peer_node, ResourceType::BLOCK);
    auto total_len =
        static_cast<uint16_t>(sizeof(ResourceAdvertPayload) + name_len + (WITH_INCARNATION ? sizeof(ResourceIncarnationToken) : 0));
    std::array<uint8_t, sizeof(ResourceAdvertPayload) + 64 + sizeof(ResourceIncarnationToken)> buf{};

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
    if (WITH_INCARNATION) {
        ResourceIncarnationToken const TOKEN = wki_block_resource_incarnation_token(resource_id);
        memcpy(buf.data() + sizeof(ResourceAdvertPayload) + name_len, &TOKEN, sizeof(TOKEN));
    }

    return wki_send(peer_node, WKI_CHAN_CONTROL, MsgType::RESOURCE_ADVERT, buf.data(), total_len);
}

auto send_net_resource_advert_to_peer(uint16_t peer_node, ker::net::NetDevice* ndev, uint32_t resource_id) -> int {
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

    return wki_send(peer_node, WKI_CHAN_CONTROL, MsgType::RESOURCE_ADVERT, buf.data(), total_len);
}

enum class ResourceAdvertStage : uint8_t {
    WAIT_FOR_IDLE,
    VFS,
    BLOCK,
    NET,
    COMPLETE,
};

auto control_stream_is_idle(WkiPeer* peer) -> bool {
    WkiChannel* channel = wki_channel_lookup_in_peer(peer, peer->node_id, WKI_CHAN_CONTROL);
    if (channel == nullptr) {
        return true;
    }

    channel->lock.lock();
    bool const IDLE = channel->active && channel->peer_node_id == peer->node_id && channel->channel_id == WKI_CHAN_CONTROL &&
                      channel->retransmit_count == 0 && channel->tx_credits == WKI_CREDITS_CONTROL;
    channel->lock.unlock();
    return IDLE;
}

void request_resource_snapshot(WkiPeer* peer) {
    if (peer == nullptr || peer->node_id == WKI_NODE_INVALID) {
        return;
    }
    peer->resource_advert_request.fetch_add(1, std::memory_order_release);
    wki_deferred_work_notify();
}

}  // namespace

void wki_resource_advertise_all() {
    if (!g_remotable_initialized) {
        return;
    }

    for (size_t p = 0; p < WKI_MAX_PEERS; p++) {
        WkiPeer* peer = &g_wki.peers.at(p);
        if (peer->node_id == WKI_NODE_INVALID || peer->state != PeerState::CONNECTED) {
            continue;
        }
        request_resource_snapshot(peer);
    }
}

void wki_resource_advertise_to_peer(uint16_t peer_node) {
    if (!g_remotable_initialized || peer_node == WKI_NODE_INVALID) {
        return;
    }

    request_resource_snapshot(wki_peer_find(peer_node));
}

void wki_resource_process_pending_adverts() {
    bool refreshed_exports = false;

    for (auto& peer : g_wki.peers) {
        uint64_t const REQUEST = peer.resource_advert_request.load(std::memory_order_acquire);
        if (peer.node_id == WKI_NODE_INVALID || peer.state != PeerState::CONNECTED || REQUEST == 0 ||
            (peer.resource_advert_active_request == REQUEST &&
             peer.resource_advert_stage == static_cast<uint8_t>(ResourceAdvertStage::COMPLETE))) {
            continue;
        }

        PeerLifecycleLease lifecycle;
        if (!lifecycle.acquire(&peer) || peer.state != PeerState::CONNECTED ||
            peer.vfs_reset_rebind_pending.load(std::memory_order_acquire)) {
            continue;
        }

        if (peer.resource_advert_active_request != REQUEST) {
            peer.resource_advert_active_request = REQUEST;
            peer.resource_advert_stage = static_cast<uint8_t>(ResourceAdvertStage::WAIT_FOR_IDLE);
            peer.resource_advert_index = 0;
        }

        auto stage = static_cast<ResourceAdvertStage>(peer.resource_advert_stage);
        if (stage == ResourceAdvertStage::WAIT_FOR_IDLE) {
            if (!control_stream_is_idle(&peer)) {
                continue;
            }
            if (!refreshed_exports) {
                wki_remote_vfs_refresh_exports();
                refreshed_exports = true;
            }
            stage = ResourceAdvertStage::VFS;
            peer.resource_advert_stage = static_cast<uint8_t>(stage);
        }

        if (stage == ResourceAdvertStage::VFS) {
            if (!wki_remote_vfs_advertise_exports_to_peer(peer.node_id)) {
                continue;
            }
            stage = ResourceAdvertStage::BLOCK;
            peer.resource_advert_stage = static_cast<uint8_t>(stage);
            peer.resource_advert_index = 0;
        }

        if (stage == ResourceAdvertStage::BLOCK) {
            size_t const COUNT = ker::dev::block_device_count();
            while (peer.resource_advert_index < COUNT) {
                size_t const INDEX = peer.resource_advert_index;
                ker::dev::BlockDevice* bdev = ker::dev::block_device_at(INDEX);
                if (bdev != nullptr && bdev->remotable != nullptr && bdev->remotable->can_remote() &&
                    send_resource_advert_to_peer(peer.node_id, bdev, static_cast<uint32_t>(INDEX)) != WKI_OK) {
                    break;
                }
                peer.resource_advert_index++;
            }
            if (peer.resource_advert_index < COUNT) {
                continue;
            }
            stage = ResourceAdvertStage::NET;
            peer.resource_advert_stage = static_cast<uint8_t>(stage);
            peer.resource_advert_index = 0;
        }

        if (stage == ResourceAdvertStage::NET) {
            size_t const COUNT = ker::net::netdev_count();
            while (peer.resource_advert_index < COUNT) {
                size_t const INDEX = peer.resource_advert_index;
                ker::net::NetDevice* ndev = ker::net::netdev_at(INDEX);
                if (ndev != nullptr && ndev->remotable != nullptr && ndev->remotable->can_remote() &&
                    send_net_resource_advert_to_peer(peer.node_id, ndev, ndev->ifindex) != WKI_OK) {
                    break;
                }
                peer.resource_advert_index++;
            }
            if (peer.resource_advert_index < COUNT) {
                continue;
            }
            peer.resource_advert_stage = static_cast<uint8_t>(ResourceAdvertStage::COMPLETE);
        }
    }
}

void wki_remotable_notify_net_changed(ker::net::NetDevice* dev) {
    if (!g_remotable_initialized || dev == nullptr || dev->remotable == nullptr || !dev->remotable->can_remote()) {
        return;
    }

    for (size_t p = 0; p < WKI_MAX_PEERS; p++) {
        WkiPeer* peer = &g_wki.peers.at(p);
        if (peer->node_id == WKI_NODE_INVALID || peer->state != PeerState::CONNECTED) {
            continue;
        }
        request_resource_snapshot(peer);
    }
}

// -----------------------------------------------------------------------------
// Discovered resource table
// -----------------------------------------------------------------------------

auto wki_resource_generation_snapshot(uint16_t node_id, ResourceType type, uint32_t resource_id) -> uint64_t {
    s_remotable_lock.lock();
    auto const* resource = find_resource_unlocked(node_id, type, resource_id);
    uint64_t const GENERATION = resource != nullptr ? resource->generation : 0;
    s_remotable_lock.unlock();
    return GENERATION;
}

auto wki_resource_generation_is_live(uint16_t node_id, ResourceType type, uint32_t resource_id, uint64_t generation) -> bool {
    return generation != 0 && wki_resource_generation_snapshot(node_id, type, resource_id) == generation;
}

auto wki_resource_observation_snapshot(uint16_t node_id, ResourceType type, uint32_t resource_id, uint64_t generation,
                                       ResourceIncarnationToken* owner_incarnation_out) -> bool {
    if (generation == 0 || owner_incarnation_out == nullptr) {
        return false;
    }

    s_remotable_lock.lock();
    auto const* resource = find_resource_unlocked(node_id, type, resource_id);
    bool const LIVE = resource != nullptr && resource->generation == generation;
    if (LIVE) {
        *owner_incarnation_out = resource->owner_incarnation;
    }
    s_remotable_lock.unlock();
    return LIVE;
}

auto wki_resource_observation_is_live(uint16_t node_id, ResourceType type, uint32_t resource_id, uint64_t generation,
                                      const ResourceIncarnationToken& owner_incarnation) -> bool {
    ResourceIncarnationToken current = {};
    return wki_resource_observation_snapshot(node_id, type, resource_id, generation, &current) &&
           wki_resource_incarnation_equal(current, owner_incarnation);
}

void wki_resources_invalidate_for_peer(uint16_t node_id) {
    // Remove from devfs /dev/wki/ tree before erasing from discovered table
    ker::vfs::devfs::devfs_wki_remove_peer_resources(node_id);

    s_remotable_lock.lock();
    // Preserve invalid VFS rows as generation tombstones until the deferred
    // worker unmounts even already-deactivated proxies from this peer. BLOCK
    // rows also remain as dormant observations: a same-incarnation advert can
    // revive the exact generation needed by a fenced published proxy.
    bool vfs_withdraw_pending = false;
    for (auto& resource : g_discovered) {
        if (resource.valid && resource.node_id == node_id &&
            (resource.resource_type == ResourceType::VFS || resource.resource_type == ResourceType::BLOCK)) {
            resource.valid = false;
            vfs_withdraw_pending = vfs_withdraw_pending || resource.resource_type == ResourceType::VFS;
        }
    }
    std::erase_if(g_discovered, [node_id](const DiscoveredResource& res) {
        return res.node_id == node_id && res.resource_type != ResourceType::VFS && res.resource_type != ResourceType::BLOCK;
    });
    std::erase_if(g_pending_vfs_mounts, [node_id](const PendingVfsMount& pending) { return pending.node_id == node_id; });
    std::erase_if(g_pending_net_attaches, [node_id](const PendingNetAttach& pending) { return pending.node_id == node_id; });
    s_remotable_lock.unlock();

    if (vfs_withdraw_pending) {
        wki_timer_notify();
    }
}

void wki_resources_rebind_vfs_for_peer(uint16_t node_id) {
    const char* hostname = wki_peer_get_hostname(node_id);
    if (hostname == nullptr || hostname[0] == '\0') {
        return;
    }

    size_t queued = 0;
    s_remotable_lock.lock();
    size_t const RESOURCE_COUNT = g_discovered.size();
    for (size_t i = 0; i < RESOURCE_COUNT; ++i) {
        auto& resource = g_discovered.at(i);
        if (!resource.valid || resource.node_id != node_id || resource.resource_type != ResourceType::VFS) {
            continue;
        }

        DiscoveredResource superseded = resource;
        superseded.valid = false;
        resource.generation = next_resource_generation_locked();

        std::array<char, VFS_MOUNT_PATH_LEN> mount_path{};
        build_vfs_mount_path(mount_path.data(), mount_path.size(), hostname, static_cast<const char*>(resource.name));
        queue_vfs_mount_locked(node_id, resource.resource_id, resource.generation, mount_path.data(), true);

        // Push only after all accesses through resource for this iteration;
        // vector growth may invalidate the reference.
        g_discovered.push_back(superseded);
        queued++;
    }
    s_remotable_lock.unlock();

    if (queued != 0) {
        log::debug("Queued VFS epoch rebind: node=0x%04x resources=%llu", node_id, static_cast<unsigned long long>(queued));
        wki_timer_notify();
    }
}

void wki_resources_rebind_net_for_peer(uint16_t node_id) {
    if (g_wki.nic_policy == WkiNicPolicy::MANUAL) {
        return;
    }
    const char* hostname = wki_peer_get_hostname(node_id);
    if (hostname == nullptr || hostname[0] == '\0') {
        return;
    }

    size_t queued = 0;
    s_remotable_lock.lock();
    for (const auto& resource : g_discovered) {
        if (!resource.valid || resource.node_id != node_id || resource.resource_type != ResourceType::NET ||
            !net_resource_ready(resource)) {
            continue;
        }
        std::array<char, NET_AUTO_ATTACH_NAME_LEN> nic_name{};
        build_net_proxy_name(nic_name.data(), nic_name.size(), hostname, static_cast<const char*>(resource.name), resource.resource_id);
        queue_net_attach_locked(node_id, resource.resource_id, resource.generation, nic_name.data(), hostname,
                                static_cast<const char*>(resource.name));
        queued++;
    }
    s_remotable_lock.unlock();

    if (queued != 0) {
        log::debug("Queued NET epoch rebind: node=0x%04x resources=%llu", node_id, static_cast<unsigned long long>(queued));
        wki_timer_notify();
    }
}

void wki_resource_foreach(ResourceVisitor visitor, void* ctx) {
    if (visitor == nullptr) {
        return;
    }

    uint64_t cursor = 0;
    while (true) {
        DiscoveredResource snapshot = {};
        bool found = false;
        s_remotable_lock.lock();
        for (const auto& resource : g_discovered) {
            if (!resource.valid || resource.generation <= cursor || (found && resource.generation >= snapshot.generation)) {
                continue;
            }
            snapshot = resource;
            found = true;
        }
        s_remotable_lock.unlock();
        if (!found) {
            return;
        }
        visitor(snapshot, ctx);
        cursor = snapshot.generation;
    }
}

// -----------------------------------------------------------------------------
// RX handlers
// -----------------------------------------------------------------------------

namespace detail {

void handle_resource_advert(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (hdr == nullptr || payload_len < sizeof(ResourceAdvertPayload)) {
        return;
    }

    const auto* adv = reinterpret_cast<const ResourceAdvertPayload*>(payload);

    // Ignore adverts from ourselves and any payload whose claimed owner does
    // not match the reliable peer identity.
    if (adv->node_id == g_wki.my_node_id || adv->node_id != hdr->src_node) {
        return;
    }

    auto type = static_cast<ResourceType>(adv->resource_type);
    const ResourceAdvertNetPayload* net_adv = nullptr;
    size_t header_size = sizeof(ResourceAdvertPayload);
    if (type == ResourceType::NET) {
        if (payload_len < sizeof(ResourceAdvertNetPayload)) {
            return;
        }
        net_adv = reinterpret_cast<const ResourceAdvertNetPayload*>(payload);
        header_size = sizeof(ResourceAdvertNetPayload);
    }

    size_t const BASE_AND_NAME_SIZE = header_size + adv->name_len;
    ResourceIncarnationToken owner_incarnation = {};
    if (BASE_AND_NAME_SIZE > payload_len ||
        !decode_resource_incarnation(hdr->src_node, type, payload, payload_len, BASE_AND_NAME_SIZE, &owner_incarnation)) {
        return;
    }

    // Build the resource entry on-stack before locking
    DiscoveredResource res;
    res.node_id = adv->node_id;
    res.resource_type = type;
    res.resource_id = adv->resource_id;
    res.owner_incarnation = owner_incarnation;
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
    bool queue_net_after_retire = false;
    uint64_t retired_net_generation = 0;
    uint64_t devfs_generation = 0;
    s_remotable_lock.lock();

    // A fence removes the resource from the live discovery view but does not
    // prove that the owner-side BLOCK backing changed. Revive only a valid,
    // exact owner incarnation and keep its local generation stable so the
    // published fenced proxy can pass its observation check. Legacy ID-only
    // rows and changed incarnations deliberately receive a fresh generation.
    if (type == ResourceType::BLOCK) {
        if (auto* tombstone = find_matching_block_tombstone_unlocked(adv->node_id, adv->resource_id, owner_incarnation);
            tombstone != nullptr) {
            devfs_generation = tombstone->generation;
            tombstone->valid = true;
            tombstone->flags = res.flags;
            tombstone->owner_incarnation = res.owner_incarnation;
            std::memcpy(static_cast<void*>(tombstone->name), static_cast<const void*>(res.name), sizeof(tombstone->name));
            std::erase_if(g_discovered, [&](const DiscoveredResource& candidate) {
                return !candidate.valid && candidate.node_id == adv->node_id && candidate.resource_type == ResourceType::BLOCK &&
                       candidate.resource_id == adv->resource_id && candidate.generation != devfs_generation;
            });
            s_remotable_lock.unlock();

            ker::vfs::devfs::devfs_wki_add_resource(adv->node_id, adv->resource_type, adv->resource_id, devfs_generation, adv->flags,
                                                    static_cast<const char*>(res.name));
            if (wki_dev_proxy_reactivate_resource_observation(adv->node_id, adv->resource_id, devfs_generation, owner_incarnation)) {
                WkiPeer* const PEER = wki_peer_find(adv->node_id);
                if (PEER != nullptr && PEER->node_id == adv->node_id) {
                    PEER->block_resume_pending.store(true, std::memory_order_release);
                    wki_timer_notify();
                }
            }
            log::debug("Revived BLOCK resource observation: node=0x%04x res_id=%u generation=%lu", adv->node_id, adv->resource_id,
                       static_cast<unsigned long>(devfs_generation));
            return;
        }

        std::erase_if(g_discovered, [&](const DiscoveredResource& candidate) {
            return !candidate.valid && candidate.node_id == adv->node_id && candidate.resource_type == ResourceType::BLOCK &&
                   candidate.resource_id == adv->resource_id;
        });
    }

    // Check if we already have this resource (upsert)
    DiscoveredResource* existing = find_resource_unlocked(adv->node_id, type, adv->resource_id);
    if (existing != nullptr) {
        DiscoveredResource superseded_vfs = {};
        bool retire_superseded_vfs = false;
        bool const NET_READY_BEFORE = type == ResourceType::NET ? net_resource_ready(*existing) : false;
        bool const NAME_CHANGED =
            std::strncmp(static_cast<const char*>(existing->name), static_cast<const char*>(res.name), sizeof(existing->name)) != 0;
        bool const FLAGS_CHANGED = existing->flags != adv->flags;
        bool const INCARNATION_CHANGED = !wki_resource_incarnation_equal(existing->owner_incarnation, res.owner_incarnation);
        bool net_fields_changed = false;
        if (type == ResourceType::NET) {
            net_fields_changed = existing->net_ipv4_addr != res.net_ipv4_addr || existing->net_ipv4_mask != res.net_ipv4_mask ||
                                 existing->net_real_mac != res.net_real_mac || existing->net_link_state != res.net_link_state ||
                                 existing->net_mtu != res.net_mtu;
        }

        if (!NAME_CHANGED && !FLAGS_CHANGED && !INCARNATION_CHANGED && !net_fields_changed) {
            s_remotable_lock.unlock();
            return;
        }

        // A valid owner token names the same backing across metadata refreshes.
        // VFS name changes remap the exported path and NET retains its legacy
        // reconfiguration behavior; those cases still need a new local
        // generation even when their owner token is unchanged.
        bool const VFS_PATH_REMAPPED = type == ResourceType::VFS && NAME_CHANGED;
        bool const NET_RECONFIGURED = type == ResourceType::NET && (NAME_CHANGED || FLAGS_CHANGED || net_fields_changed);
        bool const REPLACEMENT_GENERATION = INCARNATION_CHANGED || VFS_PATH_REMAPPED || NET_RECONFIGURED;

        if (type == ResourceType::NET && REPLACEMENT_GENERATION) {
            retired_net_generation = existing->generation;
        }

        if (type == ResourceType::VFS && REPLACEMENT_GENERATION) {
            // Retain the exact old owner identity for generation-qualified
            // teardown before mutating the live observation in place.
            superseded_vfs = *existing;
            superseded_vfs.valid = false;
            retire_superseded_vfs = true;
        }

        // Refresh mutable fields so reconnect/re-advertisement can correct a
        // stale visible name without requiring a reboot.
        existing->flags = adv->flags;
        existing->owner_incarnation = res.owner_incarnation;
        memcpy(static_cast<void*>(existing->name), static_cast<const void*>(res.name), sizeof(existing->name));
        if (type == ResourceType::NET) {
            existing->net_ipv4_addr = res.net_ipv4_addr;
            existing->net_ipv4_mask = res.net_ipv4_mask;
            existing->net_real_mac = res.net_real_mac;
            existing->net_link_state = res.net_link_state;
            existing->net_mtu = res.net_mtu;
        }

        if (REPLACEMENT_GENERATION) {
            existing->generation = next_resource_generation_locked();
        }
        devfs_generation = existing->generation;

        if (type == ResourceType::VFS && REPLACEMENT_GENERATION) {
            const char* hostname = wki_peer_get_hostname(adv->node_id);
            if (hostname != nullptr && hostname[0] != '\0') {
                build_vfs_mount_path(desired_mount_path.data(), desired_mount_path.size(), hostname, static_cast<const char*>(res.name));
                queue_vfs_mount_locked(adv->node_id, adv->resource_id, existing->generation, desired_mount_path.data(), true);
                queue_vfs_mount = true;
            }
        } else if (type == ResourceType::NET && g_wki.nic_policy != WkiNicPolicy::MANUAL) {
            const char* hostname = wki_peer_get_hostname(adv->node_id);
            bool const NET_READY_AFTER = net_resource_ready(*existing);
            bool const SHOULD_ATTACH = NET_READY_AFTER && (NAME_CHANGED || net_fields_changed || !NET_READY_BEFORE) &&
                                       (REPLACEMENT_GENERATION || !wki_remote_net_has_proxy(adv->node_id, adv->resource_id));
            if (hostname != nullptr && hostname[0] != '\0' && SHOULD_ATTACH) {
                if (REPLACEMENT_GENERATION) {
                    queue_net_after_retire = true;
                } else {
                    std::array<char, NET_AUTO_ATTACH_NAME_LEN> nic_name{};
                    build_net_proxy_name(nic_name.data(), nic_name.size(), hostname, static_cast<const char*>(res.name), adv->resource_id);
                    queue_net_attach_locked(adv->node_id, adv->resource_id, existing->generation, nic_name.data(), hostname,
                                            static_cast<const char*>(res.name));
                }
            }
        }
        if (retire_superseded_vfs) {
            g_discovered.push_back(superseded_vfs);
        }

        s_remotable_lock.unlock();

        if (retired_net_generation != 0) {
            wki_remote_net_detach_resource_generation(adv->node_id, adv->resource_id, retired_net_generation);
            ker::vfs::devfs::devfs_wki_remove_resource(adv->node_id, adv->resource_type, adv->resource_id, retired_net_generation);
        }
        if (queue_net_after_retire) {
            const char* hostname = wki_peer_get_hostname(adv->node_id);
            if (hostname != nullptr && hostname[0] != '\0') {
                std::array<char, NET_AUTO_ATTACH_NAME_LEN> nic_name{};
                build_net_proxy_name(nic_name.data(), nic_name.size(), hostname, static_cast<const char*>(res.name), adv->resource_id);
                s_remotable_lock.lock();
                queue_net_attach_locked(adv->node_id, adv->resource_id, devfs_generation, nic_name.data(), hostname,
                                        static_cast<const char*>(res.name));
                s_remotable_lock.unlock();
            }
        }

        if (queue_vfs_mount && NAME_CHANGED) {
            ker::mod::dbg::log("[WKI] VFS resource renamed: node=0x%04x res_id=%u -> %s", adv->node_id, adv->resource_id,
                               desired_mount_path.data());
        }

        ker::vfs::devfs::devfs_wki_add_resource(adv->node_id, adv->resource_type, adv->resource_id, devfs_generation, adv->flags,
                                                static_cast<const char*>(res.name));
        return;
    }

    res.generation = next_resource_generation_locked();
    g_discovered.push_back(res);

    // V2: Queue deferred VFS auto-mount (cannot spin-wait inside NAPI context)
    if (type == ResourceType::VFS) {
        const char* hostname = wki_peer_get_hostname(adv->node_id);
        if (hostname != nullptr && hostname[0] != '\0') {
            build_vfs_mount_path(desired_mount_path.data(), desired_mount_path.size(), hostname, static_cast<const char*>(res.name));
            queue_vfs_mount_locked(adv->node_id, adv->resource_id, res.generation, desired_mount_path.data(), false);
        }
    }

    // V2: Queue deferred NET auto-attach based on NIC policy
    if (type == ResourceType::NET && g_wki.nic_policy != WkiNicPolicy::MANUAL && net_resource_ready(res)) {
        const char* hostname = wki_peer_get_hostname(adv->node_id);
        if (hostname != nullptr && hostname[0] != '\0') {
            std::array<char, NET_AUTO_ATTACH_NAME_LEN> nic_name{};
            build_net_proxy_name(nic_name.data(), nic_name.size(), hostname, static_cast<const char*>(res.name), adv->resource_id);
            queue_net_attach_locked(adv->node_id, adv->resource_id, res.generation, nic_name.data(), hostname,
                                    static_cast<const char*>(res.name));
        }
    }

    s_remotable_lock.unlock();

    // Update devfs /dev/wki/ tree (outside lock - not a WKI container)
    ker::vfs::devfs::devfs_wki_add_resource(adv->node_id, adv->resource_type, adv->resource_id, res.generation, adv->flags,
                                            static_cast<const char*>(res.name));

    ker::mod::dbg::log("[WKI] Discovered resource: node=0x%04x type=%u id=%u name=%s", adv->node_id, adv->resource_type, adv->resource_id,
                       static_cast<const char*>(res.name));
}

void handle_resource_withdraw(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len) {
    if (hdr == nullptr || payload_len < sizeof(ResourceAdvertPayload)) {
        return;
    }

    const auto* adv = reinterpret_cast<const ResourceAdvertPayload*>(payload);
    auto type = static_cast<ResourceType>(adv->resource_type);
    ResourceIncarnationToken owner_incarnation = {};
    if (adv->node_id != hdr->src_node || adv->name_len != 0 ||
        !decode_resource_incarnation(hdr->src_node, type, payload, payload_len, sizeof(ResourceAdvertPayload), &owner_incarnation)) {
        return;
    }

    // Publish a generation tombstone in the task-context resource RX worker.
    // The mount worker performs devfs synchronization and blocking VFS teardown
    // after this ordered reliable control event has committed.
    if (type == ResourceType::VFS) {
        bool queued = false;
        s_remotable_lock.lock();
        auto* resource = find_resource_unlocked(adv->node_id, type, adv->resource_id);
        if (resource != nullptr && wki_resource_incarnation_equal(resource->owner_incarnation, owner_incarnation)) {
            resource->valid = false;
            queued = true;
        }
        s_remotable_lock.unlock();

        if (queued) {
            wki_timer_notify();
        }
        if (queued) {
            ker::mod::dbg::log("[WKI] Resource withdrawal deferred: node=0x%04x type=%u id=%u", adv->node_id, adv->resource_type,
                               adv->resource_id);
        }
        return;
    }

    // Retire only the exact owner incarnation. A delayed withdrawal for an
    // older backing must not remove a replacement that reused the numeric ID.
    size_t dropped_pending_net_attaches = 0;
    uint64_t retired_generation = 0;
    s_remotable_lock.lock();
    if (auto const* resource = find_resource_unlocked(adv->node_id, type, adv->resource_id);
        resource != nullptr && wki_resource_incarnation_equal(resource->owner_incarnation, owner_incarnation)) {
        retired_generation = resource->generation;
    }
    std::erase_if(g_discovered, [&](const DiscoveredResource& res) {
        return retired_generation != 0 && res.node_id == adv->node_id && res.resource_type == type && res.resource_id == adv->resource_id &&
               res.generation == retired_generation;
    });
    if (retired_generation != 0 && type == ResourceType::NET) {
        size_t const PENDING_BEFORE = g_pending_net_attaches.size();
        std::erase_if(g_pending_net_attaches, [&](const PendingNetAttach& pending) {
            return pending.node_id == adv->node_id && pending.resource_id == adv->resource_id &&
                   pending.resource_generation == retired_generation;
        });
        dropped_pending_net_attaches = PENDING_BEFORE - g_pending_net_attaches.size();
    }
    s_remotable_lock.unlock();

    if (retired_generation == 0) {
        return;
    }

    if (type == ResourceType::NET) {
        wki_remote_net_detach_resource_generation(adv->node_id, adv->resource_id, retired_generation);
    }

    // Remove from devfs /dev/wki/ tree (external, not a WKI container).
    ker::vfs::devfs::devfs_wki_remove_resource(adv->node_id, adv->resource_type, adv->resource_id, retired_generation);

    if (dropped_pending_net_attaches != 0) {
        log::debug("Dropped %llu stale NET auto-attach(es): node=0x%04x res_id=%u",
                   static_cast<unsigned long long>(dropped_pending_net_attaches), adv->node_id, adv->resource_id);
    }

    ker::mod::dbg::log("[WKI] Resource withdrawn: node=0x%04x type=%u id=%u", adv->node_id, adv->resource_type, adv->resource_id);
}

}  // namespace detail

namespace {
auto classify_remotable_rx(MsgType type, uint16_t peer_node, const uint8_t* payload, uint16_t payload_len, uint16_t* copy_len)
    -> WkiRemotableRxAdmission {
    if (copy_len == nullptr) {
        return WkiRemotableRxAdmission::DISCARD;
    }
    *copy_len = 0;
    if ((type != MsgType::RESOURCE_ADVERT && type != MsgType::RESOURCE_WITHDRAW) || payload == nullptr ||
        payload_len < sizeof(ResourceAdvertPayload)) {
        return WkiRemotableRxAdmission::DISCARD;
    }

    ResourceAdvertPayload advert = {};
    std::memcpy(&advert, payload, sizeof(advert));
    auto const RESOURCE_TYPE = static_cast<ResourceType>(advert.resource_type);
    auto decode_for_admission = [&](ResourceType resource_type, size_t base_and_name_size,
                                    ResourceIncarnationToken* token_out) -> WkiRemotableRxAdmission {
        if (decode_resource_incarnation(peer_node, resource_type, payload, payload_len, base_and_name_size, token_out)) {
            return WkiRemotableRxAdmission::DEFERRED;
        }

        // A peer can become CONNECTED from HELLO_ACK just before a concurrent
        // HELLO publishes its boot epoch. Do not consume and ACK an otherwise
        // valid incarnation-bearing control in that narrow window: the sender
        // must retransmit it after the owner identity is known.
        bool const WITH_INCARNATION = wki_resource_incarnation_negotiated(peer_node, resource_type);
        size_t const EXPECTED_SIZE = base_and_name_size + (WITH_INCARNATION ? sizeof(ResourceIncarnationToken) : 0);
        if (WITH_INCARNATION && payload_len == EXPECTED_SIZE) {
            ResourceIncarnationToken token = {};
            std::memcpy(&token, payload + base_and_name_size, sizeof(token));
            WkiPeer const* peer = wki_peer_find(peer_node);
            if (wki_resource_incarnation_valid(token) && peer != nullptr && peer->remote_boot_epoch == 0) {
                return WkiRemotableRxAdmission::RETRY;
            }
        }
        return WkiRemotableRxAdmission::DISCARD;
    };

    if (type == MsgType::RESOURCE_WITHDRAW) {
        ResourceIncarnationToken token = {};
        if (advert.name_len != 0 || payload_len > REMOTABLE_RX_PAYLOAD_MAX) {
            return WkiRemotableRxAdmission::DISCARD;
        }
        WkiRemotableRxAdmission const ADMISSION = decode_for_admission(RESOURCE_TYPE, sizeof(ResourceAdvertPayload), &token);
        if (ADMISSION != WkiRemotableRxAdmission::DEFERRED) {
            return ADMISSION;
        }
        *copy_len = payload_len;
        return WkiRemotableRxAdmission::DEFERRED;
    }

    size_t const HEADER_SIZE = RESOURCE_TYPE == ResourceType::NET ? sizeof(ResourceAdvertNetPayload) : sizeof(ResourceAdvertPayload);
    size_t const BASE_AND_NAME_SIZE = HEADER_SIZE + advert.name_len;
    ResourceIncarnationToken token = {};
    if (payload_len < HEADER_SIZE || BASE_AND_NAME_SIZE > payload_len || payload_len > REMOTABLE_RX_PAYLOAD_MAX) {
        return WkiRemotableRxAdmission::DISCARD;
    }
    WkiRemotableRxAdmission const ADMISSION = decode_for_admission(RESOURCE_TYPE, BASE_AND_NAME_SIZE, &token);
    if (ADMISSION != WkiRemotableRxAdmission::DEFERRED) {
        return ADMISSION;
    }
    *copy_len = payload_len;
    return WkiRemotableRxAdmission::DEFERRED;
}

auto resource_rx_owner_epoch_matches(const PendingResourceRx& pending) -> bool {
    if (pending.payload_len < sizeof(ResourceAdvertPayload)) {
        return false;
    }

    ResourceAdvertPayload advert = {};
    std::memcpy(&advert, pending.payload.data(), sizeof(advert));
    auto const RESOURCE_TYPE = static_cast<ResourceType>(advert.resource_type);
    if (!wki_resource_type_uses_incarnation(RESOURCE_TYPE) || !wki_resource_incarnation_negotiated(pending.hdr.src_node, RESOURCE_TYPE)) {
        return false;
    }

    size_t base_and_name_size = sizeof(ResourceAdvertPayload);
    if (pending.type == MsgType::RESOURCE_ADVERT) {
        size_t const HEADER_SIZE = RESOURCE_TYPE == ResourceType::NET ? sizeof(ResourceAdvertNetPayload) : sizeof(ResourceAdvertPayload);
        base_and_name_size = HEADER_SIZE + advert.name_len;
    }

    ResourceIncarnationToken token = {};
    return decode_resource_incarnation(pending.hdr.src_node, RESOURCE_TYPE, pending.payload.data(), pending.payload_len, base_and_name_size,
                                       &token);
}

auto resource_rx_channel_token_matches(const PendingResourceRx& pending) -> bool {
    if (pending.rx_channel == nullptr || pending.rx_channel_generation == 0) {
        return false;
    }
    pending.rx_channel->lock.lock();
    bool const MATCHES = pending.rx_channel->active && pending.rx_channel->peer_node_id == pending.hdr.src_node &&
                         pending.rx_channel->channel_id == pending.hdr.channel_id &&
                         pending.rx_channel->generation == pending.rx_channel_generation;
    pending.rx_channel->lock.unlock();
    return MATCHES;
}

auto dequeue_resource_rx(PendingResourceRx* pending) -> bool {
    if (pending == nullptr) {
        return false;
    }
    uint64_t const FLAGS = s_resource_rx_lock.lock_irqsave();
    if (g_pending_resource_rx_count == 0) {
        s_resource_rx_lock.unlock_irqrestore(FLAGS);
        return false;
    }
    *pending = g_pending_resource_rx.at(g_pending_resource_rx_head);
    g_pending_resource_rx.at(g_pending_resource_rx_head) = {};
    g_pending_resource_rx_head = (g_pending_resource_rx_head + 1) % REMOTABLE_RX_QUEUE_CAPACITY;
    g_pending_resource_rx_count--;
    s_resource_rx_lock.unlock_irqrestore(FLAGS);
    return true;
}
}  // namespace

auto wki_remotable_admit_rx(MsgType type, const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                            uint32_t rx_channel_generation) -> WkiRemotableRxAdmission {
    if (hdr == nullptr || payload == nullptr || payload_len < sizeof(ResourceAdvertPayload) || rx_channel == nullptr ||
        rx_channel_generation == 0 || hdr->channel_id != WKI_CHAN_CONTROL || rx_channel->channel_id != WKI_CHAN_CONTROL ||
        rx_channel->peer_node_id != hdr->src_node) {
        return WkiRemotableRxAdmission::DISCARD;
    }

    ResourceAdvertPayload advert = {};
    std::memcpy(&advert, payload, sizeof(advert));
    if (advert.node_id != hdr->src_node) {
        return WkiRemotableRxAdmission::DISCARD;
    }

    uint16_t copy_len = 0;
    WkiRemotableRxAdmission const CLASSIFICATION = classify_remotable_rx(type, hdr->src_node, payload, payload_len, &copy_len);
    if (CLASSIFICATION != WkiRemotableRxAdmission::DEFERRED) {
        return CLASSIFICATION;
    }

    uint64_t const FLAGS = s_resource_rx_lock.lock_irqsave();
    if (g_pending_resource_rx_count >= REMOTABLE_RX_QUEUE_CAPACITY) {
        s_resource_rx_lock.unlock_irqrestore(FLAGS);
        return WkiRemotableRxAdmission::RETRY;
    }
    auto& pending = g_pending_resource_rx.at(g_pending_resource_rx_tail);
    pending.type = type;
    pending.hdr = *hdr;
    pending.rx_channel = rx_channel;
    pending.rx_channel_generation = rx_channel_generation;
    pending.payload_len = copy_len;
    std::memcpy(pending.payload.data(), payload, copy_len);
    g_pending_resource_rx_tail = (g_pending_resource_rx_tail + 1) % REMOTABLE_RX_QUEUE_CAPACITY;
    g_pending_resource_rx_count++;
    s_resource_rx_lock.unlock_irqrestore(FLAGS);
    return WkiRemotableRxAdmission::DEFERRED;
}

void wki_remotable_process_pending_rx() {
    PendingResourceRx pending = {};
    while (dequeue_resource_rx(&pending)) {
        WkiPeer* peer = wki_peer_find(pending.hdr.src_node);
        if (!wki_peer_lifecycle_acquire(peer)) {
            continue;
        }
        bool const CURRENT_SESSION = resource_rx_channel_token_matches(pending) || resource_rx_owner_epoch_matches(pending);
        bool const CURRENT = peer->node_id == pending.hdr.src_node && peer->state == PeerState::CONNECTED && CURRENT_SESSION;
        if (CURRENT) {
            if (pending.type == MsgType::RESOURCE_ADVERT) {
                detail::handle_resource_advert(&pending.hdr, pending.payload.data(), pending.payload_len);
            } else if (pending.type == MsgType::RESOURCE_WITHDRAW) {
                detail::handle_resource_withdraw(&pending.hdr, pending.payload.data(), pending.payload_len);
            }
        }
        wki_peer_lifecycle_release(peer);
    }
}

// -----------------------------------------------------------------------------
// Deferred VFS mount/unmount processing - called from the WKI deferred worker.
// -----------------------------------------------------------------------------

void wki_remotable_process_pending_mounts() {
    while (true) {
        DiscoveredResource withdrawn = {};
        bool found_withdrawn = false;
        bool replacement_live = false;
        size_t dropped_pending_mounts = 0;

        s_remotable_lock.lock();
        for (const auto& resource : g_discovered) {
            if (!resource.valid && resource.resource_type == ResourceType::VFS) {
                withdrawn = resource;
                found_withdrawn = true;
                break;
            }
        }
        if (found_withdrawn) {
            std::erase_if(g_discovered, [&](const DiscoveredResource& resource) {
                return !resource.valid && resource.node_id == withdrawn.node_id && resource.resource_type == ResourceType::VFS &&
                       resource.resource_id == withdrawn.resource_id && resource.generation == withdrawn.generation;
            });
            size_t const PENDING_BEFORE = g_pending_vfs_mounts.size();
            std::erase_if(g_pending_vfs_mounts, [&](const PendingVfsMount& pending) {
                return pending.node_id == withdrawn.node_id && pending.resource_id == withdrawn.resource_id &&
                       pending.resource_generation == withdrawn.generation;
            });
            dropped_pending_mounts = PENDING_BEFORE - g_pending_vfs_mounts.size();
            replacement_live = find_resource_unlocked(withdrawn.node_id, ResourceType::VFS, withdrawn.resource_id) != nullptr;
        }
        s_remotable_lock.unlock();

        if (!found_withdrawn) {
            break;
        }

        sync_vfs_devfs_resource(withdrawn.node_id, withdrawn.resource_id, withdrawn.generation);
        wki_remote_vfs_unmount_resource_generation(withdrawn.node_id, withdrawn.resource_id, withdrawn.generation);
        log::debug("Deferred VFS withdrawal: node=0x%04x res_id=%u generation=%lu dropped_mounts=%llu replacement=%u", withdrawn.node_id,
                   withdrawn.resource_id, static_cast<unsigned long>(withdrawn.generation),
                   static_cast<unsigned long long>(dropped_pending_mounts), replacement_live ? 1U : 0U);
    }

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

            if (wki_remote_vfs_detach_pending_for_resource(pending.node_id, pending.resource_id)) {
                defer_vfs_mount_for_detach(pending);
                log::debug("Deferring VFS auto-mount until prior detach is acknowledged: node=0x%04x res_id=%u path=%s", pending.node_id,
                           pending.resource_id, pending.mount_path.data());
                continue;
            }

            if (wki_remote_vfs_has_mount_for_resource_generation(pending.node_id, pending.resource_id, pending.resource_generation)) {
                continue;
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

            // Do not create nested export paths here. Once the host root is
            // mounted, vfs_mkdir() would route through that remote mount. A
            // stale root can then block this sole deferred worker while the
            // withdrawal needed to replace it is still queued behind us.
            // mount_filesystem() publishes nested mounts directly, and VFS
            // readdir synthesizes their mount-table children.

            int const RET =
                wki_remote_vfs_mount(pending.node_id, pending.resource_id, pending.mount_path.data(), pending.resource_generation);
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

            if (RET == -EAGAIN) {
                defer_vfs_mount_for_detach(pending);
                log::debug("Deferring VFS auto-mount after detach gate raced attach: node=0x%04x res_id=%u path=%s", pending.node_id,
                           pending.resource_id, pending.mount_path.data());
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
            WkiPeer* peer = wki_peer_find(pending.node_id);
            if (peer == nullptr || peer->state != PeerState::CONNECTED) {
                ker::mod::dbg::log("[WKI] NET auto-attach skipped: peer 0x%04x not connected", pending.node_id);
                continue;
            }
            if (peer->vfs_reset_rebind_pending.load(std::memory_order_acquire)) {
                defer_net_attach_for_epoch(pending);
                log::debug("Deferring NET auto-attach across peer epoch reset: node=0x%04x res_id=%u generation=%lu", pending.node_id,
                           pending.resource_id, static_cast<unsigned long>(pending.resource_generation));
                continue;
            }

            uint32_t remote_ipv4_addr = 0;
            uint32_t remote_ipv4_mask = 0;
            s_remotable_lock.lock();
            DiscoveredResource const* live_resource = find_resource_unlocked(pending.node_id, ResourceType::NET, pending.resource_id);
            bool const RESOURCE_LIVE = live_resource != nullptr && live_resource->generation == pending.resource_generation;
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

            if (wki_remote_net_detach_pending_for_resource(pending.node_id, pending.resource_id)) {
                defer_net_attach_for_detach(pending);
                log::debug("Deferring NET auto-attach until prior detach is acknowledged: node=0x%04x res_id=%u nic=%s", pending.node_id,
                           pending.resource_id, pending.nic_name.data());
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

            if (peer->vfs_reset_rebind_pending.load(std::memory_order_acquire)) {
                defer_net_attach_for_epoch(pending);
                continue;
            }
            if (!wki_resource_generation_is_live(pending.node_id, ResourceType::NET, pending.resource_id, pending.resource_generation)) {
                continue;
            }

            if (wki_remote_net_detach_pending_for_resource(pending.node_id, pending.resource_id)) {
                defer_net_attach_for_detach(pending);
                continue;
            }

            ker::net::NetDevice* proxy_dev =
                wki_remote_net_attach(pending.node_id, pending.resource_id, pending.nic_name.data(), pending.resource_generation);
            if (proxy_dev == nullptr) {
                if (wki_remote_net_detach_pending_for_resource(pending.node_id, pending.resource_id)) {
                    defer_net_attach_for_detach(pending);
                    continue;
                }
                bool const REQUEUED = requeue_net_attach(pending);
                ker::mod::dbg::log("[WKI] NET auto-attach failed: %s -> node=0x%04x%s", pending.nic_name.data(), pending.node_id,
                                   REQUEUED ? " (retry queued)" : "");
                continue;
            }

            // Keep post-attach validation and policy publication serialized
            // against peer epoch cleanup.  If cleanup won the lifecycle race,
            // the exact proxy will already be gone and this raw pointer is only
            // a permanently retained tombstone.
            PeerLifecycleLease post_attach_lifecycle;
            WkiPeer* post_attach_peer = wki_peer_find(pending.node_id);
            if (post_attach_peer == nullptr || !post_attach_lifecycle.acquire(post_attach_peer) ||
                post_attach_peer->state != PeerState::CONNECTED ||
                post_attach_peer->vfs_reset_rebind_pending.load(std::memory_order_acquire) ||
                !wki_resource_generation_is_live(pending.node_id, ResourceType::NET, pending.resource_id, pending.resource_generation) ||
                !wki_remote_net_has_proxy(pending.node_id, pending.resource_id)) {
                wki_remote_net_detach(proxy_dev);
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
