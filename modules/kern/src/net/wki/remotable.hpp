#pragma once

#include <cstdint>
#include <net/address.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>

namespace ker::net {
struct NetDevice;
}

namespace ker::net::wki {

// -----------------------------------------------------------------------------
// Additional operation IDs for device remoting
// -----------------------------------------------------------------------------

// Request: empty, Response: {block_size:u64, total_blocks:u64}
constexpr uint16_t OP_BLOCK_INFO = 0x0103;

// -----------------------------------------------------------------------------
// Remotable Trait - attached to BlockDevice / NetDevice via .remotable field
// -----------------------------------------------------------------------------

struct RemotableOps {
    bool (*can_remote)();
    bool (*can_share)();
    bool (*can_passthrough)();
    auto (*on_remote_attach)(uint16_t node_id) -> int;
    void (*on_remote_detach)(uint16_t node_id);
    void (*on_remote_fault)(uint16_t node_id);
};

// -----------------------------------------------------------------------------
// Discovered Resource - advertised by remote peers
// -----------------------------------------------------------------------------

constexpr size_t DISCOVERED_RESOURCE_NAME_LEN = 64;

struct DiscoveredResource {
    uint16_t node_id = WKI_NODE_INVALID;
    ResourceType resource_type = ResourceType::BLOCK;
    uint32_t resource_id = 0;
    // Monotonic local observation generation. Deferred mount teardown uses it
    // to distinguish a withdrawn resource from a later replacement that
    // reuses the same node/resource ID.
    uint64_t generation = 0;
    // Owner-provided identity. Zero denotes non-negotiated legacy ID-only
    // discovery; nonzero values are compared together with generation.
    ResourceIncarnationToken owner_incarnation = {};
    uint8_t flags = 0;
    uint32_t net_ipv4_addr = 0;
    uint32_t net_ipv4_mask = 0;
    proto::MacAddress net_real_mac;
    uint16_t net_link_state = 0;
    uint32_t net_mtu = 1500;
    char name[DISCOVERED_RESOURCE_NAME_LEN] = {};  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    bool valid = false;
};

enum class WkiRemotableRxAdmission : uint8_t {
    DEFERRED,
    DISCARD,
    RETRY,
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// Initialize the remotable subsystem. Called from wki_init().
void wki_remotable_init();

// Advertise all local remotable block devices to all CONNECTED peers.
void wki_resource_advertise_all();

// Advertise all local remotable resources to one connected peer.
void wki_resource_advertise_to_peer(uint16_t peer_node);

// Snapshot whether this exact locally observed resource generation is still
// advertised. Generation 0 is never live.
auto wki_resource_generation_snapshot(uint16_t node_id, ResourceType type, uint32_t resource_id) -> uint64_t;
auto wki_resource_generation_is_live(uint16_t node_id, ResourceType type, uint32_t resource_id, uint64_t generation) -> bool;
auto wki_resource_observation_snapshot(uint16_t node_id, ResourceType type, uint32_t resource_id, uint64_t generation,
                                       ResourceIncarnationToken* owner_incarnation_out) -> bool;
auto wki_resource_observation_is_live(uint16_t node_id, ResourceType type, uint32_t resource_id, uint64_t generation,
                                      const ResourceIncarnationToken& owner_incarnation) -> bool;

// Capability/type gate shared by discovery and attach paths.
auto wki_resource_incarnation_negotiated(uint16_t peer_node, ResourceType type) -> bool;

// BLOCK resource IDs are immutable block-registry indices for the lifetime of
// one WOS boot; this derives their owner token without a runtime registry lock.
auto wki_block_resource_incarnation_token(uint32_t resource_id) -> ResourceIncarnationToken;

// Remove all discovered resources for a fenced peer.
void wki_resources_invalidate_for_peer(uint16_t node_id);

// Retire live VFS observations after a connected peer changes channel epoch,
// then queue cached-identity replacement generations for task-context remount.
void wki_resources_rebind_vfs_for_peer(uint16_t node_id);

// Queue fresh NET proxy attachments from still-live observations after a
// channel-only epoch reset. Actual attaches run after the peer admission gate
// has reopened in the normal deferred NET worker.
void wki_resources_rebind_net_for_peer(uint16_t node_id);

// Iterate all valid discovered resources via callback.
using ResourceVisitor = void (*)(const DiscoveredResource& res, void* ctx);
void wki_resource_foreach(ResourceVisitor visitor, void* ctx);

// Admit reliable resource control traffic into bounded storage before ACK.
// The caller holds the channel lock; this path cannot allocate or block.
auto wki_remotable_admit_rx(MsgType type, const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len, WkiChannel* rx_channel,
                            uint32_t rx_channel_generation) -> WkiRemotableRxAdmission;
void wki_remotable_process_pending_rx();

// Process deferred VFS auto-mounts and withdrawn-resource unmounts from the WKI
// deferred worker. Mount waits cannot re-enter their NAPI poll handler, and
// unmount may yield while an in-flight remote-VFS completion retires.
void wki_remotable_process_pending_mounts();

// V2: Process deferred NET auto-attaches. Called from timer tick (outside NAPI context).
// Same NAPI re-entrance issue as VFS mounts.  [V2 A5]
void wki_remotable_process_pending_net_attaches();

// Push a changed local NET resource advert to all connected peers.
void wki_remotable_notify_net_changed(net::NetDevice* dev);

// -----------------------------------------------------------------------------
// Internal - RX message handlers (called from wki.cpp dispatch)
// -----------------------------------------------------------------------------

namespace detail {

void handle_resource_advert(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_resource_withdraw(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

}  // namespace detail

}  // namespace ker::net::wki
