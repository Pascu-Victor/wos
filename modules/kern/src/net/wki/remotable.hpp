#pragma once

#include <cstdint>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>

namespace ker::net::wki {

// ─────────────────────────────────────────────────────────────────────────────
// Additional operation IDs for device remoting
// ─────────────────────────────────────────────────────────────────────────────

// Request: empty, Response: {block_size:u64, total_blocks:u64}
constexpr uint16_t OP_BLOCK_INFO = 0x0103;

// ─────────────────────────────────────────────────────────────────────────────
// Remotable Trait — attached to BlockDevice / NetDevice via .remotable field
// ─────────────────────────────────────────────────────────────────────────────

struct RemotableOps {
    bool (*can_remote)();
    bool (*can_share)();
    bool (*can_passthrough)();
    auto (*on_remote_attach)(uint16_t node_id) -> int;
    void (*on_remote_detach)(uint16_t node_id);
    void (*on_remote_fault)(uint16_t node_id);
};

// ─────────────────────────────────────────────────────────────────────────────
// Discovered Resource — advertised by remote peers
// ─────────────────────────────────────────────────────────────────────────────

constexpr size_t DISCOVERED_RESOURCE_NAME_LEN = 64;

struct DiscoveredResource {
    uint16_t node_id = WKI_NODE_INVALID;
    ResourceType resource_type = ResourceType::BLOCK;
    uint32_t resource_id = 0;
    uint8_t flags = 0;
    char name[DISCOVERED_RESOURCE_NAME_LEN] = {};  // NOLINT(modernize-avoid-c-arrays)
    bool valid = false;
};

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

// Initialize the remotable subsystem. Called from wki_init().
void wki_remotable_init();

// Advertise all local remotable block devices to all CONNECTED peers.
void wki_resource_advertise_all();

// Look up a discovered (remote) resource by node, type, and ID.
auto wki_resource_find(uint16_t node_id, ResourceType type, uint32_t resource_id) -> DiscoveredResource*;

// Look up a discovered (remote) resource by name.
auto wki_resource_find_by_name(const char* name) -> DiscoveredResource*;

// Remove all discovered resources for a fenced peer.
void wki_resources_invalidate_for_peer(uint16_t node_id);

// Iterate all valid discovered resources via callback.
using ResourceVisitor = void (*)(const DiscoveredResource& res, void* ctx);
void wki_resource_foreach(ResourceVisitor visitor, void* ctx);

// ─────────────────────────────────────────────────────────────────────────────
// Internal — RX message handlers (called from wki.cpp dispatch)
// ─────────────────────────────────────────────────────────────────────────────

namespace detail {

void handle_resource_advert(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);
void handle_resource_withdraw(const WkiHeader* hdr, const uint8_t* payload, uint16_t payload_len);

}  // namespace detail

}  // namespace ker::net::wki
