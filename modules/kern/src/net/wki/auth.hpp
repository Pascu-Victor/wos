#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <platform/sys/spinlock.hpp>

namespace ker::net::wki {

struct WkiHeader;
struct WkiPeer;
enum class MsgType : uint8_t;

constexpr size_t WKI_AUTH_KEY_SIZE = 32;
constexpr size_t WKI_AUTH_MAX_CREDENTIALS = 256;
constexpr uint64_t WKI_AUTH_REPLAY_WINDOW = 64;

enum class WkiPolicy : uint64_t {
    NONE = 0,
    COMPUTE = 1ULL << 0U,
    VFS_EXPORT = 1ULL << 1U,
    VFS_READ = 1ULL << 2U,
    VFS_WRITE = 1ULL << 3U,
    IPC = 1ULL << 4U,
    REMOTE_NET = 1ULL << 5U,
    ZONE_RDMA = 1ULL << 6U,
    DEVICE_ATTACH = 1ULL << 7U,
    BLOCK_READ = 1ULL << 8U,
    BLOCK_WRITE = 1ULL << 9U,
    ALL = (1ULL << 10U) - 1U,
};

constexpr auto wki_policy_bits(WkiPolicy policy) -> uint64_t { return static_cast<uint64_t>(policy); }

struct WkiAuthSession {
    bool active = false;
    bool pending = false;
    uint16_t key_id = 0;
    uint32_t generation = 0;
    uint64_t policy = 0;
    std::array<uint8_t, 16> session_id{};
    std::array<uint8_t, 16> retired_session_id{};
    bool retired_session_valid = false;
    std::array<uint8_t, WKI_AUTH_KEY_SIZE> tx_key{};
    std::array<uint8_t, WKI_AUTH_KEY_SIZE> rx_key{};
    std::array<uint8_t, 16> pending_session_id{};
    std::array<uint8_t, WKI_AUTH_KEY_SIZE> pending_tx_key{};
    std::array<uint8_t, WKI_AUTH_KEY_SIZE> pending_rx_key{};
    std::atomic<uint64_t> tx_counter{1};
    uint64_t rx_counter_high = 0;
    uint64_t rx_counter_bitmap = 0;
    ker::mod::sys::Spinlock replay_lock;
};

enum class WkiAuthFrameResult : uint8_t {
    REJECTED,
    DISCOVERY_HINT,
    ACCEPTED,
    AUTHENTICATED_DUPLICATE,
    PENDING_CONFIRMATION,
};

// Load the fixed-size credential table from the private QEMU fw_cfg file.
// Missing or malformed provisioning fails closed.
auto wki_auth_init() -> bool;
void wki_auth_shutdown();
auto wki_auth_ready() -> bool;
auto wki_auth_local_node_id() -> uint16_t;
auto wki_auth_peer_configured(uint16_t peer_node) -> bool;
auto wki_auth_peer_key_id(uint16_t peer_node) -> uint16_t;

// Append/verify the v3 trailer. Broadcast HELLO is the only trailer-free frame
// and is returned solely as a discovery hint; callers must not mutate peer or
// service state from it.
auto wki_auth_append_frame(WkiHeader* header, uint8_t* frame, size_t capacity) -> size_t;
auto wki_auth_verify_frame(const WkiHeader* header, const uint8_t* frame, size_t length) -> WkiAuthFrameResult;

// Install a session only after an authenticated HELLO/HELLO_ACK transcript has
// supplied both boot epochs. Teardown erases the active keys exactly once.
auto wki_auth_install_peer_session(WkiPeer* peer, uint32_t remote_boot_epoch) -> bool;
auto wki_auth_prepare_peer_session(WkiPeer* peer, uint32_t remote_boot_epoch) -> bool;
auto wki_auth_confirm_peer_session(WkiPeer* peer) -> bool;
void wki_auth_retire_peer_session(WkiPeer* peer);

// Central policy classifier. Responses and lifecycle controls required to
// terminate an already-authorized operation remain admissible.
auto wki_auth_policy_allows(const WkiPeer* peer, MsgType message, const void* payload, uint16_t payload_length) -> bool;

}  // namespace ker::net::wki
