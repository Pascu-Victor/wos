#include "auth.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/wki/auth_crypto.hpp>
#include <net/wki/auth_policy.hpp>
#include <net/wki/auth_protocol.hpp>
#include <net/wki/wire.hpp>
#include <net/wki/wki.hpp>
#include <platform/fw/qemu_fw_cfg.hpp>
#include <utility>

namespace ker::net::wki {

namespace {

constexpr auto AUTH_FW_CFG_PATH = "opt/wos/wki-auth";
constexpr std::array<uint8_t, 8> AUTH_CONFIG_MAGIC = {'W', 'K', 'I', 'A', 'U', 'T', 'H', 0};
constexpr uint16_t AUTH_CONFIG_VERSION = 1;
constexpr std::array<uint8_t, 16> ZERO_SESSION_ID{};

struct AuthConfigHeader {
    std::array<uint8_t, 8> magic{};
    uint16_t version = 0;
    uint16_t local_node = 0;
    uint16_t peer_count = 0;
    uint16_t reserved = 0;
} __attribute__((packed));

struct AuthConfigEntry {
    uint16_t peer_node = 0;
    uint16_t key_id = 0;
    uint64_t policy = 0;
    std::array<uint8_t, WKI_AUTH_KEY_SIZE> key{};
} __attribute__((packed));

struct Credential {
    bool present = false;
    uint16_t peer_node = 0;
    uint16_t key_id = 0;
    uint64_t policy = 0;
    std::array<uint8_t, WKI_AUTH_KEY_SIZE> key{};
};

struct AuthConfig {
    bool ready = false;
    uint16_t local_node = WKI_NODE_INVALID;
    uint16_t credential_count = 0;
    std::array<Credential, WKI_AUTH_MAX_CREDENTIALS> credentials{};
};

AuthConfig g_auth;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto credential_for(uint16_t peer_node) -> const Credential* {
    for (size_t index = 0; index < g_auth.credential_count; ++index) {
        if (g_auth.credentials.at(index).present && g_auth.credentials.at(index).peer_node == peer_node) {
            return &g_auth.credentials.at(index);
        }
    }
    return nullptr;
}

auto is_handshake_message(const WkiHeader& header) -> bool {
    auto const MESSAGE = static_cast<MsgType>(header.msg_type);
    return MESSAGE == MsgType::HELLO || MESSAGE == MsgType::HELLO_ACK;
}

auto is_discovery_hint(const WkiHeader& header) -> bool {
    return header.dst_node == WKI_NODE_BROADCAST && static_cast<MsgType>(header.msg_type) == MsgType::HELLO;
}

auto uses_reliable_delivery(MsgType message) -> bool {
    switch (message) {
        case MsgType::HELLO:
        case MsgType::HELLO_ACK:
        case MsgType::HELLO_CONFIRM:
        case MsgType::HEARTBEAT:
        case MsgType::HEARTBEAT_ACK:
        case MsgType::PEER_GOODBYE:
            return false;
        default:
            return true;
    }
}

auto admit_replay_counter(WkiAuthSession& session, uint64_t counter) -> WkiAuthFrameResult {
    session.replay_lock.lock();
    auth_protocol::ReplayWindow window{.high = session.rx_counter_high, .bitmap = session.rx_counter_bitmap};
    auth_protocol::ReplayResult const REPLAY = auth_protocol::replay_admit(window, counter);
    session.rx_counter_high = window.high;
    session.rx_counter_bitmap = window.bitmap;
    session.replay_lock.unlock();
    if (REPLAY == auth_protocol::ReplayResult::ACCEPTED) {
        return WkiAuthFrameResult::ACCEPTED;
    }
    if (REPLAY == auth_protocol::ReplayResult::DUPLICATE) {
        return WkiAuthFrameResult::AUTHENTICATED_DUPLICATE;
    }
    return WkiAuthFrameResult::REJECTED;
}

auto derive_peer_session(const Credential& credential, const WkiPeer& peer, uint32_t remote_boot_epoch,
                         auth_protocol::DerivedSession& derived) -> bool {
    return auth_protocol::derive_session(credential.key, credential.key_id, g_auth.local_node, peer.node_id, g_wki.local_boot_epoch,
                                         remote_boot_epoch, peer.local_channel_epoch, peer.remote_channel_epoch, g_wki.capabilities,
                                         peer.capabilities, g_wki.local_auth_nonce, peer.remote_auth_nonce, derived);
}

void erase_pending_session(WkiAuthSession& session) {
    auth_crypto::secure_erase(session.pending_tx_key.data(), session.pending_tx_key.size());
    auth_crypto::secure_erase(session.pending_rx_key.data(), session.pending_rx_key.size());
    session.pending_session_id = {};
    session.pending = false;
}

}  // namespace

auto wki_auth_init() -> bool {
    wki_auth_shutdown();

    size_t file_size = 0;
    if (!platform::fw::fw_cfg_file_size(AUTH_FW_CFG_PATH, &file_size) || file_size < sizeof(AuthConfigHeader) ||
        file_size > sizeof(AuthConfigHeader) + (WKI_AUTH_MAX_CREDENTIALS * sizeof(AuthConfigEntry))) {
        return false;
    }

    std::array<uint8_t, sizeof(AuthConfigHeader) + (WKI_AUTH_MAX_CREDENTIALS * sizeof(AuthConfigEntry))> bytes{};
    int const READ = platform::fw::fw_cfg_read_file(AUTH_FW_CFG_PATH, bytes.data(), bytes.size());
    if (READ < 0 || std::cmp_not_equal(READ, file_size)) {
        auth_crypto::secure_erase(bytes.data(), bytes.size());
        return false;
    }

    AuthConfigHeader header{};
    std::memcpy(&header, bytes.data(), sizeof(header));
    size_t const EXPECTED_SIZE = sizeof(header) + (static_cast<size_t>(header.peer_count) * sizeof(AuthConfigEntry));
    if (header.magic != AUTH_CONFIG_MAGIC || header.version != AUTH_CONFIG_VERSION || header.reserved != 0 ||
        header.local_node == WKI_NODE_INVALID || header.local_node == WKI_NODE_BROADCAST || header.peer_count > WKI_AUTH_MAX_CREDENTIALS ||
        file_size != EXPECTED_SIZE) {
        auth_crypto::secure_erase(bytes.data(), bytes.size());
        return false;
    }

    g_auth.local_node = header.local_node;
    g_auth.credential_count = header.peer_count;
    for (size_t index = 0; index < header.peer_count; ++index) {
        AuthConfigEntry entry{};
        std::memcpy(&entry, bytes.data() + sizeof(header) + (index * sizeof(entry)), sizeof(entry));
        if (entry.peer_node == WKI_NODE_INVALID || entry.peer_node == WKI_NODE_BROADCAST || entry.peer_node == header.local_node ||
            entry.key_id == 0 || entry.policy == 0 || (entry.policy & ~wki_policy_bits(WkiPolicy::ALL)) != 0 ||
            credential_for(entry.peer_node) != nullptr) {
            auth_crypto::secure_erase(bytes.data(), bytes.size());
            wki_auth_shutdown();
            return false;
        }
        auto& credential = g_auth.credentials.at(index);
        credential.present = true;
        credential.peer_node = entry.peer_node;
        credential.key_id = entry.key_id;
        credential.policy = entry.policy;
        credential.key = entry.key;
    }
    auth_crypto::secure_erase(bytes.data(), bytes.size());
    g_auth.ready = true;
    return true;
}

void wki_auth_shutdown() {
    for (auto& credential : g_auth.credentials) {
        auth_crypto::secure_erase(credential.key.data(), credential.key.size());
        credential = {};
    }
    g_auth.ready = false;
    g_auth.local_node = WKI_NODE_INVALID;
    g_auth.credential_count = 0;
}

auto wki_auth_ready() -> bool { return g_auth.ready; }

auto wki_auth_local_node_id() -> uint16_t { return g_auth.ready ? g_auth.local_node : WKI_NODE_INVALID; }

auto wki_auth_peer_configured(uint16_t peer_node) -> bool { return g_auth.ready && credential_for(peer_node) != nullptr; }

auto wki_auth_peer_key_id(uint16_t peer_node) -> uint16_t {
    Credential const* credential = credential_for(peer_node);
    return g_auth.ready && credential != nullptr ? credential->key_id : 0;
}

auto wki_auth_append_frame(WkiHeader* header, uint8_t* frame, size_t capacity) -> size_t {
    if (!g_auth.ready || header == nullptr || frame == nullptr || header->src_node != g_auth.local_node) {
        return 0;
    }
    size_t const BASE_LENGTH = WKI_HEADER_SIZE + header->payload_len;
    if (is_discovery_hint(*header)) {
        return BASE_LENGTH <= capacity ? BASE_LENGTH : 0;
    }
    if (BASE_LENGTH + WKI_AUTH_TRAILER_SIZE > capacity) {
        return 0;
    }

    WkiAuthTrailer trailer{};
    std::array<uint8_t, WKI_AUTH_KEY_SIZE> const* key = nullptr;
    std::array<uint8_t, WKI_AUTH_KEY_SIZE> session_key{};
    if (is_handshake_message(*header)) {
        Credential const* credential = credential_for(header->dst_node);
        if (credential == nullptr) {
            return 0;
        }
        key = &credential->key;
        trailer.counter = g_wki.local_boot_epoch;
    } else {
        WkiPeer* peer = wki_peer_find(header->dst_node);
        if (peer == nullptr) {
            return 0;
        }
        peer->auth_session.replay_lock.lock();
        if (!peer->auth_session.active) {
            peer->auth_session.replay_lock.unlock();
            return 0;
        }
        uint64_t next_counter = peer->auth_session.tx_counter.load(std::memory_order_relaxed);
        trailer.counter = auth_protocol::reserve_tx_counter(next_counter);
        if (trailer.counter == 0) {
            peer->auth_session.replay_lock.unlock();
            return 0;
        }
        peer->auth_session.tx_counter.store(next_counter, std::memory_order_relaxed);
        session_key = peer->auth_session.tx_key;
        key = &session_key;
        trailer.session_id = peer->auth_session.session_id;
        peer->auth_session.replay_lock.unlock();
    }

    auth_crypto::Digest tag = auth_protocol::frame_tag(*key, *header, frame + WKI_HEADER_SIZE, trailer, is_handshake_message(*header));
    std::copy_n(tag.begin(), trailer.tag.size(), trailer.tag.begin());
    std::memcpy(frame + BASE_LENGTH, &trailer, sizeof(trailer));
    auth_crypto::secure_erase(tag.data(), tag.size());
    auth_crypto::secure_erase(session_key.data(), session_key.size());
    return BASE_LENGTH + sizeof(trailer);
}

auto wki_auth_verify_frame(const WkiHeader* header, const uint8_t* frame, size_t length) -> WkiAuthFrameResult {
    if (!g_auth.ready || header == nullptr || frame == nullptr ||
        (header->dst_node != g_auth.local_node && header->dst_node != WKI_NODE_BROADCAST)) {
        return WkiAuthFrameResult::REJECTED;
    }
    size_t const BASE_LENGTH = WKI_HEADER_SIZE + header->payload_len;
    if (is_discovery_hint(*header)) {
        if (length != BASE_LENGTH || header->src_node == WKI_NODE_INVALID || header->src_node == WKI_NODE_BROADCAST ||
            !wki_auth_peer_configured(header->src_node)) {
            return WkiAuthFrameResult::REJECTED;
        }
        return WkiAuthFrameResult::DISCOVERY_HINT;
    }
    if (length != BASE_LENGTH + WKI_AUTH_TRAILER_SIZE || header->src_node == WKI_NODE_INVALID || header->src_node == WKI_NODE_BROADCAST) {
        return WkiAuthFrameResult::REJECTED;
    }

    WkiAuthTrailer trailer{};
    std::memcpy(&trailer, frame + BASE_LENGTH, sizeof(trailer));
    std::array<uint8_t, WKI_AUTH_KEY_SIZE> const* key = nullptr;
    std::array<uint8_t, WKI_AUTH_KEY_SIZE> session_key{};
    WkiPeer* peer = nullptr;
    bool pending_confirmation = false;
    if (is_handshake_message(*header)) {
        Credential const* credential = credential_for(header->src_node);
        if (credential == nullptr || trailer.session_id != ZERO_SESSION_ID || trailer.counter == 0) {
            return WkiAuthFrameResult::REJECTED;
        }
        key = &credential->key;
    } else {
        peer = wki_peer_find(header->src_node);
        if (peer == nullptr) {
            return WkiAuthFrameResult::REJECTED;
        }
        peer->auth_session.replay_lock.lock();
        if (peer->auth_session.active && trailer.session_id == peer->auth_session.session_id) {
            session_key = peer->auth_session.rx_key;
        } else if (static_cast<MsgType>(header->msg_type) == MsgType::HELLO_CONFIRM && peer->auth_session.pending &&
                   trailer.session_id == peer->auth_session.pending_session_id && trailer.counter == 1) {
            session_key = peer->auth_session.pending_rx_key;
            pending_confirmation = true;
        } else {
            peer->auth_session.replay_lock.unlock();
            return WkiAuthFrameResult::REJECTED;
        }
        peer->auth_session.replay_lock.unlock();
        key = &session_key;
    }

    if (!auth_protocol::verify_frame_tag(*key, *header, frame + WKI_HEADER_SIZE, trailer, is_handshake_message(*header))) {
        auth_crypto::secure_erase(session_key.data(), session_key.size());
        return WkiAuthFrameResult::REJECTED;
    }
    if (is_handshake_message(*header)) {
        auto const* hello = reinterpret_cast<const HelloPayload*>(frame + WKI_HEADER_SIZE);
        if (header->payload_len != sizeof(HelloPayload) || hello->node_id != header->src_node || hello->protocol_version != WKI_VERSION ||
            hello->auth_suite != WKI_AUTH_SUITE_HMAC_SHA256_HKDF_SHA256 || hello->auth_key_id != credential_for(header->src_node)->key_id ||
            hello->auth_reserved != 0 || trailer.counter == 0) {
            return WkiAuthFrameResult::REJECTED;
        }
        auto const MESSAGE = static_cast<MsgType>(header->msg_type);
        if (MESSAGE == MsgType::HELLO) {
            bool const ECHO_NONCE_ZERO = std::ranges::all_of(hello->auth_echo_nonce, [](uint8_t byte) { return byte == 0; });
            if (!ECHO_NONCE_ZERO || hello->auth_echo_channel_epoch != 0) {
                return WkiAuthFrameResult::REJECTED;
            }
        } else {
            WkiPeer const* existing = wki_peer_find(header->src_node);
            uint32_t const EXPECTED_CHANNEL =
                existing != nullptr && existing->local_channel_epoch != 0 ? existing->local_channel_epoch : g_wki.local_boot_epoch;
            if (hello->auth_echo_nonce != g_wki.local_auth_nonce || hello->auth_echo_channel_epoch != EXPECTED_CHANNEL) {
                return WkiAuthFrameResult::REJECTED;
            }
        }
        return WkiAuthFrameResult::ACCEPTED;
    }
    auth_crypto::secure_erase(session_key.data(), session_key.size());
    if (pending_confirmation) {
        return WkiAuthFrameResult::PENDING_CONFIRMATION;
    }
    // Reliable channels already retain exact authenticated bytes and use their
    // session-bound sequence window. Applying the small unordered-datagram
    // bitmap here would incorrectly expire a stalled retransmission merely
    // because unrelated channels advanced the global frame counter.
    if (uses_reliable_delivery(static_cast<MsgType>(header->msg_type))) {
        return WkiAuthFrameResult::ACCEPTED;
    }
    return admit_replay_counter(peer->auth_session, trailer.counter);
}

auto wki_auth_install_peer_session(WkiPeer* peer, uint32_t remote_boot_epoch) -> bool {
    if (!g_auth.ready || peer == nullptr || remote_boot_epoch == 0) {
        return false;
    }
    Credential const* credential = credential_for(peer->node_id);
    if (credential == nullptr) {
        return false;
    }

    auth_protocol::DerivedSession derived{};
    if (!derive_peer_session(*credential, *peer, remote_boot_epoch, derived)) {
        return false;
    }

    peer->auth_session.replay_lock.lock();
    if (peer->auth_session.active && peer->auth_session.session_id == derived.session_id) {
        peer->auth_session.replay_lock.unlock();
        auth_crypto::secure_erase(&derived, sizeof(derived));
        return true;
    }
    if (peer->auth_session.retired_session_valid && peer->auth_session.retired_session_id == derived.session_id) {
        peer->auth_session.replay_lock.unlock();
        auth_crypto::secure_erase(&derived, sizeof(derived));
        return false;
    }

    if (peer->auth_session.active) {
        peer->auth_session.retired_session_id = peer->auth_session.session_id;
        peer->auth_session.retired_session_valid = true;
        auth_crypto::secure_erase(peer->auth_session.tx_key.data(), peer->auth_session.tx_key.size());
        auth_crypto::secure_erase(peer->auth_session.rx_key.data(), peer->auth_session.rx_key.size());
    }
    peer->auth_session.key_id = credential->key_id;
    peer->auth_session.policy = credential->policy;
    peer->auth_session.session_id = derived.session_id;
    peer->auth_session.tx_key = g_auth.local_node < peer->node_id ? derived.low_to_high : derived.high_to_low;
    peer->auth_session.rx_key = g_auth.local_node < peer->node_id ? derived.high_to_low : derived.low_to_high;
    peer->auth_session.tx_counter.store(1, std::memory_order_relaxed);
    peer->auth_session.rx_counter_high = 0;
    peer->auth_session.rx_counter_bitmap = 0;
    erase_pending_session(peer->auth_session);
    ++peer->auth_session.generation;
    if (peer->auth_session.generation == 0) {
        peer->auth_session.generation = 1;
    }
    peer->auth_session.active = true;
    for (auto& word : peer->auth_dynamic_channel_ids_used) {
        word.store(0, std::memory_order_relaxed);
    }
    peer->auth_session.replay_lock.unlock();
    auth_crypto::secure_erase(&derived, sizeof(derived));
    return true;
}

auto wki_auth_prepare_peer_session(WkiPeer* peer, uint32_t remote_boot_epoch) -> bool {
    if (!g_auth.ready || peer == nullptr || remote_boot_epoch == 0) {
        return false;
    }
    Credential const* credential = credential_for(peer->node_id);
    if (credential == nullptr) {
        return false;
    }

    auth_protocol::DerivedSession derived{};
    if (!derive_peer_session(*credential, *peer, remote_boot_epoch, derived)) {
        return false;
    }

    peer->auth_session.replay_lock.lock();
    if ((peer->auth_session.active && peer->auth_session.session_id == derived.session_id) ||
        (peer->auth_session.retired_session_valid && peer->auth_session.retired_session_id == derived.session_id)) {
        peer->auth_session.replay_lock.unlock();
        auth_crypto::secure_erase(&derived, sizeof(derived));
        return false;
    }
    erase_pending_session(peer->auth_session);
    peer->auth_session.pending_session_id = derived.session_id;
    peer->auth_session.pending_tx_key = g_auth.local_node < peer->node_id ? derived.low_to_high : derived.high_to_low;
    peer->auth_session.pending_rx_key = g_auth.local_node < peer->node_id ? derived.high_to_low : derived.low_to_high;
    peer->auth_session.pending = true;
    peer->auth_session.replay_lock.unlock();
    auth_crypto::secure_erase(&derived, sizeof(derived));
    return true;
}

auto wki_auth_confirm_peer_session(WkiPeer* peer) -> bool {
    if (peer == nullptr) {
        return false;
    }
    peer->auth_session.replay_lock.lock();
    if (!peer->auth_session.pending) {
        peer->auth_session.replay_lock.unlock();
        return false;
    }
    if (peer->auth_session.active) {
        peer->auth_session.retired_session_id = peer->auth_session.session_id;
        peer->auth_session.retired_session_valid = true;
        auth_crypto::secure_erase(peer->auth_session.tx_key.data(), peer->auth_session.tx_key.size());
        auth_crypto::secure_erase(peer->auth_session.rx_key.data(), peer->auth_session.rx_key.size());
    }
    peer->auth_session.session_id = peer->auth_session.pending_session_id;
    peer->auth_session.tx_key = peer->auth_session.pending_tx_key;
    peer->auth_session.rx_key = peer->auth_session.pending_rx_key;
    peer->auth_session.tx_counter.store(1, std::memory_order_relaxed);
    peer->auth_session.rx_counter_high = 1;
    peer->auth_session.rx_counter_bitmap = 1;
    peer->auth_session.key_id = wki_auth_peer_key_id(peer->node_id);
    Credential const* credential = credential_for(peer->node_id);
    peer->auth_session.policy = credential != nullptr ? credential->policy : 0;
    ++peer->auth_session.generation;
    if (peer->auth_session.generation == 0) {
        peer->auth_session.generation = 1;
    }
    peer->auth_session.active = true;
    erase_pending_session(peer->auth_session);
    for (auto& word : peer->auth_dynamic_channel_ids_used) {
        word.store(0, std::memory_order_relaxed);
    }
    peer->auth_session.replay_lock.unlock();
    return true;
}

void wki_auth_retire_peer_session(WkiPeer* peer) {
    if (peer == nullptr) {
        return;
    }
    peer->auth_session.replay_lock.lock();
    if (peer->auth_session.active) {
        peer->auth_session.retired_session_id = peer->auth_session.session_id;
        peer->auth_session.retired_session_valid = true;
    }
    peer->auth_session.active = false;
    auth_crypto::secure_erase(peer->auth_session.tx_key.data(), peer->auth_session.tx_key.size());
    auth_crypto::secure_erase(peer->auth_session.rx_key.data(), peer->auth_session.rx_key.size());
    peer->auth_session.session_id = {};
    peer->auth_session.key_id = 0;
    peer->auth_session.policy = 0;
    peer->auth_session.tx_counter.store(1, std::memory_order_relaxed);
    peer->auth_session.rx_counter_high = 0;
    peer->auth_session.rx_counter_bitmap = 0;
    erase_pending_session(peer->auth_session);
    peer->auth_session.replay_lock.unlock();
}

auto wki_auth_policy_allows(const WkiPeer* peer, MsgType message, const void* payload, uint16_t payload_length) -> bool {
    if (peer == nullptr || !peer->auth_session.active) {
        return false;
    }
    uint64_t const REQUIRED = auth_policy::required_permissions(message, payload, payload_length);
    return REQUIRED != auth_policy::INVALID && (peer->auth_session.policy & REQUIRED) == REQUIRED;
}

}  // namespace ker::net::wki
