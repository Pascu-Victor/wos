#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <net/wki/auth_crypto.hpp>
#include <net/wki/rdma_crypto.hpp>

namespace ker::net::wki::rdma_protocol {

// Independent of the ordinary WKI v3 packed header and HELLO capabilities.
constexpr uint32_t MAGIC = 0x44525357;  // little-endian "WSRD"
constexpr uint16_t VERSION = 1;
constexpr uint16_t SUITE_CHACHA20_POLY1305_HKDF_SHA256 = 1;
constexpr uint32_t FRAGMENT_BYTES = 8192;
constexpr uint32_t MAX_TRANSFER_BYTES = 4 * 1024 * 1024;
constexpr size_t MAX_FRAGMENTS = MAX_TRANSFER_BYTES / FRAGMENT_BYTES;
constexpr uint32_t MAX_OPERATIONS = 8;
constexpr uint32_t REPLAY_BITS = 8192;
constexpr uint64_t FEATURE_PRIVATE_BACKING = 1;

enum class Transport : uint8_t { NONE = 0, ROCE = 1, IVSHMEM = 2 };
enum class Service : uint8_t { NONE = 0, ZONE = 1, VFS = 2, IPC = 3, BLOCK = 4 };
enum class Opcode : uint8_t { READ = 1, WRITE = 2, READ_DATA = 3, WRITE_END = 4, READ_END = 5, NOTIFY = 6 };

constexpr uint8_t PERMIT_READ = 1;
constexpr uint8_t PERMIT_WRITE = 2;
constexpr uint8_t PERMIT_NOTIFY = 4;
constexpr uint8_t PERMISSIONS = PERMIT_READ | PERMIT_WRITE | PERMIT_NOTIFY;
constexpr uint16_t FLAG_RESPONSE = 1;

constexpr auto transport_bit(Transport kind) -> uint8_t {
    return kind == Transport::ROCE || kind == Transport::IVSHMEM ? static_cast<uint8_t>(1U << (static_cast<uint8_t>(kind) - 1U)) : 0;
}
constexpr auto service_bit(Service service) -> uint8_t {
    return service >= Service::ZONE && service <= Service::BLOCK ? static_cast<uint8_t>(1U << (static_cast<uint8_t>(service) - 1U)) : 0;
}
constexpr uint8_t TRANSPORTS = transport_bit(Transport::ROCE) | transport_bit(Transport::IVSHMEM);
constexpr uint8_t SERVICES =
    service_bit(Service::ZONE) | service_bit(Service::VFS) | service_bit(Service::IPC) | service_bit(Service::BLOCK);

using SessionId = std::array<uint8_t, 16>;
using Capability = std::array<uint8_t, 16>;

// Offers are ordinary authenticated WKI messages. An installed offer is
// immutable for that secure incarnation; changed offers require retirement.
struct Offer {
    uint16_t version = VERSION;
    uint16_t suite = SUITE_CHACHA20_POLY1305_HKDF_SHA256;
    uint8_t transports = 0;
    uint8_t services = 0;
    uint16_t reserved = 0;
    uint32_t record_bytes = 0;
    uint32_t transfer_bytes = MAX_TRANSFER_BYTES;
    uint32_t replay_bits = REPLAY_BITS;
    uint32_t operations = MAX_OPERATIONS;
    uint64_t features = FEATURE_PRIVATE_BACKING;
    std::array<uint8_t, 16> nonce{};
    SessionId session_id{};
} __attribute__((packed));
static_assert(sizeof(Offer) == 64);

struct RegionDescriptor {
    uint32_t rkey = 0;  // lookup hint only; never sufficient authority
    uint32_t size = 0;
    uint64_t generation = 0;
    uint64_t binding_generation = 0;
    Capability capability{};
    SessionId session_id{};
    uint16_t channel_id = 0;
    uint8_t transport = 0;
    uint8_t service = 0;
    uint8_t permissions = 0;
    uint8_t version = VERSION;
    uint16_t reserved = 0;
} __attribute__((packed));
static_assert(sizeof(RegionDescriptor) == 64);

struct Header {
    uint32_t magic = MAGIC;
    uint8_t version = VERSION;
    uint8_t transport = 0;
    uint8_t service = 0;
    uint8_t opcode = 0;
    uint16_t src_node = 0;
    uint16_t dst_node = 0;
    uint16_t channel_id = 0;
    uint16_t flags = 0;
    SessionId session_id{};
    uint64_t region_generation = 0;
    Capability capability{};
    uint64_t region_offset = 0;
    uint64_t operation_id = 0;
    uint64_t binding_generation = 0;
    // Full operation authority agreed through authenticated consumer control;
    // a low16 legacy RPC cookie alone is never a receive ticket.
    uint64_t receive_ticket = 0;
    uint64_t counter = 0;
    uint32_t fragment_offset = 0;
    uint32_t total_length = 0;
    uint32_t payload_length = 0;
    uint32_t rkey = 0;
    int32_t status = 0;
    uint32_t reserved = 0;
    uint64_t reserved2 = 0;
    rdma_crypto::Tag tag{};
} __attribute__((packed));
static_assert(sizeof(Header) == 144);
static_assert(offsetof(Header, tag) == 128);
constexpr size_t HEADER_BYTES = sizeof(Header);
constexpr size_t AAD_BYTES = offsetof(Header, tag);
constexpr uint32_t RECORD_BYTES = HEADER_BYTES + FRAGMENT_BYTES;
static_assert(RECORD_BYTES <= 8986);

enum class Drop : uint8_t {
    NONE,
    LENGTH,
    VERSION,
    FIELD,
    IDENTITY,
    SESSION,
    DOMAIN,
    AUTHENTICATION,
    COUNTER,
    DUPLICATE,
    TOO_OLD,
    GRANT,
    PERMISSION,
    RANGE,
    OPERATION,
    FRAGMENT,
    RETIRED,
    PRESSURE,
    COUNT,
};

struct KeyMaterial {
    rdma_crypto::Key key{};
    std::array<uint8_t, 4> nonce_prefix{};
};

struct KeyContext {
    SessionId session_id{};
    auth_crypto::Digest negotiation{};
    uint16_t src_node = 0;
    uint16_t dst_node = 0;
    Transport transport = Transport::NONE;
    Service service = Service::NONE;
};

struct ReplayWindow {
    uint64_t high = 0;
    std::array<uint64_t, REPLAY_BITS / 64> bits{};
};

struct Coverage {
    uint32_t length = 0;
    uint32_t received = 0;
    std::array<uint64_t, MAX_FRAGMENTS / 64> bits{};
};

// Pure helpers use only private, stable caller-owned buffers. Callers provide
// serialization and wipe KeyMaterial after use/retirement. No helper allocates,
// blocks, takes a lock, logs, or invokes a consumer callback.
[[nodiscard]] auto valid_offer(const Offer& offer) -> bool;
[[nodiscard]] auto negotiation_digest(uint16_t local_node, uint16_t peer_node, const Offer& local, const Offer& remote,
                                      auth_crypto::Digest& output) -> bool;
[[nodiscard]] auto derive_key(const rdma_crypto::Key& directional_control_key, const KeyContext& context, KeyMaterial& output) -> bool;
[[nodiscard]] auto make_nonce(const KeyMaterial& key, uint64_t counter, rdma_crypto::Nonce& output) -> bool;
// Capability root is local boot-private entropy; do not derive it from a
// pairwise/control/record key that the peer also knows.
[[nodiscard]] auto derive_capability(const rdma_crypto::Key& private_root, uint16_t owner, uint16_t peer, uint64_t transport_generation,
                                     const RegionDescriptor& descriptor, Capability& output) -> bool;
[[nodiscard]] auto validate_header(const Header& header, size_t record_size) -> Drop;
[[nodiscard]] auto validate_grant(const Header& header, const RegionDescriptor& descriptor) -> Drop;
// Encode/decode preserve output on rejection. Header and ciphertext are covered
// by AEAD; the same immutable snapshot must be used throughout decode.
[[nodiscard]] auto encode(const KeyMaterial& key, const Header& header, const void* plaintext, void* record, size_t capacity) -> bool;
[[nodiscard]] auto decode(const KeyMaterial& key, const KeyContext& expected, const void* record, size_t record_size, Header& header,
                          void* plaintext, size_t capacity) -> Drop;
// Reserve returns0 on terminal exhaustion and never resets/wraps/rolls back.
[[nodiscard]] auto reserve(uint64_t& next) -> uint64_t;
[[nodiscard]] auto replay_admit(ReplayWindow& window, uint64_t counter) -> Drop;
[[nodiscard]] auto coverage_begin(Coverage& coverage, uint32_t length) -> bool;
[[nodiscard]] auto coverage_admit(Coverage& coverage, uint32_t offset, uint32_t length) -> Drop;
[[nodiscard]] auto coverage_complete(const Coverage& coverage) -> bool;

}  // namespace ker::net::wki::rdma_protocol
