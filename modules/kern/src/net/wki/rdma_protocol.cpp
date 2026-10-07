#include "rdma_protocol.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace ker::net::wki::rdma_protocol {

namespace {

static_assert(std::endian::native == std::endian::little, "WKI packed wire fields use little endian");
constexpr char OFFER_DOMAIN[] = "WOS-WKI-SECURE-RDMA-OFFER-V1";
constexpr char KEY_DOMAIN[] = "WOS-WKI-SECURE-RDMA-KEY-V1";
constexpr char CAPABILITY_DOMAIN[] = "WOS-WKI-SECURE-RDMA-CAPABILITY-V1";

class EraseOnExit final {
   public:
    EraseOnExit(void* data, size_t size) : data_(data), size_(size) {}
    ~EraseOnExit() { auth_crypto::secure_erase(data_, size_); }
    EraseOnExit(const EraseOnExit&) = delete;
    auto operator=(const EraseOnExit&) -> EraseOnExit& = delete;

   private:
    void* data_;
    size_t size_;
};

template <size_t Size>
auto nonzero(const std::array<uint8_t, Size>& value) -> bool {
    uint8_t combined = 0;
    for (auto byte : value) combined |= byte;
    return combined != 0;
}

auto valid_nodes(uint16_t source, uint16_t destination) -> bool {
    return source != 0 && source != UINT16_MAX && destination != 0 && destination != UINT16_MAX && source != destination;
}

auto valid_domain(uint8_t transport, uint8_t service) -> bool {
    return transport_bit(static_cast<Transport>(transport)) != 0 && service_bit(static_cast<Service>(service)) != 0;
}

auto valid_context(const KeyContext& context) -> bool {
    return valid_nodes(context.src_node, context.dst_node) && nonzero(context.session_id) && nonzero(context.negotiation) &&
           valid_domain(static_cast<uint8_t>(context.transport), static_cast<uint8_t>(context.service));
}

void store_le16(uint8_t* output, uint16_t value) {
    output[0] = static_cast<uint8_t>(value);
    output[1] = static_cast<uint8_t>(value >> 8U);
}

void store_le64(uint8_t* output, uint64_t value) {
    for (size_t index = 0; index < 8; ++index) output[index] = static_cast<uint8_t>(value >> (index * 8U));
}

auto valid_descriptor(const RegionDescriptor& descriptor, bool require_capability) -> bool {
    return descriptor.version == VERSION && descriptor.reserved == 0 && descriptor.rkey != 0 && descriptor.size != 0 &&
           descriptor.generation != 0 && descriptor.binding_generation != 0 && nonzero(descriptor.session_id) &&
           valid_domain(descriptor.transport, descriptor.service) && descriptor.permissions != 0 &&
           (descriptor.permissions & ~PERMISSIONS) == 0 && (!require_capability || nonzero(descriptor.capability));
}

auto valid_fragment(uint32_t total, uint32_t offset, uint32_t length) -> bool {
    return total != 0 && total <= MAX_TRANSFER_BYTES && offset < total && (offset % FRAGMENT_BYTES) == 0 &&
           length == std::min(FRAGMENT_BYTES, total - offset);
}

struct Range {
    uintptr_t start = 0;
    uintptr_t end = 0;
};

auto checked_range(const void* data, size_t length, Range& result) -> bool {
    if (data == nullptr && length != 0) return false;
    uintptr_t const START = reinterpret_cast<uintptr_t>(data);
    if (length > UINTPTR_MAX - START) return false;
    result = {.start = START, .end = START + length};
    return true;
}

auto overlaps(const Range& lhs, const Range& rhs) -> bool {
    return lhs.start != lhs.end && rhs.start != rhs.end && lhs.start < rhs.end && rhs.start < lhs.end;
}

auto same_start(const Range& lhs, const Range& rhs) -> bool { return lhs.start == rhs.start; }

auto valid_encode_buffers(const KeyMaterial& key, const Header& header, const void* plaintext, void* record) -> bool {
    Range key_range{}, header_range{}, plaintext_range{}, record_range{};
    if (!checked_range(&key, sizeof(key), key_range) || !checked_range(&header, sizeof(header), header_range) ||
        !checked_range(plaintext, header.payload_length, plaintext_range) ||
        !checked_range(record, HEADER_BYTES + header.payload_length, record_range)) {
        return false;
    }
    // The exact record header and payload may be rewritten in place. Copy the
    // header first; the AEAD's AAD and tag must never alias its output payload.
    return !overlaps(record_range, key_range) && (!overlaps(record_range, header_range) || same_start(record_range, header_range)) &&
           (!overlaps(record_range, plaintext_range) || plaintext_range.start == record_range.start + HEADER_BYTES);
}

auto valid_decode_buffers(const KeyMaterial& key, const KeyContext& expected, const void* record, size_t record_size, Header& header,
                          void* plaintext, size_t payload_length) -> bool {
    Range key_range{}, context_range{}, record_range{}, header_range{}, plaintext_range{};
    if (!checked_range(&key, sizeof(key), key_range) || !checked_range(&expected, sizeof(expected), context_range) ||
        !checked_range(record, record_size, record_range) || !checked_range(&header, sizeof(header), header_range) ||
        !checked_range(plaintext, payload_length, plaintext_range)) {
        return false;
    }
    return !overlaps(header_range, key_range) && !overlaps(header_range, context_range) && !overlaps(plaintext_range, key_range) &&
           !overlaps(plaintext_range, context_range) && !overlaps(header_range, plaintext_range) &&
           (!overlaps(header_range, record_range) || same_start(header_range, record_range)) &&
           (!overlaps(plaintext_range, record_range) || plaintext_range.start == record_range.start + HEADER_BYTES);
}

}  // namespace

auto valid_offer(const Offer& offer) -> bool {
    return offer.version == VERSION && offer.suite == SUITE_CHACHA20_POLY1305_HKDF_SHA256 && (offer.transports & ~TRANSPORTS) == 0 &&
           (offer.services & ~SERVICES) == 0 && offer.reserved == 0 && offer.record_bytes == RECORD_BYTES &&
           offer.transfer_bytes == MAX_TRANSFER_BYTES && offer.replay_bits == REPLAY_BITS && offer.operations == MAX_OPERATIONS &&
           offer.features == FEATURE_PRIVATE_BACKING && nonzero(offer.nonce) && nonzero(offer.session_id);
}

auto negotiation_digest(uint16_t local_node, uint16_t peer_node, const Offer& local, const Offer& remote, auth_crypto::Digest& output)
    -> bool {
    if (!valid_nodes(local_node, peer_node) || !valid_offer(local) || !valid_offer(remote) || local.session_id != remote.session_id) {
        return false;
    }
    std::array<uint8_t, 4> nodes{};
    store_le16(nodes.data(), std::min(local_node, peer_node));
    store_le16(nodes.data() + 2, std::max(local_node, peer_node));
    auth_crypto::Sha256 digest;
    digest.update(OFFER_DOMAIN, sizeof(OFFER_DOMAIN) - 1);
    digest.update(nodes.data(), nodes.size());
    digest.update(local_node < peer_node ? &local : &remote, sizeof(Offer));
    digest.update(local_node < peer_node ? &remote : &local, sizeof(Offer));
    output = digest.finish();
    return true;
}

auto derive_key(const rdma_crypto::Key& directional_control_key, const KeyContext& context, KeyMaterial& output) -> bool {
    if (!valid_context(context)) return false;
    // Bind all context fields explicitly; unlike a packed compiler layout,
    // this serialization has no incidental padding in the HKDF transcript.
    std::array<uint8_t, sizeof(KEY_DOMAIN) - 1 + 16 + 32 + 4 + 2 + 4> info{};
    size_t position = 0;
    std::memcpy(info.data(), KEY_DOMAIN, sizeof(KEY_DOMAIN) - 1);
    position += sizeof(KEY_DOMAIN) - 1;
    std::memcpy(info.data() + position, context.session_id.data(), context.session_id.size());
    position += context.session_id.size();
    std::memcpy(info.data() + position, context.negotiation.data(), context.negotiation.size());
    position += context.negotiation.size();
    store_le16(info.data() + position, context.src_node);
    store_le16(info.data() + position + 2, context.dst_node);
    position += 4;
    info[position++] = static_cast<uint8_t>(context.transport);
    info[position++] = static_cast<uint8_t>(context.service);
    store_le16(info.data() + position, VERSION);
    store_le16(info.data() + position + 2, SUITE_CHACHA20_POLY1305_HKDF_SHA256);

    auth_crypto::Digest prk = auth_crypto::hkdf_extract(context.session_id.data(), context.session_id.size(),
                                                        directional_control_key.data(), directional_control_key.size());
    EraseOnExit const ERASE_PRK(prk.data(), prk.size());
    std::array<uint8_t, 36> derived{};
    EraseOnExit const ERASE_DERIVED(derived.data(), derived.size());
    if (!auth_crypto::hkdf_expand(prk, info.data(), info.size(), derived.data(), derived.size())) return false;
    KeyMaterial candidate{};
    EraseOnExit const ERASE_CANDIDATE(&candidate, sizeof(candidate));
    std::memcpy(candidate.key.data(), derived.data(), candidate.key.size());
    std::memcpy(candidate.nonce_prefix.data(), derived.data() + candidate.key.size(), candidate.nonce_prefix.size());
    output = candidate;
    return true;
}

auto make_nonce(const KeyMaterial& key, uint64_t counter, rdma_crypto::Nonce& output) -> bool {
    if (counter == 0) return false;
    rdma_crypto::Nonce candidate{};
    std::memcpy(candidate.data(), key.nonce_prefix.data(), key.nonce_prefix.size());
    store_le64(candidate.data() + key.nonce_prefix.size(), counter);
    output = candidate;
    return true;
}

auto derive_capability(const rdma_crypto::Key& private_root, uint16_t owner, uint16_t peer, uint64_t transport_generation,
                       const RegionDescriptor& descriptor, Capability& output) -> bool {
    if (!valid_nodes(owner, peer) || transport_generation == 0 || !valid_descriptor(descriptor, false)) return false;
    RegionDescriptor normalized = descriptor;
    normalized.capability = {};
    std::array<uint8_t, 12> identity{};
    store_le16(identity.data(), owner);
    store_le16(identity.data() + 2, peer);
    store_le64(identity.data() + 4, transport_generation);
    auth_crypto::HmacSha256 hmac(private_root.data(), private_root.size());
    hmac.update(CAPABILITY_DOMAIN, sizeof(CAPABILITY_DOMAIN) - 1);
    hmac.update(identity.data(), identity.size());
    hmac.update(&normalized, sizeof(normalized));
    auth_crypto::Digest digest = hmac.finish();
    EraseOnExit const ERASE_DIGEST(digest.data(), digest.size());
    Capability candidate{};
    EraseOnExit const ERASE_CANDIDATE(candidate.data(), candidate.size());
    std::memcpy(candidate.data(), digest.data(), candidate.size());
    if (!nonzero(candidate)) return false;
    output = candidate;
    return true;
}

auto validate_header(const Header& header, size_t record_size) -> Drop {
    if (record_size < HEADER_BYTES || header.payload_length > FRAGMENT_BYTES || record_size != HEADER_BYTES + header.payload_length) {
        return Drop::LENGTH;
    }
    if (header.magic != MAGIC || header.version != VERSION) return Drop::VERSION;
    if (!valid_domain(header.transport, header.service)) return Drop::DOMAIN;
    if (!valid_nodes(header.src_node, header.dst_node)) return Drop::IDENTITY;
    if (!nonzero(header.session_id)) return Drop::SESSION;
    if ((header.flags & ~FLAG_RESPONSE) != 0 || header.reserved != 0 || header.reserved2 != 0) return Drop::FIELD;
    if (header.counter == 0) return Drop::COUNTER;
    if (header.rkey == 0 || header.region_generation == 0 || header.binding_generation == 0 || !nonzero(header.capability)) {
        return Drop::GRANT;
    }
    if (header.operation_id == 0 || header.receive_ticket == 0) return Drop::OPERATION;
    if (header.total_length > MAX_TRANSFER_BYTES || header.region_offset > UINT64_MAX - header.total_length) return Drop::RANGE;
    auto const OP = static_cast<Opcode>(header.opcode);
    bool const TERMINAL = OP == Opcode::WRITE_END || OP == Opcode::READ_END;
    if (header.status > 0 || (!TERMINAL && header.status != 0)) return Drop::OPERATION;
    switch (OP) {
        case Opcode::READ:
            if (header.flags != 0 || header.payload_length != 0 || header.fragment_offset != 0 || header.total_length == 0) {
                return Drop::FRAGMENT;
            }
            break;
        case Opcode::WRITE:
            if (header.flags != 0 && header.service != static_cast<uint8_t>(Service::VFS)) return Drop::FIELD;
            if (!valid_fragment(header.total_length, header.fragment_offset, header.payload_length)) return Drop::FRAGMENT;
            break;
        case Opcode::READ_DATA:
            if (header.flags != FLAG_RESPONSE) return Drop::FIELD;
            if (!valid_fragment(header.total_length, header.fragment_offset, header.payload_length)) return Drop::FRAGMENT;
            break;
        case Opcode::WRITE_END:
        case Opcode::READ_END:
            if (header.flags != FLAG_RESPONSE) return Drop::FIELD;
            if (header.payload_length != 0 || header.fragment_offset != 0) return Drop::FRAGMENT;
            break;
        case Opcode::NOTIFY:
            if (header.flags != 0 || header.total_length != 0 || header.payload_length != 0 || header.fragment_offset != 0) {
                return Drop::FRAGMENT;
            }
            break;
        default:
            return Drop::FIELD;
    }
    return Drop::NONE;
}

auto validate_grant(const Header& header, const RegionDescriptor& descriptor) -> Drop {
    Drop const HEADER_RESULT = validate_header(header, HEADER_BYTES + header.payload_length);
    if (HEADER_RESULT != Drop::NONE) return HEADER_RESULT;
    // The caller must retain this descriptor from the exact active peer and
    // transport registration. The pure descriptor does not carry those IDs.
    if (!valid_descriptor(descriptor, true) || header.rkey != descriptor.rkey || header.region_generation != descriptor.generation ||
        header.binding_generation != descriptor.binding_generation || header.session_id != descriptor.session_id ||
        header.channel_id != descriptor.channel_id || header.transport != descriptor.transport || header.service != descriptor.service ||
        !auth_crypto::constant_time_equal(header.capability.data(), descriptor.capability.data(), descriptor.capability.size())) {
        return Drop::GRANT;
    }
    uint8_t permission = 0;
    switch (static_cast<Opcode>(header.opcode)) {
        case Opcode::READ:
        case Opcode::READ_DATA:
        case Opcode::READ_END:
            permission = PERMIT_READ;
            break;
        case Opcode::WRITE:
        case Opcode::WRITE_END:
            permission = PERMIT_WRITE;
            break;
        case Opcode::NOTIFY:
            permission = PERMIT_NOTIFY;
            break;
    }
    if ((descriptor.permissions & permission) != permission) return Drop::PERMISSION;
    if (header.region_offset > descriptor.size || header.total_length > descriptor.size - header.region_offset) return Drop::RANGE;
    return Drop::NONE;
}

auto encode(const KeyMaterial& key, const Header& header, const void* plaintext, void* record, size_t capacity) -> bool {
    size_t const SIZE = HEADER_BYTES + header.payload_length;
    if (validate_header(header, SIZE) != Drop::NONE || capacity < SIZE || !valid_encode_buffers(key, header, plaintext, record)) {
        return false;
    }
    Header encoded = header;
    encoded.tag = {};
    rdma_crypto::Nonce nonce{};
    if (!make_nonce(key, encoded.counter, nonce)) return false;
    auto* ciphertext = static_cast<uint8_t*>(record) + HEADER_BYTES;
    if (!rdma_crypto::seal(key.key, nonce, &encoded, AAD_BYTES, plaintext, encoded.payload_length, ciphertext, encoded.tag)) {
        return false;
    }
    std::memcpy(record, &encoded, sizeof(encoded));
    return true;
}

auto decode(const KeyMaterial& key, const KeyContext& expected, const void* record, size_t record_size, Header& header, void* plaintext,
            size_t capacity) -> Drop {
    Range record_range{};
    if (record_size < HEADER_BYTES || record_size > RECORD_BYTES || !checked_range(record, record_size, record_range)) {
        return Drop::LENGTH;
    }
    Header candidate{};
    std::memcpy(&candidate, record, sizeof(candidate));
    Drop const HEADER_RESULT = validate_header(candidate, record_size);
    if (HEADER_RESULT != Drop::NONE) return HEADER_RESULT;
    if (!valid_context(expected)) return Drop::DOMAIN;
    if (candidate.src_node != expected.src_node || candidate.dst_node != expected.dst_node) return Drop::IDENTITY;
    if (candidate.session_id != expected.session_id) return Drop::SESSION;
    if (candidate.transport != static_cast<uint8_t>(expected.transport) || candidate.service != static_cast<uint8_t>(expected.service)) {
        return Drop::DOMAIN;
    }
    if (capacity < candidate.payload_length ||
        !valid_decode_buffers(key, expected, record, record_size, header, plaintext, candidate.payload_length)) {
        return Drop::LENGTH;
    }
    // The caller supplies material derived from this exact expected context;
    // its negotiation digest is bound by HKDF, not transmitted in Header.
    rdma_crypto::Nonce nonce{};
    if (!make_nonce(key, candidate.counter, nonce)) return Drop::COUNTER;
    auto const* ciphertext = static_cast<const uint8_t*>(record) + HEADER_BYTES;
    if (!rdma_crypto::open(key.key, nonce, &candidate, AAD_BYTES, ciphertext, candidate.payload_length, plaintext, candidate.tag)) {
        return Drop::AUTHENTICATION;
    }
    header = candidate;
    return Drop::NONE;
}

auto reserve(uint64_t& next) -> uint64_t {
    if (next == 0 || next == UINT64_MAX) {
        next = 0;
        return 0;
    }
    return next++;
}

auto replay_admit(ReplayWindow& window, uint64_t counter) -> Drop {
    if (counter == 0) return Drop::COUNTER;
    if (counter > window.high) {
        uint64_t const SHIFT = counter - window.high;
        if (SHIFT >= REPLAY_BITS) {
            window.bits.fill(0);
        } else {
            size_t const WORD_SHIFT = static_cast<size_t>(SHIFT / 64);
            unsigned const BIT_SHIFT = static_cast<unsigned>(SHIFT % 64);
            // Descending writes preserve every old source word. A zero
            // remainder must not evaluate a shift by the word width.
            for (size_t destination = window.bits.size(); destination-- != 0;) {
                uint64_t shifted = 0;
                if (destination >= WORD_SHIFT) {
                    size_t const SOURCE = destination - WORD_SHIFT;
                    shifted = window.bits[SOURCE] << BIT_SHIFT;
                    if (BIT_SHIFT != 0 && SOURCE != 0) shifted |= window.bits[SOURCE - 1] >> (64U - BIT_SHIFT);
                }
                window.bits[destination] = shifted;
            }
        }
        window.high = counter;
        window.bits[0] |= 1;
        return Drop::NONE;
    }
    uint64_t const AGE = window.high - counter;
    if (AGE >= REPLAY_BITS) return Drop::TOO_OLD;
    size_t const WORD = static_cast<size_t>(AGE / 64);
    uint64_t const BIT = uint64_t{1} << (AGE % 64);
    if ((window.bits[WORD] & BIT) != 0) return Drop::DUPLICATE;
    window.bits[WORD] |= BIT;
    return Drop::NONE;
}

auto coverage_begin(Coverage& coverage, uint32_t length) -> bool {
    if (length > MAX_TRANSFER_BYTES) return false;
    coverage = {.length = length, .received = 0, .bits = {}};
    return true;
}

auto coverage_admit(Coverage& coverage, uint32_t offset, uint32_t length) -> Drop {
    if (coverage.length > MAX_TRANSFER_BYTES || coverage.received > coverage.length || !valid_fragment(coverage.length, offset, length)) {
        return Drop::FRAGMENT;
    }
    size_t const FRAGMENT = offset / FRAGMENT_BYTES;
    size_t const WORD = FRAGMENT / 64;
    uint64_t const BIT = uint64_t{1} << (FRAGMENT % 64);
    if ((coverage.bits[WORD] & BIT) != 0) return Drop::DUPLICATE;
    if (length > coverage.length - coverage.received) return Drop::FRAGMENT;
    coverage.bits[WORD] |= BIT;
    coverage.received += length;
    return Drop::NONE;
}

auto coverage_complete(const Coverage& coverage) -> bool {
    if (coverage.length > MAX_TRANSFER_BYTES || coverage.received != coverage.length) return false;
    size_t const FRAGMENTS = (coverage.length + FRAGMENT_BYTES - 1U) / FRAGMENT_BYTES;
    for (size_t word = 0; word < coverage.bits.size(); ++word) {
        size_t const START = word * 64;
        uint64_t const EXPECTED = START >= FRAGMENTS ? 0 : FRAGMENTS - START >= 64 ? UINT64_MAX : (uint64_t{1} << (FRAGMENTS - START)) - 1;
        if (coverage.bits[word] != EXPECTED) return false;
    }
    return true;
}

}  // namespace ker::net::wki::rdma_protocol
