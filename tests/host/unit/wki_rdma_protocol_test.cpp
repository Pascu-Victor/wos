#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <net/wki/rdma_protocol.hpp>
#include <numeric>
#include <random>
#include <set>
#include <string_view>
#include <vector>

namespace protocol = ker::net::wki::rdma_protocol;
namespace crypto = ker::net::wki::rdma_crypto;

namespace {

template <size_t Size>
constexpr auto hex(std::string_view value) -> std::array<uint8_t, Size> {
    auto nibble = [](char byte) -> uint8_t { return static_cast<uint8_t>(byte <= '9' ? byte - '0' : byte - 'a' + 10); };
    std::array<uint8_t, Size> output{};
    for (size_t index = 0; index < Size; ++index) output[index] = (nibble(value[index * 2]) << 4U) | nibble(value[index * 2 + 1]);
    return output;
}

constexpr auto SESSION = hex<16>("101112131415161718191a1b1c1d1e1f");
constexpr auto NEGOTIATION = hex<32>("893fe12d721fc9d022a975cf5d34a060f3884090d8349e325af1f46b01a72f33");
constexpr auto KEY = hex<32>("bf38e5cb7cc26b382fdb60424871867cc7daa876f430705493cbdf0d8c7319eb");
constexpr auto PREFIX = hex<4>("5bcefd06");
constexpr auto CAPABILITY = hex<16>("4eaeb2ef647dbee8acdbc6b37b9d147d");
constexpr auto RECORD = hex<163>(
    "575352440101020207000b002a000000101112131415161718191a1b1c1d1e1f"
    "4d000000000000004eaeb2ef647dbee8acdbc6b37b9d147d0d00000000000000"
    "630000000000000058000000000000006f000000000000007b00000000000000"
    "0000000013000000130000002500000000000000000000000000000000000000"
    "4dbc594d7a81946ff83fcb436a5ccd374e09469afa2fa5dffcbf146a9aa585ea08ca59");

auto offer(uint8_t nonce_start = 48) -> protocol::Offer {
    protocol::Offer result{};
    result.transports = protocol::TRANSPORTS;
    result.services = protocol::SERVICES;
    result.record_bytes = protocol::RECORD_BYTES;
    result.session_id = SESSION;
    for (size_t index = 0; index < result.nonce.size(); ++index) result.nonce[index] = static_cast<uint8_t>(nonce_start + index);
    return result;
}

auto context() -> protocol::KeyContext {
    return {.session_id = SESSION,
            .negotiation = NEGOTIATION,
            .src_node = 7,
            .dst_node = 11,
            .transport = protocol::Transport::ROCE,
            .service = protocol::Service::VFS};
}

auto material() -> protocol::KeyMaterial { return {.key = KEY, .nonce_prefix = PREFIX}; }

auto descriptor() -> protocol::RegionDescriptor {
    protocol::RegionDescriptor result{};
    result.rkey = 37;
    result.size = protocol::MAX_TRANSFER_BYTES + 256;
    result.generation = 77;
    result.binding_generation = 88;
    result.capability = CAPABILITY;
    result.session_id = SESSION;
    result.channel_id = 42;
    result.transport = static_cast<uint8_t>(protocol::Transport::ROCE);
    result.service = static_cast<uint8_t>(protocol::Service::VFS);
    result.permissions = protocol::PERMIT_READ | protocol::PERMIT_WRITE;
    return result;
}

auto header() -> protocol::Header {
    protocol::Header result{};
    result.transport = static_cast<uint8_t>(protocol::Transport::ROCE);
    result.service = static_cast<uint8_t>(protocol::Service::VFS);
    result.opcode = static_cast<uint8_t>(protocol::Opcode::WRITE);
    result.src_node = 7;
    result.dst_node = 11;
    result.channel_id = 42;
    result.session_id = SESSION;
    result.region_generation = 77;
    result.capability = CAPABILITY;
    result.region_offset = 13;
    result.operation_id = 99;
    result.binding_generation = 88;
    result.receive_ticket = 111;
    result.counter = 123;
    result.total_length = 19;
    result.payload_length = 19;
    result.rkey = 37;
    return result;
}

auto plaintext() -> std::array<uint8_t, 19> {
    std::array<uint8_t, 19> result{};
    std::iota(result.begin(), result.end(), uint8_t{1});
    return result;
}

template <typename T>
auto bytes_equal(const T& lhs, const T& rhs) -> bool {
    return std::memcmp(&lhs, &rhs, sizeof(T)) == 0;
}

void expect_rejected_record(const void* record, size_t size, const protocol::KeyContext& expected = context(),
                            const protocol::KeyMaterial& key = material()) {
    auto output_header = header();
    output_header.magic = 0xa5a5a5a5;
    auto const BEFORE_HEADER = output_header;
    std::array<uint8_t, protocol::FRAGMENT_BYTES> output{};
    output.fill(0xa5);
    auto const BEFORE = output;
    EXPECT_NE(protocol::decode(key, expected, record, size, output_header, output.data(), output.size()), protocol::Drop::NONE);
    EXPECT_TRUE(bytes_equal(output_header, BEFORE_HEADER));
    EXPECT_EQ(output, BEFORE);
}

class ReplayReference {
   public:
    uint64_t high = 0;
    std::set<uint64_t> accepted;

    auto admit(uint64_t counter) -> protocol::Drop {
        if (counter == 0) return protocol::Drop::COUNTER;
        if (counter <= high && high - counter >= protocol::REPLAY_BITS) return protocol::Drop::TOO_OLD;
        if (accepted.contains(counter)) return protocol::Drop::DUPLICATE;
        high = std::max(high, counter);
        accepted.insert(counter);
        while (!accepted.empty() && high - *accepted.begin() >= protocol::REPLAY_BITS) accepted.erase(accepted.begin());
        return protocol::Drop::NONE;
    }

    void compare(const protocol::ReplayWindow& actual) const {
        EXPECT_EQ(actual.high, high);
        std::array<uint64_t, protocol::REPLAY_BITS / 64> expected{};
        for (auto counter : accepted) {
            auto const AGE = high - counter;
            expected[AGE / 64] |= uint64_t{1} << (AGE % 64);
        }
        EXPECT_EQ(actual.bits, expected);
    }
};

}  // namespace

TEST(WkiRdmaProtocol, OfferRequiresExactLimitsAndSupportsExplicitDisable) {
    auto good = offer();
    EXPECT_TRUE(protocol::valid_offer(good));
    good.transports = 0;
    good.services = 0;
    EXPECT_TRUE(protocol::valid_offer(good));
    auto reject = [](auto change) {
        auto bad = offer();
        change(bad);
        EXPECT_FALSE(protocol::valid_offer(bad));
    };
    reject([](auto& o) { o.version++; });
    reject([](auto& o) { o.suite++; });
    reject([](auto& o) { o.transports |= 0x80; });
    reject([](auto& o) { o.services |= 0x80; });
    reject([](auto& o) { o.reserved = 1; });
    reject([](auto& o) { o.record_bytes--; });
    reject([](auto& o) { o.transfer_bytes--; });
    reject([](auto& o) { o.replay_bits--; });
    reject([](auto& o) { o.operations--; });
    reject([](auto& o) { o.features |= 2; });
    reject([](auto& o) { o.features = 0; });
    reject([](auto& o) { o.nonce = {}; });
    reject([](auto& o) { o.session_id = {}; });
}

TEST(WkiRdmaProtocol, IndependentCanonicalNegotiationVectorAndRoleSymmetry) {
    auto low = offer(48), high = offer(112);
    ker::net::wki::auth_crypto::Digest output{}, reversed{};
    ASSERT_TRUE(protocol::negotiation_digest(7, 11, low, high, output));
    EXPECT_EQ(output, NEGOTIATION);
    ASSERT_TRUE(protocol::negotiation_digest(11, 7, high, low, reversed));
    EXPECT_EQ(output, reversed);
    for (size_t index = 0; index < high.nonce.size(); ++index) {
        auto changed = high;
        changed.nonce[index] ^= 1;
        ASSERT_TRUE(protocol::negotiation_digest(7, 11, low, changed, reversed));
        EXPECT_NE(reversed, output);
    }
    for (auto change : {uint8_t{0}, uint8_t{1}}) {
        auto changed = high;
        changed.services = change;
        ASSERT_TRUE(protocol::negotiation_digest(7, 11, low, changed, reversed));
        EXPECT_NE(reversed, output);
    }
    auto before = output;
    high.session_id[0] ^= 1;
    EXPECT_FALSE(protocol::negotiation_digest(7, 11, low, high, output));
    EXPECT_EQ(output, before);
    for (auto invalid : {uint16_t{0}, uint16_t{7}, uint16_t{UINT16_MAX}}) {
        EXPECT_FALSE(protocol::negotiation_digest(7, invalid, low, offer(112), output));
        EXPECT_EQ(output, before);
    }
}

TEST(WkiRdmaProtocol, IndependentHkdfKeyAndNonceVector) {
    crypto::Key control{};
    std::iota(control.begin(), control.end(), uint8_t{0});
    protocol::KeyMaterial output{};
    ASSERT_TRUE(protocol::derive_key(control, context(), output));
    EXPECT_EQ(output.key, KEY);
    EXPECT_EQ(output.nonce_prefix, PREFIX);
    crypto::Nonce nonce{};
    ASSERT_TRUE(protocol::make_nonce(output, 123, nonce));
    EXPECT_EQ(nonce, hex<12>("5bcefd067b00000000000000"));
    auto const BEFORE = nonce;
    EXPECT_FALSE(protocol::make_nonce(output, 0, nonce));
    EXPECT_EQ(nonce, BEFORE);
    ASSERT_TRUE(protocol::make_nonce(output, UINT64_MAX, nonce));
    for (size_t index = 4; index < nonce.size(); ++index) EXPECT_EQ(nonce[index], 0xff);
}

TEST(WkiRdmaProtocol, KeySeparationAndInvalidContextPreserveOutput) {
    crypto::Key control{};
    std::iota(control.begin(), control.end(), uint8_t{0});
    auto expected = material();
    auto different = [&](auto change) {
        auto ctx = context();
        change(ctx);
        protocol::KeyMaterial output{};
        ASSERT_TRUE(protocol::derive_key(control, ctx, output));
        EXPECT_NE(output.key, expected.key);
        EXPECT_NE(output.nonce_prefix, expected.nonce_prefix);
    };
    different([](auto& c) { std::swap(c.src_node, c.dst_node); });
    different([](auto& c) { c.transport = protocol::Transport::IVSHMEM; });
    different([](auto& c) { c.service = protocol::Service::BLOCK; });
    for (size_t index = 0; index < protocol::SessionId{}.size(); ++index) different([index](auto& c) { c.session_id[index] ^= 1; });
    for (size_t index = 0; index < NEGOTIATION.size(); ++index) different([index](auto& c) { c.negotiation[index] ^= 1; });
    auto invalid = [&](auto change) {
        auto ctx = context();
        change(ctx);
        auto output = expected;
        EXPECT_FALSE(protocol::derive_key(control, ctx, output));
        EXPECT_TRUE(bytes_equal(output, expected));
    };
    invalid([](auto& c) { c.session_id = {}; });
    invalid([](auto& c) { c.negotiation = {}; });
    invalid([](auto& c) { c.src_node = c.dst_node; });
    invalid([](auto& c) { c.src_node = UINT16_MAX; });
    invalid([](auto& c) { c.transport = protocol::Transport::NONE; });
    invalid([](auto& c) { c.service = protocol::Service::NONE; });
    // A key output embedded in the input material is committed only after KDF.
    protocol::KeyMaterial alias{};
    alias.key = control;
    ASSERT_TRUE(protocol::derive_key(alias.key, context(), alias));
    EXPECT_EQ(alias.key, expected.key);
    EXPECT_EQ(alias.nonce_prefix, expected.nonce_prefix);
}

TEST(WkiRdmaProtocol, PrivateRootCapabilityVectorNormalizesExistingCapability) {
    crypto::Key root{};
    std::iota(root.begin(), root.end(), uint8_t{128});
    auto desc = descriptor();
    protocol::Capability output{};
    ASSERT_TRUE(protocol::derive_capability(root, 11, 7, 9, desc, output));
    EXPECT_EQ(output, CAPABILITY);
    desc.capability.fill(0x5a);
    ASSERT_TRUE(protocol::derive_capability(root, 11, 7, 9, desc, output));
    EXPECT_EQ(output, CAPABILITY);
    desc.capability = {};
    ASSERT_TRUE(protocol::derive_capability(root, 11, 7, 9, desc, output));
    EXPECT_EQ(output, CAPABILITY);
    auto reject = [&](auto change) {
        auto bad = desc;
        change(bad);
        output = CAPABILITY;
        EXPECT_FALSE(protocol::derive_capability(root, 11, 7, 9, bad, output));
        EXPECT_EQ(output, CAPABILITY);
    };
    reject([](auto& d) { d.rkey = 0; });
    reject([](auto& d) { d.size = 0; });
    reject([](auto& d) { d.generation = 0; });
    reject([](auto& d) { d.binding_generation = 0; });
    reject([](auto& d) { d.session_id = {}; });
    reject([](auto& d) { d.permissions = 0; });
    reject([](auto& d) { d.permissions |= 0x80; });
    reject([](auto& d) { d.version++; });
    reject([](auto& d) { d.reserved++; });
    EXPECT_FALSE(protocol::derive_capability(root, 11, 7, 0, desc, output));
    EXPECT_EQ(output, CAPABILITY);
}

TEST(WkiRdmaProtocol, CapabilityBindsEveryAuthorityFieldAndPrivateRoot) {
    crypto::Key root{};
    std::iota(root.begin(), root.end(), uint8_t{128});
    auto different = [&](auto change) {
        auto desc = descriptor();
        change(desc);
        protocol::Capability output{};
        ASSERT_TRUE(protocol::derive_capability(root, 11, 7, 9, desc, output));
        EXPECT_NE(output, CAPABILITY);
    };
    different([](auto& d) { d.rkey++; });
    different([](auto& d) { d.size++; });
    different([](auto& d) { d.generation++; });
    different([](auto& d) { d.binding_generation++; });
    different([](auto& d) { d.channel_id++; });
    different([](auto& d) { d.transport = static_cast<uint8_t>(protocol::Transport::IVSHMEM); });
    different([](auto& d) { d.service = static_cast<uint8_t>(protocol::Service::BLOCK); });
    different([](auto& d) { d.permissions = protocol::PERMIT_WRITE; });
    for (size_t index = 0; index < SESSION.size(); ++index) different([index](auto& d) { d.session_id[index] ^= 1; });
    protocol::Capability output{};
    ASSERT_TRUE(protocol::derive_capability(root, 7, 11, 9, descriptor(), output));
    EXPECT_NE(output, CAPABILITY);
    ASSERT_TRUE(protocol::derive_capability(root, 11, 8, 9, descriptor(), output));
    EXPECT_NE(output, CAPABILITY);
    ASSERT_TRUE(protocol::derive_capability(root, 11, 7, 10, descriptor(), output));
    EXPECT_NE(output, CAPABILITY);
    // A different root, including the peer-known directional key, differs.
    ASSERT_TRUE(protocol::derive_capability(KEY, 11, 7, 9, descriptor(), output));
    EXPECT_NE(output, CAPABILITY);
}

TEST(WkiRdmaProtocol, HeaderShapesDirectionsStatusesAndExactLengths) {
    auto h = header();
    EXPECT_EQ(protocol::validate_header(h, protocol::HEADER_BYTES + h.payload_length), protocol::Drop::NONE);
    h.flags = protocol::FLAG_RESPONSE;
    EXPECT_EQ(protocol::validate_header(h, protocol::HEADER_BYTES + h.payload_length), protocol::Drop::NONE);
    h.service = static_cast<uint8_t>(protocol::Service::BLOCK);
    EXPECT_NE(protocol::validate_header(h, protocol::HEADER_BYTES + h.payload_length), protocol::Drop::NONE);
    h = header();
    h.opcode = static_cast<uint8_t>(protocol::Opcode::READ_DATA);
    EXPECT_NE(protocol::validate_header(h, protocol::HEADER_BYTES + h.payload_length), protocol::Drop::NONE);
    h.flags = protocol::FLAG_RESPONSE;
    EXPECT_EQ(protocol::validate_header(h, protocol::HEADER_BYTES + h.payload_length), protocol::Drop::NONE);
    h = header();
    h.opcode = static_cast<uint8_t>(protocol::Opcode::READ);
    h.payload_length = 0;
    EXPECT_EQ(protocol::validate_header(h, protocol::HEADER_BYTES), protocol::Drop::NONE);
    h.total_length = 0;
    EXPECT_NE(protocol::validate_header(h, protocol::HEADER_BYTES), protocol::Drop::NONE);
    for (auto op : {protocol::Opcode::WRITE_END, protocol::Opcode::READ_END}) {
        h = header();
        h.opcode = static_cast<uint8_t>(op);
        h.flags = protocol::FLAG_RESPONSE;
        h.payload_length = 0;
        h.total_length = 0;
        h.status = -5;
        EXPECT_EQ(protocol::validate_header(h, protocol::HEADER_BYTES), protocol::Drop::NONE);
        h.status = 1;
        EXPECT_NE(protocol::validate_header(h, protocol::HEADER_BYTES), protocol::Drop::NONE);
        h.status = 0;
        h.flags = 0;
        EXPECT_NE(protocol::validate_header(h, protocol::HEADER_BYTES), protocol::Drop::NONE);
    }
    h = header();
    h.opcode = static_cast<uint8_t>(protocol::Opcode::NOTIFY);
    h.payload_length = h.total_length = 0;
    EXPECT_EQ(protocol::validate_header(h, protocol::HEADER_BYTES), protocol::Drop::NONE);
    h.flags = protocol::FLAG_RESPONSE;
    EXPECT_NE(protocol::validate_header(h, protocol::HEADER_BYTES), protocol::Drop::NONE);
    h = header();
    EXPECT_NE(protocol::validate_header(h, protocol::HEADER_BYTES + h.payload_length - 1), protocol::Drop::NONE);
    EXPECT_NE(protocol::validate_header(h, protocol::HEADER_BYTES + h.payload_length + 1), protocol::Drop::NONE);
}

TEST(WkiRdmaProtocol, HeaderRejectsZeroUnknownAndOverflowWithoutGrantMutation) {
    auto reject = [](auto change) {
        auto bad = header();
        change(bad);
        EXPECT_NE(protocol::validate_header(bad, protocol::HEADER_BYTES + bad.payload_length), protocol::Drop::NONE);
    };
    reject([](auto& h) { h.magic++; });
    reject([](auto& h) { h.version++; });
    reject([](auto& h) { h.transport = 0; });
    reject([](auto& h) { h.service = 255; });
    reject([](auto& h) { h.opcode = 255; });
    reject([](auto& h) { h.src_node = 0; });
    reject([](auto& h) { h.dst_node = UINT16_MAX; });
    reject([](auto& h) { h.src_node = h.dst_node; });
    reject([](auto& h) { h.session_id = {}; });
    reject([](auto& h) { h.capability = {}; });
    reject([](auto& h) { h.region_generation = 0; });
    reject([](auto& h) { h.binding_generation = 0; });
    reject([](auto& h) { h.operation_id = 0; });
    reject([](auto& h) { h.receive_ticket = 0; });
    reject([](auto& h) { h.counter = 0; });
    reject([](auto& h) { h.rkey = 0; });
    reject([](auto& h) { h.flags = 2; });
    reject([](auto& h) { h.reserved = 1; });
    reject([](auto& h) { h.reserved2 = 1; });
    reject([](auto& h) { h.status = -5; });
    reject([](auto& h) { h.total_length = protocol::MAX_TRANSFER_BYTES + 1; });
    reject([](auto& h) { h.region_offset = UINT64_MAX; });
    reject([](auto& h) { h.fragment_offset = 1; });
    reject([](auto& h) { h.payload_length--; });
    auto maximum = header();
    maximum.counter = UINT64_MAX;
    EXPECT_EQ(protocol::validate_header(maximum, protocol::HEADER_BYTES + maximum.payload_length), protocol::Drop::NONE);
}

TEST(WkiRdmaProtocol, GrantChecksExactAuthorityFullRangeAndMemoryPermissions) {
    auto desc = descriptor();
    auto h = header();
    EXPECT_EQ(protocol::validate_grant(h, desc), protocol::Drop::NONE);
    for (auto op : {protocol::Opcode::READ, protocol::Opcode::WRITE, protocol::Opcode::READ_DATA, protocol::Opcode::WRITE_END,
                    protocol::Opcode::READ_END, protocol::Opcode::NOTIFY}) {
        h = header();
        h.opcode = static_cast<uint8_t>(op);
        bool const DATA = op == protocol::Opcode::WRITE || op == protocol::Opcode::READ_DATA;
        h.payload_length = DATA ? 19 : 0;
        h.flags = op == protocol::Opcode::READ_DATA || op == protocol::Opcode::WRITE_END || op == protocol::Opcode::READ_END ? 1 : 0;
        if (op == protocol::Opcode::NOTIFY) h.total_length = 0;
        uint8_t const REQUIRED = op == protocol::Opcode::WRITE || op == protocol::Opcode::WRITE_END ? protocol::PERMIT_WRITE
                                 : op == protocol::Opcode::NOTIFY                                   ? protocol::PERMIT_NOTIFY
                                                                                                    : protocol::PERMIT_READ;
        for (uint8_t permissions = 1; permissions <= protocol::PERMISSIONS; ++permissions) {
            desc.permissions = permissions;
            EXPECT_EQ(protocol::validate_grant(h, desc), (permissions & REQUIRED) != 0 ? protocol::Drop::NONE : protocol::Drop::PERMISSION);
        }
    }
    desc = descriptor();
    h = header();
    h.flags = protocol::FLAG_RESPONSE;
    desc.permissions = protocol::PERMIT_READ;
    EXPECT_EQ(protocol::validate_grant(h, desc), protocol::Drop::PERMISSION);
    desc.permissions = protocol::PERMIT_WRITE;
    EXPECT_EQ(protocol::validate_grant(h, desc), protocol::Drop::NONE);
    desc = descriptor();
    h = header();
    h.region_offset = desc.size - h.total_length;
    EXPECT_EQ(protocol::validate_grant(h, desc), protocol::Drop::NONE);
    h.region_offset++;
    EXPECT_EQ(protocol::validate_grant(h, desc), protocol::Drop::RANGE);
    h = header();
    h.region_offset = UINT64_MAX;
    EXPECT_EQ(protocol::validate_grant(h, desc), protocol::Drop::RANGE);
    auto mismatch = [](auto change) {
        auto d = descriptor();
        change(d);
        auto const BEFORE = d;
        EXPECT_NE(protocol::validate_grant(header(), d), protocol::Drop::NONE);
        EXPECT_TRUE(bytes_equal(d, BEFORE));
    };
    mismatch([](auto& d) { d.rkey++; });
    mismatch([](auto& d) { d.generation++; });
    mismatch([](auto& d) { d.binding_generation++; });
    mismatch([](auto& d) { d.channel_id++; });
    mismatch([](auto& d) { d.session_id[0] ^= 1; });
    mismatch([](auto& d) { d.transport++; });
    mismatch([](auto& d) { d.service++; });
    mismatch([](auto& d) { d.version++; });
    mismatch([](auto& d) { d.reserved++; });
    mismatch([](auto& d) { d.permissions |= 0x80; });
    for (size_t index = 0; index < CAPABILITY.size(); ++index) mismatch([index](auto& d) { d.capability[index] ^= 1; });
}

TEST(WkiRdmaProtocol, IndependentCompleteRecordVector) {
    // Fixed independently generated hashlib/HMAC/HKDF/ChaCha20Poly1305 vector.
    std::array<uint8_t, RECORD.size()> output{};
    auto plain = plaintext();
    auto h = header();
    ASSERT_TRUE(protocol::encode(material(), h, plain.data(), output.data(), output.size()));
    EXPECT_EQ(output, RECORD);
    auto out_header = header();
    std::array<uint8_t, 19> recovered{};
    ASSERT_EQ(protocol::decode(material(), context(), RECORD.data(), RECORD.size(), out_header, recovered.data(), recovered.size()),
              protocol::Drop::NONE);
    EXPECT_EQ(recovered, plain);
    EXPECT_EQ(out_header.tag, hex<16>("4dbc594d7a81946ff83fcb436a5ccd37"));
    EXPECT_EQ(protocol::validate_grant(out_header, descriptor()), protocol::Drop::NONE);
}

TEST(WkiRdmaProtocol, EveryRecordBitTamperAndTruncationPreserveBothOutputs) {
    for (size_t index = 0; index < RECORD.size(); ++index) {
        for (unsigned bit = 0; bit < 8; ++bit) {
            auto changed = RECORD;
            changed[index] ^= static_cast<uint8_t>(1U << bit);
            expect_rejected_record(changed.data(), changed.size());
        }
    }
    for (size_t length = 0; length < RECORD.size(); ++length) expect_rejected_record(RECORD.data(), length);
    std::array<uint8_t, RECORD.size() + 1> trailing{};
    std::copy(RECORD.begin(), RECORD.end(), trailing.begin());
    expect_rejected_record(trailing.data(), trailing.size());
}

TEST(WkiRdmaProtocol, ContextAndNegotiatedKeyCannotCrossDomains) {
    auto wrong = [&](auto change) {
        auto ctx = context();
        change(ctx);
        expect_rejected_record(RECORD.data(), RECORD.size(), ctx);
    };
    wrong([](auto& c) { std::swap(c.src_node, c.dst_node); });
    wrong([](auto& c) { c.session_id[0] ^= 1; });
    wrong([](auto& c) { c.transport = protocol::Transport::IVSHMEM; });
    wrong([](auto& c) { c.service = protocol::Service::BLOCK; });
    wrong([](auto& c) { c.negotiation = {}; });
    crypto::Key control{};
    std::iota(control.begin(), control.end(), uint8_t{0});
    auto ctx = context();
    ctx.negotiation[0] ^= 1;
    protocol::KeyMaterial changed{};
    ASSERT_TRUE(protocol::derive_key(control, ctx, changed));
    expect_rejected_record(RECORD.data(), RECORD.size(), ctx, changed);
    changed = material();
    changed.nonce_prefix[0] ^= 1;
    expect_rejected_record(RECORD.data(), RECORD.size(), context(), changed);
}

TEST(WkiRdmaProtocol, ExactPrivateHeaderAndPayloadInPlaceWork) {
    struct RecordStorage {
        protocol::Header header;
        std::array<uint8_t, 19> payload;
    } storage{header(), plaintext()};
    static_assert(sizeof(storage) == RECORD.size());
    auto key = material();
    auto ctx = context();
    ASSERT_TRUE(protocol::encode(key, storage.header, storage.payload.data(), &storage, sizeof(storage)));
    EXPECT_EQ(std::memcmp(&storage, RECORD.data(), RECORD.size()), 0);
    ASSERT_EQ(protocol::decode(key, ctx, &storage, sizeof(storage), storage.header, storage.payload.data(), storage.payload.size()),
              protocol::Drop::NONE);
    EXPECT_EQ(storage.payload, plaintext());
}

TEST(WkiRdmaProtocol, BufferFailuresAndAmbiguousOverlapPreserveState) {
    auto key = material();
    auto ctx = context();
    auto h = header();
    auto plain = plaintext();
    std::array<uint8_t, RECORD.size() + 20> bytes{};
    bytes.fill(0xa5);
    auto const BEFORE = bytes;
    EXPECT_FALSE(protocol::encode(key, h, nullptr, bytes.data(), bytes.size()));
    EXPECT_FALSE(protocol::encode(key, h, plain.data(), nullptr, bytes.size()));
    EXPECT_FALSE(protocol::encode(key, h, plain.data(), bytes.data(), RECORD.size() - 1));
    EXPECT_FALSE(protocol::encode(key, h, bytes.data(), bytes.data(), bytes.size()));
    EXPECT_FALSE(protocol::encode(key, h, bytes.data() + protocol::HEADER_BYTES - 1, bytes.data(), bytes.size()));
    EXPECT_FALSE(protocol::encode(key, h, bytes.data() + protocol::HEADER_BYTES + 1, bytes.data(), bytes.size()));
    EXPECT_FALSE(protocol::encode(key, h, plain.data(), &key, bytes.size()));
    EXPECT_EQ(bytes, BEFORE);
    EXPECT_TRUE(bytes_equal(key, material()));
    auto const HEADER_BEFORE = h;
    EXPECT_NE(protocol::decode(key, ctx, RECORD.data(), RECORD.size(), h, nullptr, plain.size()), protocol::Drop::NONE);
    EXPECT_NE(protocol::decode(key, ctx, RECORD.data(), RECORD.size(), h, plain.data(), plain.size() - 1), protocol::Drop::NONE);
    EXPECT_NE(protocol::decode(key, ctx, RECORD.data(), RECORD.size(), h, &h, plain.size()), protocol::Drop::NONE);
    EXPECT_NE(protocol::decode(key, ctx, RECORD.data(), RECORD.size(), h, &key, plain.size()), protocol::Drop::NONE);
    EXPECT_NE(protocol::decode(key, ctx, RECORD.data(), RECORD.size(), h, &ctx, plain.size()), protocol::Drop::NONE);
    EXPECT_TRUE(bytes_equal(h, HEADER_BEFORE));
    EXPECT_TRUE(bytes_equal(key, material()));
    auto* invalid = reinterpret_cast<void*>(UINTPTR_MAX - 7);
    EXPECT_FALSE(protocol::encode(key, h, invalid, bytes.data(), bytes.size()));
    EXPECT_FALSE(protocol::encode(key, h, plain.data(), invalid, bytes.size()));
    expect_rejected_record(nullptr, RECORD.size());
    expect_rejected_record(invalid, RECORD.size());
}

TEST(WkiRdmaProtocol, EmptyTerminalAndMaximumFragmentsRoundTrip) {
    auto h = header();
    h.opcode = static_cast<uint8_t>(protocol::Opcode::READ_END);
    h.flags = protocol::FLAG_RESPONSE;
    h.payload_length = h.total_length = 0;
    h.status = -5;
    std::array<uint8_t, protocol::HEADER_BYTES> empty{};
    ASSERT_TRUE(protocol::encode(material(), h, nullptr, empty.data(), empty.size()));
    auto decoded = header();
    ASSERT_EQ(protocol::decode(material(), context(), empty.data(), empty.size(), decoded, nullptr, 0), protocol::Drop::NONE);
    EXPECT_EQ(decoded.status, -5);
    h = header();
    h.total_length = protocol::MAX_TRANSFER_BYTES;
    h.fragment_offset = protocol::MAX_TRANSFER_BYTES - protocol::FRAGMENT_BYTES;
    h.payload_length = protocol::FRAGMENT_BYTES;
    std::array<uint8_t, protocol::FRAGMENT_BYTES> plain{}, recovered{};
    std::iota(plain.begin(), plain.end(), uint8_t{1});
    std::array<uint8_t, protocol::RECORD_BYTES> record{};
    ASSERT_TRUE(protocol::encode(material(), h, plain.data(), record.data(), record.size()));
    ASSERT_EQ(protocol::decode(material(), context(), record.data(), record.size(), decoded, recovered.data(), recovered.size()),
              protocol::Drop::NONE);
    EXPECT_EQ(recovered, plain);
}

TEST(WkiRdmaProtocol, ReservationExhaustionSticksAtZero) {
    uint64_t next = UINT64_MAX - 1;
    EXPECT_EQ(protocol::reserve(next), UINT64_MAX - 1);
    EXPECT_EQ(next, UINT64_MAX);
    EXPECT_EQ(protocol::reserve(next), 0);
    EXPECT_EQ(next, 0);
    EXPECT_EQ(protocol::reserve(next), 0);
    EXPECT_EQ(next, 0);
    next = 1;
    EXPECT_EQ(protocol::reserve(next), 1);
    EXPECT_EQ(next, 2);
}

TEST(WkiRdmaProtocol, ReplayShiftsAndHorizonMatchIndependentSet) {
    for (uint64_t shift : {uint64_t{1}, uint64_t{2}, uint64_t{63}, uint64_t{64}, uint64_t{65}, uint64_t{127}, uint64_t{128}, uint64_t{8191},
                           uint64_t{8192}, uint64_t{8193}, UINT64_MAX - 9000}) {
        protocol::ReplayWindow actual{};
        ReplayReference reference;
        for (auto age : {uint64_t{0}, uint64_t{1}, uint64_t{63}, uint64_t{64}, uint64_t{65}, uint64_t{127}, uint64_t{8191}}) {
            EXPECT_EQ(protocol::replay_admit(actual, 9000 - age), reference.admit(9000 - age));
        }
        EXPECT_EQ(protocol::replay_admit(actual, 9000 + shift), reference.admit(9000 + shift));
        reference.compare(actual);
        auto const BEFORE = actual;
        EXPECT_EQ(protocol::replay_admit(actual, 0), protocol::Drop::COUNTER);
        EXPECT_TRUE(bytes_equal(actual, BEFORE));
        EXPECT_EQ(protocol::replay_admit(actual, actual.high), protocol::Drop::DUPLICATE);
        EXPECT_TRUE(bytes_equal(actual, BEFORE));
        EXPECT_EQ(protocol::replay_admit(actual, actual.high - protocol::REPLAY_BITS), protocol::Drop::TOO_OLD);
        EXPECT_TRUE(bytes_equal(actual, BEFORE));
    }
}

TEST(WkiRdmaProtocol, ReplayRandomReorderPropertyAndRejectedStateIdentity) {
    std::mt19937_64 random(2502001);
    protocol::ReplayWindow actual{};
    ReplayReference reference;
    for (size_t step = 0; step < 10000; ++step) {
        uint64_t counter = 0;
        switch (random() % 5) {
            case 0:
                counter = reference.high + 1 + random() % 10000;
                break;
            case 1:
                counter = reference.high;
                break;
            case 2:
            case 3: {
                auto age = random() % 10000;
                counter = reference.high > age ? reference.high - age : 0;
                break;
            }
            default:
                break;
        }
        auto const BEFORE = actual;
        auto const EXPECTED = reference.admit(counter);
        EXPECT_EQ(protocol::replay_admit(actual, counter), EXPECTED);
        if (EXPECTED != protocol::Drop::NONE) EXPECT_TRUE(bytes_equal(actual, BEFORE));
        reference.compare(actual);
    }
}

TEST(WkiRdmaProtocol, CoverageUniqueFragmentsCannotCompleteHoles) {
    protocol::Coverage coverage{};
    ASSERT_TRUE(protocol::coverage_begin(coverage, protocol::MAX_TRANSFER_BYTES - 17));
    std::array<uint32_t, protocol::MAX_FRAGMENTS> order{};
    std::iota(order.begin(), order.end(), uint32_t{0});
    std::mt19937 random(2502002);
    std::shuffle(order.begin(), order.end(), random);
    uint32_t received = 0;
    for (auto index : order) {
        if (index == 173) continue;
        uint32_t const OFFSET = index * protocol::FRAGMENT_BYTES;
        uint32_t const LENGTH = std::min(protocol::FRAGMENT_BYTES, coverage.length - OFFSET);
        ASSERT_EQ(protocol::coverage_admit(coverage, OFFSET, LENGTH), protocol::Drop::NONE);
        received += LENGTH;
        EXPECT_EQ(coverage.received, received);
        auto const BEFORE = coverage;
        EXPECT_EQ(protocol::coverage_admit(coverage, OFFSET, LENGTH), protocol::Drop::DUPLICATE);
        EXPECT_TRUE(bytes_equal(coverage, BEFORE));
        EXPECT_FALSE(protocol::coverage_complete(coverage));
    }
    ASSERT_EQ(protocol::coverage_admit(coverage, 173 * protocol::FRAGMENT_BYTES, protocol::FRAGMENT_BYTES), protocol::Drop::NONE);
    EXPECT_TRUE(protocol::coverage_complete(coverage));
    EXPECT_EQ(coverage.received, coverage.length);
    auto const BEFORE = coverage;
    EXPECT_FALSE(protocol::coverage_begin(coverage, protocol::MAX_TRANSFER_BYTES + 1));
    EXPECT_TRUE(bytes_equal(coverage, BEFORE));
    ASSERT_TRUE(protocol::coverage_begin(coverage, 0));
    EXPECT_TRUE(protocol::coverage_complete(coverage));
    auto const EMPTY = coverage;
    EXPECT_NE(protocol::coverage_admit(coverage, 0, 0), protocol::Drop::NONE);
    EXPECT_TRUE(bytes_equal(coverage, EMPTY));
}

TEST(WkiRdmaProtocol, CoverageRejectsOverlapMalformedTailsAndStrayBits) {
    protocol::Coverage coverage{};
    ASSERT_TRUE(protocol::coverage_begin(coverage, protocol::FRAGMENT_BYTES + 19));
    auto const BEFORE = coverage;
    for (auto [offset, length] :
         {std::pair{0u, 0u}, std::pair{0u, 1u}, std::pair{1u, protocol::FRAGMENT_BYTES}, std::pair{protocol::FRAGMENT_BYTES, 18u},
          std::pair{protocol::FRAGMENT_BYTES, 20u}, std::pair{protocol::FRAGMENT_BYTES + 19, 1u}, std::pair{UINT32_MAX, UINT32_MAX}}) {
        EXPECT_NE(protocol::coverage_admit(coverage, offset, length), protocol::Drop::NONE);
        EXPECT_TRUE(bytes_equal(coverage, BEFORE));
    }
    ASSERT_EQ(protocol::coverage_admit(coverage, protocol::FRAGMENT_BYTES, 19), protocol::Drop::NONE);
    EXPECT_FALSE(protocol::coverage_complete(coverage));
    ASSERT_EQ(protocol::coverage_admit(coverage, 0, protocol::FRAGMENT_BYTES), protocol::Drop::NONE);
    EXPECT_TRUE(protocol::coverage_complete(coverage));
    coverage.bits.back() |= uint64_t{1} << 63;
    EXPECT_FALSE(protocol::coverage_complete(coverage));
    coverage.length = UINT32_MAX;
    EXPECT_FALSE(protocol::coverage_complete(coverage));
}
