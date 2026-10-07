#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/wki/rdma_protocol.hpp>
#include <set>

namespace protocol = ker::net::wki::rdma_protocol;

namespace {

auto word(const uint8_t* data) -> uint64_t {
    uint64_t value = 0;
    std::memcpy(&value, data, sizeof(value));
    return value;
}

template <typename T>
auto equal(const T& lhs, const T& rhs) -> bool {
    return std::memcmp(&lhs, &rhs, sizeof(T)) == 0;
}

}  // namespace

extern "C" auto LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) -> int {
    protocol::KeyMaterial key{};
    protocol::KeyContext expected{};
    expected.session_id.fill(1);
    expected.negotiation.fill(2);
    expected.src_node = 7;
    expected.dst_node = 11;
    expected.transport = protocol::Transport::ROCE;
    expected.service = protocol::Service::VFS;
    ker::net::wki::rdma_crypto::Key test_control{};
    assert(protocol::derive_key(test_control, expected, key));
    protocol::Header output{};
    output.magic = 0xa5a5a5a5;
    auto const BEFORE_HEADER = output;
    std::array<uint8_t, protocol::FRAGMENT_BYTES> plaintext{};
    plaintext.fill(0xa5);
    auto const BEFORE_PLAIN = plaintext;
    auto const RESULT = protocol::decode(key, expected, data, size, output, plaintext.data(), plaintext.size());
    if (RESULT != protocol::Drop::NONE) {
        assert(equal(output, BEFORE_HEADER));
        assert(plaintext == BEFORE_PLAIN);
    } else {
        std::array<uint8_t, protocol::RECORD_BYTES> reencoded{};
        assert(protocol::encode(key, output, plaintext.data(), reencoded.data(), reencoded.size()));
        assert(std::memcmp(reencoded.data(), data, size) == 0);
    }
    if (size >= sizeof(protocol::Offer)) {
        protocol::Offer offer{};
        std::memcpy(&offer, data, sizeof(offer));
        auto const BEFORE = offer;
        (void)protocol::valid_offer(offer);
        assert(equal(offer, BEFORE));
    }
    if (size >= sizeof(protocol::Header)) {
        protocol::Header header{};
        std::memcpy(&header, data, sizeof(header));
        auto const BEFORE = header;
        (void)protocol::validate_header(header, size);
        protocol::RegionDescriptor grant{};
        (void)protocol::validate_grant(header, grant);
        assert(equal(header, BEFORE));
    }

    // Every input also exercises a well-shaped record with adversarial bytes,
    // then a tampered copy. No unbounded allocations or transfer-sized stack.
    protocol::Header shaped{};
    shaped.transport = static_cast<uint8_t>(expected.transport);
    shaped.service = static_cast<uint8_t>(expected.service);
    shaped.opcode = static_cast<uint8_t>(protocol::Opcode::WRITE);
    shaped.src_node = expected.src_node;
    shaped.dst_node = expected.dst_node;
    shaped.session_id = expected.session_id;
    shaped.region_generation = shaped.operation_id = shaped.binding_generation = shaped.receive_ticket = shaped.counter = 1;
    shaped.capability.fill(3);
    shaped.rkey = 1;
    shaped.total_length = shaped.payload_length = static_cast<uint32_t>(std::min(size, size_t{protocol::FRAGMENT_BYTES}));
    if (shaped.total_length == 0) {
        shaped.opcode = static_cast<uint8_t>(protocol::Opcode::READ_END);
        shaped.flags = protocol::FLAG_RESPONSE;
    }
    std::array<uint8_t, protocol::RECORD_BYTES> record{};
    assert(protocol::encode(key, shaped, data, record.data(), record.size()));
    size_t const RECORD_SIZE = protocol::HEADER_BYTES + shaped.payload_length;
    assert(protocol::decode(key, expected, record.data(), RECORD_SIZE, output, plaintext.data(), plaintext.size()) == protocol::Drop::NONE);
    assert(size == 0 || std::memcmp(plaintext.data(), data, shaped.payload_length) == 0);
    auto const VALID_HEADER = output;
    auto const VALID_PLAIN = plaintext;
    record[RECORD_SIZE - 1] ^= 1;
    assert(protocol::decode(key, expected, record.data(), RECORD_SIZE, output, plaintext.data(), plaintext.size()) != protocol::Drop::NONE);
    assert(equal(output, VALID_HEADER));
    assert(plaintext == VALID_PLAIN);

    protocol::ReplayWindow replay{};
    std::set<uint64_t> accepted;
    uint64_t high = 0;
    for (size_t offset = 0, steps = 0; size - offset >= 8 && steps < 256; offset += 8, ++steps) {
        uint64_t const VALUE = word(data + offset);
        uint64_t counter = VALUE;
        switch (VALUE & 3U) {
            case 0:
                counter = 0;
                break;
            case 1:
                counter = high;
                break;
            case 2: {
                uint64_t const AGE = (VALUE >> 2U) % (protocol::REPLAY_BITS + 2);
                counter = high > AGE ? high - AGE : 0;
                break;
            }
            default:
                break;
        }
        protocol::Drop reference = protocol::Drop::NONE;
        if (counter == 0)
            reference = protocol::Drop::COUNTER;
        else if (counter <= high && high - counter >= protocol::REPLAY_BITS)
            reference = protocol::Drop::TOO_OLD;
        else if (accepted.contains(counter))
            reference = protocol::Drop::DUPLICATE;
        auto const BEFORE = replay;
        assert(protocol::replay_admit(replay, counter) == reference);
        if (reference != protocol::Drop::NONE)
            assert(equal(replay, BEFORE));
        else {
            high = std::max(high, counter);
            accepted.insert(counter);
            while (!accepted.empty() && high - *accepted.begin() >= protocol::REPLAY_BITS) accepted.erase(accepted.begin());
        }
        std::array<uint64_t, protocol::REPLAY_BITS / 64> bits{};
        for (auto admitted : accepted) {
            auto const AGE = high - admitted;
            bits[AGE / 64] |= uint64_t{1} << (AGE % 64);
        }
        assert(replay.high == high && replay.bits == bits);
    }

    protocol::Coverage coverage{};
    uint32_t const LENGTH = size >= 8 ? static_cast<uint32_t>(word(data) % (protocol::MAX_TRANSFER_BYTES + 1)) : 0;
    assert(protocol::coverage_begin(coverage, LENGTH));
    uint32_t const FRAGMENTS = (LENGTH + protocol::FRAGMENT_BYTES - 1) / protocol::FRAGMENT_BYTES;
    std::array<bool, protocol::MAX_FRAGMENTS> seen{};
    uint32_t received = 0;
    for (size_t offset = 0, steps = 0; size - offset >= 8 && steps < 256; offset += 8, ++steps) {
        uint64_t const VALUE = word(data + offset);
        uint32_t const FRAGMENT = FRAGMENTS == 0 ? 0 : static_cast<uint32_t>(VALUE % FRAGMENTS);
        uint32_t const POSITION = FRAGMENT * protocol::FRAGMENT_BYTES;
        uint32_t const BYTES = FRAGMENTS == 0 ? 0 : std::min(protocol::FRAGMENT_BYTES, LENGTH - POSITION);
        auto const BEFORE = coverage;
        auto const ADMISSION = protocol::coverage_admit(coverage, POSITION, BYTES);
        if (FRAGMENTS == 0) {
            assert(ADMISSION == protocol::Drop::FRAGMENT);
            assert(equal(coverage, BEFORE));
        } else if (seen[FRAGMENT]) {
            assert(ADMISSION == protocol::Drop::DUPLICATE);
            assert(equal(coverage, BEFORE));
        } else {
            assert(ADMISSION == protocol::Drop::NONE);
            seen[FRAGMENT] = true;
            received += BYTES;
        }
        assert(coverage.received == received);
        bool complete = true;
        for (size_t index = 0; index < FRAGMENTS; ++index) complete &= seen[index];
        assert(protocol::coverage_complete(coverage) == complete);
    }
    ker::net::wki::auth_crypto::secure_erase(&key, sizeof(key));
    ker::net::wki::auth_crypto::secure_erase(plaintext.data(), plaintext.size());
    return 0;
}
