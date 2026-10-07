#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <latch>
#include <limits>
#include <net/wki/rdma_protocol.hpp>
#include <new>
#include <numeric>
#include <random>
#include <set>
#include <thread>
#include <vector>

// Test-only access to the actual private guard/counters and erasure targets.
// The production header has no test hook and exposes no key material.
#define private public
#include <net/wki/rdma_session.hpp>
#undef private

namespace session = ker::net::wki::rdma_session;
namespace protocol = ker::net::wki::rdma_protocol;
namespace crypto = ker::net::wki::rdma_crypto;

namespace {

auto offer(uint8_t nonce = 3, uint64_t incarnation = 1) -> protocol::Offer {
    protocol::Offer output{};
    output.transports = protocol::TRANSPORTS;
    output.services = protocol::SERVICES;
    output.record_bytes = protocol::RECORD_BYTES;
    output.nonce.fill(nonce);
    for (size_t index = 0; index < sizeof(incarnation); ++index) output.nonce[index] = static_cast<uint8_t>(incarnation >> (index * 8U));
    output.session_id.fill(0x35);
    return output;
}

auto control(uint8_t seed) -> crypto::Key {
    crypto::Key output{};
    std::iota(output.begin(), output.end(), seed);
    return output;
}

auto header(protocol::Transport transport = protocol::Transport::ROCE, protocol::Service service = protocol::Service::VFS,
            bool reverse = false, uint32_t length = 19) -> protocol::Header {
    protocol::Header output{};
    output.transport = static_cast<uint8_t>(transport);
    output.service = static_cast<uint8_t>(service);
    output.opcode = static_cast<uint8_t>(protocol::Opcode::WRITE);
    output.src_node = reverse ? 11 : 7;
    output.dst_node = reverse ? 7 : 11;
    output.session_id = offer().session_id;
    output.region_generation = 13;
    output.binding_generation = 17;
    output.operation_id = 23;
    output.receive_ticket = 29;
    output.capability.fill(0x77);
    output.rkey = 31;
    output.total_length = output.payload_length = length;
    return output;
}

auto payload(size_t length = 19) -> std::vector<uint8_t> {
    std::vector<uint8_t> output(length);
    for (size_t index = 0; index < length; ++index) output[index] = static_cast<uint8_t>(index * 17 + 5);
    return output;
}

struct Pair {
    session::Session left;
    session::Session right;
    protocol::Offer local = offer();
    protocol::Offer remote = offer(7);
    crypto::Key forward = control(11);
    crypto::Key backward = control(93);

    Pair() {
        EXPECT_EQ(left.begin(7, 11, 1, local), protocol::Drop::NONE);
        EXPECT_EQ(right.begin(11, 7, 1, remote), protocol::Drop::NONE);
        EXPECT_EQ(left.install(remote, forward, backward), protocol::Drop::NONE);
        EXPECT_EQ(right.install(local, backward, forward), protocol::Drop::NONE);
    }
};

auto packet(session::Session& sender, const protocol::Header& input) -> std::vector<uint8_t> {
    auto plain = payload(input.payload_length);
    std::vector<uint8_t> record(protocol::HEADER_BYTES + plain.size(), 0xa5);
    EXPECT_EQ(sender.send(input, plain.data(), record.data(), record.size()), protocol::Drop::NONE);
    return record;
}

auto parsed(const std::vector<uint8_t>& record) -> protocol::Header {
    protocol::Header output{};
    std::memcpy(&output, record.data(), sizeof(output));
    return output;
}

template <typename T>
auto bytes(const T& value) -> std::array<uint8_t, sizeof(T)> {
    std::array<uint8_t, sizeof(T)> output{};
    std::memcpy(output.data(), &value, sizeof(value));
    return output;
}

template <typename T>
auto zero(const T& value) -> bool {
    auto data = bytes(value);
    return std::all_of(data.begin(), data.end(), [](uint8_t byte) { return byte == 0; });
}

void rejected(session::Session& receiver, std::vector<uint8_t> record, protocol::Drop expected,
              protocol::Transport ingress = protocol::Transport::ROCE) {
    auto output = header();
    output.magic = 0xa5a5a5a5;
    auto before = bytes(output);
    EXPECT_EQ(receiver.receive(ingress, record.data(), record.size(), output), expected);
    EXPECT_EQ(bytes(output), before);
    EXPECT_TRUE(std::all_of(record.begin(), record.end(), [](uint8_t byte) { return byte == 0; }));
}

void erased(const session::Session& value) {
    EXPECT_TRUE(zero(value.domains_));
    EXPECT_TRUE(zero(value.local_offer_));
    EXPECT_TRUE(zero(value.remote_offer_));
    EXPECT_TRUE(zero(value.negotiation_));
    EXPECT_EQ(value.local_, 0);
    EXPECT_EQ(value.peer_, 0);
    EXPECT_EQ(value.incarnation_, 0);
    EXPECT_EQ(value.transports_, 0);
    EXPECT_EQ(value.services_, 0);
    EXPECT_FALSE(value.begun_);
    EXPECT_FALSE(value.negotiated_);
}

}  // namespace

TEST(WkiRdmaSession, AllEightDomainsHavePairedDirectionalKeysAndIndependentState) {
    Pair pair;
    session::Statistics statistics{};
    ASSERT_EQ(pair.left.statistics(statistics), protocol::Drop::NONE);
    EXPECT_TRUE(statistics.begun);
    EXPECT_TRUE(statistics.negotiated);
    EXPECT_EQ(statistics.live_keys, 16);
    EXPECT_EQ(statistics.replay_windows, 8);
    EXPECT_EQ(statistics.incarnation, 1);
    std::set<crypto::Key> unique;
    for (size_t index = 0; index < session::DOMAIN_COUNT; ++index) {
        auto transport = static_cast<protocol::Transport>(index / 4 + 1);
        auto service = static_cast<protocol::Service>(index % 4 + 1);
        ASSERT_TRUE(pair.left.available(transport, service));
        ASSERT_TRUE(pair.right.available(transport, service));
        EXPECT_EQ(pair.left.domains_[index].tx.key, pair.right.domains_[index].rx.key);
        EXPECT_EQ(pair.left.domains_[index].rx.key, pair.right.domains_[index].tx.key);
        EXPECT_TRUE(unique.insert(pair.left.domains_[index].tx.key).second);
        EXPECT_TRUE(unique.insert(pair.left.domains_[index].rx.key).second);
        auto forward = packet(pair.left, header(transport, service));
        auto reverse = packet(pair.right, header(transport, service, true));
        protocol::Header output{};
        ASSERT_EQ(pair.right.receive(transport, forward.data(), forward.size(), output), protocol::Drop::NONE);
        EXPECT_EQ(output.counter, 1);
        auto expected_payload = payload();
        EXPECT_TRUE(std::equal(expected_payload.begin(), expected_payload.end(), forward.begin() + protocol::HEADER_BYTES));
        ASSERT_EQ(pair.left.receive(transport, reverse.data(), reverse.size(), output), protocol::Drop::NONE);
        EXPECT_EQ(output.counter, 1);
        EXPECT_EQ(pair.left.domains_[index].next_tx, 2);
        EXPECT_EQ(pair.right.domains_[index].next_tx, 2);
        EXPECT_EQ(pair.left.domains_[index].replay.high, 1);
        EXPECT_EQ(pair.right.domains_[index].replay.high, 1);
    }
    EXPECT_EQ(unique.size(), 16);
}

TEST(WkiRdmaSession, CallerControlKeysAreNotRetainedOrBorrowed) {
    Pair pair;
    pair.forward.fill(0);
    pair.backward.fill(0);
    pair.local = {};
    pair.remote = {};
    auto record = packet(pair.left, header());
    protocol::Header output{};
    ASSERT_EQ(pair.right.receive(protocol::Transport::ROCE, record.data(), record.size(), output), protocol::Drop::NONE);
    EXPECT_EQ(output.counter, 1);
}

TEST(WkiRdmaSession, BeginValidatesIdentityOfferAndMonotonicIncarnation) {
    session::Session value;
    auto local = offer();
    EXPECT_EQ(value.begin(0, 11, 1, local), protocol::Drop::IDENTITY);
    EXPECT_EQ(value.begin(7, 7, 1, local), protocol::Drop::IDENTITY);
    EXPECT_EQ(value.begin(UINT16_MAX, 11, 1, local), protocol::Drop::IDENTITY);
    EXPECT_EQ(value.begin(7, 11, 0, local), protocol::Drop::RETIRED);
    auto bad = local;
    bad.nonce.fill(0);
    EXPECT_EQ(value.begin(7, 11, 1, bad), protocol::Drop::FIELD);
    local = offer(3, 3);
    ASSERT_EQ(value.begin(7, 11, 3, local), protocol::Drop::NONE);
    EXPECT_EQ(value.begin(7, 11, 4, local), protocol::Drop::RETIRED);
    EXPECT_EQ(value.last_incarnation_, 3);
    ASSERT_EQ(value.retire(), protocol::Drop::NONE);
    for (uint64_t incarnation : {uint64_t{0}, uint64_t{1}, uint64_t{3}}) {
        EXPECT_EQ(value.begin(7, 11, incarnation, local), protocol::Drop::RETIRED);
    }
    local = offer(9, UINT64_MAX);
    ASSERT_EQ(value.begin(7, 11, UINT64_MAX, local), protocol::Drop::NONE);
    ASSERT_EQ(value.retire(), protocol::Drop::NONE);
    EXPECT_EQ(value.begin(7, 11, UINT64_MAX, local), protocol::Drop::RETIRED);
}

TEST(WkiRdmaSession, InvalidInitialRemoteOfferDoesNotInstallOrEraseValidBegin) {
    session::Session value;
    auto local = offer();
    auto remote = offer(7);
    auto tx = control(11), rx = control(93);
    ASSERT_EQ(value.begin(7, 11, 1, local), protocol::Drop::NONE);
    auto bad = remote;
    bad.reserved = 1;
    EXPECT_EQ(value.install(bad, tx, rx), protocol::Drop::FIELD);
    bad = remote;
    bad.session_id[0] ^= 1;
    EXPECT_EQ(value.install(bad, tx, rx), protocol::Drop::SESSION);
    EXPECT_TRUE(value.begun_);
    EXPECT_FALSE(value.negotiated_);
    EXPECT_TRUE(zero(value.domains_));
    EXPECT_EQ(value.install(remote, tx, rx), protocol::Drop::NONE);
}

TEST(WkiRdmaSession, PolicyIntersectionsIncludingZeroAreSafeFallback) {
    for (uint8_t local_mask : {uint8_t{0}, uint8_t{1}, uint8_t{2}, uint8_t{3}}) {
        for (uint8_t remote_mask : {uint8_t{0}, uint8_t{1}, uint8_t{2}, uint8_t{3}}) {
            session::Session value;
            auto local = offer(), remote = offer(7);
            local.transports = local_mask;
            remote.transports = remote_mask;
            local.services = 5;
            remote.services = 6;
            ASSERT_EQ(value.begin(7, 11, 1, local), protocol::Drop::NONE);
            ASSERT_EQ(value.install(remote, control(11), control(93)), protocol::Drop::NONE);
            session::Statistics stats{};
            ASSERT_EQ(value.statistics(stats), protocol::Drop::NONE);
            EXPECT_TRUE(stats.negotiated);
            EXPECT_EQ(stats.transports, local_mask & remote_mask);
            EXPECT_EQ(stats.services, 4);
            for (size_t i = 0; i < 8; ++i) {
                auto transport = static_cast<protocol::Transport>(i / 4 + 1);
                auto service = static_cast<protocol::Service>(i % 4 + 1);
                bool expected = ((local_mask & remote_mask) & protocol::transport_bit(transport)) != 0 && service == protocol::Service::IPC;
                EXPECT_EQ(value.available(transport, service), expected);
                if (!expected) EXPECT_TRUE(zero(value.domains_[i]));
            }
        }
    }
    session::Session disabled;
    auto local = offer(), remote = offer(7);
    remote.services = 0;
    ASSERT_EQ(disabled.begin(7, 11, 1, local), protocol::Drop::NONE);
    ASSERT_EQ(disabled.install(remote, control(11), control(93)), protocol::Drop::NONE);
    EXPECT_TRUE(zero(disabled.domains_));
    EXPECT_FALSE(disabled.available(protocol::Transport::NONE, protocol::Service::NONE));
    auto plain = payload();
    std::vector<uint8_t> output(163, 0xa5);
    auto before = output;
    EXPECT_EQ(disabled.send(header(), plain.data(), output.data(), output.size()), protocol::Drop::DOMAIN);
    EXPECT_EQ(output, before);
}

TEST(WkiRdmaSession, ExactDuplicateOfferDoesNotResetOrRederiveAnything) {
    Pair pair;
    auto record = packet(pair.left, header());
    auto duplicate = record;
    protocol::Header output{};
    ASSERT_EQ(pair.right.receive(protocol::Transport::ROCE, record.data(), record.size(), output), protocol::Drop::NONE);
    auto left_before = bytes(pair.left.domains_);
    auto right_before = bytes(pair.right.domains_);
    EXPECT_EQ(pair.left.install(pair.remote, control(44), control(55)), protocol::Drop::NONE);
    EXPECT_EQ(pair.right.install(pair.local, control(66), control(77)), protocol::Drop::NONE);
    EXPECT_EQ(bytes(pair.left.domains_), left_before);
    EXPECT_EQ(bytes(pair.right.domains_), right_before);
    EXPECT_EQ(parsed(packet(pair.left, header())).counter, 2);
    rejected(pair.right, duplicate, protocol::Drop::DUPLICATE);
}

TEST(WkiRdmaSession, ChangedOfferRetiresEvenIfTheNewOfferIsMalformed) {
    for (bool malformed : {false, true}) {
        Pair pair;
        auto record = packet(pair.left, header());
        auto changed = pair.remote;
        if (malformed)
            changed.reserved = 1;
        else
            changed.nonce[0] ^= 1;
        EXPECT_EQ(pair.left.install(changed, pair.forward, pair.backward), protocol::Drop::RETIRED);
        erased(pair.left);
        EXPECT_EQ(pair.left.last_incarnation_, 1);
        EXPECT_EQ(pair.left.install(pair.remote, pair.forward, pair.backward), protocol::Drop::RETIRED);
        EXPECT_EQ(pair.left.begin(7, 11, 1, pair.local), protocol::Drop::RETIRED);
        EXPECT_FALSE(pair.left.available(protocol::Transport::ROCE, protocol::Service::VFS));
        rejected(pair.left, record, protocol::Drop::RETIRED);
    }
}

TEST(WkiRdmaSession, FreshIncarnationChangesKeysAndRejectsOldCiphertext) {
    Pair pair;
    auto record = packet(pair.left, header());
    auto old_key = pair.right.domains_[1].rx.key;
    ASSERT_EQ(pair.right.retire(), protocol::Drop::NONE);
    pair.remote = offer(7, 2);
    ASSERT_EQ(pair.right.begin(11, 7, 2, pair.remote), protocol::Drop::NONE);
    ASSERT_EQ(pair.right.install(pair.local, pair.backward, pair.forward), protocol::Drop::NONE);
    EXPECT_NE(pair.right.domains_[1].rx.key, old_key);
    rejected(pair.right, record, protocol::Drop::AUTHENTICATION);
}

TEST(WkiRdmaSession, SendChecksCurrentContextBeforeReserving) {
    Pair pair;
    auto plain = payload();
    std::vector<uint8_t> record(163, 0xa5);
    auto before = record;
    auto input = header();
    input.counter = 7;
    EXPECT_EQ(pair.left.send(input, plain.data(), record.data(), record.size()), protocol::Drop::COUNTER);
    input = header();
    input.src_node = 12;
    EXPECT_EQ(pair.left.send(input, plain.data(), record.data(), record.size()), protocol::Drop::IDENTITY);
    input = header();
    input.session_id[0] ^= 1;
    EXPECT_EQ(pair.left.send(input, plain.data(), record.data(), record.size()), protocol::Drop::SESSION);
    input = header();
    input.service = 0;
    EXPECT_EQ(pair.left.send(input, plain.data(), record.data(), record.size()), protocol::Drop::DOMAIN);
    EXPECT_EQ(record, before);
    EXPECT_EQ(pair.left.domains_[1].next_tx, 1);
}

TEST(WkiRdmaSession, EncodingErrorsBurnExactlyOneCounterAndPreserveOutput) {
    Pair pair;
    auto plain = payload();
    std::vector<uint8_t> record(164, 0xa5);
    auto before = record;
    auto input = header();
    input.receive_ticket = 0;
    EXPECT_EQ(pair.left.send(input, plain.data(), record.data(), record.size()), protocol::Drop::FIELD);
    EXPECT_EQ(pair.left.domains_[1].next_tx, 2);
    input = header();
    EXPECT_EQ(pair.left.send(input, plain.data(), record.data(), 1), protocol::Drop::FIELD);
    EXPECT_EQ(pair.left.domains_[1].next_tx, 3);
    EXPECT_EQ(pair.left.send(input, nullptr, record.data(), record.size()), protocol::Drop::FIELD);
    EXPECT_EQ(pair.left.domains_[1].next_tx, 4);
    EXPECT_EQ(pair.left.send(input, plain.data(), nullptr, record.size()), protocol::Drop::FIELD);
    EXPECT_EQ(pair.left.domains_[1].next_tx, 5);
    EXPECT_EQ(pair.left.send(input, record.data() + protocol::HEADER_BYTES - 1, record.data(), record.size()), protocol::Drop::FIELD);
    EXPECT_EQ(pair.left.domains_[1].next_tx, 6);
    EXPECT_EQ(record, before);
    EXPECT_EQ(parsed(packet(pair.left, input)).counter, 6);
}

TEST(WkiRdmaSession, CounterExhaustionIsTerminalAndOtherDomainsStayIndependent) {
    Pair pair;
    pair.left.domains_[1].next_tx = UINT64_MAX - 1;
    auto final = packet(pair.left, header());
    EXPECT_EQ(parsed(final).counter, UINT64_MAX - 1);
    auto plain = payload();
    auto before = final;
    EXPECT_EQ(pair.left.send(header(), plain.data(), final.data(), final.size()), protocol::Drop::COUNTER);
    EXPECT_EQ(pair.left.domains_[1].next_tx, 0);
    EXPECT_EQ(pair.left.send(header(), plain.data(), final.data(), final.size()), protocol::Drop::COUNTER);
    EXPECT_EQ(final, before);
    EXPECT_EQ(parsed(packet(pair.left, header(protocol::Transport::IVSHMEM))).counter, 1);
    protocol::Header output{};
    EXPECT_EQ(pair.right.receive(protocol::Transport::ROCE, final.data(), final.size(), output), protocol::Drop::NONE);
}

TEST(WkiRdmaSession, ReceiveFailureErasesExactBoundedRecordAndPreservesHeader) {
    Pair pair;
    auto record = packet(pair.left, header());
    for (size_t index = 0; index < record.size(); ++index) {
        auto corrupted = record;
        corrupted[index] ^= 1;
        auto output = header();
        auto before = bytes(output);
        EXPECT_NE(pair.right.receive(protocol::Transport::ROCE, corrupted.data(), corrupted.size(), output), protocol::Drop::NONE);
        EXPECT_EQ(bytes(output), before);
        EXPECT_TRUE(std::all_of(corrupted.begin(), corrupted.end(), [](uint8_t byte) { return byte == 0; }));
    }
    rejected(pair.right, record, protocol::Drop::DOMAIN, protocol::Transport::IVSHMEM);
    rejected(pair.left, record, protocol::Drop::IDENTITY);
    ASSERT_EQ(pair.right.retire(), protocol::Drop::NONE);
    rejected(pair.right, record, protocol::Drop::RETIRED);
}

TEST(WkiRdmaSession, WrongDirectionalControlKeyNeverAuthenticates) {
    Pair pair;
    session::Session wrong;
    ASSERT_EQ(wrong.begin(11, 7, 1, pair.remote), protocol::Drop::NONE);
    ASSERT_EQ(wrong.install(pair.local, pair.backward, control(44)), protocol::Drop::NONE);
    rejected(wrong, packet(pair.left, header()), protocol::Drop::AUTHENTICATION);
}

TEST(WkiRdmaSession, ReplayReorderDuplicateAndHorizonAreSerializedWithDecryption) {
    Pair pair;
    std::vector<std::vector<uint8_t>> packets;
    for (size_t i = 0; i < 65; ++i) packets.push_back(packet(pair.left, header()));
    std::array<size_t, 65> order{};
    std::iota(order.begin(), order.end(), 0);
    std::mt19937 random(0x35);
    std::shuffle(order.begin(), order.end(), random);
    protocol::Header output{};
    for (auto index : order) {
        auto record = packets[index];
        ASSERT_EQ(pair.right.receive(protocol::Transport::ROCE, record.data(), record.size(), output), protocol::Drop::NONE);
        EXPECT_EQ(output.counter, index + 1);
        rejected(pair.right, packets[index], protocol::Drop::DUPLICATE);
    }
    EXPECT_EQ(pair.right.domains_[1].replay.high, 65);
    Pair edge;
    auto old = packet(edge.left, header());
    edge.left.domains_[1].next_tx = protocol::REPLAY_BITS + 1;
    auto newest = packet(edge.left, header());
    ASSERT_EQ(edge.right.receive(protocol::Transport::ROCE, newest.data(), newest.size(), output), protocol::Drop::NONE);
    rejected(edge.right, old, protocol::Drop::TOO_OLD);
    edge.left.domains_[1].next_tx = 2;  // private receiver-boundary fixture only
    auto boundary = packet(edge.left, header());
    ASSERT_EQ(edge.right.receive(protocol::Transport::ROCE, boundary.data(), boundary.size(), output), protocol::Drop::NONE);
    EXPECT_EQ(output.counter, 2);
}

TEST(WkiRdmaSession, IncomingMaximumCounterIsAcceptedOnceWithoutWrap) {
    Pair pair;
    auto input = header();
    input.counter = UINT64_MAX;
    auto plain = payload();
    std::vector<uint8_t> record(163);
    ASSERT_TRUE(protocol::encode(pair.left.domains_[1].tx, input, plain.data(), record.data(), record.size()));
    auto duplicate = record;
    protocol::Header output{};
    ASSERT_EQ(pair.right.receive(protocol::Transport::ROCE, record.data(), record.size(), output), protocol::Drop::NONE);
    EXPECT_EQ(output.counter, UINT64_MAX);
    EXPECT_EQ(pair.right.domains_[1].replay.high, UINT64_MAX);
    rejected(pair.right, duplicate, protocol::Drop::DUPLICATE);
}

TEST(WkiRdmaSession, InvalidReceiveRangesAndOutputAliasesAreNeverTouched) {
    Pair pair;
    auto record = packet(pair.left, header());
    auto before = record;
    auto output = header();
    auto header_before = bytes(output);
    for (size_t size : {size_t{0}, protocol::HEADER_BYTES - 1, size_t{protocol::RECORD_BYTES + 1}, SIZE_MAX}) {
        EXPECT_EQ(pair.right.receive(protocol::Transport::ROCE, record.data(), size, output), protocol::Drop::LENGTH);
        EXPECT_EQ(record, before);
    }
    EXPECT_EQ(pair.right.receive(protocol::Transport::ROCE, nullptr, 144, output), protocol::Drop::LENGTH);
    EXPECT_EQ(pair.right.receive(protocol::Transport::ROCE, reinterpret_cast<void*>(UINTPTR_MAX - 10), 144, output),
              protocol::Drop::LENGTH);
    EXPECT_EQ(
        pair.right.receive(protocol::Transport::ROCE, record.data(), record.size(), *reinterpret_cast<protocol::Header*>(record.data())),
        protocol::Drop::LENGTH);
    EXPECT_EQ(pair.right.receive(protocol::Transport::ROCE, record.data(), record.size(),
                                 *reinterpret_cast<protocol::Header*>(record.data() + 1)),
              protocol::Drop::LENGTH);
    EXPECT_EQ(record, before);
    EXPECT_EQ(bytes(output), header_before);
    auto session_before = bytes(pair.right);
    EXPECT_EQ(pair.right.receive(protocol::Transport::ROCE, &pair.right, 144, output), protocol::Drop::LENGTH);
    EXPECT_EQ(pair.right.receive(protocol::Transport::ROCE, record.data(), record.size(),
                                 *reinterpret_cast<protocol::Header*>(&pair.right.local_offer_)),
              protocol::Drop::LENGTH);
    EXPECT_EQ(bytes(pair.right), session_before);
    EXPECT_EQ(record, before);
}

TEST(WkiRdmaSession, AllBorrowedInputsAndOutputsExcludeSessionStorage) {
    Pair pair;
    auto before = bytes(pair.left);
    auto plain = payload();
    std::vector<uint8_t> record(163, 0xa5);
    auto record_before = record;
    EXPECT_EQ(pair.left.begin(7, 11, 2, pair.left.local_offer_), protocol::Drop::LENGTH);
    EXPECT_EQ(pair.left.install(pair.left.remote_offer_, pair.forward, pair.backward), protocol::Drop::LENGTH);
    EXPECT_EQ(pair.left.install(pair.remote, pair.left.domains_[0].tx.key, pair.backward), protocol::Drop::LENGTH);
    EXPECT_EQ(pair.left.send(*reinterpret_cast<protocol::Header*>(&pair.left.local_offer_), plain.data(), record.data(), record.size()),
              protocol::Drop::LENGTH);
    EXPECT_EQ(pair.left.send(header(), &pair.left, record.data(), record.size()), protocol::Drop::LENGTH);
    EXPECT_EQ(pair.left.send(header(), plain.data(), &pair.left, record.size()), protocol::Drop::LENGTH);
    EXPECT_EQ(pair.left.statistics(*reinterpret_cast<session::Statistics*>(&pair.left.domains_[0])), protocol::Drop::LENGTH);
    EXPECT_EQ(bytes(pair.left), before);
    EXPECT_EQ(record, record_before);
}

TEST(WkiRdmaSession, ExactSendInPlaceWorksAndPartialHeaderAliasBurns) {
    Pair pair;
    auto input = header();
    auto plain = payload();
    std::vector<uint8_t> record(164, 0xa5);
    std::memcpy(record.data(), &input, sizeof(input));
    auto before = record;
    auto& aliased = *reinterpret_cast<protocol::Header*>(record.data());
    EXPECT_EQ(pair.left.send(aliased, plain.data(), record.data() + 1, 163), protocol::Drop::FIELD);
    EXPECT_EQ(record, before);
    EXPECT_EQ(pair.left.domains_[1].next_tx, 2);
    std::memcpy(record.data() + protocol::HEADER_BYTES, plain.data(), plain.size());
    ASSERT_EQ(pair.left.send(aliased, record.data() + protocol::HEADER_BYTES, record.data(), 163), protocol::Drop::NONE);
    EXPECT_EQ(parsed(record).counter, 2);
    protocol::Header output{};
    ASSERT_EQ(pair.right.receive(protocol::Transport::ROCE, record.data(), 163, output), protocol::Drop::NONE);
    EXPECT_TRUE(std::equal(plain.begin(), plain.end(), record.begin() + protocol::HEADER_BYTES));
}

TEST(WkiRdmaSession, MaximumRecordAndEmptyTerminalNeedNoTransferScratch) {
    Pair pair;
    auto record = packet(pair.left, header(protocol::Transport::ROCE, protocol::Service::BLOCK, false, protocol::FRAGMENT_BYTES));
    protocol::Header output{};
    ASSERT_EQ(pair.right.receive(protocol::Transport::ROCE, record.data(), record.size(), output), protocol::Drop::NONE);
    EXPECT_EQ(output.payload_length, protocol::FRAGMENT_BYTES);
    auto input = header();
    input.opcode = static_cast<uint8_t>(protocol::Opcode::READ_END);
    input.flags = protocol::FLAG_RESPONSE;
    input.payload_length = input.total_length = 0;
    record.resize(protocol::HEADER_BYTES);
    ASSERT_EQ(pair.left.send(input, nullptr, record.data(), record.size()), protocol::Drop::NONE);
    ASSERT_EQ(pair.right.receive(protocol::Transport::ROCE, record.data(), record.size(), output), protocol::Drop::NONE);
    EXPECT_EQ(output.total_length, 0);
}

TEST(WkiRdmaSession, TryOnceContentionReturnsWhileOwnerRemainsBlocked) {
    Pair pair;
    auto record = packet(pair.left, header());
    auto valid = record;
    auto plain = payload();
    session::Statistics stats{};
    stats.incarnation = 0xa5a5;
    std::latch ready(1), release(1);
    std::thread owner([&] {
        EXPECT_FALSE(pair.right.guard_.test_and_set(std::memory_order_acquire));
        ready.count_down();
        release.wait();
        pair.right.guard_.clear(std::memory_order_release);
    });
    ready.wait();
    EXPECT_EQ(pair.right.begin(11, 7, 2, pair.remote), protocol::Drop::PRESSURE);
    EXPECT_EQ(pair.right.install(pair.local, pair.backward, pair.forward), protocol::Drop::PRESSURE);
    EXPECT_FALSE(pair.right.available(protocol::Transport::ROCE, protocol::Service::VFS));
    EXPECT_EQ(pair.right.send(header(protocol::Transport::ROCE, protocol::Service::VFS, true), plain.data(), record.data(), record.size()),
              protocol::Drop::PRESSURE);
    EXPECT_EQ(record, valid);
    EXPECT_EQ(pair.right.retire(), protocol::Drop::PRESSURE);
    EXPECT_EQ(pair.right.statistics(stats), protocol::Drop::PRESSURE);
    EXPECT_EQ(stats.incarnation, 0xa5a5);
    rejected(pair.right, valid, protocol::Drop::PRESSURE);
    EXPECT_TRUE(pair.right.begun_);
    EXPECT_TRUE(pair.right.domains_[1].active);
    release.count_down();
    owner.join();
    protocol::Header output{};
    ASSERT_EQ(pair.right.receive(protocol::Transport::ROCE, valid.data(), valid.size(), output), protocol::Drop::NONE);
    EXPECT_EQ(output.counter, 1);  // pressure did not admit replay or reset state
    ASSERT_EQ(pair.right.retire(), protocol::Drop::NONE);
    erased(pair.right);
}

TEST(WkiRdmaSession, ExplicitRetirementAndQuiescentDestructorEraseAllSecretState) {
    Pair pair;
    auto record = packet(pair.left, header());
    protocol::Header output{};
    ASSERT_EQ(pair.right.receive(protocol::Transport::ROCE, record.data(), record.size(), output), protocol::Drop::NONE);
    ASSERT_EQ(pair.right.retire(), protocol::Drop::NONE);
    erased(pair.right);
    EXPECT_EQ(pair.right.last_incarnation_, 1);
    ASSERT_EQ(pair.right.retire(), protocol::Drop::NONE);
    session::Statistics stats{};
    ASSERT_EQ(pair.right.statistics(stats), protocol::Drop::NONE);
    EXPECT_FALSE(stats.begun);
    EXPECT_EQ(stats.live_keys, 0);
    EXPECT_EQ(stats.replay_windows, 0);
    alignas(session::Session) std::array<uint8_t, sizeof(session::Session)> storage{};
    auto* value = new (storage.data()) session::Session;
    auto ninth_offer = offer(3, 9);
    ASSERT_EQ(value->begin(7, 11, 9, ninth_offer), protocol::Drop::NONE);
    ASSERT_EQ(value->install(pair.remote, pair.forward, pair.backward), protocol::Drop::NONE);
    size_t domain_offset = reinterpret_cast<uint8_t*>(&value->domains_) - storage.data();
    size_t offer_offset = reinterpret_cast<uint8_t*>(&value->local_offer_) - storage.data();
    size_t negotiation_offset = reinterpret_cast<uint8_t*>(&value->negotiation_) - storage.data();
    value->~Session();
    EXPECT_TRUE(std::all_of(storage.begin() + domain_offset, storage.begin() + domain_offset + sizeof(value->domains_),
                            [](uint8_t byte) { return byte == 0; }));
    EXPECT_TRUE(std::all_of(storage.begin() + offer_offset, storage.begin() + offer_offset + 2 * sizeof(protocol::Offer),
                            [](uint8_t byte) { return byte == 0; }));
    EXPECT_TRUE(std::all_of(storage.begin() + negotiation_offset,
                            storage.begin() + negotiation_offset + sizeof(protocol::KeyContext::negotiation),
                            [](uint8_t byte) { return byte == 0; }));
}

TEST(WkiRdmaSession, OfferIncarnationPrefixPreventsKeyReuseWhenCountersRestart) {
    Pair pair;
    auto original_offer = pair.local;
    auto original_key = pair.left.domains_[1].tx.key;
    auto original_prefix = pair.left.domains_[1].tx.nonce_prefix;
    EXPECT_EQ(parsed(packet(pair.left, header())).counter, 1);
    ASSERT_EQ(pair.left.retire(), protocol::Drop::NONE);
    auto erased_before = bytes(pair.left);
    EXPECT_EQ(pair.left.begin(7, 11, 2, original_offer), protocol::Drop::FIELD);
    EXPECT_EQ(bytes(pair.left), erased_before);
    for (size_t index = 0; index < 8; ++index) {
        auto mismatch = offer(3, 2);
        mismatch.nonce[index] ^= 1;
        EXPECT_EQ(pair.left.begin(7, 11, 2, mismatch), protocol::Drop::FIELD);
        EXPECT_EQ(bytes(pair.left), erased_before);
    }
    // Change only the LE incarnation prefix; retaining entropy/control keys
    // and the peer offer still gives a different ordered HKDF transcript.
    auto next_offer = offer(3, 2);
    ASSERT_EQ(pair.left.begin(7, 11, 2, next_offer), protocol::Drop::NONE);
    ASSERT_EQ(pair.left.install(pair.remote, pair.forward, pair.backward), protocol::Drop::NONE);
    EXPECT_NE(pair.left.domains_[1].tx.key, original_key);
    EXPECT_NE(pair.left.domains_[1].tx.nonce_prefix, original_prefix);
    EXPECT_EQ(parsed(packet(pair.left, header())).counter, 1);
}
