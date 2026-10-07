#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <net/wki/rdma_crypto.hpp>
#include <net/wki/rdma_protocol.hpp>
#include <net/wki/rdma_registry.hpp>
#include <net/wki/rdma_session.hpp>
#include <new>
#include <test/ktest.hpp>

namespace crypto = ker::net::wki::rdma_crypto;
namespace protocol = ker::net::wki::rdma_protocol;
namespace registry = ker::net::wki::rdma_registry;
namespace session = ker::net::wki::rdma_session;
namespace auth_crypto = ker::net::wki::auth_crypto;

namespace {

constexpr auto nibble(char byte) -> uint8_t { return static_cast<uint8_t>(byte >= '0' && byte <= '9' ? byte - '0' : byte - 'a' + 10); }

template <size_t Size>
constexpr auto from_hex(const char (&text)[Size * 2 + 1]) -> std::array<uint8_t, Size> {
    std::array<uint8_t, Size> output{};
    for (size_t index = 0; index < Size; ++index) {
        output[index] = static_cast<uint8_t>((nibble(text[index * 2]) << 4U) | nibble(text[index * 2 + 1]));
    }
    return output;
}

template <typename T>
struct Wiped {
    T value{};
    ~Wiped() { auth_crypto::secure_erase(&value, sizeof(value)); }
};

template <size_t Size>
auto all_zero(const std::array<uint8_t, Size>& bytes) -> bool {
    return std::all_of(bytes.begin(), bytes.end(), [](uint8_t byte) { return byte == 0; });
}

auto offer(uint64_t incarnation, uint8_t entropy) -> protocol::Offer {
    protocol::Offer output{};
    output.transports = protocol::TRANSPORTS;
    output.services = protocol::SERVICES;
    output.record_bytes = protocol::RECORD_BYTES;
    output.session_id.fill(0x35);
    output.nonce.fill(entropy);
    for (size_t index = 0; index < sizeof(incarnation); ++index) output.nonce[index] = static_cast<uint8_t>(incarnation >> (index * 8U));
    return output;
}

auto send_header(protocol::Transport transport, protocol::Service service, bool reverse = false) -> protocol::Header {
    protocol::Header output{};
    output.transport = static_cast<uint8_t>(transport);
    output.service = static_cast<uint8_t>(service);
    output.opcode = static_cast<uint8_t>(protocol::Opcode::WRITE);
    output.src_node = reverse ? 11 : 7;
    output.dst_node = reverse ? 7 : 11;
    output.channel_id = 42;
    output.session_id.fill(0x35);
    output.region_generation = 13;
    output.binding_generation = 17;
    output.operation_id = 23;
    output.receive_ticket = 29;
    output.capability.fill(0x77);
    output.rkey = 31;
    output.total_length = output.payload_length = 19;
    return output;
}

struct ProtocolFixture {
    crypto::Key control{};
    protocol::KeyMaterial material{};
    std::array<protocol::KeyMaterial, session::DOMAIN_COUNT> separated{};
    std::array<uint8_t, protocol::RECORD_BYTES> record{};
    std::array<uint8_t, protocol::FRAGMENT_BYTES> plaintext{};
    std::array<uint8_t, protocol::FRAGMENT_BYTES> output{};
    protocol::ReplayWindow replay{};
    protocol::Coverage coverage{};

    ~ProtocolFixture() {
        auth_crypto::secure_erase(control.data(), control.size());
        auth_crypto::secure_erase(&material, sizeof(material));
        auth_crypto::secure_erase(separated.data(), sizeof(separated));
        auth_crypto::secure_erase(record.data(), record.size());
        auth_crypto::secure_erase(plaintext.data(), plaintext.size());
        auth_crypto::secure_erase(output.data(), output.size());
    }
};

struct SessionFixture {
    session::Session left;
    session::Session right;
    crypto::Key forward{};
    crypto::Key backward{};
    std::array<uint8_t, protocol::FRAGMENT_BYTES> plaintext{};
    std::array<uint8_t, protocol::RECORD_BYTES> record{};
    std::array<uint8_t, protocol::RECORD_BYTES> duplicate{};
    protocol::Offer local = offer(1, 3);
    protocol::Offer remote = offer(5, 7);

    SessionFixture() {
        for (size_t index = 0; index < forward.size(); ++index) {
            forward[index] = static_cast<uint8_t>(index + 11);
            backward[index] = static_cast<uint8_t>(index + 93);
        }
        for (size_t index = 0; index < plaintext.size(); ++index) plaintext[index] = static_cast<uint8_t>(index * 17 + 5);
    }

    ~SessionFixture() {
        KEXPECT_EQ(left.retire(), protocol::Drop::NONE);
        KEXPECT_EQ(right.retire(), protocol::Drop::NONE);
        auth_crypto::secure_erase(forward.data(), forward.size());
        auth_crypto::secure_erase(backward.data(), backward.size());
        auth_crypto::secure_erase(plaintext.data(), plaintext.size());
        auth_crypto::secure_erase(record.data(), record.size());
        auth_crypto::secure_erase(duplicate.data(), duplicate.size());
    }
};

struct RegistryFixture {
    registry::Registry registry;
    crypto::Key root{};
    std::array<uint8_t, 4096> backing{};
    std::array<registry::Lease, 2> leases{};
    std::array<protocol::RegionDescriptor, 2> descriptors{};
    bool ready = false;

    RegistryFixture() { root.fill(0x5b); }

    ~RegistryFixture() {
        // Every KREQUIRE early return drops pins before exact allocation
        // reclamation. These fixtures have no external or asynchronous users.
        for (auto& lease : leases) lease.reset();
        for (auto& descriptor : descriptors) {
            if (descriptor.rkey == 0) continue;
            registry::ReleasedRegion released{};
            KEXPECT_EQ(registry.reclaim(descriptor.rkey, descriptor.generation, released), protocol::Drop::NONE);
            KEXPECT_EQ(released.backing, static_cast<void*>(backing.data()));
            KEXPECT_EQ(released.size, static_cast<uint32_t>(backing.size()));
            descriptor = {};
        }
        if (ready) {
            registry::Statistics stats{};
            KEXPECT_EQ(registry.statistics(stats), protocol::Drop::NONE);
            KEXPECT_EQ(stats.live, uint32_t{0});
            KEXPECT_EQ(stats.closing, uint32_t{0});
            KEXPECT_EQ(stats.references, uint32_t{0});
            KEXPECT_EQ(stats.claims, uint32_t{0});
            KEXPECT_EQ(stats.bytes, uint64_t{0});
            KEXPECT_EQ(registry.shutdown(), protocol::Drop::NONE);
        }
        auth_crypto::secure_erase(root.data(), root.size());
        auth_crypto::secure_erase(backing.data(), backing.size());
    }
};

auto binding() -> registry::Binding {
    registry::Binding output{};
    output.owner_node = 11;
    output.peer_node = 7;
    output.session_id.fill(0x35);
    output.secure_incarnation = 1;
    output.transport_cookie = 0x1000;
    output.transport_generation = 9;
    output.channel_cookie = 0x2000;
    output.channel_generation = 3;
    output.channel_id = 42;
    output.transport = protocol::Transport::ROCE;
    output.service = protocol::Service::VFS;
    return output;
}

auto registration(RegistryFixture& fixture) -> registry::Registration {
    return {.binding = binding(),
            .backing = fixture.backing.data(),
            .size = static_cast<uint32_t>(fixture.backing.size()),
            .binding_generation = 17,
            .permissions = protocol::PERMIT_WRITE};
}

auto incoming(const protocol::RegionDescriptor& descriptor) -> protocol::Header {
    auto output = send_header(protocol::Transport::ROCE, protocol::Service::VFS);
    output.counter = 1;
    output.rkey = descriptor.rkey;
    output.region_generation = descriptor.generation;
    output.binding_generation = descriptor.binding_generation;
    output.capability = descriptor.capability;
    return output;
}

}  // namespace

KTEST(WkiSecureRdma, Rfc8439KnownAnswerAndTamperPreserveOutput) {
    // Public RFC8439 section2.8.2, independent of our encoder/decoder.
    Wiped<crypto::Key> key{from_hex<32>("808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f")};
    constexpr auto NONCE = from_hex<12>("070000004041424344454647");
    constexpr auto AAD = from_hex<12>("50515253c0c1c2c3c4c5c6c7");
    constexpr char PLAIN[] =
        "Ladies and Gentlemen of the class of '99: If I could offer you only one tip for the future, sunscreen would be it.";
    constexpr auto CIPHER = from_hex<114>(
        "d31a8d34648e60db7b86afbc53ef7ec2a4aded51296e08fea9e2b5a736ee62d6"
        "3dbea45e8ca9671282fafb69da92728b1a71de0a9e060b2905d6a5b67ecd3b36"
        "92ddbd7f2d778b8c9803aee328091b58fab324e4fad675945585808b4831d7bc"
        "3ff4def08e4b7a9de576d26586cec64b6116");
    constexpr auto EXPECTED_TAG = from_hex<16>("1ae10b594f09e26a7e902ecbd0600691");
    Wiped<std::array<uint8_t, CIPHER.size()>> output;
    crypto::Tag tag{};
    KREQUIRE_TRUE(crypto::seal(key.value, NONCE, AAD.data(), AAD.size(), PLAIN, sizeof(PLAIN) - 1, output.value.data(), tag));
    KEXPECT_TRUE(output.value == CIPHER);
    KEXPECT_TRUE(tag == EXPECTED_TAG);
    KREQUIRE_TRUE(crypto::open(key.value, NONCE, AAD.data(), AAD.size(), CIPHER.data(), CIPHER.size(), output.value.data(), EXPECTED_TAG));
    KEXPECT_EQ(std::memcmp(output.value.data(), PLAIN, output.value.size()), 0);
    for (size_t index = 0; index < tag.size(); ++index) {
        auto wrong = EXPECTED_TAG;
        wrong[index] ^= 1;
        output.value.fill(0xa5);
        KEXPECT_FALSE(crypto::open(key.value, NONCE, AAD.data(), AAD.size(), CIPHER.data(), CIPHER.size(), output.value.data(), wrong));
        KEXPECT_TRUE(std::all_of(output.value.begin(), output.value.end(), [](uint8_t byte) { return byte == 0xa5; }));
    }
}

KTEST(WkiSecureRdma, IndependentRecordVectorDomainsAndTamper) {
    std::unique_ptr<ProtocolFixture> fixture(new (std::nothrow) ProtocolFixture{});
    KREQUIRE_TRUE(fixture != nullptr);
    KEXPECT_EQ(protocol::VERSION, uint16_t{1});
    KEXPECT_EQ(protocol::HEADER_BYTES, size_t{144});
    KEXPECT_EQ(protocol::FRAGMENT_BYTES, uint32_t{8192});
    // Independent Python hashlib/HKDF/ChaCha20Poly1305 vector retained in
    // record-model-02; this is a complete header, tag and ciphertext answer.
    protocol::KeyContext context{.session_id = from_hex<16>("101112131415161718191a1b1c1d1e1f"),
                                 .negotiation = from_hex<32>("893fe12d721fc9d022a975cf5d34a060f3884090d8349e325af1f46b01a72f33"),
                                 .src_node = 7,
                                 .dst_node = 11,
                                 .transport = protocol::Transport::ROCE,
                                 .service = protocol::Service::VFS};
    for (size_t index = 0; index < fixture->control.size(); ++index) fixture->control[index] = static_cast<uint8_t>(index);
    KREQUIRE_TRUE(protocol::derive_key(fixture->control, context, fixture->material));
    KEXPECT_TRUE(fixture->material.key == from_hex<32>("bf38e5cb7cc26b382fdb60424871867cc7daa876f430705493cbdf0d8c7319eb"));
    KEXPECT_TRUE(fixture->material.nonce_prefix == from_hex<4>("5bcefd06"));
    for (size_t index = 0; index < fixture->separated.size(); ++index) {
        auto domain = context;
        domain.transport = static_cast<protocol::Transport>(index / 4 + 1);
        domain.service = static_cast<protocol::Service>(index % 4 + 1);
        KREQUIRE_TRUE(protocol::derive_key(fixture->control, domain, fixture->separated[index]));
        for (size_t previous = 0; previous < index; ++previous)
            KEXPECT_FALSE(fixture->separated[index].key == fixture->separated[previous].key);
    }
    auto input = send_header(context.transport, context.service);
    input.session_id = context.session_id;
    input.region_generation = 77;
    input.binding_generation = 88;
    input.operation_id = 99;
    input.receive_ticket = 111;
    input.capability = from_hex<16>("4eaeb2ef647dbee8acdbc6b37b9d147d");
    input.rkey = 37;
    input.region_offset = 13;
    input.counter = 123;
    for (size_t index = 0; index < input.payload_length; ++index) fixture->plaintext[index] = static_cast<uint8_t>(index + 1);
    constexpr auto EXPECTED = from_hex<163>(
        "575352440101020207000b002a000000101112131415161718191a1b1c1d1e1f"
        "4d000000000000004eaeb2ef647dbee8acdbc6b37b9d147d0d00000000000000"
        "630000000000000058000000000000006f000000000000007b00000000000000"
        "0000000013000000130000002500000000000000000000000000000000000000"
        "4dbc594d7a81946ff83fcb436a5ccd374e09469afa2fa5dffcbf146a9aa585ea08ca59");
    KREQUIRE_TRUE(protocol::encode(fixture->material, input, fixture->plaintext.data(), fixture->record.data(), fixture->record.size()));
    KEXPECT_EQ(std::memcmp(fixture->record.data(), EXPECTED.data(), EXPECTED.size()), 0);
    protocol::Header output{};
    KREQUIRE_EQ(protocol::decode(fixture->material, context, fixture->record.data(), EXPECTED.size(), output, fixture->output.data(),
                                 fixture->output.size()),
                protocol::Drop::NONE);
    KEXPECT_EQ(std::memcmp(fixture->output.data(), fixture->plaintext.data(), input.payload_length), 0);
    for (size_t index = 0; index < EXPECTED.size(); ++index) {
        fixture->record[index] ^= 1;
        fixture->output.fill(0xa5);
        auto before = output;
        KEXPECT_NE(protocol::decode(fixture->material, context, fixture->record.data(), EXPECTED.size(), output, fixture->output.data(),
                                    fixture->output.size()),
                   protocol::Drop::NONE);
        KEXPECT_EQ(std::memcmp(&output, &before, sizeof(output)), 0);
        KEXPECT_TRUE(std::all_of(fixture->output.begin(), fixture->output.end(), [](uint8_t byte) { return byte == 0xa5; }));
        fixture->record[index] ^= 1;
    }
    auto wrong_domain = context;
    wrong_domain.transport = protocol::Transport::IVSHMEM;
    KEXPECT_EQ(protocol::decode(fixture->material, wrong_domain, fixture->record.data(), EXPECTED.size(), output, fixture->output.data(),
                                fixture->output.size()),
               protocol::Drop::DOMAIN);
}

KTEST(WkiSecureRdma, ReplayReservationAndExactCoverageBounds) {
    std::unique_ptr<ProtocolFixture> fixture(new (std::nothrow) ProtocolFixture{});
    KREQUIRE_TRUE(fixture != nullptr);
    KEXPECT_EQ(protocol::REPLAY_BITS, uint32_t{8192});
    KEXPECT_EQ(protocol::MAX_TRANSFER_BYTES, uint32_t{4 * 1024 * 1024});
    uint64_t next = UINT64_MAX - 1;
    KEXPECT_EQ(protocol::reserve(next), UINT64_MAX - 1);
    KEXPECT_EQ(protocol::reserve(next), uint64_t{0});
    KEXPECT_EQ(protocol::reserve(next), uint64_t{0});
    KEXPECT_EQ(next, uint64_t{0});
    auto& replay = fixture->replay;
    KEXPECT_EQ(protocol::replay_admit(replay, 1), protocol::Drop::NONE);
    KEXPECT_EQ(protocol::replay_admit(replay, 66), protocol::Drop::NONE);
    KEXPECT_EQ(replay.bits[1], uint64_t{2});
    KEXPECT_EQ(protocol::replay_admit(replay, 65), protocol::Drop::NONE);
    KEXPECT_EQ(protocol::replay_admit(replay, 65), protocol::Drop::DUPLICATE);
    KEXPECT_EQ(protocol::replay_admit(replay, 8194), protocol::Drop::NONE);
    KEXPECT_EQ(protocol::replay_admit(replay, 2), protocol::Drop::TOO_OLD);
    KEXPECT_EQ(protocol::replay_admit(replay, 3), protocol::Drop::NONE);
    KEXPECT_EQ(protocol::replay_admit(replay, UINT64_MAX), protocol::Drop::NONE);
    KEXPECT_EQ(protocol::replay_admit(replay, UINT64_MAX - 1), protocol::Drop::NONE);
    KEXPECT_EQ(protocol::replay_admit(replay, UINT64_MAX), protocol::Drop::DUPLICATE);
    KEXPECT_EQ(protocol::replay_admit(replay, 0), protocol::Drop::COUNTER);
    KEXPECT_EQ(replay.high, UINT64_MAX);
    KEXPECT_EQ(replay.bits[0], uint64_t{3});
    auto& coverage = fixture->coverage;
    KREQUIRE_TRUE(protocol::coverage_begin(coverage, protocol::MAX_TRANSFER_BYTES));
    KEXPECT_EQ(protocol::coverage_admit(coverage, protocol::MAX_TRANSFER_BYTES - protocol::FRAGMENT_BYTES, protocol::FRAGMENT_BYTES),
               protocol::Drop::NONE);
    KEXPECT_EQ(protocol::coverage_admit(coverage, protocol::MAX_TRANSFER_BYTES - protocol::FRAGMENT_BYTES, protocol::FRAGMENT_BYTES),
               protocol::Drop::DUPLICATE);
    KEXPECT_FALSE(protocol::coverage_complete(coverage));
    for (size_t index = 0; index < protocol::MAX_FRAGMENTS - 1; ++index) {
        KREQUIRE_EQ(protocol::coverage_admit(coverage, static_cast<uint32_t>(index * protocol::FRAGMENT_BYTES), protocol::FRAGMENT_BYTES),
                    protocol::Drop::NONE);
    }
    KEXPECT_TRUE(protocol::coverage_complete(coverage));
    KEXPECT_FALSE(protocol::coverage_begin(coverage, protocol::MAX_TRANSFER_BYTES + 1));
    KEXPECT_TRUE(protocol::coverage_complete(coverage));
    KREQUIRE_TRUE(protocol::coverage_begin(coverage, protocol::FRAGMENT_BYTES + 19));
    KEXPECT_EQ(protocol::coverage_admit(coverage, protocol::FRAGMENT_BYTES, 18), protocol::Drop::FRAGMENT);
    KEXPECT_EQ(protocol::coverage_admit(coverage, protocol::FRAGMENT_BYTES, 19), protocol::Drop::NONE);
    KEXPECT_FALSE(protocol::coverage_complete(coverage));
    KEXPECT_EQ(protocol::coverage_admit(coverage, 0, protocol::FRAGMENT_BYTES), protocol::Drop::NONE);
    KEXPECT_TRUE(protocol::coverage_complete(coverage));
    KEXPECT_TRUE(protocol::coverage_begin(coverage, 0));
    KEXPECT_TRUE(protocol::coverage_complete(coverage));
    KEXPECT_EQ(protocol::coverage_admit(coverage, 0, 0), protocol::Drop::FRAGMENT);
}

KTEST(WkiSecureRdma, SessionPairsAllDomainsWithoutReplayReset) {
    std::unique_ptr<SessionFixture> fixture(new (std::nothrow) SessionFixture{});
    KREQUIRE_TRUE(fixture != nullptr);
    KEXPECT_EQ(session::DOMAIN_COUNT, size_t{8});
    KREQUIRE_EQ(fixture->left.begin(7, 11, 1, fixture->local), protocol::Drop::NONE);
    KREQUIRE_EQ(fixture->right.begin(11, 7, 5, fixture->remote), protocol::Drop::NONE);
    KREQUIRE_EQ(fixture->left.install(fixture->remote, fixture->forward, fixture->backward), protocol::Drop::NONE);
    KREQUIRE_EQ(fixture->right.install(fixture->local, fixture->backward, fixture->forward), protocol::Drop::NONE);
    session::Statistics statistics{};
    KREQUIRE_EQ(fixture->left.statistics(statistics), protocol::Drop::NONE);
    KEXPECT_EQ(statistics.live_keys, uint32_t{16});
    KEXPECT_EQ(statistics.replay_windows, uint32_t{8});
    for (size_t domain = 0; domain < session::DOMAIN_COUNT; ++domain) {
        auto transport = static_cast<protocol::Transport>(domain / 4 + 1);
        auto service = static_cast<protocol::Service>(domain % 4 + 1);
        KREQUIRE_TRUE(fixture->left.available(transport, service));
        KREQUIRE_TRUE(fixture->right.available(transport, service));
        auto input = send_header(transport, service);
        size_t const SIZE = protocol::HEADER_BYTES + input.payload_length;
        KREQUIRE_EQ(fixture->left.send(input, fixture->plaintext.data(), fixture->record.data(), fixture->record.size()),
                    protocol::Drop::NONE);
        std::memcpy(fixture->duplicate.data(), fixture->record.data(), SIZE);
        protocol::Header output{};
        KREQUIRE_EQ(fixture->right.receive(transport, fixture->record.data(), SIZE, output), protocol::Drop::NONE);
        KEXPECT_EQ(output.counter, uint64_t{1});
        KEXPECT_EQ(std::memcmp(fixture->record.data() + protocol::HEADER_BYTES, fixture->plaintext.data(), input.payload_length), 0);
        KEXPECT_EQ(fixture->right.install(fixture->local, fixture->backward, fixture->forward), protocol::Drop::NONE);
        auto before = output;
        KEXPECT_EQ(fixture->right.receive(transport, fixture->duplicate.data(), SIZE, output), protocol::Drop::DUPLICATE);
        KEXPECT_EQ(std::memcmp(&before, &output, sizeof(output)), 0);
        KEXPECT_TRUE(std::all_of(fixture->duplicate.begin(), fixture->duplicate.begin() + SIZE, [](uint8_t byte) { return byte == 0; }));
        KEXPECT_EQ(fixture->left.install(fixture->remote, fixture->forward, fixture->backward), protocol::Drop::NONE);
        KREQUIRE_EQ(fixture->left.send(input, fixture->plaintext.data(), fixture->record.data(), fixture->record.size()),
                    protocol::Drop::NONE);
        KREQUIRE_EQ(fixture->right.receive(transport, fixture->record.data(), SIZE, output), protocol::Drop::NONE);
        KEXPECT_EQ(output.counter, uint64_t{2});
        input = send_header(transport, service, true);
        KREQUIRE_EQ(fixture->right.send(input, fixture->plaintext.data(), fixture->record.data(), fixture->record.size()),
                    protocol::Drop::NONE);
        KREQUIRE_EQ(fixture->left.receive(transport, fixture->record.data(), SIZE, output), protocol::Drop::NONE);
        KEXPECT_EQ(output.counter, uint64_t{1});
    }
    auto full = send_header(protocol::Transport::IVSHMEM, protocol::Service::BLOCK);
    full.total_length = full.payload_length = protocol::FRAGMENT_BYTES;
    KREQUIRE_EQ(fixture->left.send(full, fixture->plaintext.data(), fixture->record.data(), fixture->record.size()), protocol::Drop::NONE);
    protocol::Header output{};
    KREQUIRE_EQ(fixture->right.receive(protocol::Transport::IVSHMEM, fixture->record.data(), fixture->record.size(), output),
                protocol::Drop::NONE);
    KEXPECT_EQ(std::memcmp(fixture->record.data() + protocol::HEADER_BYTES, fixture->plaintext.data(), fixture->plaintext.size()), 0);
}

KTEST(WkiSecureRdma, SessionRetirementRequiresNewIncarnationTranscript) {
    std::unique_ptr<SessionFixture> fixture(new (std::nothrow) SessionFixture{});
    KREQUIRE_TRUE(fixture != nullptr);
    KREQUIRE_EQ(fixture->left.begin(7, 11, 1, fixture->local), protocol::Drop::NONE);
    KREQUIRE_EQ(fixture->left.install(fixture->remote, fixture->forward, fixture->backward), protocol::Drop::NONE);
    auto input = send_header(protocol::Transport::ROCE, protocol::Service::VFS);
    KREQUIRE_EQ(fixture->left.send(input, fixture->plaintext.data(), fixture->record.data(), fixture->record.size()), protocol::Drop::NONE);
    std::memcpy(fixture->duplicate.data(), fixture->record.data(), fixture->record.size());
    auto changed = fixture->remote;
    changed.nonce[8] ^= 1;
    KEXPECT_EQ(fixture->left.install(changed, fixture->forward, fixture->backward), protocol::Drop::RETIRED);
    session::Statistics statistics{};
    KREQUIRE_EQ(fixture->left.statistics(statistics), protocol::Drop::NONE);
    KEXPECT_FALSE(statistics.begun);
    KEXPECT_FALSE(statistics.negotiated);
    KEXPECT_EQ(statistics.live_keys, uint32_t{0});
    KEXPECT_EQ(statistics.replay_windows, uint32_t{0});
    KEXPECT_EQ(statistics.incarnation, uint64_t{0});
    KEXPECT_EQ(fixture->left.begin(7, 11, 1, fixture->local), protocol::Drop::RETIRED);
    KEXPECT_EQ(fixture->left.begin(7, 11, 2, fixture->local), protocol::Drop::FIELD);
    auto successor = offer(2, 3);
    KREQUIRE_EQ(fixture->left.begin(7, 11, 2, successor), protocol::Drop::NONE);
    KREQUIRE_EQ(fixture->left.install(fixture->remote, fixture->forward, fixture->backward), protocol::Drop::NONE);
    KREQUIRE_EQ(fixture->left.send(input, fixture->plaintext.data(), fixture->record.data(), fixture->record.size()), protocol::Drop::NONE);
    KEXPECT_NE(std::memcmp(fixture->record.data(), fixture->duplicate.data(), protocol::HEADER_BYTES + input.payload_length), 0);
    protocol::Header assigned{};
    std::memcpy(&assigned, fixture->record.data(), sizeof(assigned));
    KEXPECT_EQ(assigned.counter, uint64_t{1});
    KREQUIRE_EQ(fixture->left.retire(), protocol::Drop::NONE);
    KREQUIRE_EQ(fixture->left.statistics(statistics), protocol::Drop::NONE);
    KEXPECT_EQ(statistics.live_keys, uint32_t{0});
    KEXPECT_EQ(statistics.replay_windows, uint32_t{0});
}

KTEST(WkiSecureRdma, RegistryRejectsWrongBindingGrantRangeAndPermission) {
    std::unique_ptr<RegistryFixture> fixture(new (std::nothrow) RegistryFixture{});
    KREQUIRE_TRUE(fixture != nullptr);
    KEXPECT_EQ(registry::REGION_SLOTS, size_t{256});
    KEXPECT_EQ(registry::REGISTERED_BYTES, uint64_t{64 * 1024 * 1024});
    KREQUIRE_EQ(fixture->registry.initialize(11, fixture->root), protocol::Drop::NONE);
    fixture->ready = true;
    KREQUIRE_EQ(fixture->registry.register_region(registration(*fixture), fixture->descriptors[0]), protocol::Drop::NONE);
    auto input = incoming(fixture->descriptors[0]);
    auto exact = binding();
    for (size_t field = 0; field < 10; ++field) {
        auto wrong = exact;
        switch (field) {
            case 0:
                wrong.owner_node = 12;
                break;
            case 1:
                wrong.peer_node = 8;
                break;
            case 2:
                wrong.session_id[0] ^= 1;
                break;
            case 3:
                wrong.secure_incarnation = 2;
                break;
            case 4:
                wrong.transport_cookie = 0x3000;
                break;
            case 5:
                wrong.transport_generation = 10;
                break;
            case 6:
                wrong.channel_cookie = 0x4000;
                break;
            case 7:
                wrong.channel_generation = 4;
                break;
            case 8:
                wrong.transport = protocol::Transport::IVSHMEM;
                break;
            case 9:
                wrong.service = protocol::Service::IPC;
                break;
        }
        KEXPECT_EQ(fixture->registry.admit(wrong, input, fixture->leases[0]), protocol::Drop::IDENTITY);
        KEXPECT_FALSE(fixture->leases[0]);
    }
    auto wrong_grant = input;
    wrong_grant.capability[0] ^= 1;
    KEXPECT_EQ(fixture->registry.admit(exact, wrong_grant, fixture->leases[0]), protocol::Drop::GRANT);
    wrong_grant = input;
    wrong_grant.binding_generation += 1;
    KEXPECT_EQ(fixture->registry.admit(exact, wrong_grant, fixture->leases[0]), protocol::Drop::GRANT);
    wrong_grant = input;
    wrong_grant.region_offset = fixture->backing.size() - input.total_length + 1;
    KEXPECT_EQ(fixture->registry.admit(exact, wrong_grant, fixture->leases[0]), protocol::Drop::RANGE);
    auto read = input;
    read.opcode = static_cast<uint8_t>(protocol::Opcode::READ);
    read.payload_length = 0;
    KEXPECT_EQ(fixture->registry.admit(exact, read, fixture->leases[0]), protocol::Drop::PERMISSION);
    KEXPECT_TRUE(all_zero(fixture->backing));
    registry::Statistics stats{};
    KREQUIRE_EQ(fixture->registry.statistics(stats), protocol::Drop::NONE);
    KEXPECT_EQ(stats.references, uint32_t{0});
    KEXPECT_EQ(stats.claims, uint32_t{0});
}

KTEST(WkiSecureRdma, RegistryClaimCloseAndDrainHaveExactOwnership) {
    std::unique_ptr<RegistryFixture> fixture(new (std::nothrow) RegistryFixture{});
    KREQUIRE_TRUE(fixture != nullptr);
    KREQUIRE_EQ(fixture->registry.initialize(11, fixture->root), protocol::Drop::NONE);
    fixture->ready = true;
    KREQUIRE_EQ(fixture->registry.register_region(registration(*fixture), fixture->descriptors[0]), protocol::Drop::NONE);
    auto& descriptor = fixture->descriptors[0];
    auto input = incoming(descriptor);
    KREQUIRE_EQ(fixture->registry.admit(binding(), input, fixture->leases[0]), protocol::Drop::NONE);
    KREQUIRE_EQ(fixture->registry.admit(binding(), input, fixture->leases[1]), protocol::Drop::NONE);
    KEXPECT_NULL(fixture->leases[0].memory().backing);
    KREQUIRE_EQ(fixture->leases[0].claim(), protocol::Drop::NONE);
    KEXPECT_EQ(fixture->leases[0].memory().backing, static_cast<void*>(fixture->backing.data()));
    KREQUIRE_EQ(fixture->registry.revoke(descriptor.rkey, descriptor.generation), protocol::Drop::NONE);
    KEXPECT_EQ(fixture->leases[1].claim(), protocol::Drop::RETIRED);
    KEXPECT_NULL(fixture->leases[1].memory().backing);
    KEXPECT_EQ(fixture->leases[0].claim(), protocol::Drop::NONE);
    registry::ReleasedRegion released{.backing = fixture->root.data(), .size = 7};
    KEXPECT_EQ(fixture->registry.reclaim(descriptor.rkey, descriptor.generation, released), protocol::Drop::PRESSURE);
    KEXPECT_EQ(released.backing, static_cast<void*>(fixture->root.data()));
    KEXPECT_EQ(released.size, uint32_t{7});
    registry::Statistics stats{};
    KREQUIRE_EQ(fixture->registry.statistics(stats), protocol::Drop::NONE);
    KEXPECT_EQ(stats.live, uint32_t{1});
    KEXPECT_EQ(stats.closing, uint32_t{1});
    KEXPECT_EQ(stats.references, uint32_t{2});
    KEXPECT_EQ(stats.claims, uint32_t{1});
    // A pre-close claim retains backing until release; no new claim can start.
    static_cast<uint8_t*>(fixture->leases[0].memory().backing)[0] = 0x5a;
    fixture->leases[0].reset();
    KEXPECT_EQ(fixture->registry.reclaim(descriptor.rkey, descriptor.generation, released), protocol::Drop::PRESSURE);
    fixture->leases[1].reset();
    KREQUIRE_EQ(fixture->registry.reclaim(descriptor.rkey, descriptor.generation, released), protocol::Drop::NONE);
    KEXPECT_EQ(released.backing, static_cast<void*>(fixture->backing.data()));
    KEXPECT_EQ(released.size, static_cast<uint32_t>(fixture->backing.size()));
    KEXPECT_EQ(fixture->backing[0], uint8_t{0x5a});
    descriptor = {};
}

KTEST(WkiSecureRdma, RegistrySlotReuseCannotReviveOldRkeyOrCapability) {
    std::unique_ptr<RegistryFixture> fixture(new (std::nothrow) RegistryFixture{});
    KREQUIRE_TRUE(fixture != nullptr);
    KREQUIRE_EQ(fixture->registry.initialize(11, fixture->root), protocol::Drop::NONE);
    fixture->ready = true;
    KREQUIRE_EQ(fixture->registry.register_region(registration(*fixture), fixture->descriptors[0]), protocol::Drop::NONE);
    auto old = fixture->descriptors[0];
    registry::ReleasedRegion released{};
    KREQUIRE_EQ(fixture->registry.close_transport(binding().transport_cookie, binding().transport_generation), protocol::Drop::NONE);
    KREQUIRE_EQ(fixture->registry.reclaim(old.rkey, old.generation, released), protocol::Drop::NONE);
    fixture->descriptors[0] = {};
    KREQUIRE_EQ(fixture->registry.register_region(registration(*fixture), fixture->descriptors[0]), protocol::Drop::NONE);
    auto current = fixture->descriptors[0];
    KEXPECT_NE(current.rkey, old.rkey);
    KEXPECT_NE(current.generation, old.generation);
    KEXPECT_FALSE(current.capability == old.capability);
    auto stale = incoming(old);
    KEXPECT_EQ(fixture->registry.admit(binding(), stale, fixture->leases[0]), protocol::Drop::GRANT);
    stale.rkey = current.rkey;
    KEXPECT_EQ(fixture->registry.admit(binding(), stale, fixture->leases[0]), protocol::Drop::GRANT);
    stale.region_generation = current.generation;
    KEXPECT_EQ(fixture->registry.admit(binding(), stale, fixture->leases[0]), protocol::Drop::GRANT);
    KREQUIRE_EQ(fixture->registry.admit(binding(), incoming(current), fixture->leases[0]), protocol::Drop::NONE);
    KREQUIRE_EQ(fixture->registry.close_peer(binding().peer_node, binding().session_id), protocol::Drop::NONE);
    KEXPECT_EQ(fixture->leases[0].claim(), protocol::Drop::RETIRED);
    KEXPECT_NULL(fixture->leases[0].memory().backing);
    KEXPECT_EQ(fixture->registry.shutdown(), protocol::Drop::PRESSURE);
    fixture->leases[0].reset();
    KREQUIRE_EQ(fixture->registry.reclaim(current.rkey, current.generation, released), protocol::Drop::NONE);
    fixture->descriptors[0] = {};
    KEXPECT_EQ(fixture->registry.shutdown(), protocol::Drop::NONE);
    KEXPECT_EQ(fixture->registry.initialize(11, fixture->root), protocol::Drop::RETIRED);
}
