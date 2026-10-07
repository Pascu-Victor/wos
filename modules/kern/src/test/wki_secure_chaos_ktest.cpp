#include <array>
#include <cstring>
#include <memory>
#include <net/wki/chaos.hpp>
#include <net/wki/rdma_protocol.hpp>
#include <net/wki/wki.hpp>
#include <test/ktest.hpp>

namespace {
using namespace ker::net::wki;
using Record = std::array<uint8_t, rdma_protocol::HEADER_BYTES + 4>;
struct Observation {
    uint32_t secure_calls = 0;
    uint32_t ordinary_calls = 0;
    uint32_t tx_calls = 0;
    bool has_direct_peer = false;
    bool has_src_mac = false;
    uint16_t direct_peer = 0;
    uint16_t size = 0;
    Record record{};
};
Observation s_observation;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
void observe_secure(WkiTransport*, const void* record, uint16_t size, const WkiRxMetadata* metadata) {
    ++s_observation.secure_calls;
    s_observation.size = size;
    s_observation.has_direct_peer = metadata != nullptr && metadata->has_direct_peer;
    s_observation.has_src_mac = metadata != nullptr && metadata->has_src_mac;
    s_observation.direct_peer = s_observation.has_direct_peer ? metadata->direct_peer : 0;
    if (record != nullptr && size == s_observation.record.size()) std::memcpy(s_observation.record.data(), record, size);
}
void observe_ordinary(WkiTransport*, const void*, uint16_t, const WkiRxMetadata*) { ++s_observation.ordinary_calls; }
auto observe_tx(WkiTransport*, uint16_t, const void*, uint16_t) -> int {
    ++s_observation.tx_calls;
    return WKI_OK;
}
struct Fixture {
    WkiTransport transport{};
    Fixture() {
        s_observation = {};
        transport.name = "secure-chaos-ktest";
        transport.secure_kind = rdma_protocol::Transport::IVSHMEM;
        transport.secure_tx = observe_tx;
        wki_chaos_allow_runtime_control(true);
        static_cast<void>(configure("clear"));
        wki_chaos_transport_register(&transport);
        wki_chaos_selftest_set_rx_delivery_hook(observe_ordinary);
        wki_chaos_selftest_set_secure_rx_delivery_hook(observe_secure);
    }
    ~Fixture() {
        wki_chaos_selftest_set_secure_rx_delivery_hook(nullptr);
        wki_chaos_selftest_set_rx_delivery_hook(nullptr);
        wki_chaos_transport_unregister(&transport);
        static_cast<void>(configure("clear"));
        wki_chaos_allow_runtime_control(false);
    }
    auto configure(const char* command) -> bool { return wki_chaos_configure(command, std::strlen(command)) == WKI_OK; }
};
auto record() -> Record {
    Record result{};
    rdma_protocol::Header header{};
    header.transport = static_cast<uint8_t>(rdma_protocol::Transport::IVSHMEM);
    header.service = static_cast<uint8_t>(rdma_protocol::Service::ZONE);
    header.opcode = static_cast<uint8_t>(rdma_protocol::Opcode::WRITE);
    header.src_node = 250;
    header.dst_node = 249;
    header.channel_id = 1;
    header.counter = 33;
    header.payload_length = 4;
    std::memcpy(result.data(), &header, sizeof(header));
    result[rdma_protocol::HEADER_BYTES] = 0xAC;
    return result;
}
}  // namespace

KTEST(WkiSecureChaos, DelayedIngressRetainsPrivateRecordAndProvisionedPeer) {
    Fixture fixture;
    KREQUIRE_TRUE(fixture.configure("enable seed=25"));
    KREQUIRE_TRUE(fixture.configure("rule id=1 dir=rx action=delay"));
    auto input = record();
    auto const expected = input;
    WkiRxMetadata metadata{.has_direct_peer = true, .direct_peer = 250};
    wki_chaos_secure_rx_ingress(&fixture.transport, input.data(), input.size(), &metadata);
    KEXPECT_EQ(s_observation.secure_calls, 0U);
    input.fill(0xDD);
    metadata.direct_peer = 3;
    KREQUIRE_TRUE(fixture.configure("release id=1"));
    wki_chaos_drain_ready();
    KEXPECT_EQ(s_observation.secure_calls, 1U);
    KEXPECT_EQ(s_observation.ordinary_calls, 0U);
    KEXPECT_TRUE(s_observation.has_direct_peer);
    KEXPECT_FALSE(s_observation.has_src_mac);
    KEXPECT_EQ(s_observation.direct_peer, 250U);
    KEXPECT_EQ(std::memcmp(s_observation.record.data(), expected.data(), expected.size()), 0);
    KEXPECT_EQ(fixture.transport.chaos_in_flight.load(), 0U);
}

KTEST(WkiSecureChaos, CiphertextXorUsesSecureCallbackWithoutOrdinaryDispatch) {
    Fixture fixture;
    KREQUIRE_TRUE(fixture.configure("enable seed=25"));
    KREQUIRE_TRUE(fixture.configure("rule id=2 dir=rx action=corrupt corrupt=payload payload_offset=0 payload_xor=128"));
    auto const input = record();
    WkiRxMetadata metadata{.has_direct_peer = true, .direct_peer = 250};
    wki_chaos_secure_rx_ingress(&fixture.transport, input.data(), input.size(), &metadata);
    wki_chaos_drain_ready();
    KEXPECT_EQ(s_observation.secure_calls, 1U);
    KEXPECT_EQ(s_observation.ordinary_calls, 0U);
    KEXPECT_EQ(std::memcmp(s_observation.record.data(), input.data(), rdma_protocol::HEADER_BYTES), 0);
    KEXPECT_EQ(s_observation.record[rdma_protocol::HEADER_BYTES], input[rdma_protocol::HEADER_BYTES] ^ 128U);
    KEXPECT_EQ(fixture.transport.chaos_in_flight.load(), 0U);
}

KTEST(WkiSecureChaos, DuplicateIngressKeepsTypedFamilyAndReleasesEveryLease) {
    Fixture fixture;
    KREQUIRE_TRUE(fixture.configure("enable seed=25"));
    KREQUIRE_TRUE(fixture.configure("rule id=3 dir=rx action=duplicate"));
    auto const input = record();
    WkiRxMetadata metadata{.has_direct_peer = true, .direct_peer = 250};
    wki_chaos_secure_rx_ingress(&fixture.transport, input.data(), input.size(), &metadata);
    KEXPECT_EQ(s_observation.secure_calls, 1U);
    wki_chaos_drain_ready();
    KEXPECT_EQ(s_observation.secure_calls, 2U);
    KEXPECT_EQ(s_observation.ordinary_calls, 0U);
    KEXPECT_EQ(fixture.transport.chaos_in_flight.load(), 0U);
}

KTEST(WkiSecureChaos, ProviderCloseDiscardsDelayedCiphertextWithoutCallback) {
    Fixture fixture;
    KREQUIRE_TRUE(fixture.configure("enable seed=25"));
    KREQUIRE_TRUE(fixture.configure("rule id=4 dir=rx action=delay"));
    auto const input = record();
    WkiRxMetadata metadata{.has_direct_peer = true, .direct_peer = 250};
    for (size_t i = 0; i < WKI_CHAOS_MAX_QUEUED_FRAMES; ++i)
        wki_chaos_secure_rx_ingress(&fixture.transport, input.data(), input.size(), &metadata);
    WkiChaosSnapshot before{};
    KREQUIRE_TRUE(wki_chaos_snapshot(&before));
    KEXPECT_EQ(before.queued_frame_count, WKI_CHAOS_MAX_QUEUED_FRAMES);
    wki_chaos_transport_close(&fixture.transport);
    wki_chaos_drain_ready();
    WkiChaosSnapshot after{};
    KREQUIRE_TRUE(wki_chaos_snapshot(&after));
    KEXPECT_EQ(after.queued_frame_count, 0U);
    KEXPECT_EQ(s_observation.secure_calls, 0U);
    KEXPECT_EQ(s_observation.ordinary_calls, 0U);
    KEXPECT_EQ(fixture.transport.chaos_in_flight.load(), 0U);
}

KTEST(WkiSecureChaos, DisabledTypedTransmitAndInjectedDropHaveDistinctEffects) {
    Fixture fixture;
    auto const input = record();
    KEXPECT_EQ(wki_transport_send_secure(&fixture.transport, 250, input.data(), input.size()), WKI_OK);
    KEXPECT_EQ(s_observation.tx_calls, 1U);
    KREQUIRE_TRUE(fixture.configure("enable seed=25"));
    KREQUIRE_TRUE(fixture.configure("rule id=5 dir=tx action=drop type=130 op=1"));
    KEXPECT_EQ(wki_transport_send_secure(&fixture.transport, 250, input.data(), input.size()), WKI_OK);
    KEXPECT_EQ(s_observation.tx_calls, 1U);
    KEXPECT_EQ(fixture.transport.chaos_in_flight.load(), 0U);
}
