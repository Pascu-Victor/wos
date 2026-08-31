#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/packet.hpp>
#include <net/wki/blk_ring.hpp>
#include <net/wki/chaos.hpp>
#include <net/wki/chaos_model.hpp>
#include <net/wki/wki.hpp>
#include <test/ktest.hpp>

namespace {

using namespace ker::net::wki;
using TestFrame = std::array<uint8_t, WKI_HEADER_SIZE + 1>;
constexpr uint16_t BLOCK_BODY_SIZE = sizeof(uint64_t) + sizeof(uint32_t);
constexpr uint16_t BLOCK_LBA_PAYLOAD_OFFSET = sizeof(DevOpReqPayload);
constexpr uint16_t BLOCK_COUNT_PAYLOAD_OFFSET = BLOCK_LBA_PAYLOAD_OFFSET + sizeof(uint64_t);
using BlockFrame = std::array<uint8_t, WKI_HEADER_SIZE + sizeof(DevOpReqPayload) + BLOCK_BODY_SIZE>;

WkiChaosModel s_model;         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
WkiChaosModel s_second_model;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

struct RuntimeFixture {
    WkiTransport transport{};
    uint32_t copy_tx_calls = 0;
    uint32_t packet_tx_calls = 0;
    uint16_t last_copy_len = 0;
    std::array<uint8_t, WKI_CHAOS_MAX_FRAME_SIZE> last_copy_frame{};
    WkiRxHandler rx_handler = nullptr;

    RuntimeFixture();
    ~RuntimeFixture();

    RuntimeFixture(const RuntimeFixture&) = delete;
    auto operator=(const RuntimeFixture&) -> RuntimeFixture& = delete;

    auto configure(const char* command) -> bool { return wki_chaos_configure(command, std::strlen(command)) == WKI_OK; }
};

auto runtime_copy_tx(WkiTransport* transport, uint16_t /*neighbor*/, const void* data, uint16_t len) -> int {
    auto* fixture = static_cast<RuntimeFixture*>(transport->private_data);
    ++fixture->copy_tx_calls;
    fixture->last_copy_len = len;
    if (data != nullptr && len <= fixture->last_copy_frame.size()) {
        std::memcpy(fixture->last_copy_frame.data(), data, len);
    }
    return WKI_OK;
}

auto runtime_packet_tx(WkiTransport* transport, uint16_t /*neighbor*/, ker::net::PacketBuffer* packet) -> int {
    auto* fixture = static_cast<RuntimeFixture*>(transport->private_data);
    ++fixture->packet_tx_calls;
    ker::net::pkt_free(packet);
    return WKI_OK;
}

void runtime_set_rx_handler(WkiTransport* transport, WkiRxHandler handler) {
    auto* fixture = static_cast<RuntimeFixture*>(transport->private_data);
    fixture->rx_handler = handler;
}

RuntimeFixture::RuntimeFixture() {
    transport.name = "wki-chaos-ktest";
    transport.mtu = WKI_ETH_MAX_PAYLOAD;
    transport.private_data = this;
    transport.tx = runtime_copy_tx;
    transport.tx_pkt = runtime_packet_tx;
    transport.set_rx_handler = runtime_set_rx_handler;
    wki_chaos_allow_runtime_control(true);
    static_cast<void>(configure("clear"));
    wki_chaos_transport_register(&transport);
}

RuntimeFixture::~RuntimeFixture() {
    wki_chaos_selftest_set_rx_delivery_hook(nullptr);
    wki_chaos_selftest_set_block_doorbell_delivery_hook(nullptr);
    wki_chaos_transport_unregister(&transport);
    static_cast<void>(configure("clear"));
    wki_chaos_allow_runtime_control(false);
}

void packet_release_count(void* context) {
    auto* count = static_cast<uint32_t*>(context);
    ++*count;
}

auto make_counted_packet(const TestFrame& frame, uint32_t* release_count) -> ker::net::PacketBuffer* {
    auto* packet = ker::net::pkt_alloc();
    if (packet == nullptr) {
        return nullptr;
    }
    std::memcpy(packet->data, frame.data(), frame.size());
    packet->len = frame.size();
    packet->lifetime_ctx = release_count;
    packet->lifetime_release = packet_release_count;
    return packet;
}

struct RxObservation {
    uint32_t calls = 0;
    bool has_src_mac = false;
    ker::net::proto::MacAddress src_mac{};
    WkiHeader header{};
};

RxObservation s_rx_observation;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

struct BlockDoorbellObservation {
    uint32_t calls = 0;
    bool accept = true;
    WkiChaosBlockKey expected{};
    WkiChaosBlockKey delivered{};
};

BlockDoorbellObservation s_block_observation;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto observe_block_doorbell(const WkiChaosBlockKey& key) -> bool {
    ++s_block_observation.calls;
    s_block_observation.delivered = key;
    return s_block_observation.accept && wki_chaos_block_identity_equal(key, s_block_observation.expected);
}

void observe_rx(WkiTransport* /*transport*/, const void* data, uint16_t len, const WkiRxMetadata* metadata) {
    ++s_rx_observation.calls;
    s_rx_observation.has_src_mac = metadata != nullptr && metadata->has_src_mac;
    if (s_rx_observation.has_src_mac) {
        s_rx_observation.src_mac = metadata->src_mac;
    }
    if (data != nullptr && len >= WKI_HEADER_SIZE) {
        std::memcpy(&s_rx_observation.header, data, sizeof(s_rx_observation.header));
    }
}

auto make_frame(uint32_t sequence, uint32_t checksum = 0, uint16_t channel = WKI_CHAN_RESOURCE) -> TestFrame {
    TestFrame frame{};
    WkiHeader header{};
    header.version_flags = wki_version_flags(WKI_VERSION, 0);
    header.msg_type = static_cast<uint8_t>(MsgType::DEV_OP_REQ);
    header.src_node = 1;
    header.dst_node = 2;
    header.channel_id = channel;
    header.seq_num = sequence;
    header.payload_len = 1;
    header.hop_ttl = WKI_DEFAULT_TTL;
    header.checksum = checksum;
    std::memcpy(frame.data(), &header, sizeof(header));
    frame.at(WKI_HEADER_SIZE) = 0x5A;
    return frame;
}

auto make_block_frame(uint16_t op_id, uint64_t lba, uint32_t count, uint32_t sequence = 1) -> BlockFrame {
    BlockFrame frame{};
    WkiHeader header{};
    header.version_flags = wki_version_flags(WKI_VERSION, 0);
    header.msg_type = static_cast<uint8_t>(MsgType::DEV_OP_REQ);
    header.src_node = 1;
    header.dst_node = 2;
    header.channel_id = WKI_CHAN_RESOURCE;
    header.seq_num = sequence;
    header.payload_len = static_cast<uint16_t>(frame.size() - WKI_HEADER_SIZE);
    header.hop_ttl = WKI_DEFAULT_TTL;
    std::memcpy(frame.data(), &header, sizeof(header));
    DevOpReqPayload request{.op_id = op_id, .data_len = BLOCK_BODY_SIZE};
    std::memcpy(frame.data() + WKI_HEADER_SIZE, &request, sizeof(request));
    std::memcpy(frame.data() + WKI_HEADER_SIZE + BLOCK_LBA_PAYLOAD_OFFSET, &lba, sizeof(lba));
    std::memcpy(frame.data() + WKI_HEADER_SIZE + BLOCK_COUNT_PAYLOAD_OFFSET, &count, sizeof(count));
    return frame;
}

auto frame_checksum(const uint8_t* data, uint16_t len) -> uint32_t {
    if (data == nullptr || len < WKI_HEADER_SIZE) {
        return 0;
    }
    WkiHeader header{};
    std::memcpy(&header, data, sizeof(header));
    if (static_cast<size_t>(WKI_HEADER_SIZE) + header.payload_len > len) {
        return 0;
    }
    header.checksum = 0;
    uint32_t crc = wki_crc32(&header, sizeof(header));
    return header.payload_len == 0 ? crc : wki_crc32_continue(crc, data + WKI_HEADER_SIZE, header.payload_len);
}

auto reset_model(uint64_t seed = 1) -> bool {
    if (!s_model.clear()) {
        return false;
    }
    s_model.enable(seed);
    return true;
}

auto rule(uint32_t id, WkiChaosAction action, WkiChaosDirection direction = WkiChaosDirection::TX) -> WkiChaosRule {
    WkiChaosRule value{};
    value.id = id;
    value.active = true;
    value.direction = direction;
    value.action = action;
    value.every = 1;
    value.chance_permyriad = 10'000;
    return value;
}

auto make_block_key(WkiChaosSurface surface, uint16_t transport_id) -> WkiChaosBlockKey {
    return WkiChaosBlockKey{
        .surface = surface,
        .direction = WkiChaosDirection::TX,
        .origin = WkiChaosBlockOrigin::PROXY,
        .lane = WkiChaosBlockLane::ROCE,
        .transport_id = transport_id,
        .neighbor = 2,
        .zone_id = 0x1234,
        .resource_id = 7,
        .ring_generation = 0xABCDEF01,
        .ring_index = 5,
        .operation_cookie = 0x10203040,
        .block_opcode = static_cast<uint8_t>(BlkOpcode::READ),
        .attach_cookie = 9,
        .channel_generation = 10,
        .owner_boot_epoch = 11,
        .resource_incarnation = 12,
    };
}

}  // namespace

KTEST(WkiChaos, DisabledPathIsIdentity) {
    KREQUIRE_TRUE(s_model.clear());
    auto frame = make_frame(1);
    auto const result = s_model.intercept(WkiChaosDirection::TX, 1, 1, 2, frame.data(), frame.size());
    KEXPECT_EQ(result.action, WkiChaosAction::PASS);
    KEXPECT_FALSE(result.matched);
    KEXPECT_EQ(s_model.counters().observed, 0U);
    KEXPECT_EQ(s_model.trace_count(), 0U);
}

KTEST(WkiChaos, OccurrenceCountsAcrossConsecutiveSequenceNumbers) {
    KREQUIRE_TRUE(reset_model());
    auto occurrence_rule = rule(5, WkiChaosAction::DROP);
    occurrence_rule.match_occurrence = true;
    occurrence_rule.occurrence = 1;
    KREQUIRE_TRUE(s_model.upsert_rule(occurrence_rule));
    auto first = make_frame(10);
    auto second = make_frame(11);

    KEXPECT_FALSE(s_model.intercept(WkiChaosDirection::TX, 1, 1, 2, first.data(), first.size()).matched);
    auto const result = s_model.intercept(WkiChaosDirection::TX, 1, 1, 2, second.data(), second.size());
    KEXPECT_TRUE(result.matched);
    KEXPECT_EQ(result.action, WkiChaosAction::DROP);

    std::array<WkiChaosTraceRow, 1> rows{};
    KREQUIRE_EQ(s_model.trace_snapshot(rows.data(), rows.size(), 0), 1U);
    KEXPECT_EQ(rows.at(0).key.seq_num, 11U);
    KEXPECT_EQ(rows.at(0).key.stream_occurrence, 1U);
}

KTEST(WkiChaos, SeededDecisionIsStableAcrossIndependentStreamInterleaving) {
    KREQUIRE_TRUE(reset_model(99));
    KREQUIRE_TRUE(s_second_model.clear());
    s_second_model.enable(99);
    auto chance_rule = rule(7, WkiChaosAction::FAIL);
    chance_rule.chance_permyriad = 5'000;
    KREQUIRE_TRUE(s_model.upsert_rule(chance_rule));
    KREQUIRE_TRUE(s_second_model.upsert_rule(chance_rule));
    auto frame_a = make_frame(20, 0, WKI_CHAN_RESOURCE);
    auto frame_b = make_frame(21, 0, static_cast<uint16_t>(WKI_CHAN_RESOURCE + 1));
    std::array<bool, 8> first_a{};
    std::array<bool, 8> first_b{};
    std::array<bool, 8> second_a{};
    std::array<bool, 8> second_b{};
    for (size_t i = 0; i < first_a.size(); ++i) {
        first_a.at(i) = s_model.intercept(WkiChaosDirection::TX, 1, 1, 2, frame_a.data(), frame_a.size()).matched;
        first_b.at(i) = s_model.intercept(WkiChaosDirection::TX, 1, 1, 2, frame_b.data(), frame_b.size()).matched;
    }
    for (size_t i = 0; i < second_b.size(); ++i) {
        second_b.at(i) = s_second_model.intercept(WkiChaosDirection::TX, 1, 1, 2, frame_b.data(), frame_b.size()).matched;
    }
    for (size_t i = 0; i < second_a.size(); ++i) {
        second_a.at(i) = s_second_model.intercept(WkiChaosDirection::TX, 1, 1, 2, frame_a.data(), frame_a.size()).matched;
    }
    for (size_t i = 0; i < first_a.size(); ++i) {
        KEXPECT_EQ(first_a.at(i), second_a.at(i));
        KEXPECT_EQ(first_b.at(i), second_b.at(i));
    }
}

KTEST(WkiChaos, DelayAndReorderHaveBoundedExplicitRelease) {
    KREQUIRE_TRUE(reset_model());
    KREQUIRE_TRUE(s_model.upsert_rule(rule(1, WkiChaosAction::REORDER)));
    auto first = make_frame(10);
    auto second = make_frame(11);
    static_cast<void>(s_model.intercept(WkiChaosDirection::TX, 1, 1, 2, first.data(), first.size()));
    static_cast<void>(s_model.intercept(WkiChaosDirection::TX, 1, 1, 2, second.data(), second.size()));
    KEXPECT_EQ(s_model.queued_frame_count(), 2U);
    KEXPECT_EQ(s_model.release(1, 0), 2U);

    WkiChaosDeliveryView delivery{};
    KREQUIRE_TRUE(s_model.claim_ready(&delivery));
    WkiHeader header{};
    std::memcpy(&header, delivery.data, sizeof(header));
    KEXPECT_EQ(header.seq_num, 11U);
    s_model.finish_delivery(delivery.slot);
    KREQUIRE_TRUE(s_model.claim_ready(&delivery));
    std::memcpy(&header, delivery.data, sizeof(header));
    KEXPECT_EQ(header.seq_num, 10U);
    s_model.finish_delivery(delivery.slot);
    KEXPECT_EQ(s_model.queued_frame_count(), 0U);
}

KTEST(WkiChaos, RuleReplacementRejectsHeldFramePartitionAndBlockRows) {
    KREQUIRE_TRUE(reset_model());
    KREQUIRE_TRUE(s_model.upsert_rule(rule(8, WkiChaosAction::DELAY)));
    auto frame = make_frame(12);
    KREQUIRE_EQ(s_model.intercept(WkiChaosDirection::TX, 1, 1, 2, frame.data(), frame.size()).outcome, WkiChaosOutcome::QUEUED);
    KEXPECT_FALSE(s_model.upsert_rule(rule(8, WkiChaosAction::DROP)));
    KREQUIRE_EQ(s_model.release(8, 1), 1U);
    WkiChaosDeliveryView delivery{};
    KREQUIRE_TRUE(s_model.claim_ready(&delivery));
    s_model.finish_delivery(delivery.slot);
    KEXPECT_TRUE(s_model.upsert_rule(rule(8, WkiChaosAction::DROP)));

    KREQUIRE_TRUE(reset_model());
    KREQUIRE_TRUE(s_model.upsert_rule(rule(9, WkiChaosAction::PARTITION)));
    KREQUIRE_EQ(s_model.intercept(WkiChaosDirection::TX, 1, 1, 2, frame.data(), frame.size()).outcome, WkiChaosOutcome::DROPPED);
    KEXPECT_FALSE(s_model.upsert_rule(rule(9, WkiChaosAction::PASS)));
    KREQUIRE_TRUE(s_model.heal_partition(9));
    KEXPECT_TRUE(s_model.upsert_rule(rule(9, WkiChaosAction::PASS)));

    KREQUIRE_TRUE(reset_model());
    auto block_delay = rule(10, WkiChaosAction::DELAY);
    block_delay.surface = WkiChaosSurface::BLOCK_DOORBELL;
    KREQUIRE_TRUE(s_model.upsert_rule(block_delay));
    KREQUIRE_EQ(s_model.intercept_block_doorbell(make_block_key(WkiChaosSurface::BLOCK_DOORBELL, 1)).outcome, WkiChaosOutcome::QUEUED);
    auto block_drop = rule(10, WkiChaosAction::DROP);
    block_drop.surface = WkiChaosSurface::BLOCK_DOORBELL;
    KEXPECT_FALSE(s_model.upsert_rule(block_drop));
    KREQUIRE_EQ(s_model.release(10, 1), 1U);
    KREQUIRE_TRUE(s_model.claim_ready(&delivery));
    s_model.finish_delivery(delivery.slot);
    KEXPECT_TRUE(s_model.upsert_rule(block_drop));
}

KTEST(WkiChaos, CorruptionForcesValidationFailureWithoutMutatingInput) {
    KREQUIRE_TRUE(reset_model());
    KREQUIRE_TRUE(s_model.upsert_rule(rule(2, WkiChaosAction::CORRUPT, WkiChaosDirection::RX)));
    auto frame = make_frame(5, 0);
    static_cast<void>(s_model.intercept(WkiChaosDirection::RX, 1, 1, 2, frame.data(), frame.size()));

    WkiChaosDeliveryView delivery{};
    KREQUIRE_TRUE(s_model.claim_ready(&delivery));
    WkiHeader corrupted{};
    std::memcpy(&corrupted, delivery.data, sizeof(corrupted));
    WkiHeader original{};
    std::memcpy(&original, frame.data(), sizeof(original));
    KEXPECT_NE(corrupted.checksum, 0U);
    KEXPECT_EQ(original.checksum, 0U);
    KEXPECT_EQ(delivery.data[WKI_HEADER_SIZE], frame.at(WKI_HEADER_SIZE));
    s_model.finish_delivery(delivery.slot);
}

KTEST(WkiChaos, RuntimeParserAndDeliveryMatchDevOpAndCorruptOneBlockPayloadByte) {
    RuntimeFixture fixture;
    KREQUIRE_TRUE(fixture.configure("enable seed=6"));
    KEXPECT_FALSE(fixture.configure("rule id=12 dir=tx action=drop corrupt=payload payload_offset=4 payload_xor=1"));
    KEXPECT_FALSE(fixture.configure("rule id=12 dir=tx action=corrupt corrupt=payload payload_offset=4"));
    KEXPECT_FALSE(fixture.configure("rule id=12 dir=tx action=corrupt corrupt=payload payload_offset=4 payload_xor=0"));
    KEXPECT_FALSE(fixture.configure("rule id=12 dir=tx action=corrupt corrupt=checksum payload_offset=4 payload_xor=1"));
    KREQUIRE_TRUE(fixture.configure("rule id=12 dir=tx action=corrupt corrupt=checksum"));

    struct CorruptionCase {
        const char* command;
        uint16_t offset;
        uint8_t xor_mask;
    };
    constexpr std::array CASES{
        CorruptionCase{"rule id=12 dir=tx action=corrupt op=0x0100 corrupt=payload payload_offset=4 payload_xor=0x80",
                       BLOCK_LBA_PAYLOAD_OFFSET, 0x80},
        CorruptionCase{"rule id=12 dir=tx action=corrupt op=0x0100 corrupt=payload payload_offset=12 payload_xor=0x04",
                       BLOCK_COUNT_PAYLOAD_OFFSET, 0x04},
    };

    uint32_t sequence = 600;
    for (auto const& test_case : CASES) {
        KREQUIRE_TRUE(fixture.configure("clear"));
        KREQUIRE_TRUE(fixture.configure("enable seed=6"));
        KREQUIRE_TRUE(fixture.configure(test_case.command));
        fixture.copy_tx_calls = 0;
        fixture.last_copy_len = 0;
        auto frame = make_block_frame(OP_BLOCK_READ, 0x1122334455667788ULL, 7, sequence++);
        auto const original = frame;

        KEXPECT_EQ(wki_transport_send(&fixture.transport, 2, frame.data(), static_cast<uint16_t>(frame.size())), WKI_OK);
        KEXPECT_EQ(fixture.copy_tx_calls, 0U);
        wki_chaos_drain_ready();
        KREQUIRE_EQ(fixture.copy_tx_calls, 1U);
        KREQUIRE_EQ(fixture.last_copy_len, frame.size());
        for (size_t offset = 0; offset < frame.size() - WKI_HEADER_SIZE; ++offset) {
            uint8_t const EXPECTED = offset == test_case.offset
                                         ? static_cast<uint8_t>(original.at(WKI_HEADER_SIZE + offset) ^ test_case.xor_mask)
                                         : original.at(WKI_HEADER_SIZE + offset);
            KEXPECT_EQ(fixture.last_copy_frame.at(WKI_HEADER_SIZE + offset), EXPECTED);
        }
        WkiHeader delivered_header{};
        std::memcpy(&delivered_header, fixture.last_copy_frame.data(), sizeof(delivered_header));
        KEXPECT_NE(delivered_header.checksum, 0U);
        KEXPECT_EQ(delivered_header.checksum, frame_checksum(fixture.last_copy_frame.data(), fixture.last_copy_len));
        KEXPECT_EQ(std::memcmp(frame.data(), original.data(), frame.size()), 0);
    }

    fixture.copy_tx_calls = 0;
    auto wrong_op = make_block_frame(OP_BLOCK_WRITE, 0x1122334455667788ULL, 7, sequence++);
    KEXPECT_EQ(wki_transport_send(&fixture.transport, 2, wrong_op.data(), static_cast<uint16_t>(wrong_op.size())), WKI_OK);
    KEXPECT_EQ(fixture.copy_tx_calls, 1U);

    fixture.copy_tx_calls = 0;
    auto malformed = make_block_frame(OP_BLOCK_READ, 0x1122334455667788ULL, 7, sequence++);
    DevOpReqPayload request{};
    std::memcpy(&request, malformed.data() + WKI_HEADER_SIZE, sizeof(request));
    --request.data_len;
    std::memcpy(malformed.data() + WKI_HEADER_SIZE, &request, sizeof(request));
    KEXPECT_EQ(wki_transport_send(&fixture.transport, 2, malformed.data(), static_cast<uint16_t>(malformed.size())), WKI_OK);
    KEXPECT_EQ(fixture.copy_tx_calls, 1U);
}

KTEST(WkiChaos, PartitionAndOverflowAreDeterministic) {
    KREQUIRE_TRUE(reset_model(77));
    KREQUIRE_TRUE(s_model.upsert_rule(rule(3, WkiChaosAction::PARTITION)));
    auto frame = make_frame(1);
    KEXPECT_EQ(s_model.intercept(WkiChaosDirection::TX, 1, 1, 2, frame.data(), frame.size()).action, WkiChaosAction::PARTITION);
    KEXPECT_TRUE(s_model.heal_partition(3));
    KEXPECT_EQ(s_model.intercept(WkiChaosDirection::TX, 1, 1, 2, frame.data(), frame.size()).action, WkiChaosAction::PASS);

    KREQUIRE_TRUE(s_model.upsert_rule(rule(4, WkiChaosAction::DELAY)));
    for (size_t i = 0; i < WKI_CHAOS_MAX_QUEUED_FRAMES; ++i) {
        auto queued = make_frame(static_cast<uint32_t>(i + 10));
        KEXPECT_EQ(s_model.intercept(WkiChaosDirection::TX, 1, 1, 2, queued.data(), queued.size()).outcome, WkiChaosOutcome::QUEUED);
    }
    auto overflow = make_frame(99);
    KEXPECT_EQ(s_model.intercept(WkiChaosDirection::TX, 1, 1, 2, overflow.data(), overflow.size()).outcome,
               WkiChaosOutcome::QUEUE_OVERFLOW);
    KEXPECT_TRUE(s_model.queue_overflow());
    KEXPECT_TRUE(s_model.invalid());
}

KTEST(WkiChaos, EveryQueuedActionUsesFailClosedOverflowPolicy) {
    constexpr std::array ACTIONS{
        WkiChaosAction::DUPLICATE,
        WkiChaosAction::DELAY,
        WkiChaosAction::REORDER,
        WkiChaosAction::CORRUPT,
    };
    for (WkiChaosAction const ACTION : ACTIONS) {
        KREQUIRE_TRUE(reset_model());
        KREQUIRE_TRUE(s_model.upsert_rule(rule(6, WkiChaosAction::DELAY, WkiChaosDirection::RX)));
        for (size_t i = 0; i < WKI_CHAOS_MAX_QUEUED_FRAMES; ++i) {
            auto frame = make_frame(static_cast<uint32_t>(i));
            KREQUIRE_EQ(s_model.intercept(WkiChaosDirection::RX, 1, 1, 2, frame.data(), frame.size()).outcome, WkiChaosOutcome::QUEUED);
        }
        KREQUIRE_TRUE(s_model.upsert_rule(rule(7, ACTION)));
        auto overflow = make_frame(99);
        auto const result = s_model.intercept(WkiChaosDirection::TX, 1, 1, 2, overflow.data(), overflow.size());
        KEXPECT_EQ(result.action, WkiChaosAction::DROP);
        KEXPECT_EQ(result.outcome, WkiChaosOutcome::QUEUE_OVERFLOW);
        KEXPECT_TRUE(s_model.queue_overflow());
        KEXPECT_TRUE(s_model.invalid());
        KREQUIRE_EQ(s_model.release(6, 0), WKI_CHAOS_MAX_QUEUED_FRAMES);
        WkiChaosDeliveryView delivery{};
        while (s_model.claim_ready(&delivery)) {
            s_model.finish_delivery(delivery.slot);
        }
    }
}

KTEST(WkiChaos, RuntimePacketOwnershipIsExactlyOnceForEveryAction) {
    ker::net::pkt_pool_init();
    RuntimeFixture fixture;
    struct ActionCase {
        const char* rule_command;
        const char* release_command;
        int expected_result;
        uint32_t expected_packet_tx;
        uint32_t expected_copy_tx;
    };
    constexpr std::array CASES{
        ActionCase{"rule id=50 dir=tx action=pass", nullptr, WKI_OK, 1, 0},
        ActionCase{"rule id=50 dir=tx action=drop", nullptr, WKI_OK, 0, 0},
        ActionCase{"rule id=50 dir=tx action=duplicate", nullptr, WKI_OK, 1, 1},
        ActionCase{"rule id=50 dir=tx action=delay", "release id=50", WKI_OK, 0, 1},
        ActionCase{"rule id=50 dir=tx action=reorder", "release id=50", WKI_OK, 0, 1},
        ActionCase{"rule id=50 dir=tx action=corrupt", nullptr, WKI_OK, 0, 1},
        ActionCase{"rule id=50 dir=tx action=fail", nullptr, WKI_ERR_TX_FAILED, 0, 0},
        ActionCase{"rule id=50 dir=tx action=partition", nullptr, WKI_OK, 0, 0},
    };

    uint32_t sequence = 100;
    for (auto const& test_case : CASES) {
        KREQUIRE_TRUE(fixture.configure("clear"));
        KREQUIRE_TRUE(fixture.configure("enable seed=1"));
        KREQUIRE_TRUE(fixture.configure(test_case.rule_command));
        fixture.copy_tx_calls = 0;
        fixture.packet_tx_calls = 0;
        uint32_t release_count = 0;
        auto frame = make_frame(sequence++);
        auto* packet = make_counted_packet(frame, &release_count);
        KREQUIRE_NE(packet, nullptr);

        int const RESULT = wki_transport_send_pkt(&fixture.transport, 2, packet);
        KEXPECT_EQ(RESULT, test_case.expected_result);
        KEXPECT_EQ(release_count, 1U);
        if (test_case.release_command != nullptr) {
            KREQUIRE_TRUE(fixture.configure(test_case.release_command));
        }
        wki_chaos_drain_ready();
        KEXPECT_EQ(fixture.packet_tx_calls, test_case.expected_packet_tx);
        KEXPECT_EQ(fixture.copy_tx_calls, test_case.expected_copy_tx);
        KEXPECT_EQ(release_count, 1U);
    }
}

KTEST(WkiChaos, RuntimeQueueOverflowFreesPacketOnceForEveryQueuedAction) {
    ker::net::pkt_pool_init();
    RuntimeFixture fixture;
    static_assert(WKI_CHAOS_MAX_QUEUED_FRAMES == 8);
    constexpr std::array ACTION_RULES{
        "rule id=71 dir=tx action=duplicate",
        "rule id=71 dir=tx action=delay",
        "rule id=71 dir=tx action=reorder",
        "rule id=71 dir=tx action=corrupt",
    };

    uint32_t sequence = 200;
    for (auto const* action_rule : ACTION_RULES) {
        KREQUIRE_TRUE(fixture.configure("clear"));
        KREQUIRE_TRUE(fixture.configure("enable seed=2"));
        KREQUIRE_TRUE(fixture.configure("rule id=70 dir=tx action=delay limit=8"));
        fixture.copy_tx_calls = 0;
        fixture.packet_tx_calls = 0;
        uint32_t release_count = 0;
        for (size_t i = 0; i < WKI_CHAOS_MAX_QUEUED_FRAMES; ++i) {
            auto frame = make_frame(sequence++);
            auto* packet = make_counted_packet(frame, &release_count);
            KREQUIRE_NE(packet, nullptr);
            KEXPECT_EQ(wki_transport_send_pkt(&fixture.transport, 2, packet), WKI_OK);
        }
        KEXPECT_EQ(release_count, WKI_CHAOS_MAX_QUEUED_FRAMES);
        KREQUIRE_TRUE(fixture.configure(action_rule));

        auto overflow_frame = make_frame(sequence++);
        auto* overflow_packet = make_counted_packet(overflow_frame, &release_count);
        KREQUIRE_NE(overflow_packet, nullptr);
        KEXPECT_EQ(wki_transport_send_pkt(&fixture.transport, 2, overflow_packet), WKI_OK);
        KEXPECT_EQ(release_count, WKI_CHAOS_MAX_QUEUED_FRAMES + 1);
        KEXPECT_EQ(fixture.packet_tx_calls, 0U);
        KEXPECT_EQ(fixture.copy_tx_calls, 0U);

        KREQUIRE_TRUE(fixture.configure("release id=70"));
        wki_chaos_drain_ready();
        KEXPECT_EQ(fixture.copy_tx_calls, WKI_CHAOS_MAX_QUEUED_FRAMES);
        KEXPECT_EQ(release_count, WKI_CHAOS_MAX_QUEUED_FRAMES + 1);
    }
}

KTEST(WkiChaos, QueuedRxPreservesIngressMetadataAndFrameStorage) {
    RuntimeFixture fixture;
    KREQUIRE_TRUE(fixture.configure("enable seed=3"));
    KREQUIRE_TRUE(fixture.configure("rule id=80 dir=rx action=delay"));
    s_rx_observation = {};
    wki_chaos_selftest_set_rx_delivery_hook(observe_rx);

    auto frame = make_frame(321);
    WkiRxMetadata metadata{
        .has_src_mac = true,
        .src_mac = ker::net::proto::MacAddress::from_bytes({0x02, 0x10, 0x20, 0x30, 0x40, 0x50}),
    };
    wki_chaos_rx_ingress(&fixture.transport, frame.data(), frame.size(), &metadata);
    KEXPECT_EQ(s_rx_observation.calls, 0U);

    frame.fill(0xCC);
    metadata.src_mac.fill(0xDD);
    KREQUIRE_TRUE(fixture.configure("release id=80"));
    wki_chaos_drain_ready();

    KEXPECT_EQ(s_rx_observation.calls, 1U);
    KEXPECT_TRUE(s_rx_observation.has_src_mac);
    KEXPECT_EQ(s_rx_observation.header.seq_num, 321U);
    constexpr std::array<uint8_t, ker::net::proto::MacAddress::SIZE_BYTES> EXPECTED_MAC{0x02, 0x10, 0x20, 0x30, 0x40, 0x50};
    for (size_t i = 0; i < EXPECTED_MAC.size(); ++i) {
        KEXPECT_EQ(s_rx_observation.src_mac.at(i), EXPECTED_MAC.at(i));
    }
}

KTEST(WkiChaos, RuntimeEpochTraceUsesDirectionalLockOrderSafeSnapshot) {
    RuntimeFixture fixture;
    KREQUIRE_TRUE(fixture.configure("enable seed=4"));
    KREQUIRE_TRUE(fixture.configure("rule id=90 dir=tx action=drop"));
    KREQUIRE_TRUE(fixture.configure("rule id=91 dir=rx action=drop"));
    wki_chaos_peer_epoch_update(1, 0xA1A2A3A4, 0xB1B2B3B4);
    wki_chaos_peer_epoch_update(2, 0x10203040, 0x50607080);
    auto frame = make_frame(400);

    KEXPECT_EQ(wki_transport_send(&fixture.transport, 2, frame.data(), frame.size()), WKI_OK);
    wki_chaos_rx_ingress(&fixture.transport, frame.data(), frame.size(), nullptr);
    std::array<WkiChaosTraceRow, 2> rows{};
    KREQUIRE_EQ(wki_chaos_trace_snapshot(rows.data(), rows.size()), 2U);
    KEXPECT_EQ(rows.at(0).peer_boot_epoch, 0x10203040U);
    KEXPECT_EQ(rows.at(0).peer_channel_epoch, 0x50607080U);
    KEXPECT_EQ(rows.at(1).peer_boot_epoch, 0xA1A2A3A4U);
    KEXPECT_EQ(rows.at(1).peer_channel_epoch, 0xB1B2B3B4U);
}

KTEST(WkiChaos, UnregisteringTransportRejectsNewQueueAndOwnsPacketOnce) {
    ker::net::pkt_pool_init();
    RuntimeFixture fixture;
    KREQUIRE_TRUE(fixture.configure("enable seed=5"));
    KREQUIRE_TRUE(fixture.configure("rule id=100 dir=tx action=delay"));
    fixture.transport.chaos_unregistering.store(true, std::memory_order_release);
    uint32_t release_count = 0;
    auto frame = make_frame(500);
    auto* packet = make_counted_packet(frame, &release_count);
    KREQUIRE_NE(packet, nullptr);

    KEXPECT_EQ(wki_transport_send_pkt(&fixture.transport, 2, packet), WKI_ERR_TX_FAILED);
    KEXPECT_EQ(release_count, 1U);
    KEXPECT_EQ(fixture.packet_tx_calls, 0U);
    KEXPECT_EQ(fixture.copy_tx_calls, 0U);
    WkiChaosSnapshot snapshot{};
    KREQUIRE_TRUE(wki_chaos_snapshot(&snapshot));
    KEXPECT_EQ(snapshot.queued_frame_count, 0U);
    std::array<WkiChaosTraceRow, 1> rows{};
    KREQUIRE_EQ(wki_chaos_trace_snapshot(rows.data(), rows.size()), 1U);
    KEXPECT_EQ(rows.at(0).outcome, WkiChaosOutcome::TRANSPORT_REMOVED);
}

KTEST(WkiChaos, RuntimeLimitedDelayCoalescesRetransmitsAndCaptureIsAtomic) {
    RuntimeFixture fixture;
    KREQUIRE_TRUE(fixture.configure("enable seed=7"));
    KREQUIRE_TRUE(fixture.configure("rule id=105 dir=tx action=delay limit=1"));
    auto frame = make_frame(550);

    KEXPECT_EQ(wki_transport_send(&fixture.transport, 2, frame.data(), frame.size()), WKI_OK);
    for (size_t retry = 0; retry < 20; ++retry) {
        KEXPECT_EQ(wki_transport_send(&fixture.transport, 2, frame.data(), frame.size()), WKI_OK);
    }
    KEXPECT_EQ(fixture.copy_tx_calls, 0U);

    WkiChaosSnapshot snapshot{};
    std::array<WkiChaosRuleRow, 2> rules{};
    std::array<WkiChaosTraceRow, 4> trace{};
    size_t rule_count = 0;
    size_t trace_count = 0;
    KREQUIRE_TRUE(wki_chaos_capture(&snapshot, rules.data(), rules.size(), &rule_count, trace.data(), trace.size(), &trace_count));
    KEXPECT_EQ(snapshot.active_rule_count, rule_count);
    KEXPECT_EQ(snapshot.trace_count, trace_count);
    KEXPECT_EQ(snapshot.queued_frame_count, 1U);
    KEXPECT_EQ(snapshot.counters.matched, 1U);
    KEXPECT_EQ(snapshot.counters.coalesced, 20U);
    KREQUIRE_EQ(rule_count, 1U);
    KEXPECT_EQ(rules.at(0).applied, 1U);
    KREQUIRE_EQ(trace_count, 1U);
    KEXPECT_EQ(trace.at(0).outcome, WkiChaosOutcome::QUEUED);

    KREQUIRE_TRUE(fixture.configure("release id=105 count=1"));
    wki_chaos_drain_ready();
    KEXPECT_EQ(fixture.copy_tx_calls, 1U);
    KREQUIRE_TRUE(wki_chaos_capture(&snapshot, rules.data(), rules.size(), &rule_count, trace.data(), trace.size(), &trace_count));
    KEXPECT_EQ(snapshot.queued_frame_count, 0U);
    KREQUIRE_EQ(trace_count, 2U);
    KEXPECT_EQ(trace.at(1).outcome, WkiChaosOutcome::RELEASED);
    KEXPECT_EQ(trace.at(1).coalesced_count, 20U);
    wki_chaos_drain_ready();
    KEXPECT_EQ(fixture.copy_tx_calls, 1U);
}

KTEST(WkiChaos, RuntimeLimitedPartitionRetainsRetransmitsUntilHeal) {
    RuntimeFixture fixture;
    KREQUIRE_TRUE(fixture.configure("enable seed=8"));
    KREQUIRE_TRUE(fixture.configure("rule id=106 dir=tx action=partition limit=1"));
    auto frame = make_frame(551);

    KEXPECT_EQ(wki_transport_send(&fixture.transport, 2, frame.data(), frame.size()), WKI_OK);
    for (size_t retry = 0; retry < 20; ++retry) {
        KEXPECT_EQ(wki_transport_send(&fixture.transport, 2, frame.data(), frame.size()), WKI_OK);
    }
    KEXPECT_EQ(fixture.copy_tx_calls, 0U);
    WkiChaosSnapshot snapshot{};
    KREQUIRE_TRUE(wki_chaos_snapshot(&snapshot));
    KEXPECT_EQ(snapshot.queued_frame_count, 1U);
    KEXPECT_EQ(snapshot.trace_count, 1U);
    KEXPECT_EQ(snapshot.counters.matched, 1U);
    KEXPECT_EQ(snapshot.counters.coalesced, 20U);
    KEXPECT_FALSE(fixture.configure("release id=106"));
    KREQUIRE_TRUE(fixture.configure("heal id=106"));
    KREQUIRE_TRUE(wki_chaos_snapshot(&snapshot));
    KEXPECT_EQ(snapshot.queued_frame_count, 0U);
    KEXPECT_EQ(snapshot.trace_count, 2U);

    std::array<WkiChaosTraceRow, 2> trace{};
    KREQUIRE_EQ(wki_chaos_trace_snapshot(trace.data(), trace.size()), 2U);
    KEXPECT_EQ(trace.at(1).outcome, WkiChaosOutcome::HEALED);
    KEXPECT_EQ(trace.at(1).coalesced_count, 20U);
    KEXPECT_EQ(wki_transport_send(&fixture.transport, 2, frame.data(), frame.size()), WKI_OK);
    KEXPECT_EQ(fixture.copy_tx_calls, 1U);
}

KTEST(WkiChaos, TransportIdsRemainUniqueAcrossWrapAndActiveCollision) {
    WkiTransport first{};
    WkiTransport second{};
    wki_chaos_selftest_set_next_transport_id(UINT16_MAX);
    wki_chaos_transport_register(&first);
    uint16_t const FIRST_ID = first.chaos_id;
    // An idempotent registration must neither replace the ID nor reset state.
    first.chaos_in_flight.store(3, std::memory_order_relaxed);
    wki_chaos_transport_register(&first);
    KEXPECT_EQ(first.chaos_id, FIRST_ID);
    KEXPECT_EQ(first.chaos_in_flight.load(std::memory_order_relaxed), 3U);
    first.chaos_in_flight.store(0, std::memory_order_relaxed);

    // Force the allocator back onto the live ID. It must skip that collision,
    // wrap if necessary, and still return a nonzero identity.
    wki_chaos_selftest_set_next_transport_id(FIRST_ID);
    wki_chaos_transport_register(&second);
    KEXPECT_NE(FIRST_ID, 0U);
    KEXPECT_NE(second.chaos_id, 0U);
    KEXPECT_NE(second.chaos_id, FIRST_ID);

    wki_chaos_transport_unregister(&first);
    wki_chaos_selftest_set_next_transport_id(FIRST_ID);
    wki_chaos_transport_register(&first);
    KEXPECT_EQ(first.chaos_id, FIRST_ID);
    KEXPECT_NE(first.chaos_id, second.chaos_id);
    wki_chaos_transport_unregister(&first);
    wki_chaos_transport_unregister(&second);
    wki_chaos_selftest_set_next_transport_id(1);
}

KTEST(WkiChaos, RuntimeBlockParserSqeCorruptionAndImmediateDoorbellReleaseAreExact) {
    RuntimeFixture fixture;
    KREQUIRE_TRUE(fixture.configure("enable seed=9"));
    KEXPECT_FALSE(fixture.configure("rule id=110 surface=blk_doorbell dir=rx action=drop"));
    KEXPECT_FALSE(fixture.configure("rule id=110 surface=blk_doorbell dir=tx action=corrupt sqe_offset=1 sqe_xor=1"));
    KEXPECT_FALSE(fixture.configure("rule id=110 surface=blk_sqe dir=tx action=corrupt sqe_offset=24 sqe_xor=1"));
    KEXPECT_FALSE(fixture.configure("rule id=110 surface=blk_sqe dir=tx action=drop sqe_offset=1 sqe_xor=1"));
    KEXPECT_FALSE(fixture.configure("rule id=110 id=111 surface=blk_doorbell dir=tx action=drop"));
    KEXPECT_FALSE(fixture.configure("rule id=110 surface=blk_doorbell dir=tx action=drop neighbor=2 neighbor=*"));
    KEXPECT_FALSE(fixture.configure("rule id=110 surface=blk_doorbell dir=tx action=drop chance=1 chance=2"));
    KEXPECT_FALSE(fixture.configure("release id=110 id=111"));
    KEXPECT_FALSE(fixture.configure("release id=0"));
    KEXPECT_FALSE(fixture.configure("heal id=0"));
    KEXPECT_FALSE(fixture.configure("heal id=110 id=111"));

    KREQUIRE_TRUE(
        fixture.configure("rule id=110 surface=blk_sqe dir=tx action=corrupt origin=proxy lane=roce neighbor=2 zone=4660 resource=7 "
                          "ring_generation=2882400001 ring_index=5 cookie=270544960 blk_op=0 sqe_offset=11 sqe_xor=0x80"));
    BlkSqEntry descriptor{
        .tag = 0x10203040,
        .opcode = static_cast<uint8_t>(BlkOpcode::READ),
        .reserved = {},
        .lba = 0x1122334455667788,
        .block_count = 7,
        .data_slot = 3,
    };
    BlkSqEntry const ORIGINAL = descriptor;
    auto key = make_block_key(WkiChaosSurface::BLOCK_SQE, fixture.transport.chaos_id);
    wki_chaos_block_sqe(key, &descriptor, sizeof(descriptor));
    auto const* before = reinterpret_cast<const uint8_t*>(&ORIGINAL);
    auto const* after = reinterpret_cast<const uint8_t*>(&descriptor);
    for (size_t offset = 0; offset < sizeof(descriptor); ++offset) {
        uint8_t const EXPECTED = offset == 11 ? static_cast<uint8_t>(before[offset] ^ 0x80U) : before[offset];
        KEXPECT_EQ(after[offset], EXPECTED);
    }
    std::array<WkiChaosTraceRow, 2> corruption_rows{};
    KREQUIRE_EQ(wki_chaos_trace_snapshot(corruption_rows.data(), corruption_rows.size()), 1U);
    KEXPECT_EQ(corruption_rows.at(0).surface, WkiChaosSurface::BLOCK_SQE);
    KEXPECT_EQ(corruption_rows.at(0).outcome, WkiChaosOutcome::CORRUPTED);
    KEXPECT_EQ(corruption_rows.at(0).block_corruption.byte_before, before[11]);
    KEXPECT_EQ(corruption_rows.at(0).block_corruption.byte_after, after[11]);

    KREQUIRE_TRUE(fixture.configure("clear"));
    KREQUIRE_TRUE(fixture.configure("enable seed=9"));
    KREQUIRE_TRUE(
        fixture.configure("rule id=111 surface=blk_doorbell dir=tx action=delay origin=proxy lane=roce neighbor=2 zone=4660 resource=7 "
                          "ring_generation=2882400001 ring_index=5 cookie=270544960 blk_op=0"));
    key = make_block_key(WkiChaosSurface::BLOCK_DOORBELL, fixture.transport.chaos_id);
    key.delivery_deadline_us = UINT64_MAX;
    s_block_observation = {};
    s_block_observation.expected = key;
    wki_chaos_selftest_set_block_doorbell_delivery_hook(observe_block_doorbell);
    KEXPECT_EQ(wki_chaos_block_doorbell(key), WkiChaosAction::DELAY);
    KREQUIRE_TRUE(fixture.configure("release id=111 count=1"));
    wki_chaos_drain_ready();
    KEXPECT_EQ(s_block_observation.calls, 1U);
    KEXPECT_TRUE(wki_chaos_block_identity_equal(s_block_observation.delivered, key));
    WkiChaosSnapshot snapshot{};
    KREQUIRE_TRUE(wki_chaos_snapshot(&snapshot));
    KEXPECT_EQ(snapshot.queued_block_event_count, 0U);
    KEXPECT_EQ(snapshot.counters.released, 1U);
    wki_chaos_drain_ready();
    KEXPECT_EQ(s_block_observation.calls, 1U);
}

KTEST(WkiChaos, RuntimeBlockDelayRejectsExpiredAndStaleDeliveryAndUnregisterDiscardsHeldEvent) {
    RuntimeFixture fixture;
    KREQUIRE_TRUE(fixture.configure("enable seed=10"));
    KREQUIRE_TRUE(fixture.configure("rule id=120 surface=blk_doorbell dir=tx action=delay transport=*"));
    wki_chaos_selftest_set_block_doorbell_delivery_hook(observe_block_doorbell);

    auto expired = make_block_key(WkiChaosSurface::BLOCK_DOORBELL, fixture.transport.chaos_id);
    expired.delivery_deadline_us = 1;
    s_block_observation = {};
    s_block_observation.expected = expired;
    KEXPECT_EQ(wki_chaos_block_doorbell(expired), WkiChaosAction::DELAY);
    KREQUIRE_TRUE(fixture.configure("release id=120 count=1"));
    wki_chaos_drain_ready();
    KEXPECT_EQ(s_block_observation.calls, 0U);

    auto stale = make_block_key(WkiChaosSurface::BLOCK_DOORBELL, fixture.transport.chaos_id);
    stale.delivery_deadline_us = UINT64_MAX;
    s_block_observation = {};
    s_block_observation.accept = false;
    s_block_observation.expected = stale;
    KEXPECT_EQ(wki_chaos_block_doorbell(stale), WkiChaosAction::DELAY);
    KREQUIRE_TRUE(fixture.configure("release id=120 count=1"));
    wki_chaos_drain_ready();
    KEXPECT_EQ(s_block_observation.calls, 1U);

    auto held = stale;
    ++held.operation_cookie;
    KEXPECT_EQ(wki_chaos_block_doorbell(held), WkiChaosAction::DELAY);
    WkiChaosSnapshot snapshot{};
    KREQUIRE_TRUE(wki_chaos_snapshot(&snapshot));
    KEXPECT_EQ(snapshot.queued_block_event_count, 1U);
    wki_chaos_transport_unregister(&fixture.transport);
    KREQUIRE_TRUE(wki_chaos_snapshot(&snapshot));
    KEXPECT_EQ(snapshot.queued_block_event_count, 0U);
    KEXPECT_EQ(fixture.transport.chaos_id, 0U);

    std::array<WkiChaosTraceRow, 12> rows{};
    size_t const COUNT = wki_chaos_trace_snapshot(rows.data(), rows.size());
    size_t failed_releases = 0;
    size_t transport_removed = 0;
    for (size_t index = 0; index < COUNT; ++index) {
        failed_releases += rows.at(index).action == WkiChaosAction::RELEASE && rows.at(index).outcome == WkiChaosOutcome::FAILED ? 1 : 0;
        transport_removed += rows.at(index).outcome == WkiChaosOutcome::TRANSPORT_REMOVED ? 1 : 0;
    }
    KEXPECT_EQ(failed_releases, 2U);
    KEXPECT_EQ(transport_removed, 1U);
}
