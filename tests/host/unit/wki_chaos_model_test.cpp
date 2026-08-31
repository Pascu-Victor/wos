#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <net/wki/blk_ring.hpp>
#include <net/wki/chaos_model.hpp>

using namespace ker::net::wki;

namespace {

using TestFrame = std::array<uint8_t, WKI_HEADER_SIZE + 4>;
constexpr uint16_t BLOCK_BODY_SIZE = sizeof(uint64_t) + sizeof(uint32_t);
constexpr uint16_t BLOCK_LBA_PAYLOAD_OFFSET = sizeof(DevOpReqPayload);
constexpr uint16_t BLOCK_COUNT_PAYLOAD_OFFSET = BLOCK_LBA_PAYLOAD_OFFSET + sizeof(uint64_t);
using BlockFrame = std::array<uint8_t, WKI_HEADER_SIZE + sizeof(DevOpReqPayload) + BLOCK_BODY_SIZE>;

auto make_frame(uint16_t src, uint16_t dst, uint16_t channel, uint8_t type, uint32_t seq, uint32_t checksum = 0) -> TestFrame {
    TestFrame frame{};
    WkiHeader header{};
    header.version_flags = wki_version_flags(WKI_VERSION, 0);
    header.msg_type = type;
    header.src_node = src;
    header.dst_node = dst;
    header.channel_id = channel;
    header.seq_num = seq;
    header.payload_len = 4;
    header.hop_ttl = WKI_DEFAULT_TTL;
    header.checksum = checksum;
    std::memcpy(frame.data(), &header, sizeof(header));
    frame.at(WKI_HEADER_SIZE) = 0x11;
    frame.at(WKI_HEADER_SIZE + 1) = 0x22;
    frame.at(WKI_HEADER_SIZE + 2) = 0x33;
    frame.at(WKI_HEADER_SIZE + 3) = 0x44;
    return frame;
}

auto make_block_frame(uint16_t op_id, uint64_t lba, uint32_t count, uint32_t seq = 1) -> BlockFrame {
    BlockFrame frame{};
    WkiHeader header{};
    header.version_flags = wki_version_flags(WKI_VERSION, 0);
    header.msg_type = static_cast<uint8_t>(MsgType::DEV_OP_REQ);
    header.src_node = 1;
    header.dst_node = 2;
    header.channel_id = WKI_CHAN_RESOURCE;
    header.seq_num = seq;
    header.payload_len = static_cast<uint16_t>(frame.size() - WKI_HEADER_SIZE);
    header.hop_ttl = WKI_DEFAULT_TTL;
    std::memcpy(frame.data(), &header, sizeof(header));
    DevOpReqPayload request{.op_id = op_id, .data_len = BLOCK_BODY_SIZE};
    std::memcpy(frame.data() + WKI_HEADER_SIZE, &request, sizeof(request));
    std::memcpy(frame.data() + WKI_HEADER_SIZE + BLOCK_LBA_PAYLOAD_OFFSET, &lba, sizeof(lba));
    std::memcpy(frame.data() + WKI_HEADER_SIZE + BLOCK_COUNT_PAYLOAD_OFFSET, &count, sizeof(count));
    return frame;
}

auto valid_checksum(const uint8_t* data, size_t len) -> uint32_t {
    if (data == nullptr || len < WKI_HEADER_SIZE) {
        return 0;
    }
    WkiHeader header{};
    std::memcpy(&header, data, sizeof(header));
    header.checksum = 0;
    uint32_t crc = UINT32_MAX;
    auto update = [&crc](const uint8_t* bytes, size_t count) {
        for (size_t i = 0; i < count; ++i) {
            crc ^= bytes[i];
            for (uint8_t bit = 0; bit < 8; ++bit) {
                crc = (crc & 1U) != 0 ? (crc >> 1U) ^ 0xEDB88320U : crc >> 1U;
            }
        }
    };
    update(reinterpret_cast<const uint8_t*>(&header), sizeof(header));
    if (static_cast<size_t>(WKI_HEADER_SIZE) + header.payload_len <= len) {
        update(data + WKI_HEADER_SIZE, header.payload_len);
    }
    return crc ^ UINT32_MAX;
}

auto base_rule(uint32_t id, WkiChaosAction action) -> WkiChaosRule {
    WkiChaosRule rule{};
    rule.id = id;
    rule.active = true;
    rule.direction = WkiChaosDirection::TX;
    rule.action = action;
    rule.every = 1;
    rule.chance_permyriad = 10'000;
    return rule;
}

auto new_model(uint64_t seed = 0x12345678ULL) -> std::unique_ptr<WkiChaosModel> {
    auto model = std::make_unique<WkiChaosModel>();
    model->enable(seed);
    return model;
}

auto make_block_key(WkiChaosSurface surface = WkiChaosSurface::BLOCK_DOORBELL) -> WkiChaosBlockKey {
    return WkiChaosBlockKey{
        .surface = surface,
        .direction = WkiChaosDirection::TX,
        .origin = WkiChaosBlockOrigin::PROXY,
        .lane = WkiChaosBlockLane::ROCE,
        .transport_id = 7,
        .neighbor = 2,
        .zone_id = 0x1234,
        .resource_id = 9,
        .ring_generation = 0x10203040,
        .ring_index = 11,
        .operation_cookie = 0x44556677,
        .block_opcode = static_cast<uint8_t>(BlkOpcode::READ),
        .attach_cookie = 13,
        .channel_generation = 21,
        .owner_boot_epoch = 34,
        .resource_incarnation = 55,
        .delivery_deadline_us = 123'456,
    };
}

auto block_rule(uint32_t id, WkiChaosSurface surface, WkiChaosAction action) -> WkiChaosRule {
    auto value = base_rule(id, action);
    value.surface = surface;
    return value;
}

}  // namespace

TEST(WkiChaosModel, DisabledPathDoesNotMutateModel) {
    WkiChaosModel model;
    auto frame = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::DEV_OP_REQ), 4);

    auto const result = model.intercept(WkiChaosDirection::TX, 1, 0x1000, 2, frame.data(), frame.size());

    EXPECT_EQ(result.action, WkiChaosAction::PASS);
    EXPECT_FALSE(result.matched);
    EXPECT_EQ(model.counters().observed, 0u);
    EXPECT_EQ(model.trace_count(), 0u);
    EXPECT_EQ(model.queued_frame_count(), 0u);
}

TEST(WkiChaosModel, ExactRuleAndOccurrenceAreStable) {
    auto model = new_model();
    auto rule = base_rule(7, WkiChaosAction::DROP);
    rule.match_dst = true;
    rule.dst_node = 2;
    rule.match_channel = true;
    rule.channel_id = 3;
    rule.match_occurrence = true;
    rule.occurrence = 1;
    ASSERT_TRUE(model->upsert_rule(rule));
    auto frame = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::DEV_OP_REQ), 4);

    EXPECT_FALSE(model->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, frame.data(), frame.size()).matched);
    auto const second = model->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, frame.data(), frame.size());
    EXPECT_TRUE(second.matched);
    EXPECT_EQ(second.action, WkiChaosAction::DROP);
    EXPECT_FALSE(model->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, frame.data(), frame.size()).matched);
    EXPECT_EQ(model->trace_count(), 1u);
}

TEST(WkiChaosModel, OccurrenceCountsAcrossConsecutiveSequenceNumbers) {
    auto model = new_model();
    auto rule = base_rule(8, WkiChaosAction::DROP);
    rule.match_occurrence = true;
    rule.occurrence = 1;
    ASSERT_TRUE(model->upsert_rule(rule));
    auto first = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::DEV_OP_REQ), 10);
    auto second = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::DEV_OP_REQ), 11);

    EXPECT_FALSE(model->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, first.data(), first.size()).matched);
    auto const result = model->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, second.data(), second.size());
    EXPECT_TRUE(result.matched);
    EXPECT_EQ(result.action, WkiChaosAction::DROP);

    std::array<WkiChaosTraceRow, 1> rows{};
    ASSERT_EQ(model->trace_snapshot(rows.data(), rows.size(), 0), 1u);
    EXPECT_EQ(rows.at(0).key.seq_num, 11u);
    EXPECT_EQ(rows.at(0).key.stream_occurrence, 1u);
}

TEST(WkiChaosModel, SeededChanceIsIndependentOfStreamInterleaving) {
    auto first = new_model(99);
    auto second = new_model(99);
    auto rule = base_rule(3, WkiChaosAction::FAIL);
    rule.chance_permyriad = 5'000;
    ASSERT_TRUE(first->upsert_rule(rule));
    ASSERT_TRUE(second->upsert_rule(rule));
    auto frame_a = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::DEV_OP_REQ), 4);
    auto frame_b = make_frame(1, 2, 4, static_cast<uint8_t>(MsgType::DEV_OP_REQ), 5);

    std::array<bool, 8> first_a{};
    std::array<bool, 8> first_b{};
    std::array<bool, 8> second_a{};
    std::array<bool, 8> second_b{};
    for (size_t i = 0; i < first_a.size(); ++i) {
        first_a.at(i) = first->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, frame_a.data(), frame_a.size()).matched;
        first_b.at(i) = first->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, frame_b.data(), frame_b.size()).matched;
    }
    for (size_t i = 0; i < second_a.size(); ++i) {
        second_b.at(i) = second->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, frame_b.data(), frame_b.size()).matched;
    }
    for (size_t i = 0; i < second_a.size(); ++i) {
        second_a.at(i) = second->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, frame_a.data(), frame_a.size()).matched;
    }

    EXPECT_EQ(first_a, second_a);
    EXPECT_EQ(first_b, second_b);
}

TEST(WkiChaosModel, DuplicateQueuesOneExactSecondDelivery) {
    auto model = new_model();
    ASSERT_TRUE(model->upsert_rule(base_rule(1, WkiChaosAction::DUPLICATE)));
    auto frame = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::DEV_OP_REQ), 4);

    auto const result = model->intercept(WkiChaosDirection::TX, 2, 0x2000, 2, frame.data(), frame.size());
    ASSERT_EQ(result.action, WkiChaosAction::DUPLICATE);
    EXPECT_TRUE(result.queued_ready);
    WkiChaosDeliveryView delivery{};
    ASSERT_TRUE(model->claim_ready(&delivery));
    ASSERT_EQ(delivery.len, frame.size());
    EXPECT_EQ(std::memcmp(delivery.data, frame.data(), frame.size()), 0);
    model->finish_delivery(delivery.slot);
    EXPECT_FALSE(model->claim_ready(&delivery));
    EXPECT_EQ(model->counters().duplicated, 1u);
}

TEST(WkiChaosModel, DelayRequiresReleaseAndReorderReleasesNewestFirst) {
    auto delay_model = new_model();
    ASSERT_TRUE(delay_model->upsert_rule(base_rule(10, WkiChaosAction::DELAY)));
    auto frame = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::DEV_OP_REQ), 1);
    static_cast<void>(delay_model->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, frame.data(), frame.size()));
    WkiChaosDeliveryView delivery{};
    EXPECT_FALSE(delay_model->claim_ready(&delivery));
    EXPECT_EQ(delay_model->release(10, 1), 1u);
    EXPECT_TRUE(delay_model->claim_ready(&delivery));
    delay_model->finish_delivery(delivery.slot);

    auto reorder_model = new_model();
    ASSERT_TRUE(reorder_model->upsert_rule(base_rule(11, WkiChaosAction::REORDER)));
    auto first = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::DEV_OP_REQ), 10);
    auto second = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::DEV_OP_REQ), 11);
    static_cast<void>(reorder_model->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, first.data(), first.size()));
    static_cast<void>(reorder_model->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, second.data(), second.size()));
    EXPECT_EQ(reorder_model->release(11, 0), 2u);
    ASSERT_TRUE(reorder_model->claim_ready(&delivery));
    WkiHeader delivered_header{};
    std::memcpy(&delivered_header, delivery.data, sizeof(delivered_header));
    EXPECT_EQ(delivered_header.seq_num, 11u);
    reorder_model->finish_delivery(delivery.slot);
    ASSERT_TRUE(reorder_model->claim_ready(&delivery));
    std::memcpy(&delivered_header, delivery.data, sizeof(delivered_header));
    EXPECT_EQ(delivered_header.seq_num, 10u);
    reorder_model->finish_delivery(delivery.slot);
}

TEST(WkiChaosModel, LimitedDelayAndReorderCoalesceExactRetransmitsWithoutConsumingSchedule) {
    constexpr std::array ACTIONS{WkiChaosAction::DELAY, WkiChaosAction::REORDER};
    for (WkiChaosAction const ACTION : ACTIONS) {
        auto model = new_model();
        auto limited = base_rule(15, ACTION);
        limited.limit = 1;
        ASSERT_TRUE(model->upsert_rule(limited));
        auto frame = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::DEV_OP_REQ), 17);

        auto const FIRST = model->intercept(WkiChaosDirection::TX, 3, 0x3000, 2, frame.data(), frame.size());
        ASSERT_EQ(FIRST.outcome, WkiChaosOutcome::QUEUED);
        for (size_t retry = 0; retry < 20; ++retry) {
            auto const RETRY = model->intercept(WkiChaosDirection::TX, 3, 0x3000, 2, frame.data(), frame.size());
            EXPECT_EQ(RETRY.action, ACTION);
            EXPECT_EQ(RETRY.outcome, WkiChaosOutcome::COALESCED);
            EXPECT_EQ(RETRY.event_id, FIRST.event_id);
        }

        EXPECT_EQ(model->queued_frame_count(), 1U);
        EXPECT_EQ(model->trace_count(), 1U);
        EXPECT_EQ(model->counters().matched, 1U);
        EXPECT_EQ(model->counters().coalesced, 20U);
        std::array<WkiChaosRule, 1> rules{};
        ASSERT_EQ(model->rule_snapshot(rules.data(), rules.size()), 1U);
        EXPECT_EQ(rules.at(0).applied, 1U);

        auto different_payload = frame;
        different_payload.back() ^= 0x80U;
        auto const DIFFERENT = model->intercept(WkiChaosDirection::TX, 3, 0x3000, 2, different_payload.data(), different_payload.size());
        EXPECT_EQ(DIFFERENT.action, WkiChaosAction::PASS);
        auto const DIFFERENT_TRANSPORT = model->intercept(WkiChaosDirection::TX, 3, 0x3001, 2, frame.data(), frame.size());
        EXPECT_EQ(DIFFERENT_TRANSPORT.action, WkiChaosAction::PASS);
        EXPECT_EQ(model->queued_frame_count(), 1U);

        ASSERT_EQ(model->release(15, 1), 1U);
        EXPECT_EQ(model->release(15, 1), 0U);
        WkiChaosDeliveryView delivery{};
        ASSERT_TRUE(model->claim_ready(&delivery));
        EXPECT_EQ(std::memcmp(delivery.data, frame.data(), frame.size()), 0);
        model->finish_delivery(delivery.slot);
        EXPECT_FALSE(model->claim_ready(&delivery));

        std::array<WkiChaosTraceRow, 2> trace{};
        ASSERT_EQ(model->trace_snapshot(trace.data(), trace.size(), 0), 2U);
        EXPECT_EQ(trace.at(1).outcome, WkiChaosOutcome::RELEASED);
        EXPECT_EQ(trace.at(1).queued_event_id, FIRST.event_id);
        EXPECT_EQ(trace.at(1).coalesced_count, 20U);
    }
}

TEST(WkiChaosModel, RuleReplacementRejectsHeldFrameAndBlockRowsFailClosed) {
    auto frame_model = new_model();
    ASSERT_TRUE(frame_model->upsert_rule(base_rule(14, WkiChaosAction::DELAY)));
    auto frame = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::DEV_OP_REQ), 16);
    ASSERT_EQ(frame_model->intercept(WkiChaosDirection::TX, 3, 0x3000, 2, frame.data(), frame.size()).outcome, WkiChaosOutcome::QUEUED);
    EXPECT_FALSE(frame_model->upsert_rule(base_rule(14, WkiChaosAction::DROP)));
    EXPECT_EQ(frame_model->release(14, 1), 1U);
    WkiChaosDeliveryView delivery{};
    ASSERT_TRUE(frame_model->claim_ready(&delivery));
    frame_model->finish_delivery(delivery.slot);
    EXPECT_TRUE(frame_model->upsert_rule(base_rule(14, WkiChaosAction::DROP)));

    auto partition_model = new_model();
    ASSERT_TRUE(partition_model->upsert_rule(base_rule(15, WkiChaosAction::PARTITION)));
    ASSERT_EQ(partition_model->intercept(WkiChaosDirection::TX, 3, 0x3000, 2, frame.data(), frame.size()).outcome,
              WkiChaosOutcome::DROPPED);
    EXPECT_FALSE(partition_model->upsert_rule(base_rule(15, WkiChaosAction::PASS)));
    EXPECT_TRUE(partition_model->heal_partition(15));
    EXPECT_TRUE(partition_model->upsert_rule(base_rule(15, WkiChaosAction::PASS)));

    auto block_model = new_model();
    ASSERT_TRUE(block_model->upsert_rule(block_rule(16, WkiChaosSurface::BLOCK_DOORBELL, WkiChaosAction::DELAY)));
    ASSERT_EQ(block_model->intercept_block_doorbell(make_block_key()).outcome, WkiChaosOutcome::QUEUED);
    EXPECT_FALSE(block_model->upsert_rule(block_rule(16, WkiChaosSurface::BLOCK_DOORBELL, WkiChaosAction::DROP)));
    EXPECT_EQ(block_model->release(16, 1), 1U);
    ASSERT_TRUE(block_model->claim_ready(&delivery));
    block_model->finish_delivery(delivery.slot);
    EXPECT_TRUE(block_model->upsert_rule(block_rule(16, WkiChaosSurface::BLOCK_DOORBELL, WkiChaosAction::DROP)));
}

TEST(WkiChaosModel, IdenticalUnreliableControlFramesRemainDistinctOccurrences) {
    auto model = new_model();
    auto limited = base_rule(18, WkiChaosAction::DELAY);
    limited.limit = 2;
    ASSERT_TRUE(model->upsert_rule(limited));
    auto heartbeat = make_frame(1, 2, WKI_CHAN_CONTROL, static_cast<uint8_t>(MsgType::HEARTBEAT), 0);

    auto const FIRST = model->intercept(WkiChaosDirection::TX, 3, 0x3000, 2, heartbeat.data(), heartbeat.size());
    auto const SECOND = model->intercept(WkiChaosDirection::TX, 3, 0x3000, 2, heartbeat.data(), heartbeat.size());
    EXPECT_EQ(FIRST.outcome, WkiChaosOutcome::QUEUED);
    EXPECT_EQ(SECOND.outcome, WkiChaosOutcome::QUEUED);
    EXPECT_NE(FIRST.event_id, SECOND.event_id);
    EXPECT_EQ(model->queued_frame_count(), 2U);
    EXPECT_EQ(model->trace_count(), 2U);
    EXPECT_EQ(model->counters().coalesced, 0U);
    std::array<WkiChaosRule, 1> rules{};
    ASSERT_EQ(model->rule_snapshot(rules.data(), rules.size()), 1U);
    EXPECT_EQ(rules.at(0).applied, 2U);
    std::array<WkiChaosTraceRow, 2> trace{};
    ASSERT_EQ(model->trace_snapshot(trace.data(), trace.size(), 0), 2U);
    EXPECT_EQ(trace.at(0).key.stream_occurrence, 0U);
    EXPECT_EQ(trace.at(1).key.stream_occurrence, 1U);
}

TEST(WkiChaosModel, DuplicateNeverCoalescesRetransmits) {
    auto model = new_model();
    auto limited = base_rule(16, WkiChaosAction::DUPLICATE);
    limited.limit = 1;
    ASSERT_TRUE(model->upsert_rule(limited));
    auto frame = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::DEV_OP_REQ), 18);

    auto const FIRST = model->intercept(WkiChaosDirection::TX, 3, 0x3000, 2, frame.data(), frame.size());
    ASSERT_EQ(FIRST.action, WkiChaosAction::DUPLICATE);
    auto const RETRY = model->intercept(WkiChaosDirection::TX, 3, 0x3000, 2, frame.data(), frame.size());
    EXPECT_EQ(RETRY.action, WkiChaosAction::PASS);
    EXPECT_EQ(RETRY.outcome, WkiChaosOutcome::PASSED);
    EXPECT_EQ(model->counters().coalesced, 0U);
    EXPECT_EQ(model->trace_count(), 1U);
    EXPECT_EQ(model->queued_frame_count(), 1U);

    WkiChaosDeliveryView delivery{};
    ASSERT_TRUE(model->claim_ready(&delivery));
    model->finish_delivery(delivery.slot);
    EXPECT_FALSE(model->claim_ready(&delivery));
}

TEST(WkiChaosModel, LimitedPartitionRetainsOneRetransmitRecordUntilHeal) {
    auto model = new_model();
    auto limited = base_rule(17, WkiChaosAction::PARTITION);
    limited.limit = 1;
    ASSERT_TRUE(model->upsert_rule(limited));
    auto frame = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::DEV_OP_REQ), 19);

    auto const FIRST = model->intercept(WkiChaosDirection::TX, 3, 0x3000, 2, frame.data(), frame.size());
    ASSERT_EQ(FIRST.action, WkiChaosAction::PARTITION);
    ASSERT_EQ(FIRST.outcome, WkiChaosOutcome::DROPPED);
    for (size_t retry = 0; retry < 20; ++retry) {
        auto const RETRY = model->intercept(WkiChaosDirection::TX, 3, 0x3000, 2, frame.data(), frame.size());
        EXPECT_EQ(RETRY.action, WkiChaosAction::PARTITION);
        EXPECT_EQ(RETRY.outcome, WkiChaosOutcome::COALESCED);
    }
    EXPECT_EQ(model->queued_frame_count(), 1U);
    EXPECT_EQ(model->trace_count(), 1U);
    EXPECT_EQ(model->release(17, 0), 0U);
    EXPECT_TRUE(model->heal_partition(17));
    EXPECT_EQ(model->queued_frame_count(), 0U);
    EXPECT_EQ(model->intercept(WkiChaosDirection::TX, 3, 0x3000, 2, frame.data(), frame.size()).action, WkiChaosAction::PASS);

    std::array<WkiChaosTraceRow, 2> trace{};
    ASSERT_EQ(model->trace_snapshot(trace.data(), trace.size(), 0), 2U);
    EXPECT_EQ(trace.at(1).action, WkiChaosAction::PARTITION);
    EXPECT_EQ(trace.at(1).outcome, WkiChaosOutcome::HEALED);
    EXPECT_EQ(trace.at(1).queued_event_id, FIRST.event_id);
    EXPECT_EQ(trace.at(1).coalesced_count, 20U);
}

TEST(WkiChaosModel, UnboundedReliablePartitionAlsoCoalescesExactRetransmits) {
    auto model = new_model();
    ASSERT_TRUE(model->upsert_rule(base_rule(19, WkiChaosAction::PARTITION)));
    auto frame = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::DEV_OP_REQ), 20);

    EXPECT_EQ(model->intercept(WkiChaosDirection::TX, 3, 0x3000, 2, frame.data(), frame.size()).outcome, WkiChaosOutcome::DROPPED);
    for (size_t retry = 0; retry < 20; ++retry) {
        EXPECT_EQ(model->intercept(WkiChaosDirection::TX, 3, 0x3000, 2, frame.data(), frame.size()).outcome, WkiChaosOutcome::COALESCED);
    }
    EXPECT_EQ(model->queued_frame_count(), 1U);
    EXPECT_EQ(model->trace_count(), 1U);
    EXPECT_EQ(model->counters().matched, 1U);
    EXPECT_EQ(model->counters().coalesced, 20U);
    EXPECT_TRUE(model->heal_partition(19));
    EXPECT_EQ(model->queued_frame_count(), 0U);
}

TEST(WkiChaosModel, CorruptionForcesNonzeroInvalidChecksum) {
    auto model = new_model();
    auto rule = base_rule(12, WkiChaosAction::CORRUPT);
    rule.direction = WkiChaosDirection::RX;
    ASSERT_TRUE(model->upsert_rule(rule));
    auto frame = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::DEV_OP_REQ), 4, 0);

    static_cast<void>(model->intercept(WkiChaosDirection::RX, 1, 0x1000, 1, frame.data(), frame.size()));
    WkiChaosDeliveryView delivery{};
    ASSERT_TRUE(model->claim_ready(&delivery));
    WkiHeader corrupted{};
    std::memcpy(&corrupted, delivery.data, sizeof(corrupted));
    EXPECT_NE(corrupted.checksum, 0u);
    WkiHeader original{};
    std::memcpy(&original, frame.data(), sizeof(original));
    EXPECT_EQ(original.checksum, 0u);
    EXPECT_EQ(std::memcmp(delivery.data + WKI_HEADER_SIZE, frame.data() + WKI_HEADER_SIZE, 4), 0);
    model->finish_delivery(delivery.slot);
}

TEST(WkiChaosModel, DevOpMatcherRejectsWrongTypeWrongOpAndMalformedPayload) {
    auto model = new_model();
    auto rule = base_rule(13, WkiChaosAction::DROP);
    rule.match_op = true;
    rule.op_id = OP_BLOCK_READ;
    ASSERT_TRUE(model->upsert_rule(rule));

    auto wrong_op = make_block_frame(OP_BLOCK_WRITE, 0x1122334455667788ULL, 7);
    EXPECT_FALSE(model->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, wrong_op.data(), wrong_op.size()).matched);

    auto wrong_type = make_block_frame(OP_BLOCK_READ, 0x1122334455667788ULL, 7);
    WkiHeader wrong_type_header{};
    std::memcpy(&wrong_type_header, wrong_type.data(), sizeof(wrong_type_header));
    wrong_type_header.msg_type = static_cast<uint8_t>(MsgType::HEARTBEAT);
    std::memcpy(wrong_type.data(), &wrong_type_header, sizeof(wrong_type_header));
    EXPECT_FALSE(model->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, wrong_type.data(), wrong_type.size()).matched);

    auto malformed = make_block_frame(OP_BLOCK_READ, 0x1122334455667788ULL, 7);
    DevOpReqPayload malformed_request{};
    std::memcpy(&malformed_request, malformed.data() + WKI_HEADER_SIZE, sizeof(malformed_request));
    --malformed_request.data_len;
    std::memcpy(malformed.data() + WKI_HEADER_SIZE, &malformed_request, sizeof(malformed_request));
    EXPECT_FALSE(model->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, malformed.data(), malformed.size()).matched);

    auto matching = make_block_frame(OP_BLOCK_READ, 0x1122334455667788ULL, 7);
    auto const result = model->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, matching.data(), matching.size());
    EXPECT_TRUE(result.matched);
    EXPECT_EQ(result.action, WkiChaosAction::DROP);
    std::array<WkiChaosTraceRow, 1> rows{};
    ASSERT_EQ(model->trace_snapshot(rows.data(), rows.size(), 0), 1u);
    EXPECT_TRUE(rows.at(0).key.frame_valid);
    EXPECT_TRUE(rows.at(0).key.dev_op_valid);
    EXPECT_EQ(rows.at(0).key.dev_op_id, OP_BLOCK_READ);
}

TEST(WkiChaosModel, PayloadCorruptionTargetsBlockLbaAndCountOffsetsWithValidChecksum) {
    struct CorruptionCase {
        uint16_t offset;
        uint8_t xor_mask;
    };
    constexpr std::array CASES{
        CorruptionCase{.offset = BLOCK_LBA_PAYLOAD_OFFSET, .xor_mask = 0x80},
        CorruptionCase{.offset = BLOCK_COUNT_PAYLOAD_OFFSET, .xor_mask = 0x04},
    };

    uint32_t rule_id = 20;
    for (auto const& test_case : CASES) {
        auto model = new_model();
        auto rule = base_rule(rule_id++, WkiChaosAction::CORRUPT);
        rule.match_op = true;
        rule.op_id = OP_BLOCK_READ;
        rule.corrupt_mode = WkiChaosCorruptMode::PAYLOAD_XOR;
        rule.payload_offset = test_case.offset;
        rule.payload_xor = test_case.xor_mask;
        ASSERT_TRUE(model->upsert_rule(rule));
        auto frame = make_block_frame(OP_BLOCK_READ, 0x1122334455667788ULL, 7);

        auto const result = model->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, frame.data(), frame.size());
        ASSERT_TRUE(result.matched);
        WkiChaosDeliveryView delivery{};
        ASSERT_TRUE(model->claim_ready(&delivery));
        ASSERT_EQ(delivery.len, frame.size());
        for (size_t offset = 0; offset < frame.size() - WKI_HEADER_SIZE; ++offset) {
            uint8_t const EXPECTED = offset == test_case.offset
                                         ? static_cast<uint8_t>(frame.at(WKI_HEADER_SIZE + offset) ^ test_case.xor_mask)
                                         : frame.at(WKI_HEADER_SIZE + offset);
            EXPECT_EQ(delivery.data[WKI_HEADER_SIZE + offset], EXPECTED);
        }
        WkiHeader delivered_header{};
        std::memcpy(&delivered_header, delivery.data, sizeof(delivered_header));
        EXPECT_NE(delivered_header.checksum, 0u);
        EXPECT_EQ(delivered_header.checksum, valid_checksum(delivery.data, delivery.len));
        model->finish_delivery(delivery.slot);

        std::array<WkiChaosTraceRow, 2> rows{};
        ASSERT_EQ(model->trace_snapshot(rows.data(), rows.size(), 0), 2u);
        EXPECT_EQ(rows.at(0).corruption.mode, WkiChaosCorruptMode::PAYLOAD_XOR);
        EXPECT_EQ(rows.at(0).corruption.payload_offset, test_case.offset);
        EXPECT_EQ(rows.at(0).corruption.payload_xor, test_case.xor_mask);
        EXPECT_TRUE(rows.at(0).corruption.payload_changed);
        EXPECT_EQ(rows.at(0).corruption.byte_after, static_cast<uint8_t>(rows.at(0).corruption.byte_before ^ test_case.xor_mask));
    }
}

TEST(WkiChaosModel, PartitionPersistsUntilHealAndFailureIsExplicit) {
    auto model = new_model();
    ASSERT_TRUE(model->upsert_rule(base_rule(20, WkiChaosAction::PARTITION)));
    auto frame = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::HEARTBEAT), 0);
    EXPECT_EQ(model->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, frame.data(), frame.size()).action, WkiChaosAction::PARTITION);
    EXPECT_EQ(model->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, frame.data(), frame.size()).action, WkiChaosAction::PARTITION);
    EXPECT_TRUE(model->heal_partition(20));
    EXPECT_EQ(model->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, frame.data(), frame.size()).action, WkiChaosAction::PASS);

    ASSERT_TRUE(model->upsert_rule(base_rule(21, WkiChaosAction::FAIL)));
    EXPECT_EQ(model->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, frame.data(), frame.size()).outcome, WkiChaosOutcome::FAILED);
}

TEST(WkiChaosModel, QueueOverflowIsStickyAndInvalidatesCampaign) {
    auto model = new_model();
    ASSERT_TRUE(model->upsert_rule(base_rule(30, WkiChaosAction::DELAY)));
    for (size_t i = 0; i < WKI_CHAOS_MAX_QUEUED_FRAMES; ++i) {
        auto frame = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::DEV_OP_REQ), static_cast<uint32_t>(i));
        EXPECT_EQ(model->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, frame.data(), frame.size()).outcome, WkiChaosOutcome::QUEUED);
    }
    auto overflow = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::DEV_OP_REQ), 99);
    EXPECT_EQ(model->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, overflow.data(), overflow.size()).outcome,
              WkiChaosOutcome::QUEUE_OVERFLOW);
    EXPECT_TRUE(model->queue_overflow());
    EXPECT_TRUE(model->invalid());
}

TEST(WkiChaosModel, EveryQueuedActionUsesFailClosedOverflowPolicy) {
    constexpr std::array ACTIONS{
        WkiChaosAction::DUPLICATE,
        WkiChaosAction::DELAY,
        WkiChaosAction::REORDER,
        WkiChaosAction::CORRUPT,
    };
    for (WkiChaosAction const ACTION : ACTIONS) {
        auto model = new_model();
        auto filler = base_rule(32, WkiChaosAction::DELAY);
        filler.direction = WkiChaosDirection::RX;
        ASSERT_TRUE(model->upsert_rule(filler));
        for (size_t i = 0; i < WKI_CHAOS_MAX_QUEUED_FRAMES; ++i) {
            auto frame = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::DEV_OP_REQ), static_cast<uint32_t>(i));
            ASSERT_EQ(model->intercept(WkiChaosDirection::RX, 1, 0x1000, 2, frame.data(), frame.size()).outcome, WkiChaosOutcome::QUEUED);
        }

        ASSERT_TRUE(model->upsert_rule(base_rule(33, ACTION)));
        auto overflow = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::DEV_OP_REQ), 99);
        auto const result = model->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, overflow.data(), overflow.size());
        EXPECT_EQ(result.action, WkiChaosAction::DROP);
        EXPECT_EQ(result.outcome, WkiChaosOutcome::QUEUE_OVERFLOW);
        EXPECT_EQ(model->counters().dropped, 1u);
        EXPECT_TRUE(model->queue_overflow());
        EXPECT_TRUE(model->invalid());
    }
}

TEST(WkiChaosModel, TraceOverflowIsStickyAndNeverOverwritesReplayRows) {
    auto model = new_model();
    ASSERT_TRUE(model->upsert_rule(base_rule(31, WkiChaosAction::DROP)));
    auto frame = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::DEV_OP_REQ), 7);
    for (size_t i = 0; i <= WKI_CHAOS_MAX_TRACE_ROWS; ++i) {
        static_cast<void>(model->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, frame.data(), frame.size()));
    }
    EXPECT_EQ(model->trace_count(), WKI_CHAOS_MAX_TRACE_ROWS);
    EXPECT_EQ(model->trace_first_event_id(), 1u);
    EXPECT_EQ(model->trace_last_event_id(), WKI_CHAOS_MAX_TRACE_ROWS);
    EXPECT_TRUE(model->trace_overflow());
    EXPECT_TRUE(model->invalid());
}

TEST(WkiChaosModel, SameScheduleProducesIdenticalReplayRows) {
    auto first = new_model(123);
    auto second = new_model(123);
    auto rule = base_rule(40, WkiChaosAction::DROP);
    rule.every = 2;
    ASSERT_TRUE(first->upsert_rule(rule));
    ASSERT_TRUE(second->upsert_rule(rule));
    auto frame = make_frame(1, 2, 3, static_cast<uint8_t>(MsgType::DEV_OP_REQ), 7);
    for (size_t i = 0; i < 8; ++i) {
        static_cast<void>(first->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, frame.data(), frame.size(), 4, 5, 6));
        static_cast<void>(second->intercept(WkiChaosDirection::TX, 1, 0x1000, 2, frame.data(), frame.size(), 4, 5, 6));
    }
    std::array<WkiChaosTraceRow, 8> first_rows{};
    std::array<WkiChaosTraceRow, 8> second_rows{};
    size_t const FIRST_COUNT = first->trace_snapshot(first_rows.data(), first_rows.size(), 0);
    size_t const SECOND_COUNT = second->trace_snapshot(second_rows.data(), second_rows.size(), 0);
    ASSERT_EQ(FIRST_COUNT, SECOND_COUNT);
    for (size_t i = 0; i < FIRST_COUNT; ++i) {
        EXPECT_EQ(first_rows.at(i).event_id, second_rows.at(i).event_id);
        EXPECT_EQ(first_rows.at(i).key.stable_hash, second_rows.at(i).key.stable_hash);
        EXPECT_EQ(first_rows.at(i).key.stream_occurrence, second_rows.at(i).key.stream_occurrence);
        EXPECT_EQ(first_rows.at(i).rule_id, second_rows.at(i).rule_id);
        EXPECT_EQ(first_rows.at(i).action, second_rows.at(i).action);
        EXPECT_EQ(first_rows.at(i).outcome, second_rows.at(i).outcome);
        EXPECT_EQ(first_rows.at(i).local_boot_epoch, second_rows.at(i).local_boot_epoch);
        EXPECT_EQ(first_rows.at(i).peer_boot_epoch, second_rows.at(i).peer_boot_epoch);
        EXPECT_EQ(first_rows.at(i).peer_channel_epoch, second_rows.at(i).peer_channel_epoch);
    }
}

TEST(WkiChaosModel, BlockSqeCorruptionMutatesExactlyOneOwnedDescriptorByteAndTracesBothValues) {
    static_assert(sizeof(BlkSqEntry) == 24);
    auto model = new_model();
    auto corruption = block_rule(110, WkiChaosSurface::BLOCK_SQE, WkiChaosAction::CORRUPT);
    corruption.match_block_origin = true;
    corruption.block_origin = WkiChaosBlockOrigin::PROXY;
    corruption.match_block_lane = true;
    corruption.block_lane = WkiChaosBlockLane::ROCE;
    corruption.match_neighbor = true;
    corruption.neighbor = 2;
    corruption.match_zone = true;
    corruption.zone_id = 0x1234;
    corruption.match_resource = true;
    corruption.resource_id = 9;
    corruption.match_ring_generation = true;
    corruption.ring_generation = 0x10203040;
    corruption.match_ring_index = true;
    corruption.ring_index = 11;
    corruption.match_cookie = true;
    corruption.operation_cookie = 0x44556677;
    corruption.match_block_opcode = true;
    corruption.block_opcode = static_cast<uint8_t>(BlkOpcode::READ);
    corruption.sqe_offset = static_cast<uint16_t>(offsetof(BlkSqEntry, lba) + 3);
    corruption.sqe_xor = 0xA5;
    ASSERT_TRUE(model->upsert_rule(corruption));

    BlkSqEntry descriptor{
        .tag = 0x44556677,
        .opcode = static_cast<uint8_t>(BlkOpcode::READ),
        .reserved = {},
        .lba = 0x1122334455667788,
        .block_count = 7,
        .data_slot = 3,
    };
    BlkSqEntry const ORIGINAL = descriptor;
    auto wrong_key = make_block_key(WkiChaosSurface::BLOCK_SQE);
    ++wrong_key.operation_cookie;
    auto const WRONG = model->intercept_block_sqe(wrong_key, &descriptor, sizeof(descriptor));
    EXPECT_FALSE(WRONG.matched);
    EXPECT_EQ(std::memcmp(&descriptor, &ORIGINAL, sizeof(descriptor)), 0);

    auto key = make_block_key(WkiChaosSurface::BLOCK_SQE);
    auto const RESULT = model->intercept_block_sqe(key, &descriptor, sizeof(descriptor));
    ASSERT_TRUE(RESULT.matched);
    EXPECT_EQ(RESULT.action, WkiChaosAction::CORRUPT);
    EXPECT_EQ(RESULT.outcome, WkiChaosOutcome::CORRUPTED);
    auto const* before = reinterpret_cast<const uint8_t*>(&ORIGINAL);
    auto const* after = reinterpret_cast<const uint8_t*>(&descriptor);
    for (size_t offset = 0; offset < sizeof(descriptor); ++offset) {
        uint8_t const EXPECTED =
            offset == corruption.sqe_offset ? static_cast<uint8_t>(before[offset] ^ corruption.sqe_xor) : before[offset];
        EXPECT_EQ(after[offset], EXPECTED) << "offset=" << offset;
    }

    std::array<WkiChaosTraceRow, 1> rows{};
    ASSERT_EQ(model->trace_snapshot(rows.data(), rows.size(), 0), 1U);
    EXPECT_EQ(rows.at(0).surface, WkiChaosSurface::BLOCK_SQE);
    EXPECT_TRUE(wki_chaos_block_identity_equal(rows.at(0).block_key, key));
    EXPECT_EQ(rows.at(0).block_key.descriptor_len, sizeof(BlkSqEntry));
    EXPECT_EQ(rows.at(0).block_corruption.sqe_offset, corruption.sqe_offset);
    EXPECT_EQ(rows.at(0).block_corruption.sqe_xor, corruption.sqe_xor);
    EXPECT_EQ(rows.at(0).block_corruption.byte_before, before[corruption.sqe_offset]);
    EXPECT_EQ(rows.at(0).block_corruption.byte_after, after[corruption.sqe_offset]);
    EXPECT_TRUE(rows.at(0).block_corruption.changed);
}

TEST(WkiChaosModel, BlockDoorbellDelayHasExactIdentitySingleReleaseAndClearFencing) {
    auto model = new_model();
    auto delay = block_rule(120, WkiChaosSurface::BLOCK_DOORBELL, WkiChaosAction::DELAY);
    delay.match_block_origin = true;
    delay.block_origin = WkiChaosBlockOrigin::PROXY;
    delay.match_block_lane = true;
    delay.block_lane = WkiChaosBlockLane::ROCE;
    delay.match_neighbor = true;
    delay.neighbor = 2;
    delay.match_zone = true;
    delay.zone_id = 0x1234;
    delay.match_resource = true;
    delay.resource_id = 9;
    delay.match_ring_generation = true;
    delay.ring_generation = 0x10203040;
    delay.match_ring_index = true;
    delay.ring_index = 11;
    delay.match_cookie = true;
    delay.operation_cookie = 0x44556677;
    delay.match_block_opcode = true;
    delay.block_opcode = static_cast<uint8_t>(BlkOpcode::READ);
    ASSERT_TRUE(model->upsert_rule(delay));

    auto stale = make_block_key();
    ++stale.ring_generation;
    EXPECT_FALSE(model->intercept_block_doorbell(stale).matched);

    auto const key = make_block_key();
    auto const RESULT = model->intercept_block_doorbell(key);
    ASSERT_TRUE(RESULT.matched);
    EXPECT_EQ(RESULT.action, WkiChaosAction::DELAY);
    EXPECT_EQ(model->queued_block_event_count(), 1U);
    EXPECT_EQ(model->release(120, 1), 1U);
    EXPECT_EQ(model->release(120, 1), 0U);

    WkiChaosDeliveryView delivery{};
    ASSERT_TRUE(model->claim_ready(&delivery));
    EXPECT_EQ(delivery.kind, WkiChaosDeliveryView::Kind::BLOCK_DOORBELL);
    EXPECT_TRUE(wki_chaos_block_identity_equal(delivery.block_key, key));
    EXPECT_EQ(delivery.block_key.delivery_deadline_us, key.delivery_deadline_us);
    EXPECT_FALSE(model->clear());
    model->finish_delivery(delivery.slot);
    model->finish_delivery(delivery.slot);
    EXPECT_EQ(model->counters().released, 1U);
    EXPECT_EQ(model->queued_block_event_count(), 0U);
    EXPECT_FALSE(model->claim_ready(&delivery));
    EXPECT_TRUE(model->clear());
}

TEST(WkiChaosModel, ClearDiscardsHeldBlockDoorbellWithoutDeliveryOrRelease) {
    auto model = new_model();
    ASSERT_TRUE(model->upsert_rule(block_rule(121, WkiChaosSurface::BLOCK_DOORBELL, WkiChaosAction::DELAY)));
    EXPECT_EQ(model->intercept_block_doorbell(make_block_key()).outcome, WkiChaosOutcome::QUEUED);
    EXPECT_EQ(model->queued_block_event_count(), 1U);
    EXPECT_TRUE(model->clear());
    EXPECT_EQ(model->queued_block_event_count(), 0U);
    WkiChaosDeliveryView delivery{};
    EXPECT_FALSE(model->claim_ready(&delivery));
}

TEST(WkiChaosModel, TransportUnregisterDiscardsOnlyMatchingHeldBlockDoorbellExactlyOnce) {
    auto model = new_model();
    ASSERT_TRUE(model->upsert_rule(block_rule(123, WkiChaosSurface::BLOCK_DOORBELL, WkiChaosAction::DELAY)));
    auto first = make_block_key();
    auto second = first;
    second.transport_id = 8;
    second.operation_cookie = 0x778899AA;
    EXPECT_EQ(model->intercept_block_doorbell(first).outcome, WkiChaosOutcome::QUEUED);
    EXPECT_EQ(model->intercept_block_doorbell(second).outcome, WkiChaosOutcome::QUEUED);
    ASSERT_EQ(model->queued_block_event_count(), 2U);

    model->discard_transport(0, first.transport_id);
    EXPECT_EQ(model->queued_block_event_count(), 1U);
    model->discard_transport(0, first.transport_id);
    EXPECT_EQ(model->queued_block_event_count(), 1U);
    EXPECT_EQ(model->release(123, 0), 1U);
    WkiChaosDeliveryView delivery{};
    ASSERT_TRUE(model->claim_ready(&delivery));
    EXPECT_TRUE(wki_chaos_block_identity_equal(delivery.block_key, second));
    model->finish_delivery(delivery.slot);

    std::array<WkiChaosTraceRow, 8> rows{};
    size_t const COUNT = model->trace_snapshot(rows.data(), rows.size(), 0);
    size_t removed = 0;
    for (size_t index = 0; index < COUNT; ++index) {
        if (rows.at(index).outcome == WkiChaosOutcome::TRANSPORT_REMOVED) {
            ++removed;
            EXPECT_EQ(rows.at(index).block_key.transport_id, first.transport_id);
        }
    }
    EXPECT_EQ(removed, 1U);
}

TEST(WkiChaosModel, BlockDoorbellQueueOverflowIsBoundedStickyAndFailClosed) {
    auto model = new_model();
    ASSERT_TRUE(model->upsert_rule(block_rule(122, WkiChaosSurface::BLOCK_DOORBELL, WkiChaosAction::DELAY)));
    auto key = make_block_key();
    for (size_t index = 0; index < WKI_CHAOS_MAX_QUEUED_BLOCK_EVENTS; ++index) {
        key.ring_index = static_cast<uint32_t>(index);
        key.operation_cookie = static_cast<uint32_t>(100 + index);
        EXPECT_EQ(model->intercept_block_doorbell(key).outcome, WkiChaosOutcome::QUEUED);
    }
    key.ring_index = 99;
    key.operation_cookie = 999;
    auto const OVERFLOW = model->intercept_block_doorbell(key);
    EXPECT_EQ(OVERFLOW.action, WkiChaosAction::DROP);
    EXPECT_EQ(OVERFLOW.outcome, WkiChaosOutcome::QUEUE_OVERFLOW);
    EXPECT_EQ(model->queued_block_event_count(), WKI_CHAOS_MAX_QUEUED_BLOCK_EVENTS);
    EXPECT_TRUE(model->queue_overflow());
    EXPECT_TRUE(model->invalid());
}
