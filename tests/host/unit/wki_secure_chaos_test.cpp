#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <memory>
#include <net/wki/chaos_model.hpp>
#include <net/wki/rdma_protocol.hpp>

using namespace ker::net::wki;
namespace {
using Record = std::array<uint8_t, rdma_protocol::HEADER_BYTES + 4>;
struct Fixture {
    std::unique_ptr<WkiChaosModel> model = std::make_unique<WkiChaosModel>();
    rdma_protocol::KeyMaterial key{};
    rdma_protocol::KeyContext context{};
    rdma_protocol::Header header{};
    std::array<uint8_t, 4> plaintext{0x10, 0x20, 0x30, 0x40};
    Fixture() {
        model->enable(0x25324345);
        key.key.fill(0x19);
        key.nonce_prefix.fill(0x27);
        context.session_id.fill(0x31);
        context.negotiation.fill(0x52);
        context.src_node = 1;
        context.dst_node = 2;
        context.transport = rdma_protocol::Transport::ROCE;
        context.service = rdma_protocol::Service::ZONE;
        header.transport = static_cast<uint8_t>(context.transport);
        header.service = static_cast<uint8_t>(context.service);
        header.opcode = static_cast<uint8_t>(rdma_protocol::Opcode::WRITE);
        header.src_node = 1;
        header.dst_node = 2;
        header.channel_id = 31;
        header.session_id = context.session_id;
        header.capability.fill(0x43);
        header.region_generation = 7;
        header.operation_id = 11;
        header.binding_generation = 9;
        header.receive_ticket = 13;
        header.counter = 1000;
        header.total_length = 4;
        header.payload_length = 4;
        header.rkey = 17;
    }
    auto record() -> Record {
        Record output{};
        EXPECT_TRUE(rdma_protocol::encode(key, header, plaintext.data(), output.data(), output.size()));
        return output;
    }
    void rule(WkiChaosAction action, WkiChaosCorruptMode mode = WkiChaosCorruptMode::CHECKSUM) {
        WkiChaosRule rule{};
        rule.id = 1;
        rule.active = true;
        rule.action = action;
        rule.corrupt_mode = mode;
        if (mode == WkiChaosCorruptMode::PAYLOAD_XOR) {
            rule.payload_offset = 2;
            rule.payload_xor = 0x80;
        }
        ASSERT_TRUE(model->upsert_rule(rule));
    }
    auto intercept(const Record& record, uint64_t metadata = 0) -> WkiChaosInterceptResult {
        return model->intercept(WkiChaosDirection::TX, 7, 0x1234, 2, record.data(), record.size(), 10, 20, 30, metadata, true);
    }
    void rejects_authentication(const WkiChaosDeliveryView& delivery) {
        rdma_protocol::Header decoded{};
        std::array<uint8_t, 4> output{0xA1, 0xB2, 0xC3, 0xD4};
        auto const before = output;
        auto const clean = record();
        ASSERT_EQ(rdma_protocol::decode(key, context, clean.data(), clean.size(), decoded, output.data(), output.size()),
                  rdma_protocol::Drop::NONE);
        ASSERT_EQ(output, plaintext);
        output = before;
        EXPECT_EQ(rdma_protocol::decode(key, context, delivery.data, delivery.len, decoded, output.data(), output.size()),
                  rdma_protocol::Drop::AUTHENTICATION);
        EXPECT_EQ(output, before);
    }
};
}  // namespace

TEST(WkiSecureChaos, FullCounterAndServiceHaveDistinctHashesAndStreams) {
    Fixture f;
    f.rule(WkiChaosAction::PASS);
    f.intercept(f.record());
    f.header.counter += 1ULL << 32U;
    f.intercept(f.record());
    f.header.service = static_cast<uint8_t>(rdma_protocol::Service::BLOCK);
    f.intercept(f.record());
    std::array<WkiChaosTraceRow, 3> trace{};
    ASSERT_EQ(f.model->trace_snapshot(trace.data(), trace.size(), 0), 3U);
    EXPECT_TRUE(trace[0].key.secure_record);
    EXPECT_TRUE(trace[0].key.frame_valid);
    EXPECT_EQ(trace[0].key.msg_type, WKI_CHAOS_SECURE_TYPE_BASE | f.header.opcode);
    EXPECT_EQ(trace[0].key.dev_op_id, static_cast<uint8_t>(rdma_protocol::Service::ZONE));
    EXPECT_EQ(trace[0].key.secure_counter, 1000U);
    EXPECT_EQ(trace[0].key.seq_num, trace[1].key.seq_num);
    EXPECT_NE(trace[0].key.stable_hash, trace[1].key.stable_hash);
    EXPECT_EQ(trace[1].key.stream_occurrence, 1U);
    EXPECT_EQ(trace[2].key.stream_occurrence, 0U);
}

TEST(WkiSecureChaos, TagCorruptionRejectsAndNeverDisclosesTagBytes) {
    Fixture f;
    f.rule(WkiChaosAction::CORRUPT);
    auto const record = f.record();
    ASSERT_EQ(f.intercept(record).action, WkiChaosAction::CORRUPT);
    WkiChaosDeliveryView d{};
    ASSERT_TRUE(f.model->claim_ready(&d));
    EXPECT_EQ(d.kind, WkiChaosDeliveryView::Kind::SECURE_RECORD);
    for (size_t i = 0; i < record.size(); ++i) EXPECT_EQ(d.data[i], record[i] ^ (i == offsetof(rdma_protocol::Header, tag) ? 1U : 0U));
    f.rejects_authentication(d);
    f.model->finish_delivery(d.slot);
    std::array<WkiChaosTraceRow, 2> trace{};
    ASSERT_EQ(f.model->trace_snapshot(trace.data(), trace.size(), 0), 2U);
    for (auto const& row : trace) {
        EXPECT_EQ(row.checksum_before, 0U);
        EXPECT_EQ(row.checksum_after, 0U);
        EXPECT_EQ(row.corruption.byte_before, 0U);
        EXPECT_EQ(row.corruption.byte_after, 0U);
    }
}

TEST(WkiSecureChaos, CiphertextXorDoesNotRetagOrChangeHeader) {
    Fixture f;
    f.rule(WkiChaosAction::CORRUPT, WkiChaosCorruptMode::PAYLOAD_XOR);
    auto const record = f.record();
    f.intercept(record);
    WkiChaosDeliveryView d{};
    ASSERT_TRUE(f.model->claim_ready(&d));
    EXPECT_EQ(std::memcmp(d.data, record.data(), rdma_protocol::HEADER_BYTES), 0);
    EXPECT_EQ(d.data[rdma_protocol::HEADER_BYTES + 2], record[rdma_protocol::HEADER_BYTES + 2] ^ 0x80U);
    f.rejects_authentication(d);
    f.model->finish_delivery(d.slot);
}

TEST(WkiSecureChaos, DelayOwnsSnapshotAndRetainsDirectMetadata) {
    Fixture f;
    f.rule(WkiChaosAction::DELAY);
    auto record = f.record();
    auto const original = record;
    constexpr uint64_t metadata = (1ULL << 62U) | 250U;
    f.intercept(record, metadata);
    record.fill(0xDD);
    ASSERT_EQ(f.model->release(1, 1), 1U);
    WkiChaosDeliveryView d{};
    ASSERT_TRUE(f.model->claim_ready(&d));
    EXPECT_EQ(d.kind, WkiChaosDeliveryView::Kind::SECURE_RECORD);
    EXPECT_EQ(d.ingress_cookie, metadata);
    EXPECT_EQ(std::memcmp(d.data, original.data(), original.size()), 0);
    f.model->finish_delivery(d.slot);
    EXPECT_EQ(f.model->queued_frame_count(), 0U);
}

TEST(WkiSecureChaos, MalformedMagicRetainsTypedDelivery) {
    Fixture f;
    f.rule(WkiChaosAction::DELAY);
    auto record = f.record();
    record[0] ^= 0xFF;
    f.intercept(record);
    ASSERT_EQ(f.model->release(1, 1), 1U);
    WkiChaosDeliveryView d{};
    ASSERT_TRUE(f.model->claim_ready(&d));
    EXPECT_EQ(d.kind, WkiChaosDeliveryView::Kind::SECURE_RECORD);
    EXPECT_EQ(d.data[0], record[0]);
    f.model->finish_delivery(d.slot);
}

TEST(WkiSecureChaos, ExplicitOrdinaryFamilyRetainsOriginalDispatcher) {
    Fixture f;
    f.rule(WkiChaosAction::DELAY);
    auto const record = f.record();
    f.model->intercept(WkiChaosDirection::TX, 7, 0x1234, 2, record.data(), record.size());
    ASSERT_EQ(f.model->release(1, 1), 1U);
    WkiChaosDeliveryView d{};
    ASSERT_TRUE(f.model->claim_ready(&d));
    EXPECT_EQ(d.kind, WkiChaosDeliveryView::Kind::FRAME);
    f.model->finish_delivery(d.slot);
}

TEST(WkiSecureChaos, PressureIsBoundedAndProviderDiscardDrains) {
    Fixture f;
    f.rule(WkiChaosAction::DELAY);
    for (size_t i = 0; i < WKI_CHAOS_MAX_QUEUED_FRAMES; ++i) {
        ++f.header.counter;
        EXPECT_EQ(f.intercept(f.record()).outcome, WkiChaosOutcome::QUEUED);
    }
    auto const overflow = f.intercept(f.record());
    EXPECT_EQ(overflow.action, WkiChaosAction::DROP);
    EXPECT_EQ(overflow.outcome, WkiChaosOutcome::QUEUE_OVERFLOW);
    EXPECT_EQ(f.model->queued_frame_count(), WKI_CHAOS_MAX_QUEUED_FRAMES);
    f.model->discard_transport(0x1234, 7);
    EXPECT_EQ(f.model->queued_frame_count(), 0U);
}

TEST(WkiSecureChaos, TypeAndServiceSelectorsKeepFamiliesSeparate) {
    Fixture f;
    WkiChaosRule rule{};
    rule.id = 1;
    rule.active = true;
    rule.action = WkiChaosAction::DROP;
    rule.match_type = true;
    rule.msg_type = WKI_CHAOS_SECURE_TYPE_BASE | static_cast<uint8_t>(rdma_protocol::Opcode::WRITE);
    rule.match_op = true;
    rule.op_id = static_cast<uint16_t>(rdma_protocol::Service::ZONE);
    ASSERT_TRUE(f.model->upsert_rule(rule));
    auto const record = f.record();
    EXPECT_EQ(f.intercept(record).action, WkiChaosAction::DROP);
    EXPECT_EQ(f.model->intercept(WkiChaosDirection::TX, 7, 0x1234, 2, record.data(), record.size()).action, WkiChaosAction::PASS);
}
