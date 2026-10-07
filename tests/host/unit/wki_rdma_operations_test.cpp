#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/wki/rdma_protocol.hpp>
#include <numeric>
#include <random>
#include <span>
#include <thread>
#include <utility>
#include <vector>

// Only private deterministic count/exhaustion/contention seams. No production
// hook and no replacement of operation, coverage, registry or crypto bodies.
#define private public
#include <net/wki/rdma_operations.hpp>
#undef private

namespace ops = ker::net::wki::rdma_operations;
namespace reg = ker::net::wki::rdma_registry;
namespace proto = ker::net::wki::rdma_protocol;
using Drop = proto::Drop;
using ops::Outcome;
using ops::Role;

namespace {

auto authority() -> reg::Binding {
    reg::Binding value{.owner_node = 1,
                       .peer_node = 2,
                       .secure_incarnation = 3,
                       .transport_cookie = 4,
                       .transport_generation = 5,
                       .channel_cookie = 6,
                       .channel_generation = 7,
                       .channel_id = 16,
                       .transport = proto::Transport::ROCE,
                       .service = proto::Service::VFS};
    value.session_id.fill(0x71);
    return value;
}

auto frame(const ops::Spec& spec, proto::Opcode opcode, uint32_t offset = 0, uint32_t total = UINT32_MAX) -> proto::Header {
    proto::Header value{};
    value.transport = static_cast<uint8_t>(spec.binding.transport);
    value.service = static_cast<uint8_t>(spec.binding.service);
    value.opcode = static_cast<uint8_t>(opcode);
    value.src_node = spec.binding.peer_node;
    value.dst_node = spec.binding.owner_node;
    value.channel_id = spec.binding.channel_id;
    value.flags = opcode == proto::Opcode::READ_DATA || opcode == proto::Opcode::READ_END || opcode == proto::Opcode::WRITE_END
                      ? proto::FLAG_RESPONSE
                      : spec.flags;
    value.session_id = spec.binding.session_id;
    value.region_generation = spec.grant.generation;
    value.capability = spec.grant.capability;
    value.region_offset = spec.offset;
    value.operation_id = spec.operation_id;
    value.binding_generation = spec.grant.binding_generation;
    value.receive_ticket = spec.receive_ticket;
    value.counter = 1;
    value.fragment_offset = offset;
    value.total_length = total == UINT32_MAX ? spec.requested : total;
    if (opcode == proto::Opcode::WRITE || opcode == proto::Opcode::READ_DATA) {
        value.payload_length = std::min(value.total_length - offset, proto::FRAGMENT_BYTES);
    }
    value.rkey = spec.grant.rkey;
    return value;
}

auto request_opcode(Role role) -> proto::Opcode {
    return role == Role::SERVE_READ || role == Role::RECEIVE_READ ? proto::Opcode::READ
           : role == Role::RECEIVE_NOTIFY                         ? proto::Opcode::NOTIFY
                                                                  : proto::Opcode::WRITE;
}

template <class Value>
auto zero(const Value& value) -> bool {
    auto const* bytes = reinterpret_cast<const uint8_t*>(&value);
    return std::all_of(bytes, bytes + sizeof(value), [](auto byte) { return byte == 0; });
}

class OperationsTest : public testing::Test {
   protected:
    void SetUp() override {
        scratch.resize(ops::SCRATCH_BYTES, 0);
        backing.resize(proto::MAX_TRANSFER_BYTES, 0xa5);
        ker::net::wki::rdma_crypto::Key root{};
        root.fill(0x43);
        ASSERT_EQ(registry.initialize(1, root), Drop::NONE);
        ASSERT_EQ(table.initialize(scratch), Drop::NONE);
    }
    void TearDown() override {
        static_cast<void>(table.shutdown());
        erase_all();
        if (quarantined) {
            EXPECT_EQ(table.shutdown(), Drop::PRESSURE);
        } else {
            EXPECT_EQ(table.shutdown(), Drop::NONE);
        }
        for (auto& slot : registry.slots_) {
            if (!slot.active) continue;
            reg::ReleasedRegion released{};
            EXPECT_EQ(registry.reclaim(slot.descriptor.rkey, slot.descriptor.generation, released), Drop::NONE);
        }
        EXPECT_EQ(registry.shutdown(), Drop::NONE);
        // Quarantined fixture storage is still private and never reused; its
        // test owner erases it before destroying the externally quiescent table.
        std::fill(scratch.begin(), scratch.end(), 0);
    }
    auto spec(Role role = Role::RECEIVE_WRITE, uint32_t length = 64) -> ops::Spec {
        ops::Spec value{};
        value.role = role;
        value.binding = authority();
        value.operation_id = next_id++;
        value.receive_ticket = next_id++;
        value.requested = length;
        value.deadline_us = 1000;
        reg::Registration input{.binding = value.binding,
                                .backing = backing.data(),
                                .size = std::max(length, 1U),
                                .binding_generation = 20,
                                .permissions = proto::PERMISSIONS};
        EXPECT_EQ(registry.register_region(input, value.grant), Drop::NONE);
        return value;
    }
    auto lease(const ops::Spec& value) -> reg::Lease {
        reg::Lease result;
        EXPECT_EQ(registry.admit(value.binding, frame(value, request_opcode(value.role)), result), Drop::NONE);
        return result;
    }
    void prepare(const ops::Spec& value, ops::Handle& handle, bool arm = true) {
        reg::Lease pin;
        if (value.role == Role::RECEIVE_WRITE || value.role == Role::SERVE_READ || value.role == Role::RECEIVE_NOTIFY) pin = lease(value);
        ASSERT_EQ(table.prepare(value, std::move(pin), handle), Drop::NONE);
        if (arm) ASSERT_EQ(table.arm(handle, 21), Drop::NONE);
    }
    void ready_write(const ops::Spec& value, ops::Handle& handle) {
        prepare(value, handle);
        std::vector<uint8_t> payload(value.requested, 0x39);
        ASSERT_LE(value.requested, proto::FRAGMENT_BYTES);
        ASSERT_EQ(table.admit(value.binding, frame(value, proto::Opcode::WRITE), payload), Drop::NONE);
    }
    void sent(const ops::Spec& value, ops::Handle& handle) {
        prepare(value, handle);
        if (value.role == Role::SEND_WRITE) {
            ops::Work prepare_work;
            ASSERT_EQ(table.begin_prepare(handle, prepare_work), Drop::NONE);
            std::fill(prepare_work.prepare_buffer().begin(), prepare_work.prepare_buffer().end(), 0x39);
            ASSERT_EQ(table.seal(prepare_work), Drop::NONE);
        }
        ops::Work send;
        ASSERT_EQ(table.begin_send(handle, send), Drop::NONE);
        ASSERT_EQ(table.finish_send(send, 0), Drop::NONE);
    }
    void erase_all() {
        for (size_t index = 0; index < ops::OPERATION_SLOTS; ++index) {
            ops::EraseWork erase;
            if (table.begin_erase(erase) != Drop::NONE) break;
            std::fill(erase.buffer().begin(), erase.buffer().end(), 0);
            EXPECT_EQ(table.finish_erase(erase), Drop::NONE);
        }
    }
    std::vector<uint8_t> scratch;
    std::vector<uint8_t> backing;
    reg::Registry registry;
    ops::Operations table;
    uint64_t next_id = 30;
    bool quarantined = false;
};

}  // namespace

TEST_F(OperationsTest, InitializationAndGenerationsAreOneShotAndTerminal) {
    EXPECT_EQ(table.initialize(scratch), Drop::RETIRED);
    table.next_generation_ = UINT64_MAX - 1;
    auto value = spec(Role::RECEIVE_READ);
    ops::Handle handle;
    prepare(value, handle);
    EXPECT_EQ(handle.generation_, UINT64_MAX - 1);
    EXPECT_EQ(table.cancel(handle, -ECANCELED), Drop::NONE);
    handle.reset();
    erase_all();
    auto fresh = spec(Role::RECEIVE_READ);
    ops::Handle denied;
    EXPECT_EQ(table.prepare(fresh, {}, denied), Drop::COUNTER);
    EXPECT_FALSE(denied);
    EXPECT_EQ(table.next_generation_, 0U);
    EXPECT_EQ(table.prepare(fresh, {}, denied), Drop::COUNTER);
    EXPECT_EQ(table.shutdown(), Drop::NONE);
    EXPECT_EQ(table.initialize(scratch), Drop::RETIRED);
}

TEST_F(OperationsTest, InvalidInitialScratchDoesNotConsumeInitialization) {
    ops::Operations other;
    EXPECT_EQ(other.initialize({}), Drop::LENGTH);
    EXPECT_EQ(other.initialize(std::span(scratch).first(scratch.size() - 1)), Drop::LENGTH);
    EXPECT_EQ(other.initialize({reinterpret_cast<uint8_t*>(UINTPTR_MAX - 1), ops::SCRATCH_BYTES}), Drop::RANGE);
    EXPECT_EQ(other.initialize(scratch), Drop::NONE);
    EXPECT_EQ(other.shutdown(), Drop::NONE);
}

TEST_F(OperationsTest, PrepareRejectsInvalidSpecWithoutConsumingLeaseOrOutput) {
    auto const VALID = spec();
    std::vector<ops::Spec> invalid;
    auto add = [&](auto mutate) {
        auto value = VALID;
        mutate(value);
        invalid.push_back(value);
    };
    add([](auto& value) { value.role = Role::NONE; });
    add([](auto& value) { value.operation_id = 0; });
    add([](auto& value) { value.receive_ticket = 0; });
    add([](auto& value) { value.deadline_us = 0; });
    add([](auto& value) { value.requested = 0; });
    add([](auto& value) { value.requested = proto::MAX_TRANSFER_BYTES + 1; });
    add([](auto& value) { value.offset = 1; });
    add([](auto& value) { value.offset = UINT64_MAX; });
    add([](auto& value) { value.flags = 0x8000; });
    add([](auto& value) { value.binding.owner_node = 0; });
    add([](auto& value) { value.binding.peer_node = UINT16_MAX; });
    add([](auto& value) { value.binding.session_id = {}; });
    add([](auto& value) { value.binding.secure_incarnation = 0; });
    add([](auto& value) { value.binding.transport_cookie = 0; });
    add([](auto& value) { value.binding.transport_generation = 0; });
    add([](auto& value) { value.binding.channel_cookie = 0; });
    add([](auto& value) { value.binding.channel_generation = 0; });
    add([](auto& value) { value.binding.channel_id = 0; });
    add([](auto& value) { value.grant.permissions = 0; });
    add([](auto& value) { value.grant.reserved = 1; });
    for (auto const& value : invalid) {
        auto pin = lease(VALID);
        ops::Handle handle;
        EXPECT_NE(table.prepare(value, std::move(pin), handle), Drop::NONE);
        EXPECT_TRUE(pin);
        EXPECT_FALSE(handle);
    }
    ops::Statistics stats{};
    EXPECT_EQ(table.statistics(stats), Drop::NONE);
    EXPECT_EQ(stats.live, 0U);
}

TEST_F(OperationsTest, IncomingRequiresExactUnclaimedLeaseOutgoingRequiresEmptyLease) {
    auto value = spec();
    ops::Handle handle;
    EXPECT_EQ(table.prepare(value, {}, handle), Drop::GRANT);
    auto pin = lease(value);
    auto mismatch = value;
    ++mismatch.grant.generation;
    EXPECT_EQ(table.prepare(mismatch, std::move(pin), handle), Drop::GRANT);
    EXPECT_TRUE(pin);
    EXPECT_EQ(pin.claim(), Drop::NONE);
    EXPECT_EQ(table.prepare(value, std::move(pin), handle), Drop::GRANT);
    EXPECT_TRUE(pin);
    auto outgoing = value;
    outgoing.role = Role::SEND_WRITE;
    EXPECT_EQ(table.prepare(outgoing, std::move(pin), handle), Drop::GRANT);
    pin.reset();
    EXPECT_EQ(table.prepare(outgoing, {}, handle), Drop::NONE);
    EXPECT_EQ(table.slots_[handle.slot_].state, ops::State::PREPARING);
}

TEST_F(OperationsTest, FixedEightSlotsAndLiveIdentityPreventNinthOrDuplicateAdmission) {
    std::array<ops::Handle, ops::OPERATION_SLOTS> handles;
    auto first = spec(Role::RECEIVE_READ);
    prepare(first, handles[0]);
    for (size_t index = 1; index < handles.size(); ++index) prepare(spec(Role::RECEIVE_READ), handles[index]);
    auto ninth = spec(Role::RECEIVE_READ);
    ops::Handle denied;
    EXPECT_EQ(table.prepare(ninth, {}, denied), Drop::PRESSURE);
    EXPECT_EQ(table.prepare(first, {}, denied), Drop::OPERATION);
    auto same_ticket = first;
    ++same_ticket.operation_id;
    EXPECT_EQ(table.prepare(same_ticket, {}, denied), Drop::OPERATION);
    ops::Statistics stats{};
    EXPECT_EQ(table.statistics(stats), Drop::NONE);
    EXPECT_EQ(stats.live, 8U);
    EXPECT_EQ(stats.handles, 8U);
    auto const OLD_GENERATION = handles[0].generation_;
    EXPECT_EQ(table.cancel(handles[0], -ECANCELED), Drop::NONE);
    handles[0].reset();
    erase_all();
    EXPECT_EQ(table.prepare(ninth, {}, denied), Drop::NONE);
    EXPECT_GT(denied.generation_, OLD_GENERATION);
}

TEST_F(OperationsTest, IncomingArmIsOneShotAndCannotBeReplacedByGrantOrCoverage) {
    auto value = spec();
    ops::Handle handle;
    prepare(value, handle, false);
    std::array<uint8_t, 64> payload{};
    EXPECT_EQ(table.admit(value.binding, frame(value, proto::Opcode::WRITE), payload), Drop::OPERATION);
    ops::Work work;
    EXPECT_EQ(table.take_effect(handle, work), Drop::OPERATION);
    EXPECT_EQ(table.arm(handle, 0), Drop::OPERATION);
    EXPECT_EQ(table.arm(handle, 21), Drop::NONE);
    EXPECT_EQ(table.arm(handle, 21), Drop::OPERATION);
    EXPECT_EQ(table.admit(value.binding, frame(value, proto::Opcode::WRITE), payload), Drop::NONE);
    EXPECT_EQ(table.take_effect(handle, work), Drop::NONE);
    EXPECT_TRUE(work.prepare_buffer().empty());
    EXPECT_EQ(backing[0], 0xa5);
    EXPECT_EQ(table.finish_effect(work, {.status = 0, .bytes = 64, .outcome = Outcome::EXACT}), Drop::NONE);
}

TEST_F(OperationsTest, Full512ShuffledFragmentsRequireEveryByteBeforeBackingEffect) {
    auto value = spec(Role::RECEIVE_WRITE, proto::MAX_TRANSFER_BYTES);
    ops::Handle handle;
    prepare(value, handle);
    std::vector<uint8_t> oracle(value.requested);
    for (size_t index = 0; index < oracle.size(); ++index) oracle[index] = static_cast<uint8_t>((index * 29U + index / 8192U + 17U) % 251U);
    std::array<uint32_t, proto::MAX_FRAGMENTS> order{};
    std::iota(order.begin(), order.end(), 0U);
    std::mt19937 random(7101);
    std::shuffle(order.begin(), order.end(), random);
    for (size_t index = 0; index < order.size(); ++index) {
        uint32_t const OFFSET = order[index] * proto::FRAGMENT_BYTES;
        auto header = frame(value, proto::Opcode::WRITE, OFFSET);
        auto payload = std::span(oracle).subspan(OFFSET, header.payload_length);
        ASSERT_EQ(table.admit(value.binding, header, payload), Drop::NONE);
        EXPECT_EQ(backing.front(), 0xa5);
        EXPECT_EQ(backing.back(), 0xa5);
        if (index + 1 != order.size()) {
            ops::Work early;
            EXPECT_EQ(table.take_effect(handle, early), Drop::OPERATION);
            EXPECT_FALSE(early);
        }
    }
    ops::Work work;
    ASSERT_EQ(table.take_effect(handle, work), Drop::NONE);
    EXPECT_TRUE(std::equal(oracle.begin(), oracle.end(), work.staging().begin(), work.staging().end()));
    ASSERT_EQ(work.backing().backing, backing.data());
    std::memcpy(work.backing().backing, work.staging().data(), work.staging().size());
    ASSERT_EQ(table.finish_effect(work, {.status = 0, .bytes = value.requested, .outcome = Outcome::EXACT}), Drop::NONE);
    EXPECT_EQ(backing, oracle);
}

TEST_F(OperationsTest, HolesDuplicatesAndNoncanonicalOffsetsCannotCompleteCoverage) {
    auto value = spec(Role::RECEIVE_WRITE, proto::FRAGMENT_BYTES + 2);
    --value.requested;  // The grant permits both totals; the armed operation permits only one.
    ops::Handle handle;
    prepare(value, handle);
    std::array<uint8_t, proto::FRAGMENT_BYTES> payload{};
    auto first = frame(value, proto::Opcode::WRITE);
    EXPECT_EQ(table.admit(value.binding, first, payload), Drop::NONE);
    EXPECT_EQ(table.admit(value.binding, first, payload), Drop::DUPLICATE);
    auto overlap = first;
    overlap.fragment_offset = 1;
    EXPECT_EQ(table.admit(value.binding, overlap, payload), Drop::FRAGMENT);
    ops::Work early;
    EXPECT_EQ(table.take_effect(handle, early), Drop::OPERATION);
    auto last = frame(value, proto::Opcode::WRITE, proto::FRAGMENT_BYTES);
    auto changed_total = last;
    changed_total.total_length = proto::FRAGMENT_BYTES + 2;
    changed_total.payload_length = 2;
    EXPECT_EQ(table.admit(value.binding, changed_total, std::span(payload).first(2)), Drop::OPERATION);
    EXPECT_EQ(table.admit(value.binding, last, std::span(payload).first(1)), Drop::NONE);
    EXPECT_EQ(table.take_effect(handle, early), Drop::NONE);
    EXPECT_EQ(table.admit(value.binding, last, std::span(payload).first(1)), Drop::OPERATION);
    EXPECT_EQ(table.finish_effect(early, {.status = 0, .bytes = value.requested, .outcome = Outcome::EXACT}), Drop::NONE);
}

TEST_F(OperationsTest, RecordAdmissionRequiresEveryFullIdentityAndExactPrivateBinding) {
    auto value = spec();
    ops::Handle handle;
    prepare(value, handle);
    std::array<uint8_t, 64> payload{};
    auto const VALID = frame(value, proto::Opcode::WRITE);
    std::vector<proto::Header> invalid;
    auto add = [&](auto mutate) {
        auto record = VALID;
        mutate(record);
        invalid.push_back(record);
    };
    add([](auto& record) { record.operation_id += uint64_t{1} << 16; });
    add([](auto& record) { record.receive_ticket += uint64_t{1} << 16; });
    add([](auto& record) { ++record.rkey; });
    add([](auto& record) { ++record.region_generation; });
    add([](auto& record) { ++record.binding_generation; });
    add([](auto& record) { ++record.capability[0]; });
    add([](auto& record) { ++record.session_id[0]; });
    add([](auto& record) { ++record.channel_id; });
    add([](auto& record) { ++record.src_node; });
    add([](auto& record) { ++record.dst_node; });
    add([](auto& record) { ++record.region_offset; });
    add([](auto& record) {
        record.total_length = 63;
        record.payload_length = 63;
    });
    add([](auto& record) { record.flags = proto::FLAG_RESPONSE; });
    add([](auto& record) { record.service = static_cast<uint8_t>(proto::Service::IPC); });
    add([](auto& record) { record.transport = static_cast<uint8_t>(proto::Transport::IVSHMEM); });
    add([](auto& record) { record.reserved = 1; });
    for (auto const& record : invalid)
        EXPECT_NE(table.admit(value.binding, record, std::span(payload).first(record.payload_length)), Drop::NONE);
    std::vector<reg::Binding> bindings;
    auto alter = [&](auto mutate) {
        auto current = value.binding;
        mutate(current);
        bindings.push_back(current);
    };
    alter([](auto& current) { ++current.secure_incarnation; });
    alter([](auto& current) { ++current.transport_cookie; });
    alter([](auto& current) { ++current.transport_generation; });
    alter([](auto& current) { ++current.channel_cookie; });
    alter([](auto& current) { ++current.channel_generation; });
    for (auto const& current : bindings) EXPECT_NE(table.admit(current, VALID, payload), Drop::NONE);
    EXPECT_EQ(table.slots_[handle.slot_].coverage.received, 0U);
    EXPECT_TRUE(std::all_of(scratch.begin(), scratch.begin() + 64, [](auto byte) { return byte == 0; }));
}

TEST_F(OperationsTest, ServeReadAndNotifyRequireRequestsAndExposeOnlyAppropriateTaskBuffers) {
    for (auto role : {Role::SERVE_READ, Role::RECEIVE_NOTIFY}) {
        auto value = spec(role, role == Role::RECEIVE_NOTIFY ? 0 : 64);
        ops::Handle handle;
        prepare(value, handle);
        ops::Work work;
        EXPECT_EQ(table.take_effect(handle, work), Drop::OPERATION);
        ASSERT_EQ(table.admit(value.binding, frame(value, request_opcode(role)), {}), Drop::NONE);
        ASSERT_EQ(table.take_effect(handle, work), Drop::NONE);
        if (role == Role::SERVE_READ) {
            ASSERT_EQ(work.prepare_buffer().size(), 64U);
            std::fill(work.prepare_buffer().begin(), work.prepare_buffer().end(), 0x39);
        } else {
            EXPECT_TRUE(work.prepare_buffer().empty());
        }
        EXPECT_EQ(table.finish_effect(work, {.status = 0, .bytes = value.requested, .outcome = Outcome::EXACT}), Drop::NONE);
    }
}

TEST_F(OperationsTest, VfsResponseWriteNeedsItsExplicitArmedRoleFlags) {
    auto value = spec();
    value.flags = proto::FLAG_RESPONSE;
    ops::Handle handle;
    prepare(value, handle);
    std::array<uint8_t, 64> payload{};
    auto wrong = frame(value, proto::Opcode::WRITE);
    wrong.flags = 0;
    EXPECT_EQ(table.admit(value.binding, wrong, payload), Drop::OPERATION);
    EXPECT_EQ(table.admit(value.binding, frame(value, proto::Opcode::WRITE), payload), Drop::NONE);
    ops::Work work;
    EXPECT_EQ(table.take_effect(handle, work), Drop::NONE);
    EXPECT_EQ(table.finish_effect(work, {.status = 0, .bytes = 64, .outcome = Outcome::EXACT}), Drop::NONE);
}

TEST_F(OperationsTest, TaskPrepareIsExclusiveAndMustSealBeforeSend) {
    auto value = spec(Role::SEND_WRITE);
    ops::Handle handle;
    prepare(value, handle);
    ops::Work work;
    ASSERT_EQ(table.begin_prepare(handle, work), Drop::NONE);
    EXPECT_EQ(work.prepare_buffer().size(), value.requested);
    ops::Work duplicate;
    EXPECT_EQ(table.begin_prepare(handle, duplicate), Drop::PRESSURE);
    EXPECT_EQ(table.begin_send(handle, duplicate), Drop::OPERATION);
    std::fill(work.prepare_buffer().begin(), work.prepare_buffer().end(), 0x39);
    EXPECT_EQ(table.seal(work), Drop::NONE);
    EXPECT_FALSE(work);
    EXPECT_TRUE(work.prepare_buffer().empty());
    ASSERT_EQ(table.begin_send(handle, duplicate), Drop::NONE);
    EXPECT_TRUE(duplicate.prepare_buffer().empty());
    EXPECT_EQ(table.slots_[handle.slot_].state, ops::State::WAIT_END);
    EXPECT_TRUE(table.slots_[handle.slot_].send_attempted);
    EXPECT_TRUE(std::all_of(duplicate.staging().begin(), duplicate.staging().end(), [](auto byte) { return byte == 0x39; }));
    EXPECT_EQ(table.finish_send(duplicate, 0), Drop::NONE);
    EXPECT_EQ(table.begin_send(handle, duplicate), Drop::OPERATION);
}

TEST_F(OperationsTest, CompletionDuringSendWaitsForActualSendLoanRetirement) {
    auto value = spec(Role::RECEIVE_READ);
    ops::Handle handle;
    prepare(value, handle);
    ops::Work send;
    ASSERT_EQ(table.begin_send(handle, send), Drop::NONE);
    EXPECT_TRUE(send.staging().empty());
    auto end = frame(value, proto::Opcode::READ_END, 0, 0);
    ASSERT_EQ(table.admit(value.binding, end, {}), Drop::NONE);
    ops::Result result{.status = -99, .bytes = 99, .outcome = Outcome::UNCERTAIN};
    EXPECT_EQ(table.observe(handle, result), Drop::PRESSURE);
    EXPECT_EQ(result.status, -99);
    EXPECT_EQ(table.finish_send(send, 0), Drop::NONE);
    EXPECT_EQ(table.observe(handle, result), Drop::NONE);
    EXPECT_EQ(result.status, 0);
    EXPECT_EQ(result.bytes, 0U);
    EXPECT_EQ(result.outcome, Outcome::EXACT);
}

TEST_F(OperationsTest, AuthenticatedExactWriteEndSurvivesLaterLocalSendRetirementError) {
    auto value = spec(Role::SEND_WRITE);
    ops::Handle handle;
    prepare(value, handle);
    ops::Work prepare_work;
    ASSERT_EQ(table.begin_prepare(handle, prepare_work), Drop::NONE);
    std::fill(prepare_work.prepare_buffer().begin(), prepare_work.prepare_buffer().end(), 0x39);
    ASSERT_EQ(table.seal(prepare_work), Drop::NONE);
    ops::Work send;
    ASSERT_EQ(table.begin_send(handle, send), Drop::NONE);
    ASSERT_EQ(table.admit(value.binding, frame(value, proto::Opcode::WRITE_END), {}), Drop::NONE);
    ops::Result result{};
    EXPECT_EQ(table.observe(handle, result), Drop::PRESSURE);
    EXPECT_EQ(table.finish_send(send, -EIO), Drop::NONE);
    EXPECT_EQ(table.observe(handle, result), Drop::NONE);
    EXPECT_EQ(result.status, 0);
    EXPECT_EQ(result.bytes, value.requested);
    EXPECT_EQ(result.outcome, Outcome::EXACT);
}

TEST_F(OperationsTest, WriteEndRequiresFullRequestedIdentityAndDefinesUncertainty) {
    for (int status : {0, -EIO}) {
        auto value = spec(Role::SEND_WRITE);
        ops::Handle handle;
        sent(value, handle);
        auto wrong = frame(value, proto::Opcode::WRITE_END);
        --wrong.total_length;
        EXPECT_EQ(table.admit(value.binding, wrong, {}), Drop::OPERATION);
        auto end = frame(value, proto::Opcode::WRITE_END);
        end.status = status;
        ASSERT_EQ(table.admit(value.binding, end, {}), Drop::NONE);
        ops::Result result{};
        EXPECT_EQ(table.observe(handle, result), Drop::NONE);
        EXPECT_EQ(result.status, status);
        EXPECT_EQ(result.bytes, status == 0 ? value.requested : 0);
        EXPECT_EQ(result.outcome, status == 0 ? Outcome::EXACT : Outcome::UNCERTAIN);
        EXPECT_EQ(table.admit(value.binding, end, {}), Drop::RETIRED);
    }
}

TEST_F(OperationsTest, FailedOrLostWriteSendNeverBecomesExactOrMessageReplayable) {
    for (bool fail_send : {false, true}) {
        auto value = spec(Role::SEND_WRITE);
        ops::Handle handle;
        prepare(value, handle);
        ops::Work write;
        ASSERT_EQ(table.begin_prepare(handle, write), Drop::NONE);
        std::fill(write.prepare_buffer().begin(), write.prepare_buffer().end(), 0x39);
        ASSERT_EQ(table.seal(write), Drop::NONE);
        ops::Work send;
        ASSERT_EQ(table.begin_send(handle, send), Drop::NONE);
        EXPECT_EQ(table.finish_send(send, fail_send ? -EIO : 0), Drop::NONE);
        if (!fail_send) EXPECT_EQ(table.expire(value.deadline_us, -ETIMEDOUT), Drop::NONE);
        ops::Result result{};
        EXPECT_EQ(table.observe(handle, result), Drop::NONE);
        EXPECT_LT(result.status, 0);
        EXPECT_EQ(result.outcome, Outcome::UNCERTAIN);
        EXPECT_EQ(table.begin_send(handle, send), Drop::RETIRED);
    }
}

TEST_F(OperationsTest, ReadEndFirstPartialReadNeedsExactCoverageAndImmutableResultLoan) {
    auto value = spec(Role::RECEIVE_READ, 2 * proto::FRAGMENT_BYTES);
    ops::Handle handle;
    sent(value, handle);
    uint32_t const ACTUAL = proto::FRAGMENT_BYTES + 17;
    auto end = frame(value, proto::Opcode::READ_END, 0, ACTUAL);
    ASSERT_EQ(table.admit(value.binding, end, {}), Drop::NONE);
    ops::Result result{};
    EXPECT_EQ(table.observe(handle, result), Drop::PRESSURE);
    std::vector<uint8_t> oracle(ACTUAL);
    for (size_t index = 0; index < oracle.size(); ++index) oracle[index] = static_cast<uint8_t>(index * 7U + 19U);
    for (uint32_t offset : {proto::FRAGMENT_BYTES, 0U}) {
        auto data = frame(value, proto::Opcode::READ_DATA, offset, ACTUAL);
        ASSERT_EQ(table.admit(value.binding, data, std::span(oracle).subspan(offset, data.payload_length)), Drop::NONE);
    }
    ops::Work read;
    ASSERT_EQ(table.take_result(handle, read, result), Drop::NONE);
    EXPECT_EQ(result.bytes, ACTUAL);
    EXPECT_EQ(result.outcome, Outcome::EXACT);
    EXPECT_TRUE(read.prepare_buffer().empty());
    EXPECT_TRUE(std::equal(oracle.begin(), oracle.end(), read.staging().begin(), read.staging().end()));
    handle.reset();
    ops::EraseWork blocked;
    EXPECT_EQ(table.begin_erase(blocked), Drop::PRESSURE);
    read.reset();
    EXPECT_EQ(table.begin_erase(blocked), Drop::NONE);
    std::fill(blocked.buffer().begin(), blocked.buffer().end(), 0);
    EXPECT_EQ(table.finish_erase(blocked), Drop::NONE);
}

TEST_F(OperationsTest, ReadDataAloneIsNotCompletionAndReturnedLengthCannotChange) {
    auto value = spec(Role::RECEIVE_READ, 64);
    ops::Handle handle;
    sent(value, handle);
    std::array<uint8_t, 32> bytes{};
    ASSERT_EQ(table.admit(value.binding, frame(value, proto::Opcode::READ_DATA, 0, 32), bytes), Drop::NONE);
    ops::Result result{};
    EXPECT_EQ(table.observe(handle, result), Drop::PRESSURE);
    EXPECT_EQ(table.admit(value.binding, frame(value, proto::Opcode::READ_END, 0, 31), {}), Drop::OPERATION);
    EXPECT_EQ(table.admit(value.binding, frame(value, proto::Opcode::READ_END, 0, 65), {}), Drop::RANGE);
    EXPECT_EQ(table.admit(value.binding, frame(value, proto::Opcode::READ_END, 0, 32), {}), Drop::NONE);
    EXPECT_EQ(table.observe(handle, result), Drop::NONE);
    EXPECT_EQ(result.bytes, 32U);
}

TEST_F(OperationsTest, PartialReadErrorNeedsCoveredBytesAndCannotInventEarlyCompletion) {
    auto value = spec(Role::RECEIVE_READ, 64);
    ops::Handle handle;
    sent(value, handle);
    auto end = frame(value, proto::Opcode::READ_END, 0, 17);
    end.status = -EIO;
    ASSERT_EQ(table.admit(value.binding, end, {}), Drop::NONE);
    EXPECT_EQ(table.admit(value.binding, end, {}), Drop::DUPLICATE);
    ops::Result result{};
    EXPECT_EQ(table.observe(handle, result), Drop::PRESSURE);
    std::array<uint8_t, 17> payload{};
    payload.fill(0x39);
    EXPECT_EQ(table.admit(value.binding, frame(value, proto::Opcode::READ_DATA, 0, 17), payload), Drop::NONE);
    ops::Work loan;
    ASSERT_EQ(table.take_result(handle, loan, result), Drop::NONE);
    EXPECT_EQ(result.status, -EIO);
    EXPECT_EQ(result.bytes, 17U);
    EXPECT_EQ(result.outcome, Outcome::EXACT);
    EXPECT_TRUE(std::equal(payload.begin(), payload.end(), loan.staging().begin(), loan.staging().end()));
}

TEST_F(OperationsTest, QueuedCancellationAndRegistryRevokePreventEffects) {
    auto value = spec();
    ops::Handle handle;
    ready_write(value, handle);
    ASSERT_EQ(registry.revoke(value.grant.rkey, value.grant.generation), Drop::NONE);
    ops::Work work;
    EXPECT_EQ(table.take_effect(handle, work), Drop::RETIRED);
    EXPECT_FALSE(work);
    EXPECT_EQ(backing[0], 0xa5);
    EXPECT_EQ(table.cancel(handle, -ECANCELED), Drop::NONE);
    ops::Result result{};
    EXPECT_EQ(table.observe(handle, result), Drop::NONE);
    EXPECT_EQ(result.outcome, Outcome::NO_EFFECT);
}

TEST_F(OperationsTest, RegistryClaimPressureRetainsReadyOperationAndNoWork) {
    auto value = spec();
    ops::Handle handle;
    ready_write(value, handle);
    ASSERT_FALSE(registry.guard_.test_and_set());
    ops::Work work;
    EXPECT_EQ(table.take_effect(handle, work), Drop::PRESSURE);
    EXPECT_FALSE(work);
    EXPECT_EQ(table.slots_[handle.slot_].state, ops::State::READY);
    EXPECT_EQ(table.slots_[handle.slot_].work.load(), 0U);
    registry.guard_.clear();
    EXPECT_EQ(table.take_effect(handle, work), Drop::NONE);
    EXPECT_EQ(table.finish_effect(work, {.status = 0, .bytes = 64, .outcome = Outcome::EXACT}), Drop::NONE);
}

TEST_F(OperationsTest, CloseDuringClaimedEffectPreservesActualNativeOutcomeAndPinsBacking) {
    auto value = spec();
    ops::Handle handle;
    ready_write(value, handle);
    ops::Work work;
    ASSERT_EQ(table.take_effect(handle, work), Drop::NONE);
    ASSERT_EQ(table.cancel(handle, -ECANCELED), Drop::NONE);
    EXPECT_EQ(table.close_binding(value.binding, -EIO), Drop::NONE);
    EXPECT_EQ(table.expire(value.deadline_us, -ETIMEDOUT), Drop::NONE);
    EXPECT_EQ(registry.revoke(value.grant.rkey, value.grant.generation), Drop::NONE);
    reg::ReleasedRegion released{};
    EXPECT_EQ(registry.reclaim(value.grant.rkey, value.grant.generation, released), Drop::PRESSURE);
    ops::Result result{};
    EXPECT_EQ(table.observe(handle, result), Drop::PRESSURE);
    ASSERT_EQ(work.backing().backing, backing.data());
    std::memcpy(work.backing().backing, work.staging().data(), work.staging().size());
    ASSERT_FALSE(table.guard_.test_and_set());
    EXPECT_EQ(table.finish_effect(work, {.status = -EIO, .bytes = 17, .outcome = Outcome::EXACT}), Drop::PRESSURE);
    EXPECT_TRUE(work);
    table.guard_.clear();
    ASSERT_EQ(table.finish_effect(work, {.status = -EIO, .bytes = 17, .outcome = Outcome::EXACT}), Drop::NONE);
    EXPECT_EQ(table.observe(handle, result), Drop::NONE);
    EXPECT_EQ(result.status, -EIO);
    EXPECT_EQ(result.bytes, 17U);
    EXPECT_EQ(result.outcome, Outcome::EXACT);
    EXPECT_EQ(backing[0], 0x39);
}

TEST_F(OperationsTest, AbandonedEffectIsUncertainAndReleaseNeedsNeitherGuard) {
    auto value = spec();
    ops::Handle handle;
    ready_write(value, handle);
    ops::Work work;
    ASSERT_EQ(table.take_effect(handle, work), Drop::NONE);
    ops::Work moved(std::move(work));
    EXPECT_FALSE(work);
    EXPECT_TRUE(zero(work.spec()));
    ops::Handle moved_handle(std::move(handle));
    EXPECT_FALSE(handle);
    ASSERT_FALSE(table.guard_.test_and_set());
    ASSERT_FALSE(registry.guard_.test_and_set());
    std::thread release([owned = std::move(moved)]() mutable { owned.reset(); });
    release.join();
    EXPECT_EQ(table.slots_[moved_handle.slot_].work.load(), 0U);
    registry.guard_.clear();
    table.guard_.clear();
    ops::Result result{};
    EXPECT_EQ(table.observe(moved_handle, result), Drop::NONE);
    EXPECT_LT(result.status, 0);
    EXPECT_EQ(result.outcome, Outcome::UNCERTAIN);
    ASSERT_FALSE(table.guard_.test_and_set());
    auto const INDEX = moved_handle.slot_;
    moved_handle.reset();
    moved_handle.reset();
    EXPECT_EQ(table.slots_[INDEX].handles.load(), 0U);
    table.guard_.clear();
}

TEST_F(OperationsTest, MoveAssignmentRetiresOldLoanOnceAndErasesMovedAuthority) {
    auto outgoing = spec(Role::SEND_WRITE);
    ops::Handle old_handle;
    prepare(outgoing, old_handle);
    ops::Work target;
    ASSERT_EQ(table.begin_prepare(old_handle, target), Drop::NONE);
    auto incoming = spec();
    ops::Handle handle;
    ready_write(incoming, handle);
    ops::Work source;
    ASSERT_EQ(table.take_effect(handle, source), Drop::NONE);
    target = std::move(source);
    EXPECT_FALSE(source);
    EXPECT_TRUE(zero(source.spec()));
    EXPECT_EQ(table.slots_[old_handle.slot_].work.load(), 0U);
    ops::Result old_result{};
    EXPECT_EQ(table.observe(old_handle, old_result), Drop::NONE);
    EXPECT_EQ(old_result.outcome, Outcome::NO_EFFECT);
    EXPECT_EQ(table.finish_effect(target, {.status = 0, .bytes = 64, .outcome = Outcome::EXACT}), Drop::NONE);
    auto const OLD_INDEX = old_handle.slot_;
    old_handle = std::move(handle);
    EXPECT_FALSE(handle);
    EXPECT_EQ(table.slots_[OLD_INDEX].handles.load(), 0U);
    auto* self = &old_handle;
    old_handle = std::move(*self);
    EXPECT_EQ(table.slots_[old_handle.slot_].handles.load(), 1U);
}

TEST_F(OperationsTest, NativeWorkerRetainsScratchAndBackingThroughCloseUntilActualCompletion) {
    auto value = spec();
    ops::Handle handle;
    ready_write(value, handle);
    ops::Work work;
    ASSERT_EQ(table.take_effect(handle, work), Drop::NONE);
    std::atomic<bool> ready{false}, complete{false};
    Drop finished = Drop::PRESSURE;
    std::thread native([owned = std::move(work), &ready, &complete, &finished, this]() mutable {
        ready.store(true, std::memory_order_release);
        while (!complete.load(std::memory_order_acquire)) std::this_thread::yield();
        std::memcpy(owned.backing().backing, owned.staging().data(), owned.staging().size());
        for (unsigned attempt = 0; attempt < 10000 && finished == Drop::PRESSURE; ++attempt) {
            finished = table.finish_effect(owned, {.status = 0, .bytes = 64, .outcome = Outcome::EXACT});
            if (finished == Drop::PRESSURE) std::this_thread::yield();
        }
    });
    while (!ready.load(std::memory_order_acquire)) std::this_thread::yield();
    EXPECT_EQ(table.cancel(handle, -ECANCELED), Drop::NONE);
    EXPECT_EQ(registry.revoke(value.grant.rkey, value.grant.generation), Drop::NONE);
    reg::ReleasedRegion released{};
    EXPECT_EQ(registry.reclaim(value.grant.rkey, value.grant.generation, released), Drop::PRESSURE);
    ops::Result result{};
    EXPECT_EQ(table.observe(handle, result), Drop::PRESSURE);
    EXPECT_EQ(backing[0], 0xa5);
    complete.store(true, std::memory_order_release);
    native.join();
    ASSERT_EQ(finished, Drop::NONE);
    EXPECT_EQ(table.observe(handle, result), Drop::NONE);
    EXPECT_EQ(result.outcome, Outcome::EXACT);
    EXPECT_EQ(backing[0], 0x39);
}

TEST_F(OperationsTest, CancelledPrepareLoanCannotPublishOrBeObservedUntilRetired) {
    auto value = spec(Role::SEND_WRITE);
    ops::Handle handle;
    prepare(value, handle);
    ops::Work work;
    ASSERT_EQ(table.begin_prepare(handle, work), Drop::NONE);
    EXPECT_EQ(table.cancel(handle, -ECANCELED), Drop::NONE);
    EXPECT_EQ(table.seal(work), Drop::RETIRED);
    ops::Result result{};
    EXPECT_EQ(table.observe(handle, result), Drop::PRESSURE);
    work.reset();
    EXPECT_EQ(table.observe(handle, result), Drop::NONE);
    EXPECT_EQ(result.status, -ECANCELED);
    EXPECT_EQ(result.outcome, Outcome::NO_EFFECT);
}

TEST_F(OperationsTest, ErasureNeedsAllPinsAndExplicitTaskWipeBeforeReuse) {
    auto value = spec(Role::SEND_WRITE, proto::MAX_TRANSFER_BYTES);
    ops::Handle handle;
    sent(value, handle);
    ASSERT_EQ(table.admit(value.binding, frame(value, proto::Opcode::WRITE_END), {}), Drop::NONE);
    ops::EraseWork erase;
    EXPECT_EQ(table.begin_erase(erase), Drop::PRESSURE);
    auto const OLD_GENERATION = handle.generation_;
    handle.reset();
    ASSERT_EQ(table.begin_erase(erase), Drop::NONE);
    EXPECT_EQ(table.slots_[erase.slot_].state, ops::State::ERASING);
    EXPECT_TRUE(std::all_of(erase.buffer().begin(), erase.buffer().end(), [](auto byte) { return byte == 0x39; }));
    std::fill(erase.buffer().begin(), erase.buffer().end(), 0);
    ASSERT_FALSE(table.guard_.test_and_set());
    EXPECT_EQ(table.finish_erase(erase), Drop::PRESSURE);
    EXPECT_TRUE(erase);
    table.guard_.clear();
    auto const INDEX = erase.slot_;
    EXPECT_EQ(table.finish_erase(erase), Drop::NONE);
    EXPECT_FALSE(erase);
    EXPECT_TRUE(zero(table.slots_[INDEX].spec));
    EXPECT_TRUE(zero(table.slots_[INDEX].coverage));
    EXPECT_TRUE(zero(table.slots_[INDEX].result));
    auto fresh = spec(Role::RECEIVE_READ);
    ops::Handle replacement;
    prepare(fresh, replacement);
    EXPECT_EQ(replacement.slot_, INDEX);
    EXPECT_GT(replacement.generation_, OLD_GENERATION);
    EXPECT_EQ(table.admit(value.binding, frame(value, proto::Opcode::WRITE_END), {}), Drop::OPERATION);
    EXPECT_TRUE(std::all_of(scratch.begin(), scratch.begin() + proto::MAX_TRANSFER_BYTES, [](auto byte) { return byte == 0; }));
}

TEST_F(OperationsTest, RegistryLeaseRemainsPinnedThroughEraseWorkAndAbandonQuarantines) {
    auto value = spec(Role::RECEIVE_NOTIFY, 0);
    ops::Handle handle;
    prepare(value, handle);
    EXPECT_EQ(table.cancel(handle, -ECANCELED), Drop::NONE);
    handle.reset();
    {
        ops::EraseWork erase;
        ASSERT_EQ(table.begin_erase(erase), Drop::NONE);
        reg::ReleasedRegion released{};
        EXPECT_EQ(registry.reclaim(value.grant.rkey, value.grant.generation, released), Drop::PRESSURE);
    }
    ops::Statistics stats{};
    EXPECT_EQ(table.statistics(stats), Drop::NONE);
    EXPECT_EQ(stats.erasing, 1U);
    ops::EraseWork replacement;
    EXPECT_EQ(table.begin_erase(replacement), Drop::PRESSURE);
    reg::ReleasedRegion released{};
    EXPECT_EQ(registry.reclaim(value.grant.rkey, value.grant.generation, released), Drop::NONE);
    EXPECT_EQ(table.shutdown(), Drop::PRESSURE);
    quarantined = true;
}

TEST_F(OperationsTest, BindingCloseIsExactAndDeadlineClosesOnlyDueSlots) {
    auto first = spec(Role::RECEIVE_READ);
    auto second = spec(Role::RECEIVE_READ);
    ++second.binding.channel_generation;
    second.deadline_us = 1001;
    ops::Handle one, two;
    prepare(first, one);
    prepare(second, two);
    EXPECT_EQ(table.close_binding(first.binding, -ECANCELED), Drop::NONE);
    ops::Result result{};
    EXPECT_EQ(table.observe(one, result), Drop::NONE);
    EXPECT_EQ(result.outcome, Outcome::NO_EFFECT);
    EXPECT_EQ(table.observe(two, result), Drop::PRESSURE);
    EXPECT_EQ(table.expire(1000, -ETIMEDOUT), Drop::NONE);
    EXPECT_EQ(table.observe(two, result), Drop::PRESSURE);
    EXPECT_EQ(table.expire(1001, -ETIMEDOUT), Drop::NONE);
    EXPECT_EQ(table.observe(two, result), Drop::NONE);
    EXPECT_EQ(result.status, -ETIMEDOUT);
}

TEST_F(OperationsTest, EveryMetadataEntryReturnsPressureOnceAndPreservesOutputs) {
    auto value = spec();
    ops::Handle handle;
    ready_write(value, handle);
    auto pin = lease(value);
    ops::Handle denied;
    ops::Work work;
    ops::EraseWork erase;
    ops::Result result{.status = -99, .bytes = 99};
    ops::Statistics stats{.live = 99};
    ASSERT_FALSE(table.guard_.test_and_set());
    EXPECT_EQ(table.initialize(scratch), Drop::PRESSURE);
    EXPECT_EQ(table.prepare(value, std::move(pin), denied), Drop::PRESSURE);
    EXPECT_TRUE(pin);
    EXPECT_EQ(table.arm(handle, 22), Drop::PRESSURE);
    EXPECT_EQ(table.begin_prepare(handle, work), Drop::PRESSURE);
    EXPECT_EQ(table.seal(work), Drop::PRESSURE);
    EXPECT_EQ(table.begin_send(handle, work), Drop::PRESSURE);
    EXPECT_EQ(table.finish_send(work, 0), Drop::PRESSURE);
    EXPECT_EQ(table.admit(value.binding, frame(value, proto::Opcode::READ_END, 0, 0), {}), Drop::PRESSURE);
    EXPECT_EQ(table.take_effect(handle, work), Drop::PRESSURE);
    EXPECT_EQ(table.finish_effect(work, {}), Drop::PRESSURE);
    EXPECT_EQ(table.observe(handle, result), Drop::PRESSURE);
    EXPECT_EQ(table.take_result(handle, work, result), Drop::PRESSURE);
    EXPECT_EQ(table.cancel(handle, -ECANCELED), Drop::PRESSURE);
    EXPECT_EQ(table.close_binding(value.binding, -EIO), Drop::PRESSURE);
    EXPECT_EQ(table.expire(UINT64_MAX, -ETIMEDOUT), Drop::PRESSURE);
    EXPECT_EQ(table.begin_erase(erase), Drop::PRESSURE);
    EXPECT_EQ(table.finish_erase(erase), Drop::PRESSURE);
    EXPECT_EQ(table.statistics(stats), Drop::PRESSURE);
    EXPECT_EQ(table.shutdown(), Drop::PRESSURE);
    EXPECT_EQ(result.status, -99);
    EXPECT_EQ(result.bytes, 99U);
    EXPECT_EQ(stats.live, 99U);
    table.guard_.clear();
}

TEST_F(OperationsTest, ClaimCancelAndRevokeRaceHasOnlyNoEffectOrRetainedRealOutcome) {
    for (unsigned iteration = 0; iteration < 64; ++iteration) {
        auto value = spec();
        ops::Handle handle;
        ready_write(value, handle);
        ops::Work work;
        Drop claim = Drop::PRESSURE, cancel = Drop::PRESSURE, revoke = Drop::PRESSURE;
        std::barrier start(4);
        auto retry = [&](auto call, Drop& result) {
            start.arrive_and_wait();
            for (unsigned attempt = 0; attempt < 10000 && result == Drop::PRESSURE; ++attempt) {
                result = call();
                if (result == Drop::PRESSURE) std::this_thread::yield();
            }
        };
        std::thread claimant([&] { retry([&] { return table.take_effect(handle, work); }, claim); });
        std::thread canceller([&] { retry([&] { return table.cancel(handle, -ECANCELED); }, cancel); });
        std::thread revoker([&] { retry([&] { return registry.revoke(value.grant.rkey, value.grant.generation); }, revoke); });
        start.arrive_and_wait();
        claimant.join();
        canceller.join();
        revoker.join();
        ASSERT_EQ(cancel, Drop::NONE);
        ASSERT_EQ(revoke, Drop::NONE);
        ASSERT_TRUE(claim == Drop::NONE || claim == Drop::RETIRED);
        ops::Result result{};
        if (claim == Drop::NONE) {
            ASSERT_TRUE(work);
            EXPECT_EQ(table.observe(handle, result), Drop::PRESSURE);
            ASSERT_EQ(table.finish_effect(work, {.status = 0, .bytes = 64, .outcome = Outcome::EXACT}), Drop::NONE);
            EXPECT_EQ(table.observe(handle, result), Drop::NONE);
            EXPECT_EQ(result.outcome, Outcome::EXACT);
        } else {
            EXPECT_FALSE(work);
            EXPECT_EQ(table.observe(handle, result), Drop::NONE);
            EXPECT_EQ(result.outcome, Outcome::NO_EFFECT);
        }
        handle.reset();
        erase_all();
        reg::ReleasedRegion released{};
        ASSERT_EQ(registry.reclaim(value.grant.rkey, value.grant.generation, released), Drop::NONE);
    }
}
