#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <barrier>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <net/wki/rdma_protocol.hpp>
#include <thread>
#include <utility>
#include <vector>

// Force terminal allocator/count and contention edges without a production
// hook. Standard/protocol headers are already included before this scope.
#define private public
#include <net/wki/rdma_registry.hpp>
#undef private

namespace reg = ker::net::wki::rdma_registry;
namespace proto = ker::net::wki::rdma_protocol;
namespace crypto = ker::net::wki::rdma_crypto;
using Drop = proto::Drop;

namespace {

auto private_root(uint8_t seed = 17) -> crypto::Key {
    crypto::Key result{};
    for (size_t index = 0; index < result.size(); ++index) result[index] = static_cast<uint8_t>(seed + index * 7U);
    return result;
}

auto binding() -> reg::Binding {
    reg::Binding result{.owner_node = 1,
                        .peer_node = 2,
                        .secure_incarnation = 3,
                        .transport_cookie = 0x1230,
                        .transport_generation = 4,
                        .channel_cookie = 0x2340,
                        .channel_generation = 5,
                        .channel_id = 6,
                        .transport = proto::Transport::ROCE,
                        .service = proto::Service::VFS};
    result.session_id.fill(0x7a);
    return result;
}

auto incoming(const reg::Binding& authority, const proto::RegionDescriptor& descriptor, proto::Opcode opcode = proto::Opcode::WRITE)
    -> proto::Header {
    proto::Header result{};
    result.transport = static_cast<uint8_t>(authority.transport);
    result.service = static_cast<uint8_t>(authority.service);
    result.opcode = static_cast<uint8_t>(opcode);
    result.src_node = authority.peer_node;
    result.dst_node = authority.owner_node;
    result.channel_id = authority.channel_id;
    result.session_id = descriptor.session_id;
    result.region_generation = descriptor.generation;
    result.capability = descriptor.capability;
    result.operation_id = 12;
    result.binding_generation = descriptor.binding_generation;
    result.receive_ticket = 13;
    result.counter = 14;
    result.total_length = opcode == proto::Opcode::NOTIFY           ? 0
                          : descriptor.size > proto::FRAGMENT_BYTES ? proto::FRAGMENT_BYTES
                                                                    : descriptor.size;
    result.payload_length = opcode == proto::Opcode::WRITE ? result.total_length : 0;
    result.rkey = descriptor.rkey;
    return result;
}

template <class Value>
auto all_zero(const Value& value) -> bool {
    auto const* bytes = reinterpret_cast<const uint8_t*>(&value);
    for (size_t index = 0; index < sizeof(value); ++index) {
        if (bytes[index] != 0) return false;
    }
    return true;
}

class RegistryTest : public testing::Test {
   protected:
    void SetUp() override {
        storage.fill(0xa5);
        ASSERT_EQ(registry.initialize(1, private_root()), Drop::NONE);
    }

    void TearDown() override {
        // An assertion failure must not leave an outstanding lease on a
        // destroyed registry. Case-local leases unwind before fixture teardown.
        for (auto& slot : registry.slots_) {
            if (!slot.active) continue;
            reg::ReleasedRegion released{};
            EXPECT_EQ(registry.reclaim(slot.descriptor.rkey, slot.descriptor.generation, released), Drop::NONE);
        }
        EXPECT_EQ(registry.shutdown(), Drop::NONE);
    }

    auto registration(uint8_t permissions = proto::PERMISSIONS) -> reg::Registration {
        return {.binding = binding(),
                .backing = storage.data(),
                .size = static_cast<uint32_t>(storage.size()),
                .binding_generation = 7,
                .permissions = permissions};
    }

    auto publish(uint8_t permissions = proto::PERMISSIONS) -> proto::RegionDescriptor {
        proto::RegionDescriptor result{};
        EXPECT_EQ(registry.register_region(registration(permissions), result), Drop::NONE);
        return result;
    }

    reg::Registry registry;
    std::array<uint8_t, 64> storage{};
};

}  // namespace

TEST(WkiRdmaRegistry, InitializationIsOneShotAndInvalidAttemptsDoNotConsumeIt) {
    reg::Registry registry;
    EXPECT_EQ(registry.initialize(0, private_root()), Drop::IDENTITY);
    EXPECT_EQ(registry.initialize(UINT16_MAX, private_root()), Drop::IDENTITY);
    EXPECT_EQ(registry.initialize(1, {}), Drop::GRANT);
    EXPECT_FALSE(registry.initialized_once_);
    EXPECT_TRUE(all_zero(registry.capability_root_));
    EXPECT_EQ(registry.initialize(1, private_root()), Drop::NONE);
    EXPECT_EQ(registry.initialize(1, private_root(29)), Drop::RETIRED);
    EXPECT_EQ(registry.capability_root_, private_root());
    EXPECT_EQ(registry.shutdown(), Drop::NONE);
    EXPECT_TRUE(all_zero(registry.capability_root_));
    EXPECT_EQ(registry.shutdown(), Drop::NONE);
    EXPECT_EQ(registry.initialize(1, private_root(29)), Drop::RETIRED);
}

TEST_F(RegistryTest, RegistrationRequiresEveryPrivateIdentityAndCheckedBacking) {
    auto const VALID = registration();
    std::vector<reg::Registration> invalid;
    auto add = [&](auto mutate) {
        auto value = VALID;
        mutate(value);
        invalid.push_back(value);
    };
    add([](auto& value) { value.binding.owner_node = 2; });
    add([](auto& value) { value.binding.peer_node = 0; });
    add([](auto& value) { value.binding.peer_node = UINT16_MAX; });
    add([](auto& value) { value.binding.peer_node = value.binding.owner_node; });
    add([](auto& value) { value.binding.session_id = {}; });
    add([](auto& value) { value.binding.secure_incarnation = 0; });
    add([](auto& value) { value.binding.transport_cookie = 0; });
    add([](auto& value) { value.binding.transport_generation = 0; });
    add([](auto& value) { value.binding.channel_cookie = 0; });
    add([](auto& value) { value.binding.channel_generation = 0; });
    add([](auto& value) { value.binding.channel_id = 0; });
    add([](auto& value) { value.binding.transport = proto::Transport::NONE; });
    add([](auto& value) { value.binding.service = proto::Service::NONE; });
    add([](auto& value) { value.binding.service = static_cast<proto::Service>(255); });
    add([](auto& value) { value.backing = nullptr; });
    add([](auto& value) { value.size = 0; });
    add([](auto& value) { value.binding_generation = 0; });
    add([](auto& value) { value.permissions = 0; });
    add([](auto& value) { value.permissions = 0x80; });
    add([](auto& value) { value.backing = reinterpret_cast<void*>(UINTPTR_MAX - 2); });
    for (const auto& value : invalid) {
        proto::RegionDescriptor sentinel{};
        sentinel.rkey = 99;
        auto const BEFORE = sentinel;
        EXPECT_NE(registry.register_region(value, sentinel), Drop::NONE);
        EXPECT_EQ(std::memcmp(&sentinel, &BEFORE, sizeof(sentinel)), 0);
    }
    reg::Statistics stats{};
    ASSERT_EQ(registry.statistics(stats), Drop::NONE);
    EXPECT_EQ(stats.live, 0U);
    EXPECT_EQ(stats.bytes, 0U);
    EXPECT_EQ(registry.next_rkey_, 1U);
    EXPECT_EQ(registry.next_generation_, 1U);
}

TEST_F(RegistryTest, CapabilitiesComeFromPrivateRootAndExactRegistration) {
    auto const FIRST = publish();
    proto::Capability expected{};
    ASSERT_TRUE(proto::derive_capability(private_root(), 1, 2, binding().transport_generation, FIRST, expected));
    EXPECT_EQ(FIRST.capability, expected);
    auto const SECOND = publish();
    EXPECT_NE(FIRST.rkey, SECOND.rkey);
    EXPECT_NE(FIRST.generation, SECOND.generation);
    EXPECT_NE(FIRST.capability, SECOND.capability);
    reg::Registry other;
    ASSERT_EQ(other.initialize(1, private_root(29)), Drop::NONE);
    proto::RegionDescriptor different{};
    ASSERT_EQ(other.register_region(registration(), different), Drop::NONE);
    EXPECT_EQ(FIRST.rkey, different.rkey);
    EXPECT_EQ(FIRST.generation, different.generation);
    EXPECT_NE(FIRST.capability, different.capability);
    reg::ReleasedRegion released{};
    EXPECT_EQ(other.reclaim(different.rkey, different.generation, released), Drop::NONE);
    EXPECT_EQ(other.shutdown(), Drop::NONE);
}

TEST_F(RegistryTest, IncomingAuthorityIncludesNodesAndEveryLocalBindingField) {
    auto const DESCRIPTOR = publish();
    auto const HEADER = incoming(binding(), DESCRIPTOR);
    std::vector<reg::Binding> invalid;
    auto add = [&](auto mutate) {
        auto value = binding();
        mutate(value);
        invalid.push_back(value);
    };
    add([](auto& value) { ++value.owner_node; });
    add([](auto& value) { ++value.peer_node; });
    add([](auto& value) { ++value.session_id[0]; });
    add([](auto& value) { ++value.secure_incarnation; });
    add([](auto& value) { ++value.transport_cookie; });
    add([](auto& value) { ++value.transport_generation; });
    add([](auto& value) { ++value.channel_cookie; });
    add([](auto& value) { ++value.channel_generation; });
    add([](auto& value) { ++value.channel_id; });
    add([](auto& value) { value.transport = proto::Transport::IVSHMEM; });
    add([](auto& value) { value.service = proto::Service::BLOCK; });
    for (const auto& value : invalid) {
        reg::Lease lease;
        EXPECT_NE(registry.admit(value, HEADER, lease), Drop::NONE);
        EXPECT_FALSE(lease);
        EXPECT_EQ(lease.descriptor().rkey, 0U);
        EXPECT_TRUE(all_zero(lease.descriptor().capability));
    }
    for (bool change_source : {false, true}) {
        auto header = HEADER;
        if (change_source)
            ++header.src_node;
        else
            ++header.dst_node;
        reg::Lease lease;
        EXPECT_EQ(registry.admit(binding(), header, lease), Drop::IDENTITY);
    }
    reg::Statistics stats{};
    ASSERT_EQ(registry.statistics(stats), Drop::NONE);
    EXPECT_EQ(stats.references, 0U);
    EXPECT_EQ(stats.claims, 0U);
    EXPECT_EQ(storage[0], 0xa5);
}

TEST_F(RegistryTest, GrantRangeCanonicalFieldsAndResponseOpcodesFailBeforePin) {
    auto const DESCRIPTOR = publish();
    auto const VALID = incoming(binding(), DESCRIPTOR);
    std::vector<proto::Header> invalid;
    auto add = [&](auto mutate) {
        auto value = VALID;
        mutate(value);
        invalid.push_back(value);
    };
    add([](auto& value) { ++value.rkey; });
    add([](auto& value) { ++value.region_generation; });
    add([](auto& value) { ++value.binding_generation; });
    add([](auto& value) { ++value.capability[0]; });
    add([](auto& value) { ++value.session_id[0]; });
    add([](auto& value) { ++value.channel_id; });
    add([](auto& value) { value.service = static_cast<uint8_t>(proto::Service::BLOCK); });
    add([](auto& value) { value.transport = static_cast<uint8_t>(proto::Transport::IVSHMEM); });
    add([](auto& value) { value.region_offset = 1; });
    add([](auto& value) { value.region_offset = UINT64_MAX; });
    add([](auto& value) { value.reserved = 1; });
    add([](auto& value) { value.reserved2 = 1; });
    add([](auto& value) { value.flags = 0x8000; });
    add([](auto& value) { value.counter = 0; });
    add([](auto& value) { value.operation_id = 0; });
    add([](auto& value) { value.receive_ticket = 0; });
    add([](auto& value) { value.payload_length = UINT32_MAX; });
    add([](auto& value) { value.opcode = 255; });
    for (auto opcode : {proto::Opcode::READ_DATA, proto::Opcode::READ_END, proto::Opcode::WRITE_END}) {
        add([&](auto& value) {
            value.opcode = static_cast<uint8_t>(opcode);
            value.flags = proto::FLAG_RESPONSE;
            if (opcode != proto::Opcode::READ_DATA) value.payload_length = 0;
        });
    }
    for (const auto& value : invalid) {
        reg::Lease lease;
        EXPECT_NE(registry.admit(binding(), value, lease), Drop::NONE);
        EXPECT_FALSE(lease);
        EXPECT_EQ(lease.memory().backing, nullptr);
    }
    reg::Statistics stats{};
    ASSERT_EQ(registry.statistics(stats), Drop::NONE);
    EXPECT_EQ(stats.references, 0U);
}

TEST_F(RegistryTest, IncomingReadWriteNotifyRequireIndependentPermissions) {
    for (auto opcode : {proto::Opcode::READ, proto::Opcode::WRITE, proto::Opcode::NOTIFY}) {
        uint8_t const PERMISSION = opcode == proto::Opcode::READ    ? proto::PERMIT_READ
                                   : opcode == proto::Opcode::WRITE ? proto::PERMIT_WRITE
                                                                    : proto::PERMIT_NOTIFY;
        auto const DESCRIPTOR = publish(PERMISSION);
        reg::Lease accepted;
        ASSERT_EQ(registry.admit(binding(), incoming(binding(), DESCRIPTOR, opcode), accepted), Drop::NONE);
        EXPECT_EQ(accepted.memory().backing, nullptr);
        EXPECT_EQ(accepted.claim(), Drop::NONE);
        EXPECT_EQ(accepted.memory().backing, storage.data());
        for (auto other : {proto::Opcode::READ, proto::Opcode::WRITE, proto::Opcode::NOTIFY}) {
            if (other == opcode) continue;
            reg::Lease denied;
            EXPECT_EQ(registry.admit(binding(), incoming(binding(), DESCRIPTOR, other), denied), Drop::PERMISSION);
        }
    }
}

TEST_F(RegistryTest, ClaimIsIdempotentAndAdmissionCannotReplaceAnOwnedLease) {
    auto const DESCRIPTOR = publish();
    reg::Lease lease;
    ASSERT_EQ(registry.admit(binding(), incoming(binding(), DESCRIPTOR), lease), Drop::NONE);
    EXPECT_EQ(registry.admit(binding(), incoming(binding(), DESCRIPTOR), lease), Drop::OPERATION);
    EXPECT_EQ(lease.claim(), Drop::NONE);
    EXPECT_EQ(lease.claim(), Drop::NONE);
    reg::Statistics stats{};
    ASSERT_EQ(registry.statistics(stats), Drop::NONE);
    EXPECT_EQ(stats.references, 1U);
    EXPECT_EQ(stats.claims, 1U);
}

TEST_F(RegistryTest, ReclaimClosesQueuedClaimsAndKeepsClaimedBackingPinned) {
    auto const DESCRIPTOR = publish();
    reg::Lease claimed, queued;
    ASSERT_EQ(registry.admit(binding(), incoming(binding(), DESCRIPTOR), claimed), Drop::NONE);
    ASSERT_EQ(registry.admit(binding(), incoming(binding(), DESCRIPTOR), queued), Drop::NONE);
    ASSERT_EQ(claimed.claim(), Drop::NONE);
    reg::ReleasedRegion released{.backing = reinterpret_cast<void*>(0x3450), .size = 99};
    EXPECT_EQ(registry.reclaim(DESCRIPTOR.rkey, DESCRIPTOR.generation, released), Drop::PRESSURE);
    EXPECT_EQ(released.backing, reinterpret_cast<void*>(0x3450));
    EXPECT_EQ(released.size, 99U);
    EXPECT_EQ(queued.claim(), Drop::RETIRED);
    EXPECT_EQ(queued.memory().backing, nullptr);
    EXPECT_EQ(claimed.memory().backing, storage.data());
    EXPECT_EQ(claimed.memory().size, storage.size());
    EXPECT_EQ(storage[0], 0xa5);
    EXPECT_EQ(registry.revoke(DESCRIPTOR.rkey, DESCRIPTOR.generation), Drop::NONE);
    reg::Lease successor;
    EXPECT_EQ(registry.admit(binding(), incoming(binding(), DESCRIPTOR), successor), Drop::RETIRED);
    queued.reset();
    EXPECT_EQ(registry.reclaim(DESCRIPTOR.rkey, DESCRIPTOR.generation, released), Drop::PRESSURE);
    storage[0] = 0x39;
    claimed.reset();
    ASSERT_EQ(registry.reclaim(DESCRIPTOR.rkey, DESCRIPTOR.generation, released), Drop::NONE);
    EXPECT_EQ(released.backing, storage.data());
    EXPECT_EQ(released.size, storage.size());
    EXPECT_EQ(storage[0], 0x39);
    EXPECT_TRUE(all_zero(registry.slots_[0].binding));
    EXPECT_TRUE(all_zero(registry.slots_[0].descriptor));
    EXPECT_EQ(registry.reclaim(DESCRIPTOR.rkey, DESCRIPTOR.generation, released), Drop::GRANT);
}

TEST_F(RegistryTest, MoveResetAndReleaseEraseExactlyOnceWithoutTakingGuard) {
    auto const DESCRIPTOR = publish();
    reg::Lease first, replaced;
    ASSERT_EQ(registry.admit(binding(), incoming(binding(), DESCRIPTOR), first), Drop::NONE);
    ASSERT_EQ(registry.admit(binding(), incoming(binding(), DESCRIPTOR), replaced), Drop::NONE);
    ASSERT_EQ(first.claim(), Drop::NONE);
    reg::Lease moved(std::move(first));
    EXPECT_FALSE(first);
    EXPECT_TRUE(all_zero(first.descriptor()));
    replaced = std::move(moved);
    EXPECT_FALSE(moved);
    EXPECT_TRUE(all_zero(moved.descriptor()));
    EXPECT_EQ(registry.slots_[0].references.load(), 1U);
    EXPECT_EQ(registry.slots_[0].claims.load(), 1U);
    auto* self = &replaced;
    replaced = std::move(*self);
    ASSERT_FALSE(registry.guard_.test_and_set());
    std::thread release([owned = std::move(replaced)]() mutable { owned.reset(); });
    release.join();
    EXPECT_EQ(registry.slots_[0].references.load(), 0U);
    EXPECT_EQ(registry.slots_[0].claims.load(), 0U);
    EXPECT_FALSE(replaced);
    replaced.reset();
    registry.guard_.clear();
    EXPECT_TRUE(all_zero(replaced.descriptor()));
}

TEST_F(RegistryTest, EveryMetadataOperationReturnsPressureOnGuardContention) {
    auto const DESCRIPTOR = publish();
    reg::Lease lease;
    ASSERT_EQ(registry.admit(binding(), incoming(binding(), DESCRIPTOR), lease), Drop::NONE);
    ASSERT_FALSE(registry.guard_.test_and_set());
    proto::RegionDescriptor descriptor{};
    descriptor.rkey = 99;
    reg::ReleasedRegion released{.size = 99};
    reg::Statistics stats{.live = 99};
    reg::Lease denied;
    EXPECT_EQ(registry.initialize(1, private_root()), Drop::PRESSURE);
    EXPECT_EQ(registry.register_region(registration(), descriptor), Drop::PRESSURE);
    EXPECT_EQ(registry.admit(binding(), incoming(binding(), DESCRIPTOR), denied), Drop::PRESSURE);
    EXPECT_EQ(lease.claim(), Drop::PRESSURE);
    EXPECT_EQ(registry.revoke(DESCRIPTOR.rkey, DESCRIPTOR.generation), Drop::PRESSURE);
    EXPECT_EQ(registry.close_peer(2, binding().session_id), Drop::PRESSURE);
    EXPECT_EQ(registry.close_transport(binding().transport_cookie, binding().transport_generation), Drop::PRESSURE);
    EXPECT_EQ(registry.reclaim(DESCRIPTOR.rkey, DESCRIPTOR.generation, released), Drop::PRESSURE);
    EXPECT_EQ(registry.statistics(stats), Drop::PRESSURE);
    EXPECT_EQ(registry.shutdown(), Drop::PRESSURE);
    EXPECT_EQ(descriptor.rkey, 99U);
    EXPECT_EQ(released.size, 99U);
    EXPECT_EQ(stats.live, 99U);
    EXPECT_EQ(lease.memory().backing, nullptr);
    lease.reset();
    EXPECT_EQ(registry.slots_[0].references.load(), 0U);
    registry.guard_.clear();
}

TEST_F(RegistryTest, PeerAndTransportClosureMatchExactIncarnations) {
    std::array<proto::RegionDescriptor, 4> descriptors{};
    auto first = registration();
    auto different_session = first;
    ++different_session.binding.session_id[0];
    auto different_peer = first;
    ++different_peer.binding.peer_node;
    auto different_transport = first;
    ++different_transport.binding.transport_generation;
    std::array registrations{first, different_session, different_peer, different_transport};
    for (size_t index = 0; index < descriptors.size(); ++index) {
        ASSERT_EQ(registry.register_region(registrations[index], descriptors[index]), Drop::NONE);
    }
    EXPECT_EQ(registry.close_peer(2, binding().session_id), Drop::NONE);
    EXPECT_EQ(registry.close_peer(2, binding().session_id), Drop::NONE);
    for (size_t index = 0; index < descriptors.size(); ++index) {
        reg::Lease lease;
        EXPECT_EQ(registry.admit(registrations[index].binding, incoming(registrations[index].binding, descriptors[index]), lease),
                  index == 0 || index == 3 ? Drop::RETIRED : Drop::NONE);
    }
    EXPECT_EQ(registry.close_transport(binding().transport_cookie, binding().transport_generation), Drop::NONE);
    reg::Statistics stats{};
    ASSERT_EQ(registry.statistics(stats), Drop::NONE);
    EXPECT_EQ(stats.closing, 4U);
}

TEST_F(RegistryTest, ShutdownPreservesRootAndBackingUntilOwnersReclaimAllRegions) {
    auto const DESCRIPTOR = publish();
    reg::Lease lease;
    ASSERT_EQ(registry.admit(binding(), incoming(binding(), DESCRIPTOR), lease), Drop::NONE);
    ASSERT_EQ(lease.claim(), Drop::NONE);
    EXPECT_EQ(registry.shutdown(), Drop::PRESSURE);
    EXPECT_EQ(registry.capability_root_, private_root());
    EXPECT_EQ(lease.memory().backing, storage.data());
    reg::Lease denied;
    EXPECT_EQ(registry.admit(binding(), incoming(binding(), DESCRIPTOR), denied), Drop::RETIRED);
    proto::RegionDescriptor other{};
    EXPECT_EQ(registry.register_region(registration(), other), Drop::RETIRED);
    lease.reset();
    EXPECT_EQ(registry.shutdown(), Drop::PRESSURE);
    EXPECT_EQ(registry.capability_root_, private_root());
    reg::ReleasedRegion released{};
    EXPECT_EQ(registry.reclaim(DESCRIPTOR.rkey, DESCRIPTOR.generation, released), Drop::NONE);
    EXPECT_EQ(registry.shutdown(), Drop::NONE);
    EXPECT_TRUE(all_zero(registry.capability_root_));
    EXPECT_EQ(storage[0], 0xa5);
    EXPECT_EQ(registry.initialize(1, private_root()), Drop::RETIRED);
}

TEST_F(RegistryTest, ReclaimedSlotsNeverReuseHintsGenerationsOrCapabilities) {
    auto const OLD = publish();
    auto const STALE = incoming(binding(), OLD);
    reg::ReleasedRegion released{};
    EXPECT_EQ(registry.reclaim(OLD.rkey, OLD.generation, released), Drop::NONE);
    auto const FRESH = publish();
    EXPECT_GT(FRESH.rkey, OLD.rkey);
    EXPECT_GT(FRESH.generation, OLD.generation);
    EXPECT_NE(FRESH.capability, OLD.capability);
    reg::Lease lease;
    EXPECT_EQ(registry.admit(binding(), STALE, lease), Drop::GRANT);
    EXPECT_EQ(registry.revoke(OLD.rkey, OLD.generation), Drop::GRANT);
    EXPECT_EQ(registry.reclaim(OLD.rkey, OLD.generation, released), Drop::GRANT);
    EXPECT_EQ(registry.admit(binding(), incoming(binding(), FRESH), lease), Drop::NONE);
}

TEST_F(RegistryTest, AllocatorsStopAtTerminalSentinelsWithoutWrapOrOutputMutation) {
    registry.next_rkey_ = UINT32_MAX - 1;
    registry.next_generation_ = UINT64_MAX - 1;
    auto const LAST = publish();
    EXPECT_EQ(LAST.rkey, UINT32_MAX - 1);
    EXPECT_EQ(LAST.generation, UINT64_MAX - 1);
    proto::RegionDescriptor output{};
    output.rkey = 99;
    EXPECT_EQ(registry.register_region(registration(), output), Drop::COUNTER);
    EXPECT_EQ(output.rkey, 99U);
    EXPECT_EQ(registry.next_rkey_, 0U);
    EXPECT_EQ(registry.next_generation_, 0U);
    reg::ReleasedRegion released{};
    EXPECT_EQ(registry.reclaim(LAST.rkey, LAST.generation, released), Drop::NONE);
    EXPECT_EQ(registry.register_region(registration(), output), Drop::COUNTER);
}

TEST_F(RegistryTest, ReferenceAndClaimOverflowRejectWithoutWrap) {
    auto const DESCRIPTOR = publish();
    registry.slots_[0].references.store(UINT32_MAX);
    reg::Lease lease;
    EXPECT_EQ(registry.admit(binding(), incoming(binding(), DESCRIPTOR), lease), Drop::PRESSURE);
    EXPECT_EQ(registry.slots_[0].references.load(), UINT32_MAX);
    registry.slots_[0].references.store(0);
    ASSERT_EQ(registry.admit(binding(), incoming(binding(), DESCRIPTOR), lease), Drop::NONE);
    registry.slots_[0].claims.store(UINT32_MAX);
    EXPECT_EQ(lease.claim(), Drop::PRESSURE);
    EXPECT_EQ(lease.memory().backing, nullptr);
    EXPECT_EQ(registry.slots_[0].claims.load(), UINT32_MAX);
    registry.slots_[0].claims.store(0);
}

TEST_F(RegistryTest, StatisticsSaturateAtomicCountSums) {
    static_cast<void>(publish());
    static_cast<void>(publish());
    for (size_t index = 0; index < 2; ++index) {
        registry.slots_[index].references.store(UINT32_MAX);
        registry.slots_[index].claims.store(UINT32_MAX);
    }
    reg::Statistics stats{};
    ASSERT_EQ(registry.statistics(stats), Drop::NONE);
    EXPECT_EQ(stats.references, UINT32_MAX);
    EXPECT_EQ(stats.claims, UINT32_MAX);
    for (size_t index = 0; index < 2; ++index) {
        registry.slots_[index].references.store(0);
        registry.slots_[index].claims.store(0);
    }
}

TEST(WkiRdmaRegistry, FixedSlotPressureReclaimsCapacityWithoutReusingAuthority) {
    reg::Registry registry;
    ASSERT_EQ(registry.initialize(1, private_root()), Drop::NONE);
    std::array<uint8_t, reg::REGION_SLOTS> backing{};
    std::array<proto::RegionDescriptor, reg::REGION_SLOTS> descriptors{};
    for (size_t index = 0; index < backing.size(); ++index) {
        reg::Registration input{
            .binding = binding(), .backing = &backing[index], .size = 1, .binding_generation = 7, .permissions = proto::PERMIT_READ};
        ASSERT_EQ(registry.register_region(input, descriptors[index]), Drop::NONE);
    }
    proto::RegionDescriptor overflow{};
    reg::Registration input{
        .binding = binding(), .backing = backing.data(), .size = 1, .binding_generation = 7, .permissions = proto::PERMIT_READ};
    EXPECT_EQ(registry.register_region(input, overflow), Drop::PRESSURE);
    reg::Statistics stats{};
    ASSERT_EQ(registry.statistics(stats), Drop::NONE);
    EXPECT_EQ(stats.live, reg::REGION_SLOTS);
    EXPECT_EQ(stats.bytes, reg::REGION_SLOTS);
    reg::ReleasedRegion released{};
    ASSERT_EQ(registry.reclaim(descriptors[0].rkey, descriptors[0].generation, released), Drop::NONE);
    ASSERT_EQ(registry.register_region(input, overflow), Drop::NONE);
    EXPECT_GT(overflow.rkey, descriptors.back().rkey);
    EXPECT_GT(overflow.generation, descriptors.back().generation);
    descriptors[0] = overflow;
    EXPECT_EQ(registry.close_peer(2, binding().session_id), Drop::NONE);
    for (const auto& descriptor : descriptors) EXPECT_EQ(registry.reclaim(descriptor.rkey, descriptor.generation, released), Drop::NONE);
    EXPECT_EQ(registry.shutdown(), Drop::NONE);
}

TEST(WkiRdmaRegistry, ExactRegisteredByteBudgetRejectsThenRestoresCredit) {
    reg::Registry registry;
    ASSERT_EQ(registry.initialize(1, private_root()), Drop::NONE);
    std::vector<uint8_t> backing(reg::REGISTERED_BYTES, 0x5c);
    reg::Registration input{.binding = binding(),
                            .backing = backing.data(),
                            .size = static_cast<uint32_t>(backing.size()),
                            .binding_generation = 7,
                            .permissions = proto::PERMIT_READ};
    proto::RegionDescriptor full{}, extra{};
    ASSERT_EQ(registry.register_region(input, full), Drop::NONE);
    input.size = 1;
    EXPECT_EQ(registry.register_region(input, extra), Drop::PRESSURE);
    reg::Statistics stats{};
    ASSERT_EQ(registry.statistics(stats), Drop::NONE);
    EXPECT_EQ(stats.bytes, reg::REGISTERED_BYTES);
    reg::ReleasedRegion released{};
    ASSERT_EQ(registry.reclaim(full.rkey, full.generation, released), Drop::NONE);
    EXPECT_EQ(released.size, reg::REGISTERED_BYTES);
    EXPECT_EQ(backing.front(), 0x5c);
    EXPECT_EQ(backing.back(), 0x5c);
    ASSERT_EQ(registry.register_region(input, extra), Drop::NONE);
    ASSERT_EQ(registry.reclaim(extra.rkey, extra.generation, released), Drop::NONE);
    ASSERT_EQ(registry.statistics(stats), Drop::NONE);
    EXPECT_EQ(stats.bytes, 0U);
    EXPECT_EQ(registry.shutdown(), Drop::NONE);
}

TEST(WkiRdmaRegistry, ConcurrentClaimAndRevokeHaveOneLinearityBoundary) {
    unsigned claimed_count = 0, retired_count = 0;
    for (unsigned iteration = 0; iteration < 128; ++iteration) {
        reg::Registry registry;
        ASSERT_EQ(registry.initialize(1, private_root()), Drop::NONE);
        std::array<uint8_t, 32> backing{};
        backing.fill(0xa5);
        reg::Registration input{.binding = binding(),
                                .backing = backing.data(),
                                .size = backing.size(),
                                .binding_generation = 7,
                                .permissions = proto::PERMIT_WRITE};
        proto::RegionDescriptor descriptor{};
        ASSERT_EQ(registry.register_region(input, descriptor), Drop::NONE);
        reg::Lease lease;
        ASSERT_EQ(registry.admit(binding(), incoming(binding(), descriptor), lease), Drop::NONE);
        std::barrier start(3);
        Drop claim_result = Drop::PRESSURE, revoke_result = Drop::PRESSURE;
        std::thread claimant([&] {
            start.arrive_and_wait();
            for (unsigned retry = 0; retry < 10000 && claim_result == Drop::PRESSURE; ++retry) {
                claim_result = lease.claim();
                if (claim_result == Drop::PRESSURE) std::this_thread::yield();
            }
        });
        std::thread revoker([&] {
            start.arrive_and_wait();
            for (unsigned retry = 0; retry < 10000 && revoke_result == Drop::PRESSURE; ++retry) {
                revoke_result = registry.revoke(descriptor.rkey, descriptor.generation);
                if (revoke_result == Drop::PRESSURE) std::this_thread::yield();
            }
        });
        start.arrive_and_wait();
        claimant.join();
        revoker.join();
        ASSERT_EQ(revoke_result, Drop::NONE);
        ASSERT_TRUE(claim_result == Drop::NONE || claim_result == Drop::RETIRED);
        if (claim_result == Drop::NONE) {
            ++claimed_count;
            EXPECT_EQ(lease.memory().backing, backing.data());
            static_cast<uint8_t*>(lease.memory().backing)[0] = 0x39;
        } else {
            ++retired_count;
            EXPECT_EQ(lease.memory().backing, nullptr);
        }
        reg::ReleasedRegion released{};
        EXPECT_EQ(registry.reclaim(descriptor.rkey, descriptor.generation, released), Drop::PRESSURE);
        lease.reset();
        ASSERT_EQ(registry.reclaim(descriptor.rkey, descriptor.generation, released), Drop::NONE);
        EXPECT_EQ(backing[0], claim_result == Drop::NONE ? 0x39 : 0xa5);
        EXPECT_EQ(registry.shutdown(), Drop::NONE);
    }
    EXPECT_EQ(claimed_count + retired_count, 128U);
}
