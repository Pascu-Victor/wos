#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <platform/security/compartment_policy.hpp>
#include <platform/security/credential_record.hpp>
#include <random>

namespace {
namespace security = ker::mod::security;
namespace compartment = security::compartment;
namespace resource = ker::abi::resource;
using security::Authority;
constexpr uint64_t FD_CAPACITY = 256;

auto parent_profile(bool root = true) -> compartment::Profile {
    compartment::Profile profile{};
    if (root) {
        profile.credentials = security::Snapshot::bootstrap_root();
    } else {
        std::array<uint32_t, 3> groups{21, 22, 23};
        EXPECT_EQ(security::Snapshot::restore_fields({.user = {1000, 1001, 1002},
                                                      .group = {31, 32, 33},
                                                      .groups = groups,
                                                      .authority_ceiling = security::ALL_AUTHORITY,
                                                      .dumpable = true},
                                                     profile.credentials),
                  0);
    }
    for (auto& limit : profile.limits) limit = {100, 200};
    profile.network = compartment::Network::SHARED_HOST;
    return profile;
}

auto request_from(const compartment::Profile& parent) -> compartment::Request {
    return {.authority_ceiling = parent.credentials.authority_ceiling(), .limits = parent.limits};
}

void expect_equal(const compartment::Profile& first, const compartment::Profile& second) {
    EXPECT_EQ(first.credentials, second.credentials);
    EXPECT_EQ(first.network, second.network);
    for (size_t i = 0; i < first.limits.size(); ++i) {
        EXPECT_EQ(first.limits[i].soft, second.limits[i].soft);
        EXPECT_EQ(first.limits[i].hard, second.limits[i].hard);
    }
}

void expect_unchanged_rejection(const compartment::Profile& parent, const compartment::Request& request, int error,
                                uint64_t fd_capacity = FD_CAPACITY) {
    auto output = parent_profile(false);
    output.network = compartment::Network::DENIED;
    auto const BEFORE = output;
    auto const ORIGINAL_PARENT = parent;
    EXPECT_EQ(compartment::prepare(parent, request, fd_capacity, output), error);
    expect_equal(output, BEFORE);
    expect_equal(parent, ORIGINAL_PARENT);
}
}  // namespace

TEST(CompartmentPolicy, DefaultCandidateDropsAuthorityAndNetworkingWithoutAmbientRoot) {
    compartment::Profile parent{};
    parent.limits[static_cast<unsigned>(resource::Id::NOFILE)] = {FD_CAPACITY, FD_CAPACITY};
    compartment::Request request{};
    request.limits = parent.limits;
    compartment::Profile output{};
    ASSERT_EQ(compartment::prepare(parent, request, FD_CAPACITY, output), 0);
    EXPECT_EQ(output.credentials.user(), parent.credentials.user());
    EXPECT_EQ(output.credentials.user().effective, 65534U);
    EXPECT_EQ(output.credentials.authority_ceiling(), 0U);
    EXPECT_EQ(output.credentials.service_privileges(), 0U);
    EXPECT_TRUE(output.credentials.no_new_privileges());
    EXPECT_TRUE(output.credentials.service_privileges_locked());
    EXPECT_FALSE(output.credentials.dumpable());
    EXPECT_EQ(output.network, compartment::Network::DENIED);
}

TEST(CompartmentPolicy, PreservesEveryIdentityAndGroupSlotAndDoesNotMutateParent) {
    for (bool root : {false, true}) {
        auto parent = parent_profile(root);
        auto const BEFORE = parent;
        auto request = request_from(parent);
        request.dumpable = true;
        compartment::Profile output{};
        ASSERT_EQ(compartment::prepare(parent, request, FD_CAPACITY, output), 0);
        EXPECT_EQ(output.credentials.user(), parent.credentials.user());
        EXPECT_EQ(output.credentials.group(), parent.credentials.group());
        EXPECT_TRUE(std::equal(output.credentials.groups().begin(), output.credentials.groups().end(), parent.credentials.groups().begin(),
                               parent.credentials.groups().end()));
        EXPECT_EQ(output.credentials.authority_ceiling(), parent.credentials.authority_ceiling());
        EXPECT_TRUE(output.credentials.dumpable());
        expect_equal(parent, BEFORE);
        auto wire = security::credential_record::encode(output.credentials);
        security::Snapshot restored{};
        ASSERT_EQ(security::credential_record::decode(wire, restored), 0);
        EXPECT_EQ(restored, output.credentials);
    }
}

TEST(CompartmentPolicy, CeilingAndSealedPrivilegesNeverIncreaseEvenForRoot) {
    auto parent = parent_profile();
    auto request = request_from(parent);
    request.authority_ceiling &= ~(uint64_t{1} << static_cast<unsigned>(Authority::RESOURCE_LIMIT));
    request.service_privileges = 3;
    compartment::Profile child{};
    ASSERT_EQ(compartment::prepare(parent, request, FD_CAPACITY, child), 0);
    EXPECT_EQ(child.credentials.effective_service_privileges(), 3U);
    EXPECT_FALSE(child.credentials.allows(Authority::RESOURCE_LIMIT));
    auto grandchild = request_from(child);
    grandchild.authority_ceiling = security::ALL_AUTHORITY;
    expect_unchanged_rejection(child, grandchild, -EPERM);
    auto nonroot = parent_profile(false);
    request = request_from(nonroot);
    request.service_privileges = 1;
    expect_unchanged_rejection(nonroot, request, -EPERM);
}

TEST(CompartmentPolicy, EveryRequestedLimitMustFitBothInheritedBounds) {
    for (bool root : {false, true}) {
        auto parent = parent_profile(root);
        for (size_t id = 0; id < parent.limits.size(); ++id) {
            auto request = request_from(parent);
            request.limits[id].soft++;
            expect_unchanged_rejection(parent, request, -EPERM);
            request = request_from(parent);
            request.limits[id].hard++;
            expect_unchanged_rejection(parent, request, -EPERM);
            request = request_from(parent);
            request.limits[id] = {0, 0};
            compartment::Profile output{};
            EXPECT_EQ(compartment::prepare(parent, request, FD_CAPACITY, output), 0);
            EXPECT_EQ(output.limits[id].soft, 0U);
            EXPECT_EQ(output.limits[id].hard, 0U);
        }
    }
}

TEST(CompartmentPolicy, MalformedLimitsAndPhysicalFdCapacityCannotBeAdvertised) {
    auto parent = parent_profile();
    auto request = request_from(parent);
    request.limits[0] = {150, 100};
    expect_unchanged_rejection(parent, request, -EINVAL);
    request = request_from(parent);
    expect_unchanged_rejection(parent, request, -EINVAL, 0);
    expect_unchanged_rejection(parent, request, -EINVAL, 199);
    parent.limits[0] = {300, 200};
    expect_unchanged_rejection(parent, request, -EINVAL);
}

TEST(CompartmentPolicy, InfinityZeroAndMaximumFiniteBoundsHaveNoArithmeticOverflow) {
    auto parent = parent_profile();
    for (auto& limit : parent.limits) limit = {UINT64_MAX, UINT64_MAX};
    parent.limits[static_cast<unsigned>(resource::Id::NOFILE)] = {256, 256};
    for (uint64_t value : {uint64_t{0}, uint64_t{1}, UINT64_MAX - 1, UINT64_MAX}) {
        auto request = request_from(parent);
        request.limits[0] = {value, value};
        compartment::Profile output{};
        ASSERT_EQ(compartment::prepare(parent, request, FD_CAPACITY, output), 0);
        EXPECT_EQ(output.limits[0].soft, value);
        EXPECT_EQ(output.limits[0].hard, value);
    }
    parent.limits[0] = {0, 0};
    auto request = request_from(parent);
    request.limits[0] = {1, 1};
    expect_unchanged_rejection(parent, request, -EPERM);
}

TEST(CompartmentPolicy, NetworkAccessRequiresExplicitInheritedSharedPermission) {
    auto parent = parent_profile();
    auto request = request_from(parent);
    compartment::Profile child{};
    ASSERT_EQ(compartment::prepare(parent, request, FD_CAPACITY, child), 0);
    EXPECT_EQ(child.network, compartment::Network::DENIED);
    request.network = compartment::Network::SHARED_HOST;
    compartment::Profile shared{};
    ASSERT_EQ(compartment::prepare(parent, request, FD_CAPACITY, shared), 0);
    EXPECT_EQ(shared.network, compartment::Network::SHARED_HOST);
    request = request_from(child);
    request.network = compartment::Network::SHARED_HOST;
    expect_unchanged_rejection(child, request, -EPERM);
}

TEST(CompartmentPolicy, NondumpableParentAndUnknownBitsRemainRejected) {
    auto parent = parent_profile();
    auto request = request_from(parent);
    security::Snapshot snapshot{};
    ASSERT_EQ(parent.credentials.prepare({.change = security::Change::SET_DUMPABLE, .dumpable = false}, snapshot), 0);
    parent.credentials = snapshot;
    request.dumpable = true;
    expect_unchanged_rejection(parent, request, -EPERM);
    request = request_from(parent);
    request.authority_ceiling |= uint64_t{1} << 63;
    expect_unchanged_rejection(parent, request, -EINVAL);
    request = request_from(parent);
    request.service_privileges = 4;
    expect_unchanged_rejection(parent, request, -EINVAL);
    request = request_from(parent);
    request.network = static_cast<compartment::Network>(2);
    expect_unchanged_rejection(parent, request, -EINVAL);
    request = request_from(parent);
    parent.network = static_cast<compartment::Network>(2);
    expect_unchanged_rejection(parent, request, -EINVAL);
}

TEST(CompartmentPolicy, RepeatedEntryCannotUndoIrreversibleRestrictions) {
    auto parent = parent_profile();
    auto request = request_from(parent);
    request.authority_ceiling = 0;
    compartment::Profile child{};
    ASSERT_EQ(compartment::prepare(parent, request, FD_CAPACITY, child), 0);
    auto next_request = request_from(child);
    compartment::Profile next{};
    ASSERT_EQ(compartment::prepare(child, next_request, FD_CAPACITY, next), 0);
    expect_equal(next, child);
    security::Snapshot attempted{};
    EXPECT_EQ(child.credentials.prepare({.change = security::Change::SET_SERVICE_PRIVILEGES, .service_privileges = 1}, attempted), -EPERM);
    EXPECT_EQ(attempted, child.credentials);
    EXPECT_EQ(
        child.credentials.prepare({.change = security::Change::DROP_AUTHORITY, .authority_ceiling = security::ALL_AUTHORITY}, attempted),
        -EPERM);
    EXPECT_EQ(attempted, child.credentials);
}

TEST(CompartmentPolicy, DeterministicProfileChainsRespectAllSubsetProperties) {
    std::mt19937_64 random(0x3901);
    for (size_t run = 0; run < 2048; ++run) {
        auto parent = parent_profile();
        for (auto& limit : parent.limits) {
            limit.hard = random() % 201;
            limit.soft = random() % (limit.hard + 1);
        }
        parent.network = (random() & 1U) == 0 ? compartment::Network::DENIED : compartment::Network::SHARED_HOST;
        for (size_t depth = 0; depth < 4; ++depth) {
            auto request = request_from(parent);
            request.authority_ceiling &= random();
            request.service_privileges = parent.credentials.effective_service_privileges() & random();
            for (size_t id = 0; id < request.limits.size(); ++id) {
                request.limits[id].soft = random() % (parent.limits[id].soft + 1);
                request.limits[id].hard = request.limits[id].soft + random() % (parent.limits[id].hard - request.limits[id].soft + 1);
            }
            request.network = (random() & 1U) == 0 ? compartment::Network::DENIED : parent.network;
            request.dumpable = parent.credentials.dumpable() && (random() & 1U) != 0;
            compartment::Profile child{};
            ASSERT_EQ(compartment::prepare(parent, request, FD_CAPACITY, child), 0);
            EXPECT_EQ(child.credentials.authority_ceiling() & ~parent.credentials.authority_ceiling(), 0U);
            EXPECT_EQ(child.credentials.effective_service_privileges() & ~parent.credentials.effective_service_privileges(), 0U);
            EXPECT_EQ(child.credentials.user(), parent.credentials.user());
            EXPECT_EQ(child.credentials.group(), parent.credentials.group());
            EXPECT_TRUE(child.credentials.no_new_privileges());
            EXPECT_TRUE(child.credentials.service_privileges_locked());
            EXPECT_TRUE(!child.credentials.dumpable() || parent.credentials.dumpable());
            EXPECT_TRUE(child.network == compartment::Network::DENIED || parent.network == compartment::Network::SHARED_HOST);
            for (size_t id = 0; id < child.limits.size(); ++id) {
                EXPECT_LE(child.limits[id].soft, parent.limits[id].soft);
                EXPECT_LE(child.limits[id].hard, parent.limits[id].hard);
                EXPECT_LE(child.limits[id].soft, child.limits[id].hard);
            }
            parent = child;
        }
    }
}

TEST(CompartmentPolicy, CandidateNoNewPrivilegesUsesExistingSetIdExecSuppression) {
    auto parent = parent_profile(false);
    auto request = request_from(parent);
    compartment::Profile child{};
    ASSERT_EQ(compartment::prepare(parent, request, FD_CAPACITY, child), 0);
    security::Snapshot executed{};
    ASSERT_EQ(child.credentials.prepare_exec({.uid = 0, .gid = 0, .set_uid = true, .set_gid = true, .mode = 0755}, executed), 0);
    EXPECT_EQ(executed.user().effective, parent.credentials.user().effective);
    EXPECT_EQ(executed.group().effective, parent.credentials.group().effective);
    EXPECT_EQ(executed.user().saved, executed.user().effective);
    EXPECT_EQ(executed.group().saved, executed.group().effective);
    EXPECT_TRUE(executed.no_new_privileges());
    EXPECT_TRUE(executed.service_privileges_locked());
    EXPECT_EQ(executed.authority_ceiling(), child.credentials.authority_ceiling());
}

TEST(CompartmentPolicy, InPlacePreparationIsAtomicOnFailure) {
    auto parent = parent_profile();
    auto request = request_from(parent);
    auto const BEFORE = parent;
    request.limits[0].hard++;
    EXPECT_EQ(compartment::prepare(parent, request, FD_CAPACITY, parent), -EPERM);
    expect_equal(parent, BEFORE);
    request = request_from(parent);
    request.authority_ceiling = 0;
    ASSERT_EQ(compartment::prepare(parent, request, FD_CAPACITY, parent), 0);
    EXPECT_EQ(parent.credentials.authority_ceiling(), 0U);
    EXPECT_TRUE(parent.credentials.no_new_privileges());
    EXPECT_TRUE(parent.credentials.service_privileges_locked());
}

TEST(CompartmentPolicy, ExistingSealedNonrootGrantsRemainCompleteAndCannotGrow) {
    auto parent = parent_profile(false);
    auto fields = security::SnapshotFields{.user = parent.credentials.user(),
                                           .group = parent.credentials.group(),
                                           .groups = parent.credentials.groups(),
                                           .authority_ceiling = security::ALL_AUTHORITY,
                                           .service_privileges = 1,
                                           .no_new_privileges = true,
                                           .service_privileges_locked = true};
    security::Snapshot sealed{};
    ASSERT_EQ(security::Snapshot::restore_fields(fields, sealed), 0);
    parent.credentials = sealed;
    auto request = request_from(parent);
    request.service_privileges = 1;
    compartment::Profile child{};
    ASSERT_EQ(compartment::prepare(parent, request, FD_CAPACITY, child), 0);
    EXPECT_EQ(child.credentials, sealed);
    request.service_privileges = 3;
    expect_unchanged_rejection(parent, request, -EPERM);
}
