#include <gtest/gtest.h>

#include <dev/iommu/groups.hpp>
#include <vector>

namespace {
namespace groups = ker::dev::iommu::groups;
namespace scopes = ker::dev::iommu::scopes;
using Device = scopes::Device;
using Evidence = groups::Evidence;
auto cap(unsigned type, bool acs = false) -> Evidence {
    Evidence result{.readable = true};
    result.capability.pcie = 0x40;
    result.capability.pcie_version = 2;
    result.capability.pcie_type = type;
    if (acs) {
        result.capability.acs = 0x100;
        result.capability.acs_capability = 0x5f;
        result.capability.acs_control = 0x1f;
    }
    return result;
}
auto owners(size_t count) -> scopes::Result {
    scopes::Result result{.device_count = count};
    for (size_t i = 0; i < count; ++i) result.devices[i].unit = 0;
    return result;
}
auto run(const std::vector<Device>& devices, const std::vector<Evidence>& evidence) -> groups::Result {
    return groups::qualify(devices, evidence, owners(devices.size()));
}
const std::vector<Device> branch{
    {.slot = 1, .header = 1, .secondary = 1, .subordinate = 2},
    {.bus = 1, .header = 1, .parent = 0, .secondary = 2, .subordinate = 2},
    {.bus = 2, .slot = 3, .parent = 1},
    {.bus = 2, .slot = 4, .parent = 1},
    {.slot = 2},
};
TEST(IommuGroups, IntegratedRequestersStayDistinctIncludingRootMultifunction) {
    auto result = run({{.slot = 1}, {.slot = 1, .function = 1}, {.slot = 2}}, {{.readable = true}, {.readable = true}, cap(9)});
    ASSERT_TRUE(result.valid());
    EXPECT_EQ(result.count, 3);
    EXPECT_EQ(result.requester_count, 3);
    for (size_t i = 0; i < 3; ++i) EXPECT_EQ(result.owners[i], i);
}
TEST(IommuGroups, PciePciBranchSharesRootAndAddsAbsentSecondaryBusAlias) {
    auto result = run(branch, {cap(4, true), cap(7), {.readable = true}, {.readable = true}, cap(9)});
    ASSERT_TRUE(result.valid());
    EXPECT_EQ(result.count, 3);
    EXPECT_EQ(result.owners[0], 0);
    EXPECT_EQ(result.owners[1], 1);
    EXPECT_EQ(result.owners[2], 1);
    EXPECT_EQ(result.owners[3], 1);
    EXPECT_EQ(result.owners[4], 4);
    EXPECT_EQ(result.requester_count, 6);
    bool found = false;
    for (size_t r = 0; r < result.requester_count; ++r) {
        auto const& requester = result.requesters[r];
        if (requester.sid != 0x200) continue;
        found = true;
        EXPECT_TRUE(requester.alias);
        EXPECT_EQ(requester.device, groups::NONE);
        EXPECT_EQ(requester.owner, 1);
    }
    EXPECT_TRUE(found);
}
TEST(IommuGroups, MissingOrDisabledAcsMergesCompleteSubtree) {
    for (unsigned mode = 0; mode < 4; ++mode) {
        auto root = cap(4, true);
        if (mode == 0) root.capability.acs = 0;
        if (mode == 1) root.capability.acs_control = 0;
        if (mode == 2) root.capability.acs_capability &= ~4;
        if (mode == 3) root.capability.acs_control |= 0x40;
        auto result = run(branch, {root, cap(7), {.readable = true}, {.readable = true}, cap(9)});
        ASSERT_TRUE(result.valid());
        EXPECT_EQ(result.count, 2);
        for (size_t i = 0; i < 4; ++i) EXPECT_EQ(result.owners[i], 0);
        EXPECT_EQ(result.owners[4], 4);
    }
}
TEST(IommuGroups, ConventionalBridgeAliasDeduplicatesAndMarksSourceAmbiguous) {
    auto result = run(branch, {cap(4, true), {.readable = true}, {.readable = true}, {.readable = true}, cap(9)});
    ASSERT_TRUE(result.valid());
    EXPECT_EQ(result.requester_count, branch.size());
    for (size_t r = 0; r < result.requester_count; ++r) {
        auto const& requester = result.requesters[r];
        if (requester.sid == 0x100) {
            EXPECT_TRUE(requester.alias);
            EXPECT_EQ(requester.device, 1);
            EXPECT_EQ(requester.owner, 1);
        }
    }
}
TEST(IommuGroups, AliasCollisionWithPresentFunctionStillRecordsAmbiguity) {
    auto devices = branch;
    devices[2].slot = 0;
    auto result = run(devices, {cap(4, true), cap(7), {.readable = true}, {.readable = true}, cap(9)});
    ASSERT_TRUE(result.valid());
    EXPECT_EQ(result.requester_count, devices.size());
    for (size_t r = 0; r < result.requester_count; ++r)
        if (result.requesters[r].sid == 0x200) {
            EXPECT_TRUE(result.requesters[r].alias);
            EXPECT_EQ(result.requesters[r].device, 2);
        }
}
TEST(IommuGroups, MultifunctionEndpointsShareUnlessBothHaveActiveBoundary) {
    std::vector<Device> devices{
        {.slot = 1, .header = 1, .secondary = 1, .subordinate = 1}, {.bus = 1, .parent = 0}, {.bus = 1, .function = 1, .parent = 0}};
    auto result = run(devices, {cap(4, true), cap(0), cap(0)});
    ASSERT_TRUE(result.valid());
    EXPECT_EQ(result.count, 2);
    EXPECT_EQ(result.owners[2], 1);
    result = run(devices, {cap(4, true), cap(0, true), cap(0, true)});
    ASSERT_TRUE(result.valid());
    EXPECT_EQ(result.count, 3);
}
TEST(IommuGroups, CrossUnitGroupAndIncompleteEvidenceAreRejectedWithoutPartialOutput) {
    std::vector<Evidence> evidence{cap(4), cap(7), {.readable = true}, {.readable = true}, cap(9)};
    auto ownership = owners(branch.size());
    ownership.devices[2].unit = 1;
    auto result = groups::qualify(branch, evidence, ownership);
    EXPECT_EQ(result.error, groups::Error::unit);
    EXPECT_EQ(result.devices, 0);
    EXPECT_EQ(result.requester_count, 0);
    for (size_t i = 0; i < evidence.size(); ++i) {
        auto copy = evidence;
        copy[i].readable = false;
        result = run(branch, copy);
        EXPECT_EQ(result.error, groups::Error::capability);
        EXPECT_EQ(result.requester_count, 0);
    }
    EXPECT_EQ(groups::qualify(branch, {}, owners(branch.size())).error, groups::Error::incomplete);
}
TEST(IommuGroups, EnabledUnsupportedRequesterModesAndHeaderMismatchRefuse) {
    for (unsigned mode = 0; mode < 6; ++mode) {
        auto c = cap(9);
        if (mode == 0) c.capability.device_control = 1U << 9U;
        if (mode == 1) c.capability.device_control2 = 1U << 5U;
        if (mode == 2) c.capability.ats_control = 0x8000;
        if (mode == 3) c.capability.pri_control = 1;
        if (mode == 4) c.capability.pasid_control = 1;
        if (mode == 5) c.capability.sriov_control = 1;
        EXPECT_EQ(run({{}}, {c}).error, groups::Error::requester_mode);
    }
    EXPECT_EQ(run({{}}, {cap(4)}).error, groups::Error::capability);
    EXPECT_EQ(run({{.header = 1}}, {cap(0)}).error, groups::Error::capability);
}
TEST(IommuGroups, InvalidTopologyAndCapacityRefuseWithBoundedMaximumSupported) {
    auto broken = branch;
    broken[2].parent = 2;
    EXPECT_EQ(run(broken, std::vector<Evidence>(5, {.readable = true})).error, groups::Error::topology);
    std::vector<Device> devices;
    std::vector<Evidence> evidence;
    for (unsigned i = 0; i < 64; ++i) {
        devices.push_back({.bus = static_cast<uint8_t>(i), .slot = 1});
        evidence.push_back(cap(9));
    }
    auto result = run(devices, evidence);
    ASSERT_TRUE(result.valid());
    EXPECT_EQ(result.count, 64);
    EXPECT_EQ(result.requester_count, 64);
    devices.push_back({});
    evidence.push_back(cap(9));
    EXPECT_EQ(groups::qualify(devices, evidence, {}).error, groups::Error::limit);
}
}  // namespace

TEST(IommuStrict, IntegratedRootFunctionsHaveIndependentDomains) {
    std::vector<Device> devices{{.slot = 1}, {.slot = 1, .function = 2}, {.slot = 2}, {.slot = 3}};
    std::vector<Evidence> evidence{{.readable = true}, {.readable = true}, cap(9), cap(10)};
    auto const result = groups::qualify(devices, evidence, owners(devices.size()), true);
    ASSERT_TRUE(result.valid());
    ASSERT_EQ(result.count, devices.size());
    ASSERT_EQ(result.requester_count, devices.size());
    for (size_t i = 0; i < devices.size(); ++i) {
        EXPECT_EQ(result.owners[i], i);
        EXPECT_FALSE(result.requesters[i].alias);
    }
}
TEST(IommuStrict, EnabledBoundaryAndIsolatedFunctionsAreAdmitted) {
    std::vector<Device> devices{
        {.slot = 1, .header = 1, .secondary = 1, .subordinate = 1}, {.bus = 1, .parent = 0}, {.bus = 1, .function = 1, .parent = 0}};
    std::vector<Evidence> evidence{cap(4, true), cap(0, true), cap(0, true)};
    auto const result = groups::qualify(devices, evidence, owners(devices.size()), true);
    ASSERT_TRUE(result.valid());
    EXPECT_EQ(result.count, devices.size());
    for (size_t i = 0; i < devices.size(); ++i) EXPECT_EQ(result.owners[i], i);
    evidence[2] = cap(0);
    auto const shared = groups::qualify(devices, evidence, owners(devices.size()), true);
    EXPECT_EQ(shared.error, groups::Error::shared_domain);
    EXPECT_EQ(shared.error_device, 2);
    EXPECT_EQ(shared.devices, 0);
    EXPECT_EQ(shared.requester_count, 0);
    EXPECT_TRUE(groups::qualify(devices, evidence, owners(devices.size())).valid());
}
TEST(IommuStrict, EveryMissingBoundaryControlAndDirectTranslationRefuses) {
    std::vector<Device> devices{{.slot = 1, .header = 1, .secondary = 1, .subordinate = 1}, {.bus = 1, .parent = 0}};
    for (unsigned missing = 0; missing < 5; ++missing) {
        for (bool control : {false, true}) {
            auto root = cap(4, true);
            if (control)
                root.capability.acs_control &= ~(1U << missing);
            else
                root.capability.acs_capability &= ~(1U << missing);
            auto const result = groups::qualify(devices, std::vector<Evidence>{root, cap(0)}, owners(devices.size()), true);
            EXPECT_EQ(result.error, groups::Error::isolation_boundary);
            EXPECT_EQ(result.error_device, 0);
            EXPECT_EQ(result.requester_count, 0);
        }
    }
    for (bool direct : {false, true}) {
        auto root = cap(4, true);
        if (direct)
            root.capability.acs_control |= 0x40;
        else
            root.capability.acs = 0;
        auto const result = groups::qualify(devices, std::vector<Evidence>{root, cap(0)}, owners(devices.size()), true);
        EXPECT_EQ(result.error, groups::Error::isolation_boundary);
        EXPECT_EQ(result.devices, 0);
    }
}
TEST(IommuStrict, AliasBridgesAndImpossibleEndpointPlacementRefuse) {
    for (auto const& bridge : {Evidence{.readable = true}, cap(7, true), cap(8, true)}) {
        auto const result =
            groups::qualify(branch, std::vector<Evidence>{cap(4, true), bridge, cap(0), cap(0), cap(9)}, owners(branch.size()), true);
        EXPECT_EQ(result.error, groups::Error::isolation_boundary);
        EXPECT_EQ(result.error_device, 1);
        EXPECT_EQ(result.requester_count, 0);
    }
    for (unsigned type : {0U, 1U}) {
        auto const root_endpoint = groups::qualify(std::vector<Device>{{}}, std::vector<Evidence>{cap(type)}, owners(1), true);
        EXPECT_EQ(root_endpoint.error, groups::Error::isolation_boundary);
    }
    std::vector<Device> devices{{.slot = 1, .header = 1, .secondary = 1, .subordinate = 1}, {.bus = 1, .parent = 0}};
    for (auto const& endpoint : {Evidence{.readable = true}, cap(9), cap(10)}) {
        auto const result = groups::qualify(devices, std::vector<Evidence>{cap(4, true), endpoint}, owners(2), true);
        EXPECT_EQ(result.error, groups::Error::isolation_boundary);
        EXPECT_EQ(result.error_device, 1);
    }
}
TEST(IommuStrict, SwitchHierarchyRequiresAllBoundariesAndCorrectRoles) {
    std::vector<Device> devices{{.slot = 1, .header = 1, .secondary = 1, .subordinate = 3},
                                {.bus = 1, .header = 1, .parent = 0, .secondary = 2, .subordinate = 3},
                                {.bus = 2, .header = 1, .parent = 1, .secondary = 3, .subordinate = 3},
                                {.bus = 3, .parent = 2}};
    std::vector<Evidence> evidence{cap(4, true), cap(5, true), cap(6, true), cap(0)};
    ASSERT_TRUE(groups::qualify(devices, evidence, owners(devices.size()), true).valid());
    for (size_t i = 0; i < 3; ++i) {
        auto changed = evidence;
        changed[i].capability.acs_control = 0;
        EXPECT_EQ(groups::qualify(devices, changed, owners(devices.size()), true).error, groups::Error::isolation_boundary);
        changed = evidence;
        changed[i].capability.pcie_type = i == 0 ? 5 : 4;
        EXPECT_EQ(groups::qualify(devices, changed, owners(devices.size()), true).error, groups::Error::isolation_boundary);
    }
}
TEST(IommuStrict, UnsupportedRequesterModesRemainFailClosed) {
    for (unsigned mode = 0; mode < 6; ++mode) {
        auto c = cap(9);
        if (mode == 0) c.capability.device_control = 1U << 9U;
        if (mode == 1) c.capability.device_control2 = 1U << 5U;
        if (mode == 2) c.capability.ats_control = 0x8000;
        if (mode == 3) c.capability.pri_control = 1;
        if (mode == 4) c.capability.pasid_control = 1;
        if (mode == 5) c.capability.sriov_control = 1;
        auto const result = groups::qualify(std::vector<Device>{{}}, std::vector<Evidence>{c}, owners(1), true);
        EXPECT_EQ(result.error, groups::Error::requester_mode);
        EXPECT_EQ(result.requester_count, 0);
    }
}
