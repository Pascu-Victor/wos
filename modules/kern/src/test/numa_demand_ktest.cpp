#ifdef WOS_SELFTEST

#include <abi/callnums/vmem.h>

#include <array>
#include <atomic>
#include <bit>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <platform/acpi/numa_boot.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/numa_alloc.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <test/ktest.hpp>

#ifdef WOS_KASAN
#include <sanitizer/kasan.hpp>
#endif

namespace {
namespace mm = ker::mod::mm;
namespace numa = mm::numa;
namespace virt = mm::virt;
namespace phys = mm::phys;
namespace paging = mm::paging;
namespace task = ker::mod::sched::task;
namespace vmem = ker::syscall::vmem;
namespace vmabi = ker::abi::vmem;
namespace topology = ker::mod::acpi::numa;
using log = ker::mod::dbg::logger<"numa-demand-test">;
constexpr uint64_t VA = 0x6A000000ULL;
constexpr size_t PAGE = paging::PAGE_SIZE;
constexpr uint8_t CONTENT = 0x63;

auto physical(const void* pointer) -> uint64_t {
    return reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(pointer)));
}
auto frame(paging::PageTable* root, uint64_t address = VA) -> uint8_t* {
    auto const PHYSICAL = virt::translate(root, address);
    return PHYSICAL == virt::PADDR_INVALID ? nullptr : reinterpret_cast<uint8_t*>(mm::addr::get_virt_pointer(PHYSICAL));
}
auto filled(const void* pointer, uint8_t value) -> bool {
    if (pointer == nullptr) return false;
    auto const* bytes = static_cast<const uint8_t*>(pointer);
    for (size_t i = 0; i < PAGE; ++i)
        if (bytes[i] != value) return false;
    return true;
}

struct OwnerBalance {
    phys::PhysicalBalanceSnapshot before{};
    OwnerBalance() { phys::get_physical_balance_snapshot(before); }
    ~OwnerBalance() {
        phys::PhysicalBalanceSnapshot after{};
        phys::get_physical_balance_snapshot(after);
        auto const OWNER = static_cast<size_t>(mm::PhysicalPageOwner::USER_PRIVATE_MAPPING);
        KEXPECT_EQ(after.owners[OWNER].pages, before.owners[OWNER].pages);
        KEXPECT_EQ(after.owners[OWNER].objects, before.owners[OWNER].objects);
        KEXPECT_EQ(after.identity_mismatch_pages, 0U);
        log::info("owned demand closure private_pages=%llu private_objects=%llu free_before=%llu free_after=%llu",
                  static_cast<unsigned long long>(after.owners[OWNER].pages), static_cast<unsigned long long>(after.owners[OWNER].objects),
                  static_cast<unsigned long long>(before.free_pages), static_cast<unsigned long long>(after.free_pages));
    }
};

struct OwnedTask {
    std::unique_ptr<task::Task> subject{new (std::nothrow) task::Task{}};
    paging::PageTable* root{};
    ~OwnedTask() { reset(); }
    auto init() -> bool {
        if (subject == nullptr) return false;
        root = virt::create_pagemap();
        if (root == nullptr) return false;
        subject->type = task::TaskType::PROCESS;
        subject->pagemap = root;
        return true;
    }
    void reset() {
        if (root == nullptr) return;
        KEXPECT_EQ(subject->ref_count.load(std::memory_order_acquire), 1U);
        KEXPECT_EQ(subject->usercopy_pagemap_accesses.load(std::memory_order_acquire), 0U);
        KEXPECT_EQ(virt::selftest_user_pagemap_reader_count(root), 0U);
        KEXPECT_EQ(subject->detach_pagemap_after_usercopy_quiescence(), root);
        task::release_lazy_vmem_ranges(*subject);
        vmem::release_file_mmap_ranges_for_pagemap(root);
        virt::destroy_user_space(root, 0, "numa-demand", "owned-fixture");
        virt::release_pagemap(root);
        root = nullptr;
    }
    auto lazy(size_t pages = 1) -> bool {
        return subject->lazy_vmem_ranges.push_back({.start = VA,
                                                    .end = VA + PAGE * pages,
                                                    .prot = vmabi::PROT_READ | vmabi::PROT_WRITE,
                                                    .flags = vmabi::MAP_PRIVATE | vmabi::MAP_ANONYMOUS,
                                                    .file = nullptr});
    }
    auto map(uint64_t flags) -> uint8_t* {
        auto* page = static_cast<uint8_t*>(phys::page_alloc_may_fail(mm::PhysicalPageOwner::USER_PRIVATE_MAPPING, PAGE, "demand-test"));
        if (page == nullptr) return nullptr;
        std::memset(page, CONTENT, PAGE);
        virt::map_page(root, VA, physical(page), flags);
        return page;
    }
};

auto clone(OwnedTask& source, OwnedTask& destination) -> bool {
    vmem::SharedVmemPublicationGuard guard;
    return virt::deep_copy_user_pagemap_cow(source.root, destination.root);
}
auto memory_nodes() -> numa::NodeMask {
    auto const* machine = topology::boot_topology();
    numa::NodeMask mask = 0;
    if (machine != nullptr)
        for (numa::NodeId i = 0; i < machine->node_count; ++i)
            if (machine->nodes[i].usable_bytes != 0) mask |= numa::NodeMask{1} << i;
    return mask;
}
void expect_install(const virt::SelftestDemandObservation& observation, numa::NodeId node, virt::DemandBacking backing) {
    KEXPECT_TRUE(observation.admitted);
    KEXPECT_TRUE(observation.disarmed);
    KEXPECT_EQ(observation.captures, 1U);
    KEXPECT_EQ(observation.candidates, 1U);
    KEXPECT_EQ(observation.installed, 1U);
    KEXPECT_EQ(observation.discarded, 0U);
    KEXPECT_EQ(observation.allocation_failures, 0U);
    KEXPECT_EQ(observation.origin, virt::DemandOrigin::SoftwareEnsure);
    KEXPECT_EQ(observation.exclusion, virt::DemandExclusion::Software);
    KEXPECT_EQ(observation.backing, backing);
    KEXPECT_EQ(observation.requested, node);
    KEXPECT_EQ(observation.actual, node);
    KEXPECT_FALSE(observation.used_fallback);
    KEXPECT_EQ(observation.foreign_target, ker::mod::sched::can_query_current_task());
}
}  // namespace

KTEST(NumaDemand, OriginAndBackingDetermineEligibility) {
    using O = virt::DemandOrigin;
    using B = virt::DemandBacking;
    using E = virt::DemandExclusion;
    KEXPECT_EQ(virt::selftest_classify_private_demand(O::SoftwareEnsure, true, B::Anonymous, {}, true), E::Software);
    KEXPECT_EQ(virt::selftest_classify_private_demand(O::HardwareFault, false, B::Anonymous, {}, true), E::KernelFault);
    KEXPECT_EQ(virt::selftest_classify_private_demand(O::HardwareFault, true, B::Anonymous, {}, true), E::Eligible);
    KEXPECT_EQ(virt::selftest_classify_private_demand(O::HardwareFault, true, B::ZeroCow, {}, true), E::Eligible);
    KEXPECT_EQ(virt::selftest_classify_private_demand(O::HardwareFault, true, B::PrivateCowCopy, {}, true), E::PrivateCopy);
    KEXPECT_EQ(virt::selftest_classify_private_demand(O::HardwareFault, true, B::Anonymous, {}, false), E::NoLocalRam);
    for (auto mode : {numa::Mode::PREFERRED, numa::Mode::BIND, numa::Mode::INTERLEAVE})
        KEXPECT_EQ(virt::selftest_classify_private_demand(O::HardwareFault, true, B::Anonymous, {1, mode}, true), E::ExplicitPolicy);
    log::info("origin classifier only; software mapping cases do not execute a synthetic CPL3 fault or qualify user first touch");
    if (!ker::mod::sched::can_query_current_task())
        log::info("pre-scheduler software-target placement only; no scheduled foreign-executor or IF-on reclaim-yield runtime credit");
    auto const* machine = topology::boot_topology();
    if (machine != nullptr && machine->node_count != 1)
        log::info("native UMA IF-on legacy reclaim branch unexercised on this NUMA topology; Host covers dispatch only");
    bool memoryless = false;
    if (machine != nullptr)
        for (numa::NodeId node = 0; node < machine->node_count; ++node) {
            bool has_cpu = false;
            for (auto word : machine->nodes[node].cpus.words) has_cpu |= word != 0;
            memoryless |= machine->nodes[node].usable_bytes == 0 && has_cpu;
        }
    if (!memoryless) log::info("native CPU-on-memoryless-node fallback unexercised on this topology; classifier is not runtime credit");
}

KTEST(NumaDemand, LazyAnonymousBindIsZeroAndUsesTargetPolicy) {
    OwnerBalance balance;
    auto const* machine = topology::boot_topology();
    KREQUIRE_NE(machine, nullptr);
    for (numa::NodeId node = 0; node < machine->node_count; ++node) {
        if (machine->nodes[node].usable_bytes == 0) continue;
        OwnedTask owner;
        KREQUIRE_TRUE(owner.init());
        KREQUIRE_TRUE(owner.lazy());
        KREQUIRE_TRUE(owner.subject->selftest_set_memory_policy({numa::NodeMask{1} << node, numa::Mode::BIND}));
        virt::SelftestDemandObservation observation{};
        KREQUIRE_TRUE(virt::selftest_observe_private_demand(*owner.subject, VA, true, observation));
        expect_install(observation, node, virt::DemandBacking::Anonymous);
        auto* page = frame(owner.root);
        KEXPECT_TRUE(filled(page, 0));
        KEXPECT_EQ(numa::physical_node(physical(page)), node);
        KEXPECT_EQ(phys::page_ref_get(page), 1U);
    }
}

KTEST(NumaDemand, CopiedPolicySurvivesPostCaptureChange) {
    OwnerBalance balance;
    KREQUIRE_NE(topology::boot_topology(), nullptr);
    auto const MASK = memory_nodes();
    KREQUIRE_NE(MASK, 0U);
    auto const* machine = topology::boot_topology();
    numa::NodeId local = numa::INVALID_NODE;
    {
        ker::mod::sched::PreemptGuard const PREEMPT;
        auto const CPU = ker::mod::cpu::current_cpu();
        if (CPU < machine->cpu_count && CPU < machine->cpu_nodes.size()) local = machine->cpu_nodes[CPU];
    }
    auto node = static_cast<numa::NodeId>(std::countr_zero(MASK));
    for (numa::NodeId i = 0; i < machine->node_count; ++i)
        if (i != local && (MASK & (numa::NodeMask{1} << i)) != 0) node = i;
    auto const NODE = node;
    OwnedTask owner;
    KREQUIRE_TRUE(owner.init());
    KREQUIRE_TRUE(owner.lazy());
    KREQUIRE_TRUE(owner.subject->selftest_set_memory_policy({numa::NodeMask{1} << NODE, numa::Mode::BIND}));
    virt::SelftestDemandObservation observation{};
    KREQUIRE_TRUE(virt::selftest_observe_private_demand(*owner.subject, VA, true, observation,
                                                        virt::SelftestDemandAction::ChangePolicyAfterCapture, {}));
    expect_install(observation, NODE, virt::DemandBacking::Anonymous);
    KEXPECT_TRUE(observation.action_consumed);
    KEXPECT_EQ(observation.captured_policy.mode, numa::Mode::BIND);
    KEXPECT_EQ(owner.subject->memory_policy_snapshot().mode, numa::Mode::LOCAL);
    KEXPECT_EQ(numa::physical_node(physical(frame(owner.root))), NODE);
    if (NODE != observation.local_node) {
        log::info("post-capture policy contrast captured_node=%u actual_node=%u later_LOCAL_node=%u", NODE, observation.actual,
                  observation.local_node);
    } else {
        log::info("post-capture value continuity checked; this execution has no physical node contrast against later LOCAL");
    }
}

KTEST(NumaDemand, PrivateCowCopyKeepsPeerBytesAndConfinesDestination) {
    OwnerBalance balance;
    auto const MASK = memory_nodes();
    KREQUIRE_NE(MASK, 0U);
    auto const NODE = static_cast<numa::NodeId>(std::countr_zero(MASK));
    OwnedTask parent, child;
    KREQUIRE_TRUE(parent.init());
    KREQUIRE_TRUE(child.init());
    auto* original = parent.map(paging::page_types::USER);
    KREQUIRE_NE(original, nullptr);
    KREQUIRE_TRUE(clone(parent, child));
    KEXPECT_EQ(phys::page_ref_get(original), 2U);
    KREQUIRE_TRUE(child.subject->selftest_set_memory_policy({numa::NodeMask{1} << NODE, numa::Mode::BIND}));
    virt::SelftestDemandObservation observation{};
    KREQUIRE_TRUE(virt::selftest_observe_private_demand(*child.subject, VA, true, observation));
    expect_install(observation, NODE, virt::DemandBacking::PrivateCowCopy);
    KEXPECT_EQ(frame(parent.root), original);
    KEXPECT_NE(frame(child.root), original);
    KEXPECT_TRUE(filled(original, CONTENT));
    KEXPECT_TRUE(filled(frame(child.root), CONTENT));
    KEXPECT_EQ(phys::page_ref_get(original), 1U);
    KEXPECT_EQ(phys::page_ref_get(frame(child.root)), 1U);
    KEXPECT_EQ(numa::physical_node(physical(frame(child.root))), NODE);
}

KTEST(NumaDemand, GenuineZeroReserveCowUsesPlacedPrivateFrame) {
    OwnerBalance balance;
    auto const MASK = memory_nodes();
    KREQUIRE_NE(MASK, 0U);
    auto const NODE = static_cast<numa::NodeId>(std::countr_zero(MASK));
    OwnedTask owner;
    KREQUIRE_TRUE(owner.init());
    KREQUIRE_TRUE(vmem::selftest_map_anonymous_zero_page(*owner.subject, VA));
    auto* zero = frame(owner.root);
    KREQUIRE_NE(zero, nullptr);
    uint32_t const REFERENCES = phys::page_ref_get(zero);
    KREQUIRE_TRUE(REFERENCES > 1);
    KREQUIRE_TRUE(owner.subject->selftest_set_memory_policy({numa::NodeMask{1} << NODE, numa::Mode::BIND}));
    virt::SelftestDemandObservation observation{};
    KREQUIRE_TRUE(virt::selftest_observe_private_demand(*owner.subject, VA, true, observation));
    expect_install(observation, NODE, virt::DemandBacking::ZeroCow);
    KEXPECT_NE(frame(owner.root), zero);
    KEXPECT_TRUE(filled(frame(owner.root), 0));
    KEXPECT_TRUE(filled(zero, 0));
    KEXPECT_EQ(phys::page_ref_get(zero), REFERENCES - 1);
    KEXPECT_EQ(phys::page_ref_get(frame(owner.root)), 1U);
}

KTEST(NumaDemand, SoleReferencePromotionAllocatesNothing) {
    OwnerBalance balance;
    OwnedTask owner;
    KREQUIRE_TRUE(owner.init());
    auto* original = owner.map(paging::page_types::USER_READONLY | paging::PAGE_COW);
    KREQUIRE_NE(original, nullptr);
    virt::SelftestDemandObservation observation{};
    KREQUIRE_TRUE(virt::selftest_observe_private_demand(*owner.subject, VA, true, observation));
    KEXPECT_TRUE(observation.admitted && observation.disarmed);
    KEXPECT_EQ(observation.captures, 1U);
    KEXPECT_EQ(observation.promotions, 1U);
    KEXPECT_EQ(observation.candidates, 0U);
    KEXPECT_EQ(observation.installed, 0U);
    KEXPECT_EQ(frame(owner.root), original);
    KEXPECT_TRUE(filled(original, CONTENT));
    KEXPECT_EQ(phys::page_ref_get(original), 1U);
}

KTEST(NumaDemand, CowAllocationAndPermissionLossRetireEveryDestination) {
    OwnerBalance balance;
    for (auto action : {virt::SelftestDemandAction::FailCowAllocation, virt::SelftestDemandAction::DowngradeCowRead,
                        virt::SelftestDemandAction::DowngradeCowNone}) {
        OwnedTask parent, child;
        KREQUIRE_TRUE(parent.init());
        KREQUIRE_TRUE(child.init());
        auto* original = parent.map(paging::page_types::USER);
        KREQUIRE_NE(original, nullptr);
        KREQUIRE_TRUE(clone(parent, child));
        virt::SelftestDemandObservation observation{};
        KEXPECT_FALSE(virt::selftest_observe_private_demand(*child.subject, VA, true, observation, action));
        KEXPECT_TRUE(observation.admitted && observation.disarmed && observation.action_consumed);
        KEXPECT_EQ(observation.captures, 1U);
        KEXPECT_EQ(observation.candidates, 1U);
        KEXPECT_EQ(observation.installed, 0U);
        KEXPECT_EQ(observation.allocation_failures, action == virt::SelftestDemandAction::FailCowAllocation ? 1U : 0U);
        KEXPECT_EQ(observation.discarded, action == virt::SelftestDemandAction::FailCowAllocation ? 0U : 1U);
        KEXPECT_EQ(frame(parent.root), original);
        KEXPECT_EQ(frame(child.root), original);
        KEXPECT_EQ(phys::page_ref_get(original), 2U);
        KEXPECT_TRUE(filled(original, CONTENT));
    }
}

KTEST(NumaDemand, VirtualPageIndexInterleavesAcrossSelectedNodes) {
    OwnerBalance balance;
    auto const MASK = memory_nodes();
    KREQUIRE_NE(MASK, 0U);
    auto const COUNT = static_cast<size_t>(std::popcount(MASK));
    std::array<numa::NodeId, numa::MAX_NODES> nodes{};
    size_t n = 0;
    for (numa::NodeId node = 0; node < numa::MAX_NODES; ++node)
        if ((MASK & (numa::NodeMask{1} << node)) != 0) nodes[n++] = node;
    OwnedTask owner;
    KREQUIRE_TRUE(owner.init());
    KREQUIRE_TRUE(owner.lazy(COUNT * 2));
    KREQUIRE_TRUE(owner.subject->selftest_set_memory_policy({MASK, numa::Mode::INTERLEAVE}));
    for (size_t i = 0; i < COUNT * 2; ++i) {
        uint64_t const ADDRESS = VA + i * PAGE;
        auto const EXPECTED = nodes[(ADDRESS / PAGE) % COUNT];
        virt::SelftestDemandObservation observation{};
        KREQUIRE_TRUE(virt::selftest_observe_private_demand(*owner.subject, ADDRESS, false, observation));
        expect_install(observation, EXPECTED, virt::DemandBacking::Anonymous);
        KEXPECT_TRUE(filled(frame(owner.root, ADDRESS), 0));
        KEXPECT_EQ(numa::physical_node(physical(frame(owner.root, ADDRESS))), EXPECTED);
    }
}

KTEST(NumaDemand, ArmingRestoresBorrowedObservationAddressability) {
    OwnerBalance balance;
    OwnedTask owner;
    KREQUIRE_TRUE(owner.init());
    KREQUIRE_TRUE(owner.lazy());
    virt::SelftestDemandObservation observation{};
#ifdef WOS_KASAN
    if (ker::mod::kasan::is_enabled()) {
        ker::mod::kasan::poison_range(&observation, sizeof(observation), ker::mod::kasan::SHADOW_POISONED);
    }
#endif
    KREQUIRE_TRUE(virt::selftest_observe_private_demand(*owner.subject, VA, false, observation));
    KEXPECT_TRUE(observation.admitted && observation.disarmed);
    KEXPECT_EQ(observation.installed, 1U);
    KEXPECT_TRUE(filled(frame(owner.root), 0));
}

#endif  // WOS_SELFTEST
