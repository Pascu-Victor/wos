#ifdef WOS_SELFTEST

#include <abi/callnums/vmem.h>

#include <atomic>
#include <bit>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <platform/acpi/numa_boot.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/numa_alloc.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/lazy_vmem.hpp>
#include <platform/sched/task.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <test/ktest.hpp>

namespace {
namespace mm = ker::mod::mm;
namespace numa = mm::numa;
namespace phys = mm::phys;
namespace virt = mm::virt;
namespace paging = mm::paging;
namespace task = ker::mod::sched::task;
namespace vmem = ker::syscall::vmem;
namespace vmabi = ker::abi::vmem;
namespace topology = ker::mod::acpi::numa;
using log = ker::mod::dbg::logger<"numa-eager-test">;
constexpr uint64_t VA = 0x6D001000ULL;
constexpr uint64_t PAGE = paging::PAGE_SIZE;

auto counts() -> phys::PhysicalOwnerSnapshot {
    phys::PhysicalBalanceSnapshot snapshot{};
    phys::get_physical_balance_snapshot(snapshot);
    KEXPECT_EQ(snapshot.identity_mismatch_pages, 0U);
    return snapshot.owners[static_cast<size_t>(mm::PhysicalPageOwner::USER_PRIVATE_MAPPING)];
}

struct OwnedTask {
    phys::PhysicalOwnerSnapshot before{counts()};
    std::unique_ptr<task::Task> subject{new (std::nothrow) task::Task{}};
    paging::PageTable* root{};
    ~OwnedTask() {
        if (root != nullptr) {
            KEXPECT_EQ(subject->ref_count.load(std::memory_order_acquire), 1U);
            KEXPECT_EQ(subject->usercopy_pagemap_accesses.load(std::memory_order_acquire), 0U);
            KEXPECT_EQ(virt::selftest_user_pagemap_reader_count(root), 0U);
            KEXPECT_EQ(subject->detach_pagemap_after_usercopy_quiescence(), root);
            task::release_lazy_vmem_ranges(*subject);
            vmem::release_file_mmap_ranges_for_pagemap(root);
            virt::destroy_user_space(root, 0, "numa-eager", "owned-fixture");
            virt::release_pagemap(root);
        }
        auto const AFTER = counts();
        KEXPECT_EQ(AFTER.pages, before.pages);
        KEXPECT_EQ(AFTER.objects, before.objects);
    }
    auto init(numa::Policy policy) -> bool {
        if (subject == nullptr) return false;
        root = virt::create_pagemap();
        if (root == nullptr) return false;
        subject->type = task::TaskType::PROCESS;
        subject->pagemap = root;
        return subject->selftest_set_memory_policy(policy);
    }
};

auto memory_nodes() -> numa::NodeMask {
    auto const* machine = topology::boot_topology();
    numa::NodeMask nodes = 0;
    if (machine != nullptr)
        for (numa::NodeId i = 0; i < machine->node_count; ++i)
            if (machine->nodes[i].usable_bytes != 0) nodes |= numa::NodeMask{1} << i;
    return nodes;
}

auto raw_leaf(paging::PageTable* root, uint64_t address) -> uint64_t {
    auto* table = root;
    for (unsigned level = 4; level > 0; --level) {
        auto const INDEX = static_cast<size_t>((address >> (12U + 9U * (level - 1U))) & 511U);
        auto const& entry = table->entries.at(INDEX);
        if (level == 1) return std::bit_cast<uint64_t>(entry);
        if (entry.present == 0 || entry.pagesize != 0) return 0;
        table = reinterpret_cast<paging::PageTable*>(mm::addr::get_virt_pointer(static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT));
    }
    return 0;
}

auto frame(paging::PageTable* root, uint64_t address) -> uint8_t* {
    auto const PHYSICAL = virt::translate(root, address);
    return PHYSICAL == virt::PADDR_INVALID ? nullptr : reinterpret_cast<uint8_t*>(mm::addr::get_virt_pointer(PHYSICAL));
}

auto zero(const uint8_t* data) -> bool {
    if (data == nullptr) return false;
    for (size_t i = 0; i < PAGE; ++i)
        if (data[i] != 0) return false;
    return true;
}

void expect_page(OwnedTask& owner, uint64_t address, numa::NodeId node, bool writable) {
    auto* page = frame(owner.root, address);
    KEXPECT_TRUE(zero(page));
    if (page == nullptr) return;
    auto const PHYSICAL = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(page)));
    KEXPECT_EQ(numa::physical_node(PHYSICAL), node);
    KEXPECT_EQ(phys::page_ref_get(page), 1U);
    auto const RAW = raw_leaf(owner.root, address);
    KEXPECT_EQ(RAW & (paging::PAGE_PRESENT | paging::PAGE_USER | paging::PAGE_NX),
               paging::PAGE_PRESENT | paging::PAGE_USER | paging::PAGE_NX);
    KEXPECT_EQ((RAW & paging::PAGE_WRITE) != 0, writable);
    KEXPECT_EQ(RAW & (paging::PAGE_COW | paging::PAGE_SHARED), 0U);
}
}  // namespace

KTEST(NumaEager, PrivateReadPagesAreZeroAndConfinedToSuppliedActor) {
    auto nodes = memory_nodes();
    KREQUIRE_NE(nodes, 0U);
    while (nodes != 0) {
        auto const NODE = static_cast<numa::NodeId>(std::countr_zero(nodes));
        nodes &= nodes - 1;
        OwnedTask owner;
        KREQUIRE_TRUE(owner.init({numa::NodeMask{1} << NODE, numa::Mode::BIND}));
        KREQUIRE_EQ(vmem::selftest_allocate_private_anonymous(*owner.subject, VA, 3 * PAGE, vmabi::PROT_READ), VA);
        for (uint64_t i = 0; i < 3; ++i) expect_page(owner, VA + i * PAGE, NODE, false);
        auto const MEMORY = virt::collect_user_memory_stats(owner.root);
        KEXPECT_EQ(MEMORY.resident_pages, 3U);
        KEXPECT_EQ(owner.subject->lazy_vmem_ranges.size(), 1U);
        KEXPECT_EQ(owner.subject->lazy_vmem_ranges.at(0).start, VA);
        KEXPECT_EQ(owner.subject->lazy_vmem_ranges.at(0).prot, vmabi::PROT_READ);
    }
    log::info("actual private helper and supplied never-active actor only; no current-only syscall quota or user first-touch credit");
}

KTEST(NumaEager, PrivateWritableHelperInterleavesByVirtualPageIndex) {
    auto const NODES = memory_nodes();
    KREQUIRE_NE(NODES, 0U);
    OwnedTask owner;
    numa::Policy const POLICY{NODES, numa::Mode::INTERLEAVE};
    KREQUIRE_TRUE(owner.init(POLICY));
    numa::AllocationContext context;
    KREQUIRE_EQ(numa::capture_allocation_context(POLICY, context), numa::AllocationError::NONE);
    KREQUIRE_EQ(vmem::selftest_allocate_private_anonymous(*owner.subject, VA, 5 * PAGE, vmabi::PROT_READ | vmabi::PROT_WRITE), VA);
    for (uint64_t i = 0; i < 5; ++i) {
        auto const SELECTION = numa::select_nodes(POLICY, *topology::boot_topology(), context.local_node, (VA + i * PAGE) / PAGE);
        KREQUIRE_TRUE(SELECTION.valid());
        expect_page(owner, VA + i * PAGE, SELECTION.requested, true);
    }
    log::info("explicit private RW helper exercised; public MAP_POPULATE can still use shared-zero COW, not private eager credit");
}

KTEST(NumaEager, ControlledPrefixFailureRollsBackEveryPrivateFrame) {
    auto const NODES = memory_nodes();
    KREQUIRE_NE(NODES, 0U);
    auto const NODE = static_cast<numa::NodeId>(std::countr_zero(NODES));
    for (uint64_t fail : {0ULL, 1ULL, 3ULL}) {
        OwnedTask owner;
        KREQUIRE_TRUE(owner.init({numa::NodeMask{1} << NODE, numa::Mode::BIND}));
        auto const BEFORE = counts();
        auto const CURSOR = owner.subject->mmap_next.load(std::memory_order_acquire);
        KEXPECT_EQ(vmem::selftest_allocate_private_anonymous(*owner.subject, VA, 4 * PAGE, vmabi::PROT_READ, fail),
                   static_cast<uint64_t>(-ENOMEM));
        auto const AFTER = counts();
        KEXPECT_EQ(AFTER.pages, BEFORE.pages);
        KEXPECT_EQ(AFTER.objects, BEFORE.objects);
        KEXPECT_EQ(virt::collect_user_memory_stats(owner.root).resident_pages, 0U);
        KEXPECT_TRUE(virt::is_user_virtual_range_free(owner.root, VA, VA + 4 * PAGE));
        KEXPECT_TRUE(owner.subject->lazy_vmem_ranges.empty());
        KEXPECT_EQ(owner.subject->mmap_next.load(std::memory_order_acquire), CURSOR);
    }
    log::info("controlled per-call null result uses real prefix rollback; no allocator pressure or public AS-reservation failure claim");
}

KTEST(NumaEager, ReservedNoneReadAndFailureKeepActualLeafSemantics) {
    auto const NODES = memory_nodes();
    KREQUIRE_NE(NODES, 0U);
    auto const NODE = static_cast<numa::NodeId>(std::countr_zero(NODES));
    OwnedTask owner;
    KREQUIRE_TRUE(owner.init({numa::NodeMask{1} << NODE, numa::Mode::BIND}));
    virt::reserve_page_range(owner.root, VA, 1);
    auto const MARKER = raw_leaf(owner.root, VA);
    auto const BEFORE = counts();
    KREQUIRE_TRUE(vmem::selftest_materialize_anonymous_reserved(*owner.subject, VA, vmabi::PROT_NONE));
    KEXPECT_EQ(raw_leaf(owner.root, VA), MARKER);
    KEXPECT_FALSE(vmem::selftest_materialize_anonymous_reserved(*owner.subject, VA, vmabi::PROT_READ, true));
    KEXPECT_EQ(raw_leaf(owner.root, VA), MARKER);
    KEXPECT_EQ(counts().pages, BEFORE.pages);
    KEXPECT_EQ(counts().objects, BEFORE.objects);
    KREQUIRE_TRUE(vmem::selftest_materialize_anonymous_reserved(*owner.subject, VA, vmabi::PROT_READ));
    expect_page(owner, VA, NODE, false);
    auto const PHYSICAL = virt::translate(owner.root, VA);
    virt::unify_page_flags(owner.root, VA, paging::PAGE_PRESENT | paging::PAGE_NX);
    KEXPECT_EQ(virt::translate(owner.root, VA), PHYSICAL);
    KEXPECT_EQ(raw_leaf(owner.root, VA) & paging::PAGE_USER, 0U);
    virt::unify_page_flags(owner.root, VA, paging::page_types::USER | paging::PAGE_NX);
    KEXPECT_EQ(virt::translate(owner.root, VA), PHYSICAL);
    expect_page(owner, VA, NODE, true);
    log::info("actual reserved materializer and existing leaf transitions; no whole current-only PROTECT transaction/OOM atomicity credit");
}

KTEST(NumaEager, GenuineZeroReserveStillDefersPrivatePlacementToCow) {
    auto const NODES = memory_nodes();
    KREQUIRE_NE(NODES, 0U);
    auto const NODE = static_cast<numa::NodeId>(std::countr_zero(NODES));
    OwnedTask owner;
    KREQUIRE_TRUE(owner.init({numa::NodeMask{1} << NODE, numa::Mode::BIND}));
    virt::reserve_page_range(owner.root, VA, 1);
    auto const BEFORE = counts();
    KREQUIRE_TRUE(vmem::selftest_map_anonymous_zero_page(*owner.subject, VA));
    KEXPECT_TRUE(zero(frame(owner.root, VA)));
    KEXPECT_EQ(raw_leaf(owner.root, VA) & (paging::PAGE_WRITE | paging::PAGE_COW), paging::PAGE_COW);
    KEXPECT_EQ(counts().pages, BEFORE.pages);
    KEXPECT_EQ(counts().objects, BEFORE.objects);
    virt::SelftestDemandObservation observation{};
    KREQUIRE_TRUE(virt::selftest_observe_private_demand(*owner.subject, VA, true, observation));
    KEXPECT_EQ(observation.captures, 1U);
    KEXPECT_EQ(observation.installed, 1U);
    KEXPECT_EQ(observation.backing, virt::DemandBacking::ZeroCow);
    KEXPECT_EQ(observation.actual, NODE);
    KEXPECT_TRUE(observation.captured_policy == owner.subject->memory_policy_snapshot());
    expect_page(owner, VA, NODE, true);
}

#endif
