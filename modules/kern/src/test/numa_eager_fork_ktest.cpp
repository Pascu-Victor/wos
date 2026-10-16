#ifdef WOS_SELFTEST

#include <array>
#include <bit>
#include <cstdint>
#include <cstring>
#include <platform/acpi/numa_boot.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/numa_alloc.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <test/ktest.hpp>

namespace {
namespace mm = ker::mod::mm;
namespace numa = mm::numa;
namespace virt = mm::virt;
namespace phys = mm::phys;
namespace paging = mm::paging;
namespace topology = ker::mod::acpi::numa;
namespace vmem = ker::syscall::vmem;
using log = ker::mod::dbg::logger<"numa-fork-test">;
constexpr uint64_t PAGE = paging::PAGE_SIZE;
constexpr uint64_t VA = 0x6F1FE000ULL;
// Exclusive prescheduler fixtures only. The heavy owner snapshot is static;
// these tests neither assert cross-time global-free equality nor SMP quiescence.
phys::PhysicalBalanceSnapshot balance;
auto owner_counts() -> phys::PhysicalOwnerSnapshot {
    phys::get_physical_balance_snapshot(balance);
    KEXPECT_EQ(balance.identity_mismatch_pages, 0U);
    return balance.owners[static_cast<size_t>(mm::PhysicalPageOwner::USER_PRIVATE_MAPPING)];
}
struct OwnerClosure {
    phys::PhysicalOwnerSnapshot before{owner_counts()};
    ~OwnerClosure() {
        auto const AFTER = owner_counts();
        KEXPECT_EQ(AFTER.pages, before.pages);
        KEXPECT_EQ(AFTER.objects, before.objects);
    }
};
struct Root {
    paging::PageTable* value{};
    ~Root() {
        if (value == nullptr) return;
        KEXPECT_EQ(virt::selftest_user_pagemap_reader_count(value), 0U);
        virt::destroy_user_space(value, 0, "numa-fork", "exclusive-unpublished-fixture");
        virt::release_pagemap(value);
    }
    auto init() -> bool {
        value = virt::create_pagemap();
        return value != nullptr;
    }
    auto map(uint64_t address, uint8_t pattern) -> bool {
        auto* page =
            static_cast<uint8_t*>(phys::page_alloc_may_fail(mm::PhysicalPageOwner::USER_PRIVATE_MAPPING, PAGE, "numa-fork-source"));
        if (page == nullptr) return false;
        for (size_t i = 0; i < PAGE; ++i) page[i] = static_cast<uint8_t>(pattern + i * 31U);
        auto const PA = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(page)));
        virt::map_page(value, address, PA, paging::PAGE_PRESENT | paging::PAGE_USER | paging::PAGE_WRITE | paging::PAGE_NX);
        return true;
    }
};
auto memory_nodes() -> numa::NodeMask {
    auto const* machine = topology::boot_topology();
    numa::NodeMask result = 0;
    if (machine != nullptr)
        for (numa::NodeId node = 0; node < machine->node_count; ++node)
            if (machine->nodes[node].usable_bytes != 0) result |= numa::NodeMask{1} << node;
    return result;
}
auto frame(Root& root, uint64_t address) -> uint8_t* {
    auto const PA = virt::translate(root.value, address);
    return PA == virt::PADDR_INVALID ? nullptr : reinterpret_cast<uint8_t*>(mm::addr::get_virt_pointer(PA));
}
auto copy(Root& source, Root& child, numa::AllocationContext& context, numa::AllocationResult& output, uint64_t fail = UINT64_MAX) -> bool {
    // Named caller objects are instrumented and already alive before owners.
    vmem::SharedVmemPublicationGuard const PUB;
    ker::mod::sched::PreemptGuard const COW_PREEMPT;
    return virt::selftest_deep_copy_user_pagemap_eager(source.value, child.value, context, output, fail);
}
void expect_copy(Root& source, Root& child, uint64_t address, numa::NodeId expected) {
    auto* a = frame(source, address);
    auto* b = frame(child, address);
    KEXPECT_TRUE(a != nullptr && b != nullptr);
    if (a == nullptr || b == nullptr) return;
    KEXPECT_NE(a, b);
    KEXPECT_EQ(std::memcmp(a, b, PAGE), 0);
    KEXPECT_EQ(phys::page_ref_get(a), 1U);
    KEXPECT_EQ(phys::page_ref_get(b), 1U);
    auto const CHILD_PA = reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(b)));
    KEXPECT_EQ(numa::physical_node(CHILD_PA), expected);
    auto* table = child.value;
    for (unsigned level = 4; level > 1; --level) {
        auto const& e = table->entries[(address >> (12U + 9U * (level - 1U))) & 511U];
        KEXPECT_TRUE(e.present != 0 && e.pagesize == 0);
        if (e.present == 0 || e.pagesize != 0) return;
        table = reinterpret_cast<paging::PageTable*>(mm::addr::get_virt_pointer(static_cast<uint64_t>(e.frame) << paging::PAGE_SHIFT));
    }
    auto const RAW = std::bit_cast<uint64_t>(table->entries[(address >> paging::PAGE_SHIFT) & 511U]);
    KEXPECT_EQ(RAW & (paging::PAGE_PRESENT | paging::PAGE_USER | paging::PAGE_WRITE | paging::PAGE_NX),
               paging::PAGE_PRESENT | paging::PAGE_USER | paging::PAGE_WRITE | paging::PAGE_NX);
    KEXPECT_EQ(RAW & (paging::PAGE_COW | paging::PAGE_SHARED), 0U);
}
}  // namespace

KTEST(NumaEagerFork, RealCopiesConfinedAndIndependentOnEveryMemoryNode) {
    OwnerClosure const CLOSURE;
    auto nodes = memory_nodes();
    KREQUIRE_NE(nodes, 0U);
    while (nodes != 0) {
        auto const NODE = static_cast<numa::NodeId>(std::countr_zero(nodes));
        nodes &= nodes - 1;
        Root source, child;
        KREQUIRE_TRUE(source.init() && child.init());
        for (unsigned i = 0; i < 3; ++i) KREQUIRE_TRUE(source.map(VA + i * PAGE, static_cast<uint8_t>(0x30 + i)));
        numa::AllocationContext context;
        numa::AllocationResult output;
        KREQUIRE_EQ(numa::capture_allocation_context({numa::NodeMask{1} << NODE, numa::Mode::BIND}, context), numa::AllocationError::NONE);
        auto const BEFORE = owner_counts();
        KREQUIRE_TRUE(copy(source, child, context, output));
        KEXPECT_TRUE(output.span.empty());
        for (unsigned i = 0; i < 3; ++i) expect_copy(source, child, VA + i * PAGE, NODE);
        auto const AFTER = owner_counts();
        KEXPECT_EQ(AFTER.pages, BEFORE.pages + 3);
        KEXPECT_EQ(AFTER.objects, BEFORE.objects + 3);
    }
    log::info("real exclusive unpublished roots/data refs only; no scheduled fork/quota/IF-on reclaim/CPL3 snapshot credit");
}

KTEST(NumaEagerFork, InterleaveUsesVirtualPageIndexAcrossTableBoundary) {
    OwnerClosure const CLOSURE;
    auto const NODES = memory_nodes();
    KREQUIRE_NE(NODES, 0U);
    Root source, child;
    KREQUIRE_TRUE(source.init() && child.init());
    for (unsigned i = 0; i < 5; ++i) KREQUIRE_TRUE(source.map(VA + i * PAGE, static_cast<uint8_t>(0x50 + i)));
    numa::AllocationContext context;
    numa::AllocationResult output;
    KREQUIRE_EQ(numa::capture_allocation_context({NODES, numa::Mode::INTERLEAVE}, context), numa::AllocationError::NONE);
    auto const POLICY = context.policy;
    auto const LOCAL = context.local_node;
    KREQUIRE_TRUE(copy(source, child, context, output));
    for (unsigned i = 0; i < 5; ++i) {
        auto const SELECTED = numa::select_nodes(POLICY, *topology::boot_topology(), LOCAL, VA / PAGE + i);
        KREQUIRE_TRUE(SELECTED.valid());
        expect_copy(source, child, VA + i * PAGE, SELECTED.requested);
    }
    KEXPECT_TRUE(context.policy == POLICY);
    KEXPECT_EQ(context.local_node, LOCAL);
    KEXPECT_EQ(context.virtual_page_index, VA / PAGE + 4);
    log::info(
        "requested-node oracle assumes ample-free native fixture; factory may fall back within allowed interleave mask under pressure");
}

KTEST(NumaEagerFork, ControlledPrefixRefusalRetainsRealOwnersUntilCleanup) {
    OwnerClosure const CLOSURE;
    auto const NODES = memory_nodes();
    KREQUIRE_NE(NODES, 0U);
    auto const NODE = static_cast<numa::NodeId>(std::countr_zero(NODES));
    for (uint64_t fail : {0ULL, 1ULL, 3ULL}) {
        Root source, child;
        KREQUIRE_TRUE(source.init() && child.init());
        for (unsigned i = 0; i < 4; ++i) KREQUIRE_TRUE(source.map(VA + i * PAGE, static_cast<uint8_t>(0x70 + i)));
        numa::AllocationContext context;
        numa::AllocationResult output;
        KREQUIRE_EQ(numa::capture_allocation_context({numa::NodeMask{1} << NODE, numa::Mode::BIND}, context), numa::AllocationError::NONE);
        auto const BEFORE = owner_counts();
        KEXPECT_FALSE(copy(source, child, context, output, fail));
        KEXPECT_TRUE(output.span.empty());
        for (uint64_t i = 0; i < fail; ++i) expect_copy(source, child, VA + i * PAGE, NODE);
        KEXPECT_EQ(frame(child, VA + fail * PAGE), nullptr);
        auto const AFTER = owner_counts();
        KEXPECT_EQ(AFTER.pages, BEFORE.pages + fail);
        KEXPECT_EQ(AFTER.objects, BEFORE.objects + fail);
    }
    log::info("per-call refusal before real factory; actual child prefix reclaimed by ordinary root teardown, not a PMM pressure test");
}

KTEST(NumaEagerFork, InvalidPolicyAndOwnedOutputRefuseBeforeDestinationMutation) {
    OwnerClosure const CLOSURE;
    Root source, child;
    KREQUIRE_TRUE(source.init() && child.init());
    KREQUIRE_TRUE(source.map(VA, 0x90));
    auto const NODES = memory_nodes();
    KREQUIRE_NE(NODES, 0U);
    auto const NODE = static_cast<numa::NodeId>(std::countr_zero(NODES));
    numa::AllocationContext context;
    numa::AllocationResult output;
    KREQUIRE_EQ(numa::capture_allocation_context({numa::NodeMask{1} << NODE, numa::Mode::BIND}, context), numa::AllocationError::NONE);
    auto const GOOD = context.policy;
    context.policy = {0, numa::Mode::BIND};
    KEXPECT_FALSE(copy(source, child, context, output));
    KEXPECT_EQ(frame(child, VA), nullptr);
    context.policy = GOOD;
    numa::try_allocate_pages_into(output, context, 1, PAGE, mm::PhysicalPageOwner::USER_PRIVATE_MAPPING,
                                  phys::PageInitialization::FULL_OVERWRITE, numa::ReclaimMode::NEVER, "numa-fork-owned-output");
    KREQUIRE_TRUE(!output.span.empty());
    auto* const OWNED = output.span.data();
    KEXPECT_FALSE(copy(source, child, context, output));
    KEXPECT_EQ(output.span.data(), OWNED);
    KEXPECT_EQ(phys::page_ref_get(OWNED), 1U);
    KEXPECT_EQ(frame(child, VA), nullptr);
    // Output reset/destruction occurs in this instrumented TU after every guard.
}

#endif
