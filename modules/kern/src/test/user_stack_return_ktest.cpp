#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <platform/acpi/numa_boot.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/loader/elf_loader.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/numa_alloc.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/task.hpp>
#include <platform/sched/threading.hpp>
#include <test/ktest.hpp>

namespace {
namespace addr = ker::mod::mm::addr;
namespace paging = ker::mod::mm::paging;
namespace phys = ker::mod::mm::phys;
namespace task = ker::mod::sched::task;
namespace threading = ker::mod::sched::threading;
namespace virt = ker::mod::mm::virt;

constexpr uint64_t PAGE = paging::PAGE_SIZE;
constexpr uint64_t USER_LIMIT = 0x0000800000000000ULL;
constexpr uint64_t STACK_SIZE = 64 * PAGE;
constexpr uint64_t INITIAL_BYTES = 32 * 1024;
constexpr unsigned STACK_RESOURCE = static_cast<unsigned>(ker::abi::resource::Id::STACK);
constexpr size_t MAX_LAYOUT_PAGES = 128;

// Real initial/exec stack constructor and private root. Nothing is scheduled,
// registered by PID, executed or attached to a borrowed address space.
struct PrivateStack {
    ker::mod::mm::numa::AllocationContext allocation_context{};
    std::unique_ptr<task::Task> owner{new (std::nothrow) task::Task{}};
    paging::PageTable* root{};
    threading::Thread* thread{};
    uint64_t initial_threads = threading::get_active_thread_count();

    PrivateStack() = default;
    PrivateStack(const PrivateStack&) = delete;
    PrivateStack(PrivateStack&&) = delete;
    auto operator=(const PrivateStack&) -> PrivateStack& = delete;
    auto operator=(PrivateStack&&) -> PrivateStack& = delete;

    ~PrivateStack() {
        if (owner != nullptr) {
            KEXPECT_EQ(owner->usercopy_pagemap_accesses.load(std::memory_order_acquire), 0U);
            owner->thread = nullptr;
            if (root != nullptr) KEXPECT_EQ(owner->detach_pagemap_after_usercopy_quiescence(), root);
        }
        if (thread != nullptr) threading::destroy_thread(thread);
        if (root != nullptr) {
            virt::destroy_user_space(root, 0, "stack-return-ktest", "private-fixture");
            virt::release_pagemap(root);
        }
        KEXPECT_EQ(threading::get_active_thread_count(), initial_threads);
    }

    auto init(const ker::mod::mm::numa::AllocationContext* supplied = nullptr) -> bool {
        if (owner == nullptr) return false;
        root = virt::create_pagemap_may_fail();
        if (root == nullptr) return false;
        owner->type = task::TaskType::PROCESS;
        owner->pagemap = root;
        ker::loader::elf::TlsModule const TLS{};
        if (supplied != nullptr)
            allocation_context = *supplied;
        else if (ker::mod::mm::numa::capture_allocation_context({}, allocation_context) != ker::mod::mm::numa::AllocationError::NONE)
            return false;
        thread = threading::create_thread(STACK_SIZE, 0, root, 0, TLS, owner.get(), allocation_context);
        if (thread == nullptr) return false;
        owner->thread = thread;
        threading::record_stack_usage(*owner);
        return thread->layout_size / PAGE <= MAX_LAYOUT_PAGES;
    }

    auto top() const -> uint64_t { return thread->stack_base_virt + thread->stack_size; }
    auto untouched() const -> uint64_t { return top() - 16 * PAGE; }
    auto admitted(uint64_t address) const -> bool { return threading::user_stack_return_address_is_admitted(thread, root, address); }
};

struct EmptyRoot {
    paging::PageTable* root = virt::create_pagemap_may_fail();
    EmptyRoot() = default;
    EmptyRoot(const EmptyRoot&) = delete;
    EmptyRoot(EmptyRoot&&) = delete;
    auto operator=(const EmptyRoot&) -> EmptyRoot& = delete;
    auto operator=(EmptyRoot&&) -> EmptyRoot& = delete;
    ~EmptyRoot() {
        if (root != nullptr) {
            virt::destroy_user_space(root, 0, "stack-return-empty", "private-fixture");
            virt::release_pagemap(root);
        }
    }
};

auto leaf_raw(paging::PageTable* root, uint64_t va) -> uint64_t {
    auto* table = root;
    for (unsigned level = 4; level > 1; --level) {
        auto const& entry = table->entries.at((va >> (12U + (level - 1U) * 9U)) & 0x1FFU);
        if (entry.present == 0 || (level < 4 && entry.pagesize != 0)) return 0;
        table = reinterpret_cast<paging::PageTable*>(addr::get_virt_pointer(entry.frame << paging::PAGE_SHIFT));
    }
    uint64_t raw = 0;
    auto const& entry = table->entries.at((va >> paging::PAGE_SHIFT) & 0x1FFU);
    std::memcpy(&raw, &entry, sizeof(raw));
    return raw;
}

auto mapped_ref(paging::PageTable* root, uint64_t va) -> uint32_t {
    uint64_t const PHYSICAL = virt::translate(root, va);
    return PHYSICAL == virt::PADDR_INVALID ? 0 : phys::page_ref_get(addr::get_virt_pointer(PHYSICAL));
}

// Snapshot exact private PTE/frame ownership and policy/lifetime observations.
// No global allocator census or arbitrary concurrent-writer claim is made.
struct AdmissionSnapshot {
    threading::Thread thread;
    std::array<ker::abi::resource::Usage, ker::abi::resource::COUNT> resources;
    std::array<uint64_t, MAX_LAYOUT_PAGES> leaves{};
    std::array<uint32_t, MAX_LAYOUT_PAGES> frame_refs{};
    uint32_t root_refs;
    uint32_t task_refs;
    uint32_t root_readers;
    uint32_t task_accesses;

    explicit AdmissionSnapshot(const PrivateStack& fixture)
        : thread(*fixture.thread),
          resources(fixture.owner->resources.entries),
          root_refs(phys::page_ref_get(fixture.root)),
          task_refs(fixture.owner->ref_count.load(std::memory_order_acquire)),
          root_readers(virt::selftest_user_pagemap_reader_count(fixture.root)),
          task_accesses(fixture.owner->usercopy_pagemap_accesses.load(std::memory_order_acquire)) {
        for (size_t index = 0; index < thread.layout_size / PAGE; ++index) {
            uint64_t const VA = thread.layout_base_virt + index * PAGE;
            leaves.at(index) = leaf_raw(fixture.root, VA);
            frame_refs.at(index) = mapped_ref(fixture.root, VA);
        }
    }

    void expect_unchanged(const PrivateStack& fixture) const {
        KEXPECT_EQ(std::memcmp(&thread, fixture.thread, sizeof(thread)), 0);
        KEXPECT_EQ(phys::page_ref_get(fixture.root), root_refs);
        KEXPECT_EQ(fixture.owner->ref_count.load(std::memory_order_acquire), task_refs);
        KEXPECT_EQ(virt::selftest_user_pagemap_reader_count(fixture.root), root_readers);
        KEXPECT_EQ(fixture.owner->usercopy_pagemap_accesses.load(std::memory_order_acquire), task_accesses);
        KEXPECT_EQ(fixture.owner->pagemap, fixture.root);
        KEXPECT_EQ(fixture.owner->thread, fixture.thread);
        KEXPECT_EQ(fixture.owner->pid, 0U);
        KEXPECT_FALSE(fixture.owner->scheduler_published.load(std::memory_order_acquire));
        KEXPECT_TRUE(fixture.owner->lazy_vmem_ranges.empty());
        for (size_t index = 0; index < resources.size(); ++index) {
            auto const& actual = fixture.owner->resources.entries.at(index);
            auto const& expected = resources.at(index);
            KEXPECT_EQ(actual.limit.soft, expected.limit.soft);
            KEXPECT_EQ(actual.limit.hard, expected.limit.hard);
            KEXPECT_EQ(actual.current, expected.current);
            KEXPECT_EQ(actual.peak, expected.peak);
            KEXPECT_EQ(actual.denied, expected.denied);
            KEXPECT_EQ(actual.reason, expected.reason);
            KEXPECT_EQ(actual.reserved, expected.reserved);
        }
        for (size_t index = 0; index < thread.layout_size / PAGE; ++index) {
            uint64_t const VA = thread.layout_base_virt + index * PAGE;
            KEXPECT_EQ(leaf_raw(fixture.root, VA), leaves.at(index));
            KEXPECT_EQ(mapped_ref(fixture.root, VA), frame_refs.at(index));
        }
    }
};
}  // namespace

KTEST(UserStackReturn, ReservedInteriorAndSequentialBacking) {
    PrivateStack fixture;
    KREQUIRE_TRUE(fixture.init());
    KEXPECT_TRUE(threading::has_automatic_stack(fixture.thread));
    KEXPECT_EQ(threading::get_active_thread_count(), fixture.initial_threads + 1);
    KEXPECT_EQ(fixture.thread->stack_lowest_backed, fixture.top() - INITIAL_BYTES);
    KEXPECT_TRUE(fixture.owner->lazy_vmem_ranges.empty());
    uint64_t const TARGET = fixture.untouched();
    KEXPECT_TRUE(TARGET < fixture.thread->stack_lowest_backed);
    KEXPECT_EQ(virt::translate(fixture.root, TARGET), virt::PADDR_INVALID);
    KEXPECT_TRUE(virt::is_page_reserved(fixture.root, TARGET));
    AdmissionSnapshot const BEFORE(fixture);
    KEXPECT_TRUE(fixture.admitted(TARGET));
    KEXPECT_TRUE(fixture.admitted(fixture.thread->stack_base_virt));
    KEXPECT_TRUE(fixture.admitted(fixture.top() - 1));
    BEFORE.expect_unchanged(fixture);

    // Earlier-unmapped -> genuine backing -> current mapped admission. This
    // is sequential, not an assertion about arbitrary live SMP leaf writers.
    KREQUIRE_TRUE(threading::ensure_stack_backing(fixture.thread, fixture.root, TARGET, TARGET + PAGE, fixture.owner.get(),
                                                  fixture.allocation_context));
    KEXPECT_NE(virt::translate(fixture.root, TARGET), virt::PADDR_INVALID);
    KEXPECT_FALSE(virt::is_page_reserved(fixture.root, TARGET));
    KEXPECT_EQ(mapped_ref(fixture.root, TARGET), 1U);
    AdmissionSnapshot const BACKED(fixture);
    KEXPECT_TRUE(fixture.admitted(TARGET));
    BACKED.expect_unchanged(fixture);
}

KTEST(UserStackReturn, GeometryAndRemovedReservationRefuse) {
    PrivateStack fixture;
    KREQUIRE_TRUE(fixture.init());
    EmptyRoot empty;
    KREQUIRE_NE(empty.root, nullptr);
    uint64_t const TARGET = fixture.untouched();
    AdmissionSnapshot const BEFORE(fixture);
    KEXPECT_FALSE(threading::user_stack_return_address_is_admitted(nullptr, fixture.root, TARGET));
    KEXPECT_FALSE(threading::user_stack_return_address_is_admitted(fixture.thread, nullptr, TARGET));
    KEXPECT_FALSE(threading::user_stack_return_address_is_admitted(fixture.thread, empty.root, TARGET));
    for (uint64_t const ADDRESS :
         {uint64_t{0}, USER_LIMIT, UINT64_MAX, fixture.thread->layout_base_virt, fixture.thread->stack_base_virt - 1, fixture.top(),
          fixture.thread->tls_base_virt - 1, fixture.thread->tls_base_virt, fixture.thread->safestack_ptr_value,
          fixture.thread->layout_base_virt + fixture.thread->layout_size - PAGE})
        KEXPECT_FALSE(fixture.admitted(ADDRESS));
    auto refuse = [&](auto mutate) {
        auto altered = *fixture.thread;
        mutate(altered);
        KEXPECT_FALSE(threading::user_stack_return_address_is_admitted(&altered, fixture.root, TARGET));
    };
    refuse([](auto& thread) { thread.layout_base_virt = 0; });
    refuse([](auto& thread) { thread.layout_size = 0; });
    refuse([](auto& thread) { thread.stack_base_virt = 0; });
    refuse([](auto& thread) { thread.stack_size = 0; });
    refuse([](auto& thread) { thread.tls_base_virt = 0; });
    refuse([](auto& thread) { ++thread.layout_base_virt; });
    refuse([](auto& thread) { ++thread.layout_size; });
    refuse([](auto& thread) { ++thread.stack_base_virt; });
    refuse([](auto& thread) { ++thread.stack_size; });
    refuse([](auto& thread) { ++thread.tls_base_virt; });
    refuse([](auto& thread) { thread.layout_base_virt -= PAGE; });
    refuse([](auto& thread) { thread.layout_size = UINT64_MAX & ~(PAGE - 1); });
    refuse([](auto& thread) { thread.stack_size = UINT64_MAX & ~(PAGE - 1); });
    refuse([](auto& thread) { thread.layout_size = USER_LIMIT - thread.layout_base_virt; });
    refuse([](auto& thread) { thread.stack_size = USER_LIMIT - thread.stack_base_virt; });
    refuse([](auto& thread) { thread.stack_size += 4 * PAGE; });
    refuse([](auto& thread) { thread.layout_size = thread.stack_size + PAGE; });
    refuse([](auto& thread) { thread.tls_base_virt -= PAGE; });
    refuse([](auto& thread) { thread.tls_base_virt += PAGE; });
    refuse([](auto& thread) {
        thread.layout_base_virt = USER_LIMIT;
        thread.stack_base_virt = USER_LIMIT + PAGE;
        thread.tls_base_virt = thread.stack_base_virt + thread.stack_size + PAGE;
    });
    BEFORE.expect_unchanged(fixture);

    // Actually remove the leaf reservation; geometry alone must not admit it.
    virt::unmap_page(fixture.root, TARGET);
    KEXPECT_EQ(leaf_raw(fixture.root, TARGET), 0U);
    KEXPECT_FALSE(virt::is_page_mapped_or_reserved(fixture.root, TARGET));
    AdmissionSnapshot const REMOVED(fixture);
    KEXPECT_FALSE(fixture.admitted(TARGET));
    KEXPECT_TRUE(fixture.admitted(TARGET + PAGE));
    REMOVED.expect_unchanged(fixture);
}

KTEST(UserStackReturn, FaultBudgetAndAdmissionAreSeparate) {
    PrivateStack fixture;
    KREQUIRE_TRUE(fixture.init());
    auto& owner = *fixture.owner;
    uint64_t const TARGET = fixture.untouched();
    KREQUIRE_EQ(owner.resources.set(STACK_RESOURCE, {INITIAL_BYTES, UINT64_MAX}, true, 256), 0);
    AdmissionSnapshot const LIMITED(fixture);
    KEXPECT_TRUE(fixture.admitted(TARGET));
    LIMITED.expect_unchanged(fixture);
    auto const BEFORE = owner.resources.entries.at(STACK_RESOURCE);
    KEXPECT_FALSE(threading::handle_lazy_stack_fault(fixture.thread, fixture.root, TARGET, TARGET, &owner));
    auto const& denied = owner.resources.entries.at(STACK_RESOURCE);
    KEXPECT_EQ(denied.current, BEFORE.current);
    KEXPECT_EQ(denied.peak, BEFORE.peak);
    KEXPECT_EQ(denied.denied, BEFORE.denied + 1);
    KEXPECT_EQ(denied.reason, ker::abi::resource::Denial::SOFT_LIMIT);
    KEXPECT_EQ(fixture.thread->stack_lowest_backed, fixture.top() - INITIAL_BYTES);
    KEXPECT_EQ(virt::translate(fixture.root, TARGET), virt::PADDR_INVALID);
    KEXPECT_TRUE(virt::is_page_reserved(fixture.root, TARGET));

    KREQUIRE_EQ(owner.resources.set(STACK_RESOURCE, {STACK_SIZE, UINT64_MAX}, true, 256), 0);
    KREQUIRE_TRUE(threading::handle_lazy_stack_fault(fixture.thread, fixture.root, TARGET, TARGET, &owner));
    uint64_t const PHYSICAL = virt::translate(fixture.root, TARGET);
    KREQUIRE_NE(PHYSICAL, virt::PADDR_INVALID);
    KEXPECT_EQ(mapped_ref(fixture.root, TARGET), 1U);
    KEXPECT_EQ(owner.resources.entries.at(STACK_RESOURCE).current, STACK_SIZE);
    KEXPECT_EQ(owner.resources.entries.at(STACK_RESOURCE).peak, STACK_SIZE);
    KEXPECT_EQ(owner.resources.entries.at(STACK_RESOURCE).denied, BEFORE.denied + 1);
    AdmissionSnapshot const BACKED(fixture);
    KEXPECT_TRUE(fixture.admitted(TARGET));
    BACKED.expect_unchanged(fixture);

    // Lowering the limit preserves already admitted backing. The actual
    // helper still accepts the existing page; return admission changes none.
    KREQUIRE_EQ(owner.resources.set(STACK_RESOURCE, {0, UINT64_MAX}, true, 256), 0);
    KREQUIRE_TRUE(threading::ensure_stack_backing(fixture.thread, fixture.root, TARGET, TARGET + PAGE, &owner, fixture.allocation_context));
    KEXPECT_EQ(virt::translate(fixture.root, TARGET), PHYSICAL);
    AdmissionSnapshot const LOWERED(fixture);
    KEXPECT_TRUE(fixture.admitted(TARGET));
    LOWERED.expect_unchanged(fixture);
}

namespace {
auto stack_placement_owner(ker::mod::mm::PhysicalPageOwner owner) -> phys::PhysicalOwnerSnapshot {
    phys::PhysicalBalanceSnapshot snapshot{};
    phys::get_physical_balance_snapshot(snapshot);
    return snapshot.owners[static_cast<size_t>(owner)];
}
}  // namespace

KTEST(UserStackReturn, SavedPlacementCoversTlsSafeStackGrowthAndPendingRollback) {
    namespace numa = ker::mod::mm::numa;
    auto const* topology = ker::mod::acpi::numa::boot_topology();
    KREQUIRE_NE(topology, nullptr);
    numa::NodeId chosen = 0;
    for (numa::NodeId node = 1; node < topology->node_count; ++node) {
        if (topology->nodes[node].usable_bytes >= 1024 * 1024) {
            chosen = node;
            break;
        }
    }
    auto const STACK_BEFORE = stack_placement_owner(ker::mod::mm::PhysicalPageOwner::USER_THREAD_STACK);
    auto const TLS_BEFORE = stack_placement_owner(ker::mod::mm::PhysicalPageOwner::USER_THREAD_TLS);
    {
        PrivateStack fixture;
        KREQUIRE_NE(fixture.owner.get(), nullptr);
        KREQUIRE_TRUE(fixture.owner->selftest_set_memory_policy({uint64_t{1} << chosen, numa::Mode::BIND}));
        numa::AllocationContext captured{};
        KREQUIRE_EQ(numa::capture_allocation_context(fixture.owner->memory_policy_snapshot(), captured), numa::AllocationError::NONE);
        KREQUIRE_TRUE(fixture.owner->selftest_set_memory_policy({}));
        KREQUIRE_TRUE(fixture.init(&captured));
        size_t present = 0;
        for (uint64_t address = fixture.thread->layout_base_virt; address < fixture.thread->layout_base_virt + fixture.thread->layout_size;
             address += PAGE) {
            auto const physical = virt::translate(fixture.root, address);
            if (physical == virt::PADDR_INVALID) continue;
            ++present;
            KEXPECT_EQ(numa::physical_node(physical), chosen);
            KEXPECT_EQ(phys::page_ref_get(addr::get_virt_pointer(physical)), 1U);
        }
        KEXPECT_TRUE(present >= 25);
        uint64_t const SAFESTACK_START = fixture.thread->tls_base_virt + 2 * PAGE;
        for (uint64_t address = SAFESTACK_START; address < SAFESTACK_START + 16 * PAGE; address += PAGE) {
            auto const physical = virt::translate(fixture.root, address);
            KREQUIRE_NE(physical, virt::PADDR_INVALID);
            auto const* bytes = reinterpret_cast<const uint8_t*>(addr::get_virt_pointer(physical));
            bool zero = true;
            for (size_t i = 0; i < PAGE; ++i) zero &= bytes[i] == 0;
            KEXPECT_TRUE(zero);
        }
        uint64_t const TARGET = fixture.top() - 24 * PAGE;
        KREQUIRE_TRUE(threading::ensure_stack_backing(fixture.thread, fixture.root, TARGET, TARGET + PAGE, fixture.owner.get(), captured));
        KEXPECT_EQ(numa::physical_node(virt::translate(fixture.root, TARGET)), chosen);
        uint64_t const LOW = fixture.thread->stack_base_virt + 8 * PAGE;
        KEXPECT_EQ(virt::translate(fixture.root, LOW), virt::PADDR_INVALID);
        KEXPECT_EQ(virt::translate(fixture.root, LOW + PAGE), virt::PADDR_INVALID);
        auto const PENDING_BEFORE = stack_placement_owner(ker::mod::mm::PhysicalPageOwner::USER_THREAD_STACK);
        KEXPECT_FALSE(threading::selftest_back_stack_pages(fixture.root, LOW, LOW + 2 * PAGE, captured, 1));
        auto const PENDING_AFTER = stack_placement_owner(ker::mod::mm::PhysicalPageOwner::USER_THREAD_STACK);
        KEXPECT_EQ(PENDING_AFTER.pages, PENDING_BEFORE.pages);
        KEXPECT_EQ(PENDING_AFTER.objects, PENDING_BEFORE.objects);
        KEXPECT_EQ(virt::translate(fixture.root, LOW), virt::PADDR_INVALID);
        KEXPECT_EQ(virt::translate(fixture.root, LOW + PAGE), virt::PADDR_INVALID);
        auto invalid = captured;
        invalid.local_node = numa::INVALID_NODE;
        KEXPECT_FALSE(threading::selftest_back_stack_pages(fixture.root, LOW, LOW + PAGE, invalid, -1));
        KEXPECT_EQ(virt::translate(fixture.root, LOW), virt::PADDR_INVALID);
        ker::mod::dbg::log("C16B2 private managed stack/TLS node=%u; no scheduled hardware-growth or worker-current credit", chosen);
        if (chosen == 0) ker::mod::dbg::log("C16B2 no usable node1 contrast; UMA/node0 branch only");
    }
    auto const STACK_AFTER = stack_placement_owner(ker::mod::mm::PhysicalPageOwner::USER_THREAD_STACK);
    auto const TLS_AFTER = stack_placement_owner(ker::mod::mm::PhysicalPageOwner::USER_THREAD_TLS);
    KEXPECT_EQ(STACK_AFTER.pages, STACK_BEFORE.pages);
    KEXPECT_EQ(STACK_AFTER.objects, STACK_BEFORE.objects);
    KEXPECT_EQ(TLS_AFTER.pages, TLS_BEFORE.pages);
    KEXPECT_EQ(TLS_AFTER.objects, TLS_BEFORE.objects);
}
