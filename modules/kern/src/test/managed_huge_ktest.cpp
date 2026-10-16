#ifdef WOS_SELFTEST
#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <new>
#include <platform/acpi/numa_boot.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/managed_huge.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/task.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <test/ktest.hpp>
namespace {
namespace mm = ker::mod::mm;
namespace huge = mm::huge;
namespace virt = mm::virt;
namespace phys = mm::phys;
namespace numa = mm::numa;
namespace paging = mm::paging;
namespace task = ker::mod::sched::task;
using log = ker::mod::dbg::logger<"managed-huge-test">;
constexpr uint64_t VA = 0x72000000ULL, FLAGS = paging::page_types::USER | paging::PAGE_NX;
auto counts() -> phys::PhysicalOwnerSnapshot {
    phys::PhysicalBalanceSnapshot s{};
    phys::get_physical_balance_snapshot(s);
    KEXPECT_EQ(s.identity_mismatch_pages, 0U);
    return s.owners[static_cast<size_t>(mm::PhysicalPageOwner::USER_PRIVATE_MAPPING)];
}
auto physical(const void* p) -> uint64_t { return reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(p))); }
auto memory_node() -> numa::NodeId {
    auto* t = ker::mod::acpi::numa::boot_topology();
    if (t)
        for (numa::NodeId n = 0; n < t->node_count; ++n)
            if (t->nodes[n].usable_bytes >= huge::LEAF_BYTES * 16) return n;
    return numa::INVALID_NODE;
}
struct Fixture {
    phys::PhysicalOwnerSnapshot before{counts()};
    huge::Snapshot cap_before{};
    std::unique_ptr<task::Task> subject{new (std::nothrow) task::Task{}};
    paging::PageTable* root{};
    std::array<huge::PreparedSpan, 2> spans;
    std::array<huge::PreparedTables, 2> parents, cleanup;
    std::array<bool, 2> mapped{};
    virt::SelftestManagedHugeReport report{};
    Fixture() { huge::snapshot(cap_before); }
    auto init() -> bool {
        if (!subject || !cap_before.ready) return false;
        root = virt::create_pagemap();
        if (!root) return false;
        subject->type = task::TaskType::PROCESS;
        subject->pagemap = root;
        return true;
    }
    auto prepare(size_t i) -> huge::Error {
        auto n = memory_node();
        if (i >= 2 || n == numa::INVALID_NODE) return huge::Error::NotReady;
        huge::RootKey key{};
        if (auto e = huge::detail::root_key(root, key); e != huge::Error::None) return e;
        numa::AllocationContext ctx{{uint64_t{1} << n, numa::Mode::BIND}, n, 0};
        if (auto e = spans[i].prepare(key, VA + i * huge::LEAF_BYTES, ctx); e != huge::Error::None) return e;
        if (auto e = parents[i].prepare(2); e != huge::Error::None) return e;
        return cleanup[i].prepare(1);  // All actual cleanup backing owned BEFORE PS publication.
    }
    auto publish(size_t i, uint64_t flags = FLAGS) -> huge::Error {
        auto e = virt::selftest_publish_managed_huge(*subject, VA + i * huge::LEAF_BYTES, flags, spans[i], parents[i], cleanup[i], report);
        if (e == huge::Error::None) mapped[i] = true;
        return e;
    }
    auto split(size_t i) -> huge::Error {
        auto e = virt::selftest_split_managed_huge(*subject, VA + i * huge::LEAF_BYTES, cleanup[i], report);
        if (e == huge::Error::None) mapped[i] = false;
        return e;
    }
    ~Fixture() {
        for (size_t i = 0; i < 2; ++i) {
            if (mapped[i]) {
                auto e = split(i);
                KEXPECT_EQ(e, huge::Error::None);
                if (e != huge::Error::None) __builtin_trap();
            }
            spans[i].reset();
            parents[i].reset();
            cleanup[i].reset();
        }
        if (root) {
            KEXPECT_EQ(subject->ref_count.load(std::memory_order_acquire), 1U);
            KEXPECT_EQ(subject->usercopy_pagemap_accesses.load(std::memory_order_acquire), 0U);
            KEXPECT_EQ(virt::selftest_user_pagemap_reader_count(root), 0U);
            KEXPECT_EQ(subject->detach_pagemap_after_usercopy_quiescence(), root);
            task::release_lazy_vmem_ranges(*subject);
            ker::syscall::vmem::release_file_mmap_ranges_for_pagemap(root);
            virt::destroy_user_space(root, 0, "managed-huge", "never-active-fixture");
            virt::release_pagemap(root);
        }
        huge::Snapshot after{};
        huge::snapshot(after);
        KEXPECT_EQ(after.charged_pages, cap_before.charged_pages);
        KEXPECT_EQ(after.table_pages, cap_before.table_pages);
        KEXPECT_EQ(after.plans, cap_before.plans);
        auto c = counts();
        KEXPECT_EQ(c.pages, before.pages);
        KEXPECT_EQ(c.objects, before.objects);
        log::info("actual512 private-frame/cap closure; ordinary PT/cache/shadow retained context, no global-free/SMP quiescence claim");
    }
};
struct Pin {
    virt::UserPagePin value{};
    ~Pin() { virt::unpin_user_page(value); }
};
auto references(uint8_t* d) -> uint64_t {
    uint64_t total = 0;
    for (size_t i = 0; i < huge::LEAF_PAGES; ++i) total += phys::page_ref_get(d + i * paging::PAGE_SIZE);
    return total;
}
auto pattern(uint8_t* d, bool write) -> bool {
    for (size_t i = 0; i < huge::LEAF_BYTES; ++i) {
        auto v = static_cast<uint8_t>((i * 17 + 63) & 255);
        if (write)
            d[i] = v;
        else if (d[i] != v)
            return false;
    }
    return true;
}
auto zero(uint8_t* d) -> bool {
    for (size_t i = 0; i < huge::LEAF_BYTES; ++i)
        if (d[i]) return false;
    return true;
}
// Fixture-only raw access: the Task/root is exclusively owned, never posted
// or activated. These scoped negative mutations are restored before teardown.
auto raw_slot(paging::PageTable* root, uint64_t va, int target) -> uint64_t* {
    auto* table = root;
    for (int level = 4; level > target; --level) {
        auto raw = reinterpret_cast<uint64_t*>(table)[(va >> (12 + 9 * (level - 1))) & 511];
        if (!(raw & paging::PAGE_PRESENT) || (raw & 128)) return nullptr;
        table = reinterpret_cast<paging::PageTable*>(mm::addr::get_virt_pointer(raw & 0x000ffffffffff000ULL));
    }
    return &reinterpret_cast<uint64_t*>(table)[(va >> (12 + 9 * (target - 1))) & 511];
}
struct RestoreWord {
    uint64_t& slot;
    uint64_t old;
    explicit RestoreWord(uint64_t& s) : slot(s), old(s) {}
    ~RestoreWord() { slot = old; }
};
}  // namespace
KTEST(ManagedHuge, RealZeroPublicationAndAllIndependentReferences) {
    Fixture f;
    KREQUIRE_TRUE(f.init());
    KREQUIRE_EQ(f.prepare(0), huge::Error::None);
    auto* d = static_cast<uint8_t*>(f.spans[0].data());
    KREQUIRE_NE(d, nullptr);
    KEXPECT_TRUE(zero(d));
    KEXPECT_EQ(references(d), 512U);
    KREQUIRE_EQ(f.publish(0), huge::Error::None);
    KEXPECT_TRUE(f.spans[0].empty());
    KEXPECT_TRUE(f.parents[0].empty());
    KEXPECT_EQ(references(d), 512U);
    huge::Snapshot mapped{};
    huge::snapshot(mapped);
    KEXPECT_EQ(mapped.mapped_pages, f.cap_before.mapped_pages + 512);
    KEXPECT_TRUE(f.report.actual_full_flush_called);
    log::info("real registered never-active root PS; no CPL3, locality or remote-ACK concurrency claim");
}
KTEST(ManagedHuge, FirstLastPinsFutexAndDirtySurviveActualSplit) {
    Fixture f;
    KREQUIRE_TRUE(f.init());
    KREQUIRE_EQ(f.prepare(0), huge::Error::None);
    auto* d = static_cast<uint8_t*>(f.spans[0].data());
    KREQUIRE_TRUE(pattern(d, true));
    KREQUIRE_EQ(f.publish(0), huge::Error::None);
    Pin first, last;
    KREQUIRE_TRUE(virt::pin_user_page(f.root, VA, true, first.value));
    KREQUIRE_TRUE(virt::pin_user_page(f.root, VA + huge::LEAF_BYTES - paging::PAGE_SIZE, false, last.value));
    KEXPECT_EQ(first.value.physical_page, physical(d));
    KEXPECT_EQ(last.value.physical_page, physical(d + huge::LEAF_BYTES - paging::PAGE_SIZE));
    KEXPECT_EQ(references(d), 514U);
    virt::UserFutexIdentity id{};
    KREQUIRE_TRUE(virt::capture_user_futex_identity(first.value, VA + 4, id));
    KREQUIRE_TRUE(virt::user_page_pin_commit_write(first.value));
    KREQUIRE_EQ(f.split(0), huge::Error::None);
    KEXPECT_EQ(references(d), 514U);
    KEXPECT_TRUE(virt::user_page_pin_still_mapped(first.value));
    KEXPECT_TRUE(virt::user_page_pin_still_mapped(last.value));
    KEXPECT_TRUE(virt::user_futex_identity_still_mapped(first.value, id));
    KEXPECT_TRUE(virt::user_page_pin_commit_write(first.value));
    Pin dirty;
    KREQUIRE_TRUE(virt::pin_user_page(f.root, VA, true, dirty.value));
    KEXPECT_TRUE(dirty.value.dirty);
    KEXPECT_TRUE(pattern(d, false));
}
KTEST(ManagedHuge, PermissionsSurvivePreparedDemotion) {
    for (auto flags :
         {paging::PAGE_PRESENT | paging::PAGE_NX, paging::page_types::USER_READONLY | paging::PAGE_NX, paging::page_types::USER}) {
        Fixture f;
        KREQUIRE_TRUE(f.init());
        KREQUIRE_EQ(f.prepare(0), huge::Error::None);
        KREQUIRE_EQ(f.publish(0, flags), huge::Error::None);
        auto* ps = raw_slot(f.root, VA, 2);
        KREQUIRE_NE(ps, nullptr);
        KEXPECT_EQ(*ps & paging::PAGE_NX, flags & paging::PAGE_NX);
        Pin a, b;
        bool user = (flags & paging::PAGE_USER) != 0, write = user && (flags & paging::PAGE_WRITE) != 0;
        KEXPECT_EQ(virt::pin_user_page(f.root, VA, false, a.value), user);
        KEXPECT_EQ(virt::pin_user_page(f.root, VA, true, b.value), write);
        KREQUIRE_EQ(f.split(0), huge::Error::None);
        auto* pte = raw_slot(f.root, VA, 1);
        KREQUIRE_NE(pte, nullptr);
        KEXPECT_EQ(*pte & paging::PAGE_NX, flags & paging::PAGE_NX);
        Pin c, d;
        KEXPECT_EQ(virt::pin_user_page(f.root, VA, false, c.value), user);
        KEXPECT_EQ(virt::pin_user_page(f.root, VA, true, d.value), write);
    }
}
KTEST(ManagedHuge, WholeTwoLeafPreflightFailureAndStalePlanPreserveOwners) {
    Fixture f;
    KREQUIRE_TRUE(f.init());
    for (size_t i = 0; i < 2; ++i) {
        KREQUIRE_EQ(f.prepare(i), huge::Error::None);
        KREQUIRE_EQ(f.publish(i), huge::Error::None);
    }
    huge::Snapshot before{};
    huge::snapshot(before);
    for (uint16_t prefix = 0; prefix < 2; ++prefix) {
        huge::PreparedTables plan;
        KEXPECT_EQ(virt::selftest_prepare_managed_split(*f.subject, VA, VA + 2 * huge::LEAF_BYTES, plan, {prefix}), huge::Error::Exhausted);
        KEXPECT_TRUE(plan.empty());
        huge::Snapshot after{};
        huge::snapshot(after);
        KEXPECT_EQ(after.charged_pages, before.charged_pages);
        KEXPECT_EQ(after.mapped_pages, before.mapped_pages);
        KEXPECT_EQ(after.table_pages, before.table_pages);
        KEXPECT_EQ(after.plans, before.plans);
        Pin a, b;
        KEXPECT_TRUE(virt::pin_user_page(f.root, VA, true, a.value));
        KEXPECT_TRUE(virt::pin_user_page(f.root, VA + huge::LEAF_BYTES, true, b.value));
    }
    huge::PreparedTables plan;
    KREQUIRE_EQ(virt::selftest_prepare_managed_split(*f.subject, VA, VA + 2 * huge::LEAF_BYTES, plan), huge::Error::None);
    auto* s = huge::detail::PreparationAccess::plan_storage(plan);
    KREQUIRE_NE(s, nullptr);
    ++s->root.incarnation;
    KEXPECT_EQ(virt::selftest_commit_managed_split(*f.subject, plan, f.report), huge::Error::Again);
    --s->root.incarnation;
    s->descriptors[1].semantic_word ^= 2;
    KEXPECT_EQ(virt::selftest_commit_managed_split(*f.subject, plan, f.report), huge::Error::Again);
    s->descriptors[1].semantic_word ^= 2;
    auto saved_va = s->descriptors[1].key.va_2m;
    s->descriptors[1].key.va_2m = VA + 2 * huge::LEAF_BYTES;
    KEXPECT_EQ(virt::selftest_commit_managed_split(*f.subject, plan, f.report), huge::Error::Again);
    s->descriptors[1].key.va_2m = saved_va;
    KREQUIRE_EQ(virt::selftest_commit_managed_split(*f.subject, plan, f.report), huge::Error::None);
    f.mapped = {};
    KEXPECT_EQ(virt::selftest_user_pagemap_reader_count(f.root), 0U);
    log::info("per-call fail-before-table exercises actual prefix rollback; no PMM-pressure claim");
}
KTEST(ManagedHuge, CompleteSparseZeroProofNeedsNoPlanAllocation) {
    Fixture f;
    KREQUIRE_TRUE(f.init());
    huge::PreparedTables plan;
    huge::Snapshot before{};
    huge::snapshot(before);
    KEXPECT_EQ(virt::selftest_prepare_managed_split(*f.subject, VA, VA + 65 * huge::LEAF_BYTES, plan), huge::Error::None);
    huge::Snapshot after{};
    huge::snapshot(after);
    KEXPECT_TRUE(plan.empty());
    KEXPECT_EQ(before.plans, after.plans);
    KEXPECT_EQ(before.table_pages, after.table_pages);
}
KTEST(ManagedHuge, CrossTwoMegabytePinsAndFutexSurviveWholeSplit) {
    Fixture f;
    KREQUIRE_TRUE(f.init());
    for (size_t i = 0; i < 2; ++i) {
        KREQUIRE_EQ(f.prepare(i), huge::Error::None);
        KREQUIRE_EQ(f.publish(i), huge::Error::None);
    }
    Pin left, right;
    KREQUIRE_TRUE(virt::pin_user_page(f.root, VA + huge::LEAF_BYTES - paging::PAGE_SIZE, true, left.value));
    KREQUIRE_TRUE(virt::pin_user_page(f.root, VA + huge::LEAF_BYTES, true, right.value));
    virt::UserFutexIdentity l{}, r{};
    KREQUIRE_TRUE(virt::capture_user_futex_identity(left.value, VA + huge::LEAF_BYTES - 4, l));
    KREQUIRE_TRUE(virt::capture_user_futex_identity(right.value, VA + huge::LEAF_BYTES, r));
    huge::PreparedTables plan;
    KREQUIRE_EQ(virt::selftest_prepare_managed_split(*f.subject, VA, VA + 2 * huge::LEAF_BYTES, plan), huge::Error::None);
    KREQUIRE_EQ(virt::selftest_commit_managed_split(*f.subject, plan, f.report), huge::Error::None);
    f.mapped = {};
    KEXPECT_TRUE(virt::user_page_pin_still_mapped(left.value));
    KEXPECT_TRUE(virt::user_page_pin_still_mapped(right.value));
    KEXPECT_TRUE(virt::user_futex_identity_still_mapped(left.value, l));
    KEXPECT_TRUE(virt::user_futex_identity_still_mapped(right.value, r));
    KEXPECT_TRUE(virt::user_page_pin_commit_write(left.value));
    KEXPECT_TRUE(virt::user_page_pin_commit_write(right.value));
}
KTEST(ManagedHuge, UnknownTwoMegabyteAndOneGigabyteRefuseWithoutTakingRefs) {
    Fixture f;
    KREQUIRE_TRUE(f.init());
    KREQUIRE_EQ(f.prepare(0), huge::Error::None);
    auto* d = static_cast<uint8_t*>(f.spans[0].data());
    KREQUIRE_EQ(f.publish(0), huge::Error::None);
    auto before = references(d);
    auto* leaf = raw_slot(f.root, VA, 2);
    KREQUIRE_NE(leaf, nullptr);
    {
        RestoreWord restore(*leaf);
        *leaf ^= huge::LEAF_BYTES;
        Pin pin;
        KEXPECT_FALSE(virt::pin_user_page(f.root, VA, false, pin.value));
        KEXPECT_EQ(references(d), before);
    }
    auto* upper = raw_slot(f.root, VA, 3);
    KREQUIRE_NE(upper, nullptr);
    {
        RestoreWord restore(*upper);
        *upper = 0x40000087ULL;
        Pin pin;
        KEXPECT_FALSE(virt::pin_user_page(f.root, VA, true, pin.value));
        KEXPECT_EQ(references(d), before);
    }
    Pin valid;
    KEXPECT_TRUE(virt::pin_user_page(f.root, VA, true, valid.value));
}

namespace {
// Separate physical references retain patterned bytes after their actual PS
// mapping reference is retired. They carry no root-reader lease or fake Task.
struct RetainedManagedFrames {
    uint8_t* data{};
    explicit RetainedManagedFrames(uint8_t* p) : data(p) {
        for (size_t i = 0; i < huge::LEAF_PAGES; ++i) phys::page_ref_inc(data + i * paging::PAGE_SIZE);
    }
    ~RetainedManagedFrames() {
        for (size_t i = 0; i < huge::LEAF_PAGES; ++i) (void)phys::page_ref_dec(data + i * paging::PAGE_SIZE);
    }
};
struct BudgetRetirementOwner {
    virt::DestroyUserSpaceBudgetState* state{};
    Fixture* fixture{};
    ~BudgetRetirementOwner() {
        virt::destroy_user_space_budget_state_destroy(state);
        // Positive fixture aborts retain the original root. Once its spare was
        // intentionally returned, real raw retirement is its allocation-free
        // fallback; never ask the old split cleanup to consume a missing spare.
        if (fixture && fixture->root && fixture->mapped[0] && fixture->cleanup[0].empty()) {
            virt::destroy_user_space(fixture->root, 0, "managed-huge", "budget-fixture-abort");
            fixture->mapped[0] = false;
        }
    }
};
void expect_retained_pattern(uint8_t* data) {
    KEXPECT_EQ(references(data), huge::LEAF_PAGES);
    KEXPECT_TRUE(pattern(data, false));
}
}  // namespace

KTEST(ManagedHuge, RawRetirementReturnsMappingRefsWithoutCleanupTable) {
    Fixture f;
    KREQUIRE_TRUE(f.init());
    KREQUIRE_EQ(f.prepare(0), huge::Error::None);
    auto* data = static_cast<uint8_t*>(f.spans[0].data());
    KREQUIRE_TRUE(pattern(data, true));
    KREQUIRE_EQ(f.publish(0), huge::Error::None);
    RetainedManagedFrames retained(data);
    f.cleanup[0].reset();  // Retirement cannot use the prepublication spare.
    KEXPECT_EQ(references(data), 2 * huge::LEAF_PAGES);
    virt::destroy_user_space(f.root, 0, "managed-huge", "raw-PS-retirement");
    f.mapped[0] = false;  // The actual raw walker, not a split, detached it.
    expect_retained_pattern(data);
    huge::Snapshot after{};
    huge::snapshot(after);
    KEXPECT_EQ(after.charged_pages, f.cap_before.charged_pages);
    KEXPECT_EQ(after.retired_pages, f.cap_before.retired_pages);
    log::info("real raw managed512 refs retired; retained physical refs preserve all patterned bytes; no SMP ACK claim");
}

KTEST(ManagedHuge, BudgetRetirementCompletesAndReturnsActualRoot) {
    Fixture f;
    KREQUIRE_TRUE(f.init());
    KREQUIRE_EQ(f.prepare(0), huge::Error::None);
    auto* data = static_cast<uint8_t*>(f.spans[0].data());
    KREQUIRE_TRUE(pattern(data, true));
    KREQUIRE_EQ(f.publish(0), huge::Error::None);
    RetainedManagedFrames retained(data);
    BudgetRetirementOwner owner{virt::create_destroy_user_space_budget_state(f.root, 0, "managed-huge", "budget-PS-retirement"), &f};
    KREQUIRE_NE(owner.state, nullptr);
    f.cleanup[0].reset();
    bool done = false;
    // Dedicated large fixture; the existing native small file-only2048 bound
    // and its assertions remain unchanged in their original source path.
    for (size_t step = 0; step < 4096 && !done; ++step) done = virt::destroy_user_space_budgeted(owner.state, 1);
    KREQUIRE_TRUE(done);
    f.mapped[0] = false;
    KREQUIRE_EQ(f.subject->detach_pagemap_after_usercopy_quiescence(), f.root);
    task::release_lazy_vmem_ranges(*f.subject);
    ker::syscall::vmem::release_file_mmap_ranges_for_pagemap(f.root);
    KREQUIRE_TRUE(virt::finish_destroy_user_space_and_release_pagemap(owner.state));
    owner.state = nullptr;
    f.root = nullptr;
    expect_retained_pattern(data);
    huge::Snapshot after{};
    huge::snapshot(after);
    KEXPECT_EQ(after.charged_pages, f.cap_before.charged_pages);
    log::info("actual budget managed512 retirement/DONE/root return; physical refs and pattern survive; no scheduled concurrency claim");
}

KTEST(ManagedHuge, PartialBudgetDisposalDrainsDetachedTailOnly) {
    for (uint16_t prefix : {uint16_t{0}, uint16_t{63}, uint16_t{64}, uint16_t{65}, uint16_t{511}}) {
        Fixture f;
        KREQUIRE_TRUE(f.init());
        KREQUIRE_EQ(f.prepare(0), huge::Error::None);
        auto* data = static_cast<uint8_t*>(f.spans[0].data());
        KREQUIRE_TRUE(pattern(data, true));
        KREQUIRE_EQ(f.publish(0), huge::Error::None);
        RetainedManagedFrames retained(data);
        BudgetRetirementOwner owner{virt::create_destroy_user_space_budget_state(f.root, 0, "managed-huge", "dispose-PS-tail"), &f};
        KREQUIRE_NE(owner.state, nullptr);
        f.cleanup[0].reset();
        bool detached = false;
        for (size_t step = 0; step < 1024 && !detached; ++step) {
            KEXPECT_FALSE(virt::destroy_user_space_budgeted(owner.state, 1));
            huge::Snapshot current{};
            huge::snapshot(current);
            detached = current.retired_pages == f.cap_before.retired_pages + huge::LEAF_PAGES;
        }
        KREQUIRE_TRUE(detached);
        f.mapped[0] = false;
        for (uint16_t page = 0; page < prefix; ++page) KEXPECT_FALSE(virt::destroy_user_space_budgeted(owner.state, 1));
        KEXPECT_EQ(references(data), 2 * huge::LEAF_PAGES - prefix);
        virt::destroy_user_space_budget_state_destroy(owner.state);
        owner.state = nullptr;
        expect_retained_pattern(data);
        KEXPECT_EQ(virt::selftest_user_pagemap_reader_count(f.root), 0U);
        huge::Snapshot after{};
        huge::snapshot(after);
        KEXPECT_EQ(after.charged_pages, f.cap_before.charged_pages);
        // Existing Fixture cleanup now retires the still-reachable PT tree.
    }
}

KTEST(ManagedHuge, PartialDisposalPreservesReachableManagedRemainder) {
    Fixture f;
    KREQUIRE_TRUE(f.init());
    KREQUIRE_EQ(f.prepare(0), huge::Error::None);
    KREQUIRE_EQ(f.prepare(1), huge::Error::None);
    auto* first = static_cast<uint8_t*>(f.spans[0].data());
    auto* second = static_cast<uint8_t*>(f.spans[1].data());
    KREQUIRE_TRUE(pattern(first, true));
    KREQUIRE_TRUE(pattern(second, true));
    KREQUIRE_EQ(f.publish(0), huge::Error::None);
    KREQUIRE_EQ(f.publish(1), huge::Error::None);
    RetainedManagedFrames retained(first);
    BudgetRetirementOwner owner{virt::create_destroy_user_space_budget_state(f.root, 0, "managed-huge", "managed-remainder"), &f};
    KREQUIRE_NE(owner.state, nullptr);
    f.cleanup[0].reset();
    bool detached = false;
    for (size_t step = 0; step < 1024 && !detached; ++step) {
        KEXPECT_FALSE(virt::destroy_user_space_budgeted(owner.state, 1));
        huge::Snapshot current{};
        huge::snapshot(current);
        detached = current.retired_pages == f.cap_before.retired_pages + huge::LEAF_PAGES;
    }
    KREQUIRE_TRUE(detached);
    f.mapped[0] = false;
    virt::destroy_user_space_budget_state_destroy(owner.state);
    owner.state = nullptr;
    expect_retained_pattern(first);
    KEXPECT_EQ(references(second), huge::LEAF_PAGES);
    KEXPECT_TRUE(pattern(second, false));
    auto* word = raw_slot(f.root, VA + huge::LEAF_BYTES, 2);
    KREQUIRE_NE(word, nullptr);
    KEXPECT_NE(*word & (uint64_t{1} << 7), 0U);
    huge::Snapshot after{};
    huge::snapshot(after);
    KEXPECT_EQ(after.retired_pages, f.cap_before.retired_pages);
    KEXPECT_EQ(after.mapped_pages, f.cap_before.mapped_pages + huge::LEAF_PAGES);
    KEXPECT_EQ(after.charged_pages, f.cap_before.charged_pages + huge::LEAF_PAGES);
    // The unchanged Fixture still owns second's actual preowned split cleanup.
}
#endif

#ifdef WOS_SELFTEST
KTEST(ManagedHuge, CompleteZeroProofAndExactOriginalExclusive) {
    Fixture f;
    KREQUIRE_TRUE(f.init());
    huge::RangeSplitSurvey survey;
    huge::PreparedTables plan;
    KREQUIRE_EQ(virt::selftest_prepare_complete_managed_split(*f.subject, 0, uint64_t{1} << 47, survey, plan), huge::Error::None);
    KEXPECT_TRUE(survey.complete());
    KEXPECT_EQ(survey.count(), 0U);
    KEXPECT_TRUE(plan.empty());
    KREQUIRE_EQ(virt::selftest_commit_complete_managed_split_exclusive(*f.subject, survey, plan, f.report), huge::Error::None);
    KEXPECT_EQ(virt::selftest_user_pagemap_reader_count(f.root), 0U);
    KEXPECT_EQ(f.report.split, 0U);
    KEXPECT_FALSE(f.report.actual_full_flush_called);
}
KTEST(ManagedHuge, CompleteZeroProofDetectsNewPsAndCleanupCannotArmRange) {
    Fixture f;
    KREQUIRE_TRUE(f.init());
    huge::RangeSplitSurvey survey;
    huge::PreparedTables empty;
    KREQUIRE_EQ(virt::selftest_prepare_complete_managed_split(*f.subject, 0, uint64_t{1} << 47, survey, empty), huge::Error::None);
    KREQUIRE_EQ(f.prepare(0), huge::Error::None);
    KREQUIRE_EQ(f.publish(0), huge::Error::None);
    KEXPECT_EQ(virt::selftest_commit_complete_managed_split(*f.subject, survey, empty, f.report), huge::Error::Again);
    KEXPECT_EQ(virt::selftest_commit_complete_managed_split(*f.subject, survey, f.cleanup[0], f.report), huge::Error::Invalid);
}
KTEST(ManagedHuge, EarlierSingleLeafCleanupSurvivesLaterPublicationAndSaturation) {
    Fixture f;
    KREQUIRE_TRUE(f.init());
    for (size_t i = 0; i < 2; ++i) {
        KREQUIRE_EQ(f.prepare(i), huge::Error::None);
        KREQUIRE_EQ(f.publish(i), huge::Error::None);
    }
    KREQUIRE_TRUE(virt::selftest_saturate_managed_geometry_epoch(*f.subject));
    huge::RangeSplitSurvey survey;
    huge::PreparedTables empty;
    KEXPECT_EQ(virt::selftest_prepare_complete_managed_split(*f.subject, VA, VA + 2 * huge::LEAF_BYTES, survey, empty),
               huge::Error::GenerationExhausted);
    KEXPECT_FALSE(survey.complete());
    KEXPECT_TRUE(empty.empty());
    for (size_t i = 0; i < 2; ++i) {
        KREQUIRE_EQ(virt::selftest_split_managed_huge(*f.subject, VA + i * huge::LEAF_BYTES, f.cleanup[i], f.report), huge::Error::None);
        f.mapped[i] = false;
    }
    huge::GeometryStamp stamp;
    KREQUIRE_TRUE(virt::selftest_managed_geometry_stamp(*f.subject, stamp));
    KEXPECT_EQ(stamp.epoch, UINT64_MAX);
}
KTEST(ManagedHuge, SparsePartialBoundaryProofAndHeldExclusiveSplit) {
    Fixture f;
    KREQUIRE_TRUE(f.init());
    KREQUIRE_EQ(f.prepare(0), huge::Error::None);
    KREQUIRE_EQ(f.publish(0), huge::Error::None);
    huge::RangeSplitSurvey survey;
    huge::PreparedTables plan;
    KREQUIRE_EQ(virt::selftest_prepare_complete_managed_split(*f.subject, VA + 4096, VA + 65 * huge::LEAF_BYTES + 4096, survey, plan),
                huge::Error::None);
    KEXPECT_EQ(survey.count(), 1U);
    KEXPECT_EQ(plan.count(), 1U);
    KREQUIRE_EQ(virt::selftest_commit_complete_managed_split_exclusive(*f.subject, survey, plan, f.report), huge::Error::None);
    f.mapped[0] = false;
    KEXPECT_EQ(virt::selftest_user_pagemap_reader_count(f.root), 0U);
}
#endif
