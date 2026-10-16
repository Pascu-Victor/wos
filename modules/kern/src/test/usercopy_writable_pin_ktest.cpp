#ifdef WOS_SELFTEST
#include <test/numa_lifecycle_mm_debt.hpp>
#endif
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <platform/asm/cpu.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/usercopy.hpp>
#include <test/ktest.hpp>

namespace {
namespace addr = ker::mod::mm::addr;
namespace cpu = ker::mod::cpu;
namespace mm = ker::mod::mm;
namespace paging = ker::mod::mm::paging;
namespace phys = ker::mod::mm::phys;
namespace task = ker::mod::sched::task;
namespace usercopy = ker::mod::sys::usercopy;
namespace virt = ker::mod::mm::virt;

constexpr uint64_t TEST_VA = 0x49000000ULL;
constexpr uint64_t SECOND_VA = TEST_VA + paging::PAGE_SIZE;
constexpr uint64_t DIRTY_BIT = 1ULL << 6U;
constexpr uint64_t PS_BIT = 1ULL << 7U;
constexpr uint64_t IF_AC = cpu::GATE_IF_MASK | (1ULL << 18U);

auto flags_now() -> uint64_t {
    uint64_t flags = 0;
    asm volatile("pushfq; pop %0" : "=r"(flags) : : "memory");
    return flags & IF_AC;
}

auto physical(const void* page) -> uint64_t { return reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<uint64_t>(page))); }

// All trees are actual allocator-backed private roots. No Task is registered,
// executed or given a borrowed Thread/context/permanent pagemap.
struct PrivateTask {
    std::unique_ptr<task::Task> owner{new (std::nothrow) task::Task{}};
    paging::PageTable* root{};

    ~PrivateTask() {
        if (owner != nullptr) {
            task::release_lazy_vmem_ranges(*owner);
            if (root != nullptr) {
                KEXPECT_EQ(owner->usercopy_pagemap_accesses.load(std::memory_order_acquire), 0U);
                KEXPECT_EQ(owner->detach_pagemap_after_usercopy_quiescence(), root);
            }
        }
        if (root != nullptr) {
            virt::destroy_user_space(root, 0, "writable-pin-ktest", "private-fixture");
            virt::release_pagemap(root);
        }
    }

    auto init() -> bool {
        if (owner == nullptr) return false;
        root = virt::create_pagemap();
        if (root == nullptr) return false;
        owner->type = task::TaskType::PROCESS;
        owner->pagemap = root;
        return true;
    }

    auto map(uint64_t va, uint64_t flags = paging::page_types::USER, uint8_t fill = 0x39) -> uint8_t* {
        auto* page = static_cast<uint8_t*>(phys::page_alloc(mm::PhysicalPageOwner::SELFTEST, paging::PAGE_SIZE, "writable-pin-ktest"));
        if (page == nullptr) return nullptr;
        std::memset(page, fill, paging::PAGE_SIZE);
        // map_page consumes the allocation's one reference as PTE ownership.
        virt::map_page(root, va, physical(page), flags);
        return page;
    }
};

// First Task access is held even on lower-wrapper refusal, then released after
// the raw pin has returned its frame and striped-root hold.
struct ObservedPin {
    ObservedPin() = default;
    ObservedPin(const ObservedPin&) = delete;
    ObservedPin(ObservedPin&&) = delete;
    auto operator=(const ObservedPin&) -> ObservedPin& = delete;
    auto operator=(ObservedPin&&) -> ObservedPin& = delete;

    task::Task* subject{};
    paging::PageTable* captured{};
    virt::UserPagePin pin{};
    virt::UserPagePinPathObservation observation{};

    ~ObservedPin() { reset(); }
    void reset() {
        virt::unpin_user_page(pin);
        if (subject != nullptr) subject->release_usercopy_pagemap();
        subject = nullptr;
        captured = nullptr;
    }
    auto acquire(task::Task& candidate, uint64_t va, bool writable = true, bool fault_in = true) -> bool {
        reset();
        observation = {};
        if (!usercopy::range_valid(va, 1) || !candidate.try_acquire_usercopy_pagemap(captured)) return false;
        subject = &candidate;
        if (!virt::selftest_pin_user_page_for_task_observed(subject, captured, va, writable, fault_in, pin, observation)) {
            reset();
            return false;
        }
        return true;
    }
};

struct RawPin {
    RawPin() = default;
    RawPin(const RawPin&) = delete;
    RawPin(RawPin&&) = delete;
    auto operator=(const RawPin&) -> RawPin& = delete;
    auto operator=(RawPin&&) -> RawPin& = delete;

    virt::UserPagePin pin{};
    ~RawPin() { virt::unpin_user_page(pin); }
};

auto entry_raw(paging::PageTable* root, uint64_t va, unsigned wanted_level) -> uint64_t {
    auto* table = root;
    for (unsigned level = 4; level > wanted_level; --level) {
        auto const& entry = table->entries.at((va >> (12U + (level - 1U) * 9U)) & 0x1FFU);
        if (entry.present == 0 || (level < 4 && entry.pagesize != 0)) return 0;
        table = reinterpret_cast<paging::PageTable*>(addr::get_virt_pointer(entry.frame << paging::PAGE_SHIFT));
    }
    uint64_t raw = 0;
    auto const& entry = table->entries.at((va >> (12U + (wanted_level - 1U) * 9U)) & 0x1FFU);
    std::memcpy(&raw, &entry, sizeof(raw));
    return raw;
}

// Permission injection never changes a frame, references, A/D or ownership.
// Restores before root destruction on success and on every KREQUIRE return.
struct EntryChange {
    EntryChange(const EntryChange&) = delete;
    EntryChange(EntryChange&&) = delete;
    auto operator=(const EntryChange&) -> EntryChange& = delete;
    auto operator=(EntryChange&&) -> EntryChange& = delete;

    PrivateTask& fixture;
    uint64_t va;
    unsigned level;
    uint64_t original;
    bool changed{};

    EntryChange(PrivateTask& f, uint64_t address, unsigned at, uint64_t clear, uint64_t set)
        : fixture(f), va(address), level(at), original(entry_raw(f.root, address, at)) {
        changed = virt::selftest_exchange_unpublished_user_entry(*f.owner, f.root, va, level, original, (original & ~clear) | set);
    }
    ~EntryChange() {
        if (changed) {
            uint64_t const CURRENT = entry_raw(fixture.root, va, level);
            if (CURRENT != original) {
                KEXPECT_TRUE(virt::selftest_exchange_unpublished_user_entry(*fixture.owner, fixture.root, va, level, CURRENT, original));
            }
        }
    }
};

void expect_route(const ObservedPin& pin, uint32_t attempted, uint32_t succeeded, uint32_t resolved) {
    KEXPECT_EQ(pin.observation.early_attempts, attempted);
    KEXPECT_EQ(pin.observation.early_successes, succeeded);
    KEXPECT_EQ(pin.observation.writable_resolution_calls, resolved);
}

void expect_empty(const virt::UserPagePin& pin) {
    KEXPECT_NULL(pin.pagemap);
    KEXPECT_NULL(pin.hhdm_page);
    KEXPECT_EQ(pin.physical_page, virt::PADDR_INVALID);
    KEXPECT_EQ(pin.access_gate_index, static_cast<size_t>(-1));
}

void expect_drained(const PrivateTask& fixture, uint32_t readers) {
    KEXPECT_EQ(fixture.owner->usercopy_pagemap_accesses.load(std::memory_order_acquire), 0U);
    KEXPECT_EQ(virt::selftest_user_pagemap_reader_count(fixture.root), readers);
    KEXPECT_EQ(fixture.owner->ref_count.load(std::memory_order_acquire), 1U);
}

void write_byte(const virt::UserPagePin& pin, uint8_t byte) {
    usercopy::UserAccessGuard const ACCESS{};
    static_cast<uint8_t*>(pin.hhdm_page)[0] = byte;
}
}  // namespace

KTEST(UsercopyWritablePin, ExistingWritablePinUsesExactHolds) {
    PrivateTask fixture;
    KREQUIRE_TRUE(fixture.init());
    auto* page = fixture.map(TEST_VA);
    KREQUIRE_NE(page, nullptr);
    uint32_t const READERS = virt::selftest_user_pagemap_reader_count(fixture.root);
    uint64_t const FLAGS = flags_now();
    for (uint8_t iteration = 0; iteration < 3; ++iteration) {
        uint64_t const BEFORE = entry_raw(fixture.root, TEST_VA, 1);
        ObservedPin held;
        KREQUIRE_TRUE(held.acquire(*fixture.owner, TEST_VA));
        expect_route(held, 1, 1, 0);
        KEXPECT_EQ(held.pin.physical_page, physical(page));
        KEXPECT_EQ(held.pin.hhdm_page, page);
        KEXPECT_EQ(held.pin.dirty, (BEFORE & DIRTY_BIT) != 0);
        KEXPECT_EQ(phys::page_ref_get(page), 2U);
        KEXPECT_EQ(fixture.owner->usercopy_pagemap_accesses.load(std::memory_order_acquire), 1U);
        KEXPECT_EQ(virt::selftest_user_pagemap_reader_count(fixture.root), READERS + 1U);
        KEXPECT_EQ(entry_raw(fixture.root, TEST_VA, 1), BEFORE);
        write_byte(held.pin, static_cast<uint8_t>(0xA0 + iteration));
        KEXPECT_EQ(page[0], static_cast<uint8_t>(0xA0 + iteration));
        KEXPECT_EQ(flags_now(), FLAGS);
        KEXPECT_EQ(entry_raw(fixture.root, TEST_VA, 1), BEFORE);
        KEXPECT_TRUE(virt::user_page_pin_still_mapped(held.pin));
        KEXPECT_TRUE(virt::user_page_pin_commit_write(held.pin));
        KEXPECT_EQ(entry_raw(fixture.root, TEST_VA, 1), BEFORE | DIRTY_BIT);
        held.reset();
        held.reset();
        expect_empty(held.pin);
        KEXPECT_EQ(phys::page_ref_get(page), 1U);
        expect_drained(fixture, READERS);
    }
    uint8_t const BYTE = 0xE7;
    uint8_t copied = 0;
    KEXPECT_TRUE(usercopy::copy_to_task(*fixture.owner, TEST_VA, &BYTE, 1));
    KEXPECT_TRUE(usercopy::copy_from_task(*fixture.owner, TEST_VA, &copied, 1));
    KEXPECT_EQ(copied, BYTE);
    KEXPECT_EQ(page[0], BYTE);
    KEXPECT_EQ(phys::page_ref_get(page), 1U);
    KEXPECT_EQ(flags_now(), FLAGS);
    expect_drained(fixture, READERS);
}

KTEST(UsercopyWritablePin, CompletePathAndFinalLeafRestrictions) {
    PrivateTask fixture;
    KREQUIRE_TRUE(fixture.init());
    auto* page = fixture.map(TEST_VA);
    KREQUIRE_NE(page, nullptr);
    uint32_t const READERS = virt::selftest_user_pagemap_reader_count(fixture.root);
    uint64_t const FLAGS = flags_now();
    for (unsigned level = 1; level <= 4; ++level) {
        for (uint64_t const BIT : std::array<uint64_t, 3>{paging::PAGE_PRESENT, paging::PAGE_USER, paging::PAGE_WRITE}) {
            EntryChange change(fixture, TEST_VA, level, BIT, 0);
            KREQUIRE_TRUE(change.changed);
            ObservedPin held;
            KEXPECT_FALSE(held.acquire(*fixture.owner, TEST_VA, true, false));
            expect_route(held, 0, 0, 0);
            expect_empty(held.pin);
            bool const READABLE = BIT == paging::PAGE_WRITE;
            KEXPECT_EQ(held.acquire(*fixture.owner, TEST_VA, false, false), READABLE);
            expect_route(held, 0, 0, 0);
            held.reset();
            if (level == 1 && BIT != paging::PAGE_PRESENT) {
                KEXPECT_FALSE(held.acquire(*fixture.owner, TEST_VA, true, true));
                expect_route(held, 1, 0, 1);
                expect_empty(held.pin);
            }
            KEXPECT_EQ(phys::page_ref_get(page), 1U);
            expect_drained(fixture, READERS);
        }
        {
            EntryChange change(fixture, TEST_VA, level, 0, paging::PAGE_COW);
            KREQUIRE_TRUE(change.changed);
            ObservedPin held;
            KEXPECT_FALSE(held.acquire(*fixture.owner, TEST_VA, true, false));
            expect_empty(held.pin);
            KREQUIRE_TRUE(held.acquire(*fixture.owner, TEST_VA, false, false));
            held.reset();
            KEXPECT_EQ(phys::page_ref_get(page), 1U);
            expect_drained(fixture, READERS);
        }
    }
    for (unsigned level : std::array<unsigned, 2>{2, 3}) {
        EntryChange change(fixture, TEST_VA, level, 0, PS_BIT);
        KREQUIRE_TRUE(change.changed);
        ObservedPin held;
        KEXPECT_FALSE(held.acquire(*fixture.owner, TEST_VA, true, false));
        KEXPECT_FALSE(held.acquire(*fixture.owner, TEST_VA, false, false));
        expect_empty(held.pin);
        KEXPECT_EQ(phys::page_ref_get(page), 1U);
        expect_drained(fixture, READERS);
    }
    {
        EntryChange change(fixture, TEST_VA, 1, 0, paging::PAGE_SHARED | paging::PAGE_WRITE_RESTRICTED);
        KREQUIRE_TRUE(change.changed);
        ObservedPin held;
        KEXPECT_FALSE(held.acquire(*fixture.owner, TEST_VA));
        expect_route(held, 1, 0, 1);
        KREQUIRE_TRUE(held.acquire(*fixture.owner, TEST_VA, false, true));
        expect_route(held, 0, 0, 0);
        held.reset();
    }
    uint64_t const RAW = entry_raw(fixture.root, TEST_VA, 1);
    KEXPECT_FALSE(virt::selftest_exchange_unpublished_user_entry(*fixture.owner, fixture.root, TEST_VA, 1, RAW ^ DIRTY_BIT, RAW));
    KEXPECT_FALSE(virt::selftest_exchange_unpublished_user_entry(*fixture.owner, fixture.root, TEST_VA, 1, RAW, RAW ^ DIRTY_BIT));
    KEXPECT_FALSE(virt::selftest_exchange_unpublished_user_entry(*fixture.owner, virt::get_kernel_pagemap(), TEST_VA, 1, RAW, RAW));
    KEXPECT_EQ(entry_raw(fixture.root, TEST_VA, 1), RAW);
    KEXPECT_EQ(flags_now(), FLAGS);
    expect_drained(fixture, READERS);
}

KTEST(UsercopyWritablePin, RestrictedLeafKeepsWritableSibling) {
    PrivateTask fixture;
    KREQUIRE_TRUE(fixture.init());
    auto* restricted = fixture.map(TEST_VA, paging::page_types::USER | paging::PAGE_SHARED | paging::PAGE_WRITE_RESTRICTED, 0x41);
    auto* sibling = fixture.map(SECOND_VA, paging::page_types::USER, 0x52);
    KREQUIRE_NE(restricted, nullptr);
    KREQUIRE_NE(sibling, nullptr);
    for (unsigned level = 2; level <= 4; ++level) {
        uint64_t const RAW = entry_raw(fixture.root, SECOND_VA, level);
        KEXPECT_TRUE(paging::shared_write_restricted(RAW));
        KEXPECT_NE(RAW & paging::PAGE_WRITE, 0U);
    }
    uint32_t const READERS = virt::selftest_user_pagemap_reader_count(fixture.root);
    ObservedPin held;
    KREQUIRE_TRUE(held.acquire(*fixture.owner, TEST_VA, false, false));
    KEXPECT_EQ(held.pin.hhdm_page, restricted);
    held.reset();
    KEXPECT_FALSE(held.acquire(*fixture.owner, TEST_VA));
    expect_route(held, 1, 0, 1);
    KEXPECT_EQ(restricted[0], 0x41);
    KREQUIRE_TRUE(held.acquire(*fixture.owner, SECOND_VA));
    expect_route(held, 1, 1, 0);
    KEXPECT_EQ(held.pin.physical_page, physical(sibling));
    write_byte(held.pin, 0xB6);
    KEXPECT_TRUE(virt::user_page_pin_commit_write(held.pin));
    held.reset();
    KEXPECT_EQ(sibling[0], 0xB6);
    KEXPECT_EQ(restricted[0], 0x41);
    KEXPECT_EQ(phys::page_ref_get(restricted), 1U);
    KEXPECT_EQ(phys::page_ref_get(sibling), 1U);
    expect_drained(fixture, READERS);
}

KTEST(UsercopyWritablePin, FinalCommitRejectsReplacementAndRestriction) {
    PrivateTask fixture;
    KREQUIRE_TRUE(fixture.init());
    auto* original = fixture.map(TEST_VA);
    KREQUIRE_NE(original, nullptr);
    uint32_t const READERS = virt::selftest_user_pagemap_reader_count(fixture.root);
    ObservedPin held;
    KREQUIRE_TRUE(held.acquire(*fixture.owner, TEST_VA));
    auto* replacement = fixture.map(TEST_VA, paging::page_types::USER, 0x67);
    KREQUIRE_NE(replacement, nullptr);
    KEXPECT_EQ(phys::page_ref_get(original), 1U);
    KEXPECT_FALSE(virt::user_page_pin_still_mapped(held.pin));
    write_byte(held.pin, 0xD1);
    KEXPECT_FALSE(virt::user_page_pin_commit_write(held.pin));
    KEXPECT_EQ(entry_raw(fixture.root, TEST_VA, 1) & DIRTY_BIT, 0U);
    KEXPECT_EQ(replacement[0], 0x67);
    held.reset();
    KEXPECT_EQ(phys::page_ref_get(original), 0U);
    KREQUIRE_TRUE(held.acquire(*fixture.owner, TEST_VA));
    virt::unify_page_flags(fixture.root, TEST_VA, paging::page_types::USER_READONLY);
    KEXPECT_FALSE(virt::user_page_pin_still_mapped(held.pin));
    KEXPECT_FALSE(virt::user_page_pin_commit_write(held.pin));
    KEXPECT_EQ(entry_raw(fixture.root, TEST_VA, 1) & DIRTY_BIT, 0U);
    held.reset();
    virt::unify_page_flags(fixture.root, TEST_VA, paging::page_types::USER);
    KREQUIRE_TRUE(held.acquire(*fixture.owner, TEST_VA));
    {
        EntryChange change(fixture, TEST_VA, 1, 0, paging::PAGE_SHARED | paging::PAGE_WRITE_RESTRICTED);
        KREQUIRE_TRUE(change.changed);
        KEXPECT_FALSE(virt::user_page_pin_still_mapped(held.pin));
        KEXPECT_FALSE(virt::user_page_pin_commit_write(held.pin));
        KEXPECT_EQ(entry_raw(fixture.root, TEST_VA, 1) & DIRTY_BIT, 0U);
        ObservedPin denied;
        KEXPECT_FALSE(denied.acquire(*fixture.owner, TEST_VA));
        expect_route(denied, 1, 0, 1);
        KEXPECT_FALSE(denied.acquire(*fixture.owner, TEST_VA, true, false));
        RawPin raw;
        KEXPECT_FALSE(virt::pin_user_page(fixture.root, TEST_VA, true, raw.pin));
        expect_empty(raw.pin);
        KREQUIRE_TRUE(denied.acquire(*fixture.owner, TEST_VA, false, false));
        KEXPECT_TRUE(virt::user_page_pin_still_mapped(denied.pin));
        KEXPECT_EQ(phys::page_ref_get(replacement), 3U);
    }
    KEXPECT_EQ(phys::page_ref_get(replacement), 2U);
    KEXPECT_TRUE(virt::user_page_pin_still_mapped(held.pin));
    virt::unmap_page(fixture.root, TEST_VA);
    KEXPECT_FALSE(virt::user_page_pin_still_mapped(held.pin));
    KEXPECT_FALSE(virt::user_page_pin_commit_write(held.pin));
    KEXPECT_EQ(phys::page_ref_get(replacement), 1U);
    held.reset();
    KEXPECT_EQ(phys::page_ref_get(replacement), 0U);
    expect_drained(fixture, READERS);
}

KTEST(UsercopyWritablePin, LazyAndCowFallbackPreserveIsolation) {
    PrivateTask parent;
    PrivateTask child;
    KREQUIRE_TRUE(parent.init());
    KREQUIRE_TRUE(child.init());
    virt::reserve_page_range(parent.root, TEST_VA, 1);
    task::LazyVmemRange const RANGE{.start = TEST_VA, .end = TEST_VA + paging::PAGE_SIZE, .prot = 0x3, .flags = 0x20};
#ifdef WOS_SELFTEST
    ::ker::test::numa_lifecycle::mm_debt::begin_change();
    bool const LAZY_ROW_ADDED_0 = parent.owner->lazy_vmem_ranges.push_back(RANGE);
    if (LAZY_ROW_ADDED_0) ::ker::test::numa_lifecycle::mm_debt::add_lazy_row(0);
    ::ker::test::numa_lifecycle::mm_debt::end_change();
    KREQUIRE_TRUE(LAZY_ROW_ADDED_0);
#else
    KREQUIRE_TRUE(parent.owner->lazy_vmem_ranges.push_back(RANGE));
#endif
    uint32_t const PARENT_READERS = virt::selftest_user_pagemap_reader_count(parent.root);
    uint32_t const CHILD_READERS = virt::selftest_user_pagemap_reader_count(child.root);
    ObservedPin held;
    KREQUIRE_TRUE(held.acquire(*parent.owner, TEST_VA));
    expect_route(held, 1, 0, 1);
    auto* original = static_cast<uint8_t*>(held.pin.hhdm_page);
    KEXPECT_EQ(original[0], 0U);
    KEXPECT_EQ(phys::page_ref_get(original), 2U);
    write_byte(held.pin, 0x39);
    KEXPECT_TRUE(virt::user_page_pin_commit_write(held.pin));
    held.reset();
    KREQUIRE_TRUE(held.acquire(*parent.owner, TEST_VA));
    expect_route(held, 1, 1, 0);
    held.reset();
    expect_drained(parent, PARENT_READERS);
    KREQUIRE_TRUE(virt::deep_copy_user_pagemap_cow(parent.root, child.root));
    KEXPECT_EQ(phys::page_ref_get(original), 2U);
    KEXPECT_NE(entry_raw(parent.root, TEST_VA, 1) & paging::PAGE_COW, 0U);
    KEXPECT_NE(entry_raw(child.root, TEST_VA, 1) & paging::PAGE_COW, 0U);
    KREQUIRE_TRUE(held.acquire(*parent.owner, TEST_VA));
    expect_route(held, 1, 0, 1);
    KEXPECT_NE(held.pin.physical_page, physical(original));
    auto* private_page = static_cast<uint8_t*>(held.pin.hhdm_page);
    KEXPECT_EQ(private_page[0], 0x39);
    KEXPECT_EQ(phys::page_ref_get(private_page), 2U);
    KEXPECT_EQ(phys::page_ref_get(original), 1U);
    write_byte(held.pin, 0xA5);
    KEXPECT_TRUE(virt::user_page_pin_commit_write(held.pin));
    held.reset();
    KEXPECT_EQ(private_page[0], 0xA5);
    KEXPECT_EQ(original[0], 0x39);
    KEXPECT_EQ(virt::translate(child.root, TEST_VA), physical(original));
    KEXPECT_EQ(phys::page_ref_get(private_page), 1U);
    {
        EntryChange change(parent, TEST_VA, 3, paging::PAGE_WRITE, 0);
        KREQUIRE_TRUE(change.changed);
        KREQUIRE_TRUE(held.acquire(*parent.owner, TEST_VA));
        expect_route(held, 1, 0, 1);
        KEXPECT_EQ(held.pin.physical_page, physical(private_page));
        KEXPECT_NE(entry_raw(parent.root, TEST_VA, 3) & paging::PAGE_WRITE, 0U);
        held.reset();
    }
    KREQUIRE_TRUE(held.acquire(*parent.owner, TEST_VA));
    expect_route(held, 1, 1, 0);
    held.reset();
    expect_drained(parent, PARENT_READERS);
    expect_drained(child, CHILD_READERS);
}

KTEST(UsercopyWritablePin, TaskAndRootAdmissionDrainExactly) {
    PrivateTask fixture;
    PrivateTask other;
    KREQUIRE_TRUE(fixture.init());
    KREQUIRE_TRUE(other.init());
    auto* page = fixture.map(TEST_VA);
    auto* other_page = other.map(TEST_VA, paging::page_types::USER, 0x72);
    KREQUIRE_NE(page, nullptr);
    KREQUIRE_NE(other_page, nullptr);
    uint32_t const READERS = virt::selftest_user_pagemap_reader_count(fixture.root);
    ObservedPin held;
    KREQUIRE_TRUE(held.acquire(*fixture.owner, TEST_VA));
    held.reset();
    expect_drained(fixture, READERS);
    fixture.owner->quiesce_usercopy_pagemap();
    KEXPECT_FALSE(held.acquire(*fixture.owner, TEST_VA));
    expect_empty(held.pin);
    KEXPECT_EQ(fixture.owner->replace_pagemap_after_usercopy_quiescence(fixture.root), fixture.root);
    KREQUIRE_TRUE(held.acquire(*fixture.owner, TEST_VA));
    held.reset();
    // Exchange actual privately owned roots only after all access holds drain.
    // Both fixtures retain their new root; the old expected root remains live.
    auto* old_root = fixture.root;
    auto* new_root = other.root;
    KEXPECT_EQ(fixture.owner->replace_pagemap_after_usercopy_quiescence(new_root), old_root);
    KEXPECT_EQ(other.owner->replace_pagemap_after_usercopy_quiescence(old_root), new_root);
    fixture.root = new_root;
    other.root = old_root;
    uint32_t const REPLACED_READERS = virt::selftest_user_pagemap_reader_count(fixture.root);
    {
        // Capture a genuine Task hold, then supply the live former root to the
        // lower API. No Task root is changed while that access hold exists.
        ObservedPin stale;
        KREQUIRE_TRUE(fixture.owner->try_acquire_usercopy_pagemap(stale.captured));
        stale.subject = fixture.owner.get();
        KEXPECT_EQ(stale.captured, fixture.root);
        uint32_t const OTHER_READERS = virt::selftest_user_pagemap_reader_count(other.root);
        KEXPECT_FALSE(
            virt::selftest_pin_user_page_for_task_observed(stale.subject, other.root, TEST_VA, true, true, stale.pin, stale.observation));
        expect_route(stale, 0, 0, 0);
        expect_empty(stale.pin);
        KEXPECT_EQ(virt::selftest_user_pagemap_reader_count(other.root), OTHER_READERS);
    }
    KREQUIRE_TRUE(held.acquire(*fixture.owner, TEST_VA));
    expect_route(held, 1, 1, 0);
    KEXPECT_EQ(held.pin.hhdm_page, other_page);
    held.reset();
    KEXPECT_TRUE(virt::selftest_user_pagemap_exclusive_rejects_mapped_pin(fixture.root));
    KEXPECT_FALSE(held.acquire(*fixture.owner, usercopy::USER_ADDR_LIMIT));
    KEXPECT_FALSE(held.acquire(*fixture.owner, UINT64_MAX));
    KEXPECT_FALSE(held.acquire(*fixture.owner, SECOND_VA));
    expect_route(held, 1, 0, 1);
    expect_empty(held.pin);
    RawPin raw;
    virt::UserPagePinPathObservation observation{7, 8, 9};
    KEXPECT_FALSE(virt::selftest_pin_user_page_for_task_observed(nullptr, fixture.root, TEST_VA, true, true, raw.pin, observation));
    KEXPECT_EQ(observation.early_attempts, 0U);
    KEXPECT_EQ(observation.early_successes, 0U);
    KEXPECT_EQ(observation.writable_resolution_calls, 0U);
    expect_empty(raw.pin);
    KEXPECT_EQ(virt::selftest_user_pagemap_reader_count(nullptr), UINT32_MAX);
    KEXPECT_EQ(phys::page_ref_get(page), 1U);
    KEXPECT_EQ(phys::page_ref_get(other_page), 1U);
    expect_drained(fixture, REPLACED_READERS);
    expect_drained(other, READERS);
}
