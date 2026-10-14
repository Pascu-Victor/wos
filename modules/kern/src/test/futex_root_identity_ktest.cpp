#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <new>
#include <platform/mm/addr.hpp>
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <test/ktest.hpp>

namespace {
namespace addr = ker::mod::mm::addr;
namespace mm = ker::mod::mm;
namespace paging = ker::mod::mm::paging;
namespace phys = ker::mod::mm::phys;
namespace virt = ker::mod::mm::virt;

constexpr uint64_t TEST_VA = 0x4D000000ULL;
constexpr uint64_t OTHER_VA = TEST_VA + paging::PAGE_SIZE;
constexpr uint64_t SHARED_VA = OTHER_VA + paging::PAGE_SIZE;
constexpr uint64_t PEER_SHARED_VA = SHARED_VA + paging::PAGE_SIZE;
constexpr size_t ROOT_POOL_CAPACITY = 64;
constexpr size_t MANY_ROOTS = 4097;
constexpr size_t ROOT_ACCESS_STRIPES = 4096;

auto physical(const void* page) -> uint64_t { return reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<uint64_t>(page))); }

auto root_stats() -> virt::UserRootControlStats {
    virt::UserRootControlStats out{};
    virt::get_user_root_control_stats_snapshot(out);
    return out;
}

// The observer owns one real allocator reference. Every new PTE takes a
// separate reference, and an actual UserPagePin takes its own reference.
struct FrameRef {
    FrameRef() = default;
    FrameRef(const FrameRef&) = delete;
    auto operator=(const FrameRef&) -> FrameRef& = delete;
    void* page{};
    ~FrameRef() { static_cast<void>(reset()); }
    auto init() -> bool {
        page = phys::page_alloc_may_fail(mm::PhysicalPageOwner::SELFTEST, paging::PAGE_SIZE, "root-identity-frame");
        return page != nullptr;
    }
    auto reset() -> uint32_t {
        void* const retired = page;
        page = nullptr;
        return retired != nullptr ? phys::page_ref_dec(retired) : 0;
    }
};

// No Task is constructed or registered, and these roots never become active.
// The unsupported raw root is deliberately absent from the public admission
// transaction and must use its own low-level final free after tree cleanup.
struct OwnedRoot {
    OwnedRoot() = default;
    OwnedRoot(const OwnedRoot&) = delete;
    auto operator=(const OwnedRoot&) -> OwnedRoot& = delete;
    paging::PageTable* root{};
    bool mapped{};
    bool raw_unregistered{};

    ~OwnedRoot() { reset(); }
    auto init(bool unsupported_raw = false) -> bool {
        raw_unregistered = unsupported_raw;
        if (!unsupported_raw) {
            root = virt::create_pagemap_may_fail();
            return root != nullptr;
        }
        root = static_cast<paging::PageTable*>(
            phys::page_alloc_may_fail(mm::PhysicalPageOwner::SELFTEST, paging::PAGE_SIZE, "root-identity-unregistered"));
        if (root == nullptr) return false;
        if (phys::page_mark_kind(root, mm::PageKind::PAGE_TABLE)) return true;
        phys::page_free(root);
        root = nullptr;
        return false;
    }
    void reset() {
        if (root == nullptr) return;
        KEXPECT_EQ(virt::selftest_user_pagemap_reader_count(root), 0U);
        if (mapped) virt::destroy_user_space(root, 0, "root-identity-ktest", "owned-root");
        KEXPECT_EQ(phys::page_ref_get(root), 1U);
        if (raw_unregistered) {
            phys::page_free(root);
        } else {
            virt::release_pagemap(root);
        }
        root = nullptr;
        mapped = false;
    }
    auto map(FrameRef& frame, uint64_t va, uint64_t flags = paging::page_types::USER_READONLY) -> bool {
        if (root == nullptr || frame.page == nullptr || va >= 0x0000800000000000ULL || (va & (paging::PAGE_SIZE - 1U)) != 0U ||
            (flags & paging::PAGE_PRESENT) == 0U)
            return false;
        uint64_t const backing = physical(frame.page);
        // map_page does not drop the old PTE reference when only flags change
        // on the same frame. Do not acquire another reference in that case.
        bool extra_ref = false;
        if (virt::translate(root, va) != backing) {
            if (!phys::page_ref_try_inc(frame.page)) return false;
            extra_ref = true;
        }
        virt::map_page(root, va, backing, flags);
        mapped = true;
        bool const transferred = virt::translate(root, va) == backing;
        if (!transferred && extra_ref) static_cast<void>(phys::page_ref_dec(frame.page));
        return transferred;
    }
};

struct RawPin {
    RawPin() = default;
    RawPin(const RawPin&) = delete;
    auto operator=(const RawPin&) -> RawPin& = delete;
    virt::UserPagePin pin{};
    ~RawPin() { reset(); }
    void reset() { virt::unpin_user_page(pin); }
    auto acquire(OwnedRoot& owner, uint64_t va = TEST_VA, bool writable = false) -> bool {
        reset();
        return virt::pin_user_page(owner.root, va, writable, pin);
    }
};

// KREQUIRE returns still execute these destructors. Roots are retired before
// backing memory is freed, including a partially admitted allocation prefix.
template <size_t Count>
struct EmptyRoots {
    EmptyRoots() = default;
    EmptyRoots(const EmptyRoots&) = delete;
    auto operator=(const EmptyRoots&) -> EmptyRoots& = delete;
    std::array<paging::PageTable*, Count> roots{};
    ~EmptyRoots() {
        for (auto* root : roots) {
            if (root != nullptr) virt::release_pagemap(root);
        }
    }
    auto init() -> bool {
        for (auto& root : roots) {
            root = virt::create_pagemap_may_fail();
            if (root == nullptr) return false;
        }
        return true;
    }
};

auto warm_root_pool() -> bool {
    {
        EmptyRoots<ROOT_POOL_CAPACITY> warm;
        if (!warm.init()) return false;
    }
    virt::PageTablePoolStatsSnapshot pool{};
    virt::get_page_table_pool_stats_snapshot(pool);
    return pool.capacity == ROOT_POOL_CAPACITY && pool.cached_pages == pool.capacity;
}

struct RootBalance {
    phys::PhysicalBalanceSnapshot before{};
    virt::PageTablePoolStatsSnapshot pool_before{};
    virt::UserRootControlStats controls_before{root_stats()};
    uint64_t kernel_tables_before{};
    bool unchanged_pool{};

    explicit RootBalance(bool retain_pool = false) : unchanged_pool(retain_pool) {
        // The registry getter is allocation-free and not ASan-instrumented.
        // Bracket accounting snapshots so its read cannot create an apparent
        // permanent-table delta, and concurrent kernel growth is not hidden.
        // These completed-call selftest observations are not a joint atomic
        // snapshot with unrelated concurrent kernel mapping writers.
        kernel_tables_before = virt::selftest_kernel_page_table_count();
        phys::get_physical_balance_snapshot(before);
        virt::get_page_table_pool_stats_snapshot(pool_before);
        KEXPECT_EQ(virt::selftest_kernel_page_table_count(), kernel_tables_before);
    }
    RootBalance(const RootBalance&) = delete;
    auto operator=(const RootBalance&) -> RootBalance& = delete;
    ~RootBalance() {
        uint64_t const kernel_tables_after = virt::selftest_kernel_page_table_count();
        phys::PhysicalBalanceSnapshot after{};
        phys::get_physical_balance_snapshot(after);
        virt::PageTablePoolStatsSnapshot pool_after{};
        virt::get_page_table_pool_stats_snapshot(pool_after);
        auto const controls_after = root_stats();
        KEXPECT_EQ(virt::selftest_kernel_page_table_count(), kernel_tables_after);
        KEXPECT_EQ(controls_after.active_roots, controls_before.active_roots);
        KEXPECT_EQ(controls_after.control_pages, controls_before.control_pages);
        KEXPECT_TRUE(controls_after.last_serial >= controls_before.last_serial);
        for (auto owner : {mm::PhysicalPageOwner::USER_ROOT_CONTROL, mm::PhysicalPageOwner::SELFTEST}) {
            size_t const i = static_cast<size_t>(owner);
            KEXPECT_EQ(after.owners.at(i).pages, before.owners.at(i).pages);
            KEXPECT_EQ(after.owners.at(i).objects, before.owners.at(i).objects);
        }
        size_t const tables = static_cast<size_t>(mm::PhysicalPageOwner::PAGE_TABLE);
        // KASan demand paging may install genuine permanent kernel tables
        // while fresh root/frame memory is touched. Subtract only the exact
        // actual registered table population, never an allowance for growth.
        KEXPECT_TRUE(kernel_tables_after >= kernel_tables_before);
        KEXPECT_EQ(after.owners.at(tables).pages + kernel_tables_before, before.owners.at(tables).pages + kernel_tables_after);
        KEXPECT_EQ(after.owners.at(tables).objects + kernel_tables_before, before.owners.at(tables).objects + kernel_tables_after);
        size_t const reserve = static_cast<size_t>(mm::PhysicalPageOwner::PAGE_TABLE_POOL_RESERVE);
        // Intermediate tables can consume pooled roots and are physically
        // freed by destroy_user_space. Account that legitimate pool shrink.
        KEXPECT_EQ(after.owners.at(reserve).pages + pool_before.cached_pages, before.owners.at(reserve).pages + pool_after.cached_pages);
        KEXPECT_EQ(after.owners.at(reserve).objects + pool_before.cached_pages,
                   before.owners.at(reserve).objects + pool_after.cached_pages);
        if (unchanged_pool) KEXPECT_EQ(pool_after.cached_pages, pool_before.cached_pages);
        KEXPECT_EQ(after.identity_mismatch_pages, before.identity_mismatch_pages);
        KEXPECT_EQ(after.untracked_unreclaimable_pages, before.untracked_unreclaimable_pages);
    }
};

struct ManyEmptyRoots {
    using RootArray = std::array<paging::PageTable*, MANY_ROOTS>;
    static_assert(sizeof(RootArray) <= 16 * paging::PAGE_SIZE);
    ManyEmptyRoots() = default;
    ManyEmptyRoots(const ManyEmptyRoots&) = delete;
    auto operator=(const ManyEmptyRoots&) -> ManyEmptyRoots& = delete;
    void* storage{};
    RootArray* roots{};
    size_t admitted{};
    ~ManyEmptyRoots() {
        for (size_t i = 0; i < admitted; ++i) virt::release_pagemap(roots->at(i));
        if (roots != nullptr) roots->~RootArray();
        phys::page_free(storage);
    }
    auto init() -> bool {
        // 4097 pointers round to one 64 KiB buddy allocation. The full cohort
        // has about 16 MiB of roots plus small order0 control metadata.
        storage = phys::page_alloc_may_fail(mm::PhysicalPageOwner::SELFTEST, sizeof(RootArray), "root-identity-pointers");
        if (storage == nullptr) return false;
        roots = ::new (storage) RootArray{};
        for (; admitted < MANY_ROOTS; ++admitted) {
            roots->at(admitted) = virt::create_pagemap_may_fail();
            if (roots->at(admitted) == nullptr) return false;
        }
        return true;
    }
};

void expect_identity(const virt::UserFutexIdentity& actual, uint64_t root, uint64_t va, uint64_t pa, bool shared) {
    KEXPECT_EQ(actual.root_incarnation, root);
    KEXPECT_EQ(actual.virtual_word, va);
    KEXPECT_EQ(actual.physical_word, pa);
    KEXPECT_EQ(actual.shared, shared);
}
}  // namespace

KTEST(RootIdentity, PoolAddressReuseGetsNewIncarnation) {
    KREQUIRE_TRUE(warm_root_pool());
    RootBalance balance(true);
    OwnedRoot first;
    KREQUIRE_TRUE(first.init());
    uint64_t const first_serial = virt::selftest_user_root_incarnation(first.root);
    KREQUIRE_NE(first_serial, 0U);
    KEXPECT_EQ(phys::page_kind_get(first.root), mm::PageKind::PAGE_TABLE);
    KEXPECT_EQ(phys::page_ref_get(first.root), 1U);
    KEXPECT_EQ(root_stats().active_roots, balance.controls_before.active_roots + 1U);
    KEXPECT_EQ(virt::selftest_user_root_incarnation(virt::get_kernel_pagemap()), 0U);

    auto* const reused_address = first.root;
    // An unused nonpresent entry may be poisoned without owning a child tree.
    first.root->entries.at(7).frame = 0x12345;
    first.reset();
    KEXPECT_EQ(root_stats().control_pages, balance.controls_before.control_pages);
    OwnedRoot replacement;
    KREQUIRE_TRUE(replacement.init());
    KEXPECT_EQ(replacement.root, reused_address);
    uint64_t const replacement_serial = virt::selftest_user_root_incarnation(replacement.root);
    KEXPECT_TRUE(replacement_serial > first_serial);
    KEXPECT_EQ(phys::page_ref_get(replacement.root), 1U);
    paging::PageTableEntry const empty{};
    bool zeroed = true;
    for (auto const& entry : replacement.root->entries) zeroed &= std::memcmp(&entry, &empty, sizeof(entry)) == 0;
    KEXPECT_TRUE(zeroed);
}

KTEST(RootIdentity, RepeatedActualRetirementReturnsMetadata) {
    KREQUIRE_TRUE(warm_root_pool());
    RootBalance balance(true);
    uint64_t previous = balance.controls_before.last_serial;
    for (size_t i = 0; i < 1000; ++i) {
        {
            OwnedRoot root;
            KREQUIRE_TRUE(root.init());
            uint64_t const serial = virt::selftest_user_root_incarnation(root.root);
            KREQUIRE_TRUE(serial > previous);
            previous = serial;
            KEXPECT_EQ(phys::page_ref_get(root.root), 1U);
        }
        auto const retired = root_stats();
        KEXPECT_EQ(retired.active_roots, balance.controls_before.active_roots);
        KEXPECT_EQ(retired.control_pages, balance.controls_before.control_pages);
    }
    // This is 1000 empty-root transactions, not 1000 Goal39 lifecycles.
    KEXPECT_TRUE(previous >= balance.controls_before.last_serial + 1000U);
}

KTEST(RootIdentity, ActualCapacityExceedsStripedReaderCount) {
    KREQUIRE_TRUE(warm_root_pool());
    RootBalance balance(true);
    // Leave at least 16 MiB outside this bounded ~17 MiB cohort. Admission
    // failure still destroys every successfully created empty root.
    KREQUIRE_TRUE(balance.before.free_pages >= MANY_ROOTS + 4096U + 128U);
    ManyEmptyRoots cohort;
    KREQUIRE_TRUE(cohort.init());
    KREQUIRE_EQ(cohort.admitted, MANY_ROOTS);
    auto const live = root_stats();
    KEXPECT_EQ(live.active_roots, balance.controls_before.active_roots + MANY_ROOTS);
    KEXPECT_TRUE(live.control_pages >= (live.active_roots + 95U) / 96U);
    phys::PhysicalBalanceSnapshot physical_live{};
    phys::get_physical_balance_snapshot(physical_live);
    size_t const control_owner = static_cast<size_t>(mm::PhysicalPageOwner::USER_ROOT_CONTROL);
    KEXPECT_EQ(physical_live.owners.at(control_owner).pages, live.control_pages);
    KEXPECT_EQ(physical_live.owners.at(control_owner).objects, live.control_pages);

    std::array<uint64_t, ROOT_ACCESS_STRIPES / 64> seen{};
    bool collided = false;
    bool roots_valid = true;
    uint64_t previous = balance.controls_before.last_serial;
    for (size_t i = 0; i < cohort.admitted; ++i) {
        auto* root = cohort.roots->at(i);
        uint64_t const serial = virt::selftest_user_root_incarnation(root);
        roots_valid &= serial > previous && phys::page_ref_get(root) == 1U && phys::page_kind_get(root) == mm::PageKind::PAGE_TABLE;
        previous = serial;
        size_t const stripe = (reinterpret_cast<uintptr_t>(root) >> paging::PAGE_SHIFT) & (ROOT_ACCESS_STRIPES - 1U);
        uint64_t const bit = uint64_t{1} << (stripe & 63U);
        collided |= (seen.at(stripe >> 6U) & bit) != 0U;
        seen.at(stripe >> 6U) |= bit;
    }
    KEXPECT_TRUE(roots_valid);
    KEXPECT_TRUE(collided);
    KEXPECT_TRUE(live.last_serial >= previous);
}

KTEST(RootIdentity, ActualPrivateAndSharedLeafProvenance) {
    RootBalance balance;
    FrameRef backing;
    KREQUIRE_TRUE(backing.init());
    OwnedRoot first;
    OwnedRoot peer;
    KREQUIRE_TRUE(first.init());
    KREQUIRE_TRUE(peer.init());
    KREQUIRE_TRUE(first.map(backing, TEST_VA));
    KREQUIRE_TRUE(first.map(backing, OTHER_VA));
    KREQUIRE_TRUE(peer.map(backing, TEST_VA));
    KREQUIRE_TRUE(first.map(backing, SHARED_VA, paging::page_types::USER_READONLY | paging::PAGE_SHARED));
    KREQUIRE_TRUE(peer.map(backing, PEER_SHARED_VA, paging::page_types::USER_READONLY | paging::PAGE_SHARED));
    uint64_t const first_serial = virt::selftest_user_root_incarnation(first.root);
    uint64_t const peer_serial = virt::selftest_user_root_incarnation(peer.root);
    KREQUIRE_NE(first_serial, 0U);
    KREQUIRE_NE(peer_serial, first_serial);
    std::array<virt::UserFutexIdentity, 5> identities{};
    std::array<OwnedRoot*, 5> owners{&first, &first, &peer, &first, &peer};
    std::array<uint64_t, 5> words{TEST_VA + 4, OTHER_VA + 4, TEST_VA + 4, SHARED_VA + 4, PEER_SHARED_VA + 4};
    for (size_t i = 0; i < identities.size(); ++i) {
        RawPin pin;
        KREQUIRE_TRUE(pin.acquire(*owners.at(i), words.at(i)));
        KREQUIRE_TRUE(virt::capture_user_futex_identity(pin.pin, words.at(i), identities.at(i)));
        KEXPECT_TRUE(virt::user_futex_identity_still_mapped(pin.pin, identities.at(i)));
        expect_identity(identities.at(i), i == 2 || i == 4 ? peer_serial : first_serial, words.at(i), physical(backing.page) + 4, i >= 3);
    }
    KEXPECT_NE(identities.at(0).virtual_word, identities.at(1).virtual_word);
    KEXPECT_NE(identities.at(0).root_incarnation, identities.at(2).root_incarnation);
    KEXPECT_EQ(identities.at(3).physical_word, identities.at(4).physical_word);
    KEXPECT_NE(identities.at(3).virtual_word, identities.at(4).virtual_word);
    KEXPECT_EQ(phys::page_ref_get(backing.page), 6U);  // Observer and five PTEs.
    // These real leaves qualify provenance, not complete futex queue or
    // canonical shared-file/object behavior.
}

KTEST(RootIdentity, SharingAndPermissionChangesInvalidateExactSnapshot) {
    RootBalance balance;
    FrameRef backing;
    KREQUIRE_TRUE(backing.init());
    OwnedRoot root;
    KREQUIRE_TRUE(root.init());
    KREQUIRE_TRUE(root.map(backing, TEST_VA, paging::page_types::USER));
    RawPin readable;
    RawPin writable;
    KREQUIRE_TRUE(readable.acquire(root));
    KREQUIRE_TRUE(writable.acquire(root, TEST_VA, true));
    virt::UserFutexIdentity read_identity{};
    virt::UserFutexIdentity write_identity{};
    KREQUIRE_TRUE(virt::capture_user_futex_identity(readable.pin, TEST_VA, read_identity));
    KREQUIRE_TRUE(virt::capture_user_futex_identity(writable.pin, TEST_VA, write_identity));
    virt::unify_page_flags(root.root, TEST_VA, paging::page_types::USER_READONLY);
    KEXPECT_TRUE(virt::user_futex_identity_still_mapped(readable.pin, read_identity));
    KEXPECT_FALSE(virt::user_futex_identity_still_mapped(writable.pin, write_identity));
    writable.reset();

    KREQUIRE_TRUE(root.map(backing, TEST_VA, paging::page_types::USER_READONLY | paging::PAGE_SHARED));
    KEXPECT_TRUE(virt::user_page_pin_still_mapped(readable.pin));
    KEXPECT_FALSE(virt::user_futex_identity_still_mapped(readable.pin, read_identity));
    virt::UserFutexIdentity shared_identity{};
    KREQUIRE_TRUE(virt::capture_user_futex_identity(readable.pin, TEST_VA, shared_identity));
    KEXPECT_TRUE(shared_identity.shared);
    KEXPECT_TRUE(virt::user_futex_identity_still_mapped(readable.pin, shared_identity));
    KREQUIRE_TRUE(root.map(backing, TEST_VA));
    KEXPECT_FALSE(virt::user_futex_identity_still_mapped(readable.pin, shared_identity));
    KEXPECT_TRUE(virt::user_futex_identity_still_mapped(readable.pin, read_identity));

    virt::unify_page_flags(root.root, TEST_VA, paging::PAGE_PRESENT | paging::PAGE_NX);
    KEXPECT_FALSE(virt::user_page_pin_still_mapped(readable.pin));
    KEXPECT_FALSE(virt::user_futex_identity_still_mapped(readable.pin, read_identity));
    virt::UserFutexIdentity refused{};
    KEXPECT_FALSE(virt::capture_user_futex_identity(readable.pin, TEST_VA, refused));
    expect_identity(refused, 0, 0, 0, false);
    KREQUIRE_TRUE(root.map(backing, TEST_VA));
}

KTEST(RootIdentity, FrameReplacementKeepsPrivateRootAndWord) {
    RootBalance balance;
    FrameRef original;
    FrameRef replacement;
    KREQUIRE_TRUE(original.init());
    KREQUIRE_TRUE(replacement.init());
    OwnedRoot root;
    KREQUIRE_TRUE(root.init());
    KREQUIRE_TRUE(root.map(original, TEST_VA));
    {
        RawPin old_pin;
        KREQUIRE_TRUE(old_pin.acquire(root));
        virt::UserFutexIdentity old_identity{};
        KREQUIRE_TRUE(virt::capture_user_futex_identity(old_pin.pin, TEST_VA, old_identity));
        KREQUIRE_TRUE(root.map(replacement, TEST_VA));
        KEXPECT_FALSE(virt::user_futex_identity_still_mapped(old_pin.pin, old_identity));
        KEXPECT_EQ(phys::page_ref_get(original.page), 2U);  // Observer and old pin.
        RawPin new_pin;
        KREQUIRE_TRUE(new_pin.acquire(root));
        virt::UserFutexIdentity new_identity{};
        KREQUIRE_TRUE(virt::capture_user_futex_identity(new_pin.pin, TEST_VA, new_identity));
        KEXPECT_EQ(new_identity.root_incarnation, old_identity.root_incarnation);
        KEXPECT_EQ(new_identity.virtual_word, old_identity.virtual_word);
        KEXPECT_NE(new_identity.physical_word, old_identity.physical_word);
        KEXPECT_FALSE(new_identity.shared);
        KEXPECT_TRUE(virt::user_futex_identity_still_mapped(new_pin.pin, new_identity));
        virt::unmap_page(root.root, TEST_VA);
        KEXPECT_FALSE(virt::user_futex_identity_still_mapped(new_pin.pin, new_identity));
        KEXPECT_EQ(phys::page_ref_get(replacement.page), 2U);
    }
    root.reset();
    KEXPECT_EQ(original.reset(), 0U);
    KEXPECT_EQ(replacement.reset(), 0U);
}

KTEST(RootIdentity, WordBoundsBusyOutputAndUnregisteredRootRefuse) {
    RootBalance balance;
    FrameRef backing;
    KREQUIRE_TRUE(backing.init());
    OwnedRoot admitted;
    OwnedRoot unsupported;
    KREQUIRE_TRUE(admitted.init());
    KREQUIRE_TRUE(unsupported.init(true));
    KREQUIRE_TRUE(admitted.map(backing, TEST_VA));
    KREQUIRE_TRUE(unsupported.map(backing, TEST_VA));
    RawPin pin;
    RawPin unknown_pin;
    KREQUIRE_TRUE(pin.acquire(admitted));
    KREQUIRE_TRUE(unknown_pin.acquire(unsupported));
    KEXPECT_EQ(virt::selftest_user_root_incarnation(unsupported.root), 0U);
    KEXPECT_TRUE(virt::user_page_pin_still_mapped(unknown_pin.pin));
    virt::UserFutexIdentity unknown{.virtual_word = 0x55, .physical_word = 0xAA, .shared = true};
    KEXPECT_FALSE(virt::capture_user_futex_identity(unknown_pin.pin, TEST_VA, unknown));
    expect_identity(unknown, 0, 0x55, 0xAA, true);
    KEXPECT_FALSE(virt::user_futex_identity_still_mapped(unknown_pin.pin, unknown));

    for (uint64_t word :
         {TEST_VA + 1, TEST_VA + 2, TEST_VA + paging::PAGE_SIZE - 2, OTHER_VA, TEST_VA - 4, uint64_t{0x0000800000000000ULL}}) {
        virt::UserFutexIdentity refused{.virtual_word = 0x55, .physical_word = 0xAA, .shared = true};
        KEXPECT_FALSE(virt::capture_user_futex_identity(pin.pin, word, refused));
        expect_identity(refused, 0, 0x55, 0xAA, true);
    }
    for (uint64_t word : {TEST_VA, TEST_VA + 4, TEST_VA + paging::PAGE_SIZE - sizeof(int)}) {
        virt::UserFutexIdentity identity{};
        KREQUIRE_TRUE(virt::capture_user_futex_identity(pin.pin, word, identity));
        expect_identity(identity, virt::selftest_user_root_incarnation(admitted.root), word, physical(backing.page) + word - TEST_VA,
                        false);
        KEXPECT_TRUE(virt::user_futex_identity_still_mapped(pin.pin, identity));
        auto const original = identity;
        KEXPECT_FALSE(virt::capture_user_futex_identity(pin.pin, word, identity));
        expect_identity(identity, original.root_incarnation, original.virtual_word, original.physical_word, original.shared);
        identity.root_incarnation = 0;
        KEXPECT_FALSE(virt::user_futex_identity_still_mapped(pin.pin, identity));
        identity = original;
        identity.root_incarnation = original.root_incarnation == 1U ? 2U : 1U;
        KEXPECT_FALSE(virt::user_futex_identity_still_mapped(pin.pin, identity));
        identity = original;
        identity.virtual_word = OTHER_VA;
        KEXPECT_FALSE(virt::user_futex_identity_still_mapped(pin.pin, identity));
        identity = original;
        identity.physical_word += paging::PAGE_SIZE;
        KEXPECT_FALSE(virt::user_futex_identity_still_mapped(pin.pin, identity));
        identity = original;
        identity.shared = true;
        KEXPECT_FALSE(virt::user_futex_identity_still_mapped(pin.pin, identity));
    }
}
