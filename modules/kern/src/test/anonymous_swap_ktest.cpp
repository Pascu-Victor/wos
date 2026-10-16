#ifdef WOS_SELFTEST
#include <test/numa_lifecycle_mm_debt.hpp>
#endif
#include <abi/callnums/vmem.h>

#include <array>
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
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/swap.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sys/usercopy.hpp>
#include <test/fault_block_device.hpp>
#include <test/ktest.hpp>

namespace {

namespace addr = ker::mod::mm::addr;
namespace mm = ker::mod::mm;
namespace paging = ker::mod::mm::paging;
namespace phys = ker::mod::mm::phys;
namespace swap = ker::mod::mm::swap;
namespace virt = ker::mod::mm::virt;
namespace numa = ker::mod::mm::numa;
namespace usercopy = ker::mod::sys::usercopy;

constexpr uint64_t TEST_VADDR = 0x5A000000ULL;
constexpr uint64_t SWAP_PAGES = 16;
// Root-only native calls supply an explicit valid copied context before gates.
auto capture_test_swap_context(numa::AllocationContext& output) -> bool {
    if (numa::capture_allocation_context(numa::Policy{}, output) != numa::AllocationError::NONE) return false;
    output.virtual_page_index = TEST_VADDR / paging::PAGE_SIZE;
    return true;
}

using PageBytes = std::array<uint8_t, paging::PAGE_SIZE>;

auto make_storage() -> std::unique_ptr<ker::test::FaultBlockDevice> {
    return std::unique_ptr<ker::test::FaultBlockDevice>(new (std::nothrow) ker::test::FaultBlockDevice(paging::PAGE_SIZE, SWAP_PAGES));
}

auto activate_storage(ker::test::FaultBlockDevice& storage, const char* name) -> int {
    swap::SwapExtent const EXTENT{.device = storage.device(), .start_block = 0, .page_count = SWAP_PAGES};
    return swap::activate_extents(name, &EXTENT, 1);
}

auto map_pattern_page(paging::PageTable* pagemap, uint8_t pattern) -> bool {
    auto* page = phys::page_alloc(mm::PhysicalPageOwner::USER_PRIVATE_MAPPING, paging::PAGE_SIZE, "anon_swap_ktest");
    if (page == nullptr) {
        return false;
    }
    std::memset(page, pattern, paging::PAGE_SIZE);
    uint64_t const PADDR = reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<uint64_t>(page)));
    virt::map_page(pagemap, TEST_VADDR, PADDR, paging::page_types::USER);
    return true;
}

auto mapped_bytes(paging::PageTable* pagemap) -> const uint8_t* {
    uint64_t const PHYS = virt::translate(pagemap, TEST_VADDR);
    return PHYS == virt::PADDR_INVALID ? nullptr : reinterpret_cast<const uint8_t*>(addr::get_virt_pointer(PHYS));
}

void destroy_test_pagemap(paging::PageTable* pagemap, const char* reason) {
    if (pagemap == nullptr) {
        return;
    }
    virt::destroy_user_space(pagemap, 0, "anon_swap_ktest", reason);
    virt::release_pagemap(pagemap);
}

using log = ker::mod::dbg::logger<"anon-swap-test">;

auto pattern_byte(size_t index) -> uint8_t { return static_cast<uint8_t>((index * 37U + 0x53U) % 251U + 1U); }

auto private_mapping_counts() -> phys::PhysicalOwnerSnapshot {
    phys::PhysicalBalanceSnapshot snapshot{};
    phys::get_physical_balance_snapshot(snapshot);
    KEXPECT_EQ(snapshot.identity_mismatch_pages, 0U);
    return snapshot.owners[static_cast<size_t>(mm::PhysicalPageOwner::USER_PRIVATE_MAPPING)];
}

// This walker is only for the fixture's exclusive never-active 4 KiB root.
// It borrows no node beyond the call and is never used on a scheduled root.
auto owned_leaf_raw(paging::PageTable* root) -> uint64_t {
    auto* table = root;
    for (unsigned level = 4; level > 0; --level) {
        auto const INDEX = static_cast<size_t>((TEST_VADDR >> (12U + 9U * (level - 1U))) & 511U);
        auto const& entry = table->entries.at(INDEX);
        if (level == 1) return std::bit_cast<uint64_t>(entry);
        if (entry.present == 0 || entry.pagesize != 0) return 0;
        table = reinterpret_cast<paging::PageTable*>(addr::get_virt_pointer(static_cast<uint64_t>(entry.frame) << paging::PAGE_SHIFT));
    }
    return 0;
}

class TrackedSwapFixture {
   public:
    explicit TrackedSwapFixture(const char* name) : name_(name), owner_before_(private_mapping_counts()) {
        swap::get_stats(&slots_before_);
    }
    ~TrackedSwapFixture() {
        if (root_ != nullptr) {
            KEXPECT_EQ(subject_->usercopy_pagemap_accesses.load(std::memory_order_acquire), 0U);
            KEXPECT_EQ(virt::selftest_user_pagemap_reader_count(root_), 0U);
            KEXPECT_EQ(subject_->detach_pagemap_after_usercopy_quiescence(), root_);
            ker::mod::sched::task::release_lazy_vmem_ranges(*subject_);
            destroy_test_pagemap(root_, "tracked-software-refusal");
        }
        if (active_) {
            int const RESULT = swap::swapoff_path(name_);
            KEXPECT_EQ(RESULT, 0);
            // A failed deactivation must never leave an area pointing at freed
            // test-device storage. This failure-only retained owner is not a
            // successful closure; the failed VM is bounded and disposable.
            if (RESULT != 0) (void)storage_.release();
        }
        auto const OWNER_AFTER = private_mapping_counts();
        KEXPECT_EQ(OWNER_AFTER.pages, owner_before_.pages);
        KEXPECT_EQ(OWNER_AFTER.objects, owner_before_.objects);
        swap::SwapStats slots_after{};
        swap::get_stats(&slots_after);
        KEXPECT_EQ(slots_after.used_bytes, slots_before_.used_bytes);
        KEXPECT_EQ(slots_after.free_bytes, slots_before_.free_bytes);
        KEXPECT_EQ(slots_after.total_bytes, slots_before_.total_bytes);
        KEXPECT_EQ(slots_after.active_areas, slots_before_.active_areas);
    }
    TrackedSwapFixture(const TrackedSwapFixture&) = delete;
    auto operator=(const TrackedSwapFixture&) -> TrackedSwapFixture& = delete;

    auto init() -> bool {
        if (!capture_test_swap_context(placement_)) return false;
        storage_ = make_storage();
        if (storage_ == nullptr || !storage_->valid() || activate_storage(*storage_, name_) != 0) return false;
        active_ = true;
        subject_.reset(new (std::nothrow) ker::mod::sched::task::Task{});
        if (subject_ == nullptr) return false;
        root_ = virt::create_pagemap();
        if (root_ == nullptr) return false;
        subject_->type = ker::mod::sched::task::TaskType::PROCESS;
        subject_->pagemap = root_;
        namespace vmabi = ker::abi::vmem;
#ifdef WOS_SELFTEST
        ::ker::test::numa_lifecycle::mm_debt::begin_change();
        bool const LAZY_ROW_ADDED_0 = subject_->lazy_vmem_ranges.push_back({.start = TEST_VADDR,
                                                                            .end = TEST_VADDR + paging::PAGE_SIZE,
                                                                            .prot = vmabi::PROT_READ | vmabi::PROT_WRITE,
                                                                            .flags = vmabi::MAP_PRIVATE | vmabi::MAP_ANONYMOUS,
                                                                            .file = nullptr});
        if (LAZY_ROW_ADDED_0) ::ker::test::numa_lifecycle::mm_debt::add_lazy_row(0);
        ::ker::test::numa_lifecycle::mm_debt::end_change();
        if (!LAZY_ROW_ADDED_0 || !map_pattern_page(root_, 0x53)) return false;
#else
        if (!subject_->lazy_vmem_ranges.push_back({.start = TEST_VADDR,
                                                   .end = TEST_VADDR + paging::PAGE_SIZE,
                                                   .prot = vmabi::PROT_READ | vmabi::PROT_WRITE,
                                                   .flags = vmabi::MAP_PRIVATE | vmabi::MAP_ANONYMOUS,
                                                   .file = nullptr}) ||
            !map_pattern_page(root_, 0x53))
            return false;
#endif
        auto* bytes = const_cast<uint8_t*>(mapped_bytes(root_));
        if (bytes == nullptr) return false;
        for (size_t i = 0; i < paging::PAGE_SIZE; ++i) bytes[i] = pattern_byte(i);
        if (!virt::selftest_anonymous_swap_pageout(root_, TEST_VADDR, placement_)) return false;
        marker_ = owned_leaf_raw(root_);
        if ((marker_ & paging::PAGE_PRESENT) != 0 || (marker_ & paging::PAGE_RESERVED) == 0) return false;
        swap::get_stats(&slots_swapped_);
        virt::get_anonymous_swap_stats_snapshot(runtime_swapped_);
        owner_swapped_ = private_mapping_counts();
        events_swapped_ = storage_->event_count();
        return virt::selftest_anonymous_swap_is_swapped(root_, TEST_VADDR);
    }

    auto subject() -> ker::mod::sched::task::Task& { return *subject_; }

    void expect_preserved() {
        KEXPECT_EQ(owned_leaf_raw(root_), marker_);
        KEXPECT_EQ(virt::translate(root_, TEST_VADDR), virt::PADDR_INVALID);
        KEXPECT_TRUE(virt::selftest_anonymous_swap_is_swapped(root_, TEST_VADDR));
        auto const MEMORY = virt::collect_user_memory_stats(root_);
        KEXPECT_EQ(MEMORY.resident_pages, 0U);
        KEXPECT_EQ(MEMORY.swapped_pages, 1U);
        KEXPECT_EQ(MEMORY.swap_inflight_pages, 0U);
        auto const OWNER = private_mapping_counts();
        KEXPECT_EQ(OWNER.pages, owner_swapped_.pages);
        KEXPECT_EQ(OWNER.objects, owner_swapped_.objects);
        swap::SwapStats slots{};
        swap::get_stats(&slots);
        KEXPECT_EQ(slots.used_bytes, slots_swapped_.used_bytes);
        KEXPECT_EQ(slots.free_bytes, slots_swapped_.free_bytes);
        KEXPECT_EQ(storage_->event_count(), events_swapped_);
        virt::AnonymousSwapStatsSnapshot runtime{};
        virt::get_anonymous_swap_stats_snapshot(runtime);
        KEXPECT_EQ(runtime.swapped_pages, runtime_swapped_.swapped_pages);
        KEXPECT_EQ(runtime.pageout_inflight, runtime_swapped_.pageout_inflight);
        KEXPECT_EQ(runtime.pagein_inflight, runtime_swapped_.pagein_inflight);
        KEXPECT_EQ(runtime.pagein_attempts, runtime_swapped_.pagein_attempts);
        KEXPECT_EQ(runtime.pagein_successes, runtime_swapped_.pagein_successes);
        KEXPECT_EQ(runtime.pagein_failures, runtime_swapped_.pagein_failures);
        KEXPECT_EQ(subject_->usercopy_pagemap_accesses.load(std::memory_order_acquire), 0U);
        KEXPECT_EQ(virt::selftest_user_pagemap_reader_count(root_), 0U);
    }

    auto recover_pattern() -> bool { return recover_pattern(placement_); }
    auto recover_pattern(const numa::AllocationContext& placement) -> bool {
        if (!virt::selftest_anonymous_swap_pagein(root_, TEST_VADDR, placement)) return false;
        auto const* bytes = mapped_bytes(root_);
        if (bytes == nullptr) return false;
        for (size_t i = 0; i < paging::PAGE_SIZE; ++i)
            if (bytes[i] != pattern_byte(i)) return false;
        KEXPECT_EQ(phys::page_ref_get(const_cast<uint8_t*>(bytes)), 1U);
        KEXPECT_FALSE(virt::selftest_anonymous_swap_is_swapped(root_, TEST_VADDR));
        auto const MEMORY = virt::collect_user_memory_stats(root_);
        KEXPECT_EQ(MEMORY.resident_pages, 1U);
        KEXPECT_EQ(MEMORY.swapped_pages, 0U);
        KEXPECT_EQ(MEMORY.swap_inflight_pages, 0U);
        return true;
    }

    auto pagein_with(const numa::AllocationContext& placement) -> bool {
        return virt::selftest_anonymous_swap_pagein(root_, TEST_VADDR, placement);
    }
    auto resident_node() -> numa::NodeId {
        auto const physical = virt::translate(root_, TEST_VADDR);
        return physical == virt::PADDR_INVALID ? numa::INVALID_NODE : numa::physical_node(physical);
    }

   private:
    numa::AllocationContext placement_{};
    const char* name_;
    phys::PhysicalOwnerSnapshot owner_before_{};
    swap::SwapStats slots_before_{};
    std::unique_ptr<ker::test::FaultBlockDevice> storage_;
    std::unique_ptr<ker::mod::sched::task::Task> subject_;
    paging::PageTable* root_{};
    bool active_{};
    uint64_t marker_{};
    phys::PhysicalOwnerSnapshot owner_swapped_{};
    swap::SwapStats slots_swapped_{};
    virt::AnonymousSwapStatsSnapshot runtime_swapped_{};
    size_t events_swapped_{};
};

}  // namespace

KTEST(AnonymousSwap, RealPageoutPageinPreservesDataAndAccounting) {
    numa::AllocationContext placement{};
    KREQUIRE_TRUE(capture_test_swap_context(placement));
    constexpr const char* NAME = "ktest-anon-swap-roundtrip";
    auto storage = make_storage();
    KREQUIRE_NE(storage.get(), nullptr);
    KREQUIRE_TRUE(storage->valid());
    KREQUIRE_EQ(activate_storage(*storage, NAME), 0);

    auto* pagemap = virt::create_pagemap();
    KREQUIRE_NE(pagemap, nullptr);
    KREQUIRE_TRUE(map_pattern_page(pagemap, 0xA6));
    KREQUIRE_TRUE(virt::selftest_anonymous_swap_pageout(pagemap, TEST_VADDR, placement));
    KEXPECT_TRUE(virt::selftest_anonymous_swap_is_swapped(pagemap, TEST_VADDR));
    auto memory = virt::collect_user_memory_stats(pagemap);
    KEXPECT_EQ(memory.resident_pages, 0U);
    KEXPECT_EQ(memory.swapped_pages, 1U);

    KREQUIRE_TRUE(virt::selftest_anonymous_swap_pagein(pagemap, TEST_VADDR, placement));
    auto const* bytes = mapped_bytes(pagemap);
    KREQUIRE_NE(bytes, nullptr);
    PageBytes expected{};
    expected.fill(0xA6);
    KEXPECT_TRUE(std::memcmp(bytes, expected.data(), expected.size()) == 0);
    memory = virt::collect_user_memory_stats(pagemap);
    KEXPECT_EQ(memory.resident_pages, 1U);
    KEXPECT_EQ(memory.swapped_pages, 0U);

    destroy_test_pagemap(pagemap, "roundtrip");
    KEXPECT_EQ(swap::swapoff_path(NAME), 0);
}

KTEST(AnonymousSwap, SwappedForkPagesInDistinctCowFrames) {
    numa::AllocationContext placement{};
    KREQUIRE_TRUE(capture_test_swap_context(placement));
    constexpr const char* NAME = "ktest-anon-swap-fork";
    auto storage = make_storage();
    KREQUIRE_NE(storage.get(), nullptr);
    KREQUIRE_TRUE(storage->valid());
    KREQUIRE_EQ(activate_storage(*storage, NAME), 0);

    auto* parent = virt::create_pagemap();
    auto* child = virt::create_pagemap();
    KREQUIRE_NE(parent, nullptr);
    KREQUIRE_NE(child, nullptr);
    KREQUIRE_TRUE(map_pattern_page(parent, 0x3C));
    KREQUIRE_TRUE(virt::selftest_anonymous_swap_pageout(parent, TEST_VADDR, placement));
    KREQUIRE_TRUE(virt::deep_copy_user_pagemap_cow(parent, child));
    KEXPECT_TRUE(virt::selftest_anonymous_swap_is_swapped(parent, TEST_VADDR));
    KEXPECT_TRUE(virt::selftest_anonymous_swap_is_swapped(child, TEST_VADDR));
    KREQUIRE_TRUE(virt::selftest_anonymous_swap_pagein(parent, TEST_VADDR, placement));
    KREQUIRE_TRUE(virt::selftest_anonymous_swap_pagein(child, TEST_VADDR, placement));
    KEXPECT_NE(virt::translate(parent, TEST_VADDR), virt::translate(child, TEST_VADDR));

    ker::mod::sched::task::Task child_task{};
    child_task.type = ker::mod::sched::task::TaskType::PROCESS;
    child_task.pagemap = child;
    uint8_t const replacement = 0xE1;
    KREQUIRE_TRUE(usercopy::copy_to_task(child_task, TEST_VADDR, &replacement, sizeof(replacement)));
    KREQUIRE_NE(mapped_bytes(parent), nullptr);
    KREQUIRE_NE(mapped_bytes(child), nullptr);
    KEXPECT_EQ(mapped_bytes(parent)[0], 0x3C);
    KEXPECT_EQ(mapped_bytes(child)[0], replacement);
    KEXPECT_EQ(child_task.detach_pagemap_after_usercopy_quiescence(), child);

    destroy_test_pagemap(parent, "fork-parent");
    destroy_test_pagemap(child, "fork-child");
    KEXPECT_EQ(swap::swapoff_path(NAME), 0);
}

KTEST(AnonymousSwap, ProtectionCorruptionAndUnmapFailClosed) {
    numa::AllocationContext placement{};
    KREQUIRE_TRUE(capture_test_swap_context(placement));
    constexpr const char* NAME = "ktest-anon-swap-failure";
    auto storage = make_storage();
    KREQUIRE_NE(storage.get(), nullptr);
    KREQUIRE_TRUE(storage->valid());
    KREQUIRE_EQ(activate_storage(*storage, NAME), 0);

    auto* pagemap = virt::create_pagemap();
    KREQUIRE_NE(pagemap, nullptr);
    KREQUIRE_TRUE(map_pattern_page(pagemap, 0x71));
    KREQUIRE_TRUE(virt::selftest_anonymous_swap_pageout(pagemap, TEST_VADDR, placement));
    KEXPECT_TRUE(virt::protect_anonymous_swap_page(pagemap, TEST_VADDR, paging::PAGE_PRESENT | paging::PAGE_NX));
    KREQUIRE_TRUE(storage->corrupt_read(1, 31, 0x40));
    KEXPECT_FALSE(virt::selftest_anonymous_swap_pagein(pagemap, TEST_VADDR, placement));
    KEXPECT_EQ(virt::translate(pagemap, TEST_VADDR), virt::PADDR_INVALID);

    virt::unmap_page(pagemap, TEST_VADDR);
    destroy_test_pagemap(pagemap, "corruption-unmap");
    KEXPECT_EQ(swap::swapoff_path(NAME), 0);
}

KTEST(AnonymousSwap, GenericEnsureRefusesTrackedLazySwapWithoutZeroFill) {
    TrackedSwapFixture fixture("ktest-anon-swap-generic-refusal");
    KREQUIRE_TRUE(fixture.init());
    KEXPECT_FALSE(virt::ensure_user_page_mapped(&fixture.subject(), TEST_VADDR));
    fixture.expect_preserved();
    KEXPECT_FALSE(virt::ensure_user_page_writable(&fixture.subject(), TEST_VADDR));
    fixture.expect_preserved();
    KREQUIRE_TRUE(fixture.recover_pattern());
}

KTEST(AnonymousSwap, UsercopyRefusalPreservesTrackedLazySwapAndStoredBytes) {
    TrackedSwapFixture fixture("ktest-anon-swap-usercopy-refusal");
    KREQUIRE_TRUE(fixture.init());
    std::array<uint8_t, 64> output{};
    output.fill(0xC7);
    KEXPECT_FALSE(usercopy::copy_from_task(fixture.subject(), TEST_VADDR, output.data(), output.size()));
    for (auto value : output) KEXPECT_EQ(value, 0xC7U);
    fixture.expect_preserved();
    std::array<uint8_t, 64> input{};
    input.fill(0xE3);
    KEXPECT_FALSE(usercopy::copy_to_task(fixture.subject(), TEST_VADDR, input.data(), input.size()));
    fixture.expect_preserved();
    KREQUIRE_TRUE(fixture.recover_pattern());
    log::info("tracked-swap software refusal and actual root-pagein recovery only; no scheduled software page-in success credit");
}

KTEST(AnonymousSwap, InvalidNativePlacementPreservesStoredPatternForRetry) {
    TrackedSwapFixture fixture("ktest-anon-swap-placement-refusal");
    KREQUIRE_TRUE(fixture.init());
    numa::AllocationContext invalid{};
    KREQUIRE_TRUE(capture_test_swap_context(invalid));
    invalid.policy.mode = static_cast<numa::Mode>(255);
    KEXPECT_FALSE(fixture.pagein_with(invalid));
    fixture.expect_preserved();
    KREQUIRE_TRUE(fixture.recover_pattern());
}

KTEST(AnonymousSwap, CopiedBindPageinRestoresStoredBytesOnSelectedNode) {
    auto const* machine = ker::mod::acpi::numa::boot_topology();
    KREQUIRE_NE(machine, nullptr);
    for (numa::NodeId node = 0; node < machine->node_count; ++node) {
        if (machine->nodes[node].usable_bytes == 0) continue;
        TrackedSwapFixture fixture("ktest-anon-swap-bind-pagein");
        KREQUIRE_TRUE(fixture.init());
        numa::AllocationContext placement{};
        KREQUIRE_TRUE(capture_test_swap_context(placement));
        placement.policy = {numa::NodeMask{1} << node, numa::Mode::BIND};
        KREQUIRE_TRUE(fixture.recover_pattern(placement));
        KEXPECT_EQ(fixture.resident_node(), node);
    }
}
