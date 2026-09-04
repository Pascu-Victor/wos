#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <platform/mm/addr.hpp>
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
namespace usercopy = ker::mod::sys::usercopy;

constexpr uint64_t TEST_VADDR = 0x5A000000ULL;
constexpr uint64_t SWAP_PAGES = 16;
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

}  // namespace

KTEST(AnonymousSwap, RealPageoutPageinPreservesDataAndAccounting) {
    constexpr const char* NAME = "ktest-anon-swap-roundtrip";
    auto storage = make_storage();
    KREQUIRE_NE(storage.get(), nullptr);
    KREQUIRE_TRUE(storage->valid());
    KREQUIRE_EQ(activate_storage(*storage, NAME), 0);

    auto* pagemap = virt::create_pagemap();
    KREQUIRE_NE(pagemap, nullptr);
    KREQUIRE_TRUE(map_pattern_page(pagemap, 0xA6));
    KREQUIRE_TRUE(virt::selftest_anonymous_swap_pageout(pagemap, TEST_VADDR));
    KEXPECT_TRUE(virt::selftest_anonymous_swap_is_swapped(pagemap, TEST_VADDR));
    auto memory = virt::collect_user_memory_stats(pagemap);
    KEXPECT_EQ(memory.resident_pages, 0U);
    KEXPECT_EQ(memory.swapped_pages, 1U);

    KREQUIRE_TRUE(virt::selftest_anonymous_swap_pagein(pagemap, TEST_VADDR));
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
    KREQUIRE_TRUE(virt::selftest_anonymous_swap_pageout(parent, TEST_VADDR));
    KREQUIRE_TRUE(virt::deep_copy_user_pagemap_cow(parent, child));
    KEXPECT_TRUE(virt::selftest_anonymous_swap_is_swapped(parent, TEST_VADDR));
    KEXPECT_TRUE(virt::selftest_anonymous_swap_is_swapped(child, TEST_VADDR));
    KREQUIRE_TRUE(virt::selftest_anonymous_swap_pagein(parent, TEST_VADDR));
    KREQUIRE_TRUE(virt::selftest_anonymous_swap_pagein(child, TEST_VADDR));
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
    constexpr const char* NAME = "ktest-anon-swap-failure";
    auto storage = make_storage();
    KREQUIRE_NE(storage.get(), nullptr);
    KREQUIRE_TRUE(storage->valid());
    KREQUIRE_EQ(activate_storage(*storage, NAME), 0);

    auto* pagemap = virt::create_pagemap();
    KREQUIRE_NE(pagemap, nullptr);
    KREQUIRE_TRUE(map_pattern_page(pagemap, 0x71));
    KREQUIRE_TRUE(virt::selftest_anonymous_swap_pageout(pagemap, TEST_VADDR));
    KEXPECT_TRUE(virt::protect_anonymous_swap_page(pagemap, TEST_VADDR, paging::PAGE_PRESENT | paging::PAGE_NX));
    KREQUIRE_TRUE(storage->corrupt_read(1, 31, 0x40));
    KEXPECT_FALSE(virt::selftest_anonymous_swap_pagein(pagemap, TEST_VADDR));
    KEXPECT_EQ(virt::translate(pagemap, TEST_VADDR), virt::PADDR_INVALID);

    virt::unmap_page(pagemap, TEST_VADDR);
    destroy_test_pagemap(pagemap, "corruption-unmap");
    KEXPECT_EQ(swap::swapoff_path(NAME), 0);
}
