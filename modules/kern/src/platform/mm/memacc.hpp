#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <platform/mm/paging.hpp>

namespace ker::mod::mm::memacc {

struct UserMemoryBreakdown {
    uint64_t virtual_pages;
    uint64_t resident_pages;
    uint64_t shared_pages;
    uint64_t page_table_pages;
    uint64_t code_pages;
    uint64_t heap_pages;
    uint64_t mmap_pages;
    uint64_t stack_pages;
    uint64_t low_address_pages;
    uint64_t high_runtime_pages;
    uint64_t rw_pages;
    uint64_t rx_pages;
    uint64_t ro_pages;
};

struct UserImageRange {
    uint64_t start{};
    uint64_t end{};
};

struct UserMemoryLayout {
    static constexpr size_t MAX_IMAGE_RANGES = 32;

    std::array<UserImageRange, MAX_IMAGE_RANGES> images{};
    size_t image_count{};
};

auto collect_user_memory_breakdown(paging::PageTable* page_table, const UserMemoryLayout& layout) -> UserMemoryBreakdown;

}  // namespace ker::mod::mm::memacc
