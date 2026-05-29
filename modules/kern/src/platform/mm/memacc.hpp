#pragma once

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
    uint64_t other_pages;
    uint64_t rw_pages;
    uint64_t rx_pages;
    uint64_t ro_pages;
};

auto collect_user_memory_breakdown(paging::PageTable* page_table) -> UserMemoryBreakdown;

}  // namespace ker::mod::mm::memacc
