#pragma once
#include <cstdint>
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/paging.hpp>
#include <string_view>
namespace ker::mod::mm::phys {
enum class PageInitialization : uint8_t { ZERO, FULL_OVERWRITE };
auto page_alloc_may_fail(PhysicalPageOwner, uint64_t, std::string_view) -> void*;
auto page_alloc_full_overwrite_page_may_fail(PhysicalPageOwner, std::string_view) -> void*;
auto page_alloc_with_reclaim_may_fail(PhysicalPageOwner, uint64_t, std::string_view) -> void*;
auto page_alloc_full_overwrite_page_with_reclaim_may_fail(PhysicalPageOwner, std::string_view) -> void*;
auto page_alloc_order0_span_in_range_may_fail(PhysicalPageOwner, uint64_t, uint64_t, uint32_t, uint64_t, PageInitialization) -> void*;
auto page_mark_kind(void*, PageKind) -> bool;
auto page_ref_dec(void*) -> uint32_t;
auto page_table_owner_transition_if_exclusive(void*, PhysicalPageOwner, PhysicalPageOwner) -> bool;
auto find_allocator_for_page(void*, PageAllocator*&, uint32_t&) -> bool;
}  // namespace ker::mod::mm::phys
