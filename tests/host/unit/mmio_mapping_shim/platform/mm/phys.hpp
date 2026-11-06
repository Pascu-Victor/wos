#pragma once
#include <platform/mm/page_alloc.hpp>
#include <string_view>
namespace ker::mod::mm::phys {
[[nodiscard]] auto can_wait_for_reclaim() -> bool;
[[nodiscard]] auto page_alloc_may_fail(PhysicalPageOwner owner, uint64_t bytes, std::string_view label) -> void*;
void page_free(void* page);
}  // namespace ker::mod::mm::phys
