#pragma once

#include <platform/mm/page_alloc.hpp>
#include <string_view>

namespace ker::mod::mm::phys {
auto can_wait_for_reclaim() -> bool;
auto page_alloc_may_fail(PhysicalPageOwner, uint64_t, std::string_view) -> void*;
void page_free(void*);
}  // namespace ker::mod::mm::phys
