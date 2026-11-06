#pragma once
#include <cstdint>
#include <platform/mm/page_alloc.hpp>
#include <string_view>
namespace ker::mod::mm::phys {
auto can_wait_for_reclaim() -> bool;
auto page_alloc_may_fail(PhysicalPageOwner, uint64_t size, std::string_view name) -> void*;
auto page_ref_try_inc(void*) -> bool;
auto page_ref_dec(void*) -> uint32_t;
auto page_ref_get(void*) -> uint32_t;
}  // namespace ker::mod::mm::phys
