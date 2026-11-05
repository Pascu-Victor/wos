#pragma once
#include <cstdint>
namespace ker::mod::mm::virt {
struct Range {
    uint64_t start, end;
};
auto kernel_mmio_page_flags() -> uint64_t;
void map_range_to_kernel_page_table(Range, uint64_t);
}  // namespace ker::mod::mm::virt
