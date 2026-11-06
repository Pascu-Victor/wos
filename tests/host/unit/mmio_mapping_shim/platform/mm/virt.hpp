#pragma once
#include <cstdint>
namespace ker::mod::mm::virt {
struct Range {
    uint64_t start{}, end{};
};
[[nodiscard]] auto kernel_mmio_page_flags() -> uint64_t;
void map_range_to_kernel_page_table(Range range, uint64_t flags);
}  // namespace ker::mod::mm::virt
