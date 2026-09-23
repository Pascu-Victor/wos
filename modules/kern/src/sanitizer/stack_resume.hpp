#pragma once

#include <cstddef>
#include <cstdint>

namespace ker::mod::kasan {
struct StackResumeRange {
    uintptr_t begin{};
    size_t size{};
};

// The kernel has no ABI red zone: all resumable locals are at/above RSP.
// Round down the end so even an unaligned RSP preserves its live granule.
constexpr auto stack_resume_range(uintptr_t top, size_t capacity, uintptr_t rsp) -> StackResumeRange {
    if (capacity == 0 || top < capacity || (top & 7U) != 0 || (capacity & 7U) != 0) return {};
    auto const BEGIN = top - capacity;
    if (rsp <= BEGIN || rsp > top) return {};
    return {BEGIN, (rsp & ~uintptr_t{7}) - BEGIN};
}
}  // namespace ker::mod::kasan
