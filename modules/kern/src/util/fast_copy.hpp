#pragma once

#include <cstddef>

namespace ker::util {

// Fast kernel copy helper.
// Uses scalar copy for small sizes and SSE with explicit FPU/SIMD context
// save/restore for larger sizes.
void copy_fast(void* dst, const void* src, size_t len);

}  // namespace ker::util
