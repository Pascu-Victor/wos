#pragma once

#include <cstddef>

namespace ker::util {

// Fast kernel copy helper. Avoids SIMD/FPU state so it is safe in socket hot paths
// while user processes freely use AVX.
void copy_fast(void* dst, const void* src, size_t len);

}  // namespace ker::util
