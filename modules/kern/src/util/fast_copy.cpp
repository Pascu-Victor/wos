#include "fast_copy.hpp"

#include <cstddef>
#include <cstdint>

namespace ker::util {
namespace {

inline void copy_scalar(uint8_t* dst, const uint8_t* src, size_t len) {
    for (size_t i = 0; i < len; ++i) {
        dst[i] = src[i];
    }
}

}  // namespace

void copy_fast(void* dst, const void* src, size_t len) {
    auto* d = static_cast<uint8_t*>(dst);
    const auto* s = static_cast<const uint8_t*>(src);

    copy_scalar(d, s, len);
}

}  // namespace ker::util
