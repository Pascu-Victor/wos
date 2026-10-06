#pragma once
#include <cstdint>
namespace ker::mod::mm::addr {
inline uint64_t test_hhdm_offset{4096};
inline auto get_hhdm_offset() -> uint64_t { return test_hhdm_offset; }
}  // namespace ker::mod::mm::addr
