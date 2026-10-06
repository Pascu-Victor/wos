#pragma once
#include <atomic>
#include <cstdint>

namespace ker::mod::mm::addr {
inline std::atomic<uint64_t> block_test_hhdm_offset{0};
inline auto get_hhdm_offset() -> uint64_t { return block_test_hhdm_offset.load(std::memory_order_relaxed); }
}  // namespace ker::mod::mm::addr
