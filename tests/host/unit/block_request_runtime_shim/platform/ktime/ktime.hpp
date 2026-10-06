#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace ker::mod::time {
inline std::atomic<uint64_t> block_test_now_us{1000};
inline thread_local size_t block_test_clock_reads{};
inline auto get_us() -> uint64_t {
    ++block_test_clock_reads;
    return block_test_now_us.load(std::memory_order_relaxed);
}
}  // namespace ker::mod::time
