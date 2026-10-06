#pragma once

#include <atomic>

namespace ker::mod::io::serial {
inline std::atomic<bool> block_test_panic_mode{};
inline auto is_panic_mode() -> bool { return block_test_panic_mode.load(std::memory_order_acquire); }
}  // namespace ker::mod::io::serial
