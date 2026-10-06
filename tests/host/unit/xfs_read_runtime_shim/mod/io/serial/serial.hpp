#pragma once
namespace ker::mod::io::serial {
inline bool test_panic{};
inline auto is_panic_mode() -> bool { return test_panic; }
}  // namespace ker::mod::io::serial
