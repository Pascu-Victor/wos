#pragma once

#include <cstdint>

namespace httpd {

auto deadline_after_ms(int timeout_ms) -> int64_t;
auto remaining_ms_until(int64_t deadline_ms, int fallback_timeout_ms) -> int;
auto wait_for_child_timeout(int64_t pid, int32_t* status, int timeout_ms) -> bool;

}  // namespace httpd
