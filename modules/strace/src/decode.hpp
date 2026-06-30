#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "common.hpp"

namespace wos::strace {

auto callnum_name(uint64_t callnum) -> std::string_view;
auto format_entry(uint64_t pid, const PendingSyscall& pending) -> std::string;
auto format_result(int64_t result) -> std::string;

}  // namespace wos::strace
