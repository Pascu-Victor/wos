#pragma once

#include <cstdint>
#include <string_view>
#include <vector>

namespace top {

auto parse_u64(std::string_view text, uint64_t& out) -> bool;
auto parse_i64(std::string_view text, int64_t& out) -> bool;
auto split_ws(std::string_view text) -> std::vector<std::string_view>;
auto parse_double(std::string_view text) -> double;

}  // namespace top
