#pragma once

#include <cstdint>
#include <string_view>

#include "types.hpp"

namespace memacc {

auto parse_u64_arg(std::string_view text) -> uint64_t;
auto parse_u64_arg_strict(std::string_view text, uint64_t& out) -> bool;
auto parse_options(int argc, char** argv, int start) -> Options;

}  // namespace memacc
