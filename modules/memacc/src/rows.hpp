#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "types.hpp"

namespace memacc {

auto parse_rows(std::string_view text) -> std::vector<Row>;
auto read_rows(std::string_view file) -> std::vector<Row>;
auto first_record(const std::vector<Row>& rows, std::string_view name) -> const Row*;
auto get_string(const Row& row, std::string_view key) -> std::string;
auto get_u64(const Row& row, std::string_view key) -> uint64_t;

}  // namespace memacc
