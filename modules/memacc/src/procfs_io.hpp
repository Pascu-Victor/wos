#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>

namespace memacc {

constexpr std::string_view MEMACC_ROOT = "/proc/memacc";
constexpr size_t READ_CHUNK_CAPACITY = 4096;
constexpr size_t MEMACC_READ_LIMIT = 262144;

auto memacc_path(std::string_view file) -> std::string;
auto read_file(std::string_view path, size_t max_bytes = MEMACC_READ_LIMIT) -> std::optional<std::string>;
auto write_file(std::string_view path, std::string_view text) -> bool;

}  // namespace memacc
