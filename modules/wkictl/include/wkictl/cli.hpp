#pragma once

#include <cstddef>

namespace wkictl {

auto usage() -> int;
auto exec_command(char** argv) -> int;
auto read_trimmed_file(const char* path, char* out, std::size_t out_size) -> bool;

}  // namespace wkictl
