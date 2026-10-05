#pragma once

#include <cstdint>

namespace wkictl {

auto route_name(uint32_t route) -> const char*;

auto run_forward(int argc, char** argv) -> int;
auto handle_vfs(int argc, char** argv) -> int;

}  // namespace wkictl
