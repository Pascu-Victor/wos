#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

#include "top/types.hpp"

namespace top {

auto read_passwd() -> std::unordered_map<uint32_t, std::string>;
auto make_snapshot(const Snapshot* previous, const std::unordered_map<uint32_t, std::string>& users) -> Snapshot;

}  // namespace top
