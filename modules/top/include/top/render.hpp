#pragma once

#include <string>

#include "top/types.hpp"

namespace top {

auto render(const Snapshot& snap, const Snapshot* previous, int row_offset, int col_offset, bool interactive) -> std::string;

}  // namespace top
