#pragma once

#include <vector>

#include "types.hpp"

namespace memacc {

auto load_procs() -> std::vector<ProcRow>;
auto filtered_procs(const Options& opt) -> std::vector<ProcRow>;
void print_proc_table(const std::vector<ProcRow>& rows);

}  // namespace memacc
