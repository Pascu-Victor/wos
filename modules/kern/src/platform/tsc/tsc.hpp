#pragma once

#include <cstdint>

namespace ker::mod::tsc {

void     init();
uint64_t getHz();
uint64_t getNs();
uint64_t ticksToNs(uint64_t delta);

}  // namespace ker::mod::tsc
