#pragma once

#include <cstdint>

namespace ker::mod::tsc {

void init();
uint64_t get_hz();
uint64_t get_ns();
uint64_t ticks_to_ns(uint64_t delta);

}  // namespace ker::mod::tsc
