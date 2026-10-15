#pragma once
#include <cstdint>
namespace ker::mod::mm::addr {
auto get_phys_pointer(uint64_t) -> uint64_t*;
}
