#pragma once
#include <cstdint>
namespace ker::mod::smt {
auto get_core_count() -> uint64_t;
auto get_apic_id_for_cpu(uint64_t) -> uint32_t;
}  // namespace ker::mod::smt
