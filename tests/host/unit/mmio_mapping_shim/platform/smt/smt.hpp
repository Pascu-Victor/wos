#pragma once
#include <cstdint>
namespace ker::mod::smt {
[[nodiscard]] auto get_core_count() -> uint64_t;
[[nodiscard]] auto get_apic_id_for_cpu(uint64_t logical_cpu) -> uint32_t;
void halt_other_cores();
}  // namespace ker::mod::smt
