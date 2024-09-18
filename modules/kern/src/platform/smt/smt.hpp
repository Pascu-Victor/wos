#pragma once
#include <limine.h>

#include <defines/defines.hpp>
#include <platform/asm/cpu.hpp>

namespace ker::mod::smt {
constexpr static uint64_t SMT_MAX_CPUS = 256;

uint64_t getCoreCount(void);
const limine_smp_info& getCpu(uint64_t number);

void init(void);

limine_smp_info* currentCpu();
}  // namespace ker::mod::smt
