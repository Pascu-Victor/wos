#pragma once
#include <cstdint>
namespace ker::mod::cpu {
struct CpuidContext {
    int eax{}, ebx{}, ecx{}, edx{}, function{};
};
void cpuid(CpuidContext* context);
}  // namespace ker::mod::cpu
