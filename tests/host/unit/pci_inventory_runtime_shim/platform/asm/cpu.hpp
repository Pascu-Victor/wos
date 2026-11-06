#pragma once
namespace ker::mod::cpu {
struct CpuidContext {
    int eax, ebx, ecx, edx, function;
};
void cpuid(CpuidContext*);
}  // namespace ker::mod::cpu
