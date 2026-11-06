#pragma once

#include "mmio_cache_model.hpp"

namespace ker::mod::mm::mmio_cache::detail {

struct Cpuid {
    uint32_t eax{};
    uint32_t ebx{};
    uint32_t ecx{};
    uint32_t edx{};
};

struct RawCpu {
    cache_arch::RawMtrrs mtrr{};
    Cpuid basic{};
    Cpuid features{};
    Cpuid structured{};
    Cpuid extended{};
    Cpuid long_mode{};
    Cpuid address_width{};
    Cpuid encryption{};
    uint64_t cr0{};
    uint64_t cr4{};
    uint64_t pat{};
    Register tme_activate{};
    AmdInput amd{};
    IntelProtectedInput intel_protected{};
    AmdProtectedInput amd_protected{};
    uint32_t failed_msr{};
};

// CpuIo supplies cpuid(leaf, subleaf), read_msr(index, output), read_cr0(),
// read_cr4(), and local_idt_ready(). Production uses actual local instructions
// and the narrowly fault-contained reader. Host ports are finite and explicit.
// Failure may retain partial diagnostic observations; it never mints permission.
template <typename CpuIo>
[[nodiscard]] auto collect_registers(CpuIo& io, RawCpu& output) -> Status;

// Repeat exactly the observed field set, including inactive masks but excluding
// dormant bases. Any activation/value/control drift refuses; do not refresh it.
template <typename CpuIo>
[[nodiscard]] auto revalidate_registers(CpuIo& io, const RawCpu& expected, uint32_t& failed_msr) -> Status;

}  // namespace ker::mod::mm::mmio_cache::detail

#include "mmio_cache_observation.inc"
