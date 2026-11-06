#pragma once

#include "mmio_cache_observation.hpp"

namespace ker::mod::mm::mmio_cache::detail::platform {

// The production definitions are in a separate low-level TU. Host tests link
// finite ports against the actual registry/collector/model, without adding a
// production setter or a way to adopt caller-provided CPU observations.
class CpuIo {
   public:
    [[nodiscard]] auto cpuid(uint32_t leaf, uint32_t subleaf) -> Cpuid;
    [[nodiscard]] auto read_msr(uint32_t index, uint64_t& output) -> bool;
    [[nodiscard]] auto read_cr0() -> uint64_t;
    [[nodiscard]] auto read_cr4() -> uint64_t;
    [[nodiscard]] auto local_idt_ready() -> bool;
};

[[nodiscard]] auto save_disable_interrupts() -> uint64_t;
void restore_interrupts(uint64_t saved_flags);
[[nodiscard]] auto configured_cpu_count() -> size_t;
[[nodiscard]] auto verify_boot_cpu(size_t expected_logical, uint32_t& actual_apic) -> Status;
[[nodiscard]] auto current_cpu(size_t& logical, uint32_t& actual_apic) -> Status;

}  // namespace ker::mod::mm::mmio_cache::detail::platform
