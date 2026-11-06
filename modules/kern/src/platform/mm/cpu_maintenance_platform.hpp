#pragma once

#include "cpu_maintenance.hpp"

namespace ker::mod::mm::cpu_maintenance::detail::platform {
// Actual definitions live in the native TU. Host fixtures link finite ports
// against the production protocol and real18b token registry; no runtime
// hardware/identity/owner factory or setter is exposed.
[[nodiscard]] auto configured_cpu_count() -> size_t;
[[nodiscard]] auto current_cpu(size_t& logical, uint32_t& apic) -> mmio_cache::Status;
[[nodiscard]] auto save_disable_interrupts() -> uint64_t;
void restore_interrupts(uint64_t saved_flags);
[[nodiscard]] auto current_root_is_kernel() -> bool;
// IRQ-excluded native NXE/WP observation for private MMIO requests only.
[[nodiscard]] auto private_protection_status() noexcept -> mmio_cache::Status;
// Caller holds IRQ exclusion and has just revalidated its real range. Returns
// actual current CR3 as diagnostics after executing the requested local action.
[[nodiscard]] auto execute_local(Phase phase) noexcept -> uint64_t;
void send_ipi(uint32_t target_apic, uint8_t actual_vector) noexcept;
[[nodiscard]] auto clock_now_ns(uint64_t& output) -> bool;
// Actual DAEMON/current/ACTIVE/IF1/preempt0/safe-context/shutdown-held checks.
[[nodiscard]] auto controller_context_ready() -> bool;
void controller_yield();
void spin_pause() noexcept;
}  // namespace ker::mod::mm::cpu_maintenance::detail::platform
