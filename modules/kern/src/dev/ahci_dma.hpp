#pragma once

#include <cstdint>

namespace ker::dev::ahci::dma_rules {
constexpr uint32_t ST = 1U;
constexpr uint32_t FRE = 1U << 4;
constexpr uint32_t FR = 1U << 14;
constexpr uint32_t CR = 1U << 15;
constexpr uint32_t ERROR_INTERRUPTS = 0x7d000000U;
enum class Completion : uint8_t { pending, success, error };

// Shared with host tests. Backend bounds each wait. AHCI 1.3.1 section 10.3
// forbids clearing FRE while ST/CR is set. Failure never authorizes DMA reuse.
template <typename Port>
[[nodiscard]] auto stop(Port& port) -> bool {
    port.clear_command(ST);
    if (!port.wait_clear(ST | CR)) return false;
    port.clear_command(FRE);
    return port.wait_clear(FRE | FR);
}
[[nodiscard]] constexpr auto completion(uint32_t ci, uint32_t sact, uint32_t is, uint32_t tfd, uint32_t slot) -> Completion {
    if (slot >= 32 || (is & ERROR_INTERRUPTS) != 0) return Completion::error;
    if (((ci | sact) & (1U << slot)) != 0) return Completion::pending;
    return (tfd & 0x21U) != 0 ? Completion::error : Completion::success;  // ERR or device fault.
}
}  // namespace ker::dev::ahci::dma_rules
