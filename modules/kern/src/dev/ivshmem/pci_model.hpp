#pragma once

#include <cstddef>
#include <cstdint>

namespace ker::dev::ivshmem::model {
enum class Mode : uint8_t { unsupported, plain, msix };
constexpr auto mode(uint8_t revision, bool has_msix) -> Mode {
    return revision == 1 ? (has_msix ? Mode::msix : Mode::plain) : Mode::unsupported;
}
constexpr auto extent(uint64_t size, uint64_t offset, uint64_t length) -> bool { return offset <= size && length <= size - offset; }
constexpr auto bar(uint64_t address, uint64_t size, uint64_t minimum) -> bool {
    return address != 0 && size != 0 && size >= minimum && (size & (size - 1)) == 0 && (address & (size - 1)) == 0 &&
           size <= UINT64_MAX - 4095 && address <= UINT64_MAX - 4095 - size;
}
constexpr auto msix_layout(uint8_t capability, uint16_t control, uint32_t table, uint32_t pba, uint64_t bar_size) -> bool {
    if (capability < 0x40 || capability > 0xf4 || (capability & 3U) != 0 || (table & 7U) != 1 || (pba & 7U) != 1) return false;
    uint64_t const ENTRIES = (control & 0x7ffU) + 1;
    uint64_t const TABLE = table & ~uint32_t{7}, PBA = pba & ~uint32_t{7};
    uint64_t const TABLE_BYTES = ENTRIES * 16, PBA_BYTES = ((ENTRIES + 63) / 64) * 8;
    return extent(bar_size, TABLE, TABLE_BYTES) && extent(bar_size, PBA, PBA_BYTES) &&
           (TABLE + TABLE_BYTES <= PBA || PBA + PBA_BYTES <= TABLE);
}
constexpr auto doorbell(uint16_t peer, uint16_t vector = 0) -> uint32_t { return (uint32_t{peer} << 16U) | vector; }

// Caller owns stable private data before activation. No hardware interrupt
// source may be enabled until its registered handler owns the vector.
template <typename Backend>
auto activate(Mode selected, Backend& backend) -> bool {
    if (selected == Mode::plain) return backend.disable_bus_master();
    if (selected != Mode::msix || !backend.register_irq()) return false;
    if (!backend.enable_msix() || !backend.enable_bus_master()) {
        backend.unwind_irq();
        return false;
    }
    return true;
}

enum class Retirement : uint8_t { stopped, callbacks_busy, interrupts_unproven };
template <typename Backend>
auto retire(Backend& backend) -> Retirement {
    backend.close_callbacks();
    if (!backend.drain_callbacks()) {
        backend.inhibit_hardware();
        return Retirement::callbacks_busy;
    }
    backend.unpublish();
    return backend.stop_interrupts() ? Retirement::stopped : Retirement::interrupts_unproven;
}
}  // namespace ker::dev::ivshmem::model
