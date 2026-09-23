#pragma once

#include <cstdint>

namespace ker::dev::usb::xhci_terminal {
enum class State : uint8_t { active, retiring, released, retained };
enum class Result : uint8_t { released, lifecycle_busy, readers_busy, class_busy, hardware_unproven, release_failed };

// Lifecycle mutation must stop before traversing slots. Closing endpoint
// publication and cancelling idle requests then lets transfer readers drain.
// A failed software drain forbids even inspecting mutable DMA owner records.
template <typename Backend>
auto retire(Backend& backend) -> Result {
    backend.close_lifecycle();
    if (!backend.drain_lifecycle()) {
        backend.close_io();
        (void)backend.disable_bus_master();
        return Result::lifecycle_busy;
    }
    backend.close_io();
    bool const CLASSES_CLOSED = backend.close_io_and_slots();
    bool const READERS = backend.drain_io();
    bool const CLASSES = CLASSES_CLOSED && backend.drain_classes();
    bool const MASKED = backend.mask_interrupts();
    bool const HALTED = backend.halt();
    bool const BME_OFF = backend.disable_bus_master();
    if (MASKED) backend.drain_interrupts();
    if (!READERS) return Result::readers_busy;
    if (!CLASSES) return Result::class_busy;
    if (!MASKED || !HALTED || !BME_OFF) {
        backend.quarantine();
        return Result::hardware_unproven;
    }
    return backend.release() ? Result::released : Result::release_failed;
}
}  // namespace ker::dev::usb::xhci_terminal
