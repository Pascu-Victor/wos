#pragma once

#include <cstdint>
#include <defines/defines.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/acpi/apic/apic.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/interrupt/idt.hpp>

namespace ker::mod::gates {
struct InterruptFrame {
    // all registers stored in stack as well maybe usefull in the future
    uint64_t int_num;
    uint64_t err_code;
    uint64_t rip;
    uint64_t cs;
    uint64_t flags;
    uint64_t rsp;
    uint64_t ss;
} __attribute__((packed));

inline constexpr uint8_t IRQ0 = 32;
inline constexpr uint8_t IRQ1 = 33;
inline constexpr uint8_t IRQ2 = 34;
inline constexpr uint8_t IRQ3 = 35;
inline constexpr uint8_t IRQ4 = 36;
inline constexpr uint8_t IRQ5 = 37;
inline constexpr uint8_t IRQ6 = 38;
inline constexpr uint8_t IRQ7 = 39;
inline constexpr uint8_t IRQ8 = 40;
inline constexpr uint8_t IRQ9 = 41;
inline constexpr uint8_t IRQ10 = 42;
inline constexpr uint8_t IRQ11 = 43;
inline constexpr uint8_t IRQ12 = 44;
inline constexpr uint8_t IRQ13 = 45;
inline constexpr uint8_t IRQ14 = 46;
inline constexpr uint8_t IRQ15 = 47;

using interruptHandler_t = void (*)(cpu::GPRegs gpr, InterruptFrame frame);

extern "C" {
void iterrupt_handler(cpu::GPRegs gpr, InterruptFrame frame);
}

[[nodiscard]] constexpr auto is_irq(uint64_t vector) -> bool { return vector >= IRQ0 && vector <= IRQ15; }

void set_interrupt_handler(uint8_t int_num, interruptHandler_t handler);
void remove_interrupt_handler(uint8_t int_num);
bool is_interrupt_handler_set(uint8_t int_num);

// Context-based IRQ handler (for device drivers with private_data)
using irq_handler_fn = void (*)(uint8_t vector, void* private_data);
auto request_irq(uint8_t vector, irq_handler_fn handler, void* data, const char* name) -> int;
void free_irq(uint8_t vector);
auto allocate_vector() -> uint8_t;  // find free vector >= 48

}  // namespace ker::mod::gates
