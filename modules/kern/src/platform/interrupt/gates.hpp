#pragma once

#include <defines/defines.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/acpi/apic/apic.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/interrupt/idt.hpp>

namespace ker::mod::gates {
struct interruptFrame {
    // all registers stored in stack as well maybe usefull in the future
    uint64_t intNum;
    uint64_t errCode;
    uint64_t rip;
    uint64_t cs;
    uint64_t flags;
    uint64_t rsp;
    uint64_t ss;
} __attribute__((packed));

enum : uint64_t {
    IRQ0 = 32,
    IRQ1 = 33,
    IRQ2 = 34,
    IRQ3 = 35,
    IRQ4 = 36,
    IRQ5 = 37,
    IRQ6 = 38,
    IRQ7 = 39,
    IRQ8 = 40,
    IRQ9 = 41,
    IRQ10 = 42,
    IRQ11 = 43,
    IRQ12 = 44,
    IRQ13 = 45,
    IRQ14 = 46,
    IRQ15 = 47
};

typedef void (*interruptHandler_t)(cpu::GPRegs gpr, interruptFrame frame);

extern "C" {
void iterrupt_handler(cpu::GPRegs gpr, interruptFrame frame);
}

#define isIrq(vector) (vector >= IRQ0 && vector <= IRQ15)

void setInterruptHandler(uint8_t intNum, interruptHandler_t handler);
void removeInterruptHandler(uint8_t intNum);
bool isInterruptHandlerSet(uint8_t intNum);

// Context-based IRQ handler (for device drivers with private_data)
using irq_handler_fn = void (*)(uint8_t vector, void* private_data);
auto requestIrq(uint8_t vector, irq_handler_fn handler, void* data, const char* name) -> int;
void freeIrq(uint8_t vector);
auto allocateVector() -> uint8_t;  // find free vector >= 48

}  // namespace ker::mod::gates
