#pragma once

#include <defines/defines.hpp>
#include <mod/io/serial/serial.hpp>
#include <mod/interrupt/idt.hpp>
#include <mod/acpi/apic/apic.hpp>

namespace ker::mod::gates {
    struct interruptFrame
    {
        // all registers stored in stack as well maybe usefull in the future
        uint64_t intNum;
        uint64_t errCode;
        uint64_t rip;
        uint64_t cs;
        uint64_t flags;
        uint64_t rsp;
        uint64_t ss;
    };

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

    typedef void (*interruptHandler_t)(interruptFrame *frame);

    extern "C" {
        void iterrupt_handler (interruptFrame *frame);
    }

    #define isIrq(vector) (vector >= IRQ0 && vector <= IRQ15)

    void setInterruptHandler(uint8_t intNum, interruptHandler_t handler);
    void removeInterruptHandler(uint8_t intNum);
    bool isInterruptHandlerSet(uint8_t intNum);


}
