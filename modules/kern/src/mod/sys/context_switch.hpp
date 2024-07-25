#pragma once

#include <mod/asm/cpu.hpp>

namespace ker::mod::sys::context_switch
{

    struct TaskRegisters {
        cpu::GPRegs regs;
        uint64_t ip;
        uint64_t rsp;
    };

    void schedCallback(void *handlerFunc);

}