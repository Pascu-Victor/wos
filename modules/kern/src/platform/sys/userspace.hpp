#pragma once

#include <defines/defines.hpp>

extern "C" __attribute__((noreturn)) void wos_asm_enter_usermode(uint64_t rip, uint64_t rsp);
extern "C" __attribute__((noreturn)) void wos_asm_enter_exec_owner(uint64_t rip, uint64_t rsp, uint64_t kernel_stack_top);
extern "C" __attribute__((noreturn)) void wos_asm_park_exec_owner(uint64_t kernel_stack_top);
