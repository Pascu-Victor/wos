#pragma once

#include <defines/defines.hpp>

extern "C" __attribute__((noreturn)) void wos_asm_enter_usermode(uint64_t rip, uint64_t rsp);
