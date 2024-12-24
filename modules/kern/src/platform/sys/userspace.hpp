#pragma once

#include <defines/defines.hpp>

extern "C" __attribute__((noreturn)) void _wOS_asm_enterUsermode(uint64_t rip, uint64_t rsp);
