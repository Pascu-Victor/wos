#pragma once

#include <defines/defines.hpp>

__attribute__((noreturn)) extern "C" void _wOS_asm_enterUsermode(uint64_t rip, uint64_t rsp);
