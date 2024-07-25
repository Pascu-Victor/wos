#pragma once
// Halt and catch fire function.
static inline void hcf(void) noexcept {
    asm ("cli");
    for (;;) {
        asm ("hlt");
    }
}

void inline static wos_nop(...) {}

#define WOS_NOP wos_nop