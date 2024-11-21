#pragma once
// Halt and catch fire function.
__attribute__((noreturn)) static inline void hcf(void) noexcept {
    asm("cli");
    for (;;) {
        asm("hlt");
    }
}
