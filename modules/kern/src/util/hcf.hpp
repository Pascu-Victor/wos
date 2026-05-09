#pragma once
// Halt and catch fire function.
__attribute__((noreturn)) static inline void hcf() noexcept {
    asm("cli");
    for (;;) {
        asm("hlt");
    }
}
