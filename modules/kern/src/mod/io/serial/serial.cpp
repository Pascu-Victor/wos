#include "serial.hpp"

namespace ker::mod::io {
namespace serial {
bool isInit = false;
void init(void) {
    if (isInit) {
        return;
    }
    outb(0x3F8 + 1, 0x00);  // Disable all interrupts
    outb(0x3F8 + 3, 0x80);  // Enable DLAB (set baud rate divisor)
    outb(0x3F8 + 0, 0x02);  // Set divisor to 2 (lo byte) 38400 baud
    outb(0x3F8 + 1, 0x00);  //                  (hi byte)
    outb(0x3F8 + 3, 0x03);  // 8 bits, no parity, one stop bit
    outb(0x3F8 + 2, 0xC7);  // Enable FIFO, clear them, with 14-byte threshold
    outb(0x3F8 + 4, 0x0B);  // IRQs enabled, RTS/DSR set
    isInit = true;
}

void write(const char *str) {
    for (size_t i = 0; str[i] != '\0'; i++) {
        while ((inb(0x3F8 + 5) & 0x20) == 0);
        outb(0x3F8, str[i]);
    }
}

void write(const char c) {
    while ((inb(0x3F8 + 5) & 0x20) == 0);
    outb(0x3F8, c);
}

void write(uint64_t num) {
    char str[21];
    str[20] = '\0';
    std::u64toa(num, str);
    write(str);
}

void writeHex(uint64_t num) {
    char str[17];
    str[16] = '\0';
    std::u64toh(num, str);
    write(str);
}

void writeBin(uint64_t num) {
    char str[65];
    str[64] = '\0';
    for (uint64_t i = 64; i > 0; i--) {
        str[64 - i] = (num & (1ULL << (i - 1))) ? '1' : '0';
    }
    write(str);
}
}  // namespace serial
}  // namespace ker::mod::io
