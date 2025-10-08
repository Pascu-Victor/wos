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

void write(const char *str, uint64_t len) {
    for (size_t i = 0; i < len; i++) {
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
    // simple u64 -> decimal string converter
    int pos = 20;
    if (num == 0) {
        str[--pos] = '0';
    } else {
        while (num > 0 && pos > 0) {
            str[--pos] = '0' + (num % 10);
            num /= 10;
        }
    }
    // shift to start
    int len = 20 - pos;
    for (int i = 0; i < len; ++i) str[i] = str[pos + i];
    str[len] = '\0';
    write(str);
}

void writeHex(uint64_t num) {
    char str[17];
    str[16] = '\0';
    const char *hex = "0123456789abcdef";
    for (int i = 0; i < 16; ++i) {
        str[15 - i] = hex[num & 0xF];
        num >>= 4;
    }
    // trim leading zeros
    int start = 0;
    while (start < 15 && str[start] == '0') ++start;
    int len = 16 - start;
    for (int i = 0; i < len; ++i) str[i] = str[start + i];
    str[len] = '\0';
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
