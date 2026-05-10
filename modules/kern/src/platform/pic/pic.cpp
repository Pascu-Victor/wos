#include "pic.hpp"

#include "mod/io/port/port.hpp"

namespace ker::mod::pic {

namespace {

bool pic_enabled = true;

}  // namespace

void eoi(int isr_nr) {
    if (isr_nr > 40) {
        outb(0xA0, 0x20);
    }
    outb(0x20, 0x20);
}

bool enabled() { return pic_enabled; }

void disable() {
    outb(0xA1, 0xFF);
    outb(0x21, 0xFF);
    pic_enabled = false;
}

void remap() {
    outb(0x20, 0x11);
    outb(0xA0, 0x11);
    io_wait();
    outb(0x21, 0x20);
    outb(0xA1, 0x28);
    io_wait();
    outb(0x21, 0x04);
    outb(0xA1, 0x02);
    io_wait();
    outb(0x21, 0x01);
    outb(0xA1, 0x01);
    io_wait();
    outb(0x21, 0x00);
    outb(0xA1, 0x00);
}
}  // namespace ker::mod::pic
