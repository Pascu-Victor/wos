#include "pic.hpp"

namespace ker::mod::pic {
    static bool picEnabled = true;
    void eoi(int isr_nr)
    {
        if (isr_nr > 40)
            outb(0xA0, 0x20);
        outb(0x20, 0x20);
    }

    bool enabled(void)
    {
        return picEnabled;
    }

    void disable(void)
    {
        outb(0xA1, 0xFF);
        outb(0x21, 0xFF);
        picEnabled = false;
    }

    void remap(void)
    {
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
}