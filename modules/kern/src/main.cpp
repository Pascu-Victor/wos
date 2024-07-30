#include <stdint.h>
#include <stddef.h>
#include <limine.h>

#include <std/hcf.hpp>
#include <util/mem.hpp>
#include <mod/gfx/fb.hpp>
#include <mod/io/serial/serial.hpp>
#include <mod/dbg/dbg.hpp>
#include <mod/mm/mm.hpp>
#include <mod/interrupt/gdt.hpp>
#include <mod/interrupt/interrupt.hpp>
#include <util/drawing.hpp>

__attribute__((used, section(".requests")))
static volatile LIMINE_BASE_REVISION(2);

__attribute__((used, section(".requests_start_marker")))
static volatile LIMINE_REQUESTS_START_MARKER;

__attribute__((used, section(".requests_end_marker")))
static volatile LIMINE_REQUESTS_END_MARKER;

using namespace ker::mod;



#define STACK_SIZE 0x400000 // 4 MiB

__attribute__((aligned(16)))
static uint8_t stack[STACK_SIZE];

// Kernel entry point.
extern "C"
void _start(void) {
    if (LIMINE_BASE_REVISION_SUPPORTED == 0) {
        hcf();
    }

    // Init the framebuffer.
    gfx::fb::init();
    // Init logging.
    dbg::init();

    // Init memory manager.
    ker::mod::mm::init();
    
    // Init gds.
    ker::mod::desc::gdt::initDescriptors((uint64_t)stack + sizeof((uint64_t)stack));

    // Init interrupts.
    ker::mod::interrupt::init();

    gfx::fb::drawString(0, 0, "Hi from");
    gfx::fb::drawChar(8, 0, 'W', gfx::fb::TermColors::BRIGHT_RED);
    gfx::fb::drawChar(9, 0, 'O', gfx::fb::TermColors::BRIGHT_ORANGE);
    gfx::fb::drawChar(10, 0, 's', gfx::fb::TermColors::BRIGHT_YELLOW);

    std::gfx::ColorRGBA wColor = gfx::fb::TermColors::BRIGHT_RED;
    std::gfx::ColorRGBA oColor = gfx::fb::TermColors::BRIGHT_ORANGE;
    std::gfx::ColorRGBA sColor = gfx::fb::TermColors::BRIGHT_YELLOW;
    std::gfx::ColorHSVA wHsv = std::gfx::rgbaToHsva(wColor);
    std::gfx::ColorHSVA oHsv = std::gfx::rgbaToHsva(oColor);
    std::gfx::ColorHSVA sHsv = std::gfx::rgbaToHsva(sColor);

    while (true)
    {

        wHsv = std::gfx::shiftHue(wHsv, 1);
        oHsv = std::gfx::shiftHue(oHsv, 1);
        sHsv = std::gfx::shiftHue(sHsv, 1);

        wColor = std::gfx::hsvaToRgba(wHsv);
        oColor = std::gfx::hsvaToRgba(oHsv);
        sColor = std::gfx::hsvaToRgba(sHsv);

        gfx::fb::drawChar(8,  0, 'W', wColor.toPacked());
        gfx::fb::drawChar(9,  0, 'O', oColor.toPacked());
        gfx::fb::drawChar(10, 0, 's', sColor.toPacked());

        ker::mod::time::sleep(33);
    }

    // Kernel should halt and catch fire if it reaches this point.
    hcf();
}
