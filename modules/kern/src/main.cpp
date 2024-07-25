#include <stdint.h>
#include <stddef.h>
#include <limine.h>

#include <util/funcs.hpp>
#include <util/mem.hpp>
#include <mod/gfx/fb.hpp>
#include <mod/io/serial/serial.hpp>
#include <mod/dbg/dbg.hpp>
#include <mod/mm/mm.hpp>
#include <mod/interrupt/gdt.hpp>
#include <mod/interrupt/interrupt.hpp>

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

    dbg::log("Hello serial!");
    dbg::log("Hello serial!");
    dbg::log("Hello serial!");
    dbg::log("Hello serial!");
    dbg::log("Hello serial!");
    dbg::log("Hello serial!");
    dbg::log("Hello serial!");
    dbg::log("Hello serial!");

    // asm volatile("int $0x20");

    // Kernel should halt and catch fire if it reaches this point.
    hcf();
}
