#include <limine.h>
#include <stddef.h>
#include <stdint.h>

#include <mod/gfx/fb.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gdt.hpp>
#include <platform/interrupt/interrupt.hpp>
#include <platform/mm/mm.hpp>
#include <std/drawing.hpp>
#include <std/hcf.hpp>
#include <std/mem.hpp>

__attribute__((used, section(".requests"))) static volatile LIMINE_BASE_REVISION(2);

__attribute__((used, section(".requests_start_marker"))) static volatile LIMINE_REQUESTS_START_MARKER;

__attribute__((used, section(".requests_end_marker"))) static volatile LIMINE_REQUESTS_END_MARKER;

using namespace ker::mod;

#define STACK_SIZE 0x400000  // 4 MiB

__attribute__((aligned(16))) static uint8_t stack[STACK_SIZE];

// Kernel entry point.
extern "C" void _start(void) {
    if (LIMINE_BASE_REVISION_SUPPORTED == 0) {
        hcf();
    }

    // Init the framebuffer.
    gfx::fb::init();
    // Init logging.
    dbg::init();
    dbg::log("Hi from WOs");

    // Init memory manager.
    ker::mod::mm::init();

    // Init gds.
    ker::mod::desc::gdt::initDescriptors((uint64_t)stack + sizeof((uint64_t)stack));

    // Init interrupts.
    ker::mod::interrupt::init();

    // Kernel should halt and catch fire if it reaches this point.
    hcf();
}
