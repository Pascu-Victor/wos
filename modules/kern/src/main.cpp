#include <stdint.h>
#include <stddef.h>
#include <limine.h>

#include <util/funcs.hpp>
#include <util/mem.hpp>
#include <mod/gfx/fb.hpp>

__attribute__((used, section(".requests")))
static volatile LIMINE_BASE_REVISION(2);

__attribute__((used, section(".requests_start_marker")))
static volatile LIMINE_REQUESTS_START_MARKER;

__attribute__((used, section(".requests_end_marker")))
static volatile LIMINE_REQUESTS_END_MARKER;


// Kernel entry point.
extern "C"
void _start(void) {
    if (LIMINE_BASE_REVISION_SUPPORTED == 0) {
        hcf();
    }

    // Init the framebuffer.
    gfx::fb::init();

    gfx::fb::drawString(0, 0, "Hi from");
    gfx::fb::drawChar(8, 0, 'W', gfx::fb::TermColors::BRIGHT_RED);
    gfx::fb::drawChar(9, 0, 'O', gfx::fb::TermColors::BRIGHT_ORANGE);
    gfx::fb::drawChar(10, 0, 's', gfx::fb::TermColors::BRIGHT_YELLOW);

    // Kernel should halt and catch fire if it reaches this point.
    hcf();
}
