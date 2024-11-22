#include <limine.h>

#include <defines/defines.hpp>
#include <mod/gfx/fb.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/boot/handover.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gdt.hpp>
#include <platform/interrupt/interrupt.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/mm/mm.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/smt/smt.hpp>
#include <std/drawing.hpp>
#include <std/hcf.hpp>
#include <std/mem.hpp>
#include <std/string.hpp>

__attribute__((used, section(".requests"))) static volatile LIMINE_BASE_REVISION(2);

__attribute__((used, section(".requests_start_marker"))) static volatile LIMINE_REQUESTS_START_MARKER;

__attribute__((used, section(".requests"))) static volatile limine_module_request kernelModuleRequest = {
    .id = LIMINE_MODULE_REQUEST,
    .revision = 1,
    .response = nullptr,
    .internal_module_count = 0,
    .internal_modules = nullptr,
};

__attribute__((used, section(".requests_end_marker"))) static volatile LIMINE_REQUESTS_END_MARKER;

using namespace ker::mod;

#define STACK_SIZE 0x100  // 8KB

__attribute__((aligned(16))) static uint64_t stack[STACK_SIZE];

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

    gfx::fb::mapFramebuffer();

    dbg::log("Framebuffer mapped");
    dbg::log("Pages mapped");

    // Init gds.
    ker::mod::desc::gdt::initDescriptors(stack + sizeof(stack));

    // Init kmalloc
    ker::mod::mm::dyn::kmalloc::init();

    // Init interrupts.
    ker::mod::interrupt::init();

    ker::mod::sched::init();

    boot::HandoverModules modules;

    if (kernelModuleRequest.response == nullptr) {
        dbg::log("Kernel module request failed");
        hcf();
    }

    if (kernelModuleRequest.response->module_count > 32) {
        dbg::log("Too many modules loaded by limine (%x/32)", kernelModuleRequest.response->module_count);
        hcf();
    }

    modules.count = kernelModuleRequest.response->module_count;

    for (size_t i = 0; i < modules.count; i++) {
        ker::mod::dbg::log("Module: %s", kernelModuleRequest.response->modules[i]->path);
        modules.modules[i].entry = kernelModuleRequest.response->modules[i]->address;
        modules.modules[i].size = kernelModuleRequest.response->modules[i]->size;
        modules.modules[i].cmdline = kernelModuleRequest.response->modules[i]->path;
    }

    // Init smt
    ker::mod::smt::init(modules);

    // Kernel should halt and catch fire if it reaches this point.
    hcf();
}
