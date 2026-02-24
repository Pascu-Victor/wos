// WOS Kernel Entry Point
// The main function is minimal - all initialization is handled by the init system.

#include <limine.h>

#include <platform/init/init_executor.hpp>

#include "platform/init/limine_requests.hpp"

// Linker-provided symbols for init/fini arrays
extern "C" {
extern void (*__preinit_array_start[])();  // NOLINT
extern void (*__preinit_array_end[])();    // NOLINT
extern void (*__init_array_start[])();     // NOLINT
extern void (*__init_array_end[])();       // NOLINT
extern void (*__fini_array_start[])();     // NOLINT
extern void (*__fini_array_end[])();       // NOLINT
}

namespace {

void callGlobalConstructors() {  // NOLINT
    for (auto* ctor = static_cast<void (**)()>(__preinit_array_start); ctor < static_cast<void (**)()>(__preinit_array_end); ++ctor) {
        (*ctor)();
    }

    for (auto* ctor = static_cast<void (**)()>(__init_array_start); ctor < static_cast<void (**)()>(__init_array_end); ++ctor) {
        (*ctor)();
    }
}

void callGlobalDestructors() {  // NOLINT
    for (auto* dtor = static_cast<void (**)()>(__fini_array_end); dtor > static_cast<void (**)()>(__fini_array_start);) {
        --dtor;
        (*dtor)();
    }
}

}  // namespace
__attribute__((used, section(".requests"))) volatile LIMINE_BASE_REVISION(3);  // NOLINT

// Kernel entry point.
extern "C" [[noreturn]] void _start(void) {  // NOLINT
    // Check limine protocol support
    if (LIMINE_BASE_REVISION_SUPPORTED == 0) {
        while (true) {
            asm volatile("hlt");
        }
    }

    // Initialize C++ runtime
    callGlobalConstructors();
    uint64_t rsp = 0;
    asm volatile("mov %%rsp, %0" : "=r"(rsp));
    ker::init::set_kernel_rsp(rsp);
    // Run all kernel initialization phases (never returns)
    ker::init::InitExecutor::run_all();

    // Unreachable - run_all() never returns
    callGlobalDestructors();
    while (true) {
        asm volatile("hlt");
    }
}
