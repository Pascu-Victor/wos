// WOS Kernel Entry Point
// The main function is minimal - all initialization is handled by the init system.

#include <extern/limine.h>

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

void callGlobalDestructors() {  // NOLINT
    for (auto* dtor = static_cast<void (**)()>(__fini_array_end); dtor > static_cast<void (**)()>(__fini_array_start);) {
        --dtor;
        (*dtor)();
    }
}

}  // namespace

namespace ker::init::fns {

// Called from the init registry after IDT (and KASan shadow paging) are ready.
// With WOS_KASAN, instrumented global constructors access shadow memory; the
// page-fault handler must be live first so shadow pages can be demand-allocated.
void global_ctors_init() {  // NOLINT
    for (auto* ctor = static_cast<void (**)()>(__preinit_array_start); ctor < static_cast<void (**)()>(__preinit_array_end); ++ctor) {
        (*ctor)();
    }
    for (auto* ctor = static_cast<void (**)()>(__init_array_start); ctor < static_cast<void (**)()>(__init_array_end); ++ctor) {
        (*ctor)();
    }
}

}  // namespace ker::init::fns

__attribute__((used, section(".requests"))) volatile uint64_t limine_base_revision[] = LIMINE_BASE_REVISION(6);  // NOLINT

// Kernel entry point.
extern "C" [[noreturn]] void _start(void) {  // NOLINT
    // Check limine protocol support
    if (LIMINE_BASE_REVISION_SUPPORTED(limine_base_revision) == 0) {
        while (true) {
            asm volatile("hlt");
        }
    }

    // NOTE: Do NOT call callGlobalConstructors() here.
    // With WOS_KASAN enabled, instrumented global constructors access shadow
    // memory, which requires the page-fault handler (IDT) to demand-page shadow
    // pages.  Global ctors are deferred to after IDT init via the init registry.
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
