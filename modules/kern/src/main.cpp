// WOS Kernel Entry Point
// The main function is minimal - all initialization is handled by the init system.

#include <extern/limine.h>

#include <cstdint>
#include <platform/init/init_executor.hpp>

#include "platform/init/limine_requests.hpp"

// Linker-provided symbols for init/fini arrays
extern "C" {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,readability-identifier-naming): Linker init-array ABI.
extern void (*__preinit_array_start[])();
// NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,readability-identifier-naming): Linker init-array ABI.
extern void (*__preinit_array_end[])();
// NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,readability-identifier-naming): Linker init-array ABI.
extern void (*__init_array_start[])();
// NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,readability-identifier-naming): Linker init-array ABI.
extern void (*__init_array_end[])();
// NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,readability-identifier-naming): Linker fini-array ABI.
extern void (*__fini_array_start[])();
// NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,readability-identifier-naming): Linker fini-array ABI.
extern void (*__fini_array_end[])();
}

namespace {

// Compiler-generated TU constructors/destructors in __init_array/__fini_array
// are not annotated with KCFI type hashes by Clang's KCFI pass (they are
// emitted as raw assembly stubs, not typed C++ functions).  Calling them
// through void(*)() pointers with KCFI checking enabled therefore always
// triggers a KCFI violation.  These are trusted kernel-internal functions so
// CFI protection on these call sites provides no security value.
[[clang::no_sanitize("kcfi")]]
void call_global_destructors() {
    for (auto* dtor = static_cast<void (**)()>(__fini_array_end); dtor > static_cast<void (**)()>(__fini_array_start);) {
        --dtor;
        // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound): Linker provides the half-open fini-array range.
        (*dtor)();
    }
}

}  // namespace

namespace ker::init::fns {

// Called from the init registry after IDT (and KASan shadow paging) are ready.
// With WOS_KASAN, instrumented global constructors access shadow memory; the
// page-fault handler must be live first so shadow pages can be demand-allocated.
//
// [[clang::no_sanitize("kcfi")]]: compiler-generated _GLOBAL__sub_I_*.cpp TU
// constructors in __init_array are not annotated with KCFI type hashes (Clang
// does not emit hash prefixes for auto-generated ELF init-array stubs).
// Calling them via void(*)() pointers with KCFI enabled would always trap.
// These are closed, trusted kernel constructors so no CFI protection is needed.
[[clang::no_sanitize("kcfi")]]
void global_ctors_init() {
    for (auto* ctor = static_cast<void (**)()>(__preinit_array_start); ctor < static_cast<void (**)()>(__preinit_array_end); ++ctor) {
        (*ctor)();
    }
    for (auto* ctor = static_cast<void (**)()>(__init_array_start); ctor < static_cast<void (**)()>(__init_array_end); ++ctor) {
        (*ctor)();
    }
}

}  // namespace ker::init::fns

// NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,misc-use-internal-linkage): Limine request ABI.
__attribute__((used, section(".requests"))) volatile uint64_t limine_base_revision[] = LIMINE_BASE_REVISION(6);

// Kernel entry point.
extern "C" [[noreturn]] void _start(void) {  // NOLINT(readability-identifier-naming): Linker entry-point ABI.
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
    // NOLINTNEXTLINE(misc-const-correctness)
    uint64_t rsp = 0;
    asm volatile("mov %%rsp, %0" : "=r"(rsp));
    ker::init::set_kernel_rsp(rsp);
    // Run all kernel initialization phases (never returns)
    ker::init::InitExecutor::run_all();

    // Unreachable - run_all() never returns
    call_global_destructors();
    while (true) {
        asm volatile("hlt");
    }
}
