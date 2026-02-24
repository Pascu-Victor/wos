// Limine bootloader request structures
// These are placed in special sections for the bootloader to process

#include <limine.h>

#include <cstdint>

namespace ker::init {

__attribute__((used, section(".requests_start_marker"))) volatile LIMINE_REQUESTS_START_MARKER;  // NOLINT

__attribute__((used, section(".requests"))) volatile limine_module_request g_kernel_module_request = {
    .id = LIMINE_MODULE_REQUEST,
    .revision = 1,
    .response = nullptr,
    .internal_module_count = 0,
    .internal_modules = nullptr,
};

__attribute__((used, section(".requests_end_marker"))) volatile LIMINE_REQUESTS_END_MARKER;  // NOLINT

// Captured kernel stack pointer (set during early init)
uint64_t g_kernel_rsp = 0;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Accessor functions
auto get_kernel_module_request() -> volatile limine_module_request& { return g_kernel_module_request; }

auto get_kernel_rsp() -> uint64_t { return g_kernel_rsp; }

void set_kernel_rsp(uint64_t rsp) { g_kernel_rsp = rsp; }

}  // namespace ker::init
