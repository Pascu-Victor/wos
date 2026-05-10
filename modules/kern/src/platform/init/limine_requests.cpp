// Limine bootloader request structures
// These are placed in special sections for the bootloader to process

#include "limine_requests.hpp"

#include <extern/limine.h>

#include <cstdint>

namespace ker::init {

namespace {

constexpr uint64_t KERNEL_MODULE_REQUEST_REVISION = 1;
constexpr uint64_t KERNEL_CMDLINE_REQUEST_REVISION = 0;

// Limine scans these linker sections by raw storage address, so these arrays intentionally remain ABI storage.
__attribute__((used, section(".requests_start_marker"))) volatile uint64_t limine_requests_start_marker[] =  // NOLINT
    LIMINE_REQUESTS_START_MARKER;                                                                            // NOLINT

__attribute__((used, section(".requests"))) volatile limine_module_request g_kernel_module_request = {
    // NOLINT
    .id = LIMINE_MODULE_REQUEST_ID, .revision = KERNEL_MODULE_REQUEST_REVISION, .response = nullptr, .internal_module_count = 0,
    .internal_modules = nullptr,
};

__attribute__((used, section(".requests"))) volatile limine_executable_cmdline_request g_kernel_cmdline_request = {
    // NOLINT
    .id = LIMINE_EXECUTABLE_CMDLINE_REQUEST_ID,
    .revision = KERNEL_CMDLINE_REQUEST_REVISION,
    .response = nullptr,
};

__attribute__((used, section(".requests_end_marker"))) volatile uint64_t limine_requests_end_marker[] =  // NOLINT
    LIMINE_REQUESTS_END_MARKER;                                                                          // NOLINT

}  // namespace

// Captured kernel stack pointer (set during early init)
uint64_t g_kernel_rsp = 0;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables, misc-use-internal-linkage)

// Accessor functions
[[nodiscard]] auto get_kernel_module_request() -> volatile limine_module_request& { return g_kernel_module_request; }

[[nodiscard]] auto get_kernel_rsp() -> uint64_t { return g_kernel_rsp; }

void set_kernel_rsp(uint64_t rsp) { g_kernel_rsp = rsp; }

[[nodiscard]] auto get_kernel_cmdline() -> const char* {
    if (g_kernel_cmdline_request.response == nullptr) {
        return "";
    }
    if (g_kernel_cmdline_request.response->cmdline == nullptr) {
        return "";
    }
    return g_kernel_cmdline_request.response->cmdline;
}

}  // namespace ker::init
