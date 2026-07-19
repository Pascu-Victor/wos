#include "mm.hpp"

#include <extern/limine.h>

#include <cstdint>
#include <platform/dbg/dbg.hpp>

#include "platform/mm/addr.hpp"
#include "platform/mm/phys.hpp"
#include "platform/mm/virt.hpp"

namespace {

__attribute__((used, section(".requests"))) volatile limine_memmap_request memmap_request = {
    .id = LIMINE_MEMMAP_REQUEST_ID,
    .revision = 0,
    .response = nullptr,
};

__attribute__((used, section(".requests"))) volatile limine_executable_file_request kernel_file_request = {
    .id = LIMINE_EXECUTABLE_FILE_REQUEST_ID,
    .revision = 0,
    .response = nullptr,
};

__attribute__((used, section(".requests"))) volatile limine_executable_address_request kernel_address_request = {
    .id = LIMINE_EXECUTABLE_ADDRESS_REQUEST_ID,
    .revision = 0,
    .response = nullptr,
};

__attribute__((used, section(".requests"))) volatile limine_hhdm_request hhdm_request = {
    .id = LIMINE_HHDM_REQUEST_ID,
    .revision = 0,
    .response = nullptr,
};

}  // namespace

namespace ker::mod::mm {

namespace {
using log = ker::mod::dbg::logger<"mm">;
}  // namespace

void init() {
    addr::init(hhdm_request.response);
    log::info("memory manager initialized");
    phys::init(memmap_request.response);
    log::info("physical memory manager initialized");
    virt::init(memmap_request.response, kernel_file_request.response, kernel_address_request.response);
    log::info("virtual memory manager initialized");
    virt::init_pagemap();
    log::info("kernel page map initialized");
    // Set kernel CR3 for safe memset in pageAlloc when called from userspace context
    auto const KERNEL_PAGEMAP_VADDR = reinterpret_cast<uint64_t>(virt::get_kernel_pagemap());
    phys::set_kernel_cr3(reinterpret_cast<uint64_t>(addr::get_phys_pointer(KERNEL_PAGEMAP_VADDR)));
    // Kernel stacks must remain available after small allocations fragment the
    // general buddy zones. Reserve their fixed-size backing before GDT, SMP,
    // and task creation begin.
    phys::init_kernel_stack_pool();
    // Now initialize huge page zone after page map is ready
    phys::init_huge_page_zone_deferred();
    log::info("huge page zone initialized");
    // Now that all HHDM is mapped, allow allocations from high memory
}
}  // namespace ker::mod::mm
