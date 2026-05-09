#include "mm.hpp"

#include <extern/limine.h>

#include <cstdint>
#include <platform/dbg/dbg.hpp>

#include "platform/mm/addr.hpp"
#include "platform/mm/phys.hpp"
#include "platform/mm/virt.hpp"

__attribute__((used, section(".requests"))) static volatile limine_memmap_request memmapRequest = {
    .id = LIMINE_MEMMAP_REQUEST_ID,
    .revision = 0,
    .response = nullptr,
};

__attribute__((used, section(".requests"))) static volatile limine_executable_file_request kernelFileRequest = {
    .id = LIMINE_EXECUTABLE_FILE_REQUEST_ID,
    .revision = 0,
    .response = nullptr,
};

__attribute__((used, section(".requests"))) static volatile limine_executable_address_request kernelAddressRequest = {
    .id = LIMINE_EXECUTABLE_ADDRESS_REQUEST_ID,
    .revision = 0,
    .response = nullptr,
};

__attribute__((used, section(".requests"))) static volatile limine_hhdm_request hhdmRequest = {
    .id = LIMINE_HHDM_REQUEST_ID,
    .revision = 0,
    .response = nullptr,
};

namespace ker::mod::mm {
void init() {
    addr::init(hhdmRequest.response);
    dbg::log("Memory manager initialized");
    phys::init(memmapRequest.response);
    dbg::log("Physical memory manager initialized");
    virt::init(memmapRequest.response, kernelFileRequest.response, kernelAddressRequest.response);
    dbg::log("Virtual memory manager initialized");
    virt::init_pagemap();
    dbg::log("Kernel page map initialized");
    // Set kernel CR3 for safe memset in pageAlloc when called from userspace context
    phys::set_kernel_cr3((uint64_t)addr::get_phys_pointer((uint64_t)virt::get_kernel_pagemap()));
    // Now initialize huge page zone after page map is ready
    phys::init_huge_page_zone_deferred();
    dbg::log("Huge page zone initialized");
    // Now that all HHDM is mapped, allow allocations from high memory
}
}  // namespace ker::mod::mm
