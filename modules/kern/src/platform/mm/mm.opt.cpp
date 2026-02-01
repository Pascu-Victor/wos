#include "mm.hpp"

#include <platform/dbg/dbg.hpp>

__attribute__((used, section(".requests"))) static volatile limine_memmap_request memmapRequest = {
    .id = LIMINE_MEMMAP_REQUEST,
    .revision = 0,
    .response = nullptr,
};

__attribute__((used, section(".requests"))) static volatile limine_kernel_file_request kernelFileRequest = {
    .id = LIMINE_KERNEL_FILE_REQUEST,
    .revision = 0,
    .response = nullptr,
};

__attribute__((used, section(".requests"))) static volatile limine_kernel_address_request kernelAddressRequest = {
    .id = LIMINE_KERNEL_ADDRESS_REQUEST,
    .revision = 0,
    .response = nullptr,
};

__attribute__((used, section(".requests"))) static volatile limine_hhdm_request hhdmRequest = {
    .id = LIMINE_HHDM_REQUEST,
    .revision = 0,
    .response = nullptr,
};

namespace ker::mod::mm {
void init(void) {
    addr::init(hhdmRequest.response);
    dbg::log("Memory manager initialized\n");
    phys::init(memmapRequest.response);
    dbg::log("Physical memory manager initialized\n");
    virt::init(memmapRequest.response, kernelFileRequest.response, kernelAddressRequest.response);
    dbg::log("Virtual memory manager initialized\n");
    virt::initPagemap();
    dbg::log("Kernel page map initialized\n");
    // Set kernel CR3 for safe memset in pageAlloc when called from userspace context
    phys::setKernelCr3((uint64_t)addr::getPhysPointer((uint64_t)virt::getKernelPagemap()));
    // Now initialize huge page zone after page map is ready
    phys::initHugePageZoneDeferred();
    dbg::log("Huge page zone initialized\n");
    // Now that all HHDM is mapped, allow allocations from high memory
}
}  // namespace ker::mod::mm
