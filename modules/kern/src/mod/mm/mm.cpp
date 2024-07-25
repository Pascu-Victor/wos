#include "mm.hpp"

__attribute__((used, section(".requests")))
static volatile limine_memmap_request memmapRequest = {
    .id = LIMINE_MEMMAP_REQUEST,
    .revision = 0,
    .response = nullptr,
};

__attribute__((used, section(".requests")))
static volatile limine_kernel_file_request kernelFileRequest = {
    .id = LIMINE_KERNEL_FILE_REQUEST,
    .revision = 0,
    .response = nullptr,
};

__attribute__((used, section(".requests")))
static volatile limine_kernel_address_request kernelAddressRequest = {
    .id = LIMINE_KERNEL_ADDRESS_REQUEST,
    .revision = 0,
    .response = nullptr,
};

__attribute__((used, section(".requests")))
static volatile limine_hhdm_request hhdmRequest = {
    .id = LIMINE_HHDM_REQUEST,
    .revision = 0,
    .response = nullptr,
};

namespace ker::mod::mm {
    void init(void) {
        addr::init(hhdmRequest.response);
        phys::init(memmapRequest.response);
        virt::init(
            memmapRequest.response,
            kernelFileRequest.response,
            kernelAddressRequest.response
        );
    }
}   // namespace ker::mod::mm