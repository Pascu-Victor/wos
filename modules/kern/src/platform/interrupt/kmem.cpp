#include "kmem.hpp"

#include "extern/limine.h"

// ask limine for core count
//  __attribute__((used, section(".requests")))
//  static volatile limine_bootloader_info_request bootloader_info_request = {
//      .id = LIMINE_BOOTLOADER_INFO_REQUEST,
//      .revision = 0,
//      .response = nullptr,
//  };

// __attribute__((used, section(".requests")))
// static volatile limine_efi_memmap_request efi_system_table_request = {
//     .id = LIMINE_EFI_SYSTEM_TABLE_REQUEST,
//     .revision = 0,
//     .response = nullptr,
// };

namespace ker::mod::mem {
static limine_bootloader_info_response* bootloader_info;

static bool is_init = false;
void init() {
    if (is_init) {
        return;
    }

    // desc::gdt::initDescriptors();

    is_init = true;
}

}  // namespace ker::mod::mem
