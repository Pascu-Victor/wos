#include "kmem.hpp"

//ask limine for core count
// __attribute__((used, section(".requests")))
// static volatile limine_bootloader_info_request bootloader_info_request = {
//     .id = LIMINE_BOOTLOADER_INFO_REQUEST,
//     .revision = 0,
//     .response = nullptr,
// };

// __attribute__((used, section(".requests")))
// static volatile limine_efi_memmap_request efi_system_table_request = {
//     .id = LIMINE_EFI_SYSTEM_TABLE_REQUEST,
//     .revision = 0,
//     .response = nullptr,
// };
 
namespace ker::mod::mem
{
    limine_bootloader_info_response *bootloader_info;

    bool isInit = false;
    void init(void) {
        if (isInit) {
            return;
        }

        // desc::gdt::initDescriptors();

        isInit = true;
    }

}