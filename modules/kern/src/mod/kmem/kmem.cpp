#include "kmem.hpp"

__attribute__((used, section(".requests")))
static volatile limine_bootloader_info_request bootloader_info_request = {
    .id = LIMINE_BOOTLOADER_INFO_REQUEST,
    .revision = 0,
    .response = nullptr,
};

__attribute__((used, section(".requests")))
static volatile limine_efi_memmap_request efi_system_table_request = {
    .id = LIMINE_EFI_SYSTEM_TABLE_REQUEST,
    .revision = 0,
    .response = nullptr,
};

namespace ker
{
    static volatile void* __memmap = efi_system_table_request.response->memmap;
    static volatile uint64_t __memmap_size = efi_system_table_request.response->memmap_size;
    static void* __brk = nullptr;

    void kmem_init(void) {
        __page_table = (volatile page_table_entry*)mmap_read(0, 4);
        __brk = (void*)mmap_read(0, 5);
    }

    uint64_t mmap_read(uint64_t addr, uint64_t offset) {
        uint64_t* memmap = (uint64_t*)__memmap;
        uint64_t memmap_size = __memmap_size;

        uint64_t entry_size = sizeof(uint64_t) * 5;

        for (uint64_t i = 0; i < memmap_size; i++) {
            uint64_t* entry = &memmap[i * entry_size / sizeof(uint64_t)];

            if (entry[0] == addr) {
                return entry[offset];
            }
        }

        return 0;
    }

    uint64_t allocate_frame(void) {
        uint64_t* memmap = (uint64_t*)__memmap;
        uint64_t memmap_size = __memmap_size;

        uint64_t entry_size = sizeof(uint64_t) * NUM_MMAP_ENTRIES;

        for (uint64_t i = 0; i < memmap_size; i++) {
            uint64_t* entry = &memmap[i * entry_size / sizeof(uint64_t)];

            if (entry[1] == 0) {
                entry[1] = 1;
                return entry[0];
            }
        }

        return 0;
    }

    void free_frame(uint64_t addr) {
        uint64_t* memmap = (uint64_t*)__memmap;
        uint64_t memmap_size = __memmap_size;

        uint64_t entry_size = sizeof(uint64_t) * NUM_MMAP_ENTRIES;

        for (uint64_t i = 0; i < memmap_size; i++) {
            uint64_t* entry = &memmap[i * entry_size / sizeof(uint64_t)];

            if (entry[0] == addr) {
                entry[1] = 0;
                return;
            }
        }
    }

    // void* brk(void* addr) {
    //     static void* brk = __brk;

    //     if (addr == nullptr) {
    //         return brk;
    //     }

    //     brk = addr;
    //     return brk;
    // }

    // void* sbrk(uint64_t increment) {
    //     static void* brk = __brk;

    //     if (brk == nullptr) {
    //         brk = (void*)mmap_read(0, 4);
    //     }

    //     void* old_brk = brk;

    //     if (increment == 0) {
    //         return old_brk;
    //     }

    //     if (increment > 0) {
    //         brk = (void*)((uint64_t)brk + increment);
    //     } else {
    //         brk = (void*)((uint64_t)brk - increment);
    //     }

    //     return old_brk;
    // }

}