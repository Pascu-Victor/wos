#pragma once
#include <extern/elf.h>

#include <defines/defines.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/virt.hpp>
#include <std/string.hpp>

namespace ker::loader::elf {

typedef uint64_t Elf64Entry;

struct ElfFile {
    Elf64_Ehdr elfHead;         // ELF header
    Elf64_Phdr *pgHead;         // Program headers
    Elf64_Shdr *seHead;         // Section headers
    elf64_shdr *sctHeadStrTab;  // Section header string table
    uint8_t *base;              // Base address of the ELF file
};

Elf64Entry loadElf(ElfFile *elf, mod::mm::virt::PageTable *pagemap);

}  // namespace ker::loader::elf
