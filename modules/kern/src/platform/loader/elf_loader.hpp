#pragma once
#include <extern/elf.h>

#include <cstdint>
#include <defines/defines.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/virt.hpp>

namespace ker::loader::elf {

// TLS module information
struct TlsModule {
    uint64_t tlsBase;    // Base address of TLS area (not used for template)
    uint64_t tlsSize;    // Size of TLS area (from PT_TLS segment)
    uint64_t tcbOffset;  // Offset to TCB within TLS
};

using Elf64Entry = uint64_t;

struct ElfLoadResult {
    uint64_t entryPoint;         // Program entry point
    uint64_t programHeaderAddr;  // Virtual address of program headers (for AT_PHDR)
    uint64_t elfHeaderAddr;      // Virtual address of ELF header (for AT_EHDR)
};

struct ElfFile {
    Elf64_Ehdr elfHead;         // ELF header
    Elf64_Phdr* pgHead;         // Program headers
    Elf64_Shdr* seHead;         // Section headers
    Elf64_Shdr* sctHeadStrTab;  // Section header string table
    uint8_t* base;              // Base address of the ELF file
    uint64_t loadBase;          // Load base address for PIE executables
    TlsModule tlsInfo;          // TLS information for this ELF
};

auto loadElf(ElfFile* elf, ker::mod::mm::virt::PageTable* pagemap, uint64_t pid, const char* processName,
             bool registerSpecialSymbols = true) -> ElfLoadResult;

// Extract TLS information from ELF without fully loading it
auto extractTlsInfo(void* elfData) -> TlsModule;

// Remove the global getter - TLS info should be passed per-process
// TlsModule getTlsModule();

}  // namespace ker::loader::elf
