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
    uint64_t tls_base;    // Base address of TLS area (not used for template)
    uint64_t tls_size;    // Size of TLS area (from PT_TLS segment)
    uint64_t tcb_offset;  // Offset to TCB within TLS
};

using Elf64Entry = uint64_t;

struct ElfLoadResult {
    uint64_t entry_point{};              // Program entry point
    uint64_t program_header_addr{};      // Virtual address of program headers (for AT_PHDR)
    uint64_t elf_header_addr{};          // Virtual address of ELF header (for AT_EHDR)
    uint16_t program_header_count{};     // Number of program headers (for AT_PHNUM)
    uint16_t program_header_ent_size{};  // Size of each program header entry (for AT_PHENT)

    // PT_INTERP fields — set when the ELF requests a dynamic linker
    static constexpr unsigned INTERP_PATH_MAX = 256;
    char interp_path[INTERP_PATH_MAX] = {};  // e.g. "/lib/ld.so"
    bool has_interp = false;                 // true if PT_INTERP was found
};

struct ElfFile {
    Elf64_Ehdr elf_head;           // ELF header
    Elf64_Phdr* pg_head;           // Program headers
    Elf64_Shdr* se_head;           // Section headers
    Elf64_Shdr* sct_head_str_tab;  // Section header string table
    uint8_t* base;                 // Base address of the ELF file
    uint64_t load_base;            // Load base address for PIE executables
    TlsModule tls_info;            // TLS information for this ELF
};

auto load_elf(ElfFile* elf, ker::mod::mm::virt::PageTable* pagemap, uint64_t pid, const char* process_name,
              bool register_special_symbols = true, uint64_t base_address = 0) -> ElfLoadResult;

// Extract TLS information from ELF without fully loading it
auto extract_tls_info(void* elf_data) -> TlsModule;

// Remove the global getter - TLS info should be passed per-process
// TlsModule getTlsModule();

}  // namespace ker::loader::elf
