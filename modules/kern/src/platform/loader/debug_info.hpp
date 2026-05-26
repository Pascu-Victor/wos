#pragma once

#include <extern/elf.h>

#include <defines/defines.hpp>
#include <platform/mm/virt.hpp>
#include <vector>

namespace ker::loader::debug {

struct DebugSection {
    const char* name;
    uint64_t vaddr;
    uint64_t paddr;
    uint64_t size;
    uint64_t file_offset;
    uint32_t type;
};

struct DebugSymbol {
    const char* name;
    uint64_t vaddr;
    uint64_t paddr;
    uint64_t size;
    uint8_t bind;
    uint8_t type;
    bool is_tls_offset;
    uint16_t shndx;
    uint64_t raw_value;  // original st_value
};

struct ProcessDebugInfo {
    uint64_t pid{};
    const char* name{};
    uint64_t base_address{};
    uint64_t entry_point{};
    std::vector<DebugSection> sections;
    std::vector<DebugSymbol> symbols;

    // ELF header information
    Elf64_Ehdr elf_header{};
    uint64_t elf_header_addr{};

    // Program headers
    Elf64_Phdr* program_headers{};
    uint64_t program_headers_addr{};
    uint16_t program_header_count{};

    // Section headers
    Elf64_Shdr* section_headers{};
    uint64_t section_headers_addr{};
    uint16_t section_header_count{};

    // String table
    const char* string_table{};
    uint64_t string_table_addr{};
    uint64_t string_table_size{};
};

// Global debug info registry
extern std::vector<ProcessDebugInfo> debug_registry;

// Functions to manage debug info
void register_process(uint64_t pid, const char* name, uint64_t base_addr, uint64_t entry_point);
void add_debug_section(uint64_t pid, const char* name, uint64_t vaddr, uint64_t paddr, uint64_t size, uint64_t file_offset, uint32_t type);
void add_debug_symbol(uint64_t pid, const char* name, uint64_t vaddr, uint64_t paddr, uint64_t size, uint8_t bind, uint8_t type,
                      bool is_tls_offset, uint16_t shndx, uint64_t raw_value);
void set_elf_headers(uint64_t pid, const Elf64_Ehdr& header, uint64_t header_addr);
void set_program_headers(uint64_t pid, Elf64_Phdr* phdrs, uint64_t phdrs_addr, uint16_t count);
void set_section_headers(uint64_t pid, Elf64_Shdr* shdrs, uint64_t shdrs_addr, uint16_t count);
void set_string_table(uint64_t pid, const char* strtab, uint64_t strtab_addr, uint64_t size);

ProcessDebugInfo* get_process_debug_info(uint64_t pid);
void print_debug_info(uint64_t pid);
DebugSymbol* get_process_symbol(uint64_t pid, const char* name);

// Cleanup function - removes all debug info for a process
void unregister_process(uint64_t pid);

}  // namespace ker::loader::debug
