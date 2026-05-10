#pragma once

#include <extern/elf.h>

#include <defines/defines.hpp>

namespace ker::loader::debug {

// Structure to hold debug information that GDB can access
struct GdbDebugInfo {
    // Magic number for identification
    uint32_t magic;

    // Process information
    uint64_t pid;
    char name[64];  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays): debugger-visible ABI buffer.
    uint64_t base_address;
    uint64_t entry_point;

    // ELF header location
    uint64_t elf_header_addr;

    // Section information
    uint16_t section_count;
    uint64_t section_headers_addr;
    uint64_t string_table_addr;
    uint64_t string_table_size;

    // Program header information
    uint16_t program_header_count;
    uint64_t program_headers_addr;

    // Debug section addresses
    uint64_t debug_info_addr;
    uint64_t debug_info_size;
    uint64_t debug_line_addr;
    uint64_t debug_line_size;
    uint64_t debug_str_addr;
    uint64_t debug_str_size;

    // Next process in chain
    uint64_t next_process_addr;
} __attribute__((packed));

static_assert(sizeof(GdbDebugInfo) == 192, "GdbDebugInfo debugger ABI size changed");

// Global debug info chain
extern GdbDebugInfo* gdb_debug_info_chain;

// Functions to manage GDB debug info
void init_gdb_debug_info();
void add_gdb_debug_info(uint64_t pid, const char* name, uint64_t base_addr, uint64_t entry_point);
void update_gdb_debug_section(uint64_t pid, const char* section_name, uint64_t addr, uint64_t size);
void finalize_gdb_debug_info(uint64_t pid);
void remove_gdb_debug_info(uint64_t pid);

// Function to be called by GDB to get debug info
extern "C" GdbDebugInfo* get_gdb_debug_info();

}  // namespace ker::loader::debug
