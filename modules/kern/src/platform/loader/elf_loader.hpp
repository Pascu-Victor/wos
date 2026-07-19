#pragma once
#include <extern/elf.h>

#include <cstddef>
#include <cstdint>
#include <defines/defines.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/virt.hpp>
#include <util/smallvec.hpp>

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
    char interp_path[INTERP_PATH_MAX] = {};  // NOLINT: c-string ABI consumed by exec/task paths.
    bool has_interp = false;                 // true if PT_INTERP was found
};

struct ElfLazyLoadRange {
    uint64_t vaddr{};
    uint64_t size{};
    uint64_t prot{};
    uint64_t flags{};
    uint64_t file_offset{};
};

using ElfLazyLoadRangeVec = ker::util::SmallVec<ElfLazyLoadRange, 16>;

struct ElfLoadOptions {
    bool register_special_symbols = true;
    uint64_t base_address = 0;
    ElfLazyLoadRangeVec* lazy_file_ranges = nullptr;
};

// Exact, positional read used by file-backed ELF views. Implementations must
// either fill the complete destination range and return true, or return false.
// The loader validates offset + size against logical_size before invoking it.
using ElfReadAt = auto (*)(void* context, uint64_t offset, void* destination, size_t size) -> bool;

// A bounded ELF source whose relatively small metadata has already been read
// and validated by the owner. The pointed-to metadata and read_context must
// remain alive for the duration of load_elf(). Arrays contain e_phnum/e_shnum
// native Elf64 entries; section_names contains the complete shstrtab payload.
//
// contiguous_base is optional. Dynamic images do not need it. It is only used
// by the legacy static-relocation implementation, which needs random access to
// relocation and symbol-table contents not represented by this view.
struct ElfFileView {
    Elf64_Ehdr elf_header{};
    const Elf64_Phdr* program_headers{};
    const Elf64_Shdr* section_headers{};
    const char* section_names{};
    uint64_t section_names_size{};
    ElfReadAt read_at{};
    void* read_context{};
    uint64_t logical_size{};
    const uint8_t* contiguous_base{};
};

struct ElfFile {
    Elf64_Ehdr elf_head;                 // ELF header
    const Elf64_Phdr* pg_head;           // Program headers
    const Elf64_Shdr* se_head;           // Section headers
    const Elf64_Shdr* sct_head_str_tab;  // Section header string table
    const uint8_t* base;                 // Optional contiguous ELF file
    const char* section_names;           // Pre-parsed section-name table
    uint64_t section_names_size;
    ElfReadAt read_at;
    void* read_context;
    uint64_t logical_size;
    bool source_pointers_stable;
    uint64_t load_base;  // Load base address for PIE executables
    TlsModule tls_info;  // TLS information for this ELF
};

auto load_elf(ElfFile* elf, ker::mod::mm::virt::PageTable* pagemap, uint64_t pid, const char* process_name,
              bool register_special_symbols = true, uint64_t base_address = 0) -> ElfLoadResult;
auto load_elf(ElfFile* elf, ker::mod::mm::virt::PageTable* pagemap, uint64_t pid, const char* process_name, const ElfLoadOptions& options)
    -> ElfLoadResult;
auto load_elf(const ElfFileView& elf, ker::mod::mm::virt::PageTable* pagemap, uint64_t pid, const char* process_name,
              bool register_special_symbols = true, uint64_t base_address = 0) -> ElfLoadResult;
auto load_elf(const ElfFileView& elf, ker::mod::mm::virt::PageTable* pagemap, uint64_t pid, const char* process_name,
              const ElfLoadOptions& options) -> ElfLoadResult;

// Extract TLS information from ELF without fully loading it
auto extract_tls_info(void* elf_data) -> TlsModule;
auto extract_tls_info(const ElfFileView& elf) -> TlsModule;

// Remove the global getter - TLS info should be passed per-process
// TlsModule getTlsModule();

}  // namespace ker::loader::elf
