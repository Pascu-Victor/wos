#include "elf_loader.hpp"

#include <extern/elf.h>

// #define ELF_DEBUG  // Enable ELF loading debug output

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include "debug_info.hpp"
#include "mod/io/serial/serial.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/phys.hpp"
#include "platform/mm/virt.hpp"
#include "util/hcf.hpp"
// TLS relocation support
namespace {

// Define relocation types that we need to handle
constexpr uint64_t R_X86_64_NONE = 0;
constexpr uint64_t R_X86_64_64 = 1;
constexpr uint64_t R_X86_64_PC32 = 2;
constexpr uint64_t R_X86_64_GOT32 = 3;
constexpr uint64_t R_X86_64_PLT32 = 4;
constexpr uint64_t R_X86_64_COPY = 5;
constexpr uint64_t R_X86_64_GLOB_DAT = 6;
constexpr uint64_t R_X86_64_JUMP_SLOT = 7;
constexpr uint64_t R_X86_64_RELATIVE = 8;
constexpr uint64_t R_X86_64_GOTPCREL = 9;
constexpr uint64_t R_X86_64_32 = 10;
constexpr uint64_t R_X86_64_32S = 11;
constexpr uint64_t R_X86_64_16 = 12;
constexpr uint64_t R_X86_64_PC16 = 13;
constexpr uint64_t R_X86_64_8 = 14;
constexpr uint64_t R_X86_64_PC8 = 15;
constexpr uint64_t R_X86_64_DTPMOD64 = 16;
constexpr uint64_t R_X86_64_DTPOFF64 = 17;
constexpr uint64_t R_X86_64_TPOFF64 = 18;
constexpr uint64_t R_X86_64_TLSGD = 19;
constexpr uint64_t R_X86_64_TLSLD = 20;
}  // namespace

#ifndef SHF_TLS
constexpr uint64_t SHF_TLS = 0x400;
#endif

namespace ker::loader::elf {
namespace {

auto header_is_valid(const Elf64_Ehdr& ehdr) -> bool {
    return ehdr.e_ident[EI_CLASS] == ELFCLASS64                  // 64-bit
           && ehdr.e_ident[EI_OSABI] == ELFOSABI_NONE            // System V
           && (ehdr.e_type == ET_EXEC || ehdr.e_type == ET_DYN)  // Executable or PIE
           && ehdr.e_ident[EI_MAG0] == ELFMAG0                   // Magic 0
           && ehdr.e_ident[EI_MAG1] == ELFMAG1                   // Magic 1
           && ehdr.e_ident[EI_MAG2] == ELFMAG2                   // Magic 2
           && ehdr.e_ident[EI_MAG3] == ELFMAG3;                  // Magic 3
}

auto parse_elf(uint8_t* base) -> ElfFile {
    ElfFile elf{};
    elf.base = base;

    // Validate base pointer before dereferencing
    if (base == nullptr) {
        mod::dbg::log("ERROR: parseElf called with null base pointer");
        return elf;  // Return empty ElfFile
    }

    // Copy ELF header first and validate magic numbers before using offsets
    elf.elf_head = *reinterpret_cast<Elf64_Ehdr*>(base);

    // Validate ELF magic numbers immediately to catch corruption
    if (elf.elf_head.e_ident[EI_MAG0] != ELFMAG0 || elf.elf_head.e_ident[EI_MAG1] != ELFMAG1 || elf.elf_head.e_ident[EI_MAG2] != ELFMAG2 ||
        elf.elf_head.e_ident[EI_MAG3] != ELFMAG3) {
        mod::dbg::log("ERROR: Invalid ELF magic: 0x%x 0x%x 0x%x 0x%x", elf.elf_head.e_ident[EI_MAG0], elf.elf_head.e_ident[EI_MAG1],
                      elf.elf_head.e_ident[EI_MAG2], elf.elf_head.e_ident[EI_MAG3]);
        return elf;  // Return with invalid header
    }

    // Validate offsets are reasonable before dereferencing
    if (elf.elf_head.e_phoff == 0 || elf.elf_head.e_shoff == 0) {
        mod::dbg::log("ERROR: Invalid ELF offsets - phoff: 0x%x, shoff: 0x%x", elf.elf_head.e_phoff, elf.elf_head.e_shoff);
        return elf;
    }

    elf.pg_head = reinterpret_cast<Elf64_Phdr*>(base + elf.elf_head.e_phoff);
    elf.se_head = reinterpret_cast<Elf64_Shdr*>(base + elf.elf_head.e_shoff);
    elf.sct_head_str_tab = (Elf64_Shdr*)(elf.elf_head.e_shoff +                                // Section header offset
                                         (static_cast<Elf64_Off>(elf.elf_head.e_shstrndx       // Section header string table index
                                                                 * elf.elf_head.e_shentsize))  // Size of each section header
    );
    // Choose a non-zero base for PIE executables so their writable/data segments
    // live in normal user space instead of the low pages near NULL.
    if (elf.elf_head.e_type == ET_DYN) {
        elf.load_base = 0x400000ULL;
#ifdef ELF_DEBUG
        mod::dbg::log("Loading PIE executable with base address: 0x%x", elf.loadBase);
#endif
    } else {
        elf.load_base = 0;
#ifdef ELF_DEBUG
        mod::dbg::log("Loading regular executable (ET_EXEC)");
#endif
    }

    return elf;
}

void process_relocations(const ElfFile& elf, ker::mod::mm::virt::PageTable* pagemap) {
    // Get section header string table so we can detect .relr sections by name
    Elf64_Shdr const* shdr_table = elf.se_head;
    const char* shstr = reinterpret_cast<const char*>(elf.base + shdr_table[elf.elf_head.e_shstrndx].sh_offset);
    // No per-process resolver stub: we eagerly resolve and enforce RELRO.
    // Find relocation sections (REL and RELA)
    for (size_t i = 0; i < elf.elf_head.e_shnum; i++) {
        auto* section_header = (Elf64_Shdr*)((uint64_t)elf.se_head + (i * elf.elf_head.e_shentsize));

        // Handle the newer SHT_RELR compressed relocation section by name
        const char* sec_name = nullptr;
        if (elf.elf_head.e_shstrndx < elf.elf_head.e_shnum) {
            sec_name = shstr + section_header->sh_name;
        }

        if ((sec_name != nullptr) && ((std::strcmp(sec_name, ".relr") == 0) || (std::strcmp(sec_name, ".relr.dyn") == 0))) {
#ifdef ELF_DEBUG
            mod::dbg::log("Processing SHT_RELR (.relr) relocations in section %d (%s)", i, (secName != nullptr) ? secName : "");
#endif

            uint64_t const NUM_ENTRIES = section_header->sh_size / sizeof(uint64_t);
            auto* entries = reinterpret_cast<uint64_t*>(elf.base + section_header->sh_offset);

            uint64_t base = 0;
            for (uint64_t ei = 0; ei < NUM_ENTRIES; ei++) {
                uint64_t const ENT = entries[ei];
                if ((ENT & 1ULL) == 0ULL) {
                    // explicit relocation: this value is an address to relocate
                    base = ENT;
                    uint64_t const P = base + elf.load_base;
                    uint64_t const PADDR = mod::mm::virt::translate(pagemap, P);
                    if (PADDR != ker::mod::mm::virt::PADDR_INVALID) {
                        auto* phys_ptr = static_cast<uint64_t*>(mod::mm::addr::get_virt_pointer(PADDR));
                        // RELR encodes RELATIVE relocations: add loadBase to the current value
                        *phys_ptr = *phys_ptr + elf.load_base;
                    }
                } else {
                    // bitmask compressed entries: bits set indicate further relocations relative to 'base'
                    uint64_t const BITMAP = ENT & ~1ULL;
                    for (int bit = 0; bit < 63; ++bit) {
                        if ((BITMAP & (1ULL << bit)) != 0U) {
                            uint64_t const OFFSET = static_cast<uint64_t>(bit + 1) * sizeof(uint64_t);
                            uint64_t const P = base + OFFSET + elf.load_base;
                            uint64_t const PADDR = mod::mm::virt::translate(pagemap, P);
                            if (PADDR != ker::mod::mm::virt::PADDR_INVALID) {
                                auto* phys_ptr = static_cast<uint64_t*>(mod::mm::addr::get_virt_pointer(PADDR));
                                *phys_ptr = *phys_ptr + elf.load_base;
                            }
                        }
                    }
                }
            }

            continue;
        }
        if (section_header->sh_type == SHT_REL) {
#ifdef ELF_DEBUG
            mod::dbg::log("Processing SHT_REL relocations in section %d", i);
#endif

            uint64_t const NUM_RELOCATIONS = section_header->sh_size / sizeof(Elf64_Rel);
            auto* relocations = reinterpret_cast<Elf64_Rel*>(elf.base + section_header->sh_offset);

            for (uint64_t j = 0; j < NUM_RELOCATIONS; j++) {
                Elf64_Rel const* rel = &relocations[j];
                uint32_t const TYPE = ELF64_R_TYPE(rel->r_info);
                uint32_t const SYM_INDEX = ELF64_R_SYM(rel->r_info);
                uint64_t const P = rel->r_offset + elf.load_base;  // place

                // For REL entries, addend is the current value at P (read from memory at P)
                uint64_t addend = 0;
                uint64_t paddr = mod::mm::virt::translate(pagemap, P);
                if (paddr != ker::mod::mm::virt::PADDR_INVALID) {
                    auto* phys_ptr = static_cast<uint64_t*>(mod::mm::addr::get_virt_pointer(paddr));
                    addend = *phys_ptr;
                }

                // Resolve symbol value S
                uint64_t s = 0;
                const char* sym_name = "";
                if (SYM_INDEX != 0) {
                    // The relocation section's sh_link tells which symbol table to use
                    if (section_header->sh_link < elf.elf_head.e_shnum) {
                        auto* sym_tab_sec = (Elf64_Shdr*)((uint64_t)elf.se_head +
                                                          (static_cast<uint64_t>(section_header->sh_link * elf.elf_head.e_shentsize)));
                        auto* syms = reinterpret_cast<Elf64_Sym*>(elf.base + sym_tab_sec->sh_offset);
                        uint64_t const N = sym_tab_sec->sh_size / sym_tab_sec->sh_entsize;
                        // Get string table for symbol names if available
                        const char* sym_strs = nullptr;
                        if (sym_tab_sec->sh_link < elf.elf_head.e_shnum) {
                            auto* strtab_sec = (Elf64_Shdr*)((uint64_t)elf.se_head +
                                                             (static_cast<uint64_t>(sym_tab_sec->sh_link * elf.elf_head.e_shentsize)));
                            sym_strs = reinterpret_cast<const char*>(elf.base + strtab_sec->sh_offset);
                        }
                        if (SYM_INDEX < N) {
                            Elf64_Sym const* sym = &syms[SYM_INDEX];
                            sym_name = (sym_strs != nullptr) ? (sym_strs + sym->st_name) : "";
                            s = sym->st_value;
                            // If the symbol is defined in a section, add loadBase (unless TLS)
                            if (sym->st_shndx < elf.elf_head.e_shnum) {
                                auto* sym_sec = (Elf64_Shdr*)((uint64_t)elf.se_head +
                                                              (static_cast<uint64_t>(sym->st_shndx * elf.elf_head.e_shentsize)));
                                if ((sym_sec->sh_flags & SHF_TLS) == 0U) {
                                    s += elf.load_base;
                                }
                            }
                        }
                    } else {
                        // Fallback: search symbol tables if sh_link is invalid
                        for (size_t sidx = 0; sidx < elf.elf_head.e_shnum; sidx++) {
                            auto* sec = (Elf64_Shdr*)((uint64_t)elf.se_head + (sidx * elf.elf_head.e_shentsize));
                            if (sec->sh_type == SHT_SYMTAB || sec->sh_type == SHT_DYNSYM) {
                                auto* syms = reinterpret_cast<Elf64_Sym*>(elf.base + sec->sh_offset);
                                uint64_t const N = sec->sh_size / sec->sh_entsize;
                                if (SYM_INDEX < N) {
                                    Elf64_Sym const* sym = &syms[SYM_INDEX];
                                    // Try to get symbol name from string table
                                    if (sec->sh_link < elf.elf_head.e_shnum) {
                                        auto* strtab_sec = (Elf64_Shdr*)((uint64_t)elf.se_head +
                                                                         (static_cast<uint64_t>(sec->sh_link * elf.elf_head.e_shentsize)));
                                        const char* sym_strs = reinterpret_cast<const char*>(elf.base + strtab_sec->sh_offset);
                                        sym_name = (sym_strs != nullptr) ? (sym_strs + sym->st_name) : "";
                                    }
                                    s = sym->st_value;
                                    if (sym->st_shndx < elf.elf_head.e_shnum) {
                                        auto* sym_sec = (Elf64_Shdr*)((uint64_t)elf.se_head +
                                                                      (static_cast<uint64_t>(sym->st_shndx * elf.elf_head.e_shentsize)));
                                        if ((sym_sec->sh_flags & SHF_TLS) == 0U) {
                                            s += elf.load_base;
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }

#ifdef ELF_DEBUG
                mod::dbg::log("Relocation REL: P=0x%x, type=%d, sym=%d ('%s'), S=0x%x, A=0x%x", P, type, symIndex,
                              (symName != nullptr) ? symName : "", S, addend);
#endif
                switch (TYPE) {
                    case R_X86_64_TPOFF64: {
                        // TLS offset: S + A (symbol TLS offset plus addend)
                        if (paddr != ker::mod::mm::virt::PADDR_INVALID) {
                            auto* phys_ptr = static_cast<uint64_t*>(mod::mm::addr::get_virt_pointer(paddr));
                            *phys_ptr = s + addend;
                        }
                        break;
                    }
                    case R_X86_64_RELATIVE: {
                        if (paddr != ker::mod::mm::virt::PADDR_INVALID) {
                            auto* phys_ptr = static_cast<uint64_t*>(mod::mm::addr::get_virt_pointer(paddr));
                            *phys_ptr = addend + elf.load_base;
                        }
                        break;
                    }
                    case R_X86_64_GLOB_DAT:
                    case R_X86_64_JUMP_SLOT: {
                        // Ensure the target virtual address is mapped and writable for GOT/PLT writes.
                        if (paddr == ker::mod::mm::virt::PADDR_INVALID) {
                            uint64_t const TARGET_PAGE = P & ~(mod::mm::virt::PAGE_SIZE - 1);
#ifdef ELF_DEBUG
                            mod::dbg::log("GOT/PLT entry at 0x%x not mapped; allocating page 0x%x", P, targetPage);
#endif
                            auto new_paddr = (uint64_t)mod::mm::phys::page_alloc();
                            if (new_paddr != 0) {
                                auto phys_ptr_page = (uint64_t)mod::mm::addr::get_phys_pointer(new_paddr);
                                mod::mm::virt::map_page(pagemap, TARGET_PAGE, phys_ptr_page, mod::mm::paging::page_types::USER);
                                paddr = mod::mm::virt::translate(pagemap, P);
                            }
                        }

                        if (paddr != ker::mod::mm::virt::PADDR_INVALID) {
                            auto* phys_ptr = static_cast<uint64_t*>(mod::mm::addr::get_virt_pointer(paddr));
                            if (s == 0) {
                                mod::dbg::log(
                                    "ERROR: Unresolved symbol '%s' (idx=%d) for relocation at P=0x%x (type=%d). Writing 0 to catch fault.",
                                    sym_name, SYM_INDEX, P, TYPE);
                            }
                            uint64_t const WRITE_VAL = s + addend;
                            *phys_ptr = WRITE_VAL;
#ifdef ELF_DEBUG
                            // Read-back verification
                            uint64_t verifyVal = *physPtr;
                            mod::dbg::log("Wrote GOT/PLT at P=0x%x (paddr=0x%x) -> 0x%x (read-back=0x%x)", P, paddr, writeVal, verifyVal);
#endif
                        } else {
#ifdef ELF_DEBUG
                            mod::dbg::log("Failed to map/allocate GOT/PLT page for P=0x%x", P);
#endif
                        }

                        break;
                    }
                    case R_X86_64_64: {
                        if (paddr != ker::mod::mm::virt::PADDR_INVALID) {
                            auto* phys_ptr = static_cast<uint64_t*>(mod::mm::addr::get_virt_pointer(paddr));
                            *phys_ptr = s + addend;
                        }
                        break;
                    }
                    case R_X86_64_PC32:
                    case R_X86_64_PLT32: {
                        // 32-bit PC-relative
                        if (paddr != ker::mod::mm::virt::PADDR_INVALID) {
                            auto* phys_ptr32 = reinterpret_cast<uint32_t*>(mod::mm::addr::get_virt_pointer(paddr));
                            uint64_t const P64 = P;
                            auto value = static_cast<int64_t>(s + static_cast<int64_t>(addend) - static_cast<int64_t>(P64));
                            *phys_ptr32 = static_cast<uint32_t>(value);
                        }
                        break;
                    }
                    case R_X86_64_NONE:
                    case R_X86_64_GOT32:
                    case R_X86_64_COPY:
                    case R_X86_64_GOTPCREL:
                    case R_X86_64_32:
                    case R_X86_64_32S:
                    case R_X86_64_16:
                    case R_X86_64_PC16:
                    case R_X86_64_8:
                    case R_X86_64_PC8:
                    case R_X86_64_DTPMOD64:
                    case R_X86_64_DTPOFF64:
                    case R_X86_64_TLSGD:
                    case R_X86_64_TLSLD:
                    default:
#ifdef ELF_DEBUG
                        mod::dbg::log("Unhandled REL relocation type: %d", type);
#endif
                        break;
                }
            }
        } else if (section_header->sh_type == SHT_RELA) {
#ifdef ELF_DEBUG
            mod::dbg::log("Processing SHT_RELA relocations in section %d", i);
#endif

            uint64_t const NUM_RELOCATIONS = section_header->sh_size / sizeof(Elf64_Rela);
            auto* relocations = reinterpret_cast<Elf64_Rela*>(elf.base + section_header->sh_offset);

            for (uint64_t j = 0; j < NUM_RELOCATIONS; j++) {
                Elf64_Rela const* rel = &relocations[j];
                uint32_t const TYPE = ELF64_R_TYPE(rel->r_info);
                uint32_t const SYM_INDEX = ELF64_R_SYM(rel->r_info);
                uint64_t const P = rel->r_offset + elf.load_base;  // place
                int64_t const ADDEND = rel->r_addend;

                // Resolve symbol value S
                uint64_t s = 0;
                const char* sym_name = "";
                if (SYM_INDEX != 0) {
                    if (section_header->sh_link < elf.elf_head.e_shnum) {
                        auto* sym_tab_sec = (Elf64_Shdr*)((uint64_t)elf.se_head +
                                                          (static_cast<uint64_t>(section_header->sh_link * elf.elf_head.e_shentsize)));
                        auto* syms = reinterpret_cast<Elf64_Sym*>(elf.base + sym_tab_sec->sh_offset);
                        uint64_t const N = sym_tab_sec->sh_size / sym_tab_sec->sh_entsize;
                        const char* sym_strs = nullptr;
                        if (sym_tab_sec->sh_link < elf.elf_head.e_shnum) {
                            auto* strtab_sec = (Elf64_Shdr*)((uint64_t)elf.se_head +
                                                             (static_cast<uint64_t>(sym_tab_sec->sh_link * elf.elf_head.e_shentsize)));
                            sym_strs = reinterpret_cast<const char*>(elf.base + strtab_sec->sh_offset);
                        }

                        if (SYM_INDEX < N) {
                            Elf64_Sym const* sym = &syms[SYM_INDEX];
                            sym_name = (sym_strs != nullptr) ? (sym_strs + sym->st_name) : "";
                            s = sym->st_value;
                            if (sym->st_shndx < elf.elf_head.e_shnum) {
                                auto* sym_sec = (Elf64_Shdr*)((uint64_t)elf.se_head +
                                                              (static_cast<uint64_t>(sym->st_shndx * elf.elf_head.e_shentsize)));
                                if ((sym_sec->sh_flags & SHF_TLS) == 0U) {
                                    s += elf.load_base;
                                }
                            }
                        }
                    } else {
                        for (size_t sidx = 0; sidx < elf.elf_head.e_shnum; sidx++) {
                            auto* sec = (Elf64_Shdr*)((uint64_t)elf.se_head + (sidx * elf.elf_head.e_shentsize));
                            if (sec->sh_type == SHT_SYMTAB || sec->sh_type == SHT_DYNSYM) {
                                auto* syms = reinterpret_cast<Elf64_Sym*>(elf.base + sec->sh_offset);
                                uint64_t const N = sec->sh_size / sec->sh_entsize;
                                if (SYM_INDEX < N) {
                                    Elf64_Sym const* sym = &syms[SYM_INDEX];
                                    // Try to get symbol name from string table
                                    if (sec->sh_link < elf.elf_head.e_shnum) {
                                        auto* strtab_sec = (Elf64_Shdr*)((uint64_t)elf.se_head +
                                                                         (static_cast<uint64_t>(sec->sh_link * elf.elf_head.e_shentsize)));
                                        const char* sym_strs = reinterpret_cast<const char*>(elf.base + strtab_sec->sh_offset);
                                        sym_name = (sym_strs != nullptr) ? (sym_strs + sym->st_name) : "";
                                    }
                                    s = sym->st_value;
                                    if (sym->st_shndx < elf.elf_head.e_shnum) {
                                        auto* sym_sec = (Elf64_Shdr*)((uint64_t)elf.se_head +
                                                                      (static_cast<uint64_t>(sym->st_shndx * elf.elf_head.e_shentsize)));
                                        if ((sym_sec->sh_flags & SHF_TLS) == 0U) {
                                            s += elf.load_base;
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }

                uint64_t paddr = mod::mm::virt::translate(pagemap, P);
#ifdef ELF_DEBUG
                mod::dbg::log("Relocation REL: P=0x%x, type=%d, sym=%d ('%s'), S=0x%x, A=0x%x", P, type, symIndex, symName ? symName : "",
                              S, addend);
#endif

                switch (TYPE) {
                    case R_X86_64_TPOFF64: {
                        int64_t const TLS_OFFSET = ADDEND;  // use addend if present
                        if (paddr != ker::mod::mm::virt::PADDR_INVALID) {
                            auto* phys_ptr = static_cast<uint64_t*>(mod::mm::addr::get_virt_pointer(paddr));
                            *phys_ptr = static_cast<uint64_t>(TLS_OFFSET);
                        }
                        break;
                    }
                    case R_X86_64_RELATIVE: {
                        if (paddr != ker::mod::mm::virt::PADDR_INVALID) {
                            auto* phys_ptr = static_cast<uint64_t*>(mod::mm::addr::get_virt_pointer(paddr));
                            *phys_ptr = elf.load_base + static_cast<uint64_t>(ADDEND);
                        }
                        break;
                    }
                    case R_X86_64_GLOB_DAT:
                    case R_X86_64_JUMP_SLOT: {
                        if (paddr == ker::mod::mm::virt::PADDR_INVALID) {
                            uint64_t const TARGET_PAGE = P & ~(mod::mm::virt::PAGE_SIZE - 1);
#ifdef ELF_DEBUG
                            mod::dbg::log("GOT/PLT entry at 0x%x not mapped; allocating page 0x%x", P, targetPage);
#endif
                            auto new_paddr = (uint64_t)mod::mm::phys::page_alloc();
                            if (new_paddr != 0) {
                                auto phys_ptr = (uint64_t)mod::mm::addr::get_phys_pointer(new_paddr);
                                mod::mm::virt::map_page(pagemap, TARGET_PAGE, phys_ptr, mod::mm::paging::page_types::USER);
                                paddr = mod::mm::virt::translate(pagemap, P);
                            }
                        }

                        if (paddr != ker::mod::mm::virt::PADDR_INVALID) {
                            auto* phys_ptr = static_cast<uint64_t*>(mod::mm::addr::get_virt_pointer(paddr));
                            if (s == 0) {
                                mod::dbg::log(
                                    "ERROR: Unresolved symbol '%s' (idx=%d) for relocation at P=0x%x (type=%d). Writing 0 to catch fault.",
                                    sym_name, SYM_INDEX, P, TYPE);
                            }
                            uint64_t const WRITE_VAL = s + static_cast<uint64_t>(ADDEND);
                            *phys_ptr = WRITE_VAL;
#ifdef ELF_DEBUG
                            mod::dbg::log("Wrote GOT/PLT at P=0x%x -> 0x%x (S=0x%x)", P, writeVal, S);
#endif
                        } else {
#ifdef ELF_DEBUG
                            mod::dbg::log("Failed to map/allocate GOT/PLT page for P=0x%x", P);
#endif
                        }

                        break;
                    }
                    case R_X86_64_64: {
                        if (paddr != ker::mod::mm::virt::PADDR_INVALID) {
                            auto* phys_ptr = static_cast<uint64_t*>(mod::mm::addr::get_virt_pointer(paddr));
                            *phys_ptr = s + static_cast<uint64_t>(ADDEND);
                        }
                        break;
                    }
                    case R_X86_64_PC32:
                    case R_X86_64_PLT32: {
                        if (paddr != ker::mod::mm::virt::PADDR_INVALID) {
                            auto* phys_ptr32 = reinterpret_cast<uint32_t*>(mod::mm::addr::get_virt_pointer(paddr));
                            uint64_t const P64 = P;
                            auto value = static_cast<int64_t>(s + ADDEND - static_cast<int64_t>(P64));
                            *phys_ptr32 = static_cast<uint32_t>(value);
                        }
                        break;
                    }
                    default:
#ifdef ELF_DEBUG
                        mod::dbg::log("Unhandled RELA relocation type: %d", type);
#endif
                        break;
                }
            }
        }
    }
}

void process_read_only_segment(Elf64_Phdr* segment, ker::mod::mm::virt::PageTable* pagemap, uint64_t pid) {
    (void)pid;
    // PT_GNU_RELRO describes a region that should become read-only AFTER relocations.
    // These pages are already mapped by PT_LOAD segments. Do NOT remap using p_offset
    // (file offset) as a physical address; that corrupts the mapping (incl. GOT/PLT).
    // Instead, if desired, tighten permissions of already-mapped pages.

    // Defer making RELRO read-only until AFTER all relocations complete.
    // No action here during initial load to avoid write-protection issues with CR0.WP set.
    (void)segment;
    (void)pagemap;
}

void register_eh_frame(void* base, uint64_t size) {
    (void)base;
    (void)size;
    // TODO: Register the .eh_frame section for exception handling
}

void process_eh_frame_segment(Elf64_Phdr* segment, ker::mod::mm::virt::PageTable* pagemap, uint64_t pid) {
    (void)pid;
    // .eh_frame(.hdr) lies within PT_LOAD in well-formed binaries; we should not remap
    // using file offsets. Just ensure pages remain readable (optionally read-only) and
    // register the region for unwinding.

    uint64_t const VADDR = segment->p_vaddr;
    uint64_t const END = (segment->p_vaddr + segment->p_memsz + mod::mm::virt::PAGE_SIZE - 1) & ~(mod::mm::virt::PAGE_SIZE - 1);
    for (uint64_t va = VADDR; va < END; va += mod::mm::virt::PAGE_SIZE) {
        if (mod::mm::virt::is_page_mapped(pagemap, va)) {
            mod::mm::virt::unify_page_flags(pagemap, va, mod::mm::paging::page_types::USER_READONLY);
        }
    }

    // Register the .eh_frame section for exception handling
    register_eh_frame((void*)VADDR, segment->p_memsz);
}

void load_segment(uint8_t* elf_base, ker::mod::mm::virt::PageTable* pagemap, Elf64_Phdr* program_header, uint64_t page_no,
                  uint64_t base_offset) {
    // Compute aligned virtual address for this page and in-page offset of the segment start
    const uint64_t SEG_START_VA = program_header->p_vaddr + base_offset;
    const uint64_t FIRST_PAGE_OFFSET = SEG_START_VA & (mod::mm::virt::PAGE_SIZE - 1);
    const uint64_t ALIGNED_START_VA = SEG_START_VA & ~(mod::mm::virt::PAGE_SIZE - 1);
    const uint64_t PAGE_VA = ALIGNED_START_VA + (page_no * mod::mm::virt::PAGE_SIZE);

    // Additional validation for PIE executables
    if (base_offset != 0 && PAGE_VA < mod::mm::virt::PAGE_SIZE) {
        ker::mod::io::serial::write("PIE program trying to map too low address 0x%x\n", PAGE_VA);
        return;
    }

    // Map the page at a page-aligned virtual address
    bool const ALREADY_MAPPED = mod::mm::virt::is_page_mapped(pagemap, PAGE_VA);
    // HHDM-mapped pointer to the backing physical page so we can write contents
    uint64_t page_hhdm_ptr = 0;
    if (ALREADY_MAPPED) {
        mod::mm::virt::unify_page_flags(pagemap, PAGE_VA, mod::mm::paging::page_types::USER);
        uint64_t const PADDR = mod::mm::virt::translate(pagemap, PAGE_VA);
        if (PADDR == ker::mod::mm::virt::PADDR_INVALID) {
            mod::dbg::log("elf_loader: translate failed for already-mapped pageVA 0x%lx", PAGE_VA);
            hcf();
        }
        page_hhdm_ptr = (uint64_t)mod::mm::addr::get_virt_pointer(PADDR);
    } else {
        // Allocate a new physical page; allocator returns an HHDM pointer to the page memory
        auto new_page_hhdm_ptr = (uint64_t)mod::mm::phys::page_alloc();
        page_hhdm_ptr = new_page_hhdm_ptr;
        // Map using the physical address corresponding to that HHDM pointer
        mod::mm::virt::map_page(pagemap, PAGE_VA, (mod::mm::addr::paddr_t)mod::mm::addr::get_phys_pointer(new_page_hhdm_ptr),
                                mod::mm::paging::page_types::USER);
        // Zero freshly mapped page to handle bss/holes
        memset((void*)page_hhdm_ptr, 0, mod::mm::virt::PAGE_SIZE);
    }

    // Determine source file offset and destination in-page offset for this page
    const uint64_t DST_IN_PAGE = (page_no == 0) ? FIRST_PAGE_OFFSET : 0;
    const uint64_t ROOM_IN_PAGE = mod::mm::virt::PAGE_SIZE - DST_IN_PAGE;

    // How many bytes have already been copied before this page?
    uint64_t bytes_before_this_page = 0;
    if (page_no == 0) {
        bytes_before_this_page = 0;
    } else {
        // First (partial) page accounts for (PAGE_SIZE - firstPageOffset), then full pages
        bytes_before_this_page = (mod::mm::virt::PAGE_SIZE - FIRST_PAGE_OFFSET) + ((page_no - 1) * mod::mm::virt::PAGE_SIZE);
    }

    // If we've consumed the entire file content of the segment, nothing to copy for this page
    if (bytes_before_this_page >= program_header->p_filesz) {
        return;
    }

    // Compute how much to copy from this page
    uint64_t const REMAINING_IN_FILE = program_header->p_filesz - bytes_before_this_page;
    uint64_t const COPY_SIZE = REMAINING_IN_FILE < ROOM_IN_PAGE ? REMAINING_IN_FILE : ROOM_IN_PAGE;

    // Copy from ELF file to destination page at the correct in-page offset
    const uint64_t SRC_OFFSET = program_header->p_offset + bytes_before_this_page;
    memcpy((void*)(page_hhdm_ptr + DST_IN_PAGE), elf_base + SRC_OFFSET, COPY_SIZE);
}

void load_section_headers(const ElfFile& elf, ker::mod::mm::virt::PageTable* pagemap, const uint64_t& pid) {
    (void)pid;
    auto* scn_head_table = (Elf64_Shdr*)((uint64_t)elf.base                                       // Base address of the ELF file
                                         + elf.elf_head.e_shoff                                   // Section header offset
                                         + (static_cast<uint64_t>(elf.elf_head.e_shstrndx         // Section header string table index
                                                                  * elf.elf_head.e_shentsize)));  // Size of each section header

    const char* section_names = reinterpret_cast<const char*>(elf.base + scn_head_table->sh_offset);

    // Allocate memory for section headers to preserve them for debugging
    auto section_headers_size = static_cast<uint64_t>(elf.elf_head.e_shnum * elf.elf_head.e_shentsize);
    uint64_t const SECTION_HEADERS_PAGES = page_align_up(section_headers_size) / mod::mm::virt::PAGE_SIZE;
    constexpr uint64_t SECTION_HEADERS_VADDR = 0x700000000000ULL;  // High memory area for debug info

    uint64_t section_headers_phys_ptr = 0;  // Make this accessible throughout function
    for (uint64_t i = 0; i < SECTION_HEADERS_PAGES; i++) {
        auto paddr = (uint64_t)mod::mm::phys::page_alloc();
        if (paddr != ker::mod::mm::virt::PADDR_INVALID) {
            auto phys_ptr = (uint64_t)mod::mm::addr::get_phys_pointer(paddr);
            mod::mm::virt::map_page(pagemap, SECTION_HEADERS_VADDR + (i * mod::mm::virt::PAGE_SIZE), phys_ptr,
                                    mod::mm::paging::page_types::USER_READONLY | mod::mm::paging::PAGE_NX);

            // Zero the page
            memset((void*)paddr, 0, mod::mm::virt::PAGE_SIZE);

            // Remember first page for copying
            if (i == 0) {
                section_headers_phys_ptr = paddr;
            }
        }
    }

    // Copy section headers to allocated physical memory
    if (section_headers_phys_ptr != 0) {
        memcpy((void*)section_headers_phys_ptr, elf.se_head, section_headers_size);
    }
    debug::set_section_headers(pid, (Elf64_Shdr*)SECTION_HEADERS_VADDR, SECTION_HEADERS_VADDR, elf.elf_head.e_shnum);

    // Allocate memory for string table
    uint64_t const STRING_TABLE_SIZE = scn_head_table->sh_size;
    uint64_t const STRING_TABLE_PAGES = page_align_up(STRING_TABLE_SIZE) / mod::mm::virt::PAGE_SIZE;
    uint64_t const STRING_TABLE_VADDR = 0x700000201000ULL;  // After section headers

    uint64_t string_table_phys_ptr = 0;
    for (uint64_t i = 0; i < STRING_TABLE_PAGES; i++) {
        auto paddr = (uint64_t)mod::mm::phys::page_alloc();
        if (paddr != ker::mod::mm::virt::PADDR_INVALID) {
            auto phys_ptr = (uint64_t)mod::mm::addr::get_phys_pointer(paddr);
            mod::mm::virt::map_page(pagemap, STRING_TABLE_VADDR + (i * mod::mm::virt::PAGE_SIZE), phys_ptr,
                                    mod::mm::paging::page_types::USER_READONLY | mod::mm::paging::PAGE_NX);

            // Zero the page
            memset((void*)paddr, 0, mod::mm::virt::PAGE_SIZE);

            // Remember first page for copying
            if (i == 0) {
                string_table_phys_ptr = paddr;
            }
        }
    }

    // Copy string table to allocated physical memory
    if (string_table_phys_ptr != 0) {
        memcpy((void*)string_table_phys_ptr, section_names, STRING_TABLE_SIZE);
    }
    debug::set_string_table(pid, (const char*)STRING_TABLE_VADDR, STRING_TABLE_VADDR, STRING_TABLE_SIZE);

    for (size_t section_index = 0; section_index < elf.elf_head.e_shnum; section_index++) {
        auto* section_header = (Elf64_Shdr*)((uint64_t)elf.se_head + (section_index * elf.elf_head.e_shentsize));
        const char* section_name = &section_names[section_header->sh_name];
#ifdef ELF_DEBUG
        mod::dbg::log("Section name: %s", sectionName);
#endif
        (void)section_name;

        // Register sections for debugging without re-mapping any PT_LOAD-backed content
        if (section_header->sh_type == SHT_PROGBITS && section_header->sh_size > 0) {
            // If the section already has a virtual address, it is covered by PT_LOAD; just record it
            if (section_header->sh_addr != 0) {
                uint64_t const SECTION_VADDR = section_header->sh_addr + elf.load_base;
                uint64_t const FIRST_PADDR = mod::mm::virt::translate(pagemap, SECTION_VADDR);
                debug::add_debug_section(pid, section_name, SECTION_VADDR, FIRST_PADDR, section_header->sh_size, section_header->sh_offset,
                                         section_header->sh_type);
#ifdef ELF_DEBUG
                mod::dbg::log("Added debug section: %s, vaddr=%x, paddr=%x, size=%x", sectionName, sectionVaddr, firstPaddr,
                              sectionHeader->sh_size);
#endif
            } else if (std::strncmp(section_name, ".debug_", 7) == 0) {
                debug::add_debug_section(pid, section_name, 0, 0, section_header->sh_size, section_header->sh_offset,
                                         section_header->sh_type);
#ifdef ELF_DEBUG
                mod::dbg::log("Recorded debug section metadata only: %s (size=%x, fileOff=%x)", sectionName, sectionHeader->sh_size,
                              sectionHeader->sh_offset);
#endif
            }
        }
        // Additionally, record GOT sections in debug info registry for diagnostics (no remapping or copying here)
        if (((std::strncmp(section_name, ".got", 4) == 0) || (std::strncmp(section_name, ".got.plt", 8) == 0)) &&
            section_header->sh_addr != 0 && section_header->sh_size > 0) {
            uint64_t const SECTION_VADDR = section_header->sh_addr + elf.load_base;
            uint64_t const FIRST_PADDR = mod::mm::virt::translate(pagemap, SECTION_VADDR);
            debug::add_debug_section(pid, section_name, SECTION_VADDR, FIRST_PADDR, section_header->sh_size, section_header->sh_offset,
                                     section_header->sh_type);
#ifdef ELF_DEBUG
            mod::dbg::log("Recorded GOT-like section %s at vaddr: %x, paddr: %x, size: %x", sectionName, sectionVaddr, firstPaddr,
                          sectionHeader->sh_size);
#endif
        }
    }
}
}  // namespace

auto load_elf(ElfFile* elf, ker::mod::mm::virt::PageTable* pagemap, uint64_t pid, const char* process_name, bool register_special_symbols,
              uint64_t base_address) -> ElfLoadResult {
    // Validate input pointer
    if (elf == nullptr) {
        mod::dbg::log("ERROR: loadElf called with null ELF pointer (pid=%d)", pid);
        return {.entry_point = 0, .program_header_addr = 0, .elf_header_addr = 0};
    }

    ElfFile elf_file = parse_elf(reinterpret_cast<uint8_t*>(elf));

    // Apply explicit base address (used when loading ld.so at a non-zero base)
    if (base_address != 0) {
        elf_file.load_base = base_address;
    }

    if (!header_is_valid(elf_file.elf_head)) {
        mod::dbg::log("ERROR: Invalid ELF header (pid=%d)", pid);
        mod::dbg::log("  ELF base: 0x%p", elf);
        mod::dbg::log("  e_ident: [0x%x 0x%x 0x%x 0x%x] (expected [0x%x 0x%x 0x%x 0x%x])", elf_file.elf_head.e_ident[EI_MAG0],
                      elf_file.elf_head.e_ident[EI_MAG1], elf_file.elf_head.e_ident[EI_MAG2], elf_file.elf_head.e_ident[EI_MAG3], ELFMAG0,
                      ELFMAG1, ELFMAG2, ELFMAG3);
        mod::dbg::log("  e_ident[EI_CLASS]: 0x%x (expected ELFCLASS64=0x%x)", elf_file.elf_head.e_ident[EI_CLASS], ELFCLASS64);
        mod::dbg::log("  e_type: 0x%x (expected ET_EXEC=0x%x or ET_DYN=0x%x)", elf_file.elf_head.e_type, ET_EXEC, ET_DYN);
        mod::dbg::log("  e_phoff: 0x%x, e_shoff: 0x%x", elf_file.elf_head.e_phoff, elf_file.elf_head.e_shoff);
        return {.entry_point = 0, .program_header_addr = 0, .elf_header_addr = 0};
    }

    // Register this process for debugging
    debug::register_process(pid, process_name, (uint64_t)elf_file.base, elf_file.elf_head.e_entry + elf_file.load_base);

    // Collect all program headers for the executable — ld.so needs the full set
    // (PT_LOAD, PT_DYNAMIC, PT_TLS, PT_PHDR, PT_GNU_RELRO, etc.)
    std::vector<Elf64_Phdr> filtered_headers;
    for (Elf64_Half i = 0; i < elf_file.elf_head.e_phnum; i++) {
        auto* ph = (Elf64_Phdr*)((uint64_t)elf_file.pg_head + (static_cast<uint64_t>(i * elf_file.elf_head.e_phentsize)));
        filtered_headers.push_back(*ph);
    }

    // Set up AT_PHDR for the main executable (baseAddress == 0).
    // For PIE (ET_DYN): PHDRs are already mapped by the first PT_LOAD segment at their
    // original file offsets (e_phoff). Copying them to a fixed address like 0x1000 would
    // conflict with PT_LOAD segments that cover that range, and the subsequent PT_LOAD
    // mapping would overwrite the copied headers. Just point AT_PHDR at the originals.
    // For non-PIE (ET_EXEC): PT_LOAD segments start at high addresses (e.g. 0x400000),
    // so we can safely copy headers to 0x1000 without conflicts.
    uint64_t elf_header_vaddr = 0;
    uint64_t program_headers_vaddr = 0;

    if (base_address == 0 && elf_file.elf_head.e_type == ET_DYN) {
        // PIE executable: PHDRs already at loadBase + e_phoff via PT_LOAD mapping.
        // PT_PHDR.p_vaddr matches e_phoff, so ld.so computes baseAddress = AT_PHDR - p_vaddr = 0.
        elf_header_vaddr = elf_file.load_base;
        program_headers_vaddr = elf_file.load_base + elf_file.elf_head.e_phoff;
        debug::set_program_headers(pid, (Elf64_Phdr*)program_headers_vaddr, program_headers_vaddr, elf_file.elf_head.e_phnum);
    } else if (base_address == 0) {
        // Non-PIE (ET_EXEC): copy headers to 0x1000 (no PT_LOAD overlap at low addresses)
        constexpr uint64_t HEADER_COPY_VADDR = 0x1000;
        constexpr uint64_t PROGRAM_HEADERS_OFFSET_IN_HEADER = sizeof(Elf64_Ehdr);
        elf_header_vaddr = HEADER_COPY_VADDR;
        program_headers_vaddr = HEADER_COPY_VADDR + PROGRAM_HEADERS_OFFSET_IN_HEADER;

        auto program_headers_size = static_cast<uint64_t>(filtered_headers.size() * elf_file.elf_head.e_phentsize);
        uint64_t const TOTAL_HEADERS_SIZE = sizeof(Elf64_Ehdr) + program_headers_size;
        uint64_t const TOTAL_HEADERS_PAGES = page_align_up(TOTAL_HEADERS_SIZE) / mod::mm::virt::PAGE_SIZE;

        // Allocate physical pages for both ELF and program headers
        std::vector<uint64_t> header_phys_addrs;
        for (uint64_t i = 0; i < TOTAL_HEADERS_PAGES; i++) {
            auto paddr = (uint64_t)mod::mm::phys::page_alloc();
            if (paddr == ker::mod::mm::virt::PADDR_INVALID) {
                mod::dbg::log("ERROR: Failed to allocate physical page for headers");
                return {.entry_point = 0, .program_header_addr = 0, .elf_header_addr = 0};
            }
            mod::mm::virt::map_page(pagemap, HEADER_COPY_VADDR + (i * mod::mm::virt::PAGE_SIZE),
                                    (uint64_t)mod::mm::addr::get_phys_pointer(paddr), mod::mm::paging::page_types::USER);
            header_phys_addrs.push_back(paddr);
        }

        // Copy ELF header
        {
            auto* header_ptr = (Elf64_Ehdr*)header_phys_addrs[0];
            memcpy(header_ptr, &elf_file.elf_head, sizeof(Elf64_Ehdr));
            header_ptr->e_phoff = PROGRAM_HEADERS_OFFSET_IN_HEADER;
            header_ptr->e_phnum = static_cast<Elf64_Half>(filtered_headers.size());
        }

        // Copy program headers, fixing up PT_PHDR.p_vaddr to match placement
        {
            for (auto& filtered_header : filtered_headers) {
                if (filtered_header.p_type == PT_PHDR) {
                    filtered_header.p_vaddr = program_headers_vaddr;
                    filtered_header.p_paddr = program_headers_vaddr;
                }
            }

            uint64_t dest_offset = PROGRAM_HEADERS_OFFSET_IN_HEADER;
            for (auto& filtered_header : filtered_headers) {
                uint64_t const HEADER_SIZE = elf_file.elf_head.e_phentsize;
                for (uint64_t i = 0; i < HEADER_SIZE; i++) {
                    uint64_t const PAGE_IDX = dest_offset / mod::mm::virt::PAGE_SIZE;
                    uint64_t const OFFSET_IN_PAGE = dest_offset % mod::mm::virt::PAGE_SIZE;

                    if (PAGE_IDX >= header_phys_addrs.size()) {
                        mod::dbg::log("ERROR: Program header offset exceeds allocated pages");
                        return {.entry_point = 0, .program_header_addr = 0, .elf_header_addr = 0};
                    }

                    uint8_t* dest_ptr = (uint8_t*)header_phys_addrs[PAGE_IDX] + OFFSET_IN_PAGE;
                    uint8_t const* src_ptr = reinterpret_cast<uint8_t*>(&filtered_header) + i;
                    *dest_ptr = *src_ptr;
                    dest_offset++;
                }
            }
        }

        debug::set_program_headers(pid, (Elf64_Phdr*)program_headers_vaddr, program_headers_vaddr,
                                   static_cast<uint16_t>(filtered_headers.size()));
    }
    for (Elf64_Half i = 0; i < elf_file.elf_head.e_phnum; i++) {
        auto* current_header = (Elf64_Phdr*)((uint64_t)elf_file.pg_head + (static_cast<uint64_t>(i * elf_file.elf_head.e_phentsize)));

        switch (current_header->p_type) {
            case PT_GNU_STACK:
                // GNU_STACK segment presence indicates whether stack is executable; record as a debug section
#ifdef ELF_DEBUG
                mod::dbg::log("Found PT_GNU_STACK at vaddr=0x%x, flags=0x%x", currentHeader->p_vaddr, currentHeader->p_flags);
#endif
                debug::add_debug_section(pid, "PT_GNU_STACK", current_header->p_vaddr + elf_file.load_base, current_header->p_offset,
                                         current_header->p_memsz, current_header->p_offset, current_header->p_type);
                break;
            case PT_TLS:
                // Found TLS segment - store its information
                elf_file.tls_info.tls_base = current_header->p_vaddr + elf_file.load_base;
                elf_file.tls_info.tls_size = current_header->p_memsz;
                elf_file.tls_info.tcb_offset = current_header->p_memsz;  // TCB goes after TLS data
#ifdef ELF_DEBUG
                mod::dbg::log("Found PT_TLS segment: vaddr=0x%x, filesz=0x%x, memsz=0x%x", currentHeader->p_vaddr, currentHeader->p_filesz,
                              currentHeader->p_memsz);
#endif
                break;

            case PT_LOAD:
                // Loadable segment
                {
#ifdef ELF_DEBUG
                    mod::dbg::log("Loading PT_LOAD segment: vaddr=0x%x, filesz=0x%x, memsz=0x%x, offset=0x%x", currentHeader->p_vaddr,
                                  currentHeader->p_filesz, currentHeader->p_memsz, currentHeader->p_offset);
#endif
                    // Calculate number of pages accounting for offset within first page
                    uint64_t const SEG_END = current_header->p_vaddr + current_header->p_memsz;
                    uint64_t const START_PAGE_ADDR = current_header->p_vaddr & ~(mod::mm::virt::PAGE_SIZE - 1);
                    uint64_t const END_PAGE_ADDR = (SEG_END + mod::mm::virt::PAGE_SIZE - 1) & ~(mod::mm::virt::PAGE_SIZE - 1);
                    size_t const NUM_PAGES = (END_PAGE_ADDR - START_PAGE_ADDR) / mod::mm::virt::PAGE_SIZE;
#ifdef ELF_DEBUG
                    mod::dbg::log("Calculated pages: start_page=0x%x, end_page=0x%x, num_pages=%zu", startPageAddr, endPageAddr, num_pages);
#endif
                    for (uint64_t j = 0; j < NUM_PAGES; j++) {
                        load_segment(elf_file.base, pagemap, current_header, j, elf_file.load_base);
                    }
                }
                break;

            case PT_GNU_RELRO:
#ifdef ELF_DEBUG
                mod::dbg::log("Found PT_GNU_RELRO segment at vaddr=0x%x, size=0x%x", currentHeader->p_vaddr, currentHeader->p_memsz);
#endif
                // Read-only relocation segment
                process_read_only_segment(current_header, pagemap, pid);
                break;

            case PT_GNU_EH_FRAME:
#ifdef ELF_DEBUG
                mod::dbg::log("Found PT_GNU_EH_FRAME segment at vaddr=0x%x, size=0x%x", currentHeader->p_vaddr, currentHeader->p_memsz);
#endif
                // Exception handling frame segment
                process_eh_frame_segment(current_header, pagemap, pid);
                break;

            case PT_PHDR:
                // Program header table - informational only, no loading needed
#ifdef ELF_DEBUG
                mod::dbg::log("PT_PHDR segment skipped (informational only)");
#endif
                break;

            case PT_INTERP:
                // Extract the dynamic linker path from the segment data.
                // The path is a null-terminated string at p_offset within the ELF.
                // We don't load the interpreter here — the caller (exec) handles it.
                break;

            case PT_DYNAMIC:
                // TODO: should be fine since mlibc loads these
                // mod::dbg::log("WARN: PT_DYNAMIC skipped FIXME!");
                break;

            case PT_NOTE:
                // Note segments contain auxiliary info (build-id, ABI tags, etc.)
                // Map them read-only so they are accessible at runtime.
                {
                    if (current_header->p_memsz > 0 && current_header->p_vaddr != 0) {
#ifdef ELF_DEBUG
                        mod::dbg::log("Loading PT_NOTE segment: vaddr=0x%x, filesz=0x%x, memsz=0x%x, offset=0x%x", currentHeader->p_vaddr,
                                      currentHeader->p_filesz, currentHeader->p_memsz, currentHeader->p_offset);
#endif
                        uint64_t const SEG_END = current_header->p_vaddr + current_header->p_memsz;
                        uint64_t const START_PAGE_ADDR = current_header->p_vaddr & ~(mod::mm::virt::PAGE_SIZE - 1);
                        uint64_t const END_PAGE_ADDR = (SEG_END + mod::mm::virt::PAGE_SIZE - 1) & ~(mod::mm::virt::PAGE_SIZE - 1);
                        size_t const NUM_PAGES = (END_PAGE_ADDR - START_PAGE_ADDR) / mod::mm::virt::PAGE_SIZE;

                        for (uint64_t j = 0; j < NUM_PAGES; j++) {
                            load_segment(elf_file.base, pagemap, current_header, j, elf_file.load_base);
                        }

                        // Mark note pages read-only + NX (notes are data, never executed)
                        for (uint64_t va = START_PAGE_ADDR + elf_file.load_base; va < END_PAGE_ADDR + elf_file.load_base;
                             va += mod::mm::virt::PAGE_SIZE) {
                            if (mod::mm::virt::is_page_mapped(pagemap, va)) {
                                mod::mm::virt::unify_page_flags(pagemap, va,
                                                                mod::mm::paging::page_types::USER_READONLY | mod::mm::paging::PAGE_NX);
                            }
                        }
                    }
                }
                break;

            case PT_NULL:
                // Null segment - skip
                break;

            default:
                mod::dbg::log("Segment processing failed");
                mod::dbg::log("Tried to load segment type %d", current_header->p_type);
                hcf();
                break;
        }
    }

    // Load section headers with debug info
    load_section_headers(elf_file, pagemap, pid);

    // Only process relocations for statically-linked binaries.
    // Dynamically-linked ones (PT_INTERP present) have their relocations
    // handled by the dynamic linker (ld.so).
    // ld.so itself (loaded with non-zero loadBase) handles its own via relocateSelf().
    bool has_dynamic_interp = false;
    for (Elf64_Half i = 0; i < elf_file.elf_head.e_phnum; i++) {
        auto* ph = (Elf64_Phdr*)((uint64_t)elf_file.pg_head + (static_cast<uint64_t>(i * elf_file.elf_head.e_phentsize)));
        if (ph->p_type == PT_INTERP) {
            has_dynamic_interp = true;
            break;
        }
    }
    bool const SKIP_RELOCATIONS = has_dynamic_interp || elf_file.load_base != 0;
    if (!SKIP_RELOCATIONS) {
        process_relocations(elf_file, pagemap);
    }

    // Apply final permissions to PT_LOAD segments based on p_flags (after relocations complete).
    // With NX available:
    //  - PF_W set   -> USER (read/write), NX if PF_X not set
    //  - PF_W clear -> USER_READONLY (read-only), NX if PF_X not set
    //
    // Pass 0: Apply writable segment permissions
    // Pass 1: Apply read-only segment permissions
    for (int pass = 0; pass < 2; pass++) {
        for (Elf64_Half i = 0; i < elf_file.elf_head.e_phnum; i++) {
            auto* ph = (Elf64_Phdr*)((uint64_t)elf_file.pg_head + (static_cast<uint64_t>(i * elf_file.elf_head.e_phentsize)));
            if (ph->p_type == PT_LOAD) {
                const bool WRITABLE = (ph->p_flags & PF_W) != 0;
                const bool EXECUTABLE = (ph->p_flags & PF_X) != 0;

                // Pass 0: only writable segments, Pass 1: only read-only segments
                if ((pass == 0 && !WRITABLE) || (pass == 1 && WRITABLE)) {
                    continue;
                }

                uint64_t base_flags = WRITABLE ? mod::mm::paging::page_types::USER : mod::mm::paging::page_types::USER_READONLY;
                if (!EXECUTABLE) {
                    base_flags |= mod::mm::paging::PAGE_NX;
                }

                uint64_t const START = (ph->p_vaddr + elf_file.load_base) & ~(mod::mm::virt::PAGE_SIZE - 1);
                uint64_t const END = (ph->p_vaddr + ph->p_memsz + mod::mm::virt::PAGE_SIZE - 1) & ~(mod::mm::virt::PAGE_SIZE - 1);

                for (uint64_t va = START; va < END; va += mod::mm::virt::PAGE_SIZE) {
                    // In pass 1 (read-only), check if page is already writable and skip it
                    if (pass == 1) {
                        uint64_t const PADDR = mod::mm::virt::translate(pagemap, va);
                        if (PADDR != ker::mod::mm::virt::PADDR_INVALID) {
                            bool skip_page = false;
                            for (Elf64_Half j = 0; j < elf_file.elf_head.e_phnum; j++) {
                                auto* ph2 =
                                    (Elf64_Phdr*)((uint64_t)elf_file.pg_head + (static_cast<uint64_t>(j * elf_file.elf_head.e_phentsize)));
                                if (ph2->p_type == PT_LOAD && (ph2->p_flags & PF_W) != 0) {
                                    uint64_t const WSTART = (ph2->p_vaddr + elf_file.load_base) & ~(mod::mm::virt::PAGE_SIZE - 1);
                                    uint64_t const WEND =
                                        (ph2->p_vaddr + ph2->p_memsz + mod::mm::virt::PAGE_SIZE - 1) & ~(mod::mm::virt::PAGE_SIZE - 1);
                                    if (va >= WSTART && va < WEND) {
                                        skip_page = true;
                                        break;
                                    }
                                }
                            }
                            if (skip_page) {
#ifdef ELF_DEBUG_EXTRA
                                mod::dbg::log("Skipping page 0x%x (overlaps with writable segment)", va);
#endif
                                continue;
                            }
                        }
                    }

#ifdef ELF_DEBUG_EXTRA
                    mod::dbg::log("Setting page 0x%x to flags=0x%x (%s %s)", va, baseFlags, writable ? "WRITE" : "READONLY",
                                  executable ? "EXEC" : "NOEXEC");
#endif
                    mod::mm::virt::unify_page_flags(pagemap, va, base_flags);
                }
#ifdef ELF_DEBUG_EXTRA
                mod::dbg::log("PT_LOAD perms applied: vaddr=[0x%x, 0x%x) flags=0x%x -> %s%s", start, end, ph->p_flags,
                              writable ? "USER" : "USER_READONLY", executable ? "" : "+NX");
#endif
            }
        }
    }

    // Enforce RELRO after relocations: PT_GNU_RELRO pages become read-only.
    // Skip for dynamically-linked binaries and ld.so — ld.so handles RELRO after its own relocations.
    if (!SKIP_RELOCATIONS) {
        for (Elf64_Half i = 0; i < elf_file.elf_head.e_phnum; i++) {
            auto* ph = (Elf64_Phdr*)((uint64_t)elf_file.pg_head + (static_cast<uint64_t>(i * elf_file.elf_head.e_phentsize)));
            if (ph->p_type == PT_GNU_RELRO) {
#ifdef ELF_DEBUG
                mod::dbg::log("Found PT_GNU_RELRO at vaddr=0x%x, memsz=0x%x", ph->p_vaddr, ph->p_memsz);
#endif
                uint64_t const START = ph->p_vaddr + elf_file.load_base;
                uint64_t const END = (ph->p_vaddr + ph->p_memsz + mod::mm::virt::PAGE_SIZE - 1) & ~(mod::mm::virt::PAGE_SIZE - 1);

                // Find .got.plt sections to exclude from RELRO
                const char* section_names =
                    (const char*)((uint64_t)elf_file.se_head +
                                  (static_cast<uint64_t>(elf_file.elf_head.e_shstrndx * elf_file.elf_head.e_shentsize)));
                auto* shstrtab_hdr = (Elf64_Shdr*)((uint64_t)elf_file.se_head +
                                                   (static_cast<uint64_t>(elf_file.elf_head.e_shstrndx * elf_file.elf_head.e_shentsize)));
                section_names = (const char*)((uint64_t)elf_file.base + shstrtab_hdr->sh_offset);

                // Check each page to see if it overlaps with any GOT.PLT sections
                for (uint64_t va = START; va < END; va += mod::mm::virt::PAGE_SIZE) {
                    if (!mod::mm::virt::is_page_mapped(pagemap, va)) {
                        continue;
                    }

                    // Check if this page contains any .got.plt section
                    bool has_got_plt = false;
                    for (size_t section_index = 0; section_index < elf_file.elf_head.e_shnum; section_index++) {
                        auto* section_header = (Elf64_Shdr*)((uint64_t)elf_file.se_head + (section_index * elf_file.elf_head.e_shentsize));
                        const char* section_name = &section_names[section_header->sh_name];

                        if (std::strncmp(section_name, ".got.plt", 8) == 0 && section_header->sh_addr != 0) {
                            uint64_t const GOT_START = section_header->sh_addr + elf_file.load_base;
                            uint64_t const GOT_END = GOT_START + section_header->sh_size;
                            uint64_t const PAGE_END = va + mod::mm::virt::PAGE_SIZE;

                            // Check if GOT.PLT overlaps with this page
                            if (GOT_START < PAGE_END && GOT_END > va) {
                                has_got_plt = true;
#ifdef ELF_DEBUG
                                mod::dbg::log("RELRO: Skipping page 0x%x because it contains .got.plt [0x%x-0x%x)", va, gotStart, gotEnd);
#endif
                                break;
                            }
                        }
                    }

                    if (!has_got_plt) {
                        mod::mm::virt::unify_page_flags(pagemap, va, mod::mm::paging::page_types::USER_READONLY);
                    }
                }
#ifdef ELF_DEBUG
                mod::dbg::log("RELRO enforced for vaddr=[0x%x, 0x%x) (excluding .got.plt pages)", start, end);
#endif
            }
        }
    }  // !skipRelocations

    if (register_special_symbols) {
        // Iterate section headers to find symbol tables
        for (size_t sidx = 0; sidx < elf_file.elf_head.e_shnum; sidx++) {
            auto* section = (Elf64_Shdr*)((uint64_t)elf_file.se_head + (sidx * elf_file.elf_head.e_shentsize));

            if (section->sh_type == SHT_SYMTAB || section->sh_type == SHT_DYNSYM) {
                // Get string table for this symbol table
                auto* strtab = reinterpret_cast<Elf64_Shdr*>(elf_file.base + elf_file.elf_head.e_shoff +
                                                             (static_cast<size_t>(section->sh_link * elf_file.elf_head.e_shentsize)));
                const char* strs = reinterpret_cast<const char*>(elf_file.base + strtab->sh_offset);
                uint64_t const NUM_SYMBOLS = section->sh_size / section->sh_entsize;
                auto* syms = reinterpret_cast<Elf64_Sym*>(elf_file.base + section->sh_offset);

                for (uint64_t si = 0; si < NUM_SYMBOLS; si++) {
                    Elf64_Sym const* sym = &syms[si];
                    const char* sname = strs + sym->st_name;
                    if ((sname == nullptr) || (sname[0] == 0)) {
                        continue;
                    }
                }
            }
        }
    }

// Print debug info for verification
#ifdef ELF_DEBUG
    debug::printDebugInfo(pid);
#endif
    // Return entry point, program header address, and ELF header address for auxv setup
    ElfLoadResult result = {.entry_point = elf_file.elf_head.e_entry + elf_file.load_base,
                            .program_header_addr = program_headers_vaddr,
                            .elf_header_addr = elf_header_vaddr,
                            .program_header_count = elf_file.elf_head.e_phnum,
                            .program_header_ent_size = elf_file.elf_head.e_phentsize};

    // Extract PT_INTERP path if present
    for (Elf64_Half i = 0; i < elf_file.elf_head.e_phnum; i++) {
        auto* ph = (Elf64_Phdr*)((uint64_t)elf_file.pg_head + (static_cast<uint64_t>(i * elf_file.elf_head.e_phentsize)));
        if (ph->p_type == PT_INTERP && ph->p_filesz > 0 && ph->p_filesz < ElfLoadResult::INTERP_PATH_MAX) {
            const char* interp_str = reinterpret_cast<const char*>(elf_file.base + ph->p_offset);
            std::memcpy(result.interp_path, interp_str, ph->p_filesz);
            result.interp_path[ph->p_filesz] = '\0';
            result.has_interp = true;
#ifdef ELF_DEBUG
            mod::dbg::log("PT_INTERP: dynamic linker path = '%s'", result.interpPath);
#endif
            break;
        }
    }

    return result;
}

// Extract TLS information from ELF without fully loading it
auto extract_tls_info(void* elf_data) -> TlsModule {
    TlsModule tls_info = {.tls_base = 0, .tls_size = 0, .tcb_offset = 0};  // Default empty TLS info

#ifdef ELF_DEBUG
    mod::dbg::log("extractTlsInfo: Starting TLS extraction from ELF data at 0x%x", (uint64_t)elfData);
#endif

    // Validate input pointer
    if (elf_data == nullptr) {
        mod::dbg::log("ERROR: extractTlsInfo called with null elfData pointer");
        return tls_info;
    }

    // Parse the ELF to find PT_TLS segment
    ElfFile const ELF_FILE = parse_elf(static_cast<uint8_t*>(elf_data));

    // Check if parsing was successful (parseElf validates magic numbers)
    if (ELF_FILE.elf_head.e_ident[EI_MAG0] != ELFMAG0 || ELF_FILE.elf_head.e_ident[EI_MAG1] != ELFMAG1 ||
        ELF_FILE.elf_head.e_ident[EI_MAG2] != ELFMAG2 || ELF_FILE.elf_head.e_ident[EI_MAG3] != ELFMAG3) {
        mod::dbg::log("ERROR: extractTlsInfo - Invalid ELF magic in parsed data");
        return tls_info;
    }

#ifdef ELF_DEBUG
    mod::dbg::log("extractTlsInfo: ELF has %d program headers", elfFile.elfHead.e_phnum);
#endif

    // Look for PT_TLS segment - use a local pointer instead of modifying elfFile.pgHead
    for (uint32_t i = 0; i < ELF_FILE.elf_head.e_phnum; i++) {
        auto* current_phdr = reinterpret_cast<Elf64_Phdr*>(ELF_FILE.base + ELF_FILE.elf_head.e_phoff +
                                                           (static_cast<size_t>(i * ELF_FILE.elf_head.e_phentsize)));

        if (current_phdr->p_type == PT_TLS) {
            tls_info.tls_size = current_phdr->p_memsz;
            tls_info.tcb_offset = current_phdr->p_memsz;  // TCB goes after TLS data
#ifdef ELF_DEBUG
            mod::dbg::log("extractTlsInfo: Found PT_TLS segment with size %d bytes", tlsInfo.tlsSize);
#endif
            break;
        }
    }

#ifdef ELF_DEBUG
    mod::dbg::log("extractTlsInfo: Returning TLS size %d", tlsInfo.tlsSize);
#endif
    return tls_info;
}

}  // namespace ker::loader::elf
