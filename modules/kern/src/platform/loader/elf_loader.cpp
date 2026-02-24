#include "elf_loader.hpp"

#include <extern/elf.h>

// #define ELF_DEBUG  // Enable ELF loading debug output

#include <cmath>
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

auto headerIsValid(const Elf64_Ehdr& ehdr) -> bool {
    return ehdr.e_ident[EI_CLASS] == ELFCLASS64                  // 64-bit
           && ehdr.e_ident[EI_OSABI] == ELFOSABI_NONE            // System V
           && (ehdr.e_type == ET_EXEC || ehdr.e_type == ET_DYN)  // Executable or PIE
           && (ehdr).e_ident[EI_MAG0] == ELFMAG0                 // Magic 0
           && (ehdr).e_ident[EI_MAG1] == ELFMAG1                 // Magic 1
           && (ehdr).e_ident[EI_MAG2] == ELFMAG2                 // Magic 2
           && (ehdr).e_ident[EI_MAG3] == ELFMAG3;                // Magic 3
}

auto parseElf(uint8_t* base) -> ElfFile {
    ElfFile elf{};
    elf.base = base;

    // Validate base pointer before dereferencing
    if (base == nullptr) {
        mod::dbg::log("ERROR: parseElf called with null base pointer");
        return elf;  // Return empty ElfFile
    }

    // Copy ELF header first and validate magic numbers before using offsets
    elf.elfHead = *(Elf64_Ehdr*)base;

    // Validate ELF magic numbers immediately to catch corruption
    if (elf.elfHead.e_ident[EI_MAG0] != ELFMAG0 || elf.elfHead.e_ident[EI_MAG1] != ELFMAG1 || elf.elfHead.e_ident[EI_MAG2] != ELFMAG2 ||
        elf.elfHead.e_ident[EI_MAG3] != ELFMAG3) {
        mod::dbg::log("ERROR: Invalid ELF magic: 0x%x 0x%x 0x%x 0x%x", elf.elfHead.e_ident[EI_MAG0], elf.elfHead.e_ident[EI_MAG1],
                      elf.elfHead.e_ident[EI_MAG2], elf.elfHead.e_ident[EI_MAG3]);
        return elf;  // Return with invalid header
    }

    // Validate offsets are reasonable before dereferencing
    if (elf.elfHead.e_phoff == 0 || elf.elfHead.e_shoff == 0) {
        mod::dbg::log("ERROR: Invalid ELF offsets - phoff: 0x%x, shoff: 0x%x", elf.elfHead.e_phoff, elf.elfHead.e_shoff);
        return elf;
    }

    elf.pgHead = (Elf64_Phdr*)(base + elf.elfHead.e_phoff);
    elf.seHead = (Elf64_Shdr*)(base + elf.elfHead.e_shoff);
    elf.sctHeadStrTab = (Elf64_Shdr*)(elf.elfHead.e_shoff +                                // Section header offset
                                      (static_cast<Elf64_Off>(elf.elfHead.e_shstrndx       // Section header string table index
                                                              * elf.elfHead.e_shentsize))  // Size of each section header
    );
#ifdef ELF_DEBUG
    // Determine load base for PIE executables
    if (elf.elfHead.e_type == ET_DYN) {
        // For PIE executables, we need to choose a base address
        // For now just use 0x400000
        // FIXME:add some address conflict detection and ASLR
        elf.loadBase = 0;

        mod::dbg::log("Loading PIE executable with base address: 0x%x", elf.loadBase);
    } else {
        // For ET_EXEC, use 0 as there's no relocation needed
        elf.loadBase = 0;
        mod::dbg::log("Loading regular executable (ET_EXEC)");
    }
#else
    elf.loadBase = 0;
#endif

    return elf;
}

void processRelocations(const ElfFile& elf, ker::mod::mm::virt::PageTable* pagemap) {
    // Get section header string table so we can detect .relr sections by name
    Elf64_Shdr* shdrTable = elf.seHead;
    const char* shstr = (const char*)(elf.base + shdrTable[elf.elfHead.e_shstrndx].sh_offset);
    // No per-process resolver stub: we eagerly resolve and enforce RELRO.
    // Find relocation sections (REL and RELA)
    for (size_t i = 0; i < elf.elfHead.e_shnum; i++) {
        auto* sectionHeader = (Elf64_Shdr*)((uint64_t)elf.seHead + (i * elf.elfHead.e_shentsize));

        // Handle the newer SHT_RELR compressed relocation section by name
        const char* secName = nullptr;
        if (elf.elfHead.e_shstrndx < elf.elfHead.e_shnum) {
            secName = shstr + sectionHeader->sh_name;
        }

        if ((secName != nullptr) && ((std::strncmp(secName, ".relr", 5) == 0) || (std::strncmp(secName, ".relr.dyn", 9) == 0))) {
#ifdef ELF_DEBUG
            mod::dbg::log("Processing SHT_RELR (.relr) relocations in section %d (%s)", i, (secName != nullptr) ? secName : "");
#endif

            uint64_t numEntries = sectionHeader->sh_size / sizeof(uint64_t);
            auto* entries = (uint64_t*)(elf.base + sectionHeader->sh_offset);

            uint64_t base = 0;
            for (uint64_t ei = 0; ei < numEntries; ei++) {
                uint64_t ent = entries[ei];
                if ((ent & 1ULL) == 0ULL) {
                    // explicit relocation: this value is an address to relocate
                    base = ent;
                    uint64_t P = base + elf.loadBase;
                    uint64_t paddr = mod::mm::virt::translate(pagemap, P);
                    if (paddr != 0) {
                        auto* physPtr = (uint64_t*)mod::mm::addr::getVirtPointer(paddr);
                        // RELR encodes RELATIVE relocations: add loadBase to the current value
                        *physPtr = *physPtr + elf.loadBase;
                    }
                } else {
                    // bitmask compressed entries: bits set indicate further relocations relative to 'base'
                    uint64_t bitmap = ent & ~1ULL;
                    for (int bit = 0; bit < 63; ++bit) {
                        if ((bitmap & (1ULL << bit)) != 0U) {
                            uint64_t offset = (uint64_t)(bit + 1) * sizeof(uint64_t);
                            uint64_t P = base + offset + elf.loadBase;
                            uint64_t paddr = mod::mm::virt::translate(pagemap, P);
                            if (paddr != 0) {
                                auto* physPtr = (uint64_t*)mod::mm::addr::getVirtPointer(paddr);
                                *physPtr = *physPtr + elf.loadBase;
                            }
                        }
                    }
                }
            }

            continue;
        }
        if (sectionHeader->sh_type == SHT_REL) {
#ifdef ELF_DEBUG
            mod::dbg::log("Processing SHT_REL relocations in section %d", i);
#endif

            uint64_t numRelocations = sectionHeader->sh_size / sizeof(Elf64_Rel);
            auto* relocations = (Elf64_Rel*)(elf.base + sectionHeader->sh_offset);

            for (uint64_t j = 0; j < numRelocations; j++) {
                Elf64_Rel* rel = &relocations[j];
                uint32_t type = ELF64_R_TYPE(rel->r_info);
                uint32_t symIndex = ELF64_R_SYM(rel->r_info);
                uint64_t P = rel->r_offset + elf.loadBase;  // place

                // For REL entries, addend is the current value at P (read from memory at P)
                uint64_t addend = 0;
                uint64_t paddr = mod::mm::virt::translate(pagemap, P);
                if (paddr != 0) {
                    auto* physPtr = (uint64_t*)mod::mm::addr::getVirtPointer(paddr);
                    addend = *physPtr;
                }

                // Resolve symbol value S
                uint64_t S = 0;
                const char* symName = "";
                if (symIndex != 0) {
                    // The relocation section's sh_link tells which symbol table to use
                    if (sectionHeader->sh_link < elf.elfHead.e_shnum) {
                        auto* symTabSec =
                            (Elf64_Shdr*)((uint64_t)elf.seHead + (static_cast<uint64_t>(sectionHeader->sh_link * elf.elfHead.e_shentsize)));
                        auto* syms = (Elf64_Sym*)(elf.base + symTabSec->sh_offset);
                        uint64_t n = symTabSec->sh_size / symTabSec->sh_entsize;
                        // Get string table for symbol names if available
                        const char* symStrs = nullptr;
                        if (symTabSec->sh_link < elf.elfHead.e_shnum) {
                            auto* strtabSec =
                                (Elf64_Shdr*)((uint64_t)elf.seHead + (static_cast<uint64_t>(symTabSec->sh_link * elf.elfHead.e_shentsize)));
                            symStrs = (const char*)(elf.base + strtabSec->sh_offset);
                        }
                        if (symIndex < n) {
                            Elf64_Sym* sym = &syms[symIndex];
                            symName = (symStrs != nullptr) ? (symStrs + sym->st_name) : "";
                            S = sym->st_value;
                            // If the symbol is defined in a section, add loadBase (unless TLS)
                            if (sym->st_shndx < elf.elfHead.e_shnum) {
                                auto* symSec =
                                    (Elf64_Shdr*)((uint64_t)elf.seHead + (static_cast<uint64_t>(sym->st_shndx * elf.elfHead.e_shentsize)));
                                if ((symSec->sh_flags & SHF_TLS) == 0U) {
                                    S += elf.loadBase;
                                }
                            }
                        }
                    } else {
                        // Fallback: search symbol tables if sh_link is invalid
                        for (size_t sidx = 0; sidx < elf.elfHead.e_shnum; sidx++) {
                            auto* sec = (Elf64_Shdr*)((uint64_t)elf.seHead + (sidx * elf.elfHead.e_shentsize));
                            if (sec->sh_type == SHT_SYMTAB || sec->sh_type == SHT_DYNSYM) {
                                auto* syms = (Elf64_Sym*)(elf.base + sec->sh_offset);
                                uint64_t n = sec->sh_size / sec->sh_entsize;
                                if (symIndex < n) {
                                    Elf64_Sym* sym = &syms[symIndex];
                                    // Try to get symbol name from string table
                                    if (sec->sh_link < elf.elfHead.e_shnum) {
                                        auto* strtabSec = (Elf64_Shdr*)((uint64_t)elf.seHead +
                                                                        (static_cast<uint64_t>(sec->sh_link * elf.elfHead.e_shentsize)));
                                        const char* symStrs = (const char*)(elf.base + strtabSec->sh_offset);
                                        symName = (symStrs != nullptr) ? (symStrs + sym->st_name) : "";
                                    }
                                    S = sym->st_value;
                                    if (sym->st_shndx < elf.elfHead.e_shnum) {
                                        auto* symSec = (Elf64_Shdr*)((uint64_t)elf.seHead +
                                                                     (static_cast<uint64_t>(sym->st_shndx * elf.elfHead.e_shentsize)));
                                        if ((symSec->sh_flags & SHF_TLS) == 0U) {
                                            S += elf.loadBase;
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
                switch (type) {
                    case R_X86_64_TPOFF64: {
                        // TLS offset: S + A (symbol TLS offset plus addend)
                        if (paddr != 0) {
                            auto* physPtr = (uint64_t*)mod::mm::addr::getVirtPointer(paddr);
                            *physPtr = S + addend;
                        }
                        break;
                    }
                    case R_X86_64_RELATIVE: {
                        if (paddr != 0) {
                            auto* physPtr = (uint64_t*)mod::mm::addr::getVirtPointer(paddr);
                            *physPtr = addend + elf.loadBase;
                        }
                        break;
                    }
                    case R_X86_64_GLOB_DAT:
                    case R_X86_64_JUMP_SLOT: {
                        // Ensure the target virtual address is mapped and writable for GOT/PLT writes.
                        if (paddr == 0) {
                            uint64_t targetPage = P & ~(mod::mm::virt::PAGE_SIZE - 1);
#ifdef ELF_DEBUG
                            mod::dbg::log("GOT/PLT entry at 0x%x not mapped; allocating page 0x%x", P, targetPage);
#endif
                            auto newPaddr = (uint64_t)mod::mm::phys::pageAlloc();
                            if (newPaddr != 0) {
                                auto physPtrPage = (uint64_t)mod::mm::addr::getPhysPointer(newPaddr);
                                mod::mm::virt::mapPage(pagemap, targetPage, physPtrPage, mod::mm::paging::pageTypes::USER);
                                paddr = mod::mm::virt::translate(pagemap, P);
                            }
                        }

                        if (paddr != 0) {
                            auto* physPtr = (uint64_t*)mod::mm::addr::getVirtPointer(paddr);
                            if (S == 0) {
                                mod::dbg::log(
                                    "ERROR: Unresolved symbol '%s' (idx=%d) for relocation at P=0x%x (type=%d). Writing 0 to catch fault.",
                                    symName, symIndex, P, type);
                            }
                            uint64_t writeVal = S + addend;
                            *physPtr = writeVal;
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
                        if (paddr != 0) {
                            auto* physPtr = (uint64_t*)mod::mm::addr::getVirtPointer(paddr);
                            *physPtr = S + addend;
                        }
                        break;
                    }
                    case R_X86_64_PC32:
                    case R_X86_64_PLT32: {
                        // 32-bit PC-relative
                        if (paddr != 0) {
                            auto* physPtr32 = (uint32_t*)mod::mm::addr::getVirtPointer(paddr);
                            uint64_t P64 = P;
                            auto value = (int64_t)(S + (int64_t)addend - (int64_t)P64);
                            *physPtr32 = (uint32_t)value;
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
        } else if (sectionHeader->sh_type == SHT_RELA) {
#ifdef ELF_DEBUG
            mod::dbg::log("Processing SHT_RELA relocations in section %d", i);
#endif

            uint64_t numRelocations = sectionHeader->sh_size / sizeof(Elf64_Rela);
            auto* relocations = (Elf64_Rela*)(elf.base + sectionHeader->sh_offset);

            for (uint64_t j = 0; j < numRelocations; j++) {
                Elf64_Rela* rel = &relocations[j];
                uint32_t type = ELF64_R_TYPE(rel->r_info);
                uint32_t symIndex = ELF64_R_SYM(rel->r_info);
                uint64_t P = rel->r_offset + elf.loadBase;  // place
                int64_t addend = rel->r_addend;

                // Resolve symbol value S
                uint64_t S = 0;
                const char* symName = "";
                if (symIndex != 0) {
                    if (sectionHeader->sh_link < elf.elfHead.e_shnum) {
                        auto* symTabSec =
                            (Elf64_Shdr*)((uint64_t)elf.seHead + (static_cast<uint64_t>(sectionHeader->sh_link * elf.elfHead.e_shentsize)));
                        auto* syms = (Elf64_Sym*)(elf.base + symTabSec->sh_offset);
                        uint64_t n = symTabSec->sh_size / symTabSec->sh_entsize;
                        const char* symStrs = nullptr;
                        if (symTabSec->sh_link < elf.elfHead.e_shnum) {
                            auto* strtabSec =
                                (Elf64_Shdr*)((uint64_t)elf.seHead + (static_cast<uint64_t>(symTabSec->sh_link * elf.elfHead.e_shentsize)));
                            symStrs = (const char*)(elf.base + strtabSec->sh_offset);
                        }

                        if (symIndex < n) {
                            Elf64_Sym* sym = &syms[symIndex];
                            symName = (symStrs != nullptr) ? (symStrs + sym->st_name) : "";
                            S = sym->st_value;
                            if (sym->st_shndx < elf.elfHead.e_shnum) {
                                auto* symSec =
                                    (Elf64_Shdr*)((uint64_t)elf.seHead + (static_cast<uint64_t>(sym->st_shndx * elf.elfHead.e_shentsize)));
                                if ((symSec->sh_flags & SHF_TLS) == 0U) {
                                    S += elf.loadBase;
                                }
                            }
                        }
                    } else {
                        for (size_t sidx = 0; sidx < elf.elfHead.e_shnum; sidx++) {
                            auto* sec = (Elf64_Shdr*)((uint64_t)elf.seHead + (sidx * elf.elfHead.e_shentsize));
                            if (sec->sh_type == SHT_SYMTAB || sec->sh_type == SHT_DYNSYM) {
                                auto* syms = (Elf64_Sym*)(elf.base + sec->sh_offset);
                                uint64_t n = sec->sh_size / sec->sh_entsize;
                                if (symIndex < n) {
                                    Elf64_Sym* sym = &syms[symIndex];
                                    // Try to get symbol name from string table
                                    if (sec->sh_link < elf.elfHead.e_shnum) {
                                        auto* strtabSec = (Elf64_Shdr*)((uint64_t)elf.seHead +
                                                                        (static_cast<uint64_t>(sec->sh_link * elf.elfHead.e_shentsize)));
                                        const char* symStrs = (const char*)(elf.base + strtabSec->sh_offset);
                                        symName = (symStrs != nullptr) ? (symStrs + sym->st_name) : "";
                                    }
                                    S = sym->st_value;
                                    if (sym->st_shndx < elf.elfHead.e_shnum) {
                                        auto* symSec = (Elf64_Shdr*)((uint64_t)elf.seHead +
                                                                     (static_cast<uint64_t>(sym->st_shndx * elf.elfHead.e_shentsize)));
                                        if ((symSec->sh_flags & SHF_TLS) == 0U) {
                                            S += elf.loadBase;
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

                switch (type) {
                    case R_X86_64_TPOFF64: {
                        int64_t tlsOffset = addend;  // use addend if present
                        if (paddr != 0) {
                            auto* physPtr = (uint64_t*)mod::mm::addr::getVirtPointer(paddr);
                            *physPtr = (uint64_t)tlsOffset;
                        }
                        break;
                    }
                    case R_X86_64_RELATIVE: {
                        if (paddr != 0) {
                            auto* physPtr = (uint64_t*)mod::mm::addr::getVirtPointer(paddr);
                            *physPtr = elf.loadBase + (uint64_t)addend;
                        }
                        break;
                    }
                    case R_X86_64_GLOB_DAT:
                    case R_X86_64_JUMP_SLOT: {
                        if (paddr == 0) {
                            uint64_t targetPage = P & ~(mod::mm::virt::PAGE_SIZE - 1);
#ifdef ELF_DEBUG
                            mod::dbg::log("GOT/PLT entry at 0x%x not mapped; allocating page 0x%x", P, targetPage);
#endif
                            auto newPaddr = (uint64_t)mod::mm::phys::pageAlloc();
                            if (newPaddr != 0) {
                                auto physPtr = (uint64_t)mod::mm::addr::getPhysPointer(newPaddr);
                                mod::mm::virt::mapPage(pagemap, targetPage, physPtr, mod::mm::paging::pageTypes::USER);
                                paddr = mod::mm::virt::translate(pagemap, P);
                            }
                        }

                        if (paddr != 0) {
                            auto* physPtr = (uint64_t*)mod::mm::addr::getVirtPointer(paddr);
                            if (S == 0) {
                                mod::dbg::log(
                                    "ERROR: Unresolved symbol '%s' (idx=%d) for relocation at P=0x%x (type=%d). Writing 0 to catch fault.",
                                    symName, symIndex, P, type);
                            }
                            uint64_t writeVal = S + (uint64_t)addend;
                            *physPtr = writeVal;
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
                        if (paddr != 0) {
                            auto* physPtr = (uint64_t*)mod::mm::addr::getVirtPointer(paddr);
                            *physPtr = S + (uint64_t)addend;
                        }
                        break;
                    }
                    case R_X86_64_PC32:
                    case R_X86_64_PLT32: {
                        if (paddr != 0) {
                            auto* physPtr32 = (uint32_t*)mod::mm::addr::getVirtPointer(paddr);
                            uint64_t P64 = P;
                            auto value = (int64_t)(S + addend - (int64_t)P64);
                            *physPtr32 = (uint32_t)value;
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

void processReadOnlySegment(Elf64_Phdr* segment, ker::mod::mm::virt::PageTable* pagemap, uint64_t pid) {
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

void registerEhFrame(void* base, uint64_t size) {
    (void)base;
    (void)size;
    // TODO: Register the .eh_frame section for exception handling
}

void processEhFrameSegment(Elf64_Phdr* segment, ker::mod::mm::virt::PageTable* pagemap, uint64_t pid) {
    (void)pid;
    // .eh_frame(.hdr) lies within PT_LOAD in well-formed binaries; we should not remap
    // using file offsets. Just ensure pages remain readable (optionally read-only) and
    // register the region for unwinding.

    uint64_t vaddr = segment->p_vaddr;
    uint64_t end = (segment->p_vaddr + segment->p_memsz + mod::mm::virt::PAGE_SIZE - 1) & ~(mod::mm::virt::PAGE_SIZE - 1);
    for (uint64_t va = vaddr; va < end; va += mod::mm::virt::PAGE_SIZE) {
        if (mod::mm::virt::isPageMapped(pagemap, va)) {
            mod::mm::virt::unifyPageFlags(pagemap, va, mod::mm::paging::pageTypes::USER_READONLY);
        }
    }

    // Register the .eh_frame section for exception handling
    registerEhFrame((void*)(vaddr), segment->p_memsz);
}

void loadSegment(uint8_t* elfBase, ker::mod::mm::virt::PageTable* pagemap, Elf64_Phdr* programHeader, uint64_t pageNo,
                 uint64_t baseOffset) {
    // Compute aligned virtual address for this page and in-page offset of the segment start
    const uint64_t segStartVA = programHeader->p_vaddr + baseOffset;
    const uint64_t firstPageOffset = segStartVA & (mod::mm::virt::PAGE_SIZE - 1);
    const uint64_t alignedStartVA = segStartVA & ~(mod::mm::virt::PAGE_SIZE - 1);
    const uint64_t pageVA = alignedStartVA + (pageNo * mod::mm::virt::PAGE_SIZE);

    // Additional validation for PIE executables
    if (baseOffset != 0 && pageVA < mod::mm::virt::PAGE_SIZE) {
        ker::mod::io::serial::write("PIE program trying to map too low address 0x%x\n", pageVA);
        return;
    }

    // Map the page at a page-aligned virtual address
    bool alreadyMapped = mod::mm::virt::isPageMapped(pagemap, pageVA);
    // HHDM-mapped pointer to the backing physical page so we can write contents
    uint64_t pageHhdmPtr = 0;
    if (alreadyMapped) {
        mod::mm::virt::unifyPageFlags(pagemap, pageVA, mod::mm::paging::pageTypes::USER);
        uint64_t paddr = mod::mm::virt::translate(pagemap, pageVA);
        pageHhdmPtr = (uint64_t)mod::mm::addr::getVirtPointer(paddr);
    } else {
        // Allocate a new physical page; allocator returns an HHDM pointer to the page memory
        auto newPageHhdmPtr = (uint64_t)mod::mm::phys::pageAlloc();
        pageHhdmPtr = newPageHhdmPtr;
        // Map using the physical address corresponding to that HHDM pointer
        mod::mm::virt::mapPage(pagemap, pageVA, (mod::mm::addr::paddr_t)mod::mm::addr::getPhysPointer(newPageHhdmPtr),
                               mod::mm::paging::pageTypes::USER);
        // Zero freshly mapped page to handle bss/holes
        memset((void*)pageHhdmPtr, 0, mod::mm::virt::PAGE_SIZE);
    }

    // Determine source file offset and destination in-page offset for this page
    const uint64_t dstInPage = (pageNo == 0) ? firstPageOffset : 0;
    const uint64_t roomInPage = mod::mm::virt::PAGE_SIZE - dstInPage;

    // How many bytes have already been copied before this page?
    uint64_t bytesBeforeThisPage = 0;
    if (pageNo == 0) {
        bytesBeforeThisPage = 0;
    } else {
        // First (partial) page accounts for (PAGE_SIZE - firstPageOffset), then full pages
        bytesBeforeThisPage = (mod::mm::virt::PAGE_SIZE - firstPageOffset) + ((pageNo - 1) * mod::mm::virt::PAGE_SIZE);
    }

    // If we've consumed the entire file content of the segment, nothing to copy for this page
    if (bytesBeforeThisPage >= programHeader->p_filesz) {
        return;
    }

    // Compute how much to copy from this page
    uint64_t remainingInFile = programHeader->p_filesz - bytesBeforeThisPage;
    uint64_t copySize = remainingInFile < roomInPage ? remainingInFile : roomInPage;

    // Copy from ELF file to destination page at the correct in-page offset
    const uint64_t srcOffset = programHeader->p_offset + bytesBeforeThisPage;
    memcpy((void*)(pageHhdmPtr + dstInPage), elfBase + srcOffset, copySize);
}

void loadSectionHeaders(const ElfFile& elf, ker::mod::mm::virt::PageTable* pagemap, const uint64_t& pid) {
    (void)pid;
    auto* scnHeadTable = (Elf64_Shdr*)((uint64_t)elf.base                                      // Base address of the ELF file
                                       + elf.elfHead.e_shoff                                   // Section header offset
                                       + (static_cast<uint64_t>(elf.elfHead.e_shstrndx         // Section header string table index
                                                                * elf.elfHead.e_shentsize)));  // Size of each section header

    const char* sectionNames = (const char*)(elf.base + scnHeadTable->sh_offset);

    // Allocate memory for section headers to preserve them for debugging
    auto sectionHeadersSize = static_cast<uint64_t>(elf.elfHead.e_shnum * elf.elfHead.e_shentsize);
    uint64_t sectionHeadersPages = PAGE_ALIGN_UP(sectionHeadersSize) / mod::mm::virt::PAGE_SIZE;
    constexpr uint64_t sectionHeadersVaddr = 0x700000000000ULL;  // High memory area for debug info

    uint64_t sectionHeadersPhysPtr = 0;  // Make this accessible throughout function
    for (uint64_t i = 0; i < sectionHeadersPages; i++) {
        auto paddr = (uint64_t)mod::mm::phys::pageAlloc();
        if (paddr != 0) {
            auto physPtr = (uint64_t)mod::mm::addr::getPhysPointer(paddr);
            mod::mm::virt::mapPage(pagemap, sectionHeadersVaddr + (i * mod::mm::virt::PAGE_SIZE), physPtr,
                                   mod::mm::paging::pageTypes::USER);

            // Zero the page
            memset((void*)paddr, 0, mod::mm::virt::PAGE_SIZE);

            // Remember first page for copying
            if (i == 0) {
                sectionHeadersPhysPtr = paddr;
            }
        }
    }

    // Copy section headers to allocated physical memory
    if (sectionHeadersPhysPtr != 0) {
        memcpy((void*)sectionHeadersPhysPtr, elf.seHead, sectionHeadersSize);
    }
    debug::setSectionHeaders(pid, (Elf64_Shdr*)sectionHeadersVaddr, sectionHeadersVaddr, elf.elfHead.e_shnum);

    // Allocate memory for string table
    uint64_t stringTableSize = scnHeadTable->sh_size;
    uint64_t stringTablePages = PAGE_ALIGN_UP(stringTableSize) / mod::mm::virt::PAGE_SIZE;
    uint64_t stringTableVaddr = 0x700000201000ULL;  // After section headers

    uint64_t stringTablePhysPtr = 0;
    for (uint64_t i = 0; i < stringTablePages; i++) {
        auto paddr = (uint64_t)mod::mm::phys::pageAlloc();
        if (paddr != 0) {
            auto physPtr = (uint64_t)mod::mm::addr::getPhysPointer(paddr);
            mod::mm::virt::mapPage(pagemap, stringTableVaddr + (i * mod::mm::virt::PAGE_SIZE), physPtr, mod::mm::paging::pageTypes::USER);

            // Zero the page
            memset((void*)paddr, 0, mod::mm::virt::PAGE_SIZE);

            // Remember first page for copying
            if (i == 0) {
                stringTablePhysPtr = paddr;
            }
        }
    }

    // Copy string table to allocated physical memory
    if (stringTablePhysPtr != 0) {
        memcpy((void*)stringTablePhysPtr, sectionNames, stringTableSize);
    }
    debug::setStringTable(pid, (const char*)stringTableVaddr, stringTableVaddr, stringTableSize);

    for (size_t sectionIndex = 0; sectionIndex < elf.elfHead.e_shnum; sectionIndex++) {
        auto* sectionHeader = (Elf64_Shdr*)((uint64_t)elf.seHead + (sectionIndex * elf.elfHead.e_shentsize));
        const char* sectionName = &sectionNames[sectionHeader->sh_name];
#ifdef ELF_DEBUG
        mod::dbg::log("Section name: %s", sectionName);
#endif
        (void)sectionName;

        // Register sections for debugging without re-mapping any PT_LOAD-backed content
        if (sectionHeader->sh_type == SHT_PROGBITS && sectionHeader->sh_size > 0) {
            // If the section already has a virtual address, it is covered by PT_LOAD; just record it
            if (sectionHeader->sh_addr != 0) {
                uint64_t sectionVaddr = sectionHeader->sh_addr + elf.loadBase;
                uint64_t firstPaddr = mod::mm::virt::translate(pagemap, sectionVaddr);
                debug::addDebugSection(pid, sectionName, sectionVaddr, firstPaddr, sectionHeader->sh_size, sectionHeader->sh_offset,
                                       sectionHeader->sh_type);
#ifdef ELF_DEBUG
                mod::dbg::log("Added debug section: %s, vaddr=%x, paddr=%x, size=%x", sectionName, sectionVaddr, firstPaddr,
                              sectionHeader->sh_size);
#endif
            } else if (std::strncmp(sectionName, ".debug_", 7) == 0) {
                // Pure debug sections (no sh_addr) - allocate in high memory so debuggers can read them
                uint64_t debugPages = PAGE_ALIGN_UP(sectionHeader->sh_size) / mod::mm::virt::PAGE_SIZE;
                uint64_t debugVaddr = 0x600000000000ULL + (sectionIndex * 0x1000000);

                uint64_t remainingSize = sectionHeader->sh_size;
                uint64_t sourceOffset = 0;
                bool allocationSuccess = true;
                for (uint64_t i = 0; i < debugPages && remainingSize > 0; i++) {
                    auto debugPaddr = (uint64_t)mod::mm::phys::pageAlloc();
                    if (debugPaddr != 0) {
                        auto physPtr = (uint64_t)mod::mm::addr::getPhysPointer(debugPaddr);
                        mod::mm::virt::mapPage(pagemap, debugVaddr + (i * mod::mm::virt::PAGE_SIZE), physPtr,
                                               mod::mm::paging::pageTypes::USER);
                        memset((void*)debugPaddr, 0, mod::mm::virt::PAGE_SIZE);
                        uint64_t copySize = (remainingSize > mod::mm::virt::PAGE_SIZE) ? mod::mm::virt::PAGE_SIZE : remainingSize;
                        memcpy((void*)debugPaddr, elf.base + sectionHeader->sh_offset + sourceOffset, copySize);
                        remainingSize -= copySize;
                        sourceOffset += copySize;
                    } else {
                        allocationSuccess = false;
                        break;
                    }
                }

                if (allocationSuccess) {
                    if (sectionHeadersPhysPtr != 0) {
                        auto* mappedSectionHeader = (Elf64_Shdr*)(sectionHeadersPhysPtr + (sectionIndex * elf.elfHead.e_shentsize));
                        mappedSectionHeader->sh_addr = debugVaddr;
                    }
                    debug::addDebugSection(pid, sectionName, debugVaddr, debugVaddr, sectionHeader->sh_size, sectionHeader->sh_offset,
                                           sectionHeader->sh_type);
#ifdef ELF_DEBUG
                    mod::dbg::log("Allocated debug section %s at vaddr: %x, size: %x", sectionName, debugVaddr, sectionHeader->sh_size);
#endif
                } else {
#ifdef ELF_DEBUG
                    mod::dbg::log("Failed to allocate memory for debug section %s", sectionName);
#endif
                }
            }
        }
        // Additionally, record GOT sections in debug info registry for diagnostics (no remapping or copying here)
        if (((std::strncmp(sectionName, ".got", 4) == 0) || (std::strncmp(sectionName, ".got.plt", 8) == 0)) &&
            sectionHeader->sh_addr != 0 && sectionHeader->sh_size > 0) {
            uint64_t sectionVaddr = sectionHeader->sh_addr + elf.loadBase;
            uint64_t firstPaddr = mod::mm::virt::translate(pagemap, sectionVaddr);
            debug::addDebugSection(pid, sectionName, sectionVaddr, firstPaddr, sectionHeader->sh_size, sectionHeader->sh_offset,
                                   sectionHeader->sh_type);
#ifdef ELF_DEBUG
            mod::dbg::log("Recorded GOT-like section %s at vaddr: %x, paddr: %x, size: %x", sectionName, sectionVaddr, firstPaddr,
                          sectionHeader->sh_size);
#endif
        }
    }
}
}  // namespace

auto loadElf(ElfFile* elf, ker::mod::mm::virt::PageTable* pagemap, uint64_t pid, const char* processName, bool registerSpecialSymbols)
    -> ElfLoadResult {
    // Validate input pointer
    if (elf == nullptr) {
        mod::dbg::log("ERROR: loadElf called with null ELF pointer (pid=%d)", pid);
        return {.entryPoint = 0, .programHeaderAddr = 0, .elfHeaderAddr = 0};
    }

    ElfFile elfFile = parseElf((uint8_t*)elf);

    if (!headerIsValid(elfFile.elfHead)) {
        mod::dbg::log("ERROR: Invalid ELF header (pid=%d)", pid);
        mod::dbg::log("  ELF base: 0x%p", elf);
        mod::dbg::log("  e_ident: [0x%x 0x%x 0x%x 0x%x] (expected [0x%x 0x%x 0x%x 0x%x])", elfFile.elfHead.e_ident[EI_MAG0],
                      elfFile.elfHead.e_ident[EI_MAG1], elfFile.elfHead.e_ident[EI_MAG2], elfFile.elfHead.e_ident[EI_MAG3], ELFMAG0,
                      ELFMAG1, ELFMAG2, ELFMAG3);
        mod::dbg::log("  e_ident[EI_CLASS]: 0x%x (expected ELFCLASS64=0x%x)", elfFile.elfHead.e_ident[EI_CLASS], ELFCLASS64);
        mod::dbg::log("  e_type: 0x%x (expected ET_EXEC=0x%x or ET_DYN=0x%x)", elfFile.elfHead.e_type, ET_EXEC, ET_DYN);
        mod::dbg::log("  e_phoff: 0x%x, e_shoff: 0x%x", elfFile.elfHead.e_phoff, elfFile.elfHead.e_shoff);
        return {.entryPoint = 0, .programHeaderAddr = 0, .elfHeaderAddr = 0};
    }

    // Register this process for debugging
    debug::registerProcess(pid, processName, (uint64_t)elfFile.base, elfFile.elfHead.e_entry + elfFile.loadBase);

    // Filter program headers: only include PT_PHDR, PT_DYNAMIC, PT_TLS, PT_INTERP
    // PT_DYNAMIC is CRITICAL for the dynamic linker to find .dynamic section
    std::vector<Elf64_Phdr> filteredHeaders;
    for (Elf64_Half i = 0; i < elfFile.elfHead.e_phnum; i++) {
        auto* ph = (Elf64_Phdr*)((uint64_t)elfFile.pgHead + (static_cast<uint64_t>(i * elfFile.elfHead.e_phentsize)));
        if (ph->p_type == PT_PHDR || ph->p_type == PT_DYNAMIC || ph->p_type == PT_TLS || ph->p_type == PT_INTERP) {
            filteredHeaders.push_back(*ph);
        }
    }

    // Allocate and load ELF header and program headers at address 0x1000
    // ELF header at 0x1000, program headers right after
    constexpr uint64_t elfHeaderVaddr = 0x1000;  // Address 0x1000 in process space
    constexpr uint64_t programHeadersOffsetInHeader = sizeof(Elf64_Ehdr);
    uint64_t programHeadersVaddr = elfHeaderVaddr + programHeadersOffsetInHeader;

    auto programHeadersSize = static_cast<uint64_t>(filteredHeaders.size() * elfFile.elfHead.e_phentsize);
    uint64_t totalHeadersSize = sizeof(Elf64_Ehdr) + programHeadersSize;
    uint64_t totalHeadersPages = PAGE_ALIGN_UP(totalHeadersSize) / mod::mm::virt::PAGE_SIZE;

    // Allocate physical pages for both ELF and program headers
    std::vector<uint64_t> headerPhysAddrs;
    for (uint64_t i = 0; i < totalHeadersPages; i++) {
        auto paddr = (uint64_t)mod::mm::phys::pageAlloc();
        if (paddr == 0) {
            mod::dbg::log("ERROR: Failed to allocate physical page for headers");
            return {.entryPoint = 0, .programHeaderAddr = 0, .elfHeaderAddr = 0};
        }
        mod::mm::virt::mapPage(pagemap, elfHeaderVaddr + (i * mod::mm::virt::PAGE_SIZE), (uint64_t)mod::mm::addr::getPhysPointer(paddr),
                               mod::mm::paging::pageTypes::USER);
        headerPhysAddrs.push_back(paddr);
    }

    // Copy ELF header to the first page and update e_phnum to reflect filtered headers
    {
        auto* headerPtr = (Elf64_Ehdr*)headerPhysAddrs[0];
        memcpy(headerPtr, &elfFile.elfHead, sizeof(Elf64_Ehdr));
        // Update e_phoff to point to where we actually placed the program headers
        headerPtr->e_phoff = programHeadersOffsetInHeader;
        // Update e_phnum to reflect only the filtered headers
        headerPtr->e_phnum = static_cast<Elf64_Half>(filteredHeaders.size());
    }

    // Copy filtered program headers right after ELF header
    {
        uint64_t destOffset = programHeadersOffsetInHeader;
        for (auto& filteredHeader : filteredHeaders) {
            uint64_t headerSize = elfFile.elfHead.e_phentsize;
            for (uint64_t i = 0; i < headerSize; i++) {
                uint64_t pageIdx = destOffset / mod::mm::virt::PAGE_SIZE;
                uint64_t offsetInPage = destOffset % mod::mm::virt::PAGE_SIZE;

                if (pageIdx >= headerPhysAddrs.size()) {
                    mod::dbg::log("ERROR: Program header offset exceeds allocated pages");
                    return {.entryPoint = 0, .programHeaderAddr = 0, .elfHeaderAddr = 0};
                }

                uint8_t* destPtr = (uint8_t*)headerPhysAddrs[pageIdx] + offsetInPage;
                uint8_t* srcPtr = (uint8_t*)&filteredHeader + i;
                *destPtr = *srcPtr;
                destOffset++;
            }
        }
    }

    debug::setProgramHeaders(pid, (Elf64_Phdr*)programHeadersVaddr, programHeadersVaddr, static_cast<uint16_t>(filteredHeaders.size()));
    for (Elf64_Half i = 0; i < elfFile.elfHead.e_phnum; i++) {
        auto* currentHeader = (Elf64_Phdr*)((uint64_t)elfFile.pgHead + (static_cast<uint64_t>(i * elfFile.elfHead.e_phentsize)));

        switch (currentHeader->p_type) {
            case PT_GNU_STACK:
                // GNU_STACK segment presence indicates whether stack is executable; record as a debug section
#ifdef ELF_DEBUG
                mod::dbg::log("Found PT_GNU_STACK at vaddr=0x%x, flags=0x%x", currentHeader->p_vaddr, currentHeader->p_flags);
#endif
                debug::addDebugSection(pid, "PT_GNU_STACK", currentHeader->p_vaddr + elfFile.loadBase, currentHeader->p_offset,
                                       currentHeader->p_memsz, currentHeader->p_offset, currentHeader->p_type);
                break;
            case PT_TLS:
                // Found TLS segment - store its information
                elfFile.tlsInfo.tlsBase = currentHeader->p_vaddr + elfFile.loadBase;
                elfFile.tlsInfo.tlsSize = currentHeader->p_memsz;
                elfFile.tlsInfo.tcbOffset = currentHeader->p_memsz;  // TCB goes after TLS data
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
                    uint64_t segEnd = currentHeader->p_vaddr + currentHeader->p_memsz;
                    uint64_t startPageAddr = currentHeader->p_vaddr & ~(mod::mm::virt::PAGE_SIZE - 1);
                    uint64_t endPageAddr = (segEnd + mod::mm::virt::PAGE_SIZE - 1) & ~(mod::mm::virt::PAGE_SIZE - 1);
                    size_t num_pages = (endPageAddr - startPageAddr) / mod::mm::virt::PAGE_SIZE;
#ifdef ELF_DEBUG
                    mod::dbg::log("Calculated pages: start_page=0x%x, end_page=0x%x, num_pages=%zu", startPageAddr, endPageAddr, num_pages);
#endif
                    for (uint64_t j = 0; j < num_pages; j++) {
                        loadSegment(elfFile.base, pagemap, currentHeader, j, elfFile.loadBase);
                    }
                }
                break;

            case PT_GNU_RELRO:
#ifdef ELF_DEBUG
                mod::dbg::log("Found PT_GNU_RELRO segment at vaddr=0x%x, size=0x%x", currentHeader->p_vaddr, currentHeader->p_memsz);
#endif
                // Read-only relocation segment
                processReadOnlySegment(currentHeader, pagemap, pid);
                break;

            case PT_GNU_EH_FRAME:
#ifdef ELF_DEBUG
                mod::dbg::log("Found PT_GNU_EH_FRAME segment at vaddr=0x%x, size=0x%x", currentHeader->p_vaddr, currentHeader->p_memsz);
#endif
                // Exception handling frame segment
                processEhFrameSegment(currentHeader, pagemap, pid);
                break;

            case PT_PHDR:
                // Program header table - informational only, no loading needed
#ifdef ELF_DEBUG
                mod::dbg::log("PT_PHDR segment skipped (informational only)");
#endif
                break;

            case PT_INTERP:
                mod::dbg::log("WARN: PT_INTERP skipped FIXME!");
                break;

            case PT_DYNAMIC:
                // TODO: should be fine since mlibc loads these
                // mod::dbg::log("WARN: PT_DYNAMIC skipped FIXME!");
                break;

            case PT_NOTE:
                mod::dbg::log("WARN: PT_NOTE skipped FIXME!");
                break;

            case PT_NULL:
                // Null segment - skip
                break;

            default:
                mod::dbg::log("Segment processing failed");
                mod::dbg::log("Tried to load segment type %d", currentHeader->p_type);
                hcf();
                break;
        }
    }

    // Load section headers with debug info
    loadSectionHeaders(elfFile, pagemap, pid);

    // Process relocations AFTER loading all segments
    processRelocations(elfFile, pagemap);

    // Apply final permissions to PT_LOAD segments based on p_flags (after relocations complete).
    // With NX available:
    //  - PF_W set   -> USER (read/write), NX if PF_X not set
    //  - PF_W clear -> USER_READONLY (read-only), NX if PF_X not set
    //
    // Pass 0: Apply writable segment permissions
    // Pass 1: Apply read-only segment permissions
    for (int pass = 0; pass < 2; pass++) {
        for (Elf64_Half i = 0; i < elfFile.elfHead.e_phnum; i++) {
            auto* ph = (Elf64_Phdr*)((uint64_t)elfFile.pgHead + (static_cast<uint64_t>(i * elfFile.elfHead.e_phentsize)));
            if (ph->p_type == PT_LOAD) {
                const bool writable = (ph->p_flags & PF_W) != 0;
                const bool executable = (ph->p_flags & PF_X) != 0;

                // Pass 0: only writable segments, Pass 1: only read-only segments
                if ((pass == 0 && !writable) || (pass == 1 && writable)) {
                    continue;
                }

                uint64_t baseFlags = writable ? mod::mm::paging::pageTypes::USER : mod::mm::paging::pageTypes::USER_READONLY;
                if (!executable) {
                    baseFlags |= mod::mm::paging::PAGE_NX;
                }

                uint64_t start = (ph->p_vaddr + elfFile.loadBase) & ~(mod::mm::virt::PAGE_SIZE - 1);
                uint64_t end = (ph->p_vaddr + ph->p_memsz + mod::mm::virt::PAGE_SIZE - 1) & ~(mod::mm::virt::PAGE_SIZE - 1);

                for (uint64_t va = start; va < end; va += mod::mm::virt::PAGE_SIZE) {
                    // In pass 1 (read-only), check if page is already writable and skip it
                    if (pass == 1) {
                        uint64_t paddr = mod::mm::virt::translate(pagemap, va);
                        if (paddr != 0) {
                            bool skipPage = false;
                            for (Elf64_Half j = 0; j < elfFile.elfHead.e_phnum; j++) {
                                auto* ph2 =
                                    (Elf64_Phdr*)((uint64_t)elfFile.pgHead + (static_cast<uint64_t>(j * elfFile.elfHead.e_phentsize)));
                                if (ph2->p_type == PT_LOAD && (ph2->p_flags & PF_W) != 0) {
                                    uint64_t wstart = (ph2->p_vaddr + elfFile.loadBase) & ~(mod::mm::virt::PAGE_SIZE - 1);
                                    uint64_t wend =
                                        (ph2->p_vaddr + ph2->p_memsz + mod::mm::virt::PAGE_SIZE - 1) & ~(mod::mm::virt::PAGE_SIZE - 1);
                                    if (va >= wstart && va < wend) {
                                        skipPage = true;
                                        break;
                                    }
                                }
                            }
                            if (skipPage) {
#ifdef ELF_DEBUG
                                mod::dbg::log("Skipping page 0x%x (overlaps with writable segment)", va);
#endif
                                continue;
                            }
                        }
                    }

#ifdef ELF_DEBUG
                    mod::dbg::log("Setting page 0x%x to flags=0x%x (%s %s)", va, baseFlags, writable ? "WRITE" : "READONLY",
                                  executable ? "EXEC" : "NOEXEC");
#endif
                    mod::mm::virt::unifyPageFlags(pagemap, va, baseFlags);
                }
#ifdef ELF_DEBUG
                mod::dbg::log("PT_LOAD perms applied: vaddr=[0x%x, 0x%x) flags=0x%x -> %s%s", start, end, ph->p_flags,
                              writable ? "USER" : "USER_READONLY", executable ? "" : "+NX");
#endif
            }
        }
    }

    // Enforce RELRO after relocations: PT_GNU_RELRO pages become read-only
    for (Elf64_Half i = 0; i < elfFile.elfHead.e_phnum; i++) {
        auto* ph = (Elf64_Phdr*)((uint64_t)elfFile.pgHead + (static_cast<uint64_t>(i * elfFile.elfHead.e_phentsize)));
        if (ph->p_type == PT_GNU_RELRO) {
#ifdef ELF_DEBUG
            mod::dbg::log("Found PT_GNU_RELRO at vaddr=0x%x, memsz=0x%x", ph->p_vaddr, ph->p_memsz);
#endif
            uint64_t start = ph->p_vaddr + elfFile.loadBase;
            uint64_t end = (ph->p_vaddr + ph->p_memsz + mod::mm::virt::PAGE_SIZE - 1) & ~(mod::mm::virt::PAGE_SIZE - 1);

            // Find .got.plt sections to exclude from RELRO
            const char* sectionNames =
                (const char*)((uint64_t)elfFile.seHead + (static_cast<uint64_t>(elfFile.elfHead.e_shstrndx * elfFile.elfHead.e_shentsize)));
            auto* shstrtabHdr =
                (Elf64_Shdr*)((uint64_t)elfFile.seHead + (static_cast<uint64_t>(elfFile.elfHead.e_shstrndx * elfFile.elfHead.e_shentsize)));
            sectionNames = (const char*)((uint64_t)elfFile.base + shstrtabHdr->sh_offset);

            // Check each page to see if it overlaps with any GOT.PLT sections
            for (uint64_t va = start; va < end; va += mod::mm::virt::PAGE_SIZE) {
                if (!mod::mm::virt::isPageMapped(pagemap, va)) {
                    continue;
                }

                // Check if this page contains any .got.plt section
                bool hasGotPlt = false;
                for (size_t sectionIndex = 0; sectionIndex < elfFile.elfHead.e_shnum; sectionIndex++) {
                    auto* sectionHeader = (Elf64_Shdr*)((uint64_t)elfFile.seHead + (sectionIndex * elfFile.elfHead.e_shentsize));
                    const char* sectionName = &sectionNames[sectionHeader->sh_name];

                    if (std::strncmp(sectionName, ".got.plt", 8) == 0 && sectionHeader->sh_addr != 0) {
                        uint64_t gotStart = sectionHeader->sh_addr + elfFile.loadBase;
                        uint64_t gotEnd = gotStart + sectionHeader->sh_size;
                        uint64_t pageEnd = va + mod::mm::virt::PAGE_SIZE;

                        // Check if GOT.PLT overlaps with this page
                        if (gotStart < pageEnd && gotEnd > va) {
                            hasGotPlt = true;
#ifdef ELF_DEBUG
                            mod::dbg::log("RELRO: Skipping page 0x%x because it contains .got.plt [0x%x-0x%x)", va, gotStart, gotEnd);
#endif
                            break;
                        }
                    }
                }

                if (!hasGotPlt) {
                    mod::mm::virt::unifyPageFlags(pagemap, va, mod::mm::paging::pageTypes::USER_READONLY);
                }
            }
#ifdef ELF_DEBUG
            mod::dbg::log("RELRO enforced for vaddr=[0x%x, 0x%x) (excluding .got.plt pages)", start, end);
#endif
        }
    }

    if (registerSpecialSymbols) {
        // Iterate section headers to find symbol tables
        for (size_t sidx = 0; sidx < elfFile.elfHead.e_shnum; sidx++) {
            auto* section = (Elf64_Shdr*)((uint64_t)elfFile.seHead + (sidx * elfFile.elfHead.e_shentsize));

            if (section->sh_type == SHT_SYMTAB || section->sh_type == SHT_DYNSYM) {
                // Get string table for this symbol table
                auto* strtab = (Elf64_Shdr*)(elfFile.base + elfFile.elfHead.e_shoff +
                                             (static_cast<size_t>(section->sh_link * elfFile.elfHead.e_shentsize)));
                const char* strs = (const char*)(elfFile.base + strtab->sh_offset);
                uint64_t numSymbols = section->sh_size / section->sh_entsize;
                auto* syms = (Elf64_Sym*)(elfFile.base + section->sh_offset);

                for (uint64_t si = 0; si < numSymbols; si++) {
                    Elf64_Sym* sym = &syms[si];
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
    ElfLoadResult result = {.entryPoint = elfFile.elfHead.e_entry + elfFile.loadBase,
                            .programHeaderAddr = programHeadersVaddr,
                            .elfHeaderAddr = elfHeaderVaddr};
    return result;
}

// Extract TLS information from ELF without fully loading it
auto extractTlsInfo(void* elfData) -> TlsModule {
    TlsModule tlsInfo = {.tlsBase = 0, .tlsSize = 0, .tcbOffset = 0};  // Default empty TLS info

#ifdef ELF_DEBUG
    mod::dbg::log("extractTlsInfo: Starting TLS extraction from ELF data at 0x%x", (uint64_t)elfData);
#endif

    // Validate input pointer
    if (elfData == nullptr) {
        mod::dbg::log("ERROR: extractTlsInfo called with null elfData pointer");
        return tlsInfo;
    }

    // Parse the ELF to find PT_TLS segment
    ElfFile elfFile = parseElf((uint8_t*)elfData);

    // Check if parsing was successful (parseElf validates magic numbers)
    if (elfFile.elfHead.e_ident[EI_MAG0] != ELFMAG0 || elfFile.elfHead.e_ident[EI_MAG1] != ELFMAG1 ||
        elfFile.elfHead.e_ident[EI_MAG2] != ELFMAG2 || elfFile.elfHead.e_ident[EI_MAG3] != ELFMAG3) {
        mod::dbg::log("ERROR: extractTlsInfo - Invalid ELF magic in parsed data");
        return tlsInfo;
    }

#ifdef ELF_DEBUG
    mod::dbg::log("extractTlsInfo: ELF has %d program headers", elfFile.elfHead.e_phnum);
#endif

    // Look for PT_TLS segment - use a local pointer instead of modifying elfFile.pgHead
    for (uint32_t i = 0; i < elfFile.elfHead.e_phnum; i++) {
        auto* currentPhdr = (Elf64_Phdr*)(elfFile.base + elfFile.elfHead.e_phoff + (static_cast<size_t>(i * elfFile.elfHead.e_phentsize)));

        if (currentPhdr->p_type == PT_TLS) {
            tlsInfo.tlsSize = currentPhdr->p_memsz;
            tlsInfo.tcbOffset = currentPhdr->p_memsz;  // TCB goes after TLS data
#ifdef ELF_DEBUG
            mod::dbg::log("extractTlsInfo: Found PT_TLS segment with size %d bytes", tlsInfo.tlsSize);
#endif
            break;
        }
    }

#ifdef ELF_DEBUG
    mod::dbg::log("extractTlsInfo: Returning TLS size %d", tlsInfo.tlsSize);
#endif
    return tlsInfo;
}

}  // namespace ker::loader::elf
