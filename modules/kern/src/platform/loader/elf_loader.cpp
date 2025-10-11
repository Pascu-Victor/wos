#include "elf_loader.hpp"

#include <extern/elf.h>

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>

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

auto headerIsValid(const Elf64_Ehdr &ehdr) -> bool {
    return ehdr.e_ident[EI_CLASS] == ELFCLASS64                  // 64-bit
           && ehdr.e_ident[EI_OSABI] == ELFOSABI_NONE            // System V
           && (ehdr.e_type == ET_EXEC || ehdr.e_type == ET_DYN)  // Executable or PIE
           && (ehdr).e_ident[EI_MAG0] == ELFMAG0                 // Magic 0
           && (ehdr).e_ident[EI_MAG1] == ELFMAG1                 // Magic 1
           && (ehdr).e_ident[EI_MAG2] == ELFMAG2                 // Magic 2
           && (ehdr).e_ident[EI_MAG3] == ELFMAG3;                // Magic 3
}

auto parseElf(uint8_t *base) -> ElfFile {
    ElfFile elf{};
    elf.base = base;
    elf.elfHead = *(Elf64_Ehdr *)base;
    elf.pgHead = (Elf64_Phdr *)(base + elf.elfHead.e_phoff);
    elf.seHead = (Elf64_Shdr *)(base + elf.elfHead.e_shoff);
    elf.sctHeadStrTab = (Elf64_Shdr *)(elf.elfHead.e_shoff +                                // Section header offset
                                       (static_cast<Elf64_Off>(elf.elfHead.e_shstrndx       // Section header string table index
                                                               * elf.elfHead.e_shentsize))  // Size of each section header
    );

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

    return elf;
}

void processRelocations(const ElfFile &elf, ker::mod::mm::virt::PageTable *pagemap) {
    // Get section header string table so we can detect .relr sections by name
    Elf64_Shdr *shdrTable = elf.seHead;
    const char *shstr = (const char *)(elf.base + shdrTable[elf.elfHead.e_shstrndx].sh_offset);
    // No per-process resolver stub: we eagerly resolve and enforce RELRO.
    // Find relocation sections (REL and RELA)
    for (size_t i = 0; i < elf.elfHead.e_shnum; i++) {
        Elf64_Shdr *sectionHeader = (Elf64_Shdr *)((uint64_t)elf.seHead + (i * elf.elfHead.e_shentsize));

        // Handle the newer SHT_RELR compressed relocation section by name
        const char *secName = nullptr;
        if (elf.elfHead.e_shstrndx < elf.elfHead.e_shnum) {
            secName = shstr + sectionHeader->sh_name;
        }

        if ((secName != nullptr) && ((std::strncmp(secName, ".relr", 5) == 0) || (std::strncmp(secName, ".relr.dyn", 9) == 0))) {
            mod::dbg::log("Processing SHT_RELR (.relr) relocations in section %d (%s)", i, (secName != nullptr) ? secName : "");

            uint64_t numEntries = sectionHeader->sh_size / sizeof(uint64_t);
            auto *entries = (uint64_t *)(elf.base + sectionHeader->sh_offset);

            uint64_t base = 0;
            for (uint64_t ei = 0; ei < numEntries; ei++) {
                uint64_t ent = entries[ei];
                if ((ent & 1ULL) == 0ULL) {
                    // explicit relocation: this value is an address to relocate
                    base = ent;
                    uint64_t P = base + elf.loadBase;
                    uint64_t paddr = mod::mm::virt::translate(pagemap, P);
                    if (paddr != 0) {
                        auto *physPtr = (uint64_t *)mod::mm::addr::getVirtPointer((uint64_t)mod::mm::addr::getPhysPointer(paddr));
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
                                auto *physPtr = (uint64_t *)mod::mm::addr::getVirtPointer((uint64_t)mod::mm::addr::getPhysPointer(paddr));
                                *physPtr = *physPtr + elf.loadBase;
                            }
                        }
                    }
                }
            }

            continue;
        }
        if (sectionHeader->sh_type == SHT_REL) {
            mod::dbg::log("Processing SHT_REL relocations in section %d", i);

            uint64_t numRelocations = sectionHeader->sh_size / sizeof(Elf64_Rel);
            auto *relocations = (Elf64_Rel *)(elf.base + sectionHeader->sh_offset);

            for (uint64_t j = 0; j < numRelocations; j++) {
                Elf64_Rel *rel = &relocations[j];
                uint32_t type = ELF64_R_TYPE(rel->r_info);
                uint32_t symIndex = ELF64_R_SYM(rel->r_info);
                uint64_t P = rel->r_offset + elf.loadBase;  // place

                // For REL entries, addend is the current value at P (read from memory at P)
                uint64_t addend = 0;
                uint64_t paddr = mod::mm::virt::translate(pagemap, P);
                if (paddr != 0) {
                    auto *physPtr = (uint64_t *)mod::mm::addr::getVirtPointer((uint64_t)mod::mm::addr::getPhysPointer(paddr));
                    addend = *physPtr;
                }

                // Resolve symbol value S
                uint64_t S = 0;
                const char *symName = "";
                if (symIndex != 0) {
                    // The relocation section's sh_link tells which symbol table to use
                    if (sectionHeader->sh_link < elf.elfHead.e_shnum) {
                        auto *symTabSec = (Elf64_Shdr *)((uint64_t)elf.seHead +
                                                         (static_cast<uint64_t>(sectionHeader->sh_link * elf.elfHead.e_shentsize)));
                        auto *syms = (Elf64_Sym *)(elf.base + symTabSec->sh_offset);
                        uint64_t n = symTabSec->sh_size / symTabSec->sh_entsize;
                        // Get string table for symbol names if available
                        const char *symStrs = nullptr;
                        if (symTabSec->sh_link < elf.elfHead.e_shnum) {
                            auto *strtabSec = (Elf64_Shdr *)((uint64_t)elf.seHead +
                                                             (static_cast<uint64_t>(symTabSec->sh_link * elf.elfHead.e_shentsize)));
                            symStrs = (const char *)(elf.base + strtabSec->sh_offset);
                        }

                        if (symIndex < n) {
                            Elf64_Sym *sym = &syms[symIndex];
                            symName = (symStrs != nullptr) ? (symStrs + sym->st_name) : "";
                            S = sym->st_value;
                            // If the symbol is defined in a section, add loadBase (unless TLS)
                            if (sym->st_shndx < elf.elfHead.e_shnum) {
                                auto *symSec =
                                    (Elf64_Shdr *)((uint64_t)elf.seHead + (static_cast<uint64_t>(sym->st_shndx * elf.elfHead.e_shentsize)));
                                if ((symSec->sh_flags & SHF_TLS) == 0U) {
                                    S += elf.loadBase;
                                }
                            }
                        }
                    } else {
                        // Fallback: search symbol tables if sh_link is invalid
                        for (size_t sidx = 0; sidx < elf.elfHead.e_shnum; sidx++) {
                            auto *sec = (Elf64_Shdr *)((uint64_t)elf.seHead + (sidx * elf.elfHead.e_shentsize));
                            if (sec->sh_type == SHT_SYMTAB || sec->sh_type == SHT_DYNSYM) {
                                auto *syms = (Elf64_Sym *)(elf.base + sec->sh_offset);
                                uint64_t n = sec->sh_size / sec->sh_entsize;
                                if (symIndex < n) {
                                    Elf64_Sym *sym = &syms[symIndex];
                                    S = sym->st_value;
                                    if (sym->st_shndx < elf.elfHead.e_shnum) {
                                        auto *symSec = (Elf64_Shdr *)((uint64_t)elf.seHead + sym->st_shndx * elf.elfHead.e_shentsize);
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

                mod::dbg::log("Relocation REL: P=0x%x, type=%d, sym=%d ('%s'), S=0x%x, A=0x%x", P, type, symIndex, symName ? symName : "",
                              S, addend);

                switch (type) {
                    case R_X86_64_TPOFF64: {
                        // Put TLS offset (example handling)
                        int64_t tlsOffset = -8;  // default for safestack ptr
                        if (paddr != 0) {
                            uint64_t *physPtr = (uint64_t *)mod::mm::addr::getVirtPointer((uint64_t)mod::mm::addr::getPhysPointer(paddr));
                            *physPtr = (uint64_t)tlsOffset;
                        }
                        break;
                    }
                    case R_X86_64_RELATIVE: {
                        if (paddr != 0) {
                            uint64_t *physPtr = (uint64_t *)mod::mm::addr::getVirtPointer((uint64_t)mod::mm::addr::getPhysPointer(paddr));
                            *physPtr = addend + elf.loadBase;
                        }
                        break;
                    }
                    case R_X86_64_GLOB_DAT:
                    case R_X86_64_JUMP_SLOT: {
                        // Ensure the target virtual address is mapped and writable for GOT/PLT writes.
                        if (paddr == 0) {
                            uint64_t targetPage = P & ~(mod::mm::virt::PAGE_SIZE - 1);
                            mod::dbg::log("GOT/PLT entry at 0x%x not mapped; allocating page 0x%x", P, targetPage);
                            uint64_t newPaddr = (uint64_t)mod::mm::phys::pageAlloc();
                            if (newPaddr != 0) {
                                uint64_t physPtrPage = (uint64_t)mod::mm::addr::getPhysPointer(newPaddr);
                                mod::mm::virt::mapPage(pagemap, targetPage, physPtrPage, mod::mm::paging::pageTypes::USER);
                                paddr = mod::mm::virt::translate(pagemap, P);
                            }
                        }

                        if (paddr != 0) {
                            uint64_t *physPtr = (uint64_t *)mod::mm::addr::getVirtPointer((uint64_t)mod::mm::addr::getPhysPointer(paddr));
                            if (S == 0) {
                                mod::dbg::log("ERROR: Unresolved symbol for relocation at P=0x%x (type=%d). Writing 0 to catch fault.", P,
                                              type);
                            }
                            uint64_t writeVal = S + addend;
                            *physPtr = writeVal;
                            // Read-back verification
                            uint64_t verifyVal = *physPtr;
                            mod::dbg::log("Wrote GOT/PLT at P=0x%x (paddr=0x%x) -> 0x%x (read-back=0x%x)", P, paddr, writeVal, verifyVal);
                        } else {
                            mod::dbg::log("Failed to map/allocate GOT/PLT page for P=0x%x", P);
                        }

                        break;
                    }
                    case R_X86_64_64: {
                        if (paddr != 0) {
                            uint64_t *physPtr = (uint64_t *)mod::mm::addr::getVirtPointer((uint64_t)mod::mm::addr::getPhysPointer(paddr));
                            *physPtr = S + addend;
                        }
                        break;
                    }
                    case R_X86_64_PC32:
                    case R_X86_64_PLT32: {
                        // 32-bit PC-relative
                        if (paddr != 0) {
                            uint32_t *physPtr32 = (uint32_t *)mod::mm::addr::getVirtPointer((uint64_t)mod::mm::addr::getPhysPointer(paddr));
                            uint64_t P64 = P;
                            int64_t value = (int64_t)(S + (int64_t)addend - (int64_t)P64);
                            *physPtr32 = (uint32_t)value;
                        }
                        break;
                    }
                    default:
                        mod::dbg::log("Unhandled REL relocation type: %d", type);
                        break;
                }
            }
        } else if (sectionHeader->sh_type == SHT_RELA) {
            mod::dbg::log("Processing SHT_RELA relocations in section %d", i);

            uint64_t numRelocations = sectionHeader->sh_size / sizeof(Elf64_Rela);
            Elf64_Rela *relocations = (Elf64_Rela *)(elf.base + sectionHeader->sh_offset);

            for (uint64_t j = 0; j < numRelocations; j++) {
                Elf64_Rela *rel = &relocations[j];
                uint32_t type = ELF64_R_TYPE(rel->r_info);
                uint32_t symIndex = ELF64_R_SYM(rel->r_info);
                uint64_t P = rel->r_offset + elf.loadBase;  // place
                int64_t addend = rel->r_addend;

                // Resolve symbol value S
                uint64_t S = 0;
                const char *symName = "";
                if (symIndex != 0) {
                    if (sectionHeader->sh_link < elf.elfHead.e_shnum) {
                        auto *symTabSec = (Elf64_Shdr *)((uint64_t)elf.seHead +
                                                         (static_cast<uint64_t>(sectionHeader->sh_link * elf.elfHead.e_shentsize)));
                        auto *syms = (Elf64_Sym *)(elf.base + symTabSec->sh_offset);
                        uint64_t n = symTabSec->sh_size / symTabSec->sh_entsize;
                        const char *symStrs = nullptr;
                        if (symTabSec->sh_link < elf.elfHead.e_shnum) {
                            auto *strtabSec = (Elf64_Shdr *)((uint64_t)elf.seHead +
                                                             (static_cast<uint64_t>(symTabSec->sh_link * elf.elfHead.e_shentsize)));
                            symStrs = (const char *)(elf.base + strtabSec->sh_offset);
                        }

                        if (symIndex < n) {
                            Elf64_Sym *sym = &syms[symIndex];
                            symName = (symStrs != nullptr) ? (symStrs + sym->st_name) : "";
                            S = sym->st_value;
                            if (sym->st_shndx < elf.elfHead.e_shnum) {
                                auto *symSec =
                                    (Elf64_Shdr *)((uint64_t)elf.seHead + (static_cast<uint64_t>(sym->st_shndx * elf.elfHead.e_shentsize)));
                                if ((symSec->sh_flags & SHF_TLS) == 0U) {
                                    S += elf.loadBase;
                                }
                            }
                        }
                    } else {
                        for (size_t sidx = 0; sidx < elf.elfHead.e_shnum; sidx++) {
                            auto *sec = (Elf64_Shdr *)((uint64_t)elf.seHead + (sidx * elf.elfHead.e_shentsize));
                            if (sec->sh_type == SHT_SYMTAB || sec->sh_type == SHT_DYNSYM) {
                                auto *syms = (Elf64_Sym *)(elf.base + sec->sh_offset);
                                uint64_t n = sec->sh_size / sec->sh_entsize;
                                if (symIndex < n) {
                                    Elf64_Sym *sym = &syms[symIndex];
                                    S = sym->st_value;
                                    if (sym->st_shndx < elf.elfHead.e_shnum) {
                                        auto *symSec = (Elf64_Shdr *)((uint64_t)elf.seHead + sym->st_shndx * elf.elfHead.e_shentsize);
                                        if (!(symSec->sh_flags & SHF_TLS)) {
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
                mod::dbg::log("Relocation RELA: P=0x%x, type=%d, sym=%d ('%s'), S=0x%x, A=0x%x", P, type, symIndex, symName ? symName : "",
                              S, addend);

                switch (type) {
                    case R_X86_64_TPOFF64: {
                        int64_t tlsOffset = addend;  // use addend if present
                        if (paddr != 0) {
                            auto *physPtr = (uint64_t *)mod::mm::addr::getVirtPointer((uint64_t)mod::mm::addr::getPhysPointer(paddr));
                            *physPtr = (uint64_t)tlsOffset;
                        }
                        break;
                    }
                    case R_X86_64_RELATIVE: {
                        if (paddr != 0) {
                            auto *physPtr = (uint64_t *)mod::mm::addr::getVirtPointer((uint64_t)mod::mm::addr::getPhysPointer(paddr));
                            *physPtr = elf.loadBase + (uint64_t)addend;
                        }
                        break;
                    }
                    case R_X86_64_GLOB_DAT:
                    case R_X86_64_JUMP_SLOT: {
                        if (paddr == 0) {
                            uint64_t targetPage = P & ~(mod::mm::virt::PAGE_SIZE - 1);
                            mod::dbg::log("GOT/PLT entry at 0x%x not mapped; allocating page 0x%x", P, targetPage);
                            auto newPaddr = (uint64_t)mod::mm::phys::pageAlloc();
                            if (newPaddr != 0) {
                                auto physPtr = (uint64_t)mod::mm::addr::getPhysPointer(newPaddr);
                                mod::mm::virt::mapPage(pagemap, targetPage, physPtr, mod::mm::paging::pageTypes::USER);
                                paddr = mod::mm::virt::translate(pagemap, P);
                            }
                        }

                        if (paddr != 0) {
                            auto *physPtr = (uint64_t *)mod::mm::addr::getVirtPointer((uint64_t)mod::mm::addr::getPhysPointer(paddr));
                            if (S == 0) {
                                mod::dbg::log("ERROR: Unresolved symbol for relocation at P=0x%x (type=%d). Writing 0 to catch fault.", P,
                                              type);
                            }
                            uint64_t writeVal = S + (uint64_t)addend;
                            *physPtr = writeVal;
                            mod::dbg::log("Wrote GOT/PLT at P=0x%x -> 0x%x (S=0x%x)", P, writeVal, S);
                        } else {
                            mod::dbg::log("Failed to map/allocate GOT/PLT page for P=0x%x", P);
                        }

                        break;
                    }
                    case R_X86_64_64: {
                        if (paddr != 0) {
                            uint64_t *physPtr = (uint64_t *)mod::mm::addr::getVirtPointer((uint64_t)mod::mm::addr::getPhysPointer(paddr));
                            *physPtr = S + (uint64_t)addend;
                        }
                        break;
                    }
                    case R_X86_64_PC32:
                    case R_X86_64_PLT32: {
                        if (paddr != 0) {
                            uint32_t *physPtr32 = (uint32_t *)mod::mm::addr::getVirtPointer((uint64_t)mod::mm::addr::getPhysPointer(paddr));
                            uint64_t P64 = P;
                            int64_t value = (int64_t)(S + addend - (int64_t)P64);
                            *physPtr32 = (uint32_t)value;
                        }
                        break;
                    }
                    default:
                        mod::dbg::log("Unhandled RELA relocation type: %d", type);
                        break;
                }
            }
        }
    }
}

void processReadOnlySegment(Elf64_Phdr *segment, ker::mod::mm::virt::PageTable *pagemap, uint64_t pid) {
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

void registerEhFrame(void *base, uint64_t size) {
    (void)base;
    (void)size;
    // TODO: Register the .eh_frame section for exception handling
}

void processEhFrameSegment(Elf64_Phdr *segment, ker::mod::mm::virt::PageTable *pagemap, uint64_t pid) {
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
    registerEhFrame((void *)(vaddr), segment->p_memsz);
}

void loadSegment(uint8_t *elfBase, ker::mod::mm::virt::PageTable *pagemap, Elf64_Phdr *programHeader, uint64_t pageNo,
                 uint64_t baseOffset) {
    // Compute aligned virtual address for this page and in-page offset of the segment start
    const uint64_t segStartVA = programHeader->p_vaddr + baseOffset;
    const uint64_t firstPageOffset = segStartVA & (mod::mm::virt::PAGE_SIZE - 1);
    const uint64_t alignedStartVA = segStartVA & ~(mod::mm::virt::PAGE_SIZE - 1);
    const uint64_t pageVA = alignedStartVA + (pageNo * mod::mm::virt::PAGE_SIZE);

    // Additional validation for PIE executables
    if (baseOffset != 0 && pageVA < 0x1000) {
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
        uint64_t newPageHhdmPtr = (uint64_t)mod::mm::phys::pageAlloc();
        pageHhdmPtr = newPageHhdmPtr;
        // Map using the physical address corresponding to that HHDM pointer
        mod::mm::virt::mapPage(pagemap, pageVA, (mod::mm::addr::paddr_t)mod::mm::addr::getPhysPointer(newPageHhdmPtr),
                               mod::mm::paging::pageTypes::USER);
        // Zero freshly mapped page to handle bss/holes
        memset((void *)pageHhdmPtr, 0, mod::mm::virt::PAGE_SIZE);
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
        bytesBeforeThisPage = (mod::mm::virt::PAGE_SIZE - firstPageOffset) + (pageNo - 1) * mod::mm::virt::PAGE_SIZE;
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
    memcpy((void *)(pageHhdmPtr + dstInPage), elfBase + srcOffset, copySize);
}

void loadSectionHeaders(const ElfFile &elf, ker::mod::mm::virt::PageTable *pagemap, const uint64_t &pid) {
    (void)pid;
    auto *scnHeadTable = (Elf64_Shdr *)((uint64_t)elf.base               // Base address of the ELF file
                                        + elf.elfHead.e_shoff            // Section header offset
                                        + (elf.elfHead.e_shstrndx        // Section header string table index
                                           * elf.elfHead.e_shentsize));  // Size of each section header

    const char *sectionNames = (const char *)(elf.base + scnHeadTable->sh_offset);

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
            memset((void *)paddr, 0, mod::mm::virt::PAGE_SIZE);

            // Remember first page for copying
            if (i == 0) {
                sectionHeadersPhysPtr = paddr;
            }
        }
    }

    // Copy section headers to allocated physical memory
    if (sectionHeadersPhysPtr != 0) {
        memcpy((void *)sectionHeadersPhysPtr, elf.seHead, sectionHeadersSize);
    }
    debug::setSectionHeaders(pid, (Elf64_Shdr *)sectionHeadersVaddr, sectionHeadersVaddr, elf.elfHead.e_shnum);

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
            memset((void *)paddr, 0, mod::mm::virt::PAGE_SIZE);

            // Remember first page for copying
            if (i == 0) {
                stringTablePhysPtr = paddr;
            }
        }
    }

    // Copy string table to allocated physical memory
    if (stringTablePhysPtr != 0) {
        memcpy((void *)stringTablePhysPtr, sectionNames, stringTableSize);
    }
    debug::setStringTable(pid, (const char *)stringTableVaddr, stringTableVaddr, stringTableSize);

    for (size_t sectionIndex = 0; sectionIndex < elf.elfHead.e_shnum; sectionIndex++) {
        auto *sectionHeader = (Elf64_Shdr *)((uint64_t)elf.seHead + (sectionIndex * elf.elfHead.e_shentsize));
        const char *sectionName = &sectionNames[sectionHeader->sh_name];
        mod::dbg::log("Section name: %s", sectionName);
        (void)sectionName;

        // Register sections for debugging without re-mapping any PT_LOAD-backed content
        if (sectionHeader->sh_type == SHT_PROGBITS && sectionHeader->sh_size > 0) {
            // If the section already has a virtual address, it is covered by PT_LOAD; just record it
            if (sectionHeader->sh_addr != 0) {
                uint64_t sectionVaddr = sectionHeader->sh_addr + elf.loadBase;
                uint64_t firstPaddr = mod::mm::virt::translate(pagemap, sectionVaddr);
                debug::addDebugSection(pid, sectionName, sectionVaddr, firstPaddr, sectionHeader->sh_size, sectionHeader->sh_offset,
                                       sectionHeader->sh_type);
                mod::dbg::log("Added debug section: %s, vaddr=%x, paddr=%x, size=%x", sectionName, sectionVaddr, firstPaddr,
                              sectionHeader->sh_size);
            } else if (!std::strncmp(sectionName, ".debug_", 7)) {
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
                        memset((void *)debugPaddr, 0, mod::mm::virt::PAGE_SIZE);
                        uint64_t copySize = (remainingSize > mod::mm::virt::PAGE_SIZE) ? mod::mm::virt::PAGE_SIZE : remainingSize;
                        memcpy((void *)debugPaddr, elf.base + sectionHeader->sh_offset + sourceOffset, copySize);
                        remainingSize -= copySize;
                        sourceOffset += copySize;
                    } else {
                        allocationSuccess = false;
                        break;
                    }
                }

                if (allocationSuccess) {
                    if (sectionHeadersPhysPtr != 0) {
                        auto *mappedSectionHeader = (Elf64_Shdr *)(sectionHeadersPhysPtr + (sectionIndex * elf.elfHead.e_shentsize));
                        mappedSectionHeader->sh_addr = debugVaddr;
                    }
                    debug::addDebugSection(pid, sectionName, debugVaddr, debugVaddr, sectionHeader->sh_size, sectionHeader->sh_offset,
                                           sectionHeader->sh_type);
                    mod::dbg::log("Allocated debug section %s at vaddr: %x, size: %x", sectionName, debugVaddr, sectionHeader->sh_size);
                } else {
                    mod::dbg::log("Failed to allocate memory for debug section %s", sectionName);
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
            mod::dbg::log("Recorded GOT-like section %s at vaddr: %x, paddr: %x, size: %x", sectionName, sectionVaddr, firstPaddr,
                          sectionHeader->sh_size);
        }
    }
}
}  // namespace

auto loadElf(ElfFile *elf, ker::mod::mm::virt::PageTable *pagemap, uint64_t pid, const char *processName, bool registerSpecialSymbols)
    -> Elf64Entry {
    ElfFile elfFile = parseElf((uint8_t *)elf);

    if (!headerIsValid(elfFile.elfHead)) {
        return 0;
    }

    // Register this process for debugging
    debug::registerProcess(pid, processName, (uint64_t)elfFile.base, elfFile.elfHead.e_entry + elfFile.loadBase);

    // Allocate memory for ELF header and preserve it
    // TODO: make this dynamic or make llvm aware of this memory no-go zone
    constexpr uint64_t elfHeaderVaddr = 0x400000ULL;  // High memory area for ELF header
    auto elfHeaderPaddr = (uint64_t)mod::mm::phys::pageAlloc();
    if (elfHeaderPaddr != 0) {
        mod::mm::virt::mapPage(pagemap, elfHeaderVaddr, (uint64_t)mod::mm::addr::getPhysPointer(elfHeaderPaddr),
                               mod::mm::paging::pageTypes::USER);
        memcpy((void *)elfHeaderPaddr, &elfFile.elfHead, sizeof(Elf64_Ehdr));
        debug::setElfHeaders(pid, elfFile.elfHead, elfHeaderVaddr);
    }

    // Allocate memory for program headers and preserve them
    auto programHeadersSize = static_cast<uint64_t>(elfFile.elfHead.e_phnum * elfFile.elfHead.e_phentsize);
    uint64_t programHeadersPages = PAGE_ALIGN_UP(programHeadersSize) / mod::mm::virt::PAGE_SIZE;
    constexpr uint64_t programHeadersVaddr = 0x700000020000ULL;  // After ELF header

    uint64_t programHeadersPhysPtr = 0;
    for (uint64_t i = 0; i < programHeadersPages; i++) {
        auto paddr = (uint64_t)mod::mm::phys::pageAlloc();
        if (paddr != 0) {
            auto physPtr = (uint64_t)mod::mm::addr::getPhysPointer(paddr);
            mod::mm::virt::mapPage(pagemap, programHeadersVaddr + (i * mod::mm::virt::PAGE_SIZE), physPtr,
                                   mod::mm::paging::pageTypes::USER);

            // Zero the page
            memset((void *)paddr, 0, mod::mm::virt::PAGE_SIZE);

            // Remember first page for copying
            if (i == 0) {
                programHeadersPhysPtr = paddr;
            }
        }
    }

    // Copy program headers to allocated physical memory
    if (programHeadersPhysPtr != 0) {
        memcpy((void *)programHeadersPhysPtr, elfFile.pgHead, programHeadersSize);
    }
    debug::setProgramHeaders(pid, (Elf64_Phdr *)programHeadersVaddr, programHeadersVaddr, elfFile.elfHead.e_phnum);
    for (Elf64_Half i = 0; i < elfFile.elfHead.e_phnum; i++) {
        auto *currentHeader = (Elf64_Phdr *)((uint64_t)elfFile.pgHead + (static_cast<uint64_t>(i * elfFile.elfHead.e_phentsize)));

        switch (currentHeader->p_type) {
            case PT_GNU_STACK:
                // GNU_STACK segment presence indicates whether stack is executable; record as a debug section
                mod::dbg::log("Found PT_GNU_STACK at vaddr=0x%x, flags=0x%x", currentHeader->p_vaddr, currentHeader->p_flags);
                debug::addDebugSection(pid, "PT_GNU_STACK", currentHeader->p_vaddr + elfFile.loadBase, currentHeader->p_offset,
                                       currentHeader->p_memsz, currentHeader->p_offset, currentHeader->p_type);
                break;
            case PT_TLS:
                // Found TLS segment - store its information
                elfFile.tlsInfo.tlsBase = currentHeader->p_vaddr + elfFile.loadBase;
                elfFile.tlsInfo.tlsSize = currentHeader->p_memsz;
                elfFile.tlsInfo.tcbOffset = currentHeader->p_memsz;  // TCB goes after TLS data
                mod::dbg::log("Found PT_TLS segment: vaddr=0x%x, filesz=0x%x, memsz=0x%x", currentHeader->p_vaddr, currentHeader->p_filesz,
                              currentHeader->p_memsz);
                break;

            case PT_LOAD:
                // Loadable segment
                {
                    mod::dbg::log("Loading PT_LOAD segment: vaddr=0x%x, filesz=0x%x, memsz=0x%x, offset=0x%x", currentHeader->p_vaddr,
                                  currentHeader->p_filesz, currentHeader->p_memsz, currentHeader->p_offset);
                    size_t num_pages = PAGE_ALIGN_UP(currentHeader->p_memsz) / mod::mm::virt::PAGE_SIZE;
                    for (uint64_t j = 0; j < num_pages; j++) {
                        loadSegment(elfFile.base, pagemap, currentHeader, j, elfFile.loadBase);
                    }
                }
                break;

            case PT_GNU_RELRO:
                mod::dbg::log("Found PT_GNU_RELRO segment at vaddr=0x%x, size=0x%x", currentHeader->p_vaddr, currentHeader->p_memsz);
                // Read-only relocation segment
                processReadOnlySegment(currentHeader, pagemap, pid);
                break;

            case PT_GNU_EH_FRAME:
                mod::dbg::log("Found PT_GNU_EH_FRAME segment at vaddr=0x%x, size=0x%x", currentHeader->p_vaddr, currentHeader->p_memsz);
                // Exception handling frame segment
                processEhFrameSegment(currentHeader, pagemap, pid);
                break;

            case PT_INTERP:
                mod::dbg::log("WARN: PT_INTERP skipped FIXME!");
                break;

            case PT_DYNAMIC:
                mod::dbg::log("WARN: PT_DYNAMIC skipped FIXME!");
                break;

            case PT_NOTE:
                mod::dbg::log("WARN: PT_NOTE skipped FIXME!");
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
    // NOTE: Process segments in reverse order so that executable segments (typically first)
    // can override non-executable segments if pages overlap after alignment.
    for (int i = elfFile.elfHead.e_phnum - 1; i >= 0; i--) {
        auto *ph = (Elf64_Phdr *)((uint64_t)elfFile.pgHead + (static_cast<uint64_t>(i * elfFile.elfHead.e_phentsize)));
        if (ph->p_type == PT_LOAD) {
            const bool writable = (ph->p_flags & PF_W) != 0;
            const bool executable = (ph->p_flags & PF_X) != 0;
            uint64_t baseFlags = writable ? mod::mm::paging::pageTypes::USER : mod::mm::paging::pageTypes::USER_READONLY;
            if (!executable) {
                baseFlags |= mod::mm::paging::PAGE_NX;
            }
            uint64_t start = (ph->p_vaddr + elfFile.loadBase) & ~(mod::mm::virt::PAGE_SIZE - 1);
            uint64_t end = (ph->p_vaddr + ph->p_memsz + mod::mm::virt::PAGE_SIZE - 1) & ~(mod::mm::virt::PAGE_SIZE - 1);
            for (uint64_t va = start; va < end; va += mod::mm::virt::PAGE_SIZE) {
                mod::mm::virt::unifyPageFlags(pagemap, va, baseFlags);
            }
            mod::dbg::log("PT_LOAD perms applied: vaddr=[0x%x, 0x%x) flags=0x%x -> %s%s", start, end, ph->p_flags,
                          writable ? "USER" : "USER_READONLY", executable ? "" : "+NX");
        }
    }

    // Enforce RELRO after relocations: PT_GNU_RELRO pages become read-only
    for (Elf64_Half i = 0; i < elfFile.elfHead.e_phnum; i++) {
        auto *ph = (Elf64_Phdr *)((uint64_t)elfFile.pgHead + (static_cast<uint64_t>(i * elfFile.elfHead.e_phentsize)));
        if (ph->p_type == PT_GNU_RELRO) {
            uint64_t start = ph->p_vaddr + elfFile.loadBase;
            uint64_t end = (ph->p_vaddr + ph->p_memsz + mod::mm::virt::PAGE_SIZE - 1) & ~(mod::mm::virt::PAGE_SIZE - 1);
            for (uint64_t va = start; va < end; va += mod::mm::virt::PAGE_SIZE) {
                if (mod::mm::virt::isPageMapped(pagemap, va)) {
                    mod::mm::virt::unifyPageFlags(pagemap, va, mod::mm::paging::pageTypes::USER_READONLY);
                }
            }
            mod::dbg::log("RELRO enforced for vaddr=[0x%x, 0x%x)", start, end);
        }
    }

    if (registerSpecialSymbols) {
        // Helper to register a symbol by name
        auto registerSymbolIfFound = [&](const char *symName, uint64_t symVaddr, uint64_t symSize, uint8_t bind, uint8_t type, bool isTls,
                                         uint16_t shndx, uint64_t rawVal) {
            uint64_t vaddr = 0;
            if (isTls) {
                // For TLS symbols, symVaddr is an offset into TLS template
                vaddr = symVaddr;
            } else if (shndx < elfFile.elfHead.e_shnum) {
                // If symbol is defined in a section, compute vaddr from section address + sym value
                Elf64_Shdr *symSec =
                    (Elf64_Shdr *)((uint64_t)elfFile.seHead + (static_cast<uint64_t>(shndx * elfFile.elfHead.e_shentsize)));
                if (symSec->sh_addr != 0) {
                    vaddr = symSec->sh_addr + symVaddr;
                } else {
                    // Fallback: use symbol value plus loadBase
                    vaddr = symVaddr + elfFile.loadBase;
                }
            } else {
                // Fallback: use symbol value plus loadBase
                vaddr = symVaddr + elfFile.loadBase;
            }

            uint64_t paddr = isTls ? 0 : mod::mm::virt::translate(pagemap, vaddr);
            // If __dso_handle ended up with address 0 (common in some toolchains),
            // give it a safe non-zero fallback inside the module so calls don't jump to 0.
            if (!vaddr && symName && std::strncmp(symName, "__dso_handle", 12) == 0) {
                vaddr = elfFile.loadBase + elfFile.elfHead.e_entry;
                paddr = mod::mm::virt::translate(pagemap, vaddr);
            }
            if (paddr == 0) {
                // If symbol is not mapped or is in-file-only, record vaddr as paddr to preserve info
                paddr = vaddr;
            }
            debug::addDebugSymbol(pid, symName, vaddr, paddr, symSize, bind, type, isTls, shndx, rawVal);
        };

        // Iterate section headers to find symbol tables
        for (size_t sidx = 0; sidx < elfFile.elfHead.e_shnum; sidx++) {
            auto *section = (Elf64_Shdr *)((uint64_t)elfFile.seHead + (sidx * elfFile.elfHead.e_shentsize));

            if (section->sh_type == SHT_SYMTAB || section->sh_type == SHT_DYNSYM) {
                // Get string table for this symbol table
                auto *strtab = (Elf64_Shdr *)(elfFile.base + elfFile.elfHead.e_shoff +
                                              (static_cast<size_t>(section->sh_link * elfFile.elfHead.e_shentsize)));
                const char *strs = (const char *)(elfFile.base + strtab->sh_offset);
                uint64_t numSymbols = section->sh_size / section->sh_entsize;
                auto *syms = (Elf64_Sym *)(elfFile.base + section->sh_offset);

                for (uint64_t si = 0; si < numSymbols; si++) {
                    Elf64_Sym *sym = &syms[si];
                    const char *sname = strs + sym->st_name;
                    if ((sname == nullptr) || (sname[0] == 0)) {
                        continue;
                    }

                    // Check for special names
                    if (false &&
                        ((std::strncmp(sname, "__safestack_unsafe_stack_ptr", 27) == 0) || (std::strncmp(sname, "__ehdr_start", 11) == 0) ||
                         (std::strncmp(sname, "__init_array_start", 18) == 0) || (std::strncmp(sname, "__init_array_end", 16) == 0) ||
                         (std::strncmp(sname, "__fini_array_start", 18) == 0) || (std::strncmp(sname, "__fini_array_end", 16) == 0) ||
                         (std::strncmp(sname, "__preinit_array_start", 21) == 0) || (std::strncmp(sname, "__preinit_array_end", 19) == 0) ||
                         (std::strncmp(sname, "__dso_handle", 12) == 0))) {
                        uint64_t symVaddr = sym->st_value;
                        uint64_t symSize = sym->st_size;
                        uint8_t bind = ELF64_ST_BIND(sym->st_info);
                        uint8_t type = ELF64_ST_TYPE(sym->st_info);
                        uint16_t shndx = sym->st_shndx;

                        bool isTls = false;
                        if (shndx < elfFile.elfHead.e_shnum) {
                            auto *symSec =
                                (Elf64_Shdr *)((uint64_t)elfFile.seHead + (static_cast<uint64_t>(shndx * elfFile.elfHead.e_shentsize)));
                            if ((symSec->sh_flags & SHF_TLS) != 0U) {
                                isTls = true;
                            }
                        }

                        registerSymbolIfFound(sname, symVaddr, symSize, bind, type, isTls, shndx, symVaddr);
                    }
                }
            }
        }
    }

    // Print debug info for verification
    debug::printDebugInfo(pid);

    // Return entry point with base address offset for PIE executables
    return elfFile.elfHead.e_entry + elfFile.loadBase;
}

// Extract TLS information from ELF without fully loading it
auto extractTlsInfo(void *elfData) -> TlsModule {
    TlsModule tlsInfo = {.tlsBase = 0, .tlsSize = 0, .tcbOffset = 0};  // Default empty TLS info

    mod::dbg::log("extractTlsInfo: Starting TLS extraction from ELF data at 0x%x", (uint64_t)elfData);

    // Parse the ELF to find PT_TLS segment
    ElfFile elfFile = parseElf((uint8_t *)elfData);

    mod::dbg::log("extractTlsInfo: ELF has %d program headers", elfFile.elfHead.e_phnum);

    // Look for PT_TLS segment
    for (uint32_t i = 0; i < elfFile.elfHead.e_phnum; i++) {
        elfFile.pgHead = (Elf64_Phdr *)(elfFile.base + elfFile.elfHead.e_phoff + (static_cast<size_t>(i * elfFile.elfHead.e_phentsize)));

        if (elfFile.pgHead->p_type == PT_TLS) {
            tlsInfo.tlsSize = elfFile.pgHead->p_memsz;
            tlsInfo.tcbOffset = elfFile.pgHead->p_memsz;  // TCB goes after TLS data
            mod::dbg::log("extractTlsInfo: Found PT_TLS segment with size %d bytes", tlsInfo.tlsSize);
            break;
        }
    }

    mod::dbg::log("extractTlsInfo: Returning TLS size %d", tlsInfo.tlsSize);
    return tlsInfo;
}

}  // namespace ker::loader::elf
