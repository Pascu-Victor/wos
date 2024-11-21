#include "elf_loader.hpp"

namespace ker::loader::elf {
static bool headerIsValid(const Elf64_Ehdr ehdr) {
    return ehdr.e_ident[EI_CLASS] == ELFCLASS64        // 64-bit
           && ehdr.e_ident[EI_OSABI] == ELFOSABI_NONE  // System V
           && ehdr.e_type == ET_EXEC                   // Executable
           && (ehdr).e_ident[EI_MAG0] == ELFMAG0       // Magic 0
           && (ehdr).e_ident[EI_MAG1] == ELFMAG1       // Magic 1
           && (ehdr).e_ident[EI_MAG2] == ELFMAG2       // Magic 2
           && (ehdr).e_ident[EI_MAG3] == ELFMAG3;      // Magic 3
}

static ElfFile parseElf(uint8_t *base) {
    ElfFile elf;
    elf.base = base;
    elf.elfHead = *(Elf64_Ehdr *)base;
    elf.pgHead = (Elf64_Phdr *)(base + elf.elfHead.e_phoff);
    elf.seHead = (Elf64_Shdr *)(base + elf.elfHead.e_shoff);
    elf.sctHeadStrTab = (Elf64_Shdr *)(elf.elfHead.e_shoff +          // Section header offset
                                       elf.elfHead.e_shstrndx         // Section header string table index
                                           * elf.elfHead.e_shentsize  // Size of each section header
    );
    return elf;
}

void loadSegment(uint8_t *elfBase, ker::mod::mm::virt::PageTable *pagemap, Elf64_Phdr *programHeader, uint64_t pageNo) {
    auto pagePaddr = (mod::mm::addr::vaddr_t)mod::mm::phys::pageAlloc();
    if (!pagePaddr) {
        ker::mod::io::serial::write("Failed to allocate page\n");
        return;
    }

    auto pageVaddr = programHeader->p_vaddr + pageNo * mod::mm::virt::PAGE_SIZE;
    // auto pageAdd = pagepa
    mod::mm::virt::mapPage(pagemap, pageVaddr, (mod::mm::addr::paddr_t)mod::mm::addr::getPhysPointer(pagePaddr),
                           mod::mm::paging::pageTypes::USER);

    auto pageOffset = pageNo * mod::mm::virt::PAGE_SIZE;
    auto copySize = mod::mm::virt::PAGE_SIZE;

    if (pageOffset >= programHeader->p_filesz) {
        memset((void *)pagePaddr, 0, mod::mm::virt::PAGE_SIZE);
        return;
    }

    if (pageOffset + mod::mm::virt::PAGE_SIZE > programHeader->p_filesz) {
        copySize = programHeader->p_filesz - pageOffset;
    }

    memcpy((void *)pagePaddr, elfBase + programHeader->p_offset + pageOffset, copySize);

    if (copySize < mod::mm::virt::PAGE_SIZE) {
        memset((void *)(pagePaddr + copySize), 0, mod::mm::virt::PAGE_SIZE - copySize);
    }
}

void loadSectionHeaders(ElfFile elf, ker::mod::mm::virt::PageTable *pagemap) {
    Elf64_Shdr *scnHeadTable = (Elf64_Shdr *)((uint64_t)elf.base                 // Base address of the ELF file
                                              + elf.elfHead.e_shoff              // Section header offset
                                              + elf.elfHead.e_shstrndx           // Section header string table index
                                                    * elf.elfHead.e_shentsize);  // Size of each section header

    const char *sectionNames = (const char *)(elf.base + scnHeadTable->sh_offset);

    for (size_t sectionIndex = 0; sectionIndex < elf.elfHead.e_shnum; sectionIndex++) {
        Elf64_Shdr *sectionHeader = (Elf64_Shdr *)((uint64_t)elf.seHead + sectionIndex * elf.elfHead.e_shentsize);  // Get section header
        mod::dbg::log("Section name: %s\n", sectionNames + sectionHeader->sh_name);
        if (!std::strncmp(&sectionNames[sectionHeader->sh_name], ".rodata", 7)) {
            auto rodataVaddr = sectionHeader->sh_addr;
            auto rodataPaddr = mod::mm::virt::translate(pagemap, rodataVaddr);
            mod::dbg::log("Rodata vaddr: %x\n", rodataVaddr);
            mod::dbg::log("Rodata paddr: %x\n", rodataPaddr);
            memcpy((void *)rodataPaddr, elf.base + sectionHeader->sh_offset, sectionHeader->sh_size);
        }
    }
}

Elf64Entry loadElf(ElfFile *elf, ker::mod::mm::virt::PageTable *pagemap) {
    ElfFile elfFile = parseElf((uint8_t *)elf);

    if (!headerIsValid(elfFile.elfHead)) {
        return 0;
    }

    for (Elf64_Half i = 0; i < elfFile.elfHead.e_phnum; i++) {  // iterate over program headers
        if (elfFile.pgHead->p_type != PT_LOAD) continue;
        size_t num_pages = PAGE_ALIGN_UP(elfFile.pgHead->p_memsz) / mod::mm::virt::PAGE_SIZE;

        for (uint64_t j = 0; j < num_pages; j++) {
            loadSegment(elfFile.base, pagemap, elfFile.pgHead, j);
        }

        elfFile.pgHead = (Elf64_Phdr *)((uint64_t)elfFile.pgHead + elfFile.elfHead.e_phentsize);  // advance to next program header
    }

    loadSectionHeaders(elfFile, pagemap);

    return elfFile.elfHead.e_entry;
}
}  // namespace ker::loader::elf
