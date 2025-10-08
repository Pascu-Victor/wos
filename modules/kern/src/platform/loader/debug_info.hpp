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
    uint64_t fileOffset;
    uint32_t type;
};

struct DebugSymbol {
    const char* name;
    uint64_t vaddr;
    uint64_t paddr;
    uint64_t size;
    uint8_t bind;
    uint8_t type;
    bool isTlsOffset;
    uint16_t shndx;
    uint64_t rawValue;  // original st_value
};

struct ProcessDebugInfo {
    uint64_t pid;
    const char* name;
    uint64_t baseAddress;
    uint64_t entryPoint;
    std::vector<DebugSection> sections;
    std::vector<DebugSymbol> symbols;

    // ELF header information
    Elf64_Ehdr elfHeader;
    uint64_t elfHeaderAddr;

    // Program headers
    Elf64_Phdr* programHeaders;
    uint64_t programHeadersAddr;
    uint16_t programHeaderCount;

    // Section headers
    Elf64_Shdr* sectionHeaders;
    uint64_t sectionHeadersAddr;
    uint16_t sectionHeaderCount;

    // String table
    const char* stringTable;
    uint64_t stringTableAddr;
    uint64_t stringTableSize;
};

// Global debug info registry
extern std::vector<ProcessDebugInfo> debugRegistry;

// Functions to manage debug info
void registerProcess(uint64_t pid, const char* name, uint64_t baseAddr, uint64_t entryPoint);
void addDebugSection(uint64_t pid, const char* name, uint64_t vaddr, uint64_t paddr, uint64_t size, uint64_t fileOffset, uint32_t type);
void addDebugSymbol(uint64_t pid, const char* name, uint64_t vaddr, uint64_t paddr, uint64_t size, uint8_t bind, uint8_t type,
                    bool isTlsOffset, uint16_t shndx, uint64_t rawValue);
void setElfHeaders(uint64_t pid, const Elf64_Ehdr& header, uint64_t headerAddr);
void setProgramHeaders(uint64_t pid, Elf64_Phdr* phdrs, uint64_t phdrsAddr, uint16_t count);
void setSectionHeaders(uint64_t pid, Elf64_Shdr* shdrs, uint64_t shdrsAddr, uint16_t count);
void setStringTable(uint64_t pid, const char* strtab, uint64_t strtabAddr, uint64_t size);

ProcessDebugInfo* getProcessDebugInfo(uint64_t pid);
void printDebugInfo(uint64_t pid);
DebugSymbol* getProcessSymbol(uint64_t pid, const char* name);

}  // namespace ker::loader::debug
