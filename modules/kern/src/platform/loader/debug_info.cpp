#include "debug_info.hpp"

#include <platform/dbg/dbg.hpp>

namespace ker::loader::debug {

std::vector<ProcessDebugInfo> debugRegistry;

void registerProcess(uint64_t pid, const char* name, uint64_t baseAddr, uint64_t entryPoint) {
    ProcessDebugInfo info;
    info.pid = pid;
    info.name = name;
    info.baseAddress = baseAddr;
    info.entryPoint = entryPoint;
    info.sections = std::vector<DebugSection>();

    debugRegistry.push_back(info);
    ker::mod::dbg::log("Registered process for debugging: pid=%x, name=%s, base=%x, entry=%x", pid, name, baseAddr, entryPoint);
}

void addDebugSection(uint64_t pid, const char* name, uint64_t vaddr, uint64_t paddr, uint64_t size, uint64_t fileOffset, uint32_t type) {
    for (auto& process : debugRegistry) {
        if (process.pid == pid) {
            DebugSection section;
            section.name = name;
            section.vaddr = vaddr;
            section.paddr = paddr;
            section.size = size;
            section.fileOffset = fileOffset;
            section.type = type;

            process.sections.push_back(section);
            ker::mod::dbg::log("Added debug section: %s, vaddr=%x, paddr=%x, size=%x", name, vaddr, paddr, size);
            return;
        }
    }
}

void addDebugSymbol(uint64_t pid, const char* name, uint64_t vaddr, uint64_t paddr, uint64_t size, uint8_t bind, uint8_t type,
                    bool isTlsOffset, uint16_t shndx, uint64_t rawValue) {
    for (auto& process : debugRegistry) {
        if (process.pid == pid) {
            DebugSymbol sym;
            sym.name = name;
            sym.vaddr = vaddr;
            sym.paddr = paddr;
            sym.size = size;
            sym.bind = bind;
            sym.type = type;
            sym.isTlsOffset = isTlsOffset;
            sym.shndx = shndx;
            sym.rawValue = rawValue;

            process.symbols.push_back(sym);
            ker::mod::dbg::log("Added debug symbol: %s, vaddr=%x, paddr=%x, size=%x, bind=%d, type=%d", name, vaddr, paddr, size, bind,
                               type);
            return;
        }
    }
}

void setElfHeaders(uint64_t pid, const Elf64_Ehdr& header, uint64_t headerAddr) {
    for (auto& process : debugRegistry) {
        if (process.pid == pid) {
            process.elfHeader = header;
            process.elfHeaderAddr = headerAddr;
            return;
        }
    }
}

void setProgramHeaders(uint64_t pid, Elf64_Phdr* phdrs, uint64_t phdrsAddr, uint16_t count) {
    for (auto& process : debugRegistry) {
        if (process.pid == pid) {
            process.programHeaders = phdrs;
            process.programHeadersAddr = phdrsAddr;
            process.programHeaderCount = count;
            return;
        }
    }
}

void setSectionHeaders(uint64_t pid, Elf64_Shdr* shdrs, uint64_t shdrsAddr, uint16_t count) {
    for (auto& process : debugRegistry) {
        if (process.pid == pid) {
            process.sectionHeaders = shdrs;
            process.sectionHeadersAddr = shdrsAddr;
            process.sectionHeaderCount = count;
            return;
        }
    }
}

void setStringTable(uint64_t pid, const char* strtab, uint64_t strtabAddr, uint64_t size) {
    for (auto& process : debugRegistry) {
        if (process.pid == pid) {
            process.stringTable = strtab;
            process.stringTableAddr = strtabAddr;
            process.stringTableSize = size;
            return;
        }
    }
}

ProcessDebugInfo* getProcessDebugInfo(uint64_t pid) {
    for (auto& process : debugRegistry) {
        if (process.pid == pid) {
            return &process;
        }
    }
    return nullptr;
}

DebugSymbol* getProcessSymbol(uint64_t pid, const char* name) {
    ProcessDebugInfo* p = getProcessDebugInfo(pid);
    if (!p) return nullptr;
    for (auto& sym : p->symbols) {
        if (!std::strncmp(sym.name, name, 128)) {
            return &sym;
        }
    }
    return nullptr;
}

void printDebugInfo(uint64_t pid) {
    ProcessDebugInfo* info = getProcessDebugInfo(pid);
    if (!info) {
        ker::mod::dbg::log("No debug info found for PID %x", pid);
        return;
    }

    ker::mod::dbg::log("Debug info for process %s (PID %x):", info->name, pid);
    ker::mod::dbg::log("  Base address: %x", info->baseAddress);
    ker::mod::dbg::log("  Entry point: %x", info->entryPoint);
    ker::mod::dbg::log("  ELF header at: %x", info->elfHeaderAddr);
    ker::mod::dbg::log("  Program headers at: %x (count: %d)", info->programHeadersAddr, info->programHeaderCount);
    ker::mod::dbg::log("  Section headers at: %x (count: %d)", info->sectionHeadersAddr, info->sectionHeaderCount);
    ker::mod::dbg::log("  String table at: %x (size: %x)", info->stringTableAddr, info->stringTableSize);

    ker::mod::dbg::log("  Sections:");
    for (const auto& section : info->sections) {
        ker::mod::dbg::log("    %s: vaddr=%x, paddr=%x, size=%x, type=%x", section.name, section.vaddr, section.paddr, section.size,
                           section.type);
    }
}

}  // namespace ker::loader::debug
