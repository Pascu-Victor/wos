#include "debug_info.hpp"

#include <platform/dbg/dbg.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::loader::debug {

std::vector<ProcessDebugInfo> debugRegistry;
static ker::mod::sys::Spinlock debugRegistryLock;

void registerProcess(uint64_t pid, const char* name, uint64_t baseAddr, uint64_t entryPoint) {
    ProcessDebugInfo info;
    info.pid = pid;
    info.name = name;
    info.baseAddress = baseAddr;
    info.entryPoint = entryPoint;
    info.sections = std::vector<DebugSection>();

    debugRegistryLock.lock();
    debugRegistry.push_back(info);
    debugRegistryLock.unlock();
#ifdef ELF_DEBUG
    ker::mod::dbg::log("Registered process for debugging: pid=%x, name=%s, base=%x, entry=%x", pid, name, baseAddr, entryPoint);
#endif
}

void addDebugSection(uint64_t pid, const char* name, uint64_t vaddr, uint64_t paddr, uint64_t size, uint64_t fileOffset, uint32_t type) {
    debugRegistryLock.lock();
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
            debugRegistryLock.unlock();
#ifdef ELF_DEBUG
            ker::mod::dbg::log("Added debug section: %s, vaddr=%x, paddr=%x, size=%x", name, vaddr, paddr, size);
#endif
            return;
        }
    }
    debugRegistryLock.unlock();
}

void addDebugSymbol(uint64_t pid, const char* name, uint64_t vaddr, uint64_t paddr, uint64_t size, uint8_t bind, uint8_t type,
                    bool isTlsOffset, uint16_t shndx, uint64_t rawValue) {
    debugRegistryLock.lock();
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
            debugRegistryLock.unlock();
#ifdef ELF_DEBUG
            ker::mod::dbg::log("Added debug symbol: %s, vaddr=%x, paddr=%x, size=%x, bind=%d, type=%d", name, vaddr, paddr, size, bind,
                               type);
#endif
            return;
        }
    }
    debugRegistryLock.unlock();
}

void setElfHeaders(uint64_t pid, const Elf64_Ehdr& header, uint64_t headerAddr) {
    debugRegistryLock.lock();
    for (auto& process : debugRegistry) {
        if (process.pid == pid) {
            process.elfHeader = header;
            process.elfHeaderAddr = headerAddr;
            debugRegistryLock.unlock();
            return;
        }
    }
    debugRegistryLock.unlock();
}

void setProgramHeaders(uint64_t pid, Elf64_Phdr* phdrs, uint64_t phdrsAddr, uint16_t count) {
    debugRegistryLock.lock();
    for (auto& process : debugRegistry) {
        if (process.pid == pid) {
            process.programHeaders = phdrs;
            process.programHeadersAddr = phdrsAddr;
            process.programHeaderCount = count;
            debugRegistryLock.unlock();
            return;
        }
    }
    debugRegistryLock.unlock();
}

void setSectionHeaders(uint64_t pid, Elf64_Shdr* shdrs, uint64_t shdrsAddr, uint16_t count) {
    debugRegistryLock.lock();
    for (auto& process : debugRegistry) {
        if (process.pid == pid) {
            process.sectionHeaders = shdrs;
            process.sectionHeadersAddr = shdrsAddr;
            process.sectionHeaderCount = count;
            debugRegistryLock.unlock();
            return;
        }
    }
    debugRegistryLock.unlock();
}

void setStringTable(uint64_t pid, const char* strtab, uint64_t strtabAddr, uint64_t size) {
    debugRegistryLock.lock();
    for (auto& process : debugRegistry) {
        if (process.pid == pid) {
            process.stringTable = strtab;
            process.stringTableAddr = strtabAddr;
            process.stringTableSize = size;
            debugRegistryLock.unlock();
            return;
        }
    }
    debugRegistryLock.unlock();
}

ProcessDebugInfo* getProcessDebugInfo(uint64_t pid) {
    debugRegistryLock.lock();
    for (auto& process : debugRegistry) {
        if (process.pid == pid) {
            debugRegistryLock.unlock();
            return &process;
        }
    }
    debugRegistryLock.unlock();
    return nullptr;
}

DebugSymbol* getProcessSymbol(uint64_t pid, const char* name) {
    debugRegistryLock.lock();
    for (auto& process : debugRegistry) {
        if (process.pid == pid) {
            for (auto& sym : process.symbols) {
                if (!std::strncmp(sym.name, name, 128)) {
                    debugRegistryLock.unlock();
                    return &sym;
                }
            }
            debugRegistryLock.unlock();
            return nullptr;
        }
    }
    debugRegistryLock.unlock();
    return nullptr;
}

void printDebugInfo(uint64_t pid) {
    debugRegistryLock.lock();
    ProcessDebugInfo* info = nullptr;
    for (auto& process : debugRegistry) {
        if (process.pid == pid) {
            info = &process;
            break;
        }
    }
    if (!info) {
        debugRegistryLock.unlock();
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
    debugRegistryLock.unlock();
}

void unregisterProcess(uint64_t pid) {
    debugRegistryLock.lock();
    // Find and remove the process from the debug registry
    for (auto it = debugRegistry.begin(); it != debugRegistry.end(); ++it) {
        if (it->pid == pid) {
            // The vectors (sections, symbols) will be automatically cleaned up
            // when the ProcessDebugInfo is erased from the vector
            debugRegistry.erase(it);
            debugRegistryLock.unlock();
            return;
        }
    }
    debugRegistryLock.unlock();
}

}  // namespace ker::loader::debug
