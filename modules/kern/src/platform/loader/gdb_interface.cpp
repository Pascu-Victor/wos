#include "gdb_interface.hpp"

#include <platform/dbg/dbg.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>

namespace ker::loader::debug {

GdbDebugInfo* gdbDebugInfoChain = nullptr;

void initGdbDebugInfo() {
    gdbDebugInfoChain = nullptr;
    ker::mod::dbg::log("Initialized GDB debug info chain");
}

void addGdbDebugInfo(uint64_t pid, const char* name, uint64_t baseAddr, uint64_t entryPoint) {
    // Allocate memory for the debug info structure
    uint64_t debugInfoPaddr = (uint64_t)ker::mod::mm::phys::pageAlloc();
    if (debugInfoPaddr == 0) {
        ker::mod::dbg::log("Failed to allocate memory for GDB debug info");
        return;
    }

    GdbDebugInfo* debugInfo = (GdbDebugInfo*)ker::mod::mm::addr::getPhysPointer(debugInfoPaddr);

    // Initialize the debug info structure
    debugInfo->magic = 0x47444255;  // 'GDBU' in little endian
    debugInfo->pid = pid;
    std::strncpy(debugInfo->name, name, sizeof(debugInfo->name) - 1);
    debugInfo->name[sizeof(debugInfo->name) - 1] = '\0';
    debugInfo->baseAddress = baseAddr;
    debugInfo->entryPoint = entryPoint;

    // Initialize other fields
    debugInfo->elfHeaderAddr = 0;
    debugInfo->sectionCount = 0;
    debugInfo->sectionHeadersAddr = 0;
    debugInfo->stringTableAddr = 0;
    debugInfo->stringTableSize = 0;
    debugInfo->programHeaderCount = 0;
    debugInfo->programHeadersAddr = 0;
    debugInfo->debugInfoAddr = 0;
    debugInfo->debugInfoSize = 0;
    debugInfo->debugLineAddr = 0;
    debugInfo->debugLineSize = 0;
    debugInfo->debugStrAddr = 0;
    debugInfo->debugStrSize = 0;

    // Link into the chain
    debugInfo->nextProcessAddr = (uint64_t)gdbDebugInfoChain;
    gdbDebugInfoChain = debugInfo;

    ker::mod::dbg::log("Added GDB debug info for process %s (PID %x) at %x", name, pid, debugInfo);
}

void updateGdbDebugSection(uint64_t pid, const char* sectionName, uint64_t addr, uint64_t size) {
    // Find the debug info for this process
    GdbDebugInfo* current = gdbDebugInfoChain;
    while (current != nullptr) {
        if (current->pid == pid) {
            // Update the appropriate section
            if (std::strncmp(sectionName, ".debug_info", 11) == 0) {
                current->debugInfoAddr = addr;
                current->debugInfoSize = size;
            } else if (std::strncmp(sectionName, ".debug_line", 11) == 0) {
                current->debugLineAddr = addr;
                current->debugLineSize = size;
            } else if (std::strncmp(sectionName, ".debug_str", 10) == 0) {
                current->debugStrAddr = addr;
                current->debugStrSize = size;
            }

            ker::mod::dbg::log("Updated GDB debug section %s for PID %x: addr=%x, size=%x", sectionName, pid, addr, size);
            return;
        }
        current = (GdbDebugInfo*)current->nextProcessAddr;
    }
}

void finalizeGdbDebugInfo(uint64_t pid) {
    // Find the debug info for this process
    GdbDebugInfo* current = gdbDebugInfoChain;
    while (current != nullptr) {
        if (current->pid == pid) {
            ker::mod::dbg::log("Finalized GDB debug info for PID %x", pid);
            ker::mod::dbg::log("  Name: %s", current->name);
            ker::mod::dbg::log("  Base: %x, Entry: %x", current->baseAddress, current->entryPoint);
            ker::mod::dbg::log("  ELF Header: %x", current->elfHeaderAddr);
            ker::mod::dbg::log("  Section Headers: %x (count: %d)", current->sectionHeadersAddr, current->sectionCount);
            ker::mod::dbg::log("  String Table: %x (size: %x)", current->stringTableAddr, current->stringTableSize);
            ker::mod::dbg::log("  Debug Info: %x (size: %x)", current->debugInfoAddr, current->debugInfoSize);
            ker::mod::dbg::log("  Debug Line: %x (size: %x)", current->debugLineAddr, current->debugLineSize);
            ker::mod::dbg::log("  Debug Str: %x (size: %x)", current->debugStrAddr, current->debugStrSize);
            return;
        }
        current = (GdbDebugInfo*)current->nextProcessAddr;
    }
}

// Function to be called by GDB to get debug info
extern "C" GdbDebugInfo* getGdbDebugInfo() { return gdbDebugInfoChain; }

}  // namespace ker::loader::debug
