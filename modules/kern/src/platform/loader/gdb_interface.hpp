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
    char name[64];
    uint64_t baseAddress;
    uint64_t entryPoint;

    // ELF header location
    uint64_t elfHeaderAddr;

    // Section information
    uint16_t sectionCount;
    uint64_t sectionHeadersAddr;
    uint64_t stringTableAddr;
    uint64_t stringTableSize;

    // Program header information
    uint16_t programHeaderCount;
    uint64_t programHeadersAddr;

    // Debug section addresses
    uint64_t debugInfoAddr;
    uint64_t debugInfoSize;
    uint64_t debugLineAddr;
    uint64_t debugLineSize;
    uint64_t debugStrAddr;
    uint64_t debugStrSize;

    // Next process in chain
    uint64_t nextProcessAddr;
} __attribute__((packed));

// Global debug info chain
extern GdbDebugInfo* gdbDebugInfoChain;

// Functions to manage GDB debug info
void initGdbDebugInfo();
void addGdbDebugInfo(uint64_t pid, const char* name, uint64_t baseAddr, uint64_t entryPoint);
void updateGdbDebugSection(uint64_t pid, const char* sectionName, uint64_t addr, uint64_t size);
void finalizeGdbDebugInfo(uint64_t pid);

// Function to be called by GDB to get debug info
extern "C" GdbDebugInfo* getGdbDebugInfo();

}  // namespace ker::loader::debug
