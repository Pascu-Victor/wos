#pragma once

#include <defines/defines.hpp>
#include <platform/mm/paging.hpp>

// Forward declaration to avoid circular dependency
namespace ker::loader::elf {
struct TlsModule;
}

namespace ker::mod::sched::threading {

struct Thread {
    uint64_t fsbase;
    uint64_t gsbase;

    uint64_t stack;
    uint64_t stackSize;

    uint64_t tlsSize;
    uint64_t tlsBaseVirt;
    uint64_t safestackPtrValue;
    int magic = 0;
} __attribute__((packed));

void initThreading();

Thread *createThread(uint64_t stackSize, uint64_t tlsSize, mm::paging::PageTable *pageTable, const ker::loader::elf::TlsModule &tlsInfo);
void destroyThread(Thread *thread);
}  // namespace ker::mod::sched::threading
