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

    // Physical memory pointers (HHDM addresses) for cleanup
    // These are the actual allocations that need to be freed
    uint64_t tlsPhysPtr;    // HHDM pointer to TLS+TCB+SafeStack allocation
    uint64_t stackPhysPtr;  // HHDM pointer to stack allocation

    int magic = 0;
} __attribute__((packed));

void initThreading();

Thread* createThread(uint64_t stackSize, uint64_t tlsSize, mm::paging::PageTable* pageTable, const ker::loader::elf::TlsModule& tlsInfo);
void destroyThread(Thread* thread);

// OOM diagnostics - get count of active threads
auto getActiveThreadCount() -> uint64_t;
}  // namespace ker::mod::sched::threading
