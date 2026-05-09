#include "threading.hpp"

#include <string.h>

#include <cstdint>
#include <platform/dbg/dbg.hpp>
#include <platform/loader/elf_loader.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sys/spinlock.hpp>
#include <util/hcf.hpp>
#include <util/list.hpp>

#include "platform/mm/paging.hpp"
#include "platform/mm/phys.hpp"

namespace ker::mod::sched::threading {
std::list<Thread*> activeThreads;
static sys::Spinlock activeThreadsLock;

void init_threading() {}

Thread* create_thread(uint64_t stackSize, uint64_t tlsSize, mm::paging::PageTable* pageTable, const ker::loader::elf::TlsModule& tlsInfo) {
    auto* thread = new Thread();
    if (thread == nullptr) {
        dbg::log("createThread: Failed to allocate Thread object");
        return nullptr;
    }
    thread->stack_size = stackSize;
    thread->tls_size = tlsSize;

    // Use the actual TLS size from PT_TLS segment if available, otherwise use provided size
    uint64_t actualTlsSize = (tlsInfo.tlsSize > 0) ? tlsInfo.tlsSize : tlsSize;

    // Allocate memory for TLS + TCB + SafeStack
    // TCB structure is ~136 bytes + stack canary + padding
    uint64_t tcbSize = 256;          // Extra space for mlibc's Tcb structure
    uint64_t safestackSize = 65536;  // 64KB for SafeStack unsafe stack
    uint64_t totalTlsSize = actualTlsSize + tcbSize + safestackSize;

    void* tls = mm::phys::page_alloc(PAGE_ALIGN_UP(totalTlsSize));
    if (tls == nullptr) {
        dbg::log("createThread: Failed to allocate TLS memory (%d bytes)", PAGE_ALIGN_UP(totalTlsSize));
        delete thread;
        return nullptr;
    }

    void* stack = mm::phys::page_alloc(stackSize);
    if (stack == nullptr) {
        dbg::log("createThread: Failed to allocate stack memory (%d bytes)", stackSize);
        mm::phys::page_free(tls);
        delete thread;
        return nullptr;
    }

    // CRITICAL FIX: Use page-aligned size for virtual address calculation too
    uint64_t alignedTotalSize = PAGE_ALIGN_UP(totalTlsSize);
    uint64_t tlsVirtAddr = 0x7FFF00000000ULL - alignedTotalSize;
    uint64_t stackVirtAddr = tlsVirtAddr - stackSize;
    thread->magic = 0xDEADBEEF;

    // Map all pages for TLS
    for (uint64_t offset = 0; offset < alignedTotalSize; offset += mm::paging::PAGE_SIZE) {
        uint64_t tlsPhys = (uint64_t)mm::addr::get_phys_pointer((uint64_t)tls + offset);
        mm::virt::map_page(pageTable, tlsVirtAddr + offset, tlsPhys, mm::paging::page_types::USER);
    }

    // Map all pages for stack
    for (uint64_t offset = 0; offset < stackSize; offset += mm::paging::PAGE_SIZE) {
        uint64_t stackPhys = (uint64_t)mm::addr::get_phys_pointer((uint64_t)stack + offset);
        mm::virt::map_page(pageTable, stackVirtAddr + offset, stackPhys, mm::paging::page_types::USER);
    }

    // TLS and user stacks are reclaimed one 4 KiB leaf at a time during
    // destroyUserSpace() / COW teardown, so split the bulk allocations into
    // independently freeable pages once the mappings are established.
    if (!mm::phys::page_split_to_order0(tls) || !mm::phys::page_split_to_order0(stack)) {
        dbg::log("createThread: failed to split TLS/stack backing pages");
        hcf();
    }

    // TCB goes at the TOP of the TLS area (highest address)
    void* tcb = (reinterpret_cast<uint8_t*>(tls) + actualTlsSize);
    void* tcbVirtAddr = reinterpret_cast<uint8_t*>(tlsVirtAddr) + actualTlsSize;

    // SafeStack area goes after the TCB
    void* safestackArea = (reinterpret_cast<uint8_t*>(tls) + actualTlsSize + tcbSize);
    void* safestackVirtAddr = reinterpret_cast<uint8_t*>(tlsVirtAddr) + actualTlsSize + tcbSize;

    // Initialize the TLS area (clear it first)
    memset(tls, 0, actualTlsSize);

    // Initialize the TCB according to mlibc's Tcb structure
    // Zero out the entire TCB area
    memset(tcb, 0, tcbSize);

    // Set up minimal TCB structure for mlibc
    // TCB layout for x86_64 (from mlibc/tcb.hpp):
    // +0x00: selfPointer (Tcb *) - points to TCB itself
    // +0x08: dtvSize
    // +0x10: dtvPointers
    // +0x18: tid
    // +0x1C: didExit
    // +0x20: padding (8 bytes on x86_64)
    // +0x28: stackCanary
    // +0x30: cancelBits

    uint64_t* tcb_ptr = (uint64_t*)tcb;
    tcb_ptr[0] = (uint64_t)tcbVirtAddr;  // selfPointer points to TCB itself
    tcb_ptr[1] = 1;                      // dtvSize
    tcb_ptr[2] = 0;                      // dtvPointers (can be null for now)

    uint32_t* tcb_i32 = (uint32_t*)tcb;
    tcb_i32[6] = 0;  // tid (will be set later)
    tcb_i32[7] = 0;  // didExit

    // CRITICAL: Set the stack canary at offset 0x28 (40 decimal)
    tcb_ptr[5] = 0x3000000018ULL;  // stackCanary at fs:[0x28]

    tcb_i32[12] = 0;  // cancelBits at offset 0x30

    // SafeStack support: The TLS area needs to have space for __safestack_unsafe_stack_ptr
    // This is a thread-local variable that the compiler uses for SafeStack
    // According to the relocation, it should be at TLS offset -8 from the TCB

    // Set up TLS variables area RIGHT BEFORE the TCB (negative offsets from fs register)
    // Use physical address since the page table hasn't been switched yet

    // Initialize the entire TLS area with zeros first
    memset(tls, 0, actualTlsSize);

    // For mlibc TLS variables, we need to set up the proper layout:
    // The linker expects TLS variables at specific offsets from the TCB
    // Based on binary analysis, errno (__mlibc_errno) should be accessible at fs:[-176]

    // Set up key TLS variables:
    // 1. SafeStack pointer at the standard location (offset 0 in TLS segment)
    uint64_t* safestackPtr = (uint64_t*)tls;  // At TLS offset 0
    uint64_t safestackTop = (uint64_t)safestackVirtAddr + safestackSize;

    uint64_t safestackPtrValue = safestackTop - 512;  // Leave 512 bytes safety margin from top
    *safestackPtr = safestackPtrValue;                // SafeStack grows downward

    auto* errnoPtr = (uint32_t*)((uint8_t*)tls + 0xa0);  // TLS offset 160
    *errnoPtr = 0;                                       // Initialize errno to 0

    // Initialize the SafeStack area
    memset(safestackArea, 0, safestackSize);

    // Save TLS mapping info into the Thread object for later initialization
    thread->tls_size = alignedTotalSize;
    thread->tls_base_virt = tlsVirtAddr;
    thread->safestack_ptr_value = safestackPtrValue;

    // Stack grows downward, so thread->stack points to the TOP of the stack
    // Reserve space at the BOTTOM of stack for PerCpu scratch area (used by syscall handler after swapgs)
    uint64_t scratchAreaSize = sizeof(cpu::PerCpu);
    thread->stack = stackVirtAddr + stackSize - scratchAreaSize;  // Stack starts above scratch area
    thread->fsbase = reinterpret_cast<uint64_t>(tcbVirtAddr);
    // User GS_BASE points to TLS/stack base area (user-accessible)
    thread->gsbase = stackVirtAddr;  // Bottom of stack where scratch area lives

    // Initialize the scratch area at the bottom of the stack
    auto* scratchArea = reinterpret_cast<cpu::PerCpu*>(stack);
    memset(scratchArea, 0, sizeof(cpu::PerCpu));
    scratchArea->syscall_stack = 0;  // Will be set by task initialization
    scratchArea->cpu_id = 0;         // Will be set by task initialization

    // Store the physical (HHDM) pointers for cleanup
    thread->tls_phys_ptr = reinterpret_cast<uint64_t>(tls);
    thread->stack_phys_ptr = reinterpret_cast<uint64_t>(stack);

    activeThreadsLock.lock();
    activeThreads.push_back(thread);
    activeThreadsLock.unlock();
    return thread;
}

void destroy_thread(Thread* thread) {
    if (thread == nullptr) {
        return;
    }

    activeThreadsLock.lock();
    activeThreads.remove(thread);
    activeThreadsLock.unlock();

    // Free the actual physical allocations using the stored HHDM pointers
    if (thread->tls_phys_ptr != 0) {
        mm::phys::page_free(reinterpret_cast<void*>(thread->tls_phys_ptr));
    }
    if (thread->stack_phys_ptr != 0) {
        mm::phys::page_free(reinterpret_cast<void*>(thread->stack_phys_ptr));
    }

    delete thread;
}

auto get_active_thread_count() -> uint64_t {
    activeThreadsLock.lock();
    uint64_t count = activeThreads.size();
    activeThreadsLock.unlock();
    return count;
}
}  // namespace ker::mod::sched::threading
