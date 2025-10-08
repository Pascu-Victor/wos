#include "threading.hpp"

#include <string.h>

#include <platform/dbg/dbg.hpp>
#include <platform/loader/elf_loader.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/virt.hpp>
#include <util/list.hpp>

namespace ker::mod::sched::threading {
std::list<Thread *> activeThreads;

void initThreading() {}

Thread *createThread(uint64_t stackSize, uint64_t tlsSize, mm::paging::PageTable *pageTable, const ker::loader::elf::TlsModule &tlsInfo) {
    Thread *thread = new Thread();
    thread->stackSize = stackSize;
    thread->tlsSize = tlsSize;

    // Use the actual TLS size from PT_TLS segment if available, otherwise use provided size
    uint64_t actualTlsSize = (tlsInfo.tlsSize > 0) ? tlsInfo.tlsSize : tlsSize;

    // The binary was compiled with hardcoded TLS offsets based on linker calculations
    // The assembly uses offset -176 from TCB to access SafeStack pointer
    // This means the linker expected TCB at offset 176, so we need TLS size = 176
    if (actualTlsSize < 176) {
        mod::dbg::log("  Expanding TLS size from %d to 176 to match linker expectations", actualTlsSize);
        actualTlsSize = 176;  // Match what the linker expected when calculating -176 offset
    }

    // Allocate memory for TLS + TCB + SafeStack
    // TCB structure is ~136 bytes + stack canary + padding
    uint64_t tcbSize = 256;          // Extra space for mlibc's Tcb structure
    uint64_t safestackSize = 65536;  // 64KB for SafeStack unsafe stack
    uint64_t totalTlsSize = actualTlsSize + tcbSize + safestackSize;

    mod::dbg::log("TLS Layout Debug:");
    mod::dbg::log("  actualTlsSize = 0x%x (%d)", actualTlsSize, actualTlsSize);
    mod::dbg::log("  tcbSize = 0x%x (%d)", tcbSize, tcbSize);
    mod::dbg::log("  safestackSize = 0x%x (%d)", safestackSize, safestackSize);
    mod::dbg::log("  totalTlsSize = 0x%x (%d)", totalTlsSize, totalTlsSize);

    void *tls = mm::phys::pageAlloc(PAGE_ALIGN_UP(totalTlsSize));
    void *stack = mm::phys::pageAlloc(stackSize);

    // CRITICAL FIX: Use page-aligned size for virtual address calculation too
    uint64_t alignedTotalSize = PAGE_ALIGN_UP(totalTlsSize);
    mod::dbg::log("  alignedTotalSize = 0x%x", alignedTotalSize);
    uint64_t tlsVirtAddr = 0x7FFF00000000ULL - alignedTotalSize;
    uint64_t stackVirtAddr = tlsVirtAddr - stackSize;
    thread->magic = 0xDEADBEEF;

    mod::dbg::log("  tlsVirtAddr = 0x%x", tlsVirtAddr);
    mod::dbg::log("  Expected TCB at = 0x%x", tlsVirtAddr + actualTlsSize);

    // Map all pages for TLS
    for (uint64_t offset = 0; offset < alignedTotalSize; offset += mm::paging::PAGE_SIZE) {
        uint64_t tlsPhys = (uint64_t)mm::addr::getPhysPointer((uint64_t)tls + offset);
        mm::virt::mapPage(pageTable, tlsVirtAddr + offset, tlsPhys, mm::paging::pageTypes::USER);
    }

    // Map all pages for stack
    for (uint64_t offset = 0; offset < stackSize; offset += mm::paging::PAGE_SIZE) {
        uint64_t stackPhys = (uint64_t)mm::addr::getPhysPointer((uint64_t)stack + offset);
        mm::virt::mapPage(pageTable, stackVirtAddr + offset, stackPhys, mm::paging::pageTypes::USER);
    }

    // TCB goes at the TOP of the TLS area (highest address)
    void *tcb = (reinterpret_cast<uint8_t *>(tls) + actualTlsSize);
    void *tcbVirtAddr = reinterpret_cast<uint8_t *>(tlsVirtAddr) + actualTlsSize;

    // SafeStack area goes after the TCB
    void *safestackArea = (reinterpret_cast<uint8_t *>(tls) + actualTlsSize + tcbSize);
    void *safestackVirtAddr = reinterpret_cast<uint8_t *>(tlsVirtAddr) + actualTlsSize + tcbSize;

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

    uint64_t *tcb_ptr = (uint64_t *)tcb;
    tcb_ptr[0] = (uint64_t)tcbVirtAddr;  // selfPointer points to TCB itself
    tcb_ptr[1] = 1;                      // dtvSize
    tcb_ptr[2] = 0;                      // dtvPointers (can be null for now)

    uint32_t *tcb_i32 = (uint32_t *)tcb;
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
    uint64_t *safestackPtr = (uint64_t *)tls;  // At TLS offset 0
    uint64_t safestackTop = (uint64_t)safestackVirtAddr + safestackSize;

    // CRITICAL FIX: Ensure SafeStack pointer allows for negative offsets up to 256 bytes
    // The assembly does [SafeStack_ptr - 0x100], so we need at least 256 bytes below the pointer
    // to stay within mapped memory
    uint64_t safestackPtrValue = safestackTop - 512;  // Leave 512 bytes safety margin from top
    *safestackPtr = safestackPtrValue;                // SafeStack grows downward

    mod::dbg::log("  SafeStack setup: ptr at 0x%x (TLS offset 0), value = 0x%x", (uint64_t)safestackPtr, safestackPtrValue);
    mod::dbg::log("  SafeStack area: 0x%x to 0x%x (size 0x%x)", (uint64_t)safestackVirtAddr, safestackTop, safestackSize);
    mod::dbg::log("  SafeStack ptr - 0x100 = 0x%x (should be within mapped area)", safestackPtrValue - 0x100);

    // 2. mlibc errno at its actual TLS offset from objdump analysis
    // From objdump: __mlibc_errno is at TLS offset 0xa0 (160 bytes)
    // NOT at TCB - 176 as previously assumed
    uint32_t *errnoPtr = (uint32_t *)((uint8_t *)tls + 0xa0);  // TLS offset 160
    *errnoPtr = 0;                                             // Initialize errno to 0
    mod::dbg::log("  Set up errno at TLS offset 0xa0 (160 bytes from TLS base)");
    mod::dbg::log("  errno physical addr = 0x%x, errno virtual addr = 0x%x", (uint64_t)errnoPtr, (uint64_t)((uint8_t *)tlsVirtAddr + 0xa0));
    mod::dbg::log("  errno initialized to %d", *errnoPtr);

    mod::dbg::log("  Physical TLS base = 0x%x", (uint64_t)tls);
    mod::dbg::log("  Final fsbase (TCB virt addr) = 0x%x", (uint64_t)tcbVirtAddr);

    // Initialize the SafeStack area (clear it)
    memset(safestackArea, 0, safestackSize);

    // Save TLS mapping info into the Thread object for later initialization
    thread->tlsSize = alignedTotalSize;
    thread->tlsBaseVirt = tlsVirtAddr;
    thread->safestackPtrValue = safestackPtrValue;

    thread->stack = stackVirtAddr + stackSize;
    thread->fsbase = reinterpret_cast<uint64_t>(tcbVirtAddr);

    activeThreads.push_back(thread);
    return thread;
}

void destroyThread(Thread *thread) {
    activeThreads.remove(thread);
    mm::phys::pageFree((void *)thread->fsbase);
    mm::phys::pageFree((void *)thread->stack);
    delete thread;
}
}  // namespace ker::mod::sched::threading
