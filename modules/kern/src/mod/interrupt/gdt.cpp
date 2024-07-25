#include "gdt.hpp"

namespace ker::mod::desc::gdt {
    static __attribute__((aligned(64))) Gdt gdt;
    
    void setTssEntry(uint64_t base, uint8_t flags, uint8_t access)
    {
        gdt.tss.size        = 104;
        gdt.tss.base_low    = base & 0xFFFF;
        gdt.tss.base_middle = (base >> 16) & 0xFF;
        gdt.tss.access      = access;
        gdt.tss.flags       = flags;
        gdt.tss.base_high   = (base >> 24) & 0xFF;
        gdt.tss.base_higher = (base >> 32);
        gdt.tss.reserved    = 0;
    }

    static Tss_t tss;
    void initTss(uint64_t stackPointer)
    {
        setTssEntry((uintptr_t)&tss, 0x20, 0x89);
        memset((void *)&tss, 0, sizeof(Tss_t));

        tss.rsp[0] = stackPointer;
        tss.ist[0] = 0;  // Disable IST
    }

    void initGdt(uint64_t stackPointer) {
        gdt.memorySegments[0] = {0, 0, 0, 0,    0,    0}; //null
        gdt.memorySegments[1] = {0, 0, 0, 0x9A, 0xA2, 0}; //kernelCode
        gdt.memorySegments[2] = {0, 0, 0, 0x92, 0xA0, 0}; //kernelData
        gdt.memorySegments[3] = {0, 0, 0, 0xF2, 0,    0}; //userCode
        gdt.memorySegments[4] = {0, 0, 0, 0xFA, 0x20, 0}; //userData
        gdt.ptr = {sizeof(Gdt) - 1, (uint64_t)&gdt};

        initTss(stackPointer);

        gdt.ptr.base = (uint64_t)&gdt.memorySegments;
        gdt.ptr.limit = sizeof(Gdt) * 6 - 1;
    }

    extern "C" void loadGdt(uint64_t gdtr);
    
    static inline void loadTss(uint16_t tss_selector)
    {
        asm volatile("ltr %%ax" ::"a"(tss_selector)
                    : "memory");
    }
    
    void initDescriptors(uint64_t stackPointer) {
        initGdt(stackPointer);
        loadGdt((uint64_t)&gdt.ptr);
        loadTss(0x28);
    }
}

