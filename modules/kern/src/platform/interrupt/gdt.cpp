#include "gdt.hpp"

#include <platform/smt/smt.hpp>

namespace ker::mod::desc::gdt {
static __attribute__((aligned(64))) Gdt gdt;
static bool gdtEntriesInitialized = false;

// Per-CPU TSS storage (allocated dynamically)
static Tss_t* perCpuTss = nullptr;
static constexpr size_t MAX_CPUS = 256;

void setTssEntry(uint64_t base, uint8_t flags, uint8_t access) {
    gdt.tss.size = 104;
    gdt.tss.base_low = base & 0xFFFF;
    gdt.tss.base_middle = (base >> 16) & 0xFF;
    gdt.tss.access = access;
    gdt.tss.flags = flags;
    gdt.tss.base_high = (base >> 24) & 0xFF;
    gdt.tss.base_higher = (base >> 32);
    gdt.tss.reserved = 0;
}

void initTss(uint64_t* stackPointer) {
    // Allocate per-CPU TSS array if not already done
    if (perCpuTss == nullptr) {
        perCpuTss = new Tss_t[MAX_CPUS];
        memset(perCpuTss, 0, sizeof(Tss_t) * MAX_CPUS);
    }

    uint64_t cpuId = ker::mod::cpu::currentCpu();
    Tss_t* tss = &perCpuTss[cpuId];

    setTssEntry((uintptr_t)tss, 0x20, 0x89);
    memset((void*)tss, 0, sizeof(Tss_t));

    tss->rsp[0] = (uint64_t)stackPointer;
    tss->ist[0] = 0;  // Disable IST
}

GdtEntry constexpr makeGdtEntry(uint32_t limit, uint32_t base, GdtFlags flags) {
    GdtEntry entry;
    entry.limit_low = limit & 0xFFFF;
    entry.base_low = base & 0xFFFF;
    entry.base_middle = (base >> 16) & 0xFF;
    entry.segmentType = flags.segmentType;
    entry.descriptorType = flags.descriptorType;
    entry.dpl = flags.dpl;
    entry.present = flags.present;
    entry.limit_high = (limit >> 16) & 0xF;
    entry.avl = flags.avl;
    entry.longMode = flags.longMode;
    entry.defaultSize = flags.defaultSize;
    entry.granularity = flags.granularity;
    entry.base_high = (base >> 24) & 0xFF;
    return entry;
}

void initGdt(uint64_t* stackPointer) {
    // Only initialize GDT entries once (they're shared across all CPUs)
    if (!gdtEntriesInitialized) {
        /*
        null        -  0x00
        kernel Code -  (present, ring 0, type: code, non-conforming, readable)
        kernel Data -  (present, ring 0, type: data, expand-down)
        user Code   -  (present, ring 3, type: code, non-conforming, readable)
        user Data   -  (present, ring 3, type: data, expand-down)
        */
        gdt.memorySegments[GDT_ENTRY_NULL] = makeGdtEntry(0, 0,
                                                          {
                                                              .segmentType = 0,
                                                              .descriptorType = 0,
                                                              .dpl = 0,
                                                              .present = 0,
                                                              .avl = 0,
                                                              .longMode = 0,
                                                              .defaultSize = 0,
                                                              .granularity = 0,
                                                          });  // null
        gdt.memorySegments[GDT_ENTRY_KERNEL_CODE] = makeGdtEntry(0, 0,
                                                                 {
                                                                     .segmentType = 0xA,
                                                                     .descriptorType = 1,
                                                                     .dpl = 0,
                                                                     .present = 1,
                                                                     .avl = 0,
                                                                     .longMode = 1,
                                                                     .defaultSize = 0,
                                                                     .granularity = 1,
                                                                 });  // kernel Code
        gdt.memorySegments[GDT_ENTRY_KERNEL_DATA] = makeGdtEntry(0, 0,
                                                                 {
                                                                     .segmentType = 0x3,
                                                                     .descriptorType = 1,
                                                                     .dpl = 0,
                                                                     .present = 1,
                                                                     .avl = 0,
                                                                     .longMode = 1,
                                                                     .defaultSize = 0,
                                                                     .granularity = 1,
                                                                 });  // kernel Data
        gdt.memorySegments[GDT_ENTRY_USER_CODE] = makeGdtEntry(0, 0,
                                                               {
                                                                   .segmentType = 0xA,
                                                                   .descriptorType = 1,
                                                                   .dpl = 3,
                                                                   .present = 1,
                                                                   .avl = 0,
                                                                   .longMode = 1,
                                                                   .defaultSize = 0,
                                                                   .granularity = 1,
                                                               });  // user Code
        gdt.memorySegments[GDT_ENTRY_USER_DATA] = makeGdtEntry(0, 0,
                                                               {
                                                                   .segmentType = 0x3,  // Data: writable, expand-up
                                                                   .descriptorType = 1,
                                                                   .dpl = 3,
                                                                   .present = 1,
                                                                   .avl = 0,
                                                                   .longMode = 1,
                                                                   .defaultSize = 0,
                                                                   .granularity = 1,
                                                               });  // user Data

        gdt.ptr = {sizeof(Gdt) - 1, (uint64_t)&gdt};
        gdt.ptr.base = (uint64_t)&gdt.memorySegments;
        gdt.ptr.limit = sizeof(gdt.memorySegments) + sizeof(gdt.tss) - 1;

        gdtEntriesInitialized = true;
    }

    // Initialize per-CPU TSS (must be done on each CPU)
    initTss(stackPointer);
}

extern "C" void loadGdt(uint64_t gdtr);

static inline void loadTss(uint16_t tss_selector) { asm volatile("ltr %%ax" ::"a"(tss_selector) : "memory"); }

void initDescriptors(uint64_t* stackPointer) {
    initGdt(stackPointer);
    loadGdt((uint64_t)&gdt.ptr);
    loadTss(GDT_ENTRY_TSS * 8);
}
}  // namespace ker::mod::desc::gdt
