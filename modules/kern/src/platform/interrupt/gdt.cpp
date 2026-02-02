#include "gdt.hpp"

#include <platform/smt/smt.hpp>

namespace ker::mod::desc::gdt {

// Shared GDT entries (code/data segments are identical for all CPUs)
static __attribute__((aligned(64))) GdtEntry sharedGdtEntries[GDT_ENTRY_COUNT];
static bool gdtEntriesInitialized = false;

// Per-CPU GDT structures (each CPU needs its own TSS descriptor and GDTR)
// Statically allocated since heap may not be available during early boot
struct PerCpuGdt {
    GdtEntry memorySegments[GDT_ENTRY_COUNT];
    TssDescriptor tss;
    GdtPtr ptr;
    Tss_t tss_data;
} __attribute__((packed, aligned(64)));

static constexpr size_t MAX_CPUS = 256;
static PerCpuGdt perCpuGdt[MAX_CPUS];

void setTssEntry(TssDescriptor* tssDesc, uint64_t base, uint8_t flags, uint8_t access) {
    tssDesc->size = 104;
    tssDesc->base_low = base & 0xFFFF;
    tssDesc->base_middle = (base >> 16) & 0xFF;
    tssDesc->access = access;
    tssDesc->flags = flags;
    tssDesc->base_high = (base >> 24) & 0xFF;
    tssDesc->base_higher = (base >> 32);
    tssDesc->reserved = 0;
}

void initTss(uint64_t* stackPointer, uint64_t cpuId) {
    PerCpuGdt* cpuGdt = &perCpuGdt[cpuId];
    Tss_t* tss = &cpuGdt->tss_data;

    setTssEntry(&cpuGdt->tss, (uintptr_t)tss, 0x20, 0x89);
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

void initGdt(uint64_t* stackPointer, uint64_t cpuId) {
    // Initialize shared GDT entries once
    if (!gdtEntriesInitialized) {
        /*
        null        -  0x00
        kernel Code -  (present, ring 0, type: code, non-conforming, readable)
        kernel Data -  (present, ring 0, type: data, expand-down)
        user Code   -  (present, ring 3, type: code, non-conforming, readable)
        user Data   -  (present, ring 3, type: data, expand-down)
        */
        sharedGdtEntries[GDT_ENTRY_NULL] = makeGdtEntry(0, 0,
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
        sharedGdtEntries[GDT_ENTRY_KERNEL_CODE] = makeGdtEntry(0, 0,
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
        sharedGdtEntries[GDT_ENTRY_KERNEL_DATA] = makeGdtEntry(0, 0,
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
        sharedGdtEntries[GDT_ENTRY_USER_CODE] = makeGdtEntry(0, 0,
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
        sharedGdtEntries[GDT_ENTRY_USER_DATA] = makeGdtEntry(0, 0,
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

        gdtEntriesInitialized = true;
    }

    // Copy shared GDT entries to this CPU's GDT
    PerCpuGdt* cpuGdt = &perCpuGdt[cpuId];
    memcpy(cpuGdt->memorySegments, sharedGdtEntries, sizeof(sharedGdtEntries));

    // Initialize per-CPU TSS
    initTss(stackPointer, cpuId);

    // Set up GDTR for this CPU (points to this CPU's GDT)
    cpuGdt->ptr.base = (uint64_t)&cpuGdt->memorySegments;
    cpuGdt->ptr.limit = sizeof(cpuGdt->memorySegments) + sizeof(cpuGdt->tss) - 1;
}

extern "C" void loadGdt(uint64_t gdtr);

static inline void loadTss(uint16_t tss_selector) { asm volatile("ltr %%ax" ::"a"(tss_selector) : "memory"); }

void initDescriptors(uint64_t* stackPointer, uint64_t cpuId) {
    initGdt(stackPointer, cpuId);
    loadGdt((uint64_t)&perCpuGdt[cpuId].ptr);
    loadTss(GDT_ENTRY_TSS * 8);
}
}  // namespace ker::mod::desc::gdt
