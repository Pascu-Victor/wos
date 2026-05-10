#include "gdt.hpp"

#include <cstdint>
#include <cstring>

namespace ker::mod::desc::gdt {

// Shared GDT entries (code/data segments are identical for all CPUs)
static __attribute__((aligned(64))) GdtEntry shared_gdt_entries[GDT_ENTRY_COUNT];
static bool gdt_entries_initialized = false;

// Per-CPU GDT structures (each CPU needs its own TSS descriptor and GDTR)
// Statically allocated since heap may not be available during early boot
struct PerCpuGdt {
    GdtEntry memory_segments[GDT_ENTRY_COUNT];
    TssDescriptor tss;
    GdtPtr ptr;
    TssType tss_data;
} __attribute__((packed, aligned(64)));

static PerCpuGdt per_cpu_gdt[MAX_CPUS];

static void set_tss_entry(TssDescriptor* tss_desc, uint64_t base, uint8_t flags, uint8_t access) {
    tss_desc->size = 104;
    tss_desc->base_low = base & 0xFFFF;
    tss_desc->base_middle = (base >> 16) & 0xFF;
    tss_desc->access = access;
    tss_desc->flags = flags;
    tss_desc->base_high = (base >> 24) & 0xFF;
    tss_desc->base_higher = (base >> 32);
    tss_desc->reserved = 0;
}

static void init_tss(const uint64_t* stack_pointer, uint64_t cpu_id) {
    PerCpuGdt* cpu_gdt = &per_cpu_gdt[cpu_id];
    TssType* tss = &cpu_gdt->tss_data;

    set_tss_entry(&cpu_gdt->tss, (uintptr_t)tss, 0x20, 0x89);
    memset(reinterpret_cast<void*>(tss), 0, sizeof(TssType));

    tss->rsp[0] = (uint64_t)stack_pointer;
    tss->ist[0] = 0;  // Disable IST
}

static auto constexpr make_gdt_entry(uint32_t limit, uint32_t base, GdtFlags flags) -> GdtEntry {
    GdtEntry entry{};
    entry.limit_low = limit & 0xFFFF;
    entry.base_low = base & 0xFFFF;
    entry.base_middle = (base >> 16) & 0xFF;
    entry.segment_type = flags.segment_type;
    entry.descriptor_type = flags.descriptor_type;
    entry.dpl = flags.dpl;
    entry.present = flags.present;
    entry.limit_high = (limit >> 16) & 0xF;
    entry.avl = flags.avl;
    entry.long_mode = flags.long_mode;
    entry.default_size = flags.default_size;
    entry.granularity = flags.granularity;
    entry.base_high = (base >> 24) & 0xFF;
    return entry;
}

static void init_gdt(uint64_t* stack_pointer, uint64_t cpu_id) {
    // Initialize shared GDT entries once
    if (!gdt_entries_initialized) {
        /*
        null        -  0x00
        kernel Code -  (present, ring 0, type: code, non-conforming, readable)
        kernel Data -  (present, ring 0, type: data, expand-down)
        user Code   -  (present, ring 3, type: code, non-conforming, readable)
        user Data   -  (present, ring 3, type: data, expand-down)
        */
        shared_gdt_entries[GDT_ENTRY_NULL] = make_gdt_entry(0, 0,
                                                            {
                                                                .segment_type = 0,
                                                                .descriptor_type = 0,
                                                                .dpl = 0,
                                                                .present = 0,
                                                                .avl = 0,
                                                                .long_mode = 0,
                                                                .default_size = 0,
                                                                .granularity = 0,
                                                            });  // null
        shared_gdt_entries[GDT_ENTRY_KERNEL_CODE] = make_gdt_entry(0, 0,
                                                                   {
                                                                       .segment_type = 0xA,
                                                                       .descriptor_type = 1,
                                                                       .dpl = 0,
                                                                       .present = 1,
                                                                       .avl = 0,
                                                                       .long_mode = 1,
                                                                       .default_size = 0,
                                                                       .granularity = 1,
                                                                   });  // kernel Code
        shared_gdt_entries[GDT_ENTRY_KERNEL_DATA] = make_gdt_entry(0, 0,
                                                                   {
                                                                       .segment_type = 0x3,
                                                                       .descriptor_type = 1,
                                                                       .dpl = 0,
                                                                       .present = 1,
                                                                       .avl = 0,
                                                                       .long_mode = 1,
                                                                       .default_size = 0,
                                                                       .granularity = 1,
                                                                   });  // kernel Data
        shared_gdt_entries[GDT_ENTRY_USER_CODE] = make_gdt_entry(0, 0,
                                                                 {
                                                                     .segment_type = 0xA,
                                                                     .descriptor_type = 1,
                                                                     .dpl = 3,
                                                                     .present = 1,
                                                                     .avl = 0,
                                                                     .long_mode = 1,
                                                                     .default_size = 0,
                                                                     .granularity = 1,
                                                                 });  // user Code
        shared_gdt_entries[GDT_ENTRY_USER_DATA] = make_gdt_entry(0, 0,
                                                                 {
                                                                     .segment_type = 0x3,  // Data: writable, expand-up
                                                                     .descriptor_type = 1,
                                                                     .dpl = 3,
                                                                     .present = 1,
                                                                     .avl = 0,
                                                                     .long_mode = 1,
                                                                     .default_size = 0,
                                                                     .granularity = 1,
                                                                 });  // user Data

        gdt_entries_initialized = true;
    }

    // Copy shared GDT entries to this CPU's GDT
    PerCpuGdt* cpu_gdt = &per_cpu_gdt[cpu_id];
    memcpy(cpu_gdt->memory_segments, shared_gdt_entries, sizeof(shared_gdt_entries));

    // Initialize per-CPU TSS
    init_tss(stack_pointer, cpu_id);

    // Set up GDTR for this CPU (points to this CPU's GDT)
    cpu_gdt->ptr.base = (uint64_t)&cpu_gdt->memory_segments;
    cpu_gdt->ptr.limit = sizeof(cpu_gdt->memory_segments) + sizeof(cpu_gdt->tss) - 1;
}

extern "C" void load_gdt(uint64_t gdtr);

static inline void load_tss(uint16_t tss_selector) { asm volatile("ltr %%ax" ::"a"(tss_selector) : "memory"); }

void init_descriptors(uint64_t* stack_pointer, uint64_t cpu_id) {
    init_gdt(stack_pointer, cpu_id);
    load_gdt((uint64_t)&per_cpu_gdt[cpu_id].ptr);
    load_tss(GDT_ENTRY_TSS * 8);
}
}  // namespace ker::mod::desc::gdt
