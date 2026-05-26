#pragma once

#include <extern/limine.h>

#include <cstddef>
#include <defines/defines.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/mm/addr.hpp>

namespace ker::mod::desc::gdt {
static constexpr size_t MAX_CPUS = 256;

// GDT Setup
constexpr static uint64_t GDT_ENTRY_NULL = 0;
constexpr static uint64_t GDT_ENTRY_KERNEL_CODE = 1;
constexpr static uint64_t GDT_ENTRY_KERNEL_DATA = 2;
constexpr static uint64_t GDT_ENTRY_USER_DATA = 3;
constexpr static uint64_t GDT_ENTRY_USER_CODE = 4;
// 2 entries for TSS technically count in the gdt count but are stored in a separate struct
constexpr static uint64_t GDT_TSS_OFFSET = 2;
constexpr static uint64_t GDT_ENTRY_TSS = 5;

constexpr static uint64_t GDT_ENTRY_COUNT = 7 - GDT_TSS_OFFSET;

constexpr static uint64_t GDT_KERN_CS = 0x08;
constexpr static uint64_t GDT_KERN_DS = 0x10;

constexpr static uint64_t GDT_USER_CS = 0x23;
constexpr static uint64_t GDT_USER_DS = 0x1b;

constexpr static uint64_t GDT_RING3 = 0x3;

struct TssDescriptor {
    uint16_t size;
    uint16_t base_low;
    uint8_t base_middle;
    uint8_t access;
    uint8_t flags;
    uint8_t base_high;
    uint32_t base_higher;
    uint32_t reserved;
};

struct TssType {
    uint32_t reserved0;
    uint64_t rsp[3];  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    uint64_t reserved1;
    uint64_t ist[7];  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    uint64_t interrupt_ssp_table;
    uint16_t reserved3;
    uint16_t iomap_base;
} __attribute__((packed));

// global descriptor table long mode
struct GdtEntry {
    uint16_t limit_low;
    uint16_t base_low;
    uint8_t base_middle;
    uint8_t segment_type : 4;
    uint8_t descriptor_type : 1;
    uint8_t dpl : 2;
    uint8_t present : 1;
    uint8_t limit_high : 4;
    uint8_t avl : 1;
    uint8_t long_mode : 1;
    uint8_t default_size : 1;
    uint8_t granularity : 1;
    uint8_t base_high;
} __attribute__((packed));

struct GdtFlags {
    uint8_t segment_type : 4;     // type of segment
    uint8_t descriptor_type : 1;  // 0 for system, 1 for code/data
    uint8_t dpl : 2;              // descriptor privilege level
    uint8_t present : 1;          // 1 for valid entries
    uint8_t avl : 1;              // available for use by system software
    uint8_t long_mode : 1;        // 64-bit code segment
    uint8_t default_size : 1;     // 0 for 64-bit code segment
    uint8_t granularity : 1;      // 0 for 1 byte granularity, 1 for 4KB granularity
};

struct GdtPtr {
    uint16_t limit;
    uint64_t base;
} __attribute__((packed));

struct Gdt {
    GdtEntry memory_segments[GDT_ENTRY_COUNT];  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    TssDescriptor tss;
    GdtPtr ptr;
} __attribute__((packed));

static_assert(sizeof(TssDescriptor) == 16);
static_assert(sizeof(TssType) == 104);
static_assert(offsetof(TssType, rsp) == 4);
static_assert(offsetof(TssType, ist) == 36);
static_assert(offsetof(TssType, iomap_base) == 102);
static_assert(sizeof(GdtEntry) == 8);
static_assert(sizeof(GdtPtr) == 10);
static_assert(sizeof(Gdt) == 66);
static_assert(offsetof(Gdt, memory_segments) == 0);
static_assert(offsetof(Gdt, tss) == 40);
static_assert(offsetof(Gdt, ptr) == 56);

void init_descriptors(uint64_t* stack_pointer, uint64_t cpu_id);
}  // namespace ker::mod::desc::gdt
