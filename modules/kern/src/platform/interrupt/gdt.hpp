#pragma once

#include <limine.h>

#include <defines/defines.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/mm/addr.hpp>

namespace ker::mod::desc::gdt {
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

struct Tss_t {
    uint32_t reserved0;
    uint64_t rsp[3];
    uint64_t reserved1;
    uint64_t ist[7];
    uint64_t interruptSSPTable;
    uint16_t reserved3;
    uint16_t iomap_base;
} __attribute__((packed));

// global descriptor table long mode
struct GdtEntry {
    uint16_t limit_low;
    uint16_t base_low;
    uint8_t base_middle;
    uint8_t segmentType : 4;
    uint8_t descriptorType : 1;
    uint8_t dpl : 2;
    uint8_t present : 1;
    uint8_t limit_high : 4;
    uint8_t avl : 1;
    uint8_t longMode : 1;
    uint8_t defaultSize : 1;
    uint8_t granularity : 1;
    uint8_t base_high;
} __attribute__((packed));

struct GdtFlags {
    uint8_t segmentType : 4;     // type of segment
    uint8_t descriptorType : 1;  // 0 for system, 1 for code/data
    uint8_t dpl : 2;             // descriptor privilege level
    uint8_t present : 1;         // 1 for valid entries
    uint8_t avl : 1;             // available for use by system software
    uint8_t longMode : 1;        // 64-bit code segment
    uint8_t defaultSize : 1;     // 0 for 64-bit code segment
    uint8_t granularity : 1;     // 0 for 1 byte granularity, 1 for 4KB granularity
};

struct GdtPtr {
    uint16_t limit;
    uint64_t base;
} __attribute__((packed));

struct Gdt {
    GdtEntry memorySegments[GDT_ENTRY_COUNT];
    TssDescriptor tss;
    GdtPtr ptr;
} __attribute__((packed));

void initDescriptors(uint64_t* stackPointer);
}  // namespace ker::mod::desc::gdt
