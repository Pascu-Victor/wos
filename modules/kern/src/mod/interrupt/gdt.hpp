#pragma once

#include <limine.h>

#include <defines/defines.hpp>
#include <mod/io/serial/serial.hpp>

namespace ker::mod::desc::gdt {

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
        uint64_t reserved2;
        uint16_t reserved3;
        uint16_t iomap_base;
    } __attribute__((packed));

    //global descriptor table long mode
    struct GdtEntry {
        uint16_t limit_low;
        uint16_t base_low;
        uint8_t base_middle;
        uint8_t access;
        uint8_t granularity;
        uint8_t base_high;
    } __attribute__((packed));

    struct GdtPtr {
        uint16_t limit;
        uint64_t base;
    } __attribute__((packed));

    struct Gdt {
        GdtEntry memorySegments[5];
        TssDescriptor tss;
        GdtPtr ptr;
    } __attribute__((packed));

    //GDT Setup

    const static uint64_t GDT_ENTRIES = 6;

    const static uint64_t GDT_ENTRY_NULL = 0;
    const static uint64_t GDT_ENTRY_KERNEL_CODE = 1;
    const static uint64_t GDT_ENTRY_KERNEL_DATA = 2;
    const static uint64_t GDT_ENTRY_USER_CODE = 3;
    const static uint64_t GDT_ENTRY_USER_DATA = 4;
    const static uint64_t GDT_ENTRY_TSS = 5;

    const static uint64_t GDT_KERN_CS = 0x08;
    const static uint64_t GDT_KERN_DS = 0x10;

    void initDescriptors(uint64_t stackPointer);
}