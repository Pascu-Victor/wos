#pragma once
#include <extern/limine.h>

#include <cstdint>
#include <defines/defines.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gdt.hpp>
namespace ker::mod::mem {
constexpr uint64_t NUM_MMAP_ENTRIES = 256;

constexpr uint64_t PAGE_SIZE = 4096;

struct PageTableEntry {
    uint64_t present : 1;
    uint64_t writable : 1;
    uint64_t user : 1;
    uint64_t write_through : 1;
    uint64_t cache_disable : 1;
    uint64_t accessed : 1;
    uint64_t dirty : 1;
    uint64_t huge : 1;
    uint64_t global : 1;
    uint64_t avail : 3;
    uint64_t frame : 40;
    uint64_t reserved : 11;
    uint64_t no_execute : 1;
} __attribute__((packed));
static_assert(sizeof(PageTableEntry) == sizeof(uint64_t));
}  // namespace ker::mod::mem
