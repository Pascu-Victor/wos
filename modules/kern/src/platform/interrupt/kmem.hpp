#pragma once
#include <limine.h>

#include <defines/defines.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gdt.hpp>
namespace ker::mod::mem {
const static uint64_t NUM_MMAP_ENTRIES = 256;

const static constexpr uint64_t PAGE_SIZE = 4096;

struct page_table_entry {
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

static volatile page_table_entry* __page_table = nullptr;

void init(void);

uint64_t mmap_read(uint64_t addr, uint64_t offset);

uint64_t allocate_frame(void);

void free_frame(uint64_t addr);

// TODO: void* sbrk(uint64_t increment);

// TODO: void* brk(void* addr);

// TODO: void* malloc(size_t size);
}  // namespace ker::mod::mem
