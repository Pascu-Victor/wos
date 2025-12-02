#pragma once

#include <buddy_alloc/buddy_alloc.hpp>
#include <cstdint>
#include <defines/defines.hpp>

#define PAGE_ALIGN_UP(addr) (((addr) + ker::mod::mm::paging::PAGE_SIZE - 1) & (~(ker::mod::mm::paging::PAGE_SIZE - 1)))
#define PAGE_ALIGN_DOWN(addr) ((addr) & ~(ker::mod::mm::paging::PAGE_SIZE - 1))

namespace ker::mod::mm::paging {
const static uint64_t PAGE_SHIFT = 12;
const static uint64_t PAGE_SIZE = 0x1000;
struct PageZone {
    PageZone* next;
    buddy* buddyInstance;
    uint64_t start;
    uint64_t len;
    size_t pageCount;
    uint64_t zoneNum;
    const char* name;
};

struct PageTableEntry {
    uint8_t present : 1;
    uint8_t writable : 1;
    uint8_t user : 1;
    uint8_t writeThrough : 1;
    uint8_t cacheDisabled : 1;
    uint8_t accessed : 1;
    uint8_t dirty : 1;
    uint8_t pagesize : 1;
    uint8_t global : 1;
    uint8_t available : 3;
    uint64_t frame : 40;
    uint64_t reserved : 11;
    uint64_t noExecute : 1;  // NX bit (if EFER.NXE enabled)
} __attribute__((packed));

struct PageTable {
    PageTableEntry entries[512];
} __attribute__((packed));

struct PageFault {
    uint8_t present;
    uint8_t writable;
    uint8_t user;
    uint8_t reserved;
    uint8_t fetch;
    uint8_t protectionKey;
    uint8_t shadowStack;
    uint8_t criticalHandling;  // 1 = Panic if unhandled
    uint64_t flags;
} __attribute__((packed));

const static uint64_t PAGE_PRESENT = 0x1;
const static uint64_t PAGE_WRITE = 0x2;
const static uint64_t PAGE_USER = 0x4;
const static uint64_t PAGE_NX = (1ULL << 63);

namespace pageTypes {
const static uint64_t READONLY = PAGE_PRESENT;
const static uint64_t KERNEL = PAGE_PRESENT | PAGE_WRITE;
const static uint64_t USER = PAGE_PRESENT | PAGE_WRITE | PAGE_USER;
const static uint64_t USER_READONLY = PAGE_PRESENT | PAGE_USER;
}  // namespace pageTypes

namespace errorFlags {
const static uint64_t WRITE = 1;
const static uint64_t USER = 2;
const static uint64_t FETCH = 4;
const static uint64_t PROTECTION_KEY = 5;
const static uint64_t SHADOW_STACK = 6;
}  // namespace errorFlags

PageTableEntry createPageTableEntry(uint64_t frame, uint64_t flags);
PageTableEntry purgePageTableEntry(void);
PageFault createPageFault(uint64_t flags, bool isCritical = false);

inline uint64_t align(uint64_t size, uint64_t align) { return (size + align - 1) & ~(align - 1); }

}  // namespace ker::mod::mm::paging
