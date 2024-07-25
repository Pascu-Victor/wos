#include "paging.hpp"

namespace ker::mod::mm::paging {
    static inline bool isFlagSet(uint64_t flags, uint64_t flag) {
        return (flags & flag) == flag;
    }

    PageTableEntry createPageTableEntry(uint64_t frame, uint64_t flags) {
        PageTableEntry entry;
        entry.frame = frame * PAGE_SIZE;
        entry.present = flags & PAGE_PRESENT;
        entry.writable = flags & PAGE_WRITE;
        entry.user = flags & PAGE_USER;
        entry.writeThrough = 0;
        entry.cacheDisabled = 0;
        entry.accessed = 0;
        entry.dirty = 0;
        entry.huge_page = 0;
        entry.global = 0;
        entry.no_execute = 0;
        entry.available = 0;
        return entry;
    }

    PageTableEntry purgePageTableEntry(void) {
        return createPageTableEntry(0, 0);
    }

    PageFault createPageFault(uint64_t flags, bool isCritical) {
        PageFault fault;
        fault.present = 1;
        fault.writable = isFlagSet(flags, errorFlags::WRITE);
        fault.user = isFlagSet(flags, errorFlags::USER);
        fault.reserved = 0;
        fault.fetch = isFlagSet(flags, errorFlags::FETCH);
        fault.protectionKey = isFlagSet(flags, errorFlags::PROTECTION_KEY);
        fault.shadowStack = isFlagSet(flags, errorFlags::SHADOW_STACK);
        fault.criticalHandling = isCritical &&
            (  fault.fetch
            || fault.protectionKey
            || fault.shadowStack );
        fault.flags = fault.user | fault.writable | 0x1;
        return fault;
    }
}