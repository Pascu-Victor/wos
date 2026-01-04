#include "paging.hpp"

namespace ker::mod::mm::paging {
static inline bool isFlagSet(uint64_t flags, uint64_t flag) { return flags >> flag & 1; }

PageTableEntry createPageTableEntry(uint64_t frame, uint64_t flags) {
    PageTableEntry entry = {};  // CRITICAL: Zero-initialize to clear reserved bits and pagesize
    entry.frame = frame >> PAGE_SHIFT;
    entry.present = (flags & PAGE_PRESENT) > 0;
    entry.writable = (flags & PAGE_WRITE) > 0;
    entry.user = (flags & PAGE_USER) > 0;
    entry.writeThrough = 0;
    entry.cacheDisabled = 0;
    entry.accessed = 0;
    entry.dirty = 0;
    entry.pagesize = 0;  // 4KB pages, not large pages
    entry.global = 0;
    entry.available = 0;
    entry.reserved = 0;  // x86-64 requires reserved bits to be 0
    entry.noExecute = (flags & PAGE_NX) ? 1 : 0;
    return entry;
}

PageTableEntry purgePageTableEntry(void) { return createPageTableEntry(0, 0); }

PageFault createPageFault(uint64_t flags, bool isCritical) {
    PageFault fault;
    fault.present = 1;
    fault.writable = isFlagSet(flags, errorFlags::WRITE);
    fault.user = isFlagSet(flags, errorFlags::USER);
    fault.reserved = 0;
    fault.fetch = isFlagSet(flags, errorFlags::FETCH);
    fault.protectionKey = isFlagSet(flags, errorFlags::PROTECTION_KEY);
    fault.shadowStack = isFlagSet(flags, errorFlags::SHADOW_STACK);
    fault.criticalHandling = isCritical && (fault.fetch || fault.protectionKey || fault.shadowStack);
    fault.flags = fault.present | fault.writable << 1 | fault.user << 2 | fault.reserved << 3 | fault.fetch << 4 |
                  fault.protectionKey << 5 | fault.shadowStack << 6;
    return fault;
}
}  // namespace ker::mod::mm::paging
