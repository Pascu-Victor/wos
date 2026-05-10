#include "paging.hpp"

#include <cstdint>

namespace ker::mod::mm::paging {
namespace {
auto is_flag_set(uint64_t flags, uint64_t flag) -> bool { return (flags >> flag & 1) != 0U; }
}  // namespace

auto create_page_table_entry(uint64_t frame, uint64_t flags) -> PageTableEntry {
    PageTableEntry entry = {};  // CRITICAL: Zero-initialize to clear reserved bits and pagesize
    entry.frame = frame >> PAGE_SHIFT;
    entry.present = (flags & PAGE_PRESENT) > 0;
    entry.writable = (flags & PAGE_WRITE) > 0;
    entry.user = (flags & PAGE_USER) > 0;
    entry.write_through = ((flags & PAGE_PWT) != 0U) ? 1 : 0;
    entry.cache_disabled = ((flags & PAGE_PCD) != 0U) ? 1 : 0;
    entry.accessed = 0;
    entry.dirty = 0;
    entry.pagesize = 0;  // 4KB pages, not large pages
    entry.global = 0;
    entry.available = 0;
    entry.reserved = 0;  // x86-64 requires reserved bits to be 0
    entry.no_execute = ((flags & PAGE_NX) != 0U) ? 1 : 0;
    return entry;
}

auto purge_page_table_entry() -> PageTableEntry { return create_page_table_entry(0, 0); }

auto create_page_fault(uint64_t flags, bool is_critical) -> PageFault {
    PageFault fault{};
    fault.present = 1;
    fault.writable = static_cast<uint8_t>(is_flag_set(flags, error_flags::WRITE));
    fault.user = static_cast<uint8_t>(is_flag_set(flags, error_flags::USER));
    fault.reserved = 0;
    fault.fetch = static_cast<uint8_t>(is_flag_set(flags, error_flags::FETCH));
    fault.protection_key = static_cast<uint8_t>(is_flag_set(flags, error_flags::PROTECTION_KEY));
    fault.shadow_stack = static_cast<uint8_t>(is_flag_set(flags, error_flags::SHADOW_STACK));
    fault.critical_handling =
        static_cast<uint8_t>(is_critical && ((fault.fetch != 0U) || (fault.protection_key != 0U) || (fault.shadow_stack != 0U)));
    fault.flags = fault.present | fault.writable << 1 | fault.user << 2 | fault.reserved << 3 | fault.fetch << 4 |
                  fault.protection_key << 5 | fault.shadow_stack << 6;
    return fault;
}
}  // namespace ker::mod::mm::paging
