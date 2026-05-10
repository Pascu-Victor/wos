#include "gdb_interface.hpp"

#include <cstdint>
#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sys/spinlock.hpp>

#include "platform/mm/addr.hpp"

namespace ker::loader::debug {

GdbDebugInfo* gdb_debug_info_chain = nullptr;

namespace {

using log = ker::mod::dbg::logger<"gdb">;

ker::mod::sys::Spinlock gdb_debug_info_lock;

}  // namespace

void init_gdb_debug_info() {
    gdb_debug_info_chain = nullptr;
    log::info("initialized debug info chain");
}

void add_gdb_debug_info(uint64_t pid, const char* name, uint64_t base_addr, uint64_t entry_point) {
    // Allocate memory for the debug info structure
    auto const DEBUG_INFO_PADDR = reinterpret_cast<uint64_t>(ker::mod::mm::phys::page_alloc());
    if (DEBUG_INFO_PADDR == 0) {
        log::error("failed to allocate memory for debug info");
        return;
    }

    auto* debug_info = reinterpret_cast<GdbDebugInfo*>(ker::mod::mm::addr::get_phys_pointer(DEBUG_INFO_PADDR));

    // Initialize the debug info structure
    debug_info->magic = 0x47444255;  // 'GDBU' in little endian
    debug_info->pid = pid;
    std::strncpy(debug_info->name, name, sizeof(debug_info->name) - 1);  // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    debug_info->name[sizeof(debug_info->name) - 1] = '\0';
    debug_info->base_address = base_addr;
    debug_info->entry_point = entry_point;

    // Initialize other fields
    debug_info->elf_header_addr = 0;
    debug_info->section_count = 0;
    debug_info->section_headers_addr = 0;
    debug_info->string_table_addr = 0;
    debug_info->string_table_size = 0;
    debug_info->program_header_count = 0;
    debug_info->program_headers_addr = 0;
    debug_info->debug_info_addr = 0;
    debug_info->debug_info_size = 0;
    debug_info->debug_line_addr = 0;
    debug_info->debug_line_size = 0;
    debug_info->debug_str_addr = 0;
    debug_info->debug_str_size = 0;

    // Link into the chain
    debug_info->next_process_addr = reinterpret_cast<uint64_t>(gdb_debug_info_chain);
    gdb_debug_info_chain = debug_info;

    log::info("added debug info for process %s (PID %x) at %x", name, pid, debug_info);
}

void update_gdb_debug_section(uint64_t pid, const char* section_name, uint64_t addr, uint64_t size) {
    // Find the debug info for this process
    GdbDebugInfo* current = gdb_debug_info_chain;
    while (current != nullptr) {
        if (current->pid == pid) {
            // Update the appropriate section
            if (std::strncmp(section_name, ".debug_info", 11) == 0) {
                current->debug_info_addr = addr;
                current->debug_info_size = size;
            } else if (std::strncmp(section_name, ".debug_line", 11) == 0) {
                current->debug_line_addr = addr;
                current->debug_line_size = size;
            } else if (std::strncmp(section_name, ".debug_str", 10) == 0) {
                current->debug_str_addr = addr;
                current->debug_str_size = size;
            }

            log::debug("updated section %s for PID %x: addr=%x, size=%x", section_name, pid, addr, size);
            return;
        }
        current = reinterpret_cast<GdbDebugInfo*>(current->next_process_addr);
    }
}

void finalize_gdb_debug_info(uint64_t pid) {
    // Find the debug info for this process
    GdbDebugInfo* current = gdb_debug_info_chain;
    while (current != nullptr) {
        if (current->pid == pid) {
            log::info("finalized debug info for PID %x", pid);
            log::info("  Name: %s", current->name);  // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            log::info("  Base: %x, Entry: %x", current->base_address, current->entry_point);
            log::info("  ELF Header: %x", current->elf_header_addr);
            log::info("  Section Headers: %x (count: %d)", current->section_headers_addr, current->section_count);
            log::info("  String Table: %x (size: %x)", current->string_table_addr, current->string_table_size);
            log::info("  Debug Info: %x (size: %x)", current->debug_info_addr, current->debug_info_size);
            log::info("  Debug Line: %x (size: %x)", current->debug_line_addr, current->debug_line_size);
            log::info("  Debug Str: %x (size: %x)", current->debug_str_addr, current->debug_str_size);
            return;
        }
        current = reinterpret_cast<GdbDebugInfo*>(current->next_process_addr);
    }
}

void remove_gdb_debug_info(uint64_t pid) {
    gdb_debug_info_lock.lock();
    GdbDebugInfo* prev = nullptr;
    GdbDebugInfo* current = gdb_debug_info_chain;

    while (current != nullptr) {
        if (current->pid == pid) {
            // Remove from linked list
            if (prev == nullptr) {
                // Head of list
                gdb_debug_info_chain = reinterpret_cast<GdbDebugInfo*>(current->next_process_addr);
            } else {
                prev->next_process_addr = current->next_process_addr;
            }

            // Free the page that was allocated for this debug info
            ker::mod::mm::phys::page_free(current);
            gdb_debug_info_lock.unlock();
            return;
        }
        prev = current;
        current = reinterpret_cast<GdbDebugInfo*>(current->next_process_addr);
    }
    gdb_debug_info_lock.unlock();
}

// Function to be called by GDB to get debug info
extern "C" GdbDebugInfo* get_gdb_debug_info() { return gdb_debug_info_chain; }

}  // namespace ker::loader::debug
