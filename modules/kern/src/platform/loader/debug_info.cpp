#include "debug_info.hpp"

#include <cstdint>
#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <platform/sys/spinlock.hpp>
#include <vector>

#include "extern/elf.h"

namespace ker::loader::debug {

std::vector<ProcessDebugInfo> debug_registry;

namespace {

using log = ker::mod::dbg::logger<"debug">;

ker::mod::sys::Spinlock debug_registry_lock;

}  // namespace

void register_process(uint64_t pid, const char* name, uint64_t base_addr, uint64_t entry_point) {
    ProcessDebugInfo info;
    info.pid = pid;
    info.name = name;
    info.base_address = base_addr;
    info.entry_point = entry_point;
    info.sections = std::vector<DebugSection>();

    debug_registry_lock.lock();
    debug_registry.push_back(info);
    debug_registry_lock.unlock();
#ifdef ELF_DEBUG
    log::debug("registered process: pid=%x, name=%s, base=%x, entry=%x", pid, name, base_addr, entry_point);
#endif
}

void add_debug_section(uint64_t pid, const char* name, uint64_t vaddr, uint64_t paddr, uint64_t size, uint64_t file_offset, uint32_t type) {
    debug_registry_lock.lock();
    for (auto& process : debug_registry) {
        if (process.pid == pid) {
            DebugSection section{};
            section.name = name;
            section.vaddr = vaddr;
            section.paddr = paddr;
            section.size = size;
            section.file_offset = file_offset;
            section.type = type;

            process.sections.push_back(section);
            debug_registry_lock.unlock();
#ifdef ELF_DEBUG
            log::debug("added section: %s, vaddr=%x, paddr=%x, size=%x", name, vaddr, paddr, size);
#endif
            return;
        }
    }
    debug_registry_lock.unlock();
}

void add_debug_symbol(uint64_t pid, const char* name, uint64_t vaddr, uint64_t paddr, uint64_t size, uint8_t bind, uint8_t type,
                      bool is_tls_offset, uint16_t shndx, uint64_t raw_value) {
    debug_registry_lock.lock();
    for (auto& process : debug_registry) {
        if (process.pid == pid) {
            DebugSymbol sym{};
            sym.name = name;
            sym.vaddr = vaddr;
            sym.paddr = paddr;
            sym.size = size;
            sym.bind = bind;
            sym.type = type;
            sym.is_tls_offset = is_tls_offset;
            sym.shndx = shndx;
            sym.raw_value = raw_value;

            process.symbols.push_back(sym);
            debug_registry_lock.unlock();
#ifdef ELF_DEBUG
            log::debug("added symbol: %s, vaddr=%x, paddr=%x, size=%x, bind=%d, type=%d", name, vaddr, paddr, size, bind, type);
#endif
            return;
        }
    }
    debug_registry_lock.unlock();
}

void set_elf_headers(uint64_t pid, const Elf64_Ehdr& header, uint64_t header_addr) {
    debug_registry_lock.lock();
    for (auto& process : debug_registry) {
        if (process.pid == pid) {
            process.elf_header = header;
            process.elf_header_addr = header_addr;
            debug_registry_lock.unlock();
            return;
        }
    }
    debug_registry_lock.unlock();
}

void set_program_headers(uint64_t pid, Elf64_Phdr* phdrs, uint64_t phdrs_addr, uint16_t count) {
    debug_registry_lock.lock();
    for (auto& process : debug_registry) {
        if (process.pid == pid) {
            process.program_headers = phdrs;
            process.program_headers_addr = phdrs_addr;
            process.program_header_count = count;
            debug_registry_lock.unlock();
            return;
        }
    }
    debug_registry_lock.unlock();
}

void set_section_headers(uint64_t pid, Elf64_Shdr* shdrs, uint64_t shdrs_addr, uint16_t count) {
    debug_registry_lock.lock();
    for (auto& process : debug_registry) {
        if (process.pid == pid) {
            process.section_headers = shdrs;
            process.section_headers_addr = shdrs_addr;
            process.section_header_count = count;
            debug_registry_lock.unlock();
            return;
        }
    }
    debug_registry_lock.unlock();
}

void set_string_table(uint64_t pid, const char* strtab, uint64_t strtab_addr, uint64_t size) {
    debug_registry_lock.lock();
    for (auto& process : debug_registry) {
        if (process.pid == pid) {
            process.string_table = strtab;
            process.string_table_addr = strtab_addr;
            process.string_table_size = size;
            debug_registry_lock.unlock();
            return;
        }
    }
    debug_registry_lock.unlock();
}

ProcessDebugInfo* get_process_debug_info(uint64_t pid) {
    debug_registry_lock.lock();
    for (auto& process : debug_registry) {
        if (process.pid == pid) {
            debug_registry_lock.unlock();
            return &process;
        }
    }
    debug_registry_lock.unlock();
    return nullptr;
}

DebugSymbol* get_process_symbol(uint64_t pid, const char* name) {
    debug_registry_lock.lock();
    for (auto& process : debug_registry) {
        if (process.pid == pid) {
            for (auto& sym : process.symbols) {
                if (std::strncmp(sym.name, name, 128) == 0) {
                    debug_registry_lock.unlock();
                    return &sym;
                }
            }
            debug_registry_lock.unlock();
            return nullptr;
        }
    }
    debug_registry_lock.unlock();
    return nullptr;
}

void print_debug_info(uint64_t pid) {
    debug_registry_lock.lock();
    ProcessDebugInfo const* info = nullptr;
    for (auto& process : debug_registry) {
        if (process.pid == pid) {
            info = &process;
            break;
        }
    }
    if (info == nullptr) {
        debug_registry_lock.unlock();
        log::warn("no debug info found for PID %x", pid);
        return;
    }

    log::info("process %s (PID %x):", info->name, pid);
    log::info("  Base address: %x", info->base_address);
    log::info("  Entry point: %x", info->entry_point);
    log::info("  ELF header at: %x", info->elf_header_addr);
    log::info("  Program headers at: %x (count: %d)", info->program_headers_addr, info->program_header_count);
    log::info("  Section headers at: %x (count: %d)", info->section_headers_addr, info->section_header_count);
    log::info("  String table at: %x (size: %x)", info->string_table_addr, info->string_table_size);

    log::info("  Sections:");
    for (const auto& section : info->sections) {
        log::info("    %s: vaddr=%x, paddr=%x, size=%x, type=%x", section.name, section.vaddr, section.paddr, section.size, section.type);
    }
    debug_registry_lock.unlock();
}

void unregister_process(uint64_t pid) {
    debug_registry_lock.lock();
    // Find and remove the process from the debug registry
    for (auto it = debug_registry.begin(); it != debug_registry.end(); ++it) {
        if (it->pid == pid) {
            // The vectors (sections, symbols) will be automatically cleaned up
            // when the ProcessDebugInfo is erased from the vector
            debug_registry.erase(it);
            debug_registry_lock.unlock();
            return;
        }
    }
    debug_registry_lock.unlock();
}

}  // namespace ker::loader::debug
