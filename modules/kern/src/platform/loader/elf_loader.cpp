#include "elf_loader.hpp"

#include <extern/elf-em.h>
#include <extern/elf.h>

// #define ELF_DEBUG  // Enable ELF loading debug output

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <vector>

#include "abi/callnums/vmem.h"
#include "debug_info.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/ktime/ktime.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/page_alloc.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/phys.hpp"
#include "platform/mm/user_layout.hpp"
#include "platform/mm/virt.hpp"
#include "platform/perf/perf_events.hpp"
#include "util/hcf.hpp"
// TLS relocation support
namespace {

// Define relocation types that we need to handle
constexpr uint64_t R_X86_64_NONE = 0;
constexpr uint64_t R_X86_64_64 = 1;
constexpr uint64_t R_X86_64_PC32 = 2;
constexpr uint64_t R_X86_64_GOT32 = 3;
constexpr uint64_t R_X86_64_PLT32 = 4;
constexpr uint64_t R_X86_64_COPY = 5;
constexpr uint64_t R_X86_64_GLOB_DAT = 6;
constexpr uint64_t R_X86_64_JUMP_SLOT = 7;
constexpr uint64_t R_X86_64_RELATIVE = 8;
constexpr uint64_t R_X86_64_GOTPCREL = 9;
constexpr uint64_t R_X86_64_32 = 10;
constexpr uint64_t R_X86_64_32S = 11;
constexpr uint64_t R_X86_64_16 = 12;
constexpr uint64_t R_X86_64_PC16 = 13;
constexpr uint64_t R_X86_64_8 = 14;
constexpr uint64_t R_X86_64_PC8 = 15;
constexpr uint64_t R_X86_64_DTPMOD64 = 16;
constexpr uint64_t R_X86_64_DTPOFF64 = 17;
constexpr uint64_t R_X86_64_TPOFF64 = 18;
constexpr uint64_t R_X86_64_TLSGD = 19;
constexpr uint64_t R_X86_64_TLSLD = 20;
}  // namespace

#ifndef SHF_TLS
constexpr uint64_t SHF_TLS = 0x400;
#endif

namespace ker::loader::elf {
namespace {
using log = mod::dbg::logger<"elf">;

auto header_is_valid(const Elf64_Ehdr& ehdr) -> bool {
    return ehdr.e_ident[EI_CLASS] == ELFCLASS64                                                      // 64-bit
           && ehdr.e_ident[EI_DATA] == ELFDATA2LSB                                                   // little-endian
           && ehdr.e_ident[EI_VERSION] == EV_CURRENT                                                 // current ELF version
           && (ehdr.e_ident[EI_OSABI] == ELFOSABI_NONE || ehdr.e_ident[EI_OSABI] == ELFOSABI_LINUX)  // System V or GNU/Linux
           && (ehdr.e_type == ET_EXEC || ehdr.e_type == ET_DYN)                                      // Executable or PIE
           && ehdr.e_machine == EM_X86_64                                                            // x86-64
           && ehdr.e_version == EV_CURRENT                                                           // current ELF version
           && ehdr.e_ehsize == sizeof(Elf64_Ehdr)                                                    // native header size
           && ehdr.e_ident[EI_MAG0] == ELFMAG0                                                       // Magic 0
           && ehdr.e_ident[EI_MAG1] == ELFMAG1                                                       // Magic 1
           && ehdr.e_ident[EI_MAG2] == ELFMAG2                                                       // Magic 2
           && ehdr.e_ident[EI_MAG3] == ELFMAG3;                                                      // Magic 3
}

template <typename T>
auto ptr_from_addr(uint64_t addr) -> T* {
    return reinterpret_cast<T*>(addr);
}

template <typename T>
auto ptr_at(uint8_t* base, uint64_t offset) -> T* {
    return reinterpret_cast<T*>(base + offset);
}

template <typename T>
auto ptr_at(const uint8_t* base, uint64_t offset) -> const T* {
    return reinterpret_cast<const T*>(base + offset);
}

auto section_header_at(const ElfFile& elf, size_t index) -> const Elf64_Shdr* { return &elf.se_head[index]; }

auto program_header_at(const ElfFile& elf, size_t index) -> const Elf64_Phdr* { return &elf.pg_head[index]; }

auto range_fits(uint64_t offset, uint64_t size, uint64_t limit) -> bool { return offset <= limit && size <= limit - offset; }

auto table_fits(uint64_t offset, uint64_t count, uint64_t entry_size, uint64_t limit) -> bool {
    if (count == 0) {
        return offset <= limit;
    }
    return entry_size != 0 && count <= std::numeric_limits<uint64_t>::max() / entry_size && range_fits(offset, count * entry_size, limit);
}

auto checked_add(uint64_t lhs, uint64_t rhs, uint64_t& result) -> bool {
    if (lhs > std::numeric_limits<uint64_t>::max() - rhs) {
        return false;
    }
    result = lhs + rhs;
    return true;
}

auto checked_page_align_up(uint64_t value, uint64_t& result) -> bool {
    constexpr uint64_t PAGE_MASK = mod::mm::virt::PAGE_SIZE - 1;
    if (value > std::numeric_limits<uint64_t>::max() - PAGE_MASK) {
        return false;
    }
    result = (value + PAGE_MASK) & ~PAGE_MASK;
    return true;
}

auto is_power_of_two(uint64_t value) -> bool { return value != 0 && (value & (value - 1)) == 0; }

auto load_page_range(const Elf64_Phdr& ph, uint64_t& start, uint64_t& end) -> bool {
    if (ph.p_type != PT_LOAD || ph.p_memsz == 0) {
        return false;
    }

    uint64_t byte_end = 0;
    if (!checked_add(ph.p_vaddr, ph.p_memsz, byte_end) || !checked_page_align_up(byte_end, end)) {
        return false;
    }
    start = page_align_down(ph.p_vaddr);
    return start < end;
}

auto supported_program_header_type(uint32_t type) -> bool {
    switch (type) {
        case PT_NULL:
        case PT_LOAD:
        case PT_DYNAMIC:
        case PT_INTERP:
        case PT_NOTE:
        case PT_PHDR:
        case PT_TLS:
        case PT_GNU_EH_FRAME:
        case PT_GNU_STACK:
        case PT_GNU_RELRO:
        case PT_GNU_PROPERTY:
        case PT_GNU_SFRAME:
            return true;
        default:
            return false;
    }
}

auto range_is_covered_by_load_segments(const ElfFile& elf, uint64_t start, uint64_t size, bool require_non_executable) -> bool {
    uint64_t end = 0;
    if (size == 0) {
        return true;
    }
    if (!checked_add(start, size, end)) {
        return false;
    }

    uint64_t cursor = start;
    while (cursor < end) {
        uint64_t covered_until = cursor;
        for (Elf64_Half i = 0; i < elf.elf_head.e_phnum; ++i) {
            auto const* ph = program_header_at(elf, i);
            if (ph->p_type != PT_LOAD || ph->p_memsz == 0 || cursor < ph->p_vaddr) {
                continue;
            }
            uint64_t segment_end = 0;
            if (!checked_add(ph->p_vaddr, ph->p_memsz, segment_end) || cursor >= segment_end ||
                (require_non_executable && (ph->p_flags & PF_X) != 0U)) {
                continue;
            }
            covered_until = std::max(segment_end, covered_until);
        }
        if (covered_until == cursor) {
            return false;
        }
        cursor = covered_until < end ? covered_until : end;
    }
    return true;
}

auto entry_is_executable(const ElfFile& elf) -> bool {
    for (Elf64_Half i = 0; i < elf.elf_head.e_phnum; ++i) {
        auto const* ph = program_header_at(elf, i);
        if (ph->p_type != PT_LOAD || ph->p_memsz == 0 || (ph->p_flags & PF_X) == 0U) {
            continue;
        }
        uint64_t end = 0;
        if (checked_add(ph->p_vaddr, ph->p_memsz, end) && elf.elf_head.e_entry >= ph->p_vaddr && elf.elf_head.e_entry < end) {
            return true;
        }
    }
    return false;
}

auto load_segments_are_unambiguous(const ElfFile& elf) -> bool {
    for (Elf64_Half i = 0; i < elf.elf_head.e_phnum; ++i) {
        auto const* lhs = program_header_at(elf, i);
        uint64_t lhs_start = 0;
        uint64_t lhs_end = 0;
        if (!load_page_range(*lhs, lhs_start, lhs_end)) {
            continue;
        }
        for (auto j = static_cast<Elf64_Half>(i + 1); j < elf.elf_head.e_phnum; ++j) {
            auto const* rhs = program_header_at(elf, j);
            uint64_t rhs_start = 0;
            uint64_t rhs_end = 0;
            if (!load_page_range(*rhs, rhs_start, rhs_end) || lhs_start >= rhs_end || rhs_start >= lhs_end) {
                continue;
            }

            uint64_t lhs_byte_end = 0;
            uint64_t rhs_byte_end = 0;
            if (!checked_add(lhs->p_vaddr, lhs->p_memsz, lhs_byte_end) || !checked_add(rhs->p_vaddr, rhs->p_memsz, rhs_byte_end)) {
                return false;
            }
            if (lhs->p_vaddr < rhs_byte_end && rhs->p_vaddr < lhs_byte_end) {
                return false;
            }

            // A page has one final PTE. Refuse images whose overlapping
            // segments demand different permissions instead of silently
            // weakening either segment's contract.
            constexpr uint32_t PERMISSION_MASK = PF_R | PF_W | PF_X;
            if ((lhs->p_flags & PERMISSION_MASK) != (rhs->p_flags & PERMISSION_MASK)) {
                return false;
            }
        }
    }
    return true;
}

auto section_payload_is_valid(const ElfFile& elf, const Elf64_Shdr& section) -> bool {
    if (section.sh_addralign > 1 && !is_power_of_two(section.sh_addralign)) {
        return false;
    }
    if (section.sh_type == SHT_NULL || section.sh_type == SHT_NOBITS || section.sh_size == 0) {
        return true;
    }
    return range_fits(section.sh_offset, section.sh_size, elf.logical_size);
}

auto metadata_range_must_be_load_covered(uint32_t type) -> bool {
    switch (type) {
        case PT_DYNAMIC:
        case PT_INTERP:
        case PT_NOTE:
        case PT_PHDR:
        case PT_GNU_EH_FRAME:
        case PT_GNU_RELRO:
        case PT_GNU_PROPERTY:
        case PT_GNU_SFRAME:
            return true;
        default:
            return false;
    }
}

auto file_image_is_covered_by_load(const ElfFile& elf, uint64_t virtual_start, uint64_t file_start, uint64_t size) -> bool {
    if (size == 0) {
        return true;
    }
    uint64_t virtual_end = 0;
    uint64_t file_end = 0;
    if (!checked_add(virtual_start, size, virtual_end) || !checked_add(file_start, size, file_end)) {
        return false;
    }
    for (Elf64_Half i = 0; i < elf.elf_head.e_phnum; ++i) {
        auto const* load = program_header_at(elf, i);
        if (load->p_type != PT_LOAD || load->p_filesz == 0 || virtual_start < load->p_vaddr || file_start < load->p_offset) {
            continue;
        }
        uint64_t load_virtual_end = 0;
        uint64_t load_file_end = 0;
        if (!checked_add(load->p_vaddr, load->p_filesz, load_virtual_end) || !checked_add(load->p_offset, load->p_filesz, load_file_end)) {
            continue;
        }
        if (virtual_end <= load_virtual_end && file_end <= load_file_end && virtual_start - load->p_vaddr == file_start - load->p_offset) {
            return true;
        }
    }
    return false;
}

auto validated_tls_info(const ElfFile& elf, TlsModule& tls_info) -> bool {
    tls_info = {};
    unsigned tls_count = 0;
    for (Elf64_Half i = 0; i < elf.elf_head.e_phnum; ++i) {
        auto const* ph = program_header_at(elf, i);
        if (ph->p_type != PT_TLS) {
            continue;
        }
        ++tls_count;
        if (tls_count != 1 || ph->p_filesz > ph->p_memsz || ph->p_memsz > MAX_INITIAL_TLS_SIZE ||
            (ph->p_align > 1 &&
             (!is_power_of_two(ph->p_align) || (ph->p_offset & (ph->p_align - 1)) != (ph->p_vaddr & (ph->p_align - 1))))) {
            return false;
        }
        tls_info.tls_base = ph->p_vaddr;
        tls_info.tls_size = ph->p_memsz;
        tls_info.tcb_offset = ph->p_memsz;
    }
    return true;
}

auto read_source(const ElfFile& elf, uint64_t offset, void* destination, size_t size) -> bool {
    if (size == 0) {
        return true;
    }
    if (destination == nullptr || !range_fits(offset, size, elf.logical_size)) {
        return false;
    }
    if (elf.read_at != nullptr) {
        return elf.read_at(elf.read_context, offset, destination, size);
    }
    if (elf.base == nullptr) {
        return false;
    }
    std::memcpy(destination, elf.base + offset, size);
    return true;
}

auto section_name_at(const ElfFile& elf, uint32_t offset) -> const char* {
    if (elf.section_names == nullptr || offset >= elf.section_names_size) {
        return "";
    }
    const char* name = elf.section_names + offset;
    auto const REMAINING = static_cast<size_t>(elf.section_names_size - offset);
    return std::memchr(name, '\0', REMAINING) != nullptr ? name : "";
}

auto hhdm_to_phys(uint64_t hhdm_addr) -> uint64_t { return reinterpret_cast<uint64_t>(mod::mm::addr::get_phys_pointer(hhdm_addr)); }

auto perf_clamp_u16(uint64_t value) -> uint16_t { return value > UINT16_MAX ? UINT16_MAX : static_cast<uint16_t>(value); }

auto perf_clamp_u32(uint64_t value) -> uint32_t { return value > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(value); }

auto perf_elapsed_since(uint64_t started_us) -> uint32_t {
    uint64_t const NOW_US = mod::time::get_us();
    return perf_clamp_u32(NOW_US >= started_us ? NOW_US - started_us : 0);
}

auto pack_loader_page_counts(uint64_t allocated_pages, uint64_t already_mapped_pages) -> int32_t {
    return static_cast<int32_t>((static_cast<uint32_t>(perf_clamp_u16(allocated_pages)) << 16U) |
                                static_cast<uint32_t>(perf_clamp_u16(already_mapped_pages)));
}

auto loader_pt_load_op(mod::mm::user_layout::ImageRole image_role) -> mod::perf::WkiPerfLocalLoaderOp {
    return image_role == mod::mm::user_layout::ImageRole::MAIN ? mod::perf::WkiPerfLocalLoaderOp::PT_LOAD_MAIN
                                                               : mod::perf::WkiPerfLocalLoaderOp::PT_LOAD_INTERP;
}

auto loader_final_perms_op(mod::mm::user_layout::ImageRole image_role) -> mod::perf::WkiPerfLocalLoaderOp {
    return image_role == mod::mm::user_layout::ImageRole::MAIN ? mod::perf::WkiPerfLocalLoaderOp::FINAL_PERMS_MAIN
                                                               : mod::perf::WkiPerfLocalLoaderOp::FINAL_PERMS_INTERP;
}

auto prot_for_program_header(const Elf64_Phdr* program_header) -> uint64_t {
    uint64_t prot = ker::abi::vmem::PROT_READ;
    if ((program_header->p_flags & PF_W) != 0U) {
        prot |= ker::abi::vmem::PROT_WRITE;
    }
    if ((program_header->p_flags & PF_X) != 0U) {
        prot |= ker::abi::vmem::PROT_EXEC;
    }
    return prot;
}

auto append_lazy_load_range(ElfLazyLoadRangeVec* ranges, const ElfLazyLoadRange& range) -> bool {
    if (ranges == nullptr || range.size == 0) {
        return false;
    }

    if (!ranges->empty()) {
        auto& last = ranges->at(ranges->size() - 1);
        if (last.vaddr + last.size == range.vaddr && last.file_offset + last.size == range.file_offset && last.prot == range.prot &&
            last.flags == range.flags) {
            last.size += range.size;
            return true;
        }
    }

    return ranges->push_back(range);
}

auto lazy_load_page_range(const Elf64_Phdr* program_header, uint64_t page_no, uint64_t base_offset, ElfLazyLoadRange& out) -> bool {
    if (program_header == nullptr || program_header->p_type != PT_LOAD || (program_header->p_flags & PF_W) != 0U ||
        program_header->p_filesz == 0 || program_header->p_memsz < program_header->p_filesz) {
        return false;
    }
    if (page_no == 0) {
        return false;
    }

    uint64_t const SEG_START_VA = program_header->p_vaddr + base_offset;
    uint64_t const FIRST_PAGE_OFFSET = SEG_START_VA & (mod::mm::virt::PAGE_SIZE - 1);
    uint64_t const ALIGNED_START_VA = SEG_START_VA & ~(mod::mm::virt::PAGE_SIZE - 1);
    uint64_t const PAGE_VA = ALIGNED_START_VA + (page_no * mod::mm::virt::PAGE_SIZE);
    uint64_t const DST_IN_PAGE = (page_no == 0) ? FIRST_PAGE_OFFSET : 0;
    uint64_t const ROOM_IN_PAGE = mod::mm::virt::PAGE_SIZE - DST_IN_PAGE;

    uint64_t bytes_before_this_page = 0;
    if (page_no != 0) {
        bytes_before_this_page = (mod::mm::virt::PAGE_SIZE - FIRST_PAGE_OFFSET) + ((page_no - 1) * mod::mm::virt::PAGE_SIZE);
    }
    if (bytes_before_this_page >= program_header->p_filesz) {
        return false;
    }

    uint64_t const REMAINING_IN_FILE = program_header->p_filesz - bytes_before_this_page;
    uint64_t const COPY_SIZE = REMAINING_IN_FILE < ROOM_IN_PAGE ? REMAINING_IN_FILE : ROOM_IN_PAGE;
    if (DST_IN_PAGE != 0 || COPY_SIZE != mod::mm::virt::PAGE_SIZE) {
        return false;
    }
    if (program_header->p_offset > UINT64_MAX - bytes_before_this_page) {
        return false;
    }

    out = {.vaddr = PAGE_VA,
           .size = mod::mm::virt::PAGE_SIZE,
           .prot = prot_for_program_header(program_header),
           .flags = ker::abi::vmem::MAP_PRIVATE,
           .file_offset = program_header->p_offset + bytes_before_this_page};
    return true;
}

auto pt_load_page_overlaps_other_segment(const ElfFile& elf, const Elf64_Phdr* owner, uint64_t page_vaddr) -> bool {
    uint64_t const PAGE_END = page_vaddr + mod::mm::virt::PAGE_SIZE;
    for (Elf64_Half i = 0; i < elf.elf_head.e_phnum; ++i) {
        auto const* ph = program_header_at(elf, i);
        if (ph == owner || ph->p_type != PT_LOAD || ph->p_memsz == 0) {
            continue;
        }
        uint64_t const START = (ph->p_vaddr + elf.load_base) & ~(mod::mm::virt::PAGE_SIZE - 1);
        uint64_t const END = (ph->p_vaddr + elf.load_base + ph->p_memsz + mod::mm::virt::PAGE_SIZE - 1) & ~(mod::mm::virt::PAGE_SIZE - 1);
        if (page_vaddr < END && PAGE_END > START) {
            return true;
        }
    }
    return false;
}

void record_loader_event(uint64_t pid, mod::perf::WkiPerfLocalLoaderOp op, uint64_t pages, uint16_t detail, int32_t status,
                         uint32_t latency_us, uint64_t callsite, uint64_t bytes) {
    if (!mod::perf::is_wki_recording_enabled()) {
        return;
    }

    mod::perf::record_wki_event(static_cast<uint32_t>(mod::cpu::current_cpu()), pid, mod::perf::WkiPerfScope::LOCAL_LOADER,
                                static_cast<uint8_t>(op), mod::perf::WkiPerfPhase::END, perf_clamp_u16(pages), detail,
                                mod::perf::next_wki_trace_correlation(), status, latency_us, callsite);
    mod::perf::record_wki_summary(mod::perf::WkiPerfScope::LOCAL_LOADER, static_cast<uint8_t>(op), 0, 0, status, latency_us, true, 0,
                                  bytes);
}

auto parse_elf(const uint8_t* base, uint64_t size, ElfFile& elf) -> bool {
    elf = {};
    elf.base = base;
    elf.logical_size = size;
    elf.source_pointers_stable = true;

    // Do not form a typed pointer, much less dereference an offset from the
    // header, until the containing byte range has been proved present.
    if (base == nullptr || size < sizeof(Elf64_Ehdr)) {
        return false;
    }
    std::memcpy(&elf.elf_head, base, sizeof(elf.elf_head));
    if (!header_is_valid(elf.elf_head) || elf.elf_head.e_phnum == 0 || elf.elf_head.e_phentsize != sizeof(Elf64_Phdr) ||
        !table_fits(elf.elf_head.e_phoff, elf.elf_head.e_phnum, elf.elf_head.e_phentsize, size) ||
        (reinterpret_cast<uintptr_t>(base + elf.elf_head.e_phoff) % alignof(Elf64_Phdr)) != 0U) {
        return false;
    }
    elf.pg_head = ptr_at<Elf64_Phdr>(base, elf.elf_head.e_phoff);

    if (elf.elf_head.e_shnum != 0) {
        if (elf.elf_head.e_shentsize != sizeof(Elf64_Shdr) ||
            !table_fits(elf.elf_head.e_shoff, elf.elf_head.e_shnum, elf.elf_head.e_shentsize, size) ||
            (reinterpret_cast<uintptr_t>(base + elf.elf_head.e_shoff) % alignof(Elf64_Shdr)) != 0U ||
            (elf.elf_head.e_shstrndx != SHN_UNDEF && elf.elf_head.e_shstrndx >= elf.elf_head.e_shnum)) {
            return false;
        }
        elf.se_head = ptr_at<Elf64_Shdr>(base, elf.elf_head.e_shoff);
        if (elf.elf_head.e_shstrndx != SHN_UNDEF) {
            elf.sct_head_str_tab = section_header_at(elf, elf.elf_head.e_shstrndx);
            if (!range_fits(elf.sct_head_str_tab->sh_offset, elf.sct_head_str_tab->sh_size, size)) {
                return false;
            }
            if (elf.sct_head_str_tab->sh_size != 0) {
                elf.section_names = reinterpret_cast<const char*>(base + elf.sct_head_str_tab->sh_offset);
                elf.section_names_size = elf.sct_head_str_tab->sh_size;
            }
        }
    } else if (elf.elf_head.e_shoff != 0 || elf.elf_head.e_shstrndx != SHN_UNDEF) {
        return false;
    }

    // load_elf_impl selects a collision-free runtime bias after it has
    // validated the complete PT_LOAD span.
    elf.load_base = 0;
    return true;
}

auto view_elf(const ElfFileView& view) -> ElfFile {
    ElfFile elf{};
    elf.elf_head = view.elf_header;
    elf.pg_head = view.program_headers;
    elf.se_head = view.section_headers;
    bool const SECTION_TABLE_IS_BOUNDED =
        view.section_headers != nullptr && (reinterpret_cast<uintptr_t>(view.section_headers) % alignof(Elf64_Shdr)) == 0U &&
        view.elf_header.e_shnum != 0 && view.elf_header.e_shentsize == sizeof(Elf64_Shdr) && view.elf_header.e_shstrndx != SHN_UNDEF &&
        view.elf_header.e_shstrndx < view.elf_header.e_shnum &&
        table_fits(view.elf_header.e_shoff, view.elf_header.e_shnum, view.elf_header.e_shentsize, view.logical_size);
    elf.sct_head_str_tab = SECTION_TABLE_IS_BOUNDED ? &view.section_headers[view.elf_header.e_shstrndx] : nullptr;
    elf.base = view.contiguous_base;
    elf.section_names = view.section_names;
    elf.section_names_size = view.section_names_size;
    elf.read_at = view.read_at;
    elf.read_context = view.read_context;
    elf.logical_size = view.logical_size;
    elf.load_base = 0;
    return elf;
}

auto source_is_valid(const ElfFile& elf) -> bool {
    if (!header_is_valid(elf.elf_head) || elf.logical_size < sizeof(Elf64_Ehdr) || (elf.read_at == nullptr && elf.base == nullptr) ||
        elf.elf_head.e_phnum == 0) {
        return false;
    }
    if (elf.elf_head.e_phnum != 0 &&
        (elf.pg_head == nullptr || (reinterpret_cast<uintptr_t>(elf.pg_head) % alignof(Elf64_Phdr)) != 0U ||
         elf.elf_head.e_phentsize != sizeof(Elf64_Phdr) ||
         !table_fits(elf.elf_head.e_phoff, elf.elf_head.e_phnum, elf.elf_head.e_phentsize, elf.logical_size))) {
        return false;
    }
    if (elf.elf_head.e_shnum != 0) {
        if (elf.se_head == nullptr || (reinterpret_cast<uintptr_t>(elf.se_head) % alignof(Elf64_Shdr)) != 0U ||
            elf.elf_head.e_shentsize != sizeof(Elf64_Shdr) ||
            !table_fits(elf.elf_head.e_shoff, elf.elf_head.e_shnum, elf.elf_head.e_shentsize, elf.logical_size) ||
            (elf.elf_head.e_shstrndx != SHN_UNDEF && elf.elf_head.e_shstrndx >= elf.elf_head.e_shnum)) {
            return false;
        }
        if (elf.elf_head.e_shstrndx != SHN_UNDEF) {
            auto const* shstr = section_header_at(elf, elf.elf_head.e_shstrndx);
            if (shstr->sh_type != SHT_STRTAB || (shstr->sh_size != 0 && elf.section_names == nullptr) ||
                elf.section_names_size != shstr->sh_size || !range_fits(shstr->sh_offset, shstr->sh_size, elf.logical_size)) {
                return false;
            }
        } else if (elf.section_names != nullptr || elf.section_names_size != 0) {
            return false;
        }
        for (Elf64_Half i = 0; i < elf.elf_head.e_shnum; ++i) {
            auto const* section = section_header_at(elf, i);
            if (!section_payload_is_valid(elf, *section)) {
                return false;
            }
            if ((elf.section_names_size == 0 && section->sh_name != 0) ||
                (elf.section_names_size != 0 && (section->sh_name >= elf.section_names_size ||
                                                 std::memchr(elf.section_names + section->sh_name, '\0',
                                                             static_cast<size_t>(elf.section_names_size - section->sh_name)) == nullptr))) {
                return false;
            }
        }
    } else if (elf.elf_head.e_shoff != 0 || elf.elf_head.e_shstrndx != SHN_UNDEF || elf.se_head != nullptr ||
               elf.section_names != nullptr || elf.section_names_size != 0) {
        return false;
    }
    bool found_load = false;
    unsigned interp_count = 0;
    for (Elf64_Half i = 0; i < elf.elf_head.e_phnum; ++i) {
        auto const* ph = program_header_at(elf, i);
        if (!supported_program_header_type(ph->p_type) || (ph->p_flags & ~(PF_R | PF_W | PF_X)) != 0U ||
            (ph->p_filesz != 0 && !range_fits(ph->p_offset, ph->p_filesz, elf.logical_size))) {
            return false;
        }

        if (ph->p_type == PT_LOAD) {
            found_load = found_load || ph->p_memsz != 0;
            uint64_t segment_end = 0;
            uint64_t aligned_end = 0;
            if (ph->p_filesz > ph->p_memsz || (ph->p_flags & PF_R) == 0U || (ph->p_flags & (PF_W | PF_X)) == (PF_W | PF_X) ||
                (ph->p_offset & (mod::mm::virt::PAGE_SIZE - 1)) != (ph->p_vaddr & (mod::mm::virt::PAGE_SIZE - 1)) ||
                (ph->p_align > 1 &&
                 (!is_power_of_two(ph->p_align) || (ph->p_offset & (ph->p_align - 1)) != (ph->p_vaddr & (ph->p_align - 1)))) ||
                (elf.elf_head.e_type == ET_DYN && ph->p_align > mod::mm::user_layout::IMAGE_ALIGNMENT) ||
                !checked_add(ph->p_vaddr, ph->p_memsz, segment_end) || !checked_page_align_up(segment_end, aligned_end)) {
                return false;
            }
        } else if ((ph->p_type == PT_TLS || ph->p_type == PT_NOTE) && ph->p_filesz > ph->p_memsz) {
            return false;
        }

        if (ph->p_type == PT_GNU_STACK && (ph->p_flags & PF_X) != 0U) {
            return false;
        }
        if (ph->p_type == PT_INTERP) {
            ++interp_count;
            if (interp_count != 1 || ph->p_filesz < 2 || ph->p_filesz >= ElfLoadResult::INTERP_PATH_MAX) {
                return false;
            }
            std::array<char, ElfLoadResult::INTERP_PATH_MAX> path{};
            if (!read_source(elf, ph->p_offset, path.data(), static_cast<size_t>(ph->p_filesz)) ||
                path.at(static_cast<size_t>(ph->p_filesz - 1)) != '\0' ||
                std::memchr(path.data(), '\0', static_cast<size_t>(ph->p_filesz - 1)) != nullptr) {
                return false;
            }
        }
        if ((ph->p_type == PT_GNU_RELRO || ph->p_type == PT_GNU_EH_FRAME || ph->p_type == PT_TLS || ph->p_type == PT_NOTE) &&
            ph->p_memsz != 0) {
            uint64_t end = 0;
            if (!checked_add(ph->p_vaddr, ph->p_memsz, end)) {
                return false;
            }
        }
    }

    TlsModule tls_info{};
    if (!found_load || !entry_is_executable(elf) || !load_segments_are_unambiguous(elf) || !validated_tls_info(elf, tls_info)) {
        return false;
    }

    for (Elf64_Half i = 0; i < elf.elf_head.e_phnum; ++i) {
        auto const* ph = program_header_at(elf, i);
        if (metadata_range_must_be_load_covered(ph->p_type) && ph->p_memsz != 0 &&
            !range_is_covered_by_load_segments(elf, ph->p_vaddr, ph->p_memsz, true)) {
            return false;
        }
        // A TLS block may end in the alignment tail after a PT_LOAD's byte
        // extent (the normal shape of ld.so). Only initialized TLS bytes must
        // have an exact file/virtual correspondence in a readable load.
        if (ph->p_type == PT_TLS && !file_image_is_covered_by_load(elf, ph->p_vaddr, ph->p_offset, ph->p_filesz)) {
            return false;
        }
    }
    return true;
}

constexpr uint32_t SHT_RELR_VALUE = 19;

auto section_is_relr(const ElfFile& elf, const Elf64_Shdr& section) -> bool {
    if (section.sh_type == SHT_RELR_VALUE) {
        return true;
    }
    const char* const NAME = section_name_at(elf, section.sh_name);
    return std::strcmp(NAME, ".relr") == 0 || std::strcmp(NAME, ".relr.dyn") == 0;
}

auto symbol_table_is_valid(const ElfFile& elf, const Elf64_Shdr& section) -> bool {
    if ((section.sh_type != SHT_SYMTAB && section.sh_type != SHT_DYNSYM) || section.sh_entsize != sizeof(Elf64_Sym) ||
        (section.sh_size % sizeof(Elf64_Sym)) != 0U || section.sh_link >= elf.elf_head.e_shnum || elf.base == nullptr ||
        (reinterpret_cast<uintptr_t>(elf.base + section.sh_offset) % alignof(Elf64_Sym)) != 0U) {
        return false;
    }
    auto const* strings = section_header_at(elf, section.sh_link);
    return strings->sh_type == SHT_STRTAB && section_payload_is_valid(elf, *strings);
}

auto relocation_write_width(uint32_t type, uint64_t& width) -> bool {
    switch (type) {
        case R_X86_64_NONE:
            width = 0;
            return true;
        case R_X86_64_PC32:
        case R_X86_64_PLT32:
            width = sizeof(uint32_t);
            return true;
        case R_X86_64_64:
        case R_X86_64_GLOB_DAT:
        case R_X86_64_JUMP_SLOT:
        case R_X86_64_RELATIVE:
        case R_X86_64_TPOFF64:
            width = sizeof(uint64_t);
            return true;
        default:
            return false;
    }
}

auto relocation_target_is_writable_load(const ElfFile& elf, uint64_t target, uint64_t width) -> bool {
    if (width == 0) {
        return true;
    }
    uint64_t target_end = 0;
    if (!checked_add(target, width, target_end)) {
        return false;
    }
    for (Elf64_Half i = 0; i < elf.elf_head.e_phnum; ++i) {
        auto const* ph = program_header_at(elf, i);
        uint64_t segment_end = 0;
        if (ph->p_type == PT_LOAD && (ph->p_flags & PF_W) != 0U && checked_add(ph->p_vaddr, ph->p_memsz, segment_end) &&
            target >= ph->p_vaddr && target_end <= segment_end) {
            return true;
        }
    }
    return false;
}

auto relocation_runtime_address(const ElfFile& elf, uint64_t target, uint64_t& runtime_address) -> bool {
    return checked_add(elf.load_base, target, runtime_address);
}

auto read_mapped_value(ker::mod::mm::virt::PageTable* pagemap, uint64_t address, uint64_t width, uint64_t& value) -> bool {
    value = 0;
    for (uint64_t i = 0; i < width; ++i) {
        uint64_t const PADDR = mod::mm::virt::translate(pagemap, address + i);
        if (PADDR == mod::mm::virt::PADDR_INVALID) {
            return false;
        }
        auto const* byte = reinterpret_cast<const uint8_t*>(mod::mm::addr::get_virt_pointer(PADDR));
        value |= static_cast<uint64_t>(*byte) << (i * 8U);
    }
    return true;
}

auto write_mapped_value(ker::mod::mm::virt::PageTable* pagemap, uint64_t address, uint64_t width, uint64_t value) -> bool {
    for (uint64_t i = 0; i < width; ++i) {
        uint64_t const PADDR = mod::mm::virt::translate(pagemap, address + i);
        if (PADDR == mod::mm::virt::PADDR_INVALID) {
            return false;
        }
        auto* byte = reinterpret_cast<uint8_t*>(mod::mm::addr::get_virt_pointer(PADDR));
        *byte = static_cast<uint8_t>(value >> (i * 8U));
    }
    return true;
}

auto resolve_symbol(const ElfFile& elf, const Elf64_Shdr& relocation_section, uint32_t symbol_index, uint64_t& value) -> bool {
    value = 0;
    if (symbol_index == 0) {
        return true;
    }
    if (relocation_section.sh_link >= elf.elf_head.e_shnum) {
        return false;
    }
    auto const* symbol_section = section_header_at(elf, relocation_section.sh_link);
    if (!symbol_table_is_valid(elf, *symbol_section)) {
        return false;
    }
    uint64_t const SYMBOL_COUNT = symbol_section->sh_size / sizeof(Elf64_Sym);
    if (symbol_index >= SYMBOL_COUNT) {
        return false;
    }
    auto const* symbols = reinterpret_cast<const Elf64_Sym*>(elf.base + symbol_section->sh_offset);
    auto const& symbol = symbols[symbol_index];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    auto const* strings = section_header_at(elf, symbol_section->sh_link);
    if (symbol.st_name >= strings->sh_size || std::memchr(elf.base + strings->sh_offset + symbol.st_name, '\0',
                                                          static_cast<size_t>(strings->sh_size - symbol.st_name)) == nullptr) {
        return false;
    }
    if (symbol.st_shndx == SHN_UNDEF) {
        return ELF64_ST_BIND(symbol.st_info) == STB_WEAK;
    }
    value = symbol.st_value;
    if (symbol.st_shndx == SHN_ABS) {
        return true;
    }
    if (symbol.st_shndx >= elf.elf_head.e_shnum) {
        return false;
    }
    auto const* defining_section = section_header_at(elf, symbol.st_shndx);
    return (defining_section->sh_flags & SHF_TLS) != 0U || checked_add(value, elf.load_base, value);
}

auto relocation_entry_is_valid(const ElfFile& elf, const Elf64_Shdr& section, uint64_t offset, uint64_t info) -> bool {
    uint32_t const TYPE = ELF64_R_TYPE(info);
    uint32_t const SYMBOL_INDEX = ELF64_R_SYM(info);
    uint64_t width = 0;
    if (!relocation_write_width(TYPE, width) || !relocation_target_is_writable_load(elf, offset, width)) {
        return false;
    }
    if (TYPE == R_X86_64_RELATIVE && SYMBOL_INDEX != 0) {
        return false;
    }
    uint64_t runtime_address = 0;
    uint64_t symbol = 0;
    return (width == 0 || relocation_runtime_address(elf, offset, runtime_address)) && resolve_symbol(elf, section, SYMBOL_INDEX, symbol);
}

template <typename Callback>
auto for_each_relr_target(const ElfFile& elf, const Elf64_Shdr& section, Callback&& callback) -> bool {
    auto const* entries = reinterpret_cast<const uint64_t*>(elf.base + section.sh_offset);
    uint64_t const COUNT = section.sh_size / sizeof(uint64_t);
    uint64_t next = 0;
    bool have_next = false;
    for (uint64_t i = 0; i < COUNT; ++i) {
        uint64_t const ENTRY = entries[i];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        if ((ENTRY & 1U) == 0U) {
            if (!callback(ENTRY) || !checked_add(ENTRY, sizeof(uint64_t), next)) {
                return false;
            }
            have_next = true;
            continue;
        }
        if (!have_next) {
            return false;
        }
        for (unsigned bit = 1; bit < 64; ++bit) {
            if ((ENTRY & (1ULL << bit)) == 0U) {
                continue;
            }
            uint64_t target = 0;
            if (!checked_add(next, static_cast<uint64_t>(bit - 1) * sizeof(uint64_t), target) || !callback(target)) {
                return false;
            }
        }
        if (!checked_add(next, 63 * sizeof(uint64_t), next)) {
            return false;
        }
    }
    return true;
}

auto static_relocations_are_valid(const ElfFile& elf) -> bool {
    if (elf.base == nullptr) {
        return false;
    }
    for (Elf64_Half i = 0; i < elf.elf_head.e_shnum; ++i) {
        auto const* section = section_header_at(elf, i);
        if (section_is_relr(elf, *section)) {
            if (section->sh_entsize != sizeof(uint64_t) || (section->sh_size % sizeof(uint64_t)) != 0U ||
                (reinterpret_cast<uintptr_t>(elf.base + section->sh_offset) % alignof(uint64_t)) != 0U ||
                !for_each_relr_target(elf, *section, [&](uint64_t target) {
                    uint64_t runtime_address = 0;
                    return relocation_target_is_writable_load(elf, target, sizeof(uint64_t)) &&
                           relocation_runtime_address(elf, target, runtime_address);
                })) {
                return false;
            }
            continue;
        }
        if (section->sh_type != SHT_REL && section->sh_type != SHT_RELA) {
            continue;
        }
        uint64_t const ENTRY_SIZE = section->sh_type == SHT_REL ? sizeof(Elf64_Rel) : sizeof(Elf64_Rela);
        if (section->sh_entsize != ENTRY_SIZE || (section->sh_size % ENTRY_SIZE) != 0U || section->sh_link >= elf.elf_head.e_shnum ||
            (reinterpret_cast<uintptr_t>(elf.base + section->sh_offset) %
             (section->sh_type == SHT_REL ? alignof(Elf64_Rel) : alignof(Elf64_Rela))) != 0U ||
            !symbol_table_is_valid(elf, *section_header_at(elf, section->sh_link))) {
            return false;
        }
        uint64_t const COUNT = section->sh_size / ENTRY_SIZE;
        if (section->sh_type == SHT_REL) {
            auto const* entries = reinterpret_cast<const Elf64_Rel*>(elf.base + section->sh_offset);
            for (uint64_t j = 0; j < COUNT; ++j) {
                auto const& entry = entries[j];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                if (!relocation_entry_is_valid(elf, *section, entry.r_offset, entry.r_info)) {
                    return false;
                }
            }
        } else {
            auto const* entries = reinterpret_cast<const Elf64_Rela*>(elf.base + section->sh_offset);
            for (uint64_t j = 0; j < COUNT; ++j) {
                auto const& entry = entries[j];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                if (!relocation_entry_is_valid(elf, *section, entry.r_offset, entry.r_info)) {
                    return false;
                }
            }
        }
    }
    return true;
}

auto checked_add_signed(uint64_t base, int64_t addend, uint64_t& result) -> bool {
    if (addend >= 0) {
        return checked_add(base, static_cast<uint64_t>(addend), result);
    }
    uint64_t const MAGNITUDE = static_cast<uint64_t>(-(addend + 1)) + 1;
    if (base < MAGNITUDE) {
        return false;
    }
    result = base - MAGNITUDE;
    return true;
}

auto apply_static_relocation(const ElfFile& elf, ker::mod::mm::virt::PageTable* pagemap, const Elf64_Shdr& section, uint64_t offset,
                             uint64_t info, int64_t explicit_addend, bool has_explicit_addend) -> bool {
    uint32_t const TYPE = ELF64_R_TYPE(info);
    uint32_t const SYMBOL_INDEX = ELF64_R_SYM(info);
    uint64_t width = 0;
    if (!relocation_write_width(TYPE, width)) {
        return false;
    }
    if (width == 0) {
        return true;
    }
    uint64_t place = 0;
    if (!relocation_runtime_address(elf, offset, place)) {
        return false;
    }
    int64_t addend = explicit_addend;
    if (!has_explicit_addend) {
        uint64_t implicit = 0;
        if (!read_mapped_value(pagemap, place, width, implicit)) {
            return false;
        }
        addend = width == sizeof(uint32_t) ? static_cast<int32_t>(implicit) : static_cast<int64_t>(implicit);
    }
    uint64_t symbol = 0;
    if (!resolve_symbol(elf, section, SYMBOL_INDEX, symbol)) {
        return false;
    }

    uint64_t value = 0;
    switch (TYPE) {
        case R_X86_64_RELATIVE:
            if (!checked_add_signed(elf.load_base, addend, value)) {
                return false;
            }
            break;
        case R_X86_64_64:
        case R_X86_64_GLOB_DAT:
        case R_X86_64_JUMP_SLOT:
        case R_X86_64_TPOFF64:
            if (!checked_add_signed(symbol, addend, value)) {
                return false;
            }
            break;
        case R_X86_64_PC32:
        case R_X86_64_PLT32: {
            __int128 const RESULT = static_cast<__int128>(symbol) + static_cast<__int128>(addend) - static_cast<__int128>(place);
            if (RESULT < std::numeric_limits<int32_t>::min() || RESULT > std::numeric_limits<int32_t>::max()) {
                return false;
            }
            value = static_cast<uint32_t>(static_cast<int32_t>(RESULT));
            break;
        }
        default:
            return false;
    }
    return write_mapped_value(pagemap, place, width, value);
}

auto process_relocations(const ElfFile& elf, ker::mod::mm::virt::PageTable* pagemap) -> bool {
    for (Elf64_Half i = 0; i < elf.elf_head.e_shnum; ++i) {
        auto const* section = section_header_at(elf, i);
        if (section_is_relr(elf, *section)) {
            if (!for_each_relr_target(elf, *section, [&](uint64_t target) {
                    uint64_t place = 0;
                    uint64_t value = 0;
                    return relocation_runtime_address(elf, target, place) && read_mapped_value(pagemap, place, sizeof(uint64_t), value) &&
                           checked_add(value, elf.load_base, value) && write_mapped_value(pagemap, place, sizeof(uint64_t), value);
                })) {
                return false;
            }
            continue;
        }
        if (section->sh_type == SHT_REL) {
            auto const* entries = reinterpret_cast<const Elf64_Rel*>(elf.base + section->sh_offset);
            uint64_t const COUNT = section->sh_size / sizeof(Elf64_Rel);
            for (uint64_t j = 0; j < COUNT; ++j) {
                auto const& entry = entries[j];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                if (!apply_static_relocation(elf, pagemap, *section, entry.r_offset, entry.r_info, 0, false)) {
                    return false;
                }
            }
        } else if (section->sh_type == SHT_RELA) {
            auto const* entries = reinterpret_cast<const Elf64_Rela*>(elf.base + section->sh_offset);
            uint64_t const COUNT = section->sh_size / sizeof(Elf64_Rela);
            for (uint64_t j = 0; j < COUNT; ++j) {
                auto const& entry = entries[j];  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                if (!apply_static_relocation(elf, pagemap, *section, entry.r_offset, entry.r_info, entry.r_addend, true)) {
                    return false;
                }
            }
        }
    }
    return true;
}

void process_read_only_segment(const Elf64_Phdr* segment, ker::mod::mm::virt::PageTable* pagemap, uint64_t pid) {
    (void)pid;
    // PT_GNU_RELRO describes a region that should become read-only AFTER relocations.
    // These pages are already mapped by PT_LOAD segments. Do NOT remap using p_offset
    // (file offset) as a physical address; that corrupts the mapping (incl. GOT/PLT).
    // Instead, if desired, tighten permissions of already-mapped pages.

    // Defer making RELRO read-only until AFTER all relocations complete.
    // No action here during initial load to avoid write-protection issues with CR0.WP set.
    (void)segment;
    (void)pagemap;
}

void register_eh_frame(void* base, uint64_t size) {
    (void)base;
    (void)size;
    // TODO: Register the .eh_frame section for exception handling
}

void process_eh_frame_segment(const Elf64_Phdr* segment, uint64_t load_base, uint64_t pid) {
    (void)pid;
    // PT_LOAD owns both the mapping and its final permissions. The validation
    // pass already proved this metadata range is covered by a non-executable
    // load segment; PT_GNU_EH_FRAME is informational here.
    uint64_t const VADDR = segment->p_vaddr + load_base;
    register_eh_frame(ptr_from_addr<void>(VADDR), segment->p_memsz);
}

auto page_flags_for_program_header(const Elf64_Phdr* program_header) -> uint64_t {
    uint64_t flags =
        ((program_header->p_flags & PF_W) != 0U) ? mod::mm::paging::page_types::USER : mod::mm::paging::page_types::USER_READONLY;
    if ((program_header->p_flags & PF_X) == 0U) {
        flags |= mod::mm::paging::PAGE_NX;
    }
    return flags;
}

struct LoadSegmentPageStats {
    bool mapped{};
    bool allocated{};
    bool read_succeeded{};
    uint64_t bytes_copied{};
};

auto load_segment(const ElfFile& elf, ker::mod::mm::virt::PageTable* pagemap, const Elf64_Phdr* program_header, uint64_t page_no,
                  uint64_t base_offset, ker::mod::mm::virt::PageMapBatch* map_batch = nullptr) -> LoadSegmentPageStats {
    // Compute aligned virtual address for this page and in-page offset of the segment start
    const uint64_t SEG_START_VA = program_header->p_vaddr + base_offset;
    const uint64_t FIRST_PAGE_OFFSET = SEG_START_VA & (mod::mm::virt::PAGE_SIZE - 1);
    const uint64_t ALIGNED_START_VA = SEG_START_VA & ~(mod::mm::virt::PAGE_SIZE - 1);
    const uint64_t PAGE_VA = ALIGNED_START_VA + (page_no * mod::mm::virt::PAGE_SIZE);

    // Additional validation for PIE executables
    if (base_offset != 0 && PAGE_VA < mod::mm::virt::PAGE_SIZE) {
        log::error("PIE program trying to map too low address 0x%lx", PAGE_VA);
        return {};
    }

    // Map the page at a page-aligned virtual address
    bool const ALREADY_MAPPED = mod::mm::virt::is_page_mapped(pagemap, PAGE_VA);
    // HHDM-mapped pointer to the backing physical page so we can write contents
    uint64_t page_hhdm_ptr = 0;
    if (ALREADY_MAPPED) {
        mod::mm::virt::unify_page_flags(pagemap, PAGE_VA, mod::mm::paging::page_types::USER);
        uint64_t const PADDR = mod::mm::virt::translate(pagemap, PAGE_VA);
        if (PADDR == ker::mod::mm::virt::PADDR_INVALID) {
            mod::dbg::log("elf_loader: translate failed for already-mapped pageVA 0x%lx", PAGE_VA);
            hcf();
        }
        page_hhdm_ptr = reinterpret_cast<uint64_t>(mod::mm::addr::get_virt_pointer(PADDR));
    } else {
        // Allocate a new physical page; allocator returns an HHDM pointer to the page memory
        auto const NEW_PAGE_HHDM_PTR = reinterpret_cast<uint64_t>(mod::mm::phys::page_alloc_may_fail(
            mod::mm::PhysicalPageOwner::USER_EXECUTABLE_MAPPING, mod::mm::paging::PAGE_SIZE, "elf_segment"));
        if (NEW_PAGE_HHDM_PTR == 0) {
            return {};
        }
        page_hhdm_ptr = NEW_PAGE_HHDM_PTR;
        // Map using the physical address corresponding to that HHDM pointer
        auto const PAGE_FLAGS = page_flags_for_program_header(program_header);
        if (map_batch != nullptr) {
            mod::mm::virt::map_page_batched(map_batch, PAGE_VA, static_cast<mod::mm::addr::paddr_t>(hhdm_to_phys(NEW_PAGE_HHDM_PTR)),
                                            PAGE_FLAGS);
        } else {
            mod::mm::virt::map_page(pagemap, PAGE_VA, static_cast<mod::mm::addr::paddr_t>(hhdm_to_phys(NEW_PAGE_HHDM_PTR)), PAGE_FLAGS);
        }
        // Zero freshly mapped page to handle bss/holes
        std::memset(ptr_from_addr<void>(page_hhdm_ptr), 0, mod::mm::virt::PAGE_SIZE);
    }

    LoadSegmentPageStats stats{.mapped = true, .allocated = !ALREADY_MAPPED, .read_succeeded = true, .bytes_copied = 0};

    // Determine source file offset and destination in-page offset for this page
    const uint64_t DST_IN_PAGE = (page_no == 0) ? FIRST_PAGE_OFFSET : 0;
    const uint64_t ROOM_IN_PAGE = mod::mm::virt::PAGE_SIZE - DST_IN_PAGE;

    // How many bytes have already been copied before this page?
    uint64_t bytes_before_this_page = 0;
    if (page_no == 0) {
        bytes_before_this_page = 0;
    } else {
        // First (partial) page accounts for (PAGE_SIZE - firstPageOffset), then full pages
        bytes_before_this_page = (mod::mm::virt::PAGE_SIZE - FIRST_PAGE_OFFSET) + ((page_no - 1) * mod::mm::virt::PAGE_SIZE);
    }

    // If we've consumed the entire file content of the segment, nothing to copy for this page
    if (bytes_before_this_page >= program_header->p_filesz) {
        return stats;
    }

    // Compute how much to copy from this page
    uint64_t const REMAINING_IN_FILE = program_header->p_filesz - bytes_before_this_page;
    uint64_t const COPY_SIZE = REMAINING_IN_FILE < ROOM_IN_PAGE ? REMAINING_IN_FILE : ROOM_IN_PAGE;

    // Copy from ELF file to destination page at the correct in-page offset
    const uint64_t SRC_OFFSET = program_header->p_offset + bytes_before_this_page;
    if (!read_source(elf, SRC_OFFSET, ptr_from_addr<void>(page_hhdm_ptr + DST_IN_PAGE), COPY_SIZE)) {
        stats.read_succeeded = false;
        return stats;
    }
    stats.bytes_copied = COPY_SIZE;
    return stats;
}

constexpr uint64_t HEADER_COPY_VADDR = 0x1000;
constexpr uint64_t DEBUG_SECTION_HEADERS_VADDR = 0x700000000000ULL;
constexpr uint64_t DEBUG_SECTION_HEADERS_MAX_SIZE = 0x200000;
constexpr uint64_t DEBUG_STRING_TABLE_VADDR = 0x700000201000ULL;
constexpr uint64_t DEBUG_STRING_TABLE_MAX_SIZE = 0x400000;

struct DebugMetadataPlan {
    bool process_sections{};
    bool install{};
    uint64_t section_headers_size{};
    uint64_t section_headers_pages{};
    uint64_t string_table_size{};
    uint64_t string_table_pages{};
};

auto make_debug_metadata_plan(const ElfFile& elf, bool install_primary_image_metadata, DebugMetadataPlan& plan) -> bool {
    plan = {};
    if (elf.elf_head.e_shnum == 0 || elf.se_head == nullptr || elf.sct_head_str_tab == nullptr || elf.section_names == nullptr) {
        return true;
    }
    if (elf.elf_head.e_shnum > UINT64_MAX / elf.elf_head.e_shentsize) {
        return false;
    }
    uint64_t const SECTION_SIZE = static_cast<uint64_t>(elf.elf_head.e_shnum) * elf.elf_head.e_shentsize;
    uint64_t section_aligned = 0;
    uint64_t string_aligned = 0;
    if (!checked_page_align_up(SECTION_SIZE, section_aligned) || !checked_page_align_up(elf.section_names_size, string_aligned)) {
        return false;
    }
    if (SECTION_SIZE > DEBUG_SECTION_HEADERS_MAX_SIZE || elf.section_names_size > DEBUG_STRING_TABLE_MAX_SIZE) {
        // Debug metadata is optional. Bound its user mapping cost without
        // rejecting an otherwise valid executable.
        log::warn("Skipping excessive ELF debug metadata (sections=%lu strings=%lu)", SECTION_SIZE, elf.section_names_size);
        return true;
    }
    plan.process_sections = true;
    plan.install = install_primary_image_metadata;
    plan.section_headers_size = SECTION_SIZE;
    plan.section_headers_pages = section_aligned / mod::mm::virt::PAGE_SIZE;
    plan.string_table_size = elf.section_names_size;
    plan.string_table_pages = string_aligned / mod::mm::virt::PAGE_SIZE;
    return true;
}

auto load_section_headers(const ElfFile& elf, ker::mod::mm::virt::PageTable* pagemap, const uint64_t& pid,
                          const DebugMetadataPlan& metadata_plan) -> bool {
    (void)pid;
    if (elf.elf_head.e_shnum == 0 || elf.se_head == nullptr || elf.sct_head_str_tab == nullptr || elf.section_names == nullptr) {
        return true;
    }
    if (!metadata_plan.process_sections) {
        return true;
    }
    const char* section_names = elf.section_names;

    // The PID-scoped registry belongs to the primary image. The interpreter is
    // loaded into the same pagemap and may append section rows below, but must
    // not remap these fixed virtual addresses or replace the primary image's
    // header pointers.
    if (metadata_plan.install) {
        for (uint64_t i = 0; i < metadata_plan.section_headers_pages; i++) {
            auto* const PAGE = mod::mm::phys::page_alloc_may_fail(mod::mm::PhysicalPageOwner::USER_EXECUTABLE_MAPPING,
                                                                  mod::mm::paging::PAGE_SIZE, "elf_debug_sections");
            if (PAGE == nullptr) {
                return false;
            }
            auto const PAGE_HHDM = reinterpret_cast<uint64_t>(PAGE);
            std::memset(PAGE, 0, mod::mm::virt::PAGE_SIZE);
            mod::mm::virt::map_page(pagemap, DEBUG_SECTION_HEADERS_VADDR + (i * mod::mm::virt::PAGE_SIZE), hhdm_to_phys(PAGE_HHDM),
                                    mod::mm::paging::page_types::USER_READONLY | mod::mm::paging::PAGE_NX);
            uint64_t const SOURCE_OFFSET = i * mod::mm::virt::PAGE_SIZE;
            uint64_t const BYTES_LEFT = metadata_plan.section_headers_size - SOURCE_OFFSET;
            auto const COPY_SIZE = static_cast<size_t>(BYTES_LEFT < mod::mm::virt::PAGE_SIZE ? BYTES_LEFT : mod::mm::virt::PAGE_SIZE);
            std::memcpy(PAGE, reinterpret_cast<const uint8_t*>(elf.se_head) + SOURCE_OFFSET, COPY_SIZE);
        }
        debug::set_section_headers(pid, ptr_from_addr<Elf64_Shdr>(DEBUG_SECTION_HEADERS_VADDR), DEBUG_SECTION_HEADERS_VADDR,
                                   elf.elf_head.e_shnum);

        for (uint64_t i = 0; i < metadata_plan.string_table_pages; i++) {
            auto* const PAGE = mod::mm::phys::page_alloc_may_fail(mod::mm::PhysicalPageOwner::USER_EXECUTABLE_MAPPING,
                                                                  mod::mm::paging::PAGE_SIZE, "elf_debug_strings");
            if (PAGE == nullptr) {
                return false;
            }
            auto const PAGE_HHDM = reinterpret_cast<uint64_t>(PAGE);
            std::memset(PAGE, 0, mod::mm::virt::PAGE_SIZE);
            mod::mm::virt::map_page(pagemap, DEBUG_STRING_TABLE_VADDR + (i * mod::mm::virt::PAGE_SIZE), hhdm_to_phys(PAGE_HHDM),
                                    mod::mm::paging::page_types::USER_READONLY | mod::mm::paging::PAGE_NX);
            uint64_t const SOURCE_OFFSET = i * mod::mm::virt::PAGE_SIZE;
            uint64_t const BYTES_LEFT = metadata_plan.string_table_size - SOURCE_OFFSET;
            auto const COPY_SIZE = static_cast<size_t>(BYTES_LEFT < mod::mm::virt::PAGE_SIZE ? BYTES_LEFT : mod::mm::virt::PAGE_SIZE);
            std::memcpy(PAGE, section_names + SOURCE_OFFSET, COPY_SIZE);
        }
        debug::set_string_table(pid, ptr_from_addr<const char>(DEBUG_STRING_TABLE_VADDR), DEBUG_STRING_TABLE_VADDR,
                                metadata_plan.string_table_size);
    }

    for (size_t section_index = 0; section_index < elf.elf_head.e_shnum; section_index++) {
        auto const* section_header = section_header_at(elf, section_index);
        const char* section_name = section_name_at(elf, section_header->sh_name);
#ifdef ELF_DEBUG
        mod::dbg::log("Section name: %s", sectionName);
#endif
        (void)section_name;

        // Register sections for debugging without re-mapping any PT_LOAD-backed content
        if (elf.source_pointers_stable && section_header->sh_type == SHT_PROGBITS && section_header->sh_size > 0) {
            // A source-controlled section address is diagnostic metadata, not
            // mapping authority. Record it as resident only when PT_LOAD
            // actually covers the complete byte range.
            if (section_header->sh_addr != 0 &&
                range_is_covered_by_load_segments(elf, section_header->sh_addr, section_header->sh_size, false)) {
                uint64_t const SECTION_VADDR = section_header->sh_addr + elf.load_base;
                uint64_t const FIRST_PADDR = mod::mm::virt::translate(pagemap, SECTION_VADDR);
                debug::add_debug_section(pid, section_name, SECTION_VADDR, FIRST_PADDR, section_header->sh_size, section_header->sh_offset,
                                         section_header->sh_type);
#ifdef ELF_DEBUG
                mod::dbg::log("Added debug section: %s, vaddr=%x, paddr=%x, size=%x", sectionName, sectionVaddr, firstPaddr,
                              sectionHeader->sh_size);
#endif
            } else if (std::strncmp(section_name, ".debug_", 7) == 0) {
                debug::add_debug_section(pid, section_name, 0, 0, section_header->sh_size, section_header->sh_offset,
                                         section_header->sh_type);
#ifdef ELF_DEBUG
                mod::dbg::log("Recorded debug section metadata only: %s (size=%x, fileOff=%x)", sectionName, sectionHeader->sh_size,
                              sectionHeader->sh_offset);
#endif
            }
        }
        // Additionally, record GOT sections in debug info registry for diagnostics (no remapping or copying here)
        if (elf.source_pointers_stable &&
            ((std::strncmp(section_name, ".got", 4) == 0) || (std::strncmp(section_name, ".got.plt", 8) == 0)) &&
            section_header->sh_addr != 0 && section_header->sh_size > 0 &&
            range_is_covered_by_load_segments(elf, section_header->sh_addr, section_header->sh_size, false)) {
            uint64_t const SECTION_VADDR = section_header->sh_addr + elf.load_base;
            uint64_t const FIRST_PADDR = mod::mm::virt::translate(pagemap, SECTION_VADDR);
            debug::add_debug_section(pid, section_name, SECTION_VADDR, FIRST_PADDR, section_header->sh_size, section_header->sh_offset,
                                     section_header->sh_type);
#ifdef ELF_DEBUG
            mod::dbg::log("Recorded GOT-like section %s at vaddr: %x, paddr: %x, size: %x", sectionName, sectionVaddr, firstPaddr,
                          sectionHeader->sh_size);
#endif
        }
    }
    return true;
}

struct LoadImageExtent {
    uint64_t min_vaddr{};
    uint64_t max_vaddr{};
};

auto load_image_extent(const ElfFile& elf, LoadImageExtent& extent) -> bool {
    extent.min_vaddr = std::numeric_limits<uint64_t>::max();
    extent.max_vaddr = 0;
    for (Elf64_Half i = 0; i < elf.elf_head.e_phnum; ++i) {
        auto const* ph = program_header_at(elf, i);
        uint64_t start = 0;
        uint64_t end = 0;
        if (!load_page_range(*ph, start, end)) {
            continue;
        }
        extent.min_vaddr = std::min(start, extent.min_vaddr);
        extent.max_vaddr = std::max(end, extent.max_vaddr);
    }
    return extent.min_vaddr != std::numeric_limits<uint64_t>::max() && extent.min_vaddr < extent.max_vaddr;
}

auto ranges_overlap(uint64_t lhs_start, uint64_t lhs_size, uint64_t rhs_start, uint64_t rhs_size) -> bool {
    uint64_t lhs_end = 0;
    uint64_t rhs_end = 0;
    return lhs_size != 0 && rhs_size != 0 && checked_add(lhs_start, lhs_size, lhs_end) && checked_add(rhs_start, rhs_size, rhs_end) &&
           lhs_start < rhs_end && rhs_start < lhs_end;
}

auto load_segments_overlap_runtime_range(const ElfFile& elf, uint64_t range_start, uint64_t range_size) -> bool {
    for (Elf64_Half i = 0; i < elf.elf_head.e_phnum; ++i) {
        auto const* ph = program_header_at(elf, i);
        uint64_t start = 0;
        uint64_t end = 0;
        if (!load_page_range(*ph, start, end) || !checked_add(start, elf.load_base, start) || !checked_add(end, elf.load_base, end)) {
            continue;
        }
        if (ranges_overlap(start, end - start, range_start, range_size)) {
            return true;
        }
    }
    return false;
}

auto loader_owned_ranges_are_available(const ElfFile& elf, ker::mod::mm::virt::PageTable* pagemap, const DebugMetadataPlan& metadata_plan,
                                       bool is_primary_image) -> bool {
    if (!is_primary_image) {
        return true;
    }

    if (elf.elf_head.e_type == ET_EXEC) {
        uint64_t const HEADER_BYTES = sizeof(Elf64_Ehdr) + static_cast<uint64_t>(elf.elf_head.e_phnum) * sizeof(Elf64_Phdr);
        uint64_t header_size = 0;
        if (!checked_page_align_up(HEADER_BYTES, header_size) || load_segments_overlap_runtime_range(elf, HEADER_COPY_VADDR, header_size) ||
            !mod::mm::user_layout::range_is_free(pagemap, HEADER_COPY_VADDR, header_size) ||
            load_segments_overlap_runtime_range(elf, DEBUG_SECTION_HEADERS_VADDR, DEBUG_SECTION_HEADERS_MAX_SIZE) ||
            load_segments_overlap_runtime_range(elf, DEBUG_STRING_TABLE_VADDR, DEBUG_STRING_TABLE_MAX_SIZE)) {
            return false;
        }
    }

    if (!metadata_plan.install) {
        return true;
    }
    uint64_t const SECTION_BYTES = metadata_plan.section_headers_pages * mod::mm::virt::PAGE_SIZE;
    uint64_t const STRING_BYTES = metadata_plan.string_table_pages * mod::mm::virt::PAGE_SIZE;
    return (SECTION_BYTES == 0 || mod::mm::user_layout::range_is_free(pagemap, DEBUG_SECTION_HEADERS_VADDR, SECTION_BYTES)) &&
           (STRING_BYTES == 0 || mod::mm::user_layout::range_is_free(pagemap, DEBUG_STRING_TABLE_VADDR, STRING_BYTES));
}

auto runtime_image_extent(const ElfFile& elf, uint64_t& start, uint64_t& end) -> bool {
    LoadImageExtent extent{};
    return load_image_extent(elf, extent) && checked_add(elf.load_base, extent.min_vaddr, start) &&
           checked_add(elf.load_base, extent.max_vaddr, end);
}

auto applied_image_range_is_valid(const LoadImageExtent& extent, uint64_t load_base, ker::mod::mm::virt::PageTable* pagemap) -> bool {
    constexpr uint64_t USER_ADDRESS_LIMIT = 0x0000800000000000ULL;
    uint64_t start = 0;
    uint64_t end = 0;
    if ((load_base & (mod::mm::virt::PAGE_SIZE - 1)) != 0U || !checked_add(load_base, extent.min_vaddr, start) ||
        !checked_add(load_base, extent.max_vaddr, end) || start < mod::mm::virt::PAGE_SIZE || end > USER_ADDRESS_LIMIT || start >= end) {
        return false;
    }
    return mod::mm::user_layout::range_is_free(pagemap, start, end - start);
}

auto select_image_load_base(ElfFile& elf, ker::mod::mm::virt::PageTable* pagemap, const ElfLoadOptions& options) -> bool {
    LoadImageExtent extent{};
    if (!load_image_extent(elf, extent)) {
        return false;
    }

    if (elf.elf_head.e_type == ET_EXEC) {
        if (options.image_role != mod::mm::user_layout::ImageRole::MAIN || options.base_address != 0 ||
            !applied_image_range_is_valid(extent, 0, pagemap)) {
            return false;
        }
        elf.load_base = 0;
        return true;
    }

    uint64_t load_base = options.base_address;
    if (load_base == 0 &&
        !mod::mm::user_layout::choose_image_base(pagemap, options.image_role, extent.min_vaddr, extent.max_vaddr, load_base)) {
        return false;
    }
    if (!applied_image_range_is_valid(extent, load_base, pagemap)) {
        return false;
    }
    for (Elf64_Half i = 0; i < elf.elf_head.e_phnum; ++i) {
        auto const* ph = program_header_at(elf, i);
        if (ph->p_type == PT_LOAD && ph->p_align > 1 && (load_base & (ph->p_align - 1)) != 0U) {
            return false;
        }
    }
    elf.load_base = load_base;
    return true;
}

auto runtime_program_header_ranges_fit(const ElfFile& elf) -> bool {
    constexpr uint64_t USER_ADDRESS_LIMIT = 0x0000800000000000ULL;
    for (Elf64_Half i = 0; i < elf.elf_head.e_phnum; ++i) {
        auto const* ph = program_header_at(elf, i);
        if (ph->p_memsz == 0) {
            continue;
        }
        uint64_t start = 0;
        uint64_t end = 0;
        if (!checked_add(elf.load_base, ph->p_vaddr, start) || !checked_add(start, ph->p_memsz, end) || end > USER_ADDRESS_LIMIT) {
            return false;
        }
    }
    return true;
}

auto loaded_vaddr_for_file_range(const ElfFile& elf, uint64_t file_offset, uint64_t size, uint64_t& vaddr) -> bool {
    uint64_t file_end = 0;
    if (!checked_add(file_offset, size, file_end)) {
        return false;
    }
    for (Elf64_Half i = 0; i < elf.elf_head.e_phnum; ++i) {
        auto const* ph = program_header_at(elf, i);
        if (ph->p_type != PT_LOAD || file_offset < ph->p_offset) {
            continue;
        }
        uint64_t segment_file_end = 0;
        if (!checked_add(ph->p_offset, ph->p_filesz, segment_file_end) || file_end > segment_file_end) {
            continue;
        }
        uint64_t offset_in_segment = file_offset - ph->p_offset;
        uint64_t segment_vaddr = 0;
        return checked_add(ph->p_vaddr, offset_in_segment, segment_vaddr) && checked_add(elf.load_base, segment_vaddr, vaddr);
    }
    return false;
}

class DebugRegistrationRollback {
   public:
    DebugRegistrationRollback(uint64_t pid, bool armed) : process_id(pid), is_armed(armed) {}
    ~DebugRegistrationRollback() {
        if (is_armed) {
            debug::unregister_process(process_id);
        }
    }

    DebugRegistrationRollback(const DebugRegistrationRollback&) = delete;
    auto operator=(const DebugRegistrationRollback&) -> DebugRegistrationRollback& = delete;

    void release() { is_armed = false; }

   private:
    uint64_t process_id{};
    bool is_armed{};
};
}  // namespace

namespace {
auto load_elf_impl(ElfFile elf_file, ker::mod::mm::virt::PageTable* pagemap, uint64_t pid, const char* process_name,
                   const ElfLoadOptions& options) -> ElfLoadResult;
}

auto load_elf(const uint8_t* data, size_t size, ker::mod::mm::virt::PageTable* pagemap, uint64_t pid, const char* process_name,
              bool register_special_symbols, uint64_t base_address) -> ElfLoadResult {
    ElfLoadOptions const OPTIONS{
        .register_special_symbols = register_special_symbols,
        .base_address = base_address,
        .lazy_file_ranges = nullptr,
        .debug_registry_pid = 0,
        .image_role = base_address == 0 ? mod::mm::user_layout::ImageRole::MAIN : mod::mm::user_layout::ImageRole::INTERPRETER,
    };
    return load_elf(data, size, pagemap, pid, process_name, OPTIONS);
}

auto load_elf(const uint8_t* data, size_t size, ker::mod::mm::virt::PageTable* pagemap, uint64_t pid, const char* process_name,
              const ElfLoadOptions& options) -> ElfLoadResult {
    ElfFile elf{};
    if (!parse_elf(data, size, elf)) {
        mod::dbg::log("ERROR: loadElf called with invalid bounded ELF source (pid=%d, size=%zu)", pid, size);
        return {.entry_point = 0, .program_header_addr = 0, .elf_header_addr = 0};
    }
    return load_elf_impl(elf, pagemap, pid, process_name, options);
}

auto load_elf(const ElfFileView& elf, ker::mod::mm::virt::PageTable* pagemap, uint64_t pid, const char* process_name,
              bool register_special_symbols, uint64_t base_address) -> ElfLoadResult {
    ElfLoadOptions const OPTIONS{
        .register_special_symbols = register_special_symbols,
        .base_address = base_address,
        .lazy_file_ranges = nullptr,
        .debug_registry_pid = 0,
        .image_role = base_address == 0 ? mod::mm::user_layout::ImageRole::MAIN : mod::mm::user_layout::ImageRole::INTERPRETER,
    };
    return load_elf(elf, pagemap, pid, process_name, OPTIONS);
}

auto load_elf(const ElfFileView& elf, ker::mod::mm::virt::PageTable* pagemap, uint64_t pid, const char* process_name,
              const ElfLoadOptions& options) -> ElfLoadResult {
    return load_elf_impl(view_elf(elf), pagemap, pid, process_name, options);
}

namespace {
auto load_elf_impl(ElfFile elf_file, ker::mod::mm::virt::PageTable* pagemap, uint64_t pid, const char* process_name,
                   const ElfLoadOptions& options) -> ElfLoadResult {
    bool const REGISTER_SPECIAL_SYMBOLS = options.register_special_symbols;
    uint64_t const DEBUG_PID = options.debug_registry_pid != 0 ? options.debug_registry_pid : pid;
    bool const IS_PRIMARY_IMAGE = options.image_role == mod::mm::user_layout::ImageRole::MAIN;

    if (!header_is_valid(elf_file.elf_head)) {
        mod::dbg::log("ERROR: Invalid ELF header (pid=%d)", pid);
        mod::dbg::log("  ELF base: 0x%p", elf_file.base);
        mod::dbg::log("  e_ident: [0x%x 0x%x 0x%x 0x%x] (expected [0x%x 0x%x 0x%x 0x%x])", elf_file.elf_head.e_ident[EI_MAG0],
                      elf_file.elf_head.e_ident[EI_MAG1], elf_file.elf_head.e_ident[EI_MAG2], elf_file.elf_head.e_ident[EI_MAG3], ELFMAG0,
                      ELFMAG1, ELFMAG2, ELFMAG3);
        mod::dbg::log("  e_ident[EI_CLASS]: 0x%x (expected ELFCLASS64=0x%x)", elf_file.elf_head.e_ident[EI_CLASS], ELFCLASS64);
        mod::dbg::log("  e_ident[EI_OSABI]: 0x%x (expected ELFOSABI_NONE=0x%x or ELFOSABI_LINUX=0x%x)", elf_file.elf_head.e_ident[EI_OSABI],
                      ELFOSABI_NONE, ELFOSABI_LINUX);
        mod::dbg::log("  e_type: 0x%x (expected ET_EXEC=0x%x or ET_DYN=0x%x)", elf_file.elf_head.e_type, ET_EXEC, ET_DYN);
        mod::dbg::log("  e_phoff: 0x%x, e_shoff: 0x%x", elf_file.elf_head.e_phoff, elf_file.elf_head.e_shoff);
        return {.entry_point = 0, .program_header_addr = 0, .elf_header_addr = 0};
    }
    if (!source_is_valid(elf_file)) {
        mod::dbg::log("ERROR: Invalid or out-of-bounds ELF source metadata (pid=%d, logical_size=%lu)", pid, elf_file.logical_size);
        return {.entry_point = 0, .program_header_addr = 0, .elf_header_addr = 0};
    }
    if (!select_image_load_base(elf_file, pagemap, options) || !runtime_program_header_ranges_fit(elf_file)) {
        mod::dbg::log("ERROR: Unable to select a collision-free ELF load range (pid=%d)", pid);
        return {.entry_point = 0, .program_header_addr = 0, .elf_header_addr = 0};
    }

    bool has_dynamic_interp = false;
    for (Elf64_Half i = 0; i < elf_file.elf_head.e_phnum; i++) {
        if (program_header_at(elf_file, i)->p_type == PT_INTERP) {
            has_dynamic_interp = true;
            break;
        }
    }
    bool const DYNAMIC_LINKER_OWNS_RELOCATIONS = has_dynamic_interp || !IS_PRIMARY_IMAGE;
    if (!DYNAMIC_LINKER_OWNS_RELOCATIONS && elf_file.base == nullptr) {
        mod::dbg::log("ERROR: File-backed static ELF requires a contiguous base for kernel relocations (pid=%d)", pid);
        return {.entry_point = 0, .program_header_addr = 0, .elf_header_addr = 0};
    }
    if (!DYNAMIC_LINKER_OWNS_RELOCATIONS && !static_relocations_are_valid(elf_file)) {
        mod::dbg::log("ERROR: Invalid static relocation contract (pid=%d)", pid);
        return {.entry_point = 0, .program_header_addr = 0, .elf_header_addr = 0};
    }

    DebugMetadataPlan debug_metadata_plan{};
    if (!make_debug_metadata_plan(elf_file, IS_PRIMARY_IMAGE, debug_metadata_plan) ||
        !loader_owned_ranges_are_available(elf_file, pagemap, debug_metadata_plan, IS_PRIMARY_IMAGE)) {
        mod::dbg::log("ERROR: ELF collides with loader-owned metadata ranges (pid=%d)", pid);
        return {.entry_point = 0, .program_header_addr = 0, .elf_header_addr = 0};
    }

    uint64_t elf_header_vaddr = 0;
    uint64_t program_headers_vaddr = 0;
    if (IS_PRIMARY_IMAGE && elf_file.elf_head.e_type == ET_DYN) {
        uint64_t const PROGRAM_HEADERS_SIZE = static_cast<uint64_t>(elf_file.elf_head.e_phnum) * sizeof(Elf64_Phdr);
        if (!loaded_vaddr_for_file_range(elf_file, 0, sizeof(Elf64_Ehdr), elf_header_vaddr) ||
            !loaded_vaddr_for_file_range(elf_file, elf_file.elf_head.e_phoff, PROGRAM_HEADERS_SIZE, program_headers_vaddr)) {
            mod::dbg::log("ERROR: PIE headers are not contained in a PT_LOAD segment (pid=%d)", pid);
            return {.entry_point = 0, .program_header_addr = 0, .elf_header_addr = 0};
        }
    } else if (IS_PRIMARY_IMAGE) {
        uint64_t const TOTAL_HEADERS_SIZE = sizeof(Elf64_Ehdr) + (static_cast<uint64_t>(elf_file.elf_head.e_phnum) * sizeof(Elf64_Phdr));
        uint64_t const TOTAL_HEADERS_PAGES = page_align_up(TOTAL_HEADERS_SIZE) / mod::mm::virt::PAGE_SIZE;
        if (!mod::mm::user_layout::range_is_free(pagemap, HEADER_COPY_VADDR, TOTAL_HEADERS_PAGES * mod::mm::virt::PAGE_SIZE)) {
            mod::dbg::log("ERROR: Fixed ELF-header copy range collides with an existing mapping (pid=%d)", pid);
            return {.entry_point = 0, .program_header_addr = 0, .elf_header_addr = 0};
        }
    }

    // The main image owns the PID-scoped debug row. PT_INTERP loads pass
    // register_special_symbols=false and append any useful metadata to that
    // row instead of creating an unreachable duplicate with the same PID.
    if (REGISTER_SPECIAL_SYMBOLS) {
        debug::register_process(DEBUG_PID, process_name, elf_file.load_base, elf_file.elf_head.e_entry + elf_file.load_base);
    }
    DebugRegistrationRollback debug_registration_rollback(DEBUG_PID, REGISTER_SPECIAL_SYMBOLS);

    // Collect all program headers for the executable — ld.so needs the full set
    // (PT_LOAD, PT_DYNAMIC, PT_TLS, PT_PHDR, PT_GNU_RELRO, etc.)
    std::vector<Elf64_Phdr> filtered_headers;
    for (Elf64_Half i = 0; i < elf_file.elf_head.e_phnum; i++) {
        auto const* ph = program_header_at(elf_file, i);
        filtered_headers.push_back(*ph);
    }

    // Set up AT_PHDR for the main executable (baseAddress == 0).
    // For PIE (ET_DYN): PHDRs are already mapped by the first PT_LOAD segment at their
    // original file offsets (e_phoff). Copying them to a fixed address like 0x1000 would
    // conflict with PT_LOAD segments that cover that range, and the subsequent PT_LOAD
    // mapping would overwrite the copied headers. Just point AT_PHDR at the originals.
    // For non-PIE (ET_EXEC): PT_LOAD segments start at high addresses (e.g. 0x400000),
    // so we can safely copy headers to 0x1000 without conflicts.
    bool const ENABLE_LAZY_FILE_RANGES = options.lazy_file_ranges != nullptr && (has_dynamic_interp || !IS_PRIMARY_IMAGE);

    if (IS_PRIMARY_IMAGE && elf_file.elf_head.e_type == ET_DYN) {
        debug::set_program_headers(DEBUG_PID, ptr_from_addr<Elf64_Phdr>(program_headers_vaddr), program_headers_vaddr,
                                   elf_file.elf_head.e_phnum);
    } else if (IS_PRIMARY_IMAGE) {
        // Non-PIE (ET_EXEC): copy headers to 0x1000 (no PT_LOAD overlap at low addresses)
        constexpr uint64_t PROGRAM_HEADERS_OFFSET_IN_HEADER = sizeof(Elf64_Ehdr);
        elf_header_vaddr = HEADER_COPY_VADDR;
        program_headers_vaddr = HEADER_COPY_VADDR + PROGRAM_HEADERS_OFFSET_IN_HEADER;

        auto program_headers_size = static_cast<uint64_t>(filtered_headers.size() * elf_file.elf_head.e_phentsize);
        uint64_t const TOTAL_HEADERS_SIZE = sizeof(Elf64_Ehdr) + program_headers_size;
        uint64_t const TOTAL_HEADERS_PAGES = page_align_up(TOTAL_HEADERS_SIZE) / mod::mm::virt::PAGE_SIZE;

        // Allocate physical pages for both ELF and program headers
        std::vector<uint64_t> header_phys_addrs;
        for (uint64_t i = 0; i < TOTAL_HEADERS_PAGES; i++) {
            auto const PADDR = reinterpret_cast<uint64_t>(mod::mm::phys::page_alloc_may_fail(
                mod::mm::PhysicalPageOwner::USER_EXECUTABLE_MAPPING, mod::mm::paging::PAGE_SIZE, "elf_headers"));
            if (PADDR == 0) {
                mod::dbg::log("ERROR: Failed to allocate physical page for headers");
                return {.entry_point = 0, .program_header_addr = 0, .elf_header_addr = 0};
            }
            std::memset(ptr_from_addr<void>(PADDR), 0, mod::mm::virt::PAGE_SIZE);
            mod::mm::virt::map_page(pagemap, HEADER_COPY_VADDR + (i * mod::mm::virt::PAGE_SIZE), hhdm_to_phys(PADDR),
                                    mod::mm::paging::page_types::USER);
            header_phys_addrs.push_back(PADDR);
        }

        // Copy ELF header
        {
            auto* header_ptr = ptr_from_addr<Elf64_Ehdr>(header_phys_addrs.front());
            std::memcpy(header_ptr, &elf_file.elf_head, sizeof(Elf64_Ehdr));
            header_ptr->e_phoff = PROGRAM_HEADERS_OFFSET_IN_HEADER;
            header_ptr->e_phnum = static_cast<Elf64_Half>(filtered_headers.size());
        }

        // Copy program headers, fixing up PT_PHDR.p_vaddr to match placement
        {
            for (auto& filtered_header : filtered_headers) {
                if (filtered_header.p_type == PT_PHDR) {
                    filtered_header.p_vaddr = program_headers_vaddr;
                    filtered_header.p_paddr = program_headers_vaddr;
                }
            }

            uint64_t dest_offset = PROGRAM_HEADERS_OFFSET_IN_HEADER;
            for (auto& filtered_header : filtered_headers) {
                uint64_t const HEADER_SIZE = elf_file.elf_head.e_phentsize;
                for (uint64_t i = 0; i < HEADER_SIZE; i++) {
                    uint64_t const PAGE_IDX = dest_offset / mod::mm::virt::PAGE_SIZE;
                    uint64_t const OFFSET_IN_PAGE = dest_offset % mod::mm::virt::PAGE_SIZE;

                    if (PAGE_IDX >= header_phys_addrs.size()) {
                        mod::dbg::log("ERROR: Program header offset exceeds allocated pages");
                        return {.entry_point = 0, .program_header_addr = 0, .elf_header_addr = 0};
                    }

                    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access): PAGE_IDX is checked above.
                    auto* dest_ptr = ptr_from_addr<uint8_t>(header_phys_addrs[PAGE_IDX] + OFFSET_IN_PAGE);
                    uint8_t const* src_ptr = reinterpret_cast<uint8_t*>(&filtered_header) + i;
                    *dest_ptr = *src_ptr;
                    dest_offset++;
                }
            }
        }

        for (uint64_t i = 0; i < TOTAL_HEADERS_PAGES; ++i) {
            mod::mm::virt::unify_page_flags(pagemap, HEADER_COPY_VADDR + (i * mod::mm::virt::PAGE_SIZE),
                                            mod::mm::paging::page_types::USER_READONLY | mod::mm::paging::PAGE_NX);
        }

        debug::set_program_headers(DEBUG_PID, ptr_from_addr<Elf64_Phdr>(program_headers_vaddr), program_headers_vaddr,
                                   static_cast<uint16_t>(filtered_headers.size()));
    }
    for (Elf64_Half i = 0; i < elf_file.elf_head.e_phnum; i++) {
        auto const* current_header = program_header_at(elf_file, i);

        switch (current_header->p_type) {
            case PT_GNU_STACK:
                // GNU_STACK segment presence indicates whether stack is executable; record as a debug section
#ifdef ELF_DEBUG
                mod::dbg::log("Found PT_GNU_STACK at vaddr=0x%x, flags=0x%x", currentHeader->p_vaddr, currentHeader->p_flags);
#endif
                debug::add_debug_section(DEBUG_PID, "PT_GNU_STACK", current_header->p_vaddr + elf_file.load_base, current_header->p_offset,
                                         current_header->p_memsz, current_header->p_offset, current_header->p_type);
                break;
            case PT_TLS:
                // Found TLS segment - store its information
                elf_file.tls_info.tls_base = current_header->p_vaddr + elf_file.load_base;
                elf_file.tls_info.tls_size = current_header->p_memsz;
                elf_file.tls_info.tcb_offset = current_header->p_memsz;  // TCB goes after TLS data
#ifdef ELF_DEBUG
                mod::dbg::log("Found PT_TLS segment: vaddr=0x%x, filesz=0x%x, memsz=0x%x", currentHeader->p_vaddr, currentHeader->p_filesz,
                              currentHeader->p_memsz);
#endif
                break;

            case PT_LOAD:
                // Loadable segment
                {
#ifdef ELF_DEBUG
                    mod::dbg::log("Loading PT_LOAD segment: vaddr=0x%x, filesz=0x%x, memsz=0x%x, offset=0x%x", currentHeader->p_vaddr,
                                  currentHeader->p_filesz, currentHeader->p_memsz, currentHeader->p_offset);
#endif
                    // Calculate number of pages accounting for offset within first page
                    uint64_t const SEG_END = current_header->p_vaddr + current_header->p_memsz;
                    uint64_t const START_PAGE_ADDR = current_header->p_vaddr & ~(mod::mm::virt::PAGE_SIZE - 1);
                    uint64_t const END_PAGE_ADDR = (SEG_END + mod::mm::virt::PAGE_SIZE - 1) & ~(mod::mm::virt::PAGE_SIZE - 1);
                    size_t const NUM_PAGES = (END_PAGE_ADDR - START_PAGE_ADDR) / mod::mm::virt::PAGE_SIZE;
#ifdef ELF_DEBUG
                    mod::dbg::log("Calculated pages: start_page=0x%x, end_page=0x%x, num_pages=%zu", startPageAddr, endPageAddr, num_pages);
#endif
                    uint64_t const SEGMENT_STARTED_US = mod::time::get_us();
                    uint64_t allocated_pages = 0;
                    uint64_t already_mapped_pages = 0;
                    uint64_t bytes_copied = 0;
                    bool segment_read_succeeded = true;
                    mod::mm::virt::PageMapBatch map_batch{};
                    mod::mm::virt::init_page_map_batch(&map_batch, pagemap, page_flags_for_program_header(current_header));
                    for (uint64_t j = 0; j < NUM_PAGES; j++) {
                        if (ENABLE_LAZY_FILE_RANGES) {
                            ElfLazyLoadRange lazy_range{};
                            if (lazy_load_page_range(current_header, j, elf_file.load_base, lazy_range) &&
                                !pt_load_page_overlaps_other_segment(elf_file, current_header, lazy_range.vaddr) &&
                                !mod::mm::virt::is_page_mapped(pagemap, lazy_range.vaddr) &&
                                append_lazy_load_range(options.lazy_file_ranges, lazy_range)) {
                                continue;
                            }
                        }
                        LoadSegmentPageStats const PAGE_STATS =
                            load_segment(elf_file, pagemap, current_header, j, elf_file.load_base, &map_batch);
                        if (!PAGE_STATS.read_succeeded) {
                            segment_read_succeeded = false;
                            break;
                        }
                        if (!PAGE_STATS.mapped) {
                            continue;
                        }
                        if (PAGE_STATS.allocated) {
                            allocated_pages++;
                        } else {
                            already_mapped_pages++;
                        }
                        bytes_copied += PAGE_STATS.bytes_copied;
                    }
                    mod::mm::virt::flush_page_map_batch(&map_batch);
                    if (!segment_read_succeeded) {
                        mod::dbg::log("ERROR: Failed reading PT_LOAD bytes at file offset 0x%lx (pid=%d)", current_header->p_offset, pid);
                        return {.entry_point = 0, .program_header_addr = 0, .elf_header_addr = 0};
                    }
                    record_loader_event(pid, loader_pt_load_op(options.image_role), NUM_PAGES, static_cast<uint16_t>(i),
                                        pack_loader_page_counts(allocated_pages, already_mapped_pages),
                                        perf_elapsed_since(SEGMENT_STARTED_US), current_header->p_vaddr + elf_file.load_base, bytes_copied);
                }
                break;

            case PT_GNU_RELRO:
#ifdef ELF_DEBUG
                mod::dbg::log("Found PT_GNU_RELRO segment at vaddr=0x%x, size=0x%x", currentHeader->p_vaddr, currentHeader->p_memsz);
#endif
                // Read-only relocation segment
                process_read_only_segment(current_header, pagemap, DEBUG_PID);
                break;

            case PT_GNU_EH_FRAME:
#ifdef ELF_DEBUG
                mod::dbg::log("Found PT_GNU_EH_FRAME segment at vaddr=0x%x, size=0x%x", currentHeader->p_vaddr, currentHeader->p_memsz);
#endif
                // Exception handling frame segment
                process_eh_frame_segment(current_header, elf_file.load_base, DEBUG_PID);
                break;

            case PT_PHDR:
            case PT_INTERP:
            case PT_DYNAMIC:
            case PT_GNU_PROPERTY:
            case PT_GNU_SFRAME:
            case PT_NOTE:
                // Informational/user-loader segments: no kernel mapping work needed here.
#ifdef ELF_DEBUG
                if (current_header->p_type == PT_PHDR) {
                    mod::dbg::log("PT_PHDR segment skipped (informational only)");
                }
#endif
                break;

            case PT_NULL:
                // Null segment - skip
                break;

            default:
                mod::dbg::log("Unsupported ELF segment type %d", current_header->p_type);
                return {.entry_point = 0, .program_header_addr = 0, .elf_header_addr = 0};
        }
    }

    // Load section headers with debug info
    if (!load_section_headers(elf_file, pagemap, DEBUG_PID, debug_metadata_plan)) {
        mod::dbg::log("ERROR: Failed to install bounded ELF debug metadata (pid=%d)", pid);
        return {.entry_point = 0, .program_header_addr = 0, .elf_header_addr = 0};
    }

    // Only process relocations for statically-linked binaries.
    // Dynamically-linked ones (PT_INTERP present) have their relocations
    // handled by the dynamic linker (ld.so).
    // ld.so itself (loaded with non-zero loadBase) handles its own via relocateSelf().
    if (!DYNAMIC_LINKER_OWNS_RELOCATIONS) {
        if (!process_relocations(elf_file, pagemap)) {
            mod::dbg::log("ERROR: Static ELF relocation failed closed (pid=%d)", pid);
            return {.entry_point = 0, .program_header_addr = 0, .elf_header_addr = 0};
        }
    }

    // Apply final permissions to PT_LOAD segments based on p_flags (after relocations complete).
    // With NX available:
    //  - PF_W set   -> USER (read/write), NX if PF_X not set
    //  - PF_W clear -> USER_READONLY (read-only), NX if PF_X not set
    //
    // Pass 0: Apply writable segment permissions
    // Pass 1: Apply read-only segment permissions
    uint64_t const FINAL_PERMS_STARTED_US = mod::time::get_us();
    uint64_t final_perm_pages = 0;
    for (int pass = 0; pass < 2; pass++) {
        for (Elf64_Half i = 0; i < elf_file.elf_head.e_phnum; i++) {
            auto const* ph = program_header_at(elf_file, i);
            if (ph->p_type == PT_LOAD) {
                const bool WRITABLE = (ph->p_flags & PF_W) != 0;
                const bool EXECUTABLE = (ph->p_flags & PF_X) != 0;

                // Pass 0: only writable segments, Pass 1: only read-only segments
                if ((pass == 0 && !WRITABLE) || (pass == 1 && WRITABLE)) {
                    continue;
                }

                uint64_t base_flags = WRITABLE ? mod::mm::paging::page_types::USER : mod::mm::paging::page_types::USER_READONLY;
                if (!EXECUTABLE) {
                    base_flags |= mod::mm::paging::PAGE_NX;
                }

                uint64_t const START = (ph->p_vaddr + elf_file.load_base) & ~(mod::mm::virt::PAGE_SIZE - 1);
                uint64_t const END = page_align_up(ph->p_vaddr + elf_file.load_base + ph->p_memsz);

                for (uint64_t va = START; va < END; va += mod::mm::virt::PAGE_SIZE) {
                    if (!mod::mm::virt::is_page_mapped(pagemap, va)) {
                        continue;
                    }

#ifdef ELF_DEBUG_EXTRA
                    mod::dbg::log("Setting page 0x%x to flags=0x%x (%s %s)", va, baseFlags, writable ? "WRITE" : "READONLY",
                                  executable ? "EXEC" : "NOEXEC");
#endif
                    mod::mm::virt::unify_page_flags(pagemap, va, base_flags);
                    final_perm_pages++;
                }
#ifdef ELF_DEBUG_EXTRA
                mod::dbg::log("PT_LOAD perms applied: vaddr=[0x%x, 0x%x) flags=0x%x -> %s%s", start, end, ph->p_flags,
                              writable ? "USER" : "USER_READONLY", executable ? "" : "+NX");
#endif
            }
        }
    }
    record_loader_event(pid, loader_final_perms_op(options.image_role), final_perm_pages, 0, 0, perf_elapsed_since(FINAL_PERMS_STARTED_US),
                        WOS_PERF_CALLSITE(), final_perm_pages * mod::mm::virt::PAGE_SIZE);

    // Kernel-linked images use eager relocation, including JUMP_SLOT entries,
    // so the complete PT_GNU_RELRO range can become read-only. Dynamic images
    // are protected by ld.so after its relocation transaction completes.
    if (!DYNAMIC_LINKER_OWNS_RELOCATIONS) {
        for (Elf64_Half i = 0; i < elf_file.elf_head.e_phnum; i++) {
            auto const* ph = program_header_at(elf_file, i);
            if (ph->p_type == PT_GNU_RELRO) {
#ifdef ELF_DEBUG
                mod::dbg::log("Found PT_GNU_RELRO at vaddr=0x%x, memsz=0x%x", ph->p_vaddr, ph->p_memsz);
#endif
                uint64_t const START = page_align_down(ph->p_vaddr + elf_file.load_base);
                uint64_t const END = page_align_up(ph->p_vaddr + elf_file.load_base + ph->p_memsz);
                for (uint64_t va = START; va < END; va += mod::mm::virt::PAGE_SIZE) {
                    if (!mod::mm::virt::is_page_mapped(pagemap, va)) {
                        continue;
                    }
                    mod::mm::virt::unify_page_flags(pagemap, va, mod::mm::paging::page_types::USER_READONLY | mod::mm::paging::PAGE_NX);
                }
#ifdef ELF_DEBUG
                mod::dbg::log("RELRO enforced for vaddr=[0x%x, 0x%x)", start, end);
#endif
            }
        }
    }

// Print debug info for verification
#ifdef ELF_DEBUG
    debug::printDebugInfo(DEBUG_PID);
#endif
    // Return entry point, program header address, and ELF header address for auxv setup
    if (IS_PRIMARY_IMAGE) {
        debug::set_elf_headers(DEBUG_PID, elf_file.elf_head, elf_header_vaddr);
    }
    uint64_t image_start = 0;
    uint64_t image_end = 0;
    if (!runtime_image_extent(elf_file, image_start, image_end)) {
        return {.entry_point = 0, .program_header_addr = 0, .elf_header_addr = 0};
    }
    ElfLoadResult result = {.entry_point = elf_file.elf_head.e_entry + elf_file.load_base,
                            .load_base = elf_file.load_base,
                            .image_start = image_start,
                            .image_end = image_end,
                            .program_header_addr = program_headers_vaddr,
                            .elf_header_addr = elf_header_vaddr,
                            .program_header_count = elf_file.elf_head.e_phnum,
                            .program_header_ent_size = elf_file.elf_head.e_phentsize};

    // Extract PT_INTERP path if present
    for (Elf64_Half i = 0; i < elf_file.elf_head.e_phnum; i++) {
        auto const* ph = program_header_at(elf_file, i);
        if (ph->p_type == PT_INTERP && ph->p_filesz > 0 && ph->p_filesz < ElfLoadResult::INTERP_PATH_MAX) {
            if (!read_source(elf_file, ph->p_offset, &result.interp_path[0], static_cast<size_t>(ph->p_filesz))) {
                mod::dbg::log("ERROR: Failed reading PT_INTERP bytes at file offset 0x%lx (pid=%d)", ph->p_offset, pid);
                return {.entry_point = 0, .program_header_addr = 0, .elf_header_addr = 0};
            }
            result.interp_path[ph->p_filesz] = '\0';
            result.has_interp = true;
#ifdef ELF_DEBUG
            mod::dbg::log("PT_INTERP: dynamic linker path = '%s'", result.interpPath);
#endif
            break;
        }
    }

    debug_registration_rollback.release();
    return result;
}
}  // namespace

auto inspect_tls(const uint8_t* data, size_t size, TlsModule& out) -> bool {
    ElfFile elf{};
    if (!parse_elf(data, size, elf) || !source_is_valid(elf)) {
        out = {};
        return false;
    }
    return validated_tls_info(elf, out);
}

auto inspect_tls(const ElfFileView& view, TlsModule& out) -> bool {
    ElfFile const ELF = view_elf(view);
    if (!source_is_valid(ELF)) {
        out = {};
        return false;
    }
    return validated_tls_info(ELF, out);
}

}  // namespace ker::loader::elf
