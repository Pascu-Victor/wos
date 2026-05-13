#include "elf_symbol_resolver.h"

#include <QByteArray>
#include <QDebug>
#include <QFile>
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "coredump_parser.h"

// Use LLVM's demangler (linked as LLVMDemangle)
#include <llvm/Demangle/Demangle.h>
#include <qlogging.h>
#include <qtypes.h>

namespace wosdbg {
using std::string;

// -------------------- ELF64 constants --------------------

static constexpr uint8_t ELF_MAGIC[] = {0x7f, 'E', 'L', 'F'};
static constexpr uint32_t PT_LOAD = 1;
static constexpr uint32_t PT_INTERP = 3;
static constexpr uint32_t SHT_SYMTAB = 2;
static constexpr uint32_t SHT_NOTE = 7;
static constexpr uint32_t SHT_DYNSYM = 11;
static constexpr uint32_t NT_GNU_BUILD_ID = 3;
static constexpr uint64_t SHF_ALLOC = 0x2;
static constexpr uint8_t STT_FUNC = 2;
static constexpr uint8_t STT_NOTYPE = 0;

// Helper to read little-endian values
template <typename T>
static T read_le(const uint8_t* data) {
    T val;
    std::memcpy(&val, data, sizeof(T));
    return val;
}

// -------------------- SymbolTable --------------------

void SymbolTable::add(uint64_t addr, const std::string& name, uint64_t size) { syms.push_back({.addr = addr, .name = name, .size = size}); }

void SymbolTable::finish() {
    // Demangle C++ names using LLVM
    for (auto& sym : syms) {
        std::string demangled = llvm::demangle(sym.name);
        if (!demangled.empty() && demangled != sym.name) {
            sym.name = std::move(demangled);
        }
    }

    // Sort by address
    std::ranges::sort(syms, [](const SymbolEntry& a, const SymbolEntry& b) { return a.addr < b.addr; });
}

std::optional<std::string> SymbolTable::lookup(uint64_t addr) const {
    if (syms.empty()) {
        return std::nullopt;
    }

    // Binary search: find rightmost entry with addr <= target
    auto it = std::ranges::upper_bound(syms, addr, std::ranges::less{}, &SymbolEntry::addr);
    if (it == syms.begin()) {
        return std::nullopt;
    }
    --it;

    uint64_t offset = addr - it->addr;
    if (offset < 0) {
        return std::nullopt;  // shouldn't happen
    }

    // If the symbol has a known size, only match within it.
    // If size is 0 (unknown), allow reasonable offset (0x10000).
    if (it->size > 0 && offset >= it->size) {
        // Still report if offset is small (sizes can be inaccurate in hand-written asm)
        if (offset > 0x1000) {
            return std::nullopt;
        }
    } else if (it->size == 0 && offset > 0x10000) {
        return std::nullopt;
    }

    if (offset == 0) {
        return it->name;
    }

    std::array<char, 32> buf;
    std::snprintf(buf.data(), sizeof(buf), "+0x%lx", static_cast<unsigned long>(offset));
    return it->name + buf.data();
}

// -------------------- SectionMap --------------------

void SectionMap::add(uint64_t vaddr, uint64_t size, const std::string& name) {
    sections.push_back({.vaddr = vaddr, .size = size, .name = name});
}

void SectionMap::finish() {
    std::ranges::sort(sections, [](const SectionEntry& a, const SectionEntry& b) { return a.vaddr < b.vaddr; });
}

std::optional<std::string> SectionMap::lookup(uint64_t addr) const {
    if (sections.empty()) {
        return std::nullopt;
    }

    auto it = std::ranges::upper_bound(sections, addr, std::ranges::less{}, &SectionEntry::vaddr);
    if (it == sections.begin()) {
        return std::nullopt;
    }
    --it;

    uint64_t offset = addr - it->vaddr;
    if (offset >= it->size) {
        return std::nullopt;
    }

    if (offset == 0) {
        return it->name;
    }

    std::array<char, 32> buf;
    std::snprintf(buf.data(), sizeof(buf), "+0x%lx", static_cast<unsigned long>(offset));
    return it->name + buf.data();
}

// -------------------- ELF64 Parsing --------------------

// ELF64 section header structure (on-disk layout)
struct Elf64Shdr {
    uint32_t sh_name;
    uint32_t sh_type;
    uint64_t sh_flags;
    uint64_t sh_addr;
    uint64_t sh_offset;
    uint64_t sh_size;
    uint32_t sh_link;
    uint32_t sh_info;
    int64_t sh_addralign;
    int64_t sh_entsize;
};

struct Elf64Phdr {
    uint32_t p_type;
    uint32_t p_flags;
    uint64_t p_offset;
    uint64_t p_vaddr;
    uint64_t p_paddr;
    uint64_t p_filesz;
    uint64_t p_memsz;
    uint64_t p_align;
};

static Elf64Shdr read_shdr(const uint8_t* data, size_t off) {
    Elf64Shdr h;
    h.sh_name = read_le<uint32_t>(data + off);
    h.sh_type = read_le<uint32_t>(data + off + 4);
    h.sh_flags = read_le<uint64_t>(data + off + 8);
    h.sh_addr = read_le<uint64_t>(data + off + 16);
    h.sh_offset = read_le<uint64_t>(data + off + 24);
    h.sh_size = read_le<uint64_t>(data + off + 32);
    h.sh_link = read_le<uint32_t>(data + off + 40);
    h.sh_info = read_le<uint32_t>(data + off + 44);
    h.sh_addralign = read_le<int64_t>(data + off + 48);
    h.sh_entsize = read_le<int64_t>(data + off + 56);
    return h;
}

static Elf64Phdr read_phdr(const uint8_t* data, size_t off) {
    Elf64Phdr h;
    h.p_type = read_le<uint32_t>(data + off);
    h.p_flags = read_le<uint32_t>(data + off + 4);
    h.p_offset = read_le<uint64_t>(data + off + 8);
    h.p_vaddr = read_le<uint64_t>(data + off + 16);
    h.p_paddr = read_le<uint64_t>(data + off + 24);
    h.p_filesz = read_le<uint64_t>(data + off + 32);
    h.p_memsz = read_le<uint64_t>(data + off + 40);
    h.p_align = read_le<uint64_t>(data + off + 48);
    return h;
}

static std::string read_null_terminated(const uint8_t* data, size_t off, size_t max_len) {
    size_t end = off;
    while (end < off + max_len && data[end] != 0) {
        ++end;
    }
    return {reinterpret_cast<const char*>(data + off), end - off};
}

static size_t align4(size_t value) { return (value + 3U) & ~size_t{3U}; }

static QString bytes_to_hex(const uint8_t* data, size_t len) {
    QString out;
    out.reserve(static_cast<qsizetype>(len * 2));
    for (size_t i = 0; i < len; ++i) {
        out += QString("%1").arg(static_cast<uint32_t>(data[i]), 2, 16, QChar('0'));
    }
    return out;
}

static auto is_elf64(const uint8_t* elf, size_t len) -> bool {
    if (len < 64 || std::memcmp(elf, ELF_MAGIC, 4) != 0) {
        return false;
    }
    return elf[4] == 2;
}

std::unique_ptr<SectionMap> parse_elf_sections(const uint8_t* elf, size_t len) {
    if (!is_elf64(elf, len)) {
        return nullptr;
    }

    auto e_shoff = read_le<uint64_t>(elf + 40);
    auto e_shentsize = read_le<uint16_t>(elf + 58);
    auto e_shnum = read_le<uint16_t>(elf + 60);
    auto e_shstrndx = read_le<uint16_t>(elf + 62);

    if (e_shoff == 0 || e_shnum == 0 || e_shstrndx >= e_shnum) {
        return nullptr;
    }

    // Read section name string table
    size_t shstr_off = e_shoff + (static_cast<size_t>(e_shstrndx) * e_shentsize);
    if (shstr_off + 64 > len) {
        return nullptr;
    }
    auto shstr = read_shdr(elf, shstr_off);
    if (shstr.sh_offset + shstr.sh_size > len) {
        return nullptr;
    }

    auto smap = std::make_unique<SectionMap>();

    for (uint16_t i = 0; i < e_shnum; ++i) {
        size_t hdr_off = e_shoff + (static_cast<size_t>(i) * e_shentsize);
        if (hdr_off + 64 > len) {
            continue;
        }
        auto hdr = read_shdr(elf, hdr_off);

        if (!(hdr.sh_flags & SHF_ALLOC)) {
            continue;
        }
        if (hdr.sh_addr == 0 || hdr.sh_size == 0) {
            continue;
        }
        if (hdr.sh_name >= shstr.sh_size) {
            continue;
        }

        std::string name = read_null_terminated(elf + shstr.sh_offset, hdr.sh_name, shstr.sh_size - hdr.sh_name);
        if (!name.empty()) {
            smap->add(hdr.sh_addr, hdr.sh_size, name);
        }
    }

    smap->finish();
    return smap->count() > 0 ? std::move(smap) : nullptr;
}

static std::unique_ptr<SectionMap> parse_elf_sections_biased(const uint8_t* elf, size_t len, uint64_t address_bias) {
    auto sections = parse_elf_sections(elf, len);
    if (!sections || address_bias == 0) {
        return sections;
    }
    auto shifted = std::make_unique<SectionMap>();
    for (const auto& section : sections->entries()) {
        shifted->add(section.vaddr + address_bias, section.size, section.name);
    }
    shifted->finish();
    return shifted->count() > 0 ? std::move(shifted) : nullptr;
}

std::unique_ptr<SymbolTable> parse_elf_symtab(const uint8_t* elf, size_t len) {
    if (!is_elf64(elf, len)) {
        return nullptr;
    }

    auto e_shoff = read_le<uint64_t>(elf + 40);
    auto e_shentsize = read_le<uint16_t>(elf + 58);
    auto e_shnum = read_le<uint16_t>(elf + 60);

    if (e_shoff == 0 || e_shnum == 0) {
        return nullptr;
    }

    // Read all section headers
    std::vector<Elf64Shdr> shdrs;
    shdrs.reserve(e_shnum);
    for (uint16_t i = 0; i < e_shnum; ++i) {
        size_t off = e_shoff + (static_cast<size_t>(i) * e_shentsize);
        if (off + 64 > len) {
            return nullptr;
        }
        shdrs.push_back(read_shdr(elf, off));
    }

    // Find symtab (prefer .symtab over .dynsym)
    const Elf64Shdr* symtab_shdr = nullptr;
    for (uint32_t stype : {SHT_SYMTAB, SHT_DYNSYM}) {
        for (const auto& s : shdrs) {
            if (s.sh_type == stype) {
                symtab_shdr = &s;
                break;
            }
        }
        if (symtab_shdr) {
            break;
        }
    }
    if (!symtab_shdr) {
        return nullptr;
    }

    // Get the linked string table
    if (symtab_shdr->sh_link >= shdrs.size()) {
        return nullptr;
    }
    const auto& strtab_shdr = shdrs[symtab_shdr->sh_link];
    if (strtab_shdr.sh_offset + strtab_shdr.sh_size > len) {
        return nullptr;
    }
    const uint8_t* strtab = elf + strtab_shdr.sh_offset;
    size_t strtab_size = strtab_shdr.sh_size;

    // Parse symbol entries (Elf64_Sym = 24 bytes)
    size_t entsize = symtab_shdr->sh_entsize > 0 ? static_cast<size_t>(symtab_shdr->sh_entsize) : 24;
    size_t sym_off = symtab_shdr->sh_offset;
    size_t sym_end = sym_off + symtab_shdr->sh_size;

    auto table = std::make_unique<SymbolTable>();

    while (sym_off + entsize <= sym_end && sym_off + entsize <= len) {
        auto st_name = read_le<uint32_t>(elf + sym_off);
        uint8_t st_info = elf[sym_off + 4];
        // st_other at symOff + 5
        // st_shndx at symOff + 6
        auto st_value = read_le<uint64_t>(elf + sym_off + 8);
        auto st_size = read_le<uint64_t>(elf + sym_off + 16);
        sym_off += entsize;

        uint8_t stt = st_info & 0xF;
        if (stt != STT_FUNC && stt != STT_NOTYPE) {
            continue;
        }
        if (st_value == 0) {
            continue;
        }
        if (st_name >= strtab_size) {
            continue;
        }

        std::string name = read_null_terminated(strtab, st_name, strtab_size - st_name);
        if (name.empty()) {
            continue;
        }

        table->add(st_value, name, st_size);
    }

    table->finish();
    return table->count() > 0 ? std::move(table) : nullptr;
}

// QByteArray overloads
std::unique_ptr<SymbolTable> parse_elf_symtab(const QByteArray& elf) {
    return parse_elf_symtab(reinterpret_cast<const uint8_t*>(elf.constData()), static_cast<size_t>(elf.size()));
}

std::unique_ptr<SectionMap> parse_elf_sections(const QByteArray& elf) {
    return parse_elf_sections(reinterpret_cast<const uint8_t*>(elf.constData()), static_cast<size_t>(elf.size()));
}

std::unique_ptr<SymbolTable> parse_elf_symtab(const QByteArray& elf, uint64_t address_bias) {
    auto table = parse_elf_symtab(elf);
    if (!table || address_bias == 0) {
        return table;
    }
    auto shifted = std::make_unique<SymbolTable>();
    for (const auto& sym : table->entries()) {
        shifted->add(sym.addr + address_bias, sym.name, sym.size);
    }
    shifted->finish();
    return shifted->count() > 0 ? std::move(shifted) : nullptr;
}

std::unique_ptr<SectionMap> parse_elf_sections(const QByteArray& elf, uint64_t address_bias) {
    return parse_elf_sections_biased(reinterpret_cast<const uint8_t*>(elf.constData()), static_cast<size_t>(elf.size()), address_bias);
}

QString elf_build_id(const QByteArray& elf_bytes) {
    const auto* elf = reinterpret_cast<const uint8_t*>(elf_bytes.constData());
    const auto LEN = static_cast<size_t>(elf_bytes.size());
    if (!is_elf64(elf, LEN)) {
        return {};
    }

    auto e_shoff = read_le<uint64_t>(elf + 40);
    auto e_shentsize = read_le<uint16_t>(elf + 58);
    auto e_shnum = read_le<uint16_t>(elf + 60);
    if (e_shoff == 0 || e_shnum == 0) {
        return {};
    }

    for (uint16_t i = 0; i < e_shnum; ++i) {
        size_t shoff = e_shoff + (static_cast<size_t>(i) * e_shentsize);
        if (shoff + 64 > LEN) {
            continue;
        }
        auto shdr = read_shdr(elf, shoff);
        if (shdr.sh_type != SHT_NOTE || shdr.sh_offset + shdr.sh_size > LEN) {
            continue;
        }
        auto note_off = static_cast<size_t>(shdr.sh_offset);
        const size_t NOTE_END = note_off + static_cast<size_t>(shdr.sh_size);
        while (note_off + 12 <= NOTE_END) {
            auto namesz = read_le<uint32_t>(elf + note_off);
            auto descsz = read_le<uint32_t>(elf + note_off + 4);
            auto type = read_le<uint32_t>(elf + note_off + 8);
            note_off += 12;
            size_t name_off = note_off;
            size_t desc_off = align4(name_off + namesz);
            size_t next_off = align4(desc_off + descsz);
            if (desc_off > NOTE_END || desc_off + descsz > NOTE_END || next_off > NOTE_END) {
                break;
            }
            QByteArray name(reinterpret_cast<const char*>(elf + name_off), static_cast<qsizetype>(namesz));
            if (!name.isEmpty() && name.endsWith('\0')) {
                name.chop(1);
            }
            if (type == NT_GNU_BUILD_ID && name == "GNU" && descsz > 0) {
                return bytes_to_hex(elf + desc_off, descsz);
            }
            note_off = next_off;
        }
    }
    return {};
}

ElfImageInfo elf_image_info(const QByteArray& elf_bytes) {
    ElfImageInfo info;
    const auto* elf = reinterpret_cast<const uint8_t*>(elf_bytes.constData());
    const auto LEN = static_cast<size_t>(elf_bytes.size());
    if (!is_elf64(elf, LEN)) {
        return info;
    }

    info.valid = true;
    info.type = read_le<uint16_t>(elf + 16);
    info.entry = read_le<uint64_t>(elf + 24);
    auto phoff = read_le<uint64_t>(elf + 32);
    auto phentsize = read_le<uint16_t>(elf + 54);
    auto phnum = read_le<uint16_t>(elf + 56);
    if (phoff == 0 || phentsize < 56) {
        return info;
    }

    for (uint16_t i = 0; i < phnum; ++i) {
        size_t off = static_cast<size_t>(phoff) + (static_cast<size_t>(i) * phentsize);
        if (off + 56 > LEN) {
            break;
        }
        Elf64Phdr ph = read_phdr(elf, off);
        if (ph.p_type == PT_LOAD) {
            info.load_segments.push_back(ElfLoadSegment{
                .vaddr = ph.p_vaddr, .memsz = ph.p_memsz, .filesz = ph.p_filesz, .offset = ph.p_offset, .flags = ph.p_flags});
        } else if (ph.p_type == PT_INTERP && ph.p_offset + ph.p_filesz <= LEN && ph.p_filesz > 0) {
            std::string interp = read_null_terminated(elf, static_cast<size_t>(ph.p_offset), static_cast<size_t>(ph.p_filesz));
            info.interpreter = QString::fromStdString(interp);
        }
    }
    std::ranges::sort(info.load_segments, [](const ElfLoadSegment& a, const ElfLoadSegment& b) { return a.vaddr < b.vaddr; });
    return info;
}

// File-based loading
static QByteArray read_file_bytes(const QString& path) {
    QFile file(path);
    if (!file.open(QIODevice::ReadOnly)) {
        qWarning() << "Could not open ELF file:" << path;
        return {};
    }
    return file.readAll();
}

std::unique_ptr<SymbolTable> load_symbols_from_file(const QString& path) {
    QByteArray data = read_file_bytes(path);
    if (data.isEmpty()) {
        return nullptr;
    }
    return parse_elf_symtab(data);
}

std::unique_ptr<SymbolTable> load_symbols_from_file(const QString& path, uint64_t address_bias) {
    QByteArray data = read_file_bytes(path);
    if (data.isEmpty()) {
        return nullptr;
    }
    return parse_elf_symtab(data, address_bias);
}

std::unique_ptr<SectionMap> load_sections_from_file(const QString& path) {
    QByteArray data = read_file_bytes(path);
    if (data.isEmpty()) {
        return nullptr;
    }
    return parse_elf_sections(data);
}

std::unique_ptr<SectionMap> load_sections_from_file(const QString& path, uint64_t address_bias) {
    QByteArray data = read_file_bytes(path);
    if (data.isEmpty()) {
        return nullptr;
    }
    return parse_elf_sections(data, address_bias);
}

QString elf_build_id_from_file(const QString& path) {
    QByteArray data = read_file_bytes(path);
    if (data.isEmpty()) {
        return {};
    }
    return elf_build_id(data);
}

ElfImageInfo elf_image_info_from_file(const QString& path) {
    QByteArray data = read_file_bytes(path);
    if (data.isEmpty()) {
        return {};
    }
    return elf_image_info(data);
}

std::unique_ptr<SymbolTable> load_symbols_from_core_dump(const CoreDump& dump) {
    QByteArray elf = dump.embedded_elf();
    if (elf.isEmpty()) {
        return nullptr;
    }
    return parse_elf_symtab(elf);
}

std::unique_ptr<SectionMap> load_sections_from_core_dump(const CoreDump& dump) {
    QByteArray elf = dump.embedded_elf();
    if (elf.isEmpty()) {
        return nullptr;
    }
    return parse_elf_sections(elf);
}

// Address resolution
std::optional<std::string> resolve_address(uint64_t addr, const std::vector<SymbolTable*>& sym_tables,
                                           const std::vector<SectionMap*>& section_maps) {
    for (auto* t : sym_tables) {
        if (!t) {
            continue;
        }
        auto result = t->lookup(addr);
        if (result) {
            return result;
        }
    }
    for (auto* m : section_maps) {
        if (!m) {
            continue;
        }
        auto result = m->lookup(addr);
        if (result) {
            return result;
        }
    }
    return std::nullopt;
}

QString format_address(uint64_t addr, const std::vector<SymbolTable*>& sym_tables, const std::vector<SectionMap*>& section_maps) {
    QString base = format_u64(addr);
    if (!sym_tables.empty() || !section_maps.empty()) {
        auto sym = resolve_address(addr, sym_tables, section_maps);
        if (sym) {
            return base + " <" + QString::fromStdString(*sym) + ">";
        }
    }
    return base;
}

// -------------------- elfBytesAtVA --------------------

std::vector<uint8_t> elf_bytes_at_va(const QByteArray& elf, uint64_t va, size_t len) {
    const auto* data = reinterpret_cast<const uint8_t*>(elf.constData());
    const auto ELF_LEN = static_cast<size_t>(elf.size());

    if (!is_elf64(data, ELF_LEN)) {
        return {};
    }

    // e_phoff, e_phentsize, e_phnum
    auto phoff = read_le<uint64_t>(data + 32);
    auto phentsize = read_le<uint16_t>(data + 54);
    auto phnum = read_le<uint16_t>(data + 56);

    for (uint16_t i = 0; i < phnum; ++i) {
        size_t ph_off = static_cast<size_t>(phoff) + (i * phentsize);
        if (ph_off + 56 > ELF_LEN) {
            break;
        }

        auto p_type = read_le<uint32_t>(data + ph_off);
        if (p_type != PT_LOAD) {
            continue;
        }

        auto p_offset = read_le<uint64_t>(data + ph_off + 8);
        auto p_vaddr = read_le<uint64_t>(data + ph_off + 16);
        auto p_filesz = read_le<uint64_t>(data + ph_off + 32);

        if (va < p_vaddr || va >= p_vaddr + p_filesz) {
            continue;
        }

        uint64_t seg_off = va - p_vaddr;
        uint64_t file_off = p_offset + seg_off;
        uint64_t avail = p_filesz - seg_off;
        size_t to_read = static_cast<size_t>(std::min<uint64_t>(avail, static_cast<uint64_t>(len)));

        if (file_off + to_read > ELF_LEN) {
            to_read = ELF_LEN - static_cast<size_t>(file_off);
        }
        if (to_read == 0) {
            return {};
        }

        return std::vector<uint8_t>(data + file_off, data + file_off + to_read);
    }
    return {};
}

std::vector<uint8_t> elf_bytes_at_runtime_va(const QByteArray& elf, uint64_t runtime_va, uint64_t load_base, size_t len) {
    if (runtime_va < load_base) {
        return {};
    }
    return elf_bytes_at_va(elf, runtime_va - load_base, len);
}

}  // namespace wosdbg
