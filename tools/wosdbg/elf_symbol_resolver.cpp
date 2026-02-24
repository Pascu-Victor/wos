#include "elf_symbol_resolver.h"

#include <QByteArray>
#include <QDebug>
#include <QFile>
#include <algorithm>
#include <cstring>

#include "coredump_parser.h"

// Use LLVM's demangler (linked as LLVMDemangle)
#include <llvm/Demangle/Demangle.h>

namespace wosdbg {
using std::string;

// -------------------- ELF64 constants --------------------

static constexpr uint8_t ELF_MAGIC[] = {0x7f, 'E', 'L', 'F'};
static constexpr uint32_t SHT_SYMTAB = 2;
static constexpr uint32_t SHT_DYNSYM = 11;
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

void SymbolTable::add(uint64_t addr, const std::string& name, uint64_t size) { syms_.push_back({addr, name, size}); }

void SymbolTable::finish() {
    // Demangle C++ names using LLVM
    for (auto& sym : syms_) {
        std::string demangled = llvm::demangle(sym.name);
        if (!demangled.empty() && demangled != sym.name) {
            sym.name = std::move(demangled);
        }
    }

    // Sort by address
    std::sort(syms_.begin(), syms_.end(), [](const SymbolEntry& a, const SymbolEntry& b) { return a.addr < b.addr; });
}

std::optional<std::string> SymbolTable::lookup(uint64_t addr) const {
    if (syms_.empty()) {
        return std::nullopt;
    }

    // Binary search: find rightmost entry with addr <= target
    auto it = std::upper_bound(syms_.begin(), syms_.end(), addr, [](uint64_t a, const SymbolEntry& e) { return a < e.addr; });
    if (it == syms_.begin()) {
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

void SectionMap::add(uint64_t vaddr, uint64_t size, const std::string& name) { sections_.push_back({vaddr, size, name}); }

void SectionMap::finish() {
    std::sort(sections_.begin(), sections_.end(), [](const SectionEntry& a, const SectionEntry& b) { return a.vaddr < b.vaddr; });
}

std::optional<std::string> SectionMap::lookup(uint64_t addr) const {
    if (sections_.empty()) {
        return std::nullopt;
    }

    auto it = std::upper_bound(sections_.begin(), sections_.end(), addr, [](uint64_t a, const SectionEntry& e) { return a < e.vaddr; });
    if (it == sections_.begin()) {
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
struct Elf64_Shdr {
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

static Elf64_Shdr read_shdr(const uint8_t* data, size_t off) {
    Elf64_Shdr h;
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

static std::string read_null_terminated(const uint8_t* data, size_t off, size_t max_len) {
    size_t end = off;
    while (end < off + max_len && data[end] != 0) ++end;
    return {reinterpret_cast<const char*>(data + off), end - off};
}

std::unique_ptr<SectionMap> parseElfSections(const uint8_t* elf, size_t len) {
    if (len < 64 || std::memcmp(elf, ELF_MAGIC, 4) != 0) {
        return nullptr;
    }
    if (elf[4] != 2) {
        return nullptr;  // Must be ELF64
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

std::unique_ptr<SymbolTable> parseElfSymtab(const uint8_t* elf, size_t len) {
    if (len < 64 || std::memcmp(elf, ELF_MAGIC, 4) != 0) {
        return nullptr;
    }
    if (elf[4] != 2) {
        return nullptr;  // Must be ELF64
    }

    auto e_shoff = read_le<uint64_t>(elf + 40);
    auto e_shentsize = read_le<uint16_t>(elf + 58);
    auto e_shnum = read_le<uint16_t>(elf + 60);

    if (e_shoff == 0 || e_shnum == 0) {
        return nullptr;
    }

    // Read all section headers
    std::vector<Elf64_Shdr> shdrs;
    shdrs.reserve(e_shnum);
    for (uint16_t i = 0; i < e_shnum; ++i) {
        size_t off = e_shoff + (static_cast<size_t>(i) * e_shentsize);
        if (off + 64 > len) {
            return nullptr;
        }
        shdrs.push_back(read_shdr(elf, off));
    }

    // Find symtab (prefer .symtab over .dynsym)
    const Elf64_Shdr* symtab_shdr = nullptr;
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
std::unique_ptr<SymbolTable> parseElfSymtab(const QByteArray& elf) {
    return parseElfSymtab(reinterpret_cast<const uint8_t*>(elf.constData()), static_cast<size_t>(elf.size()));
}

std::unique_ptr<SectionMap> parseElfSections(const QByteArray& elf) {
    return parseElfSections(reinterpret_cast<const uint8_t*>(elf.constData()), static_cast<size_t>(elf.size()));
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

std::unique_ptr<SymbolTable> loadSymbolsFromFile(const QString& path) {
    QByteArray data = read_file_bytes(path);
    if (data.isEmpty()) {
        return nullptr;
    }
    return parseElfSymtab(data);
}

std::unique_ptr<SectionMap> loadSectionsFromFile(const QString& path) {
    QByteArray data = read_file_bytes(path);
    if (data.isEmpty()) {
        return nullptr;
    }
    return parseElfSections(data);
}

std::unique_ptr<SymbolTable> loadSymbolsFromCoreDump(const CoreDump& dump) {
    QByteArray elf = dump.embeddedElf();
    if (elf.isEmpty()) {
        return nullptr;
    }
    return parseElfSymtab(elf);
}

std::unique_ptr<SectionMap> loadSectionsFromCoreDump(const CoreDump& dump) {
    QByteArray elf = dump.embeddedElf();
    if (elf.isEmpty()) {
        return nullptr;
    }
    return parseElfSections(elf);
}

// Address resolution
std::optional<std::string> resolveAddress(uint64_t addr, const std::vector<SymbolTable*>& sym_tables,
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

QString formatAddress(uint64_t addr, const std::vector<SymbolTable*>& sym_tables, const std::vector<SectionMap*>& section_maps) {
    QString base = formatU64(addr);
    if (!sym_tables.empty() || !section_maps.empty()) {
        auto sym = resolveAddress(addr, sym_tables, section_maps);
        if (sym) {
            return base + " <" + QString::fromStdString(*sym) + ">";
        }
    }
    return base;
}

}  // namespace wosdbg
