#pragma once

#include <QString>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

class QByteArray;

namespace wosdbg {

struct CoreDump;

struct ElfLoadSegment {
    uint64_t vaddr = 0;
    uint64_t memsz = 0;
    uint64_t filesz = 0;
    uint64_t offset = 0;
    uint32_t flags = 0;
};

struct ElfImageInfo {
    bool valid = false;
    uint16_t type = 0;
    uint64_t entry = 0;
    QString interpreter;
    std::vector<ElfLoadSegment> load_segments;
};

// A symbol table entry
struct SymbolEntry {
    uint64_t addr;
    std::string name;
    uint64_t size;
};

// Sorted symbol table for address-to-name lookups (port of Python SymbolTable)
class SymbolTable {
   public:
    SymbolTable() = default;

    [[nodiscard]] int count() const { return static_cast<int>(syms.size()); }
    [[nodiscard]] int size() const { return count(); }
    [[nodiscard]] const std::vector<SymbolEntry>& entries() const { return syms; }

    void add(uint64_t addr, const std::string& name, uint64_t size);

    // Sort and demangle. Must be called after all add() calls.
    void finish();

    // Find the symbol containing or nearest-below `addr`.
    // Returns e.g. "func_name+0x1a" or empty string if no plausible match.
    [[nodiscard]] std::optional<std::string> lookup(uint64_t addr) const;

   private:
    std::vector<SymbolEntry> syms;
};

// A section map entry
struct SectionEntry {
    uint64_t vaddr;
    uint64_t size;
    std::string name;
};

// Maps virtual addresses to ELF section names (port of Python SectionMap)
class SectionMap {
   public:
    SectionMap() = default;

    [[nodiscard]] int count() const { return static_cast<int>(sections.size()); }
    [[nodiscard]] int size() const { return count(); }
    [[nodiscard]] const std::vector<SectionEntry>& entries() const { return sections; }

    void add(uint64_t vaddr, uint64_t size, const std::string& name);

    // Sort. Must be called after all add() calls.
    void finish();

    // Find the section containing `addr`. Returns e.g. ".text+0x1a".
    [[nodiscard]] std::optional<std::string> lookup(uint64_t addr) const;

   private:
    std::vector<SectionEntry> sections;
};

// Parse a symbol table from raw ELF64 bytes (no BFD dependency).
// Returns nullptr if the ELF has no parseable symbol table.
std::unique_ptr<SymbolTable> parse_elf_symtab(const uint8_t* elf, size_t len);
std::unique_ptr<SymbolTable> parse_elf_symtab(const QByteArray& elf);
std::unique_ptr<SymbolTable> parse_elf_symtab(const QByteArray& elf, uint64_t address_bias);

// Parse allocated section headers from raw ELF64 bytes.
std::unique_ptr<SectionMap> parse_elf_sections(const uint8_t* elf, size_t len);
std::unique_ptr<SectionMap> parse_elf_sections(const QByteArray& elf);
std::unique_ptr<SectionMap> parse_elf_sections(const QByteArray& elf, uint64_t address_bias);

// Convenience: load symbols/sections from an ELF file on disk
std::unique_ptr<SymbolTable> load_symbols_from_file(const QString& path);
std::unique_ptr<SymbolTable> load_symbols_from_file(const QString& path, uint64_t address_bias);
std::unique_ptr<SectionMap> load_sections_from_file(const QString& path);
std::unique_ptr<SectionMap> load_sections_from_file(const QString& path, uint64_t address_bias);
QString elf_build_id(const QByteArray& elf);
QString elf_build_id_from_file(const QString& path);
ElfImageInfo elf_image_info(const QByteArray& elf);
ElfImageInfo elf_image_info_from_file(const QString& path);

// Load symbols/sections from a coredump's embedded ELF
std::unique_ptr<SymbolTable> load_symbols_from_core_dump(const CoreDump& dump);
std::unique_ptr<SectionMap> load_sections_from_core_dump(const CoreDump& dump);

/// Extract up to `len` bytes from `elf` at virtual address `va` by walking
/// the ELF64 program headers (PT_LOAD segments).  Returns an empty vector if
/// the address is not covered.
std::vector<uint8_t> elf_bytes_at_va(const QByteArray& elf, uint64_t va, size_t len);
std::vector<uint8_t> elf_bytes_at_runtime_va(const QByteArray& elf, uint64_t runtime_va, uint64_t load_base, size_t len);

// Try to resolve an address using multiple symbol tables and section maps.
// Returns e.g. "func_name+0x1a" or empty string.
std::optional<std::string> resolve_address(uint64_t addr, const std::vector<SymbolTable*>& sym_tables,
                                           const std::vector<SectionMap*>& section_maps = {});

// Format address with optional symbol resolution
QString format_address(uint64_t addr, const std::vector<SymbolTable*>& sym_tables = {}, const std::vector<SectionMap*>& section_maps = {});

}  // namespace wosdbg
