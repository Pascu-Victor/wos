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

    int count() const { return static_cast<int>(syms_.size()); }
    int size() const { return count(); }
    const std::vector<SymbolEntry>& entries() const { return syms_; }

    void add(uint64_t addr, const std::string& name, uint64_t size);

    // Sort and demangle. Must be called after all add() calls.
    void finish();

    // Find the symbol containing or nearest-below `addr`.
    // Returns e.g. "func_name+0x1a" or empty string if no plausible match.
    std::optional<std::string> lookup(uint64_t addr) const;

   private:
    std::vector<SymbolEntry> syms_;
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

    int count() const { return static_cast<int>(sections_.size()); }
    int size() const { return count(); }
    const std::vector<SectionEntry>& entries() const { return sections_; }

    void add(uint64_t vaddr, uint64_t size, const std::string& name);

    // Sort. Must be called after all add() calls.
    void finish();

    // Find the section containing `addr`. Returns e.g. ".text+0x1a".
    std::optional<std::string> lookup(uint64_t addr) const;

   private:
    std::vector<SectionEntry> sections_;
};

// Parse a symbol table from raw ELF64 bytes (no BFD dependency).
// Returns nullptr if the ELF has no parseable symbol table.
std::unique_ptr<SymbolTable> parseElfSymtab(const uint8_t* elf, size_t len);
std::unique_ptr<SymbolTable> parseElfSymtab(const QByteArray& elf);

// Parse allocated section headers from raw ELF64 bytes.
std::unique_ptr<SectionMap> parseElfSections(const uint8_t* elf, size_t len);
std::unique_ptr<SectionMap> parseElfSections(const QByteArray& elf);

// Convenience: load symbols/sections from an ELF file on disk
std::unique_ptr<SymbolTable> loadSymbolsFromFile(const QString& path);
std::unique_ptr<SectionMap> loadSectionsFromFile(const QString& path);

// Load symbols/sections from a coredump's embedded ELF
std::unique_ptr<SymbolTable> loadSymbolsFromCoreDump(const CoreDump& dump);
std::unique_ptr<SectionMap> loadSectionsFromCoreDump(const CoreDump& dump);

// Try to resolve an address using multiple symbol tables and section maps.
// Returns e.g. "func_name+0x1a" or empty string.
std::optional<std::string> resolveAddress(uint64_t addr, const std::vector<SymbolTable*>& symTables,
                                          const std::vector<SectionMap*>& sectionMaps = {});

// Format address with optional symbol resolution
QString formatAddress(uint64_t addr, const std::vector<SymbolTable*>& symTables = {}, const std::vector<SectionMap*>& sectionMaps = {});

}  // namespace wosdbg
