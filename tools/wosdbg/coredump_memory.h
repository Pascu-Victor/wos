#pragma once

#include <QByteArray>
#include <QString>
#include <cstdint>
#include <optional>
#include <vector>

namespace wosdbg {

struct CoreDump;
struct CoreDumpSegment;
class SymbolTable;
class SectionMap;

// Find the segment containing a given virtual address
const CoreDumpSegment* find_segment_for_va(const CoreDump& dump, uint64_t va);

// Read `length` bytes starting at virtual address `vaStart` from coredump segments.
// Returns empty QByteArray if the address range is not fully covered by present segments.
// For ranges spanning multiple pages, stitches data from multiple segments.
QByteArray read_va_bytes(const CoreDump& dump, uint64_t vaStart, size_t length);

// A single annotated qword from a memory dump
struct AnnotatedQword {
    uint64_t va;     // Virtual address of this qword
    uint64_t value;  // The 8-byte value (little-endian)
    QString symbol;  // Resolved symbol name (if value is a code address)
    QString notes;   // Annotation string (RSP/RBP markers, heuristics)
    QString gutter;  // Direction indicator (" v ", ">>>", " ^ ")
};

// Generate annotation hints for a qword value at a given virtual address
QString annotate_qword(uint64_t va, uint64_t value, const CoreDump& dump, const std::vector<SymbolTable*>& sym_tables = {},
                       const std::vector<SectionMap*>& section_maps = {});

// Dump a virtual address range as annotated qwords.
// Returns a vector of AnnotatedQword entries, or empty on failure.
std::vector<AnnotatedQword> dump_range(const CoreDump& dump, uint64_t vaStart, uint64_t vaEnd,
                                       const std::vector<SymbolTable*>& sym_tables = {}, const std::vector<SectionMap*>& section_maps = {});

// A raw hex dump row (16 bytes per row)
struct HexDumpRow {
    uint64_t va;
    QByteArray bytes;      // Up to 16 bytes
    QString hex_string;    // Formatted hex bytes
    QString ascii_string;  // Printable ASCII representation
};

// Dump a range as raw hex rows (16 bytes per row)
std::vector<HexDumpRow> dump_range_hex(const CoreDump& dump, uint64_t vaStart, uint64_t vaEnd);

}  // namespace wosdbg
