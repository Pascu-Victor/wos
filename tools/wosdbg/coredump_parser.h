#pragma once

#include <QByteArray>
#include <QString>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace wosdbg {

// Magic number: "WOSCODMP" as little-endian uint64
static constexpr uint64_t COREDUMP_MAGIC = 0x504D55444F43534FULL;

// Maximum segments in a coredump (MAX_STACK_PAGES(4) + 1 fault page)
static constexpr int MAX_SEGMENTS = 5;

// Segment types
enum class SegmentType : uint32_t {
    ZeroUnmapped = 0,
    StackPage = 1,
    FaultPage = 2,
};

QString segmentTypeName(uint32_t type);

// x86-64 interrupt frame — matches WOS kernel InterruptFrame layout
struct InterruptFrame {
    uint64_t intNum;
    uint64_t errCode;
    uint64_t rip;
    uint64_t cs;
    uint64_t rflags;
    uint64_t rsp;
    uint64_t ss;
};

// x86-64 general purpose registers — matches WOS kernel GPRegs layout
// Order: r15, r14, ..., r8, rbp, rdi, rsi, rdx, rcx, rbx, rax
struct GPRegs {
    uint64_t r15;
    uint64_t r14;
    uint64_t r13;
    uint64_t r12;
    uint64_t r11;
    uint64_t r10;
    uint64_t r9;
    uint64_t r8;
    uint64_t rbp;
    uint64_t rdi;
    uint64_t rsi;
    uint64_t rdx;
    uint64_t rcx;
    uint64_t rbx;
    uint64_t rax;
};

// A single memory segment in the coredump
struct CoreDumpSegment {
    uint64_t vaddr;
    uint64_t size;
    uint64_t fileOffset;
    uint32_t type;
    uint32_t present;

    uint64_t vaddrEnd() const { return vaddr + size; }
    QString typeName() const { return segmentTypeName(type); }
    bool isPresent() const { return present != 0; }
};

// Full parsed coredump
struct CoreDump {
    // Header fields
    uint64_t magic;
    uint32_t version;
    uint32_t headerSize;
    uint64_t timestamp;
    uint64_t pid;
    uint64_t cpu;
    uint64_t intNum;
    uint64_t errCode;
    uint64_t cr2;
    uint64_t cr3;

    // CPU state at trap
    InterruptFrame trapFrame;
    GPRegs trapRegs;

    // Saved CPU state (before trap)
    InterruptFrame savedFrame;
    GPRegs savedRegs;

    // Task metadata
    uint64_t taskEntry;
    uint64_t taskPagemap;
    uint64_t elfHeaderAddr;
    uint64_t programHeaderAddr;
    uint64_t segmentCount;
    uint64_t segmentTableOffset;
    uint64_t elfSize;
    uint64_t elfOffset;

    // Segment table
    std::vector<CoreDumpSegment> segments;

    // Raw file bytes (segments reference file offsets into this)
    QByteArray raw;

    // Source file path (set after parsing for symbol resolution)
    QString sourceFilename;

    // Get the embedded ELF bytes, if present
    QByteArray embeddedElf() const;

    // Check if magic is valid
    bool isValid() const { return magic == COREDUMP_MAGIC; }
};

// Parse a coredump from raw binary data.
// Returns std::nullopt on parse failure.
std::optional<CoreDump> parseCoreDump(const QByteArray& data);

/// Load and parse a coredump from a file path. Sets sourceFilename on success.
std::unique_ptr<CoreDump> parseCoreDump(const QString& filePath);

// Get human-readable interrupt/exception name
QString interruptName(uint64_t num);

// Format a uint64 as "0x%016llx"
QString formatU64(uint64_t val);

// Parse binary name from coredump filename pattern: {binary}_{timestamp}_coredump.bin
QString parseBinaryNameFromFilename(const QString& filename);

}  // namespace wosdbg
