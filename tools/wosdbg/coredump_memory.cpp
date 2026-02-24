#include "coredump_memory.h"

#include <QDebug>
#include <cstring>

#include "coredump_parser.h"
#include "elf_symbol_resolver.h"

namespace wosdbg {

const CoreDumpSegment* findSegmentForVa(const CoreDump& dump, uint64_t va) {
    size_t count = std::min(static_cast<size_t>(dump.segmentCount), dump.segments.size());
    for (size_t i = 0; i < count; ++i) {
        const auto& seg = dump.segments[i];
        if (seg.isPresent() && seg.vaddr <= va && va < seg.vaddrEnd()) {
            return &seg;
        }
    }
    return nullptr;
}

QByteArray readVaBytes(const CoreDump& dump, uint64_t vaStart, size_t length) {
    QByteArray result;
    result.reserve(static_cast<qsizetype>(length));

    uint64_t va = vaStart;
    size_t remaining = length;

    while (remaining > 0) {
        const auto* seg = findSegmentForVa(dump, va);
        if (!seg) {
            return {};
        }

        uint64_t seg_offset = va - seg->vaddr;
        uint64_t avail = seg->size - seg_offset;
        size_t to_read = std::min(avail, static_cast<uint64_t>(remaining));
        uint64_t file_off = seg->fileOffset + seg_offset;

        if (static_cast<int64_t>(file_off + to_read) > dump.raw.size()) {
            return {};
        }

        result.append(dump.raw.constData() + file_off, static_cast<qsizetype>(to_read));
        va += to_read;
        remaining -= to_read;
    }

    return result;
}

QString annotateQword(uint64_t va, uint64_t value, const CoreDump& dump, const std::vector<SymbolTable*>& sym_tables,
                      const std::vector<SectionMap*>& section_maps) {
    QStringList notes;
    uint64_t trap_rsp = dump.trapFrame.rsp;
    uint64_t trap_rbp = dump.trapRegs.rbp;
    uint64_t saved_rsp = dump.savedFrame.rsp;
    uint64_t saved_rbp = dump.savedRegs.rbp;

    // RSP/RBP markers
    if (va == trap_rsp) {
        notes << "<-- trap RSP";
    }
    if (va == trap_rsp - 8) {
        notes << "<-- trap RSP-8";
    }
    if (va == trap_rsp + 8) {
        notes << "<-- trap RSP+8";
    }
    if (va == trap_rbp) {
        notes << "<-- trap RBP";
    }
    if (va == saved_rsp) {
        notes << "<-- saved RSP";
    }
    if (va == saved_rbp) {
        notes << "<-- saved RBP";
    }

    // Value heuristics
    if (value == 0) {
        notes << "[zero]";
    } else if (value > 0 && value < 0x1000) {
        notes << QString("[small: %1]").arg(value);
    } else if (value >= 0x400000 && value <= 0xFFFFFF) {
        auto sym = resolveAddress(value, sym_tables, section_maps);
        if (sym) {
            notes << QString("[code: %1]").arg(QString::fromStdString(*sym));
        } else {
            notes << "[code addr?]";
        }
    } else if ((value >> 40) == 0x7ffe || (value >> 40) == 0x7fff) {
        notes << "[stack ptr?]";
    } else if (value == dump.trapFrame.rip) {
        notes << "[== trap RIP]";
    } else if (value == dump.savedFrame.rip) {
        notes << "[== saved RIP]";
    }
    // Also resolve kernel-range addresses
    else if (value >= 0xffffffff80000000ULL) {
        auto sym = resolveAddress(value, sym_tables, section_maps);
        if (sym) {
            notes << QString("[kernel: %1]").arg(QString::fromStdString(*sym));
        }
    }

    return notes.join("  ");
}

std::vector<AnnotatedQword> dumpRange(const CoreDump& dump, uint64_t va_start, uint64_t va_end, const std::vector<SymbolTable*>& sym_tables,
                                      const std::vector<SectionMap*>& section_maps) {
    // Align start down to 8-byte boundary
    uint64_t va_start_aligned = va_start & ~7ULL;
    if (va_end <= va_start_aligned) {
        return {};
    }

    uint64_t length = va_end - va_start_aligned;
    if (length > 0x10000) {
        qWarning() << "Requested range too large:" << length << "bytes (max 64KiB)";
        return {};
    }

    QByteArray data = readVaBytes(dump, va_start_aligned, static_cast<size_t>(length));
    if (data.isEmpty()) {
        return {};
    }

    uint64_t trap_rsp = dump.trapFrame.rsp;

    std::vector<AnnotatedQword> result;
    result.reserve(static_cast<size_t>(length / 8));

    for (qsizetype off = 0; off + 7 < data.size(); off += 8) {
        uint64_t va = va_start_aligned + static_cast<uint64_t>(off);
        uint64_t qword;
        std::memcpy(&qword, data.constData() + off, 8);

        AnnotatedQword aq;
        aq.va = va;
        aq.value = qword;
        aq.notes = annotateQword(va, qword, dump, sym_tables, section_maps);

        // Resolve value as symbol if it looks like a code address
        auto sym = resolveAddress(qword, sym_tables, section_maps);
        if (sym) {
            aq.symbol = QString::fromStdString(*sym);
        }

        // Stack direction gutter
        if (va < trap_rsp) {
            aq.gutter = " v ";  // below RSP — stack growth direction
        } else if (va == trap_rsp) {
            aq.gutter = ">>>";  // current stack pointer
        } else {
            aq.gutter = " ^ ";  // above RSP — toward caller frames
        }

        result.push_back(std::move(aq));
    }

    return result;
}

std::vector<HexDumpRow> dumpRangeHex(const CoreDump& dump, uint64_t vaStart, uint64_t vaEnd) {
    uint64_t va_start_aligned = vaStart & ~0xFULL;  // Align to 16 bytes
    if (vaEnd <= va_start_aligned) {
        return {};
    }

    uint64_t length = vaEnd - va_start_aligned;
    if (length > 0x10000) {
        return {};
    }

    QByteArray data = readVaBytes(dump, va_start_aligned, static_cast<size_t>(length));
    if (data.isEmpty()) {
        return {};
    }

    std::vector<HexDumpRow> rows;
    for (qsizetype off = 0; off < data.size(); off += 16) {
        HexDumpRow row;
        row.va = va_start_aligned + static_cast<uint64_t>(off);
        qsizetype count = std::min(static_cast<qsizetype>(16), data.size() - off);
        row.bytes = data.mid(off, count);

        // Format hex string
        QStringList hex_parts;
        for (int i = 0; i < count; ++i) {
            hex_parts << QString("%1").arg(static_cast<uint8_t>(row.bytes[i]), 2, 16, QChar('0'));
        }
        row.hexString = hex_parts.join(' ');

        // Format ASCII
        QString ascii;
        for (int i = 0; i < count; ++i) {
            auto b = static_cast<uint8_t>(row.bytes[i]);
            ascii += (b >= 32 && b < 127) ? QChar(b) : QChar('.');
        }
        row.asciiString = ascii;

        rows.push_back(std::move(row));
    }

    return rows;
}

}  // namespace wosdbg
