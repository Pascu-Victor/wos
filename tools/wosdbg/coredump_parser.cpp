#include "coredump_parser.h"

#include <QDebug>
#include <QFile>
#include <QFileInfo>
#include <cstring>

namespace wosdbg {

QString segmentTypeName(uint32_t type) {
    switch (type) {
        case 0:
            return "Zero/Unmapped";
        case 1:
            return "StackPage";
        case 2:
            return "FaultPage";
        default:
            return QString("Unknown(%1)").arg(type);
    }
}

QString interruptName(uint64_t num) {
    switch (num) {
        case 0:
            return "#DE Divide Error";
        case 1:
            return "#DB Debug";
        case 2:
            return "NMI";
        case 3:
            return "#BP Breakpoint";
        case 4:
            return "#OF Overflow";
        case 5:
            return "#BR Bound Range";
        case 6:
            return "#UD Invalid Opcode";
        case 7:
            return "#NM Device Not Available";
        case 8:
            return "#DF Double Fault";
        case 13:
            return "#GP General Protection";
        case 14:
            return "#PF Page Fault";
        case 16:
            return "#MF x87 FP";
        case 17:
            return "#AC Alignment Check";
        case 18:
            return "#MC Machine Check";
        case 19:
            return "#XM SIMD FP";
        default:
            return QString("INT %1").arg(num);
    }
}

QString formatU64(uint64_t val) { return QString("0x%1").arg(val, 16, 16, QChar('0')); }

QString parseBinaryNameFromFilename(const QString& filename) {
    // Pattern: {binary}_{timestamp}_coredump.bin
    QFileInfo fi(filename);
    QString base = fi.completeBaseName();  // e.g. "httpd_6780485014_coredump"
    int firstUnderscore = base.indexOf('_');
    if (firstUnderscore > 0) {
        return base.left(firstUnderscore);
    }
    return {};
}

// Helper: read little-endian values from raw bytes
template <typename T>
static T readLE(const char* data) {
    T val;
    std::memcpy(&val, data, sizeof(T));
    return val;
}

static InterruptFrame parseInterruptFrame(const char* data, size_t& off) {
    InterruptFrame f;
    f.intNum = readLE<uint64_t>(data + off);
    off += 8;
    f.errCode = readLE<uint64_t>(data + off);
    off += 8;
    f.rip = readLE<uint64_t>(data + off);
    off += 8;
    f.cs = readLE<uint64_t>(data + off);
    off += 8;
    f.rflags = readLE<uint64_t>(data + off);
    off += 8;
    f.rsp = readLE<uint64_t>(data + off);
    off += 8;
    f.ss = readLE<uint64_t>(data + off);
    off += 8;
    return f;
}

static GPRegs parseGPRegs(const char* data, size_t& off) {
    GPRegs r;
    r.r15 = readLE<uint64_t>(data + off);
    off += 8;
    r.r14 = readLE<uint64_t>(data + off);
    off += 8;
    r.r13 = readLE<uint64_t>(data + off);
    off += 8;
    r.r12 = readLE<uint64_t>(data + off);
    off += 8;
    r.r11 = readLE<uint64_t>(data + off);
    off += 8;
    r.r10 = readLE<uint64_t>(data + off);
    off += 8;
    r.r9 = readLE<uint64_t>(data + off);
    off += 8;
    r.r8 = readLE<uint64_t>(data + off);
    off += 8;
    r.rbp = readLE<uint64_t>(data + off);
    off += 8;
    r.rdi = readLE<uint64_t>(data + off);
    off += 8;
    r.rsi = readLE<uint64_t>(data + off);
    off += 8;
    r.rdx = readLE<uint64_t>(data + off);
    off += 8;
    r.rcx = readLE<uint64_t>(data + off);
    off += 8;
    r.rbx = readLE<uint64_t>(data + off);
    off += 8;
    r.rax = readLE<uint64_t>(data + off);
    off += 8;
    return r;
}

std::optional<CoreDump> parseCoreDump(const QByteArray& data) {
    // Minimum size: header (16) + 7×8 fields + 2×(7×8 + 15×8) frames/regs + 8×8 task metadata = 488 bytes
    static constexpr size_t MIN_HEADER_SIZE = 488;
    if (static_cast<size_t>(data.size()) < MIN_HEADER_SIZE) {
        qWarning() << "Coredump too small:" << data.size() << "bytes (minimum" << MIN_HEADER_SIZE << ")";
        return std::nullopt;
    }

    const char* d = data.constData();
    size_t off = 0;

    CoreDump dump;
    dump.raw = data;

    // Header preamble: magic(8) + version(4) + headerSize(4)
    dump.magic = readLE<uint64_t>(d + off);
    off += 8;
    dump.version = readLE<uint32_t>(d + off);
    off += 4;
    dump.headerSize = readLE<uint32_t>(d + off);
    off += 4;

    if (dump.magic != COREDUMP_MAGIC) {
        qWarning() << "Bad coredump magic:" << formatU64(dump.magic) << "(expected" << formatU64(COREDUMP_MAGIC) << ")";
        return std::nullopt;
    }

    // 7 × uint64: timestamp, pid, cpu, intNum, errCode, cr2, cr3
    dump.timestamp = readLE<uint64_t>(d + off);
    off += 8;
    dump.pid = readLE<uint64_t>(d + off);
    off += 8;
    dump.cpu = readLE<uint64_t>(d + off);
    off += 8;
    dump.intNum = readLE<uint64_t>(d + off);
    off += 8;
    dump.errCode = readLE<uint64_t>(d + off);
    off += 8;
    dump.cr2 = readLE<uint64_t>(d + off);
    off += 8;
    dump.cr3 = readLE<uint64_t>(d + off);
    off += 8;

    // Trap state
    dump.trapFrame = parseInterruptFrame(d, off);
    dump.trapRegs = parseGPRegs(d, off);

    // Saved state
    dump.savedFrame = parseInterruptFrame(d, off);
    dump.savedRegs = parseGPRegs(d, off);

    // Task metadata: 8 × uint64
    dump.taskEntry = readLE<uint64_t>(d + off);
    off += 8;
    dump.taskPagemap = readLE<uint64_t>(d + off);
    off += 8;
    dump.elfHeaderAddr = readLE<uint64_t>(d + off);
    off += 8;
    dump.programHeaderAddr = readLE<uint64_t>(d + off);
    off += 8;
    dump.segmentCount = readLE<uint64_t>(d + off);
    off += 8;
    dump.segmentTableOffset = readLE<uint64_t>(d + off);
    off += 8;
    dump.elfSize = readLE<uint64_t>(d + off);
    off += 8;
    dump.elfOffset = readLE<uint64_t>(d + off);
    off += 8;

    // Parse segment table (fixed array of MAX_SEGMENTS entries)
    // Each segment: vaddr(8) + size(8) + fileOffset(8) + type(4) + present(4) = 32 bytes
    static constexpr size_t SEGMENT_ENTRY_SIZE = 32;
    dump.segments.reserve(MAX_SEGMENTS);
    for (int i = 0; i < MAX_SEGMENTS; ++i) {
        size_t soff = dump.segmentTableOffset + static_cast<size_t>(i) * SEGMENT_ENTRY_SIZE;
        if (soff + SEGMENT_ENTRY_SIZE > static_cast<size_t>(data.size())) {
            qWarning() << "Segment table extends beyond file at segment" << i;
            break;
        }
        CoreDumpSegment seg;
        seg.vaddr = readLE<uint64_t>(d + soff);
        seg.size = readLE<uint64_t>(d + soff + 8);
        seg.fileOffset = readLE<uint64_t>(d + soff + 16);
        seg.type = readLE<uint32_t>(d + soff + 24);
        seg.present = readLE<uint32_t>(d + soff + 28);
        dump.segments.push_back(seg);
    }

    return dump;
}

QByteArray CoreDump::embeddedElf() const {
    if (elfSize > 0 && elfOffset > 0 && static_cast<int64_t>(elfOffset + elfSize) <= raw.size()) {
        return raw.mid(static_cast<qsizetype>(elfOffset), static_cast<qsizetype>(elfSize));
    }
    return {};
}

std::unique_ptr<CoreDump> parseCoreDump(const QString& filePath) {
    QFile file(filePath);
    if (!file.open(QIODevice::ReadOnly)) {
        qWarning() << "Cannot open coredump file:" << filePath;
        return nullptr;
    }
    QByteArray data = file.readAll();
    file.close();

    auto result = parseCoreDump(data);
    if (!result) return nullptr;

    auto dump = std::make_unique<CoreDump>(std::move(*result));
    dump->sourceFilename = QFileInfo(filePath).fileName();
    return dump;
}

}  // namespace wosdbg
