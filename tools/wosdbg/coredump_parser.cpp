#include "coredump_parser.h"

#include <qlogging.h>
#include <qtypes.h>

#include <QDebug>
#include <QFile>
#include <QFileInfo>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <utility>

namespace wosdbg {

QString segment_type_name(uint32_t type) {
    switch (type) {
        case 0:
            return "Zero/Unmapped";
        case 1:
            return "StackPage";
        case 2:
            return "FaultPage";
        case 3:
            return "MemoryPage";
        default:
            return QString("Unknown(%1)").arg(type);
    }
}

QString interrupt_name(uint64_t num) {
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

QString format_u64(uint64_t val) { return QString("0x%1").arg(val, 16, 16, QChar('0')); }

QString parse_binary_name_from_filename(const QString& filename) {
    // Pattern: {binary}_{timestamp}_coredump.bin
    QFileInfo fi(filename);
    QString base = fi.completeBaseName();  // e.g. "httpd_6780485014_coredump"
    int first_underscore = base.indexOf('_');
    if (first_underscore > 0) {
        return base.left(first_underscore);
    }
    return {};
}

// Helper: read little-endian values from raw bytes
template <typename T>
static T read_le(const char* data) {
    T val;
    std::memcpy(&val, data, sizeof(T));
    return val;
}

static InterruptFrame parse_interrupt_frame(const char* data, size_t& off) {
    InterruptFrame f;
    f.int_num = read_le<uint64_t>(data + off);
    off += 8;
    f.err_code = read_le<uint64_t>(data + off);
    off += 8;
    f.rip = read_le<uint64_t>(data + off);
    off += 8;
    f.cs = read_le<uint64_t>(data + off);
    off += 8;
    f.rflags = read_le<uint64_t>(data + off);
    off += 8;
    f.rsp = read_le<uint64_t>(data + off);
    off += 8;
    f.ss = read_le<uint64_t>(data + off);
    off += 8;
    return f;
}

static GPRegs parse_gp_regs(const char* data, size_t& off) {
    GPRegs r;
    r.r15 = read_le<uint64_t>(data + off);
    off += 8;
    r.r14 = read_le<uint64_t>(data + off);
    off += 8;
    r.r13 = read_le<uint64_t>(data + off);
    off += 8;
    r.r12 = read_le<uint64_t>(data + off);
    off += 8;
    r.r11 = read_le<uint64_t>(data + off);
    off += 8;
    r.r10 = read_le<uint64_t>(data + off);
    off += 8;
    r.r9 = read_le<uint64_t>(data + off);
    off += 8;
    r.r8 = read_le<uint64_t>(data + off);
    off += 8;
    r.rbp = read_le<uint64_t>(data + off);
    off += 8;
    r.rdi = read_le<uint64_t>(data + off);
    off += 8;
    r.rsi = read_le<uint64_t>(data + off);
    off += 8;
    r.rdx = read_le<uint64_t>(data + off);
    off += 8;
    r.rcx = read_le<uint64_t>(data + off);
    off += 8;
    r.rbx = read_le<uint64_t>(data + off);
    off += 8;
    r.rax = read_le<uint64_t>(data + off);
    off += 8;
    return r;
}

static QString parse_c_string(const char* data, size_t& off, size_t size) {
    QByteArray raw(data + off, static_cast<qsizetype>(size));
    off += size;
    int nul = raw.indexOf('\0');
    if (nul >= 0) {
        raw.truncate(nul);
    }
    return QString::fromUtf8(raw);
}

namespace {

constexpr uint64_t MIN_HEADER_SIZE = 488;
constexpr uint64_t V2_SCALAR_BYTES = 13ULL * 8ULL;
constexpr uint64_t V2_PATH_BYTES = 256ULL * 3ULL;
constexpr uint64_t V3_STRING_BYTES = 64ULL * 3ULL;
constexpr uint64_t V3_SCALAR_BYTES = 36ULL * 8ULL;
constexpr uint64_t SEGMENT_ENTRY_SIZE_V1 = 32;
constexpr uint64_t SEGMENT_ENTRY_SIZE_V2 = 48;
constexpr uint64_t MAX_SEGMENT_ENTRY_SIZE = 4096;
constexpr uint64_t V2_HEADER_SIZE = MIN_HEADER_SIZE + V2_SCALAR_BYTES + V2_PATH_BYTES;
constexpr uint64_t V3_HEADER_SIZE = V2_HEADER_SIZE + V3_STRING_BYTES + V3_SCALAR_BYTES;

struct FileRange {
    uint64_t start;
    uint64_t end;
    uint64_t segment_index;
};

auto parse_failure(CoreDumpParseStatus status, QString error, uint64_t offset = 0, uint32_t detected_version = 0) -> CoreDumpParseResult {
    return CoreDumpParseResult{
        .status = status, .dump = std::nullopt, .error = std::move(error), .error_offset = offset, .detected_version = detected_version};
}

auto checked_add(uint64_t left, uint64_t right, uint64_t* result) -> bool {
    if (right > std::numeric_limits<uint64_t>::max() - left) {
        return false;
    }
    *result = left + right;
    return true;
}

auto checked_mul(uint64_t left, uint64_t right, uint64_t* result) -> bool {
    if (left != 0 && right > std::numeric_limits<uint64_t>::max() / left) {
        return false;
    }
    *result = left * right;
    return true;
}

}  // namespace

QString core_dump_parse_status_name(CoreDumpParseStatus status) {
    switch (status) {
        case CoreDumpParseStatus::OK:
            return "ok";
        case CoreDumpParseStatus::UNSUPPORTED_VERSION:
            return "unsupported";
        case CoreDumpParseStatus::TRUNCATED:
            return "truncated";
        case CoreDumpParseStatus::CORRUPT:
            return "corrupt";
        case CoreDumpParseStatus::TOO_LARGE:
            return "too_large";
    }
    return "corrupt";
}

CoreDumpParseResult parse_core_dump_checked(const QByteArray& data, const CoreDumpParseLimits& limits) {
    const auto DATA_SIZE = static_cast<uint64_t>(data.size());
    const char* d = data.constData();
    const uint32_t DETECTED_VERSION = DATA_SIZE >= 12 ? read_le<uint32_t>(d + 8) : 0;
    const auto fail = [DETECTED_VERSION](CoreDumpParseStatus status, QString error, uint64_t offset = 0) {
        return parse_failure(status, std::move(error), offset, DETECTED_VERSION);
    };
    if (DATA_SIZE > limits.max_file_bytes) {
        return fail(CoreDumpParseStatus::TOO_LARGE, QString("coredump is %1 bytes; limit is %2").arg(DATA_SIZE).arg(limits.max_file_bytes));
    }

    // Minimum size: header (16) + 7x8 fields + 2x(7x8 + 15x8) frames/regs + 8x8 task metadata = 488 bytes
    if (DATA_SIZE < MIN_HEADER_SIZE) {
        return fail(CoreDumpParseStatus::TRUNCATED,
                    QString("coredump is %1 bytes; base header requires %2").arg(DATA_SIZE).arg(MIN_HEADER_SIZE), DATA_SIZE);
    }

    size_t off = 0;

    CoreDump dump;
    dump.raw = data;

    // Header preamble: magic(8) + version(4) + headerSize(4)
    dump.magic = read_le<uint64_t>(d + off);
    off += 8;
    dump.version = read_le<uint32_t>(d + off);
    off += 4;
    dump.header_size = read_le<uint32_t>(d + off);
    off += 4;

    if (dump.magic != COREDUMP_MAGIC) {
        return fail(CoreDumpParseStatus::CORRUPT,
                    QString("bad coredump magic %1; expected %2").arg(format_u64(dump.magic), format_u64(COREDUMP_MAGIC)));
    }
    if (dump.version < 1 || dump.version > 3) {
        return fail(CoreDumpParseStatus::UNSUPPORTED_VERSION,
                    QString("unsupported coredump version %1; supported versions are 1-3").arg(dump.version), 8);
    }
    if (dump.header_size < MIN_HEADER_SIZE) {
        return fail(CoreDumpParseStatus::CORRUPT,
                    QString("header_size %1 is smaller than the v1 base header %2").arg(dump.header_size).arg(MIN_HEADER_SIZE), 12);
    }
    if (dump.header_size > DATA_SIZE) {
        return fail(CoreDumpParseStatus::TRUNCATED,
                    QString("header_size %1 extends beyond %2-byte file").arg(dump.header_size).arg(DATA_SIZE), DATA_SIZE);
    }
    const uint64_t VERSION_HEADER_SIZE = dump.version == 1 ? MIN_HEADER_SIZE : dump.version == 2 ? V2_HEADER_SIZE : V3_HEADER_SIZE;
    if (dump.header_size < VERSION_HEADER_SIZE) {
        return fail(CoreDumpParseStatus::CORRUPT,
                    QString("v%1 header_size %2 is smaller than the required %3 bytes")
                        .arg(dump.version)
                        .arg(dump.header_size)
                        .arg(VERSION_HEADER_SIZE),
                    12);
    }

    // 7 x uint64: timestamp, pid, cpu, int_num, err_code, cr2, cr3
    dump.timestamp = read_le<uint64_t>(d + off);
    off += 8;
    dump.pid = read_le<uint64_t>(d + off);
    off += 8;
    dump.cpu = read_le<uint64_t>(d + off);
    off += 8;
    dump.int_num = read_le<uint64_t>(d + off);
    off += 8;
    dump.err_code = read_le<uint64_t>(d + off);
    off += 8;
    dump.cr2 = read_le<uint64_t>(d + off);
    off += 8;
    dump.cr3 = read_le<uint64_t>(d + off);
    off += 8;

    // Trap state
    dump.trap_frame = parse_interrupt_frame(d, off);
    dump.trap_regs = parse_gp_regs(d, off);

    // Saved state
    dump.saved_frame = parse_interrupt_frame(d, off);
    dump.saved_regs = parse_gp_regs(d, off);

    // Task metadata: 8 x uint64
    dump.task_entry = read_le<uint64_t>(d + off);
    off += 8;
    dump.task_pagemap = read_le<uint64_t>(d + off);
    off += 8;
    dump.elf_header_addr = read_le<uint64_t>(d + off);
    off += 8;
    dump.program_header_addr = read_le<uint64_t>(d + off);
    off += 8;
    dump.segment_count = read_le<uint64_t>(d + off);
    off += 8;
    dump.segment_table_offset = read_le<uint64_t>(d + off);
    off += 8;
    dump.elf_size = read_le<uint64_t>(d + off);
    off += 8;
    dump.elf_offset = read_le<uint64_t>(d + off);
    off += 8;

    if (dump.version >= 2 && dump.header_size >= off + V2_SCALAR_BYTES) {
        dump.segment_entry_size = read_le<uint64_t>(d + off);
        off += 8;
        dump.page_size = read_le<uint64_t>(d + off);
        off += 8;
        dump.snapshot_flags = read_le<uint64_t>(d + off);
        off += 8;
        dump.interp_base = read_le<uint64_t>(d + off);
        off += 8;
        dump.program_header_count = read_le<uint64_t>(d + off);
        off += 8;
        dump.program_header_ent_size = read_le<uint64_t>(d + off);
        off += 8;
        dump.thread_fs_base = read_le<uint64_t>(d + off);
        off += 8;
        dump.thread_gs_base = read_le<uint64_t>(d + off);
        off += 8;
        dump.thread_stack_base = read_le<uint64_t>(d + off);
        off += 8;
        dump.thread_stack_size = read_le<uint64_t>(d + off);
        off += 8;
        dump.thread_tls_base = read_le<uint64_t>(d + off);
        off += 8;
        dump.thread_tls_size = read_le<uint64_t>(d + off);
        off += 8;
        dump.thread_safe_stack = read_le<uint64_t>(d + off);
        off += 8;
        if (dump.header_size >= off + V2_PATH_BYTES) {
            dump.exe_path = parse_c_string(d, off, 256);
            dump.cwd = parse_c_string(d, off, 256);
            dump.root = parse_c_string(d, off, 256);
        }
        if (dump.version >= 3 && dump.header_size >= off + V3_STRING_BYTES + V3_SCALAR_BYTES) {
            dump.wait_channel = parse_c_string(d, off, 64);
            dump.wki_target_hostname = parse_c_string(d, off, 64);
            dump.wki_submitter_hostname = parse_c_string(d, off, 64);
            dump.task_ptr = read_le<uint64_t>(d + off);
            off += 8;
            dump.thread_ptr = read_le<uint64_t>(d + off);
            off += 8;
            dump.parent_pid = read_le<uint64_t>(d + off);
            off += 8;
            dump.owner_pid = read_le<uint64_t>(d + off);
            off += 8;
            dump.wki_remote_pid = read_le<uint64_t>(d + off);
            off += 8;
            dump.session_id = read_le<uint64_t>(d + off);
            off += 8;
            dump.pgid = read_le<uint64_t>(d + off);
            off += 8;
            dump.task_type = read_le<uint64_t>(d + off);
            off += 8;
            dump.task_state = read_le<uint64_t>(d + off);
            off += 8;
            dump.sched_queue = read_le<uint64_t>(d + off);
            off += 8;
            dump.current_cpu = read_le<uint64_t>(d + off);
            off += 8;
            dump.domain_id = read_le<uint64_t>(d + off);
            off += 8;
            dump.domain_mask = read_le<uint64_t>(d + off);
            off += 8;
            dump.wki_target_flags = read_le<uint64_t>(d + off);
            off += 8;
            dump.wki_proxy_task_id = read_le<uint64_t>(d + off);
            off += 8;
            dump.elf_buffer_addr = read_le<uint64_t>(d + off);
            off += 8;
            dump.captured_elf_buffer_size = read_le<uint64_t>(d + off);
            off += 8;
            dump.elf_buffer_shared = read_le<uint64_t>(d + off) != 0;
            off += 8;
            dump.start_time_us = read_le<uint64_t>(d + off);
            off += 8;
            dump.user_time_us = read_le<uint64_t>(d + off);
            off += 8;
            dump.system_time_us = read_le<uint64_t>(d + off);
            off += 8;
            dump.vruntime = read_le<uint64_t>(d + off);
            off += 8;
            dump.vdeadline = read_le<uint64_t>(d + off);
            off += 8;
            dump.wake_at_us = read_le<uint64_t>(d + off);
            off += 8;
            dump.wait_channel_addr = read_le<uint64_t>(d + off);
            off += 8;
            dump.waiting_for_pid = read_le<uint64_t>(d + off);
            off += 8;
            dump.wait_status_user_addr = read_le<uint64_t>(d + off);
            off += 8;
            dump.wait_rusage_user_addr = read_le<uint64_t>(d + off);
            off += 8;
            dump.sig_pending = read_le<uint64_t>(d + off);
            off += 8;
            dump.sig_mask = read_le<uint64_t>(d + off);
            off += 8;
            dump.ptrace_tracer_pid = read_le<uint64_t>(d + off);
            off += 8;
            dump.uid = read_le<uint64_t>(d + off);
            off += 8;
            dump.gid = read_le<uint64_t>(d + off);
            off += 8;
            dump.euid = read_le<uint64_t>(d + off);
            off += 8;
            dump.egid = read_le<uint64_t>(d + off);
            off += 8;
            dump.task_flags = read_le<uint64_t>(d + off);
        }
    }
    if (dump.segment_entry_size < SEGMENT_ENTRY_SIZE_V1 || dump.segment_entry_size > MAX_SEGMENT_ENTRY_SIZE) {
        return fail(CoreDumpParseStatus::CORRUPT,
                    QString("segment entry size %1 is outside supported range %2-%3")
                        .arg(dump.segment_entry_size)
                        .arg(SEGMENT_ENTRY_SIZE_V1)
                        .arg(MAX_SEGMENT_ENTRY_SIZE),
                    dump.segment_table_offset);
    }
    if (dump.segment_count > limits.max_segments) {
        return fail(CoreDumpParseStatus::TOO_LARGE,
                    QString("segment count %1 exceeds limit %2").arg(dump.segment_count).arg(limits.max_segments),
                    dump.segment_table_offset);
    }
    if (dump.segment_table_offset < dump.header_size) {
        return fail(CoreDumpParseStatus::CORRUPT,
                    QString("segment table offset %1 overlaps %2-byte header").arg(dump.segment_table_offset).arg(dump.header_size),
                    dump.segment_table_offset);
    }
    uint64_t table_bytes = 0;
    uint64_t table_end = 0;
    if (!checked_mul(dump.segment_count, dump.segment_entry_size, &table_bytes) ||
        !checked_add(dump.segment_table_offset, table_bytes, &table_end)) {
        return fail(CoreDumpParseStatus::CORRUPT, "segment table range overflows uint64", dump.segment_table_offset);
    }
    if (table_end > DATA_SIZE) {
        return fail(CoreDumpParseStatus::TRUNCATED, QString("segment table ends at %1 beyond %2-byte file").arg(table_end).arg(DATA_SIZE),
                    DATA_SIZE);
    }

    dump.segments.reserve(static_cast<size_t>(dump.segment_count));
    std::vector<FileRange> payload_ranges;
    payload_ranges.reserve(static_cast<size_t>(dump.segment_count));
    for (uint64_t i = 0; i < dump.segment_count; ++i) {
        const uint64_t SOFF_U64 = dump.segment_table_offset + (i * dump.segment_entry_size);
        const auto soff = static_cast<size_t>(SOFF_U64);
        CoreDumpSegment seg;
        seg.vaddr = read_le<uint64_t>(d + soff);
        seg.size = read_le<uint64_t>(d + soff + 8);
        seg.file_offset = read_le<uint64_t>(d + soff + 16);
        seg.type = read_le<uint32_t>(d + soff + 24);
        seg.present = read_le<uint32_t>(d + soff + 28);
        if (dump.segment_entry_size >= SEGMENT_ENTRY_SIZE_V2) {
            seg.pte_flags = read_le<uint64_t>(d + soff + 32);
            seg.phys_addr = read_le<uint64_t>(d + soff + 40);
        }
        if (seg.present > 1) {
            return fail(CoreDumpParseStatus::CORRUPT, QString("segment %1 has invalid present value %2").arg(i).arg(seg.present),
                        SOFF_U64 + 28);
        }
        uint64_t vaddr_end = 0;
        if (!checked_add(seg.vaddr, seg.size, &vaddr_end)) {
            return fail(CoreDumpParseStatus::CORRUPT, QString("segment %1 virtual range overflows").arg(i), SOFF_U64);
        }
        if (seg.size > limits.max_segment_bytes) {
            return fail(CoreDumpParseStatus::TOO_LARGE,
                        QString("segment %1 size %2 exceeds limit %3").arg(i).arg(seg.size).arg(limits.max_segment_bytes), SOFF_U64 + 8);
        }
        if (seg.is_present()) {
            if (seg.size == 0) {
                return fail(CoreDumpParseStatus::CORRUPT, QString("segment %1 is present but has zero size").arg(i), SOFF_U64 + 8);
            }
            uint64_t payload_end = 0;
            if (!checked_add(seg.file_offset, seg.size, &payload_end)) {
                return fail(CoreDumpParseStatus::CORRUPT, QString("segment %1 file range overflows").arg(i), SOFF_U64 + 16);
            }
            if (seg.file_offset < table_end) {
                return fail(CoreDumpParseStatus::CORRUPT, QString("segment %1 payload overlaps header or segment table").arg(i),
                            SOFF_U64 + 16);
            }
            if (payload_end > DATA_SIZE) {
                return fail(CoreDumpParseStatus::TRUNCATED,
                            QString("segment %1 payload ends at %2 beyond %3-byte file").arg(i).arg(payload_end).arg(DATA_SIZE), DATA_SIZE);
            }
            payload_ranges.push_back(FileRange{.start = seg.file_offset, .end = payload_end, .segment_index = i});
        }
        dump.segments.push_back(seg);
    }

    std::ranges::sort(payload_ranges, [](const FileRange& left, const FileRange& right) {
        return left.start < right.start || (left.start == right.start && left.end < right.end);
    });
    for (size_t i = 1; i < payload_ranges.size(); ++i) {
        if (payload_ranges[i].start < payload_ranges[i - 1].end) {
            return fail(CoreDumpParseStatus::CORRUPT,
                        QString("segment %1 payload overlaps segment %2 payload")
                            .arg(payload_ranges[i].segment_index)
                            .arg(payload_ranges[i - 1].segment_index),
                        payload_ranges[i].start);
        }
    }

    if (dump.elf_size > limits.max_embedded_elf_bytes) {
        return fail(CoreDumpParseStatus::TOO_LARGE,
                    QString("embedded ELF size %1 exceeds limit %2").arg(dump.elf_size).arg(limits.max_embedded_elf_bytes),
                    dump.elf_offset);
    }
    if (dump.elf_size > 0) {
        uint64_t elf_end = 0;
        if (dump.elf_offset == 0 || !checked_add(dump.elf_offset, dump.elf_size, &elf_end)) {
            return fail(CoreDumpParseStatus::CORRUPT, "embedded ELF range is invalid", dump.elf_offset);
        }
        if (dump.elf_offset < table_end) {
            return fail(CoreDumpParseStatus::CORRUPT, "embedded ELF overlaps header or segment table", dump.elf_offset);
        }
        if (elf_end > DATA_SIZE) {
            return fail(CoreDumpParseStatus::TRUNCATED, QString("embedded ELF ends at %1 beyond %2-byte file").arg(elf_end).arg(DATA_SIZE),
                        DATA_SIZE);
        }
        for (const auto& payload : payload_ranges) {
            if (dump.elf_offset < payload.end && elf_end > payload.start) {
                return fail(CoreDumpParseStatus::CORRUPT, QString("embedded ELF overlaps segment %1 payload").arg(payload.segment_index),
                            dump.elf_offset);
            }
        }
    }

    return CoreDumpParseResult{
        .status = CoreDumpParseStatus::OK, .dump = std::move(dump), .error = {}, .error_offset = 0, .detected_version = DETECTED_VERSION};
}

QByteArray CoreDump::embedded_elf() const {
    const auto RAW_SIZE = static_cast<uint64_t>(raw.size());
    uint64_t elf_end = 0;
    if (elf_size > 0 && elf_offset > 0 && checked_add(elf_offset, elf_size, &elf_end) && elf_end <= RAW_SIZE &&
        elf_offset <= static_cast<uint64_t>(std::numeric_limits<qsizetype>::max()) &&
        elf_size <= static_cast<uint64_t>(std::numeric_limits<qsizetype>::max())) {
        return raw.mid(static_cast<qsizetype>(elf_offset), static_cast<qsizetype>(elf_size));
    }
    return {};
}

CoreDumpParseResult parse_core_dump_checked(const QString& file_path, const CoreDumpParseLimits& limits) {
    QFile file(file_path);
    if (!file.open(QIODevice::ReadOnly)) {
        return parse_failure(CoreDumpParseStatus::CORRUPT, QString("cannot open coredump file: %1").arg(file.errorString()));
    }
    const qint64 SIZE = file.size();
    if (SIZE < 0) {
        return parse_failure(CoreDumpParseStatus::CORRUPT, "cannot determine coredump file size");
    }
    if (static_cast<uint64_t>(SIZE) > limits.max_file_bytes) {
        return parse_failure(CoreDumpParseStatus::TOO_LARGE,
                             QString("coredump is %1 bytes; limit is %2").arg(SIZE).arg(limits.max_file_bytes));
    }
    if (SIZE >= std::numeric_limits<qsizetype>::max()) {
        return parse_failure(CoreDumpParseStatus::TOO_LARGE, "coredump exceeds the host byte-array read limit");
    }
    QByteArray data = file.read(SIZE + 1);
    file.close();

    if (data.size() != SIZE) {
        return parse_failure(CoreDumpParseStatus::TRUNCATED, QString("expected %1 coredump bytes but read %2").arg(SIZE).arg(data.size()),
                             data.size());
    }
    CoreDumpParseResult result = parse_core_dump_checked(data, limits);
    if (result.ok()) {
        result.dump->source_filename = QFileInfo(file_path).fileName();
    }
    return result;
}

std::optional<CoreDump> parse_core_dump(const QByteArray& data) {
    CoreDumpParseResult result = parse_core_dump_checked(data);
    if (!result.ok()) {
        qWarning() << "Coredump parse failed (" << core_dump_parse_status_name(result.status) << "):" << result.error;
        return std::nullopt;
    }
    return std::move(result.dump);
}

std::unique_ptr<CoreDump> parse_core_dump(const QString& file_path) {
    CoreDumpParseResult result = parse_core_dump_checked(file_path);
    if (!result.ok()) {
        qWarning() << "Coredump parse failed for" << file_path << "(" << core_dump_parse_status_name(result.status) << "):" << result.error;
        return nullptr;
    }
    return std::make_unique<CoreDump>(std::move(*result.dump));
}

}  // namespace wosdbg
