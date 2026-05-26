#include "coredump_parser.h"

#include <qlogging.h>
#include <qtypes.h>

#include <QDebug>
#include <QFile>
#include <QFileInfo>
#include <algorithm>
#include <cstdint>
#include <cstring>
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

std::optional<CoreDump> parse_core_dump(const QByteArray& data) {
    // Minimum size: header (16) + 7x8 fields + 2x(7x8 + 15x8) frames/regs + 8x8 task metadata = 488 bytes
    static constexpr size_t MIN_HEADER_SIZE = 488;
    if (std::cmp_less(data.size(), MIN_HEADER_SIZE)) {
        qWarning() << "Coredump too small:" << data.size() << "bytes (minimum" << MIN_HEADER_SIZE << ")";
        return std::nullopt;
    }

    const char* d = data.constData();
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
        qWarning() << "Bad coredump magic:" << format_u64(dump.magic) << "(expected" << format_u64(COREDUMP_MAGIC) << ")";
        return std::nullopt;
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

    static constexpr size_t SEGMENT_ENTRY_SIZE_V1 = 32;
    static constexpr size_t SEGMENT_ENTRY_SIZE_V2 = 48;
    constexpr size_t V2_U64_BYTES = size_t{13} * size_t{8};
    if (dump.version >= 2 && dump.header_size >= off + V2_U64_BYTES) {
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
        constexpr size_t V2_PATH_BYTES = size_t{256} * size_t{3};
        if (dump.header_size >= off + V2_PATH_BYTES) {
            dump.exe_path = parse_c_string(d, off, 256);
            dump.cwd = parse_c_string(d, off, 256);
            dump.root = parse_c_string(d, off, 256);
        }
        constexpr size_t V3_STRING_BYTES = size_t{64} * size_t{3};
        constexpr size_t V3_U64_BYTES = size_t{36} * size_t{8};
        if (dump.version >= 3 && dump.header_size >= off + V3_STRING_BYTES + V3_U64_BYTES) {
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
    dump.segment_entry_size = std::max(dump.segment_entry_size, SEGMENT_ENTRY_SIZE_V1);

    dump.segments.reserve(static_cast<size_t>(dump.segment_count));
    for (uint64_t i = 0; i < dump.segment_count; ++i) {
        size_t soff = dump.segment_table_offset + (static_cast<size_t>(i) * static_cast<size_t>(dump.segment_entry_size));
        if (soff + SEGMENT_ENTRY_SIZE_V1 > static_cast<size_t>(data.size())) {
            qWarning() << "Segment table extends beyond file at segment" << i;
            break;
        }
        CoreDumpSegment seg;
        seg.vaddr = read_le<uint64_t>(d + soff);
        seg.size = read_le<uint64_t>(d + soff + 8);
        seg.file_offset = read_le<uint64_t>(d + soff + 16);
        seg.type = read_le<uint32_t>(d + soff + 24);
        seg.present = read_le<uint32_t>(d + soff + 28);
        if (dump.segment_entry_size >= SEGMENT_ENTRY_SIZE_V2 && soff + SEGMENT_ENTRY_SIZE_V2 <= static_cast<size_t>(data.size())) {
            seg.pte_flags = read_le<uint64_t>(d + soff + 32);
            seg.phys_addr = read_le<uint64_t>(d + soff + 40);
        }
        dump.segments.push_back(seg);
    }

    return dump;
}

QByteArray CoreDump::embedded_elf() const {
    if (elf_size > 0 && elf_offset > 0 && static_cast<int64_t>(elf_offset + elf_size) <= raw.size()) {
        return raw.mid(static_cast<qsizetype>(elf_offset), static_cast<qsizetype>(elf_size));
    }
    return {};
}

std::unique_ptr<CoreDump> parse_core_dump(const QString& file_path) {
    QFile file(file_path);
    if (!file.open(QIODevice::ReadOnly)) {
        qWarning() << "Cannot open coredump file:" << file_path;
        return nullptr;
    }
    QByteArray data = file.readAll();
    file.close();

    auto result = parse_core_dump(data);
    if (!result) {
        return nullptr;
    }

    auto dump = std::make_unique<CoreDump>(std::move(*result));
    dump->source_filename = QFileInfo(file_path).fileName();
    return dump;
}

}  // namespace wosdbg
