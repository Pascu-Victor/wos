#include "debug_analysis_service.h"

#include <capstone/capstone.h>
#include <qcborcommon.h>
#include <qcontainerfwd.h>
#include <qhash.h>
#include <qhashfunctions.h>
#include <qnamespace.h>
#include <qobject.h>
#include <qtypes.h>

#include <QCoreApplication>
#include <QCryptographicHash>
#include <QDir>
#include <QDirIterator>
#include <QEventLoop>
#include <QFile>
#include <QFileInfo>
#include <QJsonDocument>
#include <QProcess>
#include <QRegularExpression>
#include <QSet>
#include <QTextStream>
#include <QTimer>
#include <QUrl>
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "config.h"
#include "coredump_memory.h"
#include "coredump_parser.h"
#include "elf_symbol_resolver.h"
#include "log_entry.h"
#include "log_processor.h"
#include "x86.h"

namespace {

constexpr int K_DEFAULT_MAX_STRING_LENGTH = 160;
constexpr qsizetype K_MAX_CAPTURED_PROCESS_OUTPUT = 256ULL * 1024;
constexpr uint64_t K_MAX_MEMORY_SEARCH_BYTES = 64ULL * 1024ULL * 1024ULL;
constexpr uint64_t K_MAX_DISASSEMBLY_FUNCTION_BYTES = 128ULL * 1024ULL;
constexpr uint64_t K_DISASSEMBLY_BYTES_AFTER_TARGET = 512;
constexpr uint64_t K_USER_RED_ZONE_BYTES = 128;
constexpr uint64_t K_PAGE_SIZE = 4096;
constexpr uint64_t K_PTE_PRESENT = 1ULL << 0;
constexpr uint64_t K_PTE_WRITE = 1ULL << 1;
constexpr uint64_t K_PTE_USER = 1ULL << 2;
constexpr uint64_t K_PTE_COW = 1ULL << 9;
constexpr uint64_t K_PTE_SHARED = 1ULL << 10;
constexpr uint64_t K_PTE_NX = 1ULL << 63;
constexpr uint64_t K_USER_SPACE_START = 0x400000ULL;
constexpr uint64_t K_USER_SPACE_END = 0x0000800000000000ULL;

auto format_hex(uint64_t value) -> QString { return wosdbg::format_u64(value); }

template <typename T>
auto sorted_hash_keys(const QHash<QString, T>& values) -> QStringList {
    QStringList keys = values.keys();
    std::ranges::sort(keys);
    return keys;
}

auto entry_type_name(EntryType type) -> QString {
    switch (type) {
        case EntryType::INSTRUCTION:
            return "instruction";
        case EntryType::INTERRUPT:
            return "interrupt";
        case EntryType::REGISTER:
            return "register";
        case EntryType::BLOCK:
            return "block";
        case EntryType::SEPARATOR:
            return "separator";
        case EntryType::OTHER:
            return "other";
    }
    return "other";
}

auto canonical_path_or_absolute(const QString& path) -> QString {
    QFileInfo const INFO(path);
    QString canonical = INFO.canonicalFilePath();
    if (!canonical.isEmpty()) {
        return canonical;
    }
    return INFO.absoluteFilePath();
}

auto clean_source_path(const QString& path) -> QString {
    if (path.isEmpty() || path == "??") {
        return {};
    }
    QFileInfo const INFO(path);
    QString canonical = INFO.canonicalFilePath();
    if (!canonical.isEmpty()) {
        return canonical;
    }
    QString clean = QDir::cleanPath(path);
    if (INFO.isRelative() && !clean.contains('/')) {
        static QHash<QString, QString> basename_cache;
        if (basename_cache.contains(clean)) {
            return basename_cache.value(clean);
        }
        QStringList matches;
        const QStringList ROOTS = {QDir::current().absoluteFilePath("toolchain/src"), QDir::current().absoluteFilePath("modules"),
                                   QDir::current().absoluteFilePath("tools")};
        for (const auto& root : ROOTS) {
            if (!QFileInfo::exists(root)) {
                continue;
            }
            QDirIterator it(root, QStringList{clean}, QDir::Files, QDirIterator::Subdirectories);
            while (it.hasNext()) {
                matches << canonical_path_or_absolute(it.next());
                if (matches.size() > 1) {
                    break;
                }
            }
            if (matches.size() > 1) {
                break;
            }
        }
        basename_cache.insert(clean, matches.size() == 1 ? matches.first() : clean);
        return basename_cache.value(clean);
    }
    return clean;
}

auto starts_with_path_root(const QString& path, const QString& root) -> bool {
    QString const CLEAN_PATH = QDir::cleanPath(path);
    QString clean_root = QDir::cleanPath(root);
    if (CLEAN_PATH == clean_root) {
        return true;
    }
    if (!clean_root.endsWith('/')) {
        clean_root += '/';
    }
    return CLEAN_PATH.startsWith(clean_root);
}

auto is_canonical_x86_address(uint64_t value) -> bool {
    const bool HIGH = (value & (1ULL << 47)) != 0;
    const uint64_t TOP = value >> 48;
    return HIGH ? TOP == 0xffffULL : TOP == 0;
}

auto read_u64_le(const char* data) -> uint64_t {
    uint64_t value = 0;
    std::memcpy(&value, data, sizeof(value));
    return value;
}

auto page_align_down(uint64_t value) -> uint64_t { return value & ~(K_PAGE_SIZE - 1); }

auto read_file_bytes_quiet(const QString& path) -> QByteArray {
    QFile file(path);
    if (!file.open(QIODevice::ReadOnly)) {
        return {};
    }
    return file.readAll();
}

auto pte_flags_to_json(uint64_t flags) -> QJsonObject {
    return QJsonObject{{"raw", format_hex(flags)},
                       {"present", (flags & K_PTE_PRESENT) != 0},
                       {"rw", (flags & K_PTE_WRITE) != 0},
                       {"user", (flags & K_PTE_USER) != 0},
                       {"nx", (flags & K_PTE_NX) != 0},
                       {"cow", (flags & K_PTE_COW) != 0},
                       {"shared", (flags & K_PTE_SHARED) != 0},
                       {"ref", QJsonValue()}};
}

auto page_fault_error_to_json(uint64_t error) -> QJsonObject {
    return QJsonObject{{"raw", format_hex(error)},    {"present", (error & 0x1U) != 0},     {"write", (error & 0x2U) != 0},
                       {"user", (error & 0x4U) != 0}, {"reservedBit", (error & 0x8U) != 0}, {"instructionFetch", (error & 0x10U) != 0}};
}

auto read_u64_at(const wosdbg::CoreDump& dump, uint64_t address) -> std::optional<uint64_t> {
    QByteArray const BYTES = wosdbg::read_va_bytes(dump, address, sizeof(uint64_t));
    if (BYTES.size() != static_cast<int>(sizeof(uint64_t))) {
        return std::nullopt;
    }
    return read_u64_le(BYTES.constData());
}

auto is_likely_user_code_address(uint64_t value, const wosdbg::CoreDump& dump) -> bool {
    if (value < K_USER_SPACE_START || value >= K_USER_SPACE_END) {
        return false;
    }
    const auto* seg = wosdbg::find_segment_for_va(dump, value);
    if (seg == nullptr) {
        return false;
    }
    return (seg->pte_flags & K_PTE_NX) == 0;
}

auto capstone_reg_to_name(x86_reg reg) -> QString {
    switch (reg) {
        case X86_REG_RAX:
        case X86_REG_EAX:
        case X86_REG_AX:
        case X86_REG_AH:
        case X86_REG_AL:
            return "rax";
        case X86_REG_RBX:
        case X86_REG_EBX:
        case X86_REG_BX:
        case X86_REG_BH:
        case X86_REG_BL:
            return "rbx";
        case X86_REG_RCX:
        case X86_REG_ECX:
        case X86_REG_CX:
        case X86_REG_CH:
        case X86_REG_CL:
            return "rcx";
        case X86_REG_RDX:
        case X86_REG_EDX:
        case X86_REG_DX:
        case X86_REG_DH:
        case X86_REG_DL:
            return "rdx";
        case X86_REG_RSI:
        case X86_REG_ESI:
        case X86_REG_SI:
        case X86_REG_SIL:
            return "rsi";
        case X86_REG_RDI:
        case X86_REG_EDI:
        case X86_REG_DI:
        case X86_REG_DIL:
            return "rdi";
        case X86_REG_RBP:
        case X86_REG_EBP:
        case X86_REG_BP:
        case X86_REG_BPL:
            return "rbp";
        case X86_REG_RSP:
        case X86_REG_ESP:
        case X86_REG_SP:
        case X86_REG_SPL:
            return "rsp";
        case X86_REG_R8:
        case X86_REG_R8D:
        case X86_REG_R8W:
        case X86_REG_R8B:
            return "r8";
        case X86_REG_R9:
        case X86_REG_R9D:
        case X86_REG_R9W:
        case X86_REG_R9B:
            return "r9";
        case X86_REG_R10:
        case X86_REG_R10D:
        case X86_REG_R10W:
        case X86_REG_R10B:
            return "r10";
        case X86_REG_R11:
        case X86_REG_R11D:
        case X86_REG_R11W:
        case X86_REG_R11B:
            return "r11";
        case X86_REG_R12:
        case X86_REG_R12D:
        case X86_REG_R12W:
        case X86_REG_R12B:
            return "r12";
        case X86_REG_R13:
        case X86_REG_R13D:
        case X86_REG_R13W:
        case X86_REG_R13B:
            return "r13";
        case X86_REG_R14:
        case X86_REG_R14D:
        case X86_REG_R14W:
        case X86_REG_R14B:
            return "r14";
        case X86_REG_R15:
        case X86_REG_R15D:
        case X86_REG_R15W:
        case X86_REG_R15B:
            return "r15";
        case X86_REG_RIP:
            return "rip";
        default:
            return {};
    }
}

auto capstone_reg_value(x86_reg reg, const QHash<QString, uint64_t>& regs) -> std::optional<uint64_t> {
    QString const NAME = capstone_reg_to_name(reg).toLower();
    if (NAME.isEmpty()) {
        return std::nullopt;
    }
    if (!regs.contains(NAME)) {
        return std::nullopt;
    }
    return regs.value(NAME);
}

auto bytes_from_hex(const QString& hex_text) -> QByteArray {
    QString hex = hex_text;
    hex.remove(QRegularExpression(R"([^0-9a-fA-F])"));
    if (hex.size() % 2 != 0) {
        hex.prepend('0');
    }
    QByteArray bytes;
    bytes.reserve(hex.size() / 2);
    for (int i = 0; i + 1 < hex.size(); i += 2) {
        bool ok = false;
        auto byte = static_cast<char>(hex.mid(i, 2).toUInt(&ok, 16));
        if (!ok) {
            return {};
        }
        bytes.append(byte);
    }
    return bytes;
}

auto first_existing_candidate(const QStringList& candidates) -> QString {
    for (const auto& candidate : candidates) {
        if (candidate.isEmpty()) {
            continue;
        }
        QFileInfo const INFO(candidate);
        if (INFO.exists() && INFO.isFile()) {
            return canonical_path_or_absolute(candidate);
        }
    }
    return {};
}

auto build_id_debug_candidates(const QString& build_id) -> QStringList {
    QString const ID = build_id.trimmed().toLower();
    if (ID.size() < 3) {
        return {};
    }
    const QString PREFIX = ID.left(2);
    const QString SUFFIX = ID.mid(2);
    const QString RELATIVE_DEBUG = QString(".build-id/%1/%2.debug").arg(PREFIX, SUFFIX);
    const QString RELATIVE_ELF = QString(".build-id/%1/%2").arg(PREFIX, SUFFIX);
    const QStringList ROOTS = {QDir::currentPath(), QDir::current().absoluteFilePath("build"),
                               QDir::current().absoluteFilePath("toolchain/sysroot/usr/lib/debug"),
                               QDir::current().absoluteFilePath("toolchain/sysroot/lib/debug"), QDir::homePath()};
    QStringList candidates;
    for (const auto& root : ROOTS) {
        candidates << QDir(root).absoluteFilePath(RELATIVE_DEBUG);
        candidates << QDir(root).absoluteFilePath(RELATIVE_ELF);
        candidates << QDir(root).absoluteFilePath(QString("usr/lib/debug/%1").arg(RELATIVE_DEBUG));
    }
    return candidates;
}

auto default_elf_candidates(const QString& exe_path = QString()) -> QStringList {
    QStringList candidates;
    const QString EXE_BASE = QFileInfo(exe_path).fileName();
    if (!EXE_BASE.isEmpty()) {
        candidates << QDir::current().absoluteFilePath(QString("build/modules/%1/%1").arg(EXE_BASE));
        candidates << QDir::current().absoluteFilePath(QString("build/modules/%1").arg(EXE_BASE));
        candidates << QDir::current().absoluteFilePath(QString("toolchain/busybox-install/bin/%1").arg(EXE_BASE));
        candidates << QDir::current().absoluteFilePath(QString("toolchain/sysroot/bin/%1").arg(EXE_BASE));
        candidates << QDir::current().absoluteFilePath(QString("configs/rootfs/bin/%1").arg(EXE_BASE));
    }
    candidates << QDir::current().absoluteFilePath("toolchain/sysroot/lib/ld.so")
               << QDir::current().absoluteFilePath("toolchain/sysroot/lib/libc.so")
               << QDir::current().absoluteFilePath("toolchain/sysroot/bin/busybox")
               << QDir::current().absoluteFilePath("toolchain/busybox-install/bin/busybox")
               << QDir::current().absoluteFilePath("toolchain/busybox-build/busybox")
               << QDir::current().absoluteFilePath("toolchain/mlibc-build/ld.so")
               << QDir::current().absoluteFilePath("toolchain/mlibc-build/libc.so")
               << QDir::current().absoluteFilePath("build/modules/kern/wos");
    return candidates;
}

auto first_candidate_with_build_id(const QStringList& candidates, const QString& build_id) -> QString {
    QString const TARGET = build_id.trimmed().toLower();
    if (TARGET.isEmpty()) {
        return {};
    }
    QStringList seen;
    for (const auto& candidate : candidates) {
        if (candidate.isEmpty() || !QFileInfo::exists(candidate)) {
            continue;
        }
        QString const PATH = canonical_path_or_absolute(candidate);
        if (seen.contains(PATH)) {
            continue;
        }
        seen << PATH;
        if (wosdbg::elf_build_id_from_file(PATH).toLower() == TARGET) {
            return PATH;
        }
    }
    return {};
}

auto hex_preview(const QByteArray& bytes, qsizetype max_bytes = 16) -> QString {
    return QString::fromLatin1(bytes.left(max_bytes).toHex(' '));
}

auto sha256_hex(const QByteArray& bytes) -> QString {
    return QString::fromLatin1(QCryptographicHash::hash(bytes, QCryptographicHash::Sha256).toHex());
}

struct ElfVaMapping {
    int segment_index = -1;
    uint64_t elf_vaddr = 0;
    uint64_t file_offset = 0;
    uint64_t segment_vaddr = 0;
    uint64_t segment_file_offset = 0;
    uint64_t segment_filesz = 0;
    uint64_t segment_memsz = 0;
    uint32_t segment_flags = 0;
    bool file_backed = false;
};

auto elf_va_mapping(const wosdbg::ElfImageInfo& info, uint64_t runtime_va, uint64_t load_base) -> std::optional<ElfVaMapping> {
    if (!info.valid || runtime_va < load_base) {
        return std::nullopt;
    }
    const uint64_t ELF_VADDR = runtime_va - load_base;
    for (int i = 0; i < static_cast<int>(info.load_segments.size()); ++i) {
        const auto& load = info.load_segments[static_cast<size_t>(i)];
        if (ELF_VADDR < load.vaddr || ELF_VADDR >= load.vaddr + load.memsz) {
            continue;
        }
        const uint64_t SEG_OFF = ELF_VADDR - load.vaddr;
        return ElfVaMapping{.segment_index = i,
                            .elf_vaddr = ELF_VADDR,
                            .file_offset = load.offset + SEG_OFF,
                            .segment_vaddr = load.vaddr,
                            .segment_file_offset = load.offset,
                            .segment_filesz = load.filesz,
                            .segment_memsz = load.memsz,
                            .segment_flags = load.flags,
                            .file_backed = SEG_OFF < load.filesz};
    }
    return std::nullopt;
}

auto elf_va_mapping_json(const ElfVaMapping& mapping) -> QJsonObject {
    return QJsonObject{{"segmentIndex", mapping.segment_index},
                       {"elfVaddr", format_hex(mapping.elf_vaddr)},
                       {"fileOffset", format_hex(mapping.file_offset)},
                       {"segmentVaddr", format_hex(mapping.segment_vaddr)},
                       {"segmentFileOffset", format_hex(mapping.segment_file_offset)},
                       {"segmentFileSize", format_hex(mapping.segment_filesz)},
                       {"segmentMemSize", format_hex(mapping.segment_memsz)},
                       {"segmentFlags", format_hex(mapping.segment_flags)},
                       {"fileBacked", mapping.file_backed}};
}

auto bytes_at_file_offset(const QByteArray& bytes, uint64_t offset, int len) -> QByteArray {
    if (offset >= static_cast<uint64_t>(bytes.size()) || len <= 0) {
        return {};
    }
    const int AVAILABLE = static_cast<int>(std::min<uint64_t>(static_cast<uint64_t>(len), static_cast<uint64_t>(bytes.size()) - offset));
    return bytes.mid(static_cast<qsizetype>(offset), AVAILABLE);
}

auto first_mismatch_offset(const QByteArray& a, const QByteArray& b) -> std::optional<uint64_t> {
    const qsizetype COMMON = std::min(a.size(), b.size());
    for (qsizetype i = 0; i < COMMON; ++i) {
        if (a[i] != b[i]) {
            return static_cast<uint64_t>(i);
        }
    }
    if (a.size() != b.size()) {
        return static_cast<uint64_t>(COMMON);
    }
    return std::nullopt;
}

auto mismatch_ranges_json(const QByteArray& a, const QByteArray& b, int max_ranges) -> QJsonArray {
    QJsonArray ranges;
    const qsizetype COMMON = std::min(a.size(), b.size());
    qsizetype i = 0;
    while (i < COMMON && ranges.size() < max_ranges) {
        while (i < COMMON && a[i] == b[i]) {
            ++i;
        }
        if (i >= COMMON) {
            break;
        }
        const qsizetype START = i;
        while (i < COMMON && a[i] != b[i]) {
            ++i;
        }
        ranges.append(QJsonObject{{"offset", format_hex(static_cast<uint64_t>(START))},
                                  {"length", QString::number(i - START)},
                                  {"actualPreview", hex_preview(a.mid(START, 16))},
                                  {"expectedPreview", hex_preview(b.mid(START, 16))}});
    }
    if (a.size() != b.size() && ranges.size() < max_ranges) {
        ranges.append(QJsonObject{{"offset", format_hex(static_cast<uint64_t>(COMMON))},
                                  {"length", QString::number(std::max(a.size(), b.size()) - COMMON)},
                                  {"kind", "size_mismatch_tail"}});
    }
    return ranges;
}

auto compare_byte_sources_json(const QByteArray& actual, const QByteArray& expected, const QByteArray& full_expected,
                               uint64_t expected_base_offset, int max_ranges) -> QJsonObject {
    const auto FIRST = first_mismatch_offset(actual, expected);
    QJsonObject out{{"matches", !FIRST.has_value()},
                    {"actualHash", sha256_hex(actual)},
                    {"expectedHash", sha256_hex(expected)},
                    {"actualPreview", hex_preview(actual, 32)},
                    {"expectedPreview", hex_preview(expected, 32)},
                    {"firstMismatchOffset", FIRST ? format_hex(*FIRST) : QString()},
                    {"mismatchRanges", mismatch_ranges_json(actual, expected, max_ranges)}};
    if (FIRST) {
        QByteArray pattern =
            actual.mid(static_cast<qsizetype>(*FIRST), std::min<qsizetype>(256, actual.size() - static_cast<qsizetype>(*FIRST)));
        if (pattern.size() >= 16) {
            const qsizetype FOUND = full_expected.indexOf(pattern);
            if (FOUND >= 0 && static_cast<uint64_t>(FOUND) != expected_base_offset + *FIRST) {
                out["actualBytesAlsoMatchFileOffset"] = format_hex(static_cast<uint64_t>(FOUND));
            }
        }
    }
    return out;
}

auto disassemble_bytes_json(const QByteArray& bytes, uint64_t address, int instruction_count, const QString& source_label) -> QJsonArray {
    QJsonArray out;
    if (bytes.isEmpty()) {
        out.append(QJsonObject{{"note", "no bytes available"}, {"byteSource", source_label}});
        return out;
    }
    csh capstone = 0;
    if (cs_open(CS_ARCH_X86, CS_MODE_64, &capstone) != CS_ERR_OK) {
        out.append(QJsonObject{{"note", "Capstone failed to initialize"}, {"byteSource", source_label}});
        return out;
    }
    cs_option(capstone, CS_OPT_SYNTAX, CS_OPT_SYNTAX_INTEL);
    cs_insn* insns = nullptr;
    size_t const COUNT = cs_disasm(capstone, reinterpret_cast<const uint8_t*>(bytes.constData()), static_cast<size_t>(bytes.size()),
                                   address, static_cast<size_t>(instruction_count), &insns);
    if (COUNT == 0) {
        cs_close(&capstone);
        out.append(QJsonObject{{"note", "Capstone disassembly failed"}, {"byteSource", source_label}});
        return out;
    }
    for (size_t i = 0; i < COUNT; ++i) {
        const auto& ins = insns[i];
        QString bytes_text;
        for (int b = 0; std::cmp_less(b, ins.size); ++b) {
            bytes_text += QString("%1 ").arg(static_cast<uint32_t>(ins.bytes[b]), 2, 16, QChar('0'));
        }
        out.append(QJsonObject{{"address", format_hex(ins.address)},
                               {"byteSource", source_label},
                               {"bytes", bytes_text.trimmed()},
                               {"mnemonic", QString::fromLatin1(ins.mnemonic)},
                               {"operands", QString::fromLatin1(ins.op_str)}});
    }
    cs_free(insns, COUNT);
    cs_close(&capstone);
    return out;
}

auto log_entry_excerpt(const LogEntry& entry) -> QJsonObject {
    return QJsonObject{{"line", entry.line_number},
                       {"type", entry_type_name(entry.type)},
                       {"address", QString::fromStdString(entry.address)},
                       {"function", QString::fromStdString(entry.function)},
                       {"assembly", QString::fromStdString(entry.assembly)},
                       {"text", QString::fromStdString(entry.original_line)}};
}

auto task_type_name(uint64_t value) -> QString {
    switch (value) {
        case 0:
            return "daemon";
        case 1:
            return "process";
        case 2:
            return "idle";
        default:
            return QString("unknown(%1)").arg(value);
    }
}

auto task_state_name(uint64_t value) -> QString {
    switch (value) {
        case 0:
            return "active";
        case 1:
            return "exiting";
        case 2:
            return "dead";
        default:
            return QString("unknown(%1)").arg(value);
    }
}

auto task_sched_queue_name(uint64_t value) -> QString {
    switch (value) {
        case 0:
            return "none";
        case 1:
            return "runnable";
        case 2:
            return "waiting";
        case 3:
            return "dead_gc";
        default:
            return QString("unknown(%1)").arg(value);
    }
}

auto task_flags_to_json(uint64_t flags) -> QJsonObject {
    return QJsonObject{{"raw", format_hex(flags)},
                       {"isThread", (flags & (1ULL << 0)) != 0},
                       {"elfBufferShared", (flags & (1ULL << 1)) != 0},
                       {"hasRun", (flags & (1ULL << 2)) != 0},
                       {"hasExited", (flags & (1ULL << 3)) != 0},
                       {"waitedOn", (flags & (1ULL << 4)) != 0},
                       {"deferredTaskSwitch", (flags & (1ULL << 5)) != 0},
                       {"yieldSwitch", (flags & (1ULL << 6)) != 0},
                       {"voluntaryBlock", (flags & (1ULL << 7)) != 0},
                       {"preemptPending", (flags & (1ULL << 8)) != 0},
                       {"inSignalHandler", (flags & (1ULL << 9)) != 0},
                       {"doSigreturn", (flags & (1ULL << 10)) != 0},
                       {"wantsBlock", (flags & (1ULL << 11)) != 0},
                       {"justWoke", (flags & (1ULL << 12)) != 0},
                       {"cpuPinned", (flags & (1ULL << 13)) != 0},
                       {"domainHard", (flags & (1ULL << 14)) != 0},
                       {"wkiPreferInline", (flags & (1ULL << 15)) != 0},
                       {"wkiSkipLegacyPlacement", (flags & (1ULL << 16)) != 0}};
}

auto find_runtime_base_by_bytes(const wosdbg::CoreDump& dump, const QByteArray& elf, const wosdbg::ElfImageInfo& info)
    -> std::optional<uint64_t> {
    if (!info.valid || info.load_segments.empty() || elf.isEmpty()) {
        return std::nullopt;
    }
    const auto* elf_data = elf.constData();
    const auto ELF_SIZE = static_cast<uint64_t>(elf.size());
    for (const auto& dump_seg : dump.segments) {
        if (!dump_seg.is_present()) {
            continue;
        }
        QByteArray const DUMP_BYTES = wosdbg::read_va_bytes(dump, dump_seg.vaddr, 128);
        if (DUMP_BYTES.size() < 32) {
            continue;
        }
        for (const auto& load : info.load_segments) {
            if (load.filesz < 32) {
                continue;
            }
            const uint64_t CANDIDATE_BASE = page_align_down(dump_seg.vaddr) - page_align_down(load.vaddr);
            const uint64_t RUNTIME_LOAD_START = CANDIDATE_BASE + load.vaddr;
            if (dump_seg.vaddr < RUNTIME_LOAD_START) {
                continue;
            }
            const uint64_t IN_LOAD = dump_seg.vaddr - RUNTIME_LOAD_START;
            if (IN_LOAD >= load.filesz || load.offset + IN_LOAD >= ELF_SIZE) {
                continue;
            }
            const auto AVAILABLE = std::min<uint64_t>({128, load.filesz - IN_LOAD, ELF_SIZE - (load.offset + IN_LOAD)});
            if (AVAILABLE < 32) {
                continue;
            }
            if (std::memcmp(DUMP_BYTES.constData(), elf_data + load.offset + IN_LOAD, static_cast<size_t>(AVAILABLE)) == 0) {
                return CANDIDATE_BASE;
            }
        }
    }
    return std::nullopt;
}

auto source_with_llvm_symbolizer(const QString& object_path, uint64_t object_relative_address) -> QJsonObject {
    if (object_path.isEmpty() || !QFileInfo::exists(object_path)) {
        return {};
    }
    QProcess process;
    process.start("llvm-symbolizer", {"--obj=" + object_path, "--inlining", format_hex(object_relative_address)});
    if (!process.waitForStarted(500) || !process.waitForFinished(1500)) {
        process.kill();
        process.waitForFinished(100);
        return {};
    }
    if (process.exitStatus() != QProcess::NormalExit || process.exitCode() != 0) {
        return {};
    }
    const QString OUTPUT = QString::fromUtf8(process.readAllStandardOutput()).trimmed();
    if (OUTPUT.isEmpty()) {
        return {};
    }
    const QStringList LINES = OUTPUT.split('\n', Qt::SkipEmptyParts);
    if (LINES.size() < 2) {
        return {};
    }
    QJsonArray frames;
    for (int i = 0; i + 1 < LINES.size(); i += 2) {
        const QString FUNCTION = LINES[i].trimmed();
        const QString LOCATION = LINES[i + 1].trimmed();
        auto const COLUMN_SPLIT = LOCATION.lastIndexOf(':');
        auto const LINE_SPLIT = COLUMN_SPLIT > 0 ? LOCATION.lastIndexOf(':', COLUMN_SPLIT - 1) : -1;
        QString file = LINE_SPLIT > 0 ? LOCATION.left(LINE_SPLIT) : LOCATION;
        int line = LINE_SPLIT > 0 ? LOCATION.mid(LINE_SPLIT + 1, COLUMN_SPLIT - LINE_SPLIT - 1).toInt() : 0;
        int column = COLUMN_SPLIT > LINE_SPLIT ? LOCATION.mid(COLUMN_SPLIT + 1).toInt() : 0;
        file = clean_source_path(file);
        if (file.isEmpty()) {
            line = 0;
            column = 0;
        }
        frames.append(QJsonObject{{"function", FUNCTION},
                                  {"file", file},
                                  {"line", line},
                                  {"column", column},
                                  {"clickable", !file.isEmpty() && line > 0 ? file + ":" + QString::number(line) : QString()}});
    }
    return QJsonObject{{"frames", frames},
                       {"function", frames.isEmpty() ? QString() : frames.first().toObject()["function"].toString()},
                       {"file", frames.isEmpty() ? QString() : frames.first().toObject()["file"].toString()},
                       {"line", frames.isEmpty() ? 0 : frames.first().toObject()["line"].toInt()},
                       {"column", frames.isEmpty() ? 0 : frames.first().toObject()["column"].toInt()},
                       {"clickable", frames.isEmpty() ? QString() : frames.first().toObject()["clickable"].toString()}};
}

}  // namespace

DebugAnalysisService::DebugAnalysisService(QObject* parent) : QObject(parent) {}

void DebugAnalysisService::set_config(const Config& new_config) { config = const_cast<Config*>(&new_config); }

void DebugAnalysisService::reload_config() {
    ConfigService::instance().reload();
    config = &ConfigService::instance().get_mutable_config();
}

auto DebugAnalysisService::tool_error(const QString& message) -> QJsonObject { return QJsonObject{{"ok", false}, {"error", message}}; }

auto DebugAnalysisService::status() const -> QJsonObject {
    const Config& cfg = (config != nullptr) ? *config : ConfigService::instance().get_config();
    QJsonArray logs;
    for (const auto& key : sorted_hash_keys(log_sessions)) {
        const auto& session = log_sessions[key];
        logs.append(QJsonObject{{"id", key}, {"path", session->path}, {"entries", static_cast<int>(session->entries.size())}});
    }

    QJsonArray dumps;
    for (const auto& key : sorted_hash_keys(dump_sessions)) {
        dumps.append(QJsonObject{{"id", key}, {"path", dump_sessions[key]->path}});
    }

    QJsonArray incidents;
    for (const auto& key : sorted_hash_keys(incident_sessions)) {
        const auto& session = incident_sessions[key];
        incidents.append(QJsonObject{{"id", key},
                                     {"source", session->source_path},
                                     {"members", static_cast<int>(session->bundle ? session->bundle->members.size() : 0)},
                                     {"loading", session->loading}});
    }

    return QJsonObject{
        {"ok", true},
        {"cwd", QDir::currentPath()},
        {"coredumpDirectory", cfg.get_coredump_directory()},
        {"logSessions", logs},
        {"coredumpSessions", dumps},
        {"incidentSessions", incidents},
        {"limits", QJsonObject{{"maxEntries", cfg.get_mcp_settings().max_entries},
                               {"maxHits", cfg.get_mcp_settings().max_hits},
                               {"maxMemoryBytes", cfg.get_mcp_settings().max_memory_bytes},
                               {"sourceWindowLines", cfg.get_mcp_settings().source_window_lines},
                               {"maxIncidentMembers", cfg.get_mcp_settings().max_incident_members},
                               {"maxIncidentMemberBytes", QString::number(cfg.get_mcp_settings().max_incident_member_bytes)},
                               {"maxIncidentTotalBytes", QString::number(cfg.get_mcp_settings().max_incident_total_bytes)},
                               {"maxIncidentArchiveBytes", QString::number(cfg.get_mcp_settings().max_incident_archive_bytes)}}}};
}

auto DebugAnalysisService::allowed_roots() const -> QStringList {
    const Config& cfg = (config != nullptr) ? *config : ConfigService::instance().get_config();
    QStringList roots = cfg.get_mcp_settings().allowed_roots;
    roots << QDir::currentPath();
    roots << cfg.get_coredump_directory();
    for (const auto& mapping : cfg.get_binary_mappings()) {
        roots << QFileInfo(cfg.resolve_path(mapping.elf_path)).absolutePath();
    }
    for (const auto& lookup : cfg.get_address_lookups()) {
        roots << QFileInfo(cfg.resolve_path(lookup.symbol_file_path)).absolutePath();
    }

    QStringList normalized;
    for (const auto& root : roots) {
        if (root.isEmpty()) {
            continue;
        }
        normalized << canonical_path_or_absolute(root);
    }
    normalized.removeDuplicates();
    return normalized;
}

auto DebugAnalysisService::is_path_allowed(const QString& path) const -> bool {
    QString const CANONICAL = canonical_path_or_absolute(path);
    for (const auto& root : allowed_roots()) {
        if (starts_with_path_root(CANONICAL, root)) {
            return true;
        }
    }
    return false;
}

auto DebugAnalysisService::resolve_path_for_read(const QString& path, const QString& fallback_dir, bool canonicalize) const -> QString {
    const Config& cfg = (config != nullptr) ? *config : ConfigService::instance().get_config();
    QString resolved = path;
    if (resolved.isEmpty()) {
        return {};
    }
    QFileInfo const INFO(resolved);
    if (!INFO.isAbsolute()) {
        const QString BASE = fallback_dir.isEmpty() ? QDir::currentPath() : fallback_dir;
        resolved = QDir(BASE).absoluteFilePath(resolved);
    }
    resolved = cfg.resolve_path(resolved);
    return canonicalize ? canonical_path_or_absolute(resolved) : QFileInfo(resolved).absoluteFilePath();
}

auto DebugAnalysisService::make_session_id(const QString& prefix, const QString& canonical_path) -> QString {
    QFileInfo const INFO(canonical_path);
    const QString KEY =
        canonical_path + ":" + QString::number(INFO.size()) + ":" + QString::number(INFO.lastModified().toMSecsSinceEpoch());
    QByteArray const HASH = QCryptographicHash::hash(KEY.toUtf8(), QCryptographicHash::Sha1).toHex().left(16);
    return prefix + "_" + QString::fromLatin1(HASH);
}

auto DebugAnalysisService::make_content_session_id(const QString& prefix, const QString& content_key) -> QString {
    const QByteArray HASH = QCryptographicHash::hash(content_key.toUtf8(), QCryptographicHash::Sha256).toHex().left(24);
    return prefix + "_" + QString::fromLatin1(HASH);
}

auto DebugAnalysisService::incident_limits() const -> wosdbg::IncidentLimits {
    const Config& cfg = (config != nullptr) ? *config : ConfigService::instance().get_config();
    const auto& settings = cfg.get_mcp_settings();
    wosdbg::IncidentLimits limits;
    limits.max_members = static_cast<size_t>(settings.max_incident_members);
    limits.max_member_bytes = static_cast<uint64_t>(settings.max_incident_member_bytes);
    limits.max_total_bytes = static_cast<uint64_t>(settings.max_incident_total_bytes);
    limits.max_archive_bytes = static_cast<uint64_t>(settings.max_incident_archive_bytes);
    limits.max_path_length = static_cast<size_t>(settings.max_incident_path_length);
    limits.max_path_depth = static_cast<size_t>(settings.max_incident_path_depth);
    limits.allowed_roots = allowed_roots();
    return limits;
}

auto DebugAnalysisService::incident_validation_to_json(const wosdbg::IncidentBundle& bundle, bool include_inventory, int max_issues) const
    -> QJsonObject {
    const Config& cfg = (config != nullptr) ? *config : ConfigService::instance().get_config();
    const int ISSUE_LIMIT =
        std::clamp(max_issues < 0 ? cfg.get_mcp_settings().max_entries : max_issues, 1, cfg.get_mcp_settings().max_entries);
    QJsonArray issues = wosdbg::incident_issues_to_json(bundle, static_cast<size_t>(ISSUE_LIMIT));
    int issue_count = static_cast<int>(bundle.issues.size());
    bool valid = bundle.valid();
    bool degraded = !valid || !bundle.issues.empty();
    QSet<QString> issue_keys;
    for (const auto& issue : bundle.issues) {
        issue_keys.insert(issue.code + ':' + issue.path);
    }
    auto add_issue = [&](const QString& code, const QString& path, const QString& message, bool fatal,
                         QJsonObject details = QJsonObject{}) {
        const QString key = code + ':' + path;
        if (issue_keys.contains(key)) {
            return;
        }
        issue_keys.insert(key);
        ++issue_count;
        if (issues.size() < ISSUE_LIMIT) {
            QJsonObject issue{{"code", code}, {"path", path}, {"message", message}, {"fatal", fatal}};
            if (!details.isEmpty()) {
                issue["details"] = std::move(details);
            }
            issues.append(issue);
        }
        degraded = true;
        valid = valid && !fatal;
    };

    QJsonArray coredump_versions;
    int coredump_version_count = 0;
    for (const auto& member : bundle.members) {
        if (member.truncated && (member.kind == "serial-log" || member.kind == "qemu-log" || member.kind == "coverage-log" ||
                                 member.kind == "telemetry-log" || member.kind == "log")) {
            add_issue("truncated_log", member.path, "captured log evidence is explicitly truncated", false);
        }
        if (member.kind != "coredump" || member.absolute_path.isEmpty()) {
            continue;
        }
        const auto parsed = wosdbg::parse_core_dump_checked(member.absolute_path);
        ++coredump_version_count;
        if (coredump_versions.size() < cfg.get_mcp_settings().max_entries) {
            coredump_versions.append(static_cast<int>(parsed.detected_version));
        }
        if (!parsed.ok()) {
            QString code = "corrupt_coredump";
            if (parsed.status == wosdbg::CoreDumpParseStatus::UNSUPPORTED_VERSION) {
                code = "unsupported_coredump_version";
            } else if (parsed.status == wosdbg::CoreDumpParseStatus::TRUNCATED) {
                code = "truncated_coredump";
            } else if (parsed.status == wosdbg::CoreDumpParseStatus::TOO_LARGE) {
                code = "coredump_too_large";
            }
            add_issue(code, member.path, parsed.error, member.required);
            continue;
        }
        if (member.truncated) {
            add_issue("truncated_coredump", member.path, "captured coredump evidence is explicitly truncated", member.required);
        }
        const bool HAS_PRESENT_MAPPING =
            std::ranges::any_of(parsed.dump->segments, [](const wosdbg::CoreDumpSegment& segment) { return segment.is_present(); });
        if (!HAS_PRESENT_MAPPING) {
            add_issue("missing_mapping", member.path, "coredump contains no captured present memory mapping", false);
        }

        const QString EMBEDDED_BUILD_ID = wosdbg::elf_build_id(parsed.dump->embedded_elf());
        const wosdbg::IncidentMember* binary = member.binary.isEmpty() ? nullptr : bundle.find_member(member.binary);
        QString binary_build_id;
        if (binary != nullptr && !binary->absolute_path.isEmpty()) {
            binary_build_id = wosdbg::elf_build_id_from_file(binary->absolute_path);
        }
        bool build_id_mismatch = false;
        auto compare_build_ids = [&](const QString& left, const QString& right, const QString& code, const QString& message,
                                     const QString& left_role, const QString& right_role) {
            if (left.isEmpty() || right.isEmpty() || left == right) {
                return;
            }
            build_id_mismatch = true;
            add_issue(code, member.path, message, false,
                      QJsonObject{{"leftRole", left_role}, {"leftBuildId", left}, {"rightRole", right_role}, {"rightBuildId", right}});
        };
        compare_build_ids(member.build_id, EMBEDDED_BUILD_ID, "declared_embedded_build_id_mismatch",
                          "manifest-declared build ID differs from the coredump embedded ELF build ID", "declared", "embedded");
        compare_build_ids(member.build_id, binary_build_id, "declared_binary_build_id_mismatch",
                          "manifest-declared build ID differs from the referenced binary build ID", "declared", "binary");
        compare_build_ids(EMBEDDED_BUILD_ID, binary_build_id, "embedded_binary_build_id_mismatch",
                          "coredump embedded ELF build ID differs from the referenced binary build ID", "embedded", "binary");
        if (build_id_mismatch) {
            add_issue("build_id_mismatch", member.path, "coredump build-ID evidence is inconsistent; binary symbols are quarantined",
                      false);
        }
        if (binary == nullptr && parsed.dump->embedded_elf().isEmpty()) {
            add_issue("missing_symbols", member.path, "coredump has neither a referenced binary nor an embedded ELF", false);
        }
    }

    const QJsonObject CLOCKS = bundle.manifest["clocks"].toObject();
    const QString CLOCK_QUALITY = CLOCKS["quality"].toString("unavailable");
    if (CLOCK_QUALITY == "partial") {
        add_issue("partial_clocks", "manifest.json", "only part of the incident has comparable timestamp evidence", false);
    } else if (CLOCK_QUALITY == "non-comparable") {
        add_issue("non_comparable_clocks", "manifest.json", "cross-node clocks are explicitly not comparable", false);
    }

    const QJsonObject CAPTURE = bundle.manifest["capture"].toObject();
    degraded = degraded || !CAPTURE["complete"].toBool(true) || !CAPTURE["errors"].toArray().isEmpty() ||
               !CAPTURE["truncatedMembers"].toArray().isEmpty();
    const bool ISSUES_TRUNCATED = issue_count > issues.size();

    QJsonObject result{{"ok", true},
                       {"valid", valid},
                       {"incidentId", bundle.incident_id()},
                       {"formatVersion", bundle.manifest["version"].toInt()},
                       {"container", bundle.archive ? "archive" : "directory"},
                       {"issues", issues},
                       {"issueCount", issue_count},
                       {"issuesReturned", issues.size()},
                       {"issuesTruncated", ISSUES_TRUNCATED},
                       {"degraded", degraded},
                       {"clockQuality", CLOCK_QUALITY},
                       {"coredumpVersions", coredump_versions},
                       {"coredumpVersionCount", coredump_version_count},
                       {"coredumpVersionsTruncated", coredump_version_count > coredump_versions.size()}};
    if (include_inventory) {
        QJsonObject inventory =
            wosdbg::incident_inventory_to_json(bundle, 0, static_cast<size_t>(cfg.get_mcp_settings().max_entries), false);
        inventory["issues"] = issues;
        result["inventory"] = inventory;
    } else {
        result["inventory"] =
            QJsonObject{{"total", QString::number(bundle.members.size())}, {"count", 0}, {"members", QJsonArray{}}, {"issues", issues}};
    }
    return result;
}

auto DebugAnalysisService::normalize_incident_value(const IncidentSession& session, const QJsonValue& value, bool* response_truncated) const
    -> QJsonValue {
    const Config& cfg = (config != nullptr) ? *config : ConfigService::instance().get_config();
    const int MAX_STRING = cfg.get_mcp_settings().max_string_length;
    const int MAX_ARRAY = cfg.get_mcp_settings().max_entries;
    if (value.isString()) {
        QString text = value.toString();
        if (session.bundle && !session.bundle->root_path.isEmpty() && starts_with_path_root(text, session.bundle->root_path)) {
            text = QDir(session.bundle->root_path).relativeFilePath(text);
        }
        const QString REPOSITORY_ROOT = QDir::currentPath();
        if (!REPOSITORY_ROOT.isEmpty()) {
            text.replace(REPOSITORY_ROOT, "<repo>");
        }
        if (text.size() > MAX_STRING) {
            text = text.left(MAX_STRING) + QString::fromUtf8("\u2026");
            if (response_truncated != nullptr) {
                *response_truncated = true;
            }
        }
        return text;
    }
    if (value.isArray()) {
        QJsonArray normalized;
        const QJsonArray input = value.toArray();
        const qsizetype count = std::min(input.size(), static_cast<qsizetype>(MAX_ARRAY));
        for (qsizetype i = 0; i < count; ++i) {
            normalized.append(normalize_incident_value(session, input[i], response_truncated));
        }
        if (count < input.size() && response_truncated != nullptr) {
            *response_truncated = true;
        }
        return normalized;
    }
    if (value.isObject()) {
        QJsonObject normalized;
        const QJsonObject input = value.toObject();
        QStringList keys = input.keys();
        std::ranges::sort(keys);
        for (const auto& key : keys) {
            normalized[key] = normalize_incident_value(session, input[key], response_truncated);
        }
        return normalized;
    }
    return value;
}

auto DebugAnalysisService::find_incident_session(const QString& id) const -> const IncidentSession* {
    const auto it = incident_sessions.find(id);
    return it == incident_sessions.end() ? nullptr : it.value().get();
}

auto DebugAnalysisService::find_incident_session(const QString& id) -> IncidentSession* {
    const auto it = incident_sessions.find(id);
    return it == incident_sessions.end() ? nullptr : it.value().get();
}

void DebugAnalysisService::erase_incident_session(const QString& id) {
    const auto it = incident_sessions.find(id);
    if (it == incident_sessions.end()) {
        return;
    }
    const auto& session = it.value();
    for (const auto& log_id : std::as_const(session->log_ids)) {
        log_sessions.remove(log_id);
    }
    for (const auto& dump_id : std::as_const(session->dump_ids)) {
        dump_sessions.remove(dump_id);
    }
    incident_sessions.erase(it);
}

auto DebugAnalysisService::validate_incident(const QJsonObject& args) const -> QJsonObject {
    const QString PATH = args["path"].toString(args["file"].toString());
    if (PATH.isEmpty()) {
        return tool_error("validate_incident requires 'path' or 'file'");
    }
    const QString SOURCE = resolve_path_for_read(PATH, {}, false);
    if (QFileInfo(SOURCE).isSymLink()) {
        auto bundle = wosdbg::load_incident_bundle(SOURCE, incident_limits());
        return bundle ? incident_validation_to_json(*bundle, args["includeInventory"].toBool(true),
                                                    bounded_int(args, "maxIssues", 200, 1, 10000))
                      : tool_error("Incident bundle loader failed without a validation result");
    }
    const QString RESOLVED = canonical_path_or_absolute(SOURCE);
    if (!QFileInfo::exists(SOURCE)) {
        return tool_error(QString("Incident bundle not found: %1").arg(PATH));
    }
    if (!is_path_allowed(RESOLVED)) {
        return tool_error(QString("Incident bundle is outside allowed roots: %1").arg(RESOLVED));
    }
    auto bundle = wosdbg::load_incident_bundle(SOURCE, incident_limits());
    if (!bundle) {
        return tool_error("Incident bundle loader failed without a validation result");
    }
    return incident_validation_to_json(*bundle, args["includeInventory"].toBool(true), bounded_int(args, "maxIssues", 200, 1, 10000));
}

auto DebugAnalysisService::load_incident(const QJsonObject& args) -> QJsonObject {
    const QString PATH = args["path"].toString(args["file"].toString());
    if (PATH.isEmpty()) {
        return tool_error("load_incident requires 'path' or 'file'");
    }
    const QString SOURCE = resolve_path_for_read(PATH, {}, false);
    if (QFileInfo(SOURCE).isSymLink()) {
        auto bundle = wosdbg::load_incident_bundle(SOURCE, incident_limits());
        if (!bundle) {
            return tool_error("Incident bundle loader failed without a validation result");
        }
        QJsonObject validation = incident_validation_to_json(*bundle, true);
        validation["loaded"] = false;
        return validation;
    }
    const QString RESOLVED = canonical_path_or_absolute(SOURCE);
    if (!QFileInfo::exists(SOURCE)) {
        return tool_error(QString("Incident bundle not found: %1").arg(PATH));
    }
    if (!is_path_allowed(RESOLVED)) {
        return tool_error(QString("Incident bundle is outside allowed roots: %1").arg(RESOLVED));
    }

    auto bundle = wosdbg::load_incident_bundle(SOURCE, incident_limits());
    if (!bundle) {
        return tool_error("Incident bundle loader failed without a validation result");
    }
    QJsonObject validation = incident_validation_to_json(*bundle, true);
    if (!bundle->valid()) {
        validation["loaded"] = false;
        return validation;
    }

    const Config& cfg = (config != nullptr) ? *config : ConfigService::instance().get_config();
    const bool LOAD_EVIDENCE = args["loadEvidence"].toBool(true);
    const int MAX_LOGS = bounded_int(args, "maxLogs", cfg.get_mcp_settings().max_entries, 0, cfg.get_mcp_settings().max_entries);
    const int MAX_DUMPS = bounded_int(args, "maxCoredumps", cfg.get_mcp_settings().max_entries, 0, cfg.get_mcp_settings().max_entries);
    const QJsonObject LOAD_POLICY{
        {"loadEvidence", LOAD_EVIDENCE}, {"maxLogs", MAX_LOGS}, {"maxCoredumps", MAX_DUMPS}, {"externalSymbols", false}};
    const QString ID = bundle->incident_id();
    if (incident_sessions.contains(ID)) {
        const auto& cached = incident_sessions[ID];
        if (cached->load_evidence == LOAD_EVIDENCE && cached->max_logs == MAX_LOGS && cached->max_coredumps == MAX_DUMPS) {
            QJsonObject result = incident_validation_to_json(*cached->bundle, true);
            result["cached"] = true;
            result["loaded"] = true;
            result["loading"] = cached->loading;
            result["loadedEvidence"] = cached->evidence.size();
            result["loadIssues"] = cached->load_issues;
            result["loadIssueCount"] = cached->load_issue_count;
            result["loadIssuesTruncated"] = cached->load_issues_truncated;
            result["loadPolicy"] = LOAD_POLICY;
            return result;
        }
        erase_incident_session(ID);
    }

    const int MAX_INCIDENT_SESSIONS = std::max(1, std::min(32, cfg.get_mcp_settings().max_entries));
    while (incident_sessions.size() >= MAX_INCIDENT_SESSIONS) {
        QString evicted_id;
        uint64_t oldest_sequence = std::numeric_limits<uint64_t>::max();
        for (auto it = incident_sessions.cbegin(); it != incident_sessions.cend(); ++it) {
            if (!it.value()->loading && it.value()->sequence < oldest_sequence) {
                oldest_sequence = it.value()->sequence;
                evicted_id = it.key();
            }
        }
        if (evicted_id.isEmpty()) {
            validation["loaded"] = false;
            validation["error"] = "Incident session limit reached while all sessions are active";
            return validation;
        }
        erase_incident_session(evicted_id);
    }

    auto session = std::make_shared<IncidentSession>();
    session->id = ID;
    session->source_path = RESOLVED;
    session->bundle = std::move(bundle);
    session->load_evidence = LOAD_EVIDENCE;
    session->max_logs = MAX_LOGS;
    session->max_coredumps = MAX_DUMPS;
    session->sequence = ++incident_session_sequence;
    session->loading = true;
    incident_sessions.insert(ID, session);
    int loaded_logs = 0;
    int loaded_dumps = 0;

    auto append_load_issue = [&](const QString& code, const QString& path, const QString& message, bool fatal = false) {
        ++session->load_issue_count;
        if (session->load_issues.size() < cfg.get_mcp_settings().max_entries) {
            session->load_issues.append(QJsonObject{{"code", code}, {"path", path}, {"message", message}, {"fatal", fatal}});
        } else {
            session->load_issues_truncated = true;
        }
    };
    auto node_id_for = [](const wosdbg::IncidentMember& member) {
        const QJsonValue value = member.metadata["nodeId"];
        return value.isDouble() ? QString::number(static_cast<qint64>(value.toDouble())) : value.toString(member.node_id);
    };
    auto evidence_base = [&](const wosdbg::IncidentMember& member) {
        return QJsonObject{{"memberPath", member.path},
                           {"kind", member.kind},
                           {"nodeId", node_id_for(member)},
                           {"required", member.required},
                           {"truncated", member.truncated},
                           {"state", member.state},
                           {"availability", member.absolute_path.isEmpty() ? "missing" : "present"}};
    };

    for (const auto& member : session->bundle->members) {
        QJsonObject evidence = evidence_base(member);
        if (!member.absolute_path.isEmpty()) {
            evidence["status"] = "not_loaded";
        } else if (member.state == "error") {
            evidence["status"] = "declared_error";
        } else if (member.state == "missing") {
            evidence["status"] = "declared_missing";
        } else {
            evidence["status"] = member.required ? "required_member_missing" : "optional_member_missing";
        }
        session->evidence.insert(member.path, evidence);
    }

    QString kernel_elf_path;
    QHash<QString, const wosdbg::IncidentMember*> binaries_by_build_id;
    QSet<QString> quarantined_binary_paths;
    for (const auto& member : session->bundle->members) {
        if (member.kind != "binary" || member.absolute_path.isEmpty()) {
            continue;
        }
        const QString ACTUAL_BUILD_ID = wosdbg::elf_build_id_from_file(member.absolute_path);
        const bool BUILD_ID_MISMATCH = !member.build_id.isEmpty() && !ACTUAL_BUILD_ID.isEmpty() && member.build_id != ACTUAL_BUILD_ID;
        const bool VERIFIED = !ACTUAL_BUILD_ID.isEmpty() && !BUILD_ID_MISMATCH;
        if (VERIFIED && !binaries_by_build_id.contains(ACTUAL_BUILD_ID)) {
            binaries_by_build_id.insert(ACTUAL_BUILD_ID, &member);
        }
        if (VERIFIED && (member.path.startsWith("binaries/kernel/") || member.metadata["role"].toString() == "kernel")) {
            kernel_elf_path = member.absolute_path;
        }
        QJsonObject evidence = session->evidence.value(member.path);
        QString binary_status = "available";
        QString build_id_status = "verified";
        if (BUILD_ID_MISMATCH) {
            binary_status = "build_id_mismatch";
            build_id_status = "mismatch";
        } else if (ACTUAL_BUILD_ID.isEmpty()) {
            binary_status = "build_id_missing";
            build_id_status = "missing";
        }
        evidence["status"] = binary_status;
        evidence["declaredBuildId"] = member.build_id;
        evidence["binaryBuildId"] = ACTUAL_BUILD_ID;
        evidence["buildIdStatus"] = build_id_status;
        evidence["quarantined"] = !VERIFIED;
        if (!VERIFIED) {
            quarantined_binary_paths.insert(member.path);
        }
        if (BUILD_ID_MISMATCH) {
            append_load_issue("binary_member_build_id_mismatch", member.path,
                              "binary member build ID differs from its manifest declaration; binary was quarantined");
        } else if (ACTUAL_BUILD_ID.isEmpty()) {
            append_load_issue("binary_build_id_missing", member.path, "binary member has no readable GNU build ID; binary was quarantined");
        }
        session->evidence.insert(member.path, evidence);
    }

    if (LOAD_EVIDENCE) {
        for (const auto& member : session->bundle->members) {
            if (member.absolute_path.isEmpty() || member.kind == "binary") {
                continue;
            }
            QJsonObject evidence = evidence_base(member);
            if (member.truncated) {
                evidence["degradation"] = "truncated";
            }
            if (member.kind == "serial-log" || member.kind == "qemu-log" || member.kind == "telemetry-log" ||
                member.kind == "coverage-log" || member.kind == "log") {
                if (loaded_logs >= MAX_LOGS) {
                    evidence["status"] = "not_loaded_response_bound";
                    append_load_issue("log_load_limit", member.path, "log was not loaded because the configured request bound was reached");
                } else {
                    const QString LOG_ID = make_content_session_id("log", ID + ':' + member.path + ':' + member.sha256);
                    const QJsonObject result = load_log_file(member.absolute_path, LOG_ID, 120000, false);
                    if (result["ok"].toBool(false)) {
                        session->log_ids.insert(member.path, LOG_ID);
                        evidence["status"] = member.truncated ? "loaded_truncated" : "loaded";
                        evidence["logId"] = LOG_ID;
                        evidence["entryCount"] = result["summary"].toObject()["entries"];
                        ++loaded_logs;
                    } else {
                        evidence["status"] = "log_parse_error";
                        evidence["error"] = result["error"];
                        append_load_issue("log_parse_error", member.path, result["error"].toString());
                    }
                }
            } else if (member.kind == "coredump") {
                if (loaded_dumps >= MAX_DUMPS) {
                    evidence["status"] = "not_loaded_response_bound";
                    append_load_issue("coredump_load_limit", member.path,
                                      "coredump was not loaded because the configured request bound was reached");
                } else {
                    const auto parsed = wosdbg::parse_core_dump_checked(member.absolute_path);
                    evidence["parseStatus"] = wosdbg::core_dump_parse_status_name(parsed.status);
                    evidence["coredumpVersion"] = static_cast<int>(parsed.detected_version);
                    if (!parsed.error.isEmpty()) {
                        evidence["parseError"] = parsed.error;
                        evidence["errorOffset"] = QString::number(parsed.error_offset);
                    }
                    if (!parsed.ok()) {
                        QString code = "corrupt_coredump";
                        if (parsed.status == wosdbg::CoreDumpParseStatus::UNSUPPORTED_VERSION) {
                            code = "unsupported_coredump_version";
                        } else if (parsed.status == wosdbg::CoreDumpParseStatus::TRUNCATED) {
                            code = "truncated_coredump";
                        } else if (parsed.status == wosdbg::CoreDumpParseStatus::TOO_LARGE) {
                            code = "coredump_too_large";
                        }
                        evidence["status"] = code;
                        append_load_issue(code, member.path, parsed.error);
                    } else {
                        QString binary_path;
                        const QString DECLARED_BUILD_ID = member.build_id;
                        QString binary_build_id;
                        bool binary_member_quarantined = false;
                        if (!member.binary.isEmpty()) {
                            if (const auto* binary = session->bundle->find_member(member.binary); binary != nullptr) {
                                binary_path = binary->absolute_path;
                                binary_member_quarantined = quarantined_binary_paths.contains(binary->path);
                                if (!binary_path.isEmpty()) {
                                    binary_build_id = wosdbg::elf_build_id_from_file(binary_path);
                                }
                            }
                        }
                        const QString EMBEDDED_BUILD_ID = wosdbg::elf_build_id(parsed.dump->embedded_elf());
                        const QString LOOKUP_BUILD_ID = !DECLARED_BUILD_ID.isEmpty() ? DECLARED_BUILD_ID : EMBEDDED_BUILD_ID;
                        if (binary_path.isEmpty() && !LOOKUP_BUILD_ID.isEmpty() && binaries_by_build_id.contains(LOOKUP_BUILD_ID)) {
                            binary_path = binaries_by_build_id[LOOKUP_BUILD_ID]->absolute_path;
                            binary_build_id = LOOKUP_BUILD_ID;
                        }
                        const bool DECLARED_EMBEDDED_MISMATCH =
                            !DECLARED_BUILD_ID.isEmpty() && !EMBEDDED_BUILD_ID.isEmpty() && DECLARED_BUILD_ID != EMBEDDED_BUILD_ID;
                        const bool DECLARED_BINARY_MISMATCH =
                            !DECLARED_BUILD_ID.isEmpty() && !binary_build_id.isEmpty() && DECLARED_BUILD_ID != binary_build_id;
                        const bool EMBEDDED_BINARY_MISMATCH =
                            !EMBEDDED_BUILD_ID.isEmpty() && !binary_build_id.isEmpty() && EMBEDDED_BUILD_ID != binary_build_id;
                        const bool BINARY_UNVERIFIED = !binary_path.isEmpty() && binary_build_id.isEmpty();
                        const bool BINARY_QUARANTINED = binary_member_quarantined || DECLARED_EMBEDDED_MISMATCH ||
                                                        DECLARED_BINARY_MISMATCH || EMBEDDED_BINARY_MISMATCH || BINARY_UNVERIFIED;
                        evidence["declaredBuildId"] = DECLARED_BUILD_ID;
                        evidence["embeddedBuildId"] = EMBEDDED_BUILD_ID;
                        evidence["binaryBuildId"] = binary_build_id;
                        auto build_id_check = [](const QString& left, const QString& right) {
                            if (!left.isEmpty() && !right.isEmpty() && left != right) {
                                return QString("mismatch");
                            }
                            return QString("compatible");
                        };
                        evidence["buildIdChecks"] = QJsonObject{{"declaredEmbedded", build_id_check(DECLARED_BUILD_ID, EMBEDDED_BUILD_ID)},
                                                                {"declaredBinary", build_id_check(DECLARED_BUILD_ID, binary_build_id)},
                                                                {"embeddedBinary", build_id_check(EMBEDDED_BUILD_ID, binary_build_id)}};
                        evidence["binaryQuarantined"] = BINARY_QUARANTINED;
                        if (BINARY_QUARANTINED) {
                            binary_path.clear();
                        }
                        const QString DUMP_ID = make_content_session_id("dump", ID + ':' + member.path + ':' + member.sha256);
                        const QJsonObject result = open_coredump_file(member.absolute_path, DUMP_ID, binary_path, kernel_elf_path, false);
                        if (result["ok"].toBool(false)) {
                            session->dump_ids.insert(member.path, DUMP_ID);
                            evidence["status"] = member.truncated ? "loaded_truncated" : "loaded";
                            evidence["dumpId"] = DUMP_ID;
                            const QJsonObject summary = result["summary"].toObject();
                            const bool PAIRWISE_BUILD_ID_MISMATCH =
                                DECLARED_EMBEDDED_MISMATCH || DECLARED_BINARY_MISMATCH || EMBEDDED_BINARY_MISMATCH;
                            QJsonValue build_id_result = summary["symbols"].toObject()["buildIdStatus"];
                            if (PAIRWISE_BUILD_ID_MISMATCH) {
                                build_id_result = "mismatch";
                            } else if (BINARY_QUARANTINED) {
                                build_id_result = "unverified-binary";
                            }
                            evidence["buildIdStatus"] = build_id_result;
                            evidence["symbolStatus"] = summary["symbols"].toObject()["symbolStatus"];
                            if (evidence["buildIdStatus"].toString() == "mismatch") {
                                append_load_issue("build_id_mismatch", member.path,
                                                  "matching binary build ID differs from the coredump embedded build ID");
                            }
                            if (evidence["symbolStatus"].toString() == "missing") {
                                append_load_issue("missing_symbols", member.path, "no matching symbols are available");
                            }
                            ++loaded_dumps;
                        } else {
                            evidence["status"] = "coredump_open_error";
                            evidence["error"] = result["error"];
                            append_load_issue("coredump_open_error", member.path, result["error"].toString());
                        }
                    }
                }
            } else if (member.kind == "coverage-manifest" || member.kind == "config") {
                QJsonParseError error;
                const QJsonDocument document = QJsonDocument::fromJson(read_file_bytes_quiet(member.absolute_path), &error);
                if (error.error == QJsonParseError::NoError && (document.isObject() || document.isArray())) {
                    evidence["status"] = "available";
                    evidence["jsonType"] = document.isObject() ? "object" : "array";
                } else {
                    evidence["status"] = "invalid_json";
                    evidence["error"] = error.errorString();
                    append_load_issue("invalid_json_evidence", member.path, error.errorString());
                }
            } else {
                evidence["status"] = "available";
            }
            session->evidence.insert(member.path, evidence);
        }
    } else {
        for (const auto& member : session->bundle->members) {
            if (!session->evidence.contains(member.path)) {
                QJsonObject evidence = evidence_base(member);
                evidence["status"] = "not_loaded";
                session->evidence.insert(member.path, evidence);
            }
        }
    }

    session->loading = false;
    validation["cached"] = false;
    validation["loaded"] = true;
    validation["loadedLogs"] = loaded_logs;
    validation["loadedCoredumps"] = loaded_dumps;
    validation["loadedEvidence"] = session->evidence.size();
    validation["loadIssues"] = session->load_issues;
    validation["loadIssueCount"] = session->load_issue_count;
    validation["loadIssuesTruncated"] = session->load_issues_truncated;
    validation["loadPolicy"] = LOAD_POLICY;
    return validation;
}

auto DebugAnalysisService::get_incident_inventory(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_incident_session(args["incidentId"].toString());
    if (session == nullptr || !session->bundle) {
        return tool_error("Unknown incidentId");
    }
    const Config& cfg = (config != nullptr) ? *config : ConfigService::instance().get_config();
    const int START = bounded_int(args, "start", 0, 0, static_cast<int>(session->bundle->members.size()));
    const int COUNT = bounded_int(args, "count", 100, 1, cfg.get_mcp_settings().max_entries);
    QJsonObject result = incident_validation_to_json(*session->bundle, false);
    QJsonObject inventory =
        wosdbg::incident_inventory_to_json(*session->bundle, static_cast<size_t>(START), static_cast<size_t>(COUNT), false);
    inventory["issues"] = result["issues"];
    QJsonArray evidence;
    const int END = std::min(START + COUNT, static_cast<int>(session->bundle->members.size()));
    for (int index = START; index < END; ++index) {
        const QString& path = session->bundle->members[static_cast<size_t>(index)].path;
        evidence.append(session->evidence.value(path));
    }
    result["inventory"] = inventory;
    result["evidence"] = evidence;
    result["loadIssues"] = session->load_issues;
    result["loadIssueCount"] = session->load_issue_count;
    result["loadIssuesTruncated"] = session->load_issues_truncated;
    result["loadPolicy"] = QJsonObject{{"loadEvidence", session->load_evidence},
                                       {"maxLogs", session->max_logs},
                                       {"maxCoredumps", session->max_coredumps},
                                       {"externalSymbols", false}};
    return result;
}

auto DebugAnalysisService::summarize_incident(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_incident_session(args["incidentId"].toString());
    if (session == nullptr || !session->bundle) {
        return tool_error("Unknown incidentId");
    }
    if (session->loading) {
        return tool_error("Incident evidence is still loading");
    }
    const Config& cfg = (config != nullptr) ? *config : ConfigService::instance().get_config();
    const int MAX_EVENTS = bounded_int(args, "maxEvents", 200, 1, cfg.get_mcp_settings().max_entries);
    const int MAX_ISSUES = bounded_int(args, "maxIssues", 64, 1, cfg.get_mcp_settings().max_entries);
    const int MAX_COREDUMPS = bounded_int(args, "maxCoredumps", 16, 1, std::min(64, cfg.get_mcp_settings().max_entries));
    QJsonObject validation = incident_validation_to_json(*session->bundle, true, MAX_ISSUES);
    int remaining_issue_budget = std::max(0, MAX_ISSUES - static_cast<int>(validation["issues"].toArray().size()));
    QJsonArray bounded_load_issues;
    const int LOAD_ISSUES_RETURNED = std::min(remaining_issue_budget, static_cast<int>(session->load_issues.size()));
    for (int index = 0; index < LOAD_ISSUES_RETURNED; ++index) {
        bounded_load_issues.append(session->load_issues[index]);
    }
    remaining_issue_budget -= LOAD_ISSUES_RETURNED;
    QJsonArray analysis_issues;
    int analysis_issue_count = 0;

    QStringList evidence_paths = session->evidence.keys();
    std::ranges::sort(evidence_paths);
    const int EVIDENCE_COUNT = static_cast<int>(evidence_paths.size());
    const bool EVIDENCE_TRUNCATED = EVIDENCE_COUNT > cfg.get_mcp_settings().max_entries;
    evidence_paths = evidence_paths.mid(0, cfg.get_mcp_settings().max_entries);
    QJsonArray evidence;
    bool degraded = session->load_issue_count > 0;
    for (const auto& path : evidence_paths) {
        const QJsonObject item = session->evidence[path];
        const QString status = item["status"].toString();
        degraded = degraded || (status != "loaded" && status != "available");
        evidence.append(item);
    }

    QStringList log_paths = session->log_ids.keys();
    std::ranges::sort(log_paths);
    QJsonArray log_ids;
    QSet<QString> log_nodes;
    QSet<QString> log_clock_domains;
    bool log_references_complete = true;
    for (const auto& path : log_paths) {
        log_ids.append(session->log_ids[path]);
        if (const auto* member = session->bundle->find_member(path); member != nullptr) {
            const QJsonValue node = member->metadata["nodeId"];
            if (!node.isUndefined() && !node.isNull()) {
                log_nodes.insert(node.isDouble() ? QString::number(static_cast<qint64>(node.toDouble())) : node.toString());
            } else {
                log_references_complete = false;
            }
            const QString domain = member->metadata["clockDomain"].toString();
            if (!domain.isEmpty()) {
                log_clock_domains.insert(domain);
            } else {
                log_references_complete = false;
            }
        } else {
            log_references_complete = false;
        }
    }

    QJsonObject timeline{{"ok", true},
                         {"events", QJsonArray{}},
                         {"lanes", QJsonArray{}},
                         {"clockOrderedEvents", QJsonArray{}},
                         {"clockQuality", "unavailable"},
                         {"crossLogCorrelations", QJsonArray{}},
                         {"truncated", false}};
    if (args["includeTimeline"].toBool(true) && !log_ids.isEmpty()) {
        timeline = build_distributed_timeline(QJsonObject{{"logIds", log_ids}, {"maxEvents", MAX_EVENTS}});
    }

    const QJsonObject CLOCKS = session->bundle->manifest["clocks"].toObject();
    const QString MANIFEST_CLOCK_QUALITY = CLOCKS["quality"].toString("unavailable");
    bool domains_comparable_across_nodes = !log_clock_domains.isEmpty();
    QSet<QString> comparable_domains;
    for (const auto& value : CLOCKS["domains"].toArray()) {
        const QJsonObject domain = value.toObject();
        if (domain["comparableAcrossNodes"].toBool(false) && domain["synchronized"].toBool(false)) {
            comparable_domains.insert(domain["id"].toString());
        }
    }
    for (const auto& domain : std::as_const(log_clock_domains)) {
        domains_comparable_across_nodes = domains_comparable_across_nodes && comparable_domains.contains(domain);
    }
    const int TOPOLOGY_NODE_COUNT = session->bundle->manifest["topology"].toObject()["nodes"].toArray().size();
    const bool POSSIBLY_MULTI_NODE = log_nodes.size() > 1 || (!log_references_complete && TOPOLOGY_NODE_COUNT > 1);
    const bool SAME_NODE_DOMAIN = log_references_complete && log_nodes.size() <= 1 && log_clock_domains.size() == 1;
    const bool CROSS_LOG_ORDER_SUPPORTED =
        log_paths.size() <= 1 || SAME_NODE_DOMAIN || (log_references_complete && domains_comparable_across_nodes);
    const bool MANIFEST_ORDER_ALLOWED =
        MANIFEST_CLOCK_QUALITY == "complete" || (MANIFEST_CLOCK_QUALITY == "single-node-unsynchronized" && !POSSIBLY_MULTI_NODE);
    const QString OBSERVED_CLOCK_QUALITY = timeline["clockQuality"].toString("unavailable");
    QString effective_clock_quality = OBSERVED_CLOCK_QUALITY;
    if (MANIFEST_CLOCK_QUALITY == "non-comparable" || MANIFEST_CLOCK_QUALITY == "unavailable") {
        effective_clock_quality = "per-log-order-only";
        if (log_paths.size() > 1 || POSSIBLY_MULTI_NODE) {
            timeline["clockOrderedEvents"] = QJsonArray{};
        }
    } else if (MANIFEST_CLOCK_QUALITY == "partial" || !CROSS_LOG_ORDER_SUPPORTED || !MANIFEST_ORDER_ALLOWED) {
        effective_clock_quality = OBSERVED_CLOCK_QUALITY == "per-log-order-only" ? "per-log-order-only" : "partial-timestamps-untrusted";
        timeline["clockOrderedEvents"] = QJsonArray{};
    } else if (OBSERVED_CLOCK_QUALITY == "all-events-timestamped") {
        effective_clock_quality = POSSIBLY_MULTI_NODE ? "globally-comparable" : "single-node-comparable";
    }
    timeline["clockQuality"] = effective_clock_quality;
    timeline["globalOrderAvailable"] =
        MANIFEST_ORDER_ALLOWED && CROSS_LOG_ORDER_SUPPORTED && OBSERVED_CLOCK_QUALITY == "all-events-timestamped";
    timeline["manifestClockQuality"] = MANIFEST_CLOCK_QUALITY;
    timeline["orderingCeiling"] = MANIFEST_CLOCK_QUALITY;
    timeline["referencesComplete"] = log_references_complete;

    QStringList dump_paths = session->dump_ids.keys();
    std::ranges::sort(dump_paths);
    const bool COREDUMPS_TRUNCATED = dump_paths.size() > MAX_COREDUMPS;
    dump_paths = dump_paths.mid(0, MAX_COREDUMPS);
    QJsonArray coredumps;
    int remaining_analysis_hits = MAX_ISSUES;
    int analysis_hits_returned = 0;
    bool analysis_hits_truncated = false;
    for (const auto& path : dump_paths) {
        const QString dump_id = session->dump_ids[path];
        const auto* dump = find_dump_session(dump_id);
        if (dump == nullptr) {
            continue;
        }
        QJsonObject chunks;
        if (remaining_analysis_hits > 0) {
            chunks = scan_chunk_corruption(QJsonObject{{"dumpId", dump_id}, {"maxHits", remaining_analysis_hits}});
            const int CHUNK_HITS = chunks["hits"].toArray().size();
            remaining_analysis_hits -= CHUNK_HITS;
            analysis_hits_returned += CHUNK_HITS;
            analysis_hits_truncated = analysis_hits_truncated || chunks["truncated"].toBool(false);
        } else {
            chunks = QJsonObject{
                {"ok", true}, {"dumpId", dump_id}, {"truncated", true}, {"skippedBySummaryBound", true}, {"hits", QJsonArray{}}};
            analysis_hits_truncated = true;
        }
        const int CHUNK_HIT_COUNT = chunks["hits"].toArray().size();
        if (CHUNK_HIT_COUNT > 0) {
            ++analysis_issue_count;
            degraded = true;
            if (remaining_issue_budget > 0) {
                analysis_issues.append(
                    QJsonObject{{"code", "corrupt_chunk"},
                                {"path", path},
                                {"message", "captured executable bytes differ from the verified matching binary at a chunk boundary"},
                                {"fatal", false},
                                {"details", QJsonObject{{"hitCount", CHUNK_HIT_COUNT}, {"hitsTruncated", chunks["truncated"]}}}});
                --remaining_issue_budget;
            }
        }

        QJsonArray correlations;
        for (const auto& log_path : log_paths) {
            if (remaining_analysis_hits == 0) {
                analysis_hits_truncated = true;
                break;
            }
            const QJsonObject result = correlate_coredump_logs(
                QJsonObject{{"dumpId", dump_id}, {"logId", session->log_ids[log_path]}, {"maxHits", remaining_analysis_hits}});
            const QJsonArray hits = result["hits"].toArray();
            if (result["ok"].toBool(false) && !hits.isEmpty()) {
                correlations.append(QJsonObject{{"logMember", log_path}, {"hits", hits}, {"truncated", result["truncated"]}});
                const int HIT_COUNT = static_cast<int>(hits.size());
                remaining_analysis_hits -= HIT_COUNT;
                analysis_hits_returned += HIT_COUNT;
            }
            analysis_hits_truncated = analysis_hits_truncated || result["truncated"].toBool(false);
        }
        coredumps.append(QJsonObject{{"memberPath", path},
                                     {"summary", coredump_summary_to_json(*dump)},
                                     {"chunkCorruption", chunks},
                                     {"logCorrelations", correlations}});
    }

    const QJsonObject CAPTURE = session->bundle->manifest["capture"].toObject();
    degraded = degraded || !CAPTURE["complete"].toBool(true) || !CAPTURE["errors"].toArray().isEmpty() ||
               !CAPTURE["truncatedMembers"].toArray().isEmpty();
    degraded = degraded || validation["degraded"].toBool(false);
    const int VALIDATION_ISSUE_COUNT = validation["issueCount"].toInt();
    const int TOTAL_ISSUE_COUNT = VALIDATION_ISSUE_COUNT + session->load_issue_count + analysis_issue_count;
    const int ISSUES_RETURNED =
        static_cast<int>(validation["issues"].toArray().size() + bounded_load_issues.size() + analysis_issues.size());
    const bool ISSUES_TRUNCATED = TOTAL_ISSUE_COUNT > ISSUES_RETURNED;
    QJsonObject semantic{{"format", session->bundle->manifest["format"]},
                         {"formatVersion", session->bundle->manifest["version"]},
                         {"manifestIncidentId", session->bundle->incident_id()},
                         {"source", session->bundle->manifest["source"]},
                         {"capture", CAPTURE},
                         {"captureLimits", session->bundle->manifest.value("limits")},
                         {"clocks", CLOCKS},
                         {"topology", session->bundle->manifest["topology"]},
                         {"validationIssues", validation["issues"]},
                         {"loadIssues", bounded_load_issues},
                         {"analysisIssues", analysis_issues},
                         {"issueCount", TOTAL_ISSUE_COUNT},
                         {"issuesReturned", ISSUES_RETURNED},
                         {"issuesTruncated", ISSUES_TRUNCATED},
                         {"evidence", evidence},
                         {"evidenceCount", EVIDENCE_COUNT},
                         {"evidenceTruncated", EVIDENCE_TRUNCATED},
                         {"timeline", timeline},
                         {"coredumps", coredumps},
                         {"coredumpsTruncated", COREDUMPS_TRUNCATED},
                         {"analysisHitLimit", MAX_ISSUES},
                         {"analysisHitsReturned", analysis_hits_returned},
                         {"analysisHitsTruncated", analysis_hits_truncated},
                         {"loadPolicy", QJsonObject{{"loadEvidence", session->load_evidence},
                                                    {"maxLogs", session->max_logs},
                                                    {"maxCoredumps", session->max_coredumps},
                                                    {"externalSymbols", false}}},
                         {"degraded", degraded}};
    bool response_truncated = false;
    semantic = normalize_incident_value(*session, semantic, &response_truncated).toObject();
    semantic["responseTruncated"] = response_truncated;
    const QByteArray CANONICAL = QJsonDocument(semantic).toJson(QJsonDocument::Compact);
    const QString DIGEST = QString::fromLatin1(QCryptographicHash::hash(CANONICAL, QCryptographicHash::Sha256).toHex());
    QJsonObject result = validation;
    result["clockQuality"] = MANIFEST_CLOCK_QUALITY;
    result["evidence"] = semantic["evidence"];
    result["evidenceCount"] = EVIDENCE_COUNT;
    result["evidenceTruncated"] = EVIDENCE_TRUNCATED;
    result["timeline"] = semantic["timeline"];
    result["coredumps"] = semantic["coredumps"];
    result["coredumpsTruncated"] = COREDUMPS_TRUNCATED;
    result["analysisHitLimit"] = MAX_ISSUES;
    result["analysisHitsReturned"] = analysis_hits_returned;
    result["analysisHitsTruncated"] = analysis_hits_truncated;
    result["loadIssues"] = semantic["loadIssues"];
    result["loadPolicy"] = semantic["loadPolicy"];
    result["analysisIssues"] = semantic["analysisIssues"];
    result["issueCount"] = TOTAL_ISSUE_COUNT;
    result["issuesReturned"] = ISSUES_RETURNED;
    result["issuesTruncated"] = ISSUES_TRUNCATED;
    result["loadIssueCount"] = session->load_issue_count;
    result["loadIssuesTruncated"] =
        session->load_issues_truncated || session->load_issue_count > static_cast<int>(bounded_load_issues.size());
    result["degraded"] = degraded;
    result["responseTruncated"] = response_truncated;
    result["semanticProjectionVersion"] = 1;
    result["semantic"] = semantic;
    result["semanticDigest"] = "sha256:" + DIGEST;
    return result;
}

auto DebugAnalysisService::list_logs() -> QJsonObject {
    QDir const DIR(QDir::currentPath());
    QStringList files = DIR.entryList({"*.log", "*.txt"}, QDir::Files, QDir::Name);
    std::ranges::sort(files, [](const QString& a, const QString& b) {
        const bool A_MODIFIED = a.contains(".modified.");
        const bool B_MODIFIED = b.contains(".modified.");
        if (A_MODIFIED != B_MODIFIED) {
            return A_MODIFIED > B_MODIFIED;
        }
        return a < b;
    });

    QJsonArray out;
    for (const auto& file : files) {
        QFileInfo const INFO(DIR.absoluteFilePath(file));
        out.append(QJsonObject{{"name", file}, {"path", INFO.absoluteFilePath()}, {"size", QString::number(INFO.size())}});
    }
    return QJsonObject{{"ok", true}, {"logs", out}};
}

auto DebugAnalysisService::load_log(const QJsonObject& args) -> QJsonObject {
    QString const PATH = args["path"].toString(args["file"].toString());
    if (PATH.isEmpty()) {
        return tool_error("load_log requires 'path' or 'file'");
    }

    QString const RESOLVED = resolve_path_for_read(PATH);
    if (!QFileInfo::exists(RESOLVED)) {
        return tool_error(QString("Log file not found: %1").arg(PATH));
    }
    if (!is_path_allowed(RESOLVED)) {
        return tool_error(QString("Log path is outside allowed roots: %1").arg(RESOLVED));
    }

    return load_log_file(RESOLVED, make_session_id("log", RESOLVED), args["timeoutMs"].toInt(120000));
}

auto DebugAnalysisService::load_log_file(const QString& resolved_path, const QString& session_id, int timeout_ms,
                                         bool allow_external_symbols) -> QJsonObject {
    const QString& RESOLVED = resolved_path;
    const QString& id = session_id;
    if (log_sessions.contains(id)) {
        return QJsonObject{{"ok", true}, {"logId", id}, {"cached", true}, {"summary", log_summary_to_json(*log_sessions[id])}};
    }

    auto session = std::make_shared<LogSession>();
    session->id = id;
    session->path = RESOLVED;
    session->display_name = QFileInfo(RESOLVED).fileName();

    LogProcessor processor(RESOLVED);
    if (allow_external_symbols) {
        processor.set_config_path(QDir::current().absoluteFilePath("wosdbg.json"));
    }

    QEventLoop loop;
    QString error;
    connect(&processor, &LogProcessor::processing_complete, &loop, &QEventLoop::quit);
    connect(&processor, &LogProcessor::error_occurred, &loop, [&](const QString& message) {
        error = message;
        loop.quit();
    });

    QTimer timeout;
    timeout.setSingleShot(true);
    connect(&timeout, &QTimer::timeout, &loop, [&]() {
        error = "Timed out while parsing log";
        loop.quit();
    });
    timeout.start(std::clamp(timeout_ms, 1000, 600000));
    processor.start_processing();
    loop.exec();

    if (!error.isEmpty()) {
        return tool_error(error);
    }

    session->entries = processor.get_entries();
    auto* saved = session.get();
    log_sessions.insert(id, session);
    return QJsonObject{{"ok", true}, {"logId", id}, {"cached", false}, {"summary", log_summary_to_json(*saved)}};
}

auto DebugAnalysisService::find_log_session(const QString& id) const -> const LogSession* {
    auto it = log_sessions.find(id);
    return it == log_sessions.end() ? nullptr : it.value().get();
}

auto DebugAnalysisService::log_entry_to_json(const LogEntry& entry, bool include_children) const -> QJsonObject {
    QJsonObject obj{{"lineNumber", entry.line_number},
                    {"type", entry_type_name(entry.type)},
                    {"address", QString::fromStdString(entry.address)},
                    {"addressValue", entry.address_value == 0 ? QString() : format_hex(entry.address_value)},
                    {"function", QString::fromStdString(entry.function)},
                    {"hexBytes", QString::fromStdString(entry.hex_bytes)},
                    {"assembly", QString::fromStdString(entry.assembly)},
                    {"originalLine", QString::fromStdString(entry.original_line)},
                    {"sourceFile", QString::fromStdString(entry.source_file)},
                    {"sourceLine", entry.source_line},
                    {"interruptNumber", QString::fromStdString(entry.interrupt_number)},
                    {"cpuStateInfo", QString::fromStdString(entry.cpu_state_info)}};
    if (include_children && !entry.child_entries.empty()) {
        QJsonArray children;
        for (const auto& child : entry.child_entries) {
            children.append(log_entry_to_json(child, false));
        }
        obj["children"] = children;
    }
    return obj;
}

auto DebugAnalysisService::log_summary_to_json(const LogSession& session) -> QJsonObject {
    int instructions = 0;
    int interrupts = 0;
    for (const auto& entry : session.entries) {
        if (entry.type == EntryType::INSTRUCTION) {
            ++instructions;
        } else if (entry.type == EntryType::INTERRUPT) {
            ++interrupts;
        }
    }
    return QJsonObject{{"id", session.id},
                       {"path", session.path},
                       {"entries", static_cast<int>(session.entries.size())},
                       {"instructions", instructions},
                       {"interrupts", interrupts}};
}

int DebugAnalysisService::bounded_int(const QJsonObject& args, const QString& key, int fallback, int min_value, int max_value) {
    int const VALUE = args[key].toInt(fallback);
    return std::clamp(VALUE, min_value, max_value);
}

auto DebugAnalysisService::get_log_entries(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_log_session(args["logId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown logId");
    }
    const Config& cfg = (config != nullptr) ? *config : ConfigService::instance().get_config();
    int start = bounded_int(args, "start", 0, 0, static_cast<int>(session->entries.size()));
    int const COUNT = bounded_int(args, "count", 50, 1, cfg.get_mcp_settings().max_entries);
    int const END = std::min(start + COUNT, static_cast<int>(session->entries.size()));

    QJsonArray entries;
    for (int i = start; i < END; ++i) {
        entries.append(log_entry_to_json(session->entries[static_cast<size_t>(i)]));
    }
    return QJsonObject{{"ok", true},
                       {"logId", session->id},
                       {"start", start},
                       {"count", entries.size()},
                       {"total", static_cast<int>(session->entries.size())},
                       {"entries", entries}};
}

auto DebugAnalysisService::search_log(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_log_session(args["logId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown logId");
    }
    QString const QUERY = args["query"].toString();
    if (QUERY.isEmpty()) {
        return tool_error("search_log requires 'query'");
    }

    const Config& cfg = (config != nullptr) ? *config : ConfigService::instance().get_config();
    int const MAX_HITS = bounded_int(args, "maxHits", cfg.get_mcp_settings().max_hits, 1, cfg.get_mcp_settings().max_hits);
    int const START_ROW = bounded_int(args, "start", 0, 0, static_cast<int>(session->entries.size()));

    QRegularExpression regex;
    if (args["regex"].toBool(false)) {
        regex = QRegularExpression(QUERY, QRegularExpression::CaseInsensitiveOption);
    } else {
        regex = QRegularExpression(QRegularExpression::escape(QUERY), QRegularExpression::CaseInsensitiveOption);
    }
    if (!regex.isValid()) {
        return tool_error("Invalid regex");
    }

    QJsonArray hits;
    bool truncated = false;
    for (int i = START_ROW; std::cmp_less(i, session->entries.size()); ++i) {
        const auto& entry = session->entries[static_cast<size_t>(i)];
        QString const COMBINED = QString::fromStdString(entry.address + " " + entry.function + " " + entry.assembly + " " +
                                                        entry.original_line + " " + entry.source_file);
        if (!regex.match(COMBINED).hasMatch()) {
            continue;
        }
        hits.append(QJsonObject{{"row", i}, {"entry", log_entry_to_json(entry, false)}});
        if (hits.size() >= MAX_HITS) {
            truncated = (i + 1) < static_cast<int>(session->entries.size());
            break;
        }
    }
    return QJsonObject{{"ok", true}, {"logId", session->id}, {"hits", hits}, {"truncated", truncated}};
}

auto DebugAnalysisService::get_log_context(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_log_session(args["logId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown logId");
    }
    int row = args["row"].toInt(-1);
    if (row < 0 && args.contains("line")) {
        int const LINE = args["line"].toInt();
        for (int i = 0; std::cmp_less(i, session->entries.size()); ++i) {
            if (session->entries[static_cast<size_t>(i)].line_number == LINE) {
                row = i;
                break;
            }
        }
    }
    if (row < 0 && args.contains("address")) {
        auto addr = parse_address_value(args["address"]);
        if (addr) {
            for (int i = 0; std::cmp_less(i, session->entries.size()); ++i) {
                if (session->entries[static_cast<size_t>(i)].address_value == *addr) {
                    row = i;
                    break;
                }
            }
        }
    }
    if (row < 0 || std::cmp_greater_equal(row, session->entries.size())) {
        return tool_error("No matching log row");
    }

    int const BEFORE = bounded_int(args, "before", 8, 0, 100);
    int const AFTER = bounded_int(args, "after", 16, 0, 100);
    int start = std::max(0, row - BEFORE);
    int const END = std::min(static_cast<int>(session->entries.size()), row + AFTER + 1);
    QJsonArray entries;
    for (int i = start; i < END; ++i) {
        entries.append(QJsonObject{{"row", i}, {"entry", log_entry_to_json(session->entries[static_cast<size_t>(i)])}});
    }
    return QJsonObject{{"ok", true}, {"logId", session->id}, {"row", row}, {"start", start}, {"entries", entries}};
}

auto DebugAnalysisService::extract_coredumps(const QJsonObject& args) -> QJsonObject {
    QString script_path;
    const QStringList CANDIDATES = {QDir::current().absoluteFilePath("bin/wos-extract-coredumps"),
                                    QDir::current().absoluteFilePath("../bin/wos-extract-coredumps"),
                                    QDir::current().absoluteFilePath("scripts/debug/extract_coredumps.sh"),
                                    QDir::current().absoluteFilePath("../scripts/debug/extract_coredumps.sh")};
    for (const auto& candidate : CANDIDATES) {
        if (QFileInfo::exists(candidate)) {
            script_path = candidate;
            break;
        }
    }
    if (script_path.isEmpty()) {
        return tool_error("wos-extract-coredumps not found");
    }
    if (!is_path_allowed(QFileInfo(script_path).absolutePath())) {
        return tool_error("Extraction script is outside allowed roots");
    }

    QStringList process_args;
    process_args << script_path;
    if (args["cluster"].toBool(true)) {
        process_args << "--cluster";
    }

    QProcess process;
    process.setWorkingDirectory(QDir::currentPath());
    process.start("bash", process_args);
    if (!process.waitForStarted(5000)) {
        return tool_error("Failed to start coredump extraction");
    }
    const int TIMEOUT_MS = args["timeoutMs"].toInt(300000);
    if (!process.waitForFinished(TIMEOUT_MS)) {
        process.kill();
        process.waitForFinished(3000);
        return tool_error("Timed out while extracting coredumps");
    }

    QByteArray const STDOUT_BYTES = process.readAllStandardOutput().left(K_MAX_CAPTURED_PROCESS_OUTPUT);
    QByteArray const STDERR_BYTES = process.readAllStandardError().left(K_MAX_CAPTURED_PROCESS_OUTPUT);
    QJsonObject listed = list_coredumps();
    return QJsonObject{{"ok", process.exitStatus() == QProcess::NormalExit && process.exitCode() == 0},
                       {"exitCode", process.exitCode()},
                       {"stdout", QString::fromUtf8(STDOUT_BYTES)},
                       {"stderr", QString::fromUtf8(STDERR_BYTES)},
                       {"coredumps", listed["coredumps"].toArray()}};
}

auto DebugAnalysisService::list_coredumps() const -> QJsonObject {
    const Config& cfg = (config != nullptr) ? *config : ConfigService::instance().get_config();
    QDir const ROOT(cfg.get_coredump_directory());
    QJsonArray dumps;
    if (!ROOT.exists()) {
        return QJsonObject{{"ok", true}, {"coredumpDirectory", ROOT.absolutePath()}, {"coredumps", dumps}};
    }

    QDirIterator it(ROOT.absolutePath(), {"*_coredump.bin"}, QDir::Files, QDirIterator::Subdirectories);
    while (it.hasNext()) {
        it.next();
        QFileInfo const INFO = it.fileInfo();
        QJsonObject item{{"path", INFO.absoluteFilePath()},
                         {"name", INFO.fileName()},
                         {"size", QString::number(INFO.size())},
                         {"binary", wosdbg::parse_binary_name_from_filename(INFO.fileName())}};
        QFile file(INFO.absoluteFilePath());
        if (file.open(QIODevice::ReadOnly)) {
            auto parsed = wosdbg::parse_core_dump(file.readAll());
            if (parsed) {
                item["pid"] = QString::number(parsed->pid);
                item["cpu"] = QString::number(parsed->cpu);
                item["interrupt"] = QString::number(parsed->int_num);
                item["interruptName"] = wosdbg::interrupt_name(parsed->int_num);
                item["rip"] = format_hex(parsed->trap_frame.rip);
                item["cr2"] = format_hex(parsed->cr2);
                item["timestamp"] = QString::number(parsed->timestamp);
            }
        }
        dumps.append(item);
    }
    return QJsonObject{{"ok", true}, {"coredumpDirectory", ROOT.absolutePath()}, {"coredumps", dumps}};
}

auto DebugAnalysisService::open_coredump(const QJsonObject& args) -> QJsonObject {
    QString const PATH = args["path"].toString(args["file"].toString());
    if (PATH.isEmpty()) {
        return tool_error("open_coredump requires 'path' or 'file'");
    }

    const Config& cfg = (config != nullptr) ? *config : ConfigService::instance().get_config();
    QString const RESOLVED = resolve_path_for_read(PATH, cfg.get_coredump_directory());
    if (!QFileInfo::exists(RESOLVED)) {
        return tool_error(QString("Coredump not found: %1").arg(PATH));
    }
    if (!is_path_allowed(RESOLVED)) {
        return tool_error(QString("Coredump path is outside allowed roots: %1").arg(RESOLVED));
    }

    auto resolve_optional_elf = [&](const QString& raw, const char* label) -> std::optional<QString> {
        if (raw.isEmpty()) {
            return QString{};
        }
        const QString resolved = resolve_path_for_read(raw);
        if (!QFileInfo::exists(resolved)) {
            return std::nullopt;
        }
        if (!is_path_allowed(resolved)) {
            return std::nullopt;
        }
        (void)label;
        return resolved;
    };
    const QString BINARY_ARGUMENT = args["elfPath"].toString(args["binaryElfPath"].toString());
    const QString KERNEL_ARGUMENT = args["kernelElfPath"].toString();
    const auto BINARY_ELF = resolve_optional_elf(BINARY_ARGUMENT, "binary ELF");
    const auto KERNEL_ELF = resolve_optional_elf(KERNEL_ARGUMENT, "kernel ELF");
    if (!BINARY_ELF) {
        return tool_error(QString("Binary ELF path is missing or outside allowed roots: %1").arg(BINARY_ARGUMENT));
    }
    if (!KERNEL_ELF) {
        return tool_error(QString("Kernel ELF path is missing or outside allowed roots: %1").arg(KERNEL_ARGUMENT));
    }

    return open_coredump_file(RESOLVED, make_session_id("dump", RESOLVED), *BINARY_ELF, *KERNEL_ELF);
}

auto DebugAnalysisService::open_coredump_file(const QString& resolved_path, const QString& session_id, const QString& binary_elf_path,
                                              const QString& kernel_elf_path, bool allow_external_discovery) -> QJsonObject {
    const QString& RESOLVED = resolved_path;
    const QString& id = session_id;
    const Config& cfg = (config != nullptr) ? *config : ConfigService::instance().get_config();
    if (dump_sessions.contains(id)) {
        return QJsonObject{{"ok", true}, {"dumpId", id}, {"cached", true}, {"summary", coredump_summary_to_json(*dump_sessions[id])}};
    }

    auto dump = wosdbg::parse_core_dump(RESOLVED);
    if (!dump) {
        return tool_error("Failed to parse coredump");
    }

    auto session = std::make_shared<DumpSession>();
    session->id = id;
    session->path = RESOLVED;
    session->display_name = QFileInfo(RESOLVED).fileName();
    session->dump = std::move(dump);

    if (!session->dump->embedded_elf().isEmpty()) {
        session->embedded_build_id = wosdbg::elf_build_id(session->dump->embedded_elf());
        session->embedded_symbols = wosdbg::load_symbols_from_core_dump(*session->dump);
        session->embedded_sections = wosdbg::load_sections_from_core_dump(*session->dump);
    }

    QString const BINARY_NAME = wosdbg::parse_binary_name_from_filename(session->dump->source_filename);
    QString elf_path = binary_elf_path;
    if (elf_path.isEmpty() && allow_external_discovery) {
        elf_path = cfg.find_elf_path_for_binary(BINARY_NAME);
    }
    if (elf_path.isEmpty() && allow_external_discovery) {
        elf_path = first_existing_candidate(default_elf_candidates(session->dump->exe_path));
    }
    if (elf_path.isEmpty() && allow_external_discovery && !session->embedded_build_id.isEmpty()) {
        elf_path = first_existing_candidate(build_id_debug_candidates(session->embedded_build_id));
    }
    if (elf_path.isEmpty() && allow_external_discovery && !session->embedded_build_id.isEmpty()) {
        elf_path = first_candidate_with_build_id(default_elf_candidates(session->dump->exe_path), session->embedded_build_id);
    }
    if (!elf_path.isEmpty()) {
        session->binary_elf_path = elf_path;
        session->binary_symbols = wosdbg::load_symbols_from_file(elf_path);
        session->binary_sections = wosdbg::load_sections_from_file(elf_path);
        session->binary_build_id = wosdbg::elf_build_id_from_file(elf_path);
        session->binary_build_id_status = "embedded-build-id-missing";
    }
    if (!session->binary_elf_path.isEmpty() && session->binary_build_id.isEmpty()) {
        session->binary_build_id_status = "binary-build-id-missing";
    } else if (!session->binary_build_id.isEmpty() && !session->embedded_build_id.isEmpty()) {
        session->binary_build_id_matches = session->binary_build_id == session->embedded_build_id;
        session->binary_build_id_status = session->binary_build_id_matches ? "match" : "mismatch";
        if (!session->binary_build_id_matches) {
            session->symbol_warning = QString("local binary build ID %1 does not match coredump embedded build ID %2")
                                          .arg(session->binary_build_id, session->embedded_build_id);
            const QString BUILD_ID_ELF_PATH =
                allow_external_discovery ? first_existing_candidate(build_id_debug_candidates(session->embedded_build_id)) : QString{};
            const QString FALLBACK_ELF_PATH =
                allow_external_discovery && BUILD_ID_ELF_PATH.isEmpty()
                    ? first_candidate_with_build_id(default_elf_candidates(session->dump->exe_path), session->embedded_build_id)
                    : BUILD_ID_ELF_PATH;
            if (!FALLBACK_ELF_PATH.isEmpty()) {
                session->binary_elf_path = FALLBACK_ELF_PATH;
                session->binary_symbols = wosdbg::load_symbols_from_file(FALLBACK_ELF_PATH);
                session->binary_sections = wosdbg::load_sections_from_file(FALLBACK_ELF_PATH);
                session->binary_build_id = wosdbg::elf_build_id_from_file(FALLBACK_ELF_PATH);
                session->binary_build_id_matches = session->binary_build_id == session->embedded_build_id;
                session->binary_build_id_status = session->binary_build_id_matches ? "match" : "mismatch";
                session->symbol_warning =
                    QString("configured binary build ID mismatched; using build-id matched ELF %1").arg(FALLBACK_ELF_PATH);
            }
        }
    }
    if (!kernel_elf_path.isEmpty()) {
        session->kernel_elf_path = kernel_elf_path;
        session->kernel_symbols = wosdbg::load_symbols_from_file(kernel_elf_path);
        session->kernel_sections = wosdbg::load_sections_from_file(kernel_elf_path);
        session->kernel_build_id = wosdbg::elf_build_id_from_file(kernel_elf_path);
    } else if (allow_external_discovery) {
        for (const auto& lookup : cfg.get_address_lookups()) {
            if (lookup.symbol_file_path.contains("kern") || lookup.symbol_file_path.contains("wos")) {
                QString const KERNEL_PATH = cfg.resolve_path(lookup.symbol_file_path);
                session->kernel_elf_path = KERNEL_PATH;
                session->kernel_symbols = wosdbg::load_symbols_from_file(KERNEL_PATH);
                session->kernel_sections = wosdbg::load_sections_from_file(KERNEL_PATH);
                session->kernel_build_id = wosdbg::elf_build_id_from_file(KERNEL_PATH);
                break;
            }
        }
    }
    discover_modules(*session, allow_external_discovery);
    if (session->binary_build_id_status == "mismatch") {
        session->symbol_status = "build-id-mismatch";
    } else if (session->binary_symbols) {
        session->symbol_status = "available";
    } else if (session->embedded_symbols) {
        session->symbol_status = "embedded-only";
    } else if (!session->binary_elf_path.isEmpty()) {
        session->symbol_status = "binary-has-no-symbols";
    }

    auto* saved = session.get();
    dump_sessions.insert(id, session);
    return QJsonObject{{"ok", true}, {"dumpId", id}, {"cached", false}, {"summary", coredump_summary_to_json(*saved)}};
}

auto DebugAnalysisService::find_dump_session(const QString& id) const -> const DumpSession* {
    auto it = dump_sessions.find(id);
    return it == dump_sessions.end() ? nullptr : it.value().get();
}

auto DebugAnalysisService::find_dump_session(const QString& id) -> DumpSession* {
    auto it = dump_sessions.find(id);
    return it == dump_sessions.end() ? nullptr : it.value().get();
}

auto DebugAnalysisService::module_json(const DumpSession::LoadedModule& module) -> QJsonObject {
    return QJsonObject{{"name", module.name},
                       {"path", module.path},
                       {"role", module.role},
                       {"base", format_hex(module.base)},
                       {"end", format_hex(module.end)},
                       {"objectStart", format_hex(module.base + module.first_load_vaddr)},
                       {"buildId", module.build_id},
                       {"addressModel", module.address_model},
                       {"alternatePaths", QJsonArray::fromStringList(module.alternate_paths)},
                       {"memoryMatched", module.memory_matched},
                       {"symbols", module.symbols ? module.symbols->count() : 0}};
}

auto DebugAnalysisService::module_elf_bytes(const DumpSession& session, const DumpSession::LoadedModule& module) -> QByteArray {
    if (module.path.startsWith("embedded:")) {
        return session.dump->embedded_elf();
    }
    return read_file_bytes_quiet(module.path);
}

void DebugAnalysisService::add_module(DumpSession& session, const QString& path, const QString& role, uint64_t base, bool memory_matched) {
    QString resolved = path;
    if (!resolved.startsWith("embedded:")) {
        resolved = resolve_path_for_read(path);
        if (resolved.isEmpty() || !QFileInfo::exists(resolved)) {
            return;
        }
    }
    QByteArray const ELF = resolved.startsWith("embedded:") ? session.dump->embedded_elf() : read_file_bytes_quiet(resolved);
    wosdbg::ElfImageInfo const INFO = wosdbg::elf_image_info(ELF);
    if (!INFO.valid || INFO.load_segments.empty()) {
        return;
    }
    const uint64_t BIAS = INFO.type == 3 ? base : 0;
    const QString BUILD_ID = wosdbg::elf_build_id(ELF);
    uint64_t first = UINT64_MAX;
    uint64_t end = 0;
    for (const auto& load : INFO.load_segments) {
        first = std::min(first, load.vaddr);
        end = std::max(end, BIAS + load.vaddr + load.memsz);
    }
    if (first == UINT64_MAX || end == 0) {
        return;
    }
    const QString NAME = resolved.startsWith("embedded:") ? QFileInfo(session.dump->exe_path).fileName() : QFileInfo(resolved).fileName();
    for (auto& existing : session.modules) {
        const bool SAME_BUILD = !BUILD_ID.isEmpty() && existing.build_id == BUILD_ID && existing.base == BIAS;
        const bool SAME_IDENTITY = existing.name == NAME && existing.role == role && existing.base == BIAS;
        if (SAME_BUILD || SAME_IDENTITY || (existing.path == resolved && existing.base == BIAS)) {
            if (existing.path != resolved && !existing.alternate_paths.contains(resolved)) {
                existing.alternate_paths << resolved;
            }
            existing.memory_matched = existing.memory_matched || memory_matched;
            return;
        }
    }
    DumpSession::LoadedModule module;
    module.name = NAME.isEmpty() ? role : NAME;
    module.path = resolved;
    module.role = role;
    module.base = BIAS;
    module.end = end;
    module.first_load_vaddr = first;
    module.build_id = BUILD_ID;
    module.address_model = INFO.type == 3 ? "runtime = elf_vaddr + load_base" : "absolute ELF virtual addresses";
    if (role == "kernel") {
        module.address_model = "absolute linked kernel virtual addresses";
    }
    module.memory_matched = memory_matched;
    module.symbols = wosdbg::parse_elf_symtab(ELF, BIAS);
    module.sections = wosdbg::parse_elf_sections(ELF, BIAS);
    session.modules.push_back(std::move(module));
}

void DebugAnalysisService::discover_modules(DumpSession& session, bool allow_external_discovery) {
    QStringList candidates;
    auto append_candidate = [&](const QString& path) {
        QString const CLEAN = canonical_path_or_absolute(path);
        if (!candidates.contains(CLEAN)) {
            candidates << CLEAN;
        }
    };
    if (!session.binary_elf_path.isEmpty()) {
        append_candidate(session.binary_elf_path);
    }
    if (allow_external_discovery) {
        for (const auto& candidate : default_elf_candidates(session.dump->exe_path)) {
            if (QFileInfo::exists(candidate)) {
                append_candidate(candidate);
            }
        }
        if (!session.embedded_build_id.isEmpty()) {
            for (const auto& candidate : build_id_debug_candidates(session.embedded_build_id)) {
                if (QFileInfo::exists(candidate)) {
                    append_candidate(candidate);
                }
            }
        }
    }

    if (!session.dump->embedded_elf().isEmpty()) {
        add_module(session, "embedded:main", "main", 0, true);
    }

    for (const auto& path : std::as_const(candidates)) {
        QByteArray const ELF = read_file_bytes_quiet(path);
        wosdbg::ElfImageInfo info = wosdbg::elf_image_info(ELF);
        if (!info.valid || info.load_segments.empty()) {
            continue;
        }
        uint64_t base = 0;
        bool matched = false;
        const QString NAME = QFileInfo(path).fileName();
        QString role = NAME;
        if (NAME == "wos") {
            role = "kernel";
        } else if (NAME == "ld.so") {
            role = "interpreter";
            base = session.dump->interp_base;
        } else if (NAME == "libc.so") {
            role = "libc";
        } else if (path == session.binary_elf_path) {
            role = "main";
        }
        if (info.type == 3) {
            if (base == 0) {
                if (auto discovered = find_runtime_base_by_bytes(*session.dump, ELF, info)) {
                    base = *discovered;
                    matched = true;
                } else {
                    continue;
                }
            } else {
                matched = !wosdbg::elf_bytes_at_runtime_va(ELF, base + info.load_segments.front().vaddr, base, 16).empty();
            }
        }
        add_module(session, path, role, base, matched);
    }
    std::ranges::sort(session.modules, [](const DumpSession::LoadedModule& a, const DumpSession::LoadedModule& b) {
        return a.base + a.first_load_vaddr < b.base + b.first_load_vaddr;
    });
}

auto DebugAnalysisService::symbol_tables(const DumpSession& session) -> std::vector<wosdbg::SymbolTable*> {
    std::vector<wosdbg::SymbolTable*> tables;
    for (const auto& module : session.modules) {
        if (module.symbols) {
            tables.push_back(module.symbols.get());
        }
    }
    if (session.binary_symbols) {
        tables.push_back(session.binary_symbols.get());
    }
    if (session.embedded_symbols) {
        tables.push_back(session.embedded_symbols.get());
    }
    if (session.kernel_symbols) {
        tables.push_back(session.kernel_symbols.get());
    }
    return tables;
}

auto DebugAnalysisService::section_maps(const DumpSession& session) -> std::vector<wosdbg::SectionMap*> {
    std::vector<wosdbg::SectionMap*> maps;
    for (const auto& module : session.modules) {
        if (module.sections) {
            maps.push_back(module.sections.get());
        }
    }
    if (session.binary_sections) {
        maps.push_back(session.binary_sections.get());
    }
    if (session.embedded_sections) {
        maps.push_back(session.embedded_sections.get());
    }
    if (session.kernel_sections) {
        maps.push_back(session.kernel_sections.get());
    }
    return maps;
}

auto DebugAnalysisService::module_for_address(const DumpSession& session, uint64_t address) -> const DumpSession::LoadedModule* {
    for (const auto& module : session.modules) {
        const uint64_t START = module.base + module.first_load_vaddr;
        if (address >= START && address < module.end) {
            return &module;
        }
    }
    return nullptr;
}

auto DebugAnalysisService::source_location_for_address(const DumpSession& session, uint64_t address,
                                                       const DumpSession::LoadedModule* module) -> QJsonObject {
    if (module == nullptr) {
        module = module_for_address(session, address);
    }
    if ((module == nullptr) || module->path.startsWith("embedded:")) {
        return {};
    }
    const uint64_t OBJECT_OFFSET = address >= module->base ? address - module->base : address;
    return source_with_llvm_symbolizer(module->path, OBJECT_OFFSET);
}

auto DebugAnalysisService::coredump_summary_to_json(const DumpSession& session) -> QJsonObject {
    const auto& dump = *session.dump;
    QJsonArray modules;
    for (const auto& module : session.modules) {
        modules.append(module_json(module));
    }
    QJsonObject task_info{{"taskPtr", format_hex(dump.task_ptr)},
                          {"threadPtr", format_hex(dump.thread_ptr)},
                          {"pid", QString::number(dump.pid)},
                          {"parentPid", QString::number(dump.parent_pid)},
                          {"ownerPid", QString::number(dump.owner_pid)},
                          {"remotePid", QString::number(dump.wki_remote_pid)},
                          {"sessionId", QString::number(dump.session_id)},
                          {"processGroupId", QString::number(dump.pgid)},
                          {"type", task_type_name(dump.task_type)},
                          {"state", task_state_name(dump.task_state)},
                          {"schedQueue", task_sched_queue_name(dump.sched_queue)},
                          {"currentCpu", QString::number(dump.current_cpu)},
                          {"domainId", QString::number(dump.domain_id)},
                          {"domainMask", format_hex(dump.domain_mask)},
                          {"wkiTargetFlags", format_hex(dump.wki_target_flags)},
                          {"wkiProxyTaskId", QString::number(dump.wki_proxy_task_id)},
                          {"wkiTargetHostname", dump.wki_target_hostname},
                          {"wkiSubmitterHostname", dump.wki_submitter_hostname},
                          {"elfBuffer", QJsonObject{{"address", format_hex(dump.elf_buffer_addr)},
                                                    {"size", QString::number(dump.captured_elf_buffer_size)},
                                                    {"shared", dump.elf_buffer_shared}}},
                          {"timing", QJsonObject{{"startTimeUs", QString::number(dump.start_time_us)},
                                                 {"userTimeUs", QString::number(dump.user_time_us)},
                                                 {"systemTimeUs", QString::number(dump.system_time_us)},
                                                 {"vruntime", QString::number(dump.vruntime)},
                                                 {"vdeadline", QString::number(dump.vdeadline)},
                                                 {"wakeAtUs", QString::number(dump.wake_at_us)}}},
                          {"wait", QJsonObject{{"channel", dump.wait_channel},
                                               {"channelAddress", format_hex(dump.wait_channel_addr)},
                                               {"waitingForPid", QString::number(dump.waiting_for_pid)},
                                               {"statusUserAddress", format_hex(dump.wait_status_user_addr)},
                                               {"rusageUserAddress", format_hex(dump.wait_rusage_user_addr)}}},
                          {"signals", QJsonObject{{"pending", format_hex(dump.sig_pending)}, {"mask", format_hex(dump.sig_mask)}}},
                          {"ptrace", QJsonObject{{"tracerPid", QString::number(dump.ptrace_tracer_pid)}}},
                          {"credentials", QJsonObject{{"uid", QString::number(dump.uid)},
                                                      {"gid", QString::number(dump.gid)},
                                                      {"euid", QString::number(dump.euid)},
                                                      {"egid", QString::number(dump.egid)}}},
                          {"flags", task_flags_to_json(dump.task_flags)}};
    return QJsonObject{{"id", session.id},
                       {"path", session.path},
                       {"pid", QString::number(dump.pid)},
                       {"cpu", QString::number(dump.cpu)},
                       {"interrupt", QString::number(dump.int_num)},
                       {"interruptName", wosdbg::interrupt_name(dump.int_num)},
                       {"errorCode", format_hex(dump.err_code)},
                       {"cr2", format_hex(dump.cr2)},
                       {"cr3", format_hex(dump.cr3)},
                       {"trapRip", format_hex(dump.trap_frame.rip)},
                       {"trapRsp", format_hex(dump.trap_frame.rsp)},
                       {"savedRip", format_hex(dump.saved_frame.rip)},
                       {"exePath", dump.exe_path},
                       {"cwd", dump.cwd},
                       {"root", dump.root},
                       {"task", task_info},
                       {"segments", static_cast<int>(dump.segments.size())},
                       {"embeddedElfBytes", QString::number(dump.elf_size)},
                       {"symbols", QJsonObject{{"binaryElfPath", session.binary_elf_path},
                                               {"binaryBuildId", session.binary_build_id},
                                               {"embeddedBuildId", session.embedded_build_id},
                                               {"binaryBuildIdMatches", session.binary_build_id_matches},
                                               {"buildIdStatus", session.binary_build_id_status},
                                               {"symbolStatus", session.symbol_status},
                                               {"symbolWarning", session.symbol_warning},
                                               {"kernelElfPath", session.kernel_elf_path},
                                               {"kernelBuildId", session.kernel_build_id},
                                               {"binarySymbols", session.binary_symbols ? session.binary_symbols->count() : 0},
                                               {"embeddedSymbols", session.embedded_symbols ? session.embedded_symbols->count() : 0},
                                               {"kernelSymbols", session.kernel_symbols ? session.kernel_symbols->count() : 0}}},
                       {"modules", modules}};
}

auto DebugAnalysisService::parse_address_value(const QJsonValue& value) -> std::optional<uint64_t> {
    if (value.isString()) {
        QString const TEXT = value.toString().trimmed();
        bool ok = false;
        uint64_t parsed = TEXT.startsWith("0x", Qt::CaseInsensitive) ? TEXT.toULongLong(&ok, 16) : TEXT.toULongLong(&ok, 10);
        if (ok) {
            return parsed;
        }
        return std::nullopt;
    }
    if (value.isDouble()) {
        double d = value.toDouble();
        if (d >= 0) {
            return static_cast<uint64_t>(d);
        }
    }
    return std::nullopt;
}

auto DebugAnalysisService::register_map(const wosdbg::CoreDump& dump, const QString& frame) -> QHash<QString, uint64_t> {
    const bool SAVED = frame.compare("saved", Qt::CaseInsensitive) == 0;
    const auto& f = SAVED ? dump.saved_frame : dump.trap_frame;
    const auto& r = SAVED ? dump.saved_regs : dump.trap_regs;
    QHash<QString, uint64_t> regs;
    regs.insert("rip", f.rip);
    regs.insert("rsp", f.rsp);
    regs.insert("cs", f.cs);
    regs.insert("ss", f.ss);
    regs.insert("rflags", f.rflags);
    regs.insert("rax", r.rax);
    regs.insert("rbx", r.rbx);
    regs.insert("rcx", r.rcx);
    regs.insert("rdx", r.rdx);
    regs.insert("rsi", r.rsi);
    regs.insert("rdi", r.rdi);
    regs.insert("rbp", r.rbp);
    regs.insert("r8", r.r8);
    regs.insert("r9", r.r9);
    regs.insert("r10", r.r10);
    regs.insert("r11", r.r11);
    regs.insert("r12", r.r12);
    regs.insert("r13", r.r13);
    regs.insert("r14", r.r14);
    regs.insert("r15", r.r15);
    regs.insert("cr2", dump.cr2);
    regs.insert("cr3", dump.cr3);
    regs.insert("fs_base", dump.thread_fs_base);
    regs.insert("gs_base", dump.thread_gs_base);
    return regs;
}

auto DebugAnalysisService::ascii_preview(const QByteArray& bytes, int max_len) -> QString {
    QString out;
    int const LIMIT = std::min(static_cast<int>(bytes.size()), max_len);
    for (int i = 0; i < LIMIT; ++i) {
        const auto CH = static_cast<unsigned char>(bytes[i]);
        if (CH == 0) {
            break;
        }
        out += (CH >= 32 && CH < 127) ? QChar(CH) : QChar('.');
    }
    return out;
}

auto DebugAnalysisService::qword_preview(const wosdbg::CoreDump& dump, uint64_t address, int qwords) -> QJsonArray {
    QJsonArray out;
    QByteArray const BYTES = wosdbg::read_va_bytes(dump, address, static_cast<size_t>(qwords) * 8);
    for (int i = 0; i < qwords && (i * 8) + 7 < BYTES.size(); ++i) {
        auto const VALUE = read_u64_le(BYTES.constData() + (i * 8ULL));
        out.append(QJsonObject{{"address", format_hex(address + static_cast<uint64_t>(i * 8))}, {"value", format_hex(VALUE)}});
    }
    return out;
}

auto DebugAnalysisService::describe_address(const DumpSession& session, uint64_t value, const QString& register_name) const
    -> AddressDescription {
    const auto& dump = *session.dump;
    AddressDescription desc;
    desc.value = value;
    desc.hex = format_hex(value);
    desc.canonical = is_canonical_x86_address(value);
    desc.confidence = "heuristic";
    const QString REG = register_name.toLower();

    if (REG == "rflags") {
        desc.classification = "flags register";
        desc.confidence = "certain";
        desc.description = "RFLAGS bitfield, not an address";
        return desc;
    }
    if (REG == "cs" || REG == "ss") {
        desc.classification = "segment selector";
        desc.confidence = "certain";
        desc.description = "x86 segment selector, not an address";
        return desc;
    }
    if (REG == "cr3") {
        desc.classification = "page-table root";
        desc.confidence = "certain";
        desc.description = "CR3 physical page-table root";
        return desc;
    }
    if (REG == "fs_base" || REG == "gs_base") {
        desc.classification = "TLS base";
        desc.confidence = "certain";
    }

    if (value == 0) {
        desc.classification = "null";
        desc.confidence = "certain";
        desc.description = "zero/null value";
        return desc;
    }
    if (value < 0x1000) {
        desc.classification = REG == "cr2" ? "low unmapped address" : "small integer";
        desc.confidence = "certain";
        desc.description = REG == "cr2" ? "low unmapped fault address; often a corrupted pointer rather than a literal null dereference"
                                        : "small scalar-looking value, not a usable pointer";
        return desc;
    }
    if (!desc.canonical) {
        desc.classification = "invalid/non-canonical";
        desc.confidence = "certain";
        desc.description = "not a canonical x86-64 virtual address";
        return desc;
    }

    const auto* module = module_for_address(session, value);
    if (module != nullptr) {
        desc.object_path = module->path;
        desc.object_name = module->name;
        desc.object_base = module->base;
        desc.object_offset = value >= module->base ? value - module->base : value;
    }
    const auto SYMS = symbol_tables(session);
    const auto SECTIONS = section_maps(session);
    auto resolved = wosdbg::resolve_address(value, SYMS, SECTIONS);
    if (resolved) {
        desc.symbol = QString::fromStdString(*resolved);
    }
    for (const auto* section_map : SECTIONS) {
        if (section_map == nullptr) {
            continue;
        }
        auto section = section_map->lookup(value);
        if (section) {
            desc.section = QString::fromStdString(*section);
            break;
        }
    }
    QJsonObject source = source_location_for_address(session, value, module);
    if (!source.isEmpty()) {
        desc.source = source["function"].toString();
        desc.source_path = source["file"].toString();
        desc.source_line = source["line"].toInt();
        desc.source_column = source["column"].toInt();
        desc.source_clickable = source["clickable"].toString();
    }

    const auto* seg = wosdbg::find_segment_for_va(dump, value);
    if (seg != nullptr) {
        desc.mapped = true;
        auto begin = dump.segments.begin();
        auto it = std::find_if(begin, dump.segments.end(), [&](const wosdbg::CoreDumpSegment& candidate) { return &candidate == seg; });
        desc.segment_index = it == dump.segments.end() ? -1 : static_cast<int>(std::distance(begin, it));
        desc.segment_offset = value - seg->vaddr;
        desc.segment_type = seg->type_name();
        QByteArray const BYTES = wosdbg::read_va_bytes(dump, value, 64);
        desc.ascii_preview =
            ascii_preview(BYTES, ((config != nullptr) ? config->get_mcp_settings().max_string_length : K_DEFAULT_MAX_STRING_LENGTH));
        desc.qword_preview = qword_preview(dump, value, 4);
    }

    const bool IS_STACK_RANGE =
        (dump.thread_stack_size > 0 && value >= dump.thread_stack_base && value < dump.thread_stack_base + dump.thread_stack_size) ||
        ((seg != nullptr) && seg->type == static_cast<uint32_t>(wosdbg::SegmentType::STACK_PAGE)) ||
        register_name.compare("rsp", Qt::CaseInsensitive) == 0 || register_name.compare("rbp", Qt::CaseInsensitive) == 0;
    const bool IS_TLS_RANGE =
        (dump.thread_tls_size > 0 && value >= dump.thread_tls_base && value < dump.thread_tls_base + dump.thread_tls_size) ||
        value == dump.thread_fs_base || value == dump.thread_gs_base;

    if (value >= 0xffffffff80000000ULL) {
        desc.classification = desc.symbol.isEmpty() ? "kernel pointer" : "kernel code";
        desc.confidence = desc.symbol.isEmpty() ? "likely" : "certain";
    } else if (IS_STACK_RANGE) {
        desc.classification = "stack pointer";
        desc.confidence = "likely";
    } else if (IS_TLS_RANGE) {
        desc.classification = "TLS/FS/GS-ish";
        desc.confidence = "likely";
    } else if ((module != nullptr) && module->role == "libc") {
        desc.classification = "libc code/data";
        desc.confidence = desc.symbol.isEmpty() ? "likely" : "certain";
    } else if ((module != nullptr) && module->role == "interpreter") {
        desc.classification = "dynamic linker code/data";
        desc.confidence = desc.symbol.isEmpty() ? "likely" : "certain";
    } else if ((module != nullptr) && module->role == "main") {
        desc.classification = "main executable code/data";
        desc.confidence = desc.symbol.isEmpty() ? "likely" : "certain";
    } else if ((!desc.symbol.isEmpty() || desc.section.startsWith(".text")) && value < 0x0000800000000000ULL) {
        desc.classification = "user code";
        desc.confidence = desc.symbol.isEmpty() ? "likely" : "certain";
    } else if (desc.mapped) {
        desc.classification = "mapped data";
        desc.confidence = "likely";
    } else {
        desc.classification = "unmapped";
        desc.confidence = "certain";
    }

    if (!desc.symbol.isEmpty()) {
        desc.description = QString("resolves to %1").arg(desc.symbol);
    } else if (desc.mapped) {
        desc.description = QString("falls inside %1 segment %2 at offset %3")
                               .arg(desc.segment_type)
                               .arg(desc.segment_index)
                               .arg(format_hex(desc.segment_offset));
    } else {
        desc.description = "canonical address, but not present in the coredump segment table";
    }
    return desc;
}

auto DebugAnalysisService::address_description_to_json(const AddressDescription& desc) -> QJsonObject {
    return QJsonObject{{"value", desc.hex},
                       {"classification", desc.classification},
                       {"confidence", desc.confidence},
                       {"description", desc.description},
                       {"canonical", desc.canonical},
                       {"mapped", desc.mapped},
                       {"segmentIndex", desc.segment_index},
                       {"segmentOffset", desc.segment_index >= 0 ? format_hex(desc.segment_offset) : QString()},
                       {"segmentType", desc.segment_type},
                       {"symbol", desc.symbol},
                       {"section", desc.section},
                       {"source", desc.source},
                       {"sourcePath", desc.source_path},
                       {"sourceLine", desc.source_line},
                       {"sourceColumn", desc.source_column},
                       {"sourceClickable", desc.source_clickable},
                       {"objectPath", desc.object_path},
                       {"objectName", desc.object_name},
                       {"objectBase", desc.object_path.isEmpty() ? QString() : format_hex(desc.object_base)},
                       {"objectOffset", desc.object_path.isEmpty() ? QString() : format_hex(desc.object_offset)},
                       {"asciiPreview", desc.ascii_preview},
                       {"qwordPreview", desc.qword_preview}};
}

auto DebugAnalysisService::coredump_registers_to_json(const DumpSession& session, const QString& frame) const -> QJsonArray {
    QJsonArray out;
    QHash<QString, uint64_t> const REGS = register_map(*session.dump, frame);
    QStringList keys = REGS.keys();
    std::ranges::sort(keys);
    for (const auto& key : keys) {
        AddressDescription const DESC = describe_address(session, REGS.value(key), key);
        out.append(QJsonObject{
            {"name", key}, {"frame", frame}, {"value", format_hex(REGS.value(key))}, {"address", address_description_to_json(DESC)}});
    }
    return out;
}

auto DebugAnalysisService::get_crash_summary(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    const auto& dump = *session->dump;
    QJsonArray suspicious;
    const auto REGS = register_map(dump, "trap");
    for (const auto* name : {"rip", "rsp", "rbp", "cr2", "rdi", "rsi", "rdx"}) {
        QString reg_name = QString::fromLatin1(name);
        AddressDescription const DESC = describe_address(*session, REGS.value(reg_name), reg_name);
        if (reg_name == "rip" || reg_name == "cr2" || DESC.classification == "unmapped" || DESC.classification == "invalid/non-canonical" ||
            DESC.classification == "stack pointer") {
            suspicious.append(QJsonObject{{"register", reg_name}, {"address", address_description_to_json(DESC)}});
        }
    }
    QJsonObject fault_instruction = faulting_instruction_report(*session);
    QJsonArray disasm = disassemble_at(*session, dump.trap_frame.rip, 24);
    return QJsonObject{{"ok", true},
                       {"summary", coredump_summary_to_json(*session)},
                       {"suspiciousRegisters", suspicious},
                       {"faultInstruction", fault_instruction},
                       {"nearTrapRip", disasm}};
}

auto DebugAnalysisService::describe_registers(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    QString frame = args["frame"].toString("trap").toLower();
    if (frame != "trap" && frame != "saved") {
        frame = "trap";
    }
    return QJsonObject{{"ok", true}, {"dumpId", session->id}, {"frame", frame}, {"registers", coredump_registers_to_json(*session, frame)}};
}

auto DebugAnalysisService::resolve_address_argument(const DumpSession& session, const QJsonObject& args, const QString& default_register)
    -> std::optional<uint64_t> {
    if (args.contains("address")) {
        return parse_address_value(args["address"]);
    }
    QString const REG = args["register"].toString(default_register).toLower();
    if (!REG.isEmpty()) {
        auto regs = register_map(*session.dump, args["frame"].toString("trap"));
        if (regs.contains(REG)) {
            return regs.value(REG);
        }
    }
    return std::nullopt;
}

auto DebugAnalysisService::follow_register(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    QString reg = args["register"].toString().toLower();
    if (reg.isEmpty()) {
        return tool_error("follow_register requires 'register'");
    }
    auto regs = register_map(*session->dump, args["frame"].toString("trap"));
    if (!regs.contains(reg)) {
        return tool_error(QString("Unknown register: %1").arg(reg));
    }
    uint64_t const VALUE = regs.value(reg);
    AddressDescription const DESC = describe_address(*session, VALUE, reg);
    QJsonObject out{{"ok", true}, {"dumpId", session->id}, {"register", reg}, {"address", address_description_to_json(DESC)}};
    if (DESC.classification.contains("code")) {
        out["suggestedView"] = "disassembly";
        out["disassembly"] = disassemble_at(*session, VALUE, 32);
    } else if (DESC.mapped) {
        out["suggestedView"] = (reg == "rsp" || reg == "rbp") ? "stack" : "memory";
        out["memory"] = get_memory_context(
            QJsonObject{{"dumpId", session->id}, {"address", format_hex(VALUE)}, {"beforeBytes", 64}, {"afterBytes", 192}});
    } else {
        out["suggestedView"] = "explanation";
    }
    return out;
}

auto DebugAnalysisService::get_memory_context(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    auto address = resolve_address_argument(*session, args, "rsp");
    if (!address) {
        return tool_error("get_memory_context requires an address or register");
    }
    const Config& cfg = (config != nullptr) ? *config : ConfigService::instance().get_config();
    int const BEFORE = bounded_int(args, "beforeBytes", 64, 0, cfg.get_mcp_settings().max_memory_bytes);
    int const AFTER = bounded_int(args, "afterBytes", 192, 1, cfg.get_mcp_settings().max_memory_bytes);
    int const TOTAL = std::min(BEFORE + AFTER, cfg.get_mcp_settings().max_memory_bytes);
    uint64_t const START = std::cmp_greater(*address, BEFORE) ? *address - static_cast<uint64_t>(BEFORE) : 0;
    uint64_t const END = START + static_cast<uint64_t>(TOTAL);
    auto rows = wosdbg::dump_range(*session->dump, START, END, symbol_tables(*session), section_maps(*session));
    QJsonArray out_rows;
    const uint64_t RED_ZONE_START =
        session->dump->trap_frame.rsp > K_USER_RED_ZONE_BYTES ? session->dump->trap_frame.rsp - K_USER_RED_ZONE_BYTES : 0;
    for (const auto& row : rows) {
        QString notes = row.notes;
        if (row.va >= RED_ZONE_START && row.va < session->dump->trap_frame.rsp) {
            notes = notes.isEmpty() ? "red-zone" : notes + "  red-zone";
        }
        out_rows.append(QJsonObject{{"address", format_hex(row.va)},
                                    {"value", format_hex(row.value)},
                                    {"symbol", row.symbol},
                                    {"notes", notes},
                                    {"redZone", row.va >= RED_ZONE_START && row.va < session->dump->trap_frame.rsp},
                                    {"gutter", row.gutter}});
    }
    return QJsonObject{
        {"ok", true}, {"dumpId", session->id}, {"anchor", format_hex(*address)}, {"start", format_hex(START)}, {"rows", out_rows}};
}

auto DebugAnalysisService::search_coredump_memory(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    const Config& cfg = (config != nullptr) ? *config : ConfigService::instance().get_config();
    QString kind = args["kind"].toString("pointer").toLower();
    int const MAX_HITS = bounded_int(args, "maxHits", cfg.get_mcp_settings().max_hits, 1, cfg.get_mcp_settings().max_hits);
    int const ALIGNMENT = bounded_int(args, "alignment", (kind == "bytes" || kind == "ascii") ? 1 : 8, 1, 4096);
    int const CONTEXT_BYTES = bounded_int(args, "contextBytes", 32, 0, 256);
    QString const SEGMENT_TYPE = args["segmentType"].toString();

    QByteArray byte_needle;
    uint64_t u64_needle = 0;
    bool has_u64_needle = false;
    if (kind == "bytes") {
        byte_needle = bytes_from_hex(args["needle"].toString());
        if (byte_needle.isEmpty()) {
            return tool_error("bytes search requires a non-empty hex 'needle'");
        }
    } else if (kind == "ascii") {
        byte_needle = args["needle"].toString().toUtf8();
        if (byte_needle.isEmpty()) {
            return tool_error("ascii search requires a non-empty 'needle'");
        }
    } else if (kind == "u64" || kind == "uint64" || kind == "qword" || kind == "pointer" || kind == "symbol") {
        QJsonValue needle;
        if (args.contains("needle")) {
            needle = args["needle"];
        } else if (args.contains("value")) {
            needle = args["value"];
        } else {
            needle = args["target"];
        }
        auto parsed = parse_address_value(needle);
        if (parsed) {
            u64_needle = *parsed;
            has_u64_needle = true;
        }
    }

    std::optional<uint64_t> range_start;
    std::optional<uint64_t> range_end;
    if (args["addressRange"].isObject()) {
        auto range = args["addressRange"].toObject();
        range_start = parse_address_value(range["from"]);
        range_end = parse_address_value(range["to"]);
    }

    struct ScanRange {
        int segment_index = -1;
        uint64_t start = 0;
        uint64_t end = 0;
        QString name;
    };
    const bool HAS_EXPLICIT_ANCHOR = args.contains("address") || args.contains("register");
    QString scope = args["scope"].toString(args["scanScope"].toString(HAS_EXPLICIT_ANCHOR ? "around" : "all")).toLower();
    if (scope.isEmpty()) {
        scope = HAS_EXPLICIT_ANCHOR ? "around" : "all";
    }
    std::vector<ScanRange> ranges;
    const auto& dump = *session->dump;
    auto append_segment_range = [&](int seg_index, uint64_t start, uint64_t end, const QString& name) {
        const auto& seg = dump.segments[static_cast<size_t>(seg_index)];
        start = std::max(start, seg.vaddr);
        end = std::min(end, seg.vaddr_end());
        if (range_start) {
            start = std::max(start, *range_start);
        }
        if (range_end) {
            end = std::min(end, *range_end);
        }
        if (end > start) {
            auto duplicate = std::ranges::find_if(ranges, [&](const ScanRange& existing) {
                return existing.segment_index == seg_index && existing.start == start && existing.end == end;
            });
            if (duplicate == ranges.end()) {
                ranges.push_back(ScanRange{.segment_index = seg_index, .start = start, .end = end, .name = name});
            }
        }
    };
    if (scope == "around") {
        auto anchor = resolve_address_argument(*session, args, "rsp");
        if (!anchor) {
            return tool_error("scope=around requires an address or register");
        }
        int const BEFORE = bounded_int(args, "beforeBytes", 256, 0, cfg.get_mcp_settings().max_memory_bytes);
        int const AFTER = bounded_int(args, "afterBytes", 256, 1, cfg.get_mcp_settings().max_memory_bytes);
        uint64_t const START = std::cmp_greater(*anchor, BEFORE) ? *anchor - static_cast<uint64_t>(BEFORE) : 0;
        uint64_t const END = *anchor + static_cast<uint64_t>(AFTER);
        for (int seg_index = 0; std::cmp_less(seg_index, dump.segments.size()); ++seg_index) {
            const auto& seg = dump.segments[static_cast<size_t>(seg_index)];
            if (seg.is_present() && START < seg.vaddr_end() && END > seg.vaddr) {
                append_segment_range(seg_index, START, END, "around");
            }
        }
    } else {
        auto is_stack_segment = [&](const wosdbg::CoreDumpSegment& seg) {
            return seg.type == static_cast<uint32_t>(wosdbg::SegmentType::STACK_PAGE) ||
                   (dump.thread_stack_size > 0 && seg.vaddr >= dump.thread_stack_base &&
                    seg.vaddr < dump.thread_stack_base + dump.thread_stack_size);
        };
        auto append_stack_containing = [&](uint64_t address, const QString& name) {
            for (int seg_index = 0; std::cmp_less(seg_index, dump.segments.size()); ++seg_index) {
                const auto& seg = dump.segments[static_cast<size_t>(seg_index)];
                if (seg.is_present() && is_stack_segment(seg) && address >= seg.vaddr && address < seg.vaddr_end()) {
                    const uint64_t PAGE_START = page_align_down(address);
                    append_segment_range(seg_index, PAGE_START, PAGE_START + K_PAGE_SIZE, name);
                }
            }
        };
        if (scope == "stack" || scope == "active_stack") {
            append_stack_containing(dump.trap_frame.rsp, "active_stack:trap_rsp");
            append_stack_containing(dump.saved_frame.rsp, "active_stack:saved_rsp");
        }
        if (scope == "active_stack") {
            if (ranges.empty()) {
                return QJsonObject{{"ok", true},           {"dumpId", session->id},          {"kind", kind},        {"scope", scope},
                                   {"hits", QJsonArray{}}, {"scannedRegions", QJsonArray{}}, {"scannedBytes", "0"}, {"truncated", false}};
            }
        }
        const bool SCAN_REMAINING_STACK = scope == "stack" || scope == "all_stack";
        for (int seg_index = 0; std::cmp_less(seg_index, dump.segments.size()); ++seg_index) {
            const auto& seg = dump.segments[static_cast<size_t>(seg_index)];
            if (!seg.is_present()) {
                continue;
            }
            const bool IS_STACK = is_stack_segment(seg);
            if ((SCAN_REMAINING_STACK || scope == "active_stack") && !IS_STACK) {
                continue;
            }
            if (scope == "fault" && seg.type != static_cast<uint32_t>(wosdbg::SegmentType::FAULT_PAGE)) {
                continue;
            }
            if (scope == "active_stack") {
                continue;
            }
            if (!SEGMENT_TYPE.isEmpty() && seg.type_name().compare(SEGMENT_TYPE, Qt::CaseInsensitive) != 0) {
                continue;
            }
            append_segment_range(seg_index, seg.vaddr, seg.vaddr_end(), scope);
        }
    }

    QJsonArray hits;
    QJsonArray scanned_regions;
    bool truncated = false;
    uint64_t scanned = 0;
    uint64_t max_scan_bytes = K_MAX_MEMORY_SEARCH_BYTES;
    if (auto parsed_max = parse_address_value(args["maxScanBytes"])) {
        max_scan_bytes = std::clamp<uint64_t>(*parsed_max, 1, K_MAX_MEMORY_SEARCH_BYTES);
    }
    for (const auto& range : ranges) {
        if (scanned >= max_scan_bytes) {
            truncated = true;
            break;
        }
        uint64_t length = range.end - range.start;
        if (scanned + length > max_scan_bytes) {
            length = max_scan_bytes - scanned;
            truncated = true;
        }
        QByteArray const BYTES = wosdbg::read_va_bytes(dump, range.start, static_cast<size_t>(length));
        scanned += static_cast<uint64_t>(BYTES.size());
        scanned_regions.append(QJsonObject{{"segmentIndex", range.segment_index},
                                           {"start", format_hex(range.start)},
                                           {"end", format_hex(range.start + static_cast<uint64_t>(BYTES.size()))},
                                           {"bytes", QString::number(BYTES.size())},
                                           {"scope", range.name}});
        if (BYTES.isEmpty()) {
            continue;
        }

        qsizetype off = 0;
        if (ALIGNMENT > 1 &&
            (kind == "u64" || kind == "uint64" || kind == "qword" || kind == "pointer" || kind == "symbol" || kind == "nonzero")) {
            const uint64_t MISALIGNMENT = range.start % static_cast<uint64_t>(ALIGNMENT);
            if (MISALIGNMENT != 0) {
                off = static_cast<qsizetype>(static_cast<uint64_t>(ALIGNMENT) - MISALIGNMENT);
            }
        }
        for (; off < BYTES.size();) {
            bool match = false;
            uint64_t hit_value = 0;
            if ((kind == "bytes" || kind == "ascii") && off + byte_needle.size() <= BYTES.size()) {
                match = std::memcmp(BYTES.constData() + off, byte_needle.constData(), static_cast<size_t>(byte_needle.size())) == 0;
                hit_value = 0;
            } else if (off + 7 < BYTES.size()) {
                hit_value = read_u64_le(BYTES.constData() + off);
                if (kind == "u64" || kind == "uint64" || kind == "qword") {
                    match = has_u64_needle && hit_value == u64_needle;
                } else if (kind == "nonzero") {
                    match = hit_value != 0;
                } else if (kind == "pointer" || kind == "symbol") {
                    auto desc = describe_address(*session, hit_value);
                    match = desc.canonical && (desc.mapped || !desc.symbol.isEmpty() || desc.classification.contains("code") ||
                                               desc.classification.contains("kernel"));
                    if (kind == "symbol") {
                        match = match && !desc.symbol.isEmpty();
                    }
                    if (has_u64_needle) {
                        match = match && hit_value == u64_needle;
                    }
                }
            }

            if (match) {
                uint64_t const HIT_ADDR = range.start + static_cast<uint64_t>(off);
                AddressDescription const POINTED = describe_address(*session, hit_value);
                uint64_t const CTX_START =
                    std::cmp_greater(HIT_ADDR, CONTEXT_BYTES) ? HIT_ADDR - static_cast<uint64_t>(CONTEXT_BYTES) : HIT_ADDR;
                QByteArray const PREVIEW = wosdbg::read_va_bytes(dump, CTX_START, static_cast<size_t>((CONTEXT_BYTES * 2) + 8));
                hits.append(QJsonObject{{"dumpId", session->id},
                                        {"address", format_hex(HIT_ADDR)},
                                        {"segmentIndex", range.segment_index},
                                        {"value", (kind == "bytes" || kind == "ascii") ? QString() : format_hex(hit_value)},
                                        {"pointsTo", address_description_to_json(POINTED)},
                                        {"preview", ascii_preview(PREVIEW, cfg.get_mcp_settings().max_string_length)},
                                        {"suggestedTool", "wosdbg.get_memory_context"}});
                if (hits.size() >= MAX_HITS) {
                    truncated = true;
                    break;
                }
            }

            off += ALIGNMENT;
        }
        if (hits.size() >= MAX_HITS || scanned >= max_scan_bytes) {
            break;
        }
    }
    return QJsonObject{{"ok", true},
                       {"dumpId", session->id},
                       {"kind", kind},
                       {"scope", scope},
                       {"hits", hits},
                       {"scannedRegions", scanned_regions},
                       {"scannedBytes", QString::number(scanned)},
                       {"truncated", truncated}};
}

auto DebugAnalysisService::find_pointers(const QJsonObject& args) const -> QJsonObject {
    QJsonObject search_args = args;
    search_args["kind"] = "pointer";
    if (!search_args.contains("alignment")) {
        search_args["alignment"] = 8;
    }
    QJsonObject result = search_coredump_memory(search_args);
    QString const TARGET = args["target"].toString().toLower();
    if (TARGET.isEmpty() || !result["ok"].toBool()) {
        return result;
    }

    QJsonArray filtered;
    for (const auto& hit_value : result["hits"].toArray()) {
        QJsonObject hit = hit_value.toObject();
        QString const CLS = hit["pointsTo"].toObject()["classification"].toString().toLower();
        QJsonObject points_to = hit["pointsTo"].toObject();
        const QString OBJECT_NAME = points_to["objectName"].toString().toLower();
        const QString OBJECT_PATH = points_to["objectPath"].toString().toLower();
        bool const KEEP = (TARGET == "code" && CLS.contains("code")) || (TARGET == "stack" && CLS.contains("stack")) ||
                          (TARGET == "kernel" && CLS.contains("kernel")) || (TARGET == "mapped" && points_to["mapped"].toBool()) ||
                          (!TARGET.isEmpty() && (OBJECT_NAME.contains(TARGET) || OBJECT_PATH.contains(TARGET) || CLS.contains(TARGET)));
        if (KEEP) {
            filtered.append(hit);
        }
    }
    result["hits"] = filtered;
    result["target"] = TARGET;
    return result;
}

auto DebugAnalysisService::pte_info_for_address(const DumpSession& session, uint64_t address) -> QJsonObject {
    const auto& dump = *session.dump;
    const uint64_t PAGE_BASE = address & ~(K_PAGE_SIZE - 1);
    const auto* seg = wosdbg::find_segment_for_va(dump, address);
    if (seg == nullptr) {
        return QJsonObject{{"address", format_hex(address)},
                           {"page", format_hex(PAGE_BASE)},
                           {"mapped", false},
                           {"summary", "not present in coredump segment table"}};
    }

    auto begin = dump.segments.begin();
    auto it = std::find_if(begin, dump.segments.end(), [&](const wosdbg::CoreDumpSegment& candidate) { return &candidate == seg; });
    const int SEGMENT_INDEX = it == dump.segments.end() ? -1 : static_cast<int>(std::distance(begin, it));
    return QJsonObject{{"address", format_hex(address)},
                       {"page", format_hex(PAGE_BASE)},
                       {"mapped", true},
                       {"segmentIndex", SEGMENT_INDEX},
                       {"segmentType", seg->type_name()},
                       {"segmentRange", QJsonObject{{"start", format_hex(seg->vaddr)}, {"end", format_hex(seg->vaddr_end())}}},
                       {"offsetInPage", format_hex(address - PAGE_BASE)},
                       {"physical", format_hex(seg->phys_addr + (address - seg->vaddr))},
                       {"pte", pte_flags_to_json(seg->pte_flags)}};
}

auto DebugAnalysisService::stack_window(const DumpSession& session, uint64_t rsp, uint64_t rbp, int before_bytes, int after_bytes) const
    -> QJsonArray {
    uint64_t const START = std::cmp_greater(rsp, before_bytes) ? rsp - static_cast<uint64_t>(before_bytes) : 0;
    uint64_t const END = rsp + static_cast<uint64_t>(after_bytes);
    auto rows = wosdbg::dump_range(*session.dump, START, END, symbol_tables(session), section_maps(session));
    QJsonArray out_rows;
    const uint64_t RED_ZONE_START = rsp > K_USER_RED_ZONE_BYTES ? rsp - K_USER_RED_ZONE_BYTES : 0;
    for (const auto& row : rows) {
        QStringList annotations;
        if (!row.notes.isEmpty()) {
            annotations << row.notes;
        }
        if (row.va >= RED_ZONE_START && row.va < rsp) {
            annotations << "red-zone";
        }
        if (row.va == rsp) {
            annotations << "current rsp";
        }
        if (row.va == rbp) {
            annotations << "current rbp";
        }
        if (row.va == rbp + 8) {
            annotations << "frame return slot";
        }
        if (row.value == session.dump->cr2) {
            annotations << "equals cr2/fault address";
        }
        if (row.value == session.dump->trap_regs.rax) {
            annotations << "equals trap rax";
        }
        if (row.value == session.dump->saved_regs.rax) {
            annotations << "equals saved rax";
        }
        AddressDescription const POINTED = describe_address(session, row.value);
        if (POINTED.mapped || !POINTED.symbol.isEmpty() || POINTED.classification.contains("code") ||
            POINTED.classification.contains("kernel")) {
            annotations << QString("points to %1").arg(POINTED.classification);
        } else if (row.value != 0 && row.value < 0x1000) {
            annotations << "suspicious small value";
        }
        out_rows.append(QJsonObject{{"address", format_hex(row.va)},
                                    {"value", format_hex(row.value)},
                                    {"gutter", row.gutter},
                                    {"symbol", row.symbol},
                                    {"annotations", annotations.join("; ")},
                                    {"pointsTo", address_description_to_json(POINTED)}});
    }
    return out_rows;
}

auto DebugAnalysisService::red_zone_report(const DumpSession& session, uint64_t rsp) const -> QJsonObject {
    const uint64_t START = rsp > K_USER_RED_ZONE_BYTES ? rsp - K_USER_RED_ZONE_BYTES : 0;
    QJsonArray live_values;
    QByteArray const BYTES = wosdbg::read_va_bytes(*session.dump, START, K_USER_RED_ZONE_BYTES);
    if (BYTES.isEmpty()) {
        return QJsonObject{{"start", format_hex(START)}, {"end", format_hex(rsp)}, {"available", false}, {"liveValues", live_values}};
    }
    for (int off = 0; off + 7 < BYTES.size(); off += 8) {
        const uint64_t VALUE = read_u64_le(BYTES.constData() + off);
        if (VALUE == 0) {
            continue;
        }
        AddressDescription desc = describe_address(session, VALUE);
        const bool NOTABLE = desc.mapped || !desc.symbol.isEmpty() || VALUE == session.dump->trap_frame.rip ||
                             VALUE == session.dump->saved_frame.rip || VALUE == session.dump->cr2 || VALUE < 0x1000;
        if (!NOTABLE) {
            continue;
        }
        live_values.append(QJsonObject{{"slot", format_hex(START + static_cast<uint64_t>(off))},
                                       {"value", format_hex(VALUE)},
                                       {"classification", desc.classification},
                                       {"symbol", desc.symbol},
                                       {"note", VALUE < 0x1000 ? "small suspicious value in red zone" : desc.description}});
    }
    return QJsonObject{{"start", format_hex(START)},
                       {"end", format_hex(rsp)},
                       {"available", true},
                       {"liveValues", live_values},
                       {"hasLiveLookingValues", !live_values.isEmpty()}};
}

auto DebugAnalysisService::frame_pointer_backtrace(const DumpSession& session, const QString& frame, int max_frames) const -> QJsonArray {
    const auto REGS = register_map(*session.dump, frame);
    uint64_t const RIP = REGS.value("rip");
    uint64_t const RSP = REGS.value("rsp");
    uint64_t rbp = REGS.value("rbp");
    QJsonArray frames;
    auto append_frame = [&](int index, uint64_t frame_rip, uint64_t frame_rsp, uint64_t frame_rbp, const QString& method,
                            const QString& note) {
        AddressDescription desc = describe_address(session, frame_rip);
        frames.append(QJsonObject{{"index", index},
                                  {"rip", format_hex(frame_rip)},
                                  {"rsp", format_hex(frame_rsp)},
                                  {"rbp", format_hex(frame_rbp)},
                                  {"method", method},
                                  {"module", desc.object_name},
                                  {"objectPath", desc.object_path},
                                  {"objectBase", desc.object_path.isEmpty() ? QString() : format_hex(desc.object_base)},
                                  {"objectOffset", desc.object_path.isEmpty() ? QString() : format_hex(desc.object_offset)},
                                  {"symbol", desc.symbol},
                                  {"source", desc.source},
                                  {"sourcePath", desc.source_path},
                                  {"sourceLine", desc.source_line},
                                  {"sourceColumn", desc.source_column},
                                  {"sourceClickable", desc.source_clickable},
                                  {"confidence", method == "stack-scan" ? "possible" : "likely"},
                                  {"note", note}});
    };

    append_frame(0, RIP, RSP, rbp, "trap-registers", frame == "saved" ? "saved scheduler/syscall context" : "fault trap context");
    int produced = 1;
    uint64_t previous_rbp = 0;
    for (; produced < max_frames; ++produced) {
        if (rbp == 0 || (rbp & 0x7U) != 0 || rbp == previous_rbp) {
            break;
        }
        auto next_rbp = read_u64_at(*session.dump, rbp);
        auto ret = read_u64_at(*session.dump, rbp + 8);
        if (!next_rbp || !ret) {
            break;
        }
        if (*ret == 0 || !is_likely_user_code_address(*ret, *session.dump)) {
            break;
        }
        append_frame(produced, *ret, rbp + 16, *next_rbp, "frame-pointer", QString("return slot %1").arg(format_hex(rbp + 8)));
        if (*next_rbp <= rbp || *next_rbp - rbp > 16 * 1024 * 1024ULL) {
            break;
        }
        previous_rbp = rbp;
        rbp = *next_rbp;
    }

    if (frames.size() <= 1) {
        QByteArray const STACK = wosdbg::read_va_bytes(*session.dump, RSP, 512);
        for (int off = 0; off + 7 < STACK.size() && frames.size() < max_frames; off += 8) {
            const uint64_t CANDIDATE = read_u64_le(STACK.constData() + off);
            if (!is_likely_user_code_address(CANDIDATE, *session.dump)) {
                continue;
            }
            append_frame(frames.size(), CANDIDATE, RSP + static_cast<uint64_t>(off), 0, "stack-scan",
                         QString("possible return address at stack slot %1").arg(format_hex(RSP + static_cast<uint64_t>(off))));
        }
    }
    return frames;
}

auto DebugAnalysisService::disassembly_start_symbol(const DumpSession& session, uint64_t address) -> std::optional<wosdbg::SymbolEntry> {
    auto match_in_table = [address](const wosdbg::SymbolTable* table) -> std::optional<wosdbg::SymbolEntry> {
        if (!table || table->entries().empty()) {
            return std::nullopt;
        }

        const auto& entries = table->entries();
        auto it = std::ranges::upper_bound(entries, address, std::ranges::less{}, &wosdbg::SymbolEntry::addr);
        if (it == entries.begin()) {
            return std::nullopt;
        }
        --it;

        const uint64_t OFFSET = address - it->addr;
        uint64_t next_symbol_distance = K_MAX_DISASSEMBLY_FUNCTION_BYTES + 1;
        if (std::next(it) != entries.end() && std::next(it)->addr > it->addr) {
            next_symbol_distance = std::next(it)->addr - it->addr;
        }
        const uint64_t SYMBOL_EXTENT = it->size > 0 ? it->size : std::min<uint64_t>(next_symbol_distance, K_MAX_DISASSEMBLY_FUNCTION_BYTES);
        if (OFFSET >= SYMBOL_EXTENT || OFFSET > K_MAX_DISASSEMBLY_FUNCTION_BYTES) {
            return std::nullopt;
        }
        return *it;
    };

    for (const auto& module : session.modules) {
        if (auto match = match_in_table(module.symbols.get())) {
            return match;
        }
    }
    if (auto match = match_in_table(session.embedded_symbols.get())) {
        return match;
    }
    if (auto match = match_in_table(session.binary_symbols.get())) {
        return match;
    }
    return match_in_table(session.kernel_symbols.get());
}

auto DebugAnalysisService::disassemble_at(const DumpSession& session, uint64_t address, int instruction_count) -> QJsonArray {
    QJsonArray out;
    csh capstone = 0;
    if (cs_open(CS_ARCH_X86, CS_MODE_64, &capstone) != CS_ERR_OK) {
        out.append(QJsonObject{{"note", "Capstone failed to initialize"}});
        return out;
    }
    cs_option(capstone, CS_OPT_SYNTAX, CS_OPT_SYNTAX_INTEL);

    uint64_t start = address;
    bool have_symbol_start = false;
    if (auto symbol = disassembly_start_symbol(session, address)) {
        start = symbol->addr;
        have_symbol_start = true;
    }
    const uint64_t BYTES_NEEDED =
        address >= start ? (address - start) + K_DISASSEMBLY_BYTES_AFTER_TARGET : K_DISASSEMBLY_BYTES_AFTER_TARGET;
    const auto READ_SIZE = static_cast<size_t>(std::min<uint64_t>(BYTES_NEEDED, K_MAX_DISASSEMBLY_FUNCTION_BYTES));
    std::vector<uint8_t> bytes;
    QByteArray const MEMORY_BYTES = wosdbg::read_va_bytes(*session.dump, start, READ_SIZE);
    QString byte_source = "coredump-memory";
    if (!MEMORY_BYTES.isEmpty()) {
        bytes.assign(reinterpret_cast<const uint8_t*>(MEMORY_BYTES.constData()),
                     reinterpret_cast<const uint8_t*>(MEMORY_BYTES.constData()) + MEMORY_BYTES.size());
    }
    if (bytes.empty()) {
        const auto* module = module_for_address(session, address);
        if (module != nullptr) {
            QByteArray const ELF =
                module->path.startsWith("embedded:") ? session.dump->embedded_elf() : read_file_bytes_quiet(module->path);
            bytes = wosdbg::elf_bytes_at_runtime_va(ELF, start, module->base, READ_SIZE);
            byte_source = QString("module-elf:%1").arg(module->name);
        }
    }
    if (bytes.empty()) {
        QByteArray const ELF = session.dump->embedded_elf();
        bytes = wosdbg::elf_bytes_at_va(ELF, start, READ_SIZE);
        byte_source = "embedded-elf";
    }
    if (bytes.empty()) {
        cs_close(&capstone);
        out.append(QJsonObject{{"note", QString("no instruction bytes mapped at %1").arg(format_hex(start))}});
        return out;
    }

    cs_insn* insns = nullptr;
    size_t const COUNT = cs_disasm(capstone, bytes.data(), bytes.size(), start, 0, &insns);
    if (COUNT == 0) {
        cs_close(&capstone);
        out.append(QJsonObject{{"note", "Capstone disassembly failed"}});
        return out;
    }

    int anchor = 0;
    for (size_t i = 0; i < COUNT; ++i) {
        if (insns[i].address >= address) {
            anchor = static_cast<int>(i);
            break;
        }
    }
    if (anchor == 0 && COUNT > 0 && insns[COUNT - 1].address < address) {
        anchor = static_cast<int>(COUNT - 1);
    }
    int const BEGIN = have_symbol_start ? std::max(0, anchor - (instruction_count / 3)) : anchor;
    int const END = std::min(static_cast<int>(COUNT), BEGIN + instruction_count);
    const auto SYMS = symbol_tables(session);
    const auto SECTIONS = section_maps(session);
    for (int i = BEGIN; i < END; ++i) {
        const auto& ins = insns[static_cast<size_t>(i)];
        QString bytes_text;
        for (int b = 0; std::cmp_less(b, ins.size); ++b) {
            bytes_text += QString("%1 ").arg(static_cast<uint32_t>(ins.bytes[b]), 2, 16, QChar('0'));
        }
        auto sym = wosdbg::resolve_address(ins.address, SYMS, SECTIONS);
        const bool TARGET_INSIDE = address > ins.address && address < ins.address + ins.size;
        QString marker;
        if (ins.address == session.dump->trap_frame.rip) {
            marker = "trap_rip";
        } else if (ins.address == session.dump->saved_frame.rip) {
            marker = "saved_rip";
        } else if (ins.address == address) {
            marker = "target";
        } else if (TARGET_INSIDE) {
            marker = "target_inside_instruction";
        }
        out.append(QJsonObject{{"address", format_hex(ins.address)},
                               {"marker", marker},
                               {"byteSource", byte_source},
                               {"bytes", bytes_text.trimmed()},
                               {"mnemonic", QString::fromLatin1(ins.mnemonic)},
                               {"operands", QString::fromLatin1(ins.op_str)},
                               {"symbol", sym ? QString::fromStdString(*sym) : QString()}});
    }
    cs_free(insns, COUNT);
    cs_close(&capstone);
    return out;
}

auto DebugAnalysisService::faulting_instruction_report(const DumpSession& session) const -> QJsonObject {
    const auto& dump = *session.dump;
    const uint64_t RIP = dump.trap_frame.rip;
    QByteArray bytes = wosdbg::read_va_bytes(dump, RIP, 16);
    QString byte_source = "coredump-memory";
    if (bytes.isEmpty()) {
        QByteArray const ELF = dump.embedded_elf();
        auto elf_bytes = wosdbg::elf_bytes_at_va(ELF, RIP, 16);
        if (!elf_bytes.empty()) {
            bytes = QByteArray(reinterpret_cast<const char*>(elf_bytes.data()), static_cast<qsizetype>(elf_bytes.size()));
            byte_source = "embedded-elf";
        }
    }
    if (bytes.isEmpty()) {
        return QJsonObject{{"available", false}, {"rip", format_hex(RIP)}, {"note", "no instruction bytes available at trap RIP"}};
    }

    csh capstone = 0;
    if (cs_open(CS_ARCH_X86, CS_MODE_64, &capstone) != CS_ERR_OK) {
        return QJsonObject{{"available", false}, {"rip", format_hex(RIP)}, {"note", "Capstone failed to initialize"}};
    }
    cs_option(capstone, CS_OPT_SYNTAX, CS_OPT_SYNTAX_INTEL);
    cs_option(capstone, CS_OPT_DETAIL, CS_OPT_ON);

    cs_insn* insns = nullptr;
    size_t const COUNT =
        cs_disasm(capstone, reinterpret_cast<const uint8_t*>(bytes.constData()), static_cast<size_t>(bytes.size()), RIP, 1, &insns);
    if (COUNT == 0) {
        cs_close(&capstone);
        return QJsonObject{{"available", false}, {"rip", format_hex(RIP)}, {"note", "Capstone could not decode the trap instruction"}};
    }

    const cs_insn& ins = insns[0];
    QString bytes_text;
    for (int i = 0; std::cmp_less(i, ins.size); ++i) {
        bytes_text += QString("%1 ").arg(static_cast<uint32_t>(ins.bytes[i]), 2, 16, QChar('0'));
    }
    QJsonArray memory_operands;
    QJsonArray register_operands;
    const auto REGS = register_map(dump, "trap");
    if (ins.detail != nullptr) {
        const cs_x86& x86 = ins.detail->x86;
        for (uint8_t i = 0; i < x86.op_count; ++i) {
            const cs_x86_op& op = x86.operands[i];
            if (op.type == X86_OP_REG) {
                QString reg = capstone_reg_to_name(op.reg);
                QJsonObject reg_obj{{"index", static_cast<int>(i)}, {"register", reg}};
                if (auto value = capstone_reg_value(op.reg, REGS)) {
                    reg_obj["value"] = format_hex(*value);
                }
                reg_obj["access"] =
                    QString("%1%2").arg((op.access & CS_AC_READ) != 0 ? "r" : "").arg((op.access & CS_AC_WRITE) != 0 ? "w" : "");
                register_operands.append(reg_obj);
            } else if (op.type == X86_OP_MEM) {
                int64_t effective = op.mem.disp;
                QStringList terms;
                if (op.mem.base != X86_REG_INVALID) {
                    QString const BASE_NAME = capstone_reg_to_name(op.mem.base);
                    auto base_value = capstone_reg_value(op.mem.base, REGS);
                    if (op.mem.base == X86_REG_RIP) {
                        effective += static_cast<int64_t>(ins.address + ins.size);
                        terms << QString("rip_next=%1").arg(format_hex(ins.address + ins.size));
                    } else if (base_value) {
                        effective += static_cast<int64_t>(*base_value);
                        terms << QString("%1=%2").arg(BASE_NAME, format_hex(*base_value));
                    } else if (!BASE_NAME.isEmpty()) {
                        terms << BASE_NAME;
                    }
                }
                if (op.mem.index != X86_REG_INVALID) {
                    QString const INDEX_NAME = capstone_reg_to_name(op.mem.index);
                    auto index_value = capstone_reg_value(op.mem.index, REGS);
                    if (index_value) {
                        effective += static_cast<int64_t>(*index_value) * op.mem.scale;
                        terms << QString("%1=%2*%3").arg(INDEX_NAME, format_hex(*index_value)).arg(op.mem.scale);
                    } else if (!INDEX_NAME.isEmpty()) {
                        terms << QString("%1*%2").arg(INDEX_NAME).arg(op.mem.scale);
                    }
                }
                if (op.mem.disp != 0) {
                    terms << QString("disp=%1").arg(op.mem.disp);
                }
                const auto EA = static_cast<uint64_t>(effective);
                QJsonObject const MEM_OBJ{
                    {"index", static_cast<int>(i)},
                    {"access",
                     QString("%1%2").arg((op.access & CS_AC_READ) != 0 ? "r" : "").arg((op.access & CS_AC_WRITE) != 0 ? "w" : "")},
                    {"effectiveAddress", format_hex(EA)},
                    {"matchesCr2", EA == dump.cr2},
                    {"calculation", terms.join(" + ")},
                    {"address", address_description_to_json(describe_address(session, EA))}};
                memory_operands.append(MEM_OBJ);
            }
        }
    }

    QJsonArray provenance;
    QByteArray const HISTORY_BYTES = wosdbg::read_va_bytes(dump, RIP > 128 ? RIP - 128 : 0, RIP > 128 ? 128 : RIP);
    if (!HISTORY_BYTES.isEmpty()) {
        const uint64_t HISTORY_START = RIP - static_cast<uint64_t>(HISTORY_BYTES.size());
        cs_insn* history = nullptr;
        size_t const HISTORY_COUNT = cs_disasm(capstone, reinterpret_cast<const uint8_t*>(HISTORY_BYTES.constData()),
                                               static_cast<size_t>(HISTORY_BYTES.size()), HISTORY_START, 0, &history);
        if (HISTORY_COUNT > 0) {
            QHash<QString, QJsonObject> last_write_from_memory;
            for (size_t h = 0; h < HISTORY_COUNT; ++h) {
                const cs_insn& prev = history[h];
                if (prev.address >= RIP || prev.detail == nullptr) {
                    continue;
                }
                QString written_reg;
                QJsonObject source_mem;
                const cs_x86& x86 = prev.detail->x86;
                for (uint8_t i = 0; i < x86.op_count; ++i) {
                    const cs_x86_op& op = x86.operands[i];
                    if (op.type == X86_OP_REG && (op.access & CS_AC_WRITE) != 0) {
                        written_reg = capstone_reg_to_name(op.reg);
                    } else if (op.type == X86_OP_MEM && (op.access & CS_AC_READ) != 0) {
                        int64_t effective = op.mem.disp;
                        QStringList terms;
                        if (op.mem.base != X86_REG_INVALID) {
                            QString const BASE_NAME = capstone_reg_to_name(op.mem.base);
                            auto base_value = capstone_reg_value(op.mem.base, REGS);
                            if (base_value) {
                                effective += static_cast<int64_t>(*base_value);
                                terms << QString("%1=%2").arg(BASE_NAME, format_hex(*base_value));
                            }
                        }
                        if (op.mem.index != X86_REG_INVALID) {
                            QString const INDEX_NAME = capstone_reg_to_name(op.mem.index);
                            auto index_value = capstone_reg_value(op.mem.index, REGS);
                            if (index_value) {
                                effective += static_cast<int64_t>(*index_value) * op.mem.scale;
                                terms << QString("%1=%2*%3").arg(INDEX_NAME, format_hex(*index_value)).arg(op.mem.scale);
                            }
                        }
                        if (op.mem.disp != 0) {
                            terms << QString("disp=%1").arg(op.mem.disp);
                        }
                        const auto EA = static_cast<uint64_t>(effective);
                        source_mem = QJsonObject{{"address", format_hex(EA)},
                                                 {"calculation", terms.join(" + ")},
                                                 {"baseRegister", capstone_reg_to_name(op.mem.base)},
                                                 {"displacement", static_cast<qint64>(op.mem.disp)}};
                    }
                }
                if (!written_reg.isEmpty() && !source_mem.isEmpty()) {
                    last_write_from_memory[written_reg] = QJsonObject{
                        {"register", written_reg},
                        {"instruction", QString("%1 %2").arg(QString::fromLatin1(prev.mnemonic), QString::fromLatin1(prev.op_str))},
                        {"instructionAddress", format_hex(prev.address)},
                        {"source", source_mem},
                        {"confidence", "possible"}};
                }
            }
            for (const auto& operand_value : memory_operands) {
                QJsonObject operand = operand_value.toObject();
                const QString CALC = operand["calculation"].toString();
                for (auto it = last_write_from_memory.cbegin(); it != last_write_from_memory.cend(); ++it) {
                    if (CALC.contains(it.key() + "=")) {
                        QJsonObject item = it.value();
                        QJsonObject source = item["source"].toObject();
                        auto source_address = parse_address_value(source["address"]);
                        if (source_address) {
                            auto value_at_source = read_u64_at(dump, *source_address);
                            const uint64_t RED_START =
                                dump.trap_frame.rsp > K_USER_RED_ZONE_BYTES ? dump.trap_frame.rsp - K_USER_RED_ZONE_BYTES : 0;
                            source["inRedZone"] = *source_address >= RED_START && *source_address < dump.trap_frame.rsp;
                            if (value_at_source) {
                                source["value"] = format_hex(*value_at_source);
                                source["valueEqualsCr2"] = *value_at_source == dump.cr2;
                                source["valueEqualsRax"] = *value_at_source == dump.trap_regs.rax;
                            }
                        }
                        item["source"] = source;
                        provenance.append(item);
                    }
                }
            }
            cs_free(history, HISTORY_COUNT);
        }
    }

    QJsonArray correlations;
    QSet<QString> correlation_keys;
    auto append_correlation = [&](const QJsonObject& object) {
        const QString KEY = QString::fromUtf8(QJsonDocument(object).toJson(QJsonDocument::Compact));
        if (correlation_keys.contains(KEY)) {
            return;
        }
        correlation_keys.insert(KEY);
        correlations.append(object);
    };
    for (auto it = REGS.cbegin(); it != REGS.cend(); ++it) {
        if (it.value() == dump.cr2) {
            append_correlation(QJsonObject{
                {"kind", "register_equals_cr2"}, {"register", it.key()}, {"value", format_hex(it.value())}, {"confidence", "certain"}});
        }
        if (it.key() != "rax" && it.value() == dump.trap_regs.rax) {
            append_correlation(QJsonObject{
                {"kind", "register_equals_rax"}, {"register", it.key()}, {"value", format_hex(it.value())}, {"confidence", "certain"}});
        }
    }
    for (uint64_t const ANCHOR : {dump.trap_frame.rsp, dump.trap_regs.rbp}) {
        QByteArray const STACK = wosdbg::read_va_bytes(dump, ANCHOR > 64 ? ANCHOR - 64 : ANCHOR, 160);
        const uint64_t START = ANCHOR > 64 ? ANCHOR - 64 : ANCHOR;
        for (int off = 0; off + 7 < STACK.size(); off += 8) {
            const uint64_t VALUE = read_u64_le(STACK.constData() + off);
            if (VALUE == dump.cr2 || VALUE == dump.trap_regs.rax) {
                append_correlation(QJsonObject{{"kind", VALUE == dump.cr2 ? "stack_slot_equals_cr2" : "stack_slot_equals_rax"},
                                               {"slot", format_hex(START + static_cast<uint64_t>(off))},
                                               {"value", format_hex(VALUE)},
                                               {"confidence", "likely"}});
            }
        }
    }
    for (const auto& value : provenance) {
        QJsonObject item = value.toObject();
        QJsonObject source = item["source"].toObject();
        if (source["inRedZone"].toBool() && (source["valueEqualsCr2"].toBool() || source["valueEqualsRax"].toBool())) {
            append_correlation(QJsonObject{{"kind", "red_zone_frame_local_corruption"},
                                           {"slot", source["address"].toString()},
                                           {"value", source["value"].toString()},
                                           {"note", "callee local stored in red zone appears corrupted"},
                                           {"confidence", "likely"}});
        }
    }

    QJsonObject out{{"available", true},
                    {"rip", format_hex(RIP)},
                    {"byteSource", byte_source},
                    {"bytes", bytes_text.trimmed()},
                    {"mnemonic", QString::fromLatin1(ins.mnemonic)},
                    {"operands", QString::fromLatin1(ins.op_str)},
                    {"memoryOperands", memory_operands},
                    {"registerOperands", register_operands},
                    {"provenance", provenance},
                    {"correlations", correlations}};
    cs_free(insns, COUNT);
    cs_close(&capstone);
    return out;
}

auto DebugAnalysisService::classify_fault(const DumpSession& session, const QJsonObject& instruction) -> QJsonObject {
    const auto& dump = *session.dump;
    QStringList labels;
    QStringList evidence;
    const bool WRITE = (dump.err_code & 0x2U) != 0;
    const bool INSTRUCTION_FETCH = (dump.err_code & 0x10U) != 0;
    if (dump.cr2 == 0 || dump.cr2 < 0x1000) {
        labels << (WRITE ? "low unmapped write through corrupted pointer" : "null/low-address deref");
        evidence << QString("cr2 is %1").arg(format_hex(dump.cr2));
    }
    if (INSTRUCTION_FETCH) {
        labels << "instruction fetch fault";
        evidence << "page-fault error code has instruction-fetch bit set";
    }
    if (WRITE) {
        labels << "write fault";
        evidence << "page-fault error code has write bit set";
    }
    const uint64_t RED_ZONE_START = dump.trap_frame.rsp > K_USER_RED_ZONE_BYTES ? dump.trap_frame.rsp - K_USER_RED_ZONE_BYTES : 0;
    if (dump.cr2 >= RED_ZONE_START && dump.cr2 < dump.trap_frame.rsp) {
        labels << "red-zone/stack-adjacent fault";
        evidence << "fault address is in the 128-byte user red zone below trap rsp";
    } else if (dump.cr2 >= dump.trap_frame.rsp && dump.cr2 < dump.trap_frame.rsp + 4096) {
        labels << "stack fault";
        evidence << "fault address is on the current stack page near rsp";
    }
    QJsonObject pte = pte_info_for_address(session, dump.cr2);
    if (!pte["mapped"].toBool()) {
        labels << "unmapped access";
        evidence << "cr2 is not present in captured user pages";
    } else {
        QJsonObject flags = pte["pte"].toObject();
        if (WRITE && flags["cow"].toBool() && !flags["rw"].toBool()) {
            labels << "COW fault";
            evidence << "PTE has COW bit and lacks write permission";
        }
        if (INSTRUCTION_FETCH && flags["nx"].toBool()) {
            labels << "NX execute fault";
            evidence << "PTE has NX bit set";
        }
    }

    for (const auto& value : instruction["memoryOperands"].toArray()) {
        QJsonObject operand = value.toObject();
        if (operand["matchesCr2"].toBool()) {
            labels << "faulting instruction effective address";
            evidence << QString("instruction operand computes %1").arg(operand["effectiveAddress"].toString());
        }
    }
    labels.removeDuplicates();
    QString severity = "application_bug";
    if (labels.contains("COW fault") || labels.contains("NX execute fault")) {
        severity = "kernel_bug_suspected";
    } else if (labels.contains("low unmapped write through corrupted pointer") || labels.contains("red-zone/stack-adjacent fault")) {
        severity = "memory_corruption_suspected";
    }
    return QJsonObject{{"primary", labels.isEmpty() ? "unknown userspace page fault" : labels.first()},
                       {"labels", QJsonArray::fromStringList(labels)},
                       {"evidence", QJsonArray::fromStringList(evidence)},
                       {"confidence", labels.isEmpty() ? "possible" : "likely"},
                       {"severity", severity},
                       {"pageFaultError", page_fault_error_to_json(dump.err_code)},
                       {"cr2Pte", pte}};
}

auto DebugAnalysisService::disassemble_coredump(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    auto address = resolve_address_argument(*session, args, "rip");
    if (!address) {
        return tool_error("disassemble_coredump requires an address or register");
    }
    const Config& cfg = (config != nullptr) ? *config : ConfigService::instance().get_config();
    int const COUNT = bounded_int(args, "instructions", 32, 1, cfg.get_mcp_settings().max_disassembly_instructions);
    return QJsonObject{{"ok", true},
                       {"dumpId", session->id},
                       {"address", format_hex(*address)},
                       {"instructions", disassemble_at(*session, *address, COUNT)}};
}

auto DebugAnalysisService::resolve_address_tool(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    auto address = resolve_address_argument(*session, args);
    if (!address) {
        return tool_error("resolve_address requires an address or register");
    }
    const auto* module = module_for_address(*session, *address);
    const QString MODULE_FILTER = args["moduleFilter"].toString(args["module"].toString()).toLower();
    if (!MODULE_FILTER.isEmpty()) {
        const bool WANTS_KERNEL = MODULE_FILTER == "kernel";
        const bool WANTS_USERSPACE = MODULE_FILTER == "userspace" || MODULE_FILTER == "user";
        const bool KERNEL_ADDRESS = *address >= 0xffffffff80000000ULL;
        const bool MODULE_MATCHES =
            (module != nullptr) && (module->name.toLower().contains(MODULE_FILTER) || module->role.toLower().contains(MODULE_FILTER));
        if ((WANTS_KERNEL && !KERNEL_ADDRESS) || (WANTS_USERSPACE && KERNEL_ADDRESS) ||
            (!WANTS_KERNEL && !WANTS_USERSPACE && !MODULE_MATCHES)) {
            return QJsonObject{{"ok", true},
                               {"dumpId", session->id},
                               {"address", format_hex(*address)},
                               {"matchedFilter", false},
                               {"moduleFilter", MODULE_FILTER}};
        }
    }
    const auto* seg = wosdbg::find_segment_for_va(*session->dump, *address);
    QJsonObject file_location;
    if (seg != nullptr) {
        file_location = QJsonObject{{"coredumpFileOffset", format_hex(seg->file_offset + (*address - seg->vaddr))},
                                    {"segmentLoadBase", format_hex(seg->vaddr)},
                                    {"segmentOffset", format_hex(*address - seg->vaddr)},
                                    {"segmentType", seg->type_name()}};
    }
    return QJsonObject{{"ok", true},
                       {"dumpId", session->id},
                       {"address", address_description_to_json(describe_address(*session, *address))},
                       {"module", (module != nullptr) ? module_json(*module) : QJsonObject{}},
                       {"source", source_location_for_address(*session, *address, module)},
                       {"mapping", pte_info_for_address(*session, *address)},
                       {"fileLocation", file_location}};
}

auto DebugAnalysisService::verify_embedded_elf(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    QByteArray const EMBEDDED = session->dump->embedded_elf();
    if (EMBEDDED.isEmpty()) {
        return tool_error("coredump has no embedded ELF buffer");
    }

    QString local_path = args["path"].toString(args["elfPath"].toString());
    if (local_path.isEmpty()) {
        local_path = session->binary_elf_path;
    }
    if (local_path.isEmpty() && !session->embedded_build_id.isEmpty()) {
        local_path = first_existing_candidate(build_id_debug_candidates(session->embedded_build_id));
    }
    if (local_path.isEmpty() && !session->embedded_build_id.isEmpty()) {
        local_path = first_candidate_with_build_id(default_elf_candidates(session->dump->exe_path), session->embedded_build_id);
    }
    if (local_path.isEmpty()) {
        return tool_error("verify_embedded_elf requires a local ELF path or discoverable build-id match");
    }
    local_path = resolve_path_for_read(local_path);
    if (!is_path_allowed(local_path)) {
        return tool_error(QString("ELF path is outside allowed roots: %1").arg(local_path));
    }
    QByteArray const LOCAL = read_file_bytes_quiet(local_path);
    if (LOCAL.isEmpty()) {
        return tool_error(QString("could not read local ELF: %1").arg(local_path));
    }

    int const MAX_RANGES = bounded_int(args, "maxRanges", 16, 1, 128);
    const auto FIRST = first_mismatch_offset(EMBEDDED, LOCAL);
    QJsonObject result{{"ok", true},
                       {"dumpId", session->id},
                       {"embeddedSize", QString::number(EMBEDDED.size())},
                       {"embeddedBuildId", wosdbg::elf_build_id(EMBEDDED)},
                       {"embeddedHash", sha256_hex(EMBEDDED)},
                       {"localPath", local_path},
                       {"localSize", QString::number(LOCAL.size())},
                       {"localBuildId", wosdbg::elf_build_id(LOCAL)},
                       {"localHash", sha256_hex(LOCAL)},
                       {"buildIdMatches", wosdbg::elf_build_id(EMBEDDED) == wosdbg::elf_build_id(LOCAL)},
                       {"matches", !FIRST.has_value()},
                       {"firstMismatchOffset", FIRST ? format_hex(*FIRST) : QString()},
                       {"mismatchRanges", mismatch_ranges_json(EMBEDDED, LOCAL, MAX_RANGES)}};
    if (FIRST) {
        QByteArray const PATTERN = EMBEDDED.mid(static_cast<qsizetype>(*FIRST), std::min<qsizetype>(256, EMBEDDED.size() - *FIRST));
        if (PATTERN.size() >= 16) {
            const qsizetype FOUND = LOCAL.indexOf(PATTERN);
            if (FOUND >= 0 && static_cast<uint64_t>(FOUND) != *FIRST) {
                result["embeddedDifferingBytesAlsoMatchLocalOffset"] = format_hex(static_cast<uint64_t>(FOUND));
            }
        }
    }
    return result;
}

auto DebugAnalysisService::check_elf_mapping(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    auto address = resolve_address_argument(*session, args, "rip");
    if (!address) {
        return tool_error("check_elf_mapping requires an address or register");
    }
    const auto* module = module_for_address(*session, *address);
    if (module == nullptr) {
        return tool_error(QString("no discovered ELF module covers %1").arg(format_hex(*address)));
    }
    const Config& cfg = (config != nullptr) ? *config : ConfigService::instance().get_config();
    int const BYTES = bounded_int(args, "bytes", static_cast<int>(K_PAGE_SIZE), 16, cfg.get_mcp_settings().max_memory_bytes);
    int const MAX_RANGES = bounded_int(args, "maxRanges", 8, 1, 64);
    const uint64_t PAGE_START = page_align_down(*address);
    const uint64_t COMPARE_START = args["page"].toBool(true) ? PAGE_START : *address;
    QByteArray const ACTUAL = wosdbg::read_va_bytes(*session->dump, COMPARE_START, static_cast<size_t>(BYTES));
    if (ACTUAL.isEmpty()) {
        return tool_error(QString("coredump has no captured bytes at %1").arg(format_hex(COMPARE_START)));
    }

    QJsonArray sources;
    auto append_source = [&](const QString& label, const QString& path, const QByteArray& elf, uint64_t base) {
        wosdbg::ElfImageInfo const INFO = wosdbg::elf_image_info(elf);
        QJsonObject source{{"label", label}, {"path", path}, {"buildId", wosdbg::elf_build_id(elf)}, {"loadBase", format_hex(base)}};
        if (!INFO.valid) {
            source["available"] = false;
            source["note"] = "not a valid ELF";
            sources.append(source);
            return;
        }
        auto mapping = elf_va_mapping(INFO, COMPARE_START, base);
        if (!mapping || !mapping->file_backed) {
            source["available"] = false;
            source["note"] = "runtime VA is not backed by a PT_LOAD file range";
            sources.append(source);
            return;
        }
        QByteArray const EXPECTED = bytes_at_file_offset(elf, mapping->file_offset, ACTUAL.size());
        source["available"] = !EXPECTED.isEmpty();
        source["vaToFile"] = elf_va_mapping_json(*mapping);
        if (!EXPECTED.isEmpty()) {
            QJsonObject comparison = compare_byte_sources_json(ACTUAL, EXPECTED, elf, mapping->file_offset, MAX_RANGES);
            source["comparison"] = comparison;
            if (!comparison["matches"].toBool()) {
                source["diagnosis"] = QString("mapped bytes at %1 do not match ELF file offset %2")
                                          .arg(format_hex(COMPARE_START), format_hex(mapping->file_offset));
            }
        }
        sources.append(source);
    };

    QByteArray const MODULE_ELF = module_elf_bytes(*session, *module);
    append_source("module", module->path, MODULE_ELF, module->base);
    if (module->role == "main" && !session->binary_elf_path.isEmpty() && session->binary_elf_path != module->path) {
        append_source("local-main", session->binary_elf_path, read_file_bytes_quiet(session->binary_elf_path), module->base);
    }
    if (module->role == "main" && !session->dump->embedded_elf().isEmpty() && module->path != "embedded:main") {
        append_source("embedded-main", "embedded:main", session->dump->embedded_elf(), module->base);
    }

    return QJsonObject{{"ok", true},
                       {"dumpId", session->id},
                       {"address", format_hex(*address)},
                       {"compareStart", format_hex(COMPARE_START)},
                       {"bytes", QString::number(ACTUAL.size())},
                       {"module", module_json(*module)},
                       {"symbol", address_description_to_json(describe_address(*session, *address))},
                       {"actualHash", sha256_hex(ACTUAL)},
                       {"actualPreview", hex_preview(ACTUAL, 32)},
                       {"sources", sources}};
}

auto DebugAnalysisService::find_duplicate_pages(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    bool const EXECUTABLE_ONLY = args["executableOnly"].toBool(true);
    bool const USER_ONLY = args["userOnly"].toBool(true);
    int const MAX_GROUPS = bounded_int(args, "maxGroups", 16, 1, 256);

    QHash<QString, QJsonArray> pages_by_hash;
    for (int i = 0; i < static_cast<int>(session->dump->segments.size()); ++i) {
        const auto& seg = session->dump->segments[static_cast<size_t>(i)];
        if (!seg.is_present() || seg.size == 0) {
            continue;
        }
        if (USER_ONLY && seg.vaddr >= K_USER_SPACE_END) {
            continue;
        }
        if (EXECUTABLE_ONLY && (seg.pte_flags & K_PTE_NX) != 0) {
            continue;
        }
        QByteArray const BYTES =
            wosdbg::read_va_bytes(*session->dump, seg.vaddr, static_cast<size_t>(std::min<uint64_t>(seg.size, K_PAGE_SIZE)));
        if (BYTES.size() < static_cast<int>(K_PAGE_SIZE)) {
            continue;
        }
        const QString HASH = sha256_hex(BYTES);
        const auto* module = module_for_address(*session, seg.vaddr);
        QJsonObject page{{"address", format_hex(seg.vaddr)},
                         {"segmentIndex", i},
                         {"segmentType", seg.type_name()},
                         {"pte", pte_flags_to_json(seg.pte_flags)},
                         {"module", (module != nullptr) ? module->name : QString()},
                         {"role", (module != nullptr) ? module->role : QString()}};
        if (module != nullptr) {
            QByteArray const ELF = module_elf_bytes(*session, *module);
            auto mapping = elf_va_mapping(wosdbg::elf_image_info(ELF), seg.vaddr, module->base);
            if (mapping) {
                page["fileOffset"] = format_hex(mapping->file_offset);
                page["elfVaddr"] = format_hex(mapping->elf_vaddr);
            }
        }
        QJsonArray arr = pages_by_hash.value(HASH);
        arr.append(page);
        pages_by_hash.insert(HASH, arr);
    }

    QJsonArray groups;
    for (auto it = pages_by_hash.cbegin(); it != pages_by_hash.cend() && groups.size() < MAX_GROUPS; ++it) {
        QJsonArray pages = it.value();
        if (pages.size() < 2) {
            continue;
        }
        QList<uint64_t> addrs;
        for (const auto& value : pages) {
            auto parsed = parse_address_value(value.toObject()["address"]);
            if (parsed) {
                addrs << *parsed;
            }
        }
        std::sort(addrs.begin(), addrs.end());
        bool regular_delta = addrs.size() > 2;
        uint64_t delta = regular_delta ? addrs[1] - addrs[0] : 0;
        for (int i = 2; regular_delta && i < addrs.size(); ++i) {
            regular_delta = addrs[i] - addrs[i - 1] == delta;
        }
        groups.append(QJsonObject{{"hash", it.key()},
                                  {"count", pages.size()},
                                  {"regularDelta", regular_delta ? format_hex(delta) : QString()},
                                  {"pages", pages}});
    }
    return QJsonObject{
        {"ok", true}, {"dumpId", session->id}, {"executableOnly", EXECUTABLE_ONLY}, {"userOnly", USER_ONLY}, {"duplicateGroups", groups}};
}

auto DebugAnalysisService::analyze_elf_integrity(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    QJsonObject mapping_args = args;
    if (!mapping_args.contains("register") && !mapping_args.contains("address")) {
        mapping_args["register"] = "rip";
    }
    if (!mapping_args.contains("bytes")) {
        mapping_args["bytes"] = static_cast<int>(K_PAGE_SIZE);
    }
    QJsonObject verify = verify_embedded_elf(QJsonObject{{"dumpId", session->id}, {"path", session->binary_elf_path}, {"maxRanges", 8}});
    QJsonObject mapping = check_elf_mapping(mapping_args);
    QJsonObject duplicates = find_duplicate_pages(QJsonObject{{"dumpId", session->id}, {"executableOnly", true}, {"maxGroups", 8}});

    QStringList labels;
    if (verify["ok"].toBool() && !verify["matches"].toBool()) {
        labels << "embedded ELF differs from local matching binary";
    }
    if (mapping["ok"].toBool()) {
        for (const auto& source_value : mapping["sources"].toArray()) {
            QJsonObject source = source_value.toObject();
            if (source["comparison"].toObject().contains("matches") && !source["comparison"].toObject()["matches"].toBool()) {
                labels << "mapped executable bytes differ from PT_LOAD source bytes";
                break;
            }
        }
    }
    if (duplicates["duplicateGroups"].toArray().size() > 0) {
        labels << "duplicate executable pages detected";
    }
    QString diagnosis = labels.isEmpty() ? "no obvious ELF/mapped-page integrity issue in sampled checks"
                                         : "possible stale/corrupt executable mapping or embedded ELF buffer";
    return QJsonObject{{"ok", true},
                       {"dumpId", session->id},
                       {"diagnosis", diagnosis},
                       {"labels", QJsonArray::fromStringList(labels)},
                       {"embeddedElf", verify},
                       {"mapping", mapping},
                       {"duplicatePages", duplicates}};
}

auto DebugAnalysisService::elf_layout_summary(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    QJsonArray modules;
    for (const auto& module : session->modules) {
        if (args.contains("module") && !module.name.contains(args["module"].toString(), Qt::CaseInsensitive) &&
            !module.role.contains(args["module"].toString(), Qt::CaseInsensitive)) {
            continue;
        }
        QByteArray const ELF = module_elf_bytes(*session, module);
        wosdbg::ElfImageInfo const INFO = wosdbg::elf_image_info(ELF);
        QJsonArray loads;
        for (int i = 0; i < static_cast<int>(INFO.load_segments.size()); ++i) {
            const auto& load = INFO.load_segments[static_cast<size_t>(i)];
            const uint64_t RUNTIME_START = module.base + load.vaddr;
            loads.append(QJsonObject{{"index", i},
                                     {"runtimeStart", format_hex(RUNTIME_START)},
                                     {"runtimeEnd", format_hex(RUNTIME_START + load.memsz)},
                                     {"elfVaddr", format_hex(load.vaddr)},
                                     {"fileOffset", format_hex(load.offset)},
                                     {"fileSize", format_hex(load.filesz)},
                                     {"memSize", format_hex(load.memsz)},
                                     {"flags", format_hex(load.flags)},
                                     {"read", (load.flags & 0x4U) != 0},
                                     {"write", (load.flags & 0x2U) != 0},
                                     {"execute", (load.flags & 0x1U) != 0}});
        }
        modules.append(QJsonObject{{"module", module_json(module)},
                                   {"elfValid", INFO.valid},
                                   {"elfType", INFO.type == 3 ? "ET_DYN/PIE" : QString("0x%1").arg(INFO.type, 0, 16)},
                                   {"entryElf", format_hex(INFO.entry)},
                                   {"entryRuntime", format_hex(module.base + INFO.entry)},
                                   {"interpreter", INFO.interpreter},
                                   {"loadSegments", loads}});
    }
    return QJsonObject{{"ok", true}, {"dumpId", session->id}, {"modules", modules}};
}

auto DebugAnalysisService::compare_expected_disassembly(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    auto address = resolve_address_argument(*session, args, "rip");
    if (!address) {
        return tool_error("compare_expected_disassembly requires an address or register");
    }
    const auto* module = module_for_address(*session, *address);
    if (module == nullptr) {
        return tool_error(QString("no discovered ELF module covers %1").arg(format_hex(*address)));
    }
    int const COUNT = bounded_int(args, "instructions", 12, 1, 64);
    int const BYTES = bounded_int(args, "bytes", 128, 16, 4096);
    QByteArray const ACTUAL = wosdbg::read_va_bytes(*session->dump, *address, static_cast<size_t>(BYTES));
    QByteArray const ELF = module_elf_bytes(*session, *module);
    auto mapping = elf_va_mapping(wosdbg::elf_image_info(ELF), *address, module->base);
    QByteArray expected;
    if (mapping && mapping->file_backed) {
        expected = bytes_at_file_offset(ELF, mapping->file_offset, BYTES);
    }
    return QJsonObject{{"ok", true},
                       {"dumpId", session->id},
                       {"address", format_hex(*address)},
                       {"module", module_json(*module)},
                       {"vaToFile", mapping ? elf_va_mapping_json(*mapping) : QJsonObject{}},
                       {"actual", disassemble_bytes_json(ACTUAL, *address, COUNT, "coredump-memory")},
                       {"expected", disassemble_bytes_json(expected, *address, COUNT, QString("module-elf:%1").arg(module->name))},
                       {"byteComparison", compare_byte_sources_json(ACTUAL, expected, ELF, mapping ? mapping->file_offset : 0, 8)}};
}

auto DebugAnalysisService::scan_chunk_corruption(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    QList<uint64_t> chunk_sizes = {K_PAGE_SIZE, 64ULL * 1024ULL, 256ULL * 1024ULL, 2ULL * 1024ULL * 1024ULL};
    if (args["chunkSizes"].isArray()) {
        chunk_sizes.clear();
        for (const auto& value : args["chunkSizes"].toArray()) {
            if (auto parsed = parse_address_value(value); parsed && *parsed > 0) {
                chunk_sizes << *parsed;
            }
        }
    }
    int const MAX_HITS = bounded_int(args, "maxHits", 32, 1, 512);
    QJsonArray hits;
    for (const auto& module : session->modules) {
        if (module.role == "kernel") {
            continue;
        }
        QByteArray const ELF = module_elf_bytes(*session, module);
        wosdbg::ElfImageInfo const INFO = wosdbg::elf_image_info(ELF);
        if (!INFO.valid) {
            continue;
        }
        for (const auto& seg : session->dump->segments) {
            if (!seg.is_present() || seg.vaddr >= K_USER_SPACE_END || module_for_address(*session, seg.vaddr) != &module) {
                continue;
            }
            auto mapping = elf_va_mapping(INFO, seg.vaddr, module.base);
            if (!mapping || !mapping->file_backed) {
                continue;
            }
            const uint64_t IN_LOAD = mapping->elf_vaddr - mapping->segment_vaddr;
            const uint64_t FILE_BACKED_BYTES = mapping->segment_filesz - IN_LOAD;
            const uint64_t COMPARE_BYTES = std::min<uint64_t>({seg.size, K_PAGE_SIZE, FILE_BACKED_BYTES});
            QByteArray const ACTUAL = wosdbg::read_va_bytes(*session->dump, seg.vaddr, static_cast<size_t>(COMPARE_BYTES));
            QByteArray const EXPECTED = bytes_at_file_offset(ELF, mapping->file_offset, ACTUAL.size());
            if (ACTUAL.isEmpty() || EXPECTED.isEmpty() || ACTUAL == EXPECTED) {
                continue;
            }
            QJsonObject comparison = compare_byte_sources_json(ACTUAL, EXPECTED, ELF, mapping->file_offset, 4);
            QJsonArray chunk_clues;
            auto matched = parse_address_value(comparison["actualBytesAlsoMatchFileOffset"]);
            if (matched) {
                qint64 const DELTA = static_cast<qint64>(*matched) - static_cast<qint64>(mapping->file_offset);
                for (uint64_t chunk : chunk_sizes) {
                    if (chunk != 0 && (std::llabs(DELTA) % static_cast<qint64>(chunk)) == 0) {
                        chunk_clues.append(QJsonObject{{"chunkSize", format_hex(chunk)},
                                                       {"fileOffsetDelta", format_hex(static_cast<uint64_t>(std::llabs(DELTA)))}});
                    }
                }
            }
            hits.append(QJsonObject{{"module", module.name},
                                    {"address", format_hex(seg.vaddr)},
                                    {"expectedFileOffset", format_hex(mapping->file_offset)},
                                    {"comparison", comparison},
                                    {"chunkClues", chunk_clues}});
            if (hits.size() >= MAX_HITS) {
                return QJsonObject{{"ok", true}, {"dumpId", session->id}, {"truncated", true}, {"hits", hits}};
            }
        }
    }
    return QJsonObject{{"ok", true}, {"dumpId", session->id}, {"truncated", false}, {"hits", hits}};
}

auto DebugAnalysisService::audit_executable_ptes(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    int const MAX_ISSUES = bounded_int(args, "maxIssues", 64, 1, 1024);
    QJsonArray issues;
    for (const auto& seg : session->dump->segments) {
        if (!seg.is_present() || seg.vaddr >= K_USER_SPACE_END) {
            continue;
        }
        const auto* module = module_for_address(*session, seg.vaddr);
        if (module == nullptr) {
            continue;
        }
        QByteArray const ELF = module_elf_bytes(*session, *module);
        auto mapping = elf_va_mapping(wosdbg::elf_image_info(ELF), seg.vaddr, module->base);
        if (!mapping) {
            continue;
        }
        const bool EXPECT_EXEC = (mapping->segment_flags & 0x1U) != 0;
        const bool EXPECT_WRITE = (mapping->segment_flags & 0x2U) != 0;
        const bool PTE_NX = (seg.pte_flags & K_PTE_NX) != 0;
        const bool PTE_WRITE = (seg.pte_flags & K_PTE_WRITE) != 0;
        const bool PTE_USER = (seg.pte_flags & K_PTE_USER) != 0;
        QStringList labels;
        if (EXPECT_EXEC && PTE_NX) {
            labels << "expected executable PT_LOAD page is NX";
        }
        if (EXPECT_EXEC && PTE_WRITE) {
            labels << "executable image page is writable";
        }
        if (!EXPECT_WRITE && PTE_WRITE) {
            labels << "read-only PT_LOAD page is writable";
        }
        if (!PTE_USER && module->role != "kernel") {
            labels << "userspace image page lacks user bit";
        }
        if (!labels.isEmpty()) {
            issues.append(QJsonObject{{"address", format_hex(seg.vaddr)},
                                      {"module", module->name},
                                      {"role", module->role},
                                      {"labels", QJsonArray::fromStringList(labels)},
                                      {"pte", pte_flags_to_json(seg.pte_flags)},
                                      {"vaToFile", elf_va_mapping_json(*mapping)}});
            if (issues.size() >= MAX_ISSUES) {
                break;
            }
        }
    }
    return QJsonObject{{"ok", true}, {"dumpId", session->id}, {"issues", issues}, {"issueCount", issues.size()}};
}

auto DebugAnalysisService::recognize_startup_stack(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    const auto REGS = register_map(*session->dump, args["frame"].toString("trap"));
    uint64_t const RSP = resolve_address_argument(*session, args, "rsp").value_or(REGS.value("rsp"));
    QJsonArray stack_slots;
    QStringList hints;
    QByteArray const STACK = wosdbg::read_va_bytes(*session->dump, RSP, 96);
    for (int off = 0; off + 7 < STACK.size(); off += 8) {
        uint64_t const VALUE = read_u64_le(STACK.constData() + off);
        AddressDescription const DESC = describe_address(*session, VALUE);
        QString hint;
        if (DESC.symbol.contains("_start") || DESC.symbol.contains("__mlibc_entry")) {
            hint = "startup-related return/code pointer";
            hints << QString("stack slot %1 points near %2").arg(format_hex(RSP + static_cast<uint64_t>(off)), DESC.symbol);
        }
        stack_slots.append(QJsonObject{{"slot", format_hex(RSP + static_cast<uint64_t>(off))},
                                       {"value", format_hex(VALUE)},
                                       {"symbol", DESC.symbol},
                                       {"module", DESC.object_name},
                                       {"hint", hint}});
    }
    QString shape = hints.isEmpty() ? "unknown" : "possible _start -> __mlibc_entry startup stack";
    return QJsonObject{{"ok", true},
                       {"dumpId", session->id},
                       {"rsp", format_hex(RSP)},
                       {"shape", shape},
                       {"hints", QJsonArray::fromStringList(hints)},
                       {"slots", stack_slots}};
}

auto DebugAnalysisService::correlate_coredump_logs(const QJsonObject& args) const -> QJsonObject {
    const auto* dump_session = find_dump_session(args["dumpId"].toString());
    if (dump_session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    QStringList log_ids;
    if (!args["logId"].toString().isEmpty()) {
        log_ids << args["logId"].toString();
    } else {
        for (const auto& key : sorted_hash_keys(log_sessions)) {
            log_ids << key;
        }
    }
    if (log_ids.isEmpty()) {
        return tool_error("No loaded logs. Call wosdbg.load_log first or pass logId.");
    }
    QStringList needles = {QString("pid=%1").arg(dump_session->dump->pid), QString("PID: 0x%1").arg(dump_session->dump->pid, 0, 16),
                           format_hex(dump_session->dump->trap_frame.rip), format_hex(dump_session->dump->cr2),
                           dump_session->dump->exe_path};
    int const MAX_HITS = bounded_int(args, "maxHits", 32, 1, 256);
    QJsonArray hits;
    for (const auto& log_id : log_ids) {
        const auto* log_session = find_log_session(log_id);
        if (log_session == nullptr) {
            continue;
        }
        for (const auto& entry : log_session->entries) {
            const QString TEXT = QString::fromStdString(entry.original_line + " " + entry.function + " " + entry.assembly);
            for (const auto& needle : needles) {
                if (!needle.isEmpty() && TEXT.contains(needle, Qt::CaseInsensitive)) {
                    hits.append(QJsonObject{
                        {"logId", log_id}, {"logPath", log_session->path}, {"matched", needle}, {"entry", log_entry_excerpt(entry)}});
                    break;
                }
            }
            if (hits.size() >= MAX_HITS) {
                return QJsonObject{{"ok", true}, {"dumpId", dump_session->id}, {"truncated", true}, {"hits", hits}};
            }
        }
    }
    return QJsonObject{{"ok", true}, {"dumpId", dump_session->id}, {"truncated", false}, {"hits", hits}};
}

auto DebugAnalysisService::reconstruct_wki_trace(const QJsonObject& args) const -> QJsonObject {
    QStringList log_ids;
    if (!args["logId"].toString().isEmpty()) {
        log_ids << args["logId"].toString();
    } else {
        for (const auto& key : sorted_hash_keys(log_sessions)) {
            log_ids << key;
        }
    }
    if (log_ids.isEmpty()) {
        return tool_error("No loaded logs. Call wosdbg.load_log first or pass logId.");
    }
    QRegularExpression const WKI_RE(R"((WKI|wki|OP_VFS|VFS_REF|READ_RDMA|READ_BULK|remote_vfs|remote_compute|RDMA))");
    QString const FILTER = args["filter"].toString();
    int const MAX_HITS = bounded_int(args, "maxHits", 128, 1, 1024);
    QJsonArray events;
    for (const auto& log_id : log_ids) {
        const auto* log_session = find_log_session(log_id);
        if (log_session == nullptr) {
            continue;
        }
        for (const auto& entry : log_session->entries) {
            const QString TEXT = QString::fromStdString(entry.original_line + " " + entry.function + " " + entry.assembly);
            if (!WKI_RE.match(TEXT).hasMatch()) {
                continue;
            }
            if (!FILTER.isEmpty() && !TEXT.contains(FILTER, Qt::CaseInsensitive)) {
                continue;
            }
            events.append(QJsonObject{{"logId", log_id}, {"entry", log_entry_excerpt(entry)}});
            if (events.size() >= MAX_HITS) {
                return QJsonObject{{"ok", true}, {"truncated", true}, {"events", events}};
            }
        }
    }
    return QJsonObject{{"ok", true}, {"truncated", false}, {"events", events}};
}

auto DebugAnalysisService::build_distributed_timeline(const QJsonObject& args) const -> QJsonObject {
    QStringList log_ids;
    for (const auto& value : args["logIds"].toArray()) {
        if (!value.toString().isEmpty()) {
            log_ids << value.toString();
        }
    }
    if (log_ids.isEmpty()) {
        for (const auto& key : sorted_hash_keys(log_sessions)) {
            log_ids << key;
        }
    }
    if (log_ids.isEmpty()) {
        return tool_error("No loaded logs. Call wosdbg.load_log first or pass logIds.");
    }

    const int MAX_EVENTS = bounded_int(args, "maxEvents", 512, 1, 4096);
    const int CONTEXT = bounded_int(args, "context", 0, 0, 32);
    const bool EXPAND = args["expandCorrelations"].toBool(true);
    const QString QUERY = args["query"].toString();
    QRegularExpression selector;
    if (!QUERY.isEmpty()) {
        selector = args["regex"].toBool(false)
                       ? QRegularExpression(QUERY, QRegularExpression::CaseInsensitiveOption)
                       : QRegularExpression(QRegularExpression::escape(QUERY), QRegularExpression::CaseInsensitiveOption);
        if (!selector.isValid()) {
            return tool_error("Invalid query regex");
        }
    } else {
        selector = QRegularExpression(
            R"(\b(WKI|VFS_REF|remote_(?:vfs|compute|ipc)|RDMA|request|response|submit|launch|complete|timeout|retry|fence|peer)\b)",
            QRegularExpression::CaseInsensitiveOption);
    }

    QSet<QString> requested_keys;
    for (const auto& value : args["correlationKeys"].toArray()) {
        if (!value.toString().isEmpty()) {
            requested_keys.insert(value.toString());
        }
    }

    const QRegularExpression IDENTIFIER_RE(
        R"(\b(cookie|request(?:_?id)?|req(?:_?id)?|task(?:_?id)?|resource(?:_?id)?|rid|peer|pid|fd|channel|sequence|seq)\s*[:=#]\s*([A-Za-z0-9_.:/-]+))",
        QRegularExpression::CaseInsensitiveOption);
    const QRegularExpression EXPLICIT_TIME_RE(R"(\b(?:ts|time|timestamp)\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)\s*(ns|us|ms|s)?\b)",
                                              QRegularExpression::CaseInsensitiveOption);
    const QRegularExpression BRACKET_TIME_RE(R"(^\s*\[\s*([0-9]+(?:\.[0-9]+)?)\s*(ns|us|ms|s)?\s*\])",
                                             QRegularExpression::CaseInsensitiveOption);

    auto correlation_fields = [&IDENTIFIER_RE](const QString& text, QSet<QString>* values) {
        QJsonObject fields;
        auto it = IDENTIFIER_RE.globalMatch(text);
        while (it.hasNext()) {
            const auto MATCH = it.next();
            const QString NAME = MATCH.captured(1).toLower();
            const QString VALUE = MATCH.captured(2);
            fields[NAME] = VALUE;
            values->insert(QString("%1=%2").arg(NAME, VALUE));
        }
        return fields;
    };
    auto timestamp_ns = [&EXPLICIT_TIME_RE, &BRACKET_TIME_RE](const QString& text) -> std::optional<double> {
        auto match = EXPLICIT_TIME_RE.match(text);
        if (!match.hasMatch()) {
            match = BRACKET_TIME_RE.match(text);
        }
        if (!match.hasMatch()) {
            return std::nullopt;
        }
        bool ok = false;
        double value = match.captured(1).toDouble(&ok);
        if (!ok) {
            return std::nullopt;
        }
        const QString UNIT = match.captured(2).toLower();
        if (UNIT == "s" || UNIT.isEmpty()) {
            value *= 1'000'000'000.0;
        } else if (UNIT == "ms") {
            value *= 1'000'000.0;
        } else if (UNIT == "us") {
            value *= 1'000.0;
        }
        return value;
    };

    struct Candidate {
        QString log_id;
        QString path;
        int lane = 0;
        int row = 0;
        QString text;
        QJsonObject entry;
        QJsonObject fields;
        QSet<QString> correlation_values;
        std::optional<double> timestamp;
        bool direct_match = false;
        bool context_match = false;
        bool correlation_match = false;
    };

    std::vector<Candidate> candidates;
    QSet<QString> seed_values = requested_keys;
    int lane = 0;
    for (const auto& log_id : log_ids) {
        const auto* session = find_log_session(log_id);
        if (session == nullptr) {
            continue;
        }
        QSet<int> direct_rows;
        for (int row = 0; std::cmp_less(row, session->entries.size()); ++row) {
            const auto& entry = session->entries[static_cast<size_t>(row)];
            const QString TEXT = QString::fromStdString(entry.original_line + " " + entry.function + " " + entry.assembly);
            bool direct = selector.match(TEXT).hasMatch();
            if (!requested_keys.isEmpty()) {
                direct =
                    std::ranges::any_of(requested_keys, [&TEXT](const QString& key) { return TEXT.contains(key, Qt::CaseInsensitive); });
            }
            if (direct) {
                direct_rows.insert(row);
            }
        }
        QSet<int> selected_rows = direct_rows;
        for (const int ROW : direct_rows) {
            for (int nearby = std::max(0, ROW - CONTEXT); nearby <= std::min(static_cast<int>(session->entries.size()) - 1, ROW + CONTEXT);
                 ++nearby) {
                selected_rows.insert(nearby);
            }
        }
        for (const int ROW : selected_rows) {
            const auto& entry = session->entries[static_cast<size_t>(ROW)];
            const QString TEXT = QString::fromStdString(entry.original_line + " " + entry.function + " " + entry.assembly);
            Candidate candidate;
            candidate.log_id = log_id;
            candidate.path = session->path;
            candidate.lane = lane;
            candidate.row = ROW;
            candidate.text = TEXT;
            candidate.entry = log_entry_to_json(entry, false);
            candidate.direct_match = direct_rows.contains(ROW);
            candidate.context_match = !direct_rows.contains(ROW);
            candidate.fields = correlation_fields(TEXT, &candidate.correlation_values);
            if (candidate.direct_match) {
                for (const auto& value : candidate.correlation_values) {
                    if (!value.endsWith("=0") && !value.endsWith("=null") && !value.endsWith("=none")) {
                        seed_values.insert(value);
                    }
                }
            }
            candidate.timestamp = timestamp_ns(TEXT);
            candidates.push_back(std::move(candidate));
        }
        ++lane;
    }

    if (EXPAND && !seed_values.isEmpty()) {
        lane = 0;
        for (const auto& log_id : log_ids) {
            const auto* session = find_log_session(log_id);
            if (session == nullptr) {
                continue;
            }
            for (int row = 0; std::cmp_less(row, session->entries.size()); ++row) {
                const bool ALREADY_SELECTED = std::ranges::any_of(
                    candidates, [&log_id, row](const Candidate& candidate) { return candidate.log_id == log_id && candidate.row == row; });
                if (ALREADY_SELECTED) {
                    continue;
                }
                const auto& entry = session->entries[static_cast<size_t>(row)];
                const QString TEXT = QString::fromStdString(entry.original_line + " " + entry.function + " " + entry.assembly);
                QSet<QString> candidate_values;
                const QJsonObject FIELDS = correlation_fields(TEXT, &candidate_values);
                const bool MATCHES_SEED =
                    std::ranges::any_of(candidate_values, [&seed_values](const QString& key) { return seed_values.contains(key); });
                if (!MATCHES_SEED) {
                    continue;
                }
                Candidate candidate;
                candidate.log_id = log_id;
                candidate.path = session->path;
                candidate.lane = lane;
                candidate.row = row;
                candidate.text = TEXT;
                candidate.entry = log_entry_to_json(entry, false);
                candidate.correlation_match = true;
                candidate.fields = FIELDS;
                candidate.correlation_values = candidate_values;
                candidate.timestamp = timestamp_ns(TEXT);
                candidates.push_back(std::move(candidate));
            }
            ++lane;
        }
    }

    std::ranges::sort(candidates, [](const Candidate& left, const Candidate& right) {
        if (left.lane != right.lane) {
            return left.lane < right.lane;
        }
        return left.row < right.row;
    });

    const bool TRUNCATED = std::cmp_greater(candidates.size(), MAX_EVENTS);
    if (TRUNCATED) {
        std::vector<std::vector<Candidate>> by_lane(static_cast<size_t>(log_ids.size()));
        for (auto& candidate : candidates) {
            by_lane[static_cast<size_t>(candidate.lane)].push_back(std::move(candidate));
        }
        candidates.clear();
        std::vector<size_t> lane_positions(by_lane.size(), 0);
        while (std::cmp_less(candidates.size(), MAX_EVENTS)) {
            bool made_progress = false;
            for (size_t lane_index = 0; lane_index < by_lane.size() && std::cmp_less(candidates.size(), MAX_EVENTS); ++lane_index) {
                if (lane_positions[lane_index] >= by_lane[lane_index].size()) {
                    continue;
                }
                candidates.push_back(std::move(by_lane[lane_index][lane_positions[lane_index]++]));
                made_progress = true;
            }
            if (!made_progress) {
                break;
            }
        }
        std::ranges::sort(candidates, [](const Candidate& left, const Candidate& right) {
            if (left.lane != right.lane) {
                return left.lane < right.lane;
            }
            return left.row < right.row;
        });
    }

    QJsonArray events;
    QJsonArray lanes;
    QHash<QString, QSet<QString>> correlation_logs;
    QHash<QString, int> correlation_counts;
    int timestamped = 0;
    for (const auto& log_id : log_ids) {
        const auto* session = find_log_session(log_id);
        if (session != nullptr) {
            lanes.append(QJsonObject{{"logId", log_id}, {"path", session->path}, {"lane", lanes.size()}});
        }
    }
    for (const auto& candidate : candidates) {
        QString match_kind = "correlation";
        if (candidate.direct_match) {
            match_kind = "query";
        } else if (candidate.context_match) {
            match_kind = "context";
        }
        QJsonObject event{{"logId", candidate.log_id}, {"logPath", candidate.path},       {"lane", candidate.lane}, {"row", candidate.row},
                          {"entry", candidate.entry},  {"correlation", candidate.fields}, {"match", match_kind}};
        if (candidate.timestamp) {
            event["timestampNs"] = *candidate.timestamp;
            ++timestamped;
        }
        events.append(event);
        for (const auto& value : candidate.correlation_values) {
            correlation_logs[value].insert(candidate.log_id);
            ++correlation_counts[value];
        }
    }

    QJsonArray cross_log_correlations;
    for (const auto& key : sorted_hash_keys(correlation_logs)) {
        const auto& log_set = correlation_logs[key];
        if (log_set.size() < 2) {
            continue;
        }
        QStringList correlated_logs = log_set.values();
        std::ranges::sort(correlated_logs);
        cross_log_correlations.append(QJsonObject{
            {"key", key}, {"occurrences", correlation_counts.value(key)}, {"logIds", QJsonArray::fromStringList(correlated_logs)}});
    }

    QJsonArray clock_ordered_events;
    std::vector<QJsonObject> timestamped_events;
    for (const auto& value : events) {
        if (value.toObject().contains("timestampNs")) {
            timestamped_events.push_back(value.toObject());
        }
    }
    std::ranges::sort(timestamped_events, [](const QJsonObject& left, const QJsonObject& right) {
        const double left_timestamp = left["timestampNs"].toDouble();
        const double right_timestamp = right["timestampNs"].toDouble();
        if (left_timestamp != right_timestamp) {
            return left_timestamp < right_timestamp;
        }
        const int left_lane = left["lane"].toInt();
        const int right_lane = right["lane"].toInt();
        if (left_lane != right_lane) {
            return left_lane < right_lane;
        }
        const int left_row = left["row"].toInt();
        const int right_row = right["row"].toInt();
        if (left_row != right_row) {
            return left_row < right_row;
        }
        return left["logId"].toString() < right["logId"].toString();
    });
    for (const auto& event : timestamped_events) {
        clock_ordered_events.append(event);
    }

    QString clock_quality = "partial-timestamps";
    if (timestamped == 0) {
        clock_quality = "per-log-order-only";
    } else if (timestamped == events.size()) {
        clock_quality = "all-events-timestamped";
    }
    return QJsonObject{{"ok", true},
                       {"query", QUERY},
                       {"lanes", lanes},
                       {"events", events},
                       {"clockOrderedEvents", clock_ordered_events},
                       {"clockQuality", clock_quality},
                       {"crossLogCorrelations", cross_log_correlations},
                       {"truncated", TRUNCATED}};
}

auto DebugAnalysisService::explain_remote_exec_path(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    QString const PATH = args["path"].toString(session->dump->exe_path);
    QString hostname;
    QString local_path = PATH;
    if (PATH.startsWith("/wki/")) {
        QStringList parts = PATH.split('/', Qt::SkipEmptyParts);
        if (parts.size() >= 2) {
            hostname = parts[1];
            local_path = "/" + parts.mid(2).join('/');
        }
    }
    QJsonArray steps{
        QJsonObject{{"stage", "remote_compute"},
                    {"summary", "task submitted with VFS_REF path when executable lives under /wki/<peer>/..."}},
        QJsonObject{{"stage", "exec"}, {"summary", "process exec opens and reads executable through VFS using task cwd/root context"}},
        QJsonObject{{"stage", "remote_vfs_read"}, {"summary", "remote VFS path dispatches reads to WKI VFS operations on the owning peer"}},
        QJsonObject{
            {"stage", "wire"},
            {"summary", "expected operations include OP_VFS_READ, OP_VFS_READ_BULK, or OP_VFS_READ_RDMA with offset/length/cookie"}},
        QJsonObject{{"stage", "loader"}, {"summary", "ELF loader maps PT_LOAD pages from the received executable buffer into userspace"}}};
    return QJsonObject{
        {"ok", true},
        {"dumpId", session->id},
        {"exePath", PATH},
        {"isRemoteWkiPath", PATH.startsWith("/wki/")},
        {"peerHostname", hostname},
        {"peerLocalPath", local_path},
        {"likelyPath", steps},
        {"nextTools", QJsonArray{"wosdbg.analyze_elf_integrity", "wosdbg.reconstruct_wki_trace", "wosdbg.correlate_coredump_logs"}}};
}

auto DebugAnalysisService::diagnose_remote_exec_corruption(const QJsonObject& args) const -> QJsonObject {
    QJsonObject integrity = analyze_elf_integrity(args);
    QJsonObject path = explain_remote_exec_path(args);
    QJsonObject chunks = scan_chunk_corruption(QJsonObject{{"dumpId", args["dumpId"]}, {"maxHits", args["maxHits"].toInt(32)}});
    QJsonObject ptes = audit_executable_ptes(QJsonObject{{"dumpId", args["dumpId"]}, {"maxIssues", 64}});
    QStringList labels;
    if (path["isRemoteWkiPath"].toBool()) {
        labels << "remote WKI executable path";
    }
    if (!chunks["hits"].toArray().isEmpty()) {
        labels << "mapped pages differ from ELF with chunk/stale-page clues";
    }
    if (!integrity["labels"].toArray().isEmpty()) {
        for (const auto& value : integrity["labels"].toArray()) {
            labels << value.toString();
        }
    }
    QString diagnosis = labels.isEmpty() ? "no remote-exec corruption signature detected"
                                         : "remote executable corruption suspected; inspect WKI read/RDMA path and ELF loader buffer";
    return QJsonObject{{"ok", true},
                       {"dumpId", args["dumpId"].toString()},
                       {"diagnosis", diagnosis},
                       {"labels", QJsonArray::fromStringList(labels)},
                       {"remotePath", path},
                       {"integrity", integrity},
                       {"chunkScan", chunks},
                       {"pteAudit", ptes}};
}

auto DebugAnalysisService::backtrace_coredump(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    QString frame = args["frame"].toString("trap").toLower();
    if (frame != "trap" && frame != "saved") {
        frame = "trap";
    }
    int const MAX_FRAMES = bounded_int(args, "maxFrames", 24, 1, 128);
    return QJsonObject{{"ok", true},
                       {"dumpId", session->id},
                       {"frame", frame},
                       {"unwinder", "frame-pointer-with-stack-scan-fallback"},
                       {"frames", frame_pointer_backtrace(*session, frame, MAX_FRAMES)}};
}

auto DebugAnalysisService::inspect_page_table(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    auto address = resolve_address_argument(*session, args, "cr2");
    if (!address) {
        return tool_error("inspect_pte requires an address or register");
    }
    return QJsonObject{{"ok", true}, {"dumpId", session->id}, {"pteWalk", pte_info_for_address(*session, *address)}};
}

auto DebugAnalysisService::annotate_stack(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    QString frame = args["frame"].toString("trap").toLower();
    if (frame != "trap" && frame != "saved") {
        frame = "trap";
    }
    auto regs = register_map(*session->dump, frame);
    uint64_t const RSP = resolve_address_argument(*session, args, "rsp").value_or(regs.value("rsp"));
    uint64_t const RBP = regs.value("rbp");
    int const BEFORE = bounded_int(args, "beforeBytes", 128, 0, 4096);
    int const AFTER = bounded_int(args, "afterBytes", 384, 8, 8192);
    return QJsonObject{{"ok", true},
                       {"dumpId", session->id},
                       {"frame", frame},
                       {"rsp", format_hex(RSP)},
                       {"rbp", format_hex(RBP)},
                       {"redZone", red_zone_report(*session, RSP)},
                       {"stack", stack_window(*session, RSP, RBP, BEFORE, AFTER)}};
}

auto DebugAnalysisService::decode_fault_instruction(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    QJsonObject instruction = faulting_instruction_report(*session);
    return QJsonObject{
        {"ok", true}, {"dumpId", session->id}, {"instruction", instruction}, {"classification", classify_fault(*session, instruction)}};
}

auto DebugAnalysisService::compact_crash_summary(const DumpSession& session, const QJsonObject& instruction,
                                                 const QJsonObject& classification, int max_frames) const -> QJsonObject {
    const auto& dump = *session.dump;
    QJsonObject effective_address;
    for (const auto& value : instruction["memoryOperands"].toArray()) {
        QJsonObject operand = value.toObject();
        if (operand["matchesCr2"].toBool()) {
            effective_address = operand;
            break;
        }
    }
    if (effective_address.isEmpty() && !instruction["memoryOperands"].toArray().isEmpty()) {
        effective_address = instruction["memoryOperands"].toArray().first().toObject();
    }

    QJsonArray key_correlations;
    for (const auto& value : instruction["correlations"].toArray()) {
        QJsonObject corr = value.toObject();
        const QString KIND = corr["kind"].toString();
        if (KIND == "register_equals_cr2" || KIND == "stack_slot_equals_cr2" || KIND == "red_zone_frame_local_corruption") {
            key_correlations.append(corr);
        }
        if (key_correlations.size() >= 6) {
            break;
        }
    }
    QJsonArray backtrace = frame_pointer_backtrace(session, "trap", max_frames);
    QJsonArray red_zone_clues = red_zone_report(session, dump.trap_frame.rsp)["liveValues"].toArray();
    QString likely_diagnosis = classification["primary"].toString();
    for (const auto& corr_value : key_correlations) {
        QJsonObject corr = corr_value.toObject();
        if (corr["kind"].toString() == "red_zone_frame_local_corruption") {
            likely_diagnosis = "callee local stored in red zone appears corrupted, feeding a low unmapped write";
            break;
        }
    }

    return QJsonObject{
        {"faultType", classification["primary"].toString()},
        {"severity", classification["severity"].toString()},
        {"confidence", classification["confidence"].toString()},
        {"faultAddress", format_hex(dump.cr2)},
        {"decodedInstruction", QString("%1 %2").arg(instruction["mnemonic"].toString(), instruction["operands"].toString()).trimmed()},
        {"effectiveAddress", effective_address},
        {"keyRegisterCorrelation", key_correlations},
        {"topBacktrace", backtrace},
        {"suspiciousRedZoneClues", red_zone_clues},
        {"likelyDiagnosis", likely_diagnosis}};
}

static auto compact_human_report(const QJsonObject& compact) -> QString {
    QStringList lines;
    lines << QString("fault: %1 (%2, %3)")
                 .arg(compact["faultType"].toString(), compact["severity"].toString(), compact["confidence"].toString());
    lines << QString("address: %1").arg(compact["faultAddress"].toString());
    lines << QString("instruction: %1").arg(compact["decodedInstruction"].toString());
    QJsonObject ea = compact["effectiveAddress"].toObject();
    if (!ea.isEmpty()) {
        lines << QString("effective-address: %1  %2").arg(ea["effectiveAddress"].toString(), ea["calculation"].toString());
    }
    QJsonArray const CORRELATIONS = compact["keyRegisterCorrelation"].toArray();
    for (const auto& value : CORRELATIONS) {
        QJsonObject corr = value.toObject();
        lines << QString("correlation: %1 %2 %3")
                     .arg(corr["kind"].toString(), corr["register"].toString(corr["slot"].toString()), corr["value"].toString());
    }
    QJsonArray bt = compact["topBacktrace"].toArray();
    for (int i = 0; i < std::min<int>(3, static_cast<int>(bt.size())); ++i) {
        QJsonObject frame = bt[i].toObject();
        lines << QString("bt#%1: %2!%3 %4")
                     .arg(i)
                     .arg(frame["module"].toString(), frame["symbol"].toString(frame["rip"].toString()),
                          frame["sourceClickable"].toString());
    }
    lines << QString("diagnosis: %1").arg(compact["likelyDiagnosis"].toString());
    return lines.join('\n');
}

auto DebugAnalysisService::analyze_coredump(const QJsonObject& args) const -> QJsonObject {
    const auto* session = find_dump_session(args["dumpId"].toString());
    if (session == nullptr) {
        return tool_error("Unknown dumpId");
    }
    const Config& cfg = (config != nullptr) ? *config : ConfigService::instance().get_config();
    int const STACK_BEFORE = bounded_int(args, "stackBeforeBytes", 128, 0, cfg.get_mcp_settings().max_memory_bytes);
    int const STACK_AFTER = bounded_int(args, "stackAfterBytes", 384, 8, cfg.get_mcp_settings().max_memory_bytes);
    int const MAX_FRAMES = bounded_int(args, "maxFrames", 24, 1, 128);

    QJsonObject instruction = faulting_instruction_report(*session);
    QJsonObject classification = classify_fault(*session, instruction);
    QJsonObject compact = compact_crash_summary(*session, instruction, classification, MAX_FRAMES);
    const QString MODE = args["mode"].toString().toLower();
    const QString FORMAT = args["format"].toString().toLower();
    if (MODE == "text" || FORMAT == "text") {
        return QJsonObject{{"ok", true}, {"dumpId", session->id}, {"text", compact_human_report(compact)}};
    }
    if (MODE == "human" || FORMAT == "human") {
        return QJsonObject{{"ok", true}, {"dumpId", session->id}, {"compact", compact}, {"human", compact_human_report(compact)}};
    }
    if (args["compact"].toBool(false) || MODE == "compact" || FORMAT == "compact") {
        return QJsonObject{{"ok", true}, {"dumpId", session->id}, {"compact", compact}};
    }
    const auto& dump = *session->dump;
    return QJsonObject{{"ok", true},
                       {"dumpId", session->id},
                       {"compact", compact},
                       {"summary", coredump_summary_to_json(*session)},
                       {"fault", QJsonObject{{"rip", address_description_to_json(describe_address(*session, dump.trap_frame.rip, "rip"))},
                                             {"cr2", address_description_to_json(describe_address(*session, dump.cr2, "cr2"))},
                                             {"error", page_fault_error_to_json(dump.err_code)},
                                             {"classification", classification}}},
                       {"contexts", QJsonObject{{"trap", QJsonObject{{"label", "current fault trap frame"},
                                                                     {"registers", coredump_registers_to_json(*session, "trap")}}},
                                                {"saved", QJsonObject{{"label", "scheduler/syscall saved task frame"},
                                                                      {"registers", coredump_registers_to_json(*session, "saved")}}}}},
                       {"instruction", instruction},
                       {"backtrace", frame_pointer_backtrace(*session, "trap", MAX_FRAMES)},
                       {"redZone", red_zone_report(*session, dump.trap_frame.rsp)},
                       {"stack", stack_window(*session, dump.trap_frame.rsp, dump.trap_regs.rbp, STACK_BEFORE, STACK_AFTER)},
                       {"memoryMap", QJsonObject{{"pageCount", static_cast<int>(dump.segments.size())},
                                                 {"faultPage", pte_info_for_address(*session, dump.cr2)},
                                                 {"ripPage", pte_info_for_address(*session, dump.trap_frame.rip)},
                                                 {"rspPage", pte_info_for_address(*session, dump.trap_frame.rsp)}}},
                       {"disassembly", disassemble_at(*session, dump.trap_frame.rip, 32)}};
}

auto DebugAnalysisService::source_context_for_path(const QString& file_path, int line, int context_lines) const -> QJsonObject {
    QString resolved = resolve_path_for_read(file_path);
    if (!QFileInfo::exists(resolved)) {
        return tool_error(QString("Source file not found: %1").arg(file_path));
    }
    if (!is_path_allowed(resolved)) {
        return tool_error(QString("Source path is outside allowed roots: %1").arg(resolved));
    }
    QFile file(resolved);
    if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) {
        return tool_error(QString("Could not open source file: %1").arg(resolved));
    }
    int start = std::max(1, line - context_lines);
    int const END = line + context_lines;
    QJsonArray lines;
    QTextStream in(&file);
    int current = 1;
    while (!in.atEnd() && current <= END) {
        QString text = in.readLine();
        if (current >= start) {
            lines.append(QJsonObject{{"line", current}, {"text", text}, {"target", current == line}});
        }
        ++current;
    }
    return QJsonObject{{"ok", true}, {"path", resolved}, {"line", line}, {"start", start}, {"lines", lines}};
}

auto DebugAnalysisService::get_source_context(const QJsonObject& args) const -> QJsonObject {
    QString const PATH = args["path"].toString(args["sourceFile"].toString());
    int const LINE = args["line"].toInt(args["sourceLine"].toInt(1));
    if (PATH.isEmpty()) {
        return tool_error("get_source_context requires 'path'");
    }
    const Config& cfg = (config != nullptr) ? *config : ConfigService::instance().get_config();
    int const CONTEXT = bounded_int(args, "contextLines", cfg.get_mcp_settings().source_window_lines, 0, 100);
    return source_context_for_path(PATH, LINE, CONTEXT);
}

auto DebugAnalysisService::list_resources() const -> QJsonArray {
    QJsonArray resources;
    for (const auto& key : sorted_hash_keys(log_sessions)) {
        resources.append(QJsonObject{{"uri", QString("wosdbg://log/%1/summary").arg(key)}, {"name", QString("Log %1 summary").arg(key)}});
    }
    for (const auto& key : sorted_hash_keys(dump_sessions)) {
        resources.append(
            QJsonObject{{"uri", QString("wosdbg://coredump/%1/summary").arg(key)}, {"name", QString("Coredump %1 summary").arg(key)}});
        resources.append(
            QJsonObject{{"uri", QString("wosdbg://coredump/%1/registers").arg(key)}, {"name", QString("Coredump %1 registers").arg(key)}});
    }
    for (const auto& key : sorted_hash_keys(incident_sessions)) {
        resources.append(QJsonObject{{"uri", QString("wosdbg://incident/%1/summary").arg(key)},
                                     {"name", QString("Incident %1 deterministic summary").arg(key)}});
        resources.append(
            QJsonObject{{"uri", QString("wosdbg://incident/%1/inventory").arg(key)}, {"name", QString("Incident %1 inventory").arg(key)}});
    }
    return resources;
}

auto DebugAnalysisService::list_resource_templates() -> QJsonArray {
    return QJsonArray{
        QJsonObject{{"uriTemplate", "wosdbg://coredump/{dumpId}/summary"}, {"name", "Coredump summary"}},
        QJsonObject{{"uriTemplate", "wosdbg://coredump/{dumpId}/registers"}, {"name", "Coredump registers"}},
        QJsonObject{{"uriTemplate", "wosdbg://coredump/{dumpId}/register/{name}"}, {"name", "Coredump register follow-up"}},
        QJsonObject{{"uriTemplate", "wosdbg://coredump/{dumpId}/memory/{address}"}, {"name", "Coredump memory context"}},
        QJsonObject{{"uriTemplate", "wosdbg://coredump/{dumpId}/disasm/{address}"}, {"name", "Coredump disassembly"}},
        QJsonObject{{"uriTemplate", "wosdbg://coredump/{dumpId}/analysis"}, {"name", "Coredump one-shot analysis"}},
        QJsonObject{{"uriTemplate", "wosdbg://coredump/{dumpId}/backtrace"}, {"name", "Coredump backtrace"}},
        QJsonObject{{"uriTemplate", "wosdbg://coredump/{dumpId}/pte/{address}"}, {"name", "Coredump PTE/mapping info"}},
        QJsonObject{{"uriTemplate", "wosdbg://log/{logId}/summary"}, {"name", "Log summary"}},
        QJsonObject{{"uriTemplate", "wosdbg://log/{logId}/entry/{row}"}, {"name", "Log entry"}},
        QJsonObject{{"uriTemplate", "wosdbg://incident/{incidentId}/summary"}, {"name", "Incident deterministic summary"}},
        QJsonObject{{"uriTemplate", "wosdbg://incident/{incidentId}/inventory"}, {"name", "Incident inventory"}},
        QJsonObject{{"uriTemplate", "wosdbg://source/{encodedPath}:{line}"}, {"name", "Source context"}},
    };
}

auto DebugAnalysisService::read_resource(const QString& uri) const -> QJsonObject {
    QUrl const URL(uri);
    if (URL.scheme() != "wosdbg") {
        return tool_error("Unsupported resource URI scheme");
    }
    QStringList parts = URL.path().split('/', Qt::SkipEmptyParts);
    QString const HOST = URL.host();
    if (HOST == "log" && parts.size() >= 2) {
        QString log_id = parts[0];
        const QString& kind = parts[1];
        const auto* session = find_log_session(log_id);
        if (session == nullptr) {
            return tool_error("Unknown logId");
        }
        if (kind == "summary") {
            return QJsonObject{{"ok", true}, {"summary", log_summary_to_json(*session)}};
        }
        if (kind == "entry" && parts.size() >= 3) {
            return get_log_context(QJsonObject{{"logId", log_id}, {"row", parts[2].toInt()}, {"before", 0}, {"after", 0}});
        }
    }
    if (HOST == "coredump" && parts.size() >= 2) {
        QString dump_id = parts[0];
        QString const KIND = parts[1];
        if (KIND == "summary") {
            return get_crash_summary(QJsonObject{{"dumpId", dump_id}});
        }
        if (KIND == "registers") {
            return describe_registers(QJsonObject{{"dumpId", dump_id}, {"frame", "trap"}});
        }
        if (KIND == "register" && parts.size() >= 3) {
            return follow_register(QJsonObject{{"dumpId", dump_id}, {"register", parts[2]}});
        }
        if (KIND == "memory" && parts.size() >= 3) {
            return get_memory_context(QJsonObject{{"dumpId", dump_id}, {"address", parts[2]}});
        }
        if (KIND == "disasm" && parts.size() >= 3) {
            return disassemble_coredump(QJsonObject{{"dumpId", dump_id}, {"address", parts[2]}});
        }
        if (KIND == "analysis") {
            return analyze_coredump(QJsonObject{{"dumpId", dump_id}});
        }
        if (KIND == "backtrace") {
            return backtrace_coredump(QJsonObject{{"dumpId", dump_id}});
        }
        if (KIND == "pte" && parts.size() >= 3) {
            return inspect_page_table(QJsonObject{{"dumpId", dump_id}, {"address", parts[2]}});
        }
    }
    if (HOST == "incident" && parts.size() >= 2) {
        const QString incident_id = parts[0];
        const QString kind = parts[1];
        if (kind == "summary") {
            return summarize_incident(QJsonObject{{"incidentId", incident_id}});
        }
        if (kind == "inventory") {
            return get_incident_inventory(QJsonObject{{"incidentId", incident_id}});
        }
    }
    if (HOST == "source") {
        QString const PAYLOAD = QUrl::fromPercentEncoding(URL.path().mid(1).toUtf8());
        int const SPLIT = PAYLOAD.lastIndexOf(':');
        if (SPLIT > 0) {
            QString path = PAYLOAD.left(SPLIT);
            int line = PAYLOAD.mid(SPLIT + 1).toInt();
            return get_source_context(QJsonObject{{"path", path}, {"line", line}});
        }
    }
    return tool_error("Resource not found");
}
