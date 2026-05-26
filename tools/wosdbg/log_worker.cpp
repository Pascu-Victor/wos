// Log processing worker - processes chunks of log files in separate processes
// This avoids libbfd thread safety issues

// Work around BFD config.h requirement
#include <qlogging.h>
#include <qtypes.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#define PACKAGE "qemu_log_worker"
#define PACKAGE_VERSION "1.0"
extern "C" {
#include <bfd.h>
}
#include <llvm/Demangle/Demangle.h>

#include <QtCore/QCoreApplication>
#include <QtCore/QDebug>
#include <QtCore/QDir>
#include <QtCore/QFile>
#include <QtCore/QFileInfo>
#include <QtCore/QIODevice>
#include <QtCore/QJsonArray>
#include <QtCore/QJsonDocument>
#include <QtCore/QJsonObject>
#include <QtCore/QRegularExpression>
#include <QtCore/QTextStream>
#include <iostream>
#include <sstream>

#include "capstone_disasm.h"
#include "config.h"
#include "log_entry.h"

// Helper function to demangle C++ symbols using LLVM's demangler
// This handles modern C++20 features like concepts that cxxabi can't
static std::string demangle_symbol(const std::string& mangled) {
    if (mangled.length() > 2 && mangled.starts_with("_Z")) {
        std::string demangled = llvm::demangle(mangled);
        // llvm::demangle returns the original string if it can't demangle
        if (demangled != mangled) {
            return demangled;
        }
    }
    return mangled;
}

// CapstoneDisassembler: see capstone_disasm.h / capstone_disasm.cpp

// Structure to hold BFD binary info
struct BinaryInfo {
    bfd* abfd = nullptr;
    asymbol** symbols = nullptr;
    long sym_count = 0;
    uint64_t from_address = 0;
    uint64_t to_address = 0;
    uint64_t load_offset = 0;  // Runtime load offset to subtract from addresses
    QString path;

    ~BinaryInfo() {
        if (symbols) {
            free(symbols);
        }
        if (abfd) {
            bfd_close(abfd);
        }
    }

    [[nodiscard]] bool contains_address(uint64_t addr) const { return addr >= from_address && addr <= to_address; }

    // Convert runtime address to file-relative address for BFD lookups
    [[nodiscard]] uint64_t to_file_address(uint64_t runtime_address) const { return runtime_address - load_offset; }
};

class LogWorker {
   public:
    LogWorker();
    ~LogWorker();

    void load_config(const QString& config_path);
    void process_chunk(const QString& input_file, const QString& output_file);

   private:
    std::vector<std::unique_ptr<BinaryInfo>> binaries;

    BinaryInfo* find_binary_for_address(uint64_t address);

    LogEntry process_line(const QString& line, int line_number, CapstoneDisassembler& disassembler);

    void resolve_address_info(uint64_t address, std::string& function, std::string& source_file, int& source_line);

    QJsonObject log_entry_to_json(const LogEntry& entry);
};

LogWorker::LogWorker() { bfd_init(); }

LogWorker::~LogWorker() { binaries.clear(); }

void LogWorker::load_config(const QString& config_path) {
    Config config;

    // Get the directory containing the config file for relative path resolution
    QFileInfo config_file_info(config_path);
    QString config_dir = config_file_info.absolutePath();

    if (!config.load_from_file(config_path)) {
        qDebug() << "Failed to load config from" << config_path << ", using defaults";
    }

    const auto& lookups = config.get_address_lookups();
    qDebug() << "Loading" << lookups.size() << "address lookups from config";

    for (const auto& lookup : lookups) {
        auto info = std::make_unique<BinaryInfo>();
        info->from_address = lookup.from_address;
        info->to_address = lookup.to_address;
        info->load_offset = lookup.load_offset;

        // Resolve relative paths against the config file directory
        QString binary_path = lookup.symbol_file_path;
        if (!QFileInfo(binary_path).isAbsolute()) {
            binary_path = config_dir + "/" + binary_path;
        }
        info->path = binary_path;

        qDebug() << "Loading binary:" << binary_path << "for range" << QString("0x%1").arg(lookup.from_address, 0, 16) << "-"
                 << QString("0x%1").arg(lookup.to_address, 0, 16) << "offset" << QString("0x%1").arg(lookup.load_offset, 0, 16);

        info->abfd = bfd_openr(binary_path.toStdString().c_str(), nullptr);
        if (!info->abfd) {
            qDebug() << "Failed to open binary:" << binary_path << "-" << bfd_errmsg(bfd_get_error());
            continue;
        }

        if (!bfd_check_format(info->abfd, bfd_object)) {
            qDebug() << "Binary format check failed:" << binary_path << "-" << bfd_errmsg(bfd_get_error());
            bfd_close(info->abfd);
            info->abfd = nullptr;
            continue;
        }

        long storage = bfd_get_symtab_upper_bound(info->abfd);
        if (storage > 0) {
            info->symbols = static_cast<asymbol**>(malloc(storage));
            info->sym_count = bfd_canonicalize_symtab(info->abfd, info->symbols);
            qDebug() << "Loaded" << info->sym_count << "symbols from" << binary_path;
        } else {
            qDebug() << "Binary has no symbols:" << binary_path;
        }

        binaries.push_back(std::move(info));
    }
}

BinaryInfo* LogWorker::find_binary_for_address(uint64_t address) {
    for (auto& info : binaries) {
        if (info->contains_address(address)) {
            return info.get();
        }
    }
    return nullptr;
}

void LogWorker::process_chunk(const QString& input_file, const QString& output_file) {
    QFile file(input_file);
    if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) {
        qDebug() << "Cannot open input file:" << input_file;
        return;
    }

    QTextStream in(&file);
    std::vector<LogEntry> entries;
    entries.reserve(10000);  // Pre-allocate for typical log sizes

    CapstoneDisassembler disassembler;

    // Pre-compile regexes for CPU state detection (instead of compiling per-line)
    static const QRegularExpression CPU_STATE_REGEX(
        R"(RAX=|RBX=|RCX=|RDX=|RSI=|RDI=|RBP=|RSP=|R\d+=|RIP=|RFL=|[CEDFGS]S =|LDT=|TR =|[GI]DT=|CR[0234]=|DR[0-7]=|CC[CDs]=|CCO=|EFER=)");

    int line_number = 1;
    QString line;
    ptrdiff_t current_interrupt_group_index = -1;  // Use index instead of pointer

    while (!in.atEnd()) {
        line = in.readLine();

        LogEntry entry = process_line(line, line_number, disassembler);

        // Handle interrupt grouping and extract key information
        if (entry.type == EntryType::INTERRUPT) {
            entries.push_back(std::move(entry));
            current_interrupt_group_index = entries.size() - 1;  // Store index of the interrupt entry
        } else if (current_interrupt_group_index != -1 &&
                   (entry.type == EntryType::REGISTER ||
                    (entry.type == EntryType::OTHER && !entry.original_line.empty() && CPU_STATE_REGEX.match(line).hasMatch()))) {
            entry.is_child = true;
            entries[current_interrupt_group_index].child_entries.push_back(std::move(entry));

            // Extract key information for interrupt summary
            LogEntry& interrupt_entry = entries[current_interrupt_group_index];

            if (entries[current_interrupt_group_index].child_entries.back().type == EntryType::REGISTER) {
                const auto& child_entry = entries[current_interrupt_group_index].child_entries.back();
                if (interrupt_entry.cpu_state_info.empty()) {
                    interrupt_entry.cpu_state_info = child_entry.assembly;
                }

                // Extract RIP from the register dump line
                static const QRegularExpression RIP_REGEX(R"(pc=([0-9a-fA-F]+))");
                QString line_str = QString::fromStdString(child_entry.original_line);
                auto rip_match = RIP_REGEX.match(line_str);
                if (rip_match.hasMatch()) {
                    interrupt_entry.address = "0x" + rip_match.captured(1).toStdString();
                    bool ok;
                    interrupt_entry.address_value = rip_match.captured(1).toULongLong(&ok, 16);
                }
            } else if (line.contains("RIP=")) {
                // Extract RIP from CPU state lines
                static const QRegularExpression RIP_REGEX(R"(RIP=([0-9a-fA-F]+))");
                auto rip_match = RIP_REGEX.match(line);
                if (rip_match.hasMatch() && interrupt_entry.address.empty()) {
                    interrupt_entry.address = "0x" + rip_match.captured(1).toStdString();
                    bool ok;
                    interrupt_entry.address_value = rip_match.captured(1).toULongLong(&ok, 16);
                }
            }
        } else {
            // Finalize interrupt entry summary when ending the group
            if (current_interrupt_group_index != -1) {
                LogEntry& interrupt_entry = entries[current_interrupt_group_index];
                if (!interrupt_entry.child_entries.empty()) {
                    // Create a concise summary for the table
                    std::string summary = "Exception 0x" + interrupt_entry.interrupt_number;
                    if (!interrupt_entry.address.empty()) {
                        summary += " at " + interrupt_entry.address;
                    }
                    interrupt_entry.assembly = std::move(summary);
                }
            }

            current_interrupt_group_index = -1;
            if (entry.type != EntryType::OTHER || !entry.original_line.empty()) {
                entries.push_back(std::move(entry));
            }
        }

        line_number++;
    }

    // Cleanup BFD resources
    // Write results to JSON file
    QJsonArray json_array;
    for (const auto& entry : entries) {
        json_array.append(log_entry_to_json(entry));
    }

    QJsonDocument doc(json_array);

    QFile output_file_obj(output_file);
    if (output_file_obj.open(QIODevice::WriteOnly)) {
        output_file_obj.write(doc.toJson());
        output_file_obj.close();
    }
}

auto LogWorker::process_line(const QString& line, int line_number, CapstoneDisassembler& disassembler) -> LogEntry {
    LogEntry entry;
    entry.line_number = line_number;
    entry.original_line = line.toStdString();
    entry.type = EntryType::OTHER;

    QString trimmed_line = line.trimmed();

    // Pre-compile static regexes for better performance
    // Relaxed regex to handle variable spacing
    // Matches: 0x[hex]: [hex bytes] [assembly]
    // Example: 0x0000000000401000: 48 89 e5                 mov    rbp,rsp
    static const QRegularExpression INSTR_REGEX(R"(^0x([0-9a-fA-F]+):\s+((?:[0-9a-fA-F]{2}\s+)+)(.+)$)");
    static const QRegularExpression INT_REGEX(R"(^Servicing hardware INT=0x([0-9a-fA-F]+))");
    static const QRegularExpression EXC_REGEX(R"(^check_exception\s+old:\s*0x([0-9a-fA-F]+)\s+new\s+0x([0-9a-fA-F]+))");
    static const QRegularExpression REG_REGEX(R"(^\s*(\d+):\s+v=([0-9a-fA-F]+)\s+e=([0-9a-fA-F]+))");

    // Check if it's an instruction line: 0x[address]:  [hex bytes]  [assembly]
    auto instr_match = INSTR_REGEX.match(trimmed_line);
    if (instr_match.hasMatch()) {
        entry.type = EntryType::INSTRUCTION;
        entry.address = ("0x" + instr_match.captured(1)).toStdString();

        bool ok;
        entry.address_value = instr_match.captured(1).toULongLong(&ok, 16);
        if (ok) {
            resolve_address_info(entry.address_value, entry.function, entry.source_file, entry.source_line);

            QString hex_str = instr_match.captured(2).simplified();
            hex_str.remove(' ');
            entry.hex_bytes = hex_str.toStdString();

            QString asm_str = instr_match.captured(3).trimmed();
            entry.assembly = disassembler.convert_to_intel(asm_str.toStdString());

            // Debug logging for disassembly
            // qDebug() << "Parsed instruction:" << QString::fromStdString(entry.address) << QString::fromStdString(entry.assembly);
        }

        return entry;
    }  // Debug why regex failed for lines that look like instructions
    if (trimmed_line.startsWith("0x") && trimmed_line.contains(":")) {
        static int fail_count = 0;
        if (fail_count < 5) {
            qDebug() << "Regex failed for line:" << trimmed_line;
            fail_count++;
        }
    }

    // Check for hardware interrupt
    auto int_match = INT_REGEX.match(trimmed_line);
    if (int_match.hasMatch()) {
        entry.type = EntryType::INTERRUPT;
        entry.interrupt_number = int_match.captured(1).toStdString();
        entry.assembly = "Hardware Interrupt " + entry.interrupt_number;
        return entry;
    }

    // Check for exception
    auto exc_match = EXC_REGEX.match(trimmed_line);
    if (exc_match.hasMatch()) {
        entry.type = EntryType::INTERRUPT;
        entry.interrupt_number = exc_match.captured(2).toStdString();  // new vector
        entry.assembly = "Exception " + entry.interrupt_number;
        return entry;
    }

    // Check for register dump lines
    auto reg_match = REG_REGEX.match(trimmed_line);
    if (reg_match.hasMatch()) {
        entry.type = EntryType::REGISTER;
        entry.assembly = "CPU state dump (v=" + reg_match.captured(2).toStdString() + " e=" + reg_match.captured(3).toStdString() + ")";
        return entry;
    }

    // Check for IN: block markers
    if (trimmed_line.startsWith("IN:")) {
        entry.type = EntryType::BLOCK;
        entry.assembly = "Execution block";
        return entry;
    }

    // Check for separator lines
    if (trimmed_line.startsWith("----")) {
        entry.type = EntryType::SEPARATOR;
        entry.assembly = "Block separator";
        return entry;
    }

    return entry;
}

void LogWorker::resolve_address_info(uint64_t address, std::string& function, std::string& source_file, int& source_line) {
    function = "";
    source_file = "";
    source_line = 0;

    BinaryInfo* binary = find_binary_for_address(address);
    if (!binary || !binary->abfd || !binary->symbols || binary->sym_count <= 0) {
        return;
    }

    bfd* target_bfd = binary->abfd;
    asymbol** target_symbols = binary->symbols;
    long target_sym_count = binary->sym_count;

    // Convert runtime address to file-relative address
    uint64_t file_address = binary->to_file_address(address);

    // STEP 1: Find the best matching symbol first (this works reliably)
    asymbol* best_match = nullptr;
    bfd_vma best_distance = UINT64_MAX;

    for (long i = 0; i < target_sym_count; i++) {
        asymbol* sym = target_symbols[i];
        if (!sym || !sym->name) {
            continue;
        }

        if (!(sym->flags & (BSF_FUNCTION | BSF_GLOBAL | BSF_LOCAL))) {
            continue;
        }

        bfd_vma sym_addr = bfd_asymbol_value(sym);
        if (sym_addr <= file_address) {
            bfd_vma distance = file_address - sym_addr;
            if (distance < best_distance) {
                best_distance = distance;
                best_match = sym;
            }
        }
    }

    if (best_match && best_match->name) {
        std::string name = demangle_symbol(best_match->name);

        if (best_distance > 0) {
            std::ostringstream oss;
            oss << name << "+0x" << std::hex << best_distance;
            function = oss.str();
        } else {
            function = name;
        }

        // STEP 2: Try to get source file/line info for the exact address.
        // Use bfd_find_nearest_line first - it resolves to the actual source line
        // for the instruction address (via DWARF line tables).
        // Fall back to bfd_find_line which only gives the symbol's declaration line.
        const char* filename = nullptr;
        unsigned int line = 0;

        if (best_match->section) {
            const char* functionname = nullptr;
            bfd_vma section_vma = bfd_section_vma(best_match->section);

            if (bfd_find_nearest_line(target_bfd, best_match->section, target_symbols, file_address - section_vma, &filename, &functionname,
                                      &line)) {
                if (filename) {
                    source_file = filename;
                }
                source_line = line;
            }
        }

        // If bfd_find_nearest_line didn't work, fall back to bfd_find_line
        // (this only gives the function declaration line, but is better than nothing)
        if (source_file.empty()) {
            if (bfd_find_line(target_bfd, target_symbols, best_match, &filename, &line)) {
                if (filename) {
                    source_file = filename;
                }
                source_line = line;
            }
        }
    }
}

auto LogWorker::log_entry_to_json(const LogEntry& entry) -> QJsonObject {
    QJsonObject obj;
    obj["lineNumber"] = entry.line_number;
    obj["type"] = static_cast<int>(entry.type);
    obj["address"] = QString::fromStdString(entry.address);
    obj["function"] = QString::fromStdString(entry.function);
    obj["hexBytes"] = QString::fromStdString(entry.hex_bytes);
    obj["assembly"] = QString::fromStdString(entry.assembly);
    obj["originalLine"] = QString::fromStdString(entry.original_line);
    obj["addressValue"] = static_cast<qint64>(entry.address_value);
    obj["isExpanded"] = entry.is_expanded;
    obj["isChild"] = entry.is_child;
    obj["interruptNumber"] = QString::fromStdString(entry.interrupt_number);
    obj["cpuStateInfo"] = QString::fromStdString(entry.cpu_state_info);
    obj["sourceFile"] = QString::fromStdString(entry.source_file);
    obj["sourceLine"] = entry.source_line;

    // Handle child entries
    QJsonArray child_array;
    for (const auto& child : entry.child_entries) {
        child_array.append(log_entry_to_json(child));
    }
    obj["childEntries"] = child_array;

    return obj;
}

int main(int argc, char* argv[]) {
    QCoreApplication app(argc, argv);

    if (argc < 3) {
        qDebug() << "Usage: log_worker <input_file> <output_file> [config_file]";
        return 1;
    }

    QString input_file = argv[1];
    QString output_file = argv[2];
    QString config_file = (argc >= 4) ? argv[3] : QString();

    LogWorker worker;

    if (!config_file.isEmpty()) {
        worker.load_config(config_file);
    }

    worker.process_chunk(input_file, output_file);

    return 0;
}
