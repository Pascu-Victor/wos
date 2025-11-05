// Log processing worker - processes chunks of log files in separate processes
// This avoids libbfd thread safety issues

// Work around BFD config.h requirement
#include <qlogging.h>
#include <qtypes.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <vector>
#define PACKAGE "qemu_log_worker"
#define PACKAGE_VERSION "1.0"
extern "C" {
#include <bfd.h>
}
#include <capstone/capstone.h>
#include <cxxabi.h>

#include <QtCore/QCoreApplication>
#include <QtCore/QDebug>
#include <QtCore/QFile>
#include <QtCore/QIODevice>
#include <QtCore/QJsonArray>
#include <QtCore/QJsonDocument>
#include <QtCore/QJsonObject>
#include <QtCore/QRegularExpression>
#include <QtCore/QTextStream>
#include <iostream>
#include <sstream>

enum class EntryType { INSTRUCTION, INTERRUPT, REGISTER, BLOCK, SEPARATOR, OTHER };

struct LogEntry {
    int lineNumber{};
    EntryType type{};
    std::string address;
    std::string function;
    std::string hexBytes;
    std::string assembly;
    std::string originalLine;
    uint64_t addressValue{};

    // For grouped entries (like interrupts)
    bool isExpanded{};
    std::vector<LogEntry> childEntries;
    bool isChild{};

    // Interrupt-specific fields
    std::string interruptNumber;
    std::string cpuStateInfo;

    LogEntry() = default;
};

// CapstoneDisassembler implementation (same as main)
class CapstoneDisassembler {
   public:
    CapstoneDisassembler();
    ~CapstoneDisassembler();
    [[nodiscard]] auto convertToIntel(const std::string& atntAssembly) const -> std::string;

   private:
    csh handle;
    static auto extractHexBytes(const std::string& line) -> std::string;
    static auto hexStringToBytes(const std::string& hex) -> std::vector<uint8_t>;
};

CapstoneDisassembler::CapstoneDisassembler() : handle(0) {
    if (cs_open(CS_ARCH_X86, CS_MODE_64, &handle) != CS_ERR_OK) {
        handle = 0;
        return;
    }
    cs_option(handle, CS_OPT_SYNTAX, CS_OPT_SYNTAX_INTEL);
}

CapstoneDisassembler::~CapstoneDisassembler() {
    if (handle) {
        cs_close(&handle);
    }
}

auto CapstoneDisassembler::convertToIntel(const std::string& atntAssembly) const -> std::string {
    if (!handle) {
        return atntAssembly;
    }

    auto hexBytes = extractHexBytes(atntAssembly);
    if (hexBytes.empty()) {
        return atntAssembly;
    }

    std::vector<uint8_t> bytes = hexStringToBytes(hexBytes);
    if (bytes.empty()) {
        return atntAssembly;
    }

    cs_insn* insn;
    size_t count = cs_disasm(handle, bytes.data(), bytes.size(), 0x1000, 0, &insn);

    if (count > 0) {
        std::string result = std::string(insn[0].mnemonic) + " " + std::string(insn[0].op_str);
        cs_free(insn, count);
        return result;
    }

    return atntAssembly;
}

auto CapstoneDisassembler::extractHexBytes(const std::string& line) -> std::string {
    QRegularExpression hexRegex(R"(:\s*([0-9a-fA-F\s]{2,})\s+)");
    QString qline = QString::fromStdString(line);
    auto match = hexRegex.match(qline);

    if (match.hasMatch()) {
        QString hexStr = match.captured(1).simplified();
        hexStr.remove(' ');
        return hexStr.toStdString();
    }

    return "";
}

auto CapstoneDisassembler::hexStringToBytes(const std::string& hex) -> std::vector<uint8_t> {
    std::vector<uint8_t> bytes;

    for (size_t i = 0; i < hex.length(); i += 2) {
        if (i + 1 < hex.length()) {
            std::string byteStr = hex.substr(i, 2);
            try {
                auto byte = static_cast<uint8_t>(std::stoul(byteStr, nullptr, 16));
                bytes.push_back(byte);
            } catch (...) {
                break;
            }
        }
    }

    return bytes;
}

class LogWorker {
   public:
    LogWorker();
    void processChunk(const QString& inputFile, const QString& outputFile);

   private:
    static LogEntry processLine(const QString& line, int lineNumber, CapstoneDisassembler& disassembler, bfd* kernelBfd,
                                asymbol** kernelSymbols, long kernelSymCount, bfd* initBfd, asymbol** initSymbols, long initSymCount);

    static std::string getFunctionNameFromAddress(uint64_t address, bfd* kernelBfd, asymbol** kernelSymbols, long kernelSymCount,
                                                  bfd* initBfd, asymbol** initSymbols, long initSymCount);

    QJsonObject logEntryToJson(const LogEntry& entry);
};

LogWorker::LogWorker() {}

void LogWorker::processChunk(const QString& inputFile, const QString& outputFile) {
    QFile file(inputFile);
    if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) {
        qDebug() << "Cannot open input file:" << inputFile;
        return;
    }

    QTextStream in(&file);
    std::vector<LogEntry> entries;
    entries.reserve(10000);  // Pre-allocate for typical log sizes

    CapstoneDisassembler disassembler;

    // Initialize BFD
    bfd_init();

    // Open executables for symbol resolution
    bfd* kernelBfd = bfd_openr("./build/modules/kern/wos", nullptr);
    bfd* initBfd = bfd_openr("./build/modules/init/init", nullptr);

    asymbol** kernelSymbols = nullptr;
    asymbol** initSymbols = nullptr;
    long kernelSymCount = 0;
    long initSymCount = 0;

    if (kernelBfd && bfd_check_format(kernelBfd, bfd_object)) {
        long storage = bfd_get_symtab_upper_bound(kernelBfd);
        if (storage > 0) {
            kernelSymbols = (asymbol**)malloc(storage);
            kernelSymCount = bfd_canonicalize_symtab(kernelBfd, kernelSymbols);
        }
    }

    if (initBfd && bfd_check_format(initBfd, bfd_object)) {
        long storage = bfd_get_symtab_upper_bound(initBfd);
        if (storage > 0) {
            initSymbols = (asymbol**)malloc(storage);
            initSymCount = bfd_canonicalize_symtab(initBfd, initSymbols);
        }
    }

    // Pre-compile regexes for CPU state detection (instead of compiling per-line)
    static const QRegularExpression cpuStateRegex(
        R"(RAX=|RBX=|RCX=|RDX=|RSI=|RDI=|RBP=|RSP=|R\d+=|RIP=|RFL=|[CEDFGS]S =|LDT=|TR =|[GI]DT=|CR[0234]=|DR[0-7]=|CC[CDs]=|CCO=|EFER=)");

    int lineNumber = 1;
    QString line;
    ptrdiff_t currentInterruptGroupIndex = -1;  // Use index instead of pointer

    while (!in.atEnd()) {
        line = in.readLine();

        LogEntry entry =
            processLine(line, lineNumber, disassembler, kernelBfd, kernelSymbols, kernelSymCount, initBfd, initSymbols, initSymCount);

        // Handle interrupt grouping and extract key information
        if (entry.type == EntryType::INTERRUPT) {
            entries.push_back(std::move(entry));
            currentInterruptGroupIndex = entries.size() - 1;  // Store index of the interrupt entry
        } else if (currentInterruptGroupIndex != -1 &&
                   (entry.type == EntryType::REGISTER ||
                    (entry.type == EntryType::OTHER && !entry.originalLine.empty() && cpuStateRegex.match(line).hasMatch()))) {
            entry.isChild = true;
            entries[currentInterruptGroupIndex].childEntries.push_back(std::move(entry));

            // Extract key information for interrupt summary
            LogEntry& interruptEntry = entries[currentInterruptGroupIndex];

            if (entries[currentInterruptGroupIndex].childEntries.back().type == EntryType::REGISTER) {
                const auto& childEntry = entries[currentInterruptGroupIndex].childEntries.back();
                if (interruptEntry.cpuStateInfo.empty()) {
                    interruptEntry.cpuStateInfo = childEntry.assembly;
                }

                // Extract RIP from the register dump line
                static const QRegularExpression ripRegex(R"(pc=([0-9a-fA-F]+))");
                QString lineStr = QString::fromStdString(childEntry.originalLine);
                auto ripMatch = ripRegex.match(lineStr);
                if (ripMatch.hasMatch()) {
                    interruptEntry.address = "0x" + ripMatch.captured(1).toStdString();
                    bool ok;
                    interruptEntry.addressValue = ripMatch.captured(1).toULongLong(&ok, 16);
                }
            } else if (line.contains("RIP=")) {
                // Extract RIP from CPU state lines
                static const QRegularExpression ripRegex(R"(RIP=([0-9a-fA-F]+))");
                auto ripMatch = ripRegex.match(line);
                if (ripMatch.hasMatch() && interruptEntry.address.empty()) {
                    interruptEntry.address = "0x" + ripMatch.captured(1).toStdString();
                    bool ok;
                    interruptEntry.addressValue = ripMatch.captured(1).toULongLong(&ok, 16);
                }
            }
        } else {
            // Finalize interrupt entry summary when ending the group
            if (currentInterruptGroupIndex != -1) {
                LogEntry& interruptEntry = entries[currentInterruptGroupIndex];
                if (!interruptEntry.childEntries.empty()) {
                    // Create a concise summary for the table
                    std::string summary = "Exception 0x" + interruptEntry.interruptNumber;
                    if (!interruptEntry.address.empty()) {
                        summary += " at " + interruptEntry.address;
                    }
                    interruptEntry.assembly = std::move(summary);
                }
            }

            currentInterruptGroupIndex = -1;
            if (entry.type != EntryType::OTHER || !entry.originalLine.empty()) {
                entries.push_back(std::move(entry));
            }
        }

        lineNumber++;
    }

    // Cleanup BFD resources
    if (kernelSymbols) {
        free(kernelSymbols);
    }
    if (initSymbols) {
        free(initSymbols);
    }
    if (kernelBfd) {
        bfd_close(kernelBfd);
    }
    if (initBfd) {
        bfd_close(initBfd);
    }

    // Write results to JSON file
    QJsonArray jsonArray;
    for (const auto& entry : entries) {
        jsonArray.append(logEntryToJson(entry));
    }

    QJsonDocument doc(jsonArray);

    QFile outputFileObj(outputFile);
    if (outputFileObj.open(QIODevice::WriteOnly)) {
        outputFileObj.write(doc.toJson());
        outputFileObj.close();
    }
}

auto LogWorker::processLine(const QString& line, int lineNumber, CapstoneDisassembler& disassembler, bfd* kernelBfd,
                            asymbol** kernelSymbols, long kernelSymCount, bfd* initBfd, asymbol** initSymbols, long initSymCount)
    -> LogEntry {
    LogEntry entry;
    entry.lineNumber = lineNumber;
    entry.originalLine = line.toStdString();
    entry.type = EntryType::OTHER;

    QString trimmedLine = line.trimmed();

    // Pre-compile static regexes for better performance
    static const QRegularExpression instrRegex(R"(^0x([0-9a-fA-F]+):\s+([0-9a-fA-F]{2}(?:\s[0-9a-fA-F]{2})*)\s{2,}(.+)$)");
    static const QRegularExpression intRegex(R"(^Servicing hardware INT=0x([0-9a-fA-F]+))");
    static const QRegularExpression excRegex(R"(^check_exception\s+old:\s*0x([0-9a-fA-F]+)\s+new\s+0x([0-9a-fA-F]+))");
    static const QRegularExpression regRegex(R"(^\s*(\d+):\s+v=([0-9a-fA-F]+)\s+e=([0-9a-fA-F]+))");

    // Check if it's an instruction line: 0x[address]:  [hex bytes]  [assembly]
    auto instrMatch = instrRegex.match(trimmedLine);
    if (instrMatch.hasMatch()) {
        entry.type = EntryType::INSTRUCTION;
        entry.address = ("0x" + instrMatch.captured(1)).toStdString();

        bool ok;
        entry.addressValue = instrMatch.captured(1).toULongLong(&ok, 16);
        if (ok) {
            entry.function = getFunctionNameFromAddress(entry.addressValue, kernelBfd, kernelSymbols, kernelSymCount, initBfd, initSymbols,
                                                        initSymCount);

            QString hexStr = instrMatch.captured(2).simplified();
            hexStr.remove(' ');
            entry.hexBytes = hexStr.toStdString();

            QString asmStr = instrMatch.captured(3).trimmed();
            entry.assembly = disassembler.convertToIntel(asmStr.toStdString());
        }

        return entry;
    }

    // Check for hardware interrupt
    auto intMatch = intRegex.match(trimmedLine);
    if (intMatch.hasMatch()) {
        entry.type = EntryType::INTERRUPT;
        entry.interruptNumber = intMatch.captured(1).toStdString();
        entry.assembly = "Hardware INT=0x" + entry.interruptNumber + " (click to expand)";
        return entry;
    }

    // Check for exception entries: "check_exception old: 0xffffffff new 0xe"
    auto excMatch = excRegex.match(trimmedLine);
    if (excMatch.hasMatch()) {
        entry.type = EntryType::INTERRUPT;
        entry.interruptNumber = excMatch.captured(2).toStdString();
        entry.assembly = "Exception 0x" + entry.interruptNumber + " (old: 0x" + excMatch.captured(1).toStdString() + ") (click to expand)";
        return entry;
    }

    // Check for register dump lines
    auto regMatch = regRegex.match(trimmedLine);
    if (regMatch.hasMatch()) {
        entry.type = EntryType::REGISTER;
        entry.assembly = "CPU state dump (v=" + regMatch.captured(2).toStdString() + " e=" + regMatch.captured(3).toStdString() + ")";
        return entry;
    }

    // Check for IN: block markers
    if (trimmedLine.startsWith("IN:")) {
        entry.type = EntryType::BLOCK;
        entry.assembly = "Execution block";
        return entry;
    }

    // Check for separator lines
    if (trimmedLine.startsWith("----")) {
        entry.type = EntryType::SEPARATOR;
        entry.assembly = "Block separator";
        return entry;
    }

    return entry;
}

auto LogWorker::getFunctionNameFromAddress(uint64_t address, bfd* kernelBfd, asymbol** kernelSymbols, long kernelSymCount, bfd* initBfd,
                                           asymbol** initSymbols, long initSymCount) -> std::string {
    // Same implementation as in main processor
    bfd* targetBfd = nullptr;
    asymbol** targetSymbols = nullptr;
    long targetSymCount = 0;

    if (address < 0x100000) {
        targetBfd = initBfd;
        targetSymbols = initSymbols;
        targetSymCount = initSymCount;
    } else {
        targetBfd = kernelBfd;
        targetSymbols = kernelSymbols;
        targetSymCount = kernelSymCount;
    }

    if (!targetBfd || !targetSymbols || targetSymCount <= 0) {
        if (targetBfd == kernelBfd) {
            targetBfd = initBfd;
            targetSymbols = initSymbols;
            targetSymCount = initSymCount;
        } else {
            targetBfd = kernelBfd;
            targetSymbols = kernelSymbols;
            targetSymCount = kernelSymCount;
        }

        if (!targetBfd || !targetSymbols || targetSymCount <= 0) {
            return "";
        }
    }

    asymbol* bestMatch = nullptr;
    bfd_vma bestDistance = UINT64_MAX;

    for (long i = 0; i < targetSymCount; i++) {
        asymbol* sym = targetSymbols[i];
        if (!sym || !sym->name) {
            continue;
        }

        if (!(sym->flags & (BSF_FUNCTION | BSF_GLOBAL | BSF_LOCAL))) {
            continue;
        }

        bfd_vma symAddr = bfd_asymbol_value(sym);
        if (symAddr <= address) {
            bfd_vma distance = address - symAddr;
            if (distance < bestDistance) {
                bestDistance = distance;
                bestMatch = sym;
            }
        }
    }

    if (bestMatch && bestMatch->name) {
        std::string name = bestMatch->name;

        if (name.length() > 2 && name.substr(0, 2) == "_Z") {
            int status = 0;
            char* demangled = abi::__cxa_demangle(name.c_str(), nullptr, nullptr, &status);
            if (status == 0 && demangled) {
                name = demangled;
                free(demangled);
            }
        }

        if (bestDistance > 0) {
            std::ostringstream oss;
            oss << name << "+0x" << std::hex << bestDistance;
            return oss.str();
        }

        return name;
    }

    return "";
}

auto LogWorker::logEntryToJson(const LogEntry& entry) -> QJsonObject {
    QJsonObject obj;
    obj["lineNumber"] = entry.lineNumber;
    obj["type"] = static_cast<int>(entry.type);
    obj["address"] = QString::fromStdString(entry.address);
    obj["function"] = QString::fromStdString(entry.function);
    obj["hexBytes"] = QString::fromStdString(entry.hexBytes);
    obj["assembly"] = QString::fromStdString(entry.assembly);
    obj["originalLine"] = QString::fromStdString(entry.originalLine);
    obj["addressValue"] = static_cast<qint64>(entry.addressValue);
    obj["isExpanded"] = entry.isExpanded;
    obj["isChild"] = entry.isChild;
    obj["interruptNumber"] = QString::fromStdString(entry.interruptNumber);
    obj["cpuStateInfo"] = QString::fromStdString(entry.cpuStateInfo);

    // Handle child entries
    QJsonArray childArray;
    for (const auto& child : entry.childEntries) {
        childArray.append(logEntryToJson(child));
    }
    obj["childEntries"] = childArray;

    return obj;
}

int main(int argc, char* argv[]) {
    QCoreApplication app(argc, argv);

    if (argc != 3) {
        qDebug() << "Usage: log_worker <input_file> <output_file>";
        return 1;
    }

    QString inputFile = argv[1];
    QString outputFile = argv[2];

    LogWorker worker;
    worker.processChunk(inputFile, outputFile);

    return 0;
}
