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
#include <vector>
#define PACKAGE "qemu_log_worker"
#define PACKAGE_VERSION "1.0"
extern "C" {
#include <bfd.h>
}
#include <capstone/capstone.h>
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
static std::string demangleSymbol(const std::string& mangled) {
    if (mangled.length() > 2 && mangled.substr(0, 2) == "_Z") {
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
    long symCount = 0;
    uint64_t fromAddress = 0;
    uint64_t toAddress = 0;
    uint64_t loadOffset = 0;  // Runtime load offset to subtract from addresses
    QString path;

    ~BinaryInfo() {
        if (symbols) {
            free(symbols);
        }
        if (abfd) {
            bfd_close(abfd);
        }
    }

    bool containsAddress(uint64_t addr) const { return addr >= fromAddress && addr <= toAddress; }

    // Convert runtime address to file-relative address for BFD lookups
    uint64_t toFileAddress(uint64_t runtimeAddress) const { return runtimeAddress - loadOffset; }
};

class LogWorker {
   public:
    LogWorker();
    ~LogWorker();

    void loadConfig(const QString& configPath);
    void processChunk(const QString& inputFile, const QString& outputFile);

   private:
    std::vector<std::unique_ptr<BinaryInfo>> binaries;

    BinaryInfo* findBinaryForAddress(uint64_t address);

    LogEntry processLine(const QString& line, int lineNumber, CapstoneDisassembler& disassembler);

    void resolveAddressInfo(uint64_t address, std::string& function, std::string& sourceFile, int& sourceLine);

    QJsonObject logEntryToJson(const LogEntry& entry);
};

LogWorker::LogWorker() { bfd_init(); }

LogWorker::~LogWorker() { binaries.clear(); }

void LogWorker::loadConfig(const QString& configPath) {
    Config config;

    // Get the directory containing the config file for relative path resolution
    QFileInfo configFileInfo(configPath);
    QString configDir = configFileInfo.absolutePath();

    if (!config.loadFromFile(configPath)) {
        qDebug() << "Failed to load config from" << configPath << ", using defaults";
    }

    const auto& lookups = config.getAddressLookups();
    qDebug() << "Loading" << lookups.size() << "address lookups from config";

    for (const auto& lookup : lookups) {
        auto info = std::make_unique<BinaryInfo>();
        info->fromAddress = lookup.fromAddress;
        info->toAddress = lookup.toAddress;
        info->loadOffset = lookup.loadOffset;

        // Resolve relative paths against the config file directory
        QString binaryPath = lookup.symbolFilePath;
        if (!QFileInfo(binaryPath).isAbsolute()) {
            binaryPath = configDir + "/" + binaryPath;
        }
        info->path = binaryPath;

        qDebug() << "Loading binary:" << binaryPath << "for range" << QString("0x%1").arg(lookup.fromAddress, 0, 16) << "-"
                 << QString("0x%1").arg(lookup.toAddress, 0, 16) << "offset" << QString("0x%1").arg(lookup.loadOffset, 0, 16);

        info->abfd = bfd_openr(binaryPath.toStdString().c_str(), nullptr);
        if (!info->abfd) {
            qDebug() << "Failed to open binary:" << binaryPath << "-" << bfd_errmsg(bfd_get_error());
            continue;
        }

        if (!bfd_check_format(info->abfd, bfd_object)) {
            qDebug() << "Binary format check failed:" << binaryPath << "-" << bfd_errmsg(bfd_get_error());
            bfd_close(info->abfd);
            info->abfd = nullptr;
            continue;
        }

        long storage = bfd_get_symtab_upper_bound(info->abfd);
        if (storage > 0) {
            info->symbols = (asymbol**)malloc(storage);
            info->symCount = bfd_canonicalize_symtab(info->abfd, info->symbols);
            qDebug() << "Loaded" << info->symCount << "symbols from" << binaryPath;
        } else {
            qDebug() << "Binary has no symbols:" << binaryPath;
        }

        binaries.push_back(std::move(info));
    }
}

BinaryInfo* LogWorker::findBinaryForAddress(uint64_t address) {
    for (auto& info : binaries) {
        if (info->containsAddress(address)) {
            return info.get();
        }
    }
    return nullptr;
}

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

    // Pre-compile regexes for CPU state detection (instead of compiling per-line)
    static const QRegularExpression cpuStateRegex(
        R"(RAX=|RBX=|RCX=|RDX=|RSI=|RDI=|RBP=|RSP=|R\d+=|RIP=|RFL=|[CEDFGS]S =|LDT=|TR =|[GI]DT=|CR[0234]=|DR[0-7]=|CC[CDs]=|CCO=|EFER=)");

    int lineNumber = 1;
    QString line;
    ptrdiff_t currentInterruptGroupIndex = -1;  // Use index instead of pointer

    while (!in.atEnd()) {
        line = in.readLine();

        LogEntry entry = processLine(line, lineNumber, disassembler);

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

auto LogWorker::processLine(const QString& line, int lineNumber, CapstoneDisassembler& disassembler) -> LogEntry {
    LogEntry entry;
    entry.lineNumber = lineNumber;
    entry.originalLine = line.toStdString();
    entry.type = EntryType::OTHER;

    QString trimmedLine = line.trimmed();

    // Pre-compile static regexes for better performance
    // Relaxed regex to handle variable spacing
    // Matches: 0x[hex]: [hex bytes] [assembly]
    // Example: 0x0000000000401000: 48 89 e5                 mov    rbp,rsp
    static const QRegularExpression instrRegex(R"(^0x([0-9a-fA-F]+):\s+((?:[0-9a-fA-F]{2}\s+)+)(.+)$)");
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
            resolveAddressInfo(entry.addressValue, entry.function, entry.sourceFile, entry.sourceLine);

            QString hexStr = instrMatch.captured(2).simplified();
            hexStr.remove(' ');
            entry.hexBytes = hexStr.toStdString();

            QString asmStr = instrMatch.captured(3).trimmed();
            entry.assembly = disassembler.convertToIntel(asmStr.toStdString());

            // Debug logging for disassembly
            // qDebug() << "Parsed instruction:" << QString::fromStdString(entry.address) << QString::fromStdString(entry.assembly);
        }

        return entry;
    } else {
        // Debug why regex failed for lines that look like instructions
        if (trimmedLine.startsWith("0x") && trimmedLine.contains(":")) {
            static int failCount = 0;
            if (failCount < 5) {
                qDebug() << "Regex failed for line:" << trimmedLine;
                failCount++;
            }
        }
    }

    // Check for hardware interrupt
    auto intMatch = intRegex.match(trimmedLine);
    if (intMatch.hasMatch()) {
        entry.type = EntryType::INTERRUPT;
        entry.interruptNumber = intMatch.captured(1).toStdString();
        entry.assembly = "Hardware Interrupt " + entry.interruptNumber;
        return entry;
    }

    // Check for exception
    auto excMatch = excRegex.match(trimmedLine);
    if (excMatch.hasMatch()) {
        entry.type = EntryType::INTERRUPT;
        entry.interruptNumber = excMatch.captured(2).toStdString();  // new vector
        entry.assembly = "Exception " + entry.interruptNumber;
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

void LogWorker::resolveAddressInfo(uint64_t address, std::string& function, std::string& sourceFile, int& sourceLine) {
    function = "";
    sourceFile = "";
    sourceLine = 0;

    BinaryInfo* binary = findBinaryForAddress(address);
    if (!binary || !binary->abfd || !binary->symbols || binary->symCount <= 0) {
        return;
    }

    bfd* targetBfd = binary->abfd;
    asymbol** targetSymbols = binary->symbols;
    long targetSymCount = binary->symCount;

    // Convert runtime address to file-relative address
    uint64_t fileAddress = binary->toFileAddress(address);

    // STEP 1: Find the best matching symbol first (this works reliably)
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
        if (symAddr <= fileAddress) {
            bfd_vma distance = fileAddress - symAddr;
            if (distance < bestDistance) {
                bestDistance = distance;
                bestMatch = sym;
            }
        }
    }

    if (bestMatch && bestMatch->name) {
        std::string name = demangleSymbol(bestMatch->name);

        if (bestDistance > 0) {
            std::ostringstream oss;
            oss << name << "+0x" << std::hex << bestDistance;
            function = oss.str();
        } else {
            function = name;
        }

        // STEP 2: Try to get source file/line info for the exact address.
        // Use bfd_find_nearest_line first — it resolves to the actual source line
        // for the instruction address (via DWARF line tables).
        // Fall back to bfd_find_line which only gives the symbol's declaration line.
        const char* filename = nullptr;
        unsigned int line = 0;

        if (bestMatch->section) {
            const char* functionname = nullptr;
            bfd_vma sectionVma = bfd_section_vma(bestMatch->section);

            if (bfd_find_nearest_line(targetBfd, bestMatch->section, targetSymbols, fileAddress - sectionVma, &filename, &functionname,
                                      &line)) {
                if (filename) sourceFile = filename;
                sourceLine = line;
            }
        }

        // If bfd_find_nearest_line didn't work, fall back to bfd_find_line
        // (this only gives the function declaration line, but is better than nothing)
        if (sourceFile.empty()) {
            if (bfd_find_line(targetBfd, targetSymbols, bestMatch, &filename, &line)) {
                if (filename) sourceFile = filename;
                sourceLine = line;
            }
        }
    }
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
    obj["sourceFile"] = QString::fromStdString(entry.sourceFile);
    obj["sourceLine"] = entry.sourceLine;

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

    if (argc < 3) {
        qDebug() << "Usage: log_worker <input_file> <output_file> [config_file]";
        return 1;
    }

    QString inputFile = argv[1];
    QString outputFile = argv[2];
    QString configFile = (argc >= 4) ? argv[3] : QString();

    LogWorker worker;

    if (!configFile.isEmpty()) {
        worker.loadConfig(configFile);
    }

    worker.processChunk(inputFile, outputFile);

    return 0;
}
