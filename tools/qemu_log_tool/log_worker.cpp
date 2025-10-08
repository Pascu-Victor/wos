// Log processing worker - processes chunks of log files in separate processes
// This avoids libbfd thread safety issues

// Work around BFD config.h requirement
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
#include <fstream>
#include <iostream>
#include <sstream>

enum class EntryType { INSTRUCTION, INTERRUPT, REGISTER, BLOCK, SEPARATOR, OTHER };

struct LogEntry {
    int lineNumber;
    EntryType type;
    std::string address;
    std::string function;
    std::string hexBytes;
    std::string assembly;
    std::string originalLine;
    uint64_t addressValue;
    
    // For grouped entries (like interrupts)
    bool isExpanded;
    std::vector<LogEntry> childEntries;
    bool isChild;
    
    // Interrupt-specific fields
    std::string interruptNumber;
    std::string cpuStateInfo;
    
    LogEntry() : lineNumber(0), type(EntryType::OTHER), addressValue(0), 
                 isExpanded(false), isChild(false) {}
};

// CapstoneDisassembler implementation (same as main)
class CapstoneDisassembler {
   public:
    CapstoneDisassembler();
    ~CapstoneDisassembler();
    std::string convertToIntel(const std::string& atntAssembly);

   private:
    csh handle;
    std::string extractHexBytes(const std::string& line);
    std::vector<uint8_t> hexStringToBytes(const std::string& hex);
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

std::string CapstoneDisassembler::convertToIntel(const std::string& atntAssembly) {
    if (!handle) return atntAssembly;

    auto hexBytes = extractHexBytes(atntAssembly);
    if (hexBytes.empty()) return atntAssembly;

    std::vector<uint8_t> bytes = hexStringToBytes(hexBytes);
    if (bytes.empty()) return atntAssembly;

    cs_insn* insn;
    size_t count = cs_disasm(handle, bytes.data(), bytes.size(), 0x1000, 0, &insn);

    if (count > 0) {
        std::string result = std::string(insn[0].mnemonic) + " " + std::string(insn[0].op_str);
        cs_free(insn, count);
        return result;
    }

    return atntAssembly;
}

std::string CapstoneDisassembler::extractHexBytes(const std::string& line) {
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

std::vector<uint8_t> CapstoneDisassembler::hexStringToBytes(const std::string& hex) {
    std::vector<uint8_t> bytes;

    for (size_t i = 0; i < hex.length(); i += 2) {
        if (i + 1 < hex.length()) {
            std::string byteStr = hex.substr(i, 2);
            try {
                uint8_t byte = static_cast<uint8_t>(std::stoul(byteStr, nullptr, 16));
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
    LogEntry processLine(const QString& line, int lineNumber, CapstoneDisassembler& disassembler, 
                        bfd* kernelBfd, asymbol** kernelSymbols, long kernelSymCount, 
                        bfd* initBfd, asymbol** initSymbols, long initSymCount);
    
    std::string getFunctionNameFromAddress(uint64_t address, bfd* kernelBfd, asymbol** kernelSymbols, long kernelSymCount,
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

    int lineNumber = 1;
    QString line;
    int currentInterruptGroupIndex = -1;  // Use index instead of pointer

    while (!in.atEnd()) {
        line = in.readLine();

        LogEntry entry = processLine(line, lineNumber, disassembler, kernelBfd, kernelSymbols, kernelSymCount, 
                                   initBfd, initSymbols, initSymCount);

        // Handle interrupt grouping and extract key information
        if (entry.type == EntryType::INTERRUPT) {
            entries.push_back(entry);
            currentInterruptGroupIndex = entries.size() - 1;  // Store index of the interrupt entry
        } else if (currentInterruptGroupIndex != -1 && 
                  (entry.type == EntryType::REGISTER || 
                   (entry.type == EntryType::OTHER && !entry.originalLine.empty() && 
                    (line.contains("RAX=") || line.contains("RBX=") || line.contains("RCX=") || line.contains("RDX=") ||
                     line.contains("RSI=") || line.contains("RDI=") || line.contains("RBP=") || line.contains("RSP=") ||
                     line.contains("R8 =") || line.contains("R9 =") || line.contains("R10=") || line.contains("R11=") ||
                     line.contains("R12=") || line.contains("R13=") || line.contains("R14=") || line.contains("R15=") ||
                     line.contains("RIP=") || line.contains("RFL=") || 
                     line.contains("ES =") || line.contains("CS =") || line.contains("SS =") || 
                     line.contains("DS =") || line.contains("FS =") || line.contains("GS =") ||
                     line.contains("LDT=") || line.contains("TR =") || line.contains("GDT=") || line.contains("IDT=") ||
                     line.contains("CR0=") || line.contains("CR2=") || line.contains("CR3=") || line.contains("CR4=") ||
                     line.contains("DR0=") || line.contains("DR1=") || line.contains("DR2=") || line.contains("DR3=") ||
                     line.contains("DR6=") || line.contains("DR7=") || 
                     line.contains("CCS=") || line.contains("CCD=") || line.contains("CCO=") || 
                     line.contains("EFER="))))) {
            entry.isChild = true;
            entries[currentInterruptGroupIndex].childEntries.push_back(entry);
            
            // Extract key information for interrupt summary
            LogEntry& interruptEntry = entries[currentInterruptGroupIndex];
            
            if (entry.type == EntryType::REGISTER) {
                if (interruptEntry.cpuStateInfo.empty()) {
                    interruptEntry.cpuStateInfo = entry.assembly;
                }
                
                // Extract RIP from the register dump line
                QString lineStr = QString::fromStdString(entry.originalLine);
                QRegularExpression ripRegex(R"(pc=([0-9a-fA-F]+))");
                auto ripMatch = ripRegex.match(lineStr);
                if (ripMatch.hasMatch()) {
                    interruptEntry.address = "0x" + ripMatch.captured(1).toStdString();
                    bool ok;
                    interruptEntry.addressValue = ripMatch.captured(1).toULongLong(&ok, 16);
                }
            } else if (line.contains("RIP=")) {
                // Extract RIP from CPU state lines
                QRegularExpression ripRegex(R"(RIP=([0-9a-fA-F]+))");
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
                    interruptEntry.assembly = summary;
                }
            }
            
            currentInterruptGroupIndex = -1;
            if (entry.type != EntryType::OTHER || !entry.originalLine.empty()) {
                entries.push_back(entry);
            }
        }

        lineNumber++;
    }

    // Cleanup BFD resources
    if (kernelSymbols) free(kernelSymbols);
    if (initSymbols) free(initSymbols);
    if (kernelBfd) bfd_close(kernelBfd);
    if (initBfd) bfd_close(initBfd);

    // Write results to JSON file
    QJsonArray jsonArray;
    for (const auto& entry : entries) {
        jsonArray.append(logEntryToJson(entry));
    }

    QJsonDocument doc(jsonArray);
    
    QFile outputFileObj(outputFile);
    if (outputFileObj.open(QIODevice::WriteOnly)) {
        outputFileObj.write(doc.toJson());
    }
}

LogEntry LogWorker::processLine(const QString& line, int lineNumber, CapstoneDisassembler& disassembler, 
                               bfd* kernelBfd, asymbol** kernelSymbols, long kernelSymCount, 
                               bfd* initBfd, asymbol** initSymbols, long initSymCount) {
    LogEntry entry;
    entry.lineNumber = lineNumber;
    entry.originalLine = line.toStdString();
    entry.type = EntryType::OTHER;

    QString trimmedLine = line.trimmed();

    // Check if it's an instruction line: 0x[address]:  [hex bytes]  [assembly]
    QRegularExpression instrRegex(R"(^0x([0-9a-fA-F]+):\s+([0-9a-fA-F]{2}(?:\s[0-9a-fA-F]{2})*)\s{2,}(.+)$)");
    auto instrMatch = instrRegex.match(trimmedLine);

    if (instrMatch.hasMatch()) {
        entry.type = EntryType::INSTRUCTION;
        entry.address = ("0x" + instrMatch.captured(1)).toStdString();

        bool ok;
        entry.addressValue = instrMatch.captured(1).toULongLong(&ok, 16);
        if (ok) {
            entry.function = getFunctionNameFromAddress(entry.addressValue, kernelBfd, kernelSymbols, kernelSymCount, 
                                                       initBfd, initSymbols, initSymCount);

            QString hexStr = instrMatch.captured(2).simplified();
            hexStr.remove(' ');
            entry.hexBytes = hexStr.toStdString();

            QString asmStr = instrMatch.captured(3).trimmed();
            std::string intelAsm = disassembler.convertToIntel(asmStr.toStdString());
            entry.assembly = intelAsm;
        }

        return entry;
    }

    // Check for hardware interrupt
    QRegularExpression intRegex(R"(^Servicing hardware INT=0x([0-9a-fA-F]+))");
    auto intMatch = intRegex.match(trimmedLine);

    if (intMatch.hasMatch()) {
        entry.type = EntryType::INTERRUPT;
        entry.interruptNumber = intMatch.captured(1).toStdString();
        entry.assembly = "Hardware INT=0x" + entry.interruptNumber + " (click to expand)";
        return entry;
    }

    // Check for exception entries: "check_exception old: 0xffffffff new 0xe"
    QRegularExpression excRegex(R"(^check_exception\s+old:\s*0x([0-9a-fA-F]+)\s+new\s+0x([0-9a-fA-F]+))");
    auto excMatch = excRegex.match(trimmedLine);

    if (excMatch.hasMatch()) {
        entry.type = EntryType::INTERRUPT;
        entry.interruptNumber = excMatch.captured(2).toStdString();
        entry.assembly = "Exception 0x" + entry.interruptNumber + " (old: 0x" + excMatch.captured(1).toStdString() + ") (click to expand)";
        return entry;
    }

    // Check for register dump lines
    QRegularExpression regRegex(R"(^\s*(\d+):\s+v=([0-9a-fA-F]+)\s+e=([0-9a-fA-F]+))");
    auto regMatch = regRegex.match(trimmedLine);

    if (regMatch.hasMatch()) {
        entry.type = EntryType::REGISTER;
        entry.assembly = "CPU state dump (v=" + regMatch.captured(2).toStdString() + 
                        " e=" + regMatch.captured(3).toStdString() + ")";
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

std::string LogWorker::getFunctionNameFromAddress(uint64_t address, bfd* kernelBfd, asymbol** kernelSymbols, long kernelSymCount,
                                                  bfd* initBfd, asymbol** initSymbols, long initSymCount) {
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
        if (!sym || !sym->name) continue;

        if (!(sym->flags & (BSF_FUNCTION | BSF_GLOBAL | BSF_LOCAL))) continue;

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

QJsonObject LogWorker::logEntryToJson(const LogEntry& entry) {
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
