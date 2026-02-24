#include "capstone_disasm.h"

#include <QStringList>

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

std::string CapstoneDisassembler::convertToIntel(const std::string& atntAssembly) const {
    if (!handle) {
        // If Capstone is not available, try manual conversion
        return manualATTToIntelConversion(atntAssembly);
    }

    // First try to extract hex bytes from the full line format
    auto hexBytes = extractHexBytes(atntAssembly);

    if (!hexBytes.empty()) {
        std::vector<uint8_t> bytes = hexStringToBytes(hexBytes);
        if (!bytes.empty()) {
            cs_insn* insn;
            size_t count = cs_disasm(handle, bytes.data(), bytes.size(), 0x1000, 0, &insn);

            if (count > 0) {
                std::string result = std::string(insn[0].mnemonic) + " " + std::string(insn[0].op_str);
                cs_free(insn, count);
                return result;
            }
        }
    }

    // If Capstone conversion failed, fall back to manual conversion
    return manualATTToIntelConversion(atntAssembly);
}

std::string CapstoneDisassembler::manualATTToIntelConversion(const std::string& atntAssembly) {
    QString input = QString::fromStdString(atntAssembly);
    QString result = input;

    // Remove common QEMU prefixes and cleanup
    result = result.replace(QRegularExpression(R"(^\s*\d+:\s*)"), "");    // Remove line numbers
    result = result.replace(QRegularExpression(R"(\s*\[.*\]\s*)"), " ");  // Remove bracketed info
    result = result.simplified();

    // Skip non-instruction lines
    if (result.isEmpty() || result.contains("Exception") || result.contains("check_") || result.contains("RAX=") ||
        result.contains("RIP=") || result.contains("CR0=")) {
        return atntAssembly;
    }

    // AT&T to Intel conversions

    // 1. Convert register references: %reg -> reg
    result = result.replace(QRegularExpression(R"(%([a-zA-Z0-9]+))"), R"(\1)");

    // 2. Convert immediate values: $value -> value
    result = result.replace(QRegularExpression(R"(\$([0-9a-fA-Fx]+))"), R"(\1)");

    // 3. Convert memory references: offset(%base,%index,scale) -> [base+index*scale+offset]
    QRegularExpression memRegex(R"((-?0x[0-9a-fA-F]+|[0-9]+)?\(([^,\)]+)(?:,([^,\)]+))?(?:,([1248]))?\))");
    QRegularExpressionMatchIterator memIter = memRegex.globalMatch(result);
    while (memIter.hasNext()) {
        QRegularExpressionMatch match = memIter.next();
        QString offset = match.captured(1);
        QString base = match.captured(2);
        QString index = match.captured(3);
        QString scale = match.captured(4);

        QString memRef = "[";
        if (!base.isEmpty()) memRef += base;
        if (!index.isEmpty()) {
            if (!base.isEmpty()) memRef += "+";
            memRef += index;
            if (!scale.isEmpty() && scale != "1") {
                memRef += "*" + scale;
            }
        }
        if (!offset.isEmpty() && offset != "0") {
            if (!base.isEmpty() || !index.isEmpty()) {
                if (offset.startsWith("-")) {
                    memRef += offset;
                } else {
                    memRef += "+" + offset;
                }
            } else {
                memRef += offset;
            }
        }
        memRef += "]";

        result = result.replace(match.captured(0), memRef);
    }

    // 4. Convert instruction format: src, dst -> dst, src (Intel order)
    QRegularExpression instrRegex(R"(^(\w+(?:[lwbq])?)\s+([^,]+),\s*(.+)$)");
    QRegularExpressionMatch instrMatch = instrRegex.match(result);
    if (instrMatch.hasMatch()) {
        QString instruction = instrMatch.captured(1);
        QString src = instrMatch.captured(2).trimmed();
        QString dst = instrMatch.captured(3).trimmed();

        // Remove AT&T size suffixes and use Intel mnemonics
        instruction = instruction.replace(QRegularExpression("[lwbq]$"), "");

        // For some instructions, keep AT&T order (like cmp, test)
        QStringList keepATTOrder = {"cmp", "test", "bt", "bts", "btr", "btc"};
        if (keepATTOrder.contains(instruction.toLower())) {
            result = QString("%1 %2, %3").arg(instruction, src, dst);
        } else {
            // Standard Intel order: instruction dst, src
            result = QString("%1 %2, %3").arg(instruction, dst, src);
        }
    }

    // 5. Handle single operand instructions
    QRegularExpression singleOpRegex(R"(^(\w+(?:[lwbq])?)\s+(.+)$)");
    QRegularExpressionMatch singleMatch = singleOpRegex.match(result);
    if (singleMatch.hasMatch() && !result.contains(',')) {
        QString instruction = singleMatch.captured(1);
        QString operand = singleMatch.captured(2).trimmed();

        instruction = instruction.replace(QRegularExpression("[lwbq]$"), "");
        result = QString("%1 %2").arg(instruction, operand);
    }

    return result.toStdString();
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
                auto byte = static_cast<uint8_t>(std::stoul(byteStr, nullptr, 16));
                bytes.push_back(byte);
            } catch (...) {
                break;
            }
        }
    }

    return bytes;
}
