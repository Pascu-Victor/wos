#include "capstone_disasm.h"

#include <qcontainerfwd.h>
#include <qregularexpression.h>

#include <QStringList>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "capstone.h"

CapstoneDisassembler::CapstoneDisassembler() {
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

std::string CapstoneDisassembler::convert_to_intel(const std::string& atnt_assembly) const {
    if (!handle) {
        // If Capstone is not available, try manual conversion
        return manual_att_to_intel_conversion(atnt_assembly);
    }

    // First try to extract hex bytes from the full line format
    auto hex_bytes = extract_hex_bytes(atnt_assembly);

    if (!hex_bytes.empty()) {
        std::vector<uint8_t> bytes = hex_string_to_bytes(hex_bytes);
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
    return manual_att_to_intel_conversion(atnt_assembly);
}

std::string CapstoneDisassembler::manual_att_to_intel_conversion(const std::string& atnt_assembly) {
    QString input = QString::fromStdString(atnt_assembly);
    QString result = input;

    // Remove common QEMU prefixes and cleanup
    result = result.replace(QRegularExpression(R"(^\s*\d+:\s*)"), "");    // Remove line numbers
    result = result.replace(QRegularExpression(R"(\s*\[.*\]\s*)"), " ");  // Remove bracketed info
    result = result.simplified();

    // Skip non-instruction lines
    if (result.isEmpty() || result.contains("Exception") || result.contains("check_") || result.contains("RAX=") ||
        result.contains("RIP=") || result.contains("CR0=")) {
        return atnt_assembly;
    }

    // AT&T to Intel conversions

    // 1. Convert register references: %reg -> reg
    result = result.replace(QRegularExpression(R"(%([a-zA-Z0-9]+))"), R"(\1)");

    // 2. Convert immediate values: $value -> value
    result = result.replace(QRegularExpression(R"(\$([0-9a-fA-Fx]+))"), R"(\1)");

    // 3. Convert memory references: offset(%base,%index,scale) -> [base+index*scale+offset]
    QRegularExpression mem_regex(R"((-?0x[0-9a-fA-F]+|[0-9]+)?\(([^,\)]+)(?:,([^,\)]+))?(?:,([1248]))?\))");
    QRegularExpressionMatchIterator mem_iter = mem_regex.globalMatch(result);
    while (mem_iter.hasNext()) {
        QRegularExpressionMatch match = mem_iter.next();
        QString offset = match.captured(1);
        QString base = match.captured(2);
        QString index = match.captured(3);
        QString scale = match.captured(4);

        QString mem_ref = "[";
        if (!base.isEmpty()) {
            mem_ref += base;
        }
        if (!index.isEmpty()) {
            if (!base.isEmpty()) {
                mem_ref += "+";
            }
            mem_ref += index;
            if (!scale.isEmpty() && scale != "1") {
                mem_ref += "*" + scale;
            }
        }
        if (!offset.isEmpty() && offset != "0") {
            if (!base.isEmpty() || !index.isEmpty()) {
                if (offset.startsWith("-")) {
                    mem_ref += offset;
                } else {
                    mem_ref += "+" + offset;
                }
            } else {
                mem_ref += offset;
            }
        }
        mem_ref += "]";

        result = result.replace(match.captured(0), mem_ref);
    }

    // 4. Convert instruction format: src, dst -> dst, src (Intel order)
    QRegularExpression instr_regex(R"(^(\w+(?:[lwbq])?)\s+([^,]+),\s*(.+)$)");
    QRegularExpressionMatch instr_match = instr_regex.match(result);
    if (instr_match.hasMatch()) {
        QString instruction = instr_match.captured(1);
        QString src = instr_match.captured(2).trimmed();
        QString dst = instr_match.captured(3).trimmed();

        // Remove AT&T size suffixes and use Intel mnemonics
        instruction = instruction.replace(QRegularExpression("[lwbq]$"), "");

        // For some instructions, keep AT&T order (like cmp, test)
        QStringList keep_att_order = {"cmp", "test", "bt", "bts", "btr", "btc"};
        if (keep_att_order.contains(instruction.toLower())) {
            result = QString("%1 %2, %3").arg(instruction, src, dst);
        } else {
            // Standard Intel order: instruction dst, src
            result = QString("%1 %2, %3").arg(instruction, dst, src);
        }
    }

    // 5. Handle single operand instructions
    QRegularExpression single_op_regex(R"(^(\w+(?:[lwbq])?)\s+(.+)$)");
    QRegularExpressionMatch single_match = single_op_regex.match(result);
    if (single_match.hasMatch() && !result.contains(',')) {
        QString instruction = single_match.captured(1);
        QString operand = single_match.captured(2).trimmed();

        instruction = instruction.replace(QRegularExpression("[lwbq]$"), "");
        result = QString("%1 %2").arg(instruction, operand);
    }

    return result.toStdString();
}

std::string CapstoneDisassembler::extract_hex_bytes(const std::string& line) {
    QRegularExpression hex_regex(R"(:\s*([0-9a-fA-F\s]{2,})\s+)");
    QString qline = QString::fromStdString(line);
    auto match = hex_regex.match(qline);

    if (match.hasMatch()) {
        QString hex_str = match.captured(1).simplified();
        hex_str.remove(' ');
        return hex_str.toStdString();
    }

    return "";
}

std::vector<uint8_t> CapstoneDisassembler::hex_string_to_bytes(const std::string& hex) {
    std::vector<uint8_t> bytes;

    for (size_t i = 0; i < hex.length(); i += 2) {
        if (i + 1 < hex.length()) {
            std::string byte_str = hex.substr(i, 2);
            try {
                auto byte = static_cast<uint8_t>(std::stoul(byte_str, nullptr, 16));
                bytes.push_back(byte);
            } catch (...) {
                break;
            }
        }
    }

    return bytes;
}
