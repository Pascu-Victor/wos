#include "qemu_log_viewer.h"

#include <qcombobox.h>
#include <qcontainerfwd.h>
#include <qlineedit.h>
#include <qmainwindow.h>
#include <qnamespace.h>
#include <qoverload.h>
#include <qpushbutton.h>
#include <qscrollbar.h>
#include <qtablewidget.h>
#include <qtreewidget.h>
#include <qwidget.h>

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "config.h"
#include "virtual_table.h"

// Work around BFD config.h requirement
#define PACKAGE "qemu_log_viewer"
#define PACKAGE_VERSION "1.0"
extern "C" {
#include <bfd.h>
}
#include <capstone/capstone.h>

#include <QtCore/QDebug>
#include <QtCore/QDir>
#include <QtCore/QRegularExpression>
#include <QtCore/QStandardPaths>
#include <QtCore/QTimer>
#include <QtGui/QDesktopServices>
#include <QtGui/QPainter>
#include <QtGui/QSyntaxHighlighter>
#include <QtGui/QTextCharFormat>
#include <QtGui/QTextCursor>
#include <QtGui/QTextDocument>
#include <QtWidgets/QApplication>
#include <QtWidgets/QFileDialog>
#include <QtWidgets/QHeaderView>
#include <QtWidgets/QMessageBox>
#include <QtWidgets/QStyle>
#include <QtWidgets/QStyledItemDelegate>
#include <QtWidgets/QTextBrowser>
#include <algorithm>
#include <iomanip>
#include <sstream>

// Include our existing processing functions
#include <cxxabi.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>

// CapstoneDisassembler implementation
class CapstoneDisassembler {
   public:
    CapstoneDisassembler();
    ~CapstoneDisassembler();
    std::string convertToIntel(const std::string& atntAssembly);

   private:
    csh handle;
    std::string extractHexBytes(const std::string& line);
    std::vector<uint8_t> hexStringToBytes(const std::string& hex);
    std::string manualATTToIntelConversion(const std::string& atntAssembly);
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
                uint8_t byte = static_cast<uint8_t>(std::stoul(byteStr, nullptr, 16));
                bytes.push_back(byte);
            } catch (...) {
                break;
            }
        }
    }

    return bytes;
}

// Syntax Highlighter for C/C++ and Assembly
class SyntaxHighlighter : public QSyntaxHighlighter {
   public:
    explicit SyntaxHighlighter(QTextDocument* parent = nullptr) : QSyntaxHighlighter(parent) { setupHighlightingRules(); }

   protected:
    void highlightBlock(const QString& text) override {
        // Apply each rule to the text
        foreach (const HighlightingRule& rule, highlightingRules) {
            QRegularExpressionMatchIterator matchIterator = rule.pattern.globalMatch(text);
            while (matchIterator.hasNext()) {
                QRegularExpressionMatch match = matchIterator.next();
                setFormat(match.capturedStart(), match.capturedLength(), rule.format);
            }
        }
    }

   private:
    struct HighlightingRule {
        QRegularExpression pattern;
        QTextCharFormat format;
    };
    QVector<HighlightingRule> highlightingRules;

    void setupHighlightingRules() {
        HighlightingRule rule;

        // C/C++ Keywords (using brighter VS Code blue for better contrast)
        QTextCharFormat keywordFormat;
        keywordFormat.setForeground(QColor("#79C3FF"));  // Brighter VS Code keyword blue
        keywordFormat.setFontWeight(QFont::Bold);
        QStringList keywordPatterns;
        keywordPatterns << "\\bif\\b" << "\\belse\\b" << "\\bfor\\b" << "\\bwhile\\b"
                        << "\\bdo\\b" << "\\breturn\\b" << "\\bbreak\\b" << "\\bcontinue\\b"
                        << "\\bswitch\\b" << "\\bcase\\b" << "\\bdefault\\b" << "\\btry\\b"
                        << "\\bcatch\\b" << "\\bthrow\\b" << "\\bclass\\b" << "\\bstruct\\b"
                        << "\\bpublic\\b" << "\\bprivate\\b" << "\\bprotected\\b" << "\\bvirtual\\b"
                        << "\\bstatic\\b" << "\\bconst\\b" << "\\bvolatile\\b" << "\\bmutable\\b"
                        << "\\btypedef\\b" << "\\busing\\b" << "\\bnamespace\\b" << "\\btemplate\\b"
                        << "\\btypename\\b" << "\\bauto\\b" << "\\bdecltype\\b" << "\\bsizeof\\b"
                        << "\\bnew\\b" << "\\bdelete\\b" << "\\bthis\\b" << "\\bnullptr\\b"
                        << "\\bextern\\b" << "\\binline\\b" << "\\bfriend\\b" << "\\boperator\\b"
                        << "\\bgoto\\b" << "\\basm\\b" << "\\bregister\\b" << "\\btrue\\b" << "\\bfalse\\b"
                        << "\\band\\b" << "\\bor\\b" << "\\bnot\\b" << "\\bxor\\b" << "\\bbitor\\b"
                        << "\\bcompl\\b" << "\\band_eq\\b" << "\\bor_eq\\b" << "\\bxor_eq\\b"
                        << "\\bnot_eq\\b" << "\\balignof\\b" << "\\balignments\\b" << "\\bconstexpr\\b"
                        << "\\bconsteval\\b" << "\\bconstinit\\b" << "\\bnoexcept\\b" << "\\bthread_local\\b"
                        << "\\bstatic_assert\\b" << "\\bexplicit\\b" << "\\boverride\\b" << "\\bfinal\\b";

        foreach (const QString& pattern, keywordPatterns) {
            rule.pattern = QRegularExpression(pattern);
            rule.format = keywordFormat;
            highlightingRules.append(rule);
        }

        // Storage class specifiers (using distinct purple)
        QTextCharFormat storageFormat;
        storageFormat.setForeground(QColor("#E586FF"));  // Bright purple for storage
        storageFormat.setFontWeight(QFont::Bold);
        QStringList storagePatterns;
        storagePatterns << "\\bstatic\\b" << "\\bextern\\b" << "\\bregister\\b" << "\\bthread_local\\b"
                        << "\\bmutable\\b" << "\\bconstexpr\\b" << "\\bconsteval\\b" << "\\bconstinit\\b";

        foreach (const QString& pattern, storagePatterns) {
            rule.pattern = QRegularExpression(pattern);
            rule.format = storageFormat;
            highlightingRules.append(rule);
        }

        // Assembly instructions (using brighter teal)
        QTextCharFormat asmInstructionFormat;
        asmInstructionFormat.setForeground(QColor("#5DD9C0"));  // Brighter VS Code class/type teal
        asmInstructionFormat.setFontWeight(QFont::Bold);
        QStringList asmPatterns;
        // Basic x86-64 instructions
        asmPatterns << "\\bmov\\b" << "\\bpush\\b" << "\\bpop\\b" << "\\bcall\\b" << "\\bret\\b"
                    << "\\bjmp\\b" << "\\bje\\b" << "\\bjne\\b" << "\\bjz\\b" << "\\bjnz\\b"
                    << "\\badd\\b" << "\\bsub\\b" << "\\bmul\\b" << "\\bdiv\\b" << "\\binc\\b"
                    << "\\bdec\\b" << "\\bcmp\\b" << "\\btest\\b" << "\\band\\b" << "\\bor\\b"
                    << "\\bxor\\b" << "\\bnot\\b" << "\\bshl\\b" << "\\bshr\\b" << "\\blea\\b"
                    << "\\bnop\\b" << "\\bint\\b" << "\\biret\\b" << "\\bhlt\\b" << "\\bcli\\b"
                    << "\\bsti\\b" << "\\bpushf\\b" << "\\bpopf\\b" << "\\bloop\\b" << "\\brepz\\b"
                    << "\\brepnz\\b" << "\\bmovsb\\b" << "\\bmovsw\\b" << "\\bmovsd\\b" << "\\bxchg\\b"
                    << "\\brol\\b" << "\\bror\\b" << "\\brcl\\b" << "\\brcr\\b" << "\\bsal\\b"
                    << "\\bsar\\b" << "\\bsetc\\b" << "\\bsetz\\b" << "\\bsets\\b"
                    << "\\bseto\\b"
                    // Extended arithmetic and logic
                    << "\\bimul\\b" << "\\bidiv\\b" << "\\bcdq\\b" << "\\bcqo\\b" << "\\bcwd\\b"
                    << "\\bsar\\b" << "\\bshl\\b" << "\\bshr\\b" << "\\bshrd\\b" << "\\bshld\\b"
                    << "\\bbt\\b" << "\\bbtr\\b" << "\\bbts\\b" << "\\bbtc\\b" << "\\bbsf\\b"
                    << "\\bbsr\\b"
                    // Conditional jumps and sets
                    << "\\bjo\\b" << "\\bjno\\b" << "\\bjb\\b" << "\\bjnb\\b" << "\\bjae\\b" << "\\bjnae\\b"
                    << "\\bjc\\b" << "\\bjnc\\b" << "\\bje\\b" << "\\bjne\\b" << "\\bjz\\b" << "\\bjnz\\b"
                    << "\\bja\\b" << "\\bjna\\b" << "\\bjbe\\b" << "\\bjnbe\\b" << "\\bjs\\b" << "\\bjns\\b"
                    << "\\bjp\\b" << "\\bjnp\\b" << "\\bjpe\\b" << "\\bjpo\\b" << "\\bjl\\b" << "\\bjnl\\b"
                    << "\\bjge\\b" << "\\bjnge\\b" << "\\bjle\\b" << "\\bjnle\\b" << "\\bjg\\b" << "\\bjng\\b"
                    << "\\bseta\\b" << "\\bsetae\\b" << "\\bsetb\\b" << "\\bsetbe\\b" << "\\bsetc\\b"
                    << "\\bsete\\b" << "\\bsetg\\b" << "\\bsetge\\b" << "\\bsetl\\b" << "\\bsetle\\b"
                    << "\\bsetna\\b" << "\\bsetnae\\b" << "\\bsetnb\\b" << "\\bsetnbe\\b" << "\\bsetnc\\b"
                    << "\\bsetne\\b" << "\\bsetng\\b" << "\\bsetnge\\b" << "\\bsetnl\\b" << "\\bsetnle\\b"
                    << "\\bsetno\\b" << "\\bsetnp\\b" << "\\bsetns\\b" << "\\bsetnz\\b" << "\\bseto\\b"
                    << "\\bsetp\\b" << "\\bsetpe\\b" << "\\bsetpo\\b" << "\\bsets\\b"
                    << "\\bsetz\\b"
                    // String operations
                    << "\\bmovs\\b" << "\\bstos\\b" << "\\blods\\b" << "\\bscas\\b" << "\\bcmps\\b"
                    << "\\brep\\b" << "\\brepe\\b" << "\\brepne\\b" << "\\brepz\\b"
                    << "\\brepnz\\b"
                    // Stack operations
                    << "\\bpusha\\b" << "\\bpushad\\b" << "\\bpopa\\b" << "\\bpopad\\b"
                    << "\\benter\\b"
                    << "\\bleave\\b"
                    // MMX instructions
                    << "\\bemms\\b" << "\\bpacksswb\\b" << "\\bpackssdw\\b" << "\\bpackuswb\\b"
                    << "\\bpaddb\\b" << "\\bpaddw\\b" << "\\bpaddd\\b" << "\\bpaddsb\\b" << "\\bpaddsw\\b"
                    << "\\bpaddusb\\b" << "\\bpaddusw\\b" << "\\bpand\\b" << "\\bpandn\\b" << "\\bpor\\b"
                    << "\\bpxor\\b" << "\\bpcmpeqb\\b" << "\\bpcmpeqw\\b" << "\\bpcmpeqd\\b"
                    << "\\bpcmpgtb\\b" << "\\bpcmpgtw\\b" << "\\bpcmpgtd\\b" << "\\bpmaddwd\\b"
                    << "\\bpmulhw\\b" << "\\bpmullw\\b" << "\\bpsllw\\b" << "\\bpslld\\b" << "\\bpsllq\\b"
                    << "\\bpsraw\\b" << "\\bpsrad\\b" << "\\bpsrlw\\b" << "\\bpsrld\\b" << "\\bpsrlq\\b"
                    << "\\bpsubb\\b" << "\\bpsubw\\b" << "\\bpsubd\\b" << "\\bpsubsb\\b" << "\\bpsubsw\\b"
                    << "\\bpsubusb\\b" << "\\bpsubusw\\b" << "\\bpunpckhbw\\b" << "\\bpunpckhwd\\b"
                    << "\\bpunpckhdq\\b" << "\\bpunpcklbw\\b" << "\\bpunpcklwd\\b"
                    << "\\bpunpckldq\\b"
                    // SSE instructions
                    << "\\bmovaps\\b" << "\\bmovups\\b" << "\\bmovss\\b" << "\\bmovlps\\b" << "\\bmovhps\\b"
                    << "\\bmovlhps\\b" << "\\bmovhlps\\b" << "\\bmovmskps\\b" << "\\bmovntps\\b"
                    << "\\baddps\\b" << "\\baddss\\b" << "\\bsubps\\b" << "\\bsubss\\b"
                    << "\\bmulps\\b" << "\\bmulss\\b" << "\\bdivps\\b" << "\\bdivss\\b"
                    << "\\bsqrtps\\b" << "\\bsqrtss\\b" << "\\brsqrtps\\b" << "\\brsqrtss\\b"
                    << "\\brcpps\\b" << "\\brcpss\\b" << "\\bminps\\b" << "\\bminss\\b"
                    << "\\bmaxps\\b" << "\\bmaxss\\b" << "\\bandps\\b" << "\\bandnps\\b"
                    << "\\borps\\b" << "\\bxorps\\b" << "\\bcmpps\\b" << "\\bcmpss\\b"
                    << "\\bcomiss\\b" << "\\bucomiss\\b" << "\\bcvtpi2ps\\b" << "\\bcvtps2pi\\b"
                    << "\\bcvtsi2ss\\b" << "\\bcvtss2si\\b" << "\\bcvttps2pi\\b" << "\\bcvttss2si\\b"
                    << "\\bshufps\\b" << "\\bunpckhps\\b" << "\\bunpcklps\\b"
                    << "\\bprefetch\\b"
                    // SSE2 instructions
                    << "\\bmovapd\\b" << "\\bmovupd\\b" << "\\bmovsd\\b" << "\\bmovlpd\\b" << "\\bmovhpd\\b"
                    << "\\bmovmskpd\\b" << "\\bmovntpd\\b" << "\\bmovdqa\\b" << "\\bmovdqu\\b"
                    << "\\bmovq\\b" << "\\bpaddq\\b" << "\\bpsubq\\b" << "\\bpmuludq\\b"
                    << "\\baddpd\\b" << "\\baddsd\\b" << "\\bsubpd\\b" << "\\bsubsd\\b"
                    << "\\bmulpd\\b" << "\\bmulsd\\b" << "\\bdivpd\\b" << "\\bdivsd\\b"
                    << "\\bsqrtpd\\b" << "\\bsqrtsd\\b" << "\\bminpd\\b" << "\\bminsd\\b"
                    << "\\bmaxpd\\b" << "\\bmaxsd\\b" << "\\bandpd\\b" << "\\bandnpd\\b"
                    << "\\borpd\\b" << "\\bxorpd\\b" << "\\bcmppd\\b" << "\\bcmpsd\\b"
                    << "\\bcomisd\\b" << "\\bucomisd\\b" << "\\bshufpd\\b" << "\\bunpckhpd\\b"
                    << "\\bunpcklpd\\b" << "\\bpshufd\\b" << "\\bpshufhw\\b"
                    << "\\bpshuflw\\b"
                    // SSE3 instructions
                    << "\\baddsubps\\b" << "\\baddsubpd\\b" << "\\bhaddps\\b" << "\\bhaddpd\\b"
                    << "\\bhsubps\\b" << "\\bhsubpd\\b" << "\\bmovshdup\\b" << "\\bmovsldup\\b"
                    << "\\bmovddup\\b" << "\\blddqu\\b"
                    << "\\bfisttp\\b"
                    // SSSE3 instructions
                    << "\\bpabsb\\b" << "\\bpabsw\\b" << "\\bpabsd\\b" << "\\bpalignr\\b"
                    << "\\bphaddw\\b" << "\\bphaddd\\b" << "\\bphaddsw\\b" << "\\bphsubw\\b"
                    << "\\bphsubd\\b" << "\\bphsubsw\\b" << "\\bpmaddubsw\\b" << "\\bpmulhrsw\\b"
                    << "\\bpshufb\\b" << "\\bpsignb\\b" << "\\bpsignw\\b"
                    << "\\bpsignd\\b"
                    // SSE4.1 instructions
                    << "\\bblendpd\\b" << "\\bblendps\\b" << "\\bblendvpd\\b" << "\\bblendvps\\b"
                    << "\\bdppd\\b" << "\\bdpps\\b" << "\\bextractps\\b" << "\\binsertps\\b"
                    << "\\bmovntdqa\\b" << "\\bmpsadbw\\b" << "\\bpackusdw\\b" << "\\bpblendvb\\b"
                    << "\\bpblendw\\b" << "\\bpcmpeqq\\b" << "\\bpextrb\\b" << "\\bpextrd\\b"
                    << "\\bpextrq\\b" << "\\bpextrw\\b" << "\\bphminposuw\\b" << "\\bpinsrb\\b"
                    << "\\bpinsrd\\b" << "\\bpinsrq\\b" << "\\bpmaxsb\\b" << "\\bpmaxsd\\b"
                    << "\\bpmaxud\\b" << "\\bpmaxuw\\b" << "\\bpminsb\\b" << "\\bpminsd\\b"
                    << "\\bpminud\\b" << "\\bpminuw\\b" << "\\bpmovsxbw\\b" << "\\bpmovsxbd\\b"
                    << "\\bpmovsxbq\\b" << "\\bpmovsxwd\\b" << "\\bpmovsxwq\\b" << "\\bpmovsxdq\\b"
                    << "\\bpmovzxbw\\b" << "\\bpmovzxbd\\b" << "\\bpmovzxbq\\b" << "\\bpmovzxwd\\b"
                    << "\\bpmovzxwq\\b" << "\\bpmovzxdq\\b" << "\\bpmuldq\\b" << "\\bpmulld\\b"
                    << "\\bptest\\b" << "\\broundpd\\b" << "\\broundps\\b" << "\\broundsd\\b"
                    << "\\broundss\\b"
                    // SSE4.2 instructions
                    << "\\bpcmpestri\\b" << "\\bpcmpestrm\\b" << "\\bpcmpistri\\b" << "\\bpcmpistrm\\b"
                    << "\\bpcmpgtq\\b" << "\\bcrc32\\b"
                    << "\\bpopcnt\\b"
                    // AVX instructions
                    << "\\bvmovaps\\b" << "\\bvmovapd\\b" << "\\bvmovups\\b" << "\\bvmovupd\\b"
                    << "\\bvmovss\\b" << "\\bvmovsd\\b" << "\\bvmovlps\\b" << "\\bvmovhps\\b"
                    << "\\bvmovlpd\\b" << "\\bvmovhpd\\b" << "\\bvmovdqa\\b" << "\\bvmovdqu\\b"
                    << "\\bvaddps\\b" << "\\bvaddpd\\b" << "\\bvaddss\\b" << "\\bvaddsd\\b"
                    << "\\bvsubps\\b" << "\\bvsubpd\\b" << "\\bvsubss\\b" << "\\bvsubsd\\b"
                    << "\\bvmulps\\b" << "\\bvmulpd\\b" << "\\bvmulss\\b" << "\\bvmulsd\\b"
                    << "\\bvdivps\\b" << "\\bvdivpd\\b" << "\\bvdivss\\b" << "\\bvdivsd\\b"
                    << "\\bvsqrtps\\b" << "\\bvsqrtpd\\b" << "\\bvsqrtss\\b" << "\\bvsqrtsd\\b"
                    << "\\bvmaxps\\b" << "\\bvmaxpd\\b" << "\\bvmaxss\\b" << "\\bvmaxsd\\b"
                    << "\\bvminps\\b" << "\\bvminpd\\b" << "\\bvminss\\b" << "\\bvminsd\\b"
                    << "\\bvandps\\b" << "\\bvandpd\\b" << "\\bvandnps\\b" << "\\bvandnpd\\b"
                    << "\\bvorps\\b" << "\\bvorpd\\b" << "\\bvxorps\\b" << "\\bvxorpd\\b"
                    << "\\bvblendps\\b" << "\\bvblendpd\\b" << "\\bvblendvps\\b" << "\\bvblendvpd\\b"
                    << "\\bvbroadcastss\\b" << "\\bvbroadcastsd\\b" << "\\bvbroadcastf128\\b"
                    << "\\bvcmpps\\b" << "\\bvcmppd\\b" << "\\bvcmpss\\b" << "\\bvcmpsd\\b"
                    << "\\bvcvtps2pd\\b" << "\\bvcvtpd2ps\\b" << "\\bvcvtss2sd\\b" << "\\bvcvtsd2ss\\b"
                    << "\\bvdpps\\b" << "\\bvhaddps\\b" << "\\bvhaddpd\\b" << "\\bvhsubps\\b" << "\\bvhsubpd\\b"
                    << "\\bvinsertf128\\b" << "\\bvextractf128\\b" << "\\bvperm2f128\\b"
                    << "\\bvshufps\\b" << "\\bvshufpd\\b" << "\\bvunpckhps\\b" << "\\bvunpcklps\\b"
                    << "\\bvunpckhpd\\b" << "\\bvunpcklpd\\b" << "\\bvzeroupper\\b"
                    << "\\bvzeroall\\b"
                    // AVX2 instructions
                    << "\\bvbroadcasti128\\b" << "\\bvextracti128\\b" << "\\bvinserti128\\b"
                    << "\\bvperm2i128\\b" << "\\bvpermd\\b" << "\\bvpermps\\b" << "\\bvpermpd\\b"
                    << "\\bvpermq\\b" << "\\bvpsllvd\\b" << "\\bvpsllvq\\b" << "\\bvpsrlvd\\b"
                    << "\\bvpsrlvq\\b" << "\\bvpsravd\\b" << "\\bvgatherdps\\b" << "\\bvgatherqps\\b"
                    << "\\bvgatherdpd\\b" << "\\bvgatherqpd\\b" << "\\bvpgatherdd\\b" << "\\bvpgatherqd\\b"
                    << "\\bvpgatherdq\\b" << "\\bvpgatherqq\\b" << "\\bvpabsb\\b" << "\\bvpabsw\\b"
                    << "\\bvpabsd\\b" << "\\bvpacksswb\\b" << "\\bvpackssdw\\b" << "\\bvpackusdw\\b"
                    << "\\bvpackuswb\\b" << "\\bvpaddb\\b" << "\\bvpaddw\\b" << "\\bvpaddd\\b"
                    << "\\bvpaddq\\b" << "\\bvpaddsb\\b" << "\\bvpaddsw\\b" << "\\bvpaddusb\\b"
                    << "\\bvpaddusw\\b" << "\\bvpalignr\\b" << "\\bvpand\\b" << "\\bvpandn\\b"
                    << "\\bvpavgb\\b" << "\\bvpavgw\\b" << "\\bvpblendvb\\b" << "\\bvpblendw\\b"
                    << "\\bvpcmpeqb\\b" << "\\bvpcmpeqw\\b" << "\\bvpcmpeqd\\b" << "\\bvpcmpeqq\\b"
                    << "\\bvpcmpgtb\\b" << "\\bvpcmpgtw\\b" << "\\bvpcmpgtd\\b" << "\\bvpcmpgtq\\b"
                    << "\\bvphaddd\\b" << "\\bvphaddw\\b" << "\\bvphaddsw\\b" << "\\bvphsubd\\b"
                    << "\\bvphsubw\\b" << "\\bvphsubsw\\b" << "\\bvpmaddubsw\\b" << "\\bvpmaddwd\\b"
                    << "\\bvpmaxsb\\b" << "\\bvpmaxsw\\b" << "\\bvpmaxsd\\b" << "\\bvpmaxub\\b"
                    << "\\bvpmaxuw\\b" << "\\bvpmaxud\\b" << "\\bvpminsb\\b" << "\\bvpminsw\\b"
                    << "\\bvpminsd\\b" << "\\bvpminub\\b" << "\\bvpminuw\\b" << "\\bvpminud\\b"
                    << "\\bvpmovmskb\\b" << "\\bvpmovsxbw\\b" << "\\bvpmovsxbd\\b" << "\\bvpmovsxbq\\b"
                    << "\\bvpmovsxwd\\b" << "\\bvpmovsxwq\\b" << "\\bvpmovsxdq\\b" << "\\bvpmovzxbw\\b"
                    << "\\bvpmovzxbd\\b" << "\\bvpmovzxbq\\b" << "\\bvpmovzxwd\\b" << "\\bvpmovzxwq\\b"
                    << "\\bvpmovzxdq\\b" << "\\bvpmuldq\\b" << "\\bvpmulhrsw\\b" << "\\bvpmulhuw\\b"
                    << "\\bvpmulhw\\b" << "\\bvpmulld\\b" << "\\bvpmullw\\b" << "\\bvpmuludq\\b"
                    << "\\bvpor\\b" << "\\bvpsadbw\\b" << "\\bvpshufb\\b" << "\\bvpshufd\\b"
                    << "\\bvpshufhw\\b" << "\\bvpshuflw\\b" << "\\bvpsignb\\b" << "\\bvpsignw\\b"
                    << "\\bvpsignd\\b" << "\\bvpslldq\\b" << "\\bvpsllw\\b" << "\\bvpslld\\b"
                    << "\\bvpsllq\\b" << "\\bvpsraw\\b" << "\\bvpsrad\\b" << "\\bvpsrldq\\b"
                    << "\\bvpsrlw\\b" << "\\bvpsrld\\b" << "\\bvpsrlq\\b" << "\\bvpsubb\\b"
                    << "\\bvpsubw\\b" << "\\bvpsubd\\b" << "\\bvpsubq\\b" << "\\bvpsubsb\\b"
                    << "\\bvpsubsw\\b" << "\\bvpsubusb\\b" << "\\bvpsubusw\\b" << "\\bvptest\\b"
                    << "\\bvpunpckhbw\\b" << "\\bvpunpckhwd\\b" << "\\bvpunpckhdq\\b" << "\\bvpunpckhqdq\\b"
                    << "\\bvpunpcklbw\\b" << "\\bvpunpcklwd\\b" << "\\bvpunpckldq\\b" << "\\bvpunpcklqdq\\b"
                    << "\\bvpxor\\b"
                    // AVX-512 Foundation instructions
                    << "\\bvmovaps\\b" << "\\bvmovapd\\b" << "\\bvmovups\\b" << "\\bvmovupd\\b"
                    << "\\bvmovdqa32\\b" << "\\bvmovdqa64\\b" << "\\bvmovdqu32\\b" << "\\bvmovdqu64\\b"
                    << "\\bvbroadcastf32x4\\b" << "\\bvbroadcastf64x4\\b" << "\\bvbroadcasti32x4\\b"
                    << "\\bvbroadcasti64x4\\b" << "\\bvextractf32x4\\b" << "\\bvextractf64x4\\b"
                    << "\\bvextracti32x4\\b" << "\\bvextracti64x4\\b" << "\\bvinsertf32x4\\b"
                    << "\\bvinsertf64x4\\b" << "\\bvinserti32x4\\b" << "\\bvinserti64x4\\b"
                    << "\\bvshuff32x4\\b" << "\\bvshuff64x2\\b" << "\\bvshufi32x4\\b" << "\\bvshufi64x2\\b"
                    << "\\bvcompresspd\\b" << "\\bvcompressps\\b" << "\\bvpcompressd\\b" << "\\bvpcompressq\\b"
                    << "\\bvexpandpd\\b" << "\\bvexpandps\\b" << "\\bvpexpandd\\b" << "\\bvpexpandq\\b"
                    << "\\bkandw\\b" << "\\bkandb\\b" << "\\bkandq\\b" << "\\bkandd\\b"
                    << "\\bkorw\\b" << "\\bkorb\\b" << "\\bkorq\\b" << "\\bkord\\b"
                    << "\\bkxorw\\b" << "\\bkxorb\\b" << "\\bkxorq\\b" << "\\bkxord\\b"
                    << "\\bknotw\\b" << "\\bknotb\\b" << "\\bknotq\\b" << "\\bknotd\\b";

        foreach (const QString& pattern, asmPatterns) {
            rule.pattern = QRegularExpression(pattern);
            rule.format = asmInstructionFormat;
            highlightingRules.append(rule);
        }

        // Registers (using brighter variable blue)
        QTextCharFormat registerFormat;
        registerFormat.setForeground(QColor("#B8E6FF"));  // Brighter VS Code variable blue
        rule.pattern = QRegularExpression(
            "\\b[re]?[a-d]x\\b|\\b[re]?[sd]i\\b|\\b[re]?[sb]p\\b|\\br[8-9]\\b|\\br1[0-5]\\b|\\beax\\b|\\bebx\\b|\\becx\\b|\\bedx\\b|"
            "\\besi\\b|\\bedi\\b|\\besp\\b|\\bebp\\b|\\beip\\b|\\brip\\b|\\bcs\\b|\\bds\\b|\\bes\\b|\\bfs\\b|\\bgs\\b|\\bss\\b|\\bmm[0-7]"
            "\\b|\\bxmm[0-9]\\b|\\bxmm1[0-5]\\b|\\bxmm[23][0-9]\\b|\\bxmm3[01]\\b|\\bymm[0-9]\\b|\\bymm1[0-5]\\b|\\bymm[23][0-9]\\b|"
            "\\bymm3[01]\\b|\\bzmm[0-9]\\b|\\bzmm1[0-5]\\b|\\bzmm[23][0-9]\\b|\\bzmm3[01]\\b|\\bk[0-7]\\b|\\bst[0-7]\\b|\\bcr[0-8]\\b|"
            "\\bdr[0-7]\\b");
        rule.format = registerFormat;
        highlightingRules.append(rule);

        // Numbers (hex and decimal) (using brighter number green)
        QTextCharFormat numberFormat;
        numberFormat.setForeground(QColor("#C8E6B8"));  // Brighter VS Code number green
        rule.pattern = QRegularExpression("\\b0x[0-9a-fA-F]+\\b|\\b[0-9]+\\b|\\$0x[0-9a-fA-F]+|\\$[0-9]+");
        rule.format = numberFormat;
        highlightingRules.append(rule);

        // Special characters and operators (using bright orange)
        QTextCharFormat specialFormat;
        specialFormat.setForeground(QColor("#FF9A6B"));  // Bright orange for operators
        specialFormat.setFontWeight(QFont::Bold);
        rule.pattern = QRegularExpression(
            "[\\+\\-\\*\\/"
            "\\%\\=\\!\\<\\>\\&\\|\\^\\~\\?\\:\\;\\,]|\\+\\+|\\-\\-|\\<\\<|\\>\\>|\\=\\=|\\!\\=|\\<\\=|\\>\\=|\\&\\&|\\|\\||\\+\\=|\\-\\=|"
            "\\*\\=|\\/\\=|\\%\\=|\\&\\=|\\|\\=|\\^\\=|\\<\\<\\=|\\>\\>\\=|\\-\\>|\\:\\:");
        rule.format = specialFormat;
        highlightingRules.append(rule);

        // Brackets and parentheses (using bright cyan)
        QTextCharFormat bracketFormat;
        bracketFormat.setForeground(QColor("#00E5FF"));  // Bright cyan for brackets
        bracketFormat.setFontWeight(QFont::Bold);
        rule.pattern = QRegularExpression("[\\(\\)\\[\\]\\{\\}]");
        rule.format = bracketFormat;
        highlightingRules.append(rule);

        // Memory addresses (using bright orange)
        QTextCharFormat memoryFormat;
        memoryFormat.setForeground(QColor("#FFD68A"));  // Brighter VS Code string orange
        rule.pattern = QRegularExpression("\\[[^\\]]+\\]|\\([^\\)]+\\)");
        rule.format = memoryFormat;
        highlightingRules.append(rule);

        // Comments (using brighter comment green)
        QTextCharFormat commentFormat;
        commentFormat.setForeground(QColor("#7CB555"));  // Brighter VS Code comment green
        commentFormat.setFontItalic(true);
        rule.pattern = QRegularExpression("//[^\n]*|/\\*.*\\*/|#[^\n]*");
        rule.format = commentFormat;
        highlightingRules.append(rule);

        // Strings (using brighter string color)
        QTextCharFormat stringFormat;
        stringFormat.setForeground(QColor("#E6B678"));  // Brighter VS Code string brown
        rule.pattern = QRegularExpression("\".*?\"|'.*?'");
        rule.format = stringFormat;
        highlightingRules.append(rule);

        // Function names (using brighter function yellow)
        QTextCharFormat functionFormat;
        functionFormat.setForeground(QColor("#FFE86A"));  // Brighter VS Code function yellow
        rule.pattern = QRegularExpression("\\b[A-Za-z_][A-Za-z0-9_]*(?=\\s*\\()");
        rule.format = functionFormat;
        highlightingRules.append(rule);

        // Types (using brighter type teal)
        QTextCharFormat typeFormat;
        typeFormat.setForeground(QColor("#5DD9C0"));  // Brighter VS Code type teal
        QStringList typePatterns;
        typePatterns << "\\bint\\b" << "\\bchar\\b" << "\\bfloat\\b" << "\\bdouble\\b"
                     << "\\blong\\b" << "\\bshort\\b" << "\\bunsigned\\b" << "\\bsigned\\b"
                     << "\\bbool\\b" << "\\bvoid\\b" << "\\bsize_t\\b" << "\\buint8_t\\b"
                     << "\\buint16_t\\b" << "\\buint32_t\\b" << "\\buint64_t\\b"
                     << "\\bint8_t\\b" << "\\bint16_t\\b" << "\\bint32_t\\b" << "\\bint64_t\\b"
                     << "\\bssize_t\\b" << "\\bptrdiff_t\\b" << "\\bintptr_t\\b" << "\\buintptr_t\\b"
                     << "\\bwchar_t\\b" << "\\bchar16_t\\b" << "\\bchar32_t\\b" << "\\bchar8_t\\b";

        foreach (const QString& pattern, typePatterns) {
            rule.pattern = QRegularExpression(pattern);
            rule.format = typeFormat;
            highlightingRules.append(rule);
        }

        // Preprocessor directives (using brighter preprocessor purple)
        QTextCharFormat preprocessorFormat;
        preprocessorFormat.setForeground(QColor("#D586C0"));  // Brighter VS Code preprocessor purple
        rule.pattern = QRegularExpression("^\\s*#\\w+");
        rule.format = preprocessorFormat;
        highlightingRules.append(rule);

        // Line numbers and addresses (using brighter number color)
        QTextCharFormat lineNumberFormat;
        lineNumberFormat.setForeground(QColor("#C8E6B8"));
        rule.pattern = QRegularExpression("^\\s*\\d+:");
        rule.format = lineNumberFormat;
        highlightingRules.append(rule);

        // Exception/Error keywords (using brighter error red)
        QTextCharFormat errorFormat;
        errorFormat.setForeground(QColor("#FF6B6B"));  // Brighter VS Code error red
        errorFormat.setFontWeight(QFont::Bold);
        QStringList errorPatterns;
        errorPatterns << "\\bERROR\\b" << "\\bFAIL\\b" << "\\bFATAL\\b" << "\\bPANIC\\b"
                      << "\\bEXCEPTION\\b" << "\\bSEGFAULT\\b" << "\\bCRASH\\b" << "\\bASSERT\\b"
                      << "\\bABORT\\b" << "\\bWARNING\\b" << "\\bWARN\\b";

        foreach (const QString& pattern, errorPatterns) {
            rule.pattern = QRegularExpression(pattern);
            rule.format = errorFormat;
            highlightingRules.append(rule);
        }

        // Macros and constants (using bright magenta)
        QTextCharFormat macroFormat;
        macroFormat.setForeground(QColor("#FF79C6"));  // Bright magenta for macros
        macroFormat.setFontWeight(QFont::Bold);
        rule.pattern = QRegularExpression("\\b[A-Z_][A-Z0-9_]{2,}\\b");
        rule.format = macroFormat;
        highlightingRules.append(rule);
    }
};

// Custom delegate for syntax highlighting in table cells
class SyntaxHighlightDelegate : public QStyledItemDelegate {
   public:
    explicit SyntaxHighlightDelegate(QObject* parent = nullptr) : QStyledItemDelegate(parent) { setupFormats(); }

    void paint(QPainter* painter, const QStyleOptionViewItem& option, const QModelIndex& index) const override {
        int column = index.column();

        if (column == 3 || column == 5) {  // Function and Assembly columns (updated indices)
            // Custom rendering for syntax highlighted columns
            QString text = index.data(Qt::DisplayRole).toString();

            // Draw the item background (selection, hover, etc.) manually
            painter->save();

            // Draw background color
            if (option.state & QStyle::State_Selected) {
                painter->fillRect(option.rect, option.palette.highlight());
            } else {
                // Use the background color from the item if set
                QVariant bg = index.data(Qt::BackgroundRole);
                if (bg.isValid()) {
                    painter->fillRect(option.rect, bg.value<QBrush>());
                } else {
                    painter->fillRect(option.rect, option.palette.base());
                }
            }

            // Draw focus rectangle if needed
            if (option.state & QStyle::State_HasFocus) {
                QStyleOptionFocusRect focusOption;
                focusOption.rect = option.rect;
                focusOption.palette = option.palette;
                focusOption.state = option.state;
                QApplication::style()->drawPrimitive(QStyle::PE_FrameFocusRect, &focusOption, painter);
            }

            painter->restore();

            // Now draw our custom highlighted text
            paintHighlightedText(painter, option, text, column);
        } else {
            // For other columns, use default rendering
            QStyledItemDelegate::paint(painter, option, index);
        }
    }

   private:
    QTextCharFormat keywordFormat;
    QTextCharFormat asmInstructionFormat;
    QTextCharFormat registerFormat;
    QTextCharFormat numberFormat;
    QTextCharFormat functionFormat;
    QTextCharFormat typeFormat;

    void setupFormats() {
        // Keywords
        keywordFormat.setForeground(QColor(86, 156, 214));  // Light blue
        keywordFormat.setFontWeight(QFont::Bold);

        // Assembly instructions
        asmInstructionFormat.setForeground(QColor(78, 201, 176));  // Light teal
        asmInstructionFormat.setFontWeight(QFont::Bold);

        // Registers
        registerFormat.setForeground(QColor(220, 220, 170));  // Light yellow

        // Numbers
        numberFormat.setForeground(QColor(181, 206, 168));  // Light green

        // Functions
        functionFormat.setForeground(QColor(220, 220, 170));  // Light yellow

        // Types
        typeFormat.setForeground(QColor(78, 201, 176));  // Light teal
    }

    void paintHighlightedText(QPainter* painter, const QStyleOptionViewItem& option, const QString& text, int column) const {
        if (text.isEmpty()) return;

        QRect textRect = option.rect.adjusted(4, 2, -4, -2);
        painter->save();
        painter->setClipRect(textRect);

        QFont font = option.font;
        painter->setFont(font);

        // Use elided text to prevent overflow
        QFontMetrics fm(font);
        QString elidedText = fm.elidedText(text, Qt::ElideRight, textRect.width());

        // Apply syntax highlighting based on column
        if (column == 5) {  // Assembly column (updated index)
            paintAssemblyHighlighting(painter, textRect, elidedText, font);
        } else if (column == 3) {  // Function column
            paintFunctionHighlighting(painter, textRect, elidedText, font);
        }

        painter->restore();
    }

    void paintAssemblyHighlighting(QPainter* painter, const QRect& rect, const QString& text, const QFont& font) const {
        QFontMetrics fm(font);
        int x = rect.x();
        int y = rect.y() + fm.ascent() + (rect.height() - fm.height()) / 2;

        // Simple word-by-word highlighting for assembly
        QStringList words = text.split(QRegularExpression("\\s+"), Qt::SkipEmptyParts);

        for (const QString& word : words) {
            if (x >= rect.right()) break;  // Stop if we're outside the rect

            QColor color = getAssemblyWordColor(word);
            painter->setPen(color);

            int wordWidth = fm.horizontalAdvance(word);
            if (x + wordWidth > rect.right()) {
                // Truncate the word if it would overflow
                QString truncated = fm.elidedText(word, Qt::ElideRight, rect.right() - x);
                painter->drawText(x, y, truncated);
                break;
            }

            painter->drawText(x, y, word);
            x += wordWidth + fm.horizontalAdvance(" ");
        }
    }

    void paintFunctionHighlighting(QPainter* painter, const QRect& rect, const QString& text, const QFont& font) const {
        QFontMetrics fm(font);
        Q_UNUSED(fm)  // Suppress unused variable warning

        // Highlight function names (VS Code function yellow)
        QColor color = QColor("#DCDCAA");  // VS Code function yellow
        if (text.contains(".asm") || text.contains(".s")) {
            color = QColor("#B5CEA8");  // VS Code number green for assembly files
        } else if (text.contains(".c") || text.contains(".cpp") || text.contains(".h") || text.contains(".hpp")) {
            color = QColor("#9CDCFE");  // VS Code variable blue for C/C++ files
        } else if (text.contains("kernel") || text.contains("vmlinux")) {
            color = QColor("#4EC9B0");  // VS Code type teal for kernel functions
        }

        painter->setPen(color);
        painter->drawText(rect, Qt::AlignLeft | Qt::AlignVCenter, text);
    }

    QColor getAssemblyWordColor(const QString& word) const {
        // Assembly instructions (VS Code teal) - Comprehensive x86-64 instruction set
        QStringList instructions = {
            // Basic x86-64 instructions
            "mov", "push", "pop", "call", "ret", "jmp", "je", "jne", "jz", "jnz", "add", "sub", "mul", "div", "inc", "dec", "cmp", "test",
            "and", "or", "xor", "not", "shl", "shr", "lea", "nop", "int", "iret", "hlt", "cli", "sti", "pushf", "popf", "loop", "repz",
            "repnz", "movsb", "movsw", "movsd", "xchg", "rol", "ror", "rcl", "rcr", "sal", "sar", "setc", "setz", "sets", "seto",
            // Extended arithmetic and logic
            "imul", "idiv", "cdq", "cqo", "cwd", "shrd", "shld", "bt", "btr", "bts", "btc", "bsf", "bsr", "popcnt",
            // Conditional jumps and sets
            "jo", "jno", "jb", "jnb", "jae", "jnae", "jc", "jnc", "ja", "jna", "jbe", "jnbe", "js", "jns", "jp", "jnp", "jpe", "jpo", "jl",
            "jnl", "jge", "jnge", "jle", "jnle", "jg", "jng", "seta", "setae", "setb", "setbe", "sete", "setg", "setge", "setl", "setle",
            "setna", "setnae", "setnb", "setnbe", "setnc", "setne", "setng", "setnge", "setnl", "setnle", "setno", "setnp", "setns",
            "setnz", "setp", "setpe", "setpo",
            // String operations
            "movs", "stos", "lods", "scas", "cmps", "rep", "repe", "repne",
            // Stack operations
            "pusha", "pushad", "popa", "popad", "enter", "leave",
            // MMX instructions
            "emms", "packsswb", "packssdw", "packuswb", "paddb", "paddw", "paddd", "paddsb", "paddsw", "paddusb", "paddusw", "pand",
            "pandn", "por", "pxor", "pcmpeqb", "pcmpeqw", "pcmpeqd", "pcmpgtb", "pcmpgtw", "pcmpgtd", "pmaddwd", "pmulhw", "pmullw",
            "psllw", "pslld", "psllq", "psraw", "psrad", "psrlw", "psrld", "psrlq", "psubb", "psubw", "psubd", "psubsb", "psubsw",
            "psubusb", "psubusw", "punpckhbw", "punpckhwd", "punpckhdq", "punpcklbw", "punpcklwd", "punpckldq",
            // SSE instructions
            "movaps", "movups", "movss", "movlps", "movhps", "movlhps", "movhlps", "movmskps", "movntps", "addps", "addss", "subps",
            "subss", "mulps", "mulss", "divps", "divss", "sqrtps", "sqrtss", "rsqrtps", "rsqrtss", "rcpps", "rcpss", "minps", "minss",
            "maxps", "maxss", "andps", "andnps", "orps", "xorps", "cmpps", "cmpss", "comiss", "ucomiss", "cvtpi2ps", "cvtps2pi", "cvtsi2ss",
            "cvtss2si", "cvttps2pi", "cvttss2si", "shufps", "unpckhps", "unpcklps", "prefetch",
            // SSE2 instructions
            "movapd", "movupd", "movlpd", "movhpd", "movmskpd", "movntpd", "movdqa", "movdqu", "movq", "paddq", "psubq", "pmuludq", "addpd",
            "addsd", "subpd", "subsd", "mulpd", "mulsd", "divpd", "divsd", "sqrtpd", "sqrtsd", "minpd", "minsd", "maxpd", "maxsd", "andpd",
            "andnpd", "orpd", "xorpd", "cmppd", "cmpsd", "comisd", "ucomisd", "shufpd", "unpckhpd", "unpcklpd", "pshufd", "pshufhw",
            "pshuflw",
            // SSE3 instructions
            "addsubps", "addsubpd", "haddps", "haddpd", "hsubps", "hsubpd", "movshdup", "movsldup", "movddup", "lddqu", "fisttp",
            // SSSE3 instructions
            "pabsb", "pabsw", "pabsd", "palignr", "phaddw", "phaddd", "phaddsw", "phsubw", "phsubd", "phsubsw", "pmaddubsw", "pmulhrsw",
            "pshufb", "psignb", "psignw", "psignd",
            // SSE4.1 instructions
            "blendpd", "blendps", "blendvpd", "blendvps", "dppd", "dpps", "extractps", "insertps", "movntdqa", "mpsadbw", "packusdw",
            "pblendvb", "pblendw", "pcmpeqq", "pextrb", "pextrd", "pextrq", "pextrw", "phminposuw", "pinsrb", "pinsrd", "pinsrq", "pmaxsb",
            "pmaxsd", "pmaxud", "pmaxuw", "pminsb", "pminsd", "pminud", "pminuw", "pmovsxbw", "pmovsxbd", "pmovsxbq", "pmovsxwd",
            "pmovsxwq", "pmovsxdq", "pmovzxbw", "pmovzxbd", "pmovzxbq", "pmovzxwd", "pmovzxwq", "pmovzxdq", "pmuldq", "pmulld", "ptest",
            "roundpd", "roundps", "roundsd", "roundss",
            // SSE4.2 instructions
            "pcmpestri", "pcmpestrm", "pcmpistri", "pcmpistrm", "pcmpgtq", "crc32",
            // AVX instructions
            "vmovaps", "vmovapd", "vmovups", "vmovupd", "vmovss", "vmovsd", "vmovlps", "vmovhps", "vmovlpd", "vmovhpd", "vmovdqa",
            "vmovdqu", "vaddps", "vaddpd", "vaddss", "vaddsd", "vsubps", "vsubpd", "vsubss", "vsubsd", "vmulps", "vmulpd", "vmulss",
            "vmulsd", "vdivps", "vdivpd", "vdivss", "vdivsd", "vsqrtps", "vsqrtpd", "vsqrtss", "vsqrtsd", "vmaxps", "vmaxpd", "vmaxss",
            "vmaxsd", "vminps", "vminpd", "vminss", "vminsd", "vandps", "vandpd", "vandnps", "vandnpd", "vorps", "vorpd", "vxorps",
            "vxorpd", "vblendps", "vblendpd", "vblendvps", "vblendvpd", "vbroadcastss", "vbroadcastsd", "vbroadcastf128", "vcmpps",
            "vcmppd", "vcmpss", "vcmpsd", "vcvtps2pd", "vcvtpd2ps", "vcvtss2sd", "vcvtsd2ss", "vdpps", "vhaddps", "vhaddpd", "vhsubps",
            "vhsubpd", "vinsertf128", "vextractf128", "vperm2f128", "vshufps", "vshufpd", "vunpckhps", "vunpcklps", "vunpckhpd",
            "vunpcklpd", "vzeroupper", "vzeroall",
            // AVX2 instructions
            "vbroadcasti128", "vextracti128", "vinserti128", "vperm2i128", "vpermd", "vpermps", "vpermpd", "vpermq", "vpsllvd", "vpsllvq",
            "vpsrlvd", "vpsrlvq", "vpsravd", "vgatherdps", "vgatherqps", "vgatherdpd", "vgatherqpd", "vpgatherdd", "vpgatherqd",
            "vpgatherdq", "vpgatherqq", "vpabsb", "vpabsw", "vpabsd", "vpacksswb", "vpackssdw", "vpackusdw", "vpackuswb", "vpaddb",
            "vpaddw", "vpaddd", "vpaddq", "vpaddsb", "vpaddsw", "vpaddusb", "vpaddusw", "vpalignr", "vpand", "vpandn", "vpavgb", "vpavgw",
            "vpblendvb", "vpblendw", "vpcmpeqb", "vpcmpeqw", "vpcmpeqd", "vpcmpeqq", "vpcmpgtb", "vpcmpgtw", "vpcmpgtd", "vpcmpgtq",
            "vphaddd", "vphaddw", "vphaddsw", "vphsubd", "vphsubw", "vphsubsw", "vpmaddubsw", "vpmaddwd", "vpmaxsb", "vpmaxsw", "vpmaxsd",
            "vpmaxub", "vpmaxuw", "vpmaxud", "vpminsb", "vpminsw", "vpminsd", "vpminub", "vpminuw", "vpminud", "vpmovmskb", "vpmovsxbw",
            "vpmovsxbd", "vpmovsxbq", "vpmovsxwd", "vpmovsxwq", "vpmovsxdq", "vpmovzxbw", "vpmovzxbd", "vpmovzxbq", "vpmovzxwd",
            "vpmovzxwq", "vpmovzxdq", "vpmuldq", "vpmulhrsw", "vpmulhuw", "vpmulhw", "vpmulld", "vpmullw", "vpmuludq", "vpor", "vpsadbw",
            "vpshufb", "vpshufd", "vpshufhw", "vpshuflw", "vpsignb", "vpsignw", "vpsignd", "vpslldq", "vpsllw", "vpslld", "vpsllq",
            "vpsraw", "vpsrad", "vpsrldq", "vpsrlw", "vpsrld", "vpsrlq", "vpsubb", "vpsubw", "vpsubd", "vpsubq", "vpsubsb", "vpsubsw",
            "vpsubusb", "vpsubusw", "vptest", "vpunpckhbw", "vpunpckhwd", "vpunpckhdq", "vpunpckhqdq", "vpunpcklbw", "vpunpcklwd",
            "vpunpckldq", "vpunpcklqdq", "vpxor",
            // AVX-512 Foundation instructions
            "vmovdqa32", "vmovdqa64", "vmovdqu32", "vmovdqu64", "vbroadcastf32x4", "vbroadcastf64x4", "vbroadcasti32x4", "vbroadcasti64x4",
            "vextractf32x4", "vextractf64x4", "vextracti32x4", "vextracti64x4", "vinsertf32x4", "vinsertf64x4", "vinserti32x4",
            "vinserti64x4", "vshuff32x4", "vshuff64x2", "vshufi32x4", "vshufi64x2", "vcompresspd", "vcompressps", "vpcompressd",
            "vpcompressq", "vexpandpd", "vexpandps", "vpexpandd", "vpexpandq", "kandw", "kandb", "kandq", "kandd", "korw", "korb", "korq",
            "kord", "kxorw", "kxorb", "kxorq", "kxord", "knotw", "knotb", "knotq", "knotd"};

        if (instructions.contains(word.toLower())) {
            return QColor("#4EC9B0");  // VS Code class/type teal
        }

        // Registers (VS Code variable blue) - Extended x86-64 register set
        QRegularExpression regRegex(
            "^[re]?[a-d]x$|^[re]?[sd]i$|^[re]?[sb]p$|^r[8-9]$|^r1[0-5]$|^eax$|^ebx$|^ecx$|^edx$|^esi$|^edi$|^esp$|^ebp$|^eip$|^rip$|^cs$|^"
            "ds$|^es$|^fs$|^gs$|^ss$|^mm[0-7]$|^[xy]mm[0-9]$|^[xy]mm1[0-5]$|^[xy]mm[23][0-9]$|^[xy]mm3[01]$|^zmm[0-9]$|^zmm1[0-5]$|^zmm[23]"
            "[0-9]$|^zmm3[01]$|^k[0-7]$|^st[0-7]$|^cr[0-8]$|^dr[0-7]$");
        if (regRegex.match(word.toLower()).hasMatch()) {
            return QColor("#9CDCFE");  // VS Code variable blue
        }

        // Numbers (hex and decimal) (VS Code number green)
        QRegularExpression numRegex("^\\$?0x[0-9a-fA-F]+$|^\\$?[0-9]+$");
        if (numRegex.match(word).hasMatch()) {
            return QColor("#B5CEA8");  // VS Code number green
        }

        // Memory addresses and brackets (VS Code string color)
        if (word.contains('[') || word.contains(']') || word.contains('(') || word.contains(')')) {
            return QColor("#D7BA7D");  // VS Code string orange
        }

        // Function names with offset (VS Code function yellow)
        if (word.contains('+') && word.contains("0x")) {
            return QColor("#DCDCAA");  // VS Code function yellow
        }

        // Default color (VS Code default text)
        return QColor("#D4D4D4");  // VS Code default text
    }
};

QemuLogViewer::QemuLogViewer(QWidget* parent)
    : QMainWindow(parent), currentSearchIndex(-1), preSearchPosition(-1), searchActive(false), nextStringBuffer(0) {
    disassembler = std::make_unique<CapstoneDisassembler>();

    // Initialize search debounce timer
    searchDebounceTimer = new QTimer(this);
    searchDebounceTimer->setSingleShot(true);
    searchDebounceTimer->setInterval(300);  // 300ms debounce
    connect(searchDebounceTimer, &QTimer::timeout, this, &QemuLogViewer::performDebouncedSearch);

    // Initialize performance optimizations first
    initializePerformanceOptimizations();

    // Initialize configuration service
    ConfigService::instance().initialize();

    setupDarkTheme();
    setupUI();

    // Set up syntax highlighters after UI is created
    disassemblyHighlighter = new SyntaxHighlighter(disassemblyView->document());
    detailsHighlighter = new SyntaxHighlighter(detailsPane->document());

    // Set up table syntax highlighting delegate
    tableDelegate = new SyntaxHighlightDelegate(this);
    logTable->setItemDelegate(tableDelegate);

    connectSignals();
    loadLogFiles();
    // Initialize interrupt navigation state
    currentSelectedInterrupt.clear();
    currentInterruptIndex = -1;
}

QemuLogViewer::~QemuLogViewer() {
    // Qt manages QTableWidgetItem cleanup automatically
    // LogProcessor now manages QProcess objects internally
    // No manual cleanup needed
}

auto QemuLogViewer::eventFilter(QObject* obj, QEvent* event) -> bool {
    if (obj == searchEdit && event->type() == QEvent::KeyPress) {
        auto* keyEvent = static_cast<QKeyEvent*>(event);
        if (keyEvent->key() == Qt::Key_Escape) {
            cancelSearch();
            return true;  // Event handled
        } else if (keyEvent->key() == Qt::Key_Return || keyEvent->key() == Qt::Key_Enter) {
            // Handle Enter and Shift+Enter for search navigation
            if (keyEvent->modifiers() & Qt::ShiftModifier) {
                // Shift+Enter: go to previous match
                if (!searchMatches.empty()) {
                    onSearchPrevious();
                }
            } else {
                // Enter: go to next match
                if (!searchMatches.empty()) {
                    onSearchNext();
                }
            }
            return true;  // Event handled
        }
    }
    return QMainWindow::eventFilter(obj, event);
}

void QemuLogViewer::cancelSearch() {
    if (searchActive && preSearchPosition >= 0) {
        // Return to the original position
        scrollToRow(preSearchPosition);
        searchActive = false;
        preSearchPosition = -1;

        // Clear search text and reset UI
        searchEdit->clear();
        searchMatches.clear();
        currentSearchIndex = -1;
        searchNextBtn->setEnabled(false);
        searchPrevBtn->setEnabled(false);
        highlightSearchMatches();

        statusLabel->setText("Search cancelled");
    }
}

void QemuLogViewer::setupDarkTheme() {
    // Use system theme detection
    QPalette palette = QApplication::palette();
    bool isDarkTheme = palette.color(QPalette::Window).lightness() < 128;

    if (!isDarkTheme) {
        // If system is not dark, apply our dark theme for better code viewing
        isDarkTheme = true;
    }

    if (isDarkTheme) {
        applyTheme("dark");
    }
    // If not dark theme, use default system styling
}

void QemuLogViewer::applyTheme(const QString& themeName) {
    QString styleSheet;

    if (themeName == "dark") {
        styleSheet = getDarkThemeCSS();
    } else if (themeName == "light") {
        styleSheet = getLightThemeCSS();
    } else if (themeName == "high-contrast") {
        styleSheet = getHighContrastThemeCSS();
    }

    setStyleSheet(styleSheet);
}

QString QemuLogViewer::getDarkThemeCSS() {
    return QString(R"(
        QMainWindow {
            background-color: #2b2b2b;
            color: #ffffff;
        }

        QToolBar {
            background-color: #3c3c3c;
            border: none;
            spacing: 3px;
            color: #ffffff;
        }

        QComboBox {
            background-color: #404040;
            color: #ffffff;
            border: 1px solid #555555;
            padding: 5px;
            border-radius: 3px;
            min-height: 20px;
        }

        QComboBox::drop-down {
            border: none;
            width: 20px;
        }

        QComboBox::down-arrow {
            width: 12px;
            height: 12px;
            border: none;
        }

        QComboBox QAbstractItemView {
            background-color: #404040;
            color: #ffffff;
            border: 1px solid #555555;
            selection-background-color: #1e3a5f;
        }

        QLineEdit {
            background-color: #404040;
            color: #ffffff;
            border: 1px solid #555555;
            padding: 5px;
            border-radius: 3px;
        }

        QLineEdit:focus {
            border: 1px solid #1e3a5f;
        }

        QPushButton {
            background-color: #404040;
            color: #ffffff;
            border: 1px solid #555555;
            padding: 5px 10px;
            border-radius: 3px;
            min-height: 20px;
        }

        QPushButton:hover {
            background-color: #4a4a4a;
        }

        QPushButton:pressed {
            background-color: #353535;
        }

        QPushButton:disabled {
            background-color: #2b2b2b;
            color: #666666;
            border: 1px solid #444444;
        }

        QCheckBox {
            color: #ffffff;
            spacing: 5px;
        }

        QCheckBox::indicator {
            width: 16px;
            height: 16px;
            background-color: #404040;
            border: 1px solid #555555;
            border-radius: 3px;
        }

        QCheckBox::indicator:checked {
            background-color: #1e3a5f;
            border: 1px solid #1e3a5f;
        }

        QTableWidget {
            background-color: #1a1a1a;
            alternate-background-color: #252525;
            color: #e8e8e8;
            gridline-color: #555555;
            selection-background-color: #1e3a5f;
            selection-color: #ffffff;
            border: 1px solid #666666;
        }

        QTableWidget::item {
            padding: 6px;
            border-bottom: 1px solid #404040;
        }

        QTableWidget::item:selected {
            background-color: #1e3a5f;
            color: #ffffff;
        }

        QHeaderView::section {
            background-color: #3c3c3c;
            color: #ffffff;
            padding: 6px;
            border: 1px solid #555555;
            font-weight: bold;
        }

        QHeaderView::section:hover {
            background-color: #4a4a4a;
        }

        QTextEdit {
            background-color: #1a1a1a;
            color: #e8e8e8;
            border: 1px solid #666666;
            selection-background-color: #1e3a5f;
            selection-color: #ffffff;
            font-family: "Consolas", "Monaco", "Courier New", monospace;
        }

        QSplitter::handle {
            background-color: #555555;
        }

        QSplitter::handle:horizontal {
            width: 3px;
        }

        QSplitter::handle:vertical {
            height: 3px;
        }

        QSplitter::handle:hover {
            background-color: #666666;
        }

        QProgressBar {
            background-color: #404040;
            border: 1px solid #555555;
            border-radius: 3px;
            text-align: center;
            color: #ffffff;
            min-height: 20px;
            font-weight: bold;
        }

        QProgressBar::chunk {
            background-color: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                stop:0 #1e3a5f, stop:0.5 #2a4a7a, stop:1 #1e3a5f);
            border-radius: 3px;
            margin: 1px;
        }

        QLabel {
            color: #ffffff;
        }

        QScrollBar:vertical {
            background-color: #3c3c3c;
            width: 12px;
            border: none;
            border-radius: 6px;
        }

        QScrollBar::handle:vertical {
            background-color: #555555;
            border-radius: 6px;
            min-height: 20px;
            margin: 2px;
        }

        QScrollBar::handle:vertical:hover {
            background-color: #666666;
        }

        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
            border: none;
            background: none;
            height: 0;
        }

        QScrollBar:horizontal {
            background-color: #3c3c3c;
            height: 12px;
            border: none;
            border-radius: 6px;
        }

        QScrollBar::handle:horizontal {
            background-color: #555555;
            border-radius: 6px;
            min-width: 20px;
            margin: 2px;
        }

        QScrollBar::handle:horizontal:hover {
            background-color: #666666;
        }

        QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {
            border: none;
            background: none;
            width: 0;
        }
    )");
}

QString QemuLogViewer::getLightThemeCSS() {
    return QString(R"(
        QMainWindow {
            background-color: #ffffff;
            color: #333333;
        }

        QToolBar {
            background-color: #e8e8e8;
            border: none;
            spacing: 3px;
            color: #333333;
        }

        QTableWidget {
            background-color: #ffffff;
            alternate-background-color: #f5f5f5;
            color: #000000;
            gridline-color: #dddddd;
            selection-background-color: #0078d4;
            selection-color: #ffffff;
            border: 1px solid #cccccc;
        }

        QTextEdit {
            background-color: #ffffff;
            color: #000000;
            border: 1px solid #cccccc;
            selection-background-color: #0078d4;
            selection-color: #ffffff;
            font-family: "Consolas", "Monaco", "Courier New", monospace;
        }
    )");
}

QString QemuLogViewer::getHighContrastThemeCSS() {
    return QString(R"(
        QMainWindow {
            background-color: #000000;
            color: #ffffff;
        }

        QToolBar {
            background-color: #1a1a1a;
            border: none;
            spacing: 3px;
            color: #ffffff;
        }

        QTableWidget {
            background-color: #000000;
            alternate-background-color: #111111;
            color: #ffffff;
            gridline-color: #888888;
            selection-background-color: #00ff00;
            selection-color: #000000;
            border: 1px solid #ffffff;
        }

        QTextEdit {
            background-color: #000000;
            color: #ffffff;
            border: 1px solid #ffffff;
            selection-background-color: #00ff00;
            selection-color: #000000;
            font-family: "Consolas", "Monaco", "Courier New", monospace;
        }
    )");
}

void QemuLogViewer::setupUI() {
    setWindowTitle("QEMU Log Viewer - WOS Kernel Debugger");
    setMinimumSize(1200, 800);
    resize(1600, 1000);

    setupToolbar();
    setupMainContent();
}

void QemuLogViewer::setupToolbar() {
    toolbar = addToolBar("Main");
    toolbar->setMovable(false);

    // File selector
    toolbar->addWidget(new QLabel("Log File:"));
    fileSelector = new QComboBox();
    fileSelector->setMinimumWidth(200);
    toolbar->addWidget(fileSelector);

    toolbar->addSeparator();

    // Search controls
    toolbar->addWidget(new QLabel("Search:"));
    searchEdit = new QLineEdit();
    searchEdit->setPlaceholderText("Search addresses, functions, assembly... (Enter: next, Shift+Enter: prev, Esc: cancel)");
    searchEdit->setMinimumWidth(300);
    toolbar->addWidget(searchEdit);

    regexCheckbox = new QCheckBox("Regex");
    toolbar->addWidget(regexCheckbox);

    hideStructuralCheckbox = new QCheckBox("Hide Structural");
    hideStructuralCheckbox->setToolTip("Hide SEPARATOR and BLOCK entries");
    hideStructuralCheckbox->setChecked(true);  // Enabled by default
    toolbar->addWidget(hideStructuralCheckbox);

    searchPrevBtn = new QPushButton("◀");
    searchPrevBtn->setToolTip("Previous match (Shift+Enter)");
    searchPrevBtn->setEnabled(false);
    toolbar->addWidget(searchPrevBtn);

    searchNextBtn = new QPushButton("▶");
    searchNextBtn->setToolTip("Next match (Enter)");
    searchNextBtn->setEnabled(false);
    toolbar->addWidget(searchNextBtn);

    toolbar->addSeparator();

    // Navigation
    toolbar->addWidget(new QLabel("Go to:"));
    navigationEdit = new QLineEdit();
    navigationEdit->setPlaceholderText("Address (0x...) or Line number");
    navigationEdit->setMinimumWidth(200);
    toolbar->addWidget(navigationEdit);

    toolbar->addSeparator();

    // Interrupt filter dropdown
    toolbar->addWidget(new QLabel("Interrupts:"));
    interruptFilterCombo = new QComboBox();
    interruptFilterCombo->setMinimumWidth(200);
    interruptFilterCombo->setToolTip("Filter interrupts by number (shows only present interrupts)");
    toolbar->addWidget(interruptFilterCombo);
    // Navigation buttons for interrupt occurrences
    interruptPrevBtn = new QPushButton("◀");
    interruptPrevBtn->setToolTip("Previous interrupt occurrence");
    interruptPrevBtn->setEnabled(false);
    toolbar->addWidget(interruptPrevBtn);

    interruptNextBtn = new QPushButton("▶");
    interruptNextBtn->setToolTip("Next interrupt occurrence");
    interruptNextBtn->setEnabled(false);
    toolbar->addWidget(interruptNextBtn);

    onlyInterruptsCheckbox = new QCheckBox("Only interrupts");
    onlyInterruptsCheckbox->setToolTip("When checked, table shows only interrupt entries");
    toolbar->addWidget(onlyInterruptsCheckbox);

    toolbar->addSeparator();

    // Status
    progressBar = new QProgressBar();
    progressBar->setVisible(false);
    progressBar->setMaximumWidth(200);
    toolbar->addWidget(progressBar);

    statusLabel = new QLabel("Ready");
    toolbar->addWidget(statusLabel);
}

void QemuLogViewer::setupMainContent() {
    auto centralWidget = new QWidget();
    setCentralWidget(centralWidget);

    auto layout = new QVBoxLayout(centralWidget);
    layout->setContentsMargins(5, 5, 5, 5);

    // Create main splitter
    mainSplitter = new QSplitter(Qt::Horizontal);
    layout->addWidget(mainSplitter);

    // Left: interrupts panel
    interruptsPanel = new QTreeWidget();
    interruptsPanel->setHeaderLabels(QStringList() << "Interrupt" << "Occurrences");
    interruptsPanel->setMinimumWidth(260);
    interruptsPanel->setSelectionMode(QAbstractItemView::SingleSelection);

    setupTable();

    // Right side - hex and disassembly views
    auto rightSplitter = new QSplitter(Qt::Vertical);

    // Hex view
    hexView = new QTextEdit();
    hexView->setReadOnly(true);
    hexView->setFont(QFont("Consolas", 12));  // Increased from 10 to 12
    hexView->setMaximumHeight(200);
    hexView->setPlaceholderText("Hex bytes will be displayed here...");
    rightSplitter->addWidget(hexView);

    // Disassembly view
    disassemblyView = new QTextEdit();
    disassemblyView->setReadOnly(true);
    disassemblyView->setFont(QFont("Consolas", 12));  // Increased from 10 to 12
    disassemblyView->setPlaceholderText("Detailed disassembly will be displayed here...");
    rightSplitter->addWidget(disassemblyView);

    // Details pane for interrupt information
    detailsPane = new QTextBrowser();
    detailsPane->setReadOnly(true);
    detailsPane->setFont(QFont("Consolas", 12));  // Increased from 10 to 12
    detailsPane->setPlaceholderText("Interrupt details will be displayed here...");
    rightSplitter->addWidget(detailsPane);

    rightSplitter->setSizes({100, 200, 200});

    mainSplitter->addWidget(interruptsPanel);
    mainSplitter->addWidget(logTable);
    mainSplitter->addWidget(rightSplitter);
    mainSplitter->setSizes({240, 800, 400});
}

void QemuLogViewer::setupTable() {
    // Create virtual table view
    logTable = new VirtualTableView(nullptr);

    // Create virtual model
    QStringList headers = {"Line", "Type", "Address", "Function", "Hex Bytes", "Assembly"};
    virtualTableModel = new VirtualTableModel(0, headers, logTable);  // 0 rows initially

    virtualTableModel->setDataProvider([this](int row, std::vector<QString>& cells, QColor& bgColor) {
        if (row < 0 || row >= static_cast<int>(visibleEntryPointers.size())) {
            cells.clear();
            bgColor = Qt::transparent;
            return;
        }

        const LogEntry* entry = visibleEntryPointers[row];
        if (!entry) {
            cells.clear();
            bgColor = Qt::transparent;
            return;
        }

        // Format cell data
        cells.clear();
        cells.reserve(6);

        cells.push_back(QString::number(entry->lineNumber));

        QString typeStr;
        switch (entry->type) {
            case EntryType::INSTRUCTION:
                typeStr = "INSTRUCTION";
                break;
            case EntryType::INTERRUPT:
                typeStr = "INTERRUPT";
                break;
            case EntryType::REGISTER:
                typeStr = "REGISTER";
                break;
            case EntryType::BLOCK:
                typeStr = "BLOCK";
                break;
            case EntryType::SEPARATOR:
                typeStr = "SEPARATOR";
                break;
            case EntryType::OTHER:
                typeStr = "OTHER";
                break;
        }
        cells.push_back(typeStr);
        cells.push_back(formatAddress(entry->address));
        cells.push_back(formatFunction(entry->function));
        cells.push_back(formatHexBytes(entry->hexBytes));
        cells.push_back(formatAssembly(entry->assembly));

        // Set background color based on type
        bgColor = getEntryTypeColor(entry->type);
    });

    logTable->setVirtualModel(virtualTableModel);

    // Configure appearance
    logTable->horizontalHeader()->resizeSection(0, 60);         // Line
    logTable->horizontalHeader()->resizeSection(1, 80);         // Type
    logTable->horizontalHeader()->resizeSection(2, 120);        // Address
    logTable->horizontalHeader()->resizeSection(3, 200);        // Function
    logTable->horizontalHeader()->resizeSection(4, 140);        // Hex Bytes
    logTable->horizontalHeader()->setStretchLastSection(true);  // Assembly

    logTable->setFont(QFont("Consolas", 11));
    logTable->verticalHeader()->setDefaultSectionSize(24);
    logTable->verticalHeader()->hide();
    logTable->setMouseTracking(true);
}

void QemuLogViewer::connectSignals() {
    connect(fileSelector, QOverload<const QString&>::of(&QComboBox::currentTextChanged), this, &QemuLogViewer::onFileSelected);

    connect(searchEdit, &QLineEdit::textChanged, this, &QemuLogViewer::onSearchTextChanged);

    // Install event filter for Esc key handling
    searchEdit->installEventFilter(this);

    connect(searchNextBtn, &QPushButton::clicked, this, &QemuLogViewer::onSearchNext);

    connect(searchPrevBtn, &QPushButton::clicked, this, &QemuLogViewer::onSearchPrevious);

    connect(regexCheckbox, &QCheckBox::toggled, this, &QemuLogViewer::onRegexToggled);

    connect(hideStructuralCheckbox, &QCheckBox::toggled, this, &QemuLogViewer::onHideStructuralToggled);

    connect(navigationEdit, &QLineEdit::returnPressed, [this]() {
        QString text = navigationEdit->text().trimmed();
        if (text.isEmpty()) return;

        if (isAddressInput(text)) {
            bool ok;
            uint64_t addr = text.toULongLong(&ok, 16);
            if (ok) jumpToAddress(addr);
        } else {
            bool ok;
            int line = text.toInt(&ok);
            if (ok && line > 0) jumpToLine(line);
        }
    });

    // Virtual table selection handling
    connect(logTable->selectionModel(), &QItemSelectionModel::selectionChanged, this,
            [this](const QItemSelection& selected, const QItemSelection&) {
                if (!selected.isEmpty()) {
                    int row = selected.first().top();
                    updateDetailsPane(row);
                }
            });

    // Virtual table click handler
    connect(logTable, &VirtualTableView::clicked, this, [this](const QModelIndex& index) {
        if (index.isValid()) {
            updateDetailsPane(index.row());
        }
    });

    // Sync scroll bars
    connect(logTable->verticalScrollBar(), &QScrollBar::valueChanged, this, &QemuLogViewer::syncScrollBars);

    // Handle VSCode link clicks
    connect(detailsPane, &QTextBrowser::anchorClicked, this, &QemuLogViewer::onDetailsPaneLinkClicked);

    // Interrupt filter changed
    connect(interruptFilterCombo, &QComboBox::currentTextChanged, this, &QemuLogViewer::onInterruptFilterChanged);
    connect(interruptNextBtn, &QPushButton::clicked, this, &QemuLogViewer::onInterruptNext);
    connect(interruptPrevBtn, &QPushButton::clicked, this, &QemuLogViewer::onInterruptPrevious);
    connect(onlyInterruptsCheckbox, &QCheckBox::toggled, this,
            &QemuLogViewer::onHideStructuralToggled);  // reuse hideStructural logic for simplicity
    connect(interruptsPanel, &QTreeWidget::itemActivated, this, &QemuLogViewer::onInterruptPanelActivated);

    // Right-click to fold/unfold interrupts
    connect(interruptsPanel, &QTreeWidget::customContextMenuRequested, [this](const QPoint& pos) {
        QTreeWidgetItem* item = interruptsPanel->itemAt(pos);
        if (item) {
            onInterruptToggleFold(item, 0);
        }
    });
    interruptsPanel->setContextMenuPolicy(Qt::CustomContextMenu);
}

void QemuLogViewer::loadLogFiles() {
    fileSelector->clear();

    QDir dir(".");
    QStringList filters;
    filters << "*.log";

    auto files = dir.entryList(filters, QDir::Files, QDir::Name);

    if (files.isEmpty()) {
        statusLabel->setText("No log files found in current directory");
        return;
    }

    // Prioritize .modified.log files
    std::sort(files.begin(), files.end(), [](const QString& a, const QString& b) {
        bool aModified = a.contains(".modified.");
        bool bModified = b.contains(".modified.");
        if (aModified != bModified) {
            return aModified > bModified;
        }
        return a < b;
    });

    fileSelector->addItems(files);

    // Show config status in the status label
    const Config& config = ConfigService::instance().getConfig();
    const auto& lookups = config.getAddressLookups();
    bool configExists = ConfigService::instance().configFileExists();

    QString statusText = QString("Found %1 log files").arg(files.size());
    if (configExists) {
        statusText += QString(" • Config: %1 symbol lookups loaded").arg(lookups.size());
    } else {
        statusText += QString(" • Config: Using defaults (%1 lookups)").arg(lookups.size());
    }

    statusLabel->setText(statusText);
}

void QemuLogViewer::onFileSelected(const QString& filename) {
    if (filename.isEmpty()) {
        return;
    }

    // Disable UI elements during processing
    fileSelector->setEnabled(false);
    searchEdit->setEnabled(false);
    navigationEdit->setEnabled(false);
    logTable->setEnabled(false);

    statusLabel->setText("Opening file...");
    progressBar->setVisible(true);
    progressBar->setValue(0);
    progressBar->setFormat("Opening file... %p%");

    // Clear current data
    // For virtual table: reset model row count to 0
    if (virtualTableModel) {
        virtualTableModel->resetModel();
    }
    logEntries.clear();
    visibleEntryPointers.clear();
    searchMatches.clear();
    currentSearchIndex = -1;

    // Clear views
    hexView->clear();
    disassemblyView->clear();
    detailsPane->clear();

    // Start processing in background
    processor = std::make_unique<LogProcessor>(filename, this);
    connect(processor.get(), &LogProcessor::progressUpdate, this, &QemuLogViewer::onProgressUpdate);
    connect(processor.get(), &LogProcessor::processingComplete, this, &QemuLogViewer::onProcessingComplete);
    connect(processor.get(), &LogProcessor::errorOccurred, [this](const QString& error) {
        QMessageBox::critical(this, "Error", error);
        progressBar->setVisible(false);
        progressBar->setFormat("");
        statusLabel->setText("Error occurred");

        // Re-enable UI
        fileSelector->setEnabled(true);
        searchEdit->setEnabled(true);
        navigationEdit->setEnabled(true);
        logTable->setEnabled(true);
    });

    processor->startProcessing();
}

void QemuLogViewer::onProgressUpdate(int percentage) {
    progressBar->setValue(percentage);

    // Update progress bar text based on percentage ranges
    if (percentage < 10) {
        progressBar->setFormat("Reading file... %p%");
        statusLabel->setText("Reading log file...");
    } else if (percentage < 50) {
        progressBar->setFormat("Parsing entries... %p%");
        statusLabel->setText("Parsing log entries...");
    } else if (percentage < 90) {
        progressBar->setFormat("Processing data... %p%");
        statusLabel->setText("Processing log data...");
    } else {
        progressBar->setFormat("Finalizing... %p%");
        statusLabel->setText("Finalizing...");
    }

    // Force GUI update
    QApplication::processEvents();
}

void QemuLogViewer::onProcessingComplete() {
    if (!processor) return;

    logEntries = processor->getEntries();

    // Show progress for optimization phase
    progressBar->setFormat("Building lookup maps... %p%");
    statusLabel->setText("Building lookup maps...");
    progressBar->setValue(92);
    QApplication::processEvents();

    // Build fast lookup maps
    buildLookupMaps();

    // Show progress for table population
    progressBar->setFormat("Populating table... %p%");
    statusLabel->setText("Populating table...");
    progressBar->setValue(95);
    QApplication::processEvents();

    // Use optimized table population
    populateTable();

    // Build searchable rows after table is populated
    progressBar->setFormat("Building search index... %p%");
    statusLabel->setText("Building search index...");
    progressBar->setValue(98);
    QApplication::processEvents();

    buildSearchableRows();

    progressBar->setValue(100);
    progressBar->setFormat("Complete");
    statusLabel->setText(QString("Loaded %1 entries").arg(logEntries.size()));

    // Hide progress bar after a short delay
    QTimer::singleShot(1000, [this]() {
        progressBar->setVisible(false);
        progressBar->setFormat("");
    });

    // Re-enable UI elements
    fileSelector->setEnabled(true);
    searchEdit->setEnabled(true);
    navigationEdit->setEnabled(true);
    logTable->setEnabled(true);

    // Populate interrupt filter dropdown
    populateInterruptFilter();
    // Populate the left interrupts panel for fast navigation
    buildInterruptPanel();

    // Re-run search if there was one
    if (!searchEdit->text().isEmpty()) {
        performSearchOptimized();
    }
}

// Populate the interrupt filter dropdown with unique interrupt numbers present
void QemuLogViewer::populateInterruptFilter() {
    if (!interruptFilterCombo) return;
    interruptFilterCombo->blockSignals(true);
    interruptFilterCombo->clear();
    std::vector<QString> interrupts;
    std::unordered_set<std::string> seen;

    for (const auto& entry : logEntries) {
        if (entry.type == EntryType::INTERRUPT) {
            if (entry.interruptNumber.empty()) continue;
            if (seen.insert(entry.interruptNumber).second) {
                interrupts.push_back(QString::fromStdString(entry.interruptNumber));
            }
        }
    }

    // Common x86 exception names for nicer display
    std::unordered_map<int, std::string> irqNames{{0x0, "Divide Error"},
                                                  {0x1, "Debug"},
                                                  {0x2, "NMI"},
                                                  {0x3, "Breakpoint"},
                                                  {0x4, "Overflow"},
                                                  {0x5, "BOUND Range Exceeded"},
                                                  {0x6, "Invalid Opcode"},
                                                  {0x7, "Device Not Available"},
                                                  {0x8, "Double Fault"},
                                                  {0x9, "Coprocessor Segment Overrun"},
                                                  {0xa, "Invalid TSS"},
                                                  {0xb, "Segment Not Present"},
                                                  {0xc, "Stack-Segment Fault"},
                                                  {0xd, "General Protection Fault"},
                                                  {0xe, "Page Fault"},
                                                  {0x10, "x87 FPU Floating-Point Error"},
                                                  {0x11, "Alignment Check"},
                                                  {0x12, "Machine Check"},
                                                  {0x13, "SIMD Floating-Point Exception"}};

    interruptFilterCombo->addItem("All");
    for (const auto& s : interrupts) {
        bool ok;
        int val = s.toInt(&ok, 16);
        QString display;
        if (ok) {
            auto it = irqNames.find(val);
            if (it != irqNames.end()) {
                display = QString("0x%1 - %2").arg(s).arg(QString::fromStdString(it->second));
            } else {
                display = QString("0x%1").arg(s);
            }
        } else {
            display = s;
        }
        // Store raw number as userData for matching
        interruptFilterCombo->addItem(display, QVariant(s));
    }

    interruptFilterCombo->setEnabled(!interrupts.empty());
    // Reset navigation state
    currentSelectedInterrupt.clear();
    currentInterruptIndex = -1;
    interruptPrevBtn->setEnabled(false);
    interruptNextBtn->setEnabled(false);
    interruptFilterCombo->blockSignals(false);
}

// Slot fired when the interrupt filter selection changes
void QemuLogViewer::onInterruptFilterChanged(const QString& text) {
    if (text.isEmpty() || text == "All") {
        populateTable();
        currentSelectedInterrupt.clear();
        currentInterruptIndex = -1;
        interruptPrevBtn->setEnabled(false);
        interruptNextBtn->setEnabled(false);
        return;
    }
    // Extract raw interrupt number from item userData if available
    QVariant data = interruptFilterCombo->currentData();
    QString raw;
    if (data.isValid() && data.canConvert<QString>()) raw = data.toString();
    if (raw.isEmpty()) raw = text;  // fallback

    currentSelectedInterrupt = raw.toStdString();

    // Ensure table is populated according to current hideStructural/onlyInterrupts state
    populateTable();

    // Build list of visible rows containing this interrupt
    std::vector<int> rows;
    for (size_t row = 0; row < visibleEntryPointers.size(); ++row) {
        const LogEntry* e = visibleEntryPointers[row];
        if (e && e->type == EntryType::INTERRUPT && e->interruptNumber == currentSelectedInterrupt) {
            rows.push_back(static_cast<int>(row));
        }
    }

    if (rows.empty()) {
        statusLabel->setText("Selected interrupt not visible (may be hidden by filters)");
        currentInterruptIndex = -1;
        interruptPrevBtn->setEnabled(false);
        interruptNextBtn->setEnabled(false);
        return;
    }

    // Jump to first occurrence
    currentInterruptIndex = 0;
    int targetRow = rows[currentInterruptIndex];
    logTable->selectRow(targetRow);
    scrollToRow(targetRow);
    updateDetailsPane(targetRow);
    statusLabel->setText(QString("Jumped to interrupt %1 (occurrence %2 of %3)").arg(text).arg(currentInterruptIndex + 1).arg(rows.size()));

    // Enable navigation buttons if multiple occurrences
    interruptPrevBtn->setEnabled(rows.size() > 1);
    interruptNextBtn->setEnabled(rows.size() > 1);
}

// Move to next occurrence of currently selected interrupt
void QemuLogViewer::onInterruptNext() {
    if (currentSelectedInterrupt.empty()) return;

    // Rebuild visible rows for current interrupt
    std::vector<int> rows;
    for (size_t row = 0; row < visibleEntryPointers.size(); ++row) {
        const LogEntry* e = visibleEntryPointers[row];
        if (e && e->type == EntryType::INTERRUPT && e->interruptNumber == currentSelectedInterrupt) {
            rows.push_back(static_cast<int>(row));
        }
    }
    if (rows.empty()) return;

    currentInterruptIndex = (currentInterruptIndex + 1) % static_cast<int>(rows.size());
    int targetRow = rows[currentInterruptIndex];
    logTable->selectRow(targetRow);
    scrollToRow(targetRow);
    updateDetailsPane(targetRow);
    statusLabel->setText(QString("Jumped to interrupt (occurrence %1 of %2)").arg(currentInterruptIndex + 1).arg(rows.size()));
}

// Move to previous occurrence of currently selected interrupt
void QemuLogViewer::onInterruptPrevious() {
    if (currentSelectedInterrupt.empty()) return;

    std::vector<int> rows;
    for (size_t row = 0; row < visibleEntryPointers.size(); ++row) {
        const LogEntry* e = visibleEntryPointers[row];
        if (e && e->type == EntryType::INTERRUPT && e->interruptNumber == currentSelectedInterrupt) {
            rows.push_back(static_cast<int>(row));
        }
    }
    if (rows.empty()) return;

    currentInterruptIndex = (currentInterruptIndex - 1 + static_cast<int>(rows.size())) % static_cast<int>(rows.size());
    int targetRow = rows[currentInterruptIndex];
    logTable->selectRow(targetRow);
    scrollToRow(targetRow);
    updateDetailsPane(targetRow);
    statusLabel->setText(QString("Jumped to interrupt (occurrence %1 of %2)").arg(currentInterruptIndex + 1).arg(rows.size()));
}

void QemuLogViewer::onSearchTextChanged() {
    // Cancel any pending search
    searchDebounceTimer->stop();

    // Start debounce timer
    searchDebounceTimer->start();
}

void QemuLogViewer::performDebouncedSearch() { performSearchOptimized(); }

void QemuLogViewer::performSearch() {
    searchMatches.clear();
    currentSearchIndex = -1;

    QString searchText = searchEdit->text().trimmed();
    if (searchText.isEmpty()) {
        searchNextBtn->setEnabled(false);
        searchPrevBtn->setEnabled(false);
        highlightSearchMatches();
        return;
    }

    // Create search pattern
    if (regexCheckbox->isChecked()) {
        searchRegex = QRegularExpression(searchText, QRegularExpression::CaseInsensitiveOption);
        if (!searchRegex.isValid()) {
            statusLabel->setText("Invalid regex pattern");
            return;
        }
    } else {
        // Camel case matching with case insensitive fallback
        QString pattern = searchText;
        // Escape special regex characters
        pattern.replace(QRegularExpression("([\\^\\$\\*\\+\\?\\{\\}\\[\\]\\(\\)\\|\\\\])"), "\\\\1");
        searchRegex = QRegularExpression(pattern, QRegularExpression::CaseInsensitiveOption);
    }

    // Search through searchable rows (already built from visible entries)
    for (int row = 0; row < static_cast<int>(searchableRows.size()); ++row) {
        if (searchRegex.match(searchableRows[row].combinedText).hasMatch()) {
            searchMatches.push_back(searchableRows[row].originalRowIndex);
        }
    }

    bool hasMatches = !searchMatches.empty();
    searchNextBtn->setEnabled(hasMatches);
    searchPrevBtn->setEnabled(hasMatches);

    if (hasMatches) {
        currentSearchIndex = 0;
        statusLabel->setText(QString("Found %1 matches").arg(searchMatches.size()));
        highlightSearchMatches();
        scrollToRow(searchMatches[0]);
    } else {
        statusLabel->setText("No matches found");
        highlightSearchMatches();
    }
}

void QemuLogViewer::onSearchNext() {
    if (searchMatches.empty()) return;

    currentSearchIndex = (currentSearchIndex + 1) % searchMatches.size();
    highlightSearchMatches();  // Update highlighting for new current match
    scrollToRowForSearch(searchMatches[currentSearchIndex]);
    statusLabel->setText(QString("Match %1 of %2").arg(currentSearchIndex + 1).arg(searchMatches.size()));
}

void QemuLogViewer::onSearchPrevious() {
    if (searchMatches.empty()) return;

    currentSearchIndex = (currentSearchIndex - 1 + searchMatches.size()) % searchMatches.size();
    highlightSearchMatches();  // Update highlighting for new current match
    scrollToRowForSearch(searchMatches[currentSearchIndex]);
    statusLabel->setText(QString("Match %1 of %2").arg(currentSearchIndex + 1).arg(searchMatches.size()));
}

void QemuLogViewer::onRegexToggled(bool enabled) {
    Q_UNUSED(enabled)
    if (!searchEdit->text().isEmpty()) {
        performDebouncedSearch();
    }
}

void QemuLogViewer::onHideStructuralToggled(bool enabled) {
    Q_UNUSED(enabled)
    populateTable();

    // Re-run search if there was one
    if (!searchEdit->text().isEmpty()) {
        performDebouncedSearch();
    }
}

void QemuLogViewer::onNavigationTextChanged() {
    // This method can be used for real-time navigation validation
    // For now, we handle navigation on Enter key press in connectSignals()
}

void QemuLogViewer::highlightSearchMatches() {
    // With virtual tables, highlighting is handled through the model/delegate
    // The visual highlighting happens dynamically as rows are rendered
    // Re-invalidate visible rows to trigger refresh with highlight updates
    if (logTable && virtualTableModel) {
        virtualTableModel->invalidateRows(0, logTable->model()->rowCount() - 1);
    }
}

void QemuLogViewer::jumpToAddress(uint64_t address) {
    // Use fast lookup map
    auto it = addressToEntryMap.find(address);
    if (it == addressToEntryMap.end()) {
        statusLabel->setText("Address not found");
        return;
    }

    const LogEntry* targetEntry = &logEntries[it->second];

    // Find the corresponding visible row
    for (size_t i = 0; i < visibleEntryPointers.size(); ++i) {
        if (visibleEntryPointers[i] == targetEntry) {
            scrollToRow(static_cast<int>(i));
            statusLabel->setText(QString("Jumped to address 0x%1").arg(address, 0, 16));
            return;
        }
    }

    statusLabel->setText("Address not visible (may be hidden)");
}

void QemuLogViewer::jumpToLine(int lineNumber) {
    // Use fast lookup map
    auto it = lineToEntryMap.find(lineNumber);
    if (it == lineToEntryMap.end()) {
        // Find the closest line >= lineNumber
        const LogEntry* targetEntry = nullptr;
        for (const auto& entry : logEntries) {
            if (entry.lineNumber >= lineNumber) {
                targetEntry = &entry;
                break;
            }
        }

        if (!targetEntry) {
            statusLabel->setText("Line not found");
            return;
        }

        // Find the corresponding visible row
        for (size_t i = 0; i < visibleEntryPointers.size(); ++i) {
            if (visibleEntryPointers[i] == targetEntry) {
                scrollToRow(static_cast<int>(i));
                statusLabel->setText(QString("Jumped to line %1").arg(lineNumber));
                return;
            }
        }

        statusLabel->setText("Line not visible (may be hidden)");
        return;
    }

    const LogEntry* targetEntry = &logEntries[it->second];

    // Find the corresponding visible row
    for (size_t i = 0; i < visibleEntryPointers.size(); ++i) {
        if (visibleEntryPointers[i] == targetEntry) {
            scrollToRow(static_cast<int>(i));
            statusLabel->setText(QString("Jumped to line %1").arg(lineNumber));
            return;
        }
    }

    statusLabel->setText("Line not visible (may be hidden)");
}

void QemuLogViewer::scrollToRow(int row) {
    if (row >= 0 && row < static_cast<int>(visibleEntryPointers.size())) {
        if (logTable) {
            logTable->scrollToLogicalRow(row, QAbstractItemView::PositionAtCenter);
            logTable->selectRow(row);
        }
    }
}

void QemuLogViewer::scrollToRowForSearch(int row) {
    if (row >= 0 && row < static_cast<int>(visibleEntryPointers.size())) {
        if (logTable) {
            // For virtual table, use scrollToLogicalRow which handles loading
            logTable->scrollToLogicalRow(row, QAbstractItemView::PositionAtCenter);
            // Select the row
            logTable->selectRow(row);
            updateDetailsPane(row);
        }
    }
}

void QemuLogViewer::onTableSelectionChanged() {
    auto selectedRows = logTable->selectionModel()->selectedRows();
    if (selectedRows.isEmpty()) {
        detailsPane->clear();
        return;
    }

    int row = selectedRows.first().row();
    if (row >= 0 && row < static_cast<int>(visibleEntryPointers.size())) {
        const LogEntry* entry = visibleEntryPointers[row];
        if (entry) {
            updateHexView(*entry);
            updateDisassemblyView(*entry);
            updateDetailsPane(row);
        }
    }
}

void QemuLogViewer::updateHexView(const LogEntry& entry) {
    QString hexText;

    if (!entry.hexBytes.empty()) {
        std::istringstream iss(entry.hexBytes);
        std::string byte;
        int pos = 0;

        hexText += QString("Address: %1\n").arg(QString::fromStdString(entry.address));
        hexText += "Hex Bytes:\n";

        while (iss >> byte && byte.length() == 2) {
            if (pos % 16 == 0) {
                hexText += QString("%1: ").arg(pos, 4, 16, QChar('0')).toUpper();
            }

            hexText += QString("%1 ").arg(QString::fromStdString(byte)).toUpper();

            if (pos % 16 == 15) {
                hexText += "\n";
            }
            pos++;
        }

        if (pos % 16 != 0) {
            hexText += "\n";
        }
    }

    hexView->setPlainText(hexText);
}

void QemuLogViewer::updateDisassemblyView(const LogEntry& entry) {
    QString disText;

    disText += QString("Line %1: %2\n\n").arg(entry.lineNumber).arg(QString::fromStdString(entry.originalLine));

    if (!entry.address.empty()) {
        disText += QString("Address: %1\n").arg(QString::fromStdString(entry.address));
    }

    if (!entry.function.empty()) {
        disText += QString("Function: %1\n").arg(QString::fromStdString(entry.function));
    }

    if (!entry.assembly.empty()) {
        disText += QString("Assembly: %1\n").arg(QString::fromStdString(entry.assembly));
    }

    disassemblyView->setPlainText(disText);
}

void QemuLogViewer::updateDetailsPane(int row) {
    if (row < 0 || row >= static_cast<int>(visibleEntryPointers.size())) {
        detailsPane->clear();
        return;
    }

    const LogEntry* entry = visibleEntryPointers[row];
    if (!entry) {
        detailsPane->clear();
        return;
    }

    QString detailsText;

    // Show basic entry information
    detailsText += QString("=== Entry Details ===\n");
    detailsText += QString("Line: %1\n").arg(entry->lineNumber);
    detailsText += QString("Type: %1\n")
                       .arg(entry->type == EntryType::INSTRUCTION ? "INSTRUCTION"
                            : entry->type == EntryType::INTERRUPT ? "INTERRUPT"
                            : entry->type == EntryType::REGISTER  ? "REGISTER"
                            : entry->type == EntryType::BLOCK     ? "BLOCK"
                            : entry->type == EntryType::SEPARATOR ? "SEPARATOR"
                                                                  : "OTHER");

    if (!entry->address.empty()) {
        detailsText += QString("Address: %1\n").arg(QString::fromStdString(entry->address));

        // Try to find symbol information using the config
        if (entry->addressValue != 0) {
            const Config& config = ConfigService::instance().getConfig();
            QString symbolFilePath = config.findSymbolFileForAddress(entry->addressValue);
            if (!symbolFilePath.isEmpty()) {
                detailsText += QString("Symbol File: %1\n").arg(symbolFilePath);

                // TODO: In the future, we could add actual symbol lookup using binutils/objdump
                // For now, just show that a symbol file mapping exists
                detailsText += QString("Symbol Lookup: Available (mapping found)\n");
            } else {
                detailsText += QString("Symbol Lookup: No mapping found for this address range\n");
            }
        }
    }

    if (!entry->function.empty()) {
        // Display function name without offset
        QString funcName = formatFunction(entry->function);
        detailsText += QString("Function: %1\n").arg(funcName);

        // Try to extract source code location from the function string
        QString fileInfo = extractFileInfo(entry->function);
        if (!fileInfo.isEmpty()) {
            detailsText += QString("Source: %1\n").arg(fileInfo);
        }
    }

    if (!entry->assembly.empty()) {
        QString intelAssembly = formatAssembly(entry->assembly);
        detailsText += QString("Assembly: %1\n").arg(intelAssembly);
    }

    // For REG entries, show the full original line which contains CPU state details
    if (entry->type == EntryType::REGISTER && !entry->originalLine.empty()) {
        detailsText += QString("CPU State: %1\n").arg(QString::fromStdString(entry->originalLine));
    }

    detailsText += QString("\n");

    // Try to get source code for INSTRUCTION entries
    if (entry->type == EntryType::INSTRUCTION && entry->addressValue != 0) {
        const Config& config = ConfigService::instance().getConfig();
        QString symbolFilePath = config.findSymbolFileForAddress(entry->addressValue);
        if (!symbolFilePath.isEmpty()) {
            QString sourceHtml = getSourceCodeForAddress(entry->addressValue, symbolFilePath);
            if (!sourceHtml.isEmpty()) {
                detailsText += "=== Source Code ===\n";
                // We'll append this as HTML later
            }
        }
    }

    // For interrupt entries, show all child details
    if (entry->type == EntryType::INTERRUPT && !entry->childEntries.empty()) {
        detailsText += QString("=== Interrupt Details (%1 entries) ===\n\n").arg(entry->childEntries.size());

        for (const auto& child : entry->childEntries) {
            detailsText += QString("Line %1: ").arg(child.lineNumber);

            if (child.type == EntryType::REGISTER) {
                detailsText += "REG ";
                // For register entries, show the full CPU state
                if (!child.originalLine.empty()) {
                    detailsText += QString("CPU State: %1").arg(QString::fromStdString(child.originalLine));
                } else if (!child.assembly.empty()) {
                    QString childIntelAssembly = formatAssembly(child.assembly);
                    detailsText += childIntelAssembly;
                }
            } else if (child.type == EntryType::OTHER) {
                detailsText += "STATE ";
                detailsText += QString::fromStdString(child.originalLine);
            } else {
                // For other types, use assembly if available, otherwise original line
                if (!child.assembly.empty()) {
                    QString childIntelAssembly = formatAssembly(child.assembly);
                    detailsText += childIntelAssembly;
                } else {
                    detailsText += QString::fromStdString(child.originalLine);
                }
            }
            detailsText += "\n";
        }
    }

    // Build HTML if we have source code to display
    QString htmlContent;
    if (entry->type == EntryType::INSTRUCTION && entry->addressValue != 0) {
        const Config& config = ConfigService::instance().getConfig();
        QString symbolFilePath = config.findSymbolFileForAddress(entry->addressValue);
        if (!symbolFilePath.isEmpty()) {
            QString sourceHtml = getSourceCodeForAddress(entry->addressValue, symbolFilePath);
            if (!sourceHtml.isEmpty()) {
                htmlContent = QString("<pre>%1</pre>\n").arg(detailsText.toHtmlEscaped());
                htmlContent += "<hr>\n";
                htmlContent += sourceHtml;
            }
        }
    }

    if (!htmlContent.isEmpty()) {
        detailsPane->setHtml(htmlContent);
    } else {
        detailsPane->setPlainText(detailsText);
    }
}

void QemuLogViewer::syncScrollBars(int value) {
    Q_UNUSED(value)
    // Additional sync logic can be added here if needed
}

void QemuLogViewer::onDetailsPaneLinkClicked(const QUrl& url) {
    // Handle VS Code links - open external URL
    if (url.scheme() == "vscode") {
        QDesktopServices::openUrl(url);
    }
}

void QemuLogViewer::onHexViewSelectionChanged() {
    // Future implementation for hex view interactions
}

bool QemuLogViewer::isAddressInput(const QString& text) { return text.startsWith("0x", Qt::CaseInsensitive); }

QString QemuLogViewer::formatAddress(const std::string& addr) {
    if (addr.empty()) return QString();
    return QString::fromStdString(addr);
}

QString QemuLogViewer::formatFunction(const std::string& func) {
    if (func.empty()) return QString();

    QString qfunc = QString::fromStdString(func);

    // Remove offset like "+0xc6f" from the end
    static const QRegularExpression offsetRegex(R"(\+0x[0-9a-fA-F]+$)");
    QString cleanFunc = qfunc;
    cleanFunc.remove(offsetRegex);

    // If it contains a path (especially .asm files), trim to just the filename
    static const QRegularExpression pathRegex(R"(^(.*/)?([^/]+\.(asm|cpp|c|h|hpp))(.*)$)");
    auto pathMatch = pathRegex.match(cleanFunc);
    if (pathMatch.hasMatch()) {
        // Keep just filename + extension + any remaining info
        QString filename = pathMatch.captured(2);
        QString remaining = pathMatch.captured(4);
        cleanFunc = filename + remaining;
    }

    return cleanFunc;
}

QString QemuLogViewer::formatHexBytes(const std::string& bytes) {
    if (bytes.empty()) return QString();
    return QString::fromStdString(bytes);
}

QString QemuLogViewer::formatAssembly(const std::string& assembly) {
    if (assembly.empty()) return QString();

    // Try to convert to Intel syntax using improved Capstone disassembler
    if (disassembler) {
        std::string intel = disassembler->convertToIntel(assembly);
        return QString::fromStdString(intel);
    }

    return QString::fromStdString(assembly);
}

QString QemuLogViewer::extractFileInfo(const std::string& func) {
    if (func.empty()) return "";
    QString qfunc = QString::fromStdString(func);

    // First check if this is an .asm file path in the function string
    // Format could be: "/path/to/file.asm" or just "file.asm"
    static const QRegularExpression asmFileRegex(R"((^|/|\\)([^/\\]+\.asm)(?:/|\\|$))");
    auto asmMatch = asmFileRegex.match(qfunc);
    if (asmMatch.hasMatch()) {
        // Extract just the .asm filename
        return asmMatch.captured(2);
    }

    // Try to extract file:line:column info from format "addr[func](file:line:column)"
    static const QRegularExpression fileRegex(R"(\(([^)]+)\))");
    auto match = fileRegex.match(qfunc);
    if (match.hasMatch()) {
        QString fileInfo = match.captured(1);

        // Check if this looks like file:line:column (not function parameters)
        static const QRegularExpression fileLineRegex(R"(^[^:]+\.(asm|cpp|c|h|hpp):\d+(?::\d+)?$)");
        if (fileLineRegex.match(fileInfo).hasMatch()) {
            // Extract just filename:line (trim the path)
            static const QRegularExpression pathRegex(R"(([^/\\]+\.(asm|cpp|c|h|hpp):\d+(?::\d+)?))");
            auto pathMatch = pathRegex.match(fileInfo);
            if (pathMatch.hasMatch()) {
                return pathMatch.captured(1);
            }
            return fileInfo;
        }
    }

    return "";
}

// Performance optimization methods
void QemuLogViewer::initializePerformanceOptimizations() {
    // Pre-allocate string buffers only (item pooling removed due to Qt ownership issues)
    stringBuffers.reserve(STRING_BUFFER_SIZE);
    for (size_t i = 0; i < STRING_BUFFER_SIZE; ++i) {
        stringBuffers.emplace_back();
        stringBuffers.back().reserve(512);  // Reserve space for typical strings
    }

    nextStringBuffer = 0;
}

void QemuLogViewer::buildLookupMaps() {
    // Build fast lookup maps for address and line navigation
    addressToEntryMap.clear();
    lineToEntryMap.clear();

    addressToEntryMap.reserve(logEntries.size());
    lineToEntryMap.reserve(logEntries.size());

    for (size_t i = 0; i < logEntries.size(); ++i) {
        const auto& entry = logEntries[i];
        if (entry.addressValue != 0) {
            addressToEntryMap[entry.addressValue] = i;
        }
        if (entry.lineNumber > 0) {
            lineToEntryMap[entry.lineNumber] = i;
        }
    }
}

void QemuLogViewer::buildSearchableRows() {
    // Build shadow search structure for fast text searching
    searchableRows.clear();
    searchableRows.reserve(visibleEntryPointers.size());

    for (size_t row = 0; row < visibleEntryPointers.size(); ++row) {
        SearchableRow searchableRow;
        searchableRow.originalRowIndex = static_cast<int>(row);

        // Concatenate all column text with separators for comprehensive searching
        QString& combinedText = searchableRow.combinedText;
        combinedText.reserve(512);

        if (visibleEntryPointers[row]) {
            const LogEntry* entry = visibleEntryPointers[row];

            // Combine all fields for searching
            combinedText = QString::number(entry->lineNumber);
            combinedText += '\t';
            combinedText += QString::fromStdString(entry->address);
            combinedText += '\t';
            combinedText += QString::fromStdString(entry->function);
            combinedText += '\t';
            combinedText += QString::fromStdString(entry->hexBytes);
            combinedText += '\t';
            combinedText += QString::fromStdString(entry->assembly);
            combinedText += '\t';
            combinedText += QString::fromStdString(entry->originalLine);
        }

        searchableRows.push_back(std::move(searchableRow));
    }
}

QTableWidgetItem* QemuLogViewer::getPooledItem() {
    // Always create a new item since Qt takes ownership
    // The item pool approach doesn't work well with Qt's ownership model
    QTableWidgetItem* item = new QTableWidgetItem();

    // Set default properties
    item->setTextAlignment(Qt::AlignLeft | Qt::AlignVCenter);

    return item;
}

void QemuLogViewer::returnItemToPool(QTableWidgetItem* item) {
    // Items are reused, so we don't actually return them
    // This is for future implementation if needed
    Q_UNUSED(item)
}

QString& QemuLogViewer::getStringBuffer() {
    QString& buffer = stringBuffers[nextStringBuffer];
    nextStringBuffer = (nextStringBuffer + 1) % STRING_BUFFER_SIZE;
    buffer.clear();
    return buffer;
}

void QemuLogViewer::preAllocateTableItems(int rowCount) {
    Q_UNUSED(rowCount)
    // Virtual table doesn't need pre-allocation
    // Rows are created on-demand as needed
}

void QemuLogViewer::batchUpdateTable(const std::vector<const LogEntry*>& entries) {
    Q_UNUSED(entries)
    // Virtual table doesn't need batch updates
    // Rows are created on-demand via the data provider
}

int QemuLogViewer::findNextIretLine(int startLineNumber) const {
    // Find the next iret instruction after the given line number
    for (size_t i = 0; i < logEntries.size(); ++i) {
        const auto& entry = logEntries[i];
        if (entry.lineNumber > startLineNumber && entry.type == EntryType::INSTRUCTION) {
            // Check if the assembly contains 'iret' (case-insensitive)
            std::string assemblyLower = entry.assembly;
            std::transform(assemblyLower.begin(), assemblyLower.end(), assemblyLower.begin(), ::tolower);
            if (assemblyLower.find("iret") != std::string::npos) {
                return entry.lineNumber;
            }
        }
    }
    return INT_MAX;  // No iret found
}

void QemuLogViewer::populateTable() {
    // Filter visible entries first
    bool hideStructural = hideStructuralCheckbox->isChecked();
    bool onlyInterrupts = onlyInterruptsCheckbox->isChecked();

    std::vector<const LogEntry*> visibleEntries;
    visibleEntries.reserve(logEntries.size());

    for (size_t i = 0; i < logEntries.size(); ++i) {
        const auto& entry = logEntries[i];
        // Only process parent entries
        if (entry.isChild) continue;

        // Apply filters
        if (hideStructural && (entry.type == EntryType::SEPARATOR || entry.type == EntryType::BLOCK)) {
            continue;
        }

        if (onlyInterrupts && entry.type != EntryType::INTERRUPT) {
            continue;
        }

        // Apply interrupt filter if selected
        if (!currentSelectedInterrupt.empty() && entry.type == EntryType::INTERRUPT) {
            if (entry.interruptNumber != currentSelectedInterrupt) {
                continue;
            }
        }

        // Check if this interrupt is folded
        if (entry.type == EntryType::INTERRUPT && foldedInterruptEntryIndices.count(i)) {
            // Add the interrupt itself
            visibleEntries.push_back(&entry);

            // Find the next iret and skip everything until then
            int iretLine = findNextIretLine(entry.lineNumber);

            // Skip all entries between this interrupt and the iret
            for (size_t j = i + 1; j < logEntries.size(); ++j) {
                const auto& nextEntry = logEntries[j];
                if (nextEntry.lineNumber >= iretLine) {
                    // We've reached or passed the iret, continue normal processing
                    i = j - 1;  // Will be incremented by loop
                    break;
                }
            }
            continue;
        }

        visibleEntries.push_back(&entry);
    }

    // Store visible entries for selection handling
    visibleEntryPointers = std::move(visibleEntries);

    // Update virtual model row count and signal refresh
    if (virtualTableModel) {
        // Need to update the model's row count before resetting
        virtualTableModel->setRowCount(static_cast<int>(visibleEntryPointers.size()));
        virtualTableModel->resetModel();
        if (!visibleEntryPointers.empty()) {
            virtualTableModel->invalidateRows(0, static_cast<int>(visibleEntryPointers.size()) - 1);
        }
    }

    // Build fast map from logEntries index to visible row for O(1) jumps from panel
    entryIndexToVisibleRow.clear();
    for (size_t row = 0; row < visibleEntryPointers.size(); ++row) {
        const LogEntry* e = visibleEntryPointers[row];
        if (!e) continue;
        // Use lineToEntryMap to find the index in logEntries
        auto it = lineToEntryMap.find(e->lineNumber);
        if (it != lineToEntryMap.end()) {
            entryIndexToVisibleRow[it->second] = static_cast<int>(row);
        }
    }

    // Rebuild searchable rows after table content changes
    buildSearchableRows();
}

void QemuLogViewer::buildInterruptPanel() {
    if (!interruptsPanel) return;
    interruptsPanel->clear();

    // Map interrupt number -> list of logEntries indices
    std::unordered_map<std::string, std::vector<size_t>> map;
    for (size_t i = 0; i < logEntries.size(); ++i) {
        const auto& e = logEntries[i];
        if (e.type == EntryType::INTERRUPT) {
            if (e.interruptNumber.empty()) continue;
            map[e.interruptNumber].push_back(i);
        }
    }

    if (map.empty()) return;

    // Same name map as used elsewhere
    std::unordered_map<int, std::string> irqNames{{0x0, "Divide Error"},
                                                  {0x1, "Debug"},
                                                  {0x2, "NMI"},
                                                  {0x3, "Breakpoint"},
                                                  {0x4, "Overflow"},
                                                  {0x5, "BOUND Range Exceeded"},
                                                  {0x6, "Invalid Opcode"},
                                                  {0x7, "Device Not Available"},
                                                  {0x8, "Double Fault"},
                                                  {0x9, "Coprocessor Segment Overrun"},
                                                  {0xa, "Invalid TSS"},
                                                  {0xb, "Segment Not Present"},
                                                  {0xc, "Stack-Segment Fault"},
                                                  {0xd, "General Protection Fault"},
                                                  {0xe, "Page Fault"},
                                                  {0x10, "x87 FPU Floating-Point Error"},
                                                  {0x11, "Alignment Check"},
                                                  {0x12, "Machine Check"},
                                                  {0x13, "SIMD Floating-Point Exception"}};

    // Create top-level items in order of first appearance
    for (const auto& [irq, indices] : map) {
        QString qirq = QString::fromStdString(irq);
        bool ok;
        int val = qirq.toInt(&ok, 16);
        QString title = qirq;
        if (ok) {
            auto it = irqNames.find(val);
            if (it != irqNames.end())
                title = QString("0x%1 - %2").arg(qirq).arg(QString::fromStdString(it->second));
            else
                title = QString("0x%1").arg(qirq);
        }

        QTreeWidgetItem* top = new QTreeWidgetItem(interruptsPanel);

        // Add children for each occurrence with entry index stored
        for (size_t idx : indices) {
            const auto& e = logEntries[idx];
            QString childText = QString("Line %1").arg(e.lineNumber);

            // Add fold indicator if this occurrence is folded
            if (foldedInterruptEntryIndices.count(idx)) {
                childText = QString("[▼ FOLDED] ") + childText;
            } else {
                childText = QString("[▲] ") + childText;
            }

            QTreeWidgetItem* child = new QTreeWidgetItem();
            child->setText(0, childText);
            child->setText(1, QString::fromStdString(e.address));
            // store entry index as user data (this uniquely identifies this specific interrupt occurrence)
            child->setData(0, Qt::UserRole, QVariant::fromValue(static_cast<qulonglong>(idx)));
            top->addChild(child);
        }

        top->setText(0, title);
        top->setText(1, QString::number(indices.size()));

        interruptsPanel->addTopLevelItem(top);
    }

    interruptsPanel->expandAll();
}

void QemuLogViewer::onInterruptPanelActivated(QTreeWidgetItem* item, int /*column*/) {
    if (!item) return;

    // If top-level item was activated, expand/collapse
    if (!item->parent()) {
        item->setExpanded(!item->isExpanded());
        return;
    }

    // Child item: retrieve entry index
    QVariant v = item->data(0, Qt::UserRole);
    if (!v.isValid()) return;
    size_t entryIndex = static_cast<size_t>(v.toULongLong());

    // Try to map entryIndex to visible row
    auto it = entryIndexToVisibleRow.find(entryIndex);
    if (it != entryIndexToVisibleRow.end()) {
        int row = it->second;
        logTable->selectRow(row);
        scrollToRow(row);
        updateDetailsPane(row);
        statusLabel->setText(QString("Jumped to interrupt (line %1)").arg(QString::number(logEntries[entryIndex].lineNumber)));
        return;
    }

    // If mapping not found (entry not visible), repopulate table and try again
    populateTable();
    it = entryIndexToVisibleRow.find(entryIndex);
    if (it != entryIndexToVisibleRow.end()) {
        int row = it->second;
        logTable->selectRow(row);
        scrollToRow(row);
        updateDetailsPane(row);
        statusLabel->setText(QString("Jumped to interrupt (line %1)").arg(QString::number(logEntries[entryIndex].lineNumber)));
    } else {
        statusLabel->setText("Selected interrupt occurrence not visible (may be hidden by filters)");
    }
}

void QemuLogViewer::onInterruptToggleFold(QTreeWidgetItem* item, int /*column*/) {
    if (!item) {
        return;
    }

    // Handle child items (individual interrupt occurrences)
    if (item->parent()) {
        QVariant v = item->data(0, Qt::UserRole);
        if (!v.isValid()) {
            return;
        }

        size_t entryIndex = static_cast<size_t>(v.toULongLong());
        if (entryIndex >= logEntries.size()) {
            return;
        }

        // Toggle the folded state for this specific occurrence
        if (foldedInterruptEntryIndices.count(entryIndex)) {
            foldedInterruptEntryIndices.erase(entryIndex);
            statusLabel->setText(QString("Interrupt at line %1 unfolded").arg(logEntries[entryIndex].lineNumber));
        } else {
            foldedInterruptEntryIndices.insert(entryIndex);
            statusLabel->setText(QString("Interrupt at line %1 folded").arg(logEntries[entryIndex].lineNumber));
        }

        // Rebuild interrupt panel to update visual indicators and repopulate table
        buildInterruptPanel();
        populateTable();
        return;
    }

    // Handle top-level items (fold all occurrences of this interrupt type)
    // First, collect all child entry indices
    for (int i = 0; i < item->childCount(); ++i) {
        QTreeWidgetItem* child = item->child(i);
        if (!child) {
            continue;
        }

        QVariant v = child->data(0, Qt::UserRole);
        if (!v.isValid()) {
            continue;
        }

        size_t entryIndex = static_cast<size_t>(v.toULongLong());
        if (entryIndex >= logEntries.size()) {
            continue;
        }

        // Toggle the folded state for each occurrence
        if (foldedInterruptEntryIndices.count(entryIndex)) {
            foldedInterruptEntryIndices.erase(entryIndex);
        } else {
            foldedInterruptEntryIndices.insert(entryIndex);
        }
    }

    // Rebuild interrupt panel to update visual indicators and repopulate table
    buildInterruptPanel();
    populateTable();
}

void QemuLogViewer::performSearchOptimized() {
    searchMatches.clear();
    searchMatches.reserve(searchableRows.size() / 10);  // Estimate 10% match rate

    QString searchText = searchEdit->text().trimmed();
    if (searchText.isEmpty()) {
        // If search text is empty, cancel search and return to original position
        if (searchActive && preSearchPosition >= 0) {
            scrollToRow(preSearchPosition);
            searchActive = false;
            preSearchPosition = -1;
        }
        currentSearchIndex = -1;
        searchNextBtn->setEnabled(false);
        searchPrevBtn->setEnabled(false);
        highlightSearchMatches();
        return;
    }

    // Store current position before starting search (only on first search)
    if (!searchActive) {
        auto selectedRows = logTable->selectionModel()->selectedRows();
        if (!selectedRows.isEmpty()) {
            preSearchPosition = selectedRows.first().row();
        } else {
            // Get the top visible row if no selection
            preSearchPosition = logTable->rowAt(0);
        }
        searchActive = true;
    }

    // Create search pattern
    if (regexCheckbox->isChecked()) {
        searchRegex = QRegularExpression(searchText, QRegularExpression::CaseInsensitiveOption);
        if (!searchRegex.isValid()) {
            statusLabel->setText("Invalid regex pattern");
            return;
        }
    } else {
        QString pattern = searchText;
        pattern.replace(QRegularExpression("([\\^\\$\\*\\+\\?\\{\\}\\[\\]\\(\\)\\|\\\\])"), "\\\\1");
        searchRegex = QRegularExpression(pattern, QRegularExpression::CaseInsensitiveOption);
    }

    // Fast search using shadow search structure - this can be vectorized by the compiler
    for (const auto& searchableRow : searchableRows) {
        if (searchRegex.match(searchableRow.combinedText).hasMatch()) {
            searchMatches.push_back(searchableRow.originalRowIndex);
        }
    }

    // Update UI state
    bool hasMatches = !searchMatches.empty();
    searchNextBtn->setEnabled(hasMatches);
    searchPrevBtn->setEnabled(hasMatches);

    if (hasMatches) {
        // Find the nearest match to the current view
        int currentViewRow = preSearchPosition >= 0 ? preSearchPosition : 0;

        // Find the closest match
        int nearestIndex = 0;
        int minDistance = std::abs(searchMatches[0] - currentViewRow);

        for (size_t i = 1; i < searchMatches.size(); ++i) {
            int distance = std::abs(searchMatches[i] - currentViewRow);
            if (distance < minDistance) {
                minDistance = distance;
                nearestIndex = static_cast<int>(i);
            }
        }

        currentSearchIndex = nearestIndex;
        statusLabel->setText(QString("Match %1 of %2").arg(currentSearchIndex + 1).arg(searchMatches.size()));
        highlightSearchMatches();
        scrollToRowForSearch(searchMatches[currentSearchIndex]);
    } else {
        statusLabel->setText("No matches found");
        highlightSearchMatches();
    }
}

QString QemuLogViewer::getSourceCodeForAddress(uint64_t address, const QString& binaryPath) {
    // Use addr2line to get source file and line number
    QProcess addr2line;
    addr2line.start("addr2line", {"-e", binaryPath, QString("0x%1").arg(address, 0, 16)});

    if (!addr2line.waitForFinished(5000)) {
        return "";
    }

    QString output = addr2line.readAllStandardOutput().trimmed();
    if (output.isEmpty() || output.contains("??")) {
        return "";
    }

    // Parse output: "filename:linenumber" or "filename:linenumber:column"
    QStringList parts = output.split(":");
    if (parts.size() < 2) {
        return "";
    }

    QString filename = parts[0];
    bool lineOk = false;
    int lineNumber = parts[1].toInt(&lineOk);

    if (!lineOk || lineNumber <= 0) {
        return "";
    }

    // Try to read the source file
    QFile sourceFile(filename);
    if (!sourceFile.open(QIODevice::ReadOnly | QIODevice::Text)) {
        // Return just the filename:line if we can't read the file
        return QString("%1:%2").arg(QFileInfo(filename).fileName()).arg(lineNumber);
    }

    QTextStream in(&sourceFile);
    QStringList lines;
    int currentLine = 0;

    while (!in.atEnd() && currentLine < lineNumber + 2) {
        lines.append(in.readLine());
        currentLine++;
    }

    sourceFile.close();

    // Build HTML with syntax highlighting and clickable lines
    QString html;
    html += QString("<b>%1:%2</b><br>").arg(QFileInfo(filename).fileName()).arg(lineNumber);
    html += "<pre style='font-family: Consolas, monospace; font-size: 10px; margin: 5px 0;'>";

    int startLine = std::max(0, lineNumber - 3);
    int endLine = std::min(static_cast<int>(lines.size()), lineNumber + 2);

    for (int i = startLine; i < endLine; ++i) {
        int displayLine = i + 1;
        QString line = lines[i];

        // Highlight the target line
        if (displayLine == lineNumber) {
            html += QString("<span style='background-color: #333300; color: #ffff99;'><b>%1 > </b>%2</span>\n")
                        .arg(displayLine, 4)
                        .arg(line.toHtmlEscaped());
        } else {
            html += QString("<span style='color: #666666;'>%1   %2</span>\n").arg(displayLine, 4).arg(line.toHtmlEscaped());
        }
    }

    html += "</pre>";

    // Add clickable link to open in VS Code with proper format
    // VS Code URI scheme: vscode://file/ABSOLUTE_PATH:LINE:COLUMN
    QFileInfo fileInfo(filename);
    QString absolutePath = fileInfo.absoluteFilePath();
    // URL encode the path
    absolutePath.replace(" ", "%20");
    html += QString("<br><a href='vscode://file/%1:%2:1' style='color: #4da6ff; text-decoration: underline;'>Open in VS Code</a>")
                .arg(absolutePath)
                .arg(lineNumber);

    return html;
}

QColor QemuLogViewer::getEntryTypeColor(EntryType type) {
    // Colors darkened to 25% of their current values for better dark theme integration
    switch (type) {
        case EntryType::INSTRUCTION:
            return QColor(9, 19, 9);  // 25% of (35, 75, 35)
        case EntryType::INTERRUPT:
            return QColor(19, 9, 9);  // 25% of (75, 35, 35)
        case EntryType::REGISTER:
            return QColor(9, 9, 19);  // 25% of (35, 35, 75)
        case EntryType::BLOCK:
            return QColor(19, 19, 9);  // 25% of (75, 75, 35)
        case EntryType::SEPARATOR:
            return QColor(13, 13, 13);  // 25% of (50, 50, 50)
        case EntryType::OTHER:
        default:
            return QColor(8, 8, 8);  // 25% of (30, 30, 30)
    }
}

void QemuLogViewer::onTableCellClicked(int row, int column) {
    Q_UNUSED(column)
    // Update details pane for any row click
    updateDetailsPane(row);
}
