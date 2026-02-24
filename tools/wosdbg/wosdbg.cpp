#include "wosdbg.h"

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

#include <QDir>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "config.h"
#include "log_client.h"
#include "virtual_table.h"
#include "virtual_table_integration.h"

// Work around BFD config.h requirement
#define PACKAGE "wosdbg"
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
#include <QtWidgets/QDockWidget>
#include <QtWidgets/QFileDialog>
#include <QtWidgets/QHeaderView>
#include <QtWidgets/QMessageBox>
#include <QtWidgets/QStyle>
#include <QtWidgets/QStyledItemDelegate>
#include <QtWidgets/QTextBrowser>
#include <algorithm>
#include <iomanip>
#include <sstream>

#include "capstone_disasm.h"

// Include our existing processing functions
#include <cxxabi.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>

// CapstoneDisassembler: see capstone_disasm.h / capstone_disasm.cpp

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

QemuLogViewer::QemuLogViewer(LogClient* client, QWidget* parent)
    : QMainWindow(parent), currentSearchIndex(-1), preSearchPosition(-1), searchActive(false), client(client), nextStringBuffer(0) {
    disassembler = std::make_unique<CapstoneDisassembler>();

    // Initialize search debounce timer
    searchDebounceTimer = new QTimer(this);
    searchDebounceTimer->setSingleShot(true);
    searchDebounceTimer->setInterval(300);  // 300ms debounce
    connect(searchDebounceTimer, &QTimer::timeout, this, &QemuLogViewer::performDebouncedSearch);

    // Initialize performance optimizations first
    initializePerformanceOptimizations();

    setupDarkTheme();
    setupUI();

    // Set up syntax highlighters after UI is created
    disassemblyHighlighter = new SyntaxHighlighter(disassemblyView->document());
    detailsHighlighter = new SyntaxHighlighter(detailsPane->document());

    // Set up table syntax highlighting delegate
    tableDelegate = new SyntaxHighlightDelegate(this);
    // logTable is VirtualTableView, it uses delegate?
    // VirtualTableView uses QStyledItemDelegate internally or custom painting?
    // VirtualTableView inherits QTableView.
    // We can set delegate on it.
    logTable->setItemDelegate(tableDelegate);

    connectSignals();

    // Connect client signals
    connect(client, &LogClient::fileListReceived, this, &QemuLogViewer::onFileListReceived);
    connect(client, &LogClient::configReceived, this, &QemuLogViewer::onConfigReceived);
    connect(client, &LogClient::fileReady, this, &QemuLogViewer::onFileReady);
    connect(client, &LogClient::dataReceived, this, &QemuLogViewer::onDataReceived);
    connect(client, &LogClient::searchResults, this, &QemuLogViewer::onSearchResults);
    connect(client, &LogClient::interruptsReceived, this, &QemuLogViewer::onInterruptsReceived);
    connect(client, &LogClient::filterApplied, this, &QemuLogViewer::onFilterApplied);
    connect(client, &LogClient::progress, this, &QemuLogViewer::onProgressUpdate);

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
    setWindowTitle("wosdbg - WOS Debugger");
    setMinimumSize(1200, 800);
    resize(1600, 1000);

    setupToolbar();
    setupMainContent();
    setupCoredumpPanels();
}

void QemuLogViewer::setupToolbar() {
    toolbar = addToolBar("Main");
    toolbar->setMovable(false);

    // File selector
    toolbar->addWidget(new QLabel("Log File:"));
    fileSelector = new QComboBox();
    fileSelector->setMinimumWidth(200);
    toolbar->addWidget(fileSelector);

    refreshFilesBtn = new QPushButton("↻");
    refreshFilesBtn->setToolTip("Refresh file list");
    toolbar->addWidget(refreshFilesBtn);

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

    toolbar->addSeparator();

    // Coredump controls
    auto* coredumpDirBtn = new QPushButton("📁 Coredumps");
    coredumpDirBtn->setToolTip("Browse/change coredump directory");
    connect(coredumpDirBtn, &QPushButton::clicked, this, &QemuLogViewer::browseCoredumpDirectory);
    toolbar->addWidget(coredumpDirBtn);

    auto* extractBtn = new QPushButton("📤 Extract");
    extractBtn->setToolTip("Extract coredumps from disk images");
    connect(extractBtn, &QPushButton::clicked, this, &QemuLogViewer::extractCoredumps);
    toolbar->addWidget(extractBtn);

    auto* refreshDumpsBtn = new QPushButton("🔄");
    refreshDumpsBtn->setToolTip("Refresh coredump list");
    connect(refreshDumpsBtn, &QPushButton::clicked, this, &QemuLogViewer::refreshCoredumps);
    toolbar->addWidget(refreshDumpsBtn);
}

void QemuLogViewer::setupMainContent() {
    // Central widget: log table only
    auto centralWidget = new QWidget();
    setCentralWidget(centralWidget);
    auto layout = new QVBoxLayout(centralWidget);
    layout->setContentsMargins(0, 0, 0, 0);

    setupTable();
    layout->addWidget(logTable);

    // --- Interrupts panel (left dock) ---
    interruptsPanel = new QTreeWidget();
    interruptsPanel->setHeaderLabels(QStringList() << "Interrupt" << "Occurrences");
    interruptsPanel->setMinimumWidth(200);
    interruptsPanel->setSelectionMode(QAbstractItemView::SingleSelection);

    interruptsDock_ = new QDockWidget("Interrupts", this);
    interruptsDock_->setWidget(interruptsPanel);
    interruptsDock_->setAllowedAreas(Qt::LeftDockWidgetArea | Qt::RightDockWidgetArea);
    addDockWidget(Qt::LeftDockWidgetArea, interruptsDock_);

    // --- Hex bytes (bottom dock) ---
    hexView = new QTextEdit();
    hexView->setReadOnly(true);
    hexView->setFont(QFont("Consolas", 12));
    hexView->setPlaceholderText("Hex bytes will be displayed here...");

    hexDock_ = new QDockWidget("Hex Bytes", this);
    hexDock_->setWidget(hexView);
    hexDock_->setAllowedAreas(Qt::AllDockWidgetAreas);
    addDockWidget(Qt::BottomDockWidgetArea, hexDock_);

    // --- Disassembly (right dock) ---
    disassemblyView = new QTextEdit();
    disassemblyView->setReadOnly(true);
    disassemblyView->setFont(QFont("Consolas", 12));
    disassemblyView->setPlaceholderText("Detailed disassembly will be displayed here...");

    disassemblyDock_ = new QDockWidget("Disassembly", this);
    disassemblyDock_->setWidget(disassemblyView);
    disassemblyDock_->setAllowedAreas(Qt::AllDockWidgetAreas);
    addDockWidget(Qt::RightDockWidgetArea, disassemblyDock_);

    // --- Interrupt details (right dock, stacked below disassembly) ---
    detailsPane = new QTextBrowser();
    detailsPane->setReadOnly(true);
    detailsPane->setOpenExternalLinks(false);
    detailsPane->setOpenLinks(false);
    detailsPane->setFont(QFont("Consolas", 12));
    detailsPane->setPlaceholderText("Interrupt details will be displayed here...");

    detailsDock_ = new QDockWidget("Interrupt Details", this);
    detailsDock_->setWidget(detailsPane);
    detailsDock_->setAllowedAreas(Qt::AllDockWidgetAreas);
    addDockWidget(Qt::RightDockWidgetArea, detailsDock_);

    // Stack disassembly and details vertically in the right dock area
    splitDockWidget(disassemblyDock_, detailsDock_, Qt::Vertical);
}

void QemuLogViewer::setupTable() {
    // Use integration helper to create virtual table
    logTable = VirtualTableIntegration::initializeVirtualTable(this, client);
    virtualTableModel = static_cast<VirtualTableModel*>(logTable->model());
    logTable->setMouseTracking(true);

    // Connect signals
    connect(logTable, &QTableView::clicked, this, [this](const QModelIndex& index) { onTableCellClicked(index.row(), index.column()); });

    // Connect selection changed
    connect(logTable->selectionModel(), &QItemSelectionModel::selectionChanged, this, &QemuLogViewer::onTableSelectionChanged);
}

void QemuLogViewer::connectSignals() {
    connect(fileSelector, QOverload<const QString&>::of(&QComboBox::currentTextChanged), this, &QemuLogViewer::onFileSelected);
    connect(refreshFilesBtn, &QPushButton::clicked, client, &LogClient::requestFileList);

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

    // Connect row for line received signal
    connect(client, &LogClient::rowForLineReceived, this, &QemuLogViewer::onRowForLineReceived);
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
    if (filename.isEmpty()) return;

    // Disable UI
    fileSelector->setEnabled(false);
    searchEdit->setEnabled(false);
    navigationEdit->setEnabled(false);
    logTable->setEnabled(false);

    statusLabel->setText("Requesting file...");
    progressBar->setVisible(true);
    progressBar->setValue(0);
    progressBar->setFormat("Requesting file... %p%");

    // Clear current data
    if (virtualTableModel) {
        virtualTableModel->setRowCount(0);
    }
    searchMatches.clear();
    currentSearchIndex = -1;

    // Clear views
    hexView->clear();
    disassemblyView->clear();
    detailsPane->clear();

    // Request file from client
    client->selectFile(filename);
}

void QemuLogViewer::onProgressUpdate(int percentage) {
    progressBar->setValue(percentage);
    progressBar->setFormat("Processing... %p%");
}

void QemuLogViewer::onFileReady(int totalLines) {
    qDebug() << "QemuLogViewer::onFileReady totalLines=" << totalLines;
    // Update model
    if (virtualTableModel) {
        virtualTableModel->setRowCount(totalLines);
    }

    // Re-enable UI
    fileSelector->setEnabled(true);
    searchEdit->setEnabled(true);
    navigationEdit->setEnabled(true);
    logTable->setEnabled(true);

    progressBar->setVisible(false);
    statusLabel->setText(QString("Loaded %1 lines").arg(totalLines));

    // Request interrupts
    client->requestInterrupts();
}

void QemuLogViewer::onFileListReceived(const QStringList& files) {
    QString current = fileSelector->currentText();
    fileSelector->blockSignals(true);
    fileSelector->clear();
    fileSelector->addItems(files);

    int index = fileSelector->findText(current);
    if (index != -1) {
        fileSelector->setCurrentIndex(index);
    }

    fileSelector->blockSignals(false);

    if (!files.isEmpty()) {
        statusLabel->setText(QString("Found %1 log files").arg(files.size()));
    }
}

void QemuLogViewer::onConfigReceived() {
    const auto& lookups = client->getConfig().getAddressLookups();
    statusLabel->setText(statusLabel->text() + QString(" • Config: %1 symbol lookups").arg(lookups.size()));
}

void QemuLogViewer::onDataReceived(int startLine, int count) {
    if (virtualTableModel) {
        virtualTableModel->invalidateRows(startLine, startLine + count - 1);
        // Force repaint to ensure "Loading..." is replaced immediately
        logTable->viewport()->update();
    }

    // Check if currently selected row is in range and update details pane if so
    if (logTable->selectionModel()->hasSelection()) {
        int currentRow = logTable->currentIndex().row();
        if (currentRow >= startLine && currentRow < startLine + count) {
            updateDetailsPane(currentRow);
        }
    }
}

void QemuLogViewer::onSearchResults(const std::vector<int>& matches) {
    searchMatches = matches;
    currentSearchIndex = -1;

    bool hasMatches = !searchMatches.empty();
    searchNextBtn->setEnabled(hasMatches);
    searchPrevBtn->setEnabled(hasMatches);

    if (hasMatches) {
        currentSearchIndex = 0;
        statusLabel->setText(QString("Match 1 of %1").arg(searchMatches.size()));
        scrollToRowForSearch(searchMatches[0]);
    } else {
        statusLabel->setText("No matches found");
    }
}

void QemuLogViewer::onInterruptsReceived(const std::vector<LogEntry>& interrupts) {
    interruptsPanel->clear();
    std::map<std::string, QTreeWidgetItem*> groups;

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

    if (interruptFilterCombo) {
        interruptFilterCombo->blockSignals(true);
        interruptFilterCombo->clear();
        interruptFilterCombo->addItem("All");
    }

    std::unordered_set<std::string> seenInterrupts;

    for (const auto& entry : interrupts) {
        std::string intNum = entry.interruptNumber;

        // Populate Panel
        if (groups.find(intNum) == groups.end()) {
            auto* item = new QTreeWidgetItem(interruptsPanel);

            // Format interrupt name
            QString s = QString::fromStdString(intNum);
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

            item->setText(0, display);
            item->setData(0, Qt::UserRole, QString::fromStdString(intNum));
            groups[intNum] = item;
        }

        auto* groupItem = groups[intNum];
        auto* child = new QTreeWidgetItem(groupItem);
        child->setText(0, QString("Line %1: %2").arg(entry.lineNumber).arg(QString::fromStdString(entry.cpuStateInfo)));
        child->setData(0, Qt::UserRole, entry.lineNumber);

        // Populate Combo Box
        if (interruptFilterCombo && seenInterrupts.insert(intNum).second) {
            QString s = QString::fromStdString(intNum);
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
            interruptFilterCombo->addItem(display, QVariant(s));
        }
    }

    // Update occurrences count
    for (auto const& [key, item] : groups) {
        item->setText(1, QString::number(item->childCount()));
    }

    if (interruptFilterCombo) {
        interruptFilterCombo->setEnabled(!interrupts.empty());
        interruptFilterCombo->blockSignals(false);
    }
}

void QemuLogViewer::onFilterApplied(int totalLines) {
    if (virtualTableModel) {
        virtualTableModel->setRowCount(totalLines);
    }
    statusLabel->setText(QString("Filtered: %1 lines").arg(totalLines));
}

// Populate the interrupt filter dropdown with unique interrupt numbers present
void QemuLogViewer::populateInterruptFilter() {
    // Deprecated: handled by onInterruptsReceived
}

void QemuLogViewer::buildInterruptPanel() {
    // Deprecated: handled by onInterruptsReceived
}

// Slot fired when the interrupt filter selection changes
void QemuLogViewer::onInterruptFilterChanged(const QString& text) {
    if (client) {
        bool hideStructural = hideStructuralCheckbox->isChecked();
        QString interruptFilter = text;
        if (interruptFilter == "All") interruptFilter = "";

        client->setFilter(hideStructural, interruptFilter);
    }
}

// Move to next occurrence of currently selected interrupt
void QemuLogViewer::onInterruptNext() {
    // With server-side filtering, the view only contains the interrupts.
    // So we just move to the next row.
    int currentRow = logTable->currentIndex().row();
    if (currentRow < logTable->model()->rowCount() - 1) {
        logTable->selectRow(currentRow + 1);
        scrollToRow(currentRow + 1);
    }
}

// Move to previous occurrence of currently selected interrupt
void QemuLogViewer::onInterruptPrevious() {
    int currentRow = logTable->currentIndex().row();
    if (currentRow > 0) {
        logTable->selectRow(currentRow - 1);
        scrollToRow(currentRow - 1);
    }
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

    if (client) {
        bool hideStructural = hideStructuralCheckbox->isChecked();
        QString interruptFilter = interruptFilterCombo->currentText();
        if (interruptFilter == "All") interruptFilter = "";

        client->setFilter(hideStructural, interruptFilter);
    }

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
    if (client) {
        client->search(QString("0x%1").arg(address, 0, 16), false);
    }
}

void QemuLogViewer::jumpToLine(int /*lineNumber*/) { statusLabel->setText("Jump to line not supported in remote mode yet"); }

void QemuLogViewer::scrollToRow(int row) {
    if (logTable && row >= 0 && row < logTable->model()->rowCount()) {
        logTable->selectRow(row);
        logTable->scrollTo(logTable->model()->index(row, 0), QAbstractItemView::PositionAtCenter);
    }
}

void QemuLogViewer::scrollToRowForSearch(int row) {
    if (logTable && row >= 0 && row < logTable->model()->rowCount()) {
        // For virtual table, use scrollToLogicalRow which handles loading
        logTable->scrollToLogicalRow(row, QAbstractItemView::PositionAtCenter);
        // Select the row
        logTable->selectRow(row);
        updateDetailsPane(row);
    }
}

void QemuLogViewer::onTableSelectionChanged() {
    auto selectedRows = logTable->selectionModel()->selectedRows();
    if (selectedRows.isEmpty()) {
        detailsPane->clear();
        return;
    }

    int row = selectedRows.first().row();
    if (client && row >= 0 && row < logTable->model()->rowCount()) {
        const LogEntry* entry = client->getEntry(row);
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
    if (!client || !logTable || row < 0 || row >= logTable->model()->rowCount()) {
        detailsPane->clear();
        return;
    }

    const LogEntry* entry = client->getEntry(row);
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

        // Show symbol resolution status based on whether we found a function
        if (!entry->function.empty()) {
            detailsText += QString("Symbol Lookup: Resolved\n");
        } else if (entry->addressValue != 0) {
            const Config& config = ConfigService::instance().getConfig();
            QString symbolFilePath = config.findSymbolFileForAddress(entry->addressValue);
            if (!symbolFilePath.isEmpty()) {
                detailsText += QString("Symbol File: %1\n").arg(symbolFilePath);
                detailsText += QString("Symbol Lookup: No symbol found at this address\n");
            } else {
                detailsText += QString("Symbol Lookup: No mapping found for this address range\n");
            }
        }
    }

    if (!entry->function.empty()) {
        // Display function name without offset
        QString funcName = formatFunction(entry->function);
        detailsText += QString("Function: %1\n").arg(funcName);

        // Show source info if available
        if (!entry->sourceFile.empty() && entry->sourceLine > 0) {
            detailsText += QString("Source: %1:%2\n").arg(QString::fromStdString(entry->sourceFile)).arg(entry->sourceLine);
        } else if (!entry->sourceFile.empty()) {
            detailsText += QString("Source File: %1\n").arg(QString::fromStdString(entry->sourceFile));
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
    if (entry->type == EntryType::INSTRUCTION && !entry->sourceFile.empty() && entry->sourceLine > 0) {
        QString sourceHtml = getSourceCodeSnippet(QString::fromStdString(entry->sourceFile), entry->sourceLine);
        if (!sourceHtml.isEmpty()) {
            detailsText += "=== Source Code ===\n";
            // We'll append this as HTML later
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
    if (entry->type == EntryType::INSTRUCTION) {
        if (!entry->sourceFile.empty() && entry->sourceLine > 0) {
            QString sourceHtml = getSourceCodeSnippet(QString::fromStdString(entry->sourceFile), entry->sourceLine);
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
    } else if (url.scheme() == "wos-remote") {
        // Format: wos-remote://path:line
        QString path = url.path();  // /path/to/file:line
        // Remove leading slash if present (Qt adds it for host-less URLs sometimes)
        // But here we used wos-remote://path:line, so host is empty, path is /path:line

        // Actually, let's parse it manually from string to be safe
        QString urlStr = url.toString();
        QString prefix = "wos-remote://";
        if (urlStr.startsWith(prefix)) {
            QString content = urlStr.mid(prefix.length());
            int lastColon = content.lastIndexOf(':');
            if (lastColon != -1) {
                QString file = content.left(lastColon);
                int line = content.mid(lastColon + 1).toInt();

                client->requestOpenSourceFile(file, line);
            }
        }
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
    // Deprecated in client-server mode
}

void QemuLogViewer::buildSearchableRows() {
    // Deprecated in client-server mode
}

QTableWidgetItem* QemuLogViewer::getPooledItem() { return new QTableWidgetItem(); }

void QemuLogViewer::returnItemToPool(QTableWidgetItem* item) { Q_UNUSED(item) }

QString& QemuLogViewer::getStringBuffer() {
    QString& buffer = stringBuffers[nextStringBuffer];
    nextStringBuffer = (nextStringBuffer + 1) % STRING_BUFFER_SIZE;
    buffer.clear();
    return buffer;
}

void QemuLogViewer::preAllocateTableItems(int rowCount) { Q_UNUSED(rowCount) }

void QemuLogViewer::batchUpdateTable(const std::vector<const LogEntry*>& entries) { Q_UNUSED(entries) }

int QemuLogViewer::findNextIretLine(int startLineNumber) const {
    Q_UNUSED(startLineNumber)
    return INT_MAX;
}

void QemuLogViewer::populateTable() {
    // Deprecated in client-server mode
}

void QemuLogViewer::onInterruptPanelActivated(QTreeWidgetItem* item, int /*column*/) {
    if (!item || !item->parent()) return;

    // Child item: retrieve line number
    QVariant v = item->data(0, Qt::UserRole);
    if (!v.isValid()) return;
    int lineNumber = v.toInt();

    // Request row for line from server
    statusLabel->setText(QString("Jumping to line %1...").arg(lineNumber));
    client->requestRowForLine(lineNumber);
}

void QemuLogViewer::onRowForLineReceived(int row) {
    if (row >= 0) {
        scrollToRow(row);
        statusLabel->setText(QString("Jumped to row %1").arg(row));
    } else {
        statusLabel->setText("Could not find row for line (maybe filtered out?)");
    }
}

void QemuLogViewer::onInterruptToggleFold(QTreeWidgetItem* item, int /*column*/) {
    Q_UNUSED(item)
    // Folding not supported in remote mode yet
}

void QemuLogViewer::performSearchOptimized() {
    QString searchText = searchEdit->text().trimmed();
    if (searchText.isEmpty()) {
        if (searchActive && preSearchPosition >= 0) {
            scrollToRow(preSearchPosition);
            searchActive = false;
            preSearchPosition = -1;
        }
        currentSearchIndex = -1;
        searchNextBtn->setEnabled(false);
        searchPrevBtn->setEnabled(false);
        searchMatches.clear();
        highlightSearchMatches();
        return;
    }

    if (!searchActive) {
        auto selectedRows = logTable->selectionModel()->selectedRows();
        if (!selectedRows.isEmpty()) {
            preSearchPosition = selectedRows.first().row();
        } else {
            preSearchPosition = logTable->rowAt(0);
        }
        searchActive = true;
    }

    client->search(searchText, regexCheckbox->isChecked());
    statusLabel->setText("Searching...");
}

QString QemuLogViewer::getSourceCodeSnippet(const QString& filename, int lineNumber) {
    if (filename.isEmpty() || lineNumber <= 0) {
        return "";
    }

    // Try to read the source file
    QFile sourceFile(filename);
    if (!sourceFile.open(QIODevice::ReadOnly | QIODevice::Text)) {
        qDebug() << "Could not open source file:" << filename << "CWD:" << QDir::currentPath();
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
    html += "<pre style='font-family: Consolas, monospace; margin: 5px 0;'>";

    int startLine = std::max(0, lineNumber - 11);
    int endLine = std::min(static_cast<int>(lines.size()), lineNumber + 10);

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
    // Use custom scheme to intercept click
    QFileInfo fileInfo(filename);
    QString absolutePath = fileInfo.absoluteFilePath();
    // URL encode the path
    absolutePath.replace(" ", "%20");
    html += QString("<br><a href='wos-remote://%1:%2' style='color: #4da6ff; text-decoration: underline;'>Open in VS Code</a>")
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

// ============================================================================
// Coredump Integration
// ============================================================================

#include <QFileDialog>
#include <QMessageBox>
#include <QtWidgets/QDockWidget>

#include "config.h"
#include "coredump_browser.h"
#include "coredump_elf_panel.h"
#include "coredump_memory.h"
#include "coredump_memory_panel.h"
#include "coredump_parser.h"
#include "coredump_register_panel.h"
#include "coredump_segment_panel.h"
#include "elf_symbol_resolver.h"

void QemuLogViewer::setupCoredumpPanels() {
    // --- Browser (left dock, tabbed with interrupts) ---
    coredumpBrowser_ = new CoredumpBrowser(this);
    browserDock_ = new QDockWidget("Coredump Browser", this);
    browserDock_->setWidget(coredumpBrowser_);
    browserDock_->setAllowedAreas(Qt::LeftDockWidgetArea | Qt::RightDockWidgetArea);
    addDockWidget(Qt::LeftDockWidgetArea, browserDock_);

    // Tab the coredump browser with the interrupts panel
    if (interruptsDock_) {
        tabifyDockWidget(interruptsDock_, browserDock_);
        interruptsDock_->raise();  // Show interrupts tab by default
    }

    // Set initial directory from config
    const auto& cfg = ConfigService::instance().getConfig();
    coredumpBrowser_->setDirectory(cfg.getCoredumpDirectory());

    connect(coredumpBrowser_, &CoredumpBrowser::coredumpSelected, this, &QemuLogViewer::openCoredump);
    connect(coredumpBrowser_, &CoredumpBrowser::extractionFinished, this, [this](bool ok, const QString& msg) {
        if (ok) {
            statusLabel->setText("Extraction complete");
            coredumpBrowser_->refresh();
        } else {
            statusLabel->setText(QString("Extraction failed: %1").arg(msg));
        }
    });

    // --- Register panel (right dock) ---
    registerPanel_ = new CoredumpRegisterPanel(this);
    addDockWidget(Qt::RightDockWidgetArea, registerPanel_);
    registerPanel_->hide();  // Hidden until a coredump is loaded
    connect(registerPanel_, &CoredumpRegisterPanel::addressClicked, this, &QemuLogViewer::onCoredumpAddressClicked);

    // --- Segment panel (right dock, tabified with registers) ---
    segmentPanel_ = new CoredumpSegmentPanel(this);
    addDockWidget(Qt::RightDockWidgetArea, segmentPanel_);
    tabifyDockWidget(registerPanel_, segmentPanel_);
    segmentPanel_->hide();

    connect(segmentPanel_, &CoredumpSegmentPanel::dumpSegmentRequested, this, [this](int /*idx*/, uint64_t vaStart, uint64_t vaEnd) {
        if (memoryPanel_) memoryPanel_->dumpRange(vaStart, vaEnd);
    });

    // --- ELF info panel (right dock, tabified) ---
    elfPanel_ = new CoredumpElfPanel(this);
    addDockWidget(Qt::RightDockWidgetArea, elfPanel_);
    tabifyDockWidget(segmentPanel_, elfPanel_);
    elfPanel_->hide();

    // --- Memory/Stack panel (bottom dock, tabbed with hex bytes) ---
    memoryPanel_ = new CoredumpMemoryPanel(this);
    addDockWidget(Qt::BottomDockWidgetArea, memoryPanel_);
    memoryPanel_->hide();
    connect(memoryPanel_, &CoredumpMemoryPanel::addressClicked, this, &QemuLogViewer::onCoredumpAddressClicked);

    // Tab memory panel with hex bytes dock
    if (hexDock_) {
        tabifyDockWidget(hexDock_, memoryPanel_);
        hexDock_->raise();  // Show hex bytes tab by default
    }
}

void QemuLogViewer::openCoredump(const QString& filePath) {
    // Parse the coredump binary
    auto dump = wosdbg::parseCoreDump(filePath);
    if (!dump) {
        statusLabel->setText(QString("Failed to open coredump: %1").arg(filePath));
        return;
    }

    currentCoreDump_ = std::move(dump);

    // Resolve symbols from filename + config
    resolveSymbolsForCoredump();

    // Build symbol source lists
    std::vector<wosdbg::SymbolTable*> symTables;
    std::vector<wosdbg::SectionMap*> sectionMaps;
    if (coreDumpSymtab_) symTables.push_back(coreDumpSymtab_.get());
    if (embeddedSymtab_) symTables.push_back(embeddedSymtab_.get());
    if (kernelSymtab_) symTables.push_back(kernelSymtab_.get());
    if (coreDumpSections_) sectionMaps.push_back(coreDumpSections_.get());
    if (embeddedSections_) sectionMaps.push_back(embeddedSections_.get());
    if (kernelSections_) sectionMaps.push_back(kernelSections_.get());

    // Update all panels
    registerPanel_->loadCoreDump(*currentCoreDump_, symTables, sectionMaps);
    registerPanel_->show();
    registerPanel_->raise();

    segmentPanel_->loadCoreDump(*currentCoreDump_);
    segmentPanel_->show();

    elfPanel_->setCoreDump(currentCoreDump_.get());
    elfPanel_->show();

    memoryPanel_->setCoreDump(currentCoreDump_.get(), symTables, sectionMaps);
    memoryPanel_->show();

    // Auto-dump the stack around RSP
    memoryPanel_->dumpStackAroundRsp();

    // Extract filename for status
    QFileInfo fi(filePath);
    statusLabel->setText(QString("Coredump: %1 | PID %2 | CPU %3 | Int %4")
                             .arg(fi.fileName())
                             .arg(currentCoreDump_->pid)
                             .arg(currentCoreDump_->cpu)
                             .arg(wosdbg::interruptName(currentCoreDump_->intNum)));
}

void QemuLogViewer::closeCoredump() {
    registerPanel_->clear();
    registerPanel_->hide();
    segmentPanel_->clear();
    segmentPanel_->hide();
    elfPanel_->clear();
    elfPanel_->hide();
    memoryPanel_->clear();
    memoryPanel_->hide();

    currentCoreDump_.reset();
    coreDumpSymtab_.reset();
    coreDumpSections_.reset();
    embeddedSymtab_.reset();
    embeddedSections_.reset();
    kernelSymtab_.reset();
    kernelSections_.reset();

    statusLabel->setText("Ready");
}

void QemuLogViewer::resolveSymbolsForCoredump() {
    if (!currentCoreDump_) return;

    const auto& cfg = ConfigService::instance().getConfig();

    // 1. Try to resolve from the coredump filename → binaryName → config ELF path
    QString binaryName = wosdbg::parseBinaryNameFromFilename(currentCoreDump_->sourceFilename);
    QString elfPath = cfg.findElfPathForBinary(binaryName);

    if (!elfPath.isEmpty()) {
        coreDumpSymtab_ = wosdbg::loadSymbolsFromFile(elfPath);
        coreDumpSections_ = wosdbg::loadSectionsFromFile(elfPath);
    }

    // 2. Try embedded ELF in the coredump itself
    QByteArray embeddedElf = currentCoreDump_->embeddedElf();
    if (!embeddedElf.isEmpty()) {
        embeddedSymtab_ = wosdbg::loadSymbolsFromCoreDump(*currentCoreDump_);
        embeddedSections_ = wosdbg::loadSectionsFromCoreDump(*currentCoreDump_);
    }

    // 3. Try kernel symbols from the address lookups in config
    const auto& lookups = cfg.getAddressLookups();
    for (const auto& lu : lookups) {
        if (lu.symbolFilePath.contains("kern") || lu.symbolFilePath.contains("wos")) {
            QString kernPath = cfg.resolvePath(lu.symbolFilePath);
            kernelSymtab_ = wosdbg::loadSymbolsFromFile(kernPath);
            kernelSections_ = wosdbg::loadSectionsFromFile(kernPath);
            break;
        }
    }

    // Update ELF panel with resolved info
    elfPanel_->setSymbolInfo(binaryName, elfPath, coreDumpSymtab_.get(), coreDumpSections_.get());
    if (embeddedSymtab_) elfPanel_->addSymbolSource("Embedded ELF", embeddedSymtab_.get(), embeddedSections_.get());
    if (kernelSymtab_) elfPanel_->addSymbolSource("Kernel", kernelSymtab_.get(), kernelSections_.get());
}

void QemuLogViewer::onCoredumpAddressClicked(uint64_t addr) {
    // Put the address in the navigation edit and jump to it in the log
    navigationEdit->setText(QString("0x%1").arg(addr, 16, 16, QChar('0')));
    jumpToAddress(addr);
}

void QemuLogViewer::browseCoredumpDirectory() {
    auto& svc = ConfigService::instance();
    const auto& cfg = svc.getConfig();
    QString dir = QFileDialog::getExistingDirectory(this, "Select Coredump Directory", cfg.getCoredumpDirectory());
    if (!dir.isEmpty()) {
        svc.getMutableConfig().setCoredumpDirectory(dir);
        svc.save();
        coredumpBrowser_->setDirectory(dir);
        statusLabel->setText(QString("Coredump directory: %1").arg(dir));
    }
}

void QemuLogViewer::extractCoredumps() {
    coredumpBrowser_->extractCoredumps();
    statusLabel->setText("Extracting coredumps...");
}

void QemuLogViewer::refreshCoredumps() {
    coredumpBrowser_->refresh();
    statusLabel->setText("Coredump list refreshed");
}
