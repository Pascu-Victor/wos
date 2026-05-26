#include "wosdbg.h"

#include <qboxlayout.h>
#include <qcombobox.h>
#include <qcontainerfwd.h>
#include <qcoreevent.h>
#include <qfileinfo.h>
#include <qforeach.h>
#include <qitemselectionmodel.h>
#include <qlineedit.h>
#include <qlogging.h>
#include <qmainwindow.h>
#include <qnamespace.h>
#include <qobject.h>
#include <qoverload.h>
#include <qpalette.h>
#include <qprogressbar.h>
#include <qpushbutton.h>
#include <qscrollbar.h>
#include <qstyleoption.h>
#include <qtablewidget.h>
#include <qtextedit.h>
#include <qtpreprocessorsupport.h>
#include <qtreewidget.h>
#include <qwidget.h>

#include <QDir>
#include <algorithm>
#include <climits>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "config.h"
#include "log_client.h"
#include "log_entry.h"
#include "virtual_table.h"
#include "virtual_table_integration.h"

// Work around BFD config.h requirement
#define PACKAGE "wosdbg"
#define PACKAGE_VERSION "1.0"
extern "C" {
#include <bfd.h>
}

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
#include <sstream>

#include "capstone_disasm.h"

// Include our existing processing functions
#include <cstdlib>

// CapstoneDisassembler: see capstone_disasm.h / capstone_disasm.cpp

// Syntax Highlighter for C/C++ and Assembly
class SyntaxHighlighter : public QSyntaxHighlighter {
   public:
    explicit SyntaxHighlighter(QTextDocument* parent = nullptr) : QSyntaxHighlighter(parent) { setup_highlighting_rules(); }

   protected:
    void highlightBlock(const QString& text) override {
        // Apply each rule to the text
        foreach (const HighlightingRule& rule, highlighting_rules) {
            QRegularExpressionMatchIterator match_iterator = rule.pattern.globalMatch(text);
            while (match_iterator.hasNext()) {
                QRegularExpressionMatch match = match_iterator.next();
                setFormat(match.capturedStart(), match.capturedLength(), rule.format);
            }
        }
    }

   private:
    struct HighlightingRule {
        QRegularExpression pattern;
        QTextCharFormat format;
    };
    QVector<HighlightingRule> highlighting_rules;

    void setup_highlighting_rules() {
        HighlightingRule rule;

        // C/C++ Keywords (using brighter VS Code blue for better contrast)
        QTextCharFormat keyword_format;
        keyword_format.setForeground(QColor("#79C3FF"));  // Brighter VS Code keyword blue
        keyword_format.setFontWeight(QFont::Bold);
        QStringList keyword_patterns;
        keyword_patterns << "\\bif\\b" << "\\belse\\b" << "\\bfor\\b" << "\\bwhile\\b"
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

        foreach (const QString& pattern, keyword_patterns) {
            rule.pattern = QRegularExpression(pattern);
            rule.format = keyword_format;
            highlighting_rules.append(rule);
        }

        // Storage class specifiers (using distinct purple)
        QTextCharFormat storage_format;
        storage_format.setForeground(QColor("#E586FF"));  // Bright purple for storage
        storage_format.setFontWeight(QFont::Bold);
        QStringList storage_patterns;
        storage_patterns << "\\bstatic\\b" << "\\bextern\\b" << "\\bregister\\b" << "\\bthread_local\\b"
                         << "\\bmutable\\b" << "\\bconstexpr\\b" << "\\bconsteval\\b" << "\\bconstinit\\b";

        foreach (const QString& pattern, storage_patterns) {
            rule.pattern = QRegularExpression(pattern);
            rule.format = storage_format;
            highlighting_rules.append(rule);
        }

        // Assembly instructions (using brighter teal)
        QTextCharFormat asm_instruction_format;
        asm_instruction_format.setForeground(QColor("#5DD9C0"));  // Brighter VS Code class/type teal
        asm_instruction_format.setFontWeight(QFont::Bold);
        QStringList asm_patterns;
        // Basic x86-64 instructions
        asm_patterns << "\\bmov\\b" << "\\bpush\\b" << "\\bpop\\b" << "\\bcall\\b" << "\\bret\\b"
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

        foreach (const QString& pattern, asm_patterns) {
            rule.pattern = QRegularExpression(pattern);
            rule.format = asm_instruction_format;
            highlighting_rules.append(rule);
        }

        // Registers (using brighter variable blue)
        QTextCharFormat register_format;
        register_format.setForeground(QColor("#B8E6FF"));  // Brighter VS Code variable blue
        rule.pattern = QRegularExpression(
            "\\b[re]?[a-d]x\\b|\\b[re]?[sd]i\\b|\\b[re]?[sb]p\\b|\\br[8-9]\\b|\\br1[0-5]\\b|\\beax\\b|\\bebx\\b|\\becx\\b|\\bedx\\b|"
            "\\besi\\b|\\bedi\\b|\\besp\\b|\\bebp\\b|\\beip\\b|\\brip\\b|\\bcs\\b|\\bds\\b|\\bes\\b|\\bfs\\b|\\bgs\\b|\\bss\\b|\\bmm[0-7]"
            "\\b|\\bxmm[0-9]\\b|\\bxmm1[0-5]\\b|\\bxmm[23][0-9]\\b|\\bxmm3[01]\\b|\\bymm[0-9]\\b|\\bymm1[0-5]\\b|\\bymm[23][0-9]\\b|"
            "\\bymm3[01]\\b|\\bzmm[0-9]\\b|\\bzmm1[0-5]\\b|\\bzmm[23][0-9]\\b|\\bzmm3[01]\\b|\\bk[0-7]\\b|\\bst[0-7]\\b|\\bcr[0-8]\\b|"
            "\\bdr[0-7]\\b");
        rule.format = register_format;
        highlighting_rules.append(rule);

        // Numbers (hex and decimal) (using brighter number green)
        QTextCharFormat number_format;
        number_format.setForeground(QColor("#C8E6B8"));  // Brighter VS Code number green
        rule.pattern = QRegularExpression(R"(\b0x[0-9a-fA-F]+\b|\b[0-9]+\b|\$0x[0-9a-fA-F]+|\$[0-9]+)");
        rule.format = number_format;
        highlighting_rules.append(rule);

        // Special characters and operators (using bright orange)
        QTextCharFormat special_format;
        special_format.setForeground(QColor("#FF9A6B"));  // Bright orange for operators
        special_format.setFontWeight(QFont::Bold);
        rule.pattern = QRegularExpression(
            "[\\+\\-\\*\\/"
            "\\%\\=\\!\\<\\>\\&\\|\\^\\~\\?\\:\\;\\,]|\\+\\+|\\-\\-|\\<\\<|\\>\\>|\\=\\=|\\!\\=|\\<\\=|\\>\\=|\\&\\&|\\|\\||\\+\\=|\\-\\=|"
            "\\*\\=|\\/\\=|\\%\\=|\\&\\=|\\|\\=|\\^\\=|\\<\\<\\=|\\>\\>\\=|\\-\\>|\\:\\:");
        rule.format = special_format;
        highlighting_rules.append(rule);

        // Brackets and parentheses (using bright cyan)
        QTextCharFormat bracket_format;
        bracket_format.setForeground(QColor("#00E5FF"));  // Bright cyan for brackets
        bracket_format.setFontWeight(QFont::Bold);
        rule.pattern = QRegularExpression(R"([\(\)\[\]\{\}])");
        rule.format = bracket_format;
        highlighting_rules.append(rule);

        // Memory addresses (using bright orange)
        QTextCharFormat memory_format;
        memory_format.setForeground(QColor("#FFD68A"));  // Brighter VS Code string orange
        rule.pattern = QRegularExpression(R"(\[[^\]]+\]|\([^\)]+\))");
        rule.format = memory_format;
        highlighting_rules.append(rule);

        // Comments (using brighter comment green)
        QTextCharFormat comment_format;
        comment_format.setForeground(QColor("#7CB555"));  // Brighter VS Code comment green
        comment_format.setFontItalic(true);
        rule.pattern = QRegularExpression("//[^\n]*|/\\*.*\\*/|#[^\n]*");
        rule.format = comment_format;
        highlighting_rules.append(rule);

        // Strings (using brighter string color)
        QTextCharFormat string_format;
        string_format.setForeground(QColor("#E6B678"));  // Brighter VS Code string brown
        rule.pattern = QRegularExpression("\".*?\"|'.*?'");
        rule.format = string_format;
        highlighting_rules.append(rule);

        // Function names (using brighter function yellow)
        QTextCharFormat function_format;
        function_format.setForeground(QColor("#FFE86A"));  // Brighter VS Code function yellow
        rule.pattern = QRegularExpression(R"(\b[A-Za-z_][A-Za-z0-9_]*(?=\s*\())");
        rule.format = function_format;
        highlighting_rules.append(rule);

        // Types (using brighter type teal)
        QTextCharFormat type_format;
        type_format.setForeground(QColor("#5DD9C0"));  // Brighter VS Code type teal
        QStringList type_patterns;
        type_patterns << "\\bint\\b" << "\\bchar\\b" << "\\bfloat\\b" << "\\bdouble\\b"
                      << "\\blong\\b" << "\\bshort\\b" << "\\bunsigned\\b" << "\\bsigned\\b"
                      << "\\bbool\\b" << "\\bvoid\\b" << "\\bsize_t\\b" << "\\buint8_t\\b"
                      << "\\buint16_t\\b" << "\\buint32_t\\b" << "\\buint64_t\\b"
                      << "\\bint8_t\\b" << "\\bint16_t\\b" << "\\bint32_t\\b" << "\\bint64_t\\b"
                      << "\\bssize_t\\b" << "\\bptrdiff_t\\b" << "\\bintptr_t\\b" << "\\buintptr_t\\b"
                      << "\\bwchar_t\\b" << "\\bchar16_t\\b" << "\\bchar32_t\\b" << "\\bchar8_t\\b";

        foreach (const QString& pattern, type_patterns) {
            rule.pattern = QRegularExpression(pattern);
            rule.format = type_format;
            highlighting_rules.append(rule);
        }

        // Preprocessor directives (using brighter preprocessor purple)
        QTextCharFormat preprocessor_format;
        preprocessor_format.setForeground(QColor("#D586C0"));  // Brighter VS Code preprocessor purple
        rule.pattern = QRegularExpression("^\\s*#\\w+");
        rule.format = preprocessor_format;
        highlighting_rules.append(rule);

        // Line numbers and addresses (using brighter number color)
        QTextCharFormat line_number_format;
        line_number_format.setForeground(QColor("#C8E6B8"));
        rule.pattern = QRegularExpression("^\\s*\\d+:");
        rule.format = line_number_format;
        highlighting_rules.append(rule);

        // Exception/Error keywords (using brighter error red)
        QTextCharFormat error_format;
        error_format.setForeground(QColor("#FF6B6B"));  // Brighter VS Code error red
        error_format.setFontWeight(QFont::Bold);
        QStringList error_patterns;
        error_patterns << "\\bERROR\\b" << "\\bFAIL\\b" << "\\bFATAL\\b" << "\\bPANIC\\b"
                       << "\\bEXCEPTION\\b" << "\\bSEGFAULT\\b" << "\\bCRASH\\b" << "\\bASSERT\\b"
                       << "\\bABORT\\b" << "\\bWARNING\\b" << "\\bWARN\\b";

        foreach (const QString& pattern, error_patterns) {
            rule.pattern = QRegularExpression(pattern);
            rule.format = error_format;
            highlighting_rules.append(rule);
        }

        // Macros and constants (using bright magenta)
        QTextCharFormat macro_format;
        macro_format.setForeground(QColor("#FF79C6"));  // Bright magenta for macros
        macro_format.setFontWeight(QFont::Bold);
        rule.pattern = QRegularExpression("\\b[A-Z_][A-Z0-9_]{2,}\\b");
        rule.format = macro_format;
        highlighting_rules.append(rule);
    }
};

// Custom delegate for syntax highlighting in table cells
class SyntaxHighlightDelegate : public QStyledItemDelegate {
   public:
    explicit SyntaxHighlightDelegate(QObject* parent = nullptr) : QStyledItemDelegate(parent) { setup_formats(); }

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
                QStyleOptionFocusRect focus_option;
                focus_option.rect = option.rect;
                focus_option.palette = option.palette;
                focus_option.state = option.state;
                QApplication::style()->drawPrimitive(QStyle::PE_FrameFocusRect, &focus_option, painter);
            }

            painter->restore();

            // Now draw our custom highlighted text
            paint_highlighted_text(painter, option, text, column);
        } else {
            // For other columns, use default rendering
            QStyledItemDelegate::paint(painter, option, index);
        }
    }

   private:
    QTextCharFormat keyword_format;
    QTextCharFormat asm_instruction_format;
    QTextCharFormat register_format;
    QTextCharFormat number_format;
    QTextCharFormat function_format;
    QTextCharFormat type_format;

    void setup_formats() {
        // Keywords
        keyword_format.setForeground(QColor(86, 156, 214));  // Light blue
        keyword_format.setFontWeight(QFont::Bold);

        // Assembly instructions
        asm_instruction_format.setForeground(QColor(78, 201, 176));  // Light teal
        asm_instruction_format.setFontWeight(QFont::Bold);

        // Registers
        register_format.setForeground(QColor(220, 220, 170));  // Light yellow

        // Numbers
        number_format.setForeground(QColor(181, 206, 168));  // Light green

        // Functions
        function_format.setForeground(QColor(220, 220, 170));  // Light yellow

        // Types
        type_format.setForeground(QColor(78, 201, 176));  // Light teal
    }

    void paint_highlighted_text(QPainter* painter, const QStyleOptionViewItem& option, const QString& text, int column) const {
        if (text.isEmpty()) {
            return;
        }

        QRect text_rect = option.rect.adjusted(4, 2, -4, -2);
        painter->save();
        painter->setClipRect(text_rect);

        QFont font = option.font;
        painter->setFont(font);

        // Use elided text to prevent overflow
        QFontMetrics fm(font);
        QString elided_text = fm.elidedText(text, Qt::ElideRight, text_rect.width());

        // Apply syntax highlighting based on column
        if (column == 5) {  // Assembly column (updated index)
            paint_assembly_highlighting(painter, text_rect, elided_text, font);
        } else if (column == 3) {  // Function column
            paint_function_highlighting(painter, text_rect, elided_text, font);
        }

        painter->restore();
    }

    static void paint_assembly_highlighting(QPainter* painter, const QRect& rect, const QString& text, const QFont& font) {
        QFontMetrics fm(font);
        int x = rect.x();
        int y = rect.y() + fm.ascent() + ((rect.height() - fm.height()) / 2);

        // Simple word-by-word highlighting for assembly
        QStringList words = text.split(QRegularExpression("\\s+"), Qt::SkipEmptyParts);

        for (const QString& word : words) {
            if (x >= rect.right()) {
                break;  // Stop if we're outside the rect
            }

            QColor color = get_assembly_word_color(word);
            painter->setPen(color);

            int word_width = fm.horizontalAdvance(word);
            if (x + word_width > rect.right()) {
                // Truncate the word if it would overflow
                QString truncated = fm.elidedText(word, Qt::ElideRight, rect.right() - x);
                painter->drawText(x, y, truncated);
                break;
            }

            painter->drawText(x, y, word);
            x += word_width + fm.horizontalAdvance(" ");
        }
    }

    static void paint_function_highlighting(QPainter* painter, const QRect& rect, const QString& text, const QFont& font) {
        QFontMetrics fm(font);
        Q_UNUSED(fm)  // Suppress unused variable warning

        // Highlight function names (VS Code function yellow)
        auto color = QColor("#DCDCAA");  // VS Code function yellow
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

    [[nodiscard]] static QColor get_assembly_word_color(const QString& word) {
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
            return {"#4EC9B0"};  // VS Code class/type teal
        }

        // Registers (VS Code variable blue) - Extended x86-64 register set
        QRegularExpression reg_regex(
            "^[re]?[a-d]x$|^[re]?[sd]i$|^[re]?[sb]p$|^r[8-9]$|^r1[0-5]$|^eax$|^ebx$|^ecx$|^edx$|^esi$|^edi$|^esp$|^ebp$|^eip$|^rip$|^cs$|^"
            "ds$|^es$|^fs$|^gs$|^ss$|^mm[0-7]$|^[xy]mm[0-9]$|^[xy]mm1[0-5]$|^[xy]mm[23][0-9]$|^[xy]mm3[01]$|^zmm[0-9]$|^zmm1[0-5]$|^zmm[23]"
            "[0-9]$|^zmm3[01]$|^k[0-7]$|^st[0-7]$|^cr[0-8]$|^dr[0-7]$");
        if (reg_regex.match(word.toLower()).hasMatch()) {
            return {"#9CDCFE"};  // VS Code variable blue
        }

        // Numbers (hex and decimal) (VS Code number green)
        QRegularExpression num_regex("^\\$?0x[0-9a-fA-F]+$|^\\$?[0-9]+$");
        if (num_regex.match(word).hasMatch()) {
            return {"#B5CEA8"};  // VS Code number green
        }

        // Memory addresses and brackets (VS Code string color)
        if (word.contains('[') || word.contains(']') || word.contains('(') || word.contains(')')) {
            return {"#D7BA7D"};  // VS Code string orange
        }

        // Function names with offset (VS Code function yellow)
        if (word.contains('+') && word.contains("0x")) {
            return {"#DCDCAA"};  // VS Code function yellow
        }

        // Default color (VS Code default text)
        return {"#D4D4D4"};  // VS Code default text
    }
};

QemuLogViewer::QemuLogViewer(LogClient* client, QWidget* parent)
    : QMainWindow(parent), current_search_index(-1), pre_search_position(-1), search_active(false), client(client), next_string_buffer(0) {
    disassembler = std::make_unique<CapstoneDisassembler>();

    // Initialize search debounce timer
    search_debounce_timer = new QTimer(this);
    search_debounce_timer->setSingleShot(true);
    search_debounce_timer->setInterval(300);  // 300ms debounce
    connect(search_debounce_timer, &QTimer::timeout, this, &QemuLogViewer::perform_debounced_search);

    // Initialize performance optimizations first
    initialize_performance_optimizations();

    setup_dark_theme();
    setup_ui();

    // Set up syntax highlighters after UI is created
    disassembly_highlighter = new SyntaxHighlighter(disassembly_view->document());
    details_highlighter = new SyntaxHighlighter(details_pane->document());

    // Set up table syntax highlighting delegate
    table_delegate = new SyntaxHighlightDelegate(this);
    // logTable is VirtualTableView, it uses delegate?
    // VirtualTableView uses QStyledItemDelegate internally or custom painting?
    // VirtualTableView inherits QTableView.
    // We can set delegate on it.
    log_table->setItemDelegate(table_delegate);

    connect_signals();

    // Connect client signals
    connect(client, &LogClient::file_list_received, this, &QemuLogViewer::on_file_list_received);
    connect(client, &LogClient::config_received, this, &QemuLogViewer::on_config_received);
    connect(client, &LogClient::file_ready, this, &QemuLogViewer::on_file_ready);
    connect(client, &LogClient::data_received, this, &QemuLogViewer::on_data_received);
    connect(client, &LogClient::search_results, this, &QemuLogViewer::on_search_results);
    connect(client, &LogClient::interrupts_received, this, &QemuLogViewer::on_interrupts_received);
    connect(client, &LogClient::filter_applied, this, &QemuLogViewer::on_filter_applied);
    connect(client, &LogClient::progress, this, &QemuLogViewer::on_progress_update);
    connect(client, &LogClient::mcp_server_status, this, &QemuLogViewer::on_mcp_server_status);

    // Initialize interrupt navigation state
    current_selected_interrupt.clear();
    current_interrupt_index = -1;

    client->request_mcp_server_status();
}

QemuLogViewer::~QemuLogViewer() {
    // Qt manages QTableWidgetItem cleanup automatically
    // LogProcessor now manages QProcess objects internally
    // No manual cleanup needed
}

auto QemuLogViewer::eventFilter(QObject* obj, QEvent* event) -> bool {
    if (obj == search_edit && event->type() == QEvent::KeyPress) {
        auto* key_event = static_cast<QKeyEvent*>(event);
        if (key_event->key() == Qt::Key_Escape) {
            cancel_search();
            return true;  // Event handled
        }
        if (key_event->key() == Qt::Key_Return || key_event->key() == Qt::Key_Enter) {
            // Handle Enter and Shift+Enter for search navigation
            if (key_event->modifiers() & Qt::ShiftModifier) {
                // Shift+Enter: go to previous match
                if (!search_matches.empty()) {
                    on_search_previous();
                }
            } else {
                // Enter: go to next match
                if (!search_matches.empty()) {
                    on_search_next();
                }
            }
            return true;  // Event handled
        }
    }
    return QMainWindow::eventFilter(obj, event);
}

void QemuLogViewer::cancel_search() {
    if (search_active && pre_search_position >= 0) {
        // Return to the original position
        scroll_to_row(pre_search_position);
        search_active = false;
        pre_search_position = -1;

        // Clear search text and reset UI
        search_edit->clear();
        search_matches.clear();
        current_search_index = -1;
        search_next_btn->setEnabled(false);
        search_prev_btn->setEnabled(false);
        highlight_search_matches();

        status_label->setText("Search cancelled");
    }
}

void QemuLogViewer::setup_dark_theme() {
    // Use system theme detection
    QPalette palette = QApplication::palette();
    bool is_dark_theme = palette.color(QPalette::Window).lightness() < 128;

    if (!is_dark_theme) {
        // If system is not dark, apply our dark theme for better code viewing
        is_dark_theme = true;
    }

    if (is_dark_theme) {
        apply_theme("dark");
    }
    // If not dark theme, use default system styling
}

void QemuLogViewer::apply_theme(const QString& theme_name) {
    QString style_sheet;

    if (theme_name == "dark") {
        style_sheet = get_dark_theme_css();
    } else if (theme_name == "light") {
        style_sheet = get_light_theme_css();
    } else if (theme_name == "high-contrast") {
        style_sheet = get_high_contrast_theme_css();
    }

    setStyleSheet(style_sheet);
}

QString QemuLogViewer::get_dark_theme_css() {
    return {R"(
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
    )"};
}

QString QemuLogViewer::get_light_theme_css() {
    return {R"(
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
    )"};
}

QString QemuLogViewer::get_high_contrast_theme_css() {
    return {R"(
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
    )"};
}

void QemuLogViewer::setup_ui() {
    setWindowTitle("wosdbg - WOS Debugger");
    setMinimumSize(1200, 800);
    resize(1600, 1000);

    setup_toolbar();
    setup_main_content();
    setup_coredump_panels();
}

void QemuLogViewer::setup_toolbar() {
    toolbar = addToolBar("Main");
    toolbar->setMovable(false);

    // File selector
    toolbar->addWidget(new QLabel("Log File:"));
    file_selector = new QComboBox();
    file_selector->setMinimumWidth(200);
    toolbar->addWidget(file_selector);

    refresh_files_btn = new QPushButton("↻");
    refresh_files_btn->setToolTip("Refresh file list");
    toolbar->addWidget(refresh_files_btn);

    toolbar->addSeparator();

    // Search controls
    toolbar->addWidget(new QLabel("Search:"));
    search_edit = new QLineEdit();
    search_edit->setPlaceholderText("Search addresses, functions, assembly... (Enter: next, Shift+Enter: prev, Esc: cancel)");
    search_edit->setMinimumWidth(300);
    toolbar->addWidget(search_edit);

    regex_checkbox = new QCheckBox("Regex");
    toolbar->addWidget(regex_checkbox);

    hide_structural_checkbox = new QCheckBox("Hide Structural");
    hide_structural_checkbox->setToolTip("Hide SEPARATOR and BLOCK entries");
    hide_structural_checkbox->setChecked(true);  // Enabled by default
    toolbar->addWidget(hide_structural_checkbox);

    search_prev_btn = new QPushButton("◀");
    search_prev_btn->setToolTip("Previous match (Shift+Enter)");
    search_prev_btn->setEnabled(false);
    toolbar->addWidget(search_prev_btn);

    search_next_btn = new QPushButton("▶");
    search_next_btn->setToolTip("Next match (Enter)");
    search_next_btn->setEnabled(false);
    toolbar->addWidget(search_next_btn);

    toolbar->addSeparator();

    // Navigation
    toolbar->addWidget(new QLabel("Go to:"));
    navigation_edit = new QLineEdit();
    navigation_edit->setPlaceholderText("Address (0x...) or Line number");
    navigation_edit->setMinimumWidth(200);
    toolbar->addWidget(navigation_edit);

    toolbar->addSeparator();

    // Interrupt filter dropdown
    toolbar->addWidget(new QLabel("Interrupts:"));
    interrupt_filter_combo = new QComboBox();
    interrupt_filter_combo->setMinimumWidth(200);
    interrupt_filter_combo->setToolTip("Filter interrupts by number (shows only present interrupts)");
    toolbar->addWidget(interrupt_filter_combo);
    // Navigation buttons for interrupt occurrences
    interrupt_prev_btn = new QPushButton("◀");
    interrupt_prev_btn->setToolTip("Previous interrupt occurrence");
    interrupt_prev_btn->setEnabled(false);
    toolbar->addWidget(interrupt_prev_btn);

    interrupt_next_btn = new QPushButton("▶");
    interrupt_next_btn->setToolTip("Next interrupt occurrence");
    interrupt_next_btn->setEnabled(false);
    toolbar->addWidget(interrupt_next_btn);

    only_interrupts_checkbox = new QCheckBox("Only interrupts");
    only_interrupts_checkbox->setToolTip("When checked, table shows only interrupt entries");
    toolbar->addWidget(only_interrupts_checkbox);

    toolbar->addSeparator();

    // Status
    progress_bar = new QProgressBar();
    progress_bar->setVisible(false);
    progress_bar->setMaximumWidth(200);
    toolbar->addWidget(progress_bar);

    status_label = new QLabel("Ready");
    toolbar->addWidget(status_label);

    toolbar->addSeparator();

    // Coredump controls
    auto* coredump_dir_btn = new QPushButton("Coredumps");
    coredump_dir_btn->setToolTip("Browse/change coredump directory");
    connect(coredump_dir_btn, &QPushButton::clicked, this, &QemuLogViewer::browse_coredump_directory);
    toolbar->addWidget(coredump_dir_btn);

    auto* extract_btn = new QPushButton("Extract");
    extract_btn->setToolTip("Extract coredumps from disk images");
    connect(extract_btn, &QPushButton::clicked, this, &QemuLogViewer::extract_coredumps);
    toolbar->addWidget(extract_btn);

    auto* refresh_dumps_btn = new QPushButton("🔄");
    refresh_dumps_btn->setToolTip("Refresh coredump list");
    connect(refresh_dumps_btn, &QPushButton::clicked, this, &QemuLogViewer::refresh_coredumps);
    toolbar->addWidget(refresh_dumps_btn);

    toolbar->addSeparator();

    mcp_toggle_btn = new QPushButton("MCP Off");
    mcp_toggle_btn->setToolTip("Start the MCP server on the active wosdbg backend");
    connect(mcp_toggle_btn, &QPushButton::clicked, this, &QemuLogViewer::on_mcp_toggle);
    toolbar->addWidget(mcp_toggle_btn);
}

void QemuLogViewer::setup_main_content() {
    // Central widget: log table only
    auto* central_widget = new QWidget();
    setCentralWidget(central_widget);
    auto* layout = new QVBoxLayout(central_widget);
    layout->setContentsMargins(0, 0, 0, 0);

    setup_table();
    layout->addWidget(log_table);

    // --- Interrupts panel (left dock) ---
    interrupts_panel = new QTreeWidget();
    interrupts_panel->setHeaderLabels(QStringList() << "Interrupt" << "Occurrences");
    interrupts_panel->setMinimumWidth(200);
    interrupts_panel->setSelectionMode(QAbstractItemView::SingleSelection);

    interrupts_dock = new QDockWidget("Interrupts", this);
    interrupts_dock->setWidget(interrupts_panel);
    interrupts_dock->setAllowedAreas(Qt::LeftDockWidgetArea | Qt::RightDockWidgetArea);
    addDockWidget(Qt::LeftDockWidgetArea, interrupts_dock);

    // --- Hex bytes (bottom dock) ---
    hex_view = new QTextEdit();
    hex_view->setReadOnly(true);
    hex_view->setFont(QFont("Consolas", 12));
    hex_view->setPlaceholderText("Hex bytes will be displayed here...");

    hex_dock = new QDockWidget("Hex Bytes", this);
    hex_dock->setWidget(hex_view);
    hex_dock->setAllowedAreas(Qt::AllDockWidgetAreas);
    addDockWidget(Qt::BottomDockWidgetArea, hex_dock);

    // --- Disassembly (right dock) ---
    disassembly_view = new QTextEdit();
    disassembly_view->setReadOnly(true);
    disassembly_view->setFont(QFont("Consolas", 12));
    disassembly_view->setPlaceholderText("Detailed disassembly will be displayed here...");

    disassembly_dock = new QDockWidget("Disassembly", this);
    disassembly_dock->setWidget(disassembly_view);
    disassembly_dock->setAllowedAreas(Qt::AllDockWidgetAreas);
    addDockWidget(Qt::RightDockWidgetArea, disassembly_dock);

    // --- Interrupt details (right dock, stacked below disassembly) ---
    details_pane = new QTextBrowser();
    details_pane->setReadOnly(true);
    details_pane->setOpenExternalLinks(false);
    details_pane->setOpenLinks(false);
    details_pane->setFont(QFont("Consolas", 12));
    details_pane->setPlaceholderText("Interrupt details will be displayed here...");

    details_dock = new QDockWidget("Interrupt Details", this);
    details_dock->setWidget(details_pane);
    details_dock->setAllowedAreas(Qt::AllDockWidgetAreas);
    addDockWidget(Qt::RightDockWidgetArea, details_dock);

    // Stack disassembly and details vertically in the right dock area
    splitDockWidget(disassembly_dock, details_dock, Qt::Vertical);
}

void QemuLogViewer::setup_table() {
    // Use integration helper to create virtual table
    log_table = VirtualTableIntegration::initialize_virtual_table(this, client);
    virtual_table_model = static_cast<VirtualTableModel*>(log_table->model());
    log_table->setMouseTracking(true);

    // Connect signals
    connect(log_table, &QTableView::clicked, this,
            [this](const QModelIndex& index) { on_table_cell_clicked(index.row(), index.column()); });

    // Connect selection changed
    connect(log_table->selectionModel(), &QItemSelectionModel::selectionChanged, this, &QemuLogViewer::on_table_selection_changed);
}

void QemuLogViewer::connect_signals() {
    connect(file_selector, QOverload<const QString&>::of(&QComboBox::currentTextChanged), this, &QemuLogViewer::on_file_selected);
    connect(refresh_files_btn, &QPushButton::clicked, client, &LogClient::request_file_list);

    connect(search_edit, &QLineEdit::textChanged, this, &QemuLogViewer::on_search_text_changed);

    // Install event filter for Esc key handling
    search_edit->installEventFilter(this);

    connect(search_next_btn, &QPushButton::clicked, this, &QemuLogViewer::on_search_next);

    connect(search_prev_btn, &QPushButton::clicked, this, &QemuLogViewer::on_search_previous);

    connect(regex_checkbox, &QCheckBox::toggled, this, &QemuLogViewer::on_regex_toggled);

    connect(hide_structural_checkbox, &QCheckBox::toggled, this, &QemuLogViewer::on_hide_structural_toggled);

    connect(navigation_edit, &QLineEdit::returnPressed, [this]() {
        QString text = navigation_edit->text().trimmed();
        if (text.isEmpty()) {
            return;
        }

        if (is_address_input(text)) {
            bool ok;
            uint64_t addr = text.toULongLong(&ok, 16);
            if (ok) {
                jump_to_address(addr);
            }
        } else {
            bool ok;
            int line = text.toInt(&ok);
            if (ok && line > 0) {
                jump_to_line(line);
            }
        }
    });

    // Virtual table selection handling
    connect(log_table->selectionModel(), &QItemSelectionModel::selectionChanged, this,
            [this](const QItemSelection& selected, const QItemSelection&) {
                if (!selected.isEmpty()) {
                    int row = selected.first().top();
                    update_details_pane(row);
                }
            });

    // Virtual table click handler
    connect(log_table, &VirtualTableView::clicked, this, [this](const QModelIndex& index) {
        if (index.isValid()) {
            update_details_pane(index.row());
        }
    });

    // Sync scroll bars
    connect(log_table->verticalScrollBar(), &QScrollBar::valueChanged, this, &QemuLogViewer::sync_scroll_bars);

    // Handle VSCode link clicks
    connect(details_pane, &QTextBrowser::anchorClicked, this, &QemuLogViewer::on_details_pane_link_clicked);

    // Interrupt filter changed
    connect(interrupt_filter_combo, &QComboBox::currentTextChanged, this, &QemuLogViewer::on_interrupt_filter_changed);
    connect(interrupt_next_btn, &QPushButton::clicked, this, &QemuLogViewer::on_interrupt_next);
    connect(interrupt_prev_btn, &QPushButton::clicked, this, &QemuLogViewer::on_interrupt_previous);
    connect(only_interrupts_checkbox, &QCheckBox::toggled, this,
            &QemuLogViewer::on_hide_structural_toggled);  // reuse hideStructural logic for simplicity
    connect(interrupts_panel, &QTreeWidget::itemActivated, this, &QemuLogViewer::on_interrupt_panel_activated);

    // Right-click to fold/unfold interrupts
    connect(interrupts_panel, &QTreeWidget::customContextMenuRequested, [this](const QPoint& pos) {
        QTreeWidgetItem* item = interrupts_panel->itemAt(pos);
        if (item) {
            on_interrupt_toggle_fold(item, 0);
        }
    });
    interrupts_panel->setContextMenuPolicy(Qt::CustomContextMenu);

    // Connect row for line received signal
    connect(client, &LogClient::row_for_line_received, this, &QemuLogViewer::on_row_for_line_received);
}

void QemuLogViewer::load_log_files() {
    file_selector->clear();

    QDir dir(".");
    QStringList filters;
    filters << "*.log";

    auto files = dir.entryList(filters, QDir::Files, QDir::Name);

    if (files.isEmpty()) {
        status_label->setText("No log files found in current directory");
        return;
    }

    // Prioritize .modified.log files
    std::ranges::sort(files, [](const QString& a, const QString& b) {
        bool a_modified = a.contains(".modified.");
        bool b_modified = b.contains(".modified.");
        if (a_modified != b_modified) {
            return a_modified > b_modified;
        }
        return a < b;
    });

    file_selector->addItems(files);

    // Show config status in the status label
    const Config& config = ConfigService::instance().get_config();
    const auto& lookups = config.get_address_lookups();
    bool config_exists = ConfigService::instance().config_file_exists();

    QString status_text = QString("Found %1 log files").arg(files.size());
    if (config_exists) {
        status_text += QString(" • Config: %1 symbol lookups loaded").arg(lookups.size());
    } else {
        status_text += QString(" • Config: Using defaults (%1 lookups)").arg(lookups.size());
    }

    status_label->setText(status_text);
}

void QemuLogViewer::on_file_selected(const QString& filename) {
    if (filename.isEmpty()) {
        return;
    }

    // Disable UI
    file_selector->setEnabled(false);
    search_edit->setEnabled(false);
    navigation_edit->setEnabled(false);
    log_table->setEnabled(false);

    status_label->setText("Requesting file...");
    progress_bar->setVisible(true);
    progress_bar->setValue(0);
    progress_bar->setFormat("Requesting file... %p%");

    // Clear current data
    if (virtual_table_model) {
        virtual_table_model->setRowCount(0);
    }
    search_matches.clear();
    current_search_index = -1;

    // Clear views
    hex_view->clear();
    disassembly_view->clear();
    details_pane->clear();

    // Request file from client
    client->select_file(filename);
}

void QemuLogViewer::on_progress_update(int percentage) {
    progress_bar->setValue(percentage);
    progress_bar->setFormat("Processing... %p%");
}

void QemuLogViewer::on_file_ready(int total_lines) {
    qDebug() << "QemuLogViewer::on_file_ready totalLines=" << total_lines;
    // Update model
    if (virtual_table_model) {
        virtual_table_model->setRowCount(total_lines);
    }

    // Re-enable UI
    file_selector->setEnabled(true);
    search_edit->setEnabled(true);
    navigation_edit->setEnabled(true);
    log_table->setEnabled(true);

    progress_bar->setVisible(false);
    status_label->setText(QString("Loaded %1 lines").arg(total_lines));

    // Request interrupts
    client->request_interrupts();
}

void QemuLogViewer::on_file_list_received(const QStringList& files) {
    QString current = file_selector->currentText();
    file_selector->blockSignals(true);
    file_selector->clear();
    file_selector->addItems(files);

    int index = file_selector->findText(current);
    if (index != -1) {
        file_selector->setCurrentIndex(index);
    }

    file_selector->blockSignals(false);

    if (!files.isEmpty()) {
        status_label->setText(QString("Found %1 log files").arg(files.size()));
    }
}

void QemuLogViewer::on_config_received() {
    const auto& lookups = client->get_config().get_address_lookups();
    status_label->setText(status_label->text() + QString(" • Config: %1 symbol lookups").arg(lookups.size()));
}

void QemuLogViewer::on_data_received(int start_line, int count) {
    if (virtual_table_model) {
        virtual_table_model->invalidateRows(start_line, start_line + count - 1);
        // Force repaint to ensure "Loading..." is replaced immediately
        log_table->viewport()->update();
    }

    // Check if currently selected row is in range and update details pane if so
    if (log_table->selectionModel()->hasSelection()) {
        int current_row = log_table->currentIndex().row();
        if (current_row >= start_line && current_row < start_line + count) {
            update_details_pane(current_row);
        }
    }
}

void QemuLogViewer::on_search_results(const std::vector<int>& matches) {
    search_matches = matches;
    current_search_index = -1;

    bool has_matches = !search_matches.empty();
    search_next_btn->setEnabled(has_matches);
    search_prev_btn->setEnabled(has_matches);

    if (has_matches) {
        current_search_index = 0;
        status_label->setText(QString("Match 1 of %1").arg(search_matches.size()));
        scroll_to_row_for_search(search_matches[0]);
    } else {
        status_label->setText("No matches found");
    }
}

void QemuLogViewer::on_interrupts_received(const std::vector<LogEntry>& interrupts) {
    interrupts_panel->clear();
    std::map<std::string, QTreeWidgetItem*> groups;

    // Common x86 exception names for nicer display
    std::unordered_map<int, std::string> irq_names{{0x0, "Divide Error"},
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

    if (interrupt_filter_combo) {
        interrupt_filter_combo->blockSignals(true);
        interrupt_filter_combo->clear();
        interrupt_filter_combo->addItem("All");
    }

    std::unordered_set<std::string> seen_interrupts;

    for (const auto& entry : interrupts) {
        std::string int_num = entry.interrupt_number;

        // Populate Panel
        if (!groups.contains(int_num)) {
            auto* item = new QTreeWidgetItem(interrupts_panel);

            // Format interrupt name
            QString s = QString::fromStdString(int_num);
            bool ok;
            int val = s.toInt(&ok, 16);
            QString display;
            if (ok) {
                auto it = irq_names.find(val);
                if (it != irq_names.end()) {
                    display = QString("0x%1 - %2").arg(s).arg(QString::fromStdString(it->second));
                } else {
                    display = QString("0x%1").arg(s);
                }
            } else {
                display = s;
            }

            item->setText(0, display);
            item->setData(0, Qt::UserRole, QString::fromStdString(int_num));
            groups[int_num] = item;
        }

        auto* group_item = groups[int_num];
        auto* child = new QTreeWidgetItem(group_item);
        child->setText(0, QString("Line %1: %2").arg(entry.line_number).arg(QString::fromStdString(entry.cpu_state_info)));
        child->setData(0, Qt::UserRole, entry.line_number);

        // Populate Combo Box
        if (interrupt_filter_combo && seen_interrupts.insert(int_num).second) {
            QString s = QString::fromStdString(int_num);
            bool ok;
            int val = s.toInt(&ok, 16);
            QString display;
            if (ok) {
                auto it = irq_names.find(val);
                if (it != irq_names.end()) {
                    display = QString("0x%1 - %2").arg(s).arg(QString::fromStdString(it->second));
                } else {
                    display = QString("0x%1").arg(s);
                }
            } else {
                display = s;
            }
            interrupt_filter_combo->addItem(display, QVariant(s));
        }
    }

    // Update occurrences count
    for (auto const& [key, item] : groups) {
        item->setText(1, QString::number(item->childCount()));
    }

    if (interrupt_filter_combo) {
        interrupt_filter_combo->setEnabled(!interrupts.empty());
        interrupt_filter_combo->blockSignals(false);
    }
}

void QemuLogViewer::on_filter_applied(int total_lines) {
    if (virtual_table_model) {
        virtual_table_model->setRowCount(total_lines);
    }
    status_label->setText(QString("Filtered: %1 lines").arg(total_lines));
}

// Populate the interrupt filter dropdown with unique interrupt numbers present
void QemuLogViewer::populate_interrupt_filter() {
    // Deprecated: handled by onInterruptsReceived
}

void QemuLogViewer::build_interrupt_panel() {
    // Deprecated: handled by onInterruptsReceived
}

// Slot fired when the interrupt filter selection changes
void QemuLogViewer::on_interrupt_filter_changed(const QString& text) {
    if (client) {
        bool hide_structural = hide_structural_checkbox->isChecked();
        QString interrupt_filter = text;
        if (interrupt_filter == "All") {
            interrupt_filter = "";
        }

        client->set_filter(hide_structural, interrupt_filter);
    }
}

// Move to next occurrence of currently selected interrupt
void QemuLogViewer::on_interrupt_next() {
    // With server-side filtering, the view only contains the interrupts.
    // So we just move to the next row.
    int current_row = log_table->currentIndex().row();
    if (current_row < log_table->model()->rowCount() - 1) {
        log_table->selectRow(current_row + 1);
        scroll_to_row(current_row + 1);
    }
}

// Move to previous occurrence of currently selected interrupt
void QemuLogViewer::on_interrupt_previous() {
    int current_row = log_table->currentIndex().row();
    if (current_row > 0) {
        log_table->selectRow(current_row - 1);
        scroll_to_row(current_row - 1);
    }
}

void QemuLogViewer::on_search_text_changed() {
    // Cancel any pending search
    search_debounce_timer->stop();

    // Start debounce timer
    search_debounce_timer->start();
}

void QemuLogViewer::perform_debounced_search() { perform_search_optimized(); }

void QemuLogViewer::perform_search() {
    search_matches.clear();
    current_search_index = -1;

    QString search_text = search_edit->text().trimmed();
    if (search_text.isEmpty()) {
        search_next_btn->setEnabled(false);
        search_prev_btn->setEnabled(false);
        highlight_search_matches();
        return;
    }

    // Create search pattern
    if (regex_checkbox->isChecked()) {
        search_regex = QRegularExpression(search_text, QRegularExpression::CaseInsensitiveOption);
        if (!search_regex.isValid()) {
            status_label->setText("Invalid regex pattern");
            return;
        }
    } else {
        // Camel case matching with case insensitive fallback
        QString pattern = search_text;
        // Escape special regex characters
        pattern.replace(QRegularExpression(R"(([\^\$\*\+\?\{\}\[\]\(\)\|\\]))"), "\\\\1");
        search_regex = QRegularExpression(pattern, QRegularExpression::CaseInsensitiveOption);
    }

    // Search through searchable rows (already built from visible entries)
    for (auto& searchable_row : searchable_rows) {
        if (search_regex.match(searchable_row.combined_text).hasMatch()) {
            search_matches.push_back(searchable_row.original_row_index);
        }
    }

    bool has_matches = !search_matches.empty();
    search_next_btn->setEnabled(has_matches);
    search_prev_btn->setEnabled(has_matches);

    if (has_matches) {
        current_search_index = 0;
        status_label->setText(QString("Found %1 matches").arg(search_matches.size()));
        highlight_search_matches();
        scroll_to_row(search_matches[0]);
    } else {
        status_label->setText("No matches found");
        highlight_search_matches();
    }
}

void QemuLogViewer::on_search_next() {
    if (search_matches.empty()) {
        return;
    }

    current_search_index = (current_search_index + 1) % search_matches.size();
    highlight_search_matches();  // Update highlighting for new current match
    scroll_to_row_for_search(search_matches[current_search_index]);
    status_label->setText(QString("Match %1 of %2").arg(current_search_index + 1).arg(search_matches.size()));
}

void QemuLogViewer::on_search_previous() {
    if (search_matches.empty()) {
        return;
    }

    current_search_index = (current_search_index - 1 + search_matches.size()) % search_matches.size();
    highlight_search_matches();  // Update highlighting for new current match
    scroll_to_row_for_search(search_matches[current_search_index]);
    status_label->setText(QString("Match %1 of %2").arg(current_search_index + 1).arg(search_matches.size()));
}

void QemuLogViewer::on_regex_toggled(bool enabled) {
    Q_UNUSED(enabled)
    if (!search_edit->text().isEmpty()) {
        perform_debounced_search();
    }
}

void QemuLogViewer::on_hide_structural_toggled(bool enabled) {
    Q_UNUSED(enabled)

    if (client) {
        bool hide_structural = hide_structural_checkbox->isChecked();
        QString interrupt_filter = interrupt_filter_combo->currentText();
        if (interrupt_filter == "All") {
            interrupt_filter = "";
        }

        client->set_filter(hide_structural, interrupt_filter);
    }

    // Re-run search if there was one
    if (!search_edit->text().isEmpty()) {
        perform_debounced_search();
    }
}

void QemuLogViewer::on_navigation_text_changed() {
    // This method can be used for real-time navigation validation
    // For now, we handle navigation on Enter key press in connectSignals()
}

void QemuLogViewer::highlight_search_matches() {
    // With virtual tables, highlighting is handled through the model/delegate
    // The visual highlighting happens dynamically as rows are rendered
    // Re-invalidate visible rows to trigger refresh with highlight updates
    if (log_table && virtual_table_model) {
        virtual_table_model->invalidateRows(0, log_table->model()->rowCount() - 1);
    }
}

void QemuLogViewer::jump_to_address(uint64_t address) {
    if (client) {
        client->search(QString("0x%1").arg(address, 0, 16), false);
    }
}

void QemuLogViewer::jump_to_line(int /*lineNumber*/) { status_label->setText("Jump to line not supported in remote mode yet"); }

void QemuLogViewer::scroll_to_row(int row) {
    if (log_table && row >= 0 && row < log_table->model()->rowCount()) {
        log_table->selectRow(row);
        log_table->scrollTo(log_table->model()->index(row, 0), QAbstractItemView::PositionAtCenter);
    }
}

void QemuLogViewer::scroll_to_row_for_search(int row) {
    if (log_table && row >= 0 && row < log_table->model()->rowCount()) {
        // For virtual table, use scrollToLogicalRow which handles loading
        log_table->scrollToLogicalRow(row, QAbstractItemView::PositionAtCenter);
        // Select the row
        log_table->selectRow(row);
        update_details_pane(row);
    }
}

void QemuLogViewer::on_table_selection_changed() {
    auto selected_rows = log_table->selectionModel()->selectedRows();
    if (selected_rows.isEmpty()) {
        details_pane->clear();
        return;
    }

    int row = selected_rows.first().row();
    if (client && row >= 0 && row < log_table->model()->rowCount()) {
        const LogEntry* entry = client->get_entry(row);
        if (entry) {
            update_hex_view(*entry);
            update_disassembly_view(*entry);
            update_details_pane(row);
        }
    }
}

void QemuLogViewer::update_hex_view(const LogEntry& entry) {
    QString hex_text;

    if (!entry.hex_bytes.empty()) {
        std::istringstream iss(entry.hex_bytes);
        std::string byte;
        int pos = 0;

        hex_text += QString("Address: %1\n").arg(QString::fromStdString(entry.address));
        hex_text += "Hex Bytes:\n";

        while (iss >> byte && byte.length() == 2) {
            if (pos % 16 == 0) {
                hex_text += QString("%1: ").arg(pos, 4, 16, QChar('0')).toUpper();
            }

            hex_text += QString("%1 ").arg(QString::fromStdString(byte)).toUpper();

            if (pos % 16 == 15) {
                hex_text += "\n";
            }
            pos++;
        }

        if (pos % 16 != 0) {
            hex_text += "\n";
        }
    }

    hex_view->setPlainText(hex_text);
}

void QemuLogViewer::update_disassembly_view(const LogEntry& entry) {
    QString dis_text;

    dis_text += QString("Line %1: %2\n\n").arg(entry.line_number).arg(QString::fromStdString(entry.original_line));

    if (!entry.address.empty()) {
        dis_text += QString("Address: %1\n").arg(QString::fromStdString(entry.address));
    }

    if (!entry.function.empty()) {
        dis_text += QString("Function: %1\n").arg(QString::fromStdString(entry.function));
    }

    if (!entry.assembly.empty()) {
        dis_text += QString("Assembly: %1\n").arg(QString::fromStdString(entry.assembly));
    }

    disassembly_view->setPlainText(dis_text);
}

void QemuLogViewer::update_details_pane(int row) {
    if (!client || !log_table || row < 0 || row >= log_table->model()->rowCount()) {
        details_pane->clear();
        return;
    }

    const LogEntry* entry = client->get_entry(row);
    if (!entry) {
        details_pane->clear();
        return;
    }

    QString details_text;

    // Show basic entry information
    details_text += QString("=== Entry Details ===\n");
    details_text += QString("Line: %1\n").arg(entry->line_number);
    details_text += QString("Type: %1\n")
                        .arg(entry->type == EntryType::INSTRUCTION ? "INSTRUCTION"
                             : entry->type == EntryType::INTERRUPT ? "INTERRUPT"
                             : entry->type == EntryType::REGISTER  ? "REGISTER"
                             : entry->type == EntryType::BLOCK     ? "BLOCK"
                             : entry->type == EntryType::SEPARATOR ? "SEPARATOR"
                                                                   : "OTHER");

    if (!entry->address.empty()) {
        details_text += QString("Address: %1\n").arg(QString::fromStdString(entry->address));

        // Show symbol resolution status based on whether we found a function
        if (!entry->function.empty()) {
            details_text += QString("Symbol Lookup: Resolved\n");
        } else if (entry->address_value != 0) {
            const Config& config = ConfigService::instance().get_config();
            QString symbol_file_path = config.find_symbol_file_for_address(entry->address_value);
            if (!symbol_file_path.isEmpty()) {
                details_text += QString("Symbol File: %1\n").arg(symbol_file_path);
                details_text += QString("Symbol Lookup: No symbol found at this address\n");
            } else {
                details_text += QString("Symbol Lookup: No mapping found for this address range\n");
            }
        }
    }

    if (!entry->function.empty()) {
        // Display function name without offset
        QString func_name = format_function(entry->function);
        details_text += QString("Function: %1\n").arg(func_name);

        // Show source info if available
        if (!entry->source_file.empty() && entry->source_line > 0) {
            details_text += QString("Source: %1:%2\n").arg(QString::fromStdString(entry->source_file)).arg(entry->source_line);
        } else if (!entry->source_file.empty()) {
            details_text += QString("Source File: %1\n").arg(QString::fromStdString(entry->source_file));
        }
    }

    if (!entry->assembly.empty()) {
        QString intel_assembly = format_assembly(entry->assembly);
        details_text += QString("Assembly: %1\n").arg(intel_assembly);
    }

    // For REG entries, show the full original line which contains CPU state details
    if (entry->type == EntryType::REGISTER && !entry->original_line.empty()) {
        details_text += QString("CPU State: %1\n").arg(QString::fromStdString(entry->original_line));
    }

    details_text += QString("\n");

    // Try to get source code for INSTRUCTION entries
    if (entry->type == EntryType::INSTRUCTION && !entry->source_file.empty() && entry->source_line > 0) {
        QString source_html = get_source_code_snippet(QString::fromStdString(entry->source_file), entry->source_line);
        if (!source_html.isEmpty()) {
            details_text += "=== Source Code ===\n";
            // We'll append this as HTML later
        }
    }

    // For interrupt entries, show all child details
    if (entry->type == EntryType::INTERRUPT && !entry->child_entries.empty()) {
        details_text += QString("=== Interrupt Details (%1 entries) ===\n\n").arg(entry->child_entries.size());

        for (const auto& child : entry->child_entries) {
            details_text += QString("Line %1: ").arg(child.line_number);

            if (child.type == EntryType::REGISTER) {
                details_text += "REG ";
                // For register entries, show the full CPU state
                if (!child.original_line.empty()) {
                    details_text += QString("CPU State: %1").arg(QString::fromStdString(child.original_line));
                } else if (!child.assembly.empty()) {
                    QString child_intel_assembly = format_assembly(child.assembly);
                    details_text += child_intel_assembly;
                }
            } else if (child.type == EntryType::OTHER) {
                details_text += "STATE ";
                details_text += QString::fromStdString(child.original_line);
            } else {
                // For other types, use assembly if available, otherwise original line
                if (!child.assembly.empty()) {
                    QString child_intel_assembly = format_assembly(child.assembly);
                    details_text += child_intel_assembly;
                } else {
                    details_text += QString::fromStdString(child.original_line);
                }
            }
            details_text += "\n";
        }
    }

    // Build HTML if we have source code to display
    QString html_content;
    if (entry->type == EntryType::INSTRUCTION) {
        if (!entry->source_file.empty() && entry->source_line > 0) {
            QString source_html = get_source_code_snippet(QString::fromStdString(entry->source_file), entry->source_line);
            if (!source_html.isEmpty()) {
                html_content = QString("<pre>%1</pre>\n").arg(details_text.toHtmlEscaped());
                html_content += "<hr>\n";
                html_content += source_html;
            }
        }
    }

    if (!html_content.isEmpty()) {
        details_pane->setHtml(html_content);
    } else {
        details_pane->setPlainText(details_text);
    }
}

void QemuLogViewer::sync_scroll_bars(int value) {
    Q_UNUSED(value)
    // Additional sync logic can be added here if needed
}

void QemuLogViewer::on_details_pane_link_clicked(const QUrl& url) {
    // Handle VS Code links - open external URL
    if (url.scheme() == "vscode") {
        QDesktopServices::openUrl(url);
    } else if (url.scheme() == "wos-remote") {
        // Format: wos-remote://path:line
        QString path = url.path();  // /path/to/file:line

        QString url_str = url.toString();
        QString prefix = "wos-remote://";
        if (url_str.startsWith(prefix)) {
            QString content = url_str.mid(prefix.length());
            int last_colon = content.lastIndexOf(':');
            if (last_colon != -1) {
                QString file = content.left(last_colon);
                int line = content.mid(last_colon + 1).toInt();

                client->request_open_source_file(file, line);
            }
        }
    }
}

void QemuLogViewer::on_hex_view_selection_changed() {
    // Future implementation for hex view interactions
}

bool QemuLogViewer::is_address_input(const QString& text) { return text.startsWith("0x", Qt::CaseInsensitive); }

QString QemuLogViewer::format_address(const std::string& addr) {
    if (addr.empty()) {
        return {};
    }
    return QString::fromStdString(addr);
}

QString QemuLogViewer::format_function(const std::string& func) {
    if (func.empty()) {
        return {};
    }

    QString qfunc = QString::fromStdString(func);

    // Remove offset like "+0xc6f" from the end
    static const QRegularExpression OFFSET_REGEX(R"(\+0x[0-9a-fA-F]+$)");
    QString clean_func = qfunc;
    clean_func.remove(OFFSET_REGEX);

    // If it contains a path (especially .asm files), trim to just the filename
    static const QRegularExpression PATH_REGEX(R"(^(.*/)?([^/]+\.(asm|cpp|c|h|hpp))(.*)$)");
    auto path_match = PATH_REGEX.match(clean_func);
    if (path_match.hasMatch()) {
        // Keep just filename + extension + any remaining info
        QString filename = path_match.captured(2);
        QString remaining = path_match.captured(4);
        clean_func = filename + remaining;
    }

    return clean_func;
}

QString QemuLogViewer::format_hex_bytes(const std::string& bytes) {
    if (bytes.empty()) {
        return {};
    }
    return QString::fromStdString(bytes);
}

QString QemuLogViewer::format_assembly(const std::string& assembly) {
    if (assembly.empty()) {
        return {};
    }

    // Try to convert to Intel syntax using improved Capstone disassembler
    if (disassembler) {
        std::string intel = disassembler->convert_to_intel(assembly);
        return QString::fromStdString(intel);
    }

    return QString::fromStdString(assembly);
}

QString QemuLogViewer::extract_file_info(const std::string& func) {
    if (func.empty()) {
        return "";
    }
    QString qfunc = QString::fromStdString(func);

    // First check if this is an .asm file path in the function string
    // Format could be: "/path/to/file.asm" or just "file.asm"
    static const QRegularExpression ASM_FILE_REGEX(R"((^|/|\\)([^/\\]+\.asm)(?:/|\\|$))");
    auto asm_match = ASM_FILE_REGEX.match(qfunc);
    if (asm_match.hasMatch()) {
        // Extract just the .asm filename
        return asm_match.captured(2);
    }

    // Try to extract file:line:column info from format "addr[func](file:line:column)"
    static const QRegularExpression FILE_REGEX(R"(\(([^)]+)\))");
    auto match = FILE_REGEX.match(qfunc);
    if (match.hasMatch()) {
        QString file_info = match.captured(1);

        // Check if this looks like file:line:column (not function parameters)
        static const QRegularExpression FILE_LINE_REGEX(R"(^[^:]+\.(asm|cpp|c|h|hpp):\d+(?::\d+)?$)");
        if (FILE_LINE_REGEX.match(file_info).hasMatch()) {
            // Extract just filename:line (trim the path)
            static const QRegularExpression PATH_REGEX(R"(([^/\\]+\.(asm|cpp|c|h|hpp):\d+(?::\d+)?))");
            auto path_match = PATH_REGEX.match(file_info);
            if (path_match.hasMatch()) {
                return path_match.captured(1);
            }
            return file_info;
        }
    }

    return "";
}

// Performance optimization methods
void QemuLogViewer::initialize_performance_optimizations() {
    // Pre-allocate string buffers only (item pooling removed due to Qt ownership issues)
    string_buffers.reserve(STRING_BUFFER_SIZE);
    for (size_t i = 0; i < STRING_BUFFER_SIZE; ++i) {
        string_buffers.emplace_back();
        string_buffers.back().reserve(512);  // Reserve space for typical strings
    }

    next_string_buffer = 0;
}

void QemuLogViewer::build_lookup_maps() {
    // Deprecated in client-server mode
}

void QemuLogViewer::build_searchable_rows() {
    // Deprecated in client-server mode
}

QTableWidgetItem* QemuLogViewer::get_pooled_item() { return new QTableWidgetItem(); }

void QemuLogViewer::return_item_to_pool(QTableWidgetItem* item) { Q_UNUSED(item) }

QString& QemuLogViewer::get_string_buffer() {
    QString& buffer = string_buffers[next_string_buffer];
    next_string_buffer = (next_string_buffer + 1) % STRING_BUFFER_SIZE;
    buffer.clear();
    return buffer;
}

void QemuLogViewer::pre_allocate_table_items(int row_count) { Q_UNUSED(row_count) }

void QemuLogViewer::batch_update_table(const std::vector<const LogEntry*>& entries) { Q_UNUSED(entries) }

int QemuLogViewer::find_next_iret_line(int start_line_number) {
    Q_UNUSED(start_line_number)
    return INT_MAX;
}

void QemuLogViewer::populate_table() {
    // Deprecated in client-server mode
}

void QemuLogViewer::on_interrupt_panel_activated(QTreeWidgetItem* item, int /*column*/) {
    if (!item || !item->parent()) {
        return;
    }

    // Child item: retrieve line number
    QVariant v = item->data(0, Qt::UserRole);
    if (!v.isValid()) {
        return;
    }
    int line_number = v.toInt();

    // Request row for line from server
    status_label->setText(QString("Jumping to line %1...").arg(line_number));
    client->request_row_for_line(line_number);
}

void QemuLogViewer::on_row_for_line_received(int row) {
    if (row >= 0) {
        scroll_to_row(row);
        status_label->setText(QString("Jumped to row %1").arg(row));
    } else {
        status_label->setText("Could not find row for line (maybe filtered out?)");
    }
}

void QemuLogViewer::on_interrupt_toggle_fold(QTreeWidgetItem* item, int /*column*/) {
    Q_UNUSED(item)
    // Folding not supported in remote mode yet
}

void QemuLogViewer::perform_search_optimized() {
    QString search_text = search_edit->text().trimmed();
    if (search_text.isEmpty()) {
        if (search_active && pre_search_position >= 0) {
            scroll_to_row(pre_search_position);
            search_active = false;
            pre_search_position = -1;
        }
        current_search_index = -1;
        search_next_btn->setEnabled(false);
        search_prev_btn->setEnabled(false);
        search_matches.clear();
        highlight_search_matches();
        return;
    }

    if (!search_active) {
        auto selected_rows = log_table->selectionModel()->selectedRows();
        if (!selected_rows.isEmpty()) {
            pre_search_position = selected_rows.first().row();
        } else {
            pre_search_position = log_table->rowAt(0);
        }
        search_active = true;
    }

    client->search(search_text, regex_checkbox->isChecked());
    status_label->setText("Searching...");
}

QString QemuLogViewer::get_source_code_snippet(const QString& filename, int line_number) {
    if (filename.isEmpty() || line_number <= 0) {
        return "";
    }

    // Try to read the source file
    QFile source_file(filename);
    if (!source_file.open(QIODevice::ReadOnly | QIODevice::Text)) {
        qDebug() << "Could not open source file:" << filename << "CWD:" << QDir::currentPath();
        // Return just the filename:line if we can't read the file
        return QString("%1:%2").arg(QFileInfo(filename).fileName()).arg(line_number);
    }

    QTextStream in(&source_file);
    QStringList lines;
    int current_line = 0;

    while (!in.atEnd() && current_line < line_number + 2) {
        lines.append(in.readLine());
        current_line++;
    }

    source_file.close();

    // Build HTML with syntax highlighting and clickable lines
    QString html;
    html += QString("<b>%1:%2</b><br>").arg(QFileInfo(filename).fileName()).arg(line_number);
    html += "<pre style='font-family: Consolas, monospace; margin: 5px 0;'>";

    int start_line = std::max(0, line_number - 11);
    int end_line = std::min(static_cast<int>(lines.size()), line_number + 10);

    for (int i = start_line; i < end_line; ++i) {
        int display_line = i + 1;
        const QString& line = lines[i];

        // Highlight the target line
        if (display_line == line_number) {
            html += QString("<span style='background-color: #333300; color: #ffff99;'><b>%1 > </b>%2</span>\n")
                        .arg(display_line, 4)
                        .arg(line.toHtmlEscaped());
        } else {
            html += QString("<span style='color: #666666;'>%1   %2</span>\n").arg(display_line, 4).arg(line.toHtmlEscaped());
        }
    }

    html += "</pre>";

    // Add clickable link to open in VS Code with proper format
    // Use custom scheme to intercept click
    QFileInfo file_info(filename);
    QString absolute_path = file_info.absoluteFilePath();
    // URL encode the path
    absolute_path.replace(" ", "%20");
    html += QString("<br><a href='wos-remote://%1:%2' style='color: #4da6ff; text-decoration: underline;'>Open in VS Code</a>")
                .arg(absolute_path)
                .arg(line_number);

    return html;
}

QColor QemuLogViewer::get_entry_type_color(EntryType type) {
    // Colors darkened to 25% of their current values for better dark theme integration
    switch (type) {
        case EntryType::INSTRUCTION:
            return {9, 19, 9};  // 25% of (35, 75, 35)
        case EntryType::INTERRUPT:
            return {19, 9, 9};  // 25% of (75, 35, 35)
        case EntryType::REGISTER:
            return {9, 9, 19};  // 25% of (35, 35, 75)
        case EntryType::BLOCK:
            return {19, 19, 9};  // 25% of (75, 75, 35)
        case EntryType::SEPARATOR:
            return {13, 13, 13};  // 25% of (50, 50, 50)
        case EntryType::OTHER:
        default:
            return {8, 8, 8};  // 25% of (30, 30, 30)
    }
}

void QemuLogViewer::on_table_cell_clicked(int row, int column) {
    Q_UNUSED(column)
    // Update details pane for any row click
    update_details_pane(row);
}

// ============================================================================
// Coredump Integration
// ============================================================================

#include <QFileDialog>
#include <QMessageBox>

#include "config.h"
#include "coredump_browser.h"
#include "coredump_disasm_panel.h"
#include "coredump_elf_panel.h"
#include "coredump_memory.h"
#include "coredump_memory_panel.h"
#include "coredump_parser.h"
#include "coredump_register_panel.h"
#include "coredump_segment_panel.h"
#include "elf_symbol_resolver.h"

void QemuLogViewer::setup_coredump_panels() {
    // --- Browser (left dock, tabbed with interrupts) ---
    coredump_browser = new CoredumpBrowser(this);
    browser_dock = new QDockWidget("Coredump Browser", this);
    browser_dock->setWidget(coredump_browser);
    browser_dock->setAllowedAreas(Qt::LeftDockWidgetArea | Qt::RightDockWidgetArea);
    addDockWidget(Qt::LeftDockWidgetArea, browser_dock);

    // Tab the coredump browser with the interrupts panel
    if (interrupts_dock) {
        tabifyDockWidget(interrupts_dock, browser_dock);
        interrupts_dock->raise();  // Show interrupts tab by default
    }

    // Set initial directory from config
    const auto& cfg = ConfigService::instance().get_config();
    coredump_browser->set_directory(cfg.get_coredump_directory());

    connect(coredump_browser, &CoredumpBrowser::coredump_selected, this, &QemuLogViewer::open_coredump);
    connect(coredump_browser, &CoredumpBrowser::extraction_finished, this, [this](bool ok, const QString& msg) {
        if (ok) {
            status_label->setText("Extraction complete");
            coredump_browser->refresh();
        } else {
            status_label->setText(QString("Extraction failed: %1").arg(msg));
        }
    });

    // --- Register panel (right dock) ---
    register_panel = new CoredumpRegisterPanel(this);
    addDockWidget(Qt::RightDockWidgetArea, register_panel);
    register_panel->hide();  // Hidden until a coredump is loaded
    connect(register_panel, &CoredumpRegisterPanel::address_clicked, this, &QemuLogViewer::on_coredump_address_clicked);

    // --- Segment panel (right dock, tabified with registers) ---
    segment_panel = new CoredumpSegmentPanel(this);
    addDockWidget(Qt::RightDockWidgetArea, segment_panel);
    tabifyDockWidget(register_panel, segment_panel);
    segment_panel->hide();

    connect(segment_panel, &CoredumpSegmentPanel::dump_segment_requested, this, [this](int /*idx*/, uint64_t vaStart, uint64_t vaEnd) {
        if (memory_panel) {
            memory_panel->dump_range(vaStart, vaEnd);
        }
    });

    // --- ELF info panel (right dock, tabified) ---
    elf_panel = new CoredumpElfPanel(this);
    addDockWidget(Qt::RightDockWidgetArea, elf_panel);
    tabifyDockWidget(segment_panel, elf_panel);
    elf_panel->hide();

    // --- Disassembly panel (right dock, tabified with ELF panel) ---
    disasm_panel = new CoredumpDisasmPanel(this);
    addDockWidget(Qt::RightDockWidgetArea, disasm_panel);
    tabifyDockWidget(elf_panel, disasm_panel);
    disasm_panel->hide();
    connect(disasm_panel, &CoredumpDisasmPanel::address_clicked, this, &QemuLogViewer::on_coredump_address_clicked);

    // --- Memory/Stack panel (bottom dock, tabbed with hex bytes) ---
    memory_panel = new CoredumpMemoryPanel(this);
    addDockWidget(Qt::BottomDockWidgetArea, memory_panel);
    memory_panel->hide();
    connect(memory_panel, &CoredumpMemoryPanel::address_clicked, this, &QemuLogViewer::on_coredump_address_clicked);

    // Tab memory panel with hex bytes dock
    if (hex_dock) {
        tabifyDockWidget(hex_dock, memory_panel);
        hex_dock->raise();  // Show hex bytes tab by default
    }
}

void QemuLogViewer::open_coredump(const QString& file_path) {
    // Parse the coredump binary
    auto dump = wosdbg::parse_core_dump(file_path);
    if (!dump) {
        status_label->setText(QString("Failed to open coredump: %1").arg(file_path));
        return;
    }

    current_core_dump = std::move(dump);

    // Resolve symbols from filename + config
    resolve_symbols_for_coredump();

    // Build symbol source lists
    std::vector<wosdbg::SymbolTable*> sym_tables;
    std::vector<wosdbg::SectionMap*> section_maps;
    if (core_dump_symtab) {
        sym_tables.push_back(core_dump_symtab.get());
    }
    if (embedded_symtab) {
        sym_tables.push_back(embedded_symtab.get());
    }
    if (kernel_symtab) {
        sym_tables.push_back(kernel_symtab.get());
    }
    if (core_dump_sections) {
        section_maps.push_back(core_dump_sections.get());
    }
    if (embedded_sections) {
        section_maps.push_back(embedded_sections.get());
    }
    if (kernel_sections) {
        section_maps.push_back(kernel_sections.get());
    }

    // Update all panels
    register_panel->load_core_dump(*current_core_dump, sym_tables, section_maps);
    register_panel->show();
    register_panel->raise();

    segment_panel->load_core_dump(*current_core_dump);
    segment_panel->show();

    elf_panel->set_core_dump(current_core_dump.get());
    elf_panel->show();

    memory_panel->set_core_dump(current_core_dump.get(), sym_tables, section_maps);
    memory_panel->show();

    disasm_panel->load_core_dump(current_core_dump.get(), sym_tables, section_maps);
    disasm_panel->show();
    disasm_panel->raise();

    // Auto-dump the stack around RSP
    memory_panel->dump_stack_around_rsp();

    // Extract filename for status
    QFileInfo fi(file_path);
    status_label->setText(QString("Coredump: %1 | PID %2 | CPU %3 | Int %4")
                              .arg(fi.fileName())
                              .arg(current_core_dump->pid)
                              .arg(current_core_dump->cpu)
                              .arg(wosdbg::interrupt_name(current_core_dump->int_num)));
}

void QemuLogViewer::close_coredump() {
    register_panel->clear();
    register_panel->hide();
    segment_panel->clear();
    segment_panel->hide();
    elf_panel->clear();
    elf_panel->hide();
    memory_panel->clear();
    memory_panel->hide();
    disasm_panel->clear();
    disasm_panel->hide();

    current_core_dump.reset();
    core_dump_symtab.reset();
    core_dump_sections.reset();
    embedded_symtab.reset();
    embedded_sections.reset();
    kernel_symtab.reset();
    kernel_sections.reset();

    status_label->setText("Ready");
}

void QemuLogViewer::resolve_symbols_for_coredump() {
    if (!current_core_dump) {
        return;
    }

    const auto& cfg = ConfigService::instance().get_config();

    // 1. Try to resolve from the coredump filename -> binaryName -> config ELF path
    QString binary_name = wosdbg::parse_binary_name_from_filename(current_core_dump->source_filename);
    QString elf_path = cfg.find_elf_path_for_binary(binary_name);

    if (!elf_path.isEmpty()) {
        core_dump_symtab = wosdbg::load_symbols_from_file(elf_path);
        core_dump_sections = wosdbg::load_sections_from_file(elf_path);
    }

    // 2. Try embedded ELF in the coredump itself
    QByteArray embedded_elf = current_core_dump->embedded_elf();
    if (!embedded_elf.isEmpty()) {
        embedded_symtab = wosdbg::load_symbols_from_core_dump(*current_core_dump);
        embedded_sections = wosdbg::load_sections_from_core_dump(*current_core_dump);
    }

    // 3. Try kernel symbols from the address lookups in config
    const auto& lookups = cfg.get_address_lookups();
    for (const auto& lu : lookups) {
        if (lu.symbol_file_path.contains("kern") || lu.symbol_file_path.contains("wos")) {
            QString kern_path = cfg.resolve_path(lu.symbol_file_path);
            kernel_symtab = wosdbg::load_symbols_from_file(kern_path);
            kernel_sections = wosdbg::load_sections_from_file(kern_path);
            break;
        }
    }

    // Update ELF panel with resolved info
    elf_panel->set_symbol_info(binary_name, elf_path, core_dump_symtab.get(), core_dump_sections.get());
    if (embedded_symtab) {
        elf_panel->add_symbol_source("Embedded ELF", embedded_symtab.get(), embedded_sections.get());
    }
    if (kernel_symtab) {
        elf_panel->add_symbol_source("Kernel", kernel_symtab.get(), kernel_sections.get());
    }
}

void QemuLogViewer::on_coredump_address_clicked(uint64_t addr) {
    // Put the address in the navigation edit and jump to it in the log
    navigation_edit->setText(QString("0x%1").arg(addr, 16, 16, QChar('0')));
    jump_to_address(addr);
}

void QemuLogViewer::browse_coredump_directory() {
    auto& svc = ConfigService::instance();
    const auto& cfg = svc.get_config();
    QString dir = QFileDialog::getExistingDirectory(this, "Select Coredump Directory", cfg.get_coredump_directory());
    if (!dir.isEmpty()) {
        svc.get_mutable_config().set_coredump_directory(dir);
        svc.save();
        coredump_browser->set_directory(dir);
        status_label->setText(QString("Coredump directory: %1").arg(dir));
    }
}

void QemuLogViewer::extract_coredumps() {
    coredump_browser->extract_coredumps();
    status_label->setText("Extracting coredumps...");
}

void QemuLogViewer::refresh_coredumps() {
    coredump_browser->refresh();
    status_label->setText("Coredump list refreshed");
}

void QemuLogViewer::on_mcp_toggle() {
    if (!client) {
        return;
    }
    if (mcp_running) {
        client->stop_mcp_server();
    } else {
        client->start_mcp_server();
    }
}

void QemuLogViewer::on_mcp_server_status(bool running, const QString& endpoint, const QString& message) {
    mcp_running = running;
    mcp_endpoint = endpoint;
    if (mcp_toggle_btn) {
        mcp_toggle_btn->setText(running ? "MCP On" : "MCP Off");
        mcp_toggle_btn->setToolTip(running ? QString("MCP server running at %1").arg(endpoint)
                                           : "Start the MCP server on the active wosdbg backend");
    }
    if (!message.isEmpty()) {
        status_label->setText(running && !endpoint.isEmpty() ? QString("%1: %2").arg(message, endpoint) : message);
    }
}
