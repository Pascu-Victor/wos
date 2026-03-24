#include "coredump_disasm_panel.h"

#include <QFont>
#include <QHBoxLayout>
#include <QLabel>
#include <QTextBlock>
#include <QVBoxLayout>

#include "coredump_parser.h"
#include "elf_symbol_resolver.h"

// How many bytes to feed Capstone per disassembly request.
// 30 worst-case x86-64 instructions (15 bytes each) + look-behind headroom.
static constexpr size_t   kDisasmBytes = 512;
static constexpr uint64_t kLookBehind  = 64;

CoredumpDisasmPanel::CoredumpDisasmPanel(QWidget* parent)
    : QDockWidget("Disassembly (coredump)", parent)
{
    if (cs_open(CS_ARCH_X86, CS_MODE_64, &cs_) != CS_ERR_OK) {
        cs_ = 0;
    } else {
        cs_option(cs_, CS_OPT_SYNTAX, CS_OPT_SYNTAX_INTEL);
        cs_option(cs_, CS_OPT_DETAIL, CS_OPT_OFF);
    }
    setupUI();
}

CoredumpDisasmPanel::~CoredumpDisasmPanel() {
    if (cs_) {
        cs_close(&cs_);
    }
}

void CoredumpDisasmPanel::setupUI() {
    auto* container = new QWidget(this);
    auto* layout    = new QVBoxLayout(container);
    layout->setContentsMargins(4, 4, 4, 4);
    layout->setSpacing(4);

    QFont mono("Monospace", 9);
    mono.setStyleHint(QFont::Monospace);

    // Toolbar
    auto* toolbar = new QHBoxLayout();

    ripLabel_ = new QLabel("Trap RIP: \xe2\x80\x94", container);
    ripLabel_->setFont(mono);
    toolbar->addWidget(ripLabel_);

    trapRipBtn_ = new QPushButton("Trap RIP", container);
    connect(trapRipBtn_, &QPushButton::clicked, this, [this]() {
        if (dump_) { disassembleAt(dump_->trapFrame.rip); }
    });
    toolbar->addWidget(trapRipBtn_);

    savedLabel_ = new QLabel("Saved RIP: \xe2\x80\x94", container);
    savedLabel_->setFont(mono);
    toolbar->addWidget(savedLabel_);

    savedRipBtn_ = new QPushButton("Saved RIP", container);
    connect(savedRipBtn_, &QPushButton::clicked, this, [this]() {
        if (dump_) { disassembleAt(dump_->savedFrame.rip); }
    });
    toolbar->addWidget(savedRipBtn_);

    toolbar->addStretch();
    layout->addLayout(toolbar);

    view_ = new QPlainTextEdit(container);
    view_->setReadOnly(true);
    view_->setFont(mono);
    view_->setLineWrapMode(QPlainTextEdit::NoWrap);
    layout->addWidget(view_, 1);

    setWidget(container);
}

void CoredumpDisasmPanel::loadCoreDump(const wosdbg::CoreDump* dump,
                                       const std::vector<wosdbg::SymbolTable*>& symTables,
                                       const std::vector<wosdbg::SectionMap*>& sectionMaps)
{
    dump_         = dump;
    symTables_    = symTables;
    sectionMaps_  = sectionMaps;

    if (!dump_) {
        clear();
        return;
    }

    ripLabel_->setText(
        QString("Trap RIP: %1").arg(wosdbg::formatAddress(dump_->trapFrame.rip,  symTables_, sectionMaps_)));
    savedLabel_->setText(
        QString("Saved RIP: %1").arg(wosdbg::formatAddress(dump_->savedFrame.rip, symTables_, sectionMaps_)));

    disassembleAt(dump_->trapFrame.rip);
}

void CoredumpDisasmPanel::disassembleAt(uint64_t va, int numInstructions) {
    if (!dump_ || !cs_) { return; }

    QByteArray elf = dump_->embeddedElf();
    if (elf.isEmpty()) {
        view_->setPlainText("(no embedded ELF in coredump)");
        return;
    }

    uint64_t start_va = (va > kLookBehind) ? va - kLookBehind : 0;
    auto bytes = wosdbg::elfBytesAtVA(elf, start_va, kDisasmBytes);
    if (bytes.empty()) {
        view_->setPlainText(QString("(no ELF bytes mapped at 0x%1)").arg(start_va, 16, 16, QChar('0')));
        return;
    }

    cs_insn* insns = nullptr;
    size_t count   = cs_disasm(cs_, bytes.data(), bytes.size(), start_va, 0, &insns);
    if (count == 0) {
        view_->setPlainText("(Capstone: disassembly failed)");
        return;
    }

    uint64_t trap_rip  = dump_->trapFrame.rip;
    uint64_t saved_rip = dump_->savedFrame.rip;

    // Anchor window at first instruction >= va
    int anchor = 0;
    for (size_t i = 0; i < count; ++i) {
        if (insns[i].address >= va) {
            anchor = static_cast<int>(i);
            break;
        }
    }
    int start_i = std::max(0, anchor - (numInstructions / 3));
    int end_i   = std::min(static_cast<int>(count), start_i + numInstructions);

    QString out;
    out += QString("; around 0x%1\n").arg(va,        16, 16, QChar('0'));
    out += QString("; trap  ==> 0x%1\n").arg(trap_rip,  16, 16, QChar('0'));
    out += QString("; saved  -- 0x%1\n\n").arg(saved_rip, 16, 16, QChar('0'));

    int target_line = -1;
    int cur_line    = static_cast<int>(out.count('\n'));

    for (int i = start_i; i < end_i; ++i) {
        const cs_insn& ins  = insns[static_cast<size_t>(i)];
        uint64_t        addr = ins.address;

        // Symbol label at this address
        auto sym = wosdbg::resolveAddress(addr, symTables_, sectionMaps_);
        if (sym) {
            out += QString("\n<%1>:\n").arg(QString::fromStdString(*sym));
            cur_line += 2;
        }

        // Gutter marker
        QString marker = "   ";
        if (addr == trap_rip) {
            marker = "==>";
            target_line = cur_line;
        } else if (addr == saved_rip) {
            marker = " --";
        }

        // Raw hex bytes (max 7 shown)
        QString hex_bytes;
        int show = std::min<int>(ins.size, 7);
        for (int b = 0; b < show; ++b) {
            hex_bytes += QString("%1 ").arg(static_cast<uint32_t>(ins.bytes[b]), 2, 16, QChar('0'));
        }
        if (ins.size > 7) { hex_bytes += "... "; }
        // Pad to fixed width (7*3 + 4 = 25 chars)
        hex_bytes = hex_bytes.leftJustified(25, ' ');

        // Mnemonic + operands, mnemonic left-justified to 8 chars
        QString mnem = QString(ins.mnemonic).leftJustified(8, ' ');
        QString line = QString("%1  %2  %3  %4 %5\n")
                           .arg(marker,
                                QString("%1").arg(addr, 16, 16, QChar('0')),
                                hex_bytes,
                                mnem,
                                QString(ins.op_str));
        out  += line;
        ++cur_line;
    }

    cs_free(insns, count);

    view_->setPlainText(out);

    if (target_line >= 0) {
        QTextCursor cursor(view_->document()->findBlockByLineNumber(target_line));
        view_->setTextCursor(cursor);
        view_->centerCursor();
    }
}

void CoredumpDisasmPanel::clear() {
    dump_ = nullptr;
    symTables_.clear();
    sectionMaps_.clear();
    view_->clear();
    ripLabel_->setText("Trap RIP: \xe2\x80\x94");
    savedLabel_->setText("Saved RIP: \xe2\x80\x94");
}
