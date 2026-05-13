#include "coredump_disasm_panel.h"

#include <qdockwidget.h>
#include <qobject.h>
#include <qplaintextedit.h>
#include <qpushbutton.h>
#include <qwidget.h>

#include <QFont>
#include <QHBoxLayout>
#include <QLabel>
#include <QTextBlock>
#include <QVBoxLayout>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "capstone.h"
#include "coredump_parser.h"
#include "elf_symbol_resolver.h"

// How many bytes to feed Capstone per disassembly request.
// 30 worst-case x86-64 instructions (15 bytes each) + look-behind headroom.
static constexpr size_t DISASM_BYTES = 512;
static constexpr uint64_t LOOK_BEHIND = 64;

CoredumpDisasmPanel::CoredumpDisasmPanel(QWidget* parent) : QDockWidget("Disassembly (coredump)", parent) {
    if (cs_open(CS_ARCH_X86, CS_MODE_64, &cs) != CS_ERR_OK) {
        cs = 0;
    } else {
        cs_option(cs, CS_OPT_SYNTAX, CS_OPT_SYNTAX_INTEL);
        cs_option(cs, CS_OPT_DETAIL, CS_OPT_OFF);
    }
    setup_ui();
}

CoredumpDisasmPanel::~CoredumpDisasmPanel() {
    if (cs) {
        cs_close(&cs);
    }
}

void CoredumpDisasmPanel::setup_ui() {
    auto* container = new QWidget(this);
    auto* layout = new QVBoxLayout(container);
    layout->setContentsMargins(4, 4, 4, 4);
    layout->setSpacing(4);

    QFont mono("Monospace", 9);
    mono.setStyleHint(QFont::Monospace);

    // Toolbar
    auto* toolbar = new QHBoxLayout();

    rip_label = new QLabel("Trap RIP: \xe2\x80\x94", container);
    rip_label->setFont(mono);
    toolbar->addWidget(rip_label);

    trap_rip_btn = new QPushButton("Trap RIP", container);
    connect(trap_rip_btn, &QPushButton::clicked, this, [this]() {
        if (dump) {
            disassemble_at(dump->trap_frame.rip);
        }
    });
    toolbar->addWidget(trap_rip_btn);

    saved_label = new QLabel("Saved RIP: \xe2\x80\x94", container);
    saved_label->setFont(mono);
    toolbar->addWidget(saved_label);

    saved_rip_btn = new QPushButton("Saved RIP", container);
    connect(saved_rip_btn, &QPushButton::clicked, this, [this]() {
        if (dump) {
            disassemble_at(dump->saved_frame.rip);
        }
    });
    toolbar->addWidget(saved_rip_btn);

    toolbar->addStretch();
    layout->addLayout(toolbar);

    view = new QPlainTextEdit(container);
    view->setReadOnly(true);
    view->setFont(mono);
    view->setLineWrapMode(QPlainTextEdit::NoWrap);
    layout->addWidget(view, 1);

    setWidget(container);
}

void CoredumpDisasmPanel::load_core_dump(const wosdbg::CoreDump* dump, const std::vector<wosdbg::SymbolTable*>& sym_tables,
                                         const std::vector<wosdbg::SectionMap*>& section_maps) {
    this->dump = dump;
    this->sym_tables = sym_tables;
    this->section_maps = section_maps;

    if (!dump) {
        clear();
        return;
    }

    rip_label->setText(QString("Trap RIP: %1").arg(wosdbg::format_address(dump->trap_frame.rip, sym_tables, section_maps)));
    saved_label->setText(QString("Saved RIP: %1").arg(wosdbg::format_address(dump->saved_frame.rip, sym_tables, section_maps)));

    disassemble_at(dump->trap_frame.rip);
}

void CoredumpDisasmPanel::disassemble_at(uint64_t va, int num_instructions) {
    if (!dump || !cs) {
        return;
    }

    QByteArray elf = dump->embedded_elf();
    if (elf.isEmpty()) {
        view->setPlainText("(no embedded ELF in coredump)");
        return;
    }

    uint64_t start_va = (va > LOOK_BEHIND) ? va - LOOK_BEHIND : 0;
    auto bytes = wosdbg::elf_bytes_at_va(elf, start_va, DISASM_BYTES);
    if (bytes.empty()) {
        view->setPlainText(QString("(no ELF bytes mapped at 0x%1)").arg(start_va, 16, 16, QChar('0')));
        return;
    }

    cs_insn* insns = nullptr;
    size_t count = cs_disasm(cs, bytes.data(), bytes.size(), start_va, 0, &insns);
    if (count == 0) {
        view->setPlainText("(Capstone: disassembly failed)");
        return;
    }

    uint64_t trap_rip = dump->trap_frame.rip;
    uint64_t saved_rip = dump->saved_frame.rip;

    // Anchor window at first instruction >= va
    int anchor = 0;
    for (size_t i = 0; i < count; ++i) {
        if (insns[i].address >= va) {
            anchor = static_cast<int>(i);
            break;
        }
    }
    int start_i = std::max(0, anchor - (num_instructions / 3));
    int end_i = std::min(static_cast<int>(count), start_i + num_instructions);

    QString out;
    out += QString("; around 0x%1\n").arg(va, 16, 16, QChar('0'));
    out += QString("; trap  ==> 0x%1\n").arg(trap_rip, 16, 16, QChar('0'));
    out += QString("; saved  -- 0x%1\n\n").arg(saved_rip, 16, 16, QChar('0'));

    int target_line = -1;
    int cur_line = static_cast<int>(out.count('\n'));

    for (int i = start_i; i < end_i; ++i) {
        const cs_insn& ins = insns[static_cast<size_t>(i)];
        uint64_t addr = ins.address;

        // Symbol label at this address
        auto sym = wosdbg::resolve_address(addr, sym_tables, section_maps);
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
        if (ins.size > 7) {
            hex_bytes += "... ";
        }
        // Pad to fixed width (7*3 + 4 = 25 chars)
        hex_bytes = hex_bytes.leftJustified(25, ' ');

        // Mnemonic + operands, mnemonic left-justified to 8 chars
        QString mnem = QString(ins.mnemonic).leftJustified(8, ' ');
        QString line =
            QString("%1  %2  %3  %4 %5\n").arg(marker, QString("%1").arg(addr, 16, 16, QChar('0')), hex_bytes, mnem, QString(ins.op_str));
        out += line;
        ++cur_line;
    }

    cs_free(insns, count);

    view->setPlainText(out);

    if (target_line >= 0) {
        QTextCursor cursor(view->document()->findBlockByLineNumber(target_line));
        view->setTextCursor(cursor);
        view->centerCursor();
    }
}

void CoredumpDisasmPanel::clear() {
    dump = nullptr;
    sym_tables.clear();
    section_maps.clear();
    view->clear();
    rip_label->setText("Trap RIP: \xe2\x80\x94");
    saved_label->setText("Saved RIP: \xe2\x80\x94");
}
