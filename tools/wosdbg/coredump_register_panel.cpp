#include "coredump_register_panel.h"

#include <qcolor.h>
#include <qdockwidget.h>
#include <qnamespace.h>
#include <qobject.h>
#include <qsizepolicy.h>
#include <qtablewidget.h>
#include <qtmetamacros.h>
#include <qwidget.h>

#include <QFont>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QLabel>
#include <QSplitter>
#include <QVBoxLayout>
#include <cstdint>
#include <vector>

#include "coredump_parser.h"
#include "elf_symbol_resolver.h"

CoredumpRegisterPanel::CoredumpRegisterPanel(QWidget* parent) : QDockWidget("Registers", parent) { setup_ui(); }

static QTableWidgetItem* make_item(const QString& text, bool monospace = true) {
    auto* item = new QTableWidgetItem(text);
    item->setFlags(item->flags() & ~Qt::ItemIsEditable);
    if (monospace) {
        QFont f("Monospace", 9);
        f.setStyleHint(QFont::Monospace);
        item->setFont(f);
    }
    return item;
}

static QTableWidgetItem* make_addr_item(uint64_t addr, const std::vector<wosdbg::SymbolTable*>& sym_tables,
                                        const std::vector<wosdbg::SectionMap*>& section_maps) {
    QString text = wosdbg::format_address(addr, sym_tables, section_maps);
    auto* item = make_item(text);
    item->setData(Qt::UserRole, QVariant::fromValue(addr));
    // Color code addresses: code addresses in blue, stack in green
    if (addr >= 0x400000 && addr <= 0xFFFFFF) {
        item->setForeground(QColor(100, 149, 237));  // Cornflower blue
    } else if (addr >= 0xffffffff80000000ULL) {
        item->setForeground(QColor(144, 238, 144));  // Light green (kernel)
    } else if ((addr >> 40) == 0x7ffe || (addr >> 40) == 0x7fff) {
        item->setForeground(QColor(255, 200, 100));  // Orange (stack)
    }
    return item;
}

void CoredumpRegisterPanel::setup_ui() {
    auto* container = new QWidget(this);
    auto* main_layout = new QVBoxLayout(container);
    main_layout->setContentsMargins(4, 4, 4, 4);
    main_layout->setSpacing(2);

    // Header info table (PID, CPU, interrupt, etc.) - compact
    header_table = new QTableWidget(container);
    header_table->setColumnCount(8);
    header_table->setRowCount(1);
    header_table->setHorizontalHeaderLabels({"PID", "CPU", "Interrupt", "Error Code", "CR2", "CR3", "Timestamp", "Entry"});
    header_table->verticalHeader()->setVisible(false);
    header_table->setFixedHeight(52);
    header_table->horizontalHeader()->setStretchLastSection(true);
    header_table->setAlternatingRowColors(true);
    header_table->verticalHeader()->setDefaultSectionSize(22);
    main_layout->addWidget(header_table);

    // Vertical splitter: register tables stacked, each scrollable
    auto* outer_splitter = new QSplitter(Qt::Vertical, container);

    // Helper to create a compact, scrollable register table inside a labeled group
    auto make_reg_group = [&](const QString& title, QTableWidget*& table) -> QWidget* {
        auto* w = new QWidget(outer_splitter);
        auto* lay = new QVBoxLayout(w);
        lay->setContentsMargins(0, 0, 0, 0);
        lay->setSpacing(1);
        auto* lbl = new QLabel(QString("<b>%1</b>").arg(title), w);
        lay->addWidget(lbl);
        table = new QTableWidget(w);
        table->setColumnCount(2);
        table->setHorizontalHeaderLabels({"Register", "Value"});
        table->verticalHeader()->setVisible(false);
        table->verticalHeader()->setDefaultSectionSize(20);
        table->horizontalHeader()->setStretchLastSection(true);
        table->setAlternatingRowColors(true);
        table->setVerticalScrollBarPolicy(Qt::ScrollBarAsNeeded);
        table->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
        lay->addWidget(table);
        w->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
        return w;
    };

    outer_splitter->addWidget(make_reg_group("Trap State (at fault)", trap_table));
    outer_splitter->addWidget(make_reg_group("Saved State (before fault)", saved_table));
    outer_splitter->setStretchFactor(0, 1);
    outer_splitter->setStretchFactor(1, 1);

    main_layout->addWidget(outer_splitter, 1);  // stretch factor 1 so splitter fills space
    container->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
    setWidget(container);

    // Connect click to address navigation
    auto connect_clicks = [this](QTableWidget* table) {
        connect(table, &QTableWidget::cellClicked, [this, table](int row, int col) {
            if (col == 1) {
                QTableWidgetItem* item = table->item(row, col);
                if (item) {
                    QVariant v = item->data(Qt::UserRole);
                    if (v.isValid()) {
                        emit address_clicked(v.toULongLong());
                    }
                }
            }
        });
    };
    connect_clicks(trap_table);
    connect_clicks(saved_table);
    connect_clicks(header_table);
}

void CoredumpRegisterPanel::populate_frame_table(QTableWidget* table, const wosdbg::CoreDump& dump, bool is_trap,
                                                 const std::vector<wosdbg::SymbolTable*>& sym_tables,
                                                 const std::vector<wosdbg::SectionMap*>& section_maps) {
    const auto& frame = is_trap ? dump.trap_frame : dump.saved_frame;
    const auto& regs = is_trap ? dump.trap_regs : dump.saved_regs;

    // 7 frame registers + 15 GP registers = 22 rows
    table->setRowCount(22);
    int row = 0;

    auto add_reg = [&](const QString& name, uint64_t val, bool is_addr = false) {
        table->setItem(row, 0, make_item(name, false));
        if (is_addr) {
            table->setItem(row, 1, make_addr_item(val, sym_tables, section_maps));
        } else {
            table->setItem(row, 1, make_item(wosdbg::format_u64(val)));
        }
        row++;
    };

    // Interrupt frame
    add_reg("RIP", frame.rip, true);
    add_reg("RSP", frame.rsp, true);
    add_reg("CS", frame.cs);
    add_reg("SS", frame.ss);
    add_reg("RFLAGS", frame.rflags);
    add_reg("int_num", frame.int_num);
    add_reg("err_code", frame.err_code);

    // General purpose registers
    add_reg("RAX", regs.rax);
    add_reg("RBX", regs.rbx);
    add_reg("RCX", regs.rcx);
    add_reg("RDX", regs.rdx);
    add_reg("RSI", regs.rsi);
    add_reg("RDI", regs.rdi);
    add_reg("RBP", regs.rbp, true);
    add_reg("R8", regs.r8);
    add_reg("R9", regs.r9);
    add_reg("R10", regs.r10);
    add_reg("R11", regs.r11);
    add_reg("R12", regs.r12);
    add_reg("R13", regs.r13);
    add_reg("R14", regs.r14);
    add_reg("R15", regs.r15);

    table->resizeColumnsToContents();
}

void CoredumpRegisterPanel::load_core_dump(const wosdbg::CoreDump& dump, const std::vector<wosdbg::SymbolTable*>& sym_tables,
                                           const std::vector<wosdbg::SectionMap*>& section_maps) {
    // Populate header info
    header_table->setItem(0, 0, make_item(QString::number(dump.pid)));
    header_table->setItem(0, 1, make_item(QString::number(dump.cpu)));
    header_table->setItem(0, 2, make_item(QString("%1 (%2)").arg(dump.int_num).arg(wosdbg::interrupt_name(dump.int_num))));
    header_table->setItem(0, 3, make_item(wosdbg::format_u64(dump.err_code)));
    header_table->setItem(0, 4, make_addr_item(dump.cr2, sym_tables, section_maps));
    header_table->setItem(0, 5, make_item(wosdbg::format_u64(dump.cr3)));
    header_table->setItem(0, 6, make_item(QString::number(dump.timestamp)));
    header_table->setItem(0, 7, make_addr_item(dump.task_entry, sym_tables, section_maps));
    header_table->resizeColumnsToContents();

    // Populate register tables
    populate_frame_table(trap_table, dump, true, sym_tables, section_maps);
    populate_frame_table(saved_table, dump, false, sym_tables, section_maps);
}

void CoredumpRegisterPanel::clear() {
    header_table->clearContents();
    trap_table->setRowCount(0);
    saved_table->setRowCount(0);
}
