#include "coredump_memory_panel.h"

#include <qabstractitemview.h>
#include <qcolor.h>
#include <qdockwidget.h>
#include <qlineedit.h>
#include <qnamespace.h>
#include <qobject.h>
#include <qpushbutton.h>
#include <qtablewidget.h>
#include <qtextedit.h>
#include <qtmetamacros.h>
#include <qwidget.h>

#include <QFont>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QLabel>
#include <QSplitter>
#include <QVBoxLayout>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "coredump_memory.h"
#include "coredump_parser.h"
#include "elf_symbol_resolver.h"

CoredumpMemoryPanel::CoredumpMemoryPanel(QWidget* parent) : QDockWidget("Memory", parent) { setup_ui(); }

void CoredumpMemoryPanel::setup_ui() {
    auto* container = new QWidget(this);
    auto* main_layout = new QVBoxLayout(container);
    main_layout->setContentsMargins(4, 4, 4, 4);
    main_layout->setSpacing(4);

    // Address range toolbar
    auto* toolbar = new QHBoxLayout();
    toolbar->addWidget(new QLabel("From:"));
    from_edit = new QLineEdit(container);
    from_edit->setPlaceholderText("0x...");
    from_edit->setMaximumWidth(180);
    QFont mono("Monospace", 9);
    mono.setStyleHint(QFont::Monospace);
    from_edit->setFont(mono);
    toolbar->addWidget(from_edit);

    toolbar->addWidget(new QLabel("To:"));
    to_edit = new QLineEdit(container);
    to_edit->setPlaceholderText("0x...");
    to_edit->setMaximumWidth(180);
    to_edit->setFont(mono);
    toolbar->addWidget(to_edit);

    dump_button = new QPushButton("Dump", container);
    connect(dump_button, &QPushButton::clicked, this, &CoredumpMemoryPanel::on_dump_button_clicked);
    toolbar->addWidget(dump_button);

    auto* rsp_button = new QPushButton("Stack @ RSP", container);
    connect(rsp_button, &QPushButton::clicked, this, &CoredumpMemoryPanel::dump_stack_around_rsp);
    toolbar->addWidget(rsp_button);

    toolbar->addStretch();
    main_layout->addLayout(toolbar);

    // Splitter: qword table on top, hex dump below
    auto* splitter = new QSplitter(Qt::Vertical, container);

    // Annotated qword table
    qword_table = new QTableWidget(splitter);
    qword_table->setColumnCount(5);
    qword_table->setHorizontalHeaderLabels({"", "Virtual Address", "Value", "Symbol", "Notes"});
    qword_table->verticalHeader()->setVisible(false);
    qword_table->horizontalHeader()->setStretchLastSection(true);
    qword_table->setAlternatingRowColors(true);
    qword_table->setSelectionBehavior(QAbstractItemView::SelectRows);
    qword_table->setColumnWidth(0, 30);  // Gutter

    connect(qword_table, &QTableWidget::cellClicked, [this](int row, int col) {
        if (col == 2) {  // Value column
            QTableWidgetItem* item = qword_table->item(row, col);
            if (item) {
                QVariant v = item->data(Qt::UserRole);
                if (v.isValid()) {
                    emit address_clicked(v.toULongLong());
                }
            }
        }
    });

    splitter->addWidget(qword_table);

    // Raw hex view
    hex_view = new QTextEdit(splitter);
    hex_view->setReadOnly(true);
    hex_view->setFont(mono);
    hex_view->setMaximumHeight(200);
    splitter->addWidget(hex_view);

    splitter->setSizes({400, 200});
    main_layout->addWidget(splitter);

    setWidget(container);
}

static QTableWidgetItem* make_mono_item(const QString& text) {
    auto* item = new QTableWidgetItem(text);
    item->setFlags(item->flags() & ~Qt::ItemIsEditable);
    QFont f("Monospace", 9);
    f.setStyleHint(QFont::Monospace);
    item->setFont(f);
    return item;
}

void CoredumpMemoryPanel::set_core_dump(const wosdbg::CoreDump* dump, const std::vector<wosdbg::SymbolTable*>& sym_tables,
                                        const std::vector<wosdbg::SectionMap*>& section_maps) {
    this->current_dump = dump;
    this->sym_tables = sym_tables;
    this->section_maps = section_maps;
}

void CoredumpMemoryPanel::dump_range(uint64_t va_start, uint64_t va_end) {
    if (!current_dump) {
        return;
    }

    // Update address fields
    from_edit->setText(wosdbg::format_u64(va_start));
    to_edit->setText(wosdbg::format_u64(va_end));

    // Populate qword table
    auto qwords = wosdbg::dump_range(*current_dump, va_start, va_end, sym_tables, section_maps);
    qword_table->setRowCount(static_cast<int>(qwords.size()));

    for (int i = 0; std::cmp_less(i, qwords.size()); ++i) {
        const auto& q = qwords[static_cast<size_t>(i)];

        // Gutter (direction indicator)
        auto* gutter_item = make_mono_item(q.gutter);
        if (q.gutter == ">>>") {
            gutter_item->setBackground(QColor(80, 80, 40));  // Highlight RSP row
        }
        qword_table->setItem(i, 0, gutter_item);

        // Virtual address
        qword_table->setItem(i, 1, make_mono_item(wosdbg::format_u64(q.va)));

        // Value
        auto* val_item = make_mono_item(wosdbg::format_u64(q.value));
        val_item->setData(Qt::UserRole, QVariant::fromValue(q.value));
        qword_table->setItem(i, 2, val_item);

        // Symbol
        auto* sym_item = make_mono_item(q.symbol);
        if (!q.symbol.isEmpty()) {
            sym_item->setForeground(QColor(100, 149, 237));  // Blue for symbols
        }
        qword_table->setItem(i, 3, sym_item);

        // Notes
        qword_table->setItem(i, 4, make_mono_item(q.notes));

        // Highlight the RSP row
        if (q.gutter == ">>>") {
            for (int c = 0; c < 5; ++c) {
                auto* item = qword_table->item(i, c);
                if (item) {
                    item->setBackground(QColor(80, 80, 40));
                }
            }
        }
    }

    qword_table->resizeColumnsToContents();

    // Populate hex view
    auto rows = wosdbg::dump_range_hex(*current_dump, va_start, va_end);
    QString hex_text;
    for (const auto& row : rows) {
        hex_text += QString("%1:  %2  |%3|\n").arg(wosdbg::format_u64(row.va)).arg(row.hex_string, -48).arg(row.ascii_string);
    }
    hex_view->setPlainText(hex_text);
}

void CoredumpMemoryPanel::dump_stack_around_rsp() {
    if (!current_dump) {
        return;
    }

    uint64_t rsp = current_dump->trap_frame.rsp;
    // Dump 256 bytes before and 256 bytes after RSP
    uint64_t start = (rsp >= 0x100) ? rsp - 0x100 : 0;
    uint64_t end = rsp + 0x100;
    dump_range(start, end);
}

void CoredumpMemoryPanel::on_dump_button_clicked() {
    if (!current_dump) {
        return;
    }

    bool ok_from;
    bool ok_to;
    uint64_t va_start = from_edit->text().toULongLong(&ok_from, 16);
    uint64_t va_end = to_edit->text().toULongLong(&ok_to, 16);

    if (!ok_from || !ok_to || va_end <= va_start) {
        return;
    }

    dump_range(va_start, va_end);
}

void CoredumpMemoryPanel::clear() {
    qword_table->setRowCount(0);
    hex_view->clear();
    from_edit->clear();
    to_edit->clear();
    current_dump = nullptr;
}
