#include "coredump_memory_panel.h"

#include <QFont>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QLabel>
#include <QSplitter>
#include <QVBoxLayout>

#include "coredump_memory.h"
#include "coredump_parser.h"
#include "elf_symbol_resolver.h"

CoredumpMemoryPanel::CoredumpMemoryPanel(QWidget* parent) : QDockWidget("Memory", parent) { setupUI(); }

void CoredumpMemoryPanel::setupUI() {
    auto* container = new QWidget(this);
    auto* main_layout = new QVBoxLayout(container);
    main_layout->setContentsMargins(4, 4, 4, 4);
    main_layout->setSpacing(4);

    // Address range toolbar
    auto* toolbar = new QHBoxLayout();
    toolbar->addWidget(new QLabel("From:"));
    fromEdit_ = new QLineEdit(container);
    fromEdit_->setPlaceholderText("0x...");
    fromEdit_->setMaximumWidth(180);
    QFont mono("Monospace", 9);
    mono.setStyleHint(QFont::Monospace);
    fromEdit_->setFont(mono);
    toolbar->addWidget(fromEdit_);

    toolbar->addWidget(new QLabel("To:"));
    toEdit_ = new QLineEdit(container);
    toEdit_->setPlaceholderText("0x...");
    toEdit_->setMaximumWidth(180);
    toEdit_->setFont(mono);
    toolbar->addWidget(toEdit_);

    dumpButton_ = new QPushButton("Dump", container);
    connect(dumpButton_, &QPushButton::clicked, this, &CoredumpMemoryPanel::onDumpButtonClicked);
    toolbar->addWidget(dumpButton_);

    auto* rsp_button = new QPushButton("Stack @ RSP", container);
    connect(rsp_button, &QPushButton::clicked, this, &CoredumpMemoryPanel::dumpStackAroundRsp);
    toolbar->addWidget(rsp_button);

    toolbar->addStretch();
    main_layout->addLayout(toolbar);

    // Splitter: qword table on top, hex dump below
    auto* splitter = new QSplitter(Qt::Vertical, container);

    // Annotated qword table
    qwordTable_ = new QTableWidget(splitter);
    qwordTable_->setColumnCount(5);
    qwordTable_->setHorizontalHeaderLabels({"", "Virtual Address", "Value", "Symbol", "Notes"});
    qwordTable_->verticalHeader()->setVisible(false);
    qwordTable_->horizontalHeader()->setStretchLastSection(true);
    qwordTable_->setAlternatingRowColors(true);
    qwordTable_->setSelectionBehavior(QAbstractItemView::SelectRows);
    qwordTable_->setColumnWidth(0, 30);  // Gutter

    connect(qwordTable_, &QTableWidget::cellClicked, [this](int row, int col) {
        if (col == 2) {  // Value column
            QTableWidgetItem* item = qwordTable_->item(row, col);
            if (item) {
                QVariant v = item->data(Qt::UserRole);
                if (v.isValid()) {
                    emit addressClicked(v.toULongLong());
                }
            }
        }
    });

    splitter->addWidget(qwordTable_);

    // Raw hex view
    hexView_ = new QTextEdit(splitter);
    hexView_->setReadOnly(true);
    hexView_->setFont(mono);
    hexView_->setMaximumHeight(200);
    splitter->addWidget(hexView_);

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

void CoredumpMemoryPanel::setCoreDump(const wosdbg::CoreDump* dump, const std::vector<wosdbg::SymbolTable*>& sym_tables,
                                      const std::vector<wosdbg::SectionMap*>& section_maps) {
    currentDump_ = dump;
    symTables_ = sym_tables;
    sectionMaps_ = section_maps;
}

void CoredumpMemoryPanel::dumpRange(uint64_t va_start, uint64_t va_end) {
    if (!currentDump_) {
        return;
    }

    // Update address fields
    fromEdit_->setText(wosdbg::formatU64(va_start));
    toEdit_->setText(wosdbg::formatU64(va_end));

    // Populate qword table
    auto qwords = wosdbg::dumpRange(*currentDump_, va_start, va_end, symTables_, sectionMaps_);
    qwordTable_->setRowCount(static_cast<int>(qwords.size()));

    for (int i = 0; i < static_cast<int>(qwords.size()); ++i) {
        const auto& q = qwords[static_cast<size_t>(i)];

        // Gutter (direction indicator)
        auto* gutter_item = make_mono_item(q.gutter);
        if (q.gutter == ">>>") {
            gutter_item->setBackground(QColor(80, 80, 40));  // Highlight RSP row
        }
        qwordTable_->setItem(i, 0, gutter_item);

        // Virtual address
        qwordTable_->setItem(i, 1, make_mono_item(wosdbg::formatU64(q.va)));

        // Value
        auto* val_item = make_mono_item(wosdbg::formatU64(q.value));
        val_item->setData(Qt::UserRole, QVariant::fromValue(q.value));
        qwordTable_->setItem(i, 2, val_item);

        // Symbol
        auto* sym_item = make_mono_item(q.symbol);
        if (!q.symbol.isEmpty()) {
            sym_item->setForeground(QColor(100, 149, 237));  // Blue for symbols
        }
        qwordTable_->setItem(i, 3, sym_item);

        // Notes
        qwordTable_->setItem(i, 4, make_mono_item(q.notes));

        // Highlight the RSP row
        if (q.gutter == ">>>") {
            for (int c = 0; c < 5; ++c) {
                auto* item = qwordTable_->item(i, c);
                if (item) {
                    item->setBackground(QColor(80, 80, 40));
                }
            }
        }
    }

    qwordTable_->resizeColumnsToContents();

    // Populate hex view
    auto rows = wosdbg::dumpRangeHex(*currentDump_, va_start, va_end);
    QString hex_text;
    for (const auto& row : rows) {
        hex_text += QString("%1:  %2  |%3|\n").arg(wosdbg::formatU64(row.va)).arg(row.hexString, -48).arg(row.asciiString);
    }
    hexView_->setPlainText(hex_text);
}

void CoredumpMemoryPanel::dumpStackAroundRsp() {
    if (!currentDump_) {
        return;
    }

    uint64_t rsp = currentDump_->trapFrame.rsp;
    // Dump 256 bytes before and 256 bytes after RSP
    uint64_t start = (rsp >= 0x100) ? rsp - 0x100 : 0;
    uint64_t end = rsp + 0x100;
    dumpRange(start, end);
}

void CoredumpMemoryPanel::onDumpButtonClicked() {
    if (!currentDump_) {
        return;
    }

    bool ok_from;
    bool ok_to;
    uint64_t va_start = fromEdit_->text().toULongLong(&ok_from, 16);
    uint64_t va_end = toEdit_->text().toULongLong(&ok_to, 16);

    if (!ok_from || !ok_to || va_end <= va_start) {
        return;
    }

    dumpRange(va_start, va_end);
}

void CoredumpMemoryPanel::clear() {
    qwordTable_->setRowCount(0);
    hexView_->clear();
    fromEdit_->clear();
    toEdit_->clear();
    currentDump_ = nullptr;
}
