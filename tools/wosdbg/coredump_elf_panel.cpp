#include "coredump_elf_panel.h"

#include <QFont>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QVBoxLayout>

CoredumpElfPanel::CoredumpElfPanel(QWidget* parent) : QDockWidget("ELF Info", parent) { setupUI(); }

void CoredumpElfPanel::setupUI() {
    auto* container = new QWidget(this);
    auto* layout = new QVBoxLayout(container);
    layout->setContentsMargins(4, 4, 4, 4);
    layout->setSpacing(4);

    QFont mono("Monospace", 9);
    mono.setStyleHint(QFont::Monospace);

    // Binary identification
    binaryLabel_ = new QLabel("Binary: (none)", container);
    binaryLabel_->setFont(mono);
    layout->addWidget(binaryLabel_);

    elfPathLabel_ = new QLabel("ELF Path: (none)", container);
    elfPathLabel_->setFont(mono);
    elfPathLabel_->setWordWrap(true);
    layout->addWidget(elfPathLabel_);

    embeddedElfLabel_ = new QLabel("Embedded ELF: (none)", container);
    embeddedElfLabel_->setFont(mono);
    layout->addWidget(embeddedElfLabel_);

    // Symbol sources tree
    auto* srcLabel = new QLabel("Symbol Sources:", container);
    layout->addWidget(srcLabel);

    sourceTree_ = new QTreeWidget(container);
    sourceTree_->setHeaderLabels({"Source", "Symbols", "Sections"});
    sourceTree_->setAlternatingRowColors(true);
    sourceTree_->setMaximumHeight(150);
    layout->addWidget(sourceTree_);

    // Section table
    auto* secLabel = new QLabel("Sections:", container);
    layout->addWidget(secLabel);

    sectionTable_ = new QTableWidget(container);
    sectionTable_->setColumnCount(4);
    sectionTable_->setHorizontalHeaderLabels({"Name", "VA Start", "VA End", "Size"});
    sectionTable_->verticalHeader()->setVisible(false);
    sectionTable_->horizontalHeader()->setStretchLastSection(true);
    sectionTable_->setAlternatingRowColors(true);
    sectionTable_->setSelectionBehavior(QAbstractItemView::SelectRows);
    sectionTable_->setEditTriggers(QAbstractItemView::NoEditTriggers);
    sectionTable_->setFont(mono);
    layout->addWidget(sectionTable_);

    setWidget(container);
}

void CoredumpElfPanel::setCoreDump(const wosdbg::CoreDump* dump) {
    if (!dump) return;

    QByteArray embeddedElf = dump->embeddedElf();
    if (!embeddedElf.isEmpty()) {
        embeddedElfLabel_->setText(QString("Embedded ELF: %1 bytes").arg(embeddedElf.size()));
    } else {
        embeddedElfLabel_->setText("Embedded ELF: (not present)");
    }
}

void CoredumpElfPanel::setSymbolInfo(const QString& binaryName, const QString& elfPath, const wosdbg::SymbolTable* symtab,
                                     const wosdbg::SectionMap* sections) {
    binaryLabel_->setText(QString("Binary: %1").arg(binaryName));
    elfPathLabel_->setText(elfPath.isEmpty() ? "ELF Path: (not resolved)" : QString("ELF Path: %1").arg(elfPath));

    // Clear and re-add sources
    sourceTree_->clear();

    if (symtab || sections) {
        addSymbolSource(elfPath.isEmpty() ? binaryName : elfPath, symtab, sections);
    }
}

void CoredumpElfPanel::addSymbolSource(const QString& label, const wosdbg::SymbolTable* symtab, const wosdbg::SectionMap* sections) {
    auto* item = new QTreeWidgetItem(sourceTree_);
    item->setText(0, label);
    item->setText(1, symtab ? QString::number(symtab->size()) : "0");
    item->setText(2, sections ? QString::number(sections->size()) : "0");

    // If we have sections, populate the section table
    if (sections && sections->size() > 0) {
        sectionTable_->setRowCount(0);
        const auto& entries = sections->entries();
        sectionTable_->setRowCount(static_cast<int>(entries.size()));

        QFont mono("Monospace", 9);
        mono.setStyleHint(QFont::Monospace);

        for (int i = 0; i < static_cast<int>(entries.size()); ++i) {
            const auto& entry = entries[static_cast<size_t>(i)];

            auto makeItem = [&](const QString& text) {
                auto* item = new QTableWidgetItem(text);
                item->setFlags(item->flags() & ~Qt::ItemIsEditable);
                item->setFont(mono);
                return item;
            };

            sectionTable_->setItem(i, 0, makeItem(QString::fromStdString(entry.name)));
            sectionTable_->setItem(i, 1, makeItem(wosdbg::formatU64(entry.vaddr)));
            sectionTable_->setItem(i, 2, makeItem(wosdbg::formatU64(entry.vaddr + entry.size)));
            sectionTable_->setItem(i, 3, makeItem(QString("0x%1").arg(entry.size, 0, 16)));
        }
        sectionTable_->resizeColumnsToContents();
    }
}

void CoredumpElfPanel::clear() {
    binaryLabel_->setText("Binary: (none)");
    elfPathLabel_->setText("ELF Path: (none)");
    embeddedElfLabel_->setText("Embedded ELF: (none)");
    sourceTree_->clear();
    sectionTable_->setRowCount(0);
}
