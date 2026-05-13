#include "coredump_elf_panel.h"

#include <qabstractitemview.h>
#include <qdockwidget.h>
#include <qlabel.h>
#include <qnamespace.h>
#include <qobject.h>
#include <qstringview.h>
#include <qtablewidget.h>
#include <qtreewidget.h>
#include <qwidget.h>

#include <QFont>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QVBoxLayout>
#include <cstddef>
#include <utility>

#include "coredump_parser.h"
#include "elf_symbol_resolver.h"

CoredumpElfPanel::CoredumpElfPanel(QWidget* parent) : QDockWidget("ELF Info", parent) { setup_ui(); }

void CoredumpElfPanel::setup_ui() {
    auto* container = new QWidget(this);
    auto* layout = new QVBoxLayout(container);
    layout->setContentsMargins(4, 4, 4, 4);
    layout->setSpacing(4);

    QFont mono("Monospace", 9);
    mono.setStyleHint(QFont::Monospace);

    // Binary identification
    binary_label = new QLabel("Binary: (none)", container);
    binary_label->setFont(mono);
    layout->addWidget(binary_label);

    elf_path_label = new QLabel("ELF Path: (none)", container);
    elf_path_label->setFont(mono);
    elf_path_label->setWordWrap(true);
    layout->addWidget(elf_path_label);

    embedded_elf_label = new QLabel("Embedded ELF: (none)", container);
    embedded_elf_label->setFont(mono);
    layout->addWidget(embedded_elf_label);

    // Symbol sources tree
    auto* src_label = new QLabel("Symbol Sources:", container);
    layout->addWidget(src_label);

    source_tree = new QTreeWidget(container);
    source_tree->setHeaderLabels({"Source", "Symbols", "Sections"});
    source_tree->setAlternatingRowColors(true);
    source_tree->setMaximumHeight(150);
    layout->addWidget(source_tree);

    // Section table
    auto* sec_label = new QLabel("Sections:", container);
    layout->addWidget(sec_label);

    section_table = new QTableWidget(container);
    section_table->setColumnCount(4);
    section_table->setHorizontalHeaderLabels({"Name", "VA Start", "VA End", "Size"});
    section_table->verticalHeader()->setVisible(false);
    section_table->horizontalHeader()->setStretchLastSection(true);
    section_table->setAlternatingRowColors(true);
    section_table->setSelectionBehavior(QAbstractItemView::SelectRows);
    section_table->setEditTriggers(QAbstractItemView::NoEditTriggers);
    section_table->setFont(mono);
    layout->addWidget(section_table);

    setWidget(container);
}

void CoredumpElfPanel::set_core_dump(const wosdbg::CoreDump* dump) {
    if (!dump) {
        return;
    }

    QByteArray embedded_elf = dump->embedded_elf();
    if (!embedded_elf.isEmpty()) {
        embedded_elf_label->setText(QString("Embedded ELF: %1 bytes").arg(embedded_elf.size()));
    } else {
        embedded_elf_label->setText("Embedded ELF: (not present)");
    }
}

void CoredumpElfPanel::set_symbol_info(const QString& binary_name, const QString& elf_path, const wosdbg::SymbolTable* symtab,
                                       const wosdbg::SectionMap* sections) {
    binary_label->setText(QString("Binary: %1").arg(binary_name));
    elf_path_label->setText(elf_path.isEmpty() ? "ELF Path: (not resolved)" : QString("ELF Path: %1").arg(elf_path));

    // Clear and re-add sources
    source_tree->clear();

    if (symtab || sections) {
        add_symbol_source(elf_path.isEmpty() ? binary_name : elf_path, symtab, sections);
    }
}

void CoredumpElfPanel::add_symbol_source(const QString& label, const wosdbg::SymbolTable* symtab, const wosdbg::SectionMap* sections) {
    auto* item = new QTreeWidgetItem(source_tree);
    item->setText(0, label);
    item->setText(1, symtab ? QString::number(symtab->size()) : "0");
    item->setText(2, sections ? QString::number(sections->size()) : "0");

    // If we have sections, populate the section table
    if (sections && sections->size() > 0) {
        section_table->setRowCount(0);
        const auto& entries = sections->entries();
        section_table->setRowCount(static_cast<int>(entries.size()));

        QFont mono("Monospace", 9);
        mono.setStyleHint(QFont::Monospace);

        for (int i = 0; std::cmp_less(i, entries.size()); ++i) {
            const auto& entry = entries[static_cast<size_t>(i)];

            auto make_item = [&](const QString& text) {
                auto* item = new QTableWidgetItem(text);
                item->setFlags(item->flags() & ~Qt::ItemIsEditable);
                item->setFont(mono);
                return item;
            };

            section_table->setItem(i, 0, make_item(QString::fromStdString(entry.name)));
            section_table->setItem(i, 1, make_item(wosdbg::format_u64(entry.vaddr)));
            section_table->setItem(i, 2, make_item(wosdbg::format_u64(entry.vaddr + entry.size)));
            section_table->setItem(i, 3, make_item(QString("0x%1").arg(entry.size, 0, 16)));
        }
        section_table->resizeColumnsToContents();
    }
}

void CoredumpElfPanel::clear() {
    binary_label->setText("Binary: (none)");
    elf_path_label->setText("ELF Path: (none)");
    embedded_elf_label->setText("Embedded ELF: (none)");
    source_tree->clear();
    section_table->setRowCount(0);
}
