#pragma once

#include <QDockWidget>
#include <QLabel>
#include <QTableWidget>
#include <QTextEdit>
#include <QTreeWidget>

#include "coredump_parser.h"
#include "elf_symbol_resolver.h"

/// Panel showing ELF metadata: embedded ELF info, loaded symbol tables,
/// section maps, and resolved binary paths.
class CoredumpElfPanel : public QDockWidget {
    Q_OBJECT

   public:
    explicit CoredumpElfPanel(QWidget* parent = nullptr);

    void set_core_dump(const wosdbg::CoreDump* dump);
    void set_symbol_info(const QString& binary_name, const QString& elf_path, const wosdbg::SymbolTable* symtab,
                         const wosdbg::SectionMap* sections);
    void add_symbol_source(const QString& label, const wosdbg::SymbolTable* symtab, const wosdbg::SectionMap* sections);
    void clear();

   signals:
    void address_clicked(uint64_t addr);

   private:
    void setup_ui();

    QLabel* binary_label = nullptr;
    QLabel* elf_path_label = nullptr;
    QLabel* embedded_elf_label = nullptr;

    QTreeWidget* source_tree = nullptr;     // Symbol sources tree
    QTableWidget* section_table = nullptr;  // Sections of the main ELF
};
