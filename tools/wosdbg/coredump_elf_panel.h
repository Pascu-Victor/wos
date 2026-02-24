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

    void setCoreDump(const wosdbg::CoreDump* dump);
    void setSymbolInfo(const QString& binaryName, const QString& elfPath, const wosdbg::SymbolTable* symtab,
                       const wosdbg::SectionMap* sections);
    void addSymbolSource(const QString& label, const wosdbg::SymbolTable* symtab, const wosdbg::SectionMap* sections);
    void clear();

   signals:
    void addressClicked(uint64_t addr);

   private:
    void setupUI();

    QLabel* binaryLabel_ = nullptr;
    QLabel* elfPathLabel_ = nullptr;
    QLabel* embeddedElfLabel_ = nullptr;

    QTreeWidget* sourceTree_ = nullptr;     // Symbol sources tree
    QTableWidget* sectionTable_ = nullptr;  // Sections of the main ELF
};
