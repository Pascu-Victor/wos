#pragma once

#include <QDockWidget>
#include <QLineEdit>
#include <QPushButton>
#include <QTableWidget>
#include <QTextEdit>
#include <cstdint>
#include <memory>
#include <vector>

namespace wosdbg {
struct CoreDump;
class SymbolTable;
class SectionMap;
}  // namespace wosdbg

// Dockable memory/stack dump panel with annotated qword view and raw hex.
class CoredumpMemoryPanel : public QDockWidget {
    Q_OBJECT

   public:
    explicit CoredumpMemoryPanel(QWidget* parent = nullptr);

    // Set the current coredump context (must be called before dump requests)
    void setCoreDump(const wosdbg::CoreDump* dump, const std::vector<wosdbg::SymbolTable*>& symTables = {},
                     const std::vector<wosdbg::SectionMap*>& sectionMaps = {});

    // Dump a virtual address range
    void dumpRange(uint64_t vaStart, uint64_t vaEnd);

    // Auto-dump the stack around trap RSP
    void dumpStackAroundRsp();

    void clear();

   signals:
    void addressClicked(uint64_t addr);

   private slots:
    void onDumpButtonClicked();

   private:
    void setupUI();

    QLineEdit* fromEdit_;
    QLineEdit* toEdit_;
    QPushButton* dumpButton_;
    QTableWidget* qwordTable_;  // Annotated qword view
    QTextEdit* hexView_;        // Raw hex dump

    const wosdbg::CoreDump* currentDump_ = nullptr;
    std::vector<wosdbg::SymbolTable*> symTables_;
    std::vector<wosdbg::SectionMap*> sectionMaps_;
};
