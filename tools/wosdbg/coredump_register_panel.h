#pragma once

#include <QDockWidget>
#include <QTableWidget>
#include <QWidget>
#include <memory>
#include <vector>

namespace wosdbg {
struct CoreDump;
class SymbolTable;
class SectionMap;
}  // namespace wosdbg

// Dockable panel showing CPU register state from a coredump.
// Shows both trap state (at fault) and saved state (before fault).
class CoredumpRegisterPanel : public QDockWidget {
    Q_OBJECT

   public:
    explicit CoredumpRegisterPanel(QWidget* parent = nullptr);

    // Load register data from a parsed coredump
    void loadCoreDump(const wosdbg::CoreDump& dump, const std::vector<wosdbg::SymbolTable*>& symTables = {},
                      const std::vector<wosdbg::SectionMap*>& sectionMaps = {});

    // Clear all data
    void clear();

   signals:
    // Emitted when user clicks an address value (e.g. RIP) to navigate
    void addressClicked(uint64_t addr);

   private:
    void setupUI();
    void populateFrameTable(QTableWidget* table, const wosdbg::CoreDump& dump, bool isTrap,
                            const std::vector<wosdbg::SymbolTable*>& symTables, const std::vector<wosdbg::SectionMap*>& sectionMaps);

    QTableWidget* headerTable_;  // PID, CPU, interrupt, CR2, CR3, timestamp
    QTableWidget* trapTable_;    // Trap frame + regs
    QTableWidget* savedTable_;   // Saved frame + regs
};
