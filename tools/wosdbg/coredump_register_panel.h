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
    void load_core_dump(const wosdbg::CoreDump& dump, const std::vector<wosdbg::SymbolTable*>& sym_tables = {},
                        const std::vector<wosdbg::SectionMap*>& section_maps = {});

    // Clear all data
    void clear();

   signals:
    // Emitted when user clicks an address value (e.g. RIP) to navigate
    void address_clicked(uint64_t addr);

   private:
    void setup_ui();
    static void populate_frame_table(QTableWidget* table, const wosdbg::CoreDump& dump, bool is_trap,
                                     const std::vector<wosdbg::SymbolTable*>& sym_tables,
                                     const std::vector<wosdbg::SectionMap*>& section_maps);

    QTableWidget* header_table;  // PID, CPU, interrupt, CR2, CR3, timestamp
    QTableWidget* trap_table;    // Trap frame + regs
    QTableWidget* saved_table;   // Saved frame + regs
};
