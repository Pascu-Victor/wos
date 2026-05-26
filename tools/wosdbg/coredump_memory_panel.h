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
    void set_core_dump(const wosdbg::CoreDump* dump, const std::vector<wosdbg::SymbolTable*>& sym_tables = {},
                       const std::vector<wosdbg::SectionMap*>& section_maps = {});

    // Dump a virtual address range
    void dump_range(uint64_t vaStart, uint64_t vaEnd);

    // Auto-dump the stack around trap RSP
    void dump_stack_around_rsp();

    void clear();

   signals:
    void address_clicked(uint64_t addr);

   private slots:
    void on_dump_button_clicked();

   private:
    void setup_ui();

    QLineEdit* from_edit;
    QLineEdit* to_edit;
    QPushButton* dump_button;
    QTableWidget* qword_table;  // Annotated qword view
    QTextEdit* hex_view;        // Raw hex dump

    const wosdbg::CoreDump* current_dump = nullptr;
    std::vector<wosdbg::SymbolTable*> sym_tables;
    std::vector<wosdbg::SectionMap*> section_maps;
};
