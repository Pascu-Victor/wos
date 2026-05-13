#pragma once

#include <capstone/capstone.h>

#include <QDockWidget>
#include <QLabel>
#include <QPlainTextEdit>
#include <QPushButton>
#include <cstdint>
#include <vector>

namespace wosdbg {
struct CoreDump;
class SymbolTable;
class SectionMap;
}  // namespace wosdbg

/// Dockable panel that disassembles code from the embedded ELF around
/// the trap RIP and the saved RIP, using Capstone in x86-64 Intel mode.
class CoredumpDisasmPanel : public QDockWidget {
    Q_OBJECT

   public:
    explicit CoredumpDisasmPanel(QWidget* parent = nullptr);
    ~CoredumpDisasmPanel() override;

    /// Load the coredump and symbol context.  Call this when a dump is opened.
    void load_core_dump(const wosdbg::CoreDump* dump, const std::vector<wosdbg::SymbolTable*>& sym_tables = {},
                        const std::vector<wosdbg::SectionMap*>& section_maps = {});

    /// Disassemble around an arbitrary virtual address (from the embedded ELF).
    void disassemble_at(uint64_t va, int num_instructions = 30);

    void clear();

   signals:
    void address_clicked(uint64_t addr);

   private:
    void setup_ui();
    QString disassemble_range(uint64_t va, int num_instructions);

    QLabel* rip_label = nullptr;
    QLabel* saved_label = nullptr;
    QPushButton* trap_rip_btn = nullptr;
    QPushButton* saved_rip_btn = nullptr;
    QPlainTextEdit* view = nullptr;

    const wosdbg::CoreDump* dump = nullptr;
    std::vector<wosdbg::SymbolTable*> sym_tables;
    std::vector<wosdbg::SectionMap*> section_maps;

    csh cs = 0;  // Capstone handle (0 = not initialised)
};
