#pragma once

#include <QDockWidget>
#include <QLabel>
#include <QPlainTextEdit>
#include <QPushButton>
#include <cstdint>
#include <vector>

#include <capstone/capstone.h>

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
    void loadCoreDump(const wosdbg::CoreDump* dump,
                      const std::vector<wosdbg::SymbolTable*>& symTables = {},
                      const std::vector<wosdbg::SectionMap*>& sectionMaps = {});

    /// Disassemble around an arbitrary virtual address (from the embedded ELF).
    void disassembleAt(uint64_t va, int numInstructions = 30);

    void clear();

   signals:
    void addressClicked(uint64_t addr);

   private:
    void setupUI();
    QString disassembleRange(uint64_t va, int numInstructions);

    QLabel*        ripLabel_    = nullptr;
    QLabel*        savedLabel_  = nullptr;
    QPushButton*   trapRipBtn_  = nullptr;
    QPushButton*   savedRipBtn_ = nullptr;
    QPlainTextEdit* view_       = nullptr;

    const wosdbg::CoreDump*               dump_         = nullptr;
    std::vector<wosdbg::SymbolTable*>     symTables_;
    std::vector<wosdbg::SectionMap*>      sectionMaps_;

    csh cs_ = 0;  // Capstone handle (0 = not initialised)
};
