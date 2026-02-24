#include "coredump_register_panel.h"

#include <QFont>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QLabel>
#include <QSplitter>
#include <QVBoxLayout>

#include "coredump_parser.h"
#include "elf_symbol_resolver.h"

CoredumpRegisterPanel::CoredumpRegisterPanel(QWidget* parent) : QDockWidget("Registers", parent) { setupUI(); }

static QTableWidgetItem* makeItem(const QString& text, bool monospace = true) {
    auto* item = new QTableWidgetItem(text);
    item->setFlags(item->flags() & ~Qt::ItemIsEditable);
    if (monospace) {
        QFont f("Monospace", 9);
        f.setStyleHint(QFont::Monospace);
        item->setFont(f);
    }
    return item;
}

static QTableWidgetItem* makeAddrItem(uint64_t addr, const std::vector<wosdbg::SymbolTable*>& symTables,
                                      const std::vector<wosdbg::SectionMap*>& sectionMaps) {
    QString text = wosdbg::formatAddress(addr, symTables, sectionMaps);
    auto* item = makeItem(text);
    item->setData(Qt::UserRole, QVariant::fromValue(addr));
    // Color code addresses: code addresses in blue, stack in green
    if (addr >= 0x400000 && addr <= 0xFFFFFF) {
        item->setForeground(QColor(100, 149, 237));  // Cornflower blue
    } else if (addr >= 0xffffffff80000000ULL) {
        item->setForeground(QColor(144, 238, 144));  // Light green (kernel)
    } else if ((addr >> 40) == 0x7ffe || (addr >> 40) == 0x7fff) {
        item->setForeground(QColor(255, 200, 100));  // Orange (stack)
    }
    return item;
}

void CoredumpRegisterPanel::setupUI() {
    auto* container = new QWidget(this);
    auto* mainLayout = new QVBoxLayout(container);
    mainLayout->setContentsMargins(4, 4, 4, 4);
    mainLayout->setSpacing(2);

    // Header info table (PID, CPU, interrupt, etc.) — compact
    headerTable_ = new QTableWidget(container);
    headerTable_->setColumnCount(8);
    headerTable_->setRowCount(1);
    headerTable_->setHorizontalHeaderLabels({"PID", "CPU", "Interrupt", "Error Code", "CR2", "CR3", "Timestamp", "Entry"});
    headerTable_->verticalHeader()->setVisible(false);
    headerTable_->setFixedHeight(52);
    headerTable_->horizontalHeader()->setStretchLastSection(true);
    headerTable_->setAlternatingRowColors(true);
    headerTable_->verticalHeader()->setDefaultSectionSize(22);
    mainLayout->addWidget(headerTable_);

    // Vertical splitter: register tables stacked, each scrollable
    auto* outerSplitter = new QSplitter(Qt::Vertical, container);

    // Helper to create a compact, scrollable register table inside a labeled group
    auto makeRegGroup = [&](const QString& title, QTableWidget*& table) -> QWidget* {
        auto* w = new QWidget(outerSplitter);
        auto* lay = new QVBoxLayout(w);
        lay->setContentsMargins(0, 0, 0, 0);
        lay->setSpacing(1);
        auto* lbl = new QLabel(QString("<b>%1</b>").arg(title), w);
        lay->addWidget(lbl);
        table = new QTableWidget(w);
        table->setColumnCount(2);
        table->setHorizontalHeaderLabels({"Register", "Value"});
        table->verticalHeader()->setVisible(false);
        table->verticalHeader()->setDefaultSectionSize(20);
        table->horizontalHeader()->setStretchLastSection(true);
        table->setAlternatingRowColors(true);
        table->setVerticalScrollBarPolicy(Qt::ScrollBarAsNeeded);
        table->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
        lay->addWidget(table);
        w->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
        return w;
    };

    outerSplitter->addWidget(makeRegGroup("Trap State (at fault)", trapTable_));
    outerSplitter->addWidget(makeRegGroup("Saved State (before fault)", savedTable_));
    outerSplitter->setStretchFactor(0, 1);
    outerSplitter->setStretchFactor(1, 1);

    mainLayout->addWidget(outerSplitter, 1);  // stretch factor 1 so splitter fills space
    container->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
    setWidget(container);

    // Connect click to address navigation
    auto connectClicks = [this](QTableWidget* table) {
        connect(table, &QTableWidget::cellClicked, [this, table](int row, int col) {
            if (col == 1) {
                QTableWidgetItem* item = table->item(row, col);
                if (item) {
                    QVariant v = item->data(Qt::UserRole);
                    if (v.isValid()) {
                        emit addressClicked(v.toULongLong());
                    }
                }
            }
        });
    };
    connectClicks(trapTable_);
    connectClicks(savedTable_);
    connectClicks(headerTable_);
}

void CoredumpRegisterPanel::populateFrameTable(QTableWidget* table, const wosdbg::CoreDump& dump, bool isTrap,
                                               const std::vector<wosdbg::SymbolTable*>& symTables,
                                               const std::vector<wosdbg::SectionMap*>& sectionMaps) {
    const auto& frame = isTrap ? dump.trapFrame : dump.savedFrame;
    const auto& regs = isTrap ? dump.trapRegs : dump.savedRegs;

    // 7 frame registers + 15 GP registers = 22 rows
    table->setRowCount(22);
    int row = 0;

    auto addReg = [&](const QString& name, uint64_t val, bool isAddr = false) {
        table->setItem(row, 0, makeItem(name, false));
        if (isAddr) {
            table->setItem(row, 1, makeAddrItem(val, symTables, sectionMaps));
        } else {
            table->setItem(row, 1, makeItem(wosdbg::formatU64(val)));
        }
        row++;
    };

    // Interrupt frame
    addReg("RIP", frame.rip, true);
    addReg("RSP", frame.rsp, true);
    addReg("CS", frame.cs);
    addReg("SS", frame.ss);
    addReg("RFLAGS", frame.rflags);
    addReg("IntNum", frame.intNum);
    addReg("ErrCode", frame.errCode);

    // General purpose registers
    addReg("RAX", regs.rax);
    addReg("RBX", regs.rbx);
    addReg("RCX", regs.rcx);
    addReg("RDX", regs.rdx);
    addReg("RSI", regs.rsi);
    addReg("RDI", regs.rdi);
    addReg("RBP", regs.rbp, true);
    addReg("R8", regs.r8);
    addReg("R9", regs.r9);
    addReg("R10", regs.r10);
    addReg("R11", regs.r11);
    addReg("R12", regs.r12);
    addReg("R13", regs.r13);
    addReg("R14", regs.r14);
    addReg("R15", regs.r15);

    table->resizeColumnsToContents();
}

void CoredumpRegisterPanel::loadCoreDump(const wosdbg::CoreDump& dump, const std::vector<wosdbg::SymbolTable*>& symTables,
                                         const std::vector<wosdbg::SectionMap*>& sectionMaps) {
    // Populate header info
    headerTable_->setItem(0, 0, makeItem(QString::number(dump.pid)));
    headerTable_->setItem(0, 1, makeItem(QString::number(dump.cpu)));
    headerTable_->setItem(0, 2, makeItem(QString("%1 (%2)").arg(dump.intNum).arg(wosdbg::interruptName(dump.intNum))));
    headerTable_->setItem(0, 3, makeItem(wosdbg::formatU64(dump.errCode)));
    headerTable_->setItem(0, 4, makeAddrItem(dump.cr2, symTables, sectionMaps));
    headerTable_->setItem(0, 5, makeItem(wosdbg::formatU64(dump.cr3)));
    headerTable_->setItem(0, 6, makeItem(QString::number(dump.timestamp)));
    headerTable_->setItem(0, 7, makeAddrItem(dump.taskEntry, symTables, sectionMaps));
    headerTable_->resizeColumnsToContents();

    // Populate register tables
    populateFrameTable(trapTable_, dump, true, symTables, sectionMaps);
    populateFrameTable(savedTable_, dump, false, symTables, sectionMaps);
}

void CoredumpRegisterPanel::clear() {
    headerTable_->clearContents();
    trapTable_->setRowCount(0);
    savedTable_->setRowCount(0);
}
