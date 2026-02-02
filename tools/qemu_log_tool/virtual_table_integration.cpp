#include "virtual_table_integration.h"

#include <qcontainerfwd.h>
#include <qfont.h>
#include <qnamespace.h>
#include <qobject.h>
#include <qtpreprocessorsupport.h>

#include <QtGui/QColor>
#include <functional>
#include <vector>

#include "qemu_log_viewer.h"
#include "virtual_table.h"

auto VirtualTableIntegration::initializeVirtualTable(QemuLogViewer* viewer, LogClient* client) -> VirtualTableView* {
    // Create virtual view
    auto virtualView = new VirtualTableView(viewer);

    // Create model with appropriate row count (initially 0)
    QStringList headers = {"Line", "Type", "Address", "Function", "Hex Bytes", "Assembly"};
    auto model = new VirtualTableModel(0, headers, virtualView);

    // Set data provider
    auto dataProvider = createDataProvider(client);
    model->setDataProvider(dataProvider);

    // Set model
    virtualView->setVirtualModel(model);

    // Configure appearance
    virtualView->horizontalHeader()->resizeSection(0, 60);         // Line
    virtualView->horizontalHeader()->resizeSection(1, 80);         // Type
    virtualView->horizontalHeader()->resizeSection(2, 120);        // Address
    virtualView->horizontalHeader()->resizeSection(3, 200);        // Function
    virtualView->horizontalHeader()->resizeSection(4, 140);        // Hex Bytes
    virtualView->horizontalHeader()->setStretchLastSection(true);  // Assembly

    virtualView->setFont(QFont("Consolas", 11));
    virtualView->verticalHeader()->setDefaultSectionSize(24);
    virtualView->verticalHeader()->hide();

    return virtualView;
}

auto VirtualTableIntegration::createDataProvider(LogClient* client) -> std::function<void(int, std::vector<QString>&, QColor&)> {
    return [client](int row, std::vector<QString>& outCells, QColor& outBgColor) {
        if (row < 0 || row >= client->getTotalLines()) {
            outCells.clear();
            outBgColor = Qt::darkGray;
            return;
        }

        const LogEntry* entry = client->getEntry(row);
        if (!entry) {
            // Data not available yet (loading)
            if (row < 5) qDebug() << "VirtualTableIntegration: Entry not found for row" << row;
            outCells.clear();
            outCells.push_back(QString::number(row + 1));  // Line number
            outCells.push_back("Loading...");
            outBgColor = Qt::black;
            return;
        }

        formatRowData(entry, outCells, outBgColor);
    };
}

void VirtualTableIntegration::formatRowData(const LogEntry* entry, std::vector<QString>& outCells, QColor& outBgColor) {
    outCells.clear();
    outCells.reserve(6);

    // Line number
    outCells.push_back(QString::number(entry->lineNumber));

    // Type
    QString typeStr;
    switch (entry->type) {
        case EntryType::INSTRUCTION:
            typeStr = "INSTRUCTION";
            break;
        case EntryType::INTERRUPT:
            typeStr = "INTERRUPT";
            break;
        case EntryType::REGISTER:
            typeStr = "REGISTER";
            break;
        case EntryType::BLOCK:
            typeStr = "BLOCK";
            break;
        case EntryType::SEPARATOR:
            typeStr = "SEPARATOR";
            break;
        case EntryType::OTHER:
            typeStr = "OTHER";
            break;
    }
    outCells.push_back(typeStr);

    // Address
    outCells.push_back(QString::fromStdString(entry->address));

    // Function
    outCells.push_back(QString::fromStdString(entry->function));

    // Hex Bytes
    outCells.push_back(QString::fromStdString(entry->hexBytes));

    // Assembly
    outCells.push_back(QString::fromStdString(entry->assembly));

    // Background color based on type
    switch (entry->type) {
        case EntryType::INSTRUCTION:
            outBgColor = QColor(9, 19, 9);  // Dark green
            break;
        case EntryType::INTERRUPT:
            outBgColor = QColor(19, 9, 9);  // Dark red
            break;
        case EntryType::REGISTER:
            outBgColor = QColor(9, 9, 19);  // Dark blue
            break;
        case EntryType::BLOCK:
            outBgColor = QColor(19, 19, 9);  // Dark yellow
            break;
        case EntryType::SEPARATOR:
            outBgColor = QColor(13, 13, 13);  // Dark gray
            break;
        case EntryType::OTHER:
        default:
            outBgColor = QColor(8, 8, 8);  // Darker gray
            break;
    }
}

#include "moc_virtual_table_integration.cpp"  // NOLINT
