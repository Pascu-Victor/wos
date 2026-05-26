#include "virtual_table_integration.h"

#include <qcontainerfwd.h>
#include <qfont.h>
#include <qlogging.h>
#include <qnamespace.h>
#include <qobject.h>

#include <QtGui/QColor>
#include <functional>
#include <vector>

#include "log_entry.h"
#include "virtual_table.h"
#include "wosdbg.h"

auto VirtualTableIntegration::initialize_virtual_table(QemuLogViewer* viewer, LogClient* client) -> VirtualTableView* {
    // Create virtual view
    auto* virtual_view = new VirtualTableView(viewer);

    // Create model with appropriate row count (initially 0)
    QStringList headers = {"Line", "Type", "Address", "Function", "Hex Bytes", "Assembly"};
    auto* model = new VirtualTableModel(0, headers, virtual_view);

    // Set data provider
    auto data_provider = create_data_provider(client);
    model->setDataProvider(data_provider);

    // Set model
    virtual_view->setVirtualModel(model);

    // Configure appearance
    virtual_view->horizontalHeader()->resizeSection(0, 60);         // Line
    virtual_view->horizontalHeader()->resizeSection(1, 80);         // Type
    virtual_view->horizontalHeader()->resizeSection(2, 120);        // Address
    virtual_view->horizontalHeader()->resizeSection(3, 200);        // Function
    virtual_view->horizontalHeader()->resizeSection(4, 140);        // Hex Bytes
    virtual_view->horizontalHeader()->setStretchLastSection(true);  // Assembly

    virtual_view->setFont(QFont("Consolas", 11));
    virtual_view->verticalHeader()->setDefaultSectionSize(24);
    virtual_view->verticalHeader()->hide();

    return virtual_view;
}

auto VirtualTableIntegration::create_data_provider(LogClient* client) -> std::function<void(int, std::vector<QString>&, QColor&)> {
    return [client](int row, std::vector<QString>& out_cells, QColor& out_bg_color) {
        if (row < 0 || row >= client->get_total_lines()) {
            out_cells.clear();
            out_bg_color = Qt::darkGray;
            return;
        }

        const LogEntry* entry = client->get_entry(row);
        if (!entry) {
            // Data not available yet (loading)
            if (row < 5) {
                qDebug() << "VirtualTableIntegration: Entry not found for row" << row;
            }
            out_cells.clear();
            out_cells.push_back(QString::number(row + 1));  // Line number
            out_cells.emplace_back("Loading...");
            out_bg_color = Qt::black;
            return;
        }

        format_row_data(entry, out_cells, out_bg_color);
    };
}

void VirtualTableIntegration::format_row_data(const LogEntry* entry, std::vector<QString>& out_cells, QColor& out_bg_color) {
    out_cells.clear();
    out_cells.reserve(6);

    // Line number
    out_cells.push_back(QString::number(entry->line_number));

    // Type
    QString type_str;
    switch (entry->type) {
        case EntryType::INSTRUCTION:
            type_str = "INSTRUCTION";
            break;
        case EntryType::INTERRUPT:
            type_str = "INTERRUPT";
            break;
        case EntryType::REGISTER:
            type_str = "REGISTER";
            break;
        case EntryType::BLOCK:
            type_str = "BLOCK";
            break;
        case EntryType::SEPARATOR:
            type_str = "SEPARATOR";
            break;
        case EntryType::OTHER:
            type_str = "OTHER";
            break;
    }
    out_cells.push_back(type_str);

    // Address
    out_cells.push_back(QString::fromStdString(entry->address));

    // Function
    out_cells.push_back(QString::fromStdString(entry->function));

    // Hex Bytes
    out_cells.push_back(QString::fromStdString(entry->hex_bytes));

    // Assembly
    out_cells.push_back(QString::fromStdString(entry->assembly));

    // Background color based on type
    switch (entry->type) {
        case EntryType::INSTRUCTION:
            out_bg_color = QColor(9, 19, 9);  // Dark green
            break;
        case EntryType::INTERRUPT:
            out_bg_color = QColor(19, 9, 9);  // Dark red
            break;
        case EntryType::REGISTER:
            out_bg_color = QColor(9, 9, 19);  // Dark blue
            break;
        case EntryType::BLOCK:
            out_bg_color = QColor(19, 19, 9);  // Dark yellow
            break;
        case EntryType::SEPARATOR:
            out_bg_color = QColor(13, 13, 13);  // Dark gray
            break;
        case EntryType::OTHER:
        default:
            out_bg_color = QColor(8, 8, 8);  // Darker gray
            break;
    }
}

#include "moc_virtual_table_integration.cpp"  // NOLINT
