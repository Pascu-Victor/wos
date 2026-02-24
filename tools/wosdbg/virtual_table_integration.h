#ifndef VIRTUAL_TABLE_INTEGRATION_H
#define VIRTUAL_TABLE_INTEGRATION_H

#include <qtmetamacros.h>

#include <QtCore/QObject>
#include <vector>

#include "log_client.h"
#include "virtual_table.h"
#include "wosdbg.h"

/**
 * Integration helper for QemuLogViewer to use VirtualTable
 * This bridges the gap between the log viewer and virtual table system
 */
class VirtualTableIntegration : public QObject {
    Q_OBJECT

   public:
    /**
     * Initialize virtual table for the viewer
     * @param viewer The QemuLogViewer instance
     * @param client The LogClient instance
     * @return The created VirtualTableView
     */
    static auto initializeVirtualTable(QemuLogViewer* viewer, LogClient* client) -> VirtualTableView*;

    /**
     * Create a data provider function for loading rows
     */
    static std::function<void(int, std::vector<QString>&, QColor&)> createDataProvider(LogClient* client);

    /**
     * Get formatted cell data for a row
     */
    static void formatRowData(const LogEntry* entry, std::vector<QString>& outCells, QColor& outBgColor);

   private:
    VirtualTableIntegration() = default;
};

#endif  // VIRTUAL_TABLE_INTEGRATION_H
