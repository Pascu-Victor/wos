#ifndef VIRTUAL_TABLE_INTEGRATION_H
#define VIRTUAL_TABLE_INTEGRATION_H

#include <qtmetamacros.h>

#include <QtCore/QObject>
#include <vector>

#include "qemu_log_viewer.h"
#include "virtual_table.h"

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
     * @param entries Reference to the log entries vector
     * @param visibleEntries Reference to the visible entry pointers
     * @return The created VirtualTableView
     */
    static auto initializeVirtualTable(QemuLogViewer* viewer, const std::vector<LogEntry>& entries,
                                       std::vector<const LogEntry*>& visibleEntries) -> VirtualTableView*;

    /**
     * Create a data provider function for loading rows
     */
    static std::function<void(int, std::vector<QString>&, QColor&)> createDataProvider(const std::vector<const LogEntry*>& visibleEntries);

    /**
     * Get formatted cell data for a row
     */
    static void formatRowData(const LogEntry* entry, std::vector<QString>& outCells, QColor& outBgColor);

   private:
    VirtualTableIntegration() = default;
};

#endif  // VIRTUAL_TABLE_INTEGRATION_H
