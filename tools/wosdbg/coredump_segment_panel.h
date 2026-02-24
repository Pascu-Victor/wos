#pragma once

#include <QDockWidget>
#include <QTableWidget>
#include <cstdint>

namespace wosdbg {
struct CoreDump;
}

// Dockable panel listing coredump memory segments.
// Double-clicking a segment triggers a memory dump of that segment.
class CoredumpSegmentPanel : public QDockWidget {
    Q_OBJECT

   public:
    explicit CoredumpSegmentPanel(QWidget* parent = nullptr);

    // Load segment data from a parsed coredump
    void loadCoreDump(const wosdbg::CoreDump& dump);

    void clear();

   signals:
    // Emitted when a segment is double-clicked — requests memory dump
    void dumpSegmentRequested(int segmentIndex, uint64_t vaStart, uint64_t vaEnd);

   private slots:
    void onSegmentActivated(int row, int column);

   private:
    void setupUI();

    QTableWidget* table_;

    // Store segment info for click handling
    struct SegInfo {
        int index;
        uint64_t vaStart;
        uint64_t vaEnd;
        bool present;
    };
    std::vector<SegInfo> segInfos_;
};
