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
    void load_core_dump(const wosdbg::CoreDump& dump);

    void clear();

   signals:
    // Emitted when a segment is double-clicked - requests memory dump
    void dump_segment_requested(int segment_index, uint64_t vaStart, uint64_t vaEnd);

   private slots:
    void on_segment_activated(int row, int column);

   private:
    void setup_ui();

    QTableWidget* table;

    // Store segment info for click handling
    struct SegInfo {
        int index;
        uint64_t vaStart;
        uint64_t vaEnd;
        bool present;
    };
    std::vector<SegInfo> seg_infos;
};
