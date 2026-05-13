#pragma once

#include <qpoint.h>
#include <qtmetamacros.h>

#include <QProcess>
#include <QString>
#include <QTreeWidget>
#include <QWidget>

namespace wosdbg {
struct CoreDump;
}

// Tree-based coredump browser panel.
// Recursively scans a directory for *_coredump.bin files,
// grouped by subdirectory (VM name).
class CoredumpBrowser : public QWidget {
    Q_OBJECT

   public:
    explicit CoredumpBrowser(QWidget* parent = nullptr);
    ~CoredumpBrowser() override;

    // Set the root directory to scan for coredumps
    void set_directory(const QString& dir);
    [[nodiscard]] QString directory() const { return coredump_dir; }

   public slots:
    // Re-scan the coredump directory
    void refresh();

    // Run extract_coredumps.sh to extract from QCOW2 disk images
    void extract_coredumps(bool cluster_mode = true);

   signals:
    // Emitted when a coredump is double-clicked / activated
    void coredump_selected(const QString& file_path);

    // Emitted when extraction finishes (success or failure)
    void extraction_finished(bool success, const QString& message);

   private slots:
    void on_item_activated(QTreeWidgetItem* item, int column);
    void on_context_menu(const QPoint& pos);
    void on_extraction_finished(int exit_code, QProcess::ExitStatus status);

   private:
    void setup_ui();
    void scan_directory();

    QTreeWidget* tree;
    QString coredump_dir;
    QProcess* extract_process = nullptr;
};
