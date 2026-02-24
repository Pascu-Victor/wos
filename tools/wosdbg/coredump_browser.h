#pragma once

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
    void setDirectory(const QString& dir);
    [[nodiscard]] QString directory() const { return coredumpDir_; }

   public slots:
    // Re-scan the coredump directory
    void refresh();

    // Run extract_coredumps.sh to extract from QCOW2 disk images
    void extractCoredumps(bool clusterMode = true);

   signals:
    // Emitted when a coredump is double-clicked / activated
    void coredumpSelected(const QString& filePath);

    // Emitted when extraction finishes (success or failure)
    void extractionFinished(bool success, const QString& message);

   private slots:
    void onItemActivated(QTreeWidgetItem* item, int column);
    void onContextMenu(const QPoint& pos);
    void onExtractionFinished(int exitCode, QProcess::ExitStatus status);

   private:
    void setupUI();
    void scanDirectory();

    QTreeWidget* tree_;
    QString coredumpDir_;
    QProcess* extractProcess_ = nullptr;
};
